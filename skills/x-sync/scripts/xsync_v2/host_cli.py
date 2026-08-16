"""Thin one-shot CLI over the shared model-neutral Host IPC protocol."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
import os
import stat
import sys
from typing import BinaryIO, Never, TextIO, cast

from .event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    canonical_json_bytes,
)
from .host_api import MAX_HOST_API_REQUEST_BYTES
from .host_ipc import HostIpcClient, HostIpcError


_MAX_INPUT_FILE_BYTES = MAX_HOST_API_REQUEST_BYTES
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)


class HostCliError(RuntimeError):
    """Stable Host CLI argument, input, response, or output failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> Never:
        raise HostCliError("HOST_CLI_ARGUMENT_INVALID")


def _positive(value: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("positive integer required") from exc
    if str(result) != value or result < 1:
        raise argparse.ArgumentTypeError("positive integer required")
    return result


def _nonnegative(value: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("nonnegative integer required") from exc
    if str(result) != value or result < 0:
        raise argparse.ArgumentTypeError("nonnegative integer required")
    return result


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(prog="xsync dialogue host", add_help=True)
    commands = parser.add_subparsers(dest="operation", required=True)

    wait = commands.add_parser("wait")
    _common(wait)
    wait.add_argument("--session", required=True)
    wait.add_argument("--timeout", type=_nonnegative, required=True)

    claim = commands.add_parser("claim")
    _common(claim)
    claim.add_argument("--session", required=True)
    claim.add_argument("--request-id", required=True)
    claim.add_argument("--claim", dest="claim_id", required=True)
    claim.add_argument("--work", dest="work_id", required=True)
    claim.add_argument("--owner", dest="owner_id", required=True)
    claim.add_argument("--lease-seconds", type=_positive, required=True)
    claim.add_argument("--max-tenure-seconds", type=_positive, required=True)

    renew = commands.add_parser("renew")
    _common(renew)
    renew.add_argument("--session", required=True)
    renew.add_argument("--request-id", required=True)
    renew.add_argument("--claim", dest="claim_id", required=True)
    renew.add_argument("--work", dest="work_id", required=True)
    renew.add_argument("--owner", dest="owner_id", required=True)
    renew.add_argument("--lease-version", type=_positive, required=True)
    renew.add_argument("--lease-seconds", type=_positive, required=True)

    publish = commands.add_parser("publish")
    _common(publish)
    publish.add_argument("--idempotency-key", required=True)
    publish.add_argument("--claim-envelope", required=True)
    publish.add_argument("--lease-version", type=_positive, required=True)
    publish.add_argument("--file", dest="result_file", required=True)
    publish.add_argument("--occurred-at", required=True)
    publish.add_argument("--actor-id", required=True)
    return parser


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--socket", required=True)
    parser.add_argument("--io-timeout", type=float, default=5.0)
    parser.add_argument("--json", action="store_true")


def _pairs(values: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in values:
        if key in result:
            raise HostCliError("HOST_CLI_INPUT_INVALID")
        result[key] = value
    return result


def _read_file(path: object) -> bytes:
    if type(path) is not str or not path or "\0" in path:
        raise HostCliError("HOST_CLI_INPUT_INVALID")
    if not _O_NOFOLLOW or not _O_CLOEXEC:
        raise HostCliError("HOST_CLI_UNSUPPORTED")
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > _MAX_INPUT_FILE_BYTES:
            raise HostCliError("HOST_CLI_INPUT_INVALID")
        chunks: list[bytes] = []
        remaining = _MAX_INPUT_FILE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            len(raw) > _MAX_INPUT_FILE_BYTES
            or before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or before.st_size != after.st_size
            or len(raw) != after.st_size
        ):
            raise HostCliError("HOST_CLI_INPUT_INVALID")
        return raw
    except HostCliError:
        raise
    except OSError as exc:
        raise HostCliError("HOST_CLI_INPUT_FAILED") from exc
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _load_object(raw: bytes) -> dict[str, object]:
    if not raw or len(raw) > _MAX_INPUT_FILE_BYTES:
        raise HostCliError("HOST_CLI_INPUT_INVALID")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
    except HostCliError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise HostCliError("HOST_CLI_INPUT_INVALID") from exc
    if type(value) is not dict:
        raise HostCliError("HOST_CLI_INPUT_INVALID")
    return value


def _claim_payload(path: object) -> tuple[dict[str, object], dict[str, object]]:
    response = _load_object(_read_file(path))
    if (
        frozenset(response)
        != frozenset(
            {
                "schema_version",
                "protocol_version",
                "operation",
                "ok",
                "payload",
            }
        )
        or response["schema_version"] != SCHEMA_VERSION
        or response["protocol_version"] != PROTOCOL_VERSION
        or response["operation"] != "claim"
        or response["ok"] is not True
        or type(response["payload"]) is not dict
    ):
        raise HostCliError("HOST_CLI_INPUT_INVALID")
    payload = cast(dict[str, object], response["payload"])
    if (
        frozenset(payload) != frozenset({"work", "lease", "fence", "context"})
        or type(payload["work"]) is not dict
        or type(payload["fence"]) is not dict
    ):
        raise HostCliError("HOST_CLI_INPUT_INVALID")
    return (
        cast(dict[str, object], payload["work"]),
        cast(dict[str, object], payload["fence"]),
    )


def _base(operation: str) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol_version": PROTOCOL_VERSION,
        "operation": operation,
    }


def _request(arguments: argparse.Namespace) -> bytes:
    operation = cast(str, arguments.operation)
    request = _base(operation)
    if operation == "wait":
        request.update(
            session_id=arguments.session,
            timeout=arguments.timeout,
        )
    elif operation == "claim":
        request.update(
            session_id=arguments.session,
            request_id=arguments.request_id,
            claim_id=arguments.claim_id,
            work_id=arguments.work_id,
            owner_id=arguments.owner_id,
            lease_seconds=arguments.lease_seconds,
            max_tenure_seconds=arguments.max_tenure_seconds,
        )
    elif operation == "renew":
        request.update(
            session_id=arguments.session,
            request_id=arguments.request_id,
            claim_id=arguments.claim_id,
            work_id=arguments.work_id,
            owner_id=arguments.owner_id,
            expected_lease_version=arguments.lease_version,
            lease_seconds=arguments.lease_seconds,
        )
    elif operation == "publish":
        work, fence = _claim_payload(arguments.claim_envelope)
        fence = {**fence, "lease_version": arguments.lease_version}
        result = _load_object(_read_file(arguments.result_file))
        request.update(
            idempotency_key=arguments.idempotency_key,
            work=work,
            fence=fence,
            result=result,
            occurred_at=arguments.occurred_at,
            actor_id=arguments.actor_id,
        )
    else:
        raise HostCliError("HOST_CLI_ARGUMENT_INVALID")
    return canonical_json_bytes(request)


def _response_is_success(raw: bytes, operation: str) -> bool:
    response = _load_object(raw)
    common = {
        "schema_version",
        "protocol_version",
        "operation",
        "ok",
    }
    ok = response.get("ok")
    expected = common | ({"payload"} if ok is True else {"error"})
    if (
        type(ok) is not bool
        or frozenset(response) != frozenset(expected)
        or response["schema_version"] != SCHEMA_VERSION
        or response["protocol_version"] != PROTOCOL_VERSION
        or response["operation"] != operation
        or (ok is True and type(response["payload"]) is not dict)
        or (ok is False and type(response["error"]) is not dict)
    ):
        raise HostCliError("HOST_CLI_RESPONSE_INVALID")
    return ok


def _streams(
    stdout: BinaryIO | None,
    stderr: TextIO | None,
) -> tuple[BinaryIO, TextIO]:
    output = sys.stdout.buffer if stdout is None else stdout
    errors = sys.stderr if stderr is None else stderr
    if not callable(getattr(output, "write", None)) or not callable(
        getattr(errors, "write", None)
    ):
        raise HostCliError("HOST_CLI_INPUT_INVALID")
    return output, errors


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: BinaryIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run one Host operation; stdout is either empty or one JSON envelope."""
    output, errors = _streams(stdout, stderr)
    try:
        arguments = _parser().parse_args(argv)
        if arguments.json is not True:
            raise HostCliError("HOST_CLI_JSON_REQUIRED")
        request = _request(arguments)
        response = HostIpcClient(
            arguments.socket,
            timeout=arguments.io_timeout,
        ).call(request)
        success = _response_is_success(response.body, arguments.operation)
        output.write(response.body + b"\n")
        output.flush()
        return 0 if success else 1
    except (HostCliError, HostIpcError) as exc:
        errors.write(exc.code + "\n")
        errors.flush()
        return 2
    except (OSError, ValueError, TypeError, RecursionError):
        errors.write("HOST_CLI_INTERNAL_ERROR\n")
        errors.flush()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["HostCliError", "main"]
