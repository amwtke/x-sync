"""Model-free long-lived CLI for one recovered X-Sync v2 runtime."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
import secrets
import signal
import sys
import threading
import time
from types import FrameType
from typing import BinaryIO, Never, TextIO

from .event_codec import PROTOCOL_VERSION, SCHEMA_VERSION, canonical_json_bytes
from .host_ipc import HostIpcServer
from .runtime import DialogueRuntime


WaitForShutdown = Callable[[HostIpcServer], str]


class RuntimeCliError(RuntimeError):
    """Stable runtime CLI configuration, lifecycle, or output failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> Never:
        raise RuntimeCliError("RUNTIME_CLI_ARGUMENT_INVALID")


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(prog="xsync dialogue runtime")
    commands = parser.add_subparsers(dest="operation", required=True)
    serve = commands.add_parser("serve")
    serve.add_argument("--state", required=True)
    serve.add_argument("--repo", required=True)
    serve.add_argument("--repository-id", required=True)
    serve.add_argument("--registry", required=True)
    serve.add_argument("--host-socket", required=True)
    serve.add_argument("--browser-port", type=int, default=0)
    serve.add_argument("--keepalive-seconds", type=float, default=15.0)
    serve.add_argument("--stream-json", action="store_true")
    return parser


def _runtime_epoch() -> str:
    return f"runtime.{secrets.token_hex(16)}"


def _browser_capability() -> str:
    return f"browser.{secrets.token_hex(32)}"


def _wall_clock() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


def _lease_clock() -> int:
    return int(time.monotonic())


def _wait_for_shutdown(server: HostIpcServer) -> str:
    stopping = threading.Event()

    def stop(_signum: int, _frame: FrameType | None) -> None:
        stopping.set()

    previous_sigint = signal.signal(signal.SIGINT, stop)
    previous_sigterm = signal.signal(signal.SIGTERM, stop)
    try:
        while not stopping.wait(0.2):
            error = server.fatal_error
            if error is not None:
                raise RuntimeCliError(error)
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
    return "signal"


def _streams(
    stdout: BinaryIO | None,
    stderr: TextIO | None,
) -> tuple[BinaryIO, TextIO]:
    output = sys.stdout.buffer if stdout is None else stdout
    errors = sys.stderr if stderr is None else stderr
    if not callable(getattr(output, "write", None)) or not callable(
        getattr(errors, "write", None)
    ):
        raise RuntimeCliError("RUNTIME_CLI_STREAM_INVALID")
    return output, errors


def _emit(output: BinaryIO, event: dict[str, object]) -> None:
    try:
        output.write(canonical_json_bytes(event) + b"\n")
        output.flush()
    except Exception as exc:
        raise RuntimeCliError("RUNTIME_CLI_OUTPUT_FAILED") from exc


def _serve(
    arguments: argparse.Namespace,
    output: BinaryIO,
    wait_for_shutdown: WaitForShutdown,
) -> int:
    if arguments.stream_json is not True:
        raise RuntimeCliError("RUNTIME_CLI_STREAM_JSON_REQUIRED")
    runtime_epoch = _runtime_epoch()
    capability = _browser_capability()
    with DialogueRuntime(
        arguments.state,
        arguments.registry,
        runtime_epoch,
        repository_directory=arguments.repo,
        repository_id=arguments.repository_id,
        lease_clock=_lease_clock,
        browser_clock=_wall_clock,
        monotonic_clock=time.monotonic,
    ) as runtime:
        resolution = runtime.recover()
        if resolution is None:
            raise RuntimeCliError("NO_ACTIVE_DIALOGUE")
        host_server = runtime.start_host_ipc(arguments.host_socket)
        browser_server = runtime.start_browser(
            resolution.config.session_id,
            capability,
            port=arguments.browser_port,
            keepalive_seconds=arguments.keepalive_seconds,
        )
        _emit(
            output,
            {
                "schema_version": SCHEMA_VERSION,
                "protocol_version": PROTOCOL_VERSION,
                "stream_sequence": 1,
                "type": "ready",
                "runtime_epoch": runtime_epoch,
                "session_id": resolution.config.session_id,
                "host_socket": host_server.path,
                "browser_url": browser_server.launch_url,
            },
        )
        reason = wait_for_shutdown(host_server)
        _emit(
            output,
            {
                "schema_version": SCHEMA_VERSION,
                "protocol_version": PROTOCOL_VERSION,
                "stream_sequence": 2,
                "type": "closed",
                "reason": reason,
            },
        )
    return 0


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: BinaryIO | None = None,
    stderr: TextIO | None = None,
    wait_for_shutdown: WaitForShutdown = _wait_for_shutdown,
) -> int:
    """Recover and serve one owner-fenced runtime until signal shutdown."""
    try:
        output, errors = _streams(stdout, stderr)
    except RuntimeCliError:
        return 2
    try:
        arguments = _parser().parse_args(argv)
        if arguments.operation != "serve" or not callable(wait_for_shutdown):
            raise RuntimeCliError("RUNTIME_CLI_ARGUMENT_INVALID")
        return _serve(arguments, output, wait_for_shutdown)
    except Exception as exc:
        code = getattr(exc, "code", "RUNTIME_CLI_FAILED")
        if type(code) is not str or not code:
            code = "RUNTIME_CLI_FAILED"
        errors.write(code + "\n")
        errors.flush()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["RuntimeCliError", "WaitForShutdown", "main"]
