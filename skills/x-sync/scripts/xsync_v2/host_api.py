"""Strict one-request/one-response JSON adapter for the Host control plane."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import cast

from .domain import TriggerKind
from .event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    ActorKind,
    DialogueActor,
    canonical_json_bytes,
)
from .host_context import HostContextError, encode_host_context
from .host_control import (
    HostClaimEnvelope,
    HostControl,
    HostControlError,
    HostReclaimRequest,
    HostWorkAdvanced,
)
from .host_result import HostResultError, decode_host_result
from .host_work import (
    HostResultPublishRequest,
    HostWorkServiceError,
)
from .lease_store import (
    ClaimRequest,
    LeaseMutationOutcome,
    LeaseRecord,
    LeaseStoreError,
    PublishFence,
    ReclaimRequest,
    RenewRequest,
)
from .work import RunnableWork, WorkError, validate_runnable_work
from .work_identity import is_protocol_id, is_sha256_digest


MAX_HOST_API_REQUEST_BYTES = 128 * 1024
_WORK_KEYS = frozenset(
    {
        "session_id",
        "registry_generation",
        "conversation_version",
        "observed_sequence",
        "trigger_event_id",
        "trigger_event_sequence",
        "topic_run_id",
        "kind",
        "trigger_work_id",
        "work_id",
        "trigger_runtime_epoch",
        "parent_turn_id",
        "contract_digest",
        "input_digest",
        "evidence_digest",
        "binding_digest",
    }
)
_FENCE_KEYS = frozenset(
    {
        "session_id",
        "claim_id",
        "work_id",
        "owner_id",
        "runtime_epoch",
        "lease_version",
        "registry_generation",
    }
)


class HostApiError(RuntimeError):
    """Stable Host JSON schema or routing failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class HostApiResponse:
    """Canonical response bytes suitable for stdout or private IPC."""

    body: bytes


def _object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise HostApiError("HOST_API_DUPLICATE_KEY")
        result[key] = value
    return result


def _load(raw: bytes) -> dict[str, object]:
    if type(raw) is not bytes or not raw or len(raw) > MAX_HOST_API_REQUEST_BYTES:
        raise HostApiError("HOST_API_REQUEST_INVALID")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_object_pairs)
    except HostApiError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise HostApiError("HOST_API_REQUEST_INVALID") from exc
    if type(value) is not dict:
        raise HostApiError("HOST_API_REQUEST_INVALID")
    return value


def _keys(value: dict[str, object], expected: frozenset[str]) -> None:
    common = frozenset({"schema_version", "protocol_version", "operation"})
    if frozenset(value) != common | expected:
        raise HostApiError("HOST_API_SCHEMA_INVALID")
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != SCHEMA_VERSION
        or type(value["protocol_version"]) is not str
        or value["protocol_version"] != PROTOCOL_VERSION
    ):
        raise HostApiError("HOST_API_PROTOCOL_UNSUPPORTED")


def _identifier(value: object) -> str:
    if not is_protocol_id(value):
        raise HostApiError("HOST_API_SCHEMA_INVALID")
    return cast(str, value)


def _positive_int(value: object) -> int:
    if type(value) is not int or value < 1:
        raise HostApiError("HOST_API_SCHEMA_INVALID")
    return value


def _nonnegative_int(value: object) -> int:
    if type(value) is not int or value < 0:
        raise HostApiError("HOST_API_SCHEMA_INVALID")
    return value


def _optional_identifier(value: object) -> str | None:
    return None if value is None else _identifier(value)


def _optional_digest(value: object) -> str | None:
    if value is None:
        return None
    if not is_sha256_digest(value):
        raise HostApiError("HOST_API_SCHEMA_INVALID")
    return cast(str, value)


def _work_tree(work: RunnableWork) -> dict[str, object]:
    work = validate_runnable_work(work)
    return {
        "session_id": work.session_id,
        "registry_generation": work.registry_generation,
        "conversation_version": work.conversation_version,
        "observed_sequence": work.observed_sequence,
        "trigger_event_id": work.trigger_event_id,
        "trigger_event_sequence": work.trigger_event_sequence,
        "topic_run_id": work.topic_run_id,
        "kind": work.kind.value,
        "trigger_work_id": work.trigger_work_id,
        "work_id": work.work_id,
        "trigger_runtime_epoch": work.trigger_runtime_epoch,
        "parent_turn_id": work.parent_turn_id,
        "contract_digest": work.contract_digest,
        "input_digest": work.input_digest,
        "evidence_digest": work.evidence_digest,
        "binding_digest": work.binding_digest,
    }


def _decode_work(value: object) -> RunnableWork:
    if type(value) is not dict or frozenset(value) != _WORK_KEYS:
        raise HostApiError("HOST_API_SCHEMA_INVALID")
    item = cast(dict[str, object], value)
    try:
        work = RunnableWork(
            _identifier(item["session_id"]),
            _positive_int(item["registry_generation"]),
            _nonnegative_int(item["conversation_version"]),
            _positive_int(item["observed_sequence"]),
            _identifier(item["trigger_event_id"]),
            _positive_int(item["trigger_event_sequence"]),
            _optional_identifier(item["topic_run_id"]),
            TriggerKind(cast(str, item["kind"])),
            _identifier(item["trigger_work_id"]),
            _identifier(item["work_id"]),
            _identifier(item["trigger_runtime_epoch"]),
            _optional_identifier(item["parent_turn_id"]),
            _optional_digest(item["contract_digest"]),
            cast(str, item["input_digest"]),
            cast(str, item["evidence_digest"]),
            cast(str, item["binding_digest"]),
        )
        return validate_runnable_work(work)
    except (TypeError, ValueError, WorkError) as exc:
        raise HostApiError("HOST_API_SCHEMA_INVALID") from exc


def _lease_tree(lease: LeaseRecord) -> dict[str, object]:
    return {
        "session_id": lease.session_id,
        "work_id": lease.work_id,
        "binding_digest": lease.binding_digest,
        "claim_id": lease.claim_id,
        "owner_id": lease.owner_id,
        "runtime_epoch": lease.runtime_epoch,
        "lease_version": lease.lease_version,
        "registry_generation": lease.registry_generation,
        "acquired_at": lease.acquired_at,
        "renewed_at": lease.renewed_at,
        "expires_at": lease.expires_at,
        "absolute_expires_at": lease.absolute_expires_at,
    }


def _fence_tree(fence: PublishFence) -> dict[str, object]:
    return {
        "session_id": fence.session_id,
        "claim_id": fence.claim_id,
        "work_id": fence.work_id,
        "owner_id": fence.owner_id,
        "runtime_epoch": fence.runtime_epoch,
        "lease_version": fence.lease_version,
        "registry_generation": fence.registry_generation,
    }


def _decode_fence(value: object) -> PublishFence:
    if type(value) is not dict or frozenset(value) != _FENCE_KEYS:
        raise HostApiError("HOST_API_SCHEMA_INVALID")
    item = cast(dict[str, object], value)
    return PublishFence(
        _identifier(item["session_id"]),
        _identifier(item["claim_id"]),
        _identifier(item["work_id"]),
        _identifier(item["owner_id"]),
        _identifier(item["runtime_epoch"]),
        _positive_int(item["lease_version"]),
        _positive_int(item["registry_generation"]),
    )


def _success(operation: str, payload: dict[str, object]) -> HostApiResponse:
    return HostApiResponse(
        canonical_json_bytes(
            {
                "ok": True,
                "operation": operation,
                "payload": payload,
                "protocol_version": PROTOCOL_VERSION,
                "schema_version": SCHEMA_VERSION,
            }
        )
    )


def _failure(operation: object, code: str) -> HostApiResponse:
    safe_operation = (
        operation
        if type(operation) is str
        and operation in {"wait", "claim", "renew", "reclaim", "publish"}
        else "invalid"
    )
    return HostApiResponse(
        canonical_json_bytes(
            {
                "error": {"code": code},
                "ok": False,
                "operation": safe_operation,
                "protocol_version": PROTOCOL_VERSION,
                "schema_version": SCHEMA_VERSION,
            }
        )
    )


class HostApi:
    """Route strict JSON operations to one model-neutral HostControl."""

    def __init__(self, control: HostControl) -> None:
        if type(control) is not HostControl:
            raise HostApiError("HOST_API_CONFIGURATION_INVALID")
        self._control = control

    def handle(self, raw: bytes) -> HostApiResponse:
        """Return one canonical response and never expose exception detail."""
        operation: object = "invalid"
        try:
            request = _load(raw)
            operation = request.get("operation")
            if operation == "wait":
                return self._wait(request)
            if operation == "claim":
                return self._claim(request)
            if operation == "renew":
                return self._renew(request)
            if operation == "reclaim":
                return self._reclaim(request)
            if operation == "publish":
                return self._publish(request)
            raise HostApiError("HOST_API_OPERATION_UNSUPPORTED")
        except (
            HostApiError,
            HostContextError,
            HostControlError,
            HostResultError,
            HostWorkServiceError,
            LeaseStoreError,
            WorkError,
        ) as exc:
            return _failure(operation, exc.code)
        except Exception:
            return _failure(operation, "INTERNAL_ERROR")

    def _wait(self, request: dict[str, object]) -> HostApiResponse:
        _keys(request, frozenset({"session_id", "timeout"}))
        timeout = request["timeout"]
        if timeout is not None and (type(timeout) is not int or timeout < 0):
            raise HostApiError("HOST_API_SCHEMA_INVALID")
        outcome = self._control.wait(
            _identifier(request["session_id"]),
            timeout=timeout,
        )
        work = outcome.work
        return _success(
            "wait",
            {
                "timed_out": outcome.timed_out,
                "through_sequence": outcome.through_sequence,
                "work": (
                    None
                    if work is None
                    else {
                        "session_id": work.session_id,
                        "work_id": work.work_id,
                        "kind": work.kind.value,
                        "attempt": work.attempt,
                        "observed_sequence": work.observed_sequence,
                    }
                ),
            },
        )

    def _claim(self, request: dict[str, object]) -> HostApiResponse:
        _keys(
            request,
            frozenset(
                {
                    "session_id",
                    "request_id",
                    "claim_id",
                    "work_id",
                    "owner_id",
                    "lease_seconds",
                    "max_tenure_seconds",
                }
            ),
        )
        envelope = self._control.claim(
            ClaimRequest(
                _identifier(request["session_id"]),
                _identifier(request["request_id"]),
                _identifier(request["claim_id"]),
                _identifier(request["work_id"]),
                _identifier(request["owner_id"]),
                _positive_int(request["lease_seconds"]),
                _positive_int(request["max_tenure_seconds"]),
            )
        )
        context = json.loads(encode_host_context(envelope.context))
        return _success(
            "claim",
            {
                "work": _work_tree(envelope.work),
                "lease": _lease_tree(envelope.lease),
                "fence": _fence_tree(envelope.fence),
                "context": context,
            },
        )

    def _renew(self, request: dict[str, object]) -> HostApiResponse:
        _keys(
            request,
            frozenset(
                {
                    "session_id",
                    "request_id",
                    "claim_id",
                    "work_id",
                    "owner_id",
                    "expected_lease_version",
                    "lease_seconds",
                }
            ),
        )
        outcome: LeaseMutationOutcome = self._control.renew(
            RenewRequest(
                _identifier(request["session_id"]),
                _identifier(request["request_id"]),
                _identifier(request["claim_id"]),
                _identifier(request["work_id"]),
                _identifier(request["owner_id"]),
                _positive_int(request["expected_lease_version"]),
                _positive_int(request["lease_seconds"]),
            )
        )
        return _success(
            "renew",
            {
                "lease": _lease_tree(outcome.lease),
                "replayed": outcome.replayed,
            },
        )

    def _reclaim(self, request: dict[str, object]) -> HostApiResponse:
        _keys(
            request,
            frozenset(
                {
                    "session_id",
                    "request_id",
                    "claim_id",
                    "work_id",
                    "owner_id",
                    "expected_work_attempt",
                    "lease_seconds",
                    "max_tenure_seconds",
                    "occurred_at",
                    "actor_id",
                }
            ),
        )
        expected_attempt = _positive_int(request["expected_work_attempt"])
        outcome = self._control.reclaim(
            HostReclaimRequest(
                ReclaimRequest(
                    _identifier(request["session_id"]),
                    _identifier(request["request_id"]),
                    _identifier(request["claim_id"]),
                    _identifier(request["work_id"]),
                    _identifier(request["owner_id"]),
                    expected_attempt,
                    _positive_int(request["lease_seconds"]),
                    _positive_int(request["max_tenure_seconds"]),
                ),
                cast(str, request["occurred_at"]),
                DialogueActor(
                    ActorKind.RUNTIME,
                    _identifier(request["actor_id"]),
                ),
            )
        )
        if type(outcome) is HostClaimEnvelope:
            return _success(
                "reclaim",
                {
                    "disposition": "claimed",
                    "work_attempt": expected_attempt,
                    "work": _work_tree(outcome.work),
                    "lease": _lease_tree(outcome.lease),
                    "fence": _fence_tree(outcome.fence),
                    "context": json.loads(encode_host_context(outcome.context)),
                },
            )
        if type(outcome) is HostWorkAdvanced:
            return _success(
                "reclaim",
                {
                    "disposition": outcome.disposition.value,
                    "work_attempt": expected_attempt,
                    "work_id": outcome.work_id,
                    "conversation_version": outcome.conversation_version,
                    "through_event_sequence": outcome.through_event_sequence,
                    "replayed": outcome.replayed,
                },
            )
        raise HostApiError("HOST_API_OUTCOME_INVALID")

    def _publish(self, request: dict[str, object]) -> HostApiResponse:
        _keys(
            request,
            frozenset(
                {
                    "idempotency_key",
                    "work",
                    "fence",
                    "result",
                    "occurred_at",
                    "actor_id",
                }
            ),
        )
        result_tree = request["result"]
        if type(result_tree) is not dict:
            raise HostApiError("HOST_API_SCHEMA_INVALID")
        try:
            result = decode_host_result(canonical_json_bytes(result_tree))
        except (TypeError, ValueError, RecursionError) as exc:
            raise HostApiError("HOST_API_SCHEMA_INVALID") from exc
        outcome = self._control.publish_result(
            HostResultPublishRequest(
                _identifier(request["idempotency_key"]),
                _decode_work(request["work"]),
                result,
                cast(str, request["occurred_at"]),
                DialogueActor(
                    ActorKind.HOST,
                    _identifier(request["actor_id"]),
                ),
                _decode_fence(request["fence"]),
            )
        )
        receipt = outcome.receipt
        return _success(
            "publish",
            {
                "command_id": receipt.command_id,
                "transaction_id": receipt.transaction_id,
                "from_sequence": receipt.from_sequence,
                "to_sequence": receipt.to_sequence,
                "event_ids": list(receipt.event_ids),
                "state_digest": receipt.state_digest,
                "conversation_version": outcome.state.conversation_version,
                "replayed": outcome.replayed,
            },
        )


__all__ = [
    "MAX_HOST_API_REQUEST_BYTES",
    "HostApi",
    "HostApiError",
    "HostApiResponse",
]
