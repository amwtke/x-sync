"""Model-free Host Supervisor with blocking wait and silent lease renewal."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
import math
from os import PathLike
import secrets
import threading
import time
from typing import Protocol, cast

from .event_codec import PROTOCOL_VERSION, SCHEMA_VERSION, canonical_json_bytes
from .host_ipc import HostIpcClient, HostIpcError
from .host_result import HostResult, HostResultError, encode_host_result
from .work_identity import is_protocol_id, is_sha256_digest


Emit = Callable[[bytes], None]
OccurredAt = Callable[[], str]
IdGenerator = Callable[[str], str]
SecretGenerator = Callable[[], str]
MonotonicClock = Callable[[], float]

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
_LEASE_KEYS = frozenset(
    {
        "session_id",
        "work_id",
        "binding_digest",
        "claim_id",
        "owner_id",
        "runtime_epoch",
        "lease_version",
        "registry_generation",
        "acquired_at",
        "renewed_at",
        "expires_at",
        "absolute_expires_at",
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
_CONTEXT_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "protocol_version",
        "work_id",
        "binding_digest",
        "evidence_digest",
        "topic_contract",
        "task_scope",
        "current_lens",
        "gates",
        "previous_question",
        "learner_turn",
        "learner_model",
        "priority_gap",
        "evidence_claims",
        "learner_model_digest",
        "through_event_sequence",
        "context_digest",
        "selected_candidate",
    }
)


class WaitStrategy(Protocol):
    """One condition wait seam used by deterministic Supervisor tests."""

    def __call__(self, condition: threading.Condition, timeout: float) -> None: ...


class HostSupervisorError(RuntimeError):
    """Stable Supervisor configuration, lifecycle, or protocol failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class HostSupervisorSubmission:
    """One work-bound typed result delivered through an ephemeral handle."""

    submission_handle: str
    idempotency_key: str
    result: HostResult
    occurred_at: str
    actor_id: str


@dataclass(slots=True)
class _ActiveClaim:
    work_attempt: int
    work: dict[str, object]
    lease: dict[str, object]
    fence: dict[str, object]
    context: dict[str, object]
    submission_handle: str


@dataclass(frozen=True, slots=True)
class _ApiResponse:
    ok: bool
    payload: dict[str, object] | None
    error_code: str | None


class _StopSupervisor(RuntimeError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _default_id(prefix: str) -> str:
    return f"{prefix}.{secrets.token_hex(16)}"


def _default_secret() -> str:
    return f"submission.{secrets.token_hex(24)}"


def _default_wait(condition: threading.Condition, timeout: float) -> None:
    condition.wait(timeout)


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        result[key] = value
    return result


def _load_object(raw: bytes) -> dict[str, object]:
    if type(raw) is not bytes or not raw:
        raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
    except HostSupervisorError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID") from exc
    if type(value) is not dict:
        raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
    return value


def _finite_positive(value: object, *, maximum: float) -> float | None:
    if type(value) not in {int, float}:
        return None
    try:
        converted = float(cast("int | float", value))
    except (OverflowError, ValueError):
        return None
    if not math.isfinite(converted) or not 0 < converted <= maximum:
        return None
    return converted


def _valid_submission(value: object) -> HostSupervisorSubmission:
    if (
        type(value) is not HostSupervisorSubmission
        or not is_protocol_id(value.submission_handle)
        or not is_protocol_id(value.idempotency_key)
        or not is_protocol_id(value.actor_id)
        or type(value.occurred_at) is not str
        or not value.occurred_at
    ):
        raise HostSupervisorError("SUBMISSION_INVALID")
    try:
        encode_host_result(value.result)
    except HostResultError as exc:
        raise HostSupervisorError("SUBMISSION_INVALID") from exc
    return value


def _required_mapping(
    value: object,
    keys: frozenset[str],
    code: str = "HOST_SUPERVISOR_RESPONSE_INVALID",
) -> dict[str, object]:
    if type(value) is not dict or frozenset(value) != keys:
        raise HostSupervisorError(code)
    return cast(dict[str, object], value)


class HostSupervisor:
    """Hold one Host owner alive across wait, renew, submit, and publish.

    Lease renewal is model-free and produces no stream envelope.  The public
    stream contains only ``ready``, newly claimed ``work``, meaningful
    ``status`` changes, and one terminal ``closed`` event.
    """

    def __init__(
        self,
        socket_path: str | PathLike[str],
        session_id: str,
        owner_id: str,
        runtime_actor_id: str,
        *,
        emit: Emit,
        occurred_at: OccurredAt,
        id_generator: IdGenerator = _default_id,
        secret_generator: SecretGenerator = _default_secret,
        monotonic_clock: MonotonicClock = time.monotonic,
        wait_strategy: WaitStrategy = _default_wait,
        wait_timeout: int = 30,
        retry_interval: float | int = 1.0,
        lease_seconds: int = 180,
        max_tenure_seconds: int = 900,
        io_timeout: float | int | None = None,
    ) -> None:
        retry = _finite_positive(retry_interval, maximum=60.0)
        active_io_timeout = (
            float(wait_timeout + 5) if io_timeout is None else io_timeout
        )
        io_value = _finite_positive(active_io_timeout, maximum=60.0)
        if (
            not is_protocol_id(session_id)
            or not is_protocol_id(owner_id)
            or not is_protocol_id(runtime_actor_id)
            or not callable(emit)
            or not callable(occurred_at)
            or not callable(id_generator)
            or not callable(secret_generator)
            or not callable(monotonic_clock)
            or not callable(wait_strategy)
            or type(wait_timeout) is not int
            or not 1 <= wait_timeout <= 55
            or type(lease_seconds) is not int
            or not 2 <= lease_seconds <= 3600
            or type(max_tenure_seconds) is not int
            or max_tenure_seconds < lease_seconds
            or max_tenure_seconds > 86_400
            or retry is None
            or io_value is None
        ):
            raise HostSupervisorError("SUPERVISOR_CONFIGURATION_INVALID")
        self._client = HostIpcClient(socket_path, timeout=io_value)
        self._session_id = session_id
        self._owner_id = owner_id
        self._runtime_actor_id = runtime_actor_id
        self._emit_line = emit
        self._occurred_at = occurred_at
        self._id_generator = id_generator
        self._secret_generator = secret_generator
        self._monotonic = monotonic_clock
        self._wait_strategy = wait_strategy
        self._wait_timeout = wait_timeout
        self._retry_interval = retry
        self._lease_seconds = lease_seconds
        self._max_tenure_seconds = max_tenure_seconds
        self._condition = threading.Condition()
        self._active: _ActiveClaim | None = None
        self._submission: HostSupervisorSubmission | None = None
        self._closed = False
        self._close_reason = "requested"
        self._ran = False
        self._stream_sequence = 0
        self._last_monotonic: float | None = None

    def submit(self, submission: HostSupervisorSubmission) -> None:
        """Queue one typed result for the currently advertised handle."""
        submission = _valid_submission(submission)
        with self._condition:
            active = self._active
            if active is None:
                raise HostSupervisorError("NO_ACTIVE_WORK")
            if submission.submission_handle != active.submission_handle:
                raise HostSupervisorError("SUBMISSION_HANDLE_INVALID")
            if self._submission is not None:
                raise HostSupervisorError("SUBMISSION_PENDING")
            self._submission = submission
            self._condition.notify_all()

    def close(self, reason: str = "requested") -> None:
        """Request bounded shutdown without publishing or releasing the lease."""
        if not is_protocol_id(reason):
            raise HostSupervisorError("CLOSE_REASON_INVALID")
        with self._condition:
            if not self._closed:
                self._closed = True
                self._close_reason = reason
            self._condition.notify_all()

    def run(self) -> str:
        """Run until close or a stable transport/protocol failure occurs."""
        with self._condition:
            if self._ran:
                raise HostSupervisorError("SUPERVISOR_ALREADY_RUN")
            self._ran = True
        reason = "requested"
        self._emit("ready", {"owner_id": self._owner_id})
        try:
            while not self._is_closed():
                wait_payload = self._wait_for_work()
                if wait_payload is None:
                    continue
                active = self._claim_or_reclaim(wait_payload)
                if active is None:
                    continue
                with self._condition:
                    if self._closed:
                        break
                    self._active = active
                self._emit(
                    "work",
                    {
                        "submission_handle": active.submission_handle,
                        "claim": {
                            "work_attempt": active.work_attempt,
                            "work": active.work,
                            "lease": active.lease,
                            "fence": active.fence,
                            "context": active.context,
                        },
                    },
                )
                self._serve_active(active)
            with self._condition:
                reason = self._close_reason
        except (HostSupervisorError, HostIpcError) as exc:
            reason = exc.code
            with self._condition:
                self._closed = True
                self._close_reason = reason
        except _StopSupervisor as exc:
            reason = exc.reason
            with self._condition:
                self._closed = True
                self._close_reason = reason
        finally:
            with self._condition:
                self._active = None
                self._submission = None
            self._emit("closed", {"reason": reason})
        return reason

    def _is_closed(self) -> bool:
        with self._condition:
            return self._closed

    def _now(self) -> float:
        try:
            raw = self._monotonic()
        except Exception as exc:
            raise HostSupervisorError("MONOTONIC_CLOCK_FAILED") from exc
        if type(raw) not in {int, float}:
            raise HostSupervisorError("MONOTONIC_CLOCK_INVALID")
        try:
            current = float(cast("int | float", raw))
        except (OverflowError, ValueError) as exc:
            raise HostSupervisorError("MONOTONIC_CLOCK_INVALID") from exc
        if not math.isfinite(current) or not 0 <= current <= float(2**53):
            raise HostSupervisorError("MONOTONIC_CLOCK_INVALID")
        if self._last_monotonic is not None and current < self._last_monotonic:
            raise HostSupervisorError("MONOTONIC_CLOCK_ROLLED_BACK")
        self._last_monotonic = current
        return current

    def _identifier(self, prefix: str) -> str:
        try:
            value = self._id_generator(prefix)
        except Exception as exc:
            raise HostSupervisorError("IDENTITY_GENERATION_FAILED") from exc
        if not is_protocol_id(value):
            raise HostSupervisorError("IDENTITY_GENERATION_FAILED")
        return value

    def _handle(self) -> str:
        try:
            value = self._secret_generator()
        except Exception as exc:
            raise HostSupervisorError("SECRET_GENERATION_FAILED") from exc
        if not is_protocol_id(value):
            raise HostSupervisorError("SECRET_GENERATION_FAILED")
        return value

    def _emit(self, kind: str, payload: dict[str, object]) -> None:
        self._stream_sequence += 1
        envelope = {
            "schema_version": SCHEMA_VERSION,
            "protocol_version": PROTOCOL_VERSION,
            "stream_sequence": self._stream_sequence,
            "type": kind,
            "session_id": self._session_id,
            **payload,
        }
        try:
            line = canonical_json_bytes(envelope) + b"\n"
            self._emit_line(line)
        except Exception as exc:
            raise HostSupervisorError("SUPERVISOR_OUTPUT_FAILED") from exc

    def _call(self, operation: str, payload: dict[str, object]) -> _ApiResponse:
        request = canonical_json_bytes(
            {
                "schema_version": SCHEMA_VERSION,
                "protocol_version": PROTOCOL_VERSION,
                "operation": operation,
                **payload,
            }
        )
        response = _load_object(self._client.call(request).body)
        ok = response.get("ok")
        common = {
            "schema_version",
            "protocol_version",
            "operation",
            "ok",
        }
        expected = common | ({"payload"} if ok is True else {"error"})
        if (
            type(ok) is not bool
            or frozenset(response) != frozenset(expected)
            or response["schema_version"] != SCHEMA_VERSION
            or response["protocol_version"] != PROTOCOL_VERSION
            or response["operation"] != operation
        ):
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        if ok is True:
            payload_value = response["payload"]
            if type(payload_value) is not dict:
                raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
            return _ApiResponse(True, cast(dict[str, object], payload_value), None)
        error = _required_mapping(response["error"], frozenset({"code"}))
        code = error["code"]
        if type(code) is not str or not is_protocol_id(code):
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        return _ApiResponse(False, None, code)

    def _wait_for_work(self) -> dict[str, object] | None:
        result = self._call(
            "wait",
            {
                "session_id": self._session_id,
                "timeout": self._wait_timeout,
            },
        )
        if not result.ok:
            raise _StopSupervisor(cast(str, result.error_code))
        payload = _required_mapping(
            result.payload,
            frozenset({"timed_out", "through_sequence", "work"}),
        )
        through_sequence = payload["through_sequence"]
        if (
            type(payload["timed_out"]) is not bool
            or type(through_sequence) is not int
            or through_sequence < 0
        ):
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        if payload["timed_out"] is True:
            if payload["work"] is not None:
                raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
            return None
        work = _required_mapping(
            payload["work"],
            frozenset(
                {
                    "session_id",
                    "work_id",
                    "kind",
                    "attempt",
                    "observed_sequence",
                }
            ),
        )
        if (
            work["session_id"] != self._session_id
            or not is_protocol_id(work["work_id"])
            or not is_protocol_id(work["kind"])
            or type(work["attempt"]) is not int
            or work["attempt"] < 1
            or type(work["observed_sequence"]) is not int
            or work["observed_sequence"] < 1
            or through_sequence < work["observed_sequence"]
        ):
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        return work

    def _claim_or_reclaim(
        self,
        waiting_work: dict[str, object],
    ) -> _ActiveClaim | None:
        work_id = cast(str, waiting_work["work_id"])
        attempt = cast(int, waiting_work["attempt"])
        claim_id = self._identifier("claim")
        claimed = self._call(
            "claim",
            {
                "session_id": self._session_id,
                "request_id": self._identifier("claim.request"),
                "claim_id": claim_id,
                "work_id": work_id,
                "owner_id": self._owner_id,
                "lease_seconds": self._lease_seconds,
                "max_tenure_seconds": self._max_tenure_seconds,
            },
        )
        if claimed.ok:
            active = self._active_claim(
                cast(dict[str, object], claimed.payload),
                attempt,
            )
            return self._register_active(active)
        if claimed.error_code == "WORK_ALREADY_LEASED":
            self._idle_wait()
            return None
        if claimed.error_code not in {
            "LEASE_RECLAIM_REQUIRED",
            "LEASE_EXPIRED",
            "LEASE_TENURE_EXPIRED",
        }:
            if claimed.error_code == "WORK_SUPERSEDED":
                return None
            raise _StopSupervisor(cast(str, claimed.error_code))
        reclaimed = self._call(
            "reclaim",
            {
                "session_id": self._session_id,
                "request_id": self._identifier("reclaim.request"),
                "claim_id": self._identifier("claim.reclaimed"),
                "work_id": work_id,
                "owner_id": self._owner_id,
                "expected_work_attempt": attempt,
                "lease_seconds": self._lease_seconds,
                "max_tenure_seconds": self._max_tenure_seconds,
                "occurred_at": self._occurred_at(),
                "actor_id": self._runtime_actor_id,
            },
        )
        if not reclaimed.ok:
            if reclaimed.error_code in {
                "LEASE_STILL_ACTIVE",
                "WORK_SUPERSEDED",
            }:
                self._idle_wait()
                return None
            raise _StopSupervisor(cast(str, reclaimed.error_code))
        payload = cast(dict[str, object], reclaimed.payload)
        disposition = payload.get("disposition")
        if disposition == "claimed":
            return self._register_active(self._active_claim(payload, attempt))
        if disposition not in {"requeued", "dead_lettered"}:
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        self._emit(
            "status",
            {
                "status": "work_advanced",
                "work_id": work_id,
                "disposition": disposition,
            },
        )
        return None

    def _active_claim(
        self,
        payload: dict[str, object],
        attempt: int,
    ) -> _ActiveClaim:
        allowed = frozenset({"work", "lease", "fence", "context"})
        allowed_reclaim = allowed | frozenset({"disposition", "work_attempt"})
        if frozenset(payload) not in {allowed, allowed_reclaim}:
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        if frozenset(payload) == allowed_reclaim and (
            payload.get("disposition") != "claimed"
            or type(payload.get("work_attempt")) is not int
            or payload.get("work_attempt") != attempt
        ):
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        work = _required_mapping(payload["work"], _WORK_KEYS)
        lease = _required_mapping(payload["lease"], _LEASE_KEYS)
        fence = _required_mapping(payload["fence"], _FENCE_KEYS)
        context = _required_mapping(payload["context"], _CONTEXT_KEYS)
        if (
            work.get("session_id") != self._session_id
            or not is_protocol_id(work.get("work_id"))
            or context.get("work_id") != work.get("work_id")
            or context.get("binding_digest") != work.get("binding_digest")
            or lease.get("work_id") != work.get("work_id")
            or lease.get("session_id") != self._session_id
            or lease.get("binding_digest") != work.get("binding_digest")
            or lease.get("owner_id") != self._owner_id
            or type(lease.get("lease_version")) is not int
            or cast(int, lease["lease_version"]) < 1
            or fence.get("work_id") != work.get("work_id")
            or fence.get("session_id") != self._session_id
            or fence.get("claim_id") != lease.get("claim_id")
            or fence.get("owner_id") != self._owner_id
            or fence.get("runtime_epoch") != lease.get("runtime_epoch")
            or fence.get("registry_generation")
            != lease.get("registry_generation")
            or fence.get("lease_version") != lease.get("lease_version")
        ):
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        return _ActiveClaim(
            attempt,
            dict(work),
            dict(lease),
            dict(fence),
            dict(context),
            self._handle(),
        )

    def _register_active(self, active: _ActiveClaim) -> _ActiveClaim:
        result = self._call(
            "register_submission",
            {
                "submission_handle": active.submission_handle,
                "work": active.work,
                "fence": active.fence,
            },
        )
        if not result.ok:
            raise _StopSupervisor(cast(str, result.error_code))
        payload = _required_mapping(
            result.payload,
            frozenset({"handle_digest", "work_id", "claim_id", "replayed"}),
        )
        if (
            not is_sha256_digest(payload["handle_digest"])
            or payload["work_id"] != active.work.get("work_id")
            or payload["claim_id"] != active.lease.get("claim_id")
            or type(payload["replayed"]) is not bool
        ):
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        return active

    def _serve_active(self, active: _ActiveClaim) -> None:
        renew_at = self._now() + self._lease_seconds / 2
        while not self._is_closed():
            submission: HostSupervisorSubmission | None = None
            with self._condition:
                if self._submission is not None:
                    submission = self._take_submission_locked()
                elif not self._closed:
                    remaining = max(0.0, renew_at - self._now())
                    self._wait_strategy(self._condition, remaining)
                    submission = self._take_submission_locked()
            if self._is_closed():
                return
            if submission is not None:
                if self._publish(active, submission):
                    self._clear_active(active)
                    return
            if self._now() >= renew_at:
                if not self._renew(active):
                    self._clear_active(active)
                    return
                renew_at = self._now() + self._lease_seconds / 2

    def _renew(self, active: _ActiveClaim) -> bool:
        result = self._call(
            "renew",
            {
                "session_id": self._session_id,
                "request_id": self._identifier("renew.request"),
                "claim_id": active.lease["claim_id"],
                "work_id": active.lease["work_id"],
                "owner_id": self._owner_id,
                "expected_lease_version": active.lease["lease_version"],
                "lease_seconds": self._lease_seconds,
            },
        )
        if not result.ok:
            self._emit(
                "status",
                {
                    "status": "work_lost",
                    "work_id": active.work["work_id"],
                    "code": result.error_code,
                },
            )
            return False
        payload = _required_mapping(
            result.payload,
            frozenset({"lease", "replayed"}),
        )
        if type(payload["replayed"]) is not bool:
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        lease = _required_mapping(payload["lease"], _LEASE_KEYS)
        version = lease.get("lease_version")
        previous_version = active.lease.get("lease_version")
        if (
            lease.get("session_id") != self._session_id
            or lease.get("work_id") != active.work.get("work_id")
            or lease.get("binding_digest") != active.work.get("binding_digest")
            or lease.get("claim_id") != active.lease.get("claim_id")
            or lease.get("owner_id") != self._owner_id
            or lease.get("runtime_epoch") != active.lease.get("runtime_epoch")
            or lease.get("registry_generation")
            != active.lease.get("registry_generation")
            or type(version) is not int
            or type(previous_version) is not int
            or version <= previous_version
        ):
            raise HostSupervisorError("HOST_SUPERVISOR_RESPONSE_INVALID")
        active.lease = dict(lease)
        active.fence = {**active.fence, "lease_version": version}
        return True

    def _take_submission_locked(self) -> HostSupervisorSubmission | None:
        submission = self._submission
        self._submission = None
        return submission

    def _publish(
        self,
        active: _ActiveClaim,
        submission: HostSupervisorSubmission,
    ) -> bool:
        result_tree = json.loads(encode_host_result(submission.result))
        result = self._call(
            "submit",
            {
                "submission_handle": active.submission_handle,
                "idempotency_key": submission.idempotency_key,
                "result": result_tree,
                "occurred_at": submission.occurred_at,
                "actor_id": submission.actor_id,
            },
        )
        if not result.ok:
            self._emit(
                "status",
                {
                    "status": "publish_rejected",
                    "work_id": active.work["work_id"],
                    "code": result.error_code,
                },
            )
            return result.error_code in {
                "WORK_SUPERSEDED",
                "SESSION_DEACTIVATED",
            }
        payload = cast(dict[str, object], result.payload)
        self._emit(
            "status",
            {
                "status": "published",
                "work_id": active.work["work_id"],
                "receipt": payload,
            },
        )
        return True

    def _clear_active(self, active: _ActiveClaim) -> None:
        with self._condition:
            if self._active is active:
                self._active = None
                self._submission = None

    def _idle_wait(self) -> None:
        with self._condition:
            if not self._closed:
                self._wait_strategy(self._condition, self._retry_interval)


__all__ = [
    "HostSupervisor",
    "HostSupervisorError",
    "HostSupervisorSubmission",
    "WaitStrategy",
]
