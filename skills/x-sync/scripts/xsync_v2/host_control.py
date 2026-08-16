"""Model-neutral Host control plane over durable work and lease services."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import math
import re
import threading
import time
from typing import Protocol, TypeAlias, cast

from .coordinator import DialogueCoordinator
from .domain import TriggerKind, WorkDeadLettered, WorkRequeued
from .event_codec import ActorKind, DialogueActor
from .event_store import DialogueCommitOutcome
from .host_context import (
    HostContextCapsule,
    HostContextError,
    HostContextSource,
    build_host_context,
)
from .host_work import (
    HostResultPublishRequest,
    HostWorkService,
    LeaseExhaustionRecordRequest,
    SubmissionHandlePublishRequest,
    SubmissionHandleRegisterRequest,
)
from .lease_store import (
    ClaimRequest,
    CurrentRunnableWork,
    CurrentWorkObservation,
    LeaseExhaustionProof,
    LeaseMutationOutcome,
    LeaseRecord,
    LeaseStore,
    PublishFence,
    ReclaimRequest,
    RenewRequest,
)
from .locking import DomainLockManager, RegistryLockMode, SessionLockAuthority
from .observers.work_wake import WorkWakeHint
from .work import RunnableWork
from .submission_store import SubmissionRegistrationOutcome


_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_DURABLE_POLL_SECONDS = 60.0
MonotonicClock = Callable[[], float]


def _finite_float(value: object) -> float | None:
    if type(value) not in {int, float}:
        return None
    try:
        converted = float(cast(int | float, value))
    except (OverflowError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


class HostControlError(RuntimeError):
    """A stable control-plane configuration or orchestration failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class HostContextProvider(Protocol):
    """Focused reader invoked only after a lease tenure is durable."""

    def load_context(
        self,
        work: RunnableWork,
        lease: LeaseRecord,
        authority: SessionLockAuthority,
    ) -> HostContextSource: ...


@dataclass(frozen=True, slots=True)
class HostClaimEnvelope:
    """One durable tenure plus its work-bound incremental context."""

    work: RunnableWork
    lease: LeaseRecord
    fence: PublishFence
    context: HostContextCapsule


@dataclass(frozen=True, slots=True)
class HostWorkMetadata:
    """Lightweight wait result which contains no model context."""

    session_id: str
    work_id: str
    kind: TriggerKind
    attempt: int
    observed_sequence: int


@dataclass(frozen=True, slots=True)
class HostWaitOutcome:
    """One durable wait observation; timeout is a normal typed outcome."""

    work: HostWorkMetadata | None
    through_sequence: int
    timed_out: bool


@dataclass(frozen=True, slots=True)
class HostReclaimRequest:
    """A reclaim mutation plus stable metadata for exhaustion resolution."""

    lease_request: ReclaimRequest
    occurred_at: str
    actor: DialogueActor


class HostWorkDisposition(StrEnum):
    """Closed set of canonical exhaustion resolutions."""

    REQUEUED = "requeued"
    DEAD_LETTERED = "dead_lettered"


@dataclass(frozen=True, slots=True)
class HostWorkAdvanced:
    """Canonical result of automatically resolving exhausted lease tenures."""

    work_id: str
    disposition: HostWorkDisposition
    conversation_version: int
    through_event_sequence: int
    replayed: bool


HostReclaimOutcome: TypeAlias = HostClaimEnvelope | HostWorkAdvanced


def _valid_timestamp(value: object) -> bool:
    if type(value) is not str or not value or value != value.strip():
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _publish_fence(lease: LeaseRecord) -> PublishFence:
    return PublishFence(
        lease.session_id,
        lease.claim_id,
        lease.work_id,
        lease.owner_id,
        lease.runtime_epoch,
        lease.lease_version,
        lease.registry_generation,
    )


class HostControl:
    """Wait, lease, contextualize, and publish without model-specific logic."""

    def __init__(
        self,
        coordinator: DialogueCoordinator,
        locks: DomainLockManager,
        leases: LeaseStore,
        work_service: HostWorkService,
        context_provider: HostContextProvider,
        *,
        monotonic_clock: MonotonicClock = time.monotonic,
        durable_poll_interval: float | int = 1.0,
    ) -> None:
        try:
            load_context = context_provider.load_context
        except Exception as exc:
            raise HostControlError("INVALID_HOST_CONTROL_CONFIGURATION") from exc
        poll_interval = _finite_float(durable_poll_interval)
        if (
            type(coordinator) is not DialogueCoordinator
            or type(locks) is not DomainLockManager
            or type(leases) is not LeaseStore
            or type(work_service) is not HostWorkService
            or leases._locks is not locks
            or work_service._leases is not leases
            or work_service._coordinator is not coordinator
            or not callable(load_context)
            or not callable(monotonic_clock)
            or poll_interval is None
            or not 0 < poll_interval <= _MAX_DURABLE_POLL_SECONDS
        ):
            raise HostControlError("INVALID_HOST_CONTROL_CONFIGURATION")
        self._coordinator = coordinator
        self._locks = locks
        self._leases = leases
        self._work_service = work_service
        self._load_context = load_context
        self._monotonic = monotonic_clock
        self._durable_poll_interval = poll_interval
        self._condition = threading.Condition()
        self._wake_generation = 0
        self._clock_lock = threading.Lock()
        self._last_monotonic: float | None = None

    def notify(self, hint: WorkWakeHint) -> None:
        """Consume a lossy wake hint; durable work remains authoritative."""
        if type(hint) is not WorkWakeHint:
            raise HostControlError("INVALID_WORK_WAKE_HINT")
        with self._condition:
            self._wake_generation += 1
            self._condition.notify_all()

    @staticmethod
    def _metadata(current: CurrentRunnableWork) -> HostWorkMetadata:
        work = current.work
        return HostWorkMetadata(
            work.session_id,
            work.work_id,
            work.kind,
            current.attempt,
            work.observed_sequence,
        )

    def _observe(self, session_id: str) -> CurrentWorkObservation:
        with self._locks.semantic_session(
            session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            return self._leases.observe_current_work(session_id, authority)

    def current(self, session_id: str) -> HostWorkMetadata | None:
        """Return lightweight current work under registry -> Session locks."""
        current = self._observe(session_id).current
        return None if current is None else self._metadata(current)

    def wait(
        self,
        session_id: str,
        *,
        timeout: float | int | None = None,
    ) -> HostWaitOutcome:
        """Wait for work with durable-before-sleep and durable-after-wake reads.

        The wake generation is sampled before each durable read.  A hint that
        races that read changes the generation, so the method rechecks without
        sleeping.  Timeouts are typed outcomes, not failures.
        """
        timeout_value: float | None = None
        if timeout is not None:
            timeout_value = _finite_float(timeout)
            if timeout_value is None or timeout_value < 0:
                raise HostControlError("INVALID_WAIT_TIMEOUT")
        now = self._monotonic_now()
        deadline = None if timeout_value is None else now + timeout_value
        if deadline is not None and not math.isfinite(deadline):
            raise HostControlError("INVALID_WAIT_TIMEOUT")
        with self._condition:
            observed_generation = self._wake_generation
        through_sequence = 0
        while True:
            observation = self._observe(session_id)
            through_sequence = observation.through_sequence
            if observation.current is not None:
                return HostWaitOutcome(
                    self._metadata(observation.current),
                    through_sequence,
                    False,
                )
            with self._condition:
                if self._wake_generation != observed_generation:
                    observed_generation = self._wake_generation
                    continue
                remaining: float | None = None
                if deadline is not None:
                    remaining = deadline - self._monotonic_now()
                    if remaining <= 0:
                        return HostWaitOutcome(None, through_sequence, True)
                self._condition.wait(
                    self._durable_poll_interval
                    if remaining is None
                    else min(remaining, self._durable_poll_interval)
                )
                observed_generation = self._wake_generation

    def _monotonic_now(self) -> float:
        with self._clock_lock:
            try:
                value = self._monotonic()
            except Exception as exc:
                raise HostControlError("MONOTONIC_CLOCK_FAILED") from exc
            current = _finite_float(value)
            if current is None:
                raise HostControlError("MONOTONIC_CLOCK_INVALID")
            if self._last_monotonic is not None and current < self._last_monotonic:
                raise HostControlError("MONOTONIC_CLOCK_ROLLED_BACK")
            self._last_monotonic = current
            return current

    def _contextualize(
        self,
        work: RunnableWork,
        lease: LeaseRecord,
        authority: SessionLockAuthority,
    ) -> HostClaimEnvelope:
        try:
            source = self._load_context(work, lease, authority)
        except HostContextError:
            raise
        except Exception as exc:
            raise HostControlError("HOST_CONTEXT_PROVIDER_FAILED") from exc
        context = build_host_context(work, source)
        return HostClaimEnvelope(work, lease, _publish_fence(lease), context)

    def claim(self, request: ClaimRequest) -> HostClaimEnvelope:
        """Claim current work, then and only then generate its typed context."""
        if type(request) is not ClaimRequest:
            raise HostControlError("INVALID_CLAIM_REQUEST")
        with self._locks.semantic_session(
            request.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            current = self._leases.current_runnable(request.session_id, authority)
            work = None if current is None else current.work
            outcome = self._leases.claim(request, authority)
            if work is None or work.work_id != outcome.lease.work_id:
                raise HostControlError("WORK_SUPERSEDED")
            return self._contextualize(work, outcome.lease, authority)

    def renew(self, request: RenewRequest) -> LeaseMutationOutcome:
        """Delegate one idempotent lease renewal under semantic authority."""
        if type(request) is not RenewRequest:
            raise HostControlError("INVALID_RENEW_REQUEST")
        with self._locks.semantic_session(
            request.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            return self._leases.renew(request, authority)

    @staticmethod
    def _validate_reclaim_request(request: object) -> HostReclaimRequest:
        if (
            type(request) is not HostReclaimRequest
            or type(request.lease_request) is not ReclaimRequest
            or not _valid_timestamp(request.occurred_at)
            or type(request.actor) is not DialogueActor
            or request.actor.kind is not ActorKind.RUNTIME
            or _ID_PATTERN.fullmatch(request.actor.actor_id) is None
        ):
            raise HostControlError("INVALID_HOST_RECLAIM_REQUEST")
        return request

    def reclaim(self, request: HostReclaimRequest) -> HostReclaimOutcome:
        """Reclaim work or resolve exhaustion canonically after releasing locks."""
        request = self._validate_reclaim_request(request)
        lease_request = request.lease_request
        with self._locks.semantic_session(
            lease_request.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            current = self._leases.current_runnable(
                lease_request.session_id,
                authority,
            )
            work = None if current is None else current.work
            outcome = self._leases.reclaim(lease_request, authority)
            if type(outcome) is LeaseMutationOutcome:
                if work is None or work.work_id != outcome.lease.work_id:
                    raise HostControlError("WORK_SUPERSEDED")
                return self._contextualize(work, outcome.lease, authority)
            if type(outcome) is not LeaseExhaustionProof:
                raise HostControlError("LEASE_RECLAIM_OUTCOME_INVALID")
            proof = outcome
        return self._record_exhaustion(request, proof)

    def _record_exhaustion(
        self,
        request: HostReclaimRequest,
        proof: LeaseExhaustionProof,
    ) -> HostWorkAdvanced:
        committed = self._work_service.record_lease_exhaustion(
            LeaseExhaustionRecordRequest(
                proof,
                request.occurred_at,
                request.actor,
            )
        )
        return self._advanced(proof.work_id, committed)

    @staticmethod
    def _advanced(
        work_id: str,
        outcome: DialogueCommitOutcome,
    ) -> HostWorkAdvanced:
        terminal = outcome.events[-1].payload if outcome.events else None
        if type(terminal) is WorkRequeued:
            disposition = HostWorkDisposition.REQUEUED
        elif type(terminal) is WorkDeadLettered:
            disposition = HostWorkDisposition.DEAD_LETTERED
        else:
            raise HostControlError("LEASE_EXHAUSTION_OUTCOME_INVALID")
        return HostWorkAdvanced(
            work_id,
            disposition,
            outcome.state.conversation_version,
            outcome.state.sequence,
            outcome.replayed,
        )

    def publish_result(
        self,
        request: HostResultPublishRequest,
    ) -> DialogueCommitOutcome:
        """Publish the shared strict Host result through trusted derivation."""
        return self._work_service.publish_result(request)

    def register_submission(
        self,
        request: SubmissionHandleRegisterRequest,
    ) -> SubmissionRegistrationOutcome:
        """Register one opaque submission handle for the current claim."""
        return self._work_service.register_submission(request)

    def submit_result(
        self,
        request: SubmissionHandlePublishRequest,
    ) -> DialogueCommitOutcome:
        """Publish a strict Host result by its opaque submission handle."""
        return self._work_service.submit_result(request)
