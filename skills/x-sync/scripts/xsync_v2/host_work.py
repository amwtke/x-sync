"""Lease-fenced application service for publishing canonical Host work.

The Host may prepare result data while a lease is held, but the dialogue
transaction marker is the only semantic commit point.  This service keeps the
registry -> Session authority live through that point and installs an exact
evidence plus lease guard immediately before marker publication.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import re
from typing import TypeAlias

from .coordinator import (
    CoordinatorError,
    DialogueCoordinator,
    DialogueExecutionRequest,
    _registration,
    _stable_id,
    _valid_timestamp,
)
from .dispatch import CommittedFactCollector
from .domain import (
    Accepted,
    CommitAgentTurn,
    CommittedDialogueEvent,
    CurrentWorkState,
    DecisionContext,
    DialogueState,
    LearnerTurnSubmitted,
    PresentCandidates,
    Rejected,
    ReportWorkFailure,
    SessionStarted,
    StartTopic,
    TopicResumed,
    TopicSelectionSubmitted,
    TopicSwitchRequested,
    TopicStarted,
    TriggerBinding,
    TriggerKind,
    WorkFailure,
    WorkFailureCategory,
    WorkRecoveryRequested,
    WorkRequeued,
    WorkStatus,
)
from .event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    ActorKind,
    DialogueActor,
    DialogueWriteRequestRecord,
    canonical_json_bytes,
    dialogue_request_digest,
    sha256_digest,
)
from .event_store import (
    DialogueCommitOutcome,
    DialogueCommitRequest,
    DialogueStoreError,
    EventMetadata,
    _DialogueTransactionLog,
)
from .host_result import (
    DialogueTurnResult,
    HostResult,
    HostResultError,
    TopicCandidatesResult,
    TopicStartedResult,
    WorkFailureResult,
    encode_host_result,
    host_command_id,
    host_result_command,
)
from .lease_store import (
    AuthoritativeWorkSnapshot,
    LeaseExhaustionProof,
    LeaseStore,
    LeaseStoreError,
    PublishFence,
    _exhaustion_proof_digest,
    _exhaustion_proof_tree,
    _work_tree,
)
from .locking import LockError, SessionLockAuthority
from .registry import DialogueRegistrationStatus, RegistryState
from .registry_store import RegistryStoreError
from .secure_fs import SecureFsError
from .state_machine import (
    MAX_AUTOMATIC_WORK_ATTEMPTS,
    PUBLISHABLE_EVIDENCE,
    RETRYABLE_WORK_FAILURES,
    decide,
)
from .work import (
    RunnableWork,
    WorkError,
    WorkOrigin,
    derive_runnable_work,
    validate_runnable_work,
)


_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")

HostWorkCommand: TypeAlias = (
    PresentCandidates | StartTopic | CommitAgentTurn | ReportWorkFailure
)
_HOST_RESULT_TYPES = frozenset(
    {
        TopicCandidatesResult,
        TopicStartedResult,
        DialogueTurnResult,
        WorkFailureResult,
    }
)


class HostWorkServiceError(RuntimeError):
    """A stable, path-free Host publish failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class HostWorkPublishRequest:
    """One idempotent Host result bound to its immutable claimed work."""

    idempotency_key: str
    work: RunnableWork
    command: HostWorkCommand
    context: DecisionContext
    occurred_at: str
    actor: DialogueActor
    fence: PublishFence


@dataclass(frozen=True, slots=True)
class HostResultPublishRequest:
    """One strict Host result before trusted command/context derivation."""

    idempotency_key: str
    work: RunnableWork
    result: HostResult
    occurred_at: str
    actor: DialogueActor
    fence: PublishFence


@dataclass(frozen=True, slots=True)
class LeaseExhaustionRecordRequest:
    """Runtime request to canonically resolve one exhausted lease history."""

    proof: LeaseExhaustionProof
    occurred_at: str
    actor: DialogueActor


def _current_queued_work(state: DialogueState) -> CurrentWorkState:
    candidates = tuple(
        item
        for item in (
            state.session_work,
            state.active_topic.work if state.active_topic is not None else None,
        )
        if item is not None and item.status is WorkStatus.QUEUED
    )
    if len(candidates) != 1:
        raise HostWorkServiceError("NO_RUNNABLE_WORK")
    return candidates[0]


def _created_trigger(event: CommittedDialogueEvent) -> TriggerBinding | None:
    payload = event.payload
    if type(payload) is SessionStarted:
        return payload.candidate_trigger
    if type(payload) is TopicSelectionSubmitted:
        return payload.next_trigger
    if type(payload) is TopicSwitchRequested:
        return payload.candidate_trigger
    if type(payload) is TopicStarted:
        return payload.initial_trigger
    if type(payload) is LearnerTurnSubmitted:
        return payload.next_trigger
    if type(payload) is TopicResumed:
        return payload.resumed_trigger
    if type(payload) is WorkRequeued:
        return payload.next_trigger
    if type(payload) is WorkRecoveryRequested:
        return payload.next_trigger
    return None


def _work_origin(
    state: DialogueState,
    events: tuple[CommittedDialogueEvent, ...],
) -> WorkOrigin | None:
    """Prove the current queued work against its committed creator event."""
    try:
        current = _current_queued_work(state)
    except HostWorkServiceError as exc:
        if exc.code == "NO_RUNNABLE_WORK":
            return None
        raise
    matches = tuple(
        event for event in events if event.sequence == current.trigger_event_sequence
    )
    if len(matches) != 1:
        raise HostWorkServiceError("AUTHORITATIVE_WORK_INVALID")
    event = matches[0]
    trigger = _created_trigger(event)
    if event.event_id != current.trigger_event_id or trigger != current.trigger:
        raise HostWorkServiceError("AUTHORITATIVE_WORK_INVALID")
    return WorkOrigin(event.event_id, event.sequence, current.trigger)


def authoritative_work_snapshot(
    coordinator: DialogueCoordinator,
    session_id: str,
    authority: SessionLockAuthority,
) -> AuthoritativeWorkSnapshot:
    """Read a current work snapshot without reacquiring or repairing locks.

    Runtime wiring may inject this function into ``LeaseStore``.  The caller
    must hold an exclusive registry -> Session authority; the dialogue is
    opened read-only so a pre-marker fence cannot quarantine staged events.
    """
    if (
        type(coordinator) is not DialogueCoordinator
        or type(authority) is not SessionLockAuthority
        or authority.session_id != session_id
    ):
        raise HostWorkServiceError("LOCK_AUTHORITY_REQUIRED")
    registry_log = coordinator._open_registry(authority.registry)
    try:
        registry_state = registry_log.tip().state
        registration = _registration(registry_state, session_id)
        if (
            registration is None
            or type(registration.activated_generation) is not int
            or registration.activated_generation < 1
        ):
            raise HostWorkServiceError("SESSION_DEACTIVATED")
        generation = registration.activated_generation
        dialogue_log = coordinator._open_existing_dialogue(
            session_id,
            generation,
            authority,
        )
        try:
            state = dialogue_log.tip().state
            events = dialogue_log.read_committed(after_sequence=0)
            origin = _work_origin(state, events)
            return AuthoritativeWorkSnapshot(state, registry_state, origin)
        finally:
            dialogue_log.close()
    finally:
        registry_log.close()


def _stable_fence_tree(fence: PublishFence) -> dict[str, object]:
    return {
        "session_id": fence.session_id,
        "claim_id": fence.claim_id,
        "work_id": fence.work_id,
        "owner_id": fence.owner_id,
        "runtime_epoch": fence.runtime_epoch,
        "registry_generation": fence.registry_generation,
    }


def _request_digest(request: HostWorkPublishRequest) -> str:
    dialogue_digest = dialogue_request_digest(
        DialogueWriteRequestRecord(
            SCHEMA_VERSION,
            "dialogue_write_request",
            request.work.session_id,
            request.work.registry_generation,
            request.work.conversation_version,
            request.command,
            request.context,
        )
    )
    return sha256_digest(
        canonical_json_bytes(
            {
                "schema_version": SCHEMA_VERSION,
                "record_type": "host_work_publish_request",
                "protocol_version": PROTOCOL_VERSION,
                "idempotency_key": request.idempotency_key,
                "work": _work_tree(request.work),
                "stable_fence": _stable_fence_tree(request.fence),
                "dialogue_request_digest": dialogue_digest,
            }
        )
    )


def _result_request_digest(request: HostResultPublishRequest) -> str:
    result_digest = sha256_digest(encode_host_result(request.result))
    return sha256_digest(
        canonical_json_bytes(
            {
                "schema_version": SCHEMA_VERSION,
                "record_type": "host_result_publish_request",
                "protocol_version": PROTOCOL_VERSION,
                "idempotency_key": request.idempotency_key,
                "work": _work_tree(request.work),
                "stable_fence": _stable_fence_tree(request.fence),
                "result_digest": result_digest,
            }
        )
    )


def _lease_exhaustion_request_digest(
    request: LeaseExhaustionRecordRequest,
) -> str:
    return sha256_digest(
        canonical_json_bytes(
            {
                "schema_version": SCHEMA_VERSION,
                "record_type": "lease_exhaustion_record_request",
                "protocol_version": PROTOCOL_VERSION,
                "proof": _exhaustion_proof_tree(request.proof),
                "occurred_at": request.occurred_at,
                "actor": {
                    "kind": request.actor.kind.value,
                    "actor_id": request.actor.actor_id,
                },
            }
        )
    )


def _lease_exhaustion_command_id(proof: LeaseExhaustionProof) -> str:
    identity_digest = sha256_digest(
        canonical_json_bytes(
            {
                "protocol_version": PROTOCOL_VERSION,
                "record_type": "lease_exhaustion_command_identity",
                "session_id": proof.session_id,
                "work_id": proof.work_id,
                "reclaim_request_id": proof.reclaim_request_id,
            }
        )
    )
    return _stable_id("lease.exhausted", identity_digest)


def _lease_exhaustion_failure_id(proof: LeaseExhaustionProof) -> str:
    return _stable_id("failure.lease", _exhaustion_proof_digest(proof))


class HostWorkService:
    """Publish Host results through one receipt-first lease-fenced marker."""

    def __init__(
        self,
        coordinator: DialogueCoordinator,
        leases: LeaseStore,
    ) -> None:
        if (
            type(coordinator) is not DialogueCoordinator
            or type(leases) is not LeaseStore
            or leases._locks is not coordinator._locks
            or leases._dialogues is not coordinator._dialogues
        ):
            raise HostWorkServiceError("INVALID_HOST_WORK_CONFIGURATION")
        self._coordinator = coordinator
        self._leases = leases

    def publish(
        self,
        request: HostWorkPublishRequest,
    ) -> DialogueCommitOutcome:
        """Publish or exactly replay one Host result after all durable guards."""
        request, request_digest = self._validate_request(request)
        collector = self._coordinator._start_operation()
        try:
            return self._publish(request, request_digest, collector)
        finally:
            self._coordinator._finish_operation(collector)

    def publish_result(
        self,
        request: HostResultPublishRequest,
    ) -> DialogueCommitOutcome:
        """Publish a strict Host result with receipt lookup before derivation."""
        request, request_digest = self._validate_result_request(request)
        collector = self._coordinator._start_operation()
        try:
            return self._publish_result(request, request_digest, collector)
        finally:
            self._coordinator._finish_operation(collector)

    def record_lease_exhaustion(
        self,
        request: LeaseExhaustionRecordRequest,
    ) -> DialogueCommitOutcome:
        """Record an exact Runtime exhaustion proof through one marker."""
        request, request_digest = self._validate_exhaustion_request(request)
        collector = self._coordinator._start_operation()
        try:
            return self._record_lease_exhaustion(
                request,
                request_digest,
                collector,
            )
        finally:
            self._coordinator._finish_operation(collector)

    @staticmethod
    def _validate_exhaustion_request(
        request: object,
    ) -> tuple[LeaseExhaustionRecordRequest, str]:
        if (
            type(request) is not LeaseExhaustionRecordRequest
            or type(request.proof) is not LeaseExhaustionProof
            or type(request.actor) is not DialogueActor
            or request.actor.kind is not ActorKind.RUNTIME
            or _ID_PATTERN.fullmatch(request.actor.actor_id) is None
            or not _valid_timestamp(request.occurred_at)
        ):
            raise HostWorkServiceError("INVALID_LEASE_EXHAUSTION_REQUEST")
        try:
            digest = _lease_exhaustion_request_digest(request)
        except (CoordinatorError, LeaseStoreError, ValueError) as exc:
            code = getattr(exc, "code", "INVALID_LEASE_EXHAUSTION_REQUEST")
            raise HostWorkServiceError(code) from exc
        return request, digest

    def _validate_request(
        self,
        request: object,
    ) -> tuple[HostWorkPublishRequest, str]:
        if (
            type(request) is not HostWorkPublishRequest
            or type(request.command)
            not in {
                PresentCandidates,
                StartTopic,
                CommitAgentTurn,
                ReportWorkFailure,
            }
            or type(request.work) is not RunnableWork
            or type(request.context) is not DecisionContext
            or type(request.actor) is not DialogueActor
            or request.actor.kind is not ActorKind.HOST
            or type(request.fence) is not PublishFence
        ):
            raise HostWorkServiceError("INVALID_HOST_WORK_REQUEST")
        expected_command_id = host_command_id(request.idempotency_key)
        if request.command.command_id != expected_command_id:
            raise HostWorkServiceError("INVALID_HOST_COMMAND_ID")
        if type(request.command) is ReportWorkFailure:
            if (
                request.command.work_id != request.work.work_id
                or request.context.trigger is not None
            ):
                raise HostWorkServiceError("INVALID_HOST_WORK_REQUEST")
        try:
            validate_runnable_work(request.work)
            self._coordinator._validate_execution(
                DialogueExecutionRequest(
                    request.work.session_id,
                    request.work.conversation_version,
                    request.command,
                    request.context,
                    request.occurred_at,
                    request.actor,
                )
            )
            digest = _request_digest(request)
        except HostWorkServiceError:
            raise
        except (CoordinatorError, LeaseStoreError, ValueError, WorkError) as exc:
            code = getattr(exc, "code", "INVALID_HOST_WORK_REQUEST")
            raise HostWorkServiceError(code) from exc
        return request, digest

    @staticmethod
    def _validate_result_request(
        request: object,
    ) -> tuple[HostResultPublishRequest, str]:
        if (
            type(request) is not HostResultPublishRequest
            or type(request.result) not in _HOST_RESULT_TYPES
            or type(request.work) is not RunnableWork
            or type(request.actor) is not DialogueActor
            or request.actor.kind is not ActorKind.HOST
            or type(request.fence) is not PublishFence
            or not _valid_timestamp(request.occurred_at)
            or _ID_PATTERN.fullmatch(request.actor.actor_id) is None
        ):
            raise HostWorkServiceError("INVALID_HOST_RESULT_REQUEST")
        try:
            host_command_id(request.idempotency_key)
            validate_runnable_work(request.work)
            digest = _result_request_digest(request)
        except (HostResultError, LeaseStoreError, ValueError, WorkError) as exc:
            code = getattr(exc, "code", "INVALID_HOST_RESULT_REQUEST")
            raise HostWorkServiceError(code) from exc
        return request, digest

    def _publish(
        self,
        request: HostWorkPublishRequest,
        request_digest: str,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome:
        coordinator = self._coordinator
        try:
            with coordinator._locks.registry_exclusive() as registry_authority:
                registry_log = coordinator._open_registry(registry_authority)
                try:
                    registry_state = registry_log.tip().state
                    registration = _registration(
                        registry_state,
                        request.work.session_id,
                    )
                    if registration is None:
                        raise HostWorkServiceError("SESSION_DEACTIVATED")
                    if (
                        type(registration.activated_generation) is not int
                        or registration.activated_generation < 1
                    ):
                        raise HostWorkServiceError("SESSION_DEACTIVATED")
                    with coordinator._locks.session_exclusive(
                        request.work.session_id,
                        registry_authority,
                    ) as session_authority:
                        dialogue_log = coordinator._open_existing_dialogue(
                            request.work.session_id,
                            registration.activated_generation,
                            session_authority,
                        )
                        try:
                            replay = self._replay_receipt(
                                dialogue_log,
                                session_authority,
                                request,
                                request_digest,
                                collector,
                            )
                            if replay is not None:
                                return replay
                            return self._publish_new(
                                dialogue_log,
                                session_authority,
                                registry_state,
                                registration.config_digest,
                                request,
                                request_digest,
                                collector,
                            )
                        finally:
                            dialogue_log.close()
                finally:
                    registry_log.close()
        except HostWorkServiceError:
            raise
        except (
            CoordinatorError,
            DialogueStoreError,
            LeaseStoreError,
            RegistryStoreError,
            SecureFsError,
            LockError,
            WorkError,
            ValueError,
        ) as exc:
            code = getattr(exc, "code", "HOST_WORK_PUBLISH_FAILED")
            raise HostWorkServiceError(code) from exc

    def _publish_result(
        self,
        request: HostResultPublishRequest,
        request_digest: str,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome:
        coordinator = self._coordinator
        command_id = host_command_id(request.idempotency_key)
        try:
            with coordinator._locks.registry_exclusive() as registry_authority:
                registry_log = coordinator._open_registry(registry_authority)
                try:
                    registry_state = registry_log.tip().state
                    registration = _registration(
                        registry_state,
                        request.work.session_id,
                    )
                    if (
                        registration is None
                        or type(registration.activated_generation) is not int
                        or registration.activated_generation < 1
                    ):
                        raise HostWorkServiceError("SESSION_DEACTIVATED")
                    with coordinator._locks.session_exclusive(
                        request.work.session_id,
                        registry_authority,
                    ) as session_authority:
                        dialogue_log = coordinator._open_existing_dialogue(
                            request.work.session_id,
                            registration.activated_generation,
                            session_authority,
                        )
                        try:
                            replay = self._replay_command_receipt(
                                dialogue_log,
                                session_authority,
                                session_id=request.work.session_id,
                                registry_generation=(
                                    request.work.registry_generation
                                ),
                                command_id=command_id,
                                request_digest=request_digest,
                                occurred_at=request.occurred_at,
                                actor=request.actor,
                                collector=collector,
                            )
                            if replay is not None:
                                return replay
                            prepared = self._prepare_result_request(
                                dialogue_log,
                                registry_state,
                                registration.config_digest,
                                request,
                                request_digest,
                            )
                            return self._publish_new(
                                dialogue_log,
                                session_authority,
                                registry_state,
                                registration.config_digest,
                                prepared,
                                request_digest,
                                collector,
                            )
                        finally:
                            dialogue_log.close()
                finally:
                    registry_log.close()
        except HostWorkServiceError:
            raise
        except (
            CoordinatorError,
            DialogueStoreError,
            HostResultError,
            LeaseStoreError,
            RegistryStoreError,
            SecureFsError,
            LockError,
            WorkError,
            ValueError,
        ) as exc:
            code = getattr(exc, "code", "HOST_RESULT_PUBLISH_FAILED")
            raise HostWorkServiceError(code) from exc

    def _prepare_result_request(
        self,
        log: _DialogueTransactionLog,
        registry_state: RegistryState,
        config_digest: str,
        request: HostResultPublishRequest,
        request_digest: str,
    ) -> HostWorkPublishRequest:
        tip = log.tip()
        events = log.read_committed(after_sequence=0)
        origin = _work_origin(tip.state, events)
        authoritative = derive_runnable_work(tip.state, origin)
        if (
            authoritative is None
            or authoritative != request.work
            or registry_state.current_session_id != request.work.session_id
            or registry_state.pending_handoff is not None
            or registry_state.generation != request.work.registry_generation
        ):
            raise HostWorkServiceError("WORK_SUPERSEDED")
        current = _current_queued_work(tip.state)
        config = self._coordinator._load_config_checked(
            request.work.session_id,
            config_digest,
        )
        evidence = self._coordinator._verify_evidence(config)
        command = host_result_command(
            request.result,
            idempotency_key=request.idempotency_key,
            work=request.work,
            selected_candidate=tip.state.selected_candidate,
        )
        trigger: TriggerBinding | None = current.trigger
        if type(command) is ReportWorkFailure:
            trigger = None
        elif type(command) is StartTopic:
            input_digest = sha256_digest(
                canonical_json_bytes(
                    {
                        "record_type": "topic_initial_turn_input",
                        "source_work_binding_digest": request.work.binding_digest,
                        "contract_digest": command.contract.contract_digest,
                        "evidence_digest": evidence.evidence_digest,
                        "host_result_request_digest": request_digest,
                    }
                )
            )
            trigger = TriggerBinding(
                (
                    TriggerKind.INITIAL_TURN
                    if evidence.health in PUBLISHABLE_EVIDENCE
                    else TriggerKind.REGROUND
                ),
                _stable_id("work.initial", request_digest),
                config.runtime_epoch,
                None,
                command.contract.contract_digest,
                input_digest,
                evidence.evidence_digest,
            )
        prepared = HostWorkPublishRequest(
            request.idempotency_key,
            request.work,
            command,
            DecisionContext(registry_state.generation, trigger, evidence),
            request.occurred_at,
            request.actor,
            request.fence,
        )
        self._validate_request(prepared)
        return prepared

    def _record_lease_exhaustion(
        self,
        request: LeaseExhaustionRecordRequest,
        request_digest: str,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome:
        coordinator = self._coordinator
        session_id = request.proof.session_id
        command_id = _lease_exhaustion_command_id(request.proof)
        try:
            with coordinator._locks.registry_exclusive() as registry_authority:
                registry_log = coordinator._open_registry(registry_authority)
                try:
                    registry_state = registry_log.tip().state
                    registration = _registration(registry_state, session_id)
                    if (
                        registration is None
                        or type(registration.activated_generation) is not int
                        or registration.activated_generation < 1
                    ):
                        raise HostWorkServiceError("SESSION_DEACTIVATED")
                    generation = registration.activated_generation
                    with coordinator._locks.session_exclusive(
                        session_id,
                        registry_authority,
                    ) as session_authority:
                        dialogue_log = coordinator._open_existing_dialogue(
                            session_id,
                            generation,
                            session_authority,
                        )
                        try:
                            replay = self._replay_command_receipt(
                                dialogue_log,
                                session_authority,
                                session_id=session_id,
                                registry_generation=generation,
                                command_id=command_id,
                                request_digest=request_digest,
                                occurred_at=request.occurred_at,
                                actor=request.actor,
                                collector=collector,
                            )
                            if replay is not None:
                                return replay
                            return self._record_new_lease_exhaustion(
                                dialogue_log,
                                session_authority,
                                registry_state,
                                registration.config_digest,
                                request,
                                request_digest,
                                collector,
                            )
                        finally:
                            dialogue_log.close()
                finally:
                    registry_log.close()
        except HostWorkServiceError:
            raise
        except (
            CoordinatorError,
            DialogueStoreError,
            LeaseStoreError,
            RegistryStoreError,
            SecureFsError,
            LockError,
            WorkError,
            ValueError,
        ) as exc:
            code = getattr(exc, "code", "LEASE_EXHAUSTION_RECORD_FAILED")
            raise HostWorkServiceError(code) from exc

    def _replay_receipt(
        self,
        log: _DialogueTransactionLog,
        authority: SessionLockAuthority,
        request: HostWorkPublishRequest,
        request_digest: str,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome | None:
        return self._replay_command_receipt(
            log,
            authority,
            session_id=request.work.session_id,
            registry_generation=request.work.registry_generation,
            command_id=request.command.command_id,
            request_digest=request_digest,
            occurred_at=request.occurred_at,
            actor=request.actor,
            collector=collector,
        )

    def _replay_command_receipt(
        self,
        log: _DialogueTransactionLog,
        authority: SessionLockAuthority,
        *,
        session_id: str,
        registry_generation: int,
        command_id: str,
        request_digest: str,
        occurred_at: str,
        actor: DialogueActor,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome | None:
        tip = log.tip()
        existing = next(
            (
                receipt
                for receipt in tip.receipts
                if receipt.command_id == command_id
            ),
            None,
        )
        if existing is None:
            return None
        if existing.request_digest != request_digest:
            raise HostWorkServiceError("IDEMPOTENCY_CONFLICT")
        events = tuple(
            event
            for event in log.read_committed(after_sequence=existing.from_sequence - 1)
            if event.sequence <= existing.to_sequence
        )
        commit_request = DialogueCommitRequest(
            session_id,
            _stable_id("dlg.tx", request_digest),
            request_digest,
            registry_generation,
            events,
            tuple(
                EventMetadata(
                    occurred_at,
                    actor,
                    command_id,
                )
                for _ in events
            ),
        )
        return self._coordinator._commit_dialogue_request(
            log,
            commit_request,
            authority,
            collector,
        )

    def _record_new_lease_exhaustion(
        self,
        log: _DialogueTransactionLog,
        authority: SessionLockAuthority,
        registry_state: RegistryState,
        config_digest: str,
        request: LeaseExhaustionRecordRequest,
        request_digest: str,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome:
        proof = request.proof
        registration = _registration(registry_state, proof.session_id)
        if (
            registry_state.current_session_id != proof.session_id
            or registry_state.pending_handoff is not None
            or registration is None
            or registration.status is not DialogueRegistrationStatus.ACTIVE
            or registration.activated_generation != registry_state.generation
        ):
            raise HostWorkServiceError("SESSION_DEACTIVATED")
        config = self._coordinator._load_config_checked(
            proof.session_id,
            config_digest,
        )
        tip = log.tip()
        events = log.read_committed(after_sequence=0)
        origin = _work_origin(tip.state, events)
        try:
            authoritative_work = derive_runnable_work(tip.state, origin)
        except WorkError as exc:
            raise HostWorkServiceError(exc.code) from exc
        if (
            authoritative_work is None
            or authoritative_work.work_id != proof.work_id
            or authoritative_work.binding_digest != proof.binding_digest
            or authoritative_work.registry_generation != registry_state.generation
        ):
            raise HostWorkServiceError("WORK_SUPERSEDED")
        current = _current_queued_work(tip.state)
        if current.work_id != proof.work_id or current.attempt != proof.work_attempt:
            raise HostWorkServiceError("WORK_SUPERSEDED")
        verified_evidence = self._coordinator._verify_evidence(config)
        if verified_evidence.evidence_digest != current.trigger.evidence_digest:
            raise HostWorkServiceError("EVIDENCE_CHECK_CONFLICT")
        exhaustion_marker_guard = self._leases._exhaustion_marker_guard(
            proof,
            authority,
        )
        proof_digest = _exhaustion_proof_digest(proof)
        command_id = _lease_exhaustion_command_id(proof)
        failure = WorkFailure(
            failure_id=_lease_exhaustion_failure_id(proof),
            category=WorkFailureCategory.LEASE_ATTEMPTS_EXHAUSTED,
            safe_error_code="LEASE_RECLAIMS_EXHAUSTED",
            proof_digest=proof_digest,
        )
        next_trigger: TriggerBinding | None = None
        if current.attempt < MAX_AUTOMATIC_WORK_ATTEMPTS:
            next_trigger = replace(
                current.trigger,
                work_id=_stable_id("work.retry", request_digest),
            )
        command = ReportWorkFailure(command_id, proof.work_id, failure)
        context = DecisionContext(
            registry_state.generation,
            next_trigger,
            verified_evidence,
        )
        decision = decide(tip.state, command, context)
        if type(decision) is Rejected:
            raise HostWorkServiceError(decision.code)
        if type(decision) is not Accepted or len(decision.events) != 2:
            raise HostWorkServiceError("DIALOGUE_DECISION_INVALID")

        def marker_guard(boundary_authority: SessionLockAuthority) -> None:
            if boundary_authority is not authority:
                raise HostWorkServiceError("LOCK_AUTHORITY_REQUIRED")
            if self._coordinator._verify_evidence(config) != verified_evidence:
                raise HostWorkServiceError("EVIDENCE_CHANGED")
            exhaustion_marker_guard(boundary_authority)

        return self._coordinator._commit_dialogue_events(
            log,
            authority,
            tip,
            decision.events,
            collector,
            request_digest=request_digest,
            registry_generation=registry_state.generation,
            occurred_at=request.occurred_at,
            actor=request.actor,
            causation_id=command_id,
            marker_publication_guard=marker_guard,
        )

    def _publish_new(
        self,
        log: _DialogueTransactionLog,
        authority: SessionLockAuthority,
        registry_state: RegistryState,
        config_digest: str,
        request: HostWorkPublishRequest,
        request_digest: str,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome:
        if type(registry_state) is not RegistryState:
            raise HostWorkServiceError("REGISTRY_STATE_CONFLICT")
        state = registry_state
        registration = _registration(state, request.work.session_id)
        if (
            state.current_session_id != request.work.session_id
            or state.pending_handoff is not None
            or state.generation != request.work.registry_generation
            or registration is None
            or registration.status is not DialogueRegistrationStatus.ACTIVE
            or registration.activated_generation != state.generation
        ):
            raise HostWorkServiceError("SESSION_DEACTIVATED")
        config = self._coordinator._load_config_checked(
            request.work.session_id,
            config_digest,
        )
        tip = log.tip()
        events = log.read_committed(after_sequence=0)
        origin = _work_origin(tip.state, events)
        try:
            authoritative_work = derive_runnable_work(tip.state, origin)
        except WorkError as exc:
            raise HostWorkServiceError(exc.code) from exc
        if authoritative_work is None:
            raise HostWorkServiceError("NO_RUNNABLE_WORK")
        if (
            authoritative_work.work_id != request.work.work_id
            or authoritative_work.binding_digest != request.work.binding_digest
            or authoritative_work.trigger_event_id != request.work.trigger_event_id
            or authoritative_work.trigger_event_sequence
            != request.work.trigger_event_sequence
            or tip.state.conversation_version != request.work.conversation_version
            or request.fence.session_id != request.work.session_id
            or request.fence.work_id != request.work.work_id
            or request.fence.registry_generation != request.work.registry_generation
        ):
            raise HostWorkServiceError("WORK_SUPERSEDED")
        verified_evidence = self._coordinator._verify_evidence(config)
        if request.context.evidence != verified_evidence:
            raise HostWorkServiceError("EVIDENCE_CHECK_CONFLICT")
        lease_marker_guard = self._leases._marker_publication_guard(
            request.fence,
            authority,
        )

        command = request.command
        context = request.context
        if type(command) is ReportWorkFailure:
            current = _current_queued_work(tip.state)
            next_trigger: TriggerBinding | None = None
            if (
                command.failure.category in RETRYABLE_WORK_FAILURES
                and current.attempt < MAX_AUTOMATIC_WORK_ATTEMPTS
            ):
                next_trigger = replace(
                    current.trigger,
                    work_id=_stable_id("work.retry", request_digest),
                )
            context = replace(context, trigger=next_trigger)

        decision = decide(tip.state, command, context)
        if type(decision) is Rejected:
            raise HostWorkServiceError(decision.code)
        if type(decision) is not Accepted or not decision.events:
            raise HostWorkServiceError("DIALOGUE_DECISION_INVALID")

        def marker_guard(boundary_authority: SessionLockAuthority) -> None:
            if boundary_authority is not authority:
                raise HostWorkServiceError("LOCK_AUTHORITY_REQUIRED")
            if self._coordinator._verify_evidence(config) != verified_evidence:
                raise HostWorkServiceError("EVIDENCE_CHANGED")
            lease_marker_guard(boundary_authority)

        return self._coordinator._commit_dialogue_events(
            log,
            authority,
            tip,
            decision.events,
            collector,
            request_digest=request_digest,
            registry_generation=request.work.registry_generation,
            occurred_at=request.occurred_at,
            actor=request.actor,
            causation_id=command.command_id,
            marker_publication_guard=marker_guard,
        )
