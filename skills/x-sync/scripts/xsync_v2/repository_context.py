"""Authoritative focused Host context derived from durable dialogue facts."""

from __future__ import annotations

from .coordinator import DialogueCoordinator, _registration
from .domain import (
    AgentTurnCommitted,
    CommittedDialogueEvent,
    EvidenceHealth,
    GateAssessment,
    GateStatus,
    LearnerModelEntry,
    LearnerTurnSubmitted,
    Lens,
    TopicContract,
    TriggerKind,
)
from .evidence import EvidenceStoreError, SessionEvidenceStore
from .host_context import (
    EvidenceContextClaim,
    HostContextError,
    HostContextSource,
    LearnerTurnContext,
)
from .host_work import authoritative_work_snapshot
from .lease_store import LeaseRecord
from .locking import SessionLockAuthority
from .registry import DialogueRegistrationStatus
from .work import RunnableWork, derive_runnable_work

_PUBLISHABLE_EVIDENCE = frozenset(
    {EvidenceHealth.CURRENT, EvidenceHealth.CAPTURED_DIRTY}
)


def _setup_lens(focus: str) -> Lens:
    try:
        return Lens(focus)
    except ValueError as exc:
        raise HostContextError("HOST_CONTEXT_STATE_INVALID") from exc


def _previous_question_from_events(
    events: tuple[CommittedDialogueEvent, ...],
    learner_turn_id: str,
) -> str:
    learner_events: list[
        tuple[CommittedDialogueEvent, LearnerTurnSubmitted]
    ] = []
    for event in events:
        if type(event.payload) is LearnerTurnSubmitted:
            learner_payload_item = event.payload
            if learner_payload_item.learner_turn_id == learner_turn_id:
                learner_events.append((event, learner_payload_item))
    if len(learner_events) != 1:
        raise HostContextError("HOST_CONTEXT_STATE_INVALID")
    learner_event, learner_payload = learner_events[0]
    agent_events: list[AgentTurnCommitted] = []
    for event in events:
        if (
            event.sequence < learner_event.sequence
            and type(event.payload) is AgentTurnCommitted
        ):
            agent_payload = event.payload
            if agent_payload.result.question_id == learner_payload.question_id:
                agent_events.append(agent_payload)
    if len(agent_events) != 1:
        raise HostContextError("HOST_CONTEXT_STATE_INVALID")
    return agent_events[0].result.question


class RepositoryHostContextProvider:
    """Build focused context from one audited state and evidence snapshot."""

    def __init__(
        self,
        coordinator: DialogueCoordinator,
        evidence: SessionEvidenceStore,
    ) -> None:
        if (
            type(coordinator) is not DialogueCoordinator
            or type(evidence) is not SessionEvidenceStore
            or coordinator._dialogues is not evidence._dialogues
            or coordinator._locks is not evidence._locks
        ):
            raise HostContextError("INVALID_REPOSITORY_CONTEXT_CONFIGURATION")
        self._coordinator = coordinator
        self._evidence = evidence

    def load_context(
        self,
        work: RunnableWork,
        lease: LeaseRecord,
        authority: SessionLockAuthority,
    ) -> HostContextSource:
        """Load one exact work-bound context under live Session authority."""
        if (
            type(work) is not RunnableWork
            or type(lease) is not LeaseRecord
            or type(authority) is not SessionLockAuthority
            or authority.session_id != work.session_id
            or lease.session_id != work.session_id
            or lease.work_id != work.work_id
            or lease.binding_digest != work.binding_digest
            or lease.registry_generation != work.registry_generation
        ):
            raise HostContextError("HOST_CONTEXT_WORK_MISMATCH")
        try:
            snapshot = authoritative_work_snapshot(
                self._coordinator,
                work.session_id,
                authority,
            )
            if snapshot.work_origin is None:
                raise HostContextError("HOST_CONTEXT_WORK_MISMATCH")
            authoritative = derive_runnable_work(
                snapshot.dialogue_state,
                snapshot.work_origin,
            )
            if authoritative != work:
                raise HostContextError("HOST_CONTEXT_WORK_MISMATCH")
            registration = _registration(
                snapshot.registry_state,
                work.session_id,
            )
            if (
                registration is None
                or registration.status is not DialogueRegistrationStatus.ACTIVE
                or registration.config_digest is None
            ):
                raise HostContextError("HOST_CONTEXT_STATE_INVALID")
            config = self._coordinator._load_config_checked(
                work.session_id,
                registration.config_digest,
            )
            dialogue_log = self._coordinator._open_existing_dialogue(
                work.session_id,
                work.registry_generation,
                authority,
            )
            try:
                events = dialogue_log.read_committed(after_sequence=0)
            finally:
                dialogue_log.close()
            check = self._evidence.verify(config)
            if (
                check.health not in _PUBLISHABLE_EVIDENCE
                or check.evidence_digest != work.evidence_digest
            ):
                raise HostContextError("HOST_CONTEXT_EVIDENCE_UNAVAILABLE")
            evidence = self._evidence.load(
                work.session_id,
                work.evidence_digest,
            )
        except HostContextError:
            raise
        except EvidenceStoreError as exc:
            raise HostContextError("HOST_CONTEXT_EVIDENCE_UNAVAILABLE") from exc

        state = snapshot.dialogue_state
        topic = state.active_topic
        contract: TopicContract | None = None if topic is None else topic.contract
        allowed_refs = (
            None if contract is None else frozenset(contract.evidence_refs)
        )
        entries = tuple(
            item
            for item in evidence.entries
            if allowed_refs is None or item.evidence_id in allowed_refs
        )
        if not entries or (
            allowed_refs is not None
            and frozenset(item.evidence_id for item in entries) != allowed_refs
        ):
            raise HostContextError("HOST_CONTEXT_EVIDENCE_UNAVAILABLE")
        claims = tuple(
            EvidenceContextClaim(
                item.evidence_id,
                item.claim,
                item.location,
                item.content_hash,
            )
            for item in entries
        )

        if topic is None:
            task_scope = config.task_scope
            lens = _setup_lens(config.focus)
            gates: tuple[GateAssessment, ...] = ()
            previous_question = None
            learner_turn = None
            learner_model: tuple[LearnerModelEntry, ...] = ()
            if state.selected_candidate is None:
                priority_gap = "Choose a repository-grounded topic."
            else:
                priority_gap = (
                    "Build a repository-grounded Topic Contract for the "
                    f"selected candidate: {state.selected_candidate}"
                )
                clarification = state.topic_clarification
                if clarification is not None and clarification.answer is not None:
                    priority_gap += (
                        "\nLearner clarification question: "
                        f"{clarification.question}"
                        "\nLearner clarification answer: "
                        f"{clarification.answer}"
                    )
        else:
            active_contract = topic.contract
            task_scope = active_contract.task_scope.summary
            lens = active_contract.starting_lens
            gates = topic.gates
            previous_question = (
                topic.current_agent_turn.question
                if topic.current_agent_turn is not None
                else (
                    None
                    if topic.last_learner_turn_id is None
                    else _previous_question_from_events(
                        events,
                        topic.last_learner_turn_id,
                    )
                )
            )
            learner_turn = (
                None
                if topic.last_learner_text is None
                else LearnerTurnContext(topic.last_learner_text)
            )
            learner_model = topic.learner_model
            unsupported: str | None = None
            for gate in gates:
                if gate.status is not GateStatus.SUPPORTED:
                    unsupported = gate.gate_id.value
                    break
            priority_gap = (
                "Connect the current learner model to repository evidence."
                if unsupported is None
                else f"The {unsupported} gate still needs direct support."
            )

        selected_candidate = (
            state.selected_candidate
            if work.kind is TriggerKind.TOPIC_SELECTION
            else None
        )
        return HostContextSource(
            contract,
            task_scope,
            lens,
            gates,
            previous_question,
            learner_turn,
            learner_model,
            priority_gap,
            claims,
            work.observed_sequence,
            selected_candidate,
        )
