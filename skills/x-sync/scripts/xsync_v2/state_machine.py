"""Pure command decision and event reduction for X-Sync v2."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import TypeAlias, cast

from .domain import (
    Accepted,
    AgentTurnCommitted,
    AgentTurnResult,
    AnswerTopicClarification,
    CandidatesPresented,
    CommitAgentTurn,
    CommittedDialogueEvent,
    ConversationPhase,
    CurrentWorkState,
    Decision,
    DecisionContext,
    DialogueCommand,
    DialogueEventPayload,
    DialogueState,
    EvidenceCheck,
    EvidenceHealth,
    FencedQuiesceContext,
    GateAssessment,
    GateId,
    GateRequirement,
    GateStatus,
    HelpRequested,
    InsightKind,
    InsightProvenance,
    InsightStatus,
    LearnerModelEntry,
    LearnerTurnSubmitted,
    Lens,
    LensChanged,
    PauseCause,
    PauseTopic,
    PendingDialogueEvent,
    PrepareSessionDeactivation,
    PresentCandidates,
    QuestionIntent,
    RecoverWork,
    Rejected,
    ReportWorkFailure,
    RequestHelp,
    RequestTopicClarification,
    ResumeTopic,
    SelectTopic,
    SetLens,
    SessionDeactivationPrepared,
    SessionLifecycle,
    SessionStarted,
    StartSession,
    StartTopic,
    SubmitCustomTopic,
    SubmitLearnerTurn,
    SwitchTopic,
    TaskScope,
    TopicClarification,
    TopicClarificationAnswered,
    TopicClarificationRequested,
    TopicContract,
    TopicLifecycle,
    TopicPaused,
    TopicResumed,
    TopicRunState,
    TopicSelectionSubmitted,
    TopicStarted,
    TopicSwitchRequested,
    TriggerBinding,
    TriggerKind,
    WorkDeadLettered,
    WorkFailed,
    WorkFailure,
    WorkFailureCategory,
    WorkRecoveryAction,
    WorkRecoveryRequested,
    WorkRequeued,
    WorkStatus,
)
from .work_identity import (
    WorkIdentityError,
    derive_canonical_work_id,
    is_canonical_trigger_binding,
    is_optional_protocol_id,
    is_optional_sha256_digest,
    is_protocol_id,
    is_sha256_digest,
)

Handler: TypeAlias = Callable[
    [DialogueState, DialogueCommand, DecisionContext], Decision
]


def _valid_text(value: object) -> bool:
    return type(value) is str and bool(value.strip())


def _valid_positive_int(value: object) -> bool:
    return type(value) is int and value >= 1


def _valid_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _accept(command_id: str, payload: DialogueEventPayload) -> Decision:
    if not _valid_text(command_id):
        return Rejected("VALIDATION_FAILED")
    return Accepted((PendingDialogueEvent(command_id, payload),))


REQUIRED_GATES = frozenset(GateId)
PUBLISHABLE_EVIDENCE = frozenset(
    {EvidenceHealth.CURRENT, EvidenceHealth.CAPTURED_DIRTY}
)
MAX_AUTOMATIC_WORK_ATTEMPTS = 3
RETRYABLE_WORK_FAILURES = frozenset(
    {
        WorkFailureCategory.HOST_TRANSIENT,
        WorkFailureCategory.RESULT_INVALID,
        WorkFailureCategory.LEASE_ATTEMPTS_EXHAUSTED,
    }
)


def conversation_version_delta(payload: DialogueEventPayload) -> int:
    """Return the single canonical version effect for a dialogue fact."""
    if type(payload) in {WorkFailed, WorkRequeued}:
        return 0
    if type(payload) in EVENT_PAYLOAD_TYPES:
        return 1
    raise ValueError("UNKNOWN_EVENT")


def _valid_contract_shape(contract: object) -> bool:
    if type(contract) is not TopicContract:
        return False
    if (
        not _valid_positive_int(contract.contract_version)
        or type(contract.task_scope) is not TaskScope
        or not is_optional_protocol_id(contract.supersedes_contract_id)
    ):
        return False
    identifiers = (
        contract.contract_id,
        contract.topic_run_id,
        contract.task_scope.task_id,
    )
    text_fields = (
        contract.title,
        contract.guiding_question,
        contract.objective,
    )
    if (
        any(not is_protocol_id(item) for item in identifiers)
        or any(not _valid_text(item) for item in text_fields)
        or not is_sha256_digest(contract.contract_digest)
    ):
        return False
    if (
        type(contract.starting_lens) is not Lens
        or type(contract.bridge_required) is not bool
        or type(contract.evidence_refs) is not tuple
        or any(not _valid_text(item) for item in contract.evidence_refs)
        or type(contract.gates) is not tuple
        or any(type(item) is not GateRequirement for item in contract.gates)
        or any(
            type(item.gate_id) is not GateId
            or type(item.required) is not bool
            for item in contract.gates
        )
        or not _valid_text(contract.task_scope.summary)
        or type(contract.task_scope.included_paths) is not tuple
        or type(contract.task_scope.excluded_paths) is not tuple
        or any(
            not _valid_text(item)
            for item in contract.task_scope.included_paths
            + contract.task_scope.excluded_paths
        )
    ):
        return False
    return True


def _valid_contract(contract: object) -> bool:
    if not _valid_contract_shape(contract):
        return False
    topic_contract = cast(TopicContract, contract)
    gate_ids = tuple(item.gate_id for item in topic_contract.gates)
    return (
        topic_contract.bridge_required
        and bool(topic_contract.evidence_refs)
        and len(topic_contract.evidence_refs)
        == len(set(topic_contract.evidence_refs))
        and len(gate_ids) == len(REQUIRED_GATES)
        and frozenset(gate_ids) == REQUIRED_GATES
        and all(item.required for item in topic_contract.gates)
    )


def _valid_evidence_shape(check: object) -> bool:
    return (
        type(check) is EvidenceCheck
        and type(check.health) is EvidenceHealth
        and is_sha256_digest(check.evidence_digest)
        and is_optional_sha256_digest(check.exact_recheck_fingerprint)
    )


def _valid_evidence(check: object) -> bool:
    if not _valid_evidence_shape(check):
        return False
    evidence = cast(EvidenceCheck, check)
    return (
        evidence.health is not EvidenceHealth.CAPTURED_DIRTY
        or bool(evidence.exact_recheck_fingerprint)
    )


def _valid_candidates_shape(candidates: object) -> bool:
    return type(candidates) is tuple and all(
        _valid_text(item) for item in candidates
    )


def _valid_candidates(candidates: object) -> bool:
    if not _valid_candidates_shape(candidates):
        return False
    candidate_tuple = cast(tuple[str, ...], candidates)
    return (
        1 <= len(candidate_tuple) <= 4
        and len(candidate_tuple) == len(set(candidate_tuple))
    )


def _valid_trigger(
    trigger: object,
    kind: TriggerKind,
    contract_digest: str | None,
    evidence_digest: str,
) -> bool:
    if not is_canonical_trigger_binding(trigger):
        return False
    binding = cast(TriggerBinding, trigger)
    return (
        binding.kind is kind
        and binding.contract_digest == contract_digest
        and binding.evidence_digest == evidence_digest
    )


def _valid_work_failure(failure: object) -> bool:
    return (
        type(failure) is WorkFailure
        and is_protocol_id(failure.failure_id)
        and type(failure.category) is WorkFailureCategory
        and _valid_text(failure.safe_error_code)
        and is_sha256_digest(failure.proof_digest)
    )


def _valid_current_work(
    work: object,
    session_id: str,
    observed_sequence: int,
) -> bool:
    if (
        type(work) is not CurrentWorkState
        or not is_protocol_id(work.work_id)
        or not is_protocol_id(work.trigger_event_id)
        or not _valid_positive_int(work.trigger_event_sequence)
        or work.trigger_event_sequence > observed_sequence
        or not is_canonical_trigger_binding(work.trigger)
        or not _valid_positive_int(work.attempt)
        or work.attempt > MAX_AUTOMATIC_WORK_ATTEMPTS
        or type(work.status) is not WorkStatus
        or (
            work.last_failure is not None
            and not _valid_work_failure(work.last_failure)
        )
        or type(work.allowed_recovery_actions) is not tuple
        or any(
            type(item) is not WorkRecoveryAction
            for item in work.allowed_recovery_actions
        )
        or len(work.allowed_recovery_actions)
        != len(set(work.allowed_recovery_actions))
    ):
        return False
    try:
        expected_id = derive_canonical_work_id(
            session_id=session_id,
            trigger_event_id=work.trigger_event_id,
            trigger_event_sequence=work.trigger_event_sequence,
            trigger=work.trigger,
        )
    except WorkIdentityError:
        return False
    if work.work_id != expected_id:
        return False
    if work.status is WorkStatus.QUEUED:
        return (
            not work.allowed_recovery_actions
            and (work.attempt == 1 or work.last_failure is not None)
        )
    if work.status is WorkStatus.FAILED:
        return work.last_failure is not None and not work.allowed_recovery_actions
    if work.status is WorkStatus.DEAD_LETTER:
        return work.last_failure is not None and bool(
            work.allowed_recovery_actions
        )
    return not work.allowed_recovery_actions


def _queued_work_from_event(
    session_id: str,
    event: CommittedDialogueEvent,
    trigger: TriggerBinding,
    *,
    attempt: int = 1,
    last_failure: WorkFailure | None = None,
) -> CurrentWorkState:
    return CurrentWorkState(
        work_id=derive_canonical_work_id(
            session_id=session_id,
            trigger_event_id=event.event_id,
            trigger_event_sequence=event.sequence,
            trigger=trigger,
        ),
        trigger_event_id=event.event_id,
        trigger_event_sequence=event.sequence,
        trigger=trigger,
        attempt=attempt,
        status=WorkStatus.QUEUED,
        last_failure=last_failure,
    )


def _retry_trigger_matches(
    current: TriggerBinding,
    candidate: TriggerBinding,
) -> bool:
    return (
        candidate.work_id != current.work_id
        and candidate.kind is current.kind
        and candidate.parent_turn_id == current.parent_turn_id
        and candidate.contract_digest == current.contract_digest
        and candidate.input_digest == current.input_digest
        and candidate.evidence_digest == current.evidence_digest
    )


def _allowed_recovery_actions(
    state: DialogueState,
) -> tuple[WorkRecoveryAction, ...]:
    if state.active_topic is None:
        return (WorkRecoveryAction.RETRY,)
    return (WorkRecoveryAction.RETRY, WorkRecoveryAction.REGROUND)


def _automatic_retry_allowed(work: CurrentWorkState) -> bool:
    failure = work.last_failure
    return (
        failure is not None
        and failure.category in RETRYABLE_WORK_FAILURES
        and work.attempt < MAX_AUTOMATIC_WORK_ATTEMPTS
    )


def _matching_trigger(
    trigger: TriggerBinding | None,
    kind: TriggerKind,
    contract_digest: str | None,
    evidence_digest: str,
) -> TriggerBinding | None:
    if _valid_trigger(trigger, kind, contract_digest, evidence_digest):
        return trigger
    return None


def _valid_model_entry(
    contract: TopicContract,
    entry: object,
    known_turn_ids: frozenset[str],
) -> bool:
    if not _valid_model_entry_shape(entry):
        return False
    model_entry = cast(LearnerModelEntry, entry)
    return (
        set(model_entry.source_turn_ids).issubset(known_turn_ids)
        and set(model_entry.evidence_refs).issubset(contract.evidence_refs)
        and (
            model_entry.provenance is not InsightProvenance.AGENT_INFERRED
            or model_entry.status is InsightStatus.WORKING_MODEL
        )
        and (
            model_entry.status is not InsightStatus.CONFIRMED
            or (
                model_entry.provenance
                in {
                    InsightProvenance.LEARNER_EXPLICIT,
                    InsightProvenance.JOINTLY_CONFIRMED,
                }
                and bool(model_entry.source_turn_ids)
                and bool(model_entry.evidence_refs)
            )
        )
    )


def _valid_model_entry_shape(entry: object) -> bool:
    return (
        type(entry) is LearnerModelEntry
        and _valid_text(entry.entry_id)
        and _valid_text(entry.statement)
        and type(entry.kind) is InsightKind
        and type(entry.status) is InsightStatus
        and type(entry.provenance) is InsightProvenance
        and type(entry.source_turn_ids) is tuple
        and all(_valid_text(item) for item in entry.source_turn_ids)
        and type(entry.evidence_refs) is tuple
        and all(_valid_text(item) for item in entry.evidence_refs)
    )


def _valid_gate_assessment(
    contract: TopicContract,
    item: object,
    known_turn_ids: frozenset[str],
) -> bool:
    if not _valid_gate_assessment_shape(item):
        return False
    assessment = cast(GateAssessment, item)
    return (
        set(assessment.source_turn_ids).issubset(known_turn_ids)
        and set(assessment.evidence_refs).issubset(contract.evidence_refs)
        and (
            assessment.status is not GateStatus.SUPPORTED
            or (
                bool(assessment.source_turn_ids)
                and bool(assessment.evidence_refs)
            )
        )
    )


def _valid_gate_assessment_shape(item: object) -> bool:
    return (
        type(item) is GateAssessment
        and type(item.gate_id) is GateId
        and type(item.status) is GateStatus
        and type(item.source_turn_ids) is tuple
        and all(_valid_text(turn_id) for turn_id in item.source_turn_ids)
        and type(item.evidence_refs) is tuple
        and all(_valid_text(evidence_id) for evidence_id in item.evidence_refs)
    )


def _valid_agent_turn_shape(result: object) -> bool:
    return (
        type(result) is AgentTurnResult
        and all(
            _valid_text(item)
            for item in (
                result.heard,
                result.one_step_further,
                result.question_id,
                result.question,
            )
        )
        and type(result.question_intent) is QuestionIntent
        and type(result.learner_model_delta) is tuple
        and all(
            _valid_model_entry_shape(item)
            for item in result.learner_model_delta
        )
        and type(result.gate_assessments) is tuple
        and all(
            _valid_gate_assessment_shape(item)
            for item in result.gate_assessments
        )
        and type(result.evidence_refs) is tuple
        and all(_valid_text(item) for item in result.evidence_refs)
    )


def _valid_agent_turn(
    contract: TopicContract,
    result: object,
    evidence: EvidenceCheck,
    known_turn_ids: frozenset[str],
) -> bool:
    if not _valid_agent_turn_shape(result):
        return False
    turn = cast(AgentTurnResult, result)
    assessments = tuple(item.gate_id for item in turn.gate_assessments)
    model_ids = tuple(entry.entry_id for entry in turn.learner_model_delta)
    model_refs_valid = all(
        _valid_model_entry(contract, entry, known_turn_ids)
        for entry in turn.learner_model_delta
    )
    gates_valid = all(
        _valid_gate_assessment(contract, item, known_turn_ids)
        for item in turn.gate_assessments
    )
    return (
        model_refs_valid
        and len(model_ids) == len(set(model_ids))
        and gates_valid
        and _valid_evidence(evidence)
        and bool(turn.evidence_refs)
        and len(turn.evidence_refs) == len(set(turn.evidence_refs))
        and set(turn.evidence_refs).issubset(contract.evidence_refs)
        and len(assessments) == len(REQUIRED_GATES)
        and frozenset(assessments) == REQUIRED_GATES
    )


MODEL_STATUS_TRANSITIONS = {
    InsightStatus.WORKING_MODEL: frozenset(
        {InsightStatus.CONFIRMED, InsightStatus.STALE, InsightStatus.DISPUTED}
    ),
    InsightStatus.OPEN_QUESTION: frozenset(
        {
            InsightStatus.WORKING_MODEL,
            InsightStatus.CONFIRMED,
            InsightStatus.STALE,
            InsightStatus.DISPUTED,
        }
    ),
    InsightStatus.CONFIRMED: frozenset(
        {InsightStatus.STALE, InsightStatus.DISPUTED}
    ),
    InsightStatus.STALE: frozenset(
        {
            InsightStatus.WORKING_MODEL,
            InsightStatus.CONFIRMED,
            InsightStatus.DISPUTED,
        }
    ),
    InsightStatus.DISPUTED: frozenset(
        {
            InsightStatus.WORKING_MODEL,
            InsightStatus.CONFIRMED,
            InsightStatus.STALE,
        }
    ),
}


def _valid_model_updates(
    current: tuple[LearnerModelEntry, ...],
    updates: tuple[LearnerModelEntry, ...],
) -> bool:
    by_id = {entry.entry_id: entry for entry in current}
    for update in updates:
        before = by_id.get(update.entry_id)
        if before is None:
            continue
        provenance_transition = (
            update.provenance is before.provenance
            or (
                before.provenance
                in {
                    InsightProvenance.AGENT_INFERRED,
                    InsightProvenance.LEARNER_EXPLICIT,
                }
                and update.provenance
                is InsightProvenance.JOINTLY_CONFIRMED
            )
        )
        if (
            update.kind is not before.kind
            or update.statement != before.statement
            or not set(before.source_turn_ids).issubset(update.source_turn_ids)
            or not set(before.evidence_refs).issubset(update.evidence_refs)
            or update.status not in MODEL_STATUS_TRANSITIONS[before.status]
            or not provenance_transition
        ):
            return False
    return True


def _apply_model_updates(
    current: tuple[LearnerModelEntry, ...],
    updates: tuple[LearnerModelEntry, ...],
) -> tuple[LearnerModelEntry, ...]:
    by_id = {entry.entry_id: entry for entry in updates}
    replaced_entries = tuple(by_id.get(entry.entry_id, entry) for entry in current)
    existing_ids = {entry.entry_id for entry in current}
    return replaced_entries + tuple(
        entry for entry in updates if entry.entry_id not in existing_ids
    )


def _valid_topic_state(
    topic: object,
    expected_lifecycle: TopicLifecycle,
    session_id: str,
    observed_sequence: int,
) -> bool:
    if (
        type(topic) is not TopicRunState
        or topic.lifecycle is not expected_lifecycle
        or not _valid_contract(topic.contract)
        or topic.topic_run_id != topic.contract.topic_run_id
        or not _valid_evidence(
            EvidenceCheck(
                topic.evidence_health,
                topic.evidence_digest,
                topic.exact_recheck_fingerprint,
            )
        )
        or type(topic.gates) is not tuple
        or any(type(item) is not GateAssessment for item in topic.gates)
        or type(topic.learner_model) is not tuple
        or any(
            type(item) is not LearnerModelEntry
            for item in topic.learner_model
        )
        or type(topic.learner_turn_ids) is not tuple
        or (
            topic.current_lens is not None
            and type(topic.current_lens) is not Lens
        )
        or any(not _valid_text(item) for item in topic.learner_turn_ids)
        or (
            topic.work is not None
            and not _valid_current_work(
                topic.work,
                session_id,
                observed_sequence,
            )
        )
    ):
        return False
    known_turn_ids = frozenset(topic.learner_turn_ids)
    gate_ids = tuple(item.gate_id for item in topic.gates)
    model_ids = tuple(item.entry_id for item in topic.learner_model)
    if (
        len(gate_ids) != len(REQUIRED_GATES)
        or frozenset(gate_ids) != REQUIRED_GATES
        or not all(
            _valid_gate_assessment(topic.contract, item, known_turn_ids)
            for item in topic.gates
        )
        or len(model_ids) != len(set(model_ids))
        or not all(
            _valid_model_entry(topic.contract, item, known_turn_ids)
            for item in topic.learner_model
        )
        or len(topic.learner_turn_ids) != len(known_turn_ids)
        or (
            not topic.learner_turn_ids
            and (
                topic.last_learner_turn_id is not None
                or topic.last_learner_text is not None
            )
        )
        or (
            bool(topic.learner_turn_ids)
            and (
                topic.last_learner_turn_id != topic.learner_turn_ids[-1]
                or not _valid_text(topic.last_learner_text)
            )
        )
        or sum(
            item is not None
            for item in (topic.current_agent_turn, topic.work)
        )
        != 1
    ):
        return False
    if topic.current_agent_turn is not None:
        evidence = EvidenceCheck(
            topic.evidence_health,
            topic.evidence_digest,
            topic.exact_recheck_fingerprint,
        )
        if (
            not _valid_agent_turn(
                topic.contract,
                topic.current_agent_turn,
                evidence,
                known_turn_ids,
            )
            or topic.current_agent_turn.gate_assessments != topic.gates
            or any(
                entry not in topic.learner_model
                for entry in topic.current_agent_turn.learner_model_delta
            )
        ):
            return False
    if topic.work is not None:
        work = topic.work
        trigger = work.trigger
        if (
            not _valid_trigger(
                trigger,
                trigger.kind,
                topic.contract.contract_digest,
                topic.evidence_digest,
            )
            or (
                expected_lifecycle is TopicLifecycle.ACTIVE
                and work.status
                not in {
                    WorkStatus.QUEUED,
                    WorkStatus.FAILED,
                    WorkStatus.DEAD_LETTER,
                }
            )
            or (
                expected_lifecycle is TopicLifecycle.PAUSED
                and work.status
                not in {WorkStatus.SUPERSEDED, WorkStatus.DEAD_LETTER}
            )
            or (
                work.status is WorkStatus.DEAD_LETTER
                and work.allowed_recovery_actions
                != (
                    WorkRecoveryAction.RETRY,
                    WorkRecoveryAction.REGROUND,
                )
            )
        ):
            return False
    return True


def _start_session(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not StartSession:
        return Rejected("TOPIC_STATE_CONFLICT")
    if (
        state.lifecycle is not SessionLifecycle.NEW
        or state.phase is not ConversationPhase.NONE
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    trigger = _matching_trigger(
        context.trigger,
        TriggerKind.TOPIC_CANDIDATES,
        None,
        context.evidence.evidence_digest,
    )
    if trigger is None:
        return Rejected("VALIDATION_FAILED")
    return _accept(command.command_id, SessionStarted(trigger))


def _present_candidates(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not PresentCandidates:
        return Rejected("TOPIC_STATE_CONFLICT")
    if (
        state.phase is not ConversationPhase.WAITING_HOST
        or state.active_topic
        or state.session_work is None
        or state.session_work.status is not WorkStatus.QUEUED
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_candidates(command.candidates):
        return Rejected("VALIDATION_FAILED")
    trigger = context.trigger
    if trigger is None or trigger != state.session_unresolved_trigger:
        return Rejected("WORK_SUPERSEDED")
    return _accept(
        command.command_id,
        CandidatesPresented(command.candidates, trigger),
    )


def _select_topic(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not SelectTopic:
        return Rejected("TOPIC_STATE_CONFLICT")
    if (
        state.phase is not ConversationPhase.CHOOSING_TOPIC
        or state.active_topic is not None
        or state.session_work is not None
        or state.selected_candidate is not None
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_text(command.candidate) or command.candidate not in state.candidates:
        return Rejected("VALIDATION_FAILED")
    trigger = _matching_trigger(
        context.trigger,
        TriggerKind.TOPIC_SELECTION,
        None,
        context.evidence.evidence_digest,
    )
    if trigger is None:
        return Rejected("VALIDATION_FAILED")
    return _accept(
        command.command_id,
        TopicSelectionSubmitted(command.candidate, trigger),
    )


def _submit_custom_topic(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not SubmitCustomTopic:
        return Rejected("TOPIC_STATE_CONFLICT")
    if (
        state.phase is not ConversationPhase.CHOOSING_TOPIC
        or state.active_topic is not None
        or state.session_work is not None
        or state.selected_candidate is not None
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_text(command.topic):
        return Rejected("VALIDATION_FAILED")
    trigger = _matching_trigger(
        context.trigger,
        TriggerKind.TOPIC_SELECTION,
        None,
        context.evidence.evidence_digest,
    )
    if trigger is None:
        return Rejected("VALIDATION_FAILED")
    return _accept(
        command.command_id,
        TopicSelectionSubmitted(command.topic, trigger),
    )


def _request_topic_clarification(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not RequestTopicClarification:
        return Rejected("TOPIC_STATE_CONFLICT")
    work = state.session_work
    if (
        state.phase is not ConversationPhase.WAITING_HOST
        or state.active_topic is not None
        or state.selected_candidate is None
        or work is None
        or work.status is not WorkStatus.QUEUED
        or work.trigger.kind is not TriggerKind.TOPIC_SELECTION
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_text(command.question_id) or not _valid_text(command.question):
        return Rejected("VALIDATION_FAILED")
    if context.trigger != work.trigger:
        return Rejected("WORK_SUPERSEDED")
    return _accept(
        command.command_id,
        TopicClarificationRequested(
            command.question_id,
            command.question,
            work.trigger,
        ),
    )


def _answer_topic_clarification(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not AnswerTopicClarification:
        return Rejected("TOPIC_STATE_CONFLICT")
    clarification = state.topic_clarification
    if (
        state.phase is not ConversationPhase.CLARIFYING_TOPIC
        or state.active_topic is not None
        or state.selected_candidate is None
        or state.session_work is not None
        or clarification is None
        or clarification.answer is not None
        or clarification.question_id != command.question_id
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_text(command.answer):
        return Rejected("VALIDATION_FAILED")
    trigger = _matching_trigger(
        context.trigger,
        TriggerKind.TOPIC_SELECTION,
        None,
        context.evidence.evidence_digest,
    )
    if trigger is None or trigger.parent_turn_id != command.question_id:
        return Rejected("VALIDATION_FAILED")
    return _accept(
        command.command_id,
        TopicClarificationAnswered(
            command.question_id,
            command.answer,
            trigger,
        ),
    )


def _start_topic(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not StartTopic:
        return Rejected("TOPIC_STATE_CONFLICT")
    legacy_selection = (
        state.phase is ConversationPhase.CHOOSING_TOPIC
        and state.session_work is None
        and state.selected_candidate is None
        and command.selected_candidate is None
    )
    durable_selection = (
        state.phase is ConversationPhase.WAITING_HOST
        and state.session_work is not None
        and state.session_work.status is WorkStatus.QUEUED
        and state.session_work.trigger.kind is TriggerKind.TOPIC_SELECTION
        and state.selected_candidate is not None
        and command.selected_candidate == state.selected_candidate
        and (
            state.topic_clarification is None
            or state.topic_clarification.answer is not None
        )
    )
    if state.active_topic is not None or not (legacy_selection or durable_selection):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_contract(command.contract):
        return Rejected("VALIDATION_FAILED")
    expected_kind = (
        TriggerKind.INITIAL_TURN
        if context.evidence.health in PUBLISHABLE_EVIDENCE
        else TriggerKind.REGROUND
    )
    trigger = _matching_trigger(
        context.trigger,
        expected_kind,
        command.contract.contract_digest,
        context.evidence.evidence_digest,
    )
    if not _valid_evidence(context.evidence) or trigger is None:
        return Rejected("VALIDATION_FAILED")
    selection_trigger = (
        cast(CurrentWorkState, state.session_work).trigger
        if durable_selection
        else None
    )
    return _accept(
        command.command_id,
        TopicStarted(
            command.contract,
            context.evidence,
            trigger,
            command.selected_candidate,
            selection_trigger,
        ),
    )


def _commit_agent_turn(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not CommitAgentTurn:
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.phase is not ConversationPhase.WAITING_HOST or not state.active_topic:
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.active_topic.open_question_id is not None:
        return Rejected("TOPIC_STATE_CONFLICT")
    topic = state.active_topic
    if topic.work is None or topic.work.status is not WorkStatus.QUEUED:
        return Rejected("TOPIC_STATE_CONFLICT")
    if topic.evidence_health not in PUBLISHABLE_EVIDENCE:
        return Rejected("EVIDENCE_STALE")
    trigger = context.trigger
    if (
        trigger is None
        or trigger != topic.work.trigger
        or context.evidence.health is not topic.evidence_health
        or context.evidence.evidence_digest != topic.evidence_digest
        or context.evidence.exact_recheck_fingerprint
        != topic.exact_recheck_fingerprint
    ):
        return Rejected("WORK_SUPERSEDED")
    if not _valid_agent_turn(
        topic.contract,
        command.result,
        context.evidence,
        frozenset(topic.learner_turn_ids),
    ):
        return Rejected("VALIDATION_FAILED")
    if not _valid_model_updates(
        topic.learner_model, command.result.learner_model_delta
    ):
        return Rejected("VALIDATION_FAILED")
    return _accept(
        command.command_id,
        AgentTurnCommitted(command.result, trigger, context.evidence),
    )


def _submit_turn(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not SubmitLearnerTurn:
        return Rejected("TOPIC_STATE_CONFLICT")
    topic = state.active_topic
    if (
        state.phase is not ConversationPhase.AWAITING_USER
        or topic is None
        or topic.open_question_id != command.question_id
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if (
        not _valid_text(command.question_id)
        or not _valid_text(command.learner_turn_id)
        or not _valid_text(command.text)
    ):
        return Rejected("VALIDATION_FAILED")
    if command.learner_turn_id in topic.learner_turn_ids:
        return Rejected("VALIDATION_FAILED")
    trigger = _matching_trigger(
        context.trigger,
        TriggerKind.LEARNER_REPLY,
        topic.contract.contract_digest,
        topic.evidence_digest,
    )
    if trigger is None or trigger.parent_turn_id != command.learner_turn_id:
        return Rejected("VALIDATION_FAILED")
    return _accept(
        command.command_id,
        LearnerTurnSubmitted(
            command.question_id,
            command.learner_turn_id,
            command.text,
            trigger,
        ),
    )


def _pause_topic(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not PauseTopic:
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.active_topic is None:
        return Rejected("TOPIC_STATE_CONFLICT")
    return _accept(command.command_id, TopicPaused(state.active_topic.topic_run_id))


def _set_lens(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not SetLens:
        return Rejected("TOPIC_STATE_CONFLICT")
    topic = state.active_topic
    if (
        topic is None
        or state.phase
        not in {ConversationPhase.AWAITING_USER, ConversationPhase.WAITING_HOST}
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if type(command.lens) is not Lens:
        return Rejected("VALIDATION_FAILED")
    if command.lens is topic.lens:
        return Rejected("LENS_UNCHANGED")
    if (
        context.evidence.health is not topic.evidence_health
        or context.evidence.evidence_digest != topic.evidence_digest
        or context.evidence.exact_recheck_fingerprint
        != topic.exact_recheck_fingerprint
    ):
        return Rejected("EVIDENCE_STALE")
    trigger = _matching_trigger(
        context.trigger,
        TriggerKind.LENS_CHANGED,
        topic.contract.contract_digest,
        topic.evidence_digest,
    )
    if trigger is None:
        return Rejected("VALIDATION_FAILED")
    expected_parent = (
        topic.current_agent_turn.question_id
        if state.phase is ConversationPhase.AWAITING_USER
        and topic.current_agent_turn is not None
        else (
            None
            if topic.work is None
            else topic.work.trigger.parent_turn_id
        )
    )
    if trigger.parent_turn_id != expected_parent:
        return Rejected("VALIDATION_FAILED")
    return _accept(
        command.command_id,
        LensChanged(topic.topic_run_id, command.lens, trigger),
    )


def _request_help(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not RequestHelp:
        return Rejected("TOPIC_STATE_CONFLICT")
    topic = state.active_topic
    if (
        state.phase is not ConversationPhase.AWAITING_USER
        or topic is None
        or topic.open_question_id != command.question_id
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not is_protocol_id(command.question_id):
        return Rejected("VALIDATION_FAILED")
    if (
        context.evidence.health is not topic.evidence_health
        or context.evidence.evidence_digest != topic.evidence_digest
        or context.evidence.exact_recheck_fingerprint
        != topic.exact_recheck_fingerprint
    ):
        return Rejected("EVIDENCE_STALE")
    trigger = _matching_trigger(
        context.trigger,
        TriggerKind.HELP,
        topic.contract.contract_digest,
        topic.evidence_digest,
    )
    if trigger is None or trigger.parent_turn_id != command.question_id:
        return Rejected("VALIDATION_FAILED")
    return _accept(
        command.command_id,
        HelpRequested(topic.topic_run_id, command.question_id, trigger),
    )


def _switch_topic(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not SwitchTopic:
        return Rejected("TOPIC_STATE_CONFLICT")
    topic = state.active_topic
    if topic is None:
        return Rejected("TOPIC_STATE_CONFLICT")
    trigger = _matching_trigger(
        context.trigger,
        TriggerKind.TOPIC_CANDIDATES,
        None,
        context.evidence.evidence_digest,
    )
    if trigger is None:
        return Rejected("VALIDATION_FAILED")
    return _accept(
        command.command_id,
        TopicSwitchRequested(topic.topic_run_id, trigger),
    )


def _resume_topic(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not ResumeTopic:
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.phase is not ConversationPhase.NONE or state.active_topic is not None:
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_text(command.topic_run_id):
        return Rejected("VALIDATION_FAILED")
    topic = next(
        (
            item
            for item in state.paused_topics
            if item.topic_run_id == command.topic_run_id
        ),
        None,
    )
    if topic is None:
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_evidence(context.evidence):
        return Rejected("VALIDATION_FAILED")
    evidence_changed = (
        context.evidence.health is not topic.evidence_health
        or context.evidence.evidence_digest != topic.evidence_digest
        or context.evidence.exact_recheck_fingerprint
        != topic.exact_recheck_fingerprint
    )
    requires_reground = evidence_changed or context.evidence.health in {
        EvidenceHealth.STALE,
        EvidenceHealth.DISPUTED,
        EvidenceHealth.UNAVAILABLE,
    }
    resumed_trigger = context.trigger
    if requires_reground:
        resumed_trigger = _matching_trigger(
            resumed_trigger,
            TriggerKind.REGROUND,
            topic.contract.contract_digest,
            context.evidence.evidence_digest,
        )
        if resumed_trigger is None:
            return Rejected("VALIDATION_FAILED")
    elif topic.current_agent_turn is not None:
        if resumed_trigger is not None:
            return Rejected("VALIDATION_FAILED")
    elif topic.work is not None:
        if topic.work.status is WorkStatus.DEAD_LETTER:
            return Rejected("TOPIC_STATE_CONFLICT")
        if topic.work.status is not WorkStatus.SUPERSEDED:
            return Rejected("STATE_INVARIANT_VIOLATION")
        saved_trigger = topic.work.trigger
        resumed_trigger = _matching_trigger(
            resumed_trigger,
            saved_trigger.kind,
            topic.contract.contract_digest,
            context.evidence.evidence_digest,
        )
        if (
            resumed_trigger is None
            or resumed_trigger.work_id == saved_trigger.work_id
            or resumed_trigger.parent_turn_id
            != saved_trigger.parent_turn_id
            or resumed_trigger.input_digest
            != saved_trigger.input_digest
        ):
            return Rejected("VALIDATION_FAILED")
    else:
        return Rejected("TOPIC_STATE_CONFLICT")
    return _accept(
        command.command_id,
        TopicResumed(
            command.topic_run_id,
            requires_reground,
            resumed_trigger,
            context.evidence,
        ),
    )


def _current_nonterminal_work(state: DialogueState) -> CurrentWorkState | None:
    session_work = state.session_work
    topic_work = state.active_topic.work if state.active_topic is not None else None
    matches = tuple(
        item
        for item in (session_work, topic_work)
        if item is not None
        and item.status
        in {WorkStatus.QUEUED, WorkStatus.FAILED, WorkStatus.DEAD_LETTER}
    )
    if len(matches) != 1:
        return None
    return matches[0]


def _replace_work(
    state: DialogueState,
    current: CurrentWorkState,
    replacement: CurrentWorkState,
) -> DialogueState:
    if state.session_work == current:
        return replace(state, session_work=replacement)
    if state.active_topic is not None and state.active_topic.work == current:
        return replace(
            state,
            active_topic=replace(state.active_topic, work=replacement),
        )
    matches = tuple(
        topic for topic in state.paused_topics if topic.work == current
    )
    if len(matches) != 1:
        raise ValueError("STATE_INVARIANT_VIOLATION")
    return replace(
        state,
        paused_topics=tuple(
            replace(topic, work=replacement) if topic is matches[0] else topic
            for topic in state.paused_topics
        ),
    )


def _report_work_failure(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not ReportWorkFailure:
        return Rejected("TOPIC_STATE_CONFLICT")
    work = _current_nonterminal_work(state)
    if (
        state.phase is not ConversationPhase.WAITING_HOST
        or work is None
        or work.status is not WorkStatus.QUEUED
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not _valid_text(command.work_id) or not _valid_work_failure(
        command.failure
    ):
        return Rejected("VALIDATION_FAILED")
    if context.evidence.evidence_digest != work.trigger.evidence_digest:
        return Rejected("VALIDATION_FAILED")
    if command.work_id != work.work_id:
        return Rejected("WORK_SUPERSEDED")
    failed = WorkFailed(
        work.work_id,
        work.trigger,
        work.attempt,
        command.failure,
    )
    if (
        command.failure.category in RETRYABLE_WORK_FAILURES
        and work.attempt < MAX_AUTOMATIC_WORK_ATTEMPTS
    ):
        next_trigger = context.trigger
        if (
            next_trigger is None
            or not is_canonical_trigger_binding(next_trigger)
            or not _retry_trigger_matches(work.trigger, next_trigger)
        ):
            return Rejected("VALIDATION_FAILED")
        return Accepted(
            (
                PendingDialogueEvent(command.command_id, failed),
                PendingDialogueEvent(
                    command.command_id,
                    WorkRequeued(
                        work.work_id,
                        work.trigger,
                        work.attempt,
                        command.failure,
                        next_trigger,
                        work.attempt + 1,
                    ),
                ),
            )
        )
    if context.trigger is not None:
        return Rejected("VALIDATION_FAILED")
    return Accepted(
        (
            PendingDialogueEvent(command.command_id, failed),
            PendingDialogueEvent(
                command.command_id,
                WorkDeadLettered(
                    work.work_id,
                    work.trigger,
                    work.attempt,
                    command.failure,
                    _allowed_recovery_actions(state),
                ),
            ),
        )
    )


def _dead_work_target(
    state: DialogueState,
    work_id: str,
) -> tuple[CurrentWorkState, TopicRunState | None, bool] | None:
    if (
        state.session_work is not None
        and state.session_work.status is WorkStatus.DEAD_LETTER
        and state.session_work.work_id == work_id
    ):
        return state.session_work, None, False
    if (
        state.active_topic is not None
        and state.active_topic.work is not None
        and state.active_topic.work.status is WorkStatus.DEAD_LETTER
        and state.active_topic.work.work_id == work_id
    ):
        return state.active_topic.work, state.active_topic, False
    matches = tuple(
        topic
        for topic in state.paused_topics
        if topic.work is not None
        and topic.work.status is WorkStatus.DEAD_LETTER
        and topic.work.work_id == work_id
    )
    if len(matches) != 1:
        return None
    topic = matches[0]
    return cast(CurrentWorkState, topic.work), topic, True


def _recover_work(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if type(command) is not RecoverWork:
        return Rejected("TOPIC_STATE_CONFLICT")
    if (
        not _valid_text(command.dead_work_id)
        or type(command.action) is not WorkRecoveryAction
    ):
        return Rejected("VALIDATION_FAILED")
    target = _dead_work_target(state, command.dead_work_id)
    if target is None:
        return Rejected("TOPIC_STATE_CONFLICT")
    work, topic, paused = target
    if (
        command.action not in work.allowed_recovery_actions
        or context.trigger is None
    ):
        return Rejected("VALIDATION_FAILED")
    trigger = context.trigger
    if command.action is WorkRecoveryAction.RETRY:
        valid_trigger = _retry_trigger_matches(work.trigger, trigger)
        valid_evidence = (
            context.evidence.evidence_digest == work.trigger.evidence_digest
        )
    else:
        valid_trigger = (
            topic is not None
            and trigger.work_id != work.trigger.work_id
            and trigger.kind is TriggerKind.REGROUND
            and trigger.parent_turn_id == work.trigger.parent_turn_id
            and trigger.contract_digest == work.trigger.contract_digest
            and trigger.evidence_digest == context.evidence.evidence_digest
        )
        valid_evidence = topic is not None
    if not valid_trigger or not valid_evidence:
        return Rejected("VALIDATION_FAILED")
    if paused and (state.phase is not ConversationPhase.NONE or state.active_topic):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not paused and state.phase is not ConversationPhase.RECOVERABLE_ERROR:
        return Rejected("TOPIC_STATE_CONFLICT")
    return _accept(
        command.command_id,
        WorkRecoveryRequested(
            work.work_id,
            command.action,
            trigger,
            context.evidence,
        ),
    )


TRANSITION_TABLE: dict[type, Handler] = {
    StartSession: _start_session,
    PresentCandidates: _present_candidates,
    SelectTopic: _select_topic,
    SubmitCustomTopic: _submit_custom_topic,
    RequestTopicClarification: _request_topic_clarification,
    AnswerTopicClarification: _answer_topic_clarification,
    StartTopic: _start_topic,
    CommitAgentTurn: _commit_agent_turn,
    SubmitLearnerTurn: _submit_turn,
    SetLens: _set_lens,
    RequestHelp: _request_help,
    PauseTopic: _pause_topic,
    SwitchTopic: _switch_topic,
    ResumeTopic: _resume_topic,
    ReportWorkFailure: _report_work_failure,
    RecoverWork: _recover_work,
}


def _valid_decision_context(context: object) -> bool:
    return (
        type(context) is DecisionContext
        and _valid_positive_int(context.registry_generation)
        and _valid_evidence(context.evidence)
        and (
            context.trigger is None
            or is_canonical_trigger_binding(context.trigger)
        )
    )


def decide(
    state: DialogueState,
    command: DialogueCommand,
    context: DecisionContext,
) -> Decision:
    """Validate a command against one immutable snapshot."""
    if type(state) is not DialogueState:
        return Rejected("STATE_INVARIANT_VIOLATION")
    try:
        _validate(state)
    except ValueError:
        return Rejected("STATE_INVARIANT_VIOLATION")
    if not _valid_decision_context(context):
        return Rejected("VALIDATION_FAILED")
    if context.registry_generation != state.registry_generation:
        return Rejected("SESSION_DEACTIVATED")
    handler = TRANSITION_TABLE.get(type(command))
    if handler is None:
        return Rejected("TOPIC_STATE_CONFLICT")
    if not is_protocol_id(command.command_id):
        return Rejected("VALIDATION_FAILED")
    if (
        type(command) is not StartSession
        and state.lifecycle is not SessionLifecycle.OPEN
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    return handler(state, command, context)


def decide_fenced_quiesce(
    state: DialogueState,
    command: PrepareSessionDeactivation,
    context: FencedQuiesceContext,
) -> Decision:
    """Prepare a fenced Session for handoff without restoring normal writes."""
    try:
        validate_state(state)
    except ValueError:
        return Rejected("STATE_INVARIANT_VIOLATION")
    if (
        type(command) is not PrepareSessionDeactivation
        or not _valid_text(command.command_id)
        or type(context) is not FencedQuiesceContext
        or not _valid_text(context.handoff_id)
        or not _valid_text(context.source_session_id)
        or not _valid_positive_int(context.source_generation)
        or not _valid_positive_int(context.fence_generation)
    ):
        return Rejected("VALIDATION_FAILED")
    if (
        state.lifecycle not in {SessionLifecycle.NEW, SessionLifecycle.OPEN}
        or context.source_session_id != state.session_id
        or context.source_generation != state.registry_generation
        or context.fence_generation != context.source_generation + 1
    ):
        return Rejected("SESSION_DEACTIVATED")
    if state.lifecycle is SessionLifecycle.NEW:
        if state.phase is not ConversationPhase.NONE:
            return Rejected("STATE_INVARIANT_VIOLATION")
        return _accept(
            command.command_id,
            SessionDeactivationPrepared(
                context.handoff_id,
                context.fence_generation,
                None,
            ),
        )
    if state.active_topic is not None:
        return _accept(
            command.command_id,
            TopicPaused(
                state.active_topic.topic_run_id,
                PauseCause.SESSION_DEACTIVATION,
                context.handoff_id,
                context.fence_generation,
            ),
        )
    if state.phase is ConversationPhase.NONE:
        return Accepted(())
    return _accept(
        command.command_id,
        SessionDeactivationPrepared(
            context.handoff_id,
            context.fence_generation,
            state.session_unresolved_trigger,
        ),
    )


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("ILLEGAL_EVENT_TRANSITION")


def _validate(state: DialogueState) -> None:
    validate_state(state)


def validate_state(state: DialogueState) -> None:
    """Validate one canonical dialogue snapshot without performing I/O."""
    if type(state) is not DialogueState:
        raise ValueError("STATE_INVARIANT_VIOLATION")
    topic = state.active_topic
    if (
        not is_protocol_id(state.session_id)
        or not _valid_positive_int(state.registry_generation)
        or not _valid_nonnegative_int(state.sequence)
        or not _valid_nonnegative_int(state.conversation_version)
        or type(state.lifecycle) is not SessionLifecycle
        or type(state.phase) is not ConversationPhase
        or type(state.candidates) is not tuple
        or type(state.paused_topics) is not tuple
        or (
            state.selected_candidate is not None
            and not _valid_text(state.selected_candidate)
        )
        or (
            state.topic_clarification is not None
            and (
                type(state.topic_clarification) is not TopicClarification
                or not _valid_text(state.topic_clarification.question_id)
                or not _valid_text(state.topic_clarification.question)
                or (
                    state.topic_clarification.answer is not None
                    and not _valid_text(state.topic_clarification.answer)
                )
            )
        )
        or (
            state.session_work is not None
            and not _valid_current_work(
                state.session_work,
                state.session_id,
                state.sequence,
            )
        )
        or (topic is not None and type(topic) is not TopicRunState)
        or any(
            type(item) is not TopicRunState
            for item in state.paused_topics
        )
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.lifecycle is SessionLifecycle.NEW and (
        state.phase is not ConversationPhase.NONE
        or topic is not None
        or state.session_work is not None
        or state.paused_topics
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.lifecycle is SessionLifecycle.ENDED and (
        state.phase is not ConversationPhase.NONE
        or topic is not None
        or state.session_work is not None
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.phase in {
        ConversationPhase.NONE,
        ConversationPhase.CHOOSING_TOPIC,
        ConversationPhase.CLARIFYING_TOPIC,
    } and topic is not None:
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if (
        state.phase is ConversationPhase.CHOOSING_TOPIC
        and not _valid_candidates(state.candidates)
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.phase is ConversationPhase.CHOOSING_TOPIC and state.selected_candidate:
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.phase is ConversationPhase.CLARIFYING_TOPIC and (
        state.selected_candidate is None
        or state.topic_clarification is None
        or state.topic_clarification.answer is not None
        or state.session_work is not None
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if (
        state.phase is not ConversationPhase.CHOOSING_TOPIC
        and state.candidates
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.phase is ConversationPhase.AWAITING_USER:
        if (
            topic is None
            or topic.current_agent_turn is None
            or topic.work is not None
            or topic.evidence_health not in PUBLISHABLE_EVIDENCE
        ):
            raise ValueError("STATE_INVARIANT_VIOLATION")
    active_work = topic.work if topic is not None else None
    if state.phase is ConversationPhase.WAITING_HOST:
        if sum(
            item is not None
            for item in (state.session_work, active_work)
        ) != 1:
            raise ValueError("STATE_INVARIANT_VIOLATION")
        work = state.session_work if state.session_work is not None else active_work
        if (
            work is None
            or work.status not in {WorkStatus.QUEUED, WorkStatus.FAILED}
            or (topic is not None and topic.current_agent_turn is not None)
        ):
            raise ValueError("STATE_INVARIANT_VIOLATION")
    elif state.phase is ConversationPhase.RECOVERABLE_ERROR:
        current_works = tuple(
            item
            for item in (state.session_work, active_work)
            if item is not None
        )
        if (
            len(current_works) != 1
            or current_works[0].status is not WorkStatus.DEAD_LETTER
        ):
            raise ValueError("STATE_INVARIANT_VIOLATION")
        if topic is not None and topic.current_agent_turn is not None:
            raise ValueError("STATE_INVARIANT_VIOLATION")
    elif state.session_work is not None:
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.session_work is not None:
        session_kind = state.session_work.trigger.kind
        if (
            session_kind not in {
                TriggerKind.TOPIC_CANDIDATES,
                TriggerKind.TOPIC_SELECTION,
            }
            or not _valid_trigger(
                state.session_work.trigger,
                session_kind,
                None,
                state.session_work.trigger.evidence_digest,
            )
            or (
                session_kind is TriggerKind.TOPIC_CANDIDATES
                and state.selected_candidate is not None
            )
            or (
                session_kind is TriggerKind.TOPIC_SELECTION
                and state.selected_candidate is None
            )
        ):
            raise ValueError("STATE_INVARIANT_VIOLATION")
    elif (
        state.selected_candidate is not None
        and state.phase is not ConversationPhase.CLARIFYING_TOPIC
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.topic_clarification is not None and (
        state.selected_candidate is None
        or topic is not None
        or state.phase
        not in {ConversationPhase.CLARIFYING_TOPIC, ConversationPhase.WAITING_HOST}
        or (
            state.phase is ConversationPhase.WAITING_HOST
            and state.topic_clarification.answer is None
        )
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if (
        state.session_work is not None
        and state.session_work.status is WorkStatus.DEAD_LETTER
        and state.session_work.allowed_recovery_actions
        != (WorkRecoveryAction.RETRY,)
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if topic is not None and not _valid_topic_state(
        topic,
        TopicLifecycle.ACTIVE,
        state.session_id,
        state.sequence,
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if not all(
        _valid_topic_state(
            item,
            TopicLifecycle.PAUSED,
            state.session_id,
            state.sequence,
        )
        for item in state.paused_topics
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if topic and any(
        item.topic_run_id == topic.topic_run_id
        for item in state.paused_topics
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    paused_ids = tuple(item.topic_run_id for item in state.paused_topics)
    if len(paused_ids) != len(set(paused_ids)):
        raise ValueError("STATE_INVARIANT_VIOLATION")


EVENT_PAYLOAD_TYPES = frozenset(
    {
        SessionStarted,
        CandidatesPresented,
        TopicSelectionSubmitted,
        TopicClarificationRequested,
        TopicClarificationAnswered,
        LensChanged,
        HelpRequested,
        TopicStarted,
        AgentTurnCommitted,
        LearnerTurnSubmitted,
        TopicPaused,
        TopicSwitchRequested,
        SessionDeactivationPrepared,
        TopicResumed,
        WorkFailed,
        WorkRequeued,
        WorkDeadLettered,
        WorkRecoveryRequested,
    }
)


def _valid_event_payload_shape(payload: object) -> bool:
    if type(payload) is SessionStarted:
        return is_canonical_trigger_binding(payload.candidate_trigger)
    if type(payload) is CandidatesPresented:
        return _valid_candidates_shape(
            payload.candidates
        ) and is_canonical_trigger_binding(payload.trigger)
    if type(payload) is TopicSelectionSubmitted:
        return _valid_text(payload.candidate) and is_canonical_trigger_binding(
            payload.next_trigger
        )
    if type(payload) is TopicClarificationRequested:
        return (
            _valid_text(payload.question_id)
            and _valid_text(payload.question)
            and is_canonical_trigger_binding(payload.selection_trigger)
        )
    if type(payload) is TopicClarificationAnswered:
        return (
            _valid_text(payload.question_id)
            and _valid_text(payload.answer)
            and is_canonical_trigger_binding(payload.next_trigger)
        )
    if type(payload) is LensChanged:
        return (
            _valid_text(payload.topic_run_id)
            and type(payload.lens) is Lens
            and is_canonical_trigger_binding(payload.next_trigger)
        )
    if type(payload) is HelpRequested:
        return (
            _valid_text(payload.topic_run_id)
            and is_protocol_id(payload.question_id)
            and is_canonical_trigger_binding(payload.next_trigger)
        )
    if type(payload) is TopicStarted:
        return (
            _valid_contract_shape(payload.contract)
            and _valid_evidence_shape(payload.evidence)
            and is_canonical_trigger_binding(payload.initial_trigger)
            and (
                payload.selected_candidate is None
                or _valid_text(payload.selected_candidate)
            )
            and (
                payload.selection_trigger is None
                or is_canonical_trigger_binding(payload.selection_trigger)
            )
        )
    if type(payload) is AgentTurnCommitted:
        return (
            _valid_agent_turn_shape(payload.result)
            and is_canonical_trigger_binding(payload.trigger)
            and _valid_evidence_shape(payload.evidence)
        )
    if type(payload) is LearnerTurnSubmitted:
        return (
            _valid_text(payload.question_id)
            and _valid_text(payload.learner_turn_id)
            and _valid_text(payload.text)
            and is_canonical_trigger_binding(payload.next_trigger)
        )
    if type(payload) is TopicPaused:
        if (
            not _valid_text(payload.topic_run_id)
            or type(payload.cause) is not PauseCause
        ):
            return False
        if payload.cause is PauseCause.USER:
            return payload.handoff_id is None and payload.fence_generation is None
        return (
            _valid_text(payload.handoff_id)
            and _valid_positive_int(payload.fence_generation)
        )
    if type(payload) is TopicSwitchRequested:
        return _valid_text(
            payload.topic_run_id
        ) and is_canonical_trigger_binding(payload.candidate_trigger)
    if type(payload) is SessionDeactivationPrepared:
        return (
            _valid_text(payload.handoff_id)
            and _valid_positive_int(payload.fence_generation)
            and (
                payload.superseded_trigger is None
                or is_canonical_trigger_binding(payload.superseded_trigger)
            )
        )
    if type(payload) is TopicResumed:
        return (
            _valid_text(payload.topic_run_id)
            and type(payload.requires_reground) is bool
            and (
                payload.resumed_trigger is None
                or is_canonical_trigger_binding(payload.resumed_trigger)
            )
            and _valid_evidence_shape(payload.evidence)
        )
    if type(payload) is WorkFailed:
        return (
            _valid_text(payload.work_id)
            and is_canonical_trigger_binding(payload.trigger)
            and _valid_positive_int(payload.attempt)
            and _valid_work_failure(payload.failure)
        )
    if type(payload) is WorkRequeued:
        return (
            _valid_text(payload.failed_work_id)
            and is_canonical_trigger_binding(payload.failed_trigger)
            and _valid_positive_int(payload.failed_attempt)
            and _valid_work_failure(payload.failure)
            and is_canonical_trigger_binding(payload.next_trigger)
            and _valid_positive_int(payload.next_attempt)
        )
    if type(payload) is WorkDeadLettered:
        return (
            _valid_text(payload.failed_work_id)
            and is_canonical_trigger_binding(payload.failed_trigger)
            and _valid_positive_int(payload.failed_attempt)
            and _valid_work_failure(payload.failure)
            and type(payload.allowed_actions) is tuple
            and bool(payload.allowed_actions)
            and all(
                type(item) is WorkRecoveryAction
                for item in payload.allowed_actions
            )
            and len(payload.allowed_actions) == len(set(payload.allowed_actions))
        )
    if type(payload) is WorkRecoveryRequested:
        return (
            _valid_text(payload.dead_work_id)
            and type(payload.action) is WorkRecoveryAction
            and is_canonical_trigger_binding(payload.next_trigger)
            and _valid_evidence_shape(payload.evidence)
        )
    return False


def _valid_committed_event_envelope(event: object) -> bool:
    try:
        return (
            type(event) is CommittedDialogueEvent
            and is_protocol_id(event.event_id)
            and _valid_positive_int(event.sequence)
            and _valid_nonnegative_int(event.from_version)
            and _valid_nonnegative_int(event.to_version)
            and is_protocol_id(event.command_id)
            and type(event.payload) in EVENT_PAYLOAD_TYPES
            and _valid_event_payload_shape(event.payload)
        )
    except (AttributeError, TypeError):
        return False


def reduce(
    state: DialogueState, event: CommittedDialogueEvent
) -> DialogueState:
    """Apply one committed fact or fail closed without mutation."""
    _validate(state)
    if not _valid_committed_event_envelope(event):
        raise ValueError("INVALID_EVENT_ENVELOPE")
    if event.sequence != state.sequence + 1:
        raise ValueError("EVENT_SEQUENCE_GAP")
    if event.from_version != state.conversation_version:
        raise ValueError("EVENT_VERSION_CONFLICT")
    if event.to_version != event.from_version + conversation_version_delta(
        event.payload
    ):
        raise ValueError("EVENT_VERSION_CONFLICT")

    payload = event.payload
    next_state = replace(
        state,
        sequence=event.sequence,
        conversation_version=event.to_version,
    )
    if type(payload) is SessionStarted:
        _require(
            state.lifecycle is SessionLifecycle.NEW
            and state.phase is ConversationPhase.NONE
            and state.session_work is None
            and _valid_trigger(
                payload.candidate_trigger,
                TriggerKind.TOPIC_CANDIDATES,
                None,
                payload.candidate_trigger.evidence_digest,
            )
        )
        next_state = replace(
            next_state,
            lifecycle=SessionLifecycle.OPEN,
            phase=ConversationPhase.WAITING_HOST,
            session_work=_queued_work_from_event(
                state.session_id,
                event,
                payload.candidate_trigger,
            ),
        )
    elif type(payload) is CandidatesPresented:
        _require(
            state.phase is ConversationPhase.WAITING_HOST
            and state.active_topic is None
            and state.session_work is not None
            and state.session_work.status is WorkStatus.QUEUED
            and _valid_candidates(payload.candidates)
            and payload.trigger == state.session_work.trigger
        )
        next_state = replace(
            next_state,
            candidates=payload.candidates,
            phase=ConversationPhase.CHOOSING_TOPIC,
            session_work=None,
        )
    elif type(payload) is TopicSelectionSubmitted:
        _require(
            state.phase is ConversationPhase.CHOOSING_TOPIC
            and state.active_topic is None
            and state.session_work is None
            and state.selected_candidate is None
            and _valid_text(payload.candidate)
            and _valid_trigger(
                payload.next_trigger,
                TriggerKind.TOPIC_SELECTION,
                None,
                payload.next_trigger.evidence_digest,
            )
        )
        next_state = replace(
            next_state,
            candidates=(),
            selected_candidate=payload.candidate,
            phase=ConversationPhase.WAITING_HOST,
            session_work=_queued_work_from_event(
                state.session_id,
                event,
                payload.next_trigger,
            ),
            topic_clarification=None,
        )
    elif type(payload) is TopicClarificationRequested:
        _require(
            state.phase is ConversationPhase.WAITING_HOST
            and state.active_topic is None
            and state.selected_candidate is not None
            and state.session_work is not None
            and state.session_work.status is WorkStatus.QUEUED
            and state.session_work.trigger.kind is TriggerKind.TOPIC_SELECTION
            and payload.selection_trigger == state.session_work.trigger
        )
        next_state = replace(
            next_state,
            phase=ConversationPhase.CLARIFYING_TOPIC,
            session_work=None,
            topic_clarification=TopicClarification(
                payload.question_id,
                payload.question,
            ),
        )
    elif type(payload) is TopicClarificationAnswered:
        clarification = state.topic_clarification
        _require(
            state.phase is ConversationPhase.CLARIFYING_TOPIC
            and state.active_topic is None
            and state.selected_candidate is not None
            and state.session_work is None
            and clarification is not None
            and clarification.answer is None
            and clarification.question_id == payload.question_id
            and _valid_trigger(
                payload.next_trigger,
                TriggerKind.TOPIC_SELECTION,
                None,
                payload.next_trigger.evidence_digest,
            )
            and payload.next_trigger.parent_turn_id == payload.question_id
        )
        next_state = replace(
            next_state,
            phase=ConversationPhase.WAITING_HOST,
            session_work=_queued_work_from_event(
                state.session_id,
                event,
                payload.next_trigger,
            ),
            topic_clarification=replace(
                cast(TopicClarification, clarification),
                answer=payload.answer,
            ),
        )
    elif type(payload) is TopicStarted:
        legacy_selection = (
            state.phase is ConversationPhase.CHOOSING_TOPIC
            and state.session_work is None
            and state.selected_candidate is None
            and payload.selected_candidate is None
            and payload.selection_trigger is None
        )
        durable_selection = (
            state.phase is ConversationPhase.WAITING_HOST
            and state.session_work is not None
            and state.session_work.status is WorkStatus.QUEUED
            and state.session_work.trigger.kind is TriggerKind.TOPIC_SELECTION
            and payload.selected_candidate == state.selected_candidate
            and payload.selection_trigger == state.session_work.trigger
        )
        _require(
            state.active_topic is None
            and (legacy_selection or durable_selection)
        )
        contract = payload.contract
        _require(_valid_contract(contract) and _valid_evidence(payload.evidence))
        expected_kind = (
            TriggerKind.INITIAL_TURN
            if payload.evidence.health in PUBLISHABLE_EVIDENCE
            else TriggerKind.REGROUND
        )
        _require(
            _valid_trigger(
                payload.initial_trigger,
                expected_kind,
                contract.contract_digest,
                payload.evidence.evidence_digest,
            )
        )
        topic = TopicRunState(
            topic_run_id=contract.topic_run_id,
            contract=contract,
            lifecycle=TopicLifecycle.ACTIVE,
            evidence_health=payload.evidence.health,
            evidence_digest=payload.evidence.evidence_digest,
            exact_recheck_fingerprint=payload.evidence.exact_recheck_fingerprint,
            gates=tuple(
                GateAssessment(requirement.gate_id)
                for requirement in contract.gates
            ),
            work=_queued_work_from_event(
                state.session_id,
                event,
                payload.initial_trigger,
            ),
        )
        next_state = replace(
            next_state,
            active_topic=topic,
            candidates=(),
            selected_candidate=None,
            topic_clarification=None,
            session_work=None,
            phase=ConversationPhase.WAITING_HOST,
        )
    elif type(payload) is AgentTurnCommitted:
        _require(
            state.phase is ConversationPhase.WAITING_HOST
            and state.active_topic is not None
            and state.active_topic.open_question_id is None
            and state.active_topic.work is not None
            and state.active_topic.work.status is WorkStatus.QUEUED
            and state.active_topic.evidence_health in PUBLISHABLE_EVIDENCE
            and _valid_evidence(payload.evidence)
            and payload.trigger == state.active_topic.work.trigger
            and payload.evidence.health is state.active_topic.evidence_health
            and payload.evidence.evidence_digest
            == state.active_topic.evidence_digest
            and payload.evidence.exact_recheck_fingerprint
            == state.active_topic.exact_recheck_fingerprint
            and _valid_agent_turn(
                state.active_topic.contract,
                payload.result,
                payload.evidence,
                frozenset(state.active_topic.learner_turn_ids),
            )
        )
        topic = cast(TopicRunState, state.active_topic)
        _require(
            _valid_model_updates(
                topic.learner_model,
                payload.result.learner_model_delta,
            )
        )
        learner_model = _apply_model_updates(
            topic.learner_model,
            payload.result.learner_model_delta,
        )
        next_state = replace(
            next_state,
            active_topic=replace(
                topic,
                learner_model=learner_model,
                gates=payload.result.gate_assessments,
                current_agent_turn=payload.result,
                work=None,
            ),
            phase=ConversationPhase.AWAITING_USER,
        )
    elif type(payload) is LearnerTurnSubmitted:
        _require(
            state.phase is ConversationPhase.AWAITING_USER
            and state.active_topic is not None
            and state.active_topic.open_question_id == payload.question_id
            and state.active_topic.work is None
            and _valid_text(payload.question_id)
            and _valid_text(payload.learner_turn_id)
            and _valid_text(payload.text)
            and payload.learner_turn_id
            not in state.active_topic.learner_turn_ids
            and _valid_trigger(
                payload.next_trigger,
                TriggerKind.LEARNER_REPLY,
                state.active_topic.contract.contract_digest,
                state.active_topic.evidence_digest,
            )
            and payload.next_trigger.parent_turn_id == payload.learner_turn_id
        )
        topic = cast(TopicRunState, state.active_topic)
        next_state = replace(
            next_state,
            active_topic=replace(
                topic,
                current_agent_turn=None,
                work=_queued_work_from_event(
                    state.session_id,
                    event,
                    payload.next_trigger,
                ),
                learner_turn_ids=(
                    *topic.learner_turn_ids,
                    payload.learner_turn_id,
                ),
                last_learner_turn_id=payload.learner_turn_id,
                last_learner_text=payload.text,
            ),
            phase=ConversationPhase.WAITING_HOST,
        )
    elif type(payload) is LensChanged:
        lens_topic = state.active_topic
        _require(
            lens_topic is not None
            and state.phase
            in {ConversationPhase.AWAITING_USER, ConversationPhase.WAITING_HOST}
            and payload.topic_run_id == lens_topic.topic_run_id
            and payload.lens is not lens_topic.lens
            and _valid_trigger(
                payload.next_trigger,
                TriggerKind.LENS_CHANGED,
                lens_topic.contract.contract_digest,
                lens_topic.evidence_digest,
            )
        )
        lens_topic = cast(TopicRunState, lens_topic)
        expected_parent = (
            lens_topic.current_agent_turn.question_id
            if state.phase is ConversationPhase.AWAITING_USER
            and lens_topic.current_agent_turn is not None
            else (
                None
                if lens_topic.work is None
                else lens_topic.work.trigger.parent_turn_id
            )
        )
        _require(payload.next_trigger.parent_turn_id == expected_parent)
        next_state = replace(
            next_state,
            active_topic=replace(
                lens_topic,
                current_lens=payload.lens,
                current_agent_turn=None,
                work=_queued_work_from_event(
                    state.session_id,
                    event,
                    payload.next_trigger,
                ),
            ),
            phase=ConversationPhase.WAITING_HOST,
        )
    elif type(payload) is HelpRequested:
        help_topic = state.active_topic
        _require(
            help_topic is not None
            and state.phase is ConversationPhase.AWAITING_USER
            and help_topic.open_question_id == payload.question_id
            and payload.topic_run_id == help_topic.topic_run_id
            and _valid_trigger(
                payload.next_trigger,
                TriggerKind.HELP,
                help_topic.contract.contract_digest,
                help_topic.evidence_digest,
            )
            and payload.next_trigger.parent_turn_id == payload.question_id
        )
        help_topic = cast(TopicRunState, help_topic)
        next_state = replace(
            next_state,
            active_topic=replace(
                help_topic,
                current_agent_turn=None,
                work=_queued_work_from_event(
                    state.session_id,
                    event,
                    payload.next_trigger,
                ),
            ),
            phase=ConversationPhase.WAITING_HOST,
        )
    elif type(payload) is TopicPaused:
        _require(
            _valid_text(payload.topic_run_id)
            and type(payload.cause) is PauseCause
            and state.active_topic is not None
            and state.active_topic.topic_run_id == payload.topic_run_id
        )
        if payload.cause is PauseCause.USER:
            _require(
                payload.handoff_id is None
                and payload.fence_generation is None
            )
        else:
            _require(
                _valid_text(payload.handoff_id)
                and payload.fence_generation == state.registry_generation + 1
            )
        topic = cast(TopicRunState, state.active_topic)
        paused_work = topic.work
        if paused_work is not None and paused_work.status in {
            WorkStatus.QUEUED,
            WorkStatus.FAILED,
        }:
            paused_work = replace(paused_work, status=WorkStatus.SUPERSEDED)
        paused = replace(
            topic,
            lifecycle=TopicLifecycle.PAUSED,
            work=paused_work,
        )
        next_state = replace(
            next_state,
            active_topic=None,
            paused_topics=(*next_state.paused_topics, paused),
            phase=ConversationPhase.NONE,
        )
    elif type(payload) is TopicSwitchRequested:
        _require(
            state.active_topic is not None
            and state.active_topic.topic_run_id == payload.topic_run_id
            and _valid_trigger(
                payload.candidate_trigger,
                TriggerKind.TOPIC_CANDIDATES,
                None,
                payload.candidate_trigger.evidence_digest,
            )
        )
        topic = cast(TopicRunState, state.active_topic)
        paused_work = topic.work
        if paused_work is not None and paused_work.status in {
            WorkStatus.QUEUED,
            WorkStatus.FAILED,
        }:
            paused_work = replace(paused_work, status=WorkStatus.SUPERSEDED)
        paused = replace(
            topic,
            lifecycle=TopicLifecycle.PAUSED,
            work=paused_work,
        )
        next_state = replace(
            next_state,
            active_topic=None,
            paused_topics=(*state.paused_topics, paused),
            candidates=(),
            selected_candidate=None,
            topic_clarification=None,
            session_work=_queued_work_from_event(
                state.session_id,
                event,
                payload.candidate_trigger,
            ),
            phase=ConversationPhase.WAITING_HOST,
        )
    elif type(payload) is SessionDeactivationPrepared:
        valid_new_source = (
            state.lifecycle is SessionLifecycle.NEW
            and state.phase is ConversationPhase.NONE
            and state.session_work is None
        )
        valid_open_source = (
            state.lifecycle is SessionLifecycle.OPEN
            and state.phase is not ConversationPhase.NONE
        )
        _require(
            (valid_new_source or valid_open_source)
            and state.active_topic is None
            and _valid_text(payload.handoff_id)
            and payload.fence_generation == state.registry_generation + 1
            and payload.superseded_trigger == state.session_unresolved_trigger
        )
        next_state = replace(
            next_state,
            lifecycle=SessionLifecycle.OPEN,
            phase=ConversationPhase.NONE,
            candidates=(),
            selected_candidate=None,
            topic_clarification=None,
            session_work=None,
        )
    elif type(payload) is TopicResumed:
        _require(
            _valid_text(payload.topic_run_id)
            and type(payload.requires_reground) is bool
            and _valid_evidence(payload.evidence)
            and state.phase is ConversationPhase.NONE
            and state.active_topic is None
        )
        matches = tuple(
            item for item in state.paused_topics
            if item.topic_run_id == payload.topic_run_id
        )
        _require(len(matches) == 1)
        paused = matches[0]
        remaining = tuple(
            item for item in state.paused_topics
            if item.topic_run_id != payload.topic_run_id
        )
        evidence_changed = (
            payload.evidence.health is not paused.evidence_health
            or payload.evidence.evidence_digest != paused.evidence_digest
            or payload.evidence.exact_recheck_fingerprint
            != paused.exact_recheck_fingerprint
        )
        active = replace(
            paused,
            lifecycle=TopicLifecycle.ACTIVE,
            evidence_health=payload.evidence.health,
            evidence_digest=payload.evidence.evidence_digest,
            exact_recheck_fingerprint=payload.evidence.exact_recheck_fingerprint,
        )
        derived_reground = evidence_changed or payload.evidence.health in {
            EvidenceHealth.STALE,
            EvidenceHealth.DISPUTED,
            EvidenceHealth.UNAVAILABLE,
        }
        _require(payload.requires_reground is derived_reground)
        if derived_reground:
            _require(
                _valid_trigger(
                    payload.resumed_trigger,
                    TriggerKind.REGROUND,
                    paused.contract.contract_digest,
                    payload.evidence.evidence_digest,
                )
            )
            active = replace(
                active,
                current_agent_turn=None,
                work=_queued_work_from_event(
                    state.session_id,
                    event,
                    cast(TriggerBinding, payload.resumed_trigger),
                ),
                gates=tuple(
                    replace(item, status=GateStatus.STALE)
                    if item.status
                    in {
                        GateStatus.EMERGING,
                        GateStatus.ASSISTED,
                        GateStatus.SUPPORTED,
                    }
                    else item
                    for item in active.gates
                ),
                learner_model=tuple(
                    replace(item, status=InsightStatus.STALE)
                    if item.status is InsightStatus.CONFIRMED
                    else item
                    for item in active.learner_model
                ),
            )
            phase = ConversationPhase.WAITING_HOST
        elif active.open_question_id is not None:
            _require(payload.resumed_trigger is None)
            _require(active.work is None)
            phase = ConversationPhase.AWAITING_USER
        elif active.work is not None:
            _require(payload.resumed_trigger is not None)
            _require(active.work.status is WorkStatus.SUPERSEDED)
            resumed_trigger = cast(TriggerBinding, payload.resumed_trigger)
            saved_trigger = active.work.trigger
            _require(
                _valid_trigger(
                    resumed_trigger,
                    saved_trigger.kind,
                    active.contract.contract_digest,
                    payload.evidence.evidence_digest,
                )
                and resumed_trigger.work_id != saved_trigger.work_id
                and resumed_trigger.parent_turn_id
                == saved_trigger.parent_turn_id
                and resumed_trigger.input_digest
                == saved_trigger.input_digest
            )
            active = replace(
                active,
                work=_queued_work_from_event(
                    state.session_id,
                    event,
                    resumed_trigger,
                ),
            )
            phase = ConversationPhase.WAITING_HOST
        else:
            raise ValueError("STATE_INVARIANT_VIOLATION")
        next_state = replace(
            next_state,
            active_topic=active,
            paused_topics=remaining,
            phase=phase,
        )
    elif type(payload) is WorkFailed:
        work = _current_nonterminal_work(state)
        _require(
            state.phase is ConversationPhase.WAITING_HOST
            and work is not None
            and work.status is WorkStatus.QUEUED
            and payload.work_id == work.work_id
            and payload.trigger == work.trigger
            and payload.attempt == work.attempt
            and _valid_work_failure(payload.failure)
        )
        work = cast(CurrentWorkState, work)
        next_state = _replace_work(
            next_state,
            work,
            replace(
                work,
                status=WorkStatus.FAILED,
                last_failure=payload.failure,
            ),
        )
    elif type(payload) is WorkRequeued:
        work = _current_nonterminal_work(state)
        _require(
            state.phase is ConversationPhase.WAITING_HOST
            and work is not None
            and work.status is WorkStatus.FAILED
            and payload.failed_work_id == work.work_id
            and payload.failed_trigger == work.trigger
            and payload.failed_attempt == work.attempt
            and payload.failure == work.last_failure
            and payload.next_attempt == work.attempt + 1
            and _automatic_retry_allowed(work)
            and _retry_trigger_matches(work.trigger, payload.next_trigger)
        )
        work = cast(CurrentWorkState, work)
        next_state = _replace_work(
            next_state,
            work,
            _queued_work_from_event(
                state.session_id,
                event,
                payload.next_trigger,
                attempt=payload.next_attempt,
                last_failure=payload.failure,
            ),
        )
    elif type(payload) is WorkDeadLettered:
        work = _current_nonterminal_work(state)
        _require(
            state.phase is ConversationPhase.WAITING_HOST
            and work is not None
            and work.status is WorkStatus.FAILED
            and payload.failed_work_id == work.work_id
            and payload.failed_trigger == work.trigger
            and payload.failed_attempt == work.attempt
            and payload.failure == work.last_failure
            and not _automatic_retry_allowed(work)
            and payload.allowed_actions == _allowed_recovery_actions(state)
        )
        work = cast(CurrentWorkState, work)
        next_state = _replace_work(
            next_state,
            work,
            replace(
                work,
                status=WorkStatus.DEAD_LETTER,
                allowed_recovery_actions=payload.allowed_actions,
            ),
        )
        next_state = replace(
            next_state,
            phase=ConversationPhase.RECOVERABLE_ERROR,
        )
    elif type(payload) is WorkRecoveryRequested:
        target = _dead_work_target(state, payload.dead_work_id)
        if target is None:
            raise ValueError("ILLEGAL_EVENT_TRANSITION")
        dead_work, target_topic, is_paused = target
        _require(
            payload.action in dead_work.allowed_recovery_actions
            and _valid_evidence(payload.evidence)
        )
        if payload.action is WorkRecoveryAction.RETRY:
            _require(
                _retry_trigger_matches(dead_work.trigger, payload.next_trigger)
                and payload.evidence.evidence_digest
                == dead_work.trigger.evidence_digest
            )
        else:
            _require(
                target_topic is not None
                and payload.next_trigger.work_id != dead_work.trigger.work_id
                and payload.next_trigger.kind is TriggerKind.REGROUND
                and payload.next_trigger.parent_turn_id
                == dead_work.trigger.parent_turn_id
                and payload.next_trigger.contract_digest
                == dead_work.trigger.contract_digest
                and payload.next_trigger.evidence_digest
                == payload.evidence.evidence_digest
            )
        recovered_work = _queued_work_from_event(
            state.session_id,
            event,
            payload.next_trigger,
        )
        if target_topic is None:
            _require(
                not is_paused
                and state.phase is ConversationPhase.RECOVERABLE_ERROR
                and state.session_work == dead_work
            )
            next_state = replace(
                next_state,
                session_work=recovered_work,
                phase=ConversationPhase.WAITING_HOST,
            )
        else:
            recovered_topic = replace(
                target_topic,
                lifecycle=TopicLifecycle.ACTIVE,
                evidence_health=payload.evidence.health,
                evidence_digest=payload.evidence.evidence_digest,
                exact_recheck_fingerprint=(
                    payload.evidence.exact_recheck_fingerprint
                ),
                current_agent_turn=None,
                work=recovered_work,
            )
            if payload.action is WorkRecoveryAction.REGROUND:
                recovered_topic = replace(
                    recovered_topic,
                    gates=tuple(
                        replace(item, status=GateStatus.STALE)
                        if item.status
                        in {
                            GateStatus.EMERGING,
                            GateStatus.ASSISTED,
                            GateStatus.SUPPORTED,
                        }
                        else item
                        for item in recovered_topic.gates
                    ),
                    learner_model=tuple(
                        replace(item, status=InsightStatus.STALE)
                        if item.status is InsightStatus.CONFIRMED
                        else item
                        for item in recovered_topic.learner_model
                    ),
                )
            if is_paused:
                _require(
                    state.phase is ConversationPhase.NONE
                    and state.active_topic is None
                )
                next_state = replace(
                    next_state,
                    active_topic=recovered_topic,
                    paused_topics=tuple(
                        item
                        for item in state.paused_topics
                        if item.topic_run_id != target_topic.topic_run_id
                    ),
                    phase=ConversationPhase.WAITING_HOST,
                )
            else:
                _require(
                    state.phase is ConversationPhase.RECOVERABLE_ERROR
                    and state.active_topic == target_topic
                )
                next_state = replace(
                    next_state,
                    active_topic=recovered_topic,
                    phase=ConversationPhase.WAITING_HOST,
                )
    else:
        raise ValueError("UNKNOWN_EVENT")

    _validate(next_state)
    return next_state
