"""Immutable vocabulary for the X-Sync v2 dialogue kernel."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias


class ConversationPhase(StrEnum):
    NONE = "none"
    CHOOSING_TOPIC = "choosing_topic"
    CLARIFYING_TOPIC = "clarifying_topic"
    AWAITING_USER = "awaiting_user"
    WAITING_HOST = "waiting_host"
    RECOVERABLE_ERROR = "recoverable_error"


class SessionLifecycle(StrEnum):
    NEW = "new"
    OPEN = "open"
    ENDED = "ended"


class TopicLifecycle(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"


class EvidenceHealth(StrEnum):
    CURRENT = "current"
    CAPTURED_DIRTY = "captured_dirty"
    STALE = "stale"
    DISPUTED = "disputed"
    UNAVAILABLE = "unavailable"


class Lens(StrEnum):
    BUSINESS = "business"
    TECHNICAL = "technical"
    MIXED = "mixed"


class GateId(StrEnum):
    MECHANISM = "mechanism"
    BOUNDARY = "boundary"
    REPOSITORY_APPLICATION = "repository_application"


class GateStatus(StrEnum):
    UNEXPLORED = "unexplored"
    EMERGING = "emerging"
    ASSISTED = "assisted"
    SUPPORTED = "supported"
    STALE = "stale"
    DISPUTED = "disputed"


class QuestionIntent(StrEnum):
    CLARIFY = "clarify"
    CAUSAL_TRACE = "causal_trace"
    ASSUMPTION_TEST = "assumption_test"
    COUNTEREXAMPLE = "counterexample"
    EVIDENCE_LOCATE = "evidence_locate"
    BUSINESS_TECHNICAL_BRIDGE = "business_technical_bridge"
    REPOSITORY_APPLY = "repository_apply"
    SYNTHESIZE = "synthesize"


class InsightKind(StrEnum):
    TECHNICAL_CONCLUSION = "technical_conclusion"
    BUSINESS_INSIGHT = "business_insight"
    BUSINESS_TECHNICAL_MAPPING = "business_technical_mapping"
    BOUNDARY = "boundary"
    OPEN_QUESTION = "open_question"


class InsightStatus(StrEnum):
    CONFIRMED = "confirmed"
    WORKING_MODEL = "working_model"
    OPEN_QUESTION = "open_question"
    STALE = "stale"
    DISPUTED = "disputed"


class InsightProvenance(StrEnum):
    LEARNER_EXPLICIT = "learner_explicit"
    AGENT_INFERRED = "agent_inferred"
    JOINTLY_CONFIRMED = "jointly_confirmed"


class TriggerKind(StrEnum):
    TOPIC_CANDIDATES = "topic_candidates"
    TOPIC_SELECTION = "topic_selection"
    INITIAL_TURN = "initial_turn"
    LEARNER_REPLY = "learner_reply"
    REGROUND = "reground"


class WorkStatus(StrEnum):
    """Canonical lifecycle of one durable Host work item."""

    QUEUED = "queued"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"
    COMPLETED = "completed"
    SUPERSEDED = "superseded"


class WorkFailureCategory(StrEnum):
    """Bounded processing failures which are safe to record in dialogue state."""

    HOST_TRANSIENT = "host_transient"
    RESULT_INVALID = "result_invalid"
    LEASE_ATTEMPTS_EXHAUSTED = "lease_attempts_exhausted"
    HOST_PERMANENT = "host_permanent"


class WorkRecoveryAction(StrEnum):
    """Explicit learner-controlled recovery from a dead-lettered work item."""

    RETRY = "retry"
    REGROUND = "reground"


class PauseCause(StrEnum):
    USER = "user"
    SESSION_DEACTIVATION = "session_deactivation"


@dataclass(frozen=True, slots=True)
class GateRequirement:
    gate_id: GateId
    required: bool = True


@dataclass(frozen=True, slots=True)
class GateAssessment:
    gate_id: GateId
    status: GateStatus = GateStatus.UNEXPLORED
    source_turn_ids: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TaskScope:
    task_id: str
    summary: str
    included_paths: tuple[str, ...]
    excluded_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TopicContract:
    contract_id: str
    contract_version: int
    topic_run_id: str
    title: str
    guiding_question: str
    objective: str
    task_scope: TaskScope
    starting_lens: Lens
    bridge_required: bool
    evidence_refs: tuple[str, ...]
    gates: tuple[GateRequirement, ...]
    supersedes_contract_id: str | None
    contract_digest: str


@dataclass(frozen=True, slots=True)
class EvidenceCheck:
    health: EvidenceHealth
    evidence_digest: str
    exact_recheck_fingerprint: str | None = None


@dataclass(frozen=True, slots=True)
class TriggerBinding:
    kind: TriggerKind
    work_id: str
    runtime_epoch: str
    parent_turn_id: str | None
    contract_digest: str | None
    input_digest: str
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class WorkFailure:
    """Sanitized, replayable proof of one bounded processing failure."""

    failure_id: str
    category: WorkFailureCategory
    safe_error_code: str
    proof_digest: str


@dataclass(frozen=True, slots=True)
class CurrentWorkState:
    """Canonical current work, distinct from the temporary lease overlay."""

    work_id: str
    trigger_event_id: str
    trigger_event_sequence: int
    trigger: TriggerBinding
    attempt: int
    status: WorkStatus
    last_failure: WorkFailure | None = None
    allowed_recovery_actions: tuple[WorkRecoveryAction, ...] = ()


@dataclass(frozen=True, slots=True)
class DecisionContext:
    registry_generation: int
    trigger: TriggerBinding | None
    evidence: EvidenceCheck


@dataclass(frozen=True, slots=True)
class LearnerModelEntry:
    entry_id: str
    kind: InsightKind
    status: InsightStatus
    provenance: InsightProvenance
    statement: str
    source_turn_ids: tuple[str, ...]
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AgentTurnResult:
    heard: str
    one_step_further: str
    question_id: str
    question: str
    question_intent: QuestionIntent
    learner_model_delta: tuple[LearnerModelEntry, ...]
    gate_assessments: tuple[GateAssessment, ...]
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TopicRunState:
    topic_run_id: str
    contract: TopicContract
    lifecycle: TopicLifecycle
    evidence_health: EvidenceHealth
    evidence_digest: str
    exact_recheck_fingerprint: str | None
    gates: tuple[GateAssessment, ...]
    learner_model: tuple[LearnerModelEntry, ...] = ()
    current_agent_turn: AgentTurnResult | None = None
    work: CurrentWorkState | None = None
    learner_turn_ids: tuple[str, ...] = ()
    last_learner_turn_id: str | None = None
    last_learner_text: str | None = None

    @property
    def open_question_id(self) -> str | None:
        if self.current_agent_turn is None:
            return None
        return self.current_agent_turn.question_id

    @property
    def unresolved_trigger(self) -> TriggerBinding | None:
        """Return the saved trigger while callers migrate to ``work``."""
        if self.work is None:
            return None
        return self.work.trigger


@dataclass(frozen=True, slots=True)
class DialogueState:
    session_id: str
    registry_generation: int
    sequence: int
    conversation_version: int
    lifecycle: SessionLifecycle
    phase: ConversationPhase
    candidates: tuple[str, ...]
    session_work: CurrentWorkState | None
    active_topic: TopicRunState | None
    paused_topics: tuple[TopicRunState, ...]
    selected_candidate: str | None = None

    @property
    def session_unresolved_trigger(self) -> TriggerBinding | None:
        """Return the saved Session trigger while callers migrate to ``work``."""
        if self.session_work is None:
            return None
        return self.session_work.trigger


@dataclass(frozen=True, slots=True)
class StartSession:
    command_id: str


@dataclass(frozen=True, slots=True)
class PresentCandidates:
    command_id: str
    candidates: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SelectTopic:
    """Select one presented candidate and request a Host-built contract."""

    command_id: str
    candidate: str


@dataclass(frozen=True, slots=True)
class StartTopic:
    command_id: str
    contract: TopicContract
    selected_candidate: str | None = None


@dataclass(frozen=True, slots=True)
class CommitAgentTurn:
    command_id: str
    result: AgentTurnResult


@dataclass(frozen=True, slots=True)
class SubmitLearnerTurn:
    command_id: str
    question_id: str
    learner_turn_id: str
    text: str


@dataclass(frozen=True, slots=True)
class PauseTopic:
    command_id: str


@dataclass(frozen=True, slots=True)
class SwitchTopic:
    """Pause the active Topic and request fresh candidates atomically."""

    command_id: str


@dataclass(frozen=True, slots=True)
class ResumeTopic:
    command_id: str
    topic_run_id: str


@dataclass(frozen=True, slots=True)
class ReportWorkFailure:
    """Record a bounded Host/coordination failure for the current work."""

    command_id: str
    work_id: str
    failure: WorkFailure


@dataclass(frozen=True, slots=True)
class RecoverWork:
    """Explicitly recover one dead-lettered work item."""

    command_id: str
    dead_work_id: str
    action: WorkRecoveryAction


@dataclass(frozen=True, slots=True)
class PrepareSessionDeactivation:
    command_id: str


@dataclass(frozen=True, slots=True)
class FencedQuiesceContext:
    handoff_id: str
    source_session_id: str
    source_generation: int
    fence_generation: int


DialogueCommand: TypeAlias = (
    StartSession
    | PresentCandidates
    | SelectTopic
    | StartTopic
    | CommitAgentTurn
    | SubmitLearnerTurn
    | PauseTopic
    | SwitchTopic
    | ResumeTopic
    | ReportWorkFailure
    | RecoverWork
)


@dataclass(frozen=True, slots=True)
class SessionStarted:
    candidate_trigger: TriggerBinding


@dataclass(frozen=True, slots=True)
class CandidatesPresented:
    candidates: tuple[str, ...]
    trigger: TriggerBinding


@dataclass(frozen=True, slots=True)
class TopicSelectionSubmitted:
    """Learner selection which creates contract-building Host work."""

    candidate: str
    next_trigger: TriggerBinding


@dataclass(frozen=True, slots=True)
class TopicStarted:
    contract: TopicContract
    evidence: EvidenceCheck
    initial_trigger: TriggerBinding
    selected_candidate: str | None = None
    selection_trigger: TriggerBinding | None = None


@dataclass(frozen=True, slots=True)
class AgentTurnCommitted:
    result: AgentTurnResult
    trigger: TriggerBinding
    evidence: EvidenceCheck


@dataclass(frozen=True, slots=True)
class LearnerTurnSubmitted:
    question_id: str
    learner_turn_id: str
    text: str
    next_trigger: TriggerBinding


@dataclass(frozen=True, slots=True)
class TopicPaused:
    topic_run_id: str
    cause: PauseCause = PauseCause.USER
    handoff_id: str | None = None
    fence_generation: int | None = None


@dataclass(frozen=True, slots=True)
class TopicSwitchRequested:
    """Atomic active-Topic pause and Session candidate-work creation."""

    topic_run_id: str
    candidate_trigger: TriggerBinding


@dataclass(frozen=True, slots=True)
class SessionDeactivationPrepared:
    handoff_id: str
    fence_generation: int
    superseded_trigger: TriggerBinding | None


@dataclass(frozen=True, slots=True)
class TopicResumed:
    topic_run_id: str
    requires_reground: bool
    resumed_trigger: TriggerBinding | None
    evidence: EvidenceCheck


@dataclass(frozen=True, slots=True)
class WorkFailed:
    """Version-neutral fact that the current queued attempt failed."""

    work_id: str
    trigger: TriggerBinding
    attempt: int
    failure: WorkFailure


@dataclass(frozen=True, slots=True)
class WorkRequeued:
    """Version-neutral creation of the next automatic attempt."""

    failed_work_id: str
    failed_trigger: TriggerBinding
    failed_attempt: int
    failure: WorkFailure
    next_trigger: TriggerBinding
    next_attempt: int


@dataclass(frozen=True, slots=True)
class WorkDeadLettered:
    """Terminal automatic-failure fact requiring explicit learner recovery."""

    failed_work_id: str
    failed_trigger: TriggerBinding
    failed_attempt: int
    failure: WorkFailure
    allowed_actions: tuple[WorkRecoveryAction, ...]


@dataclass(frozen=True, slots=True)
class WorkRecoveryRequested:
    """Semantic learner action creating a fresh recovery work item."""

    dead_work_id: str
    action: WorkRecoveryAction
    next_trigger: TriggerBinding
    evidence: EvidenceCheck


DialogueEventPayload: TypeAlias = (
    SessionStarted
    | CandidatesPresented
    | TopicSelectionSubmitted
    | TopicStarted
    | AgentTurnCommitted
    | LearnerTurnSubmitted
    | TopicPaused
    | TopicSwitchRequested
    | SessionDeactivationPrepared
    | TopicResumed
    | WorkFailed
    | WorkRequeued
    | WorkDeadLettered
    | WorkRecoveryRequested
)


@dataclass(frozen=True, slots=True)
class PendingDialogueEvent:
    command_id: str
    payload: DialogueEventPayload


@dataclass(frozen=True, slots=True)
class CommittedDialogueEvent:
    event_id: str
    sequence: int
    from_version: int
    to_version: int
    command_id: str
    payload: DialogueEventPayload


@dataclass(frozen=True, slots=True)
class Accepted:
    events: tuple[PendingDialogueEvent, ...]


@dataclass(frozen=True, slots=True)
class Rejected:
    code: str


Decision: TypeAlias = Accepted | Rejected


def initial_dialogue_state(
    session_id: str, registry_generation: int
) -> DialogueState:
    if (
        type(session_id) is not str
        or not session_id.strip()
        or type(registry_generation) is not int
        or registry_generation < 1
    ):
        raise ValueError("INVALID_DIALOGUE_ENVELOPE")
    return DialogueState(
        session_id=session_id,
        registry_generation=registry_generation,
        sequence=0,
        conversation_version=0,
        lifecycle=SessionLifecycle.NEW,
        phase=ConversationPhase.NONE,
        candidates=(),
        session_work=None,
        active_topic=None,
        paused_topics=(),
    )
