# Warm Dialogue State-Machine Kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the first independently testable v2 kernel slice: immutable dialogue records, one explicit pure state machine, and a post-commit read-only Observer Hub, without changing v1 behavior.

**Architecture:** The domain package contains no file, clock, network, HTTP, CLI, or model I/O. Typed commands are decided into typed pending events; only exact committed events enter the pure reducer. Observer callbacks receive immutable committed batches after a lock assertion and cannot call the state machine recursively. Persistence, Host adapters, Browser v2, evidence I/O, and exports are separate later plans after Phase 0 and this kernel establish real APIs.

**Tech Stack:** Python 3.11–3.14; frozen slotted dataclasses, StrEnum, Protocol, unittest, ast, compileall; pinned Ruff, mypy strict mode, and branch coverage gates.

---

## Scope and prerequisite

Execute this plan under the 2026-08-16 dual-Host capability decision recorded in the approved design: Codex is `CAPABILITY_VERIFIED + RUNTIME_PARTIALLY_VERIFIED`, Claude Code is `CAPABILITY_VERIFIED + LOCAL_RUNTIME_WAIVED`, and both use bounded new-turn recovery after tool-channel loss. This slice intentionally does not modify `skills/x-sync/scripts/xsync.py`, install the skill globally, add `/api/v2`, or invent Host platform commands.

The architecture test freezes the current v1 runtime SHA-256. If v1 changes legitimately before this plan is executed, stop and re-review this plan against the new base, then update the hash as a deliberate plan revision; never weaken the check during implementation merely to make the suite green.

The slice owns exactly these files:

- `skills/x-sync/scripts/xsync_v2/__init__.py`: package boundary.
- `skills/x-sync/scripts/xsync_v2/domain.py`: immutable enums, commands, events, and state.
- `skills/x-sync/scripts/xsync_v2/state_machine.py`: transition table, decision function, reducer, and invariants.
- `skills/x-sync/scripts/xsync_v2/observer.py`: immutable committed batches and isolated after-commit dispatch.
- `tests/xsync_v2_path.py`: one import-path fixture for source-tree tests.
- `tests/test_xsync_v2_domain.py`: frozen-domain contract.
- `tests/test_xsync_v2_state_machine.py`: legal/illegal transition and replay tests.
- `tests/test_xsync_v2_observer.py`: Observer isolation, order, duplicate, recursion, and lock tests.
- `tests/test_xsync_v2_architecture.py`: dependency-boundary and v1-freeze checks.
- `skills/x-sync/pyproject.toml`: scoped lint, strict typing, and branch coverage policy.
- `skills/x-sync/requirements-dev.txt`: pinned quality-tool versions for reproducible gates.

Anything requiring a repository write, event log, lease, SSE, projection cursor, or effect executor is out of this slice. No placeholder interface for those systems is added.

### Task 1: Immutable domain vocabulary

**Files:**
- Create: `skills/x-sync/scripts/xsync_v2/__init__.py`
- Create: `skills/x-sync/scripts/xsync_v2/domain.py`
- Create: `tests/xsync_v2_path.py`
- Create: `tests/test_xsync_v2_domain.py`

- [x] **Step 1: Write the import fixture and failing domain test**

Create `tests/xsync_v2_path.py`:

```python
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = REPO_ROOT / "skills" / "x-sync" / "scripts"
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))
```

Create `tests/test_xsync_v2_domain.py`:

```python
from dataclasses import FrozenInstanceError
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.domain import (
    ConversationPhase,
    SessionLifecycle,
    GateAssessment,
    GateId,
    GateRequirement,
    GateStatus,
    Lens,
    TaskScope,
    TopicContract,
    initial_dialogue_state,
)


class DomainTest(unittest.TestCase):
    def test_topic_contract_is_complete_and_state_is_frozen(self):
        contract = TopicContract(
            contract_id="contract-1",
            contract_version=1,
            topic_run_id="topic-1",
            title="支付一致性",
            guiding_question="支付失败后如何恢复？",
            objective="向队友说清失败边界与仓库落点",
            task_scope=TaskScope(
                task_id="task-1",
                summary="支付失败补偿",
                included_paths=("payments/",),
                excluded_paths=("ui/",),
            ),
            starting_lens=Lens.MIXED,
            bridge_required=True,
            evidence_refs=("ev.spec",),
            gates=(
                GateRequirement(GateId.MECHANISM),
                GateRequirement(GateId.BOUNDARY),
                GateRequirement(GateId.REPOSITORY_APPLICATION),
            ),
            supersedes_contract_id=None,
            contract_digest="sha256:contract",
        )
        state = initial_dialogue_state("dlg-1", registry_generation=3)
        self.assertEqual("支付失败补偿", contract.task_scope.summary)
        self.assertEqual(ConversationPhase.NONE, state.phase)
        self.assertEqual(SessionLifecycle.NEW, state.lifecycle)
        self.assertEqual(
            GateStatus.UNEXPLORED,
            GateAssessment(GateId.MECHANISM).status,
        )
        with self.assertRaises(FrozenInstanceError):
            state.phase = ConversationPhase.AWAITING_USER


if __name__ == "__main__":
    unittest.main()
```

- [x] **Step 2: Run RED**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_xsync_v2_domain -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'xsync_v2'`.

- [x] **Step 3: Implement the package and complete immutable domain**

Create `skills/x-sync/scripts/xsync_v2/__init__.py`:

```python
"""X-Sync v2 dialogue domain kernel."""
```

Create `skills/x-sync/scripts/xsync_v2/domain.py`:

```python
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
    INITIAL_TURN = "initial_turn"
    LEARNER_REPLY = "learner_reply"
    REGROUND = "reground"


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
    unresolved_trigger: TriggerBinding | None = None
    learner_turn_ids: tuple[str, ...] = ()
    last_learner_turn_id: str | None = None
    last_learner_text: str | None = None

    @property
    def open_question_id(self) -> str | None:
        if self.current_agent_turn is None:
            return None
        return self.current_agent_turn.question_id


@dataclass(frozen=True, slots=True)
class DialogueState:
    session_id: str
    registry_generation: int
    sequence: int
    conversation_version: int
    lifecycle: SessionLifecycle
    phase: ConversationPhase
    candidates: tuple[str, ...]
    session_unresolved_trigger: TriggerBinding | None
    active_topic: TopicRunState | None
    paused_topics: tuple[TopicRunState, ...]


@dataclass(frozen=True, slots=True)
class StartSession:
    command_id: str


@dataclass(frozen=True, slots=True)
class PresentCandidates:
    command_id: str
    candidates: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StartTopic:
    command_id: str
    contract: TopicContract


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
class ResumeTopic:
    command_id: str
    topic_run_id: str


DialogueCommand: TypeAlias = (
    StartSession
    | PresentCandidates
    | StartTopic
    | CommitAgentTurn
    | SubmitLearnerTurn
    | PauseTopic
    | ResumeTopic
)


@dataclass(frozen=True, slots=True)
class SessionStarted:
    candidate_trigger: TriggerBinding


@dataclass(frozen=True, slots=True)
class CandidatesPresented:
    candidates: tuple[str, ...]
    trigger: TriggerBinding


@dataclass(frozen=True, slots=True)
class TopicStarted:
    contract: TopicContract
    evidence: EvidenceCheck
    initial_trigger: TriggerBinding


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


@dataclass(frozen=True, slots=True)
class TopicResumed:
    topic_run_id: str
    requires_reground: bool
    resumed_trigger: TriggerBinding | None
    evidence: EvidenceCheck


DialogueEventPayload: TypeAlias = (
    SessionStarted
    | CandidatesPresented
    | TopicStarted
    | AgentTurnCommitted
    | LearnerTurnSubmitted
    | TopicPaused
    | TopicResumed
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
    return DialogueState(
        session_id=session_id,
        registry_generation=registry_generation,
        sequence=0,
        conversation_version=0,
        lifecycle=SessionLifecycle.NEW,
        phase=ConversationPhase.NONE,
        candidates=(),
        session_unresolved_trigger=None,
        active_topic=None,
        paused_topics=(),
    )
```

- [x] **Step 4: Run GREEN**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_xsync_v2_domain -v`

Expected: PASS, 2 tests after the immutable-subclass hardening regression.

- [ ] **Step 5: Commit**

```bash
git add skills/x-sync/scripts/xsync_v2/__init__.py skills/x-sync/scripts/xsync_v2/domain.py tests/xsync_v2_path.py tests/test_xsync_v2_domain.py
git commit -m "feat: add immutable v2 dialogue domain"
```

### Task 2: Explicit decision table and pure reducer

**Files:**
- Create: `skills/x-sync/scripts/xsync_v2/state_machine.py`
- Create: `tests/test_xsync_v2_state_machine.py`

- [x] **Step 1: Write the failing transition and replay tests**

Create `tests/test_xsync_v2_state_machine.py`:

```python
from dataclasses import replace
import random
from typing import cast
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.domain import (
    Accepted,
    AgentTurnCommitted,
    AgentTurnResult,
    CandidatesPresented,
    CommitAgentTurn,
    CommittedDialogueEvent,
    ConversationPhase,
    DecisionContext,
    EvidenceCheck,
    EvidenceHealth,
    GateId,
    GateAssessment,
    GateRequirement,
    GateStatus,
    InsightKind,
    InsightProvenance,
    InsightStatus,
    Lens,
    LearnerModelEntry,
    PauseTopic,
    PresentCandidates,
    QuestionIntent,
    Rejected,
    ResumeTopic,
    SessionLifecycle,
    StartSession,
    StartTopic,
    SubmitLearnerTurn,
    TaskScope,
    TopicContract,
    TopicLifecycle,
    TopicResumed,
    TopicStarted,
    TriggerBinding,
    TriggerKind,
    initial_dialogue_state,
)
from xsync_v2.state_machine import TRANSITION_TABLE, _validate, decide, reduce


def contract():
    return TopicContract(
        contract_id="contract-1",
        contract_version=1,
        topic_run_id="topic-1",
        title="支付一致性",
        guiding_question="支付失败后如何恢复？",
        objective="向队友说清失败边界与仓库落点",
        task_scope=TaskScope("task-1", "支付失败补偿", ("payments/",), ("ui/",)),
        starting_lens=Lens.MIXED,
        bridge_required=True,
        evidence_refs=("ev.spec",),
        gates=(
            GateRequirement(GateId.MECHANISM),
            GateRequirement(GateId.BOUNDARY),
            GateRequirement(GateId.REPOSITORY_APPLICATION),
        ),
        supersedes_contract_id=None,
        contract_digest="sha256:contract",
    )


def context(
    kind=None,
    work_id="work-1",
    evidence_health=EvidenceHealth.CURRENT,
    parent_turn_id=None,
):
    exact = (
        "sha256:exact"
        if evidence_health is EvidenceHealth.CAPTURED_DIRTY
        else None
    )
    trigger = None
    if kind is not None:
        trigger = TriggerBinding(
            kind=kind,
            work_id=work_id,
            runtime_epoch="epoch-1",
            parent_turn_id=parent_turn_id,
            contract_digest=(
                None
                if kind is TriggerKind.TOPIC_CANDIDATES
                else "sha256:contract"
            ),
            input_digest=f"sha256:input-{work_id}",
            evidence_digest="sha256:evidence",
        )
    return DecisionContext(
        registry_generation=1,
        trigger=trigger,
        evidence=EvidenceCheck(
            health=evidence_health,
            evidence_digest="sha256:evidence",
            exact_recheck_fingerprint=exact,
        ),
    )


def model_entry():
    return LearnerModelEntry(
        entry_id="model-1",
        kind=InsightKind.TECHNICAL_CONCLUSION,
        status=InsightStatus.WORKING_MODEL,
        provenance=InsightProvenance.LEARNER_EXPLICIT,
        statement="先落业务状态，再发送事件",
        source_turn_ids=("turn-1",),
        evidence_refs=("ev.spec",),
    )


def agent_turn(question_id="q1", with_model=False):
    return AgentTurnResult(
        heard="我听到你会先写业务状态。",
        one_step_further="还需要确认事件发送失败的边界。",
        question_id=question_id,
        question="如果事件发送失败，哪一层负责恢复？",
        question_intent=QuestionIntent.CAUSAL_TRACE,
        learner_model_delta=((model_entry(),) if with_model else ()),
        gate_assessments=(
            GateAssessment(
                GateId.MECHANISM,
                GateStatus.EMERGING if with_model else GateStatus.UNEXPLORED,
                ("turn-1",) if with_model else (),
                ("ev.spec",) if with_model else (),
            ),
            GateAssessment(GateId.BOUNDARY),
            GateAssessment(GateId.REPOSITORY_APPLICATION),
        ),
        evidence_refs=("ev.spec",),
    )


def commit_for_test(state, pending):
    sequence = state.sequence + 1
    return CommittedDialogueEvent(
        event_id=f"event-{sequence}",
        sequence=sequence,
        from_version=state.conversation_version,
        to_version=state.conversation_version + 1,
        command_id=pending.command_id,
        payload=pending.payload,
    )


def default_context(state, command):
    if isinstance(command, (StartSession, PresentCandidates)):
        return context(TriggerKind.TOPIC_CANDIDATES)
    if isinstance(command, StartTopic):
        return context(TriggerKind.INITIAL_TURN)
    if isinstance(command, CommitAgentTurn):
        topic = state.active_topic
        if topic is None:
            return context()
        return DecisionContext(
            registry_generation=state.registry_generation,
            trigger=topic.unresolved_trigger,
            evidence=EvidenceCheck(
                topic.evidence_health,
                topic.evidence_digest,
                topic.exact_recheck_fingerprint,
            ),
        )
    if isinstance(command, SubmitLearnerTurn):
        return context(
            TriggerKind.LEARNER_REPLY,
            work_id=f"work-{command.learner_turn_id}",
            parent_turn_id=command.learner_turn_id,
        )
    if isinstance(command, ResumeTopic):
        paused = next(
            item
            for item in state.paused_topics
            if item.topic_run_id == command.topic_run_id
        )
        if paused.evidence_health in {
            EvidenceHealth.STALE,
            EvidenceHealth.DISPUTED,
            EvidenceHealth.UNAVAILABLE,
        }:
            return context(
                TriggerKind.REGROUND,
                work_id="work-reground",
                evidence_health=paused.evidence_health,
            )
        if paused.unresolved_trigger is not None:
            return context(
                paused.unresolved_trigger.kind,
                work_id="work-resume",
                evidence_health=paused.evidence_health,
            )
    return context()


def apply(state, command, decision_context=None):
    if decision_context is None:
        decision_context = default_context(state, command)
    decision = decide(state, command, decision_context)
    if not isinstance(decision, Accepted):
        raise AssertionError(decision)
    for pending in decision.events:
        state = reduce(state, commit_for_test(state, pending))
    return state


class StateMachineTest(unittest.TestCase):
    def test_socratic_turn_uses_one_canonical_path(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        mutable_candidates = cast(tuple[str, ...], ["支付一致性"])
        mutable_command = PresentCandidates("mutable", mutable_candidates)
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(state, mutable_command, default_context(state, mutable_command)),
        )
        for bad_candidates in (
            ("支付一致性",) * 2,
            ("",),
            ("一", "二", "三", "四", "五"),
        ):
            bad_command = PresentCandidates("bad-candidates", bad_candidates)
            self.assertEqual(
                Rejected("VALIDATION_FAILED"),
                decide(state, bad_command, default_context(state, bad_command)),
            )
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(state, StartTopic("c3", contract()))
        state = apply(state, CommitAgentTurn("c4", agent_turn()))
        state = apply(
            state,
            SubmitLearnerTurn("c5", "q1", "turn-1", "先写业务状态，再发事件。"),
        )
        invalid_entry = replace(
            model_entry(),
            status=InsightStatus.CONFIRMED,
            provenance=InsightProvenance.AGENT_INFERRED,
        )
        invalid_result = replace(
            agent_turn("q2", with_model=True),
            learner_model_delta=(invalid_entry,),
        )
        invalid_command = CommitAgentTurn("invalid", invalid_result)
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                invalid_command,
                default_context(state, invalid_command),
            ),
        )
        mutable_model_refs = cast(tuple[str, ...], ["turn-1"])
        mutable_entry = replace(
            model_entry(),
            source_turn_ids=mutable_model_refs,
        )
        mutable_result = replace(
            agent_turn("q2", with_model=True),
            learner_model_delta=(mutable_entry,),
        )
        mutable_publish = CommitAgentTurn("mutable-publish", mutable_result)
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                mutable_publish,
                default_context(state, mutable_publish),
            ),
        )
        state = apply(
            state,
            CommitAgentTurn("c6", agent_turn("q2", with_model=True)),
        )
        self.assertEqual(ConversationPhase.AWAITING_USER, state.phase)
        self.assertEqual("turn-1", state.active_topic.last_learner_turn_id)
        self.assertEqual("先写业务状态，再发事件。", state.active_topic.last_learner_text)
        self.assertEqual("contract-1", state.active_topic.contract.contract_id)
        self.assertEqual(
            "先落业务状态，再发送事件",
            state.active_topic.learner_model[0].statement,
        )
        self.assertEqual(
            GateStatus.EMERGING,
            state.active_topic.gates[0].status,
        )
        self.assertEqual("q2", state.active_topic.current_agent_turn.question_id)
        duplicate_turn = SubmitLearnerTurn(
            "duplicate-turn",
            "q2",
            "turn-1",
            "重复提交旧回合。",
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(state, duplicate_turn, default_context(state, duplicate_turn)),
        )
        state = apply(
            state,
            SubmitLearnerTurn(
                "c7",
                "q2",
                "turn-2",
                "事件发布失败时，由 outbox 重试。",
            ),
        )
        same_status_result = replace(
            agent_turn("q3", with_model=True),
            learner_model_delta=(
                replace(
                    model_entry(),
                    source_turn_ids=("turn-1", "turn-2"),
                ),
            ),
        )
        same_status = CommitAgentTurn("same-status", same_status_result)
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(state, same_status, default_context(state, same_status)),
        )
        confirmed_entry = replace(
            model_entry(),
            status=InsightStatus.CONFIRMED,
            provenance=InsightProvenance.JOINTLY_CONFIRMED,
            source_turn_ids=("turn-1", "turn-2"),
        )
        confirmed_result = replace(
            agent_turn("q3", with_model=True),
            learner_model_delta=(confirmed_entry,),
        )
        state = apply(state, CommitAgentTurn("c8", confirmed_result))
        self.assertEqual(1, len(state.active_topic.learner_model))
        self.assertEqual(
            InsightStatus.CONFIRMED,
            state.active_topic.learner_model[0].status,
        )
        self.assertEqual(
            InsightProvenance.JOINTLY_CONFIRMED,
            state.active_topic.learner_model[0].provenance,
        )
        self.assertEqual("q3", state.active_topic.current_agent_turn.question_id)
        self.assertEqual(8, state.sequence)
        self.assertEqual(8, state.conversation_version)

    def test_illegal_command_is_rejected_without_mutation(self):
        state = initial_dialogue_state("dlg-1", 1)
        decision = decide(
            state, CommitAgentTurn("bad", agent_turn()), context()
        )
        self.assertEqual(Rejected("TOPIC_STATE_CONFLICT"), decision)
        self.assertEqual(0, state.sequence)

    def test_stale_evidence_cannot_publish_an_agent_turn(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(
            state,
            StartTopic("c3", contract()),
            context(TriggerKind.REGROUND, evidence_health=EvidenceHealth.STALE),
        )
        command = CommitAgentTurn("c4", agent_turn())
        decision = decide(state, command, default_context(state, command))
        self.assertEqual(Rejected("EVIDENCE_STALE"), decision)
        self.assertEqual(ConversationPhase.WAITING_HOST, state.phase)

    def test_publish_binding_and_registry_generation_are_fenced(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(state, StartTopic("c3", contract()))
        command = CommitAgentTurn("c4", agent_turn())
        late = context(TriggerKind.INITIAL_TURN, work_id="late-work")
        self.assertEqual(
            Rejected("WORK_SUPERSEDED"), decide(state, command, late)
        )
        wrong_generation = replace(
            default_context(state, command), registry_generation=2
        )
        self.assertEqual(
            Rejected("SESSION_DEACTIVATED"),
            decide(state, command, wrong_generation),
        )

    def test_resume_never_reopens_a_stale_question(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(state, StartTopic("c3", contract()))
        state = apply(state, CommitAgentTurn("c4", agent_turn()))
        state = apply(state, PauseTopic("c5"))
        resume = ResumeTopic("c6", "topic-1")
        resume_context = context(
            TriggerKind.REGROUND,
            work_id="work-reground",
            evidence_health=EvidenceHealth.STALE,
        )
        decision = decide(state, resume, resume_context)
        self.assertIsInstance(decision, Accepted)
        committed = commit_for_test(state, decision.events[0])
        forged = replace(
            committed,
            payload=TopicResumed(
                "topic-1",
                False,
                resume_context.trigger,
                resume_context.evidence,
            ),
        )
        with self.assertRaisesRegex(ValueError, "ILLEGAL_EVENT_TRANSITION"):
            reduce(state, forged)
        state = apply(state, resume, resume_context)
        self.assertEqual(ConversationPhase.WAITING_HOST, state.phase)
        self.assertIsNone(state.active_topic.open_question_id)
        self.assertEqual(EvidenceHealth.STALE, state.active_topic.evidence_health)

    def test_stale_resume_downgrades_supported_gates_and_confirmed_model(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(state, StartTopic("c3", contract()))
        state = apply(state, CommitAgentTurn("c4", agent_turn()))
        state = apply(
            state,
            SubmitLearnerTurn("c5", "q1", "turn-1", "先落业务状态。"),
        )
        confirmed = replace(
            model_entry(),
            status=InsightStatus.CONFIRMED,
        )
        supported = tuple(
            GateAssessment(
                gate_id,
                GateStatus.SUPPORTED,
                ("turn-1",),
                ("ev.spec",),
            )
            for gate_id in GateId
        )
        result = replace(
            agent_turn("q2"),
            learner_model_delta=(confirmed,),
            gate_assessments=supported,
        )
        state = apply(state, CommitAgentTurn("c6", result))
        state = apply(state, PauseTopic("c7"))
        state = apply(
            state,
            ResumeTopic("c8", "topic-1"),
            context(
                TriggerKind.REGROUND,
                work_id="work-reground",
                evidence_health=EvidenceHealth.STALE,
            ),
        )
        self.assertTrue(
            all(item.status is GateStatus.STALE for item in state.active_topic.gates)
        )
        self.assertEqual(
            InsightStatus.STALE,
            state.active_topic.learner_model[0].status,
        )

    def test_multi_event_replay_is_deterministic(self):
        initial = initial_dialogue_state("dlg-1", 1)
        state = initial
        events = []
        commands = (
            StartSession("c1"),
            PresentCandidates("c2", ("支付一致性",)),
            StartTopic("c3", contract()),
            CommitAgentTurn("c4", agent_turn()),
            SubmitLearnerTurn("c5", "q1", "turn-1", "先写业务状态，再发事件。"),
            CommitAgentTurn("c6", agent_turn("q2", with_model=True)),
        )
        for command in commands:
            decision = decide(state, command, default_context(state, command))
            self.assertIsInstance(decision, Accepted)
            event = commit_for_test(state, decision.events[0])
            events.append(event)
            state = reduce(state, event)
        for cutoff in range(len(events) + 1):
            first = initial
            second = initial
            for event in events[:cutoff]:
                first = reduce(first, event)
                second = reduce(second, event)
            self.assertEqual(first, second)
        self.assertEqual(state, first)

    def test_corrupt_replay_fails_closed(self):
        initial = initial_dialogue_state("dlg-1", 1)
        pending = decide(
            initial,
            StartSession("c1"),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        self.assertIsInstance(pending, Accepted)
        event = commit_for_test(initial, pending.events[0])
        with self.assertRaisesRegex(ValueError, "EVENT_SEQUENCE_GAP"):
            reduce(initial, replace(event, sequence=2))
        with self.assertRaisesRegex(ValueError, "EVENT_VERSION_CONFLICT"):
            reduce(initial, replace(event, from_version=9, to_version=10))
        host_context = context(TriggerKind.INITIAL_TURN)
        illegal = replace(
            event,
            payload=AgentTurnCommitted(
                agent_turn(), host_context.trigger, host_context.evidence
            ),
        )
        with self.assertRaisesRegex(ValueError, "ILLEGAL_EVENT_TRANSITION"):
            reduce(initial, illegal)
        unknown = replace(event, payload=object())
        with self.assertRaisesRegex(ValueError, "UNKNOWN_EVENT"):
            reduce(initial, unknown)
        opened = reduce(initial, event)
        candidate_context = context(TriggerKind.TOPIC_CANDIDATES)
        candidate_decision = decide(
            opened,
            PresentCandidates("candidates", ("支付一致性",)),
            candidate_context,
        )
        self.assertIsInstance(candidate_decision, Accepted)
        candidate_event = commit_for_test(opened, candidate_decision.events[0])
        mutable_candidates = cast(tuple[str, ...], ["支付一致性"])
        with self.assertRaisesRegex(ValueError, "ILLEGAL_EVENT_TRANSITION"):
            reduce(
                opened,
                replace(
                    candidate_event,
                    payload=CandidatesPresented(
                        mutable_candidates,
                        candidate_context.trigger,
                    ),
                ),
            )
        with self.assertRaisesRegex(ValueError, "EVENT_SEQUENCE_GAP"):
            reduce(opened, event)

    def test_pause_changes_topic_lifecycle_not_session_history(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(state, StartTopic("c3", contract()))
        state = apply(state, PauseTopic("c4"))
        self.assertIsNone(state.active_topic)
        self.assertEqual(TopicLifecycle.PAUSED, state.paused_topics[0].lifecycle)
        ended = replace(state, lifecycle=SessionLifecycle.ENDED)
        resume = ResumeTopic("ended", "topic-1")
        self.assertEqual(
            Rejected("TOPIC_STATE_CONFLICT"),
            decide(ended, resume, default_context(ended, resume)),
        )

    def test_resume_rebuilds_the_paused_unresolved_trigger(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(state, StartTopic("c3", contract()))
        state = apply(state, PauseTopic("c4"))
        state = apply(state, ResumeTopic("c5", "topic-1"))
        self.assertEqual(ConversationPhase.WAITING_HOST, state.phase)
        self.assertEqual(
            TriggerKind.INITIAL_TURN,
            state.active_topic.unresolved_trigger.kind,
        )

    def test_missing_or_duplicate_required_gates_are_rejected(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        missing = replace(
            contract(), gates=(GateRequirement(GateId.MECHANISM),)
        )
        duplicate = replace(
            contract(),
            gates=(
                GateRequirement(GateId.MECHANISM),
                GateRequirement(GateId.MECHANISM),
                GateRequirement(GateId.REPOSITORY_APPLICATION),
            ),
        )
        raw_lens = replace(contract(), starting_lens=cast(Lens, "mixed"))
        raw_gate = replace(
            contract(),
            gates=(
                GateRequirement(cast(GateId, "mechanism")),
                GateRequirement(GateId.BOUNDARY),
                GateRequirement(GateId.REPOSITORY_APPLICATION),
            ),
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                StartTopic("missing", missing),
                context(TriggerKind.INITIAL_TURN),
            ),
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                StartTopic("duplicate", duplicate),
                context(TriggerKind.INITIAL_TURN),
            ),
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                StartTopic("raw-lens", raw_lens),
                context(TriggerKind.INITIAL_TURN),
            ),
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                StartTopic("raw-gate", raw_gate),
                context(TriggerKind.INITIAL_TURN),
            ),
        )
        raw_evidence = replace(
            context(TriggerKind.INITIAL_TURN),
            evidence=EvidenceCheck(
                cast(EvidenceHealth, "captured_dirty"),
                "sha256:evidence",
                None,
            ),
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                StartTopic("raw-evidence", contract()),
                raw_evidence,
            ),
        )
        topic_context = context(TriggerKind.INITIAL_TURN)
        valid = decide(state, StartTopic("valid", contract()), topic_context)
        self.assertIsInstance(valid, Accepted)
        committed = commit_for_test(state, valid.events[0])
        with self.assertRaisesRegex(ValueError, "ILLEGAL_EVENT_TRANSITION"):
            reduce(
                state,
                replace(
                    committed,
                    payload=TopicStarted(
                        missing, topic_context.evidence, topic_context.trigger
                    ),
                ),
            )

    def test_every_handler_and_common_guard_fail_closed(self):
        initial = initial_dialogue_state("dlg-1", 1)
        for command_type, handler in TRANSITION_TABLE.items():
            wrong = (
                PauseTopic("wrong")
                if command_type is not PauseTopic
                else StartSession("wrong")
            )
            with self.subTest(handler=command_type.__name__):
                self.assertEqual(
                    Rejected("TOPIC_STATE_CONFLICT"),
                    handler(initial, wrong, context()),
                )

        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(initial, StartSession("missing-trigger"), context()),
        )
        opened = apply(initial, StartSession("c1"))
        incompatible = (
            StartSession("again"),
            StartTopic("early", contract()),
            CommitAgentTurn("early", agent_turn()),
            SubmitLearnerTurn("early", "q", "turn", "text"),
            PauseTopic("early"),
            ResumeTopic("early", "missing"),
        )
        for command in incompatible:
            with self.subTest(command=type(command).__name__):
                self.assertEqual(
                    Rejected("TOPIC_STATE_CONFLICT"),
                    decide(opened, command, context()),
                )
        wrong_trigger = context(TriggerKind.INITIAL_TURN)
        self.assertEqual(
            Rejected("WORK_SUPERSEDED"),
            decide(
                opened,
                PresentCandidates("wrong-trigger", ("支付一致性",)),
                wrong_trigger,
            ),
        )
        choosing = apply(opened, PresentCandidates("c2", ("支付一致性",)))
        present_again = PresentCandidates("again", ("支付一致性",))
        self.assertEqual(
            Rejected("TOPIC_STATE_CONFLICT"),
            decide(
                choosing,
                present_again,
                default_context(choosing, present_again),
            ),
        )
        waiting = apply(choosing, StartTopic("c3", contract()))
        invalid_results = (
            cast(AgentTurnResult, object()),
            replace(agent_turn(), heard=cast(str, 1)),
            replace(
                agent_turn(),
                learner_model_delta=cast(
                    tuple[LearnerModelEntry, ...],
                    [],
                ),
            ),
        )
        for index, result in enumerate(invalid_results):
            command = CommitAgentTurn(f"invalid-result-{index}", result)
            with self.subTest(result=index):
                self.assertEqual(
                    Rejected("VALIDATION_FAILED"),
                    decide(waiting, command, default_context(waiting, command)),
                )
        awaiting = apply(waiting, CommitAgentTurn("c4", agent_turn()))
        second_publish = CommitAgentTurn("second", agent_turn("q2"))
        self.assertEqual(
            Rejected("TOPIC_STATE_CONFLICT"),
            decide(
                awaiting,
                second_publish,
                default_context(awaiting, second_publish),
            ),
        )
        empty_turn = SubmitLearnerTurn("empty", "q1", "turn-empty", "")
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(awaiting, empty_turn, default_context(awaiting, empty_turn)),
        )
        wrong_parent = SubmitLearnerTurn(
            "wrong-parent", "q1", "turn-parent", "text"
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                awaiting,
                wrong_parent,
                context(
                    TriggerKind.LEARNER_REPLY,
                    parent_turn_id="different-turn",
                ),
            ),
        )
        paused = apply(awaiting, PauseTopic("pause"))
        unknown_resume = ResumeTopic("unknown", "missing")
        self.assertEqual(
            Rejected("TOPIC_STATE_CONFLICT"),
            decide(
                paused,
                unknown_resume,
                context(),
            ),
        )
        resume = ResumeTopic("resume", "topic-1")
        unexpected_trigger = context(TriggerKind.INITIAL_TURN)
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(paused, resume, unexpected_trigger),
        )

    def test_canonical_invariant_matrix_rejects_corruption(self):
        initial = initial_dialogue_state("dlg-1", 1)
        opened = apply(initial, StartSession("c1"))
        choosing = apply(opened, PresentCandidates("c2", ("支付一致性",)))
        active = apply(choosing, StartTopic("c3", contract()))
        topic = active.active_topic
        self.assertIsNotNone(topic)
        if topic is None:
            self.fail("topic fixture missing")
        paused = apply(active, PauseTopic("pause"))
        paused_topic = paused.paused_topics[0]
        awaiting = apply(active, CommitAgentTurn("question", agent_turn()))
        awaiting_topic = awaiting.active_topic
        self.assertIsNotNone(awaiting_topic)
        if awaiting_topic is None:
            self.fail("awaiting topic fixture missing")
        bogus_model = replace(
            model_entry(),
            status=cast(InsightStatus, "bogus"),
            source_turn_ids=(),
            evidence_refs=(),
        )
        corrupt_states = (
            replace(initial, phase=ConversationPhase.WAITING_HOST),
            replace(
                initial,
                lifecycle=SessionLifecycle.ENDED,
                phase=ConversationPhase.WAITING_HOST,
            ),
            replace(choosing, phase=ConversationPhase.AWAITING_USER),
            replace(opened, session_unresolved_trigger=None),
            replace(
                choosing,
                session_unresolved_trigger=context(
                    TriggerKind.TOPIC_CANDIDATES
                ).trigger,
            ),
            replace(
                active,
                active_topic=replace(topic, lifecycle=TopicLifecycle.PAUSED),
            ),
            replace(active, phase=ConversationPhase.NONE),
            replace(active, phase=ConversationPhase.CHOOSING_TOPIC),
            replace(active, phase=ConversationPhase.CLARIFYING_TOPIC),
            replace(
                opened,
                phase=ConversationPhase.CHOOSING_TOPIC,
                candidates=(),
                session_unresolved_trigger=None,
            ),
            replace(
                choosing,
                candidates=cast(tuple[str, ...], ["支付一致性"]),
            ),
            replace(
                active,
                active_topic=replace(topic, topic_run_id="other"),
            ),
            replace(
                active,
                active_topic=replace(
                    topic,
                    contract=replace(topic.contract, title=""),
                ),
            ),
            replace(
                active,
                active_topic=replace(
                    topic,
                    evidence_health=EvidenceHealth.CAPTURED_DIRTY,
                    exact_recheck_fingerprint=None,
                ),
            ),
            replace(active, active_topic=replace(topic, gates=())),
            replace(
                active,
                active_topic=replace(topic, learner_model=(bogus_model,)),
            ),
            replace(
                awaiting,
                active_topic=replace(
                    awaiting_topic,
                    current_agent_turn=replace(
                        awaiting_topic.current_agent_turn,
                        question=cast(str, 1),
                    ),
                ),
            ),
            replace(
                active,
                active_topic=replace(
                    topic,
                    learner_turn_ids=("turn", "turn"),
                    last_learner_turn_id="turn",
                ),
            ),
            replace(
                paused,
                paused_topics=(
                    replace(paused_topic, lifecycle=TopicLifecycle.ACTIVE),
                ),
            ),
            replace(
                paused,
                paused_topics=(
                    replace(paused_topic, learner_model=(bogus_model,)),
                ),
            ),
            replace(active, paused_topics=(paused_topic,)),
            replace(paused, paused_topics=(paused_topic, paused_topic)),
        )
        for index, corrupt in enumerate(corrupt_states):
            with self.subTest(index=index):
                with self.assertRaisesRegex(
                    ValueError,
                    "STATE_INVARIANT_VIOLATION",
                ):
                    _validate(corrupt)

    def test_seeded_generated_sequences_replay_and_corruptions_fail_closed(self):
        saw_pause = False
        saw_resume = False
        for seed in range(32):
            generator = random.Random(seed)
            initial = initial_dialogue_state(f"dlg-{seed}", 1)
            state = initial
            events = []
            turn_number = 0
            question_number = 0
            for step in range(40):
                command_id = f"seed-{seed}-step-{step}"
                if state.lifecycle is SessionLifecycle.NEW:
                    command = StartSession(command_id)
                    decision_context = context(TriggerKind.TOPIC_CANDIDATES)
                elif (
                    state.phase is ConversationPhase.WAITING_HOST
                    and state.active_topic is None
                ):
                    command = PresentCandidates(command_id, ("支付一致性",))
                    decision_context = default_context(state, command)
                elif state.phase is ConversationPhase.CHOOSING_TOPIC:
                    command = StartTopic(command_id, contract())
                    decision_context = default_context(state, command)
                elif (
                    state.phase is ConversationPhase.WAITING_HOST
                    and state.active_topic is not None
                ):
                    if not saw_pause or generator.random() < 0.2:
                        command = PauseTopic(command_id)
                        decision_context = context()
                        saw_pause = True
                    else:
                        question_number += 1
                        command = CommitAgentTurn(
                            command_id,
                            agent_turn(f"q-{seed}-{question_number}"),
                        )
                        decision_context = default_context(state, command)
                elif state.phase is ConversationPhase.AWAITING_USER:
                    if generator.random() < 0.2:
                        command = PauseTopic(command_id)
                        decision_context = context()
                        saw_pause = True
                    else:
                        turn_number += 1
                        topic = state.active_topic
                        self.assertIsNotNone(topic)
                        if topic is None:
                            self.fail("generated topic missing")
                        command = SubmitLearnerTurn(
                            command_id,
                            topic.open_question_id,
                            f"turn-{seed}-{turn_number}",
                            "先保存业务事实，再恢复异步事件。",
                        )
                        decision_context = default_context(state, command)
                elif (
                    state.phase is ConversationPhase.NONE
                    and state.paused_topics
                ):
                    paused_topic = state.paused_topics[-1]
                    command = ResumeTopic(command_id, paused_topic.topic_run_id)
                    if paused_topic.open_question_id is not None:
                        decision_context = context()
                    else:
                        self.assertIsNotNone(paused_topic.unresolved_trigger)
                        unresolved = paused_topic.unresolved_trigger
                        if unresolved is None:
                            self.fail("paused trigger missing")
                        decision_context = context(
                            unresolved.kind,
                            work_id=f"resume-{seed}-{step}",
                        )
                    saw_resume = True
                else:
                    self.fail(f"no generated command for {state}")
                decision = decide(state, command, decision_context)
                self.assertIsInstance(decision, Accepted)
                if not isinstance(decision, Accepted):
                    self.fail(decision)
                event = commit_for_test(state, decision.events[0])
                events.append(event)
                state = reduce(state, event)

            replayed = initial
            for event in events:
                replayed = reduce(replayed, event)
            self.assertEqual(state, replayed)

            if seed == 0:
                corruptions = (
                    events[1:],
                    (*events, events[-1]),
                    (events[1], events[0], *events[2:]),
                    (
                        replace(
                            events[0],
                            to_version=events[0].from_version,
                        ),
                        *events[1:],
                    ),
                )
                for corrupt in corruptions:
                    broken = initial
                    with self.assertRaises(ValueError):
                        for event in corrupt:
                            broken = reduce(broken, event)
        self.assertTrue(saw_pause)
        self.assertTrue(saw_resume)


if __name__ == "__main__":
    unittest.main()
```

- [x] **Step 2: Run RED**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_xsync_v2_state_machine -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'xsync_v2.state_machine'`.

- [x] **Step 3: Implement the complete transition slice**

Create `skills/x-sync/scripts/xsync_v2/state_machine.py`:

```python
"""Pure command decision and event reduction for X-Sync v2."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import TypeAlias, cast

from .domain import (
    Accepted,
    AgentTurnCommitted,
    AgentTurnResult,
    CandidatesPresented,
    CommitAgentTurn,
    CommittedDialogueEvent,
    ConversationPhase,
    Decision,
    DecisionContext,
    DialogueCommand,
    DialogueEventPayload,
    SessionLifecycle,
    DialogueState,
    EvidenceCheck,
    EvidenceHealth,
    GateAssessment,
    GateId,
    GateRequirement,
    GateStatus,
    InsightKind,
    InsightProvenance,
    InsightStatus,
    LearnerModelEntry,
    LearnerTurnSubmitted,
    Lens,
    PauseTopic,
    PendingDialogueEvent,
    PresentCandidates,
    QuestionIntent,
    Rejected,
    ResumeTopic,
    SessionStarted,
    StartSession,
    StartTopic,
    SubmitLearnerTurn,
    TaskScope,
    TopicLifecycle,
    TopicContract,
    TopicPaused,
    TopicResumed,
    TopicRunState,
    TopicStarted,
    TriggerBinding,
    TriggerKind,
)


Handler: TypeAlias = Callable[
    [DialogueState, DialogueCommand, DecisionContext], Decision
]


def _accept(command_id: str, payload: DialogueEventPayload) -> Accepted:
    return Accepted((PendingDialogueEvent(command_id, payload),))


REQUIRED_GATES = frozenset(GateId)
PUBLISHABLE_EVIDENCE = frozenset(
    {EvidenceHealth.CURRENT, EvidenceHealth.CAPTURED_DIRTY}
)


def _valid_contract(contract: object) -> bool:
    if not isinstance(contract, TopicContract):
        return False
    if (
        not isinstance(contract.contract_version, int)
        or isinstance(contract.contract_version, bool)
        or not isinstance(contract.task_scope, TaskScope)
        or not isinstance(contract.supersedes_contract_id, (str, type(None)))
    ):
        return False
    text_fields = (
        contract.contract_id,
        contract.topic_run_id,
        contract.title,
        contract.guiding_question,
        contract.objective,
        contract.contract_digest,
    )
    if any(not isinstance(item, str) for item in text_fields):
        return False
    if (
        not isinstance(contract.starting_lens, Lens)
        or not isinstance(contract.bridge_required, bool)
        or not isinstance(contract.evidence_refs, tuple)
        or any(not isinstance(item, str) for item in contract.evidence_refs)
        or not isinstance(contract.gates, tuple)
        or any(not isinstance(item, GateRequirement) for item in contract.gates)
        or any(
            not isinstance(item.gate_id, GateId)
            or not isinstance(item.required, bool)
            for item in contract.gates
        )
        or not isinstance(contract.task_scope.task_id, str)
        or not isinstance(contract.task_scope.summary, str)
        or not isinstance(contract.task_scope.included_paths, tuple)
        or not isinstance(contract.task_scope.excluded_paths, tuple)
        or any(
            not isinstance(item, str)
            for item in contract.task_scope.included_paths
            + contract.task_scope.excluded_paths
        )
    ):
        return False
    gate_ids = tuple(item.gate_id for item in contract.gates)
    return (
        contract.contract_version >= 1
        and bool(contract.contract_id.strip())
        and bool(contract.topic_run_id.strip())
        and bool(contract.title.strip())
        and bool(contract.guiding_question.strip())
        and bool(contract.objective.strip())
        and bool(contract.task_scope.task_id.strip())
        and bool(contract.task_scope.summary.strip())
        and contract.bridge_required
        and bool(contract.evidence_refs)
        and len(contract.evidence_refs) == len(set(contract.evidence_refs))
        and all(item.strip() for item in contract.evidence_refs)
        and len(gate_ids) == len(REQUIRED_GATES)
        and frozenset(gate_ids) == REQUIRED_GATES
        and all(item.required for item in contract.gates)
        and bool(contract.contract_digest.strip())
    )


def _valid_evidence(check: object) -> bool:
    return (
        isinstance(check, EvidenceCheck)
        and isinstance(check.health, EvidenceHealth)
        and isinstance(check.evidence_digest, str)
        and bool(check.evidence_digest.strip())
        and (
            check.exact_recheck_fingerprint is None
            or isinstance(check.exact_recheck_fingerprint, str)
        )
        and (
            check.health is not EvidenceHealth.CAPTURED_DIRTY
            or bool(check.exact_recheck_fingerprint)
        )
    )


def _valid_candidates(candidates: object) -> bool:
    return (
        type(candidates) is tuple
        and 1 <= len(candidates) <= 4
        and all(isinstance(item, str) and item.strip() for item in candidates)
        and len(candidates) == len(set(candidates))
    )


def _valid_trigger(
    trigger: object,
    kind: TriggerKind,
    contract_digest: str | None,
    evidence_digest: str,
) -> bool:
    return (
        isinstance(trigger, TriggerBinding)
        and trigger.kind is kind
        and isinstance(trigger.kind, TriggerKind)
        and isinstance(trigger.work_id, str)
        and bool(trigger.work_id.strip())
        and isinstance(trigger.runtime_epoch, str)
        and bool(trigger.runtime_epoch.strip())
        and trigger.contract_digest == contract_digest
        and isinstance(trigger.input_digest, str)
        and bool(trigger.input_digest.strip())
        and isinstance(trigger.evidence_digest, str)
        and trigger.evidence_digest == evidence_digest
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
    return (
        isinstance(entry, LearnerModelEntry)
        and isinstance(entry.entry_id, str)
        and bool(entry.entry_id.strip())
        and isinstance(entry.statement, str)
        and bool(entry.statement.strip())
        and isinstance(entry.kind, InsightKind)
        and isinstance(entry.status, InsightStatus)
        and isinstance(entry.provenance, InsightProvenance)
        and type(entry.source_turn_ids) is tuple
        and all(
            isinstance(item, str) and item.strip()
            for item in entry.source_turn_ids
        )
        and set(entry.source_turn_ids).issubset(known_turn_ids)
        and type(entry.evidence_refs) is tuple
        and all(
            isinstance(item, str) and item.strip()
            for item in entry.evidence_refs
        )
        and set(entry.evidence_refs).issubset(contract.evidence_refs)
        and (
            entry.provenance is not InsightProvenance.AGENT_INFERRED
            or entry.status is InsightStatus.WORKING_MODEL
        )
        and (
            entry.status is not InsightStatus.CONFIRMED
            or (
                entry.provenance
                in {
                    InsightProvenance.LEARNER_EXPLICIT,
                    InsightProvenance.JOINTLY_CONFIRMED,
                }
                and bool(entry.source_turn_ids)
                and bool(entry.evidence_refs)
            )
        )
    )


def _valid_gate_assessment(
    contract: TopicContract,
    item: object,
    known_turn_ids: frozenset[str],
) -> bool:
    return (
        isinstance(item, GateAssessment)
        and isinstance(item.gate_id, GateId)
        and isinstance(item.status, GateStatus)
        and type(item.source_turn_ids) is tuple
        and all(
            isinstance(turn_id, str) and turn_id.strip()
            for turn_id in item.source_turn_ids
        )
        and set(item.source_turn_ids).issubset(known_turn_ids)
        and type(item.evidence_refs) is tuple
        and all(
            isinstance(evidence_id, str) and evidence_id.strip()
            for evidence_id in item.evidence_refs
        )
        and set(item.evidence_refs).issubset(contract.evidence_refs)
        and (
            item.status is not GateStatus.SUPPORTED
            or (bool(item.source_turn_ids) and bool(item.evidence_refs))
        )
    )


def _valid_agent_turn(
    contract: TopicContract,
    result: object,
    evidence: EvidenceCheck,
    known_turn_ids: frozenset[str],
) -> bool:
    if not isinstance(result, AgentTurnResult):
        return False
    if any(
        not isinstance(item, str)
        for item in (
            result.heard,
            result.one_step_further,
            result.question_id,
            result.question,
        )
    ):
        return False
    if (
        not isinstance(result.learner_model_delta, tuple)
        or any(
            not isinstance(item, LearnerModelEntry)
            for item in result.learner_model_delta
        )
        or not isinstance(result.gate_assessments, tuple)
        or any(
            not isinstance(item, GateAssessment)
            for item in result.gate_assessments
        )
        or not isinstance(result.evidence_refs, tuple)
        or any(not isinstance(item, str) for item in result.evidence_refs)
    ):
        return False
    assessments = tuple(item.gate_id for item in result.gate_assessments)
    model_ids = tuple(entry.entry_id for entry in result.learner_model_delta)
    model_refs_valid = all(
        _valid_model_entry(contract, entry, known_turn_ids)
        for entry in result.learner_model_delta
    )
    gates_valid = all(
        _valid_gate_assessment(contract, item, known_turn_ids)
        for item in result.gate_assessments
    )
    return (
        all(
            text.strip()
            for text in (
                result.heard,
                result.one_step_further,
                result.question_id,
                result.question,
            )
        )
        and model_refs_valid
        and len(model_ids) == len(set(model_ids))
        and gates_valid
        and isinstance(result.question_intent, QuestionIntent)
        and _valid_evidence(evidence)
        and bool(result.evidence_refs)
        and len(result.evidence_refs) == len(set(result.evidence_refs))
        and set(result.evidence_refs).issubset(contract.evidence_refs)
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
) -> bool:
    if (
        not isinstance(topic, TopicRunState)
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
        or any(not isinstance(item, GateAssessment) for item in topic.gates)
        or type(topic.learner_model) is not tuple
        or any(
            not isinstance(item, LearnerModelEntry)
            for item in topic.learner_model
        )
        or type(topic.learner_turn_ids) is not tuple
        or any(
            not isinstance(item, str) or not item.strip()
            for item in topic.learner_turn_ids
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
                or not isinstance(topic.last_learner_text, str)
                or not topic.last_learner_text.strip()
            )
        )
        or sum(
            item is not None
            for item in (topic.current_agent_turn, topic.unresolved_trigger)
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
    if topic.unresolved_trigger is not None:
        trigger = topic.unresolved_trigger
        if (
            not isinstance(trigger.kind, TriggerKind)
            or not _valid_trigger(
                trigger,
                trigger.kind,
                topic.contract.contract_digest,
                topic.evidence_digest,
            )
        ):
            return False
    return True


def _start_session(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if not isinstance(command, StartSession):
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
    if not isinstance(command, PresentCandidates):
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.phase is not ConversationPhase.WAITING_HOST or state.active_topic:
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


def _start_topic(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if not isinstance(command, StartTopic):
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.phase is not ConversationPhase.CHOOSING_TOPIC or state.active_topic:
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
    return _accept(
        command.command_id,
        TopicStarted(command.contract, context.evidence, trigger),
    )


def _commit_agent_turn(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if not isinstance(command, CommitAgentTurn):
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.phase is not ConversationPhase.WAITING_HOST or not state.active_topic:
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.active_topic.open_question_id is not None:
        return Rejected("TOPIC_STATE_CONFLICT")
    topic = state.active_topic
    if topic.evidence_health not in PUBLISHABLE_EVIDENCE:
        return Rejected("EVIDENCE_STALE")
    trigger = context.trigger
    if (
        trigger is None
        or trigger != topic.unresolved_trigger
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
    if not isinstance(command, SubmitLearnerTurn):
        return Rejected("TOPIC_STATE_CONFLICT")
    topic = state.active_topic
    if (
        state.phase is not ConversationPhase.AWAITING_USER
        or topic is None
        or topic.open_question_id != command.question_id
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    if not command.learner_turn_id.strip() or not command.text.strip():
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
    if not isinstance(command, PauseTopic):
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.active_topic is None:
        return Rejected("TOPIC_STATE_CONFLICT")
    return _accept(command.command_id, TopicPaused(state.active_topic.topic_run_id))


def _resume_topic(
    state: DialogueState, command: DialogueCommand, context: DecisionContext
) -> Decision:
    if not isinstance(command, ResumeTopic):
        return Rejected("TOPIC_STATE_CONFLICT")
    if state.phase is not ConversationPhase.NONE or state.active_topic is not None:
        return Rejected("TOPIC_STATE_CONFLICT")
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
    elif topic.unresolved_trigger is not None:
        resumed_trigger = _matching_trigger(
            resumed_trigger,
            topic.unresolved_trigger.kind,
            topic.contract.contract_digest,
            context.evidence.evidence_digest,
        )
        if (
            resumed_trigger is None
            or resumed_trigger.work_id == topic.unresolved_trigger.work_id
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


TRANSITION_TABLE: dict[type, Handler] = {
    StartSession: _start_session,
    PresentCandidates: _present_candidates,
    StartTopic: _start_topic,
    CommitAgentTurn: _commit_agent_turn,
    SubmitLearnerTurn: _submit_turn,
    PauseTopic: _pause_topic,
    ResumeTopic: _resume_topic,
}


def decide(
    state: DialogueState,
    command: DialogueCommand,
    context: DecisionContext,
) -> Decision:
    """Validate a command against one immutable snapshot."""
    if context.registry_generation != state.registry_generation:
        return Rejected("SESSION_DEACTIVATED")
    if (
        not isinstance(command, StartSession)
        and state.lifecycle is not SessionLifecycle.OPEN
    ):
        return Rejected("TOPIC_STATE_CONFLICT")
    handler = TRANSITION_TABLE.get(type(command))
    if handler is None:
        return Rejected("TOPIC_STATE_CONFLICT")
    return handler(state, command, context)


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("ILLEGAL_EVENT_TRANSITION")


def _validate(state: DialogueState) -> None:
    topic = state.active_topic
    if (
        not isinstance(state.lifecycle, SessionLifecycle)
        or not isinstance(state.phase, ConversationPhase)
        or type(state.candidates) is not tuple
        or type(state.paused_topics) is not tuple
        or any(
            not isinstance(item, TopicRunState)
            for item in state.paused_topics
        )
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.lifecycle is SessionLifecycle.NEW and (
        state.phase is not ConversationPhase.NONE or topic is not None
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.lifecycle is SessionLifecycle.ENDED and (
        state.phase is not ConversationPhase.NONE or topic is not None
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
    if (
        state.phase is not ConversationPhase.CHOOSING_TOPIC
        and state.candidates
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.phase is ConversationPhase.AWAITING_USER:
        if (
            topic is None
            or topic.current_agent_turn is None
            or topic.unresolved_trigger is not None
            or topic.evidence_health not in PUBLISHABLE_EVIDENCE
        ):
            raise ValueError("STATE_INVARIANT_VIOLATION")
    active_trigger = topic.unresolved_trigger if topic is not None else None
    if state.phase is ConversationPhase.WAITING_HOST:
        if sum(
            item is not None
            for item in (state.session_unresolved_trigger, active_trigger)
        ) != 1:
            raise ValueError("STATE_INVARIANT_VIOLATION")
        if topic is not None and topic.current_agent_turn is not None:
            raise ValueError("STATE_INVARIANT_VIOLATION")
    elif state.session_unresolved_trigger is not None:
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if state.session_unresolved_trigger is not None and not _valid_trigger(
        state.session_unresolved_trigger,
        TriggerKind.TOPIC_CANDIDATES,
        None,
        state.session_unresolved_trigger.evidence_digest,
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if topic is not None and not _valid_topic_state(
        topic,
        TopicLifecycle.ACTIVE,
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if not all(
        _valid_topic_state(item, TopicLifecycle.PAUSED)
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


def reduce(
    state: DialogueState, event: CommittedDialogueEvent
) -> DialogueState:
    """Apply one committed fact or fail closed without mutation."""
    _validate(state)
    if event.sequence != state.sequence + 1:
        raise ValueError("EVENT_SEQUENCE_GAP")
    if event.from_version != state.conversation_version:
        raise ValueError("EVENT_VERSION_CONFLICT")
    if event.to_version != event.from_version + 1:
        raise ValueError("EVENT_VERSION_CONFLICT")

    payload = event.payload
    next_state = replace(
        state,
        sequence=event.sequence,
        conversation_version=event.to_version,
    )
    if isinstance(payload, SessionStarted):
        _require(
            state.lifecycle is SessionLifecycle.NEW
            and state.phase is ConversationPhase.NONE
            and state.session_unresolved_trigger is None
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
            session_unresolved_trigger=payload.candidate_trigger,
        )
    elif isinstance(payload, CandidatesPresented):
        _require(
            state.phase is ConversationPhase.WAITING_HOST
            and state.active_topic is None
            and _valid_candidates(payload.candidates)
            and payload.trigger == state.session_unresolved_trigger
        )
        next_state = replace(
            next_state,
            candidates=payload.candidates,
            phase=ConversationPhase.CHOOSING_TOPIC,
            session_unresolved_trigger=None,
        )
    elif isinstance(payload, TopicStarted):
        _require(
            state.phase is ConversationPhase.CHOOSING_TOPIC
            and state.active_topic is None
            and state.session_unresolved_trigger is None
        )
        contract = payload.contract
        expected_kind = (
            TriggerKind.INITIAL_TURN
            if payload.evidence.health in PUBLISHABLE_EVIDENCE
            else TriggerKind.REGROUND
        )
        _require(
            _valid_contract(contract)
            and _valid_evidence(payload.evidence)
            and _valid_trigger(
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
            unresolved_trigger=payload.initial_trigger,
        )
        next_state = replace(
            next_state,
            active_topic=topic,
            candidates=(),
            phase=ConversationPhase.WAITING_HOST,
        )
    elif isinstance(payload, AgentTurnCommitted):
        _require(
            state.phase is ConversationPhase.WAITING_HOST
            and state.active_topic is not None
            and state.active_topic.open_question_id is None
            and state.active_topic.evidence_health in PUBLISHABLE_EVIDENCE
            and payload.trigger == state.active_topic.unresolved_trigger
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
                unresolved_trigger=None,
            ),
            phase=ConversationPhase.AWAITING_USER,
        )
    elif isinstance(payload, LearnerTurnSubmitted):
        _require(
            state.phase is ConversationPhase.AWAITING_USER
            and state.active_topic is not None
            and state.active_topic.open_question_id == payload.question_id
            and bool(payload.learner_turn_id.strip())
            and bool(payload.text.strip())
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
                unresolved_trigger=payload.next_trigger,
                learner_turn_ids=(
                    *topic.learner_turn_ids,
                    payload.learner_turn_id,
                ),
                last_learner_turn_id=payload.learner_turn_id,
                last_learner_text=payload.text,
            ),
            phase=ConversationPhase.WAITING_HOST,
        )
    elif isinstance(payload, TopicPaused):
        _require(
            state.active_topic is not None
            and state.active_topic.topic_run_id == payload.topic_run_id
        )
        topic = cast(TopicRunState, state.active_topic)
        paused = replace(topic, lifecycle=TopicLifecycle.PAUSED)
        next_state = replace(
            next_state,
            active_topic=None,
            paused_topics=(*next_state.paused_topics, paused),
            phase=ConversationPhase.NONE,
        )
    elif isinstance(payload, TopicResumed):
        _require(
            state.phase is ConversationPhase.NONE
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
        _require(_valid_evidence(payload.evidence))
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
                unresolved_trigger=payload.resumed_trigger,
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
            phase = ConversationPhase.AWAITING_USER
        elif active.unresolved_trigger is not None:
            _require(payload.resumed_trigger is not None)
            resumed_trigger = cast(TriggerBinding, payload.resumed_trigger)
            _require(
                _valid_trigger(
                    resumed_trigger,
                    active.unresolved_trigger.kind,
                    active.contract.contract_digest,
                    payload.evidence.evidence_digest,
                )
                and resumed_trigger.work_id != active.unresolved_trigger.work_id
            )
            active = replace(active, unresolved_trigger=resumed_trigger)
            phase = ConversationPhase.WAITING_HOST
        else:
            raise ValueError("STATE_INVARIANT_VIOLATION")
        next_state = replace(
            next_state,
            active_topic=active,
            paused_topics=remaining,
            phase=phase,
        )
    else:
        raise ValueError("UNKNOWN_EVENT")

    _validate(next_state)
    return next_state
```

- [x] **Step 4: Run GREEN**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_xsync_v2_state_machine -v`

Expected: PASS, 19 tests after the envelope, nested-payload, and resumed-trigger lineage regressions.

- [ ] **Step 5: Commit**

```bash
git add skills/x-sync/scripts/xsync_v2/state_machine.py tests/test_xsync_v2_state_machine.py
git commit -m "feat: add explicit v2 dialogue state machine"
```

### Task 3: Read-only after-commit Observer Hub

**Files:**
- Create: `skills/x-sync/scripts/xsync_v2/observer.py`
- Create: `tests/test_xsync_v2_observer.py`

- [x] **Step 1: Write the failing Observer contract tests**

Create `tests/test_xsync_v2_observer.py`:

```python
from dataclasses import FrozenInstanceError
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.observer import (
    CommittedBatch,
    CommittedEventView,
    ImmutablePayloadView,
    ObserverHub,
    StreamKind,
)


class NoLocks:
    def assert_none_held(self):
        return None


class HeldLock:
    def assert_none_held(self):
        raise RuntimeError("DOMAIN_LOCK_HELD")


class Recorder:
    def __init__(
        self, name, calls, fail=False, recurse=None, accepted_streams=None
    ):
        self.name = name
        self.accepted_streams = (
            accepted_streams
            if accepted_streams is not None
            else frozenset({StreamKind.DIALOGUE})
        )
        self.calls = calls
        self.fail = fail
        self.recurse = recurse

    def on_batch(self, batch):
        self.calls.append(
            (self.name, tuple(event.event_id for event in batch.events))
        )
        if self.recurse:
            self.recurse(batch)
        if self.fail:
            raise RuntimeError("observer failed")


def event(event_id="e1", sequence=1):
    return CommittedEventView(
        event_id=event_id,
        sequence=sequence,
        payload=ImmutablePayloadView(
            tag="topic_paused",
            fields=(("topic_run_id", "topic-1"),),
        ),
    )


def batch(*events, kind=StreamKind.DIALOGUE, stream_id="dlg-1"):
    items = events or (event(),)
    return CommittedBatch(kind, stream_id, items)


class ObserverTest(unittest.TestCase):
    def test_failure_is_isolated_and_order_is_registration_independent(self):
        calls = []
        hub = ObserverHub(NoLocks())
        hub.register_fixed(Recorder("z-good", calls))
        hub.register_fixed(Recorder("a-fail", calls, fail=True))
        hub.freeze()
        report = hub.publish(batch())
        self.assertEqual(("a-fail", "z-good"), report.attempted)
        self.assertEqual(("a-fail",), report.failed)
        self.assertEqual(
            [("a-fail", ("e1",)), ("z-good", ("e1",))], calls
        )

    def test_duplicate_event_is_not_redelivered_to_same_observer(self):
        calls = []
        hub = ObserverHub(NoLocks())
        hub.register_fixed(Recorder("recorder", calls))
        hub.freeze()
        hub.publish(batch())
        hub.publish(batch())
        self.assertEqual([("recorder", ("e1",))], calls)

    def test_partial_duplicate_delivers_only_unseen_events(self):
        calls = []
        hub = ObserverHub(NoLocks())
        hub.register_fixed(Recorder("recorder", calls))
        hub.freeze()
        first = event("e1", 1)
        second = event("e2", 2)
        hub.publish(batch(first))
        hub.publish(batch(first, second))
        self.assertEqual(
            [("recorder", ("e1",)), ("recorder", ("e2",))], calls
        )

    def test_observer_receives_only_declared_stream_kinds(self):
        calls = []
        hub = ObserverHub(NoLocks())
        hub.register_fixed(
            Recorder(
                "registry-only",
                calls,
                accepted_streams=frozenset({StreamKind.REGISTRY}),
            )
        )
        hub.freeze()
        dialogue_report = hub.publish(batch())
        registry_report = hub.publish(
            batch(kind=StreamKind.REGISTRY, stream_id="registry")
        )
        self.assertEqual((), dialogue_report.attempted)
        self.assertEqual(("registry-only",), registry_report.attempted)
        self.assertEqual([("registry-only", ("e1",))], calls)

    def test_cross_batch_sequence_gap_fails_closed(self):
        calls = []
        hub = ObserverHub(NoLocks())
        hub.register_fixed(Recorder("recorder", calls))
        hub.freeze()
        hub.publish(batch(event("e1", 1)))
        report = hub.publish(batch(event("e3", 3)))
        self.assertEqual(("recorder",), report.failed)
        self.assertEqual("OBSERVER_SEQUENCE_GAP", report.errors[0][1])
        self.assertEqual([("recorder", ("e1",))], calls)

    def test_publish_requires_all_domain_locks_released(self):
        hub = ObserverHub(HeldLock())
        hub.register_fixed(Recorder("recorder", []))
        hub.freeze()
        with self.assertRaisesRegex(RuntimeError, "DOMAIN_LOCK_HELD"):
            hub.publish(batch())

    def test_recursive_publish_and_command_entry_are_rejected(self):
        calls = []
        hub = ObserverHub(NoLocks())
        observer = Recorder("recorder", calls, recurse=hub.publish)
        hub.register_fixed(observer)
        hub.freeze()
        report = hub.publish(batch())
        self.assertEqual(("recorder",), report.failed)
        self.assertEqual("OBSERVER_REENTRANCY", report.errors[0][1])

    def test_event_view_is_immutable(self):
        event = batch().events[0]
        with self.assertRaises(FrozenInstanceError):
            event.sequence = 9
        with self.assertRaisesRegex(ValueError, "INVALID_STREAM_KIND"):
            CommittedBatch("dialogue", "dlg-1", (event,))
        with self.assertRaisesRegex(ValueError, "MUTABLE_PAYLOAD_VIEW"):
            ImmutablePayloadView("topic_paused", [["id", "topic-1"]])
        with self.assertRaisesRegex(ValueError, "INVALID_EVENT_VIEW"):
            CommittedEventView("e2", 2, {"topic_run_id": "topic-1"})
        with self.assertRaisesRegex(ValueError, "INVALID_COMMITTED_BATCH"):
            CommittedBatch(StreamKind.DIALOGUE, "dlg-1", [event])

    def test_registration_and_batch_guards_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "MUTABLE_PAYLOAD_VIEW"):
            ImmutablePayloadView("", ())
        valid = event()
        with self.assertRaisesRegex(ValueError, "INVALID_COMMITTED_BATCH"):
            CommittedBatch(StreamKind.DIALOGUE, "dlg-1", (valid, object()))
        with self.assertRaisesRegex(ValueError, "INVALID_COMMITTED_BATCH"):
            CommittedBatch(StreamKind.DIALOGUE, "dlg-1", (valid, valid))
        with self.assertRaisesRegex(ValueError, "EVENT_SEQUENCE_GAP"):
            CommittedBatch(
                StreamKind.DIALOGUE,
                "dlg-1",
                (valid, event("e3", 3)),
            )
        hub = ObserverHub(NoLocks())
        observer = Recorder("recorder", [])
        hub.register_fixed(observer)
        with self.assertRaisesRegex(ValueError, "DUPLICATE_OBSERVER"):
            hub.register_fixed(observer)
        with self.assertRaisesRegex(ValueError, "INVALID_OBSERVER_STREAMS"):
            hub.register_fixed(
                Recorder("invalid", [], accepted_streams=frozenset())
            )
        with self.assertRaisesRegex(RuntimeError, "OBSERVER_REGISTRY_NOT_FROZEN"):
            hub.publish(batch())
        hub.freeze()
        with self.assertRaisesRegex(RuntimeError, "OBSERVER_REGISTRY_FROZEN"):
            hub.register_fixed(Recorder("late", []))


if __name__ == "__main__":
    unittest.main()
```

- [x] **Step 2: Run RED**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_xsync_v2_observer -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'xsync_v2.observer'`.

- [x] **Step 3: Implement immutable dispatch and failure isolation**

Create `skills/x-sync/scripts/xsync_v2/observer.py`:

```python
"""After-commit, read-only Observer delivery for X-Sync v2."""

from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from enum import StrEnum
from itertools import pairwise
from typing import Protocol

class StreamKind(StrEnum):
    DIALOGUE = "dialogue"
    REGISTRY = "registry"


@dataclass(frozen=True, slots=True)
class ImmutablePayloadView:
    tag: str
    fields: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if not self.tag.strip() or type(self.fields) is not tuple:
            raise ValueError("MUTABLE_PAYLOAD_VIEW")
        if any(
            type(item) is not tuple
            or len(item) != 2
            or any(not isinstance(value, str) for value in item)
            for item in self.fields
        ):
            raise ValueError("MUTABLE_PAYLOAD_VIEW")


@dataclass(frozen=True, slots=True)
class CommittedEventView:
    event_id: str
    sequence: int
    payload: ImmutablePayloadView

    def __post_init__(self) -> None:
        if (
            not self.event_id.strip()
            or self.sequence < 1
            or not isinstance(self.payload, ImmutablePayloadView)
        ):
            raise ValueError("INVALID_EVENT_VIEW")


@dataclass(frozen=True, slots=True)
class CommittedBatch:
    stream_kind: StreamKind
    stream_id: str
    events: tuple[CommittedEventView, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.stream_kind, StreamKind):
            raise ValueError("INVALID_STREAM_KIND")
        if (
            not self.stream_id.strip()
            or type(self.events) is not tuple
            or not self.events
        ):
            raise ValueError("INVALID_COMMITTED_BATCH")
        if any(not isinstance(event, CommittedEventView) for event in self.events):
            raise ValueError("INVALID_COMMITTED_BATCH")
        event_ids = tuple(event.event_id for event in self.events)
        if any(not event_id.strip() for event_id in event_ids) or len(
            event_ids
        ) != len(set(event_ids)):
            raise ValueError("INVALID_COMMITTED_BATCH")
        sequences = tuple(event.sequence for event in self.events)
        if any(right != left + 1 for left, right in pairwise(sequences)):
            raise ValueError("EVENT_SEQUENCE_GAP")


@dataclass(frozen=True, slots=True)
class DispatchReport:
    attempted: tuple[str, ...]
    failed: tuple[str, ...]
    errors: tuple[tuple[str, str], ...]


class DomainLockTracker(Protocol):
    def assert_none_held(self) -> None: ...


class Observer(Protocol):
    name: str
    accepted_streams: frozenset[StreamKind]

    def on_batch(self, batch: CommittedBatch) -> None: ...


class ObserverHub:
    """Dispatch immutable batches after all domain locks are released."""

    def __init__(self, lock_tracker: DomainLockTracker):
        self._lock_tracker = lock_tracker
        self._observers: dict[str, Observer] = {}
        self._seen: dict[str, set[tuple[StreamKind, str, str]]] = {}
        self._cursor: dict[tuple[str, StreamKind, str], int] = {}
        self._frozen = False
        self._local = threading.local()
        self._dispatch_lock = threading.Lock()

    def register_fixed(self, observer: Observer) -> None:
        """Register one startup-time Observer before freezing the hub."""
        if self._frozen:
            raise RuntimeError("OBSERVER_REGISTRY_FROZEN")
        if observer.name in self._observers:
            raise ValueError("DUPLICATE_OBSERVER")
        if (
            type(observer.accepted_streams) is not frozenset
            or not observer.accepted_streams
            or any(
                not isinstance(item, StreamKind)
                for item in observer.accepted_streams
            )
        ):
            raise ValueError("INVALID_OBSERVER_STREAMS")
        self._observers[observer.name] = observer
        self._seen[observer.name] = set()

    def freeze(self) -> None:
        """Close the Observer registry before runtime dispatch starts."""
        self._frozen = True

    def assert_command_entry_allowed(self) -> None:
        """Reject a command entry attempted recursively from an Observer."""
        if getattr(self._local, "dispatching", False):
            raise RuntimeError("OBSERVER_REENTRANCY")

    def publish(self, batch: CommittedBatch) -> DispatchReport:
        """Deliver one committed batch with isolation and idempotency."""
        self._lock_tracker.assert_none_held()
        self.assert_command_entry_allowed()
        if not self._frozen:
            raise RuntimeError("OBSERVER_REGISTRY_NOT_FROZEN")
        with self._dispatch_lock:
            return self._publish_serial(batch)

    def _publish_serial(self, batch: CommittedBatch) -> DispatchReport:
        attempted = []
        failed = []
        errors = []
        self._local.dispatching = True
        try:
            for name in sorted(self._observers):
                observer = self._observers[name]
                if batch.stream_kind not in observer.accepted_streams:
                    continue
                unseen_events = tuple(
                    event for event in batch.events
                    if (
                        batch.stream_kind,
                        batch.stream_id,
                        event.event_id,
                    ) not in self._seen[name]
                )
                if not unseen_events:
                    continue
                attempted.append(name)
                cursor_key = (name, batch.stream_kind, batch.stream_id)
                cursor = self._cursor.get(cursor_key, 0)
                has_sequence_conflict = any(
                    event.sequence <= cursor
                    for event in unseen_events
                )
                has_gap = unseen_events[0].sequence != cursor + 1
                if has_sequence_conflict or has_gap:
                    failed.append(name)
                    errors.append((name, "OBSERVER_SEQUENCE_GAP"))
                    continue
                delivery = replace(batch, events=unseen_events)
                try:
                    observer.on_batch(delivery)
                except Exception as exc:
                    failed.append(name)
                    errors.append((name, str(exc)))
                else:
                    self._seen[name].update(
                        (
                            batch.stream_kind,
                            batch.stream_id,
                            event.event_id,
                        )
                        for event in unseen_events
                    )
                    self._cursor[cursor_key] = unseen_events[-1].sequence
        finally:
            self._local.dispatching = False
        return DispatchReport(tuple(attempted), tuple(failed), tuple(errors))
```

- [x] **Step 4: Run GREEN**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_xsync_v2_observer -v`

Expected: PASS, 13 tests after the event-id/sequence integrity, deep-immutability, and frozen-registration regressions.

- [ ] **Step 5: Commit**

```bash
git add skills/x-sync/scripts/xsync_v2/observer.py tests/test_xsync_v2_observer.py
git commit -m "feat: add isolated v2 observer hub"
```

### Task 4: Architecture gate and v1 regression freeze

**Files:**
- Create: `skills/x-sync/pyproject.toml`
- Create: `skills/x-sync/requirements-dev.txt`
- Create: `tests/test_xsync_v2_architecture.py`

- [x] **Step 1: Pin the quality tools and write the scoped policy**

Create `skills/x-sync/requirements-dev.txt`:

```text
coverage==7.15.4
mypy==2.3.0
ruff==0.16.3
```

Create `skills/x-sync/pyproject.toml`:

```toml
[tool.ruff]
line-length = 88
target-version = "py311"

[tool.ruff.lint]
select = ["B", "E", "F", "RUF", "UP"]

[tool.mypy]
python_version = "3.11"
show_error_codes = true
strict = true
warn_unreachable = true

[tool.coverage.run]
branch = true
source = ["skills/x-sync/scripts/xsync_v2"]

[tool.coverage.report]
fail_under = 90
show_missing = true
skip_covered = true
```

- [x] **Step 2: Write the failing architecture test**

Create `tests/test_xsync_v2_architecture.py`:

```python
import ast
import hashlib
import inspect
from pathlib import Path
from typing import get_args
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.domain import DialogueCommand
from xsync_v2.observer import ObserverHub
from xsync_v2.state_machine import TRANSITION_TABLE, decide, reduce


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "skills" / "x-sync" / "scripts" / "xsync_v2"
V1_RUNTIME = ROOT / "skills" / "x-sync" / "scripts" / "xsync.py"
V1_RUNTIME_SHA256 = (
    "9fa72d3b3c9a9fd8ebf03c9653d70d5c78a518e3cc74edf07a07eac0dd6baf5b"
)


def imports(path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


class ArchitectureTest(unittest.TestCase):
    def test_domain_and_state_machine_have_no_io_dependencies(self):
        forbidden = {
            "asyncio", "http", "os", "pathlib", "random", "socket",
            "subprocess", "time", "urllib",
        }
        for name in ("domain.py", "state_machine.py"):
            self.assertFalse(forbidden & imports(PACKAGE / name), name)

    def test_observer_has_no_command_or_writer_dependency(self):
        text = (PACKAGE / "observer.py").read_text(encoding="utf-8")
        self.assertNotIn("CommandService", text)
        self.assertNotIn("EventWriter", text)
        self.assertNotIn("decide(", text)
        self.assertNotIn("reduce(", text)

    def test_transition_table_is_closed_and_exhaustive(self):
        self.assertEqual(
            frozenset(get_args(DialogueCommand)),
            frozenset(TRANSITION_TABLE),
        )

    def test_public_kernel_api_is_documented(self):
        public_api = (
            decide,
            reduce,
            ObserverHub,
            ObserverHub.register_fixed,
            ObserverHub.freeze,
            ObserverHub.assert_command_entry_allowed,
            ObserverHub.publish,
        )
        self.assertTrue(all(inspect.getdoc(item) for item in public_api))

    def test_v1_runtime_bytes_are_frozen_for_this_slice(self):
        digest = hashlib.sha256(V1_RUNTIME.read_bytes()).hexdigest()
        self.assertEqual(V1_RUNTIME_SHA256, digest)


if __name__ == "__main__":
    unittest.main()
```

- [x] **Step 3: Run the architecture test**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_xsync_v2_architecture -v`

Expected: PASS, 5 tests. If it fails, fix the violating source from Tasks 1–3 before continuing; do not weaken the forbidden boundary.

- [x] **Step 4: Run syntax, quality, focused, and full regression gates**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m compileall -q skills/x-sync/scripts/xsync_v2
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  tests.test_xsync_v2_domain \
  tests.test_xsync_v2_state_machine \
  tests.test_xsync_v2_observer \
  tests.test_xsync_v2_architecture -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_*.py' -v
quality_root=$(mktemp -d)
python3 -m venv "$quality_root/venv"
"$quality_root/venv/bin/python" -m pip install \
  -r skills/x-sync/requirements-dev.txt
"$quality_root/venv/bin/python" -m ruff check \
  --config skills/x-sync/pyproject.toml \
  skills/x-sync/scripts/xsync_v2
"$quality_root/venv/bin/python" -m mypy \
  --config-file skills/x-sync/pyproject.toml \
  skills/x-sync/scripts/xsync_v2
"$quality_root/venv/bin/python" -m coverage erase
"$quality_root/venv/bin/python" -m coverage run \
  --rcfile=skills/x-sync/pyproject.toml \
  -m unittest \
  tests.test_xsync_v2_domain \
  tests.test_xsync_v2_state_machine \
  tests.test_xsync_v2_observer \
  tests.test_xsync_v2_architecture
"$quality_root/venv/bin/python" -m coverage report \
  --rcfile=skills/x-sync/pyproject.toml
git diff --check
```

Expected: compileall exits 0; Ruff and strict mypy report no issues; branch coverage is at least 90%; the focused suite passes 39 tests; the full suite passes the current 83 tests (69 v1 plus 14 Phase 0) plus the 39 new tests; `git diff --check` prints nothing.

- [x] **Step 5: Review the pattern boundary**

Run:

```bash
if rg -n 'open\(|write_text|subprocess|urllib|socket|CommandService|EventWriter' \
  skills/x-sync/scripts/xsync_v2/domain.py \
  skills/x-sync/scripts/xsync_v2/state_machine.py \
  skills/x-sync/scripts/xsync_v2/observer.py
then
  echo "forbidden kernel dependency found" >&2
  exit 1
fi
```

Expected: no matches. Confirm manually that every state mutation is inside `reduce`, every command goes through `decide`, and Observer callbacks receive only frozen records.

- [ ] **Step 6: Commit the architecture gate**

```bash
git add \
  skills/x-sync/pyproject.toml \
  skills/x-sync/requirements-dev.txt \
  tests/test_xsync_v2_architecture.py
git commit -m "test: enforce v2 state-machine boundaries"
```

## Completion gate

This plan is complete only when all 122 discovered tests pass, Ruff and strict mypy are clean, core branch coverage is at least 90%, the frozen v1 runtime hash still matches, the transition table exactly covers the closed command union, the pure domain imports no I/O dependency, fixed-seed generated legal sequences and damaged replays are proven, stale evidence downgrades supported gates and confirmed model entries, paused unresolved triggers retain their parent and input-digest lineage, stale resume cannot reopen an old question, malformed envelopes and nested payloads plus mutable DTO subclasses fail closed, Observer callbacks run after the injected lock check, failures are isolated, partial duplicate delivery sends only unseen events, conflicting event-id or sequence identities fail closed, observer registrations remain frozen, per-stream gaps fail closed, stream kinds are typed and declared, and recursive publish is rejected. The eight external Phase 0 MCP tests remain a separate host-protocol regression gate, for 130 passing tests across both gates.

This slice does not claim durable Observer cursors or crash recovery. After it is committed, invoke `superpowers:writing-plans` separately for: (1) secure delta event store and registry fencing, then (2) fixed durable observers and effect executor. Those plans must use the real interfaces produced here and the Phase 0 Host verdict.

## Execution handoff after Phase 0

Plan complete and saved to `docs/superpowers/plans/2026-08-15-warm-dialogue-state-machine-observers.md`. After Phase 0 passes, choose:

1. **Subagent-Driven (recommended):** use `superpowers:subagent-driven-development`, one fresh implementation agent per task with specification and code-quality review.
2. **Inline Execution:** use `superpowers:executing-plans`, execute each task in order and stop at every verification gate.
