# ruff: noqa: RUF001

import hashlib
import random
import unittest
from dataclasses import fields, replace
from typing import cast

import tests.xsync_v2_path  # noqa: F401

# isort: split
from xsync_v2.domain import (
    Accepted,
    AgentTurnCommitted,
    AgentTurnResult,
    CandidatesPresented,
    CommitAgentTurn,
    CommittedDialogueEvent,
    ConversationPhase,
    CurrentWorkState,
    DecisionContext,
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
    PresentCandidates,
    QuestionIntent,
    RecoverWork,
    Rejected,
    ReportWorkFailure,
    ResumeTopic,
    SelectTopic,
    SessionLifecycle,
    SessionStarted,
    StartSession,
    StartTopic,
    SubmitLearnerTurn,
    TaskScope,
    TopicContract,
    TopicLifecycle,
    TopicPaused,
    TopicResumed,
    TopicSelectionSubmitted,
    TopicStarted,
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
    initial_dialogue_state,
)
from xsync_v2.state_machine import (
    MAX_AUTOMATIC_WORK_ATTEMPTS,
    TRANSITION_TABLE,
    _validate,
    conversation_version_delta,
    decide,
    reduce,
)
from xsync_v2.work_identity import derive_canonical_work_id


def digest(label: str) -> str:
    return f"sha256:{hashlib.sha256(label.encode()).hexdigest()}"


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
        contract_digest=digest("contract"),
    )


def context(
    kind=None,
    work_id="work-1",
    evidence_health=EvidenceHealth.CURRENT,
    parent_turn_id=None,
    input_digest=None,
):
    exact = (
        digest("exact")
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
                if kind
                in {TriggerKind.TOPIC_CANDIDATES, TriggerKind.TOPIC_SELECTION}
                else digest("contract")
            ),
            input_digest=(
                digest(f"input:{work_id}")
                if input_digest is None
                else input_digest
            ),
            evidence_digest=digest("evidence"),
        )
    return DecisionContext(
        registry_generation=1,
        trigger=trigger,
        evidence=EvidenceCheck(
            health=evidence_health,
            evidence_digest=digest("evidence"),
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


def work_failure(
    label: str,
    category: WorkFailureCategory = WorkFailureCategory.HOST_TRANSIENT,
) -> WorkFailure:
    return WorkFailure(
        f"failure-{label}",
        category,
        f"HOST_{label.upper()}",
        digest(f"proof:{label}"),
    )


def commit_for_test(state, pending):
    sequence = state.sequence + 1
    return CommittedDialogueEvent(
        event_id=f"event-{sequence}",
        sequence=sequence,
        from_version=state.conversation_version,
        to_version=(
            state.conversation_version
            + conversation_version_delta(pending.payload)
        ),
        command_id=pending.command_id,
        payload=pending.payload,
    )


def default_context(state, command):
    if isinstance(command, (StartSession, PresentCandidates)):
        return context(TriggerKind.TOPIC_CANDIDATES)
    if isinstance(command, SelectTopic):
        return context(TriggerKind.TOPIC_SELECTION)
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
                parent_turn_id=paused.unresolved_trigger.parent_turn_id,
                input_digest=paused.unresolved_trigger.input_digest,
            )
    return context()


def apply(state, command, decision_context=None):
    if decision_context is None:
        decision_context = default_context(state, command)
    decision = decide(state, command, decision_context)
    assert isinstance(decision, Accepted), decision
    for pending in decision.events:
        state = reduce(state, commit_for_test(state, pending))
    return state


class StateMachineTest(unittest.TestCase):
    def test_topic_selection_is_durable_work_before_host_contract(self):
        state = apply(initial_dialogue_state("dlg-1", 1), StartSession("start"))
        state = apply(
            state,
            PresentCandidates("candidates", ("支付一致性", "Outbox")),
        )
        invalid = SelectTopic("invalid", "不存在的话题")
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                invalid,
                context(TriggerKind.TOPIC_SELECTION),
            ),
        )
        selection_context = context(
            TriggerKind.TOPIC_SELECTION,
            work_id="selection-trigger",
        )
        state = apply(
            state,
            SelectTopic("select", "支付一致性"),
            selection_context,
        )
        self.assertEqual(ConversationPhase.WAITING_HOST, state.phase)
        self.assertEqual((), state.candidates)
        self.assertEqual("支付一致性", state.selected_candidate)
        self.assertIsNotNone(state.session_work)
        assert state.session_work is not None
        self.assertIs(
            TriggerKind.TOPIC_SELECTION,
            state.session_work.trigger.kind,
        )

        started = apply(
            state,
            StartTopic("start-topic", contract(), "支付一致性"),
            context(TriggerKind.INITIAL_TURN, work_id="initial-trigger"),
        )
        self.assertEqual(ConversationPhase.WAITING_HOST, started.phase)
        self.assertIsNone(started.selected_candidate)
        self.assertIsNone(started.session_work)
        self.assertIsNotNone(started.active_topic)

        forged = TopicSelectionSubmitted(
            "Outbox",
            cast(TriggerBinding, selection_context.trigger),
        )
        choosing = apply(
            apply(initial_dialogue_state("dlg-2", 1), StartSession("start-2")),
            PresentCandidates("candidates-2", ("支付一致性",)),
        )
        with self.assertRaisesRegex(ValueError, "ILLEGAL_EVENT_TRANSITION"):
            reduce(
                choosing,
                CommittedDialogueEvent(
                    "forged-selection",
                    choosing.sequence + 1,
                    choosing.conversation_version,
                    choosing.conversation_version + 1,
                    "forged",
                    forged,
                ),
            )

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
        self.assertEqual(
            "先写业务状态，再发事件。",
            state.active_topic.last_learner_text,
        )
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

    def test_work_failure_retry_dead_letter_and_explicit_recovery(self):
        state = apply(initial_dialogue_state("dlg-1", 1), StartSession("start"))
        self.assertIsInstance(state.session_work, CurrentWorkState)
        first = state.session_work
        assert first is not None
        initial_version = state.conversation_version

        retry_trigger = replace(first.trigger, work_id="trigger-attempt-2")
        failure_one = work_failure("one")
        decision = decide(
            state,
            ReportWorkFailure("fail-1", first.work_id, failure_one),
            DecisionContext(
                state.registry_generation,
                retry_trigger,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    first.trigger.evidence_digest,
                ),
            ),
        )
        self.assertIsInstance(decision, Accepted)
        assert isinstance(decision, Accepted)
        self.assertEqual(2, len(decision.events))
        self.assertIsInstance(decision.events[0].payload, WorkFailed)
        self.assertIsInstance(decision.events[1].payload, WorkRequeued)

        failed_event = commit_for_test(state, decision.events[0])
        self.assertEqual(failed_event.from_version, failed_event.to_version)
        failed_state = reduce(state, failed_event)
        assert failed_state.session_work is not None
        self.assertEqual(WorkStatus.FAILED, failed_state.session_work.status)
        requeued_event = commit_for_test(failed_state, decision.events[1])
        self.assertEqual(requeued_event.from_version, requeued_event.to_version)
        state = reduce(failed_state, requeued_event)
        second = state.session_work
        assert second is not None
        self.assertEqual(WorkStatus.QUEUED, second.status)
        self.assertEqual(2, second.attempt)
        self.assertEqual(failure_one, second.last_failure)
        self.assertNotEqual(first.work_id, second.work_id)
        self.assertEqual(initial_version, state.conversation_version)

        third_trigger = replace(second.trigger, work_id="trigger-attempt-3")
        state = apply(
            state,
            ReportWorkFailure(
                "fail-2",
                second.work_id,
                work_failure("two"),
            ),
            DecisionContext(
                state.registry_generation,
                third_trigger,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    second.trigger.evidence_digest,
                ),
            ),
        )
        third = state.session_work
        assert third is not None
        self.assertEqual(3, third.attempt)
        before_dead_letter_version = state.conversation_version

        terminal_failure = work_failure("three")
        terminal = decide(
            state,
            ReportWorkFailure("fail-3", third.work_id, terminal_failure),
            DecisionContext(
                state.registry_generation,
                None,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    third.trigger.evidence_digest,
                ),
            ),
        )
        self.assertIsInstance(terminal, Accepted)
        assert isinstance(terminal, Accepted)
        self.assertIsInstance(terminal.events[0].payload, WorkFailed)
        self.assertIsInstance(terminal.events[1].payload, WorkDeadLettered)
        state = apply(
            state,
            ReportWorkFailure("fail-3", third.work_id, terminal_failure),
            DecisionContext(
                state.registry_generation,
                None,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    third.trigger.evidence_digest,
                ),
            ),
        )
        dead = state.session_work
        assert dead is not None
        self.assertEqual(WorkStatus.DEAD_LETTER, dead.status)
        self.assertEqual(ConversationPhase.RECOVERABLE_ERROR, state.phase)
        self.assertEqual(
            before_dead_letter_version + 1,
            state.conversation_version,
        )
        self.assertEqual((WorkRecoveryAction.RETRY,), dead.allowed_recovery_actions)

        recovery_trigger = replace(dead.trigger, work_id="trigger-recovered")
        recovery = RecoverWork(
            "recover-1",
            dead.work_id,
            WorkRecoveryAction.RETRY,
        )
        recovery_decision = decide(
            state,
            recovery,
            DecisionContext(
                state.registry_generation,
                recovery_trigger,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    dead.trigger.evidence_digest,
                ),
            ),
        )
        self.assertIsInstance(recovery_decision, Accepted)
        assert isinstance(recovery_decision, Accepted)
        self.assertIsInstance(
            recovery_decision.events[0].payload,
            WorkRecoveryRequested,
        )
        recovered = apply(
            state,
            recovery,
            DecisionContext(
                state.registry_generation,
                recovery_trigger,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    dead.trigger.evidence_digest,
                ),
            ),
        )
        assert recovered.session_work is not None
        self.assertEqual(WorkStatus.QUEUED, recovered.session_work.status)
        self.assertEqual(1, recovered.session_work.attempt)
        self.assertIsNone(recovered.session_work.last_failure)
        self.assertNotEqual(dead.work_id, recovered.session_work.work_id)
        self.assertEqual(ConversationPhase.WAITING_HOST, recovered.phase)
        self.assertEqual(state.conversation_version + 1, recovered.conversation_version)

    def test_requeue_and_recovery_reject_noncanonical_triggers(self):
        opened = apply(initial_dialogue_state("dlg-1", 1), StartSession("start"))
        queued = opened.session_work
        assert queued is not None
        invalid_retry_triggers = (
            replace(queued.trigger, work_id="../escape"),
            replace(queued.trigger, work_id="x" * 129),
            replace(
                queued.trigger,
                work_id="trigger-bad-digest",
                input_digest="sha256:not-a-digest",
            ),
        )
        evidence = EvidenceCheck(
            EvidenceHealth.CURRENT,
            queued.trigger.evidence_digest,
        )
        for invalid in invalid_retry_triggers:
            with self.subTest(requeue_trigger=invalid):
                self.assertEqual(
                    Rejected("VALIDATION_FAILED"),
                    decide(
                        opened,
                        ReportWorkFailure(
                            "fail-invalid-trigger",
                            queued.work_id,
                            work_failure("invalid-trigger"),
                        ),
                        DecisionContext(
                            opened.registry_generation,
                            invalid,
                            evidence,
                        ),
                    ),
                )

        dead = apply(
            opened,
            ReportWorkFailure(
                "fail-permanent",
                queued.work_id,
                work_failure(
                    "permanent",
                    WorkFailureCategory.HOST_PERMANENT,
                ),
            ),
            DecisionContext(opened.registry_generation, None, evidence),
        )
        dead_work = dead.session_work
        assert dead_work is not None
        for invalid in invalid_retry_triggers:
            with self.subTest(recovery_trigger=invalid):
                self.assertEqual(
                    Rejected("VALIDATION_FAILED"),
                    decide(
                        dead,
                        RecoverWork(
                            "recover-invalid-trigger",
                            dead_work.work_id,
                            WorkRecoveryAction.RETRY,
                        ),
                        DecisionContext(
                            dead.registry_generation,
                            invalid,
                            evidence,
                        ),
                    ),
                )

    def test_failure_guards_and_version_neutral_policy_fail_closed(self):
        state = apply(initial_dialogue_state("dlg-1", 1), StartSession("start"))
        work = state.session_work
        assert work is not None
        failure = work_failure("guard")
        valid_trigger = replace(work.trigger, work_id="trigger-retry")
        valid_context = DecisionContext(
            state.registry_generation,
            valid_trigger,
            EvidenceCheck(EvidenceHealth.CURRENT, work.trigger.evidence_digest),
        )
        self.assertEqual(
            Rejected("WORK_SUPERSEDED"),
            decide(
                state,
                ReportWorkFailure("stale", "work-stale", failure),
                valid_context,
            ),
        )
        invalid_trigger = replace(valid_trigger, input_digest="different-input")
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                ReportWorkFailure("bad-trigger", work.work_id, failure),
                replace(valid_context, trigger=invalid_trigger),
            ),
        )
        decision = decide(
            state,
            ReportWorkFailure("valid", work.work_id, failure),
            valid_context,
        )
        assert isinstance(decision, Accepted)
        failed = commit_for_test(state, decision.events[0])
        with self.assertRaisesRegex(ValueError, "EVENT_VERSION_CONFLICT"):
            reduce(state, replace(failed, to_version=failed.from_version + 1))
        failed_state = reduce(state, failed)
        with self.assertRaisesRegex(ValueError, "ILLEGAL_EVENT_TRANSITION"):
            reduce(
                failed_state,
                replace(
                    commit_for_test(failed_state, decision.events[1]),
                    payload=replace(
                        decision.events[1].payload,
                        next_attempt=4,
                    ),
                ),
            )

        permanent = work_failure(
            "permanent",
            WorkFailureCategory.HOST_PERMANENT,
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(
                state,
                ReportWorkFailure("fatal", work.work_id, permanent),
                valid_context,
            ),
        )

    def test_dead_lettered_topic_can_be_paused_then_explicitly_regrounded(self):
        state = apply(initial_dialogue_state("dlg-1", 1), StartSession("start"))
        state = apply(state, PresentCandidates("candidates", ("支付一致性",)))
        state = apply(state, StartTopic("topic", contract()))
        topic = state.active_topic
        assert topic is not None and topic.work is not None
        work = topic.work
        terminal = work_failure(
            "unsupported",
            WorkFailureCategory.HOST_PERMANENT,
        )
        state = apply(
            state,
            ReportWorkFailure("dead", work.work_id, terminal),
            DecisionContext(
                state.registry_generation,
                None,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    work.trigger.evidence_digest,
                ),
            ),
        )
        topic = state.active_topic
        assert topic is not None and topic.work is not None
        self.assertEqual(
            (WorkRecoveryAction.RETRY, WorkRecoveryAction.REGROUND),
            topic.work.allowed_recovery_actions,
        )

        state = apply(state, PauseTopic("pause-dead"), context())
        self.assertEqual(ConversationPhase.NONE, state.phase)
        dead = state.paused_topics[0].work
        assert dead is not None
        next_trigger = TriggerBinding(
            TriggerKind.REGROUND,
            "trigger-reground",
            "epoch-2",
            dead.trigger.parent_turn_id,
            dead.trigger.contract_digest,
            digest("new-input"),
            digest("new-evidence"),
        )
        recovered = apply(
            state,
            RecoverWork(
                "reground",
                dead.work_id,
                WorkRecoveryAction.REGROUND,
            ),
            DecisionContext(
                state.registry_generation,
                next_trigger,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    digest("new-evidence"),
                ),
            ),
        )
        self.assertFalse(recovered.paused_topics)
        topic = recovered.active_topic
        assert topic is not None and topic.work is not None
        self.assertEqual(TopicLifecycle.ACTIVE, topic.lifecycle)
        self.assertEqual(WorkStatus.QUEUED, topic.work.status)
        self.assertEqual(TriggerKind.REGROUND, topic.work.trigger.kind)
        self.assertEqual(1, topic.work.attempt)
        self.assertEqual(digest("new-evidence"), topic.evidence_digest)
        self.assertEqual(ConversationPhase.WAITING_HOST, recovered.phase)

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
        with self.assertRaisesRegex(ValueError, "INVALID_EVENT_ENVELOPE"):
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
        with self.assertRaisesRegex(ValueError, "INVALID_EVENT_ENVELOPE"):
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

    def test_resume_preserves_the_saved_learner_reply_parent(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(state, StartTopic("c3", contract()))
        state = apply(state, CommitAgentTurn("c4", agent_turn()))
        state = apply(
            state,
            SubmitLearnerTurn("c5", "q1", "turn-1", "先落业务状态。"),
        )
        state = apply(state, PauseTopic("c6"))
        saved_trigger = state.paused_topics[0].unresolved_trigger
        self.assertIsNotNone(saved_trigger)
        if saved_trigger is None:
            self.fail("paused trigger missing")
        drifted = context(
            TriggerKind.LEARNER_REPLY,
            work_id="work-resumed",
            parent_turn_id="different-turn",
            input_digest=saved_trigger.input_digest,
        )
        command = ResumeTopic("c7", "topic-1")
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(state, command, drifted),
        )
        forged = CommittedDialogueEvent(
            event_id="event-forged-resume",
            sequence=state.sequence + 1,
            from_version=state.conversation_version,
            to_version=state.conversation_version + 1,
            command_id=command.command_id,
            payload=TopicResumed(
                "topic-1",
                False,
                drifted.trigger,
                drifted.evidence,
            ),
        )
        with self.assertRaisesRegex(ValueError, "ILLEGAL_EVENT_TRANSITION"):
            reduce(state, forged)

    def test_resume_rebinds_work_without_drifting_saved_input_digest(self):
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        state = apply(state, StartTopic("c3", contract()))
        state = apply(state, CommitAgentTurn("c4", agent_turn()))
        state = apply(
            state,
            SubmitLearnerTurn("c5", "q1", "turn-1", "先落业务状态。"),
        )
        state = apply(state, PauseTopic("c6"))
        saved_trigger = state.paused_topics[0].unresolved_trigger
        self.assertIsNotNone(saved_trigger)
        if saved_trigger is None:
            self.fail("paused trigger missing")

        resume = ResumeTopic("c7", "topic-1")
        rebound = context(
            saved_trigger.kind,
            work_id="work-rebound",
            parent_turn_id=saved_trigger.parent_turn_id,
            input_digest=saved_trigger.input_digest,
        )
        decision = decide(state, resume, rebound)
        self.assertIsInstance(decision, Accepted)
        resumed = reduce(state, commit_for_test(state, decision.events[0]))
        self.assertEqual(
            saved_trigger.input_digest,
            resumed.active_topic.unresolved_trigger.input_digest,
        )
        self.assertNotEqual(
            saved_trigger.work_id,
            resumed.active_topic.unresolved_trigger.work_id,
        )

        drifted = context(
            saved_trigger.kind,
            work_id="work-drifted",
            parent_turn_id=saved_trigger.parent_turn_id,
            input_digest=digest("different-input"),
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(state, ResumeTopic("c8", "topic-1"), drifted),
        )
        forged = CommittedDialogueEvent(
            event_id="event-forged-input",
            sequence=state.sequence + 1,
            from_version=state.conversation_version,
            to_version=state.conversation_version + 1,
            command_id="c8",
            payload=TopicResumed(
                "topic-1",
                False,
                drifted.trigger,
                drifted.evidence,
            ),
        )
        with self.assertRaisesRegex(ValueError, "ILLEGAL_EVENT_TRANSITION"):
            reduce(state, forged)

    def test_command_event_and_state_envelopes_fail_closed(self):
        initial = initial_dialogue_state("dlg-1", 1)
        start_context = context(TriggerKind.TOPIC_CANDIDATES)
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(initial, StartSession(""), start_context),
        )
        corrupt_states = (
            replace(initial, session_id=""),
            replace(initial, registry_generation=True),
            replace(initial, sequence=-1),
            replace(initial, conversation_version=-1),
        )
        for corrupt in corrupt_states:
            with self.subTest(state=corrupt):
                with self.assertRaisesRegex(
                    ValueError,
                    "STATE_INVARIANT_VIOLATION",
                ):
                    _validate(corrupt)
                self.assertEqual(
                    Rejected("STATE_INVARIANT_VIOLATION"),
                    decide(corrupt, StartSession("c1"), start_context),
                )

        decision = decide(initial, StartSession("c1"), start_context)
        self.assertIsInstance(decision, Accepted)
        committed = commit_for_test(initial, decision.events[0])
        invalid_events = (
            replace(committed, event_id=""),
            replace(
                committed,
                sequence=True,
                from_version=False,
                to_version=True,
            ),
            replace(committed, command_id=""),
        )
        for invalid in invalid_events:
            with self.subTest(event=invalid), self.assertRaisesRegex(
                ValueError,
                "INVALID_EVENT_ENVELOPE",
            ):
                reduce(initial, invalid)

        awaiting = apply(initial, StartSession("s1"))
        awaiting = apply(
            awaiting,
            PresentCandidates("s2", ("支付一致性",)),
        )
        awaiting = apply(awaiting, StartTopic("s3", contract()))
        awaiting = apply(awaiting, CommitAgentTurn("s4", agent_turn()))
        malformed = SubmitLearnerTurn(
            "malformed",
            "q1",
            cast(str, None),
            "回答",
        )
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(awaiting, malformed, default_context(awaiting, malformed)),
        )

    def test_every_committed_payload_shape_fails_closed(self):
        initial = initial_dialogue_state("dlg-1", 1)
        session_context = context(TriggerKind.TOPIC_CANDIDATES)
        topic_context = context(TriggerKind.INITIAL_TURN)
        learner_context = context(
            TriggerKind.LEARNER_REPLY,
            parent_turn_id="turn-1",
        )
        malformed_payloads = (
            SessionStarted(cast(TriggerBinding, None)),
            CandidatesPresented(
                cast(tuple[str, ...], ["支付一致性"]),
                session_context.trigger,
            ),
            TopicStarted(
                cast(TopicContract, None),
                topic_context.evidence,
                topic_context.trigger,
            ),
            AgentTurnCommitted(
                cast(AgentTurnResult, None),
                topic_context.trigger,
                topic_context.evidence,
            ),
            LearnerTurnSubmitted(
                "q1",
                "turn-1",
                "回答",
                cast(TriggerBinding, None),
            ),
            TopicPaused(cast(str, None)),
            TopicResumed(
                "topic-1",
                cast(bool, 1),
                learner_context.trigger,
                learner_context.evidence,
            ),
        )
        for payload in malformed_payloads:
            with self.subTest(payload=type(payload).__name__):
                event = CommittedDialogueEvent(
                    event_id=f"malformed-{type(payload).__name__}",
                    sequence=1,
                    from_version=0,
                    to_version=1,
                    command_id="malformed",
                    payload=payload,
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "INVALID_EVENT_ENVELOPE",
                ):
                    reduce(initial, event)

    def test_domain_subclasses_cannot_enter_canonical_state(self):
        class MutableTopicContract(TopicContract):
            pass

        base = contract()
        mutable = MutableTopicContract(
            **{item.name: getattr(base, item.name) for item in fields(TopicContract)}
        )
        mutable.extra = []
        state = initial_dialogue_state("dlg-1", 1)
        state = apply(state, StartSession("c1"))
        state = apply(state, PresentCandidates("c2", ("支付一致性",)))
        topic_context = context(TriggerKind.INITIAL_TURN)
        command = StartTopic("c3", mutable)
        self.assertEqual(
            Rejected("VALIDATION_FAILED"),
            decide(state, command, topic_context),
        )
        forged = CommittedDialogueEvent(
            event_id="event-subclass",
            sequence=state.sequence + 1,
            from_version=state.conversation_version,
            to_version=state.conversation_version + 1,
            command_id=command.command_id,
            payload=TopicStarted(
                mutable,
                topic_context.evidence,
                topic_context.trigger,
            ),
        )
        with self.assertRaisesRegex(ValueError, "INVALID_EVENT_ENVELOPE"):
            reduce(state, forged)

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
                digest("evidence"),
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
            replace(opened, session_work=None),
            replace(
                choosing,
                session_work=opened.session_work,
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
                session_work=None,
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
            with self.subTest(index=index), self.assertRaisesRegex(
                ValueError,
                "STATE_INVARIANT_VIOLATION",
            ):
                _validate(corrupt)

    def test_work_snapshot_invariants_fail_closed(self):
        initial = initial_dialogue_state("dlg-1", 1)
        opened = apply(initial, StartSession("start"))
        session_work = opened.session_work
        assert session_work is not None

        future_sequence = opened.sequence + 1
        future_work = replace(
            session_work,
            work_id=derive_canonical_work_id(
                session_id=opened.session_id,
                trigger_event_id=session_work.trigger_event_id,
                trigger_event_sequence=future_sequence,
                trigger=session_work.trigger,
            ),
            trigger_event_sequence=future_sequence,
        )
        corrupt_states = [
            replace(opened, session_work=future_work),
            replace(
                opened,
                session_work=replace(
                    session_work,
                    attempt=MAX_AUTOMATIC_WORK_ATTEMPTS + 1,
                ),
            ),
            replace(
                opened,
                session_work=replace(session_work, attempt=999),
            ),
        ]

        permanent = work_failure(
            "session-dead",
            WorkFailureCategory.HOST_PERMANENT,
        )
        dead_session = apply(
            opened,
            ReportWorkFailure(
                "dead-session",
                session_work.work_id,
                permanent,
            ),
            DecisionContext(
                opened.registry_generation,
                None,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    session_work.trigger.evidence_digest,
                ),
            ),
        )
        dead_session_work = dead_session.session_work
        assert dead_session_work is not None
        corrupt_states.extend(
            (
                replace(
                    dead_session,
                    session_work=replace(
                        dead_session_work,
                        allowed_recovery_actions=(
                            WorkRecoveryAction.RETRY,
                            WorkRecoveryAction.REGROUND,
                        ),
                    ),
                ),
            )
        )

        choosing = apply(opened, PresentCandidates("candidates", ("支付一致性",)))
        active = apply(choosing, StartTopic("topic", contract()))
        active_topic = active.active_topic
        assert active_topic is not None and active_topic.work is not None
        topic_work = active_topic.work
        dead_topic = apply(
            active,
            ReportWorkFailure(
                "dead-topic",
                topic_work.work_id,
                work_failure(
                    "topic-dead",
                    WorkFailureCategory.HOST_PERMANENT,
                ),
            ),
            DecisionContext(
                active.registry_generation,
                None,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    topic_work.trigger.evidence_digest,
                ),
            ),
        )
        dead_active_topic = dead_topic.active_topic
        assert (
            dead_active_topic is not None
            and dead_active_topic.work is not None
        )
        corrupt_states.extend(
            (
                replace(dead_session, active_topic=active_topic),
                replace(
                    dead_topic,
                    active_topic=replace(
                        dead_active_topic,
                        work=replace(
                            dead_active_topic.work,
                            allowed_recovery_actions=(
                                WorkRecoveryAction.RETRY,
                            ),
                        ),
                    ),
                ),
                replace(
                    dead_topic,
                    active_topic=replace(
                        dead_active_topic,
                        work=replace(
                            dead_active_topic.work,
                            allowed_recovery_actions=(
                                WorkRecoveryAction.REGROUND,
                                WorkRecoveryAction.RETRY,
                            ),
                        ),
                    ),
                ),
            )
        )

        for index, corrupt in enumerate(corrupt_states):
            with self.subTest(index=index), self.assertRaisesRegex(
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
                            parent_turn_id=unresolved.parent_turn_id,
                            input_digest=unresolved.input_digest,
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
