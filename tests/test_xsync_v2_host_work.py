import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.browser_service import (
    AnswerTopicClarificationIntent,
    BrowserCommandRequest,
    BrowserCommandService,
    RequestHelpIntent,
    SelectTopicIntent,
    SetLensIntent,
)
from xsync_v2.coordinator import (
    CoordinatorError,
    DialogueCoordinator,
    DialogueExecutionRequest,
    DialogueSessionConfig,
)
from xsync_v2.dispatch import AfterCommitDispatcher
from xsync_v2.domain import (
    CommitAgentTurn,
    ConversationPhase,
    DecisionContext,
    EvidenceCheck,
    EvidenceHealth,
    Lens,
    PresentCandidates,
    ReportWorkFailure,
    SelectTopic,
    StartTopic,
    TopicSelectionSubmitted,
    TopicStarted,
    TriggerBinding,
    TriggerKind,
    WorkDeadLettered,
    WorkFailed,
    WorkFailure,
    WorkFailureCategory,
    WorkRequeued,
    WorkStatus,
)
from xsync_v2.event_codec import ActorKind, DialogueActor, sha256_digest
from xsync_v2.event_store import _DialogueTransactionLog
from xsync_v2.host_result import (
    DialogueTurnResult,
    TopicCandidatesResult,
    TopicClarificationResult,
    TopicStartedResult,
)
from xsync_v2.host_work import (
    HostResultPublishRequest,
    HostWorkPublishRequest,
    HostWorkService,
    HostWorkServiceError,
    LeaseExhaustionRecordRequest,
    authoritative_work_snapshot,
    host_command_id,
)
from xsync_v2.lease_store import (
    ClaimRequest,
    LeaseExhaustionProof,
    LeaseStore,
    PublishFence,
    ReclaimRequest,
)
from xsync_v2.locking import DomainLockManager, RegistryLockMode
from xsync_v2.observer import ObserverHub, StreamKind
from xsync_v2.secure_fs import SecureDirectory
from xsync_v2.work import RunnableWork, derive_runnable_work
from tests.test_xsync_v2_state_machine import agent_turn, contract

HOST = DialogueActor(ActorKind.HOST, "host.test")
LEARNER = DialogueActor(ActorKind.LEARNER, "learner.test")
RUNTIME = DialogueActor(ActorKind.RUNTIME, "runtime.test")


def digest(label: str) -> str:
    return sha256_digest(label.encode())


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000

    def __call__(self) -> int:
        return self.now


class HostWorkServiceTest(unittest.TestCase):
    def test_browser_selection_becomes_lease_fenced_topic_start(self) -> None:
        candidate_work, candidate_fence = self.bootstrap_candidates()
        candidates = self.service.publish(
            self.candidates_request(candidate_work, candidate_fence)
        )
        browser = BrowserCommandService(
            self.coordinator,
            lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            clock=lambda: self.config.created_at,
        )

        selected = browser.execute(
            BrowserCommandRequest(
                "dlg-a",
                "select-compensation",
                candidates.state.conversation_version,
                SelectTopicIntent("补偿落点"),
            )
        )

        self.assertIs(type(selected.events[-1].payload), TopicSelectionSubmitted)
        self.assertEqual("补偿落点", selected.state.selected_candidate)
        selection_work = self.current_work()
        selection_fence = self.claim(selection_work, "selection-e2e")
        topic_contract = contract()
        initial_trigger = TriggerBinding(
            TriggerKind.INITIAL_TURN,
            "initial-after-selection",
            self.config.runtime_epoch,
            None,
            topic_contract.contract_digest,
            digest("initial-after-selection"),
            self.config.evidence_digest,
        )
        key = "start-selected-topic"
        started = self.service.publish(
            HostWorkPublishRequest(
                key,
                selection_work,
                StartTopic(
                    host_command_id(key),
                    topic_contract,
                    "补偿落点",
                ),
                DecisionContext(
                    1,
                    initial_trigger,
                    EvidenceCheck(
                        EvidenceHealth.CURRENT,
                        self.config.evidence_digest,
                    ),
                ),
                self.config.created_at,
                HOST,
                selection_fence,
            )
        )

        payload = started.events[-1].payload
        self.assertIs(type(payload), TopicStarted)
        assert isinstance(payload, TopicStarted)
        self.assertEqual("补偿落点", payload.selected_candidate)
        assert payload.selection_trigger is not None
        self.assertEqual(
            selection_work.trigger_work_id,
            payload.selection_trigger.work_id,
        )
        self.assertIsNotNone(started.state.active_topic)
        self.assertIsNone(started.state.selected_candidate)

    def test_topic_clarification_round_trip_creates_fresh_selection_work(self) -> None:
        candidate_work, candidate_fence = self.bootstrap_candidates()
        candidates = self.service.publish_result(
            HostResultPublishRequest(
                "clarification-candidates",
                candidate_work,
                TopicCandidatesResult(("支付失败边界", "补偿落点")),
                self.config.created_at,
                HOST,
                candidate_fence,
            )
        )
        browser = BrowserCommandService(
            self.coordinator,
            lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            clock=lambda: self.config.created_at,
        )
        browser.execute(
            BrowserCommandRequest(
                "dlg-a",
                "select-before-clarification",
                candidates.state.conversation_version,
                SelectTopicIntent("支付失败边界"),
            )
        )
        selection_work = self.current_work()
        selection_fence = self.claim(selection_work, "clarification")
        requested = self.service.publish_result(
            HostResultPublishRequest(
                "request-clarification",
                selection_work,
                TopicClarificationResult(
                    "clarification-1",
                    "你更关心业务责任还是技术补偿?",
                ),
                self.config.created_at,
                HOST,
                selection_fence,
            )
        )
        self.assertIs(
            ConversationPhase.CLARIFYING_TOPIC,
            requested.state.phase,
        )
        self.assertIsNone(requested.state.session_work)

        answered = browser.execute(
            BrowserCommandRequest(
                "dlg-a",
                "answer-clarification",
                requested.state.conversation_version,
                AnswerTopicClarificationIntent(
                    "clarification-1",
                    "先厘清业务责任, 再映射技术补偿。",
                ),
            )
        )
        self.assertIs(ConversationPhase.WAITING_HOST, answered.state.phase)
        clarified_work = self.current_work()
        self.assertNotEqual(selection_work.work_id, clarified_work.work_id)
        clarified_fence = self.claim(clarified_work, "clarified-topic")

        started = self.service.publish_result(
            HostResultPublishRequest(
                "start-after-clarification",
                clarified_work,
                TopicStartedResult(contract()),
                self.config.created_at,
                HOST,
                clarified_fence,
            )
        )
        self.assertIsNone(started.state.topic_clarification)
        self.assertIsNotNone(started.state.active_topic)

    def test_lens_change_is_a_fresh_lease_fenced_dialogue_work(self) -> None:
        opening_work, opening_fence = self.start_topic()
        opening = self.service.publish_result(
            HostResultPublishRequest(
                "opening-before-lens-change",
                opening_work,
                DialogueTurnResult(agent_turn()),
                self.config.created_at,
                HOST,
                opening_fence,
            )
        )
        browser = BrowserCommandService(
            self.coordinator,
            lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            clock=lambda: self.config.created_at,
        )
        changed = browser.execute(
            BrowserCommandRequest(
                "dlg-a",
                "switch-to-technical-lens",
                opening.state.conversation_version,
                SetLensIntent(Lens.TECHNICAL),
            )
        )
        assert changed.state.active_topic is not None
        self.assertIs(Lens.TECHNICAL, changed.state.active_topic.lens)
        lens_work = self.current_work()
        self.assertIs(TriggerKind.LENS_CHANGED, lens_work.kind)
        lens_fence = self.claim(lens_work, "lens-change")

        continued = self.service.publish_result(
            HostResultPublishRequest(
                "technical-lens-turn",
                lens_work,
                DialogueTurnResult(agent_turn("q-technical")),
                self.config.created_at,
                HOST,
                lens_fence,
            )
        )
        self.assertIs(ConversationPhase.AWAITING_USER, continued.state.phase)
        assert continued.state.active_topic is not None
        self.assertIs(Lens.TECHNICAL, continued.state.active_topic.lens)

    def test_help_request_is_a_fresh_lease_fenced_dialogue_work(self) -> None:
        opening_work, opening_fence = self.start_topic()
        opening = self.service.publish_result(
            HostResultPublishRequest(
                "opening-before-help",
                opening_work,
                DialogueTurnResult(agent_turn()),
                self.config.created_at,
                HOST,
                opening_fence,
            )
        )
        browser = BrowserCommandService(
            self.coordinator,
            lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            clock=lambda: self.config.created_at,
        )
        requested = browser.execute(
            BrowserCommandRequest(
                "dlg-a",
                "help-for-opening",
                opening.state.conversation_version,
                RequestHelpIntent("q1"),
            )
        )
        self.assertIs(ConversationPhase.WAITING_HOST, requested.state.phase)
        help_work = self.current_work()
        self.assertIs(TriggerKind.HELP, help_work.kind)
        self.assertEqual("q1", help_work.parent_turn_id)
        help_fence = self.claim(help_work, "help")

        continued = self.service.publish_result(
            HostResultPublishRequest(
                "hinted-turn",
                help_work,
                DialogueTurnResult(agent_turn("q-after-help")),
                self.config.created_at,
                HOST,
                help_fence,
            )
        )
        self.assertIs(ConversationPhase.AWAITING_USER, continued.state.phase)
        assert continued.state.active_topic is not None
        self.assertEqual(
            "q-after-help",
            continued.state.active_topic.open_question_id,
        )

    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root_path = Path(self.temporary.name).resolve()
        self.root = SecureDirectory.open(self.root_path)
        self.addCleanup(self.root.close)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.addCleanup(self.dialogues.close)
        self.locks = DomainLockManager(self.root_path / "locks")
        self.addCleanup(self.locks.close)
        self.clock = FakeClock()
        self.runtime_owners = {"owner-1"}
        self.evidence_health = EvidenceHealth.CURRENT
        self.config = DialogueSessionConfig(
            "dlg-a",
            "learner.local",
            "repo.local",
            "2026-08-16T12:00:00+08:00",
            "trigger-epoch",
            digest("evidence"),
        )
        self.coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                self.evidence_health,
                item.evidence_digest,
            ),
        )
        self.leases = LeaseStore(
            self.dialogues,
            self.locks,
            "runtime-1",
            self.clock,
            snapshot_loader=lambda session_id, authority: authoritative_work_snapshot(
                self.coordinator,
                session_id,
                authority,
            ),
            runtime_authority_verifier=lambda check, _authority: (
                check.runtime_epoch == "runtime-1"
                and check.owner_id in self.runtime_owners
            ),
        )
        self.service = HostWorkService(self.coordinator, self.leases)

    def current_work(self) -> RunnableWork:
        with self.locks.semantic_session(
            "dlg-a",
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            snapshot = authoritative_work_snapshot(
                self.coordinator,
                "dlg-a",
                authority,
            )
            work = derive_runnable_work(
                snapshot.dialogue_state,
                snapshot.work_origin,
            )
        assert work is not None
        return work

    def claim(self, work: RunnableWork, suffix: str = "1") -> PublishFence:
        with self.locks.semantic_session(
            work.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            outcome = self.leases.claim(
                ClaimRequest(
                    work.session_id,
                    f"claim-request-{suffix}",
                    f"claim-{suffix}",
                    work.work_id,
                    "owner-1",
                    30,
                    120,
                ),
                authority,
            )
        lease = outcome.lease
        return PublishFence(
            lease.session_id,
            lease.claim_id,
            lease.work_id,
            lease.owner_id,
            lease.runtime_epoch,
            lease.lease_version,
            lease.registry_generation,
        )

    def candidates_request(
        self,
        work: RunnableWork,
        fence: PublishFence,
        *,
        key: str = "publish-candidates-1",
        candidates: tuple[str, ...] = ("支付失败边界", "补偿落点"),
    ) -> HostWorkPublishRequest:
        command_id = host_command_id(key)
        trigger = TriggerBinding(
            work.kind,
            work.trigger_work_id,
            work.trigger_runtime_epoch,
            work.parent_turn_id,
            work.contract_digest,
            work.input_digest,
            work.evidence_digest,
        )
        return HostWorkPublishRequest(
            key,
            work,
            PresentCandidates(command_id, candidates),
            DecisionContext(
                work.registry_generation,
                trigger,
                EvidenceCheck(EvidenceHealth.CURRENT, work.evidence_digest),
            ),
            self.config.created_at,
            HOST,
            fence,
        )

    def exhaust_lease(
        self,
        work: RunnableWork,
        *,
        label: str,
    ) -> tuple[LeaseExhaustionProof, ReclaimRequest]:
        for reclaim_number in (1, 2, 3):
            self.clock.now += 30
            owner = f"owner-{label}-{reclaim_number + 1}"
            self.runtime_owners.add(owner)
            with self.locks.semantic_session(
                work.session_id,
                registry_mode=RegistryLockMode.EXCLUSIVE,
            ) as authority:
                current = self.leases.current_runnable(
                    work.session_id,
                    authority,
                )
                assert current is not None
                reclaim_request = ReclaimRequest(
                    work.session_id,
                    f"reclaim-{label}-{reclaim_number}",
                    f"claim-{label}-{reclaim_number + 1}",
                    work.work_id,
                    owner,
                    current.attempt,
                    30,
                    120,
                )
                outcome = self.leases.reclaim(reclaim_request, authority)
            if reclaim_number < 3:
                self.assertNotIsInstance(outcome, LeaseExhaustionProof)
            else:
                self.assertIs(type(outcome), LeaseExhaustionProof)
                assert isinstance(outcome, LeaseExhaustionProof)
                return outcome, reclaim_request
        raise AssertionError("unreachable")

    def bootstrap_candidates(self) -> tuple[RunnableWork, PublishFence]:
        self.coordinator.resolve(self.config)
        work = self.current_work()
        fence = self.claim(work)
        return work, fence

    def start_topic(self) -> tuple[RunnableWork, PublishFence]:
        work, fence = self.bootstrap_candidates()
        outcome = self.service.publish(self.candidates_request(work, fence))
        selection_trigger = TriggerBinding(
            TriggerKind.TOPIC_SELECTION,
            "selection-trigger",
            self.config.runtime_epoch,
            None,
            None,
            digest("selection-input"),
            self.config.evidence_digest,
        )
        self.coordinator.execute(
            DialogueExecutionRequest(
                "dlg-a",
                outcome.state.conversation_version,
                SelectTopic("select-topic", "支付失败边界"),
                DecisionContext(
                    1,
                    selection_trigger,
                    EvidenceCheck(
                        EvidenceHealth.CURRENT,
                        self.config.evidence_digest,
                    ),
                ),
                self.config.created_at,
                LEARNER,
            )
        )
        selection_work = self.current_work()
        selection_fence = self.claim(selection_work, "selection")
        topic_contract = contract()
        next_trigger = TriggerBinding(
            TriggerKind.INITIAL_TURN,
            "topic-trigger",
            self.config.runtime_epoch,
            None,
            topic_contract.contract_digest,
            digest("topic-input"),
            self.config.evidence_digest,
        )
        start_key = "start-topic"
        self.service.publish(
            HostWorkPublishRequest(
                start_key,
                selection_work,
                StartTopic(
                    host_command_id(start_key),
                    topic_contract,
                    "支付失败边界",
                ),
                DecisionContext(
                    1,
                    next_trigger,
                    EvidenceCheck(
                        EvidenceHealth.CURRENT,
                        self.config.evidence_digest,
                    ),
                ),
                self.config.created_at,
                HOST,
                selection_fence,
            )
        )
        topic_work = self.current_work()
        return topic_work, self.claim(topic_work, "2")

    def test_publish_is_lease_fenced_and_exact_replay_is_receipt_first(self) -> None:
        work, fence = self.bootstrap_candidates()
        request = self.candidates_request(work, fence)

        committed = self.service.publish(request)
        self.clock.now = 2_000
        original_snapshot_loader = self.leases._snapshot_loader
        with (
            mock.patch.object(
                self.coordinator,
                "_verify_evidence",
                wraps=self.coordinator._verify_evidence,
            ) as evidence_check,
            mock.patch.object(
                self.leases,
                "_marker_publication_guard",
                wraps=self.leases._marker_publication_guard,
            ) as lease_guard,
            mock.patch.object(
                self.leases,
                "_snapshot_loader",
                wraps=original_snapshot_loader,
            ) as snapshot_loader,
        ):
            replayed = self.service.publish(
                replace(
                    request,
                    fence=replace(
                        request.fence,
                        lease_version=request.fence.lease_version + 10,
                    ),
                )
            )
            evidence_check.assert_not_called()
            lease_guard.assert_not_called()
            snapshot_loader.assert_not_called()

        self.assertFalse(committed.replayed)
        self.assertTrue(replayed.replayed)
        self.assertEqual(committed.receipt, replayed.receipt)
        self.assertEqual(committed.events, replayed.events)

        changed = replace(
            request,
            command=PresentCandidates(
                request.command.command_id,
                ("不同结果",),
            ),
        )
        with self.assertRaises(HostWorkServiceError) as raised:
            self.service.publish(changed)
        self.assertEqual("IDEMPOTENCY_CONFLICT", raised.exception.code)

    def test_strict_result_publish_replays_before_evidence_or_lease_checks(
        self,
    ) -> None:
        work, fence = self.bootstrap_candidates()
        request = HostResultPublishRequest(
            "strict-candidates-1",
            work,
            TopicCandidatesResult(("支付失败边界", "补偿落点")),
            self.config.created_at,
            HOST,
            fence,
        )
        committed = self.service.publish_result(request)
        self.coordinator.resolve(
            replace(
                self.config,
                session_id="dlg-b",
                created_at="2026-08-16T12:01:00+08:00",
                runtime_epoch="trigger-epoch-b",
            )
        )
        self.evidence_health = EvidenceHealth.STALE
        self.clock.now = 2_000

        with (
            mock.patch.object(
                self.coordinator,
                "_verify_evidence",
                wraps=self.coordinator._verify_evidence,
            ) as evidence_check,
            mock.patch.object(
                self.leases,
                "_marker_publication_guard",
                wraps=self.leases._marker_publication_guard,
            ) as lease_guard,
        ):
            replayed = self.service.publish_result(
                replace(
                    request,
                    fence=replace(
                        fence,
                        lease_version=fence.lease_version + 9,
                    ),
                )
            )
        evidence_check.assert_not_called()
        lease_guard.assert_not_called()
        self.assertTrue(replayed.replayed)
        self.assertEqual(committed.receipt, replayed.receipt)

        with self.assertRaises(HostWorkServiceError) as raised:
            self.service.publish_result(
                replace(
                    request,
                    result=TopicCandidatesResult(("不同结果",)),
                )
            )
        self.assertEqual("IDEMPOTENCY_CONFLICT", raised.exception.code)

    def test_strict_topic_and_turn_results_derive_trusted_next_context(self) -> None:
        candidate_work, candidate_fence = self.bootstrap_candidates()
        candidates = self.service.publish_result(
            HostResultPublishRequest(
                "strict-candidates-2",
                candidate_work,
                TopicCandidatesResult(("支付失败边界", "补偿落点")),
                self.config.created_at,
                HOST,
                candidate_fence,
            )
        )
        selection_trigger = TriggerBinding(
            TriggerKind.TOPIC_SELECTION,
            "selection-trigger-strict",
            self.config.runtime_epoch,
            None,
            None,
            digest("selection-input-strict"),
            self.config.evidence_digest,
        )
        self.coordinator.execute(
            DialogueExecutionRequest(
                "dlg-a",
                candidates.state.conversation_version,
                SelectTopic("select-strict-topic", "支付失败边界"),
                DecisionContext(
                    1,
                    selection_trigger,
                    EvidenceCheck(
                        EvidenceHealth.CURRENT,
                        self.config.evidence_digest,
                    ),
                ),
                self.config.created_at,
                LEARNER,
            )
        )
        selection_work = self.current_work()
        selection_fence = self.claim(selection_work, "strict-selection")
        started = self.service.publish_result(
            HostResultPublishRequest(
                "strict-topic-start",
                selection_work,
                TopicStartedResult(contract()),
                self.config.created_at,
                HOST,
                selection_fence,
            )
        )

        started_payload = started.events[-1].payload
        self.assertIs(type(started_payload), TopicStarted)
        assert isinstance(started_payload, TopicStarted)
        self.assertEqual("支付失败边界", started_payload.selected_candidate)
        self.assertRegex(started_payload.initial_trigger.work_id, r"^work\.initial\.")
        self.assertEqual(
            self.config.runtime_epoch,
            started_payload.initial_trigger.runtime_epoch,
        )

        turn_work = self.current_work()
        turn_fence = self.claim(turn_work, "strict-turn")
        turn = self.service.publish_result(
            HostResultPublishRequest(
                "strict-turn-publish",
                turn_work,
                DialogueTurnResult(agent_turn()),
                self.config.created_at,
                HOST,
                turn_fence,
            )
        )
        self.assertEqual(ConversationPhase.AWAITING_USER, turn.state.phase)
        self.assertEqual(1, len(turn.events))

    def test_receipt_replays_after_session_deactivation_and_lease_expiry(self) -> None:
        work, fence = self.bootstrap_candidates()
        request = self.candidates_request(work, fence)
        committed = self.service.publish(request)
        other = replace(
            self.config,
            session_id="dlg-b",
            created_at="2026-08-16T12:01:00+08:00",
            runtime_epoch="trigger-epoch-b",
        )
        self.coordinator.resolve(other)
        self.clock.now = 2_000

        replayed = self.service.publish(request)

        self.assertTrue(replayed.replayed)
        self.assertEqual(committed.receipt, replayed.receipt)
        changed = replace(
            request,
            command=PresentCandidates(
                request.command.command_id,
                ("conflicting",),
            ),
        )
        with (
            mock.patch.object(
                self.coordinator,
                "_verify_evidence",
                wraps=self.coordinator._verify_evidence,
            ) as evidence_check,
            mock.patch.object(
                self.leases,
                "_marker_publication_guard",
                wraps=self.leases._marker_publication_guard,
            ) as lease_guard,
            self.assertRaises(HostWorkServiceError) as raised,
        ):
            self.service.publish(changed)
        evidence_check.assert_not_called()
        lease_guard.assert_not_called()
        self.assertEqual("IDEMPOTENCY_CONFLICT", raised.exception.code)

    def test_marker_guard_rechecks_lease_after_event_staging(self) -> None:
        work, fence = self.bootstrap_candidates()
        request = self.candidates_request(work, fence)
        original = SecureDirectory.write_immutable_guarded

        def expire_before_guard(directory, component, data, guard):
            self.clock.now = 1_031
            return original(directory, component, data, guard)

        with (
            mock.patch.object(
                SecureDirectory,
                "write_immutable_guarded",
                autospec=True,
                side_effect=expire_before_guard,
            ),
            self.assertRaises(HostWorkServiceError) as raised,
        ):
            self.service.publish(request)

        self.assertEqual("LEASE_EXPIRED", raised.exception.code)
        resolution = self.coordinator.resolve(self.config)
        self.assertEqual(1, resolution.dialogue_state.sequence)

    def test_marker_guard_rechecks_exact_evidence_after_staging(self) -> None:
        work, fence = self.bootstrap_candidates()
        request = self.candidates_request(work, fence)
        original = SecureDirectory.write_immutable_guarded

        def stale_before_guard(directory, component, data, guard):
            self.evidence_health = EvidenceHealth.STALE
            return original(directory, component, data, guard)

        with (
            mock.patch.object(
                SecureDirectory,
                "write_immutable_guarded",
                autospec=True,
                side_effect=stale_before_guard,
            ),
            self.assertRaises(HostWorkServiceError) as raised,
        ):
            self.service.publish(request)

        self.assertEqual("EVIDENCE_CHANGED", raised.exception.code)
        self.evidence_health = EvidenceHealth.CURRENT
        resolution = self.coordinator.resolve(self.config)
        self.assertEqual(1, resolution.dialogue_state.sequence)

    def test_failure_retry_trigger_is_runtime_derived(self) -> None:
        work, fence = self.start_topic()
        key = "publish-failure-1"
        failure = WorkFailure(
            "failure-timeout",
            WorkFailureCategory.HOST_TRANSIENT,
            "HOST_TIMEOUT",
            digest("failure-proof"),
        )
        request = HostWorkPublishRequest(
            key,
            work,
            ReportWorkFailure(host_command_id(key), work.work_id, failure),
            DecisionContext(
                work.registry_generation,
                None,
                EvidenceCheck(EvidenceHealth.CURRENT, work.evidence_digest),
            ),
            self.config.created_at,
            HOST,
            fence,
        )

        outcome = self.service.publish(request)

        requeued = outcome.events[1].payload
        self.assertIs(type(requeued), WorkRequeued)
        assert isinstance(requeued, WorkRequeued)
        self.assertNotEqual(work.trigger_work_id, requeued.next_trigger.work_id)
        self.assertRegex(requeued.next_trigger.work_id, r"^work\.retry\.")

        hostile = replace(
            request,
            idempotency_key="publish-failure-hostile",
            command=replace(
                request.command,
                command_id=host_command_id("publish-failure-hostile"),
            ),
            context=replace(
                request.context,
                trigger=replace(
                    requeued.next_trigger,
                    work_id="host-selected-retry",
                ),
            ),
        )
        with self.assertRaises(HostWorkServiceError) as raised:
            self.service.publish(hostile)
        self.assertEqual("INVALID_HOST_WORK_REQUEST", raised.exception.code)

        late_key = "publish-failure-late"
        late = replace(
            request,
            idempotency_key=late_key,
            command=replace(
                request.command,
                command_id=host_command_id(late_key),
            ),
        )
        with self.assertRaises(HostWorkServiceError) as raised:
            self.service.publish(late)
        self.assertEqual("WORK_SUPERSEDED", raised.exception.code)
        self.assertEqual(
            outcome.state.sequence,
            self.coordinator.resolve(self.config).dialogue_state.sequence,
        )

    def test_lease_exhaustion_is_exact_once_bounded_and_dead_letters_attempt_three(
        self,
    ) -> None:
        self.coordinator.resolve(self.config)
        work = self.current_work()
        self.claim(work, "exhaust-1")
        proof, final_reclaim = self.exhaust_lease(work, label="attempt-1")
        request = LeaseExhaustionRecordRequest(
            proof,
            self.config.created_at,
            RUNTIME,
        )

        first = self.service.record_lease_exhaustion(request)
        with mock.patch.object(
            self.leases,
            "_exhaustion_marker_guard",
            wraps=self.leases._exhaustion_marker_guard,
        ) as marker_guard:
            replay = self.service.record_lease_exhaustion(request)
        marker_guard.assert_not_called()
        self.assertFalse(first.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.events, replay.events)
        self.assertEqual(
            (WorkFailed, WorkRequeued),
            tuple(type(item.payload) for item in first.events),
        )

        restarted_coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                self.evidence_health,
                item.evidence_digest,
            ),
        )
        restarted_leases = LeaseStore(
            self.dialogues,
            self.locks,
            "runtime-1",
            self.clock,
            snapshot_loader=lambda session_id, authority: authoritative_work_snapshot(
                restarted_coordinator,
                session_id,
                authority,
            ),
            runtime_authority_verifier=lambda check, _authority: (
                check.runtime_epoch == "runtime-1"
                and check.owner_id in self.runtime_owners
            ),
        )
        restarted_service = HostWorkService(
            restarted_coordinator,
            restarted_leases,
        )
        with self.locks.semantic_session(
            work.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            rebuilt = restarted_leases.reclaim(final_reclaim, authority)
            conflicting = restarted_leases.reclaim(
                replace(
                    final_reclaim,
                    claim_id="claim-conflicting-body",
                    owner_id="owner-conflicting-body",
                ),
                authority,
            )
        self.assertEqual(proof, rebuilt)
        assert isinstance(rebuilt, LeaseExhaustionProof)
        assert isinstance(conflicting, LeaseExhaustionProof)
        self.assertEqual(
            rebuilt.reclaim_request_id,
            conflicting.reclaim_request_id,
        )
        self.assertNotEqual(
            rebuilt.reclaim_request_digest,
            conflicting.reclaim_request_digest,
        )
        restarted_replay = restarted_service.record_lease_exhaustion(
            LeaseExhaustionRecordRequest(
                rebuilt,
                self.config.created_at,
                RUNTIME,
            )
        )
        self.assertTrue(restarted_replay.replayed)
        self.assertEqual(first.events, restarted_replay.events)
        with self.assertRaises(HostWorkServiceError) as raised:
            restarted_service.record_lease_exhaustion(
                LeaseExhaustionRecordRequest(
                    conflicting,
                    self.config.created_at,
                    RUNTIME,
                )
            )
        self.assertEqual("IDEMPOTENCY_CONFLICT", raised.exception.code)

        work_two = self.current_work()
        self.assertNotEqual(work.work_id, work_two.work_id)
        self.claim(work_two, "exhaust-2")
        proof_two, _ = self.exhaust_lease(work_two, label="attempt-2")
        second = self.service.record_lease_exhaustion(
            LeaseExhaustionRecordRequest(
                proof_two,
                self.config.created_at,
                RUNTIME,
            )
        )
        self.assertEqual(
            (WorkFailed, WorkRequeued),
            tuple(type(item.payload) for item in second.events),
        )

        work_three = self.current_work()
        self.claim(work_three, "exhaust-3")
        proof_three, _ = self.exhaust_lease(work_three, label="attempt-3")
        terminal = self.service.record_lease_exhaustion(
            LeaseExhaustionRecordRequest(
                proof_three,
                self.config.created_at,
                RUNTIME,
            )
        )
        self.assertEqual(
            (WorkFailed, WorkDeadLettered),
            tuple(type(item.payload) for item in terminal.events),
        )
        self.assertEqual(ConversationPhase.RECOVERABLE_ERROR, terminal.state.phase)
        dead = terminal.state.session_work
        assert dead is not None
        self.assertEqual(3, dead.attempt)
        self.assertIs(WorkStatus.DEAD_LETTER, dead.status)

        for item in (work, work_two, work_three):
            component = self.leases._work_component(item.work_id)
            path = (
                self.root_path
                / "dialogues"
                / next(
                    entry.name
                    for entry in (self.root_path / "dialogues").iterdir()
                    if entry.is_dir() and entry.name.startswith("session-")
                )
                / "runtime"
                / "leases"
                / component
                / "transactions"
            )
            self.assertEqual(3, len(tuple(path.iterdir())))

    def test_forged_exhaustion_proof_writes_no_canonical_events(self) -> None:
        self.coordinator.resolve(self.config)
        work = self.current_work()
        self.claim(work, "forged")
        proof, _ = self.exhaust_lease(work, label="forged")
        forged = replace(proof, history_digest=digest("forged-history"))

        with self.assertRaises(HostWorkServiceError) as raised:
            self.service.record_lease_exhaustion(
                LeaseExhaustionRecordRequest(
                    forged,
                    self.config.created_at,
                    RUNTIME,
                )
            )

        self.assertEqual("LEASE_EXHAUSTION_PROOF_INVALID", raised.exception.code)
        self.assertEqual(
            1,
            self.coordinator.resolve(self.config).dialogue_state.sequence,
        )

    def test_exhaustion_marker_rechecks_requesting_runtime_owner(self) -> None:
        self.coordinator.resolve(self.config)
        work = self.current_work()
        self.claim(work, "owner-fence")
        proof, _ = self.exhaust_lease(work, label="owner-fence")
        request = LeaseExhaustionRecordRequest(
            proof,
            self.config.created_at,
            RUNTIME,
        )
        original = SecureDirectory.write_immutable_guarded

        def revoke_before_guard(directory, component, data, guard):
            self.runtime_owners.remove(proof.requesting_owner_id)
            return original(directory, component, data, guard)

        with (
            mock.patch.object(
                SecureDirectory,
                "write_immutable_guarded",
                autospec=True,
                side_effect=revoke_before_guard,
            ),
            self.assertRaises(HostWorkServiceError) as raised,
        ):
            self.service.record_lease_exhaustion(request)

        self.assertEqual("RUNTIME_AUTHORITY_REJECTED", raised.exception.code)
        self.assertEqual(
            1,
            self.coordinator.resolve(self.config).dialogue_state.sequence,
        )
        self.runtime_owners.add(proof.requesting_owner_id)
        committed = self.service.record_lease_exhaustion(request)
        self.assertEqual(
            (WorkFailed, WorkRequeued),
            tuple(type(item.payload) for item in committed.events),
        )

    def test_agent_turn_publish_and_replay_create_exactly_one_question(self) -> None:
        work, fence = self.start_topic()
        key = "publish-agent-turn-1"
        trigger = TriggerBinding(
            work.kind,
            work.trigger_work_id,
            work.trigger_runtime_epoch,
            work.parent_turn_id,
            work.contract_digest,
            work.input_digest,
            work.evidence_digest,
        )
        request = HostWorkPublishRequest(
            key,
            work,
            CommitAgentTurn(host_command_id(key), agent_turn()),
            DecisionContext(
                work.registry_generation,
                trigger,
                EvidenceCheck(EvidenceHealth.CURRENT, work.evidence_digest),
            ),
            self.config.created_at,
            HOST,
            fence,
        )

        committed = self.service.publish(request)
        replayed = self.service.publish(request)

        self.assertFalse(committed.replayed)
        self.assertTrue(replayed.replayed)
        self.assertEqual(ConversationPhase.AWAITING_USER, committed.state.phase)
        topic = committed.state.active_topic
        assert topic is not None
        self.assertIsNone(topic.work)
        self.assertEqual("q1", topic.open_question_id)
        self.assertEqual(committed.state.sequence, replayed.state.sequence)
        self.assertEqual(committed.receipt, replayed.receipt)

    def test_post_marker_failure_is_recoverable_by_exact_receipt(self) -> None:
        work, fence = self.bootstrap_candidates()
        request = self.candidates_request(work, fence)
        original = _DialogueTransactionLog.commit
        failed_once = False

        def fail_after_marker(
            log,
            commit_request,
            authority,
            *,
            marker_publication_guard=None,
        ):
            nonlocal failed_once
            outcome = original(
                log,
                commit_request,
                authority,
                marker_publication_guard=marker_publication_guard,
            )
            if not failed_once:
                failed_once = True
                raise RuntimeError("lost Host response")
            return outcome

        with (
            mock.patch.object(
                _DialogueTransactionLog,
                "commit",
                autospec=True,
                side_effect=fail_after_marker,
            ),
            self.assertRaisesRegex(RuntimeError, "lost Host response"),
        ):
            self.service.publish(request)

        replayed = self.service.publish(request)
        self.assertTrue(replayed.replayed)
        self.assertEqual(2, replayed.state.sequence)

    def test_observer_delivery_occurs_only_after_host_locks_release(self) -> None:
        class LockCheckingObserver:
            name = "lock-checking"
            accepted_streams = frozenset({StreamKind.DIALOGUE})

            def __init__(self, locks):
                self.locks = locks
                self.sequences = []

            def on_batch(self, batch):
                self.locks.assert_none_held()
                self.sequences.extend(event.sequence for event in batch.events)

        observer = LockCheckingObserver(self.locks)
        hub = ObserverHub(self.locks)
        hub.register_fixed(observer)
        hub.freeze()
        self.coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                self.evidence_health,
                item.evidence_digest,
            ),
            after_commit_dispatcher=AfterCommitDispatcher(hub),
        )
        self.leases._snapshot_loader = lambda session_id, authority: (
            authoritative_work_snapshot(
                self.coordinator,
                session_id,
                authority,
            )
        )
        self.service = HostWorkService(self.coordinator, self.leases)

        work, fence = self.bootstrap_candidates()
        observer.sequences.clear()
        self.service.publish(self.candidates_request(work, fence))

        self.assertEqual([2], observer.sequences)
        self.locks.assert_none_held()

    def test_public_coordinator_cannot_bypass_host_service(self) -> None:
        resolution = self.coordinator.resolve(self.config)
        current = resolution.dialogue_state.session_work
        assert current is not None
        context = DecisionContext(
            1,
            current.trigger,
            EvidenceCheck(EvidenceHealth.CURRENT, self.config.evidence_digest),
        )
        commands = (
            PresentCandidates("candidates", ("topic",)),
            StartTopic("start-topic", contract(), "topic"),
            CommitAgentTurn("turn", agent_turn()),
            ReportWorkFailure(
                "failure",
                current.work_id,
                WorkFailure(
                    "failure-id",
                    WorkFailureCategory.HOST_PERMANENT,
                    "HOST_FAILED",
                    digest("proof"),
                ),
            ),
        )
        for command in commands:
            with (
                self.subTest(command=type(command).__name__),
                self.assertRaises(CoordinatorError) as raised,
            ):
                self.coordinator.execute(
                    DialogueExecutionRequest(
                        "dlg-a",
                        1,
                        command,
                        context,
                        self.config.created_at,
                        HOST,
                    )
                )
            self.assertEqual("HOST_WORK_SERVICE_REQUIRED", raised.exception.code)


if __name__ == "__main__":
    unittest.main()
