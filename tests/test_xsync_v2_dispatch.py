from dataclasses import replace
import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401
from tests.test_xsync_v2_coordinator import config
from tests.test_xsync_v2_state_machine import agent_turn, contract
from xsync_v2.coordinator import CoordinatorError, DialogueCoordinator
from xsync_v2.dispatch import (
    AfterCommitDispatcher,
    AfterCommitError,
    DialogueCommittedFact,
    dialogue_batch,
    registry_batch,
)
from xsync_v2.domain import (
    AgentTurnCommitted,
    CandidatesPresented,
    CommittedDialogueEvent,
    EvidenceCheck,
    EvidenceHealth,
    HelpRequested,
    LearnerTurnSubmitted,
    Lens,
    LensChanged,
    PauseCause,
    SessionDeactivationPrepared,
    SessionStarted,
    TopicPaused,
    TopicResumed,
    TopicSelectionSubmitted,
    TopicSwitchRequested,
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
)
from xsync_v2.event_store import (
    _DialogueTransactionLog,
    session_directory_component,
)
from xsync_v2.locking import DomainLockManager
from xsync_v2.observer import DispatchReport, ObserverHub, StreamKind
from xsync_v2.observers.public_stream import PublicStreamObserver
from xsync_v2.observers.work_wake import WorkWakeHint, WorkWakeObserver
from xsync_v2.registry import (
    Activated,
    ActivateTarget,
    ActivationKind,
    CommittedRegistryEvent,
    DialogueCreated,
)
from xsync_v2.registry_store import _RegistryTransactionLog
from xsync_v2.secure_fs import SecureDirectory


def trigger(kind=TriggerKind.TOPIC_CANDIDATES):
    return TriggerBinding(
        kind,
        "work-1",
        "runtime-epoch-secret",
        None,
        None,
        "sha256:" + "1" * 64,
        "sha256:" + "2" * 64,
    )


def dialogue_event(event_id, sequence, payload):
    return CommittedDialogueEvent(
        event_id,
        sequence,
        sequence - 1,
        sequence,
        "command-1",
        payload,
    )


def work_failure():
    return WorkFailure(
        "failure-id-secret",
        WorkFailureCategory.HOST_TRANSIENT,
        "HOST_TIMEOUT_SECRET",
        "sha256:" + "3" * 64,
    )


class NoLocks:
    def assert_none_held(self):
        return None


class RecordingObserver:
    name = "recording"
    accepted_streams = frozenset({StreamKind.DIALOGUE, StreamKind.REGISTRY})

    def __init__(self):
        self.batches = []
        self.fail = False

    def on_batch(self, batch):
        if self.fail:
            raise RuntimeError("observer unavailable")
        self.batches.append(batch)


class DispatchTest(unittest.TestCase):
    def hub_and_observer(self):
        observer = RecordingObserver()
        hub = ObserverHub(NoLocks())
        hub.register_fixed(observer)
        hub.freeze()
        return hub, observer

    def test_dialogue_mapper_exposes_only_stable_projection_fields(self):
        topic_contract = contract()
        evidence = EvidenceCheck(EvidenceHealth.CURRENT, "sha256:" + "2" * 64)
        events = (
            dialogue_event("event-1", 1, SessionStarted(trigger())),
            dialogue_event(
                "event-2",
                2,
                CandidatesPresented(("退款窗口", "补偿边界"), trigger()),
            ),
            dialogue_event(
                "event-3",
                3,
                TopicSelectionSubmitted(
                    "补偿边界",
                    trigger(TriggerKind.TOPIC_SELECTION),
                ),
            ),
            dialogue_event(
                "event-4",
                4,
                TopicStarted(
                    topic_contract,
                    evidence,
                    trigger(TriggerKind.INITIAL_TURN),
                ),
            ),
            dialogue_event(
                "event-5",
                5,
                AgentTurnCommitted(
                    agent_turn(),
                    trigger(TriggerKind.INITIAL_TURN),
                    evidence,
                ),
            ),
        )

        mapped = dialogue_batch("session-1", events)

        self.assertEqual(StreamKind.DIALOGUE, mapped.stream_kind)
        self.assertEqual(
            (1, 2, 3, 4, 5),
            tuple(item.sequence for item in mapped.events),
        )
        self.assertEqual(
            (
                "session_started",
                "topic_candidates_presented",
                "topic_selection_submitted",
                "topic_started",
                "agent_turn_committed",
            ),
            tuple(item.payload.tag for item in mapped.events),
        )
        candidates = dict(mapped.events[1].payload.fields)["candidates"]
        self.assertEqual(["退款窗口", "补偿边界"], json.loads(candidates))
        self.assertEqual(
            "补偿边界",
            dict(mapped.events[2].payload.fields)["candidate"],
        )
        rendered = repr(mapped)
        self.assertNotIn("runtime-epoch-secret", rendered)
        self.assertNotIn(evidence.evidence_digest, rendered)
        self.assertNotIn(topic_contract.contract_digest, rendered)

    def test_registry_mapper_uses_registry_stream_without_session_alias(self):
        target = ActivateTarget("session-1", "sha256:" + "a" * 64)
        events = (
            CommittedRegistryEvent(
                "registry-event-1",
                "sha256:" + "b" * 64,
                1,
                1,
                "registry-command",
                "sha256:" + "c" * 64,
                DialogueCreated(target),
            ),
            CommittedRegistryEvent(
                "registry-event-2",
                "sha256:" + "d" * 64,
                2,
                1,
                "registry-command",
                "sha256:" + "c" * 64,
                Activated(target, 1, ActivationKind.INITIAL, None),
            ),
        )

        mapped = registry_batch("registry-1", events)

        self.assertEqual(StreamKind.REGISTRY, mapped.stream_kind)
        self.assertEqual("registry-1", mapped.stream_id)
        self.assertEqual((1, 2), tuple(item.sequence for item in mapped.events))
        self.assertEqual(
            ("dialogue_created", "dialogue_activated"),
            tuple(item.payload.tag for item in mapped.events),
        )

    def test_out_of_order_commits_are_buffered_then_published_in_sequence(self):
        hub, observer = self.hub_and_observer()
        dispatcher = AfterCommitDispatcher(hub)
        second = dialogue_batch(
            "session-1",
            (dialogue_event("event-2", 2, SessionStarted(trigger())),),
        )
        first = dialogue_batch(
            "session-1",
            (dialogue_event("event-1", 1, SessionStarted(trigger())),),
        )

        buffered = dispatcher.publish_all((second,))
        completed = dispatcher.publish_all((first,))

        self.assertEqual(1, buffered.buffered_events)
        self.assertEqual(0, completed.buffered_events)
        self.assertEqual(
            (1, 2),
            tuple(
                event.sequence
                for batch in observer.batches
                for event in batch.events
            ),
        )

    def test_streams_order_independently_and_exact_replay_reaches_hub(self):
        hub, observer = self.hub_and_observer()
        dispatcher = AfterCommitDispatcher(hub)
        dialogue = dialogue_batch(
            "session-1",
            (dialogue_event("event-1", 1, SessionStarted(trigger())),),
        )
        target = ActivateTarget("session-1", "sha256:" + "a" * 64)
        registry = registry_batch(
            "registry-1",
            (
                CommittedRegistryEvent(
                    "registry-event-1",
                    "sha256:" + "b" * 64,
                    1,
                    1,
                    "registry-command",
                    "sha256:" + "c" * 64,
                    DialogueCreated(target),
                ),
            ),
        )

        first = dispatcher.publish_all((dialogue, registry))
        replay = dispatcher.publish_all((dialogue, registry))

        self.assertEqual(2, len(first.hub_reports))
        self.assertEqual(2, len(replay.hub_reports))
        self.assertTrue(
            all(type(item) is DispatchReport for item in replay.hub_reports)
        )
        self.assertEqual(2, len(observer.batches))

    def test_observer_failure_is_reported_without_raising_or_losing_order(self):
        hub, observer = self.hub_and_observer()
        dispatcher = AfterCommitDispatcher(hub)
        first = dialogue_batch(
            "session-1",
            (dialogue_event("event-1", 1, SessionStarted(trigger())),),
        )
        observer.fail = True

        failed = dispatcher.publish_all((first,))

        self.assertEqual(("recording",), failed.hub_reports[0].failed)
        observer.fail = False
        retried = dispatcher.publish_all((first,))
        self.assertEqual((), retried.hub_reports[0].failed)
        self.assertEqual(
            (1,),
            tuple(item.sequence for item in observer.batches[0].events),
        )

    def test_remaining_dialogue_payloads_have_closed_stable_mappings(self):
        evidence = EvidenceCheck(EvidenceHealth.CURRENT, "sha256:" + "2" * 64)
        events = (
            dialogue_event(
                "event-1",
                1,
                LearnerTurnSubmitted(
                    "question-1",
                    "turn-1",
                    "先看事务边界",
                    trigger(TriggerKind.LEARNER_REPLY),
                ),
            ),
            dialogue_event(
                "event-2",
                2,
                TopicPaused("topic-1", PauseCause.USER),
            ),
            dialogue_event(
                "event-3",
                3,
                TopicSwitchRequested("topic-1", trigger()),
            ),
            dialogue_event(
                "event-4",
                4,
                SessionDeactivationPrepared("handoff-1", 2, trigger()),
            ),
            dialogue_event(
                "event-5",
                5,
                TopicResumed(
                    "topic-1",
                    True,
                    trigger(TriggerKind.REGROUND),
                    evidence,
                ),
            ),
            dialogue_event(
                "event-6",
                6,
                LensChanged(
                    "topic-1",
                    Lens.TECHNICAL,
                    trigger(TriggerKind.LENS_CHANGED),
                ),
            ),
            dialogue_event(
                "event-7",
                7,
                HelpRequested(
                    "topic-1",
                    "question-1",
                    trigger(TriggerKind.HELP),
                ),
            ),
        )

        mapped = dialogue_batch("session-1", events)

        self.assertEqual(
            (
                "learner_turn_submitted",
                "topic_paused",
                "topic_switch_requested",
                "session_deactivation_prepared",
                "topic_resumed",
                "lens_changed",
                "help_requested",
            ),
            tuple(item.payload.tag for item in mapped.events),
        )
        self.assertEqual(
            "true",
            dict(mapped.events[-3].payload.fields)["requires_reground"],
        )
        self.assertEqual(
            "technical",
            dict(mapped.events[-2].payload.fields)["lens"],
        )
        self.assertEqual(
            "question-1",
            dict(mapped.events[-1].payload.fields)["question_id"],
        )

    def test_failure_batch_maps_contiguously_without_internal_data(self):
        failed_trigger = trigger()
        failure = work_failure()
        next_trigger = replace(
            failed_trigger,
            work_id="retry-work-secret",
            runtime_epoch="retry-epoch-secret",
        )
        events = (
            dialogue_event(
                "event-failed",
                1,
                WorkFailed("failed-work-secret", failed_trigger, 1, failure),
            ),
            dialogue_event(
                "event-requeued",
                2,
                WorkRequeued(
                    "failed-work-secret",
                    failed_trigger,
                    1,
                    failure,
                    next_trigger,
                    2,
                ),
            ),
        )

        mapped = dialogue_batch("session-1", events)

        self.assertEqual((1, 2), tuple(item.sequence for item in mapped.events))
        self.assertEqual(
            ("state_changed", "work_requeued"),
            tuple(item.payload.tag for item in mapped.events),
        )
        self.assertTrue(all(item.payload.fields == () for item in mapped.events))
        rendered = repr(mapped)
        for secret in (
            "failure-id-secret",
            "HOST_TIMEOUT_SECRET",
            failure.proof_digest,
            "failed-work-secret",
            "runtime-epoch-secret",
            "retry-work-secret",
            "retry-epoch-secret",
            failed_trigger.input_digest,
            failed_trigger.evidence_digest,
        ):
            self.assertNotIn(secret, rendered)

    def test_dead_letter_and_recovery_views_are_safe_and_typed(self):
        failed_trigger = trigger()
        failure = work_failure()
        recovery_trigger = replace(
            failed_trigger,
            work_id="recovery-work-secret",
        )
        evidence = EvidenceCheck(
            EvidenceHealth.CURRENT,
            "sha256:" + "4" * 64,
            "recheck-fingerprint-secret",
        )
        events = (
            dialogue_event(
                "event-failed",
                1,
                WorkFailed("failed-work-secret", failed_trigger, 3, failure),
            ),
            dialogue_event(
                "event-dead-lettered",
                2,
                WorkDeadLettered(
                    "failed-work-secret",
                    failed_trigger,
                    3,
                    failure,
                    (WorkRecoveryAction.RETRY,),
                ),
            ),
            dialogue_event(
                "event-recovery-requested",
                3,
                WorkRecoveryRequested(
                    "failed-work-secret",
                    WorkRecoveryAction.RETRY,
                    recovery_trigger,
                    evidence,
                ),
            ),
        )

        mapped = dialogue_batch("session-1", events)

        self.assertEqual((1, 2, 3), tuple(item.sequence for item in mapped.events))
        self.assertEqual(
            ("state_changed", "state_changed", "work_recovery_requested"),
            tuple(item.payload.tag for item in mapped.events),
        )
        self.assertTrue(all(item.payload.fields == () for item in mapped.events))
        rendered = repr(mapped)
        for secret in (
            "failure-id-secret",
            "HOST_TIMEOUT_SECRET",
            failure.proof_digest,
            "failed-work-secret",
            "recovery-work-secret",
            evidence.evidence_digest,
            "recheck-fingerprint-secret",
        ):
            self.assertNotIn(secret, rendered)

    def test_cold_replay_failure_batch_advances_public_and_wake_cursors(self):
        public = PublicStreamObserver()

        class WakePort:
            def __init__(self):
                self.hints = []

            def notify(self, hint):
                self.hints.append(hint)

        port = WakePort()
        hub = ObserverHub(NoLocks())
        hub.register_fixed(public)
        hub.register_fixed(WorkWakeObserver(port))
        hub.freeze()
        subscription = public.subscribe("session-1", after_sequence=0)
        dispatcher = AfterCommitDispatcher(hub)
        failed_trigger = trigger()
        failure = work_failure()
        next_trigger = replace(failed_trigger, work_id="retry-work-secret")
        events = (
            dialogue_event("event-1", 1, SessionStarted(failed_trigger)),
            dialogue_event(
                "event-2",
                2,
                WorkFailed("failed-work-secret", failed_trigger, 1, failure),
            ),
            dialogue_event(
                "event-3",
                3,
                WorkRequeued(
                    "failed-work-secret",
                    failed_trigger,
                    1,
                    failure,
                    next_trigger,
                    2,
                ),
            ),
        )

        report = dispatcher.publish_facts(
            (DialogueCommittedFact("session-1", events, True),)
        )

        self.assertEqual((), report.diagnostics)
        self.assertEqual(0, report.buffered_events)
        visible = subscription.read_available()
        self.assertEqual((1, 2, 3), tuple(item.sequence for item in visible))
        self.assertEqual(
            ("session_started", "state_changed", "state_changed"),
            tuple(item.event_type for item in visible),
        )
        self.assertEqual(3, subscription.cursor)
        self.assertEqual([WorkWakeHint("session-1", 3)], port.hints)
        rendered = repr((visible, port.hints))
        for secret in (
            "failure-id-secret",
            "HOST_TIMEOUT_SECRET",
            failure.proof_digest,
            "failed-work-secret",
            "retry-work-secret",
        ):
            self.assertNotIn(secret, rendered)

    def test_mapping_and_delivery_failures_are_stable_diagnostics(self):
        hub, _observer = self.hub_and_observer()
        dispatcher = AfterCommitDispatcher(hub)
        mapping = dispatcher.publish_facts(
            (DialogueCommittedFact("", (), True),)
        )
        self.assertEqual(
            "AFTER_COMMIT_MAPPING_FAILED",
            mapping.diagnostics[0].code,
        )
        self.assertEqual(0, mapping.diagnostics[0].first_sequence)

        with self.assertRaisesRegex(ValueError, "INVALID_AFTER_COMMIT_BATCHES"):
            dispatcher.publish_all([])  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "INVALID_COMMITTED_FACTS"):
            dispatcher.publish_facts([])  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "INVALID_OBSERVER_HUB"):
            AfterCommitDispatcher(object())  # type: ignore[arg-type]
        with self.assertRaises(AfterCommitError):
            dialogue_batch("", ())
        with self.assertRaises(AfterCommitError):
            registry_batch("", ())

    def test_hub_infrastructure_failure_keeps_the_ordered_event_for_retry(self):
        class ToggleLocks:
            def __init__(self):
                self.fail = True

            def assert_none_held(self):
                if self.fail:
                    raise RuntimeError("lock tracker unavailable")

        tracker = ToggleLocks()
        observer = RecordingObserver()
        hub = ObserverHub(tracker)
        hub.register_fixed(observer)
        hub.freeze()
        dispatcher = AfterCommitDispatcher(hub)
        first = dialogue_batch(
            "session-1",
            (dialogue_event("event-1", 1, SessionStarted(trigger())),),
        )

        failed = dispatcher.publish_all((first,))
        tracker.fail = False
        retried = dispatcher.publish_all(())

        self.assertEqual("AFTER_COMMIT_PUBLISH_FAILED", failed.diagnostics[0].code)
        self.assertEqual(1, failed.buffered_events)
        self.assertEqual(0, retried.buffered_events)
        self.assertEqual(1, observer.batches[0].events[0].sequence)

    def test_hub_baseexception_is_stable_and_kept_for_empty_retry(self):
        class ToggleLocks:
            def __init__(self):
                self.fail = True

            def assert_none_held(self):
                if self.fail:
                    raise KeyboardInterrupt("private infrastructure detail")

        tracker = ToggleLocks()
        observer = RecordingObserver()
        hub = ObserverHub(tracker)
        hub.register_fixed(observer)
        hub.freeze()
        dispatcher = AfterCommitDispatcher(hub)
        first = dialogue_batch(
            "session-1",
            (dialogue_event("event-1", 1, SessionStarted(trigger())),),
        )

        failed = dispatcher.publish_all((first,))
        tracker.fail = False
        retried = dispatcher.publish_all(())

        self.assertEqual(
            ("AFTER_COMMIT_PUBLISH_FAILED",),
            tuple(item.code for item in failed.diagnostics),
        )
        self.assertEqual(1, failed.buffered_events)
        self.assertEqual(0, retried.buffered_events)
        self.assertEqual(1, observer.batches[0].events[0].sequence)

    def test_same_stream_sequence_with_different_identity_is_rejected(self):
        hub, observer = self.hub_and_observer()
        dispatcher = AfterCommitDispatcher(hub)
        first = dialogue_batch(
            "session-1",
            (dialogue_event("event-1", 1, SessionStarted(trigger())),),
        )
        conflict = dialogue_batch(
            "session-1",
            (dialogue_event("event-other", 1, SessionStarted(trigger())),),
        )

        dispatcher.publish_all((first,))
        rejected = dispatcher.publish_all((conflict,))

        self.assertEqual("AFTER_COMMIT_EVENT_INTEGRITY", rejected.diagnostics[0].code)
        self.assertEqual(1, len(observer.batches))


class CoordinatorDispatchTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root_path = Path(self.temporary.name).resolve()
        self.root = SecureDirectory.open(self.root_path)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.locks = DomainLockManager(self.root_path / "locks")
        self.observer = RecordingObserver()
        hub = ObserverHub(self.locks)
        hub.register_fixed(self.observer)
        hub.freeze()
        self.dispatcher = AfterCommitDispatcher(hub)

    def tearDown(self):
        self.locks.close()
        self.dialogues.close()
        self.root.close()
        self.temporary.cleanup()

    def coordinator(self, fault_hook=lambda _stage: None):
        return DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            after_commit_dispatcher=self.dispatcher,
            fault_hook=fault_hook,
        )

    def test_resolve_dispatches_registry_and_dialogue_only_after_unlock(self):
        first = self.coordinator().resolve(config("dlg-a"))

        by_stream = {
            (batch.stream_kind, batch.stream_id): tuple(
                event.sequence for event in batch.events
            )
            for batch in self.observer.batches
        }
        self.assertEqual((1, 2), by_stream[(StreamKind.REGISTRY, "registry-1")])
        self.assertEqual((1,), by_stream[(StreamKind.DIALOGUE, "dlg-a")])
        self.assertEqual("dlg-a", first.dialogue_state.session_id)
        self.locks.assert_none_held()

        before = len(self.observer.batches)
        self.coordinator().resolve(config("dlg-a"))
        self.assertEqual(before, len(self.observer.batches))

    def test_commit_followed_by_fault_is_still_dispatched(self):
        def fail(stage):
            if stage == "registry_created":
                raise RuntimeError("simulated process edge")

        with self.assertRaisesRegex(RuntimeError, "simulated process edge"):
            self.coordinator(fail).resolve(config("dlg-a"))

        self.assertEqual(1, len(self.observer.batches))
        batch = self.observer.batches[0]
        self.assertEqual(StreamKind.REGISTRY, batch.stream_kind)
        self.assertEqual((1, 2), tuple(item.sequence for item in batch.events))
        self.locks.assert_none_held()

    def test_post_marker_registry_exception_dispatches_and_next_entry_catches_up(
        self,
    ):
        coordinator = self.coordinator()
        original_commit = _RegistryTransactionLog.commit
        raised = False

        def fail_after_marker(log, request, authority):
            nonlocal raised
            outcome = original_commit(log, request, authority)
            if not outcome.replayed and not raised:
                raised = True
                raise RuntimeError("post-marker registry fault")
            return outcome

        with mock.patch.object(
            _RegistryTransactionLog,
            "commit",
            autospec=True,
            side_effect=fail_after_marker,
        ), self.assertRaisesRegex(
            RuntimeError,
            "post-marker registry fault",
        ):
            coordinator.resolve(config("dlg-a"))

        self.assertEqual(1, len(self.observer.batches))
        self.assertEqual(StreamKind.REGISTRY, self.observer.batches[0].stream_kind)
        self.assertEqual(
            (1, 2),
            tuple(event.sequence for event in self.observer.batches[0].events),
        )

        coordinator.resolve(config("dlg-a"))

        dialogue_sequences = tuple(
            event.sequence
            for batch in self.observer.batches
            if batch.stream_kind is StreamKind.DIALOGUE
            for event in batch.events
        )
        self.assertEqual((1,), dialogue_sequences)
        self.locks.assert_none_held()

    def test_post_marker_dialogue_exception_is_dispatched_before_reraising(self):
        coordinator = self.coordinator()
        original_commit = _DialogueTransactionLog.commit
        raised = False

        def fail_after_marker(log, request, authority):
            nonlocal raised
            outcome = original_commit(log, request, authority)
            if not outcome.replayed and not raised:
                raised = True
                raise RuntimeError("post-marker dialogue fault")
            return outcome

        with mock.patch.object(
            _DialogueTransactionLog,
            "commit",
            autospec=True,
            side_effect=fail_after_marker,
        ), self.assertRaisesRegex(
            RuntimeError,
            "post-marker dialogue fault",
        ):
            coordinator.resolve(config("dlg-a"))

        streams = {
            (batch.stream_kind, batch.stream_id): tuple(
                event.sequence for event in batch.events
            )
            for batch in self.observer.batches
        }
        self.assertEqual((1, 2), streams[(StreamKind.REGISTRY, "registry-1")])
        self.assertEqual((1,), streams[(StreamKind.DIALOGUE, "dlg-a")])
        self.locks.assert_none_held()

    def test_capture_audit_failure_preserves_the_commit_exception(self):
        coordinator = self.coordinator()
        original_commit = _RegistryTransactionLog.commit
        raised_after_marker = False

        def fail_after_marker(log, request, authority):
            nonlocal raised_after_marker
            outcome = original_commit(log, request, authority)
            if not outcome.replayed and not raised_after_marker:
                raised_after_marker = True
                raise RuntimeError("original-post-marker")
            return outcome

        with mock.patch.object(
            _RegistryTransactionLog,
            "commit",
            autospec=True,
            side_effect=fail_after_marker,
        ), mock.patch.object(
            coordinator,
            "_capture_durable_registry_request",
            side_effect=RuntimeError("untrusted-audit-detail"),
        ), self.assertRaisesRegex(
            RuntimeError,
            "original-post-marker",
        ) as raised:
            coordinator.resolve(config("dlg-a"))

        self.assertEqual("original-post-marker", str(raised.exception))
        self.assertEqual(
            ["AFTER_COMMIT_AUDIT_FAILED"],
            raised.exception.__notes__,
        )
        self.assertNotIn("untrusted-audit-detail", str(raised.exception))

        coordinator.resolve(config("dlg-a"))

        registry_sequences = tuple(
            event.sequence
            for batch in self.observer.batches
            if batch.stream_kind is StreamKind.REGISTRY
            for event in batch.events
        )
        self.assertEqual((1, 2), registry_sequences)

    def test_broken_exception_note_cannot_mask_the_commit_exception(self):
        class BrokenNoteError(RuntimeError):
            def add_note(self, _note):
                raise AssertionError("broken add_note must stay isolated")

        coordinator = self.coordinator()
        original_commit = _RegistryTransactionLog.commit
        original_error = BrokenNoteError("original-post-marker")
        raised_after_marker = False

        def fail_after_marker(log, request, authority):
            nonlocal raised_after_marker
            outcome = original_commit(log, request, authority)
            if not outcome.replayed and not raised_after_marker:
                raised_after_marker = True
                raise original_error
            return outcome

        with mock.patch.object(
            _RegistryTransactionLog,
            "commit",
            autospec=True,
            side_effect=fail_after_marker,
        ), mock.patch.object(
            coordinator,
            "_capture_durable_registry_request",
            side_effect=RuntimeError("audit failed"),
        ), self.assertRaises(BrokenNoteError) as raised:
            coordinator.resolve(config("dlg-a"))

        self.assertIs(original_error, raised.exception)

    def test_stale_concurrent_catch_up_cannot_overwrite_invalidation(self):
        coordinator = self.coordinator()
        coordinator.resolve(config("dlg-a"))

        operation_collector = coordinator._start_operation()
        coordinator._invalidate_durable_catch_up()
        original_collect = coordinator._collect_durable_facts
        original_commit = _RegistryTransactionLog.commit
        scan_finished = threading.Event()
        release_scan = threading.Event()
        catch_up_errors = []
        raised_after_marker = False

        def gated_collect(collector):
            original_collect(collector)
            scan_finished.set()
            if not release_scan.wait(10):
                raise RuntimeError("catch-up barrier timed out")

        def fail_after_marker(log, request, authority):
            nonlocal raised_after_marker
            outcome = original_commit(log, request, authority)
            if not outcome.replayed and not raised_after_marker:
                raised_after_marker = True
                raise RuntimeError("original-post-marker")
            return outcome

        def catch_up():
            try:
                coordinator._ensure_durable_catch_up()
            except BaseException as exc:
                catch_up_errors.append(exc)

        catch_up_thread = threading.Thread(target=catch_up)
        try:
            with mock.patch.object(
                coordinator,
                "_collect_durable_facts",
                side_effect=gated_collect,
            ), mock.patch.object(
                _RegistryTransactionLog,
                "commit",
                autospec=True,
                side_effect=fail_after_marker,
            ), mock.patch.object(
                coordinator,
                "_capture_durable_registry_request",
                side_effect=RuntimeError("audit failed"),
            ):
                catch_up_thread.start()
                self.assertTrue(scan_finished.wait(10))
                try:
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "original-post-marker",
                    ):
                        coordinator._resolve(
                            config("dlg-b"), operation_collector
                        )
                finally:
                    coordinator._finish_operation(operation_collector)
        finally:
            release_scan.set()
            catch_up_thread.join(10)

        self.assertFalse(catch_up_thread.is_alive())
        self.assertEqual([], catch_up_errors)
        self.assertFalse(coordinator._catch_up_complete)

        coordinator.resolve(config("dlg-b"))

        registry_sequences = tuple(
            event.sequence
            for batch in self.observer.batches
            if batch.stream_kind is StreamKind.REGISTRY
            for event in batch.events
        )
        self.assertEqual((1, 2, 3, 4, 5, 6), registry_sequences)
        self.assertEqual(0, self.dispatcher.publish_all(()).buffered_events)

    def test_observer_failure_does_not_change_the_committed_result(self):
        self.observer.fail = True
        resolution = self.coordinator().resolve(config("dlg-a"))

        self.assertEqual("dlg-a", resolution.registry_state.current_session_id)
        self.assertEqual(1, resolution.dialogue_state.sequence)

        self.observer.fail = False
        self.coordinator().resolve(config("dlg-a"))
        self.assertTrue(self.observer.batches)

    def test_observer_cannot_reenter_a_semantic_command(self):
        coordinator = self.coordinator()

        class ReentrantObserver:
            name = "reentrant"
            accepted_streams = frozenset({StreamKind.DIALOGUE})

            def __init__(self):
                self.code = None

            def on_batch(self, _batch):
                try:
                    coordinator.resolve(config("dlg-a"))
                except RuntimeError as exc:
                    self.code = str(exc)

        reentrant = ReentrantObserver()
        hub = ObserverHub(self.locks)
        hub.register_fixed(reentrant)
        hub.freeze()
        coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            after_commit_dispatcher=AfterCommitDispatcher(hub),
        )

        coordinator.resolve(config("dlg-a"))

        self.assertEqual("OBSERVER_REENTRANCY", reentrant.code)

    def test_cold_start_replays_registry_and_all_activated_dialogues(self):
        coordinator = self.coordinator()
        coordinator.resolve(config("dlg-a"))
        coordinator.resolve(config("dlg-b"))

        restarted_observer = RecordingObserver()
        restarted_hub = ObserverHub(self.locks)
        restarted_hub.register_fixed(restarted_observer)
        restarted_hub.freeze()
        restarted = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            after_commit_dispatcher=AfterCommitDispatcher(restarted_hub),
        )

        report = restarted.replay_committed()

        self.assertIsNotNone(report)
        streams = {
            (batch.stream_kind, batch.stream_id): tuple(
                event.sequence for event in batch.events
            )
            for batch in restarted_observer.batches
        }
        self.assertEqual(
            (1, 2, 3, 4, 5, 6),
            streams[(StreamKind.REGISTRY, "registry-1")],
        )
        self.assertEqual((1, 2), streams[(StreamKind.DIALOGUE, "dlg-a")])
        self.assertEqual((1,), streams[(StreamKind.DIALOGUE, "dlg-b")])

    def test_first_business_entry_automatically_replays_all_durable_streams(self):
        coordinator = self.coordinator()
        coordinator.resolve(config("dlg-a"))
        coordinator.resolve(config("dlg-b"))

        restarted_observer = RecordingObserver()
        restarted_hub = ObserverHub(self.locks)
        restarted_hub.register_fixed(restarted_observer)
        restarted_hub.freeze()
        restarted = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            after_commit_dispatcher=AfterCommitDispatcher(restarted_hub),
        )

        restarted.resolve(config("dlg-b"))

        streams = {
            (batch.stream_kind, batch.stream_id): tuple(
                event.sequence for event in batch.events
            )
            for batch in restarted_observer.batches
        }
        self.assertEqual(
            (1, 2, 3, 4, 5, 6),
            streams[(StreamKind.REGISTRY, "registry-1")],
        )
        self.assertEqual((1, 2), streams[(StreamKind.DIALOGUE, "dlg-a")])
        self.assertEqual((1,), streams[(StreamKind.DIALOGUE, "dlg-b")])

    def test_cold_replay_never_recreates_a_missing_active_store(self):
        coordinator = self.coordinator()
        coordinator.resolve(config("dlg-a"))
        active = self.root_path / "dialogues" / session_directory_component(
            "dlg-a"
        )
        events = active / "events"
        transactions = active / "transactions"
        removed_events = self.root_path / "removed-active-events"
        removed_transactions = self.root_path / "removed-active-transactions"
        events.rename(removed_events)
        transactions.rename(removed_transactions)

        with self.assertRaises(CoordinatorError) as raised:
            self.coordinator().replay_committed()

        self.assertEqual("HISTORICAL_DIALOGUE_MISSING", raised.exception.code)
        self.assertFalse(events.exists())
        self.assertFalse(transactions.exists())
        self.assertTrue(removed_events.is_dir())
        self.assertTrue(removed_transactions.is_dir())
        self.locks.assert_none_held()

    def test_cold_replay_fails_closed_when_historical_session_is_missing(self):
        coordinator = self.coordinator()
        coordinator.resolve(config("dlg-a"))
        coordinator.resolve(config("dlg-b"))
        historical = self.root_path / "dialogues" / session_directory_component(
            "dlg-a"
        )
        removed = self.root_path / "removed-dlg-a"
        historical.rename(removed)

        with self.assertRaises(CoordinatorError) as raised:
            self.coordinator().replay_committed()

        self.assertEqual("HISTORICAL_DIALOGUE_MISSING", raised.exception.code)
        self.assertFalse(historical.exists())
        self.assertTrue(removed.is_dir())
        self.locks.assert_none_held()

    def test_cold_replay_proves_historical_tip_without_repairing_it(self):
        coordinator = self.coordinator()
        coordinator.resolve(config("dlg-a"))
        coordinator.resolve(config("dlg-b"))
        historical = self.root_path / "dialogues" / session_directory_component(
            "dlg-a"
        )
        transactions = historical / "transactions"
        marker = next(transactions.iterdir())
        removed_marker = self.root_path / marker.name
        marker.rename(removed_marker)

        with self.assertRaises(CoordinatorError) as raised:
            self.coordinator().replay_committed()

        self.assertEqual("HISTORICAL_DIALOGUE_INTEGRITY", raised.exception.code)
        self.assertFalse(marker.exists())
        self.assertTrue(removed_marker.is_file())
        self.assertTrue(any((historical / "events").iterdir()))
        self.assertEqual([], list((historical / "quarantine").iterdir()))
        self.locks.assert_none_held()

    def test_real_adapters_receive_browser_state_and_only_a_work_hint(self):
        public = PublicStreamObserver()

        class WakePort:
            def __init__(self):
                self.hints = []

            def notify(self, hint):
                self.hints.append(hint)

        port = WakePort()
        hub = ObserverHub(self.locks)
        hub.register_fixed(public)
        hub.register_fixed(WorkWakeObserver(port))
        hub.freeze()
        subscription = public.subscribe("dlg-a", after_sequence=0)
        coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            after_commit_dispatcher=AfterCommitDispatcher(hub),
        )

        coordinator.resolve(config("dlg-a"))

        (visible,) = subscription.read_available()
        self.assertEqual("session_started", visible.event_type)
        self.assertEqual((), visible.fields)
        self.assertEqual(1, len(port.hints))
        self.assertEqual("dlg-a", port.hints[0].stream_id)
        self.assertEqual(1, port.hints[0].through_sequence)


if __name__ == "__main__":
    unittest.main()
