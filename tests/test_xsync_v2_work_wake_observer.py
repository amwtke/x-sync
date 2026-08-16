import unittest
from dataclasses import FrozenInstanceError, fields

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.observer import (
    CommittedBatch,
    CommittedEventView,
    ImmutablePayloadView,
    ObserverHub,
    StreamKind,
)
from xsync_v2.observers.work_wake import WorkWakeHint, WorkWakeObserver


def event(event_id: str, sequence: int, tag: str) -> CommittedEventView:
    return CommittedEventView(
        event_id,
        sequence,
        ImmutablePayloadView(
            tag,
            (
                ("work_id", "work-secret"),
                ("claim_id", "claim-secret"),
                ("evidence_digest", "sha256:secret"),
            ),
        ),
    )


def batch(
    *events: CommittedEventView,
    stream_id: str = "session-1",
    kind: StreamKind = StreamKind.DIALOGUE,
) -> CommittedBatch:
    return CommittedBatch(kind, stream_id, events)


class RecordingPort:
    def __init__(self) -> None:
        self.hints: list[WorkWakeHint] = []
        self.fail = False

    def notify(self, hint: WorkWakeHint) -> None:
        if self.fail:
            raise RuntimeError("wake port full")
        self.hints.append(hint)


class NoLocks:
    def assert_none_held(self) -> None:
        return None


class BatchRecorder:
    name = "z-recorder"
    accepted_streams = frozenset({StreamKind.DIALOGUE})

    def __init__(self) -> None:
        self.event_ids: list[str] = []

    def on_batch(self, committed: CommittedBatch) -> None:
        self.event_ids.extend(item.event_id for item in committed.events)


class WorkWakeObserverTest(unittest.TestCase):
    def test_work_creating_events_emit_only_a_recheck_hint(self) -> None:
        port = RecordingPort()
        observer = WorkWakeObserver(port)
        observer.on_batch(
            batch(
                event("event-1", 1, "session_started"),
                event("event-2", 2, "topic_started"),
            )
        )

        self.assertEqual(
            [WorkWakeHint("session-1", 2)],
            port.hints,
        )
        self.assertEqual(
            ("stream_id", "through_sequence"),
            tuple(item.name for item in fields(port.hints[0])),
        )
        rendered = repr(port.hints[0])
        for secret in ("work-secret", "claim-secret", "sha256:secret"):
            self.assertNotIn(secret, rendered)

    def test_all_current_trigger_producers_wake(self) -> None:
        tags = (
            "session_started",
            "topic_selection_submitted",
            "topic_clarification_answered",
            "topic_started",
            "learner_turn_submitted",
            "lens_changed",
            "help_requested",
            "topic_resumed",
            "topic_switch_requested",
            "work_requeued",
            "work_recovery_requested",
        )
        for tag in tags:
            with self.subTest(tag=tag):
                port = RecordingPort()
                observer = WorkWakeObserver(port)
                observer.on_batch(batch(event("event-1", 1, tag)))
                self.assertEqual([WorkWakeHint("session-1", 1)], port.hints)

    def test_failure_batch_wakes_once_only_when_work_is_requeued(self) -> None:
        port = RecordingPort()
        observer = WorkWakeObserver(port)

        observer.on_batch(
            batch(
                event("event-failed", 1, "state_changed"),
                event("event-requeued", 2, "work_requeued"),
            )
        )

        self.assertEqual([WorkWakeHint("session-1", 2)], port.hints)
        rendered = repr(port.hints)
        for secret in ("work-secret", "claim-secret", "sha256:secret"):
            self.assertNotIn(secret, rendered)

    def test_dead_letter_does_not_wake_but_recovery_request_does(self) -> None:
        port = RecordingPort()
        observer = WorkWakeObserver(port)

        observer.on_batch(
            batch(
                event("event-failed", 1, "state_changed"),
                event("event-dead-lettered", 2, "state_changed"),
            )
        )
        self.assertEqual([], port.hints)

        observer.on_batch(
            batch(
                event(
                    "event-recovery-requested",
                    3,
                    "work_recovery_requested",
                )
            )
        )
        self.assertEqual([WorkWakeHint("session-1", 3)], port.hints)

    def test_non_work_event_advances_cursor_without_waking(self) -> None:
        port = RecordingPort()
        observer = WorkWakeObserver(port)
        observer.on_batch(
            batch(event("event-1", 1, "agent_turn_committed"))
        )
        observer.on_batch(
            batch(event("event-2", 2, "learner_turn_submitted"))
        )
        self.assertEqual([WorkWakeHint("session-1", 2)], port.hints)

    def test_duplicate_is_idempotent_and_gap_fails_closed(self) -> None:
        port = RecordingPort()
        observer = WorkWakeObserver(port)
        first = batch(event("event-1", 1, "session_started"))
        observer.on_batch(first)
        observer.on_batch(first)
        with self.assertRaisesRegex(RuntimeError, "WORK_WAKE_SEQUENCE_GAP"):
            observer.on_batch(
                batch(event("event-3", 3, "learner_turn_submitted"))
            )
        self.assertEqual([WorkWakeHint("session-1", 1)], port.hints)

    def test_failed_notification_can_be_retried_by_observer_hub(self) -> None:
        port = RecordingPort()
        port.fail = True
        observer = WorkWakeObserver(port)
        committed = batch(event("event-1", 1, "session_started"))
        with self.assertRaisesRegex(RuntimeError, "wake port full"):
            observer.on_batch(committed)

        port.fail = False
        observer.on_batch(committed)
        self.assertEqual([WorkWakeHint("session-1", 1)], port.hints)

    def test_wake_failure_is_isolated_by_observer_hub(self) -> None:
        port = RecordingPort()
        port.fail = True
        recorder = BatchRecorder()
        hub = ObserverHub(NoLocks())
        hub.register_fixed(WorkWakeObserver(port))
        hub.register_fixed(recorder)
        hub.freeze()

        report = hub.publish(batch(event("event-1", 1, "session_started")))

        self.assertEqual(("host-wake",), report.failed)
        self.assertEqual(["event-1"], recorder.event_ids)

    def test_vector_cursor_is_per_stream(self) -> None:
        port = RecordingPort()
        observer = WorkWakeObserver(port)
        observer.on_batch(
            batch(
                event("event-a", 1, "learner_turn_submitted"),
                stream_id="session-a",
            )
        )
        observer.on_batch(
            batch(
                event("event-b", 1, "learner_turn_submitted"),
                stream_id="session-b",
            )
        )
        self.assertEqual(
            [
                WorkWakeHint("session-a", 1),
                WorkWakeHint("session-b", 1),
            ],
            port.hints,
        )

    def test_registry_batch_is_rejected_and_hint_is_immutable(self) -> None:
        port = RecordingPort()
        observer = WorkWakeObserver(port)
        with self.assertRaisesRegex(
            ValueError, "WORK_WAKE_KIND_UNSUPPORTED"
        ):
            observer.on_batch(
                batch(
                    event("event-1", 1, "session_started"),
                    stream_id="registry",
                    kind=StreamKind.REGISTRY,
                )
            )
        hint = WorkWakeHint("session-1", 1)
        with self.assertRaises(FrozenInstanceError):
            hint.through_sequence = 2

    def test_invalid_ports_hints_and_batches_fail_closed(self) -> None:
        class MissingPort:
            pass

        class NonCallablePort:
            notify = 7

        for port in (MissingPort(), NonCallablePort()):
            with self.assertRaisesRegex(
                ValueError, "INVALID_WORK_WAKE_PORT"
            ):
                WorkWakeObserver(port)
        for values in (("", 1), ("session-1", 0), ("session-1", True)):
            with self.assertRaisesRegex(
                ValueError, "INVALID_WORK_WAKE_HINT"
            ):
                WorkWakeHint(*values)
        observer = WorkWakeObserver(RecordingPort())
        with self.assertRaisesRegex(ValueError, "INVALID_COMMITTED_BATCH"):
            observer.on_batch(object())


if __name__ == "__main__":
    unittest.main()
