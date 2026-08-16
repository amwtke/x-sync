# ruff: noqa: RUF001

import threading
import unittest
from dataclasses import FrozenInstanceError

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.observer import (
    CommittedBatch,
    CommittedEventView,
    ImmutablePayloadView,
    StreamKind,
)
from xsync_v2.observers.public_stream import (
    PublicStreamError,
    PublicStreamEvent,
    PublicStreamObserver,
)


def event(
    event_id: str,
    sequence: int,
    tag: str = "agent_turn_committed",
    fields: tuple[tuple[str, str], ...] = (
        ("heard", "我听见了"),
        ("question_id", "question-1"),
        ("question", "接下来会发生什么？"),
    ),
) -> CommittedEventView:
    return CommittedEventView(
        event_id,
        sequence,
        ImmutablePayloadView(tag, fields),
    )


def batch(
    *events: CommittedEventView,
    stream_id: str = "session-1",
    kind: StreamKind = StreamKind.DIALOGUE,
) -> CommittedBatch:
    return CommittedBatch(kind, stream_id, events)


class PublicStreamObserverTest(unittest.TestCase):
    def test_projects_only_allowlisted_public_fields(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        observer.on_batch(
            batch(
                event(
                    "event-1",
                    1,
                    fields=(
                        ("question", "你会怎么验证它？"),
                        ("claim_id", "claim-secret"),
                        ("lease_version", "91"),
                        ("evidence_digest", "sha256:secret"),
                        ("runtime_epoch", "epoch-secret"),
                        ("heard", "先接住你的判断"),
                    ),
                )
            )
        )

        (projected,) = subscription.read_available()
        self.assertEqual("session-1", projected.stream_id)
        self.assertEqual("event-1", projected.event_id)
        self.assertEqual(1, projected.sequence)
        self.assertEqual("agent_turn_committed", projected.event_type)
        self.assertEqual(
            (
                ("heard", "先接住你的判断"),
                ("question", "你会怎么验证它？"),
            ),
            projected.fields,
        )
        rendered = repr(projected)
        for secret in (
            "claim-secret",
            "lease_version",
            "sha256:secret",
            "epoch-secret",
        ):
            self.assertNotIn(secret, rendered)

    def test_topic_switch_exposes_only_the_paused_topic_identity(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        observer.on_batch(
            batch(
                event(
                    "event-switch",
                    1,
                    tag="topic_switch_requested",
                    fields=(
                        ("topic_run_id", "topic-1"),
                        ("work_id", "work-secret"),
                        ("evidence_digest", "sha256:secret"),
                    ),
                )
            )
        )

        (projected,) = subscription.read_available()
        self.assertEqual("topic_switch_requested", projected.event_type)
        self.assertEqual((("topic_run_id", "topic-1"),), projected.fields)

    def test_unknown_or_internal_event_advances_cursor_without_leaking(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        observer.on_batch(
            batch(
                event(
                    "event-1",
                    1,
                    tag="lease_claimed",
                    fields=(("submission_handle", "super-secret"),),
                ),
                event(
                    "event-2",
                    2,
                    tag="session_deactivation_prepared",
                    fields=(
                        ("handoff_id", "handoff-secret"),
                        ("fence_generation", "9"),
                    ),
                ),
            )
        )

        projected = subscription.read_available()
        self.assertEqual((1, 2), tuple(item.sequence for item in projected))
        self.assertEqual(
            ("state_changed", "state_changed"),
            tuple(item.event_type for item in projected),
        )
        self.assertTrue(all(item.fields == () for item in projected))
        self.assertEqual(2, subscription.cursor)

    def test_topic_selection_exposes_only_the_selected_candidate(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        observer.on_batch(
            batch(
                event(
                    "event-select",
                    1,
                    tag="topic_selection_submitted",
                    fields=(
                        ("candidate", "Outbox"),
                        ("work_id", "work-secret"),
                        ("evidence_digest", "sha256:secret"),
                    ),
                )
            )
        )

        (projected,) = subscription.read_available()
        self.assertEqual("topic_selection_submitted", projected.event_type)
        self.assertEqual((("candidate", "Outbox"),), projected.fields)
        self.assertNotIn("work-secret", repr(projected))

    def test_topic_clarification_exposes_question_but_never_answer(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        observer.on_batch(
            batch(
                event(
                    "event-clarify",
                    1,
                    tag="topic_clarification_requested",
                    fields=(
                        ("question_id", "clarification-1"),
                        ("question", "你更关心哪个边界？"),
                        ("work_id", "work-secret"),
                    ),
                ),
                event(
                    "event-answer",
                    2,
                    tag="topic_clarification_answered",
                    fields=(
                        ("question_id", "clarification-1"),
                        ("answer", "answer-secret"),
                        ("evidence_digest", "sha256:secret"),
                    ),
                ),
            )
        )

        requested, answered = subscription.read_available()
        self.assertEqual(
            (
                ("question_id", "clarification-1"),
                ("question", "你更关心哪个边界？"),
            ),
            requested.fields,
        )
        self.assertEqual(
            (("question_id", "clarification-1"),),
            answered.fields,
        )
        self.assertNotIn("answer-secret", repr((requested, answered)))

    def test_failure_lifecycle_is_contiguous_and_fail_closed(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        secret_fields = (
            ("work_id", "work-secret"),
            ("failure_id", "failure-secret"),
            ("proof_digest", "sha256:proof-secret"),
            ("evidence_digest", "sha256:evidence-secret"),
            ("runtime_epoch", "epoch-secret"),
        )

        observer.on_batch(
            batch(
                event("event-failed", 1, tag="state_changed", fields=secret_fields),
                event("event-requeued", 2, tag="work_requeued", fields=secret_fields),
                event(
                    "event-recovery-requested",
                    3,
                    tag="work_recovery_requested",
                    fields=secret_fields,
                ),
            )
        )

        projected = subscription.read_available()
        self.assertEqual((1, 2, 3), tuple(item.sequence for item in projected))
        self.assertEqual(
            ("state_changed", "state_changed", "state_changed"),
            tuple(item.event_type for item in projected),
        )
        self.assertTrue(all(item.fields == () for item in projected))
        self.assertEqual(3, subscription.cursor)
        rendered = repr(projected)
        for _name, secret in secret_fields:
            self.assertNotIn(secret, rendered)

    def test_retained_events_support_cursor_catch_up(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=3,
            subscriber_queue_limit=3,
        )
        observer.on_batch(
            batch(
                event("event-1", 1),
                event("event-2", 2),
                event("event-3", 3),
            )
        )

        subscription = observer.subscribe("session-1", after_sequence=1)
        self.assertEqual(
            (2, 3),
            tuple(item.sequence for item in subscription.read_available()),
        )
        self.assertEqual(3, subscription.cursor)

    def test_cursor_outside_retention_window_requires_state_resync(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=2,
            subscriber_queue_limit=2,
        )
        observer.on_batch(
            batch(
                event("event-1", 1),
                event("event-2", 2),
                event("event-3", 3),
            )
        )

        with self.assertRaises(PublicStreamError) as caught:
            observer.subscribe("session-1", after_sequence=0)
        self.assertEqual("CURSOR_EXPIRED", caught.exception.code)

    def test_future_cursor_is_rejected_explicitly(self) -> None:
        observer = PublicStreamObserver()
        with self.assertRaises(PublicStreamError) as caught:
            observer.subscribe("session-1", after_sequence=1)
        self.assertEqual("CURSOR_AHEAD", caught.exception.code)

    def test_catch_up_larger_than_subscriber_queue_requires_resync(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=4,
            subscriber_queue_limit=2,
        )
        observer.on_batch(
            batch(
                event("event-1", 1),
                event("event-2", 2),
                event("event-3", 3),
            )
        )
        with self.assertRaises(PublicStreamError) as caught:
            observer.subscribe("session-1", after_sequence=0)
        self.assertEqual("RESYNC_REQUIRED", caught.exception.code)

    def test_slow_subscriber_is_disconnected_without_harming_fast_one(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=2,
        )
        slow = observer.subscribe("session-1", after_sequence=0)
        fast = observer.subscribe("session-1", after_sequence=0)

        observer.on_batch(batch(event("event-1", 1)))
        self.assertEqual((1,), tuple(item.sequence for item in fast.read_available()))
        observer.on_batch(
            batch(event("event-2", 2), event("event-3", 3))
        )

        with self.assertRaises(PublicStreamError) as caught:
            slow.read_available()
        self.assertEqual("RESYNC_REQUIRED", caught.exception.code)
        self.assertFalse(slow.connected)
        self.assertEqual(
            (2, 3),
            tuple(item.sequence for item in fast.read_available()),
        )
        self.assertTrue(fast.connected)

    def test_duplicate_is_idempotent_and_gap_stops_consumption(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        first = batch(event("event-1", 1))
        observer.on_batch(first)
        observer.on_batch(first)
        with self.assertRaisesRegex(
            RuntimeError, "PUBLIC_STREAM_SEQUENCE_GAP"
        ):
            observer.on_batch(batch(event("event-3", 3)))
        self.assertEqual(
            (1,),
            tuple(item.sequence for item in subscription.read_available()),
        )

    def test_stream_cursors_are_independent(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        first = observer.subscribe("session-1", after_sequence=0)
        second = observer.subscribe("session-2", after_sequence=0)
        observer.on_batch(batch(event("event-a", 1), stream_id="session-1"))
        observer.on_batch(batch(event("event-b", 1), stream_id="session-2"))
        self.assertEqual("event-a", first.read_available()[0].event_id)
        self.assertEqual("event-b", second.read_available()[0].event_id)

    def test_invalid_batch_is_atomic_and_registry_is_not_accepted(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        with self.assertRaisesRegex(
            ValueError, "PUBLIC_STREAM_PAYLOAD_INVALID"
        ):
            observer.on_batch(
                batch(
                    event(
                        "event-1",
                        1,
                        fields=(
                            ("question", "first"),
                            ("question", "ambiguous"),
                        ),
                    )
                )
            )
        self.assertEqual((), subscription.read_available())

        with self.assertRaisesRegex(
            ValueError, "PUBLIC_STREAM_KIND_UNSUPPORTED"
        ):
            observer.on_batch(
                batch(
                    event("registry-event", 1),
                    stream_id="registry",
                    kind=StreamKind.REGISTRY,
                )
            )

    def test_subscription_close_and_read_limit_are_explicit(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        observer.on_batch(
            batch(event("event-1", 1), event("event-2", 2))
        )
        self.assertEqual(1, len(subscription.read_available(max_events=1)))
        self.assertEqual(1, subscription.cursor)
        subscription.close()

    def test_wait_available_blocks_without_polling_and_wakes_on_commit(self) -> None:
        observer = PublicStreamObserver()
        subscription = observer.subscribe("session-1", after_sequence=0)
        entered = threading.Event()
        finished = threading.Event()
        outcomes: list[bool] = []

        def waiter() -> None:
            entered.set()
            outcomes.append(subscription.wait_available(timeout=5.0))
            finished.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        self.assertTrue(entered.wait(1.0))
        observer.on_batch(batch(event("event-1", 1)))
        self.assertTrue(finished.wait(1.0))
        thread.join(1.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual([True], outcomes)
        self.assertEqual(
            (1,),
            tuple(item.sequence for item in subscription.read_available()),
        )

    def test_wait_timeout_validation_and_close_are_explicit(self) -> None:
        observer = PublicStreamObserver()
        subscription = observer.subscribe("session-1", after_sequence=0)
        self.assertFalse(subscription.wait_available(timeout=0))
        for timeout in (-1, True, "1", float("inf"), float("nan")):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(ValueError, "INVALID_STREAM_TIMEOUT"):
                    subscription.wait_available(timeout=timeout)
        subscription.close()
        with self.assertRaises(PublicStreamError) as caught:
            subscription.wait_available(timeout=0)
        self.assertEqual("SUBSCRIPTION_CLOSED", caught.exception.code)
        with self.assertRaises(PublicStreamError) as caught:
            subscription.read_available()
        self.assertEqual("SUBSCRIPTION_CLOSED", caught.exception.code)
        with self.assertRaises(PublicStreamError) as caught:
            _ = subscription.cursor
        self.assertEqual("SUBSCRIPTION_CLOSED", caught.exception.code)
        self.assertFalse(subscription.connected)
        subscription.close()

    def test_invalid_subscribe_and_read_inputs_fail_closed(self) -> None:
        observer = PublicStreamObserver()
        for stream_id in ("", "   ", 7):
            with self.assertRaisesRegex(ValueError, "INVALID_STREAM_ID"):
                observer.subscribe(stream_id, after_sequence=0)
        for cursor in (-1, True, "0"):
            with self.assertRaisesRegex(ValueError, "INVALID_STREAM_CURSOR"):
                observer.subscribe("session-1", after_sequence=cursor)
        subscription = observer.subscribe("session-1", after_sequence=0)
        for limit in (0, True, "1"):
            with self.assertRaisesRegex(ValueError, "INVALID_READ_LIMIT"):
                subscription.read_available(max_events=limit)

    def test_conflicting_retained_event_identity_fails_closed(self) -> None:
        observer = PublicStreamObserver(
            retention_limit=8,
            subscriber_queue_limit=8,
        )
        subscription = observer.subscribe("session-1", after_sequence=0)
        observer.on_batch(batch(event("event-1", 1)))
        with self.assertRaisesRegex(
            RuntimeError, "PUBLIC_STREAM_EVENT_INTEGRITY"
        ):
            observer.on_batch(
                batch(
                    event(
                        "event-conflict",
                        1,
                        fields=(("question", "changed"),),
                    )
                )
            )
        self.assertEqual("event-1", subscription.read_available()[0].event_id)

    def test_public_event_is_immutable_and_configuration_is_bounded(self) -> None:
        projected = PublicStreamEvent(
            "session-1",
            "event-1",
            1,
            "state_changed",
            (),
        )
        with self.assertRaises(FrozenInstanceError):
            projected.sequence = 2
        invalid_events = (
            ("", "event-1", 1, "state_changed", ()),
            ("session-1", "", 1, "state_changed", ()),
            ("session-1", "event-1", True, "state_changed", ()),
            ("session-1", "event-1", 1, "", ()),
            ("session-1", "event-1", 1, "state_changed", []),
            (
                "session-1",
                "event-1",
                1,
                "state_changed",
                (("field", "a"), ("field", "b")),
            ),
        )
        for values in invalid_events:
            with self.assertRaisesRegex(
                ValueError, "INVALID_PUBLIC_STREAM_EVENT"
            ):
                PublicStreamEvent(*values)
        for values in ((0, 1), (1, 0), (True, 1)):
            with self.assertRaisesRegex(ValueError, "INVALID_STREAM_LIMIT"):
                PublicStreamObserver(
                    retention_limit=values[0],
                    subscriber_queue_limit=values[1],
                )

    def test_non_batch_input_is_rejected(self) -> None:
        observer = PublicStreamObserver()
        with self.assertRaisesRegex(ValueError, "INVALID_COMMITTED_BATCH"):
            observer.on_batch(object())


if __name__ == "__main__":
    unittest.main()
