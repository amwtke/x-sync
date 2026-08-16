import unittest
from dataclasses import FrozenInstanceError

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


def event(
    event_id="e1",
    sequence=1,
    fields=(("topic_run_id", "topic-1"),),
):
    return CommittedEventView(
        event_id=event_id,
        sequence=sequence,
        payload=ImmutablePayloadView(
            tag="topic_paused",
            fields=fields,
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

    def test_callback_error_diagnostic_never_renders_untrusted_exception(self):
        class UnrenderableError(RuntimeError):
            def __str__(self):
                raise AssertionError("exception rendering was invoked")

        class UnrenderableObserver(Recorder):
            def on_batch(self, batch):
                self.calls.append(
                    (self.name, tuple(event.event_id for event in batch.events))
                )
                raise UnrenderableError("private observer detail")

        calls = []
        hub = ObserverHub(NoLocks())
        hub.register_fixed(UnrenderableObserver("a-fail", calls))
        hub.register_fixed(Recorder("z-good", calls))
        hub.freeze()

        report = hub.publish(batch())

        self.assertEqual(("a-fail", "z-good"), report.attempted)
        self.assertEqual(("a-fail",), report.failed)
        self.assertEqual(
            (("a-fail", "OBSERVER_CALLBACK_FAILED"),),
            report.errors,
        )
        self.assertEqual(
            [("a-fail", ("e1",)), ("z-good", ("e1",))],
            calls,
        )

    def test_callback_baseexceptions_are_isolated_from_healthy_observers(self):
        class BaseExceptionObserver(Recorder):
            def __init__(self, name, calls, error):
                super().__init__(name, calls)
                self.error = error

            def on_batch(self, batch):
                self.calls.append(
                    (self.name, tuple(event.event_id for event in batch.events))
                )
                raise self.error

        calls = []
        hub = ObserverHub(NoLocks())
        hub.register_fixed(
            BaseExceptionObserver(
                "a-keyboard-interrupt",
                calls,
                KeyboardInterrupt("private keyboard detail"),
            )
        )
        hub.register_fixed(
            BaseExceptionObserver(
                "b-system-exit",
                calls,
                SystemExit("private system detail"),
            )
        )
        hub.register_fixed(Recorder("z-good", calls))
        hub.freeze()

        report = hub.publish(batch())

        self.assertEqual(
            ("a-keyboard-interrupt", "b-system-exit", "z-good"),
            report.attempted,
        )
        self.assertEqual(
            ("a-keyboard-interrupt", "b-system-exit"),
            report.failed,
        )
        self.assertEqual(
            (
                ("a-keyboard-interrupt", "OBSERVER_CALLBACK_FAILED"),
                ("b-system-exit", "OBSERVER_CALLBACK_FAILED"),
            ),
            report.errors,
        )
        self.assertEqual(
            [
                ("a-keyboard-interrupt", ("e1",)),
                ("b-system-exit", ("e1",)),
                ("z-good", ("e1",)),
            ],
            calls,
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

    def test_duplicate_identity_with_changed_payload_fails_closed_stably(self):
        calls = []
        hub = ObserverHub(NoLocks())
        hub.register_fixed(Recorder("recorder", calls))
        hub.freeze()
        hub.publish(batch(event("e1", 1)))
        conflicting = batch(
            event(
                "e1",
                1,
                fields=(("topic_run_id", "topic-corrupted"),),
            )
        )
        first_report = hub.publish(conflicting)
        retry_report = hub.publish(conflicting)
        for report in (first_report, retry_report):
            self.assertEqual(("recorder",), report.attempted)
            self.assertEqual(("recorder",), report.failed)
            self.assertEqual(
                (("recorder", "OBSERVER_EVENT_INTEGRITY"),),
                report.errors,
            )
        self.assertEqual([("recorder", ("e1",))], calls)

    def test_sequence_identity_conflict_preserves_failed_exact_retry(self):
        calls = []
        hub = ObserverHub(NoLocks())
        recorder = Recorder("recorder", calls, fail=True)
        hub.register_fixed(recorder)
        hub.freeze()
        original = batch(event("e1", 1))
        first_report = hub.publish(original)
        self.assertEqual(("recorder",), first_report.failed)
        self.assertEqual([("recorder", ("e1",))], calls)

        conflict_report = hub.publish(batch(event("e-conflict", 1)))
        self.assertEqual(("recorder",), conflict_report.attempted)
        self.assertEqual(("recorder",), conflict_report.failed)
        self.assertEqual(
            (("recorder", "OBSERVER_EVENT_INTEGRITY"),),
            conflict_report.errors,
        )
        self.assertEqual([("recorder", ("e1",))], calls)

        recorder.fail = False
        retry_report = hub.publish(original)
        self.assertEqual(("recorder",), retry_report.attempted)
        self.assertEqual((), retry_report.failed)
        self.assertEqual((), retry_report.errors)
        next_report = hub.publish(batch(event("e2", 2)))
        self.assertEqual(("recorder",), next_report.attempted)
        self.assertEqual((), next_report.failed)
        self.assertEqual(
            [
                ("recorder", ("e1",)),
                ("recorder", ("e1",)),
                ("recorder", ("e2",)),
            ],
            calls,
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

    def test_dto_subclasses_with_mutable_extras_are_rejected(self):
        class InheritedPayload(ImmutablePayloadView):
            pass

        class MutablePayload(ImmutablePayloadView):
            def __post_init__(self):
                object.__setattr__(self, "extra", [])

        class MutableEvent(CommittedEventView):
            def __post_init__(self):
                object.__setattr__(self, "extra", [])

        class MutableBatch(CommittedBatch):
            def __post_init__(self):
                object.__setattr__(self, "extra", [])

        with self.assertRaisesRegex(ValueError, "MUTABLE_PAYLOAD_VIEW"):
            InheritedPayload("topic_paused", ())
        mutable_payload = MutablePayload("topic_paused", ())
        with self.assertRaisesRegex(ValueError, "INVALID_EVENT_VIEW"):
            CommittedEventView("e1", 1, mutable_payload)
        mutable_event = MutableEvent(
            "e1",
            1,
            ImmutablePayloadView("topic_paused", ()),
        )
        with self.assertRaisesRegex(ValueError, "INVALID_COMMITTED_BATCH"):
            CommittedBatch(
                StreamKind.DIALOGUE,
                "dlg-1",
                (mutable_event,),
            )
        mutable_batch = MutableBatch(
            StreamKind.DIALOGUE,
            "dlg-1",
            (event(),),
        )
        hub = ObserverHub(NoLocks())
        hub.register_fixed(Recorder("recorder", []))
        hub.freeze()
        with self.assertRaisesRegex(ValueError, "INVALID_COMMITTED_BATCH"):
            hub.publish(mutable_batch)

    def test_frozen_registry_ignores_mutated_observer_identity(self):
        class ExplodingStreams:
            def __contains__(self, item):
                raise RuntimeError(f"unexpected stream lookup: {item}")

        calls = []
        hub = ObserverHub(NoLocks())
        unstable = Recorder("a-fixed", calls, fail=True)
        hub.register_fixed(unstable)
        hub.register_fixed(Recorder("z-good", calls))
        hub.freeze()
        unstable.name = "z-mutated"
        unstable.accepted_streams = ExplodingStreams()
        report = hub.publish(batch())
        self.assertEqual(("a-fixed", "z-good"), report.attempted)
        self.assertEqual(("a-fixed",), report.failed)
        self.assertEqual(
            [("z-mutated", ("e1",)), ("z-good", ("e1",))],
            calls,
        )

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
