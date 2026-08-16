import hashlib
import unittest
from collections.abc import Callable
from dataclasses import replace

import tests.xsync_v2_path  # noqa: F401

# isort: split
from xsync_v2.domain import (
    ConversationPhase,
    CurrentWorkState,
    DialogueState,
    SessionLifecycle,
    TriggerBinding,
    TriggerKind,
    WorkFailure,
    WorkFailureCategory,
    WorkStatus,
    initial_dialogue_state,
)
from xsync_v2.work import (
    WorkError,
    WorkOrigin,
    derive_runnable_work,
    derive_work_id,
    validate_runnable_work,
)
from xsync_v2.work_identity import (
    WorkIdentityError,
    derive_canonical_work_id,
    is_canonical_trigger_binding,
    is_optional_protocol_id,
    is_optional_sha256_digest,
    is_protocol_id,
    is_sha256_digest,
)


def digest(label: str) -> str:
    return f"sha256:{hashlib.sha256(label.encode()).hexdigest()}"


def canonical_trigger(
    *,
    event_id: str = "event-trigger-1",
    event_sequence: int = 1,
    state_sequence: int = 7,
    conversation_version: int = 4,
    input_digest: str | None = None,
) -> tuple[DialogueState, WorkOrigin]:
    provisional = TriggerBinding(
        TriggerKind.TOPIC_CANDIDATES,
        "trigger-token-1",
        "epoch-1",
        None,
        None,
        input_digest or digest("input"),
        digest("evidence"),
    )
    provisional_origin = WorkOrigin(
        event_id,
        event_sequence,
        provisional,
    )
    binding = provisional
    origin = replace(provisional_origin, event_trigger=binding)
    current_work = CurrentWorkState(
        derive_canonical_work_id(
            session_id="dlg-1",
            trigger_event_id=event_id,
            trigger_event_sequence=event_sequence,
            trigger=binding,
        ),
        event_id,
        event_sequence,
        binding,
        1,
        WorkStatus.QUEUED,
    )
    state = DialogueState(
        "dlg-1",
        3,
        state_sequence,
        conversation_version,
        SessionLifecycle.OPEN,
        ConversationPhase.WAITING_HOST,
        (),
        current_work,
        None,
        (),
    )
    return state, origin


class WorkProjectionTest(unittest.TestCase):
    def assert_error(self, code: str, callback: Callable[[], object]) -> None:
        with self.assertRaises(WorkError) as raised:
            callback()
        self.assertEqual(code, raised.exception.code)

    def test_canonical_identity_primitives_and_trigger_are_exact(self) -> None:
        state, origin = canonical_trigger()
        trigger = origin.event_trigger
        self.assertTrue(is_protocol_id("work.valid-1"))
        self.assertTrue(is_optional_protocol_id(None))
        self.assertFalse(is_protocol_id("../escape"))
        self.assertFalse(is_protocol_id("x" * 129))
        self.assertFalse(is_protocol_id(True))
        self.assertTrue(is_sha256_digest(digest("value")))
        self.assertTrue(is_optional_sha256_digest(None))
        self.assertFalse(is_sha256_digest("sha256:not-a-digest"))
        self.assertFalse(is_sha256_digest("sha256:" + "A" * 64))
        self.assertTrue(is_canonical_trigger_binding(trigger))
        self.assertFalse(
            is_canonical_trigger_binding(replace(trigger, work_id="../escape"))
        )
        self.assertFalse(
            is_canonical_trigger_binding(
                replace(trigger, input_digest="sha256:not-a-digest")
            )
        )
        self.assertIsNotNone(state.session_work)

    def test_non_waiting_state_has_no_runnable_work(self) -> None:
        state = initial_dialogue_state("dlg-1", 3)

        self.assertIsNone(derive_runnable_work(state, None))
        self.assert_error(
            "WORK_ORIGIN_UNEXPECTED",
            lambda: derive_runnable_work(
                state,
                WorkOrigin(
                    "event-trigger-1",
                    1,
                    TriggerBinding(
                        TriggerKind.TOPIC_CANDIDATES,
                        "placeholder",
                        "epoch-1",
                        None,
                        None,
                        digest("input"),
                        digest("evidence"),
                    ),
                ),
            ),
        )

    def test_projection_binds_event_and_publish_guards(self) -> None:
        state, origin = canonical_trigger()

        work = derive_runnable_work(state, origin)

        self.assertIsNotNone(work)
        assert work is not None
        self.assertEqual("dlg-1", work.session_id)
        self.assertEqual(3, work.registry_generation)
        self.assertEqual(4, work.conversation_version)
        self.assertEqual(7, work.observed_sequence)
        self.assertEqual("event-trigger-1", work.trigger_event_id)
        self.assertEqual(1, work.trigger_event_sequence)
        self.assertIsNone(work.topic_run_id)
        self.assertEqual(TriggerKind.TOPIC_CANDIDATES, work.kind)
        self.assertEqual(origin.event_trigger.work_id, work.trigger_work_id)
        self.assertRegex(work.work_id, r"^work-[0-9a-f]{64}$")
        self.assertEqual(
            "work-86a4ede474a82daaaf861a69ea2d275d28effe24d974ce5098bcb0affa13b585",
            work.work_id,
        )
        self.assertEqual(
            work.work_id,
            derive_work_id(
                session_id=state.session_id,
                origin=origin,
                trigger=origin.event_trigger,
            ),
        )
        self.assertEqual("epoch-1", work.trigger_runtime_epoch)
        self.assertEqual(digest("input"), work.input_digest)
        self.assertEqual(digest("evidence"), work.evidence_digest)
        self.assertRegex(work.binding_digest, r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(work, validate_runnable_work(work))

    def test_version_neutral_cursor_does_not_change_work_identity(self) -> None:
        state, origin = canonical_trigger()
        advanced_state = replace(state, sequence=8)

        first = derive_runnable_work(state, origin)
        advanced = derive_runnable_work(advanced_state, origin)

        assert first is not None and advanced is not None
        self.assertEqual(first.work_id, advanced.work_id)
        self.assertEqual(first.binding_digest, advanced.binding_digest)
        self.assertNotEqual(first.observed_sequence, advanced.observed_sequence)

    def test_only_queued_canonical_work_is_projected(self) -> None:
        state, origin = canonical_trigger()
        assert state.session_work is not None
        failed = replace(
            state,
            session_work=replace(
                state.session_work,
                status=WorkStatus.FAILED,
                last_failure=WorkFailure(
                    "failure-1",
                    WorkFailureCategory.HOST_TRANSIENT,
                    "HOST_TIMEOUT",
                    digest("proof"),
                ),
            ),
        )

        self.assertIsNone(derive_runnable_work(failed, None))
        self.assert_error(
            "WORK_ORIGIN_UNEXPECTED",
            lambda: derive_runnable_work(failed, origin),
        )

    def test_new_trigger_event_gets_new_normative_work_identity(self) -> None:
        first_state, first_origin = canonical_trigger()
        next_state, next_origin = canonical_trigger(
            event_id="event-trigger-2",
            event_sequence=8,
            state_sequence=8,
        )

        first = derive_runnable_work(first_state, first_origin)
        second = derive_runnable_work(next_state, next_origin)

        assert first is not None and second is not None
        self.assertNotEqual(first.work_id, second.work_id)
        self.assertNotEqual(first.binding_digest, second.binding_digest)

    def test_origin_must_prove_exact_current_trigger_payload(self) -> None:
        state, origin = canonical_trigger()
        mismatched = replace(
            origin,
            event_trigger=replace(
                origin.event_trigger,
                evidence_digest=digest("different-evidence"),
            ),
        )

        self.assert_error(
            "WORK_ORIGIN_TRIGGER_MISMATCH",
            lambda: derive_runnable_work(state, mismatched),
        )
        self.assert_error(
            "WORK_ID_INPUT_INVALID",
            lambda: derive_work_id(
                session_id=state.session_id,
                origin=mismatched,
                trigger=origin.event_trigger,
            ),
        )

    def test_arbitrary_work_id_and_non_sha_digests_are_rejected(self) -> None:
        state, origin = canonical_trigger()
        alternate_trigger = replace(
            origin.event_trigger,
            work_id="caller-chosen",
        )
        assert state.session_work is not None
        alternate = replace(
            state,
            session_work=replace(
                state.session_work,
                trigger=alternate_trigger,
            ),
        )
        alternate_work = derive_runnable_work(
            alternate,
            replace(origin, event_trigger=alternate_trigger),
        )
        original_work = derive_runnable_work(state, origin)
        assert original_work is not None and alternate_work is not None
        self.assertEqual(original_work.work_id, alternate_work.work_id)
        self.assertNotEqual(
            original_work.binding_digest,
            alternate_work.binding_digest,
        )

        self.assert_error(
            "RUNNABLE_WORK_INVALID",
            lambda: validate_runnable_work(
                replace(original_work, work_id="caller-chosen")
            ),
        )
        self.assert_error(
            "WORK_ID_INPUT_INVALID",
            lambda: derive_work_id(
                session_id=state.session_id,
                origin=None,  # type: ignore[arg-type]
                trigger=origin.event_trigger,
            ),
        )
        for invalid_work_id in (None, "bad/work"):
            with self.subTest(trigger_work_id=invalid_work_id):
                invalid_trigger_id = replace(
                    origin.event_trigger,
                    work_id=invalid_work_id,
                )
                self.assert_error(
                    "WORK_ID_INPUT_INVALID",
                    lambda invalid=invalid_trigger_id: derive_work_id(
                        session_id=state.session_id,
                        origin=replace(
                            origin,
                            event_trigger=invalid,
                        ),
                        trigger=invalid,
                    ),
                )
        self.assert_error(
            "RUNNABLE_WORK_INVALID",
            lambda: validate_runnable_work(
                replace(
                    original_work,
                    binding_digest=None,  # type: ignore[arg-type]
                )
            ),
        )

        invalid_trigger = replace(
            origin.event_trigger,
            input_digest="sha256:not-a-digest",
        )
        assert state.session_work is not None
        with self.assertRaises(WorkIdentityError):
            derive_canonical_work_id(
                session_id=state.session_id,
                trigger_event_id=origin.trigger_event_id,
                trigger_event_sequence=origin.trigger_event_sequence,
                trigger=invalid_trigger,
            )
        invalid_state = replace(
            state,
            session_work=replace(
                state.session_work,
                trigger=invalid_trigger,
            ),
        )
        invalid_origin = replace(origin, event_trigger=invalid_trigger)
        self.assert_error(
            "STATE_INVALID",
            lambda: derive_runnable_work(invalid_state, invalid_origin),
        )

    def test_invalid_state_or_origin_is_rejected_not_guessed(self) -> None:
        state, origin = canonical_trigger()
        invalid = replace(state, session_work=None)

        self.assert_error(
            "STATE_INVALID",
            lambda: derive_runnable_work(invalid, origin),
        )
        self.assert_error(
            "STATE_INVALID",
            lambda: derive_runnable_work(None, None),  # type: ignore[arg-type]
        )
        self.assert_error(
            "WORK_ORIGIN_INVALID",
            lambda: derive_runnable_work(
                state,
                replace(origin, trigger_event_sequence=8),
            ),
        )


if __name__ == "__main__":
    unittest.main()
