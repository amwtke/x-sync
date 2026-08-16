# ruff: noqa: RUF001

import hashlib
import unittest
from dataclasses import FrozenInstanceError

import tests.xsync_v2_path  # noqa: F401

# isort: split
from xsync_v2.domain import (
    ConversationPhase,
    CurrentWorkState,
    GateAssessment,
    GateId,
    GateRequirement,
    GateStatus,
    Lens,
    SessionLifecycle,
    TaskScope,
    TopicContract,
    TriggerBinding,
    TriggerKind,
    WorkFailure,
    WorkFailureCategory,
    WorkRecoveryAction,
    WorkStatus,
    initial_dialogue_state,
)


def digest(label: str) -> str:
    return f"sha256:{hashlib.sha256(label.encode()).hexdigest()}"


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
            contract_digest=digest("contract"),
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

    def test_initial_state_rejects_invalid_envelope_values(self):
        invalid = (
            ("", 1),
            ("dlg-1", 0),
            ("dlg-1", -1),
            ("dlg-1", True),
            (None, 1),
        )
        for session_id, generation in invalid:
            with self.subTest(
                session_id=session_id, generation=generation
            ), self.assertRaisesRegex(
                ValueError,
                "INVALID_DIALOGUE_ENVELOPE",
            ):
                initial_dialogue_state(session_id, generation)

    def test_canonical_work_vocabulary_is_typed_and_frozen(self):
        trigger = TriggerBinding(
            TriggerKind.TOPIC_CANDIDATES,
            "trigger-1",
            "epoch-1",
            None,
            None,
            digest("input"),
            digest("evidence"),
        )
        failure = WorkFailure(
            "failure-1",
            WorkFailureCategory.HOST_TRANSIENT,
            "HOST_TIMEOUT",
            digest("proof"),
        )
        work = CurrentWorkState(
            "work-1",
            "event-1",
            1,
            trigger,
            2,
            WorkStatus.FAILED,
            failure,
            (WorkRecoveryAction.RETRY,),
        )

        self.assertEqual(2, work.attempt)
        self.assertEqual(WorkStatus.FAILED, work.status)
        with self.assertRaises(FrozenInstanceError):
            work.status = WorkStatus.QUEUED


if __name__ == "__main__":
    unittest.main()
