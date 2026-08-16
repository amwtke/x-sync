import json
import unittest
from dataclasses import FrozenInstanceError, replace

import tests.xsync_v2_path  # noqa: F401
from tests.test_xsync_v2_state_machine import agent_turn, apply, contract
from tests.test_xsync_v2_work import canonical_trigger
from xsync_v2.domain import (
    CommitAgentTurn,
    GateAssessment,
    GateId,
    GateStatus,
    Lens,
    PresentCandidates,
    StartSession,
    StartTopic,
    SubmitLearnerTurn,
    initial_dialogue_state,
)
from xsync_v2.event_codec import sha256_digest
from xsync_v2.host_context import (
    MAX_HOST_CONTEXT_BYTES,
    EvidenceContextClaim,
    HostContextError,
    HostContextSource,
    LearnerTurnContext,
    build_host_context,
    encode_host_context,
)
from xsync_v2.work import RunnableWork, WorkOrigin, derive_runnable_work


def digest(label: str) -> str:
    return sha256_digest(label.encode())


def work() -> RunnableWork:
    state = initial_dialogue_state("dlg-1", 1)
    state = apply(state, StartSession("command-1"))
    state = apply(state, PresentCandidates("command-2", ("支付一致性",)))
    state = apply(state, StartTopic("command-3", contract()))
    state = apply(state, CommitAgentTurn("command-4", agent_turn()))
    state = apply(
        state,
        SubmitLearnerTurn(
            "command-5",
            "q1",
            "learner-turn-1",
            "我的理解",
        ),
    )
    assert state.active_topic is not None
    assert state.active_topic.work is not None
    current = state.active_topic.work
    origin = WorkOrigin(
        current.trigger_event_id,
        current.trigger_event_sequence,
        current.trigger,
    )
    item = derive_runnable_work(state, origin)
    assert item is not None
    return item


def source(*, learner_text: str = "我的理解") -> HostContextSource:
    topic_contract = contract()
    models = tuple(
        replace(
            agent_turn(with_model=True).learner_model_delta[0],
            entry_id=f"model-{number}",
            statement=f"模型结论 {number}",
        )
        for number in range(8)
    )
    return HostContextSource(
        topic_contract=topic_contract,
        task_scope=topic_contract.task_scope.summary,
        current_lens=Lens.TECHNICAL,
        gates=(
            GateAssessment(GateId.MECHANISM, GateStatus.EMERGING),
            GateAssessment(GateId.BOUNDARY, GateStatus.UNEXPLORED),
            GateAssessment(
                GateId.REPOSITORY_APPLICATION,
                GateStatus.UNEXPLORED,
            ),
        ),
        previous_question="上一个问题?",
        learner_turn=LearnerTurnContext(
            learner_text,
            "dialogue://session-1/turn/learner-turn-1",
        ),
        learner_model=models,
        priority_gap="还没有解释租约失效边界",
        evidence_claims=tuple(
            EvidenceContextClaim(
                f"ev-{number}",
                f"证据结论 {number}",
                f"src/service.py:{number}",
                digest(f"evidence-{number}"),
            )
            for number in range(5)
        ),
        through_event_sequence=work().observed_sequence,
    )


class HostContextTest(unittest.TestCase):
    def test_capsule_is_typed_bounded_and_selects_only_current_context(self) -> None:
        capsule = build_host_context(work(), source())
        encoded = encode_host_context(capsule)
        decoded = json.loads(encoded)

        self.assertLessEqual(len(encoded), MAX_HOST_CONTEXT_BYTES)
        self.assertEqual(6, len(capsule.learner_model))
        self.assertEqual(3, len(capsule.evidence_claims))
        self.assertEqual("上一个问题?", capsule.previous_question)
        self.assertEqual("我的理解", capsule.learner_turn.text)
        self.assertEqual(work().observed_sequence, capsule.through_event_sequence)
        self.assertEqual(work().evidence_digest, capsule.evidence_digest)
        self.assertEqual(capsule.context_digest, decoded["context_digest"])
        self.assertEqual(capsule.learner_model_digest, decoded["learner_model_digest"])
        self.assertNotIn("transcript", decoded)
        self.assertNotIn("repository_scan", decoded)

    def test_session_candidate_context_has_task_lens_and_focused_evidence(
        self,
    ) -> None:
        state, origin = canonical_trigger()
        candidate_work = derive_runnable_work(state, origin)
        assert candidate_work is not None
        candidate_source = HostContextSource(
            topic_contract=None,
            task_scope="为支付仓库选择一个有价值的同步话题",
            current_lens=Lens.MIXED,
            gates=(),
            previous_question=None,
            learner_turn=None,
            learner_model=(),
            priority_gap="还没有选定话题",
            evidence_claims=(
                EvidenceContextClaim(
                    "ev-payment-spec",
                    "支付失败必须可恢复",
                    "docs/payment-spec.md:20",
                    digest("payment-spec"),
                ),
            ),
            through_event_sequence=candidate_work.observed_sequence,
        )

        capsule = build_host_context(candidate_work, candidate_source)

        self.assertIsNone(capsule.topic_contract)
        self.assertEqual(candidate_source.task_scope, capsule.task_scope)
        self.assertIs(Lens.MIXED, capsule.current_lens)
        self.assertEqual(1, len(capsule.evidence_claims))

    def test_oversized_learner_turn_is_utf8_safely_truncated_with_read_proof(
        self,
    ) -> None:
        original = "界" * 20_000
        capsule = build_host_context(work(), source(learner_text=original))
        encoded = encode_host_context(capsule)

        self.assertLessEqual(len(encoded), MAX_HOST_CONTEXT_BYTES)
        self.assertTrue(capsule.learner_turn.truncated)
        self.assertEqual(digest(original), capsule.learner_turn.original_sha256)
        self.assertEqual(
            "dialogue://session-1/turn/learner-turn-1",
            capsule.learner_turn.read_ref,
        )
        self.assertTrue(original.startswith(capsule.learner_turn.text))
        capsule.learner_turn.text.encode("utf-8")

    def test_oversize_without_read_ref_fails_instead_of_silent_truncation(self) -> None:
        item = source(learner_text="x" * 20_000)
        item = replace(
            item,
            learner_turn=LearnerTurnContext(item.learner_turn.text, None),
        )
        with self.assertRaises(HostContextError) as raised:
            build_host_context(work(), item)
        self.assertEqual("HOST_CONTEXT_READ_REF_REQUIRED", raised.exception.code)

    def test_non_learner_payload_overflow_and_invalid_types_fail_closed(self) -> None:
        with self.assertRaises(HostContextError) as raised:
            build_host_context(work(), replace(source(), priority_gap="x" * 30_000))
        self.assertEqual("HOST_CONTEXT_TOO_LARGE", raised.exception.code)

        with self.assertRaises(HostContextError) as raised:
            build_host_context(work(), replace(source(), through_event_sequence=True))
        self.assertEqual("HOST_CONTEXT_SOURCE_INVALID", raised.exception.code)

    def test_source_is_bound_to_claimed_work_and_nested_values_are_validated(
        self,
    ) -> None:
        item = work()
        invalid_sources = (
            replace(source(), through_event_sequence=item.observed_sequence + 1),
            replace(source(), task_scope="漂移的任务范围"),
            replace(source(), evidence_claims=()),
            replace(source(), topic_contract=None),
            replace(
                source(),
                topic_contract=replace(contract(), contract_digest=digest("other")),
            ),
            replace(
                source(),
                topic_contract=replace(contract(), objective=123),
            ),
            replace(
                source(),
                gates=(GateAssessment("bad", GateStatus.EMERGING),),
            ),
            replace(
                source(),
                learner_model=(
                    replace(
                        agent_turn(with_model=True).learner_model_delta[0],
                        kind="bad",
                    ),
                ),
            ),
        )
        for invalid in invalid_sources:
            with self.subTest(invalid=invalid):
                with self.assertRaises(HostContextError):
                    build_host_context(item, invalid)

    def test_digest_changes_with_semantic_content_and_capsule_is_immutable(
        self,
    ) -> None:
        first = build_host_context(work(), source())
        second = build_host_context(
            work(), replace(source(), priority_gap="不同缺口")
        )
        self.assertNotEqual(first.context_digest, second.context_digest)
        with self.assertRaises(FrozenInstanceError):
            first.priority_gap = "mutate"


if __name__ == "__main__":
    unittest.main()
