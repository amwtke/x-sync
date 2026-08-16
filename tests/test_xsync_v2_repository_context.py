from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

import tests.xsync_v2_path  # noqa: F401
from tests.test_xsync_v2_state_machine import agent_turn, contract
from xsync_v2.browser_service import (
    BrowserCommandRequest,
    RequestHelpIntent,
    SelectTopicIntent,
    SubmitTurnIntent,
    SwitchTopicIntent,
)
from xsync_v2.coordinator import DialogueSessionConfig
from xsync_v2.domain import (
    TriggerKind,
)
from xsync_v2.evidence import (
    EvidenceClaimType,
    EvidenceKind,
    EvidenceSource,
)
from xsync_v2.host_context import HostContextError
from xsync_v2.host_work import (
    HostResultPublishRequest,
    HostWorkServiceError,
)
from xsync_v2.host_result import (
    DialogueTurnResult,
    TopicCandidatesResult,
    TopicStartedResult,
)
from xsync_v2.event_codec import ActorKind, DialogueActor, sha256_digest
from xsync_v2.lease_store import ClaimRequest
from xsync_v2.runtime import DialogueRuntime


class RepositoryContextRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        base = Path(self.temporary.name).resolve()
        self.repository = base / "repository"
        self.repository.mkdir(mode=0o700)
        self.state = base / "state"
        self.state.mkdir(mode=0o700)
        self.git("init", "-q")
        self.git("config", "user.name", "X-Sync Test")
        self.git("config", "user.email", "x-sync@example.invalid")
        (self.repository / "architecture.md").write_text(
            "The registry fence precedes dialogue quiescence.\n"
            "A durable marker is the commit point.\n"
            "Appendix A.\n",
            encoding="utf-8",
        )
        self.git("add", ".")
        self.git("commit", "-qm", "add architecture evidence")
        self.now = 1_000
        self.runtime = DialogueRuntime(
            self.state,
            "registry-1",
            "runtime-1",
            repository_directory=self.repository,
            repository_id="repository-1",
            runtime_authority_verifier=lambda _check, _authority: True,
            lease_clock=lambda: self.now,
            browser_clock=lambda: "2026-08-16T17:00:00+08:00",
            monotonic_clock=lambda: float(self.now),
            durable_poll_interval=0.01,
        )
        self.addCleanup(self.runtime.close)
        self.snapshot = self.runtime.evidence.capture(
            "session-1",
            (
                EvidenceSource(
                    "ev.registry-fence",
                    EvidenceKind.ADR,
                    EvidenceClaimType.DECISION,
                    "Registry fencing happens before dialogue quiescence.",
                    "architecture.md",
                    1,
                    2,
                ),
            ),
            captured_at="2026-08-16T17:00:00+08:00",
        )
        self.config = DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repository-1",
            "2026-08-16T17:00:00+08:00",
            "runtime-1",
            self.snapshot.snapshot_digest,
            task_scope="Understand the registry handoff boundary",
        )
        self.runtime.resolve(self.config)

    def git(self, *arguments: str) -> str:
        return subprocess.run(
            ["git", "-C", str(self.repository), *arguments],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def claim(self):
        current = self.runtime.host.current("session-1")
        assert current is not None
        return self.runtime.host.claim(
            ClaimRequest(
                "session-1",
                "claim-request-1",
                "claim-1",
                current.work_id,
                "owner-1",
                30,
                120,
            )
        )

    def claim_current(self, suffix: str):
        current = self.runtime.host.current("session-1")
        assert current is not None
        return self.runtime.host.claim(
            ClaimRequest(
                "session-1",
                f"claim-request-{suffix}",
                f"claim-{suffix}",
                current.work_id,
                "owner-1",
                30,
                120,
            )
        )

    def publish_candidates(self, envelope, suffix: str = ""):
        work = envelope.work
        key = f"publish-candidates{suffix}"
        return self.runtime.host.publish_result(
            HostResultPublishRequest(
                key,
                work,
                TopicCandidatesResult(
                    ("Registry handoff", "Lease fencing"),
                ),
                self.config.created_at,
                DialogueActor(ActorKind.HOST, "host.repository-context"),
                envelope.fence,
            )
        )

    def test_runtime_builds_host_context_from_the_same_snapshot_and_state(self) -> None:
        envelope = self.claim()

        self.assertEqual(
            self.snapshot.snapshot_digest,
            envelope.context.evidence_digest,
        )
        self.assertEqual(self.config.task_scope, envelope.context.task_scope)
        self.assertEqual(1, len(envelope.context.evidence_claims))
        claim = envelope.context.evidence_claims[0]
        self.assertEqual("ev.registry-fence", claim.evidence_id)
        self.assertEqual("architecture.md:1-2", claim.location)
        self.assertEqual(self.snapshot.entries[0].content_hash, claim.content_hash)

    def test_dirty_text_outside_the_cited_range_remains_publishable(self) -> None:
        (self.repository / "architecture.md").write_text(
            "The registry fence precedes dialogue quiescence.\n"
            "A durable marker is the commit point.\n"
            "Changed appendix outside the cited range.\n",
            encoding="utf-8",
        )

        envelope = self.claim()

        self.assertEqual(
            "ev.registry-fence",
            envelope.context.evidence_claims[0].evidence_id,
        )

    def test_changed_cited_text_blocks_context_after_the_lease_commit(self) -> None:
        (self.repository / "architecture.md").write_text(
            "Dialogue quiescence precedes the registry fence.\n"
            "A durable marker is the commit point.\n"
            "Appendix A.\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            HostContextError,
            "HOST_CONTEXT_EVIDENCE_UNAVAILABLE",
        ):
            self.claim()

    def test_selection_and_active_topic_context_are_derived_not_invented(self) -> None:
        candidates = self.publish_candidates(self.claim_current("candidates"))
        selected = self.runtime.browser.execute(
            BrowserCommandRequest(
                "session-1",
                "select-topic-1",
                candidates.state.conversation_version,
                SelectTopicIntent("Registry handoff"),
                "learner-1",
            )
        )
        selection = self.claim_current("selection")
        self.assertEqual("Registry handoff", selection.context.selected_candidate)

        topic_contract = replace(
            contract(),
            title="Registry handoff",
            evidence_refs=("ev.registry-fence",),
            contract_digest=sha256_digest(b"registry-contract"),
        )
        self.runtime.host.publish_result(
            HostResultPublishRequest(
                "start-registry-topic",
                selection.work,
                TopicStartedResult(topic_contract),
                self.config.created_at,
                DialogueActor(ActorKind.HOST, "host.repository-context"),
                selection.fence,
            )
        )

        opening = self.claim_current("opening")

        self.assertEqual(topic_contract, opening.context.topic_contract)
        self.assertEqual(topic_contract.task_scope.summary, opening.context.task_scope)
        self.assertIsNone(opening.context.selected_candidate)
        self.assertIn("mechanism", opening.context.priority_gap)
        self.assertEqual(selected.state.sequence + 1, opening.work.observed_sequence)

        base_result = agent_turn(with_model=False)
        result = replace(
            base_result,
            evidence_refs=("ev.registry-fence",),
        )
        answered = self.runtime.host.publish_result(
            HostResultPublishRequest(
                "publish-opening-question",
                opening.work,
                DialogueTurnResult(result),
                self.config.created_at,
                DialogueActor(ActorKind.HOST, "host.repository-context"),
                opening.fence,
            )
        )
        requested_help = self.runtime.browser.execute(
            BrowserCommandRequest(
                "session-1",
                "help-question-1",
                answered.state.conversation_version,
                RequestHelpIntent(result.question_id),
                "learner-1",
            )
        )
        help_work = self.claim_current("help")
        self.assertIs(TriggerKind.HELP, help_work.work.kind)
        self.assertEqual(result.question, help_work.context.previous_question)
        self.assertIn("smallest useful hint", help_work.context.priority_gap)
        helped_result = replace(
            agent_turn("q-after-help"),
            heard="A small hint: inspect which marker wins the race.",
            evidence_refs=("ev.registry-fence",),
        )
        helped = self.runtime.host.publish_result(
            HostResultPublishRequest(
                "publish-helped-question",
                help_work.work,
                DialogueTurnResult(helped_result),
                self.config.created_at,
                DialogueActor(ActorKind.HOST, "host.repository-context"),
                help_work.fence,
            )
        )
        self.assertGreater(helped.state.sequence, requested_help.state.sequence)
        submitted = self.runtime.browser.execute(
            BrowserCommandRequest(
                "session-1",
                "answer-question-1",
                helped.state.conversation_version,
                SubmitTurnIntent(
                    helped_result.question_id,
                    "The registry fence wins first.",
                ),
                "learner-1",
            )
        )

        learner_reply = self.claim_current("learner-reply")

        self.assertEqual(
            helped_result.question,
            learner_reply.context.previous_question,
        )
        self.assertIsNotNone(learner_reply.context.learner_turn)
        assert learner_reply.context.learner_turn is not None
        self.assertEqual(
            "The registry fence wins first.",
            learner_reply.context.learner_turn.text,
        )
        self.assertEqual(
            result.learner_model_delta,
            learner_reply.context.learner_model,
        )
        self.assertEqual(submitted.state.sequence, learner_reply.work.observed_sequence)

        switched = self.runtime.browser.execute(
            BrowserCommandRequest(
                "session-1",
                "switch-topic-1",
                submitted.state.conversation_version,
                SwitchTopicIntent(),
                "learner-1",
            )
        )
        self.assertIsNone(switched.state.active_topic)
        self.assertEqual(1, len(switched.state.paused_topics))
        late_result = replace(
            agent_turn("q2"),
            evidence_refs=("ev.registry-fence",),
        )
        with self.assertRaisesRegex(HostWorkServiceError, "WORK_SUPERSEDED"):
            self.runtime.host.publish_result(
                HostResultPublishRequest(
                    "late-old-topic-result",
                    learner_reply.work,
                    DialogueTurnResult(late_result),
                    self.config.created_at,
                    DialogueActor(ActorKind.HOST, "host.repository-context"),
                    learner_reply.fence,
                )
            )
        switch_candidates = self.claim_current("switch-candidates")
        self.assertIs(TriggerKind.TOPIC_CANDIDATES, switch_candidates.work.kind)
        self.assertIsNone(switch_candidates.context.topic_contract)

        presented = self.publish_candidates(switch_candidates, "-after-switch")
        self.assertEqual("choosing_topic", presented.state.phase.value)
        self.assertEqual(1, len(presented.state.paused_topics))


if __name__ == "__main__":
    unittest.main()
