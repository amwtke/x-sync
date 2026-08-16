from dataclasses import replace
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.domain import (
    Accepted,
    CommitAgentTurn,
    CommittedDialogueEvent,
    FencedQuiesceContext,
    PauseCause,
    PauseTopic,
    PrepareSessionDeactivation,
    PresentCandidates,
    Rejected,
    SessionDeactivationPrepared,
    StartSession,
    StartTopic,
    TopicPaused,
    TriggerKind,
    initial_dialogue_state,
)
from xsync_v2.state_machine import decide, decide_fenced_quiesce, reduce

from tests.test_xsync_v2_state_machine import agent_turn, apply, context, contract


FENCE = FencedQuiesceContext("handoff-1", "dialogue-1", 1, 2)


def commit_pending(state, pending):
    return reduce(
        state,
        CommittedDialogueEvent(
            f"fence-event-{state.sequence + 1}",
            state.sequence + 1,
            state.conversation_version,
            state.conversation_version + 1,
            pending.command_id,
            pending.payload,
        ),
    )


class DialogueFencingTest(unittest.TestCase):
    def started(self):
        state = initial_dialogue_state("dialogue-1", 1)
        return apply(state, StartSession("command-start"))

    def active(self):
        state = self.started()
        state = apply(state, PresentCandidates("command-candidates", ("支付",)))
        state = apply(state, StartTopic("command-topic", contract()))
        return apply(state, CommitAgentTurn("command-agent", agent_turn()))

    def test_active_topic_is_paused_with_a_typed_fence_cause(self):
        state = self.active()
        decision = decide_fenced_quiesce(
            state,
            PrepareSessionDeactivation("command-fence"),
            FENCE,
        )
        self.assertIsInstance(decision, Accepted)
        assert isinstance(decision, Accepted)
        self.assertEqual(1, len(decision.events))
        payload = decision.events[0].payload
        self.assertEqual(
            TopicPaused(
                "topic-1",
                PauseCause.SESSION_DEACTIVATION,
                "handoff-1",
                2,
            ),
            payload,
        )
        state = commit_pending(state, decision.events[0])
        self.assertIsNone(state.active_topic)
        self.assertEqual("topic-1", state.paused_topics[-1].topic_run_id)

    def test_setup_state_is_prepared_and_pending_work_is_superseded(self):
        state = self.started()
        original_trigger = state.session_unresolved_trigger
        decision = decide_fenced_quiesce(
            state,
            PrepareSessionDeactivation("command-fence"),
            FENCE,
        )
        assert isinstance(decision, Accepted)
        self.assertEqual(
            SessionDeactivationPrepared("handoff-1", 2, original_trigger),
            decision.events[0].payload,
        )
        state = commit_pending(state, decision.events[0])
        self.assertEqual("none", state.phase)
        self.assertIsNone(state.session_unresolved_trigger)

        choosing = apply(
            self.started(), PresentCandidates("command-candidates", ("支付",))
        )
        decision = decide_fenced_quiesce(
            choosing,
            PrepareSessionDeactivation("command-fence-2"),
            FENCE,
        )
        assert isinstance(decision, Accepted)
        choosing = commit_pending(choosing, decision.events[0])
        self.assertEqual((), choosing.candidates)
        self.assertEqual("none", choosing.phase)

    def test_already_quiescent_state_needs_no_synthetic_event(self):
        state = self.active()
        state = apply(state, PauseTopic("command-pause"), context())
        decision = decide_fenced_quiesce(
            state,
            PrepareSessionDeactivation("command-fence"),
            FENCE,
        )
        self.assertEqual(Accepted(()), decision)

    def test_unstarted_current_session_can_be_quiesced_during_recovery(self):
        state = initial_dialogue_state("dialogue-1", 1)
        decision = decide_fenced_quiesce(
            state,
            PrepareSessionDeactivation("command-fence"),
            FENCE,
        )
        assert isinstance(decision, Accepted)
        self.assertEqual(1, len(decision.events))
        self.assertEqual(
            SessionDeactivationPrepared("handoff-1", 2, None),
            decision.events[0].payload,
        )
        state = commit_pending(state, decision.events[0])
        self.assertEqual("open", state.lifecycle)
        self.assertEqual("none", state.phase)
        self.assertEqual(1, state.registry_generation)

    def test_fence_is_the_only_entry_that_can_use_the_new_generation(self):
        state = self.active()
        ordinary = decide(
            state,
            PauseTopic("ordinary"),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        self.assertIsInstance(ordinary, Accepted)
        stale_context = replace(
            context(TriggerKind.TOPIC_CANDIDATES), registry_generation=2
        )
        self.assertEqual(
            Rejected("SESSION_DEACTIVATED"),
            decide(state, PauseTopic("ordinary"), stale_context),
        )
        self.assertEqual(
            Rejected("SESSION_DEACTIVATED"),
            decide_fenced_quiesce(
                state,
                PrepareSessionDeactivation("fence"),
                FencedQuiesceContext("handoff-1", "dialogue-1", 1, 3),
            ),
        )


if __name__ == "__main__":
    unittest.main()
