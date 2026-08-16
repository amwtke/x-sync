from __future__ import annotations

import json
import unittest
from dataclasses import replace

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.domain import (
    CommittedDialogueEvent,
    DecisionContext,
    EvidenceCheck,
    EvidenceHealth,
    PresentCandidates,
    ReportWorkFailure,
    RequestTopicClarification,
    SelectTopic,
    SessionStarted,
    StartSession,
    TriggerBinding,
    TriggerKind,
    WorkFailureCategory,
    initial_dialogue_state,
)
from xsync_v2.event_codec import sha256_digest
from xsync_v2.host_result import (
    MAX_HOST_RESULT_BYTES,
    DialogueTurnResult,
    HostResultError,
    TopicCandidatesResult,
    TopicClarificationResult,
    TopicStartedResult,
    WorkFailureResult,
    decode_host_result,
    encode_host_result,
    host_result_command,
)
from xsync_v2.state_machine import Accepted, decide, reduce
from xsync_v2.work import WorkOrigin, derive_runnable_work
from tests.test_xsync_v2_state_machine import agent_turn, contract


def digest(label: str) -> str:
    return sha256_digest(label.encode())


def candidate_work():
    state = initial_dialogue_state("session-1", 1)
    trigger = TriggerBinding(
        TriggerKind.TOPIC_CANDIDATES,
        "trigger-work-1",
        "runtime-1",
        None,
        None,
        digest("input"),
        digest("evidence"),
    )
    decision = decide(
        state,
        StartSession("command-1"),
        DecisionContext(
            1,
            trigger,
            EvidenceCheck(EvidenceHealth.CURRENT, digest("evidence")),
        ),
    )
    assert isinstance(decision, Accepted)
    payload = decision.events[0].payload
    assert isinstance(payload, SessionStarted)
    event = CommittedDialogueEvent("event-1", 1, 0, 1, "command-1", payload)
    state = reduce(state, event)
    projected = derive_runnable_work(
        state,
        WorkOrigin(event.event_id, event.sequence, trigger),
    )
    assert projected is not None
    return projected


def selection_work():
    state = initial_dialogue_state("session-selection", 1)
    candidate_trigger = TriggerBinding(
        TriggerKind.TOPIC_CANDIDATES,
        "candidate-token",
        "runtime-1",
        None,
        None,
        digest("candidate-input"),
        digest("evidence"),
    )
    started = decide(
        state,
        StartSession("start-selection"),
        DecisionContext(
            1,
            candidate_trigger,
            EvidenceCheck(EvidenceHealth.CURRENT, digest("evidence")),
        ),
    )
    assert isinstance(started, Accepted)
    start_event = CommittedDialogueEvent(
        "event-selection-1",
        1,
        0,
        1,
        "start-selection",
        started.events[0].payload,
    )
    state = reduce(state, start_event)
    candidates = decide(
        state,
        PresentCandidates("present-selection", ("Registry fencing",)),
        DecisionContext(
            1,
            candidate_trigger,
            EvidenceCheck(EvidenceHealth.CURRENT, digest("evidence")),
        ),
    )
    assert isinstance(candidates, Accepted)
    candidates_event = CommittedDialogueEvent(
        "event-selection-2",
        2,
        1,
        2,
        "present-selection",
        candidates.events[0].payload,
    )
    state = reduce(state, candidates_event)
    selection_trigger = TriggerBinding(
        TriggerKind.TOPIC_SELECTION,
        "selection-token",
        "runtime-1",
        None,
        None,
        digest("selection-input"),
        digest("evidence"),
    )
    selected = decide(
        state,
        SelectTopic("select-topic", "Registry fencing"),
        DecisionContext(
            1,
            selection_trigger,
            EvidenceCheck(EvidenceHealth.CURRENT, digest("evidence")),
        ),
    )
    assert isinstance(selected, Accepted)
    selected_event = CommittedDialogueEvent(
        "event-selection-3",
        3,
        2,
        3,
        "select-topic",
        selected.events[0].payload,
    )
    state = reduce(state, selected_event)
    projected = derive_runnable_work(
        state,
        WorkOrigin(selected_event.event_id, selected_event.sequence, selection_trigger),
    )
    assert projected is not None
    return projected


class HostResultCodecTest(unittest.TestCase):
    def test_round_trips_each_closed_result_without_host_specific_fields(self) -> None:
        values = (
            TopicCandidatesResult(("Registry fencing", "Lease recovery")),
            TopicClarificationResult(
                "clarification-1",
                "Which boundary should we examine first?",
            ),
            TopicStartedResult(contract()),
            DialogueTurnResult(agent_turn()),
            WorkFailureResult(
                WorkFailureCategory.HOST_TRANSIENT,
                "HOST_TIMEOUT",
                digest("timeout-proof"),
            ),
        )

        for value in values:
            with self.subTest(result=type(value).__name__):
                raw = encode_host_result(value)
                self.assertEqual(value, decode_host_result(raw))
                self.assertNotIn(b"codex", raw.lower())
                self.assertNotIn(b"claude", raw.lower())

    def test_rejects_duplicate_unknown_oversized_and_runtime_only_failure(self) -> None:
        invalid = (
            b'{"type":"topic_candidates","type":"dialogue_turn",'
            b'"candidates":["A"]}',
            b'{"type":"topic_candidates","candidates":["A"],"secret":"x"}',
            b'{"type":"unknown"}',
            b'{"type":"work_failure","category":"lease_attempts_exhausted",'
            b'"safe_error_code":"LEASE_LOST","proof_digest":"'
            + digest("proof").encode()
            + b'"}',
            b"{" + b" " * MAX_HOST_RESULT_BYTES + b"}",
        )
        for raw in invalid:
            with self.subTest(raw=raw[:64]):
                with self.assertRaises(HostResultError):
                    decode_host_result(raw)

    def test_candidates_are_bounded_nonblank_and_unique(self) -> None:
        invalid_candidates = (
            [],
            [""],
            ["A", "A"],
            ["A", "B", "C", "D", "E"],
        )
        for candidates in invalid_candidates:
            raw = (
                '{"type":"topic_candidates","candidates":'
                + json.dumps(candidates, ensure_ascii=False)
                + "}"
            ).encode()
            with self.subTest(candidates=candidates):
                with self.assertRaisesRegex(
                    HostResultError,
                    "HOST_RESULT_SCHEMA_INVALID",
                ):
                    decode_host_result(raw)

    def test_maps_validated_result_to_existing_domain_command(self) -> None:
        work = candidate_work()

        command = host_result_command(
            TopicCandidatesResult(("Registry fencing",)),
            idempotency_key="publish-1",
            work=work,
        )

        self.assertIs(type(command), PresentCandidates)
        self.assertEqual(("Registry fencing",), command.candidates)

        clarification = host_result_command(
            TopicClarificationResult(
                "clarification-1",
                "Which boundary should we examine first?",
            ),
            idempotency_key="clarify-1",
            work=selection_work(),
            selected_candidate="Registry fencing",
        )
        self.assertIs(type(clarification), RequestTopicClarification)
        assert isinstance(clarification, RequestTopicClarification)
        self.assertEqual("clarification-1", clarification.question_id)

    def test_rejects_result_for_the_wrong_durable_work_kind(self) -> None:
        work = candidate_work()

        with self.assertRaisesRegex(
            HostResultError,
            "HOST_RESULT_WORK_MISMATCH",
        ):
            host_result_command(
                DialogueTurnResult(agent_turn()),
                idempotency_key="publish-2",
                work=work,
            )

    def test_host_failure_identity_is_stable_and_bound_to_result(self) -> None:
        work = candidate_work()
        first_result = WorkFailureResult(
            WorkFailureCategory.HOST_TRANSIENT,
            "HOST_TIMEOUT",
            digest("proof-1"),
        )
        first = host_result_command(
            first_result,
            idempotency_key="failure-1",
            work=work,
        )
        replay = host_result_command(
            first_result,
            idempotency_key="failure-1",
            work=work,
        )
        changed = host_result_command(
            replace(first_result, proof_digest=digest("proof-2")),
            idempotency_key="failure-1",
            work=work,
        )

        self.assertIs(type(first), ReportWorkFailure)
        self.assertEqual(first, replay)
        self.assertNotEqual(first.failure.failure_id, changed.failure.failure_id)


if __name__ == "__main__":
    unittest.main()
