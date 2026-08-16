# ruff: noqa: RUF001

import json
import unittest
from dataclasses import replace
from enum import StrEnum

from xsync_v2.domain import (
    Accepted,
    AgentTurnResult,
    CommitAgentTurn,
    CommittedDialogueEvent,
    ConversationPhase,
    DecisionContext,
    EvidenceCheck,
    EvidenceHealth,
    FencedQuiesceContext,
    GateAssessment,
    GateId,
    GateRequirement,
    Lens,
    PauseTopic,
    PrepareSessionDeactivation,
    PresentCandidates,
    QuestionIntent,
    RecoverWork,
    ReportWorkFailure,
    ResumeTopic,
    SelectTopic,
    SessionLifecycle,
    SessionStarted,
    StartSession,
    StartTopic,
    SubmitLearnerTurn,
    SwitchTopic,
    TaskScope,
    TopicContract,
    TriggerBinding,
    TriggerKind,
    WorkDeadLettered,
    WorkFailed,
    WorkFailure,
    WorkFailureCategory,
    WorkRecoveryAction,
    WorkRecoveryRequested,
    WorkRequeued,
    initial_dialogue_state,
)
from xsync_v2.event_codec import (
    MAX_RECORD_BYTES,
    ActorKind,
    DialogueActor,
    DialogueCommandReceipt,
    DialogueWriteRequestRecord,
    build_state_snapshot,
    build_stored_event,
    build_transaction_marker,
    canonical_json_bytes,
    decode_state_snapshot,
    decode_stored_event,
    decode_transaction_marker,
    dialogue_request_digest,
    dialogue_state_digest,
    encode_state_snapshot,
    encode_stored_event,
    encode_transaction_marker,
    receipt_from_marker,
    sha256_digest,
)
from xsync_v2.state_machine import (
    conversation_version_delta,
    decide,
    decide_fenced_quiesce,
    reduce,
)

import tests.xsync_v2_path  # noqa: F401

REAL_DIGEST = "sha256:" + "1" * 64
ACTOR = DialogueActor(ActorKind.RUNTIME, "runtime.local")
OCCURRED_AT = "2026-08-16T12:00:00+08:00"


def digest(label: str) -> str:
    return sha256_digest(label.encode())


def contract():
    return TopicContract(
        contract_id="contract-1",
        contract_version=1,
        topic_run_id="topic-1",
        title="支付一致性",
        guiding_question="支付失败后如何恢复？",
        objective="说清失败边界与仓库落点",
        task_scope=TaskScope(
            "task-1", "支付失败补偿", ("payments/",), ("ui/",)
        ),
        starting_lens=Lens.MIXED,
        bridge_required=True,
        evidence_refs=("ev.spec",),
        gates=tuple(GateRequirement(item) for item in GateId),
        supersedes_contract_id=None,
        contract_digest=digest("contract"),
    )


def trigger(kind, work_id, parent=None):
    return TriggerBinding(
        kind=kind,
        work_id=work_id,
        runtime_epoch="epoch-1",
        parent_turn_id=parent,
        contract_digest=(
            None
            if kind
            in {TriggerKind.TOPIC_CANDIDATES, TriggerKind.TOPIC_SELECTION}
            else digest("contract")
        ),
        input_digest=digest(f"input:{work_id}"),
        evidence_digest=digest("evidence"),
    )


def context(kind=None, work_id="work-1", parent=None, input_digest=None):
    binding = None if kind is None else trigger(kind, work_id, parent)
    if binding is not None and input_digest is not None:
        binding = replace(binding, input_digest=input_digest)
    from xsync_v2.domain import DecisionContext

    return DecisionContext(
        registry_generation=1,
        trigger=binding,
        evidence=EvidenceCheck(EvidenceHealth.CURRENT, digest("evidence")),
    )


def agent_turn():
    return AgentTurnResult(
        heard="我听到你会先写业务状态。",
        one_step_further="还需要确认发送失败的边界。",
        question_id="q1",
        question="事件发送失败时，哪一层负责恢复？",
        question_intent=QuestionIntent.CAUSAL_TRACE,
        learner_model_delta=(),
        gate_assessments=tuple(GateAssessment(item) for item in GateId),
        evidence_refs=("ev.spec",),
    )


def failure(category=WorkFailureCategory.HOST_TRANSIENT):
    return WorkFailure(
        failure_id="failure-1",
        category=category,
        safe_error_code="HOST_TIMEOUT",
        proof_digest=digest("failure-proof"),
    )


def committed(state, pending):
    delta = conversation_version_delta(pending.payload)
    return CommittedDialogueEvent(
        event_id=f"event-{state.sequence + 1}",
        sequence=state.sequence + 1,
        from_version=state.conversation_version,
        to_version=state.conversation_version + delta,
        command_id=pending.command_id,
        payload=pending.payload,
    )


def failure_retry_sequence():
    state = initial_dialogue_state("dialogue-1", 1)
    state, started = advance(
        state,
        StartSession("command-start"),
        context(TriggerKind.TOPIC_CANDIDATES),
    )
    work = state.session_work
    assert work is not None
    next_trigger = replace(work.trigger, work_id="trigger-retry-2")
    decision = decide(
        state,
        ReportWorkFailure("command-failure", work.work_id, failure()),
        DecisionContext(
            registry_generation=1,
            trigger=next_trigger,
            evidence=EvidenceCheck(
                EvidenceHealth.CURRENT,
                work.trigger.evidence_digest,
            ),
        ),
    )
    assert isinstance(decision, Accepted), decision
    assert len(decision.events) == 2
    events = []
    for pending in decision.events:
        event = committed(state, pending)
        state = reduce(state, event)
        events.append(event)
    return state, started, tuple(events)


def advance(state, command, decision_context):
    decision = decide(state, command, decision_context)
    assert isinstance(decision, Accepted), decision
    event = committed(state, decision.events[0])
    return reduce(state, event), event


def event_sequence():
    state = initial_dialogue_state("dialogue-1", 1)
    events = []
    state, event = advance(
        state, StartSession("command-1"), context(TriggerKind.TOPIC_CANDIDATES)
    )
    events.append(event)
    state, event = advance(
        state,
        PresentCandidates("command-2", ("支付一致性",)),
        context(TriggerKind.TOPIC_CANDIDATES),
    )
    events.append(event)
    state, event = advance(
        state,
        StartTopic("command-3", contract()),
        context(TriggerKind.INITIAL_TURN),
    )
    events.append(event)
    state, event = advance(
        state,
        CommitAgentTurn("command-4", agent_turn()),
        context(TriggerKind.INITIAL_TURN),
    )
    events.append(event)
    state, event = advance(
        state,
        SubmitLearnerTurn("command-5", "q1", "turn-1", "先写状态。"),
        context(TriggerKind.LEARNER_REPLY, "work-reply", "turn-1"),
    )
    events.append(event)
    state, event = advance(state, PauseTopic("command-6"), context())
    events.append(event)
    saved = state.paused_topics[0].unresolved_trigger
    assert saved is not None
    state, event = advance(
        state,
        ResumeTopic("command-7", "topic-1"),
        context(
            TriggerKind.LEARNER_REPLY,
            "work-resumed",
            "turn-1",
            saved.input_digest,
        ),
    )
    events.append(event)
    return state, tuple(events)


def stored(event, previous=None):
    return build_stored_event(
        "dialogue-1",
        event,
        previous,
        registry_generation=1,
        occurred_at=OCCURRED_AT,
        actor=ACTOR,
        causation_id=None,
    )


class EventCodecTest(unittest.TestCase):
    def test_canonical_json_and_hash_have_golden_vectors(self):
        raw = canonical_json_bytes({"b": 2, "a": 1})
        self.assertEqual(b'{"a":1,"b":2}', raw)
        self.assertEqual(
            "sha256:43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777",
            sha256_digest(raw),
        )

    def test_canonical_json_accepts_only_the_exact_recursive_json_subset(self):
        class DictSubclass(dict):
            pass

        class ListSubclass(list):
            pass

        class StringSubclass(str):
            pass

        class IntegerSubclass(int):
            pass

        invalid_values = (
            ("tuple",),
            1.5,
            {1: "non-string key"},
            DictSubclass(ok=True),
            ListSubclass(("value",)),
            StringSubclass("value"),
            IntegerSubclass(1),
        )
        for value in invalid_values:
            with self.subTest(value=type(value).__name__):
                with self.assertRaisesRegex(
                    ValueError, "INVALID_CANONICAL_VALUE"
                ):
                    canonical_json_bytes(value)

        recursive = []
        recursive.append(recursive)
        with self.assertRaisesRegex(ValueError, "INVALID_CANONICAL_VALUE"):
            canonical_json_bytes(recursive)

        shared = ["safe"]
        self.assertEqual(b'[["safe"],["safe"]]', canonical_json_bytes([shared, shared]))

    def test_huge_json_numbers_and_deep_json_have_stable_errors(self):
        with self.assertRaisesRegex(ValueError, "INVALID_JSON_NUMBER"):
            canonical_json_bytes(10**300)
        with self.assertRaisesRegex(ValueError, "INVALID_JSON_NUMBER"):
            decode_stored_event(b"1" * 300)
        with self.assertRaisesRegex(ValueError, "INVALID_JSON_NUMBER"):
            decode_stored_event(b"1.0")

        deeply_nested = b"[" * 2_000 + b"0" + b"]" * 2_000
        with self.assertRaisesRegex(ValueError, "INVALID_JSON_RECORD"):
            decode_stored_event(deeply_nested)

    def test_all_public_event_payloads_round_trip_exactly(self):
        _state, events = event_sequence()
        self.assertEqual(7, len(events))
        previous = None
        for event in events:
            record = stored(event, previous)
            encoded = encode_stored_event(record)
            self.assertEqual(record, decode_stored_event(encoded))
            self.assertEqual(encoded, encode_stored_event(decode_stored_event(encoded)))
            previous = record.event_hash

    def test_topic_switch_event_round_trips_with_candidate_trigger(self):
        state = initial_dialogue_state("dialogue-1", 1)
        state, _ = advance(
            state,
            StartSession("command-start"),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        state, _ = advance(
            state,
            PresentCandidates("command-candidates", ("支付一致性",)),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        state, _ = advance(
            state,
            StartTopic("command-topic", contract()),
            context(TriggerKind.INITIAL_TURN),
        )
        _state, event = advance(
            state,
            SwitchTopic("command-switch"),
            context(TriggerKind.TOPIC_CANDIDATES, "work-switch"),
        )

        record = stored(event)
        encoded = encode_stored_event(record)

        self.assertEqual("topic_switch_requested", record.event_type)
        self.assertEqual(record, decode_stored_event(encoded))

    def test_work_failure_and_recovery_union_round_trips_exactly(self):
        state, _started, retry_events = failure_retry_sequence()
        self.assertIs(type(retry_events[0].payload), WorkFailed)
        self.assertIs(type(retry_events[1].payload), WorkRequeued)
        work = state.session_work
        assert work is not None
        dead_failure = failure(WorkFailureCategory.HOST_PERMANENT)
        dead_lettered = CommittedDialogueEvent(
            event_id="event-dead-lettered",
            sequence=state.sequence + 1,
            from_version=state.conversation_version,
            to_version=state.conversation_version + 1,
            command_id="command-dead-letter",
            payload=WorkDeadLettered(
                failed_work_id=work.work_id,
                failed_trigger=work.trigger,
                failed_attempt=work.attempt,
                failure=dead_failure,
                allowed_actions=(WorkRecoveryAction.RETRY,),
            ),
        )
        recovery_trigger = replace(
            work.trigger,
            work_id="trigger-explicit-recovery",
        )
        recovery_requested = CommittedDialogueEvent(
            event_id="event-recovery-requested",
            sequence=dead_lettered.sequence + 1,
            from_version=dead_lettered.to_version,
            to_version=dead_lettered.to_version + 1,
            command_id="command-recovery",
            payload=WorkRecoveryRequested(
                dead_work_id=work.work_id,
                action=WorkRecoveryAction.RETRY,
                next_trigger=recovery_trigger,
                evidence=EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    work.trigger.evidence_digest,
                ),
            ),
        )

        expected_types = (
            "work_failed",
            "work_requeued",
            "work_dead_lettered",
            "work_recovery_requested",
        )
        previous = None
        for event, expected_type in zip(
            (*retry_events, dead_lettered, recovery_requested),
            expected_types,
            strict=True,
        ):
            record = stored(event, previous)
            encoded = encode_stored_event(record)
            decoded = decode_stored_event(encoded)
            self.assertEqual(record, decoded)
            self.assertEqual(expected_type, decoded.event_type)
            previous = record.event_hash

    def test_version_neutral_failure_batch_binds_marker_and_snapshot(self):
        state, started, retry_events = failure_retry_sequence()
        self.assertEqual(
            (1, 1),
            (retry_events[0].from_version, retry_events[0].to_version),
        )
        self.assertEqual(
            (1, 1),
            (retry_events[1].from_version, retry_events[1].to_version),
        )
        self.assertEqual(3, state.sequence)
        self.assertEqual(1, state.conversation_version)

        started_state = reduce(
            initial_dialogue_state("dialogue-1", 1),
            started,
        )
        started_record = stored(started)
        started_marker = build_transaction_marker(
            session_id="dialogue-1",
            transaction_id="transaction-start",
            command_id=started.command_id,
            request_digest=REAL_DIGEST,
            registry_generation=1,
            previous_marker_hash=None,
            state=started_state,
            events=(started_record,),
        )
        failed_record = stored(retry_events[0], started_record.event_hash)
        requeued_record = stored(retry_events[1], failed_record.event_hash)
        retry_marker = build_transaction_marker(
            session_id="dialogue-1",
            transaction_id="transaction-failure",
            command_id="command-failure",
            request_digest="sha256:" + "2" * 64,
            registry_generation=1,
            previous_marker_hash=started_marker.marker_hash,
            state=state,
            events=(failed_record, requeued_record),
        )
        self.assertEqual(
            (2, 3),
            (retry_marker.from_sequence, retry_marker.to_sequence),
        )

        snapshot = build_state_snapshot(
            state,
            last_event_hash=requeued_record.event_hash,
            last_marker_hash=retry_marker.marker_hash,
            receipts=(
                receipt_from_marker(started_marker),
                receipt_from_marker(retry_marker),
            ),
        )
        self.assertEqual(
            snapshot,
            decode_state_snapshot(encode_state_snapshot(snapshot)),
        )

    def test_codec_rejects_noncanonical_version_deltas_and_pairwise_gaps(self):
        state, started, retry_events = failure_retry_sequence()
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_EVENT"):
            stored(
                replace(
                    retry_events[0],
                    to_version=retry_events[0].from_version + 1,
                )
            )
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_EVENT"):
            stored(replace(started, to_version=started.from_version))

        failed_record = stored(retry_events[0])
        version_gap = replace(
            retry_events[1],
            from_version=retry_events[1].from_version + 1,
            to_version=retry_events[1].to_version + 1,
        )
        requeued_record = stored(version_gap, failed_record.event_hash)
        with self.assertRaisesRegex(ValueError, "INVALID_TRANSACTION_MARKER"):
            build_transaction_marker(
                session_id="dialogue-1",
                transaction_id="transaction-gap",
                command_id="command-failure",
                request_digest=REAL_DIGEST,
                registry_generation=1,
                previous_marker_hash=None,
                state=replace(state, conversation_version=2),
                events=(failed_record, requeued_record),
            )

    def test_internal_deactivation_event_round_trips_exactly(self):
        state = initial_dialogue_state("dialogue-1", 1)
        state, _event = advance(
            state,
            StartSession("command-1"),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        decision = decide_fenced_quiesce(
            state,
            PrepareSessionDeactivation("command-fence"),
            FencedQuiesceContext("handoff-1", "dialogue-1", 1, 2),
        )
        assert isinstance(decision, Accepted)
        event = committed(state, decision.events[0])
        record = build_stored_event(
            "dialogue-1",
            event,
            None,
            registry_generation=2,
            occurred_at=OCCURRED_AT,
            actor=ACTOR,
            causation_id="handoff-1",
        )
        self.assertEqual(record, decode_stored_event(encode_stored_event(record)))

    def test_marker_and_state_snapshot_bind_the_result(self):
        state, events = event_sequence()
        replayed = initial_dialogue_state("dialogue-1", 1)
        previous_event_hash = None
        previous_marker_hash = None
        receipts = []
        marker = None
        for event in events:
            replayed = reduce(replayed, event)
            stored_event = stored(event, previous_event_hash)
            marker = build_transaction_marker(
                session_id="dialogue-1",
                transaction_id=f"transaction-{event.sequence}",
                command_id=event.command_id,
                request_digest=REAL_DIGEST,
                registry_generation=1,
                previous_marker_hash=previous_marker_hash,
                state=replayed,
                events=(stored_event,),
            )
            receipts.append(receipt_from_marker(marker))
            previous_event_hash = stored_event.event_hash
            previous_marker_hash = marker.marker_hash
        assert marker is not None
        self.assertEqual(
            marker,
            decode_transaction_marker(encode_transaction_marker(marker)),
        )
        snapshot = build_state_snapshot(
            state,
            last_event_hash=previous_event_hash,
            last_marker_hash=previous_marker_hash,
            receipts=tuple(receipts),
        )
        self.assertEqual(
            snapshot,
            decode_state_snapshot(encode_state_snapshot(snapshot)),
        )
        self.assertEqual(marker.state_digest, dialogue_state_digest(state))

    def test_noncanonical_duplicate_unknown_and_trailing_data_fail_closed(self):
        _state, events = event_sequence()
        record = stored(events[0])
        raw = encode_stored_event(record)
        with self.assertRaisesRegex(ValueError, "NON_CANONICAL_RECORD"):
            decode_stored_event(raw + b"\n")
        duplicate = b'{"$type":"StoredDialogueEvent","$type":"StoredDialogueEvent"}'
        with self.assertRaisesRegex(ValueError, "DUPLICATE_JSON_KEY"):
            decode_stored_event(duplicate)
        tree = json.loads(raw)
        tree["unknown"] = True
        with self.assertRaisesRegex(ValueError, "INVALID_RECORD_KEYS"):
            decode_stored_event(canonical_json_bytes(tree))

    def test_bool_sequence_and_unknown_enum_fail_closed(self):
        _state, events = event_sequence()
        raw = encode_stored_event(
            stored(events[0])
        )
        tree = json.loads(raw)
        tree["event"]["sequence"] = True
        with self.assertRaisesRegex(ValueError, "INVALID_SCALAR_TYPE"):
            decode_stored_event(canonical_json_bytes(tree))

        topic_raw = encode_stored_event(
            stored(events[2])
        )
        topic_tree = json.loads(topic_raw)
        topic_tree["event"]["payload"]["evidence"]["health"] = "mystery"
        with self.assertRaisesRegex(ValueError, "INVALID_ENUM_VALUE"):
            decode_stored_event(canonical_json_bytes(topic_tree))

    def test_typed_encode_rejects_invalid_nested_runtime_shapes(self):
        class ForeignLens(StrEnum):
            MIXED = "mixed"

        class ForeignActorKind(StrEnum):
            RUNTIME = "runtime"

        _state, events = event_sequence()
        topic_event = events[2]
        topic_payload = topic_event.payload
        assert hasattr(topic_payload, "contract")

        bool_contract = replace(topic_payload.contract, contract_version=True)
        with self.assertRaisesRegex(ValueError, "INVALID_SCALAR_TYPE"):
            stored(
                replace(
                    topic_event,
                    payload=replace(topic_payload, contract=bool_contract),
                )
            )

        foreign_enum_contract = replace(
            topic_payload.contract,
            starting_lens=ForeignLens.MIXED,
        )
        with self.assertRaisesRegex(ValueError, "INVALID_ENUM_VALUE"):
            stored(
                replace(
                    topic_event,
                    payload=replace(topic_payload, contract=foreign_enum_contract),
                )
            )

        with self.assertRaisesRegex(ValueError, "INVALID_STORED_EVENT"):
            stored(replace(events[0], event_id=""))
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_EVENT"):
            stored(replace(events[0], sequence=-1))
        started = events[0]
        assert isinstance(started.payload, SessionStarted)
        invalid_triggers = (
            replace(started.payload.candidate_trigger, work_id="../escape"),
            replace(started.payload.candidate_trigger, work_id="x" * 129),
            replace(
                started.payload.candidate_trigger,
                input_digest="sha256:not-a-digest",
            ),
        )
        for invalid_trigger in invalid_triggers:
            invalid_event = replace(
                started,
                payload=replace(
                    started.payload,
                    candidate_trigger=invalid_trigger,
                ),
            )
            with self.subTest(trigger=invalid_trigger), self.assertRaisesRegex(
                ValueError,
                "INVALID_STORED_EVENT",
            ):
                stored(invalid_event)

            invalid_request = DialogueWriteRequestRecord(
                schema_version=2,
                record_type="dialogue_write_request",
                session_id="dialogue-1",
                expected_registry_generation=1,
                expected_conversation_version=0,
                command=StartSession("command-invalid-trigger"),
                context=replace(
                    context(TriggerKind.TOPIC_CANDIDATES),
                    trigger=invalid_trigger,
                ),
            )
            with self.assertRaisesRegex(
                ValueError,
                "INVALID_DIALOGUE_WRITE_REQUEST",
            ):
                dialogue_request_digest(invalid_request)
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_EVENT"):
            build_stored_event(
                "dialogue-1",
                events[0],
                None,
                registry_generation=1,
                occurred_at=OCCURRED_AT,
                actor=DialogueActor(ForeignActorKind.RUNTIME, "runtime.local"),
                causation_id=None,
            )

    def test_multi_event_marker_binds_contiguous_versions_and_final_state(self):
        state = initial_dialogue_state("dialogue-1", 1)
        state, first = advance(
            state,
            StartSession("command-batch"),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        state, second = advance(
            state,
            PresentCandidates("command-other", ("支付一致性",)),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        second = replace(second, command_id="command-batch")
        first_record = stored(first)
        second_record = stored(second, first_record.event_hash)

        marker = build_transaction_marker(
            session_id="dialogue-1",
            transaction_id="transaction-batch",
            command_id="command-batch",
            request_digest=REAL_DIGEST,
            registry_generation=1,
            previous_marker_hash=None,
            state=state,
            events=(first_record, second_record),
        )
        self.assertEqual((1, 2), (marker.from_sequence, marker.to_sequence))

        discontinuous = stored(
            replace(second, from_version=0, to_version=1),
            first_record.event_hash,
        )
        with self.assertRaisesRegex(ValueError, "INVALID_TRANSACTION_MARKER"):
            build_transaction_marker(
                session_id="dialogue-1",
                transaction_id="transaction-batch",
                command_id="command-batch",
                request_digest=REAL_DIGEST,
                registry_generation=1,
                previous_marker_hash=None,
                state=state,
                events=(first_record, discontinuous),
            )

        with self.assertRaisesRegex(ValueError, "INVALID_TRANSACTION_MARKER"):
            build_transaction_marker(
                session_id="dialogue-1",
                transaction_id="transaction-batch",
                command_id="command-batch",
                request_digest=REAL_DIGEST,
                registry_generation=1,
                previous_marker_hash=None,
                state=replace(state, conversation_version=3),
                events=(first_record, second_record),
            )

    def test_snapshot_validates_state_and_receipt_identity_sets(self):
        state = initial_dialogue_state("dialogue-1", 1)
        invalid_state = replace(
            state,
            lifecycle=SessionLifecycle.OPEN,
            phase=ConversationPhase.AWAITING_USER,
        )
        with self.assertRaisesRegex(ValueError, "INVALID_STATE_SNAPSHOT"):
            build_state_snapshot(
                invalid_state,
                last_event_hash=None,
                last_marker_hash=None,
                receipts=(),
            )

        state, _first = advance(
            state,
            StartSession("command-1"),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        state, _second = advance(
            state,
            PresentCandidates("command-2", ("支付一致性",)),
            context(TriggerKind.TOPIC_CANDIDATES),
        )
        state_digest = dialogue_state_digest(state)
        first_receipt = DialogueCommandReceipt(
            "command-1",
            REAL_DIGEST,
            "transaction-1",
            "sha256:" + "2" * 64,
            1,
            1,
            ("event-1",),
            REAL_DIGEST,
        )
        second_receipt = DialogueCommandReceipt(
            "command-2",
            REAL_DIGEST,
            "transaction-2",
            "sha256:" + "3" * 64,
            2,
            2,
            ("event-2",),
            state_digest,
        )
        build_state_snapshot(
            state,
            last_event_hash=REAL_DIGEST,
            last_marker_hash=second_receipt.marker_hash,
            receipts=(first_receipt, second_receipt),
        )

        duplicate_inside = replace(
            second_receipt,
            from_sequence=1,
            event_ids=("event-2", "event-2"),
        )
        with self.assertRaisesRegex(ValueError, "INVALID_STATE_SNAPSHOT"):
            build_state_snapshot(
                state,
                last_event_hash=REAL_DIGEST,
                last_marker_hash=duplicate_inside.marker_hash,
                receipts=(duplicate_inside,),
            )

        for duplicate_second in (
            replace(second_receipt, event_ids=("event-1",)),
            replace(second_receipt, transaction_id="transaction-1"),
        ):
            with self.subTest(receipt=duplicate_second):
                with self.assertRaisesRegex(
                    ValueError, "INVALID_STATE_SNAPSHOT"
                ):
                    build_state_snapshot(
                        state,
                        last_event_hash=REAL_DIGEST,
                        last_marker_hash=duplicate_second.marker_hash,
                        receipts=(first_receipt, duplicate_second),
                    )

    def test_hash_and_marker_tampering_fail_closed(self):
        state, events = event_sequence()
        record = stored(events[-1])
        tree = json.loads(encode_stored_event(record))
        tree["event_hash"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ValueError, "STORED_EVENT_HASH_MISMATCH"):
            decode_stored_event(canonical_json_bytes(tree))

        marker = build_transaction_marker(
            session_id="dialogue-1",
            transaction_id="transaction-7",
            command_id="command-7",
            request_digest=REAL_DIGEST,
            registry_generation=1,
            previous_marker_hash=None,
            state=state,
            events=(record,),
        )
        marker_tree = json.loads(encode_transaction_marker(marker))
        marker_tree["state_digest"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(
            ValueError, "TRANSACTION_MARKER_HASH_MISMATCH"
        ):
            decode_transaction_marker(canonical_json_bytes(marker_tree))

    def test_size_utf8_and_exact_record_types_are_bounded(self):
        with self.assertRaisesRegex(ValueError, "INVALID_RECORD_SIZE"):
            decode_stored_event(b"x" * (MAX_RECORD_BYTES + 1))
        with self.assertRaisesRegex(ValueError, "INVALID_JSON_RECORD"):
            decode_stored_event(b"\xff")
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_EVENT"):
            encode_stored_event(object())

    def test_semantic_request_digest_binds_versions_command_and_context(self):
        record = DialogueWriteRequestRecord(
            schema_version=2,
            record_type="dialogue_write_request",
            session_id="dialogue-1",
            expected_registry_generation=1,
            expected_conversation_version=0,
            command=StartSession("command-1"),
            context=context(TriggerKind.TOPIC_CANDIDATES),
        )
        baseline = dialogue_request_digest(record)
        self.assertRegex(baseline, r"^sha256:[0-9a-f]{64}$")
        self.assertNotEqual(
            baseline,
            dialogue_request_digest(
                replace(record, expected_conversation_version=1)
            ),
        )
        selection_request = replace(
            record,
            expected_conversation_version=2,
            command=SelectTopic("command-select", "Outbox"),
            context=context(TriggerKind.TOPIC_SELECTION, "selection-trigger"),
        )
        self.assertRegex(
            dialogue_request_digest(selection_request),
            r"^sha256:[0-9a-f]{64}$",
        )
        failure_request = replace(
            record,
            expected_conversation_version=1,
            command=ReportWorkFailure(
                "command-failure",
                "work-current",
                failure(),
            ),
            context=context(TriggerKind.TOPIC_CANDIDATES, "trigger-retry"),
        )
        recovery_request = replace(
            failure_request,
            command=RecoverWork(
                "command-recovery",
                "work-dead",
                WorkRecoveryAction.RETRY,
            ),
        )
        self.assertRegex(
            dialogue_request_digest(failure_request),
            r"^sha256:[0-9a-f]{64}$",
        )
        self.assertNotEqual(
            dialogue_request_digest(failure_request),
            dialogue_request_digest(recovery_request),
        )

        invalid_records = (
            object(),
            replace(record, schema_version=1),
            replace(record, session_id=""),
            replace(record, expected_registry_generation=True),
            replace(record, command=StartSession("")),
        )
        for invalid in invalid_records:
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError, "INVALID_DIALOGUE_WRITE_REQUEST"
            ):
                dialogue_request_digest(invalid)

    def test_public_builders_reject_invalid_envelopes_before_hashing(self):
        state, events = event_sequence()
        first = events[0]
        invalid_stored_arguments = (
            {"occurred_at": "2026-08-16T12:00:00"},
            {"previous_event_hash": "not-a-hash"},
            {"causation_id": ""},
            {"registry_generation": True},
        )
        for overrides in invalid_stored_arguments:
            kwargs = {
                "registry_generation": 1,
                "occurred_at": OCCURRED_AT,
                "actor": ACTOR,
                "causation_id": None,
            }
            previous = overrides.pop("previous_event_hash", None)
            kwargs.update(overrides)
            with self.subTest(overrides=kwargs):
                with self.assertRaisesRegex(ValueError, "INVALID_STORED_EVENT"):
                    build_stored_event(
                        "dialogue-1",
                        first,
                        previous,
                        **kwargs,
                    )

        first_record = stored(first)
        invalid_marker_arguments = (
            {"session_id": ""},
            {"request_digest": "not-a-hash"},
            {"registry_generation": True},
            {"events": []},
            {"previous_marker_hash": "not-a-hash"},
        )
        for overrides in invalid_marker_arguments:
            kwargs = {
                "session_id": "dialogue-1",
                "transaction_id": "transaction-1",
                "command_id": first.command_id,
                "request_digest": REAL_DIGEST,
                "registry_generation": 1,
                "previous_marker_hash": None,
                "state": reduce(initial_dialogue_state("dialogue-1", 1), first),
                "events": (first_record,),
            }
            kwargs.update(overrides)
            with self.subTest(overrides=overrides), self.assertRaisesRegex(
                ValueError, "INVALID_TRANSACTION_MARKER"
            ):
                build_transaction_marker(**kwargs)
        self.assertEqual(7, state.sequence)

    def test_decoders_reject_wrong_nested_shapes_and_intrinsic_marker_fields(self):
        state, events = event_sequence()
        record = stored(events[0])
        raw_tree = json.loads(encode_stored_event(record))

        mutations = (
            (("actor",), []),
            (("actor", "kind"), 1),
            (("event", "payload"), []),
            (("event", "payload", "$type"), "UnknownPayload"),
        )
        for path, replacement in mutations:
            tree = json.loads(encode_stored_event(record))
            target = tree
            for component in path[:-1]:
                target = target[component]
            target[path[-1]] = replacement
            with self.subTest(path=path), self.assertRaises(ValueError):
                decode_stored_event(canonical_json_bytes(tree))

        marker = build_transaction_marker(
            session_id="dialogue-1",
            transaction_id="transaction-7",
            command_id=events[-1].command_id,
            request_digest=REAL_DIGEST,
            registry_generation=1,
            previous_marker_hash=None,
            state=state,
            events=(stored(events[-1]),),
        )
        malformed_markers = (
            replace(marker, registry_generation=0),
            replace(marker, events=(replace(marker.events[0], sequence=0),)),
            replace(
                marker,
                events=(marker.events[0], marker.events[0]),
                to_sequence=marker.from_sequence,
            ),
        )
        for malformed in malformed_markers:
            with self.subTest(marker=malformed), self.assertRaisesRegex(
                ValueError, "INVALID_TRANSACTION_MARKER"
            ):
                encode_transaction_marker(malformed)

        tree = raw_tree
        tree["event"]["payload"]["candidate_trigger"]["work_id"] = ""
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_EVENT"):
            decode_stored_event(canonical_json_bytes(tree))

    def test_snapshot_chain_tip_and_receipt_coverage_are_exact(self):
        initial = initial_dialogue_state("dialogue-1", 1)
        empty = build_state_snapshot(
            initial,
            last_event_hash=None,
            last_marker_hash=None,
            receipts=(),
        )
        with self.assertRaisesRegex(ValueError, "INVALID_STATE_SNAPSHOT"):
            encode_state_snapshot(replace(empty, last_event_hash=REAL_DIGEST))

        state, events = event_sequence()
        last_record = stored(events[-1])
        marker = build_transaction_marker(
            session_id="dialogue-1",
            transaction_id="transaction-7",
            command_id=events[-1].command_id,
            request_digest=REAL_DIGEST,
            registry_generation=1,
            previous_marker_hash=None,
            state=state,
            events=(last_record,),
        )
        receipt = receipt_from_marker(marker)
        valid_shape = build_state_snapshot(
            state,
            last_event_hash=last_record.event_hash,
            last_marker_hash=marker.marker_hash,
            receipts=(
                replace(
                    receipt,
                    from_sequence=1,
                    event_ids=tuple(f"event-{index}" for index in range(1, 8)),
                ),
            ),
        )
        invalid = (
            replace(valid_shape, last_event_hash=None),
            replace(valid_shape, receipts=()),
            replace(
                valid_shape,
                receipts=(replace(valid_shape.receipts[0], state_digest=REAL_DIGEST),),
            ),
            replace(
                valid_shape,
                receipts=(replace(valid_shape.receipts[0], from_sequence=2),),
            ),
        )
        for snapshot in invalid:
            with self.subTest(snapshot=snapshot):
                with self.assertRaisesRegex(ValueError, "INVALID_STATE_SNAPSHOT"):
                    encode_state_snapshot(snapshot)


if __name__ == "__main__":
    unittest.main()
