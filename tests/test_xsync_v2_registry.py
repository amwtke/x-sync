import ast
from dataclasses import FrozenInstanceError, replace
import hashlib
import inspect
from pathlib import Path
from typing import get_args
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.registry import (
    ActivateTarget,
    Activated,
    BeginHandoff,
    CommandReceipt,
    CommittedRegistryEvent,
    CommittedRegistryTransaction,
    CompleteHandoff,
    CreateAndActivateDialogue,
    Deactivated,
    DeactivationStarted,
    DialogueCreated,
    DialogueRegistrationStatus,
    QuiesceProof,
    RegisteredDialogue,
    RegistryAccepted,
    RegistryCommand,
    RegistryEventPayload,
    RegistryIdempotent,
    RegistryRejected,
    RegistryState,
    decide,
    initial_registry_state,
    reduce,
    validate_transaction,
)


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_MODULE = (
    ROOT / "skills" / "x-sync" / "scripts" / "xsync_v2" / "registry.py"
)


def digest(value):
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def event_generation(state, payload):
    if type(payload) is DialogueCreated:
        return state.generation
    if type(payload) is DeactivationStarted:
        return payload.handoff.generation
    if type(payload) is Deactivated:
        return payload.proof.generation
    if type(payload) is Activated:
        return payload.generation
    raise AssertionError(payload)


def commit(state, decision, transaction_id):
    if type(decision) is not RegistryAccepted:
        raise AssertionError(decision)
    events = tuple(
        CommittedRegistryEvent(
            event_id=f"evt.{transaction_id}.{index}",
            event_digest=digest(f"event:{transaction_id}:{index}"),
            registry_sequence=state.registry_sequence + index,
            generation=event_generation(state, pending.payload),
            command_id=pending.command_id,
            body_digest=pending.body_digest,
            payload=pending.payload,
        )
        for index, pending in enumerate(decision.events, start=1)
    )
    return CommittedRegistryTransaction(
        transaction_id=transaction_id,
        transaction_digest=digest(f"transaction:{transaction_id}"),
        events=events,
    )


def create_active(registry_id="registry-1", session_id="dlg-a"):
    state = initial_registry_state(registry_id)
    command = CreateAndActivateDialogue(
        command_id="cmd.create-a",
        body_digest=digest("create-a"),
        expected_registry_sequence=0,
        expected_generation=0,
        target=ActivateTarget(session_id, digest("config-a")),
    )
    decision = decide(state, command)
    transaction = commit(state, decision, "tx.create-a")
    return reduce(state, transaction), command, transaction


def begin_handoff(state, target_id="dlg-b"):
    command = BeginHandoff(
        command_id=f"cmd.handoff.{target_id}",
        body_digest=digest(f"handoff:{target_id}"),
        expected_registry_sequence=state.registry_sequence,
        expected_generation=state.generation,
        handoff_id=f"handoff.{target_id}",
        source_session_id=state.current_session_id,
        target=ActivateTarget(target_id, digest(f"config:{target_id}")),
    )
    decision = decide(state, command)
    transaction = commit(state, decision, f"tx.handoff.{target_id}")
    return reduce(state, transaction), command, transaction


def proof_for(state):
    handoff = state.pending_handoff
    if handoff is None:
        raise AssertionError("pending handoff required")
    return QuiesceProof(
        source_session_id=handoff.source_session_id,
        target=handoff.target,
        handoff_id=handoff.handoff_id,
        generation=handoff.generation,
        dialogue_last_sequence=17,
        dialogue_last_event_id="evt.dialogue.tip",
        dialogue_event_digest=digest("dialogue-event"),
        dialogue_transaction_digest=digest("dialogue-transaction"),
        dialogue_state_digest=digest("dialogue-state"),
    )


def complete_handoff(state):
    handoff = state.pending_handoff
    if handoff is None:
        raise AssertionError("pending handoff required")
    command = CompleteHandoff(
        command_id=f"cmd.complete.{handoff.handoff_id}",
        body_digest=digest(f"complete:{handoff.handoff_id}"),
        expected_registry_sequence=state.registry_sequence,
        expected_generation=state.generation,
        handoff_id=handoff.handoff_id,
        proof=proof_for(state),
    )
    decision = decide(state, command)
    transaction = commit(state, decision, f"tx.complete.{handoff.handoff_id}")
    return reduce(state, transaction), command, transaction


def dialogue(state, session_id):
    matches = tuple(
        item for item in state.dialogues if item.session_id == session_id
    )
    if len(matches) != 1:
        raise AssertionError(matches)
    return matches[0]


class RegistryTest(unittest.TestCase):
    def test_initial_create_and_activate_is_one_atomic_transaction(self):
        initial = initial_registry_state("registry-1")
        command = CreateAndActivateDialogue(
            "cmd.create-a",
            digest("create-a"),
            0,
            0,
            ActivateTarget("dlg-a", digest("config-a")),
        )
        decision = decide(initial, command)
        self.assertIsInstance(decision, RegistryAccepted)
        assert isinstance(decision, RegistryAccepted)
        self.assertEqual(
            (DialogueCreated, Activated),
            tuple(type(item.payload) for item in decision.events),
        )
        transaction = commit(initial, decision, "tx.create-a")
        validate_transaction(initial, transaction)
        state = reduce(initial, transaction)
        self.assertEqual(2, state.registry_sequence)
        self.assertEqual(1, state.generation)
        self.assertEqual("dlg-a", state.current_session_id)
        self.assertEqual(
            DialogueRegistrationStatus.ACTIVE,
            dialogue(state, "dlg-a").status,
        )
        self.assertEqual(1, len(state.receipts))
        with self.assertRaises(FrozenInstanceError):
            state.generation = 9

    def test_begin_handoff_fences_generation_but_sequence_orders_events(self):
        active, _, _ = create_active()
        pending, _, transaction = begin_handoff(active)
        self.assertEqual(4, pending.registry_sequence)
        self.assertEqual(2, pending.generation)
        self.assertEqual(
            (DialogueCreated, DeactivationStarted),
            tuple(type(item.payload) for item in transaction.events),
        )
        self.assertEqual(
            DialogueRegistrationStatus.DEACTIVATING,
            dialogue(pending, "dlg-a").status,
        )
        self.assertEqual(
            DialogueRegistrationStatus.CREATED,
            dialogue(pending, "dlg-b").status,
        )
        self.assertEqual("dlg-a", pending.current_session_id)
        self.assertEqual(2, pending.pending_handoff.generation)
        self.assertEqual(4, pending.pending_handoff.started_registry_sequence)

    def test_complete_handoff_is_atomic_and_does_not_advance_generation(self):
        active, _, _ = create_active()
        pending, _, _ = begin_handoff(active)
        command = CompleteHandoff(
            "cmd.complete",
            digest("complete"),
            pending.registry_sequence,
            pending.generation,
            pending.pending_handoff.handoff_id,
            proof_for(pending),
        )
        decision = decide(pending, command)
        self.assertIsInstance(decision, RegistryAccepted)
        transaction = commit(pending, decision, "tx.complete")
        self.assertEqual(
            (Deactivated, Activated),
            tuple(type(item.payload) for item in transaction.events),
        )
        incomplete = replace(
            transaction,
            transaction_id="tx.incomplete",
            transaction_digest=digest("tx.incomplete"),
            events=transaction.events[:1],
        )
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_TRANSACTION"):
            validate_transaction(pending, incomplete)
        self.assertEqual("dlg-a", pending.current_session_id)

        complete = reduce(pending, transaction)
        self.assertEqual(6, complete.registry_sequence)
        self.assertEqual(2, complete.generation)
        self.assertEqual("dlg-b", complete.current_session_id)
        self.assertIsNone(complete.pending_handoff)
        self.assertEqual(
            DialogueRegistrationStatus.DEACTIVATED,
            dialogue(complete, "dlg-a").status,
        )
        self.assertEqual(
            DialogueRegistrationStatus.ACTIVE,
            dialogue(complete, "dlg-b").status,
        )

    def test_quiesce_proof_binds_every_handoff_and_dialogue_tip_field(self):
        active, _, _ = create_active()
        pending, _, _ = begin_handoff(active)
        valid = proof_for(pending)
        identity_replacements = (
            {"source_session_id": "dlg-other"},
            {"target": ActivateTarget("dlg-other", digest("config-other"))},
            {"handoff_id": "handoff.other"},
            {"generation": valid.generation + 1},
        )
        for index, changes in enumerate(identity_replacements):
            command = CompleteHandoff(
                f"cmd.bad-identity.{index}",
                digest(f"bad-identity:{index}"),
                pending.registry_sequence,
                pending.generation,
                pending.pending_handoff.handoff_id,
                replace(valid, **changes),
            )
            self.assertEqual(
                RegistryRejected("HANDOFF_PROOF_INVALID"),
                decide(pending, command),
            )

        tip_replacements = (
            {"dialogue_last_sequence": valid.dialogue_last_sequence + 1},
            {"dialogue_last_event_id": "evt.dialogue.other"},
            {"dialogue_event_digest": digest("other-event")},
            {"dialogue_transaction_digest": digest("other-transaction")},
            {"dialogue_state_digest": digest("other-state")},
        )
        for index, changes in enumerate(tip_replacements):
            changed_proof = replace(valid, **changes)
            command = CompleteHandoff(
                f"cmd.tip-proof.{index}",
                digest(f"tip-proof:{index}"),
                pending.registry_sequence,
                pending.generation,
                pending.pending_handoff.handoff_id,
                changed_proof,
            )
            decision = decide(pending, command)
            self.assertIsInstance(decision, RegistryAccepted)
            assert isinstance(decision, RegistryAccepted)
            deactivated = decision.events[0].payload
            self.assertIsInstance(deactivated, Deactivated)
            assert isinstance(deactivated, Deactivated)
            self.assertEqual(changed_proof, deactivated.proof)

        malformed_tips = (
            {"dialogue_last_sequence": False},
            {"dialogue_last_event_id": "../escape"},
            {"dialogue_event_digest": "SHA256:BAD"},
            {"dialogue_transaction_digest": "SHA256:BAD"},
            {"dialogue_state_digest": "SHA256:BAD"},
        )
        for index, changes in enumerate(malformed_tips):
            command = CompleteHandoff(
                f"cmd.malformed-proof.{index}",
                digest(f"malformed-proof:{index}"),
                pending.registry_sequence,
                pending.generation,
                pending.pending_handoff.handoff_id,
                replace(valid, **changes),
            )
            self.assertEqual(
                RegistryRejected("VALIDATION_FAILED"), decide(pending, command)
            )

    def test_command_and_transaction_retries_are_idempotent(self):
        state, command, transaction = create_active()
        retry = decide(state, command)
        self.assertIsInstance(retry, RegistryIdempotent)
        assert isinstance(retry, RegistryIdempotent)
        self.assertEqual(transaction.transaction_id, retry.receipt.transaction_id)
        self.assertIs(state, reduce(state, transaction))

        conflicting_command = replace(command, body_digest=digest("changed"))
        self.assertEqual(
            RegistryRejected("IDEMPOTENCY_CONFLICT"),
            decide(state, conflicting_command),
        )
        conflicting_transaction = replace(
            transaction,
            transaction_digest=digest("changed-transaction"),
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_INTEGRITY_ERROR"):
            reduce(state, conflicting_transaction)

    def test_full_replay_is_deterministic(self):
        initial = initial_registry_state("registry-1")
        active, _, create_transaction = create_active()
        pending, _, begin_transaction = begin_handoff(active)
        complete, _, complete_transaction = complete_handoff(pending)

        replayed = initial
        for transaction in (
            create_transaction,
            begin_transaction,
            complete_transaction,
        ):
            replayed = reduce(replayed, transaction)
        self.assertEqual(complete, replayed)
        self.assertIs(replayed, reduce(replayed, complete_transaction))

    def test_sequence_and_generation_corruption_fail_independently(self):
        active, _, _ = create_active()
        decision = decide(
            active,
            BeginHandoff(
                "cmd.begin",
                digest("begin"),
                active.registry_sequence,
                active.generation,
                "handoff-1",
                "dlg-a",
                ActivateTarget("dlg-b", digest("config-b")),
            ),
        )
        transaction = commit(active, decision, "tx.begin")
        first = transaction.events[0]
        sequence_gap = replace(
            transaction,
            events=(
                replace(first, registry_sequence=first.registry_sequence + 1),
                transaction.events[1],
            ),
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_SEQUENCE_GAP"):
            reduce(active, sequence_gap)

        wrong_generation = replace(
            transaction,
            events=(
                replace(first, generation=first.generation + 1),
                transaction.events[1],
            ),
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_GENERATION_CONFLICT"):
            reduce(active, wrong_generation)

    def test_existing_created_target_omits_duplicate_create_event(self):
        active, _, _ = create_active()
        target = ActivateTarget("dlg-b", digest("config-b"))
        precreated = replace(
            active,
            dialogues=(
                *active.dialogues,
                RegisteredDialogue(
                    target.session_id,
                    target.config_digest,
                    DialogueRegistrationStatus.CREATED,
                    active.registry_sequence,
                    None,
                    None,
                    None,
                ),
            ),
        )
        command = BeginHandoff(
            "cmd.begin-existing",
            digest("begin-existing"),
            precreated.registry_sequence,
            precreated.generation,
            "handoff-existing",
            "dlg-a",
            target,
        )
        decision = decide(precreated, command)
        self.assertIsInstance(decision, RegistryAccepted)
        assert isinstance(decision, RegistryAccepted)
        self.assertEqual(
            (DeactivationStarted,),
            tuple(type(item.payload) for item in decision.events),
        )
        transaction = commit(precreated, decision, "tx.begin-existing")
        state = reduce(precreated, transaction)
        self.assertEqual(3, state.registry_sequence)
        self.assertEqual(2, state.generation)

    def test_illegal_sources_and_nested_handoffs_fail_closed(self):
        initial = initial_registry_state("registry-1")
        create = CreateAndActivateDialogue(
            "cmd.create",
            digest("create"),
            0,
            0,
            ActivateTarget("dlg-a", digest("config-a")),
        )
        active = reduce(initial, commit(initial, decide(initial, create), "tx.create"))
        create_again = replace(
            create,
            command_id="cmd.create-again",
            body_digest=digest("create-again"),
            expected_registry_sequence=active.registry_sequence,
            expected_generation=active.generation,
        )
        self.assertEqual(
            RegistryRejected("REGISTRY_STATE_CONFLICT"),
            decide(active, create_again),
        )
        wrong_source = BeginHandoff(
            "cmd.wrong-source",
            digest("wrong-source"),
            active.registry_sequence,
            active.generation,
            "handoff-wrong",
            "dlg-other",
            ActivateTarget("dlg-b", digest("config-b")),
        )
        self.assertEqual(
            RegistryRejected("SESSION_NOT_CURRENT"),
            decide(active, wrong_source),
        )
        pending, _, _ = begin_handoff(active)
        nested = replace(
            wrong_source,
            command_id="cmd.nested",
            body_digest=digest("nested"),
            expected_registry_sequence=pending.registry_sequence,
            expected_generation=pending.generation,
            source_session_id="dlg-a",
            handoff_id="handoff-nested",
        )
        self.assertEqual(
            RegistryRejected("HANDOFF_IN_PROGRESS"),
            decide(pending, nested),
        )
        no_pending_complete = CompleteHandoff(
            "cmd.no-pending",
            digest("no-pending"),
            active.registry_sequence,
            active.generation,
            "handoff-none",
            QuiesceProof(
                "dlg-a",
                ActivateTarget("dlg-b", digest("config-b")),
                "handoff-none",
                active.generation,
                1,
                "evt.tip",
                digest("event"),
                digest("transaction"),
                digest("state"),
            ),
        )
        self.assertEqual(
            RegistryRejected("REGISTRY_STATE_CONFLICT"),
            decide(active, no_pending_complete),
        )

    def test_cas_is_checked_after_idempotency_and_before_transition(self):
        active, _, _ = create_active()
        stale_sequence = BeginHandoff(
            "cmd.stale-sequence",
            digest("stale-sequence"),
            active.registry_sequence - 1,
            active.generation,
            "handoff-stale-sequence",
            "dlg-a",
            ActivateTarget("dlg-b", digest("config-b")),
        )
        self.assertEqual(
            RegistryRejected("REGISTRY_SEQUENCE_CONFLICT"),
            decide(active, stale_sequence),
        )
        stale_generation = replace(
            stale_sequence,
            command_id="cmd.stale-generation",
            body_digest=digest("stale-generation"),
            expected_registry_sequence=active.registry_sequence,
            expected_generation=active.generation - 1,
        )
        self.assertEqual(
            RegistryRejected("REGISTRY_GENERATION_CONFLICT"),
            decide(active, stale_generation),
        )

    def test_transaction_envelope_and_shape_are_strict(self):
        initial = initial_registry_state("registry-1")
        command = CreateAndActivateDialogue(
            "cmd.create",
            digest("create"),
            0,
            0,
            ActivateTarget("dlg-a", digest("config-a")),
        )
        transaction = commit(initial, decide(initial, command), "tx.create")
        reversed_events = replace(
            transaction,
            transaction_id="tx.reversed",
            transaction_digest=digest("reversed"),
            events=tuple(reversed(transaction.events)),
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_SEQUENCE_GAP"):
            validate_transaction(initial, reversed_events)
        mixed_commands = replace(
            transaction,
            transaction_id="tx.mixed",
            transaction_digest=digest("mixed"),
            events=(
                transaction.events[0],
                replace(transaction.events[1], command_id="cmd.other"),
            ),
        )
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_TRANSACTION"):
            validate_transaction(initial, mixed_commands)
        mutable_events = replace(transaction, events=list(transaction.events))
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_TRANSACTION"):
            validate_transaction(initial, mutable_events)

    def test_transaction_integrity_and_prior_identity_collisions_fail_closed(self):
        active, _, create_transaction = create_active()
        begin = BeginHandoff(
            "cmd.begin-collisions",
            digest("begin-collisions"),
            active.registry_sequence,
            active.generation,
            "handoff-collisions",
            "dlg-a",
            ActivateTarget("dlg-b", digest("config-b")),
        )
        transaction = commit(active, decide(active, begin), "tx.begin-collisions")

        transaction_id_collision = replace(
            transaction, transaction_id=create_transaction.transaction_id
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_INTEGRITY_ERROR"):
            validate_transaction(active, transaction_id_collision)

        prior_event_collision = replace(
            transaction,
            events=(
                replace(
                    transaction.events[0],
                    event_id=create_transaction.events[0].event_id,
                ),
                transaction.events[1],
            ),
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_INTEGRITY_ERROR"):
            validate_transaction(active, prior_event_collision)

        duplicate_event_ids = replace(
            transaction,
            events=(
                transaction.events[0],
                replace(
                    transaction.events[1],
                    event_id=transaction.events[0].event_id,
                ),
            ),
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_INTEGRITY_ERROR"):
            validate_transaction(active, duplicate_event_ids)

        changed_body = digest("changed-replay-body")
        conflicting_replay = replace(
            create_transaction,
            events=tuple(
                replace(event, body_digest=changed_body)
                for event in create_transaction.events
            ),
        )
        with self.assertRaisesRegex(ValueError, "IDEMPOTENCY_CONFLICT"):
            validate_transaction(active, conflicting_replay)

    def test_each_transaction_phase_rejects_generation_or_shape_tampering(self):
        initial = initial_registry_state("registry-1")
        create = CreateAndActivateDialogue(
            "cmd.create-shapes",
            digest("create-shapes"),
            0,
            0,
            ActivateTarget("dlg-a", digest("config-a")),
        )
        create_transaction = commit(initial, decide(initial, create), "tx.create")
        activated = create_transaction.events[1].payload
        assert isinstance(activated, Activated)
        wrong_activation_generation = replace(
            create_transaction,
            events=(
                create_transaction.events[0],
                replace(
                    create_transaction.events[1],
                    generation=activated.generation + 1,
                    payload=replace(
                        activated, generation=activated.generation + 1
                    ),
                ),
            ),
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_GENERATION_CONFLICT"):
            validate_transaction(initial, wrong_activation_generation)

        active = reduce(initial, create_transaction)
        begin = BeginHandoff(
            "cmd.begin-shapes",
            digest("begin-shapes"),
            active.registry_sequence,
            active.generation,
            "handoff-shapes",
            "dlg-a",
            ActivateTarget("dlg-b", digest("config-b")),
        )
        begin_transaction = commit(active, decide(active, begin), "tx.begin")
        started = begin_transaction.events[1].payload
        assert isinstance(started, DeactivationStarted)
        wrong_started_generation = replace(
            begin_transaction,
            events=(
                begin_transaction.events[0],
                replace(
                    begin_transaction.events[1],
                    generation=started.handoff.generation + 1,
                    payload=DeactivationStarted(
                        replace(
                            started.handoff,
                            generation=started.handoff.generation + 1,
                        )
                    ),
                ),
            ),
        )
        with self.assertRaisesRegex(ValueError, "REGISTRY_GENERATION_CONFLICT"):
            validate_transaction(active, wrong_started_generation)

        mismatched_created_target = replace(
            begin_transaction,
            events=(
                replace(
                    begin_transaction.events[0],
                    payload=DialogueCreated(
                        ActivateTarget("dlg-other", digest("config-other"))
                    ),
                ),
                begin_transaction.events[1],
            ),
        )
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_TRANSACTION"):
            validate_transaction(active, mismatched_created_target)

        pending = reduce(active, begin_transaction)
        completion = CompleteHandoff(
            "cmd.complete-shapes",
            digest("complete-shapes"),
            pending.registry_sequence,
            pending.generation,
            pending.pending_handoff.handoff_id,
            proof_for(pending),
        )
        completion_transaction = commit(
            pending, decide(pending, completion), "tx.complete"
        )
        final_activation = completion_transaction.events[1].payload
        assert isinstance(final_activation, Activated)
        wrong_final_target = replace(
            completion_transaction,
            events=(
                completion_transaction.events[0],
                replace(
                    completion_transaction.events[1],
                    payload=replace(
                        final_activation,
                        target=ActivateTarget(
                            "dlg-other", digest("config-other")
                        ),
                    ),
                ),
            ),
        )
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_TRANSACTION"):
            validate_transaction(pending, wrong_final_target)

    def test_nested_record_and_state_history_invariants_are_exact(self):
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_ID"):
            initial_registry_state("../registry")

        initial = initial_registry_state("registry-1")
        command = CreateAndActivateDialogue(
            "cmd.create",
            digest("create"),
            0,
            0,
            ActivateTarget("dlg-a", digest("config-a")),
        )
        self.assertEqual(
            RegistryRejected("VALIDATION_FAILED"),
            decide(initial, replace(command, target=None)),
        )
        for corrupt in (
            replace(initial, dialogues=(object(),)),
            replace(initial, pending_handoff=object()),
            replace(initial, receipts=(object(),)),
        ):
            self.assertEqual(
                RegistryRejected("STATE_INVARIANT_VIOLATION"),
                decide(corrupt, command),
            )

        active, _, _ = create_active()
        duplicate_dialogue = replace(
            active, dialogues=(*active.dialogues, active.dialogues[0])
        )
        missing_history = replace(active, receipts=())
        receipt = active.receipts[0]
        shifted_receipt = replace(
            receipt,
            first_registry_sequence=2,
            event_ids=(receipt.event_ids[-1],),
            event_digests=(receipt.event_digests[-1],),
        )
        shifted_history = replace(active, receipts=(shifted_receipt,))
        for corrupt in (
            duplicate_dialogue,
            missing_history,
            shifted_history,
            replace(active, current_session_id=None),
        ):
            self.assertEqual(
                RegistryRejected("STATE_INVARIANT_VIOLATION"),
                decide(corrupt, command),
            )

    def test_exact_types_ids_and_hashes_reject_mutable_or_subclass_bypasses(self):
        class RegistryStateSubclass(RegistryState):
            pass

        class CreateSubclass(CreateAndActivateDialogue):
            pass

        class EventSubclass(CommittedRegistryEvent):
            pass

        initial = initial_registry_state("registry-1")
        subclass_state = RegistryStateSubclass(
            initial.registry_id,
            initial.registry_sequence,
            initial.generation,
            initial.current_session_id,
            initial.dialogues,
            initial.pending_handoff,
            initial.receipts,
        )
        command = CreateAndActivateDialogue(
            "cmd.create",
            digest("create"),
            0,
            0,
            ActivateTarget("dlg-a", digest("config-a")),
        )
        self.assertEqual(
            RegistryRejected("STATE_INVARIANT_VIOLATION"),
            decide(subclass_state, command),
        )
        subclass_command = CreateSubclass(
            command.command_id,
            command.body_digest,
            command.expected_registry_sequence,
            command.expected_generation,
            command.target,
        )
        self.assertEqual(
            RegistryRejected("VALIDATION_FAILED"),
            decide(initial, subclass_command),
        )
        self.assertEqual(
            RegistryRejected("VALIDATION_FAILED"),
            decide(initial, replace(command, expected_generation=False)),
        )
        self.assertEqual(
            RegistryRejected("VALIDATION_FAILED"),
            decide(initial, replace(command, command_id="../escape")),
        )
        self.assertEqual(
            RegistryRejected("VALIDATION_FAILED"),
            decide(initial, replace(command, body_digest="SHA256:BAD")),
        )

        transaction = commit(initial, decide(initial, command), "tx.create")
        first = transaction.events[0]
        subclass_event = EventSubclass(
            first.event_id,
            first.event_digest,
            first.registry_sequence,
            first.generation,
            first.command_id,
            first.body_digest,
            first.payload,
        )
        invalid_transaction = replace(
            transaction,
            events=(subclass_event, transaction.events[1]),
        )
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_EVENT"):
            reduce(initial, invalid_transaction)
        mutable_state = replace(initial, dialogues=[])
        self.assertEqual(
            RegistryRejected("STATE_INVARIANT_VIOLATION"),
            decide(mutable_state, command),
        )

    def test_state_invariants_reject_corrupt_current_pending_and_receipts(self):
        active, command, _ = create_active()
        corrupt_current = replace(active, current_session_id="dlg-missing")
        self.assertEqual(
            RegistryRejected("STATE_INVARIANT_VIOLATION"),
            decide(corrupt_current, command),
        )
        invalid_receipt = CommandReceipt(
            "cmd.invalid",
            digest("invalid"),
            "tx.invalid",
            digest("tx.invalid"),
            9,
            8,
            ("evt.invalid",),
            (digest("evt.invalid"),),
        )
        corrupt_receipts = replace(
            active,
            receipts=(*active.receipts, invalid_receipt),
        )
        self.assertEqual(
            RegistryRejected("STATE_INVARIANT_VIOLATION"),
            decide(corrupt_receipts, command),
        )
        pending, _, _ = begin_handoff(active)
        bad_pending = replace(
            pending,
            pending_handoff=replace(
                pending.pending_handoff,
                source_session_id="dlg-other",
            ),
        )
        self.assertEqual(
            RegistryRejected("STATE_INVARIANT_VIOLATION"),
            decide(bad_pending, command),
        )

    def test_public_union_is_closed_documented_and_has_no_io_imports(self):
        self.assertEqual(
            {
                CreateAndActivateDialogue,
                BeginHandoff,
                CompleteHandoff,
            },
            set(get_args(RegistryCommand)),
        )
        self.assertEqual(
            {
                DialogueCreated,
                DeactivationStarted,
                Deactivated,
                Activated,
            },
            set(get_args(RegistryEventPayload)),
        )
        self.assertTrue(
            all(
                inspect.getdoc(item)
                for item in (
                    initial_registry_state,
                    decide,
                    validate_transaction,
                    reduce,
                )
            )
        )
        tree = ast.parse(REGISTRY_MODULE.read_text(encoding="utf-8"))
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
        forbidden = {
            "asyncio",
            "fcntl",
            "http",
            "msvcrt",
            "os",
            "pathlib",
            "random",
            "socket",
            "subprocess",
            "threading",
            "time",
            "urllib",
        }
        self.assertFalse(forbidden & imports)


if __name__ == "__main__":
    unittest.main()
