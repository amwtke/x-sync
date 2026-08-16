import hashlib
import json
import os
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from xsync_v2 import registry_store
from xsync_v2.locking import DomainLockManager, LockError
from xsync_v2.registry import (
    ActivateTarget,
    BeginHandoff,
    CompleteHandoff,
    CreateAndActivateDialogue,
    DialogueCreated,
    QuiesceProof,
    RegistryAccepted,
    decide,
    initial_registry_state,
)
from xsync_v2.registry_codec import (
    build_stored_registry_event,
    canonical_json_bytes,
    encode_stored_registry_event,
)
from xsync_v2.registry_store import (
    RegistryCommitRequest,
    RegistryStoreError,
    _RegistryTransactionLog,
)
from xsync_v2.secure_fs import SecureDirectory, SecureFsError

import tests.xsync_v2_path  # noqa: F401


def digest(label):
    import hashlib

    return "sha256:" + hashlib.sha256(label.encode("utf-8")).hexdigest()


def create_decision(state, *, body_label="create"):
    command = CreateAndActivateDialogue(
        "cmd.create",
        digest(body_label),
        state.registry_sequence,
        state.generation,
        ActivateTarget("dlg-a", digest("config-a")),
    )
    decision = decide(state, command)
    if type(decision) is not RegistryAccepted:
        raise AssertionError(decision)
    return decision


def request(
    decision,
    *,
    transaction_id="tx.create",
    event_ids=None,
    expected_registry_sequence=0,
    expected_generation=0,
):
    if event_ids is None:
        event_ids = tuple(
            f"evt.create.{index}"
            for index in range(1, len(decision.events) + 1)
        )
    return RegistryCommitRequest(
        transaction_id,
        event_ids,
        expected_registry_sequence,
        expected_generation,
        decision,
    )


class InjectedCrash(RuntimeError):
    pass


class RegistryStoreTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root_path = Path(self.temporary.name).resolve()
        self.root = SecureDirectory.open(self.root_path)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.locks = DomainLockManager(self.root_path / "locks")
        self.initial = initial_registry_state("registry-1")

    def tearDown(self):
        self.locks.close()
        self.dialogues.close()
        self.root.close()
        self.temporary.cleanup()

    def new_log(self, fault_hook=lambda _step: None):
        with self.locks.registry_exclusive() as authority:
            return _RegistryTransactionLog.create(
                self.dialogues,
                self.initial,
                self.locks,
                authority,
                fault_hook=fault_hook,
            )

    def commit(self, log, commit_request):
        with self.locks.registry_exclusive() as authority:
            return log.commit(commit_request, authority)

    def registry_path(self):
        return self.root_path / "dialogues" / "registry"

    def test_marker_linearizes_transaction_and_replay_rebuilds_receipts(self):
        log = self.new_log()
        commit_request = request(create_decision(log.tip().state))
        outcome = self.commit(log, commit_request)
        self.assertFalse(outcome.replayed)
        self.assertEqual(2, outcome.state.registry_sequence)
        self.assertEqual(1, outcome.state.generation)
        self.assertEqual(outcome.receipt, outcome.state.receipts[-1])
        self.assertEqual(outcome.transaction.events, log.read_committed())
        self.assertRegex(
            outcome.transaction.transaction_digest,
            r"^sha256:[0-9a-f]{64}$",
        )
        log.close()

        event_names = tuple(
            item.name for item in (self.registry_path() / "events").iterdir()
        )
        self.assertEqual(
            (
                "00000000000000000001-evt.create.1.json",
                "00000000000000000002-evt.create.2.json",
            ),
            tuple(sorted(event_names)),
        )
        marker_names = tuple(
            item.name
            for item in (self.registry_path() / "transactions").iterdir()
        )
        self.assertEqual(
            ("00000000000000000001-00000000000000000002-tx.create.json",),
            marker_names,
        )

        (self.registry_path() / "state.json").unlink()
        recovered = self.new_log()
        self.assertEqual(outcome.state, recovered.tip().state)
        self.assertEqual(outcome.receipt, recovered.tip().state.receipts[-1])
        self.assertEqual(outcome.transaction.events, recovered.read_committed())
        recovered.close()

    def test_full_handoff_replay_orders_by_sequence_not_generation(self):
        log = self.new_log()
        created = self.commit(log, request(create_decision(log.tip().state)))
        begin_command = BeginHandoff(
            "cmd.begin",
            digest("begin"),
            created.state.registry_sequence,
            created.state.generation,
            "handoff-1",
            "dlg-a",
            ActivateTarget("dlg-b", digest("config-b")),
        )
        begin_decision = decide(created.state, begin_command)
        self.assertIsInstance(begin_decision, RegistryAccepted)
        assert isinstance(begin_decision, RegistryAccepted)
        pending = self.commit(
            log,
            request(
                begin_decision,
                transaction_id="tx.begin",
                event_ids=("evt.begin.1", "evt.begin.2"),
                expected_registry_sequence=created.state.registry_sequence,
                expected_generation=created.state.generation,
            ),
        )
        handoff = pending.state.pending_handoff
        self.assertIsNotNone(handoff)
        assert handoff is not None
        proof = QuiesceProof(
            handoff.source_session_id,
            handoff.target,
            handoff.handoff_id,
            handoff.generation,
            9,
            "evt.dialogue.tip",
            digest("dialogue-event"),
            digest("dialogue-transaction"),
            digest("dialogue-state"),
        )
        complete_command = CompleteHandoff(
            "cmd.complete",
            digest("complete"),
            pending.state.registry_sequence,
            pending.state.generation,
            handoff.handoff_id,
            proof,
        )
        complete_decision = decide(pending.state, complete_command)
        self.assertIsInstance(complete_decision, RegistryAccepted)
        assert isinstance(complete_decision, RegistryAccepted)
        completed = self.commit(
            log,
            request(
                complete_decision,
                transaction_id="tx.complete",
                event_ids=("evt.complete.1", "evt.complete.2"),
                expected_registry_sequence=pending.state.registry_sequence,
                expected_generation=pending.state.generation,
            ),
        )
        self.assertEqual(6, completed.state.registry_sequence)
        self.assertEqual(2, completed.state.generation)
        self.assertEqual("dlg-b", completed.state.current_session_id)
        self.assertEqual(3, len(completed.state.receipts))
        self.assertEqual(
            (0, 1, 1, 2, 2, 2),
            tuple(item.generation for item in log.read_committed()),
        )
        log.close()

        recovered = self.new_log()
        self.assertEqual(completed.state, recovered.tip().state)
        self.assertEqual(
            tuple(range(1, 7)),
            tuple(
                item.registry_sequence for item in recovered.read_committed()
            ),
        )
        recovered.close()

    def test_crash_before_marker_leaves_no_fact_and_quarantines_events(self):
        def crash(step):
            if step == "before_marker":
                raise InjectedCrash(step)

        log = self.new_log(crash)
        with self.assertRaises(InjectedCrash):
            self.commit(log, request(create_decision(log.tip().state)))
        log.close()

        event_directory = self.registry_path() / "events"
        source = min(event_directory.iterdir())
        data = source.read_bytes()
        destination = self.registry_path() / "quarantine" / (
            f"orphan-{hashlib.sha256(source.name.encode()).hexdigest()[:16]}-"
            f"{hashlib.sha256(data).hexdigest()}.json"
        )
        os.link(source, destination)
        self.assertEqual(2, source.stat().st_nlink)

        recovered = self.new_log()
        self.assertEqual(0, recovered.tip().state.registry_sequence)
        self.assertEqual((), recovered.read_committed())
        self.assertEqual(
            2,
            len(tuple((self.registry_path() / "quarantine").iterdir())),
        )
        recovered.close()

    def test_crash_after_marker_recovers_once_and_retry_is_idempotent(self):
        def crash(step):
            if step == "marker_committed":
                raise InjectedCrash(step)

        log = self.new_log(crash)
        commit_request = request(create_decision(log.tip().state))
        with self.assertRaises(InjectedCrash):
            self.commit(log, commit_request)
        log.close()

        recovered = self.new_log()
        replay = self.commit(recovered, commit_request)
        self.assertTrue(replay.replayed)
        self.assertEqual(2, replay.state.registry_sequence)
        self.assertEqual(2, len(recovered.read_committed()))
        recovered.close()

    def test_stale_store_synchronizes_under_ex_and_explicit_recovery(self):
        writer = self.new_log()
        stale = self.new_log()
        commit_request = request(create_decision(writer.tip().state))
        committed = self.commit(writer, commit_request)

        replayed = self.commit(stale, commit_request)
        self.assertTrue(replayed.replayed)
        self.assertEqual(committed.state, replayed.state)
        self.assertEqual(2, stale.tip().state.registry_sequence)

        third = self.new_log()
        with self.locks.registry_exclusive() as authority:
            self.assertEqual(committed.state, third.recover(authority).state)
        writer.close()
        stale.close()
        third.close()

    def test_same_command_and_body_replays_but_changed_body_conflicts(self):
        log = self.new_log()
        decision = create_decision(log.tip().state)
        first = self.commit(log, request(decision))
        replay = self.commit(
            log,
            request(
                decision,
                transaction_id="tx.retry-can-differ",
                event_ids=("evt.retry.1", "evt.retry.2"),
            ),
        )
        self.assertTrue(replay.replayed)
        self.assertEqual(first.receipt, replay.receipt)
        changed = RegistryAccepted(
            tuple(
                replace(item, body_digest=digest("different"))
                for item in decision.events
            )
        )
        with self.assertRaisesRegex(RegistryStoreError, "IDEMPOTENCY_CONFLICT"):
            self.commit(log, request(changed, transaction_id="tx.changed"))
        self.assertEqual(2, log.tip().state.registry_sequence)
        log.close()

    def test_real_live_exclusive_registry_authority_is_required(self):
        log = self.new_log()
        commit_request = request(create_decision(log.tip().state))
        with self.locks.registry_shared() as shared, self.assertRaisesRegex(
            RegistryStoreError, "LOCK_AUTHORITY_REQUIRED"
        ):
            log.commit(commit_request, shared)
        with self.locks.registry_exclusive() as exclusive:
            expired = exclusive
        with self.assertRaisesRegex(
            RegistryStoreError, "LOCK_AUTHORITY_REQUIRED"
        ):
            log.commit(commit_request, expired)
        with self.assertRaisesRegex(
            RegistryStoreError, "LOCK_AUTHORITY_REQUIRED"
        ):
            log.commit(commit_request, object())
        log.close()

        with self.locks.registry_shared() as shared, self.assertRaisesRegex(
            RegistryStoreError, "LOCK_AUTHORITY_REQUIRED"
        ):
            _RegistryTransactionLog.create(
                self.dialogues,
                self.initial,
                self.locks,
                shared,
            )

        foreign = DomainLockManager(self.root_path / "foreign-locks")
        self.addCleanup(foreign.close)
        log = self.new_log()
        with foreign.registry_exclusive() as foreign_authority:
            with self.assertRaisesRegex(
                RegistryStoreError, "LOCK_AUTHORITY_REQUIRED"
            ):
                log.commit(commit_request, foreign_authority)
            with self.assertRaisesRegex(
                RegistryStoreError, "LOCK_AUTHORITY_REQUIRED"
            ):
                _RegistryTransactionLog.create(
                    self.dialogues,
                    self.initial,
                    self.locks,
                    foreign_authority,
                )
        log.close()

    def test_authority_is_revalidated_immediately_before_marker_publish(self):
        log = self.new_log()
        commit_request = request(create_decision(log.tip().state))
        real_assert = self.locks.assert_registry_authority
        calls = 0

        def expire_before_marker(authority, required):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise LockError("LOCK_AUTHORITY_INVALID")
            return real_assert(authority, required)

        with self.locks.registry_exclusive() as authority, mock.patch.object(
            self.locks,
            "assert_registry_authority",
            side_effect=expire_before_marker,
        ), self.assertRaisesRegex(
            RegistryStoreError, "LOCK_AUTHORITY_REQUIRED"
        ):
            log.commit(commit_request, authority)
        self.assertEqual(
            (), tuple((self.registry_path() / "transactions").iterdir())
        )
        self.assertEqual(2, len(tuple((self.registry_path() / "events").iterdir())))
        log.close()

        recovered = self.new_log()
        self.assertEqual(0, recovered.tip().state.registry_sequence)
        self.assertEqual(2, len(tuple((self.registry_path() / "quarantine").iterdir())))
        recovered.close()

    def test_entire_batch_encodes_and_fits_before_first_event_is_written(self):
        log = self.new_log()
        commit_request = request(create_decision(log.tip().state))
        real_encode = registry_store.encode_stored_registry_event
        calls = 0

        def fail_second(record):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ValueError("INJECTED_SECOND_EVENT_CODEC_FAILURE")
            return real_encode(record)

        with mock.patch.object(
            registry_store,
            "encode_stored_registry_event",
            side_effect=fail_second,
        ), self.assertRaisesRegex(
            RegistryStoreError, "INJECTED_SECOND_EVENT_CODEC_FAILURE"
        ):
            self.commit(log, commit_request)
        self.assertEqual((), tuple((self.registry_path() / "events").iterdir()))

        with mock.patch.object(
            registry_store,
            "MAX_RECORD_BYTES",
            1,
        ), self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_RECORD_SIZE_INVALID"
        ):
            self.commit(log, commit_request)
        self.assertEqual((), tuple((self.registry_path() / "events").iterdir()))
        self.assertEqual(
            (), tuple((self.registry_path() / "transactions").iterdir())
        )
        log.close()

    def test_live_authoritative_read_and_commit_reaudit_the_committed_prefix(self):
        log = self.new_log()
        created = self.commit(log, request(create_decision(log.tip().state)))
        event = (
            self.registry_path()
            / "events"
            / "00000000000000000001-evt.create.1.json"
        )
        tree = json.loads(event.read_bytes())
        tree["event_hash"] = digest("tampered")
        event.write_bytes(canonical_json_bytes(tree))

        with self.assertRaises(RegistryStoreError):
            log.read_committed()

        begin = BeginHandoff(
            "cmd.begin-audit",
            digest("begin-audit"),
            created.state.registry_sequence,
            created.state.generation,
            "handoff-audit",
            "dlg-a",
            ActivateTarget("dlg-b", digest("config-b")),
        )
        accepted = decide(created.state, begin)
        self.assertIsInstance(accepted, RegistryAccepted)
        assert isinstance(accepted, RegistryAccepted)
        with self.assertRaises(RegistryStoreError):
            self.commit(
                log,
                request(
                    accepted,
                    transaction_id="tx.begin-audit",
                    event_ids=("evt.begin-audit.1", "evt.begin-audit.2"),
                    expected_registry_sequence=created.state.registry_sequence,
                    expected_generation=created.state.generation,
                ),
            )
        log.close()

    def test_recovery_removes_provable_secure_fs_temporary_hardlinks(self):
        log = self.new_log()
        self.commit(log, request(create_decision(log.tip().state)))
        log.close()
        event_directory = self.registry_path() / "events"
        event = event_directory / "00000000000000000001-evt.create.1.json"
        temporary = event_directory / ("tmp-" + "a" * 32)
        os.link(event, temporary)
        self.assertEqual(2, event.stat().st_nlink)

        recovered = self.new_log()
        self.assertFalse(temporary.exists())
        self.assertEqual(1, event.stat().st_nlink)
        self.assertEqual(2, recovered.tip().state.registry_sequence)
        recovered.close()

    def test_cas_and_cross_transaction_identities_are_checked_after_idempotency(self):
        log = self.new_log()
        created = self.commit(log, request(create_decision(log.tip().state)))
        begin = BeginHandoff(
            "cmd.begin-cas",
            digest("begin-cas"),
            created.state.registry_sequence,
            created.state.generation,
            "handoff-cas",
            "dlg-a",
            ActivateTarget("dlg-b", digest("config-b")),
        )
        accepted = decide(created.state, begin)
        self.assertIsInstance(accepted, RegistryAccepted)
        assert isinstance(accepted, RegistryAccepted)

        stale_sequence = request(
            accepted,
            transaction_id="tx.begin-cas",
            event_ids=("evt.begin-cas.1", "evt.begin-cas.2"),
            expected_registry_sequence=created.state.registry_sequence - 1,
            expected_generation=created.state.generation,
        )
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_SEQUENCE_CONFLICT"
        ):
            self.commit(log, stale_sequence)
        stale_generation = replace(
            stale_sequence,
            expected_registry_sequence=created.state.registry_sequence,
            expected_generation=created.state.generation - 1,
        )
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_GENERATION_CONFLICT"
        ):
            self.commit(log, stale_generation)

        reused_transaction = replace(
            stale_sequence,
            transaction_id="tx.create",
            expected_registry_sequence=created.state.registry_sequence,
        )
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_INTEGRITY_ERROR"
        ):
            self.commit(log, reused_transaction)
        reused_event = replace(
            reused_transaction,
            transaction_id="tx.begin-cas",
            event_ids=("evt.create.1", "evt.begin-cas.2"),
        )
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_INTEGRITY_ERROR"
        ):
            self.commit(log, reused_event)

        replay = self.commit(
            log,
            request(
                create_decision(self.initial),
                transaction_id="tx.retry",
                event_ids=("evt.retry.1", "evt.retry.2"),
            ),
        )
        self.assertTrue(replay.replayed)
        log.close()

    def test_corrupt_projection_is_disposable_and_never_used_for_replay(self):
        log = self.new_log()
        outcome = self.commit(log, request(create_decision(log.tip().state)))
        log.close()
        (self.registry_path() / "state.json").write_bytes(b"not-json")

        recovered = self.new_log()
        self.assertEqual(outcome.state, recovered.tip().state)
        projected = json.loads(
            (self.registry_path() / "state.json").read_bytes()
        )
        self.assertEqual(2, projected["through_registry_sequence"])
        recovered.close()

    def test_missing_or_hash_tampered_marked_event_fails_closed(self):
        log = self.new_log()
        self.commit(log, request(create_decision(log.tip().state)))
        log.close()
        first = (
            self.registry_path()
            / "events"
            / "00000000000000000001-evt.create.1.json"
        )
        raw = first.read_bytes()
        first.unlink()
        with self.assertRaisesRegex(RegistryStoreError, "FILE_NOT_FOUND"):
            self.new_log()

        first.write_bytes(raw)
        first.chmod(0o600)
        tree = json.loads(raw)
        tree["event_hash"] = digest("tampered")
        first.write_bytes(canonical_json_bytes(tree))
        with self.assertRaisesRegex(
            RegistryStoreError, "STORED_REGISTRY_EVENT_HASH_MISMATCH"
        ):
            self.new_log()

    def test_marker_gap_and_invalid_reducer_batch_fail_closed(self):
        log = self.new_log()
        self.commit(log, request(create_decision(log.tip().state)))
        log.close()
        transaction_directory = self.registry_path() / "transactions"
        marker = next(transaction_directory.iterdir())
        future = transaction_directory / (
            "00000000000000000004-00000000000000000005-tx.future.json"
        )
        future.write_bytes(marker.read_bytes())
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_TRANSACTION_SEQUENCE_GAP"
        ):
            self.new_log()
        future.unlink()

        log = self.new_log()
        invalid_command_id = "cmd.invalid"
        invalid_body = digest("invalid")
        invalid = RegistryAccepted(
            (
                replace(
                    create_decision(self.initial).events[0],
                    command_id=invalid_command_id,
                    body_digest=invalid_body,
                    payload=DialogueCreated(
                        ActivateTarget("dlg-b", digest("config-b"))
                    ),
                ),
                replace(
                    create_decision(self.initial).events[1],
                    command_id=invalid_command_id,
                    body_digest=invalid_body,
                    payload=replace(
                        create_decision(self.initial).events[1].payload,
                        generation=log.tip().state.generation + 1,
                    ),
                ),
            )
        )
        with self.assertRaisesRegex(
            RegistryStoreError, "INVALID_REGISTRY_TRANSACTION"
        ):
            self.commit(
                log,
                request(
                    invalid,
                    transaction_id="tx.invalid",
                    event_ids=("evt.invalid.1", "evt.invalid.2"),
                    expected_registry_sequence=log.tip().state.registry_sequence,
                    expected_generation=log.tip().state.generation,
                ),
            )
        self.assertEqual(2, log.tip().state.registry_sequence)
        log.close()

    def test_read_cursor_and_request_envelopes_are_strict(self):
        log = self.new_log()
        decision = create_decision(log.tip().state)
        outcome = self.commit(log, request(decision))
        self.assertEqual(
            (outcome.transaction.events[1],),
            log.read_committed(after_sequence=1),
        )
        with self.assertRaisesRegex(RegistryStoreError, "INVALID_SEQUENCE"):
            log.read_committed(after_sequence=False)
        malformed = (
            replace(request(decision), event_ids=["one", "two"]),
            replace(request(decision), event_ids=("same", "same")),
            replace(request(decision), transaction_id="../escape"),
            replace(
                request(decision),
                accepted=RegistryAccepted((decision.events[0],)),
            ),
        )
        for item in malformed:
            with self.assertRaisesRegex(
                RegistryStoreError, "INVALID_REGISTRY_COMMIT_REQUEST"
            ):
                self.commit(log, item)
        log.close()

    def test_empty_tip_context_manager_and_closed_store_contract(self):
        log = self.new_log()
        self.assertIsNone(log.tip().last_event_id)
        with log as managed:
            self.assertEqual(0, managed.tip().state.registry_sequence)
        with self.assertRaisesRegex(RegistryStoreError, "REGISTRY_STORE_CLOSED"):
            log.tip()
        log.close()

    def test_constructor_and_factory_reject_invalid_configuration(self):
        with self.locks.registry_exclusive() as authority:
            with self.assertRaisesRegex(
                RegistryStoreError, "INVALID_REGISTRY_STORE_DIRECTORY"
            ):
                _RegistryTransactionLog.create(
                    object(), self.initial, self.locks, authority
                )
            with self.assertRaisesRegex(
                RegistryStoreError, "INVALID_REGISTRY_STORE_DIRECTORY"
            ):
                _RegistryTransactionLog(
                    object(), self.initial, self.locks, authority
                )
            with self.assertRaisesRegex(
                RegistryStoreError, "INVALID_REGISTRY_STORE_CONFIGURATION"
            ):
                _RegistryTransactionLog(
                    self.dialogues, self.initial, object(), authority
                )
            with self.assertRaisesRegex(
                RegistryStoreError, "INVALID_INITIAL_REGISTRY_STATE"
            ):
                _RegistryTransactionLog(
                    self.dialogues, object(), self.locks, authority
                )

        log = self.new_log()
        committed = self.commit(log, request(create_decision(log.tip().state)))
        log.close()
        with self.locks.registry_exclusive() as authority:
            with self.assertRaisesRegex(
                RegistryStoreError, "INVALID_INITIAL_REGISTRY_STATE"
            ):
                _RegistryTransactionLog(
                    self.dialogues, committed.state, self.locks, authority
                )

    def test_request_requires_one_coherent_command_identity(self):
        log = self.new_log()
        decision = create_decision(log.tip().state)
        mismatched_command = RegistryAccepted(
            (
                decision.events[0],
                replace(decision.events[1], command_id="cmd.other"),
            )
        )
        mismatched_body = RegistryAccepted(
            (
                decision.events[0],
                replace(decision.events[1], body_digest=digest("other")),
            )
        )
        for accepted in (mismatched_command, mismatched_body):
            with self.subTest(accepted=accepted), self.assertRaisesRegex(
                RegistryStoreError, "INVALID_REGISTRY_COMMIT_REQUEST"
            ):
                self.commit(log, request(accepted))
        log.close()

    def test_retry_before_recovery_reuses_events_and_collision_is_quarantined(self):
        fail_once = True

        def crash_once(step):
            nonlocal fail_once
            if step == "before_marker" and fail_once:
                fail_once = False
                raise InjectedCrash(step)

        log = self.new_log(crash_once)
        commit_request = request(create_decision(log.tip().state))
        with self.assertRaises(InjectedCrash):
            self.commit(log, commit_request)
        outcome = self.commit(log, commit_request)
        self.assertFalse(outcome.replayed)
        self.assertEqual(2, outcome.state.registry_sequence)
        log.close()

        self.tearDown()
        self.setUp()
        colliding = self.new_log()
        collision = (
            self.registry_path()
            / "events"
            / "00000000000000000001-evt.create.1.json"
        )
        collision.write_bytes(b"uncommitted collision")
        collision.chmod(0o600)
        committed = self.commit(
            colliding, request(create_decision(colliding.tip().state))
        )
        self.assertEqual(2, committed.state.registry_sequence)
        self.assertEqual(
            1, len(tuple((self.registry_path() / "quarantine").iterdir()))
        )
        colliding.close()

    def test_codec_builder_marker_publish_and_projection_errors_are_normalized(self):
        log = self.new_log()
        commit_request = request(create_decision(log.tip().state))
        with mock.patch.object(
            registry_store,
            "build_stored_registry_event",
            side_effect=ValueError("EVENT_BUILD_REJECTED"),
        ), self.assertRaisesRegex(RegistryStoreError, "EVENT_BUILD_REJECTED"):
            self.commit(log, commit_request)
        with mock.patch.object(
            registry_store,
            "build_registry_transaction_marker",
            side_effect=ValueError("MARKER_BUILD_REJECTED"),
        ), self.assertRaisesRegex(RegistryStoreError, "MARKER_BUILD_REJECTED"):
            self.commit(log, commit_request)

        real_write = SecureDirectory.write_immutable

        def fail_only_marker(directory, component, data):
            if component.endswith("-tx.create.json"):
                raise SecureFsError("FS_DURABILITY_FAILED")
            return real_write(directory, component, data)

        with mock.patch.object(
            SecureDirectory,
            "write_immutable",
            autospec=True,
            side_effect=fail_only_marker,
        ), self.assertRaisesRegex(RegistryStoreError, "FS_DURABILITY_FAILED"):
            self.commit(log, commit_request)
        self.assertEqual(
            (), tuple((self.registry_path() / "transactions").iterdir())
        )
        log.close()

        recovered = self.new_log()
        with mock.patch.object(
            SecureDirectory,
            "replace_derived",
            side_effect=SecureFsError("FS_DURABILITY_FAILED"),
        ):
            outcome = self.commit(
                recovered, request(create_decision(recovered.tip().state))
            )
        self.assertFalse(outcome.projection_current)
        self.assertEqual(2, outcome.state.registry_sequence)
        recovered.close()

    def test_marker_filename_reference_chain_and_state_tampering_fail_closed(self):
        log = self.new_log()
        outcome = self.commit(log, request(create_decision(log.tip().state)))
        log.close()
        marker_path = next((self.registry_path() / "transactions").iterdir())
        original_marker = marker_path.read_bytes()

        renamed = marker_path.with_name(
            "00000000000000000001-00000000000000000002-tx.renamed.json"
        )
        marker_path.rename(renamed)
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_TRANSACTION_FILENAME_MISMATCH"
        ):
            self.new_log()
        renamed.rename(marker_path)

        mutations = (
            (
                "REGISTRY_TRANSACTION_EVENT_MISMATCH",
                lambda tree: tree["events"][0].update(
                    {"event_digest": digest("different-reference")}
                ),
            ),
            (
                "REGISTRY_TRANSACTION_CHAIN_MISMATCH",
                lambda tree: tree.update({"registry_id": "registry-other"}),
            ),
            (
                "REGISTRY_TRANSACTION_EVENT_MISMATCH",
                lambda tree: tree.update({"command_id": "cmd.other"}),
            ),
            (
                "REGISTRY_TRANSACTION_STATE_MISMATCH",
                lambda tree: tree.update({"generation": 99}),
            ),
        )
        for expected, mutate in mutations:
            tree = json.loads(original_marker)
            mutate(tree)
            tree["marker_hash"] = ""
            tree["marker_hash"] = "sha256:" + hashlib.sha256(
                canonical_json_bytes(tree)
            ).hexdigest()
            marker_path.write_bytes(canonical_json_bytes(tree))
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(RegistryStoreError, expected):
                    self.new_log()
            marker_path.write_bytes(original_marker)

        second_path = (
            self.registry_path()
            / "events"
            / "00000000000000000002-evt.create.2.json"
        )
        original_second = second_path.read_bytes()
        disconnected = build_stored_registry_event(
            "registry-1", outcome.transaction.events[1], None
        )
        second_path.write_bytes(encode_stored_registry_event(disconnected))
        tree = json.loads(original_marker)
        tree["events"][1]["event_hash"] = disconnected.event_hash
        tree["marker_hash"] = ""
        tree["marker_hash"] = "sha256:" + hashlib.sha256(
            canonical_json_bytes(tree)
        ).hexdigest()
        marker_path.write_bytes(canonical_json_bytes(tree))
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_EVENT_CHAIN_MISMATCH"
        ):
            self.new_log()
        second_path.write_bytes(original_second)
        marker_path.write_bytes(original_marker)

    def test_recovery_normalizes_reducer_and_transaction_validation_failures(self):
        log = self.new_log()
        outcome = self.commit(log, request(create_decision(log.tip().state)))
        receipt = outcome.receipt
        with self.assertRaisesRegex(RegistryStoreError, "REGISTRY_RECEIPT_MISMATCH"):
            log._transaction_for_receipt(
                replace(receipt, event_ids=("evt.other.1", "evt.other.2"))
            )
        log.close()

        with mock.patch.object(
            registry_store,
            "validate_committed_registry_transaction",
            side_effect=ValueError("TRANSACTION_REJECTED"),
        ), self.assertRaisesRegex(RegistryStoreError, "TRANSACTION_REJECTED"):
            self.new_log()
        with mock.patch.object(
            registry_store,
            "reduce",
            side_effect=ValueError("REDUCER_REJECTED"),
        ), self.assertRaisesRegex(RegistryStoreError, "REDUCER_REJECTED"):
            self.new_log()

    def test_incremental_synchronization_detects_removed_and_gapped_markers(self):
        writer = self.new_log()
        stale = self.new_log()
        committed = self.commit(
            writer, request(create_decision(writer.tip().state))
        )
        stale._synchronize()
        self.assertEqual(committed.state, stale.tip().state)

        marker = next((self.registry_path() / "transactions").iterdir())
        held = self.registry_path() / "held-marker"
        marker.rename(held)
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_TRANSACTION_CHAIN_MISMATCH"
        ):
            stale._synchronize()
        held.rename(marker)
        writer.close()
        stale.close()

        malformed = self.registry_path() / "transactions" / "not-a-marker.json"
        malformed.write_bytes(b"{}")
        malformed.chmod(0o600)
        with self.assertRaisesRegex(
            RegistryStoreError, "REGISTRY_TRANSACTION_SEQUENCE_GAP"
        ):
            self.new_log()


if __name__ == "__main__":
    unittest.main()
