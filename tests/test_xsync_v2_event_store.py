from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

from xsync_v2 import event_store

from xsync_v2.domain import (
    CandidatesPresented,
    CommittedDialogueEvent,
    EvidenceCheck,
    EvidenceHealth,
    SessionDeactivationPrepared,
    SessionStarted,
    TopicStarted,
    TriggerBinding,
    TriggerKind,
    WorkDeadLettered,
    WorkFailed,
    WorkFailure,
    WorkFailureCategory,
    WorkRecoveryAction,
    WorkRecoveryRequested,
    WorkRequeued,
    WorkStatus,
    initial_dialogue_state,
)
from xsync_v2.event_codec import (
    MAX_RECORD_BYTES,
    ActorKind,
    DialogueActor,
    canonical_json_bytes,
    sha256_digest,
)
from xsync_v2.event_store import (
    DialogueCommitRequest,
    DialogueStoreError,
    _DialogueTransactionLog,
    EventMetadata,
    session_directory_component,
)
from xsync_v2.locking import DomainLockManager
from xsync_v2.secure_fs import SecureDirectory, SecureFsError
from xsync_v2.state_machine import reduce
from xsync_v2.work import WorkOrigin, derive_runnable_work
from tests.test_xsync_v2_state_machine import contract


ACTOR = DialogueActor(ActorKind.RUNTIME, "runtime.local")
META = EventMetadata("2026-08-16T12:00:00+08:00", ACTOR, None)


def digest(label):
    return sha256_digest(canonical_json_bytes({"request": label}))


def candidate_trigger(work_id="work-1"):
    return TriggerBinding(
        TriggerKind.TOPIC_CANDIDATES,
        work_id,
        "epoch-1",
        None,
        None,
        digest(f"input:{work_id}"),
        digest("evidence"),
    )


def first_event(event_id="event-1", command_id="command-1"):
    return CommittedDialogueEvent(
        event_id,
        1,
        0,
        1,
        command_id,
        SessionStarted(candidate_trigger()),
    )


def second_event(from_version=1, command_id="command-2"):
    trigger = candidate_trigger()
    return CommittedDialogueEvent(
        "event-2",
        2,
        from_version,
        from_version + 1,
        command_id,
        CandidatesPresented(("支付一致性",), trigger),
    )


def request(event, *, transaction="transaction-1", request_label="one"):
    return DialogueCommitRequest(
        "dialogue-1",
        transaction,
        digest(request_label),
        1,
        (event,),
        (META,),
    )


def failure_events(state, *, dead_letter=False, command_id="command-failure"):
    work = state.session_work
    if work is None:
        raise AssertionError("expected queued Session work")
    category = (
        WorkFailureCategory.HOST_PERMANENT
        if dead_letter
        else WorkFailureCategory.HOST_TRANSIENT
    )
    failure = WorkFailure(
        "failure-1",
        category,
        "HOST_TIMEOUT",
        digest("failure-proof"),
    )
    failed = CommittedDialogueEvent(
        "event-failed",
        state.sequence + 1,
        state.conversation_version,
        state.conversation_version,
        command_id,
        WorkFailed(work.work_id, work.trigger, work.attempt, failure),
    )
    if dead_letter:
        resolution = WorkDeadLettered(
            work.work_id,
            work.trigger,
            work.attempt,
            failure,
            (WorkRecoveryAction.RETRY,),
        )
        to_version = state.conversation_version + 1
    else:
        next_trigger = replace(work.trigger, work_id="work-retry-2")
        resolution = WorkRequeued(
            work.work_id,
            work.trigger,
            work.attempt,
            failure,
            next_trigger,
            work.attempt + 1,
        )
        to_version = state.conversation_version
    resolved = CommittedDialogueEvent(
        "event-failure-resolved",
        state.sequence + 2,
        state.conversation_version,
        to_version,
        command_id,
        resolution,
    )
    return failed, resolved


def batch_request(events, *, transaction="transaction-failure", label="failure"):
    return DialogueCommitRequest(
        "dialogue-1",
        transaction,
        digest(label),
        1,
        tuple(events),
        tuple(META for _event in events),
    )


class InjectedCrash(RuntimeError):
    pass


class HostFenceFailure(SecureFsError):
    pass


class EventStoreTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root_path = Path(self.temporary.name).resolve()
        self.root = SecureDirectory.open(self.root_path)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.locks = DomainLockManager(self.root_path / "locks")
        self.initial = initial_dialogue_state("dialogue-1", 1)

    def tearDown(self):
        self.locks.close()
        self.dialogues.close()
        self.root.close()
        self.temporary.cleanup()

    def new_log(self, fault_hook=lambda _step: None):
        with self.locks.semantic_session("dialogue-1") as authority:
            return _DialogueTransactionLog.create(
                self.dialogues,
                self.initial,
                self.locks,
                authority,
                fault_hook=fault_hook,
            )

    def commit(self, log, commit_request):
        with self.locks.semantic_session("dialogue-1") as authority:
            return log.commit(commit_request, authority)

    def session_path(self):
        return self.root_path / "dialogues" / session_directory_component(
            "dialogue-1"
        )

    def test_marker_is_the_linearization_point_and_replay_rebuilds_state(self):
        log = self.new_log()
        outcome = self.commit(log, request(first_event()))
        self.assertFalse(outcome.replayed)
        self.assertEqual(1, outcome.state.sequence)
        self.assertEqual((first_event(),), log.read_committed())
        log.close()

        (self.session_path() / "state.json").unlink()
        recovered = self.new_log()
        self.assertEqual(outcome.state, recovered.tip().state)
        self.assertEqual(outcome.receipt, recovered.tip().receipts[0])
        recovered.close()

    def test_crash_before_marker_leaves_no_fact_and_orphan_is_quarantined(self):
        def crash(step):
            if step == "before_marker":
                raise InjectedCrash(step)

        log = self.new_log(crash)
        with self.assertRaises(InjectedCrash):
            self.commit(log, request(first_event()))
        log.close()

        recovered = self.new_log()
        self.assertEqual(0, recovered.tip().state.sequence)
        self.assertEqual((), recovered.read_committed())
        quarantine = self.session_path() / "quarantine"
        self.assertEqual(1, len(tuple(quarantine.iterdir())))
        recovered.close()

        repeated = self.new_log(crash)
        with self.assertRaises(InjectedCrash):
            self.commit(repeated, request(first_event()))
        repeated.close()
        recovered_again = self.new_log()
        self.assertEqual(0, recovered_again.tip().state.sequence)
        self.assertEqual(1, len(tuple(quarantine.iterdir())))
        recovered_again.close()

    def test_crash_after_marker_recovers_exactly_once(self):
        def crash(step):
            if step == "marker_committed":
                raise InjectedCrash(step)

        log = self.new_log(crash)
        commit_request = request(first_event())
        with self.assertRaises(InjectedCrash):
            self.commit(log, commit_request)
        log.close()

        recovered = self.new_log()
        replay = self.commit(recovered, commit_request)
        self.assertTrue(replay.replayed)
        self.assertEqual(1, replay.state.sequence)
        self.assertEqual(1, len(recovered.read_committed()))
        recovered.close()

    def test_same_command_is_idempotent_and_different_body_conflicts(self):
        log = self.new_log()
        commit_request = request(first_event())
        first = self.commit(log, commit_request)
        second = self.commit(log, commit_request)
        self.assertTrue(second.replayed)
        self.assertEqual(first.receipt, second.receipt)
        changed = request(first_event(), request_label="different")
        with self.assertRaisesRegex(DialogueStoreError, "IDEMPOTENCY_CONFLICT"):
            self.commit(log, changed)
        self.assertEqual(1, log.tip().state.sequence)
        log.close()

    def test_multi_event_marker_is_atomic_and_old_retry_returns_old_state(self):
        log = self.new_log()
        event_one = first_event(command_id="command-batch")
        event_two = CommittedDialogueEvent(
            "event-2",
            2,
            1,
            2,
            "command-batch",
            CandidatesPresented(("支付一致性",), candidate_trigger()),
        )
        batched = DialogueCommitRequest(
            "dialogue-1",
            "transaction-batch",
            digest("batch"),
            1,
            (event_one, event_two),
            (META, META),
        )
        first = self.commit(log, batched)
        self.assertEqual(("event-1", "event-2"), first.receipt.event_ids)
        self.assertEqual(2, first.state.sequence)

        # A later command moves the live tip, but replay of the first key must
        # still return the original committed response state.
        topic_contract = contract()
        topic_event = CommittedDialogueEvent(
            "event-3",
            3,
            2,
            3,
            "command-topic",
            TopicStarted(
                topic_contract,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    digest("evidence"),
                ),
                TriggerBinding(
                    TriggerKind.INITIAL_TURN,
                    "work-topic",
                    "epoch-1",
                    None,
                    topic_contract.contract_digest,
                    digest("input-topic"),
                    digest("evidence"),
                ),
            ),
        )
        self.commit(
            log,
            request(
                topic_event,
                transaction="transaction-topic",
                request_label="topic",
            ),
        )
        replay = self.commit(log, batched)
        self.assertTrue(replay.replayed)
        self.assertEqual(2, replay.state.sequence)
        self.assertEqual(3, log.tip().state.sequence)
        log.close()

    def test_work_failure_marker_commits_only_a_complete_resolution_pair(self):
        log = self.new_log()
        started = self.commit(log, request(first_event()))
        failed, requeued = failure_events(started.state)

        outcome = self.commit(log, batch_request((failed, requeued)))

        self.assertEqual(3, outcome.state.sequence)
        self.assertEqual(1, outcome.state.conversation_version)
        self.assertIsNotNone(outcome.state.session_work)
        self.assertEqual(WorkStatus.QUEUED, outcome.state.session_work.status)
        self.assertEqual(2, outcome.state.session_work.attempt)
        log.close()

        recovered = self.new_log()
        self.assertEqual(outcome.state, recovered.tip().state)
        recovered.close()

    def test_requeued_work_restarts_into_the_canonical_work_projection(self):
        log = self.new_log()
        started = self.commit(log, request(first_event()))
        failed, requeued = failure_events(started.state)
        committed = self.commit(log, batch_request((failed, requeued)))
        log.close()

        recovered = self.new_log()
        replayed_events = recovered.read_committed(after_sequence=1)
        self.assertEqual(committed.events, replayed_events)
        resolution = replayed_events[-1]
        self.assertIs(type(resolution.payload), WorkRequeued)
        assert isinstance(resolution.payload, WorkRequeued)
        projected = derive_runnable_work(
            recovered.tip().state,
            WorkOrigin(
                resolution.event_id,
                resolution.sequence,
                resolution.payload.next_trigger,
            ),
        )
        self.assertIsNotNone(projected)
        assert projected is not None
        current = recovered.tip().state.session_work
        assert current is not None
        self.assertEqual(current.work_id, projected.work_id)
        self.assertEqual(2, current.attempt)
        recovered.close()

    def test_recovered_work_restarts_into_the_canonical_work_projection(self):
        log = self.new_log()
        started = self.commit(log, request(first_event()))
        failed, dead_lettered = failure_events(
            started.state,
            dead_letter=True,
        )
        dead = self.commit(
            log,
            batch_request(
                (failed, dead_lettered),
                transaction="transaction-dead",
                label="dead",
            ),
        )
        dead_work = dead.state.session_work
        assert dead_work is not None
        recovery_trigger = replace(
            dead_work.trigger,
            work_id="work-explicit-recovery",
        )
        recovery_event = CommittedDialogueEvent(
            "event-recovery",
            dead.state.sequence + 1,
            dead.state.conversation_version,
            dead.state.conversation_version + 1,
            "command-recovery",
            WorkRecoveryRequested(
                dead_work.work_id,
                WorkRecoveryAction.RETRY,
                recovery_trigger,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    dead_work.trigger.evidence_digest,
                ),
            ),
        )
        committed = self.commit(
            log,
            request(
                recovery_event,
                transaction="transaction-recovery",
                request_label="recovery",
            ),
        )
        log.close()

        recovered = self.new_log()
        replayed = recovered.read_committed(after_sequence=dead.state.sequence)
        self.assertEqual(committed.events, replayed)
        projected = derive_runnable_work(
            recovered.tip().state,
            WorkOrigin(
                recovery_event.event_id,
                recovery_event.sequence,
                recovery_trigger,
            ),
        )
        self.assertIsNotNone(projected)
        assert projected is not None
        current = recovered.tip().state.session_work
        assert current is not None
        self.assertEqual(current.work_id, projected.work_id)
        self.assertEqual(1, current.attempt)
        recovered.close()

    def test_noncanonical_requeue_trigger_fails_before_event_publication(self):
        log = self.new_log()
        started = self.commit(log, request(first_event()))
        failed, requeued = failure_events(started.state)
        assert isinstance(requeued.payload, WorkRequeued)
        invalid_triggers = (
            replace(requeued.payload.next_trigger, work_id="../escape"),
            replace(requeued.payload.next_trigger, work_id="x" * 129),
            replace(
                requeued.payload.next_trigger,
                input_digest="sha256:not-a-digest",
            ),
        )
        for index, invalid in enumerate(invalid_triggers):
            malformed = replace(
                requeued,
                payload=replace(requeued.payload, next_trigger=invalid),
            )
            with self.subTest(trigger=invalid), self.assertRaisesRegex(
                DialogueStoreError,
                "INVALID_EVENT_ENVELOPE",
            ):
                self.commit(
                    log,
                    batch_request(
                        (failed, malformed),
                        transaction=f"transaction-invalid-trigger-{index}",
                        label=f"invalid-trigger-{index}",
                    ),
                )
        self.assertEqual(1, log.tip().state.sequence)
        self.assertEqual(
            (),
            tuple((self.session_path() / "events").glob("event-000*02.json")),
        )
        log.close()

    def test_work_dead_letter_pair_advances_the_shared_version_once(self):
        log = self.new_log()
        started = self.commit(log, request(first_event()))
        failed, dead_lettered = failure_events(
            started.state,
            dead_letter=True,
        )

        outcome = self.commit(
            log,
            batch_request(
                (failed, dead_lettered),
                transaction="transaction-dead-letter",
                label="dead-letter",
            ),
        )

        self.assertEqual(3, outcome.state.sequence)
        self.assertEqual(2, outcome.state.conversation_version)
        self.assertIsNotNone(outcome.state.session_work)
        self.assertEqual(
            WorkStatus.DEAD_LETTER,
            outcome.state.session_work.status,
        )
        log.close()

    def test_work_failure_batch_rejects_partial_wrong_order_or_wrong_identity(self):
        log = self.new_log()
        started = self.commit(log, request(first_event()))
        failed, requeued = failure_events(started.state)
        wrong_failure = replace(
            requeued,
            payload=replace(
                requeued.payload,
                failure=replace(
                    requeued.payload.failure,
                    failure_id="failure-other",
                ),
            ),
        )
        resolution_first = (
            replace(requeued, sequence=2, event_id="event-resolution-first"),
            replace(failed, sequence=3, event_id="event-failed-second"),
        )
        cross_command = (
            failed,
            replace(requeued, command_id="command-other"),
        )
        malformed = (
            ("orphan-failed", (failed,)),
            ("orphan-resolution", (requeued,)),
            ("wrong-order", resolution_first),
            ("wrong-failure", (failed, wrong_failure)),
            ("cross-command", cross_command),
        )
        for label, events in malformed:
            with self.subTest(label=label), self.assertRaisesRegex(
                DialogueStoreError,
                "INVALID_WORK_FAILURE_BATCH",
            ):
                self.commit(
                    log,
                    batch_request(
                        events,
                        transaction=f"transaction-{label}",
                        label=label,
                    ),
                )

        self.assertEqual(1, log.tip().state.sequence)
        self.assertEqual(
            (),
            tuple((self.session_path() / "events").glob("event-000*02.json")),
        )
        log.close()

    def test_work_failure_sequence_and_versions_use_the_shared_canonical_rule(self):
        log = self.new_log()
        started = self.commit(log, request(first_event()))
        failed, requeued = failure_events(started.state)
        malformed = (
            (
                "failed-delta",
                (replace(failed, to_version=2), requeued),
                "EVENT_VERSION_CONFLICT",
            ),
            (
                "requeued-delta",
                (failed, replace(requeued, to_version=2)),
                "EVENT_VERSION_CONFLICT",
            ),
            (
                "version-gap",
                (
                    failed,
                    replace(requeued, from_version=2, to_version=2),
                ),
                "EVENT_VERSION_CONFLICT",
            ),
            (
                "sequence-gap",
                (failed, replace(requeued, sequence=4)),
                "EVENT_SEQUENCE_GAP",
            ),
        )
        for label, events, expected in malformed:
            with self.subTest(label=label), self.assertRaisesRegex(
                DialogueStoreError,
                expected,
            ):
                self.commit(
                    log,
                    batch_request(
                        events,
                        transaction=f"transaction-version-{label}",
                        label=f"version-{label}",
                    ),
                )
        self.assertEqual(1, log.tip().state.sequence)
        log.close()

    def test_replay_rejects_a_marker_that_exposes_failed_as_the_tip(self):
        log = self.new_log()
        started = self.commit(log, request(first_event()))
        failed, _requeued = failure_events(started.state)
        orphan = batch_request(
            (failed,),
            transaction="transaction-orphan-failed",
            label="orphan-failed",
        )
        with mock.patch.object(
            event_store,
            "_validate_event_batch_shape",
        ):
            self.commit(log, orphan)
        log.close()

        with self.assertRaisesRegex(
            DialogueStoreError,
            "INVALID_WORK_FAILURE_BATCH",
        ):
            self.new_log()

    def test_stale_version_and_expired_authority_fail_closed(self):
        log = self.new_log()
        self.commit(log, request(first_event()))
        stale = request(
            second_event(from_version=0),
            transaction="transaction-2",
            request_label="two",
        )
        with self.assertRaisesRegex(DialogueStoreError, "EVENT_VERSION_CONFLICT"):
            self.commit(log, stale)

        with self.locks.semantic_session("dialogue-1") as authority:
            captured = authority
        valid = request(
            second_event(),
            transaction="transaction-2",
            request_label="two",
        )
        with self.assertRaisesRegex(DialogueStoreError, "LOCK_AUTHORITY_REQUIRED"):
            log.commit(valid, captured)
        log.close()

    def test_generation_and_durable_identities_are_bound_before_writes(self):
        log = self.new_log()
        wrong_generation = DialogueCommitRequest(
            "dialogue-1",
            "transaction-wrong-generation",
            digest("wrong-generation"),
            999,
            (first_event(),),
            (META,),
        )
        with self.assertRaisesRegex(
            DialogueStoreError,
            "REGISTRY_GENERATION_MISMATCH",
        ):
            self.commit(log, wrong_generation)
        self.assertEqual((), log.read_committed())

        self.commit(log, request(first_event()))
        duplicate_event = CommittedDialogueEvent(
            "event-1",
            2,
            1,
            2,
            "command-2",
            CandidatesPresented(("支付一致性",), candidate_trigger()),
        )
        with self.assertRaisesRegex(DialogueStoreError, "DUPLICATE_EVENT_ID"):
            self.commit(
                log,
                request(
                    duplicate_event,
                    transaction="transaction-2",
                    request_label="duplicate-event",
                ),
            )
        with self.assertRaisesRegex(
            DialogueStoreError,
            "DUPLICATE_TRANSACTION_ID",
        ):
            self.commit(
                log,
                request(
                    second_event(),
                    transaction="transaction-1",
                    request_label="duplicate-transaction",
                ),
            )
        self.assertEqual(1, log.tip().state.sequence)
        log.close()

    def test_entire_batch_is_encoded_and_bounded_before_any_event_is_written(self):
        log = self.new_log()
        event_one = first_event(command_id="command-batch")
        event_two = CommittedDialogueEvent(
            "event-2",
            2,
            1,
            2,
            "command-batch",
            CandidatesPresented(("支付一致性",), candidate_trigger()),
        )
        malformed_metadata = DialogueCommitRequest(
            "dialogue-1",
            "transaction-malformed",
            digest("malformed"),
            1,
            (event_one, event_two),
            (META, EventMetadata("not-a-timestamp", ACTOR, None)),
        )
        with self.assertRaises(DialogueStoreError):
            self.commit(log, malformed_metadata)
        self.assertEqual((), log.read_committed())
        self.assertEqual((), tuple((self.session_path() / "events").iterdir()))

        oversized_event = CommittedDialogueEvent(
            "event-oversized",
            2,
            1,
            2,
            "command-batch",
            CandidatesPresented(
                ("x" * MAX_RECORD_BYTES,),
                candidate_trigger(),
            ),
        )
        oversized_request = DialogueCommitRequest(
            "dialogue-1",
            "transaction-oversized",
            digest("oversized"),
            1,
            (event_one, oversized_event),
            (META, META),
        )
        with self.assertRaisesRegex(DialogueStoreError, "FILE_TOO_LARGE"):
            self.commit(log, oversized_request)
        self.assertEqual((), log.read_committed())
        log.close()

    def test_internal_transaction_temporary_is_recovered_before_replay(self):
        log = self.new_log()
        self.commit(log, request(first_event()))
        log.close()
        transactions = self.session_path() / "transactions"
        temporary = transactions / ("tmp-" + "d" * 32)
        temporary.write_bytes(b"orphan")
        temporary.chmod(0o600)

        recovered = self.new_log()
        self.assertFalse(temporary.exists())
        self.assertEqual(1, recovered.tip().state.sequence)
        recovered.close()

    def test_fenced_generation_seals_the_dialogue_log_across_restart(self):
        log = self.new_log()
        self.commit(log, request(first_event()))
        fenced_event = CommittedDialogueEvent(
            "event-fenced",
            2,
            1,
            2,
            "command-fenced",
            SessionDeactivationPrepared(
                "handoff-1",
                2,
                candidate_trigger(),
            ),
        )
        fenced_request = DialogueCommitRequest(
            "dialogue-1",
            "transaction-fenced",
            digest("fenced"),
            2,
            (fenced_event,),
            (META,),
        )
        outcome = self.commit(log, fenced_request)
        self.assertEqual(2, outcome.state.sequence)
        self.assertEqual(2, log.tip().last_registry_generation)
        log.close()

        recovered = self.new_log()
        self.assertEqual(2, recovered.tip().last_registry_generation)
        later = CommittedDialogueEvent(
            "event-late",
            3,
            2,
            3,
            "command-late",
            SessionStarted(candidate_trigger("work-late")),
        )
        with self.assertRaisesRegex(DialogueStoreError, "SESSION_DEACTIVATED"):
            self.commit(
                recovered,
                request(
                    later,
                    transaction="transaction-late",
                    request_label="late",
                ),
            )
        recovered.close()

    def test_authority_is_revalidated_immediately_before_marker_publish(self):
        holder = None

        def release_at_marker(step):
            nonlocal holder
            if step == "before_marker":
                holder.__exit__(None, None, None)
                holder = None

        log = self.new_log(release_at_marker)
        holder = self.locks.semantic_session("dialogue-1")
        authority = holder.__enter__()
        with self.assertRaisesRegex(
            DialogueStoreError,
            "LOCK_AUTHORITY_REQUIRED",
        ):
            log.commit(request(first_event()), authority)
        log.close()

        recovered = self.new_log()
        self.assertEqual(0, recovered.tip().state.sequence)
        recovered.close()

    def test_marker_guard_runs_after_events_and_marker_staging_exactly_once(self):
        log = self.new_log()
        calls = 0
        event_name = "event-00000000000000000001.json"
        marker_name = "transaction-00000000000000000001.json"
        event_directory = self.session_path() / "events"
        transaction_directory = self.session_path() / "transactions"

        with self.locks.semantic_session("dialogue-1") as authority:

            def host_fence(current_authority):
                nonlocal calls
                calls += 1
                self.assertIs(authority, current_authority)
                self.assertTrue((event_directory / event_name).is_file())
                self.assertFalse((transaction_directory / marker_name).exists())
                staged = tuple(transaction_directory.iterdir())
                self.assertEqual(1, len(staged))
                self.assertRegex(staged[0].name, r"^tmp-[0-9a-f]{32}$")

            outcome = log.commit(
                request(first_event()),
                authority,
                marker_publication_guard=host_fence,
            )
            replay = log.commit(
                request(first_event()),
                authority,
                marker_publication_guard=host_fence,
            )

        self.assertFalse(outcome.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(1, calls)
        self.assertTrue((transaction_directory / marker_name).is_file())
        self.assertEqual(1, log.tip().state.sequence)
        log.close()

    def test_marker_guard_failure_is_preserved_and_orphan_recovers(self):
        log = self.new_log()
        failure = HostFenceFailure("HOST_FENCE_REJECTED")
        event_directory = self.session_path() / "events"
        transaction_directory = self.session_path() / "transactions"

        def reject(_authority):
            self.assertTrue(
                (event_directory / "event-00000000000000000001.json").is_file()
            )
            raise failure

        with self.locks.semantic_session("dialogue-1") as authority:
            with self.assertRaises(HostFenceFailure) as raised:
                log.commit(
                    request(first_event()),
                    authority,
                    marker_publication_guard=reject,
                )

        self.assertIs(failure, raised.exception)
        self.assertEqual((), tuple(transaction_directory.iterdir()))
        self.assertEqual(0, log.tip().state.sequence)
        self.assertEqual((), log.read_committed())
        log.close()

        recovered = self.new_log()
        self.assertEqual(0, recovered.tip().state.sequence)
        self.assertEqual((), recovered.read_committed())
        self.assertEqual(
            1,
            len(tuple((self.session_path() / "quarantine").iterdir())),
        )
        recovered.close()

    def test_marker_guard_refuses_authority_namespace_lost_after_staging(self):
        log = self.new_log()
        lock_path = self.root_path / "locks"
        moved_lock_path = self.root_path / "locks-before-replacement"
        transaction_directory = self.session_path() / "transactions"
        holder = self.locks.semantic_session("dialogue-1")
        authority = holder.__enter__()

        def replace_lock_namespace(_authority):
            self.assertEqual(1, len(tuple(transaction_directory.iterdir())))
            lock_path.rename(moved_lock_path)
            lock_path.mkdir(mode=0o700)

        try:
            with self.assertRaisesRegex(
                DialogueStoreError,
                "LOCK_AUTHORITY_REQUIRED",
            ):
                log.commit(
                    request(first_event()),
                    authority,
                    marker_publication_guard=replace_lock_namespace,
                )
        finally:
            holder.__exit__(None, None, None)

        self.assertEqual((), tuple(transaction_directory.iterdir()))
        self.assertEqual(0, log.tip().state.sequence)
        log.close()

    def test_invalid_marker_guard_is_rejected_before_event_publication(self):
        log = self.new_log()
        with self.locks.semantic_session("dialogue-1") as authority:
            with self.assertRaises(TypeError):
                log.commit(
                    request(first_event()),
                    authority,
                    lambda _authority: None,  # type: ignore[call-arg]
                )
            with self.assertRaisesRegex(
                DialogueStoreError,
                "INVALID_MARKER_PUBLICATION_GUARD",
            ):
                log.commit(
                    request(first_event()),
                    authority,
                    marker_publication_guard=object(),  # type: ignore[arg-type]
                )
        self.assertEqual((), tuple((self.session_path() / "events").iterdir()))
        log.close()

    def test_tampered_committed_event_and_marker_gap_fail_closed(self):
        log = self.new_log()
        self.commit(log, request(first_event()))
        log.close()
        event_path = self.session_path() / "events" / "event-00000000000000000001.json"
        tree = __import__("json").loads(event_path.read_bytes())
        tree["event_hash"] = "sha256:" + "0" * 64
        event_path.write_bytes(canonical_json_bytes(tree))
        with self.assertRaisesRegex(DialogueStoreError, "STORED_EVENT_HASH_MISMATCH"):
            self.new_log()

        # Restore in a fresh project and inject a future marker name.
        self.tearDown()
        self.setUp()
        log = self.new_log()
        self.commit(log, request(first_event()))
        log.close()
        transactions = self.session_path() / "transactions"
        raw = (transactions / "transaction-00000000000000000001.json").read_bytes()
        (transactions / "transaction-00000000000000000003.json").write_bytes(raw)
        with self.assertRaisesRegex(DialogueStoreError, "TRANSACTION_SEQUENCE_GAP"):
            self.new_log()

    def test_live_commit_reaudits_prefix_and_marker_cannot_relabel_command(self):
        log = self.new_log()
        self.commit(log, request(first_event()))
        event_path = self.session_path() / "events" / "event-00000000000000000001.json"
        original_event = event_path.read_bytes()
        event_tree = __import__("json").loads(original_event)
        event_tree["event_hash"] = "sha256:" + "0" * 64
        event_path.write_bytes(canonical_json_bytes(event_tree))
        with self.assertRaisesRegex(
            DialogueStoreError,
            "STORED_EVENT_HASH_MISMATCH",
        ):
            self.commit(
                log,
                request(
                    second_event(),
                    transaction="transaction-2",
                    request_label="two",
                ),
            )
        log.close()

        event_path.write_bytes(original_event)
        marker_path = (
            self.session_path()
            / "transactions"
            / "transaction-00000000000000000001.json"
        )
        marker_tree = __import__("json").loads(marker_path.read_bytes())
        marker_tree["command_id"] = "command-relabeled"
        marker_tree["marker_hash"] = ""
        marker_tree["marker_hash"] = sha256_digest(
            canonical_json_bytes(marker_tree)
        )
        marker_path.write_bytes(canonical_json_bytes(marker_tree))
        with self.assertRaisesRegex(DialogueStoreError, "EVENT_CHAIN_MISMATCH"):
            self.new_log()

    def test_empty_tip_context_manager_and_closed_store_contract(self):
        log = self.new_log()
        self.assertIsNone(log.tip().last_event_id)
        self.assertEqual((), log.read_committed(after_sequence=1))
        with self.assertRaisesRegex(DialogueStoreError, "INVALID_SEQUENCE"):
            log.read_committed(after_sequence=False)
        log.close()
        log.close()
        with self.assertRaisesRegex(DialogueStoreError, "DIALOGUE_STORE_CLOSED"):
            log.tip()

        with self.new_log() as managed:
            self.assertEqual(0, managed.tip().state.sequence)
        with self.assertRaisesRegex(DialogueStoreError, "DIALOGUE_STORE_CLOSED"):
            managed.recover(object())

    def test_constructor_and_factory_reject_invalid_configuration(self):
        with self.locks.semantic_session("dialogue-1") as authority:
            with self.assertRaisesRegex(
                DialogueStoreError, "INVALID_STORE_DIRECTORY"
            ):
                _DialogueTransactionLog.create(
                    object(), self.initial, self.locks, authority
                )
            with self.assertRaisesRegex(
                DialogueStoreError, "INVALID_INITIAL_STATE"
            ):
                _DialogueTransactionLog.create(
                    self.dialogues, object(), self.locks, authority
                )
            with self.assertRaisesRegex(
                DialogueStoreError, "INVALID_STORE_DIRECTORY"
            ):
                _DialogueTransactionLog(
                    object(), self.initial, self.locks, authority
                )
            with self.assertRaisesRegex(
                DialogueStoreError, "INVALID_STORE_CONFIGURATION"
            ):
                _DialogueTransactionLog(
                    self.dialogues,
                    self.initial,
                    object(),
                    authority,
                )

        active = reduce(self.initial, first_event())
        with self.locks.semantic_session("dialogue-1") as authority:
            with self.assertRaisesRegex(
                DialogueStoreError, "INVALID_INITIAL_STATE"
            ):
                _DialogueTransactionLog(
                    self.dialogues,
                    active,
                    self.locks,
                    authority,
                )
        with self.assertRaisesRegex(DialogueStoreError, "INVALID_SESSION_ID"):
            session_directory_component("")

    def test_request_envelope_batch_identity_and_generation_are_strict(self):
        log = self.new_log()
        base = request(first_event())
        invalid = (
            object(),
            DialogueCommitRequest(
                "other-session",
                base.transaction_id,
                base.request_digest,
                base.registry_generation,
                base.events,
                base.metadata,
            ),
            DialogueCommitRequest(
                base.session_id,
                base.transaction_id,
                base.request_digest,
                base.registry_generation,
                base.events,
                (),
            ),
            DialogueCommitRequest(
                base.session_id,
                base.transaction_id,
                base.request_digest,
                base.registry_generation,
                (
                    first_event(command_id="command-1"),
                    second_event(command_id="command-2"),
                ),
                (META, META),
            ),
        )
        for malformed in invalid:
            with self.subTest(malformed=malformed), self.assertRaisesRegex(
                DialogueStoreError, "INVALID_COMMIT_REQUEST"
            ), self.locks.semantic_session("dialogue-1") as authority:
                log.commit(malformed, authority)

        duplicate_batch = DialogueCommitRequest(
            "dialogue-1",
            "transaction-duplicate-batch",
            digest("duplicate-batch"),
            1,
            (
                first_event("same-event", "same-command"),
                CommittedDialogueEvent(
                    "same-event",
                    2,
                    1,
                    2,
                    "same-command",
                    CandidatesPresented(("支付一致性",), candidate_trigger()),
                ),
            ),
            (META, META),
        )
        with self.assertRaisesRegex(DialogueStoreError, "DUPLICATE_EVENT_ID"):
            self.commit(log, duplicate_batch)

        fenced_at_base = DialogueCommitRequest(
            "dialogue-1",
            "transaction-fenced-at-base",
            digest("fenced-at-base"),
            1,
            (
                CommittedDialogueEvent(
                    "event-fenced",
                    1,
                    0,
                    1,
                    "command-fenced",
                    SessionDeactivationPrepared(
                        "handoff-1", 2, candidate_trigger()
                    ),
                ),
            ),
            (META,),
        )
        with self.assertRaisesRegex(
            DialogueStoreError, "REGISTRY_GENERATION_MISMATCH"
        ):
            self.commit(log, fenced_at_base)

        normal_at_fenced_generation = DialogueCommitRequest(
            "dialogue-1",
            "transaction-normal-at-fence",
            digest("normal-at-fence"),
            2,
            (first_event(),),
            (META,),
        )
        with self.assertRaisesRegex(
            DialogueStoreError, "REGISTRY_GENERATION_MISMATCH"
        ):
            self.commit(log, normal_at_fenced_generation)
        log.close()

    def test_retry_before_recovery_reuses_identical_or_replaces_colliding_orphan(self):
        fail_once = True

        def crash_once(step):
            nonlocal fail_once
            if step == "before_marker" and fail_once:
                fail_once = False
                raise InjectedCrash(step)

        log = self.new_log(crash_once)
        commit_request = request(first_event())
        with self.assertRaises(InjectedCrash):
            self.commit(log, commit_request)
        outcome = self.commit(log, commit_request)
        self.assertFalse(outcome.replayed)
        self.assertEqual(1, outcome.state.sequence)
        log.close()

        self.tearDown()
        self.setUp()
        colliding = self.new_log()
        event_path = (
            self.session_path() / "events" / "event-00000000000000000001.json"
        )
        event_path.write_bytes(b"uncommitted collision")
        event_path.chmod(0o600)
        outcome = self.commit(colliding, request(first_event()))
        self.assertEqual(1, outcome.state.sequence)
        self.assertEqual(
            1, len(tuple((self.session_path() / "quarantine").iterdir()))
        )
        colliding.close()

    def test_marker_is_preencoded_and_projection_failure_does_not_undo_commit(self):
        log = self.new_log()
        with mock.patch.object(
            event_store,
            "encode_transaction_marker",
            return_value=b"x" * (MAX_RECORD_BYTES + 1),
        ), self.assertRaisesRegex(DialogueStoreError, "FILE_TOO_LARGE"):
            self.commit(log, request(first_event()))
        self.assertEqual((), log.read_committed())

        with mock.patch.object(
            SecureDirectory,
            "replace_derived",
            side_effect=event_store.SecureFsError("FS_DURABILITY_FAILED"),
        ):
            outcome = self.commit(log, request(first_event()))
        self.assertFalse(outcome.projection_current)
        self.assertEqual(1, log.tip().state.sequence)
        log.close()

    def test_stale_reader_synchronizes_when_a_marker_appears(self):
        writer = self.new_log()
        stale = self.new_log()
        committed = self.commit(writer, request(first_event()))
        self.assertEqual(committed.state, stale.tip().state)
        self.assertEqual((first_event(),), stale.read_committed())
        writer.close()
        stale.close()

    def test_recovery_normalizes_marker_reference_reducer_and_snapshot_failures(self):
        log = self.new_log()
        self.commit(log, request(first_event()))
        log.close()
        marker_path = (
            self.session_path()
            / "transactions"
            / "transaction-00000000000000000001.json"
        )
        original = marker_path.read_bytes()

        mutations = (
            ("TRANSACTION_EVENT_MISMATCH", {"event_id": "other-event"}, None),
            ("TRANSACTION_CHAIN_MISMATCH", None, {"session_id": "other-session"}),
            (
                "INVALID_STATE_SNAPSHOT",
                None,
                {"state_digest": "sha256:" + "9" * 64},
            ),
        )
        for expected, event_change, marker_change in mutations:
            tree = __import__("json").loads(original)
            if event_change is not None:
                tree["events"][0].update(event_change)
            if marker_change is not None:
                tree.update(marker_change)
            tree["marker_hash"] = ""
            tree["marker_hash"] = sha256_digest(canonical_json_bytes(tree))
            marker_path.write_bytes(canonical_json_bytes(tree))
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(DialogueStoreError, expected):
                    self.new_log()
            marker_path.write_bytes(original)

        with mock.patch.object(
            event_store, "reduce", side_effect=ValueError("REDUCER_REJECTED")
        ), self.assertRaisesRegex(DialogueStoreError, "REDUCER_REJECTED"):
            self.new_log()
        with mock.patch.object(
            event_store,
            "build_state_snapshot",
            side_effect=ValueError("SNAPSHOT_REJECTED"),
        ), self.assertRaisesRegex(DialogueStoreError, "SNAPSHOT_REJECTED"):
            self.new_log()


if __name__ == "__main__":
    unittest.main()
