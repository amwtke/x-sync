import hashlib
import json
import multiprocessing
import threading
import unittest
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

# isort: split
from xsync_v2 import lease_store as lease_store_module
from xsync_v2.coordinator import DialogueCoordinator, DialogueSessionConfig
from xsync_v2.domain import (
    ConversationPhase,
    CurrentWorkState,
    DialogueState,
    EvidenceCheck,
    EvidenceHealth,
    SessionLifecycle,
    SessionStarted,
    TriggerBinding,
    TriggerKind,
    WorkStatus,
    initial_dialogue_state,
)
from xsync_v2.event_store import (
    _DialogueTransactionLog,
    session_directory_component,
)
from xsync_v2.lease_store import (
    AuthoritativeWorkSnapshot,
    ClaimRequest,
    CurrentRunnableWork,
    CurrentWorkObservation,
    LeaseExhaustionProof,
    LeaseOperation,
    LeaseRecord,
    LeaseStore,
    LeaseStoreError,
    PublishFence,
    ReclaimRequest,
    RenewRequest,
    RuntimeAuthorityCheck,
)
from xsync_v2.locking import DomainLockManager, SessionLockAuthority
from xsync_v2.registry import (
    DialogueRegistrationStatus,
    RegisteredDialogue,
    RegistryState,
)
from xsync_v2.secure_fs import SecureDirectory, SecureFsError
from xsync_v2.work import WorkOrigin, derive_runnable_work
from xsync_v2.work_identity import derive_canonical_work_id


def digest(label: str) -> str:
    return f"sha256:{hashlib.sha256(label.encode()).hexdigest()}"


def waiting_snapshot(
    *,
    event_id: str = "event-trigger-1",
    event_sequence: int = 1,
    state_sequence: int = 1,
    trigger_work_id: str = "trigger-token-1",
) -> tuple[DialogueState, WorkOrigin]:
    trigger = TriggerBinding(
        TriggerKind.TOPIC_CANDIDATES,
        trigger_work_id,
        "trigger-epoch",
        None,
        None,
        digest("input"),
        digest("evidence"),
    )
    work = CurrentWorkState(
        derive_canonical_work_id(
            session_id="dlg-1",
            trigger_event_id=event_id,
            trigger_event_sequence=event_sequence,
            trigger=trigger,
        ),
        event_id,
        event_sequence,
        trigger,
        1,
        WorkStatus.QUEUED,
    )
    state = DialogueState(
        "dlg-1",
        1,
        state_sequence,
        1,
        SessionLifecycle.OPEN,
        ConversationPhase.WAITING_HOST,
        (),
        work,
        None,
        (),
    )
    return state, WorkOrigin(event_id, event_sequence, trigger)


def active_registry() -> RegistryState:
    registration = RegisteredDialogue(
        "dlg-1",
        digest("config"),
        DialogueRegistrationStatus.ACTIVE,
        1,
        1,
        None,
        None,
    )
    return RegistryState("registry-1", 1, 1, "dlg-1", (registration,), None, ())


class FakeClock:
    def __init__(self, now: int = 1_000) -> None:
        self.now = now
        self.fail = False

    def __call__(self) -> int:
        if self.fail:
            raise RuntimeError("clock unavailable")
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += seconds


class SnapshotSource:
    def __init__(
        self,
        state: DialogueState,
        registry: RegistryState,
        origin: WorkOrigin | None,
    ) -> None:
        self.state = state
        self.registry = registry
        self.origin = origin
        self.calls = 0
        self.fail = False

    def __call__(self, session_id: str, authority: object) -> AuthoritativeWorkSnapshot:
        self.calls += 1
        if self.fail:
            raise RuntimeError("snapshot unavailable")
        if session_id != self.state.session_id:
            raise RuntimeError("wrong session")
        return AuthoritativeWorkSnapshot(
            self.state,
            self.registry,
            self.origin,
        )


class RuntimeAuthority:
    def __init__(self) -> None:
        self.live: set[tuple[str, str]] = set()
        self.inactive: set[tuple[str, str]] = set()
        self.calls: list[RuntimeAuthorityCheck] = []
        self.fail = False
        self.reject_call: int | None = None

    def allow(self, epoch: str, owner: str) -> None:
        self.live.add((epoch, owner))

    def __call__(self, check: RuntimeAuthorityCheck, authority: object) -> bool:
        self.calls.append(check)
        if self.fail:
            raise RuntimeError("runtime authority unavailable")
        if self.reject_call == len(self.calls):
            return False
        accepted = (check.runtime_epoch, check.owner_id) in self.live
        if check.prior_owner_must_be_inactive:
            prior = (check.prior_runtime_epoch, check.prior_owner_id)
            accepted = accepted and prior in self.inactive
        return accepted


def _competing_claim(
    root_path: str,
    request_id: str,
    claim_id: str,
    owner_id: str,
    barrier: object,
    results: object,
) -> None:
    root = SecureDirectory.open(root_path)
    dialogues = root.open_directory("dialogues")
    locks = DomainLockManager(Path(root_path) / "locks")
    state, origin = waiting_snapshot()
    source = SnapshotSource(state, active_registry(), origin)
    runtime = RuntimeAuthority()
    runtime.allow("runtime-1", owner_id)
    store = LeaseStore(
        dialogues,
        locks,
        "runtime-1",
        lambda: 1_000,
        snapshot_loader=source,
        runtime_authority_verifier=runtime,
    )
    work = derive_runnable_work(state, origin)
    assert work is not None
    try:
        barrier.wait(10)  # type: ignore[attr-defined]
        try:
            with locks.semantic_session("dlg-1") as authority:
                store.claim(
                    ClaimRequest(
                        "dlg-1",
                        request_id,
                        claim_id,
                        work.work_id,
                        owner_id,
                        30,
                        120,
                    ),
                    authority,
                )
        except LeaseStoreError as exc:
            results.put(exc.code)  # type: ignore[attr-defined]
        else:
            results.put("CLAIMED")  # type: ignore[attr-defined]
    finally:
        locks.close()
        dialogues.close()
        root.close()


class LeaseStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root_path = Path(self.temporary.name).resolve()
        self.root = SecureDirectory.open(self.root_path)
        self.addCleanup(self.root.close)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.addCleanup(self.dialogues.close)
        session = self.dialogues.ensure_directory(
            session_directory_component("dlg-1")
        )
        session.close()
        self.lock_path = self.root_path / "locks"
        self.locks = DomainLockManager(self.lock_path)
        self.addCleanup(self.locks.close)
        self.clock = FakeClock()
        state, origin = waiting_snapshot()
        self.source = SnapshotSource(state, active_registry(), origin)
        self.runtime = RuntimeAuthority()
        self.runtime.allow("runtime-1", "owner-1")

    def store(
        self,
        epoch: str = "runtime-1",
        *,
        clock: FakeClock | None = None,
        runtime: RuntimeAuthority | None = None,
        locks: DomainLockManager | None = None,
    ) -> LeaseStore:
        return LeaseStore(
            self.dialogues,
            locks or self.locks,
            epoch,
            clock or self.clock,
            snapshot_loader=self.source,
            runtime_authority_verifier=runtime or self.runtime,
        )

    def work_id(self) -> str:
        work = derive_runnable_work(self.source.state, self.source.origin)
        assert work is not None
        return work.work_id

    def test_current_work_is_authoritative_and_can_be_absent(self) -> None:
        store = self.store()
        expected = derive_runnable_work(self.source.state, self.source.origin)
        with self.locks.semantic_session("dlg-1") as authority:
            self.source.calls = 0
            observation = store.observe_current_work("dlg-1", authority)
            self.assertIs(type(observation), CurrentWorkObservation)
            self.assertEqual(1, observation.through_sequence)
            current = observation.current
            self.assertIs(type(current), CurrentRunnableWork)
            assert isinstance(current, CurrentRunnableWork)
            self.assertEqual(expected, current.work)
            self.assertEqual(1, current.attempt)
            self.assertEqual(1, self.source.calls)
            self.assertEqual(expected, store.current_work("dlg-1", authority))

        self.source.state = replace(
            initial_dialogue_state("dlg-1", 1),
            sequence=7,
            conversation_version=7,
            lifecycle=SessionLifecycle.OPEN,
            phase=ConversationPhase.CHOOSING_TOPIC,
            candidates=("topic",),
        )
        self.source.origin = None
        with self.locks.semantic_session("dlg-1") as authority:
            observation = store.observe_current_work("dlg-1", authority)
            self.assertEqual(7, observation.through_sequence)
            self.assertIsNone(observation.current)
            self.assertIsNone(store.current_runnable("dlg-1", authority))
            self.assertIsNone(store.current_work("dlg-1", authority))

        self.source.registry = replace(
            self.source.registry,
            current_session_id=None,
        )
        with self.locks.semantic_session("dlg-1") as authority:
            with self.assertRaises(LeaseStoreError) as raised:
                store.current_work("dlg-1", authority)
        self.assertEqual("SESSION_DEACTIVATED", raised.exception.code)

    def claim_request(
        self,
        request_id: str = "request-claim-1",
        *,
        claim_id: str = "claim-1",
        owner_id: str = "owner-1",
        lease_seconds: int = 30,
        max_tenure_seconds: int = 120,
        work_id: str | None = None,
    ) -> ClaimRequest:
        return ClaimRequest(
            "dlg-1",
            request_id,
            claim_id,
            work_id or self.work_id(),
            owner_id,
            lease_seconds,
            max_tenure_seconds,
        )

    def renew_request(
        self,
        request_id: str,
        version: int,
        *,
        claim_id: str = "claim-1",
        owner_id: str = "owner-1",
        lease_seconds: int = 30,
        work_id: str | None = None,
    ) -> RenewRequest:
        return RenewRequest(
            "dlg-1",
            request_id,
            claim_id,
            work_id or self.work_id(),
            owner_id,
            version,
            lease_seconds,
        )

    def reclaim_request(
        self,
        request_id: str = "request-reclaim-1",
        *,
        claim_id: str = "claim-2",
        owner_id: str = "owner-2",
        expected_work_attempt: int = 1,
        lease_seconds: int = 30,
        max_tenure_seconds: int = 120,
        work_id: str | None = None,
    ) -> ReclaimRequest:
        return ReclaimRequest(
            "dlg-1",
            request_id,
            claim_id,
            work_id or self.work_id(),
            owner_id,
            expected_work_attempt,
            lease_seconds,
            max_tenure_seconds,
        )

    @staticmethod
    def fence(lease: LeaseRecord) -> PublishFence:
        return PublishFence(
            lease.session_id,
            lease.claim_id,
            lease.work_id,
            lease.owner_id,
            lease.runtime_epoch,
            lease.lease_version,
            lease.registry_generation,
        )

    @staticmethod
    def publish(
        store: LeaseStore,
        fence: PublishFence,
        authority: SessionLockAuthority,
    ) -> LeaseRecord:
        return store._assert_publishable(fence, authority)

    def transaction_directory(self, work_id: str | None = None) -> Path:
        identifier = work_id or self.work_id()
        component = hashlib.sha256(identifier.encode()).hexdigest()
        return (
            self.root_path
            / "dialogues"
            / session_directory_component("dlg-1")
            / "runtime"
            / "leases"
            / f"work-{component}"
            / "transactions"
        )

    @staticmethod
    def canonical_bytes(tree: object) -> bytes:
        return (
            json.dumps(
                tree,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode()

    @classmethod
    def rehash_transaction(cls, tree: dict[str, object]) -> None:
        unsigned = dict(tree)
        unsigned.pop("transaction_hash", None)
        tree["transaction_hash"] = (
            "sha256:" + hashlib.sha256(cls.canonical_bytes(unsigned)).hexdigest()
        )

    def assert_error(
        self,
        code: str,
        callback: Callable[[], object],
    ) -> None:
        with self.assertRaises(LeaseStoreError) as raised:
            callback()
        self.assertEqual(code, raised.exception.code)

    def test_claim_restart_publish_and_version_neutral_cursor(self) -> None:
        store = self.store()
        before = self.source.state
        with self.locks.semantic_session("dlg-1") as authority:
            outcome = store.claim(self.claim_request(), authority)

        self.assertEqual(LeaseOperation.CLAIM, outcome.operation)
        self.assertFalse(outcome.replayed)
        self.assertEqual(1, outcome.lease.lease_version)
        self.assertEqual(1_030, outcome.lease.expires_at)
        self.assertEqual(1_120, outcome.lease.absolute_expires_at)
        self.assertEqual("runtime-1", outcome.lease.runtime_epoch)
        self.assertIs(before, self.source.state)

        self.source.state = replace(self.source.state, sequence=2)
        restarted = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            recovered = restarted.read("dlg-1", authority)
            published = self.publish(
                restarted,
                self.fence(outcome.lease),
                authority,
            )
        self.assertEqual(outcome.lease, recovered)
        self.assertEqual(outcome.lease, published)
        transactions = tuple(self.transaction_directory().iterdir())
        self.assertEqual(1, len(transactions))
        self.assertEqual(0o600, transactions[0].stat().st_mode & 0o777)

    def test_real_coordinator_event_log_projects_and_claims_work(self) -> None:
        dialogues = self.root.ensure_directory("e2e-dialogues")
        self.addCleanup(dialogues.close)
        locks = DomainLockManager(self.root_path / "e2e-locks")
        self.addCleanup(locks.close)
        evidence_digest = digest("e2e-evidence")
        config = DialogueSessionConfig(
            "dlg-e2e",
            "learner-e2e",
            "repo-e2e",
            "2026-08-16T12:00:00+08:00",
            "trigger-epoch-e2e",
            evidence_digest,
        )
        coordinator = DialogueCoordinator(
            dialogues,
            locks,
            "registry-e2e",
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
        )
        resolution = coordinator.resolve(config)

        def load_snapshot(
            session_id: str,
            authority: SessionLockAuthority,
        ) -> AuthoritativeWorkSnapshot:
            self.assertEqual("dlg-e2e", session_id)
            log = _DialogueTransactionLog.open_existing(
                dialogues,
                initial_dialogue_state("dlg-e2e", 1),
                locks,
                authority,
            )
            try:
                state = log.tip().state
                events = log.read_committed(after_sequence=0)
            finally:
                log.close()
            event = events[-1]
            self.assertIsInstance(event.payload, SessionStarted)
            payload = event.payload
            assert isinstance(payload, SessionStarted)
            return AuthoritativeWorkSnapshot(
                state,
                resolution.registry_state,
                WorkOrigin(
                    event.event_id,
                    event.sequence,
                    payload.candidate_trigger,
                ),
            )

        store = LeaseStore(
            dialogues,
            locks,
            "runtime-e2e",
            lambda: 1_000,
            snapshot_loader=load_snapshot,
            runtime_authority_verifier=lambda check, _authority: (
                check.runtime_epoch == "runtime-e2e"
                and check.owner_id == "owner-e2e"
            ),
        )
        with locks.semantic_session("dlg-e2e") as authority:
            snapshot = load_snapshot("dlg-e2e", authority)
            work = derive_runnable_work(
                snapshot.dialogue_state,
                snapshot.work_origin,
            )
            assert work is not None
            outcome = store.claim(
                ClaimRequest(
                    "dlg-e2e",
                    "request-e2e",
                    "claim-e2e",
                    work.work_id,
                    "owner-e2e",
                    30,
                    120,
                ),
                authority,
            )
        self.assertEqual(work.binding_digest, outcome.lease.binding_digest)
        self.assertRegex(work.work_id, r"^work-[0-9a-f]{64}$")

    def test_receipt_replay_precedes_snapshot_clock_and_runtime_checks(self) -> None:
        store = self.store()
        request = self.claim_request()
        with self.locks.semantic_session("dlg-1") as authority:
            first = store.claim(request, authority)

        calls = self.source.calls
        self.source.fail = True
        self.clock.fail = True
        self.runtime.fail = True
        with self.locks.semantic_session("dlg-1") as authority:
            replay = store.claim(request, authority)
            self.assert_error(
                "IDEMPOTENCY_CONFLICT",
                lambda: store.claim(
                    replace(request, owner_id="different-owner"),
                    authority,
                ),
            )
        self.assertTrue(replay.replayed)
        self.assertEqual(first.lease, replay.lease)
        self.assertEqual(calls, self.source.calls)

    def test_renew_receipt_survives_supersession_and_restart(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(self.claim_request(), authority)
        self.clock.advance(5)
        renewal = self.renew_request("request-renew-1", 1)
        with self.locks.semantic_session("dlg-1") as authority:
            renewed = store.renew(renewal, authority)

        state, origin = waiting_snapshot(
            event_id="event-trigger-2",
            event_sequence=2,
            state_sequence=2,
        )
        self.source.state = state
        self.source.origin = origin
        self.source.fail = True
        self.clock.fail = True
        self.runtime.fail = True
        restarted = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            replay = restarted.renew(renewal, authority)
        self.assertTrue(replay.replayed)
        self.assertEqual(renewed.lease, replay.lease)

    def test_competing_claim_foreign_owner_and_stale_version_fail(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            claimed = store.claim(self.claim_request(), authority)
            self.assert_error(
                "WORK_ALREADY_LEASED",
                lambda: store.claim(
                    self.claim_request(
                        "request-claim-2",
                        claim_id="claim-2",
                        owner_id="owner-2",
                    ),
                    authority,
                ),
            )
            self.assert_error(
                "LEASE_FENCED",
                lambda: store.renew(
                    self.renew_request(
                        "request-foreign",
                        1,
                        owner_id="owner-2",
                    ),
                    authority,
                ),
            )
        self.clock.advance(1)
        with self.locks.semantic_session("dlg-1") as authority:
            renewed = store.renew(
                self.renew_request("request-renew-ok", 1),
                authority,
            )
            self.assert_error(
                "LEASE_VERSION_CONFLICT",
                lambda: store.renew(
                    self.renew_request("request-renew-stale", 1),
                    authority,
                ),
            )
        self.assertEqual(claimed.lease.lease_version + 1, renewed.lease.lease_version)

    def test_two_processes_have_one_immutable_claim_winner(self) -> None:
        self.store()
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(3)
        results = context.Queue()
        processes = tuple(
            context.Process(
                target=_competing_claim,
                args=(
                    str(self.root_path),
                    f"request-race-{index}",
                    f"claim-race-{index}",
                    f"owner-race-{index}",
                    barrier,
                    results,
                ),
            )
            for index in (1, 2)
        )
        for process in processes:
            process.start()
        barrier.wait(10)
        outcomes = sorted(
            (results.get(timeout=10), results.get(timeout=10))
        )
        for process in processes:
            process.join(10)
            self.assertFalse(process.is_alive())
            self.assertEqual(0, process.exitcode)
        self.assertEqual(["CLAIMED", "WORK_ALREADY_LEASED"], outcomes)

    def test_expiry_reclaim_fences_old_claim_and_forbids_identity_reuse(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            old = store.claim(
                self.claim_request(lease_seconds=10),
                authority,
            )
        self.clock.advance(10)
        self.runtime.allow("runtime-1", "owner-2")
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_EXPIRED",
                lambda: self.publish(store, self.fence(old.lease), authority),
            )
            self.assert_error(
                "LEASE_RECLAIM_REQUIRED",
                lambda: store.claim(
                    self.claim_request(
                        "request-claim-again",
                        claim_id="claim-again",
                    ),
                    authority,
                ),
            )
            replacement = store.reclaim(self.reclaim_request(), authority)
            self.assert_error(
                "LEASE_FENCED",
                lambda: self.publish(store, self.fence(old.lease), authority),
            )
        self.assertEqual(2, replacement.lease.lease_version)

        self.clock.advance(30)
        self.runtime.allow("runtime-1", "owner-3")
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "CLAIM_ID_REUSED",
                lambda: store.reclaim(
                    self.reclaim_request(
                        "request-reuse-claim",
                        claim_id="claim-1",
                        owner_id="owner-3",
                    ),
                    authority,
                ),
            )
            self.assert_error(
                "OWNER_ID_REUSED",
                lambda: store.reclaim(
                    self.reclaim_request(
                        "request-reuse-owner",
                        claim_id="claim-3",
                        owner_id="owner-1",
                    ),
                    authority,
                ),
            )

    def test_third_reclaim_returns_stable_proof_without_growing_log(self) -> None:
        store = self.store()
        self.runtime.allow("runtime-1", "owner-2")
        self.runtime.allow("runtime-1", "owner-3")
        self.runtime.allow("runtime-1", "owner-4")
        with self.locks.semantic_session("dlg-1") as authority:
            claimed = store.claim(self.claim_request(), authority)
        self.assertEqual(1, claimed.lease.lease_version)

        successful = []
        for number in (1, 2):
            self.clock.advance(30)
            with self.locks.semantic_session("dlg-1") as authority:
                outcome = store.reclaim(
                    self.reclaim_request(
                        f"request-reclaim-{number}",
                        claim_id=f"claim-{number + 1}",
                        owner_id=f"owner-{number + 1}",
                    ),
                    authority,
                )
            self.assertNotIsInstance(outcome, LeaseExhaustionProof)
            assert not isinstance(outcome, LeaseExhaustionProof)
            successful.append(outcome)
        self.assertEqual([2, 3], [item.lease.lease_version for item in successful])

        self.clock.advance(30)
        exhausted_request = self.reclaim_request(
            "request-reclaim-3",
            claim_id="claim-4",
            owner_id="owner-4",
        )
        before = tuple(self.transaction_directory().iterdir())
        with self.locks.semantic_session("dlg-1") as authority:
            proof = store.reclaim(exhausted_request, authority)
        self.assertIs(type(proof), LeaseExhaustionProof)
        assert isinstance(proof, LeaseExhaustionProof)
        self.assertEqual("dlg-1", proof.session_id)
        self.assertEqual(self.work_id(), proof.work_id)
        self.assertEqual(1, proof.work_attempt)
        self.assertEqual(2, proof.reclaim_count)
        self.assertEqual(3, proof.last_lease_version)
        self.assertRegex(proof.history_digest, r"^sha256:[0-9a-f]{64}$")
        self.assertEqual("runtime-1", proof.requesting_runtime_epoch)
        self.assertEqual("owner-4", proof.requesting_owner_id)
        self.assertEqual("request-reclaim-3", proof.reclaim_request_id)
        self.assertRegex(
            proof.reclaim_request_digest,
            r"^sha256:[0-9a-f]{64}$",
        )
        self.assertEqual(before, tuple(self.transaction_directory().iterdir()))

        restarted = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            replayed_proof = restarted.reclaim(exhausted_request, authority)
            old_receipt = restarted.reclaim(
                self.reclaim_request(
                    "request-reclaim-2",
                    claim_id="claim-3",
                    owner_id="owner-3",
                ),
                authority,
            )
            with self.assertRaises(LeaseStoreError) as raised:
                restarted.reclaim(
                    self.reclaim_request(
                        "request-reclaim-2",
                        claim_id="claim-conflict",
                        owner_id="owner-4",
                    ),
                    authority,
                )
        self.assertEqual(proof, replayed_proof)
        self.assertNotIsInstance(old_receipt, LeaseExhaustionProof)
        assert not isinstance(old_receipt, LeaseExhaustionProof)
        self.assertTrue(old_receipt.replayed)
        self.assertEqual("IDEMPOTENCY_CONFLICT", raised.exception.code)
        self.assertEqual(3, len(tuple(self.transaction_directory().iterdir())))

        self.source.registry = replace(
            self.source.registry,
            current_session_id=None,
        )
        with self.locks.semantic_session("dlg-1") as authority:
            deactivated_replay = restarted.reclaim(
                exhausted_request,
                authority,
            )
        self.assertEqual(proof, deactivated_replay)

    def test_exhaustion_guard_rejects_early_or_lost_runtime_proof(self) -> None:
        store = self.store()
        for owner in ("owner-2", "owner-3", "owner-4"):
            self.runtime.allow("runtime-1", owner)
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(self.claim_request(), authority)
        for number in (1, 2):
            self.clock.advance(30)
            with self.locks.semantic_session("dlg-1") as authority:
                store.reclaim(
                    self.reclaim_request(
                        f"request-reclaim-{number}",
                        claim_id=f"claim-{number + 1}",
                        owner_id=f"owner-{number + 1}",
                    ),
                    authority,
                )

        work = derive_runnable_work(self.source.state, self.source.origin)
        assert work is not None
        history = store._read_history("dlg-1", work.work_id)
        forged_early = store._exhaustion_proof(
            work,
            1,
            history,
            "runtime-1",
            "owner-4",
            "request-reclaim-3",
            digest("forged-reclaim-request"),
        )
        with self.locks.semantic_session("dlg-1") as authority:
            with self.assertRaises(LeaseStoreError) as raised:
                store._exhaustion_marker_guard(forged_early, authority)
        self.assertEqual("LEASE_RECLAIM_NOT_EXHAUSTED", raised.exception.code)

        self.clock.advance(30)
        request = self.reclaim_request(
            "request-reclaim-3",
            claim_id="claim-4",
            owner_id="owner-4",
        )
        with self.locks.semantic_session("dlg-1") as authority:
            proof = store.reclaim(request, authority)
            assert isinstance(proof, LeaseExhaustionProof)
            guard = store._exhaustion_marker_guard(proof, authority)
            self.runtime.live.remove(("runtime-1", "owner-4"))
            with self.assertRaises(LeaseStoreError) as raised:
                guard(authority)
        self.assertEqual("RUNTIME_AUTHORITY_REJECTED", raised.exception.code)

    def test_epoch_change_requires_new_owner_and_proof_prior_is_inactive(self) -> None:
        old_store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            old = old_store.claim(self.claim_request(), authority)

        self.runtime.allow("runtime-2", "owner-2")
        new_store = self.store("runtime-2")
        request = self.reclaim_request()
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "RUNTIME_AUTHORITY_REJECTED",
                lambda: new_store.reclaim(request, authority),
            )
        check = self.runtime.calls[-1]
        self.assertTrue(check.prior_owner_must_be_inactive)
        self.assertEqual("runtime-1", check.prior_runtime_epoch)
        self.assertEqual("owner-1", check.prior_owner_id)

        self.runtime.inactive.add(("runtime-1", "owner-1"))
        with self.locks.semantic_session("dlg-1") as authority:
            replacement = new_store.reclaim(request, authority)
            self.assert_error(
                "LEASE_FENCED",
                lambda: self.publish(
                    old_store,
                    self.fence(old.lease),
                    authority,
                ),
            )
        self.assertEqual("runtime-2", replacement.lease.runtime_epoch)

    def test_new_store_cannot_renew_or_publish_an_old_epoch(self) -> None:
        old_store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            old = old_store.claim(self.claim_request(), authority)
        self.runtime.allow("runtime-2", "owner-1")
        new_store = self.store("runtime-2")

        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_EPOCH_FENCED",
                lambda: new_store.renew(
                    self.renew_request("request-wrong-epoch", 1),
                    authority,
                ),
            )
            self.assert_error(
                "LEASE_FENCED",
                lambda: self.publish(
                    new_store,
                    self.fence(old.lease),
                    authority,
                ),
            )

    def test_epoch_local_clock_may_restart_after_proven_owner_death(self) -> None:
        old_store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            old_store.claim(self.claim_request(), authority)

        new_clock = FakeClock(10)
        self.runtime.allow("runtime-2", "owner-2")
        self.runtime.inactive.add(("runtime-1", "owner-1"))
        new_store = self.store("runtime-2", clock=new_clock)
        with self.locks.semantic_session("dlg-1") as authority:
            replacement = new_store.reclaim(
                self.reclaim_request(),
                authority,
            )
            published = self.publish(
                new_store,
                self.fence(replacement.lease),
                authority,
            )
        self.assertEqual(10, replacement.lease.acquired_at)
        self.assertEqual(replacement.lease, published)

    def test_runtime_is_rechecked_at_the_immutable_commit_boundary(self) -> None:
        self.runtime.reject_call = 2
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "RUNTIME_AUTHORITY_REJECTED",
                lambda: store.claim(self.claim_request(), authority),
            )
        transactions = self.transaction_directory()
        self.assertEqual(
            (),
            tuple(transactions.iterdir()) if transactions.exists() else (),
        )

    def test_renew_rechecks_expiry_at_the_transaction_boundary(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(
                self.claim_request(lease_seconds=10),
                authority,
            )
        values = iter((1_001, 1_010))
        store._clock = lambda: next(values)

        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_EXPIRED",
                lambda: store.renew(
                    self.renew_request("request-boundary-expiry", 1),
                    authority,
                ),
            )
        self.assertEqual(1, len(tuple(self.transaction_directory().iterdir())))

    def test_encoding_cannot_race_a_renew_past_old_expiry(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(
                self.claim_request(lease_seconds=10),
                authority,
            )
        self.clock.now = 1_001
        original_encode = lease_store_module._encode_transaction

        def encode_at_expiry(transaction: object) -> bytes:
            encoded = original_encode(transaction)  # type: ignore[arg-type]
            self.clock.now = 1_010
            return encoded

        with mock.patch.object(
            lease_store_module,
            "_encode_transaction",
            side_effect=encode_at_expiry,
        ), self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_EXPIRED",
                lambda: store.renew(
                    self.renew_request("request-encode-expiry", 1),
                    authority,
                ),
            )
        self.assertEqual(1, len(tuple(self.transaction_directory().iterdir())))

    def test_staging_io_cannot_race_a_renew_past_old_expiry(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(
                self.claim_request(lease_seconds=10),
                authority,
            )
        self.clock.now = 1_001
        original_stage = SecureDirectory._write_temporary

        def stage_at_expiry(
            directory: SecureDirectory,
            data: bytes,
        ) -> str:
            temporary = original_stage(directory, data)
            self.clock.now = 1_010
            return temporary

        with mock.patch.object(
            SecureDirectory,
            "_write_temporary",
            autospec=True,
            side_effect=stage_at_expiry,
        ), self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_EXPIRED",
                lambda: store.renew(
                    self.renew_request("request-stage-expiry", 1),
                    authority,
                ),
            )
        names = tuple(item.name for item in self.transaction_directory().iterdir())
        self.assertEqual(("transaction-00000000000000000001.json",), names)

    def test_clock_rollback_cannot_resurrect_an_observed_expired_claim(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            claimed = store.claim(self.claim_request(), authority)
        self.clock.now = 1_040
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_EXPIRED",
                lambda: self.publish(
                    store,
                    self.fence(claimed.lease),
                    authority,
                ),
            )

        self.clock.now = 1_000
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "CLOCK_ROLLED_BACK",
                lambda: self.publish(
                    store,
                    self.fence(claimed.lease),
                    authority,
                ),
            )

    def test_clock_high_water_is_serialized_across_session_workers(self) -> None:
        store = self.store()
        values = iter((100, 99))
        store._clock = lambda: next(values)
        barrier = threading.Barrier(3)
        results: list[tuple[str, int | str]] = []

        def observe(session_id: str) -> None:
            barrier.wait()
            try:
                result: int | str = store._now()
            except LeaseStoreError as exc:
                result = exc.code
            results.append((session_id, result))

        threads = (
            threading.Thread(target=observe, args=("dlg-1",)),
            threading.Thread(target=observe, args=("dlg-2",)),
        )
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(
            {100, "CLOCK_ROLLED_BACK"},
            {result for _, result in results},
        )

    def test_nonextending_renewal_is_rejected_without_a_receipt(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(self.claim_request(), authority)
            self.assert_error(
                "LEASE_RENEWAL_NOT_EXTENDED",
                lambda: store.renew(
                    self.renew_request("request-no-extension", 1),
                    authority,
                ),
            )
        self.assertEqual(1, len(tuple(self.transaction_directory().iterdir())))

    def test_renewal_limit_still_allows_expiry_reclaim_and_old_replay(self) -> None:
        store = self.store()
        with mock.patch.object(
            lease_store_module,
            "_MAX_RENEWALS_PER_TENURE",
            2,
        ):
            with self.locks.semantic_session("dlg-1") as authority:
                store.claim(self.claim_request(), authority)
            first = self.renew_request("request-limit-renew-1", 1)
            self.clock.advance(1)
            with self.locks.semantic_session("dlg-1") as authority:
                committed = store.renew(first, authority)
            self.clock.advance(1)
            with self.locks.semantic_session("dlg-1") as authority:
                current = store.renew(
                    self.renew_request("request-limit-renew-2", 2),
                    authority,
                )
            self.clock.advance(1)
            with self.locks.semantic_session("dlg-1") as authority:
                self.assert_error(
                    "LEASE_RENEWAL_LIMIT",
                    lambda: store.renew(
                        self.renew_request("request-limit-renew-3", 3),
                        authority,
                    ),
                )
                replay = store.renew(first, authority)
            self.assertTrue(replay.replayed)
            self.assertEqual(committed.lease, replay.lease)

            self.clock.now = current.lease.expires_at
            self.runtime.allow("runtime-1", "owner-2")
            with self.locks.semantic_session("dlg-1") as authority:
                reclaimed = store.reclaim(self.reclaim_request(), authority)
            self.assertEqual(4, reclaimed.lease.lease_version)

    def test_arbitrary_epoch_string_is_not_runtime_authority(self) -> None:
        store = self.store("runtime-forged")
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "RUNTIME_AUTHORITY_REJECTED",
                lambda: store.claim(self.claim_request(), authority),
            )

    def test_renewal_is_capped_by_absolute_tenure(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(
                self.claim_request(
                    lease_seconds=20,
                    max_tenure_seconds=40,
                ),
                authority,
            )
        self.clock.advance(15)
        with self.locks.semantic_session("dlg-1") as authority:
            renewed = store.renew(
                self.renew_request(
                    "request-renew-cap",
                    1,
                    lease_seconds=30,
                ),
                authority,
            )
        self.assertEqual(1_040, renewed.lease.expires_at)
        self.clock.advance(25)
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_TENURE_EXPIRED",
                lambda: store.renew(
                    self.renew_request("request-renew-too-late", 2),
                    authority,
                ),
            )

    def test_authoritative_handoff_and_new_origin_fence_old_publish(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            claimed = store.claim(self.claim_request(), authority)

        self.source.registry = replace(self.source.registry, generation=2)
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "SESSION_DEACTIVATED",
                lambda: self.publish(
                    store,
                    self.fence(claimed.lease),
                    authority,
                ),
            )

        self.source.registry = active_registry()
        state, origin = waiting_snapshot(
            event_id="event-trigger-2",
            event_sequence=2,
            state_sequence=2,
        )
        self.source.state = state
        self.source.origin = origin
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "WORK_SUPERSEDED",
                lambda: self.publish(
                    store,
                    self.fence(claimed.lease),
                    authority,
                ),
            )

    def test_same_canonical_id_with_changed_trigger_token_fails_closed(self) -> None:
        store = self.store()
        old_work_id = self.work_id()
        with self.locks.semantic_session("dlg-1") as authority:
            claimed = store.claim(self.claim_request(), authority)

        state, origin = waiting_snapshot(trigger_work_id="trigger-token-2")
        self.source.state = state
        self.source.origin = origin
        self.assertEqual(old_work_id, self.work_id())
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "WORK_IDENTITY_CONFLICT",
                lambda: store.claim(
                    self.claim_request(
                        "request-changed-binding",
                        claim_id="claim-2",
                        owner_id="owner-2",
                    ),
                    authority,
                ),
            )
            self.assert_error(
                "LEASE_FENCED",
                lambda: self.publish(
                    store,
                    self.fence(claimed.lease),
                    authority,
                ),
            )

    def test_immutable_history_has_no_receipt_capacity_dead_end(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(
                self.claim_request(
                    lease_seconds=3_600,
                    max_tenure_seconds=86_400,
                ),
                authority,
            )
        first_renewal: RenewRequest | None = None
        for version in range(1, 81):
            self.clock.advance(1)
            request = self.renew_request(
                f"request-renew-{version}",
                version,
                lease_seconds=3_600,
            )
            first_renewal = first_renewal or request
            with self.locks.semantic_session("dlg-1") as authority:
                outcome = store.renew(request, authority)
            self.assertEqual(version + 1, outcome.lease.lease_version)

        restarted = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            current = restarted.read("dlg-1", authority)
            replay = restarted.renew(first_renewal, authority)
        assert current is not None
        self.assertEqual(81, current.lease_version)
        self.assertTrue(replay.replayed)
        self.assertEqual(81, len(tuple(self.transaction_directory().iterdir())))

    def test_commit_response_loss_replays_published_transaction(self) -> None:
        store = self.store()
        original = SecureDirectory.write_immutable_guarded

        def publish_then_fail(
            directory: SecureDirectory,
            component: str,
            data: bytes,
            guard: Callable[[], None],
        ) -> None:
            original(directory, component, data, guard)
            raise SecureFsError("FS_DURABILITY_FAILED")

        with mock.patch.object(
            SecureDirectory,
            "write_immutable_guarded",
            autospec=True,
            side_effect=publish_then_fail,
        ), self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "FS_DURABILITY_FAILED",
                lambda: store.claim(self.claim_request(), authority),
            )

        restarted = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            replay = restarted.claim(self.claim_request(), authority)
        self.assertTrue(replay.replayed)
        self.assertEqual(1, replay.lease.lease_version)

    def test_failure_before_publish_preserves_previous_transaction(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            claimed = store.claim(self.claim_request(), authority)
        self.clock.advance(1)
        with mock.patch.object(
            SecureDirectory,
            "write_immutable_guarded",
            autospec=True,
            side_effect=SecureFsError("INJECTED_FAILURE"),
        ), self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "INJECTED_FAILURE",
                lambda: store.renew(
                    self.renew_request("request-crash-renew", 1),
                    authority,
                ),
            )

        restarted = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            recovered = restarted.read("dlg-1", authority)
        self.assertEqual(claimed.lease, recovered)

    def test_canonical_tampering_and_broken_hash_link_fail_closed(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            store.claim(self.claim_request(), authority)
        self.clock.advance(1)
        with self.locks.semantic_session("dlg-1") as authority:
            store.renew(self.renew_request("request-renew-1", 1), authority)

        first_path = self.transaction_directory() / (
            "transaction-00000000000000000001.json"
        )
        second_path = self.transaction_directory() / (
            "transaction-00000000000000000002.json"
        )
        first_original = first_path.read_bytes()
        second_original = second_path.read_bytes()

        first = json.loads(first_original)
        first["lease"]["owner_id"] = "tampered-owner"
        self.rehash_transaction(first)
        first_path.write_bytes(self.canonical_bytes(first))
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_HISTORY_INVALID",
                lambda: store.read("dlg-1", authority),
            )

        first_path.write_bytes(first_original)
        second = json.loads(second_original)
        second["previous_transaction_hash"] = digest("wrong-previous")
        self.rehash_transaction(second)
        second_path.write_bytes(self.canonical_bytes(second))
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_HISTORY_INVALID",
                lambda: store.read("dlg-1", authority),
            )

        second_path.write_bytes(b"not canonical json")
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "LEASE_RECORD_INVALID",
                lambda: store.read("dlg-1", authority),
            )

    def test_lock_namespace_is_immutably_bound_to_durable_overlay(self) -> None:
        self.store()
        other_locks = DomainLockManager(self.root_path / "locks-b")
        self.addCleanup(other_locks.close)
        self.assert_error(
            "LOCK_NAMESPACE_MISMATCH",
            lambda: self.store(locks=other_locks),
        )

        with self.locks.semantic_session("dlg-other") as wrong:
            self.assert_error(
                "LOCK_AUTHORITY_SESSION_MISMATCH",
                lambda: self.store().claim(self.claim_request(), wrong),
            )

    def test_namespace_replacement_before_commit_cannot_publish(self) -> None:
        store = self.store()
        backup = self.root_path / "locks-original"
        original_new_lease = store._new_lease

        def replace_namespace(*args: object, **kwargs: object) -> LeaseRecord:
            lease = original_new_lease(*args, **kwargs)  # type: ignore[arg-type]
            self.lock_path.rename(backup)
            self.lock_path.mkdir(mode=0o700)
            return lease

        try:
            with mock.patch.object(
                store,
                "_new_lease",
                side_effect=replace_namespace,
            ), self.locks.semantic_session("dlg-1") as authority:
                self.assert_error(
                    "LOCK_NAMESPACE_CHANGED",
                    lambda: store.claim(self.claim_request(), authority),
                )
        finally:
            if self.lock_path.exists():
                self.lock_path.rmdir()
            if backup.exists():
                backup.rename(self.lock_path)
        names = (
            tuple(self.transaction_directory().iterdir())
            if self.transaction_directory().exists()
            else ()
        )
        self.assertEqual((), names)

    def test_request_snapshot_clock_and_runtime_failures_are_bounded(self) -> None:
        store = self.store()
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "INVALID_CLAIM_REQUEST",
                lambda: store.claim(
                    replace(self.claim_request(), lease_seconds=0),
                    authority,
                ),
            )
            self.assert_error(
                "INVALID_RENEW_REQUEST",
                lambda: store.renew(None, authority),  # type: ignore[arg-type]
            )
            self.assert_error(
                "INVALID_RECLAIM_REQUEST",
                lambda: store.reclaim(None, authority),  # type: ignore[arg-type]
            )
            self.assert_error(
                "INVALID_RECLAIM_REQUEST",
                lambda: store.reclaim(
                    self.reclaim_request(expected_work_attempt=0),
                    authority,
                ),
            )
            self.assert_error(
                "INVALID_PUBLISH_FENCE",
                lambda: store._assert_publishable(
                    None,  # type: ignore[arg-type]
                    authority,
                ),
            )
            self.assert_error(
                "WORK_SUPERSEDED",
                lambda: store.claim(
                    self.claim_request(work_id="other-work"),
                    authority,
                ),
            )

        self.source.fail = True
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "AUTHORITATIVE_SNAPSHOT_FAILED",
                lambda: store.claim(self.claim_request(), authority),
            )
        self.source.fail = False
        self.clock.fail = True
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "CLOCK_FAILED",
                lambda: store.claim(self.claim_request(), authority),
            )
        self.clock.fail = False
        self.runtime.fail = True
        with self.locks.semantic_session("dlg-1") as authority:
            self.assert_error(
                "RUNTIME_AUTHORITY_FAILED",
                lambda: store.claim(self.claim_request(), authority),
            )


if __name__ == "__main__":
    unittest.main()
