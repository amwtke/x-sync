import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401
from xsync_v2.coordinator import (
    DialogueCoordinator,
    DialogueSessionConfig,
)
from xsync_v2.domain import ConversationPhase, EvidenceCheck, EvidenceHealth, Lens
from xsync_v2.event_codec import ActorKind, DialogueActor, sha256_digest
from xsync_v2.host_context import (
    EvidenceContextClaim,
    HostContextSource,
    encode_host_context,
)
from xsync_v2.host_control import (
    HostClaimEnvelope,
    HostControl,
    HostReclaimRequest,
    HostWorkAdvanced,
    HostWorkDisposition,
    HostWaitOutcome,
)
from xsync_v2.host_work import (
    HostResultPublishRequest,
    HostWorkService,
    authoritative_work_snapshot,
)
from xsync_v2.host_result import TopicCandidatesResult
from xsync_v2.lease_store import (
    ClaimRequest,
    CurrentRunnableWork,
    CurrentWorkObservation,
    LeaseStore,
    ReclaimRequest,
    RenewRequest,
)
from xsync_v2.locking import DomainLockManager, RegistryLockMode
from xsync_v2.observer import (
    CommittedBatch,
    CommittedEventView,
    ImmutablePayloadView,
    StreamKind,
)
from xsync_v2.observers.work_wake import WorkWakeHint, WorkWakeObserver
from xsync_v2.secure_fs import SecureDirectory


def digest(label: str) -> str:
    return sha256_digest(label.encode())


RUNTIME = DialogueActor(ActorKind.RUNTIME, "runtime.host-control")
HOST = DialogueActor(ActorKind.HOST, "host.host-control")


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000

    def __call__(self) -> int:
        return self.now


class RecordingContextProvider:
    def __init__(self, leases: LeaseStore) -> None:
        self.leases = leases
        self.calls = []

    def load_context(self, work, lease, authority):
        durable = self.leases.read(work.session_id, authority)
        assert durable == lease
        self.calls.append((work, lease))
        return HostContextSource(
            topic_contract=None,
            task_scope="仓库 onboarding",
            current_lens=Lens.MIXED,
            gates=(),
            previous_question=None,
            learner_turn=None,
            learner_model=(),
            priority_gap="选择一个有仓库依据的话题",
            evidence_claims=(
                EvidenceContextClaim(
                    "ev-scan",
                    "仓库包含支付边界实现",
                    "src/payments.py:1",
                    work.evidence_digest,
                ),
            ),
            through_event_sequence=work.observed_sequence,
        )


class HostControlTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root_path = Path(self.temporary.name).resolve()
        self.root = SecureDirectory.open(root_path)
        self.addCleanup(self.root.close)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.addCleanup(self.dialogues.close)
        self.locks = DomainLockManager(root_path / "locks")
        self.addCleanup(self.locks.close)
        self.clock = FakeClock()
        self.owners = {"owner-1", "owner-2", "owner-3", "owner-4"}
        self.config = DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repo-1",
            "2026-08-16T12:00:00+08:00",
            "trigger-epoch",
            digest("evidence"),
        )
        self.coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
        )
        self.leases = LeaseStore(
            self.dialogues,
            self.locks,
            "runtime-1",
            self.clock,
            snapshot_loader=lambda session_id, authority: authoritative_work_snapshot(
                self.coordinator,
                session_id,
                authority,
            ),
            runtime_authority_verifier=lambda check, _authority: (
                check.owner_id in self.owners
            ),
        )
        self.service = HostWorkService(self.coordinator, self.leases)
        self.provider = RecordingContextProvider(self.leases)
        self.control = HostControl(
            self.coordinator,
            self.locks,
            self.leases,
            self.service,
            self.provider,
        )
        self.coordinator.resolve(self.config)

    def current_work(self):
        with self.locks.semantic_session(
            self.config.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            item = self.leases.current_work(self.config.session_id, authority)
        assert item is not None
        return item

    def claim_request(self, work, *, owner="owner-1", suffix="1"):
        return ClaimRequest(
            work.session_id,
            f"request-claim-{suffix}",
            f"claim-{suffix}",
            work.work_id,
            owner,
            30,
            120,
        )

    def reclaim_request(self, lease_request):
        return HostReclaimRequest(
            lease_request,
            "2026-08-16T12:00:00+08:00",
            RUNTIME,
        )

    def test_claim_mutates_lease_before_loading_bounded_typed_context(self) -> None:
        work = self.current_work()

        envelope = self.control.claim(self.claim_request(work))

        self.assertIs(type(envelope), HostClaimEnvelope)
        self.assertEqual(work, envelope.work)
        self.assertEqual(envelope.lease.claim_id, envelope.fence.claim_id)
        self.assertEqual(envelope.lease.lease_version, envelope.fence.lease_version)
        self.assertEqual(work.work_id, envelope.context.work_id)
        self.assertLessEqual(len(encode_host_context(envelope.context)), 16 * 1024)
        self.assertEqual([(work, envelope.lease)], self.provider.calls)

    def test_strict_result_publish_delegates_to_the_shared_host_boundary(
        self,
    ) -> None:
        work = self.current_work()
        envelope = self.control.claim(self.claim_request(work))

        outcome = self.control.publish_result(
            HostResultPublishRequest(
                "host-control-candidates",
                envelope.work,
                TopicCandidatesResult(("Registry fencing", "Lease recovery")),
                self.config.created_at,
                HOST,
                envelope.fence,
            )
        )

        self.assertIs(ConversationPhase.CHOOSING_TOPIC, outcome.state.phase)
        self.assertEqual(
            ("Registry fencing", "Lease recovery"),
            outcome.state.candidates,
        )

    def test_failed_claim_never_loads_context(self) -> None:
        work = self.current_work()
        self.control.claim(self.claim_request(work))
        calls = len(self.provider.calls)

        with self.assertRaisesRegex(Exception, "WORK_ALREADY_LEASED"):
            self.control.claim(
                self.claim_request(work, owner="owner-2", suffix="2")
            )

        self.assertEqual(calls, len(self.provider.calls))

    def test_two_control_claims_have_one_durable_winner(self) -> None:
        work = self.current_work()

        def attempt(owner, suffix):
            try:
                return self.control.claim(
                    self.claim_request(work, owner=owner, suffix=suffix)
                )
            except Exception as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = tuple(
                pool.map(
                    lambda values: attempt(*values),
                    (("owner-1", "1"), ("owner-2", "2")),
                )
            )

        winners = tuple(
            item for item in outcomes if type(item) is HostClaimEnvelope
        )
        losers = tuple(item for item in outcomes if isinstance(item, Exception))
        self.assertEqual(1, len(winners))
        self.assertEqual(1, len(losers))
        self.assertRegex(str(losers[0]), "WORK_ALREADY_LEASED")

    def test_wait_rechecks_durable_work_when_wake_arrives_during_read(self) -> None:
        durable = self.current_work()
        observer = WorkWakeObserver(self.control)
        calls = 0

        def racing_read(session_id, authority):
            nonlocal calls
            calls += 1
            if calls == 1:
                observer.on_batch(
                    CommittedBatch(
                        StreamKind.DIALOGUE,
                        session_id,
                        (
                            CommittedEventView(
                                "event-wake-1",
                                1,
                                ImmutablePayloadView(
                                    "learner_turn_submitted",
                                    (),
                                ),
                            ),
                        ),
                    )
                )
                return None
            return CurrentRunnableWork(durable, 1)

        with mock.patch.object(
            self.leases,
            "observe_current_work",
            side_effect=lambda session_id, authority: CurrentWorkObservation(
                1 if calls == 0 else durable.observed_sequence,
                racing_read(session_id, authority),
            ),
        ):
            observed = self.control.wait("session-1", timeout=1)

        self.assertIs(type(observed), HostWaitOutcome)
        self.assertFalse(observed.timed_out)
        self.assertIsNotNone(observed.work)
        assert observed.work is not None
        self.assertEqual(durable.work_id, observed.work.work_id)
        self.assertEqual(2, calls)

    def test_wait_timeout_is_not_an_error_and_does_one_durable_check(self) -> None:
        with mock.patch.object(
            self.leases,
            "observe_current_work",
            return_value=CurrentWorkObservation(1, None),
        ) as current:
            outcome = self.control.wait("session-1", timeout=0)
        self.assertTrue(outcome.timed_out)
        self.assertIsNone(outcome.work)
        self.assertEqual(1, outcome.through_sequence)
        current.assert_called_once()

    def test_dropped_wake_is_recovered_by_bounded_durable_recheck(self) -> None:
        durable = self.current_work()
        with (
            mock.patch.object(
                self.leases,
                "observe_current_work",
                side_effect=(
                    CurrentWorkObservation(1, None),
                    CurrentWorkObservation(
                        durable.observed_sequence,
                        CurrentRunnableWork(durable, 1),
                    ),
                ),
            ) as current,
            mock.patch.object(
                self.control._condition,
                "wait",
                return_value=None,
            ) as condition_wait,
        ):
            outcome = self.control.wait("session-1")

        self.assertFalse(outcome.timed_out)
        self.assertIsNotNone(outcome.work)
        self.assertEqual(durable.observed_sequence, outcome.through_sequence)
        self.assertEqual(2, current.call_count)
        condition_wait.assert_called_once_with(1.0)

    def test_spurious_wake_with_no_work_returns_typed_timeout(self) -> None:
        ticks = iter((0.0, 1.0))
        control = HostControl(
            self.coordinator,
            self.locks,
            self.leases,
            self.service,
            self.provider,
            monotonic_clock=lambda: next(ticks),
        )
        control.notify(WorkWakeHint("session-1", 7))
        with mock.patch.object(
            self.leases,
            "observe_current_work",
            return_value=CurrentWorkObservation(7, None),
        ):
            outcome = control.wait("session-1", timeout=1)
        self.assertTrue(outcome.timed_out)
        self.assertIsNone(outcome.work)
        self.assertEqual(7, outcome.through_sequence)

    def test_renew_and_reclaim_delegate_under_authority(self) -> None:
        work = self.current_work()
        claimed = self.control.claim(self.claim_request(work))
        self.clock.now += 20

        renewed = self.control.renew(
            RenewRequest(
                work.session_id,
                "request-renew-1",
                claimed.lease.claim_id,
                work.work_id,
                claimed.lease.owner_id,
                claimed.lease.lease_version,
                30,
            )
        )
        self.assertEqual(2, renewed.lease.lease_version)

        self.clock.now = renewed.lease.expires_at
        reclaimed = self.control.reclaim(
            self.reclaim_request(ReclaimRequest(
                work.session_id,
                "request-reclaim-1",
                "claim-2",
                work.work_id,
                "owner-2",
                1,
                30,
                120,
            ))
        )
        self.assertIs(type(reclaimed), HostClaimEnvelope)
        self.assertEqual(3, reclaimed.lease.lease_version)
        self.assertEqual("owner-2", reclaimed.lease.owner_id)

    def test_exhaustion_is_restart_replayable_and_never_loads_context(self) -> None:
        work = self.current_work()
        claimed = self.control.claim(self.claim_request(work))
        current = claimed.lease
        for number in (1, 2):
            self.clock.now = current.expires_at
            reclaimed = self.control.reclaim(
                self.reclaim_request(ReclaimRequest(
                    work.session_id,
                    f"request-reclaim-{number}",
                    f"claim-{number + 1}",
                    work.work_id,
                    f"owner-{number + 1}",
                    1,
                    30,
                    120,
                ))
            )
            self.assertIs(type(reclaimed), HostClaimEnvelope)
            current = reclaimed.lease
        calls = len(self.provider.calls)
        self.clock.now = current.expires_at

        final_request = self.reclaim_request(ReclaimRequest(
                work.session_id,
                "request-reclaim-3",
                "claim-4",
                work.work_id,
                "owner-4",
                1,
                30,
                120,
            ))
        original = self.service.record_lease_exhaustion
        failed_once = False

        def lose_first_response(record_request):
            nonlocal failed_once
            committed = original(record_request)
            if not failed_once:
                failed_once = True
                raise RuntimeError("lost exhaustion response")
            return committed

        with (
            mock.patch.object(
                self.service,
                "record_lease_exhaustion",
                side_effect=lose_first_response,
            ),
            self.assertRaisesRegex(RuntimeError, "lost exhaustion response"),
        ):
            self.control.reclaim(final_request)

        self.assertEqual(calls, len(self.provider.calls))
        restarted = HostControl(
            self.coordinator,
            self.locks,
            self.leases,
            self.service,
            self.provider,
        )
        replayed = restarted.reclaim(final_request)
        self.assertIs(type(replayed), HostWorkAdvanced)
        self.assertIs(HostWorkDisposition.REQUEUED, replayed.disposition)
        self.assertTrue(replayed.replayed)
        self.assertEqual(work.work_id, replayed.work_id)
        next_work = self.control.current(work.session_id)
        self.assertIsNotNone(next_work)
        assert next_work is not None
        self.assertNotEqual(work.work_id, next_work.work_id)

        conflicting = replace(
            final_request,
            lease_request=replace(
                final_request.lease_request,
                claim_id="claim-conflict",
                owner_id="owner-conflict",
            ),
        )
        with self.assertRaisesRegex(Exception, "IDEMPOTENCY_CONFLICT"):
            restarted.reclaim(conflicting)

    def test_wait_rejects_nonfinite_timeouts_and_clock_rollback(self) -> None:
        for timeout in (
            float("nan"),
            float("inf"),
            float("-inf"),
            10**10_000,
        ):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(
                Exception,
                "INVALID_WAIT_TIMEOUT",
            ):
                self.control.wait("session-1", timeout=timeout)

        ticks = iter((2.0, 1.0))
        control = HostControl(
            self.coordinator,
            self.locks,
            self.leases,
            self.service,
            self.provider,
            monotonic_clock=lambda: next(ticks),
        )
        with (
            mock.patch.object(
                self.leases,
                "observe_current_work",
                return_value=CurrentWorkObservation(1, None),
            ),
            mock.patch.object(control._condition, "wait", return_value=None),
            self.assertRaisesRegex(Exception, "MONOTONIC_CLOCK_ROLLED_BACK"),
        ):
            control.wait("session-1", timeout=1)

        invalid_clock = HostControl(
            self.coordinator,
            self.locks,
            self.leases,
            self.service,
            self.provider,
            monotonic_clock=lambda: 10**10_000,
        )
        with self.assertRaisesRegex(Exception, "MONOTONIC_CLOCK_INVALID"):
            invalid_clock.wait("session-1", timeout=1)

    def test_poll_interval_configuration_fails_closed(self) -> None:
        for interval in (
            True,
            0,
            -1,
            float("nan"),
            float("inf"),
            61,
            10**10_000,
        ):
            with self.subTest(interval=interval), self.assertRaisesRegex(
                Exception,
                "INVALID_HOST_CONTROL_CONFIGURATION",
            ):
                HostControl(
                    self.coordinator,
                    self.locks,
                    self.leases,
                    self.service,
                    self.provider,
                    durable_poll_interval=interval,
                )

    def test_publish_result_is_a_thin_service_delegation(self) -> None:
        request = object()
        receipt = object()
        with mock.patch.object(
            self.service,
            "publish_result",
            return_value=receipt,
        ) as publish:
            self.assertIs(receipt, self.control.publish_result(request))
        publish.assert_called_once_with(request)

if __name__ == "__main__":
    unittest.main()
