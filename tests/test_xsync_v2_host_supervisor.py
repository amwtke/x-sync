from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

# isort: split
from xsync_v2.coordinator import DialogueSessionConfig
from xsync_v2.domain import EvidenceCheck, EvidenceHealth, Lens
from xsync_v2.event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    canonical_json_bytes,
    sha256_digest,
)
from xsync_v2.host_api import HostApiResponse
from xsync_v2.host_context import EvidenceContextClaim, HostContextSource
from xsync_v2.host_result import TopicCandidatesResult
from xsync_v2.host_supervisor import (
    HostSupervisor,
    HostSupervisorError,
    HostSupervisorSubmission,
)
from xsync_v2.lease_store import ClaimRequest
from xsync_v2.runtime import DialogueRuntime


def digest(label: str) -> str:
    return sha256_digest(label.encode())


class SupervisorContextProvider:
    def load_context(self, work, _lease, _authority):
        return HostContextSource(
            topic_contract=None,
            task_scope="repository onboarding",
            current_lens=Lens.MIXED,
            gates=(),
            previous_question=None,
            learner_turn=None,
            learner_model=(),
            priority_gap="choose one grounded topic",
            evidence_claims=(
                EvidenceContextClaim(
                    "evidence-1",
                    "The repository uses durable dialogue events.",
                    "architecture.md:1",
                    digest("evidence-entry"),
                ),
            ),
            through_event_sequence=work.observed_sequence,
        )


class HostSupervisorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        control = self.root / "control"
        control.mkdir(mode=0o700)
        self.socket_path = control / "host.sock"
        self.lease_now = 1_000
        self.monotonic_now = 0.0
        self.runtime = DialogueRuntime(
            self.root,
            "registry-1",
            "runtime-1",
            evidence_verifier=lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            context_provider=SupervisorContextProvider(),
            runtime_authority_verifier=lambda check, _authority: (
                check.runtime_epoch == "runtime-1"
                and check.owner_id in {"owner-old", "owner-new"}
            ),
            lease_clock=lambda: self.lease_now,
            browser_clock=lambda: "2026-08-16T21:00:00+08:00",
            monotonic_clock=lambda: float(self.lease_now),
            durable_poll_interval=0.01,
        )
        self.addCleanup(self.runtime.close)
        self.config = DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repository-1",
            "2026-08-16T21:00:00+08:00",
            "runtime-1",
            digest("evidence"),
        )
        self.runtime.resolve(self.config)
        self.runtime.start_host_ipc(self.socket_path)
        self.identity = 0

    def next_id(self, prefix: str) -> str:
        self.identity += 1
        return f"{prefix}.{self.identity}"

    @staticmethod
    def parse(lines: list[bytes]):
        return [json.loads(line) for line in lines]

    def supervisor(self, emit, **values) -> HostSupervisor:
        options = {
            "emit": emit,
            "occurred_at": lambda: self.config.created_at,
            "id_generator": self.next_id,
            "secret_generator": lambda: "submission.secret-1",
            "monotonic_clock": lambda: self.monotonic_now,
            "wait_timeout": 1,
            "retry_interval": 1,
            "lease_seconds": 30,
            "max_tenure_seconds": 120,
        }
        options.update(values)
        return HostSupervisor(
            self.socket_path,
            "session-1",
            "owner-new",
            "runtime.supervisor-1",
            **options,
        )

    def submission(self, handle: str) -> HostSupervisorSubmission:
        return HostSupervisorSubmission(
            handle,
            "publish-candidates-1",
            TopicCandidatesResult(("Registry fencing", "Lease recovery")),
            self.config.created_at,
            "host.adapter-1",
        )

    def test_ready_work_publish_and_close_are_monotonic_ndjson(self) -> None:
        lines: list[bytes] = []
        supervisor: HostSupervisor

        def emit(line: bytes) -> None:
            lines.append(line)
            event = json.loads(line)
            if event["type"] == "work":
                supervisor.submit(self.submission(event["submission_handle"]))
            elif event["type"] == "status" and event["status"] == "published":
                supervisor.close("test-complete")

        supervisor = self.supervisor(emit)
        reason = supervisor.run()
        events = self.parse(lines)

        self.assertEqual("test-complete", reason)
        self.assertEqual([1, 2, 3, 4], [item["stream_sequence"] for item in events])
        self.assertEqual(
            ["ready", "work", "status", "closed"],
            [item["type"] for item in events],
        )
        self.assertTrue(all(line.endswith(b"\n") for line in lines))
        self.assertEqual("published", events[2]["status"])
        self.assertEqual(
            "choosing_topic",
            self.runtime.browser.current("session-1").dialogue_state.phase.value,
        )

    def test_renewal_is_silent_and_uses_the_latest_publish_fence(self) -> None:
        lines: list[bytes] = []
        waits = 0
        supervisor: HostSupervisor

        def emit(line: bytes) -> None:
            lines.append(line)
            event = json.loads(line)
            if event["type"] == "status" and event["status"] == "published":
                supervisor.close("renewed-and-published")

        def wait_strategy(_condition, _timeout) -> None:
            nonlocal waits
            waits += 1
            if waits == 1:
                self.monotonic_now += 16
                self.lease_now += 16
                return
            work = next(item for item in self.parse(lines) if item["type"] == "work")
            supervisor.submit(self.submission(work["submission_handle"]))

        supervisor = self.supervisor(emit, wait_strategy=wait_strategy)
        api = self.runtime.host_api
        with mock.patch.object(api, "_renew", wraps=api._renew) as renew:
            reason = supervisor.run()

        events = self.parse(lines)
        self.assertEqual("renewed-and-published", reason)
        self.assertEqual(1, renew.call_count)
        self.assertEqual(
            ["ready", "work", "status", "closed"],
            [item["type"] for item in events],
        )

    def test_expired_foreign_claim_is_reclaimed_before_work_delivery(self) -> None:
        current = self.runtime.host.current("session-1")
        self.assertIsNotNone(current)
        assert current is not None
        old = self.runtime.host.claim(
            ClaimRequest(
                "session-1",
                "claim-request-old",
                "claim-old",
                current.work_id,
                "owner-old",
                30,
                120,
            )
        )
        self.lease_now = old.lease.expires_at
        lines: list[bytes] = []
        supervisor: HostSupervisor

        def emit(line: bytes) -> None:
            lines.append(line)
            event = json.loads(line)
            if event["type"] == "work":
                self.assertEqual("owner-new", event["claim"]["lease"]["owner_id"])
                supervisor.submit(self.submission(event["submission_handle"]))
            elif event["type"] == "status":
                supervisor.close("reclaimed")

        supervisor = self.supervisor(emit)
        self.assertEqual("reclaimed", supervisor.run())

    def test_active_foreign_claim_is_rechecked_without_model_activity(self) -> None:
        current = self.runtime.host.current("session-1")
        self.assertIsNotNone(current)
        assert current is not None
        old = self.runtime.host.claim(
            ClaimRequest(
                "session-1",
                "claim-request-old",
                "claim-old",
                current.work_id,
                "owner-old",
                30,
                120,
            )
        )
        waits = 0
        lines: list[bytes] = []
        supervisor: HostSupervisor

        def wait_strategy(_condition, _timeout) -> None:
            nonlocal waits
            waits += 1
            self.lease_now = old.lease.expires_at

        def emit(line: bytes) -> None:
            lines.append(line)
            event = json.loads(line)
            if event["type"] == "work":
                supervisor.submit(self.submission(event["submission_handle"]))
            elif event["type"] == "status":
                supervisor.close("foreign-lease-reclaimed")

        supervisor = self.supervisor(emit, wait_strategy=wait_strategy)
        self.assertEqual("foreign-lease-reclaimed", supervisor.run())
        self.assertGreaterEqual(waits, 1)
        work = next(item for item in self.parse(lines) if item["type"] == "work")
        self.assertEqual("owner-new", work["claim"]["lease"]["owner_id"])

    def test_submission_handle_and_single_run_guards_fail_closed(self) -> None:
        lines: list[bytes] = []
        failures: list[str] = []
        supervisor: HostSupervisor

        def emit(line: bytes) -> None:
            lines.append(line)
            event = json.loads(line)
            if event["type"] == "work":
                try:
                    supervisor.submit(self.submission("wrong-handle"))
                except HostSupervisorError as exc:
                    failures.append(exc.code)
                supervisor.submit(self.submission(event["submission_handle"]))
                try:
                    supervisor.submit(self.submission(event["submission_handle"]))
                except HostSupervisorError as exc:
                    failures.append(exc.code)
            elif event["type"] == "status":
                supervisor.close("guarded")

        supervisor = self.supervisor(emit)
        with self.assertRaisesRegex(HostSupervisorError, "NO_ACTIVE_WORK"):
            supervisor.submit(self.submission("submission.secret-1"))
        self.assertEqual("guarded", supervisor.run())
        self.assertEqual(
            ["SUBMISSION_HANDLE_INVALID", "SUBMISSION_PENDING"],
            failures,
        )
        with self.assertRaisesRegex(HostSupervisorError, "SUPERVISOR_ALREADY_RUN"):
            supervisor.run()

    def test_preclosed_supervisor_emits_only_ready_and_closed(self) -> None:
        lines: list[bytes] = []
        supervisor = self.supervisor(lines.append)
        supervisor.close("cancelled")

        self.assertEqual("cancelled", supervisor.run())
        events = self.parse(lines)
        self.assertEqual(["ready", "closed"], [item["type"] for item in events])

    def test_monotonic_rollback_closes_with_a_stable_reason(self) -> None:
        lines: list[bytes] = []
        self.monotonic_now = 1

        def wait_strategy(_condition, _timeout) -> None:
            self.monotonic_now = 0.5

        supervisor = self.supervisor(lines.append, wait_strategy=wait_strategy)
        self.assertEqual("MONOTONIC_CLOCK_ROLLED_BACK", supervisor.run())
        events = self.parse(lines)
        self.assertEqual(["ready", "work", "closed"], [x["type"] for x in events])
        self.assertEqual("MONOTONIC_CLOCK_ROLLED_BACK", events[-1]["reason"])

    def test_configuration_and_output_fail_closed(self) -> None:
        with self.assertRaisesRegex(
            HostSupervisorError,
            "SUPERVISOR_CONFIGURATION_INVALID",
        ):
            self.supervisor(lambda _line: None, wait_timeout=56)

        def broken_output(_line: bytes) -> None:
            raise RuntimeError("untrusted-output-detail")

        supervisor = self.supervisor(broken_output)
        with self.assertRaisesRegex(HostSupervisorError, "SUPERVISOR_OUTPUT_FAILED"):
            supervisor.run()

    def test_invalid_configuration_and_submission_values_are_rejected(self) -> None:
        invalid_options = (
            {"retry_interval": float("inf")},
            {"retry_interval": 10**10_000},
            {"io_timeout": 61},
            {"wait_timeout": True},
            {"lease_seconds": True},
            {"max_tenure_seconds": 20},
        )
        for options in invalid_options:
            with self.subTest(options=tuple(options)), self.assertRaisesRegex(
                HostSupervisorError,
                "SUPERVISOR_CONFIGURATION_INVALID",
            ):
                self.supervisor(lambda _line: None, **options)
        supervisor = self.supervisor(lambda _line: None)
        with self.assertRaisesRegex(HostSupervisorError, "CLOSE_REASON_INVALID"):
            supervisor.close("../unsafe")
        invalid = HostSupervisorSubmission(
            "submission.secret-1",
            "publish-candidates-1",
            None,  # type: ignore[arg-type]
            self.config.created_at,
            "host.adapter-1",
        )
        with self.assertRaisesRegex(HostSupervisorError, "SUBMISSION_INVALID"):
            supervisor.submit(invalid)
        invalid_handle = HostSupervisorSubmission(
            "../unsafe",
            "publish-candidates-1",
            TopicCandidatesResult(("One", "Two")),
            self.config.created_at,
            "host.adapter-1",
        )
        with self.assertRaisesRegex(HostSupervisorError, "SUBMISSION_INVALID"):
            supervisor.submit(invalid_handle)

    def test_transport_identity_and_secret_failures_are_stable(self) -> None:
        self.runtime.close_host_ipc()
        lines: list[bytes] = []
        supervisor = self.supervisor(lines.append)
        self.assertEqual("HOST_IPC_ADDRESS_UNAVAILABLE", supervisor.run())
        self.assertEqual(
            ["ready", "closed"],
            [item["type"] for item in self.parse(lines)],
        )

        self.runtime.start_host_ipc(self.socket_path)
        for generator, code in (
            (lambda _prefix: "../invalid", "IDENTITY_GENERATION_FAILED"),
            (lambda _prefix: (_ for _ in ()).throw(RuntimeError()),
             "IDENTITY_GENERATION_FAILED"),
        ):
            lines = []
            supervisor = self.supervisor(lines.append, id_generator=generator)
            self.assertEqual(code, supervisor.run())
            self.assertNotIn("RuntimeError", lines[-1].decode())

        lines = []
        supervisor = self.supervisor(
            lines.append,
            secret_generator=lambda: "../invalid",
        )
        self.assertEqual("SECRET_GENERATION_FAILED", supervisor.run())

        lines = []
        supervisor = self.supervisor(
            lines.append,
            secret_generator=lambda: (_ for _ in ()).throw(RuntimeError()),
        )
        with self.assertRaisesRegex(HostSupervisorError, "SECRET_GENERATION_FAILED"):
            supervisor._handle()

    def test_strict_response_envelope_rejects_malformed_server_data(self) -> None:
        supervisor = self.supervisor(lambda _line: None)
        invalid_payload = canonical_json_bytes(
            {
                "schema_version": SCHEMA_VERSION,
                "protocol_version": PROTOCOL_VERSION,
                "operation": "wait",
                "ok": True,
                "payload": [],
            }
        )
        invalid_error = canonical_json_bytes(
            {
                "schema_version": SCHEMA_VERSION,
                "protocol_version": PROTOCOL_VERSION,
                "operation": "wait",
                "ok": False,
                "error": {"code": "../bad"},
            }
        )
        bad_responses = (
            b"",
            b"{",
            b"[]",
            b'{"ok":true,"ok":false}',
            b'{"ok":true}',
            invalid_payload,
            invalid_error,
        )
        for body in bad_responses:
            with self.subTest(body=body), mock.patch.object(
                supervisor._client,
                "call",
                return_value=HostApiResponse(body),
            ), self.assertRaisesRegex(
                HostSupervisorError,
                "HOST_SUPERVISOR_RESPONSE_INVALID",
            ):
                supervisor._call("wait", {})

    def test_lost_lease_and_publish_rejections_emit_bounded_status(self) -> None:
        lines: list[bytes] = []
        supervisor = self.supervisor(lines.append)
        waiting = supervisor._wait_for_work()
        self.assertIsNotNone(waiting)
        assert waiting is not None
        active = supervisor._claim_or_reclaim(waiting)
        self.assertIsNotNone(active)
        assert active is not None

        rejected = mock.Mock(ok=False, error_code="LEASE_FENCED")
        with mock.patch.object(supervisor, "_call", return_value=rejected):
            self.assertFalse(supervisor._renew(active))
        with mock.patch.object(supervisor, "_call", return_value=rejected):
            self.assertFalse(supervisor._publish(active, self.submission(active.submission_handle)))
        superseded = mock.Mock(ok=False, error_code="WORK_SUPERSEDED")
        with mock.patch.object(supervisor, "_call", return_value=superseded):
            self.assertTrue(supervisor._publish(active, self.submission(active.submission_handle)))
        statuses = self.parse(lines)
        self.assertEqual(
            ["work_lost", "publish_rejected", "publish_rejected"],
            [item["status"] for item in statuses],
        )
        self.assertTrue(
            all(
                "LEASE_FENCED" in line.decode()
                or "WORK_SUPERSEDED" in line.decode()
                for line in lines
            )
        )

        invalid_renewals = (
            {"lease": active.lease, "replayed": "no"},
            {
                "lease": {**active.lease, "lease_version": 1},
                "replayed": False,
            },
        )
        for payload in invalid_renewals:
            with (
                self.subTest(payload=payload),
                mock.patch.object(
                    supervisor,
                    "_call",
                    return_value=mock.Mock(ok=True, payload=payload),
                ),
                self.assertRaisesRegex(
                    HostSupervisorError,
                    "HOST_SUPERVISOR_RESPONSE_INVALID",
                ),
            ):
                supervisor._renew(active)

        valid = {
            "work": active.work,
            "lease": active.lease,
            "fence": active.fence,
            "context": active.context,
        }
        malformed_claims = (
            {**valid, "extra": None},
            {**valid, "lease": {**active.lease, "owner_id": "owner-wrong"}},
            {**valid, "disposition": "claimed", "work_attempt": 2},
        )
        for payload in malformed_claims:
            with self.subTest(payload=tuple(payload)), self.assertRaisesRegex(
                HostSupervisorError,
                "HOST_SUPERVISOR_RESPONSE_INVALID",
            ):
                supervisor._active_claim(payload, 1)

    def test_reclaim_exhaustion_advances_work_without_a_claim(self) -> None:
        lines: list[bytes] = []
        supervisor = self.supervisor(lines.append, wait_strategy=lambda *_: None)
        claim_failed = mock.Mock(ok=False, error_code="LEASE_RECLAIM_REQUIRED")
        requeued = mock.Mock(
            ok=True,
            payload={
                "disposition": "requeued",
                "work_attempt": 1,
                "work_id": "work-1",
                "conversation_version": 1,
                "through_event_sequence": 3,
                "replayed": False,
            },
        )
        with mock.patch.object(
            supervisor,
            "_call",
            side_effect=(claim_failed, requeued),
        ):
            self.assertIsNone(
                supervisor._claim_or_reclaim({"work_id": "work-1", "attempt": 1})
            )
        self.assertEqual("work_advanced", self.parse(lines)[0]["status"])

        still_active = mock.Mock(ok=False, error_code="LEASE_STILL_ACTIVE")
        with mock.patch.object(
            supervisor,
            "_call",
            side_effect=(claim_failed, still_active),
        ):
            self.assertIsNone(
                supervisor._claim_or_reclaim({"work_id": "work-1", "attempt": 1})
            )

    def test_wait_claim_and_clock_failures_have_stable_boundaries(self) -> None:
        lines: list[bytes] = []
        supervisor = self.supervisor(lines.append)
        with mock.patch.object(
            supervisor,
            "_call",
            return_value=mock.Mock(ok=False, error_code="SESSION_DEACTIVATED"),
        ):
            self.assertEqual("SESSION_DEACTIVATED", supervisor.run())

        invalid_wait_payloads = (
            {"timed_out": True, "through_sequence": 1, "work": {}},
            {"timed_out": False, "through_sequence": -1, "work": None},
            {
                "timed_out": False,
                "through_sequence": 1,
                "work": {
                    "session_id": "session-1",
                    "work_id": "../bad",
                    "kind": "topic_candidates",
                    "attempt": 1,
                    "observed_sequence": 1,
                },
            },
        )
        for payload in invalid_wait_payloads:
            candidate = self.supervisor(lambda _line: None)
            with (
                self.subTest(payload=payload),
                mock.patch.object(
                    candidate,
                    "_call",
                    return_value=mock.Mock(ok=True, payload=payload),
                ),
                self.assertRaisesRegex(
                    HostSupervisorError,
                    "HOST_SUPERVISOR_RESPONSE_INVALID",
                ),
            ):
                candidate._wait_for_work()

        clock_values = ("not-a-number", float("nan"), -1, 10**10_000)
        for value in clock_values:
            candidate = self.supervisor(
                lambda _line: None,
                monotonic_clock=lambda value=value: value,
            )
            with self.subTest(value=type(value)), self.assertRaisesRegex(
                HostSupervisorError,
                "MONOTONIC_CLOCK_INVALID",
            ):
                candidate._now()
        candidate = self.supervisor(
            lambda _line: None,
            monotonic_clock=lambda: (_ for _ in ()).throw(RuntimeError()),
        )
        with self.assertRaisesRegex(HostSupervisorError, "MONOTONIC_CLOCK_FAILED"):
            candidate._now()


if __name__ == "__main__":
    unittest.main()
