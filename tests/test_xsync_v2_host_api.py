# ruff: noqa: I001
from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.coordinator import DialogueSessionConfig
from xsync_v2.domain import EvidenceCheck, EvidenceHealth, Lens
from xsync_v2.event_codec import PROTOCOL_VERSION, SCHEMA_VERSION, sha256_digest
from xsync_v2.host_api import MAX_HOST_API_REQUEST_BYTES, HostApi
from xsync_v2.host_context import EvidenceContextClaim, HostContextSource
from xsync_v2.runtime import DialogueRuntime


def digest(label: str) -> str:
    return sha256_digest(label.encode())


class ApiContextProvider:
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


class HostApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name).resolve()
        self.root = root
        self.now = 1_000
        self.runtime = DialogueRuntime(
            root,
            "registry-1",
            "runtime-1",
            evidence_verifier=lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            context_provider=ApiContextProvider(),
            runtime_authority_verifier=lambda check, _authority: (
                check.runtime_epoch == "runtime-1"
                and check.owner_id in {"owner-1", "owner-2", "owner-3", "owner-4"}
            ),
            lease_clock=lambda: self.now,
            browser_clock=lambda: "2026-08-16T18:00:00+08:00",
            monotonic_clock=lambda: float(self.now),
            durable_poll_interval=0.01,
        )
        self.addCleanup(self.runtime.close)
        self.config = DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repository-1",
            "2026-08-16T18:00:00+08:00",
            "runtime-1",
            digest("evidence"),
        )
        self.runtime.resolve(self.config)
        self.api = self.runtime.host_api
        self.assertIs(type(self.api), HostApi)

    def request(self, operation: str, **payload):
        body = json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "protocol_version": PROTOCOL_VERSION,
                "operation": operation,
                **payload,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        response = self.api.handle(body).body
        value = json.loads(response)
        self.assertNotIn(b"\n", response)
        return value

    def claim(self):
        waited = self.request("wait", session_id="session-1", timeout=0)
        self.assertTrue(waited["ok"])
        work_id = waited["payload"]["work"]["work_id"]
        claimed = self.request(
            "claim",
            session_id="session-1",
            request_id="claim-request-1",
            claim_id="claim-1",
            work_id=work_id,
            owner_id="owner-1",
            lease_seconds=30,
            max_tenure_seconds=120,
        )
        self.assertTrue(claimed["ok"])
        return claimed["payload"]

    def test_wait_claim_and_publish_cross_only_canonical_json(self) -> None:
        claimed = self.claim()
        self.assertEqual("topic_candidates", claimed["work"]["kind"])
        self.assertEqual("host_context_capsule", claimed["context"]["record_type"])
        self.assertNotIn("lease", claimed["context"])
        publish = {
            "idempotency_key": "publish-candidates-1",
            "work": claimed["work"],
            "fence": claimed["fence"],
            "result": {
                "type": "topic_candidates",
                "candidates": ["Registry fencing", "Lease recovery"],
            },
            "occurred_at": self.config.created_at,
            "actor_id": "host.adapter-1",
        }

        first = self.request("publish", **publish)
        self.assertTrue(first["ok"])
        self.assertFalse(first["payload"]["replayed"])
        self.assertEqual(2, first["payload"]["conversation_version"])
        resolution = self.runtime.browser.current("session-1")
        self.assertEqual("choosing_topic", resolution.dialogue_state.phase.value)

        self.now = 2_000
        publish["fence"] = {
            **publish["fence"],
            "lease_version": 99,
        }
        replay = self.request("publish", **publish)
        self.assertTrue(replay["ok"])
        self.assertTrue(replay["payload"]["replayed"])
        self.assertEqual(
            first["payload"]["transaction_id"],
            replay["payload"]["transaction_id"],
        )

        publish["result"] = {
            "type": "topic_candidates",
            "candidates": ["Different result"],
        }
        conflict = self.request("publish", **publish)
        self.assertFalse(conflict["ok"])
        self.assertEqual("IDEMPOTENCY_CONFLICT", conflict["error"]["code"])

    def test_renew_is_a_typed_one_response_operation(self) -> None:
        claimed = self.claim()
        lease = claimed["lease"]
        self.now += 10

        renewed = self.request(
            "renew",
            session_id="session-1",
            request_id="renew-request-1",
            claim_id=lease["claim_id"],
            work_id=lease["work_id"],
            owner_id=lease["owner_id"],
            expected_lease_version=lease["lease_version"],
            lease_seconds=30,
        )

        self.assertTrue(renewed["ok"])
        self.assertFalse(renewed["payload"]["replayed"])
        self.assertEqual(2, renewed["payload"]["lease"]["lease_version"])

    def test_opaque_submit_uses_latest_fence_and_replays_after_restart(self) -> None:
        claimed = self.claim()
        handle = "submission.secret-1"
        registered = self.request(
            "register_submission",
            submission_handle=handle,
            work=claimed["work"],
            fence=claimed["fence"],
        )
        self.assertTrue(registered["ok"])
        self.assertFalse(registered["payload"]["replayed"])
        self.assertNotIn(handle, json.dumps(registered))

        self.now += 10
        renewed = self.request(
            "renew",
            session_id="session-1",
            request_id="renew-request-opaque-1",
            claim_id=claimed["lease"]["claim_id"],
            work_id=claimed["lease"]["work_id"],
            owner_id=claimed["lease"]["owner_id"],
            expected_lease_version=1,
            lease_seconds=30,
        )
        self.assertEqual(2, renewed["payload"]["lease"]["lease_version"])

        submission = {
            "submission_handle": handle,
            "idempotency_key": "submit-candidates-1",
            "result": {
                "type": "topic_candidates",
                "candidates": ["Registry fencing", "Lease recovery"],
            },
            "occurred_at": self.config.created_at,
            "actor_id": "host.adapter-1",
        }
        first = self.request("submit", **submission)
        self.assertTrue(first["ok"])
        self.assertFalse(first["payload"]["replayed"])

        self.runtime.close()
        restarted = DialogueRuntime(
            self.root,
            "registry-1",
            "runtime-2",
            evidence_verifier=lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            context_provider=ApiContextProvider(),
            runtime_authority_verifier=lambda _check, _authority: True,
            lease_clock=lambda: self.now + 1_000,
            browser_clock=lambda: "2026-08-16T18:00:00+08:00",
            monotonic_clock=lambda: float(self.now + 1_000),
            durable_poll_interval=0.01,
        )
        self.addCleanup(restarted.close)
        self.runtime = restarted
        self.api = restarted.host_api
        replay = self.request("submit", **submission)
        self.assertTrue(replay["ok"])
        self.assertTrue(replay["payload"]["replayed"])
        self.assertEqual(
            first["payload"]["transaction_id"],
            replay["payload"]["transaction_id"],
        )

        submission["result"] = {
            "type": "topic_candidates",
            "candidates": ["Different"],
        }
        conflict = self.request("submit", **submission)
        self.assertFalse(conflict["ok"])
        self.assertEqual("IDEMPOTENCY_CONFLICT", conflict["error"]["code"])

    def test_submission_registration_and_secret_mismatch_fail_closed(self) -> None:
        claimed = self.claim()
        wrong = self.request(
            "register_submission",
            submission_handle="submission.secret-1",
            work=claimed["work"],
            fence={**claimed["fence"], "claim_id": "claim-wrong"},
        )
        self.assertFalse(wrong["ok"])
        self.assertEqual("LEASE_FENCED", wrong["error"]["code"])

        missing = self.request(
            "submit",
            submission_handle="submission.unknown",
            idempotency_key="submit-missing-1",
            result={
                "type": "topic_candidates",
                "candidates": ["Registry fencing"],
            },
            occurred_at=self.config.created_at,
            actor_id="host.adapter-1",
        )
        self.assertFalse(missing["ok"])
        self.assertEqual(
            "SUBMISSION_HANDLE_NOT_FOUND",
            missing["error"]["code"],
        )

    def test_reclaim_recovers_expired_tenures_and_advances_exhaustion(self) -> None:
        claimed = self.claim()
        lease = claimed["lease"]

        def reclaim(number):
            return self.request(
                "reclaim",
                session_id="session-1",
                request_id=f"reclaim-request-{number}",
                claim_id=f"claim-reclaimed-{number}",
                work_id=lease["work_id"],
                owner_id=f"owner-{number + 1}",
                expected_work_attempt=1,
                lease_seconds=30,
                max_tenure_seconds=120,
                occurred_at=self.config.created_at,
                actor_id="runtime.supervisor-1",
            )

        self.now = lease["expires_at"]
        first = reclaim(1)
        self.assertTrue(first["ok"])
        self.assertEqual("claimed", first["payload"]["disposition"])
        self.assertEqual(lease["work_id"], first["payload"]["work"]["work_id"])

        self.now = first["payload"]["lease"]["expires_at"]
        second = reclaim(2)
        self.assertTrue(second["ok"])
        self.assertEqual("claimed", second["payload"]["disposition"])

        self.now = second["payload"]["lease"]["expires_at"]
        exhausted = reclaim(3)
        self.assertTrue(exhausted["ok"])
        self.assertEqual("requeued", exhausted["payload"]["disposition"])
        self.assertEqual(1, exhausted["payload"]["work_attempt"])
        self.assertFalse(exhausted["payload"]["replayed"])

        replayed = reclaim(3)
        self.assertTrue(replayed["ok"])
        self.assertTrue(replayed["payload"]["replayed"])
        self.assertEqual(
            exhausted["payload"]["through_event_sequence"],
            replayed["payload"]["through_event_sequence"],
        )

    def test_malformed_requests_fail_closed_without_exception_details(self) -> None:
        requests = (
            (
                b'{"schema_version":2,"schema_version":2,'
                b'"protocol_version":"x-sync-dialogue/2","operation":"wait",'
                b'"session_id":"session-1","timeout":0}'
            ),
            (
                b'{"schema_version":2,"protocol_version":"x-sync-dialogue/2",'
                b'"operation":"wait","session_id":"session-1","timeout":true}'
            ),
            (
                b'{"schema_version":2,"protocol_version":"x-sync-dialogue/2",'
                b'"operation":"deploy"}'
            ),
            (
                b'{"schema_version":2.0,"protocol_version":"x-sync-dialogue/2",'
                b'"operation":"wait","session_id":"session-1","timeout":0}'
            ),
            (
                b'{"schema_version":2,"protocol_version":"x-sync-dialogue/2",'
                b'"operation":{}}'
            ),
            b"{" + b" " * MAX_HOST_API_REQUEST_BYTES + b"}",
        )
        for raw in requests:
            with self.subTest(raw=raw[:72]):
                response = json.loads(self.api.handle(raw).body)
                self.assertFalse(response["ok"])
                self.assertNotIn("traceback", json.dumps(response).lower())
                self.assertNotIn(str(Path(self.temporary.name)), json.dumps(response))

    def test_publish_rejects_mutated_work_before_state_machine_entry(self) -> None:
        claimed = self.claim()
        claimed["work"]["binding_digest"] = digest("forged-binding")

        response = self.request(
            "publish",
            idempotency_key="publish-forged",
            work=claimed["work"],
            fence=claimed["fence"],
            result={
                "type": "topic_candidates",
                "candidates": ["Registry fencing"],
            },
            occurred_at=self.config.created_at,
            actor_id="host.adapter-1",
        )

        self.assertFalse(response["ok"])
        self.assertEqual("HOST_API_SCHEMA_INVALID", response["error"]["code"])


if __name__ == "__main__":
    unittest.main()
