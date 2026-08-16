from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import tests.xsync_v2_path  # noqa: F401
from xsync_v2.coordinator import DialogueSessionConfig
from xsync_v2.domain import EvidenceCheck, EvidenceHealth, Lens
from xsync_v2.event_codec import PROTOCOL_VERSION, SCHEMA_VERSION, sha256_digest
from xsync_v2.host_api import HostApi, MAX_HOST_API_REQUEST_BYTES
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
                and check.owner_id == "owner-1"
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

    def test_malformed_requests_fail_closed_without_exception_details(self) -> None:
        requests = (
            b'{"schema_version":2,"schema_version":2,'
            b'"protocol_version":"x-sync-dialogue/2","operation":"wait",'
            b'"session_id":"session-1","timeout":0}',
            b'{"schema_version":2,"protocol_version":"x-sync-dialogue/2",'
            b'"operation":"wait","session_id":"session-1","timeout":true}',
            b'{"schema_version":2,"protocol_version":"x-sync-dialogue/2",'
            b'"operation":"deploy"}',
            b'{"schema_version":2.0,"protocol_version":"x-sync-dialogue/2",'
            b'"operation":"wait","session_id":"session-1","timeout":0}',
            b'{"schema_version":2,"protocol_version":"x-sync-dialogue/2",'
            b'"operation":{}}',
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
