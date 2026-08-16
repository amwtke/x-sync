from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "spikes" / "phase0_host_feasibility" / "probe.py"
TOKEN_ENV = "XSYNC_PHASE0_PROBE_TOKEN"
SPEC = importlib.util.spec_from_file_location("phase0_host_probe", PROBE)
probe = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(probe)


class FakeClock:
    def __init__(self, value: float = 1_000.0):
        self.value = value
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self.value

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.value += seconds


class Phase0HostProbeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.clock = FakeClock()
        self.journal = probe.Journal(
            self.root,
            clock=self.clock,
            runtime_epoch="epoch-1",
        )

    def stop_process(self, process: subprocess.Popen[str]) -> None:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()

    def test_journal_persists_and_restart_recovers_unconfirmed_work(self):
        queued = self.journal.enqueue("first browser answer", "request-1")
        claimed = self.journal.claim_next("owner-1", lease_seconds=30)
        self.assertEqual(queued["work_id"], claimed["work_id"])
        self.assertEqual("leased", self.journal.snapshot()["works"][0]["state"])

        restarted = probe.Journal(
            self.root,
            clock=self.clock,
            runtime_epoch="epoch-2",
        )
        recovered = restarted.claim_next("owner-2", lease_seconds=30)

        self.assertEqual(queued["work_id"], recovered["work_id"])
        self.assertEqual("first browser answer", recovered["payload"]["text"])
        self.assertEqual("epoch-2", recovered["runtime_epoch"])
        on_disk = json.loads((self.root / "journal.json").read_text("utf-8"))
        self.assertEqual("epoch-2", on_disk["runtime_epoch"])

    def test_barrier_allows_only_one_concurrent_claim(self):
        self.journal.enqueue("race", "request-race")
        barrier = threading.Barrier(3)
        outcomes = []

        def claim(owner: str) -> None:
            barrier.wait()
            outcomes.append(self.journal.claim_next(owner, lease_seconds=30))

        threads = [
            threading.Thread(target=claim, args=("owner-a",)),
            threading.Thread(target=claim, args=("owner-b",)),
        ]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()

        self.assertEqual(1, sum(item is not None for item in outcomes))

    def test_serve_exclusively_owns_state_directory_across_processes(self):
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        command = [
            sys.executable,
            str(PROBE),
            "serve",
            "--state-dir",
            str(self.root),
        ]
        first = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        self.addCleanup(self.stop_process, first)
        assert first.stdout is not None
        self.assertEqual("ready", json.loads(first.stdout.readline())["type"])

        second = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=3,
            env=environment,
        )
        self.assertEqual(1, second.returncode)
        self.assertIn("STATE_DIR_IN_USE", second.stderr)

        self.stop_process(first)
        replacement = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        self.addCleanup(self.stop_process, replacement)
        assert replacement.stdout is not None
        self.assertEqual("ready", json.loads(replacement.stdout.readline())["type"])

    def test_question_answer_contract_is_idempotent_and_rejects_stale_rounds(self):
        question = self.journal.publish_question(
            "round-1", "What did you observe?", "question-request-1"
        )
        self.assertEqual("awaiting_answer", question["state"])
        self.assertEqual(
            question,
            self.journal.publish_question(
                "round-1", "What did you observe?", "question-request-1"
            ),
        )
        with self.assertRaisesRegex(probe.ProbeError, "IDEMPOTENCY_CONFLICT"):
            self.journal.publish_question(
                "round-1", "Different question", "question-request-1"
            )
        with self.assertRaisesRegex(probe.ProbeError, "QUESTION_ALREADY_OPEN"):
            self.journal.publish_question(
                "round-2", "Too early", "question-request-2"
            )

        answered = self.journal.answer_question(
            "round-1", "The browser answer", "answer-request-1"
        )
        self.assertEqual("answered", answered["question"]["state"])
        durable_answer = self.journal.answer_for("round-1")
        self.assertEqual("The browser answer", durable_answer["text"])
        self.assertEqual("answer-request-1", durable_answer["request_id"])
        self.assertEqual(self.clock.now(), durable_answer["answered_at"])
        self.assertIsNone(self.journal.answer_for("round-missing"))
        restarted = probe.Journal(
            self.root,
            clock=self.clock,
            runtime_epoch="epoch-answer-reader",
        )
        self.assertEqual(durable_answer, restarted.answer_for("round-1"))
        self.assertEqual(
            answered,
            self.journal.answer_question(
                "round-1", "The browser answer", "answer-request-1"
            ),
        )
        with self.assertRaisesRegex(probe.ProbeError, "STALE_ROUND"):
            self.journal.publish_question(
                "round-1", "Reused round", "question-request-reused-round"
            )
        claimed = self.journal.claim_next("owner", lease_seconds=30)
        self.assertEqual(
            {"round_id": "round-1", "text": "The browser answer"},
            claimed["payload"],
        )
        with self.assertRaisesRegex(probe.ProbeError, "STALE_ROUND"):
            self.journal.answer_question(
                "round-0", "old answer", "answer-request-old"
            )

        next_question = self.journal.publish_question(
            "round-2", "Next question", "question-request-2"
        )
        self.assertEqual("round-2", next_question["round_id"])
        state = self.journal.snapshot()
        self.assertEqual("round-2", state["current_question"]["round_id"])

    def test_control_cancels_current_question_without_creating_work(self):
        self.journal.publish_question(
            "round-1", "Pending question", "question-request-1"
        )
        self.journal.control("pause")
        state = self.journal.snapshot()
        self.assertEqual("cancelled", state["current_question"]["state"])
        self.assertEqual("pause", state["current_question"]["control"])
        self.assertEqual([], state["works"])
        with self.assertRaisesRegex(probe.ProbeError, "STALE_ROUND"):
            self.journal.answer_question(
                "round-1", "late answer", "answer-request-late"
            )

    def test_fake_clock_renews_and_stdin_submit_completes_work(self):
        self.journal.enqueue("answer", "request-1")
        session = probe.SupervisorSession(
            probe.LocalBroker(self.journal),
            owner_id="owner-1",
            lease_seconds=30,
            clock=self.clock,
        )
        ready = session.open()
        work = session.poll_once()
        self.assertEqual("ready", ready["type"])
        self.assertEqual("work", work["type"])

        with mock.patch.object(
            probe.time,
            "sleep",
            side_effect=AssertionError("correctness must not use real sleep"),
        ):
            self.clock.advance(11)
            self.assertEqual(1, session.renew_due())
            leased = self.journal.snapshot()["works"][0]
            self.assertEqual(1, leased["renewal_count"])
            self.assertGreater(leased["lease"]["expires_at"], self.clock.now())
            self.assertEqual(1, self.journal.status()["lease_renewals"])

            status = session.handle_stdin_line(
                json.dumps(
                    {
                        "type": "submit",
                        "submission_handle": work["submission_handle"],
                        "idempotency_key": "submit-1",
                        "result": {"reply": "next question"},
                    }
                )
            )
        self.assertEqual("status", status["type"])
        self.assertEqual("completed", status["state"])
        self.assertEqual("completed", self.journal.snapshot()["works"][0]["state"])
        self.assertEqual([1, 2, 3], [ready["stream_sequence"], work["stream_sequence"], status["stream_sequence"]])

    def test_one_session_streams_three_work_items_then_closes(self):
        for number in range(3):
            self.journal.enqueue(f"answer-{number}", f"request-{number}")
        session = probe.SupervisorSession(
            probe.LocalBroker(self.journal),
            owner_id="owner-1",
            lease_seconds=30,
            clock=self.clock,
        )
        envelopes = [session.open()]

        with mock.patch.object(
            probe.time,
            "sleep",
            side_effect=AssertionError("correctness must not use real sleep"),
        ):
            for number in range(3):
                work = session.poll_once()
                envelopes.append(work)
                envelopes.append(
                    session.handle_stdin_line(
                        json.dumps(
                            {
                                "type": "submit",
                                "submission_handle": work["submission_handle"],
                                "idempotency_key": f"submit-{number}",
                                "result": {"reply": f"question-{number + 1}"},
                            }
                        )
                    )
                )
            envelopes.append(session.close("test-complete"))

        self.assertEqual(
            ["ready", "work", "status", "work", "status", "work", "status", "closed"],
            [envelope["type"] for envelope in envelopes],
        )
        self.assertEqual(
            list(range(1, 9)),
            [envelope["stream_sequence"] for envelope in envelopes],
        )

    def test_pause_and_switch_supersede_pending_work_and_reject_late_submit(self):
        for action in ("pause", "switch"):
            with self.subTest(action=action):
                root = self.root / action
                journal = probe.Journal(
                    root,
                    clock=self.clock,
                    runtime_epoch=f"epoch-{action}",
                )
                journal.enqueue("pending", f"request-{action}")
                claimed = journal.claim_next("owner", lease_seconds=30)
                journal.control(action)

                state = journal.snapshot()
                self.assertEqual(action, state["control_state"])
                self.assertEqual("superseded", state["works"][0]["state"])
                with self.assertRaisesRegex(probe.ProbeError, "WORK_SUPERSEDED"):
                    journal.submit(
                        "owner",
                        claimed["submission_handle"],
                        claimed["claim_id"],
                        claimed["lease_version"],
                        claimed["runtime_epoch"],
                        claimed["generation"],
                        "late-submit",
                        {"reply": "too late"},
                    )

    def test_expired_claim_cannot_submit_after_same_owner_reclaims(self):
        self.journal.enqueue("answer", "request-1")
        expired = self.journal.claim_next("stable-owner", lease_seconds=10)
        self.clock.advance(11)
        current = self.journal.claim_next("stable-owner", lease_seconds=10)

        self.assertNotEqual(expired["claim_id"], current["claim_id"])
        self.assertNotEqual(
            expired["submission_handle"], current["submission_handle"]
        )
        self.assertEqual(1, expired["generation"])
        self.assertEqual(2, current["generation"])
        with self.assertRaisesRegex(
            probe.ProbeError, "INVALID_SUBMISSION_HANDLE"
        ):
            self.journal.submit(
                "stable-owner",
                expired["submission_handle"],
                expired["claim_id"],
                expired["lease_version"],
                expired["runtime_epoch"],
                expired["generation"],
                "expired-submit",
                {"reply": "stale"},
            )

        fence = {
            "claim_id": current["claim_id"],
            "lease_version": current["lease_version"],
            "runtime_epoch": current["runtime_epoch"],
            "generation": current["generation"],
        }
        invalid_fences = {
            "claim_id": "wrong-claim",
            "lease_version": current["lease_version"] + 1,
            "runtime_epoch": "wrong-epoch",
            "generation": current["generation"] + 1,
        }
        for field, invalid_value in invalid_fences.items():
            with self.subTest(field=field):
                attempted = dict(fence)
                attempted[field] = invalid_value
                with self.assertRaisesRegex(probe.ProbeError, "LEASE_FENCED"):
                    self.journal.submit(
                        "stable-owner",
                        current["submission_handle"],
                        attempted["claim_id"],
                        attempted["lease_version"],
                        attempted["runtime_epoch"],
                        attempted["generation"],
                        f"invalid-{field}",
                        {"reply": "stale"},
                    )

        completed = self.journal.submit(
            "stable-owner",
            current["submission_handle"],
            current["claim_id"],
            current["lease_version"],
            current["runtime_epoch"],
            current["generation"],
            "current-submit",
            {"reply": "current"},
        )
        self.assertEqual("completed", completed["state"])

    def test_idempotent_submit_returns_receipt_and_conflict_fails(self):
        self.journal.enqueue("answer", "request-1")
        claimed = self.journal.claim_next("owner", lease_seconds=30)
        first = self.journal.submit(
            "owner",
            claimed["submission_handle"],
            claimed["claim_id"],
            claimed["lease_version"],
            claimed["runtime_epoch"],
            claimed["generation"],
            "submit-1",
            {"reply": "same"},
        )
        again = self.journal.submit(
            "owner",
            claimed["submission_handle"],
            claimed["claim_id"],
            claimed["lease_version"],
            claimed["runtime_epoch"],
            claimed["generation"],
            "submit-1",
            {"reply": "same"},
        )
        self.assertEqual(first, again)
        with self.assertRaisesRegex(probe.ProbeError, "IDEMPOTENCY_CONFLICT"):
            self.journal.submit(
                "owner",
                claimed["submission_handle"],
                claimed["claim_id"],
                claimed["lease_version"],
                claimed["runtime_epoch"],
                claimed["generation"],
                "submit-1",
                {"reply": "different"},
            )

    def test_http_is_loopback_authenticated_and_origin_checked(self):
        server = probe.make_server(self.journal, token="probe-secret")
        self.addCleanup(server.server_close)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        host, port = server.server_address
        self.assertEqual("127.0.0.1", host)
        url = f"http://127.0.0.1:{port}/state"

        with self.assertRaises(urllib.error.HTTPError) as unauthorized:
            urllib.request.urlopen(url)
        self.assertEqual(401, unauthorized.exception.code)
        unauthorized.exception.close()

        request = urllib.request.Request(
            url,
            headers={"Authorization": "Bearer probe-secret"},
        )
        with urllib.request.urlopen(request) as response:
            self.assertEqual(200, response.status)
            self.assertEqual(1, json.load(response)["schema_version"])

        bad_origin = urllib.request.Request(
            "http://127.0.0.1:%d/enqueue" % port,
            data=json.dumps({"text": "x", "request_id": "r"}).encode(),
            headers={
                "Authorization": "Bearer probe-secret",
                "Content-Type": "application/json",
                "Origin": "https://evil.example",
            },
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as forbidden:
            urllib.request.urlopen(bad_origin)
        self.assertEqual(403, forbidden.exception.code)
        forbidden.exception.close()

    def test_http_broker_submits_with_current_lease_fence(self):
        self.journal.enqueue("answer", "request-http-submit")
        server = probe.make_server(self.journal, token="probe-secret")
        self.addCleanup(server.server_close)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        host, port = server.server_address
        broker = probe.HttpBroker(f"http://{host}:{port}", "probe-secret")
        opened = broker.open_supervisor("http-owner")
        item = broker.next_for_supervisor(
            "http-owner",
            opened["journal_sequence"],
            lease_seconds=30,
            timeout=0,
        )
        self.assertEqual("work", item["kind"])

        with self.assertRaisesRegex(probe.ProbeError, "LEASE_FENCED"):
            broker.submit(
                "http-owner",
                item["submission_handle"],
                item["claim_id"],
                item["lease_version"],
                "wrong-epoch",
                item["generation"],
                "http-invalid-submit",
                {"reply": "stale"},
            )
        completed = broker.submit(
            "http-owner",
            item["submission_handle"],
            item["claim_id"],
            item["lease_version"],
            item["runtime_epoch"],
            item["generation"],
            "http-current-submit",
            {"reply": "current"},
        )
        self.assertEqual("completed", completed["state"])

    def test_supervisor_stdin_eof_releases_lease_and_token_stays_out_of_argv(self):
        self.journal.enqueue("answer", "request-eof")
        token = "probe-secret-not-in-argv"
        server = probe.make_server(self.journal, token=token)
        self.addCleanup(server.server_close)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        host, port = server.server_address
        environment = os.environ.copy()
        environment[TOKEN_ENV] = token
        command = [
            sys.executable,
            str(PROBE),
            "supervise",
            "--url",
            f"http://{host}:{port}",
            "--token-env",
            TOKEN_ENV,
            "--owner",
            "owner-eof",
            "--lease-seconds",
            "30",
        ]
        self.assertNotIn(token, command)
        supervisor = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        self.addCleanup(self.stop_process, supervisor)
        assert supervisor.stdout is not None
        assert supervisor.stdin is not None
        ready = json.loads(supervisor.stdout.readline())
        work = json.loads(supervisor.stdout.readline())
        self.assertEqual("ready", ready["type"])
        self.assertEqual("work", work["type"])
        self.assertEqual("leased", self.journal.snapshot()["works"][0]["state"])
        process_argv = subprocess.check_output(
            ["ps", "-o", "command=", "-p", str(supervisor.pid)],
            text=True,
        )
        self.assertIn("--token-env", process_argv)
        self.assertNotIn(token, process_argv)

        supervisor.stdin.close()
        self.assertEqual(0, supervisor.wait(timeout=3))
        remaining = [
            json.loads(line)
            for line in supervisor.stdout.read().splitlines()
            if line.strip()
        ]
        self.assertTrue(
            any(
                envelope.get("type") == "closed"
                and envelope.get("reason") == "stdin-eof"
                for envelope in remaining
            )
        )
        self.assertEqual("queued", self.journal.snapshot()["works"][0]["state"])

    def test_probe_assets_are_scoped_and_schema_is_loadable(self):
        html = (PROBE.parent / "probe.html").read_text(encoding="utf-8")
        schema = json.loads(
            (PROBE.parent / "result.schema.json").read_text(encoding="utf-8")
        )
        codex_result = json.loads(
            (
                PROBE.parent
                / "results"
                / "codex-0.147.0-20260816.json"
            ).read_text(encoding="utf-8")
        )
        self.assertIn("Phase 0 Host Feasibility", html)
        self.assertEqual(
            ["GO", "CONDITIONAL", "NO-GO"],
            schema["properties"]["verdict"]["enum"],
        )
        self.assertTrue(set(schema["required"]).issubset(codex_result))
        self.assertTrue(set(codex_result).issubset(schema["properties"]))
        self.assertEqual("NO-GO", codex_result["verdict"])
        self.assertFalse(codex_result["reconnect"])
        self.assertIsNone(codex_result["idle_model_calls"])
        self.assertNotIn("xsync_v2", PROBE.read_text(encoding="utf-8"))
        self.assertNotIn("skills.x-sync", PROBE.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
