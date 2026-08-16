from __future__ import annotations

import importlib.util
import hashlib
import io
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.request


HERE = Path(__file__).resolve().parent
SPIKE = HERE.parent


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


mcp = load("phase0_mcp_server", SPIKE / "mcp_server.py")
runner = load("phase0_codex_runner", SPIKE / "codex_runner.py")


class FakeClock:
    def __init__(self) -> None:
        self.value = 100.0

    def monotonic(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class FakeJournal:
    def __init__(self) -> None:
        self.published = []
        self.answers = {}
        self.control_state = "running"
        self.current_question = None
        self._questions = {}
        self._lock = threading.Lock()

    def publish_question(self, round_id, question, request_id):
        value = {
            "round_id": round_id,
            "question": question,
            "request_id": request_id,
            "state": "awaiting_answer",
        }
        with self._lock:
            self.published.append(value)
            existing = self._questions.get(request_id)
            if existing is not None:
                if (
                    existing["round_id"] != round_id
                    or existing["question"] != question
                ):
                    raise mcp.StateContractError("IDEMPOTENCY_CONFLICT")
                return dict(existing)
            if self.current_question is not None and self.current_question[
                "state"
            ] == "awaiting_answer":
                raise mcp.StateContractError("QUESTION_ALREADY_OPEN")
            self._questions[request_id] = value
            self.current_question = value
            return dict(value)

    def answer_for(self, round_id):
        with self._lock:
            return self.answers.get(round_id)

    def snapshot(self):
        with self._lock:
            return {
                "control_state": self.control_state,
                "current_question": (
                    None
                    if self.current_question is None
                    else dict(self.current_question)
                ),
            }

    def record_answer(self, round_id, text, request_id="browser-recovery"):
        with self._lock:
            answer = {
                "round_id": round_id,
                "text": text,
                "request_id": request_id,
                "answered_at": 123.0,
            }
            self.answers[round_id] = answer
            if (
                self.current_question is not None
                and self.current_question["round_id"] == round_id
            ):
                self.current_question["state"] = "answered"
            return answer


class QueueInput:
    STOP = object()

    def __init__(self) -> None:
        self.lines = queue.Queue()

    def send(self, message) -> None:
        self.lines.put(json.dumps(message) + "\n")

    def close(self) -> None:
        self.lines.put(self.STOP)

    def __iter__(self):
        return self

    def __next__(self):
        item = self.lines.get(timeout=2)
        if item is self.STOP:
            raise StopIteration
        return item


class CapturingOutput(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.condition = threading.Condition()

    def write(self, value):
        with self.condition:
            result = super().write(value)
            self.condition.notify_all()
            return result

    def messages(self):
        with self.condition:
            return [json.loads(line) for line in self.getvalue().splitlines()]

    def wait_for(self, predicate, timeout=2):
        with self.condition:
            return self.condition.wait_for(
                lambda: any(predicate(item) for item in self.messages()),
                timeout=timeout,
            )


def valid_codex_events():
    events = [
        {"type": "thread.started", "thread_id": "thread-1"},
        {"type": "turn.started"},
    ]
    for round_number in (1, 2, 3):
        item_id = f"call-{round_number}"
        arguments = {
            "round": round_number,
            "question": f"distinct question {round_number}",
        }
        events.append(
            {
                "type": "item.started",
                "item": {
                    "id": item_id,
                    "type": "mcp_tool_call",
                    "server": "xsync_spike",
                    "tool": "publish_and_wait",
                    "arguments": arguments,
                    "status": "in_progress",
                },
            }
        )
        events.append(
            {
                "type": "item.completed",
                "item": {
                    "id": item_id,
                    "type": "mcp_tool_call",
                    "server": "xsync_spike",
                    "tool": "publish_and_wait",
                    "arguments": arguments,
                    "error": None,
                    "status": "completed",
                    "result": {
                        "structuredContent": {
                            "round": round_number,
                            "answer": f"private browser answer {round_number}",
                        },
                        "isError": False,
                    },
                },
            }
        )
    events.extend(
        [
            {
                "type": "item.completed",
                "item": {
                    "id": "final-message",
                    "type": "agent_message",
                    "text": "Private final confirmation.",
                },
            },
            {"type": "turn.completed"},
        ]
    )
    return events


class Phase0MCPServerTest(unittest.TestCase):
    def test_service_publishes_durably_then_returns_matching_answer(self):
        journal = FakeJournal()
        clock = FakeClock()
        waits = 0

        def wait(cancelled, timeout):
            nonlocal waits
            waits += 1
            clock.advance(timeout)
            if waits == 3:
                journal.answers["1"] = {
                    "round_id": "1",
                    "text": "Because the host turn is still pending.",
                    "request_id": "browser-1",
                    "answered_at": 101.0,
                }
            return cancelled.is_set()

        progress = []
        service = mcp.PublishAndWaitService(
            journal,
            clock=clock,
            wait=wait,
            poll_interval=0.25,
            progress_interval=0.5,
        )
        result = service.publish_and_wait(
            1,
            "Why must the tool stay pending?",
            cancelled=threading.Event(),
            progress=lambda value, message: progress.append((value, message)),
        )

        self.assertEqual("1", journal.published[0]["round_id"])
        self.assertEqual("mcp-question-1", journal.published[0]["request_id"])
        self.assertEqual("Because the host turn is still pending.", result["answer"])
        self.assertEqual("browser-1", result["answer_request_id"])
        self.assertGreaterEqual(len(progress), 2)

    def test_stdio_single_flight_cancel_preserves_and_exact_retry_recovers(self):
        journal = FakeJournal()
        service = mcp.PublishAndWaitService(
            journal,
            poll_interval=0.01,
            progress_interval=0.01,
        )
        input_stream = QueueInput()
        output = CapturingOutput()
        server = mcp.MCPServer(service, output)
        thread = threading.Thread(target=server.serve, args=(input_stream,))
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(input_stream.close)

        input_stream.send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-06-18"},
            }
        )
        input_stream.send(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        )
        input_stream.send(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "publish_and_wait",
                    "arguments": {"round": 1, "question": "One question?"},
                    "_meta": {"progressToken": "progress-3"},
                },
            }
        )
        self.assertTrue(
            output.wait_for(
                lambda item: item.get("method") == "notifications/progress"
            )
        )
        input_stream.send(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "publish_and_wait",
                    "arguments": {"round": 1, "question": "One question?"},
                },
            }
        )
        self.assertTrue(
            output.wait_for(
                lambda item: item.get("id") == 4
                and item.get("error", {}).get("code")
                == mcp.ROUND_ALREADY_PENDING
            )
        )
        input_stream.send(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "publish_and_wait",
                    "arguments": {"round": 2, "question": "Too early?"},
                },
            }
        )
        self.assertTrue(
            output.wait_for(
                lambda item: item.get("id") == 5
                and item.get("error", {}).get("code")
                == mcp.ANOTHER_ROUND_PENDING
            )
        )
        with server._active_lock:
            self.assertEqual(1, len(server._active))
            self.assertEqual(1, len(server._workers))
        input_stream.send(
            {
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": 3, "reason": "test cancellation"},
            }
        )
        self.assertTrue(
            output.wait_for(
                lambda item: item.get("id") == 3
                and item.get("error", {}).get("code") == -32800
            )
        )
        cancelled = next(item for item in output.messages() if item.get("id") == 3)
        self.assertEqual(
            "preserved", cancelled["error"]["data"]["durable_question"]
        )
        self.assertEqual(
            "awaiting_answer", journal.snapshot()["current_question"]["state"]
        )

        # Recovery is exact: changing the question under the same deterministic
        # round request id is an idempotency conflict.
        input_stream.send(
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "publish_and_wait",
                    "arguments": {"round": 1, "question": "Changed question?"},
                },
            }
        )
        self.assertTrue(
            output.wait_for(
                lambda item: item.get("id") == 6
                and item.get("error", {}).get("code") == -32603
                and "IDEMPOTENCY_CONFLICT"
                in str(item.get("error", {}).get("data"))
            )
        )

        # Cancellation ends only the transport wait. A new round cannot jump
        # over the unanswered durable question.
        input_stream.send(
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "publish_and_wait",
                    "arguments": {"round": 2, "question": "Still too early?"},
                },
            }
        )
        self.assertTrue(
            output.wait_for(
                lambda item: item.get("id") == 7
                and item.get("error", {}).get("code") == -32603
                and "QUESTION_ALREADY_OPEN"
                in str(item.get("error", {}).get("data"))
            )
        )

        # The browser may answer while no Host wait is attached. A later exact
        # retry recovers that durable answer instead of republishing a new round.
        journal.record_answer("1", "Recovered after transport cancellation.")
        input_stream.send(
            {
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "publish_and_wait",
                    "arguments": {"round": 1, "question": "One question?"},
                },
            }
        )
        self.assertTrue(
            output.wait_for(
                lambda item: item.get("id") == 8
                and item.get("result", {}).get("structuredContent", {}).get("answer")
                == "Recovered after transport cancellation."
            )
        )
        input_stream.close()
        thread.join(2)

        messages = output.messages()
        tools = next(
            item["result"]["tools"] for item in messages if item.get("id") == 2
        )
        self.assertEqual(["publish_and_wait"], [item["name"] for item in tools])
        self.assertFalse(thread.is_alive())

    def test_real_stdio_process_recovers_durable_answer_without_token_in_argv(self):
        with tempfile.TemporaryDirectory() as temporary:
            probe_process = subprocess.Popen(
                [
                    sys.executable,
                    "-u",
                    str(SPIKE / "probe.py"),
                    "serve",
                    "--state-dir",
                    temporary,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            mcp_process = None
            reader = None
            try:
                ready = json.loads(probe_process.stdout.readline())
                token = ready["token"]
                url = ready["url"]
                environment = os.environ.copy()
                environment[runner.PROBE_TOKEN_ENV] = token
                command = [
                    sys.executable,
                    "-u",
                    str(SPIKE / "mcp_server.py"),
                    "--probe-url",
                    url,
                    "--probe-token-env",
                    runner.PROBE_TOKEN_ENV,
                    "--poll-interval",
                    "0.02",
                    "--progress-interval",
                    "0.02",
                ]
                self.assertNotIn(token, " ".join(command))
                mcp_process = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=environment,
                )
                messages = queue.Queue()

                def read_messages():
                    for line in mcp_process.stdout:
                        messages.put(json.loads(line))

                reader = threading.Thread(target=read_messages, daemon=True)
                reader.start()

                def send(message):
                    mcp_process.stdin.write(json.dumps(message) + "\n")
                    mcp_process.stdin.flush()

                def receive(predicate):
                    for _ in range(100):
                        message = messages.get(timeout=3)
                        if predicate(message):
                            return message
                    self.fail("matching MCP response was not received")

                def http_json(path, body=None):
                    data = None if body is None else json.dumps(body).encode()
                    request = urllib.request.Request(
                        url + path,
                        data=data,
                        headers={
                            "Authorization": f"Bearer {token}",
                            **(
                                {}
                                if data is None
                                else {"Content-Type": "application/json"}
                            ),
                        },
                        method="GET" if data is None else "POST",
                    )
                    with urllib.request.urlopen(request, timeout=2) as response:
                        return json.load(response)

                send(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {"protocolVersion": "2025-06-18"},
                    }
                )
                receive(lambda item: item.get("id") == 1)
                tool = {
                    "name": "publish_and_wait",
                    "arguments": {
                        "round": 1,
                        "question": "Exact recovery question?",
                    },
                    "_meta": {"progressToken": "progress-2"},
                }
                send(
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/call",
                        "params": tool,
                    }
                )
                receive(
                    lambda item: item.get("method") == "notifications/progress"
                )
                send(
                    {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "tools/call",
                        "params": tool,
                    }
                )
                duplicate = receive(lambda item: item.get("id") == 3)
                self.assertEqual(
                    mcp.ROUND_ALREADY_PENDING, duplicate["error"]["code"]
                )
                send(
                    {
                        "jsonrpc": "2.0",
                        "method": "notifications/cancelled",
                        "params": {"requestId": 2, "reason": "test disconnect"},
                    }
                )
                cancelled = receive(lambda item: item.get("id") == 2)
                self.assertEqual(-32800, cancelled["error"]["code"])
                self.assertEqual(
                    "awaiting_answer",
                    http_json("/state")["current_question"]["state"],
                )
                http_json(
                    "/question/answer",
                    {
                        "round_id": "1",
                        "text": "Recovered durable answer.",
                        "request_id": "browser-cross-process",
                    },
                )
                send(
                    {
                        "jsonrpc": "2.0",
                        "id": 4,
                        "method": "tools/call",
                        "params": tool,
                    }
                )
                recovered = receive(lambda item: item.get("id") == 4)
                self.assertEqual(
                    "Recovered durable answer.",
                    recovered["result"]["structuredContent"]["answer"],
                )
                mcp_process.stdin.close()
                self.assertEqual(0, mcp_process.wait(timeout=3))
                reader.join(3)
                self.assertEqual("", mcp_process.stderr.read())
            finally:
                if mcp_process is not None and mcp_process.poll() is None:
                    mcp_process.terminate()
                    mcp_process.wait(timeout=3)
                if reader is not None:
                    reader.join(3)
                if probe_process.poll() is None:
                    probe_process.terminate()
                    probe_process.wait(timeout=3)
                for process in (mcp_process, probe_process):
                    if process is None:
                        continue
                    for stream in (process.stdin, process.stdout, process.stderr):
                        if stream is not None and not stream.closed:
                            stream.close()

    def test_runner_uses_one_fake_subprocess_and_validates_three_rounds(self):
        events = valid_codex_events()
        stdout = "\n".join(json.dumps(event) for event in events)
        calls = []

        def fake_subprocess(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            trace_path = root / "sanitized-trace.json"
            evidence = runner.run_codex(
                codex_binary="codex-test-double",
                python_binary=sys.executable,
                server_script=SPIKE / "mcp_server.py",
                probe_url="http://127.0.0.1:32123",
                probe_token="probe-secret",
                repository=root,
                trace_artifact=trace_path,
                run_process=fake_subprocess,
            )

            trace_text = trace_path.read_text(encoding="utf-8")
            trace = json.loads(trace_text)
            self.assertEqual(
                "x-sync-phase0-codex-sanitized-trace", trace["kind"]
            )
            self.assertEqual(0o600, trace_path.stat().st_mode & 0o777)
            self.assertNotIn("probe-secret", trace_text)
            self.assertNotIn("private browser answer", trace_text)
            self.assertNotIn("distinct question", trace_text)
            self.assertNotIn("Private final confirmation", trace_text)
            self.assertEqual(
                evidence.trace_artifact.sha256,
                hashlib.sha256(trace_path.read_bytes()).hexdigest(),
            )

        self.assertEqual(1, len(calls))
        command = calls[0][0]
        self.assertEqual("codex-test-double", command[0])
        self.assertIn("--ephemeral", command)
        self.assertIn("--json", command)
        self.assertIn("--ignore-user-config", command)
        self.assertNotIn("probe-secret", " ".join(command))
        self.assertEqual(
            "probe-secret",
            calls[0][1]["env"][runner.PROBE_TOKEN_ENV],
        )
        self.assertTrue(
            any(
                value.startswith("mcp_servers.xsync_spike.command=")
                for value in command
            )
        )
        self.assertEqual((1, 2, 3), evidence.completed_rounds)
        self.assertEqual("thread-1", evidence.thread_id)
        self.assertEqual("same_turn_three_rounds", evidence.summary()["gate"])
        self.assertEqual("PASS", evidence.summary()["status"])
        self.assertTrue(evidence.summary()["single_codex_invocation"])
        self.assertFalse(evidence.summary()["process_tree_verified"])
        self.assertNotIn("single_process", evidence.summary())
        self.assertNotIn("verdict", evidence.summary())

    def test_runner_rejects_missing_starts_overlap_and_early_final(self):
        missing_starts = [
            event
            for event in valid_codex_events()
            if event.get("type") != "item.started"
        ]
        with self.assertRaisesRegex(runner.RunnerError, "three started"):
            runner.validate_trace(["codex"], missing_starts)

        overlapping = valid_codex_events()
        overlapping[3], overlapping[4] = overlapping[4], overlapping[3]
        with self.assertRaisesRegex(runner.RunnerError, "overlapping"):
            runner.validate_trace(["codex"], overlapping)

        early_final = valid_codex_events()
        final = early_final.pop(8)
        early_final.insert(6, final)
        with self.assertRaisesRegex(runner.RunnerError, "round 3"):
            runner.validate_trace(["codex"], early_final)

    def test_runner_reads_outer_token_from_environment_not_argv(self):
        parsed = runner.build_parser().parse_args(
            ["--probe-url", "http://127.0.0.1:32123"]
        )
        self.assertEqual(runner.PROBE_TOKEN_ENV, parsed.probe_token_env)
        self.assertFalse(hasattr(parsed, "probe_token"))
        self.assertEqual(
            "secret",
            runner.probe_token_from_env(
                {runner.PROBE_TOKEN_ENV: "secret"}, runner.PROBE_TOKEN_ENV
            ),
        )
        with self.assertRaisesRegex(runner.RunnerError, "is missing"):
            runner.probe_token_from_env({}, runner.PROBE_TOKEN_ENV)

    def test_result_schema_gates_go_and_conditional_verdicts(self):
        schema = json.loads((SPIKE / "result.schema.json").read_text("utf-8"))
        verdict_gate = schema["allOf"][0]
        self.assertEqual(
            ["GO", "CONDITIONAL"],
            verdict_gate["if"]["properties"]["verdict"]["enum"],
        )
        gated = verdict_gate["then"]["properties"]
        self.assertEqual(True, gated["public_tool_channel"]["const"])
        self.assertEqual(90, gated["target_tenure_seconds"]["const"])
        self.assertEqual(3, gated["completed_cycles"]["minimum"])
        self.assertEqual(90, gated["observed_pending_seconds"]["minimum"])
        self.assertEqual(
            1,
            gated["lease_renewals"]["minimum"],
        )
        self.assertEqual(0, gated["idle_model_calls"]["const"])
        self.assertEqual(True, gated["idle_model_calls_verifiable"]["const"])
        self.assertEqual(False, gated["spawned_new_model_process"]["const"])
        self.assertIn("trace_artifacts", verdict_gate["then"]["required"])
        self.assertIn("visual_browser_verified", verdict_gate["then"]["required"])
        self.assertEqual(
            True,
            gated["browser_path_verified"]["const"],
        )
        self.assertEqual(
            True,
            gated["adaptive_followup_verified"]["const"],
        )
        self.assertEqual(7, gated["trace_artifacts"]["minItems"])
        required_scenarios = {
            branch["contains"]["properties"]["scenario"]["const"]
            for branch in gated["trace_artifacts"]["allOf"]
        }
        self.assertEqual(
            {
                "same_turn_three_rounds",
                "pause_or_switch",
                "user_cancellation",
                "tool_interruption",
                "reconnect",
                "idle_tenure",
                "browser_visual",
            },
            required_scenarios,
        )
        self.assertEqual(
            True,
            schema["properties"]["trace_artifacts"]["items"]["properties"]
            ["sanitized"]["const"],
        )
        self.assertEqual(
            True,
            schema["allOf"][1]["then"]["properties"]["reconnect"]["const"],
        )
        self.assertEqual(
            2,
            schema["allOf"][2]["then"]["properties"]
            ["progress_notifications"]["minimum"],
        )

    def test_runner_rejects_multiple_turns(self):
        events = [
            {"type": "thread.started", "thread_id": "thread-1"},
            {"type": "turn.started"},
            {"type": "turn.started"},
            {"type": "turn.completed"},
        ]
        with self.assertRaisesRegex(runner.RunnerError, "one turn.started"):
            runner.validate_trace(["codex"], events)


if __name__ == "__main__":
    unittest.main()
