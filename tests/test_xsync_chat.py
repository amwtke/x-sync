from __future__ import annotations

import http.client
import json
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

from xsync_chat.cli import resolve_repository
from xsync_chat.context import build_prompt, repository_context
from xsync_chat.host import AnswerStream, HostError, HostRunner, host_command
from xsync_chat.server import ChatRuntime, ChatServer
from xsync_chat.store import ChatError, ChatStore, state_directory
from xsync_v2.secure_fs import SecureFsError


class RepositoryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name).resolve() / "repo with spaces"
        self.repo.mkdir()
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        (self.repo / "README.md").write_text(
            "# Example\nA repository for chat tests.\n"
        )
        self.directory = state_directory(self.repo, "learner")

    def open_store(self):
        store = ChatStore(self.directory, self.repo)
        self.addCleanup(store.close)
        return store


class StoreTest(RepositoryTest):
    def test_free_question_is_durable_and_replay_does_not_duplicate(self):
        store = self.open_store()
        identifier = store.add("请解释项目结构", "request-1", "codex")
        self.assertEqual(identifier, store.add("请解释项目结构", "request-1", "codex"))
        self.assertEqual(2, len(store.snapshot()))
        with self.assertRaises(ChatError):
            store.add("不同问题", "request-1", "codex")
        store.finish(identifier, "项目包括 `README.md`。", "complete")
        original = store.snapshot()
        store.close()
        recovered = self.open_store()
        self.assertEqual(original, recovered.snapshot())
        self.assertEqual(
            0o600, stat.S_IMODE((self.directory / "session.json").stat().st_mode)
        )

    def test_interrupted_response_can_retry_without_repeating_question(self):
        store = self.open_store()
        identifier = store.add("为什么？", "request-1", "codex")
        store.close()
        recovered = self.open_store()
        self.assertEqual("error", recovered.latest(identifier)["status"])
        recovered.retry(identifier)
        recovered.finish(identifier, "因为……", "complete")
        self.assertEqual(
            ["为什么？", "因为……"], [item["text"] for item in recovered.snapshot()]
        )

    def test_exclusive_owner_and_malformed_state_fail_without_overwrite(self):
        store = self.open_store()
        with self.assertRaises(ChatError):
            ChatStore(self.directory, self.repo)
        store.close()
        file = self.directory / "session.json"
        file.write_bytes(b'{"messages":false}')
        with self.assertRaises(ChatError):
            ChatStore(self.directory, self.repo)
        self.assertEqual(b'{"messages":false}', file.read_bytes())

    def test_export_contains_both_sides_and_partial_status(self):
        store = self.open_store()
        identifier = store.add("解释 **入口**", "export-request", "claude")
        store.finish(identifier, "从 `main.py` 开始。", "cancelled", "已停止回答。")
        filename, body = store.export(store.snapshot())
        self.assertTrue(filename.endswith(".md"))
        self.assertIn("## 用户\n\n解释 **入口**", body.decode())
        self.assertIn("## Claude Code\n\n从 `main.py` 开始。", body.decode())
        self.assertIn("> 已停止回答。", body.decode())
        self.assertEqual(body, (self.directory / "exports" / filename).read_bytes())

    def test_unsafe_state_path_and_invalid_questions_are_rejected(self):
        store = self.open_store()
        for text in ("", "  ", "a\0b", "问" * 11000):
            with self.subTest(text=text[:10]), self.assertRaises(ChatError):
                store.add(text, "request-1", "codex")
        store.close()
        (self.directory / "session.json").unlink()
        target = self.repo / "private.json"
        target.write_text("keep me")
        (self.directory / "session.json").symlink_to(target)
        with self.assertRaises(SecureFsError):
            ChatStore(self.directory, self.repo)
        self.assertEqual("keep me", target.read_text())


class ContextTest(RepositoryTest):
    def test_context_uses_current_repository_and_preserves_followups(self):
        (self.repo / ".env.local").write_text("PRIVATE_VALUE=do-not-include")
        (self.repo / ".gitignore").write_text("ignored.txt\n")
        (self.repo / "ignored.txt").write_text("ignored")
        (self.repo / "link.md").symlink_to(self.repo / "README.md")
        context = repository_context(self.repo)
        self.assertIn("README.md", context["files"])
        for excluded in (".env.local", "ignored.txt", "link.md"):
            self.assertNotIn(excluded, context["files"])
        messages = [
            {"role": "user", "text": "先解释入口", "status": "complete"},
            {"role": "assistant", "text": "入口是 main.py。", "status": "complete"},
            {"role": "user", "text": "它后面会调用谁？", "status": "complete"},
            {"role": "assistant", "text": "", "status": "pending"},
        ]
        prompt = build_prompt(self.repo, messages)
        payload = json.loads("{" + prompt.split("\n{", 1)[1])
        self.assertEqual(
            [item["text"] for item in messages[:3]],
            [item["content"] for item in payload["conversation"]],
        )
        (self.repo / "README.md").write_text("# Updated behavior")
        self.assertIn("Updated behavior", build_prompt(self.repo, messages))
        self.assertNotIn("do-not-include", prompt)

    def test_explicit_target_is_canonicalized_without_fallback(self):
        child = self.repo / "nested"
        child.mkdir()
        self.assertEqual(self.repo, resolve_repository(str(child)))
        with self.assertRaises(ChatError):
            resolve_repository(str(self.repo / "missing"))


class HostStreamTest(unittest.TestCase):
    def test_codex_updates_same_message_without_duplicate_or_private_output(self):
        stream = AnswerStream("codex")
        self.assertIsNone(
            stream.feed(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "private",
                        "type": "reasoning",
                        "text": "hidden reasoning",
                    },
                }
            )
        )
        for kind, text in (
            ("item.updated", "第一段"),
            ("item.completed", "第一段，完整回答"),
        ):
            self.assertEqual(
                text,
                stream.feed(
                    {
                        "type": kind,
                        "item": {
                            "id": "answer",
                            "type": "agent_message",
                            "text": text,
                        },
                    }
                ),
            )
        stream.feed({"type": "turn.completed"})
        self.assertTrue(stream.completed)
        self.assertEqual("第一段，完整回答", stream.text)

    def test_claude_partial_complete_and_result_do_not_duplicate(self):
        stream = AnswerStream("claude")
        stream.feed({"type": "stream_event", "event": {"type": "message_start"}})
        stream.feed(
            {
                "type": "stream_event",
                "event": {"delta": {"type": "thinking_delta", "thinking": "private"}},
            }
        )
        for text in ("你好", "，这是回答。"):
            stream.feed(
                {
                    "type": "stream_event",
                    "event": {
                        "type": "content_block_delta",
                        "delta": {"type": "text_delta", "text": text},
                    },
                }
            )
        stream.feed(
            {
                "type": "assistant",
                "message": {"content": [{"type": "text", "text": "你好，这是回答。"}]},
            }
        )
        stream.feed({"type": "result", "is_error": False, "result": "你好，这是回答。"})
        self.assertEqual("你好，这是回答。", stream.text)
        self.assertTrue(stream.completed)
        stream.feed(
            {
                "type": "assistant",
                "parent_tool_use_id": "child",
                "message": {
                    "content": [{"type": "text", "text": "private child output"}]
                },
            }
        )
        self.assertEqual("你好，这是回答。", stream.text)

    def test_subprocess_receives_stdin_and_only_returns_answer_events(self):
        code = """import json, sys
prompt = sys.stdin.read()
print(json.dumps({'type':'item.completed','item':{
    'id':'tool', 'type':'command_execution',
    'aggregated_output':'private diagnostic'}}), flush=True)
print(json.dumps({'type':'item.completed','item':{
    'id':'answer','type':'agent_message','text':'收到：' + prompt}}), flush=True)
print(json.dumps({'type':'turn.completed'}), flush=True)
"""
        runner = HostRunner("codex", Path.cwd(), command=[sys.executable, "-c", code])
        updates = []
        result = runner.run("普通问题", updates.append, threading.Event())
        self.assertEqual("收到：普通问题", result)
        self.assertEqual([result], updates)

    def test_cancel_and_process_failure_produce_actionable_private_errors(self):
        code = """import json, sys, time
sys.stdin.read()
print(json.dumps({'type':'item.updated','item':{
    'id':'a','type':'agent_message','text':'partial'}}), flush=True)
time.sleep(30)
"""
        runner = HostRunner("codex", Path.cwd(), command=[sys.executable, "-c", code])
        cancel = threading.Event()
        with self.assertRaisesRegex(HostError, "停止"):
            runner.run("question", lambda _: cancel.set(), cancel)
        failure = HostRunner(
            "codex",
            Path.cwd(),
            command=[
                sys.executable,
                "-c",
                "import sys; sys.stderr.write('SECRET'); sys.exit(1)",
            ],
        )
        with self.assertRaises(HostError) as error:
            failure.run("question", lambda _: None, threading.Event())
        self.assertNotIn("SECRET", str(error.exception))

    def test_real_host_arguments_are_read_only_and_no_shell_interpolation(self):
        repository = Path("/tmp/repo with spaces")
        codex = host_command("codex", repository, "/example/codex")
        self.assertEqual("read-only", codex[codex.index("--sandbox") + 1])
        self.assertEqual(str(repository), codex[codex.index("--cd") + 1])
        self.assertEqual("-", codex[-1])
        claude = host_command("claude", repository, "/example/claude")
        self.assertIn("--include-partial-messages", claude)
        self.assertEqual("Read,Glob,Grep", claude[claude.index("--tools") + 1])
        with (
            mock.patch("shutil.which", return_value=None),
            self.assertRaises(HostError),
        ):
            host_command("claude", repository)


class ControlledRunner:
    host = "codex"

    def __init__(self):
        self.calls = []
        self.started = threading.Event()
        self.release = threading.Event()
        self.fail = False

    def run(self, prompt, update, cancel):
        self.calls.append(prompt)
        update("这是直接回答。")
        self.started.set()
        if not self.release.wait(timeout=5):
            raise HostError("test timed out")
        if cancel.is_set():
            raise HostError("已停止回答。")
        if self.fail:
            raise HostError("连接中断。")
        return "这是直接回答。\n\n依据：`README.md:1`。"


class ChatHttpTest(RepositoryTest):
    def setUp(self):
        super().setUp()
        self.runner = ControlledRunner()
        self.runtime = ChatRuntime(self.open_store(), self.runner)
        self.server = ChatServer(self.runtime, "test-capability")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.shutdown)

    def shutdown(self):
        self.runner.release.set()
        self.server.shutdown()
        self.runtime.close()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, body=None, *, headers=None):
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=5
        )
        self.addCleanup(connection.close)
        defaults = {
            "Authorization": "Bearer test-capability",
            "Origin": self.server.origin,
        }
        if body is not None:
            defaults["Content-Type"] = "application/json"
            body = json.dumps(body).encode()
        defaults.update(headers or {})
        connection.request(method, path, body=body, headers=defaults)
        response = connection.getresponse()
        return response, response.read()

    def wait_complete(self):
        with self.runtime.condition:
            self.assertTrue(
                self.runtime.condition.wait_for(
                    lambda: self.runtime.active is None, timeout=5
                )
            )

    def test_empty_chat_accepts_a_question_without_topic_or_question_id(self):
        _, raw = self.request("GET", "/api/chat/state")
        initial = json.loads(raw)
        self.assertEqual([], initial["messages"])
        self.assertTrue(initial["suggestions"])
        response, _ = self.request(
            "POST", "/api/chat/messages", {"text": "解释项目", "request_id": "first"}
        )
        self.assertEqual(200, response.status)
        self.assertTrue(self.runner.started.wait(timeout=3))
        self.runner.release.set()
        self.wait_complete()
        _, raw = self.request("GET", "/api/chat/state")
        data = json.loads(raw)
        self.assertFalse(data["busy"])
        self.assertEqual(
            ["user", "assistant"], [item["role"] for item in data["messages"]]
        )
        self.assertIn("直接回答", data["messages"][1]["text"])
        self.request(
            "POST",
            "/api/chat/messages",
            {"text": "那下一步呢？", "request_id": "second"},
        )
        self.wait_complete()
        self.assertIn("解释项目", self.runner.calls[-1])
        self.assertIn("那下一步呢？", self.runner.calls[-1])
        self.assertEqual(4, len(self.runtime.snapshot()["messages"]))

    def test_sse_delivers_partial_response_before_worker_finishes(self):
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=5
        )
        self.addCleanup(connection.close)
        connection.request(
            "GET",
            "/api/chat/events",
            headers={"Authorization": "Bearer test-capability"},
        )
        response = connection.getresponse()

        def event():
            data = None
            while True:
                line = response.readline()
                if line.startswith(b"data: "):
                    data = json.loads(line[6:])
                if line == b"\n" and data is not None:
                    return data

        self.assertEqual([], event()["messages"])
        self.request(
            "POST", "/api/chat/messages", {"text": "stream me", "request_id": "stream"}
        )
        self.assertTrue(self.runner.started.wait(timeout=3))
        observed = event()
        if not observed["messages"][-1]["text"]:
            observed = event()
        self.assertTrue(observed["busy"])
        self.assertEqual("这是直接回答。", observed["messages"][-1]["text"])
        self.runner.release.set()
        self.wait_complete()
        self.assertFalse(event()["busy"])

    def test_retry_idempotency_and_export_do_not_call_host_extra_times(self):
        self.runner.fail = True
        self.runner.release.set()
        body = {"text": "为什么", "request_id": "retry-me"}
        self.request("POST", "/api/chat/messages", body)
        self.wait_complete()
        self.request("POST", "/api/chat/messages", body)
        self.assertEqual(1, len(self.runner.calls))
        latest = self.runtime.snapshot()["messages"][-1]
        self.assertEqual("error", latest["status"])
        self.runner.fail = False
        self.request("POST", "/api/chat/retry", {"message_id": latest["id"]})
        self.wait_complete()
        self.assertEqual(2, len(self.runner.calls))
        response, body = self.request("POST", "/api/chat/export", {})
        self.assertEqual(200, response.status)
        self.assertIn("text/markdown", response.getheader("Content-Type"))
        self.assertIn(".md", response.getheader("Content-Disposition"))
        self.assertIn("为什么", body.decode())
        self.assertIn("直接回答", body.decode())
        self.assertEqual(2, len(self.runner.calls))

    def test_cross_origin_auth_and_oversize_inputs_cannot_start_work(self):
        for headers, expected in (
            ({"Authorization": "Bearer wrong"}, 401),
            ({"Origin": "https://outside.example"}, 403),
        ):
            response, _ = self.request(
                "POST",
                "/api/chat/messages",
                {"text": "hello", "request_id": "blocked"},
                headers=headers,
            )
            self.assertEqual(expected, response.status)
        response, _ = self.request(
            "POST", "/api/chat/messages", {"text": "x" * 40000, "request_id": "long"}
        )
        self.assertEqual(409, response.status)
        self.assertEqual([], self.runner.calls)
        response, html = self.request("GET", "/")
        self.assertIn(
            "frame-ancestors 'none'", response.getheader("Content-Security-Policy")
        )
        self.assertNotIn(b"test-capability", html)


if __name__ == "__main__":
    unittest.main()
