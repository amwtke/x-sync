"""Translate host JSON streams into answer text, without exposing tool output."""

from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Callable


MAX_ANSWER_BYTES = 2 * 1024 * 1024
MAX_EVENT_BYTES = 4 * 1024 * 1024


class HostError(RuntimeError):
    """A public, actionable error; never contains raw host diagnostics."""


def host_command(host: str, repository: Path, binary: str | None = None) -> list[str]:
    if host not in {"codex", "claude"}:
        raise HostError("请选择 Codex 或 Claude Code。")
    executable = binary or shutil.which(host)
    if executable is None:
        label = "Codex" if host == "codex" else "Claude Code"
        raise HostError(f"未找到 {label}，请先安装并在终端登录，再启动聊天。")
    if host == "codex":
        return [
            executable,
            "exec",
            "--json",
            "--ephemeral",
            "--color",
            "never",
            "--sandbox",
            "read-only",
            "-c",
            'approval_policy="never"',
            "--cd",
            str(repository),
            "-",
        ]
    return [
        executable,
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--no-session-persistence",
        "--tools",
        "Read,Glob,Grep",
        "--allowedTools",
        "Read,Glob,Grep",
        "--permission-mode",
        "dontAsk",
        "--strict-mcp-config",
        "--mcp-config",
        '{"mcpServers":{}}',
    ]


class AnswerStream:
    """Only assistant answer text crosses this adapter; reasoning stays private."""

    def __init__(self, host: str) -> None:
        self.host = host
        self.text = ""
        self.completed = False
        self.failed = False
        self._items: dict[str, str] = {}
        self._claude_prefix = ""
        self._claude_current = ""

    def feed(self, event: object) -> str | None:
        if not isinstance(event, dict):
            return None
        before = self.text
        kind = event.get("type")
        if self.host == "codex":
            if kind in {"item.started", "item.updated", "item.completed"}:
                item = event.get("item", {})
                if isinstance(item, dict) and item.get("type") == "agent_message":
                    text, identifier = item.get("text"), item.get("id")
                    if isinstance(text, str) and isinstance(identifier, str):
                        self._items[identifier] = text
                        self.text = "\n\n".join(self._items.values())
            elif kind == "turn.completed":
                self.completed = True
            elif kind in {"turn.failed", "error"}:
                self.failed = True
        elif not event.get("parent_tool_use_id"):
            if kind == "stream_event":
                part = event.get("event", {})
                if isinstance(part, dict):
                    delta = part.get("delta", {})
                    if part.get("type") == "message_start":
                        self._claude_prefix = self.text
                        self._claude_current = ""
                    elif isinstance(delta, dict) and delta.get("type") == "text_delta":
                        chunk = delta.get("text")
                        if isinstance(chunk, str):
                            self._claude_current += chunk
                            self.text = "\n\n".join(
                                filter(
                                    None,
                                    (
                                        self._claude_prefix,
                                        self._claude_current,
                                    ),
                                )
                            )
            elif kind == "assistant":
                message = event.get("message", {})
                blocks = message.get("content", []) if isinstance(message, dict) else []
                if isinstance(blocks, list):
                    text = "\n\n".join(
                        block["text"]
                        for block in blocks
                        if isinstance(block, dict)
                        and block.get("type") == "text"
                        and isinstance(block.get("text"), str)
                    )
                    if text:
                        self.text = "\n\n".join(
                            filter(
                                None,
                                (
                                    self._claude_prefix,
                                    text,
                                ),
                            )
                        )
            elif kind == "result":
                self.failed = bool(event.get("is_error"))
                self.completed = not self.failed
                result = event.get("result")
                if self.completed and isinstance(result, str) and result.strip():
                    self.text = result
        if len(self.text.encode("utf-8")) > MAX_ANSWER_BYTES:
            raise HostError("回答过长，请缩小问题范围后重试。")
        return self.text if self.text != before else None


class HostRunner:
    """One bounded subprocess per question, driven by the persisted conversation."""

    def __init__(
        self,
        host: str,
        repository: Path,
        *,
        timeout: float = 600,
        command: list[str] | None = None,
    ) -> None:
        self.host = host
        self.repository = repository
        self.timeout = timeout
        self.command = command or host_command(host, repository)

    def run(
        self,
        prompt: str,
        update: Callable[[str], None],
        cancel: threading.Event,
    ) -> str:
        if cancel.is_set():
            raise HostError("已停止回答。")
        try:
            process = subprocess.Popen(
                self.command,
                cwd=self.repository,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError as exc:
            raise HostError("无法启动宿主，请检查安装和登录状态后重试。") from exc
        events: queue.Queue[bytes | Exception | None] = queue.Queue(maxsize=128)
        stopped = threading.Event()

        def put(value: bytes | Exception | None) -> None:
            while not stopped.is_set():
                try:
                    events.put(value, timeout=0.1)
                    return
                except queue.Full:
                    continue

        def read() -> None:
            assert process.stdout is not None
            try:
                while not stopped.is_set():
                    line = process.stdout.readline(MAX_EVENT_BYTES + 1)
                    if not line:
                        break
                    if len(line) > MAX_EVENT_BYTES:
                        put(HostError("宿主返回的数据过大，请缩小问题范围。"))
                        break
                    put(line)
            except OSError:
                put(HostError("宿主连接中断，请重试。"))
            finally:
                put(None)

        def write() -> None:
            assert process.stdin is not None
            try:
                process.stdin.write(prompt.encode("utf-8"))
                process.stdin.close()
            except OSError:
                put(HostError("宿主未能接收问题，请检查登录状态后重试。"))

        reader = threading.Thread(target=read, daemon=True)
        writer = threading.Thread(target=write, daemon=True)
        reader.start()
        writer.start()
        deadline = time.monotonic() + self.timeout
        stream = AnswerStream(self.host)
        try:
            while True:
                if cancel.is_set():
                    raise HostError("已停止回答。")
                if time.monotonic() >= deadline:
                    raise HostError("回答超时，请重试或缩小问题范围。")
                try:
                    item = events.get(timeout=0.1)
                except queue.Empty:
                    continue
                if item is None:
                    break
                if isinstance(item, Exception):
                    raise item
                try:
                    event = json.loads(item)
                except (ValueError, UnicodeError):
                    continue
                text = stream.feed(event)
                if text is not None:
                    update(text)
                if stream.failed:
                    raise HostError(
                        "宿主未能完成回答，请检查登录、网络或使用额度后重试。"
                    )
            remaining = max(0.1, deadline - time.monotonic())
            status = process.wait(timeout=min(remaining, 5))
            if status or not stream.completed or not stream.text.strip():
                raise HostError("宿主未返回完整回答，请检查登录状态后重试。")
            return stream.text
        except subprocess.TimeoutExpired as exc:
            raise HostError("宿主未能正常结束，请重试。") from exc
        finally:
            stopped.set()
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=2)
                except ProcessLookupError:
                    pass
            reader.join(timeout=1)
            writer.join(timeout=1)
            for pipe in (process.stdin, process.stdout):
                if pipe is not None and not pipe.closed:
                    pipe.close()
