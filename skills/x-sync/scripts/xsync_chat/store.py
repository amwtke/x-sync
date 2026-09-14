"""Private, atomically saved chat history with one runtime owner."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
from typing import Any, cast

from xsync import ensure_private_git_exclude
from xsync_v2.secure_fs import SecureDirectory, SecureFsError


MAX_STATE_BYTES = 16 * 1024 * 1024
MAX_QUESTION_BYTES = 32_000
REQUEST_ID = re.compile(r"[a-zA-Z0-9._-]{1,100}\Z")
SUGGESTIONS = (
    "这个项目主要解决什么问题？",
    "一个请求从进入系统到返回结果，经过了哪些模块？",
    "如果我要第一次修改这个项目，应该先读哪些文件？",
    "这个项目有哪些值得注意的设计取舍？",
)


class ChatError(RuntimeError):
    """An actionable public chat-state error."""


def now() -> str:
    return datetime.now(UTC).isoformat()


def repository_id(repository: Path) -> str:
    return hashlib.sha256(os.fsencode(repository)).hexdigest()[:16]


def _private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
            raise ChatError("聊天目录不是当前用户拥有的普通目录。") from None
        if stat.S_IMODE(info.st_mode) != 0o700:
            os.chmod(path, 0o700, follow_symlinks=False)


def state_directory(repository: Path, learner: str) -> Path:
    ensure_private_git_exclude(repository)
    private = repository / ".x-sync"
    _private_directory(private)
    with SecureDirectory.open(private) as root:
        with root.ensure_directory("chat") as chats:
            key = "learner-" + hashlib.sha256(learner.encode()).hexdigest()[:16]
            with chats.ensure_directory(key):
                pass
    return private / "chat" / key


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ChatError("聊天记录损坏，原文件已保留。")
        result[key] = value
    return result


class ChatStore:
    """The caller serializes mutations; the file lock fences other processes."""

    def __init__(self, directory: Path, repository: Path) -> None:
        self.repository = repository
        self.directory = directory
        self._root = SecureDirectory.open(directory)
        self._lock = -1
        try:
            self._lock = os.open(
                directory / "owner.lock",
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
            )
            info = os.fstat(self._lock)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise ChatError("聊天进程锁不可用。")
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ChatError("这个仓库的聊天已在运行，请使用现有页面。") from None
            try:
                raw = self._root.read_bytes("session.json", max_bytes=MAX_STATE_BYTES)
            except SecureFsError as exc:
                if exc.code != "FILE_NOT_FOUND":
                    raise
                self.state: dict[str, Any] = {
                    "schema_version": 1,
                    "session_id": "chat-" + secrets.token_hex(12),
                    "repository_id": repository_id(repository),
                    "created_at": now(),
                    "messages": [],
                }
                self._save()
            else:
                try:
                    self.state = json.loads(raw, object_pairs_hook=_pairs)
                    self._validate()
                except (ValueError, TypeError, KeyError, UnicodeError) as exc:
                    raise ChatError("聊天记录损坏，原文件已保留。") from exc
                changed = False
                for message in self.state["messages"]:
                    if message["status"] == "pending":
                        message["status"] = "error"
                        message["error"] = "上次回答被中断，可以点击重试。"
                        changed = True
                if changed:
                    self._save()
        except BaseException:
            self.close()
            raise

    def _validate(self) -> None:
        data = self.state
        if (
            not isinstance(data, dict)
            or data.get("schema_version") != 1
            or data.get("repository_id") != repository_id(self.repository)
            or not isinstance(data.get("session_id"), str)
            or not isinstance(data.get("created_at"), str)
            or not isinstance(data.get("messages"), list)
        ):
            raise ChatError("聊天记录与当前仓库不匹配，原文件已保留。")
        ids = set()
        requests = set()
        messages = data["messages"]
        if len(messages) % 2:
            raise ChatError("聊天记录不完整，原文件已保留。")
        for index, message in enumerate(messages):
            if not isinstance(message, dict) or not all(
                isinstance(message.get(key), str)
                for key in (
                    "id",
                    "role",
                    "text",
                    "status",
                    "request_id",
                    "created_at",
                    "host",
                    "error",
                )
            ):
                raise ChatError("聊天记录格式错误，原文件已保留。")
            if (
                message["id"] in ids
                or not REQUEST_ID.fullmatch(message["request_id"])
                or message["role"] != ("user" if index % 2 == 0 else "assistant")
                or message["status"]
                not in {"complete", "pending", "error", "cancelled"}
            ):
                raise ChatError("聊天记录顺序错误，原文件已保留。")
            ids.add(message["id"])
            if index % 2 == 0:
                if message["request_id"] in requests or message["status"] != "complete":
                    raise ChatError("聊天问题记录重复，原文件已保留。")
                requests.add(message["request_id"])
            elif message["request_id"] != messages[index - 1]["request_id"]:
                raise ChatError("聊天回复与问题不匹配，原文件已保留。")

    def _save(self) -> None:
        raw = json.dumps(self.state, ensure_ascii=False, separators=(",", ":")).encode()
        if len(raw) > MAX_STATE_BYTES:
            raise ChatError("聊天记录已满，请先导出。")
        self._root.replace_derived("session.json", raw)

    def snapshot(self) -> list[dict[str, Any]]:
        return deepcopy(self.state["messages"])

    def add(self, text: object, request_id: object, host: str) -> str:
        if (
            not isinstance(text, str)
            or not text.strip()
            or len(text.encode("utf-8")) > MAX_QUESTION_BYTES
            or any(ord(char) < 32 and char not in "\n\t\r" for char in text)
        ):
            raise ChatError("请输入问题，长度不超过 32 KB。")
        if not isinstance(request_id, str) or not REQUEST_ID.fullmatch(request_id):
            raise ChatError("请求标识无效，请刷新后重试。")
        messages = self.state["messages"]
        for index in range(0, len(messages), 2):
            if messages[index]["request_id"] == request_id:
                if messages[index]["text"] != text.strip():
                    raise ChatError("同一请求不能对应不同的问题。")
                return cast(str, messages[index + 1]["id"])
        previous = deepcopy(self.state)
        identifier = "message-" + secrets.token_hex(12)
        for role, content, status in (
            ("user", text.strip(), "complete"),
            ("assistant", "", "pending"),
        ):
            messages.append(
                {
                    "id": identifier + ("-user" if role == "user" else ""),
                    "role": role,
                    "text": content,
                    "status": status,
                    "request_id": request_id,
                    "created_at": now(),
                    "host": host,
                    "error": "",
                }
            )
        try:
            self._save()
        except BaseException:
            self.state = previous
            raise
        return identifier

    def latest(self, identifier: str) -> dict[str, Any]:
        messages = self.state["messages"]
        if not messages or messages[-1]["id"] != identifier:
            raise ChatError("这条回复已不是当前回复，请刷新页面。")
        return cast(dict[str, Any], messages[-1])

    def finish(self, identifier: str, text: str, status: str, error: str = "") -> None:
        message = self.latest(identifier)
        message.update(text=text, status=status, error=error)
        self._save()

    def retry(self, identifier: str) -> None:
        message = self.latest(identifier)
        if message["status"] not in {"error", "cancelled"}:
            raise ChatError("只有中断或失败的回复可以重试。")
        message.update(text="", status="pending", error="")
        self._save()

    def export(self, messages: list[dict[str, Any]]) -> tuple[str, bytes]:
        title = self.repository.name.replace("\n", " ").replace("\r", " ")
        lines = [f"# {title} · X-Sync 对话", "", f"导出时间：{now()}", ""]
        for message in messages:
            label = (
                "用户"
                if message["role"] == "user"
                else ("Codex" if message["host"] == "codex" else "Claude Code")
            )
            lines.extend([f"## {label}", "", message["text"], ""])
            if message["status"] != "complete":
                note = message["error"] or "这条回复尚未完成。"
                lines.extend([f"> {note}", ""])
        body = ("\n".join(lines).rstrip() + "\n").encode("utf-8")
        name = "x-sync-" + datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        name += "-" + secrets.token_hex(3) + ".md"
        with self._root.ensure_directory("exports") as exports:
            exports.write_immutable(name, body)
        return name, body

    def close(self) -> None:
        if self._lock >= 0:
            os.close(self._lock)
            self._lock = -1
        self._root.close()
