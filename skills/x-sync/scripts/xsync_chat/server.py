"""A local chat page, durable conversation, and automatic host worker."""

from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import json
from pathlib import Path
import threading
from typing import Any
from urllib.parse import urlsplit

from .context import build_prompt
from .host import HostError, HostRunner
from .store import ChatError, ChatStore, SUGGESTIONS


class ChatRuntime:
    def __init__(self, store: ChatStore, runner: HostRunner) -> None:
        self.store = store
        self.runner = runner
        self.condition = threading.Condition(threading.RLock())
        self.revision = 0
        self.active: str | None = None
        self.partial = ""
        self.cancel = threading.Event()
        self.worker: threading.Thread | None = None
        self.closed = False

    def _changed(self) -> None:
        self.revision += 1
        self.condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        with self.condition:
            messages = self.store.snapshot()
            if self.active and messages and messages[-1]["id"] == self.active:
                messages[-1]["text"] = self.partial
            return {
                "session_id": self.store.state["session_id"],
                "repository": self.store.repository.name,
                "host": self.runner.host,
                "messages": messages,
                "suggestions": list(SUGGESTIONS),
                "busy": self.active is not None,
                "closed": self.closed,
                "revision": self.revision,
            }

    def submit(self, text: object, request_id: object) -> dict[str, Any]:
        with self.condition:
            if self.closed:
                raise ChatError("聊天已关闭。")
            matching = [
                message
                for message in self.store.snapshot()
                if message["role"] == "user" and message["request_id"] == request_id
            ]
            if self.active and not matching:
                raise ChatError("正在回答上一个问题，可以先停止回答。")
            identifier = self.store.add(text, request_id, self.runner.host)
            message = next(
                item for item in self.store.snapshot() if item["id"] == identifier
            )
            if message["status"] == "pending" and self.active is None:
                self._start(identifier)
            return self.snapshot()

    def retry(self, identifier: object) -> dict[str, Any]:
        with self.condition:
            if self.closed or self.active:
                raise ChatError("请等待当前回复结束后重试。")
            if not isinstance(identifier, str):
                raise ChatError("回复标识无效。")
            self.store.retry(identifier)
            self._start(identifier)
            return self.snapshot()

    def _start(self, identifier: str) -> None:
        self.active = identifier
        self.partial = ""
        self.cancel = threading.Event()
        self.worker = threading.Thread(
            target=self._answer, args=(identifier,), daemon=True
        )
        self._changed()
        self.worker.start()

    def _answer(self, identifier: str) -> None:
        status, error, text = "complete", "", ""

        def update(value: str) -> None:
            with self.condition:
                if self.active == identifier:
                    self.partial = value
                    self._changed()

        try:
            with self.condition:
                messages = self.store.snapshot()
            prompt = build_prompt(self.store.repository, messages)
            text = self.runner.run(prompt, update, self.cancel)
        except HostError as exc:
            status = "cancelled" if self.cancel.is_set() else "error"
            error = str(exc)
        except Exception:
            status, error = "error", "回答中断，问题已保存，可以点击重试。"
        finally:
            with self.condition:
                if status != "complete":
                    text = self.partial
                try:
                    self.store.finish(identifier, text, status, error)
                except Exception:
                    message = self.store.latest(identifier)
                    message.update(
                        text=text,
                        status="error",
                        error="这次回复未能保存，请先复制或导出，再重试。",
                    )
                finally:
                    self.active = None
                    self.partial = ""
                    self._changed()

    def stop(self) -> dict[str, Any]:
        with self.condition:
            self.cancel.set()
            return self.snapshot()

    def export(self) -> tuple[str, bytes]:
        with self.condition:
            return self.store.export(self.snapshot()["messages"])

    def close(self) -> None:
        with self.condition:
            self.closed = True
            self.cancel.set()
            self._changed()
        if self.worker is not None:
            self.worker.join(timeout=15)
        self.store.close()


class ChatServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, runtime: ChatRuntime, token: str, port: int = 0) -> None:
        self.runtime = runtime
        self.token = token
        super().__init__(("127.0.0.1", port), ChatHandler)
        self.origin = f"http://127.0.0.1:{self.server_port}"
        self.launch_url = f"{self.origin}/#{token}"


class ChatHandler(BaseHTTPRequestHandler):
    server: ChatServer
    protocol_version = "HTTP/1.1"

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(30)

    def log_message(self, _format: str, *args: object) -> None:
        pass

    def _headers(
        self, status: int, content_type: str, length: int | None = None
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header(
            "Content-Security-Policy",
            (
                "default-src 'none'; script-src 'self'; style-src 'self'; "
                "connect-src 'self'; img-src 'self'; frame-ancestors 'none'; "
                "base-uri 'none'; form-action 'none'"
            ),
        )
        if length is not None:
            self.send_header("Content-Length", str(length))

    def _json(self, status: int, value: object) -> None:
        raw = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self._headers(status, "application/json; charset=utf-8", len(raw))
        self.end_headers()
        self.wfile.write(raw)

    def _authorized(self) -> bool:
        expected_host = self.server.origin.removeprefix("http://")
        if self.headers.get_all("Host", []) != [expected_host]:
            self._json(403, {"error": "页面来源不匹配。"})
            return False
        origin = self.headers.get("Origin")
        if origin is not None and origin != self.server.origin:
            self._json(403, {"error": "不允许跨站访问。"})
            return False
        if self.command == "POST" and origin != self.server.origin:
            self._json(403, {"error": "不允许跨站提交。"})
            return False
        if self.headers.get("Sec-Fetch-Site") == "cross-site":
            self._json(403, {"error": "不允许跨站访问。"})
            return False
        supplied = self.headers.get_all("Authorization", [])
        if len(supplied) != 1 or not hmac.compare_digest(
            supplied[0].encode(),
            ("Bearer " + self.server.token).encode(),
        ):
            self._json(
                401, {"error": "页面连接已失效，请使用启动时的完整链接重新打开。"}
            )
            return False
        return True

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path in {"/", "/chat.js", "/chat.css"}:
            if self.headers.get("Host") != self.server.origin.removeprefix("http://"):
                self._json(403, {"error": "页面来源不匹配。"})
                return
            file = "chat.html" if path == "/" else path[1:]
            types = {
                "chat.html": "text/html",
                "chat.js": "text/javascript",
                "chat.css": "text/css",
            }
            data = (Path(__file__).resolve().parents[2] / "assets" / file).read_bytes()
            self._headers(200, types[file] + "; charset=utf-8", len(data))
            self.end_headers()
            self.wfile.write(data)
            return
        if not self._authorized():
            return
        if path == "/api/chat/state":
            self._json(200, self.server.runtime.snapshot())
        elif path == "/api/chat/events":
            self._stream()
        else:
            self._json(404, {"error": "页面不存在。"})

    def _stream(self) -> None:
        self._headers(200, "text/event-stream; charset=utf-8")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        revision = -1
        runtime = self.server.runtime

        def changed() -> bool:
            return runtime.revision != revision or runtime.closed

        try:
            while True:
                with runtime.condition:
                    runtime.condition.wait_for(changed, timeout=15)
                    if runtime.closed:
                        break
                    snapshot = (
                        runtime.snapshot() if revision != runtime.revision else None
                    )
                    revision = runtime.revision
                if snapshot is None:
                    payload = b": keepalive\n\n"
                else:
                    raw = json.dumps(
                        snapshot, ensure_ascii=False, separators=(",", ":")
                    )
                    payload = ("event: state\ndata: " + raw + "\n\n").encode()
                self.wfile.write(payload)
                self.wfile.flush()
        except (OSError, ValueError):
            pass

    def do_POST(self) -> None:
        if not self._authorized():
            self.close_connection = True
            return
        if self.headers.get("Transfer-Encoding") is not None:
            self.close_connection = True
            self._json(400, {"error": "请求格式错误。"})
            return
        try:
            lengths = self.headers.get_all("Content-Length", [])
            if len(lengths) != 1:
                raise ValueError
            length = int(lengths[0])
            if not 0 <= length <= 256 * 1024:
                self.close_connection = True
                self._json(413, {"error": "问题过长，请缩短后重试。"})
                return
            if self.headers.get_content_type() != "application/json":
                raise ValueError
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError
        except (ValueError, OSError):
            self.close_connection = True
            self._json(400, {"error": "请求格式错误。"})
            return
        runtime = self.server.runtime
        try:
            path = urlsplit(self.path).path
            if path == "/api/chat/messages" and set(value) == {"text", "request_id"}:
                result = runtime.submit(value["text"], value["request_id"])
            elif path == "/api/chat/retry" and set(value) == {"message_id"}:
                result = runtime.retry(value["message_id"])
            elif path == "/api/chat/stop" and not value:
                result = runtime.stop()
            elif path == "/api/chat/export" and not value:
                filename, data = runtime.export()
                self._headers(200, "text/markdown; charset=utf-8", len(data))
                self.send_header(
                    "Content-Disposition", f'attachment; filename="{filename}"'
                )
                self.end_headers()
                self.wfile.write(data)
                return
            else:
                self._json(400, {"error": "不支持这个操作。"})
                return
            self._json(HTTPStatus.OK, result)
        except ChatError as exc:
            self._json(409, {"error": str(exc)})
        except Exception:
            self._json(500, {"error": "暂时无法保存，请重试。"})
