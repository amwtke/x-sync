#!/usr/bin/env python3
"""Minimal stdio MCP server for the Phase 0 same-turn host spike.

The server deliberately owns no dialogue semantics. It publishes one durable
question through the authenticated loopback probe daemon and waits for the
browser to persist the matching answer. JSON-RPC input remains responsive while
a tool call is pending, so an MCP client can cancel the outstanding request.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
import sys
import threading
import time
from typing import Any, Callable, Mapping, Protocol, TextIO
import urllib.error
import urllib.parse
import urllib.request


SERVER_NAME = "x-sync-phase0"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL_VERSION = "2025-06-18"
TOOL_NAME = "publish_and_wait"
ROUND_ALREADY_PENDING = -32002
ANOTHER_ROUND_PENDING = -32003


class JournalPort(Protocol):
    """Narrow durable-state contract shared with the Phase 0 browser probe."""

    def publish_question(
        self,
        round_id: str,
        question: str,
        request_id: str,
    ) -> dict[str, Any]: ...

    def answer_for(self, round_id: str) -> dict[str, Any] | None: ...

    def snapshot(self) -> dict[str, Any]: ...


class Clock(Protocol):
    def monotonic(self) -> float: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()


class HttpJournal:
    """Call the single probe daemon instead of opening its state in this process."""

    def __init__(self, base_url: str, token: str, *, timeout: float = 2.0) -> None:
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme != "http" or parsed.hostname not in {
            "127.0.0.1",
            "localhost",
            "::1",
        }:
            raise ValueError("probe URL must be loopback HTTP")
        if not token:
            raise ValueError("probe token must not be empty")
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout

    def _request(
        self,
        path: str,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        headers = {"Authorization": f"Bearer {self._token}"}
        method = "GET"
        if data is not None:
            headers["Content-Type"] = "application/json"
            method = "POST"
        request = urllib.request.Request(
            self._base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                error = json.load(exc)
            except (json.JSONDecodeError, UnicodeDecodeError):
                error = {"error": "HTTP_ERROR"}
            raise StateContractError(str(error.get("error", "HTTP_ERROR"))) from exc
        except urllib.error.URLError as exc:
            raise StateContractError(f"probe unavailable: {exc.reason}") from exc
        if not isinstance(payload, dict):
            raise StateContractError("probe returned a non-object response")
        return payload

    def publish_question(
        self,
        round_id: str,
        question: str,
        request_id: str,
    ) -> dict[str, Any]:
        return self._request(
            "/question/publish",
            {
                "round_id": round_id,
                "question": question,
                "request_id": request_id,
            },
        )

    def answer_for(self, round_id: str) -> dict[str, Any] | None:
        value = self._request(
            "/question/answer-for",
            {"round_id": round_id},
        ).get("answer")
        if value is not None and not isinstance(value, dict):
            raise StateContractError("probe returned an invalid answer")
        return value

    def snapshot(self) -> dict[str, Any]:
        return self._request("/state")


class ToolCancelled(RuntimeError):
    """The MCP client cancelled a wait, not its already-durable question."""


class WaitingStopped(RuntimeError):
    """The browser paused or switched away from the pending question."""

    def __init__(self, control: str):
        super().__init__(f"browser changed control state to {control}")
        self.control = control


class StateContractError(RuntimeError):
    """The durable probe state does not satisfy the spike contract."""


ProgressCallback = Callable[[int, str], None]
WaitCallback = Callable[[threading.Event, float], bool]


def _system_wait(cancelled: threading.Event, timeout: float) -> bool:
    return cancelled.wait(timeout)


class PublishAndWaitService:
    """Publish one question and block until its matching durable answer exists."""

    def __init__(
        self,
        journal: JournalPort,
        *,
        clock: Clock | None = None,
        wait: WaitCallback = _system_wait,
        poll_interval: float = 0.25,
        progress_interval: float = 10.0,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if progress_interval <= 0:
            raise ValueError("progress_interval must be positive")
        self._journal = journal
        self._clock = clock or SystemClock()
        self._wait = wait
        self._poll_interval = poll_interval
        self._progress_interval = progress_interval

    def publish_and_wait(
        self,
        round_number: int,
        question: str,
        *,
        cancelled: threading.Event,
        progress: ProgressCallback,
    ) -> dict[str, Any]:
        if isinstance(round_number, bool) or not isinstance(round_number, int):
            raise ValueError("round must be an integer")
        if round_number < 1:
            raise ValueError("round must be at least 1")
        if not isinstance(question, str) or not question.strip():
            raise ValueError("question must be a non-empty string")

        round_id = str(round_number)
        request_id = f"mcp-question-{round_id}"
        self._journal.publish_question(round_id, question.strip(), request_id)

        started_at = self._clock.monotonic()
        next_progress_at = started_at
        while True:
            if cancelled.is_set():
                raise ToolCancelled("MCP client cancelled the pending request")

            durable_answer = self._journal.answer_for(round_id)
            if durable_answer is not None:
                answer = durable_answer.get("text")
                if not isinstance(answer, str) or not answer.strip():
                    raise StateContractError("durable answer is missing text")
                return {
                    "round": round_number,
                    "answer": answer,
                    "answer_request_id": durable_answer.get("request_id"),
                    "answered_at": durable_answer.get("answered_at"),
                }
            snapshot = self._journal.snapshot()
            control = snapshot.get("control_state")
            if control in {"pause", "paused", "switch", "switched"}:
                raise WaitingStopped(control)

            now = self._clock.monotonic()
            if now >= next_progress_at:
                elapsed = max(0, int(now - started_at))
                progress(
                    elapsed,
                    f"round {round_number} is waiting for a browser answer",
                )
                next_progress_at = now + self._progress_interval
            if self._wait(cancelled, self._poll_interval):
                raise ToolCancelled("MCP client cancelled the pending request")


@dataclass
class PendingRequest:
    round_number: int
    question: str
    cancelled: threading.Event = field(default_factory=threading.Event)
    reason: str = "MCP client cancelled the pending request"
    thread: threading.Thread | None = None


class MCPServer:
    """JSON-lines MCP server with one active browser wait at a time.

    Transport cancellation releases the in-memory wait but deliberately leaves
    the published question durable. A later tool call may recover it only by
    retrying the exact same round and question; the probe rejects stale or new
    rounds while that durable question is unanswered.
    """

    def __init__(self, service: PublishAndWaitService, output: TextIO) -> None:
        self._service = service
        self._output = output
        self._write_lock = threading.Lock()
        self._active_lock = threading.Lock()
        self._active: dict[str | int, PendingRequest] = {}
        self._workers: set[threading.Thread] = set()

    def serve(self, input_stream: TextIO) -> None:
        try:
            for raw_line in input_stream:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as exc:
                    self._write_error(None, -32700, "Parse error", str(exc))
                    continue
                self.handle_message(message)
        finally:
            self.close()

    def close(self) -> None:
        with self._active_lock:
            pending = list(self._active.values())
            workers = list(self._workers)
        for request in pending:
            request.reason = "MCP input stream closed"
            request.cancelled.set()
        for worker in workers:
            worker.join(timeout=1.0)

    def handle_message(self, message: Any) -> None:
        if not isinstance(message, Mapping) or message.get("jsonrpc") != "2.0":
            request_id = message.get("id") if isinstance(message, Mapping) else None
            self._write_error(request_id, -32600, "Invalid Request")
            return

        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params", {})
        if method in {"notifications/cancelled", "$/cancelRequest"}:
            self._cancel(params)
            return
        if request_id is None:
            # MCP lifecycle notifications need no acknowledgement.
            return
        if method == "initialize":
            self._initialize(request_id, params)
        elif method == "ping":
            self._write_result(request_id, {})
        elif method == "tools/list":
            self._write_result(request_id, {"tools": [tool_definition()]})
        elif method == "tools/call":
            self._start_tool_call(request_id, params)
        else:
            self._write_error(request_id, -32601, "Method not found", method)

    def _initialize(self, request_id: str | int, params: Any) -> None:
        protocol_version = DEFAULT_PROTOCOL_VERSION
        if isinstance(params, Mapping):
            requested = params.get("protocolVersion")
            if isinstance(requested, str) and requested:
                protocol_version = requested
        self._write_result(
            request_id,
            {
                "protocolVersion": protocol_version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )

    def _start_tool_call(self, request_id: str | int, params: Any) -> None:
        if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
            self._write_error(request_id, -32600, "Invalid Request", "invalid id")
            return
        try:
            round_number, question, progress_token = _parse_tool_call(params)
        except ValueError as exc:
            self._write_error(request_id, -32602, "Invalid params", str(exc))
            return

        pending = PendingRequest(round_number=round_number, question=question)
        with self._active_lock:
            if request_id in self._active:
                self._write_error(
                    request_id,
                    -32600,
                    "Invalid Request",
                    "duplicate active request id",
                )
                return
            if self._active:
                active = next(iter(self._active.values()))
                if active.round_number == round_number:
                    self._write_error(
                        request_id,
                        ROUND_ALREADY_PENDING,
                        "Round already pending",
                        {
                            "round": round_number,
                            "recovery": (
                                "wait for or cancel the active request before "
                                "retrying the exact same round and question"
                            ),
                        },
                    )
                else:
                    self._write_error(
                        request_id,
                        ANOTHER_ROUND_PENDING,
                        "Another round is pending",
                        {
                            "active_round": active.round_number,
                            "requested_round": round_number,
                        },
                    )
                return
            self._active[request_id] = pending

        def run() -> None:
            response: dict[str, Any] | None = None
            try:
                result = self._service.publish_and_wait(
                    round_number,
                    question,
                    cancelled=pending.cancelled,
                    progress=lambda value, text: self._progress(
                        progress_token, value, text
                    ),
                )
                response = self._result_message(
                    request_id,
                    {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(
                                    result,
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                ),
                            }
                        ],
                        "structuredContent": result,
                        "isError": False,
                    },
                )
            except ToolCancelled:
                response = self._error_message(
                    request_id,
                    -32800,
                    "Request cancelled",
                    {
                        "reason": pending.reason,
                        "durable_question": "preserved",
                        "round": pending.round_number,
                        "recovery": (
                            "retry the exact same round and question in a later "
                            "tool call"
                        ),
                    },
                )
            except WaitingStopped as exc:
                response = self._error_message(
                    request_id,
                    -32001,
                    "Browser stopped waiting",
                    {"control": exc.control},
                )
            except (StateContractError, ValueError) as exc:
                response = self._error_message(
                    request_id, -32603, "Tool failed", str(exc)
                )
            except Exception as exc:  # Boundary: never leak a traceback over MCP.
                response = self._error_message(
                    request_id,
                    -32603,
                    "Tool failed",
                    f"{type(exc).__name__}: {exc}",
                )
            finally:
                with self._active_lock:
                    if self._active.get(request_id) is pending:
                        self._active.pop(request_id, None)
            # Release single-flight state before publishing the terminal response,
            # so a client may retry as soon as it observes that response.
            try:
                if response is not None:
                    self._write(response)
            finally:
                with self._active_lock:
                    self._workers.discard(threading.current_thread())

        thread = threading.Thread(
            target=run,
            name=f"mcp-{TOOL_NAME}-{request_id}",
            daemon=True,
        )
        pending.thread = thread
        with self._active_lock:
            self._workers.add(thread)
            thread.start()

    def _cancel(self, params: Any) -> None:
        if not isinstance(params, Mapping):
            return
        request_id = params.get("requestId", params.get("id"))
        with self._active_lock:
            pending = self._active.get(request_id)
        if pending is None:
            return
        reason = params.get("reason")
        if isinstance(reason, str) and reason:
            pending.reason = reason
        pending.cancelled.set()

    def _progress(self, token: str | int | None, value: int, message: str) -> None:
        if token is None:
            return
        self._write(
            {
                "jsonrpc": "2.0",
                "method": "notifications/progress",
                "params": {
                    "progressToken": token,
                    "progress": value,
                    "message": message,
                },
            }
        )

    def _write_result(self, request_id: str | int, result: Any) -> None:
        self._write(self._result_message(request_id, result))

    def _result_message(self, request_id: str | int, result: Any) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def _write_error(
        self,
        request_id: str | int | None,
        code: int,
        message: str,
        data: Any = None,
    ) -> None:
        self._write(self._error_message(request_id, code, message, data))

    def _error_message(
        self,
        request_id: str | int | None,
        code: int,
        message: str,
        data: Any = None,
    ) -> dict[str, Any]:
        error: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        return {"jsonrpc": "2.0", "id": request_id, "error": error}

    def _write(self, message: Mapping[str, Any]) -> None:
        encoded = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        with self._write_lock:
            self._output.write(encoded + "\n")
            self._output.flush()


def tool_definition() -> dict[str, Any]:
    return {
        "name": TOOL_NAME,
        "title": "Publish one X-Sync question and wait",
        "description": (
            "Durably publish one question to the Phase 0 browser and wait for "
            "the matching durable answer before returning."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "round": {"type": "integer", "minimum": 1},
                "question": {"type": "string", "minLength": 1},
            },
            "required": ["round", "question"],
            "additionalProperties": False,
        },
        "outputSchema": {
            "type": "object",
            "properties": {
                "round": {"type": "integer"},
                "answer": {"type": "string"},
                "answer_request_id": {"type": ["string", "null"]},
                "answered_at": {"type": ["number", "null"]},
            },
            "required": ["round", "answer"],
        },
        "annotations": {
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    }


def _parse_tool_call(params: Any) -> tuple[int, str, str | int | None]:
    if not isinstance(params, Mapping):
        raise ValueError("tools/call params must be an object")
    if params.get("name") != TOOL_NAME:
        raise ValueError(f"unknown tool {params.get('name')!r}")
    arguments = params.get("arguments")
    if not isinstance(arguments, Mapping):
        raise ValueError("arguments must be an object")
    extra = set(arguments) - {"round", "question"}
    if extra:
        raise ValueError(f"unexpected arguments: {sorted(extra)!r}")
    round_number = arguments.get("round")
    question = arguments.get("question")
    if isinstance(round_number, bool) or not isinstance(round_number, int):
        raise ValueError("round must be an integer")
    if round_number < 1:
        raise ValueError("round must be at least 1")
    if not isinstance(question, str) or not question.strip():
        raise ValueError("question must be a non-empty string")
    progress_token: str | int | None = None
    metadata = params.get("_meta")
    if isinstance(metadata, Mapping):
        candidate = metadata.get("progressToken")
        if isinstance(candidate, (str, int)) and not isinstance(candidate, bool):
            progress_token = candidate
    return round_number, question.strip(), progress_token


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe-url", required=True)
    token = parser.add_mutually_exclusive_group(required=True)
    token.add_argument("--probe-token")
    token.add_argument("--probe-token-env")
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--progress-interval", type=float, default=10.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    probe_token = args.probe_token
    if args.probe_token_env is not None:
        probe_token = os.environ.get(args.probe_token_env)
        if not probe_token:
            raise SystemExit(
                f"probe token environment variable is missing: "
                f"{args.probe_token_env}"
            )
    journal = HttpJournal(args.probe_url, probe_token)
    service = PublishAndWaitService(
        journal,
        poll_interval=args.poll_interval,
        progress_interval=args.progress_interval,
    )
    MCPServer(service, sys.stdout).serve(sys.stdin)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
