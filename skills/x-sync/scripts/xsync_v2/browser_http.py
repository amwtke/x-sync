"""Authenticated HTTP-shaped adapter for the X-Sync v2 Browser surface.

The adapter is deliberately server-framework neutral.  A loopback transport
passes immutable request records to :class:`BrowserApi`; domain mutation stays
inside ``BrowserCommandService`` and the canonical coordinator.
"""

from __future__ import annotations

from dataclasses import dataclass
import hmac
import json
import re
from urllib.parse import parse_qs, urlsplit

from .browser_service import (
    BrowserCommandRequest,
    BrowserCommandService,
    BrowserIntent,
    BrowserServiceError,
    PauseTopicIntent,
    RecoverWorkIntent,
    ResumeTopicIntent,
    SubmitTurnIntent,
)
from .domain import (
    ConversationPhase,
    DialogueState,
    WorkRecoveryAction,
)
from .observers.public_stream import (
    PublicStreamError,
    PublicStreamEvent,
    PublicStreamObserver,
    PublicStreamSubscription,
)


MAX_HTTP_BODY_BYTES = 128 * 1024
_IF_MATCH = re.compile(r'"conversation-v(0|[1-9][0-9]*)"\Z')
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")
_COMMON_HEADERS = (
    ("Cache-Control", "no-store"),
    ("X-Content-Type-Options", "nosniff"),
    ("X-Frame-Options", "DENY"),
)


class BrowserApiError(RuntimeError):
    """Stable HTTP-boundary rejection before any domain mutation."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class BrowserHttpRequest:
    """Immutable HTTP request data accepted from a loopback server."""

    method: str
    target: str
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def __post_init__(self) -> None:
        if (
            type(self) is not BrowserHttpRequest
            or type(self.method) is not str
            or self.method not in {"GET", "POST"}
            or type(self.target) is not str
            or not self.target.startswith("/")
            or any(ord(character) < 32 for character in self.target)
            or type(self.headers) is not tuple
            or any(
                type(item) is not tuple
                or len(item) != 2
                or type(item[0]) is not str
                or type(item[1]) is not str
                or _HEADER_NAME.fullmatch(item[0]) is None
                or any(
                    ord(character) < 32 or ord(character) == 127
                    for character in item[1]
                )
                for item in self.headers
            )
            or type(self.body) is not bytes
        ):
            raise ValueError("INVALID_BROWSER_HTTP_REQUEST")


@dataclass(frozen=True, slots=True)
class BrowserHttpResponse:
    """Immutable, fully encoded response returned to a loopback transport."""

    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def __post_init__(self) -> None:
        if (
            type(self) is not BrowserHttpResponse
            or type(self.status) is not int
            or not 100 <= self.status <= 599
            or type(self.headers) is not tuple
            or any(
                type(item) is not tuple
                or len(item) != 2
                or any(type(value) is not str for value in item)
                for item in self.headers
            )
            or type(self.body) is not bytes
        ):
            raise ValueError("INVALID_BROWSER_HTTP_RESPONSE")

    def header(self, name: str) -> str | None:
        """Return one response header using ASCII case-insensitive matching."""
        lowered = name.lower()
        return next(
            (value for key, value in self.headers if key.lower() == lowered),
            None,
        )


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _headers(request: BrowserHttpRequest) -> dict[str, str]:
    result: dict[str, str] = {}
    for name, value in request.headers:
        lowered = name.lower()
        if lowered in result:
            raise BrowserApiError("VALIDATION_FAILED")
        result[lowered] = value
    return result


def _response(
    status: int,
    body: object,
    *,
    extra_headers: tuple[tuple[str, str], ...] = (),
) -> BrowserHttpResponse:
    encoded = _json_bytes(body)
    return BrowserHttpResponse(
        status,
        (
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(encoded))),
            *_COMMON_HEADERS,
            *extra_headers,
        ),
        encoded,
    )


_ERRORS: dict[str, tuple[int, str, bool, str]] = {
    "AUTH_REQUIRED": (401, "需要会话授权", False, "reload_page"),
    "BAD_HOST": (403, "请求目标不匹配", False, "reload_page"),
    "BAD_ORIGIN": (403, "请求来源不匹配", False, "reload_page"),
    "CURSOR_EXPIRED": (410, "事件游标已过期", True, "reload_state"),
    "CURSOR_AHEAD": (409, "事件游标超前", True, "reload_state"),
    "RESYNC_REQUIRED": (409, "客户端需要重新同步", True, "reload_state"),
    "VERSION_CONFLICT": (409, "状态已经更新", True, "reload_state"),
    "TOPIC_STATE_CONFLICT": (409, "当前状态不允许此操作", False, "reload_state"),
    "SESSION_DEACTIVATED": (409, "会话已切换", False, "reload_state"),
    "IDEMPOTENCY_CONFLICT": (409, "幂等键已用于其他请求", False, "new_key"),
    "TURN_PENDING": (409, "已有回答正在处理", True, "reload_state"),
    "WORK_SUPERSEDED": (409, "工作已经失效", False, "reload_state"),
    "VALIDATION_FAILED": (400, "请求格式无效", False, "fix_request"),
    "PAYLOAD_TOO_LARGE": (413, "请求内容过大", False, "reduce_payload"),
    "EVIDENCE_VERIFICATION_FAILED": (
        409,
        "证据暂时无法核验",
        True,
        "reload_state",
    ),
    "EVIDENCE_CHECK_CONFLICT": (409, "证据已经更新", True, "reload_state"),
    "EVIDENCE_CHANGED": (409, "证据已经更新", True, "reload_state"),
    "NOT_FOUND": (404, "接口不存在", False, "check_path"),
    "INTERNAL_ERROR": (500, "运行时发生内部错误", True, "retry"),
}


def _error(code: str) -> BrowserHttpResponse:
    public_code = (
        "VERSION_CONFLICT"
        if code == "CONVERSATION_VERSION_CONFLICT"
        else code
    )
    status, message, retryable, recovery = _ERRORS.get(
        public_code,
        _ERRORS["INTERNAL_ERROR"],
    )
    if public_code not in _ERRORS:
        public_code = "INTERNAL_ERROR"
    return _response(
        status,
        {
            "error": {
                "code": public_code,
                "message": message,
                "retryable": retryable,
                "recovery": recovery,
            }
        },
    )


def _allowed_actions(state: DialogueState) -> tuple[str, ...]:
    actions: set[str] = set()
    if state.active_topic is not None:
        actions.add("pause")
    if state.phase is ConversationPhase.AWAITING_USER:
        actions.add("submit_turn")
    if state.phase is ConversationPhase.NONE and state.paused_topics:
        actions.add("resume")
    if state.phase is ConversationPhase.RECOVERABLE_ERROR:
        actions.add("recover")
    return tuple(sorted(actions))


def _public_state(state: DialogueState) -> dict[str, object]:
    topic_view: dict[str, object] | None = None
    topic = state.active_topic
    if topic is not None:
        question: dict[str, str] | None = None
        turn = topic.current_agent_turn
        if state.phase is ConversationPhase.AWAITING_USER and turn is not None:
            question = {
                "id": turn.question_id,
                "heard": turn.heard,
                "one_step_further": turn.one_step_further,
                "text": turn.question,
                "intent": turn.question_intent.value,
            }
        topic_view = {
            "id": topic.topic_run_id,
            "title": topic.contract.title,
            "guiding_question": topic.contract.guiding_question,
            "objective": topic.contract.objective,
            "lens": topic.contract.starting_lens.value,
            "lifecycle": topic.lifecycle.value,
            "evidence_health": topic.evidence_health.value,
            "question": question,
        }
    return {
        "schema_version": 2,
        "session_id": state.session_id,
        "event_sequence": state.sequence,
        "conversation_version": state.conversation_version,
        "lifecycle": state.lifecycle.value,
        "phase": state.phase.value,
        "candidates": list(state.candidates),
        "topic": topic_view,
        "paused_topics": [
            {
                "id": topic.topic_run_id,
                "title": topic.contract.title,
                "lifecycle": topic.lifecycle.value,
            }
            for topic in state.paused_topics
        ],
        "allowed_actions": list(_allowed_actions(state)),
    }


def _sse_event(event: PublicStreamEvent) -> bytes:
    data = {
        "event_id": event.event_id,
        "sequence": event.sequence,
        "type": event.event_type,
        "fields": {name: value for name, value in event.fields},
    }
    return (
        f"id: {event.sequence}\n"
        f"event: {event.event_type}\n"
        f"data: {_json_bytes(data).decode('utf-8')}\n\n"
    ).encode()


class BrowserSseStream:
    """Authenticated live SSE handle backed by one bounded subscription."""

    __slots__ = ("_subscription",)

    def __init__(self, subscription: PublicStreamSubscription) -> None:
        if type(subscription) is not PublicStreamSubscription:
            raise ValueError("INVALID_BROWSER_SSE_STREAM")
        self._subscription = subscription

    @property
    def cursor(self) -> int:
        """Return the last event sequence drained by this connection."""
        return self._subscription.cursor

    def read(
        self,
        *,
        timeout: float | int,
        max_events: int = 64,
    ) -> bytes:
        """Wait for safe events and return one SSE frame block or keepalive."""
        available = self._subscription.wait_available(timeout)
        if not available:
            return b": keepalive\n\n"
        events = self._subscription.read_available(max_events=max_events)
        return b"".join(_sse_event(event) for event in events)

    def close(self) -> None:
        """Idempotently release the transient browser subscription."""
        self._subscription.close()


class BrowserApi:
    """Authenticate and map Browser HTTP records without owning domain state."""

    def __init__(
        self,
        service: BrowserCommandService,
        stream: PublicStreamObserver,
        *,
        session_id: str,
        capability: str,
        expected_host: str,
        expected_origin: str,
    ) -> None:
        if (
            type(service) is not BrowserCommandService
            or type(stream) is not PublicStreamObserver
            or not all(
                type(value) is str
                and value
                and value == value.strip()
                and "\r" not in value
                and "\n" not in value
                for value in (
                    session_id,
                    capability,
                    expected_host,
                    expected_origin,
                )
            )
        ):
            raise ValueError("INVALID_BROWSER_API_CONFIGURATION")
        self._service = service
        self._stream = stream
        self._session_id = session_id
        self._authorization = "Bearer " + capability
        self._expected_host = expected_host
        self._expected_origin = expected_origin

    def handle(self, request: BrowserHttpRequest) -> BrowserHttpResponse:
        """Handle one bounded request and return only stable public errors."""
        try:
            if type(request) is not BrowserHttpRequest:
                raise BrowserApiError("VALIDATION_FAILED")
            headers = _headers(request)
            self._authenticate(request.method, headers)
            if len(request.body) > MAX_HTTP_BODY_BYTES:
                raise BrowserApiError("PAYLOAD_TOO_LARGE")
            parsed = urlsplit(request.target)
            if parsed.scheme or parsed.netloc or parsed.fragment:
                raise BrowserApiError("VALIDATION_FAILED")
            if request.method == "GET" and parsed.path == "/api/v2/state":
                if parsed.query or request.body:
                    raise BrowserApiError("VALIDATION_FAILED")
                return self._state()
            if request.method == "GET" and parsed.path == "/api/v2/stream":
                if request.body:
                    raise BrowserApiError("VALIDATION_FAILED")
                return self._stream_response(parsed.query)
            if request.method == "POST" and parsed.path in {
                "/api/v2/turns",
                "/api/v2/topic",
            }:
                if parsed.query:
                    raise BrowserApiError("VALIDATION_FAILED")
                return self._mutation(parsed.path, headers, request.body)
            raise BrowserApiError("NOT_FOUND")
        except (BrowserApiError, BrowserServiceError) as exc:
            return _error(exc.code)
        except PublicStreamError as exc:
            return _error(exc.code)
        except Exception:
            return _error("INTERNAL_ERROR")

    def open_stream(self, request: BrowserHttpRequest) -> BrowserSseStream:
        """Authenticate and open a live stream without consuming its cursor."""
        if type(request) is not BrowserHttpRequest or request.method != "GET":
            raise BrowserApiError("VALIDATION_FAILED")
        headers = _headers(request)
        self._authenticate(request.method, headers)
        if request.body:
            raise BrowserApiError("VALIDATION_FAILED")
        parsed = urlsplit(request.target)
        if (
            parsed.scheme
            or parsed.netloc
            or parsed.fragment
            or parsed.path != "/api/v2/stream"
        ):
            raise BrowserApiError("VALIDATION_FAILED")
        cursor = self._stream_cursor(parsed.query)
        return BrowserSseStream(
            self._stream.subscribe(self._session_id, cursor)
        )

    @staticmethod
    def error_response(code: str) -> BrowserHttpResponse:
        """Map one stable adapter/runtime code without exposing exception text."""
        return _error(code)

    def _authenticate(self, method: str, headers: dict[str, str]) -> None:
        authorization = headers.get("authorization", "")
        if not hmac.compare_digest(authorization, self._authorization):
            raise BrowserApiError("AUTH_REQUIRED")
        if headers.get("host") != self._expected_host:
            raise BrowserApiError("BAD_HOST")
        origin = headers.get("origin")
        if method == "POST":
            if origin != self._expected_origin:
                raise BrowserApiError("BAD_ORIGIN")
        elif origin is not None:
            if origin != self._expected_origin:
                raise BrowserApiError("BAD_ORIGIN")
        elif headers.get("sec-fetch-site") not in {"same-origin", "none"}:
            raise BrowserApiError("BAD_ORIGIN")

    def _state(self) -> BrowserHttpResponse:
        resolution = self._service.current(self._session_id)
        state = resolution.dialogue_state
        return _response(
            200,
            _public_state(state),
            extra_headers=(("ETag", f'"conversation-v{state.conversation_version}"'),),
        )

    def _stream_response(self, query: str) -> BrowserHttpResponse:
        cursor = self._stream_cursor(query)
        subscription = self._stream.subscribe(self._session_id, cursor)
        try:
            events = subscription.read_available()
        finally:
            subscription.close()
        body = b"".join(_sse_event(event) for event in events)
        if not body:
            body = b": keepalive\n\n"
        return BrowserHttpResponse(
            200,
            (
                ("Content-Type", "text/event-stream"),
                ("Content-Length", str(len(body))),
                ("X-Accel-Buffering", "no"),
                *_COMMON_HEADERS,
            ),
            body,
        )

    @staticmethod
    def _stream_cursor(query: str) -> int:
        try:
            parameters = parse_qs(
                query,
                keep_blank_values=True,
                strict_parsing=True,
            )
        except ValueError as exc:
            raise BrowserApiError("VALIDATION_FAILED") from exc
        if set(parameters) != {"after"} or len(parameters["after"]) != 1:
            raise BrowserApiError("VALIDATION_FAILED")
        raw_cursor = parameters["after"][0]
        if (
            not raw_cursor.isascii()
            or not raw_cursor.isdigit()
            or len(raw_cursor) > 20
        ):
            raise BrowserApiError("VALIDATION_FAILED")
        return int(raw_cursor)

    def _mutation(
        self,
        path: str,
        headers: dict[str, str],
        raw_body: bytes,
    ) -> BrowserHttpResponse:
        content_type = headers.get("content-type", "").split(";", 1)[0].strip()
        if content_type != "application/json":
            raise BrowserApiError("VALIDATION_FAILED")
        key = headers.get("idempotency-key", "")
        if not key:
            raise BrowserApiError("VALIDATION_FAILED")
        matched = _IF_MATCH.fullmatch(headers.get("if-match", ""))
        if matched is None:
            raise BrowserApiError("VALIDATION_FAILED")
        try:
            body = json.loads(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BrowserApiError("VALIDATION_FAILED") from exc
        if type(body) is not dict or any(type(key) is not str for key in body):
            raise BrowserApiError("VALIDATION_FAILED")
        intent = self._intent(path, body)
        outcome = self._service.execute(
            BrowserCommandRequest(
                self._session_id,
                key,
                int(matched.group(1)),
                intent,
            )
        )
        state = outcome.state
        return _response(
            200,
            {
                "state": _public_state(state),
                "replayed": outcome.replayed,
            },
            extra_headers=(("ETag", f'"conversation-v{state.conversation_version}"'),),
        )

    @staticmethod
    def _intent(path: str, body: dict[str, object]) -> BrowserIntent:
        if path == "/api/v2/turns":
            if set(body) != {"question_id", "text"}:
                raise BrowserApiError("VALIDATION_FAILED")
            question_id = body["question_id"]
            text = body["text"]
            if type(question_id) is not str or type(text) is not str:
                raise BrowserApiError("VALIDATION_FAILED")
            return SubmitTurnIntent(question_id, text)
        action = body.get("action")
        if action == "pause" and set(body) == {"action"}:
            return PauseTopicIntent()
        if action == "resume" and set(body) == {"action", "topic_run_id"}:
            topic_run_id = body["topic_run_id"]
            if type(topic_run_id) is not str:
                raise BrowserApiError("VALIDATION_FAILED")
            return ResumeTopicIntent(topic_run_id)
        if action == "recover" and set(body) == {
            "action",
            "dead_work_id",
            "recovery",
        }:
            dead_work_id = body["dead_work_id"]
            raw_recovery = body["recovery"]
            if type(dead_work_id) is not str or type(raw_recovery) is not str:
                raise BrowserApiError("VALIDATION_FAILED")
            try:
                recovery = WorkRecoveryAction(raw_recovery)
            except (TypeError, ValueError) as exc:
                raise BrowserApiError("VALIDATION_FAILED") from exc
            return RecoverWorkIntent(dead_work_id, recovery)
        raise BrowserApiError("TOPIC_STATE_CONFLICT")


__all__ = [
    "MAX_HTTP_BODY_BYTES",
    "BrowserApi",
    "BrowserApiError",
    "BrowserHttpRequest",
    "BrowserHttpResponse",
    "BrowserSseStream",
]
