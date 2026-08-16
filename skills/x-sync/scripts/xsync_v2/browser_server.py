"""Owner-only loopback HTTP transport for the X-Sync Browser API."""

from __future__ import annotations

from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import math
from pathlib import Path
import threading
from typing import cast
from urllib.parse import SplitResult, quote, urlsplit

from .browser_http import (
    MAX_HTTP_BODY_BYTES,
    BrowserApi,
    BrowserApiError,
    BrowserHttpRequest,
    BrowserHttpResponse,
    BrowserSseStream,
)
from .browser_service import BrowserCommandService
from .observers.public_stream import (
    PublicStreamError,
    PublicStreamObserver,
)


class BrowserServerError(RuntimeError):
    """Stable loopback transport configuration or lifecycle failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class BrowserServerAddress:
    """Public non-secret address of one bound loopback server."""

    host: str
    port: int

    @property
    def authority(self) -> str:
        """Return the exact Host header required by the Browser API."""
        return f"{self.host}:{self.port}"

    @property
    def origin(self) -> str:
        """Return the exact HTTP Origin accepted by the Browser API."""
        return f"http://{self.authority}"


class _HttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False
    block_on_close = False

    api: BrowserApi
    keepalive_seconds: float
    stopping: threading.Event
    active_streams: set[BrowserSseStream]
    active_streams_lock: threading.Lock
    assets: dict[str, tuple[str, bytes]]
    expected_host: str
    expected_origin: str


_CONTENT_SECURITY_POLICY = (
    "default-src 'none'; script-src 'self'; style-src 'self'; "
    "connect-src 'self'; base-uri 'none'; form-action 'self'; "
    "frame-ancestors 'none'"
)
_ASSET_PATHS = {
    "/": ("dialogue.html", "text/html; charset=utf-8"),
    "/dialogue.css": ("dialogue.css", "text/css; charset=utf-8"),
    "/dialogue.js": ("dialogue.js", "text/javascript; charset=utf-8"),
}
_MAX_ASSET_BYTES = 512 * 1024


def _load_browser_assets() -> dict[str, tuple[str, bytes]]:
    asset_root = Path(__file__).resolve().parents[2] / "assets"
    loaded: dict[str, tuple[str, bytes]] = {}
    for route, (name, content_type) in _ASSET_PATHS.items():
        try:
            raw = (asset_root / name).read_bytes()
        except OSError as exc:
            raise BrowserServerError("BROWSER_ASSET_UNAVAILABLE") from exc
        if not raw or len(raw) > _MAX_ASSET_BYTES:
            raise BrowserServerError("BROWSER_ASSET_INVALID")
        loaded[route] = (content_type, raw)
    return loaded


class _RequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "X-Sync"
    sys_version = ""

    def do_GET(self) -> None:
        self._dispatch()

    def do_POST(self) -> None:
        self._dispatch()

    def log_message(self, _format: str, *_args: object) -> None:
        return None

    @property
    def _runtime(self) -> _HttpServer:
        return cast(_HttpServer, self.server)

    def _dispatch(self) -> None:
        request = self._read_request()
        if type(request) is BrowserHttpResponse:
            self._write_response(request)
            return
        request = cast(BrowserHttpRequest, request)
        parsed = urlsplit(request.target)
        if request.method == "GET" and parsed.path in self._runtime.assets:
            self._write_response(self._static_response(request, parsed))
            return
        if request.method == "GET" and parsed.path == "/api/v2/stream":
            try:
                stream = self._runtime.api.open_stream(request)
            except (BrowserApiError, PublicStreamError) as exc:
                self._write_response(self._runtime.api.error_response(exc.code))
                return
            except Exception:
                self._write_response(
                    self._runtime.api.error_response("INTERNAL_ERROR")
                )
                return
            self._write_live_stream(stream)
            return
        self._write_response(self._runtime.api.handle(request))

    def _static_response(
        self,
        request: BrowserHttpRequest,
        parsed: SplitResult,
    ) -> BrowserHttpResponse:
        if request.body or parsed.query or parsed.fragment:
            return self._runtime.api.error_response("VALIDATION_FAILED")
        if self.headers.get("Host") != self._runtime.expected_host:
            return self._runtime.api.error_response("BAD_HOST")
        origin = self.headers.get("Origin")
        if origin is not None and origin != self._runtime.expected_origin:
            return self._runtime.api.error_response("BAD_ORIGIN")
        if origin is None and self.headers.get("Sec-Fetch-Site") not in {
            None,
            "same-origin",
            "none",
        }:
            return self._runtime.api.error_response("BAD_ORIGIN")
        content_type, body = self._runtime.assets[parsed.path]
        return BrowserHttpResponse(
            200,
            (
                ("Content-Type", content_type),
                ("Content-Length", str(len(body))),
                ("Cache-Control", "no-store"),
                ("Content-Security-Policy", _CONTENT_SECURITY_POLICY),
                ("Referrer-Policy", "no-referrer"),
                ("X-Content-Type-Options", "nosniff"),
                ("X-Frame-Options", "DENY"),
            ),
            body,
        )

    def _read_request(self) -> BrowserHttpRequest | BrowserHttpResponse:
        api = self._runtime.api
        transfer_encoding = self.headers.get("Transfer-Encoding")
        content_lengths = self.headers.get_all("Content-Length", failobj=[])
        if transfer_encoding is not None or len(content_lengths) > 1:
            self.close_connection = True
            return api.error_response("VALIDATION_FAILED")
        length = 0
        if content_lengths:
            raw_length = content_lengths[0]
            if (
                not raw_length.isascii()
                or not raw_length.isdigit()
                or len(raw_length) > 10
            ):
                self.close_connection = True
                return api.error_response("VALIDATION_FAILED")
            length = int(raw_length)
        if length > MAX_HTTP_BODY_BYTES:
            self.close_connection = True
            return api.error_response("PAYLOAD_TOO_LARGE")
        body = self.rfile.read(length)
        if len(body) != length:
            self.close_connection = True
            return api.error_response("VALIDATION_FAILED")
        try:
            return BrowserHttpRequest(
                self.command,
                self.path,
                tuple(self.headers.raw_items()),
                body,
            )
        except ValueError:
            return api.error_response("VALIDATION_FAILED")

    def _write_response(self, response: BrowserHttpResponse) -> None:
        try:
            self.send_response(response.status)
            for name, value in response.headers:
                self.send_header(name, value)
            if self.close_connection:
                self.send_header("Connection", "close")
            self.end_headers()
            if response.body:
                self.wfile.write(response.body)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.close_connection = True

    def _write_live_stream(self, stream: BrowserSseStream) -> None:
        self.close_connection = True
        with self._runtime.active_streams_lock:
            self._runtime.active_streams.add(stream)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.flush()
            while not self._runtime.stopping.is_set():
                chunk = stream.read(
                    timeout=self._runtime.keepalive_seconds,
                )
                self.wfile.write(chunk)
                self.wfile.flush()
        except (
            BrokenPipeError,
            ConnectionResetError,
            OSError,
            PublicStreamError,
        ):
            return
        finally:
            with self._runtime.active_streams_lock:
                self._runtime.active_streams.discard(stream)
            stream.close()


class LoopbackBrowserServer:
    """Bind and supervise one authenticated Browser API on ``127.0.0.1``."""

    def __init__(
        self,
        service: BrowserCommandService,
        stream: PublicStreamObserver,
        *,
        session_id: str,
        capability: str,
        port: int = 0,
        keepalive_seconds: float | int = 15.0,
    ) -> None:
        if type(port) is not int or not 0 <= port <= 65_535:
            raise BrowserServerError("INVALID_BROWSER_SERVER_CONFIGURATION")
        if type(keepalive_seconds) not in {int, float}:
            raise BrowserServerError("INVALID_BROWSER_SERVER_CONFIGURATION")
        try:
            keepalive = float(keepalive_seconds)
        except (OverflowError, ValueError) as exc:
            raise BrowserServerError(
                "INVALID_BROWSER_SERVER_CONFIGURATION"
            ) from exc
        if not math.isfinite(keepalive) or not 0.01 <= keepalive <= 60.0:
            raise BrowserServerError("INVALID_BROWSER_SERVER_CONFIGURATION")
        assets = _load_browser_assets()
        try:
            httpd = _HttpServer(("127.0.0.1", port), _RequestHandler)
        except OSError as exc:
            raise BrowserServerError("BROWSER_BIND_FAILED") from exc
        bound_host, bound_port = cast(tuple[str, int], httpd.server_address)
        address = BrowserServerAddress(bound_host, bound_port)
        try:
            api = BrowserApi(
                service,
                stream,
                session_id=session_id,
                capability=capability,
                expected_host=address.authority,
                expected_origin=address.origin,
            )
        except Exception:
            httpd.server_close()
            raise
        httpd.api = api
        httpd.keepalive_seconds = keepalive
        httpd.stopping = threading.Event()
        httpd.active_streams = set()
        httpd.active_streams_lock = threading.Lock()
        httpd.assets = assets
        httpd.expected_host = address.authority
        httpd.expected_origin = address.origin
        self._httpd = httpd
        self._address = address
        self._launch_url = f"{address.origin}/#{quote(capability, safe='')}"
        self._thread: threading.Thread | None = None
        self._lifecycle_lock = threading.Lock()
        self._closed = False

    @property
    def address(self) -> BrowserServerAddress:
        """Return the bound loopback host and ephemeral or requested port."""
        return self._address

    @property
    def launch_url(self) -> str:
        """Return the fragment-authenticated URL to open in the owner Browser."""
        return self._launch_url

    def start(self) -> LoopbackBrowserServer:
        """Start one daemon transport thread; repeated starts fail closed."""
        with self._lifecycle_lock:
            if self._closed or self._thread is not None:
                raise BrowserServerError("BROWSER_SERVER_STATE_CONFLICT")
            thread = threading.Thread(
                target=self._httpd.serve_forever,
                kwargs={"poll_interval": 0.05},
                name="x-sync-browser",
                daemon=True,
            )
            self._thread = thread
            thread.start()
        return self

    def close(self) -> None:
        """Idempotently stop accepts, wake streams, and release the port."""
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            thread = self._thread
            self._httpd.stopping.set()
            with self._httpd.active_streams_lock:
                streams = tuple(self._httpd.active_streams)
        for stream in streams:
            stream.close()
        if thread is not None:
            self._httpd.shutdown()
            thread.join(timeout=5.0)
            if thread.is_alive():
                raise BrowserServerError("BROWSER_SERVER_SHUTDOWN_FAILED")
        self._httpd.server_close()

    def __enter__(self) -> LoopbackBrowserServer:
        """Start the server for a bounded context-manager lifetime."""
        return self.start()

    def __exit__(
        self,
        _exc_type: object,
        _exc: object,
        _traceback: object,
    ) -> None:
        """Close the server when leaving a context-manager lifetime."""
        self.close()


__all__ = [
    "BrowserServerAddress",
    "BrowserServerError",
    "LoopbackBrowserServer",
]
