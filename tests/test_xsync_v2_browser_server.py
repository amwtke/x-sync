from __future__ import annotations

import http.client
import json
from pathlib import Path
import socket
import unittest

import tests.xsync_v2_path  # noqa: F401
from tests.test_xsync_v2_browser_http import FakeCoordinator

from xsync_v2.browser_server import (
    BrowserServerError,
    LoopbackBrowserServer,
)
from xsync_v2.browser_service import BrowserCommandService
from xsync_v2.domain import EvidenceCheck, EvidenceHealth
from xsync_v2.observer import (
    CommittedBatch,
    CommittedEventView,
    ImmutablePayloadView,
    StreamKind,
)
from xsync_v2.observers.public_stream import PublicStreamObserver


class LoopbackBrowserServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.coordinator = FakeCoordinator()
        self.stream = PublicStreamObserver()
        self.service = BrowserCommandService(
            self.coordinator,
            lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            clock=lambda: "2026-08-16T13:00:00+08:00",
        )
        self.server = LoopbackBrowserServer(
            self.service,
            self.stream,
            session_id="session-1",
            capability="server-secret",
            keepalive_seconds=0.05,
        ).start()

    def tearDown(self) -> None:
        self.server.close()

    def headers(self, *, origin: bool = True) -> dict[str, str]:
        values = {
            "Authorization": "Bearer server-secret",
            "Host": self.server.address.authority,
        }
        if origin:
            values["Origin"] = self.server.address.origin
        else:
            values["Sec-Fetch-Site"] = "same-origin"
        return values

    def connection(self) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(
            self.server.address.host,
            self.server.address.port,
            timeout=2.0,
        )

    def test_binds_only_loopback_and_serves_authenticated_state(self) -> None:
        self.assertEqual("127.0.0.1", self.server.address.host)
        connection = self.connection()
        connection.request("GET", "/api/v2/state", headers=self.headers())
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()

        self.assertEqual(200, response.status)
        self.assertEqual("session-1", payload["session_id"])
        self.assertEqual('"conversation-v5"', response.getheader("ETag"))
        self.assertEqual("DENY", response.getheader("X-Frame-Options"))

        denied = self.connection()
        denied.request(
            "GET",
            "/api/v2/state",
            headers={"Host": self.server.address.authority},
        )
        denied_response = denied.getresponse()
        denied_body = denied_response.read()
        denied.close()
        self.assertEqual(401, denied_response.status)
        self.assertNotIn(b"server-secret", denied_body)

    def test_serves_fragment_authenticated_app_with_strict_browser_headers(
        self,
    ) -> None:
        self.assertEqual(
            f"{self.server.address.origin}/#server-secret",
            self.server.launch_url,
        )
        for path, content_type in (
            ("/", "text/html; charset=utf-8"),
            ("/dialogue.css", "text/css; charset=utf-8"),
            ("/dialogue.js", "text/javascript; charset=utf-8"),
        ):
            with self.subTest(path=path):
                connection = self.connection()
                connection.request(
                    "GET",
                    path,
                    headers={
                        "Host": self.server.address.authority,
                        "Sec-Fetch-Site": "none",
                    },
                )
                response = connection.getresponse()
                body = response.read()
                connection.close()
                self.assertEqual(200, response.status)
                self.assertEqual(content_type, response.getheader("Content-Type"))
                self.assertEqual("no-store", response.getheader("Cache-Control"))
                self.assertEqual("no-referrer", response.getheader("Referrer-Policy"))
                self.assertIn(
                    "default-src 'none'",
                    response.getheader("Content-Security-Policy"),
                )
                self.assertNotIn(b"server-secret", body)

        connection = self.connection()
        connection.request(
            "GET",
            "/",
            headers={"Host": "attacker.invalid"},
        )
        denied = connection.getresponse()
        denied.read()
        connection.close()
        self.assertEqual(403, denied.status)

    def test_dialogue_assets_keep_data_in_text_content_and_bearer_fetch(self) -> None:
        asset_root = (
            Path(__file__).resolve().parents[1]
            / "skills"
            / "x-sync"
            / "assets"
        )
        html = (asset_root / "dialogue.html").read_text(encoding="utf-8")
        javascript = (asset_root / "dialogue.js").read_text(encoding="utf-8")

        self.assertIn('<script src="/dialogue.js" defer></script>', html)
        self.assertNotIn("<script>", html)
        self.assertIn(
            'headers.set("Authorization", `Bearer ${capability}`)',
            javascript,
        )
        self.assertIn("/api/v2/stream?after=", javascript)
        self.assertNotIn("EventSource", javascript)
        self.assertNotIn("innerHTML", javascript)

    def test_post_turn_uses_the_same_typed_browser_service(self) -> None:
        body = json.dumps(
            {"question_id": "question-1", "text": "由 outbox 重试。"}
        ).encode()
        headers = {
            **self.headers(),
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "Idempotency-Key": "server-turn-1",
            "If-Match": '"conversation-v5"',
        }
        connection = self.connection()
        connection.request("POST", "/api/v2/turns", body=body, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()

        self.assertEqual(200, response.status)
        self.assertFalse(payload["replayed"])
        self.assertEqual(1, len(self.coordinator.requests))
        self.assertEqual(
            "由 outbox 重试。",
            self.coordinator.requests[0].command.text,
        )

    def test_live_sse_waits_then_delivers_after_commit(self) -> None:
        connection = self.connection()
        connection.request(
            "GET",
            "/api/v2/stream?after=0",
            headers=self.headers(origin=False),
        )
        response = connection.getresponse()
        self.assertEqual(200, response.status)
        self.assertEqual("text/event-stream", response.getheader("Content-Type"))

        self.stream.on_batch(
            CommittedBatch(
                StreamKind.DIALOGUE,
                "session-1",
                (
                    CommittedEventView(
                        "event-live",
                        1,
                        ImmutablePayloadView(
                            "agent_turn_committed",
                            (
                                ("question_id", "question-live"),
                                ("question", "下一步如何验证?"),
                                ("claim_id", "must-not-leak"),
                            ),
                        ),
                    ),
                ),
            )
        )
        lines = tuple(response.fp.readline() for _ in range(4))
        connection.close()
        rendered = b"".join(lines)
        self.assertIn(b"id: 1", rendered)
        self.assertIn(b"event: agent_turn_committed", rendered)
        self.assertIn(b"question-live", rendered)
        self.assertNotIn(b"must-not-leak", rendered)

    def test_live_sse_emits_keepalive_without_domain_events(self) -> None:
        connection = self.connection()
        connection.request(
            "GET",
            "/api/v2/stream?after=0",
            headers=self.headers(),
        )
        response = connection.getresponse()
        self.assertEqual(b": keepalive\n", response.fp.readline())
        self.assertEqual(b"\n", response.fp.readline())
        connection.close()
        self.assertEqual(0, self.coordinator.state.sequence - 8)

    def test_oversized_body_is_rejected_before_reading_payload(self) -> None:
        with socket.create_connection(
            (self.server.address.host, self.server.address.port),
            timeout=2.0,
        ) as client:
            request = (
                "POST /api/v2/turns HTTP/1.1\r\n"
                f"Host: {self.server.address.authority}\r\n"
                "Authorization: Bearer server-secret\r\n"
                f"Origin: {self.server.address.origin}\r\n"
                "Content-Type: application/json\r\n"
                "Idempotency-Key: too-large\r\n"
                'If-Match: "conversation-v5"\r\n'
                "Content-Length: 131073\r\n"
                "Connection: close\r\n\r\n"
            ).encode()
            client.sendall(request)
            response = client.recv(4096)
        self.assertIn(b" 413 ", response.split(b"\r\n", 1)[0])
        self.assertEqual((), tuple(self.coordinator.requests))

    def test_lifecycle_is_explicit_and_close_wakes_live_streams(self) -> None:
        with self.assertRaisesRegex(
            BrowserServerError,
            "BROWSER_SERVER_STATE_CONFLICT",
        ):
            self.server.start()

        connection = self.connection()
        connection.request(
            "GET",
            "/api/v2/stream?after=0",
            headers=self.headers(),
        )
        response = connection.getresponse()
        self.assertEqual(200, response.status)
        self.server.close()
        connection.close()
        self.server.close()

        with self.assertRaisesRegex(
            BrowserServerError,
            "BROWSER_SERVER_STATE_CONFLICT",
        ):
            self.server.start()

    def test_invalid_configuration_fails_before_serving(self) -> None:
        for port, keepalive in ((-1, 1.0), (0, 0), (0, float("inf"))):
            with self.subTest(port=port, keepalive=keepalive):
                with self.assertRaisesRegex(
                    BrowserServerError,
                    "INVALID_BROWSER_SERVER_CONFIGURATION",
                ):
                    LoopbackBrowserServer(
                        self.service,
                        self.stream,
                        session_id="session-1",
                        capability="secret",
                        port=port,
                        keepalive_seconds=keepalive,
                    )


if __name__ == "__main__":
    unittest.main()
