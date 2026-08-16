from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.browser_http import (
    BrowserApi,
    BrowserApiError,
    BrowserHttpRequest,
)
from xsync_v2.browser_service import (
    BrowserCommandRequest,
    BrowserCommandService,
    BrowserServiceError,
    PauseTopicIntent,
    RecoverWorkIntent,
    ResumeTopicIntent,
    SubmitTurnIntent,
    SwitchTopicIntent,
)
from xsync_v2.coordinator import CoordinatorError, DialogueSessionConfig
from xsync_v2.domain import (
    AgentTurnResult,
    ConversationPhase,
    CurrentWorkState,
    DialogueState,
    EvidenceCheck,
    EvidenceHealth,
    GateAssessment,
    GateId,
    Lens,
    PauseTopic,
    QuestionIntent,
    RecoverWork,
    ResumeTopic,
    SelectTopic,
    SessionLifecycle,
    SubmitLearnerTurn,
    SwitchTopic,
    TaskScope,
    TopicContract,
    TopicLifecycle,
    TopicRunState,
    TriggerBinding,
    TriggerKind,
    WorkRecoveryAction,
    WorkStatus,
)
from xsync_v2.observer import (
    CommittedBatch,
    CommittedEventView,
    ImmutablePayloadView,
    StreamKind,
)
from xsync_v2.observers.public_stream import (
    PublicStreamError,
    PublicStreamObserver,
)


def digest(label: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def contract() -> TopicContract:
    return TopicContract(
        "contract-1",
        1,
        "topic-1",
        "支付一致性",
        "支付失败后如何恢复?",
        "说清业务失败边界与仓库落点",
        TaskScope("task-1", "支付失败补偿", ("payments/",), ()),
        Lens.MIXED,
        True,
        (),
        (
            # Public projection only needs the stable gate identities.
        ),
        None,
        digest("contract"),
    )


def state() -> DialogueState:
    question = AgentTurnResult(
        "你已经定位到状态写入。",
        "还需要说明异步失败边界。",
        "question-1",
        "事件发送失败时由谁恢复?",
        QuestionIntent.CAUSAL_TRACE,
        (),
        (),
        ("secret-evidence-ref",),
    )
    topic = TopicRunState(
        "topic-1",
        contract(),
        TopicLifecycle.ACTIVE,
        EvidenceHealth.CURRENT,
        digest("evidence"),
        None,
        (
            GateAssessment(GateId.MECHANISM),
            GateAssessment(GateId.BOUNDARY),
            GateAssessment(GateId.REPOSITORY_APPLICATION),
        ),
        current_agent_turn=question,
    )
    return DialogueState(
        "session-1",
        3,
        8,
        5,
        SessionLifecycle.OPEN,
        ConversationPhase.AWAITING_USER,
        ("支付一致性", "Outbox"),
        None,
        topic,
        (),
    )


class FakeCoordinator:
    def __init__(self) -> None:
        self.config = DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repo-1",
            "2026-08-16T12:00:00+08:00",
            "runtime-1",
            digest("evidence"),
        )
        self.state = state()
        self.requests = []
        self.error_code: str | None = None

    def recover(self):
        return SimpleNamespace(config=self.config, dialogue_state=self.state)

    def execute(self, request):
        self.requests.append(request)
        if self.error_code is not None:
            raise CoordinatorError(self.error_code)
        return SimpleNamespace(state=self.state, replayed=False)


def request(
    method: str,
    path: str,
    *,
    body: object | None = None,
    headers: tuple[tuple[str, str], ...] = (),
) -> BrowserHttpRequest:
    baseline = (
        ("Authorization", "Bearer browser-secret"),
        ("Host", "127.0.0.1:43123"),
        ("Origin", "http://127.0.0.1:43123"),
    )
    encoded = b"" if body is None else json.dumps(body).encode()
    return BrowserHttpRequest(method, path, baseline + headers, encoded)


class BrowserHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.coordinator = FakeCoordinator()
        self.stream = PublicStreamObserver()
        self.service = BrowserCommandService(
            self.coordinator,
            lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            clock=lambda: "2026-08-16T12:05:00+08:00",
        )
        self.api = BrowserApi(
            self.service,
            self.stream,
            session_id="session-1",
            capability="browser-secret",
            expected_host="127.0.0.1:43123",
            expected_origin="http://127.0.0.1:43123",
        )

    def test_state_is_authenticated_safe_and_versioned(self) -> None:
        response = self.api.handle(request("GET", "/api/v2/state"))
        self.assertEqual(200, response.status)
        self.assertEqual('"conversation-v5"', response.header("ETag"))
        payload = json.loads(response.body)
        self.assertEqual("awaiting_user", payload["phase"])
        self.assertEqual("question-1", payload["topic"]["question"]["id"])
        self.assertEqual(
            ["pause", "submit_turn", "switch"], payload["allowed_actions"]
        )
        rendered = response.body.decode()
        self.assertNotIn("secret-evidence-ref", rendered)
        self.assertNotIn(digest("evidence"), rendered)
        self.assertNotIn("runtime-1", rendered)

        denied = self.api.handle(
            BrowserHttpRequest(
                "GET",
                "/api/v2/state",
                (("Host", "127.0.0.1:43123"),),
                b"",
            )
        )
        self.assertEqual(401, denied.status)
        self.assertEqual("AUTH_REQUIRED", json.loads(denied.body)["error"]["code"])
        self.assertNotIn("browser-secret", denied.body.decode())

    def test_turn_headers_and_body_become_one_typed_command(self) -> None:
        headers = (
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "turn-key-1"),
            ("If-Match", '"conversation-v5"'),
        )
        body = {"question_id": "question-1", "text": "由 outbox 重试。"}
        first = self.api.handle(
            request("POST", "/api/v2/turns", body=body, headers=headers)
        )
        second = self.api.handle(
            request("POST", "/api/v2/turns", body=body, headers=headers)
        )
        self.assertEqual((200, 200), (first.status, second.status))
        self.assertEqual(2, len(self.coordinator.requests))
        first_request, second_request = self.coordinator.requests
        self.assertIs(type(first_request.command), SubmitLearnerTurn)
        self.assertEqual("question-1", first_request.command.question_id)
        self.assertEqual("由 outbox 重试。", first_request.command.text)
        self.assertEqual(
            first_request.command.command_id,
            second_request.command.command_id,
        )
        self.assertEqual(
            first_request.command.learner_turn_id,
            second_request.command.learner_turn_id,
        )
        self.assertEqual(5, first_request.expected_conversation_version)
        self.assertEqual(3, first_request.context.registry_generation)

    def test_topic_pause_resume_and_recover_are_closed_actions(self) -> None:
        common = (
            ("Content-Type", "application/json"),
            ("If-Match", '"conversation-v5"'),
        )
        cases = (
            (
                {"action": "pause"},
                "pause-key",
                PauseTopic,
            ),
            (
                {"action": "switch"},
                "switch-key",
                SwitchTopic,
            ),
            (
                {"action": "resume", "topic_run_id": "topic-1"},
                "resume-key",
                ResumeTopic,
            ),
            (
                {
                    "action": "recover",
                    "dead_work_id": "work-dead",
                    "recovery": "retry",
                },
                "recover-key",
                RecoverWork,
            ),
        )
        for body, key, expected_type in cases:
            with self.subTest(body=body):
                base = state()
                if key == "resume-key":
                    self.coordinator.state = replace(
                        base,
                        phase=ConversationPhase.NONE,
                        active_topic=None,
                        paused_topics=(
                            replace(
                                base.active_topic,
                                lifecycle=TopicLifecycle.PAUSED,
                            ),
                        ),
                    )
                elif key == "recover-key":
                    dead = CurrentWorkState(
                        "work-dead",
                        "event-dead",
                        7,
                        TriggerBinding(
                            TriggerKind.TOPIC_CANDIDATES,
                            "work-dead",
                            "runtime-1",
                            None,
                            None,
                            digest("input"),
                            digest("evidence"),
                        ),
                        3,
                        WorkStatus.DEAD_LETTER,
                        allowed_recovery_actions=(WorkRecoveryAction.RETRY,),
                    )
                    self.coordinator.state = replace(
                        base,
                        phase=ConversationPhase.RECOVERABLE_ERROR,
                        active_topic=None,
                        session_work=dead,
                    )
                else:
                    self.coordinator.state = base
                response = self.api.handle(
                    request(
                        "POST",
                        "/api/v2/topic",
                        body=body,
                        headers=(*common, ("Idempotency-Key", key)),
                    )
                )
                self.assertEqual(200, response.status)
                self.assertIs(
                    type(self.coordinator.requests[-1].command),
                    expected_type,
                )

        self.coordinator.state = replace(
            state(),
            phase=ConversationPhase.CHOOSING_TOPIC,
            active_topic=None,
            candidates=("支付一致性", "Outbox"),
        )
        selected = self.api.handle(
            request(
                "POST",
                "/api/v2/topic",
                body={"action": "select", "candidate": "Outbox"},
                headers=(*common, ("Idempotency-Key", "select-key")),
            )
        )
        self.assertEqual(200, selected.status)
        selection_request = self.coordinator.requests[-1]
        self.assertIs(type(selection_request.command), SelectTopic)
        self.assertEqual("Outbox", selection_request.command.candidate)
        self.assertIs(
            TriggerKind.TOPIC_SELECTION,
            selection_request.context.trigger.kind,
        )

    def test_stream_is_authenticated_cursor_resumable_and_safe(self) -> None:
        self.stream.on_batch(
            CommittedBatch(
                StreamKind.DIALOGUE,
                "session-1",
                (
                    CommittedEventView(
                        "event-1",
                        1,
                        ImmutablePayloadView(
                            "agent_turn_committed",
                            (
                                ("question_id", "question-2"),
                                ("question", "下一步如何验证?"),
                                ("claim_id", "claim-secret"),
                            ),
                        ),
                    ),
                ),
            )
        )
        response = self.api.handle(
            request("GET", "/api/v2/stream?after=0")
        )
        self.assertEqual(200, response.status)
        self.assertEqual("text/event-stream", response.header("Content-Type"))
        rendered = response.body.decode()
        self.assertIn("id: 1", rendered)
        self.assertIn("event: agent_turn_committed", rendered)
        self.assertIn("question-2", rendered)
        self.assertNotIn("claim-secret", rendered)

        ahead = self.api.handle(
            request("GET", "/api/v2/stream?after=2")
        )
        self.assertEqual(409, ahead.status)
        self.assertEqual("CURSOR_AHEAD", json.loads(ahead.body)["error"]["code"])

    def test_live_stream_waits_for_after_commit_and_closes_cleanly(self) -> None:
        live = self.api.open_stream(
            request("GET", "/api/v2/stream?after=0")
        )
        self.assertEqual(b": keepalive\n\n", live.read(timeout=0))
        self.stream.on_batch(
            CommittedBatch(
                StreamKind.DIALOGUE,
                "session-1",
                (
                    CommittedEventView(
                        "event-live",
                        1,
                        ImmutablePayloadView(
                            "learner_turn_submitted",
                            (
                                ("question_id", "question-1"),
                                ("learner_turn_id", "turn-1"),
                                ("text", "由 outbox 重试。"),
                            ),
                        ),
                    ),
                ),
            )
        )
        chunk = live.read(timeout=0)
        self.assertIn(b"id: 1", chunk)
        self.assertIn("由 outbox 重试。".encode(), chunk)
        self.assertEqual(1, live.cursor)
        live.close()
        with self.assertRaisesRegex(PublicStreamError, "SUBSCRIPTION_CLOSED"):
            live.read(timeout=0)

        with self.assertRaisesRegex(BrowserApiError, "AUTH_REQUIRED"):
            self.api.open_stream(
                BrowserHttpRequest(
                    "GET",
                    "/api/v2/stream?after=0",
                    (("Host", "127.0.0.1:43123"),),
                    b"",
                )
            )

    def test_security_and_protocol_fail_closed(self) -> None:
        checks = (
            (
                BrowserHttpRequest(
                    "GET",
                    "/api/v2/state",
                    (
                        ("Authorization", "Bearer browser-secret"),
                        ("Host", "evil.invalid"),
                        ("Origin", "http://127.0.0.1:43123"),
                    ),
                    b"",
                ),
                403,
                "BAD_HOST",
            ),
            (
                BrowserHttpRequest(
                    "GET",
                    "/api/v2/state",
                    (
                        ("Authorization", "Bearer browser-secret"),
                        ("Host", "127.0.0.1:43123"),
                        ("Sec-Fetch-Site", "cross-site"),
                    ),
                    b"",
                ),
                403,
                "BAD_ORIGIN",
            ),
            (
                BrowserHttpRequest(
                    "POST",
                    "/api/v2/turns",
                    (
                        ("Authorization", "Bearer browser-secret"),
                        ("Host", "127.0.0.1:43123"),
                        ("Content-Type", "application/json"),
                        ("Idempotency-Key", "key"),
                        ("If-Match", '"conversation-v5"'),
                    ),
                    b"{}",
                ),
                403,
                "BAD_ORIGIN",
            ),
            (
                request(
                    "POST",
                    "/api/v2/turns",
                    body={"question_id": "question-1", "text": "answer"},
                    headers=(("Content-Type", "text/plain"),),
                ),
                400,
                "VALIDATION_FAILED",
            ),
        )
        for incoming, status, code in checks:
            with self.subTest(code=code):
                response = self.api.handle(incoming)
                self.assertEqual(status, response.status)
                self.assertEqual(code, json.loads(response.body)["error"]["code"])

    def test_stable_error_mapping_and_body_limit(self) -> None:
        self.coordinator.error_code = "CONVERSATION_VERSION_CONFLICT"
        response = self.api.handle(
            request(
                "POST",
                "/api/v2/turns",
                body={"question_id": "question-1", "text": "answer"},
                headers=(
                    ("Content-Type", "application/json"),
                    ("Idempotency-Key", "key"),
                    ("If-Match", '"conversation-v5"'),
                ),
            )
        )
        self.assertEqual(409, response.status)
        payload = json.loads(response.body)["error"]
        self.assertEqual("VERSION_CONFLICT", payload["code"])
        self.assertTrue(payload["retryable"])
        self.assertEqual("reload_state", payload["recovery"])

        huge = BrowserHttpRequest(
            "POST",
            "/api/v2/turns",
            (
                ("Authorization", "Bearer browser-secret"),
                ("Host", "127.0.0.1:43123"),
                ("Origin", "http://127.0.0.1:43123"),
                ("Content-Type", "application/json"),
                ("Idempotency-Key", "huge"),
                ("If-Match", '"conversation-v5"'),
            ),
            b"x" * (128 * 1024 + 1),
        )
        limited = self.api.handle(huge)
        self.assertEqual(413, limited.status)
        self.assertEqual(
            "PAYLOAD_TOO_LARGE", json.loads(limited.body)["error"]["code"]
        )

    def test_malformed_cursor_and_duplicate_headers_fail_closed(self) -> None:
        malformed = self.api.handle(
            request("GET", "/api/v2/stream?after=" + "9" * 100)
        )
        self.assertEqual(400, malformed.status)
        self.assertEqual(
            "VALIDATION_FAILED",
            json.loads(malformed.body)["error"]["code"],
        )

        duplicate = BrowserHttpRequest(
            "GET",
            "/api/v2/state",
            (
                ("Authorization", "Bearer browser-secret"),
                ("authorization", "Bearer browser-secret"),
                ("Host", "127.0.0.1:43123"),
                ("Origin", "http://127.0.0.1:43123"),
            ),
            b"",
        )
        rejected = self.api.handle(duplicate)
        self.assertEqual(400, rejected.status)
        self.assertEqual(
            "VALIDATION_FAILED",
            json.loads(rejected.body)["error"]["code"],
        )

    def test_request_and_response_boundaries_are_immutable(self) -> None:
        incoming = request("GET", "/api/v2/state")
        with self.assertRaises((AttributeError, TypeError)):
            incoming.method = "POST"
        response = self.api.handle(incoming)
        self.assertIsNone(response.header("Missing"))
        with self.assertRaises((AttributeError, TypeError)):
            response.status = 500

        with self.assertRaisesRegex(ValueError, "INVALID_BROWSER_HTTP_REQUEST"):
            BrowserHttpRequest("PUT", "/", (), b"")
        from xsync_v2.browser_http import BrowserHttpResponse

        with self.assertRaisesRegex(ValueError, "INVALID_BROWSER_HTTP_RESPONSE"):
            BrowserHttpResponse(99, (), b"")

    def test_get_fallback_origin_and_misc_protocol_edges(self) -> None:
        same_origin = BrowserHttpRequest(
            "GET",
            "/api/v2/state",
            (
                ("Authorization", "Bearer browser-secret"),
                ("Host", "127.0.0.1:43123"),
                ("Sec-Fetch-Site", "same-origin"),
            ),
            b"",
        )
        self.assertEqual(200, self.api.handle(same_origin).status)
        self.assertEqual(
            403,
            self.api.handle(
                BrowserHttpRequest(
                    "GET",
                    "/api/v2/state",
                    (
                        ("Authorization", "Bearer browser-secret"),
                        ("Host", "127.0.0.1:43123"),
                        ("Origin", "http://evil.invalid"),
                    ),
                    b"",
                )
            ).status,
        )
        cases = (
            request("GET", "/missing"),
            request("GET", "/api/v2/state?unexpected=1"),
            request("GET", "/api/v2/stream?after"),
            request("GET", "/api/v2/stream?after=0", body={"x": 1}),
            request("GET", "/api/v2/state#fragment"),
        )
        expected = (404, 400, 400, 400, 400)
        self.assertEqual(
            expected,
            tuple(self.api.handle(item).status for item in cases),
        )

        empty_stream = self.api.handle(
            request("GET", "/api/v2/stream?after=0")
        )
        self.assertEqual(b": keepalive\n\n", empty_stream.body)

    def test_mutation_json_and_header_edges_are_stable(self) -> None:
        base_headers = (
            ("Content-Type", "application/json"),
            ("Idempotency-Key", "edge-key"),
            ("If-Match", '"conversation-v5"'),
        )
        cases = (
            request(
                "POST",
                "/api/v2/turns",
                body={"question_id": "question-1", "text": "answer"},
                headers=base_headers[1:],
            ),
            request(
                "POST",
                "/api/v2/turns",
                body={"question_id": "question-1", "text": "answer"},
                headers=(base_headers[0], base_headers[2]),
            ),
            request(
                "POST",
                "/api/v2/turns",
                body={"question_id": "question-1", "text": "answer"},
                headers=(base_headers[0], base_headers[1]),
            ),
            BrowserHttpRequest(
                "POST",
                "/api/v2/turns",
                request("POST", "/").headers + base_headers,
                b"{",
            ),
            BrowserHttpRequest(
                "POST",
                "/api/v2/turns",
                request("POST", "/").headers + base_headers,
                b"[]",
            ),
            request(
                "POST",
                "/api/v2/turns?bad=1",
                body={"question_id": "question-1", "text": "answer"},
                headers=base_headers,
            ),
        )
        self.assertTrue(all(self.api.handle(item).status == 400 for item in cases))


class BrowserServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.coordinator = FakeCoordinator()
        self.service = BrowserCommandService(
            self.coordinator,
            lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            clock=lambda: "2026-08-16T12:05:00+08:00",
        )

    @staticmethod
    def command(intent, *, key: str = "direct-key", version: int = 5):
        return BrowserCommandRequest("session-1", key, version, intent)

    def test_configuration_current_and_clock_fail_closed(self) -> None:
        with self.assertRaisesRegex(
            BrowserServiceError,
            "INVALID_BROWSER_SERVICE_CONFIGURATION",
        ):
            BrowserCommandService(object(), lambda _config: None, clock=lambda: "x")

        with self.assertRaisesRegex(BrowserServiceError, "VALIDATION_FAILED"):
            self.service.current("../session")
        self.coordinator.error_code = "SESSION_DEACTIVATED"
        with self.assertRaisesRegex(BrowserServiceError, "SESSION_DEACTIVATED"):
            self.service.execute(self.command(PauseTopicIntent()))
        self.coordinator.error_code = None

        self.coordinator.recover = lambda: None
        with self.assertRaisesRegex(BrowserServiceError, "SESSION_DEACTIVATED"):
            self.service.current("session-1")

        clock_service = BrowserCommandService(
            FakeCoordinator(),
            lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            clock=lambda: "not-a-timestamp",
        )
        with self.assertRaisesRegex(BrowserServiceError, "INVALID_BROWSER_CLOCK"):
            clock_service.execute(self.command(PauseTopicIntent()))

    def test_request_and_evidence_validation_matrix(self) -> None:
        bad_requests = (
            object(),
            BrowserCommandRequest("../bad", "key", 5, PauseTopicIntent()),
            BrowserCommandRequest("session-1", "bad/key", 5, PauseTopicIntent()),
            BrowserCommandRequest("session-1", "key", True, PauseTopicIntent()),
            self.command(SubmitTurnIntent("bad/id", "answer")),
            self.command(SubmitTurnIntent("question-1", " answer")),
            self.command(ResumeTopicIntent("bad/id")),
            self.command(RecoverWorkIntent("bad/id", WorkRecoveryAction.RETRY)),
        )
        for bad in bad_requests:
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(BrowserServiceError, "VALIDATION_FAILED"):
                    self.service.execute(bad)

        failing = BrowserCommandService(
            FakeCoordinator(),
            lambda _config: (_ for _ in ()).throw(RuntimeError("secret")),
            clock=lambda: "2026-08-16T12:05:00+08:00",
        )
        with self.assertRaisesRegex(
            BrowserServiceError,
            "EVIDENCE_VERIFICATION_FAILED",
        ):
            failing.execute(self.command(PauseTopicIntent()))

        malformed = BrowserCommandService(
            FakeCoordinator(),
            lambda _config: EvidenceCheck(EvidenceHealth.CURRENT, digest("wrong")),
            clock=lambda: "2026-08-16T12:05:00+08:00",
        )
        with self.assertRaisesRegex(
            BrowserServiceError,
            "EVIDENCE_VERIFICATION_FAILED",
        ):
            malformed.execute(self.command(PauseTopicIntent()))

    def test_context_variants_are_deterministic_and_typed(self) -> None:
        base = state()
        self.service.execute(self.command(SwitchTopicIntent(), key="switch"))
        switch_request = self.coordinator.requests[-1]
        self.assertIs(type(switch_request.command), SwitchTopic)
        self.assertIs(
            TriggerKind.TOPIC_CANDIDATES,
            switch_request.context.trigger.kind,
        )
        self.assertIsNone(switch_request.context.trigger.contract_digest)

        paused_open = replace(
            base.active_topic,
            lifecycle=TopicLifecycle.PAUSED,
        )
        self.coordinator.state = replace(
            base,
            phase=ConversationPhase.NONE,
            active_topic=None,
            paused_topics=(paused_open,),
        )
        self.service.execute(self.command(ResumeTopicIntent("topic-1")))
        self.assertIsNone(self.coordinator.requests[-1].context.trigger)

        saved_trigger = TriggerBinding(
            TriggerKind.LEARNER_REPLY,
            "work-old",
            "runtime-old",
            "turn-1",
            digest("contract"),
            digest("input"),
            digest("evidence"),
        )
        paused_work = replace(
            paused_open,
            current_agent_turn=None,
            work=CurrentWorkState(
                "work-old",
                "event-old",
                7,
                saved_trigger,
                1,
                WorkStatus.SUPERSEDED,
            ),
        )
        self.coordinator.state = replace(
            base,
            phase=ConversationPhase.NONE,
            active_topic=None,
            paused_topics=(paused_work,),
        )
        self.service.execute(self.command(ResumeTopicIntent("topic-1"), key="resume"))
        rebuilt = self.coordinator.requests[-1].context.trigger
        self.assertIsNotNone(rebuilt)
        self.assertEqual(saved_trigger.input_digest, rebuilt.input_digest)
        self.assertNotEqual(saved_trigger.work_id, rebuilt.work_id)

        stale = replace(paused_open, evidence_health=EvidenceHealth.STALE)
        self.coordinator.state = replace(
            base,
            phase=ConversationPhase.NONE,
            active_topic=None,
            paused_topics=(stale,),
        )
        self.service.execute(self.command(ResumeTopicIntent("topic-1"), key="stale"))
        self.assertIs(
            TriggerKind.REGROUND,
            self.coordinator.requests[-1].context.trigger.kind,
        )

    def test_missing_targets_and_reground_recovery_are_explicit(self) -> None:
        no_topic = replace(state(), active_topic=None)
        self.coordinator.state = no_topic
        with self.assertRaisesRegex(BrowserServiceError, "TOPIC_STATE_CONFLICT"):
            self.service.execute(
                self.command(SubmitTurnIntent("question-1", "answer"))
            )
        with self.assertRaisesRegex(BrowserServiceError, "TOPIC_STATE_CONFLICT"):
            self.service.execute(self.command(ResumeTopicIntent("topic-missing")))
        with self.assertRaisesRegex(BrowserServiceError, "TOPIC_STATE_CONFLICT"):
            self.service.execute(
                self.command(
                    RecoverWorkIntent("work-missing", WorkRecoveryAction.RETRY)
                )
            )

        topic = state().active_topic
        dead_trigger = TriggerBinding(
            TriggerKind.LEARNER_REPLY,
            "work-dead",
            "runtime-1",
            "turn-1",
            digest("contract"),
            digest("input"),
            digest("evidence"),
        )
        dead = CurrentWorkState(
            "work-dead",
            "event-dead",
            7,
            dead_trigger,
            3,
            WorkStatus.DEAD_LETTER,
            allowed_recovery_actions=(
                WorkRecoveryAction.RETRY,
                WorkRecoveryAction.REGROUND,
            ),
        )
        self.coordinator.state = replace(
            state(),
            phase=ConversationPhase.RECOVERABLE_ERROR,
            active_topic=replace(topic, current_agent_turn=None, work=dead),
        )
        self.service.execute(
            self.command(
                RecoverWorkIntent("work-dead", WorkRecoveryAction.REGROUND),
                key="reground",
            )
        )
        trigger = self.coordinator.requests[-1].context.trigger
        self.assertIs(TriggerKind.REGROUND, trigger.kind)
        self.assertEqual("turn-1", trigger.parent_turn_id)


if __name__ == "__main__":
    unittest.main()
