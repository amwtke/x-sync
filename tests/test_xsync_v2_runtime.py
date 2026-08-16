from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import tests.xsync_v2_path  # noqa: F401
from xsync_v2.coordinator import DialogueSessionConfig
from xsync_v2.domain import EvidenceCheck, EvidenceHealth, Lens
from xsync_v2.event_codec import sha256_digest
from xsync_v2.host_context import EvidenceContextClaim, HostContextSource
from xsync_v2.runtime import DialogueRuntime, DialogueRuntimeError
from xsync_v2.secure_fs import SecureFsError


def digest(label: str) -> str:
    return sha256_digest(label.encode())


class RuntimeContextProvider:
    def load_context(self, work, _lease, _authority):
        return HostContextSource(
            topic_contract=None,
            task_scope="repository onboarding",
            current_lens=Lens.MIXED,
            gates=(),
            previous_question=None,
            learner_turn=None,
            learner_model=(),
            priority_gap="choose one grounded topic",
            evidence_claims=(
                EvidenceContextClaim(
                    "evidence-1",
                    "The repository contains dialogue code.",
                    "skills/x-sync/scripts/xsync_v2/runtime.py:1",
                    work.evidence_digest,
                ),
            ),
            through_event_sequence=work.observed_sequence,
        )


class DialogueRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.now = 1_000
        self.config = DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repository-1",
            "2026-08-16T15:00:00+08:00",
            "trigger-epoch-1",
            digest("evidence"),
        )

    def build_runtime(self, path: Path) -> DialogueRuntime:
        return DialogueRuntime(
            path,
            "registry-1",
            "runtime-1",
            evidence_verifier=lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            context_provider=RuntimeContextProvider(),
            runtime_authority_verifier=lambda _check, _authority: True,
            lease_clock=lambda: self.now,
            browser_clock=lambda: "2026-08-16T15:00:00+08:00",
            monotonic_clock=lambda: float(self.now),
            durable_poll_interval=0.01,
        )

    def open_runtime(self) -> DialogueRuntime:
        runtime = self.build_runtime(self.root)
        self.addCleanup(runtime.close)
        return runtime

    def test_one_graph_delivers_session_work_to_host_and_browser(self) -> None:
        runtime = self.open_runtime()
        resolution = runtime.resolve(self.config)

        current = runtime.host.current(self.config.session_id)
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual("session-1", current.session_id)
        self.assertEqual(
            resolution.dialogue_state.sequence,
            current.observed_sequence,
        )

        subscription = runtime.public_stream.subscribe("session-1", 0)
        self.addCleanup(subscription.close)
        events = subscription.read_available()
        self.assertEqual(
            ("session_started",),
            tuple(item.event_type for item in events),
        )
        self.assertEqual((1,), tuple(item.sequence for item in events))

    def test_reopen_replays_the_same_durable_graph(self) -> None:
        first = self.open_runtime()
        original = first.resolve(self.config)
        first.close()

        second = self.open_runtime()
        recovered = second.recover()
        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(original.dialogue_state, recovered.dialogue_state)
        self.assertIsNotNone(second.host.current("session-1"))
        events = second.public_stream.subscribe("session-1", 0).read_available()
        self.assertEqual((1,), tuple(item.sequence for item in events))

    def test_browser_server_uses_the_runtime_browser_and_stream(self) -> None:
        runtime = self.open_runtime()
        runtime.resolve(self.config)

        server = runtime.start_browser(
            "session-1",
            "browser-capability",
            keepalive_seconds=0.05,
        )
        self.assertEqual("127.0.0.1", server.address.host)
        self.assertTrue(server.launch_url.endswith("/#browser-capability"))
        with self.assertRaisesRegex(
            DialogueRuntimeError,
            "BROWSER_ALREADY_RUNNING",
        ):
            runtime.start_browser("session-1", "another-capability")

        runtime.close_browser()
        restarted = runtime.start_browser(
            "session-1",
            "replacement-capability",
            keepalive_seconds=0.05,
        )
        self.assertTrue(restarted.launch_url.endswith("/#replacement-capability"))

    def test_close_is_idempotent_and_all_product_entries_fail_closed(self) -> None:
        runtime = self.open_runtime()
        runtime.close()
        runtime.close()

        for operation in (
            lambda: runtime.resolve(self.config),
            runtime.recover,
            runtime.replay_committed,
            lambda: runtime.start_browser("session-1", "capability"),
            lambda: runtime.host,
            lambda: runtime.browser,
            lambda: runtime.public_stream,
        ):
            with self.subTest(operation=operation), self.assertRaisesRegex(
                DialogueRuntimeError,
                "RUNTIME_CLOSED",
            ):
                operation()

    def test_state_root_must_be_an_existing_absolute_private_directory(self) -> None:
        with self.assertRaisesRegex(
            DialogueRuntimeError,
            "INVALID_STATE_DIRECTORY",
        ):
            self.build_runtime(Path("relative-state"))
        with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
            self.build_runtime(self.root / "missing")


if __name__ == "__main__":
    unittest.main()
