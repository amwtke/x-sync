# ruff: noqa: I001
from __future__ import annotations

from io import BytesIO, StringIO
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.coordinator import DialogueSessionConfig
from xsync_v2 import runtime_cli
from xsync_v2.evidence import (
    EvidenceClaimType,
    EvidenceKind,
    EvidenceSource,
)
from xsync_v2.runtime import DialogueRuntime
from xsync_v2.runtime_cli import RuntimeCliError, main


class RuntimeCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name).resolve()
        self.repo = root / "repo"
        self.repo.mkdir(mode=0o700)
        subprocess.run(
            ["git", "init", "-q", str(self.repo)],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "-C", str(self.repo), "config", "user.email", "test@example.com"],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(self.repo), "config", "user.name", "Test"],
            check=True,
        )
        (self.repo / "architecture.md").write_text(
            "The runtime uses durable state machines.\n",
            encoding="utf-8",
        )
        subprocess.run(
            ["git", "-C", str(self.repo), "add", "architecture.md"],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(self.repo), "commit", "-qm", "initial"],
            check=True,
        )
        self.state = root / "dialogue-state"
        self.state.mkdir(mode=0o700)
        self.control = root / "control"
        self.control.mkdir(mode=0o700)
        self.socket_path = self.control / "host.sock"
        with DialogueRuntime(
            self.state,
            "registry-1",
            "runtime-bootstrap",
            repository_directory=self.repo,
            repository_id="repository-1",
            lease_clock=lambda: 1_000,
            browser_clock=lambda: "2026-08-16T22:00:00+08:00",
            monotonic_clock=lambda: 1_000.0,
        ) as runtime:
            snapshot = runtime.evidence.capture(
                "session-1",
                (
                    EvidenceSource(
                        "evidence-1",
                        EvidenceKind.SPEC,
                        EvidenceClaimType.IMPLEMENTATION,
                        "The runtime uses durable state machines.",
                        "architecture.md",
                    ),
                ),
                captured_at="2026-08-16T22:00:00+08:00",
            )
            runtime.resolve(
                DialogueSessionConfig(
                    "session-1",
                    "learner-1",
                    "repository-1",
                    "2026-08-16T22:00:00+08:00",
                    "runtime-bootstrap",
                    snapshot.snapshot_digest,
                )
            )

    def run_cli(self, *extra: str):
        output = BytesIO()
        errors = StringIO()
        seen_server = []

        def stop(server) -> str:
            seen_server.append(server.path)
            return "test-complete"

        status = main(
            [
                "serve",
                "--state",
                str(self.state),
                "--repo",
                str(self.repo),
                "--repository-id",
                "repository-1",
                "--registry",
                "registry-1",
                "--host-socket",
                str(self.socket_path),
                *extra,
            ],
            stdout=output,
            stderr=errors,
            wait_for_shutdown=stop,
        )
        return status, output.getvalue().splitlines(), errors.getvalue(), seen_server

    def test_serve_recovers_and_owns_both_private_transports(self) -> None:
        status, lines, errors, seen_server = self.run_cli("--stream-json")
        events = [json.loads(line) for line in lines]

        self.assertEqual(0, status)
        self.assertEqual("", errors)
        self.assertEqual(["ready", "closed"], [item["type"] for item in events])
        self.assertEqual([1, 2], [item["stream_sequence"] for item in events])
        self.assertEqual("session-1", events[0]["session_id"])
        self.assertEqual(str(self.socket_path), events[0]["host_socket"])
        self.assertTrue(events[0]["browser_url"].startswith("http://127.0.0.1:"))
        self.assertIn("/#browser.", events[0]["browser_url"])
        self.assertEqual([str(self.socket_path)], seen_server)
        self.assertFalse(self.socket_path.exists())

    def test_stream_mode_and_active_dialogue_fail_closed(self) -> None:
        missing_stream = self.run_cli()
        self.assertEqual(2, missing_stream[0])
        self.assertEqual([], missing_stream[1])
        self.assertEqual("RUNTIME_CLI_STREAM_JSON_REQUIRED\n", missing_stream[2])

        empty_state = self.state.parent / "empty-state"
        empty_state.mkdir(mode=0o700)
        output = BytesIO()
        errors = StringIO()
        status = main(
            [
                "serve",
                "--state",
                str(empty_state),
                "--repo",
                str(self.repo),
                "--repository-id",
                "repository-1",
                "--registry",
                "registry-empty",
                "--host-socket",
                str(self.socket_path),
                "--stream-json",
            ],
            stdout=output,
            stderr=errors,
            wait_for_shutdown=lambda _server: "unused",
        )
        self.assertEqual(2, status)
        self.assertEqual(b"", output.getvalue())
        self.assertEqual("NO_ACTIVE_DIALOGUE\n", errors.getvalue())

    def test_local_clock_signal_output_and_error_boundaries_are_stable(self) -> None:
        self.assertRegex(runtime_cli._runtime_epoch(), r"\Aruntime\.[0-9a-f]{32}\Z")
        self.assertRegex(
            runtime_cli._browser_capability(),
            r"\Abrowser\.[0-9a-f]{64}\Z",
        )
        self.assertIn("+00:00", runtime_cli._wall_clock())
        self.assertGreaterEqual(runtime_cli._lease_clock(), 0)

        class BrokenOutput(BytesIO):
            def write(self, _value):
                raise OSError("private detail")

        with self.assertRaisesRegex(RuntimeCliError, "RUNTIME_CLI_OUTPUT_FAILED"):
            runtime_cli._emit(BrokenOutput(), {"type": "ready"})

        self.assertEqual(
            2,
            main([], stdout=BytesIO(), stderr=StringIO()),
        )
        self.assertEqual(
            2,
            main(
                ["serve"],
                stdout=object(),  # type: ignore[arg-type]
                stderr=StringIO(),
            ),
        )

        class FakeServer:
            fatal_error = "HOST_IPC_ACCEPT_FAILED"

        class FakeEvent:
            def wait(self, _timeout):
                return False

            def set(self):
                return None

        with (
            mock.patch.object(runtime_cli.threading, "Event", return_value=FakeEvent()),
            mock.patch.object(
                runtime_cli.signal,
                "signal",
                side_effect=(None, None, None, None),
            ) as signals,
            self.assertRaisesRegex(RuntimeCliError, "HOST_IPC_ACCEPT_FAILED"),
        ):
            runtime_cli._wait_for_shutdown(FakeServer())  # type: ignore[arg-type]
        self.assertEqual(4, signals.call_count)

        class BadCodeError(Exception):
            code = 7

        errors = StringIO()
        with mock.patch.object(runtime_cli, "_serve", side_effect=BadCodeError()):
            status = main(
                [
                    "serve",
                    "--state",
                    str(self.state),
                    "--repo",
                    str(self.repo),
                    "--repository-id",
                    "repository-1",
                    "--registry",
                    "registry-1",
                    "--host-socket",
                    str(self.socket_path),
                    "--stream-json",
                ],
                stdout=BytesIO(),
                stderr=errors,
            )
        self.assertEqual(2, status)
        self.assertEqual("RUNTIME_CLI_FAILED\n", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
