from __future__ import annotations

import errno
import json
import os
import socket
import stat
import struct
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

from xsync_v2 import host_ipc
from xsync_v2.coordinator import DialogueSessionConfig
from xsync_v2.domain import EvidenceCheck, EvidenceHealth, Lens
from xsync_v2.event_codec import PROTOCOL_VERSION, SCHEMA_VERSION, sha256_digest
from xsync_v2.host_api import MAX_HOST_API_REQUEST_BYTES
from xsync_v2.host_context import EvidenceContextClaim, HostContextSource
from xsync_v2.host_ipc import (
    MAX_HOST_IPC_RESPONSE_BYTES,
    HostIpcClient,
    HostIpcError,
    HostIpcServer,
)
from xsync_v2.runtime import DialogueRuntime, DialogueRuntimeError


def digest(label: str) -> str:
    return sha256_digest(label.encode())


class IpcContextProvider:
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
                    "The repository uses durable dialogue events.",
                    "architecture.md:1",
                    digest("evidence-entry"),
                ),
            ),
            through_event_sequence=work.observed_sequence,
        )


class HostIpcTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.control = self.root / "control"
        self.control.mkdir(mode=0o700)
        self.socket_path = self.control / "host.sock"
        self.now = 1_000
        self.runtime = DialogueRuntime(
            self.root,
            "registry-1",
            "runtime-1",
            evidence_verifier=lambda config: EvidenceCheck(
                EvidenceHealth.CURRENT,
                config.evidence_digest,
            ),
            context_provider=IpcContextProvider(),
            runtime_authority_verifier=lambda check, _authority: (
                check.runtime_epoch == "runtime-1"
                and check.owner_id == "owner-1"
            ),
            lease_clock=lambda: self.now,
            browser_clock=lambda: "2026-08-16T18:00:00+08:00",
            monotonic_clock=lambda: float(self.now),
            durable_poll_interval=0.01,
        )
        self.addCleanup(self.runtime.close)
        self.config = DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repository-1",
            "2026-08-16T18:00:00+08:00",
            "runtime-1",
            digest("evidence"),
        )
        self.runtime.resolve(self.config)

    @staticmethod
    def body(operation: str, **payload: object) -> bytes:
        return json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "protocol_version": PROTOCOL_VERSION,
                "operation": operation,
                **payload,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()

    def call(self, operation: str, **payload: object) -> dict[str, object]:
        response = HostIpcClient(self.socket_path).call(
            self.body(operation, **payload)
        )
        return json.loads(response.body)

    def test_private_ipc_runs_wait_claim_publish_end_to_end(self) -> None:
        server = self.runtime.start_host_ipc(self.socket_path)
        self.addCleanup(server.close)

        metadata = os.stat(self.socket_path, follow_symlinks=False)
        self.assertTrue(stat.S_ISSOCK(metadata.st_mode))
        self.assertEqual(os.geteuid(), metadata.st_uid)
        self.assertEqual(0o600, stat.S_IMODE(metadata.st_mode))

        waited = self.call("wait", session_id="session-1", timeout=0)
        self.assertTrue(waited["ok"])
        work_id = waited["payload"]["work"]["work_id"]
        claimed = self.call(
            "claim",
            session_id="session-1",
            request_id="claim-request-1",
            claim_id="claim-1",
            work_id=work_id,
            owner_id="owner-1",
            lease_seconds=30,
            max_tenure_seconds=120,
        )
        self.assertTrue(claimed["ok"])
        payload = claimed["payload"]

        published = self.call(
            "publish",
            idempotency_key="publish-candidates-1",
            work=payload["work"],
            fence=payload["fence"],
            result={
                "type": "topic_candidates",
                "candidates": ["Registry fencing", "Lease recovery"],
            },
            occurred_at=self.config.created_at,
            actor_id="host.adapter-1",
        )

        self.assertTrue(published["ok"])
        self.assertEqual(
            "choosing_topic",
            self.runtime.browser.current("session-1").dialogue_state.phase.value,
        )

    def test_server_rejects_unsafe_paths_and_duplicate_binding(self) -> None:
        server = HostIpcServer(self.runtime.host_api, self.socket_path)
        server.start()
        self.addCleanup(server.close)
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_ADDRESS_IN_USE"):
            HostIpcServer(self.runtime.host_api, self.socket_path).start()

        shared = self.root / "shared"
        shared.mkdir(mode=0o755)
        shared.chmod(0o755)
        invalid = (
            Path("relative.sock"),
            shared / "host.sock",
            self.root / "missing" / "host.sock",
        )
        for path in invalid:
            with self.subTest(path=path), self.assertRaises(HostIpcError):
                HostIpcServer(self.runtime.host_api, path).start()

        link = self.root / "control-link"
        link.symlink_to(self.control, target_is_directory=True)
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_UNSAFE_PATH"):
            HostIpcServer(self.runtime.host_api, link / "other.sock").start()

    def test_client_rejects_oversize_and_truncated_frames(self) -> None:
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_REQUEST_INVALID"):
            HostIpcClient(self.socket_path).call(
                b"x" * (MAX_HOST_API_REQUEST_BYTES + 1)
            )
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_REQUEST_INVALID"):
            HostIpcClient(self.socket_path).call(None)  # type: ignore[arg-type]

        ready = threading.Event()

        def serve_truncated() -> None:
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                listener.bind(str(self.socket_path))
                os.chmod(self.socket_path, 0o600)
                listener.listen(1)
                ready.set()
                connection, _ = listener.accept()
                with connection:
                    connection.recv(1024)
                    connection.sendall(struct.pack("!I", 8) + b"{}")
            finally:
                listener.close()

        thread = threading.Thread(target=serve_truncated, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(2))
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_RESPONSE_INVALID"):
            HostIpcClient(self.socket_path).call(b"{}")
        thread.join(2)

    def test_close_never_unlinks_a_replacement_path(self) -> None:
        server = HostIpcServer(self.runtime.host_api, self.socket_path)
        server.start()
        os.unlink(self.socket_path)
        self.socket_path.write_bytes(b"replacement")
        self.socket_path.chmod(0o600)

        server.close()

        self.assertEqual(b"replacement", self.socket_path.read_bytes())

    def test_runtime_owns_exactly_one_restartable_ipc_transport(self) -> None:
        first = self.runtime.start_host_ipc(self.socket_path)
        self.assertIs(type(first), HostIpcServer)
        with self.assertRaisesRegex(DialogueRuntimeError, "HOST_IPC_ALREADY_RUNNING"):
            self.runtime.start_host_ipc(self.socket_path)

        self.runtime.close_host_ipc()
        second = self.runtime.start_host_ipc(self.socket_path)
        self.assertIsNot(first, second)
        self.runtime.close()

        self.assertFalse(self.socket_path.exists())
        with self.assertRaisesRegex(DialogueRuntimeError, "RUNTIME_CLOSED"):
            self.runtime.start_host_ipc(self.socket_path)

    def test_malformed_host_request_stays_a_canonical_api_response(self) -> None:
        server = HostIpcServer(self.runtime.host_api, self.socket_path)
        server.start()
        self.addCleanup(server.close)

        response = json.loads(HostIpcClient(self.socket_path).call(b"{}").body)

        self.assertFalse(response["ok"])
        self.assertEqual("HOST_API_OPERATION_UNSUPPORTED", response["error"]["code"])
        self.assertLess(len(json.dumps(response)), MAX_HOST_IPC_RESPONSE_BYTES)

    def test_configuration_path_and_lifecycle_boundaries_are_stable(self) -> None:
        invalid_paths = (
            object(),
            b"/tmp/bytes.sock",
            Path("relative.sock"),
            "/tmp//double.sock",
            "/tmp/../escape.sock",
            "/tmp/line\nbreak.sock",
            Path("/tmp") / ("x" * 101),
            "/tmp/\ud800.sock",
        )
        for path in invalid_paths:
            with self.subTest(path=path), self.assertRaisesRegex(
                HostIpcError,
                "HOST_IPC_UNSAFE_PATH",
            ):
                HostIpcClient(path)  # type: ignore[arg-type]

        for value in (None, True, 0, -1, float("inf"), float("nan"), 10**10000):
            with self.subTest(timeout=value), self.assertRaisesRegex(
                HostIpcError,
                "HOST_IPC_CONFIGURATION_INVALID",
            ):
                HostIpcClient(self.socket_path, timeout=value)  # type: ignore[arg-type]

        for value in (False, 0, 65):
            with self.subTest(max_clients=value), self.assertRaisesRegex(
                HostIpcError,
                "HOST_IPC_CONFIGURATION_INVALID",
            ):
                HostIpcServer(
                    self.runtime.host_api,
                    self.socket_path,
                    max_clients=value,
                )
        with self.assertRaisesRegex(
            HostIpcError,
            "HOST_IPC_CONFIGURATION_INVALID",
        ):
            HostIpcServer(object(), self.socket_path)  # type: ignore[arg-type]

        closed = HostIpcServer(self.runtime.host_api, self.socket_path)
        self.assertEqual(str(self.socket_path), closed.path)
        self.assertIsNone(closed.fatal_error)
        closed.close()
        closed.close()
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_CLOSED"):
            closed.start()

        with HostIpcServer(self.runtime.host_api, self.socket_path) as running:
            self.assertEqual(str(self.socket_path), running.path)
            with self.assertRaisesRegex(HostIpcError, "HOST_IPC_ALREADY_RUNNING"):
                running.start()
        self.assertFalse(self.socket_path.exists())

    def test_frame_codec_rejects_empty_truncated_oversize_and_io_failures(self) -> None:
        sender, receiver = socket.socketpair()
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)

        sender.sendall(struct.pack("!I", 0))
        self.assertEqual(
            b"",
            host_ipc._receive_frame(receiver, maximum=8, code="BAD_FRAME"),
        )
        sender.sendall(struct.pack("!I", 9))
        with self.assertRaisesRegex(HostIpcError, "BAD_FRAME"):
            host_ipc._receive_frame(receiver, maximum=8, code="BAD_FRAME")

        sender.close()
        with self.assertRaisesRegex(HostIpcError, "BAD_FRAME"):
            host_ipc._receive_frame(receiver, maximum=8, code="BAD_FRAME")
        with self.assertRaisesRegex(HostIpcError, "BAD_FRAME"):
            host_ipc._receive_exact(receiver, 1, "BAD_FRAME")
        with self.assertRaisesRegex(HostIpcError, "BAD_FRAME"):
            host_ipc._send_frame(receiver, object(), "BAD_FRAME")  # type: ignore[arg-type]
        with self.assertRaisesRegex(HostIpcError, "BAD_FRAME"):
            host_ipc._send_frame(
                receiver,
                b"x" * (MAX_HOST_IPC_RESPONSE_BYTES + 1),
                "BAD_FRAME",
            )
        receiver.close()
        with self.assertRaisesRegex(HostIpcError, "BAD_FRAME"):
            host_ipc._send_frame(receiver, b"x", "BAD_FRAME")

    def test_failed_start_cleans_its_owned_socket_and_can_retry(self) -> None:
        server = HostIpcServer(self.runtime.host_api, self.socket_path)
        with mock.patch.object(
            host_ipc,
            "_socket_identity",
            side_effect=HostIpcError("INJECTED_IDENTITY_FAILURE"),
        ), self.assertRaisesRegex(HostIpcError, "INJECTED_IDENTITY_FAILURE"):
            server.start()

        self.assertFalse(self.socket_path.exists())
        replacement = HostIpcServer(self.runtime.host_api, self.socket_path)
        replacement.start()
        replacement.close()

    def test_capacity_and_invalid_handler_results_are_failure_isolated(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        original_handle = type(self.runtime.host_api).handle

        def blocked_handle(api, raw):
            entered.set()
            self.assertTrue(release.wait(2))
            return original_handle(api, raw)

        with mock.patch.object(
            type(self.runtime.host_api),
            "handle",
            new=blocked_handle,
        ):
            server = HostIpcServer(
                self.runtime.host_api,
                self.socket_path,
                max_clients=1,
            ).start()
            first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            first.settimeout(2)
            first.connect(str(self.socket_path))
            first.sendall(struct.pack("!I", 2) + b"{}")
            self.assertTrue(entered.wait(2))
            with self.assertRaisesRegex(
                HostIpcError,
                "HOST_IPC_RESPONSE_INVALID",
            ):
                HostIpcClient(self.socket_path).call(b"{}")
            release.set()
            self.assertTrue(first.recv(4))
            first.close()
            server.close()

        with mock.patch.object(
            type(self.runtime.host_api),
            "handle",
            return_value=object(),
        ):
            server = HostIpcServer(self.runtime.host_api, self.socket_path).start()
            with self.assertRaisesRegex(
                HostIpcError,
                "HOST_IPC_RESPONSE_INVALID",
            ):
                HostIpcClient(self.socket_path).call(b"{}")
            server.close()

    def test_missing_insecure_and_unlistened_addresses_fail_closed(self) -> None:
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_ADDRESS_UNAVAILABLE"):
            HostIpcClient(self.socket_path).call(b"{}")

        self.socket_path.write_bytes(b"not-a-socket")
        self.socket_path.chmod(0o600)
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_ADDRESS_UNSAFE"):
            HostIpcClient(self.socket_path).call(b"{}")
        self.socket_path.unlink()

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        try:
            with self.assertRaisesRegex(HostIpcError, "HOST_IPC_CONNECT_FAILED"):
                HostIpcClient(self.socket_path).call(b"{}")
        finally:
            listener.close()

    def test_identity_checks_and_start_faults_never_publish_a_bad_address(
        self,
    ) -> None:
        location = host_ipc._location(self.socket_path)
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_UNSAFE_PATH"):
            host_ipc._verify_parent(
                location,
                host_ipc.SecureDirectoryIdentity(0, 0),
            )
        with mock.patch.object(
            host_ipc.os,
            "stat",
            side_effect=OSError(errno.EIO, "injected"),
        ), self.assertRaisesRegex(HostIpcError, "HOST_IPC_UNSAFE_PATH"):
            host_ipc._verify_parent(
                location,
                host_ipc.SecureDirectoryIdentity(0, 0),
            )
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_NOT_RUNNING"):
            HostIpcServer(
                self.runtime.host_api,
                self.socket_path,
            )._verify_live_address()

        with mock.patch.object(
            host_ipc.socket,
            "socket",
            side_effect=OSError(errno.EIO, "injected"),
        ), self.assertRaisesRegex(HostIpcError, "HOST_IPC_BIND_FAILED"):
            HostIpcServer(self.runtime.host_api, self.socket_path).start()
        self.assertFalse(self.socket_path.exists())

        with mock.patch.object(
            host_ipc.os,
            "chmod",
            side_effect=OSError(errno.EIO, "injected"),
        ), self.assertRaisesRegex(HostIpcError, "HOST_IPC_BIND_FAILED"):
            HostIpcServer(self.runtime.host_api, self.socket_path).start()
        self.assertFalse(self.socket_path.exists())

        with mock.patch.object(
            host_ipc.threading.Thread,
            "start",
            side_effect=RuntimeError("thread-start-failed"),
        ), self.assertRaisesRegex(HostIpcError, "HOST_IPC_START_FAILED"):
            HostIpcServer(self.runtime.host_api, self.socket_path).start()
        self.assertFalse(self.socket_path.exists())

        server = HostIpcServer(self.runtime.host_api, self.socket_path).start()
        self.addCleanup(server.close)
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_ADDRESS_CHANGED"):
            host_ipc._verify_socket(
                location,
                host_ipc._SocketIdentity(0, 0),
            )
        server._record_fatal("INJECTED_FATAL")
        server._record_fatal("LATER_FATAL")
        self.assertEqual("INJECTED_FATAL", server.fatal_error)

    def test_unsupported_transport_and_prebind_stat_errors_are_stable(self) -> None:
        with mock.patch.object(host_ipc, "socket", object()):
            with self.assertRaisesRegex(HostIpcError, "HOST_IPC_UNSUPPORTED"):
                HostIpcServer(self.runtime.host_api, self.socket_path).start()
            with self.assertRaisesRegex(HostIpcError, "HOST_IPC_UNSUPPORTED"):
                HostIpcClient(self.socket_path).call(b"{}")

        real_stat = os.stat

        def fail_target(path, *args, **kwargs):
            if os.fspath(path) == str(self.socket_path):
                raise OSError(errno.EACCES, "injected")
            return real_stat(path, *args, **kwargs)

        with mock.patch.object(
            host_ipc.os,
            "stat",
            side_effect=fail_target,
        ), self.assertRaisesRegex(HostIpcError, "HOST_IPC_UNSAFE_PATH"):
            HostIpcServer(self.runtime.host_api, self.socket_path).start()

    def test_close_interrupts_active_clients_and_preserves_failure_isolation(
        self,
    ) -> None:
        entered = threading.Event()
        release = threading.Event()

        def blocked_receive(*_args, **_kwargs):
            entered.set()
            self.assertTrue(release.wait(2))
            return b"{}"

        with mock.patch.object(host_ipc, "_receive_frame", new=blocked_receive):
            server = HostIpcServer(self.runtime.host_api, self.socket_path).start()
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(str(self.socket_path))
            self.assertTrue(entered.wait(2))
            closer = threading.Thread(target=server.close)
            closer.start()
            release.set()
            closer.join(2)
            self.assertFalse(closer.is_alive())
            client.close()

    def test_client_rejects_trailing_response_bytes(self) -> None:
        ready = threading.Event()

        def serve_extra() -> None:
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                listener.bind(str(self.socket_path))
                os.chmod(self.socket_path, 0o600)
                listener.listen(1)
                ready.set()
                connection, _ = listener.accept()
                with connection:
                    connection.recv(1024)
                    connection.sendall(struct.pack("!I", 2) + b"{}x")
            finally:
                listener.close()

        thread = threading.Thread(target=serve_extra, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(2))
        with self.assertRaisesRegex(HostIpcError, "HOST_IPC_RESPONSE_INVALID"):
            HostIpcClient(self.socket_path).call(b"{}")
        thread.join(2)

    def test_client_rejects_a_response_that_never_reaches_eof(self) -> None:
        ready = threading.Event()
        release = threading.Event()

        def serve_without_eof() -> None:
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                listener.bind(str(self.socket_path))
                os.chmod(self.socket_path, 0o600)
                listener.listen(1)
                ready.set()
                connection, _ = listener.accept()
                with connection:
                    connection.recv(1024)
                    connection.sendall(struct.pack("!I", 2) + b"{}")
                    release.wait(2)
            finally:
                listener.close()

        thread = threading.Thread(target=serve_without_eof, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(2))
        try:
            with self.assertRaisesRegex(
                HostIpcError,
                "HOST_IPC_RESPONSE_INVALID",
            ):
                HostIpcClient(self.socket_path, timeout=0.05).call(b"{}")
        finally:
            release.set()
            thread.join(2)

    def test_live_address_replacement_stops_accepting_and_preserves_replacement(
        self,
    ) -> None:
        server = HostIpcServer(self.runtime.host_api, self.socket_path).start()
        os.unlink(self.socket_path)
        self.socket_path.write_bytes(b"replacement")
        self.socket_path.chmod(0o600)

        for _ in range(20):
            if server.fatal_error is not None:
                break
            threading.Event().wait(0.05)

        self.assertEqual("HOST_IPC_ADDRESS_UNSAFE", server.fatal_error)
        server.close()
        self.assertEqual(b"replacement", self.socket_path.read_bytes())


if __name__ == "__main__":
    unittest.main()
