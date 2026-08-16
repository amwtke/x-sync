# ruff: noqa: I001
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import unittest
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

from xsync_v2 import host_cli
from xsync_v2.coordinator import DialogueSessionConfig
from xsync_v2.domain import EvidenceCheck, EvidenceHealth, Lens
from xsync_v2.event_codec import PROTOCOL_VERSION, SCHEMA_VERSION, sha256_digest
from xsync_v2.host_api import HostApiResponse
from xsync_v2.host_cli import HostCliError, main
from xsync_v2.host_context import EvidenceContextClaim, HostContextSource
from xsync_v2.runtime import DialogueRuntime


def digest(label: str) -> str:
    return sha256_digest(label.encode())


class CliContextProvider:
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


class HostCliTest(unittest.TestCase):
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
            context_provider=CliContextProvider(),
            runtime_authority_verifier=lambda check, _authority: (
                check.runtime_epoch == "runtime-1"
                and check.owner_id in {"owner-1", "owner-2"}
            ),
            lease_clock=lambda: self.now,
            browser_clock=lambda: "2026-08-16T20:00:00+08:00",
            monotonic_clock=lambda: float(self.now),
            durable_poll_interval=0.01,
        )
        self.addCleanup(self.runtime.close)
        self.config = DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repository-1",
            "2026-08-16T20:00:00+08:00",
            "runtime-1",
            digest("evidence"),
        )
        self.runtime.resolve(self.config)
        self.runtime.start_host_ipc(self.socket_path)

    def run_cli(self, *arguments: str):
        stdout = BytesIO()
        stderr = StringIO()
        status = main(list(arguments), stdout=stdout, stderr=stderr)
        lines = stdout.getvalue().splitlines()
        response = None if not lines else json.loads(lines[0])
        return status, lines, response, stderr.getvalue()

    def claim(self):
        waited = self.run_cli(
            "wait",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--timeout",
            "0",
            "--json",
        )
        self.assertEqual(0, waited[0])
        work_id = waited[2]["payload"]["work"]["work_id"]
        claimed = self.run_cli(
            "claim",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--request-id",
            "claim-request-1",
            "--claim",
            "claim-1",
            "--work",
            work_id,
            "--owner",
            "owner-2",
            "--lease-seconds",
            "30",
            "--max-tenure-seconds",
            "120",
            "--json",
        )
        self.assertEqual(0, claimed[0])
        return claimed

    def test_wait_claim_renew_and_publish_are_one_envelope_operations(self) -> None:
        claimed = self.claim()
        self.assertEqual(1, len(claimed[1]))
        self.assertEqual("", claimed[3])
        payload = claimed[2]["payload"]

        self.now += 5
        renewed = self.run_cli(
            "renew",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--request-id",
            "renew-request-1",
            "--claim",
            payload["lease"]["claim_id"],
            "--work",
            payload["lease"]["work_id"],
            "--owner",
            payload["lease"]["owner_id"],
            "--lease-version",
            str(payload["lease"]["lease_version"]),
            "--lease-seconds",
            "30",
            "--json",
        )
        self.assertEqual(0, renewed[0])
        self.assertEqual(2, renewed[2]["payload"]["lease"]["lease_version"])

        claim_file = self.root / "claim.json"
        claim_file.write_bytes(claimed[1][0])
        result_file = self.root / "result.json"
        result_file.write_text(
            json.dumps(
                {
                    "type": "topic_candidates",
                    "candidates": ["Registry fencing", "Lease recovery"],
                }
            ),
            encoding="utf-8",
        )
        published = self.run_cli(
            "publish",
            "--socket",
            str(self.socket_path),
            "--idempotency-key",
            "publish-candidates-1",
            "--claim-envelope",
            str(claim_file),
            "--lease-version",
            str(renewed[2]["payload"]["lease"]["lease_version"]),
            "--file",
            str(result_file),
            "--occurred-at",
            self.config.created_at,
            "--actor-id",
            "host.adapter-1",
            "--json",
        )

        self.assertEqual(0, published[0])
        self.assertEqual(1, len(published[1]))
        self.assertEqual("", published[3])
        self.assertEqual(
            "choosing_topic",
            self.runtime.browser.current("session-1").dialogue_state.phase.value,
        )

    def test_reclaim_is_the_shared_crash_recovery_primitive(self) -> None:
        claimed = self.claim()
        lease = claimed[2]["payload"]["lease"]
        self.now = lease["expires_at"]

        reclaimed = self.run_cli(
            "reclaim",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--request-id",
            "reclaim-request-1",
            "--claim",
            "claim-reclaimed-1",
            "--work",
            lease["work_id"],
            "--owner",
            "owner-1",
            "--work-attempt",
            "1",
            "--lease-seconds",
            "30",
            "--max-tenure-seconds",
            "120",
            "--occurred-at",
            self.config.created_at,
            "--actor-id",
            "runtime.supervisor-1",
            "--json",
        )

        self.assertEqual(0, reclaimed[0])
        self.assertEqual("claimed", reclaimed[2]["payload"]["disposition"])
        self.assertEqual(1, len(reclaimed[1]))
        self.assertEqual("", reclaimed[3])

    def test_submit_uses_only_the_opaque_supervisor_handle(self) -> None:
        claimed = self.claim()
        payload = claimed[2]["payload"]
        handle = "submission.secret-cli-1"
        registration = self.runtime.host_api.handle(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "protocol_version": PROTOCOL_VERSION,
                    "operation": "register_submission",
                    "submission_handle": handle,
                    "work": payload["work"],
                    "fence": payload["fence"],
                },
                separators=(",", ":"),
            ).encode()
        )
        self.assertTrue(json.loads(registration.body)["ok"])

        result_file = self.root / "opaque-result.json"
        result_file.write_text(
            json.dumps(
                {
                    "type": "topic_candidates",
                    "candidates": ["Registry fencing", "Lease recovery"],
                }
            ),
            encoding="utf-8",
        )
        submitted = self.run_cli(
            "submit",
            "--socket",
            str(self.socket_path),
            "--supervisor",
            handle,
            "--idempotency-key",
            "submit-cli-1",
            "--file",
            str(result_file),
            "--occurred-at",
            self.config.created_at,
            "--actor-id",
            "host.adapter-1",
            "--json",
        )

        self.assertEqual(0, submitted[0])
        self.assertEqual(1, len(submitted[1]))
        self.assertNotIn("fence", json.dumps(submitted[2]))
        self.assertEqual(
            "choosing_topic",
            self.runtime.browser.current("session-1").dialogue_state.phase.value,
        )

    def test_supervise_is_the_only_stream_json_operation(self) -> None:
        output = BytesIO()
        errors = StringIO()

        class FakeSupervisor:
            def run(inner_self) -> str:
                del inner_self
                options = supervisor.call_args.kwargs
                options["emit"](
                    b'{"stream_sequence":1,"type":"ready"}\n'
                )
                options["emit"](
                    b'{"stream_sequence":2,"type":"closed"}\n'
                )
                return "requested"

        with mock.patch.object(
            host_cli,
            "HostSupervisor",
            return_value=FakeSupervisor(),
        ) as supervisor:
            status = main(
                [
                    "supervise",
                    "--socket",
                    str(self.socket_path),
                    "--session",
                    "session-1",
                    "--owner",
                    "owner-1",
                    "--wait-timeout",
                    "5",
                    "--lease-seconds",
                    "30",
                    "--max-tenure-seconds",
                    "120",
                    "--stream-json",
                ],
                stdout=output,
                stderr=errors,
            )

        self.assertEqual(0, status)
        self.assertEqual("", errors.getvalue())
        self.assertEqual(
            ["ready", "closed"],
            [json.loads(line)["type"] for line in output.getvalue().splitlines()],
        )
        self.assertEqual(
            (str(self.socket_path), "session-1", "owner-1", "owner-1"),
            supervisor.call_args.args,
        )
        self.assertEqual(5, supervisor.call_args.kwargs["wait_timeout"])
        self.assertEqual(30, supervisor.call_args.kwargs["lease_seconds"])

        missing_stream = self.run_cli(
            "supervise",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--owner",
            "owner-1",
        )
        self.assertEqual(2, missing_stream[0])
        self.assertEqual([], missing_stream[1])
        self.assertEqual(
            "HOST_CLI_STREAM_JSON_REQUIRED\n",
            missing_stream[3],
        )

    def test_real_supervisor_cli_emits_ready_and_terminal_closed_events(self) -> None:
        self.runtime.close_host_ipc()
        status, lines, _response, errors = self.run_cli(
            "supervise",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--owner",
            "owner-1",
            "--wait-timeout",
            "1",
            "--lease-seconds",
            "30",
            "--max-tenure-seconds",
            "120",
            "--stream-json",
        )

        events = [json.loads(line) for line in lines]
        self.assertEqual(0, status)
        self.assertEqual("", errors)
        self.assertEqual(["ready", "closed"], [item["type"] for item in events])
        self.assertEqual([1, 2], [item["stream_sequence"] for item in events])
        self.assertEqual("HOST_IPC_ADDRESS_UNAVAILABLE", events[-1]["reason"])

    def test_api_failure_is_stdout_json_and_transport_failure_is_stderr(self) -> None:
        failed = self.run_cli(
            "claim",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--request-id",
            "claim-request-bad",
            "--claim",
            "claim-bad",
            "--work",
            "wrong-work",
            "--owner",
            "owner-1",
            "--lease-seconds",
            "30",
            "--max-tenure-seconds",
            "120",
            "--json",
        )
        self.assertEqual(1, failed[0])
        self.assertEqual(1, len(failed[1]))
        self.assertFalse(failed[2]["ok"])
        self.assertEqual("", failed[3])

        self.runtime.close_host_ipc()
        transport = self.run_cli(
            "wait",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--timeout",
            "0",
            "--json",
        )
        self.assertEqual(2, transport[0])
        self.assertEqual([], transport[1])
        self.assertEqual("HOST_IPC_ADDRESS_UNAVAILABLE\n", transport[3])
        self.assertNotIn(str(self.root), transport[3])

    def test_input_files_and_arguments_fail_closed_before_ipc(self) -> None:
        valid_claim = self.root / "claim.json"
        valid_claim.write_text(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "protocol_version": PROTOCOL_VERSION,
                    "operation": "claim",
                    "ok": True,
                    "payload": {
                        "work": {},
                        "lease": {},
                        "fence": {},
                        "context": {},
                    },
                }
            ),
            encoding="utf-8",
        )
        duplicate_result = self.root / "duplicate.json"
        duplicate_result.write_text('{"type":"a","type":"b"}', encoding="utf-8")

        invalid = self.run_cli(
            "publish",
            "--socket",
            str(self.socket_path),
            "--idempotency-key",
            "publish-invalid",
            "--claim-envelope",
            str(valid_claim),
            "--lease-version",
            "1",
            "--file",
            str(duplicate_result),
            "--occurred-at",
            self.config.created_at,
            "--actor-id",
            "host.adapter-1",
            "--json",
        )
        self.assertEqual(2, invalid[0])
        self.assertEqual([], invalid[1])
        self.assertEqual("HOST_CLI_INPUT_INVALID\n", invalid[3])

        missing_json = self.run_cli(
            "wait",
            "--socket",
            str(self.socket_path),
            "--session",
            "session-1",
            "--timeout",
            "0",
        )
        self.assertEqual(2, missing_json[0])
        self.assertEqual([], missing_json[1])
        self.assertEqual("HOST_CLI_JSON_REQUIRED\n", missing_json[3])

        with self.assertRaisesRegex(HostCliError, "HOST_CLI_INPUT_INVALID"):
            main(["wait"], stdout=object(), stderr=StringIO())  # type: ignore[arg-type]

    def test_local_parser_file_and_response_boundaries_are_stable(self) -> None:
        for parser, value in (
            (host_cli._positive, "bad"),
            (host_cli._positive, "01"),
            (host_cli._positive, "0"),
            (host_cli._nonnegative, "bad"),
            (host_cli._nonnegative, "01"),
            (host_cli._nonnegative, "-1"),
        ):
            with self.subTest(value=value), self.assertRaises(
                argparse.ArgumentTypeError
            ):
                parser(value)

        invalid_argument = self.run_cli("wait", "--json")
        self.assertEqual(2, invalid_argument[0])
        self.assertEqual("HOST_CLI_ARGUMENT_INVALID\n", invalid_argument[3])

        with self.assertRaisesRegex(HostCliError, "HOST_CLI_INPUT_INVALID"):
            host_cli._read_file(None)
        with mock.patch.object(host_cli, "_O_NOFOLLOW", 0), self.assertRaisesRegex(
            HostCliError,
            "HOST_CLI_UNSUPPORTED",
        ):
            host_cli._read_file(str(self.root / "missing"))

        directory = self.root / "directory-input"
        directory.mkdir()
        oversized = self.root / "oversized.json"
        oversized.write_bytes(b"x" * (host_cli._MAX_INPUT_FILE_BYTES + 1))
        target = self.root / "target.json"
        target.write_text("{}", encoding="utf-8")
        link = self.root / "link.json"
        link.symlink_to(target)
        for path, code in (
            (directory, "HOST_CLI_INPUT_INVALID"),
            (oversized, "HOST_CLI_INPUT_INVALID"),
            (link, "HOST_CLI_INPUT_FAILED"),
            (self.root / "missing", "HOST_CLI_INPUT_FAILED"),
        ):
            with self.subTest(path=path), self.assertRaisesRegex(
                HostCliError,
                code,
            ):
                host_cli._read_file(str(path))

        for raw in (b"", b"[1]", b"{", b"\xff"):
            with self.subTest(raw=raw), self.assertRaisesRegex(
                HostCliError,
                "HOST_CLI_INPUT_INVALID",
            ):
                host_cli._load_object(raw)

        malformed_claim = self.root / "malformed-claim.json"
        malformed_claim.write_text(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "protocol_version": PROTOCOL_VERSION,
                    "operation": "claim",
                    "ok": True,
                    "payload": {
                        "work": "not-an-object",
                        "lease": {},
                        "fence": {},
                        "context": {},
                    },
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(HostCliError, "HOST_CLI_INPUT_INVALID"):
            host_cli._claim_payload(str(malformed_claim))
        with self.assertRaisesRegex(HostCliError, "HOST_CLI_ARGUMENT_INVALID"):
            host_cli._request(argparse.Namespace(operation="other"))
        with self.assertRaisesRegex(HostCliError, "HOST_CLI_RESPONSE_INVALID"):
            host_cli._response_is_success(b"{}", "wait")

    def test_malformed_response_and_output_failure_do_not_leak_details(
        self,
    ) -> None:
        with mock.patch.object(
            host_cli.HostIpcClient,
            "call",
            return_value=HostApiResponse(b"{}"),
        ):
            malformed = self.run_cli(
                "wait",
                "--socket",
                str(self.socket_path),
                "--session",
                "session-1",
                "--timeout",
                "0",
                "--json",
            )
        self.assertEqual(2, malformed[0])
        self.assertEqual("HOST_CLI_RESPONSE_INVALID\n", malformed[3])

        class BrokenOutput(BytesIO):
            def write(self, _value):
                raise OSError("sensitive output detail")

        errors = StringIO()
        status = main(
            [
                "wait",
                "--socket",
                str(self.socket_path),
                "--session",
                "session-1",
                "--timeout",
                "0",
                "--json",
            ],
            stdout=BrokenOutput(),
            stderr=errors,
        )
        self.assertEqual(2, status)
        self.assertEqual("HOST_CLI_INTERNAL_ERROR\n", errors.getvalue())
        self.assertNotIn("sensitive", errors.getvalue())

    def test_real_module_subprocess_emits_only_one_json_line(self) -> None:
        package_root = Path(__file__).resolve().parents[1] / "skills/x-sync/scripts"
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(package_root)
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "xsync_v2.host_cli",
                "wait",
                "--socket",
                str(self.socket_path),
                "--session",
                "session-1",
                "--timeout",
                "0",
                "--json",
            ],
            cwd=self.root,
            env=environment,
            check=False,
            capture_output=True,
        )

        self.assertEqual(0, process.returncode, process.stderr.decode())
        self.assertEqual(1, len(process.stdout.splitlines()))
        self.assertEqual(b"", process.stderr)
        self.assertTrue(json.loads(process.stdout)["ok"])


if __name__ == "__main__":
    unittest.main()
