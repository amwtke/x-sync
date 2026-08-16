import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.domain import (
    CommittedDialogueEvent,
    ExportRecord,
    ExportRequested,
    ExportStatus,
    SessionStarted,
    TriggerBinding,
    TriggerKind,
    initial_dialogue_state,
)
from xsync_v2.event_codec import sha256_digest
from xsync_v2.export import (
    ExportError,
    ExportMaterializer,
    ExportSnapshot,
    build_export_document,
    completion_command,
    plan_export_intent,
    render_export,
)
from xsync_v2.secure_fs import SecureDirectory, SecureFsError
from xsync_v2.state_machine import conversation_version_delta, reduce


def digest(label: str) -> str:
    return sha256_digest(label.encode())


def open_state():
    initial = initial_dialogue_state("dialogue-1", 1)
    trigger = TriggerBinding(
        TriggerKind.TOPIC_CANDIDATES,
        "trigger-work-1",
        "epoch-1",
        None,
        None,
        digest("input"),
        digest("evidence"),
    )
    payload = SessionStarted(trigger)
    event = CommittedDialogueEvent(
        "event-1",
        1,
        0,
        conversation_version_delta(payload),
        "command-start",
        payload,
    )
    return reduce(initial, event)


def requested():
    return ExportRequested(
        "export-1",
        1,
        "intent-export-1",
        1,
        "2026-08-16T12:00:00+08:00",
        digest("request"),
    )


def snapshot(repository_digest=None):
    return ExportSnapshot(
        "export-1",
        1,
        "dialogue-1",
        "learner-1",
        "repository-1",
        digest("repository") if repository_digest is None else repository_digest,
        digest("overlay"),
        1,
        open_state(),
    )


class ExportTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path.cwd())
        root_path = Path(self.temporary.name) / "root"
        root_path.mkdir(mode=0o700)
        os.chmod(root_path, 0o700)
        self.root = SecureDirectory.open(root_path)
        self.exports = self.root.ensure_directory("exports")
        self.materializer = ExportMaterializer(self.exports)

    def tearDown(self):
        self.exports.close()
        self.root.close()
        self.temporary.cleanup()

    def test_document_is_deterministic_structured_and_transcript_free(self):
        value = snapshot()
        first_json, first_markdown = render_export(value)
        second_json, second_markdown = render_export(value)
        self.assertEqual(first_json, second_json)
        self.assertEqual(first_markdown, second_markdown)
        document = json.loads(first_json)
        self.assertEqual("x_sync_insight_export", document["record_type"])
        self.assertEqual(1, document["as_of_event_sequence"])
        self.assertEqual([], document["learner_current_model"])
        self.assertNotIn("transcript", document)
        integrity = document.pop("integrity_hash")
        from xsync_v2.event_codec import canonical_json_bytes

        self.assertEqual(sha256_digest(canonical_json_bytes(document)), integrity)
        self.assertEqual(
            document | {"integrity_hash": integrity},
            build_export_document(value),
        )

    def test_materialization_is_idempotent_and_latest_requires_completion(self):
        request = requested()
        intent = plan_export_intent("dialogue-1", request)
        artifacts = self.materializer.materialize(
            intent,
            snapshot(),
            request.requested_at,
        )
        self.assertEqual(
            artifacts,
            self.materializer.materialize(
                intent,
                snapshot(),
                request.requested_at,
            ),
        )
        session = self.exports.open_directory("dialogue-1")
        try:
            with self.assertRaisesRegex(SecureFsError, "FILE_NOT_FOUND"):
                session.read_bytes("latest.json", max_bytes=4096)
        finally:
            session.close()

        command = completion_command(
            "command-complete",
            intent,
            artifacts,
            "2026-08-16T12:00:01+08:00",
        )
        completed = ExportRecord(
            request.export_id,
            request.export_sequence,
            request.intent_id,
            request.as_of_event_sequence,
            request.requested_at,
            request.payload_digest,
            ExportStatus.COMPLETED,
            command.completed_at,
            command.json_path,
            command.json_digest,
            command.markdown_path,
            command.markdown_digest,
            command.freshness_overlay_digest,
        )
        self.materializer.update_latest("dialogue-1", completed)
        self.materializer.update_latest("dialogue-1", completed)
        session = self.exports.open_directory("dialogue-1")
        try:
            latest = json.loads(
                session.read_bytes("latest.json", max_bytes=4096)
            )
        finally:
            session.close()
        self.assertEqual("export-1", latest["export_id"])
        self.assertEqual(1, latest["export_sequence"])

    def test_partial_pair_retries_and_conflicting_body_fails_closed(self):
        request = requested()
        intent = plan_export_intent("dialogue-1", request)
        original = ExportMaterializer._ensure_file
        calls = 0

        def fail_second(directory, component, payload):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ExportError("INJECTED_CRASH")
            original(directory, component, payload)

        ExportMaterializer._ensure_file = staticmethod(fail_second)
        try:
            with self.assertRaisesRegex(ExportError, "INJECTED_CRASH"):
                self.materializer.materialize(
                    intent,
                    snapshot(),
                    request.requested_at,
                )
        finally:
            ExportMaterializer._ensure_file = staticmethod(original)

        self.materializer.materialize(intent, snapshot(), request.requested_at)
        with self.assertRaisesRegex(
            ExportError,
            "EXPORT_IDEMPOTENCY_CONFLICT",
        ):
            self.materializer.materialize(
                intent,
                snapshot(digest("different-repository")),
                request.requested_at,
            )

    def test_invalid_snapshot_and_intent_fail_closed(self):
        request = requested()
        intent = plan_export_intent("dialogue-1", request)
        with self.assertRaisesRegex(ExportError, "INVALID_EXPORT_SNAPSHOT"):
            render_export(replace(snapshot(), as_of_event_sequence=0))
        with self.assertRaisesRegex(ExportError, "EXPORT_INTENT_MISMATCH"):
            self.materializer.materialize(
                replace(intent, session_id="dialogue-2"),
                snapshot(),
                request.requested_at,
            )


if __name__ == "__main__":
    unittest.main()
