from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

import tests.xsync_v2_path  # noqa: F401
from tests.test_xsync_v2_coordinator import config

from xsync_v2.coordinator import DialogueCoordinator
from xsync_v2.domain import CompleteExport, EvidenceCheck, EvidenceHealth
from xsync_v2.export import ExportMaterializer
from xsync_v2.export_service import (
    ExportService,
    ExportServiceError,
    ExportServiceRequest,
)
from xsync_v2.locking import DomainLockManager
from xsync_v2.secure_fs import SecureDirectory


class ExportServiceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory(dir=Path.cwd())
        self.root_path = Path(self.temporary.name) / "root"
        self.root_path.mkdir(mode=0o700)
        self.root = SecureDirectory.open(self.root_path)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.exports = self.root.ensure_directory("exports")
        self.locks = DomainLockManager(self.root_path / "locks")
        self.verify = lambda item: EvidenceCheck(
            EvidenceHealth.CURRENT,
            item.evidence_digest,
        )
        self.coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=self.verify,
        )
        self.coordinator.resolve(config("dlg-a"))
        self.times = iter(
            (
                "2026-08-16T12:00:00+08:00",
                "2026-08-16T12:00:01+08:00",
                "2026-08-16T12:00:02+08:00",
                "2026-08-16T12:00:03+08:00",
            )
        )
        self.service = ExportService(
            self.coordinator,
            self.dialogues,
            self.locks,
            ExportMaterializer(self.exports),
            self.verify,
            clock=lambda: next(self.times),
        )

    def tearDown(self):
        self.locks.close()
        self.exports.close()
        self.dialogues.close()
        self.root.close()
        self.temporary.cleanup()

    def request(self):
        state = self.coordinator.recover().dialogue_state
        return ExportServiceRequest(
            "dlg-a",
            "export-key-1",
            state.conversation_version,
        )

    def test_request_materializes_completes_and_replays_without_new_clock(self):
        outcome = self.service.request(self.request())
        self.assertFalse(outcome.replayed)
        self.assertEqual("completed", outcome.record.status.value)
        state = self.coordinator.recover().dialogue_state
        self.assertEqual(1, len(state.exports))
        self.assertEqual(3, state.sequence)
        session = self.exports.open_directory("dlg-a")
        try:
            names = session.list_entries()
        finally:
            session.close()
        self.assertEqual(3, len(names))
        self.assertIn("latest.json", names)

        replay = self.service.request(self.request())
        self.assertTrue(replay.replayed)
        self.assertEqual(outcome.record, replay.record)
        self.assertEqual(3, self.coordinator.recover().dialogue_state.sequence)
        with self.assertRaisesRegex(ExportServiceError, "IDEMPOTENCY_CONFLICT"):
            self.service.request(
                ExportServiceRequest(
                    "dlg-a",
                    "export-key-1",
                    0,
                    "different-browser",
                )
            )

    def test_restart_recovers_files_written_before_completion(self):
        original = self.coordinator.execute
        failed_once = False

        def fail_completion(request):
            nonlocal failed_once
            if type(request.command) is CompleteExport and not failed_once:
                failed_once = True
                raise RuntimeError("INJECTED_RESPONSE_LOSS")
            return original(request)

        with mock.patch.object(
            self.coordinator,
            "execute",
            side_effect=fail_completion,
        ):
            with self.assertRaisesRegex(RuntimeError, "INJECTED_RESPONSE_LOSS"):
                self.service.request(self.request())
        pending = self.coordinator.recover().dialogue_state.exports[0]
        self.assertEqual("requested", pending.status.value)

        recovered = self.service.recover_current()
        self.assertEqual(1, len(recovered))
        self.assertEqual("completed", recovered[0].record.status.value)
        self.assertEqual(3, self.coordinator.recover().dialogue_state.sequence)

    def test_invalid_request_and_clock_fail_closed(self):
        with self.assertRaisesRegex(ExportServiceError, "VALIDATION_FAILED"):
            self.service.request(
                ExportServiceRequest("../escape", "key", 0)
            )
        bad = ExportService(
            self.coordinator,
            self.dialogues,
            self.locks,
            ExportMaterializer(self.exports),
            self.verify,
            clock=lambda: "not-a-time",
        )
        with self.assertRaisesRegex(ExportServiceError, "INVALID_EXPORT_CLOCK"):
            bad.request(
                ExportServiceRequest(
                    "dlg-a",
                    "export-key-bad-clock",
                    self.coordinator.recover().dialogue_state.conversation_version,
                )
            )


if __name__ == "__main__":
    unittest.main()
