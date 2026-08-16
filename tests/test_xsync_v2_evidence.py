from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

import tests.xsync_v2_path  # noqa: F401
from xsync_v2.coordinator import (
    CoordinatorError,
    DialogueCoordinator,
    DialogueSessionConfig,
)
from xsync_v2.domain import EvidenceHealth
from xsync_v2.evidence import (
    EvidenceClaimType,
    EvidenceKind,
    EvidenceSnapshot,
    EvidenceSource,
    EvidenceStoreError,
    FrozenEvidenceEntry,
    SessionEvidenceStore,
    decode_evidence_snapshot,
    encode_evidence_snapshot,
)
from xsync_v2.locking import DomainLockManager
from xsync_v2.secure_fs import SecureDirectory


class SessionEvidenceStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        base = Path(self.temporary.name).resolve()
        self.repository = base / "repository"
        self.repository.mkdir(mode=0o700)
        self.state_path = base / "state"
        self.state_path.mkdir(mode=0o700)
        self.git("init", "-q")
        self.git("config", "user.name", "X-Sync Test")
        self.git("config", "user.email", "x-sync@example.invalid")
        (self.repository / "docs").mkdir()
        (self.repository / "src").mkdir()
        (self.repository / "docs" / "spec.md").write_text(
            "Requirement: retries are bounded.\n"
            "Boundary: failed work is recoverable.\n"
            "Unrelated appendix.\n",
            encoding="utf-8",
        )
        (self.repository / "src" / "worker.py").write_text(
            "MAX_ATTEMPTS = 3\n",
            encoding="utf-8",
        )
        self.git("add", ".")
        self.git("commit", "-qm", "initial evidence")
        self.root = SecureDirectory.open(self.state_path)
        self.addCleanup(self.root.close)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.addCleanup(self.dialogues.close)
        self.locks = DomainLockManager(self.state_path / "locks")
        self.addCleanup(self.locks.close)
        self.store = SessionEvidenceStore(
            self.repository,
            "repository-1",
            self.dialogues,
            self.locks,
        )
        self.addCleanup(self.store.close)

    def git(self, *arguments: str) -> str:
        return subprocess.run(
            ["git", "-C", str(self.repository), *arguments],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    @staticmethod
    def source(**changes: object) -> EvidenceSource:
        values = {
            "evidence_id": "ev.retry-spec",
            "kind": EvidenceKind.SPEC,
            "claim_type": EvidenceClaimType.REQUIREMENT,
            "claim": "Retries are bounded and failed work remains recoverable.",
            "relative_path": "docs/spec.md",
            "start_line": 1,
            "end_line": 2,
            "imported_from": "ev.v1.retry-spec",
            **changes,
        }
        return EvidenceSource(**values)  # type: ignore[arg-type]

    def capture(self, *sources: EvidenceSource):
        return self.store.capture(
            "session-1",
            tuple(sources) or (self.source(),),
            captured_at="2026-08-16T16:00:00+08:00",
        )

    @staticmethod
    def config(digest: str) -> DialogueSessionConfig:
        return DialogueSessionConfig(
            "session-1",
            "learner-1",
            "repository-1",
            "2026-08-16T16:00:00+08:00",
            "runtime-1",
            digest,
        )

    def test_capture_is_session_local_canonical_and_current(self) -> None:
        snapshot = self.capture()

        self.assertEqual(
            snapshot,
            self.store.load("session-1", snapshot.snapshot_digest),
        )
        self.assertEqual(
            encode_evidence_snapshot(snapshot),
            encode_evidence_snapshot(
                decode_evidence_snapshot(encode_evidence_snapshot(snapshot))
            ),
        )
        self.assertEqual(
            EvidenceHealth.CURRENT,
            self.store.verify(self.config(snapshot.snapshot_digest)).health,
        )
        session_directories = tuple((self.state_path / "dialogues").iterdir())
        self.assertEqual(1, len(session_directories))
        evidence_files = tuple((session_directories[0] / "evidence").iterdir())
        self.assertEqual(1, len(evidence_files))
        self.assertEqual(0o600, evidence_files[0].stat().st_mode & 0o777)

    def test_unrelated_change_stays_current_but_focused_change_is_stale(self) -> None:
        snapshot = self.capture()
        config = self.config(snapshot.snapshot_digest)
        (self.repository / "src" / "worker.py").write_text(
            "MAX_ATTEMPTS = 4\n",
            encoding="utf-8",
        )
        self.assertEqual(EvidenceHealth.CURRENT, self.store.verify(config).health)

        (self.repository / "docs" / "spec.md").write_text(
            "Requirement: retries are unlimited.\n"
            "Boundary: failed work is recoverable.\n"
            "Unrelated appendix.\n",
            encoding="utf-8",
        )
        self.assertEqual(EvidenceHealth.STALE, self.store.verify(config).health)

    def test_exact_range_on_dirty_focused_file_is_captured_dirty(self) -> None:
        snapshot = self.capture()
        config = self.config(snapshot.snapshot_digest)
        (self.repository / "docs" / "spec.md").write_text(
            "Requirement: retries are bounded.\n"
            "Boundary: failed work is recoverable.\n"
            "Changed appendix outside the cited range.\n",
            encoding="utf-8",
        )

        check = self.store.verify(config)

        self.assertEqual(EvidenceHealth.CAPTURED_DIRTY, check.health)
        self.assertIsNotNone(check.exact_recheck_fingerprint)
        self.assertTrue(check.exact_recheck_fingerprint.startswith("sha256:"))

    def test_missing_or_unreadable_snapshot_is_unavailable(self) -> None:
        snapshot = self.capture()
        config = self.config(snapshot.snapshot_digest)
        os.unlink(self.repository / "docs" / "spec.md")
        self.assertEqual(EvidenceHealth.UNAVAILABLE, self.store.verify(config).health)
        self.assertEqual(
            EvidenceHealth.UNAVAILABLE,
            self.store.verify(
                replace(config, evidence_digest="sha256:" + "f" * 64)
            ).health,
        )

    def test_new_session_cannot_activate_without_a_publishable_snapshot(self) -> None:
        coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            "registry-1",
            evidence_verifier=self.store.verify,
        )
        bad = self.config("sha256:" + "f" * 64)

        with self.assertRaisesRegex(
            CoordinatorError,
            "EVIDENCE_NOT_PUBLISHABLE",
        ):
            coordinator.resolve(bad)

        self.assertIsNone(coordinator.recover())

    def test_sensitive_symlink_binary_and_invalid_ranges_fail_closed(self) -> None:
        (self.repository / ".env").write_text("SECRET=value\n", encoding="utf-8")
        (self.repository / "binary.dat").write_bytes(b"abc\x00def")
        (self.repository / "linked.md").symlink_to("docs/spec.md")
        invalid = (
            self.source(relative_path=".env"),
            self.source(relative_path="../outside"),
            self.source(relative_path="linked.md"),
            self.source(relative_path="binary.dat", start_line=None, end_line=None),
            self.source(start_line=0, end_line=1),
            self.source(start_line=2, end_line=1),
        )
        for index, source in enumerate(invalid):
            with self.subTest(source=source), self.assertRaises(EvidenceStoreError):
                self.store.capture(
                    f"session-{index + 2}",
                    (source,),
                    captured_at="2026-08-16T16:00:00+08:00",
                )

    def test_codec_rejects_tamper_unknown_fields_and_noncanonical_json(self) -> None:
        snapshot = self.capture()
        raw = encode_evidence_snapshot(snapshot)
        tree = json.loads(raw)
        tree["entries"][0]["claim"] = "tampered"
        tampered = json.dumps(
            tree,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        with self.assertRaisesRegex(
            EvidenceStoreError,
            "EVIDENCE_SNAPSHOT_DIGEST_MISMATCH",
        ):
            decode_evidence_snapshot(tampered)
        tree["unexpected"] = True
        with self.assertRaisesRegex(
            EvidenceStoreError,
            "EVIDENCE_SNAPSHOT_INVALID",
        ):
            decode_evidence_snapshot(
                json.dumps(tree, sort_keys=True, separators=(",", ":")).encode()
            )
        with self.assertRaisesRegex(
            EvidenceStoreError,
            "EVIDENCE_SNAPSHOT_NON_CANONICAL",
        ):
            decode_evidence_snapshot(b" " + raw)

    def test_full_file_capture_is_idempotent_and_has_a_plain_location(self) -> None:
        source = self.source(
            start_line=None,
            end_line=None,
            imported_from=None,
        )

        first = self.capture(source)
        second = self.capture(source)

        self.assertEqual(first, second)
        self.assertEqual("docs/spec.md", first.entries[0].location)
        self.assertEqual(
            first,
            decode_evidence_snapshot(encode_evidence_snapshot(first)),
        )

    def test_public_capture_load_and_config_boundaries_fail_closed(self) -> None:
        invalid_captures = (
            ("bad/session", (self.source(),), "2026-08-16T16:00:00+08:00"),
            ("session-2", (), "2026-08-16T16:00:00+08:00"),
            ("session-2", (self.source(),), "2026-08-16T16:00:00"),
            (
                "session-2",
                (self.source(), self.source()),
                "2026-08-16T16:00:00+08:00",
            ),
            (
                "session-2",
                (self.source(start_line=1, end_line=None),),
                "2026-08-16T16:00:00+08:00",
            ),
            (
                "session-2",
                (self.source(imported_from="bad/source"),),
                "2026-08-16T16:00:00+08:00",
            ),
            (
                "session-2",
                (self.source(relative_path="node_modules/package/index.js"),),
                "2026-08-16T16:00:00+08:00",
            ),
            (
                "session-2",
                (replace(self.source(), kind="spec"),),
                "2026-08-16T16:00:00+08:00",
            ),
        )
        for session_id, sources, captured_at in invalid_captures:
            with self.subTest(session_id=session_id, sources=sources):
                with self.assertRaises(EvidenceStoreError):
                    self.store.capture(
                        session_id,
                        sources,
                        captured_at=captured_at,
                    )

        snapshot = self.capture()
        with self.assertRaisesRegex(
            EvidenceStoreError,
            "EVIDENCE_CONFIG_MISMATCH",
        ):
            self.store.verify(
                replace(self.config(snapshot.snapshot_digest), repository_id="other")
            )
        for session_id, digest in (
            ("bad/session", snapshot.snapshot_digest),
            ("session-1", "not-a-digest"),
        ):
            with self.assertRaisesRegex(
                EvidenceStoreError,
                "EVIDENCE_SNAPSHOT_INVALID",
            ):
                self.store.load(session_id, digest)

    def test_codec_rejects_invalid_bytes_duplicate_keys_and_typed_shapes(self) -> None:
        snapshot = self.capture()
        raw = encode_evidence_snapshot(snapshot)
        tree = json.loads(raw)
        malformed = (
            b"",
            b"\xff",
            b'{"schema_version":2,"schema_version":2}',
            json.dumps(
                {**tree, "entries": {}},
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            json.dumps(
                {**tree, "entries": [{"unexpected": True}]},
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            json.dumps(
                {
                    **tree,
                    "entries": [{**tree["entries"][0], "kind": "unknown"}],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            json.dumps(
                {**tree, "focused_dirty_at_capture": 1},
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
        )
        for value in malformed:
            with self.subTest(value=value[:40]), self.assertRaises(
                EvidenceStoreError
            ):
                decode_evidence_snapshot(value)

        with self.assertRaisesRegex(
            EvidenceStoreError,
            "EVIDENCE_SNAPSHOT_DIGEST_MISMATCH",
        ):
            encode_evidence_snapshot(
                replace(snapshot, snapshot_digest="sha256:" + "f" * 64)
            )
        invalid_entry = FrozenEvidenceEntry(
            "ev.duplicate",
            EvidenceKind.SPEC,
            EvidenceClaimType.REQUIREMENT,
            "A duplicate identity is invalid.",
            "docs/spec.md",
            1,
            1,
            snapshot.entries[0].content_hash,
            None,
        )
        invalid_snapshot = EvidenceSnapshot(
            snapshot.snapshot_digest,
            snapshot.session_id,
            snapshot.repository_id,
            snapshot.captured_at,
            snapshot.repository_head,
            snapshot.focused_dirty_at_capture,
            snapshot.focused_status_digest,
            (invalid_entry, invalid_entry),
        )
        with self.assertRaisesRegex(
            EvidenceStoreError,
            "EVIDENCE_SNAPSHOT_INVALID",
        ):
            encode_evidence_snapshot(invalid_snapshot)

    def test_reader_rejects_range_hardlink_oversize_and_repository_swap(self) -> None:
        hardlink = self.repository / "docs" / "hard.md"
        os.link(self.repository / "docs" / "spec.md", hardlink)
        oversized = self.repository / "docs" / "oversized.md"
        oversized.write_bytes(b"x" * 1_000_001)
        invalid = (
            self.source(end_line=99),
            self.source(relative_path="docs"),
            self.source(relative_path="docs/hard.md"),
            self.source(relative_path="docs/oversized.md"),
        )
        for index, source in enumerate(invalid):
            with self.subTest(source=source), self.assertRaises(EvidenceStoreError):
                self.store.capture(
                    f"session-reader-{index}",
                    (source,),
                    captured_at="2026-08-16T16:00:00+08:00",
                )

        hardlink.unlink()
        snapshot = self.capture()
        original = self.repository.with_name("repository-original")
        self.repository.rename(original)
        self.repository.mkdir(mode=0o700)
        self.assertEqual(
            EvidenceHealth.UNAVAILABLE,
            self.store.verify(self.config(snapshot.snapshot_digest)).health,
        )
        with self.assertRaisesRegex(
            EvidenceStoreError,
            "REPOSITORY_PATH_CHANGED",
        ):
            self.store.capture(
                "session-after-swap",
                (self.source(),),
                captured_at="2026-08-16T16:00:00+08:00",
            )

    def test_git_timeout_and_invalid_head_are_stable_capture_failures(self) -> None:
        with mock.patch(
            "xsync_v2.evidence.subprocess.run",
            side_effect=subprocess.TimeoutExpired("git", 15),
        ):
            with self.assertRaisesRegex(EvidenceStoreError, "GIT_UNAVAILABLE"):
                self.capture()
        with mock.patch.object(
            type(self.store._reader),
            "git",
            return_value=b"not-a-git-object\n",
        ):
            with self.assertRaisesRegex(
                EvidenceStoreError,
                "GIT_EVIDENCE_FAILED",
            ):
                self.capture()

    def test_mutation_git_failure_and_closed_reader_remain_fail_closed(self) -> None:
        with (
            mock.patch.object(
                type(self.store._reader),
                "read",
                side_effect=(b"first", b"second"),
            ),
            mock.patch.object(self.store, "_head", return_value="a" * 40),
            mock.patch.object(self.store, "_focused_status", return_value=b""),
            self.assertRaisesRegex(
                EvidenceStoreError,
                "EVIDENCE_CHANGED_DURING_CAPTURE",
            ),
        ):
            self.capture()

        snapshot = self.capture()
        with mock.patch.object(
            self.store,
            "_focused_status",
            side_effect=EvidenceStoreError("GIT_EVIDENCE_FAILED"),
        ):
            self.assertEqual(
                EvidenceHealth.UNAVAILABLE,
                self.store.verify(self.config(snapshot.snapshot_digest)).health,
            )
        with mock.patch(
            "xsync_v2.evidence.subprocess.run",
            return_value=subprocess.CompletedProcess(
                ("git",),
                1,
                stdout=b"",
                stderr=b"failure",
            ),
        ):
            with self.assertRaisesRegex(
                EvidenceStoreError,
                "GIT_EVIDENCE_FAILED",
            ):
                self.capture()

        self.assertIs(self.store, self.store.__enter__())
        self.store.__exit__(None, None, None)
        self.store.close()
        with self.assertRaisesRegex(EvidenceStoreError, "EVIDENCE_STORE_CLOSED"):
            self.capture()

    def test_store_constructor_and_non_utf8_file_are_rejected(self) -> None:
        for repository in (object(), Path("relative-repository")):
            with self.subTest(repository=repository), self.assertRaisesRegex(
                EvidenceStoreError,
                "INVALID_EVIDENCE_STORE_CONFIGURATION",
            ):
                SessionEvidenceStore(
                    repository,  # type: ignore[arg-type]
                    "repository-1",
                    self.dialogues,
                    self.locks,
                )

        (self.repository / "invalid-utf8.md").write_bytes(b"\xff\xfe")
        with self.assertRaisesRegex(EvidenceStoreError, "EVIDENCE_FILE_UNSAFE"):
            self.store.capture(
                "session-invalid-utf8",
                (
                    self.source(
                        relative_path="invalid-utf8.md",
                        start_line=None,
                        end_line=None,
                    ),
                ),
                captured_at="2026-08-16T16:00:00+08:00",
            )


if __name__ == "__main__":
    unittest.main()
