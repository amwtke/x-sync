from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

# isort: split
from xsync_v2 import submission_store as store_module
from xsync_v2.lease_store import PublishFence
from xsync_v2.locking import DomainLockManager, RegistryLockMode
from xsync_v2.secure_fs import SecureDirectory
from xsync_v2.submission_store import (
    SubmissionHandleStore,
    SubmissionStoreError,
)
from xsync_v2.work import derive_runnable_work

from tests.test_xsync_v2_work import canonical_trigger


class SubmissionHandleStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root_path = Path(self.temporary.name).resolve()
        (self.root_path / "locks").mkdir(mode=0o700)
        self.root = SecureDirectory.open(self.root_path)
        self.addCleanup(self.root.close)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.addCleanup(self.dialogues.close)
        self.locks = DomainLockManager(self.root_path / "locks")
        self.addCleanup(self.locks.close)
        self.store = SubmissionHandleStore(self.dialogues, self.locks)
        self.addCleanup(self.store.close)
        state, origin = canonical_trigger()
        work = derive_runnable_work(state, origin)
        assert work is not None
        self.work = work
        self.fence = PublishFence(
            work.session_id,
            "claim-1",
            work.work_id,
            "owner-1",
            "runtime-1",
            1,
            work.registry_generation,
        )
        self.handle = "submission.secret-1"

    def register(self, handle: str | None = None):
        with self.locks.semantic_session(
            self.work.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            return self.store.register(
                handle or self.handle,
                self.work,
                self.fence,
                authority,
            )

    def test_hash_only_registration_replays_and_survives_restart(self) -> None:
        first = self.register()
        replay = self.register()

        self.assertFalse(first.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.record, replay.record)
        self.assertNotIn(
            self.handle.encode(),
            b"".join(
                path.read_bytes()
                for path in (self.root_path / "dialogues" / "submissions").iterdir()
            ),
        )
        self.store.close()
        restarted = SubmissionHandleStore(self.dialogues, self.locks)
        self.addCleanup(restarted.close)
        self.store = restarted
        self.assertEqual(first.record, restarted.resolve(self.handle))

    def test_wrong_secret_and_conflicting_binding_fail_closed(self) -> None:
        self.register()
        with self.assertRaisesRegex(
            SubmissionStoreError,
            "SUBMISSION_HANDLE_NOT_FOUND",
        ):
            self.store.resolve("submission.wrong-secret")
        with self.locks.semantic_session(
            self.work.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority, self.assertRaisesRegex(
            SubmissionStoreError,
            "SUBMISSION_HANDLE_CONFLICT",
        ):
            self.store.register(
                self.handle,
                self.work,
                replace(self.fence, claim_id="claim-2"),
                authority,
            )

    def test_registration_requires_matching_live_authority_and_work(self) -> None:
        with self.locks.semantic_session(
            "another-session",
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority, self.assertRaisesRegex(
            SubmissionStoreError,
            "LOCK_AUTHORITY_SESSION_MISMATCH",
        ):
            self.store.register(
                self.handle,
                self.work,
                self.fence,
                authority,
            )
        with self.locks.semantic_session(
            self.work.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority, self.assertRaisesRegex(
            SubmissionStoreError,
            "SUBMISSION_WORK_MISMATCH",
        ):
            self.store.register(
                self.handle,
                self.work,
                replace(self.fence, work_id="work-other"),
                authority,
            )

    def test_corrupt_or_noncanonical_record_is_never_resolved(self) -> None:
        outcome = self.register()
        component = f"handle-{outcome.record.handle_digest[7:]}.json"
        path = self.root_path / "dialogues" / "submissions" / component
        path.write_bytes(path.read_bytes() + b"\n")

        with self.assertRaisesRegex(
            SubmissionStoreError,
            "SUBMISSION_RECORD_NON_CANONICAL",
        ):
            self.store.resolve(self.handle)

    def test_invalid_handle_and_foreign_lock_namespace_are_rejected(self) -> None:
        with self.assertRaisesRegex(
            SubmissionStoreError,
            "SUBMISSION_HANDLE_INVALID",
        ):
            self.store.resolve("../unsafe")

        foreign_path = self.root_path / "foreign-locks"
        foreign_path.mkdir(mode=0o700)
        with (
            DomainLockManager(foreign_path) as foreign,
            foreign.semantic_session(
                self.work.session_id,
                registry_mode=RegistryLockMode.EXCLUSIVE,
            ) as authority,
            self.assertRaisesRegex(
                SubmissionStoreError,
                "LOCK_AUTHORITY_INVALID",
            ),
        ):
            self.store.register(
                self.handle,
                self.work,
                self.fence,
                authority,
            )

    def test_codec_rejects_shape_hash_and_nested_corruption(self) -> None:
        outcome = self.register()
        record = outcome.record
        component = f"handle-{record.handle_digest[7:]}.json"
        raw = (
            self.root_path / "dialogues" / "submissions" / component
        ).read_bytes()
        tree = json.loads(raw)
        invalid_schema = {**tree, "schema_version": 99}
        invalid_work = {**tree, "work": {**tree["work"], "kind": "unknown"}}
        invalid_hash = {**tree, "record_digest": "sha256:" + "0" * 64}
        malformed = (
            b"",
            b"{}",
            b'{"schema_version":2,"schema_version":2}',
            b"\xff",
            store_module.canonical_json_bytes(invalid_schema),
            store_module.canonical_json_bytes(invalid_work),
            store_module.canonical_json_bytes(invalid_hash),
        )
        for value in malformed:
            with self.subTest(value=value[:24]), self.assertRaises(
                SubmissionStoreError
            ):
                store_module._decode_record(value)

        invalid_calls = (
            lambda: store_module._component("not-a-digest"),
            lambda: store_module._fence_tree(None),  # type: ignore[arg-type]
            lambda: store_module._validate_fence(
                replace(self.fence, lease_version=0)
            ),
            lambda: store_module._validate_binding(
                None,  # type: ignore[arg-type]
                self.fence,
            ),
            lambda: store_module._validate_binding(
                replace(self.work, session_id="../unsafe"),
                self.fence,
            ),
            lambda: store_module._build_record(
                "not-a-digest",
                self.work,
                self.fence,
            ),
            lambda: store_module._encode_record(None),  # type: ignore[arg-type]
            lambda: store_module._encode_record(
                replace(record, record_digest="sha256:" + "1" * 64)
            ),
        )
        for callback in invalid_calls:
            with self.assertRaises(SubmissionStoreError):
                callback()

    def test_confirm_and_concurrent_exact_registration_are_idempotent(self) -> None:
        record = self.register().record
        with self.locks.semantic_session(
            self.work.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            self.assertEqual(
                record,
                self.store.confirm(self.handle, record, authority),
            )
            with self.assertRaisesRegex(
                SubmissionStoreError,
                "SUBMISSION_RECORD_INVALID",
            ):
                self.store.confirm(
                    self.handle,
                    None,  # type: ignore[arg-type]
                    authority,
                )
            with self.assertRaisesRegex(
                SubmissionStoreError,
                "SUBMISSION_HANDLE_CONFLICT",
            ):
                self.store.confirm(
                    self.handle,
                    replace(
                        record,
                        fence=replace(record.fence, claim_id="claim-2"),
                    ),
                    authority,
                )

        with self.locks.semantic_session(
            self.work.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority, mock.patch.object(
            self.store,
            "_read_optional",
            side_effect=(None, record),
        ), mock.patch.object(
            self.store._directory,
            "write_immutable_guarded",
            side_effect=store_module.SecureFsError("IMMUTABLE_EXISTS"),
        ):
            replay = self.store.register(
                self.handle,
                self.work,
                self.fence,
                authority,
            )
        self.assertTrue(replay.replayed)

    def test_corrupt_namespace_prevents_restart(self) -> None:
        self.store.close()
        namespace = (
            self.root_path
            / "dialogues"
            / "submissions"
            / "submission-lock-namespace.json"
        )
        namespace.write_bytes(b"{}")
        with self.assertRaises(SubmissionStoreError):
            SubmissionHandleStore(self.dialogues, self.locks)

    def test_configuration_and_resolved_hash_mismatch_fail_closed(self) -> None:
        with self.assertRaisesRegex(
            SubmissionStoreError,
            "INVALID_SUBMISSION_STORE_CONFIGURATION",
        ):
            SubmissionHandleStore(None, self.locks)  # type: ignore[arg-type]
        record = self.register().record
        different = replace(record, handle_digest="sha256:" + "f" * 64)
        with mock.patch.object(
            self.store,
            "_read_optional",
            return_value=different,
        ), self.assertRaisesRegex(
            SubmissionStoreError,
            "SUBMISSION_HANDLE_CONFLICT",
        ):
            self.store.resolve(self.handle)

        original = self.store._lock_namespace_identity
        self.store._lock_namespace_identity = (0, 0)
        try:
            with self.locks.semantic_session(
                self.work.session_id,
                registry_mode=RegistryLockMode.EXCLUSIVE,
            ) as authority, self.assertRaisesRegex(
                SubmissionStoreError,
                "LOCK_NAMESPACE_MISMATCH",
            ):
                self.store.register(
                    "submission.secret-2",
                    self.work,
                    self.fence,
                    authority,
                )
        finally:
            self.store._lock_namespace_identity = original


if __name__ == "__main__":
    unittest.main()
