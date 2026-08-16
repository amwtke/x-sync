import errno
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

# isort: split
from xsync_v2 import secure_fs
from xsync_v2.secure_fs import EntryKind, SecureDirectory, SecureFsError


class SecureFsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.root = Path(self.temporary.name) / "root"
        self.root.mkdir(mode=0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def test_created_directories_and_files_have_explicit_private_modes(self):
        previous_umask = os.umask(0)
        try:
            with SecureDirectory.open(self.root) as root:
                with root.ensure_directory("events") as events:
                    events.write_immutable("one.json", b"one\n")
                    events.replace_derived("state.json", b"state-1\n")
        finally:
            os.umask(previous_umask)

        self.assertEqual(0o700, stat.S_IMODE((self.root / "events").stat().st_mode))
        self.assertEqual(
            0o600,
            stat.S_IMODE((self.root / "events" / "one.json").stat().st_mode),
        )
        self.assertEqual(
            0o600,
            stat.S_IMODE((self.root / "events" / "state.json").stat().st_mode),
        )

    def test_single_components_are_validated_before_filesystem_access(self):
        invalid = ("", ".", "..", "a/b", "a\\b", "/absolute", "bad\0name", "line\n")
        with SecureDirectory.open(self.root) as root:
            for component in invalid:
                with self.subTest(component=component):
                    with self.assertRaisesRegex(SecureFsError, "INVALID_COMPONENT"):
                        root.ensure_directory(component)
                    with self.assertRaisesRegex(SecureFsError, "INVALID_COMPONENT"):
                        root.write_immutable(component, b"data")

    def test_root_parent_child_and_leaf_symlinks_are_rejected(self):
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir(mode=0o700)
        (outside / "sentinel").write_bytes(b"outside")

        root_link = Path(self.temporary.name) / "root-link"
        root_link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
            SecureDirectory.open(root_link)

        parent_link = Path(self.temporary.name) / "parent-link"
        parent_link.symlink_to(Path(self.temporary.name), target_is_directory=True)
        with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
            SecureDirectory.open(parent_link / "root")

        (self.root / "escape").symlink_to(outside, target_is_directory=True)
        (self.root / "leaf.json").symlink_to(outside / "sentinel")
        with SecureDirectory.open(self.root) as root:
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
                root.open_directory("escape")
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
                root.read_bytes("leaf.json", max_bytes=64)
            with self.assertRaisesRegex(SecureFsError, "IMMUTABLE_EXISTS"):
                root.write_immutable("leaf.json", b"replacement")
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
                root.replace_derived("leaf.json", b"replacement")
        self.assertEqual(b"outside", (outside / "sentinel").read_bytes())

    def test_owned_but_non_private_child_directories_are_rejected(self):
        child = self.root / "shared"
        child.mkdir(mode=0o700)
        child.chmod(0o755)

        with SecureDirectory.open(self.root) as root:
            with self.assertRaisesRegex(SecureFsError, "INSECURE_PERMISSIONS"):
                root.open_directory("shared")
            with self.assertRaisesRegex(SecureFsError, "INSECURE_PERMISSIONS"):
                root.ensure_directory("shared")

    def test_open_directory_stays_anchored_if_lexical_root_is_replaced(self):
        outside = Path(self.temporary.name) / "outside-anchor"
        outside.mkdir(mode=0o700)
        anchored = Path(self.temporary.name) / "original-root"
        root = SecureDirectory.open(self.root)
        self.root.rename(anchored)
        self.root.symlink_to(outside, target_is_directory=True)
        try:
            root.write_immutable("event.json", b"anchored")
        finally:
            root.close()
        self.assertEqual(b"anchored", (anchored / "event.json").read_bytes())
        self.assertFalse((outside / "event.json").exists())

    def test_bounded_read_accepts_only_unchanged_regular_private_files(self):
        with SecureDirectory.open(self.root) as root:
            root.write_immutable("record.json", b"12345")
            self.assertEqual(b"12345", root.read_bytes("record.json", max_bytes=5))
            with self.assertRaisesRegex(SecureFsError, "FILE_TOO_LARGE"):
                root.read_bytes("record.json", max_bytes=4)

            root.ensure_directory("directory").close()
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_FILE_TYPE"):
                root.read_bytes("directory", max_bytes=64)

            os.mkfifo(self.root / "pipe", mode=0o600)
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_FILE_TYPE"):
                root.read_bytes("pipe", max_bytes=64)

    def test_immutable_create_never_replaces_and_fsyncs_file_and_directory(self):
        real_fsync = os.fsync
        with SecureDirectory.open(self.root) as root:
            with mock.patch.object(secure_fs.os, "fsync", wraps=real_fsync) as fsync:
                root.write_immutable("event.json", b"first")
                self.assertGreaterEqual(fsync.call_count, 2)
            with self.assertRaisesRegex(SecureFsError, "IMMUTABLE_EXISTS"):
                root.write_immutable("event.json", b"second")
            self.assertEqual(b"first", root.read_bytes("event.json", max_bytes=64))

    def test_guarded_immutable_stages_then_cleans_up_a_rejected_publish(self):
        staged_names: tuple[str, ...] = ()

        with SecureDirectory.open(self.root) as root:

            def reject_publish() -> None:
                nonlocal staged_names
                staged_names = root.list_entries()
                raise RuntimeError("GUARD_REJECTED")

            with self.assertRaisesRegex(RuntimeError, "GUARD_REJECTED"):
                root.write_immutable_guarded(
                    "event.json",
                    b"event",
                    reject_publish,
                )

            self.assertEqual(1, len(staged_names))
            self.assertRegex(staged_names[0], r"^tmp-[0-9a-f]{32}$")
            self.assertEqual((), root.list_entries())

    def test_guarded_immutable_publishes_only_after_its_guard_returns(self):
        guard_calls = 0

        with SecureDirectory.open(self.root) as root:

            def allow_publish() -> None:
                nonlocal guard_calls
                guard_calls += 1
                self.assertEqual(1, len(root.list_entries()))

            root.write_immutable_guarded(
                "event.json",
                b"event",
                allow_publish,
            )

            self.assertEqual(1, guard_calls)
            self.assertEqual(("event.json",), root.list_entries())
            self.assertEqual(
                b"event",
                root.read_bytes("event.json", max_bytes=64),
            )

            with self.assertRaisesRegex(
                SecureFsError,
                "INVALID_PUBLICATION_GUARD",
            ):
                root.write_immutable_guarded(
                    "invalid.json",
                    b"event",
                    None,  # type: ignore[arg-type]
                )

    def test_internal_temporary_write_recovery_is_narrow_and_durable(self):
        with SecureDirectory.open(self.root) as root:
            orphan = "tmp-" + "a" * 32
            published = "tmp-" + "b" * 32
            (self.root / orphan).write_bytes(b"orphan")
            (self.root / published).write_bytes(b"committed")
            os.chmod(self.root / orphan, 0o600)
            os.chmod(self.root / published, 0o600)
            os.link(self.root / published, self.root / "event.json")

            real_fsync = os.fsync
            with mock.patch.object(secure_fs.os, "fsync", wraps=real_fsync) as fsync:
                root.recover_temporary_writes()
                self.assertGreaterEqual(fsync.call_count, 1)

            self.assertFalse((self.root / orphan).exists())
            self.assertFalse((self.root / published).exists())
            self.assertEqual(
                b"committed",
                root.read_bytes("event.json", max_bytes=64),
            )

            unsafe = "tmp-" + "c" * 32
            (self.root / unsafe).mkdir(mode=0o700)
            with self.assertRaisesRegex(
                SecureFsError,
                "UNSAFE_TEMPORARY_FILE",
            ):
                root.recover_temporary_writes()

    def test_temporary_recovery_rejects_two_internal_names_for_one_inode(self):
        first = self.root / ("tmp-" + "d" * 32)
        second = self.root / ("tmp-" + "e" * 32)
        first.write_bytes(b"ambiguous")
        first.chmod(0o600)
        os.link(first, second)

        with SecureDirectory.open(self.root) as root:
            with self.assertRaisesRegex(
                SecureFsError,
                "UNSAFE_TEMPORARY_FILE",
            ):
                root.recover_temporary_writes()

        self.assertTrue(first.exists())
        self.assertTrue(second.exists())

    def test_derived_replace_is_atomic_private_and_fsyncs(self):
        real_fsync = os.fsync
        with SecureDirectory.open(self.root) as root:
            root.replace_derived("state.json", b"before")
            with mock.patch.object(secure_fs.os, "fsync", wraps=real_fsync) as fsync:
                root.replace_derived("state.json", b"after")
                self.assertGreaterEqual(fsync.call_count, 2)
            self.assertEqual(b"after", root.read_bytes("state.json", max_bytes=64))
        self.assertEqual(0o600, stat.S_IMODE((self.root / "state.json").stat().st_mode))

    def test_failed_derived_rename_cleans_up_the_private_temporary(self):
        with SecureDirectory.open(self.root) as root:
            with mock.patch.object(
                secure_fs.os,
                "rename",
                side_effect=OSError(errno.EIO, "injected"),
            ):
                with self.assertRaisesRegex(SecureFsError, "FILESYSTEM_ERROR"):
                    root.replace_derived("state.json", b"state")

            self.assertEqual((), root.list_entries())

    def test_post_write_metadata_validation_removes_an_unsafe_temporary(self):
        unsafe_metadata = mock.Mock(
            st_mode=stat.S_IFREG | 0o644,
            st_uid=os.geteuid(),
            st_nlink=1,
        )
        with SecureDirectory.open(self.root) as root:
            with mock.patch.object(
                secure_fs.os,
                "fstat",
                return_value=unsafe_metadata,
            ), self.assertRaisesRegex(SecureFsError, "UNSAFE_FILE_TYPE"):
                root.write_immutable("event.json", b"event")

            self.assertEqual((), root.list_entries())

    def test_fsync_failure_is_never_reported_as_success(self):
        with SecureDirectory.open(self.root) as root:
            with mock.patch.object(
                secure_fs.os,
                "fsync",
                side_effect=OSError("injected"),
            ):
                with self.assertRaisesRegex(
                    SecureFsError,
                    "FS_DURABILITY_FAILED",
                ):
                    root.write_immutable("event.json", b"event")
        self.assertFalse((self.root / "event.json").exists())

    def test_bounded_read_detects_leaf_identity_replacement(self):
        with SecureDirectory.open(self.root) as root:
            root.write_immutable("record.json", b"record")
            root.write_immutable("replacement.json", b"replacement")
            real_stat = os.stat

            def substituted_stat(path, *args, **kwargs):
                if path == "record.json":
                    return real_stat("replacement.json", *args, **kwargs)
                return real_stat(path, *args, **kwargs)

            with mock.patch.object(
                secure_fs.os,
                "stat",
                side_effect=substituted_stat,
            ):
                with self.assertRaisesRegex(
                    SecureFsError,
                    "FILE_CHANGED_DURING_READ",
                ):
                    root.read_bytes("record.json", max_bytes=64)

    def test_stat_rejects_non_private_regular_files_and_directories(self):
        regular = self.root / "record.json"
        directory = self.root / "child"
        regular.write_bytes(b"record")
        directory.mkdir(mode=0o700)
        regular.chmod(0o644)
        directory.chmod(0o755)

        with SecureDirectory.open(self.root) as root:
            for name in ("record.json", "child"):
                with self.subTest(name=name), self.assertRaisesRegex(
                    SecureFsError,
                    "INSECURE_PERMISSIONS",
                ):
                    root.stat_entry(name)

    def test_list_and_stat_are_sorted_anchored_and_never_follow_entries(self):
        outside = Path(self.temporary.name) / "listed-outside"
        outside.write_bytes(b"outside")
        with SecureDirectory.open(self.root) as root:
            root.write_immutable("z.json", b"z")
            root.write_immutable("a.json", b"abc")
            root.ensure_directory("child").close()
            (self.root / "link.json").symlink_to(outside)
            self.assertEqual(
                ("a.json", "child", "link.json", "z.json"),
                root.list_entries(),
            )
            regular = root.stat_entry("a.json")
            directory = root.stat_entry("child")
            self.assertEqual(EntryKind.REGULAR, regular.kind)
            self.assertEqual(3, regular.size)
            self.assertEqual(EntryKind.DIRECTORY, directory.kind)
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
                root.stat_entry("link.json")

    def test_quarantine_moves_one_regular_file_without_replacement(self):
        outside = Path(self.temporary.name) / "quarantine-outside"
        outside.write_bytes(b"outside")
        with SecureDirectory.open(self.root) as root:
            with root.ensure_directory("quarantine") as quarantine:
                root.write_immutable("orphan.json", b"orphan")
                root.quarantine(
                    "orphan.json",
                    destination=quarantine,
                    destination_component="orphan.json",
                )
                self.assertNotIn("orphan.json", root.list_entries())
                self.assertEqual(
                    b"orphan",
                    quarantine.read_bytes("orphan.json", max_bytes=64),
                )

                root.write_immutable("second.json", b"second")
                with self.assertRaisesRegex(SecureFsError, "IMMUTABLE_EXISTS"):
                    root.quarantine(
                        "second.json",
                        destination=quarantine,
                        destination_component="orphan.json",
                    )
                self.assertEqual(
                    b"second",
                    root.read_bytes("second.json", max_bytes=64),
                )

                root.write_immutable("duplicate.json", b"orphan")
                root.quarantine(
                    "duplicate.json",
                    destination=quarantine,
                    destination_component="orphan.json",
                )
                self.assertNotIn("duplicate.json", root.list_entries())
                self.assertEqual(
                    b"orphan",
                    quarantine.read_bytes("orphan.json", max_bytes=64),
                )

                (self.root / "link.json").symlink_to(outside)
                with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
                    root.quarantine(
                        "link.json",
                        destination=quarantine,
                        destination_component="link.json",
                    )
        self.assertEqual(b"outside", outside.read_bytes())

    def test_quarantine_within_one_directory_avoids_a_redundant_sync(self):
        with SecureDirectory.open(self.root) as root:
            root.write_immutable("source.json", b"source")
            real_fsync = os.fsync
            with mock.patch.object(
                secure_fs.os,
                "fsync",
                wraps=real_fsync,
            ) as fsync:
                root.quarantine(
                    "source.json",
                    destination=root,
                    destination_component="moved.json",
                )
                self.assertEqual(1, fsync.call_count)

            self.assertNotIn("source.json", root.list_entries())
            self.assertEqual(b"source", root.read_bytes("moved.json", max_bytes=64))

    def test_quarantine_detects_a_changed_link_and_removes_its_partial_peer(self):
        with SecureDirectory.open(self.root) as root:
            with root.ensure_directory("quarantine") as quarantine:
                root.write_immutable("source.json", b"source")
                real_stat = os.stat

                def changed_destination(path, *args, **kwargs):
                    metadata = real_stat(path, *args, **kwargs)
                    if path != "moved.json":
                        return metadata
                    return mock.Mock(
                        st_mode=metadata.st_mode,
                        st_uid=metadata.st_uid,
                        st_nlink=metadata.st_nlink,
                        st_dev=metadata.st_dev,
                        st_ino=metadata.st_ino + 1,
                    )

                with mock.patch.object(
                    secure_fs.os,
                    "stat",
                    side_effect=changed_destination,
                ), self.assertRaisesRegex(
                    SecureFsError,
                    "FILE_CHANGED_DURING_MOVE",
                ):
                    root.quarantine(
                        "source.json",
                        destination=quarantine,
                        destination_component="moved.json",
                    )

                self.assertIn("source.json", root.list_entries())
                self.assertNotIn("moved.json", quarantine.list_entries())

    def test_interrupted_quarantine_hard_link_is_finished_on_recovery(self):
        with SecureDirectory.open(self.root) as root:
            with root.ensure_directory("quarantine") as quarantine:
                root.write_immutable("event.json", b"event")
                quarantine.write_immutable("a-unrelated.json", b"unrelated")
                os.link(
                    self.root / "event.json",
                    self.root / "quarantine" / "orphan.json",
                )
                root.recover_quarantine_moves(quarantine)
                self.assertNotIn("event.json", root.list_entries())
                self.assertEqual(
                    b"event",
                    quarantine.read_bytes("orphan.json", max_bytes=64),
                )

    def test_quarantine_recovery_rejects_ambiguous_link_topologies(self):
        peer_directory = self.root / "peer"
        peer_directory.mkdir(mode=0o700)
        with SecureDirectory.open(self.root) as root:
            with root.ensure_directory("quarantine") as quarantine:
                root.write_immutable("unmatched.json", b"unmatched")
                os.link(
                    self.root / "unmatched.json",
                    peer_directory / "external-peer.json",
                )
                with self.assertRaisesRegex(
                    SecureFsError,
                    "UNSAFE_QUARANTINE_MOVE",
                ):
                    root.recover_quarantine_moves(quarantine)

                os.unlink(peer_directory / "external-peer.json")
                root.write_immutable("too-many.json", b"too-many")
                os.link(
                    self.root / "too-many.json",
                    self.root / "quarantine" / "first.json",
                )
                os.link(
                    self.root / "too-many.json",
                    self.root / "quarantine" / "second.json",
                )
                with self.assertRaisesRegex(
                    SecureFsError,
                    "UNSAFE_QUARANTINE_MOVE",
                ):
                    root.recover_quarantine_moves(quarantine)

    def test_os_error_translation_has_a_stable_generic_fallback(self):
        translated = secure_fs._translate_os_error(
            OSError(errno.EACCES, "sensitive path omitted")
        )
        self.assertEqual("FILESYSTEM_ERROR", translated.code)
        self.assertEqual("FILESYSTEM_ERROR", str(translated))

    def test_insecure_existing_directory_and_closed_handles_fail_closed(self):
        insecure = self.root / "insecure"
        insecure.mkdir(mode=0o755)
        os.chmod(insecure, 0o755)
        root = SecureDirectory.open(self.root)
        with self.assertRaisesRegex(SecureFsError, "INSECURE_PERMISSIONS"):
            root.ensure_directory("insecure")
        root.close()
        with self.assertRaisesRegex(SecureFsError, "DIRECTORY_CLOSED"):
            root.read_bytes("anything", max_bytes=1)

    def test_missing_required_platform_primitives_fail_closed(self):
        with mock.patch.object(secure_fs, "_O_NOFOLLOW", 0):
            with self.assertRaisesRegex(
                SecureFsError,
                "SECURE_PRIMITIVES_UNAVAILABLE",
            ):
                SecureDirectory.open(self.root)

    def test_path_file_and_read_limit_error_codes_are_stable(self):
        for unsafe_root in ("", ".", "relative//root", "bad\\root", b"bytes"):
            with self.subTest(root=unsafe_root), self.assertRaisesRegex(
                SecureFsError, "UNSAFE_PATH"
            ):
                SecureDirectory.open(unsafe_root)

        with SecureDirectory.open(self.root) as root:
            for component in ("x" * 256, "\ud800"):
                with self.subTest(
                    component=repr(component)
                ), self.assertRaisesRegex(SecureFsError, "INVALID_COMPONENT"):
                    root.stat_entry(component)
            for limit in (0, -1, True, None):
                with self.subTest(limit=limit), self.assertRaisesRegex(
                    SecureFsError, "INVALID_READ_LIMIT"
                ):
                    root.read_bytes("missing", max_bytes=limit)
            with self.assertRaisesRegex(SecureFsError, "DIRECTORY_NOT_FOUND"):
                root.open_directory("missing")
            with self.assertRaisesRegex(SecureFsError, "FILE_NOT_FOUND"):
                root.read_bytes("missing", max_bytes=1)
            with self.assertRaisesRegex(SecureFsError, "FILE_NOT_FOUND"):
                root.stat_entry("missing")
            with self.assertRaisesRegex(SecureFsError, "INVALID_FILE_CONTENT"):
                root.write_immutable("bad.json", "not-bytes")

        root.close()
        root.close()

    def test_insecure_and_special_existing_entries_fail_closed(self):
        insecure_file = self.root / "insecure.json"
        insecure_file.write_bytes(b"insecure")
        os.chmod(insecure_file, 0o644)
        directory = self.root / "directory"
        directory.mkdir(mode=0o700)
        fifo = self.root / "fifo"
        os.mkfifo(fifo, mode=0o600)

        with SecureDirectory.open(self.root) as root:
            for operation in (
                lambda: root.read_bytes("insecure.json", max_bytes=64),
                lambda: root.stat_entry("insecure.json"),
                lambda: root.replace_derived("insecure.json", b"new"),
            ):
                with self.assertRaisesRegex(
                    SecureFsError, "INSECURE_PERMISSIONS"
                ):
                    operation()
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_FILE_TYPE"):
                root.replace_derived("directory", b"new")
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_FILE_TYPE"):
                root.stat_entry("fifo")
            with self.assertRaisesRegex(SecureFsError, "UNSAFE_PATH"):
                root.ensure_directory("insecure.json")

    def test_temporary_and_quarantine_recovery_reject_ambiguous_state(self):
        temporary = self.root / ("tmp-" + "d" * 32)
        temporary.write_bytes(b"ambiguous")
        os.chmod(temporary, 0o600)
        os.link(temporary, self.root / "first.json")
        os.link(temporary, self.root / "second.json")
        with SecureDirectory.open(self.root) as root:
            with self.assertRaisesRegex(
                SecureFsError, "UNSAFE_TEMPORARY_FILE"
            ):
                root.recover_temporary_writes()
            with root.ensure_directory("quarantine") as quarantine:
                with self.assertRaisesRegex(
                    SecureFsError, "INVALID_QUARANTINE_TARGET"
                ):
                    root.quarantine(
                        "first.json",
                        destination=root,
                        destination_component="first.json",
                    )
                with self.assertRaisesRegex(
                    SecureFsError, "INVALID_QUARANTINE_DIRECTORY"
                ):
                    root.recover_quarantine_moves(object())
                with self.assertRaisesRegex(
                    SecureFsError, "UNSAFE_FILE_TYPE"
                ):
                    root.quarantine(
                        "quarantine",
                        destination=quarantine,
                        destination_component="directory",
                    )

    def test_write_loop_and_temporary_name_failures_leave_no_record(self):
        with SecureDirectory.open(self.root) as root:
            existing = "tmp-" + "e" * 32
            (self.root / existing).write_bytes(b"occupied")
            os.chmod(self.root / existing, 0o600)
            with mock.patch.object(
                SecureDirectory, "_temporary_name", return_value=existing
            ), self.assertRaisesRegex(
                SecureFsError, "TEMPORARY_NAME_EXHAUSTED"
            ):
                root.write_immutable("never.json", b"value")

            with mock.patch.object(
                secure_fs.os, "write", return_value=0
            ), self.assertRaisesRegex(SecureFsError, "FILESYSTEM_ERROR"):
                root.write_immutable("short.json", b"value")
            self.assertFalse((self.root / "never.json").exists())
            self.assertFalse((self.root / "short.json").exists())


if __name__ == "__main__":
    unittest.main()
