from __future__ import annotations

import errno
import hashlib
import multiprocessing
import os
import select
import stat
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

import tests.xsync_v2_path  # noqa: F401

from xsync_v2 import locking
from xsync_v2.locking import (
    DomainLockManager,
    LockError,
    RegistryLockAuthority,
    RegistryLockMode,
    SessionLockAuthority,
)


def _hold_registry_lock(
    lock_directory: str,
    exclusive: bool,
    ready: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
) -> None:
    manager = DomainLockManager(lock_directory)
    context = (
        manager.registry_exclusive()
        if exclusive
        else manager.registry_shared()
    )
    with context:
        ready.set()
        if not release.wait(10):
            raise RuntimeError("TEST_COORDINATION_TIMEOUT")
    manager.close()


def _crash_holding_session_lock(
    lock_directory: str,
    session_id: str,
    ready: multiprocessing.synchronize.Event,
    crash: multiprocessing.synchronize.Event,
) -> None:
    manager = DomainLockManager(lock_directory)
    with manager.semantic_session(session_id):
        ready.set()
        if not crash.wait(10):
            raise RuntimeError("TEST_COORDINATION_TIMEOUT")
        os._exit(23)


def _hold_lock_across_file_replacement(
    lock_directory: str,
    lock_kind: str,
    ready: multiprocessing.synchronize.Event,
    validate: multiprocessing.synchronize.Event,
    result: multiprocessing.queues.Queue,
) -> None:
    manager = DomainLockManager(lock_directory)
    context = (
        manager.registry_exclusive()
        if lock_kind == "registry"
        else manager.semantic_session("dlg-replaced")
    )
    try:
        with context as authority:
            ready.set()
            if not validate.wait(10):
                raise RuntimeError("TEST_COORDINATION_TIMEOUT")
            try:
                if lock_kind == "registry":
                    authority.assert_registry(RegistryLockMode.EXCLUSIVE)
                else:
                    authority.assert_can_commit("dlg-replaced")
            except LockError as exc:
                result.put(exc.code)
            else:
                result.put("AUTHORITY_REMAINED_VALID")
    finally:
        manager.close()


def _try_lock_after_file_replacement(
    lock_directory: str,
    lock_kind: str,
    result: multiprocessing.queues.Queue,
) -> None:
    manager = DomainLockManager(lock_directory)
    context = (
        manager.registry_exclusive(blocking=False)
        if lock_kind == "registry"
        else manager.semantic_session("dlg-replaced", blocking=False)
    )
    try:
        try:
            with context as authority:
                if lock_kind == "registry":
                    authority.assert_registry(RegistryLockMode.EXCLUSIVE)
                else:
                    authority.assert_can_commit("dlg-replaced")
        except LockError as exc:
            result.put(exc.code)
        else:
            result.put("AUTHORITY_GRANTED")
    finally:
        manager.close()


class LockingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.temporary_directory.cleanup)
        self.lock_directory = Path(self.temporary_directory.name) / "locks"

    def manager(self) -> DomainLockManager:
        manager = DomainLockManager(self.lock_directory)
        self.addCleanup(manager.close)
        return manager

    def assert_lock_error(self, code: str, callback) -> None:
        with self.assertRaises(LockError) as raised:
            callback()
        self.assertEqual(code, raised.exception.code)
        self.assertEqual(code, str(raised.exception))

    def test_registry_shared_and_exclusive_are_cross_process(self) -> None:
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        release = context.Event()
        process = context.Process(
            target=_hold_registry_lock,
            args=(str(self.lock_directory), False, ready, release),
        )
        process.start()
        self.assertTrue(ready.wait(10))

        manager = self.manager()
        with manager.registry_shared(blocking=False) as authority:
            authority.assert_registry(RegistryLockMode.SHARED)
        self.assert_lock_error(
            "LOCK_BUSY",
            lambda: manager.registry_exclusive(blocking=False).__enter__(),
        )

        release.set()
        process.join(10)
        self.assertFalse(process.is_alive())
        self.assertEqual(0, process.exitcode)
        with manager.registry_exclusive(blocking=False) as authority:
            authority.assert_registry(RegistryLockMode.EXCLUSIVE)

    def test_registry_exclusive_blocks_readers_in_another_process(self) -> None:
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        release = context.Event()
        process = context.Process(
            target=_hold_registry_lock,
            args=(str(self.lock_directory), True, ready, release),
        )
        process.start()
        self.assertTrue(ready.wait(10))

        manager = self.manager()
        self.assert_lock_error(
            "LOCK_BUSY",
            lambda: manager.registry_shared(blocking=False).__enter__(),
        )
        release.set()
        process.join(10)
        self.assertFalse(process.is_alive())
        self.assertEqual(0, process.exitcode)

    def test_session_exclusive_is_released_when_owner_process_crashes(self) -> None:
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        crash = context.Event()
        process = context.Process(
            target=_crash_holding_session_lock,
            args=(str(self.lock_directory), "dlg-1", ready, crash),
        )
        process.start()
        self.assertTrue(ready.wait(10))

        manager = self.manager()
        with manager.semantic_session("dlg-2", blocking=False) as authority:
            authority.assert_can_commit("dlg-2")
        self.assert_lock_error(
            "LOCK_BUSY",
            lambda: manager.semantic_session(
                "dlg-1", blocking=False
            ).__enter__(),
        )
        crash.set()
        process.join(10)
        self.assertFalse(process.is_alive())
        self.assertEqual(23, process.exitcode)

        with manager.semantic_session("dlg-1", blocking=False) as authority:
            authority.assert_can_commit("dlg-1")

    def test_registry_to_session_order_and_non_reentrancy_are_enforced(self) -> None:
        manager = self.manager()
        self.assert_lock_error(
            "LOCK_ORDER_VIOLATION",
            lambda: manager.session_exclusive(
                "dlg-1", None, blocking=False
            ).__enter__(),
        )

        with manager.registry_shared() as registry:
            self.assert_lock_error(
                "LOCK_ORDER_VIOLATION",
                lambda: manager.registry_shared(blocking=False).__enter__(),
            )
            with manager.session_exclusive("dlg-1", registry) as session:
                session.assert_can_commit("dlg-1")
                self.assert_lock_error(
                    "LOCK_ORDER_VIOLATION",
                    lambda: manager.registry_exclusive(
                        blocking=False
                    ).__enter__(),
                )
                self.assert_lock_error(
                    "LOCK_ORDER_VIOLATION",
                    lambda: manager.session_exclusive(
                        "dlg-2", registry, blocking=False
                    ).__enter__(),
                )

        manager.assert_none_held()

    def test_authorities_are_scoped_exact_and_invalid_after_release(self) -> None:
        manager = self.manager()
        with manager.registry_exclusive() as registry:
            registry.assert_valid()
            registry.assert_registry(RegistryLockMode.SHARED)
            with manager.session_exclusive("dlg-1", registry) as session:
                session.assert_valid()
                session.assert_registry(RegistryLockMode.EXCLUSIVE)
                session.assert_session("dlg-1")
                session.assert_can_commit("dlg-1")
            self.assert_lock_error("LOCK_AUTHORITY_INVALID", session.assert_valid)
        self.assert_lock_error("LOCK_AUTHORITY_INVALID", registry.assert_valid)

        with manager.registry_shared() as shared:
            self.assert_lock_error(
                "LOCK_AUTHORITY_INSUFFICIENT",
                lambda: shared.assert_registry(RegistryLockMode.EXCLUSIVE),
            )

        self.assert_lock_error(
            "LOCK_AUTHORITY_CONSTRUCTION_FORBIDDEN",
            RegistryLockAuthority,
        )

    def test_authority_properties_modes_and_session_binding_are_exact(self) -> None:
        manager = self.manager()
        with manager.semantic_session(
            "dlg-1",
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as session:
            registry = session.registry
            self.assertEqual("dlg-1", session.session_id)
            self.assertEqual(RegistryLockMode.EXCLUSIVE, registry.mode)
            self.assertIs(registry, session.registry)
            manager.assert_session_authority(session, "dlg-1")
            manager.assert_registry_authority(
                registry,
                RegistryLockMode.EXCLUSIVE,
            )
            self.assert_lock_error(
                "LOCK_AUTHORITY_SESSION_MISMATCH",
                lambda: session.assert_session("dlg-2"),
            )
            self.assert_lock_error(
                "INVALID_SESSION_ID",
                lambda: session.assert_session(None),
            )
            self.assert_lock_error(
                "INVALID_LOCK_MODE",
                lambda: registry.assert_registry("exclusive"),
            )
            self.assert_lock_error(
                "INVALID_LOCK_MODE",
                lambda: session.assert_registry("exclusive"),
            )
            self.assert_lock_error(
                "LOCK_AUTHORITY_INVALID",
                lambda: manager.assert_session_authority(registry, "dlg-1"),
            )
            self.assert_lock_error(
                "LOCK_AUTHORITY_INVALID",
                lambda: manager.assert_registry_authority(
                    registry,
                    "exclusive",
                ),
            )

        self.assert_lock_error(
            "LOCK_AUTHORITY_INVALID",
            lambda: session.session_id,
        )
        self.assert_lock_error(
            "LOCK_AUTHORITY_INVALID",
            lambda: registry.mode,
        )
        self.assert_lock_error(
            "LOCK_AUTHORITY_CONSTRUCTION_FORBIDDEN",
            SessionLockAuthority,
        )

    def test_corrupted_authority_payloads_fail_closed(self) -> None:
        manager = self.manager()
        with manager.semantic_session("dlg-1") as session:
            registry = session.registry
            malformed_registry_lease = replace(
                registry._lease,
                registry_mode=None,
            )
            mismatched_registry_lease = replace(
                registry._lease,
                manager_token=object(),
            )
            malformed_session_lease = replace(
                session._lease,
                session_id=None,
            )
            internal_leases = (
                malformed_registry_lease,
                mismatched_registry_lease,
                malformed_session_lease,
            )
            with locking._ACTIVE_GUARD:
                for lease in internal_leases:
                    locking._ACTIVE_LEASES[id(lease)] = lease
            try:
                malformed_registry = RegistryLockAuthority(
                    locking._AUTHORITY_KEY,
                    malformed_registry_lease,
                )
                self.assert_lock_error(
                    "LOCK_AUTHORITY_INVALID",
                    lambda: malformed_registry.mode,
                )

                mismatched_registry = RegistryLockAuthority(
                    locking._AUTHORITY_KEY,
                    mismatched_registry_lease,
                )
                mixed_session = SessionLockAuthority(
                    locking._AUTHORITY_KEY,
                    session._lease,
                    mismatched_registry,
                )
                self.assert_lock_error(
                    "LOCK_AUTHORITY_INVALID",
                    mixed_session.assert_valid,
                )

                malformed_session = SessionLockAuthority(
                    locking._AUTHORITY_KEY,
                    malformed_session_lease,
                    registry,
                )
                self.assert_lock_error(
                    "LOCK_AUTHORITY_INVALID",
                    lambda: malformed_session.session_id,
                )
            finally:
                with locking._ACTIVE_GUARD:
                    for lease in internal_leases:
                        locking._ACTIVE_LEASES.pop(id(lease), None)

    def test_authority_is_bound_to_one_lock_namespace(self) -> None:
        manager = self.manager()
        foreign = DomainLockManager(
            Path(self.temporary_directory.name) / "other-locks"
        )
        self.addCleanup(foreign.close)
        with foreign.semantic_session("dlg-1") as authority:
            self.assert_lock_error(
                "LOCK_AUTHORITY_INVALID",
                lambda: manager.assert_session_authority(authority, "dlg-1"),
            )
        with foreign.registry_exclusive() as authority:
            self.assert_lock_error(
                "LOCK_AUTHORITY_INVALID",
                lambda: manager.assert_registry_authority(
                    authority,
                    RegistryLockMode.EXCLUSIVE,
                ),
            )

    def test_replaced_lock_directory_fences_the_old_manager(self) -> None:
        manager = self.manager()
        lock_path = Path(self.temporary_directory.name) / "locks"
        moved = Path(self.temporary_directory.name) / "moved-locks"
        lock_path.rename(moved)
        replacement = DomainLockManager(lock_path)
        self.addCleanup(replacement.close)

        with replacement.registry_exclusive():
            self.assert_lock_error(
                "LOCK_NAMESPACE_CHANGED",
                manager.registry_exclusive().__enter__,
            )

        live_path = Path(self.temporary_directory.name) / "live-locks"
        live = DomainLockManager(live_path)
        self.addCleanup(live.close)
        held = live.registry_exclusive()
        authority = held.__enter__()
        live_path.rename(Path(self.temporary_directory.name) / "live-moved")
        third = DomainLockManager(live_path)
        self.addCleanup(third.close)
        try:
            self.assert_lock_error(
                "LOCK_NAMESPACE_CHANGED",
                lambda: live.assert_registry_authority(
                    authority,
                    RegistryLockMode.EXCLUSIVE,
                ),
            )
        finally:
            held.__exit__(None, None, None)

    def test_replaced_registry_lock_file_fences_all_processes(self) -> None:
        self._assert_replaced_lock_file_fences_processes(
            "registry",
            "registry.lock",
        )

    def test_replaced_session_lock_file_fences_all_processes(self) -> None:
        digest = hashlib.sha256(b"dlg-replaced").hexdigest()
        self._assert_replaced_lock_file_fences_processes(
            "session",
            f"session-{digest}.lock",
        )

    def _assert_replaced_lock_file_fences_processes(
        self,
        lock_kind: str,
        lock_name: str,
    ) -> None:
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        validate = context.Event()
        holder_result = context.Queue()
        contender_result = context.Queue()
        holder = context.Process(
            target=_hold_lock_across_file_replacement,
            args=(
                str(self.lock_directory),
                lock_kind,
                ready,
                validate,
                holder_result,
            ),
        )
        holder.start()
        self.assertTrue(ready.wait(10))

        lock_path = self.lock_directory / lock_name
        lock_path.unlink()
        descriptor = os.open(
            lock_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        os.close(descriptor)
        lock_path.chmod(0o600)

        contender = context.Process(
            target=_try_lock_after_file_replacement,
            args=(
                str(self.lock_directory),
                lock_kind,
                contender_result,
            ),
        )
        contender.start()
        contender.join(10)
        self.assertFalse(contender.is_alive())
        self.assertEqual(0, contender.exitcode)
        self.assertEqual("LOCK_NAMESPACE_CHANGED", contender_result.get(10))

        validate.set()
        holder.join(10)
        self.assertFalse(holder.is_alive())
        self.assertEqual(0, holder.exitcode)
        self.assertEqual("LOCK_NAMESPACE_CHANGED", holder_result.get(10))

    def test_authority_cannot_be_used_from_another_thread(self) -> None:
        manager = self.manager()
        errors = []
        with manager.registry_shared() as authority:
            thread = threading.Thread(
                target=lambda: errors.append(self._authority_error(authority))
            )
            thread.start()
            thread.join(10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(["LOCK_AUTHORITY_INVALID"], errors)

    @staticmethod
    def _authority_error(authority: RegistryLockAuthority) -> str:
        try:
            authority.assert_valid()
        except LockError as exc:
            return exc.code
        return "NO_ERROR"

    @unittest.skipUnless(hasattr(os, "fork"), "requires POSIX fork")
    def test_fork_child_drops_inherited_leases_and_manager_ownership(self) -> None:
        manager = self.manager()
        read_descriptor, write_descriptor = os.pipe()
        with manager.registry_shared() as authority:
            child_pid = os.fork()
            if child_pid == 0:
                os.close(read_descriptor)
                results = []
                try:
                    authority.assert_valid()
                except LockError as exc:
                    results.append(exc.code)
                try:
                    manager.registry_shared().__enter__()
                except LockError as exc:
                    results.append(exc.code)
                try:
                    manager.close()
                except LockError as exc:
                    results.append(exc.code)
                os.write(write_descriptor, "|".join(results).encode("ascii"))
                os.close(write_descriptor)
                os._exit(0)

            os.close(write_descriptor)
            readable, _, _ = select.select([read_descriptor], [], [], 10)
            self.assertEqual([read_descriptor], readable)
            result = os.read(read_descriptor, 4096).decode("ascii")
            os.close(read_descriptor)
            waited_pid, status = os.waitpid(child_pid, 0)
            self.assertEqual(child_pid, waited_pid)
            self.assertTrue(os.WIFEXITED(status))
            self.assertEqual(0, os.WEXITSTATUS(status))
            self.assertEqual(
                "LOCK_AUTHORITY_INVALID|LOCK_MANAGER_PROCESS_MISMATCH|"
                "LOCK_MANAGER_PROCESS_MISMATCH",
                result,
            )
            authority.assert_valid()

    def test_manager_tracks_locks_for_observer_boundary(self) -> None:
        manager = self.manager()
        with manager.registry_shared():
            self.assert_lock_error("DOMAIN_LOCKS_HELD", manager.assert_none_held)
        manager.assert_none_held()

    def test_lock_directory_and_files_have_fixed_private_modes(self) -> None:
        previous_umask = os.umask(0o777)
        try:
            manager = self.manager()
            with manager.semantic_session("dlg-1"):
                pass
        finally:
            os.umask(previous_umask)

        self.assertEqual(
            0o700,
            stat.S_IMODE(self.lock_directory.stat().st_mode),
        )
        lock_files = tuple(self.lock_directory.iterdir())
        self.assertEqual(4, len(lock_files))
        self.assertTrue(
            all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in lock_files)
        )
        self.assertTrue(all(path.stat().st_nlink == 2 for path in lock_files))

    def test_symlinks_and_unsafe_existing_lock_files_fail_closed(self) -> None:
        outside = Path(self.temporary_directory.name) / "outside"
        outside.mkdir(mode=0o700)
        sentinel = outside / "sentinel"
        sentinel.write_text("unchanged", encoding="utf-8")
        symlink_parent = Path(self.temporary_directory.name) / "linked-parent"
        symlink_parent.symlink_to(outside, target_is_directory=True)
        self.assert_lock_error(
            "UNSAFE_LOCK_DIRECTORY",
            lambda: DomainLockManager(symlink_parent / "locks"),
        )
        self.assertFalse((outside / "locks").exists())
        self.assertEqual("unchanged", sentinel.read_text(encoding="utf-8"))

        symlink_directory = Path(self.temporary_directory.name) / "linked-locks"
        symlink_directory.symlink_to(outside, target_is_directory=True)
        self.assert_lock_error(
            "UNSAFE_LOCK_DIRECTORY",
            lambda: DomainLockManager(symlink_directory),
        )

        self.lock_directory.mkdir(mode=0o700)
        (self.lock_directory / "registry.lock").symlink_to(sentinel)
        manager = self.manager()
        self.assert_lock_error(
            "UNSAFE_LOCK_FILE",
            lambda: manager.registry_shared().__enter__(),
        )
        self.assertEqual("unchanged", sentinel.read_text(encoding="utf-8"))

        (self.lock_directory / "registry.lock").unlink()
        (self.lock_directory / "registry.lock").write_bytes(b"")
        (self.lock_directory / "registry.lock").chmod(0o644)
        self.assert_lock_error(
            "UNSAFE_LOCK_FILE",
            lambda: manager.registry_shared().__enter__(),
        )

    def test_invalid_inputs_and_closed_manager_have_stable_errors(self) -> None:
        manager = self.manager()
        for session_id in (
            None,
            "",
            " ",
            ".",
            "..",
            "a/b",
            "a\\b",
            "a\x00b",
            "a\x1fb",
            "a\x7fb",
            "é" * 128,
            "\ud800",
        ):
            self.assert_lock_error(
                "INVALID_SESSION_ID",
                lambda session_id=session_id: manager.semantic_session(
                    session_id
                ).__enter__(),
            )
        self.assert_lock_error(
            "INVALID_BLOCKING_MODE",
            lambda: manager.registry_shared(blocking=1).__enter__(),
        )
        self.assert_lock_error(
            "INVALID_BLOCKING_MODE",
            lambda: manager.semantic_session(
                "dlg-1",
                blocking="yes",
            ).__enter__(),
        )
        self.assert_lock_error(
            "INVALID_LOCK_MODE",
            lambda: manager.semantic_session(
                "dlg-1",
                registry_mode="shared",
            ).__enter__(),
        )
        manager.close()
        self.assert_lock_error(
            "LOCK_MANAGER_CLOSED",
            lambda: manager.registry_shared().__enter__(),
        )

    def test_invalid_lock_directories_have_stable_errors(self) -> None:
        invalid_directories = (
            object(),
            b"locks",
            "",
            "/",
            ".",
            "../locks",
            "relative//locks",
        )
        for directory in invalid_directories:
            with self.subTest(directory=directory):
                self.assert_lock_error(
                    "INVALID_LOCK_DIRECTORY",
                    lambda directory=directory: DomainLockManager(directory),
                )

        self.assertEqual(
            "relative/locks",
            DomainLockManager._validate_directory_path("relative/locks"),
        )
        with mock.patch.object(locking.os, "altsep", "\\"):
            self.assert_lock_error(
                "INVALID_LOCK_DIRECTORY",
                lambda: DomainLockManager._validate_directory_path(
                    "relative\\locks"
                ),
            )

    def test_manager_context_close_and_close_while_held(self) -> None:
        context_path = Path(self.temporary_directory.name) / "context-locks"
        manager = DomainLockManager(context_path)
        with manager as entered:
            self.assertIs(manager, entered)
        manager.close()
        self.assert_lock_error(
            "LOCK_MANAGER_CLOSED",
            manager.__enter__,
        )

        held_manager = self.manager()
        with held_manager.registry_shared():
            self.assert_lock_error("DOMAIN_LOCKS_HELD", held_manager.close)
        held_manager.close()

    def test_os_lock_retry_and_failure_translation_are_stable(self) -> None:
        interrupted = OSError(errno.EINTR, "interrupted")
        with mock.patch.object(
            locking.fcntl,
            "flock",
            side_effect=(interrupted, None),
        ) as flock:
            DomainLockManager._take_os_lock(91, locking.fcntl.LOCK_EX, True)
        self.assertEqual(2, flock.call_count)

        for error_number, expected in (
            (errno.EACCES, "LOCK_BUSY"),
            (errno.EAGAIN, "LOCK_BUSY"),
            (errno.EIO, "LOCK_ACQUIRE_FAILED"),
        ):
            with self.subTest(error_number=error_number), mock.patch.object(
                locking.fcntl,
                "flock",
                side_effect=OSError(error_number, "failure"),
            ):
                self.assert_lock_error(
                    expected,
                    lambda: DomainLockManager._take_os_lock(
                        91,
                        locking.fcntl.LOCK_EX,
                        False,
                    ),
                )

    def test_release_and_manager_close_failures_are_translated(self) -> None:
        manager = self.manager()
        registry_context = manager.registry_shared()
        authority = registry_context.__enter__()
        with mock.patch.object(
            locking.fcntl,
            "flock",
            side_effect=OSError(errno.EIO, "unlock failed"),
        ):
            self.assert_lock_error(
                "LOCK_RELEASE_FAILED",
                lambda: registry_context.__exit__(None, None, None),
            )
        self.assert_lock_error("LOCK_AUTHORITY_INVALID", authority.assert_valid)

        close_manager = self.manager()
        with close_manager.registry_shared():
            pass
        pinned_descriptor = next(
            iter(close_manager._pinned_lock_files.values())
        ).descriptor
        real_close = os.close

        def fail_pinned_close(descriptor: int) -> None:
            if descriptor == pinned_descriptor:
                raise OSError(errno.EIO, "close failed")
            real_close(descriptor)

        with mock.patch.object(locking.os, "close", side_effect=fail_pinned_close):
            self.assert_lock_error(
                "LOCK_MANAGER_CLOSE_FAILED",
                close_manager.close,
            )
        real_close(pinned_descriptor)

        directory_close_manager = self.manager()
        directory_descriptor = directory_close_manager._directory_fd

        def fail_directory_close(descriptor: int) -> None:
            if descriptor == directory_descriptor:
                raise OSError(errno.EIO, "close failed")
            real_close(descriptor)

        with mock.patch.object(
            locking.os,
            "close",
            side_effect=fail_directory_close,
        ):
            self.assert_lock_error(
                "LOCK_MANAGER_CLOSE_FAILED",
                directory_close_manager.close,
            )
        real_close(directory_descriptor)

    def test_lock_directory_os_failures_are_translated(self) -> None:
        required_flags = DomainLockManager._required_os_flags()
        with (
            mock.patch.object(
                DomainLockManager,
                "_required_os_flags",
                return_value=required_flags,
            ),
            mock.patch.object(
                locking.os,
                "open",
                side_effect=OSError(errno.EACCES, "anchor denied"),
            ),
        ):
            self.assert_lock_error(
                "UNSAFE_LOCK_DIRECTORY",
                lambda: DomainLockManager(self.lock_directory),
            )

        mkdir_path = Path(self.temporary_directory.name) / "mkdir-fails"
        with (
            mock.patch.object(
                DomainLockManager,
                "_required_os_flags",
                return_value=required_flags,
            ),
            mock.patch.object(
                locking.os,
                "mkdir",
                side_effect=OSError(errno.EIO, "mkdir failed"),
            ),
        ):
            self.assert_lock_error(
                "LOCK_DIRECTORY_OPEN_FAILED",
                lambda: DomainLockManager(mkdir_path),
            )

        chmod_path = Path(self.temporary_directory.name) / "chmod-fails"
        with (
            mock.patch.object(
                DomainLockManager,
                "_required_os_flags",
                return_value=required_flags,
            ),
            mock.patch.object(
                locking.os,
                "chmod",
                side_effect=OSError(errno.EIO, "chmod failed"),
            ),
        ):
            self.assert_lock_error(
                "LOCK_DIRECTORY_OPEN_FAILED",
                lambda: DomainLockManager(chmod_path),
            )

        unsafe_path = Path(self.temporary_directory.name) / "unsafe-mode"
        unsafe_path.mkdir(mode=0o755)
        unsafe_path.chmod(0o755)
        self.assert_lock_error(
            "UNSAFE_LOCK_DIRECTORY",
            lambda: DomainLockManager(unsafe_path),
        )

    def test_namespace_and_lock_file_os_failures_are_translated(self) -> None:
        missing_manager = self.manager()
        self.lock_directory.rename(
            Path(self.temporary_directory.name) / "missing-locks"
        )
        self.assert_lock_error(
            "LOCK_NAMESPACE_CHANGED",
            missing_manager._verify_namespace_current,
        )

        stat_path = Path(self.temporary_directory.name) / "stat-locks"
        stat_manager = DomainLockManager(stat_path)
        self.addCleanup(stat_manager.close)
        required_flags = DomainLockManager._required_os_flags()
        with mock.patch.object(
            locking.os,
            "fstat",
            side_effect=OSError(errno.EIO, "fstat failed"),
        ):
            self.assert_lock_error(
                "LOCK_NAMESPACE_CHANGED",
                stat_manager._verify_namespace_current,
            )

        for error_number, expected in (
            (errno.ELOOP, "UNSAFE_LOCK_FILE"),
            (errno.EIO, "LOCK_FILE_OPEN_FAILED"),
        ):
            with (
                self.subTest(error_number=error_number),
                mock.patch.object(stat_manager, "_ensure_open"),
                mock.patch.object(
                    DomainLockManager,
                    "_required_os_flags",
                    return_value=required_flags,
                ),
                mock.patch.object(
                    locking.os,
                    "open",
                    side_effect=OSError(error_number, "open failed"),
                ),
            ):
                self.assert_lock_error(
                    expected,
                    lambda: stat_manager._open_named_lock_file(
                        "unit.lock",
                        create=False,
                    ),
                )

        with (
            mock.patch.object(stat_manager, "_ensure_open"),
            mock.patch.object(
                DomainLockManager,
                "_required_os_flags",
                return_value=required_flags,
            ),
            mock.patch.object(
                locking.os,
                "open",
                side_effect=(
                    FileExistsError(errno.EEXIST, "exists"),
                    OSError(errno.EIO, "open failed"),
                ),
            ),
        ):
            self.assert_lock_error(
                "LOCK_FILE_OPEN_FAILED",
                lambda: stat_manager._open_named_lock_file(
                    "unit.lock",
                    create=True,
                ),
            )

    def test_identity_pair_conflict_and_link_failure_fail_closed(self) -> None:
        conflict_directory = Path(self.temporary_directory.name) / "conflict"
        conflict_directory.mkdir(mode=0o700)
        for name in ("registry.lock", ".registry.lock.identity"):
            path = conflict_directory / name
            path.write_bytes(b"")
            path.chmod(0o600)
        conflict_manager = DomainLockManager(conflict_directory)
        self.addCleanup(conflict_manager.close)
        self.assert_lock_error(
            "LOCK_NAMESPACE_CHANGED",
            lambda: conflict_manager.registry_shared().__enter__(),
        )

        link_directory = Path(self.temporary_directory.name) / "link-failure"
        link_manager = DomainLockManager(link_directory)
        self.addCleanup(link_manager.close)
        required_flags = DomainLockManager._required_os_flags()
        with (
            mock.patch.object(
                DomainLockManager,
                "_required_os_flags",
                return_value=required_flags,
            ),
            mock.patch.object(
                locking.os,
                "link",
                side_effect=OSError(errno.EIO, "link failed"),
            ),
        ):
            self.assert_lock_error(
                "LOCK_FILE_OPEN_FAILED",
                lambda: link_manager.registry_shared().__enter__(),
            )

        identity_manager = self.manager()
        with identity_manager.registry_shared():
            pass
        (self.lock_directory / ".registry.lock.identity").unlink()
        self.assert_lock_error(
            "LOCK_NAMESPACE_CHANGED",
            lambda: identity_manager.registry_shared().__enter__(),
        )

    def test_missing_secure_platform_primitives_fail_closed(self) -> None:
        with mock.patch.object(locking.os, "O_NOFOLLOW", 0):
            self.assert_lock_error(
                "LOCKING_UNSUPPORTED",
                lambda: DomainLockManager(self.lock_directory),
            )


if __name__ == "__main__":
    unittest.main()
