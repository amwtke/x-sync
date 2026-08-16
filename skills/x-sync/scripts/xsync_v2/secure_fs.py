"""Symlink-safe, dir-fd anchored filesystem primitives for X-Sync v2."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import errno
from enum import StrEnum
import os
from os import PathLike
import re
import secrets
import stat


_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)
_DIRECTORY_MODE = 0o700
_FILE_MODE = 0o600
_MAX_COMPONENT_BYTES = 255
_TEMPORARY_NAME = re.compile(r"tmp-[0-9a-f]{32}\Z")


class SecureFsError(RuntimeError):
    """A stable, path-free secure-filesystem failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class EntryKind(StrEnum):
    REGULAR = "regular"
    DIRECTORY = "directory"


@dataclass(frozen=True, slots=True)
class SecureEntry:
    name: str
    kind: EntryKind
    size: int
    mode: int
    device: int
    inode: int


@dataclass(frozen=True, slots=True)
class SecureDirectoryIdentity:
    """Pinned device/inode identity for one owner-only directory handle."""

    device: int
    inode: int


def _require_secure_primitives() -> None:
    required_dir_fd = {os.open, os.mkdir, os.rename, os.stat, os.unlink, os.link}
    if (
        not _O_NOFOLLOW
        or not _O_CLOEXEC
        or not _O_DIRECTORY
        or not required_dir_fd.issubset(os.supports_dir_fd)
        or os.link not in os.supports_follow_symlinks
        or os.stat not in os.supports_follow_symlinks
    ):
        raise SecureFsError("SECURE_PRIMITIVES_UNAVAILABLE")


def _validate_component(component: object) -> str:
    if type(component) is not str or not component:
        raise SecureFsError("INVALID_COMPONENT")
    if (
        component in {".", ".."}
        or "/" in component
        or "\\" in component
        or any(ord(character) < 32 or ord(character) == 127 for character in component)
    ):
        raise SecureFsError("INVALID_COMPONENT")
    try:
        encoded = component.encode("utf-8")
    except UnicodeError as exc:
        raise SecureFsError("INVALID_COMPONENT") from exc
    if len(encoded) > _MAX_COMPONENT_BYTES:
        raise SecureFsError("INVALID_COMPONENT")
    return component


def _open_flags(*, directory: bool = False, write: bool = False) -> int:
    flags = (os.O_WRONLY if write else os.O_RDONLY) | _O_NOFOLLOW | _O_CLOEXEC
    if directory:
        flags |= _O_DIRECTORY
    return flags


def _translate_os_error(
    exc: OSError,
    *,
    missing: str = "FILE_NOT_FOUND",
) -> SecureFsError:
    if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
        return SecureFsError("UNSAFE_PATH")
    if exc.errno == errno.ENOENT:
        return SecureFsError(missing)
    if exc.errno == errno.EEXIST:
        return SecureFsError("IMMUTABLE_EXISTS")
    return SecureFsError("FILESYSTEM_ERROR")


def _durable_sync(descriptor: int) -> None:
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise SecureFsError("FS_DURABILITY_FAILED") from exc


def _directory_metadata_is_safe(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISDIR(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == _DIRECTORY_MODE
    )


def _regular_metadata_is_safe(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == _FILE_MODE
        and metadata.st_nlink == 1
    )


def _open_anchor(path: str | PathLike[str]) -> int:
    _require_secure_primitives()
    raw = os.fspath(path)
    if type(raw) is not str or not raw or "\0" in raw or "\\" in raw:
        raise SecureFsError("UNSAFE_PATH")
    absolute = raw.startswith("/")
    raw_parts = raw.split("/")
    parts = raw_parts[1:] if absolute else raw_parts
    if any(not part or part in {".", ".."} for part in parts):
        raise SecureFsError("UNSAFE_PATH")
    try:
        current = os.open("/" if absolute else ".", _open_flags(directory=True))
    except OSError as exc:
        raise _translate_os_error(exc, missing="UNSAFE_PATH") from exc
    try:
        for part in parts:
            _validate_component(part)
            try:
                next_descriptor = os.open(
                    part,
                    _open_flags(directory=True),
                    dir_fd=current,
                )
            except OSError as exc:
                raise _translate_os_error(exc, missing="UNSAFE_PATH") from exc
            os.close(current)
            current = next_descriptor
        metadata = os.fstat(current)
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
            raise SecureFsError("UNSAFE_PATH")
        return current
    except BaseException:
        os.close(current)
        raise


class SecureDirectory:
    """An owned directory descriptor used for all descendant operations."""

    def __init__(self, descriptor: int):
        self._descriptor = descriptor

    @classmethod
    def open(cls, path: str | PathLike[str]) -> SecureDirectory:
        """Open an existing anchor without following any path-component symlink."""
        return cls(_open_anchor(path))

    def __enter__(self) -> SecureDirectory:
        self._require_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def _require_open(self) -> int:
        descriptor = self._descriptor
        if descriptor < 0:
            raise SecureFsError("DIRECTORY_CLOSED")
        return descriptor

    def close(self) -> None:
        descriptor = getattr(self, "_descriptor", -1)
        if descriptor < 0:
            return
        self._descriptor = -1
        try:
            os.close(descriptor)
        except OSError:
            pass

    def identity(self) -> SecureDirectoryIdentity:
        """Return the live anchored directory identity without a path lookup."""
        try:
            metadata = os.fstat(self._require_open())
        except OSError as exc:
            raise SecureFsError("FILESYSTEM_ERROR") from exc
        if not _directory_metadata_is_safe(metadata):
            raise SecureFsError("INSECURE_PERMISSIONS")
        return SecureDirectoryIdentity(metadata.st_dev, metadata.st_ino)

    def open_directory(self, component: str) -> SecureDirectory:
        """Open one existing private child directory without following links."""
        name = _validate_component(component)
        parent = self._require_open()
        try:
            descriptor = os.open(
                name,
                _open_flags(directory=True),
                dir_fd=parent,
            )
        except OSError as exc:
            raise _translate_os_error(exc, missing="DIRECTORY_NOT_FOUND") from exc
        metadata = os.fstat(descriptor)
        if not _directory_metadata_is_safe(metadata):
            os.close(descriptor)
            if stat.S_ISDIR(metadata.st_mode):
                raise SecureFsError("INSECURE_PERMISSIONS")
            raise SecureFsError("UNSAFE_PATH")
        return SecureDirectory(descriptor)

    def ensure_directory(self, component: str) -> SecureDirectory:
        """Create or open one private child and durably materialize creation."""
        name = _validate_component(component)
        parent = self._require_open()
        created = False
        try:
            os.mkdir(name, _DIRECTORY_MODE, dir_fd=parent)
            created = True
        except FileExistsError:
            pass
        except OSError as exc:
            raise _translate_os_error(exc, missing="UNSAFE_PATH") from exc
        try:
            descriptor = os.open(
                name,
                _open_flags(directory=True),
                dir_fd=parent,
            )
        except OSError as exc:
            raise _translate_os_error(exc, missing="UNSAFE_PATH") from exc
        try:
            if created:
                try:
                    os.fchmod(descriptor, _DIRECTORY_MODE)
                except OSError as exc:
                    raise SecureFsError("FILESYSTEM_ERROR") from exc
            metadata = os.fstat(descriptor)
            if not _directory_metadata_is_safe(metadata):
                if stat.S_ISDIR(metadata.st_mode):
                    raise SecureFsError("INSECURE_PERMISSIONS")
                raise SecureFsError("UNSAFE_PATH")
            if created:
                _durable_sync(descriptor)
                _durable_sync(parent)
            return SecureDirectory(descriptor)
        except BaseException:
            os.close(descriptor)
            raise

    def _temporary_name(self) -> str:
        return f"tmp-{secrets.token_hex(16)}"

    def recover_temporary_writes(self) -> None:
        """Remove only provable internal write remnants from this directory.

        ``write_immutable`` uses a hard-link publication step so that an
        existing immutable record is never replaced.  A process can die after
        publishing the final link but before unlinking the private temporary
        name.  In that case the otherwise valid final record has link count
        two.  Recovery recognizes only our unguessable temporary-name shape,
        proves that its optional peer is an owned private regular file in this
        same anchored directory, and removes the temporary name.  Anything
        ambiguous fails closed.
        """
        directory = self._require_open()
        try:
            names = tuple(sorted(os.listdir(directory)))
        except OSError as exc:
            raise SecureFsError("FILESYSTEM_ERROR") from exc
        validated = tuple(_validate_component(name) for name in names)
        changed = False
        for temporary in validated:
            if _TEMPORARY_NAME.fullmatch(temporary) is None:
                continue
            try:
                metadata = os.stat(
                    temporary,
                    dir_fd=directory,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise _translate_os_error(exc) from exc
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) != _FILE_MODE
                or metadata.st_nlink not in {1, 2}
            ):
                raise SecureFsError("UNSAFE_TEMPORARY_FILE")
            if metadata.st_nlink == 2:
                peers: list[str] = []
                for candidate in validated:
                    if candidate == temporary or _TEMPORARY_NAME.fullmatch(candidate):
                        continue
                    try:
                        candidate_metadata = os.stat(
                            candidate,
                            dir_fd=directory,
                            follow_symlinks=False,
                        )
                    except OSError as exc:
                        raise _translate_os_error(exc) from exc
                    if (
                        candidate_metadata.st_dev == metadata.st_dev
                        and candidate_metadata.st_ino == metadata.st_ino
                    ):
                        peers.append(candidate)
                        if (
                            not stat.S_ISREG(candidate_metadata.st_mode)
                            or candidate_metadata.st_uid != os.geteuid()
                            or stat.S_IMODE(candidate_metadata.st_mode) != _FILE_MODE
                            or candidate_metadata.st_nlink != 2
                        ):
                            raise SecureFsError("UNSAFE_TEMPORARY_FILE")
                if len(peers) != 1:
                    raise SecureFsError("UNSAFE_TEMPORARY_FILE")
            try:
                os.unlink(temporary, dir_fd=directory)
            except OSError as exc:
                raise SecureFsError("FILESYSTEM_ERROR") from exc
            changed = True
        if changed:
            _durable_sync(directory)

    def _write_temporary(self, data: bytes) -> str:
        if type(data) is not bytes:
            raise SecureFsError("INVALID_FILE_CONTENT")
        directory = self._require_open()
        descriptor = -1
        temporary = ""
        for _ in range(8):
            temporary = self._temporary_name()
            try:
                descriptor = os.open(
                    temporary,
                    _open_flags(write=True) | os.O_CREAT | os.O_EXCL,
                    _FILE_MODE,
                    dir_fd=directory,
                )
                break
            except FileExistsError:
                continue
            except OSError as exc:
                raise _translate_os_error(exc, missing="FILESYSTEM_ERROR") from exc
        if descriptor < 0:
            raise SecureFsError("TEMPORARY_NAME_EXHAUSTED")
        try:
            try:
                os.fchmod(descriptor, _FILE_MODE)
                view = memoryview(data)
                written = 0
                while written < len(view):
                    count = os.write(descriptor, view[written:])
                    if count <= 0:
                        raise SecureFsError("FILESYSTEM_ERROR")
                    written += count
            except OSError as exc:
                raise SecureFsError("FILESYSTEM_ERROR") from exc
            _durable_sync(descriptor)
            metadata = os.fstat(descriptor)
            if not _regular_metadata_is_safe(metadata):
                raise SecureFsError("UNSAFE_FILE_TYPE")
        except BaseException:
            try:
                os.unlink(temporary, dir_fd=directory)
            except OSError:
                pass
            raise
        finally:
            os.close(descriptor)
        return temporary

    def write_immutable(self, component: str, data: bytes) -> None:
        """Durably publish a new regular file, failing if any leaf exists."""
        self._write_immutable(component, data, guard=None)

    def write_immutable_guarded(
        self,
        component: str,
        data: bytes,
        guard: Callable[[], None],
    ) -> None:
        """Stage durably, then guard immediately before immutable publish.

        The guard runs after the private temporary file has been fully written
        and fsynced.  Its successful return is followed directly by the
        no-replace hard-link linearization step.  If it raises, the temporary
        file is removed and the final name is never published.
        """
        if not callable(guard):
            raise SecureFsError("INVALID_PUBLICATION_GUARD")
        self._write_immutable(component, data, guard=guard)

    def _write_immutable(
        self,
        component: str,
        data: bytes,
        *,
        guard: Callable[[], None] | None,
    ) -> None:
        name = _validate_component(component)
        directory = self._require_open()
        temporary = self._write_temporary(data)
        try:
            if guard is not None:
                guard()
            try:
                os.link(
                    temporary,
                    name,
                    src_dir_fd=directory,
                    dst_dir_fd=directory,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise _translate_os_error(exc) from exc
            os.unlink(temporary, dir_fd=directory)
            temporary = ""
            _durable_sync(directory)
        finally:
            if temporary:
                try:
                    os.unlink(temporary, dir_fd=directory)
                except OSError:
                    pass

    def replace_derived(self, component: str, data: bytes) -> None:
        """Durably atomically replace a rebuildable regular-file projection."""
        name = _validate_component(component)
        directory = self._require_open()
        try:
            existing = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            existing = None
        except OSError as exc:
            raise _translate_os_error(exc) from exc
        if existing is not None:
            if stat.S_ISLNK(existing.st_mode):
                raise SecureFsError("UNSAFE_PATH")
            if not _regular_metadata_is_safe(existing):
                if stat.S_ISREG(existing.st_mode):
                    raise SecureFsError("INSECURE_PERMISSIONS")
                raise SecureFsError("UNSAFE_FILE_TYPE")
        temporary = self._write_temporary(data)
        try:
            try:
                os.rename(
                    temporary,
                    name,
                    src_dir_fd=directory,
                    dst_dir_fd=directory,
                )
                temporary = ""
            except OSError as exc:
                raise _translate_os_error(exc) from exc
            _durable_sync(directory)
        finally:
            if temporary:
                try:
                    os.unlink(temporary, dir_fd=directory)
                except OSError:
                    pass

    def read_bytes(self, component: str, *, max_bytes: int) -> bytes:
        """Read one stable private regular file without following its leaf."""
        name = _validate_component(component)
        if type(max_bytes) is not int or max_bytes < 1:
            raise SecureFsError("INVALID_READ_LIMIT")
        directory = self._require_open()
        try:
            descriptor = os.open(
                name,
                _open_flags() | _O_NONBLOCK,
                dir_fd=directory,
            )
        except OSError as exc:
            raise _translate_os_error(exc) from exc
        try:
            try:
                before = os.fstat(descriptor)
            except OSError as exc:
                raise SecureFsError("FILESYSTEM_ERROR") from exc
            if not stat.S_ISREG(before.st_mode):
                raise SecureFsError("UNSAFE_FILE_TYPE")
            if not _regular_metadata_is_safe(before):
                raise SecureFsError("INSECURE_PERMISSIONS")
            if before.st_size > max_bytes:
                raise SecureFsError("FILE_TOO_LARGE")
            chunks: list[bytes] = []
            remaining = max_bytes + 1
            while remaining:
                try:
                    chunk = os.read(descriptor, min(remaining, 64 * 1024))
                except OSError as exc:
                    raise SecureFsError("FILESYSTEM_ERROR") from exc
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            data = b"".join(chunks)
            try:
                after = os.fstat(descriptor)
            except OSError as exc:
                raise SecureFsError("FILESYSTEM_ERROR") from exc
            try:
                named = os.stat(name, dir_fd=directory, follow_symlinks=False)
            except OSError as exc:
                raise SecureFsError("FILE_CHANGED_DURING_READ") from exc
            token_before = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
                before.st_mode,
            )
            token_after = (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
                after.st_mode,
            )
            if (
                token_before != token_after
                or named.st_dev != before.st_dev
                or named.st_ino != before.st_ino
            ):
                raise SecureFsError("FILE_CHANGED_DURING_READ")
            if len(data) > max_bytes:
                raise SecureFsError("FILE_TOO_LARGE")
            return data
        finally:
            os.close(descriptor)

    def list_entries(self) -> tuple[str, ...]:
        """List validated immediate names from the anchored descriptor."""
        directory = self._require_open()
        try:
            entries = os.listdir(directory)
        except OSError as exc:
            raise SecureFsError("FILESYSTEM_ERROR") from exc
        validated = tuple(_validate_component(name) for name in entries)
        return tuple(sorted(validated))

    def stat_entry(self, component: str) -> SecureEntry:
        """Return no-follow metadata for one safe regular file or directory."""
        name = _validate_component(component)
        directory = self._require_open()
        try:
            metadata = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except OSError as exc:
            raise _translate_os_error(exc) from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise SecureFsError("UNSAFE_PATH")
        if stat.S_ISREG(metadata.st_mode):
            if not _regular_metadata_is_safe(metadata):
                raise SecureFsError("INSECURE_PERMISSIONS")
            kind = EntryKind.REGULAR
        elif stat.S_ISDIR(metadata.st_mode):
            if not _directory_metadata_is_safe(metadata):
                raise SecureFsError("INSECURE_PERMISSIONS")
            kind = EntryKind.DIRECTORY
        else:
            raise SecureFsError("UNSAFE_FILE_TYPE")
        return SecureEntry(
            name=name,
            kind=kind,
            size=metadata.st_size,
            mode=stat.S_IMODE(metadata.st_mode),
            device=metadata.st_dev,
            inode=metadata.st_ino,
        )

    def quarantine(
        self,
        component: str,
        *,
        destination: SecureDirectory,
        destination_component: str,
    ) -> None:
        """Move one private regular file to another directory without overwrite."""
        source_name = _validate_component(component)
        destination_name = _validate_component(destination_component)
        source_directory = self._require_open()
        destination_directory = destination._require_open()
        if (
            source_directory == destination_directory
            and source_name == destination_name
        ):
            raise SecureFsError("INVALID_QUARANTINE_TARGET")
        source = self.stat_entry(source_name)
        if source.kind is not EntryKind.REGULAR:
            raise SecureFsError("UNSAFE_FILE_TYPE")
        linked = False
        try:
            try:
                os.link(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory,
                    dst_dir_fd=destination_directory,
                    follow_symlinks=False,
                )
                linked = True
            except FileExistsError as conflict:
                limit = max(1, source.size)
                try:
                    source_bytes = self.read_bytes(
                        source_name,
                        max_bytes=limit,
                    )
                    destination_bytes = destination.read_bytes(
                        destination_name,
                        max_bytes=limit,
                    )
                except SecureFsError:
                    raise
                if source_bytes != destination_bytes:
                    raise SecureFsError("IMMUTABLE_EXISTS") from conflict
                try:
                    os.unlink(source_name, dir_fd=source_directory)
                except OSError as exc:
                    raise SecureFsError("FILESYSTEM_ERROR") from exc
                _durable_sync(source_directory)
                return
            except OSError as exc:
                raise _translate_os_error(exc) from exc
            try:
                linked_metadata = os.stat(
                    destination_name,
                    dir_fd=destination_directory,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise SecureFsError("FILE_CHANGED_DURING_MOVE") from exc
            if (
                not stat.S_ISREG(linked_metadata.st_mode)
                or linked_metadata.st_uid != os.geteuid()
                or stat.S_IMODE(linked_metadata.st_mode) != _FILE_MODE
                or linked_metadata.st_nlink != 2
                or linked_metadata.st_dev != source.device
                or linked_metadata.st_ino != source.inode
            ):
                raise SecureFsError("FILE_CHANGED_DURING_MOVE")
            try:
                os.unlink(source_name, dir_fd=source_directory)
            except OSError as exc:
                try:
                    os.unlink(destination_name, dir_fd=destination_directory)
                except OSError:
                    pass
                raise SecureFsError("FILESYSTEM_ERROR") from exc
            linked = False
            _durable_sync(source_directory)
            if destination_directory != source_directory:
                _durable_sync(destination_directory)
        finally:
            if linked:
                try:
                    os.unlink(destination_name, dir_fd=destination_directory)
                except OSError:
                    pass

    def recover_quarantine_moves(self, destination: SecureDirectory) -> None:
        """Finish only provable interrupted hard-link moves to quarantine."""
        if type(destination) is not SecureDirectory:
            raise SecureFsError("INVALID_QUARANTINE_DIRECTORY")
        source_directory = self._require_open()
        destination_directory = destination._require_open()
        try:
            source_names = tuple(sorted(os.listdir(source_directory)))
            destination_names = tuple(sorted(os.listdir(destination_directory)))
        except OSError as exc:
            raise SecureFsError("FILESYSTEM_ERROR") from exc
        sources = tuple(_validate_component(name) for name in source_names)
        destinations = tuple(
            _validate_component(name) for name in destination_names
        )
        changed = False
        for source_name in sources:
            try:
                source = os.stat(
                    source_name,
                    dir_fd=source_directory,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise _translate_os_error(exc) from exc
            if not stat.S_ISREG(source.st_mode) or source.st_nlink == 1:
                continue
            if (
                source.st_uid != os.geteuid()
                or stat.S_IMODE(source.st_mode) != _FILE_MODE
                or source.st_nlink != 2
            ):
                raise SecureFsError("UNSAFE_QUARANTINE_MOVE")
            peers: list[str] = []
            for destination_name in destinations:
                try:
                    candidate = os.stat(
                        destination_name,
                        dir_fd=destination_directory,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    raise _translate_os_error(exc) from exc
                if (
                    candidate.st_dev == source.st_dev
                    and candidate.st_ino == source.st_ino
                ):
                    peers.append(destination_name)
                    if (
                        not stat.S_ISREG(candidate.st_mode)
                        or candidate.st_uid != os.geteuid()
                        or stat.S_IMODE(candidate.st_mode) != _FILE_MODE
                        or candidate.st_nlink != 2
                    ):
                        raise SecureFsError("UNSAFE_QUARANTINE_MOVE")
            if len(peers) != 1:
                raise SecureFsError("UNSAFE_QUARANTINE_MOVE")
            try:
                os.unlink(source_name, dir_fd=source_directory)
            except OSError as exc:
                raise SecureFsError("FILESYSTEM_ERROR") from exc
            changed = True
        if changed:
            _durable_sync(source_directory)
            if destination_directory != source_directory:
                _durable_sync(destination_directory)
