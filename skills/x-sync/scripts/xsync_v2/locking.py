"""Cross-process domain locks and scoped held-lock authorities."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import errno
from enum import StrEnum
import fcntl
import hashlib
import os
import stat
import threading
from typing import Final


class LockError(RuntimeError):
    """A fail-closed locking error with a stable machine-readable code."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class RegistryLockMode(StrEnum):
    """Supported registry lock strengths."""

    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class _LockKind(StrEnum):
    REGISTRY = "registry"
    SESSION = "session"


@dataclass(frozen=True, slots=True)
class _HeldLease:
    manager_token: object
    descriptor: int
    directory_descriptor: int
    lock_name: str
    lock_identity: tuple[int, int]
    kind: _LockKind
    registry_mode: RegistryLockMode | None
    session_id: str | None
    owner_pid: int
    owner_thread: int


@dataclass(frozen=True, slots=True)
class _HeldRuntimeOwner:
    manager_token: object
    descriptor: int
    directory_descriptor: int
    lock_name: str
    lock_identity: tuple[int, int]
    runtime_epoch: str
    owner_pid: int


@dataclass(frozen=True, slots=True)
class _PinnedLockFile:
    descriptor: int
    identity: tuple[int, int]


class _ThreadLockState(threading.local):
    stack: list[_HeldLease]

    def __init__(self) -> None:
        self.stack = []


_AUTHORITY_KEY: Final = object()
_THREAD_STATE = _ThreadLockState()
_ACTIVE_GUARD = threading.Lock()
_ACTIVE_LEASES: dict[int, _HeldLease] = {}
_ACTIVE_RUNTIME_OWNERS: dict[int, _HeldRuntimeOwner] = {}


def _identity_name(lock_name: str) -> str:
    return f".{lock_name}.identity"


def _metadata_identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _lock_metadata_is_safe(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
    )


def _assert_lock_identity_current(
    directory_descriptor: int,
    lock_name: str,
    expected_identity: tuple[int, int],
    held_descriptor: int,
) -> None:
    """Verify both durable names and the held fd still denote one inode."""
    try:
        held_metadata = os.fstat(held_descriptor)
        lock_metadata = os.stat(
            lock_name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        identity_metadata = os.stat(
            _identity_name(lock_name),
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise LockError("LOCK_NAMESPACE_CHANGED") from exc
    metadata = (held_metadata, lock_metadata, identity_metadata)
    if any(
        not _lock_metadata_is_safe(item)
        or item.st_nlink != 2
        or _metadata_identity(item) != expected_identity
        for item in metadata
    ):
        raise LockError("LOCK_NAMESPACE_CHANGED")


def _authority_is_live(lease: _HeldLease, kind: _LockKind) -> bool:
    structurally_live = (
        type(lease) is _HeldLease
        and lease.kind is kind
        and lease.owner_pid == os.getpid()
        and lease.owner_thread == threading.get_ident()
    )
    if not structurally_live:
        return False
    with _ACTIVE_GUARD:
        active = _ACTIVE_LEASES.get(id(lease)) is lease
    if active:
        _assert_lock_identity_current(
            lease.directory_descriptor,
            lease.lock_name,
            lease.lock_identity,
            lease.descriptor,
        )
    return active


def _runtime_owner_is_live(owner: _HeldRuntimeOwner) -> bool:
    if type(owner) is not _HeldRuntimeOwner or owner.owner_pid != os.getpid():
        return False
    with _ACTIVE_GUARD:
        active = _ACTIVE_RUNTIME_OWNERS.get(id(owner)) is owner
    if active:
        _assert_lock_identity_current(
            owner.directory_descriptor,
            owner.lock_name,
            owner.lock_identity,
            owner.descriptor,
        )
    return active


def _close_quietly(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        pass


def _validate_session_id(session_id: object) -> str:
    if type(session_id) is not str:
        raise LockError("INVALID_SESSION_ID")
    try:
        encoded = session_id.encode("utf-8")
    except UnicodeError as exc:
        raise LockError("INVALID_SESSION_ID") from exc
    if (
        not session_id
        or session_id != session_id.strip()
        or session_id in {".", ".."}
        or len(encoded) > 255
        or any(
            character in {"/", "\\", "\x00"}
            or ord(character) < 32
            or ord(character) == 127
            for character in session_id
        )
    ):
        raise LockError("INVALID_SESSION_ID")
    return session_id


def _validate_blocking(blocking: object) -> bool:
    if type(blocking) is not bool:
        raise LockError("INVALID_BLOCKING_MODE")
    return blocking


@dataclass(frozen=True, slots=True, init=False)
class RegistryLockAuthority:
    """Proof that this thread currently holds a registry lock."""

    _lease: _HeldLease

    def __init__(
        self,
        key: object = None,
        lease: _HeldLease | None = None,
    ) -> None:
        if (
            type(self) is not RegistryLockAuthority
            or key is not _AUTHORITY_KEY
            or type(lease) is not _HeldLease
            or lease.kind is not _LockKind.REGISTRY
        ):
            raise LockError("LOCK_AUTHORITY_CONSTRUCTION_FORBIDDEN")
        object.__setattr__(self, "_lease", lease)

    @property
    def mode(self) -> RegistryLockMode:
        """Return the held registry mode after validating authority scope."""
        self.assert_valid()
        mode = self._lease.registry_mode
        if type(mode) is not RegistryLockMode:
            raise LockError("LOCK_AUTHORITY_INVALID")
        return mode

    def assert_valid(self) -> None:
        """Reject an expired, forged, cross-thread, or cross-process proof."""
        if type(self) is not RegistryLockAuthority or not _authority_is_live(
            self._lease, _LockKind.REGISTRY
        ):
            raise LockError("LOCK_AUTHORITY_INVALID")

    def assert_registry(
        self,
        required: RegistryLockMode = RegistryLockMode.SHARED,
    ) -> None:
        """Require a live registry proof of at least ``required`` strength."""
        if type(required) is not RegistryLockMode:
            raise LockError("INVALID_LOCK_MODE")
        self.assert_valid()
        if (
            required is RegistryLockMode.EXCLUSIVE
            and self._lease.registry_mode is not RegistryLockMode.EXCLUSIVE
        ):
            raise LockError("LOCK_AUTHORITY_INSUFFICIENT")


@dataclass(frozen=True, slots=True, init=False)
class SessionLockAuthority:
    """Proof of registry-then-session ownership for one dialogue commit."""

    _lease: _HeldLease
    _registry: RegistryLockAuthority

    def __init__(
        self,
        key: object = None,
        lease: _HeldLease | None = None,
        registry: RegistryLockAuthority | None = None,
    ) -> None:
        if (
            type(self) is not SessionLockAuthority
            or key is not _AUTHORITY_KEY
            or type(lease) is not _HeldLease
            or lease.kind is not _LockKind.SESSION
            or type(registry) is not RegistryLockAuthority
        ):
            raise LockError("LOCK_AUTHORITY_CONSTRUCTION_FORBIDDEN")
        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_registry", registry)

    @property
    def session_id(self) -> str:
        """Return the locked Session identifier after validating the proof."""
        self.assert_valid()
        session_id = self._lease.session_id
        if type(session_id) is not str:
            raise LockError("LOCK_AUTHORITY_INVALID")
        return session_id

    @property
    def registry(self) -> RegistryLockAuthority:
        """Return the registry proof paired with this Session proof."""
        self.assert_valid()
        return self._registry

    def assert_valid(self) -> None:
        """Reject an expired, forged, cross-thread, or cross-process proof."""
        if type(self) is not SessionLockAuthority or not _authority_is_live(
            self._lease, _LockKind.SESSION
        ):
            raise LockError("LOCK_AUTHORITY_INVALID")
        self._registry.assert_valid()
        if self._lease.manager_token is not self._registry._lease.manager_token:
            raise LockError("LOCK_AUTHORITY_INVALID")

    def assert_registry(
        self,
        required: RegistryLockMode = RegistryLockMode.SHARED,
    ) -> None:
        """Require the paired registry proof at the requested strength."""
        self.assert_valid()
        self._registry.assert_registry(required)

    def assert_session(self, session_id: object) -> None:
        """Require this proof to own exactly ``session_id``."""
        self.assert_valid()
        expected = _validate_session_id(session_id)
        if self._lease.session_id != expected:
            raise LockError("LOCK_AUTHORITY_SESSION_MISMATCH")

    def assert_can_commit(self, session_id: object) -> None:
        """Require the complete shared-registry plus Session commit fence."""
        self.assert_registry(RegistryLockMode.SHARED)
        self.assert_session(session_id)


@dataclass(frozen=True, slots=True, init=False)
class RuntimeOwnerAuthority:
    """Process-wide proof that this daemon owns the runtime epoch fence."""

    _owner: _HeldRuntimeOwner

    def __init__(
        self,
        key: object = None,
        owner: _HeldRuntimeOwner | None = None,
    ) -> None:
        if (
            type(self) is not RuntimeOwnerAuthority
            or key is not _AUTHORITY_KEY
            or type(owner) is not _HeldRuntimeOwner
        ):
            raise LockError("LOCK_AUTHORITY_CONSTRUCTION_FORBIDDEN")
        object.__setattr__(self, "_owner", owner)

    @property
    def runtime_epoch(self) -> str:
        """Return the fenced epoch after revalidating the lifetime lock."""
        self.assert_valid()
        return self._owner.runtime_epoch

    def assert_valid(self) -> None:
        """Reject an expired, forged, replaced, or inherited owner proof."""
        if type(self) is not RuntimeOwnerAuthority or not _runtime_owner_is_live(
            self._owner
        ):
            raise LockError("RUNTIME_OWNER_AUTHORITY_INVALID")


def _after_fork_child() -> None:
    try:
        for lease in tuple(_ACTIVE_LEASES.values()):
            _close_quietly(lease.descriptor)
        for owner in tuple(_ACTIVE_RUNTIME_OWNERS.values()):
            _close_quietly(owner.descriptor)
        _ACTIVE_LEASES.clear()
        _ACTIVE_RUNTIME_OWNERS.clear()
        _THREAD_STATE.stack = []
    finally:
        _ACTIVE_GUARD.release()


def _before_fork() -> None:
    _ACTIVE_GUARD.acquire()


def _after_fork_parent() -> None:
    _ACTIVE_GUARD.release()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_before_fork,
        after_in_parent=_after_fork_parent,
        after_in_child=_after_fork_child,
    )


class DomainLockManager:
    """Own cross-process registry and Session lock acquisition."""

    def __init__(self, directory: str | os.PathLike[str]):
        self._owner_pid = os.getpid()
        self._manager_token = object()
        self._closed = False
        self._directory_fd = -1
        self._pin_guard = threading.Lock()
        self._pinned_lock_files: dict[str, _PinnedLockFile] = {}
        self._runtime_owner: _HeldRuntimeOwner | None = None
        self._directory_path = self._validate_directory_path(directory)
        self._directory_fd = self._open_lock_directory(self._directory_path)
        metadata = os.fstat(self._directory_fd)
        self._directory_identity = (metadata.st_dev, metadata.st_ino)

    @property
    def namespace_identity(self) -> tuple[int, int]:
        """Return the live filesystem identity of this lock namespace.

        Persistent coordination stores bind their own namespace records to
        this value so two independently-created lock directories cannot both
        claim authority over the same durable data.  Revalidation happens on
        every read; replacing the directory therefore fails closed instead of
        returning the identity captured at construction time.
        """
        self._ensure_open()
        return self._directory_identity

    @staticmethod
    def _validate_directory_path(
        directory: str | os.PathLike[str],
    ) -> str:
        try:
            value = os.fspath(directory)
        except TypeError as exc:
            raise LockError("INVALID_LOCK_DIRECTORY") from exc
        if type(value) is not str or not value or "\x00" in value:
            raise LockError("INVALID_LOCK_DIRECTORY")
        separators = {os.sep}
        if os.altsep is not None:
            separators.add(os.altsep)
        normalized = value.rstrip("".join(separators))
        if not normalized:
            raise LockError("INVALID_LOCK_DIRECTORY")
        components = normalized.split(os.sep)
        if os.path.isabs(normalized):
            components = components[1:]
        if not components or any(
            not component
            or component in {".", ".."}
            or any(separator in component for separator in separators)
            for component in components
        ):
            raise LockError("INVALID_LOCK_DIRECTORY")
        return value

    @staticmethod
    def _required_os_flags() -> tuple[int, int, int]:
        nofollow = getattr(os, "O_NOFOLLOW", None)
        directory = getattr(os, "O_DIRECTORY", None)
        cloexec = getattr(os, "O_CLOEXEC", None)
        if (
            type(nofollow) is not int
            or nofollow <= 0
            or type(directory) is not int
            or directory <= 0
            or type(cloexec) is not int
            or cloexec <= 0
            or not {os.open, os.mkdir, os.chmod, os.link, os.stat}.issubset(
                os.supports_dir_fd
            )
            or not {os.chmod, os.link, os.stat}.issubset(
                os.supports_follow_symlinks
            )
        ):
            raise LockError("LOCKING_UNSUPPORTED")
        return nofollow, directory, cloexec

    @classmethod
    def _open_lock_directory(cls, path: str) -> int:
        nofollow, directory, cloexec = cls._required_os_flags()
        normalized = path.rstrip(os.sep)
        components = normalized.split(os.sep)
        anchor = "/" if os.path.isabs(normalized) else "."
        if os.path.isabs(normalized):
            components = components[1:]
        try:
            parent_descriptor = os.open(
                anchor,
                os.O_RDONLY | nofollow | directory | cloexec,
            )
        except OSError as exc:
            raise LockError("UNSAFE_LOCK_DIRECTORY") from exc
        try:
            for component in components[:-1]:
                try:
                    next_descriptor = os.open(
                        component,
                        os.O_RDONLY | nofollow | directory | cloexec,
                        dir_fd=parent_descriptor,
                    )
                except OSError as exc:
                    code = (
                        "UNSAFE_LOCK_DIRECTORY"
                        if exc.errno in {errno.ELOOP, errno.ENOTDIR}
                        else "LOCK_DIRECTORY_OPEN_FAILED"
                    )
                    raise LockError(code) from exc
                _close_quietly(parent_descriptor)
                parent_descriptor = next_descriptor

            leaf = components[-1]
            created = False
            try:
                os.mkdir(leaf, mode=0o700, dir_fd=parent_descriptor)
                created = True
            except FileExistsError:
                pass
            except OSError as exc:
                raise LockError("LOCK_DIRECTORY_OPEN_FAILED") from exc
            if created:
                try:
                    os.chmod(
                        leaf,
                        0o700,
                        dir_fd=parent_descriptor,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    raise LockError("LOCK_DIRECTORY_OPEN_FAILED") from exc
            try:
                descriptor = os.open(
                    leaf,
                    os.O_RDONLY | nofollow | directory | cloexec,
                    dir_fd=parent_descriptor,
                )
            except OSError as exc:
                code = (
                    "UNSAFE_LOCK_DIRECTORY"
                    if exc.errno in {errno.ELOOP, errno.ENOTDIR}
                    else "LOCK_DIRECTORY_OPEN_FAILED"
                )
                raise LockError(code) from exc
        finally:
            _close_quietly(parent_descriptor)
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise LockError("UNSAFE_LOCK_DIRECTORY")
        except LockError:
            _close_quietly(descriptor)
            raise
        except OSError as exc:
            _close_quietly(descriptor)
            raise LockError("LOCK_DIRECTORY_OPEN_FAILED") from exc
        except BaseException:
            _close_quietly(descriptor)
            raise
        return descriptor

    def _ensure_open(self) -> None:
        if self._owner_pid != os.getpid():
            raise LockError("LOCK_MANAGER_PROCESS_MISMATCH")
        if self._closed or self._directory_fd < 0:
            raise LockError("LOCK_MANAGER_CLOSED")
        self._verify_namespace_current()

    def _verify_namespace_current(self) -> None:
        """Reject a lexical lock directory replaced after this manager opened."""
        nofollow, directory, cloexec = self._required_os_flags()
        try:
            descriptor = os.open(
                self._directory_path,
                os.O_RDONLY | nofollow | directory | cloexec,
            )
        except OSError as exc:
            raise LockError("LOCK_NAMESPACE_CHANGED") from exc
        try:
            metadata = os.fstat(descriptor)
        except OSError as exc:
            raise LockError("LOCK_NAMESPACE_CHANGED") from exc
        finally:
            _close_quietly(descriptor)
        if (metadata.st_dev, metadata.st_ino) != self._directory_identity:
            raise LockError("LOCK_NAMESPACE_CHANGED")

    @staticmethod
    def _thread_stack() -> list[_HeldLease]:
        process_id = os.getpid()
        thread_id = threading.get_ident()
        with _ACTIVE_GUARD:
            _THREAD_STATE.stack = [
                lease
                for lease in _THREAD_STATE.stack
                if lease.owner_pid == process_id
                and lease.owner_thread == thread_id
                and _ACTIVE_LEASES.get(id(lease)) is lease
            ]
        return _THREAD_STATE.stack

    def _open_named_lock_file(
        self,
        name: str,
        *,
        create: bool,
    ) -> tuple[int, bool]:
        """Open one private regular lock file without following a symlink."""
        self._ensure_open()
        nofollow, _, cloexec = self._required_os_flags()
        base_flags = os.O_RDWR | os.O_NONBLOCK | nofollow | cloexec
        created = False
        if create:
            try:
                descriptor = os.open(
                    name,
                    base_flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=self._directory_fd,
                )
                created = True
            except FileExistsError:
                try:
                    descriptor = os.open(
                        name,
                        base_flags,
                        dir_fd=self._directory_fd,
                    )
                except OSError as exc:
                    code = (
                        "UNSAFE_LOCK_FILE"
                        if exc.errno
                        in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}
                        else "LOCK_FILE_OPEN_FAILED"
                    )
                    raise LockError(code) from exc
            except OSError as exc:
                code = (
                    "UNSAFE_LOCK_FILE"
                    if exc.errno in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}
                    else "LOCK_FILE_OPEN_FAILED"
                )
                raise LockError(code) from exc
        else:
            try:
                descriptor = os.open(
                    name,
                    base_flags,
                    dir_fd=self._directory_fd,
                )
            except OSError as exc:
                code = (
                    "UNSAFE_LOCK_FILE"
                    if exc.errno in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}
                    else "LOCK_FILE_OPEN_FAILED"
                )
                raise LockError(code) from exc

        try:
            if created:
                os.fchmod(descriptor, 0o600)
            metadata = os.fstat(descriptor)
            if (
                not _lock_metadata_is_safe(metadata)
                or metadata.st_nlink not in {1, 2}
            ):
                raise LockError("UNSAFE_LOCK_FILE")
        except LockError:
            _close_quietly(descriptor)
            raise
        except OSError as exc:
            _close_quietly(descriptor)
            raise LockError("LOCK_FILE_OPEN_FAILED") from exc
        except BaseException:
            _close_quietly(descriptor)
            raise
        return descriptor, created

    def _open_initial_lock_file(self, name: str) -> int:
        """Create/upgrade the two-name identity pair and return a pinned fd."""
        descriptor, _ = self._open_named_lock_file(name, create=True)
        try:
            metadata = os.fstat(descriptor)
            identity = _metadata_identity(metadata)
            identity_name = _identity_name(name)
            if metadata.st_nlink == 1:
                try:
                    os.link(
                        name,
                        identity_name,
                        src_dir_fd=self._directory_fd,
                        dst_dir_fd=self._directory_fd,
                        follow_symlinks=False,
                    )
                    os.fsync(self._directory_fd)
                except FileExistsError:
                    pass
                except OSError as exc:
                    raise LockError("LOCK_FILE_OPEN_FAILED") from exc
            _assert_lock_identity_current(
                self._directory_fd,
                name,
                identity,
                descriptor,
            )
        except BaseException:
            _close_quietly(descriptor)
            raise
        return descriptor

    def _open_lock_file(self, name: str) -> tuple[int, tuple[int, int]]:
        """Open an acquisition fd bound to this manager's pinned identity."""
        self._ensure_open()
        with self._pin_guard:
            pinned = self._pinned_lock_files.get(name)
            if pinned is None:
                descriptor = self._open_initial_lock_file(name)
                metadata = os.fstat(descriptor)
                pinned = _PinnedLockFile(
                    descriptor=descriptor,
                    identity=_metadata_identity(metadata),
                )
                self._pinned_lock_files[name] = pinned
            else:
                _assert_lock_identity_current(
                    self._directory_fd,
                    name,
                    pinned.identity,
                    pinned.descriptor,
                )

            descriptor, _ = self._open_named_lock_file(name, create=False)
            try:
                _assert_lock_identity_current(
                    self._directory_fd,
                    name,
                    pinned.identity,
                    descriptor,
                )
            except BaseException:
                _close_quietly(descriptor)
                raise
        return descriptor, pinned.identity

    @staticmethod
    def _take_os_lock(
        descriptor: int,
        operation: int,
        blocking: bool,
    ) -> None:
        if not blocking:
            operation |= fcntl.LOCK_NB
        while True:
            try:
                fcntl.flock(descriptor, operation)
                return
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                code = (
                    "LOCK_BUSY"
                    if exc.errno in {errno.EACCES, errno.EAGAIN}
                    else "LOCK_ACQUIRE_FAILED"
                )
                raise LockError(code) from exc

    def _register_lease(self, lease: _HeldLease) -> None:
        self._thread_stack().append(lease)
        with _ACTIVE_GUARD:
            _ACTIVE_LEASES[id(lease)] = lease

    def _acquire_registry(
        self,
        mode: RegistryLockMode,
        blocking: bool,
    ) -> _HeldLease:
        self._ensure_open()
        blocking = _validate_blocking(blocking)
        if self._thread_stack():
            raise LockError("LOCK_ORDER_VIOLATION")
        lock_name = "registry.lock"
        descriptor, lock_identity = self._open_lock_file(lock_name)
        operation = (
            fcntl.LOCK_SH
            if mode is RegistryLockMode.SHARED
            else fcntl.LOCK_EX
        )
        try:
            self._take_os_lock(descriptor, operation, blocking)
            self._verify_namespace_current()
            _assert_lock_identity_current(
                self._directory_fd,
                lock_name,
                lock_identity,
                descriptor,
            )
        except BaseException:
            _close_quietly(descriptor)
            raise
        lease = _HeldLease(
            manager_token=self._manager_token,
            descriptor=descriptor,
            directory_descriptor=self._directory_fd,
            lock_name=lock_name,
            lock_identity=lock_identity,
            kind=_LockKind.REGISTRY,
            registry_mode=mode,
            session_id=None,
            owner_pid=os.getpid(),
            owner_thread=threading.get_ident(),
        )
        self._register_lease(lease)
        return lease

    def _acquire_session(
        self,
        session_id: object,
        registry: RegistryLockAuthority | None,
        blocking: bool,
    ) -> _HeldLease:
        self._ensure_open()
        identifier = _validate_session_id(session_id)
        blocking = _validate_blocking(blocking)
        stack = self._thread_stack()
        if len(stack) != 1 or stack[0].kind is not _LockKind.REGISTRY:
            raise LockError("LOCK_ORDER_VIOLATION")
        if type(registry) is not RegistryLockAuthority:
            raise LockError("LOCK_AUTHORITY_INVALID")
        registry.assert_valid()
        if (
            registry._lease is not stack[0]
            or registry._lease.manager_token is not self._manager_token
        ):
            raise LockError("LOCK_AUTHORITY_INVALID")

        digest = hashlib.sha256(identifier.encode("utf-8")).hexdigest()
        lock_name = f"session-{digest}.lock"
        descriptor, lock_identity = self._open_lock_file(lock_name)
        try:
            self._take_os_lock(descriptor, fcntl.LOCK_EX, blocking)
            self._verify_namespace_current()
            _assert_lock_identity_current(
                self._directory_fd,
                lock_name,
                lock_identity,
                descriptor,
            )
        except BaseException:
            _close_quietly(descriptor)
            raise
        lease = _HeldLease(
            manager_token=self._manager_token,
            descriptor=descriptor,
            directory_descriptor=self._directory_fd,
            lock_name=lock_name,
            lock_identity=lock_identity,
            kind=_LockKind.SESSION,
            registry_mode=None,
            session_id=identifier,
            owner_pid=os.getpid(),
            owner_thread=threading.get_ident(),
        )
        self._register_lease(lease)
        return lease

    def acquire_runtime_owner(
        self,
        runtime_epoch: object,
        *,
        blocking: bool = False,
    ) -> RuntimeOwnerAuthority:
        """Acquire the process-wide daemon lifetime fence for one epoch."""
        try:
            epoch = _validate_session_id(runtime_epoch)
        except LockError as exc:
            raise LockError("INVALID_RUNTIME_EPOCH") from exc
        blocking = _validate_blocking(blocking)
        self._ensure_open()
        if self._runtime_owner is not None:
            raise LockError("RUNTIME_OWNER_ALREADY_ACQUIRED")
        lock_name = "runtime-owner.lock"
        descriptor, lock_identity = self._open_lock_file(lock_name)
        try:
            try:
                self._take_os_lock(descriptor, fcntl.LOCK_EX, blocking)
            except LockError as exc:
                if exc.code == "LOCK_BUSY":
                    raise LockError("RUNTIME_ALREADY_RUNNING") from exc
                raise
            self._verify_namespace_current()
            _assert_lock_identity_current(
                self._directory_fd,
                lock_name,
                lock_identity,
                descriptor,
            )
        except BaseException:
            _close_quietly(descriptor)
            raise
        owner = _HeldRuntimeOwner(
            self._manager_token,
            descriptor,
            self._directory_fd,
            lock_name,
            lock_identity,
            epoch,
            os.getpid(),
        )
        with _ACTIVE_GUARD:
            _ACTIVE_RUNTIME_OWNERS[id(owner)] = owner
        self._runtime_owner = owner
        return RuntimeOwnerAuthority(_AUTHORITY_KEY, owner)

    def release_runtime_owner(self, authority: RuntimeOwnerAuthority) -> None:
        """Release this manager's live daemon lifetime fence."""
        self._ensure_open()
        self.assert_runtime_owner_authority(authority)
        owner = authority._owner
        release_error = False
        try:
            fcntl.flock(owner.descriptor, fcntl.LOCK_UN)
        except OSError:
            release_error = True
        try:
            os.close(owner.descriptor)
        except OSError:
            release_error = True
        with _ACTIVE_GUARD:
            _ACTIVE_RUNTIME_OWNERS.pop(id(owner), None)
        self._runtime_owner = None
        if release_error:
            raise LockError("RUNTIME_OWNER_RELEASE_FAILED")

    @staticmethod
    def _release(lease: _HeldLease) -> None:
        stack = DomainLockManager._thread_stack()
        if not stack or stack[-1] is not lease:
            raise LockError("LOCK_ORDER_VIOLATION")
        release_error = False
        try:
            fcntl.flock(lease.descriptor, fcntl.LOCK_UN)
        except OSError:
            release_error = True
        try:
            os.close(lease.descriptor)
        except OSError:
            release_error = True
        stack.pop()
        with _ACTIVE_GUARD:
            _ACTIVE_LEASES.pop(id(lease), None)
        if release_error:
            raise LockError("LOCK_RELEASE_FAILED")

    @contextmanager
    def registry_shared(
        self,
        *,
        blocking: bool = True,
    ) -> Iterator[RegistryLockAuthority]:
        """Hold the project+learner registry in shared/read mode."""
        lease = self._acquire_registry(RegistryLockMode.SHARED, blocking)
        authority = RegistryLockAuthority(_AUTHORITY_KEY, lease)
        try:
            yield authority
        finally:
            self._release(lease)

    @contextmanager
    def registry_exclusive(
        self,
        *,
        blocking: bool = True,
    ) -> Iterator[RegistryLockAuthority]:
        """Hold the project+learner registry in exclusive/write mode."""
        lease = self._acquire_registry(RegistryLockMode.EXCLUSIVE, blocking)
        authority = RegistryLockAuthority(_AUTHORITY_KEY, lease)
        try:
            yield authority
        finally:
            self._release(lease)

    @contextmanager
    def session_exclusive(
        self,
        session_id: object,
        registry: RegistryLockAuthority | None,
        *,
        blocking: bool = True,
    ) -> Iterator[SessionLockAuthority]:
        """Hold one Session after validating the already-held registry proof."""
        lease = self._acquire_session(session_id, registry, blocking)
        authority = SessionLockAuthority(_AUTHORITY_KEY, lease, registry)
        try:
            yield authority
        finally:
            self._release(lease)

    @contextmanager
    def semantic_session(
        self,
        session_id: object,
        *,
        registry_mode: RegistryLockMode = RegistryLockMode.SHARED,
        blocking: bool = True,
    ) -> Iterator[SessionLockAuthority]:
        """Acquire registry then Session locks as one safe semantic entry."""
        identifier = _validate_session_id(session_id)
        if type(registry_mode) is not RegistryLockMode:
            raise LockError("INVALID_LOCK_MODE")
        blocking = _validate_blocking(blocking)
        registry_context = (
            self.registry_shared(blocking=blocking)
            if registry_mode is RegistryLockMode.SHARED
            else self.registry_exclusive(blocking=blocking)
        )
        with registry_context as registry:
            with self.session_exclusive(
                identifier,
                registry,
                blocking=blocking,
            ) as session:
                yield session

    def assert_none_held(self) -> None:
        """Enforce the Observer boundary after every domain lock is released."""
        self._ensure_open()
        process_id = os.getpid()
        with _ACTIVE_GUARD:
            if any(
                lease.owner_pid == process_id
                for lease in _ACTIVE_LEASES.values()
            ):
                raise LockError("DOMAIN_LOCKS_HELD")

    def assert_session_authority(
        self,
        authority: SessionLockAuthority,
        session_id: object,
    ) -> None:
        """Bind a live Session authority to this exact lock namespace."""
        self._ensure_open()
        if type(authority) is not SessionLockAuthority:
            raise LockError("LOCK_AUTHORITY_INVALID")
        authority.assert_can_commit(session_id)
        if authority._lease.manager_token is not self._manager_token:
            raise LockError("LOCK_AUTHORITY_INVALID")

    def assert_registry_authority(
        self,
        authority: RegistryLockAuthority,
        required: RegistryLockMode,
    ) -> None:
        """Bind a live Registry authority to this exact lock namespace."""
        self._ensure_open()
        if (
            type(authority) is not RegistryLockAuthority
            or type(required) is not RegistryLockMode
        ):
            raise LockError("LOCK_AUTHORITY_INVALID")
        authority.assert_registry(required)
        if authority._lease.manager_token is not self._manager_token:
            raise LockError("LOCK_AUTHORITY_INVALID")

    def assert_runtime_owner_authority(
        self,
        authority: RuntimeOwnerAuthority,
    ) -> None:
        """Bind a live daemon owner proof to this exact lock namespace."""
        self._ensure_open()
        if type(authority) is not RuntimeOwnerAuthority:
            raise LockError("RUNTIME_OWNER_AUTHORITY_INVALID")
        authority.assert_valid()
        if (
            authority._owner.manager_token is not self._manager_token
            or authority._owner is not self._runtime_owner
        ):
            raise LockError("RUNTIME_OWNER_AUTHORITY_INVALID")

    def close(self) -> None:
        """Close the lock directory handle when this manager holds no locks."""
        if self._closed:
            return
        if self._owner_pid != os.getpid():
            raise LockError("LOCK_MANAGER_PROCESS_MISMATCH")
        with _ACTIVE_GUARD:
            if any(
                lease.owner_pid == self._owner_pid
                and lease.manager_token is self._manager_token
                for lease in _ACTIVE_LEASES.values()
            ):
                raise LockError("DOMAIN_LOCKS_HELD")
        if self._runtime_owner is not None:
            self.release_runtime_owner(
                RuntimeOwnerAuthority(_AUTHORITY_KEY, self._runtime_owner)
            )
        close_error = False
        with self._pin_guard:
            for pinned in self._pinned_lock_files.values():
                try:
                    os.close(pinned.descriptor)
                except OSError:
                    close_error = True
            self._pinned_lock_files.clear()
            try:
                os.close(self._directory_fd)
            except OSError:
                close_error = True
        self._directory_fd = -1
        self._closed = True
        if close_error:
            raise LockError("LOCK_MANAGER_CLOSE_FAILED")

    def __enter__(self) -> DomainLockManager:
        """Use this manager as a resource context."""
        self._ensure_open()
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Close this manager at resource-context exit."""
        self.close()

    def __del__(self) -> None:
        descriptor = getattr(self, "_directory_fd", -1)
        owner_pid = getattr(self, "_owner_pid", -1)
        if descriptor >= 0 and owner_pid == os.getpid():
            runtime_owner = getattr(self, "_runtime_owner", None)
            if type(runtime_owner) is _HeldRuntimeOwner:
                _close_quietly(runtime_owner.descriptor)
                with _ACTIVE_GUARD:
                    _ACTIVE_RUNTIME_OWNERS.pop(id(runtime_owner), None)
            pinned_lock_files = getattr(self, "_pinned_lock_files", {})
            for pinned in pinned_lock_files.values():
                _close_quietly(pinned.descriptor)
            try:
                os.close(descriptor)
            except OSError:
                pass
