"""Owner-only Unix-domain transport for the model-neutral Host JSON API."""

from __future__ import annotations

from dataclasses import dataclass
import errno
import math
import os
from os import PathLike
import socket
import stat
import struct
import threading
from typing import cast

from .host_api import HostApi, HostApiResponse, MAX_HOST_API_REQUEST_BYTES
from .secure_fs import SecureDirectory, SecureDirectoryIdentity, SecureFsError


MAX_HOST_IPC_RESPONSE_BYTES = 256 * 1024
_MAX_UNIX_SOCKET_PATH_BYTES = 100
_FRAME_HEADER_BYTES = 4
_MAX_CLIENTS = 64
_MAX_IO_TIMEOUT_SECONDS = 60.0


class HostIpcError(RuntimeError):
    """Stable private-IPC configuration, integrity, or transport failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class _SocketLocation:
    path: str
    parent_path: str
    component: str


@dataclass(frozen=True, slots=True)
class _SocketIdentity:
    device: int
    inode: int


def _location(value: str | PathLike[str]) -> _SocketLocation:
    try:
        raw = os.fspath(value)
    except TypeError as exc:
        raise HostIpcError("HOST_IPC_UNSAFE_PATH") from exc
    if (
        type(raw) is not str
        or not raw.startswith("/")
        or "\0" in raw
        or "\\" in raw
    ):
        raise HostIpcError("HOST_IPC_UNSAFE_PATH")
    parts = raw.split("/")[1:]
    if (
        not parts
        or any(not part or part in {".", ".."} for part in parts)
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in raw
        )
    ):
        raise HostIpcError("HOST_IPC_UNSAFE_PATH")
    try:
        encoded = raw.encode("utf-8")
        component = parts[-1]
        component.encode("utf-8")
    except UnicodeError as exc:
        raise HostIpcError("HOST_IPC_UNSAFE_PATH") from exc
    if len(encoded) > _MAX_UNIX_SOCKET_PATH_BYTES or len(component.encode()) > 255:
        raise HostIpcError("HOST_IPC_UNSAFE_PATH")
    parent_path = raw.rsplit("/", 1)[0] or "/"
    return _SocketLocation(raw, parent_path, component)


def _pin_parent(
    location: _SocketLocation,
) -> tuple[SecureDirectory, SecureDirectoryIdentity]:
    parent: SecureDirectory | None = None
    try:
        parent = SecureDirectory.open(location.parent_path)
        identity = parent.identity()
        _verify_parent(location, identity)
        return parent, identity
    except SecureFsError as exc:
        if parent is not None:
            parent.close()
        raise HostIpcError("HOST_IPC_UNSAFE_PATH") from exc
    except HostIpcError:
        if parent is not None:
            parent.close()
        raise


def _verify_parent(
    location: _SocketLocation,
    expected: SecureDirectoryIdentity,
) -> None:
    try:
        metadata = os.stat(location.parent_path, follow_symlinks=False)
    except OSError as exc:
        raise HostIpcError("HOST_IPC_UNSAFE_PATH") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_dev != expected.device
        or metadata.st_ino != expected.inode
    ):
        raise HostIpcError("HOST_IPC_UNSAFE_PATH")


def _socket_identity(location: _SocketLocation) -> _SocketIdentity:
    try:
        metadata = os.stat(location.path, follow_symlinks=False)
    except OSError as exc:
        raise HostIpcError("HOST_IPC_ADDRESS_UNAVAILABLE") from exc
    if (
        not stat.S_ISSOCK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
    ):
        raise HostIpcError("HOST_IPC_ADDRESS_UNSAFE")
    return _SocketIdentity(metadata.st_dev, metadata.st_ino)


def _verify_socket(
    location: _SocketLocation,
    expected: _SocketIdentity,
) -> None:
    if _socket_identity(location) != expected:
        raise HostIpcError("HOST_IPC_ADDRESS_CHANGED")


def _unlink_created_socket(
    location: _SocketLocation,
    parent_identity: SecureDirectoryIdentity,
) -> None:
    try:
        _verify_parent(location, parent_identity)
        metadata = os.stat(location.path, follow_symlinks=False)
        if not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != os.geteuid():
            return
        os.unlink(location.path)
    except (HostIpcError, OSError):
        return


def _finite_timeout(value: object) -> float:
    if type(value) not in {int, float}:
        raise HostIpcError("HOST_IPC_CONFIGURATION_INVALID")
    try:
        result = float(cast("int | float", value))
    except (OverflowError, ValueError) as exc:
        raise HostIpcError("HOST_IPC_CONFIGURATION_INVALID") from exc
    if not math.isfinite(result) or not 0 < result <= _MAX_IO_TIMEOUT_SECONDS:
        raise HostIpcError("HOST_IPC_CONFIGURATION_INVALID")
    return result


def _receive_exact(connection: socket.socket, length: int, code: str) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        try:
            chunk = connection.recv(remaining)
        except OSError as exc:
            raise HostIpcError(code) from exc
        if not chunk:
            raise HostIpcError(code)
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _receive_frame(
    connection: socket.socket,
    *,
    maximum: int,
    code: str,
) -> bytes:
    header = _receive_exact(connection, _FRAME_HEADER_BYTES, code)
    length = struct.unpack("!I", header)[0]
    if length > maximum:
        raise HostIpcError(code)
    return _receive_exact(connection, length, code) if length else b""


def _send_frame(connection: socket.socket, body: bytes, code: str) -> None:
    if type(body) is not bytes or len(body) > MAX_HOST_IPC_RESPONSE_BYTES:
        raise HostIpcError(code)
    try:
        connection.sendall(struct.pack("!I", len(body)) + body)
    except OSError as exc:
        raise HostIpcError(code) from exc


class HostIpcServer:
    """Serve one strict HostApi request per owner-only Unix connection."""

    def __init__(
        self,
        api: HostApi,
        socket_path: str | PathLike[str],
        *,
        max_clients: int = 8,
        request_timeout: float | int = 5.0,
    ) -> None:
        if type(api) is not HostApi:
            raise HostIpcError("HOST_IPC_CONFIGURATION_INVALID")
        if type(max_clients) is not int or not 1 <= max_clients <= _MAX_CLIENTS:
            raise HostIpcError("HOST_IPC_CONFIGURATION_INVALID")
        self._api = api
        self._location = _location(socket_path)
        self._max_clients = max_clients
        self._request_timeout = _finite_timeout(request_timeout)
        self._slots = threading.BoundedSemaphore(max_clients)
        self._state_lock = threading.Lock()
        self._stop = threading.Event()
        self._listener: socket.socket | None = None
        self._parent: SecureDirectory | None = None
        self._parent_identity: SecureDirectoryIdentity | None = None
        self._socket_identity: _SocketIdentity | None = None
        self._accept_thread: threading.Thread | None = None
        self._connections: set[socket.socket] = set()
        self._workers: set[threading.Thread] = set()
        self._fatal_error: str | None = None
        self._closed = False

    @property
    def path(self) -> str:
        """Return the non-secret local socket path configured for this server."""
        return self._location.path

    @property
    def fatal_error(self) -> str | None:
        """Return a stable asynchronous integrity failure, if one occurred."""
        with self._state_lock:
            return self._fatal_error

    def start(self) -> HostIpcServer:
        """Bind synchronously, verify private metadata, and begin accepting."""
        if not hasattr(socket, "AF_UNIX"):
            raise HostIpcError("HOST_IPC_UNSUPPORTED")
        with self._state_lock:
            if self._closed:
                raise HostIpcError("HOST_IPC_CLOSED")
            if self._listener is not None:
                raise HostIpcError("HOST_IPC_ALREADY_RUNNING")
        parent, parent_identity = _pin_parent(self._location)
        listener: socket.socket | None = None
        socket_identity: _SocketIdentity | None = None
        created = False
        try:
            try:
                os.stat(self._location.path, follow_symlinks=False)
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise HostIpcError("HOST_IPC_UNSAFE_PATH") from exc
            else:
                raise HostIpcError("HOST_IPC_ADDRESS_IN_USE")
            try:
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                listener.set_inheritable(False)
                listener.bind(self._location.path)
                created = True
            except OSError as exc:
                code = (
                    "HOST_IPC_ADDRESS_IN_USE"
                    if exc.errno == errno.EADDRINUSE
                    else "HOST_IPC_BIND_FAILED"
                )
                raise HostIpcError(code) from exc
            try:
                os.chmod(self._location.path, 0o600, follow_symlinks=False)
            except OSError as exc:
                raise HostIpcError("HOST_IPC_BIND_FAILED") from exc
            _verify_parent(self._location, parent_identity)
            socket_identity = _socket_identity(self._location)
            try:
                listener.listen(self._max_clients)
                listener.settimeout(0.2)
            except OSError as exc:
                raise HostIpcError("HOST_IPC_BIND_FAILED") from exc
            with self._state_lock:
                if self._closed:
                    raise HostIpcError("HOST_IPC_CLOSED")
                self._parent = parent
                self._parent_identity = parent_identity
                self._socket_identity = socket_identity
                self._listener = listener
                thread = threading.Thread(
                    target=self._accept_loop,
                    name="xsync-host-ipc",
                    daemon=True,
                )
                self._accept_thread = thread
                try:
                    thread.start()
                except RuntimeError as exc:
                    raise HostIpcError("HOST_IPC_START_FAILED") from exc
            return self
        except BaseException:
            with self._state_lock:
                if self._listener is listener:
                    self._listener = None
                    self._parent = None
                    self._parent_identity = None
                    self._socket_identity = None
                    self._accept_thread = None
            if listener is not None:
                try:
                    listener.close()
                except OSError:
                    pass
            if socket_identity is not None:
                self._unlink_if_owned(parent_identity, socket_identity)
            elif created:
                _unlink_created_socket(self._location, parent_identity)
            parent.close()
            raise

    def _record_fatal(self, code: str) -> None:
        with self._state_lock:
            if self._fatal_error is None:
                self._fatal_error = code
            self._stop.set()
            listener = self._listener
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass

    def _verify_live_address(self) -> None:
        parent_identity = self._parent_identity
        socket_identity = self._socket_identity
        if parent_identity is None or socket_identity is None:
            raise HostIpcError("HOST_IPC_NOT_RUNNING")
        _verify_parent(self._location, parent_identity)
        _verify_socket(self._location, socket_identity)

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._verify_live_address()
                listener = self._listener
                if listener is None:
                    return
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except HostIpcError as exc:
                self._record_fatal(exc.code)
                return
            except OSError:
                if not self._stop.is_set():
                    self._record_fatal("HOST_IPC_ACCEPT_FAILED")
                return
            if not self._slots.acquire(blocking=False):
                connection.close()
                continue
            with self._state_lock:
                if self._stop.is_set():
                    connection.close()
                    self._slots.release()
                    return
                worker = threading.Thread(
                    target=self._serve,
                    args=(connection,),
                    name="xsync-host-ipc-client",
                    daemon=True,
                )
                self._connections.add(connection)
                self._workers.add(worker)
                worker.start()

    def _serve(self, connection: socket.socket) -> None:
        try:
            connection.settimeout(self._request_timeout)
            request = _receive_frame(
                connection,
                maximum=MAX_HOST_API_REQUEST_BYTES,
                code="HOST_IPC_REQUEST_INVALID",
            )
            response = self._api.handle(request)
            if type(response) is not HostApiResponse:
                raise HostIpcError("HOST_IPC_RESPONSE_INVALID")
            _send_frame(connection, response.body, "HOST_IPC_RESPONSE_INVALID")
        except BaseException:
            pass
        finally:
            try:
                connection.close()
            except OSError:
                pass
            current = threading.current_thread()
            with self._state_lock:
                self._connections.discard(connection)
                self._workers.discard(current)
            self._slots.release()

    def _unlink_if_owned(
        self,
        parent_identity: SecureDirectoryIdentity,
        socket_identity: _SocketIdentity,
    ) -> None:
        try:
            _verify_parent(self._location, parent_identity)
            _verify_socket(self._location, socket_identity)
            os.unlink(self._location.path)
        except (HostIpcError, OSError):
            return

    def close(self) -> None:
        """Idempotently stop serving and remove only the pinned socket inode."""
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            self._stop.set()
            listener = self._listener
            self._listener = None
            connections = tuple(self._connections)
            accept_thread = self._accept_thread
            parent = self._parent
            parent_identity = self._parent_identity
            socket_identity = self._socket_identity
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        if (
            accept_thread is not None
            and accept_thread is not threading.current_thread()
        ):
            accept_thread.join(1.0)
        with self._state_lock:
            workers = tuple(self._workers)
        for worker in workers:
            if worker is not threading.current_thread():
                worker.join(1.0)
        if parent_identity is not None and socket_identity is not None:
            self._unlink_if_owned(parent_identity, socket_identity)
        if parent is not None:
            parent.close()

    def __enter__(self) -> HostIpcServer:
        """Start and return this bounded server owner."""
        return self.start()

    def __exit__(self, *_: object) -> None:
        """Close the private transport at context exit."""
        self.close()


class HostIpcClient:
    """Call one owner-only Host IPC endpoint with strict length framing."""

    def __init__(
        self,
        socket_path: str | PathLike[str],
        *,
        timeout: float | int = 5.0,
    ) -> None:
        self._location = _location(socket_path)
        self._timeout = _finite_timeout(timeout)

    def call(self, raw: bytes) -> HostApiResponse:
        """Send one request and require exactly one bounded framed response."""
        if type(raw) is not bytes or len(raw) > MAX_HOST_API_REQUEST_BYTES:
            raise HostIpcError("HOST_IPC_REQUEST_INVALID")
        if not hasattr(socket, "AF_UNIX"):
            raise HostIpcError("HOST_IPC_UNSUPPORTED")
        parent, parent_identity = _pin_parent(self._location)
        connection: socket.socket | None = None
        try:
            socket_identity = _socket_identity(self._location)
            try:
                connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                connection.set_inheritable(False)
                connection.settimeout(self._timeout)
                connection.connect(self._location.path)
            except OSError as exc:
                raise HostIpcError("HOST_IPC_CONNECT_FAILED") from exc
            _verify_parent(self._location, parent_identity)
            _verify_socket(self._location, socket_identity)
            _send_frame(connection, raw, "HOST_IPC_REQUEST_INVALID")
            body = _receive_frame(
                connection,
                maximum=MAX_HOST_IPC_RESPONSE_BYTES,
                code="HOST_IPC_RESPONSE_INVALID",
            )
            try:
                trailing = connection.recv(1)
            except OSError as exc:
                raise HostIpcError("HOST_IPC_RESPONSE_INVALID") from exc
            if trailing:
                raise HostIpcError("HOST_IPC_RESPONSE_INVALID")
            return HostApiResponse(body)
        finally:
            if connection is not None:
                connection.close()
            parent.close()


__all__ = [
    "MAX_HOST_IPC_RESPONSE_BYTES",
    "HostIpcClient",
    "HostIpcError",
    "HostIpcServer",
]
