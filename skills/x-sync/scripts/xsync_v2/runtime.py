"""Single composition root for the model-neutral X-Sync v2 runtime.

Host adapters may differ at their transport boundary, but they must receive
the same coordinator, state machine, lease service, Host control plane,
Browser service, and Observer graph.  This module is the only production
place that wires those objects together.
"""

from __future__ import annotations

from collections.abc import Callable
import os
from pathlib import Path
import threading

from .browser_server import LoopbackBrowserServer
from .browser_service import BrowserCommandService, Clock as BrowserClock
from .coordinator import (
    DialogueCoordinator,
    DialogueResolution,
    DialogueSessionConfig,
    EvidenceVerifier,
)
from .dispatch import AfterCommitDispatcher, AfterCommitReport
from .evidence import SessionEvidenceStore
from .host_control import HostContextProvider, HostControl, MonotonicClock
from .host_api import HostApi
from .host_ipc import HostIpcServer
from .host_work import HostWorkService, authoritative_work_snapshot
from .lease_store import (
    Clock as LeaseClock,
    LeaseStore,
    RuntimeAuthorityVerifier,
)
from .locking import DomainLockManager
from .observer import ObserverHub
from .observers.public_stream import PublicStreamObserver
from .observers.work_wake import WorkWakeHint, WorkWakeObserver
from .repository_context import RepositoryHostContextProvider
from .secure_fs import SecureDirectory
from .submission_store import SubmissionHandleStore


class DialogueRuntimeError(RuntimeError):
    """Stable composition or lifecycle failure with no filesystem detail."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class _WakeRelay:
    """Break the construction cycle without becoming a domain dependency."""

    def __init__(self) -> None:
        self._target: Callable[[WorkWakeHint], None] | None = None
        self._lock = threading.Lock()

    def bind(self, control: HostControl) -> None:
        if type(control) is not HostControl:
            raise DialogueRuntimeError("INVALID_WAKE_TARGET")
        with self._lock:
            if self._target is not None:
                raise DialogueRuntimeError("WAKE_TARGET_ALREADY_BOUND")
            self._target = control.notify

    def notify(self, hint: WorkWakeHint) -> None:
        with self._lock:
            target = self._target
        if target is None:
            raise DialogueRuntimeError("WAKE_TARGET_NOT_BOUND")
        target(hint)


def _state_path(value: str | os.PathLike[str]) -> Path:
    try:
        raw = os.fspath(value)
    except TypeError as exc:
        raise DialogueRuntimeError("INVALID_STATE_DIRECTORY") from exc
    if type(raw) is not str or not raw or "\x00" in raw:
        raise DialogueRuntimeError("INVALID_STATE_DIRECTORY")
    path = Path(raw)
    if not path.is_absolute():
        raise DialogueRuntimeError("INVALID_STATE_DIRECTORY")
    return path


class DialogueRuntime:
    """Own one complete, Host-neutral X-Sync v2 runtime graph.

    ``state_directory`` must already exist as an owner-only directory.  A
    future thin CLI is responsible for choosing and securely creating that
    repository-local anchor; this composition root never guesses a project.
    """

    def __init__(
        self,
        state_directory: str | os.PathLike[str],
        registry_id: str,
        runtime_epoch: str,
        *,
        evidence_verifier: EvidenceVerifier | None = None,
        context_provider: HostContextProvider | None = None,
        repository_directory: str | os.PathLike[str] | None = None,
        repository_id: str | None = None,
        runtime_authority_verifier: RuntimeAuthorityVerifier,
        lease_clock: LeaseClock,
        browser_clock: BrowserClock,
        monotonic_clock: MonotonicClock,
        durable_poll_interval: float | int = 1.0,
        stream_retention_limit: int = 256,
        subscriber_queue_limit: int = 64,
    ) -> None:
        path = _state_path(state_directory)
        repository_mode = repository_directory is not None or repository_id is not None
        if repository_mode:
            if (
                repository_directory is None
                or repository_id is None
                or evidence_verifier is not None
                or context_provider is not None
            ):
                raise DialogueRuntimeError("INVALID_RUNTIME_CONFIGURATION")
        elif evidence_verifier is None or context_provider is None:
            raise DialogueRuntimeError("INVALID_RUNTIME_CONFIGURATION")
        root: SecureDirectory | None = None
        dialogues: SecureDirectory | None = None
        locks: DomainLockManager | None = None
        evidence_store: SessionEvidenceStore | None = None
        submission_store: SubmissionHandleStore | None = None
        try:
            root = SecureDirectory.open(path)
            dialogues = root.ensure_directory("dialogues")
            locks = DomainLockManager(path / "locks")
            if repository_mode:
                assert repository_directory is not None
                assert repository_id is not None
                evidence_store = SessionEvidenceStore(
                    repository_directory,
                    repository_id,
                    dialogues,
                    locks,
                )
                active_evidence_verifier: EvidenceVerifier = evidence_store.verify
            else:
                assert evidence_verifier is not None
                active_evidence_verifier = evidence_verifier

            public_stream = PublicStreamObserver(
                retention_limit=stream_retention_limit,
                subscriber_queue_limit=subscriber_queue_limit,
            )
            wake_relay = _WakeRelay()
            hub = ObserverHub(locks)
            hub.register_fixed(public_stream)
            hub.register_fixed(WorkWakeObserver(wake_relay))
            hub.freeze()
            dispatcher = AfterCommitDispatcher(hub)

            coordinator = DialogueCoordinator(
                dialogues,
                locks,
                registry_id,
                evidence_verifier=active_evidence_verifier,
                after_commit_dispatcher=dispatcher,
            )
            leases = LeaseStore(
                dialogues,
                locks,
                runtime_epoch,
                lease_clock,
                snapshot_loader=lambda session_id, authority: (
                    authoritative_work_snapshot(
                        coordinator,
                        session_id,
                        authority,
                    )
                ),
                runtime_authority_verifier=runtime_authority_verifier,
            )
            submission_store = SubmissionHandleStore(dialogues, locks)
            work_service = HostWorkService(coordinator, leases, submission_store)
            if evidence_store is not None:
                active_context_provider: HostContextProvider = (
                    RepositoryHostContextProvider(coordinator, evidence_store)
                )
            else:
                assert context_provider is not None
                active_context_provider = context_provider
            host_control = HostControl(
                coordinator,
                locks,
                leases,
                work_service,
                active_context_provider,
                monotonic_clock=monotonic_clock,
                durable_poll_interval=durable_poll_interval,
            )
            host_api = HostApi(host_control)
            wake_relay.bind(host_control)
            browser_commands = BrowserCommandService(
                coordinator,
                active_evidence_verifier,
                clock=browser_clock,
            )
        except BaseException:
            if submission_store is not None:
                submission_store.close()
            if evidence_store is not None:
                evidence_store.close()
            if locks is not None:
                locks.close()
            if dialogues is not None:
                dialogues.close()
            if root is not None:
                root.close()
            raise

        self._root = root
        self._dialogues = dialogues
        self._locks = locks
        self._coordinator = coordinator
        self._leases = leases
        self._work_service = work_service
        self._submission_store = submission_store
        self._host_control = host_control
        self._host_api = host_api
        self._browser_commands = browser_commands
        self._public_stream = public_stream
        self._dispatcher = dispatcher
        self._evidence_store = evidence_store
        self._browser_server: LoopbackBrowserServer | None = None
        self._host_ipc_server: HostIpcServer | None = None
        self._lifecycle_lock = threading.Lock()
        self._closed = False

    def _require_open(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                raise DialogueRuntimeError("RUNTIME_CLOSED")

    @property
    def host(self) -> HostControl:
        """Return the sole Host-neutral wait/claim/publish control plane."""
        self._require_open()
        return self._host_control

    @property
    def host_api(self) -> HostApi:
        """Return the sole strict JSON adapter over the Host control plane."""
        self._require_open()
        return self._host_api

    @property
    def browser(self) -> BrowserCommandService:
        """Return the sole authenticated-Browser command application service."""
        self._require_open()
        return self._browser_commands

    @property
    def public_stream(self) -> PublicStreamObserver:
        """Return the bounded Browser-safe after-commit projection."""
        self._require_open()
        return self._public_stream

    @property
    def evidence(self) -> SessionEvidenceStore:
        """Return the Session evidence store in repository-backed mode."""
        self._require_open()
        if self._evidence_store is None:
            raise DialogueRuntimeError("EVIDENCE_STORE_NOT_CONFIGURED")
        return self._evidence_store

    def resolve(self, config: DialogueSessionConfig) -> DialogueResolution:
        """Resolve or hand off through the canonical Registry coordinator."""
        self._require_open()
        return self._coordinator.resolve(config)

    def recover(self) -> DialogueResolution | None:
        """Finish durable recovery before opening product command ingress."""
        self._require_open()
        return self._coordinator.recover()

    def replay_committed(self) -> AfterCommitReport | None:
        """Force durable Observer catch-up for every registered dialogue."""
        self._require_open()
        return self._coordinator.replay_committed()

    def start_browser(
        self,
        session_id: str,
        capability: str,
        *,
        port: int = 0,
        keepalive_seconds: float | int = 15.0,
    ) -> LoopbackBrowserServer:
        """Start the one owner-only loopback Browser transport."""
        self._require_open()
        self._browser_commands.current(session_id)
        with self._lifecycle_lock:
            if self._closed:
                raise DialogueRuntimeError("RUNTIME_CLOSED")
            if self._browser_server is not None:
                raise DialogueRuntimeError("BROWSER_ALREADY_RUNNING")
            server = LoopbackBrowserServer(
                self._browser_commands,
                self._public_stream,
                session_id=session_id,
                capability=capability,
                port=port,
                keepalive_seconds=keepalive_seconds,
            )
            try:
                server.start()
            except BaseException:
                server.close()
                raise
            self._browser_server = server
            return server

    def close_browser(self) -> None:
        """Idempotently stop the Browser transport while keeping core state."""
        with self._lifecycle_lock:
            server = self._browser_server
            self._browser_server = None
        if server is not None:
            server.close()

    def start_host_ipc(
        self,
        socket_path: str | os.PathLike[str],
        *,
        max_clients: int = 8,
        request_timeout: float | int = 5.0,
    ) -> HostIpcServer:
        """Start the one owner-only Unix Host JSON transport."""
        self._require_open()
        with self._lifecycle_lock:
            if self._closed:
                raise DialogueRuntimeError("RUNTIME_CLOSED")
            if self._host_ipc_server is not None:
                raise DialogueRuntimeError("HOST_IPC_ALREADY_RUNNING")
            server = HostIpcServer(
                self._host_api,
                socket_path,
                max_clients=max_clients,
                request_timeout=request_timeout,
            )
            try:
                server.start()
            except BaseException:
                server.close()
                raise
            self._host_ipc_server = server
            return server

    def close_host_ipc(self) -> None:
        """Idempotently stop Host IPC while preserving canonical state."""
        with self._lifecycle_lock:
            server = self._host_ipc_server
            self._host_ipc_server = None
        if server is not None:
            server.close()

    def close(self) -> None:
        """Idempotently release Browser, lock, and anchored directory handles."""
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            server = self._browser_server
            self._browser_server = None
            host_ipc_server = self._host_ipc_server
            self._host_ipc_server = None
        error: BaseException | None = None
        if server is not None:
            try:
                server.close()
            except BaseException as exc:
                error = exc
        if host_ipc_server is not None:
            try:
                host_ipc_server.close()
            except BaseException as exc:
                if error is None:
                    error = exc
        if self._evidence_store is not None:
            self._evidence_store.close()
        self._submission_store.close()
        try:
            self._locks.close()
        except BaseException as exc:
            if error is None:
                error = exc
        self._dialogues.close()
        self._root.close()
        if error is not None:
            raise error

    def __enter__(self) -> DialogueRuntime:
        """Return this already-open runtime for bounded ownership."""
        self._require_open()
        return self

    def __exit__(self, *_: object) -> None:
        """Release all owned runtime resources at context exit."""
        self.close()
