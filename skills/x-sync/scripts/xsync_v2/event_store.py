"""Marker-linearized durable dialogue event log for X-Sync v2."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import hashlib
import re
from typing import cast

from .domain import (
    CommittedDialogueEvent,
    DialogueState,
    PauseCause,
    SessionDeactivationPrepared,
    TopicPaused,
    WorkDeadLettered,
    WorkFailed,
    WorkRequeued,
)
from .event_codec import (
    MAX_RECORD_BYTES,
    DialogueActor,
    DialogueCommandReceipt,
    DurableEffectIntent,
    MarkerEventRef,
    StoredDialogueEvent,
    TransactionMarker,
    build_state_snapshot,
    build_stored_event,
    build_transaction_marker,
    decode_stored_event,
    decode_transaction_marker,
    encode_state_snapshot,
    encode_stored_event,
    encode_transaction_marker,
    receipt_from_marker,
)
from .locking import DomainLockManager, LockError, SessionLockAuthority
from .secure_fs import SecureDirectory, SecureFsError
from .state_machine import conversation_version_delta, reduce, validate_state


class DialogueStoreError(RuntimeError):
    """A stable, path-free durable-store failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class EventMetadata:
    """Trusted metadata injected before an event is persisted."""

    occurred_at: str
    actor: DialogueActor
    causation_id: str | None


@dataclass(frozen=True, slots=True)
class DialogueCommitRequest:
    """One already-decided event batch awaiting durable commit."""

    session_id: str
    transaction_id: str
    request_digest: str
    registry_generation: int
    events: tuple[CommittedDialogueEvent, ...]
    metadata: tuple[EventMetadata, ...]
    effect_intents: tuple[DurableEffectIntent, ...] = ()


@dataclass(frozen=True, slots=True)
class DialogueTip:
    """The replayed canonical state and durable command index."""

    state: DialogueState
    last_event_hash: str | None
    last_marker_hash: str | None
    receipts: tuple[DialogueCommandReceipt, ...]
    last_registry_generation: int

    @property
    def last_event_id(self) -> str | None:
        """Return the final committed event id, if the log is non-empty."""
        if not self.receipts:
            return None
        return self.receipts[-1].event_ids[-1]


@dataclass(frozen=True, slots=True)
class DialogueCommitOutcome:
    """A durable commit receipt plus its resulting canonical state."""

    receipt: DialogueCommandReceipt
    state: DialogueState
    events: tuple[CommittedDialogueEvent, ...]
    replayed: bool
    projection_current: bool


FaultHook = Callable[[str], None]
MarkerPublicationGuard = Callable[[SessionLockAuthority], None]


def _no_fault(_step: str) -> None:
    return None


def session_directory_component(session_id: str) -> str:
    """Map an opaque Session id to a bounded, non-secret directory name."""
    if type(session_id) is not str or not session_id.strip():
        raise DialogueStoreError("INVALID_SESSION_ID")
    digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
    return f"session-{digest}"


def _event_name(sequence: int) -> str:
    return f"event-{sequence:020d}.json"


def _marker_name(first_sequence: int) -> str:
    return f"transaction-{first_sequence:020d}.json"


def _validate_event_batch_shape(
    events: tuple[CommittedDialogueEvent, ...],
) -> None:
    """Reject non-canonical version chains and partial work failures."""
    previous: CommittedDialogueEvent | None = None
    for event in events:
        if (
            type(event.sequence) is not int
            or event.sequence < 1
            or type(event.from_version) is not int
            or event.from_version < 0
            or type(event.to_version) is not int
        ):
            raise DialogueStoreError("INVALID_COMMIT_REQUEST")
        try:
            delta = conversation_version_delta(event.payload)
        except (TypeError, ValueError) as exc:
            raise DialogueStoreError("INVALID_COMMIT_REQUEST") from exc
        if event.to_version != event.from_version + delta:
            raise DialogueStoreError("EVENT_VERSION_CONFLICT")
        if previous is not None:
            if event.sequence != previous.sequence + 1:
                raise DialogueStoreError("EVENT_SEQUENCE_GAP")
            if event.from_version != previous.to_version:
                raise DialogueStoreError("EVENT_VERSION_CONFLICT")
        previous = event

    failure_types = {WorkFailed, WorkRequeued, WorkDeadLettered}
    if not any(type(event.payload) in failure_types for event in events):
        return
    if (
        len(events) != 2
        or type(events[0].payload) is not WorkFailed
        or type(events[1].payload) not in {WorkRequeued, WorkDeadLettered}
        or events[0].command_id != events[1].command_id
    ):
        raise DialogueStoreError("INVALID_WORK_FAILURE_BATCH")
    failed = events[0].payload
    resolution = cast(WorkRequeued | WorkDeadLettered, events[1].payload)
    if (
        resolution.failed_work_id != failed.work_id
        or resolution.failed_trigger != failed.trigger
        or resolution.failed_attempt != failed.attempt
        or resolution.failure != failed.failure
    ):
        raise DialogueStoreError("INVALID_WORK_FAILURE_BATCH")


class _DialogueTransactionLog:
    """A lock-requiring append log whose transaction marker is the commit."""

    def __init__(
        self,
        session_directory: SecureDirectory,
        initial_state: DialogueState,
        lock_manager: DomainLockManager,
        authority: SessionLockAuthority,
        *,
        fault_hook: FaultHook = _no_fault,
        read_only_existing: bool = False,
    ) -> None:
        if type(session_directory) is not SecureDirectory:
            raise DialogueStoreError("INVALID_STORE_DIRECTORY")
        if (
            type(initial_state) is not DialogueState
            or type(lock_manager) is not DomainLockManager
            or not callable(fault_hook)
            or type(read_only_existing) is not bool
        ):
            raise DialogueStoreError("INVALID_STORE_CONFIGURATION")
        try:
            validate_state(initial_state)
        except ValueError as exc:
            raise DialogueStoreError("INVALID_INITIAL_STATE") from exc
        if initial_state.sequence != 0:
            raise DialogueStoreError("INVALID_INITIAL_STATE")
        self._assert_authority(
            lock_manager,
            authority,
            initial_state.session_id,
        )
        self._session = session_directory
        open_child = (
            session_directory.open_directory
            if read_only_existing
            else session_directory.ensure_directory
        )
        opened: list[SecureDirectory] = []
        try:
            self._events = open_child("events")
            opened.append(self._events)
            self._transactions = open_child("transactions")
            opened.append(self._transactions)
            self._quarantine = open_child("quarantine")
            opened.append(self._quarantine)
        except BaseException:
            for directory in reversed(opened):
                directory.close()
            raise
        self._initial_state = initial_state
        self._locks = lock_manager
        self._fault_hook = fault_hook
        self._closed = False
        if not read_only_existing:
            try:
                self._session.recover_temporary_writes()
                self._events.recover_temporary_writes()
                self._transactions.recover_temporary_writes()
                self._quarantine.recover_temporary_writes()
                self._events.recover_quarantine_moves(self._quarantine)
            except SecureFsError as exc:
                raise DialogueStoreError(exc.code) from exc
        self._tip = self._full_recover(maintain=not read_only_existing)

    @classmethod
    def create(
        cls,
        dialogues_directory: SecureDirectory,
        initial_state: DialogueState,
        lock_manager: DomainLockManager,
        authority: SessionLockAuthority,
        *,
        fault_hook: FaultHook = _no_fault,
    ) -> _DialogueTransactionLog:
        """Create or open one hashed Session directory and fully replay it."""
        if type(dialogues_directory) is not SecureDirectory:
            raise DialogueStoreError("INVALID_STORE_DIRECTORY")
        if type(initial_state) is not DialogueState:
            raise DialogueStoreError("INVALID_INITIAL_STATE")
        cls._assert_authority(
            lock_manager,
            authority,
            initial_state.session_id,
        )
        component = session_directory_component(initial_state.session_id)
        session = dialogues_directory.ensure_directory(component)
        try:
            return cls(
                session,
                initial_state,
                lock_manager,
                authority,
                fault_hook=fault_hook,
            )
        except BaseException:
            session.close()
            raise

    @classmethod
    def open_existing(
        cls,
        dialogues_directory: SecureDirectory,
        initial_state: DialogueState,
        lock_manager: DomainLockManager,
        authority: SessionLockAuthority,
    ) -> _DialogueTransactionLog:
        """Audit one existing Session without creating or repairing entries."""
        if type(dialogues_directory) is not SecureDirectory:
            raise DialogueStoreError("INVALID_STORE_DIRECTORY")
        if type(initial_state) is not DialogueState:
            raise DialogueStoreError("INVALID_INITIAL_STATE")
        cls._assert_authority(
            lock_manager,
            authority,
            initial_state.session_id,
        )
        component = session_directory_component(initial_state.session_id)
        try:
            session = dialogues_directory.open_directory(component)
        except SecureFsError as exc:
            raise DialogueStoreError(exc.code) from exc
        try:
            return cls(
                session,
                initial_state,
                lock_manager,
                authority,
                read_only_existing=True,
            )
        except BaseException:
            session.close()
            raise

    def __enter__(self) -> _DialogueTransactionLog:
        self._require_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise DialogueStoreError("DIALOGUE_STORE_CLOSED")

    def close(self) -> None:
        """Close all owned Session directory descriptors."""
        if self._closed:
            return
        self._closed = True
        self._events.close()
        self._transactions.close()
        self._quarantine.close()
        self._session.close()

    def tip(self) -> DialogueTip:
        """Return the immutable in-memory tip after incremental synchronization."""
        self._require_open()
        self._synchronize()
        return self._tip

    def recover(self, authority: SessionLockAuthority) -> DialogueTip:
        """Perform a full marker/event audit and rebuild the state projection."""
        self._require_open()
        self._require_authority(authority)
        try:
            self._session.recover_temporary_writes()
            self._events.recover_temporary_writes()
            self._transactions.recover_temporary_writes()
            self._quarantine.recover_temporary_writes()
            self._events.recover_quarantine_moves(self._quarantine)
        except SecureFsError as exc:
            raise DialogueStoreError(exc.code) from exc
        self._tip = self._full_recover()
        self._write_projection_best_effort()
        return self._tip

    def read_committed(
        self, *, after_sequence: int = 0
    ) -> tuple[CommittedDialogueEvent, ...]:
        """Read committed events after ``after_sequence`` in strict order."""
        self._require_open()
        if type(after_sequence) is not int or after_sequence < 0:
            raise DialogueStoreError("INVALID_SEQUENCE")
        self._synchronize()
        result: list[CommittedDialogueEvent] = []
        first = 1
        while first <= self._tip.state.sequence:
            marker = self._read_marker(first)
            for reference in marker.events:
                record = self._read_event(reference.sequence)
                self._match_reference(record, reference)
                if reference.sequence > after_sequence:
                    result.append(record.event)
            first = marker.to_sequence + 1
        return tuple(result)

    def read_effect_intents(self) -> tuple[DurableEffectIntent, ...]:
        """Replay all committed durable effects in marker order."""
        self._require_open()
        self._synchronize()
        result: list[DurableEffectIntent] = []
        first = 1
        while first <= self._tip.state.sequence:
            marker = self._read_marker(first)
            result.extend(marker.effect_intents)
            first = marker.to_sequence + 1
        return tuple(result)

    def commit(
        self,
        request: DialogueCommitRequest,
        authority: SessionLockAuthority,
        *,
        marker_publication_guard: MarkerPublicationGuard | None = None,
    ) -> DialogueCommitOutcome:
        """Commit a batch behind the live Session and optional Host fences.

        Event records may be durably published before this method commits.
        The transaction marker remains the sole linearization point: its
        private temporary file is staged first, then both fences are checked
        by one publication guard immediately before the no-replace publish.
        """
        self._require_open()
        self._require_authority(authority)
        if marker_publication_guard is not None and not callable(
            marker_publication_guard
        ):
            raise DialogueStoreError("INVALID_MARKER_PUBLICATION_GUARD")
        self._validate_request(request)
        self._tip = self._full_recover(maintain=False)
        command_id = request.events[0].command_id
        existing = next(
            (
                item
                for item in self._tip.receipts
                if item.command_id == command_id
            ),
            None,
        )
        if existing is not None:
            if existing.request_digest != request.request_digest:
                raise DialogueStoreError("IDEMPOTENCY_CONFLICT")
            original_tip = self._tip_at_sequence(existing.to_sequence)
            if (
                not original_tip.receipts
                or original_tip.receipts[-1] != existing
            ):
                raise DialogueStoreError("TRANSACTION_RECEIPT_MISMATCH")
            events = self._events_between(
                existing.from_sequence,
                existing.to_sequence,
            )
            original_state = original_tip.state
            if build_state_snapshot(
                original_state,
                last_event_hash=self._event_hash_at(existing.to_sequence),
                last_marker_hash=existing.marker_hash,
                receipts=tuple(
                    item
                    for item in original_tip.receipts
                    if item.to_sequence <= existing.to_sequence
                ),
            ).state_digest != existing.state_digest:
                raise DialogueStoreError("TRANSACTION_STATE_MISMATCH")
            projection_current = self._write_projection_best_effort()
            return DialogueCommitOutcome(
                existing,
                original_state,
                events,
                True,
                projection_current,
            )
        self._reject_identity_reuse(request)
        if (
            self._tip.last_registry_generation
            != self._initial_state.registry_generation
        ):
            raise DialogueStoreError("SESSION_DEACTIVATED")

        state, stored_events, encoded_events, marker, encoded_marker = (
            self._prepare_transaction(request)
        )
        for record, encoded in zip(stored_events, encoded_events, strict=True):
            self._publish_event(record, encoded)
            self._fault_hook("event_published")
        self._fault_hook("before_marker")
        host_guard_failure: BaseException | None = None

        def guard_marker_publication() -> None:
            nonlocal host_guard_failure
            # The Host fence may itself run arbitrary trusted checks.  Verify
            # the Session authority on both sides so a callback cannot make a
            # now-expired authority publish a canonical marker.
            self._require_authority(authority)
            if marker_publication_guard is not None:
                try:
                    marker_publication_guard(authority)
                except BaseException as exc:
                    host_guard_failure = exc
                    raise
            self._require_authority(authority)

        try:
            self._transactions.write_immutable_guarded(
                _marker_name(marker.from_sequence),
                encoded_marker,
                guard_marker_publication,
            )
        except SecureFsError as exc:
            if exc is host_guard_failure:
                raise
            raise DialogueStoreError(exc.code) from exc
        self._fault_hook("marker_committed")

        receipt = receipt_from_marker(marker)
        self._tip = DialogueTip(
            state,
            stored_events[-1].event_hash,
            marker.marker_hash,
            (*self._tip.receipts, receipt),
            marker.registry_generation,
        )
        projection_current = self._write_projection_best_effort()
        return DialogueCommitOutcome(
            receipt,
            state,
            request.events,
            False,
            projection_current,
        )

    @staticmethod
    def _assert_authority(
        locks: object,
        authority: object,
        session_id: object,
    ) -> None:
        if (
            type(locks) is not DomainLockManager
            or type(authority) is not SessionLockAuthority
        ):
            raise DialogueStoreError("LOCK_AUTHORITY_REQUIRED")
        try:
            locks.assert_session_authority(authority, session_id)
        except LockError as exc:
            raise DialogueStoreError("LOCK_AUTHORITY_REQUIRED") from exc

    def _require_authority(self, authority: object) -> None:
        self._assert_authority(
            self._locks,
            authority,
            self._initial_state.session_id,
        )

    def _validate_request(self, request: object) -> None:
        if (
            type(request) is not DialogueCommitRequest
            or request.session_id != self._initial_state.session_id
            or type(request.transaction_id) is not str
            or not request.transaction_id.strip()
            or type(request.request_digest) is not str
            or re.fullmatch(r"sha256:[0-9a-f]{64}", request.request_digest)
            is None
            or type(request.registry_generation) is not int
            or request.registry_generation < 1
            or type(request.events) is not tuple
            or not request.events
            or type(request.metadata) is not tuple
            or len(request.metadata) != len(request.events)
            or any(type(item) is not CommittedDialogueEvent for item in request.events)
            or any(type(item) is not EventMetadata for item in request.metadata)
            or type(request.effect_intents) is not tuple
            or any(
                type(item) is not DurableEffectIntent
                for item in request.effect_intents
            )
        ):
            raise DialogueStoreError("INVALID_COMMIT_REQUEST")
        command_id = request.events[0].command_id
        _validate_event_batch_shape(request.events)
        if any(item.command_id != command_id for item in request.events):
            raise DialogueStoreError("INVALID_COMMIT_REQUEST")

    def _reject_identity_reuse(self, request: DialogueCommitRequest) -> None:
        event_ids = tuple(item.event_id for item in request.events)
        if len(set(event_ids)) != len(event_ids):
            raise DialogueStoreError("DUPLICATE_EVENT_ID")
        committed_event_ids = {
            event_id
            for receipt in self._tip.receipts
            for event_id in receipt.event_ids
        }
        if any(event_id in committed_event_ids for event_id in event_ids):
            raise DialogueStoreError("DUPLICATE_EVENT_ID")
        requested_intent_ids = tuple(
            item.intent_id for item in request.effect_intents
        )
        if len(requested_intent_ids) != len(set(requested_intent_ids)):
            raise DialogueStoreError("DUPLICATE_EFFECT_INTENT_ID")
        committed_intent_ids: set[str] = set()
        first = 1
        while first <= self._tip.state.sequence:
            marker = self._read_marker(first)
            committed_intent_ids.update(
                item.intent_id for item in marker.effect_intents
            )
            first = marker.to_sequence + 1
        if any(item in committed_intent_ids for item in requested_intent_ids):
            raise DialogueStoreError("DUPLICATE_EFFECT_INTENT_ID")
        if any(
            receipt.transaction_id == request.transaction_id
            for receipt in self._tip.receipts
        ):
            raise DialogueStoreError("DUPLICATE_TRANSACTION_ID")

    def _validate_registry_generation(
        self,
        registry_generation: int,
        events: tuple[CommittedDialogueEvent, ...],
    ) -> None:
        base = self._initial_state.registry_generation
        if registry_generation == base:
            if any(
                type(item.payload) is SessionDeactivationPrepared
                or (
                    type(item.payload) is TopicPaused
                    and item.payload.cause is PauseCause.SESSION_DEACTIVATION
                )
                for item in events
            ):
                raise DialogueStoreError("REGISTRY_GENERATION_MISMATCH")
            return
        if registry_generation != base + 1:
            raise DialogueStoreError("REGISTRY_GENERATION_MISMATCH")
        if any(
            not (
                (
                    type(item.payload) is TopicPaused
                    and item.payload.cause is PauseCause.SESSION_DEACTIVATION
                    and item.payload.fence_generation == registry_generation
                    and type(item.payload.handoff_id) is str
                    and bool(item.payload.handoff_id.strip())
                )
                or (
                    type(item.payload) is SessionDeactivationPrepared
                    and item.payload.fence_generation == registry_generation
                    and type(item.payload.handoff_id) is str
                    and bool(item.payload.handoff_id.strip())
                )
            )
            for item in events
        ):
            raise DialogueStoreError("REGISTRY_GENERATION_MISMATCH")

    def _prepare_transaction(
        self,
        request: DialogueCommitRequest,
    ) -> tuple[
        DialogueState,
        tuple[StoredDialogueEvent, ...],
        tuple[bytes, ...],
        TransactionMarker,
        bytes,
    ]:
        self._validate_registry_generation(
            request.registry_generation,
            request.events,
        )
        previous_event_hash = self._tip.last_event_hash
        stored_events: list[StoredDialogueEvent] = []
        encoded_events: list[bytes] = []
        state = self._tip.state
        try:
            for event, metadata in zip(
                request.events,
                request.metadata,
                strict=True,
            ):
                state = reduce(state, event)
                record = build_stored_event(
                    request.session_id,
                    event,
                    previous_event_hash,
                    registry_generation=request.registry_generation,
                    occurred_at=metadata.occurred_at,
                    actor=metadata.actor,
                    causation_id=metadata.causation_id,
                )
                encoded = encode_stored_event(record)
                if len(encoded) > MAX_RECORD_BYTES:
                    raise DialogueStoreError("FILE_TOO_LARGE")
                stored_events.append(record)
                encoded_events.append(encoded)
                previous_event_hash = record.event_hash
            marker = build_transaction_marker(
                session_id=request.session_id,
                transaction_id=request.transaction_id,
                command_id=request.events[0].command_id,
                request_digest=request.request_digest,
                registry_generation=request.registry_generation,
                previous_marker_hash=self._tip.last_marker_hash,
                state=state,
                events=tuple(stored_events),
                effect_intents=request.effect_intents,
            )
            encoded_marker = encode_transaction_marker(marker)
            if len(encoded_marker) > MAX_RECORD_BYTES:
                raise DialogueStoreError("FILE_TOO_LARGE")
        except DialogueStoreError:
            raise
        except (AttributeError, TypeError, ValueError) as exc:
            code = str(exc) if isinstance(exc, ValueError) else "INVALID_COMMIT_REQUEST"
            raise DialogueStoreError(code) from exc
        return (
            state,
            tuple(stored_events),
            tuple(encoded_events),
            marker,
            encoded_marker,
        )

    def _publish_event(self, record: StoredDialogueEvent, encoded: bytes) -> None:
        name = _event_name(record.event.sequence)
        try:
            self._events.write_immutable(name, encoded)
            return
        except SecureFsError as exc:
            if exc.code != "IMMUTABLE_EXISTS":
                raise DialogueStoreError(exc.code) from exc
        try:
            existing = self._events.read_bytes(name, max_bytes=MAX_RECORD_BYTES)
        except SecureFsError as exc:
            raise DialogueStoreError(exc.code) from exc
        if existing == encoded:
            return
        quarantine_name = f"orphan-{hashlib.sha256(existing).hexdigest()}.json"
        try:
            self._events.quarantine(
                name,
                destination=self._quarantine,
                destination_component=quarantine_name,
            )
            self._events.write_immutable(name, encoded)
        except SecureFsError as exc:
            raise DialogueStoreError(exc.code) from exc

    def _read_marker(self, first_sequence: int) -> TransactionMarker:
        try:
            raw = self._transactions.read_bytes(
                _marker_name(first_sequence),
                max_bytes=MAX_RECORD_BYTES,
            )
        except SecureFsError as exc:
            raise DialogueStoreError(exc.code) from exc
        try:
            return decode_transaction_marker(raw)
        except ValueError as exc:
            raise DialogueStoreError(str(exc)) from exc

    def _read_optional_marker(
        self, first_sequence: int
    ) -> TransactionMarker | None:
        try:
            return self._read_marker(first_sequence)
        except DialogueStoreError as exc:
            if exc.code == "FILE_NOT_FOUND":
                return None
            raise

    def _read_event(self, sequence: int) -> StoredDialogueEvent:
        try:
            raw = self._events.read_bytes(
                _event_name(sequence),
                max_bytes=MAX_RECORD_BYTES,
            )
        except SecureFsError as exc:
            raise DialogueStoreError(exc.code) from exc
        try:
            return decode_stored_event(raw)
        except ValueError as exc:
            raise DialogueStoreError(str(exc)) from exc

    def _events_between(
        self, first_sequence: int, last_sequence: int
    ) -> tuple[CommittedDialogueEvent, ...]:
        return tuple(
            self._read_event(sequence).event
            for sequence in range(first_sequence, last_sequence + 1)
        )

    def _tip_at_sequence(self, target: int) -> DialogueTip:
        tip = DialogueTip(
            self._initial_state,
            None,
            None,
            (),
            self._initial_state.registry_generation,
        )
        while tip.state.sequence < target:
            marker = self._read_marker(tip.state.sequence + 1)
            if marker.to_sequence > target:
                raise DialogueStoreError("TRANSACTION_SEQUENCE_GAP")
            tip = self._apply_marker(tip, marker)
        if tip.state.sequence != target:
            raise DialogueStoreError("TRANSACTION_SEQUENCE_GAP")
        return tip

    def _event_hash_at(self, sequence: int) -> str:
        return self._read_event(sequence).event_hash

    @staticmethod
    def _match_reference(
        record: StoredDialogueEvent, reference: MarkerEventRef
    ) -> None:
        if (
            type(reference) is not MarkerEventRef
            or record.event.sequence != reference.sequence
            or record.event.event_id != reference.event_id
            or record.event_hash != reference.event_hash
        ):
            raise DialogueStoreError("TRANSACTION_EVENT_MISMATCH")

    def _apply_marker(self, tip: DialogueTip, marker: TransactionMarker) -> DialogueTip:
        if (
            marker.session_id != self._initial_state.session_id
            or marker.from_sequence != tip.state.sequence + 1
            or marker.previous_marker_hash != tip.last_marker_hash
            or tip.last_registry_generation
            != self._initial_state.registry_generation
        ):
            raise DialogueStoreError("TRANSACTION_CHAIN_MISMATCH")
        state = tip.state
        previous_event_hash = tip.last_event_hash
        records: list[StoredDialogueEvent] = []
        committed_events: list[CommittedDialogueEvent] = []
        for reference in marker.events:
            record = self._read_event(reference.sequence)
            self._match_reference(record, reference)
            if (
                record.session_id != self._initial_state.session_id
                or record.previous_event_hash != previous_event_hash
                or record.registry_generation != marker.registry_generation
                or record.event.command_id != marker.command_id
            ):
                raise DialogueStoreError("EVENT_CHAIN_MISMATCH")
            records.append(record)
            previous_event_hash = record.event_hash
            committed_events.append(record.event)
        _validate_event_batch_shape(tuple(committed_events))
        previous_event_hash = tip.last_event_hash
        for record in records:
            try:
                state = reduce(state, record.event)
            except ValueError as exc:
                raise DialogueStoreError(str(exc)) from exc
            previous_event_hash = record.event_hash
        self._validate_registry_generation(
            marker.registry_generation,
            tuple(committed_events),
        )
        try:
            receipt = receipt_from_marker(marker)
            snapshot = build_state_snapshot(
                state,
                last_event_hash=previous_event_hash,
                last_marker_hash=marker.marker_hash,
                receipts=(*tip.receipts, receipt),
            )
        except ValueError as exc:
            raise DialogueStoreError(str(exc)) from exc
        if snapshot.state_digest != marker.state_digest:
            raise DialogueStoreError("TRANSACTION_STATE_MISMATCH")
        return DialogueTip(
            state,
            previous_event_hash,
            marker.marker_hash,
            (*tip.receipts, receipt),
            marker.registry_generation,
        )

    def _synchronize(self) -> None:
        while True:
            marker = self._read_optional_marker(self._tip.state.sequence + 1)
            if marker is None:
                return
            self._tip = self._apply_marker(self._tip, marker)

    def _full_recover(self, *, maintain: bool = True) -> DialogueTip:
        tip = DialogueTip(
            self._initial_state,
            None,
            None,
            (),
            self._initial_state.registry_generation,
        )
        used_events: set[str] = set()
        used_markers: set[str] = set()
        while True:
            first = tip.state.sequence + 1
            marker = self._read_optional_marker(first)
            if marker is None:
                break
            marker_name = _marker_name(first)
            used_markers.add(marker_name)
            used_events.update(_event_name(item.sequence) for item in marker.events)
            tip = self._apply_marker(tip, marker)
        self._reject_unexpected_markers(used_markers)
        if maintain:
            self._quarantine_orphan_events(used_events)
        self._tip = tip
        if maintain:
            self._write_projection_best_effort()
        return tip

    def _reject_unexpected_markers(self, used: set[str]) -> None:
        try:
            entries = self._transactions.list_entries()
        except SecureFsError as exc:
            raise DialogueStoreError(exc.code) from exc
        unexpected = tuple(item for item in entries if item not in used)
        if unexpected:
            raise DialogueStoreError("TRANSACTION_SEQUENCE_GAP")

    def _quarantine_orphan_events(self, used: set[str]) -> None:
        try:
            entries = self._events.list_entries()
            for name in entries:
                if name in used:
                    continue
                data = self._events.read_bytes(name, max_bytes=MAX_RECORD_BYTES)
                destination = f"orphan-{hashlib.sha256(data).hexdigest()}.json"
                self._events.quarantine(
                    name,
                    destination=self._quarantine,
                    destination_component=destination,
                )
        except SecureFsError as exc:
            raise DialogueStoreError(exc.code) from exc

    def _write_projection_best_effort(self) -> bool:
        try:
            snapshot = build_state_snapshot(
                self._tip.state,
                last_event_hash=self._tip.last_event_hash,
                last_marker_hash=self._tip.last_marker_hash,
                receipts=self._tip.receipts,
            )
            self._session.replace_derived(
                "state.json", encode_state_snapshot(snapshot)
            )
            self._fault_hook("state_projected")
            return True
        except (SecureFsError, ValueError):
            return False
