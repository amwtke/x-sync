"""Marker-linearized durable transaction log for the X-Sync registry."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import hashlib
import re

from .locking import (
    DomainLockManager,
    LockError,
    RegistryLockAuthority,
    RegistryLockMode,
)
from .registry import (
    Activated,
    CommandReceipt,
    CommittedRegistryEvent,
    CommittedRegistryTransaction,
    Deactivated,
    DeactivationStarted,
    DialogueCreated,
    PendingRegistryEvent,
    RegistryAccepted,
    RegistryState,
    initial_registry_state,
    reduce,
)
from .registry_codec import (
    MAX_RECORD_BYTES,
    RegistryMarkerEventRef,
    RegistryTransactionMarker,
    StoredRegistryEvent,
    build_committed_registry_transaction,
    build_registry_state_snapshot,
    build_registry_transaction_marker,
    build_stored_registry_event,
    decode_registry_transaction_marker,
    decode_stored_registry_event,
    encode_registry_state_snapshot,
    encode_registry_transaction_marker,
    encode_stored_registry_event,
    registry_state_digest,
    validate_committed_registry_transaction,
)
from .secure_fs import SecureDirectory, SecureFsError


class RegistryStoreError(RuntimeError):
    """A stable, path-free registry persistence failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class RegistryCommitRequest:
    """One accepted batch plus caller-injected durable identities."""

    transaction_id: str
    event_ids: tuple[str, ...]
    expected_registry_sequence: int
    expected_generation: int
    accepted: RegistryAccepted


@dataclass(frozen=True, slots=True)
class RegistryTip:
    """The replayed canonical registry state and hash-chain tips."""

    state: RegistryState
    last_event_hash: str | None
    last_marker_hash: str | None

    @property
    def last_event_id(self) -> str | None:
        """Return the final committed registry event identity, if any."""
        if not self.state.receipts:
            return None
        return self.state.receipts[-1].event_ids[-1]


@dataclass(frozen=True, slots=True)
class RegistryCommitOutcome:
    """The durable receipt, canonical state, and committed registry batch."""

    receipt: CommandReceipt
    state: RegistryState
    transaction: CommittedRegistryTransaction
    replayed: bool
    projection_current: bool


FaultHook = Callable[[str], None]


def _no_fault(_step: str) -> None:
    return None


_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_HASH_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
_MARKER_PATTERN = re.compile(
    r"(?P<first>[0-9]{20})-(?P<last>[0-9]{20})-"
    r"(?P<transaction>[A-Za-z0-9][A-Za-z0-9._:-]{0,127})\.json\Z"
)


def _valid_id(value: object) -> bool:
    return type(value) is str and _ID_PATTERN.fullmatch(value) is not None


def _valid_hash(value: object) -> bool:
    return type(value) is str and _HASH_PATTERN.fullmatch(value) is not None


def _event_name(sequence: int, event_id: str) -> str:
    return f"{sequence:020d}-{event_id}.json"


def _marker_name(
    first_sequence: int, last_sequence: int, transaction_id: str
) -> str:
    return (
        f"{first_sequence:020d}-{last_sequence:020d}-"
        f"{transaction_id}.json"
    )


def _is_valid_batch_shape(accepted: RegistryAccepted) -> bool:
    payload_types = tuple(type(item.payload) for item in accepted.events)
    return payload_types in {
        (DialogueCreated, Activated),
        (DialogueCreated, DeactivationStarted),
        (DeactivationStarted,),
        (Deactivated, Activated),
    }


class _RegistryTransactionLog:
    """An EX-lock-requiring append log whose marker is the commit point."""

    def __init__(
        self,
        registry_directory: SecureDirectory,
        initial_state: RegistryState,
        locks: DomainLockManager,
        authority: RegistryLockAuthority,
        *,
        fault_hook: FaultHook = _no_fault,
    ) -> None:
        if type(registry_directory) is not SecureDirectory:
            raise RegistryStoreError("INVALID_REGISTRY_STORE_DIRECTORY")
        if type(locks) is not DomainLockManager or not callable(fault_hook):
            raise RegistryStoreError("INVALID_REGISTRY_STORE_CONFIGURATION")
        self._assert_exclusive(locks, authority)
        if type(initial_state) is not RegistryState:
            raise RegistryStoreError("INVALID_INITIAL_REGISTRY_STATE")
        try:
            expected = initial_registry_state(initial_state.registry_id)
        except (AttributeError, ValueError) as exc:
            raise RegistryStoreError("INVALID_INITIAL_REGISTRY_STATE") from exc
        if initial_state != expected:
            raise RegistryStoreError("INVALID_INITIAL_REGISTRY_STATE")

        self._registry = registry_directory
        self._locks = locks
        self._events = registry_directory.ensure_directory("events")
        self._transactions = registry_directory.ensure_directory("transactions")
        self._quarantine = registry_directory.ensure_directory("quarantine")
        self._initial_state = initial_state
        self._fault_hook = fault_hook
        self._closed = False
        self._used_event_names: set[str] = set()
        self._used_marker_names: set[str] = set()
        self._tip = RegistryTip(initial_state, None, None)
        self._tip = self._full_recover()

    @classmethod
    def create(
        cls,
        dialogues_directory: SecureDirectory,
        initial_state: RegistryState,
        locks: DomainLockManager,
        authority: RegistryLockAuthority,
        *,
        fault_hook: FaultHook = _no_fault,
    ) -> _RegistryTransactionLog:
        """Create or open ``dialogues/registry`` under a live EX lock."""
        if type(dialogues_directory) is not SecureDirectory:
            raise RegistryStoreError("INVALID_REGISTRY_STORE_DIRECTORY")
        cls._assert_exclusive(locks, authority)
        registry = dialogues_directory.ensure_directory("registry")
        try:
            return cls(
                registry,
                initial_state,
                locks,
                authority,
                fault_hook=fault_hook,
            )
        except BaseException:
            registry.close()
            raise

    def __enter__(self) -> _RegistryTransactionLog:
        self._require_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @staticmethod
    def _assert_exclusive(
        locks: object, authority: object
    ) -> RegistryLockAuthority:
        if (
            type(locks) is not DomainLockManager
            or type(authority) is not RegistryLockAuthority
        ):
            raise RegistryStoreError("LOCK_AUTHORITY_REQUIRED")
        try:
            locks.assert_registry_authority(
                authority,
                RegistryLockMode.EXCLUSIVE,
            )
        except LockError as exc:
            raise RegistryStoreError("LOCK_AUTHORITY_REQUIRED") from exc
        return authority

    def _require_exclusive(
        self, authority: object
    ) -> RegistryLockAuthority:
        return self._assert_exclusive(self._locks, authority)

    def _require_open(self) -> None:
        if self._closed:
            raise RegistryStoreError("REGISTRY_STORE_CLOSED")

    def close(self) -> None:
        """Close all owned registry directory descriptors."""
        if self._closed:
            return
        self._closed = True
        self._events.close()
        self._transactions.close()
        self._quarantine.close()
        self._registry.close()

    def tip(self) -> RegistryTip:
        """Return the immutable in-memory tip without performing filesystem I/O."""
        self._require_open()
        return self._tip

    def recover(self, authority: RegistryLockAuthority) -> RegistryTip:
        """Fully audit marker/event chains and rebuild the disposable cache."""
        self._require_open()
        self._require_exclusive(authority)
        self._tip = self._full_recover()
        return self._tip

    def read_committed(
        self, *, after_sequence: int = 0
    ) -> tuple[CommittedRegistryEvent, ...]:
        """Audit and read committed events after one registry sequence cursor."""
        self._require_open()
        if type(after_sequence) is not int or after_sequence < 0:
            raise RegistryStoreError("INVALID_SEQUENCE")
        self._tip = self._full_recover(maintain=False)
        result: list[CommittedRegistryEvent] = []
        entries = self._marker_entries()
        if {name for _, _, name in entries} != self._used_marker_names:
            raise RegistryStoreError("REGISTRY_RECOVERY_REQUIRED")
        for _, _, name in entries:
            marker = self._read_marker(name)
            for reference in marker.events:
                record = self._read_event(reference)
                self._match_reference(record, reference)
                if reference.registry_sequence > after_sequence:
                    result.append(record.event)
        return tuple(result)

    def commit(
        self,
        request: RegistryCommitRequest,
        authority: RegistryLockAuthority,
    ) -> RegistryCommitOutcome:
        """Durably commit one accepted batch while holding registry EX."""
        self._require_open()
        self._require_exclusive(authority)
        self._validate_request(request)
        self._tip = self._full_recover(maintain=False)

        first_pending = request.accepted.events[0]
        existing = next(
            (
                item
                for item in self._tip.state.receipts
                if item.command_id == first_pending.command_id
            ),
            None,
        )
        if existing is not None:
            if existing.body_digest != first_pending.body_digest:
                raise RegistryStoreError("IDEMPOTENCY_CONFLICT")
            transaction = self._transaction_for_receipt(existing)
            projection_current = self._write_projection_best_effort()
            return RegistryCommitOutcome(
                existing,
                self._tip.state,
                transaction,
                True,
                projection_current,
            )

        if request.expected_registry_sequence != self._tip.state.registry_sequence:
            raise RegistryStoreError("REGISTRY_SEQUENCE_CONFLICT")
        if request.expected_generation != self._tip.state.generation:
            raise RegistryStoreError("REGISTRY_GENERATION_CONFLICT")
        if any(
            item.transaction_id == request.transaction_id
            or any(event_id in item.event_ids for event_id in request.event_ids)
            for item in self._tip.state.receipts
        ):
            raise RegistryStoreError("REGISTRY_INTEGRITY_ERROR")

        try:
            transaction = build_committed_registry_transaction(
                self._tip.state,
                request.accepted,
                transaction_id=request.transaction_id,
                event_ids=request.event_ids,
            )
            state = reduce(self._tip.state, transaction)
        except ValueError as exc:
            raise RegistryStoreError(str(exc)) from exc

        previous_event_hash = self._tip.last_event_hash
        records: list[StoredRegistryEvent] = []
        for event in transaction.events:
            try:
                record = build_stored_registry_event(
                    self._initial_state.registry_id,
                    event,
                    previous_event_hash,
                )
            except ValueError as exc:
                raise RegistryStoreError(str(exc)) from exc
            records.append(record)
            previous_event_hash = record.event_hash

        try:
            marker = build_registry_transaction_marker(
                registry_id=self._initial_state.registry_id,
                transaction=transaction,
                previous_marker_hash=self._tip.last_marker_hash,
                state=state,
                events=tuple(records),
            )
        except ValueError as exc:
            raise RegistryStoreError(str(exc)) from exc

        try:
            encoded_events = tuple(
                encode_stored_registry_event(item) for item in records
            )
            encoded_marker = encode_registry_transaction_marker(marker)
            self._validate_encoded_batch(encoded_events, encoded_marker)
        except ValueError as exc:
            raise RegistryStoreError(str(exc)) from exc

        for record, encoded in zip(records, encoded_events, strict=True):
            self._publish_event(record, encoded)
            self._fault_hook("event_published")
        self._fault_hook("before_marker")
        marker_name = _marker_name(
            marker.from_registry_sequence,
            marker.to_registry_sequence,
            marker.transaction_id,
        )
        self._require_exclusive(authority)
        try:
            self._transactions.write_immutable(
                marker_name,
                encoded_marker,
            )
        except (SecureFsError, ValueError) as exc:
            code = exc.code if type(exc) is SecureFsError else str(exc)
            raise RegistryStoreError(code) from exc
        self._fault_hook("marker_committed")

        self._used_marker_names.add(marker_name)
        self._used_event_names.update(
            _event_name(item.event.registry_sequence, item.event.event_id)
            for item in records
        )
        self._tip = RegistryTip(
            state,
            previous_event_hash,
            marker.marker_hash,
        )
        projection_current = self._write_projection_best_effort()
        receipt = state.receipts[-1]
        return RegistryCommitOutcome(
            receipt,
            state,
            transaction,
            False,
            projection_current,
        )

    def _validate_request(self, request: object) -> None:
        if (
            type(request) is not RegistryCommitRequest
            or not _valid_id(request.transaction_id)
            or type(request.event_ids) is not tuple
            or not request.event_ids
            or len(request.event_ids) > 2
            or any(not _valid_id(item) for item in request.event_ids)
            or len(set(request.event_ids)) != len(request.event_ids)
            or type(request.expected_registry_sequence) is not int
            or request.expected_registry_sequence < 0
            or type(request.expected_generation) is not int
            or request.expected_generation < 0
            or type(request.accepted) is not RegistryAccepted
            or type(request.accepted.events) is not tuple
            or len(request.accepted.events) != len(request.event_ids)
            or any(
                type(item) is not PendingRegistryEvent
                for item in request.accepted.events
            )
            or not _is_valid_batch_shape(request.accepted)
        ):
            raise RegistryStoreError("INVALID_REGISTRY_COMMIT_REQUEST")
        first = request.accepted.events[0]
        if (
            not _valid_id(first.command_id)
            or not _valid_hash(first.body_digest)
            or any(
                item.command_id != first.command_id
                or item.body_digest != first.body_digest
                for item in request.accepted.events
            )
        ):
            raise RegistryStoreError("INVALID_REGISTRY_COMMIT_REQUEST")

    @staticmethod
    def _validate_encoded_batch(
        events: tuple[bytes, ...], marker: bytes
    ) -> None:
        records = (*events, marker)
        if any(
            type(item) is not bytes or not item or len(item) > MAX_RECORD_BYTES
            for item in records
        ):
            raise ValueError("REGISTRY_RECORD_SIZE_INVALID")

    def _publish_event(self, record: StoredRegistryEvent, encoded: bytes) -> None:
        name = _event_name(
            record.event.registry_sequence,
            record.event.event_id,
        )
        try:
            self._events.write_immutable(name, encoded)
            return
        except SecureFsError as exc:
            if exc.code != "IMMUTABLE_EXISTS":
                raise RegistryStoreError(exc.code) from exc
        try:
            existing = self._events.read_bytes(name, max_bytes=MAX_RECORD_BYTES)
        except SecureFsError as exc:
            raise RegistryStoreError(exc.code) from exc
        if existing == encoded:
            return
        quarantine_name = (
            f"orphan-{record.event.registry_sequence:020d}-"
            f"{hashlib.sha256(existing).hexdigest()}.json"
        )
        try:
            self._events.quarantine(
                name,
                destination=self._quarantine,
                destination_component=quarantine_name,
            )
            self._events.write_immutable(name, encoded)
        except SecureFsError as exc:
            raise RegistryStoreError(exc.code) from exc

    def _marker_entries(self) -> tuple[tuple[int, int, str], ...]:
        try:
            names = self._transactions.list_entries()
        except SecureFsError as exc:
            raise RegistryStoreError(exc.code) from exc
        entries: list[tuple[int, int, str]] = []
        for name in names:
            match = _MARKER_PATTERN.fullmatch(name)
            if match is None:
                raise RegistryStoreError("REGISTRY_TRANSACTION_SEQUENCE_GAP")
            first = int(match.group("first"))
            last = int(match.group("last"))
            if first < 1 or last < first:
                raise RegistryStoreError("REGISTRY_TRANSACTION_SEQUENCE_GAP")
            entries.append((first, last, name))
        entries.sort()
        return tuple(entries)

    def _read_marker(self, name: str) -> RegistryTransactionMarker:
        try:
            raw = self._transactions.read_bytes(
                name,
                max_bytes=MAX_RECORD_BYTES,
            )
        except SecureFsError as exc:
            raise RegistryStoreError(exc.code) from exc
        try:
            marker = decode_registry_transaction_marker(raw)
        except ValueError as exc:
            raise RegistryStoreError(str(exc)) from exc
        expected = _marker_name(
            marker.from_registry_sequence,
            marker.to_registry_sequence,
            marker.transaction_id,
        )
        if expected != name:
            raise RegistryStoreError("REGISTRY_TRANSACTION_FILENAME_MISMATCH")
        return marker

    def _read_event(
        self, reference: RegistryMarkerEventRef
    ) -> StoredRegistryEvent:
        name = _event_name(reference.registry_sequence, reference.event_id)
        try:
            raw = self._events.read_bytes(name, max_bytes=MAX_RECORD_BYTES)
        except SecureFsError as exc:
            raise RegistryStoreError(exc.code) from exc
        try:
            record = decode_stored_registry_event(raw)
        except ValueError as exc:
            raise RegistryStoreError(str(exc)) from exc
        return record

    @staticmethod
    def _match_reference(
        record: StoredRegistryEvent,
        reference: RegistryMarkerEventRef,
    ) -> None:
        if (
            type(reference) is not RegistryMarkerEventRef
            or record.event.registry_sequence != reference.registry_sequence
            or record.event.event_id != reference.event_id
            or record.event.event_digest != reference.event_digest
            or record.event_hash != reference.event_hash
        ):
            raise RegistryStoreError("REGISTRY_TRANSACTION_EVENT_MISMATCH")

    def _apply_marker(
        self,
        tip: RegistryTip,
        marker: RegistryTransactionMarker,
    ) -> RegistryTip:
        if (
            marker.registry_id != self._initial_state.registry_id
            or marker.from_registry_sequence != tip.state.registry_sequence + 1
            or marker.previous_marker_hash != tip.last_marker_hash
        ):
            raise RegistryStoreError("REGISTRY_TRANSACTION_CHAIN_MISMATCH")
        previous_event_hash = tip.last_event_hash
        records: list[StoredRegistryEvent] = []
        for reference in marker.events:
            record = self._read_event(reference)
            self._match_reference(record, reference)
            if (
                record.registry_id != self._initial_state.registry_id
                or record.previous_event_hash != previous_event_hash
            ):
                raise RegistryStoreError("REGISTRY_EVENT_CHAIN_MISMATCH")
            records.append(record)
            previous_event_hash = record.event_hash

        transaction = CommittedRegistryTransaction(
            marker.transaction_id,
            marker.transaction_digest,
            tuple(item.event for item in records),
        )
        try:
            validate_committed_registry_transaction(transaction)
        except ValueError as exc:
            raise RegistryStoreError(str(exc)) from exc
        first = transaction.events[0]
        if (
            first.command_id != marker.command_id
            or first.body_digest != marker.body_digest
            or transaction.events[-1].registry_sequence
            != marker.to_registry_sequence
        ):
            raise RegistryStoreError("REGISTRY_TRANSACTION_EVENT_MISMATCH")
        try:
            state = reduce(tip.state, transaction)
        except ValueError as exc:
            raise RegistryStoreError(str(exc)) from exc
        if (
            state.generation != marker.generation
            or registry_state_digest(state) != marker.state_digest
        ):
            raise RegistryStoreError("REGISTRY_TRANSACTION_STATE_MISMATCH")
        return RegistryTip(state, previous_event_hash, marker.marker_hash)

    def _transaction_for_receipt(
        self, receipt: CommandReceipt
    ) -> CommittedRegistryTransaction:
        name = _marker_name(
            receipt.first_registry_sequence,
            receipt.last_registry_sequence,
            receipt.transaction_id,
        )
        marker = self._read_marker(name)
        records = tuple(self._read_event(item) for item in marker.events)
        for record, reference in zip(records, marker.events, strict=True):
            self._match_reference(record, reference)
        transaction = CommittedRegistryTransaction(
            marker.transaction_id,
            marker.transaction_digest,
            tuple(item.event for item in records),
        )
        try:
            validate_committed_registry_transaction(transaction)
        except ValueError as exc:
            raise RegistryStoreError(str(exc)) from exc
        if (
            receipt.transaction_digest != transaction.transaction_digest
            or receipt.event_ids
            != tuple(item.event_id for item in transaction.events)
            or receipt.event_digests
            != tuple(item.event_digest for item in transaction.events)
        ):
            raise RegistryStoreError("REGISTRY_RECEIPT_MISMATCH")
        return transaction

    def _synchronize(self) -> None:
        self._recover_temporary_writes()
        entries = self._marker_entries()
        present = {name for _, _, name in entries}
        if not self._used_marker_names.issubset(present):
            raise RegistryStoreError("REGISTRY_TRANSACTION_CHAIN_MISMATCH")
        for first, _, name in entries:
            if name in self._used_marker_names:
                continue
            if first != self._tip.state.registry_sequence + 1:
                raise RegistryStoreError("REGISTRY_TRANSACTION_SEQUENCE_GAP")
            marker = self._read_marker(name)
            self._tip = self._apply_marker(self._tip, marker)
            self._used_marker_names.add(name)
            self._used_event_names.update(
                _event_name(item.registry_sequence, item.event_id)
                for item in marker.events
            )
        self._quarantine_orphan_events(self._used_event_names)

    def _full_recover(self, *, maintain: bool = True) -> RegistryTip:
        if maintain:
            self._recover_temporary_writes()
        tip = RegistryTip(self._initial_state, None, None)
        used_events: set[str] = set()
        used_markers: set[str] = set()
        for first, _, name in self._marker_entries():
            if first != tip.state.registry_sequence + 1:
                raise RegistryStoreError("REGISTRY_TRANSACTION_SEQUENCE_GAP")
            marker = self._read_marker(name)
            tip = self._apply_marker(tip, marker)
            used_markers.add(name)
            used_events.update(
                _event_name(item.registry_sequence, item.event_id)
                for item in marker.events
            )
        if maintain:
            self._quarantine_orphan_events(used_events)
        self._used_event_names = used_events
        self._used_marker_names = used_markers
        self._tip = tip
        if maintain:
            self._write_projection_best_effort()
        return tip

    def _recover_temporary_writes(self) -> None:
        try:
            for directory in (
                self._registry,
                self._events,
                self._transactions,
                self._quarantine,
            ):
                directory.recover_temporary_writes()
            self._events.recover_quarantine_moves(self._quarantine)
        except SecureFsError as exc:
            raise RegistryStoreError(exc.code) from exc

    def _quarantine_orphan_events(self, used: set[str]) -> None:
        try:
            for name in self._events.list_entries():
                if name in used:
                    continue
                data = self._events.read_bytes(name, max_bytes=MAX_RECORD_BYTES)
                destination = (
                    f"orphan-{hashlib.sha256(name.encode()).hexdigest()[:16]}-"
                    f"{hashlib.sha256(data).hexdigest()}.json"
                )
                self._events.quarantine(
                    name,
                    destination=self._quarantine,
                    destination_component=destination,
                )
        except SecureFsError as exc:
            raise RegistryStoreError(exc.code) from exc

    def _write_projection_best_effort(self) -> bool:
        try:
            snapshot = build_registry_state_snapshot(
                self._tip.state,
                last_event_hash=self._tip.last_event_hash,
                last_marker_hash=self._tip.last_marker_hash,
            )
            self._registry.replace_derived(
                "state.json",
                encode_registry_state_snapshot(snapshot),
            )
            self._fault_hook("state_projected")
            return True
        except (SecureFsError, ValueError):
            return False
