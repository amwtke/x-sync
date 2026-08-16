"""Canonical schema-v2 records for the durable X-Sync registry stream."""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass, replace
from enum import StrEnum
import hashlib
from itertools import pairwise
import json
import re
from types import UnionType
from typing import TypeAlias, Union, cast, get_args, get_origin, get_type_hints

from .registry import (
    ActivateTarget,
    Activated,
    CommandReceipt,
    CommittedRegistryEvent,
    CommittedRegistryTransaction,
    Deactivated,
    DeactivationStarted,
    DialogueCreated,
    PendingHandoff,
    PendingRegistryEvent,
    QuiesceProof,
    RegisteredDialogue,
    RegistryAccepted,
    RegistryState,
    validate_transaction,
)


SCHEMA_VERSION = 2
PROTOCOL_VERSION = "x-sync-dialogue/2"
MAX_RECORD_BYTES = 4 * 1024 * 1024


def canonical_json_bytes(value: object) -> bytes:
    """Encode a JSON-compatible tree in the one accepted canonical form."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError("INVALID_CANONICAL_VALUE") from exc


def sha256_digest(payload: bytes) -> str:
    """Return a lowercase, prefixed SHA-256 digest for exact bytes."""
    if type(payload) is not bytes:
        raise TypeError("payload must be exact bytes")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


@dataclass(frozen=True, slots=True)
class StoredRegistryEvent:
    """One immutable, hash-chained registry event file."""

    schema_version: int
    record_type: str
    protocol_version: str
    registry_id: str
    event_type: str
    previous_event_hash: str | None
    event: CommittedRegistryEvent
    event_hash: str


@dataclass(frozen=True, slots=True)
class RegistryMarkerEventRef:
    """A registry marker reference to one immutable event file."""

    registry_sequence: int
    event_id: str
    event_digest: str
    event_hash: str


@dataclass(frozen=True, slots=True)
class RegistryTransactionMarker:
    """The sole durable linearization record for a registry transaction."""

    schema_version: int
    record_type: str
    protocol_version: str
    registry_id: str
    transaction_id: str
    transaction_digest: str
    command_id: str
    body_digest: str
    generation: int
    from_registry_sequence: int
    to_registry_sequence: int
    previous_marker_hash: str | None
    state_digest: str
    events: tuple[RegistryMarkerEventRef, ...]
    marker_hash: str


@dataclass(frozen=True, slots=True)
class RegistryStateSnapshot:
    """A disposable self-verifying projection of the registry aggregate."""

    schema_version: int
    record_type: str
    protocol_version: str
    registry_id: str
    through_registry_sequence: int
    state_digest: str
    last_event_hash: str | None
    last_marker_hash: str | None
    state: RegistryState


PersistedRegistryRecord: TypeAlias = (
    StoredRegistryEvent | RegistryTransactionMarker | RegistryStateSnapshot
)


_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_HASH_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
_EVENT_TYPE_BY_PAYLOAD = {
    DialogueCreated: "dialogue_created",
    DeactivationStarted: "dialogue_deactivation_started",
    Deactivated: "dialogue_deactivated",
    Activated: "dialogue_activated",
}


_DOMAIN_TYPES = frozenset(
    {
        ActivateTarget,
        Activated,
        CommandReceipt,
        CommittedRegistryEvent,
        CommittedRegistryTransaction,
        Deactivated,
        DeactivationStarted,
        DialogueCreated,
        PendingHandoff,
        QuiesceProof,
        RegisteredDialogue,
        RegistryState,
        StoredRegistryEvent,
        RegistryMarkerEventRef,
        RegistryTransactionMarker,
        RegistryStateSnapshot,
    }
)


def _valid_id(value: object) -> bool:
    return type(value) is str and _ID_PATTERN.fullmatch(value) is not None


def _valid_hash(value: object) -> bool:
    return type(value) is str and _HASH_PATTERN.fullmatch(value) is not None


def _valid_optional_hash(value: object) -> bool:
    return value is None or _valid_hash(value)


def _valid_positive_int(value: object) -> bool:
    return type(value) is int and value >= 1


def _valid_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _to_tree(value: object) -> object:
    value_type = type(value)
    if value is None or value_type in {str, int, bool}:
        return value
    if isinstance(value, StrEnum):
        return value.value
    if value_type is tuple:
        return [_to_tree(item) for item in cast(tuple[object, ...], value)]
    if value_type in _DOMAIN_TYPES and is_dataclass(value):
        return {
            "$type": value_type.__name__,
            **{
                field.name: _to_tree(getattr(value, field.name))
                for field in fields(value)
            },
        }
    raise ValueError("UNSUPPORTED_REGISTRY_CODEC_VALUE")


def _object_without_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _parse_canonical(raw: bytes) -> object:
    if type(raw) is not bytes or not raw or len(raw) > MAX_RECORD_BYTES:
        raise ValueError("INVALID_RECORD_SIZE")
    try:
        text = raw.decode("utf-8", errors="strict")
        value = json.loads(
            text,
            object_pairs_hook=_object_without_duplicate_keys,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("INVALID_JSON_NUMBER")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("INVALID_JSON_RECORD") from exc
    if canonical_json_bytes(value) != raw:
        raise ValueError("NON_CANONICAL_RECORD")
    return value


def _decode_union(value: object, variants: tuple[object, ...]) -> object:
    if value is None and type(None) in variants:
        return None
    if type(value) is dict:
        tag = value.get("$type")
        matching = tuple(
            variant
            for variant in variants
            if isinstance(variant, type)
            and is_dataclass(variant)
            and variant.__name__ == tag
        )
        if len(matching) == 1:
            return _from_tree(value, matching[0])
    successes: list[object] = []
    for variant in variants:
        if variant is type(None):
            continue
        try:
            successes.append(_from_tree(value, variant))
        except ValueError:
            continue
    if len(successes) != 1:
        raise ValueError("INVALID_UNION_VALUE")
    return successes[0]


def _from_tree(value: object, expected: object) -> object:
    origin = get_origin(expected)
    if origin in {UnionType, Union}:
        return _decode_union(value, get_args(expected))
    if origin is tuple:
        if type(value) is not list:
            raise ValueError("INVALID_TUPLE_VALUE")
        arguments = get_args(expected)
        if len(arguments) == 2 and arguments[1] is Ellipsis:
            return tuple(_from_tree(item, arguments[0]) for item in value)
        if len(arguments) != len(value):
            raise ValueError("INVALID_TUPLE_ARITY")
        return tuple(
            _from_tree(item, argument)
            for item, argument in zip(value, arguments, strict=True)
        )
    if expected is type(None):
        if value is not None:
            raise ValueError("INVALID_NULL_VALUE")
        return None
    if expected in {str, int, bool}:
        if type(value) is not expected:
            raise ValueError("INVALID_SCALAR_TYPE")
        return value
    if isinstance(expected, type) and issubclass(expected, StrEnum):
        if type(value) is not str:
            raise ValueError("INVALID_ENUM_VALUE")
        try:
            return expected(value)
        except ValueError as exc:
            raise ValueError("INVALID_ENUM_VALUE") from exc
    if isinstance(expected, type) and expected in _DOMAIN_TYPES:
        if type(value) is not dict or value.get("$type") != expected.__name__:
            raise ValueError("INVALID_RECORD_TYPE")
        hints = get_type_hints(expected)
        expected_keys = {"$type", *(field.name for field in fields(expected))}
        if set(value) != expected_keys:
            raise ValueError("INVALID_RECORD_KEYS")
        decoded = {
            field.name: _from_tree(value[field.name], hints[field.name])
            for field in fields(expected)
        }
        try:
            return expected(**decoded)
        except (TypeError, ValueError) as exc:
            raise ValueError("INVALID_RECORD_VALUE") from exc
    raise ValueError("UNSUPPORTED_REGISTRY_CODEC_TYPE")


def _encode_typed(value: object) -> bytes:
    return canonical_json_bytes(_to_tree(value))


def _decode_typed(raw: bytes, expected: object) -> object:
    return _from_tree(_parse_canonical(raw), expected)


def _is_exact_typed(value: object, expected: object) -> bool:
    try:
        return _from_tree(_to_tree(value), expected) == value
    except (AttributeError, TypeError, ValueError):
        return False


def registry_state_digest(state: RegistryState) -> str:
    """Hash the exact canonical registry aggregate, including receipts."""
    if type(state) is not RegistryState or not _is_exact_typed(
        state, RegistryState
    ):
        raise ValueError("INVALID_REGISTRY_STATE")
    return sha256_digest(_encode_typed(state))


def _event_without_digest(
    event: CommittedRegistryEvent,
) -> CommittedRegistryEvent:
    return replace(event, event_digest="")


def _event_digest(event: CommittedRegistryEvent) -> str:
    return sha256_digest(_encode_typed(_event_without_digest(event)))


def _validate_committed_event(event: object) -> CommittedRegistryEvent:
    if (
        type(event) is not CommittedRegistryEvent
        or not _is_exact_typed(event, CommittedRegistryEvent)
        or not _valid_id(event.event_id)
        or not _valid_hash(event.event_digest)
        or not _valid_positive_int(event.registry_sequence)
        or not _valid_nonnegative_int(event.generation)
        or not _valid_id(event.command_id)
        or not _valid_hash(event.body_digest)
        or type(event.payload) not in _EVENT_TYPE_BY_PAYLOAD
    ):
        raise ValueError("INVALID_COMMITTED_REGISTRY_EVENT")
    if _event_digest(event) != event.event_digest:
        raise ValueError("REGISTRY_EVENT_DIGEST_MISMATCH")
    return event


def _transaction_without_digest(
    transaction: CommittedRegistryTransaction,
) -> CommittedRegistryTransaction:
    return replace(transaction, transaction_digest="")


def _transaction_digest(
    transaction: CommittedRegistryTransaction,
) -> str:
    return sha256_digest(
        _encode_typed(_transaction_without_digest(transaction))
    )


def validate_committed_registry_transaction(
    transaction: CommittedRegistryTransaction,
) -> None:
    """Verify intrinsic event and transaction identities without state I/O."""
    if (
        type(transaction) is not CommittedRegistryTransaction
        or not _is_exact_typed(transaction, CommittedRegistryTransaction)
        or not _valid_id(transaction.transaction_id)
        or not _valid_hash(transaction.transaction_digest)
        or type(transaction.events) is not tuple
        or not 1 <= len(transaction.events) <= 2
    ):
        raise ValueError("INVALID_COMMITTED_REGISTRY_TRANSACTION")
    events = tuple(_validate_committed_event(item) for item in transaction.events)
    first = events[0]
    if (
        tuple(item.registry_sequence for item in events)
        != tuple(
            range(
                first.registry_sequence,
                first.registry_sequence + len(events),
            )
        )
        or len({item.event_id for item in events}) != len(events)
        or any(
            item.command_id != first.command_id
            or item.body_digest != first.body_digest
            for item in events
        )
    ):
        raise ValueError("INVALID_COMMITTED_REGISTRY_TRANSACTION")
    if _transaction_digest(transaction) != transaction.transaction_digest:
        raise ValueError("REGISTRY_TRANSACTION_DIGEST_MISMATCH")


def _payload_generation(state: RegistryState, payload: object) -> int:
    if type(payload) is DialogueCreated:
        return state.generation
    if type(payload) is DeactivationStarted:
        return payload.handoff.generation
    if type(payload) is Deactivated:
        return payload.proof.generation
    if type(payload) is Activated:
        return payload.generation
    raise ValueError("INVALID_REGISTRY_ACCEPTED_BATCH")


def build_committed_registry_transaction(
    state: RegistryState,
    accepted: RegistryAccepted,
    *,
    transaction_id: str,
    event_ids: tuple[str, ...],
) -> CommittedRegistryTransaction:
    """Assign sequence and canonical digests to one accepted registry batch."""
    if (
        type(state) is not RegistryState
        or type(accepted) is not RegistryAccepted
        or type(accepted.events) is not tuple
        or not accepted.events
        or len(accepted.events) > 2
        or any(type(item) is not PendingRegistryEvent for item in accepted.events)
        or not _valid_id(transaction_id)
        or type(event_ids) is not tuple
        or len(event_ids) != len(accepted.events)
        or any(not _valid_id(item) for item in event_ids)
        or len(set(event_ids)) != len(event_ids)
    ):
        raise ValueError("INVALID_REGISTRY_ACCEPTED_BATCH")
    first = accepted.events[0]
    if (
        not _valid_id(first.command_id)
        or not _valid_hash(first.body_digest)
        or any(
            item.command_id != first.command_id
            or item.body_digest != first.body_digest
            for item in accepted.events
        )
    ):
        raise ValueError("INVALID_REGISTRY_ACCEPTED_BATCH")

    committed: list[CommittedRegistryEvent] = []
    for index, (event_id, pending) in enumerate(
        zip(event_ids, accepted.events, strict=True), start=1
    ):
        draft = CommittedRegistryEvent(
            event_id=event_id,
            event_digest="",
            registry_sequence=state.registry_sequence + index,
            generation=_payload_generation(state, pending.payload),
            command_id=pending.command_id,
            body_digest=pending.body_digest,
            payload=pending.payload,
        )
        committed.append(replace(draft, event_digest=_event_digest(draft)))
    draft_transaction = CommittedRegistryTransaction(
        transaction_id=transaction_id,
        transaction_digest="",
        events=tuple(committed),
    )
    transaction = replace(
        draft_transaction,
        transaction_digest=_transaction_digest(draft_transaction),
    )
    validate_committed_registry_transaction(transaction)
    validate_transaction(state, transaction)
    return transaction


def build_stored_registry_event(
    registry_id: str,
    event: CommittedRegistryEvent,
    previous_event_hash: str | None,
) -> StoredRegistryEvent:
    """Build and hash one immutable registry event record."""
    _validate_committed_event(event)
    if not _valid_id(registry_id) or not _valid_optional_hash(
        previous_event_hash
    ):
        raise ValueError("INVALID_STORED_REGISTRY_EVENT")
    record = StoredRegistryEvent(
        schema_version=SCHEMA_VERSION,
        record_type="registry_event",
        protocol_version=PROTOCOL_VERSION,
        registry_id=registry_id,
        event_type=_EVENT_TYPE_BY_PAYLOAD[type(event.payload)],
        previous_event_hash=previous_event_hash,
        event=event,
        event_hash="",
    )
    result = replace(record, event_hash=sha256_digest(_encode_typed(record)))
    _validate_stored_registry_event(result)
    return result


def _validate_stored_registry_event(value: object) -> StoredRegistryEvent:
    if (
        type(value) is not StoredRegistryEvent
        or not _is_exact_typed(value, StoredRegistryEvent)
        or value.schema_version != SCHEMA_VERSION
        or value.record_type != "registry_event"
        or value.protocol_version != PROTOCOL_VERSION
        or not _valid_id(value.registry_id)
        or type(value.event) is not CommittedRegistryEvent
        or value.event_type
        != _EVENT_TYPE_BY_PAYLOAD.get(type(value.event.payload))
        or not _valid_optional_hash(value.previous_event_hash)
        or not _valid_hash(value.event_hash)
    ):
        raise ValueError("INVALID_STORED_REGISTRY_EVENT")
    _validate_committed_event(value.event)
    unhashed = replace(value, event_hash="")
    if sha256_digest(_encode_typed(unhashed)) != value.event_hash:
        raise ValueError("STORED_REGISTRY_EVENT_HASH_MISMATCH")
    return value


def encode_stored_registry_event(record: StoredRegistryEvent) -> bytes:
    """Encode one valid immutable registry event."""
    return _encode_typed(_validate_stored_registry_event(record))


def decode_stored_registry_event(raw: bytes) -> StoredRegistryEvent:
    """Decode one canonical registry event and verify both hashes."""
    record = _decode_typed(raw, StoredRegistryEvent)
    return _validate_stored_registry_event(record)


def _marker_ref(record: StoredRegistryEvent) -> RegistryMarkerEventRef:
    record = _validate_stored_registry_event(record)
    return RegistryMarkerEventRef(
        registry_sequence=record.event.registry_sequence,
        event_id=record.event.event_id,
        event_digest=record.event.event_digest,
        event_hash=record.event_hash,
    )


def build_registry_transaction_marker(
    *,
    registry_id: str,
    transaction: CommittedRegistryTransaction,
    previous_marker_hash: str | None,
    state: RegistryState,
    events: tuple[StoredRegistryEvent, ...],
) -> RegistryTransactionMarker:
    """Build a marker binding a complete event batch and resulting state."""
    validate_committed_registry_transaction(transaction)
    if (
        not _valid_id(registry_id)
        or type(state) is not RegistryState
        or state.registry_id != registry_id
        or type(events) is not tuple
        or len(events) != len(transaction.events)
        or not events
        or not _valid_optional_hash(previous_marker_hash)
    ):
        raise ValueError("INVALID_REGISTRY_TRANSACTION_MARKER")
    validated_events = tuple(
        _validate_stored_registry_event(item) for item in events
    )
    if (
        tuple(item.event for item in validated_events) != transaction.events
        or any(item.registry_id != registry_id for item in validated_events)
        or any(
            current.previous_event_hash != previous.event_hash
            for previous, current in pairwise(validated_events)
        )
        or state.registry_sequence
        != transaction.events[-1].registry_sequence
    ):
        raise ValueError("INVALID_REGISTRY_TRANSACTION_MARKER")
    first = transaction.events[0]
    marker = RegistryTransactionMarker(
        schema_version=SCHEMA_VERSION,
        record_type="registry_transaction_marker",
        protocol_version=PROTOCOL_VERSION,
        registry_id=registry_id,
        transaction_id=transaction.transaction_id,
        transaction_digest=transaction.transaction_digest,
        command_id=first.command_id,
        body_digest=first.body_digest,
        generation=state.generation,
        from_registry_sequence=first.registry_sequence,
        to_registry_sequence=transaction.events[-1].registry_sequence,
        previous_marker_hash=previous_marker_hash,
        state_digest=registry_state_digest(state),
        events=tuple(_marker_ref(item) for item in validated_events),
        marker_hash="",
    )
    result = replace(marker, marker_hash=sha256_digest(_encode_typed(marker)))
    _validate_registry_transaction_marker(result)
    return result


def _validate_registry_transaction_marker(
    value: object,
) -> RegistryTransactionMarker:
    if (
        type(value) is not RegistryTransactionMarker
        or not _is_exact_typed(value, RegistryTransactionMarker)
        or value.schema_version != SCHEMA_VERSION
        or value.record_type != "registry_transaction_marker"
        or value.protocol_version != PROTOCOL_VERSION
        or not _valid_id(value.registry_id)
        or not _valid_id(value.transaction_id)
        or not _valid_hash(value.transaction_digest)
        or not _valid_id(value.command_id)
        or not _valid_hash(value.body_digest)
        or not _valid_positive_int(value.generation)
        or not _valid_positive_int(value.from_registry_sequence)
        or not _valid_positive_int(value.to_registry_sequence)
        or not _valid_optional_hash(value.previous_marker_hash)
        or not _valid_hash(value.state_digest)
        or type(value.events) is not tuple
        or not 1 <= len(value.events) <= 2
        or any(type(item) is not RegistryMarkerEventRef for item in value.events)
        or not _valid_hash(value.marker_hash)
    ):
        raise ValueError("INVALID_REGISTRY_TRANSACTION_MARKER")
    sequences = tuple(item.registry_sequence for item in value.events)
    if (
        any(not _valid_positive_int(item.registry_sequence) for item in value.events)
        or any(not _valid_id(item.event_id) for item in value.events)
        or any(not _valid_hash(item.event_digest) for item in value.events)
        or any(not _valid_hash(item.event_hash) for item in value.events)
        or len({item.event_id for item in value.events}) != len(value.events)
        or sequences
        != tuple(range(sequences[0], sequences[0] + len(sequences)))
        or value.from_registry_sequence != sequences[0]
        or value.to_registry_sequence != sequences[-1]
    ):
        raise ValueError("INVALID_REGISTRY_TRANSACTION_MARKER")
    unhashed = replace(value, marker_hash="")
    if sha256_digest(_encode_typed(unhashed)) != value.marker_hash:
        raise ValueError("REGISTRY_TRANSACTION_MARKER_HASH_MISMATCH")
    return value


def encode_registry_transaction_marker(
    marker: RegistryTransactionMarker,
) -> bytes:
    """Encode a valid registry transaction marker."""
    return _encode_typed(_validate_registry_transaction_marker(marker))


def decode_registry_transaction_marker(
    raw: bytes,
) -> RegistryTransactionMarker:
    """Decode one canonical marker and verify its self hash."""
    marker = _decode_typed(raw, RegistryTransactionMarker)
    return _validate_registry_transaction_marker(marker)


def build_registry_state_snapshot(
    state: RegistryState,
    *,
    last_event_hash: str | None,
    last_marker_hash: str | None,
) -> RegistryStateSnapshot:
    """Build the disposable registry state projection cache."""
    snapshot = RegistryStateSnapshot(
        schema_version=SCHEMA_VERSION,
        record_type="registry_state_snapshot",
        protocol_version=PROTOCOL_VERSION,
        registry_id=state.registry_id,
        through_registry_sequence=state.registry_sequence,
        state_digest=registry_state_digest(state),
        last_event_hash=last_event_hash,
        last_marker_hash=last_marker_hash,
        state=state,
    )
    return _validate_registry_state_snapshot(snapshot)


def _validate_registry_state_snapshot(
    value: object,
) -> RegistryStateSnapshot:
    if (
        type(value) is not RegistryStateSnapshot
        or not _is_exact_typed(value, RegistryStateSnapshot)
        or value.schema_version != SCHEMA_VERSION
        or value.record_type != "registry_state_snapshot"
        or value.protocol_version != PROTOCOL_VERSION
        or not _valid_id(value.registry_id)
        or not _valid_nonnegative_int(value.through_registry_sequence)
        or not _valid_hash(value.state_digest)
        or not _valid_optional_hash(value.last_event_hash)
        or not _valid_optional_hash(value.last_marker_hash)
        or type(value.state) is not RegistryState
        or value.state.registry_id != value.registry_id
        or value.state.registry_sequence != value.through_registry_sequence
        or registry_state_digest(value.state) != value.state_digest
    ):
        raise ValueError("INVALID_REGISTRY_STATE_SNAPSHOT")
    if value.through_registry_sequence == 0:
        if value.last_event_hash is not None or value.last_marker_hash is not None:
            raise ValueError("INVALID_REGISTRY_STATE_SNAPSHOT")
    elif not _valid_hash(value.last_event_hash) or not _valid_hash(
        value.last_marker_hash
    ):
        raise ValueError("INVALID_REGISTRY_STATE_SNAPSHOT")
    return value


def encode_registry_state_snapshot(snapshot: RegistryStateSnapshot) -> bytes:
    """Encode a valid disposable registry projection."""
    return _encode_typed(_validate_registry_state_snapshot(snapshot))


def decode_registry_state_snapshot(raw: bytes) -> RegistryStateSnapshot:
    """Decode and verify a registry projection without making it authoritative."""
    snapshot = _decode_typed(raw, RegistryStateSnapshot)
    return _validate_registry_state_snapshot(snapshot)
