"""Strict canonical codecs for durable X-Sync v2 dialogue records.

The codec deliberately has no filesystem responsibilities.  It turns the
closed dialogue domain into canonical UTF-8 JSON records and validates every
byte on the way back in.  Transaction markers, rather than event filenames,
define which events are committed.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import StrEnum
import hashlib
from itertools import pairwise
import json
from types import UnionType
from typing import TypeAlias, Union, cast, get_args, get_origin, get_type_hints

from .domain import (
    AgentTurnCommitted,
    AgentTurnResult,
    CandidatesPresented,
    CommitAgentTurn,
    CommittedDialogueEvent,
    CurrentWorkState,
    DecisionContext,
    DialogueCommand,
    DialogueState,
    EvidenceCheck,
    GateAssessment,
    GateRequirement,
    LearnerModelEntry,
    LearnerTurnSubmitted,
    PauseTopic,
    PresentCandidates,
    RecoverWork,
    ReportWorkFailure,
    ResumeTopic,
    SelectTopic,
    SessionDeactivationPrepared,
    SessionStarted,
    StartSession,
    StartTopic,
    SubmitLearnerTurn,
    SwitchTopic,
    TaskScope,
    TopicContract,
    TopicPaused,
    TopicResumed,
    TopicRunState,
    TopicSelectionSubmitted,
    TopicSwitchRequested,
    TopicStarted,
    TriggerBinding,
    WorkDeadLettered,
    WorkFailed,
    WorkFailure,
    WorkRecoveryRequested,
    WorkRequeued,
)
from .state_machine import conversation_version_delta, validate_state
from .work_identity import (
    is_canonical_trigger_binding,
    is_protocol_id,
    is_sha256_digest,
)


SCHEMA_VERSION = 2
PROTOCOL_VERSION = "x-sync-dialogue/2"
MAX_RECORD_BYTES = 4 * 1024 * 1024
MAX_JSON_INTEGER_DIGITS = 256


class ActorKind(StrEnum):
    """Trusted origin category persisted with a dialogue event."""

    LEARNER = "learner"
    HOST = "host"
    RUNTIME = "runtime"


@dataclass(frozen=True, slots=True)
class DialogueActor:
    """The bounded actor identity written into one event envelope."""

    kind: ActorKind
    actor_id: str


@dataclass(frozen=True, slots=True)
class StoredDialogueEvent:
    """One immutable event envelope stored before its commit marker."""

    schema_version: int
    record_type: str
    protocol_version: str
    session_id: str
    registry_generation: int
    event_type: str
    occurred_at: str
    actor: DialogueActor
    causation_id: str | None
    previous_event_hash: str | None
    event: CommittedDialogueEvent
    event_hash: str


@dataclass(frozen=True, slots=True)
class MarkerEventRef:
    """A marker's immutable reference to one stored event."""

    sequence: int
    event_id: str
    event_hash: str


@dataclass(frozen=True, slots=True)
class TransactionMarker:
    """The sole linearization record for one dialogue transaction."""

    schema_version: int
    record_type: str
    session_id: str
    transaction_id: str
    command_id: str
    request_digest: str
    registry_generation: int
    from_sequence: int
    to_sequence: int
    previous_marker_hash: str | None
    state_digest: str
    effect_intents: tuple[()]
    events: tuple[MarkerEventRef, ...]
    marker_hash: str


@dataclass(frozen=True, slots=True)
class DialogueCommandReceipt:
    """The durable idempotency result derived from a committed marker."""

    command_id: str
    request_digest: str
    transaction_id: str
    marker_hash: str
    from_sequence: int
    to_sequence: int
    event_ids: tuple[str, ...]
    state_digest: str


@dataclass(frozen=True, slots=True)
class DialogueWriteRequestRecord:
    """Canonical semantic request identity used for durable idempotency."""

    schema_version: int
    record_type: str
    session_id: str
    expected_registry_generation: int
    expected_conversation_version: int
    command: DialogueCommand
    context: DecisionContext


@dataclass(frozen=True, slots=True)
class DialogueStateSnapshot:
    """A disposable state cache that can be rebuilt from committed markers."""

    schema_version: int
    record_type: str
    session_id: str
    through_sequence: int
    state_digest: str
    last_event_hash: str | None
    last_marker_hash: str | None
    receipts: tuple[DialogueCommandReceipt, ...]
    state: DialogueState


PersistedRecord: TypeAlias = (
    StoredDialogueEvent | TransactionMarker | DialogueStateSnapshot
)


DOMAIN_TYPES = frozenset(
    {
        AgentTurnCommitted,
        AgentTurnResult,
        CandidatesPresented,
        CommittedDialogueEvent,
        CommitAgentTurn,
        CurrentWorkState,
        DecisionContext,
        DialogueState,
        EvidenceCheck,
        GateAssessment,
        GateRequirement,
        LearnerModelEntry,
        LearnerTurnSubmitted,
        PauseTopic,
        PresentCandidates,
        RecoverWork,
        ReportWorkFailure,
        ResumeTopic,
        SelectTopic,
        SessionDeactivationPrepared,
        SessionStarted,
        StartSession,
        StartTopic,
        SubmitLearnerTurn,
        SwitchTopic,
        TaskScope,
        TopicContract,
        TopicPaused,
        TopicResumed,
        TopicRunState,
        TopicSelectionSubmitted,
        TopicSwitchRequested,
        TopicStarted,
        TriggerBinding,
        WorkDeadLettered,
        WorkFailed,
        WorkFailure,
        WorkRecoveryRequested,
        WorkRequeued,
        DialogueActor,
        StoredDialogueEvent,
        MarkerEventRef,
        TransactionMarker,
        DialogueCommandReceipt,
        DialogueWriteRequestRecord,
        DialogueStateSnapshot,
    }
)
TYPE_BY_NAME = {item.__name__: item for item in DOMAIN_TYPES}


def _nonblank(value: object) -> bool:
    return type(value) is str and bool(value.strip())


EVENT_TYPE_BY_PAYLOAD = {
    SessionStarted: "session_started",
    CandidatesPresented: "topic_candidates_presented",
    TopicSelectionSubmitted: "topic_selection_submitted",
    TopicStarted: "topic_started",
    AgentTurnCommitted: "agent_turn_committed",
    LearnerTurnSubmitted: "learner_turn_submitted",
    TopicPaused: "topic_paused",
    TopicSwitchRequested: "topic_switch_requested",
    SessionDeactivationPrepared: "session_deactivation_prepared",
    TopicResumed: "topic_resumed",
    WorkFailed: "work_failed",
    WorkRequeued: "work_requeued",
    WorkDeadLettered: "work_dead_lettered",
    WorkRecoveryRequested: "work_recovery_requested",
}


def _valid_timestamp(value: object) -> bool:
    if not _nonblank(value):
        return False
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _validate_domain_identifiers(value: object) -> None:
    """Validate identifier fields without weakening exact typed encoding."""
    value_type = type(value)
    if value_type is tuple:
        for item in cast(tuple[object, ...], value):
            _validate_domain_identifiers(item)
        return
    if value_type not in DOMAIN_TYPES or not is_dataclass(value):
        return
    if value_type is TriggerBinding and not is_canonical_trigger_binding(value):
        raise ValueError("INVALID_DOMAIN_IDENTIFIER")
    for field in fields(value):
        item = getattr(value, field.name)
        if field.name.endswith("_id"):
            if isinstance(item, StrEnum):
                pass
            elif item is not None and not is_protocol_id(item):
                raise ValueError("INVALID_DOMAIN_IDENTIFIER")
        elif field.name.endswith("_ids"):
            if type(item) is not tuple or any(
                not is_protocol_id(identifier) for identifier in item
            ):
                raise ValueError("INVALID_DOMAIN_IDENTIFIER")
        elif field.name.endswith("_digest"):
            if item is not None and not is_sha256_digest(item):
                raise ValueError("INVALID_DOMAIN_DIGEST")
        _validate_domain_identifiers(item)


def _validate_committed_event(event: object) -> None:
    if (
        type(event) is not CommittedDialogueEvent
        or not is_protocol_id(event.event_id)
        or type(event.sequence) is not int
        or event.sequence < 1
        or type(event.from_version) is not int
        or event.from_version < 0
        or type(event.to_version) is not int
        or not is_protocol_id(event.command_id)
        or type(event.payload) not in EVENT_TYPE_BY_PAYLOAD
    ):
        raise ValueError("INVALID_COMMITTED_DIALOGUE_EVENT")
    try:
        expected_to_version = event.from_version + conversation_version_delta(
            event.payload
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("INVALID_COMMITTED_DIALOGUE_EVENT") from exc
    if event.to_version != expected_to_version:
        raise ValueError("INVALID_COMMITTED_DIALOGUE_EVENT")
    _validate_domain_identifiers(event)


def _validate_dialogue_state(state: object, error_code: str) -> None:
    try:
        if type(state) is not DialogueState:
            raise ValueError("STATE_INVARIANT_VIOLATION")
        validate_state(state)
        _validate_domain_identifiers(state)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError(error_code) from exc


def sha256_digest(payload: bytes) -> str:
    """Return the canonical prefixed SHA-256 digest for bytes."""
    if type(payload) is not bytes:
        raise TypeError("payload must be exact bytes")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _to_tree(value: object, expected: object) -> object:
    """Encode one value only after checking its exact runtime annotation."""
    origin = get_origin(expected)
    if origin in {UnionType, Union}:
        variants = get_args(expected)
        if value is None and type(None) in variants:
            return None
        exact_variants = tuple(
            variant
            for variant in variants
            if (
                (variant in {str, int, bool} and type(value) is variant)
                or (
                    isinstance(variant, type)
                    and variant not in {str, int, bool, type(None)}
                    and type(value) is variant
                )
            )
        )
        if len(exact_variants) != 1:
            raise ValueError("INVALID_UNION_VALUE")
        return _to_tree(value, exact_variants[0])
    if origin is tuple:
        if type(value) is not tuple:
            raise ValueError("INVALID_TUPLE_VALUE")
        arguments = get_args(expected)
        if len(arguments) == 2 and arguments[1] is Ellipsis:
            return [_to_tree(item, arguments[0]) for item in value]
        if len(arguments) != len(value):
            raise ValueError("INVALID_TUPLE_ARITY")
        return [
            _to_tree(item, argument)
            for item, argument in zip(value, arguments, strict=True)
        ]
    if expected is type(None):
        if value is not None:
            raise ValueError("INVALID_NULL_VALUE")
        return None
    if expected in {str, int, bool}:
        if type(value) is not expected:
            raise ValueError("INVALID_SCALAR_TYPE")
        return value
    if isinstance(expected, type) and issubclass(expected, StrEnum):
        if type(value) is not expected:
            raise ValueError("INVALID_ENUM_VALUE")
        return value.value
    if isinstance(expected, type) and expected in DOMAIN_TYPES:
        if type(value) is not expected or not is_dataclass(value):
            raise ValueError("INVALID_RECORD_TYPE")
        hints = get_type_hints(expected)
        return {
            "$type": expected.__name__,
            **{
                field.name: _to_tree(
                    getattr(value, field.name), hints[field.name]
                )
                for field in fields(value)
            },
        }
    raise ValueError("UNSUPPORTED_CODEC_TYPE")


def _canonical_tree_is_valid(
    value: object,
    active_containers: set[int],
) -> None:
    """Reject every Python representation outside the exact JSON subset."""
    value_type = type(value)
    if value is None or value_type in {str, bool}:
        return
    if value_type is int:
        if abs(cast(int, value)) >= 10**MAX_JSON_INTEGER_DIGITS:
            raise ValueError("INVALID_JSON_NUMBER")
        return
    if value_type not in {dict, list}:
        raise ValueError("INVALID_CANONICAL_VALUE")
    identity = id(value)
    if identity in active_containers:
        raise ValueError("INVALID_CANONICAL_VALUE")
    active_containers.add(identity)
    try:
        if value_type is dict:
            mapping = cast(dict[object, object], value)
            for key, item in mapping.items():
                if type(key) is not str:
                    raise ValueError("INVALID_CANONICAL_VALUE")
                _canonical_tree_is_valid(item, active_containers)
        else:
            for item in cast(list[object], value):
                _canonical_tree_is_valid(item, active_containers)
    finally:
        active_containers.remove(identity)


def canonical_json_bytes(value: object) -> bytes:
    """Serialize a JSON-compatible value into the one accepted byte form."""
    try:
        _canonical_tree_is_valid(value, set())
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except ValueError as exc:
        if str(exc) == "INVALID_JSON_NUMBER":
            raise
        raise ValueError("INVALID_CANONICAL_VALUE") from exc
    except (TypeError, UnicodeEncodeError, RecursionError) as exc:
        raise ValueError("INVALID_CANONICAL_VALUE") from exc


def _object_without_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _parse_json_integer(value: str) -> int:
    digits = value.removeprefix("-")
    if len(digits) > MAX_JSON_INTEGER_DIGITS:
        raise ValueError("INVALID_JSON_NUMBER")
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError("INVALID_JSON_NUMBER") from exc


def _reject_json_number(_value: str) -> object:
    raise ValueError("INVALID_JSON_NUMBER")


def _parse_canonical(raw: bytes) -> object:
    if type(raw) is not bytes or not raw or len(raw) > MAX_RECORD_BYTES:
        raise ValueError("INVALID_RECORD_SIZE")
    try:
        text = raw.decode("utf-8", errors="strict")
        value = json.loads(
            text,
            object_pairs_hook=_object_without_duplicate_keys,
            parse_int=_parse_json_integer,
            parse_float=_reject_json_number,
            parse_constant=_reject_json_number,
        )
    except ValueError as exc:
        if str(exc) in {"DUPLICATE_JSON_KEY", "INVALID_JSON_NUMBER"}:
            raise
        raise ValueError("INVALID_JSON_RECORD") from exc
    except (UnicodeDecodeError, RecursionError) as exc:
        raise ValueError("INVALID_JSON_RECORD") from exc
    try:
        canonical = canonical_json_bytes(value)
    except (ValueError, RecursionError) as exc:
        raise ValueError("INVALID_JSON_RECORD") from exc
    if canonical != raw:
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
    if isinstance(expected, type) and expected in DOMAIN_TYPES:
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
    raise ValueError("UNSUPPORTED_CODEC_TYPE")


def _encode_typed(value: object) -> bytes:
    value_type = type(value)
    if value_type not in DOMAIN_TYPES:
        raise ValueError("UNSUPPORTED_CODEC_VALUE")
    try:
        return canonical_json_bytes(_to_tree(value, value_type))
    except RecursionError as exc:
        raise ValueError("INVALID_TYPED_VALUE") from exc


def _decode_typed(raw: bytes, expected: object) -> object:
    return _from_tree(_parse_canonical(raw), expected)


def dialogue_state_digest(state: DialogueState) -> str:
    """Hash one exact canonical dialogue state."""
    _validate_dialogue_state(state, "INVALID_DIALOGUE_STATE")
    return sha256_digest(_encode_typed(state))


def dialogue_request_digest(record: DialogueWriteRequestRecord) -> str:
    """Hash one exact typed semantic request for idempotency checks."""
    if (
        type(record) is not DialogueWriteRequestRecord
        or record.schema_version != SCHEMA_VERSION
        or record.record_type != "dialogue_write_request"
        or not _nonblank(record.session_id)
        or type(record.expected_registry_generation) is not int
        or record.expected_registry_generation < 1
        or type(record.expected_conversation_version) is not int
        or record.expected_conversation_version < 0
        or type(record.command)
        not in {
            StartSession,
            PresentCandidates,
            SelectTopic,
            StartTopic,
            CommitAgentTurn,
            SubmitLearnerTurn,
            PauseTopic,
            SwitchTopic,
            ResumeTopic,
            ReportWorkFailure,
            RecoverWork,
        }
        or type(record.context) is not DecisionContext
    ):
        raise ValueError("INVALID_DIALOGUE_WRITE_REQUEST")
    try:
        _validate_domain_identifiers(record)
    except ValueError as exc:
        raise ValueError("INVALID_DIALOGUE_WRITE_REQUEST") from exc
    return sha256_digest(_encode_typed(record))


def build_stored_event(
    session_id: str,
    event: CommittedDialogueEvent,
    previous_event_hash: str | None,
    *,
    registry_generation: int,
    occurred_at: str,
    actor: DialogueActor,
    causation_id: str | None,
) -> StoredDialogueEvent:
    """Build and hash one immutable persisted dialogue event."""
    if (
        not _nonblank(session_id)
        or type(event) is not CommittedDialogueEvent
        or type(registry_generation) is not int
        or registry_generation < 1
        or not _valid_timestamp(occurred_at)
        or type(actor) is not DialogueActor
        or type(actor.kind) is not ActorKind
        or not _nonblank(actor.actor_id)
        or (causation_id is not None and not _nonblank(causation_id))
    ):
        raise ValueError("INVALID_STORED_EVENT")
    if previous_event_hash is not None and not is_sha256_digest(previous_event_hash):
        raise ValueError("INVALID_STORED_EVENT")
    try:
        _validate_committed_event(event)
        _validate_domain_identifiers(actor)
    except ValueError as exc:
        raise ValueError("INVALID_STORED_EVENT") from exc
    unhashed = StoredDialogueEvent(
        schema_version=SCHEMA_VERSION,
        record_type="dialogue_event",
        protocol_version=PROTOCOL_VERSION,
        session_id=session_id,
        registry_generation=registry_generation,
        event_type=EVENT_TYPE_BY_PAYLOAD.get(type(event.payload), ""),
        occurred_at=occurred_at,
        actor=actor,
        causation_id=causation_id,
        previous_event_hash=previous_event_hash,
        event=event,
        event_hash="",
    )
    digest = sha256_digest(_encode_typed(unhashed))
    result = StoredDialogueEvent(
        schema_version=unhashed.schema_version,
        record_type=unhashed.record_type,
        protocol_version=unhashed.protocol_version,
        session_id=unhashed.session_id,
        registry_generation=unhashed.registry_generation,
        event_type=unhashed.event_type,
        occurred_at=unhashed.occurred_at,
        actor=unhashed.actor,
        causation_id=unhashed.causation_id,
        previous_event_hash=unhashed.previous_event_hash,
        event=unhashed.event,
        event_hash=digest,
    )
    _validate_stored_event(result)
    return result


def encode_stored_event(record: StoredDialogueEvent) -> bytes:
    """Encode a valid stored event, rejecting tampered hashes."""
    _validate_stored_event(record)
    return _encode_typed(record)


def decode_stored_event(raw: bytes) -> StoredDialogueEvent:
    """Decode one exact stored event and verify its self hash."""
    record = _decode_typed(raw, StoredDialogueEvent)
    if type(record) is not StoredDialogueEvent:
        raise ValueError("INVALID_STORED_EVENT")
    _validate_stored_event(record)
    return record


def _validate_stored_event(record: object) -> None:
    if (
        type(record) is not StoredDialogueEvent
        or record.schema_version != SCHEMA_VERSION
        or record.record_type != "dialogue_event"
        or record.protocol_version != PROTOCOL_VERSION
        or not _nonblank(record.session_id)
        or type(record.registry_generation) is not int
        or record.registry_generation < 1
        or type(record.event) is not CommittedDialogueEvent
        or record.event_type != EVENT_TYPE_BY_PAYLOAD.get(type(record.event.payload))
        or not _valid_timestamp(record.occurred_at)
        or type(record.actor) is not DialogueActor
        or type(record.actor.kind) is not ActorKind
        or not _nonblank(record.actor.actor_id)
        or (record.causation_id is not None and not _nonblank(record.causation_id))
        or not is_sha256_digest(record.event_hash)
        or (
            record.previous_event_hash is not None
            and not is_sha256_digest(record.previous_event_hash)
        )
    ):
        raise ValueError("INVALID_STORED_EVENT")
    try:
        _validate_committed_event(record.event)
        _validate_domain_identifiers(record.actor)
    except ValueError as exc:
        raise ValueError("INVALID_STORED_EVENT") from exc
    unhashed = StoredDialogueEvent(
        schema_version=record.schema_version,
        record_type=record.record_type,
        protocol_version=record.protocol_version,
        session_id=record.session_id,
        registry_generation=record.registry_generation,
        event_type=record.event_type,
        occurred_at=record.occurred_at,
        actor=record.actor,
        causation_id=record.causation_id,
        previous_event_hash=record.previous_event_hash,
        event=record.event,
        event_hash="",
    )
    if sha256_digest(_encode_typed(unhashed)) != record.event_hash:
        raise ValueError("STORED_EVENT_HASH_MISMATCH")


def marker_event_ref(record: StoredDialogueEvent) -> MarkerEventRef:
    """Create the marker reference for one validated stored event."""
    _validate_stored_event(record)
    return MarkerEventRef(
        sequence=record.event.sequence,
        event_id=record.event.event_id,
        event_hash=record.event_hash,
    )


def build_transaction_marker(
    *,
    session_id: str,
    transaction_id: str,
    command_id: str,
    request_digest: str,
    registry_generation: int,
    previous_marker_hash: str | None,
    state: DialogueState,
    events: tuple[StoredDialogueEvent, ...],
) -> TransactionMarker:
    """Build a marker whose hash binds the event list and resulting state."""
    if (
        not all(_nonblank(item) for item in (session_id, transaction_id, command_id))
        or not is_sha256_digest(request_digest)
        or type(registry_generation) is not int
        or registry_generation < 1
        or type(events) is not tuple
        or not events
        or type(state) is not DialogueState
        or state.session_id != session_id
        or (
            previous_marker_hash is not None
            and not is_sha256_digest(previous_marker_hash)
        )
    ):
        raise ValueError("INVALID_TRANSACTION_MARKER")
    _validate_dialogue_state(state, "INVALID_TRANSACTION_MARKER")
    references = tuple(marker_event_ref(item) for item in events)
    if (
        any(item.session_id != session_id for item in events)
        or any(item.registry_generation != registry_generation for item in events)
        or any(item.event.command_id != command_id for item in events)
        or tuple(item.event.sequence for item in events)
        != tuple(range(events[0].event.sequence, events[-1].event.sequence + 1))
        or state.sequence != events[-1].event.sequence
        or state.conversation_version != events[-1].event.to_version
        or any(
            current.previous_event_hash != previous.event_hash
            for previous, current in pairwise(events)
        )
        or any(
            current.event.from_version != previous.event.to_version
            for previous, current in pairwise(events)
        )
    ):
        raise ValueError("INVALID_TRANSACTION_MARKER")
    marker = TransactionMarker(
        schema_version=SCHEMA_VERSION,
        record_type="dialogue_transaction_marker",
        session_id=session_id,
        transaction_id=transaction_id,
        command_id=command_id,
        request_digest=request_digest,
        registry_generation=registry_generation,
        from_sequence=references[0].sequence,
        to_sequence=references[-1].sequence,
        previous_marker_hash=previous_marker_hash,
        state_digest=dialogue_state_digest(state),
        effect_intents=(),
        events=references,
        marker_hash="",
    )
    digest = sha256_digest(_encode_typed(marker))
    result = TransactionMarker(
        schema_version=marker.schema_version,
        record_type=marker.record_type,
        session_id=marker.session_id,
        transaction_id=marker.transaction_id,
        command_id=marker.command_id,
        request_digest=marker.request_digest,
        registry_generation=marker.registry_generation,
        from_sequence=marker.from_sequence,
        to_sequence=marker.to_sequence,
        previous_marker_hash=marker.previous_marker_hash,
        state_digest=marker.state_digest,
        effect_intents=(),
        events=marker.events,
        marker_hash=digest,
    )
    _validate_transaction_marker(result)
    return result


def encode_transaction_marker(marker: TransactionMarker) -> bytes:
    """Encode a valid transaction marker."""
    _validate_transaction_marker(marker)
    return _encode_typed(marker)


def decode_transaction_marker(raw: bytes) -> TransactionMarker:
    """Decode a transaction marker and verify all intrinsic invariants."""
    marker = _decode_typed(raw, TransactionMarker)
    if type(marker) is not TransactionMarker:
        raise ValueError("INVALID_TRANSACTION_MARKER")
    _validate_transaction_marker(marker)
    return marker


def receipt_from_marker(marker: TransactionMarker) -> DialogueCommandReceipt:
    """Derive the stable command receipt committed by ``marker``."""
    _validate_transaction_marker(marker)
    return DialogueCommandReceipt(
        command_id=marker.command_id,
        request_digest=marker.request_digest,
        transaction_id=marker.transaction_id,
        marker_hash=marker.marker_hash,
        from_sequence=marker.from_sequence,
        to_sequence=marker.to_sequence,
        event_ids=tuple(item.event_id for item in marker.events),
        state_digest=marker.state_digest,
    )


def _validate_transaction_marker(marker: object) -> None:
    if (
        type(marker) is not TransactionMarker
        or marker.schema_version != SCHEMA_VERSION
        or marker.record_type != "dialogue_transaction_marker"
        or not all(
            _nonblank(item)
            for item in (marker.session_id, marker.transaction_id, marker.command_id)
        )
        or not is_sha256_digest(marker.request_digest)
        or type(marker.registry_generation) is not int
        or marker.registry_generation < 1
        or not is_sha256_digest(marker.state_digest)
        or not is_sha256_digest(marker.marker_hash)
        or (
            marker.previous_marker_hash is not None
            and not is_sha256_digest(marker.previous_marker_hash)
        )
        or type(marker.effect_intents) is not tuple
        or marker.effect_intents
        or type(marker.events) is not tuple
        or not marker.events
        or any(type(item) is not MarkerEventRef for item in marker.events)
    ):
        raise ValueError("INVALID_TRANSACTION_MARKER")
    sequences = tuple(item.sequence for item in marker.events)
    if (
        any(
            type(item.sequence) is not int or item.sequence < 1
            for item in marker.events
        )
        or any(not _nonblank(item.event_id) for item in marker.events)
        or any(
            not is_sha256_digest(item.event_hash) for item in marker.events
        )
        or len({item.event_id for item in marker.events}) != len(marker.events)
        or sequences != tuple(range(sequences[0], sequences[-1] + 1))
        or marker.from_sequence != sequences[0]
        or marker.to_sequence != sequences[-1]
    ):
        raise ValueError("INVALID_TRANSACTION_MARKER")
    unhashed = TransactionMarker(
        schema_version=marker.schema_version,
        record_type=marker.record_type,
        session_id=marker.session_id,
        transaction_id=marker.transaction_id,
        command_id=marker.command_id,
        request_digest=marker.request_digest,
        registry_generation=marker.registry_generation,
        from_sequence=marker.from_sequence,
        to_sequence=marker.to_sequence,
        previous_marker_hash=marker.previous_marker_hash,
        state_digest=marker.state_digest,
        effect_intents=(),
        events=marker.events,
        marker_hash="",
    )
    if sha256_digest(_encode_typed(unhashed)) != marker.marker_hash:
        raise ValueError("TRANSACTION_MARKER_HASH_MISMATCH")


def _validate_command_receipt(
    receipt: object,
    *,
    expected_sequence: int,
) -> None:
    if (
        type(receipt) is not DialogueCommandReceipt
        or not _nonblank(receipt.command_id)
        or not is_sha256_digest(receipt.request_digest)
        or not _nonblank(receipt.transaction_id)
        or not is_sha256_digest(receipt.marker_hash)
        or type(receipt.from_sequence) is not int
        or type(receipt.to_sequence) is not int
        or receipt.from_sequence != expected_sequence
        or receipt.to_sequence < receipt.from_sequence
        or type(receipt.event_ids) is not tuple
        or len(receipt.event_ids)
        != receipt.to_sequence - receipt.from_sequence + 1
        or any(not _nonblank(item) for item in receipt.event_ids)
        or len(set(receipt.event_ids)) != len(receipt.event_ids)
        or not is_sha256_digest(receipt.state_digest)
    ):
        raise ValueError("INVALID_STATE_SNAPSHOT")


def build_state_snapshot(
    state: DialogueState,
    *,
    last_event_hash: str | None,
    last_marker_hash: str | None,
    receipts: tuple[DialogueCommandReceipt, ...],
) -> DialogueStateSnapshot:
    """Build the disposable, self-verifying state projection record."""
    _validate_dialogue_state(state, "INVALID_STATE_SNAPSHOT")
    snapshot = DialogueStateSnapshot(
        schema_version=SCHEMA_VERSION,
        record_type="dialogue_state_snapshot",
        session_id=state.session_id,
        through_sequence=state.sequence,
        state_digest=dialogue_state_digest(state),
        last_event_hash=last_event_hash,
        last_marker_hash=last_marker_hash,
        receipts=receipts,
        state=state,
    )
    _validate_state_snapshot(snapshot)
    return snapshot


def encode_state_snapshot(snapshot: DialogueStateSnapshot) -> bytes:
    """Encode a disposable state snapshot after verifying its digest."""
    _validate_state_snapshot(snapshot)
    return _encode_typed(snapshot)


def decode_state_snapshot(raw: bytes) -> DialogueStateSnapshot:
    """Decode and validate a disposable state snapshot."""
    snapshot = _decode_typed(raw, DialogueStateSnapshot)
    if type(snapshot) is not DialogueStateSnapshot:
        raise ValueError("INVALID_STATE_SNAPSHOT")
    _validate_state_snapshot(snapshot)
    return snapshot


def _validate_state_snapshot(snapshot: object) -> None:
    if (
        type(snapshot) is not DialogueStateSnapshot
        or snapshot.schema_version != SCHEMA_VERSION
        or snapshot.record_type != "dialogue_state_snapshot"
        or not _nonblank(snapshot.session_id)
        or type(snapshot.through_sequence) is not int
        or snapshot.through_sequence < 0
        or not is_sha256_digest(snapshot.state_digest)
        or type(snapshot.receipts) is not tuple
        or any(type(item) is not DialogueCommandReceipt for item in snapshot.receipts)
        or type(snapshot.state) is not DialogueState
        or snapshot.state.session_id != snapshot.session_id
        or snapshot.state.sequence != snapshot.through_sequence
        or dialogue_state_digest(snapshot.state) != snapshot.state_digest
    ):
        raise ValueError("INVALID_STATE_SNAPSHOT")
    _validate_dialogue_state(snapshot.state, "INVALID_STATE_SNAPSHOT")
    if snapshot.through_sequence == 0:
        if (
            snapshot.last_event_hash is not None
            or snapshot.last_marker_hash is not None
            or snapshot.receipts
        ):
            raise ValueError("INVALID_STATE_SNAPSHOT")
        return
    if (
        not is_sha256_digest(snapshot.last_event_hash)
        or not is_sha256_digest(snapshot.last_marker_hash)
        or not snapshot.receipts
    ):
        raise ValueError("INVALID_STATE_SNAPSHOT")
    expected_sequence = 1
    command_ids: set[str] = set()
    transaction_ids: set[str] = set()
    marker_hashes: set[str] = set()
    event_ids: set[str] = set()
    for receipt in snapshot.receipts:
        _validate_command_receipt(receipt, expected_sequence=expected_sequence)
        if (
            receipt.command_id in command_ids
            or receipt.transaction_id in transaction_ids
            or receipt.marker_hash in marker_hashes
            or any(item in event_ids for item in receipt.event_ids)
        ):
            raise ValueError("INVALID_STATE_SNAPSHOT")
        expected_sequence = receipt.to_sequence + 1
        command_ids.add(receipt.command_id)
        transaction_ids.add(receipt.transaction_id)
        marker_hashes.add(receipt.marker_hash)
        event_ids.update(receipt.event_ids)
    if (
        expected_sequence - 1 != snapshot.through_sequence
        or snapshot.receipts[-1].marker_hash != snapshot.last_marker_hash
        or snapshot.receipts[-1].state_digest != snapshot.state_digest
    ):
        raise ValueError("INVALID_STATE_SNAPSHOT")
