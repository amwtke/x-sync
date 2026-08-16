"""After-commit, read-only Observer delivery for X-Sync v2."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from itertools import pairwise
from typing import Protocol, TypeAlias


class StreamKind(StrEnum):
    DIALOGUE = "dialogue"
    REGISTRY = "registry"


_EventKey: TypeAlias = tuple[StreamKind, str, str]
_EventIdentity: TypeAlias = tuple[
    int,
    str,
    tuple[tuple[str, str], ...],
]
_SequenceKey: TypeAlias = tuple[StreamKind, str, int]
_SequenceIdentity: TypeAlias = tuple[
    str,
    str,
    tuple[tuple[str, str], ...],
]


@dataclass(frozen=True, slots=True)
class ImmutablePayloadView:
    tag: str
    fields: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if (
            type(self) is not ImmutablePayloadView
            or type(self.tag) is not str
            or not self.tag.strip()
            or type(self.fields) is not tuple
        ):
            raise ValueError("MUTABLE_PAYLOAD_VIEW")
        if any(
            type(item) is not tuple
            or len(item) != 2
            or any(type(value) is not str for value in item)
            for item in self.fields
        ):
            raise ValueError("MUTABLE_PAYLOAD_VIEW")


@dataclass(frozen=True, slots=True)
class CommittedEventView:
    event_id: str
    sequence: int
    payload: ImmutablePayloadView

    def __post_init__(self) -> None:
        if (
            type(self) is not CommittedEventView
            or type(self.event_id) is not str
            or not self.event_id.strip()
            or type(self.sequence) is not int
            or self.sequence < 1
            or type(self.payload) is not ImmutablePayloadView
        ):
            raise ValueError("INVALID_EVENT_VIEW")


@dataclass(frozen=True, slots=True)
class CommittedBatch:
    stream_kind: StreamKind
    stream_id: str
    events: tuple[CommittedEventView, ...]

    def __post_init__(self) -> None:
        if type(self) is not CommittedBatch:
            raise ValueError("INVALID_COMMITTED_BATCH")
        if type(self.stream_kind) is not StreamKind:
            raise ValueError("INVALID_STREAM_KIND")
        if (
            type(self.stream_id) is not str
            or not self.stream_id.strip()
            or type(self.events) is not tuple
            or not self.events
        ):
            raise ValueError("INVALID_COMMITTED_BATCH")
        if any(type(event) is not CommittedEventView for event in self.events):
            raise ValueError("INVALID_COMMITTED_BATCH")
        event_ids = tuple(event.event_id for event in self.events)
        if any(not event_id.strip() for event_id in event_ids) or len(
            event_ids
        ) != len(set(event_ids)):
            raise ValueError("INVALID_COMMITTED_BATCH")
        sequences = tuple(event.sequence for event in self.events)
        if any(right != left + 1 for left, right in pairwise(sequences)):
            raise ValueError("EVENT_SEQUENCE_GAP")


@dataclass(frozen=True, slots=True)
class DispatchReport:
    attempted: tuple[str, ...]
    failed: tuple[str, ...]
    errors: tuple[tuple[str, str], ...]


class DomainLockTracker(Protocol):
    def assert_none_held(self) -> None: ...


class Observer(Protocol):
    name: str
    accepted_streams: frozenset[StreamKind]

    def on_batch(self, batch: CommittedBatch) -> None: ...


@dataclass(frozen=True, slots=True)
class _ObserverRegistration:
    name: str
    accepted_streams: frozenset[StreamKind]
    callback: Callable[[CommittedBatch], None]


class ObserverHub:
    """Dispatch immutable batches after all domain locks are released."""

    def __init__(self, lock_tracker: DomainLockTracker):
        self._lock_tracker = lock_tracker
        self._registrations: dict[str, _ObserverRegistration] = {}
        self._registry: tuple[_ObserverRegistration, ...] = ()
        self._seen: dict[str, set[_EventKey]] = {}
        self._known_events: dict[_EventKey, _EventIdentity] = {}
        self._known_sequences: dict[
            _SequenceKey,
            _SequenceIdentity,
        ] = {}
        self._cursor: dict[tuple[str, StreamKind, str], int] = {}
        self._frozen = False
        self._local = threading.local()
        self._dispatch_lock = threading.Lock()

    def register_fixed(self, observer: Observer) -> None:
        """Register one startup-time Observer before freezing the hub."""
        if self._frozen:
            raise RuntimeError("OBSERVER_REGISTRY_FROZEN")
        try:
            name = observer.name
            accepted_streams = observer.accepted_streams
            callback = observer.on_batch
        except Exception as exc:
            raise ValueError("INVALID_OBSERVER") from exc
        if type(name) is not str or not name.strip():
            raise ValueError("INVALID_OBSERVER")
        if name in self._registrations:
            raise ValueError("DUPLICATE_OBSERVER")
        if (
            type(accepted_streams) is not frozenset
            or not accepted_streams
            or any(
                type(item) is not StreamKind
                for item in accepted_streams
            )
        ):
            raise ValueError("INVALID_OBSERVER_STREAMS")
        if not callable(callback):
            raise ValueError("INVALID_OBSERVER")
        self._registrations[name] = _ObserverRegistration(
            name,
            accepted_streams,
            callback,
        )
        self._seen[name] = set()

    def freeze(self) -> None:
        """Close the Observer registry before runtime dispatch starts."""
        self._registry = tuple(
            self._registrations[name]
            for name in sorted(self._registrations)
        )
        self._frozen = True

    def assert_command_entry_allowed(self) -> None:
        """Reject a command entry attempted recursively from an Observer."""
        if getattr(self._local, "dispatching", False):
            raise RuntimeError("OBSERVER_REENTRANCY")

    def publish(self, batch: CommittedBatch) -> DispatchReport:
        """Deliver one committed batch with isolation and idempotency."""
        if type(batch) is not CommittedBatch:
            raise ValueError("INVALID_COMMITTED_BATCH")
        self._lock_tracker.assert_none_held()
        self.assert_command_entry_allowed()
        if not self._frozen:
            raise RuntimeError("OBSERVER_REGISTRY_NOT_FROZEN")
        with self._dispatch_lock:
            return self._publish_serial(batch)

    def _publish_serial(self, batch: CommittedBatch) -> DispatchReport:
        attempted = []
        failed = []
        errors = []
        self._local.dispatching = True
        try:
            identities = tuple(
                (
                    (batch.stream_kind, batch.stream_id, event.event_id),
                    (
                        event.sequence,
                        event.payload.tag,
                        event.payload.fields,
                    ),
                )
                for event in batch.events
            )
            sequence_identities = tuple(
                (
                    (batch.stream_kind, batch.stream_id, event.sequence),
                    (
                        event.event_id,
                        event.payload.tag,
                        event.payload.fields,
                    ),
                )
                for event in batch.events
            )
            event_conflict = any(
                key in self._known_events
                and self._known_events[key] != identity
                for key, identity in identities
            )
            sequence_conflict = any(
                key in self._known_sequences
                and self._known_sequences[key] != identity
                for key, identity in sequence_identities
            )
            if event_conflict or sequence_conflict:
                names = tuple(
                    registration.name
                    for registration in self._registry
                    if batch.stream_kind in registration.accepted_streams
                )
                return DispatchReport(
                    names,
                    names,
                    tuple(
                        (name, "OBSERVER_EVENT_INTEGRITY")
                        for name in names
                    ),
                )
            self._known_events.update(identities)
            self._known_sequences.update(sequence_identities)
            for registration in self._registry:
                name = registration.name
                if batch.stream_kind not in registration.accepted_streams:
                    continue
                unseen_events = tuple(
                    event for event in batch.events
                    if (
                        batch.stream_kind,
                        batch.stream_id,
                        event.event_id,
                    ) not in self._seen[name]
                )
                if not unseen_events:
                    continue
                attempted.append(name)
                cursor_key = (name, batch.stream_kind, batch.stream_id)
                cursor = self._cursor.get(cursor_key, 0)
                has_sequence_conflict = any(
                    event.sequence <= cursor
                    for event in unseen_events
                )
                has_gap = unseen_events[0].sequence != cursor + 1
                if has_sequence_conflict or has_gap:
                    failed.append(name)
                    errors.append((name, "OBSERVER_SEQUENCE_GAP"))
                    continue
                delivery = replace(batch, events=unseen_events)
                try:
                    registration.callback(delivery)
                except BaseException as exc:
                    failed.append(name)
                    code = (
                        "OBSERVER_REENTRANCY"
                        if type(exc) is RuntimeError
                        and len(exc.args) == 1
                        and type(exc.args[0]) is str
                        and exc.args[0] == "OBSERVER_REENTRANCY"
                        else "OBSERVER_CALLBACK_FAILED"
                    )
                    errors.append((name, code))
                else:
                    self._seen[name].update(
                        (
                            batch.stream_kind,
                            batch.stream_id,
                            event.event_id,
                        )
                        for event in unseen_events
                    )
                    self._cursor[cursor_key] = unseen_events[-1].sequence
        finally:
            self._local.dispatching = False
        return DispatchReport(tuple(attempted), tuple(failed), tuple(errors))
