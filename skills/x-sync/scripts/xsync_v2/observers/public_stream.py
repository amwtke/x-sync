"""Bounded, safe browser-stream projection of committed dialogue facts.

This module deliberately stops before HTTP or SSE transport.  It keeps a
small replay window and bounded per-subscriber queues; callers translate its
stable cursor errors into their transport's response envelope.
"""

from __future__ import annotations

import math
import threading
from collections import deque
from dataclasses import dataclass

from ..observer import CommittedBatch, CommittedEventView, StreamKind

_MAX_STREAM_LIMIT = 65_536

# The source DTO is an internal, immutable view.  Only these explicitly public
# fields cross the Browser boundary.  Trigger, evidence, lease and claim data
# are intentionally absent.
_PUBLIC_FIELD_ORDER: dict[str, tuple[str, ...]] = {
    "state_changed": (),
    "session_started": (),
    "topic_candidates_presented": ("candidates",),
    "topic_selection_submitted": ("candidate",),
    "topic_clarification_requested": ("question_id", "question"),
    "topic_clarification_answered": ("question_id",),
    "topic_started": (
        "topic_run_id",
        "title",
        "guiding_question",
        "objective",
        "starting_lens",
    ),
    "agent_turn_committed": (
        "heard",
        "one_step_further",
        "question_id",
        "question",
        "question_intent",
    ),
    "learner_turn_submitted": (
        "question_id",
        "learner_turn_id",
        "text",
    ),
    "lens_changed": ("topic_run_id", "lens"),
    "help_requested": ("topic_run_id", "question_id"),
    "topic_paused": ("topic_run_id", "cause"),
    "topic_switch_requested": ("topic_run_id",),
    "topic_resumed": ("topic_run_id", "requires_reground"),
}


class PublicStreamError(RuntimeError):
    """Stable adapter failure for the future HTTP/SSE boundary."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class PublicStreamEvent:
    """One browser-safe committed event with no internal authority data."""

    stream_id: str
    event_id: str
    sequence: int
    event_type: str
    fields: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if (
            type(self) is not PublicStreamEvent
            or type(self.stream_id) is not str
            or not self.stream_id.strip()
            or type(self.event_id) is not str
            or not self.event_id.strip()
            or type(self.sequence) is not int
            or self.sequence < 1
            or type(self.event_type) is not str
            or not self.event_type.strip()
            or type(self.fields) is not tuple
            or any(
                type(item) is not tuple
                or len(item) != 2
                or any(type(value) is not str for value in item)
                for item in self.fields
            )
        ):
            raise ValueError("INVALID_PUBLIC_STREAM_EVENT")
        names = tuple(name for name, _value in self.fields)
        if len(names) != len(set(names)):
            raise ValueError("INVALID_PUBLIC_STREAM_EVENT")


@dataclass(slots=True)
class _StreamState:
    retained: deque[PublicStreamEvent]
    through_sequence: int = 0


@dataclass(slots=True)
class _SubscriberState:
    stream_id: str
    cursor: int
    pending: deque[PublicStreamEvent]
    terminal_code: str | None = None


class PublicStreamSubscription:
    """A non-blocking handle consumed by a future authenticated transport."""

    __slots__ = ("_observer", "_subscriber_id")

    def __init__(
        self,
        observer: PublicStreamObserver,
        subscriber_id: int,
    ) -> None:
        self._observer = observer
        self._subscriber_id = subscriber_id

    @property
    def cursor(self) -> int:
        """Return the last sequence consumed by this subscriber."""
        return self._observer._subscription_cursor(self._subscriber_id)

    @property
    def connected(self) -> bool:
        """Report whether the bounded queue can still be consumed."""
        return self._observer._subscription_connected(self._subscriber_id)

    def read_available(
        self,
        max_events: int | None = None,
    ) -> tuple[PublicStreamEvent, ...]:
        """Drain currently available events without blocking."""
        return self._observer._read_available(
            self._subscriber_id,
            max_events,
        )

    def wait_available(self, timeout: float | int) -> bool:
        """Wait locally for queued events; timeout never triggers a model call."""
        return self._observer._wait_available(self._subscriber_id, timeout)

    def close(self) -> None:
        """Idempotently release this transient subscription."""
        self._observer._close_subscription(self._subscriber_id)


class PublicStreamObserver:
    """Project committed dialogue DTOs into bounded browser queues."""

    name = "browser-stream"
    accepted_streams = frozenset({StreamKind.DIALOGUE})

    def __init__(
        self,
        *,
        retention_limit: int = 256,
        subscriber_queue_limit: int = 64,
    ) -> None:
        if not _valid_limit(retention_limit) or not _valid_limit(
            subscriber_queue_limit
        ):
            raise ValueError("INVALID_STREAM_LIMIT")
        self._retention_limit = retention_limit
        self._subscriber_queue_limit = subscriber_queue_limit
        self._streams: dict[str, _StreamState] = {}
        self._subscribers: dict[int, _SubscriberState] = {}
        self._next_subscriber_id = 1
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)

    def subscribe(
        self,
        stream_id: str,
        after_sequence: int,
    ) -> PublicStreamSubscription:
        """Open a bounded subscription after an already-consumed cursor."""
        if type(stream_id) is not str or not stream_id.strip():
            raise ValueError("INVALID_STREAM_ID")
        if type(after_sequence) is not int or after_sequence < 0:
            raise ValueError("INVALID_STREAM_CURSOR")
        with self._lock:
            stream = self._streams.get(stream_id)
            through_sequence = (
                stream.through_sequence if stream is not None else 0
            )
            if after_sequence > through_sequence:
                raise PublicStreamError("CURSOR_AHEAD")
            retained = () if stream is None else tuple(stream.retained)
            if retained and after_sequence < retained[0].sequence - 1:
                raise PublicStreamError("CURSOR_EXPIRED")
            pending = tuple(
                item for item in retained if item.sequence > after_sequence
            )
            if len(pending) > self._subscriber_queue_limit:
                raise PublicStreamError("RESYNC_REQUIRED")
            subscriber_id = self._next_subscriber_id
            self._next_subscriber_id += 1
            self._subscribers[subscriber_id] = _SubscriberState(
                stream_id=stream_id,
                cursor=after_sequence,
                pending=deque(pending),
            )
            return PublicStreamSubscription(self, subscriber_id)

    def on_batch(self, batch: CommittedBatch) -> None:
        """Atomically enqueue one contiguous, after-commit dialogue batch."""
        if type(batch) is not CommittedBatch:
            raise ValueError("INVALID_COMMITTED_BATCH")
        if batch.stream_kind is not StreamKind.DIALOGUE:
            raise ValueError("PUBLIC_STREAM_KIND_UNSUPPORTED")
        projected = tuple(
            _project_event(batch.stream_id, item) for item in batch.events
        )
        with self._lock:
            stream = self._streams.get(batch.stream_id)
            if stream is None:
                stream = _StreamState(
                    deque(maxlen=self._retention_limit)
                )
                self._streams[batch.stream_id] = stream
            new_events = _unseen_contiguous(stream, projected)
            if not new_events:
                return
            for subscriber in self._subscribers.values():
                if (
                    subscriber.stream_id != batch.stream_id
                    or subscriber.terminal_code is not None
                ):
                    continue
                if (
                    len(subscriber.pending) + len(new_events)
                    > self._subscriber_queue_limit
                ):
                    subscriber.pending.clear()
                    subscriber.terminal_code = "RESYNC_REQUIRED"
                else:
                    subscriber.pending.extend(new_events)
            stream.retained.extend(new_events)
            stream.through_sequence = new_events[-1].sequence
            self._condition.notify_all()

    def _read_available(
        self,
        subscriber_id: int,
        max_events: int | None,
    ) -> tuple[PublicStreamEvent, ...]:
        if max_events is not None and (
            type(max_events) is not int or max_events < 1
        ):
            raise ValueError("INVALID_READ_LIMIT")
        with self._lock:
            subscriber = self._subscribers.get(subscriber_id)
            if subscriber is None:
                raise PublicStreamError("SUBSCRIPTION_CLOSED")
            if subscriber.terminal_code is not None:
                raise PublicStreamError(subscriber.terminal_code)
            count = (
                len(subscriber.pending)
                if max_events is None
                else min(max_events, len(subscriber.pending))
            )
            items = tuple(subscriber.pending.popleft() for _ in range(count))
            if items:
                subscriber.cursor = items[-1].sequence
            return items

    def _wait_available(
        self,
        subscriber_id: int,
        timeout: float | int,
    ) -> bool:
        if type(timeout) not in {int, float}:
            raise ValueError("INVALID_STREAM_TIMEOUT")
        try:
            duration = float(timeout)
        except (OverflowError, ValueError) as exc:
            raise ValueError("INVALID_STREAM_TIMEOUT") from exc
        if not math.isfinite(duration) or duration < 0:
            raise ValueError("INVALID_STREAM_TIMEOUT")
        with self._condition:
            subscriber = self._subscribers.get(subscriber_id)
            if subscriber is None:
                raise PublicStreamError("SUBSCRIPTION_CLOSED")
            if subscriber.terminal_code is not None:
                raise PublicStreamError(subscriber.terminal_code)
            if subscriber.pending:
                return True
            self._condition.wait_for(
                lambda: (
                    subscriber_id not in self._subscribers
                    or subscriber.terminal_code is not None
                    or bool(subscriber.pending)
                ),
                duration,
            )
            if subscriber_id not in self._subscribers:
                raise PublicStreamError("SUBSCRIPTION_CLOSED")
            if subscriber.terminal_code is not None:
                raise PublicStreamError(subscriber.terminal_code)
            return bool(subscriber.pending)

    def _subscription_cursor(self, subscriber_id: int) -> int:
        with self._lock:
            subscriber = self._subscribers.get(subscriber_id)
            if subscriber is None:
                raise PublicStreamError("SUBSCRIPTION_CLOSED")
            return subscriber.cursor

    def _subscription_connected(self, subscriber_id: int) -> bool:
        with self._lock:
            subscriber = self._subscribers.get(subscriber_id)
            return (
                subscriber is not None
                and subscriber.terminal_code is None
            )

    def _close_subscription(self, subscriber_id: int) -> None:
        with self._condition:
            self._subscribers.pop(subscriber_id, None)
            self._condition.notify_all()


def _valid_limit(value: object) -> bool:
    return (
        type(value) is int
        and 1 <= value <= _MAX_STREAM_LIMIT
    )


def _project_event(
    stream_id: str,
    event: CommittedEventView,
) -> PublicStreamEvent:
    field_names = tuple(name for name, _value in event.payload.fields)
    if len(field_names) != len(set(field_names)):
        raise ValueError("PUBLIC_STREAM_PAYLOAD_INVALID")
    public_order = _PUBLIC_FIELD_ORDER.get(event.payload.tag)
    if public_order is None:
        return PublicStreamEvent(
            stream_id,
            event.event_id,
            event.sequence,
            "state_changed",
            (),
        )
    source = dict(event.payload.fields)
    public_fields = tuple(
        (name, source[name]) for name in public_order if name in source
    )
    return PublicStreamEvent(
        stream_id,
        event.event_id,
        event.sequence,
        event.payload.tag,
        public_fields,
    )


def _unseen_contiguous(
    stream: _StreamState,
    projected: tuple[PublicStreamEvent, ...],
) -> tuple[PublicStreamEvent, ...]:
    retained_by_sequence = {item.sequence: item for item in stream.retained}
    for item in projected:
        known = retained_by_sequence.get(item.sequence)
        if known is not None and known != item:
            raise RuntimeError("PUBLIC_STREAM_EVENT_INTEGRITY")
    unseen = tuple(
        item for item in projected if item.sequence > stream.through_sequence
    )
    if not unseen:
        return ()
    if unseen[0].sequence != stream.through_sequence + 1:
        raise RuntimeError("PUBLIC_STREAM_SEQUENCE_GAP")
    return unseen
