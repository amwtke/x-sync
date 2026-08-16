"""Fast notification adapter for already-durable dialogue work."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from ..observer import CommittedBatch, StreamKind

_WORK_TRIGGER_EVENT_TYPES = frozenset(
    {
        "session_started",
        "topic_selection_submitted",
        "topic_clarification_answered",
        "topic_started",
        "learner_turn_submitted",
        "lens_changed",
        "help_requested",
        "topic_resumed",
        "topic_switch_requested",
        "work_requeued",
        "work_recovery_requested",
    }
)


@dataclass(frozen=True, slots=True)
class WorkWakeHint:
    """A lightweight hint that requires a durable-store recheck."""

    stream_id: str
    through_sequence: int

    def __post_init__(self) -> None:
        if (
            type(self) is not WorkWakeHint
            or type(self.stream_id) is not str
            or not self.stream_id.strip()
            or type(self.through_sequence) is not int
            or self.through_sequence < 1
        ):
            raise ValueError("INVALID_WORK_WAKE_HINT")


class WorkWakePort(Protocol):
    """A fast signal port; consumers must recheck the durable work store."""

    def notify(self, hint: WorkWakeHint) -> None: ...


class WorkWakeObserver:
    """Signal a Supervisor after a work-producing fact is committed."""

    name = "host-wake"
    accepted_streams = frozenset({StreamKind.DIALOGUE})

    def __init__(self, port: WorkWakePort):
        try:
            notify = port.notify
        except Exception as exc:
            raise ValueError("INVALID_WORK_WAKE_PORT") from exc
        if not callable(notify):
            raise ValueError("INVALID_WORK_WAKE_PORT")
        self._notify: Callable[[WorkWakeHint], None] = notify
        self._through_sequence: dict[str, int] = {}
        self._lock = threading.Lock()

    def on_batch(self, batch: CommittedBatch) -> None:
        """Emit at most one coalesced hint for one committed batch."""
        if type(batch) is not CommittedBatch:
            raise ValueError("INVALID_COMMITTED_BATCH")
        if batch.stream_kind is not StreamKind.DIALOGUE:
            raise ValueError("WORK_WAKE_KIND_UNSUPPORTED")
        with self._lock:
            through = self._through_sequence.get(batch.stream_id, 0)
            unseen = tuple(
                item for item in batch.events if item.sequence > through
            )
            if not unseen:
                return
            if unseen[0].sequence != through + 1:
                raise RuntimeError("WORK_WAKE_SEQUENCE_GAP")
            if any(
                item.payload.tag in _WORK_TRIGGER_EVENT_TYPES
                for item in unseen
            ):
                self._notify(
                    WorkWakeHint(batch.stream_id, unseen[-1].sequence)
                )
            self._through_sequence[batch.stream_id] = unseen[-1].sequence
