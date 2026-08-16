"""Canonical event views and ordered after-commit Observer dispatch."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from typing import TypeAlias, cast

from .domain import (
    AgentTurnCommitted,
    CandidatesPresented,
    CommittedDialogueEvent,
    LearnerTurnSubmitted,
    LensChanged,
    SessionDeactivationPrepared,
    SessionStarted,
    TopicClarificationAnswered,
    TopicClarificationRequested,
    TopicPaused,
    TopicResumed,
    TopicSelectionSubmitted,
    TopicStarted,
    TopicSwitchRequested,
    WorkDeadLettered,
    WorkFailed,
    WorkRecoveryRequested,
    WorkRequeued,
)
from .event_store import DialogueCommitOutcome
from .observer import (
    CommittedBatch,
    CommittedEventView,
    DispatchReport,
    ImmutablePayloadView,
    ObserverHub,
    StreamKind,
)
from .registry import (
    Activated,
    CommittedRegistryEvent,
    Deactivated,
    DeactivationStarted,
    DialogueCreated,
)
from .registry_store import RegistryCommitOutcome


class AfterCommitError(RuntimeError):
    """Stable failure raised before after-commit delivery can begin."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class AfterCommitDiagnostic:
    """A path-free infrastructure diagnostic that never changes domain state."""

    stream_kind: StreamKind
    stream_id: str
    first_sequence: int
    code: str


@dataclass(frozen=True, slots=True)
class AfterCommitReport:
    """Observer reports and ordered-buffer state for one dispatch attempt."""

    hub_reports: tuple[DispatchReport, ...]
    diagnostics: tuple[AfterCommitDiagnostic, ...]
    buffered_events: int


@dataclass(frozen=True, slots=True)
class DialogueCommittedFact:
    """A captured Dialogue outcome awaiting post-unlock delivery."""

    session_id: str
    events: tuple[CommittedDialogueEvent, ...]
    replayed: bool


@dataclass(frozen=True, slots=True)
class RegistryCommittedFact:
    """A captured Registry outcome awaiting post-unlock delivery."""

    registry_id: str
    events: tuple[CommittedRegistryEvent, ...]
    replayed: bool


CommittedFact: TypeAlias = DialogueCommittedFact | RegistryCommittedFact


class CommittedFactCollector:
    """Operation-local collection of facts committed while locks are held."""

    def __init__(self) -> None:
        self._facts: list[CommittedFact] = []

    def capture_dialogue(
        self,
        outcome: DialogueCommitOutcome,
    ) -> DialogueCommitOutcome:
        """Capture one Dialogue outcome and return it unchanged."""
        if type(outcome) is not DialogueCommitOutcome:
            raise ValueError("INVALID_DIALOGUE_COMMIT_OUTCOME")
        self._facts.append(
            DialogueCommittedFact(
                outcome.state.session_id,
                outcome.events,
                outcome.replayed,
            )
        )
        return outcome

    def capture_dialogue_events(
        self,
        session_id: str,
        events: tuple[CommittedDialogueEvent, ...],
    ) -> None:
        """Capture a fully audited Dialogue prefix during startup replay."""
        if not events:
            return
        self._facts.append(DialogueCommittedFact(session_id, events, True))

    def capture_registry(
        self,
        outcome: RegistryCommitOutcome,
    ) -> RegistryCommitOutcome:
        """Capture one Registry outcome and return it unchanged."""
        if type(outcome) is not RegistryCommitOutcome:
            raise ValueError("INVALID_REGISTRY_COMMIT_OUTCOME")
        self._facts.append(
            RegistryCommittedFact(
                outcome.state.registry_id,
                outcome.transaction.events,
                outcome.replayed,
            )
        )
        return outcome

    def capture_registry_events(
        self,
        registry_id: str,
        events: tuple[CommittedRegistryEvent, ...],
    ) -> None:
        """Capture a fully audited Registry prefix during startup replay."""
        if not events:
            return
        self._facts.append(RegistryCommittedFact(registry_id, events, True))

    def freeze(self) -> tuple[CommittedFact, ...]:
        """Return the immutable facts captured by this one operation."""
        return tuple(self._facts)


_StreamKey: TypeAlias = tuple[StreamKind, str]
_EventIdentity: TypeAlias = tuple[str, str, tuple[tuple[str, str], ...]]


def _fields(*items: tuple[str, str]) -> tuple[tuple[str, str], ...]:
    return items


def _dialogue_payload_view(event: CommittedDialogueEvent) -> ImmutablePayloadView:
    payload = event.payload
    if type(payload) is SessionStarted:
        return ImmutablePayloadView("session_started", ())
    if type(payload) is CandidatesPresented:
        candidates = json.dumps(
            payload.candidates,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        return ImmutablePayloadView(
            "topic_candidates_presented",
            _fields(("candidates", candidates)),
        )
    if type(payload) is TopicSelectionSubmitted:
        return ImmutablePayloadView(
            "topic_selection_submitted",
            _fields(("candidate", payload.candidate)),
        )
    if type(payload) is TopicClarificationRequested:
        return ImmutablePayloadView(
            "topic_clarification_requested",
            _fields(
                ("question_id", payload.question_id),
                ("question", payload.question),
            ),
        )
    if type(payload) is TopicClarificationAnswered:
        return ImmutablePayloadView(
            "topic_clarification_answered",
            _fields(("question_id", payload.question_id)),
        )
    if type(payload) is TopicStarted:
        contract = payload.contract
        return ImmutablePayloadView(
            "topic_started",
            _fields(
                ("topic_run_id", contract.topic_run_id),
                ("title", contract.title),
                ("guiding_question", contract.guiding_question),
                ("objective", contract.objective),
                ("starting_lens", contract.starting_lens.value),
            ),
        )
    if type(payload) is AgentTurnCommitted:
        result = payload.result
        return ImmutablePayloadView(
            "agent_turn_committed",
            _fields(
                ("heard", result.heard),
                ("one_step_further", result.one_step_further),
                ("question_id", result.question_id),
                ("question", result.question),
                ("question_intent", result.question_intent.value),
            ),
        )
    if type(payload) is LearnerTurnSubmitted:
        return ImmutablePayloadView(
            "learner_turn_submitted",
            _fields(
                ("question_id", payload.question_id),
                ("learner_turn_id", payload.learner_turn_id),
                ("text", payload.text),
            ),
        )
    if type(payload) is LensChanged:
        return ImmutablePayloadView(
            "lens_changed",
            (
                ("topic_run_id", payload.topic_run_id),
                ("lens", payload.lens.value),
            ),
        )
    if type(payload) is TopicPaused:
        return ImmutablePayloadView(
            "topic_paused",
            _fields(
                ("topic_run_id", payload.topic_run_id),
                ("cause", payload.cause.value),
            ),
        )
    if type(payload) is TopicSwitchRequested:
        return ImmutablePayloadView(
            "topic_switch_requested",
            _fields(("topic_run_id", payload.topic_run_id)),
        )
    if type(payload) is SessionDeactivationPrepared:
        return ImmutablePayloadView(
            "session_deactivation_prepared",
            _fields(
                ("handoff_id", payload.handoff_id),
                ("fence_generation", str(payload.fence_generation)),
            ),
        )
    if type(payload) is TopicResumed:
        return ImmutablePayloadView(
            "topic_resumed",
            _fields(
                ("topic_run_id", payload.topic_run_id),
                ("requires_reground", _json_bool(payload.requires_reground)),
            ),
        )
    if type(payload) in {WorkFailed, WorkDeadLettered}:
        return ImmutablePayloadView("state_changed", ())
    if type(payload) is WorkRequeued:
        return ImmutablePayloadView("work_requeued", ())
    if type(payload) is WorkRecoveryRequested:
        return ImmutablePayloadView("work_recovery_requested", ())
    raise AfterCommitError("UNKNOWN_DIALOGUE_EVENT")


def _registry_payload_view(event: CommittedRegistryEvent) -> ImmutablePayloadView:
    payload = event.payload
    if type(payload) is DialogueCreated:
        return ImmutablePayloadView(
            "dialogue_created",
            _fields(("session_id", payload.target.session_id)),
        )
    if type(payload) is DeactivationStarted:
        handoff = payload.handoff
        return ImmutablePayloadView(
            "dialogue_deactivation_started",
            _fields(
                ("source_session_id", handoff.source_session_id),
                ("target_session_id", handoff.target.session_id),
                ("generation", str(handoff.generation)),
            ),
        )
    if type(payload) is Deactivated:
        proof = payload.proof
        return ImmutablePayloadView(
            "dialogue_deactivated",
            _fields(
                ("source_session_id", proof.source_session_id),
                ("target_session_id", proof.target.session_id),
                ("generation", str(proof.generation)),
            ),
        )
    if type(payload) is Activated:
        fields = [
            ("session_id", payload.target.session_id),
            ("generation", str(payload.generation)),
            ("activation_kind", payload.activation_kind.value),
        ]
        if payload.handoff_id is not None:
            fields.append(("handoff_id", payload.handoff_id))
        return ImmutablePayloadView("dialogue_activated", tuple(fields))
    raise AfterCommitError("UNKNOWN_REGISTRY_EVENT")


def _json_bool(value: bool) -> str:
    return "true" if value else "false"


def dialogue_batch(
    session_id: str,
    events: tuple[CommittedDialogueEvent, ...],
) -> CommittedBatch:
    """Map one committed Dialogue transaction to an immutable Observer batch."""
    if (
        type(session_id) is not str
        or not session_id.strip()
        or type(events) is not tuple
        or not events
        or any(type(event) is not CommittedDialogueEvent for event in events)
    ):
        raise AfterCommitError("INVALID_DIALOGUE_COMMIT")
    return CommittedBatch(
        StreamKind.DIALOGUE,
        session_id,
        tuple(
            CommittedEventView(
                event.event_id,
                event.sequence,
                _dialogue_payload_view(event),
            )
            for event in events
        ),
    )


def registry_batch(
    registry_id: str,
    events: tuple[CommittedRegistryEvent, ...],
) -> CommittedBatch:
    """Map one committed Registry transaction without inventing a Session id."""
    if (
        type(registry_id) is not str
        or not registry_id.strip()
        or type(events) is not tuple
        or not events
        or any(type(event) is not CommittedRegistryEvent for event in events)
    ):
        raise AfterCommitError("INVALID_REGISTRY_COMMIT")
    return CommittedBatch(
        StreamKind.REGISTRY,
        registry_id,
        tuple(
            CommittedEventView(
                event.event_id,
                event.registry_sequence,
                _registry_payload_view(event),
            )
            for event in events
        ),
    )


class AfterCommitDispatcher:
    """Publish committed facts in per-stream order after domain unlock.

    A commit may reach this process after a later sequence (for example when
    two request threads race after releasing their writer locks).  The
    dispatcher buffers that suffix until the missing prefix arrives.  Exact
    older replays are still forwarded so an Observer whose previous callback
    failed can catch up through the Hub's own per-observer cursor.
    """

    def __init__(self, hub: ObserverHub):
        if type(hub) is not ObserverHub:
            raise ValueError("INVALID_OBSERVER_HUB")
        self._hub = hub
        self._next_sequence: dict[_StreamKey, int] = {}
        self._pending: dict[_StreamKey, dict[int, CommittedEventView]] = {}
        self._known: dict[tuple[_StreamKey, int], _EventIdentity] = {}
        self._retry: dict[
            tuple[StreamKind, str, int, int],
            CommittedBatch,
        ] = {}
        self._lock = threading.RLock()

    def assert_command_entry_allowed(self) -> None:
        """Reject a command recursively entered from an Observer callback."""
        self._hub.assert_command_entry_allowed()

    def publish_all(
        self,
        batches: tuple[CommittedBatch, ...],
    ) -> AfterCommitReport:
        """Publish valid facts without propagating post-commit failures."""
        if type(batches) is not tuple or any(
            type(batch) is not CommittedBatch for batch in batches
        ):
            raise ValueError("INVALID_AFTER_COMMIT_BATCHES")
        hub_reports: list[DispatchReport] = []
        diagnostics: list[AfterCommitDiagnostic] = []
        with self._lock:
            self._retry_failed(hub_reports, diagnostics)
            replay: list[CommittedBatch] = []
            affected: set[_StreamKey] = set()
            for batch in batches:
                key = (batch.stream_kind, batch.stream_id)
                affected.add(key)
                next_sequence = self._next_sequence.get(key, 1)
                older: list[CommittedEventView] = []
                pending = self._pending.setdefault(key, {})
                for event in batch.events:
                    identity = (
                        event.event_id,
                        event.payload.tag,
                        event.payload.fields,
                    )
                    identity_key = (key, event.sequence)
                    known = self._known.get(identity_key)
                    if known is not None and known != identity:
                        diagnostics.append(
                            AfterCommitDiagnostic(
                                batch.stream_kind,
                                batch.stream_id,
                                event.sequence,
                                "AFTER_COMMIT_EVENT_INTEGRITY",
                            )
                        )
                        continue
                    self._known[identity_key] = identity
                    if event.sequence < next_sequence:
                        older.append(event)
                        continue
                    existing = pending.get(event.sequence)
                    if existing is not None and existing != event:
                        diagnostics.append(
                            AfterCommitDiagnostic(
                                batch.stream_kind,
                                batch.stream_id,
                                event.sequence,
                                "AFTER_COMMIT_EVENT_INTEGRITY",
                            )
                        )
                        continue
                    pending[event.sequence] = event
                if older:
                    replay.append(
                        CommittedBatch(
                            batch.stream_kind,
                            batch.stream_id,
                            tuple(older),
                        )
                    )

            for batch in replay:
                report = self._publish_no_throw(
                    batch,
                    hub_reports,
                    diagnostics,
                )
                if report is None or report.failed:
                    self._remember_retry(batch)
            drainable = affected | set(self._pending)
            for key in sorted(
                drainable,
                key=lambda item: (item[0].value, item[1]),
            ):
                self._drain(key, hub_reports, diagnostics)
            buffered = sum(len(items) for items in self._pending.values()) + sum(
                len(batch.events) for batch in self._retry.values()
            )
        return AfterCommitReport(
            tuple(hub_reports),
            tuple(diagnostics),
            buffered,
        )

    def publish_facts(
        self,
        facts: tuple[CommittedFact, ...],
    ) -> AfterCommitReport:
        """Map captured outcomes and deliver them without masking a commit."""
        if type(facts) is not tuple or any(
            type(fact) not in {DialogueCommittedFact, RegistryCommittedFact}
            for fact in facts
        ):
            raise ValueError("INVALID_COMMITTED_FACTS")
        batches: list[CommittedBatch] = []
        diagnostics: list[AfterCommitDiagnostic] = []
        for fact in facts:
            try:
                if isinstance(fact, DialogueCommittedFact):
                    batches.append(dialogue_batch(fact.session_id, fact.events))
                else:
                    batches.append(
                        registry_batch(
                            fact.registry_id,
                            fact.events,
                        )
                    )
            except (AfterCommitError, ValueError):
                stream_kind = (
                    StreamKind.DIALOGUE
                    if type(fact) is DialogueCommittedFact
                    else StreamKind.REGISTRY
                )
                first_sequence = _fact_first_sequence(fact)
                diagnostics.append(
                    AfterCommitDiagnostic(
                        stream_kind,
                        fact.session_id
                        if isinstance(fact, DialogueCommittedFact)
                        else fact.registry_id,
                        first_sequence,
                        "AFTER_COMMIT_MAPPING_FAILED",
                    )
                )
        delivered = self.publish_all(tuple(batches))
        return AfterCommitReport(
            delivered.hub_reports,
            tuple(diagnostics) + delivered.diagnostics,
            delivered.buffered_events,
        )

    def _drain(
        self,
        key: _StreamKey,
        hub_reports: list[DispatchReport],
        diagnostics: list[AfterCommitDiagnostic],
    ) -> None:
        pending = self._pending.setdefault(key, {})
        sequence = self._next_sequence.get(key, 1)
        ready: list[CommittedEventView] = []
        while sequence in pending:
            ready.append(pending[sequence])
            sequence += 1
        if not ready:
            return
        batch = CommittedBatch(key[0], key[1], tuple(ready))
        report = self._publish_no_throw(batch, hub_reports, diagnostics)
        if report is None:
            self._remember_retry(batch)
            for event in ready:
                del pending[event.sequence]
            self._next_sequence[key] = sequence
            return
        if report.failed:
            self._remember_retry(batch)
        for event in ready:
            del pending[event.sequence]
        self._next_sequence[key] = sequence

    def _publish_no_throw(
        self,
        batch: CommittedBatch,
        hub_reports: list[DispatchReport],
        diagnostics: list[AfterCommitDiagnostic],
    ) -> DispatchReport | None:
        try:
            report = self._hub.publish(batch)
        except BaseException:
            diagnostics.append(
                AfterCommitDiagnostic(
                    batch.stream_kind,
                    batch.stream_id,
                    batch.events[0].sequence,
                    "AFTER_COMMIT_PUBLISH_FAILED",
                )
            )
            return None
        hub_reports.append(report)
        return report

    def _remember_retry(self, batch: CommittedBatch) -> None:
        key = (
            batch.stream_kind,
            batch.stream_id,
            batch.events[0].sequence,
            batch.events[-1].sequence,
        )
        self._retry[key] = batch

    def _retry_failed(
        self,
        hub_reports: list[DispatchReport],
        diagnostics: list[AfterCommitDiagnostic],
    ) -> None:
        for key in sorted(
            tuple(self._retry),
            key=lambda item: (item[0].value, item[1], item[2], item[3]),
        ):
            batch = self._retry[key]
            report = self._publish_no_throw(batch, hub_reports, diagnostics)
            if report is not None and not report.failed:
                del self._retry[key]


def _fact_first_sequence(fact: CommittedFact) -> int:
    if not fact.events:
        return 0
    first = fact.events[0]
    if isinstance(fact, DialogueCommittedFact):
        return cast(CommittedDialogueEvent, first).sequence
    return cast(CommittedRegistryEvent, first).registry_sequence
