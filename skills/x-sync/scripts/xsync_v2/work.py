"""Pure projection of canonical Host work from a dialogue aggregate.

The dialogue state contains canonical current work while the event log proves
which committed event created it.  Both are required: a state-tip sequence is
only an observation cursor and cannot identify a trigger across version-neutral
events or a later requeue that happens to reuse the same trigger fields.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
from typing import cast

from .domain import (
    ConversationPhase,
    DialogueState,
    SessionLifecycle,
    TriggerBinding,
    TriggerKind,
    WorkStatus,
)
from .state_machine import validate_state
from .work_identity import (
    WorkIdentityError,
    derive_canonical_work_id,
    is_canonical_trigger_binding,
    is_optional_protocol_id,
    is_optional_sha256_digest,
    is_protocol_id,
    is_sha256_digest,
)


class WorkError(RuntimeError):
    """A stable failure while deriving or validating durable work."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class WorkOrigin:
    """The committed event which authoritatively created the trigger.

    A trusted event-log loader must prove that this exact event payload owns
    the aggregate's current ``TriggerBinding`` before passing the origin here.
    """

    trigger_event_id: str
    trigger_event_sequence: int
    event_trigger: TriggerBinding


@dataclass(frozen=True, slots=True)
class RunnableWork:
    """Immutable Host work metadata with complete publish identity."""

    session_id: str
    registry_generation: int
    conversation_version: int
    observed_sequence: int
    trigger_event_id: str
    trigger_event_sequence: int
    topic_run_id: str | None
    kind: TriggerKind
    trigger_work_id: str
    work_id: str
    trigger_runtime_epoch: str
    parent_turn_id: str | None
    contract_digest: str | None
    input_digest: str
    evidence_digest: str
    binding_digest: str


def _valid_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _valid_positive_int(value: object) -> bool:
    return type(value) is int and value >= 1


def _origin_is_valid(origin: object, observed_sequence: int) -> bool:
    return (
        type(origin) is WorkOrigin
        and is_protocol_id(origin.trigger_event_id)
        and _valid_positive_int(origin.trigger_event_sequence)
        and origin.trigger_event_sequence <= observed_sequence
        and is_canonical_trigger_binding(origin.event_trigger)
    )


def _binding_tree(
    *,
    session_id: str,
    registry_generation: int,
    conversation_version: int,
    trigger_event_id: str,
    trigger_event_sequence: int,
    topic_run_id: str | None,
    kind: TriggerKind,
    trigger_work_id: str,
    work_id: str,
    trigger_runtime_epoch: str,
    parent_turn_id: str | None,
    contract_digest: str | None,
    input_digest: str,
    evidence_digest: str,
) -> dict[str, object]:
    return {
        "contract_digest": contract_digest,
        "conversation_version": conversation_version,
        "evidence_digest": evidence_digest,
        "input_digest": input_digest,
        "kind": kind.value,
        "parent_turn_id": parent_turn_id,
        "registry_generation": registry_generation,
        "session_id": session_id,
        "topic_run_id": topic_run_id,
        "trigger_event_id": trigger_event_id,
        "trigger_event_sequence": trigger_event_sequence,
        "trigger_runtime_epoch": trigger_runtime_epoch,
        "trigger_work_id": trigger_work_id,
        "work_id": work_id,
    }


def _digest(tree: object) -> str:
    encoded = json.dumps(
        tree,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def derive_work_id(
    *,
    session_id: str,
    origin: WorkOrigin,
    trigger: TriggerBinding,
) -> str:
    """Derive the only valid work id from a committed trigger origin.

    ``trigger.work_id`` is deliberately excluded from the digest input, so a
    caller cannot choose its own identity and then make that choice self-valid.
    """
    if type(origin) is not WorkOrigin:
        raise WorkError("WORK_ID_INPUT_INVALID")
    if (
        not is_protocol_id(session_id)
        or not _origin_is_valid(origin, origin.trigger_event_sequence)
        or not is_canonical_trigger_binding(trigger)
        or origin.event_trigger != trigger
    ):
        raise WorkError("WORK_ID_INPUT_INVALID")
    try:
        return derive_canonical_work_id(
            session_id=session_id,
            trigger_event_id=origin.trigger_event_id,
            trigger_event_sequence=origin.trigger_event_sequence,
            trigger=trigger,
        )
    except WorkIdentityError as exc:
        raise WorkError(exc.code) from exc


def _runnable_work_id(work: RunnableWork) -> str:
    trigger = TriggerBinding(
        work.kind,
        work.trigger_work_id,
        work.trigger_runtime_epoch,
        work.parent_turn_id,
        work.contract_digest,
        work.input_digest,
        work.evidence_digest,
    )
    try:
        return derive_canonical_work_id(
            session_id=work.session_id,
            trigger_event_id=work.trigger_event_id,
            trigger_event_sequence=work.trigger_event_sequence,
            trigger=trigger,
        )
    except WorkIdentityError as exc:
        raise WorkError("RUNNABLE_WORK_INVALID") from exc


def _work_binding_digest(work: RunnableWork) -> str:
    return _digest(
        _binding_tree(
            session_id=work.session_id,
            registry_generation=work.registry_generation,
            conversation_version=work.conversation_version,
            trigger_event_id=work.trigger_event_id,
            trigger_event_sequence=work.trigger_event_sequence,
            topic_run_id=work.topic_run_id,
            kind=work.kind,
            trigger_work_id=work.trigger_work_id,
            work_id=work.work_id,
            trigger_runtime_epoch=work.trigger_runtime_epoch,
            parent_turn_id=work.parent_turn_id,
            contract_digest=work.contract_digest,
            input_digest=work.input_digest,
            evidence_digest=work.evidence_digest,
        )
    )


def validate_runnable_work(work: object) -> RunnableWork:
    """Validate the exact wire-safe work shape and recompute its identity."""
    if type(work) is not RunnableWork:
        raise WorkError("RUNNABLE_WORK_INVALID")
    item = work
    if (
        not is_protocol_id(item.session_id)
        or not _valid_positive_int(item.registry_generation)
        or not _valid_nonnegative_int(item.conversation_version)
        or not _valid_positive_int(item.observed_sequence)
        or not is_protocol_id(item.trigger_event_id)
        or not _valid_positive_int(item.trigger_event_sequence)
        or item.trigger_event_sequence > item.observed_sequence
        or not is_optional_protocol_id(item.topic_run_id)
        or type(item.kind) is not TriggerKind
        or not is_protocol_id(item.trigger_work_id)
        or not is_protocol_id(item.work_id)
        or not is_protocol_id(item.trigger_runtime_epoch)
        or not is_optional_protocol_id(item.parent_turn_id)
        or not is_optional_sha256_digest(item.contract_digest)
        or not is_sha256_digest(item.input_digest)
        or not is_sha256_digest(item.evidence_digest)
        or not is_sha256_digest(item.binding_digest)
    ):
        raise WorkError("RUNNABLE_WORK_INVALID")
    if (
        item.work_id != _runnable_work_id(item)
        or _work_binding_digest(item) != item.binding_digest
    ):
        raise WorkError("RUNNABLE_WORK_INVALID")
    return item


def derive_runnable_work(
    state: DialogueState,
    origin: WorkOrigin | None,
) -> RunnableWork | None:
    """Project the aggregate's sole work using its proven event origin.

    ``origin`` must come from the authoritative committed event chain, not a
    browser/Host request.  Version-neutral events may advance
    ``observed_sequence`` without changing the returned binding digest.
    """
    if type(state) is not DialogueState:
        raise WorkError("STATE_INVALID")
    try:
        validate_state(state)
    except (TypeError, ValueError, RecursionError) as exc:
        raise WorkError("STATE_INVALID") from exc
    if (
        state.lifecycle is not SessionLifecycle.OPEN
        or state.phase is not ConversationPhase.WAITING_HOST
    ):
        if origin is not None:
            raise WorkError("WORK_ORIGIN_UNEXPECTED")
        return None
    topic = state.active_topic
    session_work = state.session_work
    topic_work = topic.work if topic is not None else None
    candidates = tuple(
        item
        for item in (session_work, topic_work)
        if item is not None and item.status is WorkStatus.QUEUED
    )
    if not candidates:
        if origin is not None:
            raise WorkError("WORK_ORIGIN_UNEXPECTED")
        return None
    if len(candidates) != 1:
        raise WorkError("STATE_INVALID")
    current = candidates[0]
    if not _origin_is_valid(origin, state.sequence):
        raise WorkError("WORK_ORIGIN_INVALID")
    proven_origin = cast(WorkOrigin, origin)
    trigger = current.trigger
    topic_run_id = topic.topic_run_id if topic is not None else None
    if (
        proven_origin.event_trigger != trigger
        or proven_origin.trigger_event_id != current.trigger_event_id
        or proven_origin.trigger_event_sequence != current.trigger_event_sequence
    ):
        raise WorkError("WORK_ORIGIN_TRIGGER_MISMATCH")
    expected_work_id = derive_work_id(
        session_id=state.session_id,
        origin=proven_origin,
        trigger=trigger,
    )
    if expected_work_id != current.work_id:
        raise WorkError("STATE_INVALID")
    provisional = RunnableWork(
        session_id=state.session_id,
        registry_generation=state.registry_generation,
        conversation_version=state.conversation_version,
        observed_sequence=state.sequence,
        trigger_event_id=proven_origin.trigger_event_id,
        trigger_event_sequence=proven_origin.trigger_event_sequence,
        topic_run_id=topic_run_id,
        kind=trigger.kind,
        trigger_work_id=trigger.work_id,
        work_id=expected_work_id,
        trigger_runtime_epoch=trigger.runtime_epoch,
        parent_turn_id=trigger.parent_turn_id,
        contract_digest=trigger.contract_digest,
        input_digest=trigger.input_digest,
        evidence_digest=trigger.evidence_digest,
        binding_digest="sha256:" + "0" * 64,
    )
    return validate_runnable_work(
        replace(provisional, binding_digest=_work_binding_digest(provisional))
    )
