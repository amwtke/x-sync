"""Pure registry fencing decisions and replay for X-Sync v2."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
import re
from typing import TypeAlias, cast


class DialogueRegistrationStatus(StrEnum):
    CREATED = "created"
    ACTIVE = "active"
    DEACTIVATING = "deactivating"
    DEACTIVATED = "deactivated"


class ActivationKind(StrEnum):
    INITIAL = "initial"
    HANDOFF = "handoff"


@dataclass(frozen=True, slots=True)
class ActivateTarget:
    session_id: str
    config_digest: str


@dataclass(frozen=True, slots=True)
class RegisteredDialogue:
    session_id: str
    config_digest: str
    status: DialogueRegistrationStatus
    created_registry_sequence: int
    activated_generation: int | None
    deactivated_generation: int | None
    deactivation_handoff_id: str | None


@dataclass(frozen=True, slots=True)
class PendingHandoff:
    handoff_id: str
    source_session_id: str
    target: ActivateTarget
    generation: int
    started_registry_sequence: int


@dataclass(frozen=True, slots=True)
class QuiesceProof:
    source_session_id: str
    target: ActivateTarget
    handoff_id: str
    generation: int
    dialogue_last_sequence: int
    dialogue_last_event_id: str
    dialogue_event_digest: str
    dialogue_transaction_digest: str
    dialogue_state_digest: str


@dataclass(frozen=True, slots=True)
class CommandReceipt:
    command_id: str
    body_digest: str
    transaction_id: str
    transaction_digest: str
    first_registry_sequence: int
    last_registry_sequence: int
    event_ids: tuple[str, ...]
    event_digests: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RegistryState:
    registry_id: str
    registry_sequence: int
    generation: int
    current_session_id: str | None
    dialogues: tuple[RegisteredDialogue, ...]
    pending_handoff: PendingHandoff | None
    receipts: tuple[CommandReceipt, ...]


@dataclass(frozen=True, slots=True)
class CreateAndActivateDialogue:
    command_id: str
    body_digest: str
    expected_registry_sequence: int
    expected_generation: int
    target: ActivateTarget


@dataclass(frozen=True, slots=True)
class BeginHandoff:
    command_id: str
    body_digest: str
    expected_registry_sequence: int
    expected_generation: int
    handoff_id: str
    source_session_id: str
    target: ActivateTarget


@dataclass(frozen=True, slots=True)
class CompleteHandoff:
    command_id: str
    body_digest: str
    expected_registry_sequence: int
    expected_generation: int
    handoff_id: str
    proof: QuiesceProof


RegistryCommand: TypeAlias = (
    CreateAndActivateDialogue | BeginHandoff | CompleteHandoff
)


@dataclass(frozen=True, slots=True)
class DialogueCreated:
    target: ActivateTarget


@dataclass(frozen=True, slots=True)
class DeactivationStarted:
    handoff: PendingHandoff


@dataclass(frozen=True, slots=True)
class Deactivated:
    proof: QuiesceProof


@dataclass(frozen=True, slots=True)
class Activated:
    target: ActivateTarget
    generation: int
    activation_kind: ActivationKind
    handoff_id: str | None


RegistryEventPayload: TypeAlias = (
    DialogueCreated | DeactivationStarted | Deactivated | Activated
)


@dataclass(frozen=True, slots=True)
class PendingRegistryEvent:
    command_id: str
    body_digest: str
    payload: RegistryEventPayload


@dataclass(frozen=True, slots=True)
class CommittedRegistryEvent:
    event_id: str
    event_digest: str
    registry_sequence: int
    generation: int
    command_id: str
    body_digest: str
    payload: RegistryEventPayload


@dataclass(frozen=True, slots=True)
class CommittedRegistryTransaction:
    transaction_id: str
    transaction_digest: str
    events: tuple[CommittedRegistryEvent, ...]


@dataclass(frozen=True, slots=True)
class RegistryAccepted:
    events: tuple[PendingRegistryEvent, ...]


@dataclass(frozen=True, slots=True)
class RegistryRejected:
    code: str


@dataclass(frozen=True, slots=True)
class RegistryIdempotent:
    receipt: CommandReceipt


RegistryDecision: TypeAlias = (
    RegistryAccepted | RegistryRejected | RegistryIdempotent
)


_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")


def _valid_id(value: object) -> bool:
    return type(value) is str and _ID_PATTERN.fullmatch(value) is not None


def _valid_digest(value: object) -> bool:
    return type(value) is str and _DIGEST_PATTERN.fullmatch(value) is not None


def _valid_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _valid_positive_int(value: object) -> bool:
    return type(value) is int and value >= 1


def _valid_optional_positive_int(value: object) -> bool:
    return value is None or _valid_positive_int(value)


def _valid_optional_id(value: object) -> bool:
    return value is None or _valid_id(value)


def _valid_target(value: object) -> bool:
    if type(value) is not ActivateTarget:
        return False
    return _valid_id(value.session_id) and _valid_digest(value.config_digest)


def _valid_registered(value: object, state: RegistryState) -> bool:
    if type(value) is not RegisteredDialogue:
        return False
    dialogue = value
    if (
        not _valid_id(dialogue.session_id)
        or not _valid_digest(dialogue.config_digest)
        or type(dialogue.status) is not DialogueRegistrationStatus
        or not _valid_positive_int(dialogue.created_registry_sequence)
        or dialogue.created_registry_sequence > state.registry_sequence
        or not _valid_optional_positive_int(dialogue.activated_generation)
        or not _valid_optional_positive_int(dialogue.deactivated_generation)
        or not _valid_optional_id(dialogue.deactivation_handoff_id)
    ):
        return False
    if dialogue.status is DialogueRegistrationStatus.CREATED:
        return (
            dialogue.activated_generation is None
            and dialogue.deactivated_generation is None
            and dialogue.deactivation_handoff_id is None
        )
    if dialogue.status is DialogueRegistrationStatus.ACTIVE:
        return (
            dialogue.activated_generation is not None
            and dialogue.activated_generation <= state.generation
            and dialogue.deactivated_generation is None
            and dialogue.deactivation_handoff_id is None
        )
    if dialogue.status is DialogueRegistrationStatus.DEACTIVATING:
        return (
            dialogue.activated_generation is not None
            and dialogue.activated_generation < state.generation
            and dialogue.deactivated_generation is None
            and dialogue.deactivation_handoff_id is None
        )
    return (
        dialogue.activated_generation is not None
        and dialogue.deactivated_generation is not None
        and dialogue.activated_generation < dialogue.deactivated_generation
        and dialogue.deactivated_generation <= state.generation
        and dialogue.deactivation_handoff_id is not None
    )


def _valid_pending(value: object) -> bool:
    if type(value) is not PendingHandoff:
        return False
    handoff = value
    return (
        _valid_id(handoff.handoff_id)
        and _valid_id(handoff.source_session_id)
        and _valid_target(handoff.target)
        and handoff.source_session_id != handoff.target.session_id
        and _valid_positive_int(handoff.generation)
        and _valid_positive_int(handoff.started_registry_sequence)
    )


def _valid_proof(value: object) -> bool:
    if type(value) is not QuiesceProof:
        return False
    proof = value
    return (
        _valid_id(proof.source_session_id)
        and _valid_target(proof.target)
        and proof.source_session_id != proof.target.session_id
        and _valid_id(proof.handoff_id)
        and _valid_positive_int(proof.generation)
        and _valid_nonnegative_int(proof.dialogue_last_sequence)
        and _valid_id(proof.dialogue_last_event_id)
        and _valid_digest(proof.dialogue_event_digest)
        and _valid_digest(proof.dialogue_transaction_digest)
        and _valid_digest(proof.dialogue_state_digest)
    )


def _valid_receipt(value: object) -> bool:
    if type(value) is not CommandReceipt:
        return False
    receipt = value
    if (
        not _valid_id(receipt.command_id)
        or not _valid_digest(receipt.body_digest)
        or not _valid_id(receipt.transaction_id)
        or not _valid_digest(receipt.transaction_digest)
        or not _valid_positive_int(receipt.first_registry_sequence)
        or not _valid_positive_int(receipt.last_registry_sequence)
        or receipt.first_registry_sequence > receipt.last_registry_sequence
        or type(receipt.event_ids) is not tuple
        or type(receipt.event_digests) is not tuple
        or any(not _valid_id(item) for item in receipt.event_ids)
        or any(not _valid_digest(item) for item in receipt.event_digests)
    ):
        return False
    event_count = (
        receipt.last_registry_sequence - receipt.first_registry_sequence + 1
    )
    return (
        len(receipt.event_ids) == event_count
        and len(receipt.event_digests) == event_count
        and len(receipt.event_ids) == len(set(receipt.event_ids))
    )


def _dialogue_by_id(
    dialogues: tuple[RegisteredDialogue, ...], session_id: str
) -> RegisteredDialogue | None:
    return next(
        (item for item in dialogues if item.session_id == session_id), None
    )


def _validate_state(value: object) -> RegistryState:
    if type(value) is not RegistryState:
        raise ValueError("STATE_INVARIANT_VIOLATION")
    state = value
    if (
        not _valid_id(state.registry_id)
        or not _valid_nonnegative_int(state.registry_sequence)
        or not _valid_nonnegative_int(state.generation)
        or not _valid_optional_id(state.current_session_id)
        or type(state.dialogues) is not tuple
        or type(state.receipts) is not tuple
        or (
            state.pending_handoff is not None
            and not _valid_pending(state.pending_handoff)
        )
        or any(not _valid_registered(item, state) for item in state.dialogues)
        or any(not _valid_receipt(item) for item in state.receipts)
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")

    session_ids = tuple(item.session_id for item in state.dialogues)
    command_ids = tuple(item.command_id for item in state.receipts)
    transaction_ids = tuple(item.transaction_id for item in state.receipts)
    event_ids = tuple(
        event_id for receipt in state.receipts for event_id in receipt.event_ids
    )
    if (
        len(session_ids) != len(set(session_ids))
        or len(command_ids) != len(set(command_ids))
        or len(transaction_ids) != len(set(transaction_ids))
        or len(event_ids) != len(set(event_ids))
    ):
        raise ValueError("STATE_INVARIANT_VIOLATION")

    expected_first = 1
    for receipt in state.receipts:
        if (
            receipt.first_registry_sequence != expected_first
            or receipt.last_registry_sequence > state.registry_sequence
        ):
            raise ValueError("STATE_INVARIANT_VIOLATION")
        expected_first = receipt.last_registry_sequence + 1
    if state.receipts and expected_first - 1 != state.registry_sequence:
        raise ValueError("STATE_INVARIANT_VIOLATION")
    if not state.receipts and state.registry_sequence != 0:
        raise ValueError("STATE_INVARIANT_VIOLATION")

    active = tuple(
        item
        for item in state.dialogues
        if item.status is DialogueRegistrationStatus.ACTIVE
    )
    deactivating = tuple(
        item
        for item in state.dialogues
        if item.status is DialogueRegistrationStatus.DEACTIVATING
    )
    if state.pending_handoff is None:
        if deactivating:
            raise ValueError("STATE_INVARIANT_VIOLATION")
        if state.current_session_id is None:
            if active or state.generation != 0:
                raise ValueError("STATE_INVARIANT_VIOLATION")
        else:
            current = _dialogue_by_id(state.dialogues, state.current_session_id)
            if (
                current is None
                or current.status is not DialogueRegistrationStatus.ACTIVE
                or current.activated_generation != state.generation
                or active != (current,)
            ):
                raise ValueError("STATE_INVARIANT_VIOLATION")
    else:
        handoff = state.pending_handoff
        source = _dialogue_by_id(state.dialogues, handoff.source_session_id)
        target = _dialogue_by_id(state.dialogues, handoff.target.session_id)
        if (
            state.current_session_id != handoff.source_session_id
            or handoff.generation != state.generation
            or handoff.started_registry_sequence > state.registry_sequence
            or source is None
            or source.status is not DialogueRegistrationStatus.DEACTIVATING
            or target is None
            or target.status is not DialogueRegistrationStatus.CREATED
            or target.config_digest != handoff.target.config_digest
            or active
            or deactivating != (source,)
        ):
            raise ValueError("STATE_INVARIANT_VIOLATION")
    return state


def _valid_command(value: object) -> bool:
    if type(value) not in (
        CreateAndActivateDialogue,
        BeginHandoff,
        CompleteHandoff,
    ):
        return False
    command = cast(RegistryCommand, value)
    if (
        not _valid_id(command.command_id)
        or not _valid_digest(command.body_digest)
        or not _valid_nonnegative_int(command.expected_registry_sequence)
        or not _valid_nonnegative_int(command.expected_generation)
    ):
        return False
    if type(command) is CreateAndActivateDialogue:
        return _valid_target(command.target)
    if type(command) is BeginHandoff:
        return (
            _valid_id(command.handoff_id)
            and _valid_id(command.source_session_id)
            and _valid_target(command.target)
            and command.source_session_id != command.target.session_id
        )
    complete = cast(CompleteHandoff, command)
    return _valid_id(complete.handoff_id) and _valid_proof(complete.proof)


def _valid_payload(value: object) -> bool:
    if type(value) is DialogueCreated:
        return _valid_target(value.target)
    if type(value) is DeactivationStarted:
        return _valid_pending(value.handoff)
    if type(value) is Deactivated:
        return _valid_proof(value.proof)
    if type(value) is Activated:
        activated = value
        if (
            not _valid_target(activated.target)
            or not _valid_positive_int(activated.generation)
            or type(activated.activation_kind) is not ActivationKind
            or not _valid_optional_id(activated.handoff_id)
        ):
            return False
        return (
            activated.activation_kind is ActivationKind.INITIAL
            and activated.handoff_id is None
        ) or (
            activated.activation_kind is ActivationKind.HANDOFF
            and activated.handoff_id is not None
        )
    return False


def _receipt_for_command(
    state: RegistryState, command_id: str
) -> CommandReceipt | None:
    return next(
        (item for item in state.receipts if item.command_id == command_id), None
    )


def _accept(
    command: RegistryCommand, payloads: tuple[RegistryEventPayload, ...]
) -> RegistryAccepted:
    return RegistryAccepted(
        tuple(
            PendingRegistryEvent(
                command.command_id,
                command.body_digest,
                payload,
            )
            for payload in payloads
        )
    )


def initial_registry_state(registry_id: str) -> RegistryState:
    """Return a validated, empty registry aggregate."""
    if not _valid_id(registry_id):
        raise ValueError("INVALID_REGISTRY_ID")
    return RegistryState(registry_id, 0, 0, None, (), None, ())


def decide(state: RegistryState, command: RegistryCommand) -> RegistryDecision:
    """Decide a registry command without performing I/O or mutating state."""
    try:
        state = _validate_state(state)
    except ValueError:
        return RegistryRejected("STATE_INVARIANT_VIOLATION")
    if not _valid_command(command):
        return RegistryRejected("VALIDATION_FAILED")

    receipt = _receipt_for_command(state, command.command_id)
    if receipt is not None:
        if receipt.body_digest == command.body_digest:
            return RegistryIdempotent(receipt)
        return RegistryRejected("IDEMPOTENCY_CONFLICT")
    if command.expected_registry_sequence != state.registry_sequence:
        return RegistryRejected("REGISTRY_SEQUENCE_CONFLICT")
    if command.expected_generation != state.generation:
        return RegistryRejected("REGISTRY_GENERATION_CONFLICT")

    if type(command) is CreateAndActivateDialogue:
        create = command
        if (
            state.current_session_id is not None
            or state.pending_handoff is not None
            or _dialogue_by_id(state.dialogues, create.target.session_id) is not None
        ):
            return RegistryRejected("REGISTRY_STATE_CONFLICT")
        generation = state.generation + 1
        return _accept(
            create,
            (
                DialogueCreated(create.target),
                Activated(
                    create.target,
                    generation,
                    ActivationKind.INITIAL,
                    None,
                ),
            ),
        )

    if type(command) is BeginHandoff:
        begin = command
        if state.pending_handoff is not None:
            return RegistryRejected("HANDOFF_IN_PROGRESS")
        if begin.source_session_id != state.current_session_id:
            return RegistryRejected("SESSION_NOT_CURRENT")
        source = _dialogue_by_id(state.dialogues, begin.source_session_id)
        if source is None or source.status is not DialogueRegistrationStatus.ACTIVE:
            return RegistryRejected("REGISTRY_STATE_CONFLICT")
        if any(
            item.deactivation_handoff_id == begin.handoff_id
            for item in state.dialogues
        ):
            return RegistryRejected("HANDOFF_ID_CONFLICT")

        target = _dialogue_by_id(state.dialogues, begin.target.session_id)
        payloads: tuple[RegistryEventPayload, ...]
        event_count: int
        if target is None:
            event_count = 2
        elif (
            target.status is DialogueRegistrationStatus.CREATED
            and target.config_digest == begin.target.config_digest
        ):
            event_count = 1
        else:
            return RegistryRejected("TARGET_SESSION_CONFLICT")
        generation = state.generation + 1
        handoff = PendingHandoff(
            begin.handoff_id,
            begin.source_session_id,
            begin.target,
            generation,
            state.registry_sequence + event_count,
        )
        started = DeactivationStarted(handoff)
        if target is None:
            payloads = (DialogueCreated(begin.target), started)
        else:
            payloads = (started,)
        return _accept(begin, payloads)

    complete = cast(CompleteHandoff, command)
    pending = state.pending_handoff
    if pending is None:
        return RegistryRejected("REGISTRY_STATE_CONFLICT")
    if complete.handoff_id != pending.handoff_id:
        return RegistryRejected("HANDOFF_PROOF_INVALID")
    proof = complete.proof
    if (
        proof.source_session_id != pending.source_session_id
        or proof.target != pending.target
        or proof.handoff_id != pending.handoff_id
        or proof.generation != pending.generation
    ):
        return RegistryRejected("HANDOFF_PROOF_INVALID")
    return _accept(
        complete,
        (
            Deactivated(proof),
            Activated(
                pending.target,
                pending.generation,
                ActivationKind.HANDOFF,
                pending.handoff_id,
            ),
        ),
    )


def _validate_event_envelope(value: object) -> CommittedRegistryEvent:
    if type(value) is not CommittedRegistryEvent:
        raise ValueError("INVALID_REGISTRY_EVENT")
    event = value
    if (
        not _valid_id(event.event_id)
        or not _valid_digest(event.event_digest)
        or not _valid_positive_int(event.registry_sequence)
        or not _valid_nonnegative_int(event.generation)
        or not _valid_id(event.command_id)
        or not _valid_digest(event.body_digest)
        or not _valid_payload(event.payload)
    ):
        raise ValueError("INVALID_REGISTRY_EVENT")
    return event


def _exact_receipt_match(
    receipt: CommandReceipt, transaction: CommittedRegistryTransaction
) -> bool:
    events = transaction.events
    return (
        receipt.transaction_id == transaction.transaction_id
        and receipt.transaction_digest == transaction.transaction_digest
        and receipt.first_registry_sequence == events[0].registry_sequence
        and receipt.last_registry_sequence == events[-1].registry_sequence
        and receipt.event_ids == tuple(item.event_id for item in events)
        and receipt.event_digests == tuple(item.event_digest for item in events)
    )


def _is_replay(
    state: RegistryState, transaction: CommittedRegistryTransaction
) -> bool:
    first = transaction.events[0]
    receipt = _receipt_for_command(state, first.command_id)
    if receipt is not None:
        if receipt.body_digest != first.body_digest:
            raise ValueError("IDEMPOTENCY_CONFLICT")
        if _exact_receipt_match(receipt, transaction):
            return True
        raise ValueError("REGISTRY_INTEGRITY_ERROR")
    if any(
        receipt.transaction_id == transaction.transaction_id
        or receipt.transaction_digest == transaction.transaction_digest
        for receipt in state.receipts
    ):
        raise ValueError("REGISTRY_INTEGRITY_ERROR")
    return False


def _validate_event_generation(
    state: RegistryState, event: CommittedRegistryEvent
) -> None:
    payload = event.payload
    if type(payload) is DialogueCreated:
        expected = state.generation
    elif type(payload) is DeactivationStarted:
        expected = payload.handoff.generation
        if expected != state.generation + 1:
            raise ValueError("REGISTRY_GENERATION_CONFLICT")
    elif type(payload) is Deactivated:
        expected = payload.proof.generation
        if expected != state.generation:
            raise ValueError("REGISTRY_GENERATION_CONFLICT")
    else:
        activated = cast(Activated, payload)
        expected = activated.generation
        expected_from_state = (
            state.generation + 1
            if activated.activation_kind is ActivationKind.INITIAL
            else state.generation
        )
        if expected != expected_from_state:
            raise ValueError("REGISTRY_GENERATION_CONFLICT")
    if event.generation != expected:
        raise ValueError("REGISTRY_GENERATION_CONFLICT")


def _validate_transaction_shape(
    state: RegistryState, events: tuple[CommittedRegistryEvent, ...]
) -> None:
    payload_types = tuple(type(item.payload) for item in events)
    if payload_types == (DialogueCreated, Activated):
        created = cast(DialogueCreated, events[0].payload)
        activated = cast(Activated, events[1].payload)
        if (
            activated.activation_kind is not ActivationKind.INITIAL
            or activated.target != created.target
            or activated.generation != state.generation + 1
            or state.current_session_id is not None
            or state.pending_handoff is not None
            or _dialogue_by_id(state.dialogues, created.target.session_id)
            is not None
        ):
            raise ValueError("INVALID_REGISTRY_TRANSACTION")
        return
    if payload_types == (DialogueCreated, DeactivationStarted):
        created = cast(DialogueCreated, events[0].payload)
        started = cast(DeactivationStarted, events[1].payload).handoff
        if (
            created.target != started.target
            or started.started_registry_sequence != events[-1].registry_sequence
            or _dialogue_by_id(state.dialogues, created.target.session_id)
            is not None
        ):
            raise ValueError("INVALID_REGISTRY_TRANSACTION")
        _validate_begin_context(state, started)
        return
    if payload_types == (DeactivationStarted,):
        started = cast(DeactivationStarted, events[0].payload).handoff
        target = _dialogue_by_id(state.dialogues, started.target.session_id)
        if (
            started.started_registry_sequence != events[0].registry_sequence
            or target is None
            or target.status is not DialogueRegistrationStatus.CREATED
            or target.config_digest != started.target.config_digest
        ):
            raise ValueError("INVALID_REGISTRY_TRANSACTION")
        _validate_begin_context(state, started)
        return
    if payload_types == (Deactivated, Activated):
        deactivated = cast(Deactivated, events[0].payload)
        activated = cast(Activated, events[1].payload)
        pending = state.pending_handoff
        if (
            pending is None
            or not _proof_matches_pending(deactivated.proof, pending)
            or activated.activation_kind is not ActivationKind.HANDOFF
            or activated.target != pending.target
            or activated.generation != pending.generation
            or activated.handoff_id != pending.handoff_id
        ):
            raise ValueError("INVALID_REGISTRY_TRANSACTION")
        return
    raise ValueError("INVALID_REGISTRY_TRANSACTION")


def _validate_begin_context(
    state: RegistryState, handoff: PendingHandoff
) -> None:
    source = _dialogue_by_id(state.dialogues, handoff.source_session_id)
    if (
        state.pending_handoff is not None
        or state.current_session_id != handoff.source_session_id
        or source is None
        or source.status is not DialogueRegistrationStatus.ACTIVE
        or handoff.generation != state.generation + 1
    ):
        raise ValueError("INVALID_REGISTRY_TRANSACTION")


def _proof_matches_pending(
    proof: QuiesceProof, pending: PendingHandoff
) -> bool:
    return (
        proof.source_session_id == pending.source_session_id
        and proof.target == pending.target
        and proof.handoff_id == pending.handoff_id
        and proof.generation == pending.generation
    )


def validate_transaction(
    state: RegistryState, transaction: CommittedRegistryTransaction
) -> None:
    """Validate one complete registry transaction against aggregate state."""
    _validate_state(state)
    if type(transaction) is not CommittedRegistryTransaction:
        raise ValueError("INVALID_REGISTRY_TRANSACTION")
    if (
        not _valid_id(transaction.transaction_id)
        or not _valid_digest(transaction.transaction_digest)
        or type(transaction.events) is not tuple
        or not 1 <= len(transaction.events) <= 2
    ):
        raise ValueError("INVALID_REGISTRY_TRANSACTION")

    events = tuple(_validate_event_envelope(item) for item in transaction.events)
    expected_sequence = state.registry_sequence + 1
    first_command = events[0].command_id
    first_body = events[0].body_digest
    if _receipt_for_command(state, first_command) is None:
        for event in events:
            if event.registry_sequence != expected_sequence:
                raise ValueError("REGISTRY_SEQUENCE_GAP")
            expected_sequence += 1
    if any(
        item.command_id != first_command or item.body_digest != first_body
        for item in events
    ):
        raise ValueError("INVALID_REGISTRY_TRANSACTION")
    if len({item.event_id for item in events}) != len(events):
        raise ValueError("REGISTRY_INTEGRITY_ERROR")
    if _is_replay(state, transaction):
        return
    prior_event_ids = {
        event_id for receipt in state.receipts for event_id in receipt.event_ids
    }
    if any(item.event_id in prior_event_ids for item in events):
        raise ValueError("REGISTRY_INTEGRITY_ERROR")
    for event in events:
        _validate_event_generation(state, event)
    _validate_transaction_shape(state, events)


def _replace_dialogue(
    dialogues: tuple[RegisteredDialogue, ...], updated: RegisteredDialogue
) -> tuple[RegisteredDialogue, ...]:
    return tuple(
        updated if item.session_id == updated.session_id else item
        for item in dialogues
    )


def _apply_event(
    state: RegistryState, event: CommittedRegistryEvent
) -> RegistryState:
    payload = event.payload
    if type(payload) is DialogueCreated:
        target = payload.target
        registered = RegisteredDialogue(
            target.session_id,
            target.config_digest,
            DialogueRegistrationStatus.CREATED,
            event.registry_sequence,
            None,
            None,
            None,
        )
        return replace(
            state,
            registry_sequence=event.registry_sequence,
            dialogues=(*state.dialogues, registered),
        )
    if type(payload) is DeactivationStarted:
        handoff = payload.handoff
        source = _dialogue_by_id(state.dialogues, handoff.source_session_id)
        if source is None:
            raise ValueError("INVALID_REGISTRY_TRANSACTION")
        source = replace(
            source,
            status=DialogueRegistrationStatus.DEACTIVATING,
        )
        return replace(
            state,
            registry_sequence=event.registry_sequence,
            generation=handoff.generation,
            dialogues=_replace_dialogue(state.dialogues, source),
            pending_handoff=handoff,
        )
    if type(payload) is Deactivated:
        proof = payload.proof
        source = _dialogue_by_id(state.dialogues, proof.source_session_id)
        if source is None:
            raise ValueError("INVALID_REGISTRY_TRANSACTION")
        source = replace(
            source,
            status=DialogueRegistrationStatus.DEACTIVATED,
            deactivated_generation=proof.generation,
            deactivation_handoff_id=proof.handoff_id,
        )
        return replace(
            state,
            registry_sequence=event.registry_sequence,
            current_session_id=None,
            dialogues=_replace_dialogue(state.dialogues, source),
        )

    activated = cast(Activated, payload)
    registered_target = _dialogue_by_id(
        state.dialogues, activated.target.session_id
    )
    if registered_target is None:
        raise ValueError("INVALID_REGISTRY_TRANSACTION")
    registered_target = replace(
        registered_target,
        status=DialogueRegistrationStatus.ACTIVE,
        activated_generation=activated.generation,
    )
    return replace(
        state,
        registry_sequence=event.registry_sequence,
        generation=activated.generation,
        current_session_id=activated.target.session_id,
        dialogues=_replace_dialogue(state.dialogues, registered_target),
        pending_handoff=None,
    )


def reduce(
    state: RegistryState, transaction: CommittedRegistryTransaction
) -> RegistryState:
    """Apply a validated atomic transaction, or return state for exact replay."""
    validate_transaction(state, transaction)
    first = transaction.events[0]
    if _receipt_for_command(state, first.command_id) is not None:
        return state

    reduced = state
    for event in transaction.events:
        reduced = _apply_event(reduced, event)
    receipt = CommandReceipt(
        first.command_id,
        first.body_digest,
        transaction.transaction_id,
        transaction.transaction_digest,
        transaction.events[0].registry_sequence,
        transaction.events[-1].registry_sequence,
        tuple(item.event_id for item in transaction.events),
        tuple(item.event_digest for item in transaction.events),
    )
    reduced = replace(reduced, receipts=(*reduced.receipts, receipt))
    return _validate_state(reduced)
