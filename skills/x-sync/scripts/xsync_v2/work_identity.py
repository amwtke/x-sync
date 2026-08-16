"""Pure canonical identity helpers shared by dialogue and work projections."""

from __future__ import annotations

import hashlib
import json
import re

from .domain import TriggerBinding, TriggerKind


_PROTOCOL_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256_DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")


class WorkIdentityError(ValueError):
    """A stable failure while deriving canonical work identity."""

    def __init__(self, code: str = "WORK_ID_INPUT_INVALID") -> None:
        self.code = code
        super().__init__(code)


def is_protocol_id(value: object) -> bool:
    """Return whether ``value`` is one bounded, path-safe protocol id."""
    return (
        type(value) is str
        and _PROTOCOL_ID_PATTERN.fullmatch(value) is not None
    )


def is_optional_protocol_id(value: object) -> bool:
    """Return whether ``value`` is absent or one canonical protocol id."""
    return value is None or is_protocol_id(value)


def is_sha256_digest(value: object) -> bool:
    """Return whether ``value`` is one canonical lowercase SHA-256 digest."""
    return (
        type(value) is str
        and _SHA256_DIGEST_PATTERN.fullmatch(value) is not None
    )


def is_optional_sha256_digest(value: object) -> bool:
    """Return whether ``value`` is absent or one canonical SHA-256 digest."""
    return value is None or is_sha256_digest(value)


def is_canonical_trigger_binding(value: object) -> bool:
    """Validate the exact wire-safe shape shared by every work boundary."""
    return (
        type(value) is TriggerBinding
        and type(value.kind) is TriggerKind
        and is_protocol_id(value.work_id)
        and is_protocol_id(value.runtime_epoch)
        and is_optional_protocol_id(value.parent_turn_id)
        and is_optional_sha256_digest(value.contract_digest)
        and is_sha256_digest(value.input_digest)
        and is_sha256_digest(value.evidence_digest)
    )


def derive_canonical_work_id(
    *,
    session_id: str,
    trigger_event_id: str,
    trigger_event_sequence: int,
    trigger: TriggerBinding,
) -> str:
    """Derive a work id from exactly the protocol fields in spec section 8.3."""
    if (
        not is_protocol_id(session_id)
        or not is_protocol_id(trigger_event_id)
        or type(trigger_event_sequence) is not int
        or trigger_event_sequence < 1
        or not is_canonical_trigger_binding(trigger)
    ):
        raise WorkIdentityError()
    identity = {
        "contract_digest": trigger.contract_digest,
        "evidence_digest": trigger.evidence_digest,
        "input_digest": trigger.input_digest,
        "kind": trigger.kind.value,
        "session_id": session_id,
        "trigger_event_id": trigger_event_id,
        "trigger_event_sequence": trigger_event_sequence,
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"work-{hashlib.sha256(encoded).hexdigest()}"
