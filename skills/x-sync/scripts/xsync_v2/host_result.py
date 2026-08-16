"""Strict, model-neutral JSON boundary for semantic Host results.

Supported Hosts may use different tool transports, but all must emit this
same closed result union.  Decoding never mutates dialogue state; the
returned immutable value is mapped to an existing domain command and still
passes through HostWorkService, the state machine, evidence checks, and lease
fencing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias, cast

from .domain import (
    AgentTurnResult,
    CommitAgentTurn,
    PresentCandidates,
    ReportWorkFailure,
    RequestTopicClarification,
    StartTopic,
    TopicContract,
    TriggerKind,
    WorkFailure,
    WorkFailureCategory,
)
from .event_codec import (
    PROTOCOL_VERSION,
    canonical_json_bytes,
    decode_host_domain_value,
    encode_host_domain_value,
    sha256_digest,
)
from .work import RunnableWork, WorkError, validate_runnable_work
from .work_identity import is_protocol_id, is_sha256_digest

MAX_HOST_RESULT_BYTES = 64 * 1024
_MAX_TEXT_BYTES = 32 * 1024
_DIALOGUE_WORK_KINDS = frozenset(
    {
        TriggerKind.INITIAL_TURN,
        TriggerKind.HELP,
        TriggerKind.LEARNER_REPLY,
        TriggerKind.LENS_CHANGED,
        TriggerKind.REGROUND,
    }
)


class HostResultError(RuntimeError):
    """Stable Host result syntax, schema, or work-binding failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class HostResultKind(StrEnum):
    """Closed wire tags shared by every Host adapter."""

    TOPIC_CANDIDATES = "topic_candidates"
    TOPIC_STARTED = "topic_started"
    TOPIC_CLARIFICATION = "topic_clarification"
    DIALOGUE_TURN = "dialogue_turn"
    WORK_FAILURE = "work_failure"


@dataclass(frozen=True, slots=True)
class TopicCandidatesResult:
    """Up to four repository-grounded candidate labels."""

    candidates: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TopicStartedResult:
    """One complete Topic Contract for the durable learner selection."""

    contract: TopicContract


@dataclass(frozen=True, slots=True)
class TopicClarificationResult:
    """One bounded question needed before building the Topic Contract."""

    question_id: str
    question: str


@dataclass(frozen=True, slots=True)
class DialogueTurnResult:
    """One adaptive visible Agent turn and structured model/gate updates."""

    turn: AgentTurnResult


@dataclass(frozen=True, slots=True)
class WorkFailureResult:
    """One sanitized Host processing failure without exception text."""

    category: WorkFailureCategory
    safe_error_code: str
    proof_digest: str


HostResult: TypeAlias = (
    TopicCandidatesResult
    | TopicStartedResult
    | TopicClarificationResult
    | DialogueTurnResult
    | WorkFailureResult
)
HostResultCommand: TypeAlias = (
    PresentCandidates
    | StartTopic
    | RequestTopicClarification
    | CommitAgentTurn
    | ReportWorkFailure
)


def host_command_id(idempotency_key: str) -> str:
    """Derive the sole Host command id for one stable idempotency key."""
    if not is_protocol_id(idempotency_key):
        raise HostResultError("INVALID_IDEMPOTENCY_KEY")
    digest = sha256_digest(
        canonical_json_bytes(
            {
                "protocol_version": PROTOCOL_VERSION,
                "record_type": "host_command_identity",
                "idempotency_key": idempotency_key,
            }
        )
    )
    command_id = f"host.command.{digest.removeprefix('sha256:')[:48]}"
    if not is_protocol_id(command_id):
        raise HostResultError("IDENTITY_DERIVATION_FAILED")
    return command_id


def _object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise HostResultError("HOST_RESULT_DUPLICATE_KEY")
        result[key] = value
    return result


def _load(raw: bytes) -> dict[str, object]:
    if type(raw) is not bytes or not raw or len(raw) > MAX_HOST_RESULT_BYTES:
        raise HostResultError("HOST_RESULT_INVALID")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_object_pairs)
    except HostResultError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise HostResultError("HOST_RESULT_INVALID") from exc
    if type(value) is not dict:
        raise HostResultError("HOST_RESULT_INVALID")
    return value


def _keys(value: dict[str, object], expected: frozenset[str]) -> None:
    if frozenset(value) != expected:
        raise HostResultError("HOST_RESULT_SCHEMA_INVALID")


def _candidate(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise HostResultError("HOST_RESULT_SCHEMA_INVALID")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise HostResultError("HOST_RESULT_SCHEMA_INVALID") from exc
    if size > _MAX_TEXT_BYTES:
        raise HostResultError("HOST_RESULT_SCHEMA_INVALID")
    return value


def decode_host_result(raw: bytes) -> HostResult:
    """Decode exact JSON into the closed immutable Host result union."""
    value = _load(raw)
    kind_value = value.get("type")
    try:
        kind = HostResultKind(cast(str, kind_value))
    except (TypeError, ValueError) as exc:
        raise HostResultError("HOST_RESULT_TYPE_UNSUPPORTED") from exc
    try:
        if kind is HostResultKind.TOPIC_CANDIDATES:
            _keys(value, frozenset({"type", "candidates"}))
            raw_candidates = value["candidates"]
            if type(raw_candidates) is not list:
                raise HostResultError("HOST_RESULT_SCHEMA_INVALID")
            candidates = tuple(_candidate(item) for item in raw_candidates)
            if not 1 <= len(candidates) <= 4 or len(candidates) != len(
                set(candidates)
            ):
                raise HostResultError("HOST_RESULT_SCHEMA_INVALID")
            return TopicCandidatesResult(candidates)
        if kind is HostResultKind.TOPIC_STARTED:
            _keys(value, frozenset({"type", "contract"}))
            contract = decode_host_domain_value(value["contract"], TopicContract)
            return TopicStartedResult(cast(TopicContract, contract))
        if kind is HostResultKind.TOPIC_CLARIFICATION:
            _keys(value, frozenset({"type", "question_id", "question"}))
            question_id = value["question_id"]
            question = value["question"]
            if not is_protocol_id(question_id) or type(question) is not str:
                raise HostResultError("HOST_RESULT_SCHEMA_INVALID")
            return TopicClarificationResult(
                cast(str, question_id),
                _candidate(question),
            )
        if kind is HostResultKind.DIALOGUE_TURN:
            _keys(value, frozenset({"type", "turn"}))
            turn = decode_host_domain_value(value["turn"], AgentTurnResult)
            return DialogueTurnResult(cast(AgentTurnResult, turn))
        _keys(
            value,
            frozenset(
                {"type", "category", "safe_error_code", "proof_digest"}
            ),
        )
        category = WorkFailureCategory(cast(str, value["category"]))
        safe_error_code = value["safe_error_code"]
        proof_digest = value["proof_digest"]
        if (
            category is WorkFailureCategory.LEASE_ATTEMPTS_EXHAUSTED
            or not is_protocol_id(safe_error_code)
            or not is_sha256_digest(proof_digest)
        ):
            raise HostResultError("HOST_RESULT_SCHEMA_INVALID")
        return WorkFailureResult(
            category,
            cast(str, safe_error_code),
            cast(str, proof_digest),
        )
    except HostResultError:
        raise
    except (TypeError, ValueError, RecursionError) as exc:
        raise HostResultError("HOST_RESULT_SCHEMA_INVALID") from exc


def _tree(result: HostResult) -> dict[str, object]:
    if type(result) is TopicCandidatesResult:
        return {
            "type": HostResultKind.TOPIC_CANDIDATES.value,
            "candidates": list(result.candidates),
        }
    if type(result) is TopicStartedResult:
        return {
            "type": HostResultKind.TOPIC_STARTED.value,
            "contract": encode_host_domain_value(result.contract),
        }
    if type(result) is TopicClarificationResult:
        return {
            "type": HostResultKind.TOPIC_CLARIFICATION.value,
            "question_id": result.question_id,
            "question": result.question,
        }
    if type(result) is DialogueTurnResult:
        return {
            "type": HostResultKind.DIALOGUE_TURN.value,
            "turn": encode_host_domain_value(result.turn),
        }
    if type(result) is WorkFailureResult:
        return {
            "type": HostResultKind.WORK_FAILURE.value,
            "category": result.category.value,
            "safe_error_code": result.safe_error_code,
            "proof_digest": result.proof_digest,
        }
    raise HostResultError("HOST_RESULT_INVALID")


def encode_host_result(result: HostResult) -> bytes:
    """Encode and self-validate one canonical Host result JSON record."""
    try:
        raw = canonical_json_bytes(_tree(result))
    except (TypeError, ValueError, RecursionError) as exc:
        raise HostResultError("HOST_RESULT_INVALID") from exc
    if len(raw) > MAX_HOST_RESULT_BYTES:
        raise HostResultError("HOST_RESULT_INVALID")
    decoded = decode_host_result(raw)
    if decoded != result:
        raise HostResultError("HOST_RESULT_INVALID")
    return raw


def _failure_id(idempotency_key: str, result: WorkFailureResult) -> str:
    digest = sha256_digest(
        canonical_json_bytes(
            {
                "idempotency_key": idempotency_key,
                "record_type": "host_failure_identity",
                "result": _tree(result),
            }
        )
    )
    return f"failure.host.{digest.removeprefix('sha256:')[:48]}"


def host_result_command(
    result: HostResult,
    *,
    idempotency_key: str,
    work: RunnableWork,
    selected_candidate: str | None = None,
) -> HostResultCommand:
    """Map a validated result to one command for the matching durable work."""
    try:
        work = validate_runnable_work(work)
        command_id = host_command_id(idempotency_key)
    except (HostResultError, WorkError, ValueError) as exc:
        code = getattr(exc, "code", "HOST_RESULT_WORK_MISMATCH")
        raise HostResultError(code) from exc
    if type(result) is TopicCandidatesResult:
        if work.kind is not TriggerKind.TOPIC_CANDIDATES:
            raise HostResultError("HOST_RESULT_WORK_MISMATCH")
        return PresentCandidates(command_id, result.candidates)
    if type(result) is TopicStartedResult:
        if (
            work.kind is not TriggerKind.TOPIC_SELECTION
            or type(selected_candidate) is not str
            or not selected_candidate.strip()
        ):
            raise HostResultError("HOST_RESULT_WORK_MISMATCH")
        return StartTopic(command_id, result.contract, selected_candidate)
    if type(result) is TopicClarificationResult:
        if (
            work.kind is not TriggerKind.TOPIC_SELECTION
            or type(selected_candidate) is not str
            or not selected_candidate.strip()
        ):
            raise HostResultError("HOST_RESULT_WORK_MISMATCH")
        return RequestTopicClarification(
            command_id,
            result.question_id,
            result.question,
        )
    if type(result) is DialogueTurnResult:
        if work.kind not in _DIALOGUE_WORK_KINDS:
            raise HostResultError("HOST_RESULT_WORK_MISMATCH")
        return CommitAgentTurn(command_id, result.turn)
    if type(result) is WorkFailureResult:
        return ReportWorkFailure(
            command_id,
            work.work_id,
            WorkFailure(
                _failure_id(idempotency_key, result),
                result.category,
                result.safe_error_code,
                result.proof_digest,
            ),
        )
    raise HostResultError("HOST_RESULT_INVALID")


__all__ = [
    "MAX_HOST_RESULT_BYTES",
    "DialogueTurnResult",
    "HostResult",
    "HostResultError",
    "HostResultKind",
    "TopicCandidatesResult",
    "TopicClarificationResult",
    "TopicStartedResult",
    "WorkFailureResult",
    "decode_host_result",
    "encode_host_result",
    "host_command_id",
    "host_result_command",
]
