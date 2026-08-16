"""Typed, bounded incremental context for one successfully claimed Host work."""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from typing import cast

from .domain import (
    GateAssessment,
    GateId,
    GateRequirement,
    GateStatus,
    InsightKind,
    InsightProvenance,
    InsightStatus,
    LearnerModelEntry,
    Lens,
    TaskScope,
    TopicContract,
    TriggerKind,
)
from .event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    canonical_json_bytes,
    sha256_digest,
)
from .work import RunnableWork, WorkError, validate_runnable_work
from .work_identity import is_protocol_id, is_sha256_digest


MAX_HOST_CONTEXT_BYTES = 16 * 1024
_MAX_TEXT_BYTES = 64 * 1024


class HostContextError(RuntimeError):
    """A stable failure while constructing a Host context capsule."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class EvidenceContextClaim:
    """One directly relevant evidence claim and its reproducible locator."""

    evidence_id: str
    claim: str
    location: str
    content_hash: str


@dataclass(frozen=True, slots=True)
class LearnerTurnContext:
    """Latest learner text, with proof and a focused read path if truncated."""

    text: str
    read_ref: str | None = None
    truncated: bool = False
    original_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class HostContextSource:
    """Typed current-state material supplied by a trusted focused reader.

    Ordering is semantic priority: the builder keeps at most the first six
    learner-model entries and first three evidence claims.
    """

    topic_contract: TopicContract | None
    task_scope: str
    current_lens: Lens | None
    gates: tuple[GateAssessment, ...]
    previous_question: str | None
    learner_turn: LearnerTurnContext | None
    learner_model: tuple[LearnerModelEntry, ...]
    priority_gap: str | None
    evidence_claims: tuple[EvidenceContextClaim, ...]
    through_event_sequence: int
    selected_candidate: str | None = None


@dataclass(frozen=True, slots=True)
class HostContextCapsule:
    """Canonical incremental model input bound to one immutable work item."""

    work_id: str
    binding_digest: str
    evidence_digest: str
    topic_contract: TopicContract | None
    task_scope: str
    current_lens: Lens | None
    gates: tuple[GateAssessment, ...]
    previous_question: str | None
    learner_turn: LearnerTurnContext | None
    learner_model: tuple[LearnerModelEntry, ...]
    priority_gap: str | None
    evidence_claims: tuple[EvidenceContextClaim, ...]
    learner_model_digest: str
    through_event_sequence: int
    context_digest: str
    selected_candidate: str | None = None


def _nonblank(value: object, *, max_bytes: int = _MAX_TEXT_BYTES) -> bool:
    if type(value) is not str or not value or value != value.strip():
        return False
    try:
        return len(value.encode("utf-8")) <= max_bytes
    except UnicodeError:
        return False


def _valid_optional_text(value: object) -> bool:
    return value is None or _nonblank(value)


def _domain_text(value: object) -> bool:
    return type(value) is str and bool(value.strip())


def _valid_domain_text_tuple(value: object) -> bool:
    return type(value) is tuple and all(_domain_text(item) for item in value)


def _valid_contract(value: object) -> bool:
    if type(value) is not TopicContract or type(value.task_scope) is not TaskScope:
        return False
    contract = value
    scope = contract.task_scope
    if (
        not is_protocol_id(contract.contract_id)
        or type(contract.contract_version) is not int
        or contract.contract_version < 1
        or not is_protocol_id(contract.topic_run_id)
        or not _domain_text(contract.title)
        or not _domain_text(contract.guiding_question)
        or not _domain_text(contract.objective)
        or not is_protocol_id(scope.task_id)
        or not _domain_text(scope.summary)
        or not _valid_domain_text_tuple(scope.included_paths)
        or not _valid_domain_text_tuple(scope.excluded_paths)
        or type(contract.starting_lens) is not Lens
        or type(contract.bridge_required) is not bool
        or not _valid_domain_text_tuple(contract.evidence_refs)
        or len(contract.evidence_refs) != len(set(contract.evidence_refs))
        or type(contract.gates) is not tuple
        or any(type(item) is not GateRequirement for item in contract.gates)
        or any(
            type(item.gate_id) is not GateId or type(item.required) is not bool
            for item in contract.gates
        )
        or len(contract.gates) != 3
        or frozenset(item.gate_id for item in contract.gates)
        != frozenset(
            {GateId.MECHANISM, GateId.BOUNDARY, GateId.REPOSITORY_APPLICATION}
        )
        or any(not item.required for item in contract.gates)
        or (
            contract.supersedes_contract_id is not None
            and not is_protocol_id(contract.supersedes_contract_id)
        )
        or not is_sha256_digest(contract.contract_digest)
    ):
        return False
    return contract.bridge_required and bool(contract.evidence_refs)


def _valid_gate(value: object) -> bool:
    return (
        type(value) is GateAssessment
        and type(value.gate_id) is GateId
        and type(value.status) is GateStatus
        and _valid_domain_text_tuple(value.source_turn_ids)
        and _valid_domain_text_tuple(value.evidence_refs)
        and (
            value.status is not GateStatus.SUPPORTED
            or (bool(value.source_turn_ids) and bool(value.evidence_refs))
        )
    )


def _valid_model(value: object) -> bool:
    return (
        type(value) is LearnerModelEntry
        and _domain_text(value.entry_id)
        and type(value.kind) is InsightKind
        and type(value.status) is InsightStatus
        and type(value.provenance) is InsightProvenance
        and _domain_text(value.statement)
        and _valid_domain_text_tuple(value.source_turn_ids)
        and _valid_domain_text_tuple(value.evidence_refs)
        and (
            value.provenance is not InsightProvenance.AGENT_INFERRED
            or value.status is InsightStatus.WORKING_MODEL
        )
        and (
            value.status is not InsightStatus.CONFIRMED
            or (
                value.provenance
                in {
                    InsightProvenance.LEARNER_EXPLICIT,
                    InsightProvenance.JOINTLY_CONFIRMED,
                }
                and bool(value.source_turn_ids)
                and bool(value.evidence_refs)
            )
        )
    )


def _validate_evidence_claim(value: object) -> EvidenceContextClaim:
    if (
        type(value) is not EvidenceContextClaim
        or not is_protocol_id(value.evidence_id)
        or not _nonblank(value.claim)
        or not _nonblank(value.location, max_bytes=4096)
        or not is_sha256_digest(value.content_hash)
    ):
        raise HostContextError("HOST_CONTEXT_SOURCE_INVALID")
    return value


def _validate_learner_turn(value: object) -> LearnerTurnContext:
    if type(value) is not LearnerTurnContext or type(value.text) is not str:
        raise HostContextError("HOST_CONTEXT_SOURCE_INVALID")
    try:
        value.text.encode("utf-8")
    except UnicodeError as exc:
        raise HostContextError("HOST_CONTEXT_SOURCE_INVALID") from exc
    if (
        not _valid_optional_text(value.read_ref)
        or type(value.truncated) is not bool
        or (
            value.original_sha256 is not None
            and not is_sha256_digest(value.original_sha256)
        )
    ):
        raise HostContextError("HOST_CONTEXT_SOURCE_INVALID")
    return value


def _validate_source(value: object) -> HostContextSource:
    if (
        type(value) is not HostContextSource
        or (
            value.topic_contract is not None
            and not _valid_contract(value.topic_contract)
        )
        or not _nonblank(value.task_scope, max_bytes=16 * 1024)
        or (value.current_lens is not None and type(value.current_lens) is not Lens)
        or type(value.gates) is not tuple
        or any(not _valid_gate(item) for item in value.gates)
        or not _valid_optional_text(value.previous_question)
        or (
            value.learner_turn is not None
            and type(value.learner_turn) is not LearnerTurnContext
        )
        or type(value.learner_model) is not tuple
        or any(not _valid_model(item) for item in value.learner_model)
        or not _valid_optional_text(value.priority_gap)
        or not _valid_optional_text(value.selected_candidate)
        or type(value.evidence_claims) is not tuple
        or not value.evidence_claims
        or type(value.through_event_sequence) is not int
        or value.through_event_sequence < 1
    ):
        raise HostContextError("HOST_CONTEXT_SOURCE_INVALID")
    if value.learner_turn is not None:
        turn = _validate_learner_turn(value.learner_turn)
        if turn.truncated or turn.original_sha256 is not None:
            raise HostContextError("HOST_CONTEXT_SOURCE_INVALID")
    for claim in value.evidence_claims:
        _validate_evidence_claim(claim)
    if value.topic_contract is None:
        if value.current_lens is None or value.gates or value.learner_model:
            raise HostContextError("HOST_CONTEXT_SOURCE_INVALID")
    else:
        contract_gate_ids = frozenset(
            item.gate_id for item in value.topic_contract.gates
        )
        if (
            value.current_lens is None
            or len(value.gates) != len(contract_gate_ids)
            or frozenset(item.gate_id for item in value.gates)
            != contract_gate_ids
            or any(
                not set(item.evidence_refs).issubset(
                    value.topic_contract.evidence_refs
                )
                for item in value.gates
            )
            or any(
                not set(item.evidence_refs).issubset(
                    value.topic_contract.evidence_refs
                )
                for item in value.learner_model
            )
        ):
            raise HostContextError("HOST_CONTEXT_SOURCE_INVALID")
    return value


def _to_tree(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _to_tree(getattr(value, item.name))
            for item in fields(value)
        }
    if type(value) is tuple:
        return [_to_tree(item) for item in value]
    if value is None or type(value) in {str, int, bool}:
        return value
    raise HostContextError("HOST_CONTEXT_SOURCE_INVALID")


def _capsule_tree(
    capsule: HostContextCapsule,
    *,
    include_digest: bool,
) -> dict[str, object]:
    tree = cast(dict[str, object], _to_tree(capsule))
    if not include_digest:
        tree.pop("context_digest")
    return {
        "schema_version": SCHEMA_VERSION,
        "record_type": "host_context_capsule",
        "protocol_version": PROTOCOL_VERSION,
        **tree,
    }


def _with_digest(
    work: RunnableWork,
    source: HostContextSource,
    learner_turn: LearnerTurnContext | None,
) -> HostContextCapsule:
    model = source.learner_model[:6]
    evidence = source.evidence_claims[:3]
    model_digest = sha256_digest(canonical_json_bytes(_to_tree(model)))
    provisional = HostContextCapsule(
        work_id=work.work_id,
        binding_digest=work.binding_digest,
        evidence_digest=work.evidence_digest,
        topic_contract=source.topic_contract,
        task_scope=source.task_scope,
        current_lens=source.current_lens,
        gates=source.gates,
        previous_question=source.previous_question,
        learner_turn=learner_turn,
        learner_model=model,
        priority_gap=source.priority_gap,
        evidence_claims=evidence,
        learner_model_digest=model_digest,
        through_event_sequence=source.through_event_sequence,
        context_digest="",
        selected_candidate=source.selected_candidate,
    )
    digest = sha256_digest(
        canonical_json_bytes(_capsule_tree(provisional, include_digest=False))
    )
    return HostContextCapsule(
        work_id=provisional.work_id,
        binding_digest=provisional.binding_digest,
        evidence_digest=provisional.evidence_digest,
        topic_contract=provisional.topic_contract,
        task_scope=provisional.task_scope,
        current_lens=provisional.current_lens,
        gates=provisional.gates,
        previous_question=provisional.previous_question,
        learner_turn=provisional.learner_turn,
        learner_model=provisional.learner_model,
        priority_gap=provisional.priority_gap,
        evidence_claims=provisional.evidence_claims,
        learner_model_digest=provisional.learner_model_digest,
        through_event_sequence=provisional.through_event_sequence,
        context_digest=digest,
        selected_candidate=provisional.selected_candidate,
    )


def _encode_unchecked(capsule: HostContextCapsule) -> bytes:
    return canonical_json_bytes(_capsule_tree(capsule, include_digest=True))


def build_host_context(
    work: RunnableWork,
    source: HostContextSource,
) -> HostContextCapsule:
    """Build a canonical capsule, truncating only recoverable learner text."""
    try:
        work = validate_runnable_work(work)
    except WorkError as exc:
        raise HostContextError(exc.code) from exc
    source = _validate_source(source)
    if (
        source.through_event_sequence != work.observed_sequence
        or (source.topic_contract is None) != (work.contract_digest is None)
        or (
            source.topic_contract is not None
            and source.topic_contract.contract_digest != work.contract_digest
        )
        or (
            source.topic_contract is not None
            and source.task_scope != source.topic_contract.task_scope.summary
        )
        or (
            (work.kind is TriggerKind.TOPIC_SELECTION)
            != (source.selected_candidate is not None)
        )
    ):
        raise HostContextError("HOST_CONTEXT_WORK_MISMATCH")
    learner_turn = source.learner_turn
    capsule = _with_digest(work, source, learner_turn)
    if len(_encode_unchecked(capsule)) <= MAX_HOST_CONTEXT_BYTES:
        return capsule
    if learner_turn is None or not learner_turn.text:
        raise HostContextError("HOST_CONTEXT_TOO_LARGE")
    if learner_turn.read_ref is None:
        raise HostContextError("HOST_CONTEXT_READ_REF_REQUIRED")

    original_hash = sha256_digest(learner_turn.text.encode("utf-8"))
    low = 0
    high = len(learner_turn.text)
    fitted: HostContextCapsule | None = None
    while low <= high:
        midpoint = (low + high) // 2
        truncated = LearnerTurnContext(
            learner_turn.text[:midpoint],
            learner_turn.read_ref,
            True,
            original_hash,
        )
        candidate = _with_digest(work, source, truncated)
        if len(_encode_unchecked(candidate)) <= MAX_HOST_CONTEXT_BYTES:
            fitted = candidate
            low = midpoint + 1
        else:
            high = midpoint - 1
    if fitted is None:
        raise HostContextError("HOST_CONTEXT_TOO_LARGE")
    return fitted


def encode_host_context(capsule: HostContextCapsule) -> bytes:
    """Encode and revalidate one exact canonical capsule under 16 KiB."""
    if type(capsule) is not HostContextCapsule:
        raise HostContextError("HOST_CONTEXT_INVALID")
    try:
        source = HostContextSource(
            capsule.topic_contract,
            capsule.task_scope,
            capsule.current_lens,
            capsule.gates,
            capsule.previous_question,
            None,
            capsule.learner_model,
            capsule.priority_gap,
            capsule.evidence_claims,
            capsule.through_event_sequence,
            capsule.selected_candidate,
        )
        _validate_source(source)
        if (
            not is_protocol_id(capsule.work_id)
            or not is_sha256_digest(capsule.binding_digest)
            or not is_sha256_digest(capsule.evidence_digest)
            or len(capsule.learner_model) > 6
            or len(capsule.evidence_claims) > 3
            or (
                capsule.topic_contract is not None
                and capsule.task_scope
                != capsule.topic_contract.task_scope.summary
            )
            or capsule.learner_model_digest
            != sha256_digest(canonical_json_bytes(_to_tree(capsule.learner_model)))
        ):
            raise HostContextError("HOST_CONTEXT_INVALID")
        if capsule.learner_turn is not None:
            turn = _validate_learner_turn(capsule.learner_turn)
            if turn.truncated:
                if turn.read_ref is None or turn.original_sha256 is None:
                    raise HostContextError("HOST_CONTEXT_INVALID")
            elif turn.original_sha256 is not None:
                raise HostContextError("HOST_CONTEXT_INVALID")
    except HostContextError as exc:
        if exc.code == "HOST_CONTEXT_INVALID":
            raise
        raise HostContextError("HOST_CONTEXT_INVALID") from exc
    expected = sha256_digest(
        canonical_json_bytes(_capsule_tree(capsule, include_digest=False))
    )
    if capsule.context_digest != expected:
        raise HostContextError("HOST_CONTEXT_INVALID")
    encoded = _encode_unchecked(capsule)
    if len(encoded) > MAX_HOST_CONTEXT_BYTES:
        raise HostContextError("HOST_CONTEXT_TOO_LARGE")
    return encoded
