"""Marker-linearized Host leases for canonical X-Sync work.

Each lease mutation is one immutable, hash-linked transaction file.  That
file is simultaneously the commit point and the durable idempotency receipt;
there is no mutable receipt array that can fill up or split from ``current``.
The current lease is always replayed from the chain.

Public operations require an already-held registry -> Session authority.  A
trusted loader reads the authoritative registry/dialogue tips under that
authority and supplies the committed event origin for the current work.  A
separate trusted runtime-owner port proves Supervisor liveness and is the only
way a new runtime epoch may fence an unexpired prior owner.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum
import hashlib
import json
import re
import threading
from typing import cast

from .domain import CurrentWorkState, DialogueState, TriggerKind, WorkStatus
from .event_codec import PROTOCOL_VERSION, SCHEMA_VERSION
from .event_store import session_directory_component
from .locking import DomainLockManager, LockError, SessionLockAuthority
from .registry import (
    DialogueRegistrationStatus,
    RegisteredDialogue,
    RegistryState,
)
from .secure_fs import SecureDirectory, SecureFsError
from .work import (
    RunnableWork,
    WorkError,
    WorkOrigin,
    derive_runnable_work,
    validate_runnable_work,
)


# A Supervisor injects one OS-monotonic tick source per runtime epoch.  A new
# process/time base must use a new epoch and enter through reclaim, never renew.
Clock = Callable[[], int]

_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_HASH_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
_TRANSACTION_NAME = re.compile(r"transaction-([0-9]{20})\.json\Z")
_MAX_TRANSACTION_BYTES = 64 * 1024
_MAX_LEASE_SECONDS = 3600
_MAX_TENURE_SECONDS = 24 * 60 * 60
_MAX_RENEWALS_PER_TENURE = 256
_MAX_RECLAIMS_PER_WORK = 2
_NAMESPACE_FILE = "lease-lock-namespace.json"

_NAMESPACE_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "protocol_version",
        "lock_device",
        "lock_inode",
    }
)
_TRANSACTION_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "protocol_version",
        "transaction_sequence",
        "operation",
        "request_id",
        "request_digest",
        "request",
        "recorded_at",
        "previous_transaction_hash",
        "work",
        "lease",
        "transaction_hash",
    }
)
_WORK_KEYS = frozenset(
    {
        "session_id",
        "registry_generation",
        "conversation_version",
        "observed_sequence",
        "trigger_event_id",
        "trigger_event_sequence",
        "topic_run_id",
        "kind",
        "trigger_work_id",
        "work_id",
        "trigger_runtime_epoch",
        "parent_turn_id",
        "contract_digest",
        "input_digest",
        "evidence_digest",
        "binding_digest",
    }
)
_LEASE_KEYS = frozenset(
    {
        "session_id",
        "work_id",
        "binding_digest",
        "claim_id",
        "owner_id",
        "runtime_epoch",
        "lease_version",
        "registry_generation",
        "acquired_at",
        "renewed_at",
        "expires_at",
        "absolute_expires_at",
    }
)
_CLAIM_REQUEST_KEYS = frozenset(
    {
        "session_id",
        "request_id",
        "claim_id",
        "work_id",
        "owner_id",
        "lease_seconds",
        "max_tenure_seconds",
    }
)
_RENEW_REQUEST_KEYS = frozenset(
    {
        "session_id",
        "request_id",
        "claim_id",
        "work_id",
        "owner_id",
        "expected_lease_version",
        "lease_seconds",
    }
)
_RECLAIM_REQUEST_KEYS = frozenset(
    {
        "session_id",
        "request_id",
        "claim_id",
        "work_id",
        "owner_id",
        "expected_work_attempt",
        "lease_seconds",
        "max_tenure_seconds",
    }
)


class LeaseStoreError(RuntimeError):
    """A stable, path-free work coordination failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class LeaseOperation(StrEnum):
    CLAIM = "claim"
    RENEW = "renew"
    RECLAIM = "reclaim"


@dataclass(frozen=True, slots=True)
class ClaimRequest:
    """Idempotent request for the first tenure of one canonical work item."""

    session_id: str
    request_id: str
    claim_id: str
    work_id: str
    owner_id: str
    lease_seconds: int
    max_tenure_seconds: int


@dataclass(frozen=True, slots=True)
class RenewRequest:
    """Idempotent compare-and-renew request for the current tenure."""

    session_id: str
    request_id: str
    claim_id: str
    work_id: str
    owner_id: str
    expected_lease_version: int
    lease_seconds: int


@dataclass(frozen=True, slots=True)
class ReclaimRequest:
    """Idempotent request for a fresh tenure after expiry or owner death."""

    session_id: str
    request_id: str
    claim_id: str
    work_id: str
    owner_id: str
    expected_work_attempt: int
    lease_seconds: int
    max_tenure_seconds: int


LeaseRequest = ClaimRequest | RenewRequest | ReclaimRequest


@dataclass(frozen=True, slots=True)
class PublishFence:
    """Complete lease identity required at a Host result commit boundary."""

    session_id: str
    claim_id: str
    work_id: str
    owner_id: str
    runtime_epoch: str
    lease_version: int
    registry_generation: int


@dataclass(frozen=True, slots=True)
class LeaseRecord:
    """Durable current tenure reconstructed from immutable transactions."""

    session_id: str
    work_id: str
    binding_digest: str
    claim_id: str
    owner_id: str
    runtime_epoch: str
    lease_version: int
    registry_generation: int
    acquired_at: int
    renewed_at: int
    expires_at: int
    absolute_expires_at: int


@dataclass(frozen=True, slots=True)
class LeaseMutationOutcome:
    """A committed mutation result or the exact durable receipt replay."""

    operation: LeaseOperation
    lease: LeaseRecord
    replayed: bool


@dataclass(frozen=True, slots=True)
class LeaseExhaustionProof:
    """Recomputable proof that one canonical work exhausted all tenures.

    This is not a lease mutation or a semantic failure record.  The Runtime
    must hand it to ``HostWorkService`` so the dialogue state machine can
    atomically requeue or dead-letter the work.
    """

    session_id: str
    work_id: str
    binding_digest: str
    work_attempt: int
    reclaim_count: int
    last_lease_version: int
    history_digest: str
    requesting_runtime_epoch: str
    requesting_owner_id: str
    reclaim_request_id: str
    reclaim_request_digest: str


ReclaimOutcome = LeaseMutationOutcome | LeaseExhaustionProof


@dataclass(frozen=True, slots=True)
class CurrentRunnableWork:
    """Authoritative runnable work paired with its canonical attempt."""

    work: RunnableWork
    attempt: int


@dataclass(frozen=True, slots=True)
class CurrentWorkObservation:
    """One authoritative work projection at a durable dialogue sequence."""

    through_sequence: int
    current: CurrentRunnableWork | None


@dataclass(frozen=True, slots=True)
class AuthoritativeWorkSnapshot:
    """Fresh aggregate tips plus event-log-proven trigger origin."""

    dialogue_state: DialogueState
    registry_state: RegistryState
    work_origin: WorkOrigin | None


@dataclass(frozen=True, slots=True)
class RuntimeAuthorityCheck:
    """One query to the model-free Supervisor owner/liveness authority.

    When ``prior_owner_must_be_inactive`` is true, returning true means the
    port proved through its repo-local OS lifetime fence that the prior owner
    can no longer renew or publish.  Merely presenting a different epoch string
    is never sufficient.

    The trusted implementation must serialize epoch replacement through the
    same repository registry lock held by ``SessionLockAuthority``.  Therefore
    a successful check cannot become false before the immutable transaction
    publish while that registry -> Session authority remains live.
    """

    operation: LeaseOperation | None
    runtime_epoch: str
    owner_id: str
    prior_runtime_epoch: str | None
    prior_owner_id: str | None
    prior_owner_must_be_inactive: bool


AuthoritativeSnapshotLoader = Callable[
    [str, SessionLockAuthority], AuthoritativeWorkSnapshot
]
RuntimeAuthorityVerifier = Callable[
    [RuntimeAuthorityCheck, SessionLockAuthority], bool
]
_LeaseMarkerGuard = Callable[[SessionLockAuthority], LeaseRecord]


@dataclass(frozen=True, slots=True)
class _LeaseTransaction:
    transaction_sequence: int
    operation: LeaseOperation
    request_id: str
    request_digest: str
    request: LeaseRequest
    recorded_at: int
    previous_transaction_hash: str | None
    work: RunnableWork
    lease: LeaseRecord
    transaction_hash: str


def _valid_id(value: object) -> bool:
    return type(value) is str and _ID_PATTERN.fullmatch(value) is not None


def _valid_hash(value: object) -> bool:
    return type(value) is str and _HASH_PATTERN.fullmatch(value) is not None


def _valid_optional_hash(value: object) -> bool:
    return value is None or _valid_hash(value)


def _valid_positive_int(value: object) -> bool:
    return type(value) is int and value >= 1


def _valid_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _valid_duration(value: object) -> bool:
    return _valid_positive_int(value) and cast(int, value) <= _MAX_LEASE_SECONDS


def _valid_tenure(value: object, lease_seconds: object) -> bool:
    return (
        _valid_positive_int(value)
        and cast(int, value) <= _MAX_TENURE_SECONDS
        and type(lease_seconds) is int
        and cast(int, value) >= lease_seconds
    )


def _canonical_bytes(value: object) -> bytes:
    try:
        text = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, RecursionError) as exc:
        raise LeaseStoreError("LEASE_RECORD_INVALID") from exc
    return f"{text}\n".encode()


def _hash(value: object) -> str:
    return f"sha256:{hashlib.sha256(_canonical_bytes(value)).hexdigest()}"


def _validate_exhaustion_proof(value: object) -> LeaseExhaustionProof:
    if type(value) is not LeaseExhaustionProof:
        raise LeaseStoreError("LEASE_EXHAUSTION_PROOF_INVALID")
    proof = value
    if (
        not _valid_id(proof.session_id)
        or not _valid_id(proof.work_id)
        or not _valid_hash(proof.binding_digest)
        or not _valid_positive_int(proof.work_attempt)
        or proof.reclaim_count != _MAX_RECLAIMS_PER_WORK
        or not _valid_positive_int(proof.last_lease_version)
        or not _valid_hash(proof.history_digest)
        or not _valid_id(proof.requesting_runtime_epoch)
        or not _valid_id(proof.requesting_owner_id)
        or not _valid_id(proof.reclaim_request_id)
        or not _valid_hash(proof.reclaim_request_digest)
    ):
        raise LeaseStoreError("LEASE_EXHAUSTION_PROOF_INVALID")
    return proof


def _exhaustion_proof_tree(proof: LeaseExhaustionProof) -> dict[str, object]:
    proof = _validate_exhaustion_proof(proof)
    return {
        "schema_version": SCHEMA_VERSION,
        "record_type": "lease_exhaustion_proof",
        "protocol_version": PROTOCOL_VERSION,
        "session_id": proof.session_id,
        "work_id": proof.work_id,
        "binding_digest": proof.binding_digest,
        "work_attempt": proof.work_attempt,
        "reclaim_count": proof.reclaim_count,
        "last_lease_version": proof.last_lease_version,
        "history_digest": proof.history_digest,
        "requesting_runtime_epoch": proof.requesting_runtime_epoch,
        "requesting_owner_id": proof.requesting_owner_id,
        "reclaim_request_id": proof.reclaim_request_id,
        "reclaim_request_digest": proof.reclaim_request_digest,
    }


def _exhaustion_proof_digest(proof: LeaseExhaustionProof) -> str:
    return _hash(_exhaustion_proof_tree(proof))


def _canonical_work_attempt(
    state: DialogueState,
    work: RunnableWork,
) -> int:
    topic_work = (
        state.active_topic.work if state.active_topic is not None else None
    )
    candidates = tuple(
        item
        for item in (state.session_work, topic_work)
        if type(item) is CurrentWorkState and item.status is WorkStatus.QUEUED
    )
    if len(candidates) != 1:
        raise LeaseStoreError("AUTHORITATIVE_WORK_INVALID")
    current = candidates[0]
    if (
        current.work_id != work.work_id
        or current.trigger_event_id != work.trigger_event_id
        or current.trigger_event_sequence != work.trigger_event_sequence
    ):
        raise LeaseStoreError("AUTHORITATIVE_WORK_INVALID")
    return current.attempt


def _require_mapping(
    value: object, keys: frozenset[str]
) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    return cast(dict[str, object], value)


def _work_tree(work: RunnableWork) -> dict[str, object]:
    validate_runnable_work(work)
    return {
        "session_id": work.session_id,
        "registry_generation": work.registry_generation,
        "conversation_version": work.conversation_version,
        "observed_sequence": work.observed_sequence,
        "trigger_event_id": work.trigger_event_id,
        "trigger_event_sequence": work.trigger_event_sequence,
        "topic_run_id": work.topic_run_id,
        "kind": work.kind.value,
        "trigger_work_id": work.trigger_work_id,
        "work_id": work.work_id,
        "trigger_runtime_epoch": work.trigger_runtime_epoch,
        "parent_turn_id": work.parent_turn_id,
        "contract_digest": work.contract_digest,
        "input_digest": work.input_digest,
        "evidence_digest": work.evidence_digest,
        "binding_digest": work.binding_digest,
    }


def _decode_work(value: object) -> RunnableWork:
    item = _require_mapping(value, _WORK_KEYS)
    raw_kind = item["kind"]
    if type(raw_kind) is not str:
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    try:
        kind = TriggerKind(raw_kind)
        work = RunnableWork(
            session_id=cast(str, item["session_id"]),
            registry_generation=cast(int, item["registry_generation"]),
            conversation_version=cast(int, item["conversation_version"]),
            observed_sequence=cast(int, item["observed_sequence"]),
            trigger_event_id=cast(str, item["trigger_event_id"]),
            trigger_event_sequence=cast(int, item["trigger_event_sequence"]),
            topic_run_id=cast(str | None, item["topic_run_id"]),
            kind=kind,
            trigger_work_id=cast(str, item["trigger_work_id"]),
            work_id=cast(str, item["work_id"]),
            trigger_runtime_epoch=cast(str, item["trigger_runtime_epoch"]),
            parent_turn_id=cast(str | None, item["parent_turn_id"]),
            contract_digest=cast(str | None, item["contract_digest"]),
            input_digest=cast(str, item["input_digest"]),
            evidence_digest=cast(str, item["evidence_digest"]),
            binding_digest=cast(str, item["binding_digest"]),
        )
        return validate_runnable_work(work)
    except (TypeError, ValueError, WorkError) as exc:
        raise LeaseStoreError("LEASE_RECORD_INVALID") from exc


def _lease_tree(lease: LeaseRecord) -> dict[str, object]:
    _validate_lease(lease)
    return {
        "session_id": lease.session_id,
        "work_id": lease.work_id,
        "binding_digest": lease.binding_digest,
        "claim_id": lease.claim_id,
        "owner_id": lease.owner_id,
        "runtime_epoch": lease.runtime_epoch,
        "lease_version": lease.lease_version,
        "registry_generation": lease.registry_generation,
        "acquired_at": lease.acquired_at,
        "renewed_at": lease.renewed_at,
        "expires_at": lease.expires_at,
        "absolute_expires_at": lease.absolute_expires_at,
    }


def _validate_lease(value: object) -> LeaseRecord:
    if type(value) is not LeaseRecord:
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    lease = value
    if (
        not _valid_id(lease.session_id)
        or not _valid_id(lease.work_id)
        or not _valid_hash(lease.binding_digest)
        or not _valid_id(lease.claim_id)
        or not _valid_id(lease.owner_id)
        or not _valid_id(lease.runtime_epoch)
        or not _valid_positive_int(lease.lease_version)
        or not _valid_positive_int(lease.registry_generation)
        or not _valid_nonnegative_int(lease.acquired_at)
        or not _valid_nonnegative_int(lease.renewed_at)
        or not _valid_positive_int(lease.expires_at)
        or not _valid_positive_int(lease.absolute_expires_at)
        or lease.acquired_at > lease.renewed_at
        or lease.renewed_at >= lease.expires_at
        or lease.expires_at > lease.absolute_expires_at
    ):
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    return lease


def _decode_lease(value: object) -> LeaseRecord:
    item = _require_mapping(value, _LEASE_KEYS)
    return _validate_lease(
        LeaseRecord(
            session_id=cast(str, item["session_id"]),
            work_id=cast(str, item["work_id"]),
            binding_digest=cast(str, item["binding_digest"]),
            claim_id=cast(str, item["claim_id"]),
            owner_id=cast(str, item["owner_id"]),
            runtime_epoch=cast(str, item["runtime_epoch"]),
            lease_version=cast(int, item["lease_version"]),
            registry_generation=cast(int, item["registry_generation"]),
            acquired_at=cast(int, item["acquired_at"]),
            renewed_at=cast(int, item["renewed_at"]),
            expires_at=cast(int, item["expires_at"]),
            absolute_expires_at=cast(int, item["absolute_expires_at"]),
        )
    )


def _transaction_tree(
    transaction: _LeaseTransaction, *, include_hash: bool
) -> dict[str, object]:
    tree: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "lease_transaction",
        "protocol_version": PROTOCOL_VERSION,
        "transaction_sequence": transaction.transaction_sequence,
        "operation": transaction.operation.value,
        "request_id": transaction.request_id,
        "request_digest": transaction.request_digest,
        "request": _request_tree(transaction.operation, transaction.request),
        "recorded_at": transaction.recorded_at,
        "previous_transaction_hash": transaction.previous_transaction_hash,
        "work": _work_tree(transaction.work),
        "lease": _lease_tree(transaction.lease),
    }
    if include_hash:
        tree["transaction_hash"] = transaction.transaction_hash
    return tree


def _transaction_hash(transaction: _LeaseTransaction) -> str:
    return _hash(_transaction_tree(transaction, include_hash=False))


def _encode_transaction(transaction: _LeaseTransaction) -> bytes:
    if _transaction_hash(transaction) != transaction.transaction_hash:
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    encoded = _canonical_bytes(_transaction_tree(transaction, include_hash=True))
    if len(encoded) > _MAX_TRANSACTION_BYTES:
        raise LeaseStoreError("LEASE_RECORD_TOO_LARGE")
    if _decode_transaction(encoded) != transaction:
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    return encoded


def _decode_transaction(data: bytes) -> _LeaseTransaction:
    if type(data) is not bytes:
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    try:
        tree = json.loads(data.decode())
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise LeaseStoreError("LEASE_RECORD_INVALID") from exc
    item = _require_mapping(tree, _TRANSACTION_KEYS)
    raw_operation = item["operation"]
    if type(raw_operation) is not str:
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    try:
        operation = LeaseOperation(raw_operation)
    except ValueError as exc:
        raise LeaseStoreError("LEASE_RECORD_INVALID") from exc
    transaction = _LeaseTransaction(
        transaction_sequence=cast(int, item["transaction_sequence"]),
        operation=operation,
        request_id=cast(str, item["request_id"]),
        request_digest=cast(str, item["request_digest"]),
        request=_decode_request(operation, item["request"]),
        recorded_at=cast(int, item["recorded_at"]),
        previous_transaction_hash=cast(
            str | None, item["previous_transaction_hash"]
        ),
        work=_decode_work(item["work"]),
        lease=_decode_lease(item["lease"]),
        transaction_hash=cast(str, item["transaction_hash"]),
    )
    if (
        item["schema_version"] != SCHEMA_VERSION
        or item["record_type"] != "lease_transaction"
        or item["protocol_version"] != PROTOCOL_VERSION
        or not _valid_positive_int(transaction.transaction_sequence)
        or not _valid_id(transaction.request_id)
        or not _valid_hash(transaction.request_digest)
        or transaction.request_id != transaction.request.request_id
        or transaction.request_digest
        != _request_digest(transaction.operation, transaction.request)
        or not _valid_nonnegative_int(transaction.recorded_at)
        or not _valid_optional_hash(transaction.previous_transaction_hash)
        or not _valid_hash(transaction.transaction_hash)
        or _transaction_hash(transaction) != transaction.transaction_hash
    ):
        raise LeaseStoreError("LEASE_RECORD_INVALID")
    if _canonical_bytes(_transaction_tree(transaction, include_hash=True)) != data:
        raise LeaseStoreError("LEASE_RECORD_NON_CANONICAL")
    return transaction


def _same_claim(left: LeaseRecord, right: LeaseRecord) -> bool:
    return (
        left.session_id == right.session_id
        and left.work_id == right.work_id
        and left.binding_digest == right.binding_digest
        and left.claim_id == right.claim_id
        and left.owner_id == right.owner_id
        and left.runtime_epoch == right.runtime_epoch
        and left.registry_generation == right.registry_generation
        and left.acquired_at == right.acquired_at
        and left.absolute_expires_at == right.absolute_expires_at
    )


def _validate_history(
    history: tuple[_LeaseTransaction, ...],
) -> tuple[_LeaseTransaction, ...]:
    if not history:
        return history
    request_ids: set[str] = set()
    claim_ids: set[str] = set()
    owner_ids: set[str] = set()
    renewals_in_tenure = 0
    reclaim_count = 0
    for index, transaction in enumerate(history, start=1):
        lease = transaction.lease
        request = transaction.request
        if (
            transaction.transaction_sequence != index
            or lease.lease_version != index
            or transaction.request_id in request_ids
            or lease.session_id != transaction.work.session_id
            or lease.work_id != transaction.work.work_id
            or lease.binding_digest != transaction.work.binding_digest
            or lease.registry_generation != transaction.work.registry_generation
            or request.session_id != lease.session_id
            or request.work_id != lease.work_id
            or request.claim_id != lease.claim_id
            or request.owner_id != lease.owner_id
            or (
                index == 1
                and (
                    transaction.operation is not LeaseOperation.CLAIM
                    or type(request) is not ClaimRequest
                    or transaction.previous_transaction_hash is not None
                    or lease.acquired_at != transaction.recorded_at
                    or lease.renewed_at != transaction.recorded_at
                    or lease.expires_at
                    != transaction.recorded_at + request.lease_seconds
                    or lease.absolute_expires_at
                    != transaction.recorded_at + request.max_tenure_seconds
                )
            )
        ):
            raise LeaseStoreError("LEASE_HISTORY_INVALID")
        request_ids.add(transaction.request_id)
        if index == 1:
            claim_ids.add(lease.claim_id)
            owner_ids.add(lease.owner_id)
            continue
        previous_transaction = history[index - 2]
        previous = previous_transaction.lease
        if (
            transaction.previous_transaction_hash
            != previous_transaction.transaction_hash
            or transaction.work.binding_digest
            != previous_transaction.work.binding_digest
            or transaction.work.observed_sequence
            < previous_transaction.work.observed_sequence
            or (
                transaction.operation is not LeaseOperation.RECLAIM
                and transaction.recorded_at
                < previous_transaction.recorded_at
            )
            or (
                transaction.operation is LeaseOperation.RECLAIM
                and lease.runtime_epoch == previous.runtime_epoch
                and transaction.recorded_at
                < previous_transaction.recorded_at
            )
        ):
            raise LeaseStoreError("LEASE_HISTORY_INVALID")
        if transaction.operation is LeaseOperation.RENEW:
            renewals_in_tenure += 1
            if (
                type(request) is not RenewRequest
                or renewals_in_tenure > _MAX_RENEWALS_PER_TENURE
                or not _same_claim(previous, lease)
                or request.expected_lease_version != previous.lease_version
                or transaction.recorded_at >= previous.expires_at
                or transaction.recorded_at >= previous.absolute_expires_at
                or lease.renewed_at != transaction.recorded_at
                or lease.expires_at <= transaction.recorded_at
                or lease.expires_at <= previous.expires_at
                or lease.expires_at > lease.absolute_expires_at
                or lease.expires_at
                != min(
                    transaction.recorded_at + request.lease_seconds,
                    lease.absolute_expires_at,
                )
            ):
                raise LeaseStoreError("LEASE_HISTORY_INVALID")
        elif transaction.operation is LeaseOperation.RECLAIM:
            renewals_in_tenure = 0
            reclaim_count += 1
            if (
                type(request) is not ReclaimRequest
                or reclaim_count > _MAX_RECLAIMS_PER_WORK
                or lease.claim_id in claim_ids
                or lease.owner_id in owner_ids
                or lease.acquired_at != transaction.recorded_at
                or lease.renewed_at != transaction.recorded_at
                or lease.expires_at
                != transaction.recorded_at + request.lease_seconds
                or lease.absolute_expires_at
                != transaction.recorded_at + request.max_tenure_seconds
                or (
                    transaction.recorded_at < previous.expires_at
                    and lease.runtime_epoch == previous.runtime_epoch
                )
            ):
                raise LeaseStoreError("LEASE_HISTORY_INVALID")
            claim_ids.add(lease.claim_id)
            owner_ids.add(lease.owner_id)
        else:
            raise LeaseStoreError("LEASE_HISTORY_INVALID")
    return history


def _history_digest(history: tuple[_LeaseTransaction, ...]) -> str:
    _validate_history(history)
    return _hash(
        {
            "schema_version": SCHEMA_VERSION,
            "record_type": "lease_exhaustion_history",
            "protocol_version": PROTOCOL_VERSION,
            "transaction_hashes": tuple(
                transaction.transaction_hash for transaction in history
            ),
        }
    )


def _request_tree(
    operation: LeaseOperation, request: LeaseRequest
) -> dict[str, object]:
    if operation is LeaseOperation.CLAIM:
        claim = _validate_claim_request(request)
        return {
            "session_id": claim.session_id,
            "request_id": claim.request_id,
            "claim_id": claim.claim_id,
            "work_id": claim.work_id,
            "owner_id": claim.owner_id,
            "lease_seconds": claim.lease_seconds,
            "max_tenure_seconds": claim.max_tenure_seconds,
        }
    if operation is LeaseOperation.RECLAIM:
        reclaim = _validate_reclaim_request(request)
        return {
            "session_id": reclaim.session_id,
            "request_id": reclaim.request_id,
            "claim_id": reclaim.claim_id,
            "work_id": reclaim.work_id,
            "owner_id": reclaim.owner_id,
            "expected_work_attempt": reclaim.expected_work_attempt,
            "lease_seconds": reclaim.lease_seconds,
            "max_tenure_seconds": reclaim.max_tenure_seconds,
        }
    renew = _validate_renew_request(request)
    return {
        "session_id": renew.session_id,
        "request_id": renew.request_id,
        "claim_id": renew.claim_id,
        "work_id": renew.work_id,
        "owner_id": renew.owner_id,
        "expected_lease_version": renew.expected_lease_version,
        "lease_seconds": renew.lease_seconds,
    }


def _decode_request(
    operation: LeaseOperation, value: object
) -> LeaseRequest:
    if operation is LeaseOperation.CLAIM:
        item = _require_mapping(value, _CLAIM_REQUEST_KEYS)
        return _validate_claim_request(
            ClaimRequest(
                cast(str, item["session_id"]),
                cast(str, item["request_id"]),
                cast(str, item["claim_id"]),
                cast(str, item["work_id"]),
                cast(str, item["owner_id"]),
                cast(int, item["lease_seconds"]),
                cast(int, item["max_tenure_seconds"]),
            )
        )
    if operation is LeaseOperation.RECLAIM:
        item = _require_mapping(value, _RECLAIM_REQUEST_KEYS)
        return _validate_reclaim_request(
            ReclaimRequest(
                cast(str, item["session_id"]),
                cast(str, item["request_id"]),
                cast(str, item["claim_id"]),
                cast(str, item["work_id"]),
                cast(str, item["owner_id"]),
                cast(int, item["expected_work_attempt"]),
                cast(int, item["lease_seconds"]),
                cast(int, item["max_tenure_seconds"]),
            )
        )
    item = _require_mapping(value, _RENEW_REQUEST_KEYS)
    return _validate_renew_request(
        RenewRequest(
            cast(str, item["session_id"]),
            cast(str, item["request_id"]),
            cast(str, item["claim_id"]),
            cast(str, item["work_id"]),
            cast(str, item["owner_id"]),
            cast(int, item["expected_lease_version"]),
            cast(int, item["lease_seconds"]),
        )
    )


def _request_digest(operation: LeaseOperation, request: object) -> str:
    if not isinstance(request, (ClaimRequest, RenewRequest, ReclaimRequest)):
        raise LeaseStoreError("INVALID_LEASE_REQUEST")
    return _hash(
        {
            "operation": operation.value,
            **_request_tree(operation, request),
        }
    )


def _validate_claim_request(request: object) -> ClaimRequest:
    if type(request) is not ClaimRequest:
        raise LeaseStoreError("INVALID_CLAIM_REQUEST")
    if (
        not _valid_id(request.session_id)
        or not _valid_id(request.request_id)
        or not _valid_id(request.claim_id)
        or not _valid_id(request.work_id)
        or not _valid_id(request.owner_id)
        or not _valid_duration(request.lease_seconds)
        or not _valid_tenure(
            request.max_tenure_seconds, request.lease_seconds
        )
    ):
        raise LeaseStoreError("INVALID_CLAIM_REQUEST")
    return request


def _validate_reclaim_request(request: object) -> ReclaimRequest:
    if type(request) is not ReclaimRequest:
        raise LeaseStoreError("INVALID_RECLAIM_REQUEST")
    if (
        not _valid_id(request.session_id)
        or not _valid_id(request.request_id)
        or not _valid_id(request.claim_id)
        or not _valid_id(request.work_id)
        or not _valid_id(request.owner_id)
        or not _valid_positive_int(request.expected_work_attempt)
        or not _valid_duration(request.lease_seconds)
        or not _valid_tenure(
            request.max_tenure_seconds, request.lease_seconds
        )
    ):
        raise LeaseStoreError("INVALID_RECLAIM_REQUEST")
    return request


def _validate_renew_request(request: object) -> RenewRequest:
    if type(request) is not RenewRequest:
        raise LeaseStoreError("INVALID_RENEW_REQUEST")
    if (
        not _valid_id(request.session_id)
        or not _valid_id(request.request_id)
        or not _valid_id(request.claim_id)
        or not _valid_id(request.work_id)
        or not _valid_id(request.owner_id)
        or not _valid_positive_int(request.expected_lease_version)
        or not _valid_duration(request.lease_seconds)
    ):
        raise LeaseStoreError("INVALID_RENEW_REQUEST")
    return request


def _validate_publish_fence(fence: object) -> PublishFence:
    if type(fence) is not PublishFence:
        raise LeaseStoreError("INVALID_PUBLISH_FENCE")
    if (
        not _valid_id(fence.session_id)
        or not _valid_id(fence.claim_id)
        or not _valid_id(fence.work_id)
        or not _valid_id(fence.owner_id)
        or not _valid_id(fence.runtime_epoch)
        or not _valid_positive_int(fence.lease_version)
        or not _valid_positive_int(fence.registry_generation)
    ):
        raise LeaseStoreError("INVALID_PUBLISH_FENCE")
    return fence


class LeaseStore:
    """Coordinate Host work through immutable lease transactions.

    Heartbeats and reclaim tenures are bounded.  Exhausting two reclaims
    yields a recomputable proof rather than a raw retry-limit error; the
    Host-work application service turns that proof into the canonical
    failed/requeued or failed/dead-lettered dialogue transaction.  Publish
    fencing remains private until the event-store marker accepts a
    same-instruction pre-marker guard.
    """

    def __init__(
        self,
        dialogues_directory: SecureDirectory,
        locks: DomainLockManager,
        runtime_epoch: str,
        clock: Clock,
        *,
        snapshot_loader: AuthoritativeSnapshotLoader,
        runtime_authority_verifier: RuntimeAuthorityVerifier,
    ) -> None:
        if (
            type(dialogues_directory) is not SecureDirectory
            or type(locks) is not DomainLockManager
            or not _valid_id(runtime_epoch)
            or not callable(clock)
            or not callable(snapshot_loader)
            or not callable(runtime_authority_verifier)
        ):
            raise LeaseStoreError("INVALID_LEASE_STORE_CONFIGURATION")
        self._dialogues = dialogues_directory
        self._locks = locks
        self._runtime_epoch = runtime_epoch
        self._clock = clock
        self._snapshot_loader = snapshot_loader
        self._runtime_authority_verifier = runtime_authority_verifier
        self._clock_guard = threading.Lock()
        self._last_clock_value: int | None = None
        self._lock_namespace_identity = self._bind_lock_namespace()

    @property
    def runtime_epoch(self) -> str:
        """Return the epoch this store may claim, renew, and publish for."""
        return self._runtime_epoch

    def claim(
        self,
        request: ClaimRequest,
        authority: SessionLockAuthority,
    ) -> LeaseMutationOutcome:
        """Claim unleased current work and durably publish its first tenure."""
        request = _validate_claim_request(request)
        digest = _request_digest(LeaseOperation.CLAIM, request)
        replay = self._replay_receipt(
            request.session_id,
            request.work_id,
            request.request_id,
            digest,
            authority,
        )
        if replay is not None:
            return replay
        work = self._load_current_work(request.session_id, authority)
        if request.work_id != work.work_id:
            raise LeaseStoreError("WORK_SUPERSEDED")
        history = self._read_history(request.session_id, request.work_id)
        now = self._now()
        if history:
            if history[-1].work.binding_digest != work.binding_digest:
                raise LeaseStoreError("WORK_IDENTITY_CONFLICT")
            current = history[-1].lease
            if self._lease_is_active(current, work, now):
                raise LeaseStoreError("WORK_ALREADY_LEASED")
            raise LeaseStoreError("LEASE_RECLAIM_REQUIRED")
        runtime_check = RuntimeAuthorityCheck(
            LeaseOperation.CLAIM,
            self._runtime_epoch,
            request.owner_id,
            None,
            None,
            False,
        )
        self._verify_runtime(runtime_check, authority)
        return self._commit(
            request.session_id,
            history,
            LeaseOperation.CLAIM,
            request.request_id,
            digest,
            work,
            request,
            runtime_check,
            authority,
        )

    def renew(
        self,
        request: RenewRequest,
        authority: SessionLockAuthority,
    ) -> LeaseMutationOutcome:
        """Renew exactly the current claim while preserving absolute tenure."""
        request = _validate_renew_request(request)
        digest = _request_digest(LeaseOperation.RENEW, request)
        replay = self._replay_receipt(
            request.session_id,
            request.work_id,
            request.request_id,
            digest,
            authority,
        )
        if replay is not None:
            return replay
        work = self._load_current_work(request.session_id, authority)
        if request.work_id != work.work_id:
            raise LeaseStoreError("WORK_SUPERSEDED")
        history = self._read_history(request.session_id, request.work_id)
        if not history:
            raise LeaseStoreError("LEASE_NOT_FOUND")
        if self._current_tenure_renewals(history) >= _MAX_RENEWALS_PER_TENURE:
            raise LeaseStoreError("LEASE_RENEWAL_LIMIT")
        current = history[-1].lease
        if current.runtime_epoch != self._runtime_epoch:
            raise LeaseStoreError("LEASE_EPOCH_FENCED")
        self._assert_claim_binding(
            current,
            work,
            request.claim_id,
            request.owner_id,
            request.expected_lease_version,
        )
        now = self._now()
        if now < current.renewed_at:
            raise LeaseStoreError("CLOCK_ROLLED_BACK")
        if now >= current.absolute_expires_at:
            raise LeaseStoreError("LEASE_TENURE_EXPIRED")
        if now >= current.expires_at:
            raise LeaseStoreError("LEASE_EXPIRED")
        runtime_check = RuntimeAuthorityCheck(
            LeaseOperation.RENEW,
            current.runtime_epoch,
            current.owner_id,
            current.runtime_epoch,
            current.owner_id,
            False,
        )
        self._verify_runtime(runtime_check, authority)
        next_expiry = min(
            now + request.lease_seconds,
            current.absolute_expires_at,
        )
        if next_expiry <= current.expires_at:
            raise LeaseStoreError("LEASE_RENEWAL_NOT_EXTENDED")
        return self._commit(
            request.session_id,
            history,
            LeaseOperation.RENEW,
            request.request_id,
            digest,
            work,
            request,
            runtime_check,
            authority,
        )

    def reclaim(
        self,
        request: ReclaimRequest,
        authority: SessionLockAuthority,
    ) -> ReclaimOutcome:
        """Start a bounded tenure or return its exact exhaustion proof."""
        request = _validate_reclaim_request(request)
        digest = _request_digest(LeaseOperation.RECLAIM, request)
        replay = self._replay_receipt(
            request.session_id,
            request.work_id,
            request.request_id,
            digest,
            authority,
        )
        if replay is not None:
            return replay
        history = self._read_history(request.session_id, request.work_id)
        if not history:
            raise LeaseStoreError("LEASE_NOT_FOUND")
        reclaim_count = sum(
            transaction.operation is LeaseOperation.RECLAIM
            for transaction in history
        )
        if reclaim_count >= _MAX_RECLAIMS_PER_WORK:
            proof = self._exhaustion_proof(
                history[-1].work,
                request.expected_work_attempt,
                history,
                self._runtime_epoch,
                request.owner_id,
                request.request_id,
                digest,
            )
            try:
                current = self.current_runnable(request.session_id, authority)
            except LeaseStoreError as exc:
                if exc.code == "SESSION_DEACTIVATED":
                    return proof
                raise
            if current is None or current.work.work_id != request.work_id:
                return proof
            self._assert_reclaim_eligible(
                request,
                current,
                history,
                authority,
            )
            return proof
        current = self.current_runnable(request.session_id, authority)
        if current is None or current.work.work_id != request.work_id:
            raise LeaseStoreError("WORK_SUPERSEDED")
        runtime_check = self._assert_reclaim_eligible(
            request,
            current,
            history,
            authority,
        )
        return self._commit(
            request.session_id,
            history,
            LeaseOperation.RECLAIM,
            request.request_id,
            digest,
            current.work,
            request,
            runtime_check,
            authority,
        )

    def _assert_reclaim_eligible(
        self,
        request: ReclaimRequest,
        current_work: CurrentRunnableWork,
        history: tuple[_LeaseTransaction, ...],
        authority: SessionLockAuthority,
    ) -> RuntimeAuthorityCheck:
        if (
            request.work_id != current_work.work.work_id
            or request.expected_work_attempt != current_work.attempt
        ):
            raise LeaseStoreError("WORK_SUPERSEDED")
        current = history[-1].lease
        if current.binding_digest != current_work.work.binding_digest:
            raise LeaseStoreError("WORK_IDENTITY_CONFLICT")
        now = self._now()
        epoch_changed = current.runtime_epoch != self._runtime_epoch
        if not epoch_changed and now < current.renewed_at:
            raise LeaseStoreError("CLOCK_ROLLED_BACK")
        expired = now >= current.expires_at
        if not epoch_changed and not expired:
            raise LeaseStoreError("LEASE_STILL_ACTIVE")
        if any(
            transaction.lease.claim_id == request.claim_id
            for transaction in history
        ):
            raise LeaseStoreError("CLAIM_ID_REUSED")
        if any(
            transaction.lease.owner_id == request.owner_id
            for transaction in history
        ):
            raise LeaseStoreError("OWNER_ID_REUSED")
        runtime_check = RuntimeAuthorityCheck(
            LeaseOperation.RECLAIM,
            self._runtime_epoch,
            request.owner_id,
            current.runtime_epoch,
            current.owner_id,
            epoch_changed,
        )
        self._verify_runtime(runtime_check, authority)
        return runtime_check

    def _exhaustion_marker_guard(
        self,
        proof: LeaseExhaustionProof,
        authority: SessionLockAuthority,
    ) -> _LeaseMarkerGuard:
        """Capture work and return the final marker-time exhaustion fence."""
        proof = _validate_exhaustion_proof(proof)
        work, work_attempt = self._load_current_work_with_attempt(
            proof.session_id,
            authority,
        )
        self._assert_exhausted_for_work(
            proof,
            work,
            work_attempt,
            authority,
        )

        def guard(boundary_authority: SessionLockAuthority) -> LeaseRecord:
            if boundary_authority is not authority:
                raise LeaseStoreError("LOCK_AUTHORITY_REQUIRED")
            return self._assert_exhausted_for_work(
                proof,
                work,
                work_attempt,
                boundary_authority,
            )

        return guard

    def _assert_publishable(
        self,
        fence: PublishFence,
        authority: SessionLockAuthority,
    ) -> LeaseRecord:
        fence = _validate_publish_fence(fence)
        work = self._load_current_work(fence.session_id, authority)
        return self._assert_publishable_for_work(fence, work, authority)

    @staticmethod
    def _exhaustion_proof(
        work: RunnableWork,
        work_attempt: int,
        history: tuple[_LeaseTransaction, ...],
        requesting_runtime_epoch: str,
        requesting_owner_id: str,
        reclaim_request_id: str,
        request_digest: str,
    ) -> LeaseExhaustionProof:
        validate_runnable_work(work)
        _validate_history(history)
        if not history:
            raise LeaseStoreError("LEASE_NOT_FOUND")
        reclaim_count = sum(
            transaction.operation is LeaseOperation.RECLAIM
            for transaction in history
        )
        current = history[-1].lease
        if (
            reclaim_count != _MAX_RECLAIMS_PER_WORK
            or current.work_id != work.work_id
            or current.binding_digest != work.binding_digest
            or not _valid_id(requesting_runtime_epoch)
            or not _valid_id(requesting_owner_id)
            or not _valid_id(reclaim_request_id)
            or not _valid_hash(request_digest)
        ):
            raise LeaseStoreError("LEASE_RECLAIM_NOT_EXHAUSTED")
        return _validate_exhaustion_proof(
            LeaseExhaustionProof(
                session_id=work.session_id,
                work_id=work.work_id,
                binding_digest=work.binding_digest,
                work_attempt=work_attempt,
                reclaim_count=reclaim_count,
                last_lease_version=current.lease_version,
                history_digest=_history_digest(history),
                requesting_runtime_epoch=requesting_runtime_epoch,
                requesting_owner_id=requesting_owner_id,
                reclaim_request_id=reclaim_request_id,
                reclaim_request_digest=request_digest,
            )
        )

    def _assert_exhausted_for_work(
        self,
        proof: LeaseExhaustionProof,
        work: RunnableWork,
        work_attempt: int,
        authority: SessionLockAuthority,
    ) -> LeaseRecord:
        proof = _validate_exhaustion_proof(proof)
        try:
            validate_runnable_work(work)
        except WorkError as exc:
            raise LeaseStoreError(exc.code) from exc
        if proof.session_id != work.session_id or proof.work_id != work.work_id:
            raise LeaseStoreError("WORK_SUPERSEDED")
        self._assert_authority(proof.session_id, authority)
        history = self._read_history(proof.session_id, proof.work_id)
        expected = self._exhaustion_proof(
            work,
            work_attempt,
            history,
            proof.requesting_runtime_epoch,
            proof.requesting_owner_id,
            proof.reclaim_request_id,
            proof.reclaim_request_digest,
        )
        if proof != expected:
            raise LeaseStoreError("LEASE_EXHAUSTION_PROOF_INVALID")
        if proof.requesting_runtime_epoch != self._runtime_epoch:
            raise LeaseStoreError("LEASE_EXHAUSTION_PROOF_INVALID")
        current = history[-1].lease
        if any(
            transaction.lease.owner_id == proof.requesting_owner_id
            for transaction in history
        ):
            raise LeaseStoreError("LEASE_EXHAUSTION_PROOF_INVALID")
        epoch_changed = current.runtime_epoch != proof.requesting_runtime_epoch
        now = self._now()
        if not epoch_changed and now < current.renewed_at:
            raise LeaseStoreError("CLOCK_ROLLED_BACK")
        if not epoch_changed and now < current.expires_at:
            raise LeaseStoreError("LEASE_RECLAIM_NOT_EXHAUSTED")
        self._verify_runtime(
            RuntimeAuthorityCheck(
                LeaseOperation.RECLAIM,
                proof.requesting_runtime_epoch,
                proof.requesting_owner_id,
                current.runtime_epoch,
                current.owner_id,
                epoch_changed,
            ),
            authority,
        )
        return current

    def _marker_publication_guard(
        self,
        fence: PublishFence,
        authority: SessionLockAuthority,
    ) -> _LeaseMarkerGuard:
        """Capture canonical work, then return its final marker-time fence.

        Event records and the marker temporary file exist by the time the
        returned guard runs.  Reopening a strict read-only event log then
        would correctly see those entries as uncommitted, so the guard keeps
        the already-proven work while the same registry -> Session authority
        prevents any concurrent semantic mutation.
        """
        fence = _validate_publish_fence(fence)
        work = self._load_current_work(fence.session_id, authority)
        self._assert_publishable_for_work(fence, work, authority)

        def guard(boundary_authority: SessionLockAuthority) -> LeaseRecord:
            if boundary_authority is not authority:
                raise LeaseStoreError("LOCK_AUTHORITY_REQUIRED")
            return self._assert_publishable_for_work(
                fence,
                work,
                boundary_authority,
            )

        return guard

    def _assert_publishable_for_work(
        self,
        fence: PublishFence,
        work: RunnableWork,
        authority: SessionLockAuthority,
    ) -> LeaseRecord:
        fence = _validate_publish_fence(fence)
        try:
            validate_runnable_work(work)
        except WorkError as exc:
            raise LeaseStoreError(exc.code) from exc
        if fence.session_id != work.session_id:
            raise LeaseStoreError("WORK_SUPERSEDED")
        if fence.work_id != work.work_id:
            raise LeaseStoreError("WORK_SUPERSEDED")
        history = self._read_history(fence.session_id, fence.work_id)
        if not history:
            raise LeaseStoreError("LEASE_NOT_FOUND")
        current = history[-1].lease
        if (
            fence.claim_id != current.claim_id
            or fence.owner_id != current.owner_id
            or fence.runtime_epoch != current.runtime_epoch
            or current.runtime_epoch != self._runtime_epoch
            or fence.lease_version != current.lease_version
            or fence.registry_generation != work.registry_generation
            or current.registry_generation != work.registry_generation
            or current.binding_digest != work.binding_digest
        ):
            raise LeaseStoreError("LEASE_FENCED")
        now = self._now()
        if now < current.renewed_at:
            raise LeaseStoreError("CLOCK_ROLLED_BACK")
        if now >= current.absolute_expires_at:
            raise LeaseStoreError("LEASE_TENURE_EXPIRED")
        if now >= current.expires_at:
            raise LeaseStoreError("LEASE_EXPIRED")
        self._assert_authority(fence.session_id, authority)
        self._verify_runtime(
            RuntimeAuthorityCheck(
                None,
                current.runtime_epoch,
                current.owner_id,
                current.runtime_epoch,
                current.owner_id,
                False,
            ),
            authority,
        )
        return current

    def read(
        self,
        session_id: str,
        authority: SessionLockAuthority,
    ) -> LeaseRecord | None:
        """Replay the current canonical work's last durable lease, if any."""
        if not _valid_id(session_id):
            raise LeaseStoreError("INVALID_SESSION_ID")
        work = self._load_authoritative_work(session_id, authority, current=False)
        if work is None:
            return None
        history = self._read_history(session_id, work.work_id)
        if not history:
            return None
        if history[-1].work.binding_digest != work.binding_digest:
            raise LeaseStoreError("WORK_IDENTITY_CONFLICT")
        return history[-1].lease

    def current_work(
        self,
        session_id: str,
        authority: SessionLockAuthority,
    ) -> RunnableWork | None:
        """Return active authoritative work without exposing lease history."""
        current = self.current_runnable(session_id, authority)
        return current.work if current is not None else None

    def current_runnable(
        self,
        session_id: str,
        authority: SessionLockAuthority,
    ) -> CurrentRunnableWork | None:
        """Read active work and its attempt from one authoritative snapshot."""
        return self.observe_current_work(session_id, authority).current

    def observe_current_work(
        self,
        session_id: str,
        authority: SessionLockAuthority,
    ) -> CurrentWorkObservation:
        """Read the durable sequence and current work from one snapshot."""
        if not _valid_id(session_id):
            raise LeaseStoreError("INVALID_SESSION_ID")
        snapshot = self._load_snapshot(session_id, authority)
        work = self._derive_authoritative_work(
            session_id,
            snapshot,
            current=True,
        )
        if work is None:
            current = None
        else:
            current = CurrentRunnableWork(
                work,
                _canonical_work_attempt(snapshot.dialogue_state, work),
            )
        return CurrentWorkObservation(
            snapshot.dialogue_state.sequence,
            current,
        )

    def _bind_lock_namespace(self) -> tuple[int, int]:
        try:
            identity = self._locks.namespace_identity
            tree = {
                "schema_version": SCHEMA_VERSION,
                "record_type": "lease_lock_namespace",
                "protocol_version": PROTOCOL_VERSION,
                "lock_device": identity[0],
                "lock_inode": identity[1],
            }
            encoded = _canonical_bytes(tree)
            try:
                self._dialogues.write_immutable(_NAMESPACE_FILE, encoded)
            except SecureFsError as exc:
                if exc.code != "IMMUTABLE_EXISTS":
                    raise
            durable = self._dialogues.read_bytes(
                _NAMESPACE_FILE, max_bytes=4096
            )
            try:
                decoded = json.loads(durable.decode())
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise LeaseStoreError("LOCK_NAMESPACE_RECORD_INVALID") from exc
            item = _require_mapping(decoded, _NAMESPACE_KEYS)
            if durable != _canonical_bytes(item) or item != tree:
                raise LeaseStoreError("LOCK_NAMESPACE_MISMATCH")
            return identity
        except LeaseStoreError:
            raise
        except (LockError, SecureFsError, ValueError) as exc:
            raise LeaseStoreError(getattr(exc, "code", str(exc))) from exc

    def _assert_authority(
        self, session_id: str, authority: SessionLockAuthority
    ) -> None:
        try:
            if self._locks.namespace_identity != self._lock_namespace_identity:
                raise LeaseStoreError("LOCK_NAMESPACE_MISMATCH")
            self._locks.assert_session_authority(authority, session_id)
        except LeaseStoreError:
            raise
        except LockError as exc:
            raise LeaseStoreError(exc.code) from exc

    def _load_snapshot(
        self, session_id: str, authority: SessionLockAuthority
    ) -> AuthoritativeWorkSnapshot:
        self._assert_authority(session_id, authority)
        try:
            snapshot = self._snapshot_loader(session_id, authority)
        except Exception as exc:
            raise LeaseStoreError("AUTHORITATIVE_SNAPSHOT_FAILED") from exc
        if (
            type(snapshot) is not AuthoritativeWorkSnapshot
            or type(snapshot.dialogue_state) is not DialogueState
            or type(snapshot.registry_state) is not RegistryState
            or snapshot.dialogue_state.session_id != session_id
            or (
                snapshot.work_origin is not None
                and type(snapshot.work_origin) is not WorkOrigin
            )
        ):
            raise LeaseStoreError("AUTHORITATIVE_SNAPSHOT_INVALID")
        return snapshot

    def _load_authoritative_work(
        self,
        session_id: str,
        authority: SessionLockAuthority,
        *,
        current: bool,
    ) -> RunnableWork | None:
        snapshot = self._load_snapshot(session_id, authority)
        return self._derive_authoritative_work(
            session_id,
            snapshot,
            current=current,
        )

    @staticmethod
    def _derive_authoritative_work(
        session_id: str,
        snapshot: AuthoritativeWorkSnapshot,
        *,
        current: bool,
    ) -> RunnableWork | None:
        try:
            work = derive_runnable_work(
                snapshot.dialogue_state, snapshot.work_origin
            )
        except WorkError as exc:
            raise LeaseStoreError(exc.code) from exc
        if not current:
            return work
        registration = next(
            (
                item
                for item in snapshot.registry_state.dialogues
                if item.session_id == session_id
            ),
            None,
        )
        if (
            snapshot.registry_state.current_session_id != session_id
            or snapshot.registry_state.pending_handoff is not None
            or snapshot.registry_state.generation
            != snapshot.dialogue_state.registry_generation
            or type(registration) is not RegisteredDialogue
            or registration.status is not DialogueRegistrationStatus.ACTIVE
            or registration.activated_generation
            != snapshot.registry_state.generation
        ):
            raise LeaseStoreError("SESSION_DEACTIVATED")
        return work

    def _load_current_work(
        self, session_id: str, authority: SessionLockAuthority
    ) -> RunnableWork:
        work = self._load_authoritative_work(session_id, authority, current=True)
        if work is None:
            raise LeaseStoreError("NO_RUNNABLE_WORK")
        return work

    def _load_current_work_with_attempt(
        self,
        session_id: str,
        authority: SessionLockAuthority,
    ) -> tuple[RunnableWork, int]:
        current = self.current_runnable(session_id, authority)
        if current is None:
            raise LeaseStoreError("NO_RUNNABLE_WORK")
        return current.work, current.attempt

    def _verify_runtime(
        self,
        check: RuntimeAuthorityCheck,
        authority: SessionLockAuthority,
    ) -> None:
        try:
            accepted = self._runtime_authority_verifier(check, authority)
        except Exception as exc:
            raise LeaseStoreError("RUNTIME_AUTHORITY_FAILED") from exc
        if type(accepted) is not bool or not accepted:
            raise LeaseStoreError("RUNTIME_AUTHORITY_REJECTED")

    def _now(self) -> int:
        with self._clock_guard:
            try:
                value = self._clock()
            except Exception as exc:
                raise LeaseStoreError("CLOCK_FAILED") from exc
            if not _valid_nonnegative_int(value):
                raise LeaseStoreError("CLOCK_INVALID")
            previous = self._last_clock_value
            if previous is not None and value < previous:
                raise LeaseStoreError("CLOCK_ROLLED_BACK")
            self._last_clock_value = value
            return value

    @staticmethod
    def _work_component(work_id: str) -> str:
        return f"work-{hashlib.sha256(work_id.encode()).hexdigest()}"

    @staticmethod
    def _transaction_name(sequence: int) -> str:
        return f"transaction-{sequence:020d}.json"

    def _open_transaction_directory(
        self, session_id: str, work_id: str, *, create: bool
    ) -> SecureDirectory | None:
        opened: list[SecureDirectory] = []
        try:
            session = self._dialogues.open_directory(
                session_directory_component(session_id)
            )
            opened.append(session)
            operation = (
                SecureDirectory.ensure_directory
                if create
                else SecureDirectory.open_directory
            )
            runtime = operation(session, "runtime")
            opened.append(runtime)
            leases = operation(runtime, "leases")
            opened.append(leases)
            work = operation(leases, self._work_component(work_id))
            opened.append(work)
            transactions = operation(work, "transactions")
            return transactions
        except SecureFsError as exc:
            if not create and exc.code == "DIRECTORY_NOT_FOUND":
                return None
            raise
        finally:
            for directory in reversed(opened):
                directory.close()

    def _read_history(
        self, session_id: str, work_id: str
    ) -> tuple[_LeaseTransaction, ...]:
        try:
            directory = self._open_transaction_directory(
                session_id, work_id, create=False
            )
            if directory is None:
                return ()
            with directory:
                directory.recover_temporary_writes()
                names = directory.list_entries()
                transactions: list[_LeaseTransaction] = []
                for expected, name in enumerate(names, start=1):
                    match = _TRANSACTION_NAME.fullmatch(name)
                    if match is None or int(match.group(1)) != expected:
                        raise LeaseStoreError("LEASE_HISTORY_INVALID")
                    transaction = _decode_transaction(
                        directory.read_bytes(
                            name, max_bytes=_MAX_TRANSACTION_BYTES
                        )
                    )
                    transactions.append(transaction)
                return _validate_history(tuple(transactions))
        except LeaseStoreError:
            raise
        except (SecureFsError, ValueError) as exc:
            raise LeaseStoreError(getattr(exc, "code", str(exc))) from exc

    def _replay_receipt(
        self,
        session_id: str,
        work_id: str,
        request_id: str,
        request_digest: str,
        authority: SessionLockAuthority,
    ) -> LeaseMutationOutcome | None:
        self._assert_authority(session_id, authority)
        history = self._read_history(session_id, work_id)
        receipt = next(
            (
                transaction
                for transaction in history
                if transaction.request_id == request_id
            ),
            None,
        )
        if receipt is None:
            return None
        if receipt.request_digest != request_digest:
            raise LeaseStoreError("IDEMPOTENCY_CONFLICT")
        return LeaseMutationOutcome(receipt.operation, receipt.lease, True)

    def _commit(
        self,
        session_id: str,
        history: tuple[_LeaseTransaction, ...],
        operation: LeaseOperation,
        request_id: str,
        request_digest: str,
        work: RunnableWork,
        request: LeaseRequest,
        runtime_check: RuntimeAuthorityCheck,
        authority: SessionLockAuthority,
    ) -> LeaseMutationOutcome:
        sequence = len(history) + 1
        directory = self._open_transaction_directory(
            session_id, work.work_id, create=True
        )
        if directory is None:
            raise LeaseStoreError("LEASE_DIRECTORY_UNAVAILABLE")
        with directory:
            try:
                # The immutable hard-link below is the linearization point. The
                # namespace and registry -> Session ownership must still be
                # live at that exact boundary, not merely at snapshot time.
                self._assert_authority(session_id, authority)
                self._verify_runtime(runtime_check, authority)
                recorded_at = self._now()
                lease = self._lease_at_commit(
                    operation,
                    history,
                    work,
                    request,
                    recorded_at,
                )
                provisional = _LeaseTransaction(
                    transaction_sequence=sequence,
                    operation=operation,
                    request_id=request_id,
                    request_digest=request_digest,
                    request=request,
                    recorded_at=recorded_at,
                    previous_transaction_hash=(
                        history[-1].transaction_hash if history else None
                    ),
                    work=work,
                    lease=lease,
                    transaction_hash="sha256:" + "0" * 64,
                )
                transaction = replace(
                    provisional,
                    transaction_hash=_transaction_hash(provisional),
                )
                _validate_history((*history, transaction))
                encoded = _encode_transaction(transaction)

                def publication_guard() -> None:
                    self._assert_authority(session_id, authority)
                    self._verify_runtime(runtime_check, authority)
                    boundary_now = self._now()
                    self._assert_commit_clock(
                        operation,
                        history,
                        lease,
                        boundary_now,
                    )

                directory.write_immutable_guarded(
                    self._transaction_name(sequence),
                    encoded,
                    publication_guard,
                )
            except SecureFsError as exc:
                if exc.code != "IMMUTABLE_EXISTS":
                    raise LeaseStoreError(exc.code) from exc
                concurrent = self._read_history(session_id, work.work_id)
                receipt = next(
                    (
                        item
                        for item in concurrent
                        if item.request_id == request_id
                    ),
                    None,
                )
                if receipt is not None:
                    if receipt.request_digest != request_digest:
                        raise LeaseStoreError(
                            "IDEMPOTENCY_CONFLICT"
                        ) from None
                    return LeaseMutationOutcome(
                        receipt.operation, receipt.lease, True
                    )
                raise LeaseStoreError("LEASE_CONCURRENT_MODIFICATION") from exc
        return LeaseMutationOutcome(operation, lease, False)

    def _new_lease(
        self,
        work: RunnableWork,
        claim_id: str,
        owner_id: str,
        lease_version: int,
        now: int,
        lease_seconds: int,
        max_tenure_seconds: int,
    ) -> LeaseRecord:
        return _validate_lease(
            LeaseRecord(
                session_id=work.session_id,
                work_id=work.work_id,
                binding_digest=work.binding_digest,
                claim_id=claim_id,
                owner_id=owner_id,
                runtime_epoch=self._runtime_epoch,
                lease_version=lease_version,
                registry_generation=work.registry_generation,
                acquired_at=now,
                renewed_at=now,
                expires_at=now + lease_seconds,
                absolute_expires_at=now + max_tenure_seconds,
            )
        )

    @staticmethod
    def _current_tenure_renewals(
        history: tuple[_LeaseTransaction, ...],
    ) -> int:
        count = 0
        for transaction in reversed(history):
            if transaction.operation is not LeaseOperation.RENEW:
                break
            count += 1
        return count

    def _lease_at_commit(
        self,
        operation: LeaseOperation,
        history: tuple[_LeaseTransaction, ...],
        work: RunnableWork,
        request: LeaseRequest,
        now: int,
    ) -> LeaseRecord:
        """Re-evaluate time-sensitive rules at the transaction boundary."""
        if operation is LeaseOperation.CLAIM:
            if history or type(request) is not ClaimRequest:
                raise LeaseStoreError("LEASE_CONCURRENT_MODIFICATION")
            return self._new_lease(
                work,
                request.claim_id,
                request.owner_id,
                1,
                now,
                request.lease_seconds,
                request.max_tenure_seconds,
            )
        if not history:
            raise LeaseStoreError("LEASE_NOT_FOUND")
        current = history[-1].lease
        if operation is LeaseOperation.RENEW:
            if type(request) is not RenewRequest:
                raise LeaseStoreError("INVALID_RENEW_REQUEST")
            if current.runtime_epoch != self._runtime_epoch:
                raise LeaseStoreError("LEASE_EPOCH_FENCED")
            self._assert_claim_binding(
                current,
                work,
                request.claim_id,
                request.owner_id,
                request.expected_lease_version,
            )
            if now < current.renewed_at:
                raise LeaseStoreError("CLOCK_ROLLED_BACK")
            if now >= current.absolute_expires_at:
                raise LeaseStoreError("LEASE_TENURE_EXPIRED")
            if now >= current.expires_at:
                raise LeaseStoreError("LEASE_EXPIRED")
            next_expiry = min(
                now + request.lease_seconds,
                current.absolute_expires_at,
            )
            if next_expiry <= current.expires_at:
                raise LeaseStoreError("LEASE_RENEWAL_NOT_EXTENDED")
            return replace(
                current,
                lease_version=current.lease_version + 1,
                renewed_at=now,
                expires_at=next_expiry,
            )
        if type(request) is not ReclaimRequest:
            raise LeaseStoreError("INVALID_RECLAIM_REQUEST")
        epoch_changed = current.runtime_epoch != self._runtime_epoch
        if not epoch_changed and now < current.renewed_at:
            raise LeaseStoreError("CLOCK_ROLLED_BACK")
        if not epoch_changed and now < current.expires_at:
            raise LeaseStoreError("LEASE_STILL_ACTIVE")
        return self._new_lease(
            work,
            request.claim_id,
            request.owner_id,
            current.lease_version + 1,
            now,
            request.lease_seconds,
            request.max_tenure_seconds,
        )

    @staticmethod
    def _assert_commit_clock(
        operation: LeaseOperation,
        history: tuple[_LeaseTransaction, ...],
        lease: LeaseRecord,
        now: int,
    ) -> None:
        """Reject a lease that crossed a time fence during pure encoding."""
        if now < lease.renewed_at:
            raise LeaseStoreError("CLOCK_ROLLED_BACK")
        if operation is LeaseOperation.RENEW:
            if not history:
                raise LeaseStoreError("LEASE_HISTORY_INVALID")
            previous = history[-1].lease
            if now >= previous.absolute_expires_at:
                raise LeaseStoreError("LEASE_TENURE_EXPIRED")
            if now >= previous.expires_at:
                raise LeaseStoreError("LEASE_EXPIRED")
        if now >= lease.absolute_expires_at:
            raise LeaseStoreError("LEASE_TENURE_EXPIRED")
        if now >= lease.expires_at:
            raise LeaseStoreError("LEASE_EXPIRED")

    @staticmethod
    def _lease_is_active(
        lease: LeaseRecord, work: RunnableWork, now: int
    ) -> bool:
        return (
            lease.registry_generation == work.registry_generation
            and lease.binding_digest == work.binding_digest
            and now < lease.expires_at
            and now < lease.absolute_expires_at
        )

    @staticmethod
    def _assert_claim_binding(
        lease: LeaseRecord,
        work: RunnableWork,
        claim_id: str,
        owner_id: str,
        lease_version: int,
    ) -> None:
        if (
            lease.claim_id != claim_id
            or lease.owner_id != owner_id
            or lease.registry_generation != work.registry_generation
            or lease.binding_digest != work.binding_digest
        ):
            raise LeaseStoreError("LEASE_FENCED")
        if lease.lease_version != lease_version:
            raise LeaseStoreError("LEASE_VERSION_CONFLICT")
