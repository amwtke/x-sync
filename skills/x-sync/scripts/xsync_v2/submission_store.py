"""Durable opaque submission-handle registrations for Host results.

The Agent sees a random handle.  Runtime persistence contains only its SHA-256
digest plus the immutable work and claim identity needed for receipt-first
publish.  Registrations never contain result data and never grant a lease;
the Host application service must still verify the live lease at the dialogue
marker boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import cast

from .event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    canonical_json_bytes,
    sha256_digest,
)
from .lease_store import (
    LeaseStoreError,
    PublishFence,
    _decode_work,
    _work_tree,
)
from .locking import DomainLockManager, LockError, SessionLockAuthority
from .secure_fs import SecureDirectory, SecureFsError
from .work import RunnableWork, WorkError, validate_runnable_work
from .work_identity import is_protocol_id, is_sha256_digest


_MAX_RECORD_BYTES = 64 * 1024
_NAMESPACE_FILE = "submission-lock-namespace.json"
_NAMESPACE_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "protocol_version",
        "lock_device",
        "lock_inode",
    }
)
_RECORD_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "protocol_version",
        "handle_digest",
        "work",
        "fence",
        "record_digest",
    }
)
_FENCE_KEYS = frozenset(
    {
        "session_id",
        "claim_id",
        "work_id",
        "owner_id",
        "runtime_epoch",
        "lease_version",
        "registry_generation",
    }
)


class SubmissionStoreError(RuntimeError):
    """Stable submission registration, integrity, or authority failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class SubmissionHandleRecord:
    """One immutable hash-only handle binding for a claimed work tenure."""

    handle_digest: str
    work: RunnableWork
    fence: PublishFence
    record_digest: str


@dataclass(frozen=True, slots=True)
class SubmissionRegistrationOutcome:
    """Durable registration or an exact replay of the same registration."""

    record: SubmissionHandleRecord
    replayed: bool


def _require_mapping(
    value: object,
    keys: frozenset[str],
) -> dict[str, object]:
    if type(value) is not dict or frozenset(value) != keys:
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    return cast(dict[str, object], value)


def _pairs(values: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in values:
        if key in result:
            raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
        result[key] = value
    return result


def _handle_digest(handle: object) -> str:
    if not is_protocol_id(handle) or not cast(str, handle).startswith("submission."):
        raise SubmissionStoreError("SUBMISSION_HANDLE_INVALID")
    return sha256_digest(cast(str, handle).encode("utf-8"))


def _component(handle_digest: str) -> str:
    if not is_sha256_digest(handle_digest):
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    return f"handle-{handle_digest.removeprefix('sha256:')}.json"


def _fence_tree(fence: PublishFence) -> dict[str, object]:
    if type(fence) is not PublishFence:
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    return {
        "session_id": fence.session_id,
        "claim_id": fence.claim_id,
        "work_id": fence.work_id,
        "owner_id": fence.owner_id,
        "runtime_epoch": fence.runtime_epoch,
        "lease_version": fence.lease_version,
        "registry_generation": fence.registry_generation,
    }


def _decode_fence(value: object) -> PublishFence:
    item = _require_mapping(value, _FENCE_KEYS)
    fence = PublishFence(
        cast(str, item["session_id"]),
        cast(str, item["claim_id"]),
        cast(str, item["work_id"]),
        cast(str, item["owner_id"]),
        cast(str, item["runtime_epoch"]),
        cast(int, item["lease_version"]),
        cast(int, item["registry_generation"]),
    )
    return _validate_fence(fence)


def _record_tree(
    handle_digest: str,
    work: RunnableWork,
    fence: PublishFence,
    *,
    include_digest: bool,
    record_digest: str = "",
) -> dict[str, object]:
    tree: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "submission_handle_registration",
        "protocol_version": PROTOCOL_VERSION,
        "handle_digest": handle_digest,
        "work": _work_tree(work),
        "fence": _fence_tree(fence),
    }
    if include_digest:
        tree["record_digest"] = record_digest
    return tree


def _validate_fence(fence: PublishFence) -> PublishFence:
    if (
        type(fence) is not PublishFence
        or not is_protocol_id(fence.session_id)
        or not is_protocol_id(fence.claim_id)
        or not is_protocol_id(fence.work_id)
        or not is_protocol_id(fence.owner_id)
        or not is_protocol_id(fence.runtime_epoch)
        or type(fence.lease_version) is not int
        or fence.lease_version < 1
        or type(fence.registry_generation) is not int
        or fence.registry_generation < 1
    ):
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    return fence


def _validate_binding(work: RunnableWork, fence: PublishFence) -> None:
    _validate_fence(fence)
    if type(work) is not RunnableWork:
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    try:
        validate_runnable_work(work)
    except WorkError as exc:
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID") from exc
    if (
        fence.session_id != work.session_id
        or fence.work_id != work.work_id
        or fence.registry_generation != work.registry_generation
    ):
        raise SubmissionStoreError("SUBMISSION_WORK_MISMATCH")


def _build_record(
    handle_digest: str,
    work: RunnableWork,
    fence: PublishFence,
) -> SubmissionHandleRecord:
    if not is_sha256_digest(handle_digest):
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    _validate_binding(work, fence)
    digest = sha256_digest(
        canonical_json_bytes(
            _record_tree(handle_digest, work, fence, include_digest=False)
        )
    )
    return SubmissionHandleRecord(handle_digest, work, fence, digest)


def _encode_record(record: SubmissionHandleRecord) -> bytes:
    if type(record) is not SubmissionHandleRecord:
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    expected = _build_record(record.handle_digest, record.work, record.fence)
    if record != expected:
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    encoded = canonical_json_bytes(
        _record_tree(
            record.handle_digest,
            record.work,
            record.fence,
            include_digest=True,
            record_digest=record.record_digest,
        )
    )
    if len(encoded) > _MAX_RECORD_BYTES:
        raise SubmissionStoreError("SUBMISSION_RECORD_TOO_LARGE")
    return encoded


def _decode_record(data: bytes) -> SubmissionHandleRecord:
    if type(data) is not bytes or not data or len(data) > _MAX_RECORD_BYTES:
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_pairs)
        item = _require_mapping(value, _RECORD_KEYS)
        if (
            item["schema_version"] != SCHEMA_VERSION
            or item["record_type"] != "submission_handle_registration"
            or item["protocol_version"] != PROTOCOL_VERSION
            or not is_sha256_digest(item["handle_digest"])
            or not is_sha256_digest(item["record_digest"])
        ):
            raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
        try:
            work = _decode_work(item["work"])
        except LeaseStoreError as exc:
            raise SubmissionStoreError("SUBMISSION_RECORD_INVALID") from exc
        fence = _decode_fence(item["fence"])
        record = SubmissionHandleRecord(
            cast(str, item["handle_digest"]),
            work,
            fence,
            cast(str, item["record_digest"]),
        )
        if record != _build_record(record.handle_digest, work, fence):
            raise SubmissionStoreError("SUBMISSION_RECORD_DIGEST_MISMATCH")
        if data != _encode_record(record):
            raise SubmissionStoreError("SUBMISSION_RECORD_NON_CANONICAL")
        return record
    except SubmissionStoreError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise SubmissionStoreError("SUBMISSION_RECORD_INVALID") from exc


class SubmissionHandleStore:
    """Persist and resolve immutable hash-only submission registrations."""

    def __init__(
        self,
        dialogues_directory: SecureDirectory,
        locks: DomainLockManager,
    ) -> None:
        if (
            type(dialogues_directory) is not SecureDirectory
            or type(locks) is not DomainLockManager
        ):
            raise SubmissionStoreError("INVALID_SUBMISSION_STORE_CONFIGURATION")
        self._locks = locks
        self._directory = dialogues_directory.ensure_directory("submissions")
        try:
            self._lock_namespace_identity = self._bind_lock_namespace()
        except BaseException:
            self._directory.close()
            raise

    def register(
        self,
        handle: str,
        work: RunnableWork,
        fence: PublishFence,
        authority: SessionLockAuthority,
    ) -> SubmissionRegistrationOutcome:
        """Durably register one handle hash under its live Session authority."""
        handle_digest = _handle_digest(handle)
        record = _build_record(handle_digest, work, fence)
        self._assert_authority(work.session_id, authority)
        component = _component(handle_digest)
        try:
            existing = self._read_optional(component)
            if existing is not None:
                if existing != record:
                    raise SubmissionStoreError("SUBMISSION_HANDLE_CONFLICT")
                return SubmissionRegistrationOutcome(existing, True)
            self._directory.write_immutable_guarded(
                component,
                _encode_record(record),
                lambda: self._assert_authority(work.session_id, authority),
            )
            return SubmissionRegistrationOutcome(record, False)
        except SubmissionStoreError:
            raise
        except SecureFsError as exc:
            if exc.code == "IMMUTABLE_EXISTS":
                existing = self._read_optional(component)
                if existing == record:
                    return SubmissionRegistrationOutcome(record, True)
                raise SubmissionStoreError("SUBMISSION_HANDLE_CONFLICT") from exc
            raise SubmissionStoreError(exc.code) from exc

    def resolve(self, handle: str) -> SubmissionHandleRecord:
        """Resolve an opaque handle by hash without ever persisting its secret."""
        handle_digest = _handle_digest(handle)
        record = self._read_optional(_component(handle_digest))
        if record is None:
            raise SubmissionStoreError("SUBMISSION_HANDLE_NOT_FOUND")
        if record.handle_digest != handle_digest:
            raise SubmissionStoreError("SUBMISSION_HANDLE_CONFLICT")
        return record

    def confirm(
        self,
        handle: str,
        expected: SubmissionHandleRecord,
        authority: SessionLockAuthority,
    ) -> SubmissionHandleRecord:
        """Re-read the immutable binding after acquiring its Session lock."""
        if type(expected) is not SubmissionHandleRecord:
            raise SubmissionStoreError("SUBMISSION_RECORD_INVALID")
        self._assert_authority(expected.work.session_id, authority)
        actual = self.resolve(handle)
        if actual != expected:
            raise SubmissionStoreError("SUBMISSION_HANDLE_CONFLICT")
        return actual

    def close(self) -> None:
        """Release the anchored submission directory handle."""
        self._directory.close()

    def _read_optional(self, component: str) -> SubmissionHandleRecord | None:
        try:
            data = self._directory.read_bytes(component, max_bytes=_MAX_RECORD_BYTES)
        except SecureFsError as exc:
            if exc.code == "FILE_NOT_FOUND":
                return None
            raise SubmissionStoreError(exc.code) from exc
        return _decode_record(data)

    def _bind_lock_namespace(self) -> tuple[int, int]:
        try:
            identity = self._locks.namespace_identity
            tree = {
                "schema_version": SCHEMA_VERSION,
                "record_type": "submission_lock_namespace",
                "protocol_version": PROTOCOL_VERSION,
                "lock_device": identity[0],
                "lock_inode": identity[1],
            }
            encoded = canonical_json_bytes(tree)
            try:
                self._directory.write_immutable(_NAMESPACE_FILE, encoded)
            except SecureFsError as exc:
                if exc.code != "IMMUTABLE_EXISTS":
                    raise
            durable = self._directory.read_bytes(_NAMESPACE_FILE, max_bytes=4096)
            try:
                decoded = json.loads(durable.decode("utf-8"), object_pairs_hook=_pairs)
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise SubmissionStoreError("LOCK_NAMESPACE_RECORD_INVALID") from exc
            item = _require_mapping(decoded, _NAMESPACE_KEYS)
            if durable != canonical_json_bytes(item) or item != tree:
                raise SubmissionStoreError("LOCK_NAMESPACE_MISMATCH")
            return identity
        except SubmissionStoreError:
            raise
        except (LockError, SecureFsError, ValueError) as exc:
            code = getattr(exc, "code", "SUBMISSION_STORE_FAILED")
            raise SubmissionStoreError(code) from exc

    def _assert_authority(
        self,
        session_id: str,
        authority: SessionLockAuthority,
    ) -> None:
        try:
            if self._locks.namespace_identity != self._lock_namespace_identity:
                raise SubmissionStoreError("LOCK_NAMESPACE_MISMATCH")
            self._locks.assert_session_authority(authority, session_id)
        except SubmissionStoreError:
            raise
        except LockError as exc:
            raise SubmissionStoreError(exc.code) from exc


__all__ = [
    "SubmissionHandleRecord",
    "SubmissionHandleStore",
    "SubmissionRegistrationOutcome",
    "SubmissionStoreError",
]
