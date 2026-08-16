"""Immutable, focused repository evidence for X-Sync v2 dialogue sessions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import json
import os
from pathlib import Path
import re
import stat
import subprocess
from typing import cast

from .coordinator import DialogueSessionConfig
from .domain import EvidenceCheck, EvidenceHealth
from .event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    canonical_json_bytes,
    sha256_digest,
)
from .event_store import session_directory_component
from .locking import DomainLockManager, RegistryLockMode
from .secure_fs import SecureDirectory, SecureFsError
from .work_identity import is_protocol_id, is_sha256_digest


MAX_EVIDENCE_FILE_BYTES = 1_000_000
MAX_EVIDENCE_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_EVIDENCE_SOURCES = 16
_MAX_GIT_OUTPUT_BYTES = 2 * 1024 * 1024
_GIT_OID = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")
_SENSITIVE_PARTS = frozenset(
    {
        ".aws",
        ".azure",
        ".docker",
        ".gcloud",
        ".git",
        ".gnupg",
        ".kube",
        ".ssh",
        ".x-sync",
        "credentials",
        "secrets",
    }
)
_SECRET_NAMES = frozenset(
    {
        ".dockercfg",
        ".env",
        ".envrc",
        ".netrc",
        ".npmrc",
        ".pypirc",
        "auth.json",
        "credentials",
        "credentials.json",
        "id_ed25519",
        "id_rsa",
        "secret.json",
        "secrets.json",
        "terraform.tfstate",
    }
)
_SENSITIVE_SUFFIXES = frozenset(
    {
        ".jks",
        ".kdbx",
        ".key",
        ".keystore",
        ".mobileprovision",
        ".p12",
        ".pem",
        ".pfx",
        ".tfstate",
    }
)
_GENERATED_PARTS = frozenset(
    {
        ".cache",
        ".gradle",
        ".mypy_cache",
        ".next",
        ".pytest_cache",
        ".ruff_cache",
        ".terraform",
        ".tox",
        ".venv",
        "__pycache__",
        "build",
        "coverage",
        "dist",
        "node_modules",
        "out",
        "target",
        "vendor",
        "venv",
    }
)
_SNAPSHOT_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "protocol_version",
        "snapshot_digest",
        "session_id",
        "repository_id",
        "captured_at",
        "repository_head",
        "focused_dirty_at_capture",
        "focused_status_digest",
        "entries",
    }
)
_ENTRY_KEYS = frozenset(
    {
        "evidence_id",
        "kind",
        "claim_type",
        "claim",
        "relative_path",
        "start_line",
        "end_line",
        "content_hash",
        "imported_from",
    }
)


class EvidenceStoreError(RuntimeError):
    """Stable evidence capture, codec, or safe-read failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class EvidenceKind(StrEnum):
    """Closed focused repository artifact kinds."""

    SPEC = "spec"
    ADR = "adr"
    BUG = "bug"
    TEST = "test"
    CODE = "code"
    CONFIG = "config"


class EvidenceClaimType(StrEnum):
    """Closed epistemic status captured from repository evidence."""

    REQUIREMENT = "requirement"
    DECISION = "decision"
    IMPLEMENTATION = "implementation"
    INFERENCE = "inference"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class EvidenceSource:
    """One caller-selected safe file or inclusive line range to freeze."""

    evidence_id: str
    kind: EvidenceKind
    claim_type: EvidenceClaimType
    claim: str
    relative_path: str
    start_line: int | None = None
    end_line: int | None = None
    imported_from: str | None = None


@dataclass(frozen=True, slots=True)
class FrozenEvidenceEntry:
    """One immutable evidence record with an exact selected-content hash."""

    evidence_id: str
    kind: EvidenceKind
    claim_type: EvidenceClaimType
    claim: str
    relative_path: str
    start_line: int | None
    end_line: int | None
    content_hash: str
    imported_from: str | None

    @property
    def location(self) -> str:
        """Return the stable repository-relative human-readable locator."""
        if self.start_line is None:
            return self.relative_path
        return f"{self.relative_path}:{self.start_line}-{self.end_line}"


@dataclass(frozen=True, slots=True)
class EvidenceSnapshot:
    """Canonical immutable set of focused facts bound to one Session."""

    snapshot_digest: str
    session_id: str
    repository_id: str
    captured_at: str
    repository_head: str
    focused_dirty_at_capture: bool
    focused_status_digest: str
    entries: tuple[FrozenEvidenceEntry, ...]


def _nonblank(value: object, *, max_bytes: int) -> bool:
    if type(value) is not str or not value or value != value.strip():
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        return False
    return len(encoded) <= max_bytes and not any(
        ord(character) < 32 or ord(character) == 127 for character in value
    )


def _valid_timestamp(value: object) -> bool:
    if not _nonblank(value, max_bytes=128):
        return False
    try:
        parsed = datetime.fromisoformat(cast(str, value).replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _validate_relative_path(value: object) -> str:
    if type(value) is not str or not value or "\\" in value or "\x00" in value:
        raise EvidenceStoreError("EVIDENCE_PATH_UNSAFE")
    path = Path(value)
    if path.is_absolute() or any(
        not part or part in {".", ".."} for part in path.parts
    ):
        raise EvidenceStoreError("EVIDENCE_PATH_UNSAFE")
    lowered = tuple(part.lower() for part in path.parts)
    name = path.name.lower()
    stem_tokens = frozenset(re.split(r"[._-]+", path.stem.lower()))
    secret_tokens = {
        "credential",
        "credentials",
        "secret",
        "secrets",
        "token",
        "tokens",
    }
    secret_config = path.suffix.lower() in {
        ".conf",
        ".ini",
        ".json",
        ".properties",
        ".text",
        ".tfvars",
        ".toml",
        ".txt",
        ".xml",
        ".yaml",
        ".yml",
    } and bool(stem_tokens & secret_tokens)
    if (
        any(part in _SENSITIVE_PARTS for part in lowered)
        or any(part in _GENERATED_PARTS for part in lowered)
        or name in _SECRET_NAMES
        or name.startswith(".env.")
        or secret_config
        or ".tfstate" in name
        or ".tfvars" in name
        or path.suffix.lower() in _SENSITIVE_SUFFIXES
        or len(value.encode("utf-8")) > 4096
    ):
        raise EvidenceStoreError("EVIDENCE_PATH_UNSAFE")
    return path.as_posix()


def _validate_source(value: object) -> EvidenceSource:
    if (
        type(value) is not EvidenceSource
        or not is_protocol_id(value.evidence_id)
        or type(value.kind) is not EvidenceKind
        or type(value.claim_type) is not EvidenceClaimType
        or not _nonblank(value.claim, max_bytes=8 * 1024)
        or (
            value.imported_from is not None
            and not is_protocol_id(value.imported_from)
        )
    ):
        raise EvidenceStoreError("EVIDENCE_SOURCE_INVALID")
    _validate_relative_path(value.relative_path)
    lines_absent = value.start_line is None and value.end_line is None
    lines_valid = (
        type(value.start_line) is int
        and type(value.end_line) is int
        and value.start_line >= 1
        and value.end_line >= value.start_line
    )
    if not lines_absent and not lines_valid:
        raise EvidenceStoreError("EVIDENCE_SOURCE_INVALID")
    return value


def _entry_tree(entry: FrozenEvidenceEntry) -> dict[str, object]:
    return {
        "evidence_id": entry.evidence_id,
        "kind": entry.kind.value,
        "claim_type": entry.claim_type.value,
        "claim": entry.claim,
        "relative_path": entry.relative_path,
        "start_line": entry.start_line,
        "end_line": entry.end_line,
        "content_hash": entry.content_hash,
        "imported_from": entry.imported_from,
    }


def _snapshot_tree(
    snapshot: EvidenceSnapshot,
    *,
    include_digest: bool,
) -> dict[str, object]:
    tree: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "focused_evidence_snapshot",
        "protocol_version": PROTOCOL_VERSION,
        "session_id": snapshot.session_id,
        "repository_id": snapshot.repository_id,
        "captured_at": snapshot.captured_at,
        "repository_head": snapshot.repository_head,
        "focused_dirty_at_capture": snapshot.focused_dirty_at_capture,
        "focused_status_digest": snapshot.focused_status_digest,
        "entries": [_entry_tree(item) for item in snapshot.entries],
    }
    if include_digest:
        tree["snapshot_digest"] = snapshot.snapshot_digest
    return tree


def encode_evidence_snapshot(snapshot: EvidenceSnapshot) -> bytes:
    """Strictly encode one canonical snapshot and verify its content digest."""
    snapshot = _validate_snapshot(snapshot)
    calculated = sha256_digest(
        canonical_json_bytes(_snapshot_tree(snapshot, include_digest=False))
    )
    if calculated != snapshot.snapshot_digest:
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_DIGEST_MISMATCH")
    return canonical_json_bytes(_snapshot_tree(snapshot, include_digest=True))


def _object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID")
        result[key] = value
    return result


def decode_evidence_snapshot(raw: bytes) -> EvidenceSnapshot:
    """Decode exact schema-v2 snapshot bytes and reject non-canonical input."""
    if type(raw) is not bytes or not raw or len(raw) > MAX_EVIDENCE_SNAPSHOT_BYTES:
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_object_pairs)
    except EvidenceStoreError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID") from exc
    if type(value) is not dict or frozenset(value) != _SNAPSHOT_KEYS:
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID")
    entries_value = value["entries"]
    if type(entries_value) is not list:
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID")
    entries: list[FrozenEvidenceEntry] = []
    for item in entries_value:
        if type(item) is not dict or frozenset(item) != _ENTRY_KEYS:
            raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID")
        try:
            entry = FrozenEvidenceEntry(
                cast(str, item["evidence_id"]),
                EvidenceKind(cast(str, item["kind"])),
                EvidenceClaimType(cast(str, item["claim_type"])),
                cast(str, item["claim"]),
                cast(str, item["relative_path"]),
                cast(int | None, item["start_line"]),
                cast(int | None, item["end_line"]),
                cast(str, item["content_hash"]),
                cast(str | None, item["imported_from"]),
            )
        except (TypeError, ValueError) as exc:
            raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID") from exc
        entries.append(entry)
    try:
        snapshot = EvidenceSnapshot(
            cast(str, value["snapshot_digest"]),
            cast(str, value["session_id"]),
            cast(str, value["repository_id"]),
            cast(str, value["captured_at"]),
            cast(str, value["repository_head"]),
            cast(bool, value["focused_dirty_at_capture"]),
            cast(str, value["focused_status_digest"]),
            tuple(entries),
        )
        snapshot = _validate_snapshot(snapshot)
    except (TypeError, ValueError) as exc:
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID") from exc
    calculated = sha256_digest(
        canonical_json_bytes(_snapshot_tree(snapshot, include_digest=False))
    )
    if calculated != snapshot.snapshot_digest:
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_DIGEST_MISMATCH")
    if canonical_json_bytes(value) != raw:
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_NON_CANONICAL")
    return snapshot


def _validate_snapshot(value: object) -> EvidenceSnapshot:
    if (
        type(value) is not EvidenceSnapshot
        or not is_sha256_digest(value.snapshot_digest)
        or not is_protocol_id(value.session_id)
        or not _nonblank(value.repository_id, max_bytes=1024)
        or not _valid_timestamp(value.captured_at)
        or type(value.repository_head) is not str
        or _GIT_OID.fullmatch(value.repository_head) is None
        or type(value.focused_dirty_at_capture) is not bool
        or not is_sha256_digest(value.focused_status_digest)
        or type(value.entries) is not tuple
        or not 1 <= len(value.entries) <= MAX_EVIDENCE_SOURCES
    ):
        raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID")
    seen: set[str] = set()
    for entry in value.entries:
        if (
            type(entry) is not FrozenEvidenceEntry
            or entry.evidence_id in seen
            or not is_protocol_id(entry.evidence_id)
            or type(entry.kind) is not EvidenceKind
            or type(entry.claim_type) is not EvidenceClaimType
            or not _nonblank(entry.claim, max_bytes=8 * 1024)
            or not is_sha256_digest(entry.content_hash)
            or (
                entry.imported_from is not None
                and not is_protocol_id(entry.imported_from)
            )
        ):
            raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID")
        _validate_relative_path(entry.relative_path)
        source = EvidenceSource(
            entry.evidence_id,
            entry.kind,
            entry.claim_type,
            entry.claim,
            entry.relative_path,
            entry.start_line,
            entry.end_line,
            entry.imported_from,
        )
        _validate_source(source)
        seen.add(entry.evidence_id)
    return value


class _RepositoryReader:
    def __init__(self, path: Path):
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        directory = getattr(os, "O_DIRECTORY", 0)
        cloexec = getattr(os, "O_CLOEXEC", 0)
        if not nofollow or not directory or not cloexec or not path.is_absolute():
            raise EvidenceStoreError("REPOSITORY_READER_UNAVAILABLE")
        descriptor = os.open("/", os.O_RDONLY | nofollow | directory | cloexec)
        try:
            for part in path.parts[1:]:
                next_descriptor = os.open(
                    part,
                    os.O_RDONLY | nofollow | directory | cloexec,
                    dir_fd=descriptor,
                )
                os.close(descriptor)
                descriptor = next_descriptor
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise EvidenceStoreError("REPOSITORY_PATH_UNSAFE")
        except BaseException:
            os.close(descriptor)
            raise
        self._descriptor = descriptor
        self._path = path
        self._identity = (metadata.st_dev, metadata.st_ino)

    def close(self) -> None:
        descriptor = self._descriptor
        if descriptor < 0:
            return
        self._descriptor = -1
        try:
            os.close(descriptor)
        except OSError:
            pass

    def _assert_current(self) -> None:
        if self._descriptor < 0:
            raise EvidenceStoreError("EVIDENCE_STORE_CLOSED")
        try:
            current = os.stat(self._path, follow_symlinks=False)
        except OSError as exc:
            raise EvidenceStoreError("REPOSITORY_PATH_UNAVAILABLE") from exc
        if (
            not stat.S_ISDIR(current.st_mode)
            or (current.st_dev, current.st_ino) != self._identity
        ):
            raise EvidenceStoreError("REPOSITORY_PATH_CHANGED")

    def read(self, source: EvidenceSource | FrozenEvidenceEntry) -> bytes:
        relative = _validate_relative_path(source.relative_path)
        self._assert_current()
        nofollow = cast(int, getattr(os, "O_NOFOLLOW", 0))
        directory = cast(int, getattr(os, "O_DIRECTORY", 0))
        cloexec = cast(int, getattr(os, "O_CLOEXEC", 0))
        current = os.dup(self._descriptor)
        try:
            parts = Path(relative).parts
            for part in parts[:-1]:
                next_descriptor = os.open(
                    part,
                    os.O_RDONLY | nofollow | directory | cloexec,
                    dir_fd=current,
                )
                os.close(current)
                current = next_descriptor
            descriptor = os.open(
                parts[-1],
                os.O_RDONLY | nofollow | cloexec,
                dir_fd=current,
            )
            try:
                before = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(before.st_mode)
                    or before.st_uid != os.geteuid()
                    or before.st_nlink != 1
                    or before.st_size > MAX_EVIDENCE_FILE_BYTES
                ):
                    raise EvidenceStoreError("EVIDENCE_FILE_UNSAFE")
                chunks: list[bytes] = []
                remaining = MAX_EVIDENCE_FILE_BYTES + 1
                while remaining:
                    chunk = os.read(descriptor, min(remaining, 64 * 1024))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                raw = b"".join(chunks)
                after = os.fstat(descriptor)
            finally:
                os.close(descriptor)
        except EvidenceStoreError:
            raise
        except OSError as exc:
            raise EvidenceStoreError("EVIDENCE_FILE_UNAVAILABLE") from exc
        finally:
            os.close(current)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if identity_before != identity_after or len(raw) > MAX_EVIDENCE_FILE_BYTES:
            raise EvidenceStoreError("EVIDENCE_CHANGED_DURING_READ")
        if b"\x00" in raw:
            raise EvidenceStoreError("EVIDENCE_FILE_UNSAFE")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise EvidenceStoreError("EVIDENCE_FILE_UNSAFE") from exc
        start = source.start_line
        end = source.end_line
        if start is None:
            return raw
        assert end is not None
        lines = text.splitlines(keepends=True)
        if end > len(lines):
            raise EvidenceStoreError("EVIDENCE_LINE_RANGE_INVALID")
        return "".join(lines[start - 1 : end]).encode("utf-8")

    def git(self, arguments: Sequence[str]) -> bytes:
        self._assert_current()
        environment = {
            **os.environ,
            "LC_ALL": "C",
            "LANG": "C",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
        }
        try:
            completed = subprocess.run(
                [
                    "git",
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "core.untrackedCache=false",
                    "-C",
                    os.fspath(self._path),
                    *arguments,
                ],
                check=False,
                capture_output=True,
                env=environment,
                timeout=15,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise EvidenceStoreError("GIT_UNAVAILABLE") from exc
        if (
            completed.returncode != 0
            or len(completed.stdout) > _MAX_GIT_OUTPUT_BYTES
            or len(completed.stderr) > 64 * 1024
        ):
            raise EvidenceStoreError("GIT_EVIDENCE_FAILED")
        self._assert_current()
        return completed.stdout


class SessionEvidenceStore:
    """Capture and revalidate immutable focused evidence per Dialogue Session."""

    def __init__(
        self,
        repository_directory: str | os.PathLike[str],
        repository_id: str,
        dialogues_directory: SecureDirectory,
        locks: DomainLockManager,
    ) -> None:
        try:
            raw = os.fspath(repository_directory)
        except TypeError as exc:
            raise EvidenceStoreError("INVALID_EVIDENCE_STORE_CONFIGURATION") from exc
        if (
            type(raw) is not str
            or not raw
            or "\x00" in raw
            or "\\" in raw
            or not Path(raw).is_absolute()
            or any(part in {".", ".."} for part in Path(raw).parts)
            or not _nonblank(repository_id, max_bytes=1024)
            or type(dialogues_directory) is not SecureDirectory
            or type(locks) is not DomainLockManager
        ):
            raise EvidenceStoreError("INVALID_EVIDENCE_STORE_CONFIGURATION")
        try:
            reader = _RepositoryReader(Path(raw))
        except (OSError, EvidenceStoreError) as exc:
            raise EvidenceStoreError("INVALID_EVIDENCE_STORE_CONFIGURATION") from exc
        self._repository_id = repository_id
        self._dialogues = dialogues_directory
        self._locks = locks
        self._reader = reader

    def close(self) -> None:
        """Idempotently release the anchored repository reader."""
        self._reader.close()

    def __enter__(self) -> SessionEvidenceStore:
        """Return this open evidence store for bounded ownership."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the anchored repository reader at context exit."""
        self.close()

    def capture(
        self,
        session_id: str,
        sources: tuple[EvidenceSource, ...],
        *,
        captured_at: str,
    ) -> EvidenceSnapshot:
        """Freeze exact focused records before resolving a new Session."""
        if (
            not is_protocol_id(session_id)
            or type(sources) is not tuple
            or not 1 <= len(sources) <= MAX_EVIDENCE_SOURCES
            or not _valid_timestamp(captured_at)
        ):
            raise EvidenceStoreError("EVIDENCE_CAPTURE_INVALID")
        validated = tuple(_validate_source(item) for item in sources)
        if len({item.evidence_id for item in validated}) != len(validated):
            raise EvidenceStoreError("EVIDENCE_CAPTURE_INVALID")
        with self._locks.semantic_session(
            session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ):
            return self._capture_locked(session_id, validated, captured_at)

    def _capture_locked(
        self,
        session_id: str,
        sources: tuple[EvidenceSource, ...],
        captured_at: str,
    ) -> EvidenceSnapshot:
        head_before = self._head()
        first = tuple(self._reader.read(item) for item in sources)
        status = self._focused_status(sources)
        head_after = self._head()
        second = tuple(self._reader.read(item) for item in sources)
        if head_before != head_after or first != second:
            raise EvidenceStoreError("EVIDENCE_CHANGED_DURING_CAPTURE")
        entries = tuple(
            FrozenEvidenceEntry(
                source.evidence_id,
                source.kind,
                source.claim_type,
                source.claim,
                _validate_relative_path(source.relative_path),
                source.start_line,
                source.end_line,
                sha256_digest(raw),
                source.imported_from,
            )
            for source, raw in zip(sources, first, strict=True)
        )
        provisional = EvidenceSnapshot(
            "sha256:" + "0" * 64,
            session_id,
            self._repository_id,
            captured_at,
            head_before,
            bool(status),
            sha256_digest(status),
            entries,
        )
        digest = sha256_digest(
            canonical_json_bytes(_snapshot_tree(provisional, include_digest=False))
        )
        snapshot = EvidenceSnapshot(
            digest,
            provisional.session_id,
            provisional.repository_id,
            provisional.captured_at,
            provisional.repository_head,
            provisional.focused_dirty_at_capture,
            provisional.focused_status_digest,
            provisional.entries,
        )
        raw = encode_evidence_snapshot(snapshot)
        session = self._dialogues.ensure_directory(
            session_directory_component(session_id)
        )
        try:
            evidence = session.ensure_directory("evidence")
            try:
                component = self._snapshot_component(digest)
                try:
                    evidence.write_immutable(component, raw)
                except SecureFsError as exc:
                    if exc.code != "IMMUTABLE_EXISTS":
                        raise
                    existing = evidence.read_bytes(
                        component,
                        max_bytes=MAX_EVIDENCE_SNAPSHOT_BYTES,
                    )
                    if existing != raw:
                        raise EvidenceStoreError(
                            "EVIDENCE_SNAPSHOT_CONFLICT"
                        ) from exc
            finally:
                evidence.close()
        finally:
            session.close()
        return snapshot

    def verify(self, config: DialogueSessionConfig) -> EvidenceCheck:
        """Return deterministic freshness for a Session's immutable snapshot."""
        if (
            type(config) is not DialogueSessionConfig
            or config.repository_id != self._repository_id
            or not is_protocol_id(config.session_id)
            or not is_sha256_digest(config.evidence_digest)
        ):
            raise EvidenceStoreError("EVIDENCE_CONFIG_MISMATCH")
        try:
            snapshot = self.load(config.session_id, config.evidence_digest)
            current = tuple(self._reader.read(item) for item in snapshot.entries)
        except (EvidenceStoreError, SecureFsError):
            return EvidenceCheck(EvidenceHealth.UNAVAILABLE, config.evidence_digest)
        if any(
            sha256_digest(raw) != entry.content_hash
            for raw, entry in zip(current, snapshot.entries, strict=True)
        ):
            return EvidenceCheck(EvidenceHealth.STALE, config.evidence_digest)
        try:
            status = self._focused_status(snapshot.entries)
            head = self._head()
        except EvidenceStoreError:
            return EvidenceCheck(EvidenceHealth.UNAVAILABLE, config.evidence_digest)
        if status:
            fingerprint = sha256_digest(
                canonical_json_bytes(
                    {
                        "repository_head": head,
                        "focused_status_digest": sha256_digest(status),
                        "content_hashes": [
                            item.content_hash for item in snapshot.entries
                        ],
                    }
                )
            )
            return EvidenceCheck(
                EvidenceHealth.CAPTURED_DIRTY,
                config.evidence_digest,
                fingerprint,
            )
        return EvidenceCheck(EvidenceHealth.CURRENT, config.evidence_digest)

    def load(self, session_id: str, digest: str) -> EvidenceSnapshot:
        """Load and verify one exact content-addressed Session snapshot."""
        if not is_protocol_id(session_id) or not is_sha256_digest(digest):
            raise EvidenceStoreError("EVIDENCE_SNAPSHOT_INVALID")
        session = self._dialogues.open_directory(
            session_directory_component(session_id)
        )
        try:
            evidence = session.open_directory("evidence")
            try:
                raw = evidence.read_bytes(
                    self._snapshot_component(digest),
                    max_bytes=MAX_EVIDENCE_SNAPSHOT_BYTES,
                )
            finally:
                evidence.close()
        finally:
            session.close()
        snapshot = decode_evidence_snapshot(raw)
        if (
            snapshot.snapshot_digest != digest
            or snapshot.session_id != session_id
            or snapshot.repository_id != self._repository_id
        ):
            raise EvidenceStoreError("EVIDENCE_SNAPSHOT_MISMATCH")
        return snapshot

    @staticmethod
    def _snapshot_component(digest: str) -> str:
        return f"snapshot-{digest.removeprefix('sha256:')}.json"

    def _head(self) -> str:
        raw = self._reader.git(("rev-parse", "HEAD"))
        try:
            value = raw.decode("ascii").strip()
        except UnicodeDecodeError as exc:
            raise EvidenceStoreError("GIT_EVIDENCE_FAILED") from exc
        if _GIT_OID.fullmatch(value) is None:
            raise EvidenceStoreError("GIT_EVIDENCE_FAILED")
        return value

    def _focused_status(
        self,
        sources: tuple[EvidenceSource, ...] | tuple[FrozenEvidenceEntry, ...],
    ) -> bytes:
        paths = tuple(item.relative_path for item in sources)
        return self._reader.git(
            (
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
                "--",
                *paths,
            )
        )
