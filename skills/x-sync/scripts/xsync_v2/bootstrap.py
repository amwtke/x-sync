"""Strict local bootstrap manifest for the first v2 Dialogue Session."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import os
from os import PathLike
import stat
from typing import cast

from .coordinator import DialogueSessionConfig
from .evidence import (
    MAX_EVIDENCE_SOURCES,
    EvidenceClaimType,
    EvidenceKind,
    EvidenceSource,
)
from .event_codec import PROTOCOL_VERSION, SCHEMA_VERSION, canonical_json_bytes
from .work_identity import is_protocol_id, is_sha256_digest


MAX_BOOTSTRAP_MANIFEST_BYTES = 256 * 1024
_FILE_MODE = 0o600
_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "protocol_version",
        "session_id",
        "learner_id",
        "created_at",
        "task_scope",
        "language",
        "channel",
        "style",
        "focus",
        "evidence_sources",
    }
)
_SOURCE_KEYS = frozenset(
    {
        "evidence_id",
        "kind",
        "claim_type",
        "claim",
        "relative_path",
        "start_line",
        "end_line",
        "imported_from",
    }
)


class DialogueBootstrapError(RuntimeError):
    """Stable bootstrap input or local-file failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class DialogueBootstrapManifest:
    """Immutable caller-reviewed inputs for the first Dialogue Session."""

    session_id: str
    learner_id: str
    created_at: str
    task_scope: str
    language: str
    channel: str
    style: str
    focus: str
    evidence_sources: tuple[EvidenceSource, ...]


def _duplicate_safe_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("DUPLICATE_KEY")
        result[key] = value
    return result


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


def _optional_int(value: object) -> bool:
    return value is None or (type(value) is int and value > 0)


def _optional_text(value: object) -> bool:
    return value is None or _nonblank(value, max_bytes=4096)


def _valid_timestamp(value: object) -> bool:
    if not _nonblank(value, max_bytes=128):
        return False
    try:
        parsed = datetime.fromisoformat(cast(str, value).replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _decode_source(value: object) -> EvidenceSource:
    if type(value) is not dict or frozenset(value) != _SOURCE_KEYS:
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID")
    item = cast(dict[str, object], value)
    if (
        not _nonblank(item["evidence_id"], max_bytes=128)
        or not _nonblank(item["claim"], max_bytes=16 * 1024)
        or not _nonblank(item["relative_path"], max_bytes=4096)
        or type(item["kind"]) is not str
        or type(item["claim_type"]) is not str
        or not _optional_int(item["start_line"])
        or not _optional_int(item["end_line"])
        or not _optional_text(item["imported_from"])
    ):
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID")
    start_line = cast(int | None, item["start_line"])
    end_line = cast(int | None, item["end_line"])
    if (start_line is None) != (end_line is None) or (
        start_line is not None and end_line is not None and end_line < start_line
    ):
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID")
    try:
        kind = EvidenceKind(item["kind"])
        claim_type = EvidenceClaimType(item["claim_type"])
    except (TypeError, ValueError) as exc:
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID") from exc
    return EvidenceSource(
        cast(str, item["evidence_id"]),
        kind,
        claim_type,
        cast(str, item["claim"]),
        cast(str, item["relative_path"]),
        start_line,
        end_line,
        cast(str | None, item["imported_from"]),
    )


def decode_bootstrap_manifest(raw: bytes) -> DialogueBootstrapManifest:
    """Decode one bounded exact-schema bootstrap manifest."""
    if type(raw) is not bytes or not raw or len(raw) > MAX_BOOTSTRAP_MANIFEST_BYTES:
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID")
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_duplicate_safe_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("INVALID_JSON_CONSTANT")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID") from exc
    if (
        type(value) is not dict
        or frozenset(value) != _MANIFEST_KEYS
        or value.get("schema_version") != SCHEMA_VERSION
        or value.get("record_type") != "dialogue_bootstrap_manifest"
        or value.get("protocol_version") != PROTOCOL_VERSION
    ):
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID")
    manifest = cast(dict[str, object], value)
    sources_value = manifest["evidence_sources"]
    if (
        not is_protocol_id(manifest["session_id"])
        or not _nonblank(manifest["learner_id"], max_bytes=1024)
        or not _valid_timestamp(manifest["created_at"])
        or not _nonblank(manifest["task_scope"], max_bytes=16 * 1024)
        or not _nonblank(manifest["language"], max_bytes=64)
        or type(manifest["channel"]) is not str
        or manifest["channel"] not in {"web", "terminal"}
        or type(manifest["style"]) is not str
        or manifest["style"] not in {"socratic", "regular"}
        or type(manifest["focus"]) is not str
        or manifest["focus"] not in {"business", "technical", "mixed"}
        or type(sources_value) is not list
        or not 1 <= len(sources_value) <= MAX_EVIDENCE_SOURCES
    ):
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID")
    sources = tuple(_decode_source(item) for item in sources_value)
    if len({item.evidence_id for item in sources}) != len(sources):
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID")
    return DialogueBootstrapManifest(
        cast(str, manifest["session_id"]),
        cast(str, manifest["learner_id"]),
        cast(str, manifest["created_at"]),
        cast(str, manifest["task_scope"]),
        cast(str, manifest["language"]),
        manifest["channel"],
        manifest["style"],
        manifest["focus"],
        sources,
    )


def read_bootstrap_manifest(path: str | PathLike[str]) -> DialogueBootstrapManifest:
    """Read one owner-only regular manifest without following its final link."""
    try:
        raw_path = os.fspath(path)
    except TypeError as exc:
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_UNAVAILABLE") from exc
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    cloexec = getattr(os, "O_CLOEXEC", 0)
    if type(raw_path) is not str or not raw_path or "\x00" in raw_path or not nofollow:
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_UNAVAILABLE")
    try:
        descriptor = os.open(raw_path, os.O_RDONLY | nofollow | cloexec)
    except OSError as exc:
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_UNAVAILABLE") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != _FILE_MODE
            or metadata.st_nlink != 1
            or not 1 <= metadata.st_size <= MAX_BOOTSTRAP_MANIFEST_BYTES
        ):
            raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_UNAVAILABLE")
        chunks: list[bytes] = []
        remaining = MAX_BOOTSTRAP_MANIFEST_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            len(raw) > MAX_BOOTSTRAP_MANIFEST_BYTES
            or (metadata.st_dev, metadata.st_ino, metadata.st_size)
            != (after.st_dev, after.st_ino, after.st_size)
        ):
            raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_UNAVAILABLE")
    except OSError as exc:
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_UNAVAILABLE") from exc
    finally:
        os.close(descriptor)
    return decode_bootstrap_manifest(raw)


def bootstrap_config(
    manifest: DialogueBootstrapManifest,
    repository_id: str,
    evidence_digest: str,
) -> DialogueSessionConfig:
    """Build the deterministic immutable Session config for one manifest."""
    if (
        type(manifest) is not DialogueBootstrapManifest
        or not _nonblank(repository_id, max_bytes=1024)
        or not is_sha256_digest(evidence_digest)
    ):
        raise DialogueBootstrapError("BOOTSTRAP_MANIFEST_INVALID")
    identity = canonical_json_bytes(
        {
            "repository_id": repository_id,
            "session_id": manifest.session_id,
            "learner_id": manifest.learner_id,
            "created_at": manifest.created_at,
        }
    )
    from .event_codec import sha256_digest

    epoch = "bootstrap." + sha256_digest(identity).removeprefix("sha256:")[:48]
    return DialogueSessionConfig(
        manifest.session_id,
        manifest.learner_id,
        repository_id,
        manifest.created_at,
        epoch,
        evidence_digest,
        manifest.task_scope,
        manifest.language,
        manifest.channel,
        manifest.style,
        manifest.focus,
    )


__all__ = [
    "DialogueBootstrapError",
    "DialogueBootstrapManifest",
    "bootstrap_config",
    "decode_bootstrap_manifest",
    "read_bootstrap_manifest",
]
