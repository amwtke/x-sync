"""Deterministic, model-free materialization of committed dialogue exports."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from .domain import (
    CompleteExport,
    DialogueState,
    ExportRecord,
    ExportRequested,
    ExportStatus,
    InsightKind,
    LearnerModelEntry,
    TopicRunState,
)
from .event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    DurableEffectIntent,
    EffectKind,
    canonical_json_bytes,
    sha256_digest,
)
from .secure_fs import SecureDirectory, SecureFsError
from .state_machine import validate_state
from .work_identity import is_protocol_id, is_sha256_digest

MAX_EXPORT_BYTES = 4 * 1024 * 1024
_LATEST_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "export_id",
        "export_sequence",
        "json_path",
        "json_digest",
        "markdown_path",
        "markdown_digest",
        "freshness_overlay_digest",
    }
)


class ExportError(RuntimeError):
    """Stable, path-free export failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class ExportSnapshot:
    """Trusted immutable inputs captured for one export sequence."""

    export_id: str
    export_sequence: int
    session_id: str
    learner_id: str
    repository_id: str
    repository_snapshot_digest: str
    freshness_overlay_digest: str
    as_of_event_sequence: int
    state: DialogueState


@dataclass(frozen=True, slots=True)
class ExportArtifacts:
    """Verified immutable JSON/Markdown pair awaiting domain completion."""

    export_id: str
    export_sequence: int
    session_id: str
    json_path: str
    json_digest: str
    markdown_path: str
    markdown_digest: str
    freshness_overlay_digest: str


def plan_export_intent(
    session_id: str,
    requested: ExportRequested,
) -> DurableEffectIntent:
    """Derive the one durable materialization intent for a request fact."""
    if (
        not is_protocol_id(session_id)
        or type(requested) is not ExportRequested
        or not is_protocol_id(requested.export_id)
        or not is_protocol_id(requested.intent_id)
        or type(requested.as_of_event_sequence) is not int
        or requested.as_of_event_sequence < 0
        or not is_sha256_digest(requested.payload_digest)
    ):
        raise ExportError("INVALID_EXPORT_REQUEST")
    return DurableEffectIntent(
        requested.intent_id,
        EffectKind.EXPORT_MATERIALIZATION,
        session_id,
        requested.export_id,
        requested.as_of_event_sequence,
        ("json", "markdown"),
        requested.payload_digest,
    )


def _topics(state: DialogueState) -> tuple[TopicRunState, ...]:
    return (
        state.completed_topics
        + state.paused_topics
        + (() if state.active_topic is None else (state.active_topic,))
    )


def _entry_tree(
    topic: TopicRunState,
    entry: LearnerModelEntry,
) -> dict[str, object]:
    if type(entry) is not LearnerModelEntry:
        raise ExportError("INVALID_EXPORT_SNAPSHOT")
    return {
        "entry_id": entry.entry_id,
        "topic_run_id": topic.topic_run_id,
        "kind": entry.kind.value,
        "status": entry.status.value,
        "provenance": entry.provenance.value,
        "statement": entry.statement,
        "turn_refs": list(entry.source_turn_ids),
        "evidence_refs": list(entry.evidence_refs),
    }


def build_export_document(snapshot: ExportSnapshot) -> dict[str, object]:
    """Build the canonical portable JSON document without transcript copying."""
    if type(snapshot) is not ExportSnapshot:
        raise ExportError("INVALID_EXPORT_SNAPSHOT")
    try:
        validate_state(snapshot.state)
    except ValueError as exc:
        raise ExportError("INVALID_EXPORT_SNAPSHOT") from exc
    if (
        not is_protocol_id(snapshot.export_id)
        or type(snapshot.export_sequence) is not int
        or snapshot.export_sequence < 1
        or not is_protocol_id(snapshot.session_id)
        or not is_protocol_id(snapshot.learner_id)
        or not is_protocol_id(snapshot.repository_id)
        or not is_sha256_digest(snapshot.repository_snapshot_digest)
        or not is_sha256_digest(snapshot.freshness_overlay_digest)
        or type(snapshot.as_of_event_sequence) is not int
        or snapshot.as_of_event_sequence < 0
        or snapshot.state.session_id != snapshot.session_id
        or snapshot.state.sequence != snapshot.as_of_event_sequence
    ):
        raise ExportError("INVALID_EXPORT_SNAPSHOT")

    topics = _topics(snapshot.state)
    entries = tuple(
        _entry_tree(topic, entry)
        for topic in topics
        for entry in topic.learner_model
    )
    topic_rows = tuple(
        {
            "topic_run_id": topic.topic_run_id,
            "title": topic.contract.title,
            "objective": topic.contract.objective,
            "lifecycle": topic.lifecycle.value,
            "lens": topic.lens.value,
            "evidence_health": topic.evidence_health.value,
            "takeaway": None if topic.summary is None else topic.summary.takeaway,
        }
        for topic in topics
    )
    question_types = sorted(
        {
            topic.current_agent_turn.question_intent.value
            for topic in topics
            if topic.current_agent_turn is not None
        }
    )
    unresolved = tuple(
        dict.fromkeys(
            question
            for topic in topics
            for question in (
                (() if topic.current_agent_turn is None else (
                    topic.current_agent_turn.question,
                ))
                + (() if topic.summary is None else topic.summary.open_questions)
            )
        )
    )
    next_suggestions = tuple(
        topic.summary.next_suggestion
        for topic in topics
        if topic.summary is not None and topic.summary.next_suggestion
    )
    evidence_refs = tuple(
        sorted(
            {
                ref
                for topic in topics
                for ref in (
                    topic.contract.evidence_refs
                    + tuple(
                        ref
                        for entry in topic.learner_model
                        for ref in entry.evidence_refs
                    )
                )
            }
        )
    )
    turn_refs = tuple(
        sorted(
            {
                turn
                for topic in topics
                for turn in (
                    topic.learner_turn_ids
                    + tuple(
                        turn
                        for entry in topic.learner_model
                        for turn in entry.source_turn_ids
                    )
                )
            }
        )
    )
    base: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "x_sync_insight_export",
        "protocol_version": PROTOCOL_VERSION,
        "export_id": snapshot.export_id,
        "export_sequence": snapshot.export_sequence,
        "repository_id": snapshot.repository_id,
        "learner_id": snapshot.learner_id,
        "session_id": snapshot.session_id,
        "as_of_event_sequence": snapshot.as_of_event_sequence,
        "repository_snapshot": {
            "digest": snapshot.repository_snapshot_digest,
            "freshness_overlay_digest": snapshot.freshness_overlay_digest,
        },
        "topics": list(topic_rows),
        "question_types": question_types,
        "technical_conclusions": [
            item for item in entries
            if item["kind"] == InsightKind.TECHNICAL_CONCLUSION.value
        ],
        "business_insights": [
            item for item in entries
            if item["kind"] == InsightKind.BUSINESS_INSIGHT.value
        ],
        "business_technical_mappings": [
            item for item in entries
            if item["kind"] == InsightKind.BUSINESS_TECHNICAL_MAPPING.value
        ],
        "confirmed_boundaries": [
            item for item in entries
            if item["kind"] == InsightKind.BOUNDARY.value
        ],
        "unresolved_questions": list(unresolved),
        "learner_current_model": list(entries),
        "hint_dependency": [],
        "next_suggestions": list(next_suggestions),
        "evidence_refs": list(evidence_refs),
        "turn_refs": list(turn_refs),
    }
    base["integrity_hash"] = sha256_digest(canonical_json_bytes(base))
    return base


def render_export(snapshot: ExportSnapshot) -> tuple[bytes, bytes]:
    """Render byte-stable JSON and Markdown from one trusted snapshot."""
    document = build_export_document(snapshot)
    json_bytes = canonical_json_bytes(document)
    lines = [
        f"# X-Sync 收获导出 {snapshot.export_sequence}",
        "",
        f"- Session: `{snapshot.session_id}`",
        f"- Repository: `{snapshot.repository_id}`",
        f"- As of event: `{snapshot.as_of_event_sequence}`",
        f"- Integrity: `{document['integrity_hash']}`",
        "",
        "## 话题",
        "",
    ]
    topics = cast(list[dict[str, object]], document["topics"])
    if not topics:
        lines.append("- 尚无话题成果。")
    for topic in topics:
        lines.append(
            f"- **{topic['title']}** ({topic['lifecycle']}): "
            f"{topic['takeaway'] or '尚无总结'}"
        )
    lines.extend(("", "## 当前理解", ""))
    entries = cast(list[dict[str, object]], document["learner_current_model"])
    if not entries:
        lines.append("- 尚无结构化 insight。")
    for entry in entries:
        lines.append(
            f"- [{entry['status']}] {entry['statement']} "
            f"(evidence: {', '.join(cast(list[str], entry['evidence_refs']))})"
        )
    lines.extend(("", "## 未解决问题", ""))
    unresolved = cast(list[str], document["unresolved_questions"])
    lines.extend(
        (f"- {question}" for question in unresolved)
        if unresolved
        else ("- 无。",)
    )
    markdown_bytes = ("\n".join(lines) + "\n").encode("utf-8")
    if len(json_bytes) > MAX_EXPORT_BYTES or len(markdown_bytes) > MAX_EXPORT_BYTES:
        raise ExportError("EXPORT_TOO_LARGE")
    return json_bytes, markdown_bytes


class ExportMaterializer:
    """Write verified immutable artifact pairs under one anchored root."""

    def __init__(self, exports_directory: SecureDirectory):
        if type(exports_directory) is not SecureDirectory:
            raise ExportError("INVALID_EXPORT_DIRECTORY")
        self._exports = exports_directory

    @staticmethod
    def _timestamp(value: str) -> str:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as exc:
            raise ExportError("INVALID_EXPORT_TIMESTAMP") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ExportError("INVALID_EXPORT_TIMESTAMP")
        return parsed.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")

    @staticmethod
    def _ensure_file(
        directory: SecureDirectory,
        component: str,
        payload: bytes,
    ) -> None:
        try:
            directory.write_immutable(component, payload)
        except SecureFsError as exc:
            if exc.code != "IMMUTABLE_EXISTS":
                raise ExportError(exc.code) from exc
            try:
                existing = directory.read_bytes(
                    component,
                    max_bytes=MAX_EXPORT_BYTES,
                )
            except SecureFsError as read_exc:
                raise ExportError(read_exc.code) from read_exc
            if existing != payload:
                raise ExportError("EXPORT_IDEMPOTENCY_CONFLICT") from exc

    def materialize(
        self,
        intent: DurableEffectIntent,
        snapshot: ExportSnapshot,
        requested_at: str,
    ) -> ExportArtifacts:
        """Create or verify the exact JSON/Markdown pair for one intent."""
        if (
            type(intent) is not DurableEffectIntent
            or intent.kind is not EffectKind.EXPORT_MATERIALIZATION
            or type(snapshot) is not ExportSnapshot
            or intent.session_id != snapshot.session_id
            or intent.export_id != snapshot.export_id
            or intent.as_of_sequence != snapshot.as_of_event_sequence
            or intent.formats != ("json", "markdown")
        ):
            raise ExportError("EXPORT_INTENT_MISMATCH")
        json_bytes, markdown_bytes = render_export(snapshot)
        timestamp = self._timestamp(requested_at)
        stem = f"insights.{timestamp}.{snapshot.export_id}"
        json_path = f"{stem}.json"
        markdown_path = f"{stem}.md"
        session_directory: SecureDirectory | None = None
        try:
            session_directory = self._exports.ensure_directory(snapshot.session_id)
            self._ensure_file(session_directory, json_path, json_bytes)
            self._ensure_file(session_directory, markdown_path, markdown_bytes)
            return ExportArtifacts(
                snapshot.export_id,
                snapshot.export_sequence,
                snapshot.session_id,
                json_path,
                sha256_digest(json_bytes),
                markdown_path,
                sha256_digest(markdown_bytes),
                snapshot.freshness_overlay_digest,
            )
        finally:
            if session_directory is not None:
                session_directory.close()

    def update_latest(self, session_id: str, completed: ExportRecord) -> None:
        """Replace the rebuildable pointer only from a completed domain fact."""
        if (
            not is_protocol_id(session_id)
            or type(completed) is not ExportRecord
            or completed.status is not ExportStatus.COMPLETED
            or completed.completed_at is None
            or completed.json_path is None
            or completed.json_digest is None
            or completed.markdown_path is None
            or completed.markdown_digest is None
            or completed.freshness_overlay_digest is None
        ):
            raise ExportError("EXPORT_NOT_COMPLETED")
        session_directory: SecureDirectory | None = None
        try:
            session_directory = self._exports.open_directory(session_id)
            try:
                json_bytes = session_directory.read_bytes(
                    completed.json_path,
                    max_bytes=MAX_EXPORT_BYTES,
                )
                markdown_bytes = session_directory.read_bytes(
                    completed.markdown_path,
                    max_bytes=MAX_EXPORT_BYTES,
                )
            except SecureFsError as exc:
                raise ExportError("EXPORT_ARTIFACT_MISSING") from exc
            if (
                sha256_digest(json_bytes) != completed.json_digest
                or sha256_digest(markdown_bytes) != completed.markdown_digest
            ):
                raise ExportError("EXPORT_ARTIFACT_HASH_MISMATCH")
            tree = {
                "schema_version": SCHEMA_VERSION,
                "record_type": "x_sync_latest_export",
                "export_id": completed.export_id,
                "export_sequence": completed.export_sequence,
                "json_path": completed.json_path,
                "json_digest": completed.json_digest,
                "markdown_path": completed.markdown_path,
                "markdown_digest": completed.markdown_digest,
                "freshness_overlay_digest": completed.freshness_overlay_digest,
            }
            pointer = canonical_json_bytes(tree)
            try:
                existing = session_directory.read_bytes(
                    "latest.json",
                    max_bytes=MAX_EXPORT_BYTES,
                )
            except SecureFsError as exc:
                if exc.code != "FILE_NOT_FOUND":
                    raise
            else:
                try:
                    decoded = json.loads(existing)
                    existing_sequence = decoded["export_sequence"]
                except (json.JSONDecodeError, KeyError, TypeError) as exc:
                    raise ExportError("INVALID_LATEST_EXPORT") from exc
                if (
                    type(decoded) is not dict
                    or frozenset(decoded) != _LATEST_KEYS
                    or decoded.get("schema_version") != SCHEMA_VERSION
                    or decoded.get("record_type") != "x_sync_latest_export"
                    or not is_protocol_id(decoded.get("export_id"))
                    or type(existing_sequence) is not int
                    or existing_sequence < 1
                    or not is_protocol_id(decoded.get("json_path"))
                    or not is_sha256_digest(decoded.get("json_digest"))
                    or not is_protocol_id(decoded.get("markdown_path"))
                    or not is_sha256_digest(decoded.get("markdown_digest"))
                    or not is_sha256_digest(
                        decoded.get("freshness_overlay_digest")
                    )
                    or canonical_json_bytes(decoded) != existing
                ):
                    raise ExportError("INVALID_LATEST_EXPORT")
                if existing_sequence > completed.export_sequence:
                    return
                if existing_sequence == completed.export_sequence:
                    if existing != pointer:
                        raise ExportError("EXPORT_IDEMPOTENCY_CONFLICT")
                    return
            session_directory.replace_derived("latest.json", pointer)
        except SecureFsError as exc:
            code = (
                "EXPORT_ARTIFACT_MISSING"
                if exc.code in {"DIRECTORY_NOT_FOUND", "FILE_NOT_FOUND"}
                else exc.code
            )
            raise ExportError(code) from exc
        finally:
            if session_directory is not None:
                session_directory.close()


def completion_command(
    command_id: str,
    intent: DurableEffectIntent,
    artifacts: ExportArtifacts,
    completed_at: str,
) -> CompleteExport:
    """Build the typed completion payload after artifact hash verification."""
    if (
        not is_protocol_id(command_id)
        or type(intent) is not DurableEffectIntent
        or type(artifacts) is not ExportArtifacts
        or intent.intent_id == ""
        or intent.session_id != artifacts.session_id
        or intent.export_id != artifacts.export_id
        or not is_protocol_id(artifacts.json_path)
        or not is_sha256_digest(artifacts.json_digest)
        or not is_protocol_id(artifacts.markdown_path)
        or not is_sha256_digest(artifacts.markdown_digest)
        or not is_sha256_digest(artifacts.freshness_overlay_digest)
        or type(completed_at) is not str
        or not completed_at.strip()
    ):
        raise ExportError("EXPORT_INTENT_MISMATCH")
    return CompleteExport(
        command_id,
        artifacts.export_id,
        intent.intent_id,
        completed_at,
        artifacts.json_path,
        artifacts.json_digest,
        artifacts.markdown_path,
        artifacts.markdown_digest,
        artifacts.freshness_overlay_digest,
    )
