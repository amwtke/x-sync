"""Crash-resumable application service for model-free dialogue exports."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from .coordinator import (
    CoordinatorError,
    DialogueCoordinator,
    DialogueExecutionRequest,
    DialogueResolution,
    DialogueSessionConfig,
)
from .domain import (
    CompleteExport,
    DecisionContext,
    DialogueState,
    EvidenceCheck,
    EvidenceHealth,
    ExportRecord,
    ExportStatus,
    RequestExport,
    initial_dialogue_state,
)
from .event_codec import (
    ActorKind,
    DialogueActor,
    DurableEffectIntent,
    canonical_json_bytes,
    sha256_digest,
)
from .event_store import DialogueStoreError, _DialogueTransactionLog
from .export import ExportArtifacts, ExportError, ExportMaterializer, ExportSnapshot
from .locking import DomainLockManager, LockError
from .secure_fs import SecureDirectory, SecureFsError
from .work_identity import is_protocol_id

Clock = Callable[[], str]
EvidenceVerifier = Callable[[DialogueSessionConfig], EvidenceCheck]


class ExportServiceError(RuntimeError):
    """Stable failure exposed by the export application boundary."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class ExportServiceRequest:
    """Authenticated Browser request for one deterministic export."""

    session_id: str
    idempotency_key: str
    expected_conversation_version: int
    actor_id: str = "browser"


@dataclass(frozen=True, slots=True)
class ExportServiceOutcome:
    """Completed canonical record and its verified immutable artifacts."""

    record: ExportRecord
    artifacts: ExportArtifacts
    replayed: bool


def _timestamp(value: object) -> bool:
    if type(value) is not str or not value or value != value.strip():
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _stable_id(prefix: str, session_id: str, key: str) -> str:
    material = f"{session_id}\0{key}".encode()
    return f"{prefix}.{hashlib.sha256(material).hexdigest()[:48]}"


class ExportService:
    """Request, materialize, complete, and recover exports without a model."""

    def __init__(
        self,
        coordinator: DialogueCoordinator,
        dialogues: SecureDirectory,
        locks: DomainLockManager,
        materializer: ExportMaterializer,
        evidence_verifier: EvidenceVerifier,
        *,
        clock: Clock,
    ) -> None:
        if (
            type(coordinator) is not DialogueCoordinator
            or type(dialogues) is not SecureDirectory
            or type(locks) is not DomainLockManager
            or type(materializer) is not ExportMaterializer
            or not callable(evidence_verifier)
            or not callable(clock)
        ):
            raise ExportServiceError("INVALID_EXPORT_SERVICE_CONFIGURATION")
        self._coordinator = coordinator
        self._dialogues = dialogues
        self._locks = locks
        self._materializer = materializer
        self._evidence_verifier = evidence_verifier
        self._clock = clock

    def request(self, request: ExportServiceRequest) -> ExportServiceOutcome:
        """Commit a request intent, execute it, and return exact replay output."""
        if (
            type(request) is not ExportServiceRequest
            or not is_protocol_id(request.session_id)
            or not is_protocol_id(request.idempotency_key)
            or type(request.expected_conversation_version) is not int
            or request.expected_conversation_version < 0
            or not is_protocol_id(request.actor_id)
        ):
            raise ExportServiceError("VALIDATION_FAILED")
        resolution = self._current(request.session_id)
        export_id = _stable_id("export", request.session_id, request.idempotency_key)
        intent_id = _stable_id(
            "export.intent",
            request.session_id,
            request.idempotency_key,
        )
        command_id = _stable_id(
            "export.request",
            request.session_id,
            request.idempotency_key,
        )
        payload_digest = sha256_digest(
            canonical_json_bytes(
                {
                    "actor_id": request.actor_id,
                    "export_id": export_id,
                    "intent_id": intent_id,
                    "learner_id": resolution.config.learner_id,
                    "repository_id": resolution.config.repository_id,
                    "session_id": request.session_id,
                }
            )
        )
        existing = tuple(
            item
            for item in resolution.dialogue_state.exports
            if item.export_id == export_id
        )
        if existing:
            if (
                len(existing) != 1
                or existing[0].intent_id != intent_id
                or existing[0].payload_digest != payload_digest
            ):
                raise ExportServiceError("IDEMPOTENCY_CONFLICT")
            return self._finish(request.session_id, export_id, True)
        occurred_at = self._now()
        evidence = self._verify(resolution.config)
        try:
            outcome = self._coordinator.execute(
                DialogueExecutionRequest(
                    request.session_id,
                    request.expected_conversation_version,
                    RequestExport(
                        command_id,
                        export_id,
                        intent_id,
                        occurred_at,
                        payload_digest,
                    ),
                    DecisionContext(
                        resolution.registry_state.generation,
                        None,
                        evidence,
                    ),
                    occurred_at,
                    DialogueActor(ActorKind.LEARNER, request.actor_id),
                )
            )
        except CoordinatorError as exc:
            raise ExportServiceError(exc.code) from exc
        return self._finish(
            request.session_id,
            export_id,
            outcome.replayed,
        )

    def recover_current(self) -> tuple[ExportServiceOutcome, ...]:
        """Resume all pending intents and rebuild latest for the current Session."""
        try:
            resolution = self._coordinator.recover()
        except CoordinatorError as exc:
            raise ExportServiceError(exc.code) from exc
        if resolution is None:
            return ()
        outcomes: list[ExportServiceOutcome] = []
        for record in resolution.dialogue_state.exports:
            if record.status is ExportStatus.REQUESTED:
                outcomes.append(
                    self._finish(
                        resolution.config.session_id,
                        record.export_id,
                        True,
                    )
                )
        latest = max(
            (
                item
                for item in self._current(
                    resolution.config.session_id
                ).dialogue_state.exports
                if item.status is ExportStatus.COMPLETED
            ),
            key=lambda item: item.export_sequence,
            default=None,
        )
        if latest is not None:
            try:
                self._materializer.update_latest(
                    resolution.config.session_id,
                    latest,
                )
            except ExportError as exc:
                raise ExportServiceError(exc.code) from exc
        return tuple(outcomes)

    def _finish(
        self,
        session_id: str,
        export_id: str,
        replayed: bool,
    ) -> ExportServiceOutcome:
        resolution = self._current(session_id)
        record = self._record(resolution, export_id)
        if record.status is ExportStatus.COMPLETED:
            artifacts = self._artifacts(session_id, record)
            try:
                self._materializer.update_latest(session_id, record)
            except ExportError as exc:
                raise ExportServiceError(exc.code) from exc
            return ExportServiceOutcome(record, artifacts, True)
        intent, state = self._intent_and_state(
            session_id,
            resolution.dialogue_state.registry_generation,
            record,
        )
        overlay_digest = sha256_digest(
            canonical_json_bytes(
                {
                    "evidence": self._verify(resolution.config).evidence_digest,
                    "overlay": [],
                }
            )
        )
        try:
            artifacts = self._materializer.materialize(
                intent,
                ExportSnapshot(
                    record.export_id,
                    record.export_sequence,
                    session_id,
                    resolution.config.learner_id,
                    resolution.config.repository_id,
                    resolution.config.evidence_digest,
                    overlay_digest,
                    record.as_of_event_sequence,
                    state,
                ),
                record.requested_at,
            )
        except ExportError as exc:
            raise ExportServiceError(exc.code) from exc
        current = self._current(session_id)
        completed_at = self._now()
        try:
            outcome = self._coordinator.execute(
                DialogueExecutionRequest(
                    session_id,
                    current.dialogue_state.conversation_version,
                    CompleteExport(
                        _stable_id("export.complete", session_id, record.intent_id),
                        record.export_id,
                        record.intent_id,
                        completed_at,
                        artifacts.json_path,
                        artifacts.json_digest,
                        artifacts.markdown_path,
                        artifacts.markdown_digest,
                        artifacts.freshness_overlay_digest,
                    ),
                    DecisionContext(
                        current.registry_state.generation,
                        None,
                        self._verify(current.config),
                    ),
                    completed_at,
                    DialogueActor(ActorKind.RUNTIME, "export-executor"),
                )
            )
        except CoordinatorError as exc:
            raise ExportServiceError(exc.code) from exc
        completed = self._record(
            DialogueResolution(
                current.registry_state,
                current.config,
                outcome.state,
                False,
                None,
            ),
            export_id,
        )
        try:
            self._materializer.update_latest(session_id, completed)
        except ExportError as exc:
            raise ExportServiceError(exc.code) from exc
        return ExportServiceOutcome(completed, artifacts, replayed or outcome.replayed)

    def _intent_and_state(
        self,
        session_id: str,
        generation: int,
        record: ExportRecord,
    ) -> tuple[DurableEffectIntent, DialogueState]:
        try:
            with self._locks.semantic_session(session_id) as authority:
                log = _DialogueTransactionLog.open_existing(
                    self._dialogues,
                    initial_dialogue_state(session_id, generation),
                    self._locks,
                    authority,
                )
                try:
                    matches = tuple(
                        item
                        for item in log.read_pending_effect_intents()
                        if item.intent_id == record.intent_id
                    )
                    if len(matches) != 1:
                        raise ExportServiceError("EXPORT_INTENT_MISSING")
                    state = log.state_at_sequence(record.as_of_event_sequence)
                finally:
                    log.close()
        except (DialogueStoreError, LockError, SecureFsError) as exc:
            raise ExportServiceError(getattr(exc, "code", str(exc))) from exc
        return matches[0], state

    @staticmethod
    def _record(
        resolution: DialogueResolution,
        export_id: str,
    ) -> ExportRecord:
        matches = tuple(
            item
            for item in resolution.dialogue_state.exports
            if item.export_id == export_id
        )
        if len(matches) != 1:
            raise ExportServiceError("EXPORT_NOT_FOUND")
        return matches[0]

    @staticmethod
    def _artifacts(session_id: str, record: ExportRecord) -> ExportArtifacts:
        if (
            record.status is not ExportStatus.COMPLETED
            or record.json_path is None
            or record.json_digest is None
            or record.markdown_path is None
            or record.markdown_digest is None
            or record.freshness_overlay_digest is None
        ):
            raise ExportServiceError("EXPORT_NOT_COMPLETED")
        return ExportArtifacts(
            record.export_id,
            record.export_sequence,
            session_id,
            record.json_path,
            record.json_digest,
            record.markdown_path,
            record.markdown_digest,
            record.freshness_overlay_digest,
        )

    def _current(self, session_id: str) -> DialogueResolution:
        try:
            resolution = self._coordinator.recover()
        except CoordinatorError as exc:
            raise ExportServiceError(exc.code) from exc
        if resolution is None or resolution.config.session_id != session_id:
            raise ExportServiceError("SESSION_DEACTIVATED")
        return resolution

    def _verify(self, config: DialogueSessionConfig) -> EvidenceCheck:
        try:
            result = self._evidence_verifier(config)
        except Exception as exc:
            raise ExportServiceError("EVIDENCE_VERIFICATION_FAILED") from exc
        if (
            type(result) is not EvidenceCheck
            or type(result.health) is not EvidenceHealth
            or result.evidence_digest != config.evidence_digest
        ):
            raise ExportServiceError("EVIDENCE_VERIFICATION_FAILED")
        return result

    def _now(self) -> str:
        try:
            value = self._clock()
        except Exception as exc:
            raise ExportServiceError("INVALID_EXPORT_CLOCK") from exc
        if not _timestamp(value):
            raise ExportServiceError("INVALID_EXPORT_CLOCK")
        return value
