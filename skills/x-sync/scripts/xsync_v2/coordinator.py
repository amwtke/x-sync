"""Crash-resumable application service for X-Sync dialogue activation.

The dialogue and registry modules deliberately contain pure aggregate logic.
This module is the small application layer which composes those aggregates
with their marker-linearized stores.  It owns no process-global state: every
public operation replays the durable registry while holding the registry lock,
so a different process can finish a handoff abandoned by its predecessor.
"""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from .dispatch import (
    AfterCommitDispatcher,
    AfterCommitReport,
    CommittedFactCollector,
)
from .domain import (
    Accepted,
    CommitAgentTurn,
    CompleteTopic,
    CommittedDialogueEvent,
    ConversationPhase,
    DecisionContext,
    DialogueCommand,
    DialogueState,
    EvidenceCheck,
    EvidenceHealth,
    ExportRequested,
    FencedQuiesceContext,
    PauseCause,
    PendingDialogueEvent,
    PrepareSessionDeactivation,
    PresentCandidates,
    Rejected,
    ReportWorkFailure,
    RequestTopicClarification,
    SessionDeactivationPrepared,
    SessionLifecycle,
    StartSession,
    StartTopic,
    TopicPaused,
    TriggerBinding,
    TriggerKind,
    initial_dialogue_state,
)
from .event_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    ActorKind,
    DialogueActor,
    DialogueWriteRequestRecord,
    canonical_json_bytes,
    dialogue_request_digest,
    dialogue_state_digest,
    sha256_digest,
)
from .event_store import (
    DialogueCommitOutcome,
    DialogueCommitRequest,
    DialogueStoreError,
    DialogueTip,
    EventMetadata,
    MarkerPublicationGuard,
    _DialogueTransactionLog,
    session_directory_component,
)
from .export import plan_export_intent
from .locking import (
    DomainLockManager,
    LockError,
    RegistryLockAuthority,
    SessionLockAuthority,
)
from .registry import (
    Activated,
    ActivateTarget,
    BeginHandoff,
    CommittedRegistryEvent,
    CompleteHandoff,
    CreateAndActivateDialogue,
    Deactivated,
    DialogueCreated,
    DialogueRegistrationStatus,
    PendingHandoff,
    QuiesceProof,
    RegisteredDialogue,
    RegistryAccepted,
    RegistryIdempotent,
    RegistryRejected,
    RegistryState,
    initial_registry_state,
)
from .registry import (
    decide as decide_registry,
)
from .registry_store import (
    RegistryCommitOutcome,
    RegistryCommitRequest,
    RegistryStoreError,
    _RegistryTransactionLog,
)
from .secure_fs import SecureDirectory, SecureFsError
from .state_machine import (
    conversation_version_delta,
    decide,
    decide_fenced_quiesce,
)


class CoordinatorError(RuntimeError):
    """A stable, path-free application-service failure."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class DialogueSessionConfig:
    """Immutable resolved inputs for one continuous browser conversation."""

    session_id: str
    learner_id: str
    repository_id: str
    created_at: str
    runtime_epoch: str
    evidence_digest: str
    task_scope: str = "repository onboarding"
    language: str = "zh-CN"
    channel: str = "web"
    style: str = "socratic"
    focus: str = "mixed"


@dataclass(frozen=True, slots=True)
class DialogueExecutionRequest:
    """One optimistic, idempotent normal dialogue command."""

    session_id: str
    expected_conversation_version: int
    command: DialogueCommand
    context: DecisionContext
    occurred_at: str
    actor: DialogueActor


@dataclass(frozen=True, slots=True)
class DialogueResolution:
    """The durable current-session view returned by resolve or recovery."""

    registry_state: RegistryState
    config: DialogueSessionConfig
    dialogue_state: DialogueState
    created: bool
    transitioned_from: str | None


FaultHook = Callable[[str], None]
EvidenceVerifier = Callable[[DialogueSessionConfig], EvidenceCheck]


def _no_fault(_stage: str) -> None:
    return None


def _note_after_commit_audit_failure(original: BaseException) -> None:
    """Attach only a stable note and never replace the commit exception."""
    try:
        original.add_note("AFTER_COMMIT_AUDIT_FAILED")
    except BaseException:
        return


_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
_CONFIG_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "protocol_version",
        "session_id",
        "learner_id",
        "repository_id",
        "created_at",
        "runtime_epoch",
        "evidence_digest",
        "task_scope",
        "language",
        "channel",
        "style",
        "focus",
    }
)
_CONFIG_MAX_BYTES = 64 * 1024
_RUNTIME_ACTOR = DialogueActor(ActorKind.RUNTIME, "x-sync.coordinator")


def _nonblank(value: object, *, max_bytes: int = 1024) -> bool:
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


def _validate_config(config: object) -> DialogueSessionConfig:
    if type(config) is not DialogueSessionConfig:
        raise CoordinatorError("INVALID_SESSION_CONFIG")
    if (
        _ID_PATTERN.fullmatch(config.session_id) is None
        or not _nonblank(config.learner_id)
        or not _nonblank(config.repository_id)
        or not _valid_timestamp(config.created_at)
        or not _nonblank(config.runtime_epoch)
        or _DIGEST_PATTERN.fullmatch(config.evidence_digest) is None
        or not _nonblank(config.task_scope, max_bytes=16 * 1024)
        or not _nonblank(config.language, max_bytes=64)
        or config.channel not in {"web", "terminal"}
        or config.style not in {"socratic", "regular"}
        or config.focus not in {"business", "technical", "mixed"}
    ):
        raise CoordinatorError("INVALID_SESSION_CONFIG")
    return config


def _config_tree(config: DialogueSessionConfig) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "record_type": "dialogue_session_config",
        "protocol_version": PROTOCOL_VERSION,
        "session_id": config.session_id,
        "learner_id": config.learner_id,
        "repository_id": config.repository_id,
        "created_at": config.created_at,
        "runtime_epoch": config.runtime_epoch,
        "evidence_digest": config.evidence_digest,
        "task_scope": config.task_scope,
        "language": config.language,
        "channel": config.channel,
        "style": config.style,
        "focus": config.focus,
    }


def encode_session_config(config: DialogueSessionConfig) -> bytes:
    """Return the single canonical immutable representation of a config."""
    return canonical_json_bytes(_config_tree(_validate_config(config)))


def session_config_digest(config: DialogueSessionConfig) -> str:
    """Hash the exact bytes persisted as ``config.json``."""
    return sha256_digest(encode_session_config(config))


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("DUPLICATE_CONFIG_KEY")
        result[key] = value
    return result


def decode_session_config(raw: bytes) -> DialogueSessionConfig:
    """Decode only an exact canonical v2 session-config record."""
    if type(raw) is not bytes or not raw or len(raw) > _CONFIG_MAX_BYTES:
        raise CoordinatorError("INVALID_SESSION_CONFIG_RECORD")
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("INVALID_JSON_CONSTANT")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise CoordinatorError("INVALID_SESSION_CONFIG_RECORD") from exc
    if (
        type(value) is not dict
        or frozenset(value) != _CONFIG_KEYS
        or value.get("schema_version") != SCHEMA_VERSION
        or value.get("record_type") != "dialogue_session_config"
        or value.get("protocol_version") != PROTOCOL_VERSION
    ):
        raise CoordinatorError("INVALID_SESSION_CONFIG_RECORD")
    try:
        config = DialogueSessionConfig(
            session_id=value["session_id"],
            learner_id=value["learner_id"],
            repository_id=value["repository_id"],
            created_at=value["created_at"],
            runtime_epoch=value["runtime_epoch"],
            evidence_digest=value["evidence_digest"],
            task_scope=value["task_scope"],
            language=value["language"],
            channel=value["channel"],
            style=value["style"],
            focus=value["focus"],
        )
        _validate_config(config)
        if canonical_json_bytes(value) != raw or encode_session_config(config) != raw:
            raise CoordinatorError("INVALID_SESSION_CONFIG_RECORD")
    except (KeyError, TypeError, CoordinatorError) as exc:
        if isinstance(exc, CoordinatorError):
            raise CoordinatorError("INVALID_SESSION_CONFIG_RECORD") from exc
        raise CoordinatorError("INVALID_SESSION_CONFIG_RECORD") from exc
    return config


def _seed_digest(value: object) -> str:
    return sha256_digest(canonical_json_bytes(value))


def _stable_id(prefix: str, digest: str, index: int | None = None) -> str:
    suffix = digest.removeprefix("sha256:")[:48]
    result = f"{prefix}.{suffix}"
    if index is not None:
        result = f"{result}.{index}"
    if _ID_PATTERN.fullmatch(result) is None:
        raise CoordinatorError("IDENTITY_DERIVATION_FAILED")
    return result


def _registration(
    state: RegistryState, session_id: str
) -> RegisteredDialogue | None:
    return next(
        (item for item in state.dialogues if item.session_id == session_id),
        None,
    )


class DialogueCoordinator:
    """Coordinate create/resume, normal commands, and fenced A-to-B handoff."""

    def __init__(
        self,
        dialogues_directory: SecureDirectory,
        locks: DomainLockManager,
        registry_id: str,
        *,
        evidence_verifier: EvidenceVerifier,
        after_commit_dispatcher: AfterCommitDispatcher | None = None,
        fault_hook: FaultHook = _no_fault,
    ) -> None:
        if (
            type(dialogues_directory) is not SecureDirectory
            or type(locks) is not DomainLockManager
            or _ID_PATTERN.fullmatch(registry_id) is None
            or not callable(evidence_verifier)
            or (
                after_commit_dispatcher is not None
                and type(after_commit_dispatcher) is not AfterCommitDispatcher
            )
            or not callable(fault_hook)
        ):
            raise CoordinatorError("INVALID_COORDINATOR_CONFIGURATION")
        self._dialogues = dialogues_directory
        self._locks = locks
        self._registry_id = registry_id
        self._evidence_verifier = evidence_verifier
        self._after_commit_dispatcher = after_commit_dispatcher
        self._fault_hook = fault_hook
        self._catch_up_lock = threading.Lock()
        self._catch_up_state_lock = threading.Lock()
        self._catch_up_invalidation_generation = 0
        self._catch_up_complete = False

    def _invalidate_durable_catch_up(self) -> None:
        """Atomically make every scan already in flight stale."""
        with self._catch_up_state_lock:
            self._catch_up_invalidation_generation += 1
            self._catch_up_complete = False

    def _start_operation(self) -> CommittedFactCollector:
        dispatcher = self._after_commit_dispatcher
        if dispatcher is not None:
            dispatcher.assert_command_entry_allowed()
            self._ensure_durable_catch_up()
        return CommittedFactCollector()

    def _finish_operation(
        self,
        collector: CommittedFactCollector,
    ) -> AfterCommitReport | None:
        dispatcher = self._after_commit_dispatcher
        if dispatcher is not None:
            return dispatcher.publish_facts(collector.freeze())
        return None

    def _ensure_durable_catch_up(self) -> AfterCommitReport | None:
        """Catch up a fresh dispatcher before semantic command ingress."""
        return self._durable_catch_up(force=False)

    def _durable_catch_up(
        self,
        *,
        force: bool,
    ) -> AfterCommitReport | None:
        if type(force) is not bool:
            raise CoordinatorError("INVALID_CATCH_UP_MODE")
        with self._catch_up_lock:
            dispatcher = self._after_commit_dispatcher
            with self._catch_up_state_lock:
                already_complete = self._catch_up_complete
                scan_generation = self._catch_up_invalidation_generation
                if force or not already_complete:
                    self._catch_up_complete = False
            if already_complete and not force:
                if dispatcher is not None:
                    return dispatcher.publish_all(())
                return None
            collector = CommittedFactCollector()
            completed = False
            report: AfterCommitReport | None = None
            try:
                self._collect_durable_facts(collector)
                completed = True
            except CoordinatorError:
                raise
            except (
                DialogueStoreError,
                RegistryStoreError,
                SecureFsError,
                LockError,
                ValueError,
            ) as exc:
                raise CoordinatorError(getattr(exc, "code", str(exc))) from exc
            finally:
                report = self._finish_operation(collector)
                if completed:
                    with self._catch_up_state_lock:
                        if (
                            self._catch_up_invalidation_generation
                            == scan_generation
                        ):
                            self._catch_up_complete = True
            return report

    def _collect_durable_facts(
        self,
        collector: CommittedFactCollector,
    ) -> None:
        self._recover(collector)
        with self._locks.registry_exclusive() as registry_authority:
            registry_log = self._open_registry(registry_authority)
            try:
                registry_events = registry_log.read_committed(after_sequence=0)
                collector.capture_registry_events(
                    self._registry_id,
                    registry_events,
                )
                registry_state = registry_log.tip().state
                proofs = self._deactivation_proofs(registry_events)
                for registration in sorted(
                    registry_state.dialogues,
                    key=lambda item: item.session_id,
                ):
                    with self._locks.session_exclusive(
                        registration.session_id,
                        registry_authority,
                    ) as session_authority:
                        self._validate_existing_registration(registration)
                        generation = registration.activated_generation
                        if generation is None:
                            continue
                        dialogue_log = self._open_existing_dialogue(
                            registration.session_id,
                            generation,
                            session_authority,
                        )
                        try:
                            events = dialogue_log.read_committed(after_sequence=0)
                            if (
                                registration.status
                                is DialogueRegistrationStatus.DEACTIVATED
                            ):
                                proof = proofs.get(registration.session_id)
                                self._validate_historical_proof(
                                    registration,
                                    dialogue_log.tip(),
                                    proof,
                                )
                            collector.capture_dialogue_events(
                                registration.session_id,
                                events,
                            )
                        finally:
                            dialogue_log.close()
            finally:
                registry_log.close()

    @staticmethod
    def _deactivation_proofs(
        events: tuple[CommittedRegistryEvent, ...],
    ) -> dict[str, QuiesceProof]:
        proofs: dict[str, QuiesceProof] = {}
        for event in events:
            payload = event.payload
            if type(payload) is not Deactivated:
                continue
            proof = payload.proof
            if proof.source_session_id in proofs:
                raise CoordinatorError("HISTORICAL_DIALOGUE_INTEGRITY")
            proofs[proof.source_session_id] = proof
        return proofs

    def _validate_existing_registration(
        self,
        registration: RegisteredDialogue,
    ) -> None:
        try:
            self._load_config_checked(
                registration.session_id,
                registration.config_digest,
            )
            session = self._dialogues.open_directory(
                session_directory_component(registration.session_id)
            )
            try:
                for component in ("events", "transactions", "quarantine"):
                    child = session.open_directory(component)
                    child.close()
            finally:
                session.close()
        except SecureFsError as exc:
            if exc.code in {"DIRECTORY_NOT_FOUND", "FILE_NOT_FOUND"}:
                raise CoordinatorError("HISTORICAL_DIALOGUE_MISSING") from exc
            raise

    @staticmethod
    def _validate_historical_proof(
        registration: RegisteredDialogue,
        tip: DialogueTip,
        proof: QuiesceProof | None,
    ) -> None:
        if (
            proof is None
            or proof.source_session_id != registration.session_id
            or proof.generation != registration.deactivated_generation
            or proof.handoff_id != registration.deactivation_handoff_id
            or tip.state.sequence != proof.dialogue_last_sequence
            or tip.last_event_id != proof.dialogue_last_event_id
            or tip.last_event_hash != proof.dialogue_event_digest
            or tip.last_marker_hash != proof.dialogue_transaction_digest
            or dialogue_state_digest(tip.state) != proof.dialogue_state_digest
        ):
            raise CoordinatorError("HISTORICAL_DIALOGUE_INTEGRITY")

    def replay_committed(self) -> AfterCommitReport | None:
        """Replay every durable Registry and activated Dialogue stream.

        Runtime wiring calls this before opening semantic command ingress.  It
        is intentionally independent of in-memory commit capture so a process
        crash after marker publication cannot lose Observer delivery.
        """
        dispatcher = self._after_commit_dispatcher
        if dispatcher is not None:
            dispatcher.assert_command_entry_allowed()
        return self._durable_catch_up(force=True)

    def resolve(self, config: DialogueSessionConfig) -> DialogueResolution:
        """Create, resume, or hand off to exactly ``config.session_id``."""
        collector = self._start_operation()
        try:
            return self._resolve(config, collector)
        finally:
            self._finish_operation(collector)

    def _resolve(
        self,
        config: DialogueSessionConfig,
        collector: CommittedFactCollector,
    ) -> DialogueResolution:
        config = _validate_config(config)
        requested_digest = session_config_digest(config)
        try:
            with self._locks.registry_exclusive() as registry_authority:
                registry_log = self._open_registry(registry_authority)
                try:
                    had_pending = registry_log.tip().state.pending_handoff
                    if had_pending is not None:
                        self._continue_handoff(
                            registry_log,
                            registry_authority,
                            collector,
                        )
                        if (
                            config.session_id != had_pending.target.session_id
                            or requested_digest != had_pending.target.config_digest
                        ):
                            raise CoordinatorError("HANDOFF_TARGET_CONFLICT")

                    state = registry_log.tip().state
                    current_id = state.current_session_id
                    if current_id is None:
                        self._require_publishable_evidence(config)
                        self._prepare_empty_target(
                            config,
                            state.generation + 1,
                            registry_authority,
                        )
                        self._commit_registry_create(
                            registry_log,
                            config,
                            registry_authority,
                            collector,
                        )
                        self._fault_hook("registry_created")
                        dialogue_state = self._bootstrap_current(
                            registry_log.tip().state,
                            config,
                            registry_authority,
                            collector,
                        )
                        return DialogueResolution(
                            registry_log.tip().state,
                            config,
                            dialogue_state,
                            True,
                            None,
                        )

                    if current_id == config.session_id:
                        current = _registration(state, current_id)
                        if (
                            current is None
                            or current.status
                            is not DialogueRegistrationStatus.ACTIVE
                            or current.config_digest != requested_digest
                        ):
                            raise CoordinatorError("SESSION_CONFIG_CONFLICT")
                        stored = self._load_config(config.session_id)
                        if stored != config:
                            raise CoordinatorError("SESSION_CONFIG_CONFLICT")
                        dialogue_state = self._bootstrap_current(
                            state,
                            stored,
                            registry_authority,
                            collector,
                        )
                        return DialogueResolution(
                            state,
                            stored,
                            dialogue_state,
                            False,
                            None,
                        )

                    source_id = current_id
                    source = _registration(state, source_id)
                    if source is None:
                        raise CoordinatorError("REGISTRY_STATE_CONFLICT")
                    source_config = self._load_config_checked(
                        source_id,
                        source.config_digest,
                    )
                    self._require_publishable_evidence(config)
                    self._bootstrap_current(
                        state,
                        source_config,
                        registry_authority,
                        collector,
                    )
                    self._prepare_empty_target(
                        config,
                        state.generation + 1,
                        registry_authority,
                    )
                    self._commit_registry_begin(
                        registry_log,
                        config,
                        registry_authority,
                        collector,
                    )
                    self._fault_hook("handoff_started")
                    self._continue_handoff(
                        registry_log,
                        registry_authority,
                        collector,
                    )
                    dialogue_state = self._bootstrap_current(
                        registry_log.tip().state,
                        config,
                        registry_authority,
                        collector,
                    )
                    return DialogueResolution(
                        registry_log.tip().state,
                        config,
                        dialogue_state,
                        True,
                        source_id,
                    )
                finally:
                    registry_log.close()
        except CoordinatorError:
            raise
        except (
            DialogueStoreError,
            RegistryStoreError,
            SecureFsError,
            LockError,
            ValueError,
        ) as exc:
            raise CoordinatorError(getattr(exc, "code", str(exc))) from exc

    def recover(self) -> DialogueResolution | None:
        """Finish a durable pending handoff and bootstrap the current target."""
        collector = self._start_operation()
        try:
            return self._recover(collector)
        finally:
            self._finish_operation(collector)

    def _recover(
        self,
        collector: CommittedFactCollector,
    ) -> DialogueResolution | None:
        try:
            with self._locks.registry_exclusive() as registry_authority:
                registry_log = self._open_registry(registry_authority)
                try:
                    registry_events = registry_log.read_committed(
                        after_sequence=0
                    )
                    state = registry_log.tip().state
                    self._preflight_recovery_dialogues(
                        state,
                        registry_events,
                        registry_authority,
                    )
                    pending = state.pending_handoff
                    transitioned_from = (
                        pending.source_session_id if pending is not None else None
                    )
                    if pending is not None:
                        self._continue_handoff(
                            registry_log,
                            registry_authority,
                            collector,
                        )
                    state = registry_log.tip().state
                    if state.current_session_id is None:
                        return None
                    config = self._load_config(state.current_session_id)
                    dialogue_state = self._bootstrap_current(
                        state,
                        config,
                        registry_authority,
                        collector,
                    )
                    return DialogueResolution(
                        state,
                        config,
                        dialogue_state,
                        False,
                        transitioned_from,
                    )
                finally:
                    registry_log.close()
        except CoordinatorError:
            raise
        except (
            DialogueStoreError,
            RegistryStoreError,
            SecureFsError,
            LockError,
            ValueError,
        ) as exc:
            raise CoordinatorError(getattr(exc, "code", str(exc))) from exc

    def _preflight_recovery_dialogues(
        self,
        registry_state: RegistryState,
        registry_events: tuple[CommittedRegistryEvent, ...],
        registry_authority: RegistryLockAuthority,
    ) -> None:
        """Read-only audit every registered Session before recovery mutates it."""
        proofs = self._deactivation_proofs(registry_events)
        for registration in sorted(
            registry_state.dialogues,
            key=lambda item: item.session_id,
        ):
            self._validate_existing_registration(registration)
            generation = registration.activated_generation
            if generation is None:
                pending = registry_state.pending_handoff
                if (
                    registration.status
                    is not DialogueRegistrationStatus.CREATED
                    or pending is None
                    or pending.target.session_id != registration.session_id
                    or pending.target.config_digest != registration.config_digest
                ):
                    raise CoordinatorError("HISTORICAL_DIALOGUE_INTEGRITY")
                generation = pending.generation
            with self._locks.session_exclusive(
                registration.session_id,
                registry_authority,
            ) as session_authority:
                dialogue_log = self._open_existing_dialogue(
                    registration.session_id,
                    generation,
                    session_authority,
                )
                try:
                    tip = dialogue_log.tip()
                    if (
                        registration.status
                        is DialogueRegistrationStatus.CREATED
                        and tip.state.sequence != 0
                    ):
                        raise CoordinatorError(
                            "HISTORICAL_DIALOGUE_INTEGRITY"
                        )
                    if tip.state.sequence == 0:
                        self._validate_new_dialogue_registry_proof(
                            registry_state,
                            registry_events,
                            registration,
                        )
                    elif (
                        registration.status
                        is DialogueRegistrationStatus.DEACTIVATED
                    ):
                        self._validate_historical_proof(
                            registration,
                            tip,
                            proofs.get(registration.session_id),
                        )
                finally:
                    dialogue_log.close()

    @staticmethod
    def _validate_new_dialogue_registry_proof(
        registry_state: RegistryState,
        registry_events: tuple[CommittedRegistryEvent, ...],
        registration: RegisteredDialogue,
    ) -> None:
        """Prove a zero-tip Session is the Registry's explicit new target."""
        target = ActivateTarget(
            registration.session_id,
            registration.config_digest,
        )
        created = tuple(
            event
            for event in registry_events
            if event.registry_sequence == registration.created_registry_sequence
            and type(event.payload) is DialogueCreated
            and event.payload.target == target
        )
        if len(created) != 1 or not DialogueCoordinator._event_is_receipted(
            registry_state,
            created[0],
        ):
            raise CoordinatorError("HISTORICAL_DIALOGUE_INTEGRITY")
        if registration.status is DialogueRegistrationStatus.CREATED:
            return
        generation = registration.activated_generation
        activated = tuple(
            event
            for event in registry_events
            if type(event.payload) is Activated
            and event.payload.target == target
            and event.payload.generation == generation
        )
        pending = registry_state.pending_handoff
        active_context = (
            registration.status is DialogueRegistrationStatus.ACTIVE
            and registry_state.current_session_id == registration.session_id
            and generation == registry_state.generation
        )
        legacy_new_source_context = (
            registration.status is DialogueRegistrationStatus.DEACTIVATING
            and pending is not None
            and pending.source_session_id == registration.session_id
            and pending.generation == registry_state.generation
            and generation is not None
            and generation < pending.generation
        )
        if (
            (not active_context and not legacy_new_source_context)
            or len(activated) != 1
            or not DialogueCoordinator._event_is_receipted(
                registry_state,
                activated[0],
            )
        ):
            raise CoordinatorError("HISTORICAL_DIALOGUE_INTEGRITY")

    @staticmethod
    def _event_is_receipted(
        registry_state: RegistryState,
        event: CommittedRegistryEvent,
    ) -> bool:
        for receipt in registry_state.receipts:
            if (
                receipt.command_id != event.command_id
                or receipt.body_digest != event.body_digest
                or not (
                    receipt.first_registry_sequence
                    <= event.registry_sequence
                    <= receipt.last_registry_sequence
                )
            ):
                continue
            offset = event.registry_sequence - receipt.first_registry_sequence
            if (
                offset < len(receipt.event_ids)
                and receipt.event_ids[offset] == event.event_id
                and receipt.event_digests[offset] == event.event_digest
            ):
                return True
        return False

    def execute(
        self, request: DialogueExecutionRequest
    ) -> DialogueCommitOutcome:
        """Execute one normal command only against the unfenced current Session."""
        self._validate_execution(request)
        if type(request.command) in {
            PresentCandidates,
            StartTopic,
            RequestTopicClarification,
            CommitAgentTurn,
            CompleteTopic,
            ReportWorkFailure,
        }:
            raise CoordinatorError("HOST_WORK_SERVICE_REQUIRED")
        collector = self._start_operation()
        try:
            return self._execute(request, collector)
        finally:
            self._finish_operation(collector)

    def _execute(
        self,
        request: DialogueExecutionRequest,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome:
        self._validate_execution(request)
        try:
            with self._locks.registry_exclusive() as registry_authority:
                registry_log = self._open_registry(registry_authority)
                try:
                    if registry_log.tip().state.pending_handoff is not None:
                        self._continue_handoff(
                            registry_log,
                            registry_authority,
                            collector,
                        )
                    registry_state = registry_log.tip().state
                    registration = _registration(
                        registry_state, request.session_id
                    )
                    if (
                        registry_state.current_session_id != request.session_id
                        or registry_state.pending_handoff is not None
                        or registration is None
                        or registration.status
                        is not DialogueRegistrationStatus.ACTIVE
                        or registration.activated_generation
                        != registry_state.generation
                    ):
                        raise CoordinatorError("SESSION_DEACTIVATED")
                    current_config = self._load_config_checked(
                        request.session_id,
                        registration.config_digest,
                    )
                    self._bootstrap_current(
                        registry_state,
                        current_config,
                        registry_authority,
                        collector,
                    )
                    with self._locks.session_exclusive(
                        request.session_id, registry_authority
                    ) as session_authority:
                        dialogue_log = self._open_existing_dialogue(
                            request.session_id,
                            registry_state.generation,
                            session_authority,
                        )
                        try:
                            outcome = self._execute_locked(
                                dialogue_log,
                                session_authority,
                                request,
                                current_config,
                                collector,
                            )
                            return outcome
                        finally:
                            dialogue_log.close()
                finally:
                    registry_log.close()
        except CoordinatorError:
            raise
        except (
            DialogueStoreError,
            RegistryStoreError,
            SecureFsError,
            LockError,
            ValueError,
        ) as exc:
            raise CoordinatorError(getattr(exc, "code", str(exc))) from exc

    def _open_registry(
        self, authority: RegistryLockAuthority
    ) -> _RegistryTransactionLog:
        return _RegistryTransactionLog.create(
            self._dialogues,
            initial_registry_state(self._registry_id),
            self._locks,
            authority,
        )

    def _open_dialogue(
        self,
        session_id: str,
        generation: int,
        authority: SessionLockAuthority,
    ) -> _DialogueTransactionLog:
        return _DialogueTransactionLog.create(
            self._dialogues,
            initial_dialogue_state(session_id, generation),
            self._locks,
            authority,
        )

    def _open_existing_dialogue(
        self,
        session_id: str,
        generation: int,
        authority: SessionLockAuthority,
    ) -> _DialogueTransactionLog:
        try:
            return _DialogueTransactionLog.open_existing(
                self._dialogues,
                initial_dialogue_state(session_id, generation),
                self._locks,
                authority,
            )
        except DialogueStoreError as exc:
            if exc.code in {"DIRECTORY_NOT_FOUND", "FILE_NOT_FOUND"}:
                raise CoordinatorError("HISTORICAL_DIALOGUE_MISSING") from exc
            raise

    def _prepare_empty_target(
        self,
        config: DialogueSessionConfig,
        generation: int,
        registry_authority: RegistryLockAuthority,
    ) -> None:
        with self._locks.session_exclusive(
            config.session_id, registry_authority
        ) as session_authority:
            self._persist_config(config)
            dialogue_log = self._open_dialogue(
                config.session_id, generation, session_authority
            )
            try:
                if dialogue_log.tip().state.sequence != 0:
                    raise CoordinatorError("TARGET_DIALOGUE_NOT_EMPTY")
            finally:
                dialogue_log.close()
        self._fault_hook("config_persisted")

    def _persist_config(self, config: DialogueSessionConfig) -> None:
        raw = encode_session_config(config)
        session = self._dialogues.ensure_directory(
            session_directory_component(config.session_id)
        )
        try:
            try:
                session.write_immutable("config.json", raw)
            except SecureFsError as exc:
                if exc.code != "IMMUTABLE_EXISTS":
                    raise
                existing = session.read_bytes(
                    "config.json", max_bytes=_CONFIG_MAX_BYTES
                )
                if existing != raw:
                    raise CoordinatorError("SESSION_CONFIG_CONFLICT") from exc
        finally:
            session.close()

    def _load_config(self, session_id: str) -> DialogueSessionConfig:
        session = self._dialogues.open_directory(
            session_directory_component(session_id)
        )
        try:
            raw = session.read_bytes("config.json", max_bytes=_CONFIG_MAX_BYTES)
        finally:
            session.close()
        config = decode_session_config(raw)
        if config.session_id != session_id:
            raise CoordinatorError("SESSION_CONFIG_CONFLICT")
        return config

    def _load_config_checked(
        self, session_id: str, expected_digest: str
    ) -> DialogueSessionConfig:
        config = self._load_config(session_id)
        if session_config_digest(config) != expected_digest:
            raise CoordinatorError("SESSION_CONFIG_CONFLICT")
        return config

    def _commit_registry_create(
        self,
        log: _RegistryTransactionLog,
        config: DialogueSessionConfig,
        authority: RegistryLockAuthority,
        collector: CommittedFactCollector,
    ) -> None:
        state = log.tip().state
        target = ActivateTarget(config.session_id, session_config_digest(config))
        seed = {
            "operation": "create_and_activate",
            "registry_id": state.registry_id,
            "target": {
                "session_id": target.session_id,
                "config_digest": target.config_digest,
            },
        }
        body_digest = _seed_digest(seed)
        command = CreateAndActivateDialogue(
            _stable_id("reg.create", body_digest),
            body_digest,
            state.registry_sequence,
            state.generation,
            target,
        )
        self._commit_registry_decision(
            log, command, body_digest, authority, collector
        )

    def _commit_registry_begin(
        self,
        log: _RegistryTransactionLog,
        config: DialogueSessionConfig,
        authority: RegistryLockAuthority,
        collector: CommittedFactCollector | None = None,
    ) -> None:
        collector = collector or CommittedFactCollector()
        state = log.tip().state
        source = state.current_session_id
        if source is None:
            raise CoordinatorError("REGISTRY_STATE_CONFLICT")
        target = ActivateTarget(config.session_id, session_config_digest(config))
        handoff_seed = {
            "operation": "handoff",
            "registry_id": state.registry_id,
            "source_session_id": source,
            "target": {
                "session_id": target.session_id,
                "config_digest": target.config_digest,
            },
        }
        handoff_digest = _seed_digest(handoff_seed)
        handoff_id = _stable_id("handoff", handoff_digest)
        body = {**handoff_seed, "handoff_id": handoff_id}
        body_digest = _seed_digest(body)
        command = BeginHandoff(
            _stable_id("reg.begin", body_digest),
            body_digest,
            state.registry_sequence,
            state.generation,
            handoff_id,
            source,
            target,
        )
        self._commit_registry_decision(
            log, command, body_digest, authority, collector
        )

    def _commit_registry_decision(
        self,
        log: _RegistryTransactionLog,
        command: CreateAndActivateDialogue | BeginHandoff | CompleteHandoff,
        identity_digest: str,
        authority: RegistryLockAuthority,
        collector: CommittedFactCollector,
    ) -> None:
        state = log.tip().state
        decision = decide_registry(state, command)
        if type(decision) is RegistryRejected:
            raise CoordinatorError(decision.code)
        if type(decision) is RegistryIdempotent:
            return
        if type(decision) is not RegistryAccepted:
            raise CoordinatorError("REGISTRY_DECISION_INVALID")
        event_ids = tuple(
            _stable_id("reg.event", identity_digest, index)
            for index in range(1, len(decision.events) + 1)
        )
        request = RegistryCommitRequest(
            _stable_id("reg.tx", identity_digest),
            event_ids,
            state.registry_sequence,
            state.generation,
            decision,
        )
        self._commit_registry_request(log, request, authority, collector)

    def _commit_registry_request(
        self,
        log: _RegistryTransactionLog,
        request: RegistryCommitRequest,
        authority: RegistryLockAuthority,
        collector: CommittedFactCollector,
    ) -> RegistryCommitOutcome:
        try:
            outcome = log.commit(request, authority)
        except BaseException as original:
            self._invalidate_durable_catch_up()
            try:
                self._capture_durable_registry_request(log, request, collector)
            except BaseException:
                _note_after_commit_audit_failure(original)
            raise
        return collector.capture_registry(outcome)

    def _capture_durable_registry_request(
        self,
        log: _RegistryTransactionLog,
        request: RegistryCommitRequest,
        collector: CommittedFactCollector,
    ) -> None:
        committed = log.read_committed(after_sequence=0)
        first = request.accepted.events[0]
        receipt = next(
            (
                item
                for item in log.tip().state.receipts
                if item.command_id == first.command_id
            ),
            None,
        )
        if receipt is None or receipt.body_digest != first.body_digest:
            return
        events = tuple(
            event
            for event in committed
            if receipt.first_registry_sequence
            <= event.registry_sequence
            <= receipt.last_registry_sequence
        )
        if tuple(event.event_id for event in events) != receipt.event_ids:
            raise CoordinatorError("AFTER_COMMIT_AUDIT_FAILED")
        collector.capture_registry_events(self._registry_id, events)

    def _continue_handoff(
        self,
        registry_log: _RegistryTransactionLog,
        registry_authority: RegistryLockAuthority,
        collector: CommittedFactCollector | None = None,
    ) -> None:
        collector = collector or CommittedFactCollector()
        state = registry_log.tip().state
        pending = state.pending_handoff
        if pending is None:
            return
        target_config = self._load_config_checked(
            pending.target.session_id,
            pending.target.config_digest,
        )
        proof = self._quiesce_source(
            state,
            pending,
            target_config,
            registry_authority,
            collector,
        )
        self._fault_hook("source_prepared")
        complete_seed = {
            "operation": "complete_handoff",
            "registry_id": state.registry_id,
            "handoff_id": pending.handoff_id,
            "proof": {
                "source_session_id": proof.source_session_id,
                "target_session_id": proof.target.session_id,
                "target_config_digest": proof.target.config_digest,
                "generation": proof.generation,
                "dialogue_last_sequence": proof.dialogue_last_sequence,
                "dialogue_last_event_id": proof.dialogue_last_event_id,
                "dialogue_event_digest": proof.dialogue_event_digest,
                "dialogue_transaction_digest": proof.dialogue_transaction_digest,
                "dialogue_state_digest": proof.dialogue_state_digest,
            },
        }
        body_digest = _seed_digest(complete_seed)
        command = CompleteHandoff(
            _stable_id("reg.complete", body_digest),
            body_digest,
            state.registry_sequence,
            state.generation,
            pending.handoff_id,
            proof,
        )
        decision = decide_registry(state, command)
        if type(decision) is RegistryRejected:
            raise CoordinatorError(decision.code)
        if type(decision) is not RegistryAccepted:
            raise CoordinatorError("REGISTRY_DECISION_INVALID")
        request = RegistryCommitRequest(
            _stable_id("reg.tx", body_digest),
            tuple(
                _stable_id("reg.event", body_digest, index)
                for index in range(1, len(decision.events) + 1)
            ),
            state.registry_sequence,
            state.generation,
            decision,
        )
        self._commit_registry_request(
            registry_log,
            request,
            registry_authority,
            collector,
        )
        self._fault_hook("handoff_completed")
        self._bootstrap_current(
            registry_log.tip().state,
            target_config,
            registry_authority,
            collector,
        )

    def _quiesce_source(
        self,
        registry_state: RegistryState,
        pending: PendingHandoff,
        target_config: DialogueSessionConfig,
        registry_authority: RegistryLockAuthority,
        collector: CommittedFactCollector,
    ) -> QuiesceProof:
        source = _registration(registry_state, pending.source_session_id)
        if (
            source is None
            or source.status is not DialogueRegistrationStatus.DEACTIVATING
            or source.activated_generation is None
        ):
            raise CoordinatorError("REGISTRY_STATE_CONFLICT")
        with self._locks.session_exclusive(
            source.session_id, registry_authority
        ) as session_authority:
            log = self._open_existing_dialogue(
                source.session_id,
                source.activated_generation,
                session_authority,
            )
            try:
                tip = log.tip()
                if tip.last_registry_generation not in {
                    source.activated_generation,
                    pending.generation,
                }:
                    raise CoordinatorError("HANDOFF_PROOF_INVALID")
                if tip.last_registry_generation == pending.generation:
                    self._validate_existing_quiesce(log, tip, pending)
                if tip.last_registry_generation == source.activated_generation:
                    command = PrepareSessionDeactivation(
                        _stable_id(
                            "dlg.quiesce",
                            _seed_digest(
                                {
                                    "handoff_id": pending.handoff_id,
                                    "source_session_id": source.session_id,
                                }
                            ),
                        )
                    )
                    context = FencedQuiesceContext(
                        pending.handoff_id,
                        source.session_id,
                        source.activated_generation,
                        pending.generation,
                    )
                    decision = decide_fenced_quiesce(
                        tip.state, command, context
                    )
                    if type(decision) is Rejected:
                        raise CoordinatorError(decision.code)
                    if type(decision) is not Accepted:
                        raise CoordinatorError("DIALOGUE_DECISION_INVALID")
                    if decision.events:
                        seed = {
                            "operation": "fenced_quiesce",
                            "handoff_id": pending.handoff_id,
                            "source_session_id": source.session_id,
                            "source_generation": source.activated_generation,
                            "fence_generation": pending.generation,
                            "expected_conversation_version": (
                                tip.state.conversation_version
                            ),
                        }
                        request_digest = _seed_digest(seed)
                        self._commit_dialogue_events(
                            log,
                            session_authority,
                            tip,
                            decision.events,
                            collector,
                            request_digest=request_digest,
                            registry_generation=pending.generation,
                            occurred_at=target_config.created_at,
                            actor=_RUNTIME_ACTOR,
                            causation_id=pending.handoff_id,
                        )
                        tip = log.tip()
                return self._proof_from_tip(pending, tip)
            finally:
                log.close()

    @staticmethod
    def _validate_existing_quiesce(
        log: _DialogueTransactionLog,
        tip: DialogueTip,
        pending: PendingHandoff,
    ) -> None:
        if tip.state.sequence < 1:
            raise CoordinatorError("HANDOFF_PROOF_INVALID")
        committed = log.read_committed(after_sequence=tip.state.sequence - 1)
        if len(committed) != 1:
            raise CoordinatorError("HANDOFF_PROOF_INVALID")
        payload = committed[0].payload
        valid_topic_pause = (
            type(payload) is TopicPaused
            and payload.cause is PauseCause.SESSION_DEACTIVATION
            and payload.handoff_id == pending.handoff_id
            and payload.fence_generation == pending.generation
        )
        valid_setup_prepare = (
            type(payload) is SessionDeactivationPrepared
            and payload.handoff_id == pending.handoff_id
            and payload.fence_generation == pending.generation
        )
        if not valid_topic_pause and not valid_setup_prepare:
            raise CoordinatorError("HANDOFF_PROOF_INVALID")

    @staticmethod
    def _proof_from_tip(
        pending: PendingHandoff, tip: DialogueTip
    ) -> QuiesceProof:
        if (
            tip.state.phase is not ConversationPhase.NONE
            or tip.state.active_topic is not None
            or tip.state.lifecycle is not SessionLifecycle.OPEN
            or tip.last_event_id is None
            or tip.last_event_hash is None
            or tip.last_marker_hash is None
        ):
            raise CoordinatorError("HANDOFF_PROOF_INVALID")
        return QuiesceProof(
            pending.source_session_id,
            pending.target,
            pending.handoff_id,
            pending.generation,
            tip.state.sequence,
            tip.last_event_id,
            tip.last_event_hash,
            tip.last_marker_hash,
            dialogue_state_digest(tip.state),
        )

    def _bootstrap_current(
        self,
        registry_state: RegistryState,
        config: DialogueSessionConfig,
        registry_authority: RegistryLockAuthority,
        collector: CommittedFactCollector | None = None,
    ) -> DialogueState:
        collector = collector or CommittedFactCollector()
        registration = _registration(registry_state, config.session_id)
        if (
            registry_state.pending_handoff is not None
            or registry_state.current_session_id != config.session_id
            or registration is None
            or registration.status is not DialogueRegistrationStatus.ACTIVE
            or registration.activated_generation != registry_state.generation
            or registration.config_digest != session_config_digest(config)
        ):
            raise CoordinatorError("SESSION_DEACTIVATED")
        with self._locks.session_exclusive(
            config.session_id, registry_authority
        ) as session_authority:
            log = self._open_existing_dialogue(
                config.session_id,
                registry_state.generation,
                session_authority,
            )
            try:
                tip = log.tip()
                if tip.state.sequence:
                    if (
                        tip.state.lifecycle is not SessionLifecycle.OPEN
                        or tip.last_registry_generation != registry_state.generation
                    ):
                        raise CoordinatorError("DIALOGUE_BOOTSTRAP_CONFLICT")
                    return tip.state
                config_digest = session_config_digest(config)
                trigger = TriggerBinding(
                    TriggerKind.TOPIC_CANDIDATES,
                    _stable_id("work.candidates", config_digest),
                    config.runtime_epoch,
                    None,
                    None,
                    config_digest,
                    config.evidence_digest,
                )
                context = DecisionContext(
                    registry_state.generation,
                    trigger,
                    self._verify_evidence(config),
                )
                command = StartSession(
                    _stable_id("dlg.start", config_digest)
                )
                decision = decide(tip.state, command, context)
                if type(decision) is Rejected:
                    raise CoordinatorError(decision.code)
                if type(decision) is not Accepted:
                    raise CoordinatorError("DIALOGUE_DECISION_INVALID")
                request_digest = dialogue_request_digest(
                    DialogueWriteRequestRecord(
                        SCHEMA_VERSION,
                        "dialogue_write_request",
                        config.session_id,
                        registry_state.generation,
                        0,
                        command,
                        context,
                    )
                )
                outcome = self._commit_dialogue_events(
                    log,
                    session_authority,
                    tip,
                    decision.events,
                    collector,
                    request_digest=request_digest,
                    registry_generation=registry_state.generation,
                    occurred_at=config.created_at,
                    actor=_RUNTIME_ACTOR,
                    causation_id=command.command_id,
                )
                self._fault_hook("dialogue_started")
                return outcome.state
            finally:
                log.close()

    def _execute_locked(
        self,
        log: _DialogueTransactionLog,
        authority: SessionLockAuthority,
        request: DialogueExecutionRequest,
        config: DialogueSessionConfig,
        collector: CommittedFactCollector,
    ) -> DialogueCommitOutcome:
        tip = log.tip()
        request_digest = dialogue_request_digest(
            DialogueWriteRequestRecord(
                SCHEMA_VERSION,
                "dialogue_write_request",
                request.session_id,
                request.context.registry_generation,
                request.expected_conversation_version,
                request.command,
                request.context,
            )
        )
        existing = next(
            (
                item
                for item in tip.receipts
                if item.command_id == request.command.command_id
            ),
            None,
        )
        if existing is not None:
            if existing.request_digest != request_digest:
                raise CoordinatorError("IDEMPOTENCY_CONFLICT")
            committed = tuple(
                event
                for event in log.read_committed(
                    after_sequence=existing.from_sequence - 1
                )
                if event.sequence <= existing.to_sequence
            )
            commit_request = DialogueCommitRequest(
                request.session_id,
                _stable_id("dlg.tx", request_digest),
                request_digest,
                request.context.registry_generation,
                committed,
                tuple(
                    EventMetadata(
                        request.occurred_at,
                        request.actor,
                        request.command.command_id,
                    )
                    for _ in committed
                ),
            )
            return self._commit_dialogue_request(
                log,
                commit_request,
                authority,
                collector,
            )
        if request.expected_conversation_version != tip.state.conversation_version:
            raise CoordinatorError("CONVERSATION_VERSION_CONFLICT")
        verified_evidence = self._verify_evidence(config)
        if request.context.evidence != verified_evidence:
            raise CoordinatorError("EVIDENCE_CHECK_CONFLICT")
        decision = decide(tip.state, request.command, request.context)
        if type(decision) is Rejected:
            raise CoordinatorError(decision.code)
        if type(decision) is not Accepted or not decision.events:
            raise CoordinatorError("DIALOGUE_DECISION_INVALID")
        if self._verify_evidence(config) != verified_evidence:
            raise CoordinatorError("EVIDENCE_CHANGED")
        return self._commit_dialogue_events(
            log,
            authority,
            tip,
            decision.events,
            collector,
            request_digest=request_digest,
            registry_generation=request.context.registry_generation,
            occurred_at=request.occurred_at,
            actor=request.actor,
            causation_id=request.command.command_id,
        )

    @staticmethod
    def _validate_execution(request: object) -> DialogueExecutionRequest:
        if (
            type(request) is not DialogueExecutionRequest
            or _ID_PATTERN.fullmatch(request.session_id) is None
            or type(request.expected_conversation_version) is not int
            or request.expected_conversation_version < 0
            or type(request.context) is not DecisionContext
            or type(request.actor) is not DialogueActor
            or not _valid_timestamp(request.occurred_at)
        ):
            raise CoordinatorError("INVALID_DIALOGUE_EXECUTION")
        return request

    def _verify_evidence(self, config: DialogueSessionConfig) -> EvidenceCheck:
        try:
            check = self._evidence_verifier(config)
        except Exception as exc:
            raise CoordinatorError("EVIDENCE_VERIFICATION_FAILED") from exc
        if (
            type(check) is not EvidenceCheck
            or type(check.health) is not EvidenceHealth
            or check.evidence_digest != config.evidence_digest
            or (
                check.exact_recheck_fingerprint is not None
                and not _nonblank(check.exact_recheck_fingerprint)
            )
            or (
                check.health is EvidenceHealth.CAPTURED_DIRTY
                and check.exact_recheck_fingerprint is None
            )
        ):
            raise CoordinatorError("EVIDENCE_VERIFICATION_FAILED")
        return check

    def _require_publishable_evidence(
        self,
        config: DialogueSessionConfig,
    ) -> EvidenceCheck:
        check = self._verify_evidence(config)
        if check.health not in {
            EvidenceHealth.CURRENT,
            EvidenceHealth.CAPTURED_DIRTY,
        }:
            raise CoordinatorError("EVIDENCE_NOT_PUBLISHABLE")
        return check

    def _commit_dialogue_events(
        self,
        log: _DialogueTransactionLog,
        authority: SessionLockAuthority,
        tip: DialogueTip,
        pending: tuple[PendingDialogueEvent, ...],
        collector: CommittedFactCollector,
        *,
        request_digest: str,
        registry_generation: int,
        occurred_at: str,
        actor: DialogueActor,
        causation_id: str | None,
        marker_publication_guard: MarkerPublicationGuard | None = None,
    ) -> DialogueCommitOutcome:
        events: list[CommittedDialogueEvent] = []
        sequence = tip.state.sequence
        version = tip.state.conversation_version
        for index, item in enumerate(pending, start=1):
            sequence += 1
            delta = conversation_version_delta(item.payload)
            events.append(
                CommittedDialogueEvent(
                    _stable_id("dlg.event", request_digest, index),
                    sequence,
                    version,
                    version + delta,
                    item.command_id,
                    item.payload,
                )
            )
            version += delta
        request = DialogueCommitRequest(
            tip.state.session_id,
            _stable_id("dlg.tx", request_digest),
            request_digest,
            registry_generation,
            tuple(events),
            tuple(
                EventMetadata(occurred_at, actor, causation_id)
                for _ in events
            ),
            tuple(
                plan_export_intent(tip.state.session_id, item.payload)
                for item in pending
                if type(item.payload) is ExportRequested
            ),
        )
        return self._commit_dialogue_request(
            log,
            request,
            authority,
            collector,
            marker_publication_guard=marker_publication_guard,
        )

    def _commit_dialogue_request(
        self,
        log: _DialogueTransactionLog,
        request: DialogueCommitRequest,
        authority: SessionLockAuthority,
        collector: CommittedFactCollector,
        *,
        marker_publication_guard: MarkerPublicationGuard | None = None,
    ) -> DialogueCommitOutcome:
        try:
            if marker_publication_guard is None:
                outcome = log.commit(request, authority)
            else:
                outcome = log.commit(
                    request,
                    authority,
                    marker_publication_guard=marker_publication_guard,
                )
        except BaseException as original:
            self._invalidate_durable_catch_up()
            try:
                self._capture_durable_dialogue_request(log, request, collector)
            except BaseException:
                _note_after_commit_audit_failure(original)
            raise
        return collector.capture_dialogue(outcome)

    @staticmethod
    def _capture_durable_dialogue_request(
        log: _DialogueTransactionLog,
        request: DialogueCommitRequest,
        collector: CommittedFactCollector,
    ) -> None:
        committed = log.read_committed(after_sequence=0)
        command_id = request.events[0].command_id
        receipt = next(
            (
                item
                for item in log.tip().receipts
                if item.command_id == command_id
            ),
            None,
        )
        if receipt is None or receipt.request_digest != request.request_digest:
            return
        events = tuple(
            event
            for event in committed
            if receipt.from_sequence <= event.sequence <= receipt.to_sequence
        )
        if tuple(event.event_id for event in events) != receipt.event_ids:
            raise CoordinatorError("AFTER_COMMIT_AUDIT_FAILED")
        collector.capture_dialogue_events(request.session_id, events)
