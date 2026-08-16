"""Browser-neutral command service over the canonical dialogue coordinator.

HTTP, HTML, and SSE adapters submit only the small intent records defined in
this module.  Context construction stays here so transport handlers never
copy state-machine guards or manufacture evidence claims.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Protocol, TypeAlias, cast

from .coordinator import (
    CoordinatorError,
    DialogueExecutionRequest,
    DialogueResolution,
    DialogueSessionConfig,
)
from .domain import (
    AnswerTopicClarification,
    CurrentWorkState,
    DecisionContext,
    DialogueCommand,
    DialogueState,
    EvidenceCheck,
    EvidenceHealth,
    Lens,
    PauseTopic,
    RecoverWork,
    RequestHelp,
    ResumeTopic,
    SelectTopic,
    SetLens,
    SubmitCustomTopic,
    SubmitLearnerTurn,
    SwitchTopic,
    TriggerBinding,
    TriggerKind,
    WorkRecoveryAction,
    WorkStatus,
)
from .event_codec import ActorKind, DialogueActor
from .event_store import DialogueCommitOutcome
from .work_identity import is_protocol_id, is_sha256_digest

_MAX_TEXT_BYTES = 32 * 1024
_PUBLISHABLE_EVIDENCE = frozenset(
    {EvidenceHealth.CURRENT, EvidenceHealth.CAPTURED_DIRTY}
)
Clock = Callable[[], str]
EvidenceVerifier = Callable[[DialogueSessionConfig], EvidenceCheck]


class BrowserServiceError(RuntimeError):
    """Stable failure raised before a canonical command is submitted."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class BrowserCoordinator(Protocol):
    """Narrow coordinator port required by browser commands."""

    def recover(self) -> DialogueResolution | None: ...

    def execute(
        self, request: DialogueExecutionRequest
    ) -> DialogueCommitOutcome: ...


@dataclass(frozen=True, slots=True)
class SubmitTurnIntent:
    """Learner answer to the currently visible Agent question."""

    question_id: str
    text: str


@dataclass(frozen=True, slots=True)
class SelectTopicIntent:
    """Learner choice from the currently presented candidate set."""

    candidate: str


@dataclass(frozen=True, slots=True)
class CustomTopicIntent:
    """Learner-authored topic outside the presented candidates."""

    topic: str


@dataclass(frozen=True, slots=True)
class AnswerTopicClarificationIntent:
    """Learner answer to the current Host topic clarification."""

    question_id: str
    answer: str


@dataclass(frozen=True, slots=True)
class SetLensIntent:
    """Learner request to continue the active Topic through another lens."""

    lens: Lens


@dataclass(frozen=True, slots=True)
class RequestHelpIntent:
    """Learner request for a minimal hint on the visible question."""

    question_id: str


@dataclass(frozen=True, slots=True)
class PauseTopicIntent:
    """Learner request to pause the current Topic Run."""


@dataclass(frozen=True, slots=True)
class SwitchTopicIntent:
    """Learner request to pause the active Topic and choose another."""


@dataclass(frozen=True, slots=True)
class ResumeTopicIntent:
    """Learner request to resume one paused Topic Run."""

    topic_run_id: str


@dataclass(frozen=True, slots=True)
class RecoverWorkIntent:
    """Learner-selected recovery action for one dead-lettered work item."""

    dead_work_id: str
    action: WorkRecoveryAction


BrowserIntent: TypeAlias = (
    SubmitTurnIntent
    | SelectTopicIntent
    | CustomTopicIntent
    | AnswerTopicClarificationIntent
    | SetLensIntent
    | RequestHelpIntent
    | PauseTopicIntent
    | SwitchTopicIntent
    | ResumeTopicIntent
    | RecoverWorkIntent
)


@dataclass(frozen=True, slots=True)
class BrowserCommandRequest:
    """Authenticated, optimistic Browser command before domain translation."""

    session_id: str
    idempotency_key: str
    expected_conversation_version: int
    intent: BrowserIntent
    actor_id: str = "browser"


def _valid_text(value: object, *, max_bytes: int = _MAX_TEXT_BYTES) -> bool:
    if type(value) is not str or not value or value != value.strip():
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        return False
    return len(encoded) <= max_bytes and not any(
        ord(character) < 32 and character not in {"\n", "\t"}
        for character in value
    )


def _valid_timestamp(value: object) -> bool:
    if type(value) is not str or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _sha256_tree(value: object) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _stable_id(prefix: str, *parts: str) -> str:
    material = "\0".join(parts).encode("utf-8")
    return f"{prefix}.{hashlib.sha256(material).hexdigest()[:48]}"


def _work_target(
    state: DialogueState,
    dead_work_id: str,
) -> CurrentWorkState | None:
    candidates = [state.session_work]
    if state.active_topic is not None:
        candidates.append(state.active_topic.work)
    candidates.extend(topic.work for topic in state.paused_topics)
    exact = tuple(
        work
        for work in candidates
        if work is not None and work.work_id == dead_work_id
    )
    if len(exact) == 1:
        return exact[0]
    # Receipt replay happens after the successful recovery moved to a fresh
    # work id.  Its canonical trigger is sufficient to reproduce the request
    # digest; the coordinator remains the authority on whether a receipt exists.
    current = tuple(
        work
        for work in candidates
        if work is not None and work.status is WorkStatus.QUEUED
    )
    return current[0] if len(current) == 1 else None


class BrowserCommandService:
    """Translate authenticated Browser intent into one typed domain command."""

    def __init__(
        self,
        coordinator: BrowserCoordinator,
        evidence_verifier: EvidenceVerifier,
        *,
        clock: Clock,
    ) -> None:
        if (
            not callable(getattr(coordinator, "recover", None))
            or not callable(getattr(coordinator, "execute", None))
            or not callable(evidence_verifier)
            or not callable(clock)
        ):
            raise BrowserServiceError("INVALID_BROWSER_SERVICE_CONFIGURATION")
        self._coordinator = coordinator
        self._evidence_verifier = evidence_verifier
        self._clock = clock

    def current(self, session_id: str) -> DialogueResolution:
        """Read the durable current Session without accepting a session path."""
        if not is_protocol_id(session_id):
            raise BrowserServiceError("VALIDATION_FAILED")
        try:
            resolution = self._coordinator.recover()
        except CoordinatorError as exc:
            raise BrowserServiceError(exc.code) from exc
        if resolution is None:
            raise BrowserServiceError("SESSION_DEACTIVATED")
        config = resolution.config
        state = resolution.dialogue_state
        if (
            type(config) is not DialogueSessionConfig
            or type(state) is not DialogueState
            or config.session_id != session_id
            or state.session_id != session_id
        ):
            raise BrowserServiceError("SESSION_DEACTIVATED")
        return resolution

    def execute(self, request: BrowserCommandRequest) -> DialogueCommitOutcome:
        """Build context once and delegate the only mutation to Coordinator."""
        request = self._validate_request(request)
        resolution = self.current(request.session_id)
        command_id = _stable_id(
            "browser.command",
            request.session_id,
            request.idempotency_key,
        )
        command = self._command(command_id, request)
        evidence = self._verify_evidence(resolution.config)
        context = self._context(
            resolution.dialogue_state,
            resolution.config,
            request,
            command,
            evidence,
        )
        occurred_at = self._clock()
        if not _valid_timestamp(occurred_at):
            raise BrowserServiceError("INVALID_BROWSER_CLOCK")
        try:
            return self._coordinator.execute(
                DialogueExecutionRequest(
                    request.session_id,
                    request.expected_conversation_version,
                    command,
                    context,
                    occurred_at,
                    DialogueActor(ActorKind.LEARNER, request.actor_id),
                )
            )
        except CoordinatorError as exc:
            raise BrowserServiceError(exc.code) from exc

    @staticmethod
    def _validate_request(request: object) -> BrowserCommandRequest:
        if (
            type(request) is not BrowserCommandRequest
            or not is_protocol_id(request.session_id)
            or not is_protocol_id(request.idempotency_key)
            or type(request.expected_conversation_version) is not int
            or request.expected_conversation_version < 0
            or not is_protocol_id(request.actor_id)
            or type(request.intent)
            not in {
                SubmitTurnIntent,
                SelectTopicIntent,
                CustomTopicIntent,
                AnswerTopicClarificationIntent,
                SetLensIntent,
                RequestHelpIntent,
                PauseTopicIntent,
                SwitchTopicIntent,
                ResumeTopicIntent,
                RecoverWorkIntent,
            }
        ):
            raise BrowserServiceError("VALIDATION_FAILED")
        intent = request.intent
        valid = True
        if type(intent) is SubmitTurnIntent:
            valid = is_protocol_id(intent.question_id) and _valid_text(intent.text)
        elif type(intent) is SelectTopicIntent:
            valid = _valid_text(intent.candidate)
        elif type(intent) is CustomTopicIntent:
            valid = _valid_text(intent.topic)
        elif type(intent) is AnswerTopicClarificationIntent:
            valid = is_protocol_id(intent.question_id) and _valid_text(intent.answer)
        elif type(intent) is SetLensIntent:
            valid = type(intent.lens) is Lens
        elif type(intent) is RequestHelpIntent:
            valid = is_protocol_id(intent.question_id)
        elif type(intent) is ResumeTopicIntent:
            valid = is_protocol_id(intent.topic_run_id)
        elif type(intent) is RecoverWorkIntent:
            valid = is_protocol_id(intent.dead_work_id) and type(
                intent.action
            ) is WorkRecoveryAction
        if not valid:
            raise BrowserServiceError("VALIDATION_FAILED")
        return request

    def _verify_evidence(self, config: DialogueSessionConfig) -> EvidenceCheck:
        try:
            evidence = self._evidence_verifier(config)
        except Exception as exc:
            raise BrowserServiceError("EVIDENCE_VERIFICATION_FAILED") from exc
        if (
            type(evidence) is not EvidenceCheck
            or type(evidence.health) is not EvidenceHealth
            or not is_sha256_digest(evidence.evidence_digest)
            or evidence.evidence_digest != config.evidence_digest
            or (
                evidence.health is EvidenceHealth.CAPTURED_DIRTY
                and not _valid_text(evidence.exact_recheck_fingerprint)
            )
            or (
                evidence.health is not EvidenceHealth.CAPTURED_DIRTY
                and evidence.exact_recheck_fingerprint is not None
                and not _valid_text(evidence.exact_recheck_fingerprint)
            )
        ):
            raise BrowserServiceError("EVIDENCE_VERIFICATION_FAILED")
        return evidence

    @staticmethod
    def _command(
        command_id: str,
        request: BrowserCommandRequest,
    ) -> DialogueCommand:
        intent = request.intent
        if type(intent) is SubmitTurnIntent:
            return SubmitLearnerTurn(
                command_id,
                intent.question_id,
                _stable_id(
                    "learner.turn",
                    request.session_id,
                    request.idempotency_key,
                ),
                intent.text,
            )
        if type(intent) is SelectTopicIntent:
            return SelectTopic(command_id, intent.candidate)
        if type(intent) is CustomTopicIntent:
            return SubmitCustomTopic(command_id, intent.topic)
        if type(intent) is AnswerTopicClarificationIntent:
            return AnswerTopicClarification(
                command_id,
                intent.question_id,
                intent.answer,
            )
        if type(intent) is SetLensIntent:
            return SetLens(command_id, intent.lens)
        if type(intent) is RequestHelpIntent:
            return RequestHelp(command_id, intent.question_id)
        if type(intent) is PauseTopicIntent:
            return PauseTopic(command_id)
        if type(intent) is SwitchTopicIntent:
            return SwitchTopic(command_id)
        if type(intent) is ResumeTopicIntent:
            return ResumeTopic(command_id, intent.topic_run_id)
        if type(intent) is RecoverWorkIntent:
            return RecoverWork(command_id, intent.dead_work_id, intent.action)
        raise BrowserServiceError("VALIDATION_FAILED")

    @staticmethod
    def _context(
        state: DialogueState,
        config: DialogueSessionConfig,
        request: BrowserCommandRequest,
        command: DialogueCommand,
        evidence: EvidenceCheck,
    ) -> DecisionContext:
        intent = request.intent
        if type(intent) is SubmitTurnIntent:
            intent_tree: dict[str, object] = {
                "type": "submit_turn",
                "question_id": intent.question_id,
                "text": intent.text,
            }
        elif type(intent) is SelectTopicIntent:
            intent_tree = {
                "type": "select_topic",
                "candidate": intent.candidate,
            }
        elif type(intent) is CustomTopicIntent:
            intent_tree = {
                "type": "custom_topic",
                "topic": intent.topic,
            }
        elif type(intent) is AnswerTopicClarificationIntent:
            intent_tree = {
                "type": "answer_topic_clarification",
                "question_id": intent.question_id,
                "answer": intent.answer,
            }
        elif type(intent) is SetLensIntent:
            intent_tree = {
                "type": "set_lens",
                "lens": intent.lens.value,
            }
        elif type(intent) is RequestHelpIntent:
            intent_tree = {
                "type": "request_help",
                "question_id": intent.question_id,
            }
        elif type(intent) is PauseTopicIntent:
            intent_tree = {"type": "pause_topic"}
        elif type(intent) is SwitchTopicIntent:
            intent_tree = {"type": "switch_topic"}
        elif type(intent) is ResumeTopicIntent:
            intent_tree = {
                "type": "resume_topic",
                "topic_run_id": intent.topic_run_id,
            }
        elif type(intent) is RecoverWorkIntent:
            intent_tree = {
                "type": "recover_work",
                "dead_work_id": intent.dead_work_id,
                "action": intent.action.value,
            }
        else:
            raise BrowserServiceError("VALIDATION_FAILED")
        seed = {
            "session_id": request.session_id,
            "idempotency_key": request.idempotency_key,
            "expected_conversation_version": request.expected_conversation_version,
            "current_lens": (
                None
                if state.active_topic is None
                else state.active_topic.lens.value
            ),
            "intent": intent_tree,
        }
        input_digest = _sha256_tree(seed)
        work_id = _stable_id(
            "work.browser", request.session_id, request.idempotency_key
        )
        trigger: TriggerBinding | None = None
        if type(intent) is SubmitTurnIntent:
            topic = state.active_topic
            learner_turn_id = cast(SubmitLearnerTurn, command).learner_turn_id
            if topic is None:
                raise BrowserServiceError("TOPIC_STATE_CONFLICT")
            trigger = TriggerBinding(
                TriggerKind.LEARNER_REPLY,
                work_id,
                config.runtime_epoch,
                learner_turn_id,
                topic.contract.contract_digest,
                input_digest,
                evidence.evidence_digest,
            )
        elif type(intent) is SetLensIntent:
            topic = state.active_topic
            if topic is None:
                raise BrowserServiceError("TOPIC_STATE_CONFLICT")
            parent_turn_id = (
                topic.current_agent_turn.question_id
                if topic.current_agent_turn is not None
                else (
                    None
                    if topic.work is None
                    else topic.work.trigger.parent_turn_id
                )
            )
            trigger = TriggerBinding(
                TriggerKind.LENS_CHANGED,
                work_id,
                config.runtime_epoch,
                parent_turn_id,
                topic.contract.contract_digest,
                input_digest,
                evidence.evidence_digest,
            )
        elif type(intent) is RequestHelpIntent:
            topic = state.active_topic
            if topic is None or topic.current_agent_turn is None:
                raise BrowserServiceError("TOPIC_STATE_CONFLICT")
            trigger = TriggerBinding(
                TriggerKind.HELP,
                work_id,
                config.runtime_epoch,
                intent.question_id,
                topic.contract.contract_digest,
                input_digest,
                evidence.evidence_digest,
            )
        elif type(intent) in {
            SelectTopicIntent,
            CustomTopicIntent,
            AnswerTopicClarificationIntent,
        }:
            trigger = TriggerBinding(
                TriggerKind.TOPIC_SELECTION,
                work_id,
                config.runtime_epoch,
                (
                    intent.question_id
                    if type(intent) is AnswerTopicClarificationIntent
                    else None
                ),
                None,
                input_digest,
                evidence.evidence_digest,
            )
        elif type(intent) is SwitchTopicIntent:
            trigger = TriggerBinding(
                TriggerKind.TOPIC_CANDIDATES,
                work_id,
                config.runtime_epoch,
                None,
                None,
                input_digest,
                evidence.evidence_digest,
            )
        elif type(intent) is ResumeTopicIntent:
            paused = next(
                (
                    item
                    for item in state.paused_topics
                    if item.topic_run_id == intent.topic_run_id
                ),
                None,
            )
            if paused is None:
                # Exact response replay reconstructs the context from the now
                # active Topic without granting a new transition.
                active = state.active_topic
                if active is None or active.topic_run_id != intent.topic_run_id:
                    raise BrowserServiceError("TOPIC_STATE_CONFLICT")
                trigger = active.work.trigger if active.work is not None else None
            else:
                changed = (
                    evidence.health is not paused.evidence_health
                    or evidence.evidence_digest != paused.evidence_digest
                    or evidence.exact_recheck_fingerprint
                    != paused.exact_recheck_fingerprint
                )
                if changed or evidence.health not in _PUBLISHABLE_EVIDENCE:
                    trigger = TriggerBinding(
                        TriggerKind.REGROUND,
                        work_id,
                        config.runtime_epoch,
                        None,
                        paused.contract.contract_digest,
                        input_digest,
                        evidence.evidence_digest,
                    )
                elif paused.current_agent_turn is None:
                    saved = paused.work
                    if saved is None:
                        raise BrowserServiceError("TOPIC_STATE_CONFLICT")
                    trigger = replace(
                        saved.trigger,
                        work_id=work_id,
                        runtime_epoch=config.runtime_epoch,
                    )
        elif type(intent) is RecoverWorkIntent:
            target = _work_target(state, intent.dead_work_id)
            if target is None:
                raise BrowserServiceError("TOPIC_STATE_CONFLICT")
            if intent.action is WorkRecoveryAction.RETRY:
                trigger = replace(
                    target.trigger,
                    work_id=work_id,
                    runtime_epoch=config.runtime_epoch,
                )
            else:
                trigger = TriggerBinding(
                    TriggerKind.REGROUND,
                    work_id,
                    config.runtime_epoch,
                    target.trigger.parent_turn_id,
                    target.trigger.contract_digest,
                    input_digest,
                    evidence.evidence_digest,
                )
        elif type(intent) not in {PauseTopicIntent, SwitchTopicIntent}:
            raise BrowserServiceError("VALIDATION_FAILED")
        return DecisionContext(state.registry_generation, trigger, evidence)


__all__ = [
    "AnswerTopicClarificationIntent",
    "BrowserCommandRequest",
    "BrowserCommandService",
    "BrowserIntent",
    "BrowserServiceError",
    "CustomTopicIntent",
    "PauseTopicIntent",
    "RecoverWorkIntent",
    "RequestHelpIntent",
    "ResumeTopicIntent",
    "SelectTopicIntent",
    "SetLensIntent",
    "SubmitTurnIntent",
    "SwitchTopicIntent",
]
