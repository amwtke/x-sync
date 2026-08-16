import json
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import tests.xsync_v2_path  # noqa: F401
from tests.test_xsync_v2_state_machine import contract
from xsync_v2 import coordinator as coordinator_module
from xsync_v2 import secure_fs
from xsync_v2.coordinator import (
    CoordinatorError,
    DialogueCoordinator,
    DialogueExecutionRequest,
    DialogueSessionConfig,
    decode_session_config,
    encode_session_config,
)
from xsync_v2.domain import (
    ConversationPhase,
    DecisionContext,
    EvidenceCheck,
    EvidenceHealth,
    PauseTopic,
    PresentCandidates,
    RecoverWork,
    ReportWorkFailure,
    SessionLifecycle,
    SelectTopic,
    StartTopic,
    TriggerBinding,
    TriggerKind,
    WorkDeadLettered,
    WorkFailed,
    WorkFailure,
    WorkFailureCategory,
    WorkRecoveryAction,
    WorkRecoveryRequested,
    WorkRequeued,
    WorkStatus,
    initial_dialogue_state,
)
from xsync_v2.event_codec import (
    ActorKind,
    DialogueActor,
    sha256_digest,
)
from xsync_v2.event_store import (
    DialogueTip,
    _DialogueTransactionLog,
    session_directory_component,
)
from xsync_v2.locking import DomainLockManager
from xsync_v2.host_work import (
    HostWorkPublishRequest,
    HostWorkService,
    _request_digest as host_request_digest,
    authoritative_work_snapshot,
    host_command_id,
)
from xsync_v2.lease_store import ClaimRequest, LeaseStore, PublishFence
from xsync_v2.locking import RegistryLockMode
from xsync_v2.registry import ActivateTarget, PendingHandoff, initial_registry_state
from xsync_v2.registry_store import _RegistryTransactionLog
from xsync_v2.secure_fs import SecureDirectory
from xsync_v2.work_identity import derive_canonical_work_id
from xsync_v2.work import derive_runnable_work

HOST = DialogueActor(ActorKind.HOST, "host.test")
LEARNER = DialogueActor(ActorKind.LEARNER, "learner.test")


class InjectedCrash(RuntimeError):
    pass


def digest(label):
    return sha256_digest(label.encode("utf-8"))


def config(session_id):
    minute = "00" if session_id == "dlg-a" else "01"
    return DialogueSessionConfig(
        session_id=session_id,
        learner_id="learner.local",
        repository_id="repo.local",
        created_at=f"2026-08-16T12:{minute}:00+08:00",
        runtime_epoch=f"epoch-{session_id}",
        evidence_digest=digest(f"evidence:{session_id}"),
    )


class CoordinatorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root_path = Path(self.temporary.name).resolve()
        self.root = SecureDirectory.open(self.root_path)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.locks = DomainLockManager(self.root_path / "locks")
        self.registry_id = "registry-1"

    def tearDown(self):
        self.locks.close()
        self.dialogues.close()
        self.root.close()
        self.temporary.cleanup()

    def coordinator(self, fault_hook=lambda _stage: None):
        return DialogueCoordinator(
            self.dialogues,
            self.locks,
            self.registry_id,
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
            fault_hook=fault_hook,
        )

    def host_service(self, coordinator=None):
        application = coordinator or self.coordinator()
        leases = LeaseStore(
            self.dialogues,
            self.locks,
            "runtime-test",
            lambda: 1_000,
            snapshot_loader=lambda session_id, authority: (
                authoritative_work_snapshot(
                    application,
                    session_id,
                    authority,
                )
            ),
            runtime_authority_verifier=lambda check, _authority: (
                check.runtime_epoch == "runtime-test"
                and check.owner_id == "owner-test"
            ),
        )
        return HostWorkService(application, leases), leases

    def host_request(self, request):
        application = self.coordinator()
        service, leases = self.host_service(application)
        with self.locks.semantic_session(
            request.session_id,
            registry_mode=RegistryLockMode.EXCLUSIVE,
        ) as authority:
            snapshot = authoritative_work_snapshot(
                application,
                request.session_id,
                authority,
            )
            work = derive_runnable_work(
                snapshot.dialogue_state,
                snapshot.work_origin,
            )
            assert work is not None
            key = request.command.command_id
            command = replace(
                request.command,
                command_id=host_command_id(key),
            )
            context = request.context
            if type(command) is ReportWorkFailure:
                context = replace(context, trigger=None)
            claim = leases.claim(
                ClaimRequest(
                    request.session_id,
                    f"lease.request.{key}",
                    f"lease.claim.{key}",
                    work.work_id,
                    "owner-test",
                    30,
                    120,
                ),
                authority,
            ).lease
        fence = PublishFence(
            claim.session_id,
            claim.claim_id,
            claim.work_id,
            claim.owner_id,
            claim.runtime_epoch,
            claim.lease_version,
            claim.registry_generation,
        )
        return service, HostWorkPublishRequest(
            key,
            work,
            command,
            context,
            request.occurred_at,
            request.actor,
            fence,
        )

    def publish_host(self, request):
        service, host_request = self.host_request(request)
        return host_request, service.publish(host_request)

    def registry_state(self):
        with self.locks.registry_exclusive() as authority:
            log = _RegistryTransactionLog.create(
                self.dialogues,
                initial_registry_state(self.registry_id),
                self.locks,
                authority,
            )
            try:
                return log.tip().state
            finally:
                log.close()

    def dialogue_state(self, session_id, generation):
        with self.locks.semantic_session(session_id) as authority:
            log = _DialogueTransactionLog.create(
                self.dialogues,
                initial_dialogue_state(session_id, generation),
                self.locks,
                authority,
            )
            try:
                return log.tip().state
            finally:
                log.close()

    def present_candidates(self, resolution, command_id="cmd.candidates"):
        state = resolution.dialogue_state
        assert state.session_work is not None
        context = DecisionContext(
            resolution.registry_state.generation,
            state.session_work.trigger,
            EvidenceCheck(
                EvidenceHealth.CURRENT,
                resolution.config.evidence_digest,
            ),
        )
        request = DialogueExecutionRequest(
            state.session_id,
            state.conversation_version,
            PresentCandidates(command_id, ("支付失败边界", "补偿落点")),
            context,
            resolution.config.created_at,
            HOST,
        )
        return self.publish_host(request)

    def make_active_topic(self, resolution):
        _, candidates = self.present_candidates(resolution)
        selection_trigger = TriggerBinding(
            TriggerKind.TOPIC_SELECTION,
            "work.selection",
            resolution.config.runtime_epoch,
            None,
            None,
            digest("selection-input"),
            resolution.config.evidence_digest,
        )
        selected = self.coordinator().execute(
            DialogueExecutionRequest(
                resolution.config.session_id,
                candidates.state.conversation_version,
                SelectTopic("cmd.select", "支付失败边界"),
                DecisionContext(
                    resolution.registry_state.generation,
                    selection_trigger,
                    EvidenceCheck(
                        EvidenceHealth.CURRENT,
                        resolution.config.evidence_digest,
                    ),
                ),
                resolution.config.created_at,
                LEARNER,
            )
        )
        topic_contract = contract()
        trigger = TriggerBinding(
            TriggerKind.INITIAL_TURN,
            "work.topic",
            resolution.config.runtime_epoch,
            None,
            topic_contract.contract_digest,
            digest("topic-input"),
            resolution.config.evidence_digest,
        )
        context = DecisionContext(
            resolution.registry_state.generation,
            trigger,
            EvidenceCheck(
                EvidenceHealth.CURRENT,
                resolution.config.evidence_digest,
            ),
        )
        _request, committed = self.publish_host(
            DialogueExecutionRequest(
                resolution.config.session_id,
                selected.state.conversation_version,
                StartTopic("cmd.topic", topic_contract, "支付失败边界"),
                context,
                resolution.config.created_at,
                HOST,
            )
        )
        return committed

    def test_first_creation_persists_config_bootstraps_and_resumes(self):
        config_a = config("dlg-a")
        first = self.coordinator().resolve(config_a)

        self.assertTrue(first.created)
        self.assertEqual("dlg-a", first.registry_state.current_session_id)
        self.assertEqual(1, first.registry_state.generation)
        self.assertEqual(1, first.dialogue_state.sequence)
        self.assertEqual(
            ConversationPhase.WAITING_HOST,
            first.dialogue_state.phase,
        )
        work = first.dialogue_state.session_work
        assert work is not None
        self.assertEqual(
            derive_canonical_work_id(
                session_id=first.dialogue_state.session_id,
                trigger_event_id=work.trigger_event_id,
                trigger_event_sequence=work.trigger_event_sequence,
                trigger=work.trigger,
            ),
            work.work_id,
        )
        self.assertEqual(WorkStatus.QUEUED, work.status)
        config_path = (
            self.root_path
            / "dialogues"
            / session_directory_component("dlg-a")
            / "config.json"
        )
        self.assertEqual(encode_session_config(config_a), config_path.read_bytes())

        restarted = self.coordinator().resolve(config_a)
        self.assertFalse(restarted.created)
        self.assertEqual(first.registry_state, restarted.registry_state)
        self.assertEqual(first.dialogue_state, restarted.dialogue_state)

        request, committed = self.present_candidates(restarted)
        replay = self.host_service()[0].publish(request)
        self.assertFalse(committed.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(committed.receipt, replay.receipt)

    def test_failure_retry_batch_is_version_neutral_and_deterministic(self):
        first = self.coordinator().resolve(config("dlg-a"))
        state = first.dialogue_state
        work = state.session_work
        assert work is not None
        failure = WorkFailure(
            failure_id="failure.host-timeout",
            category=WorkFailureCategory.HOST_TRANSIENT,
            safe_error_code="HOST_TIMEOUT",
            proof_digest=digest("failure-proof"),
        )
        context = DecisionContext(
            registry_generation=first.registry_state.generation,
            trigger=None,
            evidence=EvidenceCheck(
                EvidenceHealth.CURRENT,
                first.config.evidence_digest,
            ),
        )
        command = ReportWorkFailure(
            "cmd.failure-retry",
            work.work_id,
            failure,
        )
        request = DialogueExecutionRequest(
            session_id=state.session_id,
            expected_conversation_version=state.conversation_version,
            command=command,
            context=context,
            occurred_at=first.config.created_at,
            actor=HOST,
        )

        request, committed = self.publish_host(request)

        self.assertFalse(committed.replayed)
        self.assertEqual(2, len(committed.events))
        self.assertIs(type(committed.events[0].payload), WorkFailed)
        self.assertIs(type(committed.events[1].payload), WorkRequeued)
        self.assertEqual(
            ((1, 1), (1, 1)),
            tuple(
                (event.from_version, event.to_version)
                for event in committed.events
            ),
        )
        self.assertEqual(3, committed.state.sequence)
        self.assertEqual(1, committed.state.conversation_version)
        retry_work = committed.state.session_work
        assert retry_work is not None
        self.assertEqual(WorkStatus.QUEUED, retry_work.status)
        self.assertEqual(2, retry_work.attempt)
        self.assertEqual(failure, retry_work.last_failure)
        self.assertNotEqual(work.work_id, retry_work.work_id)
        self.assertEqual(
            derive_canonical_work_id(
                session_id=state.session_id,
                trigger_event_id=committed.events[1].event_id,
                trigger_event_sequence=committed.events[1].sequence,
                trigger=committed.events[1].payload.next_trigger,
            ),
            retry_work.work_id,
        )

        request_digest = host_request_digest(request)
        self.assertEqual(
            tuple(
                coordinator_module._stable_id(
                    "dlg.event",
                    request_digest,
                    index,
                )
                for index in (1, 2)
            ),
            tuple(event.event_id for event in committed.events),
        )

        replay = self.host_service()[0].publish(request)
        self.assertTrue(replay.replayed)
        self.assertEqual(committed.events, replay.events)
        self.assertEqual(committed.state, replay.state)

    def test_dead_letter_and_explicit_recovery_each_advance_once(self):
        first = self.coordinator().resolve(config("dlg-a"))
        state = first.dialogue_state
        work = state.session_work
        assert work is not None
        permanent = WorkFailure(
            failure_id="failure.host-permanent",
            category=WorkFailureCategory.HOST_PERMANENT,
            safe_error_code="HOST_REJECTED",
            proof_digest=digest("permanent-proof"),
        )
        evidence = EvidenceCheck(
            EvidenceHealth.CURRENT,
            first.config.evidence_digest,
        )
        _, failed = self.publish_host(
            DialogueExecutionRequest(
                session_id=state.session_id,
                expected_conversation_version=state.conversation_version,
                command=ReportWorkFailure(
                    "cmd.failure-dead-letter",
                    work.work_id,
                    permanent,
                ),
                context=DecisionContext(
                    registry_generation=first.registry_state.generation,
                    trigger=None,
                    evidence=evidence,
                ),
                occurred_at=first.config.created_at,
                actor=HOST,
            )
        )

        self.assertEqual(
            (WorkFailed, WorkDeadLettered),
            tuple(type(event.payload) for event in failed.events),
        )
        self.assertEqual(
            ((1, 1), (1, 2)),
            tuple(
                (event.from_version, event.to_version)
                for event in failed.events
            ),
        )
        self.assertEqual(ConversationPhase.RECOVERABLE_ERROR, failed.state.phase)
        dead_work = failed.state.session_work
        assert dead_work is not None
        self.assertEqual(WorkStatus.DEAD_LETTER, dead_work.status)

        recovery_trigger = replace(
            dead_work.trigger,
            work_id="trigger.explicit-recovery",
        )
        recovered = self.coordinator().execute(
            DialogueExecutionRequest(
                session_id=state.session_id,
                expected_conversation_version=failed.state.conversation_version,
                command=RecoverWork(
                    "cmd.explicit-recovery",
                    dead_work.work_id,
                    WorkRecoveryAction.RETRY,
                ),
                context=DecisionContext(
                    registry_generation=first.registry_state.generation,
                    trigger=recovery_trigger,
                    evidence=evidence,
                ),
                occurred_at=first.config.created_at,
                actor=HOST,
            )
        )

        self.assertEqual(1, len(recovered.events))
        self.assertIs(type(recovered.events[0].payload), WorkRecoveryRequested)
        self.assertEqual(
            (2, 3),
            (
                recovered.events[0].from_version,
                recovered.events[0].to_version,
            ),
        )
        self.assertEqual(ConversationPhase.WAITING_HOST, recovered.state.phase)
        recovery_work = recovered.state.session_work
        assert recovery_work is not None
        self.assertEqual(WorkStatus.QUEUED, recovery_work.status)
        self.assertEqual(1, recovery_work.attempt)
        self.assertIsNone(recovery_work.last_failure)
        self.assertEqual(
            derive_canonical_work_id(
                session_id=state.session_id,
                trigger_event_id=recovered.events[0].event_id,
                trigger_event_sequence=recovered.events[0].sequence,
                trigger=recovery_trigger,
            ),
            recovery_work.work_id,
        )

    def test_setup_state_handoff_prepares_a_bootstraps_b_and_rejects_old_a(self):
        first = self.coordinator().resolve(config("dlg-a"))
        switched = self.coordinator().resolve(config("dlg-b"))

        self.assertEqual("dlg-a", switched.transitioned_from)
        self.assertEqual("dlg-b", switched.registry_state.current_session_id)
        self.assertEqual(2, switched.registry_state.generation)
        self.assertEqual(1, switched.dialogue_state.sequence)
        source = self.dialogue_state("dlg-a", 1)
        self.assertEqual(2, source.sequence)
        self.assertEqual(ConversationPhase.NONE, source.phase)
        self.assertIsNone(source.session_work)

        assert first.dialogue_state.session_work is not None
        old_context = DecisionContext(
            1,
            first.dialogue_state.session_work.trigger,
            EvidenceCheck(
                EvidenceHealth.CURRENT,
                first.config.evidence_digest,
            ),
        )
        old_request = DialogueExecutionRequest(
            "dlg-a",
            source.conversation_version,
            PauseTopic("cmd.old-a"),
            old_context,
            first.config.created_at,
            HOST,
        )
        with self.assertRaisesRegex(CoordinatorError, "SESSION_DEACTIVATED"):
            self.coordinator().execute(old_request)

    def test_active_topic_handoff_pauses_the_topic_without_cross_session_state(self):
        first = self.coordinator().resolve(config("dlg-a"))
        active = self.make_active_topic(first)
        self.assertIsNotNone(active.state.active_topic)

        switched = self.coordinator().resolve(config("dlg-b"))
        source = self.dialogue_state("dlg-a", 1)
        self.assertEqual(ConversationPhase.NONE, source.phase)
        self.assertIsNone(source.active_topic)
        self.assertEqual(1, len(source.paused_topics))
        self.assertEqual("topic-1", source.paused_topics[0].topic_run_id)
        self.assertIsNone(switched.dialogue_state.active_topic)
        self.assertEqual((), switched.dialogue_state.paused_topics)

    def test_restart_finishes_each_durable_handoff_stage(self):
        for crash_stage in (
            "config_persisted",
            "handoff_started",
            "source_prepared",
            "handoff_completed",
            "dialogue_started",
        ):
            with self.subTest(crash_stage=crash_stage):
                self._reset_storage()
                self.coordinator().resolve(config("dlg-a"))

                def crash(stage, expected_stage=crash_stage):
                    if stage == expected_stage:
                        raise InjectedCrash(stage)

                with self.assertRaisesRegex(InjectedCrash, crash_stage):
                    self.coordinator(crash).resolve(config("dlg-b"))

                interrupted = self.registry_state()
                if crash_stage == "config_persisted":
                    self.assertIsNone(interrupted.pending_handoff)
                    self.assertEqual("dlg-a", interrupted.current_session_id)
                    self.assertEqual(0, self.dialogue_state("dlg-b", 2).sequence)
                elif crash_stage in {"handoff_completed", "dialogue_started"}:
                    self.assertIsNone(interrupted.pending_handoff)
                    self.assertEqual("dlg-b", interrupted.current_session_id)
                    expected_sequence = (
                        1 if crash_stage == "dialogue_started" else 0
                    )
                    self.assertEqual(
                        expected_sequence,
                        self.dialogue_state("dlg-b", 2).sequence,
                    )
                else:
                    self.assertIsNotNone(interrupted.pending_handoff)
                    self.assertEqual("dlg-a", interrupted.current_session_id)
                    if crash_stage == "source_prepared":
                        self.assertEqual(
                            ConversationPhase.NONE,
                            self.dialogue_state("dlg-a", 1).phase,
                        )

                recovered = (
                    self.coordinator().resolve(config("dlg-b"))
                    if crash_stage == "config_persisted"
                    else self.coordinator().recover()
                )
                self.assertIsNotNone(recovered)
                assert recovered is not None
                self.assertEqual("dlg-b", recovered.registry_state.current_session_id)
                self.assertIsNone(recovered.registry_state.pending_handoff)
                self.assertEqual(1, recovered.dialogue_state.sequence)
                self.assertEqual(3, len(recovered.registry_state.receipts))

    def test_restart_after_initial_activation_bootstraps_exactly_once(self):
        for crash_stage, interrupted_sequence in (
            ("registry_created", 0),
            ("dialogue_started", 1),
        ):
            with self.subTest(crash_stage=crash_stage):
                self._reset_storage()

                def crash(stage, expected_stage=crash_stage):
                    if stage == expected_stage:
                        raise InjectedCrash(stage)

                with self.assertRaisesRegex(InjectedCrash, crash_stage):
                    self.coordinator(crash).resolve(config("dlg-a"))
                interrupted = self.registry_state()
                self.assertEqual("dlg-a", interrupted.current_session_id)
                self.assertEqual(
                    interrupted_sequence,
                    self.dialogue_state("dlg-a", 1).sequence,
                )

                recovered = self.coordinator().recover()
                self.assertIsNotNone(recovered)
                assert recovered is not None
                self.assertEqual(1, recovered.dialogue_state.sequence)
                again = self.coordinator().recover()
                self.assertIsNotNone(again)
                assert again is not None
                self.assertEqual(recovered.dialogue_state, again.dialogue_state)

    def test_session_config_codec_and_constructor_fail_closed(self):
        valid = config("dlg-a")
        raw = encode_session_config(valid)
        self.assertEqual(valid, decode_session_config(raw))

        invalid_records = (
            b"",
            b"not-json",
            (
                b'{"record_type":"dialogue_session_config",'
                + b'"record_type":"dialogue_session_config"}'
            ),
            b" " + raw,
            raw + b"\n",
            b"{" + b'"payload":"' + b"x" * (64 * 1024) + b'"}',
            b"\xff",
        )
        for invalid in invalid_records:
            with self.subTest(invalid=invalid[:32]), self.assertRaisesRegex(
                CoordinatorError, "INVALID_SESSION_CONFIG_RECORD"
            ):
                decode_session_config(invalid)

        tree = json.loads(raw)
        for key, value in (
            ("session_id", []),
            ("session_id", "../escape"),
            ("created_at", ""),
            ("created_at", "not-a-timestamp"),
            ("evidence_digest", "sha256:not-a-digest"),
            ("question_count", True),
            ("channel", "remote"),
            ("style", "exam"),
            ("focus", "unknown"),
        ):
            invalid_tree = {**tree, key: value}
            invalid_raw = json.dumps(
                invalid_tree,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            with self.subTest(key=key), self.assertRaisesRegex(
                CoordinatorError, "INVALID_SESSION_CONFIG_RECORD"
            ):
                decode_session_config(invalid_raw)

        wrong_protocol = {**tree, "protocol_version": "x-sync.dialogue.v999"}
        with self.assertRaisesRegex(
            CoordinatorError,
            "INVALID_SESSION_CONFIG_RECORD",
        ):
            decode_session_config(
                json.dumps(
                    wrong_protocol,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            )

        with self.assertRaisesRegex(
            CoordinatorError,
            "IDENTITY_DERIVATION_FAILED",
        ):
            coordinator_module._stable_id("bad/prefix", digest("identity"))

        for invalid in (
            None,
            replace(valid, learner_id=""),
            replace(valid, repository_id=" leading"),
            replace(valid, runtime_epoch="bad\nvalue"),
            replace(valid, task_scope=""),
            replace(valid, language=""),
            replace(valid, question_count=101),
        ):
            with self.subTest(config=invalid), self.assertRaisesRegex(
                CoordinatorError, "INVALID_SESSION_CONFIG"
            ):
                self.coordinator().resolve(invalid)

        with self.assertRaisesRegex(
            CoordinatorError, "INVALID_COORDINATOR_CONFIGURATION"
        ):
            DialogueCoordinator(
                self.dialogues,
                self.locks,
                "bad/registry",
                evidence_verifier=lambda item: EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    item.evidence_digest,
                ),
            )

    def test_empty_recovery_config_conflict_and_execute_guards_are_stable(self):
        self.assertIsNone(self.coordinator().recover())
        first = self.coordinator().resolve(config("dlg-a"))

        conflicting = replace(first.config, task_scope="different task")
        with self.assertRaisesRegex(CoordinatorError, "SESSION_CONFIG_CONFLICT"):
            self.coordinator().resolve(conflicting)

        for invalid in (None, object()):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                CoordinatorError, "INVALID_DIALOGUE_EXECUTION"
            ):
                self.coordinator().execute(invalid)

        state = first.dialogue_state
        assert state.session_work is not None
        context = DecisionContext(
            first.registry_state.generation,
            state.session_work.trigger,
            EvidenceCheck(EvidenceHealth.CURRENT, first.config.evidence_digest),
        )
        stale_version = DialogueExecutionRequest(
            state.session_id,
            state.conversation_version + 1,
            PresentCandidates("cmd.stale", ("one",)),
            context,
            first.config.created_at,
            HOST,
        )
        with self.assertRaisesRegex(
            CoordinatorError, "HOST_WORK_SERVICE_REQUIRED"
        ):
            self.coordinator().execute(stale_version)

        invalid_transition = replace(
            stale_version,
            expected_conversation_version=state.conversation_version,
            command=PauseTopic("cmd.invalid-transition"),
        )
        with self.assertRaisesRegex(CoordinatorError, "TOPIC_STATE_CONFLICT"):
            self.coordinator().execute(invalid_transition)

        request, _ = self.present_candidates(first, "cmd.idempotent")
        changed_body = replace(
            request,
            command=PresentCandidates(
                request.command.command_id,
                ("different",),
            ),
        )
        with self.assertRaisesRegex(RuntimeError, "IDEMPOTENCY_CONFLICT"):
            self.host_service()[0].publish(changed_body)

    def test_evidence_verifier_is_authoritative_at_bootstrap_and_commit(self):
        wrong_digest = DialogueCoordinator(
            self.dialogues,
            self.locks,
            self.registry_id,
            evidence_verifier=lambda _item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                digest("wrong-evidence"),
            ),
        )
        with self.assertRaisesRegex(
            CoordinatorError, "EVIDENCE_VERIFICATION_FAILED"
        ):
            wrong_digest.resolve(config("dlg-a"))

        calls = 0

        def changing_verifier(item):
            nonlocal calls
            calls += 1
            health = EvidenceHealth.STALE if calls >= 3 else EvidenceHealth.CURRENT
            return EvidenceCheck(health, item.evidence_digest)

        coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            self.registry_id,
            evidence_verifier=changing_verifier,
        )
        first = coordinator.resolve(config("dlg-a"))
        state = first.dialogue_state
        assert state.session_work is not None
        request = DialogueExecutionRequest(
            state.session_id,
            state.conversation_version,
            PresentCandidates("cmd.evidence-change", ("topic",)),
            DecisionContext(
                first.registry_state.generation,
                state.session_work.trigger,
                EvidenceCheck(
                    EvidenceHealth.CURRENT,
                    first.config.evidence_digest,
                ),
            ),
            first.config.created_at,
            HOST,
        )
        with self.assertRaisesRegex(
            CoordinatorError,
            "HOST_WORK_SERVICE_REQUIRED",
        ):
            coordinator.execute(request)
        self.assertEqual(1, self.dialogue_state("dlg-a", 1).sequence)

    def test_switch_bootstraps_a_current_new_source_before_fencing_it(self):
        def crash_initial(stage):
            if stage == "registry_created":
                raise InjectedCrash(stage)

        with self.assertRaisesRegex(InjectedCrash, "registry_created"):
            self.coordinator(crash_initial).resolve(config("dlg-a"))
        self.assertEqual(0, self.dialogue_state("dlg-a", 1).sequence)

        switched = self.coordinator().resolve(config("dlg-b"))
        self.assertEqual("dlg-b", switched.registry_state.current_session_id)
        self.assertIsNone(switched.registry_state.pending_handoff)
        source = self.dialogue_state("dlg-a", 1)
        self.assertEqual(1, source.registry_generation)
        self.assertEqual(ConversationPhase.NONE, source.phase)

        self._reset_storage()
        self.coordinator().resolve(config("dlg-a"))

        def crash_after_activation(stage):
            if stage == "handoff_completed":
                raise InjectedCrash(stage)

        with self.assertRaisesRegex(InjectedCrash, "handoff_completed"):
            self.coordinator(crash_after_activation).resolve(config("dlg-b"))
        self.assertEqual(0, self.dialogue_state("dlg-b", 2).sequence)
        switched_again = self.coordinator().resolve(config("dlg-c"))
        self.assertEqual("dlg-c", switched_again.registry_state.current_session_id)
        self.assertIsNone(switched_again.registry_state.pending_handoff)

    def test_resolve_resumes_only_the_durable_pending_target(self):
        self.coordinator().resolve(config("dlg-a"))

        def crash(stage):
            if stage == "handoff_started":
                raise InjectedCrash(stage)

        with self.assertRaisesRegex(InjectedCrash, "handoff_started"):
            self.coordinator(crash).resolve(config("dlg-b"))
        resumed = self.coordinator().resolve(config("dlg-b"))
        self.assertEqual("dlg-b", resumed.registry_state.current_session_id)
        self.assertFalse(resumed.created)
        self.assertIsNone(resumed.registry_state.pending_handoff)

        self._reset_storage()
        self.coordinator().resolve(config("dlg-a"))
        with self.assertRaisesRegex(InjectedCrash, "handoff_started"):
            self.coordinator(crash).resolve(config("dlg-b"))
        with self.assertRaisesRegex(CoordinatorError, "HANDOFF_TARGET_CONFLICT"):
            self.coordinator().resolve(config("dlg-c"))

        recovered = self.coordinator().recover()
        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual("dlg-b", recovered.registry_state.current_session_id)

    def test_execute_finishes_pending_handoff_before_rejecting_old_session(self):
        first = self.coordinator().resolve(config("dlg-a"))
        assert first.dialogue_state.session_work is not None

        def crash(stage):
            if stage == "handoff_started":
                raise InjectedCrash(stage)

        with self.assertRaisesRegex(InjectedCrash, "handoff_started"):
            self.coordinator(crash).resolve(config("dlg-b"))

        request = DialogueExecutionRequest(
            "dlg-a",
            first.dialogue_state.conversation_version,
            PauseTopic("cmd.old-while-pending"),
            DecisionContext(
                1,
                first.dialogue_state.session_work.trigger,
                EvidenceCheck(EvidenceHealth.CURRENT, first.config.evidence_digest),
            ),
            first.config.created_at,
            HOST,
        )
        with self.assertRaisesRegex(CoordinatorError, "SESSION_DEACTIVATED"):
            self.coordinator().execute(request)
        self.assertEqual("dlg-b", self.registry_state().current_session_id)

    def test_config_storage_rejects_conflicts_and_swapped_identity(self):
        coordinator = self.coordinator()
        config_a = config("dlg-a")
        coordinator._persist_config(config_a)
        with self.assertRaisesRegex(CoordinatorError, "SESSION_CONFIG_CONFLICT"):
            coordinator._persist_config(replace(config_a, task_scope="changed"))
        with self.assertRaisesRegex(CoordinatorError, "SESSION_CONFIG_CONFLICT"):
            coordinator._load_config_checked("dlg-a", digest("wrong-config"))

        self._reset_storage()
        swapped = self.dialogues.ensure_directory(
            session_directory_component("dlg-a")
        )
        try:
            swapped.write_immutable(
                "config.json",
                encode_session_config(config("dlg-b")),
            )
        finally:
            swapped.close()
        with self.assertRaisesRegex(CoordinatorError, "SESSION_CONFIG_CONFLICT"):
            self.coordinator()._load_config("dlg-a")

        with mock.patch.object(
            SecureDirectory,
            "write_immutable",
            side_effect=secure_fs.SecureFsError("FILESYSTEM_ERROR"),
        ), self.assertRaisesRegex(secure_fs.SecureFsError, "FILESYSTEM_ERROR"):
            self.coordinator()._persist_config(config("dlg-c"))

    def test_public_operations_normalize_storage_failures(self):
        closed_path = self.root_path / "closed-dialogues"
        closed_path.mkdir(mode=0o700)
        closed = SecureDirectory.open(closed_path)
        closed.close()
        coordinator = DialogueCoordinator(
            closed,
            self.locks,
            "closed-registry",
            evidence_verifier=lambda item: EvidenceCheck(
                EvidenceHealth.CURRENT,
                item.evidence_digest,
            ),
        )
        request = DialogueExecutionRequest(
            "dlg-a",
            0,
            PauseTopic("cmd.closed"),
            DecisionContext(
                1,
                None,
                EvidenceCheck(EvidenceHealth.CURRENT, config("dlg-a").evidence_digest),
            ),
            config("dlg-a").created_at,
            HOST,
        )
        for operation in (
            lambda: coordinator.resolve(config("dlg-a")),
            coordinator.recover,
            lambda: coordinator.execute(request),
        ):
            with self.subTest(operation=operation), self.assertRaisesRegex(
                CoordinatorError,
                "DIRECTORY_CLOSED",
            ):
                operation()

    def test_internal_registry_guards_reject_impossible_or_idle_work(self):
        coordinator = self.coordinator()
        with self.locks.registry_exclusive() as authority:
            log = coordinator._open_registry(authority)
            try:
                coordinator._continue_handoff(log, authority)
                with self.assertRaisesRegex(
                    CoordinatorError,
                    "REGISTRY_STATE_CONFLICT",
                ):
                    coordinator._commit_registry_begin(log, config("dlg-b"), authority)
            finally:
                log.close()

    def test_quiesce_proof_validation_rejects_incomplete_or_unrelated_tails(self):
        pending = PendingHandoff(
            "handoff.test",
            "dlg-a",
            ActivateTarget("dlg-b", digest("config-b")),
            2,
            1,
        )
        initial = initial_dialogue_state("dlg-a", 1)
        empty_tip = DialogueTip(initial, None, None, (), 1)
        with self.assertRaisesRegex(CoordinatorError, "HANDOFF_PROOF_INVALID"):
            self.coordinator()._proof_from_tip(pending, empty_tip)
        with self.assertRaisesRegex(CoordinatorError, "HANDOFF_PROOF_INVALID"):
            self.coordinator()._validate_existing_quiesce(
                mock.Mock(),
                empty_tip,
                pending,
            )

        nonempty_tip = replace(empty_tip, state=replace(initial, sequence=1))
        no_tail = mock.Mock()
        no_tail.read_committed.return_value = ()
        with self.assertRaisesRegex(CoordinatorError, "HANDOFF_PROOF_INVALID"):
            self.coordinator()._validate_existing_quiesce(
                no_tail,
                nonempty_tip,
                pending,
            )

        unrelated = mock.Mock()
        unrelated.read_committed.return_value = (mock.Mock(payload=object()),)
        with self.assertRaisesRegex(CoordinatorError, "HANDOFF_PROOF_INVALID"):
            self.coordinator()._validate_existing_quiesce(
                unrelated,
                nonempty_tip,
                pending,
            )

    def test_bootstrap_rejects_wrong_registry_and_closed_existing_dialogue(self):
        first = self.coordinator().resolve(config("dlg-a"))
        coordinator = self.coordinator()
        with self.locks.registry_exclusive() as authority:
            with self.assertRaisesRegex(CoordinatorError, "SESSION_DEACTIVATED"):
                coordinator._bootstrap_current(
                    first.registry_state,
                    config("dlg-b"),
                    authority,
                )

            bad_log = mock.Mock()
            bad_log.tip.return_value = DialogueTip(
                replace(first.dialogue_state, lifecycle=SessionLifecycle.ENDED),
                digest("last-event"),
                digest("last-marker"),
                (),
                first.registry_state.generation,
            )
            with mock.patch.object(
                coordinator,
                "_open_existing_dialogue",
                return_value=bad_log,
            ), self.assertRaisesRegex(
                CoordinatorError,
                "DIALOGUE_BOOTSTRAP_CONFLICT",
            ):
                coordinator._bootstrap_current(
                    first.registry_state,
                    first.config,
                    authority,
                )
            bad_log.close.assert_called_once_with()

    def test_execute_rejects_evidence_context_drift_and_verifier_failure(self):
        first = self.coordinator().resolve(config("dlg-a"))
        assert first.dialogue_state.session_work is not None
        request = DialogueExecutionRequest(
            "dlg-a",
            first.dialogue_state.conversation_version,
            PresentCandidates("cmd.stale-context", ("topic",)),
            DecisionContext(
                first.registry_state.generation,
                first.dialogue_state.session_work.trigger,
                EvidenceCheck(EvidenceHealth.STALE, first.config.evidence_digest),
            ),
            first.config.created_at,
            HOST,
        )
        with self.assertRaisesRegex(
            CoordinatorError,
            "HOST_WORK_SERVICE_REQUIRED",
        ):
            self.coordinator().execute(request)

        self._reset_storage()

        def unavailable(_item):
            raise RuntimeError("verifier offline")

        coordinator = DialogueCoordinator(
            self.dialogues,
            self.locks,
            self.registry_id,
            evidence_verifier=unavailable,
        )
        with self.assertRaisesRegex(
            CoordinatorError,
            "EVIDENCE_VERIFICATION_FAILED",
        ):
            coordinator.resolve(config("dlg-a"))

    def test_recovery_quiesces_a_durable_legacy_new_source(self):
        def crash_initial(stage):
            if stage == "registry_created":
                raise InjectedCrash(stage)

        coordinator = self.coordinator(crash_initial)
        with self.assertRaisesRegex(InjectedCrash, "registry_created"):
            coordinator.resolve(config("dlg-a"))

        coordinator = self.coordinator()
        with self.locks.registry_exclusive() as authority:
            log = coordinator._open_registry(authority)
            try:
                coordinator._prepare_empty_target(config("dlg-b"), 2, authority)
                coordinator._commit_registry_begin(log, config("dlg-b"), authority)
            finally:
                log.close()

        self.assertIsNotNone(self.registry_state().pending_handoff)
        recovered = coordinator.recover()
        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual("dlg-b", recovered.registry_state.current_session_id)
        self.assertIsNone(recovered.registry_state.pending_handoff)
        source = self.dialogue_state("dlg-a", 1)
        self.assertEqual(1, source.sequence)
        self.assertEqual(ConversationPhase.NONE, source.phase)

    def _reset_storage(self):
        self.locks.close()
        self.dialogues.close()
        self.root.close()
        self.temporary.cleanup()
        self.temporary = TemporaryDirectory()
        self.root_path = Path(self.temporary.name).resolve()
        self.root = SecureDirectory.open(self.root_path)
        self.dialogues = self.root.ensure_directory("dialogues")
        self.locks = DomainLockManager(self.root_path / "locks")


if __name__ == "__main__":
    unittest.main()
