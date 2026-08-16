import ast
import hashlib
import inspect
import unittest
from pathlib import Path
from typing import get_args

import tests.xsync_v2_path  # noqa: F401

# isort: split
from xsync_v2 import event_store, registry_store
from xsync_v2.browser_http import (
    BrowserApi,
    BrowserHttpRequest,
    BrowserHttpResponse,
    BrowserSseStream,
)
from xsync_v2.browser_server import BrowserServerAddress, LoopbackBrowserServer
from xsync_v2.browser_service import (
    BrowserCommandRequest,
    BrowserCommandService,
    PauseTopicIntent,
    RecoverWorkIntent,
    ResumeTopicIntent,
    SelectTopicIntent,
    SubmitTurnIntent,
    SwitchTopicIntent,
)
from xsync_v2.coordinator import (
    DialogueCoordinator,
    decode_session_config,
    encode_session_config,
    session_config_digest,
)
from xsync_v2.dispatch import (
    AfterCommitDispatcher,
    CommittedFactCollector,
    dialogue_batch,
    registry_batch,
)
from xsync_v2.domain import DialogueCommand
from xsync_v2.evidence import (
    EvidenceClaimType,
    EvidenceKind,
    EvidenceSnapshot,
    EvidenceSource,
    EvidenceStoreError,
    FrozenEvidenceEntry,
    SessionEvidenceStore,
    decode_evidence_snapshot,
    encode_evidence_snapshot,
)
from xsync_v2.host_api import HostApi, HostApiError, HostApiResponse
from xsync_v2.host_cli import HostCliError
from xsync_v2.host_cli import main as host_cli_main
from xsync_v2.host_context import (
    EvidenceContextClaim,
    HostContextCapsule,
    HostContextSource,
    LearnerTurnContext,
    build_host_context,
    encode_host_context,
)
from xsync_v2.host_control import (
    HostClaimEnvelope,
    HostControl,
    HostControlError,
    HostReclaimRequest,
    HostWaitOutcome,
    HostWorkAdvanced,
    HostWorkDisposition,
    HostWorkMetadata,
)
from xsync_v2.host_ipc import HostIpcClient, HostIpcError, HostIpcServer
from xsync_v2.host_result import (
    DialogueTurnResult,
    HostResultError,
    HostResultKind,
    TopicCandidatesResult,
    TopicStartedResult,
    WorkFailureResult,
    decode_host_result,
    encode_host_result,
    host_result_command,
)
from xsync_v2.host_supervisor import (
    HostSupervisor,
    HostSupervisorError,
    HostSupervisorSubmission,
    WaitStrategy,
)
from xsync_v2.host_work import (
    HostResultPublishRequest,
    HostWorkPublishRequest,
    HostWorkService,
    LeaseExhaustionRecordRequest,
    authoritative_work_snapshot,
    host_command_id,
)
from xsync_v2.lease_store import (
    CurrentRunnableWork,
    CurrentWorkObservation,
    LeaseExhaustionProof,
    LeaseStore,
)
from xsync_v2.observer import ObserverHub
from xsync_v2.observers.public_stream import (
    PublicStreamObserver,
    PublicStreamSubscription,
)
from xsync_v2.observers.work_wake import WorkWakeObserver
from xsync_v2.repository_context import RepositoryHostContextProvider
from xsync_v2.runtime import DialogueRuntime, DialogueRuntimeError
from xsync_v2.secure_fs import SecureDirectory, SecureDirectoryIdentity
from xsync_v2.state_machine import TRANSITION_TABLE, decide, reduce
from xsync_v2.submission_store import (
    SubmissionHandleRecord,
    SubmissionHandleStore,
    SubmissionRegistrationOutcome,
    SubmissionStoreError,
)
from xsync_v2.work import derive_runnable_work

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "skills" / "x-sync" / "scripts" / "xsync_v2"
V1_RUNTIME = ROOT / "skills" / "x-sync" / "scripts" / "xsync.py"
V1_RUNTIME_SHA256 = (
    "9fa72d3b3c9a9fd8ebf03c9653d70d5c78a518e3cc74edf07a07eac0dd6baf5b"
)


def imports(path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


class ArchitectureTest(unittest.TestCase):
    def test_domain_and_state_machine_have_no_io_dependencies(self):
        forbidden = {
            "asyncio", "http", "os", "pathlib", "random", "socket",
            "subprocess", "time", "urllib",
        }
        for name in ("domain.py", "registry.py", "state_machine.py"):
            self.assertFalse(forbidden & imports(PACKAGE / name), name)

    def test_observer_has_no_command_or_writer_dependency(self):
        observer_paths = (
            PACKAGE / "observer.py",
            PACKAGE / "observers" / "public_stream.py",
            PACKAGE / "observers" / "work_wake.py",
        )
        for path in observer_paths:
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("CommandService", text, path.name)
            self.assertNotIn("EventWriter", text, path.name)
            self.assertNotIn("decide(", text, path.name)
            self.assertNotIn("reduce(", text, path.name)
            self.assertNotIn("LeaseStore", text, path.name)

    def test_host_control_is_transport_and_model_neutral(self):
        paths = (
            PACKAGE / "host_context.py",
            PACKAGE / "host_control.py",
            PACKAGE / "host_result.py",
            PACKAGE / "host_supervisor.py",
        )
        forbidden_imports = {
            "asyncio",
            "http",
            "socket",
            "subprocess",
            "urllib",
        }
        for path in paths:
            text = path.read_text(encoding="utf-8")
            self.assertFalse(forbidden_imports & imports(path), path.name)
            self.assertNotIn("decide(", text, path.name)
            self.assertNotIn("reduce(", text, path.name)
        api_path = PACKAGE / "host_api.py"
        api_text = api_path.read_text(encoding="utf-8")
        self.assertNotIn("state_machine", api_text)
        self.assertNotIn("coordinator", imports(api_path))
        ipc_path = PACKAGE / "host_ipc.py"
        ipc_text = ipc_path.read_text(encoding="utf-8")
        self.assertNotIn("state_machine", ipc_text)
        self.assertNotIn("coordinator", imports(ipc_path))
        cli_path = PACKAGE / "host_cli.py"
        cli_text = cli_path.read_text(encoding="utf-8")
        self.assertNotIn("state_machine", cli_text)
        self.assertNotIn("coordinator", imports(cli_path))

    def test_browser_http_is_only_an_authenticated_dto_adapter(self):
        http_path = PACKAGE / "browser_http.py"
        service_path = PACKAGE / "browser_service.py"
        server_path = PACKAGE / "browser_server.py"
        http_text = http_path.read_text(encoding="utf-8")
        service_text = service_path.read_text(encoding="utf-8")
        self.assertNotIn("state_machine", http_text)
        self.assertNotIn("coordinator", imports(http_path))
        self.assertNotIn("decide(", http_text)
        self.assertNotIn("reduce(", http_text)
        self.assertNotIn("decide(", service_text)
        self.assertNotIn("reduce(", service_text)
        self.assertNotIn("state_machine", server_path.read_text(encoding="utf-8"))
        self.assertNotIn("coordinator", imports(server_path))

    def test_transition_table_is_closed_and_exhaustive(self):
        self.assertEqual(
            frozenset(get_args(DialogueCommand)),
            frozenset(TRANSITION_TABLE),
        )

    def test_public_kernel_api_is_documented(self):
        public_api = (
            decide,
            reduce,
            ObserverHub,
            ObserverHub.register_fixed,
            ObserverHub.freeze,
            ObserverHub.assert_command_entry_allowed,
            ObserverHub.publish,
            DialogueCoordinator,
            DialogueCoordinator.resolve,
            DialogueCoordinator.recover,
            DialogueCoordinator.execute,
            DialogueCoordinator.replay_committed,
            encode_session_config,
            decode_session_config,
            session_config_digest,
            dialogue_batch,
            registry_batch,
            CommittedFactCollector,
            CommittedFactCollector.capture_dialogue,
            CommittedFactCollector.capture_registry,
            CommittedFactCollector.freeze,
            AfterCommitDispatcher,
            AfterCommitDispatcher.assert_command_entry_allowed,
            AfterCommitDispatcher.publish_all,
            AfterCommitDispatcher.publish_facts,
            derive_runnable_work,
            LeaseStore,
            LeaseStore.claim,
            LeaseStore.renew,
            LeaseStore.reclaim,
            LeaseStore.read,
            LeaseStore.current_work,
            LeaseStore.current_runnable,
            LeaseStore.observe_current_work,
            CurrentRunnableWork,
            CurrentWorkObservation,
            LeaseExhaustionProof,
            HostWorkPublishRequest,
            HostResultPublishRequest,
            LeaseExhaustionRecordRequest,
            HostWorkService,
            HostWorkService.publish,
            HostWorkService.publish_result,
            HostWorkService.record_lease_exhaustion,
            authoritative_work_snapshot,
            host_command_id,
            EvidenceContextClaim,
            LearnerTurnContext,
            HostContextSource,
            HostContextCapsule,
            build_host_context,
            encode_host_context,
            HostControlError,
            HostClaimEnvelope,
            HostWorkMetadata,
            HostWaitOutcome,
            HostReclaimRequest,
            HostWorkDisposition,
            HostWorkAdvanced,
            HostControl,
            HostControl.notify,
            HostControl.current,
            HostControl.wait,
            HostControl.claim,
            HostControl.renew,
            HostControl.reclaim,
            HostControl.publish_result,
            HostApiError,
            HostApiResponse,
            HostApi,
            HostApi.handle,
            HostCliError,
            host_cli_main,
            HostIpcError,
            HostIpcServer,
            HostIpcServer.path,
            HostIpcServer.fatal_error,
            HostIpcServer.start,
            HostIpcServer.close,
            HostIpcClient,
            HostIpcClient.call,
            HostResultError,
            HostResultKind,
            TopicCandidatesResult,
            TopicStartedResult,
            DialogueTurnResult,
            WorkFailureResult,
            decode_host_result,
            encode_host_result,
            host_result_command,
            HostSupervisorError,
            HostSupervisorSubmission,
            WaitStrategy,
            HostSupervisor,
            HostSupervisor.submit,
            HostSupervisor.close,
            HostSupervisor.run,
            SubmissionStoreError,
            SubmissionHandleRecord,
            SubmissionRegistrationOutcome,
            SubmissionHandleStore,
            SubmissionHandleStore.register,
            SubmissionHandleStore.resolve,
            SubmissionHandleStore.confirm,
            SubmissionHandleStore.close,
            BrowserCommandRequest,
            SubmitTurnIntent,
            SelectTopicIntent,
            PauseTopicIntent,
            SwitchTopicIntent,
            ResumeTopicIntent,
            RecoverWorkIntent,
            BrowserCommandService,
            BrowserCommandService.current,
            BrowserCommandService.execute,
            BrowserHttpRequest,
            BrowserHttpResponse,
            BrowserHttpResponse.header,
            BrowserSseStream,
            BrowserSseStream.read,
            BrowserSseStream.close,
            BrowserApi,
            BrowserApi.handle,
            BrowserApi.open_stream,
            BrowserApi.error_response,
            BrowserServerAddress,
            BrowserServerAddress.authority,
            BrowserServerAddress.origin,
            LoopbackBrowserServer,
            LoopbackBrowserServer.launch_url,
            LoopbackBrowserServer.start,
            LoopbackBrowserServer.close,
            PublicStreamObserver,
            PublicStreamObserver.subscribe,
            PublicStreamObserver.on_batch,
            PublicStreamSubscription,
            PublicStreamSubscription.read_available,
            PublicStreamSubscription.wait_available,
            PublicStreamSubscription.close,
            WorkWakeObserver,
            WorkWakeObserver.on_batch,
            EvidenceStoreError,
            EvidenceKind,
            EvidenceClaimType,
            EvidenceSource,
            FrozenEvidenceEntry,
            FrozenEvidenceEntry.location,
            EvidenceSnapshot,
            encode_evidence_snapshot,
            decode_evidence_snapshot,
            SessionEvidenceStore,
            SessionEvidenceStore.capture,
            SessionEvidenceStore.verify,
            SessionEvidenceStore.load,
            SessionEvidenceStore.close,
            RepositoryHostContextProvider,
            RepositoryHostContextProvider.load_context,
            DialogueRuntimeError,
            DialogueRuntime,
            DialogueRuntime.host,
            DialogueRuntime.host_api,
            DialogueRuntime.browser,
            DialogueRuntime.public_stream,
            DialogueRuntime.evidence,
            DialogueRuntime.resolve,
            DialogueRuntime.recover,
            DialogueRuntime.replay_committed,
            DialogueRuntime.start_browser,
            DialogueRuntime.close_browser,
            DialogueRuntime.start_host_ipc,
            DialogueRuntime.close_host_ipc,
            DialogueRuntime.close,
            SecureDirectoryIdentity,
            SecureDirectory,
            SecureDirectory.identity,
        )
        self.assertTrue(all(inspect.getdoc(item) for item in public_api))

    def test_raw_transaction_logs_are_not_public_write_apis(self):
        self.assertFalse(hasattr(event_store, "DialogueTransactionLog"))
        self.assertFalse(hasattr(registry_store, "RegistryTransactionLog"))
        self.assertFalse(hasattr(HostControl, "publish"))

    def test_runtime_core_is_host_neutral(self):
        forbidden = ("anthropic", "claude", "codex", "openai")
        for path in PACKAGE.rglob("*.py"):
            text = path.read_text(encoding="utf-8").lower()
            self.assertFalse(any(name in text for name in forbidden), path.name)

    def test_v1_runtime_bytes_are_frozen_for_this_slice(self):
        digest = hashlib.sha256(V1_RUNTIME.read_bytes()).hexdigest()
        self.assertEqual(V1_RUNTIME_SHA256, digest)


if __name__ == "__main__":
    unittest.main()
