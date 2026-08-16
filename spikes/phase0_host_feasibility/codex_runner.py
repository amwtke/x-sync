#!/usr/bin/env python3
"""Run the Codex half of the Phase 0 same-unfinished-turn spike once.

This runner installs nothing and changes no user configuration.  It supplies a
single temporary MCP server through command-line ``-c`` overrides, starts one
ephemeral JSONL Codex execution, and verifies that three sequential MCP calls
completed inside its one turn.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Callable, Mapping, Sequence


MCP_SERVER_ID = "xsync_spike"
MCP_TOOL = "publish_and_wait"
PROBE_TOKEN_ENV = "XSYNC_PHASE0_PROBE_TOKEN"

DEFAULT_PROMPT = """\
This is the X-Sync Phase 0 same-unfinished-turn feasibility spike.
Use only the xsync_spike MCP tool publish_and_wait for interaction.
Call it exactly three times, sequentially, with round values 1, 2, and 3.
For round 1, ask one short, friendly repository-onboarding question.
After each tool result, use that browser answer to make the next question a
natural Socratic follow-up. Do not finish, summarize, or emit a final answer
before the round 3 tool result arrives. After round 3 returns, reply with one
short sentence confirming that all three rounds completed in this same turn.
"""


class RunnerError(RuntimeError):
    """The Codex process or its observable event trace failed the spike gate."""


@dataclass(frozen=True)
class TraceArtifact:
    path: str
    sha256: str
    sanitized: bool = True

    def summary(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "sanitized": self.sanitized,
        }


@dataclass(frozen=True)
class ToolCallEvidence:
    item_id: str
    round_number: int
    question: str
    answer_sha256: str
    start_position: int
    completion_position: int


@dataclass(frozen=True)
class RunEvidence:
    command: tuple[str, ...]
    thread_id: str
    completed_rounds: tuple[int, ...]
    event_count: int
    events: tuple[dict[str, Any], ...]
    tool_calls: tuple[ToolCallEvidence, ...]
    final_message: str
    final_message_position: int
    trace_artifact: TraceArtifact | None = None

    def summary(self) -> dict[str, Any]:
        """Return the narrow sub-gate result, not a full Host verdict."""
        return {
            "gate": "same_turn_three_rounds",
            "status": "PASS",
            "thread_id": self.thread_id,
            "completed_rounds": list(self.completed_rounds),
            "event_count": self.event_count,
            "codex_invocation_count": 1,
            "single_codex_invocation": True,
            "process_tree_verified": False,
            "single_turn_event_sequence": True,
            "sequential_tool_calls": True,
            "distinct_questions": True,
            "final_after_round_three": True,
            "tool_call_ids": [call.item_id for call in self.tool_calls],
            "trace_artifact": (
                None if self.trace_artifact is None else self.trace_artifact.summary()
            ),
        }

    def sanitized_trace(self) -> dict[str, Any]:
        """Return reproducible ordering evidence without questions or answers."""
        thread_position = next(
            position
            for position, event in enumerate(self.events)
            if event.get("type") == "thread.started"
        )
        timeline: list[dict[str, Any]] = [
            {
                "position": thread_position,
                "type": "thread.started",
                "thread_id": self.thread_id,
            }
        ]
        turn_start_position = next(
            position
            for position, event in enumerate(self.events)
            if event.get("type") == "turn.started"
        )
        turn_complete_position = next(
            position
            for position, event in enumerate(self.events)
            if event.get("type") == "turn.completed"
        )
        timeline.append({"position": turn_start_position, "type": "turn.started"})
        for call in self.tool_calls:
            common = {
                "item_id": call.item_id,
                "round": call.round_number,
                "question_chars": len(call.question),
                "question_sha256": _canonical_hash(call.question),
            }
            timeline.append(
                {"position": call.start_position, "type": "item.started", **common}
            )
            timeline.append(
                {
                    "position": call.completion_position,
                    "type": "item.completed",
                    "answer_sha256": call.answer_sha256,
                    **common,
                }
            )
        timeline.append(
            {
                "position": self.final_message_position,
                "type": "item.completed",
                "item_type": "agent_message",
                "text_chars": len(self.final_message),
                "text_sha256": _canonical_hash(self.final_message),
            }
        )
        timeline.append(
            {"position": turn_complete_position, "type": "turn.completed"}
        )
        timeline.sort(key=lambda item: int(item["position"]))
        return {
            "schema_version": 1,
            "kind": "x-sync-phase0-codex-sanitized-trace",
            "thread_id": self.thread_id,
            "event_count": self.event_count,
            "command_sha256": _canonical_hash(list(self.command)),
            "timeline": timeline,
        }


ProcessRunner = Callable[..., subprocess.CompletedProcess[str]]


def _toml_string(value: str) -> str:
    # JSON double-quoted strings are also valid TOML basic strings.
    return json.dumps(value, ensure_ascii=False)


def _toml_string_array(values: Sequence[str]) -> str:
    return "[" + ",".join(_toml_string(value) for value in values) + "]"


def build_command(
    *,
    codex_binary: str,
    python_binary: str,
    server_script: Path,
    probe_url: str,
    repository: Path,
    prompt: str = DEFAULT_PROMPT,
    tool_timeout_seconds: int = 1_800,
) -> list[str]:
    """Build one deterministic Codex invocation with an inline MCP config."""
    if tool_timeout_seconds < 1:
        raise ValueError("tool_timeout_seconds must be positive")
    server_args = [
        "-u",
        str(server_script.resolve()),
        "--probe-url",
        probe_url,
        "--probe-token-env",
        PROBE_TOKEN_ENV,
    ]
    overrides = [
        f"mcp_servers.{MCP_SERVER_ID}.command={_toml_string(python_binary)}",
        (
            f"mcp_servers.{MCP_SERVER_ID}.args="
            f"{_toml_string_array(server_args)}"
        ),
        f"mcp_servers.{MCP_SERVER_ID}.required=true",
        (
            f"mcp_servers.{MCP_SERVER_ID}.default_tools_approval_mode="
            '"approve"'
        ),
        f"mcp_servers.{MCP_SERVER_ID}.startup_timeout_sec=10",
        (
            f"mcp_servers.{MCP_SERVER_ID}.tool_timeout_sec="
            f"{tool_timeout_seconds}"
        ),
        (
            f"mcp_servers.{MCP_SERVER_ID}.enabled_tools="
            f"{_toml_string_array([MCP_TOOL])}"
        ),
        (
            f"mcp_servers.{MCP_SERVER_ID}.env_vars="
            f"{_toml_string_array([PROBE_TOKEN_ENV])}"
        ),
    ]
    command = [
        codex_binary,
        "exec",
        "--ephemeral",
        "--json",
        "--ignore-user-config",
        "--ignore-rules",
        "--strict-config",
        "--sandbox",
        "read-only",
        "--cd",
        str(repository.resolve()),
    ]
    for override in overrides:
        command.extend(["-c", override])
    command.append(prompt)
    return command


def parse_jsonl(stdout: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(stdout.splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise RunnerError(
                f"Codex stdout line {line_number} is not JSON: {exc}"
            ) from exc
        if not isinstance(event, dict):
            raise RunnerError(f"Codex stdout line {line_number} is not an object")
        events.append(event)
    if not events:
        raise RunnerError("Codex emitted no JSONL events")
    return events


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _tool_arguments(item: Mapping[str, Any]) -> tuple[int, str]:
    arguments: Any = item.get("arguments")
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError as exc:
            raise RunnerError("MCP tool arguments are not valid JSON") from exc
    if not isinstance(arguments, Mapping):
        raise RunnerError("MCP tool event is missing arguments")
    round_number = arguments.get("round")
    if isinstance(round_number, bool) or not isinstance(round_number, int):
        raise RunnerError("MCP tool event has an invalid round")
    question = arguments.get("question")
    if not isinstance(question, str) or not question.strip():
        raise RunnerError("MCP tool event has an invalid question")
    return round_number, question.strip()


def _item_id(item: Mapping[str, Any]) -> str:
    item_id = item.get("id")
    if not isinstance(item_id, str) or not item_id:
        raise RunnerError("MCP tool event is missing a stable item id")
    return item_id


def _tool_answer(item: Mapping[str, Any], expected_round: int) -> str:
    result = item.get("result")
    if not isinstance(result, Mapping):
        raise RunnerError("completed MCP tool event is missing its result")
    if result.get("isError") is True:
        raise RunnerError("completed MCP tool result is marked as an error")

    candidate: Any = result.get("structuredContent")
    if candidate is None:
        candidate = result.get("structured_content")
    if candidate is None:
        content = result.get("content")
        if isinstance(content, list):
            for block in content:
                if not isinstance(block, Mapping) or block.get("type") != "text":
                    continue
                text = block.get("text")
                if not isinstance(text, str):
                    continue
                try:
                    decoded = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(decoded, Mapping):
                    candidate = decoded
                    break
    if not isinstance(candidate, Mapping):
        raise RunnerError("completed MCP tool result lacks structured answer data")
    if candidate.get("round") != expected_round:
        raise RunnerError("completed MCP tool result has the wrong round")
    answer = candidate.get("answer")
    if not isinstance(answer, str) or not answer.strip():
        raise RunnerError("completed MCP tool result has an empty answer")
    return answer.strip()


def _agent_message_text(item: Mapping[str, Any]) -> str:
    text = item.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    content = item.get("content")
    if isinstance(content, str) and content.strip():
        return content.strip()
    if isinstance(content, list):
        parts = [
            block.get("text")
            for block in content
            if isinstance(block, Mapping)
            and isinstance(block.get("text"), str)
            and block.get("text").strip()
        ]
        if parts:
            return "\n".join(parts).strip()
    raise RunnerError("final agent message is empty")


def validate_trace(
    command: Sequence[str],
    events: Sequence[dict[str, Any]],
) -> RunEvidence:
    """Validate one ordered turn trace without claiming an observed process tree."""
    failures = [
        event
        for event in events
        if event.get("type") in {"error", "turn.failed"}
    ]
    if failures:
        raise RunnerError(f"Codex emitted a failure event: {failures[0]!r}")
    thread_events = [
        (position, event)
        for position, event in enumerate(events)
        if event.get("type") == "thread.started"
    ]
    turn_starts = [
        (position, event)
        for position, event in enumerate(events)
        if event.get("type") == "turn.started"
    ]
    turn_completions = [
        (position, event)
        for position, event in enumerate(events)
        if event.get("type") == "turn.completed"
    ]
    if len(thread_events) != 1:
        raise RunnerError(f"expected one thread.started, got {len(thread_events)}")
    if len(turn_starts) != 1 or len(turn_completions) != 1:
        raise RunnerError(
            "expected one turn.started and one turn.completed, got "
            f"{len(turn_starts)} and {len(turn_completions)}"
        )

    thread_position, thread_event = thread_events[0]
    turn_start_position = turn_starts[0][0]
    turn_complete_position = turn_completions[0][0]
    if not thread_position < turn_start_position < turn_complete_position:
        raise RunnerError("thread and turn lifecycle events are out of order")

    thread_id = thread_event.get("thread_id")
    if not isinstance(thread_id, str) or not thread_id:
        raise RunnerError("thread.started is missing thread_id")

    starts: list[tuple[int, Mapping[str, Any]]] = []
    completions: list[tuple[int, Mapping[str, Any]]] = []
    agent_messages: list[tuple[int, Mapping[str, Any]]] = []
    for position, event in enumerate(events):
        if event.get("type") not in {"item.started", "item.completed"}:
            continue
        item = event.get("item")
        if not isinstance(item, Mapping):
            continue
        if (
            item.get("type") == "agent_message"
            and event.get("type") == "item.completed"
        ):
            agent_messages.append((position, item))
            continue
        if item.get("type") != "mcp_tool_call":
            continue
        if item.get("server") != MCP_SERVER_ID or item.get("tool") != MCP_TOOL:
            raise RunnerError("trace contains an unexpected MCP tool call")
        if event.get("type") == "item.started":
            starts.append((position, item))
        else:
            completions.append((position, item))

    if len(starts) != 3 or len(completions) != 3:
        raise RunnerError(
            "expected exactly three started and three completed MCP calls, got "
            f"{len(starts)} and {len(completions)}"
        )

    tool_calls: list[ToolCallEvidence] = []
    previous_completion = turn_start_position
    questions: list[str] = []
    for expected_round, (started, completed) in enumerate(
        zip(starts, completions, strict=True), start=1
    ):
        start_position, start_item = started
        completion_position, completed_item = completed
        start_id = _item_id(start_item)
        if _item_id(completed_item) != start_id:
            raise RunnerError("MCP start/completion item ids do not match")
        start_round, start_question = _tool_arguments(start_item)
        completed_round, completed_question = _tool_arguments(completed_item)
        if (
            start_round != expected_round
            or completed_round != expected_round
            or completed_question != start_question
        ):
            raise RunnerError("MCP start/completion arguments do not match rounds 1-3")
        if not (
            previous_completion
            < start_position
            < completion_position
            < turn_complete_position
        ):
            raise RunnerError("MCP calls are overlapping or outside the one turn")
        if completed_item.get("error") not in (None, ""):
            raise RunnerError(f"MCP tool call failed: {completed_item.get('error')}")
        if completed_item.get("status") != "completed":
            raise RunnerError(
                f"MCP tool call did not complete: {completed_item.get('status')!r}"
            )
        answer = _tool_answer(completed_item, expected_round)
        questions.append(start_question)
        tool_calls.append(
            ToolCallEvidence(
                item_id=start_id,
                round_number=expected_round,
                question=start_question,
                answer_sha256=_canonical_hash(answer),
                start_position=start_position,
                completion_position=completion_position,
            )
        )
        previous_completion = completion_position

    if len(set(questions)) != 3:
        raise RunnerError("all three MCP questions must be distinct")
    if len(agent_messages) != 1:
        raise RunnerError(
            f"expected exactly one final agent message, got {len(agent_messages)}"
        )
    final_message_position, final_message_item = agent_messages[0]
    if not previous_completion < final_message_position < turn_complete_position:
        raise RunnerError("final agent message must follow the round 3 result")
    final_message = _agent_message_text(final_message_item)

    return RunEvidence(
        command=tuple(command),
        thread_id=thread_id,
        completed_rounds=(1, 2, 3),
        event_count=len(events),
        events=tuple(events),
        tool_calls=tuple(tool_calls),
        final_message=final_message,
        final_message_position=final_message_position,
    )


def write_trace_artifact(path: Path, evidence: RunEvidence) -> RunEvidence:
    """Atomically persist a 0600, content-hashed, sanitized trace artifact."""
    target = path.expanduser().resolve()
    if not target.parent.is_dir():
        raise RunnerError(f"trace artifact directory does not exist: {target.parent}")
    encoded = (
        json.dumps(
            evidence.sanitized_trace(),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    temporary_path = Path(temporary)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, target)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        if temporary_path.exists():
            temporary_path.unlink()
        raise
    artifact = TraceArtifact(
        path=str(target),
        sha256=hashlib.sha256(encoded).hexdigest(),
    )
    return replace(evidence, trace_artifact=artifact)


def probe_token_from_env(
    environment: Mapping[str, str],
    variable_name: str,
) -> str:
    if not isinstance(variable_name, str) or not variable_name:
        raise RunnerError("probe token environment variable name must not be empty")
    token = environment.get(variable_name)
    if not token:
        raise RunnerError(
            f"probe token environment variable is missing: {variable_name}"
        )
    return token


def run_codex(
    *,
    codex_binary: str,
    python_binary: str,
    server_script: Path,
    probe_url: str,
    probe_token: str,
    repository: Path,
    prompt: str = DEFAULT_PROMPT,
    tool_timeout_seconds: int = 1_800,
    trace_artifact: Path | None = None,
    run_process: ProcessRunner = subprocess.run,
) -> RunEvidence:
    """Start exactly one Codex subprocess and validate its JSONL trace."""
    command = build_command(
        codex_binary=codex_binary,
        python_binary=python_binary,
        server_script=server_script,
        probe_url=probe_url,
        repository=repository,
        prompt=prompt,
        tool_timeout_seconds=tool_timeout_seconds,
    )
    environment = os.environ.copy()
    environment[PROBE_TOKEN_ENV] = probe_token
    completed = run_process(
        command,
        cwd=str(repository.resolve()),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise RunnerError(
            f"Codex exited with status {completed.returncode}: {stderr[-2_000:]}"
        )
    events = parse_jsonl(completed.stdout or "")
    evidence = validate_trace(command, events)
    if trace_artifact is not None:
        evidence = write_trace_artifact(trace_artifact, evidence)
    return evidence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe-url", required=True)
    parser.add_argument("--probe-token-env", default=PROBE_TOKEN_ENV)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument(
        "--server-script",
        type=Path,
        default=Path(__file__).with_name("mcp_server.py"),
    )
    parser.add_argument("--tool-timeout-sec", type=int, default=1_800)
    parser.add_argument("--trace-artifact", type=Path)
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        probe_token = probe_token_from_env(os.environ, args.probe_token_env)
        evidence = run_codex(
            codex_binary=args.codex_bin,
            python_binary=args.python_bin,
            server_script=args.server_script,
            probe_url=args.probe_url,
            probe_token=probe_token,
            repository=args.repo,
            prompt=args.prompt,
            tool_timeout_seconds=args.tool_timeout_sec,
            trace_artifact=args.trace_artifact,
        )
    except RunnerError as exc:
        print(f"codex Phase 0 spike failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(evidence.summary(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
