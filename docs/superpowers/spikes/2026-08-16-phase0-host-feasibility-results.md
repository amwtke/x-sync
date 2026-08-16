# Phase 0 Host Feasibility — Interim Results

**Date:** 2026-08-16  
**Scope:** Disposable transport probe only. This is not the v2 runtime and does
not authorize implementation of the production kernel yet.

## Automated harness

- A loopback-only probe daemon persists questions and browser answers.
- A stdio MCP server exposes one blocking publish_and_wait(round, question)
  tool and emits progress while the call remains pending.
- Codex and Claude adapters use the same probe HTTP contract.
- The probe is isolated under spikes/phase0_host_feasibility/; it does not
  import or modify v1 or the planned xsync_v2 package.

Automated checks:

    Host journal, lease, HTTP, control and recovery: 14/14 passed
    MCP protocol and Codex runner contract:           8/8 passed

These checks cover durable input, idempotency, a single concurrent claim,
fake-clock renewal, pause/switch superseding pending work, late-submit
rejection, MCP cancellation, progress, and single-invocation trace validation.
The MCP contract permits only one active browser wait, preserves the durable
question on transport cancellation, and allows only an exact same-round,
same-question retry to recover it. The runner now validates paired tool start
and completion ids, non-overlapping rounds, matching structured answers, and a
single final message after round 3. It proves one Codex CLI invocation, not a
measured OS process tree.

## Codex observation

Environment:

    Host: codex
    Version: codex-cli 0.147.0
    Channel: configured stdio MCP tool

Fresh real run with retained sanitized ordering evidence:

    thread_id:        01a00881-8c23-7412-bbe8-d8b06d2ba895
    thread.started:   1
    turn.started:     1
    turn.completed:   1
    completed rounds: 1, 2, 3
    event count:      10
    trace SHA-256:    213cc028d3aa3611b53cd4fb3dbb79be756a93fb358fc0eec3d9ab78b48888d0

The strengthened runner verified that all three publish_and_wait calls completed
inside one unfinished turn. It matched each start/completion pair, checked that
the calls did not overlap, verified each structured answer, and found one final
Agent message only after round 3. During the live run, questions 2 and 3 visibly
incorporated the preceding browser answer; the retained hash-only artifact
confirms that all three question payloads were distinct, but does not by itself
prove their semantic adaptation. The sanitized ordering evidence is stored at
`spikes/phase0_host_feasibility/results/codex-0.147.0-20260816.trace.json`.

Result for the narrow same_turn_three_rounds gate: **PASS**. This proves one
Codex CLI invocation and one turn event sequence; it does not claim that the
whole OS process tree was measured.

An earlier run kept round 2 pending for about 38.237 seconds, crossing three
configured 10-second MCP progress intervals. Its raw JSONL was not retained,
so that duration remains a legacy observation rather than replayable evidence.

A second real run sent switch while the MCP call was pending. The question was
durably changed to cancelled, the tool returned MCP error -32001, and no late
answer appeared.

A third real run terminated the MCP process while round 1 was pending. Codex
received Transport closed, did not start a replacement MCP process, and ended
the turn without reconnecting. The durable probe question remained available,
but it could only be continued through a later turn.

Strict Host verdict: **NO-GO** under the currently approved same-turn reconnect
requirement. Normal three-round dialogue and switch work; transparent recovery
from a lost MCP tool channel does not.

The real run still did not establish maximum pending tenure or observable
model-call telemetry during idle. The in-app browser was not available in this
environment, so answers were sent through the same authenticated HTTP endpoint
used by the page rather than by clicking the UI.

## Claude Code observation

Environment:

    Host: claude-code
    Version: 2.1.220
    Authentication: not logged in

The installed Claude Code binary supports a temporary MCP configuration and
streaming output, and the common MCP adapter is ready. A real three-round
model-driven run was not started because authenticating Claude Code would
change external account state.

Result: **PENDING — no Host verdict**.

## Product acceptance decision

The strict reconnect-oriented Phase 0 verdicts above remain unchanged as an
audit record. On 2026-08-16 the product owner changed the release criterion
from per-machine authenticated execution to Host capability compatibility and
accepted bounded degradation after an stdio tool-channel loss.

- Codex: `CAPABILITY_VERIFIED + RUNTIME_PARTIALLY_VERIFIED`.
- Claude Code: `CAPABILITY_VERIFIED + LOCAL_RUNTIME_WAIVED`; this machine cannot
  authenticate Claude Code, so a local model-driven run is not required.
- Both adapters must share one Runtime Core and protocol. A lost channel keeps
  the learner input durable, shows a reconnecting state, and resumes through a
  new Agent turn instead of claiming transparent same-turn recovery.

Under this revised criterion the dual-Host capability gate is **PASS**, while
the narrower empirical observations above continue to describe exactly what
was and was not run.

## Original Phase 0 decision

Do not start the production state-machine kernel yet. Decide whether to:

1. retain transparent same-turn reconnect as a hard requirement and redesign
   the Host channel;
2. accept a bounded degradation: persist the browser answer, show “等待搭档重新连接”,
   and resume it in a new Agent turn after tool-channel failure.

After that decision, complete a longer pending-duration observation, run the
same matrix in an authenticated Claude Code session, and perform one visual
browser pass.

Only then complete the per-Host evidence matrix under the original strict
criterion. The current Codex NO-GO record is schema-valid and includes
independently auditable normal-path ordering evidence; the browser,
adaptive-semantics, switch, interruption and long-tenure observations still
need scenario-specific retained artifacts if that stricter criterion is ever
restored.
