# Open chat runtime

`xsync.py chat serve` is the default X-Sync experience. It opens a user-led
conversation with optional suggested questions and automatic host replies.

```bash
python3 <skill-dir>/scripts/xsync.py chat serve \
  --repo /path/to/project --host codex --stream-json
```

For Claude Code, use `--host claude`. `-d` aliases `--repo`. `--learner` defaults
to the local account. `--port 0` selects a free loopback port; `--timeout 600`
bounds each host answer. No model or API key is selected by X-Sync; the installed
host CLI uses its existing authentication and configuration.

## Conversation loop

1. The authenticated browser posts free text and a stable request ID.
2. The runtime durably saves the user message and a pending assistant reply.
3. A background worker refreshes a safe repository map and README excerpts,
   includes recent complete conversation turns, and launches the selected host.
4. Only assistant text is forwarded to the browser through SSE. Tool output,
   reasoning, and raw diagnostics are not published. The browser can refresh
   or reconnect without interrupting the worker.
5. The completed answer is saved. The user can immediately ask another question.

Each answer uses a separate host invocation with bounded local history rather
than attempting to wake the already-finished foreground Agent turn. The worker
prompt asks the host to read relevant current source files and cite them. It
does not launch another X-Sync session or impose Socratic question/gate rules.

Codex uses `exec --json --ephemeral --sandbox read-only` and stdin input.
Claude Code uses print mode with `stream-json`, partial messages, and only
Read/Glob/Grep tools. Codex may deliver whole assistant message events; Claude
can deliver text deltas. Both are published as soon as the host emits them.
These adapters follow the official [Codex non-interactive interface](https://learn.chatgpt.com/docs/non-interactive-mode)
and [Claude Code programmatic interface](https://code.claude.com/docs/en/headless).

## Recovery and export

State is scoped to the canonical target and learner under:

```text
.x-sync/chat/learner-<id>/
  owner.lock
  session.json
  exports/x-sync-<timestamp>-<id>.md
```

Only one runtime owns that conversation. Startup resumes its history; pending
answers from a previous process become retryable. The page's retry reuses the
saved user message, and repeated delivery of a request ID does not create a
second question or model call. Stop cancels the current host process group.

The export button saves and downloads an independent UTF-8 Markdown document
with both sides of the conversation. Incomplete answers are labelled. Export
is available during an answer and does not ask the model to rewrite history.

## Troubleshooting

- A missing host binary stops startup with an actionable error. Install and
  authenticate the requested host yourself before retrying.
- Authentication, network, or quota failures produce a retryable page message;
  raw stderr and authentication details are not shown.
- After a runtime restart, reopen the new URL with its new fragment capability.
  The same learner's saved chat is restored.
- A second runtime cannot silently take over the first. Reuse the current page
  or stop the previous process before starting another.
- SIGINT/SIGTERM stops the runtime and its current worker, preserving history.
