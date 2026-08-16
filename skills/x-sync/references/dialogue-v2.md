# Dialogue v2 host runbook

Use this runbook only for the default adaptive dialogue. All paths below belong to the selected repository, not the skill repository.

## State and bootstrap

Use these stable repo-local locations:

```text
<repo>/.x-sync/dialogue-v2/                 runtime state root
<repo>/.x-sync/dialogue-v2/host.sock        private Host IPC socket
<repo>/.x-sync/dialogue-v2/bootstrap.json   temporary owner-only manifest
```

Use a stable registry ID derived from the canonical repository ID, with only letters, digits, `.`, `_`, `:`, or `-`. Session and evidence IDs follow the same restricted form.

An initial manifest is canonical JSON, mode `0600`, and has exactly this shape. It intentionally contains no question count:

```json
{
  "schema_version": 2,
  "record_type": "dialogue_bootstrap_manifest",
  "protocol_version": "x-sync-dialogue/2",
  "session_id": "dialogue-<stable-id>",
  "learner_id": "<learner>",
  "created_at": "<timezone-aware ISO-8601 timestamp>",
  "task_scope": "<bounded repository learning objective>",
  "language": "zh-CN",
  "channel": "web",
  "style": "socratic",
  "focus": "mixed",
  "evidence_sources": [
    {
      "evidence_id": "evidence-1",
      "kind": "spec",
      "claim_type": "requirement",
      "claim": "<reviewed claim grounded by this source>",
      "relative_path": "docs/example.md",
      "start_line": 10,
      "end_line": 24,
      "imported_from": null
    }
  ]
}
```

`kind` and `claim_type` must use the runtime's closed enums. Use repository-relative regular files and exact positive line ranges. Include only sources the Host has actually read. Remove the temporary manifest after the runtime has durably accepted it; the runtime stores the verified evidence snapshot and immutable session config.

## Launch runtime

```bash
python3 <skill-dir>/scripts/xsync.py dialogue runtime serve \
  --state <repo>/.x-sync/dialogue-v2 \
  --repo <repo> \
  --repository-id <repo-id> \
  --registry <registry-id> \
  --host-socket <repo>/.x-sync/dialogue-v2/host.sock \
  --browser-port 0 \
  --bootstrap-manifest <manifest-if-first-run> \
  --stream-json
```

On recovery, omit `--bootstrap-manifest`. Read the single `ready` NDJSON object and retain its `runtime_epoch`, `session_id`, `host_socket`, and `browser_url`.

## Run the model-neutral Host supervisor

```bash
python3 <skill-dir>/scripts/xsync.py dialogue host supervise \
  --socket <host-socket> \
  --session <session-id> \
  --owner <codex-or-claude-instance-id> \
  --stream-json
```

The supervisor yields durable work and a submission handle. Keep the same Host turn pending while browser answers arrive when supported. Otherwise, reconnect in a new turn and reclaim the durable work.

For every work item:

1. Read its public task scope, trigger, topic contract, evidence capsule, current learner model, and gate state.
2. Re-open focused repository sources when freshness or interpretation matters.
3. Produce exactly one result object accepted by the closed Host result codec: topic candidates, topic clarification, topic start, dialogue turn, topic summary, or sanitized work failure.
4. Write it to an owner-only regular JSON file.
5. Submit it with the yielded handle and a stable idempotency key:

   ```bash
   python3 <skill-dir>/scripts/xsync.py dialogue host submit \
     --socket <host-socket> \
     --supervisor <submission-handle> \
     --idempotency-key <stable-key> \
     --file <result.json> \
     --occurred-at <timezone-aware-ISO-8601> \
     --actor-id <host-instance-id> \
     --json
   ```

Use the result DTO fields and enum values emitted in the work capsule/runtime package; do not invent extra JSON fields. One dialogue-turn result contains the heard reflection, one-step-further explanation, learner-model updates, gate assessments, and exactly one next question. Topic completion uses a summary result and contains no next question.

## Completion behavior

There is no numeric finish condition. A topic completes only when its required gates and business-technical bridge are supported by current evidence. The learner may pause or switch earlier. After completion, offer another topic, a spaced review, or export; do not silently start a five-question round.
