# Bank and learning-record schema

Use UTF-8 JSON for banks, configuration, state, mastery, reports, and individual event files. Serialize timestamps as RFC 3339 strings with an offset. Generate stable opaque IDs; do not use display text as an ID.

Store shared repository records and private learner records separately:

```text
.x-sync/repositories/<repo-id>/
  scan.json
.x-sync/users/<learner>/projects/<repo-id>/
  banks/
    <bank-id>.json
  sessions/
    <session-id>/
      config.json
      state.json
      events/
        <sequence>-<event-id>.json
      reports/
        report.md
  active.json
  mastery.json
```

Allow a runtime to add indexes, locks, and temporary files, but treat these JSON records as the portable interchange format for Codex and Claude Code. In a Git repository, add `/.x-sync/` to the repository-local `.git/info/exclude`; do not modify the team's `.gitignore`. Do not place answer keys in a web payload before grading.

## Store the first-use repository scan

Before installing or selecting the first bank for a repository, create the shared `scan.json`. Require `schema_version`, `record_type: repository_scan`, `policy_version`, `scan_id`, `snapshot_id`, `repo_id`, `baseline_commit`, `state_token`, `working_tree`, `scope`, `complete`, `created_at`, `fingerprint`, `summary`, `files`, `commits`, and `integrity_hash`. A qualifying initial gate requires `scope.type: full` and `complete: true`; a path-scoped inventory is never a substitute.

Define the full scan universe as Git tracked files plus non-ignored untracked files in a normal, non-sparse worktree. Sort unique repository-relative paths. Before opening a candidate, exclude sensitive paths and generated/dependency directory segments. Open every remaining candidate without following any path-component symlink, read at most the configured bound, require a regular UTF-8 non-binary file, and record its path, SHA-256, byte/line counts, executable bit, discovery kind, LFS-pointer marker, and `tracked|untracked` source. Record exclusion counts without storing secret path names or contents. Do not recurse into Git submodules, and do not treat an LFS pointer as the unavailable object content.

Derive `scan_id` and `fingerprint` from the scan policy, repository identity, baseline commit, Git/index state token, full scope, and sorted file records; exclude `created_at` and local output paths. Repeating an unchanged scan must reuse the same materialized bytes. Revalidate the repository state and opened candidates before atomically publishing `scan.json`; an incomplete, concurrent, oversized, non-Git, or sparse scan must fail closed and must not satisfy the initial gate.

The repository scan is a discovery inventory, not a scored evidence item and not proof that a host Agent understood every file. Questions must still cite focused file or commit evidence and revalidate that evidence before presentation and grading. Expose `initial_scan_complete` separately from freshness states such as `fresh`, `captured_dirty`, and `stale`; unfinished-session recovery takes precedence over this first-use gate.

Store each bank as one object with `schema_version`, `bank_id`, `repo_id`, `baseline_commit`, `working_tree`, `created_at`, `evidence`, and `questions`. Set `working_tree.dirty` to a boolean. When it is true, require `working_tree.diff_hash` as `sha256:<64 hex digits>`. Require `evidence` and `questions` to be arrays. Treat evidence IDs as unique. Store at most one current version of a question ID in a bank; preserve older versions in immutable prior banks and session snapshots.

## Apply common field rules

Require these rules for every record:

- Use `schema_version: 1`.
- Use lowercase enum values exactly as shown.
- Use numbers, booleans, arrays, and objects as their native JSON types; do not encode them as strings.
- Use repository-relative POSIX paths and reject paths containing `..` or absolute paths.
- Keep prior records immutable. Append corrections and link them with `supersedes_id` or increment `version`.
- Preserve attempts against the question version and repository revision that the learner saw.
- Reject unknown fields when validating a stored record. Put implementation-specific metadata under `extensions`, whose keys must be namespaced, such as `org.example/cache_key`.
- Represent optional absent values by omitting the field. Use `null` only where this document explicitly permits it.

## Store evidence records

Store evidence records in `bank.evidence[]`. Require exactly these fields:

| Field | Type | Constraint |
|---|---|---|
| `schema_version` | integer | Must equal `1` |
| `record_type` | string | Must equal `evidence` |
| `id` | string | Stable and unique |
| `kind` | string | One of `spec`, `adr`, `commit`, `bug`, `test`, `code`, `config`, `official_documentation`, `inference`, `conflict` |
| `claim_type` | string | One of `requirement`, `decision`, `implementation`, `general_knowledge`, `inference`, `conflict` |
| `authority` | string | One of `repository_intent`, `repository_behavior`, `official_external`, `derived`, `conflicted` |
| `title` | string | Non-empty human-readable label |
| `claim` | string | One atomic claim supported by this evidence |
| `source` | object | Use the source fields below |
| `repository` | object | Use the repository snapshot fields below |
| `content_hash` | string | Lowercase `sha256:<64 hex digits>` |
| `status` | string | One of `active`, `stale`, `disputed`, `retired` |
| `created_at` | string | RFC 3339 |
| `verified_at` | string | RFC 3339 |
| `extensions` | object | Optional namespaced implementation data |

Use these fields inside `source`:

- Require `type`, one of `file`, `commit`, or `url`.
- For `file`, require `path`; optionally include `symbol`, `start_line`, and `end_line`. Require both line fields together, use one-based inclusive integers, and require `end_line >= start_line`.
- For `commit`, require `commit`; optionally include `path`, `old_path`, and `diff_hunk`.
- For `url`, require an HTTPS `url`; optionally include `section` and `version`. Use `url` only for official external documentation.

Use these fields inside `repository`:

- Require `root_id`, a stable repository identifier.
- Require `baseline_commit`, a full lowercase Git object ID: 40 hex characters for SHA-1 repositories or 64 for SHA-256 repositories.
- Require `branch` only as descriptive context; never use a branch name instead of `baseline_commit`.

For `kind: inference` or `claim_type: inference`, require `derived_from`, an array containing at least two evidence IDs, and set `authority` to `derived`. Do not use an inference by itself to establish a uniquely correct answer. For `kind: conflict` or `claim_type: conflict`, require `derived_from` with at least two conflicting evidence IDs and set `authority` to `conflicted`.

Example:

```json
{"schema_version":1,"record_type":"evidence","id":"ev.pipeline.async.implementation","kind":"code","claim_type":"implementation","authority":"repository_behavior","title":"Async service submits the complete pipeline","claim":"runProcessorsAsync submits the complete sequential processor run to an executor; it does not schedule each processor independently.","source":{"type":"file","path":"src/main/java/runtime/processor/defaultprocessor/DefaultProcessorService.java","symbol":"runProcessorsAsync","start_line":41,"end_line":48},"repository":{"root_id":"amwtke/x-sync-fixture","baseline_commit":"0123456789abcdef0123456789abcdef01234567","branch":"main"},"content_hash":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","status":"active","created_at":"2026-08-13T10:00:00+08:00","verified_at":"2026-08-13T10:00:00+08:00"}
```

## Store question versions

Store question records in `bank.questions[]`. Treat `(id, version)` as the unique key. Require exactly these fields:

Expose `topic: topics[0]` in the web API view model. Keep `topics` as the canonical stored field.

| Field | Type | Constraint |
|---|---|---|
| `schema_version` | integer | Must equal `1` |
| `record_type` | string | Must equal `question` |
| `id` | string | Stable across revisions of the same learning objective |
| `version` | integer | Positive and monotonically increasing per `id` |
| `status` | string | One of `draft`, `active`, `stale`, `disputed`, `retired` |
| `baseline_commit` | string | Full Git SHA used to validate all repository evidence |
| `domain` | string | One of `business`, `technical` |
| `topics` | array of strings | Non-empty; first item is the primary topic |
| `depth` | integer | `1..5`, corresponding to locate, explain, trace, diagnose, design |
| `type` | string | One of `single_choice`, `free_text` |
| `styles` | array | Non-empty subset of `regular`, `socratic` |
| `prompt` | string | Non-empty and answer-key-free |
| `choices` | array | Required only for `single_choice` |
| `answer` | object | Correct choice or rubric plus explanation; use the rules below |
| `socratic` | object | Require `max_attempts`, `probes`, and `hints` |
| `evidence_ids` | array of strings | Non-empty union of all evidence used by the question |
| `prerequisite_question_ids` | array of strings | May be empty; must not introduce a dependency cycle |
| `generation` | object | Generator provenance |
| `validation` | object | Grounding and ambiguity result |
| `created_at` | string | RFC 3339 |
| `extensions` | object | Optional namespaced implementation data |

For every `choices` item, require `id`, `text`, `misconception`, and `evidence_ids`. Make `misconception` `null` only for the correct choice. Use at least two and at most six choices. Require exactly one correct choice.

For every rubric item under `answer.rubric`, require:

```json
{"id":"criterion.unique.within.question","description":"Atomic observable criterion","weight":1.0,"evidence_ids":["ev.id"],"required":true}
```

Require `weight > 0`, require at least one evidence ID, and require rubric weights to sum to `1.0` within a tolerance of `0.000001`.

For `single_choice`, require `answer.correct_choice` and `answer.explanation`; allow `answer.rubric` when a rationale is also graded. Omit `answer.reference_answer`. For `free_text`, require `answer.reference_answer`, `answer.explanation`, and non-empty `answer.rubric`. Omit `answer.correct_choice`. Treat the reference answer as guidance for applying the rubric, not as text that must be matched verbatim.

Require `socratic.max_attempts` to be an integer from `1` through `4`. Require `socratic.probes` to be a non-empty array of prompts that ask for evidence, causality, boundaries, or synthesis. Require `socratic.hints` to contain ordered objects with unique `level` values from `1` through `4` and non-empty `text`. Do not include H0 in stored hints.

Require `generation` to contain `generator`, `generator_version`, and `prompt_version`. Require `validation` to contain:

- `grounded`: boolean;
- `ambiguity`: one of `low`, `medium`, `high`;
- `validated_at`: RFC 3339 timestamp;
- `validator`: tool or model identifier;
- `evidence_valid`: boolean.

Publish a question as `active` only when `grounded` and `evidence_valid` are true and `ambiguity` is `low`. Mark it `stale` when any evidence hash changes. Mark it `disputed` when authoritative evidence conflicts or a valid alternative answer exists.

Single-choice example:

```json
{"schema_version":1,"record_type":"question","id":"tech.pipeline.async.001","version":1,"status":"active","baseline_commit":"0123456789abcdef0123456789abcdef01234567","domain":"technical","topics":["processor-pipeline","concurrency"],"depth":3,"type":"single_choice","styles":["regular","socratic"],"prompt":"Does runProcessorsAsync execute the individual processors in parallel? Give the reason for your choice.","choices":[{"id":"A","text":"Yes; it submits every processor as an independent task.","misconception":"Confuses asynchronous submission of the whole service with processor-level parallelism.","evidence_ids":["ev.pipeline.async.implementation"]},{"id":"B","text":"No; it submits one task that still runs the ordered processor loop.","misconception":null,"evidence_ids":["ev.pipeline.async.implementation"]}],"answer":{"correct_choice":"B","explanation":"The executor receives the service task; processor iteration remains ordered inside that task.","rubric":[{"id":"distinguish.async.from.parallel","description":"Distinguish whole-pipeline asynchronous execution from processor-level parallelism.","weight":1.0,"evidence_ids":["ev.pipeline.async.implementation"],"required":true}]},"socratic":{"max_attempts":4,"probes":["Which repository artifact supports your answer?","What is asynchronous here, and what remains sequential?"],"hints":[{"level":1,"text":"Inspect the service method that submits work to the executor."},{"level":2,"text":"Distinguish task submission from processor iteration."}]},"evidence_ids":["ev.pipeline.async.implementation"],"prerequisite_question_ids":[],"generation":{"generator":"codex","generator_version":"gpt-5","prompt_version":"x-sync-question-v1"},"validation":{"grounded":true,"ambiguity":"low","validated_at":"2026-08-13T10:00:00+08:00","validator":"x-sync-validator-v1","evidence_valid":true},"created_at":"2026-08-13T10:00:00+08:00"}
```

## Store session configuration, state, and events

Write immutable event objects to `sessions/<session-id>/events/<sequence>-<event-id>.json`. Require every event to include:

```json
{"schema_version":1,"record_type":"session_event","event_id":"evt.unique","session_id":"session.unique","learner":"user.local","sequence":1,"event_type":"session_started","occurred_at":"2026-08-13T10:00:00+08:00","payload":{}}
```

Require `sequence` to start at `1` and increase by one. Also persist `from_version`, `to_version`, and the resulting materialized `state_after`; validate the contiguous version chain before using it to recover `state.json`. Permit these `event_type` values:

- `session_started`: require `payload.style`, `payload.channel`, `payload.baseline_commit`, and `payload.focus_topics`; use `style` values `regular` or `socratic` and `channel` values `terminal` or `web`;
- `question_presented`: require question `id`, `version`, and the evidence-validation timestamp;
- `teaching_requested`: accept only when replaying an older inline-teaching session; new sessions do not emit it;
- `teaching_started`: require one complete `lesson` record with H4, revision `1`, no feedback, and no completion time;
- `teaching_feedback_submitted`: require `lesson_id` and one non-empty feedback object frozen to the current `base_revision`;
- `teaching_revised`: require `lesson_id`, `feedback_id`, and the complete next article revision;
- `teaching_completed`: require `lesson_id` and `completed_at`; it reopens the same unanswered question;
- `teaching_invalidated`: require `lesson_id`, `invalidated_at`, and non-empty `stale_evidence_ids`; it closes a lesson whose evidence changed and reopens the same unanswered question;
- `answer_submitted`: require `attempt_id`, `answer`, `confidence`, `max_hint_level`, and `submitted_at`; set `answer` to a choice ID or free-text string, never an `unknown` sentinel; permit a separate `reason` string; require stored `confidence` in `[0, 1]`;
- `socratic_turn`: require `attempt_id`, `turn`, `speaker`, `kind`, and `text`; use `speaker` values `learner` or `tutor`;
- `grading_started`: require `attempt_id` and an idempotency key;
- `answer_reviewed`: require `attempt_id`, the evaluation object, and `next_review_at` or explicit `null`;
- `session_ended`: require a reason and an assessed-scope summary.

Store session inputs in `config.json`: require `session_id`, `learner`, `repo_id`, `bank_id`, `baseline_commit`, `style`, `channel`, `focus_topics`, `focus`, `max_depth`, and `task_scope`. Use `focus` values `business`, `technical`, or `mixed`; use `max_depth` values `1..5` or `null`; require non-empty `task_scope`, defaulting to `repository onboarding`. A bare host-skill invocation resolves to `style=socratic`, `channel=web`, `focus=mixed`, and `count=5`; persist those resolved values rather than treating the preset as implicit.

Store the current materialized view in `state.json`: require `session_id`, `status`, `current_index`, `current_question_id`, `total`, `attempts`, `lessons`, `consumed_continue_keys`, and `state_version`. New sessions also retain an empty `teaching_requests` array only for old-event compatibility. Use `status` values `question_open`, `teaching_open`, `teaching_feedback_saved`, `answer_saved`, `agent_review_pending`, `reviewed`, or `completed`. A missing `lessons` or `teaching_requests` field in an older session means an empty list. Store every successful continue idempotency key once in `consumed_continue_keys`; do not discard older keys when a later question advances. Increment `state_version` after every accepted transition.

Each lesson is session-local and has this shape:

```json
{"lesson_id":"lesson.unique","question_id":"tech.example","question_version":1,"hint_level":4,"started_at":"2026-08-14T00:00:00+00:00","completed_at":null,"revisions":[{"revision":1,"created_at":"2026-08-14T00:00:00+00:00","author":{"name":"x-sync-runtime","version":"0.1.0"},"document":{"title":"...","subtitle":"...","sections":[{"layer":"operation","eyebrow":"第一段","title":"...","paragraphs":["...","..."],"points":["..."],"diagram":"..."},{"layer":"logic","eyebrow":"第二段","title":"...","paragraphs":["...","..."],"points":["..."],"diagram":"..."},{"layer":"principle","eyebrow":"第三段","title":"...","paragraphs":["...","..."],"points":["..."],"diagram":"..."}],"conclusion":"...","reflection_prompt":"...","evidence_ids":["ev.example"]}}],"feedback":[{"feedback_id":"feedback.unique","base_revision":1,"text":"...","submitted_at":"2026-08-14T00:01:00+00:00","applied_revision":null}]}
```

Require exactly one active lesson, exactly three ordered document layers (`operation`, `logic`, `principle`), consecutive revisions, at most one unapplied feedback item, and `hint_level=4`. Every document revision must cite at least one evidence ID and only IDs bound to that question version. A revision applies one pending feedback by setting its `applied_revision` to the new consecutive revision. Completion is forbidden while feedback remains unapplied. Completed lessons remain append-only evidence that a later answer was aided.

Accept web confidence ratings `1..5`, normalize them before persistence with `confidence = (rating - 1) / 4`, and return only the normalized value in runtime state. Preserve the raw UI rating under a namespaced extension only when needed for diagnostics.

Build the web state response as a view model rather than exposing storage files directly. Merge `config.style` into `session.style`, expose `session.total`, and expose `pending_attempt` whenever the state is `answer_saved` or `agent_review_pending`. Return the current question without `answer`; expose its primary topic as `question.topic`. Return `view: quiz` and `lesson: null` before H4 teaching. During `teaching_open` or `teaching_feedback_saved`, return `view: lesson` plus only the active lesson's latest document, revision number, H4 level, a safe pending-feedback snapshot (or null), count, and completion availability. Never embed an answer key in the static HTML or reveal a lesson before `teaching_started`. Keep the persisted canonical fields unchanged.

Rebuild `state.json` from ordered events when it is missing or inconsistent. Validate each event's permitted state delta before accepting `state_after`: an answer event may not rewrite an earlier response, reason, confidence, evidence check, or attempt; a continue event may only append its own idempotency key and make its documented transition. Apply this transition model:

```text
session_started or next question_presented -> question_open
teaching_started -> teaching_open
teaching_feedback_submitted -> teaching_feedback_saved
teaching_revised -> teaching_open
teaching_completed -> question_open (same current_index/current_question_id)
teaching_invalidated -> question_open (same position, no attempt)
answer_submitted -> answer_saved
regular single_choice answer_reviewed -> reviewed
free_text or Socratic grading_started -> agent_review_pending
host-Agent answer_reviewed -> reviewed
next question_presented -> question_open
session_ended -> completed
```

Append `answer_reviewed` for regular single-choice auto-grading as well as host-Agent grading. Advance only from durable `reviewed` state. A repeated `continue` after `question_presented`, `socratic_turn`, or `session_ended` must return the existing state without another transition. Use `(session_id, attempt_id, question_id, question_version)` as the semantic-grading idempotency key. Append at most one successful `answer_reviewed` event for that key.

While status is `teaching_open` or `teaching_feedback_saved`, reject `answer_submitted`. `pending` returns a separate `teaching_pending` snapshot instead of a grading attempt. Publishing a revision must revalidate the current question evidence before writing any event. The exact same `(lesson_id, feedback_id, base_revision, document, author)` retry returns the already-published revision; a different body for applied feedback is rejected. `teaching_completed` changes no question position and adds no answer attempt.

If evidence becomes stale before revision or completion, append `teaching_invalidated`, set the active lesson's completion time, retain its revision and feedback history, and return to `question_open` without an attempt. Return the stale evidence IDs to the host; do not silently publish the article as current.

Require the evaluation object to contain:

| Field | Type | Constraint |
|---|---|---|
| `status` | string | One of `graded`, `unscored`, `disputed`, `stale` |
| `correctness` | number or null | `0..1`; use null unless `graded` |
| `initial_correctness` | number or null | `0..1`; use in Socratic mode |
| `final_correctness` | number or null | `0..1`; use in Socratic mode |
| `reasoning` | number or null | `0..1` |
| `evidence_use` | number or null | `0..1` |
| `max_hint_level` | integer | `0..4` |
| `unaided` | boolean | True only for successful H0 retrieval |
| `rubric_results` | array | One item per rubric criterion |
| `confidence` | number | Copy the submitted value |
| `brier_error` | number or null | Compute only for objectively graded items |
| `overconfidence` | number or null | Compute only for objectively graded items |
| `evidence_ids` | array | Evidence actually used to grade |
| `explanation` | string | Concise criterion-based feedback |
| `grader` | object | Require `name`, `version`, and `prompt_version` |
| `outcome` | string or null | One of `mastered`, `probe`, `exhausted`, `disputed`; use for Socratic review |

Require every `rubric_results` item to contain `criterion_id`, `score` (`0`, `0.5`, or `1`), `evidence_ids`, and `justification`.
Require each result to cite at least one evidence ID from that criterion's own `answer.rubric[].evidence_ids`. For a graded free-text answer, require all criteria exactly once and derive `correctness` from the weighted criterion scores; reject a conflicting caller-supplied total.

### Apply a host-Agent review

After `pending` moves a durable free-text or Socratic answer into `agent_review_pending`, write a temporary review JSON file and pass it to `review apply`. Use this input shape:

```json
{"session_id":"session.unique","question_id":"tech.pipeline.async.001","attempt_id":"attempt.unique","outcome":"mastered","rubric_results":[{"criterion_id":"distinguish.async.from.parallel","score":1,"evidence_ids":["ev.pipeline.async.implementation"],"justification":"The answer distinguishes executor submission from the ordered processor loop."}],"feedback":"Correct causal model and repository evidence.","reviewer":{"name":"codex","version":"gpt-5"},"evaluation":{"correctness":1,"initial_correctness":1,"final_correctness":1,"reasoning":1,"evidence_use":1,"max_hint_level":0,"unaided":true,"evidence_ids":["ev.pipeline.async.implementation"],"explanation":"The processor loop remains sequential.","grader":{"name":"codex","version":"gpt-5","prompt_version":"x-sync-review-v1"}}}
```

Use `outcome: probe` only for another prepared Socratic turn, `exhausted` when the attempt limit is reached, and `disputed` when evidence or the key is genuinely contestable. Submit every rubric criterion for a scored free-text answer. The runtime rejects duplicate criteria, missing criteria, out-of-range scores, and a review for the wrong session, question, or attempt.

## Store review and mastery state

Append every grading decision as an `answer_reviewed` session event. Materialize the latest review state for scheduling in project-level `mastery.json`; rebuild it from events when necessary. Use `(learner, question_id, question_version)` as the logical review key. Preserve superseded review records in `mastery.history[]` and current records in `mastery.reviews[]`. Require these review fields:

| Field | Type | Constraint |
|---|---|---|
| `schema_version` | integer | Must equal `1` |
| `record_type` | string | Must equal `review` |
| `id` | string | Unique record version ID |
| `supersedes_id` | string | Omit for the first record |
| `learner` | string | Match the owning directory |
| `question_id` | string | Existing question ID |
| `question_version` | integer | Existing question version |
| `topic_ids` | array of strings | Non-empty |
| `stage` | string | One of `new`, `learning`, `reviewing`, `stable`, `stale`, `disputed` |
| `last_attempt_id` | string | Attempt that produced this review state |
| `last_result` | string | One of `unaided_correct`, `aided_correct`, `incorrect`, `unscored`, `disputed`, `stale` |
| `successful_delayed_retrievals` | integer | Non-negative |
| `interval_days` | number | Non-negative |
| `due_at` | string or null | RFC 3339; null only for `disputed` or retired work |
| `priority_reasons` | array | Subset of `due`, `high_confidence_error`, `low_confidence_correct`, `important_uncovered`, `recent_change`, `task_relevant` |
| `updated_at` | string | RFC 3339 |
| `extensions` | object | Optional namespaced implementation data |

Example:

```json
{"schema_version":1,"record_type":"review","id":"review.pipeline.async.002","supersedes_id":"review.pipeline.async.001","learner":"user.local","question_id":"tech.pipeline.async.001","question_version":1,"topic_ids":["processor-pipeline","concurrency"],"stage":"learning","last_attempt_id":"attempt.20260813.001","last_result":"aided_correct","successful_delayed_retrievals":0,"interval_days":1,"due_at":"2026-08-14T10:10:00+08:00","priority_reasons":["low_confidence_correct","task_relevant"],"updated_at":"2026-08-13T10:10:00+08:00"}
```

When a question becomes stale, append a review record with `stage: stale` and `last_result: stale`; do not erase retained historical performance. When an item becomes disputed, append `stage: disputed`, set `due_at` to null, and exclude it from correctness and mastery aggregates until revalidated.

## Validate before use

Before presenting a question:

1. Resolve its exact `(id, version)`.
2. Require `status: active`.
3. Resolve every `evidence_id`.
4. Recompute local file or commit evidence hashes against `baseline_commit` or the checked-out revision.
5. Mark the question stale instead of presenting it when validation fails.

Before grading a response:

1. Load the same question version that was presented.
2. Recompute every evidence hash cited by that question.
3. If any cited evidence changed, preserve the submitted answer, remove or omit `auto_result`, and append `answer_reviewed` with `status: stale`, `correctness: null`, and `next_review_at: null`; do not consult the answer key or invoke Agent grading.
4. Verify the durable submission and grading idempotency key.
5. Apply every rubric criterion independently.
6. Cite the evidence actually used.
7. Return `unscored` when evidence is insufficient and `disputed` when more than one answer remains defensible.
8. Append the session result and review state; never rewrite the submitted answer.
