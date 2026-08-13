---
name: x-sync
description: Assess and improve how well a person, an AI agent, and the current repository agree on task-relevant business and technical knowledge. Use for repository onboarding, knowledge checks, Socratic interviews, architecture or incident review, pre-agent task readiness, and spaced review grounded in specs, stories, commits, bug fixes, code, tests, infrastructure, and official technology sources. Supports terminal or local HTML quizzes. Do not use as an employee ranking tool or claim that one score measures understanding of an entire repository.
---

# X-Sync

Find specific gaps between the learner's mental model, the host agent's model, and repository evidence. Improve those gaps through adaptive, evidence-backed questions. Keep the runtime deterministic and let the current Codex or Claude Code agent handle repository research and semantic review.

## Resolve paths

Treat the directory containing this `SKILL.md` as `<skill-dir>`. Run the shared runtime with:

```bash
python3 <skill-dir>/scripts/xsync.py <command>
```

Never assume a Codex- or Claude-specific environment variable exists. Resolve relative references and scripts from `<skill-dir>`.

## Start every invocation

1. Run `status --repo <repo> --learner <learner> --json` when the learner is already known.
2. Otherwise run `doctor --repo <repo> --json`, then ask for a short learner ID. Explain that `.x-sync/` stores private local learning history; `init` adds `/.x-sync/` to this repository's local `.git/info/exclude` without changing the team's `.gitignore`.
3. If no active session exists, ask one compact setup question covering:
   - interview style: `regular` or `socratic`;
   - channel: `terminal` or `web`;
   - focus: `business`, `technical`, or `mixed`;
   - task or subsystem in scope;
   - desired question count and maximum depth.
4. Prefer task-scoped assessment. If no task is supplied, define a bounded subsystem or onboarding objective instead of claiming to assess the whole repository.

## Choose the interaction style

Use `regular` for quick diagnosis and review:

1. Ask one question.
2. Record the answer and confidence.
3. Grade a single-choice answer deterministically; send free text to host-agent review.
4. Explain the result with repository evidence.
5. Advance or schedule review.

Use `socratic` to expose and repair an incorrect mental model:

1. Ask for an initial judgment and its evidence.
2. Review without immediately revealing the answer.
3. If incomplete, use the next prepared probe: evidence, causality, counterexample, or boundary.
4. Record initial and final answers separately.
5. Reveal the explanation only after mastery or after attempts are exhausted.

Read [evaluation.md](references/evaluation.md) before generating or reviewing a Socratic bank, designing scores, or resolving a disputed answer.

## Build a grounded question bank

Inspect evidence in this order:

1. Story, Spec, acceptance criteria, glossary, and business process documents.
2. ADRs, design documents, configuration decisions, and operational runbooks.
3. Important commits and diffs for requirements, technical decisions, migrations, incidents, and bug fixes.
4. Regression tests, core code paths, interfaces, data models, and configuration.
5. Framework, database, cache, queue, network, OS, container, performance, transaction, consistency, security, and observability mechanisms connected to those code paths.
6. Official external documentation for general technology claims.

Use `rg`, `rg --files`, `git log`, `git show`, and focused tests. Do not infer a business requirement only from current implementation. Do not treat a commit subject as evidence without reading its diff and current code.

Classify every claim as `requirement`, `decision`, `implementation`, `general_knowledge`, `inference`, or `conflict`. A unique-answer scored question must not rely only on an inference. Turn conflicting evidence into a conflict-analysis question or leave it unscored.

Create evidence records with the runtime so paths, lines, commits, and hashes are reproducible:

```bash
python3 <skill-dir>/scripts/xsync.py evidence snapshot \
  --repo <repo> --kind code --path src/example.py --lines 20:48 \
  --summary "Request validation and transaction boundary" --json
```

For a requirement, decision, or bug-fix commit, bind the evidence to the exact commit:

```bash
python3 <skill-dir>/scripts/xsync.py evidence snapshot \
  --repo <repo> --kind commit --claim-type decision --commit <sha> \
  --path <optional/path> --summary "Why the retry boundary changed" --json
```

The runtime hashes the canonical commit object and raw changed-object IDs, independent of local Git display configuration. Still read the human-readable diff with `git show` before writing a claim; the hash proves the captured revision, not the meaning of the change.

Generate a JSON bank following [bank-schema.md](references/bank-schema.md). Ensure:

- questions progress from recognition to explanation, tracing, diagnosis, and design;
- business and technical questions remain separate dimensions;
- extension questions state their connection to this repository;
- every repository fact and every rubric criterion cites evidence;
- single-choice questions have exactly one evidence-supported answer and plausible, non-trick distractors;
- free-text questions have criterion-level rubrics rather than a model-written ideal paragraph alone;
- Socratic questions include probes, hints, and a maximum attempt count;
- the bank records the current commit and whether the working tree was dirty.

Validate and install the bank:

```bash
python3 <skill-dir>/scripts/xsync.py bank validate --repo <repo> --file <bank.json> --json
python3 <skill-dir>/scripts/xsync.py bank install --repo <repo> --learner <learner> --file <bank.json> --json
```

Fix validation failures rather than weakening evidence requirements.

## Run a session

Start the selected bank:

```bash
python3 <skill-dir>/scripts/xsync.py start \
  --repo <repo> --learner <learner> --bank <bank-id> \
  --style regular --channel terminal --focus mixed --max-depth 4 \
  --task "Refund retry change" --count 8 --json
```

For terminal work, show the current question and save one durable answer:

```bash
python3 <skill-dir>/scripts/xsync.py question \
  --repo <repo> --learner <learner> --json
python3 <skill-dir>/scripts/xsync.py answer \
  --repo <repo> --learner <learner> --choice B \
  --confidence 0.75 --reason "Short causal rationale" --json
```

Use `--text` instead of `--choice` for free text. Ask the learner for a `1..5` confidence rating, then pass `(rating - 1) / 4` to `--confidence`. In Socratic mode also ask why. Accept `unknown` as an honest single-choice response; never force a guess.

For HTML work, start the loopback server:

```bash
python3 <skill-dir>/scripts/xsync.py serve \
  --repo <repo> --learner <learner> --port 0 --open
```

Keep the yielded server process running. Return its tokenized loopback URL. The page saves answers but never receives answer keys. Tell the learner to return to Codex or Claude Code and say `继续` after saving.

## Handle “继续”

Treat `继续`, `continue`, `check`, or “检查答案” during an active x-sync session as a review command:

1. Run:

   ```bash
   python3 <skill-dir>/scripts/xsync.py pending --repo <repo> --learner <learner> --json
   ```

2. If no semantic review is pending, run `continue` and present the result and next question.
3. For every pending free-text or Socratic attempt:
   - reopen the cited repository evidence at the recorded commit/current snapshot;
   - grade each rubric criterion separately;
   - cite only evidence declared by that criterion, and derive free-text `correctness` as the weighted sum of its criterion scores;
   - distinguish a wrong answer from ambiguous or stale evidence;
   - use `disputed` when the learner provides credible counter-evidence;
   - never award points merely for keyword overlap;
   - choose `mastered`, `probe`, `exhausted`, or `disputed`.
4. Apply the structured review using `review apply`. Include the host as `codex` or `claude-code`, criterion scores, concise feedback, and evidence IDs.
5. Run `continue --json`. In Socratic mode a `probe` outcome keeps the same knowledge point open; otherwise it advances.
6. Report what was understood, what remains uncertain, and where the evidence lives. Do not expose a hidden answer before the Socratic sequence ends.

A regular single-choice submission is also revalidated before deterministic grading. If its evidence changed, preserve the answer as `stale` and unscored; do not consult the stored answer key.

## Report progress safely

Run `report --format md` or `report --json`. Report a profile, not a universal “sync value”:

- business understanding;
- architecture and data-flow understanding;
- technical mechanisms;
- decisions, incidents, and bug fixes;
- non-functional requirements;
- unaided versus hinted performance;
- delayed retention;
- confidence calibration;
- repository evidence quality and staleness.

A session may say `7/10` for those sampled questions. Never claim that it proves the learner understands the whole repository. Never use x-sync results for employee ranking. Bind every result to learner, repository, commit, task scope, bank version, and time.

## Preserve privacy and evidence integrity

- Keep learner profiles and answers under `<repo>/.x-sync/users/`; rely on the repository-local `.git/info/exclude` rule and do not commit them.
- Bind the web server only to loopback and keep its token out of query strings and disk.
- Do not read secrets, `.env` files, learner histories, vendored code, or binaries as question evidence.
- Mark a question stale when an evidence hash no longer matches; do not score it until revalidated.
- Preserve attempts and disagreements as append-only events; do not overwrite an answer to make the score look cleaner.
- Allow “I don't know.” Treat an honest low-confidence gap as safer than a high-confidence unsupported claim.
- Keep production permissions independent from x-sync readiness. A strong profile never grants deployment authority automatically.

## Installation and compatibility

This directory is the canonical skill for both hosts. Install it with:

```bash
python3 <skill-dir>/scripts/install.py --host all --scope user
```

Use `--scope project --project <repo>` for repository-local installation. Codex invokes it as `$x-sync`; Claude Code invokes a standalone installation as `/x-sync`. When installed as the bundled Claude plugin, invoke `/x-sync:x-sync`.
