# Evaluation protocol

Use this protocol to generate, conduct, grade, and review repository-grounded questions. Treat the result as a diagnostic learning record, not as proof that a person understands the whole repository.

## Select the dialogue mode

Default a bare skill invocation to `socratic`; use `regular` when the learner explicitly requests it. Store the resolved selection as `style` in session configuration and preserve it in every attempt record. Do not ask for a mode when the default already resolves it.

For `regular` mode:

1. Present one question.
2. Collect the answer and confidence before showing any feedback.
3. Grade immediately against the stored evidence and rubric.
4. Show the result, explanation, cited repository evidence, and next action.
5. Select the next question from the learner's review queue or current difficulty frontier.

For `socratic` mode:

1. Present one problem without revealing the answer key.
2. Record the learner's initial answer and confidence.
3. Ask for repository evidence, a causal explanation, or the relevant invariant.
4. Challenge the answer with one counterexample, boundary condition, or failure scenario.
5. Ask the learner to synthesize a final answer.
6. Grade the initial answer and final answer separately, then reveal the evidence-backed explanation.

Do not turn Socratic dialogue into an endless interrogation. Stop after the learner demonstrates the model, asks to reveal the answer, says they do not know, or reaches four follow-up turns. Apply at most one prompt from each hint level:

- `H0`: give no help;
- `H1`: identify the relevant document, module, or subsystem;
- `H2`: name the relevant concept or invariant;
- `H3`: reveal part of the causal chain;
- `H4`: teach the answer and schedule it for review.

Record the maximum hint level. Never label a hinted answer as unaided mastery.

Treat an explicit “I don't know; teach me” action as an H4 interruption outside the answer lifecycle. It creates no failed attempt and records no confidence. Pause the quiz, show a dedicated repository-grounded teaching article, collect the learner's reflection, let the host revise the article, and reopen the same question only after the learner explicitly acknowledges understanding. Every later formal answer to that question carries `max_hint_level=4` and `unaided=false`.

## Ground every question in evidence

Build the question bank from these sources in descending authority:

1. Story, spec, acceptance criteria, business glossary, and explicit requirement records;
2. ADRs, design documents, and explicit technical decisions;
3. Regression tests, bug reports, postmortems, and fix commits;
4. Current interfaces, implementation, schemas, configuration, and deployment files;
5. Commit messages and diffs that preserve decision history;
6. Official framework, database, middleware, network, operating-system, and container documentation;
7. Model inference derived from the preceding sources.

Classify the source of each evidence item as `spec`, `adr`, `commit`, `bug`, `test`, `code`, `config`, `official_documentation`, `inference`, or `conflict`. Separately classify its claim as `requirement`, `decision`, `implementation`, `general_knowledge`, `inference`, or `conflict`.

Use specs and acceptance evidence to establish intended business behavior. Do not infer business intent solely from current code. Use implementation evidence to establish what the current revision does. When intent and implementation differ, create a conflict-analysis question or mark the item `disputed`; do not manufacture one correct answer.

Connect general technology questions to a concrete repository decision or runtime path. Ask how this project's Redis invalidation order affects consistency, for example, instead of asking isolated Redis trivia. Cite official documentation for claims that are not established by the repository.

Require every scorable answer criterion and every correct multiple-choice option to cite at least one evidence ID. Require each distractor to represent a plausible misconception and explain why the cited evidence rejects it. Reject trick questions and unsupported answer keys.

## Cover business and technical axes

Tag every question with exactly one `domain` and any number of topics.

Use `business` for terminology, actors, project goals, exclusions, workflows, state transitions, acceptance rules, requirement history, and business invariants.

Use `technical` for repository structure, module ownership, interfaces, data flow, runtime control flow, architecture patterns, technical decisions, bug causality, regression mechanisms, framework behavior, storage, caching, messaging, networking, OS behavior, containers, transactions, performance, consistency, availability, security, and observability.

Move outward from core project code to frameworks and middleware only after establishing the connection between them. Do not assume a dependency is operationally important merely because it appears in a manifest.

## Progress from recall to systems reasoning

Assign one positive integer `depth`:

- `1 locate`: locate or recognize a term, artifact, owner, or module;
- `2 explain`: explain a component, rule, or local flow in plain language;
- `3 trace`: trace a request, state change, or failure across components and apply a rule;
- `4 diagnose`: diagnose a bug, compare trade-offs, or explain a non-functional consequence;
- `5 design`: design a change, handle a counterfactual, preserve invariants, and propose verification.

Start at depth `1` or `2` for an unseen topic. Move upward only after two unaided successes, including at least one `free_text` response. Treat one successful delayed review as stronger evidence than repeated answers in the same session. After a failure, identify whether it came from a knowledge gap, an ambiguous question, stale evidence, or a repository conflict before lowering difficulty.

Use `single_choice` questions for sharply testable distinctions. Use `free_text` questions for mechanisms, causal chains, trade-offs, and designs. Ask for a short rationale with single-choice answers when guessing would otherwise look like understanding.

## Grade atomic criteria

Grade multiple-choice correctness as `0` or `1`. Grade a free response criterion-by-criterion with `0`, `0.5`, or `1`, using only the evidence linked to that criterion. Store these dimensions separately:

For a graded free response, derive overall `correctness` as `sum(criterion score * criterion weight) / sum(criterion weight)`. Reject a supplied overall value that conflicts with that derivation; do not let a model-authored total override the atomic results.

- `correctness`: factual and logical correctness;
- `reasoning`: quality of the causal explanation;
- `evidence_use`: correct use or identification of repository evidence;
- `initial_correctness`: understanding before Socratic prompts;
- `final_correctness`: understanding after dialogue;
- `max_hint_level`: greatest assistance given;
- `unaided`: whether the successful answer used `H0` only.

Do not collapse repository synchronization into one global number. Report a vector for the assessed scope:

- human-to-repository: business understanding, architecture/data-flow understanding, technical mechanisms, history/bugs, non-functional requirements, unaided retrieval, delayed retention, and confidence calibration;
- model-to-repository: valid evidence coverage, unsupported-claim rate, answer-key ambiguity, evidence freshness, and conflict detection;
- repository-to-human/model: coverage of specs, decisions, tests, terminology, traceability, and document/implementation consistency.

Show a simple session score such as `8/10` only as a session result. When a user needs a go/no-go signal, produce a task-scoped readiness vector for the affected business and technical areas; never claim that it measures understanding of the entire repository.

## Calibrate confidence

Collect confidence before feedback as a number in `[0, 1]`. Encourage the learner to request H4 teaching instead of guessing. Do not turn that request into an `unknown` answer attempt. Keep correctness and confidence separate.

Normalize a five-step UI confidence rating with `confidence = (rating - 1) / 4`, then compute the following for objectively graded items:

```text
brier_error = (confidence - correctness)^2
overconfidence = confidence - correctness
```

Aggregate calibration only after several comparable attempts. Prioritize high-confidence errors because they reveal incorrect mental models. Treat correct low-confidence answers as unstable knowledge and schedule an earlier review. Do not penalize a learner's knowledge score merely for reporting uncertainty honestly.

## Schedule and conduct review

Persist every attempt before showing feedback. Never overwrite the initial answer, confidence, evidence snapshot, or prior grading decision.

Schedule a first unaided success after one day. On successive unaided delayed successes, expand the interval to approximately 3, 7, 14, and 30 days. Shorten the interval after an incorrect answer, a high-confidence error, or use of `H2` through `H4`. Prefer due reviews, important uncovered topics, recently changed evidence, and the learner's current difficulty frontier when selecting the next question.

Use the same persisted state transition in terminal and web flows:

```text
regular single_choice: question_open -> answer_saved -> reviewed
free_text or Socratic: question_open -> answer_saved -> agent_review_pending -> reviewed
H4 teaching: question_open -> teaching_open
learner reflection: teaching_open -> teaching_feedback_saved
host article revision: teaching_feedback_saved -> teaching_open
learner understands: teaching_open -> question_open (same question, still unanswered)
reviewed -> question_open
reviewed -> completed
```

Make every formal answer durable at `answer_saved`. H4 teaching instead appends `teaching_started`, `teaching_feedback_submitted`, `teaching_revised`, and `teaching_completed`; none may add or rewrite an attempt. The article has exactly three layers—operation, function/data-flow logic, and underlying principle—plus a conclusion and reflection prompt. Expand only the frozen answer explanation, rubric, misconceptions, and declared evidence; label generic verification advice as learning method rather than domain fact.

Freeze each reflection against `lesson_id`, `feedback_id`, and `base_revision`. While it is pending, reject answer submission and “我已经懂了”. The host must revalidate current question evidence before publishing a complete new document revision. An exact retry of the same applied revision is idempotent; a different revision for already-applied feedback is a conflict. A browser save cannot wake the host Agent, so terminal `continue` first checks `teaching_pending`, publishes at most one revision, and then waits for the learner's next browser action.

For a regular single-choice answer, revalidate its evidence and append a deterministic `answer_reviewed` event without entering host-Agent review. Move free-text and every Socratic answer to `agent_review_pending` before semantic grading. Treat terminal `continue` as a request to check durable state, grade at most once, show the result, and advance. Make repeated `continue` commands idempotent. Do not send answer keys to the web client before grading or an explicit H4 interruption.

If evidence changes during an open lesson, append `teaching_invalidated`, close the active lesson, preserve any unapplied reflection, and reopen the same question without an attempt. Return the stale evidence IDs to the host. Do not revise or complete the article against stale evidence; any later formal answer follows the normal stale-and-unscored path.

## Handle stale and disputed items

Bind each evidence item to a repository revision and content hash. Before presenting or grading a question, validate all cited evidence against the current repository snapshot.

Set a question to `stale` when a cited artifact disappears or its content hash changes. Do not use a stale question for scoring until its evidence, answer key, and version are revalidated. Preserve historical attempts against their original question version and repository revision.

Set an attempt or question to `disputed` when:

- authoritative sources conflict;
- the learner supplies valid evidence for an answer omitted by the key;
- the rubric cannot distinguish two reasonable answers;
- the automated grader has low confidence;
- the repository and official external documentation describe incompatible versions.

Do not count disputed items as correct or incorrect. Preserve the original decision, append the dispute and supporting evidence, and require explicit revalidation before publishing a new question version. Convert useful conflicts into questions about the inconsistency itself.

## Reject hallucinated grading

Reject a generated item unless all project-specific claims resolve to valid evidence. Reject a `free_text` grade unless the grader returns criterion-level scores, evidence IDs, and a short justification. Record the question version, repository revision, generator, grader, and prompt version for reproducibility.

When evidence is insufficient, say so, teach what can be established, and leave the item unscored. Prefer an honest gap in the bank over a confident but invented answer key.
