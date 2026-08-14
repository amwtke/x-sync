import importlib.util
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "skills" / "x-sync" / "scripts" / "xsync.py"
SPEC = importlib.util.spec_from_file_location("xsync_runtime", SCRIPT)
xsync = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(xsync)


def fixture_bank(commit="0" * 40, repository_id="fixture"):
    evidence = {
        "schema_version": 1, "record_type": "evidence", "id": "ev.spec",
        "kind": "spec", "claim_type": "requirement",
        "authority": "repository_intent", "title": "Goal",
        "claim": "The project aligns people, agents, and repositories.",
        "source": {"type": "file", "path": "spec.md"},
        "repository": {"root_id": repository_id, "baseline_commit": commit},
        "content_hash": "sha256:" + hashlib.sha256(b"alignment\n").hexdigest(), "status": "active",
        "created_at": "2026-08-13T00:00:00+00:00",
        "verified_at": "2026-08-13T00:00:00+00:00"
    }
    common = {
        "schema_version": 1, "record_type": "question", "version": 1,
        "status": "active", "baseline_commit": commit,
        "styles": ["regular", "socratic"], "socratic": {
            "max_attempts": 3, "probes": ["What repository evidence supports that?"],
            "hints": [{"level": 1, "text": "Read the spec."}]},
        "evidence_ids": ["ev.spec"], "prerequisite_question_ids": [],
        "generation": {"generator": "test", "generator_version": "1", "prompt_version": "1"},
        "validation": {"grounded": True, "ambiguity": "low",
                       "validated_at": "2026-08-13T00:00:00+00:00",
                       "validator": "test", "evidence_valid": True},
        "created_at": "2026-08-13T00:00:00+00:00"
    }
    mcq = {**common, "id": "business.goal", "domain": "business",
           "topics": ["goal"], "depth": 1, "type": "single_choice",
           "prompt": "What is the goal?", "choices": [
               {"id": "A", "text": "Alignment", "misconception": None,
                "evidence_ids": ["ev.spec"]},
               {"id": "B", "text": "Deployment", "misconception": "Scope confusion",
                "evidence_ids": ["ev.spec"]}],
           "answer": {"correct_choice": "A", "explanation": "The spec says alignment."}}
    free = {**common, "id": "tech.model", "domain": "technical",
            "topics": ["runtime"], "depth": 3, "type": "free_text",
            "prompt": "Explain the runtime boundary.",
            "answer": {"reference_answer": "The host reasons; runtime persists.",
                       "explanation": "Separation keeps it portable.",
                       "rubric": [{"id": "boundary", "description": "Explains boundary",
                                   "weight": 1.0, "evidence_ids": ["ev.spec"],
                                   "required": True}]}}
    return {"schema_version": 1, "bank_id": "fixture-bank", "repo_id": repository_id,
            "baseline_commit": commit, "working_tree": {"dirty": False},
            "created_at": "2026-08-13T00:00:00+00:00", "evidence": [evidence],
            "questions": [mcq, free]}


class RuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp.name)
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run(["git", "-C", str(self.repo), "config", "user.email", "test@example.com"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "config", "user.name", "Test"], check=True)
        (self.repo / "spec.md").write_text("alignment\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.repo), "add", "spec.md"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-qm", "spec"], check=True)
        self.store = xsync.Store(self.repo, "alice")
        xsync.init_profile(self.store)
        xsync.scan_repository(self.store)
        commit = xsync.git(self.repo, "rev-parse", "HEAD")
        self.bank_path = self.repo / "bank.json"
        self.repository_id = xsync.repo_id(self.repo)
        self.bank_path.write_text(json.dumps(fixture_bank(commit, self.repository_id)), encoding="utf-8")
        xsync.install_bank(self.store, self.bank_path)

    def tearDown(self):
        self.temp.cleanup()

    def test_regular_flow_and_report_has_dimensions_not_global_score(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        self.assertNotIn("answer", started["question"])
        self.assertEqual([{"id": "A", "text": "Alignment"},
                          {"id": "B", "text": "Deployment"}], started["question"]["choices"])
        answered = xsync.submit_answer(self.store, None, "A", "attempt-1",
                                       started["state"]["state_version"], .75, "spec")
        self.assertEqual("reviewed", answered["state"]["status"])
        self.assertTrue(answered["state"]["attempts"][0]["auto_result"]["correct"])
        # Idempotent even after the state has advanced past question_open.
        again = xsync.submit_answer(self.store, None, "A", "attempt-1",
                                    started["state"]["state_version"], .75, "spec")
        self.assertEqual(1, len(again["state"]["attempts"]))
        self.assertEqual(answered["state"]["state_version"], again["state"]["state_version"])
        with self.assertRaises(xsync.XSyncError):
            xsync.submit_answer(self.store, None, "B", "attempt-1", confidence=.75,
                                reason="spec")
        next_state = xsync.continue_session(self.store, None)
        self.assertEqual("tech.model", next_state["question"]["id"])
        report = xsync.make_report(self.store, None)
        self.assertIn("业务理解", report)
        self.assertNotIn("## 总分", report)
        mastery = json.loads((self.store.project / "mastery.json").read_text())
        self.assertEqual("business.goal", mastery["reviews"][0]["question_id"])
        profile = json.loads((self.repo / ".x-sync" / "users" / "alice" /
                              "profile.json").read_text())
        self.assertEqual(1, next(iter(profile["projects"].values()))["reviewed_questions"])

    def test_socratic_free_text_requires_review_and_probe(self):
        started = xsync.start_session(self.store, "fixture-bank", "socratic", "web")
        xsync.submit_answer(self.store, None, "A", "a1", confidence=.5)
        xsync.pending_reviews(self.store, None)
        review = {"session_id": started["session_id"], "question_id": "business.goal",
                  "attempt_id": "a1", "outcome": "probe", "rubric_results": [],
                  "feedback": "Explain evidence."}
        xsync.apply_review(self.store, review)
        mastery = json.loads((self.store.project / "mastery.json").read_text())
        self.assertNotIn("business.goal", [item["question_id"] for item in mastery["reviews"]])
        probed = xsync.continue_session(self.store, None)
        self.assertEqual("question_open", probed["state"]["status"])
        self.assertIn("probe", probed["question"])
        xsync.submit_answer(self.store, None, "A", "a2", confidence=.9)
        xsync.pending_reviews(self.store, None)
        review.update(attempt_id="a2", outcome="mastered")
        xsync.apply_review(self.store, review)
        free = xsync.continue_session(self.store, None)
        xsync.submit_answer(self.store, None, "The host reviews; runtime persists.",
                           "a3", confidence=.8)
        pending = xsync.pending_reviews(self.store, None)["pending"]
        self.assertEqual(["a3"], [item["attempt_id"] for item in pending])
        xsync.apply_review(self.store, {"session_id": started["session_id"],
            "question_id": "tech.model", "attempt_id": "a3", "outcome": "mastered",
            "rubric_results": [{"rubric_id": "boundary", "earned": 1,
                                "reason": "Explains the separation.",
                                "evidence_ids": ["ev.spec"]}], "feedback": "Good.",
            "evaluation": {"correctness": 1, "initial_correctness": .5,
                           "final_correctness": 1, "reasoning": 1,
                           "evidence_use": 1, "max_hint_level": 1,
                           "unaided": False, "evidence_ids": ["ev.spec"]}})
        self.assertEqual("completed", xsync.continue_session(self.store, None)["state"]["status"])

    def test_validation_paths_snapshot_and_atomic_files(self):
        with self.assertRaises(xsync.XSyncError):
            xsync.Store(self.repo, "../escape")
        record = xsync.snapshot_evidence(self.repo, self.store, None, "code",
                                         "spec.md", "1:1", "Project goal")
        self.assertEqual("code", record["kind"])
        self.assertTrue(Path(record["path"]).is_file())
        self.assertTrue((self.store.project / "banks" / "fixture-bank.json").is_file())
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        xsync.submit_answer(self.store, None, "A", "event-test", confidence=.5)
        events = list(self.store.session_dir(started["session_id"]).joinpath("events").glob("*.json"))
        self.assertGreaterEqual(len(events), 2)

    def test_bank_snapshot_is_immutable_and_stale_bank_is_rejected(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        session_bank = self.store.session_dir(started["session_id"]) / "bank.json"
        self.assertTrue(session_bank.is_file())
        changed = fixture_bank(xsync.git(self.repo, "rev-parse", "HEAD"), self.repository_id)
        changed["questions"][0]["prompt"] = "Changed"
        self.bank_path.write_text(json.dumps(changed), encoding="utf-8")
        with self.assertRaises(xsync.XSyncError):
            xsync.install_bank(self.store, self.bank_path)
        self.assertEqual("What is the goal?", xsync.public_session(self.store)["question"]["prompt"])

        (self.repo / "spec.md").write_text("changed\n", encoding="utf-8")
        with self.assertRaises(xsync.XSyncError):
            xsync.start_session(self.store, "fixture-bank", "regular", "terminal")

    def test_state_recovers_from_append_only_event_and_next_session_is_adaptive(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        xsync.submit_answer(self.store, None, "A", "remembered", confidence=.9)
        state_path = self.store.session_dir(started["session_id"]) / "state.json"
        state_path.write_text("{broken", encoding="utf-8")
        recovered = xsync.public_session(self.store, started["session_id"])
        self.assertEqual("reviewed", recovered["session"]["status"])
        self.assertEqual("remembered", recovered["state"]["attempts"][0]["attempt_id"])
        json.loads(state_path.read_text(encoding="utf-8"))

        next_session = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        self.assertEqual("tech.model", next_session["question"]["id"])

    def test_commit_evidence_and_sensitive_path_are_checked(self):
        commit = xsync.git(self.repo, "rev-parse", "HEAD")
        patch = xsync.commit_snapshot_bytes(self.repo, commit, Path("spec.md"))
        self.assertIn(b"spec.md", patch)
        subprocess.run(["git", "-C", str(self.repo), "config", "core.quotePath", "false"],
                       check=True)
        self.assertEqual(patch, xsync.commit_snapshot_bytes(self.repo, commit, Path("spec.md")))
        with self.assertRaises(xsync.XSyncError):
            xsync.commit_snapshot_bytes(self.repo, commit, Path("not-touched.md"))
        record = xsync.snapshot_evidence(self.repo, self.store, None, "commit",
                                         "spec.md", summary="Initial decision",
                                         commit_ref=commit)
        self.assertEqual("commit", record["source"]["type"])
        bank = fixture_bank(commit, self.repository_id)
        bank["evidence"] = [record]
        for question in bank["questions"]:
            question["evidence_ids"] = [record["id"]]
            for choice in question.get("choices", []):
                choice["evidence_ids"] = [record["id"]]
            for rubric in question.get("answer", {}).get("rubric", []):
                rubric["evidence_ids"] = [record["id"]]
        xsync.validate_evidence_freshness(self.repo, xsync.validate_bank(bank))
        with self.assertRaises(xsync.XSyncError):
            xsync.snapshot_evidence(self.repo, self.store, None, "code", ".env")

    def test_focused_evidence_rejects_binary_oversized_and_invalid_ranges(self):
        (self.repo / "binary.dat").write_bytes(b"text\0binary")
        with self.assertRaises(xsync.XSyncError):
            xsync.snapshot_evidence(self.repo, self.store, None, "code", "binary.dat")
        (self.repo / "invalid.txt").write_bytes(b"\xff\xfe")
        with self.assertRaises(xsync.XSyncError):
            xsync.snapshot_evidence(self.repo, self.store, None, "code", "invalid.txt")
        (self.repo / "large.txt").write_bytes(
            b"x" * (xsync.MAX_FOCUSED_EVIDENCE_BYTES + 1)
        )
        with self.assertRaises(xsync.XSyncError):
            xsync.snapshot_evidence(self.repo, self.store, None, "code", "large.txt")
        with self.assertRaises(xsync.XSyncError):
            xsync.snapshot_evidence(self.repo, self.store, None, "code", "spec.md", "2:2")

    def test_concurrent_submissions_do_not_overwrite_each_other(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        barrier = threading.Barrier(3)
        outcomes = []

        def submit(attempt_id):
            barrier.wait()
            try:
                xsync.submit_answer(self.store, None, "A", attempt_id,
                                    started["state"]["state_version"], .5)
                outcomes.append("accepted")
            except xsync.XSyncError:
                outcomes.append("conflict")

        threads = [threading.Thread(target=submit, args=(f"race-{number}",)) for number in (1, 2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(5)
        self.assertEqual(["accepted", "conflict"], sorted(outcomes))
        self.assertEqual(1, len(xsync.public_session(self.store)["state"]["attempts"]))

    def test_durable_answer_recovers_if_auto_review_was_interrupted(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        with mock.patch.object(xsync, "apply_auto_review", side_effect=RuntimeError("crash")):
            with self.assertRaisesRegex(RuntimeError, "crash"):
                xsync.submit_answer(
                    self.store, started["session_id"], "A", "resume-auto", confidence=.8
                )
        saved = xsync.public_session(self.store, started["session_id"])
        self.assertEqual("answer_saved", saved["state"]["status"])
        recovered = xsync.submit_answer(
            self.store, started["session_id"], "A", "resume-auto", confidence=.8
        )
        self.assertEqual("reviewed", recovered["state"]["status"])
        self.assertTrue(recovered["state"]["attempts"][0]["auto_result"]["correct"])
        event_types = [
            event["event_type"]
            for event in xsync.load_event_chain(self.store.session_dir(started["session_id"]))
        ]
        self.assertEqual(1, event_types.count("answer_submitted"))
        self.assertEqual(1, event_types.count("answer_reviewed"))

    def test_unknown_interrupts_into_revisioned_h4_lesson_then_retries_same_question(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "web")
        lesson_view = xsync.request_teaching(
            self.store, started["session_id"], started["state"]["state_version"]
        )
        lesson = lesson_view["lesson"]
        self.assertEqual("lesson", lesson_view["view"])
        self.assertEqual("teaching_open", lesson_view["session"]["status"])
        self.assertEqual([], xsync.public_session(self.store)["state"]["attempts"])
        self.assertEqual(
            ["operation", "logic", "principle"],
            [section["layer"] for section in lesson["document"]["sections"]],
        )
        with self.assertRaises(xsync.XSyncError):
            xsync.submit_answer(self.store, started["session_id"], "A", "too-early",
                                confidence=.5)

        saved = xsync.submit_lesson_feedback(
            self.store, started["session_id"],
            "我理解答案来自 spec，但还想看清 evidence 到结论的链路。",
            "feedback-fixture", lesson["lesson_id"], None,
            lesson_view["session"]["state_version"],
        )
        self.assertTrue(saved["lesson"]["feedback_pending"])
        self.assertIn("evidence 到结论", saved["lesson"]["feedback_pending"]["text"])
        repeated_feedback = xsync.submit_lesson_feedback(
            self.store, started["session_id"],
            "我理解答案来自 spec，但还想看清 evidence 到结论的链路。",
            "feedback-fixture", lesson["lesson_id"], None,
            lesson_view["session"]["state_version"],
        )
        self.assertEqual(
            saved["session"]["state_version"],
            repeated_feedback["session"]["state_version"],
        )
        pending = xsync.pending_reviews(self.store, started["session_id"])
        self.assertEqual([], pending["pending"])
        self.assertEqual("feedback-fixture",
                         pending["teaching_pending"]["feedback"]["feedback_id"])
        with self.assertRaises(xsync.XSyncError):
            xsync.complete_teaching_lesson(
                self.store, started["session_id"], lesson["lesson_id"]
            )

        revised_document = copy.deepcopy(lesson["document"])
        revised_document["sections"][1]["paragraphs"].append(
            "spec.md 的 evidence claim 直接约束本题允许得出的结论。"
        )
        revision_request = {
            "session_id": started["session_id"],
            "lesson_id": lesson["lesson_id"],
            "feedback_id": "feedback-fixture",
            "base_revision": 1,
            "document": revised_document,
            "author": {"name": "test-host", "version": "1"},
        }
        revised = xsync.apply_lesson_revision(self.store, revision_request)
        self.assertEqual(2, revised["lesson"]["revision"])
        repeated_revision = xsync.apply_lesson_revision(self.store, revision_request)
        self.assertEqual(
            revised["session"]["state_version"],
            repeated_revision["session"]["state_version"],
        )
        resumed = xsync.complete_teaching_lesson(
            self.store, started["session_id"], lesson["lesson_id"],
            revised["session"]["state_version"],
        )
        self.assertEqual("quiz", resumed["view"])
        self.assertEqual("business.goal", resumed["question"]["id"])
        self.assertIsNone(resumed["lesson"])

        answered = xsync.submit_answer(
            self.store, started["session_id"], "A", "after-lesson",
            resumed["session"]["state_version"], .75, "spec evidence",
        )
        attempt = answered["state"]["attempts"][0]
        self.assertEqual(4, attempt["max_hint_level"])
        self.assertFalse(attempt["review"]["unaided"])
        event_types = [
            event["event_type"] for event in xsync.load_event_chain(
                self.store.session_dir(started["session_id"])
            )
        ]
        self.assertEqual(
            ["teaching_started", "teaching_feedback_submitted",
             "teaching_revised", "teaching_completed"],
            [event for event in event_types if event.startswith("teaching_")],
        )

    def test_terminal_h4_has_feedback_revision_and_complete_cli_paths(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")

        def run_cli(*arguments):
            result = subprocess.run(
                [sys.executable, str(SCRIPT), *arguments], text=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
            )
            return json.loads(result.stdout)

        taught = run_cli(
            "teach", "--repo", str(self.repo), "--learner", "alice",
            "--session", started["session_id"], "--state-version",
            str(started["session"]["state_version"]), "--json",
        )
        lesson = taught["lesson"]
        saved = run_cli(
            "lesson", "feedback", "--repo", str(self.repo), "--learner", "alice",
            "--session", started["session_id"], "--lesson-id", lesson["lesson_id"],
            "--text", "终端反馈：请补充证据链。", "--feedback-id", "terminal-feedback",
            "--base-revision", str(lesson["revision"]), "--state-version",
            str(taught["session"]["state_version"]), "--json",
        )
        self.assertEqual("teaching_feedback_saved", saved["session"]["status"])
        pending = xsync.pending_reviews(self.store, started["session_id"])["teaching_pending"]
        revision_document = copy.deepcopy(pending["document"])
        revision_document["sections"][1]["paragraphs"].append("Terminal revision.")
        revision_path = self.repo / "lesson-revision.json"
        revision_path.write_text(json.dumps({
            "session_id": started["session_id"],
            "lesson_id": lesson["lesson_id"],
            "feedback_id": "terminal-feedback",
            "base_revision": pending["base_revision"],
            "document": revision_document,
            "author": {"name": "terminal-test", "version": "1"},
        }), encoding="utf-8")
        revised = run_cli(
            "lesson", "revise", "--repo", str(self.repo), "--learner", "alice",
            "--session", started["session_id"], "--file", str(revision_path), "--json",
        )
        completed = run_cli(
            "lesson", "complete", "--repo", str(self.repo), "--learner", "alice",
            "--session", started["session_id"], "--lesson-id", lesson["lesson_id"],
            "--state-version", str(revised["session"]["state_version"]), "--json",
        )
        self.assertEqual("question_open", completed["session"]["status"])
        answered = xsync.submit_answer(
            self.store, started["session_id"], "A", "terminal-after-h4", confidence=.5
        )
        self.assertEqual(4, answered["state"]["attempts"][0]["max_hint_level"])

    def test_evidence_is_rechecked_before_scoring_and_host_review(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        (self.repo / "spec.md").write_text("changed\n", encoding="utf-8")
        answered = xsync.submit_answer(self.store, started["session_id"], "A",
                                       "stale-choice", confidence=1)
        attempt = answered["state"]["attempts"][0]
        self.assertNotIn("auto_result", attempt)
        self.assertEqual("stale", attempt["review"]["status"])
        mastery = xsync.rebuild_mastery(self.store)
        self.assertEqual("stale", mastery["reviews"][0]["stage"])

        (self.repo / "spec.md").write_text("alignment\n", encoding="utf-8")
        second = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        self.assertEqual("tech.model", second["question"]["id"])
        xsync.submit_answer(self.store, second["session_id"], "Host reasons; runtime stores.",
                           "stale-text", confidence=.7)
        (self.repo / "spec.md").write_text("changed again\n", encoding="utf-8")
        self.assertEqual([], xsync.pending_reviews(self.store, second["session_id"])["pending"])
        stale = xsync.public_session(self.store, second["session_id"])["state"]["attempts"][0]
        self.assertEqual("Host reasons; runtime stores.", stale["response"])
        self.assertEqual("stale", stale["review"]["status"])

    def test_stale_evidence_closes_pending_lesson_without_deadlocking_session(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "web")
        taught = xsync.request_teaching(
            self.store, started["session_id"], started["session"]["state_version"]
        )
        lesson = taught["lesson"]
        xsync.submit_lesson_feedback(
            self.store, started["session_id"], "请补充这个结论的证据。",
            "stale-feedback", lesson["lesson_id"], lesson["revision"],
            taught["session"]["state_version"],
        )
        (self.repo / "spec.md").write_text("changed during lesson\n", encoding="utf-8")

        pending = xsync.pending_reviews(self.store, started["session_id"])
        self.assertIsNone(pending["teaching_pending"])
        self.assertEqual(["ev.spec"],
                         pending["teaching_invalidated"]["stale_evidence_ids"])
        reopened = xsync.public_session(self.store, started["session_id"])
        self.assertEqual("question_open", reopened["session"]["status"])
        self.assertEqual([], reopened["state"]["attempts"])
        self.assertIsNone(reopened["lesson"])
        self.assertIsNotNone(reopened["state"]["lessons"][0]["completed_at"])

        answered = xsync.submit_answer(
            self.store, started["session_id"], "A", "stale-after-lesson", confidence=.5
        )
        attempt = answered["state"]["attempts"][0]
        self.assertEqual(4, attempt["max_hint_level"])
        self.assertEqual("stale", attempt["review"]["status"])
        events = xsync.load_event_chain(self.store.session_dir(started["session_id"]))
        self.assertIn("teaching_invalidated", [event["event_type"] for event in events])

    def test_unrelated_stale_evidence_does_not_block_a_session(self):
        extra = self.repo / "extra.md"
        extra.write_text("unrelated\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.repo), "add", "extra.md"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-qm", "extra"], check=True)
        commit = xsync.git(self.repo, "rev-parse", "HEAD")
        bank = fixture_bank(commit, self.repository_id)
        bank["bank_id"] = "unused-evidence"
        bank["evidence"].append({
            **bank["evidence"][0], "id": "ev.unused", "title": "Unused",
            "claim": "Unused evidence", "source": {"type": "file", "path": "extra.md"},
            "content_hash": "sha256:" + hashlib.sha256(b"unrelated\n").hexdigest(),
        })
        path = self.repo / "unused-bank.json"
        path.write_text(json.dumps(bank), encoding="utf-8")
        xsync.install_bank(self.store, path)
        extra.write_text("now stale\n", encoding="utf-8")
        started = xsync.start_session(self.store, "unused-evidence", "regular", "terminal")
        self.assertEqual("business.goal", started["question"]["id"])

    def test_bank_provenance_and_unique_answer_grounding_are_enforced(self):
        commit = xsync.git(self.repo, "rev-parse", "HEAD")
        missing = fixture_bank(commit, self.repository_id)
        del missing["repo_id"]
        with self.assertRaises(xsync.XSyncError):
            xsync.validate_bank(missing)

        mismatched = fixture_bank(commit, self.repository_id)
        mismatched["questions"][0]["baseline_commit"] = "f" * 40
        with self.assertRaises(xsync.XSyncError):
            xsync.validate_bank(mismatched)

        inference = fixture_bank(commit, self.repository_id)
        second = {**inference["evidence"][0], "id": "ev.second", "title": "Second"}
        derived = {**inference["evidence"][0], "id": "ev.inference", "kind": "inference",
                   "claim_type": "inference", "authority": "derived",
                   "derived_from": ["ev.spec", "ev.second"], "title": "Derived"}
        inference["evidence"].extend([second, derived])
        inference["questions"][0]["evidence_ids"] = ["ev.inference"]
        for choice in inference["questions"][0]["choices"]:
            choice["evidence_ids"] = ["ev.inference"]
        with self.assertRaises(xsync.XSyncError):
            xsync.validate_bank(inference)

        cross_repo = fixture_bank(commit, "some-other-repository")
        path = self.repo / "cross-repo.json"
        path.write_text(json.dumps(cross_repo), encoding="utf-8")
        with self.assertRaises(xsync.XSyncError):
            xsync.install_bank(self.store, path)

        malformed = fixture_bank(commit, self.repository_id)
        malformed["questions"][0]["depth"] = True
        with self.assertRaises(xsync.XSyncError):
            xsync.validate_bank(malformed)
        malformed = fixture_bank(commit, self.repository_id)
        malformed["questions"][0]["choices"][0]["text"] = ""
        with self.assertRaises(xsync.XSyncError):
            xsync.validate_bank(malformed)

    def test_review_invariants_and_pending_context_are_enforced(self):
        started = xsync.start_session(self.store, "fixture-bank", "socratic", "web")
        xsync.submit_answer(self.store, started["session_id"], "A", "review-context",
                           confidence=.8, reason="The spec says so.")
        pending = xsync.pending_reviews(self.store, started["session_id"])["pending"][0]
        self.assertEqual("The spec says so.", pending["reason"])
        self.assertEqual(.8, pending["confidence"])
        self.assertEqual(1, pending["question_version"])
        base = {"session_id": started["session_id"], "question_id": "business.goal",
                "attempt_id": "review-context", "outcome": "mastered",
                "rubric_results": [], "feedback": "conflicting"}
        with self.assertRaises(xsync.XSyncError):
            xsync.apply_review(self.store, {**base, "evaluation": {
                "correctness": 0, "max_hint_level": 0, "unaided": False,
                "evidence_ids": ["ev.spec"]}})
        with self.assertRaises(xsync.XSyncError):
            xsync.apply_review(self.store, {**base, "evaluation": {
                "correctness": 1, "max_hint_level": 4, "unaided": True,
                "evidence_ids": ["ev.spec"]}})

    def test_free_text_score_and_criterion_evidence_are_bank_bound(self):
        def open_free_text(learner, bank_id, attempt_id):
            started = xsync.start_session(
                learner, bank_id, "regular", "terminal", focus="technical", count=1
            )
            xsync.submit_answer(
                learner, started["session_id"], "The runtime persists state.",
                attempt_id, confidence=.7
            )
            xsync.pending_reviews(learner, started["session_id"])
            return started

        score_store = xsync.Store(self.repo, "rubric-score")
        xsync.init_profile(score_store)
        xsync.install_bank(score_store, self.bank_path)
        started = open_free_text(score_store, "fixture-bank", "score-conflict")
        with self.assertRaises(xsync.XSyncError):
            xsync.apply_review(score_store, {
                "session_id": started["session_id"], "question_id": "tech.model",
                "attempt_id": "score-conflict", "outcome": "mastered",
                "rubric_results": [{"rubric_id": "boundary", "earned": 0,
                                    "reason": "Boundary was not explained.",
                                    "evidence_ids": ["ev.spec"]}],
                "feedback": "Conflicting total.",
                "evaluation": {"correctness": 1, "reasoning": 0,
                               "evidence_use": 0, "max_hint_level": 0,
                               "unaided": False, "evidence_ids": ["ev.spec"]},
            })

        commit = xsync.git(self.repo, "rev-parse", "HEAD")
        bank = fixture_bank(commit, self.repository_id)
        bank["bank_id"] = "criterion-evidence-bank"
        other = {**bank["evidence"][0], "id": "ev.other", "title": "Other evidence"}
        bank["evidence"].append(other)
        bank["questions"][1]["evidence_ids"].append("ev.other")
        bank_path = self.repo / "criterion-evidence-bank.json"
        bank_path.write_text(json.dumps(bank), encoding="utf-8")
        evidence_store = xsync.Store(self.repo, "rubric-evidence")
        xsync.init_profile(evidence_store)
        xsync.install_bank(evidence_store, bank_path)
        started = open_free_text(
            evidence_store, "criterion-evidence-bank", "wrong-criterion-evidence"
        )
        with self.assertRaises(xsync.XSyncError):
            xsync.apply_review(evidence_store, {
                "session_id": started["session_id"], "question_id": "tech.model",
                "attempt_id": "wrong-criterion-evidence", "outcome": "mastered",
                "rubric_results": [{"rubric_id": "boundary", "earned": 1,
                                    "reason": "Uses unrelated evidence.",
                                    "evidence_ids": ["ev.other"]}],
                "feedback": "Wrong criterion evidence.",
                "evaluation": {"correctness": 1, "reasoning": 1,
                               "evidence_use": 1, "max_hint_level": 0,
                               "unaided": True, "evidence_ids": ["ev.other"]},
            })

        # Recovery uses the same bank-aware rule, even when payload and state
        # are tampered together to evade the event delta comparison.
        recovery_store = xsync.Store(self.repo, "rubric-recovery")
        xsync.init_profile(recovery_store)
        xsync.install_bank(recovery_store, self.bank_path)
        started = open_free_text(recovery_store, "fixture-bank", "recover-conflict")
        xsync.apply_review(recovery_store, {
            "session_id": started["session_id"], "question_id": "tech.model",
            "attempt_id": "recover-conflict", "outcome": "mastered",
            "rubric_results": [{"rubric_id": "boundary", "earned": 1,
                                "reason": "Grounded.", "evidence_ids": ["ev.spec"]}],
            "feedback": "Valid before tampering.",
            "evaluation": {"correctness": 1, "reasoning": 1, "evidence_use": 1,
                           "max_hint_level": 0, "unaided": True,
                           "evidence_ids": ["ev.spec"]},
        })
        directory = recovery_store.session_dir(started["session_id"])
        event_path = sorted((directory / "events").glob("*.json"))[-1]
        event = json.loads(event_path.read_text())
        for review in (event["payload"]["evaluation"],
                       event["state_after"]["attempts"][0]["review"]):
            review["correctness"] = .5
            review["outcome"] = "exhausted"
            review["unaided"] = False
        event["payload"]["outcome"] = "exhausted"
        event_path.write_text(json.dumps(event), encoding="utf-8")
        (directory / "state.json").unlink()
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(recovery_store, started["session_id"])

    def test_private_data_is_locally_ignored_and_task_scope_is_persisted(self):
        result = subprocess.run(
            ["git", "-C", str(self.repo), "check-ignore", "-q",
             ".x-sync/users/alice/profile.json"], check=False
        )
        self.assertEqual(0, result.returncode)
        started = xsync.start_session(
            self.store, "fixture-bank", "regular", "terminal", focus="technical",
            max_depth=3, task_scope="Change the persistence boundary"
        )
        self.assertEqual("tech.model", started["question"]["id"])
        self.assertEqual("technical", started["session"]["focus"])
        self.assertEqual(3, started["session"]["max_depth"])
        self.assertEqual("Change the persistence boundary", started["session"]["task_scope"])

    def test_concurrent_mastery_rebuild_keeps_all_observations(self):
        first = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        second = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        barrier = threading.Barrier(3)
        errors = []

        def answer(session_id, attempt_id):
            barrier.wait()
            try:
                xsync.submit_answer(self.store, session_id, "A", attempt_id, confidence=.8)
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=answer, args=(first["session_id"], "mastery-1")),
                   threading.Thread(target=answer, args=(second["session_id"], "mastery-2"))]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(5)
        self.assertEqual([], errors)
        mastery = json.loads((self.store.project / "mastery.json").read_text())
        matching = [item for item in mastery["history"]
                    if item["question_id"] == "business.goal"]
        self.assertEqual(2, len(matching))

    def test_events_are_complete_continue_is_idempotent_and_events_win_recovery(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        answered = xsync.submit_answer(
            self.store, started["session_id"], "A", "event-contract", confidence=.8
        )
        directory = self.store.session_dir(started["session_id"])
        chain = xsync.load_event_chain(directory)
        self.assertEqual(
            ["session_started", "answer_submitted", "answer_reviewed"],
            [event["event_type"] for event in chain],
        )
        reviewed = chain[-1]["payload"]
        self.assertEqual("event-contract", reviewed["attempt_id"])
        self.assertIn("evaluation", reviewed)
        self.assertIn("next_review_at", reviewed)

        advanced = xsync.continue_session(self.store, started["session_id"])
        repeated = xsync.continue_session(self.store, started["session_id"])
        self.assertEqual(advanced["state"]["state_version"], repeated["state"]["state_version"])
        presented = xsync.load_event_chain(directory)[-1]
        self.assertEqual("question_presented", presented["event_type"])
        self.assertEqual("tech.model", presented["payload"]["id"])
        self.assertIn("evidence_validated_at", presented["payload"])

        forged = {**advanced["state"], "state_version": 999}
        (directory / "state.json").write_text(json.dumps(forged), encoding="utf-8")
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(self.store, started["session_id"])
        forged = {**advanced["state"], "updated_at": "same-version-corruption"}
        (directory / "state.json").write_text(json.dumps(forged), encoding="utf-8")
        recovered = xsync.public_session(self.store, started["session_id"])
        self.assertEqual("question_open", recovered["state"]["status"])
        self.assertEqual("tech.model", recovered["question"]["id"])
        self.assertEqual(answered["state"]["state_version"] + 1,
                         recovered["state"]["state_version"])

    def test_event_recovery_fails_closed_on_semantic_corruption_or_missing_history(self):
        def first_event():
            started = xsync.start_session(
                self.store, "fixture-bank", "regular", "terminal",
                focus="business", count=1
            )
            directory = self.store.session_dir(started["session_id"])
            event_path = sorted((directory / "events").glob("*.json"))[0]
            return started, directory, event_path, json.loads(event_path.read_text())

        for mutation in ("payload", "event_id", "transition", "event_type"):
            with self.subTest(mutation=mutation):
                started, directory, event_path, event = first_event()
                if mutation == "payload":
                    event["payload"] = {}
                elif mutation == "event_id":
                    event["event_id"] = "different-event-id"
                elif mutation == "transition":
                    event["state_after"].update({
                        "status": "completed", "current_index": event["state_after"]["total"],
                        "current_question_id": None,
                    })
                else:
                    event["event_type"] = "invented_transition"
                event_path.write_text(json.dumps(event), encoding="utf-8")
                with self.assertRaises(xsync.XSyncError):
                    xsync.public_session(self.store, started["session_id"])

        started, directory, event_path, _ = first_event()
        event_path.unlink()
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(self.store, started["session_id"])

        started, directory, _, _ = first_event()
        xsync.submit_answer(self.store, started["session_id"], "A", "tail", confidence=.8)
        tail = sorted((directory / "events").glob("*.json"))[-1]
        tail.unlink()
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(self.store, started["session_id"])

        started, directory, _, _ = first_event()
        (directory / "events" / "000002-corrupt.json").write_bytes(b"\xff\xfe")
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(self.store, started["session_id"])

    def test_concurrent_and_delayed_continue_retries_are_idempotent(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        xsync.submit_answer(self.store, started["session_id"], "A", "continue-race",
                           confidence=.8)
        barrier = threading.Barrier(2)
        original_mutate = self.store.mutate
        outcomes = []

        def synchronized_mutate(directory, event_type, payload, transform):
            if event_type == "question_presented":
                barrier.wait(timeout=5)
            return original_mutate(directory, event_type, payload, transform)

        def run_continue():
            try:
                value = xsync.continue_session(self.store, started["session_id"])
                outcomes.append(("ok", value["state"]["state_version"]))
            except Exception as exc:  # pragma: no cover - asserted below
                outcomes.append((type(exc).__name__, str(exc)))

        with mock.patch.object(self.store, "mutate", side_effect=synchronized_mutate):
            threads = [threading.Thread(target=run_continue) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(5)
        self.assertEqual(["ok", "ok"], sorted(item[0] for item in outcomes))
        events = xsync.load_event_chain(self.store.session_dir(started["session_id"]))
        self.assertEqual(1, sum(event["event_type"] == "question_presented" for event in events))

        # Capture an old q1 continuation, advance/review q2, then release q1.
        # Use a fresh learner so adaptive ordering from the first scenario
        # cannot move the free-text question ahead of q1.
        delayed_store = xsync.Store(self.repo, "bob")
        xsync.init_profile(delayed_store)
        xsync.install_bank(delayed_store, self.bank_path)
        delayed_started = xsync.start_session(
            delayed_store, "fixture-bank", "regular", "terminal"
        )
        xsync.submit_answer(delayed_store, delayed_started["session_id"], "A", "old-q1",
                           confidence=.8)
        captured = threading.Event()
        release = threading.Event()
        delayed_result = []
        base_mutate = xsync.Store.mutate

        def delay_old_mutate(instance, directory, event_type, payload, transform):
            if threading.current_thread().name == "delayed-q1" and event_type == "question_presented":
                captured.set()
                release.wait(5)
            return base_mutate(instance, directory, event_type, payload, transform)

        def run_delayed():
            try:
                delayed_result.append(
                    xsync.continue_session(delayed_store, delayed_started["session_id"])
                )
            except Exception as exc:  # pragma: no cover - asserted below
                delayed_result.append(exc)

        with mock.patch.object(xsync.Store, "mutate", new=delay_old_mutate):
            thread = threading.Thread(target=run_delayed, name="delayed-q1")
            thread.start()
            self.assertTrue(captured.wait(5))
            xsync.continue_session(delayed_store, delayed_started["session_id"])
            xsync.submit_answer(
                delayed_store, delayed_started["session_id"], "Host reasons; runtime persists.",
                "new-q2", confidence=.8
            )
            xsync.pending_reviews(delayed_store, delayed_started["session_id"])
            xsync.apply_review(delayed_store, {
                "session_id": delayed_started["session_id"], "question_id": "tech.model",
                "attempt_id": "new-q2", "outcome": "mastered",
                "rubric_results": [{"rubric_id": "boundary", "earned": 1,
                                    "reason": "Explains boundary.",
                                    "evidence_ids": ["ev.spec"]}],
                "feedback": "Good.",
                "evaluation": {"correctness": 1, "reasoning": 1, "evidence_use": 1,
                               "max_hint_level": 0, "unaided": True,
                               "evidence_ids": ["ev.spec"]},
            })
            completed = xsync.continue_session(
                delayed_store, delayed_started["session_id"]
            )
            self.assertEqual("completed", completed["state"]["status"])
            release.set()
            thread.join(5)
        self.assertEqual(1, len(delayed_result))
        self.assertNotIsInstance(delayed_result[0], Exception)
        final = xsync.public_session(delayed_store, delayed_started["session_id"])
        self.assertEqual("completed", final["state"]["status"])
        self.assertIsNone(final["state"]["current_question_id"])
        events = xsync.load_event_chain(
            delayed_store.session_dir(delayed_started["session_id"])
        )
        transition_keys = [
            event["payload"].get("idempotency_key") for event in events
            if event["event_type"] in {"question_presented", "socratic_turn", "session_ended"}
        ]
        self.assertEqual(2, len(transition_keys))
        self.assertEqual(2, len(set(transition_keys)))

    def test_event_recovery_rejects_answer_or_continue_history_rewrites(self):
        def reviewed_event():
            started = xsync.start_session(
                self.store, "fixture-bank", "regular", "terminal",
                focus="business", count=1
            )
            xsync.submit_answer(
                self.store, started["session_id"], "A", uuid.uuid4().hex, confidence=.8
            )
            directory = self.store.session_dir(started["session_id"])
            event_path = sorted((directory / "events").glob("*.json"))[-1]
            return started, directory, event_path, json.loads(event_path.read_text())

        for mutation in (
            "response", "confidence", "total", "evaluation", "invalid_review",
            "unscored_with_score", "oversized_earned", "infinite_earned",
        ):
            with self.subTest(mutation=mutation):
                started, directory, event_path, event = reviewed_event()
                if mutation == "response":
                    event["state_after"]["attempts"][0]["response"] = "B"
                elif mutation == "confidence":
                    event["state_after"]["attempts"][0]["confidence"] = .01
                elif mutation == "total":
                    event["state_after"]["total"] += 1
                elif mutation == "evaluation":
                    event["payload"]["evaluation"]["correctness"] = 0
                elif mutation == "invalid_review":
                    event["payload"]["evaluation"]["correctness"] = 99
                    event["state_after"]["attempts"][0]["review"]["correctness"] = 99
                elif mutation == "unscored_with_score":
                    for review in (event["payload"]["evaluation"],
                                   event["state_after"]["attempts"][0]["review"]):
                        review.update({"status": "unscored", "correctness": 1,
                                       "outcome": "mastered"})
                else:
                    result = {"rubric_id": "invented", "earned": (
                                  float("inf") if mutation == "infinite_earned" else 999999
                              ), "score": 1, "evidence_ids": ["ev.spec"],
                              "justification": "tampered"}
                    event["payload"]["evaluation"]["rubric_results"] = [result]
                    event["state_after"]["attempts"][0]["review"]["rubric_results"] = [result]
                event_path.write_text(json.dumps(event), encoding="utf-8")
                (directory / "state.json").unlink()
                with self.assertRaises(xsync.XSyncError):
                    xsync.public_session(self.store, started["session_id"])

        started = xsync.start_session(
            self.store, "fixture-bank", "regular", "terminal",
            focus="business", count=1
        )
        with mock.patch.object(xsync, "apply_auto_review", side_effect=RuntimeError("crash")):
            with self.assertRaises(RuntimeError):
                xsync.submit_answer(
                    self.store, started["session_id"], "A", "payload-mismatch", confidence=.8
                )
        directory = self.store.session_dir(started["session_id"])
        submitted_path = sorted((directory / "events").glob("*.json"))[-1]
        submitted = json.loads(submitted_path.read_text())
        submitted["payload"]["answer"] = "B"
        submitted_path.write_text(json.dumps(submitted), encoding="utf-8")
        (directory / "state.json").unlink()
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(self.store, started["session_id"])

        for mutation in ("premature_review", "malformed_auto_result"):
            with self.subTest(mutation=mutation):
                started = xsync.start_session(
                    self.store, "fixture-bank", "regular", "terminal",
                    focus="business", count=1
                )
                with mock.patch.object(
                    xsync, "apply_auto_review", side_effect=RuntimeError("crash")
                ):
                    with self.assertRaises(RuntimeError):
                        xsync.submit_answer(
                            self.store, started["session_id"], "A", uuid.uuid4().hex,
                            confidence=.8
                        )
                directory = self.store.session_dir(started["session_id"])
                submitted_path = sorted((directory / "events").glob("*.json"))[-1]
                submitted = json.loads(submitted_path.read_text())
                attempt = submitted["state_after"]["attempts"][0]
                if mutation == "premature_review":
                    attempt["review"] = {}
                else:
                    attempt["auto_result"] = {"correct": "yes", "selected": "A"}
                submitted_path.write_text(json.dumps(submitted), encoding="utf-8")
                (directory / "state.json").unlink()
                with self.assertRaises(xsync.XSyncError):
                    xsync.public_session(self.store, started["session_id"])

        started, directory, _, _ = reviewed_event()
        xsync.continue_session(self.store, started["session_id"])
        transition_path = sorted((directory / "events").glob("*.json"))[-1]
        transition = json.loads(transition_path.read_text())
        transition["payload"]["idempotency_key"] = "different-consume-key"
        transition_path.write_text(json.dumps(transition), encoding="utf-8")
        (directory / "state.json").unlink()
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(self.store, started["session_id"])

    def test_colon_ids_cannot_collide_in_idempotency_keys(self):
        commit = xsync.git(self.repo, "rev-parse", "HEAD")
        bank = fixture_bank(commit, self.repository_id)
        first = json.loads(json.dumps(bank["questions"][0]))
        first["id"] = "c"
        second = json.loads(json.dumps(first))
        second["id"] = "b:c"
        second["prompt"] = "What is the same goal, checked again?"
        bank["bank_id"] = "colon-collision-bank"
        bank["questions"] = [first, second]
        path = self.repo / "colon-bank.json"
        path.write_text(json.dumps(bank), encoding="utf-8")
        xsync.install_bank(self.store, path)

        started = xsync.start_session(
            self.store, "colon-collision-bank", "regular", "terminal"
        )
        self.assertEqual("c", started["question"]["id"])
        xsync.submit_answer(self.store, started["session_id"], "A", "a:b", confidence=.8)
        second_question = xsync.continue_session(self.store, started["session_id"])
        self.assertEqual("b:c", second_question["question"]["id"])
        xsync.submit_answer(self.store, started["session_id"], "A", "a", confidence=.8)
        completed = xsync.continue_session(self.store, started["session_id"])
        self.assertEqual("completed", completed["state"]["status"])

        events = xsync.load_event_chain(self.store.session_dir(started["session_id"]))
        keys = [
            event["payload"]["idempotency_key"] for event in events
            if event["event_type"] in {"question_presented", "session_ended"}
        ]
        self.assertEqual(2, len(keys))
        self.assertEqual(2, len(set(keys)))
        self.assertNotEqual(
            xsync.idempotency_key("grading", started["session_id"], "a:b", "c", 1),
            xsync.idempotency_key("grading", started["session_id"], "a", "b:c", 1),
        )

    def test_event_recovery_recomputes_continue_and_grading_keys(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        xsync.submit_answer(self.store, started["session_id"], "A", "past", confidence=.8)
        xsync.continue_session(self.store, started["session_id"])
        directory = self.store.session_dir(started["session_id"])
        transition_path = sorted((directory / "events").glob("*.json"))[-1]
        transition = json.loads(transition_path.read_text())
        future_key = xsync.idempotency_key(
            "continue", started["session_id"], "future", "tech.model", 1
        )
        transition["payload"]["idempotency_key"] = future_key
        transition["state_after"]["consumed_continue_keys"][-1] = future_key
        transition_path.write_text(json.dumps(transition), encoding="utf-8")
        (directory / "state.json").unlink()
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(self.store, started["session_id"])

        learner = xsync.Store(self.repo, "grading-key")
        xsync.init_profile(learner)
        xsync.install_bank(learner, self.bank_path)
        started = xsync.start_session(
            learner, "fixture-bank", "socratic", "terminal", focus="business", count=1
        )
        xsync.submit_answer(learner, started["session_id"], "A", "grade-me", confidence=.8)
        xsync.pending_reviews(learner, started["session_id"])
        directory = learner.session_dir(started["session_id"])
        grading_path = sorted((directory / "events").glob("*.json"))[-1]
        grading = json.loads(grading_path.read_text())
        grading["payload"]["idempotency_key"] = xsync.idempotency_key(
            "grading", started["session_id"], "other", "business.goal", 1
        )
        grading_path.write_text(json.dumps(grading), encoding="utf-8")
        (directory / "state.json").unlink()
        with self.assertRaises(xsync.XSyncError):
            xsync.public_session(learner, started["session_id"])

    def test_markdown_report_escapes_learner_and_reviewer_content(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        xsync.submit_answer(self.store, started["session_id"], "A", "safe-first", confidence=.8)
        xsync.continue_session(self.store, started["session_id"])
        malicious = ("![track](https://example.invalid/pixel)<img src=x>"
                     "\x1b]52;c;Y2xpcGJvYXJk\x07\x00\u202e")
        xsync.submit_answer(
            self.store, started["session_id"], malicious, "unsafe-markdown", confidence=.5
        )
        xsync.pending_reviews(self.store, started["session_id"])
        xsync.apply_review(self.store, {
            "session_id": started["session_id"], "question_id": "tech.model",
            "attempt_id": "unsafe-markdown", "outcome": "mastered",
            "rubric_results": [{"rubric_id": "boundary", "earned": 1,
                                "reason": "Grounded.", "evidence_ids": ["ev.spec"]}],
            "feedback": malicious,
            "evaluation": {"correctness": 1, "reasoning": 1, "evidence_use": 1,
                           "max_hint_level": 0, "unaided": True,
                           "evidence_ids": ["ev.spec"]},
        })
        report = xsync.make_report(self.store, started["session_id"])
        self.assertNotIn(malicious, report)
        self.assertIn(r"\!\[track\]\(https://example\.invalid/pixel\)\<img src=x\>", report)
        for control in ("\x1b", "\x07", "\x00", "\u202e"):
            self.assertNotIn(control, report)
        for visible_escape in ("u001B", "u0007", "u0000", "u202E"):
            self.assertIn(visible_escape, report)

    def test_http_api_requires_token_and_accepts_v1_answer(self):
        started = xsync.start_session(self.store, "fixture-bank", "regular", "web")
        token = "test-token"
        html_path = ROOT / "skills" / "x-sync" / "assets" / "quiz.html"
        self.assertTrue(html_path.is_file())
        handler = type("TestHandler", (xsync.QuizHandler,), {
            "store": self.store, "session_id": started["session_id"],
            "token": token, "html": html_path.read_bytes()})
        server = xsync.http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            try:
                urllib.request.urlopen(base + "/api/v1/state")
                self.fail("missing token should be denied")
            except urllib.error.HTTPError as denied:
                self.assertEqual(401, denied.code)
                denied.close()
            request = urllib.request.Request(base + "/api/v1/state",
                                             headers={"Authorization": "Bearer " + token})
            payload = json.load(urllib.request.urlopen(request))
            self.assertNotIn("state", payload)
            self.assertEqual([{"id": "A", "text": "Alignment"},
                              {"id": "B", "text": "Deployment"}], payload["question"]["choices"])
            lesson_page = urllib.request.urlopen(base + "/lesson")
            self.assertEqual(200, lesson_page.status)
            self.assertNotIn(b"correct_choice", lesson_page.read())
            lesson_page.close()

            request = urllib.request.Request(
                base + "/api/v1/teach", method="POST", data=b"{}",
                headers={"Authorization": "Bearer " + token,
                         "Content-Type": "application/json"})
            with self.assertRaises(urllib.error.HTTPError) as missing_version:
                urllib.request.urlopen(request)
            self.assertEqual(409, missing_version.exception.code)
            missing_version.exception.close()
            unchanged = xsync.public_session(self.store, started["session_id"])
            self.assertEqual(payload["session"]["state_version"],
                             unchanged["session"]["state_version"])

            body = json.dumps({
                "state_version": payload["session"]["state_version"]
            }).encode()
            request = urllib.request.Request(base + "/api/v1/teach", method="POST", data=body,
                headers={"Authorization": "Bearer " + token,
                         "Content-Type": "application/json"})
            taught = json.load(urllib.request.urlopen(request))
            self.assertEqual("lesson", taught["view"])
            self.assertEqual("teaching_open", taught["session"]["status"])

            body = json.dumps({
                "lesson_id": taught["lesson"]["lesson_id"],
                "base_revision": taught["lesson"]["revision"],
                "text": "请补充 evidence 到结论的链路。",
                "feedback_id": "http-feedback",
                "state_version": taught["session"]["state_version"],
            }).encode()
            request = urllib.request.Request(
                base + "/api/v1/lesson/feedback", method="POST", data=body,
                headers={"Authorization": "Bearer " + token,
                         "Content-Type": "application/json"})
            saved_feedback = json.load(urllib.request.urlopen(request))
            self.assertEqual(
                "请补充 evidence 到结论的链路。",
                saved_feedback["lesson"]["feedback_pending"]["text"],
            )

            pending = xsync.pending_reviews(self.store, started["session_id"])["teaching_pending"]
            revised_document = copy.deepcopy(pending["document"])
            revised_document["sections"][1]["paragraphs"].append("HTTP lesson revision.")
            revised = xsync.apply_lesson_revision(self.store, {
                "session_id": started["session_id"],
                "lesson_id": pending["lesson_id"],
                "feedback_id": pending["feedback"]["feedback_id"],
                "base_revision": pending["base_revision"],
                "document": revised_document,
                "author": {"name": "http-test", "version": "1"},
            })
            body = json.dumps({
                "lesson_id": pending["lesson_id"],
                "state_version": revised["session"]["state_version"],
            }).encode()
            request = urllib.request.Request(
                base + "/api/v1/lesson/complete", method="POST", data=body,
                headers={"Authorization": "Bearer " + token,
                         "Content-Type": "application/json"})
            resumed = json.load(urllib.request.urlopen(request))
            self.assertEqual("quiz", resumed["view"])
            body = json.dumps({"answer": "A", "reason": "spec", "confidence": .5,
                               "state_version": resumed["session"]["state_version"],
                               "attempt_id": "web-attempt"}).encode()
            request = urllib.request.Request(base + "/api/v1/answer", method="POST", data=body,
                headers={"Authorization": "Bearer " + token,
                         "Content-Type": "application/json"})
            answered = json.load(urllib.request.urlopen(request))
            self.assertNotIn("state", answered)
            self.assertEqual("reviewed", answered["session"]["status"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def test_cli_documented_flags(self):
        command = [sys.executable, str(SCRIPT), "doctor", "--repo", str(self.repo), "--json"]
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, check=True)
        doctor = json.loads(result.stdout)
        self.assertTrue(doctor["ok"])
        self.assertRegex(doctor["default_learner"], xsync.LEARNER_RE)
        command = [sys.executable, str(SCRIPT), "bank", "validate", "--repo", str(self.repo),
                   "--file", str(self.bank_path), "--json"]
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, check=True)
        self.assertTrue(json.loads(result.stdout)["valid"])
        status = xsync.learner_status(self.store)
        self.assertTrue(status["profile_exists"])
        report_session = xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        xsync.submit_answer(self.store, report_session["session_id"], "A", "report-attempt",
                            confidence=.75)
        report = xsync.build_report_data(self.store, report_session["session_id"])
        self.assertEqual({"business", "architecture_data_flow", "technical_mechanisms",
                          "decisions_bugs", "non_functional"},
                         set(report["human_repository_profile"]))

    def test_first_repository_scan_covers_the_entire_safe_engineering_tree(self):
        (self.repo / ".gitignore").write_text("ignored.txt\n", encoding="utf-8")
        (self.repo / "ignored.txt").write_text("do not inventory\n", encoding="utf-8")
        (self.repo / "node_modules").mkdir()
        (self.repo / "node_modules" / "dep.js").write_text("third party\n", encoding="utf-8")
        (self.repo / ".env.production").write_text("TOKEN=secret\n", encoding="utf-8")
        subprocess.run([
            "git", "-C", str(self.repo), "add", "-f", ".gitignore",
            "node_modules/dep.js", ".env.production",
        ], check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-qm", "scan fixtures"], check=True)

        (self.repo / "notes").mkdir()
        (self.repo / "notes" / "架构.md").write_text("system map\n", encoding="utf-8")
        (self.repo / "secrets.yaml").write_text("api_key: SUPER-SECRET\n", encoding="utf-8")
        (self.repo / "credentials.yml").write_text("password: hidden\n", encoding="utf-8")
        (self.repo / "token.txt").write_text("hidden-token\n", encoding="utf-8")
        (self.repo / "late-binary.dat").write_bytes(b"a" * 9000 + b"\0tail")
        (self.repo / "invalid.dat").write_bytes(b"\xff\xfe")
        (self.repo / "large.txt").write_bytes(
            b"x" * (xsync.MAX_FOCUSED_EVIDENCE_BYTES + 1)
        )
        (self.repo / "link.txt").symlink_to("spec.md")

        opened = []
        original = xsync.scan_relative_file

        def recording_reader(repo, relative):
            opened.append(relative.as_posix())
            return original(repo, relative)

        with mock.patch.object(xsync, "scan_relative_file", side_effect=recording_reader):
            result = xsync.scan_repository(self.store)
        manifest = xsync.validate_repository_scan(
            json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
        )
        by_path = {item["path"]: item for item in manifest["files"]}
        self.assertIn("spec.md", by_path)
        self.assertEqual("tracked", by_path["spec.md"]["source"])
        self.assertEqual("untracked", by_path["notes/架构.md"]["source"])
        for excluded in (
            "ignored.txt", "node_modules/dep.js", ".env.production",
            "secrets.yaml", "credentials.yml", "token.txt", "late-binary.dat",
            "invalid.dat", "large.txt", "link.txt",
        ):
            with self.subTest(excluded=excluded):
                self.assertNotIn(excluded, by_path)
        self.assertNotIn("ignored.txt", opened)
        self.assertNotIn("node_modules/dep.js", opened)
        self.assertNotIn(".env.production", opened)
        self.assertNotIn("secrets.yaml", opened)
        self.assertNotIn("credentials.yml", opened)
        self.assertNotIn("token.txt", opened)
        exclusions = manifest["summary"]["excluded"]
        self.assertGreaterEqual(exclusions["generated_or_vendor"], 1)
        self.assertGreaterEqual(exclusions["sensitive"], 1)
        self.assertGreaterEqual(exclusions["binary_or_non_utf8"], 2)
        self.assertGreaterEqual(exclusions["oversized"], 1)
        self.assertGreaterEqual(exclusions["symlink"], 1)
        self.assertTrue(manifest["complete"])

    def test_repository_scan_is_content_idempotent_and_status_exposes_gate(self):
        first = xsync.scan_repository(self.store)
        scan_path = Path(first["manifest_path"])
        before = (scan_path.read_bytes(), scan_path.stat().st_mtime_ns)
        second = xsync.scan_repository(self.store)
        after = (scan_path.read_bytes(), scan_path.stat().st_mtime_ns)
        self.assertEqual(first["scan_id"], second["scan_id"])
        self.assertTrue(second["reused"])
        self.assertEqual(before, after)
        manifest = json.loads(scan_path.read_text(encoding="utf-8"))
        self.assertEqual(1, next(
            item["lines"] for item in manifest["files"] if item["path"] == "spec.md"
        ))

        status = xsync.learner_status(self.store)["repository_scan"]
        self.assertTrue(status["initial_scan_complete"])
        self.assertFalse(status["required"])
        (self.repo / "new-untracked.md").write_text("new knowledge\n", encoding="utf-8")
        self.assertEqual("stale", xsync.repository_scan_status(self.store)["state"])
        refreshed = xsync.scan_repository(self.store)
        self.assertNotEqual(first["scan_id"], refreshed["scan_id"])

    def test_scan_rejects_same_status_content_change_after_candidate_recheck(self):
        race = self.repo / "race.txt"
        race.write_text("AAAA\n", encoding="utf-8")
        prior = xsync.scan_repository(self.store)
        scan_path = Path(prior["manifest_path"])
        preserved = scan_path.read_bytes()
        original_stat = race.stat()
        original_verify = xsync.verify_scan_candidates

        def mutate_after_recheck(repo, observations):
            original_verify(repo, observations)
            race.write_text("BBBB\n", encoding="utf-8")
            os.utime(race, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

        with mock.patch.object(xsync, "verify_scan_candidates", side_effect=mutate_after_recheck):
            with self.assertRaisesRegex(xsync.XSyncError, "扫描期间发生变化"):
                xsync.scan_repository(self.store)
        self.assertEqual(preserved, scan_path.read_bytes())

    def test_scan_freshness_probe_failure_does_not_hide_active_session(self):
        started = xsync.start_session(
            self.store, "fixture-bank", "regular", "terminal", count=1,
            focus="business",
        )
        with mock.patch.object(
            xsync, "repository_state_token", side_effect=xsync.XSyncError("probe unavailable")
        ):
            status = xsync.learner_status(self.store)
        self.assertEqual("unknown", status["repository_scan"]["state"])
        self.assertFalse(status["repository_scan"]["required"])
        self.assertEqual(started["session_id"], status["active_session"]["session_id"])

    def test_missing_scan_blocks_bank_use_until_explicit_full_scan(self):
        scan_path = xsync.repository_scan_directory(self.store) / "scan.json"
        scan_path.unlink()
        status = xsync.repository_scan_status(self.store)
        self.assertTrue(status["required"])
        with self.assertRaisesRegex(xsync.XSyncError, "首次运行必须先扫描"):
            xsync.start_session(self.store, "fixture-bank", "regular", "terminal")
        xsync.scan_repository(self.store)
        started = xsync.start_session(
            self.store, "fixture-bank", "regular", "terminal", count=1,
            focus="business",
        )
        self.assertEqual("question_open", started["session"]["status"])

    def test_scan_cli_and_non_git_failure_are_explicit(self):
        command = [sys.executable, str(SCRIPT), "scan", "--repo", str(self.repo), "--json"]
        completed = subprocess.run(
            command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["complete"])
        self.assertTrue(Path(payload["manifest_path"]).is_file())

        with tempfile.TemporaryDirectory() as directory:
            plain = Path(directory)
            (plain / "source.py").write_text("print('hello')\n", encoding="utf-8")
            plain_store = xsync.Store(plain)
            with self.assertRaisesRegex(xsync.XSyncError, "需要 Git 仓库"):
                xsync.scan_repository(plain_store)
            self.assertFalse((plain / ".x-sync" / "repositories").exists())

    def test_scan_rejects_sparse_and_supports_sha256_git_object_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            sparse = Path(directory)
            subprocess.run(["git", "init", "-q", str(sparse)], check=True)
            subprocess.run(["git", "-C", str(sparse), "config", "core.sparseCheckout", "true"], check=True)
            with self.assertRaisesRegex(xsync.XSyncError, "不支持 sparse checkout"):
                xsync.scan_repository(xsync.Store(sparse))

        with tempfile.TemporaryDirectory() as directory:
            sha_repo = Path(directory)
            initialized = subprocess.run(
                ["git", "init", "-q", "--object-format=sha256", str(sha_repo)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            if initialized.returncode:
                self.skipTest("installed Git does not support SHA-256 repositories")
            subprocess.run(["git", "-C", str(sha_repo), "config", "user.email", "test@example.com"], check=True)
            subprocess.run(["git", "-C", str(sha_repo), "config", "user.name", "Test"], check=True)
            (sha_repo / "source.py").write_text("print('ok')\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(sha_repo), "add", "source.py"], check=True)
            subprocess.run(["git", "-C", str(sha_repo), "commit", "-qm", "initial"], check=True)
            result = xsync.scan_repository(xsync.Store(sha_repo))
            self.assertEqual(64, len(result["baseline_commit"]))

    def test_bare_start_defaults_and_explicit_overrides(self):
        defaults = xsync.parser().parse_args([
            "start", "--repo", str(self.repo), "--learner", "alice",
            "--bank", "fixture-bank",
        ])
        self.assertEqual("socratic", defaults.style)
        self.assertEqual("web", defaults.channel)
        self.assertEqual("mixed", defaults.focus)
        self.assertEqual(5, defaults.count)
        self.assertIsNone(defaults.max_depth)

        explicit = xsync.parser().parse_args([
            "start", "--repo", str(self.repo), "--learner", "alice",
            "--bank", "fixture-bank", "--style", "regular", "--channel", "terminal",
            "--focus", "technical", "--count", "1", "--max-depth", "3",
        ])
        self.assertEqual("regular", explicit.style)
        self.assertEqual("terminal", explicit.channel)
        self.assertEqual("technical", explicit.focus)
        self.assertEqual(1, explicit.count)
        self.assertEqual(3, explicit.max_depth)

        with mock.patch.object(xsync.getpass, "getuser", return_value="jin xiao"):
            self.assertEqual("jin-xiao", xsync.default_learner())
        with mock.patch.object(xsync.getpass, "getuser", return_value="肖劲"):
            self.assertRegex(xsync.default_learner(), r"user-[0-9a-f]{12}")

        status = xsync.learner_status(self.store)
        self.assertEqual(["fixture-bank"], status["bank_ids"])
        with self.assertRaisesRegex(xsync.XSyncError, "少于请求的 3 道"):
            xsync.start_session(
                self.store, "fixture-bank", "socratic", "web", count=3,
                focus="mixed"
            )

        terminal = xsync.start_session(
            self.store, "fixture-bank", "regular", "terminal", count=1,
            focus="business"
        )
        with self.assertRaisesRegex(xsync.XSyncError, "配置为 terminal"):
            xsync.serve(self.store, terminal["session_id"], "127.0.0.1", 0, False)

    def test_finite_mixed_session_balances_domains_and_prerequisites(self):
        commit = xsync.git(self.repo, "rev-parse", "HEAD")
        bank = fixture_bank(commit, self.repository_id)
        bank["bank_id"] = "mixed-five"
        source_business = bank["questions"][0]
        business = []
        for index in range(5):
            question = json.loads(json.dumps(source_business))
            question["id"] = f"business.{index}"
            question["prompt"] = f"Business question {index}?"
            business.append(question)
        technical = json.loads(json.dumps(bank["questions"][1]))
        technical["id"] = "technical.boundary"
        technical["prerequisite_question_ids"] = ["business.4"]
        bank["questions"] = [*business, technical]
        path = self.repo / "mixed-five.json"
        path.write_text(json.dumps(bank), encoding="utf-8")
        xsync.install_bank(self.store, path)

        started = xsync.start_session(
            self.store, "mixed-five", "socratic", "web", count=5, focus="mixed"
        )
        snapshot = json.loads(
            (self.store.session_dir(started["session_id"]) / "bank.json").read_text()
        )
        self.assertEqual(5, len(snapshot["questions"]))
        self.assertEqual(
            {"business", "technical"},
            {question["domain"] for question in snapshot["questions"]},
        )
        selected_ids = []
        for question in snapshot["questions"]:
            self.assertLessEqual(
                set(question["prerequisite_question_ids"]), set(selected_ids)
            )
            selected_ids.append(question["id"])

        # Prefer a feasible short prerequisite path when an earlier target's
        # chain cannot fit into the finite mixed session.
        def q(question_id, domain, prerequisites=()):
            return {"id": question_id, "domain": domain,
                    "prerequisite_question_ids": list(prerequisites)}

        competing_paths = [
            q("b0", "business"),
            q("p1", "business"),
            q("p2", "business", ["p1"]),
            q("p3", "business", ["p2"]),
            q("p4", "business", ["p3"]),
            q("p5", "business", ["p4"]),
            q("t-long", "technical", ["p5"]),
            q("b-short", "business"),
            q("t-short", "technical", ["b-short"]),
        ]
        selected = xsync.select_session_questions(competing_paths, 5, "mixed")
        selected_ids = [question["id"] for question in selected]
        self.assertIn("t-short", selected_ids)
        self.assertNotIn("t-long", selected_ids)
        self.assertEqual(5, len(selected_ids))
        emitted = set()
        for question in selected:
            self.assertLessEqual(set(question["prerequisite_question_ids"]), emitted)
            emitted.add(question["id"])


if __name__ == "__main__":
    unittest.main()
