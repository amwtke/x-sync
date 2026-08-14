import re
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
QUIZ = ROOT / "skills" / "x-sync" / "assets" / "quiz.html"


class QuizContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = QUIZ.read_text(encoding="utf-8")

    def test_socratic_probe_replaces_original_prompt_safely(self):
        self.assertRegex(
            self.html,
            r'text\(\$\("prompt"\), question\.probe\s*\? `追问：\$\{question\.probe\}`'
            r"\s*:\s*\(question\.display_prompt \|\| question\.prompt\)\);",
        )
        self.assertNotIn("innerHTML", self.html)

    def test_confidence_scale_is_normalized_from_five_steps(self):
        ratings = re.findall(r'name="confidence" value="([1-5])"', self.html)
        self.assertEqual(["1", "2", "3", "4", "5"], ratings)
        self.assertIn("confidence: (confidenceLevel - 1) / 4", self.html)

    def test_uses_only_v1_quiz_and_lesson_api_paths(self):
        paths = re.findall(r'fetch\("(/api/[^"?]+)', self.html)
        self.assertEqual(
            {
                "/api/v1/state",
                "/api/v1/answer",
                "/api/v1/teach",
                "/api/v1/lesson/feedback",
                "/api/v1/lesson/complete",
            },
            set(paths),
        )
        self.assertTrue(all(path.startswith("/api/v1/") for path in paths))

    def test_unknown_is_an_explicit_interrupt_not_an_answer_choice(self):
        self.assertIn('id="unknown-answer"', self.html)
        self.assertIn("我不知道，告诉我", self.html)
        self.assertIn('fetch("/api/v1/teach"', self.html)
        self.assertNotIn('saveAnswer("unknown"', self.html)
        self.assertRegex(
            self.html,
            r"storageRemove\(questionDraftKey\);\s*"
            r'\$\("answer-form"\)\.reset\(\);\s*questionKey = "";',
        )

    def test_every_lesson_entry_discards_the_pre_teaching_answer_draft(self):
        self.assertRegex(
            self.html,
            r"function renderLesson\(payload\) \{\s*"
            r"clearAnswerForTeaching\(payload\);",
        )
        self.assertIn("draftScope(payload, activeQuestionKey(payload, question), \"answer\")", self.html)

    def test_dedicated_h4_lesson_collects_feedback_before_retry(self):
        for marker in (
            'id="lesson-page"', 'id="lesson-article"', "H4", "测试已中断",
            'id="lesson-feedback"', 'id="save-lesson-feedback"',
            'id="lesson-understood"', "我已经懂了", "回到终端说“继续”",
        ):
            with self.subTest(marker=marker):
                self.assertIn(marker, self.html)
        self.assertNotIn('id="teaching-panel"', self.html)
        self.assertIn('payload.view === "lesson" ? "/lesson" : "/"', self.html)

    def test_browser_never_references_answer_key_fields(self):
        for forbidden in ("answer_key", "correct_choice", "reference_answer", "rubric"):
            with self.subTest(field=forbidden):
                self.assertNotIn(forbidden, self.html)


if __name__ == "__main__":
    unittest.main()
