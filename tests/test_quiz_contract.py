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
            r"const activePrompt = question\.probe\s*\? `追问：\$\{question\.probe\}`"
            r"\s*:\s*\(question\.display_prompt \|\| question\.prompt\);",
        )
        self.assertIn('text($("prompt"), activePrompt);', self.html)
        self.assertNotIn("innerHTML", self.html)

    def test_confidence_scale_is_normalized_from_five_steps(self):
        ratings = re.findall(r'name="confidence" value="([1-5])"', self.html)
        self.assertEqual(["1", "2", "3", "4", "5"], ratings)
        self.assertIn("confidence: (confidenceLevel - 1) / 4", self.html)

    def test_uses_only_v1_quiz_api_paths(self):
        paths = re.findall(r'fetch\("(/api/[^"?]+)', self.html)
        self.assertEqual(["/api/v1/state", "/api/v1/answer"], paths)

    def test_single_choice_includes_unknown(self):
        self.assertIn('unknownInput.value = "unknown";', self.html)
        self.assertIn('text(unknownCopy, "不知道 / 当前证据不足");', self.html)

    def test_browser_never_references_answer_key_fields(self):
        for forbidden in ("answer_key", "correct_choice", "reference_answer", "rubric"):
            with self.subTest(field=forbidden):
                self.assertNotIn(forbidden, self.html)


if __name__ == "__main__":
    unittest.main()
