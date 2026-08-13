from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "x-sync" / "SKILL.md"
OPENAI = ROOT / "skills" / "x-sync" / "agents" / "openai.yaml"
README = ROOT / "README.md"


class DefaultInvocationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.skill = SKILL.read_text(encoding="utf-8")
        cls.openai = OPENAI.read_text(encoding="utf-8")
        cls.readme = README.read_text(encoding="utf-8")

    def test_bare_invocation_has_requested_preset(self):
        for setting in (
            "`style=socratic`", "`channel=web`", "`focus=mixed`", "`count=5`"
        ):
            with self.subTest(setting=setting):
                self.assertIn(setting, self.skill)
        self.assertIn("Do not ask a setup question", self.skill)
        self.assertIn("both `business` and `technical` domains", self.skill)

    def test_unfinished_session_is_resumed_and_html_is_opened(self):
        self.assertIn("Resume an unfinished active session", self.skill)
        self.assertIn("Never silently abandon an unfinished session", self.skill)
        self.assertIn("--session <id> --port 0 --open", self.skill)
        self.assertIn("keep the yielded process running", self.skill)
        self.assertIn("if it is `terminal`", self.skill)
        self.assertIn("do not call `serve`", self.skill)

    def test_codex_prompt_and_user_docs_match_bare_invocation(self):
        self.assertIn("$x-sync", self.openai)
        for phrase in ("interactive HTML", "Socratic", "mixed business and technical",
                       "5 questions"):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.openai)
        self.assertIn("$x-sync\n```", self.readme)
        self.assertIn("/x-sync\n```", self.readme)
        self.assertIn("苏格拉底模式", self.readme)
        self.assertIn("5 道题", self.readme)


if __name__ == "__main__":
    unittest.main()
