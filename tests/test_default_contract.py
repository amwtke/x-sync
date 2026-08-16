import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "x-sync" / "SKILL.md"
OPENAI = ROOT / "skills" / "x-sync" / "agents" / "openai.yaml"
README = ROOT / "README.md"
V2_REFERENCE = ROOT / "skills" / "x-sync" / "references" / "dialogue-v2.md"


class DefaultInvocationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.skill = SKILL.read_text(encoding="utf-8")
        cls.openai = OPENAI.read_text(encoding="utf-8")
        cls.readme = README.read_text(encoding="utf-8")
        cls.v2_reference = V2_REFERENCE.read_text(encoding="utf-8")

    def test_bare_invocation_is_adaptive_dialogue_v2(self) -> None:
        for phrase in (
            "A bare invocation always means Dialogue v2",
            "one visible question at a time",
            "natural topic completion",
            "Do not start or resume a v1 question-bank session",
            "An unfinished legacy session never overrides a bare v2 invocation",
        ):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.skill)
        self.assertNotIn("`count=5`", self.skill)
        self.assertNotIn("5 questions", self.openai)
        self.assertNotIn("question_count", self.v2_reference)

    def test_v2_launch_and_host_loop_are_operational(self) -> None:
        self.assertIn("references/dialogue-v2.md", self.skill)
        self.assertIn("dialogue runtime serve", self.v2_reference)
        self.assertIn("dialogue host supervise", self.v2_reference)
        self.assertIn("dialogue host submit", self.v2_reference)
        self.assertIn("browser_url", self.v2_reference)
        self.assertIn("There is no numeric finish condition", self.v2_reference)

    def test_legacy_quiz_is_explicit_only(self) -> None:
        self.assertIn("## Explicit legacy quiz mode", self.skill)
        self.assertIn("scripts/xsync.py status", self.skill)
        self.assertIn("scripts/xsync.py start", self.skill)
        self.assertIn("legacy 模式", self.readme)
        self.assertIn("它不是默认入口", self.readme)

    def test_safe_repository_evidence_boundary_is_preserved(self) -> None:
        for exclusion in (
            "`.git/`",
            "`.x-sync/`",
            "credential/token/key stores",
            "generated dependency trees",
            "binaries",
            "oversized files",
            "symlinks",
            "submodules",
            "paths outside the repository",
        ):
            with self.subTest(exclusion=exclusion):
                self.assertIn(exclusion, self.skill)
        self.assertIn("evidence is stale, disputed, unavailable", self.skill)

    def test_codex_prompt_and_readme_match_v2_default(self) -> None:
        self.assertIn("$x-sync", self.openai)
        self.assertIn("default Dialogue v2", self.openai)
        self.assertIn("no fixed question count", self.openai)
        self.assertIn("默认 Dialogue v2", self.readme)
        self.assertIn("没有固定题数", self.readme)
        self.assertIn("旧 v1 测验不会劫持裸调用", self.readme)

    def test_explicit_target_project_short_flag_is_end_to_end(self) -> None:
        self.assertIn("`-d <target-project>`", self.skill)
        self.assertIn("retain that target for follow-ups", self.skill)
        self.assertIn("-d <target-project>", self.openai)
        self.assertIn("$x-sync -d /path/to/target-project", self.readme)
        self.assertIn("/x-sync -d ../target-project", self.readme)
        self.assertIn("/x-sync:x-sync -d ../target-project", self.readme)
        self.assertIn("问题只针对该目标项目", self.readme)


if __name__ == "__main__":
    unittest.main()
