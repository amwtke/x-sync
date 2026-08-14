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

    def test_first_use_scans_the_safe_whole_project_before_bank_selection(self):
        resume = self.skill.index("Resume an unfinished active session")
        scan = self.skill.index("Before preparing any new session")
        reuse = self.skill.index("Reuse an installed bank only after the first-scan gate")
        self.assertLess(resume, scan)
        self.assertLess(scan, reuse)
        self.assertIn("scripts/xsync.py scan --repo <repo> --json", self.skill)
        self.assertIn("initial_scan_complete", self.skill)
        self.assertIn("A fresh installed bank never bypasses this first scan", self.skill)
        self.assertIn("Every eligible Git-tracked or non-ignored untracked regular file", self.skill)
        for exclusion in (
            "`.git/`", "`.x-sync/`", "known secret/credential/token stores",
            "vendored/generated dependency trees", "binaries", "oversized files",
            "symlinks", "submodules", "paths outside the repository",
        ):
            with self.subTest(exclusion=exclusion):
                self.assertIn(exclusion, self.skill)
        self.assertIn("Never delay or alter unfinished-session recovery", self.skill)
        self.assertIn("Later invocations do not repeat the full scan merely because they are bare", self.skill)

    def test_readme_explains_first_scan_scope_and_first_only_behavior(self):
        for phrase in (
            "扫描整个安全工程目录",
            "然后才会判断能否复用题库或需要生成新题库",
            "Git 已跟踪和未忽略的未跟踪普通文件",
            "后续调用不会仅因启动 X-Sync 就重复全量扫描",
            "非 Git 目录、unborn repository 或 sparse checkout 会明确停止",
        ):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.readme)

    def test_codex_prompt_and_user_docs_match_bare_invocation(self):
        self.assertIn("$x-sync", self.openai)
        for phrase in ("interactive HTML", "Socratic", "mixed business and technical",
                       "5 questions"):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.openai)
        self.assertIn("resume any unfinished session first", self.openai)
        self.assertIn("before the first new session, scan the safe whole project", self.openai)
        self.assertLess(
            self.openai.index("resume any unfinished session first"),
            self.openai.index("before the first new session"),
        )
        self.assertIn("$x-sync\n```", self.readme)
        self.assertIn("/x-sync\n```", self.readme)
        self.assertIn("苏格拉底模式", self.readme)
        self.assertIn("5 道题", self.readme)

    def test_explicit_target_project_short_flag_is_end_to_end(self):
        self.assertIn("`-d <target-project>`", self.skill)
        self.assertIn("short alias of `--repo TARGET_PROJECT`", self.skill)
        self.assertIn("Propagate the canonical target to every runtime command", self.skill)
        self.assertIn("-d <target-project>", self.openai)
        self.assertIn("$x-sync -d /path/to/target-project", self.readme)
        self.assertIn("/x-sync -d ../target-project", self.readme)
        self.assertIn("/x-sync:x-sync -d ../target-project", self.readme)
        self.assertIn("问题只针对该目标项目", self.readme)
        self.assertIn("retain it for `继续`", self.skill)
        self.assertIn("后续“继续”也沿用它", self.readme)


if __name__ == "__main__":
    unittest.main()
