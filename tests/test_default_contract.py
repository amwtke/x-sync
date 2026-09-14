from __future__ import annotations

import importlib.util
from pathlib import Path
import re
import unittest
from unittest import mock

import tests.xsync_v2_path  # noqa: F401
from xsync_chat.cli import parser

ROOT = Path(__file__).resolve().parents[1]
SKILL_DIR = ROOT / "skills" / "x-sync"


class DefaultInvocationContractTest(unittest.TestCase):
    def test_open_chat_command_routes_to_automatic_runtime(self):
        spec = importlib.util.spec_from_file_location(
            "xsync_default_test", SKILL_DIR / "scripts" / "xsync.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        arguments = [
            "serve",
            "--repo",
            "/repo with spaces",
            "--host",
            "claude",
            "--stream-json",
        ]
        with mock.patch("xsync_chat.cli.main", return_value=0) as runtime:
            self.assertEqual(0, module.main(["chat", *arguments]))
        runtime.assert_called_once_with(arguments)

    def test_open_chat_target_and_host_are_explicit_and_no_quiz_count_exists(self):
        options = parser().parse_args(
            ["serve", "-d", "../target project", "--host", "claude"]
        )
        self.assertEqual("../target project", options.repo)
        self.assertEqual("claude", options.host)
        self.assertFalse(hasattr(options, "count"))
        self.assertFalse(hasattr(options, "bank"))

    def test_skill_metadata_defaults_to_free_form_chat(self):
        metadata = (SKILL_DIR / "agents" / "openai.yaml").read_text()
        self.assertIn("$x-sync", metadata)
        self.assertIn("free-form repository chat", metadata)
        self.assertIn("automatic Codex replies", metadata)
        self.assertNotIn("default Dialogue v2", metadata)
        self.assertIn("allow_implicit_invocation: true", metadata)

    def test_mode_specific_references_are_present_and_discoverable(self):
        text = (SKILL_DIR / "SKILL.md").read_text()
        for filename in (
            "open-chat.md",
            "dialogue-v2.md",
            "evaluation.md",
            "bank-schema.md",
        ):
            self.assertIn("references/" + filename, text)
        for target in re.findall(r"\]\((references/[^)]+)\)", text):
            self.assertTrue((SKILL_DIR / target).is_file(), target)


if __name__ == "__main__":
    unittest.main()
