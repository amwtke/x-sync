from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALLER = REPO_ROOT / "skills" / "x-sync" / "scripts" / "install.py"
SKILL_SOURCE = REPO_ROOT / "skills" / "x-sync"
MARKER = ".x-sync-install.json"
PLUGIN_MANIFEST = REPO_ROOT / ".claude-plugin" / "plugin.json"
MARKETPLACE_MANIFEST = REPO_ROOT / ".claude-plugin" / "marketplace.json"


def load_installer():
    spec = importlib.util.spec_from_file_location("x_sync_install", INSTALLER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.module = load_installer()
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.home = self.root / "home"
        self.home.mkdir()
        patcher = mock.patch.object(Path, "home", classmethod(lambda cls: self.home))
        patcher.start()
        self.addCleanup(patcher.stop)

    def invoke(self, arguments):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = self.module.main(arguments)
        return status, stdout.getvalue(), stderr.getvalue()

    def test_user_install_for_all_hosts(self):
        status, _, _ = self.invoke(["--host", "all", "--scope", "user"])
        self.assertEqual(status, 0)

        locations = {
            "codex": Path(".agents/skills/x-sync"),
            "claude": Path(".claude/skills/x-sync"),
        }
        host_markers = {}
        for host, relative in locations.items():
            with self.subTest(host=host):
                target = self.home / relative
                self.assertEqual(
                    (target / "SKILL.md").read_bytes(),
                    (SKILL_SOURCE / "SKILL.md").read_bytes(),
                )
                self.assertTrue((target / "scripts" / "install.py").is_file())
                self.assertEqual(
                    (target / "scripts" / "xsync_v2" / "domain.py").read_bytes(),
                    (
                        SKILL_SOURCE / "scripts" / "xsync_v2" / "domain.py"
                    ).read_bytes(),
                )
                marker = json.loads((target / MARKER).read_text(encoding="utf-8"))
                self.assertEqual(marker["host"], host)
                self.assertEqual(marker["scope"], "user")
                self.assertEqual(marker["managed_by"], "x-sync-installer")
                self.assertIn(
                    "scripts/xsync_v2/domain.py",
                    marker["content"]["files"],
                )
                self.assertFalse(
                    any(
                        part
                        in {
                            "__pycache__",
                            ".mypy_cache",
                            ".pytest_cache",
                            ".ruff_cache",
                        }
                        or part.startswith(".coverage")
                        for path in marker["content"]["files"]
                        for part in Path(path).parts
                    )
                )
                host_markers[host] = marker
        self.assertEqual(
            host_markers["codex"]["content"],
            host_markers["claude"]["content"],
        )

    def test_project_install_uses_requested_root(self):
        cases = (
            ("codex", Path(".agents/skills/x-sync")),
            ("claude", Path(".claude/skills/x-sync")),
        )
        for host, relative in cases:
            with self.subTest(host=host):
                project = self.root / f"project-{host}"
                status, _, _ = self.invoke(
                    ["--host", host, "--scope", "project", "--project", str(project)]
                )
                self.assertEqual(status, 0)
                marker = json.loads((project / relative / MARKER).read_text(encoding="utf-8"))
                self.assertEqual(marker["host"], host)
                self.assertEqual(marker["scope"], "project")

    def test_dry_run_writes_nothing(self):
        status, stdout, _ = self.invoke(["--host", "all", "--dry-run"])
        self.assertEqual(status, 0)
        self.assertFalse((self.home / ".agents").exists())
        self.assertFalse((self.home / ".claude").exists())
        self.assertIn("would install codex", stdout)

    def test_installer_ignores_runtime_cache_files(self):
        ignored = self.module.ignore_source_entries(
            str(SKILL_SOURCE),
            [
                "SKILL.md",
                "__pycache__",
                ".mypy_cache",
                ".pytest_cache",
                ".ruff_cache",
                ".coverage",
                ".coverage.worker-1",
                "module.pyc",
                ".DS_Store",
            ],
        )
        self.assertEqual(
            {
                "__pycache__",
                ".mypy_cache",
                ".pytest_cache",
                ".ruff_cache",
                ".coverage",
                ".coverage.worker-1",
                "module.pyc",
                ".DS_Store",
            },
            ignored,
        )

    def test_unmanaged_directory_is_never_overwritten(self):
        target = self.home / ".agents" / "skills" / "x-sync"
        target.mkdir(parents=True)
        sentinel = target / "keep.txt"
        sentinel.write_text("mine", encoding="utf-8")

        status, _, stderr = self.invoke(["--host", "codex"])

        self.assertEqual(status, 1)
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "mine")
        self.assertIn("unmanaged", stderr)

    def test_managed_directory_can_be_updated(self):
        self.assertEqual(self.invoke(["--host", "claude"])[0], 0)
        target = self.home / ".claude" / "skills" / "x-sync"
        stale = target / "stale.txt"
        stale.write_text("old", encoding="utf-8")

        self.assertEqual(self.invoke(["--host", "claude"])[0], 0)

        self.assertFalse(stale.exists())
        self.assertTrue((target / "SKILL.md").is_file())

    def test_managed_install_updates_when_canonical_content_changes(self):
        source = self.root / "canonical"
        shutil.copytree(SKILL_SOURCE, source)
        self.module.SOURCE_DIR = source
        self.assertEqual(self.invoke(["--host", "codex"])[0], 0)
        target = self.home / ".agents" / "skills" / "x-sync"
        original = (target / "SKILL.md").read_text(encoding="utf-8")
        (source / "SKILL.md").write_text(original + "\nupdated\n", encoding="utf-8")

        self.assertEqual(self.invoke(["--host", "codex"])[0], 0)

        self.assertEqual(
            (target / "SKILL.md").read_text(encoding="utf-8"), original + "\nupdated\n"
        )

    def test_modified_managed_content_is_not_overwritten(self):
        self.assertEqual(self.invoke(["--host", "codex"])[0], 0)
        target = self.home / ".agents" / "skills" / "x-sync"
        skill = target / "SKILL.md"
        skill.write_text("personal changes\n", encoding="utf-8")

        status, _, stderr = self.invoke(["--host", "codex"])

        self.assertEqual(status, 1)
        self.assertEqual(skill.read_text(encoding="utf-8"), "personal changes\n")
        self.assertIn("unmanaged", stderr)

    def test_forged_marker_without_installed_files_is_rejected(self):
        target = self.home / ".agents" / "skills" / "x-sync"
        target.mkdir(parents=True)
        (target / MARKER).write_text(
            json.dumps(self.module.marker_payload("codex", "user")), encoding="utf-8"
        )

        status, _, stderr = self.invoke(["--host", "codex"])

        self.assertEqual(status, 1)
        self.assertTrue((target / MARKER).is_file())
        self.assertIn("unmanaged", stderr)

    def test_marker_for_a_different_host_does_not_authorize_overwrite(self):
        target = self.home / ".agents" / "skills" / "x-sync"
        target.mkdir(parents=True)
        marker = self.module.marker_payload("claude", "user")
        (target / MARKER).write_text(json.dumps(marker), encoding="utf-8")

        status, _, stderr = self.invoke(["--host", "codex"])

        self.assertEqual(status, 1)
        self.assertIn("unmanaged", stderr)

    def test_all_preflights_before_writing(self):
        claude = self.home / ".claude" / "skills" / "x-sync"
        claude.mkdir(parents=True)
        (claude / "personal.txt").write_text("keep", encoding="utf-8")

        self.assertEqual(self.invoke(["--host", "all"])[0], 1)

        self.assertFalse((self.home / ".agents").exists())

    def test_project_parent_symlink_cannot_escape_project(self):
        project = self.root / "project"
        outside = self.root / "outside"
        project.mkdir()
        outside.mkdir()
        (project / ".agents").symlink_to(outside, target_is_directory=True)

        status, _, stderr = self.invoke(
            ["--host", "codex", "--scope", "project", "--project", str(project)]
        )

        self.assertEqual(status, 1)
        self.assertFalse((outside / "skills").exists())
        self.assertIn("symlink parent", stderr)

    def test_user_parent_symlink_cannot_escape_home(self):
        outside = self.root / "outside-user"
        outside.mkdir()
        (self.home / ".agents").symlink_to(outside, target_is_directory=True)

        status, _, stderr = self.invoke(["--host", "codex", "--scope", "user"])

        self.assertEqual(status, 1)
        self.assertFalse((outside / "skills").exists())
        self.assertIn("symlink parent", stderr)

    def test_symlink_project_root_is_rejected(self):
        real_project = self.root / "real-project"
        project_link = self.root / "project-link"
        real_project.mkdir()
        project_link.symlink_to(real_project, target_is_directory=True)

        status, _, stderr = self.invoke(
            [
                "--host",
                "claude",
                "--scope",
                "project",
                "--project",
                str(project_link),
            ]
        )

        self.assertEqual(status, 1)
        self.assertFalse((real_project / ".claude").exists())
        self.assertIn("symlink parent", stderr)

    def test_rejects_project_inside_canonical_source(self):
        status, _, stderr = self.invoke(
            ["--host", "codex", "--scope", "project", "--project", str(SKILL_SOURCE)]
        )
        self.assertEqual(status, 1)
        self.assertIn("inside the source", stderr)

    def test_cli_rejects_project_with_user_scope(self):
        result = subprocess.run(
            [
                sys.executable,
                str(INSTALLER),
                "--scope",
                "user",
                "--project",
                str(self.root),
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--project can only be used", result.stderr)

    def test_claude_plugin_discovers_repository_skills(self):
        manifest = json.loads(PLUGIN_MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(manifest["name"], "x-sync")
        self.assertEqual(manifest["skills"], "./skills/")
        self.assertEqual(manifest["repository"], "https://github.com/amwtke/x-sync")
        self.assertNotIn("license", manifest)

    def test_claude_marketplace_exposes_repository_plugin(self):
        marketplace = json.loads(MARKETPLACE_MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(marketplace["name"], "x-sync")
        plugin = marketplace["plugins"][0]
        self.assertEqual(plugin["name"], "x-sync")
        self.assertEqual(plugin["source"], "./")
        self.assertEqual(plugin["repository"], "https://github.com/amwtke/x-sync")


if __name__ == "__main__":
    unittest.main()
