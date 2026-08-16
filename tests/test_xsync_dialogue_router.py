from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import importlib.util
import unittest
from unittest import mock

import tests.xsync_v2_path  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "skills" / "x-sync" / "scripts" / "xsync.py"


def load_runtime():
    spec = importlib.util.spec_from_file_location("xsync_dialogue_router_test", RUNTIME)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DialogueRouterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runtime = load_runtime()

    def test_runtime_and_host_are_thin_exact_argument_routes(self) -> None:
        with mock.patch("xsync_v2.runtime_cli.main", return_value=17) as runtime:
            status = self.runtime.main(
                ["dialogue", "runtime", "serve", "--stream-json"]
            )
        self.assertEqual(17, status)
        runtime.assert_called_once_with(["serve", "--stream-json"])

        with mock.patch("xsync_v2.host_cli.main", return_value=19) as host:
            status = self.runtime.main(
                ["dialogue", "host", "supervise", "--stream-json"]
            )
        self.assertEqual(19, status)
        host.assert_called_once_with(["supervise", "--stream-json"])

    def test_help_and_unknown_adapter_do_not_enter_the_v1_store(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(0, self.runtime.main(["dialogue", "--help"]))
        self.assertEqual("usage: xsync dialogue {runtime,host} ...\n", output.getvalue())

        errors = StringIO()
        with redirect_stderr(errors):
            self.assertEqual(2, self.runtime.main(["dialogue", "unknown"]))
        self.assertEqual(
            "x-sync: unknown dialogue command: unknown\n",
            errors.getvalue(),
        )

    def test_existing_v1_parser_contract_is_unchanged(self) -> None:
        arguments = self.runtime.parser().parse_args(
            ["status", "--repo", ".", "--learner", "learner-1", "--json"]
        )
        self.assertEqual("status", arguments.command)
        self.assertEqual("learner-1", arguments.learner)
        self.assertTrue(arguments.json)


if __name__ == "__main__":
    unittest.main()
