"""P12 (D-7/D-8): the docs drift gate and the first-run nudge in `--help`.

The README's numbers (tests badge, eval sample) rotted once — D-7 adds
``tools/check_docs.py`` so CI fails when they drift again. D-8 ends
`dacli --help` with a pointer at the setup wizard.
"""

import importlib.util
import unittest
from pathlib import Path

from click.testing import CliRunner

from dacli.scripts.cli import cli

REPO = Path(__file__).resolve().parent.parent


def _load_check_docs():
    spec = importlib.util.spec_from_file_location(
        "check_docs", REPO / "tools" / "check_docs.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HelpEpilogTest(unittest.TestCase):
    def test_help_ends_with_first_time_nudge(self):
        result = CliRunner().invoke(cli, ["--help"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("First time?", result.output)
        self.assertIn("setup wizard", result.output)


class DocsDriftTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.check_docs = _load_check_docs()
        cls.readme = (REPO / "README.md").read_text(encoding="utf-8")

    def test_tests_badge_matches_collected_count(self):
        badge = self.check_docs.badge_test_count(self.readme)
        collected = self.check_docs.collected_test_count()
        self.assertEqual(
            badge,
            collected,
            f"README tests badge says {badge} but pytest collects {collected}",
        )

    def test_eval_sample_matches_golden_suite_size(self):
        sample = self.check_docs.eval_sample_task_count(self.readme)
        suite = self.check_docs.golden_suite_task_count()
        self.assertEqual(
            sample,
            suite,
            f"README eval sample shows {sample} tasks but the golden suite has {suite}",
        )

    def test_every_command_is_documented(self):
        self.assertEqual(self.check_docs.missing_commands(self.readme), [])


if __name__ == "__main__":
    unittest.main()
