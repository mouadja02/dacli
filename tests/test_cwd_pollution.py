"""P02: a globally-installed dacli must not drop .dacli/ into the cwd.

Info-only commands (--version, --help) touch no filesystem; outside a project a
writing command falls back to the global state dir (not cwd) and says so once.
Pure unittest driving the Click group through CliRunner.
"""

import logging
import os
import tempfile
import unittest
from pathlib import Path

from click.testing import CliRunner

import dacli.core.logging_setup as logging_setup
from dacli.core import paths
from dacli.scripts.cli import cli

_ENV_KEYS = ("DACLI_HOME", "DACLI_STATE_PATH", "DACLI_DEBUG")


class CwdPollutionTest(unittest.TestCase):
    def setUp(self):
        # A scratch global dir so a fallback writes here, never the real ~/.config.
        self.home = tempfile.mkdtemp(prefix="dacli_home_")
        self._saved = {k: os.environ.get(k) for k in _ENV_KEYS}
        os.environ.pop("DACLI_STATE_PATH", None)
        os.environ.pop("DACLI_DEBUG", None)
        os.environ["DACLI_HOME"] = self.home
        # Drop any handler a prior test left pinned to some other dir, and force
        # the next setup_logging() to re-resolve base_dir for the current cwd.
        dacli_logger = logging.getLogger("dacli")
        for h in list(dacli_logger.handlers):
            dacli_logger.removeHandler(h)
        logging_setup._configured = False

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _assert_no_state(self):
        self.assertNotIn(".dacli", os.listdir("."), "cwd should stay clean")
        self.assertFalse(
            (Path(self.home) / "dacli.log").exists(),
            "a clean run must not write the global log either",
        )

    def test_version_creates_no_state(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["--version"])
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn("DACLI version", result.output)
            self._assert_no_state()

    def test_help_creates_no_state(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["--help"])
            self.assertEqual(result.exit_code, 0, result.output)
            self._assert_no_state()

    def test_project_dir_resolves_local_state(self):
        with tempfile.TemporaryDirectory(prefix="dacli_proj_") as d:
            (Path(d) / ".git").mkdir()
            prev = os.getcwd()
            os.chdir(d)
            try:
                root = paths.project_root()
                self.assertEqual(root, Path(d).resolve())
                self.assertEqual(paths.state_dir(), Path(d).resolve() / ".dacli")
            finally:
                os.chdir(prev)


if __name__ == "__main__":
    unittest.main()
