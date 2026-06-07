"""Sandbox run-dir retention sweep (P01 / 02.3).

Each sandbox execution leaves a ``run_*`` workspace on disk. The sweep keeps the
most recent ``MAX_SANDBOX_RUNS`` and prunes the rest, so artifacts can't grow
unbounded.
"""

import os
import unittest

from pathlib import Path
from tempfile import TemporaryDirectory

from dacli.sandbox.runtime import MAX_SANDBOX_RUNS, _sweep_run_dirs


class SandboxRetentionTest(unittest.TestCase):
    def _make_runs(self, root: Path, n: int) -> list[Path]:
        """Create *n* ``run_*`` dirs with strictly increasing mtimes (oldest first)."""
        dirs = []
        for i in range(n):
            d = root / f"run_{i:03d}"
            d.mkdir()
            (d / "script.py").write_text("# fake", encoding="utf-8")
            os.utime(d, (1_000_000 + i, 1_000_000 + i))  # ascending mtime
            dirs.append(d)
        return dirs

    def test_sweep_keeps_only_newest_n(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._make_runs(root, 25)

            _sweep_run_dirs(root)

            survivors = sorted(p.name for p in root.iterdir() if p.is_dir())
            self.assertEqual(len(survivors), MAX_SANDBOX_RUNS)
            # The newest 20 (run_005 .. run_024) survive; the oldest 5 are gone.
            expected = sorted(f"run_{i:03d}" for i in range(25 - MAX_SANDBOX_RUNS, 25))
            self.assertEqual(survivors, expected)

    def test_sweep_noop_when_under_limit(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._make_runs(root, 5)

            _sweep_run_dirs(root)

            self.assertEqual(len([p for p in root.iterdir() if p.is_dir()]), 5)

    def test_sweep_ignores_non_run_dirs(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._make_runs(root, 25)
            keep_me = root / "results"  # not a run_* dir
            keep_me.mkdir()

            _sweep_run_dirs(root)

            self.assertTrue(keep_me.exists())
            runs = [p for p in root.iterdir() if p.is_dir() and p.name.startswith("run_")]
            self.assertEqual(len(runs), MAX_SANDBOX_RUNS)

    def test_sweep_missing_workdir_is_noop(self):
        with TemporaryDirectory() as tmp:
            missing = Path(tmp) / "does-not-exist"
            _sweep_run_dirs(missing)  # must not raise


if __name__ == "__main__":
    unittest.main()
