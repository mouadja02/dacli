"""Atomic state-write helpers (P03).

Covers the round-trip, the crash-safety guarantee (a failed write must leave the
*prior* file intact), and the no-temp-litter invariant.
"""

import json
import unittest

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from dacli.core.atomicio import write_json_atomic, write_text_atomic


def _tmp_files(d: Path):
    return [p for p in d.iterdir() if p.suffix == ".tmp"]


class WriteTextAtomicTest(unittest.TestCase):
    def test_round_trip_text(self):
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "state.txt"
            write_text_atomic(target, "hello ünïcode")
            self.assertEqual(target.read_text(encoding="utf-8"), "hello ünïcode")
            self.assertEqual(_tmp_files(Path(tmp)), [])

    def test_creates_missing_parent_dirs(self):
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "a" / "b" / "state.txt"
            write_text_atomic(target, "deep")
            self.assertEqual(target.read_text(encoding="utf-8"), "deep")

    def test_overwrite_replaces_content(self):
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "state.txt"
            write_text_atomic(target, "v1")
            write_text_atomic(target, "v2")
            self.assertEqual(target.read_text(encoding="utf-8"), "v2")
            self.assertEqual(_tmp_files(Path(tmp)), [])

    def test_no_tmp_litter_after_success(self):
        with TemporaryDirectory() as tmp:
            for i in range(5):
                write_text_atomic(Path(tmp) / "s.txt", str(i))
            self.assertEqual(_tmp_files(Path(tmp)), [])


class WriteJsonAtomicTest(unittest.TestCase):
    def test_round_trip_json_with_kwargs(self):
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "state.json"
            obj = {"b": 2, "a": 1, "nested": [1, 2, 3]}
            write_json_atomic(target, obj, indent=2, sort_keys=True)
            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), obj)
            # kwargs are honoured (sorted keys -> "a" before "b").
            self.assertLess(
                target.read_text().index('"a"'), target.read_text().index('"b"')
            )


class CrashSimulationTest(unittest.TestCase):
    def test_failed_write_leaves_prior_file_intact(self):
        """If the write fails mid-flight, the original file must survive untouched
        and no temp litter may remain."""
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "dacli.json"
            write_json_atomic(target, {"secrets": "precious"})
            original = target.read_text(encoding="utf-8")

            # Simulate a crash between truncate-equivalent and durable replace.
            with mock.patch("dacli.core.atomicio.os.fsync", side_effect=OSError("boom")), \
                    self.assertRaises(OSError):
                write_json_atomic(target, {"secrets": "OVERWRITTEN"})

            # The prior file is byte-for-byte intact (the dangerous failure mode).
            self.assertEqual(target.read_text(encoding="utf-8"), original)
            # And no half-written temp file was left behind.
            self.assertEqual(_tmp_files(Path(tmp)), [])

    def test_failed_first_write_creates_no_target(self):
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "new.json"
            with mock.patch("dacli.core.atomicio.os.fsync", side_effect=OSError("boom")), \
                    self.assertRaises(OSError):
                write_text_atomic(target, "partial")
            self.assertFalse(target.exists())
            self.assertEqual(_tmp_files(Path(tmp)), [])


if __name__ == "__main__":
    unittest.main()
