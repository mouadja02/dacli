"""P12 Part 1.1 — orjson on the hot spill paths, with a json fallback.

The large-result serializers in ``context/spill.py`` and
``context/sources/terminal.py`` serialize with orjson when available (faster on
big tables) and fall back to stdlib ``json`` otherwise. orjson emits *bytes*, so
the spill write goes through a bytes-aware atomic writer and stays crash-safe.
"""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock


def _tmp_files(d: Path):
    return [p for p in d.iterdir() if p.suffix == ".tmp"]


class FastJsonTest(unittest.TestCase):
    def test_dumps_round_trips_to_string(self):
        from dacli.core.fastjson import dumps

        obj = {"b": 2, "a": 1, "rows": [{"x": 1}, {"x": 2}]}
        s = dumps(obj)
        self.assertIsInstance(s, str)
        self.assertEqual(json.loads(s), obj)

    def test_dumps_bytes_returns_bytes(self):
        from dacli.core.fastjson import dumps_bytes

        obj = {"rows": [1, 2, 3]}
        b = dumps_bytes(obj)
        self.assertIsInstance(b, bytes)
        self.assertEqual(json.loads(b), obj)

    def test_default_callable_handles_unserializable(self):
        from dacli.core.fastjson import dumps

        # A Decimal is not natively JSON-serializable; default=str must rescue it
        # in both the orjson and the json path.
        from decimal import Decimal

        s = dumps({"price": Decimal("1.5")}, default=str)
        self.assertEqual(json.loads(s), {"price": "1.5"})

    def test_orjson_is_used_when_available(self):
        # orjson is in the environment for this repo; the flag must reflect that.
        import dacli.core.fastjson as fj

        self.assertTrue(fj.HAVE_ORJSON)


class WriteBytesAtomicTest(unittest.TestCase):
    def test_round_trip(self):
        from dacli.core.atomicio import write_bytes_atomic

        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "blob.json"
            write_bytes_atomic(target, b'{"ok": true}')
            self.assertEqual(target.read_bytes(), b'{"ok": true}')
            self.assertEqual(_tmp_files(Path(tmp)), [])

    def test_failed_write_leaves_prior_file_intact(self):
        from dacli.core.atomicio import write_bytes_atomic

        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "blob.json"
            write_bytes_atomic(target, b"original")
            with mock.patch("dacli.core.atomicio.os.fsync", side_effect=OSError("boom")), \
                    self.assertRaises(OSError):
                write_bytes_atomic(target, b"OVERWRITTEN")
            self.assertEqual(target.read_bytes(), b"original")
            self.assertEqual(_tmp_files(Path(tmp)), [])


class SpillRoundTripTest(unittest.TestCase):
    def test_result_store_round_trips_large_table(self):
        from dacli.context.spill import ResultStore

        with TemporaryDirectory() as tmp:
            store = ResultStore(root=tmp, session_id="s1")
            data = [{"id": i, "val": f"row{i}"} for i in range(2000)]
            handle = store.write("query_snowflake", data)
            got = store.read(handle, start=0, count=5)
            self.assertEqual(got["total_rows"], 2000)
            self.assertEqual(got["data"], data[:5])

    def test_spilled_at_is_tz_aware(self):
        from datetime import datetime as _dt

        from dacli.context.spill import ResultStore

        with TemporaryDirectory() as tmp:
            store = ResultStore(root=tmp, session_id="s2")
            handle = store.write("q", [{"a": 1}])
            payload = json.loads((Path(tmp) / "s2" / f"{handle}.json").read_text())
            parsed = _dt.fromisoformat(payload["spilled_at"])
            self.assertIsNotNone(parsed.tzinfo)

    def test_scrollback_store_round_trips(self):
        from dacli.context.sources.terminal import ScrollbackStore

        with TemporaryDirectory() as tmp:
            store = ScrollbackStore(root=tmp, session_id="s3")
            big = "\n".join(f"line {i}" for i in range(5000))
            cid = store.write({"command_id": "cmd_x", "command": "ls", "output": big, "exit_code": 0})
            got = store.read(cid, start=0, count=3)
            self.assertEqual(got["total_lines"], 5000)
            self.assertEqual(got["output"], "line 0\nline 1\nline 2")


if __name__ == "__main__":
    unittest.main()
