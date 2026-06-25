"""P12 Part 2.1 — single tz-aware timestamp helper for all ledgers/audit.

`core/timeutils.now_iso()` is the one source of audit timestamps. Every
append-only ledger (governance audit, usage store) must emit tz-aware UTC
ISO-8601 so trails are comparable; no naive `datetime.now()` in ledger code.
"""

import unittest
from datetime import datetime, timezone


class NowIsoTest(unittest.TestCase):
    def test_returns_tz_aware_utc_iso8601(self):
        from dacli.core.timeutils import now_iso

        s = now_iso()
        parsed = datetime.fromisoformat(s)
        # Must carry timezone info and be UTC (offset zero).
        self.assertIsNotNone(parsed.tzinfo)
        self.assertEqual(parsed.utcoffset(), timezone.utc.utcoffset(None))

    def test_is_a_string(self):
        from dacli.core.timeutils import now_iso

        self.assertIsInstance(now_iso(), str)


if __name__ == "__main__":
    unittest.main()
