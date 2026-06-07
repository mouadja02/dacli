"""P12 Part 2.1 — single tz-aware timestamp helper for all ledgers/audit.

`core/timeutils.now_iso()` is the one source of audit timestamps. Every
append-only ledger (routing, model-routing, self-correction, governance audit,
usage store) must emit tz-aware UTC ISO-8601 so trails are comparable; no naive
`datetime.now()` may remain in ledger code.
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


class LedgerTimestampsAreAwareTest(unittest.TestCase):
    """No ledger/audit record may carry a naive timestamp."""

    def _assert_aware(self, iso_string: str):
        parsed = datetime.fromisoformat(iso_string)
        self.assertIsNotNone(
            parsed.tzinfo, f"timestamp {iso_string!r} is naive (no tzinfo)"
        )

    def test_routing_decision_timestamp_is_aware(self):
        from dacli.core.router import RoutingDecision

        d = RoutingDecision(task="t", tier="tool", target=None,
                            confidence=0.9, rationale="r")
        self._assert_aware(d.timestamp)

    def test_model_choice_timestamp_is_aware(self):
        from dacli.reasoning.model_router import ModelChoice

        c = ModelChoice(kind="k", tier="t", model="m", stakes="s", rationale="r")
        self._assert_aware(c.timestamp)

    def test_correction_audit_record_timestamp_is_aware(self):
        import tempfile
        from pathlib import Path

        from dacli.core.loop import CorrectionAuditLog

        with tempfile.TemporaryDirectory() as tmp:
            log = CorrectionAuditLog(path=str(Path(tmp) / "corrections.jsonl"))
            log.log({"event": "corrected"})
            rec = log.recent(1)[0]
            self._assert_aware(rec["ts"])


if __name__ == "__main__":
    unittest.main()
