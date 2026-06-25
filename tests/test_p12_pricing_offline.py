"""P12 Part 1.4 — pricing is cache-first and never blocks startup offline.

Startup (``DACLI.__init__`` -> ``fetch_pricing``) must not hang on a network
call: a fresh cache is served without touching the network, and when the network
is unreachable the call degrades to ``None`` (cost reported as unknown, tokens
still tracked) rather than raising or waiting on a long timeout.
"""

import json
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from dacli.ai import pricing


def _write_cache(cache_dir: str, payload: dict, fetched_at: float) -> None:
    Path(cache_dir, "models_cache.json").write_text(
        json.dumps({"fetched_at": fetched_at, "payload": payload}), encoding="utf-8"
    )


class CacheFirstTest(unittest.TestCase):
    def test_fresh_cache_is_served_without_network(self):
        with TemporaryDirectory() as tmp:
            _write_cache(tmp, {"openai": {"models": {}}}, fetched_at=time.time())
            with mock.patch("httpx.get", side_effect=AssertionError("network hit")) as g:
                payload = pricing.fetch_api_json(cache_dir=tmp)
        self.assertEqual(payload, {"openai": {"models": {}}})
        g.assert_not_called()


class OfflineFallbackTest(unittest.TestCase):
    def test_offline_no_cache_returns_none_without_raising(self):
        with TemporaryDirectory() as tmp, \
                mock.patch("httpx.get", side_effect=OSError("offline")):
            # This is the exact call startup makes (agent.py).
            result = pricing.fetch_pricing("openai", "gpt-4o", cache_dir=tmp)
        self.assertIsNone(result)

    def test_offline_falls_back_to_stale_cache(self):
        with TemporaryDirectory() as tmp:
            stale = time.time() - (pricing.CACHE_TTL_SECONDS + 100)
            _write_cache(tmp, {"openai": {"models": {"gpt-4o": {"cost": {"input": 1}}}}}, stale)
            with mock.patch("httpx.get", side_effect=OSError("offline")):
                payload = pricing.fetch_api_json(cache_dir=tmp)
        # Stale-but-present cache is better than nothing when offline.
        self.assertEqual(payload, {"openai": {"models": {"gpt-4o": {"cost": {"input": 1}}}}})


class ShortTimeoutTest(unittest.TestCase):
    def test_network_fetch_uses_a_short_timeout(self):
        captured = {}

        def fake_get(url, **kwargs):
            captured["timeout"] = kwargs.get("timeout")
            raise OSError("offline")  # short-circuit; we only inspect the timeout

        with TemporaryDirectory() as tmp, \
                mock.patch("httpx.get", side_effect=fake_get):
            pricing.fetch_api_json(cache_dir=tmp)

        self.assertIsNotNone(captured.get("timeout"))
        self.assertLessEqual(captured["timeout"], 5.0)


if __name__ == "__main__":
    unittest.main()
