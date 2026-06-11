"""P05 — startup latency fixes.

* Fix A: pricing resolution is lazy. Constructing ``DACLI`` must not call
  ``fetch_pricing`` (a cold cache means a blocking network hit); the first
  reader goes through ``_get_pricing()``, which memoizes a single fetch and
  swallows failures (offline ⇒ cost unknown, tokens still tracked).
* Fix B: ``initialize()`` connects enabled connectors concurrently while
  preserving per-connector success/failure attribution.

Offline + deterministic. Run with:
    python -m unittest tests.test_perf_p05
"""

import asyncio
import contextlib
import os
import shutil
import tempfile
import unittest
from unittest import mock

_TEMP_DIRS = []


def tearDownModule():
    for d in _TEMP_DIRS:
        shutil.rmtree(d, ignore_errors=True)


def _settings():
    """Hermetic settings with placeholder creds + isolated state (mirrors
    tests/test_initialize_and_sessions_p04._settings)."""
    from dacli.config.settings import Settings

    settings = Settings(
        llm={"provider": "scripted", "model": "scripted",
             "api_key": "scripted", "base_url": "https://api.test.local"},
        github={"token": "x"},
        snowflake={"account": "a", "user": "u", "password": "p",
                   "warehouse": "w", "role": "r", "database": "d"},
        pinecone={"api_key": "k", "index_name": "i", "environment": "e"},
        embeddings={"provider": "openai", "api_key": "k", "model": "m"},
    )
    root = tempfile.mkdtemp(prefix="dacli_p05_test_")
    _TEMP_DIRS.append(root)
    settings.agent.state_path = os.path.join(root, "state.json")
    settings.agent.history_path = os.path.join(root, "history.json")
    with contextlib.suppress(Exception):
        settings.sandbox.enabled = False
    with contextlib.suppress(Exception):
        settings.terminal.enabled = False
    return settings


class _OkLLM:
    async def initialize(self):
        return None


def _agent(llm=None):
    from dacli.core.agent import DACLI
    return DACLI(settings=_settings(), llm=llm or _OkLLM())


class LazyPricingTest(unittest.TestCase):
    """Acceptance: constructing DACLI makes no pricing fetch (no network)."""

    def test_construction_does_not_fetch_pricing(self):
        def _boom(*args, **kwargs):
            raise AssertionError("fetch_pricing called during __init__")

        with mock.patch("dacli.core.agent.fetch_pricing", side_effect=_boom):
            _agent()  # must not raise

    def test_get_pricing_is_memoized(self):
        sentinel = object()
        agent = _agent()
        with mock.patch(
            "dacli.core.agent.fetch_pricing", return_value=sentinel
        ) as fetch:
            self.assertIs(agent._get_pricing(), sentinel)
            self.assertIs(agent._get_pricing(), sentinel)
        self.assertEqual(fetch.call_count, 1)

    def test_get_pricing_failure_yields_none_and_is_not_retried(self):
        agent = _agent()
        with mock.patch(
            "dacli.core.agent.fetch_pricing", side_effect=RuntimeError("offline")
        ) as fetch:
            self.assertIsNone(agent._get_pricing())
            self.assertIsNone(agent._get_pricing())
        self.assertEqual(fetch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
