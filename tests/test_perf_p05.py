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


class _RendezvousConnector:
    """Connector whose connect() only completes if its peer is connecting
    concurrently: each side signals its own event and awaits the other's.
    Under a serial loop the first one times out."""

    def __init__(self, name, own, peer):
        self.name = name
        self._own = own
        self._peer = peer

    async def connect(self):
        self._own.set()
        await asyncio.wait_for(self._peer.wait(), timeout=2.0)


class _OkConnector:
    name = "fine"

    async def connect(self):
        return None


class _BoomConnector:
    name = "boom"

    async def connect(self):
        raise RuntimeError("connector down")


class ParallelInitializeTest(unittest.TestCase):
    """Acceptance: initialize() connects concurrently and still attributes
    per-connector success/failure."""

    def test_connectors_connect_concurrently(self):
        agent = _agent()

        async def run():
            a_evt, b_evt = asyncio.Event(), asyncio.Event()
            pair = [
                _RendezvousConnector("alpha", a_evt, b_evt),
                _RendezvousConnector("beta", b_evt, a_evt),
            ]
            with mock.patch.object(agent.registry, "enabled_connectors",
                                   return_value=pair), \
                 mock.patch.object(agent.registry, "get_catalog", return_value={}):
                return await agent.initialize()

        statuses = []
        agent._on_status_update = statuses.append
        self.assertTrue(asyncio.run(run()))
        joined = "\n".join(statuses)
        self.assertIn("Connecting to alpha ...", joined)
        self.assertIn("Connecting to beta ...", joined)
        self.assertNotIn("Failed to initialize alpha", joined)
        self.assertNotIn("Failed to initialize beta", joined)

    def test_mixed_results_keep_per_connector_attribution(self):
        agent = _agent()
        statuses = []
        agent._on_status_update = statuses.append

        async def run():
            with mock.patch.object(agent.registry, "enabled_connectors",
                                   return_value=[_OkConnector(), _BoomConnector()]), \
                 mock.patch.object(agent.registry, "get_catalog", return_value={}):
                return await agent.initialize()

        self.assertTrue(asyncio.run(run()))  # LLM ok → True despite connector failure
        joined = "\n".join(statuses)
        self.assertIn("Connecting to fine ...", joined)
        self.assertIn("Failed to initialize boom: connector down", joined)
        # The final summary attributes success and failure to the right names.
        self.assertIn("Active tools: LLM, fine", joined)
        self.assertIn("Failed to initialize: boom", joined)


if __name__ == "__main__":
    unittest.main()
