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
import time
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
        connector_config={
            "github": {"token": "x"},
            "snowflake": {"account": "a", "user": "u", "password": "p",
                          "warehouse": "w", "role": "r", "database": "d"},
            "pinecone": {"api_key": "k", "index_name": "i", "environment": "e",
                         "embedding_provider": "openai", "embedding_api_key": "k",
                         "embedding_model": "m"},
        },
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
    from dacli.core.host import DacliHost
    return DacliHost(settings=_settings(), llm=llm or _OkLLM())


class LazyPricingTest(unittest.TestCase):
    """Acceptance: constructing DACLI makes no pricing fetch (no network)."""

    def test_construction_does_not_fetch_pricing(self):
        def _boom(*args, **kwargs):
            raise AssertionError("fetch_pricing called during __init__")

        with mock.patch("dacli.core.host.fetch_pricing", side_effect=_boom):
            _agent()  # must not raise

    def test_get_pricing_is_memoized(self):
        sentinel = object()
        agent = _agent()
        with mock.patch(
            "dacli.core.host.fetch_pricing", return_value=sentinel
        ) as fetch:
            self.assertIs(agent._get_pricing(), sentinel)
            self.assertIs(agent._get_pricing(), sentinel)
        self.assertEqual(fetch.call_count, 1)

    def test_get_pricing_failure_yields_none_and_is_not_retried(self):
        agent = _agent()
        with mock.patch(
            "dacli.core.host.fetch_pricing", side_effect=RuntimeError("offline")
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


class TokenCounterHotPathTest(unittest.TestCase):
    """The tiktoken encoder is built once per counter, never per count()."""

    def test_encoder_built_once_not_per_count(self):
        import tiktoken
        from dacli.context.tokenizer import TiktokenCounter

        calls = {"n": 0}
        real = tiktoken.get_encoding

        def counting(name):
            calls["n"] += 1
            return real(name)

        with mock.patch("tiktoken.get_encoding", side_effect=counting):
            c = TiktokenCounter(model=None, provider=None)
            for _ in range(50):
                c.count("some text to tokenize repeatedly")
        self.assertEqual(calls["n"], 1)

    def test_token_pass_stays_under_budget(self):
        from dacli.context.tokenizer import make_counter

        counter = make_counter(None)
        msgs = [{"role": "user", "content": "lorem ipsum " * 50} for _ in range(20)]
        start = time.perf_counter()
        for _ in range(100):
            counter.count_messages(msgs)
        # Generous: a per-count encoder rebuild would blow well past this.
        self.assertLess(time.perf_counter() - start, 5.0)


class ToolbarCostTest(unittest.TestCase):
    """The bottom toolbar reads session cost in O(1), without the heavy summary."""

    def _store(self):
        from dacli.core.store import DacliStore

        d = tempfile.mkdtemp(prefix="dacli_toolbar_")
        _TEMP_DIRS.append(d)
        return DacliStore(base_dir=d)

    def test_session_cost_skips_usage_summary(self):
        from dacli.ai.pricing import TokenUsage

        store = self._store()
        store.record_usage(
            "sess-1", "gpt-x", TokenUsage(input=1000, output=500), cost=0.25
        )
        self.assertAlmostEqual(store.session_cost_usd("sess-1"), 0.25, places=6)
        # The toolbar path must not deep-copy every model/session bucket per keystroke.
        with mock.patch.object(
            store, "usage_summary", side_effect=AssertionError("summary in toolbar path")
        ):
            self.assertAlmostEqual(store.session_cost_usd("sess-1"), 0.25, places=6)

    def test_session_cost_zero_for_unknown_session(self):
        self.assertEqual(self._store().session_cost_usd("nope"), 0.0)


if __name__ == "__main__":
    unittest.main()
