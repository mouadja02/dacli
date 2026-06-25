"""Offline tests for the bounded retry/backoff wrapper around LLM calls (P05).

Fully offline: no real network and no real sleeps. The retry helper is driven
with a fake call ``fn`` and injected retryable exception types, and
``asyncio.sleep`` is patched so backoff is asserted without wall-clock waits.
"""

import asyncio
import types
import unittest
from unittest import mock

from dacli.ai.llm import LLMClient


def _run(coro):
    return asyncio.run(coro)


class _Retryable(Exception):
    # Stand-in for a provider transient error (429/503/dropped connection).
    pass


class _Permanent(Exception):
    # Stand-in for an auth / 4xx-validation error that must fail fast.
    pass


def _client(attempts=4, base=0.5):
    # Build an LLMClient without importing any provider SDK: __init__ only reads
    # settings.llm.provider, and _with_retry only reads retry_attempts /
    # retry_base_delay. A namespace stub keeps the test offline and instant.
    settings = types.SimpleNamespace(
        llm=types.SimpleNamespace(
            provider="openai",
            retry_attempts=attempts,
            retry_base_delay=base,
        )
    )
    return LLMClient(settings)


class WithRetryTest(unittest.TestCase):
    def setUp(self):
        # Patch the module's asyncio.sleep + random so tests are instant and the
        # backoff is deterministic (jitter -> 0).
        self._slept = []

        async def _fake_sleep(delay):
            self._slept.append(delay)

        self._sleep_patch = mock.patch(
            "dacli.ai.llm.asyncio.sleep", side_effect=_fake_sleep
        )
        self._rand_patch = mock.patch("dacli.ai.llm.random.random", return_value=0.0)
        self._sleep_patch.start()
        self._rand_patch.start()

    def tearDown(self):
        self._sleep_patch.stop()
        self._rand_patch.stop()

    def test_succeeds_after_transient_failures(self):
        # Fails twice then succeeds -> returns the success, called exactly 3x.
        calls = {"n": 0}

        async def fn():
            calls["n"] += 1
            if calls["n"] < 3:
                raise _Retryable("transient")
            return "ok"

        result = _run(_client()._with_retry(fn, retryable=(_Retryable,)))

        self.assertEqual(result, "ok")
        self.assertEqual(calls["n"], 3)

    def test_raises_after_exhausting_attempts(self):
        # Always fails retryable -> raises after exactly ``attempts`` calls.
        calls = {"n": 0}

        async def fn():
            calls["n"] += 1
            raise _Retryable("always")

        with self.assertRaises(_Retryable):
            _run(_client(attempts=4)._with_retry(fn, retryable=(_Retryable,)))

        self.assertEqual(calls["n"], 4)

    def test_non_retryable_fails_fast(self):
        # A permanent error is not retried -> raises immediately, called once.
        calls = {"n": 0}

        async def fn():
            calls["n"] += 1
            raise _Permanent("auth")

        with self.assertRaises(_Permanent):
            _run(_client(attempts=4)._with_retry(fn, retryable=(_Retryable,)))

        self.assertEqual(calls["n"], 1)

    def test_on_retry_called_with_increasing_attempts(self):
        # on_retry fires once per retry (not on the final failure) with
        # increasing attempt numbers.
        seen = []

        async def fn():
            raise _Retryable("nope")

        def on_retry(attempt, delay, error):
            seen.append(attempt)

        with self.assertRaises(_Retryable):
            _run(
                _client(attempts=4)._with_retry(
                    fn, retryable=(_Retryable,), on_retry=on_retry
                )
            )

        self.assertEqual(seen, [1, 2, 3])

    def test_backoff_grows_and_uses_no_real_sleep(self):
        # Delays grow exponentially (base * 2**i, jitter pinned to 0) and the
        # patched sleep records them instead of waiting.
        async def fn():
            raise _Retryable("nope")

        with self.assertRaises(_Retryable):
            _run(_client(attempts=4, base=0.5)._with_retry(fn, retryable=(_Retryable,)))

        self.assertEqual(self._slept, [0.5, 1.0, 2.0])
        self.assertTrue(all(b > a for a, b in zip(self._slept, self._slept[1:], strict=False)))

    def test_attempts_override_beats_settings(self):
        # An explicit attempts= overrides settings.llm.retry_attempts.
        calls = {"n": 0}

        async def fn():
            calls["n"] += 1
            raise _Retryable("x")

        with self.assertRaises(_Retryable):
            _run(_client(attempts=9)._with_retry(fn, attempts=2, retryable=(_Retryable,)))

        self.assertEqual(calls["n"], 2)


class RetryConfigTest(unittest.TestCase):
    def test_llm_settings_expose_retry_defaults(self):
        from dacli.config.settings import LLMSettings

        s = LLMSettings(
            provider="openai", model="m", api_key="k", base_url="https://x"
        )
        self.assertEqual(s.retry_attempts, 4)
        self.assertEqual(s.retry_base_delay, 0.5)
        # An overall per-call timeout is also config-driven.
        self.assertEqual(s.timeout, 120)


if __name__ == "__main__":
    unittest.main()
