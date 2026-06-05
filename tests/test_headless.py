import unittest
from unittest import mock

from config.settings import load_config


class _Sentinel:
    """A stand-in object accepted anywhere the LLM client is expected."""
    async def initialize(self):
        return None


class LLMInjectionTest(unittest.TestCase):
    def setUp(self):
        # Keep construction offline + fast.
        self._pricing_patch = mock.patch("core.agent.fetch_pricing", return_value=None)
        self._pricing_patch.start()
        self.addCleanup(self._pricing_patch.stop)

    def test_injected_llm_is_used(self):
        from core.agent import DACLI
        settings = load_config()
        try:
            settings.sandbox.enabled = False
        except Exception:
            pass
        sentinel = _Sentinel()
        agent = DACLI(settings=settings, llm=sentinel)
        self.assertIs(agent.llm, sentinel)

    def test_default_llm_constructed_when_none(self):
        from core.agent import DACLI
        from reasoning.llm import LLMClient
        settings = load_config()
        try:
            settings.sandbox.enabled = False
        except Exception:
            pass
        agent = DACLI(settings=settings)
        self.assertIsInstance(agent.llm, LLMClient)


if __name__ == "__main__":
    unittest.main()


import asyncio


def _run(coro):
    return asyncio.run(coro)


class ScriptedLLMTest(unittest.TestCase):
    def test_returns_text_tool_calls_and_usage(self):
        from reasoning.scripted import ScriptedLLM
        llm = ScriptedLLM([
            {"text": "hi", "tool_calls": [{"name": "update_plan", "arguments": {"todos": []}}],
             "usage": {"input": 10, "output": 2}},
            {"text": "done"},
        ])
        content, tool_calls = _run(llm.generate(messages=[], tools=[], system_prompt=""))
        self.assertEqual(content, "hi")
        self.assertEqual(len(tool_calls), 1)
        self.assertEqual(tool_calls[0]["name"], "update_plan")
        self.assertIn("id", tool_calls[0])
        self.assertEqual(tool_calls[0]["arguments"], {"todos": []})
        self.assertEqual(llm.last_usage, {"input": 10, "output": 2})

        content2, tc2 = _run(llm.generate(messages=[]))
        self.assertEqual(content2, "done")
        self.assertEqual(tc2, [])

    def test_assigns_unique_ids(self):
        from reasoning.scripted import ScriptedLLM
        llm = ScriptedLLM([
            {"tool_calls": [{"name": "a", "arguments": {}}, {"name": "b", "arguments": {}}]},
        ])
        _content, tcs = _run(llm.generate(messages=[]))
        ids = [tc["id"] for tc in tcs]
        self.assertEqual(len(ids), len(set(ids)))

    def test_raises_when_exhausted(self):
        from reasoning.scripted import ScriptedLLM, ScriptExhausted
        llm = ScriptedLLM([{"text": "only one"}])
        _run(llm.generate(messages=[]))
        with self.assertRaises(ScriptExhausted):
            _run(llm.generate(messages=[]))

    def test_accepts_model_kwarg(self):
        from reasoning.scripted import ScriptedLLM
        llm = ScriptedLLM([{"text": "x"}])
        content, _tc = _run(llm.generate(messages=[], model="some-model"))
        self.assertEqual(content, "x")
