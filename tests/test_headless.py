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
