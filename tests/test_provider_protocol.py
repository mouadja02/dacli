"""A-2 — the Provider protocol behind the LLMClient facade.

Capability is a **declared property** of each provider class
(``supports_tools``), checked at configure time by ``LLMClient.initialize()``
— never discovered as a turn-time error. The facade's public surface
(``generate`` / ``classify`` / ``last_usage``) is provider-agnostic and
delegates the request mechanics to the selected provider; the retry/backoff
helper stays shared in the facade (see tests/test_llm_retry.py).

Fully offline: no provider SDK is touched.

    python -m unittest tests.test_provider_protocol
"""

import asyncio
import types
import unittest
from unittest import mock

from dacli.ai import providers
from dacli.ai.llm import LLMClient
from dacli.ai.providers import (
    AnthropicProvider,
    GoogleProvider,
    OpenAIProvider,
    OpenRouterProvider,
    Provider,
    create_provider,
)


def _run(coro):
    return asyncio.run(coro)


def _settings(provider="openai"):
    return types.SimpleNamespace(llm=types.SimpleNamespace(provider=provider, model="m"))


class DeclaredCapabilityTest(unittest.TestCase):
    def test_tool_capable_providers_declare_it(self):
        for cls in (OpenAIProvider, AnthropicProvider, OpenRouterProvider):
            self.assertTrue(cls.supports_tools, cls.__name__)

    def test_google_declares_no_tool_support(self):
        self.assertFalse(GoogleProvider.supports_tools)

    def test_openrouter_is_openai_compatible(self):
        self.assertTrue(issubclass(OpenRouterProvider, OpenAIProvider))

    def test_registry_covers_the_supported_provider_names(self):
        self.assertEqual(
            set(providers.PROVIDERS), {"openai", "anthropic", "google", "openrouter"}
        )


class FactoryTest(unittest.TestCase):
    def test_create_provider_returns_the_registered_class(self):
        for name, cls in providers.PROVIDERS.items():
            built = create_provider(name, _settings(name), retry=None)
            self.assertIsInstance(built, cls)

    def test_unknown_provider_is_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            create_provider("nope", _settings("nope"), retry=None)
        self.assertIn("Unsupported LLM provider", str(ctx.exception))


class ConfigureTimeToolCheckTest(unittest.TestCase):
    """A provider with ``supports_tools = False`` is rejected at configure time."""

    def test_no_tools_provider_rejected_at_initialize(self):
        class _NoTools(Provider):
            name = "mock"
            supports_tools = False

            async def initialize(self):  # pragma: no cover - must not be reached
                raise AssertionError("initialize() must not run for a no-tools provider")

            async def generate(self, *a, **k):  # pragma: no cover
                raise AssertionError("unreachable")

            def normalize_usage(self, raw):  # pragma: no cover
                return {}

        client = LLMClient(_settings("mock"))
        with mock.patch.dict(providers.PROVIDERS, {"mock": _NoTools}), \
                self.assertRaises(ValueError) as ctx:
            _run(client.initialize())

        message = str(ctx.exception)
        self.assertIn("'mock'", message)
        self.assertIn("tool use", message)
        # The alternatives offered are exactly the declared tool-capable ones.
        for alternative in ("openai", "anthropic", "openrouter"):
            self.assertIn(alternative, message)
        self.assertNotIn("'google'", message)


class FacadeDelegationTest(unittest.TestCase):
    """generate() delegates to the active provider and mirrors last_usage."""

    def test_generate_delegates_and_copies_usage(self):
        seen = {}

        class _Stub(Provider):
            name = "stub"

            async def initialize(self):
                self.client = object()

            async def generate(self, messages, tools=None, system_prompt=None,
                               on_text=None, model=None, on_retry=None):
                seen.update(messages=messages, tools=tools, model=model)
                self.last_usage = {"input": 3, "output": 7}
                return "hello", [{"id": "1", "name": "t", "arguments": {}}]

            def normalize_usage(self, raw):
                return {}

        client = LLMClient(_settings("stub"))
        with mock.patch.dict(providers.PROVIDERS, {"stub": _Stub}):
            content, tool_calls = _run(
                client.generate(messages=[{"role": "user", "content": "hi"}])
            )

        self.assertEqual(content, "hello")
        self.assertEqual(tool_calls, [{"id": "1", "name": "t", "arguments": {}}])
        # The configured model is resolved by the facade before delegating.
        self.assertEqual(seen["model"], "m")
        # last_usage is mirrored onto the facade for the kernel's cost tracking.
        self.assertEqual(client.last_usage, {"input": 3, "output": 7})


if __name__ == "__main__":
    unittest.main()
