"""Offline tests for the 'google' provider fail-fast (P02, Option B).

The Gemini path never supported tool calling, which every real agent turn
requires, so configuring ``provider: google`` must fail fast at
``initialize()`` with a human-readable ``ValueError`` — not surface as a
``NotImplementedError`` deep inside the first tool-bearing turn, and never
silently drop tools. Fully offline: no provider SDK is imported.
"""

import asyncio
import types
import unittest

from dacli.ai.llm import LLMClient


def _client(provider):
    # Build an LLMClient without importing any provider SDK: __init__ only
    # reads settings.llm.provider, and the google branch of initialize()
    # raises before touching any other setting.
    settings = types.SimpleNamespace(llm=types.SimpleNamespace(provider=provider))
    return LLMClient(settings)


class GoogleProviderFailFastTest(unittest.TestCase):
    def test_initialize_raises_clear_value_error(self):
        # The config error is a ValueError (not NotImplementedError, not a
        # silent failure) and tells the user what to do instead.
        with self.assertRaises(ValueError) as ctx:
            asyncio.run(_client("google").initialize())

        message = str(ctx.exception)
        self.assertIn("'google'", message)
        self.assertIn("tool use", message)
        for alternative in ("openai", "anthropic", "openrouter"):
            self.assertIn(alternative, message)

    def test_provider_match_is_case_insensitive(self):
        # initialize() lowercases the provider; "Google" must not slip past
        # the guard into the generic "unsupported provider" branch.
        with self.assertRaises(ValueError) as ctx:
            asyncio.run(_client("Google").initialize())

        self.assertIn("tool use", str(ctx.exception))

    def test_generate_surfaces_the_same_config_error(self):
        # generate() lazily initializes, so a google-configured client fails
        # with the same early error on its first turn — tools or not.
        client = _client("google")

        with self.assertRaises(ValueError) as ctx:
            asyncio.run(
                client.generate(
                    messages=[{"role": "user", "content": "hi"}],
                    tools=[{"type": "function", "function": {"name": "t", "description": "", "parameters": {}}}],
                )
            )

        self.assertIn("tool use", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
