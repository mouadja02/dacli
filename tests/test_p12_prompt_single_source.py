"""P12 Part 2.2 — one system-prompt source of truth.

The agent runs on ``prompts/fragments/core.md`` (composed live by the context
pipeline). ``/prompt`` used to display the *legacy* ``system_message.md`` — a
divergent, Snowflake-specific prompt the agent no longer uses. After P12 there is
one source: ``/prompt`` shows exactly the live system prompt, and the legacy
duplicate is gone.
"""

import unittest

from click.testing import CliRunner

from dacli.prompts.system_prompt import (
    CORE_FRAGMENT,
    compose_system_prompt,
    get_default_system_prompt,
    load_system_prompt,
)


class SingleSourceTest(unittest.TestCase):
    def test_prompt_view_equals_live_system_prompt_source(self):
        # What `/prompt` renders (get_default_system_prompt) must be byte-for-byte
        # the live invariant base the pipeline composes (compose_system_prompt).
        self.assertEqual(get_default_system_prompt(), compose_system_prompt("", []))

    def test_live_source_is_core_fragment(self):
        core = CORE_FRAGMENT.read_text(encoding="utf-8").strip()
        self.assertEqual(get_default_system_prompt(), core)

    def test_legacy_system_message_file_is_gone(self):
        legacy = CORE_FRAGMENT.parent.parent / "system_message.md"
        self.assertFalse(
            legacy.exists(), "divergent legacy system_message.md must be removed"
        )

    def test_load_system_prompt_sources_from_core(self):
        # The agent's static-fallback loader must build on the same single source.
        self.assertIn(get_default_system_prompt(), load_system_prompt())


class PromptCommandTest(unittest.TestCase):
    def test_prompt_command_shows_live_core_not_legacy(self):
        from dacli.scripts.cli import cli

        result = CliRunner().invoke(cli, ["prompt"])
        self.assertEqual(result.exit_code, 0, msg=result.output)
        # The current/generic core identity is shown...
        self.assertIn("Data Engineering AI Agent", result.output)
        # ...and the retired Snowflake-specific identity is not.
        self.assertNotIn("Hybrid Data Warehouse", result.output)


if __name__ == "__main__":
    unittest.main()
