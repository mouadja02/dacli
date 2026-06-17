"""The chat slash-command registry (P10 cli.py split)."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from dacli.config import CLI_COMMANDS
from dacli.tui import slash


def _bare_names() -> set:
    return {usage.split()[0] for usage, _desc in CLI_COMMANDS}


def _ctx() -> slash.ChatContext:
    ui = MagicMock()
    return slash.ChatContext(
        ui=ui,
        console=MagicMock(),
        memory=MagicMock(),
        agent=MagicMock(),
        store=MagicMock(),
        settings=SimpleNamespace(llm=SimpleNamespace(provider="p", model="m")),
        config_path=None,
    )


class SlashRegistryTest(unittest.TestCase):
    def test_registry_and_completion_list_are_one_source(self):
        # Every advertised command (minus the exit aliases the loop handles) has
        # a handler, and no handler is advertised that the menu doesn't list.
        self.assertEqual(set(slash.HANDLERS), _bare_names() - slash.EXIT_COMMANDS)

    def test_exit_sets_should_exit(self):
        ctx = _ctx()
        asyncio.run(slash.dispatch(ctx, "/exit"))
        self.assertTrue(ctx.should_exit)

    def test_unknown_command_notices(self):
        ctx = _ctx()
        asyncio.run(slash.dispatch(ctx, "/nope"))
        self.assertFalse(ctx.should_exit)
        ctx.ui.notice.assert_called_once()
        self.assertIn("Unknown command", ctx.ui.notice.call_args.args[0])

    def test_help_routes_to_ui(self):
        ctx = _ctx()
        asyncio.run(slash.dispatch(ctx, "/help"))
        ctx.ui.help.assert_called_once_with(CLI_COMMANDS)

    def test_clear_clears_messages(self):
        ctx = _ctx()
        asyncio.run(slash.dispatch(ctx, "/clear"))
        ctx.memory.clear_messages.assert_called_once()

    def test_args_are_parsed_case_preserving(self):
        # /load keeps the session-id argument's case; the command itself lowercases.
        ctx = _ctx()
        ctx.memory.load_session.return_value = True
        asyncio.run(slash.dispatch(ctx, "/LOAD MySession"))
        ctx.memory.load_session.assert_called_once_with("MySession")


if __name__ == "__main__":
    unittest.main()
