"""Transcript navigability: /find, /last-error, /expand and the Textual gate (P11)."""

import asyncio
import unittest
from io import StringIO
from types import SimpleNamespace
from unittest.mock import MagicMock

from rich.console import Console

from dacli.config import CLI_COMMANDS
from dacli.connectors.base import ToolResult, ToolStatus
from dacli.context.spill import ResultStore
from dacli.tui import DacliUI, slash
from dacli.tui.transcript import TranscriptLog


def _ui() -> tuple[DacliUI, StringIO]:
    buf = StringIO()
    console = Console(file=buf, width=200, force_terminal=False, color_system=None)
    return DacliUI(console=console), buf


def _ok(name: str, data) -> ToolResult:
    return ToolResult(tool_name=name, status=ToolStatus.SUCCESS, data=data)


def _err(name: str, error: str) -> ToolResult:
    return ToolResult(tool_name=name, status=ToolStatus.ERROR, error=error)


class TranscriptLogTest(unittest.TestCase):
    def test_ids_are_sequential(self):
        log = TranscriptLog()
        a = log.add("query", _ok("query", [1, 2]), "2 items", elided=False)
        b = log.add("read", _ok("read", {"x": 1}), "1 field", elided=False)
        self.assertEqual((a.rid, b.rid), ("t1", "t2"))

    def test_last_error_walks_back(self):
        log = TranscriptLog()
        log.add("query", _ok("query", [1]), "1 item", elided=False)
        log.add("write", _err("write", "boom"), "boom", elided=False)
        log.add("query", _ok("query", [2]), "1 item", elided=False)
        rec = log.last_error()
        self.assertEqual(rec.tool_name, "write")
        self.assertEqual(rec.error, "boom")

    def test_last_error_none_when_all_ok(self):
        log = TranscriptLog()
        log.add("query", _ok("query", [1]), "1 item", elided=False)
        self.assertIsNone(log.last_error())

    def test_search_matches_summary_and_name(self):
        log = TranscriptLog()
        log.add("snowflake_query", _ok("snowflake_query", [1]), "42 rows", elided=False)
        log.add("github_list", _ok("github_list", [1]), "3 rows", elided=False)
        self.assertEqual([r.rid for r in log.search("snowflake")], ["t1"])
        self.assertEqual([r.rid for r in log.search("42")], ["t1"])

    def test_inline_data_resolves_without_store(self):
        log = TranscriptLog()
        rec = log.add("query", _ok("query", [{"a": 1}]), "1 row", elided=False)
        self.assertEqual(log.resolve_data(rec), [{"a": 1}])

    def test_elided_result_spills_to_store(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            store = ResultStore(root=tmp, session_id="s")
            log = TranscriptLog(store=store)
            rows = [{"i": i} for i in range(300)]
            rec = log.add("query", _ok("query", rows), "300 rows", elided=True)
            self.assertIsNotNone(rec.handle)
            self.assertIsNone(rec.data)  # kept off-process, fetched on demand
            self.assertEqual(log.resolve_data(rec), rows)


class ToolEndTest(unittest.TestCase):
    def test_success_surfaces_referenceable_id(self):
        ui, buf = _ui()
        ui.tool_end("query", _ok("query", [{"a": 1}, {"a": 2}]))
        self.assertEqual(len(ui.transcript_log._records), 1)
        self.assertIn("t1", buf.getvalue())

    def test_huge_result_is_marked_elided(self):
        ui, buf = _ui()
        ui.tool_end("query", _ok("query", [{"i": i} for i in range(300)]))
        rec = ui.transcript_log.get("t1")
        self.assertTrue(rec.elided)
        self.assertIn("/expand t1", buf.getvalue())

    def test_failure_is_recorded_for_last_error(self):
        ui, _ = _ui()
        ui.tool_end("write", _err("write", "permission denied"))
        self.assertEqual(ui.transcript_log.last_error().tool_name, "write")


class ExpandTest(unittest.TestCase):
    def test_expand_renders_full_result_past_cap(self):
        ui, buf = _ui()
        ui.tool_end("query", _ok("query", [{"i": i} for i in range(300)]))
        buf.truncate(0)
        buf.seek(0)
        ui.expand("t1")
        out = buf.getvalue()
        # A row beyond the 120-row render cap is present once fully expanded.
        self.assertIn("299", out)

    def test_unknown_id_is_friendly(self):
        ui, buf = _ui()
        ui.expand("t99")
        self.assertIn("t99", buf.getvalue())
        self.assertIn("No result", buf.getvalue())


class LastErrorTest(unittest.TestCase):
    def test_renders_last_failure(self):
        ui, buf = _ui()
        ui.tool_end("query", _ok("query", [{"a": 1}]))
        ui.tool_end("write", _err("write", "permission denied"))
        buf.truncate(0)
        buf.seek(0)
        ui.last_error()
        out = buf.getvalue()
        self.assertIn("permission denied", out)
        # Remediation hint for a permission error.
        self.assertIn("/audit", out)

    def test_no_errors_notice(self):
        ui, buf = _ui()
        ui.last_error()
        self.assertIn("No tool errors", buf.getvalue())


class FindTest(unittest.TestCase):
    def test_matches_history_and_transcript(self):
        ui, buf = _ui()
        ui.tool_end("snowflake_query", _ok("snowflake_query", [{"a": 1}]))
        history = [
            SimpleNamespace(role="user", content="load the orders table", timestamp="2026-06-17T10:00:00"),
            SimpleNamespace(role="assistant", content="done", timestamp="2026-06-17T10:00:01"),
        ]
        ui.find("orders", history)
        out = buf.getvalue()
        self.assertIn("orders", out)
        self.assertIn("user", out)

    def test_no_match_notice(self):
        ui, buf = _ui()
        ui.find("nonesuch", [])
        self.assertIn("No matches", buf.getvalue())


class SlashWiringTest(unittest.TestCase):
    def _ctx(self):
        return slash.ChatContext(
            ui=MagicMock(),
            console=MagicMock(),
            memory=MagicMock(),
            agent=MagicMock(),
            store=MagicMock(),
            settings=SimpleNamespace(llm=SimpleNamespace(provider="p", model="m")),
            config_path=None,
        )

    def test_commands_registered_and_advertised(self):
        for cmd in ("/find", "/last-error", "/expand", "/transcript"):
            self.assertIn(cmd, slash.HANDLERS)
            self.assertIn(cmd, {usage.split()[0] for usage, _ in CLI_COMMANDS})

    def test_expand_routes_to_ui(self):
        ctx = self._ctx()
        asyncio.run(slash.dispatch(ctx, "/expand t3"))
        ctx.ui.expand.assert_called_once_with("t3")

    def test_find_requires_argument(self):
        ctx = self._ctx()
        asyncio.run(slash.dispatch(ctx, "/find"))
        ctx.ui.notice.assert_called_once()
        ctx.ui.find.assert_not_called()

    def test_last_error_routes_to_ui(self):
        ctx = self._ctx()
        asyncio.run(slash.dispatch(ctx, "/last-error"))
        ctx.ui.last_error.assert_called_once()

    def test_completer_lists_new_commands(self):
        completer = slash.SlashCommandCompleter(CLI_COMMANDS)
        names = {c for c, _ in completer._commands}
        self.assertTrue({"/find", "/expand", "/transcript"} <= names)


class TranscriptGateTest(unittest.TestCase):
    def test_missing_textual_extra_gives_install_hint(self):
        from dacli.tui import transcript_app

        ctx = slash.ChatContext(
            ui=MagicMock(),
            console=MagicMock(),
            memory=MagicMock(),
            agent=MagicMock(),
            store=MagicMock(),
            settings=SimpleNamespace(llm=SimpleNamespace(provider="p", model="m")),
            config_path=None,
        )
        ctx.memory.get_full_history.return_value = []
        orig = transcript_app.is_available
        transcript_app.is_available = lambda: False
        try:
            asyncio.run(slash.dispatch(ctx, "/transcript"))
        finally:
            transcript_app.is_available = orig
        ctx.ui.notice.assert_called_once()
        # Escaped \[ in the source renders as the literal extra name dacli[tui].
        self.assertIn("[tui]", ctx.ui.notice.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
