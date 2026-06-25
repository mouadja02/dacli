"""Tests for the Rich TUI layer and the LLM streaming path.

These cover the two pieces most likely to break silently:
* ``DacliUI`` rendering — every transcript primitive must render without
  raising (a rendering error must never reach the control loop).
* OpenAI streaming reassembly — tool calls arrive as indexed fragments across
  chunks and must be stitched back into whole calls with parsed arguments.
"""

import asyncio
import types

from rich.console import Console

from dacli.connectors.base import ToolResult, ToolStatus
from dacli.ai.llm import LLMClient
from dacli.tui import DacliUI, get_theme, THEMES, DEFAULT_THEME


def _recording_ui() -> DacliUI:
    console = Console(record=True, width=80, force_terminal=False)
    return DacliUI(version="9.9.9", author="tester", console=console)


# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------
def test_get_theme_falls_back_to_default():
    assert get_theme("does-not-exist").name == DEFAULT_THEME
    assert get_theme(None).name == DEFAULT_THEME
    for name in THEMES:
        assert get_theme(name).name == name


def test_every_theme_defines_all_style_keys():
    from dacli.tui.theme import STYLE_KEYS
    for spec in THEMES.values():
        for key in STYLE_KEYS:
            assert key in spec.styles, f"{spec.name} missing style '{key}'"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def test_transcript_primitives_render():
    ui = _recording_ui()
    ui.banner()
    ui.welcome(model="m", provider="p", connectors=["snowflake"], cwd="/tmp")
    ui.tool_start("execute_query", {"query": "SELECT 1", "warehouse": "WH"})
    ui.tool_end("execute_query", ToolResult("execute_query", ToolStatus.SUCCESS,
                                            data=[{"a": 1, "b": 2}], execution_time_ms=10.0))
    ui.tool_end("execute_query", ToolResult("execute_query", ToolStatus.ERROR, error="boom"))
    ui.agent_message("**done**")
    ui.error("nope")
    ui.help(THEMES.items() and [("/help", "help")])
    out = ui.console.export_text()
    assert "execute_query" in out
    assert "done" in out


def test_stream_view_leaves_final_markdown():
    ui = _recording_ui()
    ui.on_stream_start()
    assert ui.stream.active
    for delta in ["Hello ", "**world**"]:
        ui.on_text(delta)
    ui.on_stream_end("Hello **world**")
    assert not ui.stream.active
    assert "world" in ui.console.export_text()


def test_set_theme_known_and_unknown():
    ui = _recording_ui()
    assert ui.set_theme("ocean") is True
    assert ui.theme.name == "ocean"
    # Unknown name falls back but reports False.
    assert ui.set_theme("bogus") is False


def test_every_theme_toolbar_colors_are_prompt_toolkit_parseable():
    # Regression: prompt-toolkit parses the bar colors on every redraw and
    # raises on Rich-only names like "grey15" — which crashes the input loop.
    from prompt_toolkit.styles.style import parse_color
    from prompt_toolkit.formatted_text import to_formatted_text
    ui = _recording_ui()
    for name in THEMES:
        ui.set_theme(name)
        tb = ui.bottom_toolbar(provider="p", model="m", connectors=["a"], ctx_pct=1, session="s")
        for frag in to_formatted_text(tb):
            for part in frag[0].split():
                if part.startswith(("fg:", "bg:")):
                    parse_color(part[3:])  # must not raise


def test_bottom_toolbar_is_formatted_text():
    from prompt_toolkit.formatted_text import to_formatted_text
    ui = _recording_ui()
    tb = ui.bottom_toolbar(provider="anthropic", model="x", connectors=["a", "b"],
                           ctx_pct=42, session="sess")
    frags = to_formatted_text(tb)
    text = "".join(t[1] for t in frags)
    # ctx renders as a gauge now (P13/M6): "ctx ▰▰▱▱▱ 42%".
    assert "ctx" in text and "42%" in text
    assert "sess" in text


# ---------------------------------------------------------------------------
# Result render cap (P05 Fix C)
# ---------------------------------------------------------------------------
def test_tool_end_caps_rendered_rows_but_keeps_real_count():
    ui = _recording_ui()
    rows = [{"id": i, "name": f"val_{i}"} for i in range(1000)]
    ui.tool_end("execute_query", ToolResult(
        "execute_query", ToolStatus.SUCCESS, data=rows, execution_time_ms=5.0))
    out = ui.console.export_text()
    assert "1000 rows" in out                      # summary stays truthful
    assert "showing 120 of 1,000" in out           # capped render is labelled
    assert "val_0" in out and "val_999" in out     # head and tail survive
    assert "val_500" not in out                    # the middle is elided
    assert out.count("val_") <= 120


def test_tool_end_caps_rendered_list_items():
    ui = _recording_ui()
    items = [f"item_{i}" for i in range(500)]
    ui.tool_end("list_things", ToolResult(
        "list_things", ToolStatus.SUCCESS, data=items, execution_time_ms=5.0))
    out = ui.console.export_text()
    assert "500 items" in out
    assert "showing 120 of 500" in out
    assert "item_0" in out and "item_499" in out
    assert "item_250" not in out


def test_tool_end_small_results_render_in_full():
    ui = _recording_ui()
    rows = [{"id": i} for i in range(5)]
    ui.tool_end("execute_query", ToolResult(
        "execute_query", ToolStatus.SUCCESS, data=rows, execution_time_ms=5.0))
    out = ui.console.export_text()
    assert "5 rows" in out
    assert "showing" not in out  # no cap footer under the limit


def test_max_render_rows_is_configurable():
    import types as _types
    console = Console(record=True, width=80, force_terminal=False)
    settings = _types.SimpleNamespace(ui=_types.SimpleNamespace(max_render_rows=12))
    ui = DacliUI(settings=settings, version="9.9.9", author="t", console=console)
    rows = [{"id": i} for i in range(100)]
    ui.tool_end("execute_query", ToolResult(
        "execute_query", ToolStatus.SUCCESS, data=rows, execution_time_ms=5.0))
    out = ui.console.export_text()
    assert "100 rows" in out
    assert "showing 12 of 100" in out


# ---------------------------------------------------------------------------
# /keys panel (P08 U-2)
# ---------------------------------------------------------------------------
def test_keys_panel_renders_shortcuts():
    ui = _recording_ui()
    ui.keys_panel()
    out = ui.console.export_text()
    assert "Keyboard shortcuts" in out
    assert "Tab" in out
    assert "Ctrl-R" in out
    assert "/help" in out


def test_keys_command_is_registered_for_autocomplete():
    from dacli.config import CLI_COMMANDS
    assert "/keys" in [cmd.split()[0] for cmd, _desc in CLI_COMMANDS]


# ---------------------------------------------------------------------------
# Approval / plan rendering (P08 U-3)
# ---------------------------------------------------------------------------
def _approval_request(**overrides):
    base = {
        "tool_name": "execute_snowflake_query",
        "tier": types.SimpleNamespace(value="risky"),
        "classification": types.SimpleNamespace(
            is_prod=False, prod_marker="", reasons=["DML on orders"]
        ),
        "policy": types.SimpleNamespace(
            decision=types.SimpleNamespace(value="require_approval"), source="default"
        ),
        "rollback_plan": types.SimpleNamespace(
            strategy="transaction rollback", primitive="txn", verify_detail="BEGIN ok"
        ),
        "args": {},
        "dry_run_preview": None,
        "shadow": None,
    }
    base.update(overrides)
    request = types.SimpleNamespace(**base)
    request.describe = lambda: "Action      : execute_snowflake_query"
    return request


def test_approval_panel_renders_structured_request():
    ui = _recording_ui()
    ui.approval_panel(_approval_request())
    out = ui.console.export_text()
    assert "approval needed" in out
    assert "risky" in out
    assert "execute_snowflake_query" in out
    assert "DML on orders" in out
    assert "transaction rollback" in out


def test_approval_panel_renders_sql_dry_run_preview():
    ui = _recording_ui()
    ui.approval_panel(
        _approval_request(dry_run_preview="UPDATE orders SET status = 'x'")
    )
    out = ui.console.export_text()
    assert "Dry-run preview" in out
    assert "UPDATE" in out


def test_approval_panel_renders_shadow_row_delta_table():
    ui = _recording_ui()
    shadow = types.SimpleNamespace(
        ran=True, diff={"rows_before": 100, "rows_after": 90, "row_delta": -10}
    )
    ui.approval_panel(_approval_request(shadow=shadow))
    out = ui.console.export_text()
    assert "rows before" in out
    assert "100" in out and "90" in out and "-10" in out


def test_approval_panel_renders_dag_plan():
    ui = _recording_ui()
    plan = types.SimpleNamespace(
        render=lambda: "Plan for: migrate\n1. [a] create table\n2. [b] load data"
    )
    ui.approval_panel(plan)
    out = ui.console.export_text()
    assert "Plan for: migrate" in out
    assert "create table" in out


def test_approval_panel_never_raises_on_malformed_input():
    ui = _recording_ui()
    ui.approval_panel(object())
    ui.approval_panel(None)
    # describe() raising + a shadow diff that can't compute a delta.
    weird = _approval_request(
        classification=None,
        policy=None,
        rollback_plan=None,
        shadow=types.SimpleNamespace(ran=True, diff={"rows_before": "x", "rows_after": None}),
    )
    weird.describe = lambda: 1 / 0
    ui.approval_panel(weird)
    assert "approval needed" in ui.console.export_text()


# ---------------------------------------------------------------------------
# Tool liveness (P08 U-4)
# ---------------------------------------------------------------------------
def test_tool_progress_updates_then_tool_end_clears():
    ui = _recording_ui()
    ui.tool_progress("trigger_airflow_dag", "run r1: queued — polling (1/10)")
    ui.tool_progress("trigger_airflow_dag", "run r1: running — polling (2/10)")
    assert ui._progress_live is not None
    ui.tool_end("trigger_airflow_dag", ToolResult(
        "trigger_airflow_dag", ToolStatus.SUCCESS, execution_time_ms=1.0))
    assert ui._progress_live is None


def test_tool_progress_feeds_spinner_while_streaming():
    ui = _recording_ui()
    ui.on_stream_start()
    ui.tool_progress("launch_dagster_run", "run r2: STARTED")
    assert ui._progress_live is None  # the stream owns the live region
    assert "launch_dagster_run" in ui.activity
    ui.on_stream_end("done")


def test_tool_progress_never_raises_on_weird_data():
    ui = _recording_ui()
    ui.tool_progress(None, object())  # malformed input must not raise
    ui.tool_start("x", {})
    assert ui._progress_live is None


# ---------------------------------------------------------------------------
# Error remediation hints (P08 U-5)
# ---------------------------------------------------------------------------
def test_tool_end_health_error_hints_debug_connector():
    ui = _recording_ui()
    ui.tool_end("q", ToolResult(
        "q", ToolStatus.ERROR, error="connector 'foo' is not healthy: boom"))
    out = ui.console.export_text()
    assert "/debug-connector" in out


def test_error_decryption_failure_hints_connect():
    ui = _recording_ui()
    ui.error("failed to decrypt secrets store")
    assert "/connect" in ui.console.export_text()


def test_unmapped_error_has_no_hint():
    ui = _recording_ui()
    ui.tool_end("q", ToolResult("q", ToolStatus.ERROR, error="some odd failure"))
    assert "↳" not in ui.console.export_text()


# ---------------------------------------------------------------------------
# Bottom toolbar $cost segment (P08 U-6)
# ---------------------------------------------------------------------------
def test_bottom_toolbar_shows_cost_segment():
    from prompt_toolkit.formatted_text import to_formatted_text
    ui = _recording_ui()
    tb = ui.bottom_toolbar(provider="p", model="m", connectors=["a"],
                           ctx_pct=10, session="s", cost="$0.12")
    text = "".join(t[1] for t in to_formatted_text(tb))
    assert "$0.12" in text


def test_bottom_toolbar_omits_cost_segment_when_blank():
    from prompt_toolkit.formatted_text import to_formatted_text
    ui = _recording_ui()
    tb = ui.bottom_toolbar(provider="p", model="m", connectors=["a"],
                           ctx_pct=10, session="s")
    text = "".join(t[1] for t in to_formatted_text(tb))
    assert "$" not in text


# ---------------------------------------------------------------------------
# OpenAI streaming reassembly
# ---------------------------------------------------------------------------
def _chunk(content=None, tool_fragments=None):
    """Build a minimal OpenAI streaming chunk object."""
    tool_calls = None
    if tool_fragments:
        tool_calls = []
        for index, tc_id, name, args in tool_fragments:
            fn = types.SimpleNamespace(name=name, arguments=args)
            tool_calls.append(types.SimpleNamespace(index=index, id=tc_id, function=fn))
    delta = types.SimpleNamespace(content=content, tool_calls=tool_calls)
    choice = types.SimpleNamespace(delta=delta)
    return types.SimpleNamespace(choices=[choice])


class _FakeStream:
    def __init__(self, chunks):
        self._chunks = chunks

    def __aiter__(self):
        async def gen():
            for c in self._chunks:
                yield c
        return gen()


class _FakeCompletions:
    def __init__(self, chunks):
        self._chunks = chunks

    async def create(self, **kwargs):
        assert kwargs.get("stream") is True
        return _FakeStream(self._chunks)


def _bypass_client(chunks) -> LLMClient:
    # Build an LLMClient without __init__ (no real settings/SDK). The streaming
    # path now routes through _with_retry, so supply the bits it reads:
    # ``_provider`` (retryable-exception lookup) and a minimal retry config.
    client = LLMClient.__new__(LLMClient)
    client._provider = "openai"
    client.settings = types.SimpleNamespace(
        llm=types.SimpleNamespace(retry_attempts=1, retry_base_delay=0.5)
    )
    client._client = types.SimpleNamespace(chat=types.SimpleNamespace(completions=_FakeCompletions(chunks)))
    return client


def test_stream_openai_reassembles_text_and_tool_calls():
    chunks = [
        _chunk(content="Let me "),
        _chunk(content="check."),
        # tool call split across fragments, arguments arrive piecemeal
        _chunk(tool_fragments=[(0, "call_1", "run_query", '{"q":')]),
        _chunk(tool_fragments=[(0, None, None, ' "SELECT 1"}')]),
    ]
    client = _bypass_client(chunks)

    seen = []
    content, tool_calls = asyncio.run(
        client._stream_openai({"model": "x", "messages": []}, on_text=seen.append)
    )

    assert content == "Let me check."
    assert seen == ["Let me ", "check."]
    assert tool_calls == [{"id": "call_1", "name": "run_query", "arguments": {"q": "SELECT 1"}}]


def test_stream_openai_tolerates_bad_json_arguments():
    chunks = [_chunk(tool_fragments=[(0, "c1", "f", "{not json")])]
    client = _bypass_client(chunks)
    content, tool_calls = asyncio.run(client._stream_openai({"model": "x", "messages": []}, on_text=None))
    assert content == ""
    assert tool_calls == [{"id": "c1", "name": "f", "arguments": {}}]
