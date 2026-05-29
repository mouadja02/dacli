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

from connectors.base import ToolResult, ToolStatus
from reasoning.llm import LLMClient
from tui import DacliUI, get_theme, THEMES, DEFAULT_THEME


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
    from tui.theme import STYLE_KEYS
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
    assert "ctx 42%" in text
    assert "sess" in text


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


def test_stream_openai_reassembles_text_and_tool_calls():
    chunks = [
        _chunk(content="Let me "),
        _chunk(content="check."),
        # tool call split across fragments, arguments arrive piecemeal
        _chunk(tool_fragments=[(0, "call_1", "run_query", '{"q":')]),
        _chunk(tool_fragments=[(0, None, None, ' "SELECT 1"}')]),
    ]
    client = LLMClient.__new__(LLMClient)  # bypass __init__ (no settings needed)
    client._client = types.SimpleNamespace(chat=types.SimpleNamespace(completions=_FakeCompletions(chunks)))

    seen = []
    content, tool_calls = asyncio.run(
        client._stream_openai({"model": "x", "messages": []}, on_text=seen.append)
    )

    assert content == "Let me check."
    assert seen == ["Let me ", "check."]
    assert tool_calls == [{"id": "call_1", "name": "run_query", "arguments": {"q": "SELECT 1"}}]


def test_stream_openai_tolerates_bad_json_arguments():
    chunks = [_chunk(tool_fragments=[(0, "c1", "f", "{not json")])]
    client = LLMClient.__new__(LLMClient)
    client._client = types.SimpleNamespace(chat=types.SimpleNamespace(completions=_FakeCompletions(chunks)))
    content, tool_calls = asyncio.run(client._stream_openai({"model": "x", "messages": []}, on_text=None))
    assert content == ""
    assert tool_calls == [{"id": "c1", "name": "f", "arguments": {}}]
