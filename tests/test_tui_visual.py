"""Snapshot & robustness tests for the TUI design system (P13).

Every renderer is exercised through a recording console so output is
deterministic and assertable. The structural invariants under test:
* glyph resolution degrades to ASCII on incapable terminals / NO_COLOR,
* ASCII mode emits no Unicode glyphs from the design set,
* NO_COLOR output carries no ANSI color codes,
* renderers never raise on malformed input (the control-loop guarantee).
"""

import types

import pytest
from rich.console import Console

from dacli.connectors.base import ToolResult, ToolStatus
from dacli.tui import DacliUI
from dacli.tui.design import ASCII, SPACING, TIER_STYLE, UNICODE, resolve_glyphs

# Glyphs that must never appear in ASCII-mode output.
_UNICODE_GLYPHS = "⏺⎿❯▌✓✗⚠ℹ○◐⏸●▰▱…·—↑↓⛁◴⎇│"


def _ui(width=100, glyphs=None, **ui_knobs) -> DacliUI:
    console = Console(record=True, width=width, force_terminal=False)
    knobs = {"glyphs": glyphs or "auto", "max_render_rows": 120}
    knobs.update(ui_knobs)
    settings = types.SimpleNamespace(ui=types.SimpleNamespace(**knobs))
    return DacliUI(settings=settings, version="9.9.9", author="t", console=console)


def _drive_transcript(ui: DacliUI) -> str:
    ui.welcome(model="m", provider="p", connectors=["snowflake"], cwd="/tmp")
    ui.tool_start("execute_query", {"query": "SELECT 1", "warehouse": "WH"})
    ui.tool_end("execute_query", ToolResult(
        "execute_query", ToolStatus.SUCCESS,
        data=[{"a": 1, "b": 2}], execution_time_ms=10.0))
    ui.tool_end("execute_query", ToolResult(
        "execute_query", ToolStatus.ERROR, error="rate limit hit"))
    ui.agent_message("**done**")
    ui.notice("saved", style="success")
    ui.error("nope")
    return ui.console.export_text()


# ---------------------------------------------------------------------------
# M1 — glyph resolution
# ---------------------------------------------------------------------------
def test_resolve_glyphs_explicit_settings_win():
    console = Console(record=True, width=80)
    ascii_settings = types.SimpleNamespace(ui=types.SimpleNamespace(glyphs="ascii"))
    unicode_settings = types.SimpleNamespace(ui=types.SimpleNamespace(glyphs="unicode"))
    assert resolve_glyphs(console, ascii_settings) is ASCII
    assert resolve_glyphs(console, unicode_settings) is UNICODE


def test_resolve_glyphs_ascii_under_no_color(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    console = Console(record=True, width=80)
    assert resolve_glyphs(console, None) is ASCII


def test_resolve_glyphs_ascii_under_dumb_terminal(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("TERM", "dumb")
    console = Console(record=True, width=80)
    assert resolve_glyphs(console, None) is ASCII


def test_resolve_glyphs_ascii_under_non_utf8_console(monkeypatch, tmp_path):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("TERM", raising=False)
    f = open(tmp_path / "out.txt", "w", encoding="cp1252")  # noqa: SIM115
    try:
        console = Console(file=f, width=80)
        assert resolve_glyphs(console, None) is ASCII
    finally:
        f.close()


def test_resolve_glyphs_never_raises_on_garbage():
    assert resolve_glyphs(object(), object()) in (ASCII, UNICODE)
    assert resolve_glyphs(None, None) in (ASCII, UNICODE)


def test_design_tokens_are_complete():
    # Both glyph sets fill every field, and the spacing contract holds.
    for field in UNICODE.__dataclass_fields__:
        assert getattr(ASCII, field) is not None
        assert getattr(UNICODE, field) is not None
    assert set(SPACING) >= {"gutter_w", "indent", "panel_pad", "section_gap"}
    assert set(TIER_STYLE) == {"safe", "write", "risky", "irreversible"}


# ---------------------------------------------------------------------------
# M1 — snapshot: unicode vs ascii, color vs NO_COLOR, widths
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("width", [60, 100, 160])
def test_transcript_renders_at_width(width):
    out = _drive_transcript(_ui(width=width))
    assert "execute_query" in out
    assert "done" in out
    # Nothing overflows: no exported line exceeds the console width.
    assert all(len(line) <= width for line in out.splitlines())


def test_unicode_transcript_uses_design_glyphs():
    ui = _ui(glyphs="unicode")
    out = _drive_transcript(ui)
    assert UNICODE.agent in out
    assert UNICODE.result in out
    assert UNICODE.ok in out
    assert UNICODE.err in out


def test_ascii_transcript_has_no_unicode_design_glyphs():
    ui = _ui(glyphs="ascii")
    out = _drive_transcript(ui)
    for glyph in _UNICODE_GLYPHS:
        assert glyph not in out, f"unicode glyph {glyph!r} leaked into ascii mode"
    out.encode("ascii")  # the whole transcript must be ASCII-safe


def test_no_color_output_has_no_ansi_codes():
    console = Console(
        record=True, width=100, force_terminal=True, no_color=True
    )
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs="ascii", max_render_rows=120)
    )
    ui = DacliUI(settings=settings, console=console)
    _drive_transcript(ui)
    exported = ui.console.export_text(styles=True)
    # export_text(styles=True) keeps ANSI sequences — color codes must be gone.
    for code in ("[31m", "[32m", "[33m", "[34m", "[35m", "[36m", "[38;"):
        assert code not in exported


def test_ascii_bottom_toolbar_is_ascii_safe():
    from prompt_toolkit.formatted_text import to_formatted_text

    ui = _ui(glyphs="ascii")
    tb = ui.bottom_toolbar(
        provider="p", model="m", connectors=["a", "b"], ctx_pct=42,
        session="sess", cost="$0.10",
    )
    text = "".join(t[1] for t in to_formatted_text(tb))
    text.encode("ascii")
    assert "ctx" in text and "sess" in text


def test_ascii_prompt_is_ascii_safe():
    from prompt_toolkit.formatted_text import to_formatted_text

    ui = _ui(glyphs="ascii")
    text = "".join(t[1] for t in to_formatted_text(ui.prompt_html()))
    text.encode("ascii")
    assert "you" in text


# ---------------------------------------------------------------------------
# M1 — robustness: renderers never raise
# ---------------------------------------------------------------------------
def test_renderers_survive_malformed_input():
    ui = _ui()
    ui.tool_end("t", ToolResult("t", ToolStatus.SUCCESS, data=[], execution_time_ms=0))
    ui.tool_end("t", ToolResult("t", ToolStatus.SUCCESS, data=None, execution_time_ms=0))
    ui.tool_end("t", "not a ToolResult")
    ui.tool_end("t", None)
    ui.notice("", style="not-a-style")
    ui.error("")
    ui.status("multi\nline\nstatus")
    big = [{"c": "x" * 500} for _ in range(50_000)]
    ui.tool_end("t", ToolResult("t", ToolStatus.SUCCESS, data=big, execution_time_ms=1))
    out = ui.console.export_text()
    assert "50000 rows" in out


def test_reduced_motion_spinner_is_static():
    ui = _ui(reduced_motion=True)
    ui.on_stream_start()
    frame_a = ui.stream.__rich__()
    frame_b = ui.stream.__rich__()
    # Static spinner: same frame glyph and verb regardless of elapsed time.
    assert frame_a.plain.split()[0] == frame_b.plain.split()[0]
    ui.on_stream_end("done")


# ---------------------------------------------------------------------------
# M2 — live formatted markdown streaming
# ---------------------------------------------------------------------------
def _render_stream_frame(ui: DacliUI) -> str:
    frame = ui.stream.__rich__()
    scratch = Console(record=True, width=100, force_terminal=False)
    scratch.push_theme(ui.theme.rich_theme())
    scratch.print(frame)
    return scratch.export_text()


def test_streaming_renders_markdown_constructs_live():
    ui = _ui()
    ui.on_stream_start()
    for delta in ["# Head", "ing\n", "- bullet one\n- bullet ", "two\n", "**bold**\n"]:
        ui.on_text(delta)
    out = _render_stream_frame(ui)
    ui.on_stream_end("done")
    # Markdown formatting applied mid-stream: heading text present, the
    # literal markers consumed by the renderer.
    assert "Heading" in out
    assert "# Head" not in out
    assert "**bold**" not in out and "bold" in out


def test_streaming_open_code_fence_does_not_swallow_text():
    ui = _ui()
    ui.on_stream_start()
    ui.on_text("intro\n\n```sql\nSELECT 1\n")  # fence still open mid-stream
    mid = _render_stream_frame(ui)
    assert "SELECT 1" in mid
    ui.on_text("```\n\nafter the block\n")
    late = _render_stream_frame(ui)
    ui.on_stream_end("done")
    assert "after the block" in late
    # The synthetic closing fence never leaks into the real buffer.
    assert ui.stream._buffer == ""  # cleared by end()


def test_streaming_reparse_is_throttled():
    ui = _ui()
    ui.on_stream_start()
    ui.on_text("hello world\n")
    first = ui.stream.__rich__()
    ui.on_text("x")  # < threshold, no newline → cached renderable reused
    second = ui.stream.__rich__()
    assert second is first
    ui.on_text("y" * 60)  # over the threshold → re-parse
    third = ui.stream.__rich__()
    assert third is not first
    ui.on_stream_end("done")


def test_streaming_reduced_motion_falls_back_to_plain_text():
    ui = _ui(reduced_motion=True)
    ui.on_stream_start()
    ui.on_text("# Heading\n")
    frame = ui.stream.__rich__()
    ui.on_stream_end("done")
    # Plain Text fallback keeps the raw markdown untouched.
    assert "# Heading" in frame.plain


def test_streaming_markdown_failure_falls_back_cleanly(monkeypatch):
    ui = _ui()
    ui.on_stream_start()
    with monkeypatch.context() as m:
        m.setattr(
            "dacli.tui.ui.Markdown",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        ui.on_text("# Heading\n")
        frame = ui.stream.__rich__()  # must not raise
    assert "# Heading" in frame.plain  # plain-text fallback
    assert ui.stream._md_failed is True
    ui.on_stream_end("done")


def test_final_scrollback_output_unchanged():
    ui = _ui()
    ui.on_stream_start()
    ui.on_text("Hello **world**\n")
    ui.on_stream_end("Hello **world**")
    out = ui.console.export_text()
    assert "world" in out
    assert "**world**" not in out  # final pass is polished markdown


def test_every_theme_defines_a_code_theme():
    from dacli.tui import THEMES

    for spec in THEMES.values():
        assert spec.code_theme
    # Light theme must not inherit a dark code palette.
    assert THEMES["light"].code_theme != THEMES["dark"].code_theme
