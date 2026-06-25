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
            "dacli.tui.stream.Markdown",
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


# ---------------------------------------------------------------------------
# M3 — tool cards: tier-colored rail, dict key/value table, truthful counts
# ---------------------------------------------------------------------------
def test_dict_result_renders_as_aligned_key_values_not_json():
    ui = _ui()
    data = {
        "table": "orders",
        "rows_loaded": 1042,
        "schema": {"id": "int", "name": "str", "ts": "timestamp", "x": 1, "y": 2},
        "files": ["a.csv", "b.csv", "c.csv", "d.csv"],
    }
    ui.tool_end("load", ToolResult("load", ToolStatus.SUCCESS, data=data,
                                   execution_time_ms=3.0))
    out = ui.console.export_text()
    assert "4 fields" in out
    assert "rows_loaded" in out and "1042" in out
    # Nested values are compact previews with truthful sizes, not JSON dumps.
    assert "(5 keys)" in out
    assert "(4 items)" in out
    assert "a.csv" in out  # [brackets] in data must not be eaten as markup
    assert '"id": "int"' not in out


def test_risky_result_rail_uses_tier_color():
    console = Console(record=True, width=100, force_terminal=True)
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs="unicode", max_render_rows=120)
    )
    ui = DacliUI(settings=settings, console=console)  # dark: warning=yellow
    ui.tool_end("update_orders", ToolResult(
        "update_orders", ToolStatus.SUCCESS, data=None,
        execution_time_ms=1.0, metadata={"tier": "risky"}))
    exported = ui.console.export_text(styles=True)
    rail_line = next(line for line in exported.splitlines() if "⎿" in line)
    assert "[33m" in rail_line  # yellow rail = risky tier


def test_safe_result_rail_stays_muted():
    console = Console(record=True, width=100, force_terminal=True)
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs="unicode", max_render_rows=120)
    )
    ui = DacliUI(settings=settings, console=console)
    ui.tool_end("select", ToolResult(
        "select", ToolStatus.SUCCESS, data=None,
        execution_time_ms=1.0, metadata={"tier": "safe"}))
    exported = ui.console.export_text(styles=True)
    rail_line = next(line for line in exported.splitlines() if "⎿" in line)
    assert "[33m" not in rail_line and "[31m" not in rail_line


def test_dispatcher_tags_tier_for_the_ui():
    # The governance path stamps the blast-radius tier into result.metadata —
    # presentation-only, and absent entirely when no governor is wired (the
    # golden-transcript path).
    import asyncio
    import types as _t

    from dacli.connectors.dispatcher import Dispatcher
    from tests.golden_echo import EchoConnector

    class _Gov:
        async def review(self, tool_name, spec, arguments, connector):
            tier = _t.SimpleNamespace(value="write")
            return _t.SimpleNamespace(
                allowed=True,
                classification=_t.SimpleNamespace(tier=tier),
                short_circuit=None, blocked_reason=None,
            )

        def record_outcome(self, decision, result):
            pass

    echo = EchoConnector(None)
    registry = _t.SimpleNamespace(
        resolve=lambda name: (echo, name),
        get_operation_spec=lambda name: None,
        is_builtin=lambda name: True,
    )
    dispatcher = Dispatcher(registry=registry, governor=_Gov())
    result = asyncio.run(dispatcher.execute("echo_say", {"text": "x"}))
    assert result.success
    assert result.metadata.get("tier") == "write"


# ---------------------------------------------------------------------------
# M4 — progress, plan tree, text diff
# ---------------------------------------------------------------------------
def test_progress_advances_through_five_steps():
    ui = _ui()
    with ui.progress("connector init", total=5) as advance:
        for i in range(5):
            advance(f"step {i + 1}")
    # Transient bar leaves nothing behind, and nothing raised headless.
    assert ui.console is not None


def test_progress_reduced_motion_prints_static_steps():
    ui = _ui(reduced_motion=True)
    with ui.progress("dbt run", total=3) as advance:
        advance("compile")
        advance("run models")
        advance("test")
    out = ui.console.export_text()
    assert "dbt run" in out
    assert "step 1/3" in out and "step 3/3" in out


def test_progress_never_raises_without_total_or_console_quirks():
    ui = _ui()
    with ui.progress("poll airflow") as advance:
        advance()
        advance("still polling")


def _sample_dag():
    # plan_tree is fully duck-typed (getattr over goal/nodes and per-node
    # status/description/depends_on/irreversible/breadth_first/items/id), so a
    # SimpleNamespace stands in for the retired TaskDAG/Subtask types.
    def node(**kw):
        kw.setdefault("depends_on", [])
        kw.setdefault("irreversible", False)
        kw.setdefault("breadth_first", False)
        kw.setdefault("items", [])
        kw.setdefault("status", "pending")
        return types.SimpleNamespace(**kw)

    return types.SimpleNamespace(
        goal="stand up bronze->silver->gold",
        nodes=[
            node(id="a", description="create bronze schema", status="completed"),
            node(id="b", description="load raw files", depends_on=["a"], status="running"),
            node(id="c", description="profile all tables", depends_on=["b"],
                 breadth_first=True, items=["t1", "t2", "t3"]),
            node(id="d", description="drop legacy schema", depends_on=["b"],
                 irreversible=True, status="paused"),
        ],
    )


def test_plan_tree_renders_statuses_dependencies_and_tiers():
    ui = _ui(glyphs="unicode")
    ui.plan_tree(_sample_dag())
    out = ui.console.export_text()
    assert "stand up bronze->silver->gold" in out
    assert "✓" in out          # completed
    assert "◐" in out          # running
    assert "⏸" in out          # paused
    assert "irreversible" in out
    assert "breadth-first ×3" in out
    # Dependents are nested under their dependency, deeper than the root.
    root_indent = out.index("create bronze schema") - out.rindex(
        "\n", 0, out.index("create bronze schema"))
    child_indent = out.index("load raw files") - out.rindex(
        "\n", 0, out.index("load raw files"))
    assert child_indent > root_indent


def test_plan_tree_never_raises_on_malformed_dag():
    ui = _ui()
    ui.plan_tree(None)
    ui.plan_tree(object())
    ui.plan_tree(types.SimpleNamespace(goal="g", nodes=[object(), None]))


def test_text_diff_renders_red_green_lines():
    console = Console(record=True, width=100, force_terminal=True)
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs="unicode", max_render_rows=120)
    )
    ui = DacliUI(settings=settings, console=console)
    ui.text_diff("a\nb\nc\n", "a\nB\nc\n", title="shadow diff")
    plain = ui.console.export_text()
    assert "shadow diff" in plain
    assert "-b" in plain and "+B" in plain


def test_text_diff_identical_inputs_say_no_differences():
    ui = _ui()
    ui.text_diff("same", "same")
    assert "no differences" in ui.console.export_text()


def test_text_diff_never_raises_on_weird_input():
    ui = _ui()
    ui.text_diff(None, None)
    ui.text_diff("", "x" * 100_000)
    ui.text_diff("héllo\x00", b"bytes".decode(errors="ignore"))


# ---------------------------------------------------------------------------
# M5 — governance approval panel
# ---------------------------------------------------------------------------
def _approval_request(**overrides):
    base = {
        "tool_name": "execute_snowflake_query",
        "tier": types.SimpleNamespace(value="irreversible"),
        "classification": types.SimpleNamespace(
            is_prod=True, prod_marker="PROD_DB", reasons=["DROP on orders"]
        ),
        "policy": types.SimpleNamespace(
            decision=types.SimpleNamespace(value="require_approval"),
            source="default",
        ),
        "rollback_plan": types.SimpleNamespace(
            strategy="snapshot restore", primitive="snapshot",
            verify_detail="snapshot taken",
        ),
        "args": {},
        "dry_run_preview": None,
        "shadow": None,
        "cost_estimate": None,
    }
    base.update(overrides)
    request = types.SimpleNamespace(**base)
    request.describe = lambda: "Action      : execute_snowflake_query"
    return request


def test_approval_panel_irreversible_shows_all_five_elements():
    ui = _ui(glyphs="unicode")
    shadow = types.SimpleNamespace(
        ran=True, diff={"before": "rows: 100", "after": "rows: 0"}
    )
    ui.approval_panel(_approval_request(
        dry_run_preview="DROP TABLE orders", shadow=shadow))
    out = ui.console.export_text()
    assert "approval needed" in out
    assert "irreversible" in out                 # blast radius + tier
    assert "PROD: PROD_DB" in out                # prod badge
    assert "DROP on orders" in out               # why
    assert "snapshot restore" in out             # rollback
    assert "snapshot taken" in out               # ...and that it's verified
    assert "DROP TABLE" in out                   # dry-run preview
    assert "-rows: 100" in out and "+rows: 0" in out  # shadow text diff
    assert "[y]es / [N]o" in out                 # decision affordance, N default


def test_approval_panel_write_without_diff_stays_lightweight():
    ui = _ui()
    ui.approval_panel(_approval_request(
        tier=types.SimpleNamespace(value="write"),
        classification=types.SimpleNamespace(
            is_prod=False, prod_marker="", reasons=["INSERT into staging"]),
    ))
    out = ui.console.export_text()
    assert "write" in out
    assert "PROD" not in out
    assert "Shadow" not in out
    assert "[y]es / [N]o" in out


# ---------------------------------------------------------------------------
# M6 — responsive status bar + context gauge + turn header
# ---------------------------------------------------------------------------
def _toolbar_text(ui, **kw):
    from prompt_toolkit.formatted_text import to_formatted_text

    defaults = {
        "provider": "anthropic", "model": "claude-sonnet-4-6",
        "connectors": ["snowflake", "bigquery", "dbt"],
        "ctx_pct": 58, "session": "session_20260613",
    }
    defaults.update(kw)
    tb = ui.bottom_toolbar(**defaults)
    return "".join(t[1] for t in to_formatted_text(tb))


def test_toolbar_ctx_renders_as_real_gauge():
    ui = _ui(glyphs="unicode")
    text = _toolbar_text(ui, ctx_pct=58)
    assert "▰▰▰▱▱ 58%" in text


def test_gauge_is_defensive():
    from dacli.tui.design import UNICODE, gauge

    assert gauge(0, UNICODE) == "▱▱▱▱▱ 0%"
    assert gauge(100, UNICODE) == "▰▰▰▰▰ 100%"
    assert gauge(250, UNICODE) == "▰▰▰▰▰ 100%"   # clamped
    assert gauge(-5, UNICODE) == "▱▱▱▱▱ 0%"      # clamped
    assert gauge("garbage", UNICODE) == "▱▱▱▱▱ 0%"  # never raises


@pytest.mark.parametrize("width", [60, 100, 160])
def test_toolbar_never_exceeds_width(width):
    ui = _ui(width=width)
    text = _toolbar_text(
        ui, width=width, cost="$0.42", test_mode="[TEST mongodb]"
    )
    assert len(text) <= width


def test_toolbar_narrow_collapses_to_essentials():
    ui = _ui(width=60)
    text = _toolbar_text(ui, width=60, cost="$0.42")
    assert "claude-sonnet-4-6" in text     # model survives
    assert "58%" in text                   # gauge survives
    assert "$0.42" in text                 # cost survives
    assert "snowflake" not in text         # connectors dropped
    assert "session_20260613" not in text  # session dropped
    assert "/help" not in text


def test_toolbar_wide_keeps_everything():
    ui = _ui(width=160)
    text = _toolbar_text(ui, width=160, cost="$0.42")
    assert "snowflake" in text
    assert "session_20260613" in text
    assert "/help" in text


def test_toolbar_test_mode_survives_narrow():
    ui = _ui(width=60)
    text = _toolbar_text(ui, width=60, test_mode="[TEST x]")
    assert "[TEST x]" in text


def test_turn_header_renders_rule_with_context():
    ui = _ui()
    ui.turn_header(model="claude-sonnet-4-6", session="sess_1", elapsed="12s")
    out = ui.console.export_text()
    assert "claude-sonnet-4-6" in out
    assert "sess_1" in out and "12s" in out


# ---------------------------------------------------------------------------
# M7 — accessibility, themes, banner restraint
# ---------------------------------------------------------------------------
def test_new_themes_are_registered_and_complete():
    from dacli.tui import THEMES
    from dacli.tui.theme import STYLE_KEYS

    for name in ("nord", "gruvbox", "contrast"):
        assert name in THEMES
        for key in STYLE_KEYS:
            assert key in THEMES[name].styles, f"{name} missing '{key}'"
        assert THEMES[name].code_theme


def test_contrast_theme_has_no_dim_styles():
    # WCAG-minded: "dim" destroys contrast; the high-contrast theme bans it.
    from dacli.tui import THEMES

    for key, style in THEMES["contrast"].styles.items():
        assert "dim" not in style, f"contrast.{key} uses dim"


def test_high_contrast_knob_forces_contrast_theme():
    ui = _ui(high_contrast=True, theme="dark")
    assert ui.theme.name == "contrast"


def test_no_color_knob_disables_color():
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs="ascii", no_color=True, max_render_rows=120)
    )
    ui = DacliUI(settings=settings)  # builds its own console
    assert ui.console.no_color is True


@pytest.mark.parametrize("theme_name", ["nord", "gruvbox", "contrast"])
def test_new_themes_render_a_full_transcript(theme_name):
    console = Console(record=True, width=100, force_terminal=True)
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs="unicode", max_render_rows=120)
    )
    ui = DacliUI(settings=settings, theme_name=theme_name, console=console)
    assert ui.theme.name == theme_name
    out = _drive_transcript(ui)
    assert "execute_query" in out


def test_banner_ascii_mode_is_ascii_safe():
    ui = _ui(glyphs="ascii")
    ui.banner()
    ui.console.export_text().encode("ascii")


def test_banner_compact_on_tiny_terminal():
    console = Console(record=True, width=44, height=10, force_terminal=False)
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs="unicode", max_render_rows=120)
    )
    ui = DacliUI(settings=settings, version="1.0", console=console)
    ui.banner()
    out = ui.console.export_text()
    assert "DACLI" in out
    assert "█" not in out  # the full wordmark stays away from small screens


def test_banner_full_on_capable_terminal():
    ui = _ui(glyphs="unicode", width=100)
    ui.banner()
    assert "█" in ui.console.export_text()


def test_diff_panel_delta_glyph_degrades_to_ascii():
    ui = _ui(glyphs="ascii")
    ui.diff_panel({
        "table_a": "orders_v1", "table_b": "orders_v2",
        "row_count_a": 100, "row_count_b": 90, "row_delta": -10,
        "columns": [{"name": "status", "delta": 0.05,
                     "null_rate_a": 0.01, "null_rate_b": 0.06}],
        "sample": {"rows_compared": 50, "rows_differing": 3},
        "method": "sampled",
    })
    out = ui.console.export_text()
    assert "Δ" not in out          # no unicode delta in ascii mode
    assert "d rows" in out         # ascii delta marker
    out.encode("ascii")            # whole panel is ascii-safe


def test_diff_panel_unicode_keeps_delta_glyph():
    ui = _ui(glyphs="unicode")
    ui.diff_panel({"row_count_a": 1, "row_count_b": 2, "row_delta": 1,
                   "sample": {"rows_compared": 1, "rows_differing": 0}})
    assert "Δ rows" in ui.console.export_text()


def test_approval_panel_border_tracks_tier():
    console = Console(record=True, width=100, force_terminal=True)
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs="unicode", max_render_rows=120)
    )
    ui = DacliUI(settings=settings, console=console)
    ui.approval_panel(_approval_request())  # irreversible
    exported = ui.console.export_text(styles=True)
    # dark theme: error = bold red → the border row carries red.
    top_border = next(line for line in exported.splitlines() if "approval" in line)
    assert "31m" in top_border


# ---------------------------------------------------------------------------
# P05 — welcome card: location legibility + empty-connectors nudge
# ---------------------------------------------------------------------------
def test_welcome_shows_config_and_state_paths():
    ui = _ui(width=100)
    ui.welcome(
        model="m", provider="p", connectors=["snowflake"], cwd="/tmp",
        config="/proj/config.yaml", state="/proj/.dacli",
    )
    out = ui.console.export_text()
    assert "config" in out
    assert "/proj/config.yaml" in out
    assert "/proj/.dacli" in out


def test_welcome_config_none_renders_placeholder():
    ui = _ui(width=100)
    ui.welcome(
        model="m", provider="p", connectors=["snowflake"], cwd="/tmp",
        config=None, state="/proj/.dacli",
    )
    assert "(none)" in ui.console.export_text()


def test_welcome_empty_connectors_nudges_setup():
    ui = _ui(width=100)
    ui.welcome(model="m", provider="p", connectors=[], cwd="/tmp")
    out = ui.console.export_text()
    assert "No connectors yet" in out
    assert "/setup" in out
    assert "/connect" in out


def test_welcome_with_connectors_omits_nudge():
    ui = _ui(width=100)
    ui.welcome(model="m", provider="p", connectors=["snowflake"], cwd="/tmp")
    assert "No connectors yet" not in ui.console.export_text()
