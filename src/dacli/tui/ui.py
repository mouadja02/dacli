"""DACLI terminal UI.

A single :class:`DacliUI` owns *all* presentation for the interactive chat:
the banner, the streaming agent view, the compact tool transcript, the slash
tables and the persistent bottom status bar. ``scripts/cli.py`` stays thin and
just wires the agent callbacks to these methods.

Design goals (best-of from Claude Code / Codex / OpenCode TUIs):
- **Scrollback transcript** — everything is printed as it happens so the native
  terminal history (and copy/paste) keeps working; no alternate screen.
- **Live token streaming** — agent text appears as it is generated, with a
  thinking indicator that shows elapsed time and the current activity.
- **Compact tool calls** — ``⏺ tool(args)`` then ``⎿ ✓ N rows · 340ms`` with the
  result table underneath. Huge results are rendered head+tail (bounded by
  ``ui.max_render_rows``) so the terminal survives a 50k-row SELECT; the
  *data* itself — and the off-context spill — is never truncated.
- **Persistent status bar** — provider·model · connectors · context · session,
  rendered by prompt-toolkit beneath the input.

Reliability: rendering never raises into the control loop. The streaming view
is transient (it leaves the polished markdown behind), so a dropped frame or a
resize can't corrupt the scrollback.
"""

from __future__ import annotations

import time
from typing import Any
from collections.abc import Iterable

from rich.console import Console, Group, RenderableType
from rich.markdown import Markdown
from rich.live import Live
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from dacli.connectors.base import ToolResult
from .design import SPACING, TIER_STYLE, Glyphs, resolve_glyphs
from .theme import ThemeSpec, get_theme
import contextlib

__author__ = ""  # populated by caller if needed

# The spinner glyph shimmers through this short palette for a subtle animation.
_SHIMMER = ("accent", "tool", "info", "ok", "assistant")
_VERBS = (
    "Thinking",
    "Reasoning",
    "Working",
    "Crunching",
    "Cooking",
    "Pondering",
    "Churning",
    "Wrangling",
    "Untangling",
    "Synthesizing",
    "Plotting",
    "Distilling",
)

# Error remediation hints: substring of the error → a one-line next step.
# Deliberately a small lookup (the startup path attaches hints the same way for
# failed connectors); first match wins.
_REMEDIATION_HINTS: tuple[tuple[str, str], ...] = (
    ("not healthy", "Try /debug-connector <name> to diagnose, or /connect to reconfigure."),
    ("health check", "Try /debug-connector <name> to diagnose, or /connect to reconfigure."),
    ("decrypt", "Stored secrets could not be read - re-enter them via /connect."),
    ("unknown tool", "See /tools for what's enabled; /setup to enable more."),
    ("blocked by governance", "See /audit for the decision; adjust config/policy.yaml if intended."),
    ("permission denied", "Scope too narrow - see /audit; widen it in config/policy.yaml if intended."),
    ("unauthorized", "Credentials look invalid - update them via /connect."),
    ("forbidden", "Credentials look invalid - update them via /connect."),
    ("401", "Credentials look invalid - update them via /connect."),
    ("403", "Credentials look invalid - update them via /connect."),
    ("rate limit", "Provider rate limit - wait a moment and retry."),
    ("429", "Provider rate limit - wait a moment and retry."),
    ("timed out", "The platform didn't answer in time - check connectivity, then retry."),
)


def _remediation_hint(message: Any) -> str | None:
    """First matching one-line suggestion for a rendered error, or None."""
    try:
        msg = str(message or "").lower()
    except Exception:
        return None
    return next(
        (hint for needle, hint in _REMEDIATION_HINTS if needle in msg), None
    )


class StreamView:
    """A transient Rich ``Live`` region for one LLM ``generate`` call.

    While the model is producing tokens this shows a thinking spinner (with
    elapsed time + current activity) and then the streaming text — rendered as
    *live Markdown* so headings, lists and code blocks appear formatted while
    the model types. On :meth:`end` it tears the live region down and
    re-prints the completed text as polished markdown so it stays in the
    scrollback.

    Reliability: the markdown pass is throttled (re-parsed only on a newline
    or every ``_MD_REPARSE_CHARS`` chars), trial-rendered off-screen, and on
    any failure the view falls back to the plain-text stream for the rest of
    the turn. Reduced motion always streams plain text.
    """

    # Re-parse markdown when this many chars arrived since the last parse
    # (a newline always triggers a re-parse). Parsing per token is too costly.
    _MD_REPARSE_CHARS = 40

    def __init__(self, ui: DacliUI):
        self._ui = ui
        self._live: Live | None = None
        self._buffer = ""
        self._start = 0.0
        # Incremental-markdown state (reset per turn).
        self._md_cache: RenderableType | None = None
        self._md_seen = 0
        self._md_failed = False

    @property
    def active(self) -> bool:
        return self._live is not None

    def begin(self) -> None:
        self._buffer = ""
        self._start = time.monotonic()
        self._md_cache = None
        self._md_seen = 0
        self._md_failed = False
        self._live = Live(
            self,
            console=self._ui.console,
            refresh_per_second=12,
            transient=True,
            auto_refresh=True,
        )
        self._live.start()

    def feed(self, delta: str) -> None:
        if not delta:
            return
        self._buffer += delta
        if self._live:
            self._live.refresh()

    def end(self, content: str | None = None) -> None:
        text = content if content is not None else self._buffer
        self._teardown()
        if text and text.strip():
            self._ui.agent_message(text)

    def abort(self) -> None:
        self._teardown()

    def _teardown(self) -> None:
        if self._live:
            with contextlib.suppress(Exception):
                self._live.stop()
            self._live = None
        self._buffer = ""

    def __rich__(self) -> RenderableType:
        elapsed = time.monotonic() - self._start
        glyphs = self._ui.glyphs
        if not self._buffer:
            reduced = self._ui.reduced_motion
            frames = glyphs.spinner_frames
            frame = frames[0] if reduced else frames[int(elapsed * 10) % len(frames)]
            verb = _VERBS[0] if reduced else _VERBS[int(elapsed / 3) % len(_VERBS)]
            shimmer = "accent" if reduced else _SHIMMER[int(elapsed * 3) % len(_SHIMMER)]
            line = Text()
            line.append(f"{frame} ", style=f"bold {shimmer}")
            line.append(f"{verb}", style="assistant")
            # Animated trailing dots so the line breathes even mid-token-wait.
            dots = "." if reduced else "." * (1 + int(elapsed * 2) % 3)
            line.append(f"{dots} ", style="muted")
            if self._ui.activity:
                line.append(f"{self._ui.activity} ", style="muted")
            line.append(
                f"({elapsed:.0f}s {glyphs.dot} ctrl-c to interrupt)", style="muted"
            )
            return line
        markdown = self._streaming_markdown()
        if markdown is not None:
            return markdown
        body = Text(f"{glyphs.agent} ", style="gutter")
        body.append(self._buffer, style="assistant")
        body.append(glyphs.caret, style="accent")
        return body

    def _streaming_markdown(self) -> RenderableType | None:
        """Formatted view of the partial buffer, or None to stream plain text.

        Throttled: the buffer is re-parsed only when a newline arrived or it
        grew by ``_MD_REPARSE_CHARS`` since the last parse; otherwise the
        cached renderable is reused. A half-open code fence gets a synthetic
        closing fence *for the render pass only* so it never swallows the
        rest of the answer. Any parse/render failure permanently falls back
        to plain text for this turn — never raises into the live region.
        """
        if self._md_failed or self._ui.reduced_motion:
            return None
        grown = len(self._buffer) - self._md_seen
        fresh = self._buffer[self._md_seen:]
        if (
            self._md_cache is not None
            and grown < self._MD_REPARSE_CHARS
            and "\n" not in fresh
        ):
            return self._md_cache
        try:
            text = self._buffer + self._ui.glyphs.caret
            if self._buffer.count("```") % 2 == 1:
                # Balance the open fence for this render pass only — the real
                # buffer is never mutated.
                text += "\n```"
            markdown = Markdown(text, code_theme=self._ui.theme.code_theme)
            view = self._ui._guttered(self._ui.glyphs.agent, "gutter", markdown)
            # Trial-render off-screen so a pathological buffer can never raise
            # inside the Live refresh thread.
            self._ui.console.render_lines(
                view, self._ui.console.options, pad=False
            )
        except Exception:
            self._md_failed = True
            return None
        self._md_cache = view
        self._md_seen = len(self._buffer)
        return view


class DacliUI:
    """All terminal presentation for the interactive session."""

    def __init__(
        self,
        settings: Any = None,
        theme_name: str | None = None,
        version: str = "",
        author: str = "",
        console: Console | None = None,
    ):
        self.settings = settings
        self.version = version
        self.author = author
        ui_settings = getattr(settings, "ui", None)
        if getattr(ui_settings, "high_contrast", False):
            # Accessibility override: force the high-contrast palette.
            self.theme: ThemeSpec = get_theme("contrast")
        else:
            self.theme = get_theme(
                theme_name or getattr(ui_settings, "theme", None)
            )
        if console is None:
            # Rich honors the NO_COLOR env var natively; ui.no_color forces it.
            no_color = True if getattr(ui_settings, "no_color", False) else None
            self.console = Console(theme=self.theme.rich_theme(), no_color=no_color)
        else:
            # A caller-provided console may not know our semantic styles — push
            # the theme so 'border', 'accent', … resolve everywhere.
            self.console = console
            self.console.push_theme(self.theme.rich_theme())
        self.glyphs: Glyphs = resolve_glyphs(self.console, settings)
        self.reduced_motion: bool = bool(getattr(ui_settings, "reduced_motion", False))
        self.stream = StreamView(self)
        self.activity = ""  # current background activity, shown in the spinner
        # Transient liveness line for a long-running tool (see tool_progress).
        self._progress_live: Live | None = None

    # ------------------------------------------------------------------
    # Theme
    # ------------------------------------------------------------------
    def set_theme(self, name: str) -> bool:
        """Switch theme live. Returns True if ``name`` was a known theme."""
        spec = get_theme(name)
        self.theme = spec
        self.console.push_theme(spec.rich_theme())
        return spec.name == (name or "").strip().lower()

    # ------------------------------------------------------------------
    # Banner / welcome
    # ------------------------------------------------------------------
    def banner(self) -> None:
        art = [
            "██████╗   █████╗   ██████╗ ██╗      ██╗",
            "██╔══██╗ ██╔══██╗ ██╔════╝ ██║      ██║",
            "██║  ██║ ███████║ ██║      ██║      ██║",
            "██║  ██║ ██╔══██║ ██║      ██║      ██║",
            "██████╔╝ ██║  ██║ ╚██████╗ ███████╗ ██║",
            "╚═════╝  ╚═╝  ╚═╝  ╚═════╝ ╚══════╝ ╚═╝",
        ]
        # Pick a different color (solid or gradient) for each run from a predefined set
        gradient = self.theme.banner_gradients
        lines = Text()
        for i, row in enumerate(art):
            lines.append(row + "\n", style=gradient[i % len(gradient)])
        tagline = Text("Your specialized data-engineering CLI agent", style="muted")
        meta = Text()
        if self.version:
            meta.append(f"v{self.version}", style="accent")
        if self.author:
            meta.append(f"  {self.glyphs.dot}  {self.author}", style="muted")
        body = (
            Group(lines, tagline, meta)
            if (self.version or self.author)
            else Group(lines, tagline)
        )
        self.console.print(Padding(body, (1, 2, 0, 2)))

    def welcome(
        self, *, model: str, provider: str, connectors: list[str], cwd: str
    ) -> None:
        """A compact 'session ready' card with the essentials + quick tips."""
        info = Text()
        info.append("model      ", style="muted")
        info.append(f"{provider}{self.glyphs.dot}{model}\n", style="accent")
        info.append("connectors ", style="muted")
        info.append(
            (", ".join(connectors) if connectors else "none") + "\n", style="info"
        )
        info.append("cwd        ", style="muted")
        info.append(cwd, style="info")

        tips = Text()
        tips.append("\n")
        tips.append("/", style="accent")
        tips.append(" for commands   ", style="muted")
        tips.append(self.glyphs.arrows, style="accent")
        tips.append(" history   ", style="muted")
        tips.append("Tab", style="accent")
        tips.append(" complete   ", style="muted")
        tips.append("ctrl-c", style="accent")
        tips.append(" interrupt", style="muted")

        self.console.print(
            Panel(
                Group(info, tips),
                title=f"[success]{self.glyphs.ok} session ready[/success]",
                title_align="left",
                box=self.glyphs.box,
                border_style="accent",
                padding=SPACING["panel_pad"],
            )
        )

    # ------------------------------------------------------------------
    # User input — a distinct, framed 'you' entry vs dacli's ⏺ output
    # ------------------------------------------------------------------
    def prompt_html(self):
        """The prompt-toolkit prompt for the user's input line.

        Renders a colored left bar + ``you`` label so the human's turns are
        visually distinct from dacli's ``⏺`` agent output. Uses the theme's
        toolbar color (always a valid prompt-toolkit hex), falling back to a
        named ansi color so a custom theme can never crash the input loop.
        """
        from prompt_toolkit.formatted_text import HTML

        c = self.theme.toolbar_fg if _valid_pt_color(self.theme.toolbar_fg) else "ansicyan"
        return HTML(
            f'<style fg="{c}"><b>{self.glyphs.caret}</b></style> <b>you</b> '
            f'<style fg="{c}">{self.glyphs.user_caret}</style> '
        )

    def user_message(self, text: str) -> None:
        """Echo a user message as a distinct, bordered 'you' panel.

        Used by the history view (and available for transcript echo). The live
        input loop relies on :meth:`prompt_html` instead, so the typed line is
        not duplicated.
        """
        body = Text(text.strip(), style="user")
        self.console.print(
            Panel(
                body,
                title=f"[accent]{self.glyphs.caret} you[/accent]",
                title_align="left",
                box=self.glyphs.box,
                border_style="border",
                padding=(0, 1),
            )
        )

    def clear_screen(self, header: str = "") -> None:
        """Clear the terminal viewport (like PowerShell ``clear``/``cls``).

        Wipes the visible scrollback but keeps conversation history/state. An
        optional one-line ``header`` is reprinted so the screen isn't blank.
        """
        self.console.clear()
        if header:
            self.console.print(f"[muted]{header}[/muted]\n")

    # ------------------------------------------------------------------
    # Transcript primitives
    # ------------------------------------------------------------------
    def _guttered(
        self,
        marker: str,
        marker_style: str,
        renderable: RenderableType,
        indent: int = 0,
    ) -> Table:
        grid = Table.grid(padding=(0, 1, 0, 0))
        grid.add_column(width=1 + indent, justify="right")
        grid.add_column(ratio=1)
        grid.add_row(Text(marker, style=marker_style), renderable)
        return grid

    def agent_message(self, content: str) -> None:
        """Print a finished agent turn as guttered markdown."""
        self.console.print(
            self._guttered(
                self.glyphs.agent,
                "gutter",
                Markdown(content, code_theme=self.theme.code_theme),
            )
        )
        self.console.print()

    def _notice_icon(self, style: str) -> str:
        # Leading icon per notice style — success/warning/error read at a
        # glance without reading the text (and without relying on color).
        return {
            "success": self.glyphs.ok,
            "warning": self.glyphs.warn,
            "error": self.glyphs.err,
            "bad": self.glyphs.err,
            "info": self.glyphs.info,
        }.get(style, "")

    def notice(self, message: str, style: str = "info") -> None:
        icon = self._notice_icon(style)
        prefix = f"{icon} " if icon else ""
        self.console.print(f"[{style}]{prefix}{message}[/{style}]")

    def error(self, message: str) -> None:
        self._clear_progress()
        self.console.print(
            self._guttered(self.glyphs.err, "bad", Text(message, style="error"))
        )
        hint = _remediation_hint(message)
        if hint:
            self.console.print(
                Padding(
                    Text(f"{self.glyphs.hint} {hint}", style="muted"),
                    (0, 0, 0, SPACING["indent"]),
                )
            )

    def status(self, message: str) -> None:
        """Background activity from the kernel.

        While a turn is streaming this feeds the thinking line; outside a turn
        (e.g. during initialization) it prints a dim line — except the noisy
        per-iteration counter, which only ever updates the spinner.
        """
        self.activity = message
        if self.stream.active:
            return
        if message.startswith("Iteration"):
            return
        # Keep continuation lines aligned under the gutter indent.
        indented = message.replace("\n", "\n  ")
        self.console.print(f"  [muted]{indented}[/muted]")

    # ------------------------------------------------------------------
    # Streaming hooks (wired into the kernel)
    # ------------------------------------------------------------------
    def on_stream_start(self) -> None:
        self._clear_progress()  # only one live region may own the console
        self.stream.begin()

    def on_text(self, delta: str) -> None:
        self.stream.feed(delta)

    def on_stream_end(self, content: str) -> None:
        self.stream.end(content)

    # ------------------------------------------------------------------
    # Tool liveness (optional on_tool_progress callback)
    # ------------------------------------------------------------------
    def tool_progress(self, tool_name: str, message: str) -> None:
        """Liveness for a long-running tool: one transient status line.

        Polling connectors (Airflow/Dagster) report each iteration here so a
        minutes-long run doesn't sit behind a static tool card. The line is
        transient — ``tool_end`` clears it, keeping the scrollback clean. While
        the streaming spinner owns the live region, the message feeds it
        instead. Never raises into the control loop.
        """
        try:
            if self.stream.active:
                self.activity = f"{tool_name}: {message}"
                return
            frames = self.glyphs.spinner_frames
            frame = (
                frames[0]
                if self.reduced_motion
                else frames[int(time.monotonic() * 10) % len(frames)]
            )
            line = Text()
            line.append(f"  {frame} ", style="tool")
            line.append(f"{tool_name} ", style="muted")
            line.append(str(message), style="step")
            if self._progress_live is None:
                self._progress_live = Live(
                    line,
                    console=self.console,
                    transient=True,
                    refresh_per_second=8,
                )
                self._progress_live.start()
            else:
                self._progress_live.update(line)
        except Exception:
            self._clear_progress()

    def _clear_progress(self) -> None:
        live, self._progress_live = self._progress_live, None
        if live is not None:
            with contextlib.suppress(Exception):
                live.stop()

    # ------------------------------------------------------------------
    # Tool transcript
    # ------------------------------------------------------------------
    def tool_start(self, tool_name: str, args: dict[str, Any]) -> None:
        self._clear_progress()
        header = Text(tool_name, style="tool")
        preview = _arg_preview(args, ellipsis=self.glyphs.ellipsis)
        if preview:
            header.append(f"  {preview}", style="muted")
        self.console.print(self._guttered(self.glyphs.tool, "tool", header))

        sql = args.get("query") or args.get("sql")
        if isinstance(sql, str) and sql.strip():
            syntax = Syntax(
                sql.strip(),
                "sql",
                theme=self.theme.code_theme,
                word_wrap=True,
                background_color="default",
            )
            self.console.print(Padding(syntax, (0, 0, 0, 2 * SPACING["indent"])))

    def _render_cap(self) -> int:
        # How many rows/items/fields the transcript renders before head+tail
        # elision kicks in. Bounded out of the box; settings may widen it.
        cap = getattr(getattr(self.settings, "ui", None), "max_render_rows", None)
        return cap if isinstance(cap, int) and cap > 0 else 120

    def tool_end(self, tool_name: str, result: Any) -> None:
        self._clear_progress()
        if not isinstance(result, ToolResult):
            self.console.print(
                Padding(
                    self._guttered(
                        self.glyphs.result, "muted", Text(str(result), style="step")
                    ),
                    (0, 0, 0, SPACING["indent"]),
                )
            )
            return

        if not result.success:
            summary = Text()
            summary.append(f"{self.glyphs.err} ", style="bad")
            summary.append(str(result.error or "failed"), style="error")
            self.console.print(
                Padding(
                    self._guttered(self.glyphs.result, "bad", summary),
                    (0, 0, 0, SPACING["indent"]),
                )
            )
            hint = _remediation_hint(result.error)
            if hint:
                self.console.print(
                    Padding(
                        Text(f"{self.glyphs.hint} {hint}", style="muted"),
                        (0, 0, 0, 2 * SPACING["indent"]),
                    )
                )
            self.console.print()
            return

        summary = Text()
        summary.append(f"{self.glyphs.ok} ", style="ok")
        data = result.data
        body: RenderableType | None = None
        cap = self._render_cap()
        gap = self.glyphs.ellipsis
        # Rail color = semantics: a write/risky/irreversible action keeps its
        # blast-radius color on the result rail (tagged by the dispatcher);
        # plain reads stay calm. Errors use the error rail above.
        tier = (result.metadata or {}).get("tier")
        rail = TIER_STYLE.get(tier, "muted") if tier != "safe" else "muted"

        if isinstance(data, list) and data and isinstance(data[0], dict):
            summary.append(
                f"{len(data)} row{'s' if len(data) != 1 else ''}", style="success"
            )
            body = _rows_table(data, max_rows=cap, ellipsis=gap)
        elif isinstance(data, list):
            summary.append(
                f"{len(data)} item{'s' if len(data) != 1 else ''}", style="success"
            )
            if data:
                indexed, footer = _capped_indexed(data, cap, "items", ellipsis=gap)
                listing = Text(
                    "\n".join(
                        gap if v is _GAP else f"{i}. {_cell(v)}" for i, v in indexed
                    ),
                    style="step",
                )
                body = Group(listing, footer) if footer else listing
        elif isinstance(data, dict):
            summary.append(
                f"{len(data)} field{'s' if len(data) != 1 else ''}", style="success"
            )
            indexed, footer = _capped_indexed(
                list(data.items()), cap, "fields", ellipsis=gap
            )
            kv = Table.grid(padding=(0, 2, 0, 0))
            kv.add_column(style="muted", no_wrap=True)
            kv.add_column(style="step", overflow="fold")
            for _i, item in indexed:
                if item is _GAP:
                    kv.add_row(gap, gap)
                    continue
                k, v = item
                kv.add_row(str(k), _compact_preview(v, ellipsis=gap))
            body = Group(kv, footer) if footer else kv
        elif data is None:
            summary.append("done", style="success")
        else:
            summary.append("done", style="success")
            body = Text(_cell(data), style="step")

        summary.append(
            f"  {self.glyphs.dot}  {result.execution_time_ms:.0f}ms", style="muted"
        )
        self.console.print(
            Padding(
                self._guttered(self.glyphs.result, rail, summary),
                (0, 0, 0, SPACING["indent"]),
            )
        )
        if body is not None:
            self.console.print(Padding(body, (0, 0, 0, 2 * SPACING["indent"])))
        self.console.print()

    # ------------------------------------------------------------------
    # Approval / plan rendering (governance sign-off)
    # ------------------------------------------------------------------
    def approval_panel(self, request) -> None:
        """Render a governance approval request (or a DAG plan) for sign-off.

        Structured when the request carries a dry-run preview / shadow diff or
        is a plan; plain text otherwise. A malformed request can never raise —
        it falls back to ``describe()`` text, then to ``str()``.
        """
        tier = getattr(getattr(request, "tier", None), "value", "?")
        # Border weight follows blast radius: irreversible screams, a safe
        # action stays lightweight. The strongest tier always wins visually.
        border = {
            "irreversible": "error",
            "risky": "warning",
            "write": "info",
            "safe": "border",
        }.get(tier, "warning")
        try:
            body = self._approval_body(request, tier)
        except Exception:
            describe = getattr(request, "describe", None)
            try:
                text = str(describe()) if callable(describe) else str(request)
            except Exception:
                text = str(request)
            body = Text(text, style="step")

        decision = Text()
        decision.append("Proceed?  ", style="bold" if tier == "irreversible" else "prompt")
        decision.append("[y]es / [N]o", style="accent")
        decision.append("  (No is the safe default)", style="muted")

        tier_style = TIER_STYLE.get(tier, "muted")
        self.console.print(
            Panel(
                Group(body, Text(), decision),
                title=(
                    f"[{border}]approval needed[/{border}] {self.glyphs.dot} "
                    f"[{tier_style}]{tier}[/{tier_style}]"
                ),
                title_align="left",
                box=self.glyphs.box,
                border_style=border,
                padding=SPACING["panel_pad"],
            )
        )

    def _approval_body(self, request, tier: str) -> RenderableType:
        describe = getattr(request, "describe", None)
        if not callable(describe):
            # A DAG plan (plan-approve-execute) renders its inspectable text.
            render = getattr(request, "render", None)
            if callable(render):
                return Text(str(render()), style="step")
            return Text(str(request), style="step")

        grid = Table.grid(padding=(0, 2, 0, 0))
        grid.add_column(style="muted", no_wrap=True)
        grid.add_column()
        grid.add_row(
            "Action", Text(str(getattr(request, "tool_name", "?")), style="accent")
        )
        tier_text = Text(tier, style=TIER_STYLE.get(tier, "muted"))
        cls = getattr(request, "classification", None)
        if getattr(cls, "is_prod", False):
            tier_text.append(f"  (PROD: {cls.prod_marker})", style="error")
        grid.add_row("Blast radius", tier_text)
        reasons = "; ".join(getattr(cls, "reasons", None) or [])
        if reasons:
            grid.add_row("Why", Text(reasons, style="step"))
        policy = getattr(request, "policy", None)
        if policy is not None:
            decision = getattr(getattr(policy, "decision", None), "value", "?")
            source = getattr(policy, "source", "?")
            grid.add_row("Decision", Text(f"{decision}  [{source}]", style="step"))
        plan = getattr(request, "rollback_plan", None)
        if plan is not None:
            rollback = Text(str(getattr(plan, "strategy", "?")), style="step")
            if getattr(plan, "primitive", None) not in ("noop", "none", None):
                rollback.append(
                    f"  (verified: {getattr(plan, 'verify_detail', '')})",
                    style="muted",
                )
            grid.add_row("Rollback", rollback)
        estimate = getattr(request, "cost_estimate", None)
        if estimate:
            bits = []
            if estimate.get("bytes") is not None:
                bits.append(f"{estimate['bytes']:,} bytes scanned")
            if estimate.get("credits") is not None:
                bits.append(f"{estimate['credits']} credits")
            if estimate.get("usd") is not None:
                bits.append(f"≈ ${estimate['usd']:,.2f}")
            grid.add_row("Est. cost", Text("  ·  ".join(bits), style="warning"))

        parts: list[RenderableType] = [grid]
        preview = getattr(request, "dry_run_preview", None)
        if preview:
            parts.append(Text("Dry-run preview", style="muted"))
            parts.append(
                Syntax(
                    str(preview).strip(),
                    "sql",
                    theme=self.theme.code_theme,
                    word_wrap=True,
                    background_color="default",
                )
            )
        shadow = getattr(request, "shadow", None)
        if shadow is not None and getattr(shadow, "ran", False):
            diff = getattr(shadow, "diff", None) or {}
            if "before" in diff or "after" in diff:
                # Textual before/after from the shadow run → red/green diff
                # (the same renderer `dacli diff` and dry-run previews use).
                parts.append(Text("Shadow diff (on a clone)", style="muted"))
                parts.append(
                    _unified_diff_text(
                        str(diff.get("before", "")), str(diff.get("after", ""))
                    )
                )
            elif "rows_before" in diff and "rows_after" in diff:
                parts.append(self._shadow_delta_table(diff))
            else:
                parts.append(Text(f"Shadow: {shadow.summary()}", style="step"))
        return Group(*parts) if len(parts) > 1 else parts[0]

    @staticmethod
    def _shadow_delta_table(diff: dict[str, Any]) -> Table:
        # Tiny before/after table for a shadow row-count delta.
        delta = diff.get("row_delta")
        if delta is None:
            try:
                delta = diff["rows_after"] - diff["rows_before"]
            except Exception:
                delta = "?"
        table = Table(
            title="[muted]Shadow run (on a clone)[/muted]",
            title_justify="left",
            show_header=True,
            header_style="muted",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("rows before", justify="right", style="info")
        table.add_column("rows after", justify="right", style="info")
        table.add_column("Δ", justify="right", style="accent")
        table.add_row(str(diff["rows_before"]), str(diff["rows_after"]), str(delta))
        return table

    def plan_panel(self, preview) -> None:
        """Render a static plan + governance preview (`dacli plan`).

        One row per DAG step: tier (blast radius), the policy decision that
        would fire, and the rollback primitive that would be attached. Nothing
        here executes — it is the inspectable plan-approve-execute front half.
        """
        table = Table(
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("#", justify="right", style="muted")
        table.add_column("step", style="step", overflow="fold")
        table.add_column("tier", no_wrap=True)
        table.add_column("decision", style="info", no_wrap=True)
        table.add_column("rollback", style="muted", overflow="fold")

        needs_approval = 0
        for i, step in enumerate(preview.steps, 1):
            tier = getattr(step.tier, "value", str(step.tier))
            tier_text = Text(tier, style=TIER_STYLE.get(tier, "muted"))
            cls = step.classification
            if getattr(cls, "is_prod", False):
                tier_text.append(f" (PROD: {cls.prod_marker})", style="error")

            desc = Text(step.node.description)
            deps = step.node.depends_on
            if deps:
                desc.append(f"  (after {', '.join(deps)})", style="muted")
            if step.node.breadth_first:
                desc.append(
                    f"  [breadth-first ×{len(step.node.items) or '?'}]", style="info"
                )

            decision = getattr(step.policy.decision, "value", "?")
            if step.policy.requires_human:
                needs_approval += 1
                decision += " — needs approval"

            rollback = step.rollback
            if rollback.primitive == "noop":
                undo = "nothing to undo (read-only)"
            elif rollback.available:
                undo = rollback.primitive
                if not rollback.verified:
                    undo += " (verified at execution)"
            else:
                undo = "no native undo — would be refused unless verified"
            if step.platform:
                undo += f" · {step.platform}"

            table.add_row(str(i), desc, tier_text, decision, undo)

        summary = Text()
        summary.append(f"{len(preview.steps)} step(s)", style="info")
        summary.append("  ·  ", style="muted")
        if needs_approval:
            summary.append(f"{needs_approval} need(s) approval", style="warning")
        else:
            summary.append("no approvals needed", style="success")
        summary.append("  ·  nothing was executed", style="muted")

        self.console.print(
            Panel(
                Group(Text(preview.goal, style="accent"), Text(), table, Text(), summary),
                title=(
                    f"[accent]plan preview[/accent] {self.glyphs.dot} "
                    "[muted]dry run, no execution[/muted]"
                ),
                title_align="left",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )

    # ------------------------------------------------------------------
    # Long-running-work feedback (M4): progress, plan tree, text diff
    # ------------------------------------------------------------------
    @contextlib.contextmanager
    def progress(self, description: str, total: int | None = None):
        """Reusable progress for multi-step work (init fan-out, dbt, polls).

        Yields an ``advance(label)`` callable. With a known ``total`` it draws
        a real bar; without one, an indeterminate spinner line. Under
        ``reduced_motion`` it prints static ``step n/total`` lines instead —
        no animation. Rendering failures degrade to silence; the context
        manager itself never raises into the caller.
        """
        if self.reduced_motion:
            done = {"n": 0}

            def advance_static(label: str = "") -> None:
                done["n"] += 1
                of = f"/{total}" if total else ""
                suffix = f"  {label}" if label else ""
                with contextlib.suppress(Exception):
                    self.console.print(
                        f"  [muted]step {done['n']}{of}[/muted]"
                        f"[step]{suffix}[/step]"
                    )

            with contextlib.suppress(Exception):
                self.console.print(f"  [muted]{description}[/muted]")
            yield advance_static
            return

        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TaskProgressColumn,
            TextColumn,
            TimeElapsedColumn,
        )

        columns: list[Any] = [
            SpinnerColumn(style="accent"),
            TextColumn("[step]{task.description}[/step]"),
        ]
        if total is not None:
            columns += [BarColumn(), TaskProgressColumn()]
        columns.append(TimeElapsedColumn())

        progress = Progress(*columns, console=self.console, transient=True)
        task_id = None
        try:
            self._clear_progress()  # one live region owns the console
            progress.start()
            task_id = progress.add_task(description, total=total)
        except Exception:
            progress = None

        def advance(label: str = "") -> None:
            if progress is None or task_id is None:
                return
            with contextlib.suppress(Exception):
                progress.advance(task_id)
                if label:
                    progress.update(task_id, description=label)

        try:
            yield advance
        finally:
            if progress is not None:
                with contextlib.suppress(Exception):
                    progress.stop()

    def plan_tree(self, dag: Any) -> None:
        """Render a :class:`TaskDAG` as a status tree (reused by `dacli plan`).

        Per node: a status icon (pending/running/done/paused/failed, color
        paired with the glyph), its dependencies, and the irreversible /
        breadth-first markers in the shared tier palette. Malformed DAGs
        render best-effort — never raise.
        """
        from rich.tree import Tree

        g = self.glyphs
        try:
            goal = str(getattr(dag, "goal", "") or "plan")
            nodes = list(getattr(dag, "nodes", None) or [])
        except Exception:
            self.console.print(Text(str(dag), style="step"))
            return

        icon_style = {
            "pending": (g.pending, "muted"),
            "running": (g.running, "info"),
            "completed": (g.ok, "success"),
            "failed": (g.err, "error"),
            "paused": (g.paused, "warning"),
        }
        tree = Tree(Text(goal, style="accent"), guide_style="border")
        branches: dict[str, Any] = {}
        for node in nodes:
            try:
                status = getattr(getattr(node, "status", None), "value", None) or str(
                    getattr(node, "status", "pending")
                )
                icon, style = icon_style.get(status, (g.pending, "muted"))
                label = Text()
                label.append(f"{icon} ", style=style)
                label.append(str(getattr(node, "description", node)), style="step")
                deps = list(getattr(node, "depends_on", None) or [])
                if getattr(node, "irreversible", False):
                    label.append("  irreversible", style=TIER_STYLE["irreversible"])
                if getattr(node, "breadth_first", False):
                    items = list(getattr(node, "items", None) or [])
                    label.append(
                        f"  [breadth-first ×{len(items) or '?'}]", style="info"
                    )
                if len(deps) > 1:
                    label.append(
                        f"  (also after {', '.join(deps[1:])})", style="muted"
                    )
                parent = branches.get(deps[0]) if deps else tree
                branch = (parent if parent is not None else tree).add(label)
                node_id = str(getattr(node, "id", "") or "")
                if node_id:
                    branches[node_id] = branch
            except Exception:
                continue
        self.console.print(tree)
        self.console.print()

    def text_diff(
        self,
        before: str,
        after: str,
        *,
        title: str = "diff",
        from_label: str = "before",
        to_label: str = "after",
    ) -> None:
        """Render a red/green unified diff panel (shadow previews, `dacli diff`)."""
        body = _unified_diff_text(
            before, after, from_label=from_label, to_label=to_label
        )
        self.console.print(
            Panel(
                body,
                title=f"[accent]{title}[/accent]",
                title_align="left",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )

    def diff_panel(self, data: dict[str, Any]) -> None:
        """Render a data-diff result (`dacli diff` / the data-diff skill)."""
        data = data or {}
        a, b = data.get("row_count_a", "?"), data.get("row_count_b", "?")
        delta = data.get("row_delta", "?")

        counts = Table(
            show_header=True, header_style="muted", box=None, padding=(0, 2, 0, 0)
        )
        counts.add_column(data.get("table_a", "a"), justify="right", style="info")
        counts.add_column(data.get("table_b", "b"), justify="right", style="info")
        counts.add_column("Δ rows", justify="right", style="accent")
        counts.add_row(str(a), str(b), str(delta))

        parts: list[RenderableType] = [counts]
        changed = [
            c for c in (data.get("columns") or []) if c.get("delta")
        ]
        if changed:
            cols = Table(
                title="[muted]null-rate deltas (sampled)[/muted]",
                title_justify="left",
                show_header=True, header_style="muted", box=None,
                padding=(0, 2, 0, 0),
            )
            cols.add_column("column", style="step")
            cols.add_column("null% a", justify="right", style="info")
            cols.add_column("null% b", justify="right", style="info")
            cols.add_column("Δ", justify="right", style="warning")
            for c in changed:
                cols.add_row(
                    str(c.get("name")),
                    f"{c.get('null_rate_a', 0):.1%}",
                    f"{c.get('null_rate_b', 0):.1%}",
                    f"{c.get('delta', 0):+.1%}",
                )
            parts.append(cols)

        sample = data.get("sample") or {}
        summary = Text()
        summary.append(
            f"sample: {sample.get('rows_compared', 0)} row(s) compared, ",
            style="muted",
        )
        differing = sample.get("rows_differing", 0)
        summary.append(
            f"{differing} differing",
            style="warning" if differing else "success",
        )
        parts.append(summary)
        if data.get("method"):
            parts.append(Text(str(data["method"]), style="muted"))

        self.console.print(
            Panel(
                Group(*parts),
                title=(
                    f"[accent]data diff[/accent] {self.glyphs.dot} "
                    "[muted]read-only[/muted]"
                ),
                title_align="left",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )

    # ------------------------------------------------------------------
    # Slash-command tables
    # ------------------------------------------------------------------
    def help(self, commands: Iterable) -> None:
        table = Table(
            title="[accent]Commands[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Command", style="accent", no_wrap=True)
        table.add_column("Description", style="step")
        for cmd, desc in commands:
            table.add_row(cmd, desc)
        self.console.print(table)
        self.console.print()

    def keys_panel(self) -> None:
        """`/keys`: the TUI keybinding map, so shortcuts are discoverable."""
        table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
        table.add_column("Key", style="accent", no_wrap=True)
        table.add_column("Action", style="step")
        for key, action in (
            ("Tab / Shift-Tab", "Open / step through slash-command completions"),
            (self.glyphs.arrows, "Browse input history"),
            ("Ctrl-R", "Reverse-search input history"),
            ("Ctrl-C", "Interrupt the running turn"),
            ("Enter", "Send the message"),
            ("paste", "Pasted text keeps its newlines (multiline message)"),
            ("/help", "List all slash commands"),
        ):
            table.add_row(key, action)
        self.console.print(
            Panel(
                table,
                title="[accent]Keyboard shortcuts[/accent]",
                title_align="left",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )
        self.console.print()

    def connectors_table(self, registry) -> None:
        table = Table(
            title="[accent]Connectors[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Connector", style="info")
        table.add_column("Status", justify="left")
        table.add_column("Operations", justify="right", style="step")
        for connector_id, info in registry.get_catalog().items():
            if registry.is_connector_enabled(connector_id):
                ops = [
                    op for op in info["operations"] if registry.is_operation_enabled(op)
                ]
                table.add_row(
                    f"{info['icon']} {info['name']}",
                    f"[ok]{self.glyphs.enabled} enabled[/ok]",
                    str(len(ops)),
                )
            else:
                table.add_row(
                    f"{info['icon']} {info['name']}",
                    f"[muted]{self.glyphs.disabled} disabled[/muted]",
                    self.glyphs.dash,
                )
        self.console.print(table)
        self.console.print("[muted]Use /setup to reconfigure connectors[/muted]\n")

    def config_table(self, settings) -> None:
        table = Table(
            title="[accent]Configuration[/accent]",
            show_header=False,
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Setting", style="muted")
        table.add_column("Value", style="info")
        table.add_row("LLM provider", settings.llm.provider)
        table.add_row("LLM model", settings.llm.model)
        table.add_row("Theme", self.theme.name)
        table.add_row("Memory window", str(settings.agent.memory_window))
        table.add_row("Max iterations", str(settings.agent.max_iterations))
        self.console.print(table)
        self.console.print()

    def sessions_table(self, sessions: list[dict[str, Any]], limit: int = 10) -> None:
        if not sessions:
            self.console.print("[muted]No sessions found.[/muted]\n")
            return
        table = Table(
            title="[accent]Sessions[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Session", style="accent", no_wrap=True)
        table.add_column("Updated", style="step")
        table.add_column("Active task", style="info")
        table.add_column("Errors", justify="right", style="step")
        for s in sessions[:limit]:
            updated = (s.get("updated_at") or s.get("created_at") or "")[:19]
            table.add_row(
                str(s.get("session_id", "?")),
                updated,
                str(s.get("active_task") or self.glyphs.dash),
                str(s.get("errors_count", 0)),
            )
        self.console.print(table)
        self.console.print()

    def catalog_table(self, entries: list[Any]) -> None:
        """Known objects from the catalog cache (F-6: `dacli catalog`)."""
        if not entries:
            self.console.print(
                "[muted]Catalog cache is empty — objects appear here once the "
                "agent introspects or creates them.[/muted]\n"
            )
            return
        table = Table(
            title="[accent]Catalog[/accent]",
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Connector", style="info")
        table.add_column("Type", style="step")
        table.add_column("Object", style="accent")
        table.add_column("~Rows", justify="right", style="step")
        table.add_column("Verified", style="muted")
        table.add_column("", style="warning")
        for entry in entries:
            scope = getattr(entry, "scope", {}) or {}
            name = ".".join(
                str(scope[k]) for k in ("database", "schema", "object") if scope.get(k)
            ) or "(unscoped)"
            rce = getattr(entry, "row_count_estimate", None)
            verified = getattr(entry, "last_verified", None)
            stale = entry.is_stale() if hasattr(entry, "is_stale") else False
            table.add_row(
                getattr(entry, "connector", "?"),
                getattr(entry, "object_type", "?"),
                name,
                str(rce) if rce is not None else self.glyphs.dash,
                verified.isoformat(timespec="seconds")
                if hasattr(verified, "isoformat") else self.glyphs.dash,
                "stale" if stale else "",
            )
        self.console.print(table)
        self.console.print(
            "[muted]Stale entries are hints — the agent re-verifies them before "
            "acting. /schema <object> shows columns.[/muted]\n"
        )

    def schema_panel(self, entry: Any) -> None:
        """Columns/types/row-count/last-verified for one object (F-6)."""
        scope = getattr(entry, "scope", {}) or {}
        name = ".".join(
            str(scope[k]) for k in ("database", "schema", "object") if scope.get(k)
        ) or "(unscoped)"
        header = Text()
        header.append(f"{name}\n", style="accent")
        header.append("Connector   ", style="muted")
        header.append(f"{getattr(entry, 'connector', '?')}\n", style="info")
        header.append("Type        ", style="muted")
        header.append(f"{getattr(entry, 'object_type', '?')}\n", style="info")
        rce = getattr(entry, "row_count_estimate", None)
        header.append("~Rows       ", style="muted")
        header.append(f"{rce if rce is not None else self.glyphs.dash}\n", style="info")
        verified = getattr(entry, "last_verified", None)
        header.append("Verified    ", style="muted")
        header.append(
            verified.isoformat(timespec="seconds")
            if hasattr(verified, "isoformat") else self.glyphs.dash,
            style="info",
        )
        if hasattr(entry, "is_stale") and entry.is_stale():
            header.append("  (stale — re-verify before relying on it)", style="warning")
        self.console.print(
            Panel(header, title="[accent]Schema[/accent]", box=self.glyphs.box,
                  border_style="border", padding=SPACING["panel_pad"])
        )

        columns = getattr(entry, "columns", None) or []
        if not columns:
            self.console.print(
                "[muted]No cached columns for this object — ask the agent to "
                "introspect it to fill them in.[/muted]\n"
            )
            return
        table = Table(
            show_header=True,
            header_style="muted",
            border_style="border",
            box=None,
            padding=(0, 2, 0, 0),
        )
        table.add_column("Column", style="info")
        table.add_column("Type", style="step")
        table.add_column("Description", style="muted")
        for col in columns:
            table.add_row(
                str(col.get("name", "?")),
                str(col.get("type") or col.get("data_type") or ""),
                str(col.get("description", "")),
            )
        self.console.print(table)
        self.console.print()

    def status_panel(self, memory) -> None:
        # Render the current agent status: session panel, plan and statistics.
        summary = memory.get_progress_summary()

        # Main status panel
        status_text = Text()
        status_text.append("Session     ", style="muted")
        status_text.append(f"{summary['session_id']}\n", style="accent")
        status_text.append("Active task ", style="muted")
        status_text.append(
            f"{summary.get('active_task') or self.glyphs.dash}", style="phase"
        )
        self.console.print(
            Panel(
                status_text,
                title="[accent]Status[/accent]",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )

        # Plan (todo list)
        if summary.get("todos"):
            table = Table(
                title="[accent]Plan[/accent]",
                show_header=True,
                header_style="muted",
                border_style="border",
                box=None,
                padding=(0, 2, 0, 0),
            )
            table.add_column("#", style="muted", justify="right")
            table.add_column("Status")
            table.add_column("Task", style="info")
            for i, todo in enumerate(summary.get("todos", []), 1):
                status = todo.get("status", "pending")
                status_icon = {
                    "pending": self.glyphs.pending,
                    "in_progress": self.glyphs.running,
                    "completed": self.glyphs.ok,
                }.get(status, self.glyphs.pending)
                table.add_row(str(i), f"{status_icon} {status}", todo.get("content", ""))
            self.console.print(table)

        # Stats
        stats_table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
        stats_table.add_column("Metric", style="muted")
        stats_table.add_column("Value", style="info", justify="right")
        stats_table.add_row("Schemas created", str(summary.get("schemas_created", 0)))
        stats_table.add_row("Tables created", str(summary.get("tables_created", 0)))
        stats_table.add_row("Tables loaded", str(summary.get("tables_loaded", 0)))
        stats_table.add_row("Total rows", str(summary.get("total_rows_loaded", 0)))
        stats_table.add_row("Files discovered", str(summary.get("files_discovered", 0)))
        stats_table.add_row("Errors", str(summary.get("errors_count", 0)))
        self.console.print(
            Panel(
                stats_table,
                title="[accent]Statistics[/accent]",
                box=self.glyphs.box,
                border_style="border",
                padding=SPACING["panel_pad"],
            )
        )

        if summary.get("last_error"):
            self.console.print(f"[error]Last error:[/error] {summary['last_error']}")

    def history(self, messages: list[Any], limit: int = 20) -> None:
        for msg in messages[-limit:]:
            role = getattr(msg, "role", "?")
            content = getattr(msg, "content", "")
            is_user = role == "user"
            marker = self.glyphs.caret if is_user else self.glyphs.agent
            marker_style = "accent" if is_user else "gutter"
            text_style = "user" if is_user else "step"
            preview = (
                content
                if len(content) <= 200
                else content[:200] + self.glyphs.ellipsis
            )
            self.console.print(
                self._guttered(marker, marker_style, Text(preview, style=text_style))
            )
        self.console.print()

    def panel(
        self, renderable: RenderableType, title: str, style: str = "border"
    ) -> None:
        self.console.print(
            Panel(renderable, title=title, box=self.glyphs.box,
                  border_style=style, padding=SPACING["panel_pad"])
        )

    def rule(self, label: str = "") -> None:
        self.console.print(Rule(label, style="border"))

    # ------------------------------------------------------------------
    # Persistent bottom status bar (prompt-toolkit)
    # ------------------------------------------------------------------
    def bottom_toolbar(
        self,
        *,
        provider: str,
        model: str,
        connectors: list[str],
        ctx_pct: int,
        session: str,
        test_mode: str = "",
        cost: str = "",
    ):
        """Return prompt-toolkit formatted text for the bottom bar."""
        from prompt_toolkit.formatted_text import HTML

        def esc(s: str) -> str:
            return (
                (s or "")
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )

        g = self.glyphs
        conns = ",".join(connectors) if connectors else "none"
        if len(conns) > 28:
            conns = conns[:27] + g.ellipsis
        sep = g.bar_sep
        test_seg = (
            f' {sep}  <b><style fg="ansired">{esc(test_mode)}</style></b>'
            if test_mode
            else ""
        )
        cost_seg = f" {sep}  {esc(cost)} " if cost else ""
        text = (
            f" <b>{esc(provider)}</b>{g.dot}{esc(model)} "
            f" {sep}  {g.bar_conn}{esc(conns)} "
            f" {sep}  {g.bar_ctx}ctx {ctx_pct}% "
            f"{cost_seg}"
            f" {sep}  {g.bar_session}{esc(session)} "
            f"{test_seg}"
            f" {sep}  /help "
        )
        # prompt-toolkit parses these colors itself and raises (crashing the
        # input loop) on anything that isn't hex/ansi — so only pass colors we
        # know are valid, otherwise fall back to its default bar styling.
        attrs = " ".join(
            f'{attr}="{value}"'
            for attr, value in (
                ("bg", self.theme.toolbar_bg),
                ("fg", self.theme.toolbar_fg),
            )
            if _valid_pt_color(value)
        )
        if attrs:
            return HTML(f"<style {attrs}>{text}</style>")
        return HTML(text)


# ----------------------------------------------------------------------
# Rendering helpers (shared, no UI state)
# ----------------------------------------------------------------------
def _valid_pt_color(value: str) -> bool:
    """True if ``value`` is a color prompt-toolkit can parse (hex or ansi*)."""
    if not value:
        return False
    if value.startswith("#") and len(value) == 7:
        return all(c in "0123456789abcdefABCDEF" for c in value[1:])
    return value == "default" or value.startswith("ansi")


def _cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _unified_diff_text(
    before: str,
    after: str,
    *,
    from_label: str = "before",
    to_label: str = "after",
) -> Text:
    """Red/green unified diff as Rich ``Text`` (color paired with +/- glyphs)."""
    import difflib

    a = str(before or "").splitlines()
    b = str(after or "").splitlines()
    body = Text()
    lines = list(
        difflib.unified_diff(a, b, fromfile=from_label, tofile=to_label, lineterm="")
    )
    if not lines:
        body.append("no differences", style="muted")
        return body
    for line in lines:
        if line.startswith(("+++", "---")):
            body.append(line + "\n", style="muted")
        elif line.startswith("@@"):
            body.append(line + "\n", style="accent")
        elif line.startswith("+"):
            body.append(line + "\n", style="success")
        elif line.startswith("-"):
            body.append(line + "\n", style="error")
        else:
            body.append(line + "\n", style="step")
    body.rstrip()
    return body


def _compact_preview(value: Any, max_len: int = 80, ellipsis: str = "…") -> str:
    """One-line preview of a value — nested containers summarize, never dump.

    A nested dict shows its first few keys and the true size; a nested list
    shows its first few items and the true length. Scalars render as-is,
    truncated to ``max_len``.
    """
    if isinstance(value, dict):
        keys = list(value)
        head = ", ".join(str(k) for k in keys[:4])
        more = f", {ellipsis}" if len(keys) > 4 else ""
        noun = "key" if len(keys) == 1 else "keys"
        return f"{{{head}{more}}}  ({len(keys)} {noun})"
    if isinstance(value, (list, tuple)):
        head = ", ".join(_cell(v)[:24] for v in value[:3])
        more = f", {ellipsis}" if len(value) > 3 else ""
        noun = "item" if len(value) == 1 else "items"
        return f"[{head}{more}]  ({len(value)} {noun})"
    s = _cell(value)
    if len(s) > max_len:
        s = s[: max_len - 1] + ellipsis
    return s.replace("\n", " ")


def _arg_preview(args: dict[str, Any], max_len: int = 80, ellipsis: str = "…") -> str:
    """Compact ``key=value`` preview for a tool call (SQL shown separately)."""
    parts: list[str] = []
    for key, value in (args or {}).items():
        if key in ("query", "sql"):
            continue
        s = str(value).replace("\n", " ")
        if len(s) > 50:
            s = s[:49] + ellipsis
        parts.append(f"{key}={s}")
    preview = ", ".join(parts)
    if len(preview) > max_len:
        preview = preview[: max_len - 1] + ellipsis
    return preview


# Sentinel marking the elided middle in a capped head+tail render.
_GAP = object()


def _capped_indexed(
    items: list, cap: int, noun: str, ellipsis: str = "…"
) -> tuple[list[tuple[int, Any]], Text | None]:
    """1-based ``(index, item)`` pairs capped to head+tail, plus a footer.

    Under the cap, every item is returned (footer ``None``). Over it, the head
    and tail survive with their *true* indices, a ``_GAP`` marks the elision,
    and the footer says how much was shown — the data itself is untouched.
    """
    total = len(items)
    if total <= cap:
        return list(enumerate(items, 1)), None
    tail_n = max(1, cap // 6)
    head_n = cap - tail_n
    indexed: list[tuple[int, Any]] = list(enumerate(items[:head_n], 1))
    indexed.append((0, _GAP))
    indexed.extend(enumerate(items[-tail_n:], total - tail_n + 1))
    footer = Text(
        f"{ellipsis} showing {head_n + tail_n:,} of {total:,} {noun}. Full result "
        "preserved - use the result handle / /export to see all.",
        style="muted",
    )
    return indexed, footer


def _rows_table(
    rows: list[dict[str, Any]], max_rows: int = 120, ellipsis: str = "…"
) -> RenderableType:
    """Render row-dicts as a table — every column, head+tail rows when huge.

    The cap bounds only what is *printed*; ``result.data`` and the off-context
    spill keep every row.
    """
    columns = list(rows[0].keys())
    for row in rows[1:]:
        for key in row:
            if key not in columns:
                columns.append(key)

    table = Table(
        show_header=True, header_style="muted", box=None, padding=(0, 2, 0, 0)
    )
    table.add_column("#", style="muted", justify="right")
    for col in columns:
        table.add_column(str(col), style="info", overflow="fold")

    indexed, footer = _capped_indexed(rows, max_rows, "rows", ellipsis=ellipsis)
    for i, row in indexed:
        if row is _GAP:
            table.add_row(ellipsis, *[ellipsis for _ in columns])
            continue
        table.add_row(str(i), *[_cell(row.get(col)) for col in columns])
    return Group(table, footer) if footer else table
