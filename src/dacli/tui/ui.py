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

from rich import box
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
from .theme import ThemeSpec, get_theme
import contextlib

__author__ = ""  # populated by caller if needed

# Braille spinner frames + rotating verbs for the thinking indicator.
_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
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

# Glyphs (gutter markers borrowed from the Claude Code transcript style).
G_AGENT = "⏺"
G_TOOL = "⏺"
G_RESULT = "⎿"
G_USER = "❯"

# Leading icon per notice style — a small visual cue so success/warning/error
# read at a glance without reading the text.
_NOTICE_ICONS = {
    "success": "✓",
    "warning": "⚠",
    "error": "✗",
    "bad": "✗",
    "info": "ℹ",
}

# Blast-radius tier → style. Shared by the audit view and the approval panel so
# "risky" reads the same color everywhere.
TIER_STYLE = {
    "safe": "success",
    "write": "info",
    "risky": "warning",
    "irreversible": "error",
}

# Error remediation hints: substring of the error → a one-line next step.
# Deliberately a small lookup (the startup path attaches hints the same way for
# failed connectors); first match wins.
_REMEDIATION_HINTS: tuple[tuple[str, str], ...] = (
    ("not healthy", "Try /debug-connector <name> to diagnose, or /connect to reconfigure."),
    ("health check", "Try /debug-connector <name> to diagnose, or /connect to reconfigure."),
    ("decrypt", "Stored secrets could not be read — re-enter them via /connect."),
    ("unknown tool", "See /tools for what's enabled; /setup to enable more."),
    ("blocked by governance", "See /audit for the decision; adjust config/policy.yaml if intended."),
    ("permission denied", "Scope too narrow — see /audit; widen it in config/policy.yaml if intended."),
    ("unauthorized", "Credentials look invalid — update them via /connect."),
    ("forbidden", "Credentials look invalid — update them via /connect."),
    ("401", "Credentials look invalid — update them via /connect."),
    ("403", "Credentials look invalid — update them via /connect."),
    ("rate limit", "Provider rate limit — wait a moment and retry."),
    ("429", "Provider rate limit — wait a moment and retry."),
    ("timed out", "The platform didn't answer in time — check connectivity, then retry."),
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
    elapsed time + current activity) and then the streaming text. On
    :meth:`end` it tears the live region down and re-prints the completed text
    as polished markdown so it stays in the scrollback.
    """

    def __init__(self, ui: DacliUI):
        self._ui = ui
        self._live: Live | None = None
        self._buffer = ""
        self._start = 0.0

    @property
    def active(self) -> bool:
        return self._live is not None

    def begin(self) -> None:
        self._buffer = ""
        self._start = time.monotonic()
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
        if not self._buffer:
            frame = _FRAMES[int(elapsed * 10) % len(_FRAMES)]
            verb = _VERBS[int(elapsed / 3) % len(_VERBS)]
            shimmer = _SHIMMER[int(elapsed * 3) % len(_SHIMMER)]
            line = Text()
            line.append(f"{frame} ", style=f"bold {shimmer}")
            line.append(f"{verb}", style="assistant")
            # Animated trailing dots so the line breathes even mid-token-wait.
            dots = "." * (1 + int(elapsed * 2) % 3)
            line.append(f"{dots} ", style="muted")
            if self._ui.activity:
                line.append(f"{self._ui.activity} ", style="muted")
            line.append(f"({elapsed:.0f}s · ctrl-c to interrupt)", style="muted")
            return line
        body = Text(f"{G_AGENT} ", style="gutter")
        body.append(self._buffer, style="assistant")
        body.append("▌", style="accent")
        return body


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
        self.theme: ThemeSpec = get_theme(
            theme_name or getattr(getattr(settings, "ui", None), "theme", None)
        )
        if console is None:
            self.console = Console(theme=self.theme.rich_theme())
        else:
            # A caller-provided console may not know our semantic styles — push
            # the theme so 'border', 'accent', … resolve everywhere.
            self.console = console
            self.console.push_theme(self.theme.rich_theme())
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
            meta.append(f"  ·  {self.author}", style="muted")
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
        info.append(f"{provider}·{model}\n", style="accent")
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
        tips.append("↑↓", style="accent")
        tips.append(" history   ", style="muted")
        tips.append("Tab", style="accent")
        tips.append(" complete   ", style="muted")
        tips.append("ctrl-c", style="accent")
        tips.append(" interrupt", style="muted")

        self.console.print(
            Panel(
                Group(info, tips),
                title="[success]✓ session ready[/success]",
                title_align="left",
                box=box.ROUNDED,
                border_style="accent",
                padding=(1, 2),
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
            f'<style fg="{c}"><b>▌</b></style> <b>you</b> '
            f'<style fg="{c}">❯</style> '
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
                title="[accent]▌ you[/accent]",
                title_align="left",
                box=box.ROUNDED,
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
        self.console.print(self._guttered(G_AGENT, "gutter", Markdown(content)))
        self.console.print()

    def notice(self, message: str, style: str = "info") -> None:
        icon = _NOTICE_ICONS.get(style, "")
        prefix = f"{icon} " if icon else ""
        self.console.print(f"[{style}]{prefix}{message}[/{style}]")

    def error(self, message: str) -> None:
        self._clear_progress()
        self.console.print(self._guttered("✗", "bad", Text(message, style="error")))
        hint = _remediation_hint(message)
        if hint:
            self.console.print(
                Padding(Text(f"↳ {hint}", style="muted"), (0, 0, 0, 2))
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
            frame = _FRAMES[int(time.monotonic() * 10) % len(_FRAMES)]
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
        preview = _arg_preview(args)
        if preview:
            header.append(f"  {preview}", style="muted")
        self.console.print(self._guttered(G_TOOL, "tool", header))

        sql = args.get("query") or args.get("sql")
        if isinstance(sql, str) and sql.strip():
            syntax = Syntax(
                sql.strip(),
                "sql",
                theme="monokai",
                word_wrap=True,
                background_color="default",
            )
            self.console.print(Padding(syntax, (0, 0, 0, 4)))

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
                    self._guttered(G_RESULT, "muted", Text(str(result), style="step")),
                    (0, 0, 0, 2),
                )
            )
            return

        if not result.success:
            summary = Text()
            summary.append("✗ ", style="bad")
            summary.append(str(result.error or "failed"), style="error")
            self.console.print(
                Padding(self._guttered(G_RESULT, "bad", summary), (0, 0, 0, 2))
            )
            hint = _remediation_hint(result.error)
            if hint:
                self.console.print(
                    Padding(Text(f"↳ {hint}", style="muted"), (0, 0, 0, 4))
                )
            self.console.print()
            return

        summary = Text()
        summary.append("✓ ", style="ok")
        data = result.data
        body: RenderableType | None = None
        cap = self._render_cap()

        if isinstance(data, list) and data and isinstance(data[0], dict):
            summary.append(
                f"{len(data)} row{'s' if len(data) != 1 else ''}", style="success"
            )
            body = _rows_table(data, max_rows=cap)
        elif isinstance(data, list):
            summary.append(
                f"{len(data)} item{'s' if len(data) != 1 else ''}", style="success"
            )
            if data:
                indexed, footer = _capped_indexed(data, cap, "items")
                listing = Text(
                    "\n".join(
                        "…" if v is _GAP else f"{i}. {_cell(v)}" for i, v in indexed
                    ),
                    style="step",
                )
                body = Group(listing, footer) if footer else listing
        elif isinstance(data, dict):
            summary.append(
                f"{len(data)} field{'s' if len(data) != 1 else ''}", style="success"
            )
            indexed, footer = _capped_indexed(list(data.items()), cap, "fields")
            kv = Text()
            for _i, item in indexed:
                if item is _GAP:
                    kv.append("…\n", style="muted")
                    continue
                k, v = item
                kv.append(f"{k}: ", style="muted")
                kv.append(f"{_cell(v)}\n", style="step")
            body = Group(kv, footer) if footer else kv
        elif data is None:
            summary.append("done", style="success")
        else:
            summary.append("done", style="success")
            body = Text(_cell(data), style="step")

        summary.append(f"  ·  {result.execution_time_ms:.0f}ms", style="muted")
        self.console.print(
            Padding(self._guttered(G_RESULT, "muted", summary), (0, 0, 0, 2))
        )
        if body is not None:
            self.console.print(Padding(body, (0, 0, 0, 4)))
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
        border = "error" if tier == "irreversible" else "warning"
        try:
            body = self._approval_body(request, tier)
        except Exception:
            describe = getattr(request, "describe", None)
            try:
                text = str(describe()) if callable(describe) else str(request)
            except Exception:
                text = str(request)
            body = Text(text, style="step")
        tier_style = TIER_STYLE.get(tier, "muted")
        self.console.print(
            Panel(
                body,
                title=(
                    f"[{border}]approval needed[/{border}] · "
                    f"[{tier_style}]{tier}[/{tier_style}]"
                ),
                title_align="left",
                border_style=border,
                padding=(1, 2),
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

        parts: list[RenderableType] = [grid]
        preview = getattr(request, "dry_run_preview", None)
        if preview:
            parts.append(Text("Dry-run preview", style="muted"))
            parts.append(
                Syntax(
                    str(preview).strip(),
                    "sql",
                    theme="monokai",
                    word_wrap=True,
                    background_color="default",
                )
            )
        shadow = getattr(request, "shadow", None)
        if shadow is not None and getattr(shadow, "ran", False):
            diff = getattr(shadow, "diff", None) or {}
            if "rows_before" in diff and "rows_after" in diff:
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
                title="[accent]plan preview[/accent] · [muted]dry — no execution[/muted]",
                title_align="left",
                border_style="border",
                padding=(1, 2),
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
            ("↑ / ↓", "Browse input history"),
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
                border_style="border",
                padding=(1, 2),
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
                    "[ok]● enabled[/ok]",
                    str(len(ops)),
                )
            else:
                table.add_row(
                    f"{info['icon']} {info['name']}", "[muted]○ disabled[/muted]", "—"
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
                str(s.get("active_task") or "—"),
                str(s.get("errors_count", 0)),
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
        status_text.append(f"{summary.get('active_task') or '—'}", style="phase")
        self.console.print(
            Panel(
                status_text,
                title="[accent]Status[/accent]",
                border_style="border",
                padding=(1, 2),
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
                    "pending": "○",
                    "in_progress": "◐",
                    "completed": "●",
                }.get(status, "○")
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
                border_style="border",
                padding=(1, 2),
            )
        )

        if summary.get("last_error"):
            self.console.print(f"[error]Last error:[/error] {summary['last_error']}")

    def history(self, messages: list[Any], limit: int = 20) -> None:
        for msg in messages[-limit:]:
            role = getattr(msg, "role", "?")
            content = getattr(msg, "content", "")
            is_user = role == "user"
            marker = "▌" if is_user else G_AGENT
            marker_style = "accent" if is_user else "gutter"
            text_style = "user" if is_user else "step"
            preview = content if len(content) <= 200 else content[:200] + "…"
            self.console.print(
                self._guttered(marker, marker_style, Text(preview, style=text_style))
            )
        self.console.print()

    def panel(
        self, renderable: RenderableType, title: str, style: str = "border"
    ) -> None:
        self.console.print(
            Panel(renderable, title=title, border_style=style, padding=(1, 2))
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

        conns = ",".join(connectors) if connectors else "none"
        if len(conns) > 28:
            conns = conns[:27] + "…"
        test_seg = (
            f' │  <b><style fg="ansired">{esc(test_mode)}</style></b>'
            if test_mode
            else ""
        )
        cost_seg = f" │  {esc(cost)} " if cost else ""
        text = (
            f" <b>{esc(provider)}</b>·{esc(model)} "
            f" │  ⛁ {esc(conns)} "
            f" │  ◴ ctx {ctx_pct}% "
            f"{cost_seg}"
            f" │  ⎇ {esc(session)} "
            f"{test_seg}"
            f" │  /help "
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


def _arg_preview(args: dict[str, Any], max_len: int = 80) -> str:
    """Compact ``key=value`` preview for a tool call (SQL shown separately)."""
    parts: list[str] = []
    for key, value in (args or {}).items():
        if key in ("query", "sql"):
            continue
        s = str(value).replace("\n", " ")
        if len(s) > 50:
            s = s[:49] + "…"
        parts.append(f"{key}={s}")
    preview = ", ".join(parts)
    if len(preview) > max_len:
        preview = preview[: max_len - 1] + "…"
    return preview


# Sentinel marking the elided middle in a capped head+tail render.
_GAP = object()


def _capped_indexed(
    items: list, cap: int, noun: str
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
        f"… showing {head_n + tail_n:,} of {total:,} {noun}. Full result "
        "preserved — use the result handle / /export to see all.",
        style="muted",
    )
    return indexed, footer


def _rows_table(rows: list[dict[str, Any]], max_rows: int = 120) -> RenderableType:
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

    indexed, footer = _capped_indexed(rows, max_rows, "rows")
    for i, row in indexed:
        if row is _GAP:
            table.add_row("…", *["…" for _ in columns])
            continue
        table.add_row(str(i), *[_cell(row.get(col)) for col in columns])
    return Group(table, footer) if footer else table
