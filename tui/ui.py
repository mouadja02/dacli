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
  full result table underneath (data work: never truncated).
- **Persistent status bar** — provider·model · connectors · context · session,
  rendered by prompt-toolkit beneath the input.

Reliability: rendering never raises into the control loop. The streaming view
is transient (it leaves the polished markdown behind), so a dropped frame or a
resize can't corrupt the scrollback.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Optional

from rich.console import Console, Group, RenderableType
from rich.markdown import Markdown
from rich.live import Live
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from connectors.base import ToolResult
from .theme import ThemeSpec, get_theme

__author__ = ""  # populated by caller if needed

# Braille spinner frames + rotating verbs for the thinking indicator.
_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
_VERBS = ("Thinking", "Reasoning", "Working", "Crunching", "Cooking", "Pondering", "Churning")

# Glyphs (gutter markers borrowed from the Claude Code transcript style).
G_AGENT = "⏺"
G_TOOL = "⏺"
G_RESULT = "⎿"
G_USER = "❯"


class StreamView:
    """A transient Rich ``Live`` region for one LLM ``generate`` call.

    While the model is producing tokens this shows a thinking spinner (with
    elapsed time + current activity) and then the streaming text. On
    :meth:`end` it tears the live region down and re-prints the completed text
    as polished markdown so it stays in the scrollback.
    """

    def __init__(self, ui: "DacliUI"):
        self._ui = ui
        self._live: Optional[Live] = None
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

    def end(self, content: Optional[str] = None) -> None:
        text = content if content is not None else self._buffer
        self._teardown()
        if text and text.strip():
            self._ui.agent_message(text)

    def abort(self) -> None:
        self._teardown()

    def _teardown(self) -> None:
        if self._live:
            try:
                self._live.stop()
            except Exception:
                pass
            self._live = None
        self._buffer = ""

    def __rich__(self) -> RenderableType:
        elapsed = time.monotonic() - self._start
        if not self._buffer:
            frame = _FRAMES[int(elapsed * 10) % len(_FRAMES)]
            verb = _VERBS[int(elapsed / 4) % len(_VERBS)]
            line = Text()
            line.append(f"{frame} ", style="accent")
            line.append(f"{verb}… ", style="assistant")
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
        theme_name: Optional[str] = None,
        version: str = "",
        author: str = "",
        console: Optional[Console] = None,
    ):
        self.settings = settings
        self.version = version
        self.author = author
        self.theme: ThemeSpec = get_theme(theme_name or getattr(getattr(settings, "ui", None), "theme", None))
        if console is None:
            self.console = Console(theme=self.theme.rich_theme())
        else:
            # A caller-provided console may not know our semantic styles — push
            # the theme so 'border', 'accent', … resolve everywhere.
            self.console = console
            self.console.push_theme(self.theme.rich_theme())
        self.stream = StreamView(self)
        self.activity = ""  # current background activity, shown in the spinner

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
        gradient = self.theme.banner_gradient
        lines = Text()
        for i, row in enumerate(art):
            lines.append(row + "\n", style=gradient[i % len(gradient)])
        tagline = Text("Your autonomous data-engineering CLI agent", style="muted")
        meta = Text()
        if self.version:
            meta.append(f"v{self.version}", style="accent")
        if self.author:
            meta.append(f"  ·  {self.author}", style="muted")
        body = Group(lines, tagline, meta) if (self.version or self.author) else Group(lines, tagline)
        self.console.print(Padding(body, (1, 2, 0, 2)))

    def welcome(self, *, model: str, provider: str, connectors: List[str], cwd: str) -> None:
        """A compact 'session ready' card with the essentials + quick tips."""
        info = Text()
        info.append("model      ", style="muted")
        info.append(f"{provider}·{model}\n", style="accent")
        info.append("connectors ", style="muted")
        info.append((", ".join(connectors) if connectors else "none") + "\n", style="info")
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
                title="[success]✓ ready[/success]",
                border_style="border",
                padding=(1, 2),
            )
        )

    # ------------------------------------------------------------------
    # Transcript primitives
    # ------------------------------------------------------------------
    def _guttered(self, marker: str, marker_style: str, renderable: RenderableType, indent: int = 0) -> Table:
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
        self.console.print(f"[{style}]{message}[/{style}]")

    def error(self, message: str) -> None:
        self.console.print(self._guttered("✗", "bad", Text(message, style="error")))

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
        self.stream.begin()

    def on_text(self, delta: str) -> None:
        self.stream.feed(delta)

    def on_stream_end(self, content: str) -> None:
        self.stream.end(content)

    # ------------------------------------------------------------------
    # Tool transcript
    # ------------------------------------------------------------------
    def tool_start(self, tool_name: str, args: Dict[str, Any]) -> None:
        header = Text(tool_name, style="tool")
        preview = _arg_preview(args)
        if preview:
            header.append(f"  {preview}", style="muted")
        self.console.print(self._guttered(G_TOOL, "tool", header))

        sql = args.get("query") or args.get("sql")
        if isinstance(sql, str) and sql.strip():
            syntax = Syntax(sql.strip(), "sql", theme="monokai", word_wrap=True, background_color="default")
            self.console.print(Padding(syntax, (0, 0, 0, 4)))

    def tool_end(self, tool_name: str, result: Any) -> None:
        if not isinstance(result, ToolResult):
            self.console.print(Padding(self._guttered(G_RESULT, "muted", Text(str(result), style="step")), (0, 0, 0, 2)))
            return

        if not result.success:
            summary = Text()
            summary.append("✗ ", style="bad")
            summary.append(str(result.error or "failed"), style="error")
            self.console.print(Padding(self._guttered(G_RESULT, "bad", summary), (0, 0, 0, 2)))
            self.console.print()
            return

        summary = Text()
        summary.append("✓ ", style="ok")
        data = result.data
        body: Optional[RenderableType] = None

        if isinstance(data, list) and data and isinstance(data[0], dict):
            summary.append(f"{len(data)} row{'s' if len(data) != 1 else ''}", style="success")
            body = _rows_table(data)
        elif isinstance(data, list):
            summary.append(f"{len(data)} item{'s' if len(data) != 1 else ''}", style="success")
            if data:
                body = Text("\n".join(f"{i}. {_cell(v)}" for i, v in enumerate(data, 1)), style="step")
        elif isinstance(data, dict):
            summary.append(f"{len(data)} field{'s' if len(data) != 1 else ''}", style="success")
            kv = Text()
            for k, v in data.items():
                kv.append(f"{k}: ", style="muted")
                kv.append(f"{_cell(v)}\n", style="step")
            body = kv
        elif data is None:
            summary.append("done", style="success")
        else:
            summary.append("done", style="success")
            body = Text(_cell(data), style="step")

        summary.append(f"  ·  {result.execution_time_ms:.0f}ms", style="muted")
        self.console.print(Padding(self._guttered(G_RESULT, "muted", summary), (0, 0, 0, 2)))
        if body is not None:
            self.console.print(Padding(body, (0, 0, 0, 4)))
        self.console.print()

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
                ops = [op for op in info["operations"] if registry.is_operation_enabled(op)]
                table.add_row(f"{info['icon']} {info['name']}", "[ok]● enabled[/ok]", str(len(ops)))
            else:
                table.add_row(f"{info['icon']} {info['name']}", "[muted]○ disabled[/muted]", "—")
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

    def sessions_table(self, sessions: List[Dict[str, Any]], limit: int = 10) -> None:
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
        table.add_column("Phase", style="info")
        table.add_column("Errors", justify="right", style="step")
        for s in sessions[:limit]:
            updated = (s.get("updated_at") or s.get("created_at") or "")[:19]
            table.add_row(
                str(s.get("session_id", "?")),
                updated,
                str(s.get("current_phase", "?")),
                str(s.get("errors_count", 0)),
            )
        self.console.print(table)
        self.console.print()

    def history(self, messages: List[Any], limit: int = 20) -> None:
        for msg in messages[-limit:]:
            role = getattr(msg, "role", "?")
            content = getattr(msg, "content", "")
            marker = G_USER if role == "user" else G_AGENT
            style = "user" if role == "user" else "assistant"
            preview = content if len(content) <= 200 else content[:200] + "…"
            self.console.print(self._guttered(marker, style, Text(preview, style="step")))
        self.console.print()

    def panel(self, renderable: RenderableType, title: str, style: str = "border") -> None:
        self.console.print(Panel(renderable, title=title, border_style=style, padding=(1, 2)))

    def rule(self, label: str = "") -> None:
        self.console.print(Rule(label, style="border"))

    # ------------------------------------------------------------------
    # Persistent bottom status bar (prompt-toolkit)
    # ------------------------------------------------------------------
    def bottom_toolbar(self, *, provider: str, model: str, connectors: List[str], ctx_pct: int, session: str):
        """Return prompt-toolkit formatted text for the bottom bar."""
        from prompt_toolkit.formatted_text import HTML

        def esc(s: str) -> str:
            return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        conns = ",".join(connectors) if connectors else "none"
        if len(conns) > 28:
            conns = conns[:27] + "…"
        text = (
            f' <b>{esc(provider)}</b>·{esc(model)} '
            f' │  ⛁ {esc(conns)} '
            f' │  ◴ ctx {ctx_pct}% '
            f' │  ⎇ {esc(session)} '
            f' │  /help '
        )
        # prompt-toolkit parses these colors itself and raises (crashing the
        # input loop) on anything that isn't hex/ansi — so only pass colors we
        # know are valid, otherwise fall back to its default bar styling.
        attrs = " ".join(
            f'{attr}="{value}"'
            for attr, value in (("bg", self.theme.toolbar_bg), ("fg", self.theme.toolbar_fg))
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


def _arg_preview(args: Dict[str, Any], max_len: int = 80) -> str:
    """Compact ``key=value`` preview for a tool call (SQL shown separately)."""
    parts: List[str] = []
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


def _rows_table(rows: List[Dict[str, Any]]) -> Table:
    """Render row-dicts as a full table — every row, every column (data work)."""
    columns = list(rows[0].keys())
    for row in rows[1:]:
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    table = Table(show_header=True, header_style="muted", box=None, padding=(0, 2, 0, 0))
    table.add_column("#", style="muted", justify="right")
    for col in columns:
        table.add_column(str(col), style="info", overflow="fold")

    for i, row in enumerate(rows, 1):
        table.add_row(str(i), *[_cell(row.get(col)) for col in columns])
    return table
