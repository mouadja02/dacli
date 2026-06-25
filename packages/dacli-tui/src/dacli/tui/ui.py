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

from typing import Any

from rich.console import Console, Group, RenderableType
from rich.markdown import Markdown
from rich.live import Live
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from .design import ASCII as ASCII_GLYPHS
from .design import SPACING, TIER_STYLE, Glyphs, gauge, resolve_glyphs
from .theme import ThemeSpec, get_theme
from .render_util import _remediation_hint, _valid_pt_color
from .stream import StreamMixin, StreamView
from .transcript import TranscriptLog, TranscriptMixin
from .panels import PanelsMixin

# Re-exported for callers that import it from here (e.g. tui/__init__).
__all__ = ["TIER_STYLE", "DacliUI", "StreamView"]

__author__ = ""  # populated by caller if needed


class DacliUI(StreamMixin, TranscriptMixin, PanelsMixin):
    """All terminal presentation for the interactive session.

    The rendering surfaces live in sibling modules and are composed here as
    mixins (09-P10): streaming/spinners in :mod:`tui.stream`, the tool-call
    transcript in :mod:`tui.transcript`, and the panels/tables in
    :mod:`tui.panels`. This class keeps the shared chrome (banner, welcome,
    notices, prompt, toolbar) and the live state they all read.
    """

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
        # In-session tool-outcome log for /find, /last-error, /expand (P11). The
        # spill store is bound once the agent is built (see bind_result_store).
        self.transcript_log = TranscriptLog()

    def bind_result_store(self, store: Any) -> None:
        """Point the transcript log at the session spill store (post agent init)."""
        self.transcript_log.store = store

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
    _ART_UNICODE = (
        "██████╗   █████╗   ██████╗ ██╗      ██╗",
        "██╔══██╗ ██╔══██╗ ██╔════╝ ██║      ██║",
        "██║  ██║ ███████║ ██║      ██║      ██║",
        "██║  ██║ ██╔══██║ ██║      ██║      ██║",
        "██████╔╝ ██║  ██║ ╚██████╗ ███████╗ ██║",
        "╚═════╝  ╚═╝  ╚═╝  ╚═════╝ ╚══════╝ ╚═╝",
    )
    _ART_ASCII = (
        " ____    _    ____ _     ___ ",
        "|  _ \\  / \\  / ___| |   |_ _|",
        "| | | |/ _ \\| |   | |    | | ",
        "| |_| / ___ \\ |___| |___ | | ",
        "|____/_/   \\_\\____|_____|___|",
    )

    def banner(self) -> None:
        """The ASCII wordmark — gradient on capable terminals, plain ASCII art
        otherwise, and a compact one-liner when the terminal is too small for
        the full mark to read calm."""
        try:
            width = self.console.width
            height = self.console.height
        except Exception:
            width, height = 80, 24
        if width < 48 or height < 14:
            line = Text()
            line.append("DACLI", style="bold accent")
            line.append(
                f"  {self.glyphs.dot}  data-engineering CLI agent", style="muted"
            )
            if self.version:
                line.append(f"  {self.glyphs.dot}  v{self.version}", style="muted")
            self.console.print(Padding(line, (1, 2, 0, 2)))
            return

        art = self._ART_UNICODE if self.glyphs is not ASCII_GLYPHS else self._ART_ASCII
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
        self,
        *,
        model: str,
        provider: str,
        connectors: list[str],
        cwd: str,
        config: str | None = None,
        state: str | None = None,
    ) -> None:
        """A compact 'session ready' card with the essentials + quick tips.

        ``config``/``state`` make the on-disk locations visible up front so a
        user never has to guess where secrets and session state live."""
        info = Text()
        info.append("model      ", style="muted")
        info.append(f"{provider}{self.glyphs.dot}{model}\n", style="accent")
        info.append("connectors ", style="muted")
        info.append(
            (", ".join(connectors) if connectors else "none") + "\n", style="info"
        )
        info.append("cwd        ", style="muted")
        info.append(cwd, style="info")
        if config is not None or state is not None:
            info.append("\nconfig     ", style="muted")
            info.append(config or "(none)", style="info")
            info.append(f"  {self.glyphs.dot}  state ", style="muted")
            info.append(state or "(none)", style="info")

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

        # No connectors enabled yet — point at the two ways to fix that rather
        # than leaving an empty card that looks broken.
        if not connectors:
            tips.append("\n")
            tips.append("No connectors yet", style="warning")
            tips.append(" — run ", style="muted")
            tips.append("/setup", style="accent")
            tips.append(" or ", style="muted")
            tips.append("/connect <tool>", style="accent")

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
    def turn_header(self, *, model: str, session: str, elapsed: str = "") -> None:
        """A slim one-line context rule above a turn (``ui.show_header``)."""
        bits = [model, session]
        if elapsed:
            bits.append(elapsed)
        label = f"  {self.glyphs.dot}  ".join(b for b in bits if b)
        self.console.print(Rule(f"[muted]{label}[/muted]", style="border"))

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
        wh_cost: str = "",
        width: int | None = None,
    ):
        """Return prompt-toolkit formatted text for the bottom bar.

        Responsive: under ~80 columns the bar collapses to the essentials
        (model, context gauge, cost, test-mode); segments are dropped before
        they could ever wrap. ``ctx_pct`` renders as a real gauge driven by
        the assembler's budget snapshot (see ``_ctx_pct`` in cli.py).
        """
        from prompt_toolkit.formatted_text import HTML

        def esc(s: str) -> str:
            return (
                (s or "")
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )

        g = self.glyphs
        try:
            bar_width = int(width if width is not None else self.console.width)
        except Exception:
            bar_width = 100

        conns = ",".join(connectors) if connectors else "none"
        if len(conns) > 28:
            conns = conns[:27] + g.ellipsis
        ctx_gauge = gauge(ctx_pct, g)

        # Display-order segments as (plain, markup, drop_rank). The bar must
        # *never* exceed the terminal width (a wrapped bottom bar is the
        # ugliest failure a TUI can have), so segments are dropped by rank —
        # highest first — until the plain text fits. Rank 0 never drops:
        # model, the context gauge, and test-mode (it changes semantics).
        segments: list[tuple[str, str, int]] = [
            (
                f"{provider}{g.dot}{model}",
                f"<b>{esc(provider)}</b>{g.dot}{esc(model)}",
                0,
            ),
            (f"{g.bar_conn}{conns}", f"{g.bar_conn}{esc(conns)}", 4),
            (f"{g.bar_ctx}ctx {ctx_gauge}", f"{g.bar_ctx}ctx {esc(ctx_gauge)}", 0),
        ]
        if cost:
            segments.append((cost, esc(cost), 2))
        if wh_cost:
            segments.append((f"wh {wh_cost}", f"wh {esc(wh_cost)}", 2))
        segments.append((f"{g.bar_session}{session}", f"{g.bar_session}{esc(session)}", 3))
        if test_mode:
            segments.append(
                (test_mode, f'<b><style fg="ansired">{esc(test_mode)}</style></b>', 0)
            )
        segments.append(("/help", "/help", 5))

        sep_plain = f" {g.bar_sep}  "

        def fits(segs: list[tuple[str, str, int]]) -> bool:
            plain = " " + sep_plain.join(p for p, _m, _r in segs) + " "
            return len(plain) <= bar_width

        for rank in (5, 4, 3, 2, 1):
            if fits(segments):
                break
            segments = [s for s in segments if s[2] != rank]
        if not fits(segments):
            # Last resort: drop the provider prefix from the model segment.
            plain0, _markup0, _ = segments[0]
            short = plain0.split(g.dot, 1)[-1]
            segments[0] = (short, f"<b>{esc(short)}</b>", 0)

        text = " " + sep_plain.join(m for _p, m, _r in segments) + " "
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
