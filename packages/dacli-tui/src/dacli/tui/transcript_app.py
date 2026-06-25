"""Optional Textual full-screen transcript viewer (the ``dacli[tui]`` extra).

Rich is the default and the foundation; this is the only Textual surface in the
project. It's lazy-imported so a plain install never pays for Textual — callers
check :func:`is_available` first and print an install hint when it's absent.

In-app: scroll the whole session, filter with ``/`` (substring over turns and
tool results), and jump to the last error with ``e``.
"""

from __future__ import annotations

from typing import Any, ClassVar


def is_available() -> bool:
    try:
        import textual  # noqa: F401
    except ImportError:
        return False
    return True


def build_app(turns: list[Any], records: list[Any]):
    """Construct the transcript App. Call only when :func:`is_available`."""
    from textual.app import App
    from textual.widgets import Footer, Header, Input, RichLog

    class TranscriptApp(App):
        CSS = "#search { dock: top; }"
        BINDINGS: ClassVar = [
            ("q", "quit", "Quit"),
            ("slash", "focus_search", "Find"),
            ("e", "last_error", "Last error"),
        ]

        def __init__(self):
            super().__init__()
            self._turns = turns
            self._records = records
            self._query = ""

        def compose(self):
            yield Header(show_clock=False)
            yield Input(placeholder="filter… (Enter applies, Esc clears)", id="search")
            yield RichLog(id="log", wrap=True, markup=True, highlight=True)
            yield Footer()

        def on_mount(self):
            self.title = "dacli transcript"
            self._render()

        def _render(self):
            log = self.query_one("#log", RichLog)
            log.clear()
            q = self._query.lower()
            for turn in self._turns:
                content = getattr(turn, "content", "") or ""
                if q and q not in content.lower():
                    continue
                role = getattr(turn, "role", "?")
                log.write(f"[bold]{role}[/bold]  {content}")
            for rec in self._records:
                hay = f"{rec.tool_name} {rec.summary} {rec.error or ''}".lower()
                if q and q not in hay:
                    continue
                tag = "green" if rec.success else "red"
                suffix = f"  ✗ {rec.error}" if rec.error else ""
                log.write(f"[{tag}]{rec.rid}[/{tag}] {rec.tool_name} — {rec.summary}{suffix}")

        def action_focus_search(self):
            self.query_one("#search", Input).focus()

        def action_last_error(self):
            rec = next((r for r in reversed(self._records) if not r.success), None)
            if rec is not None:
                self._query = rec.error or rec.rid
                self.query_one("#search", Input).value = self._query
                self._render()

        def on_input_submitted(self, event):
            self._query = event.value.strip()
            self._render()

    return TranscriptApp()
