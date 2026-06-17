"""Streaming + spinner surfaces: the live LLM stream view and progress/status."""
from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING, Any

from rich.console import RenderableType
from rich.live import Live
from rich.markdown import Markdown
from rich.text import Text

if TYPE_CHECKING:
    from dacli.tui.ui import DacliUI


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

class StreamMixin:
    """Status line, stream callbacks, and the progress context manager."""

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

