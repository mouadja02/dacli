"""Tool-call transcript: tool_start / tool_end rendering and the render cap."""
from __future__ import annotations

import contextlib
import time
from typing import Any

from rich.console import Group, RenderableType
from rich.live import Live
from rich.padding import Padding
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from dacli.connectors.base import ToolResult
from dacli.tui.design import SPACING, TIER_STYLE
from dacli.tui.render_util import (
    _GAP, _arg_preview, _capped_indexed, _cell, _compact_preview,
    _remediation_hint, _rows_table,
)


class TranscriptMixin:
    """tool_start / tool_end and their helpers."""

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
                # Text() so data containing [brackets] is never eaten as markup.
                kv.add_row(Text(str(k)), Text(_compact_preview(v, ellipsis=gap)))
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
