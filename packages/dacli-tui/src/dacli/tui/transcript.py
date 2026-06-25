"""Tool-call transcript: tool_start / tool_end rendering and the render cap.

The same surface keeps an in-session :class:`TranscriptLog` so the navigability
commands (``/find``, ``/last-error``, ``/expand``) can re-render a result without
re-running the tool. A result the live view head/tail-elides is spilled to the
session :class:`~dacli.context.spill.ResultStore`; ``/expand`` reads it back from
there — never from the connector.
"""
from __future__ import annotations

import contextlib
import time
from dataclasses import dataclass, field
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

# /expand renders the full result; a cap this large means _capped_indexed never
# elides (total <= cap), so every row/item/field prints.
_UNCAPPED = 10**9


@dataclass
class ToolRecord:
    """One tool outcome, addressable by ``rid`` for /expand and /last-error."""

    rid: str
    tool_name: str
    success: bool
    error: str | None
    summary: str  # the one-line count label, searchable by /find
    elided: bool
    data: Any = None  # inline for small results
    handle: str | None = None  # spill handle for elided results (off-process)


@dataclass
class TranscriptLog:
    """In-session record of tool outcomes for the navigability commands.

    Small results stay inline; an elided (head/tail-capped) result is spilled to
    ``store`` so a long session doesn't pin every big table in memory, and
    ``/expand`` fetches it back by handle.
    """

    store: Any = None
    _records: list[ToolRecord] = field(default_factory=list)

    def add(self, tool_name: str, result: Any, summary: str, *, elided: bool) -> ToolRecord:
        rid = f"t{len(self._records) + 1}"
        data, handle = result.data, None
        if elided and result.success and self.store is not None:
            handle = self.store.write(tool_name, result.data)
            data = None
        rec = ToolRecord(
            rid, tool_name, bool(result.success), result.error, summary, elided, data, handle
        )
        self._records.append(rec)
        return rec

    def records(self) -> list[ToolRecord]:
        return list(self._records)

    def get(self, rid: str) -> ToolRecord | None:
        return next((r for r in self._records if r.rid == rid), None)

    def last_error(self) -> ToolRecord | None:
        return next((r for r in reversed(self._records) if not r.success), None)

    def resolve_data(self, rec: ToolRecord) -> Any:
        if rec.handle is not None and self.store is not None:
            return self.store.read(rec.handle).get("data")
        return rec.data

    def search(self, query: str) -> list[ToolRecord]:
        q = query.lower()
        return [
            r
            for r in self._records
            if q in (r.summary or "").lower()
            or q in (r.tool_name or "").lower()
            or q in (r.error or "").lower()
        ]


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

    def _result_body(self, data: Any, cap: int) -> tuple[str, RenderableType | None, bool]:
        """``(count label, body, elided)`` for one result, head/tail-capped at ``cap``."""
        gap = self.glyphs.ellipsis
        if isinstance(data, list) and data and isinstance(data[0], dict):
            label = f"{len(data)} row{'s' if len(data) != 1 else ''}"
            return label, _rows_table(data, max_rows=cap, ellipsis=gap), len(data) > cap
        if isinstance(data, list):
            label = f"{len(data)} item{'s' if len(data) != 1 else ''}"
            body: RenderableType | None = None
            if data:
                indexed, footer = _capped_indexed(data, cap, "items", ellipsis=gap)
                listing = Text(
                    "\n".join(gap if v is _GAP else f"{i}. {_cell(v)}" for i, v in indexed),
                    style="step",
                )
                body = Group(listing, footer) if footer else listing
            return label, body, len(data) > cap
        if isinstance(data, dict):
            label = f"{len(data)} field{'s' if len(data) != 1 else ''}"
            indexed, footer = _capped_indexed(list(data.items()), cap, "fields", ellipsis=gap)
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
            return label, (Group(kv, footer) if footer else kv), len(data) > cap
        if data is None:
            return "done", None, False
        return "done", Text(_cell(data), style="step"), False

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
            self.transcript_log.add(
                tool_name, result, str(result.error or "failed"), elided=False
            )
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

        cap = self._render_cap()
        label, body, elided = self._result_body(result.data, cap)
        rec = self.transcript_log.add(tool_name, result, label, elided=elided)
        # Rail color = semantics: a write/risky/irreversible action keeps its
        # blast-radius color on the result rail (tagged by the dispatcher);
        # plain reads stay calm. Errors use the error rail above.
        tier = (result.metadata or {}).get("tier")
        rail = TIER_STYLE.get(tier, "muted") if tier != "safe" else "muted"

        summary = Text()
        summary.append(f"{self.glyphs.ok} ", style="ok")
        summary.append(label, style="success")
        summary.append(
            f"  {self.glyphs.dot}  {result.execution_time_ms:.0f}ms"
            f"  {self.glyphs.dot} {rec.rid}",
            style="muted",
        )
        self.console.print(
            Padding(
                self._guttered(self.glyphs.result, rail, summary),
                (0, 0, 0, SPACING["indent"]),
            )
        )
        if body is not None:
            self.console.print(Padding(body, (0, 0, 0, 2 * SPACING["indent"])))
        if elided:
            self.console.print(
                Padding(
                    Text(f"{self.glyphs.hint} /expand {rec.rid} for all {label}", style="muted"),
                    (0, 0, 0, 2 * SPACING["indent"]),
                )
            )
        self.console.print()

    # ------------------------------------------------------------------
    # Navigability: /expand, /last-error, /find (P11)
    # ------------------------------------------------------------------
    def expand(self, rid: str) -> None:
        """Re-render a recorded tool result in full (from the spill, never re-run)."""
        rec = self.transcript_log.get(rid)
        if rec is None:
            self.notice(
                f"No result with id {rid!r}. The id is shown after a tool's result line.",
                style="warning",
            )
            return
        if not rec.success:
            self.notice(f"{rec.tool_name} failed: {rec.error or 'error'}", style="error")
            return
        label, body, _ = self._result_body(self.transcript_log.resolve_data(rec), _UNCAPPED)
        header = Text()
        header.append(f"{rec.tool_name} ", style="tool")
        header.append(f"{label} (full)", style="success")
        self.console.print(
            Padding(self._guttered(self.glyphs.result, "muted", header), (0, 0, 0, SPACING["indent"]))
        )
        if body is not None:
            self.console.print(Padding(body, (0, 0, 0, 2 * SPACING["indent"])))
        self.console.print()

    def last_error(self) -> None:
        """Re-render the most recent failed tool result, with its remediation hint."""
        rec = self.transcript_log.last_error()
        if rec is None:
            self.notice("No tool errors recorded this session.", style="success")
            return
        summary = Text()
        summary.append(f"{self.glyphs.err} ", style="bad")
        summary.append(f"{rec.tool_name}: ", style="muted")
        summary.append(rec.error or "failed", style="error")
        self.console.print(
            Padding(self._guttered(self.glyphs.result, "bad", summary), (0, 0, 0, SPACING["indent"]))
        )
        hint = _remediation_hint(rec.error)
        if hint:
            self.console.print(
                Padding(
                    Text(f"{self.glyphs.hint} {hint}", style="muted"),
                    (0, 0, 0, 2 * SPACING["indent"]),
                )
            )
        self.console.print()

    def find(self, query: str, history: list[Any]) -> None:
        """Print history turns and tool results matching ``query`` (case-insensitive)."""
        hits = 0
        for msg in history:
            content = getattr(msg, "content", "") or ""
            if query.lower() in content.lower():
                hits += 1
                role = getattr(msg, "role", "?")
                ts = (getattr(msg, "timestamp", "") or "")[:19]
                self._find_line(f"{role} {ts}".strip(), content, query)
        for rec in self.transcript_log.search(query):
            hits += 1
            label = f"{rec.rid} {rec.tool_name}"
            self._find_line(label, rec.error or rec.summary, query)
        if hits:
            self.notice(f"{hits} match{'es' if hits != 1 else ''} for {query!r}.", style="muted")
        else:
            self.notice(f"No matches for {query!r}.", style="muted")

    def _find_line(self, label: str, text: str, query: str) -> None:
        low = text.lower()
        i = low.find(query.lower())
        start = max(0, i - 30) if i >= 0 else 0
        window = text[start : start + 120].replace("\n", " ")
        snippet = ("…" if start > 0 else "") + window + ("…" if start + 120 < len(text) else "")
        line = Text()
        line.append(f"{label}  ", style="accent")
        body = Text(snippet, style="step")
        body.highlight_words([query], "reverse")
        line.append_text(body)
        self.console.print(Padding(line, (0, 0, 0, SPACING["indent"])))

    # ------------------------------------------------------------------
    # Approval / plan rendering (governance sign-off)
    # ------------------------------------------------------------------
