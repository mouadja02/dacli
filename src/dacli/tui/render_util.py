"""Pure rendering helpers shared across the tui surfaces (extracted from ui.py, P10)."""
from __future__ import annotations

from typing import Any

from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text


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
