"""Width-responsive table builder for the governance preview surfaces.

Rich folds a squeezed column into a vertical ribbon of fragments
("ro/ll/ba/ck") whenever the minimum column widths outgrow the terminal, and it
silently drops whole columns when things get tighter still — both of which gut
``plan``/``catalog``/``schema`` in an 80-column terminal, a tmux split, or a
piped (non-TTY) context. :func:`responsive_table` avoids both: short columns
never wrap (they ellipsize on one line), the free-text column ellipsizes too,
the least-important columns elide first when space runs out, and below
:data:`STACK_THRESHOLD` the whole table flips to a stacked key/value layout that
keeps every identifier intact.

Only the *rendering* is bounded — callers keep the full data.
"""

from __future__ import annotations

from dataclasses import dataclass

from rich.console import Group, JustifyMethod, RenderableType
from rich.table import Table
from rich.text import Text

# Below this console width a row-per-record table can't stay legible; stack it.
STACK_THRESHOLD = 70

# Inter-column padding the governance tables use ((0, 2, 0, 0)); counted when
# estimating whether the kept columns fit.
_PAD = 2

# A ratio (free-text) column flexes to fill leftover space, so when deciding
# what fits it only needs this floor, not its full content width.
_FLEX_FLOOR = 12


@dataclass
class Col:
    """One column's render contract for :func:`responsive_table`."""

    header: str
    style: str = "info"
    justify: JustifyMethod = "left"
    no_wrap: bool = True
    ratio: int | None = None      # set on the free-text column so it flexes
    max_width: int | None = None
    drop_rank: int = 0            # 0 = keep always; higher elides first
    primary: bool = False        # heading of each block in stacked mode


def responsive_table(
    console,
    cols: list[Col],
    rows: list[list[RenderableType | str]],
    *,
    header_style: str = "muted",
    reserve: int = 0,
) -> RenderableType:
    """A Rich table that degrades gracefully as the console narrows.

    ``rows`` are cell lists aligned to ``cols``; a cell is either a plain
    string (styled by its column) or a pre-styled :class:`~rich.text.Text`.
    ``reserve`` is width the caller spends on chrome the table renders inside
    (panel border + padding), so the fit math sees the real content width.
    """
    width = _width(console) - reserve
    if width < STACK_THRESHOLD:
        return _stacked(cols, rows)

    keep = _fit(cols, rows, width)
    table = Table(
        show_header=True, header_style=header_style, box=None, padding=(0, 2, 0, 0)
    )
    for j in keep:
        c = cols[j]
        table.add_column(
            c.header, style=c.style, justify=c.justify, no_wrap=c.no_wrap,
            overflow="ellipsis", ratio=c.ratio, max_width=c.max_width,
        )
    for row in rows:
        table.add_row(*(row[j] for j in keep))
    return table


def _width(console) -> int:
    try:
        return int(console.width)
    except Exception:
        return 80


def _plain(cell: RenderableType | str) -> str:
    return cell.plain if isinstance(cell, Text) else str(cell)


def _fit(cols: list[Col], rows: list[list[RenderableType | str]], width: int) -> list[int]:
    """Column indices to keep — drop the least important until the rest fit."""
    keep = list(range(len(cols)))

    def needed(idxs: list[int]) -> int:
        total = 0
        for j in idxs:
            c = cols[j]
            natural = max([len(c.header)] + [len(_plain(r[j])) for r in rows])
            # A ratio column flexes, so it only needs a small floor; a fixed
            # column needs its natural width (bounded by max_width).
            w = min(natural, _FLEX_FLOOR) if c.ratio else natural
            if c.max_width:
                w = min(w, c.max_width)
            total += w + _PAD
        return total

    def droppable() -> list[int]:
        return [j for j in keep if cols[j].drop_rank > 0]

    while needed(keep) > width and droppable():
        keep.remove(max(droppable(), key=lambda j: cols[j].drop_rank))
    return keep


def _as_text(cell: RenderableType | str, style: str) -> Text:
    return cell if isinstance(cell, Text) else Text(str(cell), style=style)


def _stacked(cols: list[Col], rows: list[list[RenderableType | str]]) -> RenderableType:
    """One key/value block per row — full identifiers, no char-stacking."""
    primary = next((i for i, c in enumerate(cols) if c.primary), 0)
    label_w = max(
        (len(c.header) for i, c in enumerate(cols) if i != primary and c.header),
        default=0,
    )
    blocks: list[RenderableType] = []
    for n, row in enumerate(rows):
        block = Text()
        if n:
            block.append("\n")
        block.append_text(_as_text(row[primary], "accent"))
        block.append("\n")
        for i, c in enumerate(cols):
            if i == primary or not c.header:
                continue
            val = _as_text(row[i], c.style)
            if not val.plain.strip():
                continue
            block.append(f"  {c.header:<{label_w}}  ", style="muted")
            block.append_text(val)
            block.append("\n")
        blocks.append(block)
    return Group(*blocks)
