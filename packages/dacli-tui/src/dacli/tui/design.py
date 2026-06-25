"""Design tokens for the DACLI terminal UI.

Single source of truth for **spacing, glyphs, box styles and semantic color
maps** so every renderer in ``tui/ui.py`` makes the same choices. Two glyph
sets exist: ``UNICODE`` for capable terminals and ``ASCII`` for everything
else (non-UTF-8 encodings, ``TERM=dumb``, ``NO_COLOR`` environments, or an
explicit ``ui.glyphs: ascii`` setting). :func:`resolve_glyphs` picks one.

Reliability note: this module is presentation-only and import-safe — it never
touches config, credentials or the control loop. Capability detection is
best-effort and degrades to ASCII on any doubt.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from rich import box
from rich.text import Text


@dataclass(frozen=True)
class Glyphs:
    """One coherent glyph set; every UI marker comes from here."""

    # Transcript gutter markers.
    agent: str
    tool: str
    result: str
    user_caret: str
    # Status icons (always paired with color, never color-only).
    ok: str
    warn: str
    err: str
    info: str
    pending: str
    running: str
    paused: str
    # Connector enablement dots.
    enabled: str
    disabled: str
    # Misc affordances.
    caret: str          # streaming tail caret
    hint: str           # remediation-hint arrow
    gauge_on: str       # filled cell of the context gauge
    gauge_off: str      # empty cell of the context gauge
    ellipsis: str
    dot: str            # separator dot in summaries ("· 340ms")
    dash: str           # placeholder dash in empty table cells
    arrows: str         # history-keys hint in the welcome tips
    delta: str          # change marker in diff/plan tables ("Δ rows")
    mult: str           # breadth-first multiplier ("×3")
    # Bottom-bar segment icons (empty in ASCII mode; labels carry meaning).
    bar_conn: str
    bar_ctx: str
    bar_session: str
    bar_sep: str
    # Spinner animation frames (one frame per char/element).
    spinner_frames: str
    # Box style for panels/tables.
    box: box.Box


UNICODE = Glyphs(
    agent="⏺", tool="⏺", result="⎿", user_caret="❯",
    ok="✓", warn="⚠", err="✗", info="ℹ", pending="○", running="◐", paused="⏸",
    enabled="●", disabled="○",
    caret="▌", hint="↳", gauge_on="▰", gauge_off="▱", ellipsis="…",
    dot="·", dash="—", arrows="↑↓", delta="Δ", mult="×",
    bar_conn="⛁ ", bar_ctx="◴ ", bar_session="⎇ ", bar_sep="│",
    spinner_frames="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏",
    box=box.ROUNDED,
)

ASCII = Glyphs(
    agent="*", tool="*", result=">", user_caret=">",
    ok="+", warn="!", err="x", info="i", pending="o", running="~", paused="=",
    enabled="*", disabled="o",
    caret="|", hint="->", gauge_on="#", gauge_off="-", ellipsis="...",
    dot=".", dash="-", arrows="Up/Down", delta="d", mult="x",
    bar_conn="", bar_ctx="", bar_session="", bar_sep="|",
    spinner_frames="|/-\\",
    box=box.ASCII,
)

# One spacing system: gutter width, body indent, panel padding, section gap.
SPACING: dict[str, Any] = {
    "gutter_w": 1,
    "indent": 2,
    "panel_pad": (1, 2),
    "section_gap": 1,
}

# Blast-radius tier → semantic style. Shared by the audit view, the plan
# preview and the approval panel so "risky" reads the same color everywhere.
TIER_STYLE = {
    "safe": "success",
    "write": "info",
    "risky": "warning",
    "irreversible": "error",
}


def tier_legend(dot: str = "·") -> Text:
    """A one-line ``safe · write · risky · irreversible`` key in tier colors.

    Shared under the dense governance panels (plan, audit, approval) so the
    blast-radius colors are decodable without memorizing them.
    """
    legend = Text()
    for i, (name, style) in enumerate(TIER_STYLE.items()):
        if i:
            legend.append(f"  {dot}  ", style="muted")
        legend.append(name, style=style)
    return legend


def gauge(pct: Any, glyphs: Glyphs, cells: int = 5) -> str:
    """Render a percentage as a tiny bar gauge, e.g. ``▰▰▰▱▱ 58%``.

    Defensive: a non-numeric ``pct`` renders as an empty gauge rather than
    raising (the status bar must never crash the input loop).
    """
    try:
        clamped = min(100, max(0, int(pct)))
    except Exception:
        clamped = 0
    filled = round(cells * clamped / 100)
    return glyphs.gauge_on * filled + glyphs.gauge_off * (cells - filled) + f" {clamped}%"


def _console_can_encode(console: Any, probe: str = "⏺⎿✓▰") -> bool:
    """Best-effort: can this console's encoding represent our glyphs?"""
    try:
        encoding = getattr(console.options, "encoding", "") or "ascii"
        probe.encode(encoding)
    except Exception:
        return False
    return True


def resolve_glyphs(console: Any, settings: Any = None) -> Glyphs:
    """Pick the glyph set for this console + settings.

    ASCII when: ``ui.glyphs == "ascii"``, the console can't encode Unicode,
    ``NO_COLOR`` is set, or the terminal is dumb. Unicode otherwise. An
    explicit ``ui.glyphs == "unicode"`` wins over the heuristics.
    """
    try:
        preference = str(
            getattr(getattr(settings, "ui", None), "glyphs", "auto") or "auto"
        ).strip().lower()
    except Exception:
        preference = "auto"
    if preference == "ascii":
        return ASCII
    if preference == "unicode":
        return UNICODE
    # auto: degrade on any capability doubt.
    if os.environ.get("NO_COLOR"):
        return ASCII
    if os.environ.get("TERM", "").lower() == "dumb":
        return ASCII
    if not _console_can_encode(console):
        return ASCII
    return UNICODE
