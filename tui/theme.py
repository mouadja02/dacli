"""Color themes for the DACLI terminal UI.

A theme is just a named :class:`rich.theme.Theme` plus a couple of palette
constants the UI uses outside of Rich's style system (the banner gradient and
the prompt-toolkit bottom bar). Every theme defines the *same* set of style
keys so the rest of the UI never has to know which theme is active.

Reliability note: themes are presentation-only. Nothing here touches agent
behavior, config, or credentials — a bad/unknown theme name simply falls back
to ``dark``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from rich.theme import Theme

# The complete set of semantic style keys every theme must provide. Keeping
# this list explicit means a half-written theme fails loudly in tests rather
# than rendering blank styles at runtime.
STYLE_KEYS = (
    "info", "warning", "error", "success", "prompt",
    "tool", "sql", "user", "assistant", "phase", "step",
    "accent", "muted", "gutter", "border", "ok", "bad",
)


@dataclass(frozen=True)
class ThemeSpec:
    """A named theme: Rich styles + the two raw-color hooks the UI needs."""

    name: str
    styles: Dict[str, str]
    # Per-line colors for the ASCII wordmark (cycled top-to-bottom).
    banner_gradient: List[str]
    # prompt-toolkit bottom-bar colors: (foreground, background).
    toolbar_fg: str
    toolbar_bg: str

    def rich_theme(self) -> Theme:
        return Theme(self.styles)


_DARK = ThemeSpec(
    name="dark",
    styles={
        "info": "cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "bold green",
        "prompt": "bold #8be9fd",
        "tool": "#bd93f9",
        "sql": "#50fa7b",
        "user": "bold #f8f8f2",
        "assistant": "#8be9fd",
        "phase": "bold yellow",
        "step": "dim white",
        "accent": "#bd93f9",
        "muted": "dim #6272a4",
        "gutter": "#bd93f9",
        "border": "#44475a",
        "ok": "bold #50fa7b",
        "bad": "bold #ff5555",
    },
    banner_gradient=["#bd93f9", "#a06ff2", "#8be9fd", "#6fb8f0", "#50fa7b"],
    toolbar_fg="#bd93f9",
    toolbar_bg="#1e1f29",
)

_LIGHT = ThemeSpec(
    name="light",
    styles={
        "info": "#0066cc",
        "warning": "#b58900",
        "error": "bold #cc0000",
        "success": "bold #2e7d32",
        "prompt": "bold #6f42c1",
        "tool": "#6f42c1",
        "sql": "#2e7d32",
        "user": "bold #1a1a1a",
        "assistant": "#0066cc",
        "phase": "bold #b58900",
        "step": "#888888",
        "accent": "#6f42c1",
        "muted": "#999999",
        "gutter": "#6f42c1",
        "border": "#cccccc",
        "ok": "bold #2e7d32",
        "bad": "bold #cc0000",
    },
    banner_gradient=["#6f42c1", "#7d4fd0", "#0066cc", "#0088bb", "#2e7d32"],
    toolbar_fg="#6f42c1",
    toolbar_bg="#eceff4",
)

_OCEAN = ThemeSpec(
    name="ocean",
    styles={
        "info": "#56b6c2",
        "warning": "#e5c07b",
        "error": "bold #e06c75",
        "success": "bold #98c379",
        "prompt": "bold #61afef",
        "tool": "#c678dd",
        "sql": "#98c379",
        "user": "bold #abb2bf",
        "assistant": "#61afef",
        "phase": "bold #e5c07b",
        "step": "dim #5c6370",
        "accent": "#61afef",
        "muted": "dim #5c6370",
        "gutter": "#61afef",
        "border": "#3e4451",
        "ok": "bold #98c379",
        "bad": "bold #e06c75",
    },
    banner_gradient=["#61afef", "#56b6c2", "#56b6c2", "#98c379", "#98c379"],
    toolbar_fg="#61afef",
    toolbar_bg="#21252b",
)

_MONO = ThemeSpec(
    name="mono",
    styles={
        "info": "white",
        "warning": "bold white",
        "error": "bold white on red",
        "success": "bold white",
        "prompt": "bold white",
        "tool": "white",
        "sql": "white",
        "user": "bold white",
        "assistant": "white",
        "phase": "bold white",
        "step": "dim white",
        "accent": "bold white",
        "muted": "dim white",
        "gutter": "white",
        "border": "grey50",
        "ok": "bold white",
        "bad": "bold white",
    },
    banner_gradient=["white", "grey85", "grey70", "grey85", "white"],
    # prompt-toolkit only accepts hex or ansi* color names — not Rich names
    # like "grey15" — so the toolbar colors are always hex.
    toolbar_fg="#e4e4e4",
    toolbar_bg="#303030",
)

THEMES: Dict[str, ThemeSpec] = {t.name: t for t in (_DARK, _LIGHT, _OCEAN, _MONO)}
DEFAULT_THEME = "dark"


def get_theme(name: str | None) -> ThemeSpec:
    """Return the theme spec for ``name``, falling back to the default."""
    if not name:
        return THEMES[DEFAULT_THEME]
    return THEMES.get(name.strip().lower(), THEMES[DEFAULT_THEME])
