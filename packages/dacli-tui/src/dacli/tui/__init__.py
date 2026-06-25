"""DACLI terminal UI package (Rich-based)."""

from .design import ASCII, SPACING, TIER_STYLE, UNICODE, Glyphs, gauge, resolve_glyphs
from .ui import DacliUI, StreamView
from .theme import THEMES, DEFAULT_THEME, get_theme, ThemeSpec

__all__ = [
    "ASCII",
    "DEFAULT_THEME",
    "SPACING",
    "THEMES",
    "TIER_STYLE",
    "UNICODE",
    "DacliUI",
    "Glyphs",
    "StreamView",
    "ThemeSpec",
    "gauge",
    "get_theme",
    "resolve_glyphs",
]

__version__ = "0.3.0"
