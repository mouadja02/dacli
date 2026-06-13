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
    styles: dict[str, str]
    # Per-line colors for the ASCII wordmark (cycled top-to-bottom).
    banner_gradient: list[str]
    # prompt-toolkit bottom-bar colors: (foreground, background).
    toolbar_fg: str
    toolbar_bg: str
    # Multiple palettes the UI can rotate through per session.
    banner_palettes: tuple = ()
    # Pygments theme for code blocks/SQL — matched to the palette so light
    # themes never render a dark code block. Unknown names fall back to
    # Pygments' default inside Rich, so this can never raise.
    code_theme: str = "monokai"

    @property
    def banner_gradients(self) -> list[str]:
        """Pick a palette for this session (rotates on each run)."""
        if not self.banner_palettes:
            return self.banner_gradient
        import random
        return list(random.choice(self.banner_palettes))

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
    # Professional palettes from coolors.co — curated for dark backgrounds.
    banner_palettes=(
        # Electric violet → cyan (original)
        ["#bd93f9", "#a06ff2", "#8be9fd", "#6fb8f0", "#50fa7b", "#bd93f9"],
        # Deep sea blues — coolors.co/palette/03045e-0077b6-00b4d8-90e0ef-caf0f8
        ["#03045e", "#0077b6", "#00b4d8", "#90e0ef", "#caf0f8", "#00b4d8"],
        # Neon spectrum — coolors.co/palette/f72585-7209b7-3a0ca3-4361ee-4cc9f0
        ["#f72585", "#b5179e", "#7209b7", "#4361ee", "#4cc9f0", "#f72585"],
        # Sunset fire — coolors.co/palette/03071e-9d0208-e85d04-f48c06-ffba08
        ["#ffba08", "#f48c06", "#e85d04", "#dc2f02", "#d00000", "#9d0208"],
        # Violet haze — coolors.co/palette/10002b-5a189a-7b2cbf-c77dff-e0aaff
        ["#10002b", "#3c096c", "#7b2cbf", "#9d4edd", "#c77dff", "#e0aaff"],
        # Teal cascade — coolors.co/palette/05668d-028090-00a896-02c39a-f0f3bd
        ["#05668d", "#028090", "#00a896", "#02c39a", "#80ffdb", "#56cfe1"],
        # Warm amber — coolors.co/palette/000814-001d3d-003566-ffc300-ffd60a
        ["#ffd60a", "#ffc300", "#003566", "#001d3d", "#003566", "#ffc300"],
        # Rose to steel — coolors.co/palette/355070-6d597a-b56576-e56b6f-eaac8b
        ["#355070", "#6d597a", "#b56576", "#e56b6f", "#eaac8b", "#355070"],
    ),
    toolbar_fg="#bd93f9",
    toolbar_bg="#1e1f29",
    code_theme="dracula",
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
    # Professional palettes from coolors.co — curated for light backgrounds.
    banner_palettes=(
        # Purple → blue → green (original)
        ["#6f42c1", "#7d4fd0", "#0066cc", "#0088bb", "#2e7d32", "#6f42c1"],
        # Navy authority — coolors.co/palette/0d1b2a-1b263b-415a77-778da9-e0e1dd
        ["#0d1b2a", "#1b263b", "#415a77", "#778da9", "#415a77", "#1b263b"],
        # Forest earth — coolors.co/palette/606c38-283618-dda15e-bc6c25
        ["#283618", "#606c38", "#283618", "#a98467", "#6c584c", "#283618"],
        # French flag — coolors.co/palette/e63946-457b9d-1d3557
        ["#1d3557", "#457b9d", "#a8dadc", "#e63946", "#1d3557", "#457b9d"],
        # Slate & coral — coolors.co/palette/2d3142-4f5d75-bfc0c0-ef8354
        ["#2d3142", "#4f5d75", "#ef8354", "#4f5d75", "#2d3142", "#4f5d75"],
        # Evergreen — coolors.co/palette/dad7cd-588157-3a5a40-344e41
        ["#344e41", "#3a5a40", "#588157", "#a3b18a", "#588157", "#344e41"],
        # Deep indigo — coolors.co/palette/22223b-4a4e69-9a8c98-c9ada7-f2e9e4
        ["#22223b", "#4a4e69", "#9a8c98", "#c9ada7", "#9a8c98", "#22223b"],
        # Cobalt gold — coolors.co/palette/00296b-003f88-00509d-fdc500-ffd500
        ["#00296b", "#003f88", "#00509d", "#fdc500", "#00509d", "#003f88"],
    ),
    toolbar_fg="#6f42c1",
    toolbar_bg="#eceff4",
    code_theme="default",
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
    # Professional palettes from coolors.co — curated for the ocean theme.
    banner_palettes=(
        # One Dark blues (original)
        ["#61afef", "#56b6c2", "#56b6c2", "#98c379", "#98c379", "#61afef"],
        # Ocean breeze — coolors.co/palette/8ecae6-219ebc-023047-ffb703-fb8500
        ["#023047", "#219ebc", "#8ecae6", "#ffb703", "#219ebc", "#023047"],
        # Coastal green — coolors.co/palette/cad2c5-84a98c-52796f-354f52-2f3e46
        ["#2f3e46", "#354f52", "#52796f", "#84a98c", "#52796f", "#354f52"],
        # Deep navy — coolors.co/palette/0d1b2a-1b263b-415a77-778da9-e0e1dd
        ["#0d1b2a", "#1b263b", "#415a77", "#778da9", "#e0e1dd", "#778da9"],
        # Coral reef — coolors.co/palette/006d77-83c5be-edf6f9-ffddd2-e29578
        ["#006d77", "#83c5be", "#edf6f9", "#ffddd2", "#e29578", "#006d77"],
        # Emerald cascade — coolors.co/palette/22577a-38a3a5-57cc99-80ed99-c7f9cc
        ["#22577a", "#38a3a5", "#57cc99", "#80ed99", "#57cc99", "#38a3a5"],
        # Sapphire teal — coolors.co/palette/012a4a-2a6f97-468faf-89c2d9-a9d6e5
        ["#012a4a", "#2a6f97", "#468faf", "#89c2d9", "#a9d6e5", "#89c2d9"],
    ),
    toolbar_fg="#61afef",
    toolbar_bg="#21252b",
    code_theme="one-dark",
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
    # Mono theme keeps a single greyscale palette — no rotation.
    # prompt-toolkit only accepts hex or ansi* color names — not Rich names
    # like "grey15" — so the toolbar colors are always hex.
    toolbar_fg="#e4e4e4",
    toolbar_bg="#303030",
    code_theme="bw",
)

_NORD = ThemeSpec(
    name="nord",
    styles={
        "info": "#88c0d0",
        "warning": "#ebcb8b",
        "error": "bold #bf616a",
        "success": "bold #a3be8c",
        "prompt": "bold #81a1c1",
        "tool": "#b48ead",
        "sql": "#a3be8c",
        "user": "bold #eceff4",
        "assistant": "#88c0d0",
        "phase": "bold #ebcb8b",
        "step": "#d8dee9",
        "accent": "#81a1c1",
        "muted": "#616e88",
        "gutter": "#81a1c1",
        "border": "#4c566a",
        "ok": "bold #a3be8c",
        "bad": "bold #bf616a",
    },
    banner_gradient=["#5e81ac", "#81a1c1", "#88c0d0", "#8fbcbb", "#a3be8c"],
    banner_palettes=(
        ["#5e81ac", "#81a1c1", "#88c0d0", "#8fbcbb", "#a3be8c", "#81a1c1"],
        ["#bf616a", "#d08770", "#ebcb8b", "#a3be8c", "#b48ead", "#bf616a"],
    ),
    toolbar_fg="#88c0d0",
    toolbar_bg="#2e3440",
    code_theme="nord",
)

_GRUVBOX = ThemeSpec(
    name="gruvbox",
    styles={
        "info": "#83a598",
        "warning": "#fabd2f",
        "error": "bold #fb4934",
        "success": "bold #b8bb26",
        "prompt": "bold #fe8019",
        "tool": "#d3869b",
        "sql": "#b8bb26",
        "user": "bold #ebdbb2",
        "assistant": "#83a598",
        "phase": "bold #fabd2f",
        "step": "#d5c4a1",
        "accent": "#fe8019",
        "muted": "#928374",
        "gutter": "#fe8019",
        "border": "#504945",
        "ok": "bold #b8bb26",
        "bad": "bold #fb4934",
    },
    banner_gradient=["#fb4934", "#fe8019", "#fabd2f", "#b8bb26", "#8ec07c"],
    banner_palettes=(
        ["#fb4934", "#fe8019", "#fabd2f", "#b8bb26", "#8ec07c", "#fe8019"],
        ["#83a598", "#8ec07c", "#b8bb26", "#fabd2f", "#fe8019", "#83a598"],
    ),
    toolbar_fg="#fabd2f",
    toolbar_bg="#282828",
    code_theme="gruvbox-dark",
)

# High-contrast theme (``ui.high_contrast`` forces it). WCAG-minded: no dim
# styles, near-white body text, saturated accents on a black background.
# Colorblind-safe by construction: every status color is paired with a glyph
# (the design-system invariant), and warning/info lean yellow/cyan rather
# than a red/green-only axis.
_CONTRAST = ThemeSpec(
    name="contrast",
    styles={
        "info": "bold #00ffff",
        "warning": "bold #ffff00",
        "error": "bold #ff5050",
        "success": "bold #00ff00",
        "prompt": "bold #ffffff",
        "tool": "bold #ff80ff",
        "sql": "#00ff00",
        "user": "bold #ffffff",
        "assistant": "#ffffff",
        "phase": "bold #ffff00",
        "step": "#f0f0f0",
        "accent": "bold #00ffff",
        "muted": "#c0c0c0",
        "gutter": "bold #00ffff",
        "border": "#ffffff",
        "ok": "bold #00ff00",
        "bad": "bold #ff5050",
    },
    banner_gradient=["#ffffff", "#00ffff", "#ffffff", "#00ffff", "#ffffff"],
    toolbar_fg="#ffffff",
    toolbar_bg="#000000",
    code_theme="github-dark",
)

THEMES: dict[str, ThemeSpec] = {
    t.name: t for t in (_DARK, _LIGHT, _OCEAN, _MONO, _NORD, _GRUVBOX, _CONTRAST)
}
DEFAULT_THEME = "dark"


def get_theme(name: str | None) -> ThemeSpec:
    """Return the theme spec for ``name``, falling back to the default."""
    if not name:
        return THEMES[DEFAULT_THEME]
    return THEMES.get(name.strip().lower(), THEMES[DEFAULT_THEME])
