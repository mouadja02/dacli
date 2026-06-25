"""First-class context sources (𝒞).

A *source* exposes some part of the live environment to the context assembler as
a provenance-tagged, budget-aware layer. Today:

- :mod:`context.sources.terminal` — the governed terminal's scrollback, with a
  just-in-time fetch handle so a 10k-line command output never enters the model's
  context verbatim (it spills to the session workspace and is summarised).
"""

from dacli.context.sources.terminal import ScrollbackStore, ScrollbackSource

__all__ = ["ScrollbackSource", "ScrollbackStore"]
