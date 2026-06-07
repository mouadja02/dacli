"""Long-horizon compaction.

When the conversation grows past the budget, older turns are summarized into a
compact note — preserving **decisions, created objects, unresolved errors, and
open TODOs**, discarding redundant tool chatter. The most recent turns are kept
verbatim.

Two reliability rules from the framework:

- **Budget pressure triggers compaction, not turn count.** Turn count is a poor
  proxy for token load when tool results vary wildly in size, so we measure
  actual tokens against the budget (:func:`needs_compaction`).
- **Compaction never destroys.** The summary is written to durable memory with
  provenance, and the raw conversation history remains on disk
  (``history_*.json``); compaction only rewrites the *in-memory working copy* the
  model sees. A dropped fact is always recoverable, and decisions should be
  re-anchored to live-env facts rather than trusting an Nth-generation summary.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any
from collections.abc import Callable

COMPACTION_SYSTEM = (
    "You compact conversation history for an autonomous data-engineering agent. "
    "Produce a terse, factual note that PRESERVES: decisions made, objects created "
    "or modified (schemas/tables/files/workflows) with their exact names, "
    "unresolved errors, and open TODOs. DISCARD redundant tool chatter and "
    "resolved intermediate steps. Use compact bullet points. Do not invent facts."
)


@dataclass
class CompactionResult:
    messages: list[dict[str, Any]]   # new working message list (note + recent)
    note: str                        # the summary text (also stored to memory)
    compacted_count: int             # how many old messages were folded in


def _history_tokens(messages: list[dict[str, Any]], counter: Any) -> int:
    return counter.count_messages(messages)


def needs_compaction(
    messages: list[dict[str, Any]],
    counter: Any,
    token_budget: int,
    *,
    pressure: float = 0.9,
    keep_recent: int = 6,
) -> bool:
    """True when history tokens exceed ``pressure × token_budget`` and there is
    something foldable (more messages than we'd keep)."""
    if len(messages) <= keep_recent + 1:
        return False
    return _history_tokens(messages, counter) > pressure * token_budget


def _render_transcript(messages: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for m in messages:
        role = m.get("role", "?")
        content = m.get("content")
        if content is None and m.get("tool_calls"):
            calls = ", ".join(
                tc.get("function", {}).get("name", "?") for tc in m["tool_calls"]
            )
            content = f"(tool calls: {calls})"
        lines.append(f"{role.upper()}: {content}")
    return "\n".join(lines)


async def compact(
    messages: list[dict[str, Any]],
    llm: Any,
    *,
    keep_recent: int = 6,
    store_fn: Callable[[str], Any] | None = None,
) -> CompactionResult:
    """Fold the oldest turns into a summary note, keeping the recent tail verbatim.

    Does **not** mutate ``messages``. ``store_fn`` (e.g. ``memory.remember_fact``)
    persists the note to durable memory with provenance. If the LLM summary fails,
    falls back to a non-destructive marker so the loop never breaks.
    """
    old = messages[:-keep_recent] if keep_recent else list(messages)
    recent = messages[-keep_recent:] if keep_recent else []
    if not old:
        return CompactionResult(messages=list(messages), note="", compacted_count=0)

    transcript = _render_transcript(old)
    prompt = (
        "Compact the following earlier conversation into a preservation note.\n\n"
        f"{transcript}"
    )
    try:
        note, _ = await llm.generate(
            messages=[{"role": "user", "content": prompt}],
            tools=None,
            system_prompt=COMPACTION_SYSTEM,
        )
    except Exception:
        note = ""
    note = (note or "").strip()
    if not note:
        # Reliability: never drop content we couldn't summarize. Keep everything.
        return CompactionResult(messages=list(messages), note="", compacted_count=0)

    if store_fn is not None:
        # persistence is best-effort; must not break the loop
        with contextlib.suppress(Exception):
            store_fn(note)

    note_msg = {
        "role": "user",
        "content": f"[Earlier conversation summary — {len(old)} turns compacted]\n{note}",
    }
    return CompactionResult(
        messages=[note_msg, *recent],
        note=note,
        compacted_count=len(old),
    )
