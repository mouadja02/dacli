"""Progressive disclosure of connectors.

The mechanism that makes "no MCP, many platforms" fit in a context window: the
system prompt lists connectors by id + a one-line description (the *digest*,
~zero tokens), and a connector's full operation schemas are packed **only when it
is disclosed** — either because the task is clearly about it, or because the
model explicitly called the ``load_connector_tools`` meta-tool.

Disclosure is intentionally simple here (lexical task↔connector overlap). In
 the router's classifier drives it; the interface (``disclose``) stays the
same. There is always a "browse all connectors" affordance: the full digest is in
the prompt and ``load_connector_tools`` can pull any connector on demand, so a
mis-scored connector is never *unreachable* — it just costs one extra step.
"""

from __future__ import annotations

import re
from typing import Any
from collections.abc import Iterable

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")

# How many connectors to auto-disclose from task relevance alone, and the
# minimum overlap score to qualify. Kept small: auto-disclosure is a convenience
# so the model needn't call load_connector_tools for the obvious connector; the
# meta-tool covers everything else.
DEFAULT_MAX_AUTO = 2
MIN_RELEVANCE = 0.10


def _tokens(text: str) -> set[str]:
    return {t.lower() for t in _TOKEN_RE.findall(text or "")}


def connector_relevance(task: str, entry: dict) -> float:
    """Token-overlap relevance of a task against one digest entry.

    Folds the connector id, display name, description and operation count label
    into the searchable text. Normalized by the query token count so a verbose
    description can't win on incidental matches.
    """
    q = _tokens(task)
    if not q:
        return 0.0
    doc = (
        _tokens(str(entry.get("id", "")))
        | _tokens(str(entry.get("name", "")))
        | _tokens(str(entry.get("description", "")))
    )
    if not doc:
        return 0.0
    overlap = len(q & doc)
    return overlap / len(q) if overlap else 0.0


def rank_connectors(task: str, digest: list[dict]) -> list[tuple]:
    """Return ``[(entry, score), ...]`` sorted by descending relevance (>0)."""
    scored = [(e, connector_relevance(task, e)) for e in digest]
    scored = [(e, s) for e, s in scored if s > 0]
    scored.sort(key=lambda es: es[1], reverse=True)
    return scored


def disclose(
    task: str,
    registry: Any,
    already_disclosed: Iterable[str] | None = None,
    *,
    max_auto: int = DEFAULT_MAX_AUTO,
    min_relevance: float = MIN_RELEVANCE,
) -> set[str]:
    """Compute the set of connector ids to disclose (full schemas) this turn.

    Union of:
    - ``already_disclosed`` — connectors the model explicitly loaded via the
      ``load_connector_tools`` meta-tool (sticky for the session), and
    - up to ``max_auto`` connectors whose task relevance clears
      ``min_relevance`` — a convenience so the obvious connector is ready without
      an extra round trip.

    Built-ins (``system``) are not part of this set: their tools are always live
    (see ``ConnectorRegistry.get_tool_definitions``).
    """
    disclosed: set[str] = set(already_disclosed or [])

    digest = registry.get_tool_digest()
    valid_ids = {e["id"] for e in digest}
    # Drop any stale/disabled ids that may linger in already_disclosed.
    disclosed &= valid_ids

    for entry, score in rank_connectors(task, digest):
        if len(disclosed) >= len(valid_ids):
            break
        if score < min_relevance:
            break
        if len([d for d in disclosed if d not in (already_disclosed or [])]) >= max_auto:
            break
        disclosed.add(entry["id"])

    return disclosed
