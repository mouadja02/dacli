"""Selection-policy context assembler (Phase 3.1).

Replaces the fixed message window (``memory.get_context_messages``) with a
*policy*: gather candidate chunks from each layer, rank them, and pack to a token
budget with the highest-value content at the **head and tail** (to dodge
lost-in-the-middle), tagging every chunk with its provenance.

Layering (framework's three-layer model + skills + history):

- **L1 priors**   — ``DACLI.md`` + connection profiles. Pinned, always present.
- **L3 live-env** — fresh introspected structure (catalog cache). *Outranks L2*
  when both answer the same question: freshness beats cached confidence.
- **L2 memory**   — ranked durable facts (hypotheses; re-verify before acting).
- **skills**      — the connectors digest (progressive disclosure, 3.3).
- **history**     — recent conversation turns; the current task and the latest
  tool result are pinned unconditionally.

Placement strategy: priors + live + memory + digest go into the **system prompt**
(the head); genuine conversation turns stay in ``messages`` with the current task
as the **tail**. This achieves head/tail emphasis structurally, without injecting
synthetic-role messages that can confuse tool-calling models. Within the
retrieval block, candidates are ranked and only those that fit their budget cap
are kept.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

from context.budget import (
    Budget,
    BudgetTracker,
    HISTORY,
    LIVE,
    MEMORY,
    PINNED,
    PRIORS,
    SKILLS,
)
from context.tokenizer import TokenCounter

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def _tokens(text: str) -> Set[str]:
    return {t.lower() for t in _TOKEN_RE.findall(text or "")}


def _overlap(task_tokens: Set[str], doc: str) -> float:
    if not task_tokens:
        return 0.0
    d = _tokens(doc)
    if not d:
        return 0.0
    hit = len(task_tokens & d)
    return hit / len(task_tokens) if hit else 0.0


@dataclass
class ContextChunk:
    """One placed piece of context, with provenance (3.1 / `--explain`)."""

    source: str                 # PRIORS | LIVE | MEMORY | SKILLS | HISTORY | "task"
    label: str                  # short human label (e.g. "DACLI.md", "msg[-1] user")
    tokens: int
    pinned: bool = False
    timestamp: Optional[str] = None
    text: str = ""              # retained for --explain; not re-sent anywhere

    def explain_row(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "label": self.label,
            "tokens": self.tokens,
            "pinned": self.pinned,
            "timestamp": self.timestamp or "",
        }


@dataclass
class Context:
    """The assembled, inspectable context handed to the LLM for one turn."""

    system_prompt: str
    messages: List[Dict[str, Any]]
    tools: List[Dict[str, Any]]
    chunks: List[ContextChunk] = field(default_factory=list)
    budget: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def explain(self) -> List[Dict[str, Any]]:
        """Per-chunk provenance rows for ``dacli context --explain``."""
        return [c.explain_row() for c in self.chunks]


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _format_catalog_entry(entry: Any) -> str:
    scope = getattr(entry, "scope", {}) or {}
    name = ".".join(
        str(scope[k]) for k in ("database", "schema", "object") if scope.get(k)
    ) or "(unscoped)"
    bits = [f"{getattr(entry, 'connector', '?')} {getattr(entry, 'object_type', 'object')} {name}"]
    rce = getattr(entry, "row_count_estimate", None)
    if rce is not None:
        bits.append(f"~{rce} rows")
    if getattr(entry, "is_stale", None) and entry.is_stale():
        bits.append("STALE — re-verify")
    return " · ".join(bits)


def _section(title: str, lines: Sequence[str]) -> str:
    body = "\n".join(lines)
    return f"## {title}\n{body}"


def build_context(
    task: str,
    *,
    memory: Any,
    registry: Any,
    recent_messages: List[Dict[str, Any]],
    counter: TokenCounter,
    budget: Budget,
    disclosed: Optional[Set[str]] = None,
    base_system_prompt: str = "",
    priors_text: Optional[str] = None,
    live_provider: Optional[Callable[[str], List[Any]]] = None,
    max_memory: int = 5,
    max_live: int = 8,
) -> Context:
    """Assemble one turn of context under ``budget``.

    Args mirror the framework's ``build_context(task, budget)`` with the
    collaborators injected (memory, registry, the conversation so far, and the
    token counter). ``disclosed`` is the progressive-disclosure set (3.3);
    ``base_system_prompt`` is the invariant core (3.6 supplies it). ``priors_text``
    overrides priors loading (tests); ``live_provider`` is the pluggable
    introspection hook (defaults to the catalog cache).
    """
    tracker = BudgetTracker(budget)
    chunks: List[ContextChunk] = []
    task_tokens = _tokens(task)

    # -- L1 priors (pinned, always) ------------------------------------------
    if priors_text is None:
        try:
            from memory.priors import load_priors
            priors_text = load_priors()
        except Exception:
            priors_text = ""
    priors_section = ""
    if priors_text:
        ptok = counter.count(priors_text)
        tracker.add(PRIORS, ptok, pinned=True)
        chunks.append(ContextChunk(PRIORS, "DACLI.md priors", ptok, pinned=True, timestamp=_now_iso(), text=priors_text))
        priors_section = _section("Persistent priors", [priors_text])

    # -- L3 live-env (fresh, authoritative) ----------------------------------
    live_entries: List[Any] = []
    if live_provider is not None:
        try:
            live_entries = list(live_provider(task)) or []
        except Exception:
            live_entries = []
    elif getattr(memory, "catalog", None) is not None:
        live_entries = memory.catalog.list_objects()
    # Rank by task overlap (freshness implicit: stale entries are labelled).
    ranked_live = sorted(
        live_entries,
        key=lambda e: _overlap(task_tokens, _format_catalog_entry(e)),
        reverse=True,
    )
    live_lines: List[str] = []
    for entry in ranked_live[:max_live]:
        line = _format_catalog_entry(entry)
        ltok = counter.count(line)
        if not tracker.add(LIVE, ltok):
            break
        ts = getattr(entry, "last_verified", None)
        chunks.append(ContextChunk(LIVE, line[:60], ltok, timestamp=ts.isoformat() if hasattr(ts, "isoformat") else _now_iso(), text=line))
        live_lines.append(f"- {line}")
    live_section = _section("Live environment (verified structure)", live_lines) if live_lines else ""

    # -- L2 memory (ranked hypotheses) ---------------------------------------
    mem_entries: List[Any] = []
    if hasattr(memory, "retrieve"):
        try:
            mem_entries = memory.retrieve(task, top_k=max_memory) or []
        except Exception:
            mem_entries = []
    mem_lines: List[str] = []
    for entry in mem_entries:
        content = getattr(entry, "content", str(entry))
        mtok = counter.count(content)
        if not tracker.add(MEMORY, mtok):
            break
        ts = getattr(entry, "last_verified", None)
        chunks.append(ContextChunk(MEMORY, content[:60], mtok, timestamp=ts.isoformat() if hasattr(ts, "isoformat") else _now_iso(), text=content))
        mem_lines.append(f"- {content}")
    mem_section = _section("Relevant memory (hypotheses — re-verify before risky actions)", mem_lines) if mem_lines else ""

    # -- skills / connectors digest (progressive disclosure) -----------------
    digest = registry.get_tool_digest() if hasattr(registry, "get_tool_digest") else []
    digest_lines: List[str] = []
    for e in digest:
        line = f"- {e['id']} ({e.get('name', e['id'])}): {e.get('description', '')} [{e.get('operations', '?')} ops]"
        dtok = counter.count(line)
        if not tracker.add(SKILLS, dtok):
            break
        digest_lines.append(line)
    digest_section = ""
    if digest_lines:
        note = "Only names + descriptions are shown. Call load_connector_tools(connector_id) to disclose a connector's full operations before using it."
        digest_section = _section("Available connectors", [note, *digest_lines])

    # -- compose the system prompt (the head) --------------------------------
    system_prompt = "\n\n".join(
        s for s in (base_system_prompt, priors_section, live_section, mem_section, digest_section) if s
    )

    # -- history (recent turns; task + latest tool result pinned, task at tail)
    messages = _select_history(recent_messages, counter, tracker, chunks)

    # -- tools (only disclosed connectors' full schemas + built-ins) ---------
    if hasattr(registry, "get_tool_definitions"):
        tools = registry.get_tool_definitions(connector_ids=disclosed if disclosed is not None else None)
    else:
        tools = []

    return Context(
        system_prompt=system_prompt,
        messages=messages,
        tools=tools,
        chunks=chunks,
        budget=tracker.snapshot(),
    )


def _last_index(messages: List[Dict[str, Any]], role: str) -> int:
    for i in range(len(messages) - 1, -1, -1):
        if messages[i].get("role") == role:
            return i
    return -1


def _select_history(
    recent_messages: List[Dict[str, Any]],
    counter: TokenCounter,
    tracker: BudgetTracker,
    chunks: List[ContextChunk],
) -> List[Dict[str, Any]]:
    """Record + charge the conversation turns, returning them **intact**.

    Crucially, this does **not** drop messages: dropping a ``tool`` result while
    keeping its assistant ``tool_calls`` (or vice versa) would split a pair and
    break the provider tool protocol. History reduction is the job of
    *compaction* (``context.compaction``), which rewrites the list coherently
    when budget pressure is detected.

    So here we include every turn, pin the current task (last user message) and
    the latest tool result, and charge the budget — letting HISTORY usage exceed
    its cap. That overflow is precisely the compaction trigger; it is recorded in
    the budget snapshot rather than silently resolved by dropping content.
    """
    n = len(recent_messages)
    if n == 0:
        return []

    last_user = _last_index(recent_messages, "user")
    last_tool = _last_index(recent_messages, "tool")

    out: List[Dict[str, Any]] = []
    for i, msg in enumerate(recent_messages):
        tok = counter.count_messages([msg])
        is_task = i == last_user
        pinned = i == last_user or (last_tool >= 0 and i == last_tool)
        # charge() records usage without rejecting — preserves tool pairing.
        tracker.charge(PINNED if pinned else HISTORY, tok)
        chunks.append(ContextChunk(
            "task" if is_task else HISTORY,
            f"msg[{i}] {msg.get('role', '?')}" + (" (current task)" if is_task else ""),
            tok,
            pinned=pinned,
            timestamp=msg.get("timestamp"),
            text=str(msg.get("content", ""))[:200],
        ))
        out.append(msg)
    return out
