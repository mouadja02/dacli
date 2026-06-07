"""Token counting for budget accounting.

A real tokenizer, offline and in the hot path: budget packing runs on every turn,
so it must not make network calls (reliability-first). We use ``tiktoken`` locally
for all providers — accurate for OpenAI/OpenRouter, and a close approximation for
Anthropic (``o200k_base``). Anthropic's *server-side* ``count_tokens`` is exposed
only as an optional one-shot calibration (:func:`anthropic_count_tokens`), never
inside the packing loop.

Everything goes through the :class:`TokenCounter` protocol so a different counter
can be injected later (e.g. a model-exact tokenizer in) without touching
callers. If ``tiktoken`` is somehow unavailable, we degrade to a chars/4 heuristic
rather than failing — counting is advisory for budgeting, never correctness.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

# Default encoding when a model isn't recognised. o200k_base is GPT-4o / o-series
# and a reasonable stand-in for Claude (no public exact tokenizer offline).
_DEFAULT_ENCODING = "o200k_base"


@runtime_checkable
class TokenCounter(Protocol):
    """Counts tokens for a string or a list of chat messages."""

    def count(self, text: str) -> int: ...

    def count_messages(self, messages: list[dict[str, Any]]) -> int: ...


def _message_text(message: dict[str, Any]) -> str:
    """Flatten a chat message to the text we charge tokens for.

    Covers plain ``content`` strings, ``tool_calls`` (name + arguments), and the
    OpenAI list-of-parts content shape. A small per-message overhead is added by
    the counter, not here.
    """
    parts: list[str] = []
    content = message.get("content")
    if isinstance(content, str):
        parts.append(content)
    elif isinstance(content, list):
        for block in content:
            if isinstance(block, dict):
                parts.append(str(block.get("text") or block.get("content") or ""))
            else:
                parts.append(str(block))
    elif content is not None:
        parts.append(str(content))

    for tc in message.get("tool_calls") or []:
        fn = tc.get("function") if isinstance(tc, dict) else None
        if isinstance(fn, dict):
            parts.append(str(fn.get("name", "")))
            parts.append(str(fn.get("arguments", "")))
    return "\n".join(p for p in parts if p)


class _BaseCounter:
    # Per-message structural overhead (role tag, delimiters). Mirrors the small
    # constant OpenAI documents per message; good enough for budgeting.
    PER_MESSAGE_OVERHEAD = 4

    def count(self, text: str) -> int:  # pragma: no cover - overridden
        raise NotImplementedError

    def count_messages(self, messages: list[dict[str, Any]]) -> int:
        total = 0
        for message in messages:
            total += self.count(_message_text(message)) + self.PER_MESSAGE_OVERHEAD
        return total


class HeuristicCounter(_BaseCounter):
    """chars/4 fallback when tiktoken is unavailable."""

    def count(self, text: str) -> int:
        if not text:
            return 0
        return max(1, len(text) // 4)


class TiktokenCounter(_BaseCounter):
    """tiktoken-backed counter; encoding chosen from provider/model."""

    def __init__(self, model: str | None = None, provider: str | None = None):
        import tiktoken  # local import so the module loads without the dep

        self._model = model
        self._provider = (provider or "").lower()
        enc = None
        if model and self._provider in ("openai", "openrouter"):
            try:
                enc = tiktoken.encoding_for_model(model)
            except KeyError:
                enc = None
        self._enc = enc or tiktoken.get_encoding(_DEFAULT_ENCODING)

    def count(self, text: str) -> int:
        if not text:
            return 0
        return len(self._enc.encode(text, disallowed_special=()))


def make_counter(settings: Any = None) -> TokenCounter:
    """Build the best available counter for the configured model/provider.

    Falls back to the chars/4 heuristic if tiktoken can't be imported, so the
    runtime never hard-depends on it for budgeting.
    """
    model = provider = None
    llm = getattr(settings, "llm", None) if settings is not None else None
    if llm is not None:
        model = getattr(llm, "model", None)
        provider = getattr(llm, "provider", None)
    try:
        return TiktokenCounter(model=model, provider=provider)
    except Exception:
        return HeuristicCounter()


def anthropic_count_tokens(client: Any, model: str, messages: list[dict[str, Any]]) -> int:
    """Optional one-shot calibration against Anthropic's server-side counter.

    NOT for the packing hot path — it makes a network call. Useful for offline
    validation of how close the local approximation is. Returns -1 if the API is
    unavailable rather than raising.
    """
    try:
        resp = client.messages.count_tokens(model=model, messages=messages)
        return int(getattr(resp, "input_tokens", -1))
    except Exception:
        return -1
