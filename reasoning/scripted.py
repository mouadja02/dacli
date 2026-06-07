"""Deterministic, offline stand-in for :class:`reasoning.llm.LLMClient`.

Driven by an ordered list of scripted *responses*; each ``generate()`` call pops
the next one and returns it in the exact shape the kernel parses
(``core/kernel.py``): ``(content, tool_calls)`` where each tool call is
``{"id", "name", "arguments"}``. An empty ``tool_calls`` ends the agent loop
(final answer). Running past the end raises :class:`ScriptExhausted` — a real
signal that the agent looped more than the scenario anticipated.

A scripted response is a dict::

    {
      "text": "optional assistant text",
      "tool_calls": [ {"name": "update_plan", "arguments": {...}} ],  # optional
      "usage": {"input": 100, "output": 20},                          # optional
    }
"""

from __future__ import annotations

from typing import Any
import contextlib


class ScriptExhausted(RuntimeError):
    """Raised when ``generate()`` is called after the script is exhausted."""


class ScriptedLLM:
    """An offline LLM double satisfying the kernel's LLM contract."""

    def __init__(self, responses: list[dict[str, Any]]):
        self._responses: list[dict[str, Any]] = list(responses or [])
        self._i = 0
        #: Provider-normalized usage of the most recent generate() call.
        self.last_usage: dict[str, int] = {}
        self.exhausted: bool = False

    async def initialize(self) -> None:
        # No network, nothing to set up.
        return None

    async def generate(
        self,
        messages: list[dict[str, Any]] | None = None,
        tools: list[dict[str, Any]] | None = None,
        system_prompt: str | None = None,
        on_text: Any | None = None,
        model: str | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        if self._i >= len(self._responses):
            self.exhausted = True
            raise ScriptExhausted(
                f"ScriptedLLM exhausted after {len(self._responses)} response(s): "
                "the agent requested another generation the scenario did not script."
            )
        spec = self._responses[self._i]
        self._i += 1

        text = spec.get("text") or ""
        self.last_usage = dict(spec.get("usage") or {})

        tool_calls: list[dict[str, Any]] = []
        for j, tc in enumerate(spec.get("tool_calls") or [], start=1):
            tool_calls.append(
                {
                    "id": tc.get("id") or f"call_{self._i}_{j}",
                    "name": tc["name"],
                    "arguments": tc.get("arguments") or {},
                }
            )

        # Presentation parity with streaming providers (headless on_text is a
        # no-op; the chat UI streams). Never let a presentation hook break us.
        if on_text and text:
            with contextlib.suppress(Exception):
                on_text(text)

        return text, tool_calls
