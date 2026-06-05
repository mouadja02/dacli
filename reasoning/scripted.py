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

from typing import Any, Dict, List, Optional, Tuple


class ScriptExhausted(RuntimeError):
    """Raised when ``generate()`` is called after the script is exhausted."""


class ScriptedLLM:
    """An offline LLM double satisfying the kernel's LLM contract."""

    def __init__(self, responses: List[Dict[str, Any]]):
        self._responses: List[Dict[str, Any]] = list(responses or [])
        self._i = 0
        #: Provider-normalized usage of the most recent generate() call.
        self.last_usage: Dict[str, int] = {}

    async def initialize(self) -> None:
        # No network, nothing to set up.
        return None

    async def generate(
        self,
        messages: Optional[List[Dict[str, Any]]] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        system_prompt: Optional[str] = None,
        on_text: Optional[Any] = None,
        model: Optional[str] = None,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        if self._i >= len(self._responses):
            raise ScriptExhausted(
                f"ScriptedLLM exhausted after {len(self._responses)} response(s): "
                "the agent requested another generation the scenario did not script."
            )
        spec = self._responses[self._i]
        self._i += 1

        text = spec.get("text") or ""
        self.last_usage = dict(spec.get("usage") or {})

        tool_calls: List[Dict[str, Any]] = []
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
            try:
                on_text(text)
            except Exception:
                pass

        return text, tool_calls
