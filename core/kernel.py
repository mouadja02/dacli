"""Orchestration kernel (𝒪).

The iteration loop, extracted verbatim from the old ``DACLI.process_message``.
It owns nothing platform-specific: it talks only to ``reasoning`` (generate),
the ``Dispatcher`` (execute), and ``memory`` (context in/out).
"""

import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from connectors.base import ToolStatus


@dataclass
class AgentResponse:
    # Response from the agent
    content: str
    tool_calls: List[Dict[str, Any]]
    thinking: Optional[str] = None
    needs_user_input: bool = False
    error: Optional[str] = None
    iteration: int = 0


class Kernel:
    def __init__(
        self,
        llm: Any,
        dispatcher: Any,
        memory: Any,
        tools: List[Dict[str, Any]],
        system_prompt: str,
        max_iterations: int,
        on_status_update: Optional[Callable[[str], None]] = None,
        on_stream_start: Optional[Callable[[], None]] = None,
        on_text: Optional[Callable[[str], None]] = None,
        on_stream_end: Optional[Callable[[str], None]] = None,
        context_builder: Optional[Callable[[str, List[Dict[str, Any]], Set[str]], Any]] = None,
        result_spill: Optional[Callable[[Any], str]] = None,
        maybe_compact: Optional[Callable[[List[Dict[str, Any]]], Awaitable[List[Dict[str, Any]]]]] = None,
        on_usage: Optional[Callable[[Dict[str, int], str], None]] = None,
        on_retry: Optional[Callable[..., None]] = None,
    ):
        self._llm = llm
        self._dispatcher = dispatcher
        self._memory = memory
        self._tools = tools
        self._system_prompt = system_prompt
        self._max_iterations = max_iterations
        self._on_status_update = on_status_update
        # (Context Constructor) collaborators — all optional. When
        # ``context_builder`` is None the kernel runs the legacy fixed-window
        # path verbatim (this is what keeps the golden transcript green):
        # - ``context_builder(task, working, disclosed) -> Context`` re-assembles
        #   the system prompt, tools and messages each iteration (3.1/3.3/3.6).
        # - ``result_spill(result) -> str`` produces the model-facing tool message
        #   (off-context spill of large results, 3.4).
        # - ``maybe_compact(working) -> working`` compacts older turns under
        #   budget pressure (3.5).
        self._context_builder = context_builder
        self._result_spill = result_spill
        self._maybe_compact = maybe_compact
        # Streaming hooks: emitted around each LLM generation so the UI can show
        # tokens as they arrive. All optional — the loop is identical without them.
        self._on_stream_start = on_stream_start
        self._on_text = on_text
        self._on_stream_end = on_stream_end
        # Usage sink: called after each LLM call with (usage_dict, user_message)
        # so the agent can price + persist token consumption (optional).
        self._on_usage = on_usage
        # Retry-status sink (P05): called once per LLM retry so the TUI can show
        # "⟳ retrying in 2.1s …". Defaults to the kernel's own status emitter so
        # a retried turn is never silent even before P13. Only forwarded to the
        # LLM when the client supports it (test doubles may not).
        self._on_retry = on_retry or self._emit_retry
        self._llm_accepts_on_retry = self._supports_on_retry(llm)
        self._current_iteration = 0

    @staticmethod
    def _supports_on_retry(llm: Any) -> bool:
        # True when llm.generate accepts an ``on_retry`` kwarg, so test doubles
        # with a narrower signature are never handed an unexpected argument.
        import inspect

        try:
            params = inspect.signature(llm.generate).parameters
        except (TypeError, ValueError, AttributeError):
            return False
        if "on_retry" in params:
            return True
        return any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )

    def _emit_retry(self, *, attempt: int, delay: float, error: Exception) -> None:
        # Surface a transient LLM failure + backoff on the status line.
        self._emit_status(
            f"⟳ retrying in {delay:.1f}s ({type(error).__name__}) "
            f"— attempt {attempt}"
        )

    def _emit_status(self, message: str) -> None:
        if self._on_status_update:
            self._on_status_update(message)

    def _begin_stream(self) -> None:
        if self._on_stream_start:
            self._on_stream_start()

    def _end_stream(self, content: str) -> None:
        if self._on_stream_end:
            self._on_stream_end(content)

    def _capture_episode(self, goal: str, trace: List[Dict[str, Any]], outcome: str) -> None:
        # Episodic capture (2.5). Guarded: a memory without capture_episode
        # (e.g. the golden-transcript fake) is simply skipped.
        if not trace:
            return
        capture = getattr(self._memory, "capture_episode", None)
        if capture is None:
            return
        try:
            capture(goal, trace, outcome)
        except Exception:
            pass  # episodic capture must never break the control loop

    def _seed_working(self) -> List[Dict[str, Any]]:
        """Seed the working conversation list.

        New path: the fixed window is gone — seed from the *full*
        history and let the assembler/compaction manage tokens. Legacy path:
        keep the existing windowed slice so behavior is unchanged.
        """
        if self._context_builder is not None and hasattr(self._memory, "get_full_history"):
            return [
                {"role": m.role, "content": m.content}
                for m in self._memory.get_full_history()
            ]
        return list(self._memory.get_context_messages())

    def _assemble(
        self, task: str, working: List[Dict[str, Any]], disclosed: Set[str]
    ):
        """Return ``(messages, tools, system_prompt)`` for this iteration.

        New path delegates to the injected context builder; legacy path returns
        the working list with the static tools + system prompt (verbatim old
        behavior).
        """
        if self._context_builder is None:
            return working, self._tools, self._system_prompt
        ctx = self._context_builder(task, working, disclosed)
        return ctx.messages, ctx.tools, ctx.system_prompt

    def _tool_message_content(self, result: Any) -> str:
        """Model-facing tool message: spilled summary (new) or inline (legacy)."""
        if self._result_spill is not None:
            return self._result_spill(result)
        return result.to_message()

    async def orchestrate(self, user_message: str, *, model: Optional[str] = None) -> AgentResponse:
        # Process a user message and generate a response.
        # ``model`` (model tiering, ℛ) overrides the LLM model for *this*
        # run only; None preserves the configured default, so the single-model
        # path — and the golden transcript — is byte-for-byte unchanged.
        # Add user message to memory
        self._memory.add_user_message(user_message)

        # Working conversation that accumulates across iterations. In the new
        # path the assembler re-packs this into the LLM messages each turn; in
        # the legacy path it *is* the LLM messages.
        working = self._seed_working()

        # Connectors disclosed this turn (progressive disclosure, 3.3): seeded
        # empty and grown when the model calls load_connector_tools.
        disclosed: Set[str] = set()

        # Trace of tool calls -> outcomes for episodic capture (2.5).
        trace: List[Dict[str, Any]] = []

        # Iteration loop
        self._current_iteration = 0

        while self._current_iteration < self._max_iterations:
            self._current_iteration += 1
            self._emit_status(f"Iteration {self._current_iteration}/{self._max_iterations}")

            try:
                # Compaction under budget pressure (new path only; no-op legacy).
                if self._maybe_compact is not None:
                    working = await self._maybe_compact(working)

                # Assemble this iteration's context (system prompt, tools, msgs).
                messages, tools, system_prompt = self._assemble(
                    user_message, working, disclosed
                )

                # Generate response from LLM (streamed to the UI when wired).
                content = ""
                self._begin_stream()
                try:
                    gen_kwargs = dict(
                        messages=messages,
                        tools=tools,
                        system_prompt=system_prompt,
                        on_text=self._on_text,
                    )
                    # Only thread ``model`` when set — keeps the call signature
                    # identical to the legacy path (and to test doubles that don't
                    # accept a ``model`` kwarg) unless tiering is actually in use.
                    if model is not None:
                        gen_kwargs["model"] = model
                    # Likewise thread the retry-status sink only when the client
                    # supports it (P05); test doubles without ``on_retry`` are
                    # left untouched.
                    if self._llm_accepts_on_retry:
                        gen_kwargs["on_retry"] = self._on_retry
                    content, tool_calls = await self._llm.generate(**gen_kwargs)
                finally:
                    # Always tear the live region down — even if generation
                    # raised — so the terminal is never left mid-stream.
                    self._end_stream(content)

                # Record token usage/cost for this call (best-effort, optional).
                if self._on_usage is not None:
                    self._on_usage(getattr(self._llm, "last_usage", {}) or {}, user_message)

                # If no tool call, we have a final response
                if not tool_calls:
                    self._memory.add_assistant_message(content)
                    self._capture_episode(user_message, trace, "completed")
                    return AgentResponse(
                        content=content,
                        tool_calls=[],
                        iteration=self._current_iteration
                    )

                # Execute tool calls sequentially to avoid race conditions
                tool_results = []
                needs_user_input = False

                # Add assistant message with tool_calls ONCE before processing
                assistant_msg = {
                    "role": "assistant",
                    "content": content or None,
                    "tool_calls": [
                        {
                            "id": tc["id"],
                            "type": "function",
                            "function": {
                                "name": tc["name"],
                                "arguments": json.dumps(tc["arguments"])
                            }
                        }
                        for tc in tool_calls
                    ]
                }
                working.append(assistant_msg)

                for tool_call in tool_calls:
                    tool_name = tool_call["name"]
                    tool_call_id = tool_call["id"]
                    arguments = tool_call.get("arguments", {})

                    result = await self._dispatcher.execute(tool_name, arguments)
                    tool_results.append(result)
                    trace.append({
                        "tool": tool_name,
                        "arguments": arguments,
                        "status": getattr(result.status, "value", str(result.status)),
                        "error": result.error,
                    })

                    # Progressive disclosure (3.3): a load_connector_tools call
                    # asks for a connector's full schemas on the next iteration.
                    disclose_id = (getattr(result, "metadata", None) or {}).get("disclose")
                    if disclose_id:
                        disclosed.add(disclose_id)

                    # Check if user input is needed
                    if result.status == ToolStatus.PENDING_APPROVAL:
                        needs_user_input = True
                        break

                    # Add tool result for next iteration. The model-facing content
                    # is spilled (3.4) when large; the human still sees the full
                    # table via the UI callback in the dispatcher.
                    working.append({
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "content": self._tool_message_content(result)
                    })

                # If user input is needed, return early
                if needs_user_input:
                    return AgentResponse(
                        content=content,
                        tool_calls=tool_calls,
                        needs_user_input=True,
                        iteration=self._current_iteration
                    )

            except Exception as e:
                return AgentResponse(
                    content="",
                    tool_calls=[],
                    error=str(e),
                    iteration=self._current_iteration
                )

        # Max iterations reached
        self._capture_episode(user_message, trace, "incomplete")
        return AgentResponse(
            content="Maximum iterations reached. Please provide more guidance.",
            tool_calls=[],
            needs_user_input=True,
            iteration=self._current_iteration
        )
