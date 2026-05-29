"""Orchestration kernel (𝒪).

The iteration loop, extracted verbatim from the old ``DACLI.process_message``.
It owns nothing platform-specific: it talks only to ``reasoning`` (generate),
the ``Dispatcher`` (execute), and ``memory`` (context in/out).
"""

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

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
    ):
        self._llm = llm
        self._dispatcher = dispatcher
        self._memory = memory
        self._tools = tools
        self._system_prompt = system_prompt
        self._max_iterations = max_iterations
        self._on_status_update = on_status_update
        self._current_iteration = 0

    def _emit_status(self, message: str) -> None:
        if self._on_status_update:
            self._on_status_update(message)

    async def orchestrate(self, user_message: str) -> AgentResponse:
        # Process a user message and generate a response.
        # Add user message to memory
        self._memory.add_user_message(user_message)

        # get conversation context
        messages = self._memory.get_context_messages()

        # Iteration loop
        self._current_iteration = 0

        while self._current_iteration < self._max_iterations:
            self._current_iteration += 1
            self._emit_status(f"Iteration {self._current_iteration}/{self._max_iterations}")

            try:
                # Generate response from LLM
                content, tool_calls = await self._llm.generate(
                    messages=messages,
                    tools=self._tools,
                    system_prompt=self._system_prompt
                )

                # If no tool call, we have a final response
                if not tool_calls:
                    self._memory.add_assistant_message(content)
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
                messages.append(assistant_msg)

                for tool_call in tool_calls:
                    tool_name = tool_call["name"]
                    tool_call_id = tool_call["id"]
                    arguments = tool_call.get("arguments", {})

                    result = await self._dispatcher.execute(tool_name, arguments)
                    tool_results.append(result)

                    # Check if user input is needed
                    if result.status == ToolStatus.PENDING_APPROVAL:
                        needs_user_input = True
                        break

                    # Add tool result to messages for next iteration
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "content": result.to_message()
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
        return AgentResponse(
            content="Maximum iterations reached. Please provide more guidance.",
            tool_calls=[],
            needs_user_input=True,
            iteration=self._current_iteration
        )
