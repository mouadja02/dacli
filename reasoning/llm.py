import json
import asyncio
from typing import Callable, Dict, List, Optional, Tuple

from config.settings import Settings

# Type of the optional streaming callback: receives each text delta as it
# arrives. Returning None; it is presentation-only and must not raise into the
# generate path (the UI guards its own rendering).
OnText = Optional[Callable[[str], None]]


class LLMClient:
    # Multi-provider LLM client

    def __init__(self, settings: Settings):
        # Initialize LLM client with settings
        self.settings = settings
        self._client = None
        self._provider = settings.llm.provider

    async def initialize(self) -> None:
        # Initialize the LLM client based on the provider
        provider = self._provider.lower()

        if provider == "openai":
            from openai import AsyncOpenAI
            self._client = AsyncOpenAI(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url,
                timeout=self.settings.llm.timeout,
            )
        elif provider == "anthropic":
            from anthropic import AsyncAnthropic
            self._client = AsyncAnthropic(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url,
                timeout=self.settings.llm.timeout,
            )
        elif provider == "google":
            from google.generativeai import genai
            self._client = genai.Client(api_key=self.settings.llm.api_key)
        elif provider == "openrouter":
            from openai import AsyncOpenAI
            self._client = AsyncOpenAI(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url or "https://openrouter.ai/api/v1",
                timeout=self.settings.llm.timeout
            )
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")

    async def generate(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None, system_prompt: Optional[str] = None, on_text: OnText = None) -> Tuple[str, List[Dict]]:
        """
        Generate a response from the LLM.

        Args:
            messages: Conversation messages
            tools: Available tool definitions
            system_prompt: System prompt to use
            on_text: Optional callback invoked with each text delta as it is
                generated. When provided (and the provider supports it) the
                response is streamed; the return value is unchanged. Providers
                without streaming call it once with the full text instead, so
                the UI behaves identically.

        Returns:
            Tuple of (response content, tool calls)
        """
        if not self._client:
            await self.initialize()

        provider = self._provider.lower()
        if provider in ["openai", "openrouter"]:
            return await self._generate_openai(messages, tools, system_prompt, on_text=on_text)
        elif provider == "anthropic":
            return await self._generate_anthropic(messages, tools, system_prompt, on_text=on_text)
        elif provider == "google":
            return await self._generate_google(messages, tools, system_prompt, on_text=on_text)
        else:
            raise ValueError(f"Unsupported provider: {provider}")

    @staticmethod
    def _emit(on_text: OnText, delta: str) -> None:
        # Stream a delta to the UI without ever letting a rendering error break
        # generation (reliability-first).
        if on_text and delta:
            try:
                on_text(delta)
            except Exception:
                pass

    async def classify(self, text: str, labels: List[str], instructions: Optional[str] = None) -> str:
        """
        Thin classification helper used by the router (Phase 4).

        Sends a tool-free completion asking the model to pick exactly one label
        from ``labels`` and normalizes the answer back onto that set when possible.
        """
        system = instructions or (
            "You are a classifier. Respond with exactly one of the allowed labels "
            "and nothing else."
        )
        label_list = ", ".join(labels)
        prompt = (
            f"Allowed labels: {label_list}\n\n"
            f"Text to classify:\n{text}\n\n"
            "Respond with exactly one label from the allowed list."
        )
        content, _ = await self.generate(
            messages=[{"role": "user", "content": prompt}],
            tools=None,
            system_prompt=system,
        )
        answer = (content or "").strip()

        # Exact (case-insensitive) match first, then substring fallback.
        for label in labels:
            if answer.lower() == label.lower():
                return label
        for label in labels:
            if label.lower() in answer.lower():
                return label
        return answer

    async def _generate_openai(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None, system_prompt: Optional[str] = None, on_text: OnText = None) -> Tuple[str, List[Dict]]:
        # Generate using OpenAI-compatibile API

        # Prepare messages includes system prompt
        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        # Prepare request
        request_kwargs = {
            "model": self.settings.llm.model,
            "messages": full_messages,
            "temperature": self.settings.llm.temperature,
            "max_tokens": self.settings.llm.max_tokens,
        }

        # Add tools if provided
        if tools:
            request_kwargs["tools"] = tools
            request_kwargs["tool_choice"] = "auto"

        if on_text is not None:
            return await self._stream_openai(request_kwargs, on_text)

        # Make request
        response = await self._client.chat.completions.create(**request_kwargs)

        # Extract response
        choice = response.choices[0]
        content = choice.message.content or ""

        # Extract tool calls
        tool_calls = []
        if hasattr(choice.message, "tool_calls") and choice.message.tool_calls:
            for tc in choice.message.tool_calls:
                tool_calls.append({"id": tc.id, "name": tc.function.name, "arguments": json.loads(tc.function.arguments)})

        return content, tool_calls

    async def _stream_openai(self, request_kwargs: Dict, on_text: OnText) -> Tuple[str, List[Dict]]:
        # Streaming variant: accumulate text deltas (emitted live) and reassemble
        # tool calls, which arrive as indexed fragments across chunks.
        request_kwargs = {**request_kwargs, "stream": True}
        stream = await self._client.chat.completions.create(**request_kwargs)

        content = ""
        # index -> {"id", "name", "arguments"(str)}
        acc: Dict[int, Dict[str, str]] = {}

        async for chunk in stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if getattr(delta, "content", None):
                content += delta.content
                self._emit(on_text, delta.content)
            for tc in (getattr(delta, "tool_calls", None) or []):
                slot = acc.setdefault(tc.index, {"id": "", "name": "", "arguments": ""})
                if getattr(tc, "id", None):
                    slot["id"] = tc.id
                fn = getattr(tc, "function", None)
                if fn is not None:
                    if getattr(fn, "name", None):
                        slot["name"] = fn.name
                    if getattr(fn, "arguments", None):
                        slot["arguments"] += fn.arguments

        tool_calls = []
        for index in sorted(acc):
            slot = acc[index]
            if not slot["name"]:
                continue
            try:
                arguments = json.loads(slot["arguments"] or "{}")
            except json.JSONDecodeError:
                arguments = {}
            tool_calls.append({"id": slot["id"], "name": slot["name"], "arguments": arguments})

        return content, tool_calls

    async def _generate_anthropic(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None, system_prompt: Optional[str] = None, on_text: OnText = None) -> Tuple[str, List[Dict]]:
        # Generate using Anthropic API
        # Prepare request
        request_kwargs = {
            "model": self.settings.llm.model,
            "max_tokens": self.settings.llm.max_tokens,
            "messages": messages,
        }

        if system_prompt:
            request_kwargs["system"] = system_prompt

        # Convert tools to Anthropic format
        if tools:
            anthropic_tools = []
            for tool in tools:
                anthropic_tools.append({
                    "name": tool["function"]["name"],
                    "description": tool["function"]["description"],
                    "input_schema": tool["function"]["parameters"]
                })
            request_kwargs["tools"] = anthropic_tools

        if on_text is not None:
            return await self._stream_anthropic(request_kwargs, on_text)

        response = await self._client.messages.create(**request_kwargs)
        return self._extract_anthropic(response.content)

    async def _stream_anthropic(self, request_kwargs: Dict, on_text: OnText) -> Tuple[str, List[Dict]]:
        # Streaming variant: emit text events live, then read the assembled
        # final message for the authoritative content + tool_use blocks.
        async with self._client.messages.stream(**request_kwargs) as stream:
            async for text in stream.text_stream:
                self._emit(on_text, text)
            final = await stream.get_final_message()
        return self._extract_anthropic(final.content)

    @staticmethod
    def _extract_anthropic(blocks) -> Tuple[str, List[Dict]]:
        content = ""
        tool_calls = []
        for block in blocks:
            if block.type == "text":
                content += block.text
            elif block.type == "tool_use":
                tool_calls.append({"id": block.id, "name": block.name, "arguments": block.input})
        return content, tool_calls

    async def _generate_google(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None, system_prompt: Optional[str] = None, on_text: OnText = None) -> Tuple[str, List[Dict]]:
        # Generate using Google Gemini API

        # Reliability: tool calling is not implemented for Gemini. Rather than
        # silently dropping tool use (which would make the agent appear to ignore
        # its tools), fail loudly so the misconfiguration is visible.
        if tools:
            raise NotImplementedError(
                "Tool calling is not supported for the 'google' provider yet. "
                "Use a provider with tool support (openai, anthropic, openrouter) "
                "or disable tools for this request."
            )

        model = self._client.GenerativeModel(self.settings.llm.model, system_instruction=system_prompt)

        # Convert messages to Gemini format
        gemini_messages = []
        for msg in messages:
            role = "user" if msg["role"] == "user" else "model"
            gemini_messages.append({"role": role, "parts": [msg["content"]]})

        response = await asyncio.to_thread(model.generate_content, gemini_messages)

        # Gemini has no streaming path here; emit the full text once so the
        # streaming UI still renders it.
        self._emit(on_text, response.text)
        return response.text, []
