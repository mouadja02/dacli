"""Per-provider LLM implementations behind the :class:`Provider` protocol (A-2).

Each provider owns its SDK client, request shaping, streaming reassembly, and
usage normalization, and **declares** its capabilities (``supports_tools``) so
"does this provider support tools?" is a configure-time property, not a
turn-time surprise. The :class:`~dacli.ai.llm.LLMClient` facade selects
a provider in ``initialize()`` and delegates; bounded retry/backoff stays in
the facade and is injected as ``retry`` so no provider duplicates it.
"""

from __future__ import annotations

import json
import contextlib
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from dacli.config.settings import Settings
    from dacli.ai.llm import OnRetry, OnText


def emit(on_text: OnText, delta: str) -> None:
    # Stream a delta to the UI without ever letting a rendering error break
    # generation (reliability-first).
    if on_text and delta:
        with contextlib.suppress(Exception):
            on_text(delta)


class Provider(ABC):
    """One LLM provider: declared capabilities + the request/stream mechanics.

    ``retry`` is the facade's ``_with_retry`` (bounded, jittered exponential
    backoff) — shared, never reimplemented per provider. ``client`` is the
    provider's SDK client, built lazily in :meth:`initialize` with the SDK's
    own retries disabled so the configured count is authoritative.
    """

    name: ClassVar[str] = ""
    supports_tools: ClassVar[bool] = True

    def __init__(self, settings: Settings, *, retry):
        self.settings = settings
        self._retry = retry
        self.client: Any = None
        # Provider-normalized token usage of the most recent generate() call;
        # the facade copies it onto its own ``last_usage`` after delegating.
        self.last_usage: dict[str, int] = {}

    @abstractmethod
    async def initialize(self) -> None:
        """Construct the SDK client (``max_retries=0`` — retry is ours)."""

    @abstractmethod
    async def generate(
        self,
        messages: list[dict[str, str]],
        tools: list[dict] | None = None,
        system_prompt: str | None = None,
        on_text: OnText = None,
        model: str | None = None,
        on_retry: OnRetry = None,
    ) -> tuple[str, list[dict]]:
        """Return ``(content, tool_calls)`` for one completion."""

    @abstractmethod
    def normalize_usage(self, raw) -> dict[str, int]:
        """Map the provider's raw usage object onto the shared usage dict."""

    def retryable_exceptions(self) -> tuple[type, ...]:
        # Provider-specific *transient* error classes that are safe to retry
        # (429 rate limit, dropped connection, 5xx). Auth / 4xx-validation
        # errors are deliberately excluded so they fail fast. Imported lazily
        # (mirroring initialize()) and tolerant of a missing SDK -> () means
        # "retry nothing", so a transient blip simply surfaces unchanged.
        return ()


class OpenAIProvider(Provider):
    """OpenAI (and any OpenAI-compatible endpoint via ``base_url``)."""

    name = "openai"

    async def initialize(self) -> None:
        from openai import AsyncOpenAI
        self.client = AsyncOpenAI(
            api_key=self.settings.llm.api_key,
            base_url=self.settings.llm.base_url,
            timeout=self.settings.llm.timeout,
            max_retries=0,
        )

    def retryable_exceptions(self) -> tuple[type, ...]:
        try:
            from openai import (
                RateLimitError,
                APIConnectionError,
                InternalServerError,
            )
        except ImportError:
            return ()
        return (RateLimitError, APIConnectionError, InternalServerError)

    async def generate(self, messages: list[dict[str, str]], tools: list[dict] | None = None, system_prompt: str | None = None, on_text: OnText = None, model: str | None = None, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
        # Generate using OpenAI-compatibile API

        # Prepare messages includes system prompt
        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        # Prepare request
        request_kwargs = {
            "model": model or self.settings.llm.model,
            "messages": full_messages,
            "temperature": self.settings.llm.temperature,
            "max_tokens": self.settings.llm.max_tokens,
        }

        # Add tools if provided
        if tools:
            request_kwargs["tools"] = tools
            request_kwargs["tool_choice"] = "auto"

        if on_text is not None:
            return await self._stream(request_kwargs, on_text, on_retry=on_retry)

        # Make request (retried on transient errors; permanent errors fail fast).
        response = await self._retry(
            lambda: self.client.chat.completions.create(**request_kwargs),
            on_retry=on_retry,
            retryable=self.retryable_exceptions(),
        )

        # Extract response
        choice = response.choices[0]
        content = choice.message.content or ""

        # Extract tool calls
        tool_calls = []
        if hasattr(choice.message, "tool_calls") and choice.message.tool_calls:
            tool_calls.extend(
                {"id": tc.id, "name": tc.function.name, "arguments": json.loads(tc.function.arguments)}
                for tc in choice.message.tool_calls
            )

        self.last_usage = self.normalize_usage(getattr(response, "usage", None))
        return content, tool_calls

    async def _stream(self, request_kwargs: dict, on_text: OnText, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
        # Streaming variant: accumulate text deltas (emitted live) and reassemble
        # tool calls, which arrive as indexed fragments across chunks.
        request_kwargs = {**request_kwargs, "stream": True, "stream_options": {"include_usage": True}}

        # The whole stream is retried as a unit: a transient error while
        # establishing *or* consuming the stream restarts it from scratch.
        # at-most-once-token caveat — partial deltas already emitted to the UI on
        # a failed attempt are discarded; the restart re-emits from the top.
        async def _do() -> tuple[str, list[dict]]:
            stream = await self.client.chat.completions.create(**request_kwargs)

            content = ""
            usage_obj = None
            # index -> {"id", "name", "arguments"(str)}
            acc: dict[int, dict[str, str]] = {}

            async for chunk in stream:
                if getattr(chunk, "usage", None):
                    usage_obj = chunk.usage  # final usage chunk (include_usage)
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                if getattr(delta, "content", None):
                    content += delta.content
                    emit(on_text, delta.content)
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

            self.last_usage = self.normalize_usage(usage_obj)
            return content, tool_calls

        return await self._retry(_do, on_retry=on_retry, retryable=self.retryable_exceptions())

    def normalize_usage(self, raw) -> dict[str, int]:
        # OpenAI prompt_tokens includes cached tokens -> split them out so cost
        # isn't double-counted.
        if raw is None:
            return {}
        details = getattr(raw, "prompt_tokens_details", None)
        cached = (getattr(details, "cached_tokens", 0) or 0) if details is not None else 0
        return {
            "input": max(0, (getattr(raw, "prompt_tokens", 0) or 0) - cached),
            "output": getattr(raw, "completion_tokens", 0) or 0,
            "cache_read": cached,
            "cache_creation": 0,
        }


class OpenRouterProvider(OpenAIProvider):
    """OpenRouter — OpenAI-compatible, with the OpenRouter endpoint default."""

    name = "openrouter"

    async def initialize(self) -> None:
        from openai import AsyncOpenAI
        self.client = AsyncOpenAI(
            api_key=self.settings.llm.api_key,
            base_url=self.settings.llm.base_url or "https://openrouter.ai/api/v1",
            timeout=self.settings.llm.timeout,
            max_retries=0,
        )


class AnthropicProvider(Provider):
    """Anthropic Messages API."""

    name = "anthropic"

    async def initialize(self) -> None:
        from anthropic import AsyncAnthropic
        self.client = AsyncAnthropic(
            api_key=self.settings.llm.api_key,
            base_url=self.settings.llm.base_url,
            timeout=self.settings.llm.timeout,
            max_retries=0,
        )

    def retryable_exceptions(self) -> tuple[type, ...]:
        try:
            from anthropic import (
                RateLimitError,
                APIConnectionError,
                InternalServerError,
            )
        except ImportError:
            return ()
        return (RateLimitError, APIConnectionError, InternalServerError)

    async def generate(self, messages: list[dict[str, str]], tools: list[dict] | None = None, system_prompt: str | None = None, on_text: OnText = None, model: str | None = None, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
        # Generate using Anthropic API
        # Prepare request
        request_kwargs = {
            "model": model or self.settings.llm.model,
            "max_tokens": self.settings.llm.max_tokens,
            "messages": messages,
        }

        if system_prompt:
            request_kwargs["system"] = system_prompt

        # Convert tools to Anthropic format
        if tools:
            request_kwargs["tools"] = [
                {
                    "name": tool["function"]["name"],
                    "description": tool["function"]["description"],
                    "input_schema": tool["function"]["parameters"]
                }
                for tool in tools
            ]

        if on_text is not None:
            return await self._stream(request_kwargs, on_text, on_retry=on_retry)

        async def _do() -> tuple[str, list[dict]]:
            response = await self.client.messages.create(**request_kwargs)
            self.last_usage = self.normalize_usage(getattr(response, "usage", None))
            return self._extract(response.content)

        return await self._retry(_do, on_retry=on_retry, retryable=self.retryable_exceptions())

    async def _stream(self, request_kwargs: dict, on_text: OnText, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
        # Streaming variant: emit text events live, then read the assembled
        # final message for the authoritative content + tool_use blocks. The
        # whole stream is retried as a unit (see OpenAIProvider._stream for the
        # at-most-once-token caveat on restart).
        async def _do() -> tuple[str, list[dict]]:
            async with self.client.messages.stream(**request_kwargs) as stream:
                async for text in stream.text_stream:
                    emit(on_text, text)
                final = await stream.get_final_message()
            self.last_usage = self.normalize_usage(getattr(final, "usage", None))
            return self._extract(final.content)

        return await self._retry(_do, on_retry=on_retry, retryable=self.retryable_exceptions())

    def normalize_usage(self, raw) -> dict[str, int]:
        # Anthropic reports cache tokens as fields separate from input_tokens.
        if raw is None:
            return {}
        return {
            "input": getattr(raw, "input_tokens", 0) or 0,
            "output": getattr(raw, "output_tokens", 0) or 0,
            "cache_read": getattr(raw, "cache_read_input_tokens", 0) or 0,
            "cache_creation": getattr(raw, "cache_creation_input_tokens", 0) or 0,
        }

    @staticmethod
    def _extract(blocks) -> tuple[str, list[dict]]:
        content = ""
        tool_calls = []
        for block in blocks:
            if block.type == "text":
                content += block.text
            elif block.type == "tool_use":
                tool_calls.append({"id": block.id, "name": block.name, "arguments": block.input})
        return content, tool_calls


class GoogleProvider(Provider):
    """Gemini — declared, but it never supported tool calling (P02, Option B).

    ``supports_tools = False`` is the load-bearing property: the facade rejects
    it at configure time with a clear error instead of a turn-time
    ``NotImplementedError``. The methods below are defensive for direct use.
    """

    name = "google"
    supports_tools = False

    async def initialize(self) -> None:
        raise unsupported_tools_error(self.name)

    async def generate(self, messages: list[dict[str, str]], tools: list[dict] | None = None, system_prompt: str | None = None, on_text: OnText = None, model: str | None = None, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
        raise unsupported_tools_error(self.name)

    def normalize_usage(self, raw) -> dict[str, int]:
        return {}


# Registry order is also the order alternatives are suggested in errors.
PROVIDERS: dict[str, type[Provider]] = {
    "openai": OpenAIProvider,
    "anthropic": AnthropicProvider,
    "google": GoogleProvider,
    "openrouter": OpenRouterProvider,
}


def unsupported_tools_error(name: str) -> ValueError:
    """The configure-time error for a provider that declares no tool support."""
    capable = [n for n, cls in PROVIDERS.items() if cls.supports_tools]
    alternatives = ", ".join(f"'{n}'" for n in capable[:-1]) + f", or '{capable[-1]}'"
    return ValueError(
        f"The '{name}' provider does not yet support tool use, which dacli requires. "
        f"Use {alternatives}."
    )


def create_provider(name: str, settings: Settings, *, retry) -> Provider:
    """Instantiate the provider registered under ``name`` (already lowercased)."""
    cls = PROVIDERS.get(name)
    if cls is None:
        raise ValueError(f"Unsupported LLM provider: {name}")
    return cls(settings, retry=retry)
