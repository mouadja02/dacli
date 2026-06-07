import json
import random
import asyncio
import logging
import contextlib
from typing import Any, TypeAlias
from collections.abc import Awaitable, Callable

from config.settings import Settings

logger = logging.getLogger(__name__)

# Type of the optional streaming callback: receives each text delta as it
# arrives. Returning None; it is presentation-only and must not raise into the
# generate path (the UI guards its own rendering).
OnText: TypeAlias = Callable[[str], None] | None

# Type of the optional retry-status callback. Invoked once per *retry* (not on
# the final failure) with the upcoming attempt number, the backoff delay about
# to be slept, and the transient error that triggered it, so the TUI/logger can
# render "⟳ retrying in 2.1s (429)".
OnRetry = Callable[..., None] | None


class LLMClient:
    # Multi-provider LLM client

    def __init__(self, settings: Settings):
        # Initialize LLM client with settings
        self.settings = settings
        # Concrete client type varies by provider (AsyncOpenAI / AsyncAnthropic /
        # genai.Client), each conditionally imported in initialize(); typed Any so
        # provider-specific attribute access (.chat, .messages, .GenerativeModel)
        # type-checks without a fragile union.
        self._client: Any = None
        self._provider = settings.llm.provider
        # Provider-normalized token usage of the most recent generate() call,
        # read by the kernel for cost tracking. Reset on each generate().
        self.last_usage: dict[str, int] = {}

    async def initialize(self) -> None:
        # Initialize the LLM client based on the provider
        provider = self._provider.lower()

        # ``max_retries=0`` hands all retry/backoff to our own ``_with_retry`` so
        # the configured count is authoritative (no compounding with the SDK's
        # built-in default) and every retry flows through the on_retry status
        # callback, including the streaming paths the SDK never retries.
        if provider == "openai":
            from openai import AsyncOpenAI
            self._client = AsyncOpenAI(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url,
                timeout=self.settings.llm.timeout,
                max_retries=0,
            )
        elif provider == "anthropic":
            from anthropic import AsyncAnthropic
            self._client = AsyncAnthropic(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url,
                timeout=self.settings.llm.timeout,
                max_retries=0,
            )
        elif provider == "google":
            from google import genai  # type: ignore  # optional google-genai dep, not installed in CI
            self._client = genai.Client(api_key=self.settings.llm.api_key)
        elif provider == "openrouter":
            from openai import AsyncOpenAI
            self._client = AsyncOpenAI(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url or "https://openrouter.ai/api/v1",
                timeout=self.settings.llm.timeout,
                max_retries=0,
            )
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")

    async def generate(self, messages: list[dict[str, str]], tools: list[dict] | None = None, system_prompt: str | None = None, on_text: OnText = None, model: str | None = None, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
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
            model: Optional per-call model override (model tiering, ℛ).
                When None the configured ``settings.llm.model`` is used, so the
                default single-model path is byte-for-byte unchanged. The
                ``ModelRouter`` passes the cheap or strong tier id here.

        Returns:
            Tuple of (response content, tool calls)
        """
        if not self._client:
            await self.initialize()

        self.last_usage = {}  # populated by the provider path below
        model = model or self.settings.llm.model
        provider = self._provider.lower()
        if provider in ["openai", "openrouter"]:
            return await self._generate_openai(messages, tools, system_prompt, on_text=on_text, model=model, on_retry=on_retry)
        if provider == "anthropic":
            return await self._generate_anthropic(messages, tools, system_prompt, on_text=on_text, model=model, on_retry=on_retry)
        if provider == "google":
            return await self._generate_google(messages, tools, system_prompt, on_text=on_text, model=model, on_retry=on_retry)
        raise ValueError(f"Unsupported provider: {provider}")

    def _retryable_exceptions(self) -> tuple[type, ...]:
        # Provider-specific *transient* error classes that are safe to retry
        # (429 rate limit, dropped connection, 5xx). Auth / 4xx-validation
        # errors are deliberately excluded so they fail fast. Imported lazily
        # (mirroring initialize()) and tolerant of a missing SDK -> () means
        # "retry nothing", so a transient blip simply surfaces unchanged.
        provider = self._provider.lower()
        try:
            if provider in ("openai", "openrouter"):
                from openai import (
                    RateLimitError,
                    APIConnectionError,
                    InternalServerError,
                )
                return (RateLimitError, APIConnectionError, InternalServerError)
            if provider == "anthropic":
                from anthropic import (
                    RateLimitError,
                    APIConnectionError,
                    InternalServerError,
                )
                return (RateLimitError, APIConnectionError, InternalServerError)
            if provider == "google":
                from google.api_core.exceptions import (
                    ResourceExhausted,
                    ServiceUnavailable,
                    InternalServerError as GoogleInternalServerError,
                )
                return (ResourceExhausted, ServiceUnavailable, GoogleInternalServerError)
        except ImportError:
            return ()
        return ()

    @staticmethod
    def _default_on_retry(*, attempt: int, delay: float, error: Exception) -> None:
        # Fallback status sink when no on_retry is wired (e.g. P13 TUI absent):
        # log the transient failure + backoff so a retried turn is never silent.
        logger.warning(
            "LLM call failed (%s); retrying in %.1fs (attempt %d)",
            type(error).__name__,
            delay,
            attempt,
        )

    async def _with_retry(
        self,
        fn: Callable[[], Awaitable],
        *,
        attempts: int | None = None,
        base: float | None = None,
        on_retry: OnRetry = None,
        retryable: tuple[type[BaseException], ...] | None = None,
    ):
        """Run ``fn`` with bounded, jittered exponential backoff (P05).

        ``fn`` is an argument-free coroutine factory invoked once per attempt;
        for streaming paths it re-establishes the stream from scratch on retry
        (at-most-once-token caveat: any partial tokens already emitted to the UI
        are discarded when the stream is restarted). Only ``retryable`` classes
        are retried — everything else propagates immediately (fail fast).
        """
        attempts = attempts or self.settings.llm.retry_attempts
        base = base if base is not None else self.settings.llm.retry_base_delay
        retryable = retryable if retryable is not None else self._retryable_exceptions()
        on_retry = on_retry or self._default_on_retry
        for i in range(attempts):
            try:
                return await fn()
            except retryable as e:
                if i == attempts - 1:
                    raise
                delay = base * 2 ** i + random.random() * 0.3
                # a status sink must never break the retry loop
                with contextlib.suppress(Exception):
                    on_retry(attempt=i + 1, delay=delay, error=e)
                await asyncio.sleep(delay)
        # Unreachable: attempts >= 1, so the loop always returns or raises. Kept
        # so the function provably never returns None.
        raise RuntimeError("_with_retry exhausted without returning or raising")

    @staticmethod
    def _emit(on_text: OnText, delta: str) -> None:
        # Stream a delta to the UI without ever letting a rendering error break
        # generation (reliability-first).
        if on_text and delta:
            with contextlib.suppress(Exception):
                on_text(delta)

    async def classify(self, text: str, labels: list[str], instructions: str | None = None, model: str | None = None) -> str:
        """
        Thin classification helper used by the router.

        Sends a tool-free completion asking the model to pick exactly one label
        from ``labels`` and normalizes the answer back onto that set when possible.
        ``model`` lets the caller force the cheap tier — classification
        is the canonical cheap-model job.
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
            model=model,
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

    async def _generate_openai(self, messages: list[dict[str, str]], tools: list[dict] | None = None, system_prompt: str | None = None, on_text: OnText = None, model: str | None = None, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
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
            return await self._stream_openai(request_kwargs, on_text, on_retry=on_retry)

        # Make request (retried on transient errors; permanent errors fail fast).
        response = await self._with_retry(
            lambda: self._client.chat.completions.create(**request_kwargs),
            on_retry=on_retry,
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

        self.last_usage = self._usage_openai(getattr(response, "usage", None))
        return content, tool_calls

    async def _stream_openai(self, request_kwargs: dict, on_text: OnText, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
        # Streaming variant: accumulate text deltas (emitted live) and reassemble
        # tool calls, which arrive as indexed fragments across chunks.
        request_kwargs = {**request_kwargs, "stream": True, "stream_options": {"include_usage": True}}

        # The whole stream is retried as a unit: a transient error while
        # establishing *or* consuming the stream restarts it from scratch.
        # at-most-once-token caveat — partial deltas already emitted to the UI on
        # a failed attempt are discarded; the restart re-emits from the top.
        async def _do() -> tuple[str, list[dict]]:
            stream = await self._client.chat.completions.create(**request_kwargs)

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

            self.last_usage = self._usage_openai(usage_obj)
            return content, tool_calls

        return await self._with_retry(_do, on_retry=on_retry)

    async def _generate_anthropic(self, messages: list[dict[str, str]], tools: list[dict] | None = None, system_prompt: str | None = None, on_text: OnText = None, model: str | None = None, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
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
            return await self._stream_anthropic(request_kwargs, on_text, on_retry=on_retry)

        async def _do() -> tuple[str, list[dict]]:
            response = await self._client.messages.create(**request_kwargs)
            self.last_usage = self._usage_anthropic(getattr(response, "usage", None))
            return self._extract_anthropic(response.content)

        return await self._with_retry(_do, on_retry=on_retry)

    async def _stream_anthropic(self, request_kwargs: dict, on_text: OnText, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
        # Streaming variant: emit text events live, then read the assembled
        # final message for the authoritative content + tool_use blocks. The
        # whole stream is retried as a unit (see _stream_openai for the
        # at-most-once-token caveat on restart).
        async def _do() -> tuple[str, list[dict]]:
            async with self._client.messages.stream(**request_kwargs) as stream:
                async for text in stream.text_stream:
                    self._emit(on_text, text)
                final = await stream.get_final_message()
            self.last_usage = self._usage_anthropic(getattr(final, "usage", None))
            return self._extract_anthropic(final.content)

        return await self._with_retry(_do, on_retry=on_retry)

    @staticmethod
    def _usage_openai(usage) -> dict[str, int]:
        # OpenAI prompt_tokens includes cached tokens -> split them out so cost
        # isn't double-counted.
        if usage is None:
            return {}
        details = getattr(usage, "prompt_tokens_details", None)
        cached = (getattr(details, "cached_tokens", 0) or 0) if details is not None else 0
        return {
            "input": max(0, (getattr(usage, "prompt_tokens", 0) or 0) - cached),
            "output": getattr(usage, "completion_tokens", 0) or 0,
            "cache_read": cached,
            "cache_creation": 0,
        }

    @staticmethod
    def _usage_anthropic(usage) -> dict[str, int]:
        # Anthropic reports cache tokens as fields separate from input_tokens.
        if usage is None:
            return {}
        return {
            "input": getattr(usage, "input_tokens", 0) or 0,
            "output": getattr(usage, "output_tokens", 0) or 0,
            "cache_read": getattr(usage, "cache_read_input_tokens", 0) or 0,
            "cache_creation": getattr(usage, "cache_creation_input_tokens", 0) or 0,
        }

    @staticmethod
    def _usage_google(um) -> dict[str, int]:
        if um is None:
            return {}
        cached = getattr(um, "cached_content_token_count", 0) or 0
        return {
            "input": max(0, (getattr(um, "prompt_token_count", 0) or 0) - cached),
            "output": getattr(um, "candidates_token_count", 0) or 0,
            "cache_read": cached,
            "cache_creation": 0,
        }

    @staticmethod
    def _extract_anthropic(blocks) -> tuple[str, list[dict]]:
        content = ""
        tool_calls = []
        for block in blocks:
            if block.type == "text":
                content += block.text
            elif block.type == "tool_use":
                tool_calls.append({"id": block.id, "name": block.name, "arguments": block.input})
        return content, tool_calls

    async def _generate_google(self, messages: list[dict[str, str]], tools: list[dict] | None = None, system_prompt: str | None = None, on_text: OnText = None, model: str | None = None, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
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

        gen_model = self._client.GenerativeModel(model or self.settings.llm.model, system_instruction=system_prompt)

        # Convert messages to Gemini format
        gemini_messages = []
        for msg in messages:
            role = "user" if msg["role"] == "user" else "model"
            gemini_messages.append({"role": role, "parts": [msg["content"]]})

        response = await self._with_retry(
            lambda: asyncio.to_thread(gen_model.generate_content, gemini_messages),
            on_retry=on_retry,
        )
        self.last_usage = self._usage_google(getattr(response, "usage_metadata", None))

        # Gemini has no streaming path here; emit the full text once so the
        # streaming UI still renders it.
        self._emit(on_text, response.text)
        return response.text, []
