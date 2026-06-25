from __future__ import annotations

import random
import asyncio
import logging
import contextlib
from typing import TYPE_CHECKING, Any, TypeAlias
from collections.abc import Awaitable, Callable

from dacli.ai.providers import Provider, create_provider, unsupported_tools_error

if TYPE_CHECKING:
    # ai is the leaf wheel; it must not import core at runtime. Settings is only a
    # type annotation here — the client reads it by attribute (duck-typed).
    from dacli.config.settings import Settings

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
    # Multi-provider LLM client: a thin facade that selects a Provider in
    # initialize() and delegates request mechanics to it (A-2). The public
    # surface (generate / classify / last_usage) is provider-agnostic; the
    # shared retry/backoff lives here so no provider duplicates it.

    def __init__(self, settings: Settings):
        # Initialize LLM client with settings
        self.settings = settings
        # The concrete SDK client (AsyncOpenAI / AsyncAnthropic) owned by the
        # active provider; mirrored here so callers (and tests) can inspect or
        # inject it on the facade. Typed Any so provider-specific attribute
        # access type-checks without a fragile union.
        self._client: Any = None
        self._provider = settings.llm.provider
        # The active Provider implementation, selected in initialize().
        self._provider_impl: Provider | None = None
        # Provider-normalized token usage of the most recent generate() call,
        # read by the kernel for cost tracking. Reset on each generate().
        self.last_usage: dict[str, int] = {}

    async def initialize(self) -> None:
        # Select and initialize the provider. ``supports_tools`` is checked
        # here so a provider that cannot do tool calling (which every real
        # agent turn requires) fails fast at configuration time, not deep
        # inside the first turn (P02, Option B — honest removal).
        provider = create_provider(
            self._provider.lower(), self.settings, retry=self._with_retry
        )
        if not provider.supports_tools:
            raise unsupported_tools_error(provider.name)
        await provider.initialize()
        self._provider_impl = provider
        self._client = provider.client

    def _impl(self) -> Provider:
        # The active provider, created lazily for paths that bypass
        # initialize(). A fake SDK client injected on the facade (tests) is
        # pushed through to the provider so both always see the same client.
        impl = getattr(self, "_provider_impl", None)
        if impl is None:
            impl = create_provider(
                self._provider.lower(), self.settings, retry=self._with_retry
            )
            self._provider_impl = impl
        if impl.client is not self._client:
            impl.client = self._client
        return impl

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

        self.last_usage = {}  # populated from the provider below
        model = model or self.settings.llm.model
        impl = self._impl()
        content, tool_calls = await impl.generate(
            messages, tools, system_prompt, on_text=on_text, model=model, on_retry=on_retry
        )
        self.last_usage = impl.last_usage
        return content, tool_calls

    def _retryable_exceptions(self) -> tuple[type, ...]:
        # The active provider's declared transient-error classes (429 rate
        # limit, dropped connection, 5xx); an unknown provider retries nothing,
        # so a transient blip simply surfaces unchanged.
        try:
            return self._impl().retryable_exceptions()
        except ValueError:
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

    async def _stream_openai(self, request_kwargs: dict, on_text: OnText, on_retry: OnRetry = None) -> tuple[str, list[dict]]:
        # Back-compat seam: the OpenAI-compatible streaming path is also driven
        # directly through the facade (tests inject a fake SDK client on
        # ``_client``). Delegates to the provider; identical to generate()'s
        # streaming path.
        impl = self._impl()
        result = await impl._stream(request_kwargs, on_text, on_retry=on_retry)
        self.last_usage = impl.last_usage
        return result

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
