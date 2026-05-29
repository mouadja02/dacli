from typing import Callable, Dict, Optional

from config.settings import Settings
from core.memory import AgentMemory
from core.kernel import Kernel, AgentResponse
from reasoning.llm import LLMClient
from connectors.base import ToolResult
from connectors.registry import ConnectorRegistry, CONNECTORS_CONFIG_PATH
from connectors.dispatcher import Dispatcher
from connectors.system.connector import SystemConnector
from prompts.system_prompt import load_system_prompt


class DACLI:
    """Thin wiring object.

    Holds the session and wires the kernel + components. All the orchestration
    lives in :class:`core.kernel.Kernel`; all platform behavior lives behind the
    :class:`connectors.registry.ConnectorRegistry` / :class:`Dispatcher`. No
    platform names appear here.
    """

    def __init__(
        self,
        settings: Settings,
        memory: Optional[AgentMemory] = None,
        system_prompt: Optional[str] = None,
        on_status_update: Optional[Callable[[str], None]] = None,
        on_tool_start: Optional[Callable[[str, Dict], None]] = None,
        on_tool_end: Optional[Callable[[str, ToolResult], None]] = None,
        on_user_input_needed: Optional[Callable[[str], str]] = None,
        on_stream_start: Optional[Callable[[], None]] = None,
        on_text: Optional[Callable[[str], None]] = None,
        on_stream_end: Optional[Callable[[str], None]] = None,
        connectors_config_path: str = CONNECTORS_CONFIG_PATH,
    ):
        self.settings = settings

        self.memory = memory or AgentMemory(
            state_path=settings.agent.state_path,
            history_path=settings.agent.history_path,
            memory_window=settings.agent.memory_window,
        )

        self.system_prompt = system_prompt or load_system_prompt()
        self._on_status_update = on_status_update

        # Reasoning client
        self.llm = LLMClient(settings)

        # Built-in 'system' connector (always on) is injected into the registry
        # so request_user_input / update_progress flow through one dispatch path.
        self._system_connector = SystemConnector(
            settings=settings,
            memory=self.memory,
            on_user_input_needed=on_user_input_needed,
        )

        # Connector plugin registry (manifest-discovered) + generic dispatcher.
        self.registry = ConnectorRegistry(
            settings,
            config_path=connectors_config_path,
            extra_connectors=[self._system_connector],
        )
        self.dispatcher = Dispatcher(
            self.registry,
            memory=self.memory,
            on_tool_start=on_tool_start,
            on_tool_end=on_tool_end,
        )

        # Context Constructor (Phase 3) wiring. Builds the per-turn collaborators
        # the kernel uses instead of a fixed window: a token counter + budget, a
        # selection-policy assembler (with progressive disclosure + dynamic
        # prompt), off-context result spill, and budget-pressure compaction.
        self._context = self._build_context_pipeline()

        # The kernel owns the loop and talks only to reasoning/dispatcher/memory.
        self.kernel = Kernel(
            llm=self.llm,
            dispatcher=self.dispatcher,
            memory=self.memory,
            tools=self.registry.get_tool_definitions(),
            system_prompt=self.system_prompt,
            max_iterations=settings.agent.max_iterations,
            on_status_update=on_status_update,
            on_stream_start=on_stream_start,
            on_text=on_text,
            on_stream_end=on_stream_end,
            context_builder=self._context["build"],
            result_spill=self._context["spill"],
            maybe_compact=self._context["maybe_compact"],
        )

    def _build_context_pipeline(self) -> Dict:
        """Construct the Phase 3 context collaborators and return them as hooks.

        Returns ``{"build", "spill", "maybe_compact", "explain"}``. ``explain`` is
        used by ``dacli context --explain`` to inspect an assembled context.
        """
        from context.assembler import build_context
        from context.budget import Budget
        from context.compaction import compact, needs_compaction
        from context.disclosure import disclose
        from context.spill import ResultStore, summarize_or_inline
        from context.tokenizer import make_counter
        from prompts.system_prompt import compose_system_prompt

        settings = self.settings
        counter = make_counter(settings)
        budget = Budget.from_settings(settings)
        store = ResultStore(session_id=self.memory.session_id)

        # Late-bind collaborators the system connector needs (3.3 / 3.4).
        self._system_connector.bind_registry(self.registry)
        self._system_connector.bind_result_store(store)

        def _build(task, working, disclosed):
            effective = disclose(task, self.registry, already_disclosed=disclosed)
            base = compose_system_prompt(task, effective)
            return build_context(
                task,
                memory=self.memory,
                registry=self.registry,
                recent_messages=working,
                counter=counter,
                budget=budget,
                disclosed=effective,
                base_system_prompt=base,
            )

        def _spill(result) -> str:
            return summarize_or_inline(
                result, counter, settings.context.spill_threshold_tokens, store
            )

        async def _maybe_compact(working):
            if needs_compaction(
                working, counter, budget.total,
                pressure=settings.context.compaction_pressure,
            ):
                result = await compact(
                    working,
                    self.llm,
                    store_fn=lambda note: self._remember_compaction(note),
                )
                return result.messages
            return working

        return {
            "build": _build,
            "spill": _spill,
            "maybe_compact": _maybe_compact,
            "counter": counter,
            "budget": budget,
        }

    def _remember_compaction(self, note: str) -> None:
        # Persist a compaction summary to durable memory with provenance so a
        # folded fact is never lost (raw history also stays on disk).
        remember = getattr(self.memory, "remember_fact", None)
        if remember is not None:
            remember(note, source="compaction", tags=["compaction"])

    def _emit_status(self, message: str) -> None:
        if self._on_status_update:
            self._on_status_update(message)

    async def initialize(self) -> bool:
        # Initialize only enabled connectors and connections
        self._emit_status("Initializing agent...")

        successfully_initialized = []
        failed_initializations = []
        skipped = []

        try:
            # Initialize LLM (always required)
            self._emit_status("Connecting to LLM provider ...")
            await self.llm.initialize()
            successfully_initialized.append("LLM")
        except Exception as e:
            self._emit_status(f"Failed to initialize LLM: {str(e)}")
            failed_initializations.append("LLM")

        catalog = self.registry.get_catalog()
        enabled = self.registry.enabled_connectors()
        enabled_ids = {c.name for c in enabled}

        for connector in enabled:
            display = catalog.get(connector.name, {}).get("name", connector.name)
            try:
                self._emit_status(f"Connecting to {display} ...")
                await connector.connect()
                successfully_initialized.append(display)
            except Exception as e:
                self._emit_status(f"Failed to initialize {display}: {str(e)}")
                failed_initializations.append(display)

        for connector_id, meta in catalog.items():
            if connector_id not in enabled_ids:
                skipped.append(meta.get("name", connector_id))

        output_message = "Agent initialized!\nActive tools: " + ", ".join(successfully_initialized)

        if skipped:
            output_message += "\nSkipped (disabled): " + ", ".join(skipped)

        if failed_initializations:
            output_message += "\nFailed to initialize: " + ", ".join(failed_initializations)

        self._emit_status(output_message)
        return True

    async def shutdown(self) -> None:
        # Clean up resources for enabled connectors only
        for connector in self.registry.enabled_connectors():
            await connector.disconnect()

    async def process_message(self, user_message: str) -> AgentResponse:
        # Delegate the control loop to the kernel.
        return await self.kernel.orchestrate(user_message)

    def get_progress(self) -> Dict:
        # Get current progress summary
        return self.memory.get_progress_summary()
