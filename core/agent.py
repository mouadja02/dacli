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

        # The kernel owns the loop and talks only to reasoning/dispatcher/memory.
        self.kernel = Kernel(
            llm=self.llm,
            dispatcher=self.dispatcher,
            memory=self.memory,
            tools=self.registry.get_tool_definitions(),
            system_prompt=self.system_prompt,
            max_iterations=settings.agent.max_iterations,
            on_status_update=on_status_update,
        )

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
