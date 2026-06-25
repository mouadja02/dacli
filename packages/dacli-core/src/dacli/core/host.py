"""Thin extension host — the live entry (M09).

Replaces the ``DACLI`` god object on the interactive path. It constructs the
governed core (Governor, secrets, Verifier, context economy), auto-discovers
extensions (the bundled seeds + anything under ``~/.dacli/extensions``), and
builds the kernel. No orchestration — a turn is one ``kernel.orchestrate``.

The 13 platform connectors and the system/skills/sandbox built-ins still run here
through the *same* dispatcher until they move over (M11/M12). The
snowflake/github/shell seeds replace their old Connectors, so those connector ids
are excluded from the connector path and served as extensions instead. A
:class:`_HostRegistry` fronts both behind the one surface the Dispatcher and the
context pipeline read: extensions resolve first, the connector registry second.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from collections.abc import Callable, Iterable

from dacli.config.settings import Settings
from dacli.connectors.base import ToolResult
from dacli.connectors.dispatcher import Dispatcher
from dacli.connectors.registry import ConnectorRegistry, CONNECTORS_CONFIG_PATH
from dacli.connectors.system.connector import SystemConnector
from dacli.context.pipeline import build_context_pipeline
from dacli.context.sources.terminal import ScrollbackSource, ScrollbackStore
from dacli.core import runtime
from dacli.core.extensions import ExtensionDispatchRegistry, ExtensionHost
from dacli.core.governance_wiring import build_governor
from dacli.core.kernel import AgentResponse, Kernel
from dacli.core.logging_setup import get_logger, is_debug
from dacli.core.memory import AgentMemory
from dacli.ai.pricing import TokenUsage, fetch_pricing
from dacli.core.secrets import SecretStore
from dacli.core.store import DacliStore
from dacli.core.verify import Verifier
from dacli.prompts.system_prompt import load_system_prompt
from dacli.ai.llm import LLMClient
from dacli.sandbox.connector import SandboxConnector
from dacli.sandbox.factory import build_sandbox_runtime
from dacli.sandbox.terminal import TerminalSession
from dacli.skills.connector import SkillConnector
from dacli.skills.registry import SkillRegistry
from dacli.skills.spec import SkillContext

log = get_logger(__name__)

# Connector ids the seeds took over (M08). Excluded from the connector path so
# the register(api) seeds own their tool names. ``shell`` ships no manifest, but
# it's named here so the intent reads in one place.
_SEED_CONNECTORS = frozenset({"snowflake", "github", "shell"})


class _HostRegistry:
    """One resolution surface over the extension registry + the connector registry.

    The Dispatcher and the context pipeline call ``resolve`` / ``get_operation_spec``
    / ``get_tool_definitions`` / ``is_builtin``; everything else (catalog, digest,
    enable state, lifecycle) is connector-registry territory and delegates there.
    Extensions win a name clash, but there are none — the seed connectors are
    excluded from the connector path.
    """

    def __init__(self, connectors: ConnectorRegistry, ext_dispatch, ext_registry):
        self._connectors = connectors
        self._ext = ext_dispatch
        self._ext_registry = ext_registry

    def resolve(self, tool_name: str):
        return self._ext.resolve(tool_name) or self._connectors.resolve(tool_name)

    def get_operation_spec(self, tool_name: str):
        return self._ext.get_operation_spec(tool_name) or self._connectors.get_operation_spec(tool_name)

    def is_builtin(self, name: str) -> bool:
        if name in self._ext_registry.extension_ids():
            return True
        return self._connectors.is_builtin(name)

    def get_tool_definitions(self, connector_ids: Iterable[str] | None = None) -> list[dict[str, Any]]:
        # Connectors honor progressive disclosure; extensions are always-on (like
        # built-ins) so their tools are offered every turn.
        defs = list(self._connectors.get_tool_definitions(connector_ids=connector_ids))
        defs.extend(self._ext_registry.get_tool_definitions())
        return defs

    def __getattr__(self, name: str):
        # Delegate the wide connector-registry surface (get_tool_digest,
        # get_catalog, is_connector_enabled, enabled_connectors, rebuild_index, …).
        return getattr(self._connectors, name)


class DacliHost:
    """Wires the governed core + extensions + kernel for one interactive session."""

    def __init__(
        self,
        settings: Settings,
        memory: AgentMemory | None = None,
        system_prompt: str | None = None,
        on_status_update: Callable[[str], None] | None = None,
        on_tool_start: Callable[[str, dict], None] | None = None,
        on_tool_end: Callable[[str, ToolResult], None] | None = None,
        on_tool_progress: Callable[[str, str], None] | None = None,
        on_user_input_needed: Callable[[str], str] | None = None,
        on_approval: Callable[[object], bool] | None = None,
        on_stream_start: Callable[[], None] | None = None,
        on_text: Callable[[str], None] | None = None,
        on_stream_end: Callable[[str], None] | None = None,
        connectors_config_path: str = CONNECTORS_CONFIG_PATH,
        store: DacliStore | None = None,
        llm: object | None = None,
    ):
        self.settings = settings
        self.memory = memory or AgentMemory(
            state_path=settings.agent.state_path,
            history_path=settings.agent.history_path,
            memory_window=settings.agent.memory_window,
        )
        self.system_prompt = system_prompt or load_system_prompt()
        self._on_status_update = on_status_update
        self.llm = llm or LLMClient(settings)

        _base_dir = str(Path(settings.agent.state_path).parent)
        self.store = store or DacliStore(base_dir=_base_dir)
        self._pricing = None
        self._pricing_loaded = False
        self._pricing_base_dir = _base_dir

        # Per-extension secret store (M07): the config_provider the host hands to
        # every extension's ``api.config()``. Decrypts at call time.
        self.secrets = SecretStore(base_dir=_base_dir)

        # Always-on built-in connectors. Shell is no longer here — it's a seed.
        self._system_connector = SystemConnector(
            settings=settings, memory=self.memory,
            on_user_input_needed=on_user_input_needed,
        )
        self._system_connector.bind_llm(self.llm)
        self.skills = SkillRegistry()
        self._skill_connector = SkillConnector(self.skills)
        _sandbox_on = getattr(getattr(settings, "sandbox", None), "enabled", True)
        self._sandbox_connector = SandboxConnector(settings) if _sandbox_on else None

        # The governed terminal session the shell seed runs in. The seed reads it
        # off core.runtime (no instance to late-bind), so the host owns it and
        # stashes it process-wide.
        self._terminal_session = None
        self._scrollback_store = None
        self._scrollback_source = None
        _term = getattr(settings, "terminal", None)
        if _term is None or getattr(_term, "enabled", False):
            _sid = getattr(self.memory, "session_id", "default") or "default"
            _ws_root = getattr(_term, "workspace_root", ".dacli/sessions")
            self._terminal_session = TerminalSession(
                session_id=_sid,
                shell=getattr(_term, "shell", "auto"),
                workspace_root=_ws_root,
                wall_clock_seconds=getattr(_term, "wall_clock_seconds", 120),
                idle_timeout_ms=getattr(_term, "idle_timeout_ms", 400),
                journal=getattr(_term, "journal", True),
            )
            self._scrollback_store = ScrollbackStore(root=_ws_root, session_id=_sid)
            self._scrollback_source = ScrollbackSource(
                session=self._terminal_session, store=self._scrollback_store,
            )
            runtime.set_terminal(
                self._terminal_session, self._scrollback_store, _term
            )

        _builtins = [self._system_connector, self._skill_connector]
        if self._sandbox_connector is not None:
            _builtins.append(self._sandbox_connector)
        self.registry = ConnectorRegistry(
            settings,
            config_path=connectors_config_path,
            extra_connectors=_builtins,
            enforce_postconditions=True,
            exclude=_SEED_CONNECTORS,
        )
        if self._scrollback_source is not None:
            self._system_connector.bind_scrollback(self._scrollback_source)

        # Auto-discover extensions (seeds + ~/.dacli/extensions). Each gets its
        # config decrypted on demand through the secret store.
        self.ext_host = ExtensionHost(
            config_provider=self.secrets.config, settings=settings
        )
        self.ext_host.load()
        self._ext_registry = self.ext_host.registry
        self._combined = _HostRegistry(
            self.registry, ExtensionDispatchRegistry(self._ext_registry), self._ext_registry,
        )

        self.verifier = Verifier(enforce=True)
        self.governor = build_governor(
            settings,
            session_id=getattr(self.memory, "session_id", ""),
            on_approval=on_approval,
            env_resolver=self._resolve_environment,
            on_cost=self._record_warehouse_cost,
        )
        from dacli.core.test_mode import test_mode as _test_mode
        self.dispatcher = Dispatcher(
            self._combined,
            memory=self.memory,
            on_tool_start=on_tool_start,
            on_tool_end=on_tool_end,
            on_tool_progress=on_tool_progress,
            verifier=self.verifier,
            governor=self.governor,
            test_mode=_test_mode,
        )
        self._skill_connector.bind_context_provider(
            lambda: SkillContext(
                memory=self.memory, registry=self._combined, dispatcher=self.dispatcher
            )
        )

        self._sandbox_backend = None
        if self._sandbox_connector is not None:
            _sid = getattr(self.memory, "session_id", "default") or "default"
            _rt, self._sandbox_backend = build_sandbox_runtime(
                settings, self.dispatcher.execute,
                registry=self._combined, session_id=_sid,
            )
            self._sandbox_connector.bind_runtime(_rt)

        self._context = build_context_pipeline(
            settings, self.memory, self._combined, self.llm, self._system_connector
        )
        if self._sandbox_connector is not None:
            self._sandbox_connector.bind_result_store(self._context["store"])

        self.kernel = Kernel(
            llm=self.llm,
            dispatcher=self.dispatcher,
            memory=self.memory,
            tools=self._combined.get_tool_definitions(),
            system_prompt=self.system_prompt,
            max_iterations=settings.agent.max_iterations,
            on_status_update=on_status_update,
            on_stream_start=on_stream_start,
            on_text=on_text,
            on_stream_end=on_stream_end,
            context_builder=self._context["build"],
            result_spill=self._context["spill"],
            maybe_compact=self._context["maybe_compact"],
            on_usage=self._usage_sink,
            debug=is_debug(),
        )

    # ------------------------------------------------------------------
    # Pricing / usage (verbatim from the old agent)
    # ------------------------------------------------------------------
    def _get_pricing(self):
        if not self._pricing_loaded:
            self._pricing_loaded = True
            try:
                self._pricing = fetch_pricing(
                    self.settings.llm.provider, self.settings.llm.model,
                    cache_dir=self._pricing_base_dir,
                )
            except Exception:
                log.debug("pricing fetch failed", exc_info=True)
                self._pricing = None
        return self._pricing

    def _usage_sink(self, usage_dict: dict[str, int], user_message: str) -> None:
        usage = TokenUsage.from_dict(usage_dict)
        if usage.total == 0:
            return
        pricing = self._get_pricing()
        cost = pricing.cost_for(usage) if pricing else 0.0
        try:
            self.store.record_usage(
                self.memory.session_id, self.settings.llm.model, usage, cost,
                first_prompt=user_message,
            )
            self.store.save()
        except Exception:
            log.debug("usage persist failed", exc_info=True)

    def _record_warehouse_cost(self, usd: float) -> None:
        try:
            self.store.record_warehouse_cost(getattr(self.memory, "session_id", ""), usd)
        except Exception:
            log.debug("could not record warehouse cost", exc_info=True)

    def _resolve_environment(self, connector_id: str, args: dict, connector) -> str | None:
        try:
            if connector_id == "snowflake":
                db = getattr(getattr(self.settings, "snowflake", None), "database", "") or ""
                return db or None
            if connector_id == "github":
                return getattr(getattr(self.settings, "github", None), "branch", None)
        except Exception:
            return None
        return None

    def _emit_status(self, message: str) -> None:
        if self._on_status_update:
            self._on_status_update(message)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def _connect_one(self, connector, catalog: dict) -> tuple[str, bool, str | None]:
        display = catalog.get(connector.name, {}).get("name", connector.name)
        try:
            self._emit_status(f"Connecting to {display} ...")
            await connector.connect()
        except Exception as e:
            return display, False, str(e)
        return display, True, None

    async def initialize(self) -> bool:
        self._emit_status("Initializing agent...")
        successfully_initialized: list[str] = []
        failed_initializations: list[str] = []

        try:
            self._emit_status("Connecting to LLM provider ...")
            await self.llm.initialize()
            successfully_initialized.append("LLM")
        except Exception as e:
            self._emit_status(f"Failed to initialize LLM: {e!s}")
            failed_initializations.append("LLM")

        catalog = self.registry.get_catalog()
        enabled = self.registry.enabled_connectors()
        enabled_ids = {c.name for c in enabled}

        results = await asyncio.gather(
            *(self._connect_one(c, catalog) for c in enabled), return_exceptions=True
        )
        for connector, outcome in zip(enabled, results, strict=True):
            if isinstance(outcome, BaseException):  # defensive; _connect_one catches
                display = catalog.get(connector.name, {}).get("name", connector.name)
                ok, error = False, str(outcome)
            else:
                display, ok, error = outcome
            if ok:
                successfully_initialized.append(display)
            else:
                self._emit_status(f"Failed to initialize {display}: {error}")
                failed_initializations.append(display)

        try:
            self.registry.rebuild_index()
        except Exception:
            log.debug("registry index rebuild failed", exc_info=True)

        skipped = [
            meta.get("name", cid)
            for cid, meta in catalog.items() if cid not in enabled_ids
        ]

        lines = ["Agent initialized!\nActive tools: " + ", ".join(successfully_initialized)]
        ext_ids = self._ext_registry.extension_ids()
        if ext_ids:
            lines.append("Extensions: " + ", ".join(sorted(ext_ids)))
        if skipped:
            lines.append("Skipped (disabled): " + ", ".join(skipped))
        if failed_initializations:
            lines.append("Failed to initialize: " + ", ".join(failed_initializations))

        self._emit_status("\n".join(lines))
        return "LLM" in successfully_initialized

    async def shutdown(self) -> None:
        for connector in self.registry.enabled_connectors():
            await connector.disconnect()
        if self._sandbox_connector is not None:
            self._sandbox_connector.close()
        if self._terminal_session is not None:
            try:
                self._terminal_session.close()
            except Exception:
                log.debug("terminal session close failed", exc_info=True)
        runtime.clear_terminal()

    async def process_message(self, user_message: str) -> AgentResponse:
        """One turn through the kernel. No orchestration — that machinery stayed
        in the old agent (M09)."""
        return await self.kernel.orchestrate(user_message)

    def get_progress(self) -> dict:
        return self.memory.get_progress_summary()
