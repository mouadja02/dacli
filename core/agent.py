from pathlib import Path
from typing import Callable, Dict, Optional

from config.settings import Settings
from core.memory import AgentMemory
from core.kernel import Kernel, AgentResponse
from core.pricing import TokenUsage, fetch_pricing
from core.store import DacliStore
from core.verify import Verifier
from core.router import TierRouter, RoutingAuditLog
from governance import (
    Governor, ActionClassifier, PolicyEngine, PermissionRegistry, Scope,
    AuditLedger, RollbackStrategist, ShadowExecutor,
)
from governance.policy_engine import load_policy_config
from reasoning.llm import LLMClient
from connectors.base import ToolResult
from connectors.registry import ConnectorRegistry, CONNECTORS_CONFIG_PATH
from connectors.dispatcher import Dispatcher
from connectors.system.connector import SystemConnector
from context.pipeline import build_context_pipeline
from prompts.system_prompt import load_system_prompt
from skills.registry import SkillRegistry
from skills.spec import SkillContext
from skills.connector import SkillConnector
from sandbox.connector import SandboxConnector
from sandbox.runtime import SandboxRuntime
from sandbox.policy import SandboxPolicy


class DACLI:
    """Thin wiring object: holds the session and wires kernel + components.

    All orchestration lives in the kernel; all platform behavior lives behind
    the registry / dispatcher. No platform names appear here.
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
        on_approval: Optional[Callable[[object], bool]] = None,
        on_stream_start: Optional[Callable[[], None]] = None,
        on_text: Optional[Callable[[str], None]] = None,
        on_stream_end: Optional[Callable[[str], None]] = None,
        connectors_config_path: str = CONNECTORS_CONFIG_PATH,
        store: Optional[DacliStore] = None,
    ):
        self.settings = settings
        self.memory = memory or AgentMemory(
            state_path=settings.agent.state_path,
            history_path=settings.agent.history_path,
            memory_window=settings.agent.memory_window,
        )

        self.system_prompt = system_prompt or load_system_prompt()
        self._on_status_update = on_status_update
        self.llm = LLMClient(settings)  # reasoning client

        # Usage/cost tracking: persistent .dacli/dacli.json + models.dev pricing
        # for the configured provider+model (None when offline -> tokens still
        # tracked, cost reported as unknown).
        _base_dir = str(Path(settings.agent.state_path).parent)
        self.store = store or DacliStore(base_dir=_base_dir)
        self._pricing = fetch_pricing(settings.llm.provider, settings.llm.model, cache_dir=_base_dir)

        # Built-in 'system' connector (always on) is injected into the registry
        # so request_user_input / update_plan flow through one dispatch path.
        self._system_connector = SystemConnector(
            settings=settings,
            memory=self.memory,
            on_user_input_needed=on_user_input_needed,
        )

        # Skills (Phase 4): a contracted-procedure registry, surfaced as a
        # built-in connector so every skill flows through the one dispatch path
        # and the one post-condition gate. ``context_provider`` is late-bound
        # because the SkillContext needs the dispatcher we build just below.
        self.skills = SkillRegistry()
        self._skill_connector = SkillConnector(self.skills)

        # Sandbox (Phase 5.6): the code-execution tier, surfaced as a built-in
        # connector so its single ``run_sandbox_code`` op flows through the one
        # dispatch path. The runtime is late-bound after the dispatcher exists
        # (its SDK needs the governed ``dispatcher.execute``).
        _sandbox_on = getattr(getattr(settings, "sandbox", None), "enabled", True)
        self._sandbox_connector = SandboxConnector(settings) if _sandbox_on else None

        # Connector plugin registry (manifest-discovered) + generic dispatcher.
        # ``enforce_postconditions`` makes "no post-condition, no registration"
        # structural: a capability that cannot be verified cannot be offered.
        _builtins = [self._system_connector, self._skill_connector]
        if self._sandbox_connector is not None:
            _builtins.append(self._sandbox_connector)
        self.registry = ConnectorRegistry(
            settings,
            config_path=connectors_config_path,
            extra_connectors=_builtins,
            enforce_postconditions=True,
        )

        # Post-condition verifier (Phase 4): a verified success is the only kind
        # of success. Wired into the dispatcher so a failed check downgrades the
        # result before it is ever treated as done.
        self.verifier = Verifier(enforce=True)

        # Governance (Phase 5, 𝒢): classify blast radius → policy → permissions
        # → rollback → human approval, all before an action runs; record every
        # decision in an append-only audit ledger. Built from config/policy.yaml
        # so a team tunes velocity vs. caution without code changes.
        self.governor = self._build_governor(settings, on_approval)
        self.dispatcher = Dispatcher(
            self.registry,
            memory=self.memory,
            on_tool_start=on_tool_start,
            on_tool_end=on_tool_end,
            verifier=self.verifier,
            governor=self.governor,
        )
        # Now the dispatcher exists, give skills their runtime collaborators.
        self._skill_connector.bind_context_provider(
            lambda: SkillContext(
                memory=self.memory, registry=self.registry, dispatcher=self.dispatcher
            )
        )

        # ... and give the sandbox its runtime, whose SDK calls back through the
        # *governed* dispatcher.execute — so code-execution is not a bypass.
        if self._sandbox_connector is not None:
            self._sandbox_connector.bind_runtime(
                SandboxRuntime(
                    SandboxPolicy.from_settings(settings),
                    self.dispatcher.execute,
                    registry=self.registry,
                )
            )

        # Tier router (Phase 4): classifies each task tool-vs-sandbox with
        # confidence-aware escalation; decisions are logged for audit/calibration.
        _state_dir = str(Path(settings.agent.state_path).parent)
        self.router = TierRouter(
            llm=self.llm,
            registry=self.registry,
            memory=self.memory,
            audit_log=RoutingAuditLog(path=f"{_state_dir}/routing.jsonl"),
        )

        # Context Constructor (Phase 3) wiring — see context.pipeline.
        self._context = build_context_pipeline(
            settings, self.memory, self.registry, self.llm, self._system_connector
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
            on_stream_start=on_stream_start,
            on_text=on_text,
            on_stream_end=on_stream_end,
            context_builder=self._context["build"],
            result_spill=self._context["spill"],
            maybe_compact=self._context["maybe_compact"],
            on_usage=self._usage_sink,
        )

    def _usage_sink(self, usage_dict: Dict[str, int], user_message: str) -> None:
        # Price one LLM call's token usage and fold it into the persistent store.
        # Best-effort: usage tracking must never break the control loop.
        usage = TokenUsage.from_dict(usage_dict)
        if usage.total == 0:
            return
        cost = self._pricing.cost_for(usage) if self._pricing else 0.0
        try:
            self.store.record_usage(
                self.memory.session_id,
                self.settings.llm.model,
                usage,
                cost,
                first_prompt=user_message,
            )
            self.store.save()
        except Exception:
            pass

    def _build_governor(self, settings: Settings, on_approval) -> Optional[Governor]:
        gov = getattr(settings, "governance", None)
        if gov is not None and not gov.enabled:
            return None  # explicitly disabled (trusted offline run)

        policy_path = getattr(gov, "policy_path", "config/policy.yaml") if gov else "config/policy.yaml"
        config = load_policy_config(policy_path)
        policy = PolicyEngine(config)

        # Least-privilege: connectors get the configured default scope unless the
        # policy profile grants more. Write/admin is opt-in per connection.
        try:
            default_scope = Scope(getattr(gov, "default_scope", "read_only"))
        except Exception:
            default_scope = Scope.READ_ONLY
        permissions = PermissionRegistry.from_policy_config(config, default_scope=default_scope)
        # Built-in harness connectors (system/skills/sandbox) are not external
        # platforms — least-privilege scoping targets platform blast radius, and
        # their sub-actions (e.g. each governed sdk.run inside the sandbox) are
        # gated independently. Exempt them so the harness itself isn't crippled.
        for _builtin in ("system", "skills", "sandbox"):
            permissions.grant(_builtin, Scope.ADMIN)

        state_dir = str(Path(settings.agent.state_path).parent)
        audit_path = (getattr(gov, "audit_path", None) or f"{state_dir}/audit.jsonl") if gov else f"{state_dir}/audit.jsonl"
        ledger = AuditLedger(path=audit_path)

        return Governor(
            classifier=ActionClassifier(prod_markers=policy.prod_markers or None),
            policy=policy,
            permissions=permissions,
            strategist=RollbackStrategist(),
            shadow_executor=ShadowExecutor(),
            ledger=ledger,
            session_id=getattr(self.memory, "session_id", ""),
            approval_fn=on_approval,
            env_resolver=self._resolve_environment,
            enforce=True,
            use_shadow=bool(getattr(gov, "shadow_execution", True)) if gov else True,
        )

    def _resolve_environment(self, connector_id: str, args: Dict, connector) -> Optional[str]:
        # Best-effort environment label for the policy engine: the connection's
        # own target (e.g. the Snowflake database / GitHub branch). A prod-looking
        # target maps to 'prod', otherwise the literal target name is returned so
        # a policy.yaml override for that environment can match.
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

    async def initialize(self) -> bool:
        # Initialize only enabled connectors and connections
        self._emit_status("Initializing agent...")

        successfully_initialized = []
        failed_initializations = []

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

        skipped = [
            meta.get("name", cid)
            for cid, meta in catalog.items() if cid not in enabled_ids
        ]

        lines = ["Agent initialized!\nActive tools: " + ", ".join(successfully_initialized)]
        if skipped:
            lines.append("Skipped (disabled): " + ", ".join(skipped))
        if failed_initializations:
            lines.append("Failed to initialize: " + ", ".join(failed_initializations))

        self._emit_status("\n".join(lines))
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
