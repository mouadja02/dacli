from pathlib import Path
from typing import Callable, Dict, Optional

from config.settings import Settings
from core.memory import AgentMemory
from core.kernel import Kernel, AgentResponse
from core.pricing import TokenUsage, fetch_pricing
from core.store import DacliStore
from core.verify import Verifier
from core.router import TierRouter, RoutingAuditLog
from core.planner import Planner
from core.loop import PlanActObserveVerify, StepResult, StepContext, CorrectionAuditLog
from core.blackboard import Blackboard
from core.subagent import Lead, Assignment, WorkerOutput
from reasoning.model_router import ModelRouter, ModelRoutingAuditLog, Stakes
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

        # Skills: a contracted-procedure registry, surfaced as a
        # built-in connector so every skill flows through the one dispatch path
        # and the one post-condition gate. ``context_provider`` is late-bound
        # because the SkillContext needs the dispatcher we build just below.
        self.skills = SkillRegistry()
        self._skill_connector = SkillConnector(self.skills)

        # Sandbox: the code-execution tier, surfaced as a built-in
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

        # Post-condition verifier: a verified success is the only kind
        # of success. Wired into the dispatcher so a failed check downgrades the
        # result before it is ever treated as done.
        self.verifier = Verifier(enforce=True)

        # Governance (𝒢): classify blast radius → policy → permissions
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

        # Tier router: classifies each task tool-vs-sandbox with
        # confidence-aware escalation; decisions are logged for audit/calibration.
        _state_dir = str(Path(settings.agent.state_path).parent)
        self.router = TierRouter(
            llm=self.llm,
            registry=self.registry,
            memory=self.memory,
            audit_log=RoutingAuditLog(path=f"{_state_dir}/routing.jsonl"),
        )

        # Context Constructor wiring — see context.pipeline.
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

        # Orchestration & multi-agent (𝒪 / ℛ) — additive. The kernel
        # stays the default single-step path (``process_message``); the
        # planner→act→observe→verify controller is the opt-in path for complex,
        # multi-step goals (``process_goal``). All components are offline-safe.
        self._on_approval_cb = on_approval
        self._build_orchestration(settings, _state_dir)

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

    # ==================================================================
    # Orchestration & multi-agent (𝒪 / ℛ)
    # ==================================================================
    def _build_orchestration(self, settings: Settings, state_dir: str) -> None:
        orch = getattr(settings, "orchestration", None)
        self._orchestration_on = bool(getattr(orch, "enabled", True)) if orch else True

        # ℛ model tiering: cheap = explicit cheap_model, else the fallback_model,
        # else the default; strong = explicit strong_model, else the default. So a
        # single-model config routes everything to the same model (no behavior
        # change), while a tiered config spends strong tokens only where they pay.
        llm_cfg = settings.llm
        cheap = llm_cfg.cheap_model or llm_cfg.fallback_model or llm_cfg.model
        strong = llm_cfg.strong_model or llm_cfg.model
        self.model_router = ModelRouter(
            llm=self.llm,
            cheap_model=cheap,
            strong_model=strong,
            default_model=llm_cfg.model,
            audit_log=ModelRoutingAuditLog(path=f"{state_dir}/model_routing.jsonl"),
        )

        # The DAG planner (with the complexity gate) and the shared blackboard.
        gate = getattr(orch, "complexity_gate", 2) if orch else 2
        self.planner = Planner(llm=self.llm, complexity_gate=gate)
        self.blackboard = Blackboard(path=f"{state_dir}/blackboard.json")

        # The lead for breadth-first sub-agent fan-out.
        self.lead = Lead(
            self.blackboard,
            max_subagents=getattr(orch, "max_subagents", 6) if orch else 6,
            summary_tokens=getattr(orch, "subagent_summary_tokens", 2000) if orch else 2000,
            on_event=self._emit_status,
        )

        # The plan→act→observe→verify controller (verify-in-loop + bounded,
        # informed self-correction). Its executor bridges each node to the kernel.
        self.orchestrator = PlanActObserveVerify(
            executor=self._make_node_executor(),
            verifier=self._verify_node,
            model_router=self.model_router,
            on_approval=self._on_approval_cb,
            correction_log=CorrectionAuditLog(path=f"{state_dir}/corrections.jsonl"),
            correction_budget=getattr(orch, "correction_budget", 2) if orch else 2,
            require_approval=getattr(orch, "require_plan_approval", True) if orch else True,
            on_event=self._emit_status,
        )

    def _make_node_executor(self):
        """An executor that runs one DAG node through the existing kernel.

        A breadth-first node fans out to isolated-context sub-agents (6.5);
        every other node is a focused kernel run. On a correction attempt the
        model tier the router escalated to is threaded into the kernel, and the
        environmental feedback from the previous failure is prepended to the
        prompt — informed retry, not a blind one.
        """
        async def executor(node, ctx: StepContext) -> StepResult:
            if node.breadth_first and getattr(
                getattr(self.settings, "orchestration", None), "subagents_enabled", True
            ):
                return await self._run_breadth_first(node)

            prompt = node.description
            if ctx.feedback:
                prompt = (
                    f"{node.description}\n\n"
                    f"The previous attempt failed verification. Use this feedback "
                    f"to correct it (do not repeat the same step blindly):\n{ctx.feedback}"
                )

            # Resolve the model tier for this attempt (ℛ). On a correction attempt
            # the loop already escalated ``ctx.model`` to strong; on a first
            # attempt we pick by stakes — an irreversible step is a strong-model
            # job, ordinary steps run cheap. Every choice is logged for audit.
            model = ctx.model
            if model is None:
                choice = self.model_router.choose(
                    "irreversible_plan" if node.irreversible else "routing",
                    stakes=Stakes.HIGH if node.irreversible else Stakes.MEDIUM,
                    irreversible=node.irreversible,
                )
                model = choice.model
            response = await self.kernel.orchestrate(prompt, model=model)
            success = not response.error and not response.needs_user_input
            return StepResult(
                success=success,
                output=response.content,
                error=response.error,
                feedback=response.error or (None if success else response.content),
            )
        return executor

    async def _verify_node(self, node, result: StepResult):
        """Node-level verify. The heavy, environment-anchored checks already ran
        inside the dispatcher (post-conditions + governance) for
        every tool the node used; here we confirm the node didn't error or stall.
        """
        if not result.success:
            return False, result.error or "node did not complete"
        return True, "node completed; tool post-conditions verified in-dispatch"

    async def _run_breadth_first(self, node) -> StepResult:
        """Fan a breadth-first node out to parallel, isolated-context sub-agents."""
        items = node.items or [node.description]
        report = await self.lead.fan_out(node.description, items, self._make_subagent_worker())
        ok = not report.failures
        return StepResult(
            success=ok,
            output=report.merged_summary,
            error=("; ".join(report.failures) if report.failures else None),
            feedback=("; ".join(report.failures) if report.failures else None),
        )

    def _make_subagent_worker(self):
        """A worker that runs one sub-agent in an isolated context window.

        Isolation = a *fresh* :class:`AgentMemory`/:class:`Kernel` per sub-agent
        (its own working list), but the *shared* dispatcher/registry/governor — so
        sub-agents get clean context and parallel windows while every action they
        take is still governed and verified identically. Sub-agent work runs on
        the cheap tier where adequate (token-cost mitigation).
        """
        async def worker(assignment: Assignment) -> WorkerOutput:
            sub_memory = AgentMemory(
                state_path=self.settings.agent.state_path,
                history_path=self.settings.agent.history_path,
                memory_window=self.settings.agent.memory_window,
            )
            sub_kernel = Kernel(
                llm=self.llm,
                dispatcher=self.dispatcher,
                memory=sub_memory,
                tools=self.registry.get_tool_definitions(),
                system_prompt=self.system_prompt,
                max_iterations=self.settings.agent.max_iterations,
                context_builder=self._context["build"],
                result_spill=self._context["spill"],
                maybe_compact=self._context["maybe_compact"],
            )
            focus = f"{assignment.task} — focus only on: {assignment.item}. Return a concise result."
            cheap = self.model_router.choose("summarization", stakes=Stakes.LOW).model
            response = await sub_kernel.orchestrate(focus, model=cheap)
            return WorkerOutput(
                text=response.content or "",
                success=not response.error and not response.needs_user_input,
                error=response.error,
            )
        return worker

    async def process_message(self, user_message: str) -> AgentResponse:
        # Delegate the control loop to the kernel (the default single-step path).
        return await self.kernel.orchestrate(user_message)

    async def process_goal(self, goal: str):
        """Orchestrated entry point for complex, multi-step goals.

        The **complexity gate** decides: a goal that does not decompose into
        enough subtasks runs single-step through the kernel (no planner ceremony).
        A genuinely multi-step goal is decomposed into an inspectable DAG,
        presented for approval (plan-approve-execute), then driven by the
        plan→act→observe→verify controller with bounded self-correction. Returns
        the kernel's :class:`AgentResponse` for the simple path, or the
        :class:`~core.loop.OrchestrationResult` for the orchestrated path.
        """
        if not self._orchestration_on or not self.planner.is_complex(goal):
            return await self.process_message(goal)

        dag = self.planner.decompose(goal)
        # Present the plan for approval (the low-friction governance posture).
        if self.orchestrator.require_approval and self._on_approval_cb is not None:
            self._emit_status("Proposed plan:\n" + dag.render())
            try:
                approved = bool(self._on_approval_cb(dag))
            except Exception:
                approved = False
            if not approved:
                self._emit_status("Plan not approved — not executing.")
                return dag
        self.blackboard.record_decision(f"approved plan for goal: {goal}", agent="lead")
        return await self.orchestrator.run_dag(dag)

    def get_progress(self) -> Dict:
        # Get current progress summary
        return self.memory.get_progress_summary()
