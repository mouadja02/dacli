import asyncio
from pathlib import Path
from collections.abc import Callable

from dacli.config.settings import Settings
from dacli.core.logging_setup import get_logger, is_debug
from dacli.core.memory import AgentMemory
from dacli.core.kernel import Kernel, AgentResponse
from dacli.core.pricing import TokenUsage, fetch_pricing
from dacli.core.store import DacliStore
from dacli.core.verify import Verifier
from dacli.core.router import TierRouter, RoutingAuditLog
from dacli.core.planner import Planner
from dacli.core.loop import PlanActObserveVerify, StepResult, StepContext, CorrectionAuditLog
from dacli.core.blackboard import Blackboard
from dacli.core.subagent import Lead, Assignment, WorkerOutput
from dacli.reasoning.model_router import ModelRouter, ModelRoutingAuditLog, Stakes
from dacli.governance import (
    Governor, ActionClassifier, PolicyEngine, PermissionRegistry, Scope,
    AuditLedger, RollbackStrategist, ShadowExecutor,
)
from dacli.governance.policy_engine import load_policy_config
from dacli.reasoning.llm import LLMClient
from dacli.connectors.base import ToolResult
from dacli.connectors.registry import ConnectorRegistry, CONNECTORS_CONFIG_PATH
from dacli.connectors.dispatcher import Dispatcher
from dacli.connectors.system.connector import SystemConnector
from dacli.context.pipeline import build_context_pipeline
from dacli.prompts.system_prompt import load_system_prompt
from dacli.skills.registry import SkillRegistry
from dacli.skills.spec import SkillContext
from dacli.skills.connector import SkillConnector
from dacli.sandbox.connector import SandboxConnector
from dacli.sandbox.factory import build_sandbox_runtime
from dacli.sandbox.terminal import TerminalSession
from dacli.connectors.shell.connector import ShellConnector
from dacli.context.sources.terminal import ScrollbackStore, ScrollbackSource

log = get_logger(__name__)


class DACLI:
    """Thin wiring object: holds the session and wires kernel + components.

    All orchestration lives in the kernel; all platform behavior lives behind
    the registry / dispatcher. No platform names appear here.
    """

    def __init__(
        self,
        settings: Settings,
        memory: AgentMemory | None = None,
        system_prompt: str | None = None,
        on_status_update: Callable[[str], None] | None = None,
        on_tool_start: Callable[[str, dict], None] | None = None,
        on_tool_end: Callable[[str, ToolResult], None] | None = None,
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
        # Reasoning client. Injectable so a headless/test harness can supply a
        # deterministic ScriptedLLM; defaults to the real provider client.
        self.llm = llm or LLMClient(settings)

        # Usage/cost tracking: persistent .dacli/dacli.json + models.dev pricing
        # for the configured provider+model (None when offline -> tokens still
        # tracked, cost reported as unknown). Pricing is resolved lazily on the
        # first read — a cold cache means a blocking network hit, and startup
        # must never wait on that.
        _base_dir = str(Path(settings.agent.state_path).parent)
        self.store = store or DacliStore(base_dir=_base_dir)
        self._pricing = None
        self._pricing_loaded = False
        self._pricing_base_dir = _base_dir

        # Built-in 'system' connector (always on) is injected into the registry
        # so request_user_input / update_plan flow through one dispatch path.
        self._system_connector = SystemConnector(
            settings=settings,
            memory=self.memory,
            on_user_input_needed=on_user_input_needed,
        )
        # Give the system connector the reasoning LLM so ``generate_connector``
        # can write a new connector from an in-chat description.
        self._system_connector.bind_llm(self.llm)

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

        # Terminal: the governed shell tier (Era 2). A persistent, jailed,
        # journaled shell session surfaced as a built-in connector so its single
        # ``run_shell_command`` op flows through the *same* classify → policy →
        # rollback → audit spine as every tool — the free-text terminal is not a
        # governance bypass. Disabled → the tier (and its tool) simply does not
        # exist this run (deny by absence). Construction only makes the workspace
        # dirs; the shell subprocess is spawned lazily on the first command.
        self._terminal_session = None
        self._scrollback_store = None
        self._scrollback_source = None
        self._shell_connector = None
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
            self._shell_connector = ShellConnector(
                settings, session=self._terminal_session,
                scrollback_store=self._scrollback_store,
            )

        # Connector plugin registry (manifest-discovered) + generic dispatcher.
        # ``enforce_postconditions`` makes "no post-condition, no registration"
        # structural: a capability that cannot be verified cannot be offered.
        _builtins = [self._system_connector, self._skill_connector]
        if self._sandbox_connector is not None:
            _builtins.append(self._sandbox_connector)
        if self._shell_connector is not None:
            _builtins.append(self._shell_connector)
        self.registry = ConnectorRegistry(
            settings,
            config_path=connectors_config_path,
            extra_connectors=_builtins,
            enforce_postconditions=True,
        )
        # Give the system connector the scrollback source so ``fetch_scrollback``
        # can answer "what did step N output?" by command_id (JIT, off-context).
        if self._scrollback_source is not None:
            self._system_connector.bind_scrollback(self._scrollback_source)

        # Post-condition verifier: a verified success is the only kind
        # of success. Wired into the dispatcher so a failed check downgrades the
        # result before it is ever treated as done.
        self.verifier = Verifier(enforce=True)

        # Governance (𝒢): classify blast radius → policy → permissions
        # → rollback → human approval, all before an action runs; record every
        # decision in an append-only audit ledger. Built from config/policy.yaml
        # so a team tunes velocity vs. caution without code changes.
        self.governor = self._build_governor(settings, on_approval)
        from dacli.core.test_mode import test_mode as _test_mode
        self.dispatcher = Dispatcher(
            self.registry,
            memory=self.memory,
            on_tool_start=on_tool_start,
            on_tool_end=on_tool_end,
            verifier=self.verifier,
            governor=self.governor,
            test_mode=_test_mode,
        )
        # Now the dispatcher exists, give skills their runtime collaborators.
        self._skill_connector.bind_context_provider(
            lambda: SkillContext(
                memory=self.memory, registry=self.registry, dispatcher=self.dispatcher
            )
        )

        # ... and give the sandbox its runtime, whose SDK calls back through the
        # *governed* dispatcher.execute — so code-execution is not a bypass. The
        # factory picks docker (a hardened per-session container) when an engine
        # is reachable, else the local subprocess runtime; both expose the same
        # surface and route through the same governance spine.
        self._sandbox_backend = None
        if self._sandbox_connector is not None:
            _sid = getattr(self.memory, "session_id", "default") or "default"
            _runtime, self._sandbox_backend = build_sandbox_runtime(
                settings, self.dispatcher.execute,
                registry=self.registry, session_id=_sid,
            )
            self._sandbox_connector.bind_runtime(_runtime)

        # Context Constructor wiring — see context.pipeline.
        self._context = build_context_pipeline(
            settings, self.memory, self.registry, self.llm, self._system_connector
        )
        # Share the session's spilled-result store with the sandbox so model code
        # can `sdk.fetch_result(handle)` a large result back to process it in code
        # (off model context) — the same `res_*` handles the tool tier spills to.
        if self._sandbox_connector is not None:
            self._sandbox_connector.bind_result_store(self._context["store"])

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
            # In --debug mode the kernel re-raises truly unexpected exceptions
            # instead of masking them as resp.error (P06).
            debug=is_debug(),
        )

        # Orchestration & multi-agent (𝒪 / ℛ) — opt-in, gated behind
        # ``settings.orchestration.enabled`` (off by default). The kernel is the
        # default single-step path (``process_message``); the
        # planner→act→observe→verify controller is the path for genuinely
        # multi-step goals (``process_goal``), reached via a conservative
        # complexity gate. When disabled, NONE of the subsystems below are built
        # — a plain startup constructs no planner/blackboard/lead/orchestrator/
        # model-router/tier-router and writes no blackboard.json (P08).
        self._on_approval_cb = on_approval
        orch = getattr(settings, "orchestration", None)
        self._orchestration_on = bool(getattr(orch, "enabled", False)) if orch else False
        # Declared up front so callers can rely on the attrs existing (== None
        # when orchestration is off) rather than catching AttributeError.
        self.router = None
        self.model_router = None
        self.planner = None
        self.blackboard = None
        self.lead = None
        self.orchestrator = None
        if self._orchestration_on:
            _state_dir = str(Path(settings.agent.state_path).parent)
            self._build_orchestration(settings, _state_dir)

    def _get_pricing(self):
        # Lazy, memoized pricing lookup. The first call may hit the network on a
        # cold cache; failures degrade to None (cost unknown) and are not retried.
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
        # Price one LLM call's token usage and fold it into the persistent store.
        # Best-effort: usage tracking must never break the control loop.
        usage = TokenUsage.from_dict(usage_dict)
        if usage.total == 0:
            return
        pricing = self._get_pricing()
        cost = pricing.cost_for(usage) if pricing else 0.0
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
            log.debug("usage persist failed", exc_info=True)  # swallow but record

    def _build_governor(self, settings: Settings, on_approval) -> Governor | None:
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
        # The shell tier, by contrast, IS scoped by least privilege — that is the
        # whole point of a governed terminal. Its ceiling is the configured
        # ``terminal.scope`` (default 'write'), so an `rm file` (risky) or
        # `rm -rf` (irreversible) is permission-denied unless the operator widened
        # the shell scope. The *command's* tier (from the command classifier), not
        # the op's declared risk, is what the check sees.
        _term = getattr(settings, "terminal", None)
        try:
            _shell_scope = Scope(str(getattr(_term, "scope", "write")).strip().lower())
        except Exception:
            _shell_scope = Scope.WRITE
        permissions.grant("shell", _shell_scope)

        state_dir = str(Path(settings.agent.state_path).parent)
        audit_path = (getattr(gov, "audit_path", None) or f"{state_dir}/audit.jsonl") if gov else f"{state_dir}/audit.jsonl"
        ledger = AuditLedger(path=audit_path)

        # The classifier embeds the shell command classifier; give it the
        # terminal's egress posture so a `curl`/`wget` in a shell command is
        # judged against the same allowlist a connector fetch would be.
        return Governor(
            classifier=ActionClassifier(
                prod_markers=policy.prod_markers or None,
                network=getattr(_term, "network", "allowlist"),
                egress_allowlist=list(getattr(_term, "egress_allowlist", []) or []),
            ),
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

    def _resolve_environment(self, connector_id: str, args: dict, connector) -> str | None:
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

    async def _connect_one(self, connector, catalog: dict) -> tuple[str, bool, str | None]:
        # One connector's connect, with its display name and outcome attributed.
        display = catalog.get(connector.name, {}).get("name", connector.name)
        try:
            self._emit_status(f"Connecting to {display} ...")
            await connector.connect()
        except Exception as e:
            return display, False, str(e)
        return display, True, None

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
            self._emit_status(f"Failed to initialize LLM: {e!s}")
            failed_initializations.append("LLM")

        catalog = self.registry.get_catalog()
        enabled = self.registry.enabled_connectors()
        enabled_ids = {c.name for c in enabled}

        # Connect concurrently — startup latency is the slowest health check,
        # not the sum. Status strings are unchanged, but "Connecting to X ..."
        # lines may interleave across connectors (ordering is best-effort).
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
        # The LLM is the only hard requirement; connector failures stay
        # non-fatal (they surface via failed_connectors() and the notice above).
        return "LLM" in successfully_initialized

    async def shutdown(self) -> None:
        # Clean up resources for enabled connectors only
        for connector in self.registry.enabled_connectors():
            await connector.disconnect()
        # Tear down the per-session sandbox container (docker runtime) and close
        # the persistent shell session, if any.
        if self._sandbox_connector is not None:
            self._sandbox_connector.close()
        if self._terminal_session is not None:
            try:
                self._terminal_session.close()
            except Exception:
                log.debug("terminal session close failed", exc_info=True)

    # ==================================================================
    # Orchestration & multi-agent (𝒪 / ℛ)
    # ==================================================================
    def _build_orchestration(self, settings: Settings, state_dir: str) -> None:
        # Only reached when ``settings.orchestration.enabled`` (gated in __init__).
        orch = getattr(settings, "orchestration", None)

        # Tier router: classifies each task tool-vs-shell-vs-sandbox with
        # confidence-aware escalation; decisions are logged for audit/calibration.
        # Used for real by ``process_goal`` to pick a tier before kernel work.
        self.router = TierRouter(
            llm=self.llm,
            registry=self.registry,
            memory=self.memory,
            audit_log=RoutingAuditLog(path=f"{state_dir}/routing.jsonl"),
        )

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
        """The live entry point. A conservative **complexity gate** keeps common
        turns on the cheap single-step kernel loop; a genuinely multi-step goal
        (or an explicit ``/plan``) is escalated to the orchestrated planner path,
        whose result is folded back into an :class:`AgentResponse` so the UI /
        headless callers see one stable contract.
        """
        if self._should_orchestrate(user_message):
            result = await self.process_goal(user_message)
            return self._as_response(result)
        return await self.kernel.orchestrate(user_message)

    def _should_orchestrate(self, message: str) -> bool:
        """The complexity gate for the *live* path. Conservative on purpose:
        orchestration must be enabled, and the turn must either be an explicit
        ``/plan`` request or decompose into enough steps (the planner's gate).
        Everything else stays on the cheap kernel loop.
        """
        if not self._orchestration_on or self.planner is None:
            return False
        stripped = (message or "").strip()
        if stripped.lower().startswith("/plan"):
            return True
        return self.planner.is_complex(stripped)

    async def process_goal(self, goal: str):
        """Orchestrated entry point for complex, multi-step goals.

        The **complexity gate** decides: a goal that does not decompose into
        enough subtasks runs single-step through the kernel (no planner ceremony).
        A genuinely multi-step goal is **routed to a tier** by the
        :class:`~core.router.TierRouter` (explicit tool/shell/sandbox choice —
        not left implicit to the LLM picking a tool), decomposed into an
        inspectable DAG, presented for approval (plan-approve-execute), then
        driven by the plan→act→observe→verify controller with bounded
        self-correction. Returns the kernel's :class:`AgentResponse` for the
        simple path, or the :class:`~core.loop.OrchestrationResult` for the
        orchestrated path.
        """
        if not self._orchestration_on or self.planner is None:
            return await self.kernel.orchestrate(goal)

        forced = goal.strip().lower().startswith("/plan")
        clean = goal.strip()[len("/plan"):].strip() if forced else goal.strip()
        if not forced and not self.planner.is_complex(clean):
            return await self.kernel.orchestrate(clean)

        # Route to an execution tier *explicitly* before any kernel work — this
        # closes the gap where tiering happened implicitly via the LLM choosing
        # run_sandbox_code / run_shell_command as ordinary tools. The decision is
        # logged for audit/calibration by the router's audit log.
        decision = await self.router.route(clean)
        self._emit_status(
            f"Routed to {decision.tier} tier "
            f"({decision.confidence:.2f}) — {decision.rationale}"
        )

        dag = self.planner.decompose(clean)
        self.blackboard.record_decision(
            f"routed goal to {decision.tier} tier; planned {len(dag)} step(s)",
            agent="lead",
        )
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
        self.blackboard.record_decision(f"approved plan for goal: {clean}", agent="lead")
        return await self.orchestrator.run_dag(dag)

    def _as_response(self, result) -> AgentResponse:
        """Fold an orchestrated outcome back into the :class:`AgentResponse`
        contract the live callers (CLI / headless) expect.
        """
        if isinstance(result, AgentResponse):
            return result  # simple path already returned a kernel response
        from dacli.core.planner import TaskDAG
        from dacli.core.loop import OrchestrationResult
        if isinstance(result, TaskDAG):
            return AgentResponse(
                content="Proposed plan (not approved — nothing executed):\n" + result.render(),
                tool_calls=[],
                needs_user_input=True,
            )
        if isinstance(result, OrchestrationResult):
            parts = [result.summary()]
            parts.extend(
                f"- {outcome.node_id}: {outcome.detail}"
                for outcome in result.outcomes
                if getattr(outcome, "detail", None)
            )
            error = (
                f"{len(result.escalated)} step(s) escalated to human review"
                if result.escalated else None
            )
            return AgentResponse(
                content="\n".join(parts),
                tool_calls=[],
                error=error,
                needs_user_input=bool(result.paused),
            )
        # Defensive: anything else stringifies into a plain response.
        return AgentResponse(content=str(result), tool_calls=[])

    def get_progress(self) -> dict:
        # Get current progress summary
        return self.memory.get_progress_summary()
