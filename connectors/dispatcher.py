"""Generic tool dispatcher.

Collapses the old 100-line ``if tool_name == "...":`` ladder in
``DACLI._execute_tool`` into a single lookup + call. The registry resolves the
tool name to ``(connector, op)``; the connector's ``invoke`` does the work. The
dispatcher keeps the cross-cutting concerns that wrapped the old ladder: timing,
the start/end callbacks, and memory logging.

Deliberately *removed* here (was a correctness hazard): the regex-driven memory
mutation that ran on Snowflake results, e.g.::

    if "CREATE SCHEMA" in query: self.memory.add_created_schema(...)
    elif "CREATE" in query and "FILE FORMAT" in query: ...
    elif "CREATE" in query and "TABLE" in query: ...

These silently corrupted state on any non-standard SQL. They are enumerated in
``docs/phase2-deferred-postconditions.md`` to be reimplemented as proper
post-conditions / catalog updates in Phase 2.
"""

import time
from typing import Any, Callable, Dict, Optional

from connectors.base import ToolResult, ToolStatus, Risk
from connectors.registry import ConnectorRegistry
# NOTE: ``core.verify`` is imported lazily inside ``_verify`` — importing it at
# module top would pull in ``core/__init__`` (which eagerly imports the agent,
# which imports this dispatcher) and create a circular import.

# Risk levels at which a successful op may have changed live structure, so its
# catalog effects (create/invalidate) must be applied. SAFE (read-only) ops are
# skipped — this is where Phase 1's risk metadata earns its keep.
_MUTATING_RISKS = {Risk.WRITE, Risk.RISKY, Risk.IRREVERSIBLE}


class Dispatcher:
    def __init__(
        self,
        registry: ConnectorRegistry,
        memory: Any = None,
        on_tool_start: Optional[Callable[[str, Dict], None]] = None,
        on_tool_end: Optional[Callable[[str, ToolResult], None]] = None,
        verifier: Any = None,
        governor: Any = None,
    ):
        self._registry = registry
        self._memory = memory
        self._on_tool_start = on_tool_start
        self._on_tool_end = on_tool_end
        # Phase 4: optional post-condition runner. When present, a successful op
        # that declares post-conditions is verified before it is accepted; a
        # failed post-condition downgrades the result to ERROR so the kernel and
        # the catalog never treat an unverified outcome as done.
        self._verifier = verifier
        # Phase 5: optional governance gate (𝒢). When present, every action is
        # classified by blast radius and run through the policy engine *before*
        # ``invoke`` — denied/blocked actions short-circuit and never execute;
        # the outcome (and its post-condition verdict) is recorded in the audit
        # ledger. The sandbox SDK shares this same governor, so code-execution
        # is governed identically.
        self._governor = governor

    async def execute(self, tool_name: str, arguments: Dict[str, Any]) -> ToolResult:
        start_time = time.time()

        # Emit tool start
        if self._on_tool_start:
            self._on_tool_start(tool_name, arguments)

        resolved = self._registry.resolve(tool_name)

        if resolved is None:
            result = ToolResult(
                tool_name=tool_name,
                status=ToolStatus.ERROR,
                error=f"Unknown tool: {tool_name}",
            )
        else:
            connector, op = resolved

            # Governance pre-flight (Phase 5): classify blast radius → policy →
            # permissions → rollback → human approval, all *before* execution. A
            # denied/blocked action short-circuits here and never runs.
            decision = None
            if self._governor is not None:
                spec = self._registry.get_operation_spec(tool_name)
                decision = await self._governor.review(tool_name, spec, arguments, connector)
                if not decision.allowed:
                    short = decision.short_circuit or ToolResult(
                        tool_name=tool_name, status=ToolStatus.DENIED,
                        error=decision.blocked_reason or "blocked by governance",
                    )
                    if self._on_tool_end:
                        self._on_tool_end(tool_name, short)
                    return short

            try:
                result = await connector.invoke(op, arguments)
            except Exception as e:
                result = ToolResult(
                    tool_name=tool_name,
                    status=ToolStatus.ERROR,
                    error=str(e),
                    execution_time_ms=(time.time() - start_time) * 1000,
                )
            else:
                # Post-condition gate (Phase 4): only a *verified* success is a
                # success. Runs before logging/catalog effects so a failed check
                # never lets a bad outcome propagate as done.
                result = await self._verify(tool_name, connector, arguments, result)

            # Record the execution outcome + post-condition verdict in the audit
            # ledger so the decision is reconstructable end to end (Phase 5.4).
            if self._governor is not None and decision is not None:
                self._governor.record_outcome(decision, result)

        # Log tool execution
        if self._memory is not None:
            self._memory.log_tool_execution(
                tool_name=tool_name,
                input_params=arguments,
                result=result.data if result.success else None,
                error=result.error,
                execution_time_ms=result.execution_time_ms,
            )

            # Post-condition: apply structured catalog effects (create /
            # write-invalidation). Reimplements the regex side-effects deleted in
            # Phase 1 — now driven by the connector's structured result, gated on
            # the operation's declared risk, and only on success.
            self._apply_catalog_effects(tool_name, resolved, result)

        # Emit tool end
        if self._on_tool_end:
            self._on_tool_end(tool_name, result)

        return result

    async def _verify(self, tool_name, connector, arguments, result: ToolResult) -> ToolResult:
        # Run the operation's declared post-conditions, if any, and gate on them.
        if self._verifier is None or not result.success:
            return result
        spec = self._registry.get_operation_spec(tool_name)
        postconditions = getattr(spec, "postconditions", None) if spec else None
        if not postconditions:
            return result

        from core.verify import VerificationContext

        ctx = VerificationContext(
            args=dict(arguments or {}),
            result=result,
            target=connector,
            memory=self._memory,
        )
        report = await self._verifier.verify(postconditions, ctx, label=tool_name)
        # Record the verdict on the result for audit/UI regardless of outcome.
        result.metadata = {**(result.metadata or {}), "verification": report.to_dict()}

        if not report.passed and getattr(self._verifier, "enforce", True):
            # A failed post-condition is not an accepted result. Downgrade so the
            # kernel surfaces it and catalog effects are skipped.
            return ToolResult(
                tool_name=result.tool_name,
                status=ToolStatus.ERROR,
                data=result.data,
                error=report.summary(),
                execution_time_ms=result.execution_time_ms,
                metadata=result.metadata,
            )
        return result

    def _apply_catalog_effects(self, tool_name, resolved, result: ToolResult) -> None:
        if resolved is None or not result.success:
            return
        if not hasattr(self._memory, "apply_catalog_effects"):
            return
        effects = (result.metadata or {}).get("catalog_effects")
        if not effects:
            return

        # Invariant: a write-INVALIDATION may only come from an op whose declared
        # risk is mutating (write/risky/irreversible) — this is where Phase 1's
        # risk metadata earns its keep. A SAFE op (e.g. introspection) may still
        # CREATE/refresh catalog entries from what it observed live.
        spec = self._registry.get_operation_spec(tool_name)
        mutating = spec is None or spec.risk in _MUTATING_RISKS
        applicable = [
            e for e in effects
            if mutating or e.get("action") != "invalidate"
        ]
        if not applicable:
            return
        connector, _op = resolved
        self._memory.apply_catalog_effects(connector.name, applicable)
