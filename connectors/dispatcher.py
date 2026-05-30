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
    ):
        self._registry = registry
        self._memory = memory
        self._on_tool_start = on_tool_start
        self._on_tool_end = on_tool_end

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
            try:
                result = await connector.invoke(op, arguments)
            except Exception as e:
                result = ToolResult(
                    tool_name=tool_name,
                    status=ToolStatus.ERROR,
                    error=str(e),
                    execution_time_ms=(time.time() - start_time) * 1000,
                )

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
