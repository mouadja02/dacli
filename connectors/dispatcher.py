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

from connectors.base import ToolResult, ToolStatus
from connectors.registry import ConnectorRegistry


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

        # Emit tool end
        if self._on_tool_end:
            self._on_tool_end(tool_name, result)

        return result
