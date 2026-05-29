"""Built-in 'system' connector.

The two built-in tools (``request_user_input`` and ``update_progress``) used to
be special-cased inside the agent's dispatch ladder. Modelling them as a
connector means *all* tools flow through the single dispatch path.

Unlike the platform connectors, the system connector needs runtime collaborators
(the user-input callback and the agent's memory), so it is constructed and
injected by the agent rather than discovered from a manifest. It is always
enabled.
"""

from typing import Any, Callable, Dict, List, Optional

from connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus


class SystemConnector(Connector):

    name = "system"

    def __init__(
        self,
        settings: Any = None,
        memory: Any = None,
        on_user_input_needed: Optional[Callable[[str], str]] = None,
    ):
        super().__init__(settings)
        self._memory = memory
        self._on_user_input_needed = on_user_input_needed
        # Optional back-reference to the registry, wired by the agent *after* the
        # registry is built (the registry is constructed with this connector
        # injected, so we cannot take it at __init__). Used by
        # ``load_connector_tools`` to validate an id and echo its operations.
        self._registry: Any = None
        # On-disk store for spilled tool results (Phase 3.4), late-bound by the
        # agent so ``fetch_result`` can read what the kernel's spill hook wrote.
        self._result_store: Any = None
        # Always ready; no external connection.
        self._is_connected = True

    def bind_registry(self, registry: Any) -> None:
        """Late-bind the registry (progressive disclosure, Phase 3.3)."""
        self._registry = registry

    def bind_result_store(self, store: Any) -> None:
        """Late-bind the spilled-result store (off-context spill, Phase 3.4)."""
        self._result_store = store

    # ------------------------------------------------------------------
    # Connector contract
    # ------------------------------------------------------------------
    def operations(self) -> List[OperationSpec]:
        return [
            OperationSpec(
                name="request_user_input",
                description="Request input from the user when stuck, encountering errors, or needing clarification. Use this to put the user in the loop.",
                parameters={
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "The question or information request for the user"
                        },
                        "context": {
                            "type": "string",
                            "description": "Context about what led to this request"
                        }
                    },
                    "required": ["question"]
                },
                capability="system.user_input",
                risk=Risk.SAFE,
                display_name="Request User Input",
                category="system",
            ),
            OperationSpec(
                name="update_progress",
                description="Update the current progress status. Use to track phases and steps completed.",
                parameters={
                    "type": "object",
                    "properties": {
                        "phase": {
                            "type": "string",
                            "description": "Current phase (phase_0_infrastructure, phase_1_discovery, etc.)"
                        },
                        "step": {
                            "type": "string",
                            "description": "Description of the step completed"
                        },
                        "status": {
                            "type": "string",
                            "enum": ["in_progress", "completed", "failed"],
                            "description": "Status of the phase"
                        }
                    },
                    "required": ["phase", "step"]
                },
                capability="system.progress",
                risk=Risk.SAFE,
                display_name="Update Progress",
                category="system",
            ),
            OperationSpec(
                name="load_connector_tools",
                description="Disclose the full operations of a connector you want to use. The system prompt lists connectors by id with a one-line description but NOT their full tool schemas (to save context). Call this with a connector id to make that connector's tools available on the next step.",
                parameters={
                    "type": "object",
                    "properties": {
                        "connector_id": {
                            "type": "string",
                            "description": "The connector id to disclose (e.g. 'snowflake', 'github'), as listed in the connectors digest.",
                        },
                    },
                    "required": ["connector_id"],
                },
                capability="system.disclosure",
                risk=Risk.SAFE,
                display_name="Load Connector Tools",
                category="system",
            ),
            OperationSpec(
                name="fetch_result",
                description="Fetch the full (or a slice of a) large tool result that was spilled off-context. When a previous result is summarized with a 'handle', use that handle here to read the actual rows.",
                parameters={
                    "type": "object",
                    "properties": {
                        "handle": {
                            "type": "string",
                            "description": "The result handle from a spilled-result summary (e.g. 'res_120355_ab12cd').",
                        },
                        "start": {
                            "type": "integer",
                            "description": "0-based row index to start from (default 0).",
                        },
                        "count": {
                            "type": "integer",
                            "description": "How many rows to return (omit for all from start).",
                        },
                    },
                    "required": ["handle"],
                },
                capability="system.fetch_result",
                risk=Risk.SAFE,
                display_name="Fetch Result",
                category="system",
            ),
        ]

    async def invoke(self, op: str, args: Dict[str, Any]) -> ToolResult:
        if op == "request_user_input":
            return self._request_user_input(args)
        elif op == "update_progress":
            return self._update_progress(args)
        elif op == "load_connector_tools":
            return self._load_connector_tools(args)
        elif op == "fetch_result":
            return self._fetch_result(args)
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    async def health(self) -> ToolResult:
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS, data={"ready": True})

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------
    def _request_user_input(self, args: Dict[str, Any]) -> ToolResult:
        question = args.get("question", "")
        context = args.get("context", "")

        if self._on_user_input_needed:
            user_response = self._on_user_input_needed(
                f"{context}\n\n{question}" if context else question
            )
            return ToolResult(
                tool_name="request_user_input",
                status=ToolStatus.SUCCESS,
                data={"user_response": user_response}
            )
        else:
            return ToolResult(
                tool_name="request_user_input",
                status=ToolStatus.PENDING_APPROVAL,
                data={"question": question, "context": context}
            )

    def _load_connector_tools(self, args: Dict[str, Any]) -> ToolResult:
        # Progressive disclosure (Phase 3.3). The kernel reads
        # ``metadata['disclose']`` and adds the id to the turn's disclosed set,
        # so the connector's full schemas are packed on the next iteration.
        connector_id = (args.get("connector_id") or "").strip()
        if not connector_id:
            return ToolResult(
                tool_name="load_connector_tools",
                status=ToolStatus.ERROR,
                error="connector_id is required.",
            )

        # Validate against the registry when bound; echo the connector's
        # operations so the model knows what just became available.
        op_names: List[str] = []
        if self._registry is not None:
            if not self._registry.is_connector_enabled(connector_id):
                available = [d["id"] for d in self._registry.get_tool_digest()]
                return ToolResult(
                    tool_name="load_connector_tools",
                    status=ToolStatus.ERROR,
                    error=(
                        f"Unknown or disabled connector '{connector_id}'. "
                        f"Available: {', '.join(available) or '(none)'}."
                    ),
                )
            connector = self._registry.get_connector(connector_id)
            if connector is not None:
                op_names = [
                    spec.name
                    for spec in connector.operations()
                    if self._registry.is_operation_enabled(spec.name)
                ]

        return ToolResult(
            tool_name="load_connector_tools",
            status=ToolStatus.SUCCESS,
            data={"connector_id": connector_id, "operations": op_names},
            metadata={"disclose": connector_id},
        )

    def _fetch_result(self, args: Dict[str, Any]) -> ToolResult:
        # Off-context spill read path (Phase 3.4). Returns rows from the on-disk
        # store written by the kernel's spill hook.
        if self._result_store is None:
            return ToolResult(
                tool_name="fetch_result",
                status=ToolStatus.ERROR,
                error="No result store is available in this session.",
            )
        handle = (args.get("handle") or "").strip()
        if not handle:
            return ToolResult(
                tool_name="fetch_result",
                status=ToolStatus.ERROR,
                error="handle is required.",
            )
        start = int(args.get("start") or 0)
        count = args.get("count")
        count = int(count) if count is not None else None
        payload = self._result_store.read(handle, start=start, count=count)
        if "error" in payload:
            return ToolResult(tool_name="fetch_result", status=ToolStatus.ERROR, error=payload["error"])
        # Return the rows as the result data so the CLI renders them as a table.
        return ToolResult(
            tool_name="fetch_result",
            status=ToolStatus.SUCCESS,
            data=payload.get("data"),
            metadata={k: v for k, v in payload.items() if k != "data"},
        )

    def _update_progress(self, args: Dict[str, Any]) -> ToolResult:
        # Imported lazily to avoid a core <-> connectors import cycle at load time.
        from core.memory import PhaseStatus

        phase = args.get("phase", "")
        step = args.get("step", "")
        status = args.get("status", "in_progress")

        status_enum = {
            "in_progress": PhaseStatus.IN_PROGRESS,
            "completed": PhaseStatus.COMPLETED,
            "failed": PhaseStatus.FAILED
        }.get(status, PhaseStatus.IN_PROGRESS)

        self._memory.update_phase(phase, status=status_enum, step_completed=step)

        return ToolResult(
            tool_name="update_progress",
            status=ToolStatus.SUCCESS,
            data={"phase": phase, "step": step, "status": status}
        )
