"""Built-in 'system' connector.

The built-in tools (``request_user_input``, ``update_plan``, …) used to be
special-cased inside the agent's dispatch ladder. Modelling them as a connector
means *all* tools flow through the single dispatch path.

Unlike the platform connectors, the system connector needs runtime collaborators
(the user-input callback and the agent's memory), so it is constructed and
injected by the agent rather than discovered from a manifest. It is always
enabled.
"""

from typing import Any
from collections.abc import Callable

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.verify import result_succeeded, data_has_keys


class SystemConnector(Connector):

    name = "system"

    def __init__(
        self,
        settings: Any = None,
        memory: Any = None,
        on_user_input_needed: Callable[[str], str] | None = None,
    ):
        super().__init__(settings)
        self._memory = memory
        self._on_user_input_needed = on_user_input_needed
        # Optional back-reference to the registry, wired by the agent *after* the
        # registry is built (the registry is constructed with this connector
        # injected, so we cannot take it at __init__). Used by
        # ``load_connector_tools`` to validate an id and echo its operations.
        self._registry: Any = None
        # On-disk store for spilled tool results, late-bound by the
        # agent so ``fetch_result`` can read what the kernel's spill hook wrote.
        self._result_store: Any = None
        # Scrollback source for the governed terminal, late-bound by the agent so
        # ``fetch_scrollback`` can answer "what did step N output?" by command_id
        # without ever inlining a 10k-line dump into context.
        self._scrollback: Any = None
        # Reasoning LLM, late-bound by the agent so ``generate_connector`` can
        # write a new connector from a natural-language description in-chat.
        self._llm: Any = None
        # Always ready; no external connection.
        self._is_connected = True

    def bind_registry(self, registry: Any) -> None:
        """Late-bind the registry (progressive disclosure)."""
        self._registry = registry

    def bind_result_store(self, store: Any) -> None:
        """Late-bind the spilled-result store (off-context spill)."""
        self._result_store = store

    def bind_scrollback(self, source: Any) -> None:
        """Late-bind the terminal scrollback source (Era 2 JIT fetch)."""
        self._scrollback = source

    def bind_llm(self, llm: Any) -> None:
        """Late-bind the reasoning LLM (used by ``generate_connector``)."""
        self._llm = llm

    # ------------------------------------------------------------------
    # Connector contract
    # ------------------------------------------------------------------
    def operations(self) -> list[OperationSpec]:
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
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="update_plan",
                description=(
                    "Maintain your task plan as a todo list. Pass the FULL ordered "
                    "list every call — it replaces the previous plan. Use it to "
                    "break a multi-step task into steps and show the user progress: "
                    "keep exactly one item 'in_progress' while you work it, then "
                    "mark it 'completed' and start the next."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "todos": {
                            "type": "array",
                            "description": "The complete, ordered todo list for the current task.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "content": {
                                        "type": "string",
                                        "description": "Concise description of the step.",
                                    },
                                    "status": {
                                        "type": "string",
                                        "enum": ["pending", "in_progress", "completed"],
                                        "description": "Status of this step.",
                                    },
                                },
                                "required": ["content", "status"],
                            },
                        }
                    },
                    "required": ["todos"],
                },
                capability="system.plan",
                risk=Risk.SAFE,
                display_name="Update Plan",
                category="system",
                postconditions=[data_has_keys("todos", "total", name="plan_echoed")],
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
                postconditions=[data_has_keys("connector_id", "operations", name="disclosed")],
            ),
            OperationSpec(
                name="generate_connector",
                description=(
                    "Create a brand-new connector for a platform dacli doesn't support yet, "
                    "from a natural-language description. Generates the connector code, writes "
                    "it to connectors/<id>/, validates it, and registers it DISABLED. Tell the "
                    "user to run /connect <id> to add credentials and /import-connector <id> to "
                    "enable it (a restart loads it). Use only when no existing connector fits."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Short connector id, lowercase, no spaces (e.g. 'stripe', 'jira').",
                        },
                        "description": {
                            "type": "string",
                            "description": "What the connector integrates with and which operations it should expose.",
                        },
                    },
                    "required": ["name", "description"],
                },
                capability="system.generate_connector",
                # Writes generated code to disk; validation imports it. WRITE so
                # governance gates it before it runs.
                risk=Risk.WRITE,
                display_name="Generate Connector",
                category="system",
                postconditions=[data_has_keys("connector", "validated", name="connector_generated")],
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
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="fetch_scrollback",
                description="Fetch the full (or a line-window of a) terminal command's output by its command_id. When run_shell_command returns a 'scrollback_handle' (its command_id) and the output was spilled, use this to read what the command actually printed — including answering 'what did step N output?'.",
                parameters={
                    "type": "object",
                    "properties": {
                        "command_id": {
                            "type": "string",
                            "description": "The command_id / scrollback_handle from a run_shell_command result.",
                        },
                        "start": {
                            "type": "integer",
                            "description": "0-based line index to start from (default 0).",
                        },
                        "count": {
                            "type": "integer",
                            "description": "How many lines to return (omit for all from start).",
                        },
                    },
                    "required": ["command_id"],
                },
                capability="system.fetch_scrollback",
                risk=Risk.SAFE,
                display_name="Fetch Scrollback",
                category="system",
                postconditions=[result_succeeded()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        if op == "request_user_input":
            return self._request_user_input(args)
        if op == "update_plan":
            return self._update_plan(args)
        if op == "load_connector_tools":
            return self._load_connector_tools(args)
        if op == "generate_connector":
            return await self._generate_connector(args)
        if op == "fetch_result":
            return self._fetch_result(args)
        if op == "fetch_scrollback":
            return self._fetch_scrollback(args)
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
    def _request_user_input(self, args: dict[str, Any]) -> ToolResult:
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
        return ToolResult(
            tool_name="request_user_input",
            status=ToolStatus.PENDING_APPROVAL,
            data={"question": question, "context": context}
        )

    def _load_connector_tools(self, args: dict[str, Any]) -> ToolResult:
        # Progressive disclosure. The kernel reads
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
        op_names: list[str] = []
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

    async def _generate_connector(self, args: dict[str, Any]) -> ToolResult:
        # In-chat connector generation. Mirrors /new-connector minus the prompts,
        # via the shared, non-interactive generate_connector_files().
        name = (args.get("name") or "").strip()
        description = (args.get("description") or "").strip()
        if not name or not description:
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error="Both 'name' and 'description' are required.",
            )
        if self._llm is None:
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error="No LLM is available to generate a connector in this session.",
            )

        # Hard human gate: the agent must NEVER create a connector on its own.
        # We always ask the user to confirm here — independent of the model —
        # and we surface the existing connectors so they can decline in favor of
        # extending one instead of spawning a redundant parallel connector.
        existing = []
        if self._registry is not None:
            try:
                existing = self._registry.get_connector_ids()
            except Exception:
                existing = []
        existing_str = ", ".join(existing) if existing else "(none)"

        if self._on_user_input_needed is None:
            # No way to ask a human → fail closed (do not create).
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error=(
                    "Creating a connector needs your confirmation, but no interactive "
                    "prompt is available here. Run /new-connector yourself instead."
                ),
            )

        question = (
            f"The agent wants to CREATE A NEW connector '{name}'.\n"
            f"  Description: {description}\n"
            f"  Existing connectors: {existing_str}\n"
            f"If one of those already covers this platform, decline and ask the agent "
            f"to extend it instead.\n"
            f"Create new connector '{name}'? (yes / no)"
        )
        answer = (self._on_user_input_needed(question) or "").strip().lower()
        if answer not in ("y", "yes", "ok", "okay", "confirm", "create", "go", "do it"):
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.SUCCESS,
                data={
                    "connector": None,
                    "validated": False,
                    "cancelled": True,
                    "message": (
                        f"User declined creating '{name}'. Do NOT create it. Ask whether they "
                        f"want to extend an existing connector ({existing_str}) instead."
                    ),
                },
            )

        from dacli.core.connector_generator import generate_connector_files

        try:
            result = await generate_connector_files(
                name, description, self.settings, self._llm
            )
        except FileExistsError as exc:
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error=str(exc),
            )
        except Exception as exc:
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error=f"Generation failed: {exc}",
            )

        return ToolResult(
            tool_name="generate_connector",
            status=ToolStatus.SUCCESS,
            data={
                "connector": result.name,
                "path": str(result.path),
                "validated": result.validated,
                "validation_message": result.message,
                "enabled": False,
                "next_steps": [
                    f"/connect {result.name}",
                    f"/import-connector {result.name}",
                    f"/testmode {result.name}",
                ],
            },
        )

    def _fetch_result(self, args: dict[str, Any]) -> ToolResult:
        # Off-context spill read path. Returns rows from the on-disk
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

    def _fetch_scrollback(self, args: dict[str, Any]) -> ToolResult:
        # JIT read of a spilled terminal output by command_id. The full text
        # lives in the session workspace (and the human's TUI); only the model's
        # context copy was bounded.
        if self._scrollback is None:
            return ToolResult(
                tool_name="fetch_scrollback",
                status=ToolStatus.ERROR,
                error="No terminal scrollback is available in this session.",
            )
        command_id = (args.get("command_id") or args.get("handle") or "").strip()
        if not command_id:
            return ToolResult(
                tool_name="fetch_scrollback",
                status=ToolStatus.ERROR,
                error="command_id is required.",
            )
        start = int(args.get("start") or 0)
        count = args.get("count")
        count = int(count) if count is not None else None
        payload = self._scrollback.get(command_id, start=start, count=count)
        if "error" in payload:
            return ToolResult(tool_name="fetch_scrollback", status=ToolStatus.ERROR, error=payload["error"])
        return ToolResult(
            tool_name="fetch_scrollback",
            status=ToolStatus.SUCCESS,
            data=payload.get("output"),
            metadata={k: v for k, v in payload.items() if k != "output"},
        )

    def _update_plan(self, args: dict[str, Any]) -> ToolResult:
        # Generic todo-list planning (Claude-Code style); replaces the whole list.
        valid = {"pending", "in_progress", "completed"}
        todos: list[dict[str, Any]] = []
        for item in args.get("todos") or []:
            if not isinstance(item, dict):
                continue
            content = str(item.get("content", "")).strip()
            if not content:
                continue
            status = item.get("status", "pending")
            todos.append({
                "content": content,
                "status": status if status in valid else "pending",
            })

        self._memory.set_todos(todos)

        completed = sum(1 for t in todos if t["status"] == "completed")
        return ToolResult(
            tool_name="update_plan",
            status=ToolStatus.SUCCESS,
            data={"todos": todos, "completed": completed, "total": len(todos)},
        )
