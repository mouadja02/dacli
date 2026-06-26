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
import logging

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.core.verify import result_succeeded, data_has_keys

log = logging.getLogger(__name__)


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
        # Extension host, late-bound after construction for generation + reload.
        self._extension_host: Any = None
        # Secret store, late-bound for config introspection by list_extensions.
        self._secrets: Any = None
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

    def bind_extension_host(self, host: Any) -> None:
        """Late-bind the extension host (generation + hot-reload)."""
        self._extension_host = host

    def bind_secrets(self, store: Any) -> None:
        """Late-bind the secret store for config introspection."""
        self._secrets = store

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
                            "description": "The question or information request for the user",
                        },
                        "context": {
                            "type": "string",
                            "description": "Context about what led to this request",
                        },
                    },
                    "required": ["question"],
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
                postconditions=[
                    data_has_keys("connector_id", "operations", name="disclosed")
                ],
            ),
            OperationSpec(
                name="generate_connector",
                description=(
                    "Generate a new extension for a service dacli doesn't support yet. "
                    "The LLM writes a register(api) module, validates it, and hot-reloads "
                    "it into the session — no restart needed. Call this autonomously when "
                    "the user's task needs a service with no existing extension. After "
                    "generation, tell the user to run /connect <id> to add credentials."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Short extension id, lowercase, no spaces (e.g. 'stripe', 'jira').",
                        },
                        "description": {
                            "type": "string",
                            "description": "What the extension integrates with and which operations it should expose.",
                        },
                    },
                    "required": ["name", "description"],
                },
                capability="system.generate_connector",
                risk=Risk.WRITE,
                display_name="Generate Extension",
                category="system",
                postconditions=[
                    data_has_keys("connector", "validated", name="connector_generated")
                ],
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
            OperationSpec(
                name="list_extensions",
                description="List loaded extensions and whether their credentials are configured.",
                parameters={"type": "object", "properties": {}},
                capability="system.list_extensions",
                risk=Risk.SAFE,
                display_name="List Extensions",
                category="system",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="edit_extension",
                description=(
                    "Fix a broken extension by sending its source and error to the LLM. "
                    "Call this IMMEDIATELY when a tool call fails with a Python error "
                    "traceable to extension code. Do not report the error to the user — "
                    "fix it first, then retry the original operation."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Extension id to fix (e.g. 'aws_s3').",
                        },
                        "error": {
                            "type": "string",
                            "description": "The error message or traceback from the failed tool call.",
                        },
                    },
                    "required": ["name", "error"],
                },
                capability="system.edit_extension",
                risk=Risk.WRITE,
                display_name="Edit Extension",
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
        if op == "list_extensions":
            return self._list_extensions(args)
        if op == "edit_extension":
            return await self._edit_extension(args)
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    async def health(self) -> ToolResult:
        return ToolResult(
            tool_name=self.name, status=ToolStatus.SUCCESS, data={"ready": True}
        )

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
                data={"user_response": user_response},
            )
        return ToolResult(
            tool_name="request_user_input",
            status=ToolStatus.PENDING_APPROVAL,
            data={"question": question, "context": context},
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

    def _list_extensions(self, args: dict[str, Any]) -> ToolResult:
        if self._extension_host is None:
            return ToolResult(
                tool_name="list_extensions",
                status=ToolStatus.ERROR,
                error="Extension host not bound.",
            )
        reg = self._extension_host.registry
        extensions = []
        for ext_id in reg.extension_ids():
            configured = (
                ext_id in self._secrets.extensions() if self._secrets else False
            )
            extensions.append(
                {
                    "id": ext_id,
                    "status": "loaded",
                    "configured": configured,
                }
            )
        for ext_id, reason in reg.failed_extensions().items():
            extensions.append(
                {
                    "id": ext_id,
                    "status": "failed",
                    "reason": reason,
                }
            )
        return ToolResult(
            tool_name="list_extensions",
            status=ToolStatus.SUCCESS,
            data={"extensions": extensions, "count": len(extensions)},
        )

    async def _generate_connector(self, args: dict[str, Any]) -> ToolResult:
        """In-chat extension generation via core/generate.py + hot-reload."""
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
                error="No LLM available in this session.",
            )
        if self._extension_host is None:
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error="Extension host not bound; cannot generate.",
            )

        # Auto-generate without user confirmation — the agent decides when
        # a new extension is needed. The governance layer already gates risky ops.
        existing = []
        if self._extension_host is not None:
            try:
                base = self._extension_host.base_dir()
                if base.exists():
                    existing = [p.name for p in base.iterdir() if p.is_dir()]
            except Exception:
                log.debug("failed to list extensions", exc_info=True)

        # Reject if extension already exists (use edit_extension to fix it).
        norm_name = name.lower().replace(" ", "_").replace("-", "_")
        if norm_name in existing:
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error=f"extension '{norm_name}' already exists. Use edit_extension to fix it.",
            )

        from dacli.core.generate import (
            ClarifyUI,
            GenerationError,
            SecretInlineError,
            generate_extension,
            _fetch_context7_docs_silent,
        )

        # Fetch Context7 docs silently (no user prompt in the LLM-driven path).
        docs = None
        try:
            docs = await _fetch_context7_docs_silent(name, description)
        except Exception:
            log.debug("Context7 docs fetch failed", exc_info=True)

        # Autonomous ClarifyUI: auto-picks first option / confirms yes.
        # No user interaction needed — the agent makes sensible choices.
        class _AutoUI(ClarifyUI):
            def __init__(self, ask_fn=None):
                self._ask = ask_fn

            def select(self, question: str, options: list[str]) -> str:
                # Pick first option (most common/standard choice).
                return options[0] if options else ""

            def confirm(self, question: str) -> bool:
                return True

            def input(self, prompt: str, *, secret: bool = False) -> str:
                # If we have a user callback, use it for secrets only.
                if secret and self._ask:
                    return self._ask(prompt)
                return ""

        try:
            result = await generate_extension(
                name,
                description,
                llm=self._llm,
                ui=_AutoUI(self._on_user_input_needed),
                host=self._extension_host,
                docs=docs,
            )
        except FileExistsError as exc:
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error=str(exc),
            )
        except SecretInlineError as exc:
            return ToolResult(
                tool_name="generate_connector",
                status=ToolStatus.ERROR,
                error=f"Rejected: {exc}",
            )
        except (GenerationError, ValueError) as exc:
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
                "reloaded": result.reloaded,
                "validation_message": result.message,
                "next_steps": (
                    [f"/connect {result.name}"]
                    if result.reloaded
                    else [f"Edit {result.path}", "/reload"]
                ),
            },
        )

    async def _edit_extension(self, args: dict[str, Any]) -> ToolResult:
        """Fix a broken extension by sending its source + error to the LLM."""
        name = (args.get("name") or "").strip()
        error = (args.get("error") or "").strip()
        if not name or not error:
            return ToolResult(
                tool_name="edit_extension",
                status=ToolStatus.ERROR,
                error="Both 'name' and 'error' are required.",
            )
        if self._llm is None:
            return ToolResult(
                tool_name="edit_extension",
                status=ToolStatus.ERROR,
                error="No LLM available.",
            )
        if self._extension_host is None:
            return ToolResult(
                tool_name="edit_extension",
                status=ToolStatus.ERROR,
                error="Extension host not bound.",
            )

        from dacli.core.generate import (
            GenerationError,
            SecretInlineError,
            edit_extension,
        )

        try:
            result = await edit_extension(
                name, error, llm=self._llm, host=self._extension_host
            )
        except FileNotFoundError as exc:
            return ToolResult(
                tool_name="edit_extension",
                status=ToolStatus.ERROR,
                error=str(exc),
            )
        except (GenerationError, SecretInlineError, ValueError) as exc:
            return ToolResult(
                tool_name="edit_extension",
                status=ToolStatus.ERROR,
                error=f"Edit failed: {exc}",
            )

        return ToolResult(
            tool_name="edit_extension",
            status=ToolStatus.SUCCESS,
            data={
                "extension": result.name,
                "path": str(result.path),
                "fixed": result.reloaded,
                "message": result.message,
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
            return ToolResult(
                tool_name="fetch_result",
                status=ToolStatus.ERROR,
                error=payload["error"],
            )
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
            return ToolResult(
                tool_name="fetch_scrollback",
                status=ToolStatus.ERROR,
                error=payload["error"],
            )
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
            todos.append(
                {
                    "content": content,
                    "status": status if status in valid else "pending",
                }
            )

        self._memory.set_todos(todos)

        completed = sum(1 for t in todos if t["status"] == "completed")
        return ToolResult(
            tool_name="update_plan",
            status=ToolStatus.SUCCESS,
            data={"todos": todos, "completed": completed, "total": len(todos)},
        )
