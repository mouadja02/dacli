"""Session manager + trust-aware memory wiring for dacli.

 split this object's responsibilities in two:

* **Session metadata** (conversation history, tool-execution log, phase/progress
  tracking, errors) stays here — it is *about this run*, not durable knowledge.
* **Durable knowledge** moved into the typed, trust-aware :mod:`memory` package:
  facts live in :class:`~memory.store.MemoryStore` and introspected structure in
  :class:`~memory.catalog.CatalogCache`. The Snowflake/dbt-specific state fields
  (``created_tables``, ``schemas_created``, ``inferred_schemas`` …) are gone;
  what they tracked is now connector-scoped catalog entries with
  ``last_verified`` + TTL.

The ``add_created_*`` methods survive as thin, backward-compatible wrappers that
write catalog entries (the SQL parsing that decides *what* to record now lives in
the connector, driven by structured results — never regex on the dispatch path).
"""

import json

from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from dacli.core.atomicio import write_json_atomic
from dacli.core.logging_setup import get_logger
from dacli.core.timeutils import now_iso
from dacli.memory.store import MemoryStore, MemoryEntry
from dacli.memory.catalog import CatalogCache

log = get_logger(__name__)
from dacli.memory.semantic import SemanticMemory
from dacli.memory.episodic import EpisodicMemory
from dacli.memory.procedural import ProceduralMemory
from dacli.memory.retrieval import retrieve


@dataclass
class ToolExecution:
    # Record of a tool execution
    tool_name: str
    timestamp: str
    status: str
    input_params: dict[str, Any]
    result: Any | None = None
    error: str | None = None
    execution_time_ms: float = 0.0

@dataclass
class Message:
    # A message in the conversation
    role: str # "user", "assistant", "system", "tool"
    content: str
    timestamp: str = field(default_factory=now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

@dataclass
class AgentState:
    """Session metadata — *about this run*, not durable knowledge.

    Connector-scoped facts (created tables/schemas/file formats, loaded row
    counts, inferred schemas) deliberately do **not** live here anymore; they
    are catalog entries (see :class:`~memory.catalog.CatalogCache`).
    """

    # Identification info
    session_id: str
    created_at: str
    updated_at: str

    # Work progress — a generic todo list (Claude-Code style), not pipeline phases.
    # Each item: {"content": str, "status": "pending"|"in_progress"|"completed"}.
    todos: list[dict[str, Any]] = field(default_factory=list)

    # Generic discovery surface (not platform-specific)
    discovered_files: dict[str, Any] = field(default_factory=dict)

    # Error tracking
    last_error: str | None = None
    errors_count: int = 0

class AgentMemory:
    """Session manager wired to the trust-aware :mod:`memory` package.

    Features:
    - Conversation history with windowed context
    - Persistent session metadata + phase/progress tracking
    - Tool execution logging
    - Durable, trust-aware fact store (``store``) and catalog cache (``catalog``)
    - Staleness-penalized retrieval and episodic capture
    """

    def __init__(
        self,
        state_path: str = ".dacli/state/",
        history_path: str = ".dacli/history/",
        memory_window: int = 25,
        memory_path: str = ".dacli/memory/",
    ):
        """
        Initialize agent memory.

        Args:
            state_path: Directory for per-session state files
            history_path: Directory for conversation history
            memory_window: Number of messages to keep in context
            memory_path: Directory for the durable store + catalog cache
        """
        self.state_path = Path(state_path)
        self.history_path = Path(history_path)
        self.memory_window = memory_window

        # Ensure directories exist
        self.state_path.mkdir(parents=True, exist_ok=True)
        self.history_path.mkdir(parents=True, exist_ok=True)

        # Durable, trust-aware memory (project-scoped — persists across sessions).
        memory_dir = Path(memory_path)
        self.store = MemoryStore(path=str(memory_dir / "store.jsonl"))
        self.catalog = CatalogCache(path=str(memory_dir / "catalog.json"))
        self.semantic = SemanticMemory(self.store)
        self.episodic = EpisodicMemory(self.store)
        self.procedural = ProceduralMemory(self.store)

        # Current session data
        self._session_id: str | None = None
        self._messages: list[Message] = []
        self._tool_history: list[ToolExecution] = []
        self._state: AgentState | None = None

    @property
    def session_id(self) -> str:
        # Get current session ID
        if not self._session_id:
            self._session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self._session_id

    @property
    def state(self) -> AgentState:
        # Get current agent state
        if not self._state:
            self._state = self._create_new_state()
        return self._state

    def _create_new_state(self) -> AgentState:
        # Create a new agent state
        now = now_iso()
        return AgentState(
            session_id=self.session_id,
            created_at=now,
            updated_at=now,
        )


    # ========================
    # Message Management
    # ========================
    def add_message(self, role: str, content: str, metadata: dict[str, Any] | None = None):
        # Add a message to the conversation history
        message = Message(role=role, content=content, timestamp=now_iso(), metadata=metadata or {})
        self._messages.append(message)
        self._save_history()

    def add_user_message(self, content: str) -> None:
        # Add a user message
        self.add_message(role="user", content=content)

    def add_assistant_message(self, content: str) -> None:
        # Add an assistant message
        self.add_message(role="assistant", content=content)

    def add_tool_result(self, tool_name: str, result: Any, error: str | None = None) -> None:
        # Add a tool result message
        self.add_message(role="tool", content=str(result), metadata={"tool_name": tool_name, "error": error})

    def get_context_messages(self) -> list[dict[str, str]]:
        # Get messages for LLM context within the memory window
        windowed = self._messages[-self.memory_window:]
        return [{"role": m.role, "content": m.content} for m in windowed]

    def get_full_history(self) -> list[Message]:
        # Get all messages in the conversation
        return self._messages.copy()

    def clear_messages(self) -> None:
        # Clear all messages (new conversation)
        self._messages = []

    # ========================
    # Tool Execution Tracking
    # ========================

    def log_tool_execution(self, tool_name: str, input_params: dict[str, Any], result: Any | None = None, error: str | None = None, execution_time_ms: float = 0.0) -> None:
        # Log tool execution
        tool_execution = ToolExecution(
            tool_name=tool_name,
            timestamp=datetime.now().isoformat(),
            status="success" if error is None else "error",
            input_params=input_params,
            result=result,
            error=error,
            execution_time_ms=execution_time_ms
        )
        self._tool_history.append(tool_execution)
        self._save_state()

    def get_tool_history(self, tool_name: str | None = None) -> list[ToolExecution]:
        # Get tool execution history
        if tool_name:
            return [t for t in self._tool_history if t.tool_name == tool_name]
        return self._tool_history.copy()

    # ========================
    # State Management
    # ========================

    def set_todos(self, todos: list[dict[str, Any]]) -> None:
        """Replace the task todo list (Claude-Code style planning).

        Each todo is ``{"content": str, "status": "pending"|"in_progress"|
        "completed"}``. The full list is replaced on every call.
        """
        self.state.todos = list(todos)
        self.state.updated_at = now_iso()
        self._save_state()

    def add_discovered_file(self, source: str, filename: str) -> None:
        # Record a discovered file
        self.state.discovered_files.setdefault(source, [])
        if filename not in self.state.discovered_files[source]:
            self.state.discovered_files[source].append(filename)
        self._save_state()

    # ========================
    # Durable memory: catalog + facts
    # ========================

    def record_catalog_object(
        self,
        connector: str,
        object_type: str,
        scope: dict[str, Any],
        **kwargs: Any,
    ):
        """Record/refresh an introspected object in the catalog cache."""
        return self.catalog.record_object(connector, object_type, scope, **kwargs)

    def invalidate_catalog(
        self,
        connector: str,
        scope: dict[str, Any],
        object_type: str | None = None,
    ):
        """Mark a catalog scope as no longer trustworthy (write-invalidation)."""
        return self.catalog.invalidate_scope(connector, scope, object_type=object_type)

    def apply_catalog_effects(self, connector: str, effects: list[dict[str, Any]]) -> None:
        """Apply structured catalog effects emitted by a connector op.

        Each effect: ``{"action": "create"|"invalidate", "object_type": str,
        "scope": {...}, ...}``. This is the post-condition path that replaces the
        deleted regex side-effects — driven by structured results, not string
        matching on the raw query.
        """
        for effect in effects or []:
            action = effect.get("action")
            object_type = effect.get("object_type", "table")
            scope = effect.get("scope", {})
            if action == "create":
                self.record_catalog_object(
                    connector,
                    object_type,
                    scope,
                    source=effect.get("source", "ddl"),
                    row_count_estimate=effect.get("row_count_estimate"),
                    columns=effect.get("columns"),
                )
            elif action == "invalidate":
                self.invalidate_catalog(connector, scope, object_type=object_type)

    def remember_fact(
        self,
        content: str,
        *,
        scope: dict[str, Any] | None = None,
        source: str = "inference",
        confidence: float | None = None,
        tags: list[str] | None = None,
    ) -> MemoryEntry:
        """Store a durable semantic fact (config, convention, learned constraint)."""
        return self.semantic.add(
            content, scope=scope, source=source, confidence=confidence, tags=tags
        )

    def retrieve(self, query: str, top_k: int = 5) -> list[MemoryEntry]:
        """Staleness-penalized retrieval across durable facts (hypotheses)."""
        return retrieve(query, self.store.active(), top_k=top_k)

    def capture_episode(
        self,
        goal: str,
        steps: list[dict[str, Any]],
        outcome: str = "completed",
        **kwargs: Any,
    ) -> MemoryEntry:
        """Store a task trace on completion (episodic capture, 2.5)."""
        return self.episodic.capture(goal, steps, outcome=outcome, **kwargs)

    # -- Backward-compatible post-condition wrappers (deferred from) --
    # These take already-extracted object names (the connector does the SQL
    # parsing) and write catalog entries instead of mutating list fields.
    def add_created_schema(self, schema_name: str, connector: str = "snowflake", database: str | None = None) -> None:
        db, schema, _ = self._split_qualified(schema_name, database=database)
        self.record_catalog_object(connector, "schema", {"database": db, "schema": schema or schema_name}, source="ddl")

    def add_created_table(self, table_name: str, connector: str = "snowflake", database: str | None = None, schema: str | None = None) -> None:
        db, sch, obj = self._split_qualified(table_name, database=database, schema=schema)
        self.record_catalog_object(connector, "table", {"database": db, "schema": sch, "object": obj}, source="ddl")

    def add_created_file_format(self, file_format: str, connector: str = "snowflake", database: str | None = None, schema: str | None = None) -> None:
        db, sch, obj = self._split_qualified(file_format, database=database, schema=schema)
        self.record_catalog_object(connector, "file_format", {"database": db, "schema": sch, "object": obj}, source="ddl")

    def add_loaded_table(self, table_name: str, row_count: int = 0, connector: str = "snowflake", database: str | None = None, schema: str | None = None) -> None:
        # Replaces the old (buggy) dict-mutation; row count is a catalog estimate.
        db, sch, obj = self._split_qualified(table_name, database=database, schema=schema)
        self.record_catalog_object(connector, "table", {"database": db, "schema": sch, "object": obj}, source="copy_into", row_count_estimate=row_count)

    @staticmethod
    def _split_qualified(name: str, database: str | None = None, schema: str | None = None):
        """Split a possibly-qualified ``DB.SCHEMA.OBJECT`` name into parts."""
        cleaned = (name or "").strip().rstrip(";").strip()
        parts = [p.strip().strip('"') for p in cleaned.split(".") if p.strip()]
        if len(parts) >= 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return database, parts[0], parts[1]
        return database, schema, parts[0] if parts else cleaned

    # ========================
    # Persistence
    # ========================

    def _get_state_file(self) -> Path:
        # Get the state file path for current session
        return self.state_path / f"state_{self.session_id}.json"

    def _get_history_file(self) -> Path:
        # Get the history file path for current session
        return self.history_path / f"history_{self.session_id}.json"

    def _save_state(self) -> None:
        # Save the state to a file
        state_file = self._get_state_file()
        state_data = asdict(self.state)
        state_data["tool_history"] = [asdict(t) for t in self._tool_history]

        write_json_atomic(state_file, state_data, indent=2, default=str)

    def _save_history(self) -> None:
        # Save the conversation history to a file
        history_file = self._get_history_file()
        history_data = [asdict(m) for m in self._messages]

        write_json_atomic(history_file, history_data, indent=2)

    def list_sessions(self) -> list[dict[str, Any]]:
        """List all available sessions."""
        sessions = []

        for state_file in self.state_path.glob("state_*.json"):
            try:
                with open(state_file, encoding="utf-8") as f:
                    data = json.load(f)

                todos = data.get("todos", []) or []
                active = next((t.get("content") for t in todos if t.get("status") == "in_progress"), None)
                sessions.append({
                    "session_id": data.get("session_id"),
                    "created_at": data.get("created_at"),
                    "updated_at": data.get("updated_at"),
                    "active_task": active,
                    "errors_count": data.get("errors_count", 0),
                })
            except Exception:
                continue

        # Sort by updated_at descending
        sessions.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
        return sessions

    def load_session(self, session_id: str) -> bool:
        """
        Load a previous session.

        Args:
            session_id: Session ID to load

        Returns:
            True if session was loaded successfully
        """
        state_file = self.state_path / f"state_{session_id}.json"
        history_file = self.history_path / f"history_{session_id}.json"

        if not state_file.exists():
            return False

        try:
            # Load state
            with open(state_file, encoding="utf-8") as f:
                state_data = json.load(f)

            # Extract tool history
            tool_history = state_data.pop("tool_history", [])
            self._tool_history = [ToolExecution(**t) for t in tool_history]

            # One-shot migration: fold any legacy pipeline-specific fields from
            # an old state file into the catalog cache, then drop them.
            self._migrate_legacy_state(state_data)

            # Create state object, tolerating unknown/removed legacy keys.
            known = AgentState.__dataclass_fields__.keys()
            self._state = AgentState(**{k: v for k, v in state_data.items() if k in known})
            self._session_id = session_id

            # Load history if exists
            if history_file.exists():
                with open(history_file, encoding="utf-8") as f:
                    history_data = json.load(f)
                self._messages = [Message(**m) for m in history_data]

            return True

        except Exception:
            log.warning("error loading session %s", session_id, exc_info=True)
            return False

    def _migrate_legacy_state(self, state_data: dict[str, Any]) -> None:
        """Migrate Snowflake-specific legacy fields into catalog entries."""
        connector = "snowflake"
        for schema_name in state_data.get("schemas_created", []) or []:
            self.add_created_schema(schema_name, connector=connector)
        for ff in state_data.get("file_formats_created", []) or []:
            self.add_created_file_format(ff, connector=connector)
        for table in state_data.get("created_tables", []) or []:
            self.add_created_table(table, connector=connector)
        for table, row_count in (state_data.get("loaded_tables", {}) or {}).items():
            self.add_loaded_table(table, row_count=row_count, connector=connector)

    def get_progress_summary(self) -> dict[str, Any]:
        # Get a summary of current progress (counts derived from the catalog).
        tables = self.catalog.list_objects(object_type="table")
        loaded = [t for t in tables if t.row_count_estimate is not None]
        todos = list(self.state.todos)
        active = next((t.get("content") for t in todos if t.get("status") == "in_progress"), None)
        return {
            "session_id": self.session_id,
            "todos": todos,
            "active_task": active,
            "tables_created": len(tables),
            "tables_loaded": len(loaded),
            "total_rows_loaded": sum(t.row_count_estimate or 0 for t in loaded),
            "schemas_created": len(self.catalog.list_objects(object_type="schema")),
            "file_formats_created": len(self.catalog.list_objects(object_type="file_format")),
            "files_discovered": sum(len(f) for f in self.state.discovered_files.values()),
            "errors_count": self.state.errors_count,
            "last_error": self.state.last_error,
        }

    def export_state(self) -> str:
        # Export current state as JSON string (session metadata + catalog snapshot)
        payload = asdict(self.state)
        payload["catalog"] = [e.to_record() for e in self.catalog.list_objects()]
        return json.dumps(payload, indent=2, default=str)
