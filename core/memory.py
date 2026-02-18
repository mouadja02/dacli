"""
Light memory and state management for dacli

Provides persistent storage for:
- Conversation history
- Agent state and progress
- Tool execution history
- Session management
"""

import json

from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from enum import Enum


class PhaseStatus(Enum):
    # Status of a phase in the workflow.
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class ToolExecution:
    # Record of a tool execution
    tool_name: str
    timestamp: str
    status: str
    input_params: Dict[str, Any]
    result: Optional[Any] = None
    error: Optional[str] = None
    execution_time_ms: float = 0.0


@dataclass
class PhaseProgress:
    phase_name: str
    status: str = PhaseStatus.NOT_STARTED.value
    total_steps: int = 0
    current_step: int = 0
    steps_completed: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


@dataclass
class Message:
    # A message in the conversation
    role: str  # "user", "assistant", "system", "tool"
    content: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentState:
    # Describe the agent state

    # Identification info
    session_id: str
    created_at: str
    updated_at: str

    # Work progress
    current_phase: str = "Initialization"
    phases: Dict[str, PhaseStatus] = field(default_factory=dict)

    # Discovered entities
    discovered_files: Dict[str, Any] = field(default_factory=dict)
    inferred_schemas: Dict[str, Any] = field(default_factory=dict)
    created_tables: List[str] = field(default_factory=list)
    loaded_tables: Dict[str, int] = field(default_factory=dict)  # table -> row count

    # Configuration state
    infrastructure_ready: bool = False
    schemas_created: List[str] = field(default_factory=list)
    file_formats_created: List[str] = field(default_factory=list)

    # Error tracking
    last_error: Optional[str] = None
    errors_count: int = 0

    # dbt integration
    dbt_sources_registered: List[str] = field(default_factory=list)
    dbt_models_created: List[str] = field(default_factory=list)


class AgentMemory:
    """
    Persistent memory and state management for the agent.

    Features:
    - Conversation history with windowed context
    - Persistent state across sessions
    - Tool execution logging
    - Progress tracking through phases
    """

    def __init__(
        self,
        state_path: str = ".dacli/state/",
        history_path: str = ".dacli/history/",
        memory_window: int = 25,
    ):
        """
        Initialize agent memory.

        Args:
            state_path: Directory for state files
            history_path: Directory for conversation history
            memory_window: Number of messages to keep in context
        """
        self.state_path = Path(state_path)
        self.history_path = Path(history_path)
        self.memory_window = memory_window

        # Ensure directories exist
        self.state_path.mkdir(parents=True, exist_ok=True)
        self.history_path.mkdir(parents=True, exist_ok=True)

        # Current session data
        self._session_id: Optional[str] = None
        self._messages: List[Message] = []
        self._tool_history: List[ToolExecution] = []
        self._state: Optional[AgentState] = None

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
        now = datetime.now().isoformat()
        return AgentState(
            session_id=self.session_id,
            created_at=now,
            updated_at=now,
            phases={
                "phase_0_infrastructure": asdict(
                    PhaseProgress(phase_name="Infrastructure Setup", total_steps=9)
                ),
                "phase_1_discovery": asdict(
                    PhaseProgress(
                        phase_name="File Discovery",
                        total_steps=0,  # Dynamic based on files
                    )
                ),
                "phase_2_tables": asdict(
                    PhaseProgress(phase_name="Create Tables", total_steps=0)
                ),
                "phase_3_load": asdict(
                    PhaseProgress(phase_name="Load Data", total_steps=0)
                ),
                "phase_4_validate": asdict(
                    PhaseProgress(phase_name="Validate Data", total_steps=0)
                ),
            },
        )

    # ========================
    # Message Management
    # ========================
    def add_message(
        self, role: str, content: str, metadata: Optional[Dict[str, Any]] = None
    ):
        # Add a message to the conversation history
        message = Message(
            role=role,
            content=content,
            timestamp=datetime.now().isoformat(),
            metadata=metadata or {},
        )
        self._messages.append(message)
        self._save_history()

    def add_user_message(self, content: str) -> None:
        # Add a user message
        self.add_message(role="user", content=content)

    def add_assistant_message(self, content: str) -> None:
        # Add an assistant message
        self.add_message(role="assistant", content=content)

    def add_tool_result(
        self, tool_name: str, result: Any, error: Optional[str] = None
    ) -> None:
        # Add a tool result message
        self.add_message(
            role="tool",
            content=str(result),
            metadata={"tool_name": tool_name, "error": error},
        )

    def get_context_messages(self) -> List[Dict[str, str]]:
        # Get messages for LLM context within the memory window
        windowed = self._messages[-self.memory_window :]
        return [{"role": m.role, "content": m.content} for m in windowed]

    def get_full_history(self) -> List[Message]:
        # Get all messages in the conversation
        return self._messages.copy()

    def clear_messages(self) -> None:
        # Clear all messages (new conversation)
        self._messages = []

    # ========================
    # Tool Execution Tracking
    # ========================

    def log_tool_execution(
        self,
        tool_name: str,
        input_params: Dict[str, Any],
        result: Optional[Any] = None,
        error: Optional[str] = None,
        execution_time_ms: float = 0.0,
    ) -> None:
        # Log tool execution
        tool_execution = ToolExecution(
            tool_name=tool_name,
            timestamp=datetime.now().isoformat(),
            status="success" if error is None else "error",
            input_params=input_params,
            result=result,
            error=error,
            execution_time_ms=execution_time_ms,
        )
        self._tool_history.append(tool_execution)
        self._save_state()

    def get_tool_history(self, tool_name: Optional[str] = None) -> List[ToolExecution]:
        # Get tool execution history
        if tool_name:
            return [t for t in self._tool_history if t.tool_name == tool_name]
        return self._tool_history.copy()

    # ========================
    # State Management
    # ========================

    def update_phase(
        self,
        phase_key: str,
        status: Optional[PhaseStatus] = None,
        current_step: Optional[int] = None,
        step_completed: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        # Update phase progress
        if phase_key not in self.state.phases:
            self.state.phases[phase_key] = asdict(PhaseProgress(phase_name=phase_key))

        phase = self.state.phases[phase_key]

        if status:
            phase["status"] = status.value
            if status == PhaseStatus.IN_PROGRESS and not phase.get("started_at"):
                phase["started_at"] = datetime.now().isoformat()
            elif status == PhaseStatus.COMPLETED:
                phase["completed_at"] = datetime.now().isoformat()

        if current_step is not None:
            phase["current_step"] = current_step

        if step_completed:
            if step_completed not in phase.get("steps_completed", []):
                phase.setdefault("steps_completed", []).append(step_completed)

        if error:
            phase.setdefault("errors", []).append(error)
            self.state.last_error = error
            self.state.errors_count += 1

        self.state.updated_at = datetime.now().isoformat()
        self._save_state()

    def set_current_phase(self, phase: str) -> None:
        # Set current phase
        self.state.current_phase = phase
        self._save_state()

    def add_discovered_file(self, source: str, filename: str) -> None:
        # Record a discovered file
        self.state.discovered_files.setdefault(source, [])
        if filename not in self.state.discovered_files[source]:
            self.state.discovered_files[source].append(filename)
        self._save_state()

    def add_inferred_schema(self, filename: str, schema: List[Dict]) -> None:
        # Record an inferred schema
        self.state.inferred_schemas[filename] = schema
        self._save_state()

    def add_created_table(self, table_name: str) -> None:
        # Record a created table
        if table_name not in self.state.created_tables:
            self.state.created_tables.append(table_name)
        self._save_state()

    def add_loaded_table(self, table_name: str) -> None:
        # Record a loaded table
        self.state.loaded_tables.append(table_name)
        self._save_state()

    def set_infrastructure_ready(self) -> None:
        # Mark infrastructure a sready
        self.state.infrastructure_ready = True
        self._save_state()

    def add_created_schema(self, schema_name: str) -> None:
        # Record a created schema
        if schema_name not in self.state.schemas_created:
            self.state.schemas_created.append(schema_name)
        self._save_state()

    def add_created_file_format(self, file_format: str) -> None:
        # Record a created file format
        if file_format not in self.state.file_formats_created:
            self.state.file_formats_created.append(file_format)
        self._save_state()

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

        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state_data, f, indent=2, default=str)

    def _save_history(self) -> None:
        # Save the conversation history to a file
        history_file = self._get_history_file()
        history_data = [asdict(m) for m in self._messages]

        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(history_data, f, indent=2)

    def list_sessions(self) -> List[Dict[str, Any]]:
        """List all available sessions."""
        sessions = []

        for state_file in self.state_path.glob("state_*.json"):
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                sessions.append(
                    {
                        "session_id": data.get("session_id"),
                        "created_at": data.get("created_at"),
                        "updated_at": data.get("updated_at"),
                        "current_phase": data.get("current_phase"),
                        "errors_count": data.get("errors_count", 0),
                        "tables_created": len(data.get("created_tables", [])),
                    }
                )
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
            with open(state_file, "r", encoding="utf-8") as f:
                state_data = json.load(f)

            # Extract tool history
            tool_history = state_data.pop("tool_history", [])
            self._tool_history = [ToolExecution(**t) for t in tool_history]

            # Create state object
            self._state = AgentState(**state_data)
            self._session_id = session_id

            # Load history if exists
            if history_file.exists():
                with open(history_file, "r", encoding="utf-8") as f:
                    history_data = json.load(f)
                self._messages = [Message(**m) for m in history_data]

            return True

        except Exception as e:
            print(f"Error loading session: {e}")
            return False

    def get_progress_summary(self) -> Dict[str, Any]:
        # Get a summary of current progress
        return {
            "session_id": self.session_id,
            "current_phase": self.state.current_phase,
            "infrastructure_ready": self.state.infrastructure_ready,
            "tables_created": len(self.state.created_tables),
            "tables_loaded": len(self.state.loaded_tables),
            "total_rows_loaded": sum(self.state.loaded_tables.values()),
            "schemas_created": len(self.state.schemas_created),
            "file_formats_created": len(self.state.file_formats_created),
            "files_discovered": sum(
                len(f) for f in self.state.discovered_files.values()
            ),
            "errors_count": self.state.errors_count,
            "last_error": self.state.last_error,
            "phases": {
                k: {
                    "status": v.get("status"),
                    "progress": f"{v.get('current_step', 0)} / {v.get('total_steps', 0)}",
                }
                for k, v in self.state.phases.items()
            },
        }

    def export_state(self) -> str:
        # Export current state as JSON string
        return json.dumps(asdict(self.state), indent=2, default=str)
