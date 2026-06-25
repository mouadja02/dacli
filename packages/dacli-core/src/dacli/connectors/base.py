"""Connector contract for DACLI.

This module is the evolution of the old ``tools/Base.py``. A *connector* is a
self-describing capability provider: it owns the JSON schemas for the
operations it exposes (instead of the agent hand-writing them) and routes calls
through a single ``invoke`` entry point.

It also keeps ``ToolResult`` / ``ToolStatus`` here so the rest of the runtime
imports its result type from one place.
"""

import contextlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ToolStatus(Enum):
    # Status of the tool execution. DENIED/BLOCKED are governance verdicts
    #: DENIED = a human declined; BLOCKED = policy refused before any
    # human was asked (e.g. an irreversible op with no verified rollback path).
    SUCCESS, ERROR, TIMEOUT, CANCELLED, PENDING_APPROVAL = "success", "error", "timeout", "cancelled", "pending_approval"
    DENIED, BLOCKED = "denied", "blocked"


@dataclass
class ToolResult:
    # Result of a connector operation
    tool_name: str
    status: ToolStatus
    data: Any = None
    error: str | None = None
    execution_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        # Check if the tool execution was successful
        return self.status == ToolStatus.SUCCESS

    def to_dict(self) -> dict[str, Any]:
        # Convert the result to dictionary for serialization
        return {
            "tool_name": self.tool_name,
            "status": self.status.value,
            "data": self.data,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata
        }

    def to_message(self) -> str:
        # Convert the result to a message string for LLM context
        if self.success:
            if isinstance(self.data, list):
                if len(self.data) == 0:
                    return f"[{self.tool_name}] Executed successfully. No results returned"
                return f"[{self.tool_name}] Executed successfully. Returned {len(self.data)} rows:\n{self._format_data()}"
            if self.data:
                return f"[{self.tool_name}] Executed successfully:\n{self._format_data()}"
            return f"[{self.tool_name}] Executed successfully."
        return f"[{self.tool_name}] failed with error: {self.error}"

    def _format_data(self) -> str:
        # Format data for the LLM context. Data work: send the FULL result set —
        # no row cap — so the model never has to guess or summarize ("... N more
        # rows"). The CLI renders the same data as a table for the human.
        if isinstance(self.data, list) and len(self.data) > 0 and isinstance(self.data[0], dict):
            return "\n".join(f" Row {i+1}: {row}" for i, row in enumerate(self.data))
        return str(self.data)


class Risk(str, Enum):
    """Risk hint for an operation.

    Captured at the contract level from day 1. Enforcement (approval gating)
    lands in; capturing it now avoids re-walking every operation later.
    """
    SAFE = "safe"               # read-only / no side effects
    WRITE = "write"             # creates or mutates state, recoverable
    RISKY = "risky"             # arbitrary or hard-to-predict side effects
    IRREVERSIBLE = "irreversible"  # destructive, not easily undone


@dataclass
class OperationSpec:
    """Self-describing operation a connector exposes to the LLM.

    ``name`` is the LLM-facing tool name (e.g. ``execute_snowflake_query``).
    ``parameters`` is the JSON schema for the arguments. ``risk`` and
    ``capability`` are metadata consumed by later phases (governance / routing).
    """
    name: str
    description: str
    parameters: dict[str, Any]
    capability: str
    risk: Risk = Risk.SAFE
    # Optional presentation metadata for the setup wizard. Falls back to
    # ``name`` / ``description`` when absent.
    display_name: str | None = None
    category: str | None = None
    # Mandatory post-conditions. Each is a ``core.verify.PostCondition``
    # run after the op executes; the result is rejected if any fail. Typed as
    # ``Any`` to keep this module dependency-free (no import of core.verify).
    # The connector registry enforces "at least one" when ``enforce_postconditions``
    # is on — fluent success is not proof the intended state change is correct.
    postconditions: list[Any] = field(default_factory=list)

    def to_tool_definition(self) -> dict[str, Any]:
        """Render as an OpenAI-style function tool definition."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class Connector(ABC):
    """Base class for all connectors.

    A connector is capability-scoped and self-describing. It declares its
    operations (with schemas + risk metadata) and routes calls through a single
    ``invoke`` method. Lifecycle is ``connect`` / ``disconnect``; ``health``
    replaces the old ad-hoc ``validate``.
    """

    #: Stable connector id (e.g. "snowflake"). Subclasses must set this.
    name: str = ""

    #: Optional liveness callback, set by the dispatcher around ``invoke`` when
    # the host UI wants progress (e.g. each poll of a long-running run). Never
    # set in headless/eval runs, so ``emit_progress`` is a no-op there.
    _on_progress: Any = None

    def __init__(self, settings: Any):
        self.settings = settings
        self._is_connected = False

    def emit_progress(self, message: str) -> None:
        """Report liveness from a long-running operation. No-op when unset."""
        callback = self._on_progress
        if callback is None:
            return
        # A UI hiccup must never break the operation.
        with contextlib.suppress(Exception):
            callback(message)

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    @is_connected.setter
    def is_connected(self, value: bool):
        self._is_connected = value

    @abstractmethod
    def operations(self) -> list[OperationSpec]:
        """Return the operations this connector exposes."""
        raise NotImplementedError

    @abstractmethod
    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        """Execute operation ``op`` with ``args`` and return a ToolResult."""
        raise NotImplementedError

    @abstractmethod
    async def health(self) -> ToolResult:
        """Check connectivity / readiness. Replaces ad-hoc validate()."""
        raise NotImplementedError

    async def estimate_cost(self, op: str, args: dict[str, Any]) -> dict[str, Any] | None:
        """Optional pre-execution cost preview for ``op``.

        Connectors with a native estimator (BigQuery ``dry_run``) return e.g.
        ``{"bytes": ..., "usd": ...}`` (keys: ``bytes``/``credits``/``usd``,
        all optional). ``None`` means no estimate is available. The Governor
        consults this only when ``governance.cost_confirm_usd`` is configured.
        """
        return None

    async def connect(self) -> bool:
        """Establish a connection if needed. Default: mark healthy via health()."""
        result = await self.health()
        self.is_connected = result.success
        return self.is_connected

    async def disconnect(self) -> None:
        """Clean up connection resources."""
        self.is_connected = False
