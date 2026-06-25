"""SkillSpec contract + Skill base class (per the harness-scaling ref).

Every skill declares, up front, exactly what it is for and how its output is
checked. The non-negotiable field is ``postconditions``: a skill without at least
one cannot be registered (see :class:`~skills.registry.SkillRegistry`). This is
the structural cure for *confident-but-unchecked* at the skill level.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from dacli.core.verify import PostCondition


@dataclass
class SkillSpec:
    """The contract for one skill (identity, scope, schemas, checks, routing)."""

    # Identity
    name: str
    description: str
    version: str = "1.0.0"

    # Capability scope — precise and bounded. ``cannot_do`` is enforced as a
    # post-condition where possible, catching scope creep.
    can_do: list[str] = field(default_factory=list)
    cannot_do: list[str] = field(default_factory=list)

    # Contracts (JSON-Schema; used for the tool definition and handoff checks).
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)

    # Mandatory post-condition checkers (``core.verify.PostCondition``).
    postconditions: list[PostCondition] = field(default_factory=list)

    # Routing metadata.
    min_confidence: float = 0.75
    escalation_target: str | None = None
    tier: str = "tool"          # default tier hint for the router
    category: str = ""

    def to_tool_definition(self) -> dict[str, Any]:
        """Render as an OpenAI-style function tool (so a skill is LLM-callable)."""
        params = self.input_schema or {"type": "object", "properties": {}}
        return {
            "type": "function",
            "function": {
                "name": self.name.replace("-", "_"),
                "description": self.description,
                "parameters": params,
            },
        }


@dataclass
class SkillContext:
    """Runtime collaborators a skill may use to do its work.

    A skill composes connectors and reads memory; it gets them here rather than
    importing globals, so it stays testable in isolation.
    """

    memory: Any = None
    registry: Any = None
    dispatcher: Any = None
    extra: dict[str, Any] = field(default_factory=dict)


class Skill(ABC):
    """Base class for a named, contracted procedure.

    Subclasses set :attr:`spec` (a :class:`SkillSpec`) and implement
    :meth:`execute`. The registry reads ``spec`` to enforce post-conditions and
    to surface the skill via progressive disclosure.
    """

    spec: SkillSpec

    @abstractmethod
    async def execute(self, args: dict[str, Any], context: SkillContext) -> Any:
        """Run the skill and return a ``ToolResult`` (or result-like object)."""
        raise NotImplementedError
