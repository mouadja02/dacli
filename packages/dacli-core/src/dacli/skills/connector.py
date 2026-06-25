"""SkillConnector — exposes registered skills as first-class, verified tools.

A skill is a *named, contracted procedure*; this adapter turns each enabled
skill into a connector :class:`OperationSpec` so it flows through the **one**
dispatch path — and, crucially, through the **same post-condition gate** as every
connector op (the spec carries the skill's own ``postconditions``). That is how a
skill "composes connectors and carries its own post-conditions" without any
special-casing in the kernel.
"""

from __future__ import annotations

from typing import Any
from collections.abc import Callable

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.skills.registry import SkillRegistry
from dacli.skills.spec import SkillContext


class SkillConnector(Connector):

    name = "skills"

    def __init__(
        self,
        registry: SkillRegistry,
        context_provider: Callable[[], SkillContext] | None = None,
    ):
        super().__init__(settings=None)
        self._skills = registry
        self._context_provider = context_provider or (lambda: SkillContext())
        self._is_connected = True

    def bind_context_provider(self, provider: Callable[[], SkillContext]) -> None:
        self._context_provider = provider

    def operations(self) -> list[OperationSpec]:
        specs: list[OperationSpec] = []
        for skill in self._skills.all():
            if not self._skills.is_enabled(skill.spec.name):
                continue
            specs.append(OperationSpec(
                name=skill.spec.name.replace("-", "_"),
                description=skill.spec.description,
                parameters=skill.spec.input_schema or {"type": "object", "properties": {}},
                capability=f"skill.{skill.spec.name}",
                risk=Risk.SAFE,
                display_name=skill.spec.name,
                category=skill.spec.category or "skill",
                # The skill's own post-conditions ride on the op spec, so the
                # dispatcher's verifier runs them automatically after execute().
                postconditions=list(skill.spec.postconditions),
            ))
        return specs

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        skill = self._skills.get(op)
        if skill is None:
            return ToolResult(
                tool_name=op,
                status=ToolStatus.ERROR,
                error=f"Unknown skill '{op}'.",
            )
        ctx = self._context_provider()
        result = await skill.execute(args or {}, ctx)
        if isinstance(result, ToolResult):
            return result
        # Tolerate a skill that returns a plain dict.
        return ToolResult(tool_name=op, status=ToolStatus.SUCCESS, data=result)

    async def health(self) -> ToolResult:
        return ToolResult(tool_name=self.name, status=ToolStatus.SUCCESS, data={"skills": self._skills.names()})
