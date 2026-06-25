"""Render an ER diagram / lineage / pipeline DAG as Mermaid text from the catalog.

Pure-text output → trivially reliable, instantly valuable to data architects, and
it exercises the full skill contract. Its post-conditions are the point:

* ``mermaid_parses`` — the rendered text is a structurally valid Mermaid diagram.
* ``entities_exist_in_catalog`` — *every entity the diagram references exists in
  the live catalog* (the environment is the oracle). A diagram that mentions a
  non-existent table is rejected, not shipped.
"""

from __future__ import annotations

import re
from typing import Any

from dacli.connectors.base import ToolResult, ToolStatus
from dacli.core.verify import PostCondition, VerificationContext
from dacli.skills.spec import Skill, SkillSpec, SkillContext


_MERMAID_HEADERS = (
    "erDiagram", "flowchart", "graph", "sequenceDiagram", "classDiagram", "stateDiagram",
)


def _canon(name: Any) -> str:
    return str(name or "").strip().strip('"').upper()


# ---------------------------------------------------------------------------
# Post-conditions
# ---------------------------------------------------------------------------
def mermaid_parses() -> PostCondition:
    """The output is structurally valid Mermaid (header present, braces balanced)."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        text = data.get("mermaid")
        if not isinstance(text, str) or not text.strip():
            return False, "no mermaid text produced"
        first = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
        if not first.startswith(_MERMAID_HEADERS):
            return False, f"unrecognized diagram header: {first!r}"
        if text.count("{") != text.count("}"):
            return False, "unbalanced '{{}}' blocks in diagram"
        return True, ""
    return PostCondition(
        "mermaid_parses", check, "rendered text is valid Mermaid", anchored=True,
    )


def entities_exist_in_catalog() -> PostCondition:
    """Every referenced entity must exist as a table in the live catalog."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        entities = [_canon(e) for e in (data.get("entities") or [])]
        if not entities:
            return True, "no entities referenced"
        known = _known_tables(ctx.memory)
        if known is None:
            return True, "no catalog available (unverified)"
        missing = [e for e in entities if e not in known]
        if missing:
            return False, f"diagram references entities not in catalog: {missing}"
        return True, ""
    return PostCondition(
        "entities_exist_in_catalog", check,
        "every referenced entity exists in the live catalog", anchored=True,
    )


def _known_tables(memory: Any):
    catalog = getattr(memory, "catalog", None)
    if catalog is None or not hasattr(catalog, "list_objects"):
        return None
    known = set()
    for entry in catalog.list_objects(object_type="table"):
        scope = getattr(entry, "scope", {}) or {}
        known.add(_canon(scope.get("object")))
    return known


# ---------------------------------------------------------------------------
# Skill
# ---------------------------------------------------------------------------
class MermaidSkill(Skill):
    spec = SkillSpec(
        name="diagram-mermaid",
        description=(
            "Render an ER diagram, lineage, or pipeline DAG as Mermaid text from "
            "the live catalog. Output is pure text — paste it into any Mermaid "
            "renderer. Only entities present in the catalog are drawn."
        ),
        version="1.0.0",
        can_do=[
            "render an ER diagram of catalog tables and their columns",
            "render a schema→table flow/DAG",
            "produce Mermaid text from already-introspected structure",
        ],
        cannot_do=[
            "query or mutate any live platform",
            "invent tables or columns not present in the catalog",
        ],
        input_schema={
            "type": "object",
            "properties": {
                "diagram_type": {
                    "type": "string",
                    "enum": ["er", "flow"],
                    "description": "ER entity diagram (default) or schema→table flow DAG.",
                },
                "connector": {"type": "string", "description": "Filter to one connector id."},
                "schema": {"type": "string", "description": "Filter to one schema."},
            },
            "required": [],
        },
        output_schema={
            "type": "object",
            "properties": {
                "mermaid": {"type": "string"},
                "entities": {"type": "array", "items": {"type": "string"}},
                "diagram_type": {"type": "string"},
            },
            "required": ["mermaid", "entities"],
        },
        postconditions=[mermaid_parses(), entities_exist_in_catalog()],
        min_confidence=0.75,
        tier="tool",
        category="diagram",
    )

    async def execute(self, args: dict[str, Any], context: SkillContext) -> ToolResult:
        args = dict(args or {})
        diagram_type = (args.get("diagram_type") or "er").lower()
        connector = args.get("connector")
        schema = args.get("schema")

        tables = self._tables(context.memory, connector=connector, schema=schema)
        if not tables:
            return ToolResult(
                tool_name=self.spec.name,
                status=ToolStatus.SUCCESS,
                data={"mermaid": "erDiagram\n", "entities": [], "diagram_type": diagram_type},
                metadata={"note": "no tables in catalog for the requested scope"},
            )

        if diagram_type == "flow":
            text, entities = self._render_flow(tables)
        else:
            text, entities = self._render_er(tables)

        return ToolResult(
            tool_name=self.spec.name,
            status=ToolStatus.SUCCESS,
            data={"mermaid": text, "entities": entities, "diagram_type": diagram_type},
            metadata={"entity_count": len(entities)},
        )

    # ------------------------------------------------------------------
    def _tables(self, memory: Any, *, connector=None, schema=None) -> list[Any]:
        catalog = getattr(memory, "catalog", None)
        if catalog is None or not hasattr(catalog, "list_objects"):
            return []
        out = []
        for entry in catalog.list_objects(connector=connector, object_type="table"):
            scope = getattr(entry, "scope", {}) or {}
            if schema and _canon(scope.get("schema")) != _canon(schema):
                continue
            out.append(entry)
        return out

    def _entity_name(self, entry: Any) -> str:
        scope = getattr(entry, "scope", {}) or {}
        return _canon(scope.get("object")) or "UNKNOWN"

    def _render_er(self, tables: list[Any]) -> tuple[str, list[str]]:
        lines = ["erDiagram"]
        entities: list[str] = []
        for entry in tables:
            name = self._entity_name(entry)
            entities.append(name)
            lines.append(f"    {name} {{")
            cols = getattr(entry, "columns", None) or []
            if cols:
                for col in cols:
                    ctype = re.sub(r"\s+", "_", str(col.get("type") or "STRING"))
                    cname = re.sub(r"\W+", "_", str(col.get("name") or "col"))
                    lines.append(f"        {ctype} {cname}")
            else:
                lines.append("        STRING _placeholder")
            lines.append("    }")
        return "\n".join(lines) + "\n", entities

    def _render_flow(self, tables: list[Any]) -> tuple[str, list[str]]:
        lines = ["flowchart TD"]
        entities: list[str] = []
        for entry in tables:
            scope = getattr(entry, "scope", {}) or {}
            name = self._entity_name(entry)
            entities.append(name)
            schema = _canon(scope.get("schema")) or "SCHEMA"
            lines.append(f"    {schema}[{schema}] --> {name}[{name}]")
        return "\n".join(lines) + "\n", entities
