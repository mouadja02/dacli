---
id: diagram-mermaid
name: Diagram as Code (Mermaid)
description: Render an ER diagram, lineage, or pipeline DAG as Mermaid text from the live catalog.
class: dacli.skills.diagram_mermaid.skill.MermaidSkill
enabled: true
category: diagram
---

# Diagram as Code (Mermaid)

Turn the **live catalog** into a Mermaid diagram — an ER diagram of
tables and their columns, or a schema→table flow DAG. Output is pure text, so it
is trivially reliable and instantly useful to data architects and modelers.

## When to use

- "Draw the ER diagram for the BRONZE schema."
- "Show me a diagram of the tables you've created so far."
- "Render the lineage from schemas to tables."

## Contract

- **Input:** `diagram_type` (`er` | `flow`, default `er`), optional `connector`
  and `schema` filters.
- **Output:** `{ mermaid: <text>, entities: [<TABLE>...], diagram_type }`.
- **Scope:** reads the catalog only. It will **not** query or mutate any live
  platform, and it will **not** invent tables or columns.

## Post-conditions (mandatory)

1. **`mermaid_parses`** — the rendered text is a structurally valid Mermaid
   diagram (recognized header, balanced blocks).
2. **`entities_exist_in_catalog`** — *every* entity the diagram references exists
   as a table in the live catalog. A diagram that mentions a non-existent table
   is rejected, never shipped. This is the environment acting as the oracle.

## Notes

This seeds Wave 4 of (drawio / Excalidraw). The catalog is the single
source of truth: introspect first (so the catalog is fresh), then render.
