---
name: diagram-mermaid
description: Render an ER diagram, lineage, or pipeline DAG as Mermaid text from the live catalog.
---

# Diagram as code (Mermaid)

Turn the live catalog into a Mermaid diagram — an ER diagram of tables and their
columns, or a schema→table flow. Output is plain text, so it's reliable and
immediately useful.

## When

- "Draw the ER diagram for the BRONZE schema."
- "Show a diagram of the tables created so far."
- "Render the lineage from schemas to tables."

## Method

Introspect first so the catalog is fresh, then render from it. The catalog is
the source of truth: don't invent tables or columns, and don't query or mutate a
live platform to draw — every entity in the diagram must exist in the catalog.

`erDiagram` for entities and relationships; `flowchart LR` for a schema→table
flow. Keep the header and block structure valid so the text parses.
