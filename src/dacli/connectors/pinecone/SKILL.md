---
name: pinecone
description: Semantic search over a Pinecone vector index (read-only).
---

# Pinecone connector

Retrieval-only access to a Pinecone vector index: semantic documentation search
and index introspection. No destructive lifecycle ops are exposed — the agent's
job here is retrieval, not index management.

## Operations
- `search_snowflake_docs` *(safe)* — top-k semantic search over indexed docs.
- `describe_pinecone_index` *(safe; introspection)* — dimension, vector count,
  namespaces; re-verifies the index is reachable and shaped as expected.

## Governance
- **Post-conditions:** `returns_matches` (search returns a list),
  `reports_dimension` (introspection reports the index dimension).
- **Rollback:** not applicable — every operation is read-only, so the connector
  registers no rollback planner and is capped at `safe`.
- **Scope:** ships `read_only`.

## Golden task
Run a semantic search and confirm a list of matches is returned (the
`returns_matches` post-condition).
