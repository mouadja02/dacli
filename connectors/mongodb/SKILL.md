---
id: mongodb
name: MongoDB
description: Query and mutate MongoDB collections with sample-inferred schemas.
---

# MongoDB connector

The first NoSQL connector — proof the connector contract is not secretly
SQL-only. CLI-first via `mongosh`.

## Operations
- `find_mongodb_documents`, `count_mongodb_documents` *(safe)*.
- `insert_mongodb_documents` *(write)*, `delete_mongodb_documents`
  *(irreversible)*.
- `introspect_mongodb_collection` *(safe; introspection)* — **schema inference by
  sampling** (field → observed-type histogram) + estimated count; feeds the
  catalog, which now handles schemaless collections.

## Governance
- **Post-conditions (no SQL oracle — counts/acknowledgement/shape):**
  `mongo_insert_acknowledged` (server-acknowledged with the expected count),
  `mongo_delete_acknowledged` (acknowledged with a deletedCount),
  `introspect_reports_structure`, `returns_documents`.
- **Rollback:** MongoDB has no general native undo, so a delete is backed by a
  `mongodump` copy-aside (`mongodump_snapshot`). `verify_rollback` confirms the
  collection exists (so it is dumpable) before the mutation.
- **Scope:** ships `read_only`; grant `write`/`admin` per deployment.

## CLI path
`mongosh "<uri>" --quiet --eval "<js>"` (results serialized via `EJSON.stringify`).

## Golden task
Sample a collection and return an inferred field/type schema plus a document
count.
