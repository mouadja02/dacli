---
id: postgres
name: PostgreSQL
description: Run SQL on PostgreSQL with transactional rollback and EXPLAIN previews.
---

# PostgreSQL connector

A source/operational relational store that maps directly onto the SQL connector
contract. CLI-first via `psql`.

## Operations
- `execute_postgres_query` *(risky; refined by SQL verb)*.
- `explain_postgres_query` *(safe)* — `EXPLAIN (FORMAT JSON)` plan preview /
  pre-condition gate.
- `introspect_postgres_table` *(safe; introspection)* — live columns from
  `information_schema`.

## Governance
- **Post-conditions:** `postgres_ddl_object_exists` (a CREATE is confirmed in
  `information_schema`), `introspect_reports_structure`.
- **Rollback:** **transactional** DDL/DML (`BEGIN … ROLLBACK` — a true undo) and
  `pg_dump` snapshots for DROP/TRUNCATE. `verify_rollback` confirms the relation
  exists (so a dump/restore point is real) before an irreversible op.
- **Scope:** ships `read_only`; grant wider per deployment.

## CLI path
`psql -h … -U … -d … --csv -c '<sql>'`.

## Golden task
`CREATE TABLE …`, then confirm via `information_schema` that the relation exists.
