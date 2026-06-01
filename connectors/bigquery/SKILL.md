---
id: bigquery
name: BigQuery
description: Query and transform on BigQuery with dry-run cost preview + snapshots.
---

# BigQuery connector

Reuses the SQL connector contract (it slots into the Snowflake-shaped op set), so
the agent reasons about BigQuery the same way it reasons about Snowflake. CLI-first
via `bq`.

## Operations
- `execute_bigquery_query` *(risky; refined by SQL verb)* — Standard-SQL.
- `bigquery_dry_run` *(safe)* — exact bytes-processed preview WITHOUT running;
  the ideal pre-condition / cost gate.
- `introspect_bigquery_table` *(safe; introspection)* — live schema via `bq show`.

## Governance
- **Post-conditions:** `bigquery_ddl_object_exists` (a CREATE is confirmed via
  `bq show`), `introspect_reports_structure`, `dry_run_reports_validity`.
- **Rollback:** transactions for DML; **table snapshots + time travel**
  (`FOR SYSTEM_TIME AS OF`) for DROP/TRUNCATE. `verify_rollback` confirms the
  target table exists (so a restore point is real) before an irreversible op.
- **Scope:** ships `read_only`; grant wider per deployment.

## CLI path
`bq --format=json query --use_legacy_sql=false [--dry_run] '<sql>'`,
`bq --format=json show <project:dataset.table>`.

## Golden task
`CREATE TABLE …`, then confirm via `bq show` that the object exists.
