---
name: databricks
description: Run SQL on a Databricks SQL warehouse with Delta time-travel rollback.
---

# Databricks connector

SQL against a Databricks SQL warehouse via the Statement Execution API (driven by
the `databricks` CLI). Reuses the SQL connector contract.

## Operations
- `execute_databricks_sql` *(risky; refined by SQL verb)*.
- `introspect_databricks_table` *(safe; introspection)* — Unity Catalog schema.

## Governance
- **Post-conditions:** `databricks_statement_succeeded` (the warehouse reported
  state `SUCCEEDED`, deeper than the CLI's rc), `introspect_reports_structure`.
- **Rollback:** **Delta time travel** (`RESTORE TABLE … TO VERSION/TIMESTAMP
  AS OF`) and shallow clone. `verify_rollback` confirms the target table exists
  before an irreversible DROP/TRUNCATE, so a restore point is real.
- **Scope:** ships `read_only`; grant wider per deployment.

## CLI path
`databricks api post /api/2.0/sql/statements --json {…}`,
`databricks api get /api/2.1/unity-catalog/tables/<catalog.schema.table>`.

## Golden task
Execute a statement and confirm the warehouse reported state `SUCCEEDED`.
