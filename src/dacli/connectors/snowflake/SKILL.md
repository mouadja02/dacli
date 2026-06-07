---
name: snowflake
description: Execute SQL and manage a Snowflake warehouse under tiered governance.
---

# Snowflake connector

Run SQL against Snowflake (Bronze-layer DDL/DML, COPY INTO, validation queries)
and re-verify objects from `INFORMATION_SCHEMA`.

## Operations
- `execute_snowflake_query` *(risky; the classifier refines the real tier from
  the SQL verb — a SELECT is safe, a DROP is irreversible)*.
- `introspect_snowflake_object` *(safe)* — read live structure from
  `INFORMATION_SCHEMA`; feeds the catalog so memory can be re-verified.
- `validate_snowflake_connection` *(safe)*.

## Governance
- **Post-conditions:** `create_table_matches_information_schema` (column set
  equals intent), `copy_into_loaded_rows` (a 0-row load is a silent failure).
- **Rollback:** `BEGIN/ROLLBACK` for DML; **Time Travel + UNDROP** for dropped
  tables; zero-copy `CLONE` for shadow execution. `verify_rollback` reads live
  `DATA_RETENTION_TIME_IN_DAYS` before allowing a DROP/TRUNCATE.
- **Scope:** ships `read_only`; grant wider in `config/policy.yaml`.

## CLI path
Snowflake's `snowsql` CLI may be used for scripted batches; this connector uses
the Python connector for fine-grained result handling.

## Golden task
Create a table, then confirm via `INFORMATION_SCHEMA` that its live column set
matches intent (the `create_table_matches_information_schema` post-condition).
