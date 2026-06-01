---
id: mysql
name: MySQL
description: Run SQL on MySQL with transactional DML rollback and mysqldump snapshots.
---

# MySQL connector

A source/operational relational store on the SQL connector contract. CLI-first
via `mysql`.

## Operations
- `execute_mysql_query` *(risky; refined by SQL verb)*.
- `explain_mysql_query` *(safe)* — `EXPLAIN FORMAT=JSON` plan preview.
- `introspect_mysql_table` *(safe; introspection)* — live columns from
  `information_schema`.

## Governance
- **Post-conditions:** `mysql_ddl_object_exists` (a CREATE is confirmed in
  `information_schema`), `introspect_reports_structure`.
- **Rollback:** **transactional DML** on InnoDB (`BEGIN … ROLLBACK`); but MySQL
  **DDL auto-commits and is not transactional**, so DROP/TRUNCATE relies on a
  `mysqldump` snapshot. `verify_rollback` confirms the table exists before an
  irreversible op so a dump/restore point is real.
- **Scope:** ships `read_only`; grant wider per deployment.

## CLI path
`mysql -h … -u … -D … --batch -e '<sql>'`.

## Golden task
`CREATE TABLE …`, then confirm via `information_schema` that the table exists.
