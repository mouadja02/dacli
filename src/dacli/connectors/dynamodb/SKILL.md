---
name: dynamodb
description: Scan/get/put/delete DynamoDB items with point-in-time-recovery rollback.
---

# DynamoDB connector

Key-value / wide-column store — the connector that stresses the abstraction the
most, proving the contract generalizes past SQL. CLI-first via `aws dynamodb`.

## Operations
- `scan_dynamodb_table`, `get_dynamodb_item` *(safe)*.
- `put_dynamodb_item` *(write)*, `delete_dynamodb_item` *(risky)*.
- `delete_dynamodb_table` *(irreversible)*.
- `introspect_dynamodb_table` *(safe; introspection)* — key schema + attributes.

## Governance
- **Post-conditions (live read-back oracle):** `dynamo_item_present` (get-item
  confirms a put, key derived from the table's key schema), `dynamo_item_absent`
  (get-item confirms a delete), `dynamo_table_absent`,
  `introspect_reports_structure`.
- **Rollback:** **point-in-time recovery (PITR)** is the rollback primitive
  (`dynamodb_pitr`); on-demand backups are a durable fallback. `verify_rollback`
  confirms PITR is enabled (`describe-continuous-backups`) before an irreversible
  table delete.
- **Scope:** ships `read_only`; grant wider per deployment.

## CLI path
`aws dynamodb scan|get-item|put-item|delete-item|delete-table|describe-table|
describe-continuous-backups --table-name … --output json`.

## Golden task
Put an item, then confirm via `get-item` (using the table's key schema) that it
exists.
