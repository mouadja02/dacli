---
name: quality-assert
description: Assert a metric condition (a column's null rate, a table's row count) and act on a breach through the governed path.
---

# Data-quality assertion

Assert a condition on a data object and act on a breach. The check is read-only;
remediation is a normal governed action — propose by default, execute only with
approval. `dacli assert define|run` drives this from the CLI; the same method
applies in a turn.

## Metrics

- `null_rate` — fraction of NULLs in a column, measured exactly with
  `SELECT COUNT(*), COUNT(column) FROM table` (not a sample).
- `row_count` — `SELECT COUNT(*) FROM table`.

Both run through a governed query tool — classified (SELECT only) and audited.

## Predicate

`op` + `threshold` express the breach: `null_rate > 0.01` is breached when the
live null rate exceeds 1%.

## Remediation

On breach, propose a fix (e.g. `dbt run` on the model that populates the table).
Dispatch it only with approval, through the same governed path — classification,
verified rollback, dry-run, human confirm. Without an approver the fix is
refused: report the breach, mutate nothing.
