---
id: quality-assert
name: Data-quality assertion
description: Assert a metric condition (null_rate of a column, row_count of a table) on a connector via the governed query op; on breach, propose (or with apply, run) a governed remediation.
class: dacli.skills.quality_assert.skill.QualityAssertSkill
enabled: true
category: data-quality
---

# Data-quality assertion

Assert a condition on a data object and act on a breach. The check is read-only;
the remediation is a normal governed action — proposed by default, executed only
with `apply` and only through the approval gate.

## Metrics

- `null_rate` — fraction of NULLs in `column`, measured exactly with
  `SELECT COUNT(*), COUNT(column) FROM table` (not a sample).
- `row_count` — `SELECT COUNT(*) FROM table`.

Both run through the **governed dispatcher** (`Dispatcher.execute`), so the
measurement is classified (safe — SELECT only) and audited like any read.

## Predicate

`op` + `threshold` express the *breach*: `metric=null_rate, op=">",
threshold=0.01` is breached when the live null rate exceeds 1%.

## Remediation (`apply: true`)

On breach the skill proposes a fix (by default `dbt_run` on the model that
populates the table). With `apply` the fix is dispatched through the same
governed path — classification, verified rollback, dry-run, human approval. With
no approver wired (headless deny mode) the fix is refused; the breach is reported
but nothing is mutated.

## Contract

- **Input:** `connector`, `table`, `metric` (`null_rate` | `row_count`), `op`,
  `threshold`, optional `column` (required for `null_rate`), optional `apply`.
- **Output:** `{ name, predicate, value, breached, proposed_fix? }`.
- **Scope:** the measurement is strictly read-only; identifiers are validated.

## Post-conditions (mandatory)

1. **`outcome_reports_expected_shape`** — a numeric value and a boolean breach
   verdict.
2. **`breach_not_auto_applied`** — without `apply`, a breach proposes a fix but
   never applies it (the gate is not bypassed).
