---
id: data-diff
name: Data Diff (diff-before-promote)
description: Compare two tables (row counts, per-column null rates, a bounded sample) via the governed query op; optionally promote only after the diff and a governance gate.
class: dacli.skills.data_diff.skill.DataDiffSkill
enabled: true
category: data-quality
---

# Data Diff (diff-before-promote)

Compare two tables/objects on the **same** connector and report what changed —
then, in `promote` mode, replace the target with the candidate **only after**
the diff is computed and governance approves the (irreversible) promote.

## Method

Deliberately bounded — never pulls whole tables into memory:

1. **Row counts** — `SELECT COUNT(*)` on each side; the delta is
   `row_count_b - row_count_a`.
2. **Per-column null rates** — computed over a bounded sample
   (`SELECT * … LIMIT <sample_size>`, default 100) on each side; the per-column
   delta is `null_rate_b - null_rate_a`.
3. **Sampled value comparison** — the paired sample rows are compared
   position-wise; `rows_differing` counts mismatches. This is a heuristic
   smoke-signal (platform default order), not a full reconciliation.

Every query flows through the **governed dispatcher** (`Dispatcher.execute`),
so the diff is classified (safe — SELECT only) and audited like any other
action.

## Promote (`mode: "promote"`)

`CREATE OR REPLACE TABLE <table_b> AS SELECT * FROM <table_a>` is dispatched
through the same governed path. `REPLACE` classifies as **irreversible**, so the
full machinery applies: verified rollback path, dry-run, explicit human
approval. With no approver wired (headless deny mode) the promote is refused —
fail-closed. The diff always rides along in the result, so a promote without a
diff is structurally impossible (and post-condition enforced).

## Contract

- **Input:** `connector` (id), `table_a` (candidate), `table_b` (target),
  optional `sample_size` (default 100), `mode` (`diff` | `promote`).
- **Output:** `{ table_a, table_b, row_count_a, row_count_b, row_delta,
  columns: [{name, null_rate_a, null_rate_b, delta}], sample: {…}, method,
  mode, promoted? }`.
- **Scope:** `diff` is strictly read-only. Identifiers are validated (no
  quoting tricks / multi-statement strings).

## Post-conditions (mandatory)

1. **`diff_reports_expected_shape`** — counts are non-negative integers and the
   reported delta is arithmetically consistent.
2. **`promote_is_diff_gated`** — a result claiming `promoted` must carry the
   diff it was gated on.
