---
name: data-diff
description: Compare two tables (row counts, per-column null rates, a bounded sample) before promoting one over the other.
---

# Data diff (diff before promote)

Compare two tables on the same connector and report what changed, then promote
only after the diff and a governance approval. `dacli diff <connector> <a> <b>`
runs the read-only diff from the CLI; do the same from a turn with the
connector's query tool.

## Method

Bounded — never pull whole tables into memory:

1. **Row counts** — `SELECT COUNT(*)` per side; the delta is `count_b - count_a`.
2. **Per-column null rates** — over a bounded sample (`SELECT * … LIMIT n`,
   default 100) per side; the per-column delta is `null_rate_b - null_rate_a`.
3. **Sampled value comparison** — paired sample rows compared position-wise. A
   smoke signal (platform default order), not a full reconciliation.

Every query goes through a governed query tool, so the diff is classified
(SELECT only) and audited.

## Promote

`CREATE OR REPLACE TABLE <target> AS SELECT * FROM <candidate>` classifies as
irreversible: verified rollback, dry-run, and explicit approval all apply. Run
the diff first — promoting without reviewing the diff defeats the point.

## Scope

The diff is read-only. Validate identifiers (no quotes, spaces, or statement
separators) so an interpolated name can't smuggle a second statement.
