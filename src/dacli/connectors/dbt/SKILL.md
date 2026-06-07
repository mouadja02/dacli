---
name: dbt
description: Transform data with dbt — compile, run, test, build, and read lineage.
---

# dbt connector

Promotes dbt to a first-class, governed connector: the single highest-leverage
addition for analytics engineers, because dbt is what makes the warehouses
*transform* rather than merely *query*. CLI-first — every op shells out to `dbt`.

## When to use
- "Build the staging models and make sure the tests pass."
- "Compile the project and show me the model lineage."
- "Run just `tag:nightly` and confirm nothing failed."

## Operations
- `dbt_compile` *(safe)* — generate `target/manifest.json`.
- `dbt_run` *(risky)* — materialize models into the warehouse.
- `dbt_test` *(safe)* — run data/schema tests.
- `dbt_build` *(risky)* — run + test in DAG order.
- `introspect_dbt_manifest` *(safe; introspection)* — model/source inventory +
  lineage; feeds the catalog.

## Governance
- **Post-conditions (environment-as-oracle, from dbt's own artifacts):**
  `dbt_nodes_succeeded` (no node failed in `run_results.json`),
  `dbt_tests_passed` (every test node passed),
  `dbt_manifest_lists_nodes` (manifest parsed with a node inventory).
- **Rollback:** transforms are git-versioned (revert the model commit) and the
  target table is snapshot/cloned before a run (`git_versioned_transform`).
- **Scope:** ships `read_only`; grant `risky` to allow `dbt run`/`build`.

## CLI path
`dbt compile|run|test|build --project-dir … --profiles-dir … --target … --select …`.

## Golden task
`dbt build`, then confirm via `run_results.json` that every node materialized and
every test passed.
