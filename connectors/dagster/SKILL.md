---
id: dagster
name: Dagster
description: Launch and inspect Dagster runs and asset materializations via GraphQL.
---

# Dagster connector

Operate and debug Dagster pipelines through the GraphQL API.

## Operations
- `list_dagster_assets` *(safe; introspection)*.
- `get_dagster_run` *(safe)* — run status.
- `get_dagster_asset_materialization` *(safe)* — latest materialization /
  freshness.
- `launch_dagster_run` *(risky)* — launch a job + wait for a terminal status.

## Governance
- **Post-conditions:** `dagster_run_succeeded` (the run reached `SUCCESS`, polled
  to terminal), `asset_materialized` (materialization + freshness reported).
- **Rollback:** launching a run has external side effects with **no native undo**
  — terminate the run to stop further steps.
- **Scope:** ships `read_only`; grant wider per deployment.

## API path
`POST /graphql` (Dagster Cloud token header when applicable).

## Golden task
Launch a job run, poll to a terminal status, and confirm it reached `SUCCESS`.
