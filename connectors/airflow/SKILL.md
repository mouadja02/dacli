---
name: airflow
description: Operate and debug Airflow DAGs — trigger, monitor, pause, inspect runs.
---

# Airflow connector

Turns dacli into a pipeline *operator*, not just a builder. Talks to the stable
Airflow REST API.

## Operations
- `list_airflow_dags` *(safe; introspection)*.
- `get_airflow_dag_run`, `get_airflow_task_instances` *(safe)* — debug a run.
- `trigger_airflow_dag` *(risky)* — trigger + wait for a terminal state.
- `pause_airflow_dag` *(risky; reversible via unpause)*.
- `delete_airflow_dag` *(irreversible; gated hard)*.

## Governance
- **Post-conditions:** `airflow_run_succeeded` (the run reached `success`, polled
  to terminal), `airflow_dag_paused` (re-read confirms paused),
  `airflow_dag_absent` (re-read confirms deletion).
- **Rollback:** pause → **unpause** (`airflow_unpause`, verified); trigger and
  delete have **no native undo** — delete is gated hard (re-deploy the DAG file
  from version control restores only the definition, not run history).
- **Scope:** ships `read_only`; grant wider per deployment.

## API path
`GET/POST/PATCH/DELETE /api/v1/dags…` (basic auth or bearer token).

## Golden task
Trigger a DAG run, poll to a terminal state, and confirm it reached `success`.
