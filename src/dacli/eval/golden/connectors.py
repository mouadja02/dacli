"""Per-connector golden tasks.

The Wave-1 CLI connectors (s3, gcs, bigquery, databricks) run *concretely* against
the simulator: the connector's real operation executes and its real, environment-
anchored post-conditions verify the outcome (object present after a put, ``bq
show`` confirming a CREATE, the statement STATE for Databricks). Every discovered
connector also gets a **structural** golden task that runs its Definition-of-Done
checks — a machine-verifiable guarantee that its 𝒮/𝒢 wiring (anchored post-
condition, rollback parity, introspection) exists, even for platforms we can't
fully simulate offline.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import yaml

from dacli.connectors.dod import check_connector_dod
from dacli.core.verify import VerificationContext, run_postconditions
from dacli.eval.sim.cli import SimCli
from dacli.eval.sim.platforms import (
    sim_settings, s3_responder, gcs_responder,
    bigquery_responder, databricks_responder,
)
from dacli.eval.types import GoldenTask, Stakes, TaskResult


_CONNECTORS_ROOT = Path(__file__).resolve().parents[2] / "connectors"


# ===========================================================================
# concrete, sim-backed connector tasks
# ===========================================================================
async def _run_op_with_postconditions(
    conn: Any,
    op_name: str,
    args: dict[str, Any],
    task_id: str,
    *,
    extra_postconditions: list | None = None,
) -> TaskResult:
    """Invoke an op and run its (environment-anchored) post-conditions.

    ``steps_total=2``: step 1 is the op call, step 2 the post-condition gate. The
    failing step is recorded so regression can see a task that starts failing
    *earlier* (op vs. verification).
    """
    op = next((o for o in conn.operations() if o.name == op_name), None)
    if op is None:
        return TaskResult(task_id, False, steps_total=2, failed_step=1,
                          error=f"op {op_name} not found")
    res = await conn.invoke(op_name, args)
    if not res.success:
        return TaskResult(task_id, False, steps_total=2, failed_step=1,
                          error=res.error or "op failed", detail="op did not succeed")
    pcs = list(op.postconditions or []) + list(extra_postconditions or [])
    ctx = VerificationContext(args=args, result=res, target=conn)
    report = await run_postconditions(pcs, ctx)
    return TaskResult(
        task_id, success=report.passed, steps_total=2,
        failed_step=None if report.passed else 2,
        detail=report.summary(),
    )


def _s3_put() -> TaskResult:
    from dacli.connectors.s3.connector import S3Connector

    async def run() -> TaskResult:
        conn = S3Connector(sim_settings("s3"), runner=SimCli(s3_responder(head_exists=True)))
        return await _run_op_with_postconditions(
            conn, "put_s3_object", {"key": "k", "content": "hi"}, "s3.put")
    return run


def _s3_delete() -> TaskResult:
    from dacli.connectors.s3.connector import S3Connector

    async def run() -> TaskResult:
        conn = S3Connector(sim_settings("s3"), runner=SimCli(s3_responder(head_exists=False)))
        return await _run_op_with_postconditions(
            conn, "delete_s3_object", {"key": "k"}, "s3.delete")
    return run


def _gcs_put() -> TaskResult:
    from dacli.connectors.gcs.connector import GCSConnector

    async def run() -> TaskResult:
        conn = GCSConnector(sim_settings("gcs"), runner=SimCli(gcs_responder(ls_exists=True)))
        return await _run_op_with_postconditions(
            conn, "put_gcs_object", {"key": "k", "content": "hi"}, "gcs.put")
    return run


def _gcs_delete() -> TaskResult:
    from dacli.connectors.gcs.connector import GCSConnector

    async def run() -> TaskResult:
        conn = GCSConnector(sim_settings("gcs"), runner=SimCli(gcs_responder(ls_exists=False)))
        return await _run_op_with_postconditions(
            conn, "delete_gcs_object", {"key": "k"}, "gcs.delete")
    return run


def _bigquery_create() -> TaskResult:
    from dacli.connectors.bigquery.connector import BigQueryConnector, bigquery_ddl_object_exists

    async def run() -> TaskResult:
        conn = BigQueryConnector(sim_settings("bigquery"),
                                 runner=SimCli(bigquery_responder(object_exists=True)))
        return await _run_op_with_postconditions(
            conn, "execute_bigquery_query",
            {"query": "CREATE TABLE ds.customers (ID INT64)"},
            "bigquery.create",
            extra_postconditions=[bigquery_ddl_object_exists()])
    return run


def _bigquery_select() -> TaskResult:
    from dacli.connectors.bigquery.connector import BigQueryConnector

    async def run() -> TaskResult:
        conn = BigQueryConnector(
            sim_settings("bigquery"),
            runner=SimCli(bigquery_responder(rows=[{"id": 1}, {"id": 2}])))
        return await _run_op_with_postconditions(
            conn, "execute_bigquery_query",
            {"query": "SELECT id FROM ds.t"}, "bigquery.select")
    return run


def _databricks_select() -> TaskResult:
    from dacli.connectors.databricks.connector import DatabricksConnector

    async def run() -> TaskResult:
        conn = DatabricksConnector(sim_settings("databricks"),
                                   runner=SimCli(databricks_responder()))
        return await _run_op_with_postconditions(
            conn, "execute_databricks_sql", {"query": "SELECT 1 AS c"}, "databricks.select")
    return run


# (task_id, connector_id, description, factory, stakes)
_EXECUTABLE: list[tuple[str, str, str, Any, Stakes]] = [
    ("s3.put", "s3", "put an object; head-object confirms it landed", _s3_put, Stakes.WRITE),
    ("s3.delete", "s3", "delete an object; head-object confirms it's gone", _s3_delete, Stakes.WRITE),
    ("gcs.put", "gcs", "put an object; ls confirms it landed", _gcs_put, Stakes.WRITE),
    ("gcs.delete", "gcs", "delete an object; ls confirms it's gone", _gcs_delete, Stakes.WRITE),
    ("bigquery.create", "bigquery", "CREATE TABLE; bq show confirms it exists", _bigquery_create, Stakes.WRITE),
    ("bigquery.select", "bigquery", "SELECT returns rows of the expected shape", _bigquery_select, Stakes.READ_ONLY),
    ("databricks.select", "databricks", "SELECT statement reaches SUCCEEDED state", _databricks_select, Stakes.READ_ONLY),
]


# ===========================================================================
# structural (DoD) golden tasks — coverage for every discovered connector
# ===========================================================================
def _discover() -> list[tuple[str, dict[str, Any], Path]]:
    out: list[tuple[str, dict[str, Any], Path]] = []
    for manifest_path in sorted(_CONNECTORS_ROOT.glob("*/manifest.yaml")):
        with open(manifest_path, encoding="utf-8") as f:
            manifest = yaml.safe_load(f) or {}
        cid = manifest.get("id")
        class_path = manifest.get("class")
        if cid and class_path:
            out.append((cid, manifest, manifest_path.parent))
    return out


def _spec_task(cid: str, manifest: dict[str, Any], cdir: Path) -> GoldenTask:
    import types as _types

    async def run() -> TaskResult:
        class_path = manifest["class"]
        module_path, _, class_name = class_path.rpartition(".")
        cls = getattr(importlib.import_module(module_path), class_name)
        conn = cls(_types.SimpleNamespace())
        violations = check_connector_dod(cid, manifest, conn, cdir)
        ok = not violations
        return TaskResult(
            f"{cid}.dod", success=ok, steps_total=1,
            failed_step=None if ok else 1,
            detail=("DoD satisfied" if ok else "; ".join(str(v) for v in violations)),
        )

    return GoldenTask(
        id=f"{cid}.dod", connector=cid,
        description=f"{cid}: golden-task & governance wiring meets the Definition of Done",
        run=run, stakes=Stakes.READ_ONLY, tags=["dod", "structural"],
    )


def build_connector_suite() -> list[GoldenTask]:
    tasks: list[GoldenTask] = []

    # concrete, sim-backed tasks for the connectors we can simulate offline
    for task_id, cid, desc, factory, stakes in _EXECUTABLE:
        tasks.append(GoldenTask(
            id=task_id, connector=cid, description=desc,
            run=factory(), stakes=stakes, tags=["executable", "sim"],
        ))

    # structural DoD task for every discovered connector (full coverage)
    for cid, manifest, cdir in _discover():
        tasks.append(_spec_task(cid, manifest, cdir))

    return tasks
