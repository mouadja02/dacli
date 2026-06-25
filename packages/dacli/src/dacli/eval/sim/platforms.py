"""Per-platform simulated responders + settings stubs.

Each factory returns a pure ``responder(argv) -> CliResult`` that mimics exactly
the shape the real platform CLI emits for the relevant operations, so the
connector's *real* environment-anchored post-conditions run unchanged against the
fake. These mirror the canned outputs already proven in the connector golden tests.
"""

from __future__ import annotations

import json
import types
from typing import Any

from dacli.connectors.cli_base import CliResult


# ---------------------------------------------------------------------------
# settings stubs (manifest-config pattern, 09/A-4: connectors read their config
# via ``ConnectorConfig(settings, "<id>")`` from ``connector_config.<id>``).
# ---------------------------------------------------------------------------
def sim_settings(connector_id: str) -> Any:
    cfgs = {
        "s3": {"bucket": "b", "prefix": "", "region": "", "profile": "",
               "aws_binary": "aws", "timeout": 300},
        "gcs": {"bucket": "b", "prefix": "", "project": "p",
                "credentials_path": "", "gcloud_binary": "gcloud", "timeout": 300},
        "bigquery": {"project": "proj", "dataset": "ds", "location": "US",
                     "bq_binary": "bq", "timeout": 300},
        "databricks": {"host": "h", "token": "t", "warehouse_id": "w",
                       "catalog": "main", "schema": "default",
                       "databricks_binary": "databricks", "timeout": 300},
    }
    return types.SimpleNamespace(connector_config={connector_id: cfgs[connector_id]})


# Back-compat alias used by the package __init__.
SIM_SETTINGS = sim_settings


# ---------------------------------------------------------------------------
# S3 — a live head-object after each mutation is the oracle
# ---------------------------------------------------------------------------
def s3_responder(*, head_exists: bool = True, versioned: bool = False,
                 mutation_rc: int = 0):
    def respond(argv: list[str]) -> CliResult:
        joined = " ".join(argv)
        if "head-object" in argv:
            return (CliResult(0, "", "", argv) if head_exists
                    else CliResult(255, "", "Not Found", argv))
        if "versioning" in joined:
            body = {"Status": "Enabled"} if versioned else {}
            return CliResult(0, json.dumps(body), "", argv)
        # the mutation itself (cp / rm / put-object / delete-object)
        return CliResult(mutation_rc, "", "" if mutation_rc == 0 else "mutation failed", argv)
    return respond


# ---------------------------------------------------------------------------
# GCS — a live ls after each mutation is the oracle
# ---------------------------------------------------------------------------
def gcs_responder(*, ls_exists: bool = True, versioned: bool = False,
                  mutation_rc: int = 0):
    def respond(argv: list[str]) -> CliResult:
        joined = " ".join(argv)
        if "ls" in argv:
            if ls_exists:
                return CliResult(0, json.dumps([{"url": "gs://b/k", "size": 2}]), "", argv)
            return CliResult(1, "[]", "", argv)
        if "versioning" in joined or "describe" in joined:
            return CliResult(0, json.dumps({"versioning": {"enabled": versioned}}), "", argv)
        return CliResult(mutation_rc, "", "" if mutation_rc == 0 else "mutation failed", argv)
    return respond


# ---------------------------------------------------------------------------
# BigQuery — ``bq show`` is the oracle for a CREATE
# ---------------------------------------------------------------------------
def bigquery_responder(*, object_exists: bool = True,
                       rows: list[dict] | None = None):
    def respond(argv: list[str]) -> CliResult:
        if "show" in argv:
            if object_exists:
                return CliResult(0, json.dumps(
                    {"schema": {"fields": [{"name": "ID", "type": "INT64"}]}}), "", argv)
            return CliResult(1, "", "Not found: Table ds:customers", argv)
        return CliResult(0, json.dumps(rows if rows is not None else []), "", argv)
    return respond


# ---------------------------------------------------------------------------
# Databricks — the statement STATE is the oracle
# ---------------------------------------------------------------------------
def databricks_responder(*, state: str = "SUCCEEDED"):
    def respond(argv: list[str]) -> CliResult:
        payload: dict = {"status": {"state": state}}
        if state == "SUCCEEDED":
            payload["manifest"] = {"schema": {"columns": [{"name": "c"}]}}
            payload["result"] = {"data_array": [["1"]]}
        return CliResult(0, json.dumps(payload), "", argv)
    return respond
