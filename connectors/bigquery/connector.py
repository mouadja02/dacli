"""BigQuery connector (Wave 1) — CLI-first via `bq`.

Reuses the SQL connector contract (it slots straight into the Snowflake-shaped
operation set). BigQuery's governance wins are native and first-class:

* **`dry_run`** — an exact bytes/effect preview, the ideal pre-condition gate;
* **table snapshots + time travel** — the rollback primitives.

The environment is the oracle: a CREATE confirms the object via `bq show`, and an
irreversible DROP/TRUNCATE is only allowed once a recoverable table is verified.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

from core.logging_setup import get_logger

log = get_logger(__name__)

from connectors.base import OperationSpec, Risk, ToolResult
from connectors.cli_base import CliConnector
from core.verify import PostCondition, VerificationContext, result_succeeded


_CREATE_OBJ_RE = re.compile(
    r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|MATERIALIZED\s+VIEW)\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?`?([\w.\-:]+)`?",
    re.IGNORECASE,
)
_DROP_TRUNC_RE = re.compile(
    r"^(?:DROP\s+(?:TABLE|VIEW|MATERIALIZED\s+VIEW)|TRUNCATE\s+TABLE)\s+"
    r"(?:IF\s+EXISTS\s+)?`?([\w.\-:]+)`?",
    re.IGNORECASE,
)


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", (sql or "").strip().rstrip(";").strip())


def _split_ref(ref: str) -> dict[str, str | None]:
    """Split ``project:dataset.table`` / ``dataset.table`` / ``table``."""
    project = None
    rest = ref.replace("`", "")
    if ":" in rest:
        project, rest = rest.split(":", 1)
    parts = rest.split(".")
    if len(parts) == 3:
        return {"project": parts[0], "dataset": parts[1], "table": parts[2]}
    if len(parts) == 2:
        return {"project": project, "dataset": parts[0], "table": parts[1]}
    return {"project": project, "dataset": None, "table": parts[-1]}


def bigquery_ddl_object_exists() -> PostCondition:
    """After a CREATE, the object must actually exist in BigQuery (via `bq show`)."""
    def applies(ctx: VerificationContext) -> bool:
        return _CREATE_OBJ_RE.match(_norm(ctx.args.get("query", ""))) is not None

    async def check(ctx: VerificationContext):
        m = _CREATE_OBJ_RE.match(_norm(ctx.args.get("query", "")))
        if not m:
            return True, "not a CREATE statement"
        ref = _split_ref(m.group(1))
        target = ctx.target
        if target is None or not hasattr(target, "invoke"):
            return True, "no introspector available (unverified)"
        res = await target.invoke("introspect_bigquery_table", {
            "dataset": ref.get("dataset"), "table": ref.get("table"),
            "project": ref.get("project"),
        })
        data = getattr(res, "data", None) or {}
        if not data.get("exists"):
            return False, f"object {m.group(1)} not found via bq show after CREATE"
        return True, ""
    return PostCondition(
        "bigquery_ddl_object_exists", check,
        "created object exists per `bq show`", anchored=True, applies_when=applies,
    )


def introspect_reports_structure() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None)
        if not isinstance(data, dict) or "exists" not in data:
            return False, "introspection did not return {exists, ...}"
        return True, ""
    return PostCondition(
        "introspect_reports_structure", check,
        "introspection returns a definite existence verdict", anchored=True,
    )


def dry_run_reports_validity() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if "valid" not in data:
            return False, "dry-run did not report validity"
        return True, ""
    return PostCondition(
        "dry_run_reports_validity", check,
        "dry-run returns a validity verdict + cost estimate", anchored=True,
    )


class BigQueryConnector(CliConnector):
    name = "bigquery"
    binary = "bq"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        cfg = getattr(settings, "bigquery", None)
        self.binary = getattr(cfg, "bq_binary", "bq") or "bq"

    def operations(self) -> list[OperationSpec]:
        sql_param = {
            "type": "object",
            "properties": {"query": {"type": "string", "description": "Standard-SQL statement (single statement)."}},
            "required": ["query"],
        }
        return [
            OperationSpec(
                name="execute_bigquery_query",
                description="Execute a Standard-SQL statement on BigQuery via `bq query`. The classifier refines the tier from the SQL verb.",
                parameters=sql_param,
                capability="bigquery.query", risk=Risk.RISKY,
                display_name="Execute SQL Query", category="query",
                postconditions=[result_succeeded(), bigquery_ddl_object_exists()],
            ),
            OperationSpec(
                name="bigquery_dry_run",
                description="Validate a query and preview exact bytes processed WITHOUT running it (`bq query --dry_run`). Use as a pre-condition/cost gate.",
                parameters=sql_param,
                capability="bigquery.dry_run", risk=Risk.SAFE,
                display_name="Dry-run / Cost Preview", category="introspection",
                postconditions=[dry_run_reports_validity()],
            ),
            OperationSpec(
                name="introspect_bigquery_table",
                description="Read a table/view's live schema via `bq show`. Feeds the catalog so memory can be re-verified.",
                parameters={
                    "type": "object",
                    "properties": {
                        "dataset": {"type": "string"},
                        "table": {"type": "string"},
                        "project": {"type": "string"},
                    },
                    "required": ["table"],
                },
                capability="bigquery.introspection", risk=Risk.SAFE,
                display_name="Introspect Table", category="introspection",
                postconditions=[introspect_reports_structure()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "execute_bigquery_query":
            return await self._query(args.get("query", ""))
        if op == "bigquery_dry_run":
            return await self._dry_run_query(args.get("query", ""))
        if op == "introspect_bigquery_table":
            return await self._introspect(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    def _cfg(self):
        return getattr(self.settings, "bigquery", None)

    def _base_argv(self) -> list[str]:
        cfg = self._cfg()
        argv = [self.binary, "--format=json"]
        if cfg and getattr(cfg, "project", ""):
            argv += [f"--project_id={cfg.project}"]
        if cfg and getattr(cfg, "location", ""):
            argv += [f"--location={cfg.location}"]
        return argv

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 300) if cfg else 300

    async def _query(self, sql: str) -> ToolResult:
        started = time.time()
        argv = [*self._base_argv(), "query", "--use_legacy_sql=false", sql]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("execute_bigquery_query",
                              f"bq query failed (rc={res.rc}): {(res.stderr or res.stdout)[-1500:]}",
                              started, query=sql[:200])
        rows: Any
        try:
            rows = json.loads(res.stdout) if res.stdout.strip() else []
        except json.JSONDecodeError:
            rows = res.stdout
        effects = self._catalog_effects(sql)
        return self._ok("execute_bigquery_query", rows, started,
                        query=sql[:200], catalog_effects=effects,
                        rows_returned=len(rows) if isinstance(rows, list) else None)

    async def _dry_run_query(self, sql: str) -> ToolResult:
        started = time.time()
        argv = [*self._base_argv(), "query", "--use_legacy_sql=false", "--dry_run", sql]
        res = await self._run(argv, timeout=self._timeout())
        bytes_processed = None
        m = re.search(r"process(?:es)?\s+([\d,]+)\s+bytes", res.stdout, re.IGNORECASE)
        if m:
            bytes_processed = int(m.group(1).replace(",", ""))
        data = {"valid": res.ok, "bytes_processed": bytes_processed, "raw": res.stdout.strip()}
        if not res.ok:
            data["error"] = (res.stderr or res.stdout)[-1000:]
        return self._ok("bigquery_dry_run", data, started, query=sql[:200])

    async def _introspect(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        cfg = self._cfg()
        project = args.get("project") or (getattr(cfg, "project", "") if cfg else "")
        dataset = args.get("dataset") or (getattr(cfg, "dataset", "") if cfg else "")
        table = args.get("table") or ""
        ref = ".".join([p for p in (dataset, table) if p])
        if project:
            ref = f"{project}:{ref}"
        scope = {"project": project or None, "dataset": dataset or None, "object": table}
        argv = [*self._base_argv(), "show", "--schema=false", ref]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            # `bq show` returns non-zero when the object does not exist.
            return self._ok("introspect_bigquery_table",
                            {"exists": False, "scope": scope, "columns": None}, started,
                            scope=scope)
        columns = None
        try:
            meta = json.loads(res.stdout) if res.stdout.strip() else {}
            fields = (meta.get("schema", {}) or {}).get("fields", [])
            columns = [{"name": f.get("name"), "type": f.get("type")} for f in fields] or None
        except json.JSONDecodeError:
            log.debug("bq_show output was not valid JSON; columns unknown", exc_info=True)
        effects = [{"action": "create", "object_type": "table", "scope": scope,
                    "source": "bigquery.bq_show", "columns": columns}]
        return self._ok("introspect_bigquery_table",
                        {"exists": True, "scope": scope, "columns": columns}, started,
                        scope=scope, catalog_effects=effects)

    @staticmethod
    def _catalog_effects(sql: str) -> list[dict[str, Any]]:
        text = _norm(sql)
        m = _CREATE_OBJ_RE.match(text)
        if m:
            ref = _split_ref(m.group(1))
            return [{"action": "create", "object_type": "table",
                     "scope": {"dataset": ref.get("dataset"), "object": ref.get("table")}}]
        m = _DROP_TRUNC_RE.match(text)
        if m:
            ref = _split_ref(m.group(1))
            return [{"action": "invalidate", "object_type": "table",
                     "scope": {"dataset": ref.get("dataset"), "object": ref.get("table")}}]
        return []

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        primitive = getattr(plan, "primitive", "")
        if primitive == "transaction":
            return True, "DML wrapped in BEGIN TRANSACTION … ROLLBACK"
        if primitive == "bq_snapshot":
            return True, "table snapshot can be taken before the change"
        if primitive == "bq_time_travel_snapshot":
            m = _DROP_TRUNC_RE.match(_norm(args.get("query", "")))
            if not m:
                return False, "could not parse the DROP/TRUNCATE target — not provably reversible"
            ref = _split_ref(m.group(1))
            res = await self.invoke("introspect_bigquery_table", {
                "dataset": ref.get("dataset"), "table": ref.get("table"),
                "project": ref.get("project")})
            data = getattr(res, "data", None) or {}
            if data.get("exists"):
                return True, (f"table {m.group(1)} exists — recoverable via time travel "
                              f"(FOR SYSTEM_TIME AS OF) / snapshot")
            return False, f"table {m.group(1)} not found — cannot guarantee a restore point"
        return False, f"no verifiable rollback path for primitive '{primitive}'"
