"""Databricks connector (Wave 1) — CLI-first via `databricks`.

SQL runs against a SQL warehouse through the Statement Execution API (driven by
the `databricks api` CLI). Governance wins are Delta-native:

* **Delta time travel** (`RESTORE TABLE … TO VERSION/TIMESTAMP AS OF`) — rollback;
* **shallow clone** — first-class shadow execution.

The platform is the oracle: success is the *statement state* (`SUCCEEDED`), not
merely that the CLI returned, and an irreversible DROP/TRUNCATE is allowed only
once the target table is confirmed to exist (so a Delta restore point is real).
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Optional

from connectors.base import OperationSpec, Risk, ToolResult
from connectors.cli_base import CliConnector
from core.verify import PostCondition, VerificationContext, result_succeeded


_DROP_TRUNC_RE = re.compile(
    r"^(?:DROP\s+TABLE|TRUNCATE\s+TABLE)\s+(?:IF\s+EXISTS\s+)?`?([\w.\-]+)`?",
    re.IGNORECASE,
)


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", (sql or "").strip().rstrip(";").strip())


def databricks_statement_succeeded() -> PostCondition:
    """The SQL warehouse reported state SUCCEEDED (deeper than the CLI's rc)."""
    def check(ctx: VerificationContext):
        meta = getattr(ctx.result, "metadata", None) or {}
        state = meta.get("statement_state")
        if state is None:
            return True, "no statement state reported (rc already gated)"
        if str(state).upper() != "SUCCEEDED":
            return False, f"statement state '{state}' (expected SUCCEEDED)"
        return True, ""
    return PostCondition(
        "databricks_statement_succeeded", check,
        "SQL warehouse reported statement state SUCCEEDED", anchored=True,
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


class DatabricksConnector(CliConnector):
    name = "databricks"
    binary = "databricks"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        cfg = getattr(settings, "databricks", None)
        self.binary = getattr(cfg, "databricks_binary", "databricks") or "databricks"

    def operations(self) -> List[OperationSpec]:
        return [
            OperationSpec(
                name="execute_databricks_sql",
                description="Execute a SQL statement on a Databricks SQL warehouse. The classifier refines the tier from the SQL verb.",
                parameters={
                    "type": "object",
                    "properties": {"query": {"type": "string", "description": "SQL statement (single statement)."}},
                    "required": ["query"],
                },
                capability="databricks.query", risk=Risk.RISKY,
                display_name="Execute SQL", category="query",
                postconditions=[result_succeeded(), databricks_statement_succeeded()],
            ),
            OperationSpec(
                name="introspect_databricks_table",
                description="Read a Unity Catalog table's live schema. Feeds the catalog so memory can be re-verified.",
                parameters={
                    "type": "object",
                    "properties": {
                        "catalog": {"type": "string"},
                        "schema": {"type": "string"},
                        "table": {"type": "string"},
                    },
                    "required": ["table"],
                },
                capability="databricks.introspection", risk=Risk.SAFE,
                display_name="Introspect Table", category="introspection",
                postconditions=[introspect_reports_structure()],
            ),
        ]

    async def invoke(self, op: str, args: Dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "execute_databricks_sql":
            return await self._query(args.get("query", ""))
        if op == "introspect_databricks_table":
            return await self._introspect(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    def _cfg(self):
        return getattr(self.settings, "databricks", None)

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 300) if cfg else 300

    async def _query(self, sql: str) -> ToolResult:
        started = time.time()
        cfg = self._cfg()
        body: Dict[str, Any] = {
            "warehouse_id": getattr(cfg, "warehouse_id", "") if cfg else "",
            "statement": sql,
            "wait_timeout": "50s",
        }
        if cfg and getattr(cfg, "catalog", ""):
            body["catalog"] = cfg.catalog
        if cfg and getattr(cfg, "db_schema", ""):
            body["schema"] = cfg.db_schema
        argv = [self.binary, "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("execute_databricks_sql",
                              f"databricks statement failed (rc={res.rc}): "
                              f"{(res.stderr or res.stdout)[-1500:]}", started, query=sql[:200])
        payload = self._parse_json(res.stdout)
        state = ((payload.get("status") or {}).get("state")) if payload else None
        rows = self._rows_from_payload(payload)
        return self._ok("execute_databricks_sql", rows, started,
                        query=sql[:200], statement_state=state,
                        catalog_effects=self._catalog_effects(sql))

    async def _introspect(self, args: Dict[str, Any]) -> ToolResult:
        started = time.time()
        cfg = self._cfg()
        catalog = args.get("catalog") or (getattr(cfg, "catalog", "") if cfg else "")
        schema = args.get("schema") or (getattr(cfg, "db_schema", "") if cfg else "")
        table = args.get("table") or ""
        full_name = ".".join([p for p in (catalog, schema, table) if p])
        scope = {"catalog": catalog or None, "schema": schema or None, "object": table}
        argv = [self.binary, "api", "get", f"/api/2.1/unity-catalog/tables/{full_name}"]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._ok("introspect_databricks_table",
                            {"exists": False, "scope": scope, "columns": None}, started, scope=scope)
        payload = self._parse_json(res.stdout) or {}
        cols = [{"name": c.get("name"), "type": c.get("type_text") or c.get("type_name")}
                for c in (payload.get("columns") or [])] or None
        effects = [{"action": "create", "object_type": "table", "scope": scope,
                    "source": "databricks.unity_catalog", "columns": cols}]
        return self._ok("introspect_databricks_table",
                        {"exists": True, "scope": scope, "columns": cols}, started,
                        scope=scope, catalog_effects=effects)

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_json(text: str) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _rows_from_payload(payload: Optional[Dict[str, Any]]) -> Any:
        if not payload:
            return None
        result = payload.get("result") or {}
        data_array = result.get("data_array")
        columns = ((payload.get("manifest") or {}).get("schema") or {}).get("columns") or []
        names = [c.get("name") for c in columns]
        if data_array is None:
            return None
        if names:
            return [dict(zip(names, row)) for row in data_array]
        return data_array

    @staticmethod
    def _catalog_effects(sql: str) -> List[Dict[str, Any]]:
        m = _DROP_TRUNC_RE.match(_norm(sql))
        if m:
            return [{"action": "invalidate", "object_type": "table",
                     "scope": {"object": m.group(1).split(".")[-1]}}]
        return []

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: Dict[str, Any]):
        primitive = getattr(plan, "primitive", "")
        if primitive == "delta_shallow_clone":
            return True, "shadow on a shallow CLONE; original untouched until promote"
        if primitive == "delta_time_travel":
            m = _DROP_TRUNC_RE.match(_norm(args.get("query", "")))
            if not m:
                return False, "could not parse the DROP/TRUNCATE target — not provably reversible"
            full = m.group(1)
            parts = full.split(".")
            ref = {"table": parts[-1]}
            if len(parts) >= 2:
                ref["schema"] = parts[-2]
            if len(parts) >= 3:
                ref["catalog"] = parts[-3]
            res = await self.invoke("introspect_databricks_table", ref)
            data = getattr(res, "data", None) or {}
            if data.get("exists"):
                return True, f"table {full} exists — recoverable via Delta RESTORE (time travel)"
            return False, f"table {full} not found — cannot guarantee a Delta restore point"
        return False, f"no verifiable rollback path for primitive '{primitive}'"
