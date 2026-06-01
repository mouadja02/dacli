"""PostgreSQL connector (Wave 2) — CLI-first via `psql`.

Maps directly onto the SQL connector contract. Governance wins are first-class:

* **transactional DDL/DML** — a true `BEGIN … ROLLBACK` undo (PostgreSQL is
  fully transactional, DDL included);
* **`EXPLAIN`** — a pre-condition / plan preview before running a statement;
* **`pg_dump`** — a durable snapshot of the touched object for DROP/TRUNCATE.

The environment is the oracle: a CREATE is confirmed against
`information_schema`, and an irreversible DROP/TRUNCATE is allowed only once the
target relation is verified to exist (so a dump/restore point is real).
"""

from __future__ import annotations

import csv
import io
import os
import re
import time
from typing import Any, Dict, List, Optional

from connectors.base import OperationSpec, Risk, ToolResult, ToolStatus
from connectors.cli_base import CliConnector
from core.verify import PostCondition, VerificationContext, result_succeeded


_IDENT = r'[\w."]+'
_CREATE_RE = re.compile(
    r"^CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:(?:TEMP\w*|UNLOGGED|MATERIALIZED|GLOBAL|LOCAL)\s+)*"
    rf"(TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})",
    re.IGNORECASE,
)
_DROP_RE = re.compile(
    rf"^DROP\s+(?:TABLE|VIEW|MATERIALIZED\s+VIEW)\s+(?:IF\s+EXISTS\s+)?({_IDENT})",
    re.IGNORECASE,
)
_TRUNC_RE = re.compile(rf"^TRUNCATE\s+(?:TABLE\s+)?({_IDENT})", re.IGNORECASE)


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", (sql or "").strip().rstrip(";").strip())


def _split_rel(qualified: str) -> Dict[str, str]:
    parts = [p.strip().strip('"') for p in qualified.split(".") if p.strip()]
    if len(parts) >= 2:
        return {"schema": parts[-2], "object": parts[-1]}
    return {"schema": "public", "object": parts[-1] if parts else ""}


def _drop_or_truncate_target(sql: str) -> Optional[Dict[str, str]]:
    text = _norm(sql)
    for rx in (_DROP_RE, _TRUNC_RE):
        m = rx.match(text)
        if m:
            return _split_rel(m.group(1))
    return None


def postgres_ddl_object_exists() -> PostCondition:
    """After a CREATE, the relation must exist in information_schema."""
    def applies(ctx: VerificationContext) -> bool:
        return _CREATE_RE.match(_norm(ctx.args.get("query", ""))) is not None

    async def check(ctx: VerificationContext):
        m = _CREATE_RE.match(_norm(ctx.args.get("query", "")))
        if not m:
            return True, "not a CREATE statement"
        rel = _split_rel(m.group(2))
        target = ctx.target
        if target is None or not hasattr(target, "invoke"):
            return True, "no introspector available (unverified)"
        res = await target.invoke("introspect_postgres_table", rel)
        data = getattr(res, "data", None) or {}
        if not data.get("exists"):
            return False, f"relation {rel} not found in information_schema after CREATE"
        return True, ""
    return PostCondition(
        "postgres_ddl_object_exists", check,
        "created relation exists in information_schema", anchored=True, applies_when=applies,
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


class PostgresConnector(CliConnector):
    name = "postgres"
    binary = "psql"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        cfg = getattr(settings, "postgres", None)
        self.binary = getattr(cfg, "psql_binary", "psql") or "psql"

    def operations(self) -> List[OperationSpec]:
        sql_param = {
            "type": "object",
            "properties": {"query": {"type": "string", "description": "SQL statement (single statement)."}},
            "required": ["query"],
        }
        return [
            OperationSpec(
                name="execute_postgres_query",
                description="Execute a SQL statement on PostgreSQL via `psql`. The classifier refines the tier from the SQL verb.",
                parameters=sql_param,
                capability="postgres.query", risk=Risk.RISKY,
                display_name="Execute SQL", category="query",
                postconditions=[result_succeeded(), postgres_ddl_object_exists()],
            ),
            OperationSpec(
                name="explain_postgres_query",
                description="Get the query plan WITHOUT running side effects (`EXPLAIN (FORMAT JSON)`). Use as a pre-condition gate.",
                parameters=sql_param,
                capability="postgres.explain", risk=Risk.SAFE,
                display_name="Explain Plan", category="analysis",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="introspect_postgres_table",
                description="Read a relation's live columns from information_schema. Feeds the catalog so memory can be re-verified.",
                parameters={
                    "type": "object",
                    "properties": {
                        "schema": {"type": "string"},
                        "object": {"type": "string"},
                    },
                    "required": ["object"],
                },
                capability="postgres.introspection", risk=Risk.SAFE,
                display_name="Introspect Table", category="introspection",
                postconditions=[introspect_reports_structure()],
            ),
        ]

    async def invoke(self, op: str, args: Dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "execute_postgres_query":
            return await self._query(args.get("query", ""))
        if op == "explain_postgres_query":
            return await self._explain(args.get("query", ""))
        if op == "introspect_postgres_table":
            return await self._introspect(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    def _cfg(self):
        return getattr(self.settings, "postgres", None)

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 300) if cfg else 300

    def _conn_flags(self) -> List[str]:
        cfg = self._cfg()
        flags: List[str] = []
        if cfg:
            if getattr(cfg, "host", ""):
                flags += ["-h", str(cfg.host)]
            if getattr(cfg, "port", ""):
                flags += ["-p", str(cfg.port)]
            if getattr(cfg, "user", ""):
                flags += ["-U", str(cfg.user)]
            if getattr(cfg, "database", ""):
                flags += ["-d", str(cfg.database)]
        return flags

    def _env(self) -> Dict[str, str]:
        cfg = self._cfg()
        env = dict(os.environ)
        if cfg and getattr(cfg, "password", ""):
            env["PGPASSWORD"] = cfg.password
        if cfg and getattr(cfg, "sslmode", ""):
            env["PGSSLMODE"] = cfg.sslmode
        return env

    async def _psql(self, sql: str, *, tuples_only: bool = False):
        argv = [self.binary, *self._conn_flags(), "--csv", "-v", "ON_ERROR_STOP=1"]
        if tuples_only:
            argv += ["-t"]
        argv += ["-c", sql]
        return await self._run(argv, env=self._env(), timeout=self._timeout())

    @staticmethod
    def _parse_csv(text: str) -> List[Dict[str, Any]]:
        if not text.strip():
            return []
        reader = csv.DictReader(io.StringIO(text))
        try:
            return [dict(row) for row in reader]
        except csv.Error:
            return []

    async def _query(self, sql: str) -> ToolResult:
        started = time.time()
        res = await self._psql(sql)
        if not res.ok:
            return self._fail("execute_postgres_query",
                              f"psql failed (rc={res.rc}): {(res.stderr or res.stdout)[-1500:]}",
                              started, query=sql[:200])
        rows = self._parse_csv(res.stdout)
        return self._ok("execute_postgres_query", rows or None, started,
                        query=sql[:200], rows_returned=len(rows),
                        catalog_effects=self._catalog_effects(sql))

    async def _explain(self, sql: str) -> ToolResult:
        started = time.time()
        res = await self._psql(f"EXPLAIN (FORMAT JSON) {sql}", tuples_only=True)
        if not res.ok:
            return self._fail("explain_postgres_query",
                              f"EXPLAIN failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}",
                              started, query=sql[:200])
        return self._ok("explain_postgres_query", {"plan": res.stdout.strip()}, started, query=sql[:200])

    async def _introspect(self, args: Dict[str, Any]) -> ToolResult:
        started = time.time()
        schema = (args.get("schema") or "public").replace("'", "''")
        obj = (args.get("object") or "").replace("'", "''")
        scope = {"schema": schema, "object": obj}
        sql = (
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{obj}' "
            "ORDER BY ordinal_position"
        )
        res = await self._psql(sql)
        if not res.ok:
            return self._fail("introspect_postgres_table",
                              f"introspection failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}",
                              started, scope=scope)
        rows = self._parse_csv(res.stdout)
        exists = len(rows) > 0
        cols = [{"name": r.get("column_name"), "type": r.get("data_type")} for r in rows] or None
        effects = ([{"action": "create", "object_type": "table", "scope": scope,
                     "source": "postgres.information_schema", "columns": cols}] if exists else [])
        return self._ok("introspect_postgres_table",
                        {"exists": exists, "scope": scope, "columns": cols}, started,
                        scope=scope, catalog_effects=effects)

    @staticmethod
    def _catalog_effects(sql: str) -> List[Dict[str, Any]]:
        text = _norm(sql)
        m = _CREATE_RE.match(text)
        if m:
            return [{"action": "create", "object_type": "table",
                     "scope": _split_rel(m.group(2))}]
        target = _drop_or_truncate_target(sql)
        if target:
            return [{"action": "invalidate", "object_type": "table", "scope": target}]
        return []

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: Dict[str, Any]):
        primitive = getattr(plan, "primitive", "")
        if primitive == "transaction":
            return True, "PostgreSQL is fully transactional — BEGIN … ROLLBACK undoes this"
        if primitive == "pg_dump_snapshot":
            target = _drop_or_truncate_target(args.get("query", ""))
            if not target:
                return False, "could not parse the DROP/TRUNCATE target — not provably reversible"
            res = await self.invoke("introspect_postgres_table", target)
            data = getattr(res, "data", None) or {}
            if data.get("exists"):
                return True, f"relation {target} exists — pg_dump snapshot is possible before DROP"
            return False, f"relation {target} not found — cannot snapshot a restore point"
        return False, f"no verifiable rollback path for primitive '{primitive}'"
