"""MySQL connector (Wave 2) — CLI-first via `mysql`.

Maps onto the SQL connector contract. Governance honesty matters here: unlike
PostgreSQL, **MySQL DDL auto-commits and is NOT transactional**, so a DROP/
TRUNCATE is irreversible unless a `mysqldump` snapshot was taken — the rollback
strategist reflects exactly that. DML on InnoDB *is* transactional.

The environment is the oracle: a CREATE is confirmed against
`information_schema`, and an irreversible DROP/TRUNCATE is allowed only once the
target table is verified to exist (so a dump/restore point is real).
"""

from __future__ import annotations

import os
import re
import time
from typing import Any

from dacli.connectors.base import OperationSpec, Risk, ToolResult
from dacli.connectors.cli_base import CliConnector
from dacli.core.verify import PostCondition, VerificationContext, result_succeeded


_IDENT = r"[`\w.]+"
_CREATE_RE = re.compile(
    r"^CREATE\s+(?:(?:TEMPORARY)\s+)?(TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"({_IDENT})",
    re.IGNORECASE,
)
_DROP_RE = re.compile(rf"^DROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?({_IDENT})", re.IGNORECASE)
_TRUNC_RE = re.compile(rf"^TRUNCATE\s+(?:TABLE\s+)?({_IDENT})", re.IGNORECASE)


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", (sql or "").strip().rstrip(";").strip())


def _split_rel(qualified: str) -> dict[str, str]:
    parts = [p.strip().strip("`") for p in qualified.split(".") if p.strip()]
    if len(parts) >= 2:
        return {"schema": parts[-2], "object": parts[-1]}
    return {"object": parts[-1] if parts else ""}


def _drop_or_truncate_target(sql: str) -> dict[str, str] | None:
    text = _norm(sql)
    for rx in (_DROP_RE, _TRUNC_RE):
        m = rx.match(text)
        if m:
            return _split_rel(m.group(1))
    return None


def mysql_ddl_object_exists() -> PostCondition:
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
        res = await target.invoke("introspect_mysql_table", rel)
        data = getattr(res, "data", None) or {}
        if not data.get("exists"):
            return False, f"table {rel} not found in information_schema after CREATE"
        return True, ""
    return PostCondition(
        "mysql_ddl_object_exists", check,
        "created table exists in information_schema", anchored=True, applies_when=applies,
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


class MySQLConnector(CliConnector):
    name = "mysql"
    binary = "mysql"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        cfg = getattr(settings, "mysql", None)
        self.binary = getattr(cfg, "mysql_binary", "mysql") or "mysql"

    def operations(self) -> list[OperationSpec]:
        sql_param = {
            "type": "object",
            "properties": {"query": {"type": "string", "description": "SQL statement (single statement)."}},
            "required": ["query"],
        }
        return [
            OperationSpec(
                name="execute_mysql_query",
                description="Execute a SQL statement on MySQL via `mysql`. The classifier refines the tier from the SQL verb.",
                parameters=sql_param,
                capability="mysql.query", risk=Risk.RISKY,
                display_name="Execute SQL", category="query",
                postconditions=[result_succeeded(), mysql_ddl_object_exists()],
            ),
            OperationSpec(
                name="explain_mysql_query",
                description="Get the query plan WITHOUT side effects (`EXPLAIN FORMAT=JSON`). Use as a pre-condition gate.",
                parameters=sql_param,
                capability="mysql.explain", risk=Risk.SAFE,
                display_name="Explain Plan", category="analysis",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="introspect_mysql_table",
                description="Read a table's live columns from information_schema. Feeds the catalog so memory can be re-verified.",
                parameters={
                    "type": "object",
                    "properties": {
                        "schema": {"type": "string"},
                        "object": {"type": "string"},
                    },
                    "required": ["object"],
                },
                capability="mysql.introspection", risk=Risk.SAFE,
                display_name="Introspect Table", category="introspection",
                postconditions=[introspect_reports_structure()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "execute_mysql_query":
            return await self._query(args.get("query", ""))
        if op == "explain_mysql_query":
            return await self._explain(args.get("query", ""))
        if op == "introspect_mysql_table":
            return await self._introspect(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    def _cfg(self):
        return getattr(self.settings, "mysql", None)

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 300) if cfg else 300

    def _conn_flags(self) -> list[str]:
        cfg = self._cfg()
        flags: list[str] = []
        if cfg:
            if getattr(cfg, "host", ""):
                flags += ["-h", str(cfg.host)]
            if getattr(cfg, "port", ""):
                flags += ["-P", str(cfg.port)]
            if getattr(cfg, "user", ""):
                flags += ["-u", str(cfg.user)]
            if getattr(cfg, "database", ""):
                flags += ["-D", str(cfg.database)]
        return flags

    def _env(self) -> dict[str, str]:
        cfg = self._cfg()
        env = dict(os.environ)
        if cfg and getattr(cfg, "password", ""):
            env["MYSQL_PWD"] = cfg.password
        return env

    async def _mysql(self, sql: str):
        # --batch gives tab-separated output with a header row.
        argv = [self.binary, *self._conn_flags(), "--batch", "-e", sql]
        return await self._run(argv, env=self._env(), timeout=self._timeout())

    @staticmethod
    def _parse_tsv(text: str) -> list[dict[str, Any]]:
        lines = [ln for ln in text.splitlines() if ln != ""]
        if not lines:
            return []
        header = lines[0].split("\t")
        return [dict(zip(header, ln.split("\t"), strict=True)) for ln in lines[1:]]

    async def _query(self, sql: str) -> ToolResult:
        started = time.time()
        res = await self._mysql(sql)
        if not res.ok:
            return self._fail("execute_mysql_query",
                              f"mysql failed (rc={res.rc}): {(res.stderr or res.stdout)[-1500:]}",
                              started, query=sql[:200])
        rows = self._parse_tsv(res.stdout)
        return self._ok("execute_mysql_query", rows or None, started,
                        query=sql[:200], rows_returned=len(rows),
                        catalog_effects=self._catalog_effects(sql))

    async def _explain(self, sql: str) -> ToolResult:
        started = time.time()
        res = await self._mysql(f"EXPLAIN FORMAT=JSON {sql}")
        if not res.ok:
            return self._fail("explain_mysql_query",
                              f"EXPLAIN failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}",
                              started, query=sql[:200])
        return self._ok("explain_mysql_query", {"plan": res.stdout.strip()}, started, query=sql[:200])

    @staticmethod
    def _escape_literal(value: str) -> str:
        # MySQL treats backslash as an escape inside string literals (unless
        # NO_BACKSLASH_ESCAPES), so quote-doubling alone is bypassable via
        # `\'` — escape backslashes first, then double the quotes.
        return value.replace("\\", "\\\\").replace("'", "''")

    async def _introspect(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        cfg = self._cfg()
        schema = self._escape_literal(
            args.get("schema") or (getattr(cfg, "database", "") if cfg else "")
        )
        obj = self._escape_literal(args.get("object") or "")
        scope = {"schema": schema, "object": obj}
        sql = (
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{obj}' "
            "ORDER BY ordinal_position"
        )
        res = await self._mysql(sql)
        if not res.ok:
            return self._fail("introspect_mysql_table",
                              f"introspection failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}",
                              started, scope=scope)
        rows = self._parse_tsv(res.stdout)
        exists = len(rows) > 0
        cols = [{"name": r.get("column_name") or r.get("COLUMN_NAME"),
                 "type": r.get("data_type") or r.get("DATA_TYPE")} for r in rows] or None
        effects = ([{"action": "create", "object_type": "table", "scope": scope,
                     "source": "mysql.information_schema", "columns": cols}] if exists else [])
        return self._ok("introspect_mysql_table",
                        {"exists": exists, "scope": scope, "columns": cols}, started,
                        scope=scope, catalog_effects=effects)

    @staticmethod
    def _catalog_effects(sql: str) -> list[dict[str, Any]]:
        text = _norm(sql)
        m = _CREATE_RE.match(text)
        if m:
            return [{"action": "create", "object_type": "table", "scope": _split_rel(m.group(2))}]
        target = _drop_or_truncate_target(sql)
        if target:
            return [{"action": "invalidate", "object_type": "table", "scope": target}]
        return []

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        primitive = getattr(plan, "primitive", "")
        if primitive == "transaction":
            return True, "InnoDB DML is transactional — BEGIN … ROLLBACK undoes this"
        if primitive == "mysqldump_snapshot":
            target = _drop_or_truncate_target(args.get("query", ""))
            if not target:
                return False, "could not parse the DROP/TRUNCATE target — not provably reversible"
            res = await self.invoke("introspect_mysql_table", target)
            data = getattr(res, "data", None) or {}
            if data.get("exists"):
                return True, (f"table {target} exists — mysqldump snapshot is possible "
                              f"(DDL is not transactional, so this is the only undo)")
            return False, f"table {target} not found — cannot snapshot a restore point"
        return False, f"no verifiable rollback path for primitive '{primitive}'"
