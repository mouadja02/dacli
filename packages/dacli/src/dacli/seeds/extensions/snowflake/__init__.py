"""Snowflake seed — execute SQL, introspect objects, validate the connection.

A reference ``register(api)`` extension (reporting/02 seed set). It replaces the
old ``connectors/snowflake`` Connector: credentials come from ``api.config()``
(entered via /connect, stored encrypted), the SDK is lazy-imported on first use,
and each tool declares its risk + post-conditions so governed dispatch treats it
exactly like a connector op did.
"""

from __future__ import annotations

import re
import time
from typing import Any

from dacli.connectors.base import ToolResult, ToolStatus
from dacli.core.verify import result_succeeded


# ---------------------------------------------------------------------------
# Structured catalog-effect parsing (owned by the seed — SQL is its domain).
# Drives the result's catalog_effects so a freshly mutated object stops being
# trusted as a cached fact.
# ---------------------------------------------------------------------------
_IDENT = r'[A-Za-z0-9_$."]+'
_OBJ_TYPE = {"SCHEMA": "schema", "TABLE": "table", "VIEW": "view",
             "FILE FORMAT": "file_format", "STAGE": "stage"}
_CREATE_RE = re.compile(
    r"^CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:(?:TEMP|TEMPORARY|TRANSIENT|VOLATILE|LOCAL|GLOBAL|SECURE|MATERIALIZED)\s+)*"
    r"(SCHEMA|TABLE|VIEW|FILE\s+FORMAT|STAGE)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"({_IDENT})", re.IGNORECASE)
_DROP_RE = re.compile(
    rf"^DROP\s+(SCHEMA|TABLE|VIEW|FILE\s+FORMAT|STAGE)\s+(?:IF\s+EXISTS\s+)?({_IDENT})",
    re.IGNORECASE)
_ALTER_RE = re.compile(
    rf"^ALTER\s+(SCHEMA|TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?({_IDENT})", re.IGNORECASE)
_TABLE_WRITES = [re.compile(p, re.IGNORECASE) for p in (
    rf"^COPY\s+INTO\s+({_IDENT})", rf"^INSERT\s+(?:OVERWRITE\s+)?INTO\s+({_IDENT})",
    rf"^MERGE\s+INTO\s+({_IDENT})", rf"^UPDATE\s+({_IDENT})",
    rf"^DELETE\s+FROM\s+({_IDENT})", rf"^TRUNCATE\s+(?:TABLE\s+)?({_IDENT})")]


def _scope(qualified: str, object_type: str) -> dict[str, Any]:
    parts = [p.strip().strip('"') for p in qualified.split(".") if p.strip()]
    if object_type == "schema":
        if len(parts) >= 2:
            return {"database": parts[-2], "schema": parts[-1]}
        return {"schema": parts[-1]} if parts else {}
    if len(parts) >= 3:
        return {"database": parts[0], "schema": parts[1], "object": parts[2]}
    if len(parts) == 2:
        return {"schema": parts[0], "object": parts[1]}
    return {"object": parts[0]} if parts else {}


def parse_catalog_effects(query: str) -> list[dict[str, Any]]:
    """``create`` for DDL that makes an object, ``invalidate`` for DROP/ALTER and
    content-mutating writes; ``[]`` for a read (e.g. SELECT)."""
    if not query:
        return []
    text = re.sub(r"\s+", " ", query.strip().rstrip(";").strip())
    m = _CREATE_RE.match(text)
    if m:
        ot = _OBJ_TYPE[re.sub(r"\s+", " ", m.group(1).upper())]
        return [{"action": "create", "object_type": ot, "scope": _scope(m.group(2), ot)}]
    for rx in (_DROP_RE, _ALTER_RE):
        m = rx.match(text)
        if m:
            ot = _OBJ_TYPE[re.sub(r"\s+", " ", m.group(1).upper())]
            return [{"action": "invalidate", "object_type": ot, "scope": _scope(m.group(2), ot)}]
    for rx in _TABLE_WRITES:
        m = rx.match(text)
        if m and not m.group(1).startswith("@"):  # COPY INTO @stage is an unload
            return [{"action": "invalidate", "object_type": "table",
                     "scope": _scope(m.group(1), "table")}]
    return []


def register(api):
    cfg = api.config
    # Lazy, cached connection — keyed in the registration closure, not a module
    # global, so a reload starts clean.
    state: dict[str, Any] = {"cursor": None}

    api.config_field("account", required=True, description="Snowflake account identifier")
    api.config_field("user", required=True, description="Login user")
    api.config_field("password", secret=True, description="Login password")
    api.config_field("role", description="Role to assume")
    api.config_field("warehouse", description="Warehouse to use")
    api.config_field("database", description="Default database")
    api.config_field("schema", default="PUBLIC", description="Default schema")

    def cursor():
        if state["cursor"] is not None:
            return state["cursor"]
        try:
            import snowflake.connector
        except ImportError as e:
            raise ConnectionError(
                "The Snowflake SDK is not installed. Install it with: "
                "pip install 'dacli[snowflake]'"
            ) from e
        c = cfg()
        conn = snowflake.connector.connect(
            account=c.get("account", ""), user=c.get("user", ""),
            password=c.get("password", ""), role=c.get("role", ""),
            warehouse=c.get("warehouse", ""), database=c.get("database", ""),
            schema=c.get("schema", "PUBLIC"),
            login_timeout=c.get("login_timeout", 60),
            network_timeout=c.get("network_timeout", 60),
        )
        state["cursor"] = conn.cursor()
        return state["cursor"]

    @api.tool(
        name="execute_snowflake_query",
        description="Execute a SQL query on Snowflake. Use for schema/table/file-format creation, COPY INTO, and validation queries. Execute ONE statement at a time.",
        parameters={"query": {"type": "string", "description": "A single SQL statement."}},
        risk="risky",
        postconditions=[result_succeeded()],
        display_name="Execute SQL Query",
        category="query",
    )
    async def execute_snowflake_query(args, ctx):
        query = args.get("query", "")
        t0 = time.time()
        try:
            cur = cursor()
            cur.execute(query)
            effects = parse_catalog_effects(query)
            trimmed = query[:200].replace("\n", " ").replace("  ", " ").replace(";", "")
            if cur.description:
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                results = [dict(zip(columns, row, strict=True)) for row in rows]
                total = cur.rowcount if cur.rowcount >= 0 else len(results)
                return ctx.ok(
                    results, query=trimmed, rows_returned=len(results),
                    total_rows=total, columns=columns, catalog_effects=effects,
                )
            rows_affected = cur.rowcount if cur.rowcount >= 0 else 0
            return ctx.ok(
                None, query=trimmed, rows_affected=rows_affected, catalog_effects=effects,
            )
        except Exception as e:
            return _err(e, time.time() - t0, {"query": query})

    @api.tool(
        name="introspect_snowflake_object",
        description="Check whether a Snowflake object exists and read its live structure from INFORMATION_SCHEMA. Read-only.",
        parameters={
            "object_type": {"type": "string", "enum": ["schema", "table", "view"]},
            "database": {"type": "string"},
            "schema": {"type": "string"},
            "object": {"type": "string"},
        },
        risk="safe",
        postconditions=[result_succeeded()],
        display_name="Introspect Object",
        category="introspection",
    )
    async def introspect_snowflake_object(args, ctx):
        object_type = (args.get("object_type") or "table").lower()
        database = args.get("database") or cfg().get("database", "")
        schema = args.get("schema")
        obj = args.get("object")
        scope = {"database": database, "schema": schema, "object": obj}
        try:
            cur = cursor()
            if object_type == "schema":
                sql = (f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA "
                       f"WHERE SCHEMA_NAME = '{(schema or '').upper()}'")
            else:
                sql = (f"SELECT COLUMN_NAME, DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS "
                       f"WHERE TABLE_SCHEMA = '{(schema or '').upper()}' "
                       f"AND TABLE_NAME = '{(obj or '').upper()}' ORDER BY ORDINAL_POSITION")
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            records = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
            exists = len(records) > 0
            cols = None
            if object_type != "schema" and exists:
                cols = [{"name": r.get("COLUMN_NAME"), "type": r.get("DATA_TYPE")} for r in records]
            effects = [{"action": "create", "object_type": object_type, "scope": scope,
                        "source": "snowflake.information_schema", "columns": cols}] if exists else []
            return ctx.ok(
                {"exists": exists, "object_type": object_type, "scope": scope, "columns": cols},
                catalog_effects=effects, source="snowflake.information_schema",
            )
        except Exception as e:
            return _err(e, 0.0, {"scope": scope})

    @api.tool(
        name="validate_snowflake_connection",
        description="Test the Snowflake connection and read the current context (warehouse, database, schema, role, user).",
        parameters={},
        risk="safe",
        postconditions=[result_succeeded()],
        display_name="Validate Connection",
        category="connection",
    )
    async def validate_snowflake_connection(args, ctx):
        try:
            cur = cursor()
            cur.execute(
                "SELECT CURRENT_WAREHOUSE() AS WAREHOUSE, CURRENT_DATABASE() AS DATABASE, "
                "CURRENT_SCHEMA() AS SCHEMA, CURRENT_ROLE() AS ROLE, CURRENT_USER() AS USER;")
            columns = [d[0] for d in cur.description]
            context = dict(zip(columns, cur.fetchone(), strict=True))
            return ctx.ok(context, query="VALIDATE CONNECTION AND GET CONTEXT")
        except Exception as e:
            return _err(e, 0.0, {})


def _err(exc: Exception, elapsed_s: float, metadata: dict[str, Any]) -> ToolResult:
    return ToolResult(
        tool_name="snowflake", status=ToolStatus.ERROR, data=None, error=str(exc),
        execution_time_ms=elapsed_s * 1000, metadata=metadata,
    )
