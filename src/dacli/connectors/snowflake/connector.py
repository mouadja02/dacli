import re
import time
from typing import Any

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.config.settings import ConnectorConfig, Settings
from dacli.core.verify import PostCondition, VerificationContext, result_succeeded


# ---------------------------------------------------------------------------
# Structured catalog-effect parsing (deferred from)
# ---------------------------------------------------------------------------
# Replaces the brittle regex side-effects deleted from the dispatch path. This
# lives in the connector (SQL is its domain) and is driven by the *executed*
# statement, robust to ``CREATE OR REPLACE``, ``IF NOT EXISTS``, qualified and
# quoted identifiers, leading modifiers (TRANSIENT/TEMP/...), and multi-line SQL.
_IDENT = r'[A-Za-z0-9_$."]+'
_OBJ_TYPE_MAP = {
    "SCHEMA": "schema",
    "TABLE": "table",
    "VIEW": "view",
    "FILE FORMAT": "file_format",
    "TASK": "task",
    "STORED PROCEDURE": "stored_procedure",
    "SEQUENCE": "sequence",
    "UDF": "udf",
    "user-defined function": "udf",
    "STAGE": "stage",
    "WAREHOUSE": "warehouse",
}

_CREATE_RE = re.compile(
    r"^CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:(?:TEMP|TEMPORARY|TRANSIENT|VOLATILE|LOCAL|GLOBAL|SECURE|MATERIALIZED)\s+)*"
    r"(SCHEMA|TABLE|VIEW|FILE\s+FORMAT|STAGE)\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"({_IDENT})",
    re.IGNORECASE,
)
_DROP_RE = re.compile(
    r"^DROP\s+(SCHEMA|TABLE|VIEW|FILE\s+FORMAT|STAGE)\s+(?:IF\s+EXISTS\s+)?"
    rf"({_IDENT})",
    re.IGNORECASE,
)
_ALTER_RE = re.compile(
    r"^ALTER\s+(SCHEMA|TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?"
    rf"({_IDENT})",
    re.IGNORECASE,
)
# Statements that mutate a table's contents -> invalidate that table.
_TABLE_WRITE_RES = [
    re.compile(rf"^COPY\s+INTO\s+({_IDENT})", re.IGNORECASE),
    re.compile(rf"^INSERT\s+(?:OVERWRITE\s+)?INTO\s+({_IDENT})", re.IGNORECASE),
    re.compile(rf"^MERGE\s+INTO\s+({_IDENT})", re.IGNORECASE),
    re.compile(rf"^UPDATE\s+({_IDENT})", re.IGNORECASE),
    re.compile(rf"^DELETE\s+FROM\s+({_IDENT})", re.IGNORECASE),
    re.compile(rf"^TRUNCATE\s+(?:TABLE\s+)?({_IDENT})", re.IGNORECASE),
]


def _scope_from_name(qualified: str, object_type: str) -> dict[str, Any]:
    """Split ``DB.SCHEMA.OBJECT`` into a catalog scope dict."""
    parts = [p.strip().strip('"') for p in qualified.split(".") if p.strip()]
    if object_type == "schema":
        # A schema name is at most DB.SCHEMA.
        if len(parts) >= 2:
            return {"database": parts[-2], "schema": parts[-1]}
        return {"schema": parts[-1]} if parts else {}
    if len(parts) >= 3:
        return {"database": parts[0], "schema": parts[1], "object": parts[2]}
    if len(parts) == 2:
        return {"schema": parts[0], "object": parts[1]}
    return {"object": parts[0]} if parts else {}


def parse_catalog_effects(query: str) -> list[dict[str, Any]]:
    """Derive structured catalog effects from an executed SQL statement.

    Returns a list of ``{"action", "object_type", "scope"}`` effects:
    ``create`` for DDL that creates an object, ``invalidate`` for DROP / ALTER /
    content-mutating writes (so a freshly-mutated object is no longer trusted as
    a fact). Returns ``[]`` for read-only statements (e.g. SELECT).
    """
    if not query:
        return []
    text = re.sub(r"\s+", " ", query.strip().rstrip(";").strip())

    m = _CREATE_RE.match(text)
    if m:
        object_type = _OBJ_TYPE_MAP[re.sub(r"\s+", " ", m.group(1).upper())]
        return [{"action": "create", "object_type": object_type,
                 "scope": _scope_from_name(m.group(2), object_type)}]

    for rx in (_DROP_RE, _ALTER_RE):
        m = rx.match(text)
        if m:
            object_type = _OBJ_TYPE_MAP[re.sub(r"\s+", " ", m.group(1).upper())]
            return [{"action": "invalidate", "object_type": object_type,
                     "scope": _scope_from_name(m.group(2), object_type)}]

    for rx in _TABLE_WRITE_RES:
        m = rx.match(text)
        if m:
            target = m.group(1)
            if target.startswith("@"):
                return []  # COPY INTO @stage ... is an unload, not a table write
            return [{"action": "invalidate", "object_type": "table",
                     "scope": _scope_from_name(target, "table")}]

    return []


# ---------------------------------------------------------------------------
# — post-condition support: parse the *intended* column set
# ---------------------------------------------------------------------------
_CREATE_TABLE_HEAD = re.compile(
    r"^CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:(?:TEMP|TEMPORARY|TRANSIENT|VOLATILE|LOCAL|GLOBAL)\s+)*"
    r"TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"({_IDENT})\s*\(",
    re.IGNORECASE,
)
# Lines inside the column list that are table constraints, not columns.
_CONSTRAINT_KEYWORDS = {
    "CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "INDEX",
}


def _split_top_level(body: str) -> list[str]:
    """Split a column-list body on top-level commas (ignores commas in types)."""
    parts, depth, current = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def parse_create_table(query: str) -> dict[str, Any] | None:
    """Extract the declared table + column names from a CREATE TABLE statement.

    Returns ``{"scope": {...}, "columns": [NAME, ...]}`` (column names upper-cased)
    or ``None`` if the statement is not a column-list CREATE TABLE (e.g. CTAS,
    or any non-CREATE-TABLE SQL). Used by the post-condition that confirms the
    *intended* schema against information_schema.
    """
    if not query:
        return None
    text = re.sub(r"\s+", " ", query.strip().rstrip(";").strip())
    head = _CREATE_TABLE_HEAD.match(text)
    if not head:
        return None
    # Grab the balanced body after the opening paren of the column list.
    open_idx = text.index("(", head.end() - 1)
    depth, body_chars = 0, []
    for ch in text[open_idx:]:
        if ch == "(":
            depth += 1
            if depth == 1:
                continue
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        body_chars.append(ch)
    body = "".join(body_chars)
    columns: list[str] = []
    for raw in _split_top_level(body):
        tokens = raw.strip().split()
        if not tokens:
            continue
        first = tokens[0].strip('"').upper()
        if first in _CONSTRAINT_KEYWORDS:
            continue  # a table constraint, not a column
        columns.append(first)
    scope = _scope_from_name(head.group(1), "table")
    return {"scope": scope, "columns": columns}


def _canon_col(name: str | None) -> str:
    return (name or "").strip().strip('"').upper()


def create_table_matches_intent() -> PostCondition:
    """Environment-as-oracle: after CREATE TABLE, ask information_schema.

    The object must now exist *and* its live column set must equal the columns
    we declared. A deliberately-wrong CREATE TABLE (missing/extra column) is
    caught here, never accepted because the DDL "ran without error".
    """
    def applies(ctx: VerificationContext) -> bool:
        return parse_create_table(ctx.args.get("query", "")) is not None

    async def check(ctx: VerificationContext):
        parsed = parse_create_table(ctx.args.get("query", ""))
        if not parsed:
            return True, "not a column-list CREATE TABLE"
        scope = parsed["scope"]
        target = ctx.target
        if target is None or not hasattr(target, "invoke"):
            return True, "no introspector available (unverified)"
        res = await target.invoke("introspect_snowflake_object", {
            "object_type": "table",
            "database": scope.get("database"),
            "schema": scope.get("schema"),
            "object": scope.get("object"),
        })
        data = getattr(res, "data", None) or {}
        if not data.get("exists"):
            return False, f"table {scope} not found in information_schema after CREATE"
        live_cols = {_canon_col(c.get("name")) for c in (data.get("columns") or [])}
        want_cols = {_canon_col(c) for c in parsed["columns"]}
        if want_cols and live_cols and want_cols != live_cols:
            missing = sorted(want_cols - live_cols)
            extra = sorted(live_cols - want_cols)
            return False, (
                f"column set mismatch — declared {sorted(want_cols)} but live is "
                f"{sorted(live_cols)} (missing={missing}, unexpected={extra})"
            )
        return True, ""

    return PostCondition(
        "create_table_matches_information_schema", check,
        "object exists in information_schema; column set matches intent",
        anchored=True, applies_when=applies,
    )


def copy_into_loaded_rows() -> PostCondition:
    """A COPY INTO that loaded zero rows is a silent failure — reject it."""
    def applies(ctx: VerificationContext) -> bool:
        q = re.sub(r"\s+", " ", (ctx.args.get("query", "") or "").strip())
        return bool(re.match(r"^COPY\s+INTO\s+(?!@)", q, re.IGNORECASE))

    def check(ctx: VerificationContext):
        meta = getattr(ctx.result, "metadata", None) or {}
        rows = meta.get("rows_affected")
        if rows is not None and rows == 0:
            return False, "COPY INTO loaded 0 rows (expected > 0)"
        return True, ""

    return PostCondition(
        "copy_into_loaded_rows", check,
        "load moved a non-zero number of rows", anchored=True, applies_when=applies,
    )


def introspect_reports_structure() -> PostCondition:
    """Introspection must return a definite existence verdict + scope to act on."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None)
        if not isinstance(data, dict) or "exists" not in data or "scope" not in data:
            return False, "introspection did not return {exists, scope}"
        return True, ""
    return PostCondition(
        "introspect_reports_structure", check,
        "introspection returns a definite existence verdict", anchored=True,
    )


class SnowflakeConnector(Connector):
    """
    Snowflake connector

    [UC1] Bronze layer operations:
    - Schema creation
    - File format creation
    - Table creation
    - COPY INTO operations
    - Data validation queries
    """

    name = "snowflake"

    def __init__(self, settings: Settings):
        super().__init__(settings)
        # Typed Any: the SDK is lazy-imported in connect(), so the concrete
        # SnowflakeConnection/cursor types aren't visible at module scope.
        self._connection: Any = None
        self._cursor: Any = None

    # ------------------------------------------------------------------
    # Connector contract
    # ------------------------------------------------------------------
    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="execute_snowflake_query",
                description="Execute a SQL query on Snowflake. Use for Bronze layer operations: schema creation, file format creation, table creation, COPY INTO, and validation queries. Execute ONE statement at a time.",
                parameters={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The SQL query to execute. Must be a single statement."
                        }
                    },
                    "required": ["query"]
                },
                capability="snowflake.query",
                risk=Risk.RISKY,
                display_name="Execute SQL Query",
                category="query",
                postconditions=[
                    result_succeeded(),
                    create_table_matches_intent(),
                    copy_into_loaded_rows(),
                ],
            ),
            OperationSpec(
                name="introspect_snowflake_object",
                description="Check whether a Snowflake object exists and read its live structure from INFORMATION_SCHEMA. Use to (re)verify a schema/table/view before relying on a cached assumption. Read-only.",
                parameters={
                    "type": "object",
                    "properties": {
                        "object_type": {"type": "string", "enum": ["schema", "table", "view"]},
                        "database": {"type": "string", "description": "Database name (optional; defaults to the connection database)."},
                        "schema": {"type": "string", "description": "Schema name."},
                        "object": {"type": "string", "description": "Table/view name (omit for object_type=schema)."},
                    },
                    "required": ["object_type"],
                },
                capability="snowflake.introspection",
                risk=Risk.SAFE,
                display_name="Introspect Object",
                category="introspection",
                postconditions=[introspect_reports_structure()],
            ),
            OperationSpec(
                name="validate_snowflake_connection",
                description="Test the Snowflake connection and get current context (warehouse, database, schema, role, user).",
                parameters={
                    "type": "object",
                    "properties": {},
                    "required": []
                },
                capability="snowflake.connection",
                risk=Risk.SAFE,
                display_name="Validate Connection",
                category="connection",
                postconditions=[result_succeeded()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        if op == "execute_snowflake_query":
            return await self._execute_query(query=args.get("query", ""))
        if op == "introspect_snowflake_object":
            return await self._introspect(args)
        if op == "validate_snowflake_connection":
            return await self.health()
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def connect(self) -> bool:
        # Establish connection with the user snowflake account. The SDK is
        # lazy-imported here (not at module top) so the connector still loads and
        # appears in the registry when the optional extra isn't installed; only an
        # actual connect attempt asks for it, with an actionable install hint.
        try:
            import snowflake.connector
        except ImportError as e:
            raise ConnectionError(
                "The Snowflake SDK is not installed. Install it with: "
                "pip install 'dacli[snowflake]'"
            ) from e
        try:
            cfg = ConnectorConfig(self.settings, "snowflake")

            self._connection = snowflake.connector.connect(
                account=cfg.get("account", ""),
                user=cfg.get("user", ""),
                password=cfg.get("password", ""),
                role=cfg.get("role", ""),
                warehouse=cfg.get("warehouse", ""),
                database=cfg.get("database", ""),
                schema=cfg.get("schema", "PUBLIC"),
                login_timeout=cfg.get("login_timeout", 60),
                network_timeout=cfg.get("network_timeout", 60),
            )
            self._cursor = self._connection.cursor()
            self.is_connected = True
            return True
        except Exception as e:
            self._is_connected = False
            raise ConnectionError(f"Failed to connect to Snowflake: {e!s}") from e

    # ------------------------------------------------------------------
    # Governance: rollback-path verification
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        """Confirm a native rollback path actually exists before an irreversible op.

        The governor calls this for ``irreversible`` actions; the action is
        blocked unless this returns truthy. We anchor to the platform rather than
        assume: a ``DROP``/``TRUNCATE`` is only recoverable while the object's
        **Time Travel** retention (DATA_RETENTION_TIME_IN_DAYS) is > 0. We read
        that live; any uncertainty returns *not verified* (fail-safe).
        """
        primitive = getattr(plan, "primitive", "")
        if primitive == "transaction":
            return True, "DML is wrapped in a transaction (BEGIN/ROLLBACK)"
        if primitive != "time_travel_undrop":
            return False, f"no verifiable rollback path for primitive '{primitive}'"
        try:
            if not self.is_connected:
                await self.connect()
            # Account-level retention is the floor; an object may set its own.
            self._cursor.execute("SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN ACCOUNT")
            rows = self._cursor.fetchall()
            cols = [d[0].upper() for d in self._cursor.description]
            value = None
            for row in rows:
                rec = dict(zip(cols, row, strict=True))
                value = rec.get("VALUE") or rec.get("value")
                break
            retention = int(value) if value is not None and str(value).isdigit() else 0
            if retention > 0:
                return True, f"Time Travel retention is {retention} day(s) — UNDROP available"
            return False, "Time Travel retention is 0 — DROP/TRUNCATE cannot be undone"
        except Exception as e:
            return False, f"could not verify Time Travel retention: {e}"

    async def disconnect(self) -> None:
        # Close Snowflake connection
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
        self._is_connected = False

    async def health(self) -> ToolResult:
        # Validate the Snowflake connection
        start_time = time.time()

        try:
            if not self.is_connected:
                await self.connect()

            # Context query
            self._cursor.execute("SELECT CURRENT_WAREHOUSE() AS WAREHOUSE,CURRENT_DATABASE() AS DATABASE,CURRENT_SCHEMA() AS SCHEMA,CURRENT_ROLE() AS ROLE,CURRENT_USER() AS USER;")
            result = self._cursor.fetchone()
            columns = [desc[0] for desc in self._cursor.description]
            context = dict(zip(columns, result, strict=True))

            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data=context,
                execution_time_ms=execution_time,
                metadata={"query": "VALIDATE CONNECTION AND GET CONTEXT"}
            )

        except Exception:
            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                data=None,
                execution_time_ms=execution_time,
                metadata={"query": "SELECT CURRENT_WAREHOUSE() AS WAREHOUSE,CURRENT_DATABASE() AS DATABASE,CURRENT_SCHEMA() AS SCHEMA,CURRENT_ROLE() AS ROLE,CURRENT_USER() AS USER"}
            )

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------
    async def _introspect(self, args: dict[str, Any]) -> ToolResult:
        """Read an object's live structure from INFORMATION_SCHEMA.

        Emits a ``create`` catalog effect (with live columns) so the catalog is
        refreshed from ground truth — this is the introspection op the
        verification protocol re-checks a cached fact against.
        """
        start_time = time.time()
        object_type = (args.get("object_type") or "table").lower()
        database = args.get("database") or ConnectorConfig(self.settings, "snowflake").get("database", "")
        schema = args.get("schema")
        obj = args.get("object")
        scope = {"database": database, "schema": schema, "object": obj}

        try:
            if not self.is_connected:
                await self.connect()

            if object_type == "schema":
                sql = (
                    f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA "
                    f"WHERE SCHEMA_NAME = '{(schema or '').upper()}'"
                )
            else:
                sql = (
                    f"SELECT COLUMN_NAME, DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_SCHEMA = '{(schema or '').upper()}' "
                    f"AND TABLE_NAME = '{(obj or '').upper()}' ORDER BY ORDINAL_POSITION"
                )

            self._cursor.execute(sql)
            rows = self._cursor.fetchall()
            columns = [desc[0] for desc in self._cursor.description]
            records = [dict(zip(columns, row, strict=True)) for row in rows]
            exists = len(records) > 0

            cols = None
            if object_type != "schema" and exists:
                cols = [{"name": r.get("COLUMN_NAME"), "type": r.get("DATA_TYPE")} for r in records]

            effects = []
            if exists:
                effects = [{
                    "action": "create",
                    "object_type": object_type,
                    "scope": scope,
                    "source": "snowflake.information_schema",
                    "columns": cols,
                }]

            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data={"exists": exists, "object_type": object_type, "scope": scope, "columns": cols},
                execution_time_ms=execution_time,
                metadata={"catalog_effects": effects, "source": "snowflake.information_schema"},
            )
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                data=None,
                error=str(e),
                execution_time_ms=execution_time,
                metadata={"scope": scope},
            )

    async def _execute_query(self, query: str, **kwargs) -> ToolResult:
        """
        Execute a SQL query on Snowflake.

        Args:
            query: SQL query to execute
            **kwargs: Additional parameters (fetch_limit, etc.)

        Returns:
            ToolResult with query results or error
        """

        start_time = time.time()
        # Data work: fetch the FULL result set by default (no cutoff). A caller
        # may still pass an explicit ``fetch_limit`` as a safety valve.
        fetch_limit = kwargs.get("fetch_limit")

        try:
            if not self.is_connected:
                await self.connect()

            # Execute the query
            self._cursor.execute(query)

            # Check if the query returns resulmts
            if self._cursor.description:
                # Fetch results (all rows unless an explicit limit was requested)
                rows = self._cursor.fetchmany(fetch_limit) if fetch_limit else self._cursor.fetchall()
                columns = [desc[0] for desc in self._cursor.description]

                # Convert to list of dicts
                results = [dict(zip(columns, row, strict=True)) for row in rows]

                total_rows = self._cursor.rowcount if self._cursor.rowcount >= 0 else len(results)

                execution_time = (time.time() - start_time) * 1000

                return ToolResult(
                    tool_name=self.name,
                    status=ToolStatus.SUCCESS,
                    data=results,
                    execution_time_ms=execution_time,
                    metadata={
                        "query": query[:200].replace("\n", " ").replace("  ", " ").replace(";", ""),
                        "rows_returned": len(results),
                        "total_rows": total_rows,
                        "columns": columns,
                        "catalog_effects": parse_catalog_effects(query),
                    }
                )

            # DDL or DML without results
            rows_affected = self._cursor.rowcount if self._cursor.rowcount >= 0 else 0
            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data=None,
                execution_time_ms=execution_time,
                metadata={
                    "query": query[:200].replace("\n", " ").replace("  ", " ").replace(";", ""),
                    "rows_affected": rows_affected,
                    "catalog_effects": parse_catalog_effects(query),
                }
            )
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                data=None,
                execution_time_ms=execution_time,
                error=str(e),
                metadata={"query": query}
            )
