import re
import snowflake.connector
import time
from typing import Any, Dict, List

from connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from config.settings import Settings


# ---------------------------------------------------------------------------
# Structured catalog-effect parsing (Phase 2, deferred from Phase 1)
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


def _scope_from_name(qualified: str, object_type: str) -> Dict[str, Any]:
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


def parse_catalog_effects(query: str) -> List[Dict[str, Any]]:
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
        self._connection = None
        self._cursor = None

    # ------------------------------------------------------------------
    # Connector contract
    # ------------------------------------------------------------------
    def operations(self) -> List[OperationSpec]:
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
            ),
        ]

    async def invoke(self, op: str, args: Dict[str, Any]) -> ToolResult:
        if op == "execute_snowflake_query":
            return await self._execute_query(query=args.get("query", ""))
        elif op == "introspect_snowflake_object":
            return await self._introspect(args)
        elif op == "validate_snowflake_connection":
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
        # Establish connection with the user snowflake account
        try:
            snowflake_settings = self.settings.snowflake

            self._connection = snowflake.connector.connect(
                account=snowflake_settings.account,
                user=snowflake_settings.user,
                password=snowflake_settings.password,
                role=snowflake_settings.role,
                warehouse=snowflake_settings.warehouse,
                database=snowflake_settings.database,
                schema=snowflake_settings.db_schema,
                login_timeout=snowflake_settings.login_timeout,
                network_timeout=snowflake_settings.network_timeout
            )
            self._cursor = self._connection.cursor()
            self.is_connected = True
            return True
        except Exception as e:
            self._is_connected = False
            raise ConnectionError(f"Failed to connect to Snowflake: {str(e)}")
        return False

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
            context = dict(zip(columns, result))

            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data=context,
                execution_time_ms=execution_time,
                metadata={"query": "VALIDATE CONNECTION AND GET CONTEXT"}
            )

        except Exception as e:
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
    async def _introspect(self, args: Dict[str, Any]) -> ToolResult:
        """Read an object's live structure from INFORMATION_SCHEMA.

        Emits a ``create`` catalog effect (with live columns) so the catalog is
        refreshed from ground truth — this is the introspection op the
        verification protocol re-checks a cached fact against.
        """
        start_time = time.time()
        object_type = (args.get("object_type") or "table").lower()
        database = args.get("database") or self.settings.snowflake.database
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
            records = [dict(zip(columns, row)) for row in rows]
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
                results = [dict(zip(columns, row)) for row in rows]

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

            else:  # DDL or DML without results
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
