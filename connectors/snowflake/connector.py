import snowflake.connector
import time
from typing import Any, Dict, List

from connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from config.settings import Settings


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
        fetch_limit = kwargs.get("fetch_limit", 100)

        try:
            if not self.is_connected:
                await self.connect()

            # Execute the query
            self._cursor.execute(query)

            # Check if the query returns resulmts
            if self._cursor.description:
                # Fetch results
                rows = self._cursor.fetchmany(fetch_limit)
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
