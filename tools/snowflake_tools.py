import snowflake.connector
import time
from typing import Any, Dict, List, Optional
from tabulate import tabulate

from tools.Base import BaseTool, ToolResult, ToolStatus
from config.settings import Settings


class SnowflakeTool(BaseTool):
    """
    Snowflake toolkit

    [UC1] Bronze layer operations:
    - Schema creation
    - File format creation
    - Table creation
    - COPY INTO operations
    - Data validation queries
    """

    def __init__(self, settings: Settings):
        # Initialize the Snowflake tool with settings
        super().__init__(settings)
        self._connection = None
        self._cursor = None

    @property
    def name(self) -> str:
        return "snowflake"

    @property
    def description(self) -> str:
        return "Snowflake toolkit"

    async def connect(self) -> bool:
        # Establish connection with the user snowflake account
        try:
            snowflake_settings = self.settings.snowflake

            self._connection = snowflake.connector.connect(
                account= snowflake_settings.account,
                user= snowflake_settings.user,
                password= snowflake_settings.password,
                role= snowflake_settings.role,
                warehouse = snowflake_settings.warehouse,
                database= snowflake_settings.database,
                schema= snowflake_settings.db_schema,
                login_timeout= snowflake_settings.login_timeout,
                network_timeout= snowflake_settings.network_timeout
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
    
    async def validate(self) -> ToolResult:
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

    async def execute(self, query: str, **kwargs) -> ToolResult:
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

            else: # DDL or DML without results
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

    def get_schema(self) -> Dict[str, Any]:
        # Return JSON schema for Snowflake tool parameters
        return {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL query to execute on Snowflake"
                },
                "fetch_limit": {
                    "type": "integer",
                    "description": "Maximum number of rows to fetch",
                    "default": 1000
                }
            },
            "required": ["query"]
        }

    def format_results_table(self, results: List[Dict[str, Any]], max_rows: int = 20) -> str:
        """
        Format query results as a table string.
        
        Args:
            results: List of result dictionaries
            max_rows: Maximum rows to display
        
        Returns:
            Formatted table string

            Inputy exmaple:
            [
                {"id": 1, "name": "Person X1", "number": 0102030405},
                {"id": 2, "name": "Person X2", "number": 0203040506},
                {"id": 3, "name": "Person X3", "number": 0304050607}
            ]

            Output example:
            +----+-------------+--------------+
            | id | name        | number       |
            +----+-------------+--------------+
            | 1  | Person X1   | 0102030405   |
            | 2  | Person X2   | 0203040506   |
            | 3  | Person X3   | 0304050607   |
            +----+-------------+--------------+
        """
        if not results:
            return "No results"
        
        try:          
            display_data = results[:max_rows]
            headers = list(display_data[0].keys())
            rows = [[str(row.get(h, "")) for h in headers] for row in display_data]
            
            table = tabulate(rows, headers=headers, tablefmt=self.settings.ui.table_format)
            
            if len(results) > max_rows:
                table += f"\n\n... and {len(results) - max_rows} more rows"
            
            return table
            
        except ImportError:
            # Fallback without tabulate
            lines = []
            for i, row in enumerate(results[:max_rows]):
                lines.append(f"Row {i+1}: {row}")
            if len(results) > max_rows:
                lines.append(f"... and {len(results) - max_rows} more rows")
            return "\n".join(lines)