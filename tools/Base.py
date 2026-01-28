from abc import ABC, adbstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

class ToolStatus(Enum):
    # Status of the tool execution
    SUCCESS, ERROR, TIMEOUT, CANCELLED, PENDING_APPROVAL = "sucess", "error", "timeout", "cancelled", "pending_approval"

@dataclass
class ToolResult:
    # Result of a toolkit execution
    tool_name: str
    status: ToolStatus
    data: Any = None
    error: Optional[str] = None
    execution_time_ms : float = 0.0
    timestamp : datetime = field(default_factory=datetime.now)
    metadata : Dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool :
        # Check if the tool execution was succesful
        return self.status == ToolStatus.SUCCESS

    def to_dict(self) -> Dict[str, Any]:
        # Convert the result to dictionary for serialization
        return {
            "tool_name": self.tool_name,
            "status" : self.status,
            "data" : self.data,
            "error" : self.error,
            "execution_time_ms" : self.execution_time_ms,
            "timestamp" : self.timestamp.isoformat(),
            "metadata" : self.metadata
        }

    def to_message(self) -> str:
        # Convert the result to a message string for LLM context
        if self.success:
            if isinstance(self.data, list):
                if len(self.data) == 0:
                    return f"[{self.tool_name}] Executed successfully. No results returned"
                return  f"[{self.tool_name}] Executed successfully. Returned {len(self.data)} rows:\n{self._format_data()}"
            elif self.data:
                return  f"[{self.tool_name}] Executed successfully:\n{self._format_data()}"
            else:
                return  f"[{self.tool_name}] Executed successfully."
        else:
            return  f"[{self.tool_name}] failed with error: {self.error}"

    def _format_data(self) -> str:
        # Format data for display
        if isinstance(self.data, list) and len(self.data)>0:
            # Limit to first 20 rows for the console print
            # TODO: Add a log files to log full data
            display_data = self.data[:20]
            if isinstance(display_data[0], dict):
                # Format as table-like structure
                lines = []
                for i, row in enumerate(display_data):
                    lines.append(f" Row {i+1}: {row}")
                if len(self.data) > 20:
                    lines.append(f"... and {len(self.data - 20)} more rows")
                return "\n".join(lines)
        return str(self.data)


class BaseTool(ABC):
    """
    Base class for all the tools
    Tools should implement:
        - name: tool identifier
        - description: what the does
        - execute(): main execution function
        - validate(): check if the tool can be executed
    """

    def __init__(self, settings: Any):
        # Initialize the tool with settings
        self.settings = settings
        self._is_connected = False

    @property
    @adbstractmethod
    def name(self) -> str:
        # Return the tool name
        pass

    @property
    @adbstractmethod
    def description(self) -> str:
        # Return the tool description
        pass
    
    @property
    def is_connected(self) -> str:
        # Check if tool is connected/ready
        return self.is_connected()  

    @adbstractmethod
    async def execute(self, **kwargs) -> ToolResult:
        """
        Execute the tool with given parameters.
        
        Returns:
            ToolResult with execution status and data
        """
        pass

    @adbstractmethod
    async def validate(self, **kwargs) -> ToolResult:
        pass

    async def connect(self) -> bool:
        """
        Establish connection if needed.
        
        Returns:
            True if connection successful
        """
        result = await self.validate()
        self.is_connected = result.success
        return self.is_connected

    async def disconnect(self) -> bool:
        # Clean up connection resources.
        self.is_connected = False
    
    def get_schema(self) -> Dict[str, Any]:
        # Return JSON schema for tool parameters. Override in subclasses to provide parameter validation
        return {
            "type": "object",
            "proprieties": {},
            "required": []
        }
