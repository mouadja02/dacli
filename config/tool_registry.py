# Tool Registry - Manages dynamic tool loading based on configuration
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from enum import Enum


class ToolCategory(str, Enum):
    """Categories of tools available in DACLI"""

    SNOWFLAKE = "snowflake"
    GITHUB = "github"
    PINECONE = "pinecone"


# Define all available tools with their metadata
TOOL_CATALOG = {
    ToolCategory.SNOWFLAKE: {
        "name": "Snowflake",
        "description": "Execute SQL queries and manage your Snowflake data warehouse",
        "icon": "❄️",
        "operations": {
            "execute_snowflake_query": {
                "name": "Execute SQL Query",
                "description": "Execute a SQL query on Snowflake",
                "category": "query",
            },
            "validate_snowflake_connection": {
                "name": "Validate Connection",
                "description": "Test Snowflake connection and get current context",
                "category": "connection",
            },
        },
        "required_config": ["account", "user", "password", "warehouse", "database"],
    },
    ToolCategory.GITHUB: {
        "name": "GitHub",
        "description": "Manage files, workflows, and repositories on GitHub",
        "icon": "🐙",
        "operations": {
            "list_github_directory": {
                "name": "List Directory",
                "description": "List contents of a directory in the repository",
                "category": "read",
            },
            "read_github_file": {
                "name": "Read File",
                "description": "Read content of a file from the repository",
                "category": "read",
            },
            "push_github_file": {
                "name": "Push File",
                "description": "Create or update a file in the repository",
                "category": "write",
            },
            "delete_github_file": {
                "name": "Delete File",
                "description": "Delete a file from the repository",
                "category": "write",
            },
            "trigger_github_workflow": {
                "name": "Trigger Workflow",
                "description": "Trigger a GitHub Actions workflow",
                "category": "workflow",
            },
            "list_github_workflow_runs": {
                "name": "List Workflow Runs",
                "description": "List recent workflow runs",
                "category": "workflow",
            },
            "get_github_workflow_run": {
                "name": "Get Workflow Run",
                "description": "Get status of a specific workflow run",
                "category": "workflow",
            },
            "get_github_workflow_run_jobs": {
                "name": "Get Workflow Jobs",
                "description": "Get jobs and logs for a workflow run",
                "category": "workflow",
            },
        },
        "required_config": ["token", "owner", "repo"],
    },
    ToolCategory.PINECONE: {
        "name": "Pinecone (Vector Search)",
        "description": "Search documentation using vector embeddings for RAG",
        "icon": "🌲",
        "operations": {
            "search_snowflake_docs": {
                "name": "Search Documentation",
                "description": "Search Snowflake docs in Pinecone vector store",
                "category": "search",
            }
        },
        "required_config": ["api_key", "index_name"],
    },
}


class ToolOperationConfig(BaseModel):
    """Configuration for a single tool operation"""

    enabled: bool = True


class ToolConfig(BaseModel):
    """Configuration for a tool category"""

    enabled: bool = False
    operations: Dict[str, bool] = Field(default_factory=dict)

    def get_enabled_operations(self) -> List[str]:
        """Get list of enabled operation names"""
        if not self.enabled:
            return []
        return [op for op, enabled in self.operations.items() if enabled]


class ToolsSettings(BaseModel):
    """Main tools configuration"""

    # Whether initial setup has been completed
    setup_completed: bool = False

    # Tool configurations
    snowflake: ToolConfig = Field(default_factory=ToolConfig)
    github: ToolConfig = Field(default_factory=ToolConfig)
    pinecone: ToolConfig = Field(default_factory=ToolConfig)

    def get_enabled_tools(self) -> List[ToolCategory]:
        """Get list of enabled tool categories"""
        enabled = []
        if self.snowflake.enabled:
            enabled.append(ToolCategory.SNOWFLAKE)
        if self.github.enabled:
            enabled.append(ToolCategory.GITHUB)
        if self.pinecone.enabled:
            enabled.append(ToolCategory.PINECONE)
        return enabled

    def get_all_enabled_operations(self) -> List[str]:
        """Get all enabled operations across all tools"""
        operations = []
        operations.extend(self.snowflake.get_enabled_operations())
        operations.extend(self.github.get_enabled_operations())
        operations.extend(self.pinecone.get_enabled_operations())
        return operations

    def get_tool_config(self, category: ToolCategory) -> ToolConfig:
        """Get configuration for a specific tool category"""
        return getattr(self, category.value)

    def set_tool_config(self, category: ToolCategory, config: ToolConfig):
        """Set configuration for a specific tool category"""
        setattr(self, category.value, config)


class ToolRegistry:
    """
    Dynamic tool registry that manages tool loading based on configuration.

    This class is responsible for:
    - Tracking which tools are enabled/disabled
    - Providing tool definitions to the LLM based on enabled tools
    - Validating tool configurations
    """

    def __init__(self, tools_settings: ToolsSettings):
        self.settings = tools_settings
        self._tool_definitions_cache: Optional[List[Dict]] = None

    def is_tool_enabled(self, category: ToolCategory) -> bool:
        """Check if a tool category is enabled"""
        return self.settings.get_tool_config(category).enabled

    def is_operation_enabled(self, operation_name: str) -> bool:
        """Check if a specific operation is enabled"""
        return operation_name in self.settings.get_all_enabled_operations()

    def get_enabled_categories(self) -> List[ToolCategory]:
        """Get all enabled tool categories"""
        return self.settings.get_enabled_tools()

    def invalidate_cache(self):
        """Invalidate the tool definitions cache"""
        self._tool_definitions_cache = None

    @staticmethod
    def get_catalog() -> Dict:
        """Get the full tool catalog"""
        return TOOL_CATALOG

    @staticmethod
    def get_tool_info(category: ToolCategory) -> Dict:
        """Get metadata for a specific tool category"""
        return TOOL_CATALOG.get(category, {})
