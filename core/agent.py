import json
import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple, Callable
from dataclasses import dataclass

from config.settings import Settings
from config.tool_registry import ToolsSettings, ToolRegistry, ToolCategory
from core.memory import AgentMemory, PhaseStatus
from tools.base import ToolResult, ToolStatus
from tools.snowflake_tools import SnowflakeTool
from tools.github_tools import GithubTool
from tools.pinecone_tools import PineconeTool
from prompts.system_prompt import load_system_prompt


@dataclass
class AgentResponse:
    # Response from the agent
    content: str
    tool_calls: List[Dict[str, Any]]
    thinking: Optional[str] = None
    needs_user_input: bool = False
    error: Optional[str] = None
    iteration: int = 0


class LLMClient:
    # Multi-provider LLM client

    def __init__(self, settings: Settings):
        # Initialize LLM client with settings
        self.settings = settings
        self._client: Optional[Any] = None
        self._provider = settings.llm.provider

    async def initialize(self) -> None:
        # Initialize the LLM client based on the provider
        provider = self._provider.lower()

        if provider == "openai":
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url,
                timeout=self.settings.llm.timeout,
            )
        elif provider == "anthropic":
            from anthropic import AsyncAnthropic

            self._client = AsyncAnthropic(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url,
                timeout=self.settings.llm.timeout,
            )
        elif provider == "google":
            import google.generativeai as genai  # type: ignore[import-untyped]

            genai.configure(api_key=self.settings.llm.api_key)
            self._client = genai  # store module so _generate_google can call .GenerativeModel()
        elif provider == "openrouter":
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI(
                api_key=self.settings.llm.api_key,
                base_url=self.settings.llm.base_url or "https://openrouter.ai/api/v1",
                timeout=self.settings.llm.timeout,
            )
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")

    async def generate(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        system_prompt: Optional[str] = None,
    ) -> Tuple[str, List[Dict]]:
        """
        Generate a response from the LLM.

        Args:
            messages: Conversation messages
            tools: Available tool definitions
            system_prompt: System prompt to use

        Returns:
            Tuple of (response content, tool calls)
        """
        if not self._client:
            await self.initialize()

        provider = self._provider.lower()
        if provider in ["openai", "openrouter"]:
            return await self._generate_openai(messages, tools, system_prompt)
        elif provider == "anthropic":
            return await self._generate_anthropic(messages, tools, system_prompt)
        elif provider == "google":
            return await self._generate_google(messages, tools, system_prompt)
        else:
            raise ValueError(f"Unsupported provider: {provider}")

    async def _generate_openai(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        system_prompt: Optional[str] = None,
    ) -> Tuple[str, List[Dict]]:
        # Generate using OpenAI-compatibile API

        # Prepare messages includes system prompt
        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        # Prepare request
        request_kwargs = {
            "model": self.settings.llm.model,
            "messages": full_messages,
            "temperature": self.settings.llm.temperature,
            "max_tokens": self.settings.llm.max_tokens,
        }

        # Add tools if provided
        if tools:
            request_kwargs["tools"] = tools
            request_kwargs["tool_choice"] = "auto"

        # Make request
        response = await self._client.chat.completions.create(**request_kwargs)

        # Extract response
        choice = response.choices[0]
        content = choice.message.content or ""

        # Extract tool calls
        tool_calls = []
        if hasattr(choice.message, "tool_calls") and choice.message.tool_calls:
            for tc in choice.message.tool_calls:
                tool_calls.append(
                    {
                        "id": tc.id,
                        "name": tc.function.name,
                        "arguments": json.loads(tc.function.arguments),
                    }
                )

        return content, tool_calls

    async def _generate_anthropic(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        system_prompt: Optional[str] = None,
    ) -> Tuple[str, List[Dict]]:
        # Generate using Anthropic API
        # Prepare request
        request_kwargs = {
            "model": self.settings.llm.model,
            "max_tokens": self.settings.llm.max_tokens,
            "messages": messages,
        }

        if system_prompt:
            request_kwargs["system"] = system_prompt

        # Convert tools to Anthropic format
        if tools:
            anthropic_tools = []
            for tool in tools:
                anthropic_tools.append(
                    {
                        "name": tool["function"]["name"],
                        "description": tool["function"]["description"],
                        "input_schema": tool["function"]["parameters"],
                    }
                )
            request_kwargs["tools"] = anthropic_tools

        response = await self._client.messages.create(**request_kwargs)

        # Extract response
        content = ""
        tool_calls = []

        for block in response.content:
            if block.type == "text":
                content += block.text
            elif block.type == "tool_use":
                tool_calls.append(
                    {"id": block.id, "name": block.name, "arguments": block.input}
                )

        return content, tool_calls

    async def _generate_google(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        system_prompt: Optional[str] = None,
    ) -> Tuple[str, List[Dict]]:
        # Generate using Google Gemini API
        model = self._client.GenerativeModel(
            self.settings.llm.model, system_instruction=system_prompt
        )

        # Convert messages to Gemini format
        gemini_messages = []
        for msg in messages:
            role = "user" if msg["role"] == "user" else "model"
            gemini_messages.append({"role": role, "parts": [msg["content"]]})

        # TODO: Tool calling for Gemini would need additional handling
        response = await asyncio.to_thread(model.generate_content, gemini_messages)

        return response.text, []


class DACLI:
    # Main Data Agent CLI: Orchestrates the workflow for building data agent using:
    def __init__(
        self,
        settings: Settings,
        tools_settings: Optional[ToolsSettings] = None,
        memory: Optional[AgentMemory] = None,
        system_prompt: Optional[str] = None,
        on_status_update: Optional[Callable[[str], None]] = None,
        on_tool_start: Optional[Callable[[str, Dict], None]] = None,
        on_tool_end: Optional[Callable[[str, ToolResult], None]] = None,
        on_user_input_needed: Optional[Callable[[str], str]] = None,
    ):
        # Initialize the DACLI class
        self.settings = settings

        # Tool registry for dynamic tool management
        self.tools_settings = tools_settings or ToolsSettings()
        self.tool_registry = ToolRegistry(self.tools_settings)

        self.memory = memory or AgentMemory(
            state_path=settings.agent.state_path,
            history_path=settings.agent.history_path,
            memory_window=settings.agent.memory_window,
        )

        self.system_prompt = system_prompt or load_system_prompt()

        # Callbacks
        self._on_status_update = on_status_update
        self._on_tool_start = on_tool_start
        self._on_tool_end = on_tool_end
        self._on_user_input_needed = on_user_input_needed

        # Initialize components
        self.llm = LLMClient(settings)

        # Tools - only instantiate if enabled
        self.snowflake = None
        self.github = None
        self.pinecone = None

        if self.tool_registry.is_tool_enabled(ToolCategory.SNOWFLAKE):
            self.snowflake = SnowflakeTool(settings)
        if self.tool_registry.is_tool_enabled(ToolCategory.GITHUB):
            self.github = GithubTool(settings)
        if self.tool_registry.is_tool_enabled(ToolCategory.PINECONE):
            self.pinecone = PineconeTool(settings)

        # Iteration tracking
        self._current_iteration = 0
        self._max_iterations = settings.agent.max_iterations

        # Tool definitions for LLM (built dynamically based on enabled tools)
        self._tools = self._build_tool_definitions()

    def _build_tool_definitions(self) -> List[Dict]:
        """
        Build tool definitions for LLM dynamically based on enabled tools.
        Only includes tools that are enabled in the tool registry.
        """
        tools = []

        # ================================================================
        # SNOWFLAKE TOOLS
        # ================================================================
        if self.tool_registry.is_tool_enabled(ToolCategory.SNOWFLAKE):
            if self.tool_registry.is_operation_enabled("execute_snowflake_query"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "execute_snowflake_query",
                            "description": "Execute a SQL query on Snowflake. Use for Bronze layer operations: schema creation, file format creation, table creation, COPY INTO, and validation queries. Execute ONE statement at a time.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "query": {
                                        "type": "string",
                                        "description": "The SQL query to execute. Must be a single statement.",
                                    }
                                },
                                "required": ["query"],
                            },
                        },
                    }
                )

            if self.tool_registry.is_operation_enabled("validate_snowflake_connection"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "validate_snowflake_connection",
                            "description": "Test the Snowflake connection and get current context (warehouse, database, schema, role, user).",
                            "parameters": {
                                "type": "object",
                                "properties": {},
                                "required": [],
                            },
                        },
                    }
                )

        # ================================================================
        # PINECONE TOOLS (Vector Search / RAG)
        # ================================================================
        if self.tool_registry.is_tool_enabled(ToolCategory.PINECONE):
            if self.tool_registry.is_operation_enabled("search_snowflake_docs"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "search_snowflake_docs",
                            "description": "Search Snowflake documentation in Pinecone vector store. Use when templates fail or need clarification on Snowflake concepts.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "query": {
                                        "type": "string",
                                        "description": "Search query for Snowflake documentation",
                                    }
                                },
                                "required": ["query"],
                            },
                        },
                    }
                )

        # ================================================================
        # GITHUB TOOLS
        # ================================================================
        if self.tool_registry.is_tool_enabled(ToolCategory.GITHUB):
            if self.tool_registry.is_operation_enabled("list_github_directory"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "list_github_directory",
                            "description": "List contents of a directory in the GitHub repository.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "path": {
                                        "type": "string",
                                        "description": "The directory path to list (e.g. 'models', 'analyses'). Use empty string or '/' for root.",
                                    }
                                },
                                "required": ["path"],
                            },
                        },
                    }
                )

            if self.tool_registry.is_operation_enabled("read_github_file"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "read_github_file",
                            "description": "Read the content of a file from the GitHub repository.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "path": {
                                        "type": "string",
                                        "description": "The file path to read (e.g. 'dbt_project.yml').",
                                    }
                                },
                                "required": ["path"],
                            },
                        },
                    }
                )

            if self.tool_registry.is_operation_enabled("push_github_file"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "push_github_file",
                            "description": "Create or update a file in the GitHub repository (commits changes).",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "path": {
                                        "type": "string",
                                        "description": "The file path to create or update.",
                                    },
                                    "content": {
                                        "type": "string",
                                        "description": "The full content of the file.",
                                    },
                                    "message": {
                                        "type": "string",
                                        "description": "Commit message describing the change.",
                                    },
                                },
                                "required": ["path", "content", "message"],
                            },
                        },
                    }
                )

            if self.tool_registry.is_operation_enabled("delete_github_file"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "delete_github_file",
                            "description": "Delete a file in the GitHub repository (commits changes).",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "path": {
                                        "type": "string",
                                        "description": "The file path to delete.",
                                    },
                                    "message": {
                                        "type": "string",
                                        "description": "Commit message describing the deletion.",
                                    },
                                },
                                "required": ["path", "message"],
                            },
                        },
                    }
                )

            if self.tool_registry.is_operation_enabled("trigger_github_workflow"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "trigger_github_workflow",
                            "description": "Trigger a GitHub Actions workflow.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "workflow_id": {
                                        "type": "string",
                                        "description": "Workflow filename (e.g. 'deploy_dbt.yml').",
                                    },
                                    "inputs": {
                                        "type": "object",
                                        "description": "Optional inputs for the workflow_dispatch event.",
                                    },
                                },
                                "required": ["workflow_id"],
                            },
                        },
                    }
                )

            if self.tool_registry.is_operation_enabled("list_github_workflow_runs"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "list_github_workflow_runs",
                            "description": "List recent workflow runs for the repository.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "limit": {
                                        "type": "integer",
                                        "description": "Number of runs to return (default 5).",
                                    }
                                },
                                "required": [],
                            },
                        },
                    }
                )

            if self.tool_registry.is_operation_enabled("get_github_workflow_run"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "get_github_workflow_run",
                            "description": "Get status and details of a workflow run.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "run_id": {
                                        "type": "integer",
                                        "description": "The ID of the workflow run.",
                                    }
                                },
                                "required": ["run_id"],
                            },
                        },
                    }
                )

            if self.tool_registry.is_operation_enabled("get_github_workflow_run_jobs"):
                tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": "get_github_workflow_run_jobs",
                            "description": "Get jobs, steps, and failure logs for a workflow run.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "run_id": {
                                        "type": "integer",
                                        "description": "The ID of the workflow run.",
                                    }
                                },
                                "required": ["run_id"],
                            },
                        },
                    }
                )

        # ================================================================
        # CORE TOOLS (always available)
        # ================================================================
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": "request_user_input",
                    "description": "Request input from the user when stuck, encountering errors, or needing clarification. Use this to put the user in the loop.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "The question or information request for the user",
                            },
                            "context": {
                                "type": "string",
                                "description": "Context about what led to this request",
                            },
                        },
                        "required": ["question"],
                    },
                },
            }
        )

        tools.append(
            {
                "type": "function",
                "function": {
                    "name": "update_progress",
                    "description": "Update the current progress status. Use to track phases and steps completed.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "phase": {
                                "type": "string",
                                "description": "Current phase (phase_0_infrastructure, phase_1_discovery, etc.)",
                            },
                            "step": {
                                "type": "string",
                                "description": "Description of the step completed",
                            },
                            "status": {
                                "type": "string",
                                "enum": ["in_progress", "completed", "failed"],
                                "description": "Status of the phase",
                            },
                        },
                        "required": ["phase", "step"],
                    },
                },
            }
        )

        return tools

    def _emit_status(self, message: str) -> None:
        # Emit a status update
        if self._on_status_update:
            self._on_status_update(message)

    async def initialize(self) -> bool:
        # Initialize only enabled tools and connections
        self._emit_status("Initializing agent...")

        successfully_initialized = []
        failed_initializations = []
        skipped = []

        try:
            # Initialize LLM (always required)
            self._emit_status("Connecting to LLM provider ...")
            await self.llm.initialize()
            successfully_initialized.append("LLM")

        except Exception as e:
            self._emit_status(f"Failed to initialize LLM: {str(e)}")
            failed_initializations.append("LLM")

        # Initialize Snowflake (if enabled)
        if self.snowflake:
            try:
                self._emit_status("Connecting to Snowflake ...")
                await self.snowflake.connect()
                successfully_initialized.append("Snowflake")
            except Exception as e:
                self._emit_status(f"Failed to initialize Snowflake: {str(e)}")
                failed_initializations.append("Snowflake")
        else:
            skipped.append("Snowflake")

        # Initialize Pinecone (if enabled)
        if self.pinecone:
            try:
                self._emit_status("Connecting to Pinecone ...")
                await self.pinecone.connect()
                successfully_initialized.append("Pinecone")
            except Exception as e:
                self._emit_status(f"Failed to initialize Pinecone: {str(e)}")
                failed_initializations.append("Pinecone")
        else:
            skipped.append("Pinecone")

        # Initialize GitHub (if enabled)
        if self.github:
            try:
                self._emit_status("Connecting to GitHub ...")
                await self.github.connect()
                successfully_initialized.append("GitHub")
            except Exception as e:
                self._emit_status(f"Failed to initialize GitHub: {str(e)}")
                failed_initializations.append("GitHub")
        else:
            skipped.append("GitHub")

        output_message = "Agent initialized!\nActive tools: " + ", ".join(
            successfully_initialized
        )

        if skipped:
            output_message += "\nSkipped (disabled): " + ", ".join(skipped)

        if failed_initializations:
            output_message += "\nFailed to initialize: " + ", ".join(
                failed_initializations
            )

        self._emit_status(output_message)
        return True

    async def shutdown(self) -> None:
        # Clean up resources for enabled tools only
        if self.snowflake:
            await self.snowflake.disconnect()
        if self.github:
            await self.github.disconnect()
        if self.pinecone:
            await self.pinecone.disconnect()

    async def _execute_tool(
        self, tool_name: str, arguments: Dict[str, Any]
    ) -> ToolResult:
        # Execute a tool call
        start_time = time.time()

        # Emit tool start
        if self._on_tool_start:
            self._on_tool_start(tool_name, arguments)

        result = None

        try:
            if tool_name == "execute_snowflake_query":
                result = await self.snowflake.execute(query=arguments.get("query", ""))

                # Update memory with execution
                if result.success:
                    query = arguments.get("query", "").upper()
                    if "CREATE SCHEMA" in query:
                        schema_name = query.split(".")[-1].replace(";", "").strip()
                        self.memory.add_created_schema(schema_name)
                    elif "CREATE" in query and "FILE FORMAT" in query:
                        # Extract format name
                        ff_name = query.split("FILE FORMAT")[1].split("(")[0].strip()
                        self.memory.add_created_file_format(ff_name)
                    elif "CREATE" in query and "TABLE" in query:
                        # Extract table name
                        parts = query.split("TABLE")[1].split("(")[0].strip()
                        self.memory.add_created_table(parts)
            elif tool_name == "validate_snowflake_connection":
                result = await self.snowflake.validate()

            elif tool_name == "search_snowflake_docs":
                result = await self.pinecone.execute(query=arguments.get("query", ""))

            elif tool_name == "list_github_directory":
                result = await self.github.execute(
                    operation="list_directory", **arguments
                )

            elif tool_name == "read_github_file":
                result = await self.github.execute(operation="read_file", **arguments)

            elif tool_name == "push_github_file":
                result = await self.github.execute(
                    operation="create_or_update_file", **arguments
                )

            elif tool_name == "trigger_github_workflow":
                result = await self.github.execute(
                    operation="trigger_workflow", **arguments
                )

            elif tool_name == "list_github_workflow_runs":
                # Map limit to per_page
                if "limit" in arguments:
                    arguments["per_page"] = arguments.pop("limit")
                result = await self.github.execute(
                    operation="list_workflow_runs", **arguments
                )

            elif tool_name == "get_github_workflow_run":
                result = await self.github.execute(
                    operation="get_workflow_run", **arguments
                )

            elif tool_name == "get_github_workflow_run_jobs":
                result = await self.github.execute(
                    operation="get_workflow_run_jobs", **arguments
                )

            elif tool_name == "request_user_input":
                # Handle user input request
                question = arguments.get("question", "")
                context = arguments.get("context", "")

                if self._on_user_input_needed:
                    user_response = self._on_user_input_needed(
                        f"{context}\n\n{question}" if context else question
                    )
                    result = ToolResult(
                        tool_name=tool_name,
                        status=ToolStatus.SUCCESS,
                        data={"user_response": user_response},
                    )
                else:
                    result = ToolResult(
                        tool_name=tool_name,
                        status=ToolStatus.PENDING_APPROVAL,
                        data={"question": question, "context": context},
                    )

            elif tool_name == "update_progress":
                phase = arguments.get("phase", "")
                step = arguments.get("step", "")
                status = arguments.get("status", "in_progress")

                status_enum = {
                    "in_progress": PhaseStatus.IN_PROGRESS,
                    "completed": PhaseStatus.COMPLETED,
                    "failed": PhaseStatus.FAILED,
                }.get(status, PhaseStatus.IN_PROGRESS)

                self.memory.update_phase(phase, status=status_enum, step_completed=step)

                result = ToolResult(
                    tool_name=tool_name,
                    status=ToolStatus.SUCCESS,
                    data={"phase": phase, "step": step, "status": status},
                )

            else:
                result = ToolResult(
                    tool_name=tool_name,
                    status=ToolStatus.ERROR,
                    error=f"Unknown tool: {tool_name}",
                )

        except Exception as e:
            result = ToolResult(
                tool_name=tool_name,
                status=ToolStatus.ERROR,
                error=str(e),
                execution_time_ms=(time.time() - start_time) * 1000,
            )

        # Log tool execution
        self.memory.log_tool_execution(
            tool_name=tool_name,
            input_params=arguments,
            result=result.data if result.success else None,
            error=result.error,
            execution_time_ms=result.execution_time_ms,
        )

        # Emit tool end
        if self._on_tool_end:
            self._on_tool_end(tool_name, result)

        return result

    async def process_message(self, user_message: str) -> AgentResponse:
        # Process a user message and generate a response.
        # Add user message to memory
        self.memory.add_user_message(user_message)

        # get conversation context
        messages: List[Dict[str, Any]] = list(self.memory.get_context_messages())

        # Iteration loop
        self._current_iteration = 0

        while self._current_iteration < self._max_iterations:
            self._current_iteration += 1
            self._emit_status(
                f"Iteration {self._current_iteration}/{self._max_iterations}"
            )

            try:
                # Generate response from LLM
                content, tool_calls = await self.llm.generate(
                    messages=messages,
                    tools=self._tools,
                    system_prompt=self.system_prompt,
                )

                # If no tool call, we have a final response
                if not tool_calls:
                    self.memory.add_assistant_message(content)
                    return AgentResponse(
                        content=content,
                        tool_calls=[],
                        iteration=self._current_iteration,
                    )

                # Execute tool calls sequentially to avoid race conditions
                tool_results = []
                needs_user_input = False

                # Add assistant message with tool_calls ONCE before processing
                assistant_msg = {
                    "role": "assistant",
                    "content": content or None,
                    "tool_calls": [
                        {
                            "id": tc["id"],
                            "type": "function",
                            "function": {
                                "name": tc["name"],
                                "arguments": json.dumps(tc["arguments"]),
                            },
                        }
                        for tc in tool_calls
                    ],
                }
                messages.append(assistant_msg)

                for tool_call in tool_calls:
                    tool_name = tool_call["name"]
                    tool_call_id = tool_call["id"]
                    arguments = tool_call.get("arguments", {})

                    result = await self._execute_tool(tool_name, arguments)
                    tool_results.append(result)

                    # Check if user input is needed
                    if result.status == ToolStatus.PENDING_APPROVAL:
                        needs_user_input = True
                        break

                    # Add tool result to messages for next iteration
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call_id,
                            "content": result.to_message(),
                        }
                    )

                # If user input is needed, return early
                if needs_user_input:
                    return AgentResponse(
                        content=content,
                        tool_calls=tool_calls,
                        needs_user_input=True,
                        iteration=self._current_iteration,
                    )

            except Exception as e:
                return AgentResponse(
                    content="",
                    tool_calls=[],
                    error=str(e),
                    iteration=self._current_iteration,
                )

        # Max iterations reached
        return AgentResponse(
            content="Maximum iterations reached. Please provide more guidance.",
            tool_calls=[],
            needs_user_input=True,
            iteration=self._current_iteration,
        )

    async def run_interactive(self) -> None:
        # Run the agent in interactive mode in the CLI

        # Initialize
        if not await self.initialize():
            self._emit_status("Failed to initialize agent. Check configuration.")
            return

        self._emit_status("Agent ready. Type 'exit' to quit, 'status' for progress.")

        # Interactive loop
        while True:
            try:
                # Get user input (this would be handled by CLI)
                user_input = input("\n> ")

                if user_input.lower() == "exit":
                    break

                if user_input.lower() == "status":
                    summary = self.memory.get_progress_summary()
                    print(json.dumps(summary, indent=2))
                    continue

                # Process user input
                response = await self.process_message(user_input)

                if response.error:
                    print(f"Error: {response.error}")
                else:
                    print(f"\n{response.content}")

                    if response.needs_user_input:
                        print("\n[Agent needs your input to continue]")

            except KeyboardInterrupt:
                print("\n\nUser interrupted the conversation.")
            except Exception as e:
                raise e
            finally:
                await self.shutdown()

    def get_progress(self) -> Dict[str, Any]:
        # Get current progress summary
        return self.memory.get_progress_summary()
