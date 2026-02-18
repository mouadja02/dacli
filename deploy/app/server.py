"""
DACLI Agent - AWS AgentCore Runtime Server
==========================================
FastAPI server that exposes the DACLI agent as an AgentCore Runtime endpoint.
Handles:
  - Agent invocation (POST /invoke)
  - Health checks (GET /health)
  - Readiness probes (GET /ready)
  - OpenTelemetry tracing & metrics
  - AWS X-Ray integration
  - CloudWatch structured logging
"""

import os
import json
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import boto3
import structlog
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

# ── Observability Setup ───────────────────────────────────────────────────────
from deploy.app.observability import (
    setup_telemetry,
    get_tracer,
    record_agent_invocation,
    record_tool_call,
    record_token_usage,
    record_thinking_step,
)

# ── DACLI Core ────────────────────────────────────────────────────────────────
from config.settings import Settings
from config.tool_registry import ToolsSettings
from core.agent import DACLI
from core.memory import AgentMemory

# ── Logging ───────────────────────────────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
logger = structlog.get_logger(__name__)

# ── Pydantic Models ───────────────────────────────────────────────────────────


class InvokeRequest(BaseModel):
    """AgentCore Runtime invocation payload."""

    session_id: Optional[str] = Field(
        default=None, description="Session ID for memory continuity"
    )
    message: str = Field(..., description="User message to process")
    stream: bool = Field(default=False, description="Enable streaming response")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Optional metadata"
    )


class InvokeResponse(BaseModel):
    """AgentCore Runtime response payload."""

    session_id: str
    request_id: str
    content: str
    tool_calls: list = Field(default_factory=list)
    thinking: Optional[str] = None
    iteration: int = 0
    token_usage: Dict[str, int] = Field(default_factory=dict)
    duration_ms: float = 0.0
    status: str = "success"
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str = "1.0.0"
    environment: str
    agent_initialized: bool


# ── Global State ──────────────────────────────────────────────────────────────
_agent_cache: Dict[str, DACLI] = {}  # session_id -> DACLI instance
_settings: Optional[Settings] = None
_tools_settings: Optional[ToolsSettings] = None


def _load_settings() -> Settings:
    """Load settings from AWS Secrets Manager or environment variables."""
    global _settings, _tools_settings

    if _settings:
        return _settings

    # Try to load secrets from AWS Secrets Manager
    secret_name = os.environ.get("DACLI_SECRET_NAME", "dacli/config")
    region = os.environ.get("AWS_REGION", "us-east-1")

    config_data = {}
    try:
        client = boto3.client("secretsmanager", region_name=region)
        response = client.get_secret_value(SecretId=secret_name)
        config_data = json.loads(response["SecretString"])
        logger.info("secrets_loaded", source="aws_secrets_manager", secret=secret_name)
    except Exception as e:
        logger.warning(
            "secrets_load_failed", error=str(e), fallback="environment_variables"
        )

    # Build settings from env vars (fallback / override)
    settings_dict = {
        "llm": {
            "provider": config_data.get(
                "LLM_PROVIDER", os.environ.get("LLM_PROVIDER", "openrouter")
            ),
            "model": config_data.get(
                "LLM_MODEL", os.environ.get("LLM_MODEL", "x-ai/grok-4.1-fast")
            ),
            "api_key": config_data.get(
                "LLM_API_KEY", os.environ.get("LLM_API_KEY", "")
            ),
            "base_url": config_data.get(
                "LLM_BASE_URL",
                os.environ.get("LLM_BASE_URL", "https://openrouter.ai/api/v1"),
            ),
            "temperature": float(
                config_data.get(
                    "LLM_TEMPERATURE", os.environ.get("LLM_TEMPERATURE", "0.1")
                )
            ),
            "max_tokens": int(
                config_data.get(
                    "LLM_MAX_TOKENS", os.environ.get("LLM_MAX_TOKENS", "4096")
                )
            ),
            "timeout": int(
                config_data.get("LLM_TIMEOUT", os.environ.get("LLM_TIMEOUT", "120"))
            ),
        },
        "github": {
            "token": config_data.get(
                "GITHUB_TOKEN", os.environ.get("GITHUB_TOKEN", "")
            ),
            "owner": config_data.get(
                "GITHUB_OWNER", os.environ.get("GITHUB_OWNER", "")
            ),
            "repo": config_data.get("GITHUB_REPO", os.environ.get("GITHUB_REPO", "")),
            "branch": config_data.get(
                "GITHUB_BRANCH", os.environ.get("GITHUB_BRANCH", "main")
            ),
        },
        "snowflake": {
            "account": config_data.get(
                "SNOWFLAKE_ACCOUNT", os.environ.get("SNOWFLAKE_ACCOUNT", "")
            ),
            "user": config_data.get(
                "SNOWFLAKE_USER", os.environ.get("SNOWFLAKE_USER", "")
            ),
            "password": config_data.get(
                "SNOWFLAKE_PASSWORD", os.environ.get("SNOWFLAKE_PASSWORD", "")
            ),
            "warehouse": config_data.get(
                "SNOWFLAKE_WAREHOUSE",
                os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            ),
            "database": config_data.get(
                "SNOWFLAKE_DATABASE",
                os.environ.get("SNOWFLAKE_DATABASE", "DATA_WAREHOUSE"),
            ),
            "schema": config_data.get(
                "SNOWFLAKE_SCHEMA", os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
            ),
            "role": config_data.get(
                "SNOWFLAKE_ROLE", os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
            ),
            "timeout": 60,
            "query_timeout": 300,
        },
        "pinecone": {
            "api_key": config_data.get(
                "PINECONE_API_KEY", os.environ.get("PINECONE_API_KEY", "")
            ),
            "index_name": config_data.get(
                "PINECONE_INDEX", os.environ.get("PINECONE_INDEX", "snowflake-docs")
            ),
            "environment": config_data.get(
                "PINECONE_ENV", os.environ.get("PINECONE_ENV", "us-east-1")
            ),
            "top_k": 5,
            "include_metadata": True,
        },
        "embeddings": {
            "provider": "openai",
            "api_key": config_data.get(
                "OPENAI_API_KEY", os.environ.get("OPENAI_API_KEY", "")
            ),
            "model": "text-embedding-3-small",
        },
        "agent": {
            "max_iterations": int(os.environ.get("AGENT_MAX_ITERATIONS", "50")),
            "memory_window": int(os.environ.get("AGENT_MEMORY_WINDOW", "25")),
            "auto_approve_safe_ops": True,
            "confirm_data_loads": False,
            "confirm_destructive_ops": False,
            "step_by_step_mode": False,
            "log_level": os.environ.get("LOG_LEVEL", "INFO"),
            "save_history": True,
            "history_path": "/app/.dacli/history/",
            "state_path": "/app/.dacli/state/",
        },
        "tools": {
            "setup_completed": True,
            "snowflake": {
                "enabled": True,
                "operations": {
                    "execute_snowflake_query": True,
                    "validate_snowflake_connection": True,
                },
            },
            "github": {
                "enabled": True,
                "operations": {
                    "list_github_directory": True,
                    "read_github_file": True,
                    "push_github_file": True,
                    "delete_github_file": True,
                    "trigger_github_workflow": True,
                    "list_github_workflow_runs": True,
                    "get_github_workflow_run": True,
                    "get_github_workflow_run_jobs": True,
                },
            },
            "pinecone": {
                "enabled": True,
                "operations": {"search_snowflake_docs": True},
            },
        },
    }

    _settings = Settings(**settings_dict)
    _tools_settings = ToolsSettings(**settings_dict.get("tools", {}))
    return _settings


async def _get_or_create_agent(session_id: str) -> DACLI:
    """Get existing agent for session or create a new one."""
    if session_id in _agent_cache:
        return _agent_cache[session_id]

    settings = _load_settings()

    # Observability callbacks
    def on_status(msg: str):
        logger.info("agent_status", session_id=session_id, message=msg)

    def on_tool_start(tool_name: str, args: Dict):
        logger.info("tool_start", session_id=session_id, tool=tool_name, args=args)
        record_tool_call(tool_name=tool_name, session_id=session_id, status="started")

    def on_tool_end(tool_name: str, result):
        status = "success" if result.success else "error"
        logger.info(
            "tool_end",
            session_id=session_id,
            tool=tool_name,
            status=status,
            duration_ms=result.execution_time_ms,
        )
        record_tool_call(
            tool_name=tool_name,
            session_id=session_id,
            status=status,
            duration_ms=result.execution_time_ms,
        )

    memory = AgentMemory(
        state_path=settings.agent.state_path,
        history_path=settings.agent.history_path,
        memory_window=settings.agent.memory_window,
    )

    agent = DACLI(
        settings=settings,
        memory=memory,
        on_status_update=on_status,
        on_tool_start=on_tool_start,
        on_tool_end=on_tool_end,
    )

    await agent.initialize()
    _agent_cache[session_id] = agent
    logger.info("agent_created", session_id=session_id)
    return agent


# ── FastAPI Lifespan ──────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown."""
    logger.info(
        "dacli_agentcore_starting",
        version="1.0.0",
        env=os.environ.get("DACLI_ENV", "development"),
    )

    # Setup OpenTelemetry
    setup_telemetry(
        service_name="dacli-agentcore",
        service_version="1.0.0",
        environment=os.environ.get("DACLI_ENV", "development"),
    )

    # Pre-warm a default agent session
    try:
        await _get_or_create_agent("default")
        logger.info("agent_warmup_complete")
    except Exception as e:
        logger.warning("agent_warmup_failed", error=str(e))

    yield

    # Shutdown: clean up all agent sessions
    logger.info("dacli_agentcore_shutting_down", active_sessions=len(_agent_cache))
    for session_id, agent in _agent_cache.items():
        try:
            await agent.shutdown()
        except Exception as e:
            logger.error("agent_shutdown_error", session_id=session_id, error=str(e))
    _agent_cache.clear()


# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="DACLI AgentCore Runtime",
    description="DACLI Data Engineering Agent deployed on AWS AgentCore",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Middleware ────────────────────────────────────────────────────────────────


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """Log all requests with timing and request ID."""
    request_id = str(uuid.uuid4())
    start_time = time.time()

    logger.info(
        "request_start",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
    )

    response = await call_next(request)
    duration_ms = (time.time() - start_time) * 1000

    logger.info(
        "request_end",
        request_id=request_id,
        status_code=response.status_code,
        duration_ms=round(duration_ms, 2),
    )

    response.headers["X-Request-ID"] = request_id
    response.headers["X-Duration-Ms"] = str(round(duration_ms, 2))
    return response


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Health check endpoint for AgentCore and load balancers."""
    return HealthResponse(
        status="healthy",
        environment=os.environ.get("DACLI_ENV", "development"),
        agent_initialized=len(_agent_cache) > 0,
    )


@app.get("/ready", tags=["System"])
async def readiness_check():
    """Readiness probe - checks if agent is ready to serve requests."""
    if not _agent_cache:
        raise HTTPException(status_code=503, detail="Agent not yet initialized")
    return {"status": "ready", "sessions": len(_agent_cache)}


@app.get("/metrics", tags=["Observability"])
async def prometheus_metrics():
    """Prometheus metrics endpoint."""
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from fastapi.responses import Response

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/invoke", response_model=InvokeResponse, tags=["Agent"])
async def invoke_agent(request: InvokeRequest, background_tasks: BackgroundTasks):
    """
    Main AgentCore Runtime invocation endpoint.
    Processes a user message through the DACLI agent and returns the response.
    """
    request_id = str(uuid.uuid4())
    session_id = request.session_id or "default"
    start_time = time.time()

    logger.info(
        "invoke_start",
        request_id=request_id,
        session_id=session_id,
        message_length=len(request.message),
        stream=request.stream,
    )

    tracer = get_tracer()

    with tracer.start_as_current_span("dacli.invoke") as span:
        span.set_attribute("session.id", session_id)
        span.set_attribute("request.id", request_id)
        span.set_attribute("message.length", len(request.message))

        try:
            agent = await _get_or_create_agent(session_id)

            # Record invocation metric
            record_agent_invocation(session_id=session_id, request_id=request_id)

            # Process message
            with tracer.start_as_current_span("dacli.process_message") as msg_span:
                response = await agent.process_message(request.message)
                msg_span.set_attribute("iterations", response.iteration)
                msg_span.set_attribute("tool_calls_count", len(response.tool_calls))

            duration_ms = (time.time() - start_time) * 1000

            # Record thinking steps
            if response.thinking:
                record_thinking_step(
                    session_id=session_id,
                    thinking=response.thinking,
                    iteration=response.iteration,
                )

            # Estimate token usage (actual tokens from LLM response if available)
            token_usage = {
                "input_tokens": len(request.message.split()) * 2,  # rough estimate
                "output_tokens": len(response.content.split()) * 2,
                "total_tokens": 0,
            }
            token_usage["total_tokens"] = (
                token_usage["input_tokens"] + token_usage["output_tokens"]
            )
            record_token_usage(session_id=session_id, **token_usage)

            span.set_attribute("response.iterations", response.iteration)
            span.set_attribute("response.duration_ms", duration_ms)

            logger.info(
                "invoke_complete",
                request_id=request_id,
                session_id=session_id,
                iterations=response.iteration,
                tool_calls=len(response.tool_calls),
                duration_ms=round(duration_ms, 2),
            )

            return InvokeResponse(
                session_id=session_id,
                request_id=request_id,
                content=response.content,
                tool_calls=response.tool_calls,
                thinking=response.thinking,
                iteration=response.iteration,
                token_usage=token_usage,
                duration_ms=round(duration_ms, 2),
                status="success",
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            span.record_exception(e)
            logger.error(
                "invoke_error",
                request_id=request_id,
                session_id=session_id,
                error=str(e),
                duration_ms=round(duration_ms, 2),
            )
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/reset", tags=["Agent"])
async def reset_session(session_id: str):
    """Reset an agent session (clear memory and state)."""
    if session_id in _agent_cache:
        agent = _agent_cache.pop(session_id)
        await agent.shutdown()
        logger.info("session_reset", session_id=session_id)
    return {"status": "reset", "session_id": session_id}


@app.get("/sessions", tags=["Agent"])
async def list_sessions():
    """List all active agent sessions."""
    return {
        "active_sessions": list(_agent_cache.keys()),
        "count": len(_agent_cache),
    }


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "deploy.app.server:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8080")),
        log_level=os.environ.get("LOG_LEVEL", "info").lower(),
        workers=1,  # Single worker for stateful agent sessions
        loop="asyncio",
    )
