"""
DACLI AgentCore - Observability Module
=======================================
OpenTelemetry + AWS X-Ray + CloudWatch integration for:
  - Agent invocation tracing
  - Tool call spans & metrics
  - Token consumption tracking
  - Thinking step logging
  - Prometheus metrics export
"""

import os
import time
import logging
from typing import Optional

# OpenTelemetry
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, ConsoleMetricExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.semconv.resource import ResourceAttributes

# OTLP exporters (for CloudWatch / X-Ray ADOT collector)
try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    OTLP_AVAILABLE = True
except ImportError:
    OTLP_AVAILABLE = False

# Prometheus
try:
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from prometheus_client import start_http_server, Counter, Histogram, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# AWS X-Ray
try:
    from aws_xray_sdk.core import xray_recorder, patch_all
    from aws_xray_sdk.core.context import Context
    XRAY_AVAILABLE = True
except ImportError:
    XRAY_AVAILABLE = False

import structlog

logger = structlog.get_logger(__name__)

# ── Global Tracer / Meter ─────────────────────────────────────────────────────
_tracer: Optional[trace.Tracer] = None
_meter: Optional[metrics.Meter] = None

# ── Prometheus Metrics ────────────────────────────────────────────────────────
if PROMETHEUS_AVAILABLE:
    AGENT_INVOCATIONS = Counter(
        "dacli_agent_invocations_total",
        "Total number of agent invocations",
        ["session_id", "status"],
    )
    TOOL_CALLS = Counter(
        "dacli_tool_calls_total",
        "Total number of tool calls",
        ["tool_name", "status"],
    )
    TOOL_DURATION = Histogram(
        "dacli_tool_duration_ms",
        "Tool call duration in milliseconds",
        ["tool_name"],
        buckets=[10, 50, 100, 500, 1000, 5000, 10000, 30000],
    )
    TOKEN_USAGE = Counter(
        "dacli_tokens_total",
        "Total tokens consumed",
        ["session_id", "token_type"],
    )
    ACTIVE_SESSIONS = Gauge(
        "dacli_active_sessions",
        "Number of active agent sessions",
    )
    THINKING_STEPS = Counter(
        "dacli_thinking_steps_total",
        "Total thinking steps recorded",
        ["session_id"],
    )
    AGENT_ITERATIONS = Histogram(
        "dacli_agent_iterations",
        "Number of iterations per invocation",
        ["session_id"],
        buckets=[1, 2, 5, 10, 20, 50, 100],
    )


def setup_telemetry(
    service_name: str = "dacli-agentcore",
    service_version: str = "1.0.0",
    environment: str = "development",
) -> None:
    """
    Configure OpenTelemetry with OTLP (AWS ADOT Collector) and Prometheus exporters.
    Falls back to console exporter if OTLP is unavailable.
    """
    global _tracer, _meter

    # Resource attributes
    resource = Resource.create({
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version,
        ResourceAttributes.DEPLOYMENT_ENVIRONMENT: environment,
        "cloud.provider": "aws",
        "cloud.region": os.environ.get("AWS_REGION", "us-east-1"),
        "agent.framework": "dacli",
    })

    # ── Tracer Provider ───────────────────────────────────────────────────────
    tracer_provider = TracerProvider(resource=resource)

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

    if OTLP_AVAILABLE:
        try:
            otlp_span_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
            tracer_provider.add_span_processor(BatchSpanProcessor(otlp_span_exporter))
            logger.info("otlp_tracer_configured", endpoint=otlp_endpoint)
        except Exception as e:
            logger.warning("otlp_tracer_failed", error=str(e), fallback="console")
            tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    else:
        tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(tracer_provider)
    _tracer = trace.get_tracer(service_name, service_version)

    # ── Meter Provider ────────────────────────────────────────────────────────
    metric_readers = []

    if PROMETHEUS_AVAILABLE:
        try:
            prometheus_reader = PrometheusMetricReader()
            metric_readers.append(prometheus_reader)
            logger.info("prometheus_metrics_configured")
        except Exception as e:
            logger.warning("prometheus_metrics_failed", error=str(e))

    if OTLP_AVAILABLE:
        try:
            otlp_metric_exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
            metric_readers.append(PeriodicExportingMetricReader(otlp_metric_exporter, export_interval_millis=30000))
            logger.info("otlp_metrics_configured", endpoint=otlp_endpoint)
        except Exception as e:
            logger.warning("otlp_metrics_failed", error=str(e))

    if not metric_readers:
        metric_readers.append(PeriodicExportingMetricReader(ConsoleMetricExporter()))

    meter_provider = MeterProvider(resource=resource, metric_readers=metric_readers)
    metrics.set_meter_provider(meter_provider)
    _meter = metrics.get_meter(service_name, service_version)

    # ── AWS X-Ray ─────────────────────────────────────────────────────────────
    if XRAY_AVAILABLE and os.environ.get("AWS_XRAY_DAEMON_ADDRESS"):
        try:
            xray_recorder.configure(
                service=service_name,
                daemon_address=os.environ.get("AWS_XRAY_DAEMON_ADDRESS", "localhost:2000"),
                context_missing="LOG_ERROR",
            )
            patch_all()
            logger.info("xray_configured", daemon=os.environ.get("AWS_XRAY_DAEMON_ADDRESS"))
        except Exception as e:
            logger.warning("xray_configuration_failed", error=str(e))

    logger.info("telemetry_setup_complete", service=service_name, version=service_version, env=environment)


def get_tracer() -> trace.Tracer:
    """Get the configured OpenTelemetry tracer."""
    global _tracer
    if _tracer is None:
        _tracer = trace.get_tracer("dacli-agentcore")
    return _tracer


def get_meter() -> metrics.Meter:
    """Get the configured OpenTelemetry meter."""
    global _meter
    if _meter is None:
        _meter = metrics.get_meter("dacli-agentcore")
    return _meter


# ── Metric Recording Helpers ──────────────────────────────────────────────────

def record_agent_invocation(session_id: str, request_id: str, status: str = "started") -> None:
    """Record an agent invocation metric."""
    if PROMETHEUS_AVAILABLE:
        AGENT_INVOCATIONS.labels(session_id=session_id, status=status).inc()

    logger.info(
        "metric.agent_invocation",
        session_id=session_id,
        request_id=request_id,
        status=status,
    )


def record_tool_call(
    tool_name: str,
    session_id: str,
    status: str,
    duration_ms: float = 0.0,
) -> None:
    """Record a tool call metric with duration."""
    if PROMETHEUS_AVAILABLE:
        TOOL_CALLS.labels(tool_name=tool_name, status=status).inc()
        if duration_ms > 0:
            TOOL_DURATION.labels(tool_name=tool_name).observe(duration_ms)

    logger.info(
        "metric.tool_call",
        tool_name=tool_name,
        session_id=session_id,
        status=status,
        duration_ms=duration_ms,
    )


def record_token_usage(
    session_id: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    total_tokens: int = 0,
) -> None:
    """Record token consumption metrics."""
    if PROMETHEUS_AVAILABLE:
        TOKEN_USAGE.labels(session_id=session_id, token_type="input").inc(input_tokens)
        TOKEN_USAGE.labels(session_id=session_id, token_type="output").inc(output_tokens)
        TOKEN_USAGE.labels(session_id=session_id, token_type="total").inc(total_tokens)

    logger.info(
        "metric.token_usage",
        session_id=session_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
    )


def record_thinking_step(session_id: str, thinking: str, iteration: int) -> None:
    """Record an agent thinking step for observability."""
    if PROMETHEUS_AVAILABLE:
        THINKING_STEPS.labels(session_id=session_id).inc()

    logger.info(
        "metric.thinking_step",
        session_id=session_id,
        iteration=iteration,
        thinking_length=len(thinking),
        thinking_preview=thinking[:200] + "..." if len(thinking) > 200 else thinking,
    )


def update_active_sessions(count: int) -> None:
    """Update the active sessions gauge."""
    if PROMETHEUS_AVAILABLE:
        ACTIVE_SESSIONS.set(count)
