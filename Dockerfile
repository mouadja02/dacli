# ============================================================
# DACLI Agent - AWS AgentCore Runtime Container
# Multi-stage build optimized for ARM64 (AWS Graviton)
# ============================================================

# ── Stage 1: Builder ─────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies first (layer caching)
COPY requirements.txt requirements-aws.txt ./
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --target=/build/deps \
    -r requirements.txt \
    -r requirements-aws.txt

# ── Stage 2: Runtime ─────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL maintainer="Mouad Jaouhari <github@mj-dev.net>"
LABEL version="1.0.0"
LABEL description="DACLI Data Engineering Agent - AWS AgentCore Runtime"

# Security: non-root user
RUN groupadd -r dacli && useradd -r -g dacli -d /app -s /sbin/nologin dacli

WORKDIR /app

# Install runtime system dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /build/deps /usr/local/lib/python3.11/site-packages/

# Copy application source
COPY --chown=dacli:dacli . .

# Create runtime directories
RUN mkdir -p /app/.dacli/state /app/.dacli/history /app/logs && \
    chown -R dacli:dacli /app

# Environment defaults (overridden by AgentCore secrets/env)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    LOG_LEVEL=INFO \
    DACLI_ENV=development \
    PORT=8080

# Switch to non-root user
USER dacli

# Health check for AgentCore
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

EXPOSE ${PORT}

# AgentCore Runtime entrypoint
CMD ["python", "-m", "deploy.app.server"]
