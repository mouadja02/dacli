# 🤖 DACLI AgentCore - AWS Deployment Guide

Deploy the **DACLI Data Engineering Agent** to **AWS AgentCore** using Terraform and GitHub Actions.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AWS AgentCore Platform                        │
│                                                                      │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │  AgentCore       │    │  AgentCore       │    │  AgentCore   │  │
│  │  Runtime         │◄──►│  Memory          │    │  Identity    │  │
│  │  (DACLI Agent)   │    │  (Semantic/      │    │  (OAuth +    │  │
│  │  [ARM64 Docker]  │    │   Summary/Prefs) │    │   API Keys)  │  │
│  └────────┬─────────┘    └──────────────────┘    └──────────────┘  │
│           │                                                          │
│           ▼                                                          │
│  ┌──────────────────┐                                               │
│  │  AgentCore       │  MCP-compatible tool endpoints                │
│  │  Gateway         │  ├── Snowflake Tools                          │
│  │  (Tools Gateway) │  ├── GitHub Tools                             │
│  └──────────────────┘  └── Pinecone/RAG Tools                      │
└─────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Observability Stack                              │
│                                                                      │
│  CloudWatch Logs          CloudWatch Metrics       AWS X-Ray        │
│  ├── /dacli/runtime       ├── DACLI/AgentCore      ├── Traces       │
│  ├── /dacli/invocations   │   ├── AgentInvocations │   ├── Spans    │
│  ├── /dacli/tool-calls    │   ├── ToolCallErrors   │   └── Segments │
│  ├── /dacli/thinking      │   ├── TotalTokens      │                │
│  └── /dacli/tokens        │   └── AgentErrors      │                │
│                                                                      │
│  CloudWatch Dashboard     SNS Alarms               OpenTelemetry    │
│  └── Live agent view      ├── High error rate       └── OTLP/ADOT   │
│                           ├── Tool failures                          │
│                           └── Token budget                           │
└─────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| AWS CLI | ≥ 2.x | AWS authentication |
| Terraform | ≥ 1.6.0 | Infrastructure as Code |
| Docker | ≥ 24.x | Container build |
| Python | ≥ 3.11 | Agent runtime |

## Quick Start

### 1. Bootstrap AWS Backend (one-time)

```bash
# Configure AWS credentials
aws configure

# Run bootstrap script (creates S3 state bucket + DynamoDB lock table + GitHub OIDC)
bash terraform/bootstrap.sh us-east-1 dev
```

### 2. Configure Secrets

```bash
# Copy example tfvars
cp terraform/terraform.tfvars.example terraform/terraform.tfvars

# Edit non-sensitive values in terraform.tfvars
# Set secrets as environment variables (never in files!)
export TF_VAR_llm_api_key="sk-or-v1-..."
export TF_VAR_github_token="ghp_..."
export TF_VAR_snowflake_account="FBQDAFG-OHC90635"
export TF_VAR_snowflake_user="mouad"
export TF_VAR_snowflake_password="..."
export TF_VAR_pinecone_api_key="pcsk_..."
export TF_VAR_openai_api_key="sk-proj-..."
```

### 3. Deploy Infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### 4. Build & Push Docker Image

```bash
# Get ECR URL from Terraform output
ECR_URL=$(terraform output -raw ecr_repository_url)

# Authenticate Docker to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin $ECR_URL

# Build for ARM64 (AWS Graviton)
docker buildx build \
  --platform linux/arm64 \
  --tag $ECR_URL:latest \
  --push .
```

## GitHub Actions Setup

### Required Secrets

Add these secrets in your GitHub repository settings (`Settings > Secrets > Actions`):

| Secret | Description |
|--------|-------------|
| `AWS_GITHUB_ACTIONS_ROLE_ARN` | IAM role ARN from `terraform output github_actions_role_arn` |
| `LLM_API_KEY` | OpenRouter / OpenAI API key |
| `GH_TOKEN` | GitHub Personal Access Token |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `PINECONE_API_KEY` | Pinecone API key |
| `OPENAI_API_KEY` | OpenAI API key (for embeddings) |

### Deployment Triggers

| Event | Action |
|-------|--------|
| Push to `main` | Full deploy (build → terraform apply → smoke test) |
| Pull Request | Build + Terraform plan (posted as PR comment) |
| Manual dispatch | Deploy to any environment |

## Monitoring & Observability

### CloudWatch Dashboard

Access the live dashboard:
```bash
terraform output cloudwatch_dashboard_url
```

The dashboard shows:
- **Agent Invocations** - Request rate over time
- **Error Rate** - Agent errors + tool call failures
- **Token Consumption** - Input/output/total tokens per hour
- **Agent Thinking Steps** - Live log of agent reasoning
- **Tool Calls** - Real-time tool execution log with duration
- **Bedrock Metrics** - Native model invocation latency

### Log Groups

| Log Group | Content |
|-----------|---------|
| `/dacli/agentcore/runtime` | General agent logs |
| `/dacli/agentcore/invocations` | Per-invocation records |
| `/dacli/agentcore/tool-calls` | Tool execution with timing |
| `/dacli/agentcore/thinking` | Agent reasoning steps |
| `/dacli/agentcore/tokens` | Token usage per session |
| `/aws/bedrock/model-invocations` | Raw Bedrock model calls |

### CloudWatch Alarms

| Alarm | Threshold | Action |
|-------|-----------|--------|
| High Error Rate | > 10 errors / 5min | SNS email |
| Tool Call Failures | > 5 failures / 5min | SNS email |
| Token Budget | > 1M tokens / hour | SNS email |

### X-Ray Tracing

Every agent invocation creates an X-Ray trace with spans for:
- `dacli.invoke` - Full invocation
- `dacli.process_message` - LLM reasoning loop
- Individual tool calls

### Prometheus Metrics

Available at `/metrics` endpoint:
```
dacli_agent_invocations_total{session_id, status}
dacli_tool_calls_total{tool_name, status}
dacli_tool_duration_ms{tool_name}
dacli_tokens_total{session_id, token_type}
dacli_active_sessions
dacli_thinking_steps_total{session_id}
```

## AgentCore Components

### Runtime
The DACLI agent runs as a containerized FastAPI server on AWS Graviton (ARM64). It exposes:
- `POST /invoke` - Process agent messages
- `GET /health` - Health check
- `GET /ready` - Readiness probe
- `GET /metrics` - Prometheus metrics

### Memory
Multi-strategy persistent memory:
- **Semantic** - Vector-based context retrieval
- **Summary** - Compressed conversation summaries
- **User Preference** - Learned user preferences

### Identity
Manages credentials for external services:
- GitHub API token
- Snowflake username/password
- LLM provider API key

### Gateway (Tools Gateway)
MCP-compatible endpoint exposing DACLI tools:
- Snowflake SQL execution
- GitHub file operations
- Pinecone vector search

## File Structure

```
dacli/
├── .github/
│   └── workflows/
│       └── deploy.yml          # GitHub Actions CI/CD pipeline
├── deploy/
│   └── app/
│       ├── server.py           # FastAPI AgentCore Runtime server
│       └── observability.py    # OpenTelemetry + X-Ray + Prometheus
├── terraform/
│   ├── providers.tf            # AWS provider + S3 backend
│   ├── variables.tf            # All input variables
│   ├── main.tf                 # ECR, S3, Secrets, AgentCore resources
│   ├── networking.tf           # VPC, subnets, security groups
│   ├── iam.tf                  # IAM roles (Runtime + GitHub Actions OIDC)
│   ├── monitoring.tf           # CloudWatch, X-Ray, SNS alarms
│   ├── outputs.tf              # Resource outputs
│   ├── bootstrap.sh            # One-time backend setup script
│   ├── terraform.tfvars        # Your values (gitignored)
│   └── terraform.tfvars.example # Template (safe to commit)
├── Dockerfile                  # Multi-stage ARM64 container
├── .dockerignore               # Build context exclusions
├── requirements-aws.txt        # AWS-specific dependencies
└── requirements.txt            # Core dependencies
```

## Cost Estimation

| Service | Estimated Monthly Cost |
|---------|----------------------|
| AgentCore Runtime | ~$0.10/1000 invocations |
| AgentCore Memory | ~$0.10/GB stored |
| ECR | ~$0.10/GB stored |
| CloudWatch Logs | ~$0.50/GB ingested |
| NAT Gateway | ~$32/month |
| Secrets Manager | ~$0.40/secret |
| **Total (light usage)** | **~$40-60/month** |

> 💡 Disable NAT Gateway and use VPC endpoints to reduce costs significantly.
