# ============================================================
# DACLI AgentCore - Terraform Variables
# ============================================================

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "dacli"
}

# ── Container ─────────────────────────────────────────────────────────────────

variable "ecr_repository_name" {
  description = "ECR repository name for the DACLI agent container"
  type        = string
  default     = "dacli-agentcore"
}

variable "container_image_tag" {
  description = "Docker image tag to deploy"
  type        = string
  default     = "latest"
}

# ── AgentCore Runtime ─────────────────────────────────────────────────────────

variable "agent_runtime_name" {
  description = "Name of the AgentCore Runtime"
  type        = string
  default     = "dacli-agent-runtime"
}

variable "agent_description" {
  description = "Description of the DACLI agent"
  type        = string
  default     = "DACLI - Autonomous Data Engineering CLI Agent for Snowflake, GitHub, and Pinecone"
}

variable "agent_max_iterations" {
  description = "Maximum agent reasoning iterations"
  type        = number
  default     = 50
}

variable "agent_memory_window" {
  description = "Conversation memory window size"
  type        = number
  default     = 25
}

# ── AgentCore Memory ──────────────────────────────────────────────────────────

variable "memory_retention_days" {
  description = "Number of days to retain agent memory"
  type        = number
  default     = 30
}

variable "memory_strategies" {
  description = "Memory strategies to enable"
  type        = list(string)
  default     = ["SEMANTIC", "SUMMARY", "USER_PREFERENCE"]
}

# ── AgentCore Gateway ─────────────────────────────────────────────────────────

variable "gateway_name" {
  description = "Name of the AgentCore Gateway"
  type        = string
  default     = "dacli-tools-gateway"
}

variable "gateway_description" {
  description = "Description of the tools gateway"
  type        = string
  default     = "MCP-compatible gateway exposing DACLI tools (Snowflake, GitHub, Pinecone)"
}

# ── AgentCore Identity ────────────────────────────────────────────────────────

variable "identity_name" {
  description = "Name of the AgentCore Identity"
  type        = string
  default     = "dacli-agent-identity"
}

# ── Networking ────────────────────────────────────────────────────────────────

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24"]
}

# ── Secrets ───────────────────────────────────────────────────────────────────

variable "llm_provider" {
  description = "LLM provider: 'bedrock' for AgentCore (IAM auth), 'openrouter' for local dev"
  type        = string
  default     = "bedrock"
  sensitive   = false
}

variable "llm_model" {
  description = "LLM model identifier. For Bedrock: cross-region inference profile ID"
  type        = string
  default     = "us.nvidia.nemotron-nano-3-30d"
}

variable "llm_api_key" {
  description = "LLM API key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "github_token" {
  description = "GitHub personal access token"
  type        = string
  sensitive   = true
  default     = ""
}

variable "github_owner" {
  description = "GitHub repository owner"
  type        = string
  default     = "mouadja02"
}

variable "github_repo" {
  description = "GitHub repository name"
  type        = string
  default     = "datawarehouse_agent"
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
  sensitive   = true
  default     = ""
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
  default     = ""
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
  default     = ""
}

variable "pinecone_api_key" {
  description = "Pinecone API key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "openai_api_key" {
  description = "OpenAI API key for embeddings"
  type        = string
  sensitive   = true
  default     = ""
}

# ── Monitoring ────────────────────────────────────────────────────────────────

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "alarm_email" {
  description = "Email address for CloudWatch alarms"
  type        = string
  default     = "github@mj-dev.net"
}

variable "enable_xray" {
  description = "Enable AWS X-Ray tracing"
  type        = bool
  default     = true
}
