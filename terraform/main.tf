# ============================================================
# DACLI AgentCore - Core AWS Resources
# ECR, S3, Secrets Manager, AgentCore Runtime/Memory/Gateway/Identity
# ============================================================

# ── ECR Repository ────────────────────────────────────────────────────────────

data "aws_ecr_repository" "dacli" {
  name = var.ecr_repository_name
}

resource "aws_ecr_lifecycle_policy" "dacli" {
  repository = data.aws_ecr_repository.dacli.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 10 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 10
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# ── S3 Bucket for Agent Artifacts & State ────────────────────────────────────

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "agent_artifacts" {
  bucket = "${var.project_name}-${var.environment}-artifacts-${random_id.bucket_suffix.hex}"

  tags = {
    Name = "${var.project_name}-${var.environment}-artifacts"
  }
}

resource "aws_s3_bucket_versioning" "agent_artifacts" {
  bucket = aws_s3_bucket.agent_artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "agent_artifacts" {
  bucket = aws_s3_bucket.agent_artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "agent_artifacts" {
  bucket                  = aws_s3_bucket.agent_artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "agent_artifacts" {
  bucket = aws_s3_bucket.agent_artifacts.id

  rule {
    id     = "expire-old-state"
    status = "Enabled"

    filter {
      prefix = "state/"
    }

    expiration {
      days = 90
    }
  }

  rule {
    id     = "expire-old-history"
    status = "Enabled"

    filter {
      prefix = "history/"
    }

    expiration {
      days = var.memory_retention_days
    }
  }
}

# ── AWS Secrets Manager ───────────────────────────────────────────────────────

resource "aws_secretsmanager_secret" "dacli_config" {
  name                    = "dacli/config"
  description             = "DACLI agent configuration secrets"
  recovery_window_in_days = 7

  tags = {
    Name = "${var.project_name}-${var.environment}-config-secret"
  }
}

resource "aws_secretsmanager_secret_version" "dacli_config" {
  secret_id = aws_secretsmanager_secret.dacli_config.id

  secret_string = jsonencode({
    # LLM — AWS Bedrock (IAM auth, no API key needed)
    LLM_PROVIDER        = var.llm_provider
    LLM_MODEL           = var.llm_model
    LLM_BASE_URL        = var.aws_region # boto3 region for bedrock-runtime client
    GITHUB_TOKEN        = var.github_token
    GITHUB_OWNER        = var.github_owner
    GITHUB_REPO         = var.github_repo
    GITHUB_BRANCH       = "main"
    SNOWFLAKE_ACCOUNT   = var.snowflake_account
    SNOWFLAKE_USER      = var.snowflake_user
    SNOWFLAKE_PASSWORD  = var.snowflake_password
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
    SNOWFLAKE_DATABASE  = "DATA_WAREHOUSE"
    SNOWFLAKE_SCHEMA    = "PUBLIC"
    SNOWFLAKE_ROLE      = "ACCOUNTADMIN"
    PINECONE_API_KEY    = var.pinecone_api_key
    PINECONE_INDEX      = "snowflake-docs"
    PINECONE_ENV        = "us-east-1"
    OPENAI_API_KEY      = var.openai_api_key # still used for embeddings
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# ── AgentCore Runtime ─────────────────────────────────────────────────────────

# ── AgentCore Runtime ─────────────────────────────────────────────────────────

resource "aws_bedrockagentcore_agent_runtime" "dacli" {
  agent_runtime_name = replace(var.agent_runtime_name, "-", "_")
  role_arn           = aws_iam_role.agentcore_runtime.arn

  agent_runtime_artifact {
    container_configuration {
      container_uri = "${data.aws_ecr_repository.dacli.repository_url}:${var.container_image_tag}"
    }
  }

  network_configuration {
    network_mode = "VPC"
    network_mode_config {
      subnets         = aws_subnet.private[*].id
      security_groups = [aws_security_group.agentcore_runtime.id]
    }
  }

  environment_variables = {
    DACLI_ENV                   = var.environment
    AWS_REGION                  = var.aws_region
    DACLI_SECRET_NAME           = aws_secretsmanager_secret.dacli_config.name
    LOG_LEVEL                   = "INFO"
    AGENT_MAX_ITERATIONS        = tostring(var.agent_max_iterations)
    AGENT_MEMORY_WINDOW         = tostring(var.agent_memory_window)
    OTEL_SERVICE_NAME           = "dacli-agentcore"
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:4317"
    AWS_XRAY_DAEMON_ADDRESS     = "localhost:2000"
    S3_ARTIFACTS_BUCKET         = aws_s3_bucket.agent_artifacts.bucket
  }

  depends_on = [
    aws_iam_role_policy.agentcore_runtime_policy,
    data.aws_ecr_repository.dacli,
  ]
}

# ── AgentCore Memory ──────────────────────────────────────────────────────────

resource "aws_bedrockagentcore_memory" "dacli" {
  name                  = replace("${var.project_name}_${var.environment}_memory", "-", "_")
  description           = "Persistent memory for DACLI agent"
  event_expiry_duration = var.memory_retention_days
  encryption_key_arn    = aws_kms_key.dacli.arn
}

resource "aws_bedrockagentcore_memory_strategy" "strategies" {
  for_each = toset(var.memory_strategies)

  name        = replace("${var.project_name}_${var.environment}_${lower(each.value)}", "-", "_")
  memory_id   = aws_bedrockagentcore_memory.dacli.id
  type        = each.value
  namespaces  = ["default"]
  description = "Strategy ${each.value} for DACLI memory"
}

# ── AgentCore Identity ────────────────────────────────────────────────────────

resource "aws_bedrockagentcore_workload_identity" "dacli" {
  name = var.identity_name
}

# ── AgentCore Runtime Endpoint ──────────────────────────────────────────────

resource "aws_bedrockagentcore_agent_runtime_endpoint" "dacli" {
  name             = replace("${var.agent_runtime_name}_endpoint", "-", "_")
  agent_runtime_id = aws_bedrockagentcore_agent_runtime.dacli.agent_runtime_id
  description      = "Primary endpoint for DACLI agent runtime"
}

# ── AgentCore Gateway (Tools Gateway) ────────────────────────────────────────

resource "aws_bedrockagentcore_gateway" "dacli_tools" {
  name        = var.gateway_name
  description = var.gateway_description
  role_arn    = aws_iam_role.agentcore_runtime.arn

  authorizer_type = "AWS_IAM"
  protocol_type   = "MCP"

  tags = {
    Name = "${var.project_name}-${var.environment}-gateway"
  }

  depends_on = [aws_bedrockagentcore_agent_runtime.dacli]
}

resource "aws_bedrockagentcore_gateway_target" "snowflake_tools" {
  name               = "snowflake-tools"
  gateway_identifier = aws_bedrockagentcore_gateway.dacli_tools.gateway_id
  description        = "Snowflake tools endpoint"

  target_configuration {
    mcp {
      mcp_server {
        endpoint = "${aws_bedrockagentcore_agent_runtime_endpoint.dacli.agent_runtime_endpoint_arn}/tools/snowflake"
      }
    }
  }
}

resource "aws_bedrockagentcore_gateway_target" "github_tools" {
  name               = "github-tools"
  gateway_identifier = aws_bedrockagentcore_gateway.dacli_tools.gateway_id
  description        = "GitHub tools endpoint"

  target_configuration {
    mcp {
      mcp_server {
        endpoint = "${aws_bedrockagentcore_agent_runtime_endpoint.dacli.agent_runtime_endpoint_arn}/tools/github"
      }
    }
  }
}

# ── KMS Key for Encryption ────────────────────────────────────────────────────

resource "aws_kms_key" "dacli" {
  description             = "KMS key for DACLI AgentCore encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:${local.partition}:iam::${local.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow AgentCore Runtime"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.agentcore_runtime.arn
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey",
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name = "${var.project_name}-${var.environment}-kms"
  }
}

resource "aws_kms_alias" "dacli" {
  name          = "alias/${var.project_name}-${var.environment}"
  target_key_id = aws_kms_key.dacli.key_id
}
