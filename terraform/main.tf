# ============================================================
# DACLI AgentCore - Core AWS Resources
# ECR, S3, Secrets Manager, AgentCore Runtime/Memory/Gateway/Identity
# ============================================================

# ── ECR Repository ────────────────────────────────────────────────────────────

resource "aws_ecr_repository" "dacli" {
  name                 = var.ecr_repository_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-ecr"
  }
}

resource "aws_ecr_lifecycle_policy" "dacli" {
  repository = aws_ecr_repository.dacli.name

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
    LLM_PROVIDER       = var.llm_provider
    LLM_MODEL          = var.llm_model
    LLM_BASE_URL       = var.aws_region  # boto3 region for bedrock-runtime client
    GITHUB_TOKEN       = var.github_token
    GITHUB_OWNER       = var.github_owner
    GITHUB_REPO        = var.github_repo
    GITHUB_BRANCH      = "main"
    SNOWFLAKE_ACCOUNT  = var.snowflake_account
    SNOWFLAKE_USER     = var.snowflake_user
    SNOWFLAKE_PASSWORD = var.snowflake_password
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
    SNOWFLAKE_DATABASE = "DATA_WAREHOUSE"
    SNOWFLAKE_SCHEMA   = "PUBLIC"
    SNOWFLAKE_ROLE     = "ACCOUNTADMIN"
    PINECONE_API_KEY   = var.pinecone_api_key
    PINECONE_INDEX     = "snowflake-docs"
    PINECONE_ENV       = "us-east-1"
    OPENAI_API_KEY     = var.openai_api_key  # still used for embeddings
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# ── AgentCore Runtime ─────────────────────────────────────────────────────────

resource "aws_bedrockagentcore_agent_runtime" "dacli" {
  agent_runtime_name = var.agent_runtime_name
  description        = var.agent_description

  agent_runtime_artifact {
    container_configuration {
      container_uri = "${aws_ecr_repository.dacli.repository_url}:${var.container_image_tag}"
    }
  }

  network_configuration {
    network_mode = "VPC"
    vpc_configuration {
      subnet_ids         = aws_subnet.private[*].id
      security_group_ids = [aws_security_group.agentcore_runtime.id]
    }
  }

  role_arn = aws_iam_role.agentcore_runtime.arn

  environment_variables = {
    DACLI_ENV            = var.environment
    AWS_REGION           = var.aws_region
    DACLI_SECRET_NAME    = aws_secretsmanager_secret.dacli_config.name
    LOG_LEVEL            = "INFO"
    AGENT_MAX_ITERATIONS = tostring(var.agent_max_iterations)
    AGENT_MEMORY_WINDOW  = tostring(var.agent_memory_window)
    OTEL_SERVICE_NAME    = "dacli-agentcore"
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:4317"
    AWS_XRAY_DAEMON_ADDRESS = "localhost:2000"
    S3_ARTIFACTS_BUCKET  = aws_s3_bucket.agent_artifacts.bucket
  }

  observability_configuration {
    enabled = true
  }

  depends_on = [
    aws_iam_role_policy.agentcore_runtime_policy,
    aws_ecr_repository.dacli,
  ]

  tags = {
    Name = "${var.project_name}-${var.environment}-runtime"
  }
}

# ── AgentCore Memory ──────────────────────────────────────────────────────────

resource "aws_bedrockagentcore_memory" "dacli" {
  name        = "${var.project_name}-${var.environment}-memory"
  description = "Persistent memory for DACLI agent - stores conversation context, tool results, and user preferences"

  memory_configuration {
    enabled = true

    retention_policy {
      retention_days = var.memory_retention_days
    }

    dynamic "memory_strategy" {
      for_each = var.memory_strategies
      content {
        strategy_type = memory_strategy.value
      }
    }
  }

  encryption_configuration {
    kms_key_arn = aws_kms_key.dacli.arn
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-memory"
  }
}

# ── AgentCore Identity ────────────────────────────────────────────────────────

resource "aws_bedrockagentcore_agent_identity" "dacli" {
  name        = var.identity_name
  description = "Identity for DACLI agent - manages OAuth flows and credential access"

  # Outbound credentials for external services
  credential_configuration {
    # GitHub OAuth
    credential_provider {
      name        = "github"
      type        = "API_KEY"
      secret_arn  = aws_secretsmanager_secret.dacli_config.arn
      secret_key  = "GITHUB_TOKEN"
    }

    # Snowflake credentials
    credential_provider {
      name        = "snowflake"
      type        = "USERNAME_PASSWORD"
      secret_arn  = aws_secretsmanager_secret.dacli_config.arn
      username_key = "SNOWFLAKE_USER"
      password_key = "SNOWFLAKE_PASSWORD"
    }

    # LLM API key
    credential_provider {
      name       = "llm_provider"
      type       = "API_KEY"
      secret_arn = aws_secretsmanager_secret.dacli_config.arn
      secret_key = "LLM_API_KEY"
    }
  }

  role_arn = aws_iam_role.agentcore_runtime.arn

  tags = {
    Name = "${var.project_name}-${var.environment}-identity"
  }
}

# ── AgentCore Gateway (Tools Gateway) ────────────────────────────────────────

resource "aws_bedrockagentcore_gateway" "dacli_tools" {
  name        = var.gateway_name
  description = var.gateway_description

  # Expose DACLI tools as MCP-compatible endpoints
  gateway_configuration {
    protocol = "MCP"

    # Snowflake tool endpoint
    tool_endpoint {
      name        = "snowflake-tools"
      description = "Snowflake SQL execution and validation tools"
      endpoint    = "${aws_bedrockagentcore_agent_runtime.dacli.endpoint}/tools/snowflake"

      tool_schema {
        openapi_schema = jsonencode({
          openapi = "3.0.0"
          info = {
            title   = "Snowflake Tools"
            version = "1.0.0"
          }
          paths = {
            "/execute_query" = {
              post = {
                operationId = "execute_snowflake_query"
                summary     = "Execute a SQL query on Snowflake"
                requestBody = {
                  required = true
                  content = {
                    "application/json" = {
                      schema = {
                        type = "object"
                        properties = {
                          query = { type = "string", description = "SQL query to execute" }
                        }
                        required = ["query"]
                      }
                    }
                  }
                }
              }
            }
          }
        })
      }
    }

    # GitHub tool endpoint
    tool_endpoint {
      name        = "github-tools"
      description = "GitHub repository management tools"
      endpoint    = "${aws_bedrockagentcore_agent_runtime.dacli.endpoint}/tools/github"

      tool_schema {
        openapi_schema = jsonencode({
          openapi = "3.0.0"
          info = {
            title   = "GitHub Tools"
            version = "1.0.0"
          }
          paths = {
            "/push_file" = {
              post = {
                operationId = "push_github_file"
                summary     = "Push a file to GitHub"
                requestBody = {
                  required = true
                  content = {
                    "application/json" = {
                      schema = {
                        type = "object"
                        properties = {
                          path    = { type = "string" }
                          content = { type = "string" }
                          message = { type = "string" }
                        }
                        required = ["path", "content", "message"]
                      }
                    }
                  }
                }
              }
            }
          }
        })
      }
    }
  }

  security_configuration {
    # OAuth 2.0 authorization for gateway access
    auth_type = "AWS_IAM"
  }

  role_arn = aws_iam_role.agentcore_runtime.arn

  tags = {
    Name = "${var.project_name}-${var.environment}-gateway"
  }

  depends_on = [aws_bedrockagentcore_agent_runtime.dacli]
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
