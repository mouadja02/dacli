# ============================================================
# DACLI AgentCore - Monitoring & Observability
# CloudWatch Dashboards, Alarms, Log Groups, X-Ray
# ============================================================

# ── CloudWatch Log Groups ─────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "agent_runtime" {
  name              = "/dacli/agentcore/runtime"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-${var.environment}-runtime-logs"
  }
}

resource "aws_cloudwatch_log_group" "agent_invocations" {
  name              = "/dacli/agentcore/invocations"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-${var.environment}-invocations-logs"
  }
}

resource "aws_cloudwatch_log_group" "agent_tool_calls" {
  name              = "/dacli/agentcore/tool-calls"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-${var.environment}-tool-calls-logs"
  }
}

resource "aws_cloudwatch_log_group" "agent_thinking" {
  name              = "/dacli/agentcore/thinking"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-${var.environment}-thinking-logs"
  }
}

resource "aws_cloudwatch_log_group" "agent_tokens" {
  name              = "/dacli/agentcore/tokens"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-${var.environment}-tokens-logs"
  }
}

# ── CloudWatch Metric Filters ─────────────────────────────────────────────────

resource "aws_cloudwatch_log_metric_filter" "agent_errors" {
  name           = "${var.project_name}-${var.environment}-agent-errors"
  log_group_name = aws_cloudwatch_log_group.agent_runtime.name
  pattern        = "{ $.level = \"error\" }"

  metric_transformation {
    name          = "AgentErrors"
    namespace     = "DACLI/AgentCore"
    value         = "1"
    default_value = "0"
    unit          = "Count"
  }
}

resource "aws_cloudwatch_log_metric_filter" "tool_call_errors" {
  name           = "${var.project_name}-${var.environment}-tool-errors"
  log_group_name = aws_cloudwatch_log_group.agent_tool_calls.name
  pattern        = "{ $.status = \"error\" }"

  metric_transformation {
    name          = "ToolCallErrors"
    namespace     = "DACLI/AgentCore"
    value         = "1"
    default_value = "0"
    unit          = "Count"
  }
}

resource "aws_cloudwatch_log_metric_filter" "invocation_count" {
  name           = "${var.project_name}-${var.environment}-invocations"
  log_group_name = aws_cloudwatch_log_group.agent_invocations.name
  pattern        = "{ $.event = \"invoke_start\" }"

  metric_transformation {
    name          = "AgentInvocations"
    namespace     = "DACLI/AgentCore"
    value         = "1"
    default_value = "0"
    unit          = "Count"
  }
}

resource "aws_cloudwatch_log_metric_filter" "token_usage" {
  name           = "${var.project_name}-${var.environment}-tokens"
  log_group_name = aws_cloudwatch_log_group.agent_tokens.name
  pattern        = "{ $.event = \"metric.token_usage\" }"

  metric_transformation {
    name          = "TotalTokens"
    namespace     = "DACLI/AgentCore"
    value         = "$.total_tokens"
    default_value = "0"
    unit          = "Count"
  }
}

# ── SNS Topic for Alarms ──────────────────────────────────────────────────────

resource "aws_sns_topic" "dacli_alarms" {
  name              = "${var.project_name}-${var.environment}-alarms"
  kms_master_key_id = aws_kms_key.dacli.id

  tags = {
    Name = "${var.project_name}-${var.environment}-alarms"
  }
}

resource "aws_sns_topic_subscription" "alarm_email" {
  topic_arn = aws_sns_topic.dacli_alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# ── CloudWatch Alarms ─────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "high_error_rate" {
  alarm_name          = "${var.project_name}-${var.environment}-high-error-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "AgentErrors"
  namespace           = "DACLI/AgentCore"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "DACLI agent error rate is too high"
  alarm_actions       = [aws_sns_topic.dacli_alarms.arn]
  ok_actions          = [aws_sns_topic.dacli_alarms.arn]
  treat_missing_data  = "notBreaching"

  tags = {
    Name = "${var.project_name}-${var.environment}-high-error-rate-alarm"
  }
}

resource "aws_cloudwatch_metric_alarm" "tool_call_failures" {
  alarm_name          = "${var.project_name}-${var.environment}-tool-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ToolCallErrors"
  namespace           = "DACLI/AgentCore"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Too many tool call failures in DACLI agent"
  alarm_actions       = [aws_sns_topic.dacli_alarms.arn]
  treat_missing_data  = "notBreaching"

  tags = {
    Name = "${var.project_name}-${var.environment}-tool-failures-alarm"
  }
}

resource "aws_cloudwatch_metric_alarm" "high_token_usage" {
  alarm_name          = "${var.project_name}-${var.environment}-high-token-usage"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "TotalTokens"
  namespace           = "DACLI/AgentCore"
  period              = 3600
  statistic           = "Sum"
  threshold           = 1000000
  alarm_description   = "DACLI agent token consumption exceeded 1M tokens/hour"
  alarm_actions       = [aws_sns_topic.dacli_alarms.arn]
  treat_missing_data  = "notBreaching"

  tags = {
    Name = "${var.project_name}-${var.environment}-high-token-alarm"
  }
}

# ── CloudWatch Dashboard ──────────────────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "dacli_agentcore" {
  dashboard_name = "${var.project_name}-${var.environment}-agentcore"

  dashboard_body = jsonencode({
    widgets = [
      # ── Header ──────────────────────────────────────────────────────────────
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 2
        properties = {
          markdown = "# 🤖 DACLI AgentCore - Monitoring Dashboard\n**Environment:** ${var.environment} | **Region:** ${var.aws_region} | **Agent:** ${var.agent_runtime_name}"
        }
      },

      # ── Agent Invocations ────────────────────────────────────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 2
        width  = 8
        height = 6
        properties = {
          title  = "Agent Invocations"
          view   = "timeSeries"
          stacked = false
          metrics = [
            ["DACLI/AgentCore", "AgentInvocations", { stat = "Sum", period = 300, color = "#2196F3" }]
          ]
          period = 300
          yAxis  = { left = { min = 0 } }
        }
      },

      # ── Error Rate ───────────────────────────────────────────────────────────
      {
        type   = "metric"
        x      = 8
        y      = 2
        width  = 8
        height = 6
        properties = {
          title  = "Error Rate"
          view   = "timeSeries"
          stacked = false
          metrics = [
            ["DACLI/AgentCore", "AgentErrors", { stat = "Sum", period = 300, color = "#F44336" }],
            ["DACLI/AgentCore", "ToolCallErrors", { stat = "Sum", period = 300, color = "#FF9800" }]
          ]
          period = 300
          yAxis  = { left = { min = 0 } }
        }
      },

      # ── Token Consumption ────────────────────────────────────────────────────
      {
        type   = "metric"
        x      = 16
        y      = 2
        width  = 8
        height = 6
        properties = {
          title  = "Token Consumption"
          view   = "timeSeries"
          stacked = true
          metrics = [
            ["DACLI/AgentCore", "TotalTokens", { stat = "Sum", period = 3600, color = "#9C27B0" }]
          ]
          period = 3600
          yAxis  = { left = { min = 0 } }
        }
      },

      # ── Agent Thinking Steps ─────────────────────────────────────────────────
      {
        type   = "log"
        x      = 0
        y      = 8
        width  = 12
        height = 8
        properties = {
          title   = "Agent Thinking Steps (Live)"
          view    = "table"
          query   = "SOURCE '/dacli/agentcore/thinking' | fields @timestamp, session_id, iteration, thinking_preview | sort @timestamp desc | limit 20"
          region  = var.aws_region
        }
      },

      # ── Tool Calls ───────────────────────────────────────────────────────────
      {
        type   = "log"
        x      = 12
        y      = 8
        width  = 12
        height = 8
        properties = {
          title   = "Tool Calls (Live)"
          view    = "table"
          query   = "SOURCE '/dacli/agentcore/tool-calls' | fields @timestamp, session_id, tool_name, status, duration_ms | sort @timestamp desc | limit 20"
          region  = var.aws_region
        }
      },

      # ── Agent Invocation Log ─────────────────────────────────────────────────
      {
        type   = "log"
        x      = 0
        y      = 16
        width  = 24
        height = 8
        properties = {
          title   = "Agent Invocations Log"
          view    = "table"
          query   = "SOURCE '/dacli/agentcore/invocations' | fields @timestamp, session_id, request_id, iterations, tool_calls, duration_ms | sort @timestamp desc | limit 50"
          region  = var.aws_region
        }
      },

      # ── Bedrock Native Metrics ───────────────────────────────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 24
        width  = 12
        height = 6
        properties = {
          title  = "Bedrock Model Invocations"
          view   = "timeSeries"
          metrics = [
            ["AWS/Bedrock", "Invocations", { stat = "Sum", period = 300 }],
            ["AWS/Bedrock", "InvocationThrottles", { stat = "Sum", period = 300, color = "#F44336" }]
          ]
          period = 300
        }
      },

      # ── Bedrock Latency ──────────────────────────────────────────────────────
      {
        type   = "metric"
        x      = 12
        y      = 24
        width  = 12
        height = 6
        properties = {
          title  = "Bedrock Model Latency (ms)"
          view   = "timeSeries"
          metrics = [
            ["AWS/Bedrock", "InvocationLatency", { stat = "p50", period = 300, label = "p50" }],
            ["AWS/Bedrock", "InvocationLatency", { stat = "p95", period = 300, label = "p95" }],
            ["AWS/Bedrock", "InvocationLatency", { stat = "p99", period = 300, label = "p99" }]
          ]
          period = 300
        }
      },
    ]
  })
}

# ── X-Ray Sampling Rule ───────────────────────────────────────────────────────

resource "aws_xray_sampling_rule" "dacli" {
  count = var.enable_xray ? 1 : 0

  rule_name      = "${var.project_name}-${var.environment}-sampling"
  priority       = 1000
  reservoir_size = 5
  fixed_rate     = 0.05
  url_path       = "*"
  host           = "*"
  http_method    = "*"
  service_type   = "*"
  service_name   = "dacli-agentcore"
  resource_arn   = "*"
  version        = 1

  attributes = {
    environment = var.environment
    project     = var.project_name
  }
}

# ── CloudWatch Bedrock Model Invocation Logging ───────────────────────────────

resource "aws_cloudwatch_log_group" "bedrock_model_invocations" {
  name              = "/aws/bedrock/model-invocations"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-${var.environment}-bedrock-invocations"
  }
}

resource "aws_bedrock_model_invocation_logging_configuration" "dacli" {
  logging_config {
    embedding_data_delivery_enabled = true
    image_data_delivery_enabled     = false
    text_data_delivery_enabled      = true

    cloudwatch_config {
      log_group_name = aws_cloudwatch_log_group.bedrock_model_invocations.name
      role_arn       = aws_iam_role.agentcore_runtime.arn

      large_data_delivery_s3_config {
        bucket_name = aws_s3_bucket.agent_artifacts.bucket
        key_prefix  = "bedrock-invocations/"
      }
    }

    s3_config {
      bucket_name = aws_s3_bucket.agent_artifacts.bucket
      key_prefix  = "bedrock-model-logs/"
    }
  }
}
