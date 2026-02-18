# ============================================================
# DACLI AgentCore - Terraform Outputs
# ============================================================

output "ecr_repository_url" {
  description = "ECR repository URL for pushing Docker images"
  value       = data.aws_ecr_repository.dacli.repository_url
}

output "ecr_repository_name" {
  description = "ECR repository name"
  value       = data.aws_ecr_repository.dacli.name
}

output "agent_runtime_id" {
  description = "AgentCore Runtime ID"
  value       = aws_bedrockagentcore_agent_runtime.dacli.agent_runtime_id
}

output "agent_runtime_arn" {
  description = "AgentCore Runtime ARN"
  value       = aws_bedrockagentcore_agent_runtime.dacli.agent_runtime_arn
}

output "agent_runtime_endpoint_arn" {
  description = "AgentCore Runtime Endpoint ARN"
  value       = aws_bedrockagentcore_agent_runtime_endpoint.dacli.agent_runtime_endpoint_arn
}

output "agent_memory_id" {
  description = "AgentCore Memory resource ID"
  value       = aws_bedrockagentcore_memory.dacli.id
}

output "workload_identity_arn" {
  description = "AgentCore Workload Identity ARN"
  value       = aws_bedrockagentcore_workload_identity.dacli.workload_identity_arn
}

output "gateway_id" {
  description = "AgentCore Gateway ID"
  value       = aws_bedrockagentcore_gateway.dacli_tools.gateway_id
}

output "gateway_url" {
  description = "AgentCore Gateway MCP URL"
  value       = aws_bedrockagentcore_gateway.dacli_tools.gateway_url
}

output "secrets_manager_arn" {
  description = "ARN of the Secrets Manager secret containing DACLI config"
  value       = aws_secretsmanager_secret.dacli_config.arn
  sensitive   = true
}

output "s3_artifacts_bucket" {
  description = "S3 bucket for agent artifacts and state"
  value       = aws_s3_bucket.agent_artifacts.bucket
}

output "cloudwatch_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.dacli_agentcore.dashboard_name}"
}

output "github_actions_role_arn" {
  description = "IAM role ARN for GitHub Actions OIDC"
  value       = aws_iam_role.github_actions.arn
}

output "agentcore_runtime_role_arn" {
  description = "IAM role ARN for AgentCore Runtime execution"
  value       = aws_iam_role.agentcore_runtime.arn
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "kms_key_arn" {
  description = "KMS key ARN for encryption"
  value       = aws_kms_key.dacli.arn
  sensitive   = true
}

output "sns_alarm_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarms"
  value       = aws_sns_topic.dacli_alarms.arn
}

output "log_groups" {
  description = "CloudWatch log group names"
  value = {
    runtime     = aws_cloudwatch_log_group.agent_runtime.name
    invocations = aws_cloudwatch_log_group.agent_invocations.name
    tool_calls  = aws_cloudwatch_log_group.agent_tool_calls.name
    thinking    = aws_cloudwatch_log_group.agent_thinking.name
    tokens      = aws_cloudwatch_log_group.agent_tokens.name
  }
}
