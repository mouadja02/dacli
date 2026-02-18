#!/usr/bin/env bash
# ============================================================
# DACLI AgentCore - Terraform Bootstrap Script
# Creates the S3 backend and DynamoDB lock table ONCE
# before running terraform init/apply
# ============================================================
# Usage: bash terraform/bootstrap.sh [aws-region] [environment]
# ============================================================

set -euo pipefail

AWS_REGION="${1:-us-east-1}"
ENVIRONMENT="${2:-prod}"
STATE_BUCKET="dacli-terraform-state"
LOCK_TABLE="dacli-terraform-locks"
GITHUB_OIDC_THUMBPRINT="6938fd4d98bab03faadb97b34396831e3780aea1"

echo "🚀 DACLI AgentCore - Terraform Bootstrap"
echo "Region: ${AWS_REGION}"
echo "Environment: ${ENVIRONMENT}"
echo ""

# ── S3 State Bucket ───────────────────────────────────────────────────────────
echo "📦 Creating S3 state bucket: ${STATE_BUCKET}"
if aws s3api head-bucket --bucket "${STATE_BUCKET}" 2>/dev/null; then
    echo "  ✅ Bucket already exists"
else
    if [ "${AWS_REGION}" = "us-east-1" ]; then
        aws s3api create-bucket \
            --bucket "${STATE_BUCKET}" \
            --region "${AWS_REGION}"
    else
        aws s3api create-bucket \
            --bucket "${STATE_BUCKET}" \
            --region "${AWS_REGION}" \
            --create-bucket-configuration LocationConstraint="${AWS_REGION}"
    fi
    echo "  ✅ Bucket created"
fi

# Enable versioning
aws s3api put-bucket-versioning \
    --bucket "${STATE_BUCKET}" \
    --versioning-configuration Status=Enabled
echo "  ✅ Versioning enabled"

# Enable encryption
aws s3api put-bucket-encryption \
    --bucket "${STATE_BUCKET}" \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            }
        }]
    }'
echo "  ✅ Encryption enabled"

# Block public access
aws s3api put-public-access-block \
    --bucket "${STATE_BUCKET}" \
    --public-access-block-configuration \
        BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
echo "  ✅ Public access blocked"

# ── DynamoDB Lock Table ───────────────────────────────────────────────────────
echo ""
echo "🔒 Creating DynamoDB lock table: ${LOCK_TABLE}"
if aws dynamodb describe-table --table-name "${LOCK_TABLE}" --region "${AWS_REGION}" 2>/dev/null; then
    echo "  ✅ Table already exists"
else
    aws dynamodb create-table \
        --table-name "${LOCK_TABLE}" \
        --attribute-definitions AttributeName=LockID,AttributeType=S \
        --key-schema AttributeName=LockID,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --region "${AWS_REGION}"

    aws dynamodb wait table-exists \
        --table-name "${LOCK_TABLE}" \
        --region "${AWS_REGION}"
    echo "  ✅ Table created"
fi

# ── GitHub OIDC Provider ──────────────────────────────────────────────────────
echo ""
echo "🔑 Setting up GitHub Actions OIDC provider"
OIDC_URL="https://token.actions.githubusercontent.com"

if aws iam list-open-id-connect-providers | grep -q "token.actions.githubusercontent.com"; then
    echo "  ✅ OIDC provider already exists"
else
    aws iam create-open-id-connect-provider \
        --url "${OIDC_URL}" \
        --client-id-list "sts.amazonaws.com" \
        --thumbprint-list "${GITHUB_OIDC_THUMBPRINT}"
    echo "  ✅ OIDC provider created"
fi

echo ""
echo "✅ Bootstrap complete!"
echo ""
echo "Next steps:"
echo "  1. cd terraform"
echo "  2. terraform init"
echo "  3. terraform plan -var-file=terraform.tfvars"
echo "  4. terraform apply -var-file=terraform.tfvars"
echo ""
echo "Or push to main branch to trigger GitHub Actions deployment."
