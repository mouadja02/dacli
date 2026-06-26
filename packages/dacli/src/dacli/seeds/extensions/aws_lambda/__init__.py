import asyncio
from typing import Any

import boto3
from botocore.exceptions import ClientError


def _make_client(cfg: dict[str, Any]):
    """Create a boto3 Lambda client using the supplied configuration."""
    return boto3.client(
        "lambda",
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=cfg["region"],
    )


def register(api):
    # ----------------------------------------------------------------------
    # Configuration fields (provided later via /connect)
    # ----------------------------------------------------------------------
    api.config_field("access_key", secret=True, required=True, description="AWS Access Key ID")
    api.config_field("secret_key", secret=True, required=True, description="AWS Secret Access Key")
    api.config_field("region", required=True, description="AWS region, e.g. us-east-1")

    # ----------------------------------------------------------------------
    # Helper: run blocking boto3 calls in a thread
    # ----------------------------------------------------------------------
    async def _run(fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    # ----------------------------------------------------------------------
    # LIST FUNCTIONS
    # ----------------------------------------------------------------------
    @api.tool(
        name="list_functions",
        description="List all AWS Lambda functions in the configured region",
        parameters={},
        risk="safe",
        postconditions=["result_succeeded", "data_is_list"],
    )
    async def list_functions(args, ctx):
        cfg = api.config()
        client = _make_client(cfg)

        def _list():
            paginator = client.get_paginator("list_functions")
            functions: list[dict[str, Any]] = []
            for page in paginator.paginate():
                functions.extend(page.get("Functions", []))
            return functions

        try:
            functions = await _run(_list)
            return ctx.ok(functions)
        except ClientError as e:
            return ctx.fail(f"Failed to list functions: {e}")

    # ----------------------------------------------------------------------
    # GET FUNCTION
    # ----------------------------------------------------------------------
    @api.tool(
        name="get_function",
        description="Retrieve details of a specific Lambda function",
        parameters={
            "function_name": {"type": "string", "description": "Name of the Lambda function"}
        },
        risk="safe",
        postconditions=["result_succeeded"],
    )
    async def get_function(args, ctx):
        cfg = api.config()
        client = _make_client(cfg)

        def _get():
            return client.get_function(FunctionName=args["function_name"])

        try:
            resp = await _run(_get)
            return ctx.ok(resp)
        except ClientError as e:
            return ctx.fail(f"Failed to get function '{args['function_name']}': {e}")

    # ----------------------------------------------------------------------
    # CREATE FUNCTION
    # ----------------------------------------------------------------------
    @api.tool(
        name="create_function",
        description="Create a new AWS Lambda function from a deployment package stored in S3",
        parameters={
            "function_name": {"type": "string", "description": "Name of the new function"},
            "runtime": {
                "type": "string",
                "description": "Runtime identifier, e.g. python3.9, nodejs14.x"
            },
            "role_arn": {"type": "string", "description": "ARN of the IAM role for the function"},
            "handler": {"type": "string", "description": "Handler name, e.g. app.lambda_handler"},
            "s3_bucket": {"type": "string", "description": "S3 bucket containing the deployment zip"},
            "s3_key": {"type": "string", "description": "S3 object key for the deployment zip"},
            # Optional fields
            "description": {"type": "string"},
            "timeout": {"type": "integer", "minimum": 1},
            "memory_size": {"type": "integer", "minimum": 128},
            "environment": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": "Key-value map of environment variables"
            },
            "tags": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": "Tags to apply to the function"
            },
        },
        risk="write",
        postconditions=["result_succeeded"],
    )
    async def create_function(args, ctx):
        cfg = api.config()
        client = _make_client(cfg)

        def _create():
            payload: dict[str, Any] = {
                "FunctionName": args["function_name"],
                "Runtime": args["runtime"],
                "Role": args["role_arn"],
                "Handler": args["handler"],
                "Code": {
                    "S3Bucket": args["s3_bucket"],
                    "S3Key": args["s3_key"],
                },
                "Publish": True,
            }

            # Optional parameters – include only if supplied
            mapping = {
                "description": "Description",
                "timeout": "Timeout",
                "memory_size": "MemorySize",
                "environment": "Environment",
                "tags": "Tags",
            }
            for arg_key, api_key in mapping.items():
                if arg_key in args:
                    payload[api_key] = args[arg_key]

            return client.create_function(**payload)

        try:
            resp = await _run(_create)
            return ctx.ok(resp)
        except ClientError as e:
            return ctx.fail(f"Failed to create function '{args['function_name']}': {e}")

    # ----------------------------------------------------------------------
    # UPDATE FUNCTION CODE
    # ----------------------------------------------------------------------
    @api.tool(
        name="update_function_code",
        description="Update the code of an existing Lambda function from an S3 object",
        parameters={
            "function_name": {"type": "string", "description": "Name of the Lambda function"},
            "s3_bucket": {"type": "string", "description": "S3 bucket with new code"},
            "s3_key": {"type": "string", "description": "S3 object key for new code"},
            "publish": {"type": "boolean", "description": "Whether to publish a new version", "default": True},
        },
        risk="write",
        postconditions=["result_succeeded"],
    )
    async def update_function_code(args, ctx):
        cfg = api.config()
        client = _make_client(cfg)

        def _update():
            return client.update_function_code(
                FunctionName=args["function_name"],
                S3Bucket=args["s3_bucket"],
                S3Key=args["s3_key"],
                Publish=args.get("publish", True),
            )

        try:
            resp = await _run(_update)
            return ctx.ok(resp)
        except ClientError as e:
            return ctx.fail(f"Failed to update code for function '{args['function_name']}': {e}")

    # ----------------------------------------------------------------------
    # DELETE FUNCTION
    # ----------------------------------------------------------------------
    @api.tool(
        name="delete_function",
        description="Delete an AWS Lambda function",
        parameters={
            "function_name": {"type": "string", "description": "Name of the Lambda function to delete"}
        },
        risk="write",
        postconditions=["result_succeeded"],
    )
    async def delete_function(args, ctx):
        cfg = api.config()
        client = _make_client(cfg)

        def _delete():
            return client.delete_function(FunctionName=args["function_name"])

        try:
            await _run(_delete)
            return ctx.ok(f"Function '{args['function_name']}' deleted")
        except ClientError as e:
            return ctx.fail(f"Failed to delete function '{args['function_name']}': {e}")

    # ----------------------------------------------------------------------
    # GET FUNCTION CONFIGURATION (describe)
    # ----------------------------------------------------------------------
    @api.tool(
        name="describe_function",
        description="Retrieve the configuration of a Lambda function without code details",
        parameters={
            "function_name": {"type": "string", "description": "Name of the Lambda function"}
        },
        risk="safe",
        postconditions=["result_succeeded"],
    )
    async def describe_function(args, ctx):
        cfg = api.config()
        client = _make_client(cfg)

        def _describe():
            return client.get_function_configuration(FunctionName=args["function_name"])

        try:
            resp = await _run(_describe)
            return ctx.ok(resp)
        except ClientError as e:
            return ctx.fail(f"Failed to describe function '{args['function_name']}': {e}")

    # ----------------------------------------------------------------------
    # Register commands / shortcuts if desired (optional, not required for core CRUD)
    # ----------------------------------------------------------------------
    # Example: a shortcut to list functions with Ctrl+L
    api.shortcut("Ctrl+L", list_functions)