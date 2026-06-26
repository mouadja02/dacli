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
    # CREATE FUNCTION (inline code or S3)
    # ----------------------------------------------------------------------
    @api.tool(
        name="create_function",
        description=(
            "Create a new AWS Lambda function. Provide EITHER inline_code (a Python "
            "source string — the tool zips it for you) OR s3_bucket + s3_key."
        ),
        parameters={
            "function_name": {"type": "string", "description": "Name of the new function"},
            "runtime": {
                "type": "string",
                "description": "Runtime identifier, e.g. python3.12, nodejs20.x"
            },
            "role_arn": {"type": "string", "description": "ARN of the IAM role for the function"},
            "handler": {"type": "string", "description": "Handler name, e.g. lambda_function.lambda_handler"},
            "inline_code": {
                "type": "string",
                "description": "Python source code to deploy inline (zipped automatically). Use this for simple functions instead of S3."
            },
            "s3_bucket": {"type": "string", "description": "S3 bucket containing the deployment zip (alternative to inline_code)"},
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
        import io
        import zipfile

        cfg = api.config()
        client = _make_client(cfg)

        inline_code = args.get("inline_code")
        s3_bucket = args.get("s3_bucket")
        s3_key = args.get("s3_key")

        if not inline_code and not (s3_bucket and s3_key):
            return ctx.fail("Provide either inline_code or both s3_bucket + s3_key")

        def _create():
            # Build the Code block: inline zip or S3 reference.
            if inline_code:
                buf = io.BytesIO()
                # Derive the module filename from the handler (e.g. "app.handler" -> "app.py")
                handler = args.get("handler", "lambda_function.lambda_handler")
                module_name = handler.split(".")[0] + ".py"
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(module_name, inline_code)
                code = {"ZipFile": buf.getvalue()}
            else:
                code = {"S3Bucket": s3_bucket, "S3Key": s3_key}

            payload: dict[str, Any] = {
                "FunctionName": args["function_name"],
                "Runtime": args.get("runtime", "python3.12"),
                "Role": args["role_arn"],
                "Handler": args.get("handler", "lambda_function.lambda_handler"),
                "Code": code,
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
        description="Update the code of an existing Lambda function. Provide either inline_code or s3_bucket + s3_key.",
        parameters={
            "function_name": {"type": "string", "description": "Name of the Lambda function"},
            "handler": {"type": "string", "description": "Handler name (needed for inline_code to derive the module filename)"},
            "inline_code": {"type": "string", "description": "Python source code to deploy inline (zipped automatically)"},
            "s3_bucket": {"type": "string", "description": "S3 bucket with new code"},
            "s3_key": {"type": "string", "description": "S3 object key for new code"},
            "publish": {"type": "boolean", "description": "Whether to publish a new version"},
        },
        risk="write",
        postconditions=["result_succeeded"],
    )
    async def update_function_code(args, ctx):
        import io
        import zipfile

        cfg = api.config()
        client = _make_client(cfg)

        inline_code = args.get("inline_code")
        s3_bucket = args.get("s3_bucket")
        s3_key = args.get("s3_key")

        if not inline_code and not (s3_bucket and s3_key):
            return ctx.fail("Provide either inline_code or both s3_bucket + s3_key")

        def _update():
            kwargs: dict[str, Any] = {
                "FunctionName": args["function_name"],
                "Publish": args.get("publish", True),
            }
            if inline_code:
                buf = io.BytesIO()
                handler = args.get("handler", "lambda_function.lambda_handler")
                module_name = handler.split(".")[0] + ".py"
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(module_name, inline_code)
                kwargs["ZipFile"] = buf.getvalue()
            else:
                kwargs["S3Bucket"] = s3_bucket
                kwargs["S3Key"] = s3_key
            return client.update_function_code(**kwargs)

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