import asyncio
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def _make_s3_client(cfg: dict[str, Any]) -> boto3.client:
    """Create a boto3 S3 client using the supplied configuration."""
    session_kwargs = {
        "aws_access_key_id": cfg.get("access_key"),
        "aws_secret_access_key": cfg.get("secret_key"),
    }
    if cfg.get("region"):
        session_kwargs["region_name"] = cfg["region"]
    session = boto3.session.Session(**session_kwargs)
    return session.client("s3")


def register(api):
    # ----------------------------------------------------------------------
    # Configuration fields (provided later via /connect)
    # ----------------------------------------------------------------------
    api.config_field("bucket", required=True, description="Target bucket")
    api.config_field("access_key", secret=True, description="AWS Access Key ID")
    api.config_field("secret_key", secret=True, description="AWS Secret Access Key")
    api.config_field(
        "region", required=False, description="AWS region (e.g., us-east-1)", secret=False
    )

    # ----------------------------------------------------------------------
    # Helper to run blocking boto3 calls in a thread
    # ----------------------------------------------------------------------
    async def _run(fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    # ----------------------------------------------------------------------
    # Tool: list_buckets
    # ----------------------------------------------------------------------
    @api.tool(
        name="list_buckets",
        description="List all S3 buckets accessible with the configured credentials",
        parameters={},
        risk="safe",
        postconditions=["result_succeeded", "data_is_list"],
    )
    async def list_buckets(args, ctx):
        cfg = api.config()
        client = _make_s3_client(cfg)

        def _list():
            return client.list_buckets().get("Buckets", [])

        try:
            buckets = await _run(_list)
            rows = [{"Name": b["Name"], "CreationDate": b["CreationDate"].isoformat()} for b in buckets]
            return ctx.ok(rows)
        except (BotoCoreError, ClientError) as e:
            return ctx.fail(f"Failed to list buckets: {e}")

    # ----------------------------------------------------------------------
    # Tool: list_objects
    # ----------------------------------------------------------------------
    @api.tool(
        name="list_objects",
        description="List objects in the configured bucket under a given prefix",
        parameters={"prefix": {"type": "string"}},
        risk="safe",
        postconditions=["result_succeeded", "data_is_list"],
    )
    async def list_objects(args, ctx):
        cfg = api.config()
        client = _make_s3_client(cfg)
        prefix = args.get("prefix", "")

        def _list():
            paginator = client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=cfg["bucket"], Prefix=prefix)
            return [
                {
                    "Key": obj["Key"],
                    "Size": obj["Size"],
                    "LastModified": obj["LastModified"].isoformat(),
                }
                for page in pages
                for obj in page.get("Contents", [])
            ]

        try:
            objects = await _run(_list)
            return ctx.ok(objects)
        except (BotoCoreError, ClientError) as e:
            return ctx.fail(f"Failed to list objects: {e}")

    # ----------------------------------------------------------------------
    # Tool: get_object
    # ----------------------------------------------------------------------
    @api.tool(
        name="get_object",
        description="Retrieve the content of an object from the configured bucket",
        parameters={"key": {"type": "string"}},
        risk="safe",
        postconditions=["result_succeeded"],
    )
    async def get_object(args, ctx):
        cfg = api.config()
        client = _make_s3_client(cfg)
        key = args["key"]

        def _get():
            resp = client.get_object(Bucket=cfg["bucket"], Key=key)
            body = resp["Body"].read()
            # Assume UTF-8 text; if binary, callers can base64‑encode themselves.
            return body.decode("utf-8", errors="replace")

        try:
            content = await _run(_get)
            return ctx.ok(content)
        except client.exceptions.NoSuchKey:
            return ctx.fail(f"Object '{key}' does not exist.")
        except (BotoCoreError, ClientError) as e:
            return ctx.fail(f"Failed to get object: {e}")

    # ----------------------------------------------------------------------
    # Tool: put_object
    # ----------------------------------------------------------------------
    @api.tool(
        name="put_object",
        description="Upload content to an object in the configured bucket (overwrites if exists)",
        parameters={
            "key": {"type": "string"},
            "content": {"type": "string"},
        },
        risk="write",
        postconditions=["result_succeeded"],
    )
    async def put_object(args, ctx):
        cfg = api.config()
        client = _make_s3_client(cfg)
        key = args["key"]
        content = args["content"]

        def _put():
            client.put_object(Bucket=cfg["bucket"], Key=key, Body=content.encode("utf-8"))

        try:
            await _run(_put)
            return ctx.ok(f"Object '{key}' uploaded successfully.")
        except (BotoCoreError, ClientError) as e:
            return ctx.fail(f"Failed to upload object: {e}")

    # ----------------------------------------------------------------------
    # Tool: delete_object
    # ----------------------------------------------------------------------
    @api.tool(
        name="delete_object",
        description="Delete an object from the configured bucket",
        parameters={"key": {"type": "string"}},
        risk="write",
        postconditions=["result_succeeded"],
    )
    async def delete_object(args, ctx):
        cfg = api.config()
        client = _make_s3_client(cfg)
        key = args["key"]

        def _delete():
            client.delete_object(Bucket=cfg["bucket"], Key=key)

        try:
            await _run(_delete)
            return ctx.ok(f"Object '{key}' deleted successfully.")
        except (BotoCoreError, ClientError) as e:
            return ctx.fail(f"Failed to delete object: {e}")

    # ----------------------------------------------------------------------
    # Tool: bucket_stats
    # ----------------------------------------------------------------------
    @api.tool(
        name="bucket_stats",
        description="Provide simple statistics for the configured bucket (object count and total size)",
        parameters={},
        risk="safe",
        postconditions=["result_succeeded"],
    )
    async def bucket_stats(args, ctx):
        cfg = api.config()
        client = _make_s3_client(cfg)

        def _stats():
            paginator = client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=cfg["bucket"])
            total_size = 0
            object_count = 0
            for page in pages:
                for obj in page.get("Contents", []):
                    object_count += 1
                    total_size += obj["Size"]
            return {"object_count": object_count, "total_size_bytes": total_size}

        try:
            stats = await _run(_stats)
            return ctx.ok(stats)
        except (BotoCoreError, ClientError) as e:
            return ctx.fail(f"Failed to compute bucket stats: {e}")