import asyncio
import json
from typing import Any

def register(api):
    # Configuration fields – secrets are marked secret=True and never stored in code.
    api.config_field("region", required=True, description="AWS region, e.g. us-east-1")
    api.config_field("table", required=False, description="Default DynamoDB table (overridable per call)")
    api.config_field("access_key", secret=True, description="AWS Access Key ID")
    api.config_field("secret_key", secret=True, description="AWS Secret Access Key")
    api.config_field(
        "session_token", secret=True, required=False, description="AWS Session Token (if using temporary credentials)"
    )

    # -------------------------------------------------------------------------
    # Helper: create a boto3 DynamoDB client from the current config.
    # -------------------------------------------------------------------------
    def _make_client(cfg: dict[str, Any]):
        import boto3

        session_kwargs = {
            "aws_access_key_id": cfg.get("access_key"),
            "aws_secret_access_key": cfg.get("secret_key"),
            "region_name": cfg["region"],
        }
        if cfg.get("session_token"):
            session_kwargs["aws_session_token"] = cfg["session_token"]
        session = boto3.session.Session(**session_kwargs)
        return session.client("dynamodb")

    def _resolve_table(args, cfg):
        """Resolve table from tool args, falling back to config default."""
        return args.get("table") or cfg.get("table")

    # -------------------------------------------------------------------------
    # Tool: List items (scan) from the table – safe operation.
    # -------------------------------------------------------------------------
    @api.tool(
        name="dynamodb_scan",
        description="Scan a DynamoDB table and return items (optionally limited).",
        parameters={
            "table": {"type": "string", "description": "Table name (defaults to configured)"},
            "limit": {"type": "integer", "minimum": 1, "description": "Maximum number of items to return"},
        },
        risk="safe",
        postconditions=["result_succeeded", "data_is_list"],
    )
    async def dynamodb_scan(args, ctx):
        cfg = api.config()
        table = _resolve_table(args, cfg)
        if not table:
            return ctx.fail("No table specified and none configured")
        client = _make_client(cfg)

        limit = args.get("limit")
        scan_kwargs = {"TableName": table}
        if limit:
            scan_kwargs["Limit"] = limit

        try:
            response = await asyncio.to_thread(client.scan, **scan_kwargs)
            items = response.get("Items", [])
            # Convert DynamoDB JSON to plain Python dicts
            plain_items = [json.loads(json.dumps(item, default=str)) for item in items]
            return ctx.ok(plain_items)
        except Exception as exc:
            return ctx.fail(f"Scan failed: {exc}")

    # -------------------------------------------------------------------------
    # Tool: Get a single item by its primary key – safe operation.
    # -------------------------------------------------------------------------
    @api.tool(
        name="dynamodb_get",
        description="Retrieve a single item from a table using its primary key.",
        parameters={
            "table": {"type": "string", "description": "Table name (defaults to configured)"},
            "key": {
                "type": "object",
                "description": "Primary key map (attribute name → value).",
                "additionalProperties": {"type": "string"},
            }
        },
        risk="safe",
        postconditions=["result_succeeded"],
    )
    async def dynamodb_get(args, ctx):
        cfg = api.config()
        table = _resolve_table(args, cfg)
        if not table:
            return ctx.fail("No table specified and none configured")
        client = _make_client(cfg)

        key = args.get("key")
        if not isinstance(key, dict) or not key:
            return ctx.fail("Invalid or missing 'key' parameter")

        # DynamoDB expects the key in the AttributeValue format.
        def to_attr(val):
            return {"S": str(val)}  # Simplified: treat all keys as strings

        dynamo_key = {k: to_attr(v) for k, v in key.items()}

        try:
            response = await asyncio.to_thread(
                client.get_item, TableName=table, Key=dynamo_key
            )
            item = response.get("Item")
            if not item:
                return ctx.fail("Item not found")
            plain_item = json.loads(json.dumps(item, default=str))
            return ctx.ok(plain_item)
        except Exception as exc:
            return ctx.fail(f"GetItem failed: {exc}")

    # -------------------------------------------------------------------------
    # Tool: Put (write) an item into the table – write risk.
    # -------------------------------------------------------------------------
    @api.tool(
        name="dynamodb_put",
        description="Put (create or replace) an item into a DynamoDB table.",
        parameters={
            "table": {"type": "string", "description": "Table name (defaults to configured)"},
            "item": {
                "type": "object",
                "description": "Full item to store (attribute name → value).",
                "additionalProperties": {"type": "string"},
            }
        },
        risk="write",
        postconditions=["result_succeeded", "shell_writes_observed"],
    )
    async def dynamodb_put(args, ctx):
        cfg = api.config()
        table = _resolve_table(args, cfg)
        if not table:
            return ctx.fail("No table specified and none configured")
        client = _make_client(cfg)

        item = args.get("item")
        if not isinstance(item, dict) or not item:
            return ctx.fail("Invalid or missing 'item' parameter")

        # Convert plain dict to DynamoDB AttributeValue format (simplified to strings)
        dynamo_item = {k: {"S": str(v)} for k, v in item.items()}

        try:
            await asyncio.to_thread(
                client.put_item, TableName=table, Item=dynamo_item
            )
            return ctx.ok({"message": "Item written successfully"})
        except Exception as exc:
            return ctx.fail(f"PutItem failed: {exc}")

    # -------------------------------------------------------------------------
    # Optional: expose a simple command to show current config (non‑secret parts)
    # -------------------------------------------------------------------------
    @api.command(name="dynamodb_show_config")
    def show_config(_args, _ctx):
        cfg = api.config()
        # Remove secret fields before displaying
        safe_cfg = {k: v for k, v in cfg.items() if k not in ("access_key", "secret_key", "session_token")}
        return {"config": safe_cfg}