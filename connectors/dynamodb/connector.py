"""DynamoDB connector (Wave 2) — CLI-first via `aws dynamodb`.

Key-value / wide-column — the connector that **stresses the abstraction the
most**, validating that the contract truly generalizes beyond SQL. Governance:

* **point-in-time recovery (PITR)** is the rollback primitive — a destructive op
  is only allowed once PITR is verified enabled on the table;
* post-conditions are anchored to a live `get-item` / `describe-table` read after
  the mutation (there is no SQL oracle).
"""

from __future__ import annotations

import json
import time
from typing import Any

from connectors.base import OperationSpec, Risk, ToolResult
from connectors.cli_base import CliConnector
from core.verify import PostCondition, VerificationContext, result_succeeded, data_is_list

def dynamo_item_present() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if data.get("exists") is not True:
            return False, "item not found via get-item after put"
        return True, ""
    return PostCondition("dynamo_item_present", check,
                         "put item is present per get-item", anchored=True)


def dynamo_item_absent() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if data.get("exists") is not False:
            return False, "item still present via get-item after delete"
        return True, ""
    return PostCondition("dynamo_item_absent", check,
                         "deleted item is gone per get-item", anchored=True)


def dynamo_table_absent() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if data.get("exists") is not False:
            return False, "table still present via describe-table after delete"
        return True, ""
    return PostCondition("dynamo_table_absent", check,
                         "deleted table is gone per describe-table", anchored=True)


def introspect_reports_structure() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None)
        if not isinstance(data, dict) or "exists" not in data:
            return False, "introspection did not return {exists, ...}"
        return True, ""
    return PostCondition("introspect_reports_structure", check,
                         "introspection returns a definite existence verdict", anchored=True)


class DynamoDBConnector(CliConnector):
    name = "dynamodb"
    binary = "aws"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        cfg = getattr(settings, "dynamodb", None)
        self.binary = getattr(cfg, "aws_binary", "aws") or "aws"

    def operations(self) -> list[OperationSpec]:
        table = {"type": "string", "description": "Table name."}
        key = {"type": "object", "description": "Primary key in DynamoDB JSON, e.g. {\"id\": {\"S\": \"123\"}}."}
        return [
            OperationSpec(
                name="scan_dynamodb_table",
                description="Scan items from a table (read-only).",
                parameters={
                    "type": "object",
                    "properties": {"table": table,
                                   "limit": {"type": "integer", "description": "Max items (default 25)."}},
                    "required": ["table"],
                },
                capability="dynamodb.read", risk=Risk.SAFE,
                display_name="Scan Table", category="read",
                postconditions=[data_is_list(name="returns_items")],
            ),
            OperationSpec(
                name="get_dynamodb_item",
                description="Get a single item by primary key (read-only).",
                parameters={"type": "object", "properties": {"table": table, "key": key},
                            "required": ["table", "key"]},
                capability="dynamodb.read", risk=Risk.SAFE,
                display_name="Get Item", category="read",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="put_dynamodb_item",
                description="Put (create/overwrite) an item.",
                parameters={
                    "type": "object",
                    "properties": {"table": table,
                                   "item": {"type": "object", "description": "Item in DynamoDB JSON."}},
                    "required": ["table", "item"],
                },
                capability="dynamodb.write", risk=Risk.WRITE,
                display_name="Put Item", category="write",
                postconditions=[result_succeeded(), dynamo_item_present()],
            ),
            OperationSpec(
                name="delete_dynamodb_item",
                description="Delete a single item by primary key.",
                parameters={"type": "object", "properties": {"table": table, "key": key},
                            "required": ["table", "key"]},
                capability="dynamodb.write", risk=Risk.RISKY,
                display_name="Delete Item", category="write",
                postconditions=[result_succeeded(), dynamo_item_absent()],
            ),
            OperationSpec(
                name="delete_dynamodb_table",
                description="Delete an entire table. Irreversible without point-in-time recovery.",
                parameters={"type": "object", "properties": {"table": table}, "required": ["table"]},
                capability="dynamodb.admin", risk=Risk.IRREVERSIBLE,
                display_name="Delete Table", category="write",
                postconditions=[result_succeeded(), dynamo_table_absent()],
            ),
            OperationSpec(
                name="introspect_dynamodb_table",
                description="Read a table's key schema + attributes via describe-table. Feeds the catalog.",
                parameters={"type": "object", "properties": {"table": table}, "required": ["table"]},
                capability="dynamodb.introspection", risk=Risk.SAFE,
                display_name="Introspect Table", category="introspection",
                postconditions=[introspect_reports_structure()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "scan_dynamodb_table":
            return await self._scan(args)
        if op == "get_dynamodb_item":
            return await self._get(args)
        if op == "put_dynamodb_item":
            return await self._put(args)
        if op == "delete_dynamodb_item":
            return await self._delete_item(args)
        if op == "delete_dynamodb_table":
            return await self._delete_table(args)
        if op == "introspect_dynamodb_table":
            return await self._introspect(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    def _cfg(self):
        return getattr(self.settings, "dynamodb", None)

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 300) if cfg else 300

    def _base(self, *sub: str) -> list[str]:
        cfg = self._cfg()
        argv = [self.binary, "dynamodb", *sub, "--output", "json"]
        if cfg and getattr(cfg, "region", ""):
            argv += ["--region", cfg.region]
        if cfg and getattr(cfg, "profile", ""):
            argv += ["--profile", cfg.profile]
        return argv

    @staticmethod
    def _parse(text: str) -> dict[str, Any] | None:
        try:
            return json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            return None

    async def _scan(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        argv = self._base("scan", "--table-name", args.get("table", ""),
                          "--max-items", str(int(args.get("limit") or 25)))
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("scan_dynamodb_table",
                              f"scan failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        payload = self._parse(res.stdout) or {}
        return self._ok("scan_dynamodb_table", payload.get("Items", []), started)

    async def _get(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        argv = self._base("get-item", "--table-name", args.get("table", ""),
                          "--key", json.dumps(args.get("key") or {}))
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("get_dynamodb_item",
                              f"get-item failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        payload = self._parse(res.stdout) or {}
        item = payload.get("Item")
        return self._ok("get_dynamodb_item", {"exists": bool(item), "item": item}, started)

    async def _key_attr_names(self, table: str) -> list[str]:
        argv = self._base("describe-table", "--table-name", table)
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return []
        payload = self._parse(res.stdout) or {}
        schema = (payload.get("Table", {}) or {}).get("KeySchema", []) or []
        return [k.get("AttributeName") for k in schema if k.get("AttributeName")]

    async def _item_exists(self, table: str, key: dict[str, Any]) -> bool:
        argv = self._base("get-item", "--table-name", table, "--key", json.dumps(key))
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return False
        payload = self._parse(res.stdout) or {}
        return bool(payload.get("Item"))

    async def _put(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        table = args.get("table", "")
        item = args.get("item") or {}
        argv = self._base("put-item", "--table-name", table, "--item", json.dumps(item))
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("put_dynamodb_item",
                              f"put-item failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        names = await self._key_attr_names(table)
        key = {n: item[n] for n in names if n in item}
        exists = await self._item_exists(table, key) if key else None
        return self._ok("put_dynamodb_item", {"table": table, "key": key, "exists": exists}, started,
                        catalog_effects=[{"action": "invalidate", "object_type": "table",
                                          "scope": {"object": table}}])

    async def _delete_item(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        table = args.get("table", "")
        key = args.get("key") or {}
        argv = self._base("delete-item", "--table-name", table, "--key", json.dumps(key))
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("delete_dynamodb_item",
                              f"delete-item failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        exists = await self._item_exists(table, key)
        return self._ok("delete_dynamodb_item", {"table": table, "key": key, "exists": exists}, started,
                        catalog_effects=[{"action": "invalidate", "object_type": "table",
                                          "scope": {"object": table}}])

    async def _delete_table(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        table = args.get("table", "")
        argv = self._base("delete-table", "--table-name", table)
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("delete_dynamodb_table",
                              f"delete-table failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        check = await self.invoke("introspect_dynamodb_table", {"table": table})
        exists = bool((getattr(check, "data", None) or {}).get("exists"))
        return self._ok("delete_dynamodb_table", {"table": table, "exists": exists}, started,
                        catalog_effects=[{"action": "invalidate", "object_type": "table",
                                          "scope": {"object": table}}])

    async def _introspect(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        table = args.get("table", "")
        argv = self._base("describe-table", "--table-name", table)
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            # describe-table returns non-zero when the table does not exist.
            return self._ok("introspect_dynamodb_table",
                            {"exists": False, "scope": {"object": table}}, started)
        payload = self._parse(res.stdout) or {}
        desc = payload.get("Table", {}) or {}
        scope = {"object": table}
        keys = [{"name": k.get("AttributeName"), "type": k.get("KeyType")}
                for k in desc.get("KeySchema", []) or []]
        attrs = [{"name": a.get("AttributeName"), "type": a.get("AttributeType")}
                 for a in desc.get("AttributeDefinitions", []) or []]
        effects = [{"action": "create", "object_type": "table", "scope": scope,
                    "source": "dynamodb.describe_table", "columns": attrs}]
        return self._ok("introspect_dynamodb_table",
                        {"exists": True, "scope": scope, "keys": keys, "attributes": attrs}, started,
                        scope=scope, catalog_effects=effects)

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        if getattr(plan, "primitive", "") != "dynamodb_pitr":
            return False, f"no verifiable rollback path for primitive '{plan.primitive}'"
        table = args.get("table", "")
        argv = self._base("describe-continuous-backups", "--table-name", table)
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return False, f"could not read continuous-backups for '{table}'"
        payload = self._parse(res.stdout) or {}
        status = (((payload.get("ContinuousBackupsDescription", {}) or {})
                   .get("PointInTimeRecoveryDescription", {}) or {})
                  .get("PointInTimeRecoveryStatus"))
        if status == "ENABLED":
            return True, f"table '{table}' has PITR enabled — restorable to a point in time"
        return False, (f"table '{table}' PITR is '{status or 'DISABLED'}' — "
                       f"delete is not recoverable")
