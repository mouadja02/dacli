"""S3 connector (Wave 1) — CLI-first via `aws`.

Object storage for lake ingestion + the staging source for warehouse loads.
Governance: bucket **versioning** (or copy-aside) is the rollback primitive — a
delete/overwrite is only allowed once versioning is verified, so a prior version
can be restored.

Post-conditions are environment-anchored: after a put/delete the connector
re-`head`s the object against live S3 and the check asserts on that live truth.
"""

from __future__ import annotations

import json
import time
from typing import Any

from dacli.connectors.base import OperationSpec, Risk, ToolResult
from dacli.core.logging_setup import get_logger

log = get_logger(__name__)
from dacli.connectors.cli_base import CliConnector
from dacli.core.verify import PostCondition, VerificationContext, result_succeeded


def s3_object_present() -> PostCondition:
    """After a put, a live head-object confirms the key exists."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if data.get("exists") is not True:
            return False, f"object '{data.get('key')}' not found after put (head-object)"
        return True, ""
    return PostCondition("s3_object_present", check,
                         "uploaded object is present per head-object", anchored=True)


def s3_object_absent() -> PostCondition:
    """After a delete, a live head-object confirms the key is gone."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if data.get("exists") is not False:
            return False, f"object '{data.get('key')}' still present after delete"
        return True, ""
    return PostCondition("s3_object_absent", check,
                         "deleted object is gone per head-object", anchored=True)


def lists_objects() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if "objects" not in data:
            return False, "listing did not return an 'objects' array"
        return True, ""
    return PostCondition("lists_objects", check, "listing returns an objects array", anchored=True)


class S3Connector(CliConnector):
    name = "s3"
    binary = "aws"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        cfg = getattr(settings, "s3", None)
        self.binary = getattr(cfg, "aws_binary", "aws") or "aws"

    def operations(self) -> list[OperationSpec]:
        key_param = {
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Object key (path within the bucket)."},
                "bucket": {"type": "string", "description": "Bucket (defaults to the configured bucket)."},
            },
            "required": ["key"],
        }
        return [
            OperationSpec(
                name="list_s3_objects",
                description="List objects under a prefix (`aws s3api list-objects-v2`). Read-only; re-verifies live lake state.",
                parameters={
                    "type": "object",
                    "properties": {
                        "prefix": {"type": "string"},
                        "bucket": {"type": "string"},
                    },
                },
                capability="s3.introspection", risk=Risk.SAFE,
                display_name="List Objects", category="introspection",
                postconditions=[lists_objects()],
            ),
            OperationSpec(
                name="read_s3_object",
                description="Read an object's contents (`aws s3 cp s3://… -`).",
                parameters=key_param,
                capability="s3.read", risk=Risk.SAFE,
                display_name="Read Object", category="read",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="put_s3_object",
                description="Upload content to a key (`aws s3 cp - s3://…`). Overwrites if the key exists.",
                parameters={
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                        "content": {"type": "string"},
                        "bucket": {"type": "string"},
                    },
                    "required": ["key", "content"],
                },
                capability="s3.write", risk=Risk.WRITE,
                display_name="Put Object", category="write",
                postconditions=[result_succeeded(), s3_object_present()],
            ),
            OperationSpec(
                name="delete_s3_object",
                description="Delete an object (`aws s3api delete-object`). Irreversible unless bucket versioning is enabled.",
                parameters=key_param,
                capability="s3.write", risk=Risk.IRREVERSIBLE,
                display_name="Delete Object", category="write",
                postconditions=[result_succeeded(), s3_object_absent()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "list_s3_objects":
            return await self._list(args)
        if op == "read_s3_object":
            return await self._read(args)
        if op == "put_s3_object":
            return await self._put(args)
        if op == "delete_s3_object":
            return await self._delete(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    def _cfg(self):
        return getattr(self.settings, "s3", None)

    def _bucket(self, args: dict[str, Any]) -> str:
        cfg = self._cfg()
        return args.get("bucket") or (getattr(cfg, "bucket", "") if cfg else "")

    def _global_flags(self) -> list[str]:
        cfg = self._cfg()
        flags: list[str] = []
        if cfg and getattr(cfg, "profile", ""):
            flags += ["--profile", cfg.profile]
        if cfg and getattr(cfg, "region", ""):
            flags += ["--region", cfg.region]
        return flags

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 300) if cfg else 300

    async def _head(self, bucket: str, key: str) -> bool:
        argv = [self.binary, *self._global_flags(), "s3api", "head-object",
                "--bucket", bucket, "--key", key]
        res = await self._run(argv, timeout=self._timeout())
        return res.ok

    async def _list(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        bucket = self._bucket(args)
        cfg = self._cfg()
        prefix = args.get("prefix")
        if prefix is None and cfg and getattr(cfg, "prefix", ""):
            prefix = cfg.prefix
        argv = [self.binary, *self._global_flags(), "s3api", "list-objects-v2",
                "--bucket", bucket, "--output", "json"]
        if prefix:
            argv += ["--prefix", prefix]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("list_s3_objects",
                              f"list failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}", started)
        objects: list[dict[str, Any]] = []
        try:
            payload = json.loads(res.stdout) if res.stdout.strip() else {}
            objects.extend(
                {"key": c.get("Key"), "size": c.get("Size")}
                for c in payload.get("Contents", []) or []
            )
        except json.JSONDecodeError:
            log.debug("s3 list output was not valid JSON", exc_info=True)
        return self._ok("list_s3_objects",
                        {"bucket": bucket, "prefix": prefix, "objects": objects, "count": len(objects)},
                        started)

    async def _read(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        bucket, key = self._bucket(args), args.get("key", "")
        argv = [self.binary, *self._global_flags(), "s3", "cp", f"s3://{bucket}/{key}", "-"]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("read_s3_object",
                              f"read failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}", started, key=key)
        return self._ok("read_s3_object", {"bucket": bucket, "key": key, "content": res.stdout}, started, key=key)

    async def _put(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        bucket, key = self._bucket(args), args.get("key", "")
        content = args.get("content", "")
        argv = [self.binary, *self._global_flags(), "s3", "cp", "-", f"s3://{bucket}/{key}"]
        res = await self._run(argv, timeout=self._timeout(), stdin=content)
        if not res.ok:
            return self._fail("put_s3_object",
                              f"put failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}", started, key=key)
        exists = await self._head(bucket, key)
        return self._ok("put_s3_object", {"bucket": bucket, "key": key, "exists": exists}, started, key=key)

    async def _delete(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        bucket, key = self._bucket(args), args.get("key", "")
        argv = [self.binary, *self._global_flags(), "s3api", "delete-object",
                "--bucket", bucket, "--key", key]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("delete_s3_object",
                              f"delete failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}", started, key=key)
        exists = await self._head(bucket, key)
        return self._ok("delete_s3_object",
                        {"bucket": bucket, "key": key, "deleted": not exists, "exists": exists}, started, key=key)

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        if getattr(plan, "primitive", "") != "versioned_copy_aside":
            return False, f"no verifiable rollback path for primitive '{plan.primitive}'"
        bucket = self._bucket(args)
        argv = [self.binary, *self._global_flags(), "s3api", "get-bucket-versioning",
                "--bucket", bucket, "--output", "json"]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return False, f"could not read bucket versioning for '{bucket}'"
        try:
            status = (json.loads(res.stdout) if res.stdout.strip() else {}).get("Status")
        except json.JSONDecodeError:
            status = None
        if status == "Enabled":
            return True, f"bucket '{bucket}' versioning enabled — prior version restorable"
        return False, (f"bucket '{bucket}' versioning is '{status or 'disabled'}' — "
                       f"delete/overwrite not reversible")
