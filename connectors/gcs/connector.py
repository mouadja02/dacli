"""GCS connector (Wave 1) — CLI-first via `gcloud storage`.

Mirrors the S3 connector for Google Cloud Storage. Governance: object
**versioning** is the rollback primitive — a delete/overwrite is only allowed once
versioning is verified, so a prior generation can be restored.

Post-conditions are environment-anchored: after a put/delete the connector
re-`ls`-es the object against live GCS and the check asserts on that live truth.
"""

from __future__ import annotations

import json
import time
from typing import Any

from connectors.base import OperationSpec, Risk, ToolResult
from core.logging_setup import get_logger

log = get_logger(__name__)
from connectors.cli_base import CliConnector
from core.verify import PostCondition, VerificationContext, result_succeeded


def gcs_object_present() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if data.get("exists") is not True:
            return False, f"object '{data.get('key')}' not found after put"
        return True, ""
    return PostCondition("gcs_object_present", check,
                         "uploaded object is present per gcloud storage ls", anchored=True)


def gcs_object_absent() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if data.get("exists") is not False:
            return False, f"object '{data.get('key')}' still present after delete"
        return True, ""
    return PostCondition("gcs_object_absent", check,
                         "deleted object is gone per gcloud storage ls", anchored=True)


def lists_objects() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if "objects" not in data:
            return False, "listing did not return an 'objects' array"
        return True, ""
    return PostCondition("lists_objects", check, "listing returns an objects array", anchored=True)


class GCSConnector(CliConnector):
    name = "gcs"
    binary = "gcloud"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        cfg = getattr(settings, "gcs", None)
        self.binary = getattr(cfg, "gcloud_binary", "gcloud") or "gcloud"

    def operations(self) -> list[OperationSpec]:
        key_param = {
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Object name (path within the bucket)."},
                "bucket": {"type": "string", "description": "Bucket (defaults to the configured bucket)."},
            },
            "required": ["key"],
        }
        return [
            OperationSpec(
                name="list_gcs_objects",
                description="List objects under a prefix (`gcloud storage ls`). Read-only; re-verifies live lake state.",
                parameters={
                    "type": "object",
                    "properties": {
                        "prefix": {"type": "string"},
                        "bucket": {"type": "string"},
                    },
                },
                capability="gcs.introspection", risk=Risk.SAFE,
                display_name="List Objects", category="introspection",
                postconditions=[lists_objects()],
            ),
            OperationSpec(
                name="read_gcs_object",
                description="Read an object's contents (`gcloud storage cat`).",
                parameters=key_param,
                capability="gcs.read", risk=Risk.SAFE,
                display_name="Read Object", category="read",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="put_gcs_object",
                description="Upload content to a key (`gcloud storage cp - gs://…`). Overwrites if the key exists.",
                parameters={
                    "type": "object",
                    "properties": {
                        "key": {"type": "string"},
                        "content": {"type": "string"},
                        "bucket": {"type": "string"},
                    },
                    "required": ["key", "content"],
                },
                capability="gcs.write", risk=Risk.WRITE,
                display_name="Put Object", category="write",
                postconditions=[result_succeeded(), gcs_object_present()],
            ),
            OperationSpec(
                name="delete_gcs_object",
                description="Delete an object (`gcloud storage rm`). Irreversible unless object versioning is enabled.",
                parameters=key_param,
                capability="gcs.write", risk=Risk.IRREVERSIBLE,
                display_name="Delete Object", category="write",
                postconditions=[result_succeeded(), gcs_object_absent()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "list_gcs_objects":
            return await self._list(args)
        if op == "read_gcs_object":
            return await self._read(args)
        if op == "put_gcs_object":
            return await self._put(args)
        if op == "delete_gcs_object":
            return await self._delete(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    def _cfg(self):
        return getattr(self.settings, "gcs", None)

    def _bucket(self, args: dict[str, Any]) -> str:
        cfg = self._cfg()
        return args.get("bucket") or (getattr(cfg, "bucket", "") if cfg else "")

    def _global_flags(self) -> list[str]:
        cfg = self._cfg()
        flags: list[str] = []
        if cfg and getattr(cfg, "project", ""):
            flags += [f"--project={cfg.project}"]
        return flags

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 300) if cfg else 300

    def _uri(self, bucket: str, key: str) -> str:
        return f"gs://{bucket}/{key}"

    async def _exists(self, bucket: str, key: str) -> bool:
        argv = [self.binary, "storage", "ls", self._uri(bucket, key), "--format=json", *self._global_flags()]
        res = await self._run(argv, timeout=self._timeout())
        return res.ok and bool(res.stdout.strip()) and res.stdout.strip() != "[]"

    async def _list(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        bucket = self._bucket(args)
        cfg = self._cfg()
        prefix = args.get("prefix")
        if prefix is None and cfg and getattr(cfg, "prefix", ""):
            prefix = cfg.prefix
        uri = self._uri(bucket, prefix or "")
        argv = [self.binary, "storage", "ls", uri, "--format=json", *self._global_flags()]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("list_gcs_objects",
                              f"list failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}", started)
        objects: list[dict[str, Any]] = []
        try:
            payload = json.loads(res.stdout) if res.stdout.strip() else []
            for item in payload:
                if isinstance(item, dict):
                    objects.append({"key": item.get("url") or item.get("name"),
                                    "size": item.get("size")})
                else:
                    objects.append({"key": str(item), "size": None})
        except json.JSONDecodeError:
            log.debug("gcs list output was not valid JSON", exc_info=True)
        return self._ok("list_gcs_objects",
                        {"bucket": bucket, "prefix": prefix, "objects": objects, "count": len(objects)},
                        started)

    async def _read(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        bucket, key = self._bucket(args), args.get("key", "")
        argv = [self.binary, "storage", "cat", self._uri(bucket, key), *self._global_flags()]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("read_gcs_object",
                              f"read failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}", started, key=key)
        return self._ok("read_gcs_object", {"bucket": bucket, "key": key, "content": res.stdout}, started, key=key)

    async def _put(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        bucket, key = self._bucket(args), args.get("key", "")
        content = args.get("content", "")
        argv = [self.binary, "storage", "cp", "-", self._uri(bucket, key), *self._global_flags()]
        res = await self._run(argv, timeout=self._timeout(), stdin=content)
        if not res.ok:
            return self._fail("put_gcs_object",
                              f"put failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}", started, key=key)
        exists = await self._exists(bucket, key)
        return self._ok("put_gcs_object", {"bucket": bucket, "key": key, "exists": exists}, started, key=key)

    async def _delete(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        bucket, key = self._bucket(args), args.get("key", "")
        argv = [self.binary, "storage", "rm", self._uri(bucket, key), *self._global_flags()]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return self._fail("delete_gcs_object",
                              f"delete failed (rc={res.rc}): {(res.stderr or res.stdout)[-1000:]}", started, key=key)
        exists = await self._exists(bucket, key)
        return self._ok("delete_gcs_object",
                        {"bucket": bucket, "key": key, "deleted": not exists, "exists": exists}, started, key=key)

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        if getattr(plan, "primitive", "") != "versioned_copy_aside":
            return False, f"no verifiable rollback path for primitive '{plan.primitive}'"
        bucket = self._bucket(args)
        argv = [self.binary, "storage", "buckets", "describe", f"gs://{bucket}",
                "--format=json", *self._global_flags()]
        res = await self._run(argv, timeout=self._timeout())
        if not res.ok:
            return False, f"could not read bucket versioning for '{bucket}'"
        enabled = False
        try:
            meta = json.loads(res.stdout) if res.stdout.strip() else {}
            enabled = bool((meta.get("versioning") or {}).get("enabled"))
        except json.JSONDecodeError:
            log.debug("gcs versioning output was not valid JSON", exc_info=True)
        if enabled:
            return True, f"bucket '{bucket}' versioning enabled — prior generation restorable"
        return False, f"bucket '{bucket}' versioning disabled — delete/overwrite not reversible"
