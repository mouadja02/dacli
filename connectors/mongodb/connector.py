"""MongoDB connector (Wave 2) — CLI-first via `mongosh`.

The first NoSQL connector — included deliberately to **prove the connector
contract isn't secretly SQL-only**. It forces two honest generalizations:

* **Schemaless catalog** — there is no `information_schema`, so introspection
  *infers* a schema by sampling documents (field → observed-type histogram).
* **No SQL oracle for post-conditions** — checks lean on driver
  acknowledgements and document counts/shape, not a CREATE-matches-intent oracle.

Rollback is honest too: MongoDB has no general native undo, so a delete is backed
by a `mongodump` copy-aside, verified to be possible before the mutation runs.
"""

from __future__ import annotations

import json
import time
from typing import Any

from connectors.base import OperationSpec, Risk, ToolResult
from connectors.cli_base import CliConnector
from core.verify import PostCondition, VerificationContext, result_succeeded, data_is_list


def mongo_insert_acknowledged() -> PostCondition:
    """The driver acknowledged the insert and inserted the expected count."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if not data.get("acknowledged"):
            return False, "insert was not acknowledged by the server"
        inserted = data.get("insertedCount")
        want = len(ctx.args.get("documents") or [])
        if want and inserted is not None and inserted != want:
            return False, f"inserted {inserted} of {want} documents"
        if inserted == 0:
            return False, "insert acknowledged but 0 documents inserted"
        return True, ""
    return PostCondition(
        "mongo_insert_acknowledged", check,
        "server acknowledged the insert with the expected count", anchored=True,
    )


def mongo_delete_acknowledged() -> PostCondition:
    """The server acknowledged the delete and reported a deleted count."""
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if not data.get("acknowledged"):
            return False, "delete was not acknowledged by the server"
        if "deletedCount" not in data:
            return False, "no deletedCount reported"
        return True, ""
    return PostCondition(
        "mongo_delete_acknowledged", check,
        "server acknowledged the delete with a deletedCount", anchored=True,
    )


def introspect_reports_structure() -> PostCondition:
    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None)
        if not isinstance(data, dict) or "exists" not in data:
            return False, "introspection did not return {exists, ...}"
        return True, ""
    return PostCondition(
        "introspect_reports_structure", check,
        "introspection returns a definite existence verdict + inferred schema", anchored=True,
    )


class MongoDBConnector(CliConnector):
    name = "mongodb"
    binary = "mongosh"

    def __init__(self, settings: Any, runner=None):
        super().__init__(settings, runner=runner)
        cfg = getattr(settings, "mongodb", None)
        self.binary = getattr(cfg, "mongosh_binary", "mongosh") or "mongosh"

    def operations(self) -> list[OperationSpec]:
        coll = {"type": "string", "description": "Collection name."}
        flt = {"type": "object", "description": "Query filter (MongoDB query document)."}
        return [
            OperationSpec(
                name="find_mongodb_documents",
                description="Find documents in a collection (read-only).",
                parameters={
                    "type": "object",
                    "properties": {
                        "collection": coll, "filter": flt,
                        "limit": {"type": "integer", "description": "Max documents (default 20)."},
                    },
                    "required": ["collection"],
                },
                capability="mongodb.read", risk=Risk.SAFE,
                display_name="Find Documents", category="read",
                postconditions=[data_is_list(name="returns_documents")],
            ),
            OperationSpec(
                name="count_mongodb_documents",
                description="Count documents matching a filter (read-only).",
                parameters={
                    "type": "object",
                    "properties": {"collection": coll, "filter": flt},
                    "required": ["collection"],
                },
                capability="mongodb.read", risk=Risk.SAFE,
                display_name="Count Documents", category="read",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="insert_mongodb_documents",
                description="Insert one or more documents into a collection.",
                parameters={
                    "type": "object",
                    "properties": {
                        "collection": coll,
                        "documents": {"type": "array", "items": {"type": "object"},
                                      "description": "Documents to insert."},
                    },
                    "required": ["collection", "documents"],
                },
                capability="mongodb.write", risk=Risk.WRITE,
                display_name="Insert Documents", category="write",
                postconditions=[result_succeeded(), mongo_insert_acknowledged()],
            ),
            OperationSpec(
                name="delete_mongodb_documents",
                description="Delete documents matching a filter (deleteMany). Irreversible without a snapshot.",
                parameters={
                    "type": "object",
                    "properties": {"collection": coll, "filter": flt},
                    "required": ["collection", "filter"],
                },
                capability="mongodb.write", risk=Risk.IRREVERSIBLE,
                display_name="Delete Documents", category="write",
                postconditions=[result_succeeded(), mongo_delete_acknowledged()],
            ),
            OperationSpec(
                name="introspect_mongodb_collection",
                description="Infer a collection's schema by sampling documents (field → type histogram) + an estimated count. Feeds the catalog.",
                parameters={
                    "type": "object",
                    "properties": {"collection": coll},
                    "required": ["collection"],
                },
                capability="mongodb.introspection", risk=Risk.SAFE,
                display_name="Introspect Collection", category="introspection",
                postconditions=[introspect_reports_structure()],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        args = dict(args or {})
        if op == "find_mongodb_documents":
            return await self._find(args)
        if op == "count_mongodb_documents":
            return await self._count(args)
        if op == "insert_mongodb_documents":
            return await self._insert(args)
        if op == "delete_mongodb_documents":
            return await self._delete(args)
        if op == "introspect_mongodb_collection":
            return await self._introspect(args)
        return self._unknown_op(op)

    # ------------------------------------------------------------------
    def _cfg(self):
        return getattr(self.settings, "mongodb", None)

    def _timeout(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "timeout", 300) if cfg else 300

    def _db(self) -> str:
        cfg = self._cfg()
        return (getattr(cfg, "database", "") if cfg else "") or "test"

    def _sample_size(self) -> int:
        cfg = self._cfg()
        return getattr(cfg, "sample_size", 100) if cfg else 100

    def _coll_expr(self, collection: str) -> str:
        return (f"db.getSiblingDB({json.dumps(self._db())})"
                f".getCollection({json.dumps(collection)})")

    async def _eval(self, js: str):
        cfg = self._cfg()
        uri = getattr(cfg, "uri", "") if cfg else ""
        argv = [self.binary]
        if uri:
            argv.append(uri)
        argv += ["--quiet", "--eval", js]
        return await self._run(argv, timeout=self._timeout())

    @staticmethod
    def _parse(text: str) -> Any:
        try:
            return json.loads(text) if text.strip() else None
        except json.JSONDecodeError:
            return None

    async def _find(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        coll = self._coll_expr(args.get("collection", ""))
        flt = json.dumps(args.get("filter") or {})
        limit = int(args.get("limit") or 20)
        js = f"print(EJSON.stringify({coll}.find({flt}).limit({limit}).toArray()))"
        res = await self._eval(js)
        if not res.ok:
            return self._fail("find_mongodb_documents",
                              f"find failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        docs = self._parse(res.stdout)
        return self._ok("find_mongodb_documents", docs if isinstance(docs, list) else [], started)

    async def _count(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        coll = self._coll_expr(args.get("collection", ""))
        flt = json.dumps(args.get("filter") or {})
        js = f"print(EJSON.stringify({{count: {coll}.countDocuments({flt})}}))"
        res = await self._eval(js)
        if not res.ok:
            return self._fail("count_mongodb_documents",
                              f"count failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        return self._ok("count_mongodb_documents", self._parse(res.stdout) or {"count": 0}, started)

    async def _insert(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        coll = self._coll_expr(args.get("collection", ""))
        docs = json.dumps(args.get("documents") or [])
        js = (f"var r = {coll}.insertMany(EJSON.parse({json.dumps(docs)})); "
              f"print(EJSON.stringify({{acknowledged: r.acknowledged, "
              f"insertedCount: Object.keys(r.insertedIds).length}}))")
        res = await self._eval(js)
        if not res.ok:
            return self._fail("insert_mongodb_documents",
                              f"insert failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        data = self._parse(res.stdout) or {}
        return self._ok("insert_mongodb_documents", data, started,
                        catalog_effects=[{"action": "invalidate", "object_type": "collection",
                                          "scope": {"object": args.get("collection")}}])

    async def _delete(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        coll = self._coll_expr(args.get("collection", ""))
        flt = json.dumps(args.get("filter") or {})
        js = (f"var r = {coll}.deleteMany({flt}); "
              f"print(EJSON.stringify({{acknowledged: r.acknowledged, deletedCount: r.deletedCount}}))")
        res = await self._eval(js)
        if not res.ok:
            return self._fail("delete_mongodb_documents",
                              f"delete failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        data = self._parse(res.stdout) or {}
        return self._ok("delete_mongodb_documents", data, started,
                        catalog_effects=[{"action": "invalidate", "object_type": "collection",
                                          "scope": {"object": args.get("collection")}}])

    async def _introspect(self, args: dict[str, Any]) -> ToolResult:
        started = time.time()
        collection = args.get("collection", "")
        coll = self._coll_expr(collection)
        n = self._sample_size()
        js = (
            f"var c = {coll}; var cnt = c.estimatedDocumentCount(); var f = {{}}; "
            f"c.aggregate([{{$sample: {{size: {n}}}}}]).forEach(function(d){{"
            f"Object.keys(d).forEach(function(k){{"
            f"var t = Array.isArray(d[k]) ? 'array' : (d[k]===null?'null':typeof d[k]); "
            f"f[k] = f[k] || {{}}; f[k][t] = (f[k][t]||0)+1; }}); }}); "
            f"print(EJSON.stringify({{exists: (cnt>0 || Object.keys(f).length>0), "
            f"count: cnt, fields: f}}))"
        )
        res = await self._eval(js)
        if not res.ok:
            return self._fail("introspect_mongodb_collection",
                              f"introspection failed (rc={res.rc}): {(res.stderr or res.stdout)[-1200:]}", started)
        data = self._parse(res.stdout) or {"exists": False, "count": 0, "fields": {}}
        scope = {"database": self._db(), "object": collection}
        cols = [{"name": k, "type": "|".join(sorted((v or {}).keys()))}
                for k, v in (data.get("fields") or {}).items()] or None
        effects = ([{"action": "create", "object_type": "collection", "scope": scope,
                     "source": "mongodb.sample_inference", "columns": cols}]
                   if data.get("exists") else [])
        data["scope"] = scope
        return self._ok("introspect_mongodb_collection", data, started,
                        scope=scope, catalog_effects=effects)

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        if getattr(plan, "primitive", "") != "mongodump_snapshot":
            return False, f"no verifiable rollback path for primitive '{plan.primitive}'"
        collection = args.get("collection", "")
        res = await self.invoke("introspect_mongodb_collection", {"collection": collection})
        data = getattr(res, "data", None) or {}
        if data.get("exists"):
            return True, (f"collection '{collection}' exists — mongodump copy-aside is "
                          f"possible before the delete (MongoDB has no native undo)")
        return False, f"collection '{collection}' not found — cannot snapshot a restore point"
