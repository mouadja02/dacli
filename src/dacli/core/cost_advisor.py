"""Warehouse cost / credit advisor (P14, slice C).

Two questions, cross-platform (Snowflake / BigQuery / Databricks):

* **before** a query — what will this cost? :func:`estimate` reuses the existing
  ``Connector.estimate_cost`` hook (the same plumbing the F-4 cost gate uses;
  BigQuery answers it exactly via ``bq --dry_run``).
* **after** a session — what did the warehouse spend? :func:`session_cost` reads
  the platform's own history view (Snowflake ``QUERY_HISTORY``, BigQuery
  ``INFORMATION_SCHEMA.JOBS``, Databricks ``system.query.history``) through the
  **governed** read-only query op and aggregates it.

No new deps and no governance bypass: every read runs through the dispatcher, so
it is classified (safe — SELECT only) and audited like any other read. Prices are
public on-demand list rates and intentionally conservative (the worst case is the
useful one for a gate).
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any
from collections.abc import Callable

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)

# Public on-demand list prices (deliberately conservative; the gate should see
# the worst case). Updated by hand — not a live feed.
USD_PER_TIB_BQ = 6.25          # BigQuery on-demand, per TiB scanned
USD_PER_CREDIT_SNOWFLAKE = 3.0  # Snowflake Standard on-demand, per credit
USD_PER_DBU_DATABRICKS = 0.55   # Databricks SQL on-demand, per DBU


def _num(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _col(row: dict[str, Any], *names: str) -> Any:
    """Case-insensitive column read (platforms vary on column casing)."""
    lowered = {str(k).lower(): v for k, v in row.items()}
    for n in names:
        if n.lower() in lowered:
            return lowered[n.lower()]
    return None


@dataclass
class CostEstimate:
    connector: str
    usd: float | None = None
    bytes: int | None = None
    credits: float | None = None
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"connector": self.connector, "usd": self.usd, "bytes": self.bytes,
                "credits": self.credits, "detail": self.detail}


@dataclass
class SessionCost:
    connector: str
    queries: int = 0
    bytes: int | None = None
    credits: float | None = None
    usd: float | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None

    def to_dict(self) -> dict[str, Any]:
        return {"connector": self.connector, "queries": self.queries,
                "bytes": self.bytes, "credits": self.credits, "usd": self.usd,
                "error": self.error}


# ---------------------------------------------------------------------------
# Pre-run estimate (delegates to the connector's native estimator)
# ---------------------------------------------------------------------------
async def estimate(connector: Any, op: str, args: dict[str, Any]) -> CostEstimate | None:
    """Pre-run cost preview via ``Connector.estimate_cost`` (None when unavailable)."""
    hook = getattr(connector, "estimate_cost", None)
    if not callable(hook):
        return None
    try:
        est = hook(op, dict(args or {}))
        if inspect.isawaitable(est):
            est = await est
    except Exception:
        log.debug("estimate_cost hook raised", exc_info=True)
        return None
    if not isinstance(est, dict):
        return None
    usd = est.get("usd")
    bits = []
    if est.get("bytes") is not None:
        bits.append(f"{est['bytes']:,} bytes")
    if usd is not None:
        bits.append(f"≈ ${usd:,.2f}")
    return CostEstimate(
        connector=getattr(connector, "name", "") or "",
        usd=usd, bytes=est.get("bytes"), credits=est.get("credits"),
        detail="  ·  ".join(bits) or "(no detail)")


# ---------------------------------------------------------------------------
# Post-hoc session cost (per-platform history view through the governed path)
# ---------------------------------------------------------------------------
def _bq_aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = sum(_num(_col(r, "total_bytes_billed", "total_bytes_processed")) for r in rows)
    return {"queries": len(rows), "bytes": int(total), "credits": None,
            "usd": round(total / 2**40 * USD_PER_TIB_BQ, 6)}


def _snowflake_aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    credits = sum(_num(_col(r, "credits_used", "credits_used_cloud_services")) for r in rows)
    scanned = sum(_num(_col(r, "bytes_scanned")) for r in rows)
    return {"queries": len(rows), "bytes": int(scanned) or None, "credits": round(credits, 6),
            "usd": round(credits * USD_PER_CREDIT_SNOWFLAKE, 6)}


def _databricks_aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dbus = sum(_num(_col(r, "usage_quantity", "dbus")) for r in rows)
    return {"queries": len(rows), "bytes": None, "credits": round(dbus, 6),
            "usd": round(dbus * USD_PER_DBU_DATABRICKS, 6)}


@dataclass
class _Platform:
    sql: str
    aggregate: Callable[[list[dict[str, Any]]], dict[str, Any]]


_PLATFORMS: dict[str, _Platform] = {
    "bigquery": _Platform(
        sql=("SELECT total_bytes_billed FROM "
             "`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
             "WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) "
             "ORDER BY creation_time DESC LIMIT {limit}"),
        aggregate=_bq_aggregate),
    "snowflake": _Platform(
        sql=("SELECT CREDITS_USED_CLOUD_SERVICES, BYTES_SCANNED "
             "FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) "
             "ORDER BY START_TIME DESC LIMIT {limit}"),
        aggregate=_snowflake_aggregate),
    "databricks": _Platform(
        sql=("SELECT usage_quantity FROM system.billing.usage "
             "WHERE usage_date >= current_date() - INTERVAL 1 DAY LIMIT {limit}"),
        aggregate=_databricks_aggregate),
}


def supports_cost(connector_id: str) -> bool:
    return connector_id in _PLATFORMS


def _query_tool(registry: Any, connector_id: str) -> str:
    get = getattr(registry, "get_connector", None)
    conn: Any = get(connector_id) if callable(get) else None
    if conn is not None:
        for op in conn.operations():
            if op.capability == f"{connector_id}.query":
                return op.name
    return f"execute_{connector_id}_query"


async def session_cost(connector_id: str, dispatcher: Any, *, limit: int = 200) -> SessionCost:
    """Aggregate recent warehouse spend from the platform's history view.

    Runs one read-only history query through the governed dispatcher. A blocked
    read or a connector without a cost view degrades to an error outcome.
    """
    plat = _PLATFORMS.get(connector_id)
    if plat is None:
        return SessionCost(connector=connector_id,
                           error=f"no warehouse cost view for '{connector_id}'")
    registry = getattr(dispatcher, "_registry", None) or getattr(dispatcher, "registry", None)
    tool = _query_tool(registry, connector_id)
    res = await dispatcher.execute(tool, {"query": plat.sql.format(limit=limit)})
    status = getattr(getattr(res, "status", None), "value", None)
    if status in ("denied", "blocked"):
        return SessionCost(connector=connector_id,
                           error=f"history query blocked by governance: {res.error}")
    if not res.success:
        return SessionCost(connector=connector_id,
                           error=f"history query failed: {res.error}")
    rows = [r for r in (res.data if isinstance(res.data, list) else []) if isinstance(r, dict)]
    return SessionCost(connector=connector_id, **plat.aggregate(rows))
