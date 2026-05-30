"""Model pricing via the models.dev API, plus token-usage accounting.

dacli tracks how many tokens each LLM call consumes and what it costs. Pricing
is looked up from https://models.dev/api.json (a community database of model
specs/pricing), filtered to the configured provider + model. The payload is
cached on disk with a TTL so we don't hit the network every turn, and we degrade
gracefully when offline (tokens are still tracked; cost is reported as unknown).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

MODELS_DEV_URL = "https://models.dev/api.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # refresh pricing at most once a day


@dataclass
class TokenUsage:
    """Token counts for one or many LLM calls (provider-normalized)."""

    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_creation: int = 0

    def add(self, other: "TokenUsage") -> None:
        self.input += other.input
        self.output += other.output
        self.cache_read += other.cache_read
        self.cache_creation += other.cache_creation

    @property
    def total(self) -> int:
        return self.input + self.output + self.cache_read + self.cache_creation

    def as_dict(self) -> Dict[str, int]:
        return {
            "input": self.input,
            "output": self.output,
            "cache_read": self.cache_read,
            "cache_creation": self.cache_creation,
        }

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "TokenUsage":
        d = d or {}
        return cls(
            input=int(d.get("input", 0) or 0),
            output=int(d.get("output", 0) or 0),
            cache_read=int(d.get("cache_read", 0) or 0),
            cache_creation=int(d.get("cache_creation", 0) or 0),
        )


@dataclass
class ModelPricing:
    """USD cost per 1M tokens for a single model (models.dev `cost` block)."""

    provider: str
    model: str
    input: float = 0.0
    output: float = 0.0
    cache_read: float = 0.0
    cache_write: float = 0.0

    def cost_for(self, usage: TokenUsage) -> float:
        """Compute USD cost for a usage record (prices are per 1M tokens)."""
        return (
            usage.input * self.input
            + usage.output * self.output
            + usage.cache_read * self.cache_read
            + usage.cache_creation * self.cache_write
        ) / 1_000_000


# ----------------------------------------------------------------------------
# models.dev lookup
# ----------------------------------------------------------------------------
def _ci_get(d: Any, key: str) -> Any:
    """Case-insensitive dict lookup (model/provider ids vary in casing)."""
    if not isinstance(d, dict) or not key:
        return None
    if key in d:
        return d[key]
    lowered = key.lower()
    for k, v in d.items():
        if isinstance(k, str) and k.lower() == lowered:
            return v
    return None


def _find_model(payload: Any, provider: str, model: str) -> Optional[Tuple[dict, dict]]:
    """Locate (provider_entry, model_entry) for provider+model in the payload.

    Tries the named provider first, then falls back to scanning every provider
    for the model id (e.g. an OpenRouter-routed Anthropic model).
    """
    if not isinstance(payload, dict) or not model:
        return None

    prov = _ci_get(payload, provider)
    if isinstance(prov, dict):
        m = _ci_get(prov.get("models", {}), model)
        if isinstance(m, dict):
            return prov, m

    for pval in payload.values():
        if not isinstance(pval, dict):
            continue
        m = _ci_get(pval.get("models", {}), model)
        if isinstance(m, dict):
            return pval, m
    return None


def pricing_from_payload(payload: Any, provider: str, model: str) -> Optional[ModelPricing]:
    """Build :class:`ModelPricing` from an in-memory api.json payload (pure)."""
    found = _find_model(payload, (provider or "").strip(), (model or "").strip())
    if not found:
        return None
    _, entry = found
    cost = entry.get("cost") or {}
    return ModelPricing(
        provider=provider,
        model=model,
        input=float(cost.get("input", 0) or 0),
        output=float(cost.get("output", 0) or 0),
        cache_read=float(cost.get("cache_read", 0) or 0),
        cache_write=float(cost.get("cache_write", 0) or 0),
    )


# ----------------------------------------------------------------------------
# cached fetch
# ----------------------------------------------------------------------------
def _cache_path(cache_dir: str) -> Path:
    return Path(cache_dir) / "models_cache.json"


def _load_cache(cache_dir: str) -> Tuple[float, Any]:
    try:
        data = json.loads(_cache_path(cache_dir).read_text(encoding="utf-8"))
        return float(data.get("fetched_at", 0)), data.get("payload")
    except Exception:
        return 0.0, None


def _save_cache(cache_dir: str, payload: Any) -> None:
    try:
        path = _cache_path(cache_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"fetched_at": time.time(), "payload": payload}),
            encoding="utf-8",
        )
    except Exception:
        pass  # caching is best-effort


def fetch_api_json(cache_dir: str = ".dacli", force_refresh: bool = False) -> Any:
    """Return the models.dev payload, using a TTL cache and offline fallback."""
    fetched_at, payload = _load_cache(cache_dir)
    fresh = payload is not None and (time.time() - fetched_at) < CACHE_TTL_SECONDS
    if fresh and not force_refresh:
        return payload

    try:
        import httpx

        resp = httpx.get(MODELS_DEV_URL, timeout=10.0)
        resp.raise_for_status()
        payload = resp.json()
        _save_cache(cache_dir, payload)
        return payload
    except Exception:
        return payload  # stale cache or None when offline


def fetch_pricing(
    provider: str,
    model: str,
    cache_dir: str = ".dacli",
    force_refresh: bool = False,
) -> Optional[ModelPricing]:
    """Resolve pricing for provider+model, or ``None`` if unavailable/offline."""
    payload = fetch_api_json(cache_dir, force_refresh=force_refresh)
    return pricing_from_payload(payload, provider, model)
