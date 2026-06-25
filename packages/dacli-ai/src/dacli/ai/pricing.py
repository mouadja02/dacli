"""Model pricing via the models.dev API, plus token-usage accounting.

dacli tracks how many tokens each LLM call consumes and what it costs. Pricing
is looked up from https://models.dev/api.json (a community database of model
specs/pricing), filtered to the configured provider + model. The payload is
cached on disk with a TTL so we don't hit the network every turn, and we degrade
gracefully when offline (tokens are still tracked; cost is reported as unknown).
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _write_json_atomic(path: Path, obj: Any) -> None:
    # ai is the leaf wheel and can't reach core.atomicio. The cache is best-effort
    # (a torn write just forces a re-fetch), but cheap crash-safety is still worth
    # it: write a sibling temp, fsync, os.replace (atomic on POSIX + Windows).
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(json.dumps(obj))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)

MODELS_DEV_URL = "https://models.dev/api.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # refresh pricing at most once a day
# Short network timeout: pricing is a startup nicety, not a blocker. A
# first-run offline user (no cache yet) must not wait long before we fall back to
# "cost unknown". Keep it well under a human's patience threshold.
HTTP_TIMEOUT_SECONDS = 5.0

# Minimum similarity score for a fuzzy model match to be trusted. Below this we
# return no pricing (better an honest "unknown" than a wrong, confident price).
SIMILARITY_THRESHOLD = 0.62


@dataclass
class TokenUsage:
    """Token counts for one or many LLM calls (provider-normalized)."""

    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_creation: int = 0

    def add(self, other: TokenUsage) -> None:
        self.input += other.input
        self.output += other.output
        self.cache_read += other.cache_read
        self.cache_creation += other.cache_creation

    @property
    def total(self) -> int:
        return self.input + self.output + self.cache_read + self.cache_creation

    def as_dict(self) -> dict[str, int]:
        return {
            "input": self.input,
            "output": self.output,
            "cache_read": self.cache_read,
            "cache_creation": self.cache_creation,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> TokenUsage:
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
    # The models.dev entry we actually priced against. Equal to ``model`` on an
    # exact hit; on a fuzzy hit it names the closest catalog model (e.g.
    # ``openai/gpt-oss-120b`` for a requested ``openai/gpt-oss-120b:nitro``).
    resolved_model: str = ""
    resolved_provider: str = ""
    match: str = "exact"           # "exact" | "normalized" | "similar"
    similarity: float = 1.0

    @property
    def is_fuzzy(self) -> bool:
        return self.match != "exact"

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


# Provider-routing variant suffixes (OpenRouter et al.) that don't change the
# underlying model's price — stripped before matching, e.g.
# ``openai/gpt-oss-120b:nitro`` -> ``openai/gpt-oss-120b``.
_VARIANT_SUFFIX_RE = re.compile(r":[^:/]+$")


def _normalize_model_id(model: str) -> str:
    """Lowercase + drop the routing-variant suffix for matching."""
    s = (model or "").strip().lower()
    # Strip a trailing ``:variant`` (nitro/floor/free/beta/extended/online/...).
    # models.dev ids never carry a ``:`` so this only removes routing noise.
    s = _VARIANT_SUFFIX_RE.sub("", s)
    return s.strip()


def _basename(model_id: str) -> str:
    # The vendor/model -> model part ("openai/gpt-oss-120b" -> "gpt-oss-120b").
    return model_id.rsplit("/", 1)[-1]


def _iter_models(payload: dict, provider: str):
    """Yield ``(provider_id, provider_entry, model_id, model_entry)``.

    The configured provider's models come first so a routed model is priced
    against *that* provider's catalog (e.g. OpenRouter pricing for an
    OpenRouter-routed model) before falling back to other providers.
    """
    seen_provider = None
    prov = _ci_get(payload, provider)
    if isinstance(prov, dict):
        seen_provider = next((k for k in payload if k.lower() == (provider or "").lower()), provider)
        for mid, entry in (prov.get("models", {}) or {}).items():
            if isinstance(entry, dict):
                yield seen_provider, prov, mid, entry
    for pid, pval in payload.items():
        if pid == seen_provider or not isinstance(pval, dict):
            continue
        for mid, entry in (pval.get("models", {}) or {}).items():
            if isinstance(entry, dict):
                yield pid, pval, mid, entry


def _score(query_norm: str, candidate_id: str) -> float:
    """Similarity in [0,1] between a normalized query and a candidate model id."""
    cand_norm = _normalize_model_id(candidate_id)
    if query_norm == cand_norm:
        return 1.0
    # A matching basename is a strong signal even if the vendor prefix differs.
    base_q, base_c = _basename(query_norm), _basename(cand_norm)
    if base_q == base_c:
        return 0.97
    full = SequenceMatcher(None, query_norm, cand_norm).ratio()
    base = SequenceMatcher(None, base_q, base_c).ratio()
    # Reward containment (e.g. "gpt-oss-120b" inside "openai/gpt-oss-120b").
    contain = 0.9 if (base_q and base_q in base_c) or (base_c and base_c in base_q) else 0.0
    return max(full, base, contain)


def _find_model(payload: Any, provider: str, model: str) -> tuple[dict, dict, str, str, str, float] | None:
    """Locate the best (provider_entry, model_entry, ...) for provider+model.

    Returns ``(provider_entry, model_entry, resolved_provider_id, resolved_model_id,
    match_kind, similarity)`` or ``None``. Match resolution, in order:

    1. **exact** case-insensitive id in the named provider, then any provider;
    2. **normalized** exact (after stripping the routing-variant suffix);
    3. **similar** — the closest catalog id by similarity, above a threshold,
       preferring the configured provider when its best match ties.
    """
    if not isinstance(payload, dict) or not model:
        return None

    # 1. exact (preserves the original behavior + tests).
    prov = _ci_get(payload, provider)
    if isinstance(prov, dict):
        m = _ci_get(prov.get("models", {}), model)
        if isinstance(m, dict):
            return prov, m, provider, model, "exact", 1.0
    for pid, pval in payload.items():
        if not isinstance(pval, dict):
            continue
        m = _ci_get(pval.get("models", {}), model)
        if isinstance(m, dict):
            return pval, m, pid, model, "exact", 1.0

    # 2 & 3. normalized-exact + similarity over all candidates, provider-first.
    query_norm = _normalize_model_id(model)
    if not query_norm:
        return None

    best = None  # (score, is_same_provider, provider_id, prov_entry, model_id, entry)
    same_provider_id = next((k for k in payload if k.lower() == (provider or "").lower()), None)
    for pid, pval, mid, entry in _iter_models(payload, provider):
        score = _score(query_norm, mid)
        same = (pid == same_provider_id)
        # A normalized-exact hit (score 1.0) wins immediately within the
        # provider-first ordering.
        if score >= 1.0 and same:
            return pval, entry, pid, mid, "normalized", 1.0
        cand = (score, same, pid, pval, mid, entry)
        if best is None or (score, same) > (best[0], best[1]):
            best = cand

    if best and best[0] >= SIMILARITY_THRESHOLD:
        score, _same, pid, pval, mid, entry = best
        kind = "normalized" if score >= 1.0 else "similar"
        return pval, entry, pid, mid, kind, round(score, 3)
    return None


def pricing_from_payload(payload: Any, provider: str, model: str) -> ModelPricing | None:
    """Build :class:`ModelPricing` from an in-memory api.json payload (pure).

    Falls back to a similarity search when an exact id isn't in the catalog, so
    a routed/variant model (``…:nitro``) is priced against its closest match.
    """
    found = _find_model(payload, (provider or "").strip(), (model or "").strip())
    if not found:
        return None
    _prov_entry, entry, resolved_provider, resolved_model, kind, similarity = found
    cost = entry.get("cost") or {}
    return ModelPricing(
        provider=provider,
        model=model,
        input=float(cost.get("input", 0) or 0),
        output=float(cost.get("output", 0) or 0),
        cache_read=float(cost.get("cache_read", 0) or 0),
        cache_write=float(cost.get("cache_write", 0) or 0),
        resolved_model=resolved_model,
        resolved_provider=resolved_provider,
        match=kind,
        similarity=similarity,
    )


# ----------------------------------------------------------------------------
# cached fetch
# ----------------------------------------------------------------------------
def _cache_path(cache_dir: str) -> Path:
    return Path(cache_dir) / "models_cache.json"


def _load_cache(cache_dir: str) -> tuple[float, Any]:
    try:
        data = json.loads(_cache_path(cache_dir).read_text(encoding="utf-8"))
        return float(data.get("fetched_at", 0)), data.get("payload")
    except Exception:
        return 0.0, None


def _save_cache(cache_dir: str, payload: Any) -> None:
    try:
        path = _cache_path(cache_dir)
        _write_json_atomic(path, {"fetched_at": time.time(), "payload": payload})
    except Exception:
        log.debug("pricing cache write failed", exc_info=True)  # best-effort


def fetch_api_json(cache_dir: str = ".dacli", force_refresh: bool = False) -> Any:
    """Return the models.dev payload, using a TTL cache and offline fallback."""
    fetched_at, payload = _load_cache(cache_dir)
    fresh = payload is not None and (time.time() - fetched_at) < CACHE_TTL_SECONDS
    if fresh and not force_refresh:
        return payload

    try:
        import httpx

        resp = httpx.get(MODELS_DEV_URL, timeout=HTTP_TIMEOUT_SECONDS)
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
) -> ModelPricing | None:
    """Resolve pricing for provider+model, or ``None`` if unavailable/offline."""
    payload = fetch_api_json(cache_dir, force_refresh=force_refresh)
    return pricing_from_payload(payload, provider, model)
