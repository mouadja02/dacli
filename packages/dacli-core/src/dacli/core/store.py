"""Persistent project state for dacli — the ``.claude.json`` analogue.

Writes ``.dacli/dacli.json`` next to the session ``state/`` and ``history/``
dirs. Holds: startup counters, a **secret-redacted** snapshot of the effective
config, and accumulated token/cost usage (all-time totals, split by model, plus
a per-session breakdown). This is what the ``/usage`` command renders.

Credentials in the ``secrets`` block are Fernet-encrypted (see :mod:`core.crypto`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dacli.core.atomicio import write_json_atomic
from dacli.core.logging_setup import get_logger
from dacli.ai.pricing import TokenUsage
from dacli.core.timeutils import now_iso as _now_iso

log = get_logger(__name__)

# Config keys whose values must never be written to disk.
_SECRET_KEYS = {"api_key", "password", "token", "secret", "access_key", "secret_key"}


def _empty_bucket() -> dict[str, Any]:
    return {
        "input": 0,
        "output": 0,
        "cache_read": 0,
        "cache_creation": 0,
        "requests": 0,
        "costUSD": 0.0,
    }


def _redact(value: Any) -> Any:
    """Recursively replace secret-keyed values with ``***``."""
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if isinstance(k, str) and k.lower() in _SECRET_KEYS and v not in (None, ""):
                out[k] = "***"
            else:
                out[k] = _redact(v)
        return out
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


class DacliStore:
    """Load/update/persist ``.dacli/dacli.json``."""

    def __init__(self, base_dir: str = ".dacli", install_method: str = "source"):
        self.base_dir = Path(base_dir)
        self.path = self.base_dir / "dacli.json"
        self._install_method = install_method
        self._data: dict[str, Any] = self._default()
        self.load()

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------
    def _default(self) -> dict[str, Any]:
        return {
            "version": 1,
            "numStartups": 0,
            "installMethod": self._install_method,
            "firstStartTime": None,
            "lastStartTime": None,
            "config": {},
            # Real credentials live here (source of truth the config loader reads).
            # The `config` block above is a redacted, human-readable snapshot.
            "secrets": {},
            "usage": {"totals": _empty_bucket(), "byModel": {}, "sessions": {}},
        }

    def load(self) -> DacliStore:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                self._data = self._merge_defaults(data)
        except Exception:
            # Missing/corrupt file -> keep defaults, but leave a breadcrumb: a
            # silently dropped store is exactly the "sometimes it doesn't
            # remember" failure that is otherwise impossible to debug.
            log.debug("store load failed (%s); using defaults", self.path, exc_info=True)
        return self

    def _merge_defaults(self, data: dict[str, Any]) -> dict[str, Any]:
        base = self._default()
        base.update(data)
        base["secrets"] = data.get("secrets") or {}
        usage = data.get("usage") or {}
        base["usage"] = {
            "totals": {**_empty_bucket(), **(usage.get("totals") or {})},
            "byModel": usage.get("byModel") or {},
            "sessions": usage.get("sessions") or {},
        }
        return base

    def save(self) -> None:
        try:
            write_json_atomic(self.path, self._data, indent=2, default=str)
        except Exception:
            # Best-effort; never crash the session over telemetry — but record
            # it so a lost usage/secret write isn't completely invisible.
            log.debug("store save failed (%s)", self.path, exc_info=True)

    def is_first_run(self) -> bool:
        """True before the first :meth:`record_startup` ever lands — the signal
        the chat loop uses to offer onboarding once."""
        return not self._data.get("firstStartTime")

    # ------------------------------------------------------------------
    # mutations
    # ------------------------------------------------------------------
    def record_startup(self) -> None:
        now = _now_iso()
        self._data["numStartups"] = int(self._data.get("numStartups", 0)) + 1
        if not self._data.get("firstStartTime"):
            self._data["firstStartTime"] = now
        self._data["lastStartTime"] = now

    def snapshot_config(self, settings: Any) -> None:
        """Store a secret-redacted snapshot of the effective settings."""
        try:
            dump = settings.model_dump(mode="json")
        except Exception:
            dump = {}
        red = _redact(dump)
        sf = red.get("snowflake") or {}
        gh = red.get("github") or {}
        pc = red.get("pinecone") or {}
        emb = red.get("embeddings") or {}
        self._data["config"] = {
            "llm": red.get("llm") or {},
            "agent": red.get("agent") or {},
            "ui": red.get("ui") or {},
            "connectors": {
                "snowflake": {
                    k: sf.get(k)
                    for k in (
                        "account",
                        "user",
                        "warehouse",
                        "role",
                        "database",
                        "db_schema",
                    )
                },
                "github": {
                    k: gh.get(k) for k in ("owner", "repo", "branch", "repository_url")
                },
                "pinecone": {
                    k: pc.get(k) for k in ("index_name", "environment", "top_k")
                },
                "embeddings": {k: emb.get(k) for k in ("provider", "model")},
            },
        }

    def set_secret(self, section: str, field: str, value: str) -> None:
        """Store a real credential, Fernet-encrypted.

        E.g. ``set_secret('snowflake', 'password', 'secret123')``.
        The value is encrypted before being written to the ``secrets`` block
        so plaintext credentials never touch ``.dacli/dacli.json``.
        """
        from dacli.core.crypto import encrypt_value, is_encrypted

        if value and not is_encrypted(value):
            value = encrypt_value(value, base_dir=str(self.base_dir))
        self._data.setdefault("secrets", {}).setdefault(section, {})[field] = value

    def get_secrets(self) -> dict[str, Any]:
        """Return decrypted secrets.

        Plaintext values (from pre-encryption stores) are transparently
        re-encrypted in place on first read so migration is invisible.
        """
        from dacli.core.crypto import (
            CredentialDecryptionError,
            decrypt_value,
            encrypt_value,
            is_encrypted,
            surface_decryption_failures,
        )

        raw = self._data.get("secrets", {})
        decrypted: dict[str, Any] = {}
        migrated = False
        undecryptable: list = []
        for section, fields in raw.items():
            if not isinstance(fields, dict):
                continue
            decrypted[section] = {}
            for field, val in fields.items():
                if isinstance(val, str) and val and not is_encrypted(val):
                    self._data.setdefault("secrets", {}).setdefault(section, {})[
                        field
                    ] = encrypt_value(val, base_dir=str(self.base_dir))
                    migrated = True
                if not isinstance(val, str):
                    decrypted[section][field] = val
                    continue
                try:
                    decrypted[section][field] = decrypt_value(
                        val, base_dir=str(self.base_dir), name=f"{section}.{field}"
                    )
                except CredentialDecryptionError:
                    # Wrong/rotated key: omit the field (so downstream sees it as
                    # unconfigured) and aggregate for a single startup warning
                    # rather than handing the ciphertext to a connector.
                    undecryptable.append(f"{section}.{field}")
        if undecryptable:
            surface_decryption_failures(undecryptable)
        if migrated:
            try:
                self.save()
            except Exception:
                log.debug("secret re-encryption save failed", exc_info=True)
        return decrypted

    def _accumulate(
        self, bucket: dict[str, Any], usage: TokenUsage, cost: float
    ) -> None:
        bucket["input"] = bucket.get("input", 0) + usage.input
        bucket["output"] = bucket.get("output", 0) + usage.output
        bucket["cache_read"] = bucket.get("cache_read", 0) + usage.cache_read
        bucket["cache_creation"] = (
            bucket.get("cache_creation", 0) + usage.cache_creation
        )
        bucket["requests"] = bucket.get("requests", 0) + 1
        bucket["costUSD"] = round(bucket.get("costUSD", 0.0) + (cost or 0.0), 6)

    def record_usage(
        self,
        session_id: str,
        model: str,
        usage: TokenUsage,
        cost: float,
        first_prompt: str | None = None,
    ) -> None:
        """Fold one turn's usage into totals, by-model, and per-session buckets."""
        now = _now_iso()
        u = self._data["usage"]
        self._accumulate(u["totals"], usage, cost)
        self._accumulate(u["byModel"].setdefault(model, _empty_bucket()), usage, cost)

        sess = u["sessions"].get(session_id)
        if sess is None:
            sess = _empty_bucket()
            sess["startedAt"] = now
            if first_prompt:
                sess["firstPrompt"] = first_prompt[:200]
            u["sessions"][session_id] = sess
        sess["model"] = model
        sess["updatedAt"] = now
        self._accumulate(sess, usage, cost)

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    def session_cost_usd(self, session_id: str) -> float:
        """One session's running cost — O(1), no bucket copies.

        The bottom toolbar reads this on every keystroke (complete_while_typing),
        so it can't go through usage_summary, which copies every model/session
        bucket. The total is already accumulated per turn by record_usage.
        """
        sess = self._data["usage"]["sessions"].get(session_id)
        return float(sess.get("costUSD", 0.0)) if sess else 0.0

    def record_warehouse_cost(self, session_id: str, usd: float) -> None:
        """Fold an observed/estimated warehouse cost into the session bucket.

        Separate from the LLM ``costUSD`` so the toolbar can show both. The
        session bucket is created lazily, mirroring record_usage.
        """
        if not usd:
            return
        sess = self._data["usage"]["sessions"].setdefault(session_id, _empty_bucket())
        sess["warehouseUSD"] = round(sess.get("warehouseUSD", 0.0) + float(usd), 6)

    def session_warehouse_usd(self, session_id: str) -> float:
        """This session's accumulated warehouse cost — O(1), toolbar-safe."""
        sess = self._data["usage"]["sessions"].get(session_id)
        return float(sess.get("warehouseUSD", 0.0)) if sess else 0.0

    def usage_summary(self, session_id: str | None = None) -> dict[str, Any]:
        u = self._data["usage"]
        return {
            "numStartups": self._data.get("numStartups", 0),
            "totals": dict(u["totals"]),
            "byModel": {m: dict(b) for m, b in u["byModel"].items()},
            "session": dict(u["sessions"].get(session_id, {})) if session_id else None,
        }

    @property
    def data(self) -> dict[str, Any]:
        return self._data
