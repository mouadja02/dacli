"""Shadow / clone-first execution (𝒢) — validate on real-shaped data.

A flagship reliability feature: a ``risky``/``irreversible`` *transform* is run
first against a **zero-copy clone** of its target (cheap on Snowflake /
Databricks / BigQuery), the result is **diffed** against the original (row
counts, checksums, sample), and the change is **promoted to the real object only
on approval**. The original is never touched until a human has seen the diff.

This module is connector-agnostic: it drives any connector that implements the
small *shadow protocol*::

    supports_shadow: bool                      # advertises the capability
    async def create_clone(args) -> clone_ref          # zero-copy clone
    async def run_on_clone(clone_ref, args) -> Any      # transform on the clone
    async def diff_clone(clone_ref, args) -> dict        # {row_delta, checksums, ...}
    async def promote_clone(clone_ref, args) -> Any      # apply to the real object
    async def drop_clone(clone_ref) -> None              # cleanup

Connectors that lack shadow support simply don't get clone-first execution; the
governor falls back to confirm-with-rollback-plan.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)


def supports_shadow(connector: Any) -> bool:
    return bool(getattr(connector, "supports_shadow", False)) and all(
        callable(getattr(connector, m, None))
        for m in ("create_clone", "run_on_clone", "diff_clone", "promote_clone")
    )


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@dataclass
class ShadowResult:
    """Outcome of a shadow run, presented to the human before promotion."""

    ran: bool
    clone_ref: str | None = None
    diff: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    promoted: bool = False

    def summary(self) -> str:
        if not self.ran:
            return f"shadow run did not execute: {self.error or 'unknown'}"
        bits = []
        if "row_delta" in self.diff:
            bits.append(f"row Δ={self.diff['row_delta']}")
        if "rows_before" in self.diff and "rows_after" in self.diff:
            bits.append(f"{self.diff['rows_before']} → {self.diff['rows_after']} rows")
        if "checksum_changed" in self.diff:
            bits.append("checksum changed" if self.diff["checksum_changed"] else "checksum unchanged")
        return "shadow diff: " + (", ".join(bits) if bits else "(no diff fields)")

    def to_dict(self) -> dict[str, Any]:
        return {
            "ran": self.ran,
            "clone_ref": self.clone_ref,
            "diff": self.diff,
            "error": self.error,
            "promoted": self.promoted,
        }


class ShadowExecutor:
    """Runs a transform on a clone and produces a diff for approval."""

    async def run(self, connector: Any, args: dict[str, Any]) -> ShadowResult:
        if not supports_shadow(connector):
            return ShadowResult(ran=False, error="connector does not support shadow execution")
        clone_ref = None
        try:
            clone_ref = await _maybe_await(connector.create_clone(args))
            await _maybe_await(connector.run_on_clone(clone_ref, args))
            diff = await _maybe_await(connector.diff_clone(clone_ref, args))
            return ShadowResult(ran=True, clone_ref=str(clone_ref), diff=dict(diff or {}))
        except Exception as e:
            return ShadowResult(ran=False, clone_ref=str(clone_ref) if clone_ref else None,
                                error=str(e))

    async def promote(self, connector: Any, args: dict[str, Any], shadow: ShadowResult) -> ShadowResult:
        """Apply the validated change to the real object, then drop the clone."""
        if not shadow.ran:
            return shadow
        try:
            await _maybe_await(connector.promote_clone(shadow.clone_ref, args))
            shadow.promoted = True
        except Exception as e:
            shadow.error = f"promotion failed: {e}"
        finally:
            drop = getattr(connector, "drop_clone", None)
            if callable(drop):
                try:
                    await _maybe_await(drop(shadow.clone_ref))
                except Exception:
                    log.debug("shadow clone drop failed after promotion", exc_info=True)
        return shadow

    async def discard(self, connector: Any, shadow: ShadowResult) -> None:
        """Throw away the clone when the human declines to promote."""
        drop = getattr(connector, "drop_clone", None)
        if shadow.clone_ref and callable(drop):
            try:
                await _maybe_await(drop(shadow.clone_ref))
            except Exception:
                log.debug("shadow clone discard failed", exc_info=True)
