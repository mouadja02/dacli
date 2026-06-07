"""Connector *Definition of Done* — the mechanical merge gate.

The thesis is that pouring connectors onto the spine is only safe because
capability (𝒮) and its governance counterpart (𝒢) ship **together**, always.
This module turns that promise into a *mechanical* check instead of a vibe:
``tests/test_connector_dod.py`` runs :func:`audit_connectors` over every
discovered connector and fails CI if any DoD rule is unmet. This is the structural
cure for *governance debt* — "adding skills/tools without corresponding
governance creates compounding risk" (harness-scaling, gotcha #4).

The DoD checklist (README §5 / the roadmap) distilled into rules:

1. **manifest** — id, name, description, class, required_config, enabled, and a
   declared **permission scope** (``default_scope``, read-only by default).
2. **operations** — at least one, each with a JSON-schema ``parameters`` object.
3. **post-conditions** — every op has ≥1; every *mutating* op (write/risky/
   irreversible) has ≥1 **anchored** check that is more than bare
   ``result_succeeded`` (anchor to the environment, not the model).
4. **introspection** — a read-only op that can re-verify live state (and ideally
   feed the Phase-2 catalog), so memory stays trustworthy.
5. **rollback** — any connector with a mutating op has a registered native
   rollback planner; any connector exposing an *irreversible* op also implements
   ``verify_rollback`` (so the path can be *proven* to exist, not assumed).
6. **SKILL.md** — progressive-disclosure doc next to the connector.
7. **golden task** — a verifiable outcome declared in the manifest, referencing a
   real operation.

The checker is pure/dependency-light so it runs identically in CI and locally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dacli.connectors.base import Connector, Risk
from dacli.governance.permissions import _coerce_scope
from dacli.governance.rollback import _PLATFORM_PLANNERS

# Risk tiers whose ops mutate state and therefore need an anchored post-condition
# and a rollback story.
_MUTATING = {Risk.WRITE, Risk.RISKY, Risk.IRREVERSIBLE}

_REQUIRED_MANIFEST_FIELDS = ("id", "name", "description", "class", "enabled")


@dataclass
class DodViolation:
    connector_id: str
    rule: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.connector_id}] {self.rule}: {self.detail}"


@dataclass
class DodReport:
    """Aggregate DoD result across connectors."""

    violations: list[DodViolation] = field(default_factory=list)
    checked: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.violations

    def summary(self) -> str:
        if self.passed:
            return f"DoD PASSED for {len(self.checked)} connector(s): {', '.join(self.checked)}"
        lines = [f"DoD FAILED — {len(self.violations)} violation(s):"]
        lines += [f"  - {v}" for v in self.violations]
        return "\n".join(lines)


def _has_introspection(connector: Connector) -> bool:
    for spec in connector.operations():
        cat = (spec.category or "").lower()
        cap = (spec.capability or "").lower()
        if spec.risk == Risk.SAFE and (cat in ("introspection", "read") or cap.endswith(".introspection")):
            return True
    return False


def _anchored_beyond_success(spec: Any) -> bool:
    """True if the op declares an anchored post-condition that isn't just result_succeeded."""
    for pc in getattr(spec, "postconditions", None) or []:
        name = getattr(pc, "name", "")
        anchored = getattr(pc, "anchored", False)
        if anchored and name != "result_succeeded":
            return True
    return False


def check_connector_dod(
    connector_id: str,
    manifest: dict[str, Any],
    connector: Connector,
    connector_dir: Path | None = None,
) -> list[DodViolation]:
    """Run every DoD rule for one connector; return the list of violations."""
    v: list[DodViolation] = []

    def fail(rule: str, detail: str) -> None:
        v.append(DodViolation(connector_id, rule, detail))

    # --- 1. manifest fields + permission scope -------------------------------
    for key in _REQUIRED_MANIFEST_FIELDS:
        if key not in manifest or manifest.get(key) in (None, ""):
            fail("manifest", f"missing required field '{key}'")
    if not isinstance(manifest.get("required_config", []), list):
        fail("manifest", "'required_config' must be a list")
    scope_raw = manifest.get("default_scope")
    if scope_raw is None:
        fail("permission_scope", "manifest must declare 'default_scope' (read_only by default)")
    elif _coerce_scope(scope_raw) is None:
        fail("permission_scope", f"invalid default_scope '{scope_raw}' "
                                 f"(use read_only | write | risky | admin)")

    # --- 2. operations + schemas --------------------------------------------
    ops = connector.operations()
    if not ops:
        fail("operations", "connector exposes no operations")
    for spec in ops:
        params = spec.parameters or {}
        if not isinstance(params, dict) or params.get("type") != "object":
            fail("schema", f"op '{spec.name}' parameters must be a JSON-schema object")

    # --- 3. post-conditions (mandatory; anchored for mutating ops) ----------
    for spec in ops:
        pcs = getattr(spec, "postconditions", None) or []
        if not pcs:
            fail("postconditions", f"op '{spec.name}' declares no post-condition")
            continue
        if spec.risk in _MUTATING and not _anchored_beyond_success(spec):
            fail("postconditions",
                 f"mutating op '{spec.name}' ({spec.risk.value}) needs an anchored "
                 f"post-condition beyond result_succeeded (verify the effect)")

    # --- 4. introspection ----------------------------------------------------
    if not _has_introspection(connector):
        fail("introspection", "no read-only introspection op to re-verify live state")

    # --- 5. rollback parity --------------------------------------------------
    risks = {spec.risk for spec in ops}
    has_mutating = bool(risks & _MUTATING)
    has_irreversible = Risk.IRREVERSIBLE in risks
    if has_mutating and connector_id not in _PLATFORM_PLANNERS:
        fail("rollback", "mutating ops but no native rollback planner registered "
                         "in governance.rollback._PLATFORM_PLANNERS")
    if has_irreversible and not callable(getattr(connector, "verify_rollback", None)):
        fail("rollback", "exposes an irreversible op but implements no "
                         "verify_rollback hook (the path cannot be proven to exist)")

    # --- 6. SKILL.md ---------------------------------------------------------
    if connector_dir is not None and not (connector_dir / "SKILL.md").exists():
        fail("skill_md", "missing SKILL.md (progressive-disclosure doc)")

    # --- 7. golden task ------------------------------------------------------
    golden = manifest.get("golden_task")
    if not isinstance(golden, dict):
        fail("golden_task", "manifest must declare a 'golden_task' block")
    else:
        for key in ("name", "op", "description"):
            if not golden.get(key):
                fail("golden_task", f"golden_task missing '{key}'")
        op_name = golden.get("op")
        if op_name and op_name not in {s.name for s in ops}:
            fail("golden_task", f"golden_task op '{op_name}' is not a real operation")

    return v


def audit_connectors(
    connectors_root: str | None = None,
    settings: Any = None,
) -> DodReport:
    """Discover every connector via its manifest and run the DoD gate on each.

    Mirrors :class:`~connectors.registry.ConnectorRegistry` discovery so the gate
    sees exactly what the runtime would load. ``settings`` may be a stub — the
    DoD only calls ``operations()`` (which must not require live credentials).
    """
    import importlib

    import yaml

    root = Path(connectors_root) if connectors_root else Path(__file__).parent
    report = DodReport()

    for manifest_path in sorted(root.glob("*/manifest.yaml")):
        with open(manifest_path, encoding="utf-8") as f:
            manifest = yaml.safe_load(f) or {}
        connector_id = manifest.get("id")
        class_path = manifest.get("class")
        if not connector_id or not class_path:
            report.violations.append(
                DodViolation(connector_id or str(manifest_path), "manifest",
                             "manifest lacks 'id' or 'class'"))
            continue
        module_path, _, class_name = class_path.rpartition(".")
        cls = getattr(importlib.import_module(module_path), class_name)
        connector = cls(settings)
        report.checked.append(connector_id)
        report.violations.extend(
            check_connector_dod(connector_id, manifest, connector, manifest_path.parent)
        )

    return report
