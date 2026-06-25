"""Per-connector golden tasks.

Every discovered connector gets a **structural** golden task that runs its
Definition-of-Done checks — a machine-verifiable guarantee that its 𝒮/𝒢 wiring
(anchored post-condition, rollback parity, introspection) exists, even for
platforms we can't fully simulate offline.

The sim-backed concrete tasks (s3/gcs/bigquery/databricks put/delete/select against
the CLI simulator) went with the connectors they targeted (M11). The destructive
gate and post-condition catch they demonstrated now live, platform-free, in
:mod:`eval.golden.spine`.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import yaml

from dacli.connectors.dod import check_connector_dod
from dacli.eval.types import GoldenTask, Stakes, TaskResult


# Resolve via the installed package, not __file__ math: since M13 split the tree,
# dacli.connectors ships in the dacli-core wheel — no longer a sibling of this
# (assembler-side) eval module under one dacli/ dir.
import dacli.connectors as _connectors

_CONNECTORS_ROOT = Path(_connectors.__file__).resolve().parent


def _discover() -> list[tuple[str, dict[str, Any], Path]]:
    out: list[tuple[str, dict[str, Any], Path]] = []
    for manifest_path in sorted(_CONNECTORS_ROOT.glob("*/manifest.yaml")):
        with open(manifest_path, encoding="utf-8") as f:
            manifest = yaml.safe_load(f) or {}
        cid = manifest.get("id")
        class_path = manifest.get("class")
        if cid and class_path:
            out.append((cid, manifest, manifest_path.parent))
    return out


def _spec_task(cid: str, manifest: dict[str, Any], cdir: Path) -> GoldenTask:
    import types as _types

    async def run() -> TaskResult:
        class_path = manifest["class"]
        module_path, _, class_name = class_path.rpartition(".")
        cls = getattr(importlib.import_module(module_path), class_name)
        conn = cls(_types.SimpleNamespace())
        violations = check_connector_dod(cid, manifest, conn, cdir)
        ok = not violations
        return TaskResult(
            f"{cid}.dod", success=ok, steps_total=1,
            failed_step=None if ok else 1,
            detail=("DoD satisfied" if ok else "; ".join(str(v) for v in violations)),
        )

    return GoldenTask(
        id=f"{cid}.dod", connector=cid,
        description=f"{cid}: golden-task & governance wiring meets the Definition of Done",
        run=run, stakes=Stakes.READ_ONLY, tags=["dod", "structural"],
    )


def build_connector_suite() -> list[GoldenTask]:
    # Structural DoD task for every discovered connector.
    return [_spec_task(cid, manifest, cdir) for cid, manifest, cdir in _discover()]
