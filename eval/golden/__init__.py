"""Golden task suites (workstream 8.1).

A versioned set of tasks, each with a **machine-verifiable outcome** anchored to
the environment-as-oracle. Two families:

* :mod:`eval.golden.connectors` — per-connector tasks. The Wave-1 CLI connectors
  run *concretely* against the simulator (a real op + its real, environment-
  anchored post-conditions); every connector additionally gets a structural
  golden task that verifies its Definition-of-Done wiring (anchored post-condition
  + rollback parity + introspection) — a machine-checkable outcome, not a vibe.
* :mod:`eval.golden.spine` — the spine behaviors from Phases 1–6 (the destructive-
  action gate, post-condition catch, routing accuracy, self-correction).

The suite itself is treated as versioned code, reviewed each wave and expanded
with adversarial/destructive-edge tasks (PHASE8 §6 risk: golden suites go stale).
"""

from typing import List

from eval.types import GoldenTask
from eval.golden.connectors import build_connector_suite
from eval.golden.spine import build_spine_suite


def build_golden_suite() -> List[GoldenTask]:
    """The full sim suite CI runs on each PR: connectors + spine behaviors."""
    return build_connector_suite() + build_spine_suite()


__all__ = ["build_golden_suite", "build_connector_suite", "build_spine_suite"]
