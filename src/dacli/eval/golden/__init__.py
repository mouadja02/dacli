"""Golden task suites.

A versioned set of tasks, each with a **machine-verifiable outcome** anchored to
the environment-as-oracle. Two families:

* :mod:`eval.golden.connectors` — per-connector tasks. The Wave-1 CLI connectors
  run *concretely* against the simulator (a real op + its real, environment-
  anchored post-conditions); every connector additionally gets a structural
  golden task that verifies its Definition-of-Done wiring (anchored post-condition
  + rollback parity + introspection) — a machine-checkable outcome, not a vibe.
* :mod:`eval.golden.spine` — the core spine behaviors (the destructive-
  action gate, post-condition catch, routing accuracy, self-correction).

The suite itself is treated as versioned code, reviewed each wave and expanded
with adversarial/destructive-edge tasks (golden suites going stale is a known risk).
"""

from dacli.eval.types import GoldenTask
from dacli.eval.golden.connectors import build_connector_suite
from dacli.eval.golden.spine import build_spine_suite
from dacli.eval.golden.terminal import build_terminal_suite


def build_golden_suite() -> list[GoldenTask]:
    """The full sim suite CI runs on each PR: connectors + spine + shell tier."""
    return build_connector_suite() + build_spine_suite() + build_terminal_suite()


__all__ = [
    "build_connector_suite",
    "build_golden_suite",
    "build_spine_suite",
    "build_terminal_suite",
]
