"""F-9 (P11) — CI gating scenarios replay offline to their contracted exits.

The composite action (.github/actions/dacli-gate) is packaging; what must not
rot are the example scenarios it points at and the exit-code contract they
demonstrate. Both replay hermetically (scripted LLM, built-ins only).
"""

import json
import unittest
from pathlib import Path
from unittest import mock

from click.testing import CliRunner

SCENARIOS = Path(__file__).parent.parent / "scenarios"


class CiScenarioSmokeTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch("dacli.core.host.fetch_pricing", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _replay(self, name: str):
        from dacli.scripts.cli import cli

        return CliRunner().invoke(
            cli, ["replay", str(SCENARIOS / name), "--json"]
        )

    def test_smoke_scenario_exits_0(self):
        res = self._replay("smoke_headless.json")
        self.assertEqual(res.exit_code, 0, msg=res.output)
        payload = json.loads(res.output)
        self.assertTrue(payload["ok"])

    def test_governance_gate_scenario_exits_2(self):
        res = self._replay("ci_governance_gate.json")
        self.assertEqual(res.exit_code, 2, msg=res.output)
        payload = json.loads(res.output)
        statuses = [
            tc.get("status") for t in payload["turns"] for tc in t["tool_calls"]
        ]
        self.assertTrue(any(s in ("denied", "blocked") for s in statuses))

    def test_gate_action_points_at_real_scenarios(self):
        # The composite action and its docs must reference files that exist.
        action = (
            Path(__file__).parent.parent
            / ".github" / "actions" / "dacli-gate" / "action.yml"
        )
        self.assertTrue(action.exists())
        body = action.read_text(encoding="utf-8")
        self.assertIn("dacli replay", body)
        for referenced in ("scenarios/smoke_headless.json",):
            self.assertTrue(
                (Path(__file__).parent.parent / referenced).exists(),
                msg=f"{referenced} referenced by the action is missing",
            )


if __name__ == "__main__":
    unittest.main()
