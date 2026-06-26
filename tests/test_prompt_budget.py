"""M14 — keep the core prompt + always-on tool defs within a token budget.

The pivot target is a Pi-style minimal core (reporting/01). The core prompt is the
surface we control; the six system tool defs are load-bearing (the model can't use
``load_connector_tools`` / ``fetch_result`` / ``update_plan`` without them), so we
budget the two together and pin the core so it can't quietly regrow.
"""

import json

from dacli.context.tokenizer import make_counter
from dacli.connectors.system.connector import SystemConnector
from dacli.prompts.system_prompt import CORE_FRAGMENT

# The packaged core prompt, on its own, must stay lean. It was 1144 tokens before
# M14; this caps the regrowth with headroom.
CORE_BUDGET = 700
# Core prompt + the always-on system tool defs. Pi's whole surface is ~1k; ours
# carries six system tools (~990 tokens of schema) so the combined ceiling is higher.
COMBINED_BUDGET = 2000


def _system_tool_defs() -> list[dict]:
    return [op.to_tool_definition() for op in SystemConnector().operations()]


def test_core_prompt_under_budget():
    counter = make_counter()
    core_tokens = counter.count(CORE_FRAGMENT.read_text(encoding="utf-8"))
    assert core_tokens <= CORE_BUDGET, f"core.md is {core_tokens} tokens (budget {CORE_BUDGET})"


def test_core_plus_tool_defs_under_budget():
    counter = make_counter()
    core_tokens = counter.count(CORE_FRAGMENT.read_text(encoding="utf-8"))
    tool_tokens = counter.count(json.dumps(_system_tool_defs()))
    total = core_tokens + tool_tokens
    assert total <= COMBINED_BUDGET, (
        f"core ({core_tokens}) + tool defs ({tool_tokens}) = {total} "
        f"tokens (budget {COMBINED_BUDGET})"
    )
