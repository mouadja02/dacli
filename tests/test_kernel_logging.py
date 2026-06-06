"""P06: the kernel catch-all logs a traceback, and --debug re-raises.

The whole-loop ``except`` in the kernel used to flatten real bugs (KeyError,
AttributeError) into a one-line ``error=str(e)`` with no traceback. These tests
pin the new behavior: normal mode swallows-and-logs (with traceback); debug mode
re-raises unexpected exceptions so they aren't masked.
"""

import asyncio
import logging

import pytest

from core.kernel import Kernel
from core.logging_setup import setup_logging


class BoomLLM:
    """An LLM whose generate() raises an unexpected bug."""

    async def generate(self, **kwargs):
        raise KeyError("boom-unexpected")


class MiniMemory:
    def __init__(self):
        self.user = []
        self.final = []

    def add_user_message(self, content):
        self.user.append(content)

    def get_context_messages(self):
        return [{"role": "user", "content": "GO"}]

    def add_assistant_message(self, content):
        self.final.append(content)


def _kernel(debug):
    return Kernel(
        llm=BoomLLM(),
        dispatcher=object(),
        memory=MiniMemory(),
        tools=[],
        system_prompt="SYS",
        max_iterations=3,
        debug=debug,
    )


def test_normal_mode_swallows_into_error_and_logs_traceback(tmp_path, caplog):
    setup_logging(debug=False, base_dir=str(tmp_path), force=True)
    kernel = _kernel(debug=False)
    with caplog.at_level(logging.ERROR, logger="dacli"):
        resp = asyncio.run(kernel.orchestrate("hi"))
    assert resp.error is not None
    assert "boom-unexpected" in resp.error
    # A traceback was logged (exc_info present on at least one record).
    assert any(r.exc_info for r in caplog.records), "expected a logged traceback"


def test_debug_mode_reraises_unexpected_exception(tmp_path):
    setup_logging(debug=True, base_dir=str(tmp_path), force=True)
    kernel = _kernel(debug=True)
    with pytest.raises(KeyError):
        asyncio.run(kernel.orchestrate("hi"))
