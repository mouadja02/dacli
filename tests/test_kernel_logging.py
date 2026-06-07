"""P06: the kernel catch-all logs a traceback, and --debug re-raises.

The whole-loop ``except`` in the kernel used to flatten real bugs (KeyError,
AttributeError) into a one-line ``error=str(e)`` with no traceback. These tests
pin the new behavior: normal mode swallows-and-logs (with traceback); debug mode
re-raises unexpected exceptions so they aren't masked. Pure unittest (no pytest).
"""

import asyncio
import logging
import os
import tempfile
import unittest

from dacli.core.kernel import Kernel
from dacli.core.logging_setup import setup_logging


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


class KernelLoggingTest(unittest.TestCase):
    def test_normal_mode_swallows_into_error_and_logs_traceback(self):
        d = tempfile.mkdtemp(prefix="dacli_kernlog_")
        setup_logging(debug=False, base_dir=d, force=True)
        kernel = _kernel(debug=False)
        resp = asyncio.run(kernel.orchestrate("hi"))
        self.assertIsNotNone(resp.error)
        self.assertIn("boom-unexpected", resp.error)
        for h in logging.getLogger("dacli").handlers:
            h.flush()
        with open(os.path.join(d, "dacli.log"), encoding="utf-8") as f:
            content = f.read()
        # The full traceback is recorded before flattening to resp.error.
        self.assertIn("kernel loop failed", content)
        self.assertIn("Traceback", content)

    def test_debug_mode_reraises_unexpected_exception(self):
        d = tempfile.mkdtemp(prefix="dacli_kernlog_")
        setup_logging(debug=True, base_dir=d, force=True)
        kernel = _kernel(debug=True)
        with self.assertRaises(KeyError):
            asyncio.run(kernel.orchestrate("hi"))


if __name__ == "__main__":
    unittest.main()
