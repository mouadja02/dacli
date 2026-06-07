"""P12 Part 1.3 — connector SDK imports are lazy (inside connect()).

A connector for an optional extra (snowflake, pinecone) must import, construct,
and appear in the registry even when its heavy SDK is not installed. Only an
actual ``connect()`` attempt asks for the SDK, and a missing one degrades to an
actionable "install dacli[<name>]" hint — never an import crash at cold start.
"""

import asyncio
import subprocess
import sys
import unittest
from unittest import mock


def _import_with_sdk_blocked(*blocked: str) -> subprocess.CompletedProcess:
    """Import the connector modules in a fresh interpreter with SDKs blocked.

    Setting a name to ``None`` in ``sys.modules`` makes ``import <name>`` raise
    ImportError — a faithful stand-in for "the extra isn't installed". Doing it
    in a subprocess keeps the block fully isolated from the test process.
    """
    blockers = "; ".join(f"sys.modules[{name!r}] = None" for name in blocked)
    code = (
        "import sys; " + blockers + "; "
        "import dacli.connectors.snowflake.connector as s; "
        "import dacli.connectors.pinecone.connector as p; "
        "assert hasattr(s, 'SnowflakeConnector'); "
        "assert hasattr(p, 'PineconeConnector'); "
        "print('ok')"
    )
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )


class ColdStartUnaffectedTest(unittest.TestCase):
    def test_connector_modules_import_without_their_sdks(self):
        proc = _import_with_sdk_blocked(
            "snowflake", "snowflake.connector", "pinecone"
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("ok", proc.stdout)


class ConnectDegradesCleanlyTest(unittest.TestCase):
    def test_snowflake_connect_without_sdk_gives_install_hint(self):
        import dacli.connectors.snowflake.connector as mod

        conn = mod.SnowflakeConnector(settings=mock.MagicMock())
        with mock.patch.dict(sys.modules, {"snowflake": None, "snowflake.connector": None}), \
                self.assertRaises(ConnectionError) as ctx:
            asyncio.run(conn.connect())
        self.assertIn("dacli[snowflake]", str(ctx.exception))

    def test_pinecone_connect_without_sdk_gives_install_hint(self):
        import dacli.connectors.pinecone.connector as mod

        conn = mod.PineconeConnector(settings=mock.MagicMock())
        with mock.patch.dict(sys.modules, {"pinecone": None}), \
                self.assertRaises(ConnectionError) as ctx:
            asyncio.run(conn.connect())
        self.assertIn("dacli[pinecone]", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
