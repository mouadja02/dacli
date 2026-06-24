"""M08 — the snowflake, github, shell seeds reach their M01 golden turns.

Each seed is a bundled ``register(api)`` extension (no manifest). Loaded through
the normal discovery path and dispatched through the real governed Dispatcher, it
must produce the same result data + metadata the connector did at M01, governed
identically. The SDK seams are faked the same way ``test_m01_characterization``
fakes them (a cursor, an httpx response, a sim terminal session).
"""

import asyncio
import base64
import json
import shutil
import tempfile
from pathlib import Path

import dacli.eval.golden as _golden
from dacli.connectors.dispatcher import Dispatcher
from dacli.connectors.base import ToolStatus
from dacli.core import paths, runtime
from dacli.core.extensions import ExtensionDispatchRegistry, load_extensions
from dacli.governance.audit import AuditLedger
from dacli.governance.governor import Governor
from dacli.governance.permissions import PermissionRegistry, Scope

FIXTURES = Path(_golden.__file__).resolve().parent / "transcripts"
SEEDS = paths.bundled_seeds_dir("extensions")

# Same volatile keys test_m01_characterization scrubs before comparing.
_VOLATILE = {
    "execution_time_ms", "timestamp", "scrollback_handle", "command_id",
    "backups", "duration_ms", "pid", "started_at", "finished_at",
    "session_id", "cwd", "session", "backend",
}


def _scrub(obj, tmp=None):
    if isinstance(obj, dict):
        return {k: _scrub(v, tmp) for k, v in obj.items() if k not in _VOLATILE}
    if isinstance(obj, list):
        return [_scrub(v, tmp) for v in obj]
    if isinstance(obj, str) and tmp:
        return obj.replace(str(tmp), "<WORKSPACE>")
    return obj


def _golden_result(case):
    blob = json.loads((FIXTURES / f"{case}.json").read_text(encoding="utf-8"))
    return blob["results"][0]


def _dispatcher(extension_id):
    """The governed dispatcher M04 wires, with approval granted — no verifier, so
    the result shape matches the M01 fixtures (which carry no verification block)."""
    reg = ExtensionDispatchRegistry(load_extensions(SEEDS))
    perms = PermissionRegistry(default_scope=Scope.READ_ONLY)
    perms.grant(extension_id, Scope.ADMIN)
    gov = Governor(
        permissions=perms,
        ledger=AuditLedger(path=str(Path(tempfile.mkdtemp(prefix="dacli_m08_")) / "a.jsonl")),
        session_id="m08", approval_fn=lambda req: True, use_shadow=False,
    )
    return Dispatcher(reg, memory=None, governor=gov)


def _dispatch(extension_id, tool, args):
    return asyncio.run(_dispatcher(extension_id).execute(tool, args))


# ---------------------------------------------------------------------------
# faked SDK seams (mirroring test_m01_characterization)
# ---------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, columns, rows):
        self._rows = rows
        self.description = [(c,) for c in columns]
        self.rowcount = len(rows)

    def execute(self, sql):
        self._last = sql

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0]


class FakeSnowflakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpx:
    def __init__(self, resp):
        self._resp = resp

    async def get(self, *a, **k):
        return self._resp


# ---------------------------------------------------------------------------
# parity
# ---------------------------------------------------------------------------
def test_snowflake_read_matches_m01(monkeypatch):
    import snowflake.connector

    cur = FakeCursor(["ID", "NAME"], [(1, "ada"), (2, "linus")])
    monkeypatch.setattr(snowflake.connector, "connect",
                        lambda **kw: FakeSnowflakeConn(cur), raising=False)

    res = _dispatch("snowflake", "execute_snowflake_query",
                    {"query": "SELECT ID, NAME FROM analytics.users"})
    golden = _golden_result("snowflake_read")

    assert res.status is ToolStatus.SUCCESS
    assert _scrub(res.data) == golden["data"]
    assert _scrub(dict(res.metadata)) == golden["metadata"]


def test_github_read_matches_m01(monkeypatch):
    import httpx

    payload = {
        "path": "dbt_project.yml",
        "content": base64.b64encode(b"name: warehouse\nversion: 1.0\n").decode(),
        "sha": "feedface", "size": 31,
    }
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: FakeHttpx(_Resp(200, payload)))

    res = _dispatch("github", "read_github_file", {"path": "dbt_project.yml"})
    golden = _golden_result("github_read")

    assert res.status is ToolStatus.SUCCESS
    assert _scrub(res.data) == golden["data"]
    assert _scrub(dict(res.metadata)) == golden["metadata"]


def test_shell_command_matches_m01():
    from dacli.context.sources.terminal import ScrollbackStore
    from dacli.eval.sim.shell import make_sim_session
    from types import SimpleNamespace

    tmp = tempfile.mkdtemp(prefix="dacli_m08_shell_")
    try:
        session, _sim = make_sim_session("m08sh", tmp)
        (session.workspace.root / "hello.txt").write_text("hi there\n", encoding="utf-8")
        store = ScrollbackStore(root=tmp, session_id="m08sh")
        runtime.set_terminal(session, store, SimpleNamespace(
            network="allowlist", egress_allowlist=[], max_output_chars=2000))

        res = _dispatch("shell", "run_shell_command", {"command": "cat hello.txt"})
        golden = _golden_result("shell_command")

        assert res.status is ToolStatus.SUCCESS
        assert _scrub(res.data, tmp) == golden["data"]
        assert _scrub(dict(res.metadata), tmp) == golden["metadata"]
    finally:
        runtime.clear_terminal()
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# discovery: exactly the three seeds, no manifests
# ---------------------------------------------------------------------------
def test_bundled_seeds_expose_exactly_three():
    reg = load_extensions(SEEDS)
    assert sorted(reg.extension_ids()) == ["github", "shell", "snowflake"]
    assert reg.failed_extensions() == {}


def test_empty_overlay_falls_through_to_seeds(monkeypatch, tmp_path):
    # No project overlay, empty global dir -> resource_dir resolves the bundled seeds.
    monkeypatch.setattr(paths, "project_root", lambda *a, **k: None)
    monkeypatch.setattr(paths, "user_config_dir", lambda: tmp_path / "empty")
    assert paths.resource_dir("extensions") == SEEDS
    assert sorted(load_extensions().extension_ids()) == ["github", "shell", "snowflake"]


def test_seeds_carry_no_manifest():
    assert list(SEEDS.glob("*/manifest.yaml")) == []
