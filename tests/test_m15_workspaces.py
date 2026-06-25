"""M15 — workspaces.

A workspace is the ``~/.dacli/workspaces/<name>/`` overlay: its own extensions,
secrets, state, history and audit ledger. The global ``~/.dacli/`` is the default
workspace. Switching re-resolves extensions + secrets through the precedence chain
(M02) without a process restart, and never leaks one workspace's secrets or audit
into another.
"""

import asyncio
from pathlib import Path

import pytest

from dacli.core import paths


@pytest.fixture(autouse=True)
def _reset_active_workspace():
    # The active workspace is process-wide; keep tests independent.
    paths.set_active_workspace(None)
    yield
    paths.set_active_workspace(None)


# ---- paths: workspace resolution ------------------------------------------

def test_default_workspace_is_the_global_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    assert paths.active_workspace() is None
    assert paths.workspace_root() == tmp_path / "home"


def test_named_workspace_lives_under_workspaces_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    paths.set_active_workspace("alpha")
    assert paths.active_workspace() == "alpha"
    assert paths.workspace_root() == tmp_path / "home" / "workspaces" / "alpha"


def test_default_name_clears_the_active_workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    paths.set_active_workspace("alpha")
    paths.set_active_workspace("default")
    assert paths.active_workspace() is None


def test_invalid_workspace_name_is_refused():
    with pytest.raises(ValueError):
        paths.set_active_workspace("../escape")


def test_list_workspaces_returns_named_dirs(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    (tmp_path / "home" / "workspaces" / "alpha").mkdir(parents=True)
    (tmp_path / "home" / "workspaces" / "beta").mkdir(parents=True)
    assert paths.list_workspaces() == ["alpha", "beta"]


def test_resource_dir_resolves_to_the_active_workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)  # no project marker, so the workspace tier wins
    ws_ext = tmp_path / "home" / "workspaces" / "alpha" / "extensions"
    ws_ext.mkdir(parents=True)
    paths.set_active_workspace("alpha")
    assert paths.resource_dir("extensions") == ws_ext


def test_project_overlay_still_wins_over_the_active_workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    (tmp_path / ".git").mkdir()
    monkeypatch.chdir(tmp_path)
    proj_ext = tmp_path / ".dacli" / "extensions"
    proj_ext.mkdir(parents=True)
    (tmp_path / "home" / "workspaces" / "alpha" / "extensions").mkdir(parents=True)
    paths.set_active_workspace("alpha")
    assert paths.resource_dir("extensions") == proj_ext


def test_session_state_base_follows_the_workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    # No workspace: the configured state_path's parent, unchanged.
    assert paths.session_state_base(".dacli/state/") == Path(".dacli")
    paths.set_active_workspace("alpha")
    assert paths.session_state_base(".dacli/state/") == (
        tmp_path / "home" / "workspaces" / "alpha"
    )


# ---- host: per-workspace isolation ----------------------------------------

_EXT = """
def register(api):
    @api.tool(name="{tool}", description="d", risk="safe",
              postconditions=["result_succeeded"])
    async def {tool}(args, ctx):
        return ctx.ok("ok")
"""


def _write_workspace_ext(home: Path, ws: str, ext: str, tool: str) -> None:
    pkg = home / "workspaces" / ws / "extensions" / ext
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text(_EXT.format(tool=tool), encoding="utf-8")


def _settings(tmp: Path):
    from dacli.config.settings import Settings

    return Settings.model_validate({
        "llm": {"provider": "scripted", "model": "scripted",
                "api_key": "scripted", "base_url": "https://api.test.local"},
        "agent": {"state_path": str(tmp / "state") + "/",
                  "history_path": str(tmp / "history") + "/"},
        "terminal": {"enabled": False},
        "sandbox": {"enabled": False},
    })


def _host(tmp: Path, ws: str | None):
    from dacli.core.host import DacliHost
    from dacli.ai.scripted import ScriptedLLM

    paths.set_active_workspace(ws)
    return DacliHost(settings=_settings(tmp), llm=ScriptedLLM([]))


@pytest.fixture
def _workspaces(monkeypatch, tmp_path):
    home = tmp_path / "home"
    monkeypatch.setenv("DACLI_HOME", str(home))
    monkeypatch.chdir(tmp_path)  # no project marker so the workspace tier wins
    _write_workspace_ext(home, "alpha", "pinger", "ping_alpha")
    _write_workspace_ext(home, "beta", "ponger", "pong_beta")
    return home


def test_host_resolves_its_own_workspace_extensions(_workspaces, tmp_path):
    alpha = _host(tmp_path, "alpha")
    beta = _host(tmp_path, "beta")
    alpha_tools = {d["function"]["name"] for d in alpha._combined.get_tool_definitions()}
    beta_tools = {d["function"]["name"] for d in beta._combined.get_tool_definitions()}
    assert "ping_alpha" in alpha_tools and "pong_beta" not in alpha_tools
    assert "pong_beta" in beta_tools and "ping_alpha" not in beta_tools


def test_each_workspace_audit_holds_only_its_own_actions(_workspaces, tmp_path):
    home = _workspaces

    alpha = _host(tmp_path, "alpha")
    asyncio.run(alpha.dispatcher.execute("ping_alpha", {}))

    beta = _host(tmp_path, "beta")
    asyncio.run(beta.dispatcher.execute("pong_beta", {}))

    alpha_audit = (home / "workspaces" / "alpha" / "audit.jsonl").read_text()
    beta_audit = (home / "workspaces" / "beta" / "audit.jsonl").read_text()
    assert "ping_alpha" in alpha_audit and "pong_beta" not in alpha_audit
    assert "pong_beta" in beta_audit and "ping_alpha" not in beta_audit
    # No leak into the global default workspace's ledger.
    assert not (home / "audit.jsonl").exists()


def test_workspace_secrets_do_not_leak_across_workspaces(_workspaces, tmp_path):
    alpha = _host(tmp_path, "alpha")
    alpha.secrets.set("pinger", "token", "alpha-secret", secret=True)
    alpha.secrets.save()

    beta = _host(tmp_path, "beta")
    assert beta.secrets.config("pinger") == {}
    assert alpha.secrets.config("pinger") == {"token": "alpha-secret"}


# ---- core.workspaces: create / list / select ------------------------------

def test_create_makes_a_workspace_dir(monkeypatch, tmp_path):
    from dacli.core import workspaces

    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    path = workspaces.create("alpha")
    assert path == tmp_path / "home" / "workspaces" / "alpha"
    assert path.is_dir()
    assert workspaces.list_names() == ["alpha"]


def test_create_refuses_a_bad_name(monkeypatch, tmp_path):
    from dacli.core import workspaces

    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    with pytest.raises(ValueError):
        workspaces.create("../escape")


def test_select_sets_the_active_workspace(monkeypatch, tmp_path):
    from dacli.core import workspaces

    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    root = workspaces.select("alpha")
    assert workspaces.current() == "alpha"
    assert root == tmp_path / "home" / "workspaces" / "alpha"
    assert root.is_dir()  # selecting a new workspace creates its overlay
    workspaces.select("default")
    assert workspaces.current() is None


# ---- slash: /workspace ----------------------------------------------------

def _slash_ctx(make_host=None):
    from unittest.mock import MagicMock

    from dacli.config.settings import Settings
    from dacli.tui import slash

    return slash.ChatContext(
        ui=MagicMock(),
        console=MagicMock(),
        memory=MagicMock(),
        agent=MagicMock(),
        store=MagicMock(),
        settings=Settings(),
        config_path=None,
        make_host=make_host,
    )


def test_workspace_command_lists_workspaces(monkeypatch, tmp_path):
    from dacli.core import workspaces
    from dacli.tui import slash

    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    workspaces.create("alpha")
    ctx = _slash_ctx()
    asyncio.run(slash.dispatch(ctx, "/workspace"))
    msg = ctx.ui.notice.call_args.args[0]
    assert "alpha" in msg and "default" in msg


def test_workspace_command_switches_and_rebuilds_the_host(monkeypatch, tmp_path):
    from dacli.tui import slash

    monkeypatch.setenv("DACLI_HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)

    built = []

    class _Agent:
        def __init__(self):
            self.workspace = paths.active_workspace()
            self.memory = "mem"
            self.store = "store"

        async def initialize(self):
            return True

        async def shutdown(self):
            return None

    def make_host(settings):
        a = _Agent()
        built.append(a)
        return a

    ctx = _slash_ctx(make_host=make_host)
    asyncio.run(slash.dispatch(ctx, "/workspace new alpha"))

    assert paths.active_workspace() == "alpha"
    assert ctx.agent is built[-1]
    assert ctx.agent.workspace == "alpha"
    assert ctx.memory == "mem"
