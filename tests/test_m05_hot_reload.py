"""M05 — hot reload.

The :class:`ExtensionHost` re-discovers extensions, validates each changed module
in a child process before importing it, and adopts the new version in the same
process — no restart. A changed module that fails validation keeps its previously
loaded version. Session state survives a reload: an extension appends state to an
append-only session log and replays it from its ``session_start`` handler.
"""

import asyncio
from textwrap import dedent

from dacli.core.extensions import ExtensionHost, ToolContext


def _write_ext(root, name, body):
    pkg = root / name
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text(dedent(body), encoding="utf-8")


def _call(host, tool_name, args=None):
    rt = host.registry.resolve(tool_name)
    ctx = ToolContext(rt.spec.name)
    return asyncio.run(rt.handler(dict(args or {}), ctx))


def _sample(version):
    return f"""
    def register(api):
        @api.tool(
            name="sample_list",
            description="{version}",
            risk="safe",
            postconditions=["result_succeeded"],
        )
        async def sample_list(args, ctx):
            return ctx.ok("{version}")
    """


STATEFUL = """
    def register(api):
        state = {"n": 0}

        def on_start(reason):
            for entry in api.entries():
                state["n"] = entry["n"]
        api.on("session_start", on_start)

        @api.tool(name="bump", description="%s", risk="safe",
                  postconditions=["result_succeeded"])
        async def bump(args, ctx):
            state["n"] += 1
            api.append_entry({"n": state["n"]})
            return ctx.ok(state["n"])
"""


def test_edited_extension_is_live_after_reload(tmp_path):
    _write_ext(tmp_path, "sample", _sample("v1"))
    host = ExtensionHost(tmp_path)
    host.load()
    assert _call(host, "sample_list").data == "v1"

    _write_ext(tmp_path, "sample", _sample("v2"))
    result = host.reload()

    assert result.reloaded == ["sample"]
    assert _call(host, "sample_list").data == "v2"


def test_unchanged_extension_is_not_reloaded(tmp_path):
    _write_ext(tmp_path, "sample", _sample("v1"))
    host = ExtensionHost(tmp_path)
    host.load()

    result = host.reload()

    assert result.unchanged == ["sample"]
    assert result.reloaded == []


def test_broken_edit_rejected_prior_tool_still_works(tmp_path):
    _write_ext(tmp_path, "sample", _sample("v1"))
    host = ExtensionHost(tmp_path)
    host.load()

    # Drop the post-condition: register(api) now raises, so the edit is invalid.
    _write_ext(
        tmp_path,
        "sample",
        """
        def register(api):
            @api.tool(name="sample_list", description="broken", risk="safe",
                      postconditions=[])
            async def sample_list(args, ctx):
                return ctx.ok("v2")
        """,
    )
    result = host.reload()

    assert "sample" in result.failed
    assert "post-condition" in result.failed["sample"]
    # The prior version is still the one that runs.
    assert _call(host, "sample_list").data == "v1"


def test_broken_edit_is_not_imported_into_the_process(tmp_path):
    # A module whose import has a side effect must not run that side effect when
    # its changed version fails validation — validation happens in a child.
    _write_ext(tmp_path, "sample", _sample("v1"))
    host = ExtensionHost(tmp_path)
    host.load()

    marker = tmp_path / "ran.txt"
    _write_ext(
        tmp_path,
        "sample",
        f"""
        from pathlib import Path
        Path({str(marker)!r}).write_text("ran")
        syntax error here =
        """,
    )
    result = host.reload()

    assert "sample" in result.failed
    assert not marker.exists()


def test_session_state_survives_reload(tmp_path):
    _write_ext(tmp_path, "counter", STATEFUL % "v1")
    host = ExtensionHost(tmp_path)
    host.load()

    assert _call(host, "bump").data == 1  # n: 0 -> 1, logged

    _write_ext(tmp_path, "counter", STATEFUL % "v2")  # force a reload
    host.reload()

    # The fresh module replayed the logged state from its session_start handler.
    assert _call(host, "bump").data == 2


def _lifecycle(version):
    return f"""
    def register(api):
        def on_start(reason):
            api.append_entry(("start", reason))
        def on_shutdown(reason):
            api.append_entry(("shutdown", reason))
        api.on("session_start", on_start)
        api.on("session_shutdown", on_shutdown)

        @api.tool(name="noop", description="{version}", risk="safe",
                  postconditions=["result_succeeded"])
        async def noop(args, ctx):
            return ctx.ok(None)
    """


def test_session_events_fire_shutdown_then_start_on_reload(tmp_path):
    _write_ext(tmp_path, "lifecycle", _lifecycle("v1"))
    host = ExtensionHost(tmp_path)
    host.load()

    _write_ext(tmp_path, "lifecycle", _lifecycle("v2"))
    host.reload()

    assert host.log.entries("lifecycle") == [
        ("start", "startup"),
        ("shutdown", "reload"),
        ("start", "reload"),
    ]


def test_deleted_extension_is_dropped_on_reload(tmp_path):
    _write_ext(tmp_path, "sample", _sample("v1"))
    host = ExtensionHost(tmp_path)
    host.load()
    assert host.registry.resolve("sample_list") is not None

    (tmp_path / "sample" / "__init__.py").unlink()
    result = host.reload()

    assert result.removed == ["sample"]
    assert host.registry.resolve("sample_list") is None
