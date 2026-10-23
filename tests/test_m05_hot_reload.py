"""M05 — hot reload.

The :class:`ExtensionHost` re-discovers extensions, validates each changed module
in a child process before importing it, and adopts the new version in the same
process — no restart. A changed module that fails validation keeps its previously
loaded version. Session state survives a reload: an extension appends state to an
append-only session log and replays it from its ``session_start`` handler.
"""

import asyncio
import os
from textwrap import dedent

from dacli.core.extensions import (
    ExtensionHost,
    ToolContext,
    _import_extension,
    _validate_extension_in_subprocess,
)


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


def test_validator_executes_current_source_not_stale_bytecode(tmp_path):
    package = tmp_path / "sample"
    package.mkdir()
    init_file = package / "__init__.py"
    marker = tmp_path / "validated.txt"
    source = (
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text(\"old\")\n"
        "def register(api):\n"
        "    pass\n"
    )
    init_file.write_text(source, encoding="utf-8")
    ok, message = _validate_extension_in_subprocess("sample", init_file)
    assert ok, message
    assert marker.read_text(encoding="utf-8") == "old"

    marker.unlink()
    stat = init_file.stat()
    updated = source.replace('write_text("old")', 'write_text("new")')
    assert len(updated) == len(source)
    init_file.write_text(updated, encoding="utf-8")
    os.utime(init_file, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    ok, message = _validate_extension_in_subprocess("sample", init_file)

    assert ok, message
    assert marker.read_text(encoding="utf-8") == "new"


def test_validator_uses_captured_source(tmp_path):
    init_file = tmp_path / "__init__.py"
    marker = tmp_path / "unreviewed.txt"
    init_file.write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('ran')\n"
        "def register(api):\n    pass\n",
        encoding="utf-8",
    )
    source = 'def register(api):\n    if "雪" != chr(38634):\n        raise ValueError("wrong encoding")\n'

    ok, message = _validate_extension_in_subprocess("sample", init_file, source)

    assert (ok, message, marker.exists()) == (True, "valid", False)


def test_import_uses_captured_source(tmp_path):
    init_file = tmp_path / "__init__.py"
    marker = tmp_path / "unreviewed.txt"
    init_file.write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('ran')\n",
        encoding="utf-8",
    )

    module = _import_extension("sample", init_file, 'value = "雪"\n')

    assert (module.value, marker.exists()) == ("雪", False)


def test_invalid_utf8_is_isolated(tmp_path):
    init_file = tmp_path / "sample" / "__init__.py"
    init_file.parent.mkdir()
    init_file.write_bytes(b"\xff")
    host = ExtensionHost(tmp_path)

    result = host.load()

    assert ("sample" in result.failed, host.registry.extension_ids()) == (True, [])


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
