"""M03 — ExtensionAPI + register(api) loader.

A hand-written ``<dir>/sample/__init__.py`` registers a tool that shows up in the
new registry's tool definitions; a broken module is isolated and reported rather
than fatal; and a tool that declares no post-condition is refused at registration.
"""

from textwrap import dedent

import pytest

from dacli.core import paths
from dacli.core.extensions import ExtensionAPI, load_extensions
from dacli.core.verify import MissingPostConditionError


def _write_ext(root, name, body):
    pkg = root / name
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(dedent(body), encoding="utf-8")
    return pkg


SAMPLE = """
    def register(api):
        api.config_field("bucket", required=True, description="Target bucket")
        api.config_field("access_key", secret=True)

        @api.tool(
            name="sample_list",
            description="List objects under a prefix",
            parameters={"prefix": {"type": "string"}},
            risk="safe",
            postconditions=["result_succeeded"],
        )
        async def sample_list(args, ctx):
            return ctx.ok([])
"""


def test_sample_tool_appears_in_definitions(tmp_path):
    _write_ext(tmp_path, "sample", SAMPLE)
    reg = load_extensions(tmp_path)

    assert reg.failed_extensions() == {}
    names = [d["function"]["name"] for d in reg.get_tool_definitions()]
    assert names == ["sample_list"]
    # The bare property map is wrapped into a real object schema.
    params = reg.get_tool_definitions()[0]["function"]["parameters"]
    assert params == {"type": "object", "properties": {"prefix": {"type": "string"}}}
    assert reg.resolve("sample_list").spec.risk.value == "safe"


def test_config_fields_recorded(tmp_path):
    _write_ext(tmp_path, "sample", SAMPLE)
    reg = load_extensions(tmp_path)
    fields = {f.name: f for f in reg.config_fields("sample")}
    assert fields["bucket"].required is True
    assert fields["access_key"].is_secret is True


def test_broken_extension_isolated_not_fatal(tmp_path):
    _write_ext(tmp_path, "broken", "this is not valid python =\n")
    _write_ext(tmp_path, "sample", SAMPLE)

    reg = load_extensions(tmp_path)  # must not raise

    assert "sample_list" in [d["function"]["name"] for d in reg.get_tool_definitions()]
    assert "broken" in reg.failed_extensions()
    assert "sample" not in reg.failed_extensions()


def test_extension_without_register_reported(tmp_path):
    _write_ext(tmp_path, "noreg", "x = 1\n")
    reg = load_extensions(tmp_path)
    assert "register" in reg.failed_extensions()["noreg"]


def test_tool_without_postcondition_refused():
    api = ExtensionAPI("ext")
    with pytest.raises(MissingPostConditionError):
        api.tool(name="t", description="d", risk="safe", postconditions=[])


def test_tool_without_postcondition_fails_extension_load(tmp_path):
    _write_ext(
        tmp_path,
        "nopc",
        """
        def register(api):
            @api.tool(name="t", description="d", risk="safe", postconditions=[])
            async def t(args, ctx):
                return ctx.ok(None)
        """,
    )
    reg = load_extensions(tmp_path)
    assert "nopc" not in reg.extension_ids()
    assert "post-condition" in reg.failed_extensions()["nopc"]


def test_tool_without_risk_refused():
    api = ExtensionAPI("ext")
    with pytest.raises(ValueError, match="risk"):
        api.tool(name="t", description="d", postconditions=["result_succeeded"])


def test_partial_registration_not_committed_on_raise(tmp_path):
    _write_ext(
        tmp_path,
        "partial",
        """
        def register(api):
            @api.tool(name="good", description="d", risk="safe",
                      postconditions=["result_succeeded"])
            async def good(args, ctx):
                return ctx.ok(None)
            raise RuntimeError("boom")
        """,
    )
    reg = load_extensions(tmp_path)
    assert reg.resolve("good") is None
    assert "partial" in reg.failed_extensions()


def test_default_dir_uses_resource_dir(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.DACLI_HOME_ENV, str(tmp_path))
    ext_root = tmp_path / "extensions"
    _write_ext(ext_root, "sample", SAMPLE)
    # No project marker around tmp_path, so resource_dir falls to the global root.
    monkeypatch.chdir(tmp_path)
    reg = load_extensions()
    assert "sample_list" in [d["function"]["name"] for d in reg.get_tool_definitions()]


def test_config_returns_empty_without_provider():
    api = ExtensionAPI("ext")
    assert api.config() == {}


def test_config_uses_provider():
    api = ExtensionAPI("s3", config_provider=lambda name: {"bucket": name})
    assert api.config() == {"bucket": "s3"}
