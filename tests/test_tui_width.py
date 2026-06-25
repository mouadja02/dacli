"""Width-responsive render regression tests (P06).

The governance preview surfaces — `audit`, `catalog`, `schema` — are where a
skeptical user judges the product, and they are exactly the tables Rich mangles
into vertical character stacks ("ro/ll/ba/ck") once the sum of minimum column
widths exceeds the terminal. These tests render every such surface to a 70- and
a 40-column console and assert the headers never char-stack, then check that
wide output is unchanged.
"""

import types

import pytest
from rich.console import Console

from dacli.scripts.cli import _print_audit
from dacli.tui import DacliUI


def _ui(width: int, glyphs: str = "unicode") -> DacliUI:
    console = Console(record=True, width=width, force_terminal=False)
    settings = types.SimpleNamespace(
        ui=types.SimpleNamespace(glyphs=glyphs, max_render_rows=120)
    )
    return DacliUI(settings=settings, console=console)


def _thin(line: str) -> bool:
    # Rich char-stacks a squeezed column into a vertical ribbon of tiny
    # fragments ("roll"/"back", "creat"/"e"/"bronz"/"e"). A line that belongs to
    # such a ribbon carries no word longer than a few chars; a normal line has
    # at least one real word.
    toks = [t for t in line.split() if any(c.isalnum() for c in t)]
    if not toks:
        return False
    return max(len(t) for t in toks) <= 5


def _char_stacked(out: str) -> bool:
    run = 0
    for line in out.splitlines():
        if _thin(line):
            run += 1
            if run >= 4:
                return True
        else:
            run = 0
    return False


def _catalog_entry(name="db.schema.a_very_long_object_name_that_will_not_fit"):
    scope = dict(zip(("database", "schema", "object"), name.split("."), strict=False))
    return types.SimpleNamespace(
        connector="snowflake",
        object_type="table",
        scope=scope,
        row_count_estimate=1_234_567,
        last_verified=None,
        is_stale=lambda: True,
        columns=[
            {"name": "customer_identifier", "type": "varchar",
             "description": "the natural key used across the warehouse"},
            {"name": "ts", "type": "timestamp_ntz", "description": "load time"},
        ],
    )


def _audit_ledger():
    decisions = [
        {
            "tool_name": "execute_snowflake_query",
            "tier": "irreversible",
            "decision_id": "dec_0001",
            "events": [
                {"kind": "classification",
                 "summary": "DROP TABLE prod.users — irreversible"},
                {"kind": "approval", "summary": "approved by operator"},
                {"kind": "execution", "summary": "ran in 412ms"},
                {"kind": "post_condition", "summary": "passed"},
            ],
        },
    ]
    return types.SimpleNamespace(decisions=lambda **_: decisions)


# Every governance surface, rendered through a UI at a given width.
def _render(surface: str, ui: DacliUI) -> str:
    if surface == "audit":
        _print_audit(None, "sess", target=ui)
    elif surface == "catalog":
        ui.catalog_table([_catalog_entry(), _catalog_entry("db.s.orders")])
    elif surface == "schema":
        ui.schema_panel(_catalog_entry())
    return ui.console.export_text()


# _print_audit needs a ledger; the others build their own data.
def _render_at(surface: str, width: int) -> str:
    ui = _ui(width)
    if surface == "audit":
        _print_audit(_audit_ledger(), "sess", target=ui)
        return ui.console.export_text()
    return _render(surface, ui)


SURFACES = ["audit", "catalog", "schema"]


@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("width", [70, 40])
def test_governance_surface_does_not_char_stack(surface, width):
    out = _render_at(surface, width)
    assert not _char_stacked(out), (
        f"{surface} at width {width} char-stacked its headers:\n{out}"
    )
    # And nothing spills past the terminal edge.
    assert all(len(line) <= width for line in out.splitlines())


@pytest.mark.parametrize("surface", SURFACES)
def test_wide_output_keeps_full_content(surface):
    out = _render_at(surface, 120)
    assert not _char_stacked(out)
    if surface == "catalog":
        assert "Connector" in out and "orders" in out
    elif surface == "schema":
        assert "customer_identifier" in out
    elif surface == "audit":
        assert "execute_snowflake_query" in out


def test_narrow_catalog_stacks_keys_and_keeps_object_name():
    out = _render_at("catalog", 40)
    # Stacked mode keeps the full identifier (the point of the surface).
    assert "orders" in out
    assert "snowflake" in out
