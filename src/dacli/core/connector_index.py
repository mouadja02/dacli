"""Community connector index — fetch + install shared connectors (F-8).

``/new-connector`` generates connectors and ``/push-connector`` shares them;
this is the receiving end: ``dacli connector install <name> --index <src>``
fetches a connector folder from an index into ``connectors/<name>/``.

Index format (JSON or YAML)::

    connectors:
      <name>:
        description: "…"
        files: [manifest.yaml, connector.py, SKILL.md]   # plain filenames only
        base: "https://raw.githubusercontent.com/…/<name>"   # optional

``base`` defaults to a ``<name>/`` folder next to the index itself, so a plain
git repo of connector folders plus one index file is a working index — local
paths and http(s) URLs both work.

Trust posture (the registry treats downloads exactly like generated code):

* the connector is registered **disabled** in ``config/connectors.yaml``;
* it is validated with the **sandboxed** subprocess validator from prompt 06
  (`core.connector_generator.validate_connector`) — its module-level code never
  runs in the agent's process here;
* a bad install is fault-isolated by the registry (recorded in ``_failed``).
"""

from __future__ import annotations

import re
import shutil
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import yaml

from dacli.connectors.registry import (
    CONNECTORS_CONFIG_PATH,
    load_connectors_config,
    save_connectors_config,
)
from dacli.core.logging_setup import get_logger

log = get_logger(__name__)

_CONNECTORS_DIR = Path(__file__).resolve().parent.parent / "connectors"

#: Connector names must be importable package names; this also blocks traversal.
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
#: Files inside a connector folder: plain names only, no separators/dotfiles.
_FILE_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-]{0,127}$")

_MAX_FILE_BYTES = 1_000_000  # a connector source file should be tiny
_FETCH_TIMEOUT_S = 30


def _is_url(source: str) -> bool:
    return urllib.parse.urlparse(str(source)).scheme in ("http", "https")


def _read_source(source: str) -> bytes:
    """Read bytes from a local path or an http(s) URL (size-capped)."""
    if _is_url(source):
        with urllib.request.urlopen(source, timeout=_FETCH_TIMEOUT_S) as resp:
            return resp.read(_MAX_FILE_BYTES + 1)
    return Path(source).read_bytes()


def _join(base: str, *parts: str) -> str:
    if _is_url(base):
        return base.rstrip("/") + "/" + "/".join(parts)
    return str(Path(base).joinpath(*parts))


def load_index(index_source: str) -> dict[str, Any]:
    """Load and shape-check the index (JSON or YAML, local path or URL)."""
    raw = _read_source(index_source)
    data = yaml.safe_load(raw.decode("utf-8")) or {}
    connectors = data.get("connectors") if isinstance(data, dict) else None
    if not isinstance(connectors, dict):
        raise ValueError("index has no 'connectors' mapping")
    return data


def install_connector(
    name: str,
    settings: Any,
    index_source: str,
    *,
    connectors_dir: Path | None = None,
    config_path: str = CONNECTORS_CONFIG_PATH,
    force: bool = False,
) -> tuple[bool, str]:
    """Fetch ``name`` from the index into ``connectors/<name>/`` (disabled).

    Returns ``(ok, message)``. On success the user still has to ``/connect``
    (or enable it in the wizard) and restart — a downloaded connector never
    activates itself.
    """
    dest_root = connectors_dir or _CONNECTORS_DIR

    if not _NAME_RE.match(name or ""):
        return False, (
            f"invalid connector name '{name}' (lowercase letters, digits and "
            "underscores only)"
        )

    try:
        index = load_index(index_source)
    except Exception as exc:
        return False, f"could not load index from {index_source}: {exc}"

    entry = index["connectors"].get(name)
    if not isinstance(entry, dict):
        available = ", ".join(sorted(index["connectors"])) or "(none)"
        return False, f"'{name}' is not in the index. Available: {available}"

    files = entry.get("files") or []
    if not isinstance(files, list) or "manifest.yaml" not in files or "connector.py" not in files:
        return False, (
            f"index entry for '{name}' must list at least manifest.yaml and "
            "connector.py in 'files'"
        )
    for fname in files:
        if not isinstance(fname, str) or not _FILE_RE.match(fname) or ".." in fname:
            return False, f"refusing suspicious file name in index entry: {fname!r}"

    base = entry.get("base") or _join(_index_dir(index_source), name)

    dest = dest_root / name
    if dest.exists():
        if not force:
            return False, (
                f"connectors/{name}/ already exists — pass --force to overwrite"
            )
        shutil.rmtree(dest)

    # Fetch everything before writing anything, so a half-reachable index
    # never leaves a partial folder for the registry to trip over.
    contents: dict[str, bytes] = {}
    for fname in files:
        try:
            body = _read_source(_join(base, fname))
        except Exception as exc:
            return False, f"failed to fetch {fname} from {base}: {exc}"
        if len(body) > _MAX_FILE_BYTES:
            return False, f"{fname} exceeds the {_MAX_FILE_BYTES // 1000} kB cap"
        contents[fname] = body

    dest.mkdir(parents=True)
    for fname, body in contents.items():
        (dest / fname).write_bytes(body)
    if "__init__.py" not in contents:
        (dest / "__init__.py").write_text("", encoding="utf-8")

    # Register disabled (deny-by-default), exactly like generated connectors.
    try:
        config = load_connectors_config(config_path)
        config.setdefault("connectors", {}).setdefault(name, {})["enabled"] = False
        save_connectors_config(config, config_path)
    except Exception:
        log.debug("could not persist disabled state for %s", name, exc_info=True)

    # Sandboxed validation (prompt 06): untrusted code never imports in-process.
    from dacli.core.connector_generator import validate_connector

    ok, detail = validate_connector(name, settings, connectors_dir=dest_root)
    if not ok:
        return False, (
            f"installed connectors/{name}/ but validation FAILED: {detail}. "
            "It stays disabled — fix it with /debug-connector or delete the folder."
        )
    return True, (
        f"installed connectors/{name}/ (disabled). Validation passed: {detail} "
        f"Run /connect {name} and restart dacli to enable it."
    )


def _index_dir(index_source: str) -> str:
    if _is_url(index_source):
        return index_source.rsplit("/", 1)[0]
    return str(Path(index_source).parent)
