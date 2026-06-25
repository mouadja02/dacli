"""Run bundle export (F-10) — compliance / postmortem artifact.

``dacli audit`` reconstructs decisions and ``dacli replay`` re-runs scenarios;
this combines a session's evidence into one exportable zip: the conversation
transcript, the session state (tool log + todos), the audit-ledger slice for
that session, and the usage summary, described by a small ``manifest.json``.

Everything written into the bundle passes through :func:`core.store._redact`
so secret-keyed values (api_key/password/token/…) never leave the machine.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any

from dacli.core.store import DacliStore, _redact
from dacli.core.timeutils import now_iso

BUNDLE_VERSION = 1


def _audit_path(settings: Any) -> Path:
    # Same resolution as `dacli audit`: explicit governance.audit_path, else
    # <state_dir parent>/audit.jsonl.
    state_dir = str(Path(settings.agent.state_path).parent)
    gov = getattr(settings, "governance", None)
    configured = getattr(gov, "audit_path", None) if gov else None
    return Path(configured or f"{state_dir}/audit.jsonl")


def _latest_session_id(state_dir: Path) -> str | None:
    newest: tuple[float, str] | None = None
    for state_file in state_dir.glob("state_*.json"):
        sid = state_file.stem[len("state_"):]
        try:
            mtime = state_file.stat().st_mtime
        except OSError:
            continue
        if newest is None or mtime > newest[0]:
            newest = (mtime, sid)
    return newest[1] if newest else None


def _load_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _dumps(payload: Any) -> str:
    return json.dumps(_redact(payload), indent=2, default=str)


def export_run_bundle(
    settings: Any,
    session_id: str | None,
    out_path: str | None = None,
) -> dict[str, Any]:
    """Write the bundle zip and return its manifest.

    ``session_id`` defaults to the most recently updated session. Raises
    ``FileNotFoundError`` when no session (or the named one) exists.
    """
    state_dir = Path(settings.agent.state_path)
    history_dir = Path(settings.agent.history_path)

    sid = session_id or _latest_session_id(state_dir)
    if not sid:
        raise FileNotFoundError(f"no sessions found under {state_dir}")
    state_file = state_dir / f"state_{sid}.json"
    history_file = history_dir / f"history_{sid}.json"
    if not state_file.exists() and not history_file.exists():
        raise FileNotFoundError(f"session not found: {sid}")

    out = Path(out_path or f"dacli_run_{sid}.zip")
    if out.parent and not out.parent.exists():
        out.parent.mkdir(parents=True, exist_ok=True)

    members: dict[str, str] = {}

    history = _load_json(history_file)
    if history is not None:
        members["history.json"] = _dumps(history)
    state = _load_json(state_file)
    if state is not None:
        members["state.json"] = _dumps(state)

    # Audit slice: only this session's lines, each redacted individually so the
    # bundle stays JSONL-greppable like the source ledger.
    audit_file = _audit_path(settings)
    audit_lines: list[str] = []
    if audit_file.exists():
        try:
            for line in audit_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except Exception:
                    continue
                if event.get("session_id") == sid:
                    audit_lines.append(json.dumps(_redact(event), default=str))
        except Exception:
            audit_lines = []
    if audit_lines:
        members["audit.jsonl"] = "\n".join(audit_lines) + "\n"

    store = DacliStore(base_dir=str(Path(settings.agent.state_path).parent))
    members["usage.json"] = _dumps(store.usage_summary(sid))

    manifest = {
        "bundle_version": BUNDLE_VERSION,
        "session_id": sid,
        "created_at": now_iso(),
        "contents": sorted(members),
        "counts": {
            "messages": len(history) if isinstance(history, list) else 0,
            "audit_events": len(audit_lines),
        },
    }
    members["manifest.json"] = json.dumps(manifest, indent=2)

    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name in sorted(members):
            zf.writestr(name, members[name])

    manifest["path"] = str(out)
    return manifest
