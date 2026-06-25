"""Diagnostics for `dacli doctor` / `/doctor` — one collector, two surfaces.

Answers the questions a new user can't otherwise answer without reading source:
where config/state/log resolve (P01), whether the LLM key is present and where it
came from (never the value), the governance/sandbox/terminal posture, and which
connectors loaded vs. were skipped and why.

:func:`collect` returns a :class:`Diagnostics`; ``dacli doctor`` renders it through
:class:`~dacli.tui.ui.DacliUI`, ``--json`` emits :meth:`Diagnostics.to_dict`, and
the in-chat ``/doctor`` renders the same data. Read-only and offline — the only
network it ever touches is the opt-in ``--ping`` models/list probe.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from dacli.config.settings import is_llm_configured
from dacli.core import paths

#: Short, bounded timeout for the opt-in LLM ping (seconds).
PING_TIMEOUT = 5.0


@dataclass
class Diagnostics:
    config: dict[str, Any]
    state_dir: dict[str, Any]
    log: dict[str, Any]
    llm: dict[str, Any]
    governance: dict[str, Any]
    sandbox: dict[str, Any]
    terminal: dict[str, Any]
    connectors: dict[str, Any]
    cost: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "problems": self.problems,
            "config": self.config,
            "state_dir": self.state_dir,
            "log": self.log,
            "llm": self.llm,
            "governance": self.governance,
            "sandbox": self.sandbox,
            "terminal": self.terminal,
            "connectors": self.connectors,
            "cost": self.cost,
        }

    @property
    def problems(self) -> list[str]:
        # Hard problems make `dacli doctor` exit non-zero so CI/scripts can gate
        # on it. A failed ping is a soft signal (network may be down by design),
        # so it never counts here.
        out = []
        if not self.llm["key"] or not self.llm["model"]:
            out.append("llm not configured (need a provider key and model)")
        if self.config["path"] is None and self.config["requested"]:
            out.append(f"config not found at {self.config['requested']}")
        return out

    @property
    def ok(self) -> bool:
        return not self.problems


def _llm_key_source(raw_cfg: dict[str, Any]) -> str | None:
    # Where the resolved key came from, without ever reading its value: a
    # ${VAR} reference that resolves -> env; a literal in config.yaml -> config;
    # otherwise the encrypted dacli.json secrets store. None means no key.
    from dacli.config.settings import _dacli_base_dir, _load_dacli_secrets

    raw_key = (raw_cfg.get("llm") or {}).get("api_key", "")
    if isinstance(raw_key, str) and raw_key.startswith("${") and raw_key.endswith("}"):
        if os.environ.get(raw_key[2:-1]):
            return "env"
    elif raw_key:
        return "config"
    secrets = _load_dacli_secrets(_dacli_base_dir(raw_cfg))
    if (secrets.get("llm") or {}).get("api_key"):
        return "encrypted store"
    return None


def _docker_image_present(docker_bin: str, image: str) -> bool:
    import subprocess

    try:
        r = subprocess.run(
            [docker_bin, "image", "inspect", image],
            capture_output=True, text=True, timeout=30,
        )
        return r.returncode == 0
    except Exception:
        return False


def _sandbox_info(settings: Any) -> dict[str, Any]:
    sb = settings.sandbox
    if not sb.enabled:
        return {"enabled": False, "runtime": None, "image_present": None, "fallback": False}
    mode = (sb.runtime or "auto").strip().lower()
    docker_bin = sb.docker_bin or "docker"
    docker_ok = False
    if mode in ("docker", "auto"):
        from dacli.sandbox.docker_runtime import DockerSandboxRuntime

        docker_ok = DockerSandboxRuntime.available(docker_bin)
    if docker_ok:
        return {
            "enabled": True,
            "runtime": "docker",
            "image_present": _docker_image_present(docker_bin, sb.docker_image),
            "fallback": False,
        }
    return {
        "enabled": True,
        "runtime": "subprocess",
        "image_present": None,
        # `runtime: docker` was asked for but no engine is reachable.
        "fallback": mode == "docker",
    }


async def _ping_llm(settings: Any) -> bool:
    # Cheap models/list against the configured provider's SDK client, bounded by
    # PING_TIMEOUT. Best-effort: any failure (offline, auth, provider without a
    # models endpoint) is a plain False, never an exception.
    import asyncio

    from dacli.ai.llm import LLMClient

    try:
        client = LLMClient(settings)
        await client.initialize()
        sdk = client._impl().client
        await asyncio.wait_for(sdk.models.list(), timeout=PING_TIMEOUT)
        return True
    except Exception:
        return False


def collect(settings: Any, *, config_path: str | None = None, ping: bool = False) -> Diagnostics:
    """Gather diagnostics. ``ping`` adds the bounded LLM models/list probe."""
    import yaml

    config_file = paths.resolve_config_path(config_path)
    raw_cfg: dict[str, Any] = {}
    if config_file is not None:
        try:
            raw_cfg = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
        except Exception:
            raw_cfg = {}

    root = paths.project_root()
    if os.environ.get(paths.STATE_PATH_ENV):
        kind = "override"
    elif root is not None:
        kind = "project-local"
    else:
        kind = "global"

    key_present = is_llm_configured(settings) or bool(
        settings.llm.api_key and not str(settings.llm.api_key).startswith("${")
    )
    ping_state = "skipped"
    if ping and key_present:
        import asyncio

        ping_state = "ok" if asyncio.run(_ping_llm(settings)) else "failed"

    policy_path = paths.resolve_policy_path(settings)
    policy_default = policy_path == paths.packaged_asset("config", "policy.yaml")

    from dacli.connectors.registry import CONNECTORS_CONFIG_PATH, ConnectorRegistry

    registry = ConnectorRegistry(settings, config_path=CONNECTORS_CONFIG_PATH)
    enabled = [
        cid for cid in registry.get_catalog() if registry.is_connector_enabled(cid)
    ]
    skipped = registry.failed_connectors()

    from dacli.sandbox.shells import select_backend

    return Diagnostics(
        config={
            "path": str(config_file) if config_file else None,
            "found": config_file is not None,
            "requested": config_path,
        },
        state_dir={"path": str(paths.state_dir()), "kind": kind},
        log={"path": str(paths.state_dir() / "dacli.log")},
        llm={
            "provider": settings.llm.provider,
            "model": settings.llm.model,
            "key": key_present,
            "source": _llm_key_source(raw_cfg) if key_present else None,
            "ping": ping_state,
        },
        governance={
            "enabled": bool(settings.governance.enabled),
            "policy": str(policy_path),
            "policy_default": policy_default,
        },
        sandbox=_sandbox_info(settings),
        terminal={
            "enabled": bool(settings.terminal.enabled),
            "shell": select_backend(settings.terminal.shell).name,
            "scope": settings.terminal.scope,
        },
        connectors={
            "enabled": len(enabled),
            "skipped": len(skipped),
            "skipped_detail": skipped,
        },
        cost={
            # The F-4 gate threshold (None = off) and which enabled connectors
            # the cost advisor can estimate/report on. Offline: no warehouse is
            # queried here — `dacli cost` does that.
            "confirm_usd": settings.governance.cost_confirm_usd,
            "advisors": [c for c in ("snowflake", "bigquery", "databricks") if c in enabled],
        },
    )
