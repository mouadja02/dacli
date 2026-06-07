"""Pick the sandbox runtime for a session.

``sandbox.runtime`` in settings chooses the backend:

* ``"docker"`` — a hardened per-session container (strong host isolation; the
  agent can ``pip install`` + run Python). Falls back to the subprocess runtime
  if no Docker engine is reachable, so the agent still works.
* ``"subprocess"`` — the local in-process runtime (no Docker dependency;
  weaker OS isolation — fine for trusted/offline use and CI).
* ``"auto"`` (default) — Docker when a working engine is present, else subprocess.

Either runtime exposes the same surface (``run_script`` / ``bind_result_store`` /
``close``) so the connector and governance spine are identical regardless.
"""

from __future__ import annotations

from typing import Any

from dacli.sandbox.policy import SandboxPolicy
from dacli.sandbox.runtime import SandboxRuntime


def build_sandbox_runtime(
    settings: Any,
    execute_fn,
    *,
    registry: Any = None,
    result_store: Any = None,
    session_id: str = "default",
) -> tuple[Any, str]:
    """Return ``(runtime, backend_name)`` for the configured/available backend."""
    policy = SandboxPolicy.from_settings(settings)
    sb = getattr(settings, "sandbox", None)
    mode = (getattr(sb, "runtime", "auto") or "auto").strip().lower()
    docker_bin = getattr(sb, "docker_bin", "docker") or "docker"

    if mode in ("docker", "auto"):
        # Imported lazily so a Docker-less environment never pays for the import.
        from dacli.sandbox.docker_runtime import DockerSandboxRuntime

        if DockerSandboxRuntime.available(docker_bin):
            return (
                DockerSandboxRuntime(
                    policy, execute_fn,
                    registry=registry, result_store=result_store, session_id=session_id,
                    image=getattr(sb, "docker_image", "dacli-sandbox:latest"),
                    docker_bin=docker_bin,
                    cpus=getattr(sb, "docker_cpus", 2.0),
                    pids_limit=getattr(sb, "docker_pids_limit", 256),
                    auto_build=getattr(sb, "docker_auto_build", True),
                ),
                "docker",
            )
        if mode == "docker":
            # Explicitly asked for Docker but none is reachable — degrade to the
            # subprocess runtime rather than disabling code execution entirely.
            pass

    return (
        SandboxRuntime(policy, execute_fn, registry=registry, result_store=result_store),
        "subprocess",
    )
