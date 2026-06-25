"""Docker-backed sandbox runtime — a hardened, per-session container.

Where :class:`~sandbox.runtime.SandboxRuntime` runs each script in a local
subprocess (weak OS isolation, especially on Windows), this runtime gives **each
session its own Docker container** the agent can do real work in — ``pip install``
dependencies, run Python — fully reused across runs in the session (so an install
in one run is there in the next).

Isolation from the host, layered:

* **non-root** code (image drops to uid 1000) with ``--cap-drop ALL`` and
  ``--security-opt no-new-privileges`` (no privilege escalation),
* **no host filesystem** except one bind mount: the session's ``workdir`` →
  ``/workspace`` (the mandatory *volume* link for exchanging data off-context),
* **resource caps**: ``--memory`` (no swap), ``--cpus``, ``--pids-limit``,
* the container only ever runs the **baked, read-only worker** (``/opt/dacli``);
  the host code is never mounted in.

The mandatory *network* link is the **governed bridge**: the container reaches
the parent on ``host.docker.internal`` and authenticates with a per-session
token, so model code touches a real platform only through the same
classify→policy→rollback→audit spine as the tool tier — and never sees a secret.
Outbound egress for the code itself (e.g. PyPI for ``pip install``) honours the
sandbox ``network`` policy via the in-container egress guard.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import subprocess
import uuid
from pathlib import Path
from typing import Any

from dacli.sandbox.bridge import start_bridge
from dacli.sandbox.policy import SandboxPolicy
from dacli.sandbox.runtime import SandboxRunResult
from dacli.sandbox.sdk import ConnectorSDK
import contextlib

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)

#: Default image tag built from ``sandbox/docker/Dockerfile``.
DEFAULT_IMAGE = "dacli-sandbox:latest"
_DOCKER_DIR = Path(__file__).resolve().parent / "docker"


class DockerSandboxRuntime:
    def __init__(
        self,
        policy: SandboxPolicy,
        execute_fn,
        *,
        registry: Any = None,
        result_store: Any = None,
        session_id: str = "default",
        image: str = DEFAULT_IMAGE,
        docker_bin: str = "docker",
        cpus: float = 2.0,
        pids_limit: int = 256,
        bridge_host: str = "host.docker.internal",
        auto_build: bool = True,
    ):
        self.policy = policy
        self._execute = execute_fn
        self._registry = registry
        self._result_store = result_store
        self.session_id = session_id or "default"
        self.image = image
        self.docker_bin = docker_bin
        self.cpus = float(cpus)
        self.pids_limit = int(pids_limit)
        self.bridge_host = bridge_host
        self.auto_build = auto_build

        safe = "".join(c if c.isalnum() else "-" for c in self.session_id).strip("-") or "default"
        self.container = f"dacli-sbx-{safe}"
        self._token = secrets.token_hex(16)
        self._started = False
        self._image_ready = False

    # ------------------------------------------------------------------
    # availability / late binding
    # ------------------------------------------------------------------
    @staticmethod
    def available(docker_bin: str = "docker") -> bool:
        """True if a working Docker engine is reachable (used by the factory)."""
        try:
            r = subprocess.run(
                [docker_bin, "version", "--format", "{{.Server.Version}}"],
                capture_output=True, text=True, timeout=20,
            )
            return r.returncode == 0 and bool(r.stdout.strip())
        except Exception:
            return False

    def bind_result_store(self, store: Any) -> None:
        self._result_store = store

    # ------------------------------------------------------------------
    # docker helpers
    # ------------------------------------------------------------------
    def _docker(self, *args: str, timeout: float = 120.0,
                check: bool = False) -> subprocess.CompletedProcess:
        return subprocess.run(
            [self.docker_bin, *args],
            capture_output=True, text=True, timeout=timeout, check=check,
        )

    def _image_exists(self) -> bool:
        return self._docker("image", "inspect", self.image, timeout=30).returncode == 0

    def _ensure_image(self) -> None:
        if self._image_ready or self._image_exists():
            self._image_ready = True
            return
        if not self.auto_build:
            raise RuntimeError(
                f"sandbox image '{self.image}' is not present and auto_build is off")
        build = self._docker("build", "-t", self.image, str(_DOCKER_DIR), timeout=1800)
        if build.returncode != 0:
            raise RuntimeError(f"docker build failed:\n{build.stderr[-2000:]}")
        self._image_ready = True

    def _container_running(self) -> bool:
        r = self._docker("inspect", "-f", "{{.State.Running}}", self.container, timeout=30)
        return r.returncode == 0 and r.stdout.strip() == "true"

    def _ensure_container(self) -> None:
        if self._started and self._container_running():
            return
        self._ensure_image()

        # Remove any stale container holding the per-session name.
        self._docker("rm", "-f", self.container, timeout=60)

        workdir_abs = str(Path(self.policy.workdir).resolve())
        Path(workdir_abs).mkdir(parents=True, exist_ok=True)
        mem = max(64, int(self.policy.max_memory_mb))

        args: list[str] = [
            "run", "-d", "--name", self.container,
            "--label", "dacli.sandbox=1", "--label", f"dacli.session={self.session_id}",
            # --- host isolation ---
            "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges",
            "--pids-limit", str(self.pids_limit),
            "--memory", f"{mem}m", "--memory-swap", f"{mem}m",
            "--cpus", str(self.cpus),
            # --- the one mandatory volume link (data, off-context) ---
            "-v", f"{workdir_abs}:/workspace",
            # --- reach the governed bridge on the host (Linux engines need this) ---
            "--add-host", "host.docker.internal:host-gateway",
            self.image,
        ]
        run = self._docker(*args, timeout=180)
        if run.returncode != 0:
            raise RuntimeError(f"docker run failed:\n{run.stderr[-2000:]}")
        self._started = True

    # ------------------------------------------------------------------
    # the run
    # ------------------------------------------------------------------
    async def run_script(self, code: str) -> SandboxRunResult:
        try:
            self._ensure_container()
        except Exception as exc:
            return SandboxRunResult(ok=False, error=f"docker sandbox unavailable: {exc}")

        run_id = uuid.uuid4().hex[:10]
        run_dir = Path(self.policy.workdir).resolve() / f"run_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)
        script = run_dir / "script.py"
        script.write_text(code, encoding="utf-8")
        # The container runs as non-root (uid 1000).  The workdir may have been
        # created by tempfile.mkdtemp (mode 0700) or under a restrictive umask,
        # so ensure the bind-mounted paths are world-traversable and the run dir
        # is world-writable — the worker writes result.json and user scripts may
        # create files there too (e.g. marker.txt in the volume round-trip test).
        os.chmod(self.policy.workdir, 0o755)
        os.chmod(run_dir, 0o777)
        os.chmod(script, 0o644)
        cpath = f"/workspace/run_{run_id}"   # same dir, container-side

        sdk = ConnectorSDK(self._execute, registry=self._registry,
                           result_store=self._result_store, workdir=str(run_dir))
        call_count = {"n": 0}

        # Governed boundary, bound host-reachable (0.0.0.0) + token-gated so only
        # *this* session's container can issue governed calls.
        server, port = await start_bridge(
            sdk, host="0.0.0.0", token=self._token,
            on_run=lambda: call_count.__setitem__("n", call_count["n"] + 1),
        )

        exec_args = [
            self.docker_bin, "exec",
            "-w", cpath,   # run in the run dir so relative file writes land there
            "-e", f"DACLI_SANDBOX_BRIDGE_HOST={self.bridge_host}",
            "-e", f"DACLI_SANDBOX_BRIDGE_PORT={port}",
            "-e", f"DACLI_SANDBOX_BRIDGE_TOKEN={self._token}",
            "-e", f"DACLI_SANDBOX_NETWORK={self.policy.network}",
            "-e", f"DACLI_SANDBOX_ALLOWLIST={','.join(self.policy.egress_allowlist)}",
            self.container,
            "python", "/opt/dacli/worker.py", "--script", f"{cpath}/script.py", "--workdir", cpath,
        ]

        timed_out = False
        proc = await asyncio.create_subprocess_exec(
            *exec_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=self.policy.wall_clock_seconds)
        except asyncio.TimeoutError:
            timed_out = True
            with contextlib.suppress(Exception):
                proc.kill()
            # Best-effort: stop the runaway worker inside the container.
            self._docker("exec", self.container, "pkill", "-f", f"run_{run_id}", timeout=30)
            stdout_b, stderr_b = b"", b"sandbox run exceeded wall-clock limit"
        finally:
            server.close()
            await server.wait_closed()

        stdout = stdout_b.decode("utf-8", "replace") if stdout_b else ""
        stderr = stderr_b.decode("utf-8", "replace") if stderr_b else ""

        returned, err, ok = None, None, (proc.returncode == 0 and not timed_out)
        result_file = run_dir / "result.json"
        if result_file.exists():
            try:
                payload = json.loads(result_file.read_text(encoding="utf-8"))
                returned = payload.get("returned")
                err = payload.get("error")
                ok = bool(payload.get("ok")) and not timed_out
            except Exception:
                log.debug("unreadable sandbox result.json in %s", run_dir, exc_info=True)
        if timed_out:
            err = "wall-clock timeout"

        artifacts = [str(p.relative_to(run_dir)) for p in run_dir.iterdir()
                     if p.name not in ("script.py", "result.json")]

        return SandboxRunResult(
            ok=ok,
            output=self._truncate(stdout),
            stderr=self._truncate(stderr),
            returned=returned,
            error=err,
            timed_out=timed_out,
            exit_code=proc.returncode,
            workdir=str(run_dir),
            artifacts=sorted(artifacts),
            calls=call_count["n"],
        )

    def _truncate(self, text: str) -> str:
        cap = self.policy.max_output_chars
        if not text:
            return ""
        if len(text) <= cap:
            return text
        return text[:cap] + f"\n…[truncated {len(text) - cap} chars; full output on disk]"

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Tear the session container down (best-effort)."""
        if self._started:
            with contextlib.suppress(Exception):
                self._docker("rm", "-f", self.container, timeout=60)
            self._started = False
