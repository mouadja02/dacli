"""Sandbox runtime — the isolated executor.

Runs agent-written Python in a separate process under a :class:`SandboxPolicy`
(wall-clock + memory limits, egress guard) and exposes the governed
:class:`~sandbox.sdk.ConnectorSDK` to that code over a localhost bridge.

Flow per run:

1. create an isolated run workspace under the sandbox workdir,
2. start an asyncio localhost bridge bound to an ephemeral port,
3. spawn ``python -m sandbox._worker`` with the egress/resource env + rlimits,
4. service the worker's ``run`` requests by awaiting the **parent's** governed
   SDK (so every action is classified + policy-checked — not a bypass),
5. enforce the wall-clock limit, capture (and truncate) stdout/stderr,
6. read the structured ``result.json`` the worker leaves behind.

Only the bounded stdout + the structured return value flow back to the caller;
the bulk data the script produced stays on disk in the run workspace.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dacli.sandbox.bridge import start_bridge
from dacli.sandbox.policy import SandboxPolicy
from dacli.sandbox.sdk import ConnectorSDK
import contextlib

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)

# Each sandbox run leaves a ``run_*`` workspace on disk. Without pruning these
# grow unbounded; retain at most this many (the most recent), sweeping the rest.
MAX_SANDBOX_RUNS = 20


def _sweep_run_dirs(workdir: Path, keep: int = MAX_SANDBOX_RUNS) -> None:
    """Prune old ``run_*`` sandbox workspaces under *workdir*, keeping the most
    recent *keep* (ordered by mtime). Missing dir or fewer than *keep* runs is a
    no-op. Best-effort: a dir that vanishes or won't delete is skipped."""
    try:
        runs = [p for p in workdir.iterdir()
                if p.is_dir() and p.name.startswith("run_")]
    except FileNotFoundError:
        return
    if len(runs) <= keep:
        return
    runs.sort(key=lambda p: p.stat().st_mtime)
    for stale in runs[:-keep]:
        shutil.rmtree(stale, ignore_errors=True)


@dataclass
class SandboxRunResult:
    ok: bool
    output: str = ""                 # bounded stdout (model-visible)
    stderr: str = ""
    returned: Any = None             # structured return from the script
    error: str | None = None
    timed_out: bool = False
    exit_code: int | None = None
    workdir: str = ""
    artifacts: list[str] = field(default_factory=list)
    calls: int = 0                   # number of governed SDK calls the run made

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok, "output": self.output, "stderr": self.stderr,
            "returned": self.returned, "error": self.error,
            "timed_out": self.timed_out, "exit_code": self.exit_code,
            "workdir": self.workdir, "artifacts": self.artifacts, "calls": self.calls,
        }


class SandboxRuntime:
    def __init__(self, policy: SandboxPolicy, execute_fn, *, registry: Any = None,
                 result_store: Any = None):
        self.policy = policy
        self._execute = execute_fn       # governed dispatcher.execute
        self._registry = registry
        self._result_store = result_store

    def bind_result_store(self, store: Any) -> None:
        """Late-bind the session's spilled-result store (for ``sdk.fetch_result``)."""
        self._result_store = store

    def close(self) -> None:
        """No-op for the subprocess runtime (each run is its own short-lived process)."""
        return

    def _truncate(self, text: str) -> str:
        cap = self.policy.max_output_chars
        if text is None:
            return ""
        if len(text) <= cap:
            return text
        return text[:cap] + f"\n…[truncated {len(text) - cap} chars; full output on disk]"

    async def run_script(self, code: str) -> SandboxRunResult:
        run_id = uuid.uuid4().hex[:10]
        workdir = Path(self.policy.workdir)
        run_dir = workdir / f"run_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)
        # Bound on-disk artifact growth: keep the newest MAX_SANDBOX_RUNS runs
        # (the just-created one is freshest, so it always survives the sweep).
        _sweep_run_dirs(workdir)
        script_path = run_dir / "script.py"
        script_path.write_text(code, encoding="utf-8")

        sdk = ConnectorSDK(self._execute, registry=self._registry,
                           result_store=self._result_store, workdir=str(run_dir))
        call_count = {"n": 0}

        # The governed boundary (loopback, no token — never host-exposed). Every
        # `run` is classified + policy-checked by the parent; reads/fetches too.
        server, port = await start_bridge(
            sdk, host="127.0.0.1", token=None,
            on_run=lambda: call_count.__setitem__("n", call_count["n"] + 1),
        )
        self.policy.bridge_port = port

        env = dict(os.environ)
        env.update(self.policy.to_env())
        env["DACLI_SANDBOX_MAX_MEM_MB"] = str(self.policy.max_memory_mb)
        # Make the package importable in the child (so `dacli.sandbox.policy`
        # resolves) by putting the `src/` root on PYTHONPATH.
        project_root = str(Path(__file__).resolve().parents[2])
        env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")

        preexec = self._preexec_fn() if os.name == "posix" else None
        timed_out = False
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "dacli.sandbox._worker",
            "--port", str(port), "--script", str(script_path), "--workdir", str(run_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=project_root,
            env=env,
            **({"preexec_fn": preexec} if preexec else {}),
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=self.policy.wall_clock_seconds)
        except asyncio.TimeoutError:
            timed_out = True
            with contextlib.suppress(Exception):
                proc.kill()
            stdout_b, stderr_b = b"", b"sandbox run exceeded wall-clock limit"
        finally:
            server.close()
            await server.wait_closed()

        stdout = stdout_b.decode("utf-8", "replace") if stdout_b else ""
        stderr = stderr_b.decode("utf-8", "replace") if stderr_b else ""

        returned, err, ok = None, None, proc.returncode == 0 and not timed_out
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

    def _preexec_fn(self):
        # POSIX: cap address space in the child before exec. Best-effort.
        mem_mb = self.policy.max_memory_mb

        def _limit():
            try:
                import resource
                soft = max(64, int(mem_mb)) * 1024 * 1024
                resource.setrlimit(resource.RLIMIT_AS, (soft, soft))
            except Exception:
                pass  # silent-swallow-ok: runs post-fork pre-exec; acquiring a log lock here can deadlock

        return _limit
