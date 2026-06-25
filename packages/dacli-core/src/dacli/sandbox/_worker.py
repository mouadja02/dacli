"""Sandbox worker — the isolated child process.

Runs the agent-written script in a separate process with:

* an **egress guard** (model code can't open arbitrary network connections —
  platform access goes only through the governed SDK bridge),
* **resource limits** (memory rlimit on POSIX),
* a **secret-free SDK** whose ``run`` calls are marshalled over a localhost
  socket to the parent, which executes them through the Governor.

The script's textual output goes to stdout/stderr (captured + truncated by the
parent). A final ``result.json`` records the structured return value / error.

Invoked as::  python -m dacli.sandbox._worker --port P --script S --workdir W
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import traceback
from pathlib import Path
import contextlib


class _BridgeSDK:
    """Worker-side proxy: same surface as ConnectorSDK, marshalled over a socket.

    ``run`` round-trips a request to the parent (which applies governance);
    ``save_rows`` / ``read_rows`` use the shared workspace directly (off-context
    by construction — large data never crosses the bridge or enters context).
    """

    def __init__(self, sock: socket.socket, workdir: Path):
        self._sock = sock
        self._rfile = sock.makefile("r", encoding="utf-8")
        self._wfile = sock.makefile("w", encoding="utf-8")
        self.workdir = workdir
        self._returned = None

    def _rpc(self, request: dict) -> dict:
        self._wfile.write(json.dumps(request) + "\n")
        self._wfile.flush()
        line = self._rfile.readline()
        if not line:
            raise RuntimeError("sandbox bridge closed unexpectedly")
        return json.loads(line)

    def run(self, tool_name: str, **args) -> dict:
        return self._rpc({"op": "run", "tool": tool_name, "args": args})

    def available_tools(self) -> list:
        return self._rpc({"op": "tools"}).get("tools", [])

    def fetch_result(self, handle: str, start: int = 0, count=None) -> list:
        """Load rows from a spilled tool result (a ``res_*`` handle) into code.

        Returns the full requested window of rows. Raises ``RuntimeError`` on an
        unknown handle / error so a failed load is never a silent empty list.
        """
        resp = self._rpc({"op": "fetch_result", "handle": handle, "start": start, "count": count})
        if isinstance(resp, dict) and resp.get("error"):
            raise RuntimeError(f"fetch_result({handle!r}) failed: {resp['error']}")
        rows = resp.get("rows") if isinstance(resp, dict) else None
        return rows if rows is not None else []

    def fetch_rows(self, handle: str, start: int = 0, count=None) -> list:
        return self.fetch_result(handle, start=start, count=count)

    def save_rows(self, name: str, rows, fmt: str = "jsonl") -> str:
        path = self.workdir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")
        return str(path)

    def read_rows(self, name: str, limit=None) -> list:
        path = self.workdir / name
        out = []
        if not path.exists():
            return out
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f):
                if limit is not None and i >= limit:
                    break
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out

    def finish(self, value) -> None:
        self._returned = value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--script", required=True)
    parser.add_argument("--workdir", required=True)
    ns = parser.parse_args()

    workdir = Path(ns.workdir)
    result_path = workdir / "result.json"

    # Apply the egress policy + resource limits BEFORE user code runs.
    from dacli.sandbox.policy import install_egress_guard, apply_resource_limits

    network = os.environ.get("DACLI_SANDBOX_NETWORK", "off")
    allowlist = [a for a in os.environ.get("DACLI_SANDBOX_ALLOWLIST", "").split(",") if a]
    bridge_port = int(os.environ.get("DACLI_SANDBOX_BRIDGE_PORT", "0") or 0)
    with contextlib.suppress(Exception):
        apply_resource_limits(int(os.environ.get("DACLI_SANDBOX_MAX_MEM_MB", "1024")))

    # Connect to the parent bridge first (loopback) — then lock down egress.
    sock = socket.create_connection(("127.0.0.1", ns.port))
    install_egress_guard(network, allowlist, bridge_port)

    sdk = _BridgeSDK(sock, workdir)
    code = Path(ns.script).read_text(encoding="utf-8")

    namespace = {"sdk": sdk, "__name__": "__sandbox__"}
    ok, error = True, None
    try:
        exec(compile(code, ns.script, "exec"), namespace)
        # A script may set a top-level RESULT instead of calling sdk.finish().
        if sdk._returned is None and "RESULT" in namespace:
            sdk.finish(namespace["RESULT"])
    except Exception:
        ok = False
        error = traceback.format_exc()
        print(error, file=sys.stderr)

    with contextlib.suppress(Exception):
        result_path.write_text(
            json.dumps({"ok": ok, "error": error, "returned": sdk._returned}, default=str),
            encoding="utf-8",
        )
    with contextlib.suppress(Exception):
        sock.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
