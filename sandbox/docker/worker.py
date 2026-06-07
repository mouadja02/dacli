"""Standalone sandbox worker for the Docker runtime.

This file is baked **read-only** into the sandbox image at ``/opt/dacli/worker.py``
and run inside the per-session container by ``docker exec``. It deliberately has
**no dependency on the dacli project** (stdlib only) — the container never mounts
the host code; only the per-run ``/workspace/run_<id>`` data dir is shared.

It mirrors ``sandbox/_worker.py`` but:

* connects out to the governed bridge on the **host** (``host.docker.internal``)
  rather than loopback, and authenticates with the per-session token, and
* its ``sdk`` exposes the same surface (``run`` / ``tools`` / ``save_rows`` /
  ``read_rows`` / ``fetch_result`` / ``fetch_rows`` / ``finish``).

Invoked as::  python /opt/dacli/worker.py --script S --workdir W
with bridge coordinates + egress policy supplied via environment variables.
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
    """Container-side proxy to the governed parent SDK (marshalled over a socket)."""

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

    def hello(self, token: str) -> None:
        resp = self._rpc({"op": "hello", "token": token})
        if not resp.get("ok"):
            raise RuntimeError(f"sandbox bridge rejected handshake: {resp.get('error')}")

    def run(self, tool_name: str, **args) -> dict:
        return self._rpc({"op": "run", "tool": tool_name, "args": args})

    def available_tools(self) -> list:
        return self._rpc({"op": "tools"}).get("tools", [])

    def fetch_result(self, handle: str, start: int = 0, count=None) -> list:
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


def _install_egress_guard(network: str, allowlist, bridge_host: str) -> None:
    """Block outbound connections that violate the egress policy.

    The bridge host is always reachable. ``off`` permits nothing else; ``allowlist``
    permits the listed host suffixes; ``open`` installs nothing (so e.g. pip can
    reach PyPI). A violation raises ``PermissionError`` so it is visible in stderr.
    """
    if network == "open":
        return
    allow = [a.strip().lower() for a in (allowlist or []) if a and a.strip()]
    bridge = (bridge_host or "").lower()
    _orig_connect = socket.socket.connect
    _orig_getaddrinfo = socket.getaddrinfo

    def _ok(host: str) -> bool:
        host = (host or "").lower()
        if host in ("127.0.0.1", "::1", "localhost") or host == bridge:
            return True
        if network == "off":
            return False
        return any(host == a or host.endswith("." + a) or host.endswith(a) for a in allow)

    def guarded_connect(self, address):
        try:
            host = address[0]
            port = address[1] if len(address) > 1 else None
        except Exception:
            raise PermissionError("sandbox egress blocked: malformed address") from None
        if _ok(str(host)):
            return _orig_connect(self, address)
        raise PermissionError(
            f"sandbox egress blocked: connection to {host}:{port} violates network policy '{network}'")

    def guarded_getaddrinfo(host, *args, **kwargs):
        if _ok(str(host)):
            return _orig_getaddrinfo(host, *args, **kwargs)
        raise PermissionError(
            f"sandbox egress blocked: DNS lookup for {host} violates policy '{network}'")

    socket.socket.connect = guarded_connect  # type: ignore[assignment]
    socket.getaddrinfo = guarded_getaddrinfo  # type: ignore[assignment]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--script", required=True)
    parser.add_argument("--workdir", required=True)
    ns = parser.parse_args()

    workdir = Path(ns.workdir)
    result_path = workdir / "result.json"

    host = os.environ.get("DACLI_SANDBOX_BRIDGE_HOST", "host.docker.internal")
    port = int(os.environ.get("DACLI_SANDBOX_BRIDGE_PORT", "0") or 0)
    token = os.environ.get("DACLI_SANDBOX_BRIDGE_TOKEN", "")
    network = os.environ.get("DACLI_SANDBOX_NETWORK", "open")
    allowlist = [a for a in os.environ.get("DACLI_SANDBOX_ALLOWLIST", "").split(",") if a]

    # Connect to the parent's governed bridge BEFORE locking egress down.
    sock = socket.create_connection((host, port))
    _install_egress_guard(network, allowlist, host)

    sdk = _BridgeSDK(sock, workdir)
    sdk.hello(token)

    code = Path(ns.script).read_text(encoding="utf-8")
    namespace = {"sdk": sdk, "__name__": "__sandbox__"}
    ok, error = True, None
    try:
        exec(compile(code, ns.script, "exec"), namespace)
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
