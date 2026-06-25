"""The governed SDK bridge — the one inbound boundary sandbox code crosses.

A newline-delimited JSON server. The isolated worker (a local subprocess **or** a
per-session Docker container) connects and sends requests; the parent answers by
awaiting the *governed* :class:`~sandbox.sdk.ConnectorSDK`, so every ``run`` is
classified + policy-checked + audited exactly like a direct tool call. Code in
the sandbox therefore reaches a platform **only** through this governed channel,
and never sees a credential.

There is exactly one governance boundary for both runtimes (per
"scale 𝒮 with 𝒢" — a second mechanism is a second place for governance to drift).

Protocol (one JSON object per line, response is one JSON object per line)::

    {"op":"hello","token":T}                                  -> {"ok":bool}
    {"op":"run","tool":NAME,"args":{...}}                     -> governed summary
    {"op":"tools"}                                            -> {"tools":[...]}
    {"op":"fetch_result","handle":H,"start":S,"count":C}      -> {"rows":[...]}

When a ``token`` is configured (the Docker runtime, which must bind a
host-reachable address so the container can connect) the connection is
**unauthenticated until** a matching ``hello`` arrives — so only this session's
container, which was handed the per-session secret, can issue governed calls.
The subprocess runtime binds loopback with no token (no host exposure).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from collections.abc import Callable
import contextlib

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)


async def start_bridge(
    sdk: Any,
    *,
    host: str = "127.0.0.1",
    token: str | None = None,
    on_run: Callable[[], None] | None = None,
) -> tuple[asyncio.AbstractServer, int]:
    """Start the governed bridge on ``host`` (ephemeral port). Returns ``(server, port)``.

    ``on_run`` (if given) is called once per governed ``run`` (e.g. to count the
    sub-actions a sandbox run made, for the audit trail).
    """

    async def handle_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        authed = token is None
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    req = json.loads(line.decode("utf-8"))
                except Exception:
                    continue
                op = req.get("op")

                if op == "hello":
                    authed = (token is None) or (req.get("token") == token)
                    resp: Any = {"ok": authed}
                    if not authed:
                        resp["error"] = "bad bridge token"
                elif not authed:
                    resp = {"error": "unauthenticated: send {'op':'hello','token':...} first"}
                elif op == "run":
                    if on_run is not None:
                        on_run()
                    resp = await sdk.run(req.get("tool", ""), req.get("args") or {})
                elif op == "tools":
                    resp = {"tools": sdk.available_tools()}
                elif op == "fetch_result":
                    try:
                        rows = sdk.fetch_result(
                            req.get("handle") or "",
                            start=req.get("start") or 0,
                            count=req.get("count"),
                        )
                        resp = {"rows": rows,
                                "returned": len(rows) if isinstance(rows, list) else None}
                    except Exception as exc:
                        resp = {"error": str(exc)}
                else:
                    resp = {"error": f"unknown bridge op '{op}'"}

                writer.write((json.dumps(resp, default=str) + "\n").encode("utf-8"))
                await writer.drain()
                # A failed auth handshake gets its one reply, then we drop the conn.
                if op == "hello" and not authed:
                    break
        except Exception:
            log.debug("bridge connection handler errored; dropping conn", exc_info=True)
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    server = await asyncio.start_server(handle_conn, host, 0)
    port = server.sockets[0].getsockname()[1]
    return server, port
