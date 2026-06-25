"""Sandbox policy — what a code-execution run may touch.

A :class:`SandboxPolicy` bounds a sandbox run: where it works, how long it may
run, how much memory it may use, how much output may flow back to model context,
and what network egress is permitted. The defaults are conservative — network is
restricted to the local governance bridge, so model-written code can reach a
platform **only** through the capability-gated SDK (which is governed and holds
the credentials), never directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)


@dataclass
class SandboxPolicy:
    workdir: str = ".dacli/sandbox/"
    wall_clock_seconds: int = 300
    max_memory_mb: int = 1024
    max_output_chars: int = 20000
    network: str = "allowlist"          # "off" | "allowlist" | "open"
    egress_allowlist: list[str] = field(default_factory=list)
    # The loopback port of the governance bridge the worker may always reach
    # (set per-run by the runtime). Everything else is subject to ``network``.
    bridge_port: int | None = None

    @classmethod
    def from_settings(cls, settings) -> SandboxPolicy:
        sb = getattr(settings, "sandbox", None)
        if sb is None:
            return cls()
        return cls(
            workdir=sb.workdir,
            wall_clock_seconds=sb.wall_clock_seconds,
            max_memory_mb=sb.max_memory_mb,
            max_output_chars=sb.max_output_chars,
            network=sb.network,
            egress_allowlist=list(sb.egress_allowlist or []),
        )

    def to_env(self) -> dict:
        """Serialize the egress policy into env vars the worker reads on startup."""
        return {
            "DACLI_SANDBOX_NETWORK": self.network,
            "DACLI_SANDBOX_ALLOWLIST": ",".join(self.egress_allowlist),
            "DACLI_SANDBOX_BRIDGE_PORT": str(self.bridge_port or 0),
        }


def install_egress_guard(network: str, allowlist: list[str], bridge_port: int) -> None:
    """Monkeypatch ``socket`` so the worker honors the egress policy.

    Called *inside the worker process* before user code runs. For ``off`` only
    the loopback bridge is reachable; for ``allowlist`` loopback + the listed
    host suffixes are reachable; ``open`` installs nothing. Connections that
    violate the policy raise ``PermissionError`` rather than silently failing, so
    a blocked egress attempt is visible in the run's stderr.
    """
    if network == "open":
        return

    import socket

    allow = [a.strip().lower() for a in allowlist if a.strip()]
    _orig_connect = socket.socket.connect
    _orig_getaddrinfo = socket.getaddrinfo

    def _is_loopback(host: str) -> bool:
        return host in ("127.0.0.1", "::1", "localhost")

    def _allowed_host(host: str) -> bool:
        host = (host or "").lower()
        if _is_loopback(host):
            return True
        if network == "off":
            return False
        return any(host == a or host.endswith("." + a) or host.endswith(a) for a in allow)

    def guarded_connect(self, address):  # type: ignore[no-redef]
        try:
            host = address[0]
            port = address[1] if len(address) > 1 else None
        except Exception:
            raise PermissionError("sandbox egress blocked: malformed address") from None
        # The governance bridge on loopback is always permitted.
        if _is_loopback(str(host)) and (bridge_port == 0 or port == bridge_port or network != "off"):
            return _orig_connect(self, address)
        if _allowed_host(str(host)):
            return _orig_connect(self, address)
        raise PermissionError(
            f"sandbox egress blocked: connection to {host}:{port} violates "
            f"network policy '{network}'"
        )

    def guarded_getaddrinfo(host, *args, **kwargs):  # type: ignore[no-redef]
        if _allowed_host(str(host)):
            return _orig_getaddrinfo(host, *args, **kwargs)
        raise PermissionError(
            f"sandbox egress blocked: DNS lookup for {host} violates policy '{network}'")

    socket.socket.connect = guarded_connect  # type: ignore[assignment]
    socket.getaddrinfo = guarded_getaddrinfo  # type: ignore[assignment]


def apply_resource_limits(max_memory_mb: int) -> None:
    """Apply POSIX rlimits inside the worker (no-op / advisory on Windows)."""
    try:
        import resource  # POSIX only
    except Exception:
        return
    try:
        soft_bytes = max(64, int(max_memory_mb)) * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (soft_bytes, soft_bytes))
    except Exception:
        log.debug("setrlimit(RLIMIT_AS) failed; running without a memory cap", exc_info=True)
