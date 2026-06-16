"""Transports that carry bytes to/from a live shell process.

A :class:`Transport` is the I/O substrate under a
:class:`~sandbox.shells.base.ShellBackend`. Three implementations, picked by
:func:`make_transport` in order of fidelity:

* :class:`WinptyTransport` — ``pywinpty`` (faithful TTY on Windows; colour, ``cls``)
* :class:`PosixPtyTransport` — ``ptyprocess`` (faithful TTY on POSIX/WSL)
* :class:`PipeTransport` — pure-stdlib ``subprocess`` with line-buffered pipes
  (the degraded-but-functional fallback when no PTY library is installed)

The session never imports a PTY library directly; it asks for a transport and
gets whatever is available, so the agent boots on a bare Python with no extras.
"""

from __future__ import annotations

import os
import queue
import subprocess
import threading
from typing import Protocol, runtime_checkable
import contextlib

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)


@runtime_checkable
class Transport(Protocol):
    """Minimal byte pipe to a shell subprocess."""

    kind: str

    def start(self, argv: list[str], *, cwd: str | None = None,
              env: dict | None = None) -> None: ...

    def write(self, data: str) -> None: ...

    def read_available(self, timeout: float) -> str:
        """Return whatever output is available within ``timeout`` seconds ("" if none)."""
        ...

    def is_alive(self) -> bool: ...

    def send_interrupt(self) -> None: ...

    def close(self) -> None: ...


class PipeTransport:
    """Stdlib fallback: a subprocess with a background stdout/stderr reader.

    No PTY — so fancy TTY programs that demand a terminal may behave in their
    "dumb"/non-interactive mode — but spawning, writing a command, capturing
    merged output + the sentinel line, and closing all work everywhere Python
    runs. A daemon thread pumps decoded output into a queue so reads never block
    the event loop beyond ``timeout``.
    """

    kind = "pipe"

    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None
        self._q: queue.Queue[str] = queue.Queue()
        self._reader: threading.Thread | None = None

    def start(self, argv: list[str], *, cwd: str | None = None,
              env: dict | None = None) -> None:
        self._proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,   # merge so ordering matches a real terminal
            bufsize=1,
            universal_newlines=True,
            encoding="utf-8",
            errors="replace",
        )

        def _pump(stream):
            try:
                for line in iter(stream.readline, ""):
                    self._q.put(line)
            except Exception:
                log.debug("subprocess stdout pump stopped on error", exc_info=True)
            finally:
                with contextlib.suppress(Exception):
                    stream.close()

        self._reader = threading.Thread(target=_pump, args=(self._proc.stdout,), daemon=True)
        self._reader.start()

    def write(self, data: str) -> None:
        if self._proc and self._proc.stdin:
            self._proc.stdin.write(data)
            self._proc.stdin.flush()

    def read_available(self, timeout: float) -> str:
        chunks: list[str] = []
        try:
            chunks.append(self._q.get(timeout=timeout))
        except queue.Empty:
            return ""
        # Drain anything else already buffered without blocking further.
        while True:
            try:
                chunks.append(self._q.get_nowait())
            except queue.Empty:
                break
        return "".join(chunks)

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def send_interrupt(self) -> None:
        if not self._proc:
            return
        try:
            if os.name == "nt":
                self._proc.send_signal(subprocess.signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
            else:
                import signal
                self._proc.send_signal(signal.SIGINT)
        except Exception:
            log.debug("send_interrupt to subprocess failed", exc_info=True)

    def close(self) -> None:
        if not self._proc:
            return
        try:
            if self._proc.stdin:
                with contextlib.suppress(Exception):
                    self._proc.stdin.close()
            self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except Exception:
                self._proc.kill()
        except Exception:
            log.debug("subprocess close/terminate failed", exc_info=True)


class WinptyTransport:
    """pywinpty-backed transport (faithful TTY on Windows)."""

    kind = "winpty"

    def __init__(self) -> None:
        self._pty = None

    def start(self, argv: list[str], *, cwd: str | None = None,
              env: dict | None = None) -> None:
        from winpty import PtyProcess  # type: ignore

        self._pty = PtyProcess.spawn(argv, cwd=cwd, env=env)

    def write(self, data: str) -> None:
        if self._pty is not None:
            self._pty.write(data)

    def read_available(self, timeout: float) -> str:
        if self._pty is None:
            return ""
        try:
            # pywinpty read is blocking; a tiny size with the library's own
            # timeout semantics keeps it responsive. Best-effort.
            return self._pty.read(8192)
        except EOFError:
            return ""
        except Exception:
            return ""

    def is_alive(self) -> bool:
        return self._pty is not None and self._pty.isalive()

    def send_interrupt(self) -> None:
        if self._pty is not None:
            with contextlib.suppress(Exception):
                self._pty.write("\x03")

    def close(self) -> None:
        if self._pty is not None:
            with contextlib.suppress(Exception):
                self._pty.terminate(force=True)


class PosixPtyTransport:
    """ptyprocess-backed transport (faithful TTY on POSIX / WSL)."""

    kind = "ptyprocess"

    def __init__(self) -> None:
        self._pty = None

    def start(self, argv: list[str], *, cwd: str | None = None,
              env: dict | None = None) -> None:
        from ptyprocess import PtyProcess  # type: ignore

        self._pty = PtyProcess.spawn(argv, cwd=cwd, env=env)

    def write(self, data: str) -> None:
        if self._pty is not None:
            self._pty.write(data.encode("utf-8"))

    def read_available(self, timeout: float) -> str:
        if self._pty is None:
            return ""
        import select

        try:
            fd = self._pty.fd
            r, _, _ = select.select([fd], [], [], timeout)
            if not r:
                return ""
            return self._pty.read(8192).decode("utf-8", errors="replace")
        except EOFError:
            return ""
        except Exception:
            return ""

    def is_alive(self) -> bool:
        return self._pty is not None and self._pty.isalive()

    def send_interrupt(self) -> None:
        if self._pty is not None:
            with contextlib.suppress(Exception):
                self._pty.write(b"\x03")

    def close(self) -> None:
        if self._pty is not None:
            with contextlib.suppress(Exception):
                self._pty.terminate(force=True)


def _pty_available() -> str | None:
    if os.name == "nt":
        try:
            import winpty  # noqa: F401
            return "winpty"
        except Exception:
            return None
    try:
        import ptyprocess  # noqa: F401
        return "ptyprocess"
    except Exception:
        return None


def make_transport(prefer_pty: bool = True) -> Transport:
    """Return the highest-fidelity transport available on this host."""
    if prefer_pty:
        which = _pty_available()
        if which == "winpty":
            return WinptyTransport()
        if which == "ptyprocess":
            return PosixPtyTransport()
    return PipeTransport()
