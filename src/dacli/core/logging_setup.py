"""Central logging setup for dacli (P06 — observability).

One place configures the stdlib :mod:`logging` tree so that the many
*best-effort* swallows scattered across the codebase can leave a breadcrumb
instead of vanishing. A lazy :class:`~logging.handlers.RotatingFileHandler` writes
to ``<state_dir>/dacli.log`` (5 files × ~1 MB) — the dir/file are created on the
**first emitted record**, not at setup, so a clean WARNING-level run (e.g.
``dacli --version``) litters no state. The log dir is resolved through P01's
:func:`dacli.core.paths.state_dir` (project-local when in a project, else the
per-user global dir). The default level is **WARNING**; the ``--debug`` CLI flag
(or ``DACLI_DEBUG=1``) flips the whole tree to **DEBUG** so the swallow
breadcrumbs (logged at ``debug``) actually reach the file.

Usage:

    # once, at CLI / headless startup
    from dacli.core.logging_setup import setup_logging
    setup_logging(debug=args.debug)  # base_dir defaults to paths.state_dir()

    # everywhere else
    from dacli.core.logging_setup import get_logger
    log = get_logger(__name__)
    log.debug("usage persist failed", exc_info=True)  # swallow but record

SECURITY: **never log raw credential values.** dacli.log is a plaintext file on
disk (unlike the Fernet-encrypted ``secrets`` block in dacli.json). Log
credential *names* or redacted markers only — see ``core.store._redact`` and
``core.crypto.surface_decryption_failures`` for the redaction posture.
"""

from __future__ import annotations

import contextlib
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

#: Root of the dacli logger tree. ``get_logger("core.store")`` returns the child
#: ``dacli.core.store`` which inherits this logger's level and handler.
_ROOT_NAME = "dacli"

# Process-wide debug state, set by the last setup_logging() call. Read by the
# kernel (to decide whether to re-raise unexpected exceptions) via is_debug().
_DEBUG = False
_configured = False

_TRUTHY = {"1", "true", "yes", "on"}


class _LazyRotatingFileHandler(RotatingFileHandler):
    """Rotating handler that defers creating the dir/file to the first record.

    ``delay=True`` already defers opening the file; we extend that to ``mkdir``
    the parent then too, so nothing is written until a record at the active
    level is actually emitted. A read-only state dir degrades silently (the
    eager path fell back to a NullHandler for the same reason) instead of
    spamming stderr through the default error handler.
    """

    def _open(self):
        Path(self.baseFilename).parent.mkdir(parents=True, exist_ok=True)
        return super()._open()

    def handleError(self, record):
        pass


def _debug_requested(debug: bool | None) -> bool:
    """Resolve the debug switch: explicit arg wins, else ``DACLI_DEBUG`` env."""
    if debug is not None:
        return bool(debug)
    return os.environ.get("DACLI_DEBUG", "").strip().lower() in _TRUTHY


def setup_logging(
    debug: bool | None = None,
    base_dir: str | None = None,
    *,
    force: bool = False,
) -> logging.Logger:
    """Configure the ``dacli`` logger tree once. Returns the root dacli logger.

    ``debug=None`` defers to the ``DACLI_DEBUG`` environment variable.
    ``base_dir=None`` resolves the log dir through :func:`paths.state_dir`.
    Repeated calls are idempotent (the level is refreshed but the handler is not
    duplicated) unless ``force=True`` re-installs the handler — handy in tests.
    """
    global _DEBUG, _configured

    if base_dir is None:
        from dacli.core import paths

        base_dir = str(paths.state_dir())

    debug = _debug_requested(debug)
    _DEBUG = debug
    level = logging.DEBUG if debug else logging.WARNING

    logger = logging.getLogger(_ROOT_NAME)
    logger.setLevel(level)

    if _configured and not force:
        # Already wired this process: just refresh the level on logger+handlers.
        for h in logger.handlers:
            h.setLevel(level)
        return logger

    # Tear down any handlers we previously installed so force/re-init doesn't
    # stack duplicate file handlers (which would double every line).
    for h in list(logger.handlers):
        logger.removeHandler(h)
        # Intentionally silent: this is the logging bootstrap itself, mid
        # handler-swap — there is no sound sink to record into here (the very
        # handlers we'd log through are being torn down). Not an app-level
        # swallow; the P06 "swallow-and-record" rule doesn't apply.
        with contextlib.suppress(Exception):
            h.close()

    # Lazy: the dir/file aren't touched until the first record emits, so a clean
    # WARNING-level run creates no state. base_dir resolved via paths.state_dir().
    handler: logging.Handler = _LazyRotatingFileHandler(
        str(Path(base_dir) / "dacli.log"),
        maxBytes=1_000_000,  # ~1 MB per file
        backupCount=4,  # + the active file = 5 files total
        encoding="utf-8",
        delay=True,
    )

    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    )
    logger.addHandler(handler)
    # Leave propagate=True so pytest's caplog (which captures via the root
    # logger) sees our records; in production the root logger has no handlers,
    # so nothing leaks to stderr.
    _configured = True
    return logger


def get_logger(name: str) -> logging.Logger:
    """Return a child of the ``dacli`` logger for ``name`` (usually ``__name__``).

    Children inherit the root dacli logger's level and handler, so a single
    :func:`setup_logging` call governs the whole tree.
    """
    if name == _ROOT_NAME or name.startswith(_ROOT_NAME + "."):
        return logging.getLogger(name)
    return logging.getLogger(f"{_ROOT_NAME}.{name}")


def is_debug() -> bool:
    """Whether the last :func:`setup_logging` enabled debug mode."""
    return _DEBUG
