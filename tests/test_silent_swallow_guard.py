"""Roadmap 07 — guard against silent exception swallows.

The project policy is swallow-*and-record*: a fail-soft ``except`` may keep the
program running, but it must leave a ``log.debug(..., exc_info=True)`` breadcrumb
so a swallowed bug after a refactor doesn't vanish at the default WARNING level.

``test_no_unlogged_silent_swallows`` walks the package AST and fails if any
``except`` handler is bodyless (its body is exactly ``pass`` or ``...``) without
the ``silent-swallow-ok`` sentinel. The sentinel marks the handful of handlers
where recording is genuinely impossible or unsafe (logging-the-logger failure,
post-fork pre-exec); adding a new bare ``except: pass`` anywhere else breaks CI.
"""

import ast
from pathlib import Path

import dacli

# dacli is a PEP 420 namespace split across four wheels (M13), so it has no single
# __file__; __path__ lists every installed portion (.../packages/<pkg>/src/dacli).
SRC_ROOTS = [Path(p) for p in dacli.__path__]
SENTINEL = "silent-swallow-ok"


def _is_bodyless(handler: ast.ExceptHandler) -> bool:
    if len(handler.body) != 1:
        return False
    stmt = handler.body[0]
    if isinstance(stmt, ast.Pass):
        return True
    return (
        isinstance(stmt, ast.Expr)
        and isinstance(stmt.value, ast.Constant)
        and stmt.value.value is Ellipsis
    )


def _silent_swallows() -> list[str]:
    offenders = []
    for root in SRC_ROOTS:
        for path in root.rglob("*.py"):
            lines = path.read_text(encoding="utf-8").splitlines()
            tree = ast.parse("\n".join(lines), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ExceptHandler) or not _is_bodyless(node):
                    continue
                body_line = lines[node.body[0].lineno - 1]
                if SENTINEL in body_line:
                    continue
                rel = path.relative_to(root.parent)
                offenders.append(f"{rel}:{node.body[0].lineno}")
    return offenders


def test_no_unlogged_silent_swallows():
    offenders = _silent_swallows()
    assert not offenders, (
        "bodyless except handlers without a log breadcrumb (add log.debug(..., "
        f"exc_info=True), or mark '{SENTINEL}' if recording is impossible):\n  "
        + "\n  ".join(offenders)
    )
