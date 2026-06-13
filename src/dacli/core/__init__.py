"""
DACLI: Data Engineer Agent CLI Tool
=======================================
Author: mouadja02

The version is single-sourced from the top-level package (``dacli.__version__``,
read at build time by setuptools); re-exported here so ``from dacli.core import
__version__`` keeps working without a second literal to drift.
"""

from dacli import __version__

__author__ = "mouadja02"

__all__ = [
    "DACLI",
    "AgentMemory",
    "Settings",
    "__version__",
]


# Lazy attribute access (PEP 562). ``core.agent`` pulls in the connectors, which
# now import ``core.verify``. Eagerly importing the agent here would
# make ``import core.verify`` re-enter this module before the agent is defined —
# a circular import. Exposing the public names lazily keeps ``from core import
# DACLI`` working while letting submodules import ``core.*`` freely.
def __getattr__(name):
    if name == "DACLI":
        from dacli.core.agent import DACLI
        return DACLI
    if name == "AgentMemory":
        from dacli.core.memory import AgentMemory
        return AgentMemory
    if name == "Settings":
        from dacli.config.settings import Settings
        return Settings
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
