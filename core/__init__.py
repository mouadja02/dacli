"""
DACLI: Data Engineer Agent CLI Tool
=======================================
Author: mouadja02
Version: 0.1.0
"""

__version__ = "0.1.0"
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
        from core.agent import DACLI
        return DACLI
    if name == "AgentMemory":
        from core.memory import AgentMemory
        return AgentMemory
    if name == "Settings":
        from config.settings import Settings
        return Settings
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
