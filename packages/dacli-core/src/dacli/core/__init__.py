"""
DACLI: Data Engineer Agent CLI Tool
=======================================
Author: mouadja02

``dacli`` is a PEP 420 namespace split across four wheels (M13); each wheel
single-sources its own version. This is the dacli-core literal, read at build
time by setuptools (``[tool.setuptools.dynamic]``).
"""

__version__ = "0.3.0"

__author__ = "mouadja02"

__all__ = [
    "AgentMemory",
    "DacliHost",
    "Settings",
    "__version__",
]


# Lazy attribute access (PEP 562). ``core.host`` pulls in the connectors, which
# import ``core.verify``. Eagerly importing the host here would make
# ``import core.verify`` re-enter this module before the host is defined — a
# circular import. Exposing the public names lazily keeps ``from core import
# DacliHost`` working while letting submodules import ``core.*`` freely.
def __getattr__(name):
    if name == "DacliHost":
        from dacli.core.host import DacliHost
        return DacliHost
    if name == "AgentMemory":
        from dacli.core.memory import AgentMemory
        return AgentMemory
    if name == "Settings":
        from dacli.config.settings import Settings
        return Settings
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
