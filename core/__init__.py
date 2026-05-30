"""
DACLI: Data Engineer Agent CLI Tool
=======================================
Author: mouadja02
Version: 0.1.0
"""

__version__ = "0.1.0"
__author__ = "mouadja02"

from core.agent import DACLI
from core.memory import AgentMemory
from config.settings import Settings

__all__ = [
    "DACLI",
    "AgentMemory",
    "Settings",
    "__version__",
]
