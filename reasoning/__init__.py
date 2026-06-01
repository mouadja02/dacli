# Reasoning module for DACLI: the provider-agnostic LLM client (ℛ) and the
# model-tiering router that picks cheap vs. strong per call.

from reasoning.llm import LLMClient
from reasoning.model_router import (
    ModelRouter, ModelTier, Stakes, ModelChoice, ModelRoutingAuditLog,
)

__all__ = [
    "LLMClient",
    "ModelRouter",
    "ModelTier",
    "Stakes",
    "ModelChoice",
    "ModelRoutingAuditLog",
]
