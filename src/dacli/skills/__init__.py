"""Skills (𝒮) — named, contracted procedures.

A *skill* is a named procedure with a contract: a :class:`~skills.spec.SkillSpec`
declaring scope (``can_do`` / ``cannot_do``), input/output schemas, and — most
importantly — **mandatory post-conditions**. Skills compose connectors and carry
their own checks; the registry refuses to load one that declares none.
"""

from dacli.skills.spec import Skill, SkillSpec, SkillContext
from dacli.skills.registry import SkillRegistry

__all__ = ["Skill", "SkillContext", "SkillRegistry", "SkillSpec"]
