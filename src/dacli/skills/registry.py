"""Skill registry — discovers skills and enforces the post-condition rule.

A skill lives in ``skills/<name>/`` with a ``SKILL.md`` carrying YAML front
matter (id, name, description, ``class`` import path, ``enabled``). The registry:

* discovers every ``skills/*/SKILL.md``,
* imports + instantiates the skill class,
* **rejects any skill whose ``SkillSpec`` declares no post-condition** (the
  mandatory rule — enforced at load time, exactly like the connector registry),
* exposes a progressive-disclosure digest (id/name/description) and full tool
  definitions for the enabled skills.

This mirrors the connector registry so "add a skill" is "drop a folder", not
"edit the agent".
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import yaml

from dacli.core.verify import require_postconditions
from dacli.skills.spec import Skill


def _parse_front_matter(text: str) -> dict[str, Any]:
    """Read the YAML front matter block delimited by leading/trailing ``---``."""
    stripped = text.lstrip()
    if not stripped.startswith("---"):
        return {}
    body = stripped[3:]
    end = body.find("\n---")
    if end == -1:
        return {}
    try:
        return yaml.safe_load(body[:end]) or {}
    except Exception:
        return {}


class SkillRegistry:
    """Discovers contracted skills and resolves skill names to instances."""

    def __init__(
        self,
        skills_dir: str | None = None,
        *,
        extra_skills: list[Skill] | None = None,
    ):
        self._skills_dir = Path(skills_dir) if skills_dir else Path(__file__).parent
        self._skills: dict[str, Skill] = {}
        self._manifests: dict[str, dict[str, Any]] = {}
        self._enabled: dict[str, bool] = {}

        self._discover()
        for skill in extra_skills or []:
            self._register(skill, {"id": skill.spec.name}, enabled=True)

    # ------------------------------------------------------------------
    def _discover(self) -> None:
        for skill_md in sorted(self._skills_dir.glob("*/SKILL.md")):
            try:
                meta = _parse_front_matter(skill_md.read_text(encoding="utf-8"))
            except Exception:
                continue
            class_path = meta.get("class")
            if not class_path:
                continue
            try:
                skill = self._instantiate(class_path)
            except Exception as e:  # a broken skill must not take down discovery
                raise SkillLoadError(f"Failed to load skill at {skill_md}: {e}") from e
            self._register(skill, meta, enabled=bool(meta.get("enabled", True)))

    def _instantiate(self, class_path: str) -> Skill:
        module_path, _, class_name = class_path.rpartition(".")
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        return cls()

    def _register(self, skill: Skill, meta: dict[str, Any], *, enabled: bool) -> None:
        spec = skill.spec
        # MANDATORY: no post-condition, no registration.
        require_postconditions(f"skill:{spec.name}", spec.postconditions)
        self._skills[spec.name] = skill
        self._manifests[spec.name] = {
            "id": spec.name,
            "name": meta.get("name", spec.name),
            "description": meta.get("description", spec.description),
            "category": spec.category,
        }
        self._enabled[spec.name] = enabled

    # ------------------------------------------------------------------
    def is_enabled(self, name: str) -> bool:
        return bool(self._enabled.get(name, False))

    def get(self, name: str) -> Skill | None:
        # Tolerate the underscore form used in tool names (diagram_mermaid).
        if name in self._skills:
            return self._skills[name]
        hyphen = name.replace("_", "-")
        return self._skills.get(hyphen)

    def all(self) -> list[Skill]:
        return list(self._skills.values())

    def names(self) -> list[str]:
        return list(self._skills.keys())

    def digest(self) -> list[dict[str, Any]]:
        """Progressive-disclosure surface: name + one-liner per enabled skill."""
        out = []
        for name, manifest in self._manifests.items():
            if not self.is_enabled(name):
                continue
            out.append({
                "id": manifest["id"],
                "name": manifest["name"],
                "description": manifest["description"],
                "category": manifest.get("category", ""),
            })
        return out

    def get_tool_definitions(self) -> list[dict[str, Any]]:
        return [
            s.spec.to_tool_definition()
            for name, s in self._skills.items()
            if self.is_enabled(name)
        ]

    def get_spec(self, name: str):
        skill = self.get(name)
        return skill.spec if skill else None


class SkillLoadError(RuntimeError):
    """Raised when a discovered skill cannot be imported/instantiated."""
