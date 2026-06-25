"""Skills — progressive-disclosure ``SKILL.md`` the agent reads on demand.

A skill is a Markdown file ``<skills>/<name>/SKILL.md`` with YAML front matter
(``name``, ``description``). No executable class, no parallel registry: a skill is
guidance the agent loads when a task calls for it. Skills resolve through the same
``.dacli`` overlay as every other resource — :func:`paths.resource_dir`,
project → global → seed — so a user drops a ``SKILL.md`` to teach a method without
touching the agent.

The system prompt carries only the digest (name + one-line description); the agent
reads the full file when it decides the skill is relevant (Pi-style progressive
disclosure). This is the one skill path — the old executable ``SkillRegistry`` is
gone.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from dacli.core import paths


def _front_matter(text: str) -> dict[str, Any]:
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


def discover_skills(skills_dir: str | Path | None = None) -> list[dict[str, str]]:
    """Return ``[{name, description, path}]`` for every ``*/SKILL.md`` under the
    resolved skills dir, sorted by name. Defaults to ``resource_dir("skills")``."""
    base = Path(skills_dir) if skills_dir else paths.resource_dir("skills")
    if not base.exists():
        return []
    out: list[dict[str, str]] = []
    for skill_md in sorted(base.glob("*/SKILL.md")):
        meta = _front_matter(skill_md.read_text(encoding="utf-8"))
        name = str(meta.get("name") or skill_md.parent.name)
        out.append({
            "name": name,
            "description": str(meta.get("description", "")).strip(),
            "path": str(skill_md),
        })
    return out
