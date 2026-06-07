from pathlib import Path
from collections.abc import Iterable

# Default location for the system prompt
SYSTEM_PROMPT_FILE = Path(__file__).parent / "system_message.md"

# Decomposed prompt fragments: an invariant core + per-connector
# fragments disclosed only when that connector is in play.
FRAGMENTS_DIR = Path(__file__).parent / "fragments"
CORE_FRAGMENT = FRAGMENTS_DIR / "core.md"


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def compose_system_prompt(
    task: str = "",
    disclosed_connectors: Iterable[str] | None = None,
) -> str:
    """Assemble the dynamic system prompt.

    Returns the invariant core plus the fragments for the disclosed connectors
    (``prompts/fragments/<connector_id>.md``). It deliberately does **not** load
    ``DACLI.md`` priors — those are layer L1 of the context assembler
    (``context.assembler.build_context``), which pins them, so loading them here
    too would duplicate them. ``task`` is accepted for future task-conditioned
    fragments; unused today.

    Falls back to the legacy monolithic ``system_message.md`` if the core
    fragment is missing.
    """
    core = _read(CORE_FRAGMENT)
    if not core:
        return _read(SYSTEM_PROMPT_FILE)

    parts = [core]
    for connector_id in disclosed_connectors or []:
        fragment = _read(FRAGMENTS_DIR / f"{connector_id}.md")
        if fragment:
            parts.append(fragment)
    return "\n\n".join(parts)

def get_default_system_prompt() -> str:
    """Return the default system prompt (the invariant core fragment)."""
    return compose_system_prompt()

def load_system_prompt(custom_path: str | None = None) -> str:
    # Load the system prompt from file
    if custom_path:
        custom_file = Path(custom_path)
        try:
            if custom_file.exists():
                return custom_file.read_text(encoding="utf-8")
            raise FileNotFoundError(f"System prompt not found at {custom_path}")
        except Exception as e:
            raise FileNotFoundError(f"Error loading system prompt from {custom_path}: {e}") from e

    # Check default location
    prompt_content = ""
    if SYSTEM_PROMPT_FILE.exists():
        try:
            prompt_content = SYSTEM_PROMPT_FILE.read_text(encoding="utf-8")
        except Exception as e:
            raise FileNotFoundError(f"Error loading system prompt from {SYSTEM_PROMPT_FILE}: {e}") from e
    else:
        raise FileNotFoundError(f"Default system prompt not found at {SYSTEM_PROMPT_FILE}")

    # Persistent priors (2.6): DACLI.md is the top layer of context —
    # connection profiles, naming conventions, the medallion rules. It supersedes
    # the legacy prompts/GUIDELINES.md, which is used only as a fallback.
    from dacli.memory.priors import load_priors

    priors = load_priors()
    if priors:
        prompt_content = f"{priors}\n\n---\n\n{prompt_content}"
    else:
        guidelines_file = SYSTEM_PROMPT_FILE.parent / "GUIDELINES.md"
        if guidelines_file.exists():
            try:
                guidelines_content = guidelines_file.read_text(encoding="utf-8")
                prompt_content += f"\n\n{guidelines_content}"
            except Exception:
                pass  # Ignore if guidelines cannot be read

    return prompt_content

def save_system_prompt(content: str, custom_path: str | None = None) -> Path:
    # Save the system prompt to file
    target = Path(custom_path) if custom_path else SYSTEM_PROMPT_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target