from pathlib import Path
from collections.abc import Iterable

from dacli.core.logging_setup import get_logger
from dacli.core.paths import user_prompt_overlay

log = get_logger(__name__)

# Decomposed prompt fragments: an invariant core + per-connector
# fragments disclosed only when that connector is in play.
FRAGMENTS_DIR = Path(__file__).parent / "fragments"
CORE_FRAGMENT = FRAGMENTS_DIR / "core.md"

# The installed `dacli` package dir. Nothing here is editable for a pip user; the
# overlay (paths.user_prompt_overlay) is the writable layer instead.
PACKAGE_DIR = Path(__file__).resolve().parent.parent

# The single source of truth for the system prompt (07.E). The agent runs on the
# composed `core.md`; `/prompt` displays the same source. There is no longer a
# separate `system_message.md` to drift from it.
SYSTEM_PROMPT_FILE = CORE_FRAGMENT


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

    Layering (precedence top to bottom): the invariant core
    (``prompts/fragments/core.md`` — the one packaged source), then the editable
    user overlay (``paths.user_prompt_overlay()``) when present, then the fragments
    for the disclosed connectors (``prompts/fragments/<connector_id>.md``). The
    overlay sits after core so a user can extend or correct the base, and before
    connector fragments so connector-specific rules still get the last word.

    It deliberately does **not** load ``DACLI.md`` priors — those are layer L1 of
    the context assembler (``context.assembler.build_context``), which pins them, so
    loading them here too would duplicate them. ``task`` is accepted for future
    task-conditioned fragments; unused today.
    """
    parts = [_read(CORE_FRAGMENT)]
    overlay = _read(user_prompt_overlay())
    if overlay:
        parts.append(overlay)
    for connector_id in disclosed_connectors or []:
        fragment = _read(FRAGMENTS_DIR / f"{connector_id}.md")
        if fragment:
            parts.append(fragment)
    return "\n\n".join(p for p in parts if p)

def get_default_system_prompt() -> str:
    """Return the default system prompt (the invariant core fragment)."""
    return compose_system_prompt()

def load_system_prompt(custom_path: str | None = None) -> str:
    # Load the system prompt. With no custom override this is the single live
    # source (the composed `core.md`), so the agent's static fallback and the
    # live pipeline share one prompt; priors are then layered on as below.
    if custom_path:
        custom_file = Path(custom_path)
        try:
            if custom_file.exists():
                return custom_file.read_text(encoding="utf-8")
            raise FileNotFoundError(f"System prompt not found at {custom_path}")
        except Exception as e:
            raise FileNotFoundError(f"Error loading system prompt from {custom_path}: {e}") from e

    prompt_content = compose_system_prompt()

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
                # Ignore if guidelines cannot be read
                log.debug("failed to read %s", guidelines_file, exc_info=True)

    return prompt_content

def save_system_prompt(content: str, custom_path: str | None = None) -> Path:
    """Write the prompt to the editable overlay, or to ``custom_path`` for export.

    With no path it targets the user overlay (``paths.user_prompt_overlay()``),
    never the packaged read-only ``core.md``. Writing anywhere inside the installed
    package is refused — that file is overwritten on ``pip install -U``.
    """
    target = Path(custom_path) if custom_path else user_prompt_overlay()
    if target.resolve().is_relative_to(PACKAGE_DIR):
        raise ValueError(
            f"refusing to write into the packaged (read-only) prompt dir: {target}. "
            "Customize via the overlay (dacli prompt --edit) or DACLI.md instead."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target