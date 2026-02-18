from pathlib import Path
from typing import Optional

# Default location for the user prompt
USER_PROMPT_FILE = Path(__file__).parent / "user_prompt.md"

DEFAULT_USER_PROMPT = """
You are a helpful assistant.
"""


def get_default_user_prompt() -> str:
    """Return the default user prompt for the Data Warehouse Agent."""
    return DEFAULT_USER_PROMPT


def load_user_prompt(custom_path: Optional[str] = None) -> str:
    # Load the user prompt from file
    if custom_path:
        custom_file = Path(custom_path)
        try:
            if custom_file.exists():
                return custom_file.read_text(encoding="utf-8")
            else:
                # Fallback or raise? For now let's raise as per system_prompt logic
                raise FileNotFoundError(f"User prompt not found at {custom_path}")
        except Exception as e:
            raise FileNotFoundError(
                f"Error loading user prompt from {custom_path}: {e}"
            )

    # Check default location
    if USER_PROMPT_FILE.exists():
        try:
            return USER_PROMPT_FILE.read_text(encoding="utf-8")
        except Exception as e:
            # Just return default if file read fails? Or raise?
            # system_prompt raises, so we raise too.
            raise FileNotFoundError(
                f"Error loading user prompt from {USER_PROMPT_FILE}: {e}"
            )

    return DEFAULT_USER_PROMPT


def save_user_prompt(content: str, custom_path: Optional[str] = None) -> Path:
    # Save the user prompt to file
    target = Path(custom_path) if custom_path else USER_PROMPT_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target
