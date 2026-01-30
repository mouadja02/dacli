from pathlib import Path
from typing import Optional

# Default location for the system prompt
SYSTEM_PROMPT_FILE = Path(__file__).parent / "system_message.md"

def get_default_system_prompt() -> str:
    """Return the default system prompt for the Data Warehouse Agent."""
    return DEFAULT_SYSTEM_PROMPT

def load_system_prompt(custom_path: Optional[str] = None) -> str:
    # Load the system prompt from file
    if custom_path:
        custom_file = Path(custom_path)
        try:
            if custom_file.exists():
                return custom_file.read_text(encoding="utf-8")
            else:
                raise FileNotFoundError(f"System prompt not found at {custom_path}")
        except Exception as e:
            raise FileNotFoundError(f"Error loading system prompt from {custom_path}: {e}")

    # Check default location
    if SYSTEM_PROMPT_FILE.exists():
        try:
            return SYSTEM_PROMPT_FILE.read_text(encoding="utf-8")
        except Exception as e:
            raise FileNotFoundError(f"Error loading system prompt from {SYSTEM_PROMPT_FILE}: {e}")
    else:
        raise FileNotFoundError(f"Default system prompt not found at {SYSTEM_PROMPT_FILE}")

    raise FileNotFoundError("System prompt not found")

def save_system_prompt(content: str, custom_path: Optional[str] = None) -> Path:
    # Save the system prompt to file
    target = Path(custom_path) if custom_path else SYSTEM_PROMPT_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target