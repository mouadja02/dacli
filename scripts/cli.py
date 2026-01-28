import asyncio
import os
import sys
import click

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.table import Table
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text
from rich.syntax import Syntax
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.theme import Theme
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.styles import Style as PTStyle

from scripts import __version__
from scripts.config.settings import load_config, Settings, save_config
from scripts.agent import DACLI_core
from scripts.tools import DACLI_tools, get_available_tools
from scripts.memory import DACLI_memory, set_memory
from scripts.prompts.system_prompt import load_system_prompt, save_system_prompt, SYSTEM_PROMPT_FILE
from scripts.prompts.user_prompt import load_user_prompt, save_user_prompt, USER_PROMPT_FILE
from scripts.config import CLI_COMMANDS

# -----------------------------------------
#  CUSTOMIZE CONSOLE THEME
# -----------------------------------------
CUSTOM_THEME = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "prompt": "bold magenta",
    "tool": "blue",
    "sql": "green",
    "user": "bold white",
    "assistant": "cyan",
    "phase": "bold yellow",
    "step": "dim white",
})

console = Console(theme=CUSTOM_THEME)


# -----------------------------------------
#  UI components
# -----------------------------------------
def print_banner():
    banner_in_box = """
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║             ██████╗   █████╗   ██████╗ ██╗      ██╗              ║
║             ██╔══██╗ ██╔══██╗ ██╔════╝ ██║      ██║              ║
║             ██║  ██║ ███████║ ██║      ██║      ██║              ║
║             ██║  ██║ ██╔══██║ ██║      ██║      ██║              ║
║             ██████╔╝ ██║  ██║ ╚██████╗ ███████╗ ██║              ║
║             ╚═════╝  ╚═╝  ╚═╝  ╚═════╝ ╚══════╝ ╚═╝              ║
║            Your Autonomous Data Engineering CLI Agent            ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
"""
    console.print(banner_in_box, style="dim")
    console.print(f"Version: {__version__}", style="dim")
    console.print(f"Author: {__author__}", style="dim")

def print_help():
    # Print available tolls
    table = Table(title="Available Tools", show_header=True, header_style="bold magenta")
    table.add_column("Tool Name", style="cyan")
    table.add_column("Description")

    for cmd, desc in CLI_COMMANDS:
        table.add_row(cmd, desc)

    console.print(table)


def print_status(memory: DACLI_memory):
    # Print the current agent status
    status = memory.get_status()

    status_text = Text()
    status_text.append("Session: ", style="dim")
    status_text.append(f"{status['session_id']}\n", style="cyan")
    status_text.append("Current Phase: ", style="dim")
    status_text.append(f"{status['current_phase']}\n", style="phase")
    status_text.append("Infrastructure: ", style="dim")
    sttaus_text.append(
        "✅ Ready" if status['infrastructure_ready'] else "⌛ Pending",
        style="success" if status['infrastructure_ready'] else "warning"
    )

    console.print(Panel(status_text, title="Agent Status", border_style="cyan"))

    # Progress table
    table = Table(title="Phase Progress", show_header=True)
    table.add_column("Phase", style="dim")
    table.add_column("Status")
    table.add_column("Progress")
    for phase, info in status.get('phases', {}).items():
        status = info.get("status")
        status_icon = {"not_started": "◻️", "in_progress": "🔄️", "completed": "✅", "failed": "❌","paused": "⏸️"}.get(status, "◻️")
        progress = info.get("progress", "0/0")
        table.add_row(phase.replace("_", " ").title(), f"{status_icon} {status}", progress)