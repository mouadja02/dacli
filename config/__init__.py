from config.settings import Settings, load_config

CLI_COMMANDS = [
    ("/help", "Show this help message"),
    ("/init", "Generate a DACLI.md priors draft from your config"),
    ("/status", "Show current progress and state"),
    ("/usage", "Show token usage and cost (overall, by model, this session)"),
    ("/context", "Explain the assembled context (sources, tokens, budget)"),
    ("/tools", "Show enabled tools and capabilities"),
    ("/setup", "Run the tool configuration wizard"),
    ("/history", "Show conversation history"),
    ("/sessions", "List available sessions"),
    ("/load <id>", "Load a previous session"),
    ("/export", "Export current state to JSON"),
    ("/config", "Show current configuration"),
    ("/theme <name>", "Switch UI theme (dark, light, ocean, mono)"),
    ("/prompt", "View/edit the system prompt"),
    ("/clear", "Clear conversation history"),
    ("/reset", "Reset agent state"),
    ("/exit", "Exit the CLI"),
]

__all__ = ["Settings", "load_config", "CLI_COMMANDS"]
