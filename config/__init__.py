from config.settings import Settings, load_config

CLI_COMMANDS = [
    ("/help", "Show this help message"),
    ("/init", "Generate a DACLI.md priors draft from your config"),
    ("/status", "Show current progress and state"),
    ("/usage", "Show token usage and cost (overall, by model, this session)"),
    ("/context", "Explain the assembled context (sources, tokens, budget)"),
    ("/audit", "Show governance decisions for this session (why the agent acted)"),
    ("/tools", "Show enabled tools and capabilities"),
    ("/connect [tool]", "Configure a connector — interactive, or pass a name"),
    ("/new-connector", "Generate a new connector from natural language description"),
    ("/testmode [tool]", "Toggle staging test mode (health-gated, side-effect-free) for new connectors"),
    ("/import-connector", "Import a tested connector from sandbox to local"),
    ("/push-connector", "Git commit and push a new connector"),
    ("/debug-connector <name>", "Debug a failing connector with LLM assistance"),
    ("/setup", "Run the tool configuration wizard"),
    ("/history", "Show conversation history"),
    ("/sessions", "List available sessions"),
    ("/load <id>", "Load a previous session"),
    ("/export", "Export current state to JSON"),
    ("/config", "Show current configuration"),
    ("/theme <name>", "Switch UI theme (dark, light, ocean, mono)"),
    ("/prompt", "View/edit the system prompt"),
    ("/clear", "Clear conversation history"),
    ("/cls", "Clear the screen (keeps conversation history)"),
    ("/reset", "Reset agent state"),
    ("/exit", "Exit the CLI"),
    ("/run", "headless one-shot run (mostly for testing)"),
    ("/replay", "replay a headless scenario file"),
]

__all__ = ["Settings", "load_config", "CLI_COMMANDS"]
