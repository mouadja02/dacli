# MCP Bridge

Opt-in **client** adapter for one external [MCP](https://modelcontextprotocol.io)
server. dacli's core stays tools-as-code (no MCP); this bridge only lets you
plug an ecosystem MCP server (dbt MCP, a warehouse MCP, …) in as a normal,
governed dacli connector.

## Posture

- **Off by default.** The connector is disabled in its manifest, and even when
  enabled it is inert until `mcp.command` (stdio) or `mcp.url`
  (streamable-http) is configured. A default install never speaks MCP.
- **Not a governance bypass.** Every proxied call flows through
  `Dispatcher.execute` → `Governor` (classify → policy → audit) like any other
  operation. Proxied tools default to the **risky** tier — pin individual
  tools down via `mcp.risk_overrides` only after you trust them.
- **Generic verification only.** MCP tools cannot declare environment-anchored
  post-conditions, so proxied calls are held to the generic
  `result_succeeded` gate. Prefer a native dacli connector when one exists.

## Configuration (`config.yaml`)

```yaml
mcp:
  command: "uvx"             # or url: "https://host/mcp" for streamable-http
  args: ["some-mcp-server"]
  default_risk: risky        # safe | write | risky | irreversible
  risk_overrides:
    list_models: safe        # pin a tool you trust to a lower tier
```

The MCP SDK is an optional extra: `pip install "dacli[mcp]"`.

## Operations

- `mcp_list_tools` — introspection: lists the server's tools (name +
  description + declared risk tier). Safe, read-only.
- `mcp_<tool>` — one operation per discovered server tool, registered at
  connect() time with the server's input schema.
