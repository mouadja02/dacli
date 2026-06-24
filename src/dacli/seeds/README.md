# seeds

Bundled, read-only resources shipped in the wheel — the lowest-precedence layer
of the `.dacli` overlay (`core.paths.resource_dir`). A project `.dacli/<kind>` or
the per-user global dir shadows what lives here.

One subdir per resource kind: `extensions/`, `skills/`, `themes/`. `extensions/`
holds the three seed connectors — `snowflake`, `github`, `shell` — each a single
`register(api)` module (no manifest). They're the lowest-precedence extensions and
double as the worked examples the agent reads to extend itself (reporting/03).
