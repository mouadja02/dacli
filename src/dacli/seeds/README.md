# seeds

Bundled, read-only resources shipped in the wheel — the lowest-precedence layer
of the `.dacli` overlay (`core.paths.resource_dir`). A project `.dacli/<kind>` or
the per-user global dir shadows what lives here.

One subdir per resource kind: `extensions/`, `skills/`, `themes/`. Populated in M08
when snowflake, github, and shell are rewritten as `register(api)` seeds; empty for
now.
