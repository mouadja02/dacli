# M01 characterization snapshots

Committed snapshots of the live surface as it shipped before the Pi-style pivot
(roadmap M03–M16). They exist so the rebuilt spine can be diffed against current
behavior, not against hope.

- `snowflake_read`, `github_read`, `shell_command` — one governed single-step
  turn each, replayed through the real Kernel + Dispatcher + Governor with a
  scripted LLM and faked connector seams (no live model, no credentials).
- `slash_inventory` — the `/help` command list (`config.CLI_COMMANDS`).
- `doctor_shape` — the structure of `dacli doctor`'s JSON (keys + value types;
  values are machine-specific so they aren't pinned).
- `generated_connector_flow` — generate → import → rediscover → use.

Captured and asserted by `tests/test_m01_characterization.py`. After an
*intended* behavior change (e.g. the M09/M10 rebaseline), regenerate with:

    DACLI_M01_RECORD=1 pytest tests/test_m01_characterization.py
