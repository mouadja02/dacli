# Connector templates

Reference implementations the connector **generator** uses as few-shot examples
(`core/connector_generator.py`) and that humans can copy when writing a connector
by hand.

These are *patterns*, not live connectors — they live under `templates/` (which
the registry does **not** scan, since it has no `manifest.yaml` at the connector
level) so they never load into the agent.

| File | Pattern | Base class |
|------|---------|-----------|
| `rest_connector_template.py` | REST / SDK-driven platform | `dacli.connectors.base.Connector` |
| `cli_connector_template.py`  | First-class CLI-driven platform | `dacli.connectors.cli_base.CliConnector` |
| `manifest_template.yaml`     | Manifest with `config_fields` | — |

## The contract every connector must honor

1. Subclass `Connector` (REST/SDK) or `CliConnector` (CLI binary).
2. `name` matches the manifest `id`.
3. `operations()` returns `OperationSpec`s, **each with ≥1 post-condition**
   (the registry refuses to register an operation that can't be verified).
4. Read config via `load_connector_config("<id>", settings=settings)` — generated
   connectors have no typed `Settings` section.
5. The manifest declares `config_fields` so `/connect` can prompt for credentials.
