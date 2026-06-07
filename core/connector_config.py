"""Runtime config access for connectors without a Settings section.

Built-in connectors read their config from a typed ``Settings`` section (e.g.
``settings.snowflake``). **Generated** connectors have no such section — their
credentials are collected by ``/connect`` and stored (Fernet-encrypted) in the
``.dacli/dacli.json`` ``secrets`` block under the connector's id.

This module is the read side of that store for generated connectors. The
generator emits connectors whose ``__init__`` does::

    from core.connector_config import load_connector_config
    self.cfg = load_connector_config("<connector_id>")

so a generated connector reads its decrypted config the same way a built-in one
reads ``self.settings.<id>`` — transparently, never touching plaintext on disk.
"""

from __future__ import annotations

import os
from typing import Any


def _resolve_base_dir(base_dir: str | None) -> str:
    if base_dir:
        return base_dir
    # Mirror core.crypto's resolution so the key/secret locations always agree.
    state_path = os.environ.get("DACLI_STATE_PATH", ".dacli/state/")
    from pathlib import Path

    return str(Path(state_path).parent)


def load_connector_config(
    connector_id: str,
    base_dir: str | None = None,
    settings: Any = None,
) -> dict[str, Any]:
    """Return the decrypted config dict for ``connector_id``.

    Reads the ``secrets`` block of ``.dacli/dacli.json`` via :class:`DacliStore`
    (which decrypts transparently) and returns the section for this connector,
    or an empty dict if nothing has been configured yet. Never raises — a
    connector under construction must degrade to "unconfigured", not crash.

    The base dir is resolved (in priority): explicit ``base_dir`` > the
    ``settings.agent.state_path`` parent (so a connector that passes its
    ``settings`` reads the *configured* location, not just the default) >
    ``DACLI_STATE_PATH`` env > ``.dacli/state/``. Generated connectors should
    pass ``settings`` so a custom ``state_path`` resolves correctly.
    """
    try:
        if base_dir is None and settings is not None:
            state_path = getattr(getattr(settings, "agent", None), "state_path", None)
            if state_path:
                from core.crypto import resolve_base_dir

                base_dir = str(resolve_base_dir(state_path))

        from core.store import DacliStore

        store = DacliStore(base_dir=_resolve_base_dir(base_dir))
        section = store.get_secrets().get(connector_id, {})
        return dict(section) if isinstance(section, dict) else {}
    except Exception:
        return {}
