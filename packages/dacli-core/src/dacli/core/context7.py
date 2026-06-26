"""Context7 integration — fetch up-to-date API docs for extension generation.

Calls the Context7 REST API (https://context7.com/docs/api-guide) to resolve a
library name and pull documentation snippets relevant to the extension being
generated. The docs are injected into the generation prompt so the LLM writes
code against current APIs instead of hallucinating endpoints.
"""

from __future__ import annotations

import httpx


_BASE = "https://context7.com/api/v2"
_TIMEOUT = 30.0


async def resolve_library(
    name: str, query: str, *, api_key: str
) -> list[dict]:
    """Search Context7 for libraries matching *name*.

    Returns a list of dicts with at least ``id`` and ``title`` fields.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"libraryName": name, "query": query}
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{_BASE}/libs/search", headers=headers, params=params, timeout=_TIMEOUT
        )
        resp.raise_for_status()
        if not resp.content:
            return []
        try:
            return resp.json().get("results", [])
        except Exception:
            return []


async def fetch_docs(
    library_id: str, query: str, *, api_key: str, tokens: int = 5000
) -> str:
    """Fetch documentation snippets for *library_id* relevant to *query*.

    Returns a plain-text block suitable for injecting into a prompt.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"libraryId": library_id, "query": query, "tokens": tokens}
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{_BASE}/context", headers=headers, params=params, timeout=_TIMEOUT
        )
        resp.raise_for_status()
        if not resp.content:
            return ""
        try:
            data = resp.json()
        except Exception:
            return ""

    parts: list[str] = []

    for snippet in data.get("codeSnippets", []):
        title = snippet.get("codeTitle", "")
        if title:
            parts.append(f"### {title}")
        for code in snippet.get("codeList", []):
            lang = code.get("language", "")
            parts.append(f"```{lang}\n{code.get('code', '')}\n```")

    for info in data.get("infoSnippets", []):
        content = info.get("content", "")
        if content:
            parts.append(content)

    return "\n\n".join(parts) if parts else ""


async def get_library_docs(
    service_name: str, description: str, *, api_key: str
) -> str | None:
    """End-to-end: resolve library + fetch docs. Returns docs text or None."""
    try:
        results = await resolve_library(service_name, description, api_key=api_key)
    except Exception:
        return None

    if not results:
        return None

    # Use the top result.
    library_id = results[0].get("id")
    if not library_id:
        return None

    try:
        docs = await fetch_docs(library_id, description, api_key=api_key)
        return docs or None
    except Exception:
        return None
