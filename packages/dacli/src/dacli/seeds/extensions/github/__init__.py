"""GitHub seed — read/list/push/delete files in a repo over the REST API.

A reference ``register(api)`` extension (reporting/02 seed set), replacing the
old ``connectors/github`` Connector. Token + repo coordinates come from
``api.config()``; the httpx client is built lazily and cached in the registration
closure. The workflow-dispatch operations of the old connector are intentionally
left out — the seed is a worked example of the file path, not a full port.
"""

from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

from dacli.core.verify import data_has_keys, result_succeeded


def register(api):
    state: dict[str, Any] = {"client": None}

    api.config_field("token", secret=True, description="GitHub personal access token")
    api.config_field("owner", description="Repository owner")
    api.config_field("repo", description="Repository name")
    api.config_field("repository_url", description="Full repo URL (owner/repo derived if unset)")
    api.config_field("branch", default="main", description="Default branch")

    def gh() -> SimpleNamespace:
        c = api.config()
        owner, repo = c.get("owner", "") or "", c.get("repo", "") or ""
        url = c.get("repository_url", "") or ""
        if url and (not owner or not repo):
            parts = urlparse(url).path.strip("/").split("/")
            if len(parts) >= 2:
                owner = owner or parts[0]
                repo = repo or parts[1].replace(".git", "")
        return SimpleNamespace(
            token=c.get("token", "") or "", owner=owner, repo=repo,
            branch=c.get("branch", "main") or "main", timeout=c.get("timeout", 60),
        )

    def client(conf: SimpleNamespace):
        if state["client"] is None:
            import httpx
            state["client"] = httpx.AsyncClient(
                base_url="https://api.github.com",
                headers={
                    "Authorization": f"Bearer {conf.token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                timeout=conf.timeout,
            )
        return state["client"]

    async def _get_sha(conf, path: str) -> str | None:
        resp = await client(conf).get(
            f"/repos/{conf.owner}/{conf.repo}/contents/{path}?ref={conf.branch}")
        return resp.json().get("sha") if resp.status_code == 200 else None

    @api.tool(
        name="list_github_directory",
        description="List the contents of a directory in the repository.",
        parameters={"path": {"type": "string", "description": "Directory path (empty for root)."}},
        risk="safe",
        postconditions=[data_has_keys("entries", name="lists_entries")],
        display_name="List Directory",
        category="read",
    )
    async def list_github_directory(args, ctx):
        conf = gh()
        path = args.get("path", "")
        resp = await client(conf).get(
            f"/repos/{conf.owner}/{conf.repo}/contents/{path}?ref={conf.branch}")
        if resp.status_code == 404:
            return ctx.fail(f"Directory not found: {path}", operation="list_directory")
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return ctx.fail(f"Path '{path}' is a file, not a directory.", operation="list_directory")
        entries = [{"name": i.get("name"), "type": i.get("type"),
                    "path": i.get("path"), "size": i.get("size", 0)} for i in data]
        return ctx.ok({"path": path or "/", "entries": entries}, operation="list_directory")

    @api.tool(
        name="read_github_file",
        description="Read a file from the repository.",
        parameters={"path": {"type": "string", "description": "File path within the repo."}},
        risk="safe",
        postconditions=[result_succeeded()],
        display_name="Read File",
        category="read",
    )
    async def read_github_file(args, ctx):
        conf = gh()
        path = args.get("path", "")
        resp = await client(conf).get(
            f"/repos/{conf.owner}/{conf.repo}/contents/{path}?ref={conf.branch}")
        if resp.status_code == 404:
            return ctx.fail(f"File not found: {path}", operation="read_file")
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            entries = [{"name": i.get("name"), "type": i.get("type"),
                        "path": i.get("path"), "size": i.get("size", 0)} for i in data]
            return ctx.ok({"path": path or "/", "entries": entries, "is_directory": True},
                          operation="read_file")
        content = base64.b64decode(data.get("content")).decode("utf-8")
        return ctx.ok(
            {"path": data.get("path"), "content": content,
             "sha": data.get("sha"), "size": data.get("size", 0)},
            operation="read_file",
        )

    @api.tool(
        name="push_github_file",
        description="Create or update a file in the repository (one commit).",
        parameters={
            "path": {"type": "string"},
            "content": {"type": "string"},
            "message": {"type": "string", "description": "Commit message."},
        },
        risk="write",
        postconditions=[data_has_keys("commit_sha", name="commit_landed")],
        display_name="Push File",
        category="write",
    )
    async def push_github_file(args, ctx):
        conf = gh()
        path, content = args.get("path", ""), args.get("content", "")
        sha = await _get_sha(conf, path)
        body = {"message": args.get("message", ""), "branch": conf.branch,
                "content": base64.b64encode(content.encode("utf-8")).decode("utf-8")}
        if sha:
            body["sha"] = sha
        resp = await client(conf).put(
            f"/repos/{conf.owner}/{conf.repo}/contents/{path}", json=body)
        if resp.status_code not in (200, 201):
            return ctx.fail(f"HTTP {resp.status_code}: {resp.text}", operation="create_or_update_file")
        data = resp.json()
        return ctx.ok(
            {"path": data["content"]["path"], "sha": data["content"]["sha"],
             "commit_sha": data["commit"]["sha"], "commit_message": data["commit"]["message"],
             "action": "updated" if sha else "created"},
            operation="create_or_update_file",
        )

    @api.tool(
        name="delete_github_file",
        description="Delete a file from the repository.",
        parameters={"path": {"type": "string"}, "message": {"type": "string", "description": "Commit message."}},
        risk="irreversible",
        postconditions=[data_has_keys("commit_sha", name="commit_landed")],
        display_name="Delete File",
        category="write",
    )
    async def delete_github_file(args, ctx):
        conf = gh()
        path = args.get("path", "")
        sha = await _get_sha(conf, path)
        if not sha:
            return ctx.fail(f"File not found: {path}", operation="delete_file")
        resp = await client(conf).request(
            "DELETE", f"/repos/{conf.owner}/{conf.repo}/contents/{path}?ref={conf.branch}",
            json={"message": args.get("message", ""), "sha": sha})
        if resp.status_code != 200:
            return ctx.fail(f"HTTP {resp.status_code}: {resp.text}", operation="delete_file")
        data = resp.json()
        return ctx.ok({"path": path, "deleted": True, "commit_sha": data["commit"]["sha"]},
                      operation="delete_file")
