import asyncio
import base64
import time
import io
import zipfile
import httpx
from types import SimpleNamespace
from typing import Any, ClassVar
from urllib.parse import urlparse

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.config.settings import ConnectorConfig, Settings
from dacli.core.logging_setup import get_logger
from dacli.core.verify import (
    PostCondition, VerificationContext, result_succeeded, data_has_keys,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# — GitHub post-conditions (environment-as-oracle)
# ---------------------------------------------------------------------------
def _sha256(text: str) -> str:
    import hashlib
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def push_commit_landed() -> PostCondition:
    """A push is 'done' only if the commit is reachable AND the stored bytes match.

    ``push_github_file`` used to return success the moment the API call returned;
    here we re-read the file off the branch and confirm (a) a commit SHA came back
    and (b) the committed content hashes equal to what we pushed.
    """
    async def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        if not data.get("commit_sha"):
            return False, "no commit SHA returned — commit did not land"
        target = ctx.target
        path = ctx.args.get("path")
        if target is None or not hasattr(target, "invoke") or not path:
            return True, "could not read back committed file (unverified)"
        res = await target.invoke("read_github_file", {"path": path})
        read = getattr(res, "data", None) or {}
        stored = read.get("content")
        if stored is None:
            return True, "committed file not readable back yet (unverified)"
        if _sha256(stored) != _sha256(ctx.args.get("content", "")):
            return False, "committed content hash differs from pushed content"
        return True, ""

    return PostCondition(
        "push_commit_landed", check,
        "commit SHA reachable on branch; committed content hash matches",
        anchored=True,
    )


def workflow_run_concluded() -> PostCondition:
    """A triggered workflow is 'done' only if its run reached a *successful*
    conclusion — not merely that the dispatch API returned. ``trigger`` polls to
    completion, so the conclusion is on the result; a failed/cancelled run is
    rejected (the model still receives the error_details payload to debug)."""
    def applies(ctx: VerificationContext) -> bool:
        data = getattr(ctx.result, "data", None) or {}
        return "conclusion" in data or "success" in data

    def check(ctx: VerificationContext):
        data = getattr(ctx.result, "data", None) or {}
        concl = data.get("conclusion")
        if concl is not None and concl != "success":
            return False, f"workflow concluded '{concl}', not success"
        if data.get("success") is False:
            return False, "workflow run did not succeed"
        return True, ""

    return PostCondition(
        "workflow_run_concluded", check,
        "triggered run reached a successful conclusion", anchored=True,
        applies_when=applies,
    )


def delete_removed_file() -> PostCondition:
    """After a delete, the file must be gone from the branch."""
    async def check(ctx: VerificationContext):
        target = ctx.target
        path = ctx.args.get("path")
        if target is None or not hasattr(target, "invoke") or not path:
            return True, "could not confirm deletion (unverified)"
        res = await target.invoke("read_github_file", {"path": path})
        read = getattr(res, "data", None) or {}
        if read.get("content") is not None:
            return False, f"file '{path}' still present after delete"
        return True, ""

    return PostCondition(
        "delete_removed_file", check, "file no longer reachable on branch", anchored=True,
    )


class GithubConnector(Connector):
    """
    GitHub API connector for managing dbt project files and workflows.

    Operations:
    - read_file: Read a file from the repository
    - create_or_update_file: Create or update a file (commits)
    - delete_file: Delete a file from the repository
    - list_directory: List contents of a directory
    - trigger_workflow: Trigger a GitHub Actions workflow
    - list_workflow_runs: List recent workflow runs
    - get_workflow_run: Get status of a workflow run
    - get_workflow_run_jobs: Get jobs and logs of a workflow run
    """

    name = "github"

    AVAILABLE_OPERATIONS: ClassVar[list[str]] = ["read_file", "create_or_update_file", "delete_file", "list_directory",
                            "create_or_update_workflow", "trigger_workflow", "list_workflow_runs",
                            "get_workflow_run", "get_workflow_run_jobs"]

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._client: httpx.AsyncClient | None = None

    @property
    def _gh(self):
        # Resolve github config from connector_config (manifest-config pattern,
        # 09/A-4) into a namespace with the old typed-section defaults, deriving
        # owner/repo from repository_url when not set explicitly (the behaviour
        # the deleted GithubSettings validator used to provide).
        cfg = ConnectorConfig(self.settings, "github")
        owner = cfg.get("owner", "") or ""
        repo = cfg.get("repo", "") or ""
        url = cfg.get("repository_url", "") or ""
        if url and (not owner or not repo):
            parts = urlparse(url).path.strip("/").split("/")
            if len(parts) >= 2:
                owner = owner or parts[0]
                repo = repo or parts[1].replace(".git", "")
        return SimpleNamespace(
            token=cfg.get("token", "") or "",
            repository_url=url,
            owner=owner,
            repo=repo,
            branch=cfg.get("branch", "main") or "main",
            timeout=cfg.get("timeout", 60),
            workflow_timeout=cfg.get("workflow_timeout", 600),
        )

    # ------------------------------------------------------------------
    # Connector contract
    # ------------------------------------------------------------------
    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="list_github_directory",
                description="List contents of a directory in the GitHub repository.",
                parameters={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The directory path to list (e.g. 'models', 'analyses'). Use empty string or '/' for root."
                        }
                    },
                    "required": ["path"]
                },
                capability="github.read",
                risk=Risk.SAFE,
                display_name="List Directory",
                category="read",
                postconditions=[data_has_keys("entries", name="lists_entries")],
            ),
            OperationSpec(
                name="read_github_file",
                description="Read the content of a file from the GitHub repository.",
                parameters={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The file path to read (e.g. 'dbt_project.yml')."
                        }
                    },
                    "required": ["path"]
                },
                capability="github.read",
                risk=Risk.SAFE,
                display_name="Read File",
                category="read",
                postconditions=[result_succeeded()],
            ),
            OperationSpec(
                name="push_github_file",
                description="Create or update a file in the GitHub repository (commits changes).",
                parameters={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The file path to create or update."
                        },
                        "content": {
                            "type": "string",
                            "description": "The full content of the file."
                        },
                        "message": {
                            "type": "string",
                            "description": "Commit message describing the change."
                        }
                    },
                    "required": ["path", "content", "message"]
                },
                capability="github.write",
                risk=Risk.WRITE,
                display_name="Push File",
                category="write",
                postconditions=[result_succeeded(), push_commit_landed()],
            ),
            OperationSpec(
                name="delete_github_file",
                description="Delete a file in the GitHub repository (commits changes).",
                parameters={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The file path to delete."
                        },
                        "message": {
                            "type": "string",
                            "description": "Commit message describing the deletion."
                        }
                    },
                    "required": ["path", "message"]
                },
                capability="github.write",
                risk=Risk.IRREVERSIBLE,
                display_name="Delete File",
                category="write",
                postconditions=[result_succeeded(), delete_removed_file()],
            ),
            OperationSpec(
                name="trigger_github_workflow",
                description="Trigger a GitHub Actions workflow.",
                parameters={
                    "type": "object",
                    "properties": {
                        "workflow_id": {
                            "type": "string",
                            "description": "Workflow filename (e.g. 'deploy_dbt.yml')."
                        },
                        "inputs": {
                            "type": "object",
                            "description": "Optional inputs for the workflow_dispatch event."
                        }
                    },
                    "required": ["workflow_id"]
                },
                capability="github.workflow",
                risk=Risk.RISKY,
                display_name="Trigger Workflow",
                category="workflow",
                postconditions=[result_succeeded(), workflow_run_concluded()],
            ),
            OperationSpec(
                name="list_github_workflow_runs",
                description="List recent workflow runs for the repository.",
                parameters={
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Number of runs to return (default 5)."
                        }
                    },
                    "required": []
                },
                capability="github.workflow",
                risk=Risk.SAFE,
                display_name="List Workflow Runs",
                category="workflow",
                postconditions=[data_has_keys("runs", name="lists_runs")],
            ),
            OperationSpec(
                name="get_github_workflow_run",
                description="Get status and details of a workflow run.",
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {
                            "type": "integer",
                            "description": "The ID of the workflow run."
                        }
                    },
                    "required": ["run_id"]
                },
                capability="github.workflow",
                risk=Risk.SAFE,
                display_name="Get Workflow Run",
                category="workflow",
                postconditions=[data_has_keys("status", name="reports_status")],
            ),
            OperationSpec(
                name="get_github_workflow_run_jobs",
                description="Get jobs, steps, and failure logs for a workflow run.",
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {
                            "type": "integer",
                            "description": "The ID of the workflow run."
                        }
                    },
                    "required": ["run_id"]
                },
                capability="github.workflow",
                risk=Risk.SAFE,
                display_name="Get Workflow Jobs",
                category="workflow",
                postconditions=[data_has_keys("jobs", name="lists_jobs")],
            ),
        ]

    # ------------------------------------------------------------------
    # Governance: rollback-path verification (DoD)
    # ------------------------------------------------------------------
    async def verify_rollback(self, plan, args: dict[str, Any]):
        """Prove a native git undo path exists before an irreversible action.

        ``delete_github_file`` is irreversible; it is only recoverable if the
        file currently has a blob SHA we can restore from. We read it live and
        refuse the delete if no restorable blob can be confirmed (fail-safe).
        """
        primitive = getattr(plan, "primitive", "")
        if primitive == "git_revert":
            return True, "prior commit/blob recoverable by SHA (revert)"
        if primitive == "git_restore_blob":
            path = args.get("path")
            if not path:
                return False, "no path given — cannot confirm a restorable blob"
            res = await self.invoke("read_github_file", {"path": path})
            data = getattr(res, "data", None) or {}
            sha = data.get("sha")
            if sha:
                return True, f"file '{path}' has blob SHA {sha[:7]} — restorable after delete"
            return False, f"no blob SHA for '{path}' — deletion not provably reversible"
        return False, f"no verifiable rollback path for primitive '{primitive}'"

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        # Translate the LLM-facing operation + args onto the internal handlers.
        args = dict(args or {})
        if op == "list_github_directory":
            return await self._execute("list_directory", path=args.get("path", ""))
        if op == "read_github_file":
            return await self._execute("read_file", path=args.get("path", ""))
        if op == "push_github_file":
            return await self._execute(
                "create_or_update_file",
                path=args.get("path", ""),
                content=args.get("content", ""),
                message=args.get("message", ""),
            )
        if op == "delete_github_file":
            return await self._execute(
                "delete_file",
                path=args.get("path", ""),
                message=args.get("message", ""),
            )
        if op == "trigger_github_workflow":
            return await self._execute("trigger_workflow", name=args.get("workflow_id", ""))
        if op == "list_github_workflow_runs":
            kwargs = {}
            if "limit" in args:
                # Preserve the historical limit -> per_page remap.
                kwargs["per_page"] = args["limit"]
            return await self._execute("list_workflow_runs", **kwargs)
        if op == "get_github_workflow_run":
            return await self._execute("get_workflow_run", run_id=args.get("run_id"))
        if op == "get_github_workflow_run_jobs":
            return await self._execute("get_workflow_run_jobs", run_id=args.get("run_id"))
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def connect(self) -> bool:
        # Establish connection with the GitHub API
        try:
            self._client = httpx.AsyncClient(
                base_url="https://api.github.com",
                headers={
                    "Authorization": f"Bearer {self._gh.token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28"
                },
                timeout=self._gh.timeout
            )
            self._is_connected = True
            return True
        except Exception as e:
            self._is_connected = False
            raise ConnectionError(f"Failed to connect to GitHub: {e!s}") from e

    async def health(self) -> ToolResult:
        # Validate the GitHub connection
        start_time = time.time()

        try:
            if not self._is_connected:
                await self.connect()

            response = await self._client.get(f"/repos/{self._gh.owner}/{self._gh.repo}")
            response.raise_for_status()
            data = response.json()

            execution_time = (time.time() - start_time) * 1000

            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data={
                    "connected": True,
                    "repo": data.get("full_name"),
                    "default_branch": data.get("default_branch"),
                    "private": data.get("private"),
                },
                execution_time_ms=execution_time,
            )
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                error=str(e),
                execution_time_ms=execution_time,
            )

    async def _execute(self, operation: str, **kwargs) -> ToolResult:
        # Execute a GitHub operation
        start_time = time.time()

        if operation not in self.AVAILABLE_OPERATIONS:
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                error=f"Unknown operation '{operation}'. Available: {self.AVAILABLE_OPERATIONS}",
                execution_time_ms=0
            )

        try:
            if not self._is_connected:
                await self.connect()

            handler = getattr(self, f"_op_{operation}")
            result_data = await handler(**kwargs)

            execution_time = (time.time() - start_time) * 1000

            if isinstance(result_data, dict) and "error" in result_data:
                return ToolResult(
                    tool_name=self.name,
                    status=ToolStatus.ERROR,
                    error=result_data["error"],
                    execution_time_ms=execution_time,
                    metadata={"operation": operation},
                )

            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data=result_data,
                execution_time_ms=execution_time,
                metadata={"operation": operation},
            )
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                error=str(e),
                execution_time_ms=execution_time,
                metadata={"operation": operation},
            )

    # ================================================================
    # File Operations
    # ================================================================
    async def _parse_directory_entries(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # Helper to parse directory listing
        return [{
            "name": item.get("name"),
            "type": item.get("type"),
            "path": item.get("path"),
            "size": item.get("size", 0)
        } for item in data]

    async def _get_file_sha(self, path: str, branch: str) -> str | None:
        # Helper to get file SHA if it exists
        try:
            # We use a direct client call to avoid recursion or overhead of full _op_read_file
            # but _op_read_file handles 404 cleanly, so we can use a simplified version of it
            # or just call the API directly here.
            # Using API directly for efficiency:
            url = f"/repos/{self._gh.owner}/{self._gh.repo}/contents/{path}?ref={branch}"
            response = await self._client.get(url)
            if response.status_code == 200:
                return response.json().get("sha")
            if response.status_code != 404:
                log.debug(
                    "failed to get SHA for %s on %s (status %s)",
                    path, branch, response.status_code,
                )
        except Exception:
            log.debug("error fetching SHA for %s", path, exc_info=True)
        return None

    async def _op_read_file(self, path: str = "", **kwargs) -> dict[str, Any]:
        # Read a file from the repository
        branch = self._gh.branch
        response = await self._client.get(f"/repos/{self._gh.owner}/{self._gh.repo}/contents/{path}?ref={branch}")

        if response.status_code == 404:
            return {"error": f"File not found: {path}"}

        response.raise_for_status()

        data = response.json()

        if isinstance(data, list):  # Path is a directory, return content listing
            return {
                "path": path or "/",
                "entries": await self._parse_directory_entries(data),
                "is_directory": True
            }

        # Get file content
        content = base64.b64decode(data.get("content")).decode("utf-8")

        return {
            "path": data.get("path"),
            "content": content,
            "sha": data.get("sha"),
            "size": data.get("size", 0)
        }

    async def _op_create_or_update_file(self, path: str, content: str = "", message: str = "", **kwargs) -> dict[str, Any]:
        # Create or update a file in the repository
        branch = self._gh.branch

        # Check if file exists to get its SHA (required for updates)
        sha = kwargs.get("sha")
        if not sha:
            sha = await self._get_file_sha(path, branch)

        url = f"/repos/{self._gh.owner}/{self._gh.repo}/contents/{path}"
        body = {
            "message": message,
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "branch": branch
        }

        if sha:
            body["sha"] = sha  # Specify the SHA for updates

        response = await self._client.put(url, json=body)

        if response.status_code not in (200, 201):
            return {"error": f"HTTP {response.status_code}: {response.text}"}

        data = response.json()
        return {
            "path": data["content"]["path"],
            "sha": data["content"]["sha"],
            "commit_sha": data["commit"]["sha"],
            "commit_message": data["commit"]["message"],
            "action": "updated" if sha else "created",
        }

    async def _op_delete_file(self, path: str = "", message: str = "", **kwargs) -> dict[str, Any]:
        # Delete a file from the repository
        branch = self._gh.branch
        sha = kwargs.get("sha")
        if not sha:
            sha = await self._get_file_sha(path, branch)

        if not sha:
            return {"error": f"File not found: {path}"}

        url = f"/repos/{self._gh.owner}/{self._gh.repo}/contents/{path}?ref={branch}"
        body = {
            "message": message,
            "sha": sha
        }

        response = await self._client.request("DELETE", url, json=body)

        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}: {response.text}"}

        data = response.json()
        return {
            "path": path,
            "deleted": True,
            "commit_sha": data["commit"]["sha"]
        }

    async def _op_list_directory(self, path: str = "", **kwargs) -> dict[str, Any]:
        # List contents of a directory
        branch = self._gh.branch
        response = await self._client.get(f"/repos/{self._gh.owner}/{self._gh.repo}/contents/{path}?ref={branch}")

        if response.status_code == 404:
            return {"error": f"Directory not found: {path}"}
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            return {"error": f"Path '{path}' is a file, not a directory."}

        entries = await self._parse_directory_entries(data)

        return {
            "path": path or "/",
            "entries": entries
        }

    # ================================================================
    # Workflow Operations
    # ================================================================
    async def _op_create_or_update_workflow(self, name: str = "", content: str = "", message: str = "", **kwargs) -> dict[str, Any]:
        # Create or update a workflow in the repository
        path = name
        if not path.startswith(".github/workflows/"):
            if not path.endswith(".yml") and not path.endswith(".yaml"):
                path = f".github/workflows/{path}.yml"
            else:
                path = f".github/workflows/{path}"

        branch = self._gh.branch

        # Update on target branch
        result = await self._op_create_or_update_file(path=path, content=content, message=message, branch=branch, **kwargs)
        if "error" in result:
            return result

        # Ensure it is also on main if we didn't just update main (redundancy check not strictly required but saves a call)
        if branch != "main":
            # Original logic always pushed to main as well to ensure actions pickup
            result_main = await self._op_create_or_update_file(path=path, content=content, message=message, branch="main", **kwargs)
            if "error" in result_main:
                return result_main

        return result

    async def _op_list_workflow_runs(self, name: str = "", **kwargs) -> dict[str, Any]:
        # List recent workflow runs
        per_page = kwargs.get("per_page", 5)
        branch = self._gh.branch

        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs",
            params={"branch": branch, "per_page": per_page},
        )
        response.raise_for_status()

        data = response.json()
        runs = [
            {
                "id": run["id"],
                "name": run.get("name", ""),
                "status": run["status"],
                "conclusion": run.get("conclusion"),
                "created_at": run["created_at"],
                "html_url": run["html_url"],
            }
            for run in data.get("workflow_runs", [])
        ]

        return {"total_count": data.get("total_count", 0), "runs": runs}

    async def _op_get_workflow_run(self, run_id: int = 0, **kwargs) -> dict[str, Any]:
        # Get status of a specific workflow run
        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs/{run_id}"
        )

        if response.status_code == 404:
            return {"error": f"Workflow run not found: {run_id}"}
        response.raise_for_status()

        run = response.json()
        return {
            "id": run["id"],
            "name": run.get("name", ""),
            "status": run["status"],
            "conclusion": run.get("conclusion"),
            "created_at": run["created_at"],
            "updated_at": run["updated_at"],
            "html_url": run["html_url"],
            "run_attempt": run.get("run_attempt", 1),
        }

    async def _op_trigger_workflow(self, name: str = "", **kwargs) -> dict[str, Any]:
        # Check if there is no workflow run in progress
        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs?status=in_progress"
        )
        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}: {response.text}"}

        data = response.json()
        if data["total_count"] > 0:
            return {"error": "A workflow run is already in progress"}

        # Normalize workflow path and ID
        if not name.startswith(".github/workflows/"):
            if not name.endswith(".yml") and not name.endswith(".yaml"):
                workflow_id = f"{name}.yml"
                path = f".github/workflows/{workflow_id}"
            else:
                workflow_id = name
                path = f".github/workflows/{name}"
        else:
            path = name
            workflow_id = name.split("/")[-1]

        # Check if the workflow exists
        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/contents/{path}"
        )
        if response.status_code != 200:
            return {"error": f"Workflow file not found: {path}"}

        # Record timestamp BEFORE triggering
        before_timestamp = time.time()

        # Trigger the workflow
        response = await self._client.post(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/workflows/{workflow_id}/dispatches",
            json={"ref": self._gh.branch}
        )
        if response.status_code != 204:
            return {"error": f"Failed to trigger workflow: HTTP {response.status_code}: {response.text}"}

        # Wait for run to appear in API
        await asyncio.sleep(5)

        # Find the run we just triggered (by timestamp)
        run_id = None
        for _attempt in range(6):  # Try for 30 seconds
            response = await self._client.get(
                f"/repos/{self._gh.owner}/{self._gh.repo}/actions/workflows/{workflow_id}/runs",
                params={"per_page": 5}
            )
            if response.status_code != 200:
                return {"error": f"Failed to fetch runs: HTTP {response.status_code}"}

            data = response.json()

            # Find run created after we triggered
            from datetime import datetime
            for run in data["workflow_runs"]:
                run_created = datetime.strptime(
                    run["created_at"],
                    "%Y-%m-%dT%H:%M:%SZ"
                ).timestamp()

                if run_created >= before_timestamp:
                    run_id = run["id"]
                    break

            if run_id:
                break

            await asyncio.sleep(5)

        if not run_id:
            return {"error": "Could not find the triggered workflow run"}

        # Poll until completion
        poll_interval = 30
        timeout = self._gh.workflow_timeout
        start_time = time.time()

        while time.time() - start_time < timeout:
            response = await self._client.get(
                f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs/{run_id}"
            )
            if response.status_code != 200:
                return {"error": f"Failed to check run status: HTTP {response.status_code}"}

            data = response.json()
            status = data["status"]

            if status == "completed":
                conclusion = data["conclusion"]
                duration = int(time.time() - start_time)

                if conclusion == "success":
                    return {
                        "success": True,
                        "conclusion": conclusion,
                        "status": status,
                        "run_id": run_id,
                        "html_url": data.get("html_url"),
                        "duration_seconds": duration
                    }
                # Workflow failed - get detailed error information
                error_details = await self._get_workflow_errors_with_logs(run_id)

                return {
                    "success": False,
                    "conclusion": conclusion,
                    "status": status,
                    "run_id": run_id,
                    "html_url": data.get("html_url"),
                    "duration_seconds": duration,
                    "error_details": error_details
                }

            await asyncio.sleep(poll_interval)

        # Timeout occurred
        return {
            "error": "Workflow execution timed out",
            "success": False,
            "run_id": run_id,
            "timeout_seconds": timeout
        }

    async def _op_get_workflow_run_jobs(self, run_id: int = 0, **kwargs) -> dict[str, Any]:
        # Get jobs and step-level details for a workflow run
        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs/{run_id}/jobs"
        )

        if response.status_code == 404:
            return {"error": f"Workflow run not found: {run_id}"}
        response.raise_for_status()

        data = response.json()
        jobs = []
        for job in data.get("jobs", []):
            steps = [
                {
                    "name": step["name"],
                    "status": step["status"],
                    "conclusion": step.get("conclusion"),
                    "number": step["number"],
                }
                for step in job.get("steps", [])
            ]

            job_info = {
                "id": job["id"],
                "name": job["name"],
                "status": job["status"],
                "conclusion": job.get("conclusion"),
                "started_at": job.get("started_at"),
                "completed_at": job.get("completed_at"),
                "steps": steps,
            }

            # If the job failed, fetch its log tail for debugging
            if job.get("conclusion") == "failure":
                log = await self._get_workflow_errors_with_logs(job["id"])
                if log:
                    job_info["log_tail"] = log[-3000:]

            jobs.append(job_info)

        return {"run_id": run_id, "total_jobs": data.get("total_count", 0), "jobs": jobs}

    # -----------------------------------------------------------
    #     Functions to get the workflow logs and parse errors
    # -----------------------------------------------------------
    async def _fetch_job_raw_log(self, job_id: int) -> str | None:
        # Fetch the raw log for a specific job
        try:
            response = await self._client.get(
                f"/repos/{self._gh.owner}/{self._gh.repo}/actions/jobs/{job_id}/logs",
                follow_redirects=True,
            )
            if response.status_code == 200:
                return response.text
        except Exception:
            log.debug("failed to fetch job log for job %s", job_id, exc_info=True)
        return None

    async def _get_workflow_logs(self, run_id: int) -> dict[str, Any]:
        # Download and extract workflow logs
        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs/{run_id}/logs",
            follow_redirects=True
        )

        if response.status_code != 200:
            return {"error": f"Failed to download logs: HTTP {response.status_code}"}

        logs = {}
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                for file_name in zip_file.namelist():
                    with zip_file.open(file_name) as log_file:
                        logs[file_name] = log_file.read().decode('utf-8')
        except Exception as e:
            return {"error": f"Failed to extract logs: {e!s}"}

        return logs

    def _extract_clean_errors(self, log_content: str) -> list:
        # Extract clean error messages from log content
        lines = log_content.split('\n')
        errors = []
        current_error = []
        in_error_block = False

        for line in lines:
            # Remove timestamp prefix
            clean_line = line.split('Z ', 1)[-1] if 'Z ' in line and line[0].isdigit() else line

            # Detect error start
            if 'Encountered an error:' in clean_line or '##[error]' in clean_line:
                in_error_block = True
                if current_error:
                    errors.append('\n'.join(current_error))
                    current_error = []
                continue

            # Capture error content
            if in_error_block:
                # Skip GitHub Actions metadata
                if clean_line.startswith('##['):
                    continue

                # Stop at empty line
                if not clean_line.strip():
                    if current_error:
                        errors.append('\n'.join(current_error))
                        current_error = []
                    in_error_block = False
                    continue

                # Add meaningful lines
                if clean_line.strip():
                    current_error.append(clean_line.strip())

        # Add last error if exists
        if current_error:
            errors.append('\n'.join(current_error))

        return errors

    async def _get_workflow_errors_with_logs(self, run_id: int) -> dict[str, Any]:
        # Get detailed error information including logs

        # Get jobs info
        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs/{run_id}/jobs"
        )
        if response.status_code != 200:
            return {"error": f"Failed to fetch jobs: HTTP {response.status_code}"}

        jobs_data = response.json()

        # Download logs
        logs = await self._get_workflow_logs(run_id)
        if "error" in logs:
            # If we can't get logs, return basic error info
            return await self.get_workflow_errors(run_id)

        result = {
            "failed_jobs": []
        }

        for job in jobs_data["jobs"]:
            if job["conclusion"] in ["failure", "cancelled", "timed_out"]:
                job_errors = {
                    "job_name": job["name"],
                    "conclusion": job["conclusion"],
                    "html_url": job["html_url"],
                    "errors": []
                }

                # Find failed steps
                for step in job["steps"]:
                    if step["conclusion"] in ["failure", "cancelled", "timed_out"]:
                        # Find matching log
                        step_log = None
                        for log_file_name, log_content in logs.items():
                            if step["name"] in log_file_name or f"{step['number']}_" in log_file_name:
                                step_log = log_content
                                break

                        if step_log:
                            error_messages = self._extract_clean_errors(step_log)
                            if error_messages:
                                job_errors["errors"].append({
                                    "step": step["name"],
                                    "error_messages": error_messages
                                })

                result["failed_jobs"].append(job_errors)

        return result

    async def get_workflow_errors(self, run_id: int) -> dict[str, Any]:
        # Get basic error information (fallback when logs unavailable)
        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs/{run_id}"
        )
        if response.status_code != 200:
            return {"error": f"Failed to fetch run: HTTP {response.status_code}"}

        response = await self._client.get(
            f"/repos/{self._gh.owner}/{self._gh.repo}/actions/runs/{run_id}/jobs"
        )
        if response.status_code != 200:
            return {"error": f"Failed to fetch jobs: HTTP {response.status_code}"}

        jobs_data = response.json()

        result = {
            "failed_jobs": []
        }

        for job in jobs_data["jobs"]:
            if job["conclusion"] in ["failure", "cancelled", "timed_out"]:
                job_error = {
                    "job_name": job["name"],
                    "conclusion": job["conclusion"],
                    "html_url": job["html_url"],
                    "failed_steps": []
                }

                for step in job["steps"]:
                    if step["conclusion"] in ["failure", "cancelled", "timed_out"]:
                        job_error["failed_steps"].append({
                            "step_name": step["name"],
                            "conclusion": step["conclusion"]
                        })

                result["failed_jobs"].append(job_error)

        return result
