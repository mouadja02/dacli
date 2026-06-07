"""Sandbox: spilled-result access + the Docker runtime (per-session container).

Three layers:

* :class:`FetchResultSdkTest` / :class:`FetchResultBridgeTest` — the ``fetch_result``
  fix (load a spilled ``res_*`` result's rows into sandbox code), offline. The
  bridge test spawns the real subprocess worker.
* :class:`FactoryTest` — the runtime selector (subprocess / auto / docker).
* :class:`DockerRuntimeTest` — the hardened per-session container, exercised
  against a real Docker engine. Skipped when no engine is reachable (so CI on a
  Docker-less runner still passes); on the maintainer's box it builds the image
  once and proves: Python runs, code is non-root, the baked stack imports, the
  /workspace volume round-trips, the governed bridge fetches a result, and a
  pip-installed dep persists across runs in the session.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace

from dacli.context.spill import ResultStore
from dacli.sandbox.docker_runtime import DockerSandboxRuntime
from dacli.sandbox.factory import build_sandbox_runtime
from dacli.sandbox.policy import SandboxPolicy
from dacli.sandbox.runtime import SandboxRuntime
from dacli.sandbox.sdk import ConnectorSDK


def _run(coro):
    return asyncio.run(coro)


async def _noop_execute(tool, args):
    from dacli.connectors.base import ToolResult, ToolStatus
    return ToolResult(tool_name=tool, status=ToolStatus.SUCCESS, data=[])


_DOCKER = DockerSandboxRuntime.available()


# ===========================================================================
# fetch_result — in-process SDK (no subprocess)
# ===========================================================================
class FetchResultSdkTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dacli_fr_")
        self.store = ResultStore(root=self.tmp, session_id="s")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_returns_full_rows_not_rebounded(self):
        rows = [{"i": i} for i in range(200)]
        handle = self.store.write("q", rows)
        sdk = ConnectorSDK(_noop_execute, result_store=self.store, workdir=self.tmp)
        got = sdk.fetch_result(handle)
        # The whole point: fetch_result is NOT re-bounded to a 20-row preview the
        # way run() bounds fresh results — you get every row to process in code.
        self.assertEqual(len(got), 200)
        self.assertEqual(got[0], {"i": 0})

    def test_slice(self):
        handle = self.store.write("q", [{"i": i} for i in range(100)])
        sdk = ConnectorSDK(_noop_execute, result_store=self.store, workdir=self.tmp)
        window = sdk.fetch_result(handle, start=10, count=5)
        self.assertEqual([r["i"] for r in window], [10, 11, 12, 13, 14])

    def test_unknown_handle_raises_loudly(self):
        sdk = ConnectorSDK(_noop_execute, result_store=self.store, workdir=self.tmp)
        with self.assertRaises(RuntimeError):
            sdk.fetch_result("res_does_not_exist")

    def test_no_store_raises(self):
        sdk = ConnectorSDK(_noop_execute, result_store=None, workdir=self.tmp)
        with self.assertRaises(RuntimeError):
            sdk.fetch_result("anything")


# ===========================================================================
# fetch_result — over the real bridge (subprocess worker), offline
# ===========================================================================
class FetchResultBridgeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dacli_frb_")
        self.store = ResultStore(root=self.tmp, session_id="s")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_worker_can_fetch_a_spilled_result(self):
        rows = [{"k": i, "v": i * i} for i in range(120)]
        handle = self.store.write("big_query", rows)
        policy = SandboxPolicy(workdir=self.tmp, wall_clock_seconds=60, network="off")
        rt = SandboxRuntime(policy, _noop_execute, result_store=self.store)
        code = (
            f"rows = sdk.fetch_result({handle!r})\n"
            "sdk.finish({'n': len(rows), 'first': rows[0], 'last': rows[-1]})\n"
        )
        res = _run(rt.run_script(code))
        self.assertTrue(res.ok, msg=res.error or res.stderr)
        self.assertEqual(res.returned["n"], 120)
        self.assertEqual(res.returned["first"], {"k": 0, "v": 0})
        self.assertEqual(res.returned["last"], {"k": 119, "v": 119 * 119})

    def test_unknown_handle_fails_the_run(self):
        policy = SandboxPolicy(workdir=self.tmp, wall_clock_seconds=60, network="off")
        rt = SandboxRuntime(policy, _noop_execute, result_store=self.store)
        code = "rows = sdk.fetch_result('res_nope'); sdk.finish({'n': len(rows)})"
        res = _run(rt.run_script(code))
        # The load must fail loudly — not silently 'succeed' on zero rows.
        self.assertFalse(res.ok)
        self.assertIn("res_nope", (res.error or "") + (res.stderr or ""))


# ===========================================================================
# runtime factory
# ===========================================================================
def _settings(runtime: str, workdir: str, *, network: str = "off"):
    return SimpleNamespace(sandbox=SimpleNamespace(
        runtime=runtime, workdir=workdir, wall_clock_seconds=60, max_memory_mb=512,
        max_output_chars=2000, network=network, egress_allowlist=[],
        docker_image="dacli-sandbox:latest", docker_bin="docker",
        docker_cpus=1.0, docker_pids_limit=64, docker_auto_build=True,
    ))


class FactoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dacli_fac_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_subprocess_explicit(self):
        rt, backend = build_sandbox_runtime(
            _settings("subprocess", self.tmp), _noop_execute)
        self.assertEqual(backend, "subprocess")
        self.assertIsInstance(rt, SandboxRuntime)

    def test_auto_picks_an_available_backend(self):
        _rt, backend = build_sandbox_runtime(_settings("auto", self.tmp), _noop_execute)
        self.assertIn(backend, ("docker", "subprocess"))
        # auto must agree with what Docker reports as available on this host.
        self.assertEqual(backend == "docker", _DOCKER)

    @unittest.skipUnless(_DOCKER, "docker engine not available")
    def test_docker_when_requested(self):
        rt, backend = build_sandbox_runtime(_settings("docker", self.tmp), _noop_execute)
        self.assertEqual(backend, "docker")
        self.assertIsInstance(rt, DockerSandboxRuntime)


# ===========================================================================
# the real Docker runtime — hardened per-session container
# ===========================================================================
@unittest.skipUnless(_DOCKER, "docker engine not available")
class DockerRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="dacli_docker_")
        cls.policy = SandboxPolicy(
            workdir=cls.tmp, wall_clock_seconds=180, max_memory_mb=512, network="off")
        cls.rt = DockerSandboxRuntime(
            cls.policy, _noop_execute, session_id="pytest-docker",
            cpus=1.0, pids_limit=128)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.rt.close()
        finally:
            shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_runs_python_in_container(self):
        res = _run(self.rt.run_script(
            "import sys; sdk.finish({'maj': sys.version_info[0], 'plat': sys.platform})"))
        self.assertTrue(res.ok, msg=res.error or res.stderr)
        self.assertEqual(res.returned["maj"], 3)
        # The container is Linux even though the host is Windows.
        self.assertTrue(str(res.returned["plat"]).startswith("linux"))

    def test_code_is_non_root(self):
        res = _run(self.rt.run_script("import os; sdk.finish({'uid': os.getuid()})"))
        self.assertTrue(res.ok, msg=res.error or res.stderr)
        self.assertNotEqual(res.returned["uid"], 0)

    def test_baked_stack_imports(self):
        res = _run(self.rt.run_script(
            "import pandas, numpy, pyarrow; sdk.finish({'pandas': pandas.__version__})"))
        self.assertTrue(res.ok, msg=res.error or res.stderr)
        self.assertTrue(res.returned["pandas"])

    def test_workspace_volume_round_trips(self):
        # Code writes into /workspace; the host sees it under the run dir.
        res = _run(self.rt.run_script(
            "open('marker.txt', 'w').write('hello-from-container'); "
            "sdk.finish({'wrote': True})"))
        self.assertTrue(res.ok, msg=res.error or res.stderr)
        marker = os.path.join(res.workdir, "marker.txt")
        self.assertTrue(os.path.isfile(marker))
        with open(marker, encoding="utf-8") as f:
            self.assertEqual(f.read(), "hello-from-container")

    def test_fetch_result_over_governed_bridge(self):
        store = ResultStore(root=self.tmp, session_id="docker-fetch")
        handle = store.write("q", [{"i": i} for i in range(75)])
        self.rt.bind_result_store(store)
        res = _run(self.rt.run_script(
            f"rows = sdk.fetch_result({handle!r}); sdk.finish({{'n': len(rows)}})"))
        self.assertTrue(res.ok, msg=res.error or res.stderr)
        self.assertEqual(res.returned["n"], 75)

    def test_pip_install_persists_across_runs_in_session(self):
        # The headline capability: install a dep in one run, use it in the next
        # (same session container). Needs egress, so use an 'open' runtime; skip
        # if the network/package index is unreachable rather than failing.
        net_tmp = tempfile.mkdtemp(prefix="dacli_docker_net_")
        policy = SandboxPolicy(
            workdir=net_tmp, wall_clock_seconds=180, max_memory_mb=512, network="open")
        rt = DockerSandboxRuntime(
            policy, _noop_execute, session_id="pytest-docker-net",
            cpus=1.0, pids_limit=128)
        try:
            install = _run(rt.run_script(
                "import subprocess, sys\n"
                "r = subprocess.run([sys.executable, '-m', 'pip', 'install', '--quiet', 'humanize'],"
                " capture_output=True, text=True)\n"
                "sdk.finish({'rc': r.returncode, 'err': r.stderr[-400:]})\n"))
            if not install.ok or install.returned.get("rc") != 0:
                self.skipTest(f"pip install unavailable (offline?): {install.returned or install.error}")
            use = _run(rt.run_script(
                "import humanize; sdk.finish({'v': humanize.intcomma(1234567)})"))
            self.assertTrue(use.ok, msg=use.error or use.stderr)
            self.assertEqual(use.returned["v"], "1,234,567")
        finally:
            rt.close()
            shutil.rmtree(net_tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
