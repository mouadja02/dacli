"""``SimShell`` — a deterministic, offline shell for the eval inner loop.

The shell tier's golden tasks must run in CI with **no real subprocess** yet keep
the post-conditions *honest*: a write is verified by re-observing the real
filesystem (:func:`core.verify.shell_writes_observed`), not by trusting stdout.
So ``SimShell`` is not a canned-string responder — it is a tiny **interpreter**
of a safe command vocabulary that performs the *real* file effect inside the
session's jailed workspace. The environment is still the oracle; we have only
removed the OS shell process (and its non-determinism) from the loop.

It plugs into :class:`~sandbox.terminal.TerminalSession` via the injectable
``command_runner`` seam — exactly as :class:`~eval.sim.cli.SimCli` plugs into the
CLI connectors — so the same governance spine runs over it unchanged.

Recognised vocabulary (enough for the golden suite): ``echo``/``printf`` with
``>``/``>>`` redirects, ``mkdir [-p]``, ``touch``, ``cat``, ``ls``, ``pwd``,
``rm [-rf]``, ``cp``, ``mv``, ``seq`` (for large-output spill tests) and
``&&``/``;`` sequencing. Anything else returns a canned response (configurable)
or a benign ``rc 0`` — never an uncontrolled real command.
"""

from __future__ import annotations

import os
import shlex
import shutil

from dacli.sandbox.shells.base import RawExec


class SimShell:
    """A programmable, deterministic shell over a real (jailed) workspace dir."""

    def __init__(
        self,
        *,
        responses: dict[str, tuple[str, int]] | None = None,
        fail_commands: list[str] | None = None,
    ):
        # Exact-or-prefix canned responses: command (or its prefix) -> (out, rc).
        self.responses = dict(responses or {})
        # Commands whose *leading program* should report a non-zero exit even
        # though they "ran" (to exercise the shell_exit_zero post-condition).
        self.fail_commands = set(fail_commands or [])
        self.calls: list[str] = []

    # The TerminalSession command_runner signature.
    def __call__(self, command: str, *, cwd: str = ".", timeout: float | None = None) -> RawExec:
        command = (command or "").strip()
        self.calls.append(command)

        # Canned responses win (exact then prefix) — lets a task force a specific
        # stdout/rc without teaching the interpreter a new verb.
        if command in self.responses:
            out, rc = self.responses[command]
            return RawExec(output=out, exit_code=rc)
        for prefix, (out, rc) in self.responses.items():
            if command.startswith(prefix):
                return RawExec(output=out, exit_code=rc)

        out_lines: list[str] = []
        rc = 0
        # Sequence on && / ; ; stop a && chain on first failure (real shell semantics).
        segments = _split_sequence(command)
        for seg, joiner in segments:
            seg_out, seg_rc = self._run_segment(seg, cwd)
            if seg_out:
                out_lines.append(seg_out)
            rc = seg_rc
            if seg_rc != 0 and joiner == "&&":
                break
        return RawExec(output="\n".join(ln for ln in out_lines if ln != ""), exit_code=rc)

    # ------------------------------------------------------------------
    def called_with(self, needle: str) -> bool:
        return any(needle in c for c in self.calls)

    # ------------------------------------------------------------------
    def _run_segment(self, segment: str, cwd: str) -> tuple[str, int]:
        try:
            tokens = shlex.split(segment, posix=True)
        except ValueError:
            tokens = segment.split()
        if not tokens:
            return "", 0
        prog = os.path.basename(tokens[0]).lower()
        args = tokens[1:]

        if prog in self.fail_commands:
            return f"{prog}: simulated failure", 1

        # Redirect handling (echo/printf ... > file / >> file).
        target, mode, rest = _extract_redirect(args)

        if prog in ("echo", "printf"):
            text = _echo_text(prog, rest)
            if target is not None:
                return self._write(cwd, target, text + ("\n" if text else ""), append=(mode == ">>"))
            return text, 0
        if prog in ("mkdir", "md"):
            return self._mkdir(cwd, [a for a in rest if not a.startswith("-")])
        if prog == "touch":
            return self._touch(cwd, rest)
        if prog in ("cat", "type"):
            return self._cat(cwd, rest)
        if prog in ("ls", "dir"):
            return self._ls(cwd, rest)
        if prog == "pwd":
            return cwd, 0
        if prog in ("rm", "del", "erase", "rmdir", "rd"):
            return self._rm(cwd, rest)
        if prog in ("cp", "copy"):
            return self._cp(cwd, rest)
        if prog in ("mv", "move", "rename"):
            return self._mv(cwd, rest)
        if prog == "seq":
            return _seq(rest)
        if prog == "cd":
            return "", 0  # cwd tracking is the session's job
        # Unknown but already governed/allowed → benign success.
        return "", 0

    # -- filesystem effects (inside the jail) --------------------------
    def _abs(self, cwd: str, rel: str) -> str:
        return rel if os.path.isabs(rel) else os.path.join(cwd, rel)

    def _write(self, cwd: str, target: str, text: str, *, append: bool) -> tuple[str, int]:
        path = self._abs(cwd, target)
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "a" if append else "w", encoding="utf-8") as f:
                f.write(text)
            return "", 0
        except Exception as e:
            return f"write failed: {e}", 1

    def _mkdir(self, cwd: str, dirs: list[str]) -> tuple[str, int]:
        for d in dirs:
            try:
                os.makedirs(self._abs(cwd, d), exist_ok=True)
            except Exception as e:
                return f"mkdir: {e}", 1
        return "", 0

    def _touch(self, cwd: str, files: list[str]) -> tuple[str, int]:
        for f in [a for a in files if not a.startswith("-")]:
            try:
                open(self._abs(cwd, f), "a", encoding="utf-8").close()
            except Exception as e:
                return f"touch: {e}", 1
        return "", 0

    def _cat(self, cwd: str, files: list[str]) -> tuple[str, int]:
        out: list[str] = []
        for f in [a for a in files if not a.startswith("-")]:
            p = self._abs(cwd, f)
            if not os.path.exists(p):
                return f"cat: {f}: No such file or directory", 1
            try:
                with open(p, encoding="utf-8") as fh:
                    out.append(fh.read().rstrip("\n"))
            except Exception as e:
                return f"cat: {e}", 1
        return "\n".join(out), 0

    def _ls(self, cwd: str, args: list[str]) -> tuple[str, int]:
        targets = [a for a in args if not a.startswith("-")] or ["."]
        out: list[str] = []
        for t in targets:
            p = self._abs(cwd, t)
            if not os.path.exists(p):
                return f"ls: {t}: No such file or directory", 1
            if os.path.isdir(p):
                out.extend(sorted(os.listdir(p)))
            else:
                out.append(os.path.basename(p))
        return "\n".join(out), 0

    def _rm(self, cwd: str, args: list[str]) -> tuple[str, int]:
        targets = [a for a in args if not a.startswith("-")]
        recursive = any(a for a in args if a.startswith("-") and ("r" in a.lower()))
        for t in targets:
            p = self._abs(cwd, t)
            try:
                if os.path.isdir(p):
                    if recursive:
                        shutil.rmtree(p, ignore_errors=True)
                    else:
                        return f"rm: {t}: is a directory", 1
                elif os.path.exists(p):
                    os.remove(p)
            except Exception as e:
                return f"rm: {e}", 1
        return "", 0

    def _cp(self, cwd: str, args: list[str]) -> tuple[str, int]:
        files = [a for a in args if not a.startswith("-")]
        if len(files) < 2:
            return "cp: missing destination", 1
        src, dst = self._abs(cwd, files[0]), self._abs(cwd, files[-1])
        try:
            if os.path.isdir(src):
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src, dst)
            return "", 0
        except Exception as e:
            return f"cp: {e}", 1

    def _mv(self, cwd: str, args: list[str]) -> tuple[str, int]:
        files = [a for a in args if not a.startswith("-")]
        if len(files) < 2:
            return "mv: missing destination", 1
        try:
            shutil.move(self._abs(cwd, files[0]), self._abs(cwd, files[-1]))
            return "", 0
        except Exception as e:
            return f"mv: {e}", 1


# ---------------------------------------------------------------------------
# parsing helpers
# ---------------------------------------------------------------------------
def _split_sequence(command: str) -> list[tuple[str, str]]:
    """Split on && and ; keeping the joiner that *preceded* each segment."""
    out: list[tuple[str, str]] = []
    buf = ""
    i = 0
    joiner = ""
    while i < len(command):
        if command[i:i + 2] == "&&":
            out.append((buf.strip(), joiner))
            buf = ""
            joiner = "&&"
            i += 2
            continue
        if command[i] == ";":
            out.append((buf.strip(), joiner))
            buf = ""
            joiner = ";"
            i += 1
            continue
        buf += command[i]
        i += 1
    if buf.strip():
        out.append((buf.strip(), joiner))
    return [(s, j) for s, j in out if s]


def _extract_redirect(args: list[str]) -> tuple[str | None, str | None, list[str]]:
    """Pull a trailing ``> file`` / ``>> file`` out of args; return (target, mode, rest)."""
    target: str | None = None
    mode: str | None = None
    rest: list[str] = []
    i = 0
    while i < len(args):
        a = args[i]
        if a in (">", ">>"):
            mode = a
            if i + 1 < len(args):
                target = args[i + 1]
                i += 2
                continue
            i += 1
            continue
        # glued form: ">file" / ">>file"
        if a.startswith(">>"):
            mode, target = ">>", a[2:]
            i += 1
            continue
        if a.startswith(">"):
            mode, target = ">", a[1:]
            i += 1
            continue
        rest.append(a)
        i += 1
    return target, mode, rest


def _echo_text(prog: str, rest: list[str]) -> str:
    # printf's first arg is a format; echo just joins. Good enough for the sim.
    if prog == "printf" and rest:
        return rest[0].replace("\\n", "\n")
    return " ".join(rest)


def _seq(args: list[str]) -> tuple[str, int]:
    nums = [a for a in args if not a.startswith("-")]
    try:
        if len(nums) == 1:
            start, end = 1, int(nums[0])
        elif len(nums) >= 2:
            start, end = int(nums[0]), int(nums[1])
        else:
            return "", 0
    except ValueError:
        return "seq: invalid argument", 1
    if end - start > 1_000_000:
        return "seq: range too large", 1
    return "\n".join(str(n) for n in range(start, end + 1)), 0


def make_sim_session(
    session_id: str,
    workspace_root: str,
    *,
    sim: SimShell | None = None,
    **kwargs,
):
    """Convenience: a :class:`TerminalSession` driven by a :class:`SimShell`."""
    from dacli.sandbox.terminal import TerminalSession

    sim = sim or SimShell()
    session = TerminalSession(
        session_id=session_id,
        command_runner=sim,
        workspace_root=workspace_root,
        journal=kwargs.pop("journal", False),
        **kwargs,
    )
    return session, sim
