"""Shell command classifier (𝒢) — *blast-radius first*, for the shell tier.

Extends the blast-radius idea of :mod:`governance.classifier` to free-text shell
commands. A free-text terminal is the easiest way to reintroduce ungoverned
destructive execution — the exact failure Era 1 eliminated — so this module is
the load-bearing mitigation: **every command is mapped to one of the existing
blast-radius tiers before it can run**, and an unrecognised command **defaults to
``risky`` (deny-by-default, not allow-by-default)**.

The parse is shell-aware:

* splits a line into segments on ``;``, ``&&``, ``||``, ``&`` and pipelines ``|``;
* strips wrapper prefixes (``sudo``, ``env``, ``nohup``, ``time``, ``xargs`` …)
  and leading ``VAR=value`` assignments to find the real program;
* resolves redirects (``>`` overwrite vs ``>>`` append);
* recognises destructive *patterns* (``rm -rf``, ``git push --force``,
  ``mkfs``, ``dd``, fork bombs, ``curl … | sh``) that no single program token
  conveys;
* flags network egress (``curl``/``wget``/``ssh`` …) to non-allowlisted hosts;
* flags attempts to escape the session workspace jail (``cd ..`` / absolute paths).

The result is a :class:`CommandVerdict` whose ``tier`` plugs straight into the
**existing** :class:`~governance.policy_engine.PolicyEngine` — no second policy
mechanism, honoring "scale 𝒮 with 𝒢".
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field

# The verb → tier vocabulary (tier order, sub-verb sets, destructive-SQL
# regex) is shared with the action/SQL classifier via governance.vocab so the
# two classifiers cannot drift (A-3).
from dacli.governance.vocab import (
    IRREVERSIBLE_SUBVERBS as _IRREVERSIBLE_SUBVERBS,
    READ_SUBVERBS as _READ_SUBVERBS,
    RISKY_SUBVERBS as _RISKY_SUBVERBS,
    WRITE_SUBVERBS as _WRITE_SUBVERBS,
    DESTRUCTIVE_SQL_RE as _SQL_IRREVERSIBLE,
    Tier,
    max_tier as _max_tier,
)


# ---------------------------------------------------------------------------
# Program tables (the known-good lists; everything else defaults to risky)
# ---------------------------------------------------------------------------
# Read-only inspectors — auto-run.
_SAFE_PROGRAMS = {
    "ls", "dir", "pwd", "cat", "type", "head", "tail", "less", "more",
    "echo", "printf", "whoami", "hostname", "date", "env", "printenv",
    "which", "where", "find", "grep", "egrep", "fgrep", "rg", "ripgrep",
    "wc", "sort", "uniq", "cut", "tr", "stat", "file", "tree", "df", "du",
    "ps", "uname", "clear", "cls", "history", "id", "groups", "basename",
    "dirname", "realpath", "readlink", "diff", "cmp", "md5sum", "sha256sum",
    "shasum", "column", "nl", "true", "false", "sleep", "test", "tldr",
    "man", "help", "uptime", "free", "vmstat", "lsblk", "lscpu",
}

# New-state creators that are recoverable — auto-run + post-condition.
_WRITE_PROGRAMS = {
    "mkdir", "md", "touch", "cp", "copy", "ln", "mktemp", "tee",
}

# State mutators with hard-to-predict effects — confirm + rollback plan.
_RISKY_PROGRAMS = {
    "mv", "move", "rename", "ren", "chmod", "chown", "chgrp", "kill",
    "pkill", "taskkill", "ln", "patch", "install", "systemctl", "service",
    "crontab", "at", "sed", "perl", "awk",
}

# Programs whose blast radius is essentially "anything" (interpreters, package
# managers, network fetchers): deny-by-default → risky unless a read-only
# subcommand/flag downgrades them below.
_RISKY_RUNNERS = {
    "python", "python3", "py", "node", "ruby", "php", "bash", "sh", "zsh",
    "powershell", "pwsh", "cmd", "make", "cmake", "pip", "pip3", "npm",
    "npx", "yarn", "pnpm", "cargo", "go", "mvn", "gradle", "pytest",
    "docker", "docker-compose", "kubectl", "helm", "terraform", "ansible",
}

# Irreversible programs / sub-actions — dry-run / refuse / explicit approval.
_IRREVERSIBLE_PROGRAMS = {
    "mkfs", "fdisk", "shred", "dd", "format",
}

# Wrapper prefixes that delegate to a *real* program following them.
_WRAPPERS = {
    "sudo", "doas", "env", "nohup", "time", "command", "exec", "nice",
    "ionice", "stdbuf", "setsid", "watch", "xargs", "timeout", "caffeinate",
}

# The read/write/risky/irreversible CLI sub-verb sets and the destructive-SQL
# regex are imported from governance.vocab above (shared with the SQL
# classifier).

# Hosts in a fetch-pipe-shell are the canonical "remote code execution" smell.
_PIPE_SHELL = re.compile(r"\|\s*(sudo\s+)?(sh|bash|zsh|python[0-9.]*|powershell|pwsh)\b")

_HOST_RE = re.compile(r"https?://([^/\s:]+)", re.IGNORECASE)
_NET_PROGRAMS = {"curl", "wget", "nc", "ncat", "ssh", "scp", "sftp", "ftp",
                 "telnet", "rsync", "git"}  # git only when a URL is present


@dataclass
class CommandVerdict:
    """The auditable blast-radius verdict for one shell command line."""

    tier: Tier
    leading: str | None
    command: str
    reasons: list[str] = field(default_factory=list)
    writes: list[str] = field(default_factory=list)
    overwrites: list[str] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)
    egress_hosts: list[str] = field(default_factory=list)
    irreversible: bool = False
    escapes_jail: bool = False
    unknown: bool = False
    segments: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "tier": self.tier.value,
            "leading": self.leading,
            "reasons": list(self.reasons),
            "writes": list(self.writes),
            "overwrites": list(self.overwrites),
            "deletes": list(self.deletes),
            "egress_hosts": list(self.egress_hosts),
            "irreversible": self.irreversible,
            "escapes_jail": self.escapes_jail,
            "unknown": self.unknown,
            "segments": list(self.segments),
        }


# ---------------------------------------------------------------------------
# parsing helpers
# ---------------------------------------------------------------------------
_SEGMENT_SPLIT = re.compile(r"\|\||&&|\||;|&")


def _split_segments(command: str) -> list[str]:
    """Split a line into command segments (pipelines + sequencing)."""
    return [s.strip() for s in _SEGMENT_SPLIT.split(command) if s.strip()]


def _safe_tokens(segment: str) -> list[str]:
    try:
        return shlex.split(segment, posix=True)
    except ValueError:
        # Unbalanced quotes etc. — fall back to a whitespace split so we still
        # see the leading program (and stay conservative).
        return segment.split()


def _strip_wrappers_and_assignments(tokens: list[str]) -> list[str]:
    out = list(tokens)
    changed = True
    while changed and out:
        changed = False
        head = out[0]
        if "=" in head and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", head):
            out = out[1:]
            changed = True
            continue
        base = head.rsplit("/", 1)[-1].rsplit("\\", 1)[-1].lower()
        if base in _WRAPPERS:
            # Skip the wrapper and its own flags (e.g. `timeout 5`, `env -i`).
            rest = out[1:]
            # Drop a single numeric/flag arg commonly attached to wrappers.
            while rest and (rest[0].startswith("-") or rest[0].isdigit()):
                rest = rest[1:]
            out = rest
            changed = True
    return out


def _program_base(token: str) -> str:
    return token.rsplit("/", 1)[-1].rsplit("\\", 1)[-1].lower()


# ---------------------------------------------------------------------------
# the classifier
# ---------------------------------------------------------------------------
class CommandClassifier:
    """Maps a shell command line → :class:`CommandVerdict`."""

    def __init__(
        self,
        *,
        network: str = "allowlist",
        egress_allowlist: list[str] | None = None,
    ):
        self.network = (network or "allowlist").lower()
        self.allowlist = [h.strip().lower() for h in (egress_allowlist or []) if h.strip()]

    # -- public ---------------------------------------------------------
    def classify(self, command: str) -> CommandVerdict:
        command = (command or "").strip()
        verdict = CommandVerdict(tier=Tier.SAFE, leading=None, command=command)
        if not command:
            verdict.tier = Tier.RISKY
            verdict.unknown = True
            verdict.reasons.append("empty command → default-deny (risky)")
            return verdict

        verdict.segments = _split_segments(command)

        # Whole-line patterns first (they cut across segments).
        if _PIPE_SHELL.search(command):
            verdict.irreversible = True
            verdict.tier = _max_tier(verdict.tier, Tier.IRREVERSIBLE)
            verdict.reasons.append("pipes a downloaded payload into a shell interpreter "
                                   "(remote code execution) → irreversible")

        for seg in verdict.segments:
            self._classify_segment(seg, verdict)

        if verdict.leading is None and verdict.segments:
            verdict.leading = _program_base(_safe_tokens(verdict.segments[0])[:1] or [""][0]) or None

        # Network egress posture.
        if verdict.egress_hosts:
            non_allow = [h for h in verdict.egress_hosts if not self._host_allowed(h)]
            if self.network == "off" and verdict.egress_hosts:
                verdict.tier = _max_tier(verdict.tier, Tier.RISKY)
                verdict.reasons.append(f"network egress to {verdict.egress_hosts} but egress is OFF → risky (deny)")
            elif non_allow:
                verdict.tier = _max_tier(verdict.tier, Tier.RISKY)
                verdict.reasons.append(f"egress to non-allowlisted host(s) {non_allow} → risky (default-deny egress)")

        if verdict.escapes_jail:
            verdict.tier = _max_tier(verdict.tier, Tier.RISKY)

        return verdict

    # -- per-segment ----------------------------------------------------
    def _classify_segment(self, segment: str, verdict: CommandVerdict) -> None:
        tokens = _safe_tokens(segment)
        if not tokens:
            return
        # Redirects (operate on the raw segment so we keep `>`/`>>`).
        self._scan_redirects(segment, verdict)

        core = _strip_wrappers_and_assignments(tokens)
        if not core:
            return
        program = _program_base(core[0])
        if verdict.leading is None:
            verdict.leading = program
        args = core[1:]
        low_args = [a.lower() for a in args]

        # version/help is always safe regardless of program.
        if any(a in ("--version", "-v", "-version", "--help", "-h", "-?", "/?") for a in low_args) and len(args) <= 2:
            verdict.reasons.append(f"'{program}' invoked for version/help only → safe")
            return

        # Egress host extraction.
        self._scan_egress(program, segment, verdict)

        # Destructive patterns (highest priority).
        if self._is_destructive_pattern(program, args, low_args, segment, verdict):
            return

        # SQL passed through a DB CLI.
        if _SQL_IRREVERSIBLE.search(segment):
            verdict.tier = _max_tier(verdict.tier, Tier.IRREVERSIBLE)
            verdict.irreversible = True
            verdict.reasons.append("embedded destructive SQL (DROP/TRUNCATE/DELETE/ALTER) → irreversible")
            return

        # cwd-escape detection.
        if program == "cd" and self._cd_escapes(args):
            verdict.escapes_jail = True
            verdict.reasons.append(f"`cd {' '.join(args)}` escapes the workspace jail → risky")
            return
        if program == "cd":
            verdict.reasons.append("cd within jail → safe")
            return

        tier = self._program_tier(program, args, low_args, verdict)
        verdict.tier = _max_tier(verdict.tier, tier)

    # -- program/subcommand → tier -------------------------------------
    def _program_tier(self, program: str, args: list[str], low_args: list[str],
                      verdict: CommandVerdict) -> Tier:
        # CLIs with read/write/destructive sub-verbs (git, dbt, aws, kubectl…).
        subverb = next((a for a in args if not a.startswith("-")), None)
        sv = subverb.lower() if subverb else None

        if sv is not None and (program in {"git", "dbt", "aws", "gcloud", "bq",
                                            "databricks", "az", "kubectl", "helm",
                                            "docker", "terraform", "gh", "snowsql"}):
            if sv in _IRREVERSIBLE_SUBVERBS:
                verdict.irreversible = True
                verdict.reasons.append(f"'{program} {sv}' is destructive → irreversible")
                return Tier.IRREVERSIBLE
            if sv in _RISKY_SUBVERBS:
                verdict.reasons.append(f"'{program} {sv}' mutates state → risky")
                return Tier.RISKY
            if sv in _WRITE_SUBVERBS:
                verdict.reasons.append(f"'{program} {sv}' creates recoverable state → write")
                return Tier.WRITE
            if sv in _READ_SUBVERBS:
                verdict.reasons.append(f"'{program} {sv}' is read-only → safe")
                return Tier.SAFE
            verdict.unknown = True
            verdict.reasons.append(f"'{program} {sv}' unrecognised sub-command → default-deny (risky)")
            return Tier.RISKY

        if program in _IRREVERSIBLE_PROGRAMS:
            verdict.irreversible = True
            verdict.reasons.append(f"'{program}' is inherently destructive → irreversible")
            return Tier.IRREVERSIBLE
        if program in _RISKY_PROGRAMS:
            # `sed -i` rewrites a file in place (an overwrite); plain sed is a filter.
            if program in {"sed", "perl", "awk"} and not any(a.startswith("-i") for a in args):
                verdict.reasons.append(f"'{program}' as a read filter → safe")
                return Tier.SAFE
            verdict.reasons.append(f"'{program}' mutates state → risky")
            return Tier.RISKY
        if program in _RISKY_RUNNERS:
            verdict.reasons.append(f"'{program}' runs arbitrary code (unbounded blast radius) → risky")
            return Tier.RISKY
        if program in _WRITE_PROGRAMS:
            self._note_write_target(program, args, verdict)
            verdict.reasons.append(f"'{program}' creates recoverable state → write")
            return Tier.WRITE
        if program in _SAFE_PROGRAMS:
            verdict.reasons.append(f"'{program}' is read-only → safe")
            return Tier.SAFE

        verdict.unknown = True
        verdict.reasons.append(f"unrecognised command '{program}' → default-deny (risky)")
        return Tier.RISKY

    # -- pattern detectors ---------------------------------------------
    def _is_destructive_pattern(self, program: str, args: list[str],
                                low_args: list[str], segment: str,
                                verdict: CommandVerdict) -> bool:
        joined = " ".join(low_args)
        # rm -rf / rm -r -f / rm --recursive --force
        if program in {"rm", "del", "erase", "rmdir", "rd"}:
            recursive = any(f in low_args for f in ("-r", "-rf", "-fr", "-r,", "--recursive", "/s", "-rd")) \
                or "-r" in joined.replace("-rf", "-r ").split()
            forced = any(f in low_args for f in ("-f", "-rf", "-fr", "--force", "/f", "/q"))
            combined = any(f in low_args for f in ("-rf", "-fr", "-rF", "-Rf"))
            recursive = recursive or combined
            forced = forced or combined
            targets = [a for a in args if not a.startswith("-") and not a.startswith("/")]
            verdict.deletes.extend(targets)
            if recursive or forced or program in {"rmdir", "rd"}:
                verdict.irreversible = True
                verdict.tier = _max_tier(verdict.tier, Tier.IRREVERSIBLE)
                verdict.reasons.append(f"recursive/forced delete (`{program} {' '.join(args)}`) "
                                       f"has no native undo → irreversible")
                return True
            # plain `rm file` — still destructive, treat as risky (rollback via copy-aside)
            verdict.tier = _max_tier(verdict.tier, Tier.RISKY)
            verdict.reasons.append(f"delete (`{program} {' '.join(args)}`) → risky (copy-aside rollback)")
            return True

        # git push --force / -f, git reset --hard, git clean -fd, git branch -D
        if program == "git":
            sv = next((a for a in args if not a.startswith("-")), "").lower()
            if sv == "push" and any(a in low_args for a in ("--force", "-f", "--force-with-lease")):
                verdict.irreversible = True
                verdict.tier = _max_tier(verdict.tier, Tier.IRREVERSIBLE)
                verdict.reasons.append("`git push --force` rewrites remote history → irreversible")
                return True
            if sv == "reset" and "--hard" in low_args:
                verdict.irreversible = True
                verdict.tier = _max_tier(verdict.tier, Tier.IRREVERSIBLE)
                verdict.reasons.append("`git reset --hard` discards working changes → irreversible")
                return True
            if sv == "clean" and any("f" in a for a in low_args if a.startswith("-")):
                verdict.irreversible = True
                verdict.tier = _max_tier(verdict.tier, Tier.IRREVERSIBLE)
                verdict.reasons.append("`git clean -f` deletes untracked files → irreversible")
                return True

        # chmod -R 777 / chown -R on broad targets
        if program in {"chmod", "chown"} and any(a in low_args for a in ("-r", "-rf", "--recursive")):
            verdict.tier = _max_tier(verdict.tier, Tier.RISKY)
            verdict.reasons.append(f"recursive `{program} -R` → risky")
            return True

        # `> /dev/...`, fork bombs handled by redirect/pattern scans elsewhere
        if ":(){" in segment.replace(" ", "") or ":|:&" in segment.replace(" ", ""):
            verdict.irreversible = True
            verdict.tier = _max_tier(verdict.tier, Tier.IRREVERSIBLE)
            verdict.reasons.append("fork bomb pattern → irreversible (refuse)")
            return True

        return False

    def _scan_redirects(self, segment: str, verdict: CommandVerdict) -> None:
        # Append (`>>`) is recoverable (write); truncate/overwrite (`>`) is risky.
        # Match `>>` first so it isn't seen as `>`.
        appends = re.findall(r">>\s*([^\s|&;<>]+)", segment)
        # Remove the `>>` matches before scanning for single `>`.
        no_append = re.sub(r">>\s*[^\s|&;<>]+", " ", segment)
        overwrites = re.findall(r"(?<![0-9])>\s*([^\s|&;<>]+)", no_append)
        for tgt in appends:
            verdict.writes.append(tgt)
            verdict.tier = _max_tier(verdict.tier, Tier.WRITE)
            verdict.reasons.append(f"append redirect to '{tgt}' → write")
        for tgt in overwrites:
            if tgt.startswith("/dev/") and tgt not in ("/dev/null", "/dev/stdout", "/dev/stderr"):
                verdict.irreversible = True
                verdict.tier = _max_tier(verdict.tier, Tier.IRREVERSIBLE)
                verdict.reasons.append(f"overwrite of device '{tgt}' → irreversible")
                continue
            if tgt in ("/dev/null", "nul", "NUL"):
                continue
            verdict.overwrites.append(tgt)
            verdict.tier = _max_tier(verdict.tier, Tier.RISKY)
            verdict.reasons.append(f"overwrite redirect to '{tgt}' (clobbers existing data) → risky")

    def _scan_egress(self, program: str, segment: str, verdict: CommandVerdict) -> None:
        if program not in _NET_PROGRAMS:
            return
        hosts = _HOST_RE.findall(segment)
        # ssh/scp host arg (user@host) when no URL.
        if not hosts and program in {"ssh", "scp", "sftp", "rsync"}:
            m = re.search(r"(?:[\w.-]+@)?([\w.-]+\.[\w.-]+)", segment)
            if m:
                hosts = [m.group(1)]
        for h in hosts:
            host = h.lower()
            if host not in verdict.egress_hosts:
                verdict.egress_hosts.append(host)

    def _note_write_target(self, program: str, args: list[str], verdict: CommandVerdict) -> None:
        targets = [a for a in args if not a.startswith("-")]
        if program in {"mkdir", "md", "touch"}:
            verdict.writes.extend(targets)
        elif program in {"cp", "copy"} and len(targets) >= 2:
            verdict.writes.append(targets[-1])

    @staticmethod
    def _cd_escapes(args: list[str]) -> bool:
        if not args:
            return False
        target = args[0].strip().strip('"').strip("'")
        if target.startswith("/") or re.match(r"^[A-Za-z]:[\\/]", target):
            return True
        # `cd ..` that pops above the jail root is caught here heuristically:
        # any path that begins by going up is treated as an escape attempt.
        return target == ".." or target.startswith("../") or target.startswith("..\\")

    def _host_allowed(self, host: str) -> bool:
        if self.network == "open":
            return True
        if self.network == "off":
            return False
        return any(host == a or host.endswith("." + a) or host.endswith(a) for a in self.allowlist)


def classify_command(
    command: str,
    *,
    network: str = "allowlist",
    egress_allowlist: list[str] | None = None,
) -> CommandVerdict:
    """Convenience wrapper: classify ``command`` with a one-off classifier."""
    return CommandClassifier(network=network, egress_allowlist=egress_allowlist).classify(command)
