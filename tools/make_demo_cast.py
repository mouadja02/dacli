#!/usr/bin/env python3
"""Generate assets/demo.cast — a deterministic asciicast of dacli's offline surfaces.

Runs a fixed list of no-credential commands (`plan`, `eval --quick`, `doctor`),
captures their real output, and weaves it into an asciicast v2 stream with a typed
shell prompt. No API key, no network — regenerable by anyone:

    python tools/make_demo_cast.py
    npx --yes svg-term-cli --in assets/demo.cast --out assets/demo.svg --window --no-cursor

The home directory is scrubbed so the recording carries no local paths.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CAST = ROOT / "assets" / "demo.cast"
COLS, ROWS = 98, 31
PROMPT = "\033[1;32mcrm-warehouse\033[0m \033[1;34m$\033[0m "

# (command argv, caption shown above the command, seconds to hold the result)
STEPS = [
    (["dacli", "plan", "drop table prod.users"],
     "every action is tiered by blast radius — before anything runs", 6.5),
    (["dacli", "plan",
      "load CSVs into Snowflake RAW, build CORE silver and MART gold, "
      "then drop MART.DAILY_REVENUE"],
     "a plain-language goal decomposes into a governed plan", 8.5),
    (["dacli", "eval", "--quick"],
     "reliability is measured across repeated rollouts (pass^k), not one lucky run", 9.5),
    (["dacli", "doctor"],
     "where config, state, and the LLM key resolve — the key value never printed", 7.0),
]


def capture(argv):
    env = {**os.environ, "COLUMNS": str(COLS), "LINES": str(ROWS), "NO_COLOR": "1"}
    out = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, text=True).stdout
    home = str(Path.home())
    return out.replace(home, "~").replace(home.replace("\\", "/"), "~").replace("\\", "/")


def main():
    events = []
    t = 0.4

    def emit(data, dt=0.0):
        nonlocal t
        t += dt
        events.append([round(t, 3), "o", data])

    emit("\033[2J\033[3J\033[H", 0.6)
    emit("\033[1;36m  dacli\033[0m \033[2m— a data-engineering agent for the terminal\033[0m\r\n\r\n", 0.3)
    emit("  plans work, runs governed tool calls, verifies against the platform.\r\n", 0.4)
    emit("  \033[2meverything below is offline: no API key, no network, no credentials.\033[0m\r\n", 0.4)
    emit("", 4.0)

    for argv, caption, hold in STEPS:
        emit("\033[2J\033[3J\033[H", 0.6)          # clear screen + scrollback
        emit(f"\033[2m# {caption}\033[0m\r\n", 0.5)
        emit(PROMPT, 0.3)
        shown = " ".join(a if " " not in a else f'"{a}"' for a in argv)
        for ch in shown:                            # type the command
            emit(ch, 0.045)
        emit("\r\n", 0.35)                          # Enter
        body = capture(argv).replace("\n", "\r\n")
        emit(body, 0.5)
        emit("", hold)                              # hold

    emit("\033[2J\033[3J\033[H", 0.6)
    emit(PROMPT + "\033[2m# governed, verified, reproducible — github.com/mouadja02/dacli\033[0m\r\n", 0.3)
    emit("", 3.5)

    header = {"version": 2, "width": COLS, "height": ROWS,
              "env": {"SHELL": "/bin/bash", "TERM": "xterm-256color"}}
    CAST.parent.mkdir(exist_ok=True)
    with CAST.open("w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(header) + "\n")
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    print(f"wrote {CAST}  ({len(events)} events, {events[-1][0]:.1f}s)")


if __name__ == "__main__":
    sys.exit(main())
