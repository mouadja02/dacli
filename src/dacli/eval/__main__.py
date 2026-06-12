"""``python -m eval`` — run the sim golden suite and print the dashboard.

This is the entry CI uses on each PR (``--quick`` for a fast, low-k run). It is
fully offline: simulated platforms, no credentials, no network, no cost.

    python -m eval                 # full stakes-tiered run + dashboard
    python -m eval --quick         # fast CI run (scaled-down k)
    python -m eval --regression    # also diff against the previous run
    python -m eval --json          # machine-readable dashboard
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from dacli.eval.golden import build_golden_suite
from dacli.eval.harness import EvalHarness
from dacli.eval.dashboard import Dashboard
from dacli.eval.regression import compare
from dacli.eval.calibration import calibrate


async def _main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="eval", description="dacli reliability eval (pass^k)")
    parser.add_argument("--quick", action="store_true",
                        help="fast CI run: scale k down (destructive stays ≥2)")
    parser.add_argument("--k-scale", type=float, default=None,
                        help="explicit k multiplier (overrides --quick)")
    parser.add_argument("--regression", action="store_true",
                        help="diff against the previous run in history")
    parser.add_argument("--calibrate", action="store_true",
                        help="print data-driven threshold recommendations")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("--report", default=None, metavar="PATH",
                        help="write a shareable reliability report; format inferred "
                             "from the extension (.md / .html)")
    parser.add_argument("--no-persist", action="store_true",
                        help="do not append this run to history")
    parser.add_argument("--history", default=".dacli/eval/history.jsonl")
    args = parser.parse_args(argv)

    k_scale = args.k_scale if args.k_scale is not None else (0.4 if args.quick else 1.0)
    harness = EvalHarness(history_path=args.history, k_scale=k_scale)

    prev = harness.history.latest()  # baseline before we append this run
    suite = build_golden_suite()
    report = await harness.run_suite("sim", suite, persist=not args.no_persist)

    dashboard = Dashboard.from_report(report)
    regression = compare(prev, report) if (args.regression and prev is not None) else None
    recommendation = calibrate(report) if args.calibrate else None

    if args.report:
        from dacli.eval.report import write_report

        target = write_report(args.report, dashboard, regression)
        print(f"report written to {target}", file=sys.stderr)

    if args.json:
        out = {"dashboard": dashboard.to_dict()}
        if regression is not None:
            out["regression"] = regression.to_dict()
        if recommendation is not None:
            out["calibration"] = recommendation.to_dict()
        print(json.dumps(out, indent=2, default=str))
    else:
        print(dashboard.render())
        print(f"\nsuite pass^k: {report.pass_k:.2%}  ·  pass@1: {report.pass_at_1:.2%}  "
              f"·  tasks: {len(report.results)}")
        if regression is not None:
            print("\nRegression vs. previous run:")
            print("  " + regression.summary())
        if recommendation is not None:
            print("\n" + recommendation.to_markdown())

    # Exit non-zero if a destructive action ran unguarded, or (when comparing) a
    # regression was detected — so CI fails loudly.
    if report.total_unguarded_executions > 0:
        print("\nFAIL: unguarded destructive execution(s) detected.", file=sys.stderr)
        return 2
    if regression is not None and regression.regressed:
        print("\nFAIL: reliability regression detected.", file=sys.stderr)
        return 1
    return 0


def main(argv=None) -> int:
    return asyncio.run(_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
