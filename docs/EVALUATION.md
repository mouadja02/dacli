# Evaluation & reliability

dacli *proves* reliability instead of asserting it. The `eval/` package measures the whole harness against
versioned **golden task suites** and reports **pass^k** — consistency across *repeated* rollouts, not
single-shot luck.

> The sharpest warning in the harness literature is **pass^k collapse**: agents that ace a demo and flake over
> repeated runs. For a data agent, a 95%-reliable `DROP`-guard is a catastrophe waiting for its 1-in-20.
> Single-shot benchmarks lie; we measure consistency.

```bash
dacli eval                 # full, stakes-tiered run + dashboard
dacli eval --quick         # fast run (scaled-down k) — what CI runs on each PR
dacli eval --regression    # also diff against the previous run in history
dacli eval --calibrate     # print data-driven threshold recommendations
dacli eval --json          # machine-readable output

python -m dacli.eval --quick     # equivalent module entrypoint
```

Everything here is **offline**: deterministic simulated platforms, no credentials, no network, no cost.

---

## pass^k — the headline metric

- **pass@1** answers *"did it work once?"*
- **pass^k** answers *"does it work on **every** one of k independent rollouts?"* — the τ-bench-style
  consistency metric, and dacli's true reliability KPI.

```
Agent A: pass@1 = 0.90, pass^10 = 0.35   ← high peak, low consistency
Agent B: pass@1 = 0.75, pass^10 = 0.65   ← the reliable one in production
```

`k` is **tiered by stakes**: read-only tasks run at low `k`, write tasks higher, and **destructive/governance
tasks at the highest bar** (the cheap sim suite runs often; expensive high-k runs are reserved for where a
1-in-20 flake is catastrophic).

---

## What's in the suite — `eval/golden/`

| Family | Tasks |
|---|---|
| **Per-connector** | The Wave-1 CLI connectors (S3, GCS, BigQuery, Databricks) run *concretely* against the simulator: a real op executes and its real, environment-anchored post-conditions verify the outcome. **Every** discovered connector additionally gets a structural golden task that runs its Definition-of-Done checks. |
| **Spine behaviors** | The core harness mechanisms: the destructive-action gate (𝒢), an anchored post-condition catching confident-but-unchecked output (𝒮), routing accuracy (𝒮/𝒪), bounded informed self-correction (𝒪), and demotion of stale-but-confident memory (ℳ). |

The headline task, `spine.drop_guard`: an irreversible S3 delete with bucket versioning **off** (so the
rollback path cannot be verified) must be **BLOCKED on every rollout**, with zero unguarded executions.

---

## Simulated platforms — `eval/sim/`

The inner loop runs against deterministic, offline stand-ins so the suite is cheap, safe, and repeatable in
CI. A simulated platform is a programmable responder over the connectors' injectable CLI runner — the
connector's *real* post-conditions run unchanged against the fake. Responders can be made **seeded-flaky** so
pass^k measures genuine consistency, and support **error injection** for regression tests. Live-sandbox runs
reconcile sim vs. reality at milestones; a divergence is treated as a sim bug to fix.

---

## Regression detection — `eval/regression.py`

Net improvement can mask important regressions, so dacli compares each run against history and flags the
*shape* of failures, not just the count:

- **New failures** — a task that used to pass^k and now doesn't.
- **Earlier-failure recurrence** — a task now failing at an *earlier* step than before (degradation a rolling
  average hides).
- **Cost / latency drift** — the task got more expensive or slower.
- **Unguarded executions** — any destructive action that ran without a gate (a hard, never-tolerated failure).

`dacli eval --regression` exits non-zero when a regression is detected, so CI fails loudly.

---

## The reliability dashboard — `eval/dashboard.py`

Per connector/skill and overall: success rate, **pass^k**, escalation rate, self-correction rate,
governance-interrupt rate, and tokens/latency per task.

```text
Reliability dashboard — suite: sim
----------------------------------------------------------------------------------------------
connector          tasks  pass@1  pass^k   succ    esc   corr    gov  unguard     tok       ms
----------------------------------------------------------------------------------------------
bigquery               3    1.00    1.00   1.00   0.00   0.00   0.00        0       0      0.1
databricks             2    1.00    1.00   1.00   0.00   0.00   0.00        0       0      0.1
s3                     3    1.00    1.00   1.00   0.00   0.00   0.00        0       0      0.1
spine                  5    1.00    1.00   1.00   0.00   0.20   0.20        0       0      1.0
...
OVERALL               26    1.00    1.00   1.00   0.00   0.04   0.04        0       0     29.1
----------------------------------------------------------------------------------------------
✓ zero unguarded destructive executions.
```

---

## Self-improvement, gated by eval — `eval/selfimprove.py`

Successful task traces (episodic memory) are distilled into parameterized **procedural runbooks** the router
can reuse — raising reliability and cutting tokens on recurring work. The capstone guarantee:

> A runbook is **promoted only if it measurably beats the ad-hoc path on the golden suite (pass^k)**.

No unvetted "learning" enters the trusted path; the comparison is recorded in the audit ledger so the
promotion is auditable and revocable. This is the defense against the system "learning" a
subtly-wrong-but-passing shortcut.

---

## Calibration feedback — `eval/calibration.py`

Eval output feeds back into the tunable thresholds so calibration is **data-driven, not guessed**: the
router's `min_confidence`, the memory staleness horizon, and governance tier overrides. `dacli eval
--calibrate` emits concrete, documented recommendations — it never silently mutates config.

---

## What the suite guarantees

- ✅ Every connector/skill has a golden suite with machine-verifiable outcomes; CI runs the sim suite per PR.
- ✅ pass^k is reported per task; the destructive-action gate holds across `k` runs with **zero** unguarded
  executions.
- ✅ Regression detection flags a deliberately-introduced degradation, including earlier-failure recurrence.
- ✅ The dashboard surfaces success, pass^k, cost, latency, escalation, and correction rates.
- ✅ At least one episodic trace is distilled into a runbook that beats the ad-hoc path on pass^k before
  promotion, with the comparison recorded in the audit ledger.
- ✅ Threshold calibration is driven by eval output and documented.
