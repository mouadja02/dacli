# Contributing to dacli

Thanks for your interest in dacli. This project is a *reliability-first* data-engineering agent, and the bar
for contributions reflects that. The one non-negotiable rule:

> **Scale skills and governance together.** Every new capability ships with its post-conditions, rollback
> strategy, permission scope, and golden task — or it does not ship. CI enforces this.

## Development setup

```bash
git clone https://github.com/mouadja02/dacli.git
cd dacli
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
# dacli is four wheels (M13); install all four editable, dev extras on the assembler.
pip install -e packages/dacli-ai -e packages/dacli-core \
            -e packages/dacli-tui -e "packages/dacli[dev]"
```

The editable install (`-e`) is required so the `dacli` command runs your working tree, not a stale
site-packages copy. For the exact pinned environment CI uses: `pip install -r requirements.lock`,
then the four wheels `--no-deps`.

## Before you open a PR

Run the same checks CI runs:

```bash
# 1. Lint
ruff check .

# 2. Connector Definition-of-Done gate (governance-debt guard)
python -m unittest tests.test_connector_dod -v

# 3. Full test suite (pytest — `unittest discover` silently skips bare-function tests)
pytest tests -q

# 4. Offline reliability suite (pass^k) against simulated platforms
python -m dacli.eval --quick

# 5. Docs drift gate (README badge / eval sample / command reference vs. reality)
python tools/check_docs.py
```

All five must pass. The `eval --quick` run exits non-zero on any unguarded destructive execution.

## Project conventions

- **No agent frameworks, no MCP.** Tools are plain Python/CLI the agent composes as code.
- **The kernel is platform-agnostic.** Anything platform-specific is a connector plugin. A change to add a
  platform should not touch `core/`, `reasoning/`, or `governance/`.
- **The environment is the oracle.** Verification anchors to native platform features (transactions,
  time travel, `dry_run`, clones, row counts, `dbt test`), not to the model's opinion.
- **Match the surrounding code.** Mirror the existing module's docstring style, naming, and comment density.
- Tests are offline and deterministic: CLI-first connectors are driven by an injected fake runner; never shell
  out to a live platform in a test.

## Adding a connector

See [docs/CONNECTORS.md](docs/CONNECTORS.md#adding-a-connector--checklist). In short: drop a folder with
`connector.py` + `manifest.yaml` + `SKILL.md`, register a rollback planner if it mutates state, add config to
`config/settings.py`, and add an offline golden test. The DoD gate verifies the rest.

## Adding to the eval suite

The golden suite is versioned code, reviewed and expanded over time. When you add a connector or a behavior,
add a corresponding golden task under `eval/golden/` (see [docs/EVALUATION.md](docs/EVALUATION.md)). Destructive
paths get a high pass^k bar; include adversarial/destructive-edge tasks deliberately.

## Commit & PR hygiene

- Keep PRs focused; one capability or fix per PR where practical.
- Write a clear PR description: what changed, why, and how you verified it (paste the test/eval output).
- Branch from `main`; do not commit directly to `main`.
- Don't commit secrets or local state — `config.yaml`, `.env`, `config/connectors.yaml`, and `.dacli/` are
  git-ignored for a reason.

## Releasing (maintainers)

Releases are **tag-driven and automated** — see **[RELEASING.md](RELEASING.md)** for the full process
(including the one-time PyPI trusted-publisher setup). The short version:

```bash
python tools/bump_version.py minor      # single-sourced version literal
git commit -am "release: v0.2.0"
git tag -a v0.2.0 -m "v0.2.0" && git push --follow-tags
```

Pushing the `vX.Y.Z` tag runs [`.github/workflows/release.yml`](.github/workflows/release.yml), which
re-runs the full CI gate, guards that the tag matches `dacli.__version__`, builds + `twine check`s the
sdist/wheel, creates a GitHub Release with auto-generated notes, and publishes to PyPI from the protected
`pypi` environment. The PyPI publish can require a maintainer's approval via that environment's reviewers.

## Reporting issues

Open a GitHub issue with: what you expected, what happened, the relevant config (secrets redacted), and — if a
governance decision is involved — the output of `dacli audit --full` for the session.
