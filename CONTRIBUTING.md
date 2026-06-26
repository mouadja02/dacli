# Contributing to dacli

dacli is a data-engineering agent — a thin extension host over a governed core.
One rule:
> **Governance and verification are core, even for generated code.** Every tool — bundled,
> generated, or installed — declares a blast-radius tier and at least one environment-anchored
> post-condition, or it doesn't register. CI enforces it.

## Development setup

```bash
git clone https://github.com/mouadja02/dacli.git
cd dacli
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
# dacli is four wheels; install all four editable, dev extras on the assembler.
pip install -e packages/dacli-ai -e packages/dacli-core \
            -e packages/dacli-tui -e "packages/dacli[dev]"
```

The editable install (`-e`) is required so the `dacli` command runs your working tree, not a
stale site-packages copy. For the pinned environment CI uses: `pip install -r requirements.lock`,
then the four wheels `--no-deps`.

## Before you open a PR

```bash
ruff check .
pytest tests -q
dacli eval --quick          # offline pass^k suite — non-zero on any unguarded destructive run
python tools/check_docs.py  # docs drift gate (tests badge / eval sample / command reference)
```

All four must pass.

## Project conventions

- **No agent frameworks, no MCP.** Tools are plain Python the agent composes.
- **The core is platform-agnostic.** Anything platform-specific is a `register(api)` extension,
  not a change to `core/`, the `dacli-ai` provider layer, or `governance/`.
- **The environment is the oracle.** Verification anchors to native platform features
  (transactions, time travel, `dry_run`, clones, row counts, `dbt test`), not the model's opinion.
- **Match the surrounding code.** Mirror the existing module's docstring style, naming, and
  comment density. Comments explain *why*, not *what*.
- Tests are offline and deterministic: CLI-first connectors are driven by an injected fake
  runner; never shell out to a live platform in a test.

## Adding a capability

A connector, a tool, a slash command, a theme — all of them are extensions. Write a Python module
exporting `register(api)`; declare each tool's `risk` and `postconditions` (the registry refuses a
tool with neither). The agent generates these the same way you'd write one by hand. The full
contract — the ExtensionAPI surface, lifecycle events, validation — is **[docs/EXTENSIONS.md](docs/EXTENSIONS.md)**.

A skill is a `SKILL.md` doc under `skills/<name>/`: a method the agent reads on demand. Add one to
the seeds (`packages/dacli/src/dacli/seeds/skills/`) or your `~/.dacli/skills/`.

## Adding to the eval suite

The golden suite under `eval/golden/` is versioned, reviewed code. When you add a capability, add a
golden task (see [docs/EVALUATION.md](docs/EVALUATION.md)). Destructive paths get a high `pass^k`
bar; include adversarial/destructive-edge tasks deliberately.

## Commit & PR hygiene

- One capability or fix per PR where practical.
- Write a clear PR description: what changed, why, and how you verified it (paste test/eval output).
- Branch from `main`; don't commit to `main`.
- Don't commit secrets or local state — `config.yaml`, `.env`, and `.dacli/` are git-ignored.

## Releasing (maintainers)

Releases are tag-driven and automated — see **[RELEASING.md](RELEASING.md)**. The short version:

```bash
python tools/bump_version.py minor
git commit -am "release: v0.3.0"
git tag -a v0.3.0 -m "v0.3.0" && git push --follow-tags
```

Pushing the `vX.Y.Z` tag re-runs the CI gate, guards the tag against `dacli.__version__`, builds
and `twine check`s the artifacts, creates a GitHub Release, and publishes to PyPI from the
protected `pypi` environment.

## Reporting issues

Open a GitHub issue with: what you expected, what happened, the relevant config (secrets redacted),
and — if a governance decision is involved — the output of `dacli audit --full` for the session.
