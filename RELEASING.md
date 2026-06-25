# Releasing dacli

Releases are **tag-driven**. Pushing a `vX.Y.Z` tag triggers
[`.github/workflows/release.yml`](.github/workflows/release.yml), which gates the
build, creates a GitHub Release, and publishes to PyPI. There is no manual
artifact upload step.

dacli follows [Semantic Versioning](https://semver.org): `MAJOR.MINOR.PATCH`.

---

## The version — one literal per wheel, bumped in lockstep

Since M13 dacli is four wheels, each single-sourcing its own `__version__`:

- `packages/dacli-ai/src/dacli/ai/__init__.py`
- `packages/dacli-core/src/dacli/core/__init__.py`
- `packages/dacli-tui/src/dacli/tui/__init__.py`
- `packages/dacli/src/dacli/scripts/__init__.py`

Each `pyproject.toml` reads its literal at build time
(`[tool.setuptools.dynamic] version = {attr = "..."}`). The four release together,
so the helper bumps all four to the same version (and fails on drift):

```bash
python tools/bump_version.py --show     # current version
python tools/bump_version.py patch      # 0.1.0 -> 0.1.1
python tools/bump_version.py minor      # 0.1.3 -> 0.2.0
python tools/bump_version.py major      # 0.2.1 -> 1.0.0
python tools/bump_version.py 0.4.0      # or set an exact version
```

---

## Cutting a release

1. **Land everything on `main`.** Releases are cut from `main` with CI green.

2. **Bump, commit, tag, push:**

   ```bash
   python tools/bump_version.py minor          # e.g. 0.1.0 -> 0.2.0
   git commit -am "release: v0.2.0"
   git tag -a v0.2.0 -m "v0.2.0"
   git push --follow-tags
   ```

   The tag **must** match the bumped version (`v0.2.0` ↔ `__version__ == "0.2.0"`);
   the workflow's first gate fails loudly otherwise.

3. **Watch the workflow.** `Release` runs on the tag and:
   - re-runs the CI gate (ruff, full test suite, connector DoD, docs-drift, the
     `pass^k` eval, and the headless smoke);
   - builds the sdist + wheel and `twine check`s them;
   - creates a **GitHub Release** with notes auto-generated from the PRs merged
     since the previous tag, with the artifacts attached;
   - publishes to **PyPI** from the protected `pypi` environment.

4. **Verify:** the release shows up at
   `https://github.com/mouadja02/dacli/releases` and
   `https://pypi.org/project/dacli/`, and `pip install dacli==0.2.0` works.

### Pre-releases

Tag with a PEP 440 suffix to ship a pre-release (e.g. `v0.2.0rc1`). The tag/version
guard still applies, so set the literal to match (`python tools/bump_version.py 0.2.0rc1`).
GitHub marks suffixed tags as pre-releases automatically.

---

## One-time PyPI setup (trusted publishing)

The workflow publishes with **PyPI Trusted Publishing** (OIDC) — there is **no API
token** stored in the repo. Configure it once:

1. **Create a GitHub environment** named `pypi`
   (repo → *Settings* → *Environments* → *New environment*). Optionally add
   *Required reviewers* so a human approves each publish, and restrict it to the
   `main` branch / tags.

2. **Register the trusted publisher on PyPI.** If the project does not exist yet,
   use a *pending publisher* (PyPI → *Your projects* → *Publishing* →
   *Add a pending publisher*); otherwise the project's *Settings → Publishing*.
   Fill in:

   | Field | Value |
   |---|---|
   | PyPI Project Name | `dacli` |
   | Owner | `mouadja02` |
   | Repository name | `dacli` |
   | Workflow name | `release.yml` |
   | Environment name | `pypi` |

3. Push your first `vX.Y.Z` tag. The first publish creates the project on PyPI
   and binds the trusted publisher; subsequent releases need no further setup.

> Switching to **TestPyPI** first? Point the `pypi-publish` job at it by adding
> `repository-url: https://test.pypi.org/legacy/` to the
> `pypa/gh-action-pypi-publish` step and register the publisher on
> `test.pypi.org` the same way.

---

## If a release goes wrong

- **Never re-tag a published version.** PyPI refuses to overwrite an existing
  version, and moving a Git tag confuses everyone who already fetched it. Bump to
  the next patch (`vX.Y.Z+1`) and release again.
- A failed gate means **nothing shipped** — the GitHub Release and PyPI publish
  jobs only run after `build` succeeds. Fix forward on `main`, then re-tag with a
  new version.
- To pull a bad GitHub Release, delete it from the *Releases* page (the tag can
  stay); a bad PyPI version can only be *yanked*, not replaced.
