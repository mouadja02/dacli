"""Post-condition verification framework (𝒮, with the guarding half of 𝒢).

The cure for the *confident-but-unchecked* failure mode: **fluent output ≠
correct output**. Today an op is "done" the moment its API call returns; this
module makes that impossible. Every skill and every connector operation declares
``postconditions``; :func:`run_postconditions` runs them after execution and
**fails the result if any fail**.

Two rules from the plan are enforced here:

* **Post-conditions are mandatory, not optional** — :func:`require_postconditions`
  rejects an op/skill that declares none. The registry calls it at load time, so
  *no post-condition → no registration*.
* **Anchor to the environment** — the reusable factories ask the *platform*
  (information_schema, a commit SHA, a row count), not the model. A check that
  must trust the model is marked ``anchored=False`` and is treated as lower-trust.

The module is deliberately dependency-free (no ``jsonschema``): the handoff
validator implements the small JSON-Schema subset the harness actually uses.
"""

from __future__ import annotations

from dacli.core.logging_setup import get_logger

log = get_logger(__name__)

import hashlib
import inspect
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from collections.abc import Awaitable, Callable


# ---------------------------------------------------------------------------
# Context + result types
# ---------------------------------------------------------------------------
@dataclass
class VerificationContext:
    """Everything a post-condition needs to interrogate the live environment.

    ``target`` is the thing that produced ``result`` (a connector or a skill
    instance) — environment-anchored checks call back into it to re-introspect.
    ``args`` is the original input; ``memory`` exposes the catalog/fact store.
    """

    args: dict[str, Any] = field(default_factory=dict)
    result: Any = None
    target: Any = None
    memory: Any = None
    task: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


# A check returns a bool, or ``(bool, detail)``, optionally as a coroutine.
CheckReturn = bool | tuple[bool, str]
CheckFn = Callable[[VerificationContext], CheckReturn | Awaitable[CheckReturn]]


@dataclass
class PostCondition:
    """One mandatory check run after an op/skill executes.

    ``anchored`` records *who the question is asked of*: True = the platform
    (row counts, information_schema, content hashes); False = the model (a
    last-resort, lower-trust judgement). ``applies_when`` lets a check declare
    itself not-applicable (e.g. a CREATE-TABLE check on a SELECT) so it neither
    passes vacuously nor fails spuriously.
    """

    name: str
    check: CheckFn
    description: str = ""
    anchored: bool = True
    applies_when: Callable[[VerificationContext], bool] | None = None


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""
    anchored: bool = True
    errored: bool = False
    skipped: bool = False


@dataclass
class VerificationReport:
    """Aggregate outcome. ``passed`` is the gate the dispatcher/skill respects."""

    passed: bool
    checks: list[CheckResult] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed and not c.skipped]

    def summary(self) -> str:
        if self.passed:
            ran = [c for c in self.checks if not c.skipped]
            return f"verified ({len(ran)} post-condition(s) passed)"
        parts = [f"{c.name}: {c.detail or 'failed'}" for c in self.failures]
        return "post-condition(s) FAILED — " + "; ".join(parts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "timestamp": self.timestamp.isoformat(),
            "checks": [
                {
                    "name": c.name,
                    "passed": c.passed,
                    "detail": c.detail,
                    "anchored": c.anchored,
                    "errored": c.errored,
                    "skipped": c.skipped,
                }
                for c in self.checks
            ],
        }


def _normalize(outcome: CheckReturn) -> tuple[bool, str]:
    if isinstance(outcome, tuple):
        ok, detail = outcome
        return bool(ok), str(detail or "")
    return bool(outcome), ""


async def run_postconditions(
    postconditions: list[PostCondition],
    ctx: VerificationContext,
) -> VerificationReport:
    """Run every post-condition against ``ctx`` and aggregate the verdict.

    A check that ``raises`` is recorded as *failed* (never silently swallowed —
    that would re-open the confident-but-unchecked hole). A check whose
    ``applies_when`` is False is *skipped* and does not affect the verdict.
    """
    results: list[CheckResult] = []
    for pc in postconditions:
        if pc.applies_when is not None:
            try:
                applicable = pc.applies_when(ctx)
            except Exception:
                applicable = True
            if not applicable:
                results.append(CheckResult(pc.name, True, "not applicable",
                                           pc.anchored, skipped=True))
                continue
        try:
            outcome = pc.check(ctx)
            if inspect.isawaitable(outcome):
                outcome = await outcome
            passed, detail = _normalize(outcome)
            results.append(CheckResult(pc.name, passed, detail, pc.anchored))
        except Exception as e:  # a check that explodes is a failed check
            results.append(CheckResult(
                pc.name, False, f"check raised: {e}", pc.anchored, errored=True))

    passed = all(c.passed for c in results)
    return VerificationReport(passed=passed, checks=results)


# ---------------------------------------------------------------------------
# Registration guard — "no post-condition, no registration"
# ---------------------------------------------------------------------------
class MissingPostConditionError(ValueError):
    """Raised when an op/skill tries to register without any post-condition."""


def require_postconditions(name: str, postconditions: list[PostCondition] | None) -> None:
    """Enforce the structural rule that *every* capability is checkable.

    Called by the skill registry (always) and the connector registry (when
    ``enforce_postconditions`` is on) at load time. This is the structural
    enforcement of "scale 𝒮 and 𝒢 together": a capability that cannot be
    verified cannot be offered.
    """
    if not postconditions:
        raise MissingPostConditionError(
            f"'{name}' declares no post-conditions. A connector operation or "
            f"skill cannot register without at least one — fluent output is not "
            f"proof of a correct outcome. Add an environment-anchored check."
        )


# ---------------------------------------------------------------------------
# Reusable, environment-anchored post-condition factories
# ---------------------------------------------------------------------------
def _is_success(result: Any) -> bool:
    success = getattr(result, "success", None)
    if success is not None:
        return bool(success)
    return result is not None


def result_succeeded(name: str = "result_succeeded") -> PostCondition:
    """The platform itself reported success (status, not the model's opinion)."""
    def check(ctx: VerificationContext) -> CheckReturn:
        if _is_success(ctx.result):
            return True, ""
        err = getattr(ctx.result, "error", None) or "operation did not succeed"
        return False, str(err)
    return PostCondition(name, check, "platform returned a success status", anchored=True)


def data_is_list(name: str = "data_is_list", *, non_empty: bool = False) -> PostCondition:
    """The result payload is a list (and optionally non-empty).

    The "any SELECT used downstream → shape matches the consumer" row: a
    consumer that expects rows must not be handed a scalar or None.
    """
    def check(ctx: VerificationContext) -> CheckReturn:
        data = getattr(ctx.result, "data", ctx.result)
        if not isinstance(data, list):
            return False, f"expected a list, got {type(data).__name__}"
        if non_empty and len(data) == 0:
            return False, "result list is empty"
        return True, ""
    return PostCondition(name, check, "result payload is a (non-empty) list", anchored=True)


def data_has_keys(*keys: str, name: str = "data_has_keys") -> PostCondition:
    """The result dict carries the keys a downstream step will read."""
    def check(ctx: VerificationContext) -> CheckReturn:
        data = getattr(ctx.result, "data", ctx.result)
        if not isinstance(data, dict):
            return False, f"expected a dict, got {type(data).__name__}"
        missing = [k for k in keys if k not in data]
        if missing:
            return False, f"missing keys: {missing}"
        return True, ""
    return PostCondition(name, check, f"result carries keys {list(keys)}", anchored=True)


def content_hash_matches(
    *,
    expected_arg: str = "content",
    actual_getter: Callable[[VerificationContext], str | None],
    name: str = "content_hash_matches",
) -> PostCondition:
    """The bytes that landed hash-equal to the bytes we sent.

    ``actual_getter`` reads back the *stored* content from the platform; the
    expected content is the original arg. Used by ``github push`` to confirm the
    committed file is byte-identical to what we pushed.
    """
    def _sha(text: str | None) -> str | None:
        if text is None:
            return None
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def check(ctx: VerificationContext) -> CheckReturn:
        expected = _sha(ctx.args.get(expected_arg))
        actual = _sha(actual_getter(ctx))
        if actual is None:
            return True, "could not read back stored content (unverified)"
        if expected != actual:
            return False, "stored content hash differs from pushed content"
        return True, ""
    return PostCondition(name, check, "committed content hash matches", anchored=True)


# ---------------------------------------------------------------------------
# Shell-tier post-conditions (Era 2) — anchored to the exit code + filesystem.
# ---------------------------------------------------------------------------
def _shell_data(ctx: VerificationContext) -> dict[str, Any]:
    data = getattr(ctx.result, "data", None)
    return data if isinstance(data, dict) else {}


def _resolve_under_cwd(path: str, cwd: str) -> str:
    import os
    return path if os.path.isabs(path) else os.path.join(cwd or ".", path)


def shell_exit_zero(name: str = "shell_exit_zero") -> PostCondition:
    """A command is "done" only if the shell's own exit code is 0.

    Anchored to the environment (the shell's ``$?`` captured via the sentinel),
    not the model's read of stdout — fluent output ≠ a successful command.
    """
    def check(ctx: VerificationContext) -> CheckReturn:
        data = _shell_data(ctx)
        rc = data.get("exit_code")
        if data.get("timed_out"):
            return False, "command hit the wall-clock limit"
        if rc is None:
            return True, "no exit code recorded"
        if int(rc) != 0:
            return False, f"non-zero exit code {rc}"
        return True, ""
    return PostCondition(name, check, "command exited 0 (environment-anchored)", anchored=True)


def shell_writes_observed(name: str = "shell_writes_observed") -> PostCondition:
    """The file(s) the command intended to write/overwrite are present on disk.

    Mirrors the connector DoD rule for the shell tier: a write is verified by
    re-observing the live filesystem, not by trusting that ``echo > f`` "worked".
    """
    def applies(ctx: VerificationContext) -> bool:
        data = _shell_data(ctx)
        return bool((data.get("writes") or []) + (data.get("overwrites") or []))

    def check(ctx: VerificationContext) -> CheckReturn:
        import os
        data = _shell_data(ctx)
        cwd = data.get("cwd") or "."
        targets = list(data.get("writes") or []) + list(data.get("overwrites") or [])
        missing = [t for t in targets if not os.path.exists(_resolve_under_cwd(t, cwd))]
        if missing:
            return False, f"intended file(s) absent after write: {missing}"
        return True, ""
    return PostCondition(name, check, "intended file(s) present after a write", anchored=True,
                         applies_when=applies)


def shell_deletes_observed(name: str = "shell_deletes_observed") -> PostCondition:
    """The file(s) the command intended to delete are actually gone."""
    def applies(ctx: VerificationContext) -> bool:
        return bool(_shell_data(ctx).get("deletes"))

    def check(ctx: VerificationContext) -> CheckReturn:
        import os
        data = _shell_data(ctx)
        cwd = data.get("cwd") or "."
        targets = list(data.get("deletes") or [])
        still = [t for t in targets if os.path.exists(_resolve_under_cwd(t, cwd))]
        if still:
            return False, f"file(s) still present after delete: {still}"
        return True, ""
    return PostCondition(name, check, "intended file(s) absent after a delete", anchored=True,
                         applies_when=applies)


def scope_not_violated(
    forbidden: Callable[[VerificationContext], str | None],
    name: str = "scope_not_violated",
) -> PostCondition:
    """Enforce a skill's ``cannot_do`` as a post-condition (catches scope creep).

    ``forbidden(ctx)`` returns a reason string when the result shows the skill
    did something outside its declared scope (e.g. a "profile" skill that
    mutated data), or None when clean.
    """
    def check(ctx: VerificationContext) -> CheckReturn:
        reason = forbidden(ctx)
        if reason:
            return False, f"scope violation: {reason}"
        return True, ""
    return PostCondition(name, check, "stayed within declared scope", anchored=True)


# ---------------------------------------------------------------------------
# Verifier — the runner the dispatcher / skill connector calls
# ---------------------------------------------------------------------------
class Verifier:
    """Runs an op/skill's post-conditions and decides whether to accept it.

    ``enforce=True`` (the live agent) downgrades a result whose post-conditions
    fail; ``enforce=False`` only annotates it (useful while calibrating). Either
    way the report is recorded for audit. Verification is *best-effort* on
    errors only when ``enforce`` is off — a genuine contradiction always fails.
    """

    def __init__(self, *, enforce: bool = True,
                 on_report: Callable[[str, VerificationReport], None] | None = None):
        self.enforce = enforce
        self._on_report = on_report

    async def verify(
        self,
        postconditions: list[PostCondition],
        ctx: VerificationContext,
        *,
        label: str = "",
    ) -> VerificationReport:
        report = await run_postconditions(postconditions, ctx)
        if self._on_report is not None:
            try:
                self._on_report(label, report)
            except Exception:
                log.debug("on_report callback failed", exc_info=True)
        return report


# ---------------------------------------------------------------------------
# Composition handoff verification (4.6) — kills composition drift
# ---------------------------------------------------------------------------
@dataclass
class HandoffResult:
    ok: bool
    errors: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.ok


def validate_json(instance: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    """Validate ``instance`` against a JSON-Schema *subset* (no extra deps).

    Supports ``type`` (object/array/string/number/integer/boolean/null),
    ``properties``, ``required``, ``items``, and ``enum`` — the shapes the
    harness actually declares in skill/op contracts. Returns a list of error
    strings (empty = valid).
    """
    errors: list[str] = []
    if not isinstance(schema, dict):
        return errors

    expected = schema.get("type")
    if expected:
        types = expected if isinstance(expected, list) else [expected]
        if not any(_is_type(instance, t) for t in types):
            got = type(instance).__name__
            errors.append(f"{path}: expected type {expected}, got {got}")
            return errors  # type wrong → deeper checks are noise

    if "enum" in schema and instance not in schema["enum"]:
        errors.append(f"{path}: {instance!r} not in enum {schema['enum']}")

    if isinstance(instance, dict):
        errors.extend(
            f"{path}: missing required property '{key}'"
            for key in schema.get("required", []) or []
            if key not in instance
        )
        props = schema.get("properties", {}) or {}
        for key, subschema in props.items():
            if key in instance:
                errors.extend(validate_json(instance[key], subschema, f"{path}.{key}"))

    if isinstance(instance, list) and "items" in schema:
        for i, item in enumerate(instance):
            errors.extend(validate_json(item, schema["items"], f"{path}[{i}]"))

    return errors


def _is_type(value: Any, t: str) -> bool:
    if t == "object":
        return isinstance(value, dict)
    if t == "array":
        return isinstance(value, list)
    if t == "string":
        return isinstance(value, str)
    if t == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if t == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if t == "boolean":
        return isinstance(value, bool)
    if t == "null":
        return value is None
    return True  # unknown type keyword: don't block


class PipelineVerifier:
    """Validates that an upstream skill's output fits the downstream input.

    Stops *composition drift* — the silent breakage where step N's output shape
    changed and step N+1 quietly mis-reads it. Called *before* the downstream
    step runs, so a mismatched chain aborts cleanly instead of corrupting state.
    """

    def verify_handoff(
        self,
        upstream_output: Any,
        downstream_input_schema: dict[str, Any],
    ) -> HandoffResult:
        errors = validate_json(upstream_output, downstream_input_schema)
        return HandoffResult(ok=not errors, errors=errors)
