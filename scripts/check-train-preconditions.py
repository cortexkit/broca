#!/usr/bin/env python3
"""Refuse if this repo's CI workflow cannot produce a check for a train push.

WHY THIS IS A CI STEP AND NOT A NOTE. Enabling required status checks while the workflow
triggers on the default branch alone makes master UNPUSHABLE with no check ever arriving: the
protection refuses a sha with no check, and no workflow ran on the branch that produced it.
That deadlock is silent in the reassuring direction -- protection enabled, CI apparently
configured, and the first symptom is a refusal that reads like a credentials problem. Nearly
every seat in the fleet had the master-only trigger when this was found (QTA, 2026-09-06).

WHAT IT ASSERTS, and why each arm exists:

  1. `train/**` IS IN THE PUSH TRIGGER. The precondition above.
  2. THE DEFAULT BRANCH IS STILL IN THE PUSH TRIGGER. While a repo is unprotected, that
     trigger is the ONLY observer of a push that skips the train. Dropping it is safe only
     after protection refuses an unchecked sha, so a workflow that has quietly become
     train-only is a regression this must catch (QTA's arm; without it the checker passes the
     exact state protection is meant to prevent).
  3. NO PATH-DEPENDENT JOB OR STEP CONDITION outside the allow-list. A step gated on
     `github.ref` or `github.event_name` runs on one ref and skips on the other, so the train's
     green check would cover less than the master run did -- the check passes while the work it
     names did not happen.

  A PLATFORM GUARD IS NOT A PATH-DEPENDENT CONDITION. `matrix.os` and `runner.os` are
  identical however the push arrived; the rule is about how a push ARRIVES, not where it runs.

CONDITIONS ARE READ FROM THE PARSED DOCUMENT, NEVER GREPPED. A folded `if: >-` puts the
condition on the lines BELOW the key, so a line-oriented scan shows the key and hides the
condition -- it returns zero matches on a workflow whose gate is entirely path-dependent
(CKCRED's shape, demonstrated on a real instance by QTA).

`on` IS YAML 1.1's BOOLEAN TRUE, not the string "on". A scanner that looks up the string key
finds no triggers at all and reports clean.

THE OUTPUT NAMES WHAT WAS EXAMINED, NOT A VERDICT. That denominator is what caught this
checker reporting `success` while running NOTHING: on a Windows matrix leg, pwsh SOURCES a
script rather than executing it and exits 0. One occurrence of the summary line for two legs
was the only visible difference between a gate that ran and a gate that did not (QTA, within
an hour of CKTUI hitting the loud half of the same trap). A parse that silently finds zero
jobs prints the same reassuring line as a healthy workflow unless the count is on it.

Usage: check-train-preconditions.py [workflow.yml ...]
Exit 0 pass, 1 drift (named), 2 cannot compare (unreadable, unparseable, or nothing examined).
"""

from __future__ import annotations

import fnmatch
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("check-train-preconditions: PyYAML not available -- cannot compare", file=sys.stderr)
    sys.exit(2)

# Conditions that are CORRECT BY DESIGN, each with the reason it is exempt.
#
# An allow-list rather than "zero path-dependent conditions" because a check that is red by
# construction gets muted, taking the real signal with it (CKE2E's form). A seat whose bridge
# legitimately needs an event-based condition must be able to say so once, here, with a reason
# a later reader can weigh -- rather than deleting the checker the first time it is correctly
# relaxed. Broca's list is EMPTY today, which is the degenerate case.
ALLOWED_CONDITIONS: dict[str, str] = {}

PATH_DEPENDENT_TOKENS = ("github.ref", "github.event_name", "github.head_ref", "github.base_ref")
DEFAULT_BRANCH = "master"


def conditions_in(node, path: str):
    """Yield (location, condition) for every `if:` in a job/step tree, values read not grepped."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "if":
                yield path, str(value)
            else:
                yield from conditions_in(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from conditions_in(item, f"{path}[{index}]")


class CannotCompare(Exception):
    """The workflow could not be examined, which must never read as a clean result."""


def check(path: Path) -> list[str]:
    try:
        doc = yaml.safe_load(path.read_text())
    except Exception as exc:
        print(f"check-train-preconditions: cannot parse {path}: {exc}", file=sys.stderr)
        sys.exit(2)
    try:
        drift, summary = examine(doc, path.name)
    except CannotCompare as exc:
        print(f"check-train-preconditions: {path} {exc} -- cannot compare", file=sys.stderr)
        sys.exit(2)
    # THE DENOMINATOR. Printed before any verdict so a run that examined nothing cannot look
    # like a run that examined everything and found nothing wrong.
    print(f"check-train-preconditions: {summary}")
    return drift


def examine(doc, name: str) -> tuple[list[str], str]:
    """The drift in one parsed workflow, and the line naming what was examined."""
    if not isinstance(doc, dict):
        raise CannotCompare("is not a mapping")

    # YAML 1.1 parses the bare key `on` as boolean True. Accept both spellings so a quoted
    # "on" does not read as an absent trigger.
    triggers = doc.get(True, doc.get("on"))
    if not isinstance(triggers, dict):
        raise CannotCompare("has no trigger mapping")

    push = triggers.get("push") or {}
    branches = list(push.get("branches") or [])

    jobs = doc.get("jobs") or {}
    if not jobs:
        raise CannotCompare("declares no jobs")

    conditions = list(conditions_in(jobs, "jobs"))
    summary = (
        f"{name}: {len(branches)} push branches, {len(jobs)} jobs, "
        f"{len(conditions)} conditions read"
    )

    drift: list[str] = []

    # MUST COVER ARBITRARY TRAIN NAMES, not merely start with the prefix. The predicate
    # here was `b.startswith("train/")`, which a LITERAL branch like `train/manual`
    # satisfies -- so a workflow that can never produce a check for `train/r175` passed
    # the guard whose entire job is preventing exactly that. The error message already
    # said "train/**"; the code asked for less.
    #
    # Probing with several dissimilar names rather than demanding the literal string
    # `train/**`: it accepts `train/*` too, and it rejects a narrowing like `train/r*`
    # that would cover some trains and silently strand others.
    probes = ["train/r175", "train/l174", "train/loop-prompt-guard", "train/9"]
    uncovered = [
        probe for probe in probes
        if not any(fnmatch.fnmatchcase(probe, b) for b in branches)
    ]
    if uncovered:
        drift.append(
            f"push trigger does not cover arbitrary train branches: {branches} "
            f"leaves {uncovered} without a workflow, so those pushes get no check "
            f"and the train waits forever. Use 'train/**'."
        )

    if DEFAULT_BRANCH not in branches:
        drift.append(
            f"push trigger dropped the default branch {DEFAULT_BRANCH!r}: {branches} -- while "
            f"this repo is unprotected that trigger is the only observer of an off-train push"
        )

    for location, condition in conditions:
        if condition in ALLOWED_CONDITIONS:
            continue
        for token in PATH_DEPENDENT_TOKENS:
            if token in condition:
                drift.append(f"path-dependent condition at {location}: {condition!r}")
                break

    return drift, summary


# BUILT-IN CONTROLS, run before the real workflow on every invocation. Each arm reports a
# drift only when it finds something, so a broken arm (a trigger lookup that misses `on`, a
# walker that never matches the `if` key, a token list that lost an entry) reports clean, which
# is exactly what a healthy workflow reports. A planted clean workflow must pass, and each
# planted break must produce its own arm's drift and no other. The breaks are YAML text, not
# dicts, so the parser's handling of the bare `on` key and of a folded `if: >-` is part of what
# is checked.
CONTROL_CLEAN = """
on:
  push:
    branches: [master, "train/**"]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - if: runner.os == 'Linux'
        run: true
"""

CONTROL_BREAKS = {
    "train": (
        CONTROL_CLEAN.replace('[master, "train/**"]', "[master, train/manual]"),
        "does not cover arbitrary train branches",
    ),
    "default branch": (
        CONTROL_CLEAN.replace('[master, "train/**"]', '["train/**"]'),
        "dropped the default branch",
    ),
    "folded condition": (
        CONTROL_CLEAN.replace(
            "      - if: runner.os == 'Linux'\n",
            "      - if: >-\n          github.ref == 'refs/heads/master'\n",
        ),
        "path-dependent condition at jobs.test.steps[0]",
    ),
}


def run_controls() -> str | None:
    """`None` when every control behaved, otherwise what went wrong."""
    try:
        return _run_controls()
    except CannotCompare as exc:
        return f"a control workflow could not be examined: it {exc}"


def _run_controls() -> str | None:
    drift, _ = examine(yaml.safe_load(CONTROL_CLEAN), "control")
    if drift:
        return f"the clean control workflow reported drift: {drift}"
    for arm, (text, expected) in CONTROL_BREAKS.items():
        if text == CONTROL_CLEAN:
            return f"the {arm} control planted nothing (its replacement matched no text)"
        drift, _ = examine(yaml.safe_load(text), "control")
        if len(drift) != 1 or expected not in drift[0]:
            return f"the {arm} control expected exactly one drift containing {expected!r}, got {drift}"
    return None


def main() -> int:
    blind = run_controls()
    if blind is not None:
        print(f"check-train-preconditions: checker is BLIND: {blind}", file=sys.stderr)
        return 2
    print(f"check-train-preconditions: controls OK (clean + {len(CONTROL_BREAKS)} planted breaks)")

    args = sys.argv[1:]
    paths = [Path(a) for a in args] if args else [Path(".github/workflows/ci.yml")]

    examined = 0
    all_drift: list[str] = []
    for path in paths:
        if not path.is_file():
            print(f"check-train-preconditions: {path} not found -- cannot compare", file=sys.stderr)
            return 2
        all_drift.extend(check(path))
        examined += 1

    if examined == 0:
        print("check-train-preconditions: examined no workflows -- cannot compare", file=sys.stderr)
        return 2

    if all_drift:
        for item in all_drift:
            print(f"check-train-preconditions: DRIFT: {item}", file=sys.stderr)
        return 1

    print(f"check-train-preconditions: OK ({examined} workflow(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main())
