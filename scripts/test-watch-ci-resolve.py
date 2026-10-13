#!/usr/bin/env python3
"""Hermetic arms for watch-ci.sh's run resolution: which run does it adopt for a sha?

A SHA DOES NOT IDENTIFY A RUN. Pushing the same sha again -- the normal way to retry after a
sibling repository moves -- creates a second run for that sha, and until GitHub registers it
the only run a sha lookup can find is the PREVIOUS one, already completed. That happened: a
re-push reported the old run's red, and the red looked meaningful, because the old run had
built against a sibling state that no longer existed.

The fix is WATCH_CI_NOT_BEFORE: the caller passes the instant it pushed, and older runs are
refused rather than adopted. These arms drive the real script against a fake `gh` that
serves a canned run list, so they need no network and run in CI -- unlike test-watch-ci.py,
which needs an authenticated gh and is deliberately not wired.

The fake applies the script's own --jq expression with the real `jq`, so the selection the
arms prove is the selection the script actually sends, not a re-implementation of it.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WATCH = ROOT / "scripts" / "watch-ci.sh"

# A REAL commit: watch-ci.sh resolves its argument through git before it asks gh anything,
# so an invented sha is refused as an unknown ref and never reaches the selection under
# test. That refusal is exactly what the first draft of these arms measured -- all four
# "failed" on the argument check, including the control, which is what exposed it.
SHA = subprocess.run(
    ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True
).stdout.strip()
OLD_RUN = 111  # completed failure, created BEFORE the push
NEW_RUN = 222  # the run the push actually started
OLD_AT = "2026-09-23T11:54:14Z"
NEW_AT = "2026-09-23T11:59:26Z"
PUSHED_AT = 1790164760  # 2026-09-23T11:59:20Z: between the two runs


FAKE_GH = r"""#!/usr/bin/env bash
# Serves `run list` from $FAKE_RUNS, applying the caller's own --jq with real jq.
# `run view` answers only what the watch needs to decide the run is finished and green.
set -euo pipefail
cmd="$1 $2"; shift 2
# `run view <id>`: the id is the first argument after the verb.
rid=""
case "$cmd" in "run view") rid="${1:-}";; esac
jq_expr=""
while [ $# -gt 0 ]; do
  case "$1" in --jq) jq_expr="$2"; shift 2;; *) shift;; esac
done
case "$cmd" in
  "run list")
    printf '%s' "$FAKE_RUNS" | jq -r "$jq_expr" ;;
  "run view")
    case "$jq_expr" in
      *url*) echo "https://example.invalid/run" ;;
      *)
        # Per-run conclusions from $FAKE_CONCLUSIONS ({"<id>": "<conclusion>"}); success by default.
        conc=$(printf '%s' "${FAKE_CONCLUSIONS:-{\}}" | jq -r --arg r "$rid" '.[$r] // "success"')
        jq -n --arg c "$conc" '{status:"completed",conclusion:$c,jobs:[]}' | jq -r "${jq_expr:-.}" ;;
    esac ;;
  *) echo "fake gh: unexpected $cmd" >&2; exit 64 ;;
esac
"""


def runs(*entries: tuple[int, str], conclusions: dict[int, str] | None = None) -> str:
    # gh lists newest first; ordering is part of what is under test.
    conclusions = conclusions or {}
    return json.dumps(
        [
            {
                "databaseId": rid,
                "headSha": SHA,
                "createdAt": at,
                "conclusion": conclusions.get(rid, "success"),
            }
            for rid, at in entries
        ]
    )


class ResolveTests(unittest.TestCase):
    def setUp(self) -> None:
        if shutil.which("jq") is None:
            self.fail("jq is required: the fake applies the script's own --jq with it")
        self.tmp = Path(tempfile.mkdtemp(prefix="watch-ci-resolve-"))
        gh = self.tmp / "gh"
        gh.write_text(FAKE_GH)
        gh.chmod(0o755)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def watch(
        self,
        run_list: str,
        not_before: int | None,
        conclusions: dict[int, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "PATH": f"{self.tmp}:{os.environ['PATH']}",
            "REPO": "cortexkit/broca",
            # Keep the resolver from falling back to a host-installed gh.
            "OPERATOR_GH_FALLBACK_PATHS": "",
            "FAKE_RUNS": run_list,
            "FAKE_CONCLUSIONS": json.dumps({str(k): v for k, v in (conclusions or {}).items()}),
            "WATCH_CI_RESOLVE_ATTEMPTS": "2",
            "WATCH_CI_RESOLVE_SLEEP": "0",
        }
        env.pop("WATCH_CI_NOT_BEFORE", None)
        if not_before is not None:
            env["WATCH_CI_NOT_BEFORE"] = str(not_before)
        return subprocess.run(
            ["bash", str(WATCH), SHA],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def adopted(self, result: subprocess.CompletedProcess[str]) -> str:
        for line in result.stdout.splitlines():
            if line.startswith("watching run "):
                return line.split()[2]
        return ""

    def test_without_a_bound_the_stale_run_is_adopted(self) -> None:
        """THE DEFECT, pinned as the control. With only the old run registered, a sha lookup
        adopts it. This arm is what makes the next one mean something: if it stopped
        adopting the old run, the bounded arm below could pass for a reason unrelated to
        the bound."""
        result = self.watch(runs((OLD_RUN, OLD_AT)), not_before=None)
        self.assertEqual(self.adopted(result), str(OLD_RUN), result.stderr)

    def test_a_run_older_than_the_push_is_refused(self) -> None:
        """The same list, now with the push instant: the old run must NOT be adopted, and the
        refusal must say why -- a no-op push is the likeliest cause, and the alternative
        reading ("CI is not triggering") sends the reader to the workflow file."""
        result = self.watch(runs((OLD_RUN, OLD_AT)), not_before=PUSHED_AT)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(self.adopted(result), "")
        self.assertIn("created after the push", result.stderr)
        self.assertIn("no-op", result.stderr)

    def test_the_run_the_push_started_is_adopted(self) -> None:
        """Both runs present: the bound must pick the NEW one. Listed newest first, as gh
        lists them, so a filter that merely took the first match would also pass here --
        which is why the old-only arm above exists."""
        result = self.watch(runs((NEW_RUN, NEW_AT), (OLD_RUN, OLD_AT)), not_before=PUSHED_AT)
        self.assertEqual(self.adopted(result), str(NEW_RUN), result.stderr)

    def test_a_cancelled_run_with_a_live_twin_follows_the_twin(self) -> None:
        """THE DEFECT FROM v0.3.118's TRAIN. GitHub created two push runs for one push in the same
        second, and the concurrency group cancelled one at once. Both pass the push-time bound,
        so the watch adopted the cancelled one and reported red while its twin ran. The watch
        must follow the twin and report ITS verdict. The cancelled run is listed first (newest
        first, same second), which is the order that produced the defect."""
        twin = NEW_RUN + 1
        conclusions = {NEW_RUN: "cancelled", twin: "success"}
        result = self.watch(
            runs((NEW_RUN, NEW_AT), (twin, NEW_AT), conclusions=conclusions),
            not_before=PUSHED_AT,
            conclusions=conclusions,
        )
        self.assertEqual(self.adopted(result), str(NEW_RUN), result.stderr)
        self.assertIn(f"following {twin}", result.stdout)
        self.assertIn(f"CI_DONE run={twin} conclusion=success", result.stdout)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_a_cancelled_run_without_a_twin_is_still_red(self) -> None:
        """THE CONTROL. A genuinely cancelled run, with no other run for the sha, must stay a
        failure. Without this arm, a watch that treated every cancellation as success would
        pass the twin arm above."""
        conclusions = {NEW_RUN: "cancelled"}
        result = self.watch(
            runs((NEW_RUN, NEW_AT), conclusions=conclusions),
            not_before=PUSHED_AT,
            conclusions=conclusions,
        )
        self.assertIn(f"CI_DONE run={NEW_RUN} conclusion=cancelled", result.stdout)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_a_malformed_bound_refuses_rather_than_filtering_nothing(self) -> None:
        """An unparseable bound must not degrade into no bound at all. That would restore the
        defect while looking configured."""
        env_result = self.watch(runs((OLD_RUN, OLD_AT)), not_before=None)
        self.assertEqual(self.adopted(env_result), str(OLD_RUN))
        bad = subprocess.run(
            ["bash", str(WATCH), SHA],
            cwd=ROOT,
            env={
                **os.environ,
                "PATH": f"{self.tmp}:{os.environ['PATH']}",
                "REPO": "cortexkit/broca",
                "OPERATOR_GH_FALLBACK_PATHS": "",
                "FAKE_RUNS": runs((OLD_RUN, OLD_AT)),
                "WATCH_CI_NOT_BEFORE": "2026-09-23T11:59:20Z",
                "WATCH_CI_RESOLVE_ATTEMPTS": "2",
                "WATCH_CI_RESOLVE_SLEEP": "0",
            },
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(bad.returncode, 2)
        self.assertIn("epoch seconds", bad.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
