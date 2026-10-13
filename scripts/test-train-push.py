#!/usr/bin/env python3
"""Executable arms for train-push.sh's concurrency guard.

WHY THESE EXIST. The guard used to read a REMOTE REF to decide whether a train was
running. That proxy is wrong in both directions: a ref outlives its process (blocking an
ordinary retry) and a ref arrives LATE -- a train between resolve-HEAD and push holds no
ref at all, so a second train started in that window was permitted, which is precisely the
concurrency the guard exists to refuse. The fix reads a pid. These arms pin the four
outcomes, because the previous version's four arms were manual shell probes: they proved
the change once and could never prove it again.

THE GUARD RUNS AGAINST A FIXTURE REPO, NOT THIS ONE. Each arm builds a throwaway repo with
a local bare "origin", so arm 4 can plant a train ref without touching a real remote. The
script is copied in rather than symlinked, since it resolves its own directory.

WHAT IS ASSERTED, and why these observables rather than the exit code: the guard sits early
and the script continues into gh/CI work that cannot run in a fixture, so the exit code
after the guard says nothing about the guard. The load-bearing observables are the refusal
TEXT (arms 1, 2, 4) and the lock FILE's state (arm 3) -- both produced by the guard itself
before anything else runs.
"""

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "train-push.sh"


def git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=False
    )


def build_fixture(tmp):
    """A local bare repository containing the workflow and tools used by the train-push test."""
    bare = tmp / "origin.git"
    work = tmp / "work"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True)
    subprocess.run(["git", "init", "-q", "-b", "master", str(work)], check=True)
    git(work, "config", "user.email", "t@example.invalid")
    git(work, "config", "user.name", "t")
    (work / "scripts").mkdir(parents=True, exist_ok=True)
    shutil.copy2(SCRIPT, work / "scripts" / "train-push.sh")
    shutil.copy2(ROOT / "scripts" / "watch-ci.sh", work / "scripts" / "watch-ci.sh")
    # The script sources siblings from its own scripts/ directory.
    for sibling in (ROOT / "scripts" / "lib").glob("*"):
        dest = work / "scripts" / "lib"
        dest.mkdir(parents=True, exist_ok=True)
        if sibling.is_file():
            shutil.copy2(sibling, dest / sibling.name)
    workflow = work / ".github" / "workflows" / "ci.yml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text(
        "name: CI\n"
        "on:\n"
        "  push:\n"
        "    branches:\n"
        "      - master\n"
        "      - train/**\n"
        "jobs:\n"
        "  check:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - run: true\n"
    )
    fixture_bin = work / "fixturebin"
    fixture_bin.mkdir()
    fake_gh = fixture_bin / "gh"
    # Make the fake GitHub CLI report a successful CI run so unrelated checks do not stop the race test early.
    fake_gh.write_text(
        "#!/bin/sh\n"
        '[ "${FAKE_GH_SUCCESS:-0}" = "1" ] || exit 0\n'
        'if [ "${1:-}" = "run" ] && [ "${2:-}" = "list" ]; then\n'
        "  printf '123\\n'\n"
        'elif [ "${1:-}" = "run" ] && [ "${2:-}" = "view" ]; then\n'
        '  case "$*" in\n'
        '    *"--json url"*) printf "https://example.invalid/123\\n" ;;\n'
        '    *"--json status"*) printf "completed\\n" ;;\n'
        '    *"--json conclusion"*) printf "success\\n" ;;\n'
        '    *"--json jobs"*) ;;\n'
        '  esac\n'
        'elif [ "${1:-}" = "repo" ] && [ "${2:-}" = "view" ]; then\n'
        "  printf 'master\\n'\n"
        "fi\n"
    )
    fake_gh.chmod(0o755)
    (work / "seed.txt").write_text("seed\n")
    git(work, "add", "-A")
    git(work, "commit", "-q", "-m", "seed")
    git(work, "remote", "add", "origin", str(bare))
    git(work, "push", "-q", "origin", "master")
    git(work, "fetch", "-q", "origin")
    git(work, "remote", "set-head", "origin", "master")
    return work


def install_ls_remote_barrier(work, barrier):
    """Hold the first remote lookup until the test has checked the competing train."""
    binm = barrier / "bin"
    binm.mkdir()
    real = shutil.which("git")
    shim = binm / "git"
    shim.write_text(
        "#!/bin/bash\n"
        'for a in "$@"; do\n'
        '  if [ "$a" = "ls-remote" ]; then\n'
        '    if mkdir "$TRAIN_PUSH_TEST_BARRIER/entered" 2>/dev/null; then\n'
        '      while [ ! -e "$TRAIN_PUSH_TEST_BARRIER/release" ]; do sleep 0.01; done\n'
        '    fi\n'
        '    break\n'
        '  fi\n'
        'done\n'
        'exec "$TRAIN_PUSH_TEST_REAL_GIT" "$@"\n'
    )
    shim.chmod(0o755)
    return os.pathsep.join((str(binm), str(work / "fixturebin"))), {
        "TRAIN_PUSH_TEST_BARRIER": str(barrier),
        "TRAIN_PUSH_TEST_REAL_GIT": real,
    }


def run_guard_result(work, train_name, timeout=60, path_prefix=None, extra_env=None):
    """Run train-push.sh far enough to exercise its guards, preserving the exit code."""
    env = dict(os.environ)
    env.setdefault("REPO", "cortexkit/broca")
    env.setdefault("FAKE_GH_SUCCESS", "0")
    env["TRAIN_PUSH_PROBE_ATTEMPTS"] = "1"
    env["TRAIN_PUSH_PROBE_SLEEP"] = "0"
    if extra_env:
        env.update(extra_env)
    if path_prefix is None:
        path_prefix = str(work / "fixturebin")
    env["PATH"] = f"{path_prefix}{os.pathsep}{env.get('PATH', '')}"
    return subprocess.run(
        ["bash", "scripts/train-push.sh", train_name],
        cwd=work,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env=env,
    )


def run_guard(work, train_name, timeout=60, path_prefix=None):
    """Run train-push.sh far enough to exercise the guard, and return its output."""
    proc = run_guard_result(work, train_name, timeout, path_prefix)
    return proc.stdout + proc.stderr


def start_guard(work, train_name, path_prefix, extra_env):
    env = dict(os.environ)
    env["REPO"] = "cortexkit/broca"
    env["FAKE_GH_SUCCESS"] = "1"
    env.update(extra_env)
    env["TRAIN_PUSH_PROBE_ATTEMPTS"] = "1"
    env["TRAIN_PUSH_PROBE_SLEEP"] = "0"
    env["WATCH_CI_RESOLVE_ATTEMPTS"] = "1"
    env["WATCH_CI_RESOLVE_SLEEP"] = "0"
    env["PATH"] = f"{path_prefix}{os.pathsep}{env.get('PATH', '')}"
    return subprocess.Popen(
        ["bash", "scripts/train-push.sh", train_name],
        cwd=work,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )


def lock_dir(work):
    out = git(work, "rev-parse", "--git-dir").stdout.strip()
    return (work / out) / "train-push-locks"


def held(work):
    """The single atomic lock: a DIRECTORY, because mkdir is the POSIX atomic primitive."""
    return lock_dir(work) / "held"


def plant_owner(work, pid, name, start=None):
    """Plant a lock owner. `start` is the owner's recorded process start time (line 2).

    Omitting it plants a PRE-FIELD owner, which the guard treats as live -- that is the
    compatibility arm, and it is why the older tests here still exercise what they always did.
    """
    h = held(work)
    h.mkdir(parents=True, exist_ok=True)
    body = f"{pid} {name}\n" if start is None else f"{pid} {name}\n{start}\n"
    (h / "owner").write_text(body)
    return h


def proc_start(pid):
    return subprocess.run(
        ["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True
    ).stdout.strip()


def run_push_train(remote_sha, head_sha):
    """Drive the REAL push_train() body, extracted from the real script, with git stubbed.

    Extracted rather than run end to end because a full train needs a gating workflow, a
    probe push and a live GitHub query before it reaches this function. The extraction is
    by the function's own braces in the shipped file, so what runs is what ships.
    """
    text = SCRIPT.read_text()
    start = text.index("push_train() {")
    end = text.index("\n}\n", start) + 3
    body = text[start:end]
    harness = f"""
set -e
refuse() {{ echo "REFUSED: $*"; exit 3; }}
push_private_history_guard() {{ :; }}
say() {{ :; }}
git() {{ echo "GIT-PUSHED $*"; }}
remote=origin
train_ref=train/x
train_remote_sha={remote_sha!r}
head_sha={head_sha!r}
{body}
push_train
echo "PUSHED_AT=$pushed_at"
"""
    proc = subprocess.run(
        ["bash", "-c", harness], capture_output=True, text=True, timeout=30, check=False
    )
    return proc.returncode, proc.stdout + proc.stderr


class PrivateHistoryGuardArms(unittest.TestCase):
    """Private history is restricted by ancestry, not by a repository's current URL."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._tmp.name)
        self.work = build_fixture(self.tmp)
        self.private_root = git(self.work, "rev-parse", "HEAD").stdout.strip()

    def tearDown(self):
        self._tmp.cleanup()

    def run_with_slug(self, slug):
        return run_guard_result(
            self.work,
            "private-history",
            extra_env={
                "REPO": slug,
                "TRAIN_PUSH_PRIVATE_ROOT": self.private_root,
            },
        )

    def test_private_history_descendant_is_refused_for_public_slug(self):
        result = self.run_with_slug("cortexkit/broca")
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 2, output)
        self.assertIn(
            "this commit carries the private history and may only go to cortexkit/broca-private",
            output,
        )
        self.assertNotIn("GIT-PUSHED", output, "refuse before any commit can be pushed")

    def test_private_history_descendant_is_allowed_for_private_slug(self):
        result = self.run_with_slug("cortexkit/broca-private")
        output = result.stdout + result.stderr
        self.assertNotIn("this commit carries the private history", output)
        self.assertIn("first train in this repo", output, "the history guard must allow the push")

    def test_unrelated_history_is_allowed_for_public_slug(self):
        unrelated = git(self.work, "commit-tree", "HEAD^{tree}", "-m", "unrelated root").stdout.strip()
        self.assertTrue(unrelated, "fixture must create a second root commit")
        self.assertEqual(
            git(self.work, "merge-base", "--is-ancestor", self.private_root, unrelated).returncode,
            1,
            "the fixture's two root commits must be unrelated",
        )
        git(self.work, "reset", "--hard", unrelated)
        git(self.work, "push", "--force", "origin", "HEAD:refs/heads/master")
        git(self.work, "fetch", "-q", "origin")

        result = self.run_with_slug("cortexkit/broca")
        output = result.stdout + result.stderr
        self.assertNotIn("this commit carries the private history", output)
        self.assertIn("first train in this repo", output, "unrelated history must pass the guard")


class SameShaPushArms(unittest.TestCase):
    """A push of the sha the remote already holds starts no CI run.

    Before this refusal, re-running a train after a red on the same sha pushed nothing, and
    the watch then adopted the PREVIOUS run's verdict -- a stale red reported as current, and
    meaningful-looking, because that run had built against a sibling state that no longer
    existed. The watch now refuses runs older than the push; this refusal makes the same
    case fail in one step instead of after the watch's whole resolve budget.
    """

    def test_pushing_the_sha_the_remote_already_holds_refuses_fast(self):
        rc, out = run_push_train("a" * 40, "a" * 40)
        self.assertEqual(rc, 3, out)
        self.assertIn("starts no CI run", out)
        self.assertIn("git push origin --delete train/x", out)
        self.assertNotIn("GIT-PUSHED", out, "it must refuse BEFORE pushing, not after")

    def test_a_new_sha_still_pushes_and_records_the_instant(self):
        """The control: without it, a guard that refused every push would pass the arm above.
        The instant must be set, because the watch's bound is computed from it."""
        rc, out = run_push_train("a" * 40, "b" * 40)
        self.assertEqual(rc, 0, out)
        self.assertIn("GIT-PUSHED", out)
        self.assertRegex(out, r"PUSHED_AT=\d{10}")

    def test_a_first_push_is_unaffected(self):
        rc, out = run_push_train("", "b" * 40)
        self.assertEqual(rc, 0, out)
        self.assertIn("GIT-PUSHED", out)

    def test_the_watch_is_handed_the_push_instant(self):
        """The bound only works if train-push passes it. A source check is weak on its own,
        so this pins the one line that connects the two scripts; the watch side of the
        contract is proven by behaviour in test-watch-ci-resolve.py."""
        text = SCRIPT.read_text()
        self.assertIn('WATCH_CI_NOT_BEFORE="$pushed_at" "$script_dir/watch-ci.sh"', text)


class ConcurrencyGuardArms(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._tmp.name)
        self.work = build_fixture(self.tmp)

    def tearDown(self):
        self._tmp.cleanup()

    def test_clean_state_does_not_refuse_for_concurrency(self):
        """Arm 1: no lock and no ref must not trip the guard.

        The anchor arm. Without it, a guard that refused unconditionally would satisfy
        arms 2-4 and stop every train on the box.
        """
        out = run_guard(self.work, "arm1")
        self.assertNotIn("is RUNNING as pid", out)
        self.assertNotIn("a train is already running", out)

    def test_a_live_lock_refuses_and_names_the_pid(self):
        """Arm 2: a lock whose process is ALIVE must refuse.

        Names the pid because the operator's next decision is wait-or-kill, and that needs
        the process, not the fact of a process.
        """
        sleeper = subprocess.Popen(["sleep", "60"])
        try:
            plant_owner(self.work, sleeper.pid, "othertrain")
            out = run_guard(self.work, "arm2")
            self.assertIn("othertrain is RUNNING", out)
            self.assertIn(str(sleeper.pid), out)
            self.assertIn("a train is already running", out)
        finally:
            sleeper.kill()
            sleeper.wait()

    def test_a_stale_lock_is_cleared_and_does_not_refuse(self):
        """Arm 3: a lock whose process is GONE must be removed, not obeyed.

        This is the self-heal property. A train killed with SIGKILL runs no trap, so the
        file outlives it; if a dead lock refused, one hard kill would wedge the box until
        someone deleted a file they did not know about.
        """
        dead = subprocess.Popen(["true"])
        dead.wait()
        plant_owner(self.work, dead.pid, "deadtrain")
        out = run_guard(self.work, "arm3")
        owner = (held(self.work) / "owner").read_text()
        self.assertNotIn("deadtrain", owner, "a stale owner must be replaced, not obeyed")
        self.assertNotIn("deadtrain is RUNNING", out)
        self.assertNotIn("a train is already running", out)

    def test_a_leftover_ref_lists_its_delete_and_proceeds(self):
        """Arm 4: a train ref with no live process must NOT refuse.

        The behaviour change. A ref without a process cannot race this train, and refusing
        there blocked the ordinary retry after a red CI -- the case AFT hits six times in a
        day. It is still listed with the delete composed, because composing `--delete` by
        hand next to several refs is where the wrong one goes.
        """
        git(self.work, "push", "-q", "origin", "HEAD:refs/heads/train/leftover")
        out = run_guard(self.work, "arm4")
        self.assertIn("no live train here", out)
        self.assertIn("git push origin --delete train/leftover", out)
        self.assertNotIn("a train is already running", out)

    def test_the_lock_is_claimed_before_any_ref_moves(self):
        """The ordering that makes arm 2 reachable at all.

        A lock written after the push leaves the resolve-HEAD-to-push window uncovered,
        which is the exact hole the ref-based guard had. Asserted by checking the lock
        exists after a run that never got as far as pushing.
        """
        run_guard(self.work, "ordering")
        owner = (held(self.work) / "owner").read_text()
        self.assertIn("ordering", owner, "this train must own the lock even on an early stop")


    def test_a_recycled_pid_does_not_wedge_the_lock_forever(self):
        """CALLO found this in the copy AFT lifted from here: a finished train's pid landed on
        a long-lived app and `kill -0` answered yes ever after, refusing every later train.

        The failure gets MORE likely the longer a repo sits quiet -- the opposite of a race --
        so nobody meets it for weeks and then meets it certainly.

        Both halves are asserted. Reclaiming a recycled pid is the fix; NOT dispossessing a
        genuinely live owner is what keeps the fix from being a regression, and a change that
        only satisfied the first would pass a one-armed version of this test.
        """
        sleeper = subprocess.Popen(["sleep", "60"])
        try:
            # ALIVE, and NOT the train: the recorded start time does not match the process
            # holding that number now. That is exactly what a recycled pid looks like.
            plant_owner(self.work, sleeper.pid, "finished-train", start="Wed Jan  1 00:00:00 2020")
            out = run_guard(self.work, "after-recycle")
            self.assertNotIn(
                "a train is already running", out,
                f"a live pid with a MISMATCHED start time is a recycled pid, not a running "
                f"train; refusing here is the permanent wedge. got: {out!r}",
            )

            # CONTROL: same pid, REAL start time -- a genuinely live owner, must still refuse.
            real = proc_start(sleeper.pid)
            self.assertTrue(real, "this platform must supply a start time or the pair means nothing")
            plant_owner(self.work, sleeper.pid, "othertrain", start=real)
            out = run_guard(self.work, "against-live")
            self.assertIn(
                "a train is already running", out,
                f"a matching pid AND start time is a live train and must not be dispossessed. "
                f"got: {out!r}",
            )
        finally:
            sleeper.kill()
            sleeper.wait()

    def test_two_trains_started_together_cannot_both_be_admitted(self):
        """The second train must be refused while the first still owns the lock."""
        barrier = self.tmp / "network-barrier"
        barrier.mkdir()
        binm, extra_env = install_ls_remote_barrier(self.work, barrier)
        entered = barrier / "entered"
        release = barrier / "release"
        winner = start_guard(self.work, "racer-a", str(binm), extra_env)
        loser = None

        try:
            deadline = time.monotonic() + 10
            while not entered.exists():
                if winner.poll() is not None:
                    out, _ = winner.communicate()
                    self.fail(f"winner exited before reaching the barrier:\n{out}")
                if time.monotonic() >= deadline:
                    self.fail("winner did not reach the remote-lookup barrier")
                time.sleep(0.01)

            owner = (held(self.work) / "owner").read_text().splitlines()[0].split()
            self.assertEqual(owner[1], "racer-a", f"unexpected lock owner: {owner!r}")
            self.assertIsNone(winner.poll(), "winner must still be blocked at the barrier")

            loser = start_guard(self.work, "racer-b", str(binm), extra_env)
            loser_out, _ = loser.communicate(timeout=10)
            self.assertEqual(loser.returncode, 2, loser_out)
            self.assertIn("a train is already running", loser_out)

            # The release file does not exist yet, so the first train is still at the
            # barrier and still owns the lock when the second train's refusal is observed.
            self.assertFalse(release.exists())
            self.assertIsNone(winner.poll(), "winner must remain blocked while the loser checks")
            owner = (held(self.work) / "owner").read_text().splitlines()[0].split()
            self.assertEqual(owner[1], "racer-a", f"loser changed the live owner: {owner!r}")
        finally:
            if loser is not None and loser.poll() is None:
                loser.kill()
                loser.communicate()
            release.touch()
            try:
                winner.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                winner.kill()
                winner.communicate()


if __name__ == "__main__":
    # EXIT 2 WHEN THE SCRIPT IS MISSING, rather than reporting "OK (skipped)" over a
    # measurement that never happened. Exit 2 is "cannot compare".
    if not SCRIPT.exists():
        print(f"cannot compare: {SCRIPT} not found", file=sys.stderr)
        raise SystemExit(2)
    if shutil.which("git") is None:
        print("cannot compare: git not on PATH", file=sys.stderr)
        raise SystemExit(2)
    unittest.main(verbosity=2)
