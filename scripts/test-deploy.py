#!/usr/bin/env python3
"""Hermetic deploy orchestration and immutable, post-CI publication contracts."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]


class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="broca deploy ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "work"
        self.root.mkdir()
        self.home = Path(self.tmp.name) / "home"
        self.home.mkdir()
        self.env = dict(os.environ, HOME=str(self.home), GIT_CONFIG_NOSYSTEM="1")
        for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "TAG_WHEN_GREEN_LOG"):
            self.env.pop(key, None)
        self.git("init", "-q", "-b", "master")
        self.git("config", "user.name", "Deploy fixture")
        self.git("config", "user.email", "deploy@example.invalid")
        self.scripts = self.root / "scripts"
        self.scripts.mkdir()
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.0.2"\n')
        self.events = self.home / "events"
        self.env["EVENTS"] = str(self.events)

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, env=self.env,
                                       text=True, stderr=subprocess.STDOUT).strip()

    def commit(self):
        self.git("add", "-A")
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def run_script(self, name, *args, **env):
        command = ["bash", str(self.scripts / name), *args]
        with subprocess.Popen(command, cwd=self.root, env=dict(self.env, **env), text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True) as process:
            try:
                stdout, stderr = process.communicate(timeout=20)
            except subprocess.TimeoutExpired:
                # A failed FIFO rendezvous must fail the test, not leave a stage
                # child holding the captured output pipe open indefinitely.
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate(timeout=5)
                raise
            return subprocess.CompletedProcess(command, process.wait(), stdout, stderr)

    def records(self):
        return [json.loads(line) for line in self.events.read_text().splitlines()] if self.events.exists() else []


class DeployTests(Fixture):
    def setUp(self):
        super().setUp()
        shutil.copy2(ROOT / "scripts/deploy.sh", self.scripts)
        stub = self.scripts / "stub.py"
        stub.write_text(r'''
import json, os, pathlib, signal, stat, subprocess, sys
name, *args = sys.argv[1:]
home = pathlib.Path(os.environ["HOME"])
with open(os.environ["EVENTS"], "a") as log:
    log.write(json.dumps([name, args]) + "\n")
if name == "stage":
    if os.environ.get("STAGE_ENTERED_FIFO"):
        (home / "stage-active").touch()
        with open(os.environ["STAGE_ENTERED_FIFO"], "w") as entered:
            entered.write("staging\n")
        with open(os.environ["METADATA_RELEASE_FIFO"]) as release:
            assert release.read() == "metadata active\n"
        assert (home / "metadata-active").exists()
        (home / "stage-active").unlink()
    subprocess.run(["git", "describe", "--exact-match", "--tags", "HEAD"], check=True)
    seam = home / ".local/share/cortexkit/staging"
    seam.mkdir(parents=True)
    short = subprocess.check_output(["git", "rev-parse", "--short=8", "HEAD"], text=True).strip()
    (seam / ("ck-broca." + short)).write_bytes(b"signed release")
    client = pathlib.Path("target/release/broca-session")
    client.parent.mkdir(parents=True)
    if not os.environ.get("MISSING_CLIENT"):
        client.write_text("#!/bin/sh\nexit 0\n")
        client.chmod(0o755)
    (home / "stage-finished").touch()
if name == "place" and "--report" in args:
    assert (home / "stage-finished").exists()
    assert (home / "metadata-done").read_text() == "0"
    backup = home / "rollback copy"
    backup.write_bytes(b"previous release")
    pathlib.Path(args[args.index("--report")+1]).write_text(json.dumps({
        "backup": str(backup), "pid": "4321", "sessions_at_restart": ["session.wal"],
        "client_backups": {client: str(home / (client + ".rollback")) for client in ("broca-session", "broca-run")},
        "sessions_sealed": ["session.wal"], "sessions_unconfirmed": [], "seal_error": ""}))
if name == "publish":
    pathlib.Path(os.environ["TAG_WHEN_GREEN_LOG"]).write_text("background CI\n")
if name == "accept" and os.environ.get("WARMING_ACCEPTS"):
    # The daemon refuses a bind while the restarted module is still starting.
    with open(os.environ["EVENTS"]) as events:
        attempts = sum(1 for line in events if json.loads(line)[0] == "accept")
    if attempts <= int(os.environ["WARMING_ACCEPTS"]):
        print("ERROR bind management surface 'broca': subc rejected: module_warming")
        sys.exit(1)
if name == "listing":
    assert args == ["auth", "list", "--json"]
    print(json.dumps({"v": 1, "credentials": []}))
    if os.environ.get("FAIL_STEP") == name:
        print("token-DO-NOT-LOG", file=sys.stderr)
if name == "metadata":
    listing = pathlib.Path(args[args.index("--vault-listing") + 1])
    assert listing.stat().st_mode & 0o077 == 0
    assert json.loads(listing.read_text()) == {"v": 1, "credentials": []}
    output = os.fstat(sys.stdout.fileno())
    assert stat.S_ISREG(output.st_mode) and output.st_mode & 0o077 == 0
    if os.environ.get("STAGE_ENTERED_FIFO"):
        with open(os.environ["STAGE_ENTERED_FIFO"]) as entered:
            assert entered.read() == "staging\n"
        assert (home / "stage-active").exists()
        (home / "metadata-active").touch()
        if os.environ.get("HOLD_METADATA"):
            child = subprocess.Popen([sys.executable, "-c",
                "import signal; print('ready', flush=True); signal.pause()"],
                stdout=subprocess.PIPE, text=True)
            assert child.stdout.readline() == "ready\n"
            def stopped(signum, frame):
                # Waiting, not terminating, proves the group signal reached the child.
                child.wait(timeout=5)
                (home / "metadata-stopped").write_text("child reaped")
                sys.exit(143)
            signal.signal(signal.SIGTERM, stopped)
        with open(os.environ["METADATA_RELEASE_FIFO"], "w") as release:
            release.write("metadata active\n")
        if os.environ.get("HOLD_METADATA"):
            signal.pause()
    status = int(os.environ.get("METADATA_STATUS", "0"))
    (home / "metadata-done").write_text(str(status))
    print(f"metadata fixture result: {status}")
    sys.exit(status)
sys.exit(19 if os.environ.get("FAIL_STEP") == name else 0)
''')
        (self.scripts / "check-credential-metadata.py").write_text(
            'import runpy, sys\nsys.argv[1:] = ["metadata", *sys.argv[1:]]\n'
            'runpy.run_path(__file__.replace("check-credential-metadata.py", "stub.py"), run_name="__main__")\n')
        (self.scripts / "acceptance-worker-turn.py").write_text(
            'import runpy, sys\nsys.argv[1:] = ["worker-accept"]\n'
            'runpy.run_path(__file__.replace("acceptance-worker-turn.py", "stub.py"), run_name="__main__")\n')
        for file, step in (("stage-release.sh", "stage"), ("place-release.sh", "place"),
                           ("verify-deploy.sh", "verify"), ("acceptance-turn.sh", "accept"),
                           ("tag-when-green.sh", "publish")):
            (self.scripts / file).write_text(
                f'#!/bin/sh\nexec python3 "$(dirname "$0")/stub.py" {step} "$@"\n')
            (self.scripts / file).chmod(0o755)
        (self.scripts / "ck").write_text(
            '#!/bin/sh\nexec python3 "$(dirname "$0")/stub.py" listing "$@"\n')
        (self.scripts / "ck").chmod(0o755)
        self.env["PATH"] = str(self.scripts) + os.pathsep + self.env["PATH"]
        # A stage build is ignored just as target/ is in the real repository.
        (self.root / ".gitignore").write_text("target/\n")
        self.sha = self.commit()

    def deploy(self, *args, **env):
        return self.run_script("deploy.sh", "--marker", "new marker", "--control", "known control", *args, **env)

    def steps(self):
        return [record[0] for record in self.records()]

    def assert_gated_steps(self, following):
        steps = self.steps()
        self.assertCountEqual(steps[:3], ["stage", "listing", "metadata"])
        self.assertLess(steps.index("listing"), steps.index("metadata"))
        self.assertEqual(steps[3:], following)

    def overlap(self):
        # Neither stage nor metadata can finish before the other starts, so a
        # serial implementation fails the rendezvous instead of passing by luck.
        entered = self.home / "stage-entered.fifo"
        release = self.home / "metadata-release.fifo"
        os.mkfifo(entered)
        os.mkfifo(release)
        return {"STAGE_ENTERED_FIFO": str(entered), "METADATA_RELEASE_FIFO": str(release)}

    def assert_metadata_log(self, result, status):
        logdir = Path(next(line.removeprefix("deploy: logs ") for line in result.stdout.splitlines()
                           if line.startswith("deploy: logs ")))
        log = logdir / "account-metadata.log"
        self.assertIn(f"metadata fixture result: {status}", result.stdout)
        self.assertIn(f"metadata fixture result: {status}", log.read_text())
        self.assertEqual(log.stat().st_mode & 0o077, 0)
        self.assertEqual(list(logdir.glob("credential-listing.*")), [])
        self.assertEqual(list(logdir.glob("credential-output.*")), [])

    def test_dirty_tree_refuses_before_tag_or_staging(self):
        (self.root / "uncommitted").write_text("dirty")
        result = self.deploy()
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("dirty working tree", result.stderr)
        self.assertEqual(self.steps(), [])
        self.assertEqual(self.git("tag", "--list"), "")

    def test_stage_failure_never_places_or_restarts(self):
        result = self.deploy(FAIL_STEP="stage")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("STAGING FAILED", result.stderr)
        self.assertIn("stage", self.steps())
        self.assertTrue(set(self.steps()) <= {"stage", "listing", "metadata"})

    def assert_metadata_stops_deploy(self, status):
        result = self.deploy(METADATA_STATUS=str(status))
        self.assertEqual(result.returncode, status, result.stdout + result.stderr)
        self.assertIn("ACCOUNT METADATA CHECK FAILED", result.stderr)
        self.assert_gated_steps([])
        self.assert_metadata_log(result, status)

    def test_missing_listing_command_refuses_before_placement(self):
        (self.scripts / "ck").write_text("#!/bin/sh\nexit 127\n")
        self.commit()
        result = self.deploy()
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("credential metadata listing unavailable", result.stdout + result.stderr)
        self.assertEqual(self.steps(), ["stage"])

    def test_failed_listing_refuses_without_logging_raw_stderr(self):
        result = self.deploy(FAIL_STEP="listing")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("credential metadata listing unavailable", result.stdout + result.stderr)
        self.assertNotIn("token-DO-NOT-LOG", result.stdout + result.stderr)
        self.assertCountEqual(self.steps(), ["stage", "listing"])

    def test_metadata_refusal_never_places_or_restarts(self):
        self.assert_metadata_stops_deploy(1)

    def test_metadata_cannot_compare_never_places_or_restarts(self):
        self.assert_metadata_stops_deploy(2)

    def test_metadata_check_runs_concurrently_with_staging(self):
        result = self.deploy(**self.overlap())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assert_gated_steps(["place", "verify", "accept", "worker-accept", "publish"])
        self.assertTrue((self.home / "stage-finished").exists())
        self.assertTrue((self.home / "metadata-active").exists())
        self.assert_metadata_log(result, 0)

    def test_overlapping_metadata_refusal_still_blocks_restart(self):
        result = self.deploy(METADATA_STATUS="1", **self.overlap())
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assert_gated_steps([])
        self.assertTrue((self.home / "stage-finished").exists())
        self.assertIn("ACCOUNT METADATA CHECK FAILED", result.stderr)
        self.assert_metadata_log(result, 1)

    def test_stage_failure_stops_the_background_gate_and_its_child(self):
        result = self.deploy(FAIL_STEP="stage", HOLD_METADATA="1", **self.overlap())
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("STAGING FAILED", result.stderr)
        self.assert_gated_steps([])
        self.assertEqual((self.home / "metadata-stopped").read_text(), "child reaped")
        logdir = next((self.home / ".local/state/cortexkit/broca/deploys").iterdir())
        self.assertEqual(list(logdir.glob("credential-listing.*")), [])
        self.assertEqual(list(logdir.glob("credential-output.*")), [])

    def test_failed_placement_stops_before_verification_and_acceptance(self):
        result = self.deploy(FAIL_STEP="place")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("PLACEMENT/RESTART FAILED", result.stderr)
        self.assert_gated_steps(["place"])
        self.assertIn("Rollback:", result.stderr)

    def test_failed_acceptance_prints_executable_rollback_command(self):
        result = self.deploy(FAIL_STEP="accept")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("LIVE ACCEPTANCE FAILED", result.stderr)
        self.assert_gated_steps(["place", "verify", "accept"])
        command = next(line.removeprefix("Rollback: ") for line in result.stderr.splitlines()
                       if line.startswith("Rollback: "))
        (self.events).unlink()
        restored = subprocess.run(["bash", "-c", command], cwd=self.root, env=self.env,
                                  text=True, capture_output=True, timeout=10)
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(self.records()[0][1], ["--staged", str(self.home / "rollback copy"),
                         "--session-staged", str(self.home / "broca-session.rollback"),
                         "--run-staged", str(self.home / "broca-run.rollback"),
                         "--dest", str(self.home / ".local/share/cortexkit/bin/ck-broca"),
                         "--restart", "--live-approved"])

    def test_verify_failure_prints_rollback_and_never_accepts(self):
        result = self.deploy(FAIL_STEP="verify")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("LIVE VERIFICATION FAILED", result.stderr)
        self.assertIn("Rollback:", result.stderr)
        self.assert_gated_steps(["place", "verify"])

    def test_missing_built_session_is_a_loud_acceptance_failure(self):
        result = self.deploy(MISSING_CLIENT="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("LIVE ACCEPTANCE FAILED", result.stderr)
        self.assertIn("Rollback:", result.stderr)
        self.assert_gated_steps(["place", "verify"])

    def test_acceptance_waits_out_a_module_still_starting(self):
        result = self.deploy(WARMING_ACCEPTS="2", BROCA_DEPLOY_WARMING_RETRY_SECS="0")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assert_gated_steps(["place", "verify", "accept", "accept", "accept", "worker-accept", "publish"])
        self.assertIn("still starting (module_warming)", result.stdout)

    def test_acceptance_failure_other_than_warming_is_not_retried(self):
        result = self.deploy(FAIL_STEP="accept", BROCA_DEPLOY_WARMING_RETRY_SECS="0")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(self.steps().count("accept"), 1)

    def test_existing_version_tag_on_other_commit_refuses(self):
        self.git("tag", "v0.0.2")
        (self.root / "fix").write_text("next commit")
        self.commit()
        result = self.deploy()
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("already names", result.stderr)
        self.assertEqual(self.steps(), [])

    def test_failed_worker_admission_never_publishes(self):
        result = self.deploy(FAIL_STEP="worker-accept")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assert_gated_steps(["place", "verify", "accept", "worker-accept"])
        self.assertIn("WORKER ADMISSION ACCEPTANCE FAILED", result.stderr)
        self.assertIn("Rollback:", result.stderr)

    def test_success_passes_probes_and_approval_reports_pid_and_only_then_publishes(self):
        result = self.deploy("--live-approved")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        records = dict(self.records())
        self.assert_gated_steps(["place", "verify", "accept", "worker-accept", "publish"])
        self.assertEqual(records["stage"], ["--marker", "new marker", "--control", "known control"])
        self.assertEqual(records["listing"], ["auth", "list", "--json"])
        self.assertEqual(records["metadata"][0], "--vault-listing")
        self.assertIn("--restart", records["place"])
        self.assertIn("--live-approved", records["place"])
        self.assertIn(hashlib.sha256(b"signed release").hexdigest(), records["verify"])
        self.assertEqual(records["accept"], ["0.0.2"])
        self.assertEqual(records["worker-accept"], [])
        self.assertEqual(records["publish"], [self.sha, "0.0.2"])
        self.assert_metadata_log(result, 0)
        self.assertIn("pid        4321", result.stdout)
        self.assertIn("sessions sealed at restart (WAL readback): session.wal", result.stdout)
        self.assertIn("acceptance PASS", result.stdout)
        self.assertIn("CI log", result.stdout)
        self.assertEqual(self.git("status", "--porcelain"), "")


class PublicationTests(Fixture):
    def setUp(self):
        super().setUp()
        shutil.copy2(ROOT / "scripts/tag-when-green.sh", self.scripts)
        self.origin = Path(self.tmp.name) / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", str(self.origin)], check=True)
        self.env["ORIGIN_FIXTURE"] = str(self.origin)
        (self.scripts / "watch-ci.sh").write_text(r'''#!/bin/sh
exec python3 "$(dirname "$0")/watch.py" "$@"
''')
        (self.scripts / "watch.py").write_text(r'''
import json, os, pathlib, subprocess, sys, time
origin = os.environ["ORIGIN_FIXTURE"]
def ref(name):
    result = subprocess.run(["git", "--git-dir", origin, "rev-parse", "--verify", name],
                            capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ""
event = {"sha": sys.argv[1], "master_before_green": ref("refs/heads/master"),
         "tag_before_green": ref("refs/tags/v0.0.2")}
pathlib.Path(os.environ["EVENTS"]).write_text(json.dumps(event) + "\n")
if os.environ.get("WAIT_FOR_GREEN"):
    deadline = time.monotonic() + 10
    while not pathlib.Path(os.environ["WAIT_FOR_GREEN"]).exists():
        if time.monotonic() > deadline: raise SystemExit(2)
        time.sleep(.01)
raise SystemExit(int(os.environ.get("CI_EXIT", "0")))
''')
        self.base = self.commit()
        self.git("remote", "add", "origin", str(self.origin))
        self.git("push", "-q", "origin", "HEAD:refs/heads/master")
        (self.root / "change").write_text("deployed code")
        self.sha = self.commit()
        self.git("tag", "-a", "v0.0.2", "-m", "local staging tag")
        self.initial_status = self.git("status", "--porcelain")

    def remote(self, ref):
        return self.git("ls-remote", "origin", ref)

    def publish(self, **env):
        return self.run_script("tag-when-green.sh", "--watch", str(self.root), self.sha, "0.0.2", **env)

    def assert_checkout_untouched(self):
        self.assertEqual(self.git("rev-parse", "HEAD"), self.sha)
        self.assertEqual(self.git("symbolic-ref", "--short", "HEAD"), "master")
        self.assertEqual(self.git("status", "--porcelain"), self.initial_status)

    def test_red_ci_never_advances_master_or_publishes_tag(self):
        result = self.publish(CI_EXIT="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertTrue(self.remote("refs/heads/master").startswith(self.base))
        self.assertEqual(self.remote("refs/tags/v0.0.2"), "")
        self.assertTrue(self.remote(f"refs/heads/train/deploy-{self.sha}").startswith(self.sha))
        self.assert_checkout_untouched()

    def test_green_ci_then_fast_forward_then_tag_exact_deployed_sha(self):
        result = self.publish()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        observed = self.records()[0]
        self.assertEqual(observed, {"sha": self.sha, "master_before_green": self.base, "tag_before_green": ""})
        self.assertTrue(self.remote("refs/heads/master").startswith(self.sha))
        self.assertTrue(self.remote("refs/tags/v0.0.2^{}").startswith(self.sha))
        self.assert_checkout_untouched()

    def test_non_fast_forward_leaves_master_untouched_and_still_tags(self):
        sibling = self.git("commit-tree", f"{self.sha}^{{tree}}", "-p", self.base, "-m", "another seat")
        self.git("push", "-q", "origin", f"{sibling}:refs/heads/master")
        result = self.publish()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("origin/master was NOT advanced", result.stderr)
        self.assertTrue(self.remote("refs/heads/master").startswith(sibling))
        self.assertTrue(self.remote("refs/tags/v0.0.2^{}").startswith(self.sha))
        self.assert_checkout_untouched()

    def test_detached_launch_returns_before_ci_and_finishes_after_green(self):
        release = self.home / "green"
        log = self.home / "ci.log"
        result = self.run_script("tag-when-green.sh", self.sha, "0.0.2",
                                 WAIT_FOR_GREEN=str(release), TAG_WHEN_GREEN_LOG=str(log))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"CI log {log}", result.stdout)
        deadline = time.monotonic() + 10
        try:
            while not self.events.exists() and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(self.events.exists(), log.read_text())
            self.assertTrue(self.remote("refs/heads/master").startswith(self.base))
            self.assertEqual(self.remote("refs/tags/v0.0.2"), "")
        finally:
            release.touch()
            while "published v0.0.2" not in log.read_text() and time.monotonic() < deadline:
                time.sleep(.01)
        self.assertIn("published v0.0.2", log.read_text())
        self.assert_checkout_untouched()


if __name__ == "__main__":
    unittest.main(verbosity=2)
