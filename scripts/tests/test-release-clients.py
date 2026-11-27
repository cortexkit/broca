#!/usr/bin/env python3
"""Deployment clients are built, published, placed and verified with the module."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))


def load_suite(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gate = load_suite("client_gate_fixture", "test-place-release-gate.py")
deploy = load_suite("client_deploy_fixture", "test-deploy.py")


class StageClientsTests(unittest.TestCase):
    def test_stage_builds_and_publishes_module_and_session_signed_binaries_sidecar_first(self):
        with tempfile.TemporaryDirectory(prefix="release clients ") as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            for name in ("stage-release.sh", "dsym.sh", "feature-floor.sh", "running-image.sh", "signing-posture.sh", "nonshipping-markers.sh"):
                shutil.copy2(ROOT / "scripts" / name, scripts / name)
            (scripts / "lib").mkdir()
            shutil.copy2(ROOT / "scripts/lib/artifact-probe.sh", scripts / "lib/artifact-probe.sh")
            script = scripts / "stage-release.sh"
            source = script.read_text()
            anchor = "BUILD_SCRATCH=/tmp/broca-release-staging"
            self.assertEqual(source.count(anchor), 1, "staging scratch assignment must be replaced exactly once")
            replacement = f'BUILD_SCRATCH="{root / "scratch"}"'
            rewritten = source.replace(anchor, replacement, 1)
            self.assertEqual([line for line in rewritten.splitlines() if line.startswith("BUILD_SCRATCH=")],
                             [replacement], "fixture staging must use its owned scratch directory")
            script.write_text(rewritten)
            (scripts / "prune-staging.sh").write_text("#!/bin/sh\nexit 0\n")
            (scripts / "prune-staging.sh").chmod(0o755)
            (root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.0.2"\n')
            (root / "Cargo.lock").write_text("locked dependencies\n")
            tools = root / "tools"
            tools.mkdir()
            log = root / "publish.log"
            stubs = {
                "git": '''#!/bin/sh
case "$1" in
 status) exit 0 ;;
 describe) echo v0.0.2 ;;
 rev-parse) echo deadbeef ;;
 *) exit 1 ;;
esac
''',
                "python3": '''#!/bin/sh
case "$1" in
 *check-path-deps.py) exit 0 ;;
 *cargo-watchdog.py) while [ "$1" != -- ]; do shift; done; shift; exec "$@" ;;
 *) exit 1 ;;
esac
''',
                "cargo": '''#!/bin/sh
printf '%s\n' "$*" > "$CARGO_LOG"
target="${CARGO_TARGET_DIR:-target}/release"
mkdir -p "$target"
for name in ck-broca broca-session; do
 case " $* " in *" --bin $name "*) ;; *) continue ;; esac
 printf '#!/bin/sh\necho "%s 0.0.2"\n' "$name" > "$target/$name"
 chmod 755 "$target/$name"
done
''',
                "uname": "#!/bin/sh\necho Darwin\n",
                "file": '#!/bin/sh\necho "Mach-O 64-bit executable arm64"\n',
                "dsymutil": '#!/bin/sh\nmkdir -p "$3/Contents/Resources/DWARF"\necho symbols > "$3/Contents/Resources/DWARF/ck-broca"\n',
                "strip": "#!/bin/sh\nexit 0\n",
                "dwarfdump": '#!/bin/sh\nprintf "UUID: 11111111-2222-3333-4444-555555555555 (arm64) %s\\n" "$2"\n',
                "cc": '#!/bin/sh\nwhile [ "$1" != -o ]; do shift; done\necho control > "$2"\n',
                "nm": '#!/bin/sh\ncase "$2" in *strip-control*) echo "000 - 00 0000 SO source.c" ;; *) echo "000 T main" ;; esac\n',
                "codesign": gate.CODESIGN_STUB.replace("/usr/bin/env python3", sys.executable),
                "mv": '''#!/bin/sh
for target; do :; done
case "$target" in
 */staging/*) printf '%s\n' "$target" >> "$PUBLISH_LOG" ;;
esac
exec "$REAL_MV" "$@"
''',
            }
            for name, body in stubs.items():
                (tools / name).write_text(body)
                (tools / name).chmod(0o755)
            real_mv = shutil.which("mv")
            assert real_mv is not None
            env = dict(os.environ, HOME=str(root / "home"), PATH=f"{tools}:{os.environ['PATH']}",
                       PUBLISH_LOG=str(log), CARGO_LOG=str(root / "cargo.log"), REAL_MV=real_mv,
                       CARGO_TARGET_DIR=str(root / "custom target"))
            result = subprocess.run(["bash", str(script)], cwd=root, env=env, text=True,
                                    capture_output=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            args = (root / "cargo.log").read_text()
            self.assertIn("-p broca-module-serve", args)
            self.assertNotIn("broca-run", args)
            self.assertFalse((root / "target/release").exists())
            seam = root / "home/.local/share/cortexkit/staging"
            renames = log.read_text().splitlines()
            for name in ("ck-broca", "broca-session"):
                self.assertIn(f"--bin {name}", args)
                artifact = seam / f"{name}.deadbeef"
                self.assertTrue(artifact.is_file(), result.stdout)
                self.assertEqual(gate.read_flags(artifact), "adhoc,runtime")
                self.assertNotIn(b"entitlements=get-task-allow", artifact.read_bytes())
                check = subprocess.run(["shasum", "-c", artifact.name + ".sha256.postsign"],
                                       cwd=seam, capture_output=True, text=True)
                self.assertEqual(check.returncode, 0, check.stdout + check.stderr)
                self.assertLess(renames.index(str(artifact) + ".sha256.postsign"), renames.index(str(artifact)))
                self.assertTrue((root / f"scratch/{name}.deadbeef.incoming").is_file())


class ClientPlacementTests(gate.GateBase):
    def setUp(self):
        super().setUp()
        gate.stub_binary(self.dest, version="0.3.148", features=[], signed="adhoc,runtime")
        gate.stub_binary(self.staged, features=[], signed="adhoc,runtime")
        for name in ("broca-session",):
            gate.stub_client(self.bindir / name, name, "0.3.148")
        self.restart_log = self.tmp / "restart.log"
        ck = self.fake_bin / "ck"
        ck.write_text(f'#!/bin/sh\necho restart >> "{self.restart_log}"\n')
        ck.chmod(0o755)

    def test_missing_client_refuses_before_any_placement_or_restart(self):
        before = self.dest.read_bytes()
        (self.tmp / "broca-session.staged").unlink()
        result = self.place("--restart")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("client-missing", result.stderr)
        self.assertIn("broca-session", result.stderr)
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertFalse(self.restart_log.exists())

    def test_client_version_mismatch_refuses_before_restart(self):
        before = self.dest.read_bytes()
        gate.stub_client(self.tmp / "broca-session.staged", "broca-session", "0.3.198")
        result = self.place("--restart")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("client-version", result.stderr)
        self.assertIn("0.3.198", result.stderr)
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertFalse(self.restart_log.exists())

    def test_clients_rename_before_restart_and_keep_verifiable_backups(self):
        originals = {name: (self.bindir / name).read_bytes() for name in ("broca-session",)}
        # The restart stub observes destination bytes, rather than trusting placement's log.
        ck = self.fake_bin / "ck"
        ck.write_text('#!/bin/sh\nset -e\n' + ''.join(
            f'cmp "{self.bindir / name}" "{self.tmp / (name + ".staged")}"\n'
            for name in originals) + f'echo restart >> "{self.restart_log}"\n')
        report = self.tmp / "report.json"
        result = self.place("--restart", "--report", str(report), PLACE_RELEASE_WAIT_SECONDS="0",
                            BROCA_TEST_BIN=str(self.dest))
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        self.assertTrue(self.restart_log.exists(), result.stderr)
        saved = json.loads(report.read_text())
        self.assertEqual(set(saved["client_backups"]), {"broca-session"})
        for name, original in originals.items():
            self.assertEqual((self.bindir / name).read_bytes(), (self.tmp / f"{name}.staged").read_bytes())
            backup = Path(saved["client_backups"][name])
            self.assertEqual(backup.read_bytes(), original)
            check = subprocess.run(["shasum", "-c", backup.name + ".sha256"], cwd=backup.parent,
                                   text=True, capture_output=True)
            self.assertEqual(check.returncode, 0, check.stdout + check.stderr)

    def restart_args_with_listed_broca(self, broca):
        """The arguments of the restart, when `ck --json module list` lists `broca` as broca."""
        listing = self.tmp / "modules.json"
        listing.write_text(json.dumps({"modules": [
            {"module_id": "other", "drain_timeout_ms": 5000,
             "pending": [{"field": "drain_timeout_ms", "value": 7000}]},
            dict(broca, module_id="broca")]}))
        ck = self.fake_bin / "ck"
        ck.write_text(f'''#!/bin/sh
if [ "$*" = "--json module list" ]; then cat "{listing}"; exit 0; fi
echo "$*" >> "{self.restart_log}"
''')
        result = self.place("--restart", PLACE_RELEASE_WAIT_SECONDS="0", BROCA_TEST_BIN=str(self.dest))
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        return [line for line in self.restart_log.read_text().splitlines()
                if line.startswith("module restart")]

    def test_a_pending_drain_budget_is_passed_to_the_restart(self):
        # The daemon holds 30 s; the entry's 10 minutes applies only at its next launch of broca.
        self.assertEqual(self.restart_args_with_listed_broca({
            "drain_timeout_ms": 30000,
            "pending": [{"applies": "with_next_launch_or_health_change",
                         "field": "drain_timeout_ms", "value": 600000}]}),
            ["module restart broca --drain-ms 600000"])

    def test_an_applied_drain_budget_leaves_the_restart_plain(self):
        self.assertEqual(self.restart_args_with_listed_broca({"drain_timeout_ms": 600000}),
                         ["module restart broca"])

    def test_placement_keeps_only_the_newest_rollbacks_of_each_binary(self):
        rollback = self.rollback
        rollback.mkdir(parents=True, exist_ok=True)
        seeded = {}
        for name in ("ck-broca", "broca-session"):
            seeded[name] = []
            for day in range(1, 6):
                snapshot = rollback / f"{name}.rollback-202001{day:02d}T000000Z"
                snapshot.write_text("old")
                (rollback / f"{snapshot.name}.sha256").write_text("sum")
                if name == "ck-broca":
                    (rollback / f"{snapshot.name}.dSYM").mkdir()
                seeded[name].append(snapshot.name)
        others = ["ck-broca.current", "ck-other.rollback-20200101T000000Z",
                  "broca-session.rollback-notes"]
        for other in others:
            (rollback / other).write_text("keep")
        result = self.place("--restart", PLACE_RELEASE_WAIT_SECONDS="0", BROCA_TEST_BIN=str(self.dest))
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        names = {path.name for path in rollback.iterdir()}
        for name, old in seeded.items():
            snapshots = sorted(n for n in names
                               if n.startswith(f"{name}.rollback-") and n not in others
                               and not n.endswith((".sha256", ".dSYM")))
            # The snapshot this placement wrote, and the two newest before it.
            self.assertEqual(len(snapshots), 3, sorted(names))
            self.assertEqual(snapshots[:2], old[-2:])
            for gone in old[:-2]:
                self.assertNotIn(gone, names)
                self.assertNotIn(f"{gone}.sha256", names)
                self.assertNotIn(f"{gone}.dSYM", names)
            for kept in snapshots:
                self.assertIn(f"{kept}.sha256", names)
        for other in others:
            self.assertIn(other, names)

    def test_client_rename_failure_stops_before_module_placement_and_restart(self):
        before = self.dest.read_bytes()
        mv = self.fake_bin / "mv"
        mv.write_text('#!/bin/sh\nfor target; do :; done\n' +
                      f'[ "$target" != "{self.bindir / "broca-session"}" ] || exit 17\n' +
                      f'exec "{shutil.which("mv")}" "$@"\n')
        mv.chmod(0o755)
        result = self.place("--restart")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertFalse(self.restart_log.exists())

    def test_rollback_can_restore_absent_clients(self):
        result = self.place("--remove-session")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((self.bindir / "broca-session").exists())


class HealthSnapshotTests(gate.GateBase):
    deploy = gate.PlacementGateTests.deploy
    restart_fixture = gate.PlacementGateTests.restart_fixture

    def test_health_snapshot_precedes_restart_warns_on_failure_and_keeps_newest_50(self):
        home = self.tmp / "home"
        snapshots = home / ".local/state/cortexkit/broca/health-before-restart"
        restarted = self.tmp / "restarted"
        health = json.dumps({
            "module_id": "broca", "status": "ok", "detail": None,
            "metrics": {"session_readers": {"entries": [{"principal": "direct", "count": 37}]},
                        "management_ops": {"session.read": 91}},
            "extra": {"future_field": [1, 2, 3]},
        }, indent=2) + "\n"
        for mode, failure in (("--restart", ""), ("--restart-only", ""),
                              ("--restart-only", "ck"), ("--restart-only", "directory"),
                              ("--restart-only", "json")):
            with self.subTest(mode=mode, failure=failure):
                env = self.restart_fixture()
                env["HOME"] = str(home)
                restarted.unlink(missing_ok=True)
                if snapshots.is_file():
                    snapshots.unlink()
                else:
                    shutil.rmtree(snapshots, ignore_errors=True)
                snapshots.parent.mkdir(parents=True, exist_ok=True)
                seeded = []
                if failure == "directory":
                    snapshots.write_text("not a directory")
                else:
                    snapshots.mkdir(mode=0o755)
                    for index in range(55):
                        path = snapshots / f"20000101T0000{index:02d}.000000Z-1.json"
                        path.write_text("{}\n")
                        seeded.append(path)
                ck = Path(env["PLACE_RELEASE_CK"])
                ck.write_text(f'''#!/usr/bin/env python3
import os, pathlib, stat, sys
directory = pathlib.Path({str(snapshots)!r})
restarted = pathlib.Path({str(restarted)!r})
health = {health!r}
failure = {failure!r}
if sys.argv[1:] == ["--json", "health", "broca"]:
    assert not restarted.exists(), "health was requested after restart"
    assert not list(directory.glob("*-4242.json")), "snapshot published before health completed"
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(os.fstat(1).st_mode) == 0o600
    sys.stdout.write(health[:20])
    sys.stdout.flush()
    assert not list(directory.glob("*-4242.json")), "partial reply must stay private"
    if failure == "ck":
        print("health unavailable\\ntry later", file=sys.stderr)
        sys.exit(7)
    if failure != "json":
        sys.stdout.write(health[20:])
elif sys.argv[1:] == ["module", "restart", "broca"]:
    if failure == "directory":
        assert directory.is_file()
    else:
        saved = list(directory.glob("*-4242.json"))
        assert not list(directory.glob(".health-*")), "temporary reply leaked"
        if failure:
            assert not saved, "failed health must not publish a snapshot"
            assert len(list(directory.glob("*.json"))) == 55
        else:
            assert len(saved) == 1, "full health must be saved before restarting"
            assert saved[0].read_bytes() == health.encode(), "health reply was changed"
            assert stat.S_IMODE(saved[0].stat().st_mode) == 0o600
            assert len(list(directory.glob("*.json"))) == 50
    restarted.touch()
else:
    raise AssertionError(sys.argv)
''')
                result = self.place(mode, **env)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertTrue(restarted.exists(), result.stderr)
                if failure:
                    warnings = result.stderr.splitlines()
                    self.assertEqual(len(warnings), 1, result.stderr)
                    self.assertIn("warning: could not save pre-restart health:", warnings[0])
                    if failure == "ck":
                        self.assertIn("exited 7: health unavailable try later", warnings[0])
                    elif failure == "directory":
                        self.assertIn("File exists", warnings[0])
                    else:
                        self.assertIn("Unterminated string", warnings[0])
                else:
                    self.assertEqual(result.stderr, "")
                    saved = list(snapshots.glob("*-4242.json"))
                    self.assertEqual(len(saved), 1)
                    self.assertRegex(saved[0].name, r"^\d{8}T\d{6}\.\d{6}Z-4242\.json$")
                    self.assertEqual(sorted(snapshots.glob("*.json")), sorted([*seeded[6:], *saved]))


class ClientVerificationTests(gate.GateBase):
    verify = gate.VerifyDeploySigningTests.verify
    def setUp(self):
        super().setUp()
        gate.stub_binary(self.dest, features=[], signed="adhoc,runtime")

    def test_placement_and_verification_leave_retired_runner_and_rollback_untouched(self):
        gate.stub_binary(self.staged, features=[], signed="adhoc,runtime")
        executed = self.tmp / "retired-runner-executed"
        legacy = [self.bindir / "broca-run", self.bindir / "broca-run.rollback"]
        for path in legacy:
            path.write_text(f'#!/bin/sh\ntouch "{executed}"\nexit 42\n')
            path.chmod(0o700)

        def snapshot(path):
            stat = path.stat()
            return path.read_bytes(), stat.st_ino, stat.st_mode, stat.st_mtime_ns

        before = {path: snapshot(path) for path in legacy}
        placed = self.place()
        self.assertEqual(placed.returncode, 0, placed.stdout + placed.stderr)
        verified = self.verify()
        self.assertEqual(verified.returncode, 0, verified.stdout + verified.stderr)
        self.assertEqual({path: snapshot(path) for path in legacy}, before)
        self.assertFalse(executed.exists())
        self.assertEqual(list(self.rollback.glob("broca-run*")), [])
        self.assertNotIn("broca-run", placed.stdout + placed.stderr + verified.stdout + verified.stderr)

    def test_wrong_client_version_fails_verification_even_with_matching_digest(self):
        client = self.bindir / "broca-session"
        gate.stub_client(client, "broca-session", "0.3.198")
        shutil.copy2(Path(str(client) + ".sha256.postsign"), self.tmp / "broca-session.staged.sha256.postsign")
        result = self.verify()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("client --version mismatch", result.stderr)

    def test_missing_placed_client_fails_verification(self):
        (self.bindir / "broca-session").unlink()
        result = self.verify()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("missing executable client", result.stderr)

    def test_client_digest_mismatch_fails_with_matching_version(self):
        with (self.bindir / "broca-session").open("ab") as output:
            output.write(b"\n# stale client bytes\n")
        result = self.verify()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("client sha256 mismatch", result.stderr)

    def test_missing_staged_client_sidecar_fails_verification(self):
        (self.tmp / "broca-session.staged.sha256.postsign").unlink()
        result = self.verify()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("missing staged client sidecar", result.stderr)


class ClientRollbackTests(deploy.DeployTests):
    def test_rollback_command_names_module_and_session_client(self):
        result = self.deploy(FAIL_STEP="verify")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        rollback = next(line for line in result.stderr.splitlines() if line.startswith("Rollback: "))
        for name in ("broca-session.rollback", "bin/ck-broca"):
            self.assertIn(name, rollback)
        self.assertIn("--session-staged", rollback)
        self.assertNotIn("broca-run", rollback)
        self.assertNotIn("--run-staged", rollback)

    def test_first_install_rollback_explicitly_restores_absent_clients(self):
        stub = self.scripts / "stub.py"
        stub.write_text(stub.read_text().replace('str(home / (client + ".rollback"))', '""'))
        self.commit()
        result = self.deploy(FAIL_STEP="verify")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        rollback = next(line for line in result.stderr.splitlines() if line.startswith("Rollback: "))
        self.assertIn("--remove-session", rollback)
        self.assertNotIn("--remove-run", rollback)


if __name__ == "__main__":
    unittest.main()
