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
    def test_stage_builds_and_publishes_all_three_signed_binaries_sidecar_first(self):
        with tempfile.TemporaryDirectory(prefix="release clients ") as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            for name in ("stage-release.sh", "dsym.sh", "wal-floor.sh", "running-image.sh", "signing-posture.sh", "nonshipping-markers.sh"):
                shutil.copy2(ROOT / "scripts" / name, scripts / name)
            script = scripts / "stage-release.sh"
            script.write_text(script.read_text().replace(
                "BUILD_SCRATCH=/tmp/broca-release-staging", f'BUILD_SCRATCH="{root}/scratch"'))
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
mkdir -p target/release
for name in ck-broca broca-session broca-run; do
 case " $* " in *" --bin $name "*) ;; *) continue ;; esac
 printf '#!/bin/sh\necho "%s 0.0.2"\n' "$name" > "target/release/$name"
 chmod 755 "target/release/$name"
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
                       PUBLISH_LOG=str(log), CARGO_LOG=str(root / "cargo.log"), REAL_MV=real_mv)
            result = subprocess.run(["bash", str(script)], cwd=root, env=env, text=True,
                                    capture_output=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            args = (root / "cargo.log").read_text()
            self.assertIn("-p broca-module-serve -p broca-run", args)
            seam = root / "home/.local/share/cortexkit/staging"
            renames = log.read_text().splitlines()
            for name in ("ck-broca", "broca-session", "broca-run"):
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
        for name in ("broca-session", "broca-run"):
            gate.stub_client(self.bindir / name, name, "0.3.148")
        self.restart_log = self.tmp / "restart.log"
        ck = self.fake_bin / "ck"
        ck.write_text(f'#!/bin/sh\necho restart >> "{self.restart_log}"\n')
        ck.chmod(0o755)

    def test_missing_client_refuses_before_any_placement_or_restart(self):
        before = self.dest.read_bytes()
        (self.tmp / "broca-run.staged").unlink()
        result = self.place("--restart")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("client-missing", result.stderr)
        self.assertIn("broca-run", result.stderr)
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertFalse(self.restart_log.exists())

    def test_client_version_mismatch_refuses_before_restart(self):
        before = self.dest.read_bytes()
        gate.stub_client(self.tmp / "broca-run.staged", "broca-run", "0.3.198")
        result = self.place("--restart")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("client-version", result.stderr)
        self.assertIn("0.3.198", result.stderr)
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertFalse(self.restart_log.exists())

    def test_clients_rename_before_restart_and_keep_verifiable_backups(self):
        originals = {name: (self.bindir / name).read_bytes() for name in ("broca-session", "broca-run")}
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
        for name, original in originals.items():
            self.assertEqual((self.bindir / name).read_bytes(), (self.tmp / f"{name}.staged").read_bytes())
            backup = Path(saved["client_backups"][name])
            self.assertEqual(backup.read_bytes(), original)
            check = subprocess.run(["shasum", "-c", backup.name + ".sha256"], cwd=backup.parent,
                                   text=True, capture_output=True)
            self.assertEqual(check.returncode, 0, check.stdout + check.stderr)

    def test_client_rename_failure_stops_before_module_placement_and_restart(self):
        before = self.dest.read_bytes()
        mv = self.fake_bin / "mv"
        mv.write_text('#!/bin/sh\nfor target; do :; done\n' +
                      f'[ "$target" != "{self.bindir / "broca-run"}" ] || exit 17\n' +
                      f'exec "{shutil.which("mv")}" "$@"\n')
        mv.chmod(0o755)
        result = self.place("--restart")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertFalse(self.restart_log.exists())

    def test_rollback_can_restore_absent_clients(self):
        result = self.place("--remove-session", "--remove-run")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((self.bindir / "broca-session").exists())
        self.assertFalse((self.bindir / "broca-run").exists())


class ClientVerificationTests(gate.GateBase):
    verify = gate.VerifyDeploySigningTests.verify
    def setUp(self):
        super().setUp()
        gate.stub_binary(self.dest, features=[], signed="adhoc,runtime")

    def test_wrong_client_version_fails_verification_even_with_matching_digest(self):
        client = self.bindir / "broca-run"
        gate.stub_client(client, "broca-run", "0.3.198")
        shutil.copy2(Path(str(client) + ".sha256.postsign"), self.tmp / "broca-run.staged.sha256.postsign")
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
        (self.tmp / "broca-run.staged.sha256.postsign").unlink()
        result = self.verify()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("missing staged client sidecar", result.stderr)


class ClientRollbackTests(deploy.DeployTests):
    def test_rollback_command_names_module_and_both_clients(self):
        result = self.deploy(FAIL_STEP="verify")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        rollback = next(line for line in result.stderr.splitlines() if line.startswith("Rollback: "))
        for name in ("broca-session.rollback", "broca-run.rollback", "bin/ck-broca"):
            self.assertIn(name, rollback)
        self.assertIn("--session-staged", rollback)
        self.assertIn("--run-staged", rollback)

    def test_first_install_rollback_explicitly_restores_absent_clients(self):
        stub = self.scripts / "stub.py"
        stub.write_text(stub.read_text().replace('str(home / (client + ".rollback"))', '""'))
        self.commit()
        result = self.deploy(FAIL_STEP="verify")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        rollback = next(line for line in result.stderr.splitlines() if line.startswith("Rollback: "))
        self.assertIn("--remove-session", rollback)
        self.assertIn("--remove-run", rollback)


if __name__ == "__main__":
    unittest.main()
