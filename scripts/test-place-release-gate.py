#!/usr/bin/env python3
"""Hermetic arms for place-release.sh's placement gate and verify-deploy.sh's signing check.

WHAT THE GATE PROTECTS. The subconscious daemon hands each module a secret launch nonce that
proves the module is the process it started. Only the hardened runtime (code-signature flag
`runtime`, and no `com.apple.security.get-task-allow` entitlement) stops a same-user process
from attaching a debugger and reading it. place-release signs its copy again
before the rename, and `codesign --force` replaces the signature whole, so a hardened staged
artifact became an unhardened placed one for as long as that re-sign did not ask for
`-o runtime`. The gate therefore reads the SIGNED TEMP COPY, the bytes the rename publishes.

The WAL feature floor is the rollback half of the WAL reader gate: a build that cannot read
feature names this machine's WALs already use must not be placed.

HOW THESE RUN ANYWHERE. `codesign`, `file`, `stat`, `ps` and `lsof` are stubs on PATH, and the
binaries are shell scripts, so the arms run the real scripts on Linux CI as well as macOS. The
codesign stub behaves like the real one in the respect that matters: signing REPLACES any
earlier signature, and only an explicit `-o runtime` produces the `runtime` flag. It records
the signature inside the file's own bytes (a trailing comment line), so the posture travels
with the file through the rename, the way a real embedded signature does.
"""

from __future__ import annotations

import os
import hashlib
import importlib.util
import json
import shutil
import struct
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLACE = ROOT / "scripts" / "place-release.sh"
VERIFY = ROOT / "scripts" / "verify-deploy.sh"


def wal_frame(seq: int, record: dict) -> bytes:
    payload = json.dumps(record, separators=(",", ":")).encode()
    version, fence = 1, 1
    digest = hashlib.sha256(
        bytes((version,)) + struct.pack("<Q", seq) + struct.pack("<Q", fence) + payload
    ).digest()
    return struct.pack("<IBQQ", len(payload), version, seq, fence) + digest + payload


@unittest.skipUnless(sys.platform.startswith("linux"), "native Linux /proc and ELF tests")
class LinuxDeployTests(unittest.TestCase):
    """Use real ELF files and native file/ps/stat probes, never the Darwin signing stubs."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory(prefix="linux deploy ")
        self.tmp = Path(self.tmpdir.name)
        self.dest = self.tmp / "bin" / "ck-broca"
        self.dest.parent.mkdir()
        self.staged = self.tmp / "staged"
        self.state = self.tmp / "state"
        self.state.mkdir()
        self.processes: list[subprocess.Popen] = []
        source = self.tmp / "fixture.c"
        source.write_text('''
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/prctl.h>
int main(int argc, char **argv) {
    if (argc > 1 && !strcmp(argv[1], "--version")) {
        puts("ck-broca 0.0.1"); return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "--wal-known-features")) {
        puts("known-feature"); return 0;
    }
    if (getenv("NO_DUMP")) prctl(PR_SET_DUMPABLE, 0);
    for (;;) sleep(1);
}
''')
        subprocess.run(["cc", str(source), "-o", str(self.dest)], check=True, capture_output=True)
        shutil.copy2(self.dest, self.staged)
        with self.staged.open("ab") as output:
            output.write(b"\nnew-release-marker\n")
        for client in ("broca-session", "broca-run"):
            source.write_text(f'#include <stdio.h>\nint main(void) {{ puts("{client} 0.0.1"); return 0; }}\n')
            staged_client = self.tmp / f"{client}.staged"
            subprocess.run(["cc", str(source), "-o", str(staged_client)], check=True, capture_output=True)
            shutil.copy2(staged_client, self.dest.parent / client)
            write_client_sidecar(staged_client)
        self.env = dict(os.environ, BROCA_STATE_ROOT=str(self.state),
                        ROLLBACK_DIR=str(self.tmp / "rollback"))
        self.env.pop("PLACE_RELEASE_GATE_CMD", None)

    def tearDown(self) -> None:
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
        self.tmpdir.cleanup()

    def launch(self, **extra: str) -> subprocess.Popen:
        process = subprocess.Popen([str(self.dest)], env=dict(self.env, **extra))
        self.processes.append(process)
        return process

    def shell(self, command: str, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["bash", "-c", 'source scripts/running-image.sh; ' + command,
                               "test", *args], cwd=ROOT, env=self.env, capture_output=True,
                              text=True, timeout=30)

    def place(self, *, staged: Path | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["bash", str(PLACE), "--staged", str(staged or self.staged), "--dest", str(self.dest)],
                              env=self.env, capture_output=True,
                              text=True, timeout=60)

    def verify(self, sha: str, *, staged: Path | None = None,
               marker: str = "new-release-marker", control: str = "ck-broca") -> subprocess.CompletedProcess[str]:
        return subprocess.run(["bash", str(VERIFY), "--bin", str(self.dest), "--sha", sha, "--staged", str(staged or self.staged),
                               "--marker", marker, "--control", control],
                               env=self.env, capture_output=True, text=True, timeout=60)

    def test_linux_release_verification_uses_placed_artifact_client_sidecars(self) -> None:
        release = self.tmp / "release seam"
        release.mkdir()
        artifact = release / "ck-broca.deadbeef"
        shutil.copy2(self.staged, artifact)
        client_shas = {}
        for client in ("broca-session", "broca-run"):
            staged_client = release / f"{client}.deadbeef"
            shutil.copy2(self.tmp / f"{client}.staged", staged_client)
            # Same version, different bytes: stale fixture sidecars must not verify this release.
            with staged_client.open("ab") as output:
                output.write(b"\nrelease client trailer\n")
            write_client_sidecar(staged_client)
            client_shas[client] = hashlib.sha256(staged_client.read_bytes()).hexdigest()
        placed = self.place(staged=artifact)
        self.assertEqual(placed.returncode, 0, placed.stdout + placed.stderr)
        self.launch()
        sha = hashlib.sha256(artifact.read_bytes()).hexdigest()
        verified = self.verify(sha, staged=artifact)
        self.assertEqual(verified.returncode, 0, verified.stdout + verified.stderr)
        for client, digest in client_shas.items():
            self.assertIn(f"{self.dest.parent / client} -- version 0.0.1, sha256 {digest} MATCH", verified.stdout)

        stale = self.verify(sha)
        self.assertEqual(stale.returncode, 1, stale.stdout + stale.stderr)
        for client in client_shas:
            self.assertIn(f"client sha256 mismatch at {self.dest.parent / client}", stale.stderr)
            self.assertIn(str(self.tmp / f"{client}.staged.sha256.postsign"), stale.stderr)

    def test_linux_elf_placement_preserves_running_image_and_rollback(self) -> None:
        process = self.launch()
        before = self.dest.read_bytes()
        old_inode = os.stat(f"/proc/{process.pid}/exe").st_ino
        result = self.place()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.dest.read_bytes(), self.staged.read_bytes())
        self.assertNotEqual(self.dest.stat().st_ino, old_inode)
        self.assertEqual(os.stat(f"/proc/{process.pid}/exe").st_ino, old_inode)
        backups = list((self.tmp / "rollback").glob("ck-broca.rollback-*"))
        image = next(p for p in backups if not p.name.endswith(".sha256"))
        self.assertEqual(image.read_bytes(), before)
        sha = hashlib.sha256(self.staged.read_bytes()).hexdigest()
        stale = self.verify(sha)
        self.assertEqual(stale.returncode, 1, stale.stdout + stale.stderr)
        self.assertIn("MISMATCH -- the running process predates", stale.stdout)

    def test_linux_verify_confirms_running_inode_and_hash(self) -> None:
        result = self.place()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.launch()
        sha = hashlib.sha256(self.staged.read_bytes()).hexdigest()
        verified = self.verify(sha)
        self.assertEqual(verified.returncode, 0, verified.stdout + verified.stderr)
        self.assertIn("running sha256", verified.stdout)
        bad_sha = self.verify("0" * 64)
        self.assertEqual(bad_sha.returncode, 1, bad_sha.stdout + bad_sha.stderr)
        self.assertIn("MISMATCH -- placed bytes", bad_sha.stdout)

    def test_linux_running_hash_detects_stale_bytes(self) -> None:
        self.launch()
        placed = self.place()
        self.assertEqual(placed.returncode, 0, placed.stdout + placed.stderr)
        result = self.verify(hashlib.sha256(self.staged.read_bytes()).hexdigest())
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("MISMATCH -- running bytes differ", result.stdout)

    def test_linux_ambiguous_process_refuses_verification(self) -> None:
        self.launch()
        self.launch()
        result = self.verify(hashlib.sha256(self.dest.read_bytes()).hexdigest())
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("unknown process identity", result.stdout)

    def test_linux_non_elf_refuses_without_publication(self) -> None:
        before = self.dest.read_bytes()
        self.staged.write_text("#!/bin/sh\necho ck-broca 0.0.1\n")
        result = self.place()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("not a native executable", result.stderr)
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertFalse((self.tmp / "rollback").exists())

    def test_linux_stage_refuses_non_elf_build(self) -> None:
        source = (ROOT / "scripts/stage-release.sh").read_text()
        start = source.index('source "$ROOT/scripts/running-image.sh"')
        end = source.index("# REFUSE AN UNSTRIPPED RELEASE BINARY", start)
        env = dict(self.env, ROOT=str(ROOT), BUILT=str(self.staged))
        good = subprocess.run(["bash", "-c", source[start:end]], env=env, capture_output=True, text=True)
        self.assertEqual(good.returncode, 0, good.stdout + good.stderr)
        self.staged.write_text("not an ELF build")
        bad = subprocess.run(["bash", "-c", source[start:end]], env=env, capture_output=True, text=True)
        self.assertEqual(bad.returncode, 2, bad.stdout + bad.stderr)
        self.assertIn("not an ELF executable", bad.stderr)

    def test_linux_verify_refuses_process_exit_during_probes(self) -> None:
        placed = self.place()
        self.assertEqual(placed.returncode, 0, placed.stdout + placed.stderr)
        process = self.launch()
        tools = self.tmp / "probe-tools"
        tools.mkdir()
        probe = tools / "strings"
        # Only strings is instrumented: image identity and hashes still use the native tools.
        probe.write_text('#!/bin/sh\n"$REAL_STRINGS" "$@"\nkill "$EXIT_PID" 2>/dev/null || true\n')
        probe.chmod(0o755)
        real_strings = shutil.which("strings")
        assert real_strings is not None
        self.env.update(REAL_STRINGS=real_strings, EXIT_PID=str(process.pid),
                        PATH=f"{tools}:{os.environ['PATH']}")
        result = self.verify(hashlib.sha256(self.staged.read_bytes()).hexdigest())
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("running image changed or became unreadable", result.stdout)

    def test_linux_unknown_running_image_refuses_verification(self) -> None:
        self.launch(NO_DUMP="1")
        # Wait for the process to disable ptrace access, not merely for Popen to return.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.stat(f"/proc/{self.processes[-1].pid}/exe")
            except (PermissionError, FileNotFoundError):
                self.assertIsNone(self.processes[-1].poll(), "fixture must still be running")
                break
            time.sleep(0.01)
        else:
            self.fail("host did not enforce PR_SET_DUMPABLE=0 on /proc image access")
        identity = self.shell('broca_running_inode "$1"', str(self.processes[-1].pid))
        self.assertNotEqual(identity.returncode, 0, "unreadable running image must remain unknown")
        self.assertEqual(identity.stdout, "")
        # Some sandboxes hide non-dumpable processes from ps too. Exercise the stat boundary
        # directly so its refusal remains covered even when the earlier process check refuses.
        mapped = self.shell('broca_file_inode "/proc/$1/exe"', str(self.processes[-1].pid))
        self.assertNotEqual(mapped.returncode, 0, "failed stat must not invent an image identity")
        self.assertEqual(mapped.stdout, "")
        result = self.verify(hashlib.sha256(self.dest.read_bytes()).hexdigest())
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertTrue("could not read both inodes" in result.stdout or
                        "unknown process identity" in result.stdout or
                        "identity unknown" in result.stdout, result.stdout + result.stderr)

    def test_linux_process_start_uses_proc_not_bsd_date(self) -> None:
        started = time.time()
        self.launch()
        sys.path.insert(0, str(ROOT / "scripts"))
        try:
            spec = importlib.util.spec_from_file_location("deploy_live_runs", ROOT / "scripts/live-runs.py")
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            epoch = module.process_start_epoch(str(self.dest))
        finally:
            sys.path.pop(0)
        self.assertIsNotNone(epoch)
        self.assertLess(abs(epoch - started), 3)

    def test_linux_gate_checks_rigs_before_enabling_local_run(self) -> None:
        source = (ROOT / "scripts/gate.sh").read_text()
        start = source.index('if [ "$(uname -s)" = Linux ]; then')
        end = source.index("# The gate uses the machine's rustc wrapper", start)
        block = source[start:end]
        env = self.env.copy()
        names = ("SUBC_CORE_BIN", "FAKE_AFT_STUB_BIN", "CK_CLAUSTRUM_BIN", "CK_AUTH_BIN")
        for name in (*names, "BROCA_ALLOW_LINUX_GATE", "GITHUB_ACTIONS"):
            env.pop(name, None)
        refused = subprocess.run(["bash", "-c", block], env=env, capture_output=True, text=True)
        self.assertEqual(refused.returncode, 2, refused.stdout + refused.stderr)
        env.update({name: str(self.dest) for name in names})
        allowed = subprocess.run(["bash", "-c", block + '\n[ "$BROCA_ALLOW_LINUX_GATE" = 1 ]'],
                                 env=env, capture_output=True, text=True)
        self.assertEqual(allowed.returncode, 0, allowed.stdout + allowed.stderr)

    def test_real_linux_subc_stage_place_restart_verify(self) -> None:
        """Exercise production publication and restart with a real broca and supervisor."""
        required = os.environ.get("BROCA_REQUIRE_LINUX_DEPLOY") == "1"
        subc = os.environ.get("SUBC_CORE_BIN", "")
        built = os.environ.get("BROCA_DEPLOY_TEST_BIN", "")
        ck = str(Path(subc).with_name("ck")) if subc else ""
        missing = [p or "unset rig path" for p in (subc, ck, built) if not p or not os.access(p, os.X_OK)]
        if missing:
            if required:
                self.fail(f"real Linux deployment requires native binaries: {missing}")
            self.skipTest("real deploy binaries not supplied; set BROCA_REQUIRE_LINUX_DEPLOY=1 to require")
        self.assertTrue(Path(built).read_bytes().startswith(b"\x7fELF"), "broca must be a real ELF")
        shutil.copy2(built, self.dest)
        # An existing installation must differ in bytes, so placement has something to publish.
        # ELF loaders ignore this trailer; the supervisor still executes the actual broca image.
        with self.dest.open("ab") as output:
            output.write(b"\nprevious installation trailer\n")
        config = self.tmp / "config"
        runtime = self.tmp / "runtime"
        data = self.tmp / "data"
        for path in (config / "cortexkit", runtime, data):
            path.mkdir(parents=True)
        module_env = {"BROCA_STATE_ROOT": str(self.state),
                      "BROCA_PROVIDER_BASE_URL": "http://127.0.0.1:9"}
        (config / "cortexkit/subc.jsonc").write_text(json.dumps({"version": 1, "modules": {
            "broca": {"program": str(self.dest), "args": [], "env": module_env,
                      "enabled": True, "health": {"cadence_ms": 200, "deadline_ms": 1000}}
        }}))
        self.env.update(XDG_CONFIG_HOME=str(config), XDG_RUNTIME_DIR=str(runtime),
                        XDG_DATA_HOME=str(data), SUBC_PORT="0", HOME=str(self.tmp / "home"),
                        PLACE_RELEASE_CK=ck, PLACE_RELEASE_WAIT_SECONDS="60")
        # The isolated HOME must not discard the runner's trust for this mounted worktree.
        self.env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="safe.directory",
                        GIT_CONFIG_VALUE_0=str(ROOT))
        for name in ("SUBC_MODULE_ID", "SUBC_LAUNCH_NONCE", "SUBC_LAUNCH_SECRET_FD",
                     "SUBC_CONNECTION", "SUBC_CONNECTION_FILE"):
            self.env.pop(name, None)
        log = self.tmp / "subc.log"
        with log.open("wb") as output:
            supervisor = subprocess.Popen([subc], env=self.env, stdout=output, stderr=output,
                                          start_new_session=True)
        self.processes.append(supervisor)
        try:
            deadline = time.monotonic() + 60
            old_pid = ""
            while time.monotonic() < deadline:
                found = self.shell('broca_pid_for_binary "$1"', str(self.dest))
                old_pid = found.stdout.strip()
                if old_pid and (runtime / "subc-connection.json").exists():
                    break
                self.assertIsNone(supervisor.poll(), log.read_text())
                time.sleep(0.1)
            self.assertTrue(old_pid, log.read_text())
            # Empty WALs have no open run, but ensure the live-run gate really scans a root.
            (self.state / "wal").mkdir(exist_ok=True)
            (self.state / "wal/deploy-control.wal").touch()

            # Run the script's publication sequence, without its clean-tag build prerequisite.
            # The input is the real release ELF built by the caller, not a synthetic tool stub.
            source = (ROOT / "scripts/stage-release.sh").read_text()
            start = source.index('SEAM="$HOME/.local/share/cortexkit/staging"')
            end = source.index('echo "== staged"', start)
            scratch = self.tmp / "scratch"
            scratch.mkdir()
            env = dict(self.env, BUILT=built, BUILD_SCRATCH=str(scratch))
            # The slice skips the script's prologue, so load the helpers it calls, as the prologue does.
            prologue = "set -euo pipefail\nsource scripts/nonshipping-markers.sh\n"
            staged = subprocess.run(["bash", "-c", prologue + source[start:end]],
                                    cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)
            self.assertEqual(staged.returncode, 0, staged.stdout + staged.stderr)
            seam = Path(self.env["HOME"]) / ".local/share/cortexkit/staging"
            sha, name, _ = (seam / "ck-broca.current").read_text().split()
            artifact = seam / name
            self.assertEqual(hashlib.sha256(artifact.read_bytes()).hexdigest(), sha)
            self.assertEqual(artifact.read_bytes(), Path(built).read_bytes())
            placed = subprocess.run(["bash", str(PLACE), "--staged", str(artifact), "--dest",
                                     str(self.dest), "--restart"], env=self.env, capture_output=True,
                                    text=True, timeout=120)
            self.assertEqual(placed.returncode, 0, placed.stdout + placed.stderr + log.read_text())
            new_pid = self.shell('broca_pid_for_binary "$1"', str(self.dest)).stdout.strip()
            self.assertNotEqual(old_pid, new_pid)
            # Client sidecars must come from the release just placed, not the setup fixtures.
            verified = self.verify(sha, staged=artifact, marker="ck-broca", control="broca")
            self.assertEqual(verified.returncode, 0, verified.stdout + verified.stderr)
            self.assertIn("running sha256", verified.stdout)
            for client in ("broca-session", "broca-run"):
                self.assertIn(f"client   {self.dest.parent / client} -- version ", verified.stdout)
            print(f"real Linux deploy: pid {old_pid} -> {new_pid}; ELF sha256 {sha}; inode/hash MATCH")
        finally:
            # The supervisor and modules inherit this isolated process group, not the test's.
            import signal
            try:
                os.killpg(supervisor.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            supervisor.wait(timeout=20)


SIG_MARK = b"\n# stub-codesign "

CODESIGN_STUB = textwrap.dedent(
    '''\
    #!/usr/bin/env python3
    """Stand-in for macOS codesign, covering the invocations the release scripts make."""
    import os, sys

    MARK = b"\\n# stub-codesign "
    args = sys.argv[1:]
    log = os.environ.get("STUB_CODESIGN_LOG")
    if log:
        with open(log, "a") as f:
            f.write(" ".join(args) + "\\n")
    path = args[-1]

    def read_sig(data):
        i = data.rfind(MARK)
        if i < 0:
            return None
        fields = dict(
            kv.split("=", 1) for kv in data[i + len(MARK):].decode().split() if "=" in kv
        )
        return fields

    if "-s" in args or "--sign" in args:
        if os.environ.get("STUB_CODESIGN_FAIL"):
            print("errSecInternalComponent", file=sys.stderr)
            sys.exit(1)
        runtime = any(
            a in ("-o", "--options") and i + 1 < len(args) and "runtime" in args[i + 1].split(",")
            for i, a in enumerate(args)
        )
        # A codesign that drops the option: the only way to reach the runtime-flag check
        # through the real script, which now always asks for it.
        if os.environ.get("STUB_CODESIGN_DROP_RUNTIME"):
            runtime = False
        with open(path, "rb") as f:
            data = f.read()
        i = data.rfind(MARK)
        if i >= 0:
            data = data[:i]
        flags = "adhoc,runtime" if runtime else "adhoc"
        ents = os.environ.get("STUB_CODESIGN_ENTITLEMENTS", "")
        data += MARK + f"flags={flags} entitlements={ents}\\n".encode()
        with open(path, "wb") as f:
            f.write(data)
        sys.exit(0)

    if not os.path.exists(path):
        print(f"{path}: No such file or directory", file=sys.stderr)
        sys.exit(1)
    with open(path, "rb") as f:
        sig = read_sig(f.read())
    if sig is None:
        print(f"{path}: code object is not signed at all", file=sys.stderr)
        sys.exit(1)
    if "--entitlements" in args:
        print(f"Executable={path}", file=sys.stderr)
        if sig.get("entitlements"):
            key = "com.apple.security." + sig["entitlements"]
            print(f'<?xml version="1.0"?><plist version="1.0"><dict><key>{key}</key><true/></dict></plist>')
        sys.exit(0)
    hexflags = "0x10002" if "runtime" in sig["flags"].split(",") else "0x2"
    print(f"Executable={path}", file=sys.stderr)
    print(
        f"CodeDirectory v=20500 size=1 flags={hexflags}({sig['flags']}) hashes=1+2 location=embedded",
        file=sys.stderr,
    )
    sys.exit(0)
    '''
)

# `stat -Lf %i` (BSD) and `stat -Lc %i` (GNU) both answer the inode here, so the scripts' inode
# reads are exact on either platform.
STAT_STUB = textwrap.dedent(
    '''\
    #!/usr/bin/env python3
    import os, sys
    args = sys.argv[1:]
    if len(args) == 3 and args[0] in ("-Lf", "-Lc") and args[1] == "%i":
        print(os.stat(args[2]).st_ino)
        sys.exit(0)
    print("stat stub: unsupported " + " ".join(args), file=sys.stderr)
    sys.exit(2)
    '''
)

# One daemon process, pid 4242, running whatever file BROCA_TEST_BIN names.
PS_STUB = textwrap.dedent(
    '''\
    #!/usr/bin/env python3
    import os, sys
    if sys.argv[1:2] == ["-p"]:
        sys.exit(0 if sys.argv[2] == "4242" else 1)
    print("  PID COMMAND")
    print(f"4242 {os.environ['BROCA_TEST_BIN']} --subc socket")
    '''
)

LSOF_STUB = textwrap.dedent(
    '''\
    #!/usr/bin/env python3
    import os, sys
    b = os.environ["BROCA_TEST_BIN"]
    print("COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME")
    print(f"ck-broca 4242 user txt REG 1 1 {os.stat(b).st_ino} {b}")
    '''
)


def stub_binary(
    path: Path, *, version: str | None = "0.3.149", features: list[str] | None = None,
    signed: str | None = None,
) -> None:
    """A shell script standing in for ck-broca.

    version=None makes `--version` fail; features=None makes `--wal-known-features` an unknown
    flag (the pre-flag build, which exits 1 asking for --subc). `signed` pre-seeds a signature
    with those flags, as staging or an earlier placement would have left it.
    """
    smoke = f'echo "ck-broca {version}"' if version is not None else "echo boom >&2; exit 1"
    if features is None:
        known = 'echo \'Error: "missing required --subc <connection-file>"\' >&2; exit 1'
    else:
        known = "".join(f'echo "{n}"; ' for n in features) + "exit 0"
    body = (
        "#!/bin/sh\n"
        f"# identity {path.name}\n"
        'case "$1" in\n'
        f"  --version) {smoke} ;;\n"
        f"  --wal-known-features) {known} ;;\n"
        "  *) echo 'Error: \"missing required --subc <connection-file>\"' >&2; exit 1 ;;\n"
        "esac\n"
    ).encode()
    if signed is not None:
        body += SIG_MARK + f"flags={signed} entitlements=\n".encode()
    path.write_bytes(body)
    path.chmod(0o755)
    dsym = Path(str(path) + ".dSYM")
    if dsym.exists():
        shutil.rmtree(dsym)
    dwarf = dsym / "Contents" / "Resources" / "DWARF"
    dwarf.mkdir(parents=True)
    (dwarf / "ck-broca").write_bytes(f"debug symbols for {path.name} {version}".encode())


def write_client_sidecar(path: Path) -> None:
    Path(str(path) + ".sha256.postsign").write_text(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
    )


def stub_client(path: Path, name: str, version: str = "0.3.149") -> None:
    stub_binary(path, version=version, signed="adhoc,runtime")
    path.write_bytes(path.read_bytes().replace(b"ck-broca ", f"{name} ".encode()))
    write_client_sidecar(path)


def read_flags(path: Path) -> str | None:
    data = path.read_bytes()
    i = data.rfind(SIG_MARK)
    if i < 0:
        return None
    for kv in data[i + len(SIG_MARK):].decode().split():
        if kv.startswith("flags="):
            return kv[len("flags="):]
    return None


class GateBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="place-release-gate-"))
        self.bindir = self.tmp / "bin"
        self.bindir.mkdir()
        self.dest = self.bindir / "ck-broca"
        self.staged = self.tmp / "ck-broca.staged"
        self.rollback = self.tmp / "rollback"
        self.state = self.tmp / "state"
        self.state.mkdir()
        self.floor = self.state / "wal-feature-floor"
        self.codesign_log = self.tmp / "codesign.log"
        self.fake_bin = self.tmp / "fake-bin"
        self.fake_bin.mkdir()
        for client in ("broca-session", "broca-run"):
            stub_client(self.tmp / f"{client}.staged", client)
            shutil.copy2(self.tmp / f"{client}.staged", self.bindir / client)
        for name, body in (
            ("codesign", CODESIGN_STUB),
            ("stat", STAT_STUB),
            ("ps", PS_STUB),
            ("lsof", LSOF_STUB),
            # The scripts compare `file -b` kinds and only place Mach-O; the stand-in binaries
            # are scripts, so both sides answer the same Mach-O kind here.
            ("file", '#!/bin/sh\necho "Mach-O 64-bit executable arm64"\n'),
            # These hermetic arms cover Darwin signing even on a Linux test runner.
            ("uname", '#!/bin/sh\necho Darwin\n'),
            ("dwarfdump", '#!/bin/sh\nprintf "UUID: 11111111-2222-3333-4444-555555555555 (arm64) %s\\n" "$2"\n'),
        ):
            p = self.fake_bin / name
            p.write_text(body)
            p.chmod(0o755)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def env(self, **extra: str) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith("STUB_CODESIGN")}
        env.update(
            PATH=f"{self.fake_bin}:{os.environ['PATH']}",
            PLACE_RELEASE_GATE_CMD="exit 0",
            ROLLBACK_DIR=str(self.rollback),
            BROCA_STATE_ROOT=str(self.state),
            STUB_CODESIGN_LOG=str(self.codesign_log),
            PLACE_RELEASE_RUN_SECONDS="20",
        )
        env.update(extra)
        return env

    def place(self, *args: str, **extra: str) -> subprocess.CompletedProcess[str]:
        cmd = ["bash", str(PLACE)]
        if "--restart-only" not in args:
            cmd.extend(["--staged", str(self.staged)])
        cmd.extend(["--dest", str(self.dest)])
        cmd.extend(args)
        return subprocess.run(
            cmd,
            env=self.env(**extra),
            capture_output=True,
            text=True,
            timeout=120,
        )


class PlacementGateTests(GateBase):
    """One arm per refusal, each asserting the check's name, the file it read, and that nothing
    moved; then the positive arms, without which a gate that refused everything would pass."""

    def assert_refused(self, result: subprocess.CompletedProcess[str], check: str) -> None:
        out = result.stdout + result.stderr
        self.assertEqual(result.returncode, 1, out)
        self.assertIn(f"placement gate check '{check}' failed", result.stderr, out)
        # The refusal names the file it read: the signed temp copy, never the staged file.
        self.assertIn(f"file  {self.dest}.place.", result.stderr, out)
        self.assertEqual(self.dest.read_bytes(), self.deployed_bytes, "a refusal must place nothing")
        self.assertFalse(self.rollback.exists(), "the gate runs before the rollback snapshot")
        leftovers = [p.name for p in self.bindir.iterdir() if ".place." in p.name]
        self.assertEqual(leftovers, [], "the temp copy must be removed on refusal")

    def deploy(self, **kw) -> None:
        stub_binary(self.dest, version="0.3.148", **kw)
        self.deployed_bytes = self.dest.read_bytes()

    # ---- refusals ------------------------------------------------------------------------

    def test_restart_with_live_run_still_refuses(self) -> None:
        """When --restart is passed, the live-run gate must refuse before placement so that
        in-flight runs are not cut."""
        self.deploy(signed="adhoc,runtime")
        stub_binary(self.staged, features=[], signed="adhoc,runtime")
        result = self.place(
            "--restart",
            PLACE_RELEASE_GATE_CMD='echo "live runs          1"; exit 1',
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("place-release: REFUSING -- a run is live. Restart cuts it.", result.stderr)
        self.assertEqual(self.dest.read_bytes(), self.deployed_bytes, "a refusal must place nothing")
        self.assertFalse(self.rollback.exists(), "the gate runs before the rollback snapshot")

    def test_restart_only_with_live_run_still_refuses(self) -> None:
        """When --restart-only is passed, the live-run gate must refuse without restarting."""
        self.deploy(signed="adhoc,runtime")
        result = self.place(
            "--restart-only",
            PLACE_RELEASE_GATE_CMD='echo "live runs          1"; exit 1',
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("place-release: REFUSING -- a run is live. Restart cuts it.", result.stderr)
        self.assertEqual(self.dest.read_bytes(), self.deployed_bytes, "restart-only must not place bytes")
        self.assertFalse(self.rollback.exists(), "restart-only must not create a rollback snapshot")

    def restart_fixture(self, *, new_pid=True, right_inode=True):
        self.deploy(signed="adhoc,runtime")
        stub_binary(self.staged, features=[], signed="adhoc,runtime")
        restarted = self.tmp / "restarted"
        ck = self.fake_bin / "ck"
        ck.write_text('#!/bin/sh\n' + (f'touch "{restarted}"\n' if new_pid else 'exit 0\n'))
        ck.chmod(0o755)
        (self.fake_bin / "ps").write_text(f'''#!/usr/bin/env python3
from pathlib import Path
pid = 4243 if Path({str(restarted)!r}).exists() else 4242
print("  PID COMMAND")
print(f"{{pid}} {self.dest} --subc socket")
''')
        (self.fake_bin / "lsof").write_text(f'''#!/usr/bin/env python3
from pathlib import Path
inode = Path({str(self.dest)!r}).stat().st_ino
if Path({str(restarted)!r}).exists() and not {right_inode!r}: inode = 1
print("COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME")
print(f"ck-broca 4243 user txt REG 1 1 {{inode}} {self.dest}")
''')
        return dict(PLACE_RELEASE_CK=str(ck), BROCA_TEST_BIN=str(self.dest),
                    PLACE_RELEASE_WAIT_SECONDS="0", PLACE_RELEASE_POLL_SECONDS="0")

    def test_live_approved_logs_approver_keeps_rollback_and_waits_for_new_image(self):
        env = self.restart_fixture()
        report = self.tmp / "report.json"
        wal = self.state / "wal"
        wal.mkdir()
        session = wal / "session.wal"
        unsealed = wal / "unsealed.wal"
        finished = wal / "finished.wal"
        session.write_bytes(
            wal_frame(1, {"type": "run_started", "run_id": "run-before"})
            + wal_frame(2, {"type": "step_started"})
        )
        # Listed by recent writes, but its last run had already finished: nothing to seal.
        finished.write_bytes(
            wal_frame(1, {"type": "run_started", "run_id": "run-done"})
            + wal_frame(2, {"type": "run_finished", "reason": "completed"})
        )
        unsealed.write_bytes(
            wal_frame(1, {"type": "run_started", "run_id": "run-open"})
            + wal_frame(2, {"type": "step_started"})
        )
        restart_records = self.tmp / "restart-records.wal"
        restart_records.write_bytes(
            wal_frame(3, {"type": "run_finished", "reason": "interrupted"})
            + wal_frame(4, {"type": "run_started", "run_id": "run-after"})
            + wal_frame(5, {"type": "step_started"})
        )
        restarted = self.tmp / "restarted"
        ck = Path(env["PLACE_RELEASE_CK"])
        ck.write_text(f'#!/bin/sh\ncat "{restart_records}" >> "{session}"\ntouch "{restarted}"\n')
        ck.chmod(0o755)
        result = self.place("--restart", "--live-approved", "--report", str(report),
                            PLACE_RELEASE_GATE_CMD='echo "live runs          3"; echo "  live  session.wal  last write 0s ago"; echo "  live  unsealed.wal  last write 0s ago"; echo "  live  finished.wal  last write 0s ago"; exit 1', **env)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("live restart approved by " + subprocess.check_output(["id", "-un"], text=True).strip(), result.stdout)
        self.assertIn("live runs accepted immediately before restart", result.stdout)
        self.assertIn("(pid 4243)", result.stdout)
        data = json.loads(report.read_text())
        self.assertEqual(data["pid"], "4243")
        self.assertEqual(data["sessions_at_restart"], ["session.wal", "unsealed.wal", "finished.wal"])
        self.assertEqual(data["active_runs_at_restart"], [
            {"session": "session.wal", "run_id": "run-before"},
            {"session": "unsealed.wal", "run_id": "run-open"},
            {"session": "finished.wal", "run_id": None, "no_open_run": True},
        ])
        self.assertEqual(data["sessions_sealed"], ["session.wal"])
        self.assertEqual(data["sessions_unconfirmed"], ["unsealed.wal"])
        self.assertEqual(data["sessions_idle"], ["finished.wal"])
        self.assertEqual(data["sessions_cancelled"], [])
        self.assertEqual(data["runs_sealed"], [{"session": "session.wal", "run_id": "run-before"}])
        self.assertEqual(data["runs_unconfirmed"], [{"session": "unsealed.wal", "run_id": "run-open"}])
        self.assertEqual(data["seal_error"], "")
        self.assertEqual(Path(data["backup"]).read_bytes(), self.deployed_bytes)
        self.assertTrue(Path(data["backup"] + ".sha256").exists())

    def test_shutdown_cancelled_terminal_is_reported_as_a_loud_failure(self):
        env = self.restart_fixture()
        report = self.tmp / "report.json"
        wal = self.state / "wal"
        wal.mkdir()
        session = wal / "session.wal"
        session.write_bytes(
            wal_frame(1, {"type": "run_started", "run_id": "run-cancelled"})
            + wal_frame(2, {"type": "step_started"})
        )
        restart_records = self.tmp / "restart-records.wal"
        restart_records.write_bytes(wal_frame(3, {"type": "run_finished", "reason": "cancelled"}))
        restarted = self.tmp / "restarted"
        ck = Path(env["PLACE_RELEASE_CK"])
        ck.write_text(f'#!/bin/sh\ncat "{restart_records}" >> "{session}"\ntouch "{restarted}"\n')
        ck.chmod(0o755)

        result = self.place(
            "--restart", "--live-approved", "--report", str(report),
            PLACE_RELEASE_GATE_CMD='echo "live runs          1"; echo "  live  session.wal  last write 0s ago"; exit 1',
            **env,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("FAILURE", result.stderr)
        self.assertIn("cancelled", result.stderr)
        data = json.loads(report.read_text())
        self.assertEqual(data["sessions_cancelled"], ["session.wal"])
        self.assertEqual(data["runs_cancelled"], [{"session": "session.wal", "run_id": "run-cancelled"}])

    def test_live_approved_accepts_a_run_that_starts_between_placement_and_restart(self):
        env = self.restart_fixture()
        counter = self.tmp / "gate-called"
        gate = f'if [ -f "{counter}" ]; then echo "live runs          1"; exit 1; fi; touch "{counter}"; exit 0'
        result = self.place("--restart", "--live-approved", PLACE_RELEASE_GATE_CMD=gate, **env)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("live runs accepted immediately before restart", result.stdout)
        self.assertIn("(pid 4243)", result.stdout)

    def test_live_approval_never_overrides_unknown_live_run_state(self):
        env = self.restart_fixture()
        gate = 'echo "cannot compare: probe reason one" >&2; exit 2'
        result = self.place("--restart", "--live-approved", PLACE_RELEASE_GATE_CMD=gate, **env)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("gate could not determine live runs", result.stderr)
        # The refusal names the gate's own reason, not only that it refused.
        self.assertIn("cannot compare: probe reason one", result.stderr)
        self.assertEqual(self.dest.read_bytes(), self.deployed_bytes)
        self.assertFalse(self.rollback.exists())

    def test_second_gate_that_cannot_tell_names_its_reason(self):
        # The re-check right before the restart must keep the gate's reason even when the gate
        # printed no live-run lines.
        env = self.restart_fixture()
        counter = self.tmp / "gate-called"
        gate = (
            f'if [ -f "{counter}" ]; then echo "cannot compare: probe reason two" >&2; exit 2; fi; '
            f'touch "{counter}"; exit 0'
        )
        result = self.place("--restart", "--live-approved", PLACE_RELEASE_GATE_CMD=gate, **env)
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertIn("no longer clear (exit 2)", result.stderr)
        self.assertIn("cannot compare: probe reason two", result.stderr)

    def test_live_approved_still_requires_a_new_pid(self):
        env = self.restart_fixture(new_pid=False)
        result = self.place("--restart", "--live-approved", PLACE_RELEASE_GATE_CMD="exit 1", **env)
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        self.assertIn("TIMED OUT waiting for placed image", result.stderr)

    def test_live_approved_still_requires_the_placed_inode(self):
        env = self.restart_fixture(right_inode=False)
        result = self.place("--restart", "--live-approved", PLACE_RELEASE_GATE_CMD="exit 1", **env)
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        self.assertIn("TIMED OUT waiting for placed image", result.stderr)

    def test_refuses_a_copy_without_the_runtime_flag(self) -> None:
        self.deploy(signed="adhoc")
        stub_binary(self.staged, features=[])
        result = self.place(STUB_CODESIGN_DROP_RUNTIME="1")
        self.assert_refused(result, "runtime-flag")
        self.assertIn("flags=adhoc,", result.stderr)

    def test_refuses_a_copy_with_get_task_allow(self) -> None:
        self.deploy(signed="adhoc")
        stub_binary(self.staged, features=[])
        result = self.place(STUB_CODESIGN_ENTITLEMENTS="get-task-allow")
        self.assert_refused(result, "get-task-allow")
        self.assertIn("com.apple.security.get-task-allow", result.stderr)

    def test_refuses_a_copy_whose_smoke_run_fails(self) -> None:
        self.deploy(signed="adhoc")
        stub_binary(self.staged, version=None, features=[])
        result = self.place()
        self.assert_refused(result, "smoke-run")
        self.assertIn("'--version' exited 1", result.stderr)

    def test_refuses_a_copy_that_runs_but_reports_no_version(self) -> None:
        """Exit 0 is not enough: a file that runs but is not ck-broca must not pass."""
        self.deploy(signed="adhoc")
        stub_binary(self.staged, features=[])
        self.staged.write_bytes(self.staged.read_bytes().replace(b"ck-broca 0.3.149", b"hello"))
        result = self.place()
        self.assert_refused(result, "smoke-run")

    def test_refuses_to_replace_a_hardened_binary_with_an_unhardened_one(self) -> None:
        self.deploy(signed="adhoc,runtime")
        stub_binary(self.staged, features=[])
        result = self.place(STUB_CODESIGN_DROP_RUNTIME="1")
        self.assert_refused(result, "hardened-downgrade")
        self.assertIn(f"deployed binary is hardened (flags=adhoc,runtime, {self.dest})", result.stderr)

    def test_refuses_a_build_that_does_not_cover_the_floor(self) -> None:
        self.deploy(signed="adhoc")
        self.floor.write_text("alpha\nbeta\n")
        stub_binary(self.staged, features=["alpha"])
        result = self.place()
        self.assert_refused(result, "wal-feature-floor")
        self.assertIn("does not know: beta", result.stderr)
        self.assertIn(str(self.floor), result.stderr)

    def test_refuses_a_build_that_cannot_read_v2_archive_indexes(self) -> None:
        """A build that seals v2 archive containers first adds `archive-index/v2` to the floor.
        The first stub reports v0.3.162's known-feature table, which predates that name, so
        placement must refuse it; the second reports a table that lists it, which places."""
        self.deploy(signed="adhoc,runtime")
        self.floor.write_text("archive-index/v2\nscope/v1\n")
        stub_binary(self.staged, features=["plan-manifest/v1", "scope/v1", "steer-queue/v1"])
        result = self.place()
        self.assert_refused(result, "wal-feature-floor")
        self.assertIn("does not know: archive-index/v2", result.stderr)
        stub_binary(self.staged, signed="adhoc,runtime", features=[
            "archive-index/v2", "plan-manifest/v1", "scope/v1", "steer-queue/v1"])
        result = self.place()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("floor     archive-index/v2, scope/v1 -- covered", result.stdout)

    def test_refuses_a_pre_flag_build_when_the_floor_is_not_empty(self) -> None:
        self.deploy(signed="adhoc")
        self.floor.write_text("alpha\n")
        stub_binary(self.staged, features=None)
        result = self.place()
        self.assert_refused(result, "wal-feature-floor")
        self.assertIn("'--wal-known-features' exited 1", result.stderr)

    def test_an_unreadable_floor_is_not_an_empty_one(self) -> None:
        if os.geteuid() == 0:
            self.skipTest("root reads a mode-000 file")
        self.deploy(signed="adhoc")
        self.floor.write_text("alpha\n")
        self.floor.chmod(0)
        stub_binary(self.staged, features=None)
        result = self.place()
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("cannot read the floor", result.stderr)
        self.assertEqual(self.dest.read_bytes(), self.deployed_bytes)

    # ---- positive arms -------------------------------------------------------------------

    def assert_placed(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("== placed atomically", result.stdout)
        self.assertIn(b"# identity ck-broca.staged", self.dest.read_bytes())

    def test_the_placement_re_sign_adds_the_hardened_runtime(self) -> None:
        """THE RE-SIGN DEFECT. The staged artifact arrives without `runtime` (as every one did
        before staging asked for it), the deployed one is unhardened too, and placement must
        still publish a hardened binary, because its own re-sign asks for `-o runtime` and keeps
        the identifier. Removing `-o runtime` from that re-sign fails this arm."""
        self.deploy(signed="adhoc")
        stub_binary(self.staged, features=[], signed="adhoc")
        result = self.place()
        self.assert_placed(result)
        self.assertEqual(read_flags(self.dest), "adhoc,runtime")
        self.assertIn("signing   flags=adhoc,runtime", result.stdout)
        sign_calls = [l for l in self.codesign_log.read_text().splitlines() if " -s " in f" {l} "]
        self.assertEqual(len(sign_calls), 3, sign_calls)
        for call, name in zip(sign_calls, ("ck-broca", "broca-session", "broca-run")):
            self.assertIn("-o runtime", call)
            self.assertIn(f"--identifier {name}", call)

    def test_placement_carries_the_dsym_and_keeps_the_old_one_with_rollback(self) -> None:
        self.deploy(signed="adhoc,runtime")
        old_dwarf = (self.dest.parent / "ck-broca.dSYM/Contents/Resources/DWARF/ck-broca").read_bytes()
        stub_binary(self.staged, features=[], signed="adhoc,runtime")
        new_dwarf = Path(str(self.staged) + ".dSYM/Contents/Resources/DWARF/ck-broca").read_bytes()

        result = self.place()

        self.assert_placed(result)
        self.assertIn(f"dSYM      {self.dest}.dSYM", result.stdout)
        self.assertEqual(
            (self.dest.parent / "ck-broca.dSYM/Contents/Resources/DWARF/ck-broca").read_bytes(),
            new_dwarf,
        )
        backups = [
            p for p in self.rollback.glob("ck-broca.rollback-*")
            if p.is_file() and not p.name.endswith(".sha256")
        ]
        self.assertEqual(len(backups), 1, backups)
        backup_dwarf = Path(str(backups[0]) + ".dSYM/Contents/Resources/DWARF/ck-broca")
        self.assertEqual(backup_dwarf.read_bytes(), old_dwarf)

    def test_darwin_placement_refuses_a_staged_binary_without_its_dsym(self) -> None:
        self.deploy(signed="adhoc,runtime")
        stub_binary(self.staged, features=[], signed="adhoc,runtime")
        shutil.rmtree(Path(str(self.staged) + ".dSYM"))

        result = self.place()

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("staged Mach-O has no dSYM directory", result.stderr)
        self.assertEqual(self.dest.read_bytes(), self.deployed_bytes)
        self.assertFalse(self.rollback.exists())

    def test_a_covered_floor_places_and_the_card_prints_it(self) -> None:
        self.deploy(signed="adhoc,runtime")
        self.floor.write_text("alpha\nbeta\n")
        stub_binary(self.staged, features=["alpha", "beta", "gamma"], signed="adhoc,runtime")
        result = self.place()
        self.assert_placed(result)
        self.assertIn("floor     alpha, beta -- covered", result.stdout)
        self.assertIn("version   ck-broca 0.3.149", result.stdout)

    def test_no_floor_file_places_a_pre_flag_build_as_today(self) -> None:
        self.deploy(signed="adhoc")
        stub_binary(self.staged, features=None)
        self.assertFalse(self.floor.exists())
        result = self.place()
        self.assert_placed(result)
        self.assertIn("floor     none -- empty, not checked", result.stdout)

    def test_an_empty_floor_file_places_a_pre_flag_build(self) -> None:
        self.deploy(signed="adhoc")
        self.floor.write_text("\n  \n")
        stub_binary(self.staged, features=None)
        result = self.place()
        self.assert_placed(result)

    def test_plain_placement_with_live_run_succeeds_and_prints_not_loaded_line(self) -> None:
        """A plain placement proceeds when runs are live because rename(2) is atomic and
        preserves the running process's inode. It prints the not-loaded line so operators
        know the new binary will take effect only after a restart."""
        self.deploy(signed="adhoc,runtime")
        stub_binary(self.staged, features=[], signed="adhoc,runtime")
        result = self.place(
            PLACE_RELEASE_GATE_CMD='echo "live runs          1"; exit 1'
        )
        self.assert_placed(result)
        self.assertIn(
            "== gate: 1 run is live -- binary is NOT loaded until a restart, which re-checks",
            result.stdout,
        )

    def test_plain_placement_with_multiple_live_runs_succeeds_and_prints_not_loaded_line(self) -> None:
        """Pluralization: when multiple runs are live, format as 'N runs are live'."""
        self.deploy(signed="adhoc,runtime")
        stub_binary(self.staged, features=[], signed="adhoc,runtime")
        result = self.place(
            PLACE_RELEASE_GATE_CMD='echo "live runs          3"; exit 1'
        )
        self.assert_placed(result)
        self.assertIn(
            "== gate: 3 runs are live -- binary is NOT loaded until a restart, which re-checks",
            result.stdout,
        )


class VerifyDeploySigningTests(GateBase):
    """verify-deploy reads the posture of the file the running process maps."""

    def verify(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(VERIFY), "--bin", str(self.dest), "--staged", str(self.staged),
             "--marker", "wal-known-features", "--control", "missing required"],
            env=self.env(BROCA_TEST_BIN=str(self.dest)),
            capture_output=True,
            text=True,
            timeout=60,
        )

    def test_a_hardened_running_image_passes(self) -> None:
        stub_binary(self.dest, features=[], signed="adhoc,runtime")
        result = self.verify()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("signing  flags=adhoc,runtime  hardened, no get-task-allow", result.stdout)

    def test_an_unhardened_running_image_fails(self) -> None:
        stub_binary(self.dest, features=[], signed="adhoc")
        result = self.verify()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("NOT HARDENED", result.stdout)
        self.assertIn(str(self.dest), result.stdout)

    def test_a_running_image_with_get_task_allow_fails(self) -> None:
        stub_binary(self.dest, features=[])
        self.dest.write_bytes(
            self.dest.read_bytes() + SIG_MARK + b"flags=adhoc,runtime entitlements=get-task-allow\n"
        )
        result = self.verify()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("GET-TASK-ALLOW", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
