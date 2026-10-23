#!/usr/bin/env python3
"""Hermetic arms for place-release.sh --restart: the restart happens only when it's safe.

THE INCIDENT (2026-09-24). A watcher read a quiet window, a run started in the seconds after,
and place-release.sh correctly refused. The operator's hand-typed `ck module restart broca`
ran anyway, and the shutdown seal cut the run, to reload the same binary that was already
running. A restart that doesn't depend on the placement can't know the placement was refused.

--restart moves the restart inside the script, behind two conditions: the placement
succeeded, and a SECOND live-run check, taken just before the restart, is still clear. These
arms drive the real script with a scripted gate (PLACE_RELEASE_GATE_CMD, answering from a list
of exit codes), a fake `ck` that records restart requests, and fake ps/lsof process images.
The staged and deployed files are compiled Mach-O binaries, so file-kind and signing
checks run for real instead of being stubbed out. They answer `--version` like ck-broca and
exit 0 on everything else, so the placement gate's smoke run passes and
`--wal-known-features` reports an empty table.
"""

from __future__ import annotations

import os
import hashlib
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLACE = ROOT / "scripts" / "place-release.sh"


class RestartTests(unittest.TestCase):
    def setUp(self) -> None:
        if shutil.which("codesign") is None or os.uname().sysname != "Darwin":
            reason = "place-release.sh places Mach-O binaries; these arms need macOS"
            # script-suites.sh sets this on a Mac, where a skip would hide a broken tool setup.
            if os.environ.get("BROCA_REQUIRE_NATIVE_MACOS") == "1":
                self.fail(f"BROCA_REQUIRE_NATIVE_MACOS=1 but {reason}")
            self.skipTest(reason)
        self.tmp = Path(tempfile.mkdtemp(prefix="place-release-restart-"))
        # Two real, single-architecture Mach-O binaries with different bytes, built here so the
        # script's file-kind comparison and signing run for real. (System binaries are
        # universal, which is not what the script places.)
        self.dest = self.tmp / "ck-broca"
        self.staged = self.tmp / "ck-broca.staged"
        for out, word in ((self.dest, "deployed"), (self.staged, "staged")):
            src = self.tmp / f"{word}.c"
            src.write_text(
                "#include <stdio.h>\n#include <string.h>\n"
                "int main(int argc, char **argv){"
                'if (argc > 1 && strcmp(argv[1], "--version") == 0) puts("ck-broca 0.0.1");'
                f'return sizeof("{word}") == 0;}}\n'
            )
            subprocess.run(["cc", "-g", "-o", str(out), str(src)], check=True)
            # Placement on macOS refuses a binary without an adjacent dSYM whose UUID matches,
            # so each fixture gets a real one rather than a stand-in directory.
            subprocess.run(
                ["dsymutil", str(out), "-o", f"{out}.dSYM"],
                check=True, capture_output=True,
            )
        self.old_inode = self.dest.stat().st_ino
        for client in ("broca-session", "broca-run"):
            src = self.tmp / f"{client}.c"
            src.write_text(f'#include <stdio.h>\nint main(void) {{ puts("{client} 0.0.1"); return 0; }}\n')
            subprocess.run(["cc", "-o", str(self.tmp / f"{client}.staged"), str(src)], check=True)
            shutil.copy2(self.tmp / f"{client}.staged", self.tmp / client)
        self.rollback = self.tmp / "rollback"
        self.ck_log = self.tmp / "ck.log"
        ck = self.tmp / "ck"
        ck.write_text(f'#!/bin/sh\necho "$@" >> "{self.ck_log}"\n')
        ck.chmod(0o755)
        self.ck = ck
        self.fake_bin = self.tmp / "fake-bin"
        self.fake_bin.mkdir()
        ps = self.fake_bin / "ps"
        ps.write_text(f'''#!/bin/sh
if [ "$1" = -p ]; then
  exit 0
fi
if [ -f "{self.ck_log}" ]; then
  n=$(cat "{self.tmp / 'polls'}" 2>/dev/null || echo 0)
  n=$((n+1))
  echo "$n" > "{self.tmp / 'polls'}"
  case "$PLACE_RELEASE_TEST_PROCESS" in
    persistent) pid=111 ;;
    delayed) if [ "$n" -le 2 ]; then pid=111; else pid=222; fi ;;
    *) pid=222 ;;
  esac
else
  pid=111
fi
printf '  PID COMMAND\\n%s %s --subc socket\\n' "$pid" "{self.dest}"
if [ -n "$PLACE_RELEASE_TEST_BUSY" ]; then
  # A real process table keeps writing after the matching row. A reader that stops at the
  # first match closes the pipe on this writer, which dies of SIGPIPE (status 141).
  yes "99999 /usr/bin/other-process" | head -n 200000
fi
''')
        ps.chmod(0o755)
        lsof = self.fake_bin / "lsof"
        lsof.write_text(f'''#!/bin/sh
pid=$2
inode={self.old_inode}
if [ "$pid" = 222 ] && [ "$PLACE_RELEASE_TEST_PROCESS" != stale ]; then
  inode=$(stat -f %i "{self.dest}")
fi
printf 'COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\\nck-broca %s user txt REG 1 1 %s %s\\n' "$pid" "$inode" "{self.dest}"
''')
        lsof.chmod(0o755)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def place(
        self, gate_codes: list[int], *flags: str, process: str = "immediate", busy: bool = False,
        restart_only: bool = False, wait_seconds: int = 1,
    ) -> subprocess.CompletedProcess[str]:
        """Run the real script. The gate answers each call with the next code in the list."""
        counter = self.tmp / "gate-calls"
        counter.write_text("0")
        codes = " ".join(str(c) for c in gate_codes)
        gate = (
            f'n=$(cat "{counter}"); echo $((n+1)) > "{counter}"; '
            f'set -- {codes}; shift $n; echo "live runs          $1"; exit "$1"'
        )
        env = {
            **os.environ,
            "PLACE_RELEASE_GATE_CMD": gate,
            "PLACE_RELEASE_CK": str(self.ck),
            "ROLLBACK_DIR": str(self.rollback),
            "PATH": f"{self.fake_bin}:{os.environ['PATH']}",
            "PLACE_RELEASE_TEST_PROCESS": process,
            "PLACE_RELEASE_WAIT_SECONDS": str(wait_seconds),
            "PLACE_RELEASE_POLL_SECONDS": "0.05",
            "PLACE_RELEASE_TEST_BUSY": "1" if busy else "",
            # A private state root, so the host's real WAL feature floor cannot decide these arms.
            "BROCA_STATE_ROOT": str(self.tmp / "state"),
        }
        return subprocess.run(
            ["bash", str(PLACE), *(["--restart-only"] if restart_only else ["--staged", str(self.staged)]),
             "--dest", str(self.dest), *flags],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

    def restarted(self) -> bool:
        return self.ck_log.exists() and "module restart broca" in self.ck_log.read_text()

    def test_client_resigning_preserves_staged_postsign_hashes(self) -> None:
        expected = {}
        for client in ("broca-session", "broca-run"):
            staged = self.tmp / f"{client}.staged"
            subprocess.run(["codesign", "--force", "--sign", "-", "-o", "runtime",
                            "--identifier", client, str(staged)], check=True, capture_output=True)
            expected[client] = hashlib.sha256(staged.read_bytes()).hexdigest()
        result = self.place([0])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for client, sha in expected.items():
            self.assertEqual(hashlib.sha256((self.tmp / client).read_bytes()).hexdigest(), sha)

    def test_restart_only_clear_gate_restarts_and_prints_new_pid(self) -> None:
        before = self.dest.read_bytes()
        result = self.place([0], restart_only=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(self.restarted())
        self.assertIn("pid 222", result.stdout)
        self.assertEqual(self.dest.read_bytes(), before, "restart-only must not place bytes")
        self.assertFalse(self.rollback.exists(), "restart-only must not create a rollback")
        self.assertEqual((self.tmp / "gate-calls").read_text().strip(), "1")

    def test_restart_only_live_run_refuses_without_restarting(self) -> None:
        before = self.dest.read_bytes()
        result = self.place([1], restart_only=True)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertFalse(self.restarted(), "a live run must block restart-only")
        self.assertEqual(self.dest.read_bytes(), before)
        self.assertEqual((self.tmp / "gate-calls").read_text().strip(), "1")

    def test_restart_only_old_image_stays_times_out(self) -> None:
        result = self.place([0], restart_only=True, process="persistent")
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        self.assertTrue(self.restarted())
        self.assertIn(f"latest pid=111 inode={self.old_inode}", result.stderr)

    def test_restart_only_rejects_staged(self) -> None:
        result = self.place([0], "--staged", str(self.staged), restart_only=True)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("--restart-only cannot be combined with --staged", result.stderr)
        self.assertFalse(self.restarted())
        self.assertEqual((self.tmp / "gate-calls").read_text().strip(), "0")

    def test_a_refused_placement_never_restarts(self) -> None:
        """THE INCIDENT. The first gate sees a live run, so the placement is refused, and the
        restart must not happen: nothing new was placed, so a restart could only cut the run
        for nothing."""
        before = self.dest.read_bytes()
        result = self.place([1], "--restart")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertFalse(self.restarted(), "a refused placement must never restart the module")
        self.assertEqual(self.dest.read_bytes(), before, "a refused placement must place nothing")

    def test_a_run_that_starts_after_the_first_check_blocks_the_restart(self) -> None:
        """The window that cut the run: clear at the first check, live by the time of the
        restart. The new bytes are placed but not loaded, and the exit code says so."""
        result = self.place([0, 1], "--restart")
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertFalse(self.restarted(), "a live run at the second check must block the restart")
        self.assertEqual(self.dest.read_bytes()[:4], self.staged.read_bytes()[:4])
        self.assertIn("PLACED but NOT restarted", result.stderr)
        self.assertIn("--restart-only", result.stderr)

    def test_a_clear_gate_at_both_checks_places_and_restarts(self) -> None:
        """THE CONTROL. Without it, a script that never restarted would pass both arms above."""
        result = self.place([0, 0], "--restart")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(self.restarted(), "a clear gate at both checks must restart")

    def test_delayed_new_image_prints_new_pid(self) -> None:
        """Do not return at restart; wait past old-image polls for the new pid."""
        result = self.place([0, 0], "--restart", process="delayed", wait_seconds=3)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("pid 222", result.stdout)
        self.assertGreater(int((self.tmp / "polls").read_text()), 2)

    def test_old_process_never_leaves_times_out(self) -> None:
        result = self.place([0, 0], "--restart", process="persistent")
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        self.assertIn(f"old pid=111 inode={self.old_inode}", result.stderr)
        self.assertIn(f"latest pid=111 inode={self.old_inode}", result.stderr)
        self.assertIn(f"placed inode={self.dest.stat().st_ino}", result.stderr)

    def test_new_pid_with_old_inode_is_not_accepted(self) -> None:
        result = self.place([0, 0], "--restart", process="stale")
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        self.assertIn(f"latest pid=222 inode={self.old_inode}", result.stderr)
        self.assertIn(f"placed inode={self.dest.stat().st_ino}", result.stderr)

    def test_a_long_process_table_does_not_kill_the_wait(self) -> None:
        """THE 2026-09-26 INCIDENT. With `pipefail`, a reader that `exit`s on its first match
        leaves ps writing into a closed pipe: status 141 killed place-release after placing
        and before restarting. The stub's table continues well past the matching row."""
        result = self.place([0, 0], "--restart", busy=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(self.restarted(), "a long process table must not stop the restart")
        self.assertIn("pid 222", result.stdout)

    def test_without_the_flag_nothing_restarts(self) -> None:
        """The flag is opt-in: an existing caller that doesn't pass it must see no restart."""
        result = self.place([0, 0])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(self.restarted())

    def test_plain_placement_with_live_run_succeeds_without_restarting(self) -> None:
        """Plain placement does not restart, so live runs do not block it."""
        result = self.place([1])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(self.restarted())
        self.assertIn("1 run is live -- binary is NOT loaded until a restart, which re-checks", result.stdout)
        self.assertEqual(self.dest.read_bytes()[:4], self.staged.read_bytes()[:4])


if __name__ == "__main__":
    unittest.main(verbosity=2)
