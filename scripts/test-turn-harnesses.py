#!/usr/bin/env python3
"""Hermetic client turns for acceptance and lineage-specimen harnesses."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class TurnHarnesses(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="turn-harness-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        for name in ("acceptance-turn.sh", "mint-lineage-specimen.sh"):
            shutil.copy2(ROOT / "scripts" / name, scripts)
        (scripts / "lib").mkdir()
        shutil.copy2(ROOT / "scripts/lib/acceptance-models.txt", scripts / "lib")
        self.bins = self.root / "bin"
        self.bins.mkdir()
        self.home = self.root / "home"
        conn = self.home / ".local/share/cortexkit/run/subc-connection.json"
        conn.parent.mkdir(parents=True)
        conn.write_text("{}")
        self.state = self.root / "state"
        (self.state / "wal").mkdir(parents=True)
        (self.state / "run-index.db").touch()
        client = self.root / "target/release/broca-session"
        client.parent.mkdir(parents=True)
        self.executable(client, r'''#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
session = args[args.index("--session") + 1]
root = args[args.index("--project-root") + 1]
harness = args[args.index("--harness") + 1] if "--harness" in args else "runner"
h = 0xcbf29ce484222325
for b in f"{root}\x1f{harness}\x1f{session}".encode():
    h = ((h ^ b) * 0x100000001b3) & 0xffffffffffffffff
(pathlib.Path(os.environ["BROCA_STATE_ROOT"]) / "wal" / f"{h:016x}.wal").write_text(session)
print("TOOL_CALL read\nTOOL_RESULT is_error=false")
if os.environ.get("TURN_KIND") != "incomplete": print("RUN_FINISHED reason=Completed")
sys.stdout.flush()
if os.environ.get("TURN_KIND") == "large":
    for _ in range(512):
        sys.stdout.write("x" * 8192 + "\n")
    sys.stdout.flush()
''')
        self.executable(self.bins / "sqlite3", "#!/bin/sh\ncase \"$2\" in PRAGMA*) echo lineage_id;; *) echo stored-id;; esac\n")
        self.executable(self.bins / "timeout", '#!/bin/sh\nshift\nexec "$@"\n')
        self.executable(self.bins / "cp", r'''#!/usr/bin/env python3
import os, pathlib, shutil, sys
source, dest = sys.argv[1:]
if dest.startswith("/tmp/specimen-original-"):
    dest = str(pathlib.Path(os.environ["BACKUP_ROOT"]) / pathlib.Path(dest).name)
shutil.copyfile(source, dest)
''')
        self.env = {**os.environ, "HOME": str(self.home), "BROCA_STATE_ROOT": str(self.state),
                    "PATH": f"{self.bins}:{os.environ['PATH']}", "BACKUP_ROOT": str(self.root)}

    @staticmethod
    def executable(path, text):
        path.write_text(text)
        path.chmod(0o755)

    def run_script(self, name, **env):
        return subprocess.run(["bash", "scripts/" + name], cwd=self.root,
                              env={**self.env, **env}, capture_output=True, text=True, timeout=10)

    def test_missing_timeout_is_named_prerequisite_not_turn_failure(self):
        (self.bins / "timeout").unlink()
        # Exclude any host timeout; keep only commands needed to reach the prerequisite check.
        for command in ("bash", "dirname", "date", "mktemp", "rm", "tr", "grep", "cut", "sed", "seq", "python3", "cat"):
            path = shutil.which(command)
            if path is None:
                self.fail(f"fixture prerequisite missing: {command}")
            os.symlink(path, self.bins / command)
        for script in ("acceptance-turn.sh", "mint-lineage-specimen.sh"):
            with self.subTest(script=script):
                result = self.run_script(script, PATH=str(self.bins))
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn(script.removesuffix(".sh") + ": timeout is required", result.stderr)
                self.assertNotIn("served by", result.stdout)

    def test_large_completed_turn_does_not_sigpipe_writer(self):
        result = self.run_script("mint-lineage-specimen.sh", TURN_KIND="large")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("specimen 1", result.stdout)
        wals = list((self.state / "wal").glob("*.wal"))
        self.assertEqual(len(wals), 2)
        self.assertEqual(wals[0].read_text(), wals[1].read_text())

    def test_incomplete_turn_still_refuses_specimen(self):
        result = self.run_script("mint-lineage-specimen.sh", TURN_KIND="incomplete")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("victim turn did not complete", result.stderr)

    def test_acceptance_completed_tool_turn_still_passes(self):
        result = self.run_script("acceptance-turn.sh")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS -- tool dispatched", result.stdout)

    def test_acceptance_reads_shared_candidate_file(self):
        (self.root / "scripts/lib/acceptance-models.txt").write_text("shared/model\n")
        result = self.run_script("acceptance-turn.sh", ACCEPTANCE_MODELS="")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("served by shared/model", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
