#!/usr/bin/env python3
"""The deploy acceptance turn (`acceptance-turn.sh`), run against a stand-in `broca-session`
client that prints a scripted tool turn, so no daemon or model is involved."""

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
        shutil.copy2(ROOT / "scripts" / "acceptance-turn.sh", scripts)
        (scripts / "lib").mkdir()
        shutil.copy2(ROOT / "scripts/lib/acceptance-models.txt", scripts / "lib")
        for name in ("acceptance_contract.py", "artifact-probe.sh", "exit-guard.sh"):
            shutil.copy2(ROOT / "scripts/lib" / name, scripts / "lib" / name)
        self.bins = self.root / "bin"
        self.bins.mkdir()
        self.home = self.root / "home"
        conn = self.home / ".local/share/cortexkit/run/subc-connection.json"
        conn.parent.mkdir(parents=True)
        conn.write_text("{}")
        self.state = self.root / "state"
        self.state.mkdir()
        target = self.root / "target"
        client = target / "release/broca-session"
        client.parent.mkdir(parents=True)
        self.executable(client, r'''#!/usr/bin/env python3
import sys
args = sys.argv[1:]
if "describe" in args:
    print('{"result":{"capabilities":[]}}')
    sys.exit(0)
print("TOOL_CALL seq=1 id=fixture-call name=read\nTOOL_RESULT seq=2 id=fixture-call is_error=false text=ok")
print("RUN_FINISHED reason=Completed")
''')
        placed = self.home / ".local/share/cortexkit/bin/ck-broca"
        placed.parent.mkdir(parents=True)
        placed.write_text("#!/bin/sh\n# legacy fixture build\n")
        self.executable(self.bins / "timeout", '#!/bin/sh\nshift\nexec "$@"\n')
        self.env = {**os.environ, "HOME": str(self.home), "BROCA_STATE_ROOT": str(self.state),
                    "PATH": f"{self.bins}:{os.environ['PATH']}",
                    "CARGO_TARGET_DIR": str(target)}

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
        result = self.run_script("acceptance-turn.sh", PATH=str(self.bins))
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("acceptance-turn: timeout is required", result.stderr)
        self.assertNotIn("served by", result.stdout)

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
