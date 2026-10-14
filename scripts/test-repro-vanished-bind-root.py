#!/usr/bin/env python3
"""Exercise vanished-root reproduction only against disposable owned fixtures."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/repro-vanished-bind-root.sh"


class VanishedRootOwnership(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="vanished-root-")
        self.addCleanup(self.tmp.cleanup)
        self.parent = Path(self.tmp.name)
        self.root = self.parent / "project"
        self.conn = self.parent / "connection.json"
        self.conn.write_text("{}")
        self.client = self.parent / "client"
        self.client.write_text(r'''#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
root = pathlib.Path(args[args.index("--project-root") + 1])
if "run" in args:
    if os.environ.get("LOSE_MARKER"):
        for marker in root.glob(".broca-vanished-root-owner"):
            marker.unlink()
    print("RUN_STARTED run_id=fixture\nRUN_FINISHED")
else:
    print("STATUS state=Completed" if root.exists() else "invalid_project_root")
''')
        self.client.chmod(0o755)

    def run_repro(self, **env):
        return subprocess.run(["bash", str(SCRIPT)], cwd=ROOT,
                              env={**os.environ, "REPRO_ROOT": str(self.root),
                                   "BROCA_SESSION_BIN": str(self.client),
                                   "SUBC_CONNECTION_FILE": str(self.conn), **env},
                              text=True, capture_output=True, timeout=5)

    def test_missing_client_never_deletes_caller_root(self):
        self.root.mkdir()
        sentinel = self.root / "keep"
        sentinel.write_text("caller data")
        result = self.run_repro(BROCA_SESSION_BIN=str(self.parent / "missing"))
        self.assertEqual(result.returncode, 2)
        self.assertTrue(sentinel.exists(), result.stdout + result.stderr)
        self.assertEqual(sentinel.read_text(), "caller data")

    def test_existing_root_refuses_before_running_or_removing(self):
        self.root.mkdir()
        sentinel = self.root / "keep"
        sentinel.write_text("caller data")
        result = self.run_repro()
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("root already exists", result.stderr)
        self.assertEqual(sentinel.read_text(), "caller data")
        self.assertNotIn("run     fixture", result.stdout)

    def test_newly_created_owned_root_is_removed_for_probe(self):
        result = self.run_repro()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("REPRODUCED", result.stdout)
        self.assertFalse(self.root.exists())

    def test_lost_ownership_marker_refuses_removal(self):
        result = self.run_repro(LOSE_MARKER="1")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("root ownership", result.stderr)
        self.assertTrue(self.root.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
