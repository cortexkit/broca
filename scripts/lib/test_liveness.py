"""Offline maintenance refusal tests, also run by broca-store on each CI platform."""

from __future__ import annotations

import contextlib
import importlib.util
import hashlib
import io
import json
import os
import pathlib
import sqlite3
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
from lib import liveness, state_root


def load_script(name):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), SCRIPTS / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import maintenance script {name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sweep = load_script("sweep-empty-wals")
rekey = load_script("rekey-harness")
PROBE = liveness.probe


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="broca liveness ")
        self.addCleanup(self.temp.cleanup)
        self.base = pathlib.Path(self.temp.name).resolve()
        self.root = self.base / "non-default root with spaces"
        self.home = self.base / "tool home"
        self.cwd = self.base / "daemon working directory"
        for directory in (self.root / "wal", self.home, self.cwd):
            directory.mkdir(parents=True)
        self.old_key = "project\x1frunner\x1fsession"
        self.new_key = "project\x1fbroca\x1fsession"
        with sqlite3.connect(self.root / "run-index.db") as db:
            db.execute("CREATE TABLE meta (session TEXT PRIMARY KEY, prefix_len INT)")
            db.execute("CREATE TABLE message (session TEXT, body TEXT)")
            db.execute("CREATE TABLE run_index (run_id TEXT PRIMARY KEY, session TEXT)")
            db.execute("INSERT INTO meta VALUES (?, 0)", (self.old_key,))
        db.close()
        (self.root / "wal" / rekey.wal_name(self.old_key)).write_bytes(b"")
        payload = bytes(range(16))
        header = struct.pack("<BQQ", 2, 0, 0)
        self.lineage = struct.pack("<I", len(payload)) + header + hashlib.sha256(header + payload).digest() + payload
        (self.root / "wal" / "lineage.wal").write_bytes(self.lineage)
        payload = b'{"type":"tool_result","output":"protected bytes"}'
        header = struct.pack("<BQQ", 1, 1, 1)
        content = struct.pack("<I", len(payload)) + header + hashlib.sha256(header + payload).digest() + payload
        (self.root / "wal" / "content.wal").write_bytes(content)
        (self.root / "wal-manifest").write_bytes(b"original mapping\n")

    def snapshot(self):
        # The native Windows probe runs powershell.exe, which inherits this test's USERPROFILE
        # and creates its own profile caches under it. Those are PowerShell's writes, not the
        # tool's, so only the native test skips that one subtree.
        skipped = self.home / "AppData" if getattr(self, "native_windows_probe", False) else None
        return {str(p.relative_to(self.base)): p.read_bytes() if p.is_file() else None
                for directory in (self.root, self.home)
                for p in [directory, *directory.rglob("*")]
                if skipped is None or not (p == skipped or skipped in p.parents)}

    def run_tool(self, tool, probe, *, apply=True):
        argv = ["maintenance", "--state-root", str(self.root)]
        if tool is rekey:
            argv += ["--from", "runner", "--to", "broca"]
        if apply:
            argv += ["--apply"]
        output = io.StringIO()
        connections = []
        original_connect = sqlite3.connect

        def connect_database(*args, **kwargs):
            db = original_connect(*args, **kwargs)
            connections.append(db)
            return db

        with (mock.patch.object(sys, "argv", argv),
              mock.patch.dict(os.environ, {"HOME": str(self.home), "USERPROFILE": str(self.home)}),
              mock.patch.object(liveness, "probe", side_effect=probe),
              mock.patch.object(sqlite3, "connect", side_effect=connect_database) as connect,
              contextlib.redirect_stdout(output), contextlib.redirect_stderr(output)):
            status = tool.main()
        for db in connections:
            db.close()
        return status, output.getvalue(), connect.call_count

    def assert_refused(self, probe, state):
        for tool, caller in ((sweep, "sweep"), (rekey, "rekey")):
            with self.subTest(caller=caller):
                before = self.snapshot()
                status, output, opens = self.run_tool(tool, probe)
                self.assertEqual(status, 3, output)
                self.assertIn(f"{caller}: liveness {state}; refusing offline mutation:", output)
                self.assertNotIn("cannot compare", output)
                self.assertEqual(opens, 0, "refusal must precede even SQLite initialisation")
                self.assertEqual(self.snapshot(), before, "bytes, backups and manifests must be unchanged")

    def context_probe(self, env=None, cwd=None, pids=None):
        env = env if env is not None else {"BROCA_STATE_ROOT": str(self.root)}
        cwd = cwd if cwd is not None else self.cwd
        return lambda root: PROBE(root, enumerate_processes=lambda _: pids or [4242],
                                  read_context=lambda *_: (env, cwd))

    def test_missing_enumerator_refuses_before_writes(self):
        with mock.patch.object(liveness.subprocess, "run", side_effect=FileNotFoundError("no enumerator")):
            self.assert_refused(PROBE, "unknown")

    def test_failed_enumeration_is_not_empty(self):
        failed = subprocess.CompletedProcess([], 2, "", "enumerator failed")
        with mock.patch.object(liveness.subprocess, "run", return_value=failed):
            self.assert_refused(PROBE, "unknown")

    def test_empty_enumeration_allows_known_stopped_controls(self):
        stdout = "[]" if sys.platform == "win32" else ""
        status = 0 if sys.platform == "win32" else 1
        with mock.patch.object(liveness.subprocess, "run", return_value=subprocess.CompletedProcess([], status, stdout)):
            self.assertEqual(PROBE(self.root).state, "no")
            stopped = lambda root: PROBE(root)
            result, output, _ = self.run_tool(rekey, stopped)
            self.assertEqual(result, 0, output)
            with sqlite3.connect(self.root / "run-index.db") as db:
                self.assertEqual(db.execute("SELECT session FROM meta").fetchone()[0], self.new_key)
            db.close()
            self.assertTrue(list(self.root.glob("*.pre-rekey-*.db")))
            self.assertTrue(list(self.home.glob("rekey-manifest-*.json")))
            result, output, _ = self.run_tool(sweep, stopped)
            self.assertEqual(result, 0, output)
            self.assertFalse((self.root / "wal" / rekey.wal_name(self.new_key)).exists())
            self.assertEqual((self.root / "wal" / "lineage.wal").read_bytes(), self.lineage)
            self.assertTrue(list(self.home.glob("sweep-manifest-*.json")))

    def test_idle_daemon_on_nondefault_root_refuses(self):
        self.assert_refused(self.context_probe(), "yes")

    def test_unreadable_environment_refuses(self):
        def unreadable(*_):
            raise PermissionError("process environment unreadable")
        self.assert_refused(lambda root: PROBE(root, enumerate_processes=lambda _: [4242], read_context=unreadable), "unknown")
        self.assert_refused(self.context_probe(env={}), "unknown")

    def test_unreadable_working_directory_refuses(self):
        def unreadable(*_):
            raise PermissionError("process working directory unreadable")
        self.assert_refused(lambda root: PROBE(root, enumerate_processes=lambda _: [4242], read_context=unreadable), "unknown")
        self.assert_refused(self.context_probe(cwd=self.base / "inaccessible cwd"), "unknown")

    def test_relative_explicit_root_uses_daemon_cwd(self):
        relative = os.path.relpath(self.root, self.cwd)
        self.assert_refused(self.context_probe(env={"BROCA_STATE_ROOT": relative}), "yes")

    def test_root_aliases_refuse(self):
        alias = self.base / "alias with spaces"
        second_alias = self.base / "another root alias"
        for path in (alias, second_alias):
            if sys.platform == "win32":
                subprocess.run(["cmd", "/c", "mklink", "/J", str(path), str(self.root)], check=True, capture_output=True)
                self.addCleanup(lambda path=path: path.rmdir())
            else:
                path.symlink_to(self.root, target_is_directory=True)
        self.assert_refused(self.context_probe(env={"BROCA_STATE_ROOT": str(alias)}), "yes")
        self.assert_refused(self.context_probe(env={"BROCA_STATE_ROOT": str(self.cwd / ".." / self.root.name)}), "yes")
        with mock.patch.object(self, "root", second_alias):
            self.assert_refused(self.context_probe(env={"BROCA_STATE_ROOT": str(alias)}), "yes")

    def test_distinct_root_is_allowed(self):
        other = self.base / "genuinely distinct root"
        other.mkdir()
        distinct = self.context_probe(env={"BROCA_STATE_ROOT": str(other)})
        self.assertEqual(distinct(self.root).state, "no")
        result, output, _ = self.run_tool(rekey, distinct)
        self.assertEqual(result, 0, output)
        result, output, _ = self.run_tool(sweep, distinct)
        self.assertEqual(result, 0, output)

    def test_unresolvable_canonical_root_refuses(self):
        self.assert_refused(self.context_probe(env={"BROCA_STATE_ROOT": str(self.base / "missing root")}), "unknown")

    def test_input_errors_are_not_liveness_refusals(self):
        (self.root / "run-index.db").unlink()
        before = self.snapshot()
        result, output, opens = self.run_tool(rekey, self.context_probe())
        self.assertEqual(result, 2)
        self.assertIn("cannot compare", output)
        self.assertNotIn("liveness yes;", output)
        self.assertNotIn("liveness unknown;", output)
        self.assertEqual(opens, 0)
        self.assertEqual(self.snapshot(), before)

    def test_read_only_modes_do_not_probe(self):
        def forbidden(_):
            self.fail("read-only maintenance must not require process inspection")
        for tool in (sweep, rekey):
            before = self.snapshot()
            result, output, _ = self.run_tool(tool, forbidden, apply=False)
            self.assertEqual(result, 0, output)
            self.assertEqual(self.snapshot(), before)

    def test_native_idle_process_context_preserves_spaces_and_relative_root(self):
        env = dict(os.environ, BROCA_STATE_ROOT=os.path.relpath(self.root, self.cwd))
        executable = sys.executable
        code = "import sys; print('ready', flush=True); sys.stdin.read()"
        if sys.platform.startswith("linux"):
            code = "import ctypes; ctypes.CDLL(None).prctl(15, b'ck-broca', 0, 0, 0); " + code
        elif sys.platform == "win32":
            self.native_windows_probe = True
            executable = str(self.base / "ck-broca.exe")
            shutil.copy2(sys.executable, executable)
            env["PYTHONHOME"] = sys.prefix
            env["PATH"] = str(pathlib.Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")
        child = subprocess.Popen([executable, "-u", "-c", code],
                                 env=env, cwd=self.cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert child.stdout is not None and child.stdin is not None
        try:
            self.assertEqual(child.stdout.readline().strip(), "ready")
            if sys.platform == "darwin":
                actual = lambda root: PROBE(root, enumerate_processes=lambda _: [child.pid])
            else:
                actual = PROBE
            self.assert_refused(actual, "yes")
            binary = os.environ.get("STORE_MAINTENANCE_BIN")
            if binary and sys.platform != "darwin":
                before = self.snapshot()
                output = subprocess.run([binary, "wal-manifest", str(self.root), "--write"],
                                        capture_output=True, text=True, check=False)
                self.assertEqual(output.returncode, 3, output.stderr)
                self.assertIn("liveness yes; refusing offline mutation:", output.stderr)
                self.assertEqual(self.snapshot(), before)
        finally:
            child.terminate()
            child.wait(timeout=10)
            child.stdin.close()
            child.stdout.close()


class EnumeratorTests(unittest.TestCase):
    def test_shell_helper_unknown_is_not_an_import_error(self):
        with tempfile.TemporaryDirectory(prefix="missing canonical root ") as directory:
            output = subprocess.run([sys.executable, "-B", str(SCRIPTS / "lib" / "liveness.py"),
                                     str(pathlib.Path(directory) / "absent")],
                                    capture_output=True, text=True, check=False)
            self.assertEqual(output.returncode, 4, output.stderr)
            self.assertEqual(json.loads(output.stdout)["state"], "unknown")

    def test_enumerator_exit_status_and_output_contract(self):
        for platform in ("linux", "darwin"):
            self.assertEqual(liveness.enumerate_daemons(platform, lambda *a, **k: subprocess.CompletedProcess([], 1, "")), [])
            self.assertEqual(liveness.enumerate_daemons(platform, lambda *a, **k: subprocess.CompletedProcess([], 0, "12\n34\n")), [12, 34])
            for code, output in ((2, ""), (1, "12"), (0, ""), (0, "not a pid")):
                with self.assertRaises(RuntimeError):
                    liveness.enumerate_daemons(platform, lambda *a, **k: subprocess.CompletedProcess([], code, output))
        self.assertEqual(liveness.enumerate_daemons("win32", lambda *a, **k: subprocess.CompletedProcess([], 0, "[]")), [])
        self.assertEqual(liveness.enumerate_daemons("win32", lambda *a, **k: subprocess.CompletedProcess([], 0, "[12,34]")), [12, 34])
        for code, output in ((2, "[]"), (0, "null"), (0, "[true]"), (0, "{}")):
            with self.assertRaises(RuntimeError):
                liveness.enumerate_daemons("win32", lambda *a, **k: subprocess.CompletedProcess([], code, output))

    def test_daemon_data_home_ladder_is_shared(self):
        base = pathlib.Path(tempfile.gettempdir()).resolve()
        for platform in ("linux", "darwin", "win32"):
            env = {"HOME": str(base / "home"), "USERPROFILE": str(base / "profile"), "APPDATA": str(base / "app"), "XDG_DATA_HOME": str(base / "xdg")}
            self.assertEqual(liveness.daemon_root(env, platform), base / "xdg" / "cortexkit" / "broca")
            del env["XDG_DATA_HOME"]
            expected = base / "app" if platform == "win32" else base / "home" / ".local" / "share"
            self.assertEqual(liveness.daemon_root(env, platform), expected / "cortexkit" / "broca")
            del env["APPDATA"]
            if platform == "win32":
                self.assertEqual(liveness.daemon_root(env, platform), base / "profile" / "AppData" / "Roaming" / "cortexkit" / "broca")
            with self.assertRaises(state_root.StateRootError):
                liveness.daemon_root({"XDG_DATA_HOME": "relative"}, platform)


if __name__ == "__main__":
    print(sys.version, flush=True)
    unittest.main(verbosity=2)
