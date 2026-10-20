#!/usr/bin/env python3
"""Operator WAL readers agree on live, expired, moved and unreadable inputs."""
from __future__ import annotations

import hashlib
import contextlib
import importlib.util
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
import uuid
from unittest import mock

from lib.archive_read import ArchiveRefusal, logical_wals, read_lineage_id, read_records

SCRIPTS = pathlib.Path(__file__).resolve().parent
EXPECTED_SUITE_ANCHORS = 17
OLD = uuid.UUID("00000000-0000-4000-8000-000000000001")
PORTABLE = uuid.UUID("00000000-0000-4000-8000-000000000002")
EPHEMERAL = uuid.UUID("00000000-0000-4000-8000-000000000003")
EXPIRED = uuid.UUID("00000000-0000-4000-8000-000000000004")


def frame(version: int, seq: int, fence: int, payload: bytes) -> bytes:
    header = struct.pack("<BQQ", version, seq, fence)
    return struct.pack("<I", len(payload)) + header + hashlib.sha256(header + payload).digest() + payload


def lineage(identity: uuid.UUID) -> bytes:
    return frame(2, 0, 0, identity.bytes)


def address(session: dict) -> str:
    # This is the WAL filename hash, not an expected value derived from the filter.
    value = "\x1f".join(session[key] for key in ("project_root", "harness", "session"))
    h = 0xCBF29CE484222325
    for byte in value.encode():
        h = ((h ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"{h:016x}"


def wal_bytes(label: str, session: dict, identity: uuid.UUID) -> bytes:
    def start(episode: str) -> dict:
        return {"type": "run_started", "run_id": f"{label}-{episode}", "session": session,
                "config": {"model": {"provider_module_id": label, "model_id": "model"},
                           "frozen_render_config": {"family_id": "family"}}}

    # Each reader has a positive observation: a finalize leak, a billed empty
    # completion, timed steps, a bind identity and a final nonterminal run.
    records = [
        start("leak"),
        {"type": "budget_fired", "kind": "finalize"},
        {"type": "step_started", "step_id": 1},
        {"type": "model_step_finished", "step_id": 1,
         "assistant_message": {"content": [{"type": "text", "text": label},
                                             {"type": "tool_call", "tool_call_id": label}]},
         "usage": {"output_tokens": 40}},
        {"type": "run_finished", "reason": "completed"},
        start("empty"),
        {"type": "step_started", "step_id": 1},
        {"type": "model_step_finished", "step_id": 1,
         "assistant_message": {"content": []}, "usage": {"output_tokens": 40}},
        {"type": "run_finished", "reason": "completed"},
        start("live"),
    ]
    raw = lineage(identity)
    for seq, record in enumerate(records, 1):
        record["ts_ms"] = seq * 1000
        raw += frame(1, seq, 1, json.dumps(record, separators=(",", ":")).encode()) + lineage(identity)
    return raw


def tombstone(root: pathlib.Path, addr: str, identity: uuid.UUID, *, seq: int, expiry: int):
    directory = root / "wal-ephemeral" / f"{addr}.expired"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / str(identity)).write_text(json.dumps(
        {"lineage_id": str(identity), "seq": seq, "expired_at_ms": expiry}))


class RetentionReaders(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        (self.root / "wal").mkdir()
        (self.root / "wal-ephemeral").mkdir()
        self.paths = {}
        self.contents = {}
        self.sessions = {}
        for label, directory, identity in (
            ("portable", "wal", PORTABLE),
            ("ephemeral", "wal-ephemeral", EPHEMERAL),
            ("expired", "wal-ephemeral", EXPIRED),
        ):
            session = {"project_root": "/work/retention", "harness": "runner", "session": label}
            addr = address(session)
            path = self.root / directory / f"{addr}.wal"
            self.paths[label] = path
            self.sessions[label] = session
            self.contents[label] = wal_bytes(label, session, identity)
            path.write_bytes(self.contents[label])
            # Older A expires much later than B: ordering by clock or skipping
            # every address with any tombstone would incorrectly hide both live B's.
            tombstone(self.root, addr, OLD, seq=1, expiry=10**15)
        tombstone(self.root, self.paths["expired"].stem, EXPIRED, seq=2, expiry=1)
        # Even a tombstone for B at a different address must not hide B.
        tombstone(self.root, "ffffffffffffffff", PORTABLE, seq=1, expiry=1)
        (self.root / "wal-ephemeral" / "ignored.deleting").write_bytes(b"unreadable deletion residue")
        residue = self.root / "wal-ephemeral" / "ignored.expired"
        residue.mkdir()
        (residue / "ignored.wal").write_bytes(b"unreadable tombstone-directory residue")
        with contextlib.closing(sqlite3.connect(self.root / "run-index.db")) as db:
            with db:
                db.execute("CREATE TABLE meta (session TEXT PRIMARY KEY, prefix_len INTEGER)")
                db.execute("CREATE TABLE message (session TEXT, body TEXT)")
                db.execute("CREATE TABLE run_index (run_id TEXT PRIMARY KEY, session TEXT)")
                for label, session in self.sessions.items():
                    key = "\x1f".join(session[k] for k in ("project_root", "harness", "session"))
                    db.execute("INSERT INTO run_index VALUES (?, ?)", (label, json.dumps(session, separators=(",", ":"))))
                    db.execute("INSERT INTO meta VALUES (?, 1)", (key,))
                    db.execute("INSERT INTO message VALUES (?, ?)", (key, label))

    def run_reader(self, script: str, *args: str):
        return subprocess.run([sys.executable, str(SCRIPTS / script), "--state-root", str(self.root), *args],
                              env={**os.environ, "HOME": str(self.root)},
                              capture_output=True, text=True, timeout=30)

    def commands(self):
        return [
            ("live-runs.py", "--boot-epoch", "1"),
            ("wal-scan.py", "--control", "run_started", "--pattern", "portable", "--pattern", "ephemeral", "--pattern", "expired"),
            ("step-timing.py",),
            ("find-empty-completions.py", "--hours", "1000000"),
            ("find-finalize-leaks.py", "--baseline", "2"),
            ("wal-moved-roots.py",),
            ("rekey-harness.py", "--from", "runner", "--to", "broca"),
        ]

    def assert_live_results(self):
        for command in self.commands():
            script = command[0]
            with self.subTest(reader=script):
                result = self.run_reader(*command)
                out = result.stdout
                expected = 1 if script in ("live-runs.py", "find-empty-completions.py") else 0
                self.assertEqual(result.returncode, expected, out + result.stderr)
                if script == "live-runs.py":
                    self.assertIn("live runs          2", out)
                    for label in ("portable", "ephemeral"):
                        self.assertIn(f"  live  {self.paths[label].name}", out)
                    self.assertNotIn(self.paths["expired"].name, out)
                elif script == "wal-scan.py":
                    self.assertIn("files read          2", out)
                    self.assertIn("'expired'\n      matches           0 across 0 files", out)
                    for label in ("portable", "ephemeral"):
                        self.assertIn(self.paths[label].name, out)
                elif script == "step-timing.py":
                    for label in ("portable", "ephemeral"):
                        self.assertIn(f"  {label}/family steps: 2", out)
                    self.assertNotIn("  expired/family steps:", out)
                elif script == "find-empty-completions.py":
                    self.assertIn("scanned 2 WAL(s)", out, result.stderr)
                    for label in ("portable", "ephemeral"):
                        self.assertIn(str(self.paths[label]), out)
                    self.assertNotIn(str(self.paths["expired"]), out)
                elif script == "find-finalize-leaks.py":
                    self.assertIn("wal files scanned    2", out)
                    self.assertIn("  portable/model: 1", out)
                    self.assertIn("  ephemeral/model: 1", out)
                    self.assertNotIn("  expired/model:", out)
                elif script == "wal-moved-roots.py":
                    self.assertIn("wals=2 empty=0 scanned=2", out)
                elif script == "rekey-harness.py":
                    self.assertIn("migratable                       2", out)
                    self.assertIn("rows with no WAL on disk         0", out)
                    self.assertIn("tombstoned WALs left untouched   1", out)

    def test_all_seven_readers_find_durable_live_wals(self):
        durable = self.root / "wal-durable"
        durable.mkdir()
        source = self.paths["portable"]
        source.rename(durable / source.name)
        self.paths["portable"] = durable / source.name
        self.assertEqual({p.name for p in logical_wals(self.root / "wal")},
                         {self.paths[label].name for label in ("portable", "ephemeral")})
        self.assert_live_results()

    def test_all_seven_readers_select_only_live_lineages(self):
        self.assert_live_results()

    def test_empty_completion_wide_window_reports_when_localtime_refuses_cutoff(self):
        spec = importlib.util.spec_from_file_location("empty_completions", SCRIPTS / "find-empty-completions.py")
        assert spec is not None and spec.loader is not None
        reader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(reader)
        localtime = reader.time.localtime
        for error in (OSError, OverflowError, ValueError):
            def windows_localtime(value):
                if value < 0:
                    raise error("timestamp out of range")
                return localtime(value)

            with self.subTest(error=error.__name__):
                out = io.StringIO()
                with mock.patch.object(sys, "argv", [str(SCRIPTS / "find-empty-completions.py"),
                                                   "--state-root", str(self.root), "--hours", "1000000"]), \
                        mock.patch.object(reader.time, "time", return_value=1_800_000_000), \
                        mock.patch.object(reader.time, "localtime", side_effect=windows_localtime), \
                        contextlib.redirect_stdout(out):
                    self.assertEqual(reader.main(), 1)
                self.assertIn("scanned 2 WAL(s) written since Unix timestamp -1800000000", out.getvalue())
                self.assertIn("2 SUSPECT(S)", out.getvalue())
                for label in ("portable", "ephemeral"):
                    self.assertIn(str(self.paths[label]), out.getvalue())
                self.assertNotIn(str(self.paths["expired"]), out.getvalue())

    def test_all_seven_readers_refuse_unreadable_lineage_frames(self):
        corrupt = bytearray(lineage(EPHEMERAL))
        corrupt[-1] ^= 1
        (self.root / "wal-ephemeral" / "unreadable.wal").write_bytes(corrupt)
        for command in self.commands():
            with self.subTest(reader=command[0]):
                result = self.run_reader(*command)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("unreadable.wal", result.stdout + result.stderr)
                self.assertIn("digest mismatch", result.stdout + result.stderr)

    def test_rekey_apply_leaves_tombstoned_wal_and_rows_untouched(self):
        import importlib.util
        import contextlib
        import io
        from unittest import mock

        spec = importlib.util.spec_from_file_location("retention_rekey", SCRIPTS / "rekey-harness.py")
        if spec is None or spec.loader is None:
            raise RuntimeError("cannot load rekey-harness.py")
        rekey = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(rekey)
        argv = ["rekey-harness.py", "--state-root", str(self.root), "--from", "runner", "--to", "broca", "--apply"]
        output = io.StringIO()
        with (mock.patch.object(sys, "argv", argv),
              mock.patch.object(rekey, "writer_is_live_on", return_value=False),
              mock.patch.dict("os.environ", {"HOME": str(self.root)}),
              contextlib.redirect_stdout(output), contextlib.redirect_stderr(output)):
            code = rekey.main()
        self.assertEqual(code, 0, output.getvalue())
        for label in ("portable", "ephemeral"):
            session = dict(self.sessions[label], harness="broca")
            target = self.paths[label].with_name(address(session) + ".wal")
            self.assertEqual(target.read_bytes(), self.contents[label])
            self.assertFalse(self.paths[label].exists())
        self.assertEqual(self.paths["expired"].read_bytes(), self.contents["expired"])
        with contextlib.closing(sqlite3.connect(self.root / "run-index.db")) as db:
            with db:
                actual = json.loads(db.execute("SELECT session FROM run_index WHERE run_id='expired'").fetchone()[0])
                self.assertEqual(actual, self.sessions["expired"])

    def test_legacy_upgrade_repeated_ids_and_conflicts(self):
        path = self.root / "wal" / "legacy.wal"
        raw = frame(1, 1, 1, b'{"type":"run_started"}')
        path.write_bytes(raw)
        tombstone(self.root, path.stem, OLD, seq=1, expiry=1)
        self.assertIsNone(read_lineage_id(path))
        self.assertIn(path, logical_wals(self.root / "wal"))
        path.write_bytes(raw + lineage(PORTABLE) * 2)
        self.assertEqual(read_lineage_id(path), str(PORTABLE))
        self.assertEqual(len(read_records(path)), 1)
        self.assertIn(path, logical_wals(self.root / "wal"))
        path.write_bytes(raw + lineage(PORTABLE) + lineage(OLD))
        with self.assertRaisesRegex(ArchiveRefusal, "conflicting lineage IDs"):
            logical_wals(self.root / "wal")

    def test_ephemeral_only_tree_is_still_scanned(self):
        portable = self.paths["portable"]
        portable.rename(self.root / "wal-ephemeral" / portable.name)
        self.paths["portable"] = self.root / "wal-ephemeral" / portable.name
        (self.root / "wal").rmdir()
        self.assert_live_results()

    def assert_sidecar_skip(self, suffix):
        hidden = self.paths["portable"]
        hidden.with_suffix(suffix).write_text("{}")
        hidden.write_bytes(b"not a readable WAL")
        self.assertEqual(logical_wals(self.root / "wal"), [self.paths["ephemeral"]])
        for command in self.commands()[:-1]:
            if command[0] == "find-finalize-leaks.py":
                command = (command[0], "--baseline", "1")
            with self.subTest(reader=command[0]):
                result = self.run_reader(*command)
                expected = 1 if command[0] in ("live-runs.py", "find-empty-completions.py") else 0
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                self.assertIn(hidden.stem, result.stderr)
                self.assertIn(suffix, result.stderr)
                self.assertNotIn(hidden.name, result.stdout)
                self.assertNotIn("portable/family steps:", result.stdout)
                self.assertNotIn("portable/model: 1", result.stdout)
        self.assertEqual(hidden.read_bytes(), b"not a readable WAL")

    def test_moving_sidecar_skips_readers(self):
        self.assert_sidecar_skip(".moving")

    def test_moved_sidecar_skips_readers(self):
        self.assert_sidecar_skip(".moved")

    def all_commands(self):
        return self.commands() + [
            ("sweep-empty-wals.py",),
            ("wal-addressability.py",),
            ("wal-addressability.py", "--key", "/work/retention", "runner", "portable"),
        ]

    def test_sidecars_refuse_rekey_and_addressability(self):
        # A sidecar can be the only live artifact, including at a rename destination.
        self.paths["portable"].unlink()
        destination = address(dict(self.sessions["portable"], harness="broca"))
        for addr in (self.paths["portable"].stem, destination):
            for suffix in (".moving", ".moved"):
                marker = self.root / "wal" / f"{addr}{suffix}"
                marker.write_text("{}")
                before = (self.root / "run-index.db").read_bytes()
                for command in (self.commands()[-1] + ("--apply",), *self.all_commands()[-2:]):
                    with self.subTest(address=addr, sidecar=suffix, reader=command[0]):
                        result = self.run_reader(*command)
                        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                        self.assertIn(str(marker), result.stderr)
                        self.assertIn("rerun after the move finishes", result.stderr)
                self.assertEqual((self.root / "run-index.db").read_bytes(), before)
                self.assertEqual(marker.read_text(), "{}")
                marker.unlink()

    def test_sweep_apply_preserves_move_sidecars_and_zero_byte_wals(self):
        spec = importlib.util.spec_from_file_location("move_sweep", SCRIPTS / "sweep-empty-wals.py")
        if spec is None or spec.loader is None:
            raise RuntimeError("cannot load sweep-empty-wals.py")
        sweep = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sweep)
        for suffix in (".moving", ".moved"):
            path = self.root / "wal" / f"zero{suffix}.wal"
            path.write_bytes(b"")
            path.with_suffix(suffix).write_text("{}")
        unguarded = self.root / "wal" / "unguarded.wal"
        unguarded.write_bytes(b"")
        output = io.StringIO()
        argv = ["sweep-empty-wals.py", "--state-root", str(self.root), "--apply"]
        with (mock.patch.object(sys, "argv", argv),
              mock.patch.object(sweep.liveness, "probe") as probe,
              mock.patch.dict("os.environ", {"HOME": str(self.root)}),
              contextlib.redirect_stdout(output), contextlib.redirect_stderr(output)):
            probe.return_value.refusal.return_value = None
            self.assertEqual(sweep.main(), 0, output.getvalue())
        self.assertFalse(unguarded.exists())
        for suffix in (".moving", ".moved"):
            path = self.root / "wal" / f"zero{suffix}.wal"
            self.assertEqual(path.read_bytes(), b"")
            self.assertEqual(path.with_suffix(suffix).read_text(), "{}")

    def cut_file(self):
        directory = self.root / "agent-data" / "cuts"
        directory.mkdir(parents=True, exist_ok=True)
        return directory / "cut-reader.json"

    def assert_cut_refusal(self, path, *messages):
        before = {p: p.read_bytes() for p in self.paths.values()}
        for command in self.all_commands():
            with self.subTest(reader=command):
                result = self.run_reader(*command)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn(str(path), result.stderr)
                self.assertIn("rerun after the move finishes", result.stderr)
                for message in messages:
                    self.assertIn(message, result.stderr)
        self.assertEqual({p: p.read_bytes() for p in self.paths.values()}, before)

    def assert_pending_refusal(self, op):
        path = self.cut_file()
        # The receiver can install a session it has never held before. Its pending
        # intent must block scans even without a local WAL address or move sidecar.
        refs = [{"ref_kind": "broca-session", "ref": "0123456789abcdef0123456789abcdef"}]
        raw = json.dumps({"cutId": "cut-reader", "refs": refs,
                          "intent": {"op": op, "request": {"refs": refs}}}).encode()
        path.write_bytes(raw)
        self.assert_cut_refusal(path, "cut-reader", op, refs[0]["ref"])
        self.assertEqual(path.read_bytes(), raw)

    def test_pending_release_refuses_readers(self):
        self.assert_pending_refusal("release")

    def test_pending_activate_refuses_readers(self):
        self.assert_pending_refusal("activate")

    def test_corrupt_cut_file_refuses_readers(self):
        path = self.cut_file()
        for raw in (b"{", b"\xff", b"[]", b'{"intent": {}}'):
            with self.subTest(cut=raw):
                path.write_bytes(raw)
                self.assert_cut_refusal(path, "cannot read cut state")
                self.assertEqual(path.read_bytes(), raw)

    def test_unreadable_cut_file_refuses_readers(self):
        path = self.cut_file()
        path.symlink_to(path.with_suffix(".missing"))
        self.assert_cut_refusal(path, "cannot read cut state")
        self.assertTrue(path.is_symlink())

    def test_completed_and_reversible_cut_states_are_scannable(self):
        path = self.cut_file()
        path.with_suffix(".tmp").write_bytes(b"incomplete atomic write")
        for intent in (None, {"op": "prepare"}, {"op": "export"}, {"op": "import"}, {"op": "abort"}):
            with self.subTest(intent=intent):
                path.write_text(json.dumps({"refs": [], "intent": intent}))
                self.assert_live_results()

    def test_staging_is_not_discovered_and_explicit_staging_roots_refuse(self):
        staging = self.root / "wal-staging" / "cut-reader"
        (staging / "wal").mkdir(parents=True)
        staged_bytes = self.contents["portable"]
        (staging / "wal" / "staged.wal").write_bytes(staged_bytes)
        (staging / "direct.wal").write_bytes(staged_bytes)
        self.assert_live_results()
        for directory in (staging, staging / "wal"):
            with self.assertRaisesRegex(ArchiveRefusal, "wal-staging is not a live WAL population"):
                logical_wals(directory)
        alias = self.root / "staging-alias"
        alias.symlink_to(staging, target_is_directory=True)
        with self.assertRaisesRegex(ArchiveRefusal, "wal-staging is not a live WAL population"):
            logical_wals(alias)
        shutil.copyfile(self.root / "run-index.db", staging / "run-index.db")
        for command in self.all_commands():
            with self.subTest(reader=command):
                result = subprocess.run([sys.executable, str(SCRIPTS / command[0]),
                                         "--state-root", str(staging), *command[1:]],
                                        capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("wal-staging is not a live WAL population", result.stderr)
        self.assertEqual((staging / "wal" / "staged.wal").read_bytes(), staged_bytes)


if __name__ == "__main__":
    print(sys.version, flush=True)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RetentionReaders)
    if suite.countTestCases() != EXPECTED_SUITE_ANCHORS:
        raise SystemExit("suite anchor mismatch")
    unittest.main(verbosity=2)
