#!/usr/bin/env python3
"""Executable verification suite for scripts/rekey-harness.py."""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import hashlib
import json
import os
import pathlib
import sqlite3
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT = pathlib.Path(__file__).with_name("rekey-harness.py")
SPEC = importlib.util.spec_from_file_location("rekey_harness", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot import {SCRIPT}")
rekey_harness = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = rekey_harness
SPEC.loader.exec_module(rekey_harness)

WRITER = pathlib.Path(
    json.loads(subprocess.run(["cargo", "metadata", "--format-version", "1", "--no-deps"],
                              capture_output=True, text=True, cwd=SCRIPT.parent.parent,
                              check=True).stdout)["target_directory"]
) / "debug/examples/archive-fixture"

EXPECTED_SUITE_ANCHORS = 4


def identity(project_root: str, harness: str, session: str) -> str:
    return json.dumps(
        {"project_root": project_root, "harness": harness, "session": session},
        separators=(",", ":"),
    )


def wal_fixture(text: str) -> bytes:
    """A readable legacy WAL; migration preserves its framed bytes unchanged."""
    payload = json.dumps({"type": "tool_result", "output": text}, separators=(",", ":")).encode()
    header = struct.pack("<BQQ", 1, 1, 1)
    return struct.pack("<I", len(payload)) + header + hashlib.sha256(header + payload).digest() + payload


class RekeyHarnessTests(unittest.TestCase):
    def _fixture(self, tmp_path, *, archive_source: bool, archive_dest: bool):
        """One live source, and an archived WAL at whichever end the caller names.

        The archive is a DIRECTORY of addresses rather than a sealed container: the
        script learns archived addresses through `archived_addresses`, so what matters
        is that the address is known, not how the bytes are packed. Patching that one
        function keeps the fixture honest about the thing under test -- the collision
        decision -- without asserting a container format this test does not own.
        """
        root = tmp_path / "state"
        wal_dir = root / "wal"
        wal_dir.mkdir(parents=True)
        home = tmp_path / "home"
        home.mkdir()

        src_root = "/work/subject"
        us = rekey_harness.US
        old_key = f"{src_root}{us}runner{us}subject"
        new_key = f"{src_root}{us}broca{us}subject"

        with sqlite3.connect(root / "run-index.db") as db:
            db.execute("CREATE TABLE meta (session TEXT PRIMARY KEY, prefix_len INT)")
            db.execute("CREATE TABLE message (session TEXT NOT NULL, body TEXT)")
            db.execute("CREATE TABLE run_index (run_id TEXT PRIMARY KEY, session TEXT)")
            db.execute("INSERT INTO meta(session, prefix_len) VALUES (?, 1)", (old_key,))
            db.execute(
                "INSERT INTO run_index(run_id, session) VALUES (?, ?)",
                ("subject-run", identity(src_root, "runner", "subject")),
            )

        # A REAL SEALED CONTAINER, written by the same fixture writer the archive-read
        # suite uses, rather than a patched address set. The script discovers archived
        # addresses by loading lib/archive_read.py itself, so stubbing that out would
        # test my stub's agreement with my test instead of the script's behaviour.
        (wal_dir / "keep.wal").write_bytes(wal_fixture("an unrelated live session"))
        to_archive = []
        if archive_source:
            path = wal_dir / rekey_harness.wal_name(old_key)
            path.write_bytes(wal_fixture("source transcript"))
            to_archive.append(path)
        else:
            (wal_dir / rekey_harness.wal_name(old_key)).write_bytes(wal_fixture("source transcript"))
        if archive_dest:
            path = wal_dir / rekey_harness.wal_name(new_key)
            path.write_bytes(wal_fixture("destination transcript"))
            to_archive.append(path)
        if to_archive:
            (root / "wal-archive").mkdir(exist_ok=True)
            subprocess.run(
                [str(WRITER), str(root / "wal-archive" / "fold-1000.ark"), *map(str, to_archive)],
                check=True,
            )
            # The fold unlinks its sources; the fixture writer does not, so do it here.
            for path in to_archive:
                path.unlink()
        return root, home, old_key, new_key

    def _run(self, root, home):
        argv = [str(SCRIPT), "--from", "runner", "--to", "broca",
                "--state-root", str(root), "--apply"]
        out = io.StringIO()
        with (
            mock.patch.object(sys, "argv", argv),
            mock.patch.object(rekey_harness, "writer_is_live_on", return_value=False),
            mock.patch.dict(os.environ, {"HOME": str(home)}),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(out),
        ):
            return rekey_harness.main(), out.getvalue()

    def test_index_only_destination_is_occupied_not_free(self) -> None:
        """The store half of the same gate: a destination can hold a run_index row
        with no meta row, because one-shot sends skip store projection. Reading meta
        alone reported the destination free and merged the two identities."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = pathlib.Path(tmp)
            root = tmp_path / "state"
            wal_dir = root / "wal"
            wal_dir.mkdir(parents=True)
            home = tmp_path / "home"
            home.mkdir()

            us = rekey_harness.US
            src_root = "/work/subject"
            old_key = f"{src_root}{us}runner{us}subject"
            with sqlite3.connect(root / "run-index.db") as db:
                db.execute("CREATE TABLE meta (session TEXT PRIMARY KEY, prefix_len INT)")
                db.execute("CREATE TABLE message (session TEXT NOT NULL, body TEXT)")
                db.execute("CREATE TABLE run_index (run_id TEXT PRIMARY KEY, session TEXT)")
                db.execute("INSERT INTO meta(session, prefix_len) VALUES (?, 1)", (old_key,))
                db.executemany(
                    "INSERT INTO run_index(run_id, session) VALUES (?, ?)",
                    [
                        ("source-run", identity(src_root, "runner", "subject")),
                        # The destination, present ONLY here -- no meta row.
                        ("dest-run", identity(src_root, "broca", "subject")),
                    ],
                )
            (wal_dir / rekey_harness.wal_name(old_key)).write_bytes(wal_fixture("source transcript"))

            code, output = self._run(root, home)
            self.assertEqual(code, 1, output)
            self.assertIn("store-key collisions", output)
            self.assertNotIn("migratable                       1", output)

    def test_archived_source_refuses_rather_than_stranding_its_history(self) -> None:
        """An absent live WAL can still have archived history that must not be stranded."""
        with tempfile.TemporaryDirectory() as tmp:
            root, home, _, _ = self._fixture(
                pathlib.Path(tmp), archive_source=True, archive_dest=False
            )
            code, output = self._run(root, home)
            self.assertEqual(code, 2, output)
            self.assertIn("ARCHIVED, not missing", output)

    def test_archived_destination_refuses_rather_than_shadowing_it(self) -> None:
        """A folded destination has no live WAL but must still refuse a collision.

        Without the check the plan reads clean, the source is renamed onto that
        address, and live-first resolution hides the destination's archived
        transcript -- success reported, history silently replaced.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root, home, old_key, _ = self._fixture(
                pathlib.Path(tmp), archive_source=False, archive_dest=True
            )
            code, output = self._run(root, home)
            self.assertEqual(code, 2, output)
            self.assertIn("DESTINATIONS already hold an ARCHIVED log", output)
            # And the source must still be where it was: a refusal that already moved
            # bytes is not a refusal.
            live = root / "wal" / rekey_harness.wal_name(old_key)
            self.assertTrue(live.exists(), "refusal must not have renamed anything")
            self.assertEqual(live.read_bytes(), wal_fixture("source transcript"))

    def test_excluded_lineage_run_index_session_is_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = pathlib.Path(tmp)
            root = tmp_path / "state"
            wal_dir = root / "wal"
            wal_dir.mkdir(parents=True)
            home = tmp_path / "home"
            home.mkdir()

            ordinary_root = "/work/migratable"
            excluded_root = "/work/excluded"
            ordinary_key = f"{ordinary_root}{rekey_harness.US}runner{rekey_harness.US}ordinary"
            excluded_key = f"{excluded_root}{rekey_harness.US}runner{rekey_harness.US}excluded"
            ordinary_index = identity(ordinary_root, "runner", "ordinary")
            excluded_index = identity(excluded_root, "runner", "excluded")

            db_path = root / "run-index.db"
            with sqlite3.connect(db_path) as db:
                db.execute("CREATE TABLE meta (session TEXT PRIMARY KEY, prefix_len INTEGER)")
                db.execute("CREATE TABLE message (session TEXT NOT NULL, body TEXT NOT NULL)")
                db.execute("CREATE TABLE run_index (run_id TEXT PRIMARY KEY, session TEXT NOT NULL)")
                db.executemany(
                    "INSERT INTO meta(session, prefix_len) VALUES (?, 1)",
                    [(ordinary_key,), (excluded_key,)],
                )
                db.executemany(
                    "INSERT INTO message(session, body) VALUES (?, ?)",
                    [(ordinary_key, "ordinary transcript"), (excluded_key, "excluded transcript")],
                )
                db.executemany(
                    "INSERT INTO run_index(run_id, session) VALUES (?, ?)",
                    [("ordinary-run", ordinary_index), ("excluded-run", excluded_index)],
                )

            ordinary_wal = wal_dir / rekey_harness.wal_name(ordinary_key)
            excluded_wal = wal_dir / rekey_harness.wal_name(excluded_key)
            ordinary_wal.write_bytes(wal_fixture("ordinary wal"))
            excluded_wal.write_bytes(wal_fixture("excluded wal"))

            argv = [
                str(SCRIPT),
                "--from",
                "runner",
                "--to",
                "broca",
                "--state-root",
                str(root),
                "--exclude-root",
                excluded_root,
                "--apply",
            ]
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(rekey_harness, "writer_is_live_on", return_value=False),
                mock.patch.dict(os.environ, {"HOME": str(home)}),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                code = rekey_harness.main()
            self.assertEqual(code, 0, stdout.getvalue() + stderr.getvalue())

            migrated_key = ordinary_key.replace(
                f"{rekey_harness.US}runner{rekey_harness.US}",
                f"{rekey_harness.US}broca{rekey_harness.US}",
                1,
            )
            migrated_index = ordinary_index.replace('"harness":"runner"', '"harness":"broca"')
            with sqlite3.connect(db_path) as db:
                excluded_after = db.execute(
                    "SELECT session FROM run_index WHERE run_id='excluded-run'"
                ).fetchone()[0]
                ordinary_after = db.execute(
                    "SELECT session FROM run_index WHERE run_id='ordinary-run'"
                ).fetchone()[0]
                self.assertEqual(excluded_after.encode(), excluded_index.encode())
                self.assertEqual(ordinary_after, migrated_index)
                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM meta WHERE session=?", (excluded_key,)).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    db.execute(
                        "SELECT COUNT(*) FROM message WHERE session=?", (excluded_key,)
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    db.execute("SELECT COUNT(*) FROM meta WHERE session=?", (migrated_key,)).fetchone()[0],
                    1,
                )

            self.assertEqual(excluded_wal.read_bytes(), wal_fixture("excluded wal"))
            excluded_target = excluded_key.replace(
                f"{rekey_harness.US}runner{rekey_harness.US}",
                f"{rekey_harness.US}broca{rekey_harness.US}",
                1,
            )
            self.assertFalse((wal_dir / rekey_harness.wal_name(excluded_target)).exists())
            self.assertFalse(ordinary_wal.exists())
            self.assertEqual(
                (wal_dir / rekey_harness.wal_name(migrated_key)).read_bytes(), wal_fixture("ordinary wal")
            )


def suite_names() -> list[str]:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RekeyHarnessTests)
    return [test.id() for test in suite]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="list suite-owned test names")
    args = parser.parse_args()

    names = suite_names()
    if len(names) != EXPECTED_SUITE_ANCHORS:
        print(
            f"suite anchor mismatch before execution: expected {EXPECTED_SUITE_ANCHORS}, "
            f"listed {len(names)}"
        )
        return 1
    print(f"suite anchors before execution: {len(names)}")
    for name in names:
        print(f"  {name}")
    if args.list:
        return 0

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RekeyHarnessTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"executed count: {result.testsRun}")
    if result.testsRun <= 0:
        print("refusing vacuous test execution")
        return 1
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
