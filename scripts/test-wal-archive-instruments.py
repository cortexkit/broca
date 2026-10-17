#!/usr/bin/env python3
"""Archive reachability and sweep scope over mixed live/archive fixtures."""
import hashlib
import importlib.util
import pathlib
import json
import sqlite3
import struct
import tempfile
import unittest


def load(name):
    spec = importlib.util.spec_from_file_location(name, pathlib.Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wa = load("wal-addressability")
sweep = load("sweep-empty-wals")


def container(*members, sealed=True):
    """A sealed container holding one member per (addr, payload) pair.

    Two members distinguish a complete inventory walk from one that stops at
    the first member.
    """
    blob = b""
    index = struct.pack("<I", len(members))
    offset = 0
    for addr, payload in members:
        digest = hashlib.sha256(payload).digest()
        blob += (b"WALM" + struct.pack("<HH", 1, 0) + addr.encode()
                 + struct.pack("<Q", len(payload)) + digest + payload)
        index += addr.encode() + struct.pack("<QQ", offset, len(payload))
        offset += 64 + len(payload)
    return blob + (index + struct.pack("<I", len(index)) + b"WALK" if sealed else b"")


class ArchiveInstruments(unittest.TestCase):
    def test_durable_archive_is_reachable_without_ordinary_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            archive = root / "wal-archive-durable"
            archive.mkdir()
            addr = "1234567890abcdef"
            (archive / "fold-10.ark").write_bytes(container((addr, b"durable transcript")))
            self.assertEqual(wa.archived_addresses(root / "wal"), {addr})
            self.assertEqual(wa.archived_size(root / "wal", addr), len(b"durable transcript"))
    def test_store_named_archived_session_reachable_and_container_never_swept(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            wal = root / "wal"
            archive = root / "wal-archive"
            wal.mkdir()
            archive.mkdir()
            key = "/p\x1frunner\x1fcold"
            addr = wa.fnv1a64(key)
            # A SECOND MEMBER, so an inventory walk that returns after the first is
            # distinguishable from one that walks the whole container.
            second_key = "/p\x1frunner\x1fsecond"
            second = wa.fnv1a64(second_key)
            path = archive / "fold-10.ark"
            path.write_bytes(container((addr, b"archived transcript"), (second, b"second")))
            (wal / "warm.wal").write_bytes(b"warm" * 40)
            (wal / "empty.wal").write_bytes(b"")
            (wal / "lineage.wal").write_bytes(b"x" * 69)
            db_path = root / "run-index.db"
            with sqlite3.connect(db_path) as db:
                db.execute("CREATE TABLE meta(session TEXT, prefix_len INTEGER)")
                db.execute("INSERT INTO meta VALUES (?, 8)", (key,))
                # THE STORE MUST NAME THE SECOND MEMBER TOO, or the extra member proves
                # nothing: a walk that stops after the first still resolves everything
                # the store asked about. It is the second session being NAMED that turns
                # an early return into a reported-missing row.
                db.execute("INSERT INTO meta VALUES (?, 4)", (second_key,))
                # A REAL STORE ALWAYS HAS BOTH TABLES. This fixture built only meta and
                # so was never representative: `addressable_keys` has always read
                # run_index unconditionally, and the store axis now does too.
                db.execute("CREATE TABLE run_index(run_id TEXT, session TEXT)")
                db.execute(
                    "INSERT INTO run_index VALUES ('r', ?)",
                    (json.dumps({"project_root": "/p", "harness": "runner",
                                 "session": "cold"}, separators=(",", ":")),),
                )
            self.assertEqual(wa.unreachable_from_store(db_path, wal), ([], 2))
            self.assertEqual(wa.archived_size(wal, addr), len(b"archived transcript"))
            self.assertEqual(sweep.classify(wal), ([wal / "empty.wal"], 1, 1))
            self.assertEqual(sweep.classify(archive), ([], 0, 0))
            path.write_bytes(container((addr, b"archived transcript"), (second, b"second"), sealed=False))
            self.assertEqual(
                    wa.unreachable_from_store(db_path, wal),
                    ([(key, 8), (second_key, 4)], 2),
                )
            path.write_bytes(container((addr, b"archived transcript"), (second, b"second"))[:-8] + struct.pack("<I", 0) + b"WALK")
            self.assertEqual(wa.unreachable_from_store(db_path, wal), ([], 2))
            damaged = bytearray(path.read_bytes())
            damaged[64] ^= 1
            path.write_bytes(damaged)
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                wa.unreachable_from_store(db_path, wal)

            # An index-only session without a WAL must still enter the store axis.
            # One-shot sends can skip projection, leaving no corresponding meta row.
            path.write_bytes(container((addr, b"archived transcript"), (second, b"second")))
            with sqlite3.connect(db_path) as db:
                db.execute(
                    "INSERT INTO run_index VALUES ('orphan', ?)",
                    (json.dumps({"project_root": "/p", "harness": "runner",
                                 "session": "index-only"}, separators=(",", ":")),),
                )
            missing, named = wa.unreachable_from_store(db_path, wal)
            self.assertEqual(named, 3, "population must include index-only rows")
            self.assertEqual(
                missing,
                [("/p\x1frunner\x1findex-only", 0)],
                "an index-only session with no WAL must be reported missing",
            )


if __name__ == "__main__":
    unittest.main()
