#!/usr/bin/env python3
"""Verify per-run shutdown readback against framed WAL records."""
from __future__ import annotations

import hashlib
import json
import pathlib
import struct
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from lib.seal_readback import CANCELLED, SEALED, UNCONFIRMED, active_run_id, classify_run


def frame(seq: int, record: dict) -> bytes:
    payload = json.dumps(record, separators=(",", ":")).encode()
    version, fence = 1, 1
    digest = hashlib.sha256(
        bytes((version,)) + struct.pack("<Q", seq) + struct.pack("<Q", fence) + payload
    ).digest()
    return struct.pack("<IBQQ", len(payload), version, seq, fence) + digest + payload


class SealReadbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="place-release-seal-")
        self.addCleanup(self.tmp.cleanup)
        self.wal = pathlib.Path(self.tmp.name) / "session.wal"

    def write_records(self, *records: dict) -> None:
        self.wal.write_bytes(b"".join(frame(seq, record) for seq, record in enumerate(records, 1)))

    def test_a_revived_session_seals_the_run_that_was_live_at_restart(self) -> None:
        self.write_records(
            {"type": "run_started", "run_id": "run-before"},
            {"type": "step_started"},
            {"type": "run_finished", "reason": "interrupted"},
            {"type": "run_started", "run_id": "run-after"},
            {"type": "step_started"},
        )
        self.assertEqual(classify_run(self.wal, "run-before"), SEALED)
        self.assertEqual(active_run_id(self.wal), "run-after")

    def test_a_live_run_without_a_terminal_is_unconfirmed(self) -> None:
        self.write_records(
            {"type": "run_started", "run_id": "run-open"},
            {"type": "step_started"},
        )
        self.assertEqual(active_run_id(self.wal), "run-open")
        self.assertEqual(classify_run(self.wal, "run-open"), UNCONFIRMED)

    def test_a_cancelled_terminal_is_classified_as_failure(self) -> None:
        self.write_records(
            {"type": "run_started", "run_id": "run-cancelled"},
            {"type": "run_finished", "reason": "cancelled"},
        )
        self.assertEqual(classify_run(self.wal, "run-cancelled"), CANCELLED)

    def test_auth_pause_does_not_confirm_a_restart_seal(self) -> None:
        self.write_records(
            {"type": "run_started", "run_id": "run-auth-paused"},
            {"type": "run_paused", "reason": "auth_required"},
            {"type": "resume_started"},
            {"type": "step_started"},
        )
        self.assertEqual(classify_run(self.wal, "run-auth-paused"), UNCONFIRMED)

    def test_restart_pause_then_resume_counts_as_sealed(self) -> None:
        self.write_records(
            {"type": "run_started", "run_id": "run-paused"},
            {"type": "run_paused", "reason": "restart"},
            {"type": "resume_started"},
            {"type": "step_started"},
        )
        self.assertEqual(classify_run(self.wal, "run-paused"), SEALED)
        self.assertEqual(active_run_id(self.wal), "run-paused")


if __name__ == "__main__":
    unittest.main()
