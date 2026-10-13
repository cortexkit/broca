#!/usr/bin/env python3
"""Executable verification suite for how scripts/find-finalize-leaks.py reads gated WAL frames."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import struct
import sys
import tempfile
import unittest
import unittest.mock
from typing import Any

SCRIPT = pathlib.Path(__file__).with_name("find-finalize-leaks.py")
SPEC = importlib.util.spec_from_file_location("find_finalize_leaks", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot import {SCRIPT}")
leaks = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = leaks
SPEC.loader.exec_module(leaks)

EXPECTED_SUITE_ANCHORS = 5

# Session logs written by the real runner for a resume close-out: two dispatched calls with no
# result, the resume's `resume_started` record, an `outcome_unknown` result for each, and the
# Interrupted seal. The unsealed one is cut just after `resume_started`, so its episode still
# has no terminal. `crates/broca-crash-harness/tests/it/script_fixtures.rs` checks they still match
# what the runner writes.
FIXTURES = pathlib.Path(__file__).with_name("fixtures")
SEALED_CLOSE_OUT = FIXTURES / "resume-close-out-sealed.wal"
UNSEALED_CLOSE_OUT = FIXTURES / "resume-close-out-unsealed.wal"

# A feature name no build ships. A test that needs it known patches it into
# `lib.wal_gate.KNOWN_FEATURES`; everywhere else it reads as unknown.
TEST_FEATURE = "test-feature/v1"


def frame(seq: int, payload_value: dict[str, Any], version: int) -> bytes:
    payload = json.dumps(payload_value, separators=(",", ":")).encode()
    digest = hashlib.sha256(
        bytes((version,)) + struct.pack("<Q", seq) + struct.pack("<Q", 1) + payload
    ).digest()
    return (
        struct.pack("<I", len(payload))
        + bytes((version,))
        + struct.pack("<Q", seq)
        + struct.pack("<Q", 1)
        + digest
        + payload
    )


def finalize_turn() -> list[dict[str, Any]]:
    """One budget-finalize turn whose tool call is never answered: one leak."""
    return [
        {"type": "run_started", "config": {"model": {"provider_module_id": "p", "model_id": "m"}}},
        {"type": "budget_fired", "kind": "finalize"},
        {
            "type": "model_step_finished",
            "assistant_message": {"content": [{"type": "tool_call", "tool_call_id": "c1"}]},
        },
        {"type": "run_finished", "reason": "completed"},
    ]


def wal_bytes(records: list[dict[str, Any]], gate_last: list[str] | None = None) -> bytes:
    """Frame `records` as version 1, except the last as a version-3 frame requiring `gate_last`."""
    out = b""
    for seq, record in enumerate(records, 1):
        if gate_last is not None and seq == len(records):
            out += frame(seq, {"requires": gate_last, "record": record}, 3)
        else:
            out += frame(seq, record, 1)
    return out


class FinalizeLeakGateTests(unittest.TestCase):
    def test_restart_pause_is_nonterminal_and_read_through(self) -> None:
        pause = {"type": "run_paused", "reason": "restart"}
        raw = wal_bytes([{"type": "run_started"}, pause], gate_last=["restart-pause/v1"])
        bodies, complete, missing = leaks.records(raw)
        self.assertTrue(complete)
        self.assertEqual(missing, [])
        self.assertEqual(bodies[-1], pause)

    def test_gated_frame_with_known_features_is_decoded(self) -> None:
        raw = wal_bytes(finalize_turn(), gate_last=[TEST_FEATURE])
        with unittest.mock.patch.object(
            leaks.wal_gate, "KNOWN_FEATURES", frozenset({TEST_FEATURE})
        ):
            bodies, complete, missing = leaks.records(raw)
        self.assertEqual(missing, [])
        self.assertTrue(complete, "a known gated frame is read through, not a stop")
        self.assertEqual(bodies, finalize_turn())

    def test_unknown_feature_is_newer_writer_not_a_partial_file(self) -> None:
        raw = wal_bytes(finalize_turn(), gate_last=["aa-newer/v1"])
        bodies, complete, missing = leaks.records(raw)
        self.assertEqual(missing, ["aa-newer/v1"])
        self.assertEqual(bodies, [], "no record of a newer build's WAL is used")
        self.assertTrue(complete, "the file is intact, not ended mid-frame")

    def test_scan_reports_newer_writer_by_name_and_reads_the_rest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            wal_dir = root / "wal"
            wal_dir.mkdir()
            (wal_dir / "known.wal").write_bytes(wal_bytes(finalize_turn()))
            (wal_dir / "newer.wal").write_bytes(
                wal_bytes(finalize_turn(), gate_last=["aa-newer/v1"])
            )
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with unittest.mock.patch.object(
                    sys, "argv", ["find-finalize-leaks.py", "--state-root", str(root), "--baseline", "1"]
                ):
                    code = leaks.main()
            self.assertEqual(code, 2, stderr.getvalue())
            out = stdout.getvalue()
            self.assertIn("wal files scanned    2", out)
            self.assertIn("ended mid-frame      0", out)
            self.assertIn("newer-writer         1", out)
            self.assertIn("newer-writer newer.wal: requires aa-newer/v1", out)
            # known.wal is still read although newer.wal is refused: its leak is counted.
            self.assertIn("leaked tool calls    1", out)
            self.assertIn("newer-writer", stderr.getvalue())
            self.assertNotIn("mid-frame", stderr.getvalue())

    def test_resume_started_is_decoded_and_skipped_without_moving_the_count(self) -> None:
        # The real close-out decodes whole, `resume_started` included, and adds no leak.
        bodies, complete, missing = leaks.records(SEALED_CLOSE_OUT.read_bytes())
        self.assertEqual((complete, missing), (True, []))
        self.assertIn("resume_started", [body.get("type") for body in bodies])
        # A finalize-turn leak is still counted once when a resume record precedes its seal.
        turn = finalize_turn()
        resume = {"type": "resume_started", "episode": "r", "resumed_from_seq": 3,
                  "replayed": {"messages": 1, "model_steps": 1, "tool_results": 0},
                  "redone": [], "indeterminate": []}
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            wal_dir = root / "wal"
            wal_dir.mkdir()
            (wal_dir / "close-out.wal").write_bytes(SEALED_CLOSE_OUT.read_bytes())
            (wal_dir / "leak.wal").write_bytes(wal_bytes(turn[:-1] + [resume, turn[-1]]))
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with unittest.mock.patch.object(
                    sys, "argv", ["find-finalize-leaks.py", "--state-root", str(root), "--baseline", "1"]
                ):
                    code = leaks.main()
            out = stdout.getvalue()
            self.assertIn("wal files scanned    2", out, stderr.getvalue())
            self.assertIn("ended mid-frame      0", out)
            self.assertIn("leaked tool calls    1", out)


def main() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(FinalizeLeakGateTests)
    names = [test.id() for test in suite]
    if len(names) != EXPECTED_SUITE_ANCHORS:
        print(
            f"suite anchor mismatch before execution: expected {EXPECTED_SUITE_ANCHORS}, "
            f"listed {len(names)}"
        )
        return 1
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if result.testsRun <= 0:
        print("refusing vacuous test execution")
        return 1
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
