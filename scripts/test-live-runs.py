#!/usr/bin/env python3
"""Executable verification suite for how scripts/live-runs.py reads gated WAL frames."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import struct
import sys
import tempfile
import time
import unittest
import unittest.mock
from typing import Any

SCRIPT = pathlib.Path(__file__).with_name("live-runs.py")
SPEC = importlib.util.spec_from_file_location("live_runs", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot import {SCRIPT}")
live_runs = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = live_runs
SPEC.loader.exec_module(live_runs)

EXPECTED_SUITE_ANCHORS = 11

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
STARTED = {"type": "run_started", "run_id": "r"}
FINISHED = {"type": "run_finished", "reason": "completed"}


def frame(seq: int, record: dict[str, Any], requires: list[str] | None = None) -> bytes:
    """A frame as broca writes it: version 1, or version 3 when `requires` is given."""
    if requires is None:
        version, value = 1, record
    else:
        version, value = 3, {"requires": requires, "record": record}
    payload = json.dumps(value, separators=(",", ":")).encode()
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


def big_result(seq: int) -> bytes:
    """A record larger than two read blocks. Placed last, it makes the backwards tail read
    pass over whole blocks before reaching the frames written ahead of it."""
    return frame(seq, {"type": "tool_result", "output": "x" * 3 * live_runs._CHUNK})


class LiveRunsGateTests(unittest.TestCase):
    def test_restart_pause_is_quiet_and_a_later_step_reopens_it(self) -> None:
        paused = frame(1, STARTED) + frame(2, {"type": "run_paused", "reason": "restart"}, ["restart-pause/v1"])
        self.assertEqual(self.state(paused), "sealed")
        self.assertEqual(self.state(paused + frame(3, {"type": "resume_started"}) + frame(4, {"type": "step_started"})), "open")

    def test_fixture_frame_is_the_layout_broca_writes(self) -> None:
        # The same frame `gated_frame_wire_bytes_are_pinned` in crates/broca-wal/src/framing.rs
        # pins, compared with its digest literal, so these fixtures cannot drift from broca's
        # real layout while every test here keeps agreeing with itself.
        raw = frame(1, {"type": "turn_finished", "step_id": 1}, [TEST_FEATURE])
        payload = b'{"requires":["test-feature/v1"],"record":{"type":"turn_finished","step_id":1}}'
        self.assertEqual(raw[53:], payload)
        self.assertEqual(raw[4], 3)
        self.assertEqual(
            raw[21:53].hex(),
            "e47e87cd99121c97a249b823131b808ddd189d68ca27079b641ccb4d7bc85b47",
        )

    def state(self, raw: bytes) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "a.wal"
            path.write_bytes(raw)
            return live_runs.tail_state(path)

    def test_gated_transition_with_known_features_is_read_like_any_other(self) -> None:
        with unittest.mock.patch.object(
            live_runs.wal_gate, "KNOWN_FEATURES", frozenset({TEST_FEATURE})
        ):
            self.assertEqual(
                self.state(frame(1, STARTED, [TEST_FEATURE])), live_runs.OPEN
            )
            self.assertEqual(
                self.state(frame(1, STARTED) + frame(2, FINISHED, [TEST_FEATURE])),
                live_runs.SEALED,
            )

    def test_unknown_gated_terminal_is_newer_writer_never_sealed(self) -> None:
        # Read as tokens alone this file ends in run_finished and would be called sealed, but
        # the terminal is a newer build's record whose meaning this script cannot know.
        raw = frame(1, STARTED) + frame(2, FINISHED, ["aa-newer/v1"])
        self.assertEqual(self.state(raw), live_runs.NEWER_WRITER)

    def test_unknown_gated_frame_in_an_earlier_block_is_still_seen(self) -> None:
        # The deciding token's gated frame sits blocks before the end of the file, behind a
        # large later record, so the scan must keep what it already read.
        raw = frame(1, STARTED) + frame(2, FINISHED, ["aa-newer/v1"]) + big_result(3)
        self.assertGreater(len(raw) - raw.index(b"run_finished"), 2 * live_runs._CHUNK)
        self.assertEqual(self.state(raw), live_runs.NEWER_WRITER)
        # And a gated frame after the last transition, with no transition of its own.
        raw = frame(1, STARTED) + frame(2, FINISHED) + frame(3, {"type": "steered"}, ["aa-newer/v1"])
        self.assertEqual(self.state(raw), live_runs.NEWER_WRITER)

    def test_unknown_gated_frame_before_the_deciding_transition_does_not_decide(self) -> None:
        # Only the last transition decides whether a run is open; an earlier gated record
        # followed by a plain terminal leaves the verdict to that terminal.
        raw = frame(1, STARTED, ["aa-newer/v1"]) + frame(2, FINISHED)
        self.assertEqual(self.state(raw), live_runs.SEALED)
        # A file with no transition at all but an unknown gated record is not called idle.
        self.assertEqual(
            self.state(frame(1, {"type": "steered"}, ["aa-newer/v1"])), live_runs.NEWER_WRITER
        )

    def run_main(self, root: pathlib.Path, boot: int) -> tuple[int, str, str]:
        stdout, stderr = io.StringIO(), io.StringIO()
        argv = ["live-runs.py", "--state-root", str(root), "--boot-epoch", str(boot)]
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with unittest.mock.patch.object(sys, "argv", argv):
                code = live_runs.main()
        return code, stdout.getvalue(), stderr.getvalue()

    def test_report_names_newer_writer_files_and_does_not_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            wal_dir = root / "wal"
            wal_dir.mkdir()
            (wal_dir / "newer.wal").write_bytes(frame(1, STARTED) + frame(2, FINISHED, ["aa-newer/v1"]))
            (wal_dir / "sealed.wal").write_bytes(frame(1, STARTED) + frame(2, FINISHED))
            (wal_dir / "abandoned.wal").write_bytes(frame(1, STARTED))
            old = time.time() - 3_600
            os.utime(wal_dir / "abandoned.wal", (old, old))

            # newer.wal was written after the boot epoch passed in, so the scan cannot tell
            # whether it holds a live run and must not approve a restart.
            code, out, err = self.run_main(root, int(time.time()) - 60)
            self.assertEqual(code, 2, err)
            self.assertIn("wal files scanned  3", out)
            self.assertIn("live runs          0", out)
            self.assertIn("abandoned          1", out)
            self.assertIn("newer-writer       1  (a newer broca wrote these; 1 since", out)
            self.assertIn("  newer-writer  newer.wal", out)
            self.assertIn("newer-writer", err)
            self.assertNotIn("cannot read", err)

    def test_newer_writer_file_older_than_the_process_is_listed_not_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            wal_dir = root / "wal"
            wal_dir.mkdir()
            path = wal_dir / "newer.wal"
            path.write_bytes(frame(1, STARTED, ["aa-newer/v1"]))
            old = time.time() - 3_600
            os.utime(path, (old, old))
            code, out, err = self.run_main(root, int(time.time()) - 60)
            self.assertEqual(code, 0, err)
            self.assertIn("newer-writer       1  (a newer broca wrote these; 0 since", out)

    def test_resume_started_neither_seals_nor_opens_a_run(self) -> None:
        # The crash-orphan pass writes `resume_started` before it seals a run. A sealed
        # close-out is not live; one cut after `resume_started` but before its terminal still is.
        self.assertEqual(live_runs.tail_state(SEALED_CLOSE_OUT), live_runs.SEALED)
        self.assertEqual(live_runs.tail_state(UNSEALED_CLOSE_OUT), live_runs.OPEN)
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            wal_dir = root / "wal"
            wal_dir.mkdir()
            (wal_dir / "sealed.wal").write_bytes(SEALED_CLOSE_OUT.read_bytes())
            code, out, err = self.run_main(root, int(time.time()) - 60)
            self.assertEqual(code, 0, err)
            self.assertIn("wal files scanned  1", out)
            self.assertIn("live runs          0", out)
            (wal_dir / "unsealed.wal").write_bytes(UNSEALED_CLOSE_OUT.read_bytes())
            code, out, err = self.run_main(root, int(time.time()) - 60)
            self.assertEqual(code, 1, err)
            self.assertIn("live runs          1", out)
            self.assertIn("  live  unsealed.wal", out)


class KnownFeatureTableTests(unittest.TestCase):
    """The scripts' feature table is broca's, read from framing.rs, never a copy."""

    def test_table_is_brocas_and_covers_a_feature_every_tool_call_writes(self) -> None:
        # Positive control: `dispatch-module/v1` gates every dispatched tool call since broca
        # v0.3.166, so a table without it reads nearly every recent WAL as a newer writer and
        # the gate can never report a quiet daemon.
        known = live_runs.wal_gate.KNOWN_FEATURES
        self.assertIn("dispatch-module/v1", known)
        source = live_runs.wal_gate.FRAMING_RS.read_text()
        self.assertEqual(known, live_runs.wal_gate.broca_known_features(source))
        # A WAL whose gated tool-call frame needs only that feature is read, not set aside.
        raw = frame(1, STARTED) + frame(2, {"type": "step_started"}, ["dispatch-module/v1"])
        self.assertEqual(self.state(raw), live_runs.OPEN)

    def test_parser_refuses_a_table_it_cannot_fully_resolve(self) -> None:
        parse = live_runs.wal_gate.broca_known_features
        good = (
            'pub const KNOWN_FEATURES: &[&str] = &[\n    A_FEATURE,\n];\n'
            'pub const A_FEATURE: &str = "a/v1";\n'
        )
        self.assertEqual(parse(good), frozenset({"a/v1"}))
        with self.assertRaises(ValueError):
            parse(good.replace("KNOWN_FEATURES", "RENAMED"))
        with self.assertRaises(ValueError):
            parse(good.replace('pub const A_FEATURE: &str = "a/v1";', ""))
        with self.assertRaises(ValueError):
            parse('pub const KNOWN_FEATURES: &[&str] = &[\n];\n')

    state = LiveRunsGateTests.state


def main() -> int:
    loader = unittest.defaultTestLoader
    suite = loader.loadTestsFromTestCase(LiveRunsGateTests)
    suite.addTests(loader.loadTestsFromTestCase(KnownFeatureTableTests))
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
