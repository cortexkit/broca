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

EXPECTED_SUITE_ANCHORS = 22

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

    def test_a_log_deleted_mid_scan_rescans_instead_of_refusing(self) -> None:
        # Retention deletes an expired retained log between the listing and the tail read.
        # That log held no live run, so the gate rescans and decides on what remains.
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            wal_dir = root / "wal"
            wal_dir.mkdir()
            (wal_dir / "gone.wal").write_bytes(frame(1, STARTED) + frame(2, FINISHED))
            (wal_dir / "sealed.wal").write_bytes(frame(1, STARTED) + frame(2, FINISHED))
            real = live_runs.tail_state

            def delete_then_read(path: pathlib.Path) -> str:
                if path.name == "gone.wal" and path.exists():
                    path.unlink()
                return real(path)

            with unittest.mock.patch.object(live_runs, "tail_state", delete_then_read):
                code, out, err = self.run_main(root, int(time.time()) - 60)
            self.assertEqual(code, 0, err)
            self.assertIn("wal files scanned  1", out)
            self.assertNotIn("cannot compare", err)

    def test_a_log_that_keeps_vanishing_still_refuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "wal").mkdir()
            (root / "wal" / "flaky.wal").write_bytes(frame(1, STARTED) + frame(2, FINISHED))

            def always_gone(path: pathlib.Path) -> str:
                raise live_runs.WalUnreadable(path, FileNotFoundError(2, "gone")) from FileNotFoundError(2, "gone")

            with unittest.mock.patch.object(live_runs, "tail_state", always_gone):
                code, _out, err = self.run_main(root, int(time.time()) - 60)
            self.assertEqual(code, 2, err)
            self.assertIn("cannot compare: cannot read flaky.wal", err)

    def test_an_unreadable_log_that_still_exists_refuses_without_rescanning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "wal").mkdir()
            (root / "wal" / "locked.wal").write_bytes(frame(1, STARTED))
            calls = []

            def denied(path: pathlib.Path) -> str:
                calls.append(path.name)
                raise live_runs.WalUnreadable(path, PermissionError(13, "denied")) from PermissionError(13, "denied")

            with unittest.mock.patch.object(live_runs, "tail_state", denied):
                code, _out, err = self.run_main(root, int(time.time()) - 60)
            self.assertEqual(code, 2, err)
            self.assertEqual(calls, ["locked.wal"])

    def run_main_against_processes(self, pids: list[str]) -> tuple[int, str, str]:
        """Run the gate with no --boot-epoch, so it must find the daemon itself. One WAL
        holds an open run written now; each matching process started a minute ago."""
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "wal").mkdir()
            (root / "wal" / "live.wal").write_bytes(frame(1, STARTED))
            stdout, stderr = io.StringIO(), io.StringIO()
            argv = ["live-runs.py", "--state-root", str(root)]
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr), \
                 unittest.mock.patch.object(sys, "argv", argv), \
                 unittest.mock.patch.object(live_runs, "matching_pids", return_value=pids), \
                 unittest.mock.patch.object(live_runs, "start_epoch",
                                            return_value=int(time.time()) - 60):
                code = live_runs.main()
            return code, stdout.getvalue(), stderr.getvalue()

    def test_two_matching_processes_refuse_instead_of_picking_one(self) -> None:
        # Taking the first match let a process that started after the daemon supply the
        # epoch, and the daemon's live runs then read as abandoned.
        code, out, err = self.run_main_against_processes(["111", "222"])
        self.assertEqual(code, 2, out + err)
        self.assertIn("cannot compare: 2 processes match 'bin/ck-broca' (pids 111 222)", err)
        self.assertNotIn("live runs", out)

    def test_one_matching_process_supplies_the_epoch(self) -> None:
        # The control: the same scan with a single daemon finds the open run live.
        code, out, err = self.run_main_against_processes(["111"])
        self.assertEqual(code, 1, out + err)
        self.assertIn("live runs          1", out)

    DAEMON = "/home/u/.local/share/cortexkit/bin/ck-broca"

    def matches(self, processes: dict[int, list[str]]) -> list[str]:
        """matching_pids over a fake /proc holding each pid's argv as its cmdline file."""
        with tempfile.TemporaryDirectory() as tmp:
            proc = pathlib.Path(tmp)
            for pid, argv in processes.items():
                (proc / str(pid)).mkdir()
                (proc / str(pid) / "cmdline").write_bytes(
                    b"".join(os.fsencode(arg) + b"\0" for arg in argv)
                )
            (proc / "self").mkdir()  # non-numeric entries are not processes
            return live_runs.matching_pids("bin/ck-broca", proc_root=proc)

    def test_the_scripts_own_process_is_never_a_match(self) -> None:
        own = os.getpid()
        self.assertEqual(
            self.matches({own: [self.DAEMON, "--subc", "c"], 4242: [self.DAEMON, "--subc", "c"]}),
            ["4242"],
        )

    def test_a_later_argument_naming_the_daemon_path_is_not_a_match(self) -> None:
        self.assertEqual(
            self.matches({
                111: [self.DAEMON, "--subc", "/run/subc-connection.json"],
                222: ["bash", "-c", f"sleep 5; cp x {self.DAEMON}"],
                333: ["tail", "-f", f"{self.DAEMON}.log"],
                444: ["python3", "scripts/live-runs.py", "--process", "bin/ck-broca"],
            }),
            ["111"],
        )

    def test_the_rollback_command_deploy_prints_is_not_a_match(self) -> None:
        # deploy.sh's rollback_command: `<repo>/scripts/place-release.sh --staged <backup>
        # --dest $HOME/.local/share/cortexkit/bin/ck-broca --restart --live-approved`. Its
        # `#!/usr/bin/env bash` shebang leaves bash as argv[0]; an explicit `bash <script>` too.
        rollback = [
            "/home/u/src/broca/scripts/place-release.sh", "--staged",
            "/home/u/.local/share/cortexkit/rollback/ck-broca.20261022T101500Z",
            "--dest", self.DAEMON, "--restart", "--live-approved",
        ]
        daemon = {111: [self.DAEMON, "--subc", "/run/subc-connection.json"]}
        self.assertEqual(self.matches({**daemon, 555: ["/bin/bash", *rollback]}), ["111"])
        self.assertEqual(self.matches({**daemon, 556: ["bash", *rollback]}), ["111"])

    def test_two_daemon_executables_still_refuse(self) -> None:
        # The control: two daemon-executable entries both match, and the gate refuses to pick.
        two = {111: [self.DAEMON, "--subc", "/run/a.json"], 112: [self.DAEMON, "--subc", "/run/b.json"]}
        self.assertEqual(self.matches(two), ["111", "112"])
        with unittest.mock.patch.object(live_runs, "matching_pids", return_value=["111", "112"]):
            with self.assertRaises(live_runs.AmbiguousProcess):
                live_runs.process_start_epoch("bin/ck-broca")

    def test_an_unreadable_process_of_this_user_is_unknown_not_absent(self) -> None:
        # It might be the daemon, so skipping it could hide the real boot time.
        with tempfile.TemporaryDirectory() as tmp:
            proc = pathlib.Path(tmp)
            (proc / "9" / "cmdline").mkdir(parents=True)  # reading it fails with an OSError
            with self.assertRaises(live_runs.AmbiguousProcess):
                live_runs.matching_pids("bin/ck-broca", proc_root=proc)

    def test_an_executable_path_with_spaces_is_read_whole(self) -> None:
        spaced = "/tmp/linux deploy x/bin/ck-broca"
        self.assertEqual(self.matches({7: [spaced, "--subc", "c"], 8: ["/bin/sh", spaced]}), ["7"])

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
        self.assertEqual(
            known,
            live_runs.wal_gate.broca_known_features(
                source, live_runs.wal_gate._broca_wal_module_source
            ),
        )
        # An entry naming another module's constant resolves through that module's source.
        self.assertIn("visibility-late/v1", known)
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
        qualified = (
            'pub const KNOWN_FEATURES: &[&str] = &[\n    A_FEATURE,\n    crate::other::B_FEATURE,\n];\n'
            'pub const A_FEATURE: &str = "a/v1";\n'
        )
        modules = {"other": 'pub const B_FEATURE: &str = "b/v1";\n'}
        self.assertEqual(
            parse(qualified, lambda module: modules.get(module, "")),
            frozenset({"a/v1", "b/v1"}),
        )
        # A qualified constant is read from its own module, never from framing.rs, and a
        # module that doesn't define it fails loudly.
        with self.assertRaises(ValueError):
            parse(qualified + 'pub const B_FEATURE: &str = "b/v1";\n')
        with self.assertRaises(ValueError):
            parse(qualified, lambda module: "")

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
