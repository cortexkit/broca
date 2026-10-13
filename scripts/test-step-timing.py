#!/usr/bin/env python3
"""Executable verification suite for scripts/step-timing.py."""

from __future__ import annotations

import argparse
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

SCRIPT = pathlib.Path(__file__).with_name("step-timing.py")
SPEC = importlib.util.spec_from_file_location("step_timing", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot import {SCRIPT}")
step_timing = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = step_timing
SPEC.loader.exec_module(step_timing)

EXPECTED_SUITE_ANCHORS = 9

# Session logs written by the real runner for a resume close-out: two dispatched calls with no
# result, the resume's `resume_started` record, an `outcome_unknown` result for each, and the
# Interrupted seal. The unsealed one is cut just after `resume_started`, so its episode still
# has no terminal. `crates/broca-crash-harness/tests/it/script_fixtures.rs` checks they still match
# what the runner writes.
FIXTURES = pathlib.Path(__file__).with_name("fixtures")
SEALED_CLOSE_OUT = FIXTURES / "resume-close-out-sealed.wal"
UNSEALED_CLOSE_OUT = FIXTURES / "resume-close-out-unsealed.wal"

# A feature name no build ships. Tests that need a gated frame whose features ARE known patch
# it into `lib.wal_gate.KNOWN_FEATURES`; everywhere else it reads as unknown.
TEST_FEATURE = "test-feature/v1"


def frame(
    seq: int, value: dict[str, Any], fence: int = 7, version: int | None = None
) -> bytes:
    payload = json.dumps(value, separators=(",", ":")).encode()
    version = step_timing.FRAMING_VERSION if version is None else version
    digest = hashlib.sha256(
        bytes((version,))
        + struct.pack("<Q", seq)
        + struct.pack("<Q", fence)
        + payload
    ).digest()
    return (
        struct.pack("<I", len(payload))
        + bytes((version,))
        + struct.pack("<Q", seq)
        + struct.pack("<Q", fence)
        + digest
        + payload
    )


def gated(requires: list[str], record: dict[str, Any]) -> dict[str, Any]:
    """The payload of a version-3 frame, in broca's field order."""
    return {"requires": requires, "record": record}


def lineage_frame() -> bytes:
    payload = bytes(range(step_timing.LINEAGE_PAYLOAD_LEN))
    version = step_timing.LINEAGE_FRAMING_VERSION
    digest = hashlib.sha256(
        bytes((version,))
        + struct.pack("<Q", 0)
        + struct.pack("<Q", 0)
        + payload
    ).digest()
    return (
        struct.pack("<I", len(payload))
        + bytes((version,))
        + struct.pack("<Q", 0)
        + struct.pack("<Q", 0)
        + digest
        + payload
    )


def started(
    run_id: str = "run-1",
    project_root: str = "/work/project",
    origin: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "type": "run_started",
        "run_id": run_id,
        "session": {
            "project_root": project_root,
            "harness": "test",
            "session": run_id,
        },
        "config": {
            "model": {
                "provider_module_id": "known-provider",
                "model_id": "known-model",
            },
            "frozen_render_config": {"family_id": "known-family"},
        },
        "input": [],
        "origin": origin or {"kind": "fresh"},
    }


def envelope(ts_ms: int, record: dict[str, Any]) -> dict[str, Any]:
    return {"ts_ms": ts_ms, "record": record}


def write_wal(root: pathlib.Path, name: str, values: list[dict[str, Any]]) -> pathlib.Path:
    wal_dir = root / "wal"
    wal_dir.mkdir(exist_ok=True)
    path = wal_dir / name
    lineage = lineage_frame()
    # A value shaped like `gated(...)` is written as a version-3 frame, as broca writes one.
    records = b"".join(
        frame(
            index,
            value,
            version=step_timing.GATED_FRAMING_VERSION if "requires" in value else None,
        )
        + lineage
        for index, value in enumerate(values, 1)
    )
    path.write_bytes(lineage + records)
    return path


def known_values() -> list[dict[str, Any]]:
    return [
        envelope(900, started()),
        envelope(1_000, {"type": "step_started", "step_id": 1}),
        envelope(
            2_500,
            {
                "type": "model_step_finished",
                "step_id": 1,
                "assistant_message": {
                    "message_id": "message-1",
                    "content": [
                        {"type": "text", "text": "hello"},
                        {"type": "reasoning", "text": "think"},
                    ],
                },
                "usage": {},
            },
        ),
        envelope(3_000, {"type": "step_started", "step_id": 2}),
        envelope(
            5_500,
            {
                "type": "model_step_finished",
                "step_id": 2,
                "assistant_message": {
                    "message_id": "message-2",
                    "content": [
                        {"type": "redacted_reasoning", "data": "opaque"},
                        {
                            "type": "tool_call",
                            "tool_name": "ignored",
                            "input": {"large": "tool input is not requested content"},
                        },
                    ],
                },
                "usage": {},
            },
        ),
        envelope(
            5_600,
            {"type": "run_finished", "reason": "completed", "usage": {}},
        ),
    ]


class StepTimingTests(unittest.TestCase):
    def test_known_frame_timestamps_report_planted_durations_and_payload_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            values = known_values()
            timestamp_anchors = sum("ts_ms" in value for value in values)
            self.assertEqual(timestamp_anchors, 6, "count timestamp anchors before writing")
            write_wal(root, "known.wal", values)

            result = step_timing.scan_corpus(root)
            self.assertEqual([step.duration_ms for step in result.steps], [1_500, 2_500])
            self.assertEqual([step.content_bytes for step in result.steps], [10, 6])
            self.assertEqual(
                [step.rate_bytes_per_second for step in result.steps],
                [10 * 1000 / 1_500, 6 * 1000 / 2_500],
            )
            report, refusal = step_timing.render_report(result, 10, 1.0)
            self.assertIsNone(refusal)
            self.assertIn("duration_ms=2500 content_bytes=6", report)
            self.assertIn("provider=known-provider family=known-family", report)

    def test_empty_corpus_refuses_instead_of_reporting_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "wal").mkdir()
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = step_timing.main(["--state-root", str(root)])
            self.assertEqual(code, 2)
            self.assertIn("REFUSING: empty corpus", stderr.getvalue())

    def test_directory_with_no_decodable_frames_refuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            wal_dir = root / "wal"
            wal_dir.mkdir()
            (wal_dir / "empty.wal").write_bytes(b"")
            (wal_dir / "torn.wal").write_bytes(b"short")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = step_timing.main(["--state-root", str(root)])
            self.assertEqual(code, 2)
            # Population discovery now verifies lineage framing before timing decoding;
            # an unreadable frame still refuses, rather than reporting a zero corpus.
            self.assertIn("REFUSING:", stderr.getvalue())
            self.assertIn("torn tail (incomplete header)", stderr.getvalue())

    def test_decoder_envelope_mutation_refuses_without_manufacturing_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_wal(root, "known.wal", known_values())

            # Mutation: decode every record but deliberately skip the envelope timestamp.
            result = step_timing.scan_corpus(root, timestamp_reader=lambda _envelope: None)
            report, refusal = step_timing.render_report(result, 10, 1.0)
            self.assertIsNotNone(refusal)
            self.assertIn("finished steps missing either envelope timestamp: 2", report)
            self.assertIn(
                "zero-duration timestamp pairs: 0 (control finished model steps=2 > 0",
                report,
            )
            self.assertIn("positive-duration measurements: 0", report)
            self.assertNotIn("throughput (bytes/s)", report)

    def test_fixture_and_import_exclusions_report_unfiltered_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_wal(root, "eligible.wal", known_values())
            write_wal(
                root,
                "fixture.wal",
                [
                    envelope(1, started("fixture", "/private/tmp/fixture")),
                    envelope(2, {"type": "run_finished", "reason": "completed"}),
                ],
            )
            write_wal(
                root,
                "import.wal",
                [
                    envelope(
                        1,
                        started(
                            "import",
                            "/work/import",
                            {
                                "kind": "seeded_from",
                                "origin_kind": "import",
                                "source_ref": "fixture",
                            },
                        ),
                    ),
                    envelope(2, {"type": "run_finished", "reason": "completed"}),
                ],
            )

            result = step_timing.scan_corpus(root)
            report, refusal = step_timing.render_report(result, 10, 1.0)
            self.assertIsNone(refusal)
            self.assertEqual(result.decoded_records, 10)
            self.assertEqual(len(result.eligible_episodes), 1)
            self.assertEqual(result.excluded_fixture_episodes, 1)
            self.assertEqual(result.excluded_import_episodes, 1)
            for line in report.splitlines():
                if line.startswith("excluded ") or line.startswith("eligible episodes"):
                    self.assertIn("unfiltered decoded records=10", line)

    def test_record_type_from_a_newer_build_is_skipped_not_measured(self) -> None:
        # A newer build may add informational record types this script has never seen. One
        # sits inside a step (sharing its step_id) and one follows the terminal: neither may
        # crash the decode, open or close a step, or change the episode's outcome.
        newer = {"type": "model_attempt_finished", "step_id": 1, "attempt": 1, "reason": "error"}
        values = known_values()
        with_newer = values[:2] + [envelope(2_000, newer)] + values[2:] + [envelope(6_000, newer)]
        with tempfile.TemporaryDirectory() as tmp:
            plain_root = pathlib.Path(tmp) / "plain"
            newer_root = pathlib.Path(tmp) / "newer"
            plain_root.mkdir()
            newer_root.mkdir()
            write_wal(plain_root, "known.wal", values)
            write_wal(newer_root, "known.wal", with_newer)

            plain = step_timing.scan_corpus(plain_root)
            result = step_timing.scan_corpus(newer_root)
            strip = lambda steps: [(s.step_id, s.duration_ms, s.content_bytes, s.outcome) for s in steps]
            self.assertEqual(strip(result.steps), strip(plain.steps))
            self.assertEqual(strip(result.steps)[0], ("1", 1_500, 10, "terminal:completed"))
            self.assertEqual(result.unmatched_starts, [])
            self.assertEqual(result.finish_without_start, plain.finish_without_start)
            self.assertEqual(result.unassigned_records, 0)
            # The unfiltered frame count reports every decoded frame, the new ones included.
            self.assertEqual(result.decoded_records, plain.decoded_records + 2)


    def test_resume_started_is_skipped_and_the_sealed_run_reads_interrupted(self) -> None:
        # Real close-out logs: every frame decodes, `resume_started` opens and closes no step,
        # the sealed episode reads terminal:interrupted and the unsealed one stays open.
        expected = {SEALED_CLOSE_OUT: "terminal:interrupted", UNSEALED_CLOSE_OUT: "non_terminal:open"}
        for fixture, outcome in expected.items():
            with tempfile.TemporaryDirectory() as tmp:
                root = pathlib.Path(tmp)
                (root / "wal").mkdir()
                (root / "wal" / "a.wal").write_bytes(fixture.read_bytes())
                result = step_timing.scan_corpus(root)
                self.assertEqual(
                    [(s.step_id, s.outcome) for s in result.steps], [("1", outcome)], fixture.name
                )
                self.assertEqual([e.outcome for e in result.eligible_episodes], [outcome])
                self.assertEqual(result.unmatched_starts, [])
                self.assertEqual(result.unassigned_records, 0)

    def test_gated_frame_with_known_features_is_decoded(self) -> None:
        # The terminal is written gated. Once its feature is known the record must be read like
        # any other: the episode is still completed, and no frame goes uncounted.
        values = known_values()
        terminal = values[-1]["record"]
        with_gate = values[:-1] + [gated([TEST_FEATURE], terminal)]
        with tempfile.TemporaryDirectory() as tmp:
            plain_root = pathlib.Path(tmp) / "plain"
            gated_root = pathlib.Path(tmp) / "gated"
            plain_root.mkdir()
            gated_root.mkdir()
            write_wal(plain_root, "known.wal", values)
            path = write_wal(gated_root, "known.wal", with_gate)
            self.assertIn(
                b'{"requires":["test-feature/v1"],"record":',
                path.read_bytes(),
                "the fixture holds a gated frame",
            )

            plain = step_timing.scan_corpus(plain_root)
            with unittest.mock.patch.object(
                step_timing.wal_gate, "KNOWN_FEATURES", frozenset({TEST_FEATURE})
            ):
                result = step_timing.scan_corpus(gated_root)
            self.assertEqual(result.newer_writer_wals, [])
            self.assertEqual(result.decoded_records, plain.decoded_records)
            self.assertEqual(
                [(s.step_id, s.duration_ms, s.outcome) for s in result.steps],
                [(s.step_id, s.duration_ms, s.outcome) for s in plain.steps],
            )
            self.assertEqual(result.steps[0].outcome, "terminal:completed")

    def test_unknown_feature_is_reported_newer_writer_and_the_scan_continues(self) -> None:
        # The same gated terminal with its feature unknown: that WAL is a newer build's, so it
        # is listed as newer-writer and nothing in it is measured, while the other WAL in the
        # corpus is still measured. Never a refusal, never called corruption.
        values = known_values()
        newer = values[:-1] + [gated(["zz-later/v1", "aa-newer/v1"], values[-1]["record"])]
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_wal(root, "known.wal", values)
            write_wal(root, "newer.wal", newer)

            result = step_timing.scan_corpus(root)
            self.assertEqual(
                [(path.name, missing) for path, missing in result.newer_writer_wals],
                [("newer.wal", ["aa-newer/v1", "zz-later/v1"])],
            )
            self.assertEqual([step.duration_ms for step in result.steps], [1_500, 2_500])
            self.assertEqual(result.decoded_records, len(values))
            report, refusal = step_timing.render_report(result, 10, 1.0)
            self.assertIsNone(refusal)
            self.assertIn(
                "newer-writer WALs (a newer broca wrote them; not measured): 1", report
            )
            self.assertIn("newer-writer newer.wal: requires aa-newer/v1, zz-later/v1", report)
            self.assertNotIn("digest", report)


def suite_names() -> list[str]:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(StepTimingTests)
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

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(StepTimingTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"executed count: {result.testsRun}")
    if result.testsRun <= 0:
        print("refusing vacuous test execution")
        return 1
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
