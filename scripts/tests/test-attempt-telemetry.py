#!/usr/bin/env python3
"""Synthetic WAL tests for the attempt timing probe."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import pathlib
import struct
import sys
import tempfile
import unittest
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
SCRIPT = ROOT / "scripts" / "attempt-telemetry.py"
SPEC = importlib.util.spec_from_file_location("attempt_telemetry", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot import {SCRIPT}")
attempt_telemetry = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = attempt_telemetry
SPEC.loader.exec_module(attempt_telemetry)

EXPECTED_SUITE_ANCHORS = 4
BASE = 1_000_000_000


def frame(seq: int, record: dict[str, Any]) -> bytes:
    payload = json.dumps(record, separators=(",", ":")).encode()
    version = 1
    fence = 7
    digest = hashlib.sha256(bytes((version,)) + struct.pack("<Q", seq)
                            + struct.pack("<Q", fence) + payload).digest()
    return (struct.pack("<I", len(payload)) + bytes((version,))
            + struct.pack("<Q", seq) + struct.pack("<Q", fence) + digest + payload)


def started(run_id: str, provider: str, model: str, project_root: str = "/work/project",
            origin: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "type": "run_started",
        "run_id": run_id,
        "session": {"project_root": project_root},
        "config": {"model": {"provider_module_id": provider, "model_id": model}},
        "origin": origin or {"kind": "fresh"},
    }


def attempt(step: int, ordinal: int, start: int, end: int, outcome: str,
            **progress: Any) -> dict[str, Any]:
    return {
        "type": "model_attempt_finished",
        "step_id": step,
        "attempt": ordinal,
        "sent_at_ms": start,
        "ts_ms": end,
        "outcome": outcome,
        **progress,
    }


def write_wal(root: pathlib.Path, records: list[dict[str, Any]], name: str = "0123456789abcdef.wal"):
    wal_dir = root / "wal"
    wal_dir.mkdir(parents=True, exist_ok=True)
    path = wal_dir / name
    path.write_bytes(b"".join(frame(seq, record) for seq, record in enumerate(records, 1)))
    return path


def scan(root: pathlib.Path, until_ms: int = 2_000_000_000):
    return attempt_telemetry.scan_corpus(root, 0, until_ms)


class AttemptTelemetryTests(unittest.TestCase):
    def test_attempts_join_the_active_run_start_and_exclusions_are_applied(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_wal(root, [
                started("run-a", "provider-a", "model-a"),
                attempt(1, 0, BASE, BASE + 100, "complete", first_content_at_ms=BASE + 10,
                        last_content_at_ms=BASE + 80),
                started("run-b", "provider-b", "model-b"),
                attempt(2, 0, BASE + 200, BASE + 300, "http_error"),
                started("fixture", "ignored", "fixture-model", "/tmp/fixture"),
                attempt(3, 0, BASE + 400, BASE + 500, "complete"),
                started("import", "ignored", "import-model", origin={"kind": "seeded_from"}),
                attempt(4, 0, BASE + 600, BASE + 700, "complete"),
            ])

            result = scan(root)
            self.assertEqual([(item.provider, item.model, item.run_id, item.step_id)
                              for item in result.attempts], [
                                  ("provider-a", "model-a", "run-a", "1"),
                                  ("provider-b", "model-b", "run-b", "2"),
                              ])
            self.assertEqual(result.excluded_attempts, 2)
            self.assertEqual(result.attempts[0].session_addr, "0123456789abcdef")

    def test_absent_content_fields_are_unknown_and_no_content_is_not_a_zero_gap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_wal(root, [
                started("run", "provider", "model"),
                attempt(1, 0, BASE, BASE + 600_000, "content_timeout"),
            ])
            result = scan(root)
            report = attempt_telemetry._report_data(result, 0, 2_000_000_000)
            group = report["provider_models"][0]
            self.assertEqual(group["no_content_attempts"], 1)
            self.assertEqual(group["time_to_first_content"], {
                "count": 0, "unknown": 1, "p50_ms": None, "p90_ms": None, "max_ms": None,
            })
            self.assertEqual(group["trailing_gap"]["count"], 0)
            self.assertEqual(group["trailing_gap"]["unknown"], 1)
            self.assertEqual(report["idle_bound_sensitivity"][0]["true_cuts"], 1)
            text = attempt_telemetry.render_text(report)
            self.assertIn("no-content attempts=1", text)
            self.assertIn("broca doesn't record the longest gap between content events", text)

    def test_duration_content_gaps_and_suffix_byte_math(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_wal(root, [
                started("run", "provider", "model"),
                attempt(1, 0, BASE, BASE + 6_000, "complete",
                        first_content_at_ms=BASE + 1_000, last_content_at_ms=BASE + 4_000,
                        content_bytes=30, events_after_last_content=2,
                        non_content_bytes_after_last_content=70),
            ])
            report = attempt_telemetry._report_data(scan(root), 0, 2_000_000_000)
            group = report["provider_models"][0]
            self.assertEqual(group["total_duration"]["p50_ms"], 6_000)
            self.assertEqual(group["time_to_first_content"]["p50_ms"], 1_000)
            self.assertEqual(group["trailing_gap"]["p50_ms"], 2_000)
            self.assertEqual(group["non_content_suffix"], {
                "attempt_count": 1, "bytes_total": 70, "bytes_measured_total": 70,
                "bytes_unknown_attempts": 0,
                "events_unknown_attempts": 0,
            })
            self.assertEqual(report["longest_observed_initial_or_trailing_gaps"][0]["gap_kind"],
                             "trailing_gap")
            self.assertFalse(report["mid_stream_longest_gap"]["available"])

    def test_each_idle_bound_counts_false_and_true_cuts_from_measured_edges(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_wal(root, [
                started("run", "provider", "model"),
                # Three minutes without initial content, then a normal completion.
                attempt(1, 0, BASE, BASE + 4 * 60_000, "complete",
                        first_content_at_ms=BASE + 3 * 60_000,
                        last_content_at_ms=BASE + 3 * 60_000),
                # Seven-minute trailing gap ends in a provider failure.
                attempt(2, 0, BASE, BASE + 8 * 60_000, "http_error",
                        first_content_at_ms=BASE + 60_000, last_content_at_ms=BASE + 60_000),
                # Eleven-minute trailing gap ends normally.
                attempt(3, 0, BASE, BASE + 12 * 60_000, "complete",
                        first_content_at_ms=BASE + 60_000, last_content_at_ms=BASE + 60_000),
                # No content at all, cancelled after sixteen minutes.
                attempt(4, 0, BASE, BASE + 16 * 60_000, "cancelled"),
            ])

            report = attempt_telemetry._report_data(scan(root), 0, 2_000_000_000)
            counts = [(row["false_cuts"], row["true_cuts"], row["unknown_gap"])
                      for row in report["idle_bound_sensitivity"]]
            self.assertEqual(counts, [(2, 2, 0), (1, 2, 0), (1, 1, 0), (0, 1, 0)])
            self.assertEqual(report["idle_bound_sensitivity"][3]["bound_ms"], 15 * 60_000)


def suite_names() -> list[str]:
    return [
        AttemptTelemetryTests(name).id()
        for name in unittest.defaultTestLoader.getTestCaseNames(AttemptTelemetryTests)
    ]


def main() -> int:
    names = suite_names()
    if len(names) != EXPECTED_SUITE_ANCHORS:
        print(f"suite anchor mismatch: expected {EXPECTED_SUITE_ANCHORS}, listed {len(names)}")
        return 1
    print(f"suite anchors before execution: {len(names)}")
    for name in names:
        print(f"  {name}")
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(AttemptTelemetryTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"executed count: {result.testsRun}")
    if result.testsRun <= 0:
        print("refusing vacuous test execution")
        return 1
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
