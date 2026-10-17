#!/usr/bin/env python3
"""Writer-backed logical-population regressions for the diagnostic instruments."""
import contextlib
import importlib.util
import io
import hashlib
import json
import pathlib
import struct
import subprocess
import sys
import tempfile
import time
import unittest

from lib.archive_read import ArchiveRefusal, in_window, logical_wals, read_records, _index, _trailer

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
spec = importlib.util.spec_from_file_location("timing_fixtures", SCRIPTS / "test-step-timing.py")
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)


class ArchiveReadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["cargo", "build", "--locked", "-p", "broca-wal", "--example", "archive-fixture"],
                       cwd=ROOT, check=True)
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version=1"], cwd=ROOT))
        cls.writer = pathlib.Path(metadata["target_directory"]) / "debug/examples/archive-fixture"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        (self.root / "wal").mkdir()
        (self.root / "wal-archive").mkdir()

    def wal(self, addr, payload):
        path = self.root / "wal" / (addr + ".wal")
        path.write_bytes(payload)
        return path

    def seal(self, stamp, *paths):
        path = self.root / "wal-archive" / f"fold-{stamp}.ark"
        subprocess.run([str(self.writer), str(path), *map(str, paths)], check=True)
        return path

    def cli(self, script, *args):
        return subprocess.run([sys.executable, str(SCRIPTS / script), "--state-root", str(self.root), *args],
                              capture_output=True, text=True, timeout=30)

    def records(self):
        values = fixtures.known_values()
        for value in values:
            if value["record"]["type"] == "model_step_finished":
                value["record"]["usage"] = {"output_tokens": 40}
        return values

    def test_durable_archive_and_live_override_without_ordinary_trees(self):
        live = fixtures.write_wal(self.root, "aaaaaaaaaaaaaaaa.wal", self.records())
        live_bytes = live.read_bytes()
        container = self.seal(10, live)
        durable_archive = self.root / "wal-archive-durable"
        durable_archive.mkdir()
        container.rename(durable_archive / container.name)
        live.unlink()
        (self.root / "wal").rmdir()
        (self.root / "wal-archive").rmdir()
        selected = logical_wals(self.root / "wal")
        self.assertEqual([wal.name for wal in selected], [live.name])
        self.assertTrue(read_records(selected[0]))
        durable = self.root / "wal-durable"
        durable.mkdir()
        current = durable / live.name
        current.write_bytes(live_bytes)
        self.assertEqual(logical_wals(self.root / "wal"), [current])

    def assert_archived_sidecar_skip(self, suffix):
        hidden = fixtures.write_wal(self.root, "aaaaaaaaaaaaaaaa.wal", self.records())
        visible = fixtures.write_wal(self.root, "bbbbbbbbbbbbbbbb.wal", self.records())
        self.seal(10, hidden, visible)
        hidden.unlink()
        visible.unlink()
        marker = hidden.with_suffix(suffix)
        marker.write_text("{}")
        self.assertEqual([p.name for p in logical_wals(self.root / "wal")], [visible.name])
        for script, args in (("wal-scan.py", ("--control", "run_started", "--pattern", "run_started")),
                             ("step-timing.py", ()),
                             ("find-empty-completions.py", ("--hours", "1000000")),
                             ("wal-moved-roots.py", ())):
            with self.subTest(reader=script):
                result = self.cli(script, *args)
                self.assertIn(result.returncode, (0, 1), result.stdout + result.stderr)
                self.assertIn(str(marker), result.stderr)
                self.assertNotIn(hidden.name, result.stdout)
        for script, args in (("wal-addressability.py", ("--key", "/work", "runner", "absent")),
                             ("rekey-harness.py", ("--from", "runner", "--to", "broca"))):
            with self.subTest(reader=script):
                result = self.cli(script, *args)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn(str(marker), result.stderr)
        self.assertEqual(marker.read_text(), "{}")

    def test_moving_sidecar_hides_archived_only_address(self):
        self.assert_archived_sidecar_skip(".moving")

    def test_moved_sidecar_hides_archived_only_address(self):
        self.assert_archived_sidecar_skip(".moved")

    def test_v2_writer_index_reads_all_members_without_fallback(self):
        a = self.wal("aaaaaaaaaaaaaaaa", b"a")
        b = self.wal("bbbbbbbbbbbbbbbb", b"b")
        container = self.seal(10, b, a)
        with container.open("rb") as source:
            end, length = _trailer(source)
            entries, start = _index(source, end, length)
        self.assertEqual(len(entries), 2)
        self.assertEqual(start, 130)
        damaged = bytearray(container.read_bytes())
        damaged[-72] ^= 1
        container.write_bytes(damaged)
        with self.assertRaisesRegex(ArchiveRefusal, "index digest mismatch"):
            with container.open("rb") as source:
                _index(source, end, length)
        damaged[-72] ^= 1
        container.write_bytes(damaged)
        a.unlink()
        b.unlink()
        self.assertEqual([m.read_bytes() for m in logical_wals(self.root / "wal")], [b"a", b"b"])

    def test_flipped_first_header_byte_refuses_even_with_unusable_trailer_length(self):
        live = self.wal("aaaaaaaaaaaaaaaa", b"payload")
        container = self.seal(10, live)
        live.unlink()
        damaged = bytearray(container.read_bytes())
        damaged[0] ^= 1
        damaged[-8:-4] = struct.pack("<I", 0)
        container.write_bytes(damaged)
        with self.assertRaises(ArchiveRefusal):
            logical_wals(self.root / "wal")

    def test_v1_index_still_reads(self):
        live = self.wal("aaaaaaaaaaaaaaaa", b"payload")
        container = self.seal(10, live)
        raw = container.read_bytes()
        length = struct.unpack("<I", raw[-8:-4])[0]
        v1 = raw[:-8 - 64]
        container.write_bytes(v1 + struct.pack("<I", length - 64) + b"WALK")
        live.unlink()
        self.assertEqual(logical_wals(self.root / "wal")[0].read_bytes(), b"payload")

    def test_four_step_population_conserved_across_fold(self):
        a = fixtures.write_wal(self.root, "aaaaaaaaaaaaaaaa.wal", self.records())
        b = fixtures.write_wal(self.root, "bbbbbbbbbbbbbbbb.wal", self.records())
        before = fixtures.step_timing.scan_corpus(self.root)
        self.assertEqual((before.wal_files, len(before.steps)), (2, 4))
        self.seal(10, b)
        b.unlink()
        after = fixtures.step_timing.scan_corpus(self.root)
        self.assertEqual((after.wal_files, len(after.steps)), (2, 4))
        self.assertEqual([s.duration_ms for s in after.steps], [s.duration_ms for s in before.steps])
        self.assertTrue(a.exists())

    def test_all_triage_populations_conserved_across_fold(self):
        a = fixtures.write_wal(self.root, "aaaaaaaaaaaaaaaa.wal", self.records())
        b = fixtures.write_wal(self.root, "bbbbbbbbbbbbbbbb.wal", self.records())
        commands = [("wal-scan.py", "--control", "run_started", "--pattern", "model_step_finished"),
                    ("wal-moved-roots.py",),
                    ("find-empty-completions.py", "--hours", "1000000")]
        before = [self.cli(*command) for command in commands]
        self.seal(10, b)
        b.unlink()
        after = [self.cli(*command) for command in commands]
        for command, old, new in zip(commands, before, after):
            with self.subTest(instrument=command[0]):
                self.assertEqual(old.returncode, 0, old.stderr + old.stdout)
                self.assertEqual(new.returncode, 0, new.stderr + new.stdout)
                if command[0] == "find-empty-completions.py":
                    self.assertIn("scanned 2 WAL(s)", old.stdout)
                    self.assertIn("scanned 2 WAL(s)", new.stdout)
                    self.assertIn("fold-10.ark", new.stderr)
                else:
                    self.assertEqual(new.stdout, old.stdout)
        self.assertTrue(a.exists())

    def test_live_wins_without_double_count(self):
        prefix = fixtures.frame(1, {"type": "run_started"})
        appended = prefix + fixtures.frame(2, {"type": "run_finished"})
        live = self.wal("aaaaaaaaaaaaaaaa", prefix)
        self.seal(10, live)
        live.write_bytes(appended)
        population = logical_wals(self.root / "wal")
        self.assertEqual(len(population), 1)
        self.assertEqual(population[0].read_bytes(), appended)

    def test_highest_numeric_stamp_wins(self):
        live = self.wal("aaaaaaaaaaaaaaaa", b"old")
        self.seal(9, live)
        live.write_bytes(b"new")
        self.seal(10, live)
        live.unlink()
        population = logical_wals(self.root / "wal")
        self.assertEqual(len(population), 1)
        self.assertEqual(population[0].read_bytes(), b"new")

    def test_unsealed_refused_by_reader_and_all_instruments(self):
        live = fixtures.write_wal(self.root, "aaaaaaaaaaaaaaaa.wal", self.records())
        container = self.seal(10, live)
        container.write_bytes(container.read_bytes()[:-8])
        with self.assertRaisesRegex(ArchiveRefusal, "unsealed"):
            logical_wals(self.root / "wal")
        self.assert_instruments_refuse("unsealed")

    def test_digest_refused_by_reader_and_all_instruments(self):
        live = fixtures.write_wal(self.root, "aaaaaaaaaaaaaaaa.wal", self.records())
        container = self.seal(10, live)
        live.unlink()
        damaged = bytearray(container.read_bytes())
        damaged[64] ^= 1
        container.write_bytes(damaged)
        with self.assertRaisesRegex(ArchiveRefusal, "digest mismatch"):
            logical_wals(self.root / "wal")[0].read_bytes()
        self.assert_instruments_refuse("digest mismatch")

    def assert_instruments_refuse(self, message):
        for script, args in [("step-timing.py", []), ("wal-moved-roots.py", []),
                             ("wal-scan.py", ["--control", "run_started"]),
                             ("find-empty-completions.py", ["--hours", "1000000"])]:
            with self.subTest(instrument=script):
                result = self.cli(script, *args)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("REFUSING", result.stderr)
                self.assertIn(message, result.stderr)

    def test_sealed_index_recovery_and_header_validation(self):
        live = self.wal("aaaaaaaaaaaaaaaa", b"verified payload")
        container = self.seal(10, live)
        live.unlink()
        original = container.read_bytes()
        # A damaged trailer length can be recovered when the intact index
        # proves that the header walk reached every member.
        container.write_bytes(original[:-8] + struct.pack("<I", 0) + b"WALK")
        self.assertEqual(logical_wals(self.root / "wal")[0].read_bytes(), b"verified payload")
        # Without an index boundary, diagnostics cannot prove completeness.
        container.write_bytes(original[:64 + len(b"verified payload")] + struct.pack("<I", 0) + b"WALK")
        with self.assertRaises(ArchiveRefusal):
            logical_wals(self.root / "wal")
        damaged = bytearray(original)
        damaged[4] = 2
        container.write_bytes(damaged)
        with self.assertRaisesRegex(ArchiveRefusal, "invalid archive member header"):
            logical_wals(self.root / "wal")

    def test_time_window_uses_fold_time_not_assumed_retention(self):
        # wal-scan reads framed records, not arbitrary archive-member bytes.
        live = self.wal("aaaaaaaaaaaaaaaa", fixtures.frame(1, {"type": "run_paused", "run_id": "payload", "reason": "restart"}))
        self.seal(int(time.time() * 1000), live)
        live.unlink()
        member = logical_wals(self.root / "wal")[0]
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertFalse(in_window(member, member.activity_upper_bound + 1, set()))
        self.assertEqual(output.getvalue(), "")
        with contextlib.redirect_stderr(output):
            self.assertTrue(in_window(member, member.activity_upper_bound - 1, set()))
        self.assertIn(member.container.name, output.getvalue())
        self.assertIn("last activity <=", output.getvalue())
        recent = self.cli("wal-scan.py", "--control", "payload", "--newer-than", "24h")
        self.assertEqual(recent.returncode, 0)
        self.assertIn("historical idle retention unavailable", recent.stderr)
        self.assertAlmostEqual(member.activity_upper_bound, int(member.container.stem.split("-")[1]) / 1000)

    def compaction_wal(self):
        records = [json.loads(line) for line in
                   (ROOT / "crates/broca-wal/tests/fixtures/compaction_records.jsonl").read_text().splitlines()]
        blob = fixtures.lineage_frame()
        for seq, record in enumerate(records, 1):
            feature = "compaction-wait/v1" if record["type"] == "compaction_wait" else "compaction/v1"
            blob += fixtures.frame(seq, fixtures.gated([feature], record), version=3)
        return records, blob

    def test_compaction_records_decode_and_scan_live_and_archived(self):
        records, blob = self.compaction_wal()
        live = self.wal("aaaaaaaaaaaaaaaa", blob)
        for archived in (False, True):
            with self.subTest(archived=archived):
                if archived:
                    self.seal(10, live)
                    live.unlink()
                population = logical_wals(self.root / "wal")
                self.assertEqual(len(population), 1)
                frames = read_records(population[0])
                self.assertEqual([frame.record for frame in frames], records)
                self.assertEqual([frame.seq for frame in frames], list(range(1, 8)))
                self.assertEqual({frame.fence for frame in frames}, {7})
                self.assertEqual({frame.record["type"] for frame in frames}, {
                    "compaction_request", "compaction_message", "compaction_answer", "compaction_wait"})
                for frame, record in zip(frames, records):
                    self.assertEqual(frame.payload, json.dumps(record, separators=(",", ":")).encode())
                # A known applied-view record is the positive control even after unlink.
                result = self.cli("wal-scan.py", "--control", '"type":"compaction_message"',
                                  "--pattern", '"type":"compaction_wait"')
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                self.assertIn("matches           2 across 1 files", result.stdout)
                self.assertIn("matches           1 across 1 files", result.stdout)

    def test_scanner_excludes_envelopes_and_preserves_record_bytes(self):
        # Deliberately put a quoted 'record' and nested record before the real key.
        # The record's whitespace, Unicode and exponent spelling must not be normalized.
        record = b'{"type": "compaction_request", "provider": "caf\xc3\xa9", "request_id":"cr-1", "number":1e1}'
        envelope = (b' \n{"note":"record", "nested":{"record":{"type":"decoy"}}, '
                    b'"requires":["compaction/v1"], "record" : ' + record + b'}')
        header = struct.pack("<BQQ", 3, 1, 7)
        blob = struct.pack("<I", len(envelope)) + header + hashlib.sha256(header + envelope).digest() + envelope
        live = self.wal("aaaaaaaaaaaaaaaa", blob)
        self.assertEqual(read_records(live)[0].payload, record)
        result = self.cli("wal-scan.py", "--control", '"type": "compaction_request"',
                          "--pattern", "compaction/v1", "--pattern", "decoy", "--expect-absent")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(result.stdout.count("matches           0 across 0 files"), 2)
        result = self.cli("wal-scan.py", "--control", "compaction/v1")
        self.assertEqual(result.returncode, 2, result.stderr + result.stdout)
        self.assertIn("control 'compaction/v1' matched nothing", result.stderr)
        # A timestamp-wrapped legacy fixture excludes that wrapper as well.
        live.write_bytes(fixtures.frame(1, {"ts_ms": 99999, "record": json.loads(record)}))
        result = self.cli("wal-scan.py", "--control", "compaction_request",
                          "--pattern", "ts_ms", "--expect-absent")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)

    def assert_framing_refused_live_and_archived(self, blob, message):
        live = self.wal("aaaaaaaaaaaaaaaa", blob)
        for archived in (False, True):
            with self.subTest(archived=archived):
                if archived:
                    self.seal(10 + len(list((self.root / "wal-archive").glob("*.ark"))), live)
                    live.unlink()
                with self.assertRaisesRegex(ArchiveRefusal, message):
                    read_records(logical_wals(self.root / "wal")[0])
                result = self.cli("wal-scan.py", "--control", "compaction_request", "--expect-absent")
                self.assertEqual(result.returncode, 2, result.stderr + result.stdout)
                self.assertIn("REFUSING", result.stderr)
                self.assertRegex(result.stderr, message)
                self.assertNotIn("files read", result.stdout, "no verdict over a partially decoded corpus")

    def test_unknown_feature_refused_live_and_archived_by_name(self):
        _, blob = self.compaction_wal()
        # The known records satisfy the control; the final unknown record must still refuse.
        blob += fixtures.frame(8, fixtures.gated(["future-compaction/v99"], {"type": "compaction_request"}), version=3)
        self.assert_framing_refused_live_and_archived(blob, "newer-writer, missing features: future-compaction/v99")

    def test_frame_digest_mismatch_refused_live_and_archived(self):
        _, blob = self.compaction_wal()
        damaged = bytearray(blob)
        # Change a string byte while retaining valid JSON: without the checksum,
        # this frame would otherwise be accepted, not rejected by the JSON parser.
        damaged[blob.rindex(b"restart during compaction wait")] ^= 1
        # The archive writer seals these damaged bytes with a correct member digest:
        # this tests the WAL frame checksum, not just the container's checksum.
        self.assert_framing_refused_live_and_archived(damaged, "digest mismatch at sequence 7")

    def test_torn_tail_refused_live_and_archived(self):
        _, blob = self.compaction_wal()
        self.assert_framing_refused_live_and_archived(blob[:-1], "torn tail \\(incomplete payload\\)")
        self.assert_framing_refused_live_and_archived(blob + b"\x01", "torn tail \\(incomplete header\\)")

    def test_frame_metadata_and_malformed_records_refused(self):
        record = {"type": "compaction_request", "provider": "magic-context", "request_id": "cr-1"}
        for blob, message in [
            (fixtures.frame(1, record, version=4), "unsupported WAL framing version 4"),
            (struct.pack("<I", 64 * 1024 * 1024 + 1) + fixtures.frame(1, record)[4:], "exceeds .*byte cap"),
            (fixtures.frame(2, record), "expected sequence 1, found 2"),
            (fixtures.frame(1, record, version=2), "invalid lineage frame"),
            (fixtures.frame(1, {"type": 7}), "no tagged WAL record"),
            (fixtures.frame(1, {"requires": "compaction/v1", "record": record}, version=3), "no list of feature names"),
        ]:
            with self.subTest(message=message):
                live = self.wal("aaaaaaaaaaaaaaaa", blob)
                with self.assertRaisesRegex(ArchiveRefusal, message):
                    read_records(live)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ArchiveReadTests)
    print(f"archive suite anchors: {suite.countTestCases()}", flush=True)
    if suite.countTestCases() != 20:
        raise SystemExit("archive suite anchor count changed")
    unittest.main(verbosity=2)
