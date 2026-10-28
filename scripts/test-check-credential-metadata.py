#!/usr/bin/env python3
"""The metadata deploy gate compares real auth records and never prints vault material."""
from __future__ import annotations

import contextlib
import copy
import errno
import hashlib
import importlib.util
import io
import json
import multiprocessing
import pathlib
import os
import re
import subprocess
import struct
import sys
import tempfile
import time
import unittest
from unittest import mock
from concurrent.futures import ProcessPoolExecutor
from lib import archive_read

SCRIPTS = pathlib.Path(__file__).resolve().parent
ROOT = SCRIPTS.parent
SPEC = importlib.util.spec_from_file_location("account_metadata", SCRIPTS / "check-credential-metadata.py")
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)
SCAN = GATE.scan


RECENT = object()


class InlinePool:
    """Keep race injection deterministic; a separate CLI test runs the real pool."""
    def __init__(self, **kwargs):
        self.options = kwargs

    def map(self, function, tasks, **kwargs):
        return map(function, tasks)

    def shutdown(self, **kwargs):
        pass


class MetadataDeployGate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Reuse WalRecord's exact-byte pins as templates, then round-trip every
        # fixture through FileWal so Python cannot invent fields broca never writes.
        pins = (ROOT / "crates/broca-wal/src/record/pinned_bytes_tests.rs").read_text()
        records = [json.loads(raw) for raw in re.findall(
            r'^[ \t]*r#"(\{"type":[^\n]*\})"#,?[ \t]*$', pins, re.MULTILINE)]
        cls.start_template = next(record for record in records if record["type"] == "run_started"
                                  and set(record["config"].get("auth_selection", {})) == {"credential_id"})
        cls.vault_request_template = next(record["config"]["auth_selection"]["vault_request"]
                                          for record in records if record["type"] == "run_started"
                                          and "vault_request" in record["config"].get("auth_selection", {}))
        cls.refreeze_template = next(record for record in records if record["type"] == "run_auth_refrozen"
                                     and "ts_ms" not in record)
        cls.activity_template = next(record for record in records if record["type"] == "model_step_finished")
        cls.attempt_template = next(record for record in records if record["type"] == "model_attempt_finished")
        cls.finish_template = next(record for record in records if record["type"] == "run_finished")
        cls.step_template = next(record for record in records if record["type"] == "step_started")
        subprocess.run(["cargo", "build", "--locked", "-p", "broca-wal", "--example", "credential-metadata-fixture"],
                       cwd=ROOT, check=True)
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version=1"], cwd=ROOT))
        cls.writer = pathlib.Path(metadata["target_directory"]) / "debug/examples/credential-metadata-fixture"

    def setUp(self):
        pool = mock.patch.object(GATE, "ProcessPoolExecutor", InlinePool)
        pool.start()
        self.addCleanup(pool.stop)
        scratch = SCRIPTS.parent / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        (self.root / "wal").mkdir()
        self.now = int(time.time() * 1000)

    def write_records(self, records, name="session.wal", tree="wal"):
        path = self.root / tree / name
        path.parent.mkdir(exist_ok=True)
        path.unlink(missing_ok=True)
        subprocess.run([str(self.writer), str(path)], input=json.dumps(records), text=True, check=True)
        return path

    def start(self, credential, timestamp=RECENT, resolved=None, method: str | None = "oauth"):
        record = copy.deepcopy(self.start_template)
        if timestamp is not None:
            record["written_at_ms"] = self.now if timestamp is RECENT else timestamp
        selection = record["config"]["auth_selection"]
        selection["credential_id"] = credential
        if method is not None:
            selection["auth_method"] = method
        if resolved:
            selection["resolved_credential_id"] = resolved
        return record

    def timed(self, template, field="ts_ms", timestamp=RECENT):
        record = copy.deepcopy(template)
        record[field] = self.now if timestamp is RECENT else timestamp
        return record

    def run_gate(self, rows):
        return self.run_listing({"v": 1, "credentials": rows})

    def run_listing(self, document, raw=False):
        output = io.StringIO()
        listing = self.root / "listing.json"
        listing.write_text(document if raw else json.dumps(document))
        with mock.patch.object(sys, "argv", ["check-credential-metadata.py", "--state-root", str(self.root),
                                            "--vault-listing", str(listing)]), \
              mock.patch.object(GATE.time, "time", return_value=self.now / 1000), \
              contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            status = GATE.main()
        text = output.getvalue()
        self.assertNotIn("handle-DO-NOT-LOG", text)
        self.assertNotIn("token-DO-NOT-LOG", text)
        return status, text

    def test_pass_ignores_unused_unmapped_credentials(self):
        path = self.write_records([self.start("broca-alias", resolved="opaque-claude")])
        record = SCAN.read_records(path)[0].record
        self.assertEqual(record["written_at_ms"], self.now)
        self.assertNotIn("ts_ms", record)
        status, output = self.run_gate([
            {"id": "opaque-claude", "provider_ids": ["anthropic"], "auth_method": "oauth",
             "handle": "handle-DO-NOT-LOG", "token": "token-DO-NOT-LOG"},
            {"id": "apikey:cline-pass"}, {"id": "apikey:muse"}, {"id": "oauth:cursor"},
        ])
        self.assertEqual(status, 0, output)
        self.assertIn("PASS; 1 recently used credentials", output)
        self.assertRegex(output, r"read 1 logs in \d+\.\d{2}s")

    def cline_pass_start(self):
        # Preserve the production module-run shape using the pinned RunStarted
        # and VaultRequest templates, then let FileWal serialize the actual record.
        record = self.start("apikey:cline-pass", timestamp=None, resolved="apikey:cline-pass", method=None)
        record["config"]["model"]["provider_module_id"] = "cline-pass"
        record["config"]["auth_selection"]["vault_request"] = copy.deepcopy(self.vault_request_template)
        return record

    def test_cline_pass_module_runs_are_skipped_and_named_without_metadata(self):
        for tree in ("wal", "wal-ephemeral"):
            path = self.write_records([self.cline_pass_start(), self.timed(self.attempt_template)],
                                      f"cline-{tree}.wal", tree=tree)
            record = SCAN.read_records(path)[0].record
            self.assertEqual(record["config"]["model"]["provider_module_id"], "cline-pass")
            self.assertEqual(set(record["config"]["auth_selection"]),
                             {"credential_id", "resolved_credential_id", "vault_request"})
            self.assertNotIn("written_at_ms", record)
        self.write_records([self.start("opaque-claude")])
        status, output = self.run_gate([
            {"id": "opaque-claude", "provider_ids": ["anthropic"], "auth_method": "oauth"},
            {"id": "apikey:cline-pass", "handle": "handle-DO-NOT-LOG", "token": "token-DO-NOT-LOG"},
        ])
        self.assertEqual(status, 0, output)
        self.assertIn("PASS; 1 recently used credentials selected by the chooser", output)
        self.assertEqual(output.split("skipped as not chooser-selected: ", 1)[1].strip(), "apikey:cline-pass")

    def test_only_non_chooser_runs_are_a_comparable_skip_not_empty_history(self):
        self.write_records([self.cline_pass_start(), self.timed(self.attempt_template)])
        status, output = self.run_gate([{"id": "apikey:cline-pass"}])
        self.assertEqual(status, 0, output)
        self.assertIn("PASS; 0 recently used credentials selected by the chooser", output)
        self.assertIn("skipped as not chooser-selected: apikey:cline-pass", output)

    def test_legacy_subscription_selections_still_require_metadata(self):
        for method in ("oauth", "chatgpt"):
            with self.subTest(method=method):
                credential = f"{method}:new-provider:legacy"
                self.write_records([self.start(credential, timestamp=None, method=None),
                                    self.timed(self.attempt_template)])
                status, output = self.run_gate([{"id": credential}])
                self.assertEqual(status, 1, output)
                self.assertIn(f"{credential}: missing provider_ids and auth_method", output)

    def test_method_predicate_honors_explicit_metadata_before_legacy_key_spelling(self):
        self.assertTrue(GATE.chooser_selects_accounts({"credential_id": "apikey:cline-pass", "auth_method": "oauth"}))
        self.assertFalse(GATE.chooser_selects_accounts({"credential_id": "oauth:anthropic", "auth_method": "apikey"}))
        self.assertFalse(GATE.chooser_selects_accounts({"credential_id": "antigravity:new-provider"}))
        with self.assertRaisesRegex(GATE.CannotCompare, "unknown authentication method"):
            GATE.chooser_selects_accounts({"credential_id": "opaque", "auth_method": "future-method"})
        with self.assertRaisesRegex(GATE.CannotCompare, "no recognizable authentication method"):
            GATE.chooser_selects_accounts({"credential_id": "opaque"})

    def test_refreeze_reclassifies_the_active_selection(self):
        refreeze = self.timed(self.refreeze_template)
        refreeze["auth_selection"]["credential_id"] = "apikey:cline-pass"
        self.write_records([self.start("oauth:anthropic:old", self.now - GATE.WEEK_MS - 1, method=None),
                            refreeze])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), (set(), {"apikey:cline-pass"}, 1, 0, 0))

    def test_a_credential_seen_in_scope_is_not_reported_as_skipped(self):
        self.write_records([self.start("apikey:provider-module", resolved="shared", method=None)], "module.wal")
        self.write_records([self.start("opaque-subscription", resolved="shared")])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"shared"}, set(), 2, 0, 0))

    def test_refuse_names_each_used_credential_and_missing_field(self):
        self.write_records([self.start("oauth:anthropic:private"), self.start("no-method"),
                            self.start("absent"), self.start("both")])
        status, output = self.run_gate([
            {"id": "oauth:anthropic:private", "auth_method": "oauth"},
            {"id": "no-method", "provider_ids": ["new-provider"]}, {"id": "both"},
        ])
        self.assertEqual(status, 1, output)
        self.assertIn("oauth:anthropic:private: missing provider_ids", output)
        self.assertIn("no-method: missing auth_method", output)
        self.assertIn("both: missing provider_ids and auth_method", output)
        self.assertIn("absent: absent from live vault listing", output)

    def test_empty_history_cannot_compare(self):
        status, output = self.run_gate([{"id": "unused"}])
        self.assertEqual(status, 2, output)
        self.assertIn("cannot compare", output)
        self.assertIn("no credentials recorded in broca runs in the last 7 days", output)
        self.assertNotIn("PASS", output)

    def test_archive_age_or_longer_windows_cannot_be_certified_live_only(self):
        with self.assertRaisesRegex(GATE.CannotCompare, "shorter than the 14-day archive idle period"):
            GATE.recent_credentials(self.root, self.now, days=14)

    def test_empty_live_listing_cannot_compare(self):
        self.write_records([self.start("recent")])
        status, output = self.run_gate([])
        self.assertEqual(status, 2, output)
        self.assertIn("cannot compare", output)
        self.assertIn("live vault listing is empty", output)

    def test_unknown_listing_version_cannot_compare(self):
        self.write_records([self.start("recent")])
        status, output = self.run_listing({"v": 2, "credentials": [{"id": "recent"}]})
        self.assertEqual(status, 2, output)
        self.assertIn("cannot compare", output)
        self.assertIn("vault listing has an unknown version", output)

    def test_missing_credentials_array_cannot_compare(self):
        self.write_records([self.start("recent")])
        status, output = self.run_listing({"v": 1})
        self.assertEqual(status, 2, output)
        self.assertIn("cannot compare", output)
        self.assertIn("vault listing has no credentials array", output)

    def test_recent_activity_and_refreeze_count_but_old_runs_do_not(self):
        refreeze = self.timed(self.refreeze_template)
        refreeze["auth_selection"]["credential_id"] = "oauth:anthropic:refrozen"
        refreeze["auth_selection"]["resolved_credential_id"] = "refrozen"
        path = self.write_records([
            self.start("old-unused", self.now - GATE.WEEK_MS - 1),
            self.timed(self.finish_template, timestamp=self.now - GATE.WEEK_MS - 1),
            self.start("oauth:anthropic:long-running", timestamp=None, resolved="long-running", method=None),
            copy.deepcopy(self.step_template),
            self.timed(self.activity_template, field="written_at_ms"),
            refreeze,
        ])
        records = [frame.record for frame in SCAN.read_records(path)]
        self.assertNotIn("written_at_ms", records[2])
        self.assertNotIn("ts_ms", records[2])
        self.assertNotIn("ts_ms", records[3])
        self.assertEqual(records[4]["written_at_ms"], self.now)
        self.assertEqual(records[5]["ts_ms"], self.now)
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"long-running", "refrozen"}, set(), 1, 0, 1))

    def test_recent_ts_ms_activity_counts_a_legacy_untimed_selection(self):
        self.write_records([self.start("oauth:anthropic:legacy", timestamp=None, resolved="legacy", method=None),
                            self.timed(self.attempt_template)])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"legacy"}, set(), 1, 0, 1))

    def test_untimed_refreeze_waits_for_later_activity_without_refusing(self):
        refreeze = copy.deepcopy(self.refreeze_template)
        refreeze["auth_selection"]["credential_id"] = "oauth:anthropic:refrozen-legacy"
        self.write_records([self.start("oauth:anthropic:legacy", timestamp=None, method=None), refreeze,
                            self.timed(self.attempt_template)])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"oauth:anthropic:refrozen-legacy"}, set(), 1, 0, 1))

    def test_untimed_records_alone_are_not_evidence_of_recency(self):
        self.write_records([self.start("oauth:anthropic:untimed", timestamp=None, method=None),
                            copy.deepcopy(self.step_template)])
        status, output = self.run_gate([{"id": "untimed"}])
        self.assertEqual(status, 2, output)
        self.assertIn("no credentials recorded", output)
        self.assertNotIn("no recorded timestamp", output)

    def test_recent_terminal_activity_counts_and_ends_the_selection(self):
        self.write_records([self.start("oauth:anthropic:ended", timestamp=None, resolved="ended", method=None),
                            self.timed(self.finish_template),
                            self.timed(self.attempt_template)])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"ended"}, set(), 1, 0, 1))

    def test_old_live_logs_and_archives_are_not_read(self):
        old = self.write_records([self.start("old-unused", self.now - GATE.WEEK_MS - 1)], "old.wal")
        with old.open("ab") as output:
            output.write(b"old unread corruption")
        old_seconds = (self.now - GATE.WEEK_MS - 1000) / 1000
        os.utime(old, (old_seconds, old_seconds))
        recent = self.write_records([self.start("recent")])
        archive = self.root / "wal-archive"
        archive.mkdir()
        (archive / "fold-1.ark").write_bytes(b"must not be opened")
        with mock.patch.object(SCAN, "read_records_reverse", wraps=SCAN.read_records_reverse) as read:
            status, output = self.run_gate([{"id": "recent", "provider_ids": ["anthropic"], "auth_method": "oauth"}])
        self.assertEqual(status, 0, output)
        read.assert_called_once_with(recent)
        self.assertIn("read 1 logs", output)

    def test_population_includes_moved_and_tombstoned_logs_in_all_live_trees(self):
        paths = [self.write_records([self.start(tree)], f"{tree}.wal", tree=tree)
                 for tree in ("wal", "wal-ephemeral", "wal-durable")]
        for path in paths:
            path.with_suffix(".expired").write_text("not inspected")
            path.with_suffix(".moving").write_text("not inspected")
        self.assertEqual(GATE.recent_credentials(self.root, self.now),
                         ({"wal", "wal-ephemeral", "wal-durable"}, set(), 3, 0, 0))

    def test_pass_reports_log_count_and_elapsed_time(self):
        self.write_records([self.start("recent")], "first.wal")
        self.write_records([self.start("recent")], "second.wal")
        with mock.patch.object(GATE.time, "monotonic", side_effect=[100, 100.25]):
            status, output = self.run_gate([{"id": "recent", "provider_ids": ["anthropic"], "auth_method": "oauth"}])
        self.assertEqual(status, 0, output)
        self.assertIn("read 2 logs in 0.25s", output)

    def test_reverse_and_full_forward_scans_agree_over_the_fixture_corpus(self):
        old = self.now - GATE.WEEK_MS - 1
        cutoff = self.now - GATE.WEEK_MS
        refreeze = self.timed(self.refreeze_template)
        refreeze["auth_selection"]["credential_id"] = "oauth:anthropic:rotated"
        queued = self.start("queued", old)
        queued["submission_id"] = "sub-run-7"
        cases = [
            ("refreeze", [self.start("old-open", old), self.timed(self.attempt_template), refreeze,
                          self.timed(self.activity_template, "written_at_ms")],
             {"old-open", "oauth:anthropic:rotated"}, set(), 0),
            ("activity-only", [self.start("activity-only", old), self.timed(self.attempt_template)],
             {"activity-only"}, set(), 0),
            ("untimed", [self.start("oauth:anthropic:legacy", timestamp=None, method=None),
                         self.timed(self.attempt_template)], {"oauth:anthropic:legacy"}, set(), 1),
            ("queued", [self.start("before-queued"), self.timed(self.finish_template), queued,
                        self.timed(self.attempt_template)], {"before-queued", "queued"}, set(), 1),
            ("terminal", [self.start("ended", old), self.timed(self.finish_template),
                          self.timed(self.attempt_template)], {"ended"}, set(), 0),
            ("module", [self.cline_pass_start(), self.timed(self.attempt_template)],
             set(), {"apikey:cline-pass"}, 1),
            ("old", [self.start("not-recent", old), self.timed(self.attempt_template, timestamp=old),
                     self.timed(self.finish_template, timestamp=old)], set(), set(), 0),
        ]
        for name, records, expected, skipped, fallbacks in cases:
            with self.subTest(name=name):
                path = self.write_records(records, f"{name}.wal")
                forward = SCAN.scan_forward(path, cutoff)
                result = SCAN.scan_wal((path, cutoff))
                self.assertIsNone(result.error)
                self.assertEqual(forward, (expected, skipped))
                self.assertEqual((result.credentials, result.skipped), forward)
                self.assertEqual(result.fallbacks, fallbacks)

    def test_reverse_scan_does_not_read_or_decode_the_old_prefix(self):
        old = self.now - GATE.WEEK_MS - 1
        bulky = self.timed(self.activity_template, "written_at_ms", old)
        bulky["assistant_message"]["content"] = [{"type": "text", "text": "old history " * 1000}]
        records = [self.start("prefix", old), *[copy.deepcopy(bulky) for _ in range(100)],
                   self.start("governing", old), self.timed(self.attempt_template)]
        path = self.write_records(records)
        cutoff = self.now - GATE.WEEK_MS
        self.assertEqual(SCAN.scan_forward(path, cutoff), ({"governing"}, set()))
        damaged = bytearray(path.read_bytes())
        damaged[damaged.index(b"prefix")] ^= 1
        path.write_bytes(damaged)
        with mock.patch.object(pathlib.Path, "read_bytes", side_effect=AssertionError("whole-file read")), \
             mock.patch.object(archive_read, "_decode_record", wraps=archive_read._decode_record) as decode:
            result = SCAN.scan_wal((path, cutoff))
        self.assertIsNone(result.error)
        self.assertEqual(result.credentials, {"governing"})
        self.assertEqual(result.fallbacks, 0)
        self.assertEqual([call.args[2] for call in decode.call_args_list if call.args[1] != 2], [103, 102])

    def test_read_tail_corruption_and_torn_frames_refuse_with_the_path(self):
        reasons = {"digest": "digest mismatch", "torn": "torn or corrupt frame",
                   "sequence": "record sequence", "unknown": "newer-writer", "json": "invalid record JSON"}
        for damage, reason in reasons.items():
            with self.subTest(damage=damage):
                path = self.write_records([self.start("recent", self.now - GATE.WEEK_MS - 1),
                                           self.timed(self.attempt_template)])
                data = bytearray(path.read_bytes())
                last = next(parts for parts in reversed(list(archive_read._verified_frames(path))) if parts[1] != 2)
                offset = int(last[0].rsplit(" ", 1)[1])
                end = offset + archive_read.WAL_HEADER_LEN + len(last[4])
                if damage == "digest":
                    data[end - 1] ^= 1
                elif damage == "torn":
                    data.extend(b"\x01")
                else:
                    header = bytearray(data[offset + 4:offset + 21])
                    payload = bytes(data[offset + archive_read.WAL_HEADER_LEN:end])
                    if damage == "sequence":
                        struct.pack_into("<Q", header, 1, last[2] + 7)
                    elif damage == "unknown":
                        header[0] = 3
                        record = json.loads(payload)
                        if last[1] == 3:
                            record = record["record"]
                        payload = json.dumps({"requires": ["future-credential-scan-feature"],
                                              "record": record}).encode()
                    else:
                        payload = b'{"type":"model_attempt_finished",}'
                    data = data[:offset] + struct.pack("<I", len(payload)) + header + hashlib.sha256(header + payload).digest() + payload + data[end:]
                path.write_bytes(data)
                with self.assertRaises(SCAN.ArchiveRefusal):
                    SCAN.read_records(path)
                status, output = self.run_gate([{"id": "recent"}])
                self.assertEqual(status, 2, output)
                self.assertIn(str(path), output)
                self.assertIn(reason, output)

    def test_reverse_frame_iteration_matches_forward_across_chunk_seams(self):
        path = self.write_records([self.start("first"), self.timed(self.attempt_template),
                                   self.timed(self.activity_template, "written_at_ms")])
        expected = list(reversed(SCAN.read_records(path)))
        for chunk_size in (1, 7, 53, 4096):
            with self.subTest(chunk_size=chunk_size):
                self.assertEqual(list(SCAN.read_records_reverse(path, chunk_size)), expected)

    def test_pool_is_capped_and_workers_are_lowered_in_priority(self):
        for index in range(8):
            self.write_records([self.start("recent")], f"worker-{index}.wal")
        with mock.patch.object(GATE, "ProcessPoolExecutor", side_effect=InlinePool) as pool:
            result = GATE.recent_credentials(self.root, self.now)
        self.assertEqual(result, ({"recent"}, set(), 8, 0, 0))
        pool.assert_called_once_with(max_workers=4, initializer=SCAN.lower_worker_priority)
        with mock.patch.object(SCAN.os, "nice") as nice:
            SCAN.lower_worker_priority()
        nice.assert_called_once_with(10)

    def test_pool_startup_failure_is_a_safe_cannot_compare(self):
        self.write_records([self.start("recent")])
        with mock.patch.object(GATE, "ProcessPoolExecutor", side_effect=GATE.BrokenExecutor("token-DO-NOT-LOG")):
            status, output = self.run_gate([{"id": "recent"}])
        self.assertEqual(status, 2, output)
        self.assertIn("invalid or unreadable input (BrokenExecutor)", output)

    def test_worker_can_be_spawned_and_cli_uses_the_real_pool(self):
        paths = [self.write_records([self.start("worker")], f"spawn-{index}.wal") for index in range(2)]
        with ProcessPoolExecutor(max_workers=2, initializer=SCAN.lower_worker_priority,
                                 mp_context=multiprocessing.get_context("spawn")) as pool:
            results = list(pool.map(SCAN.scan_wal, ((path, self.now - GATE.WEEK_MS) for path in paths)))
        self.assertEqual([result.credentials for result in results], [{"worker"}, {"worker"}])
        self.assertTrue(all(result.error is None for result in results))
        listing = self.root / "live-listing.json"
        listing.write_text(json.dumps({"v": 1, "credentials": [
            {"id": "worker", "provider_ids": ["anthropic"], "auth_method": "oauth"},
        ]}))
        result = subprocess.run([sys.executable, str(SCRIPTS / "check-credential-metadata.py"),
                                 "--state-root", str(self.root), "--vault-listing", str(listing)],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("read 2 logs", result.stdout)
        self.assertIn("0 full forward fallbacks", result.stdout)

    def test_retention_deletion_before_stat_or_read_is_skipped_and_counted(self):
        discover = GATE.live_logs
        reader = SCAN.read_records_reverse
        for phase in ("stat", "read"):
            with self.subTest(phase=phase):
                vanished = self.write_records([self.start("deleted")], "deleted.wal")
                survivor = self.write_records([self.start("survivor")], "survivor.wal")

                def list_then_delete(*args, **kwargs):
                    paths = discover(*args, **kwargs)
                    if phase == "stat":
                        vanished.unlink()
                    return paths

                def delete_then_read(path):
                    if phase == "read" and path == vanished:
                        vanished.unlink()
                    return reader(path)

                with mock.patch.object(GATE, "live_logs", side_effect=list_then_delete), \
                     mock.patch.object(SCAN, "read_records_reverse", side_effect=delete_then_read) as read:
                    status, output = self.run_gate([
                        {"id": "survivor", "provider_ids": ["anthropic"], "auth_method": "oauth"},
                    ])
                self.assertEqual(status, 0, output)
                self.assertFalse(vanished.exists())
                read.assert_any_call(survivor)
                self.assertIn("read 1 logs", output)
                self.assertIn("1 logs vanished during the scan", output)

    def test_a_missing_file_error_for_a_present_log_fails_closed_with_its_path(self):
        path = self.write_records([self.start("recent")])
        error = FileNotFoundError(errno.ENOENT, "token-DO-NOT-LOG", str(path))
        with mock.patch.object(SCAN, "read_records_reverse", side_effect=error):
            status, output = self.run_gate([{"id": "recent"}])
        self.assertTrue(path.exists())
        self.assertEqual(status, 2, output)
        self.assertIn(f"FileNotFoundError: errno={errno.ENOENT}, path={json.dumps(str(path))}", output)
        self.assertNotIn("PASS", output)

    def test_an_error_rechecking_a_missing_log_still_fails_closed(self):
        path = self.write_records([self.start("recent")])
        reader = SCAN.read_records_reverse
        lstat = pathlib.Path.lstat

        def delete_then_read(wal):
            path.unlink()
            return reader(wal)

        def denied_recheck(wal, *args, **kwargs):
            if wal == path:
                raise PermissionError(errno.EACCES, "token-DO-NOT-LOG", str(wal))
            return lstat(wal, *args, **kwargs)

        with mock.patch.object(SCAN, "read_records_reverse", side_effect=delete_then_read), \
             mock.patch.object(pathlib.Path, "lstat", new=denied_recheck):
            status, output = self.run_gate([{"id": "recent"}])
        self.assertEqual(status, 2, output)
        self.assertIn(f"PermissionError: errno={errno.EACCES}, path={json.dumps(str(path))}", output)

    def test_a_dangling_log_entry_is_not_a_confirmed_vanish(self):
        path = self.write_records([self.start("recent")])
        discover = GATE.live_logs

        def replace_after_listing(*args, **kwargs):
            paths = discover(*args, **kwargs)
            path.unlink()
            path.symlink_to(path.with_suffix(".missing"))
            return paths

        with mock.patch.object(GATE, "live_logs", side_effect=replace_after_listing):
            status, output = self.run_gate([{"id": "recent"}])
        self.assertTrue(path.is_symlink())
        self.assertEqual(status, 2, output)
        self.assertIn(f"FileNotFoundError: errno={errno.ENOENT}, path={json.dumps(str(path))}", output)

    def test_if_every_log_vanishes_the_comparison_is_still_empty(self):
        path = self.write_records([self.start("recent")])
        discover = GATE.live_logs

        def list_then_delete(*args, **kwargs):
            paths = discover(*args, **kwargs)
            path.unlink()
            return paths

        with mock.patch.object(GATE, "live_logs", side_effect=list_then_delete):
            status, output = self.run_gate([{"id": "recent"}])
        self.assertEqual(status, 2, output)
        self.assertIn("no credentials recorded", output)
        self.assertNotIn("PASS", output)

    def test_archive_refusal_names_os_error_paths_without_os_error_text(self):
        source = self.root / 'source"quoted'
        destination = self.root / "destination"
        for embedded in (True, False):
            with self.subTest(embedded=embedded):
                error = OSError(errno.EXDEV, "token-DO-NOT-LOG", str(source), None, str(destination))
                refusal = SCAN.ArchiveRefusal(f"cannot enumerate WAL population: {error}" if embedded
                                             else "cannot enumerate WAL population")
                refusal.__cause__ = error
                with mock.patch.object(GATE, "live_logs", side_effect=refusal):
                    status, output = self.run_gate([{"id": "recent"}])
                self.assertEqual(status, 2, output)
                self.assertIn(f"OSError: errno={errno.EXDEV}, path={json.dumps(str(source))}", output)
                self.assertIn(f"path2={json.dumps(str(destination))}", output)

    def test_archive_refusal_prints_its_class_and_reason(self):
        self.write_records([self.start("recent")])
        with mock.patch.object(SCAN, "read_records_reverse", side_effect=SCAN.ArchiveRefusal("torn tail (incomplete payload)")):
            status, output = self.run_gate([{"id": "recent"}])
        self.assertEqual(status, 2, output)
        self.assertIn("ArchiveRefusal: torn tail (incomplete payload)", output)

    def test_json_parser_details_are_not_printed(self):
        self.write_records([self.start("recent")])
        parser_error = json.JSONDecodeError("token-DO-NOT-LOG", "handle-DO-NOT-LOG", 0)
        refusal = SCAN.ArchiveRefusal(f"invalid record JSON: {parser_error}")
        refusal.__cause__ = parser_error
        with mock.patch.object(SCAN, "read_records_reverse", side_effect=refusal):
            status, output = self.run_gate([{"id": "recent"}])
        self.assertEqual(status, 2, output)
        self.assertIn("ArchiveRefusal: invalid record JSON: [parser detail withheld]", output)

    def test_invalid_vault_json_does_not_print_parser_text_or_input(self):
        self.write_records([self.start("recent")])
        status, output = self.run_listing('{"handle-DO-NOT-LOG": "token-DO-NOT-LOG",', raw=True)
        self.assertEqual(status, 2, output)
        self.assertIn("invalid or unreadable input (JSONDecodeError)", output)
        self.assertNotIn("Expecting", output)


if __name__ == "__main__":
    unittest.main()
