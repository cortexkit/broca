#!/usr/bin/env python3
"""The metadata deploy gate compares real auth records and never prints vault material."""
from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import json
import pathlib
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parent
ROOT = SCRIPTS.parent
SPEC = importlib.util.spec_from_file_location("account_metadata", SCRIPTS / "check-credential-metadata.py")
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


RECENT = object()


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
        scratch = SCRIPTS.parent / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        (self.root / "wal").mkdir()
        self.now = int(time.time() * 1000)

    def write_records(self, records, name="session.wal"):
        path = self.root / "wal" / name
        path.unlink(missing_ok=True)
        subprocess.run([str(self.writer), str(path)], input=json.dumps(records), text=True, check=True)
        return path

    def start(self, credential, timestamp=RECENT, resolved=None):
        record = copy.deepcopy(self.start_template)
        if timestamp is not None:
            record["written_at_ms"] = self.now if timestamp is RECENT else timestamp
        selection = record["config"]["auth_selection"]
        selection["credential_id"] = credential
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
        record = GATE.read_records(path)[0].record
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
        refreeze["auth_selection"]["credential_id"] = "refrozen"
        path = self.write_records([
            self.start("old-unused", self.now - GATE.WEEK_MS - 1),
            self.timed(self.finish_template, timestamp=self.now - GATE.WEEK_MS - 1),
            self.start("long-running", timestamp=None),
            copy.deepcopy(self.step_template),
            self.timed(self.activity_template, field="written_at_ms"),
            refreeze,
        ])
        records = [frame.record for frame in GATE.read_records(path)]
        self.assertNotIn("written_at_ms", records[2])
        self.assertNotIn("ts_ms", records[2])
        self.assertNotIn("ts_ms", records[3])
        self.assertEqual(records[4]["written_at_ms"], self.now)
        self.assertEqual(records[5]["ts_ms"], self.now)
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"long-running", "refrozen"}, 1))

    def test_recent_ts_ms_activity_counts_a_legacy_untimed_selection(self):
        self.write_records([self.start("legacy", timestamp=None), self.timed(self.attempt_template)])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"legacy"}, 1))

    def test_untimed_refreeze_waits_for_later_activity_without_refusing(self):
        refreeze = copy.deepcopy(self.refreeze_template)
        refreeze["auth_selection"]["credential_id"] = "refrozen-legacy"
        self.write_records([self.start("legacy", timestamp=None), refreeze,
                            self.timed(self.attempt_template)])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"refrozen-legacy"}, 1))

    def test_untimed_records_alone_are_not_evidence_of_recency(self):
        self.write_records([self.start("untimed", timestamp=None), copy.deepcopy(self.step_template)])
        status, output = self.run_gate([{"id": "untimed"}])
        self.assertEqual(status, 2, output)
        self.assertIn("no credentials recorded", output)
        self.assertNotIn("no recorded timestamp", output)

    def test_recent_terminal_activity_counts_and_ends_the_selection(self):
        self.write_records([self.start("ended", timestamp=None), self.timed(self.finish_template),
                            self.timed(self.attempt_template)])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), ({"ended"}, 1))

    def test_old_live_logs_and_archives_are_not_read(self):
        old = self.write_records([self.start("old-unused", self.now - GATE.WEEK_MS - 1)], "old.wal")
        old_seconds = (self.now - GATE.WEEK_MS - 1000) / 1000
        os.utime(old, (old_seconds, old_seconds))
        recent = self.write_records([self.start("recent")])
        archive = self.root / "wal-archive"
        archive.mkdir()
        (archive / "fold-1.ark").write_bytes(b"must not be opened")
        with mock.patch.object(GATE, "read_records", wraps=GATE.read_records) as read:
            status, output = self.run_gate([{"id": "recent", "provider_ids": ["anthropic"], "auth_method": "oauth"}])
        self.assertEqual(status, 0, output)
        read.assert_called_once_with(recent)
        self.assertIn("read 1 logs", output)

    def test_pass_reports_log_count_and_elapsed_time(self):
        self.write_records([self.start("recent")], "first.wal")
        self.write_records([self.start("recent")], "second.wal")
        with mock.patch.object(GATE.time, "monotonic", side_effect=[100, 100.25]):
            status, output = self.run_gate([{"id": "recent", "provider_ids": ["anthropic"], "auth_method": "oauth"}])
        self.assertEqual(status, 0, output)
        self.assertIn("read 2 logs in 0.25s", output)

    def test_archive_refusal_prints_its_class_and_reason(self):
        self.write_records([self.start("recent")])
        with mock.patch.object(GATE, "read_records", side_effect=GATE.ArchiveRefusal("torn tail (incomplete payload)")):
            status, output = self.run_gate([{"id": "recent"}])
        self.assertEqual(status, 2, output)
        self.assertIn("ArchiveRefusal: torn tail (incomplete payload)", output)

    def test_json_parser_details_are_not_printed(self):
        self.write_records([self.start("recent")])
        parser_error = json.JSONDecodeError("token-DO-NOT-LOG", "handle-DO-NOT-LOG", 0)
        refusal = GATE.ArchiveRefusal(f"invalid record JSON: {parser_error}")
        refusal.__cause__ = parser_error
        with mock.patch.object(GATE, "read_records", side_effect=refusal):
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
