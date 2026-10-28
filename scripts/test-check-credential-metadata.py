#!/usr/bin/env python3
"""The metadata deploy gate compares real auth records and never prints vault material."""
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
import time
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("account_metadata", SCRIPTS / "check-credential-metadata.py")
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def frame(seq: int, record: dict) -> bytes:
    payload = json.dumps(record).encode()
    header = struct.pack("<BQQ", 1, seq, 1)
    return struct.pack("<I", len(payload)) + header + hashlib.sha256(header + payload).digest() + payload


class MetadataDeployGate(unittest.TestCase):
    def setUp(self):
        scratch = SCRIPTS.parent / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        (self.root / "wal").mkdir()
        self.now = int(time.time() * 1000)

    def write_records(self, records):
        (self.root / "wal/session.wal").write_bytes(b"".join(
            frame(seq, record) for seq, record in enumerate(records, 1)
        ))

    def start(self, credential, timestamp=None, resolved=None):
        selection = {"credential_id": credential}
        if resolved:
            selection["resolved_credential_id"] = resolved
        return {"type": "run_started", "ts_ms": self.now if timestamp is None else timestamp,
                "config": {"auth_selection": selection}}

    def run_gate(self, rows):
        return self.run_listing({"v": 1, "credentials": rows})

    def run_listing(self, document):
        output = io.StringIO()
        listing = self.root / "listing.json"
        listing.write_text(json.dumps(document))
        with mock.patch.object(sys, "argv", ["check-credential-metadata.py", "--state-root", str(self.root),
                                            "--vault-listing", str(listing)]), \
             contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            status = GATE.main()
        text = output.getvalue()
        self.assertNotIn("handle-DO-NOT-LOG", text)
        self.assertNotIn("token-DO-NOT-LOG", text)
        return status, text

    def test_pass_ignores_unused_unmapped_credentials(self):
        self.write_records([self.start("broca-alias", resolved="opaque-claude")])
        status, output = self.run_gate([
            {"id": "opaque-claude", "provider_ids": ["anthropic"], "auth_method": "oauth",
             "handle": "handle-DO-NOT-LOG", "token": "token-DO-NOT-LOG"},
            {"id": "apikey:cline-pass"}, {"id": "apikey:muse"}, {"id": "oauth:cursor"},
        ])
        self.assertEqual(status, 0, output)
        self.assertIn("PASS; 1 recently used credentials", output)

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
        self.assertNotIn("PASS", output)

    def test_empty_live_listing_cannot_compare(self):
        self.write_records([self.start("recent")])
        status, output = self.run_gate([])
        self.assertEqual(status, 2, output)
        self.assertIn("cannot compare", output)

    def test_unknown_listing_version_cannot_compare(self):
        self.write_records([self.start("recent")])
        status, output = self.run_listing({"v": 2, "credentials": [{"id": "recent"}]})
        self.assertEqual(status, 2, output)
        self.assertIn("cannot compare", output)

    def test_missing_credentials_array_cannot_compare(self):
        self.write_records([self.start("recent")])
        status, output = self.run_listing({"v": 1})
        self.assertEqual(status, 2, output)
        self.assertIn("cannot compare", output)

    def test_recent_activity_and_refreeze_count_but_old_runs_do_not(self):
        self.write_records([
            self.start("old-unused", self.now - GATE.WEEK_MS - 1),
            {"type": "run_finished", "ts_ms": self.now - GATE.WEEK_MS - 1},
            {"type": "queue_entry", "ts_ms": self.now},
            self.start("long-running", self.now - GATE.WEEK_MS - 1),
            {"type": "step_started", "ts_ms": self.now},
            {"type": "run_auth_refrozen", "ts_ms": self.now,
             "auth_selection": {"credential_id": "refrozen"}},
        ])
        self.assertEqual(GATE.recent_credentials(self.root, self.now), {"long-running", "refrozen"})


if __name__ == "__main__":
    unittest.main()
