#!/usr/bin/env python3
"""Verify consent provenance, raw fixture bytes and the held-call mapping."""

import hashlib
import json
from pathlib import Path
import re
import sys
import unittest
from collections import Counter

ROOT = Path(__file__).resolve().parent
BASE_COMMIT = "a87da8c86f526f2d489adc433517246e1399e604"
LATE_COMMIT = "880a065865ae4aaecc0ccbef0e5fa73ae13d014a"
CORPORA = {
    "upstream/consent-requester-v1": (
        "consent-requester-v2", BASE_COMMIT,
        "e4eb488651dbd1e3c3164644a4e4bc0ba3d39f81cb4f3e1f09cc7913564a3248",
    ),
    "upstream/consent-requester-v2": (
        "consent-requester-v2", BASE_COMMIT,
        "d276eaed5330b95923166d006b2f1b5d83a9a95e050cfe99446d685b22c2e568",
    ),
    "upstream/consent-late-answer-v1": (
        "consent-late-answer-v1", LATE_COMMIT,
        "ab436b9d31fa27553aa9a5c7e5af2a3723a108246c222badcb85bc3de4d1f2d6",
    ),
}
CONTRACTS = {
    "upstream/contracts/consent-requester-v1.md": (
        "consent-requester-v2", BASE_COMMIT,
        "74adafc24e98e7e4958306c03c6db83fe1969763a56002adc0abc867ec3c4042",
    ),
    "upstream/contracts/consent-requester-v2.md": (
        "consent-requester-v2", BASE_COMMIT,
        "068f61d4ee572f0fbe336dad1d1c26f2e418c63456c2d112d9c1b42e1b12a4ae",
    ),
    "upstream/contracts/consent-late-answer-v1.md": (
        "consent-late-answer-v1", LATE_COMMIT,
        "5644837fbc391ba443e55ff1a7d888c444afdbce7ad201a632935c48285be5e9",
    ),
}
HELD_FIELDS = (
    "kind", "title", "prompt", "options", "default", "urgency", "on_expiry",
    "material_damage", "late_execution", "target", "expires_in_ms", "call_key",
    "module", "tool", "schema_pin", "executor", "args_jcs", "args_hash",
)
PASSTHROUGH = HELD_FIELDS[:11]
BODY_FIELDS = set(PASSTHROUGH) | {
    "args_digest", "binding", "dedup_key", "requester_ref",
}
IDENTITY_KEYS = {
    "principal", "requester", "owner", "subject", "agent_id", "head_agent_id",
    "device_id", "harness", "asker", "head", "parent", "via_worker",
    "scope_ref", "scope_epoch", "requester_session", "approval_scope",
    "session_ref", "parent_session_ref",
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    # These golden objects have ASCII strings and no floating-point or unsafe integers.
    # Reject values outside that domain rather than claim general RFC 8785 support.
    def domain(item):
        if isinstance(item, dict):
            return all(isinstance(key, str) and key.isascii() and domain(val)
                       for key, val in item.items())
        if isinstance(item, list):
            return all(domain(val) for val in item)
        if isinstance(item, str):
            return item.isascii()
        if item is None or isinstance(item, bool):
            return True
        return isinstance(item, int) and abs(item) <= 2**53 - 1

    if not domain(value):
        raise ValueError("golden canonicalization domain exceeded")
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def manifest_entries(directory):
    entries = {}
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        checksum, name = line.split("  ", 1)
        path = Path(name)
        if (not re.fullmatch(r"[0-9a-f]{64}", checksum) or path.is_absolute()
                or ".." in path.parts or name in entries):
            raise ValueError("invalid or duplicate checksum entry: " + name)
        entries[name] = checksum
    return entries


def table(heading):
    text = (ROOT / "README.md").read_text()
    section = text.split("## " + heading + "\n", 1)[1].split("\n## ", 1)[0]
    rows = []
    for line in section.splitlines():
        if line.startswith("|"):
            cells = [cell.strip().strip("`") for cell in line.strip("|").split("|")]
            if not all(re.fullmatch(r"[-: ]+", cell) for cell in cells):
                rows.append(cells)
    return rows[1:]


def golden():
    frozen = json.loads((ROOT / "broca/held-call-input.json").read_bytes())
    request = json.loads((ROOT / "broca/held-call-request.json").read_bytes())
    return frozen, request


class ConsentFixtures(unittest.TestCase):
    def test_upstream_pins(self):
        rows = table("Source pins")
        expected = {name + "/SHA256SUMS": values for name, values in CORPORA.items()}
        expected.update(CONTRACTS)
        self.assertEqual(len(rows), len(expected), "source pin row count differs")
        self.assertEqual({row[0] for row in rows}, set(expected), "source pin paths differ")
        for path, tag, commit, checksum in rows:
            self.assertEqual((tag, commit, checksum), expected[path], path)
            self.assertEqual(digest((ROOT / path).read_bytes()), checksum, path)

    def test_upstream_coverage_and_bytes(self):
        for name in CORPORA:
            directory = ROOT / name
            entries = manifest_entries(directory)
            files = {path.relative_to(directory).as_posix()
                     for path in directory.rglob("*") if path.is_file()}
            self.assertEqual(set(entries), files - {"SHA256SUMS"}, name)
            for path, checksum in entries.items():
                self.assertEqual(digest((directory / path).read_bytes()), checksum,
                                 name + "/" + path)

    def test_local_coverage_and_bytes(self):
        entries = manifest_entries(ROOT)
        files = {path.relative_to(ROOT).as_posix()
                 for path in ROOT.rglob("*") if path.is_file()}
        self.assertEqual(set(entries), files - {"SHA256SUMS"}, "local checksum coverage differs")
        for name, checksum in entries.items():
            self.assertFalse((ROOT / name).is_symlink(), name)
            self.assertEqual(digest((ROOT / name).read_bytes()), checksum, name)

    def test_mapping_exactly_once(self):
        fields = [row[0] for row in table("HeldCallRequest mapping")]
        self.assertEqual(Counter(fields), Counter(HELD_FIELDS),
                         "each HeldCallRequest field must appear exactly once")
        text = (ROOT / "README.md").read_text()
        self.assertIn("Stored elicitation_id: `result.elicitation_id`.", text)

    def test_ledger_settled(self):
        rows = table("Assumptions ledger")
        self.assertEqual(Counter(row[0] for row in rows),
                         Counter("L" + str(index) for index in range(1, 16)))
        for ledger_id, disposition, section, decision in rows:
            self.assertEqual(disposition, "Settled", ledger_id + " stops downstream readiness")
            self.assertTrue(section, ledger_id)
            self.assertTrue(decision, ledger_id)

    def test_ack_branch_and_provider(self):
        text = (ROOT / "README.md").read_text()
        self.assertIn("Selected ack branch: **C**.", text)
        self.assertIn("Consent provider id: `cingulate`.", text)
        self.assertIn("Broca sends no `consent.ack`", text)

    def test_no_identity_in_body(self):
        _, request = golden()
        params = request["params"]
        self.assertEqual(set(params), BODY_FIELDS, "generic request keys differ")
        def check(item):
            if isinstance(item, dict):
                self.assertFalse(set(item) & IDENTITY_KEYS, "identity-looking request key")
                for value in item.values():
                    check(value)
            elif isinstance(item, list):
                for value in item:
                    check(value)
        check(params)

    def test_binding_exact_argument_bytes(self):
        frozen, request = golden()
        binding = request["params"]["binding"]
        self.assertEqual(set(binding), {"args_hash", "module", "schema_pin", "tool"})
        self.assertEqual(binding["args_hash"], digest(frozen["args_jcs"].encode("utf-8")))
        self.assertEqual(frozen["args_hash"], binding["args_hash"])
        self.assertEqual(request["params"]["args_digest"], binding["args_hash"])
        for field in ("module", "schema_pin", "tool"):
            self.assertEqual(binding[field], frozen[field], field)
        reparsed_hash = digest(canonical(json.loads(frozen["args_jcs"])))
        self.assertNotEqual(binding["args_hash"], reparsed_hash, "argument order probe is vacuous")
        self.assertNotIn("approval_digest", request["params"])

    def test_deny_on_expiry(self):
        _, request = golden()
        params = request["params"]
        self.assertEqual(params["default"], "deny")
        self.assertEqual(params["on_expiry"], "deny")
        options = {option["id"]: option["effect"] for option in params["options"]}
        self.assertEqual(options["deny"], "decline")
        self.assertEqual(options["approve"], "grant")
        self.assertEqual(len(params["options"]), 2, "extra approval options are unsupported")
        self.assertEqual(options, {"approve": "grant", "deny": "decline"})
        rendering = json.loads((ROOT / "broca/expired-rendering.json").read_bytes())
        expired = json.loads((ROOT / rendering["source"]).read_bytes())
        self.assertTrue(expired["result"]["records"], "expiry vector is empty")
        for record in expired["result"]["records"]:
            self.assertEqual(record["state"], rendering["state"])
            # The upstream timer-expired record carries the default choice, even though
            # nobody answered. That metadata must not turn timer expiry into approval.
            self.assertEqual(rendering["late_decision"], "none")
            self.assertFalse(rendering["choice_metadata_authorizes"])
            self.assertFalse(rendering["ordinary_dispatch"])
            self.assertFalse(rendering["late_dispatch"])
        self.assertTrue(any("answered_choice_id" in record
                            for record in expired["result"]["records"]),
                        "legacy choice-metadata probe is vacuous")

    def test_held_call_request_mapping(self):
        frozen, request = golden()
        self.assertEqual(Counter(frozen.keys()), Counter(HELD_FIELDS))
        self.assertEqual(request["method"], "consent.request")
        params = request["params"]
        for field in PASSTHROUGH:
            self.assertEqual(params[field], frozen[field], field)
        self.assertEqual(params["requester_ref"], frozen["call_key"])
        self.assertEqual(params["dedup_key"], frozen["call_key"])
        self.assertEqual(len(frozen["call_key"]), 57)
        self.assertTrue(all(0x20 <= ord(char) <= 0x7e for char in frozen["call_key"]))
        raw = (ROOT / "broca/held-call-request.json").read_bytes()
        self.assertEqual(raw, canonical(request) + b"\n", "golden request is not canonical")


def load_tests(loader, tests, pattern):
    first = ["test_local_coverage_and_bytes", "test_upstream_pins",
             "test_upstream_coverage_and_bytes"]
    names = first + [name for name in loader.getTestCaseNames(ConsentFixtures)
                     if name not in first]
    return unittest.TestSuite(ConsentFixtures(name) for name in names)


if __name__ == "__main__":
    print("Python " + sys.version.split()[0], flush=True)
    unittest.main(verbosity=2)
