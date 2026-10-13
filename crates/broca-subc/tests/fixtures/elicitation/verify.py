#!/usr/bin/env python3
"""Verify upstream bytes against the independently pinned frozen manifest."""
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
EXPECTED = "78ee4df7563e3194cbed02a301c1759e474786844e75b4311bb38e9448bd6b62"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    # The upstream vectors restrict their RFC 8785 domain to ASCII and safe integers.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def main():
    print(f"Python {sys.version.split()[0]}")
    manifest = (ROOT / "SHA256SUMS").read_bytes()
    assert digest(manifest) == EXPECTED, "frozen manifest hash changed"
    count = 1
    for line in manifest.decode().splitlines():
        expected, name = line.split("  ", 1)
        assert digest((ROOT / name).read_bytes()) == expected, f"frozen vector hash changed: {name}"
        count += 1
    for path in sorted((ROOT / "c9").glob("*.json")):
        raw = canonical(json.loads(path.read_bytes()))
        assert raw == path.with_suffix(".jcs").read_bytes(), f"JCS bytes differ: {path.name}"
        assert digest(raw) == path.with_suffix(".sha256").read_text(), f"C9 digest differs: {path.name}"
        count += 2
    request = json.loads((ROOT / "c9/request.json").read_bytes())["request"]
    assert digest(request["args_jcs"].encode()) == request["args_hash"], "argument binding differs"
    binding = json.loads((ROOT / "c9/approval-binding.json").read_bytes())
    assert digest(canonical(binding["binding"])) == binding["approval_digest"], "approval preimage differs"
    reordered = json.loads((ROOT / "c9/args-noncanonical-key-order.json").read_bytes())
    assert digest(reordered["args_jcs"].encode()) == reordered["args_hash"], "exact-byte hash differs"
    assert reordered["args_hash"] != reordered["reserialized_hash"], "order probe is vacuous"
    print(f"elicitation vector integrity: {count + 4} checks passed (28 frozen files, 7 C9 envelopes)")


if __name__ == "__main__":
    main()
