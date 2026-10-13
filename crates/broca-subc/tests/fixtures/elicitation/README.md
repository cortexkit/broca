# Pinned elicitation requester vectors

Payloads are vendored byte-for-byte from prefrontal tag `elicitation-records-v1-c9`, commit
`5951123fd14a2a6cbf3ff7aaed5eaab8f0c6c1cd`. Source directories are
`test-vectors/elicitation-records-v1/c9/` and
`test-vectors/elicitation-requester-v1/`. `records-README.md` and
`requester-README.md` are locally trimmed fixture notes.

`SHA256SUMS` hashes all 28 local fixture files, including the JSON, JCS, hash sidecars
and trimmed READMEs. Its pinned SHA-256 is
`78ee4df7563e3194cbed02a301c1759e474786844e75b4311bb38e9448bd6b62`.
Run `python3 crates/broca-subc/tests/fixtures/elicitation/verify.py` from the
repository root. It verifies committed bytes and the `c9/` envelope sidecars;
tests never read the uncommitted evidence directory.

The `c9/` JCS bytes pin operation payloads, not subc framing. The adapter wraps the
payload in `{method, params}`. The scripted provider checks the received
canonical parameter bytes against the independently vendored request, and
returns the pinned reply. Requester v1 timestamps written as `@...@` strings are
fixture placeholders, not a production timestamp schema.
