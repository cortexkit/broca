# Vendored fetch-plan vectors

`fetch-plan-v1/` contains payload files copied byte-for-byte from
`test-vectors/fetch-plan-v1/` in the prefrontal repository at commit
`65195cc3fbc437eb55950874b593a3d1eb319e60`. The local README describes fixture
formats and reproduction; payloads and digest sidecars are not edited here.

The admission tests (`tests/vectors.rs`) read every `admission/*.json` case and the plan,
composition and send files those cases name, and check broca's admission against each
expected outcome. broca does not take a compaction provider, so a case whose well-formed
plan names a `compaction_item` is expected refused `invalid_params {field:
"plan.compaction_item"}`; everything else the case records is then checked on the same plan
without that item. They also check that every plan's and composition's `.jcs` and `.sha256`
files agree with broca's own canonicalisation.

To re-vendor a newer commit: replace the payload files in `fetch-plan-v1/` with the new copy, retain
the local README trims, update the commit
above, and update the expected digests pinned in `tests/vectors.rs` (`PINNED_IDENTITIES`).
