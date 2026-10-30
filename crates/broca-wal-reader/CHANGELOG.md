# Changelog

## Unreleased — preparing 0.1.0

### Added

- Read-only v4 frame decoding with header CRC32C and payload SHA-256 checks.
- Published-head proofs, writer succession, and validated resumable checkpoints.
- Billing projections for provider attempts and cache warms, including
  `attempt_id` identity, unreadable evidence, and named full-replay restarts.
- Package metadata and an Astrocyte reader guide covering retention and the
  billing watermark contract.

### Open release gates

This is release preparation, not a published release. Passing package checks
does not close these gates:

- **G-ASTRO-1 — ASTRO API review and signoff:** frame and identity rules, billing
  fields, `attempt_id` deduplication, unreadable evidence, and checkpoint handling
  must be accepted by ASTRO before publishing. The consumer API is still under
  review; the README example describes the API in this source tree.
- **R4a — archive reader:** archive format/decoder and its acceptance checks are
  pending separate archive-support work. Publication is parked until R4a lands
  on master or the designated release-review chair issues an explicit waiver.
  This crate does not yet read archives; the selected archive decoder is
  pure-Rust `ruzstd`, with no dictionary.
- **Registry upload:** the repository maintainer publishes to crates.io only after
  the release gates close. The version in Cargo.toml is not evidence of an
  uploaded release.
- **Native publication dry-runs:** `cargo publish --dry-run` must still pass on
  macOS and Windows with access to crates.io. Successful offline package and
  packaged-test builds do not close these registry-facing verification gates.
- **ASTRO staging adoption and checks:** after publication, Astrocyte must consume
  the published 0.1 release on a staging `broca-state/2` root. Record ASTRO's own
  checks that billing counts each `attempt_id` exactly once across kill/restart,
  archive/unarchive, and restore. The archive cases also wait for R4a; a broca unit
  test or package dry-run is not a substitute for these consumer-owned checks.
