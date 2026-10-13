//! Fails when the benchmark's subc envelope drifts from the one production ships.
//!
//! The envelope is the subc data-plane frame header and payload encoding exercised by replay.
//! Benchmark and production currently share an exact registry pin. Keep the version-pair
//! guard for any future split: a benchmark represents production only while their framing
//! agrees. This checks reviewed versions, not frame bytes; adding a pair requires reviewing
//! the framing changes between those releases.

use std::collections::BTreeSet;

/// The accepted pair, verified representative at the time it was pinned.
///
/// Bump ONLY after confirming the framing the bench exercises is unchanged between the two. If it
/// has changed, the honest fix is moving the bench pin, not widening this constant.
///
/// COMPARED AS AN UNORDERED PAIR, deliberately. My first version wrote it as (older, newer) and
/// failed on the correct pins, because the versions arrive LEXICALLY sorted -- "0.13.0" precedes
/// "0.9.0", since '1' precedes '9'. Sorting version strings as text is the sort of thing that
/// reads as ordered and is not, so this compares membership instead of position and never needs
/// to be right about which is newer.
const ACCEPTED: &[[&str; 2]] = &[
    ["0.9.0", "0.13.0"],
    ["0.9.0", "0.14.0"],
    ["0.9.0", "0.17.0"],
    ["0.9.0", "0.18.0"],
    ["0.9.0", "0.19.0"],
    ["0.9.0", "0.19.1"],
    // 0.19.2: manifest.rs only (build-provenance explanation); frame.rs untouched since 2026-07-13.
    ["0.9.0", "0.19.2"],
    // 0.20.0/0.21.0: BindIdentity non_exhaustive + project_id, drain triad, FLAG_SUBSCRIPTION
    // allocated (bit 7, emission held). frame.rs untouched since 2026-07-13.
    ["0.9.0", "0.21.0"],
    // 0.21.1: src/lib.rs +12 (additive) and one golden decision-table row; frame.rs
    // untouched since 2026-07-13, so the wire framing this benchmark stands in for
    // cannot have moved under it.
    ["0.9.0", "0.21.1"],
    // 0.22.0: additive only -- manifest.rs +27/-2 and session.rs +6, with frame.rs untouched
    // since July. The wire framing this benchmark stands in for cannot have moved under it,
    // which is what admits the pair rather than the version number alone.
    ["0.9.0", "0.22.0"],
    // 0.23.0: a module-live-roots control response plus its golden. frame.rs untouched since
    // 2026-07-13, so the wire framing this benchmark stands in for cannot have moved under it.
    ["0.9.0", "0.23.0"],
    // 0.24.0: unknown_module became a terminal route.open refusal -- a semantic change to the
    // retry predicate, not to framing. frame.rs is byte-identical across the bump (empty diff
    // over 73ceedb9) and untouched since 2026-07-13.
    ["0.9.0", "0.24.0"],
    // 0.24.1: docs and tests only -- MODULE_RELOADING documented as a pre-send guarantee and
    // pinned by a journal assertion. frame.rs empty diff across the bump, untouched since July.
    ["0.9.0", "0.24.1"],
    // 0.25.0: the daemon mints one machine id and serves it on HELLO_ACK (a new optional
    // field), plus not_ready on catalog entries. frame.rs untouched since 2026-07-13.
    ["0.9.0", "0.25.0"],
    // 0.25.1: redacts the launch nonce from every Debug impl that could print it. No framing
    // change -- frame.rs untouched since 2026-07-13.
    ["0.9.0", "0.25.1"],
    // 0.25.2: adds error_codes::is_established_route_dead(flags, code) -- an additive predicate,
    // no framing change; frame.rs untouched since 2026-07-13.
    ["0.9.0", "0.25.2"],
    // 0.26.0: ToolCallRequest gains an optional call_key and ManifestProvenance gains
    // launch_nonce_source (non_exhaustive, with a builder). Only manifest.rs and tool_call.rs
    // changed; frame.rs untouched since 2026-07-13.
    ["0.9.0", "0.26.0"],
    // 0.27.0: the daemon's scope types (scope on route open, route bind and the control
    // plane) and ToolCallRequest.schema_pin, all optional fields. frame.rs untouched since
    // 2026-07-13.
    ["0.9.0", "0.27.0"],
    // 0.28.0: ToolCallRequest.origin (relayed-call attribution), route.open role_versions and
    // the bind's scope stamp. All optional fields on control and tool-call bodies; frame.rs
    // is byte-identical across the bump, so the wire framing this benchmark stands in for
    // cannot have moved under it.
    ["0.9.0", "0.28.0"],
    // 0.28.1: scope inputs decode principals strictly, and the daemon's transport (0.9.1) now
    // refuses an invalid or oversized frame before emitting any bytes. Neither changes the bytes
    // of a valid frame: frame.rs is byte-identical to its 2026-07-13 form (a frame build error
    // added on the way was reverted before 0.28.1).
    ["0.9.0", "0.28.1"],
    // 0.29.0: optional ToolCallRequest.preset and ScopeAttributes.flow_id; frame.rs is byte-identical
    // across the bump, so the request-body additions do not change the framing measured here.
    ["0.10.0", "0.29.0"],
    // 0.29.1: adds the flow-scopes/v1 capability name and the terminal route-open code
    // target_flow_unsupported. frame.rs is unchanged.
    ["0.10.0", "0.29.1"],
];

// 0.19.0 admitted 2026-09-06. The ENTIRE crate diff over the bump is `src/manifest.rs`, 208
// insertions and 155 deletions, converting `ModuleManifest::builder` from a four-argument form to
// a two-argument one with `trust_tier` and `bindings` as builder methods. `frame.rs` is untouched
// -- verified by diffing the two crate trees rather than reading the wave notice, and its last
// commit is 2026-07-13, predating every pin in this list.
//
// A manifest change cannot move this bench for the reason stated below: the manifest is a
// HELLO-time structure and this measures the DATA-PLANE envelope. That same reasoning admitted
// 0.14.0 and 0.18.0, and it holds here for the strongest version of the argument -- manifest.rs is
// the ONLY file that changed.

// 0.18.0 admitted 2026-09-04, and it is the cheapest review yet BY CONSTRUCTION: the whole diff
// is `src/tool_call.rs` (new), one `pub mod` line, Cargo.toml, and four golden vectors.
// `frame.rs` is ABSENT FROM THE FILE LIST -- not merely unchanged in content, absent -- so the
// envelope this bench measures cannot have moved. That new type is the ToolCallRequest broca now
// emits, and it lives in a request BODY the daemon splices opaquely, never in framing.
//
// 0.14.0 admitted 2026-08-29, and the question this test exists to force was answered at source
// rather than from the wave notice that announced the bump.
//
// Every subc-protocol commit between the 0.13.0 pin and 0.14.0 touches `manifest.rs` (plus its
// Cargo.toml and a lib.rs re-export) for the self-signal registry. `frame.rs` -- the envelope this
// bench measures -- is byte-identical across the bump, verified with a diff over the range rather
// than inferred from the changelog.
//
// The manifest is a HELLO-time structure and the bench exercises the DATA-PLANE envelope, so a
// manifest field cannot change what is being measured. That distinction is the whole answer: the
// version moved and the wire did not.
//
// 0.17.0 admitted 2026-09-02, same question, same method, and the answer is bounded more
// cheaply than by a range diff: `frame.rs` has not been touched since 2026-07-13
// (5c1e2d23, the wire-v2 transport shapes), which PREDATES every pin in this list. So the
// envelope is unchanged across all three admitted versions by a single check, not three.
//
// 0.15 through 0.17 moved for provenance: canonical form validation on `build_git_sha` and
// `build_lock_digest`, and a constructor that owns `wire_crate_version`. HELLO-time again.
//
// WHY THIS LIST GROWS RATHER THAN GETTING A RANGE OR A FLOOR: an accepted-above-X rule would
// admit every future bump silently, which is precisely the notification this test exists to
// produce. Each entry costs one deliberate look at one module, and the look is the product.

fn subc_protocol_versions() -> BTreeSet<String> {
    // Read the LOCK rather than the manifests: the manifest carries a requirement ("0.9"), the
    // lock carries what actually got compiled. A caret requirement can resolve to something the
    // manifest never names, and the resolved version is the one the bench measures against.
    let lock = include_str!("../../../Cargo.lock");
    let mut found = BTreeSet::new();
    let mut lines = lock.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim() == "name = \"subc-protocol\"" {
            if let Some(version_line) = lines.peek() {
                if let Some(rest) = version_line.trim().strip_prefix("version = \"") {
                    if let Some(v) = rest.strip_suffix('"') {
                        found.insert(v.to_string());
                    }
                }
            }
        }
    }
    found
}

#[test]
fn the_bench_envelope_matches_a_reviewed_pair() {
    let versions = subc_protocol_versions();

    // An empty read is an instrument failure, not a clean result. If the lock format changes or
    // the crate is renamed, this test would otherwise pass by finding nothing -- reporting
    // agreement it never checked.
    assert!(
        !versions.is_empty(),
        "found no subc-protocol entries in Cargo.lock; the parser is broken, not the pins"
    );

    if versions.len() == 1 {
        // Unified. Nothing to be unrepresentative against.
        return;
    }

    let observed: Vec<&str> = versions.iter().map(String::as_str).collect();
    assert_eq!(
        observed.len(),
        2,
        "expected at most two subc-protocol instances, found {observed:?}"
    );
    let reviewed = ACCEPTED.iter().any(|accepted| {
        accepted.iter().all(|v| observed.contains(v)) && observed.len() == accepted.len()
    });
    let pair = (observed[0], observed[1]);

    assert!(
        reviewed,
        "subc-protocol pins moved to {pair:?}, which no one has reviewed.\n\
         \n\
         broca-bench and broca-replay measure replay framing against the OLDER envelope; the\n\
         shipped daemon links the newer one. That is fine while the framing they share is\n\
         unchanged, and quietly wrong when it is not -- the bench keeps reporting confidently\n\
         about a wire format nothing runs.\n\
         \n\
         Confirm the framing the bench exercises is unchanged across those two versions, then\n\
         add the pair to ACCEPTED. If it HAS changed, move the bench pin instead."
    );
}
