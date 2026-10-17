//! The backup descriptor is a DEPLOYMENT ARTIFACT, and until now it lived only on the disk
//! whose loss it exists to survive.
//!
//! `engram-catalog.json` at the repo root declares which parts of broca's state root engram
//! captures. It is placed at `<state-root>/engram-catalog.json`, and it was NOT tracked here —
//! zero copies in git against 54 other tracked `.json` files, so the search that found it was
//! reaching. A rebuilt machine would have had no declaration of what broca needs captured, in
//! exactly the scenario the file is for.
//!
//! It also explains a smaller failure: I described this descriptor to the fleet FROM MEMORY and
//! got the entry count, two mechanisms and one entry name wrong, because there was no copy in
//! the tree to read in passing — only one in a runtime directory nobody browses.
//!
//! The first test always runs and is the real one. The second compares the tracked copy against
//! the placed copy and can only run where a state root exists; it says so loudly when it skips
//! rather than reporting a pass over nothing.

use std::path::PathBuf;

fn tracked_descriptor() -> serde_json::Value {
    // CARGO_MANIFEST_DIR is crates/broca-module-serve; the descriptor is at the repo root.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("engram-catalog.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("tracked descriptor unreadable at {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("tracked descriptor is not JSON: {e}"))
}

/// The backup descriptor declares nine entries. Engram includes only `portable` entries in
/// each backup generation; `rebuild`, `device-local` and `excluded` entries are left out.
/// Only durable sessions are backed up, in their own trees. `wal/` and
/// `wal-archive/` hold disposable sessions whose callers relaunch rather than restore
/// them, so both are `excluded`. Retention
/// transcripts in `wal-ephemeral/` are `excluded` too: deleting a live WAL cannot remove a
/// backup that already holds it, so a retained session must never be captured. Billing does
/// not depend on these trees surviving a disk loss: the billing collector ingests export
/// facts into its own backed-up store.
#[test]
fn the_tracked_descriptor_declares_brocas_nine_entries_with_their_classes() {
    let d = tracked_descriptor();
    assert_eq!(d["module_id"], "broca");

    let entries = d["entries"].as_array().expect("entries is a list");
    let mut got: Vec<(String, String, String)> = entries
        .iter()
        .map(|e| {
            (
                e["entry_id"].as_str().unwrap_or_default().to_string(),
                e["class"].as_str().unwrap_or_default().to_string(),
                e["mechanism"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    got.sort();

    let mut want: Vec<(String, String, String)> = [
        // `rebuild` IS IMPRECISE HERE AND IS KEPT DELIBERATELY. The class means "safely
        // skippable at capture", which is true, but it READS as "the owner can reproject
        // this" -- and broca cannot: `crates/broca-wal/src/record.rs` has zero cap-shaped
        // records (control: RunStarted and two others do exist), because cap rows arrive as
        // `cap.install` over the wire. ASTRO then corrected the other half: their v2 cutover
        // deleted the cap delivery machine, so no peer re-provides it either. On machine loss
        // caps.db is simply gone, and nothing recovers it. Zero rows today, so nothing is at
        // risk; the fleet ruling (ENGRAM, #store-contract) was to record the recovery rather
        // than grow a taxonomy for one instance. If a producer ever returns, its contract is
        // written first and this row changes with it.
        ("broca/caps", "rebuild", "whole-db"),
        ("broca/durable-wals", "portable", "filetree"),
        ("broca/durable-archive", "portable", "filetree"),
        ("broca/ephemeral-wals", "excluded", "filetree"),
        ("broca/leases", "device-local", "filetree"),
        ("broca/request-dumps", "device-local", "filetree"),
        ("broca/run-index", "rebuild", "whole-db"),
        ("broca/session-archive", "excluded", "filetree"),
        ("broca/session-wals", "excluded", "filetree"),
    ]
    .iter()
    .map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()))
    .collect();
    want.sort();

    assert_eq!(
        got, want,
        "the descriptor's entry set or classes moved; a class change silently changes what engram \
         captures, so this must be a deliberate edit rather than a surprise"
    );

    // Pin the captured entries independently: making wal/ or wal-archive/ portable
    // would silently include disposable sessions in every backup generation.
    let mut portable: Vec<&str> = entries
        .iter()
        .filter(|e| e["class"] == "portable")
        .filter_map(|e| e["entry_id"].as_str())
        .collect();
    portable.sort();
    assert_eq!(portable, ["broca/durable-archive", "broca/durable-wals"]);
    for (id, path) in [
        ("broca/session-wals", "wal"),
        ("broca/session-archive", "wal-archive"),
        ("broca/ephemeral-wals", "wal-ephemeral"),
        ("broca/durable-wals", "wal-durable"),
        ("broca/durable-archive", "wal-archive-durable"),
    ] {
        let entry = entries.iter().find(|e| e["entry_id"] == id).unwrap();
        assert_eq!(entry["path"], path, "{id} must describe `{path}/`");
    }
    // Engram refuses an `excluded` entry without a non-empty reason, and shows the reason in
    // `backup.status`, so every deliberate omission must say why.
    for entry in entries.iter().filter(|e| e["class"] == "excluded") {
        assert!(
            entry["reason"]
                .as_str()
                .is_some_and(|reason| !reason.trim().is_empty()),
            "{} is excluded without a reason",
            entry["entry_id"]
        );
    }
}

/// The tracked copy and the PLACED copy must be byte-identical, or the repo is documenting a
/// deployment that does not exist.
///
/// Skips where no state root is present (CI, a fresh checkout). The skip is loud on purpose: a
/// test about a file being absent must not itself pass silently over an absence.
#[test]
fn the_placed_descriptor_matches_the_tracked_one() {
    let Some(home) = std::env::var_os("HOME") else {
        eprintln!("SKIP the_placed_descriptor_matches_the_tracked_one: HOME unset; nothing placed");
        return;
    };
    let placed = PathBuf::from(home)
        .join(".local/share/cortexkit/broca")
        .join("engram-catalog.json");
    if !placed.exists() {
        eprintln!(
            "SKIP the_placed_descriptor_matches_the_tracked_one: no descriptor at {} \u{2014} \
             this machine has no broca state root, so there is nothing to compare",
            placed.display()
        );
        return;
    }

    let placed_raw = std::fs::read_to_string(&placed).expect("placed descriptor readable");
    let placed_json: serde_json::Value =
        serde_json::from_str(&placed_raw).expect("placed descriptor is JSON");

    assert_eq!(
        placed_json,
        tracked_descriptor(),
        "the placed descriptor at {} differs from the tracked one; engram captures what is \
         PLACED, so the repo would be describing a backup that does not happen",
        placed.display()
    );
}
