//! `engram-catalog.json` at the repo root declares which parts of broca's state root engram
//! captures. broca places it at `<state-root>/engram-catalog.json` at start. The first test
//! pins the declared entries and always runs; the second compares the tracked copy with the
//! placed one and runs only where a state root exists, saying so loudly when it skips.

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

/// Engram includes only `portable` entries in each backup generation; `rebuild`,
/// `device-local` and `excluded` entries are left out. The session store keeps one complete
/// store per backup class: `session-store-durable/` is captured, `session-store/` (disposable
/// sessions their callers relaunch) and `session-store-ephemeral/` (sessions with a retention
/// policy, which a backup must not outlive) are not, and its moves' journals and staged copies
/// (`session-store-moves/`) stay on the device. Billing does not depend on these trees surviving
/// a disk loss: the billing collector ingests export facts into its own backed-up store.
#[test]
fn the_tracked_descriptor_declares_brocas_entries_with_their_classes() {
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
        // `caps.db` is left over from spend caps, which broca no longer has: nothing opens or
        // writes it, and it is left on disk rather than deleted. The entry stays so engram still
        // knows what the file is. `rebuild` only means "safely skippable at capture"; nothing
        // reprojects it, and nothing needs to, because no build reads it.
        ("broca/caps", "rebuild", "whole-db"),
        // Its durable root holds the sessions whose creating send carried a class in
        // `backup_classes`, the heads (an agent's long-lived main session) that `session.import`
        // brought onto broca from another runner's files, and the
        // long-lived sessions an operator moved there. A head is backed up only when it is
        // created with `session_class: "head"`.
        ("broca/session-store-durable", "portable", "filetree"),
        ("broca/session-store", "excluded", "filetree"),
        ("broca/session-store-ephemeral", "excluded", "filetree"),
        ("broca/session-store-moves", "device-local", "filetree"),
        ("broca/media-uploads", "excluded", "filetree"),
        ("broca/leases", "device-local", "filetree"),
        ("broca/request-dumps", "device-local", "filetree"),
        ("broca/run-index", "rebuild", "whole-db"),
        // System One's batch store keeps caller text for seven days by ruling; a backup
        // generation would keep it longer.
        ("broca/system-one", "excluded", "whole-db"),
        ("broca/grants", "excluded", "whole-db"),
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

    // Pin the captured entries independently: making the session store's ordinary or ephemeral
    // root portable would silently include disposable or retained sessions in every backup
    // generation.
    let mut portable: Vec<&str> = entries
        .iter()
        .filter(|e| e["class"] == "portable")
        .filter_map(|e| e["entry_id"].as_str())
        .collect();
    portable.sort();
    assert_eq!(portable, ["broca/session-store-durable"]);
    for (id, path) in [
        ("broca/session-store-durable", "session-store-durable"),
        ("broca/session-store", "session-store"),
        ("broca/session-store-ephemeral", "session-store-ephemeral"),
        ("broca/session-store-moves", "session-store-moves"),
        ("broca/media-uploads", "media-uploads"),
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
