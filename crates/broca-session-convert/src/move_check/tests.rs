//! `move-check` over copies of state roots: each session's media are read from today's media
//! store that the folder of its log chooses, and the report counts every log that wouldn't move
//! by class and times the check.

use std::path::{Path, PathBuf};

use broca_core::media_store::MediaStoreClass;
use broca_protocol::BindIdentity;
use broca_wal::{Wal as _, WalRecord};
use serde_json::json;

use super::{check, MoveCheckReport};
use crate::Args;

#[test]
fn mismatch_examples_keep_the_complete_error() {
    let mut report = MoveCheckReport::default();
    let detail = format!(
        "{}: a placed message names a record that didn't accept it",
        "prefix ".repeat(30)
    );
    MoveCheckReport::count(&mut report.mismatches, &detail);
    report.example(&detail, "synthetic.wal");
    assert_eq!(
        report.mismatches.keys().next().unwrap().chars().count(),
        120
    );
    assert_eq!(
        report.examples.get(&detail),
        Some(&vec!["synthetic.wal".into()])
    );
}

/// A 1x1 PNG.
const PNG: &str = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";

fn png() -> Vec<u8> {
    (0..PNG.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&PNG[at..at + 2], 16).unwrap())
        .collect()
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    broca_session_store::codec::hex(&sha2::Sha256::digest(bytes))
}

fn session() -> BindIdentity {
    BindIdentity::new("/project", "runner", "s")
}

#[test]
fn source_history_versions_are_read_without_allocating_or_changing_source_rows() {
    use broca_core::ServedHistory as _;
    let root = scratch("history-version");
    let state = state_root(&root, &[]);
    let source = log_in(&state, "wal");
    let index = state.join("run-index.db");
    assert_eq!(
        super::source_history_version(&source, &session(), true).unwrap(),
        1
    );
    assert_eq!(
        super::source_history_version(&source, &session(), false).unwrap(),
        0
    );
    assert!(!index.exists(), "a missing source index is not created");
    let store = broca_store::SqliteStore::open(&index).unwrap();
    let address = broca_core::session_addr(&session());
    assert_eq!(
        super::source_history_version(&source, &session(), true).unwrap(),
        1
    );
    assert_eq!(
        store.history_read_version(&address).unwrap(),
        None,
        "the read-only rehearsal allocates no row"
    );
    let version = store
        .served_snapshot(&address, true, Some("source-lineage"), None)
        .unwrap()
        .history_version;
    assert!(
        version > 1,
        "a publishing row allocation has a wall-clock floor"
    );
    drop(store);
    let before = std::fs::read(&index).unwrap();
    assert_eq!(
        super::source_history_version(&source, &session(), true).unwrap(),
        version
    );
    assert_eq!(std::fs::read(&index).unwrap(), before);
    std::fs::remove_dir_all(root).unwrap();
}

fn scratch(tag: &str) -> PathBuf {
    let root = broca_test_support::scratch_root().join(format!(
        "session-convert-move-check-{}-{tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

/// What one test log holds besides its run.
#[derive(Clone, Copy, Default)]
struct Holds {
    /// Its input holds an image of [`png`]'s bytes.
    image: bool,
    /// Its run has an input-token budget, which the new run loop refuses.
    input_budget: bool,
    /// Its historical request cannot be reproduced by today's offline renderer.
    legacy_replay_limit: bool,
}

/// Writes at `path` a log of one run answering in text, whose attempt recorded the digest of the
/// request today's renderer builds for it, with the image's bytes read from today's media store
/// of class `class` in `state_root` when the input holds one.
fn write_log(path: &Path, holds: Holds, state_root: &Path, class: MediaStoreClass) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut wal = broca_wal::FileWal::open(path)
        .unwrap()
        .with_feature_floor(state_root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    let lineage = wal.lineage_id().map(|id| id.to_string());
    let frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    let bytes = png();
    let mut content = vec![json!({"type": "text", "text": "Look."})];
    if holds.image {
        content.push(
            json!({"type": "media", "kind": "image", "mime": "image/png",
            "byte_size": bytes.len(), "width": 1, "height": 1, "sha256": sha256(&bytes)}),
        );
    }
    let mut start = record(json!({
        "type": "run_started", "run_id": "r1", "ts_ms": 1_000,
        "session": {"project_root": "/project", "harness": "runner", "session": "s"},
        "config": {
            "model": {"provider_module_id": "anthropic", "model_id": "claude-test"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0,
            "renderer_version": frozen.renderer_version,
            "frozen_render_config": serde_json::to_value(&frozen).unwrap(),
        },
        "input": [{"role": "user", "content": content}],
        "origin": {"kind": "fresh"},
    }));
    let WalRecord::RunStarted { config, .. } = &mut start else {
        unreachable!()
    };
    config.frozen_render_config = serde_json::to_value(&frozen).unwrap();
    if holds.input_budget {
        config.budget = Some(serde_json::from_value(json!({"max_input_tokens": 5})).unwrap());
    }
    let config = (**config).clone();
    let mut written = vec![start, record(json!({"type": "step_started", "step_id": 1}))];
    let (_, _, body) = broca_module::wal_render::render_step_with_media(
        &broca_module::wal_render::OfflineMediaContext {
            state_root,
            identity: &session(),
            class,
            lineage: lineage.as_deref(),
        },
        &written,
        &config,
        "r1",
        1,
        None,
    )
    .unwrap();
    written.extend(
        [
            json!({"type": "model_attempt_finished", "step_id": 1, "attempt": 0,
                "outcome": "complete", "ts_ms": 1_001,
                "request_body_len": body.len(), "request_body_sha256": if holds.legacy_replay_limit { sha256(b"historical request") } else { sha256(&body) },
                "response_body_len": 0, "response_body_sha256": sha256(b"")}),
            json!({"type": "model_step_finished", "step_id": 1,
                "assistant_message": {"message_id": "m1",
                    "content": [{"type": "text", "text": "Seen."}]},
                "usage": {"input_tokens": 3, "output_tokens": 1},
                "finish_reason": "stop", "retries_used": 0}),
            json!({"type": "run_finished", "reason": "completed", "ts_ms": 1_003,
                "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
        ]
        .into_iter()
        .map(record),
    );
    for record in &written {
        wal.append(1, record, true).unwrap();
    }
}

/// A copy of a state root whose media stores exist, as a module's do. `holding` names the
/// classes of store that hold the image's bytes.
fn state_root(root: &Path, holding: &[MediaStoreClass]) -> PathBuf {
    let state = root.join("state");
    std::fs::create_dir_all(&state).unwrap();
    broca_core::media_store::create_media_directories(&state).unwrap();
    let bytes = png();
    for class in holding {
        std::fs::write(
            state
                .join(class.directory())
                .join("objects")
                .join(sha256(&bytes)),
            &bytes,
        )
        .unwrap();
    }
    state
}

/// The log's path in the state root's folder `folder`, named as today's storage names it.
fn log_in(state: &Path, folder: &str) -> PathBuf {
    state
        .join(folder)
        .join(format!("{}.wal", broca_core::session_addr(&session())))
}

/// `move-check` over `files`, with its scratch folder under `root`.
fn checked(root: &Path, files: Vec<PathBuf>) -> MoveCheckReport {
    checked_with_inventory(root, files, None)
}

fn checked_with_inventory(
    root: &Path,
    files: Vec<PathBuf>,
    inventory_root: Option<PathBuf>,
) -> MoveCheckReport {
    check(&Args {
        command: "move-check".into(),
        out_file: None,
        limit: usize::MAX,
        sample: 40,
        only: None,
        prefixes: false,
        max_records: None,
        every: 1,
        all_steps: false,
        inventory_root,
        from_legacy: false,
        keep: false,
        roots: Vec::new(),
        dir: Some(root.join("scratch")),
        files,
    })
}

#[test]
fn inventory_rekeys_are_checked_read_only_and_unresolved_identities_are_named() {
    use broca_core::SessionRefIndex;
    let root = scratch("inventory-rekeys");
    let state = state_root(&root, &[]);
    let effective = BindIdentity::new("/renamed-project", "broca", "s");
    let address = broca_core::session_addr(&effective);
    let log = root.join("set-aside").join(format!("{address}.wal"));
    write_log(&log, Holds::default(), &state, MediaStoreClass::NonDurable);
    let index = state.join("run-index.db");
    let store = broca_store::SqliteStore::open(&index).unwrap();
    store.record_session(&address, &effective).unwrap();
    let connection = rusqlite::Connection::open(&index).unwrap();
    connection
        .execute_batch(
            "INSERT OR REPLACE INTO agent_data_projection VALUES(1,1);
         INSERT OR IGNORE INTO agent_data_census_done VALUES(1);
         UPDATE agent_data_census SET covered=1;",
        )
        .unwrap();
    drop(connection);
    drop(store);
    let before = std::fs::read(&index).unwrap();
    let original = std::fs::read(&log).unwrap();
    let report = checked_with_inventory(&root, vec![log.clone()], Some(state.clone()));
    assert_eq!(
        report.verified,
        1,
        "{}",
        serde_json::to_string(&report).unwrap()
    );
    assert!(report.refused.is_empty());
    assert_eq!(std::fs::read(&index).unwrap(), before);
    assert_eq!(std::fs::read(&log).unwrap(), original);

    let different = root.join("set-aside/different-address.wal");
    std::fs::rename(&log, &different).unwrap();
    let report = checked_with_inventory(&root, vec![different.clone()], Some(state.clone()));
    assert_eq!(report.refused.get("identity_unresolved"), Some(&1));
    assert!(report
        .examples
        .keys()
        .any(|key| key.contains("differs from source address")));
    // Without an inventory the historical first-run identity remains the rehearsal default.
    assert_eq!(checked(&root, vec![different.clone()]).verified, 1);
    let connection = rusqlite::Connection::open(&index).unwrap();
    connection
        .execute("DELETE FROM agent_data_session_ref", [])
        .unwrap();
    let report = checked_with_inventory(&root, vec![different], Some(state.clone()));
    assert_eq!(report.refused.get("identity_unresolved"), Some(&1));
    assert!(report
        .examples
        .keys()
        .any(|key| key.contains("no inventory row")));
    drop(connection);
    std::fs::remove_dir_all(root).unwrap();
}

/// A session holding media is staged with the bytes today's media store holds for it, and
/// verified, its request rendered with them: it moves, rather than being refused
/// `media_missing` for want of a media store.
#[test]
fn a_session_holding_media_is_checked_with_todays_media() {
    let root = scratch("media");
    let state = state_root(&root, &[MediaStoreClass::NonDurable]);
    let log = log_in(&state, "wal");
    let holds = Holds {
        image: true,
        ..Holds::default()
    };
    write_log(&log, holds, &state, MediaStoreClass::NonDurable);
    let report = checked(&root, vec![log]);
    assert_eq!(report.refused, Default::default(), "{:?}", report.examples);
    assert_eq!((report.logs, report.verified), (1, 1));
    assert_eq!(report.verified_media, 1);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn legacy_replay_limits_are_visible_beside_verified_sessions() {
    let root = scratch("legacy-replay-limit");
    let state = state_root(&root, &[]);
    let log = log_in(&state, "wal");
    write_log(
        &log,
        Holds {
            legacy_replay_limit: true,
            ..Holds::default()
        },
        &state,
        MediaStoreClass::NonDurable,
    );
    let report = checked(&root, vec![log]);
    assert_eq!(
        (report.logs, report.verified, report.legacy_replay_limit),
        (1, 1, 1)
    );
    assert!(report.refused.is_empty(), "{:?}", report.examples);
    assert_eq!(report.legacy_replay_limit_examples.len(), 1);
    assert!(report.legacy_replay_limit_examples[0].ends_with(": step 1 of r1"));
    std::fs::remove_dir_all(root).unwrap();
}

/// The media store a session's bytes are read from is the one of the class its log's folder
/// chooses, as a move chooses it: a durable log's image held only by the ordinary store is
/// missing, and read once the durable store holds it.
#[test]
fn the_logs_folder_chooses_the_media_store() {
    let holds = Holds {
        image: true,
        ..Holds::default()
    };
    let root = scratch("media-class");
    let state = state_root(&root, &[MediaStoreClass::NonDurable]);
    let log = log_in(&state, "wal-durable");
    write_log(&log, holds, &state, MediaStoreClass::NonDurable);
    let report = checked(&root, vec![log]);
    assert_eq!(report.refused, [("media_missing".to_owned(), 1)].into());
    let _ = std::fs::remove_dir_all(&root);

    let root = scratch("media-durable");
    let state = state_root(&root, &[MediaStoreClass::Durable]);
    let log = log_in(&state, "wal-durable");
    write_log(&log, holds, &state, MediaStoreClass::Durable);
    let report = checked(&root, vec![log]);
    assert_eq!(report.refused, Default::default(), "{:?}", report.examples);
    assert_eq!((report.verified, report.verified_media), (1, 1));
    let _ = std::fs::remove_dir_all(&root);
}

/// Over several logs, every log that wouldn't move is counted once under its class, beside the
/// verified ones; the features behind `unsupported_feature` are counted by name; and the time
/// each phase took is reported.
#[test]
fn the_report_counts_each_refusal_class_and_times_the_check() {
    let root = scratch("report");
    let state = state_root(&root, &[MediaStoreClass::NonDurable]);
    let verified = log_in(&state, "wal");
    write_log(
        &verified,
        Holds::default(),
        &state,
        MediaStoreClass::NonDurable,
    );
    // A log outside a state root's log folders has no media store to read its image from.
    let corpus = root.join("corpus");
    let missing = corpus.join("missing.wal");
    let image = Holds {
        image: true,
        ..Holds::default()
    };
    write_log(&missing, image, &state, MediaStoreClass::NonDurable);
    let budget = corpus.join("budget.wal");
    let input_budget = Holds {
        input_budget: true,
        ..Holds::default()
    };
    write_log(&budget, input_budget, &state, MediaStoreClass::NonDurable);
    let no_run = corpus.join("no-run.wal");
    broca_wal::FileWal::open(&no_run)
        .unwrap()
        .append(
            1,
            &record(json!({"type": "step_started", "step_id": 1})),
            true,
        )
        .unwrap();
    // A folder where a log was expected: naming it reads nothing.
    let unreadable = corpus.join("folder.wal");
    std::fs::create_dir_all(&unreadable).unwrap();
    let report = checked(&root, vec![verified, missing, budget, no_run, unreadable]);
    assert_eq!(report.mode, "move");
    assert_eq!((report.logs, report.verified), (5, 1));
    assert_eq!(
        report.refused,
        [
            ("media_missing".to_owned(), 1),
            ("no_run".to_owned(), 1),
            ("unreadable".to_owned(), 1),
            ("unsupported_feature".to_owned(), 1),
        ]
        .into()
    );
    assert_eq!(
        report.unsupported_features,
        [("input_token_budget".to_owned(), 1)].into()
    );
    let timing = &report.timing;
    assert!(
        timing.stage_us > 0 && timing.verify_us > 0,
        "{:?}",
        timing.stage_us
    );
    assert!(timing.per_verified_session_us.max >= timing.verify_us);
    assert!(timing.wall_us >= timing.stage_us + timing.verify_us);
    let _ = std::fs::remove_dir_all(&root);
}
