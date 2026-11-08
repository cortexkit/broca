//! The staging writer and the verifier together: a session staged from its WAL verifies against
//! it, and a difference planted on either side fails the check that compares it.

use std::path::{Path, PathBuf};

use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, Wal, WalLineageId, WalRecord};
use futures::executor::block_on;
use serde_json::json;

use crate::stage::{stage, Stage, StageError, Staged};
use crate::verify::{verify, Mode, Verified, Verify};

fn session() -> BindIdentity {
    BindIdentity::new("/project", "runner", "s")
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[0x5a; 32])
}

fn scratch(tag: &str) -> PathBuf {
    let root = broca_test_support::scratch_root().join(format!(
        "session-convert-stage-{}-{tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

/// A run start whose frozen render config renders through today's Anthropic family.
fn run_started(run: &str, at: u64, input: &str) -> WalRecord {
    let frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    let mut record: WalRecord = serde_json::from_value(json!({
        "type": "run_started", "run_id": run, "ts_ms": at,
        "session": {"project_root": "/project", "harness": "runner", "session": "s"},
        "config": {
            "model": {"provider_module_id": "anthropic", "model_id": "claude-test"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0,
            "renderer_version": frozen.renderer_version,
            "frozen_render_config": serde_json::to_value(&frozen).unwrap(),
        },
        "input": [{"role": "user", "content": [{"type": "text", "text": input}]}],
        "origin": {"kind": "fresh"},
    }))
    .unwrap();
    if let WalRecord::RunStarted { config, .. } = &mut record {
        config.frozen_render_config = serde_json::to_value(&frozen).unwrap();
    }
    record
}

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

/// Writes a WAL of two runs, each one model step answering in text, whose attempts recorded
/// the digest of the request today's renderer builds for them, and reads it back.
fn written(root: &Path) -> (Vec<FramedRecord>, Option<WalLineageId>) {
    let path = root.join("session.wal");
    let mut wal = broca_wal::FileWal::open(&path).unwrap();
    let lineage = wal.lineage_id();
    let mut written: Vec<WalRecord> = Vec::new();
    let mut append = |record: WalRecord, written: &mut Vec<WalRecord>| {
        wal.append(1, &record, true).unwrap();
        written.push(record);
    };
    for (run, step, at, input, answer) in [
        ("r1", 1u64, 1_000u64, "Hello.", "One."),
        ("r2", 2, 2_000, "Again.", "Two."),
    ] {
        let start = run_started(run, at, input);
        let WalRecord::RunStarted { config, .. } = &start else {
            unreachable!()
        };
        let config = (**config).clone();
        append(start, &mut written);
        append(
            record(json!({"type": "step_started", "step_id": step})),
            &mut written,
        );
        let (_, _, body) = broca_module::wal_render::render_step_with_lineage(
            lineage.map(|id| id.to_string()).as_deref(),
            &written,
            &config,
            run,
            step,
            None,
            None,
        )
        .unwrap();
        append(
            record(json!({
                "type": "model_attempt_finished", "step_id": step, "attempt": 0,
                "outcome": "complete", "ts_ms": at + 1,
                "request_body_len": body.len(),
                "request_body_sha256": crate::verify::sha256_hex(&body),
                "response_body_len": 0,
                "response_body_sha256": crate::verify::sha256_hex(b""),
            })),
            &mut written,
        );
        append(
            record(json!({
                "type": "model_step_finished", "step_id": step,
                "assistant_message": {"message_id": format!("m{step}"),
                    "content": [{"type": "text", "text": answer}]},
                "usage": {"input_tokens": 3, "output_tokens": 1},
                "finish_reason": "stop", "retries_used": 0,
            })),
            &mut written,
        );
        append(
            record(json!({
                "type": "run_finished", "reason": "completed", "ts_ms": at + 3,
                "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0,
            })),
            &mut written,
        );
    }
    drop(wal);
    let read =
        broca_wal::read_records_range(&path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
            .unwrap();
    let broca_wal::WalRangeRecords::Complete(records) = read.records() else {
        panic!("the WAL reads back whole")
    };
    (records.to_vec(), read.lineage_id)
}

fn staged(root: &Path, records: &[FramedRecord], lineage: Option<WalLineageId>) -> Staged {
    staged_in(root, records, lineage, &deployment())
}

/// Stages the session with `deployment`, which creates it in its mode.
fn staged_in(
    root: &Path,
    records: &[FramedRecord],
    lineage: Option<WalLineageId>,
    deployment: &DeploymentKey,
) -> Staged {
    let lineage = lineage.map(|id| id.to_string());
    block_on(stage(Stage {
        records,
        lineage: lineage.as_deref(),
        session: &session(),
        deployment,
        store_root: &root.join("session-store"),
        staging: &root.join("session-store/staging/store-move-test"),
    }))
    .unwrap()
}

fn verified(
    root: &Path,
    records: &[FramedRecord],
    lineage: Option<WalLineageId>,
    converted: &crate::Converted,
    mode: Mode,
) -> Result<Verified, crate::verify::Mismatch> {
    verified_in(root, records, lineage, converted, mode, &deployment())
}

/// Verifies the staged session with `deployment`, whose mode the session must be stored in.
fn verified_in(
    root: &Path,
    records: &[FramedRecord],
    lineage: Option<WalLineageId>,
    converted: &crate::Converted,
    mode: Mode,
    deployment: &DeploymentKey,
) -> Result<Verified, crate::verify::Mismatch> {
    block_on(verify(Verify {
        records,
        lineage,
        session: &session(),
        deployment,
        store_root: &root.join("session-store"),
        staging: &root.join("session-store/staging/store-move-test"),
        converted,
        mode,
    }))
}

/// A session is staged in the mode its deployment key creates sessions in, its setup parts in
/// that mode's setup store, and it reloads and verifies in that mode, plain and encrypted alike.
/// Read back expecting the other mode, it fails the staged-session check by name.
#[test]
fn a_session_is_staged_and_verified_in_the_mode_new_sessions_take() {
    use broca_session_store::keys::Cipher;
    for cipher in Cipher::ALL {
        let root = scratch(&format!("mode-{}", cipher.label()));
        let (records, lineage) = written(&root);
        let creating = deployment().creating(cipher);
        let staged = staged_in(&root, &records, lineage, &creating);
        let staging = root.join("session-store/staging/store-move-test");
        let view = block_on(Session::view(
            LocalBackend::new(&staging),
            SessionAddr(broca_core::session_addr(&session())),
            &crate::stage::triple(&session()),
            &creating,
        ))
        .unwrap();
        assert_eq!(view.head().cipher(), cipher);
        drop(view);
        let setup = match cipher {
            Cipher::XChaCha20Poly1305 => "setup",
            Cipher::None => "setup-plain",
        };
        assert!(
            root.join("session-store").join(setup).exists(),
            "{} parts in {setup}",
            cipher.label()
        );
        let verified = verified_in(
            &root,
            &records,
            lineage,
            &staged.converted,
            Mode::Move,
            &creating,
        )
        .unwrap();
        assert_eq!((verified.runs, verified.messages), (2, 4));
        assert!(verified.checks.contains(&"next_request"));
        let other = match cipher {
            Cipher::XChaCha20Poly1305 => Cipher::None,
            Cipher::None => Cipher::XChaCha20Poly1305,
        };
        let failed = verified_in(
            &root,
            &records,
            lineage,
            &staged.converted,
            Mode::Move,
            &deployment().creating(other),
        )
        .unwrap_err();
        assert_eq!(failed.check, "staged", "{failed}");
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// A staged session is a normal session of the new storage at the session's own address, its
/// setup parts in the shared setup store, and it agrees with its WAL on every check, the
/// offline mode's every recorded request included.
#[test]
fn a_staged_session_agrees_with_its_wal_on_every_check() {
    let root = scratch("agrees");
    let (records, lineage) = written(&root);
    let staged = staged(&root, &records, lineage);
    assert_eq!(staged.runs, 2);
    assert_eq!(staged.messages, 4);
    assert!(
        staged.appends_in_conversation,
        "the WAL is off the contract"
    );
    assert!(root.join("session-store/setup").exists());
    let staging = root.join("session-store/staging/store-move-test");
    let addr = broca_core::session_addr(&session());
    assert!(staging.join("heads").join(&addr).join("head").exists());
    assert!(
        !root.join("session-store/heads").exists(),
        "nothing is published"
    );

    let verified = verified(&root, &records, lineage, &staged.converted, Mode::Offline).unwrap();
    assert_eq!(
        verified.checks,
        [
            "run_status",
            "run_result",
            "send_status",
            "read",
            "history_raw",
            "history_display",
            "entries",
            "head",
            "billing",
            "media",
            "last_step",
            "first_open",
            "next_request",
            "all_steps",
        ]
    );
    assert_eq!(
        (verified.runs, verified.messages, verified.media),
        (2, 4, 0)
    );
    assert!(
        !staging.join("next-request").exists(),
        "the next request's copy is gone"
    );
    // Today's storage records no marks of a message: every entry's are absent, not empty.
    let view = block_on(Session::view(
        LocalBackend::new(&staging),
        SessionAddr(addr),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap();
    let entries = block_on(broca_module::store_entries::all_entries(&*view)).unwrap();
    assert_eq!(entries.len(), 4);
    for record in &staged.converted.records {
        if let Record::EntryFacts(facts) = record {
            assert_eq!(facts.marks, None);
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// A difference planted in the WAL the staged session is checked against fails the check that
/// compares it: a changed answer fails `run.result`, a changed prompt fails `session.read`.
#[test]
fn a_planted_difference_fails_the_check_that_compares_it() {
    let root = scratch("planted");
    let (records, lineage) = written(&root);
    let staged = staged(&root, &records, lineage);
    let planted = |change: &dyn Fn(&mut WalRecord) -> bool| {
        let mut planted = records.clone();
        assert!(planted.iter_mut().any(|frame| change(&mut frame.record)));
        planted
    };
    let answer = planted(&|record| match record {
        WalRecord::ModelStepFinished {
            assistant_message, ..
        } => {
            std::sync::Arc::make_mut(assistant_message).content =
                vec![serde_json::from_value(json!({"type": "text", "text": "Changed."})).unwrap()];
            true
        }
        _ => false,
    });
    let failed = verified(&root, &answer, lineage, &staged.converted, Mode::Move).unwrap_err();
    assert_eq!(failed.check, "run_result", "{failed}");

    let prompt = planted(&|record| match record {
        WalRecord::RunStarted { input, .. } => {
            *input = std::sync::Arc::new(vec![serde_json::from_value(json!({
                "role": "user", "content": [{"type": "text", "text": "Changed."}],
            }))
            .unwrap()]);
            true
        }
        _ => false,
    });
    let failed = verified(&root, &prompt, lineage, &staged.converted, Mode::Move).unwrap_err();
    assert_eq!(failed.check, "read", "{failed}");
    let _ = std::fs::remove_dir_all(&root);
}

/// A session that names a medium isn't staged until the new storage can store input media:
/// it is refused by name, and nothing is written.
#[test]
fn a_session_holding_media_is_refused_before_anything_is_written() {
    let root = scratch("media");
    let (mut records, lineage) = written(&root);
    if let WalRecord::RunStarted { input, .. } = &mut records[0].record {
        *input = std::sync::Arc::new(vec![serde_json::from_value(json!({
            "role": "user", "content": [{"type": "media", "kind": "image",
                "mime": "image/png", "byte_size": 3, "width": 1, "height": 1,
                "sha256": "a".repeat(64)}],
        }))
        .unwrap()]);
    }
    let lineage = lineage.map(|id| id.to_string());
    let refused = block_on(stage(Stage {
        records: &records,
        lineage: lineage.as_deref(),
        session: &session(),
        deployment: &deployment(),
        store_root: &root.join("session-store"),
        staging: &root.join("session-store/staging/store-move-test"),
    }))
    .unwrap_err();
    assert_eq!(refused, StageError::NotMovable("media"));
    assert!(!root.join("session-store").exists());
    let _ = std::fs::remove_dir_all(&root);
}

/// The WAL's own preconditions name open work and a tool batch left open, and pass a session
/// whose runs all ended.
#[test]
fn the_wal_preconditions_name_open_work_and_an_open_batch() {
    use crate::preconditions::{converted_refusal, wal_refusal, OPEN_TOOL_BATCH, SESSION_BUSY};
    let root = scratch("preconditions");
    let (records, lineage) = written(&root);
    assert_eq!(wal_refusal(&records, lineage), None);
    let prepared =
        crate::stage::prepare(&records, lineage.map(|id| id.to_string()).as_deref()).unwrap();
    assert_eq!(block_on(converted_refusal(&prepared)).unwrap(), None);
    // The last run hasn't ended.
    assert_eq!(
        wal_refusal(&records[..records.len() - 1], lineage),
        Some(SESSION_BUSY)
    );
    // A batch started and never closed, in a run that ended.
    let mut open = records.clone();
    let last = open.len() - 1;
    open.insert(
        last,
        FramedRecord {
            seq: 0,
            fence: 1,
            record: record(json!({
                "type": "tool_batch_started", "batch_id": "b1", "step_id": 2,
                "planned_calls": [],
            })),
        },
    );
    assert_eq!(wal_refusal(&open, lineage), Some(OPEN_TOOL_BATCH));
    let _ = std::fs::remove_dir_all(&root);
}
