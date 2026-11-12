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
    written_with(root, false, &["Hello.", "Again."])
}

/// [`written`] with a run for each of `inputs`, or with `tagged` a WAL whose runner tags are
/// stored on its records, as today's writer stamps them on a lineage that numbers its parts, each
/// run's input one message of two text parts.
fn written_with(
    root: &Path,
    tagged: bool,
    inputs: &[&str],
) -> (Vec<FramedRecord>, Option<WalLineageId>) {
    let path = root.join("session.wal");
    // Stored runner tags are a WAL feature the log's floor must name before they're written.
    let mut wal = broca_wal::FileWal::open(&path)
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    let lineage = wal.lineage_id();
    let mut written: Vec<WalRecord> = Vec::new();
    let mut tags = broca_core::tags::LineageTags::unstarted();
    let mut append = |mut record: WalRecord, written: &mut Vec<WalRecord>| {
        if tagged {
            broca_core::tags::stamp(&mut tags, &mut record);
        }
        wal.append(1, &record, true).unwrap();
        written.push(record);
    };
    let answers = ["One.", "Two.", "Three.", "Four.", "Five.", "Six."];
    for (index, (input, answer)) in inputs.iter().zip(answers).enumerate() {
        let step = index as u64 + 1;
        let at = step * 1_000;
        let run = format!("r{step}");
        let run = run.as_str();
        let input = *input;
        let mut start = run_started(run, at, input);
        if tagged {
            if let WalRecord::RunStarted { input: parts, .. } = &mut start {
                *parts = std::sync::Arc::new(vec![serde_json::from_value(json!({
                    "role": "user",
                    "content": [{"type": "text", "text": input}, {"type": "text", "text": "More."}],
                }))
                .unwrap()]);
            }
        }
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

#[test]
fn staging_retains_only_mismatched_run_identity_provenance() {
    let root = scratch("run-session-provenance");
    let (mut records, lineage) = written(&root);
    let historical = BindIdentity::new("/historical-project", "runner", "s");
    let mut starts = records
        .iter_mut()
        .filter_map(|frame| match &mut frame.record {
            WalRecord::RunStarted { session, .. } => Some(session),
            _ => None,
        });
    *starts.nth(1).unwrap() = historical.clone();
    let staged = staged(&root, &records, lineage);
    let starts: Vec<_> = staged
        .converted
        .records
        .iter()
        .filter_map(|record| match record {
            broca_session_store::record::Record::RunStart(start) => Some(start),
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 2);
    let provenance = |start: &broca_session_store::record::RunStart| {
        start
            .run_choices
            .as_ref()
            .and_then(|choices| choices.pointer("/record/session"))
            .cloned()
    };
    assert_eq!(provenance(starts[0]), None);
    assert_eq!(
        provenance(starts[1]),
        Some(serde_json::to_value(historical).unwrap())
    );
    std::fs::remove_dir_all(root).unwrap();
}

fn staged(root: &Path, records: &[FramedRecord], lineage: Option<WalLineageId>) -> Staged {
    staged_in(root, records, lineage, &deployment())
}

/// The last update today's head serves for the session whose WAL is `records`, read with no
/// file behind it.
fn todays_updated_at(records: &[FramedRecord]) -> u64 {
    broca_module::head_time::updated_at_of_wal(records, None)
}

/// A run of a log written before its records carried times: none of them says when.
fn timeless_run(run: &str) -> Vec<WalRecord> {
    let mut start = run_started(run, 0, "Hello.");
    if let WalRecord::RunStarted { ts_ms, .. } = &mut start {
        *ts_ms = None;
    }
    let mut records = vec![start];
    records.extend(
        [
            json!({"type": "step_started", "step_id": 1}),
            json!({"type": "model_step_finished", "step_id": 1,
                "assistant_message": {"message_id": "m1",
                    "content": [{"type": "text", "text": "One."}]},
                "usage": {"input_tokens": 3, "output_tokens": 1},
                "finish_reason": "stop", "retries_used": 0}),
            json!({"type": "run_finished", "reason": "completed",
                "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
        ]
        .into_iter()
        .map(record),
    );
    records
}

fn framed(records: Vec<WalRecord>) -> Vec<FramedRecord> {
    (1u64..)
        .zip(records)
        .map(|(seq, record)| FramedRecord {
            seq,
            fence: 1,
            record,
        })
        .collect()
}

/// The last update the head of the session `staging` holds serves, read as the module reads
/// it, or why it can't say.
fn staged_updated_at(
    staging: &Path,
    converted: &crate::Converted,
) -> Result<u64, broca_module::store_head::StoreHeadError> {
    let view = block_on(Session::view(
        LocalBackend::new(staging),
        SessionAddr(broca_core::session_addr(&session())),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap();
    let parts = broca_session_loop::parts::MemoryParts {
        parts: converted.parts.clone(),
    };
    block_on(broca_module::store_head::build(&view, &parts, false))
        .map(|head| head.reply.updated_at.expect("a built head has a time"))
}

#[test]
fn a_converted_session_without_a_move_keeps_its_record_time_across_a_seal() {
    let root = scratch("unmoved-time");
    let (records, lineage) = written(&root);
    let expected = broca_module::head_time::newest_record_ms(&records).unwrap();
    let lineage = lineage.map(|id| id.to_string());
    let converted = crate::convert(&records, lineage.as_deref()).unwrap();
    let backend = broca_session_store::backend::MemoryBackend::default();
    let mut store = block_on(Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(&session())),
        crate::stage::triple(&session()),
        &deployment(),
        true,
    ))
    .unwrap();
    block_on(store.append(&converted.records, true)).unwrap();
    let parts = broca_session_loop::parts::MemoryParts {
        parts: converted.parts,
    };
    let before = block_on(broca_module::store_head::build(&store, &parts, false)).unwrap();
    assert_eq!(before.reply.updated_at, Some(expected));
    block_on(store.seal(None, expected + 100)).unwrap();
    block_on(store.close()).unwrap();
    let store = block_on(Session::view(
        &backend,
        SessionAddr(broca_core::session_addr(&session())),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap();
    let after = block_on(broca_module::store_head::build(&store, &parts, false)).unwrap();
    assert_eq!(after.reply.updated_at, Some(expected));
    let _ = std::fs::remove_dir_all(&root);
}

/// A moved session's head goes on serving the last update today's head served for it, which
/// its `Moved` record carries: for a log none of whose records carries a time too, which the
/// new storage could otherwise only refuse (`session_time_unknown`). Converted record times
/// do not override that frozen value, but records written after the move advance it.
#[test]
fn a_moved_sessions_head_keeps_todays_last_update() {
    use broca_session_store::record::{Moved, Record};
    let root = scratch("moved-time");
    let staging = root.join("session-store/staging/store-move-test");
    // Today's head serves the live WAL file's write time for a log whose records say nothing.
    let timeless = framed(timeless_run("r1"));
    let today = broca_module::head_time::updated_at_of_wal(&timeless, Some(1_234_000));
    assert_eq!(today, 1_234_000);
    let moved = Moved {
        move_id: "store-move:test".into(),
        source_lineage: None,
        source_activity_ms: today,
    };
    let stage_with_moved = |records: &[FramedRecord], moved: &Moved| {
        let _ = std::fs::remove_dir_all(&root);
        block_on(stage(Stage {
            source_history_version: 1,
            records,
            lineage: None,
            session: &session(),
            deployment: &deployment(),
            store_root: &root.join("session-store"),
            staging: &staging,
            media: None,
            moved,
        }))
        .unwrap()
    };
    let staged = stage_with_moved(&timeless, &moved);
    assert_eq!(
        staged_updated_at(&staging, &staged.converted).unwrap(),
        today
    );

    // Today's head prefers the live WAL file's time even when a record's clock is ahead.
    let (timed, lineage) = written(&root.join("timed"));
    let newest = broca_module::head_time::newest_record_ms(&timed).unwrap();
    let earlier = Moved {
        move_id: "store-move:test".into(),
        source_lineage: None,
        source_activity_ms: newest - 1,
    };
    let _ = lineage;
    let staged = stage_with_moved(&timed, &earlier);
    assert_eq!(
        staged_updated_at(&staging, &staged.converted).unwrap(),
        earlier.source_activity_ms
    );
    // The frozen source can also be earlier than the latest run's start. Neither that start
    // nor transcript records copied by a seal may replace the value today's head selected.
    let before_start = Moved {
        source_activity_ms: 1,
        ..earlier
    };
    let staged = stage_with_moved(&timed, &before_start);
    assert_eq!(staged_updated_at(&staging, &staged.converted).unwrap(), 1);
    let mut stored = block_on(Session::load(
        LocalBackend::new(&staging),
        SessionAddr(broca_core::session_addr(&session())),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap();
    block_on(stored.seal(None, newest + 100)).unwrap();
    block_on(stored.close()).unwrap();
    assert_eq!(staged_updated_at(&staging, &staged.converted).unwrap(), 1);

    // A later run on the new storage advances it past the move's time.
    let mut both = timeless_run("r1");
    let mut later = run_started("r2", 2_000_000, "Again.");
    if let WalRecord::RunStarted { written_at_ms, .. } = &mut later {
        *written_at_ms = Some(2_000_000);
    }
    both.push(later);
    let both = framed(both);
    let converted = crate::convert(&both, None).unwrap();
    let split = converted
        .records
        .iter()
        .rposition(|record| matches!(record, Record::RunStart(_)))
        .unwrap();
    let mut records = converted.records[..split].to_vec();
    records.push(Record::Moved(moved.clone()));
    records.extend_from_slice(&converted.records[split..]);
    let backend = broca_session_store::backend::MemoryBackend::default();
    let mut store = block_on(Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(&session())),
        crate::stage::triple(&session()),
        &deployment(),
        true,
    ))
    .unwrap();
    block_on(store.append(&records, false)).unwrap();
    let parts = broca_session_loop::parts::MemoryParts {
        parts: converted.parts.clone(),
    };
    let head = block_on(broca_module::store_head::build(&store, &parts, false)).unwrap();
    assert_eq!(head.reply.updated_at, Some(2_000_000));
    block_on(store.seal(None, 3_000_000)).unwrap();
    block_on(store.close()).unwrap();
    let store = block_on(Session::view(
        &backend,
        SessionAddr(broca_core::session_addr(&session())),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap();
    let head = block_on(broca_module::store_head::build(&store, &parts, false)).unwrap();
    assert_eq!(head.reply.updated_at, Some(2_000_000));
    let _ = std::fs::remove_dir_all(&root);
}

/// The record a move of the session whose WAL is `records` writes.
fn moved(records: &[FramedRecord]) -> broca_session_store::record::Moved {
    broca_session_store::record::Moved {
        move_id: "store-move:test".into(),
        source_lineage: None,
        source_activity_ms: todays_updated_at(records),
    }
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
        source_history_version: 1,
        records,
        lineage: lineage.as_deref(),
        session: &session(),
        deployment,
        store_root: &root.join("session-store"),
        staging: &root.join("session-store/staging/store-move-test"),
        media: None,
        moved: &moved(records),
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
        media: None,
        media_root: None,
        todays_updated_at: todays_updated_at(records),
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

/// A session holding a message too large to serve verifies. Today's storage and the new one both
/// refuse the page that holds it, and each refusal counts the whole page's size. That size
/// includes the members a move changes by design (the head, incarnation and history version), so
/// the two refusals differ by exactly what the pages served before it differ by.
#[test]
fn a_message_too_large_to_serve_is_refused_alike() {
    let root = scratch("too-large");
    let large = "x".repeat(5 << 20);
    let (records, lineage) = written_with(
        &root,
        false,
        &["Hello.", "Again.", "Third.", "Fourth.", &large],
    );
    let staged = staged(&root, &records, lineage);
    let verified = verified(&root, &records, lineage, &staged.converted, Mode::Move).unwrap();
    assert!(verified.checks.contains(&"read"));
    assert!(verified.checks.contains(&"history_display"));
    let _ = std::fs::remove_dir_all(&root);
}

/// A session on a lineage whose runner tags are stored on its records verifies, the next send's
/// request included. The next send repeats the session's last run start with a one-part input,
/// numbered on from the lineage's last part as today's writer numbers an appended run start, not
/// with the tags of the two-part input it repeats, which don't cover its parts.
#[test]
fn a_session_with_stored_runner_tags_verifies_its_next_send() {
    let root = scratch("stored-tags");
    let (records, lineage) = written_with(&root, true, &["Hello.", "Again."]);
    assert!(
        records.iter().any(|frame| matches!(
            &frame.record,
            WalRecord::RunStarted { tags: Some(tags), .. } if tags[0].parts.len() == 2
        )),
        "the run starts carry tags for their two parts"
    );
    let staged = staged(&root, &records, lineage);
    let verified = verified(&root, &records, lineage, &staged.converted, Mode::Move).unwrap();
    assert!(verified.checks.contains(&"next_request"));
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

fn delivered_messages_rebuild_after_staging(mode: &str) {
    let root = scratch(&format!("delivery-receipt-{mode}"));
    let records = framed(vec![
        run_started("r1", 1_000, "Start."),
        record(
            json!({"type": "send_delivered", "submission_id": "q1", "run_id": "r1",
            "mode": mode, "input": [{"role": if mode == "turn_system" { "system" } else { "user" },
                "content": [{"type": "text", "text": "Continue."}]}]}),
        ),
        record(json!({"type": "step_started", "step_id": 1})),
        record(json!({"type": "model_step_finished", "step_id": 1,
            "assistant_message": {"message_id": "m1", "content": [{"type": "text", "text": "Done."}]},
            "usage": {}, "retries_used": 0})),
        record(
            json!({"type": "run_finished", "reason": "completed", "usage": {}, "retries_used": 0}),
        ),
    ]);
    staged(&root, &records, None);
    let staging = root.join("session-store/staging/store-move-test");
    let view = block_on(Session::view(
        LocalBackend::new(&staging),
        SessionAddr(broca_core::session_addr(&session())),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap();
    let last = view.next_seq() - 1;
    let history = block_on(view.media_history(last));
    assert!(
        history.is_ok(),
        "the stored session must fold for media: {history:?}"
    );
    drop(view);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_delivered_steers_receipt_survives_staging_and_the_media_fold() {
    delivered_messages_rebuild_after_staging("steer");
}

#[test]
fn a_turn_system_deliverys_receipt_survives_staging_and_the_media_fold() {
    delivered_messages_rebuild_after_staging("turn_system");
}

#[test]
fn conversion_rebasing_keeps_legacy_views_at_their_storage_sequences() {
    let root = scratch("legacy-view-origin");
    let (records, lineage) = written(&root);
    let lineage = lineage.map(|id| id.to_string());
    let mut converted = crate::convert(&records, lineage.as_deref()).unwrap();
    // Legacy-view pointers already account for the destination's initial WriterOpened record.
    // Receipts instead count from 1 when inverse::invert reconstructs the original WAL before
    // storage. Keep the legacy-view pointers' existing origin when storing.
    let view_seq = crate::FIRST_SEQ + 3;
    let Record::EntryFacts(facts) = converted
        .records
        .iter_mut()
        .find(|record| matches!(record, Record::EntryFacts(_)))
        .unwrap()
    else {
        unreachable!()
    };
    facts.legacy_view_seq = Some(view_seq);
    for (first, expected) in [
        (crate::FIRST_SEQ, view_seq),
        (crate::FIRST_SEQ + 7, view_seq + 7),
        (crate::FIRST_SEQ, view_seq),
    ] {
        converted.start_at(first);
        let recorded = converted.records.iter().find_map(|record| match record {
            Record::EntryFacts(facts) => facts.legacy_view_seq,
            _ => None,
        });
        assert_eq!(recorded, Some(expected));
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_legacy_request_replay_limit_is_verified_and_counted_once() {
    let root = scratch("legacy-replay-limit");
    let (mut records, lineage) = written(&root);
    let attempt = records
        .iter_mut()
        .rev()
        .find_map(|frame| match &mut frame.record {
            WalRecord::ModelAttemptFinished {
                request_body_sha256,
                ..
            } => Some(request_body_sha256),
            _ => None,
        })
        .unwrap();
    *attempt =
        crate::verify::sha256_hex(b"a historical request that offline replay cannot reproduce");
    let staged = staged(&root, &records, lineage);
    let summary = verified(&root, &records, lineage, &staged.converted, Mode::Offline).unwrap();
    let summary = serde_json::to_value(summary).unwrap();
    assert_eq!(summary["legacy_replay_limit"], 1);
    assert_eq!(
        summary["legacy_replay_limit_examples"],
        json!([{"run_id": "r2", "step": 2}])
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn request_comparison_never_accepts_conversion_drift() {
    let today = b"today's independently rendered request";
    let digest = crate::verify::sha256_hex(today);
    assert_eq!(
        crate::verify::compare_request(today, today, &digest),
        Ok(false)
    );
    assert_eq!(
        crate::verify::compare_request(today, today, "historical"),
        Ok(true)
    );
    for recorded in [digest.as_str(), "historical"] {
        assert_eq!(
            crate::verify::compare_request(b"different request", today, recorded),
            Err("the rebuilt request differs from today's render")
        );
    }
}

/// Media bytes by digest, as today's media store would give them back: a medium it holds whole,
/// one whose bytes no longer match their digest, or none.
#[derive(Debug, Default)]
struct Media {
    whole: std::collections::BTreeMap<String, Vec<u8>>,
    corrupt: std::collections::BTreeSet<String>,
}

impl broca_core::media_store::MediaBytes for Media {
    fn read_verified(
        &self,
        reference: &broca_protocol::MediaRef,
    ) -> Result<Vec<u8>, broca_core::media_store::MediaReadError> {
        use broca_core::media_store::MediaReadError;
        if self.corrupt.contains(&reference.sha256) {
            return Err(MediaReadError::Corrupt(reference.sha256.clone()));
        }
        self.whole
            .get(&reference.sha256)
            .cloned()
            .ok_or_else(|| MediaReadError::Missing(reference.sha256.clone()))
    }
}

fn sha256(bytes: &[u8]) -> String {
    crate::verify::sha256_hex(bytes)
}

/// The reference of an image holding `bytes`.
fn image_ref(bytes: &[u8]) -> serde_json::Value {
    json!({"kind": "image", "mime": "image/png", "byte_size": bytes.len(), "width": 1,
        "height": 1, "sha256": sha256(bytes)})
}

/// An image block holding `bytes`.
fn image(bytes: &[u8]) -> serde_json::Value {
    let mut block = image_ref(bytes);
    block["type"] = json!("media");
    block
}

/// The WAL of [`written`], its first run's input holding `images` beside its text.
fn with_images(root: &Path, images: &[&[u8]]) -> (Vec<FramedRecord>, Option<WalLineageId>) {
    let (mut records, lineage) = written(root);
    if let WalRecord::RunStarted { input, .. } = &mut records[0].record {
        let mut content = vec![json!({"type": "text", "text": "Hello."})];
        content.extend(images.iter().map(|bytes| image(bytes)));
        *input = std::sync::Arc::new(vec![serde_json::from_value(json!({
            "role": "user", "content": content,
        }))
        .unwrap()]);
    }
    (records, lineage)
}

fn stage_with(
    root: &Path,
    records: &[FramedRecord],
    lineage: Option<WalLineageId>,
    media: &Media,
) -> Result<Staged, StageError> {
    let lineage = lineage.map(|id| id.to_string());
    block_on(stage(Stage {
        source_history_version: 1,
        records,
        lineage: lineage.as_deref(),
        session: &session(),
        deployment: &deployment(),
        store_root: &root.join("session-store"),
        staging: &root.join("session-store/staging/store-move-test"),
        media: Some(media),
        moved: &moved(records),
    }))
}

/// A session naming a medium today's media store doesn't hold, or holds with bytes that no
/// longer match its digest, is refused as `media_missing`, naming the first such medium, whether
/// it is absent or corrupt, and how many there are; nothing is written.
#[test]
fn a_session_whose_media_cant_be_read_is_refused_before_anything_is_written() {
    let (one, two): (&[u8], &[u8]) = (b"first image", b"second image");
    let (first, second) = if sha256(one) < sha256(two) {
        (one, two)
    } else {
        (two, one)
    };
    for (state, media) in [
        ("absent", Media::default()),
        (
            "corrupt",
            Media {
                corrupt: [sha256(first), sha256(second)].into(),
                ..Media::default()
            },
        ),
    ] {
        let root = scratch(&format!("media-{state}"));
        let (records, lineage) = with_images(&root, &[first, second]);
        let refused = stage_with(&root, &records, lineage, &media).unwrap_err();
        assert_eq!(
            refused,
            StageError::MediaMissing(crate::stage::MediaMissing {
                digest: sha256(first),
                state,
                missing: 2,
            })
        );
        assert!(!root.join("session-store").exists(), "{state}");
        // With no media store at all, every medium is absent.
        let lineage_text = lineage.map(|id| id.to_string());
        let none = crate::stage::prepare(&records, lineage_text.as_deref(), None).unwrap_err();
        assert!(
            matches!(&none, StageError::MediaMissing(missing) if missing.state == "absent"),
            "{none:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// A session holding media is staged with an object for each, which reads back as today's bytes
/// on the verifier's media check; a staged object whose bytes differ fails that check by name.
#[test]
fn a_session_holding_media_is_staged_with_its_bytes() {
    let root = scratch("media-staged");
    let (one, two): (&[u8], &[u8]) = (b"first image", b"second image");
    let (records, lineage) = with_images(&root, &[one, two]);
    let media = Media {
        whole: [(sha256(one), one.to_vec()), (sha256(two), two.to_vec())].into(),
        ..Media::default()
    };
    let staged = stage_with(&root, &records, lineage, &media).unwrap();
    assert_eq!(staged.media, 2);
    let staging = root.join("session-store/staging/store-move-test");
    let view = block_on(Session::view(
        LocalBackend::new(&staging),
        SessionAddr(broca_core::session_addr(&session())),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap();
    for bytes in [one, two] {
        let named = block_on(view.named_media(&sha256(bytes)))
            .unwrap()
            .expect("the staged session names the medium");
        assert!(named.stored(), "an object holds it");
        assert_eq!(block_on(view.named_media_bytes(&named)).unwrap(), bytes);
    }
    drop(view);
    let check = |media: &Media| {
        block_on(verify(Verify {
            records: &records,
            lineage,
            session: &session(),
            deployment: &deployment(),
            store_root: &root.join("session-store"),
            staging: &staging,
            converted: &staged.converted,
            mode: Mode::Move,
            media: Some(media),
            media_root: None,
            todays_updated_at: todays_updated_at(&records),
        }))
        .unwrap_err()
    };
    // The media agree, so the checks past them run. Without today's media root, the last step
    // cannot compare its converted request with today's offline media render.
    let past = check(&media);
    assert_eq!(past.check, crate::verify::LAST_STEP, "{past}");
    let mut changed = media;
    changed.whole.insert(sha256(two), b"other bytes".to_vec());
    let failed = check(&changed);
    assert_eq!(failed.check, crate::verify::MEDIA, "{failed}");
    let _ = std::fs::remove_dir_all(&root);
}

/// A tool's image is staged as the object of the tool result naming it, and reads back as
/// today's bytes on the verifier's media check.
#[test]
fn a_tool_results_image_is_staged_with_its_bytes() {
    let root = scratch("media-tool");
    let (mut records, lineage) = written(&root);
    let shot: &[u8] = b"a screenshot";
    let last = records.last().unwrap().seq;
    let run = [
        json!({"type": "step_started", "step_id": 3}),
        json!({"type": "model_step_finished", "step_id": 3,
            "assistant_message": {"message_id": "m3", "content": [{"type": "tool_call",
                "tool_call_id": "c1", "tool_name": "photo", "input": {}}]},
            "usage": {"input_tokens": 3, "output_tokens": 1},
            "finish_reason": "tool_calls", "retries_used": 0}),
        json!({"type": "tool_batch_started", "batch_id": "b1", "step_id": 3,
            "planned_calls": ["c1"]}),
        json!({"type": "tool_dispatch_intent", "batch_id": "b1", "tool_call_id": "c1",
            "args": {}}),
        json!({"type": "tool_result", "batch_id": "b1", "result": {"tool_call_id": "c1",
            "output": {"kind": "parts", "parts": [{"kind": "text", "text": "the screen"},
                {"kind": "media", "media": image_ref(shot)}]}, "is_error": false},
            "outcome": "completed"}),
        json!({"type": "tool_batch_finished", "batch_id": "b1"}),
        json!({"type": "step_started", "step_id": 4}),
        json!({"type": "model_step_finished", "step_id": 4,
            "assistant_message": {"message_id": "m4",
                "content": [{"type": "text", "text": "Seen."}]},
            "usage": {"input_tokens": 3, "output_tokens": 1},
            "finish_reason": "stop", "retries_used": 0}),
        json!({"type": "run_finished", "reason": "completed", "ts_ms": 3_003,
            "usage": {"input_tokens": 6, "output_tokens": 2}, "retries_used": 0}),
    ];
    let start = std::iter::once(run_started("r3", 3_000, "Look."));
    for (at, record) in start.chain(run.into_iter().map(self::record)).enumerate() {
        records.push(FramedRecord {
            seq: last + 1 + at as u64,
            fence: 1,
            record,
        });
    }
    let media = Media {
        whole: [(sha256(shot), shot.to_vec())].into(),
        ..Media::default()
    };
    let staged = stage_with(&root, &records, lineage, &media).unwrap();
    assert_eq!(staged.media, 1);
    let staging = root.join("session-store/staging/store-move-test");
    let view = block_on(Session::view(
        LocalBackend::new(&staging),
        SessionAddr(broca_core::session_addr(&session())),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap();
    let named = block_on(view.named_media(&sha256(shot)))
        .unwrap()
        .expect("the staged session names the image");
    assert!(named.stored(), "an object holds it");
    assert_eq!(block_on(view.named_media_bytes(&named)).unwrap(), shot);
    drop(view);
    // The media agree, so the checks past them run, up to the session's next request: with no
    // media store of today's to render its media from, today's offline render refuses it, which
    // that check names.
    let failed = block_on(verify(Verify {
        records: &records,
        lineage,
        session: &session(),
        deployment: &deployment(),
        store_root: &root.join("session-store"),
        staging: &staging,
        converted: &staged.converted,
        mode: Mode::Move,
        media: Some(&media),
        media_root: None,
        todays_updated_at: todays_updated_at(&records),
    }))
    .unwrap_err();
    assert_eq!(failed.check, crate::verify::NEXT_REQUEST, "{failed}");
    assert!(failed.detail.contains("media"), "{failed}");
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
        crate::stage::prepare(&records, lineage.map(|id| id.to_string()).as_deref(), None).unwrap();
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

/// `send.status` of a send delivered with a tool result names the call whose result carried it,
/// in the new storage as today's storage does. Read from a WAL of a run whose tool batch carried
/// a queued send's text, converted into a session in each of the new storage's modes.
#[test]
fn an_enveloped_notes_send_status_names_its_ride_key_alike() {
    note_status_alike(true);
}

/// [`an_enveloped_notes_send_status_names_its_ride_key_alike`] for a note delivered before ride
/// envelopes existed: its `send.status` names the carrier call's own key.
#[test]
fn a_legacy_notes_send_status_names_its_carrier_key_alike() {
    note_status_alike(false);
}

/// `run.result` names a completed run's final message by the id today's handler serves for it,
/// in the new storage as today's storage does: on a log whose runs froze a compaction plan
/// version, the form with the log's lineage. Read from such a log converted into a session in
/// each of the new storage's modes.
#[test]
fn a_final_message_is_named_alike_on_a_log_with_a_compaction_plan() {
    use broca_module::store_status::{todays, Reader};
    use broca_session_store::backend::MemoryBackend;
    use broca_session_store::keys::Cipher;
    let root = scratch("final-mid");
    // The lineage a log's header names, which the final message's id carries.
    let lineage = broca_wal::FileWal::open(root.join("session.wal"))
        .unwrap()
        .lineage_id();
    let mut start = run_started("r1", 1_000, "Hello.");
    if let WalRecord::RunStarted {
        compaction_plan_version,
        ..
    } = &mut start
    {
        *compaction_plan_version = Some(broca_core::compaction_v2::PLAN_VERSION);
    }
    let mut records = vec![start];
    records.extend(
        [
            json!({"type": "step_started", "step_id": 1}),
            json!({"type": "model_step_finished", "step_id": 1,
                "assistant_message": {"message_id": "m1",
                    "content": [{"type": "text", "text": "One."}]},
                "usage": {"input_tokens": 3, "output_tokens": 1},
                "finish_reason": "stop", "retries_used": 0}),
            json!({"type": "run_finished", "reason": "completed", "ts_ms": 1_003,
                "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
        ]
        .into_iter()
        .map(record),
    );
    let framed: Vec<FramedRecord> = (1u64..)
        .zip(records)
        .map(|(seq, record)| FramedRecord {
            seq,
            fence: 1,
            record,
        })
        .collect();
    let run = broca_protocol::RunId("r1".into());
    let today =
        serde_json::to_value(todays::run_result(&framed, &run, "s", lineage).unwrap()).unwrap();
    let lineage_text = lineage.map(|id| id.to_string());
    assert_eq!(
        today["final_message"]["mid"],
        json!(broca_core::message_mid(true, lineage_text.as_deref(), 1).unwrap()),
        "{today}"
    );
    let converted = crate::convert(&framed, lineage_text.as_deref()).unwrap();
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    for cipher in Cipher::ALL {
        let backend = MemoryBackend::default();
        let mut store = block_on(Session::create(
            &backend,
            SessionAddr(broca_core::session_addr(&session())),
            crate::stage::triple(&session()),
            &deployment().creating(cipher),
            false,
        ))
        .unwrap();
        block_on(store.append(&converted.records, false)).unwrap();
        let parts = broca_session_loop::parts::MemoryParts {
            parts: converted.parts.clone(),
        };
        let reader = Reader {
            labels: Default::default(),
            parts: &parts,
            live: false,
            settled: None,
            session_name: "s",
            live_step: None,
        };
        let served = block_on(reader.run_result(&store, &run)).unwrap();
        assert_eq!(
            serde_json::to_value(served).unwrap(),
            today,
            "{}",
            cipher.label()
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// An imported session's seeded run: a run start that seeds a user prompt and an assistant reply
/// from the source, ended completed with no step. Today's `run.result` serves the seeded reply as
/// the run's final message; the converted session serves the same, not a refusal of a completed
/// run that placed no message. A later run of the session answers from its own reply, as before.
#[test]
fn an_imported_runs_final_message_is_served_alike() {
    use broca_module::store_status::{todays, Reader};
    use broca_session_store::backend::MemoryBackend;
    let mut seed = run_started("run-iid-import", 1_000, "Imported prompt.");
    if let WalRecord::RunStarted { input, origin, .. } = &mut seed {
        *input = std::sync::Arc::new(vec![
            serde_json::from_value(json!({
                "role": "user", "content": [{"type": "text", "text": "Imported prompt."}],
            }))
            .unwrap(),
            serde_json::from_value(json!({
                "role": "assistant", "content": [{"type": "text", "text": "Imported reply."}],
            }))
            .unwrap(),
        ]);
        *origin = serde_json::from_value(
            json!({"kind": "seeded_from", "origin_kind": "import", "source_ref": "src:1"}),
        )
        .unwrap();
    }
    let mut records = vec![
        seed,
        record(
            json!({"type": "run_finished", "reason": "completed", "ts_ms": 1_001,
            "usage": {}, "retries_used": 0}),
        ),
        run_started("r2", 2_000, "Next."),
    ];
    records.extend(
        [
            json!({"type": "step_started", "step_id": 1}),
            json!({"type": "model_step_finished", "step_id": 1,
                "assistant_message": {"message_id": "m1",
                    "content": [{"type": "text", "text": "Own reply."}]},
                "usage": {"input_tokens": 3, "output_tokens": 1},
                "finish_reason": "stop", "retries_used": 0}),
            json!({"type": "run_finished", "reason": "completed", "ts_ms": 2_003,
                "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
        ]
        .into_iter()
        .map(record),
    );
    let framed = framed(records);
    let converted = crate::convert(&framed, None).unwrap();
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    let backend = MemoryBackend::default();
    let mut store = block_on(Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(&session())),
        crate::stage::triple(&session()),
        &deployment(),
        false,
    ))
    .unwrap();
    block_on(store.append(&converted.records, false)).unwrap();
    let parts = broca_session_loop::parts::MemoryParts {
        parts: converted.parts.clone(),
    };
    let reader = Reader {
        labels: Default::default(),
        parts: &parts,
        live: false,
        settled: None,
        session_name: "s",
        live_step: None,
    };
    for (run, ordinal, text) in [
        ("run-iid-import", 2, "Imported reply."),
        ("r2", 4, "Own reply."),
    ] {
        let run = broca_protocol::RunId(run.into());
        let today =
            serde_json::to_value(todays::run_result(&framed, &run, "s", None).unwrap()).unwrap();
        assert_eq!(today["final_message"]["ordinal"], json!(ordinal), "{today}");
        assert_eq!(today["final_message"]["text"], json!(text), "{today}");
        let served = block_on(reader.run_result(&store, &run)).unwrap();
        assert_eq!(serde_json::to_value(served).unwrap(), today, "{}", run.0);
    }
}

/// Compares today's and the new storage's `send.status` of a note delivered with a tool result,
/// with a ride envelope when `enveloped`.
fn note_status_alike(enveloped: bool) {
    use broca_module::store_status::{todays, Reader};
    use broca_session_store::backend::MemoryBackend;
    use broca_session_store::keys::Cipher;
    for cipher in Cipher::ALL {
        {
            let root = scratch(&format!("note-status-{}-{enveloped}", cipher.label()));
            // A WAL of today's, for the lineage its header names, which a key derived from the
            // log names; the records are read as they would be appended.
            let lineage = broca_wal::FileWal::open(root.join("session.wal"))
                .unwrap()
                .lineage_id();
            let mut delivered = json!({"type": "send_delivered", "submission_id": "sub-q",
                "run_id": "r1", "mode": "tool_result", "tool_call_id": "c1",
                "appended_text": "\n\nnote", "written_at_ms": 5});
            if enveloped {
                delivered["ride"] = json!({"call_key": "lineage:enveloped", "send_id": "q",
                    "sender": "broca", "forms": ["tool_result"], "form": "append"});
            }
            let mut records = vec![run_started("r1", 1_000, "Hello.")];
            records.extend(
                [
                    json!({"type": "delivery_ordered", "submission_id": "sub-q",
                        "order": {"modes": ["tool_result"]}}),
                    json!({"type": "queued", "submission_id": "sub-q", "send_id": "q",
                        "request": {"model": {"provider_module_id": "p", "model_id": "m"},
                            "input": [{"role": "user",
                                "content": [{"type": "text", "text": "note"}]}],
                            "policy_allowed": [], "tool_choice": {"type": "auto"},
                            "generation": {}, "stop_when": []},
                        "written_at_ms": 5}),
                    json!({"type": "step_started", "step_id": 1}),
                    json!({"type": "model_step_finished", "step_id": 1,
                        "assistant_message": {"message_id": "m1", "content": [
                            {"type": "tool_call", "tool_call_id": "c1", "tool_name": "read",
                                "input": {}}]},
                        "usage": {"input_tokens": 3, "output_tokens": 1},
                        "finish_reason": "tool_calls", "retries_used": 0}),
                    json!({"type": "tool_batch_started", "batch_id": "b1", "step_id": 1,
                        "planned_calls": ["c1"]}),
                    json!({"type": "tool_dispatch_intent", "batch_id": "b1",
                        "tool_call_id": "c1", "args": {}}),
                    json!({"type": "tool_result", "batch_id": "b1", "result": {
                        "tool_call_id": "c1", "output": {"kind": "text", "text": "ok"},
                        "is_error": false}, "outcome": "completed"}),
                    json!({"type": "tool_batch_finished", "batch_id": "b1"}),
                    delivered,
                    json!({"type": "turn_finished", "step_id": 1}),
                    json!({"type": "step_started", "step_id": 2}),
                    json!({"type": "model_step_finished", "step_id": 2,
                        "assistant_message": {"message_id": "m2",
                            "content": [{"type": "text", "text": "Done."}]},
                        "usage": {"input_tokens": 3, "output_tokens": 1},
                        "finish_reason": "stop", "retries_used": 0}),
                    json!({"type": "run_finished", "reason": "completed", "ts_ms": 1_003,
                        "usage": {"input_tokens": 6, "output_tokens": 2},
                        "retries_used": 0}),
                ]
                .into_iter()
                .map(record),
            );
            let framed: Vec<FramedRecord> = (1u64..)
                .zip(records)
                .map(|(seq, record)| FramedRecord {
                    seq,
                    fence: 1,
                    record,
                })
                .collect();
            let today =
                todays::send_status(&framed, lineage, broca_protocol::RunId("q".into()), "q")
                    .unwrap();
            let lineage_text = lineage.map(|id| id.to_string());
            let converted = crate::convert(&framed, lineage_text.as_deref()).unwrap();
            assert!(converted.lost.is_empty(), "{:?}", converted.lost);
            let backend = MemoryBackend::default();
            let mut store = block_on(Session::create(
                &backend,
                SessionAddr(broca_core::session_addr(&session())),
                crate::stage::triple(&session()),
                &deployment().creating(cipher),
                true,
            ))
            .unwrap();
            block_on(store.append(&converted.records, false)).unwrap();
            let parts = broca_session_loop::parts::MemoryParts {
                parts: converted.parts.clone(),
            };
            let reader = Reader {
                labels: Default::default(),
                parts: &parts,
                live: false,
                settled: None,
                session_name: "s",
                live_step: None,
            };
            let served =
                block_on(reader.send_status(&store, broca_protocol::RunId("q".into()), "q", None))
                    .unwrap();
            let what = format!("{} enveloped={enveloped}", cipher.label());
            let today = serde_json::to_value(today).unwrap();
            assert_eq!(serde_json::to_value(served).unwrap(), today, "{what}");
            let key = today["delivered"]["call_key"]
                .as_str()
                .unwrap_or_else(|| panic!("today's names the carrier, {what}: {today}"));
            if enveloped {
                assert_eq!(key, "lineage:enveloped", "{what}");
            } else {
                assert_ne!(key, "lineage:enveloped", "{what}");
            }
            let _ = std::fs::remove_dir_all(&root);
        }
    }
}

/// The queued send of an ordered delivery whose only mode is `mode`, under submission `sub-q`.
fn ordered_send(mode: &str) -> [WalRecord; 2] {
    [
        record(json!({"type": "delivery_ordered", "submission_id": "sub-q",
            "order": {"modes": [mode], "dedupe_key": "nudge"}})),
        record(
            json!({"type": "queued", "submission_id": "sub-q", "send_id": "q",
            "request": {"model": {"provider_module_id": "p", "model_id": "m"},
                "input": [{"role": "user", "content": [{"type": "text", "text": "note"}]}],
                "policy_allowed": [], "tool_choice": {"type": "auto"},
                "generation": {}, "stop_when": []},
            "written_at_ms": 1_001}),
        ),
    ]
}

/// A run that queued `queued` during its step and then answered in text and completed: the
/// queued send is still pending when the run ends.
fn completed_with_queued(queued: Vec<WalRecord>) -> Vec<FramedRecord> {
    let mut records = vec![
        run_started("r1", 1_000, "Hello."),
        record(json!({"type": "step_started", "step_id": 1})),
    ];
    records.extend(queued);
    records.extend([
        record(json!({"type": "model_step_finished", "step_id": 1,
            "assistant_message": {"message_id": "m1",
                "content": [{"type": "text", "text": "Done."}]},
            "usage": {"input_tokens": 3, "output_tokens": 1},
            "finish_reason": "stop", "retries_used": 0})),
        record(
            json!({"type": "run_finished", "reason": "completed", "ts_ms": 1_003,
            "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
        ),
    ]);
    framed(records)
}

/// An ordered send whose only delivery mode is `tool_result`, queued while a run that made no
/// later tool call finished, waits for a later run's tool result: today's storage keeps it
/// queued. It moves as it is: the staged session answers its `send.status` as today's does,
/// and the next send's first tool result delivers it alike. Any other pending send still keeps
/// the session from moving.
#[test]
fn an_ordered_send_waiting_for_a_tool_result_moves_and_delivers_alike() {
    use crate::preconditions::{wal_refusal, SESSION_BUSY};
    let records = completed_with_queued(ordered_send("tool_result").into());
    assert_eq!(wal_refusal(&records, None), None);
    let root = scratch("waiting-order");
    let staged = staged(&root, &records, None);
    let verified = verified(&root, &records, None, &staged.converted, Mode::Move).unwrap();
    for check in ["send_status", "first_open", "next_request", "delivery"] {
        assert!(
            verified.checks.contains(&check),
            "{check}: {:?}",
            verified.checks
        );
    }
    let _ = std::fs::remove_dir_all(&root);

    let steer = completed_with_queued(ordered_send("steer").into());
    assert_eq!(wal_refusal(&steer, None), Some(SESSION_BUSY));
    let [_, plain] = ordered_send("tool_result");
    let plain = completed_with_queued(vec![plain]);
    assert_eq!(wal_refusal(&plain, None), Some(SESSION_BUSY));
}

/// A session whose one run started a step and paused with `reason`, under send id `k1`.
fn paused(reason: &str) -> Vec<FramedRecord> {
    let mut start = run_started("r1", 1_000, "Hello.");
    if let WalRecord::RunStarted { send_id, .. } = &mut start {
        *send_id = Some("k1".into());
    }
    framed(vec![
        start,
        record(json!({"type": "step_started", "step_id": 1})),
        record(json!({"type": "run_paused", "run_id": "r1", "ts_ms": 1_002, "reason": reason})),
    ])
}

/// A latest run paused for sign-in, with no call held, waits for its own send's retry: today's
/// storage leaves it paused. It moves as it is: the staged session answers its `run.status`,
/// `run.result` and `send.status` as today's does, and its resume sends the step it couldn't
/// send alike. Its session has no next send, which today's storage refuses with the pause. A run
/// paused for any other reason still keeps the session from moving.
#[test]
fn a_run_paused_for_sign_in_moves_and_resumes_alike() {
    use crate::preconditions::{paused_for_sign_in, wal_refusal, SESSION_BUSY};
    let records = paused("auth_required");
    assert_eq!(wal_refusal(&records, None), None);
    assert_eq!(paused_for_sign_in(&records).as_deref(), Some("r1"));
    let root = scratch("paused-sign-in");
    let staged = staged(&root, &records, None);
    let verified = verified(&root, &records, None, &staged.converted, Mode::Move).unwrap();
    for check in [
        "run_status",
        "run_result",
        "send_status",
        "first_open",
        "resume",
    ] {
        assert!(
            verified.checks.contains(&check),
            "{check}: {:?}",
            verified.checks
        );
    }
    assert!(!verified.checks.contains(&"next_request"));
    let _ = std::fs::remove_dir_all(&root);

    let restart = paused("restart");
    assert_eq!(wal_refusal(&restart, None), Some(SESSION_BUSY));
    assert_eq!(paused_for_sign_in(&restart), None);
}

/// The run-index rows that let a session move: every row terminal with its usage and no
/// withdrawal owed, and, as the one exception, the paused row of the run the move carries paused
/// for sign-in.
#[test]
fn only_the_carried_paused_runs_row_may_be_unfinished() {
    use crate::preconditions::run_rows_settled;
    use broca_core::{RunIndexRow, RunIndexState};
    let row = |run: &str, state: RunIndexState, usage: bool| RunIndexRow {
        run_id: run.into(),
        session: session(),
        submission_id: None,
        request_identity_digest: None,
        state,
        usage: usage.then(Default::default),
        final_step_finish_reason: None,
        retries_used: 0,
        title: None,
        withdrawals_owed: false,
    };
    let finished = row("r0", RunIndexState::Completed, true);
    let paused = row("r1", RunIndexState::Paused, false);
    assert!(run_rows_settled(std::slice::from_ref(&finished), None));
    assert!(run_rows_settled(
        &[finished.clone(), paused.clone()],
        Some("r1")
    ));
    assert!(!run_rows_settled(&[finished.clone(), paused.clone()], None));
    assert!(!run_rows_settled(std::slice::from_ref(&paused), Some("r2")));
    let active = row("r1", RunIndexState::Active, false);
    assert!(!run_rows_settled(&[active], Some("r1")));
    let owing = RunIndexRow {
        withdrawals_owed: true,
        ..paused
    };
    assert!(!run_rows_settled(&[owing], Some("r1")));
    let unreconciled = row("r0", RunIndexState::Completed, false);
    assert!(!run_rows_settled(&[unreconciled], Some("r1")));
}

/// A session compacted under the first compaction protocol moves read-only: it stages and
/// verifies its reads, history and entries as any session does, without rebuilding the requests
/// the new run loop refuses to render or opening it as a writer, and says so.
#[test]
fn a_session_compacted_under_the_first_protocol_moves_read_only() {
    use crate::preconditions::{converted_refusal, wal_refusal, UNSUPPORTED_FEATURE};
    let mut records = vec![
        run_started("r1", 1_000, "Hello."),
        record(
            json!({"type": "compaction_message", "provider": "mc", "kind": "setup",
            "compaction_id": "c", "version": 1, "range": {"from": 0, "to": 0},
            "replacement": [],
            "sha256": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"}),
        ),
        record(json!({"type": "step_started", "step_id": 1})),
    ];
    records.extend([
        // A recorded request, which the new run loop would refuse to rebuild.
        record(
            json!({"type": "model_attempt_finished", "step_id": 1, "attempt": 0,
            "outcome": "complete", "ts_ms": 1_001, "request_body_len": 0,
            "request_body_sha256": crate::verify::sha256_hex(b""),
            "response_body_len": 0, "response_body_sha256": crate::verify::sha256_hex(b"")}),
        ),
        record(json!({"type": "model_step_finished", "step_id": 1,
            "assistant_message": {"message_id": "m1",
                "content": [{"type": "text", "text": "Done."}]},
            "usage": {"input_tokens": 3, "output_tokens": 1},
            "finish_reason": "stop", "retries_used": 0})),
        record(
            json!({"type": "run_finished", "reason": "completed", "ts_ms": 1_003,
            "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
        ),
    ]);
    let records = framed(records);
    assert_eq!(wal_refusal(&records, None), None);
    let prepared = crate::stage::prepare(&records, None, None).unwrap();
    assert_eq!(
        block_on(converted_refusal(&prepared)).unwrap(),
        Some(UNSUPPORTED_FEATURE),
        "the new run loop refuses to run it"
    );
    let root = scratch("first-protocol");
    let staged = staged(&root, &records, None);
    let verified = verified(&root, &records, None, &staged.converted, Mode::Move).unwrap();
    assert!(verified.read_only);
    for check in ["read", "history_raw", "history_display", "entries"] {
        assert!(
            verified.checks.contains(&check),
            "{check}: {:?}",
            verified.checks
        );
    }
    for skipped in ["last_step", "first_open", "next_request"] {
        assert!(
            !verified.checks.contains(&skipped),
            "{skipped}: {:?}",
            verified.checks
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
