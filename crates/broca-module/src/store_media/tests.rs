//! The media fold refuses to render past a record it didn't take in.

use std::sync::Arc;

use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::{BindIdentity, MediaRef};
use broca_session_loop::parts::MemoryParts;
use broca_session_store::record::{Record, StepStart};

use super::MediaFold;

fn step(step_id: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: "r".into(),
        step_id,
        mid: None,
    })
}

#[derive(Debug)]
struct NoBytes;

impl MediaBytes for NoBytes {
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        Err(MediaReadError::Missing(reference.sha256.clone()))
    }
}

fn fold() -> MediaFold {
    MediaFold::new(BindIdentity::new("/project", "store-actor", "s1"))
}

#[test]
fn a_batch_that_skips_a_record_stops_the_fold_and_every_later_run_refuses() {
    let parts = MemoryParts::default();
    let mut fold = fold();
    fold.take(&[(1, step(1))], &parts);
    assert_eq!(fold.next_seq(), 2);
    // Record 2 never reached the fold.
    fold.take(&[(3, step(2))], &parts);
    let reason = fold.broken().expect("the fold stopped");
    assert!(reason.contains("expected record 2"), "{reason}");
    let refused = fold
        .for_run(Some(Arc::new(NoBytes)))
        .renderer_inputs()
        .err()
        .expect("a run can't render from a stopped fold");
    assert_eq!(refused, reason);
    // Nothing taken later mends it: the fold can't know what record 2 held.
    fold.take(&[(2, step(2))], &parts);
    assert!(fold.broken().is_some());
}

#[test]
fn records_already_taken_are_passed_over() {
    let parts = MemoryParts::default();
    let mut fold = fold();
    fold.take(&[(1, step(1)), (2, step(2))], &parts);
    // A catch-up that starts before the fold's position repeats nothing.
    fold.take(&[(1, step(1)), (2, step(2)), (3, step(3))], &parts);
    assert_eq!(fold.next_seq(), 4);
    assert!(fold.broken().is_none());
}

#[test]
fn a_run_reads_every_record_after_its_seed_in_order_and_refuses_a_gap() {
    let parts = MemoryParts::default();
    let mut fold = fold();
    fold.take(&[(1, step(1))], &parts);
    let (_, seed, read_tail) = fold
        .for_run(Some(Arc::new(NoBytes)))
        .renderer_inputs()
        .unwrap();
    assert_eq!(seed.at_seq, 1);
    fold.take(&[(2, step(2)), (3, step(3))], &parts);
    let seqs = |frames: Vec<broca_wal::FramedRecord>| -> Vec<u64> {
        frames.iter().map(|frame| frame.seq).collect()
    };
    assert_eq!(seqs(read_tail(1).unwrap()), [2, 3]);
    assert_eq!(seqs(read_tail(3).unwrap()), Vec::<u64>::new());
    fold.take(&[(4, step(4))], &parts);
    // A reader whose head is behind what it already read past is missing record 3.
    let error = read_tail(2).unwrap_err();
    assert!(error.contains("after 2 start at 4"), "{error}");
}

#[test]
fn a_run_without_a_way_to_read_media_objects_refuses_by_name() {
    let error = fold().for_run(None).renderer_inputs().err().unwrap();
    assert!(error.contains("media objects can't be read"), "{error}");
}

fn event(name: &str) -> Record {
    Record::Event(broca_session_store::record::Event {
        name: name.into(),
        run_id: None,
        body: None,
    })
}

/// A run's append hands the media fold the records it wrote, and the records the actor appended
/// since the fold last took any come from what the writer keeps in memory: neither reads the
/// open log, so an append costs the same however long the log is. A fold behind what the writer
/// keeps stops, naming the gap, rather than read the log.
#[tokio::test]
async fn an_append_feeds_the_media_fold_without_reading_the_open_log() {
    use broca_session_loop::Writer as _;
    use broca_session_store::backend::{MemoryBackend, SessionAddr};
    use broca_session_store::session::Session;

    let backend = MemoryBackend::default();
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        broca_session_store::head::SessionTriple {
            project_root: "/project".into(),
            harness: "store-actor".into(),
            session: "s1".into(),
        },
        &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let parts = MemoryParts::default();
    for n in 0..50 {
        session
            .append(&[event(&format!("history-{n}"))], false)
            .await
            .unwrap();
    }
    let mut fold = fold();
    fold.catch_up(&session, &parts).await;
    assert!(fold.broken().is_none(), "{:?}", fold.broken());
    // The actor appends one of its own, then the run appends.
    session.append(&[event("actor")], false).await.unwrap();
    let reads = backend.log_reads();
    let tool_media = broca_session_loop::PendingMedia::default();
    let mut appends = super::RunAppends {
        session: &mut session,
        tool_media: &tool_media,
        fold: Some(&mut fold),
        parts: &parts,
    };
    appends.append(&[event("run")], false).await.unwrap();
    assert_eq!(backend.log_reads(), reads, "an append read the open log");
    assert!(fold.broken().is_none(), "{:?}", fold.broken());
    assert_eq!(fold.next_seq(), session.next_seq());

    // A run's start drops what the writer keeps from before it; a fold that missed records
    // there can't take the next append.
    let mut behind = super::MediaFold::new(BindIdentity::new("/project", "store-actor", "s1"));
    session
        .append(
            &[Record::RunStart(Box::new(
                broca_session_store::record::RunStart {
                    named_speaker: None,
                    imported_ids: None,
                    message_marks: None,
                    run_id: "r2".into(),
                    model: serde_json::from_value(
                        serde_json::json!({"provider_module_id": "p", "model_id": "m"}),
                    )
                    .unwrap(),
                    setup: Vec::new(),
                    renderer_version: 1,
                    submission_id: None,
                    send_id: None,
                    request_identity_digest: None,
                    ts_ms: None,
                    written_at_ms: None,
                    session_class: None,
                    retention_secs: None,
                    title: None,
                    context_limit: None,
                    service_tier: None,
                    route_decision_id: None,
                    compaction_plan_version: None,
                    auth_selection: None,
                    run_choices: None,
                    origin: None,
                    principal: None,
                    scope: None,
                    hooks: None,
                    keep_warm: None,
                    on_restart: None,
                    seeded: None,
                    refusal_schedule: None,
                },
            ))],
            false,
        )
        .await
        .unwrap();
    let mut appends = super::RunAppends {
        session: &mut session,
        tool_media: &tool_media,
        fold: Some(&mut behind),
        parts: &parts,
    };
    let reads = backend.log_reads();
    appends.append(&[event("after")], false).await.unwrap();
    assert_eq!(
        backend.log_reads(),
        reads,
        "a fold behind the kept records read the log"
    );
    let reason = behind.broken().expect("the fold behind stopped");
    assert!(reason.contains("expected record 1"), "{reason}");
}
