//! The actor's retention deadline for a session kept by the older retention seconds.

use broca_protocol::RunFinishReason;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::{Record, RunEnd, RunStart};
use broca_session_store::session::Session;

use super::super::tests::{deployment, triple, ADDR};
use super::Timer;

const SECONDS: u64 = 60;

fn start(run: &str, ts: u64) -> Record {
    Record::RunStart(Box::new(RunStart {
        imported_ids: None,
        render_identity: None,
        message_marks: None,
        named_speaker: None,
        kept_notice: None,
        run_id: run.into(),
        model: serde_json::from_value(
            serde_json::json!({"provider_module_id": "p", "model_id": "m"}),
        )
        .unwrap(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: Some(ts),
        written_at_ms: Some(ts),
        session_class: None,
        retention_secs: Some(SECONDS),
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
    }))
}

fn end(run: &str, ts: u64) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: Some(ts),
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

/// The deadline of a session kept by the older retention seconds is decided again after every
/// write, from the records appended since the last decision only: over 50 runs each record is
/// read once, rather than the whole session after each run.
#[tokio::test]
async fn the_legacy_deadline_reads_each_record_once() {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let clock = broca_core::clock::Clock::default();
    let mut timer = Timer::default();
    for n in 0..50_u64 {
        let run = format!("r{n}");
        let ended = 10_000 + n * 1_000;
        session
            .append(&[start(&run, ended - 500), end(&run, ended)], true)
            .await
            .unwrap();
        assert_eq!(
            timer.deadline(&session, false, Some(&clock)).await,
            Some(ended + SECONDS * 1_000),
            "after {run}"
        );
        // Deciding again with nothing appended reads nothing.
        timer.deadline(&session, false, Some(&clock)).await;
    }
    // The session holds what its creation wrote too, which the first decision read.
    let held = session.records_from(0).await.unwrap().len();
    assert_eq!(timer.records_read, held);
}
