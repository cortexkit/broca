//! Sessions that hold open work the new storage serves as today's does, or that moved read-only,
//! converting in place at startup.

use super::*;

/// Converts the rig's live session at startup and returns its ledger row, with the WAL's bytes
/// before the conversion.
async fn converted(rig: &Rig, records: &[WalRecord]) -> (crate::conversion_ledger::Row, Vec<u8>) {
    rig.write(records);
    let before = std::fs::read(rig.wal()).unwrap();
    let engine = engine(rig, false, true);
    engine
        .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
        .await
        .unwrap();
    let rows = engine.ledger.rows().unwrap();
    assert_eq!(rows.len(), 1);
    (rows[0].clone(), before)
}

/// A live session whose latest run paused for sign-in converts with the run still paused: the
/// recovery before the conversion appends nothing to its log, as today's open appends nothing,
/// and the converted session holds the pause, so the run's own send resumes it.
#[tokio::test]
async fn cutover_carries_a_run_paused_for_sign_in_paused() {
    let rig = Rig::start("cutover-paused").await;
    let mut start = run_started("r1", 1_000);
    if let WalRecord::RunStarted { send_id, .. } = &mut start {
        *send_id = Some("k1".into());
    }
    let (row, before) = converted(
        &rig,
        &[
            start,
            record(json!({"type": "step_started", "step_id": 1})),
            record(json!({"type": "run_paused", "run_id": "r1", "ts_ms": 1_002,
                "reason": "auth_required"})),
        ],
    )
    .await;
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    assert_eq!(std::fs::read(&row.source.backup).unwrap(), before);
    let view = rig.runtime.current_view(&key()).await.unwrap().unwrap();
    let run = view.operations.run.as_ref().unwrap();
    assert_eq!(run.run_id, "r1");
    assert_eq!(run.paused, Some(broca_protocol::PauseReason::AuthRequired));
    assert_eq!(run.finished, None);
}

/// A live session holding an ordered send that waits for a tool result converts with the send
/// still queued, as today's storage keeps it.
#[tokio::test]
async fn cutover_carries_an_ordered_send_waiting_for_a_tool_result_queued() {
    let rig = Rig::start("cutover-ordered").await;
    let (row, _) = converted(
        &rig,
        &[
            run_started("r1", 1_000),
            record(json!({"type": "step_started", "step_id": 1})),
            record(json!({"type": "delivery_ordered", "submission_id": "sub-q",
                "order": {"modes": ["tool_result"], "dedupe_key": "nudge"}})),
            record(
                json!({"type": "queued", "submission_id": "sub-q", "send_id": "q",
                "request": {"model": {"provider_module_id": "p", "model_id": "m"},
                    "input": [{"role": "user", "content": [{"type": "text", "text": "note"}]}],
                    "policy_allowed": [], "tool_choice": {"type": "auto"},
                    "generation": {}, "stop_when": []},
                "written_at_ms": 1_001}),
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
        ],
    )
    .await;
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    let view = rig.runtime.current_view(&key()).await.unwrap().unwrap();
    assert_eq!(
        broca_module::store_status::queued_send_fate(&view, "q")
            .await
            .unwrap(),
        Some(broca_core::delivery_order::QueuedSendFate::Pending)
    );
}

/// A live session compacted under the first compaction protocol converts read-only: its ledger
/// row says so, and a new send to it is refused `not_served_on_new_storage`, naming the feature,
/// with nothing written.
#[tokio::test]
async fn cutover_carries_a_first_protocol_session_read_only() {
    let rig = Rig::start("cutover-first-protocol").await;
    let (row, _) = converted(
        &rig,
        &[
            run_started("r1", 1_000),
            record(
                json!({"type": "compaction_message", "provider": "mc", "kind": "setup",
                "compaction_id": "c", "version": 1, "range": {"from": 0, "to": 0},
                "replacement": [],
                "sha256": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"}),
            ),
            record(json!({"type": "step_started", "step_id": 1})),
            record(json!({"type": "model_step_finished", "step_id": 1,
                "assistant_message": {"message_id": "m1",
                    "content": [{"type": "text", "text": "Done."}]},
                "usage": {"input_tokens": 3, "output_tokens": 1},
                "finish_reason": "stop", "retries_used": 0})),
            record(
                json!({"type": "run_finished", "reason": "completed", "ts_ms": 1_003,
                "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
            ),
        ],
    )
    .await;
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    assert!(
        row.detail
            .as_deref()
            .is_some_and(|detail| detail.starts_with("read_only:")),
        "{row:?}"
    );
    let before = rig.runtime.current_view(&key()).await.unwrap().unwrap();
    let placed = broca_module::store_read::transcript_len(&before);
    let refused = rig
        .registry
        .route_send(
            &key(),
            broca_core::RunRequest::from_submission(
                serde_json::from_value(json!({
                    "model": {"provider_module_id": "anthropic", "model_id": "claude-test"},
                    "input": [{"role": "user", "content": [{"type": "text", "text": "More."}]}]
                }))
                .unwrap(),
                broca_protocol::RunId("r2".into()),
                key(),
                broca_protocol::SubmissionId("submission-r2".into()),
            ),
            broca_module::executor::RunKind::AppendEpisode,
        )
        .await;
    assert!(
        matches!(
            refused,
            Ok(broca_module::actor::SendOutcome::NotServedOnNewStorage {
                feature: "first_protocol_compaction"
            })
        ),
        "{refused:?}"
    );
    let after = rig.runtime.current_view(&key()).await.unwrap().unwrap();
    assert_eq!(broca_module::store_read::transcript_len(&after), placed);
    assert!(after
        .operations
        .run
        .as_ref()
        .is_some_and(|run| run.run_id == "r1"));
}
