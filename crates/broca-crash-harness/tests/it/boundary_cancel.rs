use super::*;
use broca_module::actor::CancelOutcome;
use broca_wal::record::RunCancelAt;

#[tokio::test]
async fn acknowledged_boundary_cancel_survives_every_record_cut_and_resume_cut() {
    let reference = Session::open(
        scratch("boundary-reference"),
        Script::with(&[Step::Call("one"), Step::Text("forbidden")]),
    );
    reference.script.hold(Hold::Model, true);
    reference.send("boundary", "work", Delivery::Queue).await;
    reference.wait_entered().await;
    assert_eq!(
        reference
            .registry
            .route_cancel_at(
                &session(),
                RunId("run-boundary".into()),
                RunCancelAt::StepBoundary
            )
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    let acknowledged = reference.log().record_count();
    assert!(matches!(
        reference.log().records().last(),
        Some(WalRecord::RunCancelRequested { .. })
    ));
    reference.script.hold(Hold::Model, false);
    let log = reference.settle().await;
    assert_eq!(
        reference.script.dispatched.lock().unwrap().as_slice(),
        ["one"]
    );
    let mut cuts = 0;
    let mut inside_resume = 0;
    for records in acknowledged..=log.record_count() {
        let bytes = &log.bytes[..log.end_of_records(records)];
        let (cut, after, dispatched) = recover(Scenario::SteerDuringToolCall, bytes).await;
        check_cancelled(&cut, &after, &dispatched);
        cuts += 1;
        for resume_records in cut.record_count() + 1..=after.record_count() {
            let bytes = &after.bytes[..after.end_of_records(resume_records)];
            let (cut, after, dispatched) = recover(Scenario::SteerDuringToolCall, bytes).await;
            check_cancelled(&cut, &after, &dispatched);
            inside_resume += 1;
        }
    }
    assert!(
        cuts >= 7,
        "must cover model result, batch, intent, result and terminal boundaries"
    );
    assert!(inside_resume > 0, "must cut recovery's own records too");
    println!("boundary cancellation: {cuts} post-ack record cuts and {inside_resume} resume cuts");
}

fn check_cancelled(cut: &LogImage, after: &LogImage, dispatched: &[String]) {
    assert_eq!(
        &after.bytes[..cut.bytes.len()],
        cut.bytes.as_slice(),
        "recovery must not edit durable bytes"
    );
    let before = cut.records();
    let records = after.records();
    assert!(
        records.iter().any(|record| matches!(
            record,
            WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Cancelled,
                ..
            }
        )),
        "acknowledged cancel must seal Cancelled"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::StepStarted { .. }))
            .count(),
        1,
        "acknowledged boundary cancel must not start a new model step"
    );
    let sent = intents(&before);
    assert!(
        dispatched.iter().all(|id| !sent.contains(id)),
        "must never redispatch a durable intent"
    );
    let replay = broca_core::replay(&records).unwrap();
    check_tool_pairing(&replay.prompt).expect("recovery must close every surviving tool use");
}

#[tokio::test]
async fn boundary_cancel_of_queued_submission_is_not_active_and_changes_nothing() {
    let live = Session::open(
        scratch("boundary-queue"),
        Script::with(&[Step::Call("one"), Step::Text("done")]),
    );
    live.script.hold(Hold::Tool, true);
    live.send("active", "work", Delivery::Queue).await;
    live.wait_entered().await;
    live.send("queued", "later", Delivery::Queue).await;
    let before = live.log().bytes;
    assert_eq!(
        live.registry
            .route_cancel_at(
                &session(),
                RunId("run-queued".into()),
                RunCancelAt::StepBoundary
            )
            .await
            .unwrap(),
        CancelOutcome::NotActive
    );
    assert_eq!(live.log().bytes, before);
    live.script.hold(Hold::Tool, false);
    live.settle().await;
}

#[test]
fn boundary_cancel_kill_child() {
    let Ok(root) = std::env::var("BROCA_BOUNDARY_CANCEL_KILL_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let live = Session::open(
                root.clone(),
                Script::with(&[Step::Call("one"), Step::Text("forbidden")]),
            );
            live.script.hold(Hold::Tool, true);
            live.send("boundary", "work", Delivery::Queue).await;
            live.wait_entered().await;
            assert_eq!(
                live.registry
                    .route_cancel_at(
                        &session(),
                        RunId("run-boundary".into()),
                        RunCancelAt::StepBoundary
                    )
                    .await
                    .unwrap(),
                CancelOutcome::Ack
            );
            std::fs::write(root.join("cancel-acknowledged"), b"ack").unwrap();
            std::future::pending::<()>().await;
        });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boundary_cancel_ack_survives_sigkill_during_tool_call() {
    let root = scratch("boundary-sigkill");
    let method = broca_crash_harness::kill_child_test_at_marker(
        "boundary-sigkill",
        "boundary_cancel::boundary_cancel_kill_child",
        &[("BROCA_BOUNDARY_CANCEL_KILL_ROOT", root.as_os_str())],
        &root.join("cancel-acknowledged"),
    );
    assert_eq!(format!("{method:?}"), "Sigkill");
    let durable = LogImage::read(&FileWalStore::new(root.join("wal")).wal_path(&session()));
    assert!(matches!(
        durable.records().last(),
        Some(WalRecord::RunCancelRequested { .. })
    ));
    let (cut, after, dispatched) = recover(Scenario::SteerDuringToolCall, &durable.bytes).await;
    check_cancelled(&cut, &after, &dispatched);
    assert!(dispatched.is_empty());
}

#[tokio::test]
async fn boundary_cancel_then_immediate_is_durable_and_cannot_be_downgraded() {
    let live = Session::open(
        scratch("boundary-escalation"),
        Script::with(&[Step::Call("one")]),
    );
    live.script.hold(Hold::Model, true);
    live.send("boundary", "work", Delivery::Queue).await;
    live.wait_entered().await;
    let run = RunId("run-boundary".into());
    assert_eq!(
        live.registry
            .route_cancel_at(&session(), run.clone(), RunCancelAt::StepBoundary)
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    assert_eq!(
        live.registry
            .route_cancel(&session(), run.clone())
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    let log = live.settle().await;
    let records = log.records();
    let points: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunCancelRequested { at, .. } => Some(*at),
            _ => None,
        })
        .collect();
    assert_eq!(points, [RunCancelAt::StepBoundary, RunCancelAt::Now]);
    let state = broca_core::replay(&records).unwrap();
    assert_eq!(state.cancel_at, Some(RunCancelAt::Now));
    assert!(records
        .iter()
        .all(|record| !matches!(record, WalRecord::ModelStepFinished { .. })));
    let before = live.log().bytes;
    assert_eq!(
        live.registry
            .route_cancel_at(&session(), run, RunCancelAt::StepBoundary)
            .await
            .unwrap(),
        CancelOutcome::NotActive
    );
    assert_eq!(live.log().bytes, before);
}

#[tokio::test]
async fn recovered_boundary_cancel_remains_idempotent_and_immediate_escalation_is_durable() {
    let reference = Session::open(
        scratch("boundary-recovered-reference"),
        Script::with(&[Step::Call("one")]),
    );
    reference.script.hold(Hold::Model, true);
    reference.send("boundary", "work", Delivery::Queue).await;
    reference.wait_entered().await;
    let run = RunId("run-boundary".into());
    assert_eq!(
        reference
            .registry
            .route_cancel_at(&session(), run.clone(), RunCancelAt::StepBoundary)
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    reference.script.hold(Hold::Model, false);
    let complete = reference.settle().await;
    let cut = complete
        .records()
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap()
        + 1;
    let recovery = Session::open(scratch("boundary-recovered-escalation"), Script::with(&[]));
    recovery.script.hold(Hold::Tool, true);
    let path = recovery.wal_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, &complete.bytes[..complete.end_of_records(cut)]).unwrap();
    recovery.registry.status(&session()).await.unwrap();
    recovery.wait_entered().await;
    let before = recovery.log().bytes;
    assert_eq!(
        recovery
            .registry
            .route_cancel_at(&session(), run.clone(), RunCancelAt::StepBoundary)
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    assert_eq!(
        recovery.log().bytes,
        before,
        "a replayed boundary request must not be written twice"
    );
    assert_eq!(
        recovery
            .registry
            .route_cancel(&session(), run)
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    assert!(
        matches!(
            recovery.log().records().last(),
            Some(WalRecord::RunCancelRequested {
                at: RunCancelAt::Now,
                ..
            })
        ),
        "recovered boundary intent must make escalation durable before ack"
    );
    recovery.script.hold(Hold::Tool, false);
    let log = recovery.settle().await;
    assert!(matches!(
        log.records().last(),
        Some(WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Cancelled,
            ..
        })
    ));
}

#[tokio::test]
async fn paused_boundary_cancel_acks_and_seals_without_another_step() {
    let live = Session::open(scratch("boundary-paused"), Script::with(&[]));
    *live.script.pause_next_run.lock().unwrap() = true;
    live.send("boundary", "work", Delivery::Queue).await;
    live.settle().await;
    assert_eq!(
        live.registry
            .route_cancel_at(
                &session(),
                RunId("run-boundary".into()),
                RunCancelAt::StepBoundary
            )
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    let log = live.settle().await;
    assert!(log
        .records()
        .iter()
        .any(|record| matches!(record, WalRecord::RunCancelRequested { .. })));
    assert!(!log
        .records()
        .iter()
        .any(|record| matches!(record, WalRecord::StepStarted { .. })));
    assert!(matches!(
        log.records().last(),
        Some(WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Cancelled,
            ..
        })
    ));
}
