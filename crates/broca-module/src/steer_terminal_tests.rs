use super::tests::{min_config, text_req, SeamOpener};
use super::*;
use crate::registry::LineageRegistry;
use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore};
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    BindIdentity, Delivery, RunFinishReason, Usage,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
struct ClosedInboxExecutor {
    runs: AtomicUsize,
    first_closed: Notify,
    release_first: Notify,
    next_started: Notify,
    release_next: Notify,
    next_input: std::sync::Mutex<Vec<Message>>,
}

#[async_trait]
impl RunExecutor for ClosedInboxExecutor {
    async fn execute(
        &self,
        job: RunJob,
        _: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let run = self.runs.fetch_add(1, Ordering::SeqCst);
        job.writer.record(WalRecord::RunStarted {
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: job.req.run_id.clone(),
            session: job.req.session.clone(),
            config: Box::new(min_config()),
            input: job.req.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: job.req.submission_id.clone(),
        });
        job.writer.sync().await?;
        if let Some(ack) = job.on_admitted {
            let _ = ack.send(Ok(job.req.run_id));
        }
        job.control.steer.as_ref().unwrap().close();
        if run == 0 {
            self.first_closed.notify_one();
            self.release_first.notified().await;
        } else {
            *self.next_input.lock().unwrap() = job.req.input;
            self.next_started.notify_one();
            self.release_next.notified().await;
        }
        job.writer.record(WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        });
        job.writer.sync().await?;
        Ok(RunSummary {
            reason: RunFinishReason::Completed,
            paused: None,
            steps: 0,
            usage: Usage::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
}

#[tokio::test]
async fn refused_live_steer_is_queued_for_next_turn() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-closed-steer-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let key = BindIdentity::new(dir.join("proj"), "runner", "s");
    let executor = Arc::new(ClosedInboxExecutor::default());
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let registry = LineageRegistry::new(Arc::new(SeamOpener {
        lease_store: Arc::new(FileLeaseStore::new(dir.join("lease"))),
        wal_store: wal.clone(),
        idle_ttl: Duration::from_secs(60),
        executor: executor.clone(),
    }));
    registry
        .route_send(&key, text_req(&key, "first"), RunKind::FreshOrResume)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), executor.first_closed.notified())
        .await
        .unwrap();
    let late = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "next turn only".into(),
        }],
    )];
    let mut request = text_req(&key, "next").to_submission();
    request.input = late.clone();
    let outcome = registry
        .route_submission_with_delivery(
            &key,
            RunId("next".into()),
            request,
            RunKind::FreshOrResume,
            SendKey::SendId("late-steer".into()),
            false,
            Delivery::Steer,
        )
        .await
        .unwrap();
    let queued = wal.read_records(&key).unwrap();
    executor.release_first.notify_one();
    if !matches!(outcome, SendOutcome::Pending { .. }) {
        executor.release_next.notify_one();
        panic!("refused steer must be queued, got {outcome:?}");
    }
    tokio::time::timeout(Duration::from_secs(10), executor.next_started.notified())
        .await
        .unwrap();
    let next_input = executor.next_input.lock().unwrap().clone();
    executor.release_next.notify_one();
    assert!(queued.iter().any(
        |frame| matches!(&frame.record, WalRecord::Queued { request, .. } if request.input == late)
    ));
    assert!(!queued
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::Steered { .. })));
    assert_eq!(
        next_input, late,
        "the queued steer reaches the next run's input"
    );
    assert!(registry.stop_and_seal().await);
    std::fs::remove_dir_all(dir).unwrap();
}
