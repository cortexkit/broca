//! Exercise projection health through the ordinary library, not a cfg(test) copy of its metrics.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    Arc,
};

use broca_core::{
    MaterializedStore, Persister, RunRequest, SessionStore, StorePrefix, StubProvider,
    StubToolPlane,
};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock},
    provider::{NormalizedEvent, ProviderControl},
    BindIdentity, FinishReason, MessageId, RunFinishReason, RunId, SubmissionId, Usage,
};
use broca_wal::FramedRecord;

struct RecoverableStore {
    fail: AtomicBool,
    attempts: AtomicUsize,
    projected_records: AtomicUsize,
}

impl MaterializedStore for RecoverableStore {
    fn read_prefix_with_watermark(&self, _: &BindIdentity) -> Result<StorePrefix, String> {
        Ok(StorePrefix::empty())
    }
}

impl Persister for RecoverableStore {
    fn project(&self, _: &BindIdentity, records: &[FramedRecord]) -> Result<u64, String> {
        self.attempts.fetch_add(1, SeqCst);
        if self.fail.load(SeqCst) {
            return Err("injected projection-store failure".into());
        }
        self.projected_records.store(records.len(), SeqCst);
        Ok(records.last().map_or(0, |record| record.seq))
    }
}

// A separate integration binary keeps exact process-wide metric assertions independent of
// other tests' projection failures, while still linking the non-test library.
#[tokio::test]
async fn projection_failure_then_recovery_updates_production_metrics() {
    let root = broca_test_support::scratch_root()
        .join(format!("projection-health-{}", std::process::id()));
    let session = BindIdentity::new(&root, "projection", "health");
    let store = Arc::new(RecoverableStore {
        fail: AtomicBool::new(true),
        attempts: AtomicUsize::new(0),
        projected_records: AtomicUsize::new(0),
    });
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let lease = broca_core::FileLeaseStore::new(root.join("leases"));
    let warnings_before = broca_core::run::projection_failures();
    let mut warnings_after_failure = warnings_before;
    assert_eq!(broca_core::run::projection_behind_sessions(), 0);

    for append in [false, true] {
        let request = RunRequest::from_submission(
            serde_json::from_value(serde_json::json!({
                "model": {"provider_module_id": "stub", "model_id": "m"},
                "input": [{"role": "user", "content": [{"type": "text", "text": "go"}]}]
            }))
            .unwrap(),
            RunId(format!("health-{append}")),
            session.clone(),
            SubmissionId(format!("sub-{append}")),
        );
        let model = StubProvider::new(vec![vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("answer".into()),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ]]);
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let shared: Arc<dyn SessionStore> = store.clone();
        let summary = if append {
            broca_core::run_loop_append_episode(
                request,
                &model,
                &StubToolPlane::new(),
                &lease,
                &broca_core::AllowAll,
                &wal,
                Some(shared),
                &tx,
            )
            .await
        } else {
            broca_core::run_loop_with_store(
                request,
                &model,
                &StubToolPlane::new(),
                &lease,
                &broca_core::AllowAll,
                &wal,
                Some(shared),
                &tx,
            )
            .await
        }
        .unwrap();
        drop(tx);
        collector.await.unwrap();
        assert_eq!(summary.reason, RunFinishReason::Completed);
        assert!(store.attempts.load(SeqCst) > 0);
        if !append {
            let failed_count = broca_core::run::projection_failures();
            assert!(failed_count > warnings_before);
            warnings_after_failure = failed_count;
            assert_eq!(broca_core::run::projection_behind_sessions(), 1);
            assert_eq!(store.projected_records.load(SeqCst), 0);
            store.fail.store(false, SeqCst);
        } else {
            assert_eq!(broca_core::run::projection_behind_sessions(), 0);
            assert_eq!(
                broca_core::run::projection_failures(),
                warnings_after_failure
            );
            assert!(store.projected_records.load(SeqCst) > 0);
        }
    }
    assert!(broca_core::run::projection_failures() > warnings_before);
    std::fs::remove_dir_all(root).unwrap();
}
