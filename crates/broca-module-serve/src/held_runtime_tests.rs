use super::*;
use broca_core::tool_plane::LateResultRegistration;
use broca_module::late_results_puller::{FeedError, FeedReply, HeldDelivery, LateResultsFeed};
use broca_protocol::ToolCallId;
use cortexkit_role_tool_provider::late_results::{Cursor, LateEntry};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};

/// A provider's late-results feed with one entry, which records that intake pulled it. No
/// session store is installed, so the entry is only pulled, never delivered: what the tests
/// using it observe is when intake starts reading the feed.
#[derive(Default)]
struct Feed(AtomicBool);

#[async_trait::async_trait]
impl LateResultsFeed for Feed {
    async fn pull(
        &self,
        _: &str,
        _: Option<&Cursor>,
        _: Option<u32>,
    ) -> Result<FeedReply, FeedError> {
        self.0.store(true, Ordering::SeqCst);
        Ok(FeedReply {
            entries: vec![LateEntry::new(
                "result",
                "reserved:broca",
                "ref",
                1,
                "reserved:broca",
                "held:4",
                "held-result",
                1,
            )
            .with_result(json!({"content":[{"type":"text","text":"late"}]}))],
            cursor: Cursor {
                provider_incarnation: "inc".into(),
                seq: 1,
            },
            more: false,
        })
    }
    async fn ack(&self, _: &str, _: &Cursor) -> Result<(), FeedError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl HeldDelivery for Feed {
    async fn deliver(
        &self,
        _: &LateResultRegistration,
        _: &ToolCallId,
        _: &LateEntry,
    ) -> Result<(), String> {
        Ok(())
    }
}

struct NoWithdraw;
#[async_trait::async_trait]
impl WithdrawTransport for NoWithdraw {
    async fn withdraw(
        &self,
        _: &LateResultRegistration,
        _: bool,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        serde_json::Value,
    ) {
        panic!("no call in this fixture was cancelled")
    }
}

fn registration(key: String, bind: &BindIdentity) -> LateResultRegistration {
    LateResultRegistration {
        lineage: key.rsplit_once(':').unwrap().0.into(),
        call_key: key,
        module: "basal".into(),
        bind: bind.clone(),
        scope: None,
    }
}

#[tokio::test]
async fn large_stale_outbox_does_not_delay_held_result_intake() {
    let root = broca_test_support::scratch_root().join("held-runtime-pullers-first");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
    let mut calls = Vec::new();
    // A large stale withdrawal outbox: watches retained for calls whose intake already
    // finished, so the withdrawal sweep has thousands of rows to walk.
    for session in 0..120 {
        let bind = BindIdentity::new("/project", "broca", format!("stale-{session:03}"));
        for call in 0..174 {
            calls.push(registration(format!("stale-{session}:{}", call + 4), &bind));
        }
    }
    store.register_calls(&calls).unwrap();
    // Old intake pruning did not retire the independently retained withdrawal watches.
    store
        .remove_calls(
            &calls
                .iter()
                .map(|call| (call.module.clone(), call.call_key.clone()))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let held = BindIdentity::new("/project", "broca", "held");
    store
        .register_calls(&[registration("held:4".into(), &held)])
        .unwrap();
    let feed = Arc::new(Feed::default());
    let withdrawals = HeldWithdrawals::new(store.clone(), Arc::new(NoWithdraw));
    let intake = LateResultsIntake::new(store.clone(), "broca", feed.clone(), feed.clone());
    let runtime = Arc::new(HeldRuntime {
        slots: HeldSlots::new(),
        intake,
        store,
        state_root: root,
        needs_rebuild: false,
        withdrawals,
        progress: Arc::new(Mutex::new(RebuildReport::default())),
    });
    let stopping = tokio_util::sync::CancellationToken::new();
    runtime.bind_stopping_signal(stopping.clone());
    let recovery = {
        let runtime = runtime.clone();
        let stopping = stopping.clone();
        tokio::spawn(async move {
            runtime.recover_feeds(vec!["basal".into()], stopping).await;
        })
    };
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !feed.0.load(Ordering::SeqCst)
            || runtime.withdrawals.health()["sweep_running"] != true
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let health = runtime.withdrawals.health();
    stopping.cancel();
    recovery.await.unwrap();
    assert!(
        delivered.is_ok(),
        "withdrawal recovery delayed intake: {health}"
    );
    assert_eq!(
        health["sweep_running"], true,
        "intake must work before the outbox sweep ends"
    );
    assert!(health["rows_remaining"].as_u64().unwrap() > 0);
}
