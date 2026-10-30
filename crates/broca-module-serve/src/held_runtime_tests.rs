use super::*;
use broca_core::tool_plane::LateResultRegistration;
use broca_core::FileWalStore;
use broca_module::late_results_puller::{FeedError, FeedReply, HeldDelivery, LateResultsFeed};
use broca_protocol::content::{AssistantMessage, ContentBlock, ToolCall};
use broca_protocol::{BatchId, MessageId, RunId, StepId, ToolCallId};
use broca_wal::{DispatchOutcome, WalRecord};
use cortexkit_role_tool_provider::late_results::{Cursor, LateEntry};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};

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
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct NoWithdraw;
#[async_trait::async_trait]
impl WithdrawTransport for NoWithdraw {
    async fn withdraw(
        &self,
        _: &LateResultRegistration,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        serde_json::Value,
    ) {
        panic!("no call in this fixture was cancelled")
    }
}

fn config() -> broca_wal::RunConfig {
    broca_wal::RunConfig {
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
        model: broca_protocol::request::ModelHandle::new("fake", "fake"),
        tools: vec![].into(),
        tool_choice: Default::default(),
        generation: Default::default(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        context_management: None,
    }
}

fn records(bind: &BindIdentity) -> Vec<WalRecord> {
    vec![
        WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
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
            ts_ms: Some(1),
            run_id: RunId("run".into()),
            session: bind.clone(),
            config: Box::new(config()),
            input: vec![].into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("a".into()),
                    tool_name: "ask".into(),
                    input: json!({}),
                    thought_signature: None,
                })],
            }),
            usage: Default::default(),
            retries_used: 0,
            finish_reason: None,
            service_tier_used: None,
            speed_used: None,
        },
        WalRecord::ToolBatchStarted {
            batch_id: BatchId("b".into()),
            step_id: StepId(1),
            planned_calls: vec![ToolCallId("a".into())],
        },
    ]
}

fn intent(id: &str) -> WalRecord {
    WalRecord::ToolDispatchIntent {
        dispatched_at_ms: None,
        batch_id: BatchId("b".into()),
        tool_call_id: ToolCallId(id.into()),
        tool_name: Some("ask".into()),
        args: Arc::new(json!({})),
        module_id: Some("basal".into()),
        schema_pin: None,
        hooks: None,
        approval_digest: None,
        call_key: None,
    }
}

fn write(wal: &FileWalStore, bind: &BindIdentity, records: &[WalRecord]) {
    let path = wal.wal_path(bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let bytes: Vec<_> = records
        .iter()
        .enumerate()
        .flat_map(|(i, record)| {
            broca_wal::framing::encode_payload(
                i as u64 + 1,
                1,
                &serde_json::to_vec(record).unwrap(),
            )
            .unwrap()
        })
        .collect();
    std::fs::write(path, bytes).unwrap();
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
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
    let mut calls = Vec::new();
    for session in 0..120 {
        let bind = BindIdentity::new("/project", "broca", format!("stale-{session:03}"));
        let mut log = records(&bind);
        if let WalRecord::ModelStepFinished {
            assistant_message, ..
        } = &mut log[1]
        {
            Arc::make_mut(assistant_message).content = (0..174)
                .map(|call| {
                    ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId(format!("inline-{call}")),
                        tool_name: "ask".into(),
                        input: json!({}),
                        thought_signature: None,
                    })
                })
                .collect();
        }
        if let WalRecord::ToolBatchStarted { planned_calls, .. } = &mut log[2] {
            *planned_calls = (0..174)
                .map(|call| ToolCallId(format!("inline-{call}")))
                .collect();
        }
        for call in 0..174 {
            let key = format!("stale-{session}:{}", log.len() + 1);
            let id = format!("inline-{call}");
            log.push(intent(&id));
            log.push(WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                hooks: None,
                batch_id: BatchId("b".into()),
                result: Arc::new(broca_protocol::ToolResult {
                    tool_call_id: ToolCallId(id),
                    output: broca_protocol::ToolOutput::Text {
                        text: "inline".into(),
                    },
                    is_error: false,
                }),
                outcome: DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            });
            calls.push(registration(key, &bind));
        }
        log.push(WalRecord::ToolBatchFinished {
            batch_id: BatchId("b".into()),
        });
        write(&wal, &bind, &log);
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
    let mut log = records(&held);
    log.push(intent("a"));
    log.push(WalRecord::ToolCallProviderHeld {
        batch_id: BatchId("b".into()),
        tool_call_id: ToolCallId("a".into()),
        call_key: "held:4".into(),
        module: "basal".into(),
        reason: "running".into(),
        detail_digest: None,
        held_at_ms: 1,
        sequential: false,
    });
    log.push(WalRecord::RunPaused {
        run_id: RunId("run".into()),
        reason: broca_protocol::PauseReason::ToolHeld,
        error: None,
        ts_ms: Some(2),
    });
    write(&wal, &held, &log);
    store
        .register_calls(&[registration("held:4".into(), &held)])
        .unwrap();
    let feed = Arc::new(Feed::default());
    let withdrawals = HeldWithdrawals::new(store.clone(), wal.clone(), Arc::new(NoWithdraw));
    let intake = LateResultsIntake::new(store.clone(), wal, "broca", feed.clone(), feed.clone());
    let runtime = Arc::new(HeldRuntime {
        slots: HeldSlots::new(),
        intake,
        store,
        wal_root: root,
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
