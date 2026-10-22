//! Session-wide limits exercise the real WAL-backed runner, not a parallel accounting model.
use broca_core::{
    run_loop, AllowAll, FileLeaseStore, FileWalStore, RunRequest, StubProvider, StubToolPlane,
    WalStore,
};
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::{
    AssistantMessage, BindIdentity, BudgetConfig, ContentBlock, FinishReason, GenerationConfig,
    Message, MessageId, ModelHandle, Role, RunFinishReason, RunId, ToolCall, ToolCallId,
    ToolChoice, Usage,
};
use broca_wal::{DispatchOutcome, WalRecord};
use tokio::sync::mpsc;

struct Rig {
    root: std::path::PathBuf,
    session: BindIdentity,
    wal: FileWalStore,
    lease: FileLeaseStore,
    plane: StubToolPlane,
}
impl Rig {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "input-cap-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        Self {
            session: BindIdentity::new(&root, "input-cap", "s"),
            wal: FileWalStore::new(root.join("wal")),
            lease: FileLeaseStore::new(root.join("lease")),
            plane: StubToolPlane::new().with_canned_text("edit", "done"),
            root,
        }
    }
    fn request(&self, id: &str, limit: Option<u64>) -> RunRequest {
        RunRequest {
            directives: None,
            session_class: None,
            retention: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId(id.into()),
            session: self.session.clone(),
            model: ModelHandle::new("stub", "m"),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text { text: id.into() }],
            )],
            tools: vec![broca_protocol::Tool {
                name: "edit".into(),
                description: "edit".into(),
                input_schema: serde_json::json!({"type":"object"}),
                module: None,
                execution_mode: Default::default(),
                reply: None,
            }],
            policy_allowed: ["edit".into()].into_iter().collect(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: vec![],
            submission_id: None,
            budget: limit.map(|limit| BudgetConfig {
                max_input_tokens: Some(limit),
                ..Default::default()
            }),
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
            structured_output: None,
        }
    }
    async fn send(
        &self,
        id: &str,
        limit: Option<u64>,
        provider: &StubProvider,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        let (tx, mut rx) = mpsc::channel(128);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let result = run_loop(
            self.request(id, limit),
            provider,
            &self.plane,
            &self.lease,
            &AllowAll,
            &self.wal,
            &tx,
        )
        .await;
        drop(tx);
        drain.await.unwrap();
        result
    }
    fn records(&self) -> Vec<WalRecord> {
        self.wal
            .read_records(&self.session)
            .unwrap()
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    }
    fn detail(&self, run: &str) -> broca_protocol::BudgetExhaustedDetail {
        broca_core::reconcile_run_state(self.records().iter(), run)
            .unwrap()
            .budget_detail
            .unwrap()
    }
}
impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn step(input: Option<u64>, cached: u64, writes: u64, calls: bool) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content: if calls {
                    vec![ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId("t".into()),
                        tool_name: "edit".into(),
                        input: serde_json::json!({}),
                        thought_signature: None,
                    })]
                } else {
                    vec![ContentBlock::Text {
                        text: "done".into(),
                    }]
                },
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: input,
                cached_input_tokens: Some(cached),
                cache_write_tokens: Some(writes),
                output_tokens: Some(999),
                ..Default::default()
            },
            finish_reason: if calls {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
            retries_used: 0,
        }),
    ]
}

#[tokio::test]
async fn exactly_at_limit_starts_no_next_model_step() {
    let rig = Rig::new();
    assert_eq!(
        rig.send(
            "first",
            Some(10),
            &StubProvider::new(vec![step(Some(3), 4, 3, false)])
        )
        .await
        .unwrap()
        .reason,
        RunFinishReason::BudgetExhausted
    );
    assert_eq!(rig.detail("first").spent, 10);
    let provider = StubProvider::new(vec![step(Some(0), 0, 0, false)]);
    assert_eq!(
        rig.send("second", None, &provider).await.unwrap().reason,
        RunFinishReason::BudgetExhausted
    );
    assert!(
        provider.calls().is_empty(),
        "no paid step starts at the limit"
    );
    assert_eq!(rig.detail("second").spent, 10);
}

#[tokio::test]
async fn crossing_step_dispatches_no_tools_and_repairs_each_call() {
    let rig = Rig::new();
    let provider = StubProvider::new(vec![step(Some(4), 5, 6, true), step(Some(1), 0, 0, false)]);
    let summary = rig.send("r", Some(14), &provider).await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::BudgetExhausted);
    assert_eq!(provider.calls().len(), 1);
    assert_eq!(rig.plane.exec_count(&ToolCallId("t".into())), 0);
    let records = rig.records();
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::ToolDispatchIntent { .. })));
    let results: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ToolResult {
                result,
                reason,
                outcome,
                ..
            } => Some((result, reason, outcome)),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].0.is_error);
    assert_eq!(results[0].1.as_deref(), Some("budget_exhausted"));
    assert_eq!(*results[0].2, DispatchOutcome::MaxSteps);
    assert_eq!(rig.detail("r").spent, 15);
    let replayed = broca_core::replay(&records).unwrap();
    assert!(replayed.open_tool_turn.is_none());
}

#[tokio::test]
async fn wal_sum_survives_new_episode_and_new_runner() {
    let rig = Rig::new();
    assert_eq!(
        rig.send(
            "first",
            Some(20),
            &StubProvider::new(vec![step(Some(3), 4, 3, false)])
        )
        .await
        .unwrap()
        .reason,
        RunFinishReason::Completed
    );
    // A separate invocation opens a new writer and seeds from durable session history.
    assert_eq!(
        rig.send(
            "second",
            None,
            &StubProvider::new(vec![step(Some(4), 2, 4, false)])
        )
        .await
        .unwrap()
        .reason,
        RunFinishReason::BudgetExhausted
    );
    let detail = rig.detail("second");
    assert_eq!(
        (detail.limit, detail.spent, detail.cause),
        (20, 20, "limit_reached".to_string())
    );
    let frames = rig.wal.read_records(&rig.session).unwrap();
    let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&frames).unwrap();
    let state = checkpoint.input_budget.as_ref().unwrap();
    assert_eq!(
        (state.limit, state.spent, state.usage_unreported),
        (Some(20), 20, false)
    );
    let encoded = broca_core::encode_admission_checkpoint(&checkpoint).unwrap();
    assert_eq!(
        broca_core::decode_admission_checkpoint(&encoded)
            .unwrap()
            .input_budget,
        checkpoint.input_budget
    );
}

#[tokio::test]
async fn lower_only_omission_and_late_adoption() {
    let rig = Rig::new();
    let zero = || StubProvider::new(vec![step(Some(0), 0, 0, false)]);
    rig.send("uncapped", None, &zero()).await.unwrap();
    rig.send("adopt", Some(20), &zero()).await.unwrap();
    let before = rig.records().len();
    assert!(rig
        .send("raise", Some(21), &zero())
        .await
        .unwrap_err()
        .to_string()
        .contains("budget.max_input_tokens"));
    assert_eq!(rig.records().len(), before);
    rig.send("lower", Some(10), &zero()).await.unwrap();
    rig.send("omit", None, &zero()).await.unwrap();
    let state = broca_core::replay(&rig.records()).unwrap().input_budget;
    assert_eq!((state.limit, state.spent), (Some(10), 0));
    assert!(rig
        .send("zero", Some(0), &zero())
        .await
        .unwrap_err()
        .to_string()
        .contains("budget.max_input_tokens"));
}

#[tokio::test]
async fn unreported_usage_fails_closed_before_dispatch() {
    let rig = Rig::new();
    rig.send(
        "first",
        Some(50),
        &StubProvider::new(vec![step(Some(7), 2, 1, false)]),
    )
    .await
    .unwrap();
    let provider = StubProvider::new(vec![step(None, 9, 9, true), step(Some(0), 0, 0, false)]);
    assert_eq!(
        rig.send("second", None, &provider).await.unwrap().reason,
        RunFinishReason::BudgetExhausted
    );
    assert_eq!(provider.calls().len(), 1);
    assert_eq!(rig.plane.exec_count(&ToolCallId("t".into())), 0);
    assert_eq!(rig.detail("second").cause, "usage_unreported");
    assert_eq!(
        rig.detail("second").spent,
        10,
        "only measured steps before the unreported step count"
    );
}
