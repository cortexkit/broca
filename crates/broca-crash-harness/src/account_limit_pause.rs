use super::{cut_log, write_log};
use crate::{
    scenario::{HarnessPlane, Render, ScriptedProvider},
    wal::{wal_path, LogImage, ObservedWalStore},
};
use broca_core::{
    AllowAll, Clock, FileLeaseStore, Provider, RunControl, RunRequest, StubReply, StubToolPlane,
};
use broca_protocol::{
    caller::{ControlUnit, PauseReason, RunFinishReason},
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{
        CallOptions, ExecutionMode, GenerationConfig, ModelHandle, StopCondition, Tool, ToolChoice,
    },
    BindIdentity, FinishReason, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::WalRecord;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc;

fn request() -> RunRequest {
    RunRequest {
        input_written_at_ms: None,
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
        run_id: RunId("timed-account".into()),
        session: BindIdentity::new("/fixture/project", "runner", "timed"),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            turn_system: false,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "edit once".into(),
            }],
        }],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: serde_json::json!({"type":"object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: ["edit".to_owned()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

fn script(tool: bool) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(if tool { "tool" } else { "answer" }.into()),
                content: if tool {
                    vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("once".into()),
                        tool_name: "edit".into(),
                        input: serde_json::json!({}),
                    })]
                } else {
                    vec![ContentBlock::Text {
                        text: "done".into(),
                    }]
                },
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: if tool {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
            retries_used: 0,
        }),
    ]
}

struct Limited {
    inner: ScriptedProvider,
    reject: AtomicBool,
    clock: Clock,
    sent: Mutex<Vec<(StepId, Vec<u8>)>>,
}

impl Limited {
    fn new(clock: Clock, reject: bool) -> Self {
        Self {
            inner: ScriptedProvider::new(
                vec![script(true), script(false)],
                Render::IdentityEmbedding,
            ),
            reject: AtomicBool::new(reject),
            clock,
            sent: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl Provider for Limited {
    async fn step(&self, step: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.step_with_runtime(
            step,
            Arc::new(opts.clone()),
            broca_core::provider::StepRuntime {
                cancel: Default::default(),
                deadline: None,
                seal: None,
                progress: None,
                first_attempt: 0,
                attempt_hook: None,
            },
        )
        .await
    }
    async fn step_with_runtime(
        &self,
        step: StepId,
        opts: Arc<CallOptions>,
        runtime: broca_core::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let bytes = self.render_request(&opts);
        self.sent.lock().unwrap().push((step, bytes.clone()));
        let rejected = step == StepId(2) && self.reject.swap(false, Ordering::SeqCst);
        let observed = self.clock.now_ms();
        let mut attempt: broca_protocol::ModelAttempt = serde_json::from_value(serde_json::json!({
            "attempt":runtime.first_attempt, "request_body_len":bytes.len(),
            "request_body_sha256":broca_core::render::request_digest(&bytes).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "response_body_len":0, "response_body_sha256":"0".repeat(64),
            "outcome":if rejected {"http_error"} else {"complete"}, "ts_ms":observed
        }))
        .unwrap();
        if rejected {
            attempt.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
                disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
                observed_at_ms: observed,
                resets_at_ms: Some(61_000),
                resets_at_source: Some("body".into()),
                limit_window: None,
            });
        }
        let source = if rejected {
            None
        } else {
            Some(self.inner.step(step, &opts).await)
        };
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            if tx
                .send(NormalizedEvent::Control(ProviderControl::AttemptFinished {
                    attempt,
                }))
                .await
                .is_err()
            {
                return;
            }
            if let Some(mut source) = source {
                while let Some(event) = source.recv().await {
                    if tx.send(event).await.is_err() {
                        break;
                    }
                }
            } else {
                let _ = tx.send(NormalizedEvent::Control(ProviderControl::Error {
                    error:serde_json::from_value(serde_json::json!({"class":"transient", "message":"account exhausted", "provider_code":"account_limit_exhausted", "resets_at_ms":61_000})).unwrap(), retries_used:0,
                })).await;
            }
        });
        rx
    }
    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        self.inner.render_request(opts)
    }
    fn reports_attempt_evidence(&self) -> bool {
        true
    }
}

fn clock(root: &std::path::Path, now: u64) -> Clock {
    std::fs::create_dir_all(root).unwrap();
    std::fs::write(
        root.join("test-clock"),
        serde_json::to_vec(&serde_json::json!({"now_ms":now})).unwrap(),
    )
    .unwrap();
    Clock::for_state_root(root).unwrap()
}

async fn drive(
    root: &std::path::Path,
    store: Arc<ObservedWalStore>,
    provider: &Limited,
    plane: &HarnessPlane,
) -> (broca_core::RunSummary, Vec<u64>) {
    let (tx, mut rx) = mpsc::channel(256);
    let observed = store.clone();
    let collector = tokio::spawn(async move {
        let mut published = Vec::new();
        while let Some(event) = rx.recv().await {
            if matches!(
                event,
                broca_core::OutputEvent::Control(ControlUnit::Paused {
                    reason: PauseReason::AccountLimit,
                    ..
                })
            ) {
                published.extend(observed.synced());
            }
        }
        published
    });
    let summary = broca_core::run_loop_with_control(
        request(),
        provider,
        plane,
        &FileLeaseStore::new(root.join("leases")),
        &AllowAll,
        store.as_ref(),
        None,
        RunControl {
            account_limit_pause: true,
            clock: Some(provider.clock.clone()),
            auth_selection: Some(
                serde_json::from_value(serde_json::json!({"credential_id":"oauth:stub:work"}))
                    .unwrap(),
            ),
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    (summary, collector.await.unwrap())
}

fn plane() -> HarnessPlane {
    HarnessPlane::new(StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text {
            text: "edited".into(),
        })
    }))
}

#[tokio::test]
async fn account_limit_pause_and_resume_survive_every_record_and_torn_cut_without_tool_resend() {
    let base = broca_test_support::scratch_root().join("account-limit-cuts");
    let reference_root = base.join("reference");
    let reference_clock = clock(&reference_root, 1000);
    let provider = Limited::new(reference_clock.clone(), true);
    let store = Arc::new(ObservedWalStore::new(reference_root.join("wal")));
    let (paused, published_after) =
        drive(&reference_root, store.clone(), &provider, &plane()).await;
    assert_eq!(paused.paused, Some(PauseReason::AccountLimit));
    let paused_log = LogImage::read(&wal_path(&reference_root, &request().session));
    let pause_seq = paused_log
        .framed()
        .iter()
        .find_map(|frame| {
            matches!(
                frame.record,
                WalRecord::RunPaused {
                    reason: PauseReason::AccountLimit,
                    ..
                }
            )
            .then_some(frame.seq)
        })
        .unwrap();
    assert!(
        published_after.contains(&pause_seq),
        "pause publication must follow its existing fsync barrier"
    );
    assert!(paused_log
        .framed()
        .iter()
        .filter(|f| matches!(
            f.record,
            WalRecord::ToolResult { .. } | WalRecord::ModelAttemptFinished { .. }
        ))
        .all(|frame| frame.seq < pause_seq));
    std::fs::write(reference_root.join("test-clock"), br#"{"now_ms":61000}"#).unwrap();
    reference_clock.refresh().unwrap();
    assert_eq!(
        drive(&reference_root, store, &provider, &plane())
            .await
            .0
            .reason,
        RunFinishReason::Completed
    );
    let reference = LogImage::read(&wal_path(&reference_root, &request().session));
    let reference_sent = provider.sent.lock().unwrap().clone();
    assert_eq!(reference_sent.len(), 3);
    assert_eq!(reference_sent[1], reference_sent[2]);
    assert!(reference
        .records()
        .iter()
        .any(|r| matches!(r, WalRecord::ResumeStarted { .. })));

    for records in 0..=reference.record_count() {
        for torn in [false, true] {
            if torn && records == reference.record_count() {
                continue;
            }
            let root = base.join(format!("cut-{records}-{torn}"));
            let cut = cut_log(&reference, records, torn);
            let prefix = LogImage::parse(cut.clone());
            write_log(&wal_path(&root, &request().session), &cut);
            let provider = Limited::new(clock(&root, 61_000), false);
            let plane = plane();
            let store = Arc::new(ObservedWalStore::new(root.join("wal")));
            let result = drive(&root, store, &provider, &plane).await.0;
            let before = prefix.records();
            let intent = before
                .iter()
                .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. }));
            let terminal = before.iter().any(|r| {
                matches!(
                    r,
                    WalRecord::ToolResult { .. } | WalRecord::ToolNoDispatch { .. }
                )
            });
            assert_eq!(
                result.reason,
                if intent && !terminal {
                    RunFinishReason::Interrupted
                } else {
                    RunFinishReason::Completed
                },
                "cut {records}, torn={torn}"
            );
            if intent {
                assert_eq!(
                    plane.dispatch_count(&ToolCallId("once".into())),
                    0,
                    "cut {records}, torn={torn}"
                );
            }
            let after = LogImage::read(&wal_path(&root, &request().session));
            assert!(after
                .bytes
                .starts_with(&prefix.bytes[..prefix.end_of_records(prefix.record_count())]));
            assert_eq!(
                after
                    .records()
                    .iter()
                    .filter(|r| matches!(r, WalRecord::ToolDispatchIntent { .. }))
                    .count(),
                1,
                "cut {records}, torn={torn}"
            );
            for (step, bytes) in provider.sent.lock().unwrap().iter() {
                let expected = reference_sent
                    .iter()
                    .find(|(reference_step, _)| reference_step == step)
                    .unwrap();
                assert_eq!(
                    *bytes, expected.1,
                    "request drift at cut {records}, torn={torn}, step={step:?}"
                );
            }
            assert!(provider.inner.rejected().is_empty());
        }
    }
}
crate::require_process_kill!();
