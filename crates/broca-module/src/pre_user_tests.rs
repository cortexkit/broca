use super::*;
use crate::{opener::DefaultOpener, registry::LineageRegistry};
use broca_core::hooks::{HookAnswer, HookCall};
use broca_core::{
    FileLeaseStore, FileWalStore, HookPlane, HookUnavailable, Provider, StartMode, StubToolPlane,
};
use broca_protocol::content::{
    AssistantMessage, ContentBlock, FinishReason, Message, Role, ToolCall,
};
use broca_protocol::{BindIdentity, Delivery, MessageId, NormalizedEvent, StepId, ToolCallId};
use serde_json::json;
use std::sync::{atomic::AtomicBool, Mutex};
use tokio::sync::{Notify, Semaphore};

struct HooksRig {
    calls: Mutex<Vec<(String, HookCall)>>,
    unavailable: AtomicBool,
    second_unavailable: AtomicBool,
    pass: AtomicBool,
    hold: AtomicBool,
    entered: Semaphore,
    inbox: Mutex<Option<Arc<broca_core::SteerInbox>>>,
    close_inbox: AtomicBool,
}
#[async_trait::async_trait]
impl HookPlane for HooksRig {
    async fn hook(
        &self,
        provider: &str,
        _: &BindIdentity,
        call: HookCall,
    ) -> Result<HookAnswer, HookUnavailable> {
        self.calls
            .lock()
            .unwrap()
            .push((provider.into(), call.clone()));
        self.entered.add_permits(1);
        if self.close_inbox.load(Ordering::SeqCst) {
            self.inbox.lock().unwrap().as_ref().unwrap().close();
        }
        if self.hold.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.unavailable.load(Ordering::SeqCst)
            || (provider == "p1" && self.second_unavailable.load(Ordering::SeqCst))
        {
            return Err(HookUnavailable);
        }
        if self.pass.load(Ordering::SeqCst) {
            return Ok(HookAnswer::Pass);
        }
        Ok(serde_json::from_value(
            json!({"answer":"ops", "ops":[{"op":"append","block":0,"text":"-hooked"}]}),
        )
        .unwrap())
    }
}
impl Default for HooksRig {
    fn default() -> Self {
        Self {
            calls: Mutex::new(vec![]),
            unavailable: AtomicBool::new(false),
            second_unavailable: AtomicBool::new(false),
            pass: AtomicBool::new(false),
            hold: AtomicBool::new(false),
            entered: Semaphore::new(0),
            inbox: Mutex::new(None),
            close_inbox: AtomicBool::new(false),
        }
    }
}
impl HooksRig {
    fn count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}
struct Model {
    permits: Semaphore,
    entered: Notify,
    tool_calls: usize,
}
#[async_trait::async_trait]
impl Provider for Model {
    async fn step(
        &self,
        step: StepId,
        _: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(16);
        self.entered.notify_one();
        self.permits.acquire().await.unwrap().forget();
        let content = if step.0 == 1 && self.tool_calls > 0 {
            (0..self.tool_calls)
                .map(|i| {
                    ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId(format!("c{i}")),
                        tool_name: "edit".into(),
                        input: json!({}),
                        thought_signature: None,
                    })
                })
                .collect()
        } else {
            vec![ContentBlock::Text {
                text: "answer".into(),
            }]
        };
        tx.send(NormalizedEvent::Control(
            broca_protocol::ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId(format!("m{}", step.0)),
                    content,
                },
            },
        ))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(
            broca_protocol::ProviderControl::Finished {
                retries_used: 0,
                usage: Default::default(),
                finish_reason: if self.tool_calls > 0 && step.0 == 1 {
                    FinishReason::ToolCalls
                } else {
                    FinishReason::Stop
                },
            },
        ))
        .await
        .unwrap();
        rx
    }
}
struct CoreExecutor {
    wal: Arc<FileWalStore>,
    model: Arc<Model>,
    hooks: Arc<HooksRig>,
    ignore_graceful_stop: bool,
    writer: Mutex<Option<std::sync::Weak<broca_core::WalHandle>>>,
}
#[async_trait::async_trait]
impl RunExecutor for CoreExecutor {
    fn hook_plane(&self) -> Option<Arc<dyn HookPlane>> {
        Some(self.hooks.clone())
    }
    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        *self.writer.lock().unwrap() = Some(Arc::downgrade(&job.writer));
        *self.hooks.inbox.lock().unwrap() = job.control.steer.clone();
        if self.ignore_graceful_stop {
            job.control.stop_requested = None;
        }
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            match job.kind {
                RunKind::FreshOrResume => StartMode::FreshOrResume,
                RunKind::AppendEpisode => StartMode::AppendEpisode,
                RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            },
            self.model.as_ref(),
            &StubToolPlane::new().with_canned_text("edit", "tool"),
            &broca_core::AllowAll,
            self.wal.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}
struct Fixture {
    root: std::path::PathBuf,
    key: BindIdentity,
    wal: Arc<FileWalStore>,
    hooks: Arc<HooksRig>,
    model: Arc<Model>,
    registry: LineageRegistry,
    manifest: Arc<broca_wal::PlanManifest>,
    executor: Arc<CoreExecutor>,
}
fn manifest(hook: &str, policies: &[&str], tool_calls: usize) -> Arc<broca_wal::PlanManifest> {
    let subs: Vec<_> = policies.iter().map(|policy| json!({"hook":hook,"ops":["append"],"on_unavailable":policy,"budget_ms":30000})).collect();
    let items: Vec<_> = subs.iter().enumerate().map(|(i, sub)| json!({"provider":format!("p{i}"),"preset":"frozen","params":{"tag":i},"subscriptions":[sub]})).collect();
    Arc::new(broca_wal::PlanManifest {
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({"step_transform_items":items}).to_string(),
        )
        .unwrap(),
        plan_identity: "plan".into(),
        composition_digest: String::new(),
        tools: if tool_calls == 0 {
            vec![]
        } else {
            vec![broca_protocol::request::Tool {
                name: "edit".into(),
                description: String::new(),
                input_schema: json!({"type":"object"}),
                module: Some("aft".into()),
                execution_mode: broca_protocol::request::ExecutionMode::Parallel,
                reply: None,
            }]
        }
        .into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: subs
            .into_iter()
            .enumerate()
            .map(|(i, sub)| broca_wal::FrozenDeclaration {
                provider: format!("p{i}"),
                answer: broca_wal::OpaqueJson::from_json_text(
                    json!({"subscriptions":[sub]}).to_string(),
                )
                .unwrap(),
            })
            .collect(),
    })
}
impl Fixture {
    fn new(hook: &str, policies: &[&str], tool_calls: usize) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "module-input-hooks-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("project")).unwrap();
        let key = BindIdentity::new(root.join("project"), "caller-not-route-harness", "s");
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let hooks = Arc::new(HooksRig::default());
        let model = Arc::new(Model {
            permits: Semaphore::new(0),
            entered: Notify::new(),
            tool_calls,
        });
        let executor = Arc::new(CoreExecutor {
            wal: wal.clone(),
            model: model.clone(),
            hooks: hooks.clone(),
            ignore_graceful_stop: hook != "pre_user",
            writer: Mutex::new(None),
        });
        let registry = registry(&root, wal.clone(), executor.clone());
        Self {
            root,
            key,
            wal,
            hooks,
            model,
            registry,
            manifest: manifest(hook, policies, tool_calls),
            executor,
        }
    }
    fn request(&self, text: &str) -> SubmissionRequest {
        let mut request = super::tests::text_req(&self.key, "unused").to_submission();
        request.input = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )];
        request.tools = None;
        request.scope = broca_protocol::SendScope::Internal;
        request
    }
    async fn send(&self, id: &str, text: &str, delivery: Delivery, first: bool) -> SendOutcome {
        tokio::time::timeout(
            Duration::from_secs(15),
            self.registry.route_submission_with_plan(
                &self.key,
                RunId(id.into()),
                self.request(text),
                RunKind::FreshOrResume,
                SendKey::SendId(id.into()),
                false,
                delivery,
                first.then(|| SendPlan {
                    identity: "plan".into(),
                    manifest: Some(self.manifest.clone()),
                }),
            ),
        )
        .await
        .expect("send completes")
        .unwrap()
    }
    async fn start(&self) {
        assert!(matches!(
            self.send("first", "first", Delivery::Queue, true).await,
            SendOutcome::Active { .. }
        ));
        wait(&self.model.entered).await;
        // StepStarted was enqueued before the blocked model call, but the writer may not
        // have appended it yet. Byte-invariance tests must start from a synced prefix.
        let writer = self
            .executor
            .writer
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        writer.sync().await.unwrap();
    }
    fn records(&self) -> Vec<WalRecord> {
        self.wal
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    }
    async fn stop(&self) {
        assert!(
            self.registry
                .stop_and_seal_with_budget(Duration::ZERO, Duration::from_secs(15))
                .await
        );
    }
    async fn idle(&self) {
        broca_test_support::event_timeout(
            "idle",
            async {
                while self.registry.route_status(&self.key).await.unwrap() != LineageStatus::Idle {
                    tokio::task::yield_now().await;
                }
            },
            || format!("slot={:?}", self.registry.slot_state(&self.key)),
        )
        .await
        .expect("run completes");
    }
}
fn registry(
    root: &std::path::Path,
    wal: Arc<FileWalStore>,
    executor: Arc<CoreExecutor>,
) -> LineageRegistry {
    LineageRegistry::new(Arc::new(
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal,
            executor,
        )
        .with_idle_ttl(Duration::from_secs(3600)),
    ))
}
async fn wait_hook(hooks: &HooksRig) {
    // This bound is shorter than the frozen 30-second hook budget. Disabling send
    // cancellation must fail the test before timeout pass-through can release the actor.
    tokio::time::timeout(Duration::from_secs(15), hooks.entered.acquire())
        .await
        .expect("hook entered")
        .unwrap()
        .forget();
}
async fn wait(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(15), notify.notified())
        .await
        .expect("fixture entered stage");
}
fn text(input: &[Message]) -> String {
    input
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}
fn terminal_is_last(records: &[WalRecord]) {
    assert!(matches!(
        records.last(),
        Some(WalRecord::RunFinished { .. })
    ));
}

#[tokio::test]
async fn steer_transforms_each_user_once_and_keyed_retry_reads_original_digest() {
    let f = Fixture::new("pre_user", &["pass"], 0);
    f.start().await;
    let first_records = f.records();
    let WalRecord::RunStarted {
        input,
        hooks: Some(first_hooks),
        ..
    } = &first_records[0]
    else {
        panic!("hooked first run missing")
    };
    assert_eq!(text(input), "first-hooked");
    assert_eq!(first_hooks.original, Some(json!(f.request("first").input)));
    assert_eq!(first_hooks.ops.len(), 1);
    assert_eq!(first_hooks.final_value, None);
    let mut request = f.request("one");
    request.input.push(Message::new(
        Role::User,
        vec![ContentBlock::Text { text: "two".into() }],
    ));
    let send = || {
        f.registry.route_submission_with_delivery(
            &f.key,
            RunId("steer".into()),
            request.clone(),
            RunKind::FreshOrResume,
            SendKey::SendId("steer".into()),
            false,
            Delivery::Steer,
        )
    };
    assert!(matches!(send().await.unwrap(), SendOutcome::Active { .. }));
    let n = f.hooks.count();
    assert_eq!(n, 3); // first send plus two distinct user messages
    assert!(matches!(send().await.unwrap(), SendOutcome::Active { .. }));
    assert_eq!(f.hooks.count(), n);
    let records = f.records();
    let WalRecord::Steered {
        input,
        hooks: Some(hooks),
        request_identity_digest,
        ..
    } = records.last().unwrap()
    else {
        panic!("steer missing")
    };
    assert_eq!(text(input), "one-hookedtwo-hooked");
    assert_eq!(hooks.original, Some(json!(request.input)));
    assert_eq!(hooks.ops.len(), 2);
    assert_eq!(
        request_identity_digest.as_deref(),
        Some(broca_core::send_identity_digest(&request, Delivery::Steer).as_str())
    );
    let calls = f.hooks.calls.lock().unwrap().clone();
    assert_eq!(calls[0].1.harness, f.key.harness);
    assert_eq!(calls[0].1.lineage_id, None);
    assert!(calls[1].1.lineage_id.is_some());
    f.stop().await;
}

#[tokio::test]
async fn closed_steer_carries_one_transform_into_queue_and_drain() {
    let f = Fixture::new("pre_user", &["pass"], 0);
    f.start().await;
    f.hooks.close_inbox.store(true, Ordering::SeqCst);
    assert!(matches!(
        f.send("fallback", "next", Delivery::Steer, false).await,
        SendOutcome::Pending { .. }
    ));
    assert_eq!(f.hooks.count(), 2);
    assert!(matches!(
        f.send("fallback", "next", Delivery::Steer, false).await,
        SendOutcome::Pending { .. }
    ));
    assert_eq!(f.hooks.count(), 2);
    let records = f.records();
    let WalRecord::Queued {
        hooks: Some(hooks),
        request,
        ..
    } = records.last().unwrap()
    else {
        panic!("queue missing")
    };
    assert_eq!(text(&request.input), "next");
    assert_eq!(hooks.original, None);
    assert_eq!(
        text(&serde_json::from_value::<Vec<Message>>(hooks.final_value.clone().unwrap()).unwrap()),
        "next-hooked"
    );
    f.hooks.close_inbox.store(false, Ordering::SeqCst);
    f.model.permits.add_permits(2);
    f.idle().await;
    let records = f.records();
    let started = records
        .iter()
        .find(|r| matches!(r, WalRecord::RunStarted { run_id, .. } if run_id.0 == "fallback"))
        .unwrap();
    let WalRecord::RunStarted {
        input,
        hooks: Some(hooks),
        ..
    } = started
    else {
        panic!("hooked run missing")
    };
    assert_eq!(text(input), "next-hooked");
    assert_eq!(hooks.original, Some(json!(request.input)));
    assert_eq!(hooks.final_value, None);
    assert_eq!(hooks.ops.len(), 1);
    assert_eq!(f.hooks.count(), 2);
    assert!(matches!(
        f.send("fallback", "next", Delivery::Steer, false).await,
        SendOutcome::Finished { .. }
    ));
    assert_eq!(f.hooks.count(), 2);
    f.stop().await;
}

#[tokio::test]
async fn queued_restart_uses_frozen_final_and_unavailable_without_rehooking() {
    let mut f = Fixture::new("pre_user", &["pass", "pass"], 0);
    f.start().await;
    // Both providers become unavailable for this send; hooks retain passed-through providers.
    f.hooks.unavailable.store(true, Ordering::SeqCst);
    assert!(matches!(
        f.send("queued", "next", Delivery::Queue, false).await,
        SendOutcome::Pending { .. }
    ));
    assert_eq!(f.hooks.count(), 4);
    let records = f.records();
    let pending = broca_core::pending_submissions(&records);
    assert_eq!(
        pending[0].hooks.as_ref().unwrap().unavailable,
        Some(vec!["p0".into(), "p1".into()])
    );
    let mut checkpoint = broca_core::AdmissionCheckpointV1::default();
    for (i, record) in records.iter().enumerate() {
        checkpoint.fold_record(i as u64 + 1, record).unwrap();
    }
    let encoded = broca_core::encode_admission_checkpoint(&checkpoint).unwrap();
    let decoded = broca_core::decode_admission_checkpoint(&encoded).unwrap();
    assert_eq!(decoded.pending_fifo[0].hooks, pending[0].hooks);
    f.stop().await;
    f.registry = registry(&f.root, f.wal.clone(), f.executor.clone());
    f.model.permits.add_permits(1);
    // Opening the actor drains the recorded queue after the prior run's shutdown terminal.
    f.registry.actor_for(&f.key).await.unwrap();
    f.idle().await;
    assert_eq!(f.hooks.count(), 4);
    let records = f.records();
    let WalRecord::RunStarted {
        input,
        hooks: Some(hooks),
        ..
    } = records
        .iter()
        .find(|r| matches!(r, WalRecord::RunStarted { run_id, .. } if run_id.0 == "queued"))
        .unwrap()
    else {
        panic!("drained run missing")
    };
    assert_eq!(text(input), "next");
    assert_eq!(hooks.original, Some(json!(f.request("next").input)));
    assert_eq!(hooks.unavailable, Some(vec!["p0".into(), "p1".into()]));
    assert_eq!(hooks.final_value, None);
    assert!(matches!(
        f.send("queued", "next", Delivery::Queue, false).await,
        SendOutcome::Finished { .. }
    ));
    assert_eq!(f.hooks.count(), 4);
    f.stop().await;
}

#[tokio::test]
async fn unavailable_refuse_sends_leave_wal_bytes_and_running_run_unchanged() {
    let f = Fixture::new("pre_user", &["refuse"], 0);
    f.start().await;
    f.hooks.unavailable.store(true, Ordering::SeqCst);
    let before = std::fs::read(f.wal.wal_path(&f.key)).unwrap();
    for delivery in [Delivery::Steer, Delivery::Queue, Delivery::Interrupt] {
        assert_eq!(
            f.send("refused", "next", delivery, false).await,
            SendOutcome::PreUserUnavailable {
                provider: "p0".into()
            }
        );
        assert_eq!(std::fs::read(f.wal.wal_path(&f.key)).unwrap(), before);
        assert!(matches!(
            f.registry.route_status(&f.key).await.unwrap(),
            LineageStatus::Active { .. }
        ));
    }
    f.stop().await;
}

#[tokio::test]
async fn refused_first_send_is_admitted_then_errors_without_a_model_step() {
    let f = Fixture::new("pre_user", &["refuse"], 0);
    f.hooks.unavailable.store(true, Ordering::SeqCst);
    assert!(matches!(
        f.send("first", "first", Delivery::Queue, true).await,
        SendOutcome::Active { .. }
    ));
    f.idle().await;
    let records = f.records();
    assert_eq!(records.len(), 2);
    assert!(matches!(records[0], WalRecord::RunStarted { .. }));
    assert!(
        matches!(&records[1], WalRecord::RunFinished { reason: broca_protocol::RunFinishReason::Error, error: Some(error), .. } if error.provider_code.as_deref() == Some("pre_user_unavailable") && error.message.contains("p0"))
    );
    assert!(matches!(
        f.send("first", "first", Delivery::Queue, false).await,
        SendOutcome::Finished {
            reason: broca_protocol::RunFinishReason::Error,
            ..
        }
    ));
    assert_eq!(f.hooks.count(), 1);
    f.stop().await;
}

#[tokio::test]
async fn paused_restart_steer_is_transformed_and_retry_is_hookless() {
    let f = Fixture::new("pre_user", &["pass"], 0);
    // Seed a durable restart pause, as a process restart does before opening its actors.
    let mut wal = f.wal.open(&f.key).unwrap();
    wal.append(
        1,
        &WalRecord::RunStarted {
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: Some(Box::new(f.manifest.as_ref().clone())),
            scope: None,
            send_id: Some("seed".into()),
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: RunId("seed".into()),
            session: f.key.clone(),
            config: Box::new(super::tests::min_config()),
            input: f.request("seed").input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        false,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::RunPaused {
            run_id: RunId("seed".into()),
            reason: broca_protocol::PauseReason::Restart,
            error: None,
            ts_ms: None,
        },
        true,
    )
    .unwrap();
    drop(wal);
    assert!(matches!(
        f.send("steer", "next", Delivery::Steer, false).await,
        SendOutcome::Paused { .. }
    ));
    assert_eq!(f.hooks.count(), 1);
    let records = f.records();
    assert!(
        matches!(records.last().unwrap(), WalRecord::Steered { input, hooks: Some(_), .. } if text(input) == "next-hooked")
    );
    assert!(matches!(
        f.send("steer", "next", Delivery::Steer, false).await,
        SendOutcome::Active { .. }
    ));
    assert_eq!(f.hooks.count(), 1);
    f.stop().await;
}

#[tokio::test]
async fn caller_cancelling_mid_pre_user_discards_input_without_interrupting() {
    let f = Fixture::new("pre_user", &["pass"], 0);
    f.start().await;
    // Consume the first call's notify before waiting for the next call.
    wait_hook(&f.hooks).await;
    f.hooks.hold.store(true, Ordering::SeqCst);
    let before = std::fs::read(f.wal.wal_path(&f.key)).unwrap();
    let registry = f.registry.clone();
    let key = f.key.clone();
    let request = f.request("next");
    let task = tokio::spawn(async move {
        registry
            .route_submission_with_delivery(
                &key,
                RunId("cancelled-send".into()),
                request,
                RunKind::FreshOrResume,
                SendKey::SendId("cancelled-send".into()),
                false,
                Delivery::Interrupt,
            )
            .await
    });
    wait_hook(&f.hooks).await;
    task.abort();
    let _ = task.await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(15), f.registry.route_status(&f.key))
            .await
            .expect("cancelled send releases the actor")
            .unwrap(),
        LineageStatus::Active { .. }
    ));
    assert_eq!(std::fs::read(f.wal.wal_path(&f.key)).unwrap(), before);
    f.stop().await;
}

#[tokio::test]
async fn seal_mid_post_assistant_persists_fallback_once_before_run_finished() {
    let f = Fixture::new("post_assistant", &["pass"], 0);
    f.hooks.hold.store(true, Ordering::SeqCst);
    f.start().await;
    f.model.permits.add_permits(1);
    wait_hook(&f.hooks).await;
    f.stop().await;
    let records = f.records();
    terminal_is_last(&records);
    let steps: Vec<_> = records
        .iter()
        .filter(|r| matches!(r, WalRecord::ModelStepFinished { .. }))
        .collect();
    assert_eq!(steps.len(), 1);
    assert!(
        matches!(steps[0], WalRecord::ModelStepFinished { assistant_message, hooks: Some(hooks), .. } if text(&[Message::new(Role::Assistant, assistant_message.content.clone())]) == "answer" && hooks.unavailable == Some(vec!["p0".into()]))
    );
}

async fn seal_post_tool(policies: &[&str], calls: usize) {
    let f = Fixture::new("post_tool", policies, calls);
    f.hooks.hold.store(true, Ordering::SeqCst);
    f.start().await;
    f.model.permits.add_permits(1);
    wait_hook(&f.hooks).await;
    if calls > 1 {
        wait_hook(&f.hooks).await;
    }
    f.stop().await;
    let records = f.records();
    terminal_is_last(&records);
    let results: Vec<_> = records
        .iter()
        .filter_map(|r| match r {
            WalRecord::ToolResult {
                result,
                hooks,
                reason,
                ..
            } => Some((result, hooks, reason)),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), calls);
    for (i, (result, hooks, reason)) in results.iter().enumerate() {
        assert_eq!(result.tool_call_id.0, format!("c{i}"));
        if let Some(index) = policies.iter().position(|policy| *policy == "refuse") {
            assert_eq!(reason.as_deref(), Some("post_tool_unavailable"));
            assert!(serde_json::to_string(result)
                .unwrap()
                .contains(&format!("p{index}")));
            assert!(result.is_error);
        } else {
            assert!(reason.is_none());
            assert_eq!(hooks.as_ref().unwrap().unavailable, Some(vec!["p0".into()]));
        }
    }
    assert_eq!(f.hooks.count(), calls); // later refuse provider is never called
}
#[tokio::test]
async fn seal_mid_post_tool_pass_persists_fallback_once() {
    seal_post_tool(&["pass"], 1).await;
}
#[tokio::test]
async fn seal_mid_post_tool_refuse_persists_fallback_once() {
    seal_post_tool(&["refuse"], 1).await;
}
#[tokio::test]
async fn seal_mid_post_tool_pass_then_refuse_withholds_unanswered_provider() {
    seal_post_tool(&["pass", "refuse"], 1).await;
}
#[tokio::test]
async fn seal_parallel_post_tool_slots_are_source_ordered_and_written_once() {
    seal_post_tool(&["pass"], 2).await;
}

#[tokio::test]
async fn queued_restart_carries_final_original_ops_and_unavailable() {
    let mut f = Fixture::new("pre_user", &["pass", "pass"], 0);
    f.hooks.second_unavailable.store(true, Ordering::SeqCst);
    f.start().await;
    assert!(matches!(
        f.send("queued", "next", Delivery::Queue, false).await,
        SendOutcome::Pending { .. }
    ));
    assert_eq!(f.hooks.count(), 4);
    assert!(matches!(
        f.send("queued", "next", Delivery::Queue, false).await,
        SendOutcome::Pending { .. }
    ));
    assert_eq!(f.hooks.count(), 4);
    let records = f.records();
    let WalRecord::Queued {
        hooks: Some(queued_hooks),
        request,
        ..
    } = records.last().unwrap()
    else {
        panic!("queued hooks missing")
    };
    assert_eq!(text(&request.input), "next");
    assert_eq!(queued_hooks.original, None);
    assert_eq!(queued_hooks.ops.len(), 1);
    let final_input = queued_hooks.final_value.clone().unwrap();
    f.stop().await;
    f.registry = registry(&f.root, f.wal.clone(), f.executor.clone());
    f.model.permits.add_permits(1);
    f.registry.actor_for(&f.key).await.unwrap();
    f.idle().await;
    assert_eq!(f.hooks.count(), 4);
    let records = f.records();
    let WalRecord::RunStarted {
        input,
        hooks: Some(hooks),
        request_identity_digest,
        ..
    } = records
        .iter()
        .find(|r| matches!(r, WalRecord::RunStarted { run_id, .. } if run_id.0 == "queued"))
        .unwrap()
    else {
        panic!("drained run missing")
    };
    assert_eq!(json!(input), final_input);
    assert_eq!(text(input), "next-hooked");
    assert_eq!(hooks.original, Some(json!(request.input)));
    assert_eq!(hooks.ops, queued_hooks.ops);
    assert_eq!(hooks.unavailable, Some(vec!["p1".into()]));
    assert_eq!(hooks.final_value, None);
    assert_eq!(
        request_identity_digest.as_deref(),
        Some(broca_core::send_identity_digest(&f.request("next"), Delivery::Queue).as_str())
    );
    assert!(matches!(
        f.send("queued", "next", Delivery::Queue, false).await,
        SendOutcome::Finished { .. }
    ));
    assert_eq!(f.hooks.count(), 4);
    f.stop().await;
}

#[tokio::test]
async fn all_pass_inputs_have_no_hooks_and_legacy_queue_drain_is_hookless() {
    let f = Fixture::new("pre_user", &["pass"], 0);
    f.hooks.pass.store(true, Ordering::SeqCst);
    f.start().await;
    assert!(matches!(
        f.send("queued", "next", Delivery::Queue, false).await,
        SendOutcome::Pending { .. }
    ));
    let records = f.records();
    assert!(matches!(
        records[0],
        WalRecord::RunStarted { hooks: None, .. }
    ));
    assert!(matches!(
        records.last().unwrap(),
        WalRecord::Queued { hooks: None, .. }
    ));
    // A missing hooks member is also the legacy queue shape; neither may be transformed later.
    f.hooks.pass.store(false, Ordering::SeqCst);
    f.model.permits.add_permits(2);
    f.idle().await;
    assert_eq!(f.hooks.count(), 2);
    let records = f.records();
    assert!(
        matches!(records.iter().find(|r| matches!(r, WalRecord::RunStarted { run_id, .. } if run_id.0 == "queued")).unwrap(), WalRecord::RunStarted { hooks: None, input, .. } if text(input) == "next")
    );
    f.stop().await;
}
