//! Resume after truncating the file WAL at every approval record boundary to prove
//! that durable intents are never dispatched twice and Prepared calls never rerun
//! their model step. A forced process kill also proves that an unanswered call is
//! closed with a withdrawal and an unfinished dispatched call is not resent.
use async_trait::async_trait;
use broca_core::{
    hooks::{HookAnswer, HookCall},
    tool_plane::*,
    *,
};
use broca_crash_harness::wal::LogImage;
use broca_crash_harness::{
    scenario::{HarnessPlane, ScenarioExtension},
    KillReport, Render, Resumed, RunnerHandle, Scenario,
};
use broca_protocol::*;
use broca_wal::{DeferredDecision, WalRecord};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::mpsc;

fn tool(name: &str) -> request::Tool {
    serde_json::from_value(json!({"name":name,"description":"work","input_schema":{"type":"object"},"module":"tools","execution_mode":"parallel"})).unwrap()
}
fn call(id: &str, name: &str) -> ToolCall {
    ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: name.into(),
        input: json!({"n":1e-6}),
        thought_signature: None,
    }
}
fn scope() -> SessionScope {
    serde_json::from_value(
        json!({"owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1}),
    )
    .unwrap()
}
fn req() -> RunRequest {
    let submission = serde_json::from_value(json!({"model":{"provider_module_id":"stub","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"go"}]}],"tools":[tool("edit"),tool("other")],"policy_allowed":["edit","other"],"stop_when":[{"type":"step_count","n":3}]})).unwrap();
    let mut req = RunRequest::from_submission(
        submission,
        RunId("approval".into()),
        BindIdentity::new("/project", "test", "approval"),
        SubmissionId("send".into()),
    );
    req.submission_id = None;
    req.scope = Some(scope());
    req.on_restart = Some(request::OnRestart::Resume);
    req
}
fn plan() -> broca_wal::PlanManifest {
    let sub = json!({"hook":"pre_tool","phase":"approve","ops":[],"tools":["edit"],"on_unavailable":"refuse","budget_ms":500});
    let value = json!({"step_transform_items":[{"provider":"approve","preset":"head","params":{},"subscriptions":[sub.clone()]}]});
    broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap(),
        plan_identity: "p".into(),
        composition_digest: "c".into(),
        tools: vec![tool("edit"), tool("other")].into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: Vec::new(),
        declarations: vec![broca_wal::FrozenDeclaration {
            provider: "approve".into(),
            answer: broca_wal::OpaqueJson::from_json_text(
                json!({"subscriptions":[sub]}).to_string(),
            )
            .unwrap(),
        }],
    }
}
struct Model {
    steps: AtomicUsize,
    sibling: bool,
}
#[async_trait]
impl Provider for Model {
    async fn step(
        &self,
        step: StepId,
        _: &request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.steps.fetch_add(1, Ordering::SeqCst);
        let content = if step.0 == 1 {
            let mut calls = vec![ContentBlock::ToolCall(call("a", "edit"))];
            if self.sibling {
                calls.push(ContentBlock::ToolCall(call("b", "other")));
            } else {
                calls.push(ContentBlock::ToolCall(call("b", "edit")));
            }
            calls
        } else {
            vec![ContentBlock::Text {
                text: "done".into(),
            }]
        };
        let (tx, rx) = mpsc::channel(4);
        tx.send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId(format!("m{}", step.0)),
                    content,
                },
            },
        ))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: if step.0 == 1 {
                content::FinishReason::ToolCalls
            } else {
                content::FinishReason::Stop
            },
            retries_used: 0,
        }))
        .await
        .unwrap();
        rx
    }
}
struct Hook;
#[async_trait]
impl HookPlane for Hook {
    async fn hook(
        &self,
        _: &str,
        _: &BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        call: HookCall,
    ) -> Result<HookAnswer, HookUnavailable> {
        if serde_json::to_value(&call.subject).unwrap()["phase"] == "approve" {
            Ok(serde_json::from_value(json!({"answer":"ask","ask":{"prompt":"Allow?","expires_at_ms":broca_core::run::epoch_millis().unwrap()+60_000,"on_expiry":"deny","material_damage":false,"late_execution":"execute"}})).unwrap())
        } else {
            Ok(HookAnswer::Pass)
        }
    }
}
struct Questions {
    stop: Option<tokio_util::sync::CancellationToken>,
    keys: Mutex<Vec<String>>,
    root: PathBuf,
    kill: bool,
}
impl Questions {
    fn question(&self, ctx: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        let not_found = || ApprovalRouteError {
            code: "consent_not_found".into(),
            retryable: false,
        };
        let key = id.strip_prefix("q:").ok_or_else(not_found)?;
        let seq = key
            .rsplit_once(':')
            .and_then(|(_, seq)| seq.parse::<u64>().ok())
            .ok_or_else(not_found)?;
        // Each truncated log is copied without daemon storage. Rebuild the card's
        // argument binding from ToolCallDeferred so a read after restart returns
        // the original filing even before ApprovalRecorded exists.
        let frames = FileWalStore::new(&self.root)
            .read_records(&ctx.bind)
            .unwrap();
        let prepared = frames.iter().find(|frame| frame.seq == seq);
        let Some(WalRecord::ToolCallDeferred {
            args_hash,
            module,
            tool,
            schema_pin,
            ..
        }) = prepared.map(|frame| &frame.record)
        else {
            return Err(not_found());
        };
        let mut record = json!({
            "kind":"consent",
            "state":"pending",
            "elicitation_id":id,
            "requester_ref":key,
            "binding":{"args_hash":args_hash,"module":module,"tool":tool,"schema_pin":schema_pin},
            "default":"deny",
            "on_expiry":"deny",
            "late_execution":"execute",
            "options":[{"id":"approve","effect":"grant","label":"Allow once"},{"id":"deny","effect":"decline","label":"Not now"}]
        });
        if !self.kill {
            record["state"] = json!("answered");
            record["answered_choice_id"] = json!("approve");
            record["approval_digest"] = json!("digest");
        }
        Ok(ApprovalQuestion {
            terminal: !self.kill,
            record,
        })
    }
}
#[async_trait]
impl ElicitationPlane for Questions {
    async fn request(
        &self,
        _: &BatchCtx,
        key: &str,
        record: &WalRecord,
        _: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        if let Some(stop) = &self.stop {
            stop.cancel();
        }
        self.keys.lock().unwrap().push(key.into());
        let WalRecord::ToolCallDeferred { args_hash, .. } = record else {
            unreachable!()
        };
        Ok(ApprovalFiling {
            elicitation_id: format!("q:{key}"),
            args_hash: args_hash.clone(),
        })
    }
    async fn answers(
        &self,
        ctx: &BatchCtx,
        since: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        if self.kill && self.root.join("sibling-sent").exists() {
            std::fs::write(self.root.join("kill-ready"), "ready").unwrap();
        }
        let keys = self.keys.lock().unwrap().clone();
        let records = if self.kill {
            Vec::new()
        } else {
            keys.iter()
                .skip(since.map_or(0, |cursor| cursor.seq as usize))
                .map(|key| self.question(ctx, &format!("q:{key}")).map(|q| q.record))
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "inc".into(),
                seq: keys.len() as u64,
            },
        })
    }
    async fn get(&self, ctx: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.question(ctx, id)
    }
    async fn r#await(
        &self,
        ctx: &BatchCtx,
        id: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        if self.kill {
            // Wait for the other tool call to enter dispatch before publishing
            // kill-ready. Keep this unanswered card waiting so the process kill
            // cuts both an open approval wait and an in-flight tool call.
            let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
            loop {
                poll.tick().await;
                self.answers(ctx, None).await?;
                if self.root.join("kill-ready").exists() {
                    return std::future::pending().await;
                }
            }
        }
        self.question(ctx, id)
    }
    async fn withdraw(&self, _: &BatchCtx, _: &str) -> ApprovalWithdrawal {
        // Return a retryable refusal so the real-kill test can inspect the
        // pending withdrawal after the interrupted call is closed.
        ApprovalWithdrawal::Retry(ApprovalRouteError {
            code: "transport_busy".into(),
            retryable: true,
        })
    }
}
struct Tools {
    sent: Mutex<Vec<String>>,
    root: PathBuf,
    kill: bool,
}
#[async_trait]
impl ToolPlane for Tools {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn recheck_approval_scope(
        &self,
        ctx: &BatchCtx,
        _: &ToolCall,
        _: Option<&str>,
    ) -> Result<(), ToolPlaneError> {
        assert_eq!(ctx.scope.as_ref(), Some(&scope()));
        Ok(())
    }
    async fn dispatch(&self, _: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.sent.lock().unwrap().push(call.tool_call_id.0.clone());
        if self.kill {
            let file = std::fs::File::create(self.root.join("sibling-sent")).unwrap();
            file.sync_all().unwrap();
            std::future::pending::<()>().await;
        }
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text { text: "ok".into() },
            is_error: false,
        })
    }
    async fn dispatch_approved(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        bytes: &str,
        key: &str,
        _: &broca_core::tool_plane::DispatchSignals,
    ) -> DispatchResult {
        assert_eq!(bytes, r#"{"n":0.000001}"#);
        let frames = FileWalStore::new(&self.root)
            .read_records(&ctx.bind)
            .unwrap();
        assert!(frames.iter().any(|frame| matches!(&frame.record,
            WalRecord::ApprovalRecorded { call_key, decision:DeferredDecision::Approved, .. }
            | WalRecord::LateApprovalRecorded { call_key, decision:broca_wal::LateDecision::Approved, .. }
            if call_key == key)));
        self.dispatch(ctx, call).await
    }
}
fn root(label: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    broca_test_support::scratch_root().join(format!(
        "approval-harness-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ))
}
async fn run(
    root: &Path,
    request: RunRequest,
    kill: bool,
) -> (RunSummary, Vec<String>, usize, Vec<String>) {
    let model = Model {
        steps: AtomicUsize::new(0),
        sibling: kill,
    };
    let tools = Tools {
        sent: Mutex::new(Vec::new()),
        root: root.into(),
        kill,
    };
    let questions = Arc::new(Questions {
        stop: None,
        keys: Mutex::new(Vec::new()),
        root: root.into(),
        kill,
    });
    let control = RunControl {
        manifest: Some(Arc::new(plan())),
        hook_plane: Some(Arc::new(Hook)),
        elicitation_plane: Some(questions.clone()),
        ..Default::default()
    };
    let (out, _rx) = mpsc::channel(1000);
    let summary = run_loop_with_control(
        request,
        &model,
        &tools,
        &FileLeaseStore::new(root.join("leases")),
        &AllowAll,
        &FileWalStore::new(root),
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    let sent = tools.sent.lock().unwrap().clone();
    let keys = questions.keys.lock().unwrap().clone();
    (summary, sent, model.steps.load(Ordering::SeqCst), keys)
}

struct ApprovalScenario;

impl ScenarioExtension for ApprovalScenario {
    fn setup(&self, root: &Path, control: &mut RunControl, plane: &mut HarnessPlane) {
        let root = root.join("wal");
        control.manifest = Some(Arc::new(plan()));
        control.hook_plane = Some(Arc::new(Hook));
        control.elicitation_plane = Some(Arc::new(Questions {
            stop: None,
            keys: Mutex::new(Vec::new()),
            root: root.clone(),
            kill: false,
        }));
        plane.set_inner(Arc::new(Tools {
            sent: Mutex::new(Vec::new()),
            root,
            kill: false,
        }));
    }

    fn check(&self, kill: &KillReport, resumed: &Resumed) {
        let before = kill.durable.framed();
        let after = resumed.log.framed();
        for frame in &before {
            if matches!(frame.record, WalRecord::ToolCallDeferred { .. }) {
                assert_eq!(after.iter().filter(|next| *next == frame).count(), 1);
            }
            if let WalRecord::ToolDispatchIntent { tool_call_id, .. } = &frame.record {
                assert_eq!(resumed.plane.dispatch_count(tool_call_id), 0);
            }
        }
        if before.iter().any(|frame| {
            matches!(
                frame.record,
                WalRecord::ModelStepFinished {
                    step_id: StepId(1),
                    ..
                }
            )
        }) {
            assert!(
                !resumed.provider.sent().contains_key(&0),
                "step 1 must not be regenerated"
            );
            assert_eq!(
                after
                    .iter()
                    .filter(|frame| matches!(
                        frame.record,
                        WalRecord::ModelStepFinished {
                            step_id: StepId(1),
                            ..
                        }
                    ))
                    .count(),
                1
            );
        }
        let lineage = resumed.log.lineage_id().unwrap();
        for frame in &after {
            if let WalRecord::ApprovalRecorded { call_key, .. } = &frame.record {
                assert!(call_key.starts_with(&format!("{lineage}:")));
            }
        }
    }
}

fn approval_scripts() -> Vec<Vec<NormalizedEvent>> {
    [
        vec![
            ContentBlock::ToolCall(call("a", "edit")),
            ContentBlock::ToolCall(call("b", "edit")),
        ],
        vec![ContentBlock::Text {
            text: "done".into(),
        }],
    ]
    .into_iter()
    .enumerate()
    .map(|(index, content)| {
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId(format!("m{}", index + 1)),
                    content,
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: if index == 0 {
                    content::FinishReason::ToolCalls
                } else {
                    content::FinishReason::Stop
                },
                retries_used: 0,
            }),
        ]
    })
    .collect()
}

const APPROVAL: Scenario = Scenario {
    name: "approval_deferred",
    request: req,
    control: RunControl::default,
    scripts: approval_scripts,
    render: Render::Canonical,
    plane: StubToolPlane::new,
    expected: caller::RunFinishReason::Completed,
    extension: Some(&ApprovalScenario),
};

struct ExpiringHook;

#[async_trait]
impl HookPlane for ExpiringHook {
    async fn hook(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&SessionScope>,
        call: HookCall,
    ) -> Result<HookAnswer, HookUnavailable> {
        if serde_json::to_value(&call.subject).unwrap()["phase"] == "approve" {
            Ok(serde_json::from_value(json!({"answer":"ask","ask":{
                "prompt":"Allow?","expires_at_ms":2000,"on_expiry":"deny",
                "material_damage":false,"late_execution":"execute"
            }}))
            .unwrap())
        } else {
            Ok(HookAnswer::Pass)
        }
    }
}

struct LateQuestions {
    inner: Questions,
    root: PathBuf,
    clock: Clock,
}

impl LateQuestions {
    fn expire_filed(&self, ctx: &BatchCtx) {
        let filed = FileWalStore::new(&self.inner.root).read_records(&ctx.bind).unwrap().iter().any(|frame|
            matches!(&frame.record, WalRecord::AwaitingApproval { pending, .. } if pending.iter().any(|entry| entry.elicitation_id.is_some())));
        if filed && !self.late(ctx) {
            std::fs::write(self.root.join("test-clock"), r#"{"now_ms":3000}"#).unwrap();
            self.clock.refresh().unwrap();
        }
    }
    fn late(&self, ctx: &BatchCtx) -> bool {
        FileWalStore::new(&self.inner.root)
            .read_records(&ctx.bind)
            .unwrap()
            .iter()
            .any(|frame| {
                matches!(
                    frame.record,
                    WalRecord::RunPaused {
                        reason: PauseReason::AuthRequired,
                        ..
                    }
                )
            })
    }

    fn question(&self, ctx: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        let mut question = self.inner.question(ctx, id)?;
        if !self.late(ctx) {
            question.terminal = false;
            question.record["state"] = json!("pending");
            let object = question.record.as_object_mut().unwrap();
            object.remove("answered_choice_id");
            object.remove("approval_digest");
        }
        Ok(question)
    }
}

#[async_trait]
impl ElicitationPlane for LateQuestions {
    async fn request(
        &self,
        ctx: &BatchCtx,
        key: &str,
        record: &WalRecord,
        now: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        self.inner.request(ctx, key, record, now).await
    }
    async fn answers(
        &self,
        ctx: &BatchCtx,
        since: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        self.expire_filed(ctx);
        let keys = self.inner.keys.lock().unwrap().clone();
        let seq = keys.len() as u64 + u64::from(self.late(ctx));
        let records = if since.is_some_and(|cursor| cursor.seq >= seq) {
            Vec::new()
        } else {
            keys.iter()
                .map(|key| {
                    self.question(ctx, &format!("q:{key}"))
                        .map(|question| question.record)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "late-incarnation".into(),
                seq,
            },
        })
    }
    async fn get(&self, ctx: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.question(ctx, id)
    }
    async fn r#await(
        &self,
        ctx: &BatchCtx,
        id: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        if !self.late(ctx) {
            std::fs::write(self.root.join("test-clock"), r#"{"now_ms":3000}"#).unwrap();
            self.clock.refresh().unwrap();
        }
        self.question(ctx, id)
    }
    async fn withdraw(&self, ctx: &BatchCtx, id: &str) -> ApprovalWithdrawal {
        self.inner.withdraw(ctx, id).await
    }
}

struct LateScenario;

impl ScenarioExtension for LateScenario {
    fn setup(&self, root: &Path, control: &mut RunControl, plane: &mut HarnessPlane) {
        let wal = root.join("wal");
        let frames = match FileWalStore::new(&wal).read_records(&req().session) {
            Ok(frames) => frames,
            Err(broca_wal::WalError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Vec::new()
            }
            Err(error) => panic!("read late approval scenario: {error}"),
        };
        let expired = frames.iter().any(|frame| {
            matches!(
                frame.record,
                WalRecord::ApprovalRecorded {
                    decision: DeferredDecision::Expired,
                    ..
                }
            )
        });
        std::fs::write(
            root.join("test-clock"),
            json!({"now_ms":if expired { 3000 } else { 1000 }}).to_string(),
        )
        .unwrap();
        let clock = Clock::for_state_root(root).unwrap();
        control.clock = Some(clock.clone());
        control.manifest = Some(Arc::new(plan()));
        control.hook_plane = Some(Arc::new(ExpiringHook));
        control.elicitation_plane = Some(Arc::new(LateQuestions {
            inner: Questions {
                stop: None,
                keys: Mutex::new(Vec::new()),
                root: wal.clone(),
                kill: false,
            },
            root: root.into(),
            clock,
        }));
        plane.set_inner(Arc::new(Tools {
            sent: Mutex::new(Vec::new()),
            root: wal,
            kill: false,
        }));
    }

    fn check(&self, kill: &KillReport, resumed: &Resumed) {
        ApprovalScenario.check(kill, resumed);
        let before = kill.durable.records();
        if before
            .iter()
            .any(|record| matches!(record, WalRecord::LateDispatchIntent { .. }))
        {
            assert_eq!(
                resumed.plane.dispatch_count(&ToolCallId("a".into())),
                0,
                "{}: late intent must not be resent",
                kill.label()
            );
        }
        let after = resumed.log.records();
        assert!(
            after
                .iter()
                .filter(|record| matches!(record, WalRecord::LateDispatchIntent { .. }))
                .count()
                <= 1
        );
        if after
            .iter()
            .any(|record| matches!(record, WalRecord::LateDispatchIntent { .. }))
        {
            assert_eq!(
                after
                    .iter()
                    .filter(|record| matches!(record, WalRecord::LateResult { .. }))
                    .count(),
                1
            );
        }
    }
}

fn late_scripts() -> Vec<Vec<NormalizedEvent>> {
    let mut scripts = approval_scripts();
    if let NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) =
        &mut scripts[0][0]
    {
        message.content.truncate(1);
    }
    scripts
}

const LATE_APPROVAL: Scenario = Scenario {
    name: "late_approval",
    scripts: late_scripts,
    render: Render::DeferredAuthResume,
    extension: Some(&LateScenario),
    ..APPROVAL
};

struct UnavailableQuestions;

fn missing_card() -> ApprovalRouteError {
    ApprovalRouteError {
        code: "elicitation_not_found".into(),
        retryable: false,
    }
}

#[async_trait]
impl ElicitationPlane for UnavailableQuestions {
    async fn request(
        &self,
        _: &BatchCtx,
        _: &str,
        _: &WalRecord,
        _: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        Err(missing_card())
    }
    async fn answers(
        &self,
        _: &BatchCtx,
        _: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        Ok(ApprovalPage {
            records: Vec::new(),
            cursor: ApprovalCursor {
                provider_incarnation: "missing".into(),
                seq: 0,
            },
        })
    }
    async fn get(&self, _: &BatchCtx, _: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        Err(missing_card())
    }
    async fn r#await(
        &self,
        _: &BatchCtx,
        _: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        Err(missing_card())
    }
}

struct WithdrawnCard;

#[async_trait]
impl ScenarioExtension for WithdrawnCard {
    fn setup(&self, root: &Path, control: &mut RunControl, plane: &mut HarnessPlane) {
        ApprovalScenario.setup(root, control, plane);
        control.elicitation_plane = Some(Arc::new(UnavailableQuestions));
    }

    fn check(&self, kill: &KillReport, resumed: &Resumed) {
        ApprovalScenario.check(kill, resumed);
    }

    async fn complete_reference(
        &self,
        _: &Path,
        store: &broca_crash_harness::wal::ObservedWalStore,
        _: &broca_crash_harness::ScriptedProvider,
    ) {
        let request = req();
        let frames = store.read_records(&request.session).unwrap();
        let wal = Arc::new(Mutex::new(store.open(&request.session).unwrap()));
        let lineage = wal.lock().unwrap().lineage_id().unwrap();
        let fence = frames.last().unwrap().fence;
        for frame in &frames {
            if let WalRecord::ToolCallDeferred {
                batch_id, scope, ..
            } = &frame.record
            {
                let ctx = BatchCtx {
                    bind: request.session.clone(),
                    run_id: request.run_id.clone(),
                    batch_id: batch_id.clone(),
                    scope: Some(scope.clone()),
                    media_capture: None,
                };
                let key = format!("{lineage}:{}", frame.seq);
                broca_core::consent::close_out(
                    &UnavailableQuestions,
                    &ctx,
                    &key,
                    &frame.record,
                    None,
                    broca_core::run::epoch_millis().unwrap(),
                    |record| {
                        let result = wal
                            .lock()
                            .unwrap()
                            .append(fence, &record, true)
                            .map(|_| ())
                            .map_err(|_| ApprovalRouteError {
                                code: "wal_error".into(),
                                retryable: true,
                            });
                        std::future::ready(result)
                    },
                )
                .await
                .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn withdrawal_acknowledgement_holds_every_invariant_at_every_boundary() {
    let handle = RunnerHandle::new(Scenario {
        name: "withdrawn_card",
        extension: Some(&WithdrawnCard),
        ..APPROVAL
    })
    .await;
    assert_eq!(
        handle
            .reference
            .log
            .records()
            .iter()
            .filter(|record| matches!(record, WalRecord::QuestionWithdrawalAcknowledged { .. }))
            .count(),
        2
    );
    let kills = handle.every_record_boundary(true).await;
    assert_eq!(kills.len(), handle.record_boundaries(true).len());
}

#[tokio::test]
async fn late_approval_intent_and_result_hold_every_invariant_at_every_boundary() {
    let handle = RunnerHandle::new(LATE_APPROVAL).await;
    for kind in [
        "tool_call_deferred",
        "approval_recorded",
        "late_approval_recorded",
        "late_dispatch_intent",
        "late_result",
    ] {
        assert!(
            handle
                .reference
                .log
                .records()
                .iter()
                .any(|record| serde_json::to_value(record).unwrap()["type"] == kind),
            "missing {kind}"
        );
    }
    let kills = handle.every_record_boundary(true).await;
    assert_eq!(kills.len(), handle.record_boundaries(true).len());
}

#[tokio::test]
async fn every_approval_record_boundary_resumes_without_second_dispatch_or_model_step() {
    let handle = RunnerHandle::new(APPROVAL).await;
    for kind in [
        "tool_call_deferred",
        "awaiting_approval",
        "approval_recorded",
    ] {
        assert!(
            handle
                .reference
                .log
                .records()
                .iter()
                .any(|record| serde_json::to_value(record).unwrap()["type"] == kind),
            "missing {kind}"
        );
    }
    let kills = handle.every_record_boundary(true).await;
    assert_eq!(kills.len(), handle.record_boundaries(true).len());
}

#[tokio::test]
async fn approval_wait_kill_child() {
    let Some(root) = std::env::var_os("BROCA_APPROVAL_KILL_ROOT") else {
        return;
    };
    let mut request = req();
    request.on_restart = None;
    run(Path::new(&root), request, true).await;
}
#[tokio::test]
async fn real_kill_during_wait_closes_deferred_and_dispatched_sibling_without_resend() {
    let dir = root("kill");
    std::fs::create_dir_all(&dir).unwrap();
    broca_crash_harness::kill_child_test_at_marker(
        "approval wait with sibling",
        "approval_wait_kill_child",
        &[("BROCA_APPROVAL_KILL_ROOT", dir.as_os_str())],
        &dir.join("kill-ready"),
    );
    let mut request = req();
    request.on_restart = None;
    let (summary, sent, steps, _) = run(&dir, request, false).await;
    assert_eq!(summary.reason, caller::RunFinishReason::Interrupted);
    assert!(sent.is_empty());
    assert_eq!(steps, 0);
    let store = FileWalStore::new(&dir);
    let frames = store.read_records(&req().session).unwrap();
    assert!(frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolResult {
            withdraw_elicitation: Some(_),
            ..
        }
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolResult {
            outcome: broca_wal::DispatchOutcome::Unknown,
            ..
        }
    )));
    let lineage = LogImage::read(&store.wal_path(&req().session))
        .lineage_id()
        .unwrap();
    assert_eq!(
        broca_core::deferred_call_obligations(lineage, &frames)
            .withdrawals
            .len(),
        1
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn resumed_wait_keeps_the_wal_frozen_restart_policy_when_request_changes() {
    let dir = root("restart-policy");
    let store = FileWalStore::new(&dir);
    let leases = FileLeaseStore::new(dir.join("leases"));
    let mut request = req();
    for pass in 0..2 {
        let stop = tokio_util::sync::CancellationToken::new();
        let questions = Arc::new(Questions {
            stop: Some(stop.clone()),
            keys: Mutex::new(Vec::new()),
            root: dir.clone(),
            kill: false,
        });
        let control = RunControl {
            manifest: Some(Arc::new(plan())),
            hook_plane: Some(Arc::new(Hook)),
            elicitation_plane: Some(questions),
            stop_requested: Some(stop),
            ..Default::default()
        };
        let tools = Tools {
            sent: Mutex::new(Vec::new()),
            root: dir.clone(),
            kill: false,
        };
        let model = Model {
            steps: AtomicUsize::new(0),
            sibling: false,
        };
        let (out, _rx) = mpsc::channel(1000);
        if pass == 1 {
            request.on_restart = None;
        }
        let summary = run_loop_with_control(
            request.clone(),
            &model,
            &tools,
            &leases,
            &AllowAll,
            &store,
            None,
            control,
            &out,
        )
        .await
        .unwrap();
        assert_eq!(
            summary.paused,
            Some(caller::PauseReason::Restart),
            "the continuing episode owns restart policy, not the resuming send"
        );
        if pass == 1 {
            assert_eq!(model.steps.load(Ordering::SeqCst), 0);
        }
        let frames = store.read_records(&request.session).unwrap();
        assert!(!frames.iter().any(|frame| matches!(
            frame.record,
            WalRecord::ToolResult { .. } | WalRecord::ToolBatchFinished { .. }
        )));
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(
                    frame.record,
                    WalRecord::ModelStepFinished {
                        step_id: StepId(1),
                        ..
                    }
                ))
                .count(),
            1
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}

broca_crash_harness::require_process_kill!();
