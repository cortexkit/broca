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
        _: &BatchCtx,
        _: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        if self.kill && self.root.join("sibling-sent").exists() {
            std::fs::write(self.root.join("kill-ready"), "ready").unwrap();
        }
        let records = if self.kill {
            Vec::new()
        } else {
            self.keys.lock().unwrap().iter().map(|key| json!({"state":"answered","call_key":key,"elicitation_id":format!("q:{key}"),"answered_choice_id":"approve","approval_digest":"digest"})).collect()
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "inc".into(),
                seq: 1,
            },
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
        _: &str,
        _: &broca_core::tool_plane::DispatchSignals,
    ) -> DispatchResult {
        assert_eq!(bytes, r#"{"n":0.000001}"#);
        let frames = FileWalStore::new(&self.root)
            .read_records(&ctx.bind)
            .unwrap();
        assert!(frames.iter().any(|frame| matches!(&frame.record, WalRecord::ApprovalRecorded { call_key, decision:DeferredDecision::Approved, .. } if call_key == key)));
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

#[tokio::test]
async fn every_approval_record_boundary_resumes_without_second_dispatch_or_model_step() {
    let reference = root("reference");
    let (summary, sent, _, _) = run(&reference, req(), false).await;
    assert_eq!(summary.reason, caller::RunFinishReason::Completed);
    assert_eq!(sent.len(), 2);
    let path = FileWalStore::new(&reference).wal_path(&req().session);
    let log = LogImage::read(&path);
    for cut in 1..=log.record_count() {
        let dir = root("cut");
        let store = FileWalStore::new(&dir);
        let path = store.wal_path(&req().session);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &log.bytes[..log.end_of_records(cut)]).unwrap();
        let before = store.read_records(&req().session).unwrap();
        let prepared: Vec<_> = before
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
            .map(|frame| frame.seq)
            .collect();
        let intents: Vec<_> = before
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::ToolDispatchIntent { tool_call_id, .. } => Some(tool_call_id.0.clone()),
                _ => None,
            })
            .collect();
        let (_, resumed_sent, _, keys) = run(&dir, req(), false).await;
        assert!(
            resumed_sent.iter().all(|id| !intents.contains(id)),
            "cut {cut} must never resend a durable intent"
        );
        let after = store.read_records(&req().session).unwrap();
        for seq in prepared {
            assert_eq!(
                after
                    .iter()
                    .filter(|frame| frame.seq == seq
                        && matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
                    .count(),
                1
            );
        }
        if !keys.is_empty() {
            let lineage = log.lineage_id().unwrap();
            for key in keys {
                assert!(key.starts_with(&format!("{lineage}:")));
            }
        }
        let finished_before = before
            .iter()
            .filter(|frame| {
                matches!(
                    frame.record,
                    WalRecord::ModelStepFinished {
                        step_id: StepId(1),
                        ..
                    }
                )
            })
            .count();
        if finished_before > 0 {
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
                1,
                "Prepared resume never regenerates its model step"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::remove_dir_all(reference).unwrap();
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
