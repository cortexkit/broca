use super::hook_tests::{call, frozen, sub, tool};
use super::*;
use crate::hooks::{HookPlan, HookRuntime};
use crate::tool_plane::*;
use cortexkit_role_step_transform_provider::{
    answer::HookAnswer,
    hook::{HookCall, Subject},
    subscription::Phase,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

struct Hooks {
    validates: AtomicUsize,
    second: &'static str,
    expires: u64,
    seen: Mutex<Vec<Value>>,
}
#[async_trait::async_trait]
impl crate::HookPlane for Hooks {
    async fn hook(
        &self,
        _: &str,
        _: &BindIdentity,
        hook: HookCall,
    ) -> Result<HookAnswer, crate::HookUnavailable> {
        if let Subject::PreTool { input, .. } = &hook.subject {
            self.seen.lock().unwrap().push(input.clone());
        }
        if let Subject::PreTool {
            phase: Phase::Validate,
            step_id,
            ..
        } = &hook.subject
        {
            assert_eq!(
                step_id, "1",
                "second validation retains the originating step identity"
            );
        }
        match hook.subject.phase() {
            Some(Phase::Mutate) if self.second=="mutate_denied" => Ok(HookAnswer::Mutate {input:json!({"f":"denied"}),note:None}),
            Some(Phase::Mutate) if self.second=="mutate_allowed" => Ok(HookAnswer::Mutate {input:json!({"f":"mutated","n":1e-6,"keep":true}),note:None}),
            Some(Phase::Approve) => Ok(serde_json::from_value(json!({"answer":"ask", "ask":{
                "prompt":"Allow?", "expires_at_ms":self.expires, "on_expiry":"deny", "material_damage":false, "late_execution":if self.second=="notify" { "notify_only" } else { "execute" }
            }})).unwrap()),
            Some(Phase::Validate) => {
                let n = self.validates.fetch_add(1, Ordering::SeqCst);
                match (n, self.second) {
                    (0, "first_deny") | (1.., "deny") => Ok(HookAnswer::Deny { text: "not allowed".into() }),
                    (1.., "unavailable") => Err(crate::HookUnavailable),
                    (1.., "hold") => std::future::pending().await,
                    _ => Ok(HookAnswer::Pass),
                }
            }
            _ => Ok(HookAnswer::Pass),
        }
    }
}
struct Questions {
    store: crate::FileWalStore,
    completed: Arc<std::sync::atomic::AtomicBool>,
    lineage: broca_wal::WalLineageId,
    head: Arc<std::sync::atomic::AtomicU64>,
    keys: Mutex<Vec<String>>,
    attempts: AtomicUsize,
    polls: AtomicUsize,
    transient: bool,
    cursors: Mutex<Vec<Option<ApprovalCursor>>>,
    answer: Mutex<&'static str>,
    stop: Option<CancellationToken>,
}
#[async_trait::async_trait]
impl ElicitationPlane for Questions {
    async fn request(
        &self,
        ctx: &BatchCtx,
        key: &str,
        record: &WalRecord,
        _: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        let seq: u64 = key.rsplit_once(':').unwrap().1.parse().unwrap();
        assert_eq!(
            key,
            format!("{}:{seq}", self.lineage),
            "the filing must reuse the Prepared anchor"
        );
        assert!(
            self.head.load(Ordering::SeqCst) >= seq,
            "Prepared synced before filing"
        );
        assert!(
            self.store
                .read_records(&ctx.bind)
                .unwrap()
                .iter()
                .any(|frame| frame.seq == seq && frame.record == *record),
            "Prepared frame must be durable before filing"
        );
        self.keys.lock().unwrap().push(key.into());
        if self.transient && self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ApprovalRouteError {
                code: "module_reloading".into(),
                retryable: true,
            });
        }
        if let Some(stop) = &self.stop {
            stop.cancel();
        }
        let WalRecord::ToolCallDeferred { args_hash, .. } = record else {
            panic!("filing must use Prepared")
        };
        Ok(ApprovalFiling {
            elicitation_id: format!("q:{key}"),
            args_hash: args_hash.clone(),
        })
    }
    async fn get(&self, _: &BatchCtx, _: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        let terminal = *self.answer.lock().unwrap() == "terminal";
        Ok(ApprovalQuestion {
            terminal,
            record: json!({"state":if terminal { "expired" } else { "pending" }}),
        })
    }
    async fn answers(
        &self,
        ctx: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        self.cursors.lock().unwrap().push(cursor.cloned());
        let answer = *self.answer.lock().unwrap();
        if answer == "none" && self.completed.load(Ordering::SeqCst) {
            let frames = self.store.read_records(&ctx.bind).unwrap();
            let terminal=frames.iter().find(|frame| matches!(&frame.record,WalRecord::ToolResult{result,..} if result.tool_call_id.0=="sibling"));
            assert!(
                terminal.is_some_and(|frame| self.head.load(Ordering::SeqCst) >= frame.seq),
                "each completed sibling must be synced before answer polling continues"
            );
        }
        let n = self.polls.fetch_add(1, Ordering::SeqCst);
        if answer == "overdue" {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if answer == "reset" && n == 1 {
            assert!(cursor.is_some());
            return Err(ApprovalRouteError {
                code: "cursor_incarnation_changed".into(),
                retryable: false,
            });
        }
        if answer == "reset" && n == 2 {
            assert!(cursor.is_none(), "old incarnation must be re-read fresh");
        }
        let unreadable = answer.starts_with("unreadable_");
        if unreadable && n > 0 {
            assert!(
                cursor.is_none(),
                "unreadable answer must be read again from the same position"
            );
        }
        let mut records = if matches!(answer, "none" | "terminal") || (answer == "reset" && n == 0)
        {
            Vec::new()
        } else {
            self.keys.lock().unwrap().iter().map(|key| json!({"state":"answered", "call_key":key, "elicitation_id":format!("q:{key}"),
                "answered_choice_id": if answer == "decline" { "deny" } else { "approve" }, "approval_digest":"digest"})).collect()
        };
        if unreadable && n == 0 {
            match answer {
                "unreadable_choice_missing" => {
                    records[0]
                        .as_object_mut()
                        .unwrap()
                        .remove("answered_choice_id");
                }
                "unreadable_choice_type" => records[0]["answered_choice_id"] = json!(7),
                "unreadable_id_missing" => {
                    records[0].as_object_mut().unwrap().remove("elicitation_id");
                }
                "unreadable_id_type" => records[0]["elicitation_id"] = json!(7),
                "unreadable_digest_missing" => {
                    records[0]
                        .as_object_mut()
                        .unwrap()
                        .remove("approval_digest");
                }
                "unreadable_digest_type" => records[0]["approval_digest"] = json!(7),
                _ => unreachable!(),
            }
        }
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: if unreadable {
                    "stable".into()
                } else {
                    format!("inc-{}", n / 2)
                },
                seq: if unreadable { 1 } else { n as u64 + 1 },
            },
        })
    }
}
struct Tools {
    interrupt_on_recheck: Mutex<Option<CancellationToken>>,
    default_module: Mutex<String>,
    routes: Mutex<std::collections::HashMap<(BatchId, ToolCallId), String>>,
    hold: Mutex<Option<Arc<tokio::sync::Notify>>>,
    started: std::sync::atomic::AtomicBool,
    completed: Arc<std::sync::atomic::AtomicBool>,
    outcome: Mutex<&'static str>,
    refuse_scope: std::sync::atomic::AtomicBool,
    sent: Mutex<Vec<(String, String, BatchId)>>,
    store: crate::FileWalStore,
    bind: BindIdentity,
}
#[async_trait::async_trait]
impl ToolPlane for Tools {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        for call in calls {
            let module = manifest
                .get(&call.tool_name)
                .and_then(|tool| tool.module.clone())
                .unwrap_or_else(|| self.default_module.lock().unwrap().clone());
            self.routes
                .lock()
                .unwrap()
                .insert((ctx.batch_id.clone(), call.tool_call_id.clone()), module);
        }
        Ok(())
    }
    fn dispatch_target(&self, ctx: &BatchCtx, id: &ToolCallId) -> Option<String> {
        self.routes
            .lock()
            .unwrap()
            .get(&(ctx.batch_id.clone(), id.clone()))
            .cloned()
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, _: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.started.store(true, Ordering::SeqCst);
        let hold = self.hold.lock().unwrap().clone();
        if let Some(hold) = hold {
            hold.notified().await;
        }
        self.completed.store(true, Ordering::SeqCst);
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "ordinary".into(),
            },
            is_error: false,
        })
    }
    async fn recheck_approval_scope(
        &self,
        ctx: &BatchCtx,
        _: &ToolCall,
        _: Option<&str>,
    ) -> Result<(), ToolPlaneError> {
        assert!(ctx.scope.is_some());
        if self.refuse_scope.load(Ordering::SeqCst) {
            return Err(ToolPlaneError::ScopeRefused {
                code: "scope_ended".into(),
                detail: "ended".into(),
            });
        }
        if let Some(token) = self.interrupt_on_recheck.lock().unwrap().as_ref() {
            token.cancel();
        }
        Ok(())
    }
    async fn dispatch_approved(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        bytes: &str,
        key: &str,
        _: &str,
    ) -> DispatchResult {
        let frames = self.store.read_records(&self.bind).unwrap();
        let target = frames
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::ToolCallDeferred { module, .. }
                    if key.ends_with(&format!(":{}", frame.seq)) =>
                {
                    module.clone()
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            self.dispatch_target(ctx, &call.tool_call_id).as_deref(),
            Some(target.as_str()),
            "dispatch retains the Prepared target"
        );
        assert!(frames.iter().any(|frame| matches!(&frame.record, WalRecord::ApprovalRecorded { call_key, decision: broca_wal::DeferredDecision::Approved, .. } if call_key == key) || matches!(&frame.record,
                WalRecord::LateApprovalRecorded { call_key, decision:broca_wal::LateDecision::Approved, .. } if call_key == key)), "no dispatch without durable approval");
        let prepared = frames
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::ToolCallDeferred { args_jcs, .. }
                    if key.ends_with(&format!(":{}", frame.seq)) =>
                {
                    Some(args_jcs)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            bytes.as_bytes(),
            prepared.as_bytes(),
            "saved arguments reach the provider verbatim"
        );
        self.sent
            .lock()
            .unwrap()
            .push((key.into(), bytes.into(), ctx.batch_id.clone()));
        let outcome = *self.outcome.lock().unwrap();
        match outcome {
            "not_sent" => DispatchResult::NotSent {
                reason: "refused".into(),
            },
            "unknown" => DispatchResult::SentButOutcomeUnknown {
                reason: "timeout".into(),
            },
            _ => self.dispatch(ctx, call).await,
        }
    }
}
struct Rig {
    unhinted: bool,
    writer: WalHandle,
    root: std::path::PathBuf,
    bind: BindIdentity,
    tools: Tools,
    plan: HookPlan,
    hooks: Arc<Hooks>,
    control: RunControl,
    questions: Arc<Questions>,
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn held_wait_bounds_real_answer_reads_and_picks_up_mid_backoff_answer() {
    let mut rig = Rig::new("pass", "none", false, false);
    let origin = epoch_millis().unwrap();
    std::fs::write(
        rig.root.join("test-clock"),
        json!({"now_ms":origin}).to_string(),
    )
    .unwrap();
    let clock = crate::Clock::for_state_root(&rig.root).unwrap();
    rig.control.clock = Some(clock.clone());
    let (entered, mut waiting) = mpsc::unbounded_channel();
    let (release, resume) = mpsc::unbounded_channel();
    rig.control.approval_wait_probe = Some(Arc::new(ApprovalWaitProbe {
        entered,
        release: tokio::sync::Mutex::new(resume),
    }));
    let polls_at_answer = std::cell::Cell::new(None);
    let driver = async {
        let mut elapsed = 0;
        loop {
            let (answer_read_in_flight, work_in_flight) = waiting.recv().await.unwrap();
            if !answer_read_in_flight
                && !work_in_flight
                && rig.questions.polls.load(Ordering::SeqCst) > 0
            {
                if elapsed == 10_000 {
                    let polls = rig.questions.polls.load(Ordering::SeqCst);
                    assert!(
                        polls <= 15,
                        "held wait made {polls} actual answer RPCs in 10 seconds"
                    );
                    polls_at_answer.set(Some(polls));
                    *rig.questions.answer.lock().unwrap() = "decline";
                    elapsed += 1;
                } else {
                    elapsed += 50;
                }
                assert!(
                    elapsed <= 11_001,
                    "answer was not picked up within the one-second backoff cap"
                );
                std::fs::write(
                    rig.root.join("test-clock"),
                    json!({"now_ms":origin+elapsed}).to_string(),
                )
                .unwrap();
                clock.refresh().unwrap();
            }
            release.send(()).unwrap();
        }
    };
    let result = tokio::select! {
        result = rig.batch(&crate::AllowAll, vec![call("held")], None, false) => result,
        () = driver => unreachable!(),
    };
    assert_eq!(result.results.len(), 1);
    assert!(
        polls_at_answer.get().is_some(),
        "wait completed before the answer gate opened"
    );
    assert!(rig.questions.polls.load(Ordering::SeqCst) > polls_at_answer.get().unwrap());
    assert!(clock.now_ms() <= origin + 11_001);
    assert!(
        rig.tools.sent.lock().unwrap().is_empty(),
        "declined call was dispatched"
    );
    rig.finish().await;
}
impl Rig {
    fn new(second: &'static str, answer: &'static str, transient: bool, stop: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "approval-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let bind = BindIdentity::new("/project", "test", "approval");
        let store = crate::FileWalStore::new(&root);
        let writer = WalHandle::spawn(store.open(&bind).unwrap(), 1).unwrap();
        let hooks = Arc::new(Hooks {
            validates: AtomicUsize::new(0),
            second,
            expires: epoch_millis().unwrap() + 60_000,
            seen: Mutex::new(Vec::new()),
        });
        let stop = stop.then(CancellationToken::new);
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let questions = Arc::new(Questions {
            completed: completed.clone(),
            store: crate::FileWalStore::new(&root),
            lineage: writer.lineage_id().unwrap(),
            head: writer.durable_head(),
            keys: Mutex::new(Vec::new()),
            attempts: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            transient,
            cursors: Mutex::new(Vec::new()),
            answer: Mutex::new(answer),
            stop: stop.clone(),
        });
        let mut approve = sub("pre_tool", Some("approve"), "refuse");
        approve["tools"] = json!(["edit"]);
        let plan = HookPlan::from_manifest(&frozen(
            vec![vec![
                sub("pre_tool", Some("mutate"), "pass"),
                approve,
                sub("pre_tool", Some("validate"), "pass"),
            ]],
            None,
            false,
        ))
        .unwrap();
        let control = RunControl {
            hook_plane: Some(hooks.clone()),
            elicitation_plane: Some(questions.clone()),
            stop_requested: stop,
            ..Default::default()
        };
        Self {
            unhinted: false,
            writer,
            root,
            bind: bind.clone(),
            tools: Tools {
                interrupt_on_recheck: Mutex::new(None),
                default_module: Mutex::new("aft".into()),
                routes: Mutex::new(Default::default()),
                hold: Mutex::new(None),
                started: std::sync::atomic::AtomicBool::new(false),
                completed,
                outcome: Mutex::new("completed"),
                refuse_scope: std::sync::atomic::AtomicBool::new(false),
                sent: Mutex::new(Vec::new()),
                store,
                bind,
            },
            plan,
            hooks,
            control,
            questions,
        }
    }
    async fn batch<A: ApprovalPolicy>(
        &self,
        approval: &A,
        calls: Vec<ToolCall>,
        resume: Option<ResumeCtx>,
        restart: bool,
    ) -> BatchOutcome {
        let runtime = HookRuntime {
            plan: &self.plan,
            plane: self.control.hook_plane.as_ref().unwrap(),
            bind: &self.bind,
            writer: &self.writer,
            cancel: self.control.cancel.as_ref(),
            stop: self.control.stop_requested.as_ref(),
            seal: None,
        };
        let mut history = approval_gate::History::new(&self.writer, self.frames()).unwrap();
        let (out, _rx) = mpsc::channel(100);
        let mut tools = vec![tool()];
        if self.unhinted {
            tools[0].module = None;
        }
        tools[0].execution_mode = ExecutionMode::Parallel;
        if calls.iter().any(|call| call.tool_name == "other") {
            let mut other = tools[0].clone();
            other.name = "other".into();
            tools.push(other);
        }
        tokio::time::timeout(std::time::Duration::from_secs(3), Box::pin(approval_gate::batch(&RunId("r".into()), StepId(1), &self.bind, Some(&serde_json::from_value(json!({"owner":{"kind":"reserved", "module_id":"agent"}, "ref":"s", "scope_epoch":1})).unwrap()),
            calls, &ToolManifest::from_tools(tools), &self.tools, approval, &self.writer, resume, &[], false, &self.control, Some(&runtime), None, &out, &mut history, restart))).await.expect("scripted approval batch must resolve").unwrap()
    }
    fn frames(&self) -> Vec<broca_wal::FramedRecord> {
        self.tools.store.read_records(&self.bind).unwrap()
    }
    async fn finish(self) {
        let _ = self.writer.shutdown().await;
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

#[tokio::test]
async fn narrowed_input_is_frozen_as_jcs_and_filed_once_after_sync() {
    let rig = Rig::new("mutate_allowed", "approve", true, false);
    let narrowed = json!({"f":"narrowed", "n":1e-6});
    let approval = NarrowMutatedInput;
    let result = rig.batch(&approval, vec![call("c")], None, false).await;
    assert_eq!(result.results.len(), 1);
    assert_eq!(
        *rig.hooks.seen.lock().unwrap(),
        vec![
            json!({"f":"original"}),
            narrowed.clone(),
            narrowed.clone(),
            narrowed.clone()
        ]
    );
    let sent = rig.tools.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].1, r#"{"f":"narrowed","n":0.000001}"#);
    assert_ne!(sent[0].1, narrowed.to_string());
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record,WalRecord::ToolCallDeferred {args_hash,..} if args_hash=="d30c6c3de7b2f82a7500ffd0e91b5f2d23bf9df2336290b99b06c3f086132f67")));
    let keys = rig.questions.keys.lock().unwrap().clone();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
    drop(keys);
    drop(sent);
    rig.finish().await;
}

#[tokio::test]
async fn first_validation_refuses_before_prepared_and_filing() {
    let rig = Rig::new("first_deny", "approve", false, false);
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], None, false)
        .await;
    assert!(result.results[0].is_error);
    assert!(rig.questions.keys.lock().unwrap().is_empty());
    assert!(!rig
        .frames()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. })));
    rig.finish().await;
}

#[tokio::test]
async fn second_validation_consumes_unavailability_even_when_subscription_passes() {
    let rig = Rig::new("unavailable", "approve", false, false);
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], None, false)
        .await;
    assert!(result.results[0].is_error);
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::ToolResult { reason: Some(reason), .. } if reason == "pre_tool_unavailable")));
    assert!(!rig
        .frames()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::ToolDispatchIntent { .. })));
    rig.finish().await;
}

#[tokio::test]
async fn old_incarnation_is_re_read_and_sibling_keys_are_distinct() {
    let rig = Rig::new("pass", "reset", false, false);
    let result = rig
        .batch(&crate::AllowAll, vec![call("a"), call("b")], None, false)
        .await;
    assert_eq!(result.results.len(), 2);
    let sent = rig.tools.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 2);
    assert_ne!(sent[0].0, sent[1].0);
    drop(sent);
    assert!(rig.questions.polls.load(Ordering::SeqCst) >= 3);
    rig.finish().await;
}

#[tokio::test]
async fn restart_pause_refiles_same_key_without_mutate_and_continues_approved_calls() {
    let mut rig = Rig::new("pass", "approve", false, true);
    let paused = rig
        .batch(&crate::AllowAll, vec![call("c")], None, true)
        .await;
    assert_eq!(paused.interruption, Some(BatchInterruption::RestartPause));
    assert_eq!(paused.open.len(), 1);
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record,WalRecord::AwaitingApproval{pending,..} if pending.len()==1 && pending[0].call_id.0=="c")));
    assert!(!rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolBatchFinished { .. } | WalRecord::ToolResult { .. }
    )));
    let validates = rig.hooks.validates.load(Ordering::SeqCst);
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: paused
            .open
            .iter()
            .map(|held| crate::replay::PreparedCall {
                record_index: 0,
                prepared: held.prepared.clone(),
                approval: None,
                elicitation_id: held.id.clone(),
            })
            .collect(),
    };
    rig.control.stop_requested = None;
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), true)
        .await;
    assert_eq!(result.results.len(), 1);
    assert_eq!(
        rig.hooks.validates.load(Ordering::SeqCst),
        validates + 1,
        "resume runs only second validation"
    );
    assert_eq!(rig.questions.keys.lock().unwrap().len(), 2);
    {
        let keys = rig.questions.keys.lock().unwrap().clone();
        assert_eq!(keys[0], keys[1]);
    }
    assert_eq!(rig.tools.sent.lock().unwrap().len(), 1);
    assert_eq!(
        rig.frames()
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
            .count(),
        1
    );
    rig.finish().await;
}

#[tokio::test]
async fn cancel_racing_restart_pause_withdraws_deferred_questions() {
    let mut rig = Rig::new("pass", "none", false, true);
    let paused = rig
        .batch(&crate::AllowAll, vec![call("c")], None, true)
        .await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    rig.control.cancel = Some(cancel);
    let (out, _rx) = mpsc::channel(8);
    let summary = pause_approval(
        &rig.writer,
        &out,
        &rig.control,
        &rig.bind,
        &RunId("r".into()),
        1,
        Usage::default(),
        0,
        &paused.open,
    )
    .await
    .unwrap();
    assert!(summary.paused.is_none());
    assert_eq!(summary.reason, RunFinishReason::Cancelled);
    let frames = rig.frames();
    assert!(frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Cancelled,
            ..
        }
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolResult {
            withdraw_elicitation: Some(_),
            ..
        }
    )));
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::RunPaused { .. })));
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn cancel_and_nonresuming_stop_during_second_validation_consume_approval() {
    for cancel in [true, false] {
        let mut rig = Rig::new("hold", "approve", false, false);
        let token = CancellationToken::new();
        if cancel {
            rig.control.cancel = Some(token.clone());
        } else {
            rig.control.stop_requested = Some(token.clone());
        }
        let batch = rig.batch(&crate::AllowAll, vec![call("c")], None, false);
        let interrupt = async {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));
            while rig.hooks.validates.load(Ordering::SeqCst) < 2 {
                interval.tick().await;
            }
            token.cancel();
        };
        let (batch, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(batch, interrupt)
        })
        .await
        .expect("in-wait stop must be reachable");
        assert_eq!(
            batch.interruption,
            Some(if cancel {
                BatchInterruption::CallerCancel
            } else {
                BatchInterruption::StopRequested
            })
        );
        let frames = rig.frames();
        assert!(frames.iter().any(|frame| matches!(
            frame.record,
            WalRecord::ApprovalRecorded {
                decision: broca_wal::DeferredDecision::Approved,
                ..
            }
        )));
        assert!(frames.iter().any(|frame| matches!(
            frame.record,
            WalRecord::ToolResult {
                outcome: DispatchOutcome::Interrupted,
                withdraw_elicitation: None,
                ..
            }
        )));
        assert!(!frames
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::ToolDispatchIntent { .. })));
        assert!(rig.tools.sent.lock().unwrap().is_empty());
        rig.finish().await;
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn sibling_result_is_synced_while_approval_waits_and_expiry_uses_brocas_clock() {
    let mut rig = Rig::new("pass", "none", false, false);
    let clock = crate::Clock::for_state_root(&rig.root).unwrap();
    rig.control.clock = Some(clock.clone());
    let mut sibling = call("sibling");
    sibling.tool_name = "other".into();
    let batch = rig.batch(&crate::AllowAll, vec![call("c"), sibling], None, false);
    let expiry = async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));
        loop {
            interval.tick().await;
            let frames = rig.frames();
            let sibling_done = frames.iter().any(|frame| matches!(&frame.record, WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "sibling"));
            let filed = frames.iter().any(|frame| matches!(&frame.record, WalRecord::AwaitingApproval { pending, .. } if pending.iter().any(|entry| entry.elicitation_id.is_some())));
            if sibling_done && filed {
                assert!(!frames
                    .iter()
                    .any(|frame| matches!(frame.record, WalRecord::ToolBatchFinished { .. })));
                std::fs::write(
                    rig.root.join("test-clock"),
                    json!({"now_ms":rig.hooks.expires+1}).to_string(),
                )
                .unwrap();
                clock.refresh().unwrap();
                break;
            }
        }
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(batch, expiry)
    })
    .await
    .unwrap();
    assert_eq!(result.results.len(), 2);
    assert!(rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::ApprovalRecorded {
            decision: broca_wal::DeferredDecision::Expired,
            ..
        }
    )));
    assert_eq!(
        rig.frames()
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ToolResult { .. }))
            .count(),
        1,
        "expiry has only its approval closing result"
    );
    rig.finish().await;
}

#[tokio::test]
async fn absent_elicitation_adapter_closes_before_prepared_and_installed_plane_prepares() {
    for installed in [false, true] {
        let mut rig = Rig::new("pass", "approve", false, false);
        if !installed {
            rig.control.elicitation_plane = None;
        }
        let result = rig
            .batch(&crate::AllowAll, vec![call("c")], None, false)
            .await;
        let frames = rig.frames();
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
                .count(),
            usize::from(installed)
        );
        if installed {
            assert!(!result.results[0].is_error);
            assert_eq!(rig.tools.sent.lock().unwrap().len(), 1);
        } else {
            assert!(result.results[0].is_error);
            assert!(
                matches!(&result.results[0].output, ToolOutput::Error { message } if message.starts_with("pre_tool_unavailable:"))
            );
            assert!(rig.tools.sent.lock().unwrap().is_empty());
            assert!(rig.questions.keys.lock().unwrap().is_empty());
            assert_eq!(
                rig.hooks.seen.lock().unwrap().len(),
                3,
                "mutate, approve and first validate run once"
            );
            assert_eq!(rig.hooks.validates.load(Ordering::SeqCst), 1);
            assert!(frames.iter().any(|frame| matches!(&frame.record, WalRecord::ToolResult { reason: Some(reason), .. } if reason == "pre_tool_unavailable")));
            assert!(!frames.iter().any(|frame| matches!(
                frame.record,
                WalRecord::ApprovalRecorded { .. }
                    | WalRecord::AwaitingApproval { .. }
                    | WalRecord::ToolDispatchIntent { .. }
            )));
        }
        rig.finish().await;
    }
}

#[tokio::test]
async fn decline_never_dispatches_and_uses_only_approval_recorded() {
    let rig = Rig::new("pass", "decline", false, false);
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], None, false)
        .await;
    assert!(result.results[0].is_error);
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    let frames = rig.frames();
    assert!(!frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolDispatchIntent { .. } | WalRecord::ToolResult { .. }
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ApprovalRecorded {
            decision: broca_wal::DeferredDecision::Declined,
            approval_digest: None,
            ..
        }
    )));
    rig.finish().await;
}

struct UnavailableElicitationPlane;
#[async_trait::async_trait]
impl ElicitationPlane for UnavailableElicitationPlane {}

#[tokio::test]
async fn installed_plane_filing_refusal_records_unavailable_after_prepared() {
    let mut rig = Rig::new("pass", "approve", false, false);
    rig.control.elicitation_plane = Some(Arc::new(UnavailableElicitationPlane));
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], None, false)
        .await;
    assert!(result.results[0].is_error);
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    let frames = rig.frames();
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
            .count(),
        1
    );
    assert!(frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ApprovalRecorded {
            decision: broca_wal::DeferredDecision::Unavailable,
            closing_result: Some(_),
            ..
        }
    )));
    assert!(!frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolDispatchIntent { .. } | WalRecord::ToolResult { .. }
    )));
    rig.finish().await;
}

struct UsagePricedWarmer {
    calls: AtomicUsize,
}
#[async_trait::async_trait]
impl crate::keep_warm_wait::KeepWarmDriver for UsagePricedWarmer {
    fn timing(&self, _: &RunConfig) -> Result<crate::keep_warm_wait::WarmTiming, String> {
        Ok(crate::keep_warm_wait::WarmTiming {
            ttl_ms: 1000,
            lead_ms: 100,
            after_send_ms: Some(1),
        })
    }
    fn last_sent_at_ms(&self) -> Option<u64> {
        Some(epoch_millis().unwrap() - 2)
    }
    fn costs(&self, _: &RunConfig) -> Result<crate::keep_warm::WarmCosts, String> {
        Ok(crate::keep_warm::WarmCosts {
            warm: 1.0,
            bust: 3.0,
        })
    }
    fn recorded_cost(&self, _: &RunConfig, usage: &Usage) -> Result<f64, String> {
        Ok(usage.output_tokens.unwrap() as f64 / 10.0)
    }
    async fn warm(&self, _: &RunConfig, _: &RunId) -> crate::keep_warm_wait::WarmTick {
        self.calls.fetch_add(1, Ordering::SeqCst);
        crate::keep_warm_wait::WarmTick::ResponsesSent {
            usage: Usage {
                output_tokens: Some(11),
                ..Default::default()
            },
            cost: 1.1,
        }
    }
}
#[tokio::test]
async fn restart_wait_spend_uses_batch_cache_warmed_usage_and_starts_no_extra_warm() {
    let config: RunConfig = serde_json::from_value(json!({"model":{"provider_module_id":"stub","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"family_id":"openai_responses"}})).unwrap();
    let warmer = Arc::new(UsagePricedWarmer {
        calls: AtomicUsize::new(0),
    });
    let driver: Arc<dyn crate::keep_warm_wait::KeepWarmDriver> = warmer.clone();
    let run = RunId("r".into());
    let unsupported = std::sync::atomic::AtomicBool::new(false);
    let missed = std::sync::atomic::AtomicBool::new(false);
    let warm = || crate::keep_warm_wait::WarmWait {
        driver: &driver,
        config: &config,
        run_id: &run,
        cancel: None,
        unsupported_logged: &unsupported,
        warm_missed: &missed,
    };
    let usage = Usage {
        output_tokens: Some(11),
        ..Default::default()
    };
    let frames: Vec<_> = (1..=3)
        .map(|seq| broca_wal::FramedRecord {
            seq,
            fence: 1,
            record: WalRecord::CacheWarmed {
                trigger: Some("keep_warm".into()),
                run_id: run.clone(),
                step_id: StepId(if seq < 3 { 1 } else { 2 }),
                usage,
                request_body_sha256: "request".into(),
                warm_body_sha256: "warm".into(),
                served_account_ref: None,
                ratelimit_headers: Default::default(),
                sent_at_ms: 0,
                ttl_ms: Some(1000),
                expires_at_ms: Some(1000),
            },
        })
        .collect();
    let spend = crate::keep_warm_wait::batch_spend(Some(&warm()), StepId(1), &frames);
    assert_eq!(
        spend, 2.2,
        "only this batch's two returned-cost warms count"
    );
    crate::keep_warm_wait::wait_tools_with_spend(
        tokio::time::sleep(std::time::Duration::from_millis(5)),
        Some(warm()),
        spend,
    )
    .await;
    assert_eq!(
        warmer.calls.load(Ordering::SeqCst),
        0,
        "restart cannot purchase another cache-bust allowance"
    );
}

#[cfg(feature = "test-clock")]
async fn expired_run(second: &'static str) -> Rig {
    let mut rig = Rig::new(second, "none", false, false);
    let clock = crate::Clock::for_state_root(&rig.root).unwrap();
    rig.control.clock = Some(clock.clone());
    rig.control.manifest = Some(Arc::new(frozen(
        vec![vec![
            sub("pre_tool", Some("approve"), "refuse"),
            sub("pre_tool", Some("validate"), "pass"),
        ]],
        None,
        false,
    )));
    let mut req = hook_tests::request();
    req.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    req.session = rig.bind.clone();
    req.scope = Some(
        serde_json::from_value(
            json!({"owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1}),
        )
        .unwrap(),
    );
    let event = |step, content, finish_reason| {
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: broca_protocol::MessageId(format!("m{step}")),
                    content,
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason,
                retries_used: 0,
            }),
        ]
    };
    let provider = crate::StubProvider::new(vec![
        event(
            1,
            vec![ContentBlock::ToolCall(call("c"))],
            FinishReason::ToolCalls,
        ),
        event(
            2,
            vec![ContentBlock::Text {
                text: "done".into(),
            }],
            FinishReason::Stop,
        ),
    ]);
    let (out, _rx) = mpsc::channel(1000);
    let run = Box::pin(run_body_with_approval_history(
        &mut req,
        replay(&[]).unwrap(),
        None,
        false,
        false,
        false,
        &rig.control,
        &provider,
        &rig.tools,
        &crate::AllowAll,
        &rig.writer,
        || Ok(false),
        None,
        &out,
        None,
        Some(&rig.tools.store),
        None,
    ));
    let expire = async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));
        loop {
            interval.tick().await;
            if rig.frames().iter().any(|frame| matches!(&frame.record,WalRecord::AwaitingApproval{pending,..} if pending.iter().any(|entry|entry.elicitation_id.is_some()))) { break }
        }
        std::fs::write(
            rig.root.join("test-clock"),
            json!({"now_ms":rig.hooks.expires+1}).to_string(),
        )
        .unwrap();
        clock.refresh().unwrap();
    };
    let (summary, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(run, expire)
    })
    .await
    .unwrap();
    assert_eq!(summary.unwrap().reason, RunFinishReason::Completed);
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    rig
}
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_execute_uses_originating_plan_is_isolated_and_at_most_once_at_every_cut() {
    let mut rig = expired_run("pass").await;
    *rig.questions.answer.lock().unwrap() = "approve";
    rig.control.manifest = Some(Arc::new(frozen(Vec::new(), None, false)));
    let before = rig.frames();
    let prompt = serde_json::to_vec(
        &replay(
            &before
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .prompt,
    )
    .unwrap();
    let mut history = approval_gate::History::new(&rig.writer, before.clone()).unwrap();
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    let after = rig.frames();
    assert_eq!(rig.tools.sent.lock().unwrap().len(), 1);
    assert!(rig.tools.sent.lock().unwrap()[0].2 .0.starts_with("late:"));
    assert_eq!(
        after
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::LateApprovalRecorded { .. }))
            .count(),
        1
    );
    assert_eq!(
        after
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::LateDispatchIntent { .. }))
            .count(),
        1
    );
    assert_eq!(
        after
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ToolBatchStarted { .. }))
            .count(),
        before
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ToolBatchStarted { .. }))
            .count()
    );
    assert_eq!(
        prompt,
        serde_json::to_vec(
            &replay(
                &after
                    .iter()
                    .map(|frame| frame.record.clone())
                    .collect::<Vec<_>>()
            )
            .unwrap()
            .prompt
        )
        .unwrap()
    );
    // Reopen a WAL that retained the late dispatch intent but lost the result.
    // The tool may already have executed, so recovery must record an unknown
    // outcome instead of using the saved approval to dispatch again.
    let bytes = std::fs::read(rig.tools.store.wal_path(&rig.bind)).unwrap();
    let mut offset = 0;
    while offset < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[offset..]) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                if matches!(framed.record, WalRecord::LateResult { .. }) {
                    break;
                }
                offset += consumed;
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
            _ => panic!("reference WAL is complete"),
        }
    }
    let cut_store = crate::FileWalStore::new(rig.root.join("late-cut"));
    let path = cut_store.wal_path(&rig.bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &bytes[..offset]).unwrap();
    let cut_writer = WalHandle::spawn(cut_store.open(&rig.bind).unwrap(), 2).unwrap();
    let cut_tools = Tools {
        interrupt_on_recheck: Mutex::new(None),
        default_module: Mutex::new("aft".into()),
        routes: Mutex::new(Default::default()),
        hold: Mutex::new(None),
        started: std::sync::atomic::AtomicBool::new(false),
        completed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        outcome: Mutex::new("completed"),
        refuse_scope: std::sync::atomic::AtomicBool::new(false),
        sent: Mutex::new(Vec::new()),
        store: cut_store,
        bind: rig.bind.clone(),
    };
    let mut cut_history = approval_gate::History::new(
        &cut_writer,
        cut_tools.store.read_records(&rig.bind).unwrap(),
    )
    .unwrap();
    approval_gate::admission(
        &mut cut_history,
        &cut_writer,
        &rig.bind,
        &rig.control,
        &cut_tools,
    )
    .await
    .unwrap();
    assert!(cut_tools.sent.lock().unwrap().is_empty());
    assert!(cut_tools
        .store
        .read_records(&rig.bind)
        .unwrap()
        .iter()
        .any(|frame| matches!(
            frame.record,
            WalRecord::LateResult {
                outcome: broca_wal::LateOutcome::Unknown,
                ..
            }
        )));
    let _ = cut_writer.shutdown().await;
    rig.finish().await;
}
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_none_requires_terminal_get_and_later_approval_is_not_lost_at_stream_head() {
    let rig = expired_run("pass").await;
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    assert!(!rig
        .frames()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::LateApprovalRecorded { .. })));
    *rig.questions.answer.lock().unwrap() = "terminal";
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    assert!(rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::LateApprovalRecorded {
            decision: broca_wal::LateDecision::None,
            ..
        }
    )));
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    rig.finish().await;
    let rig = expired_run("pass").await;
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    *rig.questions.answer.lock().unwrap() = "approve";
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    assert_eq!(rig.tools.sent.lock().unwrap().len(), 1);
    rig.finish().await;
}
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_validation_and_plane_outcomes_are_terminal_and_never_retried() {
    for mode in ["deny", "not_sent", "unknown"] {
        let rig = expired_run(if mode == "deny" { "deny" } else { "pass" }).await;
        *rig.questions.answer.lock().unwrap() = "approve";
        *rig.tools.outcome.lock().unwrap() = mode;
        let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
        approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await
        .unwrap();
        approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await
        .unwrap();
        assert_eq!(
            rig.tools.sent.lock().unwrap().len(),
            usize::from(mode != "deny")
        );
        if mode == "deny" {
            assert!(rig.frames().iter().any(|frame|matches!(&frame.record,WalRecord::LateResult{result,..} if matches!(&result.output,ToolOutput::Error{message} if message.starts_with("pre_tool_denied:")))));
        }
        let outcomes: Vec<_> = rig
            .frames()
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::LateResult { outcome, .. } => Some(*outcome),
                _ => None,
            })
            .collect();
        assert_eq!(
            outcomes,
            [if mode == "unknown" {
                broca_wal::LateOutcome::Unknown
            } else {
                broca_wal::LateOutcome::NotSent
            }]
        );
        rig.finish().await;
    }
}

#[tokio::test]
async fn in_wait_stop_drains_a_dispatched_sibling_before_restart_pause() {
    let mut rig = Rig::new("pass", "none", false, false);
    let hold = Arc::new(tokio::sync::Notify::new());
    *rig.tools.hold.lock().unwrap() = Some(hold.clone());
    let stop = CancellationToken::new();
    rig.control.stop_requested = Some(stop.clone());
    let done = std::sync::atomic::AtomicBool::new(false);
    let mut sibling = call("sibling");
    sibling.tool_name = "other".into();
    let batch = async {
        let result = rig
            .batch(&crate::AllowAll, vec![call("c"), sibling], None, true)
            .await;
        done.store(true, Ordering::SeqCst);
        result
    };
    let stop_and_release = async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));
        while !rig.tools.started.load(Ordering::SeqCst) {
            interval.tick().await;
        }
        stop.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !done.load(Ordering::SeqCst),
            "an in-flight dispatch must drain before the pause returns"
        );
        hold.notify_one();
    };
    let (paused, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(batch, stop_and_release)
    })
    .await
    .unwrap();
    assert_eq!(paused.interruption, Some(BatchInterruption::RestartPause));
    assert!(rig.frames().iter().any(|frame|matches!(&frame.record,WalRecord::ToolResult{result,..} if result.tool_call_id.0=="sibling")));
    assert!(!rig
        .frames()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::ToolBatchFinished { .. })));
    rig.finish().await;
}

#[tokio::test]
async fn approved_not_dispatched_restart_resumes_only_second_validation() {
    let mut rig = Rig::new("hold", "approve", false, false);
    let stop = CancellationToken::new();
    rig.control.stop_requested = Some(stop.clone());
    let batch = rig.batch(&crate::AllowAll, vec![call("c")], None, true);
    let interrupt = async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));
        while rig.hooks.validates.load(Ordering::SeqCst) < 2 {
            interval.tick().await;
        }
        stop.cancel();
    };
    let (paused, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(batch, interrupt)
    })
    .await
    .unwrap();
    assert_eq!(paused.interruption, Some(BatchInterruption::RestartPause));
    assert!(paused.open[0].digest.is_some());
    let frames = rig.frames();
    let recorded = frames
        .iter()
        .find(|frame| {
            matches!(
                frame.record,
                WalRecord::ApprovalRecorded {
                    decision: broca_wal::DeferredDecision::Approved,
                    ..
                }
            )
        })
        .unwrap()
        .record
        .clone();
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: paused.open[0].prepared.clone(),
            approval: Some(Arc::new(recorded)),
            elicitation_id: paused.open[0].id.clone(),
        }],
    };
    rig.control.stop_requested = None;
    rig.hooks = Arc::new(Hooks {
        validates: AtomicUsize::new(0),
        second: "pass",
        expires: rig.hooks.expires,
        seen: Mutex::new(Vec::new()),
    });
    rig.control.hook_plane = Some(rig.hooks.clone());
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), true)
        .await;
    assert_eq!(result.results.len(), 1);
    assert_eq!(rig.questions.keys.lock().unwrap().len(), 1);
    assert_eq!(
        rig.hooks.seen.lock().unwrap().len(),
        1,
        "only the validate hook is re-entered"
    );
    assert_eq!(rig.tools.sent.lock().unwrap().len(), 1);
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_notify_and_decline_record_once_and_stop_answer_reads() {
    for mode in ["notify", "decline"] {
        let rig = expired_run(if mode == "notify" { "notify" } else { "pass" }).await;
        *rig.questions.answer.lock().unwrap() = if mode == "notify" {
            "approve"
        } else {
            "decline"
        };
        let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
        approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await
        .unwrap();
        let reads = rig.questions.polls.load(Ordering::SeqCst);
        approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await
        .unwrap();
        assert_eq!(rig.questions.polls.load(Ordering::SeqCst), reads);
        assert_eq!(
            rig.frames()
                .iter()
                .filter(|frame| matches!(frame.record, WalRecord::LateApprovalRecorded { .. }))
                .count(),
            1
        );
        assert!(!rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::LateDispatchIntent { .. } | WalRecord::LateResult { .. }
        )));
        assert!(rig.tools.sent.lock().unwrap().is_empty());
        rig.finish().await;
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn answer_read_after_broca_expiry_is_classified_late_before_terminal_deduplication() {
    let mut rig = Rig::new("pass", "overdue", false, false);
    let clock = crate::Clock::for_state_root(&rig.root).unwrap();
    rig.control.clock = Some(clock.clone());
    let hold = Arc::new(tokio::sync::Notify::new());
    *rig.tools.hold.lock().unwrap() = Some(hold.clone());
    let mut sibling = call("sibling");
    sibling.tool_name = "other".into();
    let batch = rig.batch(&crate::AllowAll, vec![call("c"), sibling], None, false);
    let expire_and_drain = async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));
        while rig.questions.polls.load(Ordering::SeqCst) == 0 {
            interval.tick().await;
        }
        std::fs::write(
            rig.root.join("test-clock"),
            json!({"now_ms":rig.hooks.expires+1}).to_string(),
        )
        .unwrap();
        clock.refresh().unwrap();
        loop {
            interval.tick().await;
            if rig
                .frames()
                .iter()
                .any(|frame| matches!(frame.record, WalRecord::LateApprovalRecorded { .. }))
            {
                break;
            }
        }
        hold.notify_one();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(batch, expire_and_drain)
    })
    .await
    .unwrap();
    assert_eq!(result.results.len(), 2);
    let frames = rig.frames();
    assert!(frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ApprovalRecorded {
            decision: broca_wal::DeferredDecision::Expired,
            ..
        }
    )));
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(
                frame.record,
                WalRecord::LateApprovalRecorded {
                    decision: broca_wal::LateDecision::Approved,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(
        rig.tools.sent.lock().unwrap().is_empty(),
        "live wait records late consent but execution belongs to admission"
    );
    rig.finish().await;
}

#[tokio::test]
async fn second_validation_scope_refusal_consumes_approval_without_withdrawal() {
    let rig = Rig::new("pass", "approve", false, false);
    rig.tools.refuse_scope.store(true, Ordering::SeqCst);
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], None, false)
        .await;
    assert!(result.results[0].is_error);
    let frames = rig.frames();
    assert!(frames.iter().any(|frame|matches!(&frame.record,WalRecord::ToolResult{reason:Some(reason),withdraw_elicitation:None,..} if reason=="scope_ended")));
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::ToolDispatchIntent { .. })));
    assert!(
        crate::deferred_call_obligations(rig.writer.lineage_id().unwrap(), &frames)
            .withdrawals
            .is_empty()
    );
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn deferred_resume_uses_saved_target_when_catalog_resolution_changes() {
    let mut rig = Rig::new("pass", "approve", false, true);
    rig.unhinted = true;
    let paused = rig
        .batch(&crate::AllowAll, vec![call("c")], None, true)
        .await;
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: paused
            .open
            .iter()
            .map(|held| crate::replay::PreparedCall {
                record_index: 0,
                prepared: held.prepared.clone(),
                approval: None,
                elicitation_id: held.id.clone(),
            })
            .collect(),
    };
    rig.control.stop_requested = None;
    *rig.tools.default_module.lock().unwrap() = "changed-provider".into();
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), true)
        .await;
    assert!(!result.results[0].is_error);
    assert_eq!(rig.tools.sent.lock().unwrap().len(), 1);
    rig.finish().await;
}

#[tokio::test]
async fn approved_schema_pin_is_refused_instead_of_silently_dropped_by_raw_carrier() {
    let rig = Rig::new("pass", "approve", false, false);
    let batch = BatchId("r-b1".into());
    rig.writer.record(WalRecord::ToolBatchStarted {
        batch_id: batch.clone(),
        step_id: StepId(1),
        planned_calls: vec![ToolCallId("c".into())],
    });
    let mut prepared = crate::replay::deferred_tests::prepared("c", &batch.0);
    if let WalRecord::ToolCallDeferred { schema_pin, .. } = &mut prepared {
        *schema_pin = Some(broca_wal::DispatchSchemaPin {
            schema_digest: "sha256:pinned".into(),
            semantics: 1,
        });
    }
    let seq = rig.writer.record(prepared.clone()).into_result().unwrap();
    let key = crate::context::call_key(rig.writer.lineage_id().unwrap(), seq);
    let approval = WalRecord::ApprovalRecorded {
        call_key: key,
        call_id: ToolCallId("c".into()),
        elicitation_id: Some("q".into()),
        decision: broca_wal::DeferredDecision::Approved,
        approval_digest: Some("digest".into()),
        closing_result: None,
    };
    rig.writer.record(approval.clone());
    rig.writer.sync().await.unwrap();
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: Arc::new(prepared),
            approval: Some(Arc::new(approval)),
            elicitation_id: Some("q".into()),
        }],
    };
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), false)
        .await;
    assert!(result.results[0].is_error);
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    assert!(rig.frames().iter().any(|frame|matches!(&frame.record,WalRecord::ToolResult{reason:Some(reason),..} if reason=="approval_schema_pin_unsupported")));
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
async fn record_late_consent(rig: &Rig) {
    let frame = rig
        .frames()
        .into_iter()
        .find(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
        .unwrap();
    let key = crate::context::call_key(rig.writer.lineage_id().unwrap(), frame.seq);
    rig.writer.record(WalRecord::LateApprovalRecorded {
        call_key: key.clone(),
        elicitation_id: format!("q:{key}"),
        decision: broca_wal::LateDecision::Approved,
        approval_digest: Some("digest".into()),
        late_execution: Some(broca_wal::LateExecution::Execute),
    });
    rig.writer.sync().await.unwrap();
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_cancel_at_successful_scope_recheck_starts_no_dispatch() {
    for cancel in [true, false] {
        let mut rig = expired_run("pass").await;
        record_late_consent(&rig).await;
        let token = CancellationToken::new();
        if cancel {
            rig.control.cancel = Some(token.clone());
        } else {
            rig.control.stop_requested = Some(token.clone());
        }
        *rig.tools.interrupt_on_recheck.lock().unwrap() = Some(token.clone());
        let before = rig.frames();
        let mut history = approval_gate::History::new(&rig.writer, before.clone()).unwrap();
        approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await
        .unwrap();
        assert!(
            token.is_cancelled(),
            "successful recheck must reach the cancellation race"
        );
        assert!(
            rig.tools.sent.lock().unwrap().is_empty(),
            "a pending cancel or stop forbids a new late dispatch"
        );
        assert!(!rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::LateDispatchIntent { .. } | WalRecord::LateResult { .. }
        )));
        assert_eq!(rig.frames(), before);
        rig.finish().await;
    }
}

#[tokio::test]
async fn unreadable_answer_does_not_advance_cursor() {
    for answer in [
        "unreadable_choice_missing",
        "unreadable_choice_type",
        "unreadable_id_missing",
        "unreadable_id_type",
        "unreadable_digest_missing",
        "unreadable_digest_type",
    ] {
        let rig = Rig::new("pass", answer, false, false);
        let result = rig
            .batch(&crate::AllowAll, vec![call("c")], None, false)
            .await;
        assert!(
            !result.results[0].is_error,
            "corrected approval must be read instead of closed unavailable"
        );
        assert_eq!(rig.tools.sent.lock().unwrap().len(), 1);
        assert_eq!(
            *rig.questions.cursors.lock().unwrap(),
            vec![None, None],
            "both reads start at the same cursor position"
        );
        let frames = rig.frames();
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(
                    frame.record,
                    WalRecord::ApprovalRecorded {
                        decision: broca_wal::DeferredDecision::Approved,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(!frames.iter().any(|frame| matches!(
            frame.record,
            WalRecord::ApprovalRecorded {
                decision: broca_wal::DeferredDecision::Unavailable,
                ..
            }
        )));
        rig.finish().await;
    }
}

#[cfg(feature = "test-clock")]
fn tools_for_store(store: crate::FileWalStore, bind: BindIdentity) -> Tools {
    Tools {
        interrupt_on_recheck: Mutex::new(None),
        default_module: Mutex::new("aft".into()),
        routes: Mutex::new(Default::default()),
        hold: Mutex::new(None),
        started: std::sync::atomic::AtomicBool::new(false),
        completed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        outcome: Mutex::new("completed"),
        refuse_scope: std::sync::atomic::AtomicBool::new(false),
        sent: Mutex::new(Vec::new()),
        store,
        bind,
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn running_approved_resume_without_elicitation_plane_never_dispatches() {
    let mut rig = expired_run("pass").await;
    let bytes = std::fs::read(rig.tools.store.wal_path(&rig.bind)).unwrap();
    let mut offset = 0;
    while offset < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[offset..]) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                if matches!(framed.record, WalRecord::ApprovalRecorded { .. }) {
                    break;
                }
                offset += consumed;
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
            _ => panic!("fixture WAL is complete"),
        }
    }
    let store = crate::FileWalStore::new(rig.root.join("running-approved"));
    let path = store.wal_path(&rig.bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &bytes[..offset]).unwrap();
    let writer = WalHandle::spawn(store.open(&rig.bind).unwrap(), 2).unwrap();
    let prepared = store
        .read_records(&rig.bind)
        .unwrap()
        .into_iter()
        .find(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
        .unwrap();
    let key = crate::context::call_key(writer.lineage_id().unwrap(), prepared.seq);
    writer.record(WalRecord::ApprovalRecorded {
        call_key: key,
        call_id: ToolCallId("c".into()),
        elicitation_id: Some("q".into()),
        decision: broca_wal::DeferredDecision::Approved,
        approval_digest: Some("digest".into()),
        closing_result: None,
    });
    writer.sync().await.unwrap();
    let tools = tools_for_store(store, rig.bind.clone());
    let before = tools.store.read_records(&rig.bind).unwrap();
    let prior = replay(
        &before
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert!(
        matches!(&prior.resolution,ReplayResolution::Resume{pending:Some(pending)} if pending.approved.len()==1)
    );
    rig.control.elicitation_plane = None;
    let mut request = hook_tests::request();
    request.session = rig.bind.clone();
    let provider = crate::StubProvider::new(Vec::new());
    let (out, _rx) = mpsc::channel(1000);
    let result = Box::pin(run_body_with_approval_history(
        &mut request,
        prior,
        None,
        true,
        false,
        false,
        &rig.control,
        &provider,
        &tools,
        &crate::AllowAll,
        &writer,
        || Ok(false),
        None,
        &out,
        None,
        Some(&tools.store),
        None,
    ))
    .await;
    assert!(
        matches!(result,Err(RunError::Config(ref reason)) if reason.starts_with("deferred_call_open:")),
        "running approval must use the same absent-plane refusal as a restart pause: {result:?}"
    );
    assert!(tools.sent.lock().unwrap().is_empty());
    assert_eq!(tools.store.read_records(&rig.bind).unwrap(), before);
    let _ = writer.shutdown().await;
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn recorded_late_approval_without_elicitation_plane_never_dispatches() {
    let mut rig = expired_run("pass").await;
    record_late_consent(&rig).await;
    rig.control.elicitation_plane = None;
    let before = rig.frames();
    let prior = replay(
        &before
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut request = hook_tests::request();
    request.session = rig.bind.clone();
    request.run_id = RunId("next".into());
    let provider = crate::StubProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: broca_protocol::MessageId("next".into()),
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
    let (out, _rx) = mpsc::channel(1000);
    let result = Box::pin(run_body_with_approval_history(
        &mut request,
        prior,
        None,
        true,
        true,
        false,
        &rig.control,
        &provider,
        &rig.tools,
        &crate::AllowAll,
        &rig.writer,
        || Ok(false),
        None,
        &out,
        None,
        Some(&rig.tools.store),
        None,
    ))
    .await;
    assert!(
        matches!(result,Err(RunError::Config(ref reason)) if reason.starts_with("deferred_call_open:")),
        "recorded late approval must refuse before admitting or dispatching: {result:?}"
    );
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    assert_eq!(rig.frames(), before);
    rig.finish().await;
}

struct DenyMutatedInput {
    seen: Mutex<Vec<Value>>,
}
#[async_trait::async_trait]
impl ApprovalPolicy for DenyMutatedInput {
    async fn decide(&self, call: &ToolCall) -> ApprovalResponse {
        self.seen.lock().unwrap().push(call.input.clone());
        if call.input == json!({"f":"denied"}) {
            ApprovalResponse::Deny {
                tool_call_id: call.tool_call_id.clone(),
                reason: "mutated input denied".into(),
            }
        } else {
            ApprovalResponse::Allow {
                tool_call_id: call.tool_call_id.clone(),
            }
        }
    }
}
struct NarrowMutatedInput;
#[async_trait::async_trait]
impl ApprovalPolicy for NarrowMutatedInput {
    async fn decide(&self, call: &ToolCall) -> ApprovalResponse {
        assert_eq!(
            call.input,
            json!({"f":"mutated","n":1e-6,"keep":true}),
            "policy narrows the post-mutation input"
        );
        ApprovalResponse::AllowNarrowed {
            tool_call_id: call.tool_call_id.clone(),
            input: json!({"f":"narrowed","n":1e-6}),
        }
    }
}

#[tokio::test]
async fn approval_gated_mutated_input_denied_before_ask_never_prepares_or_files() {
    let rig = Rig::new("mutate_denied", "approve", false, false);
    let policy = DenyMutatedInput {
        seen: Mutex::new(Vec::new()),
    };
    let result = rig.batch(&policy, vec![call("c")], None, false).await;
    assert!(
        result.results[0].is_error,
        "policy must refuse the input produced by mutate"
    );
    assert_eq!(*policy.seen.lock().unwrap(), vec![json!({"f":"denied"})]);
    assert_eq!(
        *rig.hooks.seen.lock().unwrap(),
        vec![json!({"f":"original"})],
        "deny stops before approve and validate hooks"
    );
    assert_eq!(rig.hooks.validates.load(Ordering::SeqCst), 0);
    assert!(rig.questions.keys.lock().unwrap().is_empty());
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    let frames = rig.frames();
    assert!(frames.iter().any(|frame|matches!(&frame.record,WalRecord::ToolNoDispatch{reason:NoDispatchReason::Denied{reason},..} if reason=="mutated input denied")));
    assert!(!frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolCallDeferred { .. }
            | WalRecord::ApprovalRecorded { .. }
            | WalRecord::AwaitingApproval { .. }
            | WalRecord::ToolDispatchIntent { .. }
            | WalRecord::ToolResult { .. }
    )));
    rig.finish().await;
}

/// A lagging projection keeps the entire open turn in the WAL tail. Reading a valid empty
/// prefix still takes the store-assisted resume path, rather than its full-WAL fallback.
struct LaggingApprovalStore {
    reads: AtomicUsize,
}
impl crate::MaterializedStore for LaggingApprovalStore {
    fn read_prefix_with_watermark(&self, _: &BindIdentity) -> Result<crate::StorePrefix, String> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(crate::StorePrefix::empty())
    }
}
impl crate::Persister for LaggingApprovalStore {
    fn project(&self, _: &BindIdentity, _: &[broca_wal::FramedRecord]) -> Result<u64, String> {
        Ok(0)
    }
}

#[tokio::test]
async fn store_resume_open_batch_pause_preserves_next_request_bytes() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut paused = Rig::new("pass", "none", false, true);
        let mut uninterrupted = Rig::new("pass", "approve", false, false);
        let manifest = Arc::new(frozen(vec![vec![
            sub("pre_tool", Some("mutate"), "pass"),
            sub("pre_tool", Some("approve"), "refuse"),
            sub("pre_tool", Some("validate"), "pass"),
        ]], None, false));
        paused.control.manifest = Some(manifest.clone());
        uninterrupted.control.manifest = Some(manifest);
        let event = |content, reason| vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {message:AssistantMessage {
                message_id:broca_protocol::MessageId("m".into()), content,
            }}),
            NormalizedEvent::Control(ProviderControl::Finished {usage:Usage::default(), finish_reason:reason, retries_used:0}),
        ];
        let scripts = vec![event(vec![ContentBlock::ToolCall(call("c"))], FinishReason::ToolCalls),
            event(vec![ContentBlock::Text {text:"done".into()}], FinishReason::Stop)];
        let resumed_model = crate::StubProvider::new(scripts.clone());
        let control_model = crate::StubProvider::new(scripts);
        let store = Arc::new(LaggingApprovalStore {reads:AtomicUsize::new(0)});
        let mut request = hook_tests::request();
        request.session = paused.bind.clone();
        request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
        request.scope = Some(serde_json::from_value(json!({"owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1})).unwrap());
        let (out, _rx) = mpsc::channel(1000);
        let summary = run_loop_on_writer_admitted_with_prior_snapshot(request.clone(), &paused.writer,
            StartMode::FreshOrResume, &resumed_model, &paused.tools, &crate::AllowAll, &paused.tools.store,
            Some(store.clone()), paused.control.clone(), &out, None, None).await.unwrap();
        assert_eq!(summary.paused, Some(broca_protocol::PauseReason::Restart));
        let records: Vec<_> = paused.frames().into_iter().map(|frame| frame.record).collect();
        // Both pause reasons and an ordinary NotSent tool turn have the same missing
        // assistant in the committed prompt. Approval-specific state is not the boundary.
        for approval in [false, true] {
            for reason in [broca_protocol::PauseReason::Restart, broca_protocol::PauseReason::AuthRequired] {
                let mut records = records.clone();
                if !approval {
                    records.retain(|record| !matches!(record, WalRecord::ToolCallDeferred {..} | WalRecord::AwaitingApproval {..}));
                }
                if let Some(WalRecord::RunPaused {reason:pause, ..}) = records.last_mut() { *pause = reason; }
                let prior = crate::context::resume_state_from_store(&crate::StorePrefix::empty(), &records, None, None, None).unwrap();
                assert_eq!(prior.completed_steps, 1);
                assert_eq!(prior.prompt.iter().filter(|message| message.role == broca_protocol::Role::Assistant).count(), 0);
                assert!(matches!(&prior.resolution, ReplayResolution::AuthRequired {pending:Some(_), ..}));
                assert!(!store_prior_needs_episode_spans(&prior));
            }
        }
        paused.control.stop_requested = None;
        *paused.questions.answer.lock().unwrap() = "approve";
        let summary = run_loop_on_writer_admitted_with_prior_snapshot(request.clone(), &paused.writer,
            StartMode::ResumeAfterAuth, &resumed_model, &paused.tools, &crate::AllowAll, &paused.tools.store,
            Some(store.clone()), paused.control.clone(), &out, None, None).await.unwrap();
        assert_eq!(summary.reason, RunFinishReason::Completed);
        assert!(store.reads.load(Ordering::SeqCst) > 0, "the regression must execute store-assisted reconstruction");
        request.session = uninterrupted.bind.clone();
        run_loop_on_writer_admitted_with_prior_snapshot(request, &uninterrupted.writer,
            StartMode::FreshOrResume, &control_model, &uninterrupted.tools, &crate::AllowAll, &uninterrupted.tools.store,
            Some(Arc::new(LaggingApprovalStore {reads:AtomicUsize::new(0)})), uninterrupted.control.clone(), &out, None, None).await.unwrap();
        let resumed = resumed_model.calls();
        let control = control_model.calls();
        assert_eq!(resumed.len(), 2);
        assert_eq!(control.len(), 2);
        assert_eq!(crate::Provider::render_request(&resumed_model, &resumed[1]),
            crate::Provider::render_request(&control_model, &control[1]),
            "the shared request-digest renderer must produce identical next-request bytes with and without the open-batch pause");
        paused.finish().await;
        uninterrupted.finish().await;
    }).await.expect("bounded store-assisted restart regression");
}
