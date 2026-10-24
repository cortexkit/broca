use super::*;
use crate::hooks::{HookPlan, HookRuntime, Trace};
use crate::{HookPlane, HookUnavailable};
use cortexkit_role_step_transform_provider::{
    answer::{HookAnswer, Operation},
    hook::{HookCall, Subject},
    subscription::Phase,
};
use serde_json::{json, Value};
use std::sync::Mutex;

struct MemoryWal(Arc<Mutex<Vec<WalRecord>>>);
impl broca_wal::Wal for MemoryWal {
    fn append(&mut self, _: u64, record: &WalRecord, _: bool) -> Result<u64, broca_wal::WalError> {
        let mut records = self.0.lock().unwrap();
        records.push(record.clone());
        Ok(records.len() as u64)
    }
    fn sync(&self) -> Result<(), broca_wal::WalError> {
        Ok(())
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }
    fn tail_after_seq(&self, _: u64) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }
    fn last_seq(&self) -> u64 {
        self.0.lock().unwrap().len() as u64
    }
}
fn writer() -> (WalHandle, Arc<Mutex<Vec<WalRecord>>>) {
    let records = Arc::new(Mutex::new(Vec::new()));
    (
        WalHandle::spawn(Box::new(MemoryWal(records.clone())), 1).unwrap(),
        records,
    )
}

fn records_with_observed_result_times(records: &Mutex<Vec<WalRecord>>) -> Vec<WalRecord> {
    let mut records = records.lock().unwrap().clone();
    for record in &mut records {
        if let WalRecord::ToolResult { finished_at_ms, .. } = record {
            // Content and source order are deterministic; the observation clock is not.
            assert!(
                finished_at_ms.take().is_some(),
                "a written result has an observed finish time"
            );
        }
    }
    records
}
enum Reply {
    Answer(HookAnswer),
    Unavailable,
    Cancel(CancellationToken),
    Never,
}
type SeenCalls = Arc<Mutex<Vec<(String, HookCall)>>>;
type AnswerFn = dyn Fn(&str, &HookCall) -> Reply + Send + Sync;
struct Scripted {
    calls: SeenCalls,
    answer: Box<AnswerFn>,
}
#[async_trait::async_trait]
impl HookPlane for Scripted {
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
        match (self.answer)(provider, &call) {
            Reply::Answer(answer) => Ok(answer),
            Reply::Unavailable => Err(HookUnavailable),
            Reply::Cancel(token) => {
                token.cancel();
                std::future::pending().await
            }
            Reply::Never => std::future::pending().await,
        }
    }
}
fn scripted(
    answer: impl Fn(&str, &HookCall) -> Reply + Send + Sync + 'static,
) -> (Arc<dyn HookPlane>, SeenCalls) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    (
        Arc::new(Scripted {
            calls: calls.clone(),
            answer: Box::new(answer),
        }),
        calls,
    )
}
pub(super) fn sub(hook: &str, phase: Option<&str>, policy: &str) -> Value {
    let mut value = json!({"hook":hook,"ops":if hook=="pre_tool" { json!([]) } else { json!(["append","prepend","replace"]) },"on_unavailable":policy,"budget_ms":500});
    if let Some(phase) = phase {
        value["phase"] = phase.into();
    }
    if hook == "post_assistant" {
        value["ops"] = json!(["append", "prepend"]);
    }
    value
}
pub(super) fn frozen(
    subscriptions: Vec<Vec<Value>>,
    permissions: Option<Vec<&str>>,
    grants: bool,
) -> broca_wal::PlanManifest {
    let items: Vec<Value> = subscriptions.iter().enumerate().map(|(i, subs)| json!({"provider":format!("p{i}"),"preset":"default","params":{"tag":i},"subscriptions":subs})).collect();
    let mut plan = json!({"step_transform_items":items});
    if grants {
        plan["user_grants"] = json!([{"module":"p0","hook":"post_tool","tools":["edit"]}]);
    }
    broca_wal::PlanManifest {
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        tools: vec![tool()].into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![broca_wal::ProviderFetch {
            provider: "aft".into(),
            outcome: broca_wal::FetchOutcome::Answered {
                catalog_digest: String::new(),
                tools: vec![tool()].into(),
                text: None,
                reply: None,
                result_ops: permissions.map(|ops| {
                    [("edit".into(), ops.into_iter().map(str::to_owned).collect())]
                        .into_iter()
                        .collect()
                }),
            },
        }],
        declarations: subscriptions
            .into_iter()
            .enumerate()
            .map(|(i, subs)| broca_wal::FrozenDeclaration {
                provider: format!("p{i}"),
                answer: broca_wal::OpaqueJson::from_json_text(
                    json!({"subscriptions":subs}).to_string(),
                )
                .unwrap(),
            })
            .collect(),
    }
}
pub(super) fn tool() -> Tool {
    Tool {
        name: "edit".into(),
        description: "Edit".into(),
        input_schema: json!({"type":"object","properties":{"f":{"type":"string"}},"required":["f"]}),
        module: Some("aft".into()),
        execution_mode: ExecutionMode::Sequential,
        reply: None,
    }
}
pub(super) fn call(id: &str) -> ToolCall {
    ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "edit".into(),
        input: json!({"f":"original"}),
        thought_signature: Some("signed".into()),
    }
}
fn append(text: &str) -> HookAnswer {
    HookAnswer::Ops {
        ops: vec![Operation::Append {
            block: 0,
            text: text.into(),
            note: None,
        }],
    }
}
fn runtime<'a>(
    plan: &'a HookPlan,
    plane: &'a Arc<dyn HookPlane>,
    bind: &'a BindIdentity,
    writer: &'a WalHandle,
) -> HookRuntime<'a> {
    HookRuntime {
        plan,
        plane,
        bind,
        writer,
        cancel: None,
        stop: None,
        seal: None,
    }
}
async fn batch<A: ApprovalPolicy>(
    calls: Vec<ToolCall>,
    plane: &crate::StubToolPlane,
    approval: &A,
    writer: &WalHandle,
    runtime: &HookRuntime<'_>,
    resume: Option<ResumeCtx>,
) -> BatchOutcome {
    let (out, _rx) = mpsc::channel(64);
    dispatch_batch(
        &RunId("r".into()),
        StepId(1),
        runtime.bind,
        None,
        calls,
        &ToolManifest::from_tools(vec![tool()]),
        plane,
        approval,
        writer,
        resume,
        &[],
        false,
        None,
        Some(runtime),
        None,
        &out,
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn mutated_input_is_revalidated_before_validate_and_terminal_is_not_rehooked() {
    let manifest = frozen(
        vec![vec![
            sub("pre_tool", Some("mutate"), "pass"),
            sub("pre_tool", Some("validate"), "refuse"),
        ]],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, call| match call.subject.phase() {
        Some(Phase::Mutate) => Reply::Answer(HookAnswer::Mutate {
            input: json!({"f":"x".repeat(crate::capability::DEFAULT_MAX_INPUT_BYTES)}),
            note: Some("rewrite".into()),
        }),
        _ => Reply::Answer(HookAnswer::Pass),
    });
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let plane = crate::StubToolPlane::new().with_canned_text("edit", "ok");
    let output = batch(
        vec![call("c")],
        &plane,
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(plane.exec_count(&ToolCallId("c".into())), 0);
    let terminal = records
        .lock()
        .unwrap()
        .iter()
        .find(|r| matches!(r, WalRecord::ToolNoDispatch { .. }))
        .cloned()
        .unwrap();
    let WalRecord::ToolNoDispatch {
        reason: NoDispatchReason::ValidationFailed { .. },
        hooks: Some(metadata),
        ..
    } = terminal
    else {
        panic!("expected validation terminal with hooks")
    };
    assert_eq!(metadata.original, Some(json!({"f":"original"})));
    assert_eq!(
        metadata.final_value,
        Some(json!({"f":"x".repeat(crate::capability::DEFAULT_MAX_INPUT_BYTES)}))
    );
    assert_eq!(metadata.ops[0].op, "mutate");
    let resume = ResumeCtx {
        batch_already_started: true,
        prepared: Vec::new(),
        existing: [(ToolCallId("c".into()), [output.results[0].clone()].into())].into(),
        ..Default::default()
    };
    batch(
        vec![call("c")],
        &plane,
        &crate::AllowAll,
        &writer,
        &runtime,
        Some(resume),
    )
    .await;
    assert_eq!(seen.lock().unwrap().len(), 1);
    let _ = writer.shutdown().await;
}

struct Narrow;
#[async_trait::async_trait]
impl ApprovalPolicy for Narrow {
    async fn decide(&self, call: &ToolCall) -> ApprovalResponse {
        assert_eq!(call.input, json!({"f":"mutated"}));
        ApprovalResponse::AllowNarrowed {
            input: json!({"f":"narrowed"}),
            tool_call_id: call.tool_call_id.clone(),
        }
    }
}
#[tokio::test]
async fn validate_sees_approved_narrowed_args_and_denial_is_one_intentless_barrier_one_terminal() {
    let manifest = frozen(
        vec![vec![
            sub("pre_tool", Some("mutate"), "pass"),
            sub("pre_tool", Some("validate"), "refuse"),
        ]],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, call| match &call.subject {
        Subject::PreTool {
            phase: Phase::Mutate,
            input,
            ..
        } => {
            assert_eq!(input, &json!({"f":"original"}));
            Reply::Answer(HookAnswer::Mutate {
                input: json!({"f":"mutated"}),
                note: None,
            })
        }
        Subject::PreTool {
            phase: Phase::Validate,
            input,
            tool_call_id,
            ..
        } => {
            assert_eq!(input, &json!({"f":"narrowed"}));
            Reply::Answer(if tool_call_id == "c" {
                HookAnswer::Deny {
                    text: "not allowed".into(),
                }
            } else {
                HookAnswer::Pass
            })
        }
        _ => unreachable!(),
    });
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let dispatched_records = records.clone();
    let plane=crate::StubToolPlane::new().with_tool("edit",move |call| {
        assert_eq!(call.input,json!({"f":"narrowed"}));
        assert!(dispatched_records.lock().unwrap().iter().any(|r|matches!(r,WalRecord::ToolResult{result,outcome:DispatchOutcome::Completed,reason:Some(reason),..} if result.tool_call_id.0=="c" && reason=="pre_tool_denied")));
        crate::StubReply::Output(ToolOutput::Text{text:"ok".into()})
    });
    batch(
        vec![call("c"), call("sibling")],
        &plane,
        &Narrow,
        &writer,
        &runtime,
        None,
    )
    .await;
    let records = records.lock().unwrap().clone();
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r,WalRecord::ToolResult{result,..}if result.tool_call_id.0=="c"))
            .count(),
        1
    );
    assert!(!records
        .iter()
        .any(|r| matches!(r,WalRecord::ToolDispatchIntent{tool_call_id,..}if tool_call_id.0=="c")));
    assert_eq!(seen.lock().unwrap().len(), 4);
    drop(records);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn mutated_input_cannot_bypass_approval_deny() {
    let manifest = frozen(
        vec![vec![
            sub("pre_tool", Some("mutate"), "pass"),
            sub("pre_tool", Some("validate"), "refuse"),
        ]],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| {
        Reply::Answer(HookAnswer::Mutate {
            input: json!({"f":"mutated"}),
            note: None,
        })
    });
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let plane = crate::StubToolPlane::new();
    batch(
        vec![call("c")],
        &plane,
        &crate::DenyAll {
            reason: "denied".into(),
        },
        &writer,
        &runtime,
        None,
    )
    .await;
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(records.lock().unwrap().iter().any(|r|matches!(r,WalRecord::ToolNoDispatch{reason:NoDispatchReason::Denied{..},hooks:Some(h),..}if h.final_value==Some(json!({"f":"mutated"})))));
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn pre_tool_unavailable_refuse_is_a_single_completed_intentless_result() {
    let manifest = frozen(
        vec![vec![sub("pre_tool", Some("validate"), "refuse")]],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, _) = scripted(|_, _| Reply::Unavailable);
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    batch(
        vec![call("c")],
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    let records = records.lock().unwrap().clone();
    assert_eq!(records.iter().filter(|r|matches!(r,WalRecord::ToolResult{outcome:DispatchOutcome::Completed,reason:Some(r),..}if r=="pre_tool_unavailable")).count(),1);
    assert!(!records.iter().any(|r| matches!(
        r,
        WalRecord::ToolDispatchIntent { .. } | WalRecord::ToolNoDispatch { .. }
    )));
    drop(records);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn pre_tool_cancel_stops_every_planned_dispatch_and_finishes_announced_batch() {
    let manifest = frozen(
        vec![vec![sub("pre_tool", Some("mutate"), "pass")]],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let (hooks, seen) = scripted(move |_, call| {
        if matches!(&call.subject,Subject::PreTool{tool_call_id,..}if tool_call_id=="second") {
            Reply::Cancel(token.clone())
        } else {
            Reply::Answer(HookAnswer::Pass)
        }
    });
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let mut runtime = runtime(&plan, &hooks, &bind, &writer);
    runtime.cancel = Some(&cancel);
    let plane = crate::StubToolPlane::new();
    let result = batch(
        vec![call("first"), call("second"), call("third")],
        &plane,
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert_eq!(result.interruption, Some(BatchInterruption::CallerCancel));
    assert_eq!(seen.lock().unwrap().len(), 2);
    assert!(!records
        .lock()
        .unwrap()
        .iter()
        .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. })));
    assert!(matches!(
        plane.log().last(),
        Some(crate::StubEvent::BatchFinished)
    ));
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn json_between_limits_stays_structured_with_zero_post_tool_calls() {
    let manifest = frozen(vec![vec![sub("post_tool", None, "refuse")]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Answer(append("!")));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let value = json!({"data":"x".repeat(TOOL_RESULT_MAX_BYTES-POST_TOOL_HEADROOM/2)});
    let expected = value.clone();
    let plane = crate::StubToolPlane::new().with_tool("edit", move |_| {
        crate::StubReply::Output(ToolOutput::Json {
            value: value.clone(),
        })
    });
    let result = batch(
        vec![call("c")],
        &plane,
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert_eq!(
        result.results[0].output,
        ToolOutput::Json { value: expected }
    );
    assert!(seen.lock().unwrap().is_empty());
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn headroom_subject_is_capped_once_before_hook_and_final_append_is_not_recapped() {
    let manifest = frozen(vec![vec![sub("post_tool", None, "refuse")]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, call| {
        let blocks = call.subject.blocks().unwrap();
        assert_eq!(blocks[0].len(), 1_032_192);
        assert!(blocks[0].contains("POST_TOOL_HEADROOM"));
        Reply::Answer(append(&"!".repeat(16_384)))
    });
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let plane = crate::StubToolPlane::new().with_canned_text("edit", &"x".repeat(1_048_577));
    let result = batch(
        vec![call("c")],
        &plane,
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    let ToolOutput::Text { text } = &result.results[0].output else {
        panic!()
    };
    assert_eq!(text.len(), 1_048_576);
    assert!(text.ends_with(&"!".repeat(16_384)));
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(records.lock().unwrap().iter().any(|r|matches!(r,WalRecord::ToolResult{hooks:Some(h),..}if h.original.as_ref().unwrap()["output"]["text"].as_str().unwrap().len()==1_032_192)));
    let _ = writer.shutdown().await;
}

async fn post_answer(
    answer: HookAnswer,
    permissions: Option<Vec<&str>>,
    grants: bool,
) -> (ToolResult, Vec<WalRecord>) {
    let manifest = frozen(
        vec![vec![sub("post_tool", None, "refuse")]],
        permissions,
        grants,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, _) = scripted(move |_, _| Reply::Answer(answer.clone()));
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let result = batch(
        vec![call("c")],
        &crate::StubToolPlane::new().with_canned_text("edit", "original"),
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    let _ = writer.shutdown().await;
    let records = records.lock().unwrap().clone();
    (result.results[0].clone(), records)
}
fn replaced() -> HookAnswer {
    HookAnswer::Ops {
        ops: vec![Operation::Replace {
            block: 0,
            value: "replacement".into(),
            note: None,
        }],
    }
}
#[tokio::test]
async fn final_post_tool_text_above_full_ceiling_is_unavailable() {
    let (result, records) =
        post_answer(append(&"x".repeat(TOOL_RESULT_MAX_BYTES)), None, false).await;
    assert!(result.is_error);
    assert!(records.iter().any(|r|matches!(r,WalRecord::ToolResult{reason:Some(reason),..}if reason=="post_tool_unavailable")));
}
#[tokio::test]
async fn post_tool_replace_requires_both_frozen_catalog_permission_and_user_grant() {
    for (permissions, grants) in [(None, true), (Some(vec!["replace"]), false)] {
        let (result, _) = post_answer(replaced(), permissions, grants).await;
        assert!(result.is_error);
    }
    let (result, records) = post_answer(replaced(), Some(vec!["replace"]), true).await;
    assert_eq!(
        result.output,
        ToolOutput::Text {
            text: "replacement".into()
        }
    );
    assert!(records.iter().any(|r|matches!(r,WalRecord::ToolResult{hooks:Some(h),..}if h.original.as_ref().unwrap()["output"]["text"]=="original")));
}
#[tokio::test]
async fn catalog_result_ops_disallowed_append_is_unavailable() {
    assert!(
        post_answer(append("!"), Some(vec![]), false)
            .await
            .0
            .is_error
    );
}

#[tokio::test]
async fn post_tool_cancel_applies_remaining_refuse_policy_without_calling_later_provider() {
    let manifest = frozen(
        vec![
            vec![sub("post_tool", None, "pass")],
            vec![sub("post_tool", None, "refuse")],
        ],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let (hooks, seen) = scripted(move |_, _| Reply::Cancel(token.clone()));
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let mut runtime = runtime(&plan, &hooks, &bind, &writer);
    runtime.cancel = Some(&cancel);
    let output = batch(
        vec![call("c")],
        &crate::StubToolPlane::new().with_canned_text("edit", "secret"),
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    let ToolOutput::Error { message } = &output.results[0].output else {
        panic!()
    };
    assert!(message.contains("p1"));
    assert!(message.contains("call executed"));
    assert!(!message.contains("secret"));
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(records.lock().unwrap().iter().any(|r|matches!(r,WalRecord::ToolResult{reason:Some(reason),hooks:Some(h),..}if reason=="post_tool_unavailable"&&h.unavailable==Some(vec!["p0".into()]))));
    let _ = writer.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn end_to_end_budget_is_capped_even_when_plan_budget_is_larger() {
    let mut subscription = sub("post_assistant", None, "pass");
    subscription["budget_ms"] = json!(40_000);
    let manifest = frozen(vec![vec![subscription]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Never);
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let start = tokio::time::Instant::now();
    let mut chain = Box::pin(runtime.chain(
        Subject::PostAssistant {
            step_id: "1".into(),
            blocks: vec!["original".into()],
        },
        Trace::default(),
        |_| true,
    ));
    assert!(futures::poll!(chain.as_mut()).is_pending());
    tokio::time::advance(std::time::Duration::from_millis(30_000)).await;
    let std::task::Poll::Ready(output) = futures::poll!(chain.as_mut()) else {
        panic!("hook still running after engine ceiling");
    };
    assert_eq!(start.elapsed(), std::time::Duration::from_millis(30_000));
    assert_eq!(output.trace.unavailable, vec!["p0"]);
    assert_eq!(seen.lock().unwrap().len(), 1);
    drop(chain);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn tool_call_only_ops_are_unavailable_and_all_non_text_parts_stay_byte_identical() {
    let manifest = frozen(vec![vec![sub("post_assistant", None, "pass")]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Answer(append("!")));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let content = vec![
        ContentBlock::Reasoning {
            text: "thinking".into(),
            signature: Some("signature".into()),
        },
        ContentBlock::ToolCall(call("c")),
    ];
    let before = serde_json::to_vec(&content).unwrap();
    let output = runtime
        .chain(
            Subject::PostAssistant {
                step_id: "1".into(),
                blocks: vec![],
            },
            Trace::default(),
            |_| true,
        )
        .await;
    let mut final_content = content;
    crate::hooks::apply_message_ops(&mut final_content, &output.operations).unwrap();
    assert_eq!(before, serde_json::to_vec(&final_content).unwrap());
    assert_eq!(output.trace.unavailable, vec!["p0"]);
    assert!(seen.lock().unwrap()[0]
        .1
        .subject
        .blocks()
        .unwrap()
        .is_empty());
    let _ = writer.shutdown().await;
}

pub(super) fn request() -> RunRequest {
    let submission=serde_json::from_value(json!({"model":{"provider_module_id":"stub","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"go"}]}],"tools":[tool()],"policy_allowed":["edit"],"stop_when":[{"type":"step_count","n":3}]})).unwrap();
    let mut request = RunRequest::from_submission(
        submission,
        RunId("r".into()),
        BindIdentity::new("/project", "caller", "s"),
        broca_protocol::SubmissionId("send".into()),
    );
    request.submission_id = None;
    request
}
struct Messages {
    requests: Arc<Mutex<Vec<CallOptions>>>,
}
#[async_trait::async_trait]
impl Provider for Messages {
    async fn step(&self, step: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.requests.lock().unwrap().push(opts.clone());
        let mut content = vec![
            ContentBlock::Reasoning {
                text: "reason".into(),
                signature: Some("sig".into()),
            },
            ContentBlock::Text {
                text: "answer".into(),
            },
        ];
        if step.0 == 1 {
            content.push(ContentBlock::ToolCall(call("c")));
        }
        let (tx, rx) = mpsc::channel(8);
        tx.send(NormalizedEvent::Display(DisplayEvent::TextDelta {
            mid: None,
            part: None,
            message_id: broca_protocol::MessageId(format!("m{}", step.0)),
            tool_call_id: None,
            delta: "answer".into(),
        }))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: broca_protocol::MessageId(format!("m{}", step.0)),
                    content,
                },
            },
        ))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: if step.0 == 1 {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
            retries_used: 0,
        }))
        .await
        .unwrap();
        rx
    }
}
#[tokio::test]
async fn post_assistant_record_control_next_prompt_and_display_all_use_final_message() {
    for changed in [true, false] {
        let manifest = Arc::new(frozen(
            vec![vec![sub("post_assistant", None, "pass")]],
            None,
            false,
        ));
        let (plane, seen) = scripted(move |_, call| {
            assert_eq!(call.harness, "caller");
            assert_eq!(call.subject.blocks().unwrap(), ["answer"]);
            Reply::Answer(if changed {
                append("+hook")
            } else {
                HookAnswer::Pass
            })
        });
        let control = RunControl {
            manifest: Some(manifest),
            hook_plane: Some(plane),
            ..Default::default()
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let provider = Messages {
            requests: requests.clone(),
        };
        let (writer, records) = writer();
        let (out, mut events) = mpsc::channel(128);
        run_body(
            &mut request(),
            crate::replay::replay(&[]).unwrap(),
            None,
            false,
            false,
            false,
            &control,
            &provider,
            &crate::StubToolPlane::new().with_canned_text("edit", "ok"),
            &crate::AllowAll,
            &writer,
            || Ok(false),
            None,
            &out,
            None,
        )
        .await
        .unwrap();
        let expected = if changed { "answer+hook" } else { "answer" };
        let records = records.lock().unwrap().clone();
        for record in records.iter() {
            if let WalRecord::ModelStepFinished {
                assistant_message,
                hooks,
                ..
            } = record
            {
                assert_eq!(
                    assistant_message.content[1],
                    ContentBlock::Text {
                        text: expected.into()
                    }
                );
                assert_eq!(hooks.is_some(), changed);
                assert_eq!(
                    assistant_message.content[0],
                    ContentBlock::Reasoning {
                        text: "reason".into(),
                        signature: Some("sig".into())
                    }
                );
            }
        }
        drop(records);
        assert!(requests.lock().unwrap()[1]
            .prompt
            .iter()
            .any(|message| message.role == Role::Assistant
                && message.content[1]
                    == ContentBlock::Text {
                        text: expected.into()
                    }));
        let mut display = Vec::new();
        while let Ok(event) = events.try_recv() {
            match event {
                OutputEvent::Display(event) => display.push(event),
                OutputEvent::Control(ControlUnit::AssistantMessage { message }) => assert_eq!(
                    message.content[1],
                    ContentBlock::Text {
                        text: expected.into()
                    }
                ),
                _ => {}
            }
        }
        assert_eq!(
            display
                .iter()
                .filter(|event| matches!(event, DisplayEvent::DisplayReset))
                .count(),
            if changed { 2 } else { 0 }
        );
        if changed {
            assert!(
                matches!(&display[..4],[DisplayEvent::TextDelta{delta,..},DisplayEvent::DisplayReset,DisplayEvent::ReasoningDelta{..},DisplayEvent::TextDelta{delta:final_text,..}]if delta=="answer"&&final_text=="answer+hook")
            );
        }
        assert_eq!(seen.lock().unwrap().len(), 2);
        let _ = writer.shutdown().await;
    }
}

#[tokio::test]
async fn frozen_transform_resume_without_plane_neither_dispatches_nor_writes() {
    // Make a real admitted prefix, then cut at the durable model step. The resumed
    // state is otherwise executable, so an absent-plane check cannot pass merely
    // because a hand-built resume fixture lacks its frozen configuration.
    let manifest = Arc::new(frozen(
        vec![vec![sub("pre_tool", Some("mutate"), "pass")]],
        None,
        false,
    ));
    let (hook_plane, _) = scripted(|_, _| Reply::Answer(HookAnswer::Pass));
    let control = RunControl {
        manifest: Some(manifest),
        hook_plane: Some(hook_plane),
        ..Default::default()
    };
    let (seed_writer, seed) = writer();
    let (out, _rx) = mpsc::channel(128);
    run_body(
        &mut request(),
        crate::replay::replay(&[]).unwrap(),
        None,
        false,
        false,
        false,
        &control,
        &Messages {
            requests: Arc::new(Mutex::new(Vec::new())),
        },
        &crate::StubToolPlane::new().with_canned_text("edit", "ok"),
        &crate::AllowAll,
        &seed_writer,
        || Ok(false),
        None,
        &out,
        None,
    )
    .await
    .unwrap();
    let _ = seed_writer.shutdown().await;
    let mut prefix = Vec::new();
    for record in seed.lock().unwrap().iter() {
        prefix.push(record.clone());
        if matches!(record, WalRecord::ModelStepFinished { .. }) {
            break;
        }
    }
    let mut prior = crate::replay::replay(&prefix).unwrap();
    // The run's off-render reconstruction restores the manifest beside transcript replay.
    prior.manifest = prefix.iter().find_map(|record| match record {
        WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } => Some(Arc::new(manifest.as_ref().clone())),
        _ => None,
    });
    assert!(prior.manifest.is_some());
    assert!(prior.run_config.is_some());
    let (writer, records) = writer();
    let plane = crate::StubToolPlane::new();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Messages {
        requests: requests.clone(),
    };
    let result = run_body(
        &mut request(),
        prior,
        None,
        true,
        false,
        false,
        &RunControl::default(),
        &provider,
        &plane,
        &crate::AllowAll,
        &writer,
        || Ok(false),
        None,
        &out,
        None,
    )
    .await;
    assert!(result.is_err());
    assert!(records.lock().unwrap().is_empty());
    assert!(requests.lock().unwrap().is_empty());
    assert!(plane.log().is_empty());
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn post_assistant_cancel_keeps_accepted_ops_and_marks_entire_remaining_chain_unavailable() {
    let manifest = frozen(
        vec![
            vec![sub("post_assistant", None, "pass")],
            vec![sub("post_assistant", None, "pass")],
            vec![sub("post_assistant", None, "pass")],
        ],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let (hooks, seen) = scripted(move |provider, _| {
        if provider == "p0" {
            Reply::Answer(append("accepted"))
        } else {
            Reply::Cancel(token.clone())
        }
    });
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let mut runtime = runtime(&plan, &hooks, &bind, &writer);
    runtime.cancel = Some(&cancel);
    let output = runtime
        .chain(
            Subject::PostAssistant {
                step_id: "1".into(),
                blocks: vec!["original".into()],
            },
            Trace::default(),
            |_| true,
        )
        .await;
    assert_eq!(output.subject.blocks().unwrap(), ["originalaccepted"]);
    assert_eq!(output.trace.unavailable, vec!["p1", "p2"]);
    assert_eq!(seen.lock().unwrap().len(), 2);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn sequential_post_tool_finishes_before_next_dispatch() {
    let manifest = frozen(vec![vec![sub("post_tool", None, "pass")]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let hook_order = order.clone();
    let (hooks, _) = scripted(move |_, call| {
        if let Subject::PostTool { tool_call_id, .. } = &call.subject {
            hook_order
                .lock()
                .unwrap()
                .push(format!("hook:{tool_call_id}"));
        }
        Reply::Answer(HookAnswer::Pass)
    });
    let dispatched = order.clone();
    let plane = crate::StubToolPlane::new().with_tool("edit", move |call| {
        dispatched
            .lock()
            .unwrap()
            .push(format!("dispatch:{}", call.tool_call_id.0));
        crate::StubReply::Output(ToolOutput::Text { text: "ok".into() })
    });
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    batch(
        vec![call("first"), call("second")],
        &plane,
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert_eq!(
        *order.lock().unwrap(),
        [
            "dispatch:first",
            "hook:first",
            "dispatch:second",
            "hook:second"
        ]
    );
    let _ = writer.shutdown().await;
}

async fn excluded_case(kind: &str) {
    let manifest = frozen(vec![vec![sub("post_tool", None, "pass")]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Answer(HookAnswer::Pass));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let plane = crate::StubToolPlane::new().with_tool("edit", move |call| {
        if call.tool_call_id.0 == "c" {
            crate::StubReply::PlaneUnavailable("offline".into())
        } else {
            crate::StubReply::Output(ToolOutput::Json {
                value: json!({"ok":true}),
            })
        }
    });
    let mut calls = vec![call("c")];
    let mut resume = None;
    match kind {
        "no_dispatch" => calls[0].tool_name = "missing".into(),
        "duplicate" => {
            calls[0].tool_name = "missing".into();
            calls.push(call("c"));
        }
        "route_drained" => plane.fail_next_batch_start(ToolPlaneError::ScopeDrained {
            reason: "scope_ended".into(),
        }),
        "already" => {
            resume = Some(ResumeCtx {
                batch_already_started: true,
                prepared: Vec::new(),
                existing: [(
                    ToolCallId("c".into()),
                    [ToolResult {
                        tool_call_id: ToolCallId("c".into()),
                        output: ToolOutput::Text {
                            text: "durable".into(),
                        },
                        is_error: false,
                    }]
                    .into(),
                )]
                .into(),
                ..Default::default()
            })
        }
        "not_sent" => {}
        _ => unreachable!(),
    }
    calls.push(call("sibling"));
    batch(calls, &plane, &crate::AllowAll, &writer, &runtime, resume).await;
    assert!(seen.lock().unwrap().is_empty(), "{kind}");
    let _ = writer.shutdown().await;
}
#[tokio::test]
async fn no_post_tool_for_no_dispatch() {
    excluded_case("no_dispatch").await;
}
#[tokio::test]
async fn no_post_tool_for_duplicate_synthetic() {
    excluded_case("duplicate").await;
}
#[tokio::test]
async fn no_post_tool_for_route_drained_synthetic() {
    excluded_case("route_drained").await;
}
#[tokio::test]
async fn no_post_tool_for_not_sent() {
    excluded_case("not_sent").await;
}
#[tokio::test]
async fn no_post_tool_for_resumed_already() {
    excluded_case("already").await;
}
#[tokio::test]
async fn no_post_tool_for_unknown_closure() {
    excluded_unknown(false).await;
}

#[tokio::test]
async fn no_post_tool_for_interrupted_closure() {
    excluded_unknown(true).await;
}

async fn excluded_unknown(interrupted: bool) {
    let manifest = frozen(vec![vec![sub("post_tool", None, "pass")]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Answer(HookAnswer::Pass));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let plane = crate::StubToolPlane::new()
        .with_tool("edit", |_| crate::StubReply::OutcomeUnknown("lost".into()));
    let result = batch(
        if interrupted {
            vec![call("c"), call("interrupted")]
        } else {
            vec![call("c")]
        },
        &plane,
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert_eq!(result.interruption, Some(BatchInterruption::Ambiguous));
    assert_eq!(plane.exec_count(&ToolCallId("interrupted".into())), 0);
    assert!(seen.lock().unwrap().is_empty());
    let _ = writer.shutdown().await;
}

fn result_record(id: &str, text: &str) -> WalRecord {
    WalRecord::ToolResult {
        finished_at_ms: None,
        tags: None,
        batch_id: BatchId("b".into()),
        result: Arc::new(ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        }),
        outcome: DispatchOutcome::Completed,
        reason: None,
        hooks: None,
        withdraw_elicitation: None,
        provider_held: None,
    }
}
#[tokio::test]
async fn seal_linearization_keeps_answered_slots_until_handover_and_orders_parallel_fallbacks() {
    use crate::shutdown_seal::HookStage;
    let seal = crate::ShutdownSeal::new();
    let (writer, records) = writer();
    let first = HookStage::PostTool {
        source_index: 0,
        tool_call_id: "z".into(),
    };
    let second = HookStage::PostTool {
        source_index: 1,
        tool_call_id: "a".into(),
    };
    let model = WalRecord::ModelStepFinished {
        tags: None,
        step_id: StepId(1),
        assistant_message: Arc::new(AssistantMessage {
            message_id: broca_protocol::MessageId("m".into()),
            content: vec![ContentBlock::Text {
                text: "model".into(),
            }],
        }),
        usage: Usage::default(),
        retries_used: 0,
        finish_reason: None,
        service_tier_used: None,
        speed_used: None,
        hooks: None,
    };
    assert!(seal.deposit_hook(second.clone(), result_record("a", "fallback-a")));
    assert!(seal.deposit_hook(first.clone(), result_record("z", "fallback-z")));
    assert!(seal.deposit_hook(HookStage::PostAssistant, model.clone()));
    assert!(seal.deposit_hook(first.clone(), result_record("z", "answered-z")));
    seal.seal_and_collect(std::time::Duration::ZERO).await;
    assert!(!seal.record_hook(&first, &writer, result_record("z", "loop-must-not-write")));
    assert!(!seal.deposit_hook(second, result_record("a", "loop-must-not-write")));
    seal.record_hook_fallbacks(&writer).await.unwrap();
    assert_eq!(
        records_with_observed_result_times(&records),
        vec![
            model,
            result_record("z", "answered-z"),
            result_record("a", "fallback-a")
        ]
    );
    let _ = writer.shutdown().await;
}
#[tokio::test]
async fn loop_wins_handover_and_seal_writes_no_second_terminal() {
    use crate::shutdown_seal::HookStage;
    let seal = crate::ShutdownSeal::new();
    let (writer, records) = writer();
    let stage = HookStage::PostTool {
        source_index: 0,
        tool_call_id: "a".into(),
    };
    assert!(seal.deposit_hook(stage.clone(), result_record("a", "fallback")));
    assert!(seal.record_hook(&stage, &writer, result_record("a", "answered")));
    seal.seal_and_collect(std::time::Duration::ZERO).await;
    seal.record_hook_fallbacks(&writer).await.unwrap();
    assert_eq!(
        records_with_observed_result_times(&records),
        vec![result_record("a", "answered")]
    );
    let _ = writer.shutdown().await;
}
#[tokio::test]
async fn lost_model_handover_parks_without_any_write() {
    let seal = crate::ShutdownSeal::new();
    let (writer, records) = writer();
    seal.seal_and_collect(std::time::Duration::ZERO).await;
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        record_model_hook(
            Some(&seal),
            true,
            &writer,
            result_record("a", "must-not-write"),
        ),
    )
    .await;
    assert!(result.is_err());
    assert!(records.lock().unwrap().is_empty());
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn frozen_declarations_must_match_item_count_and_provider_identity() {
    let mut manifest = frozen(vec![vec![sub("post_assistant", None, "pass")]], None, false);
    manifest.declarations.clear();
    assert!(HookPlan::from_manifest(&manifest).is_err());
    let mut manifest = frozen(vec![vec![sub("post_assistant", None, "pass")]], None, false);
    manifest.declarations[0].provider = "different".into();
    assert!(HookPlan::from_manifest(&manifest).is_err());
}

#[tokio::test]
async fn answers_are_checked_against_frozen_declaration_without_redeclaration() {
    let mut manifest = frozen(vec![vec![sub("post_tool", None, "refuse")]], None, false);
    manifest.declarations[0].answer = broca_wal::OpaqueJson::from_json_text(json!({"subscriptions":[{"hook":"post_tool","ops":["prepend"],"on_unavailable":"refuse","budget_ms":500}]}).to_string()).unwrap();
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Answer(append("!")));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let output = batch(
        vec![call("c")],
        &crate::StubToolPlane::new().with_canned_text("edit", "original"),
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert!(output.results[0].is_error);
    assert_eq!(seen.lock().unwrap().len(), 1);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn post_assistant_replace_without_reduction_owner_is_unavailable() {
    let mut subscription = sub("post_assistant", None, "pass");
    subscription["ops"] = json!(["replace"]);
    let manifest = frozen(vec![vec![subscription]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, _) = scripted(|_, _| Reply::Answer(replaced()));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let output = runtime
        .chain(
            Subject::PostAssistant {
                step_id: "1".into(),
                blocks: vec!["original".into()],
            },
            Trace::default(),
            |_| true,
        )
        .await;
    assert_eq!(output.subject.blocks().unwrap(), ["original"]);
    assert_eq!(output.trace.unavailable, ["p0"]);
    let _ = writer.shutdown().await;
}

struct BlockingHooks {
    started: mpsc::UnboundedSender<String>,
}
#[async_trait::async_trait]
impl HookPlane for BlockingHooks {
    async fn hook(
        &self,
        _: &str,
        _: &BindIdentity,
        call: HookCall,
    ) -> Result<HookAnswer, HookUnavailable> {
        let id = match call.subject {
            Subject::PostTool { tool_call_id, .. } => tool_call_id,
            _ => "assistant".into(),
        };
        self.started.send(id).unwrap();
        std::future::pending().await
    }
}
#[tokio::test]
async fn seal_during_post_assistant_preserves_fallback_and_parks_before_dispatch() {
    let manifest = Arc::new(frozen(
        vec![vec![sub("post_assistant", None, "pass")]],
        None,
        false,
    ));
    let (started, mut arrived) = mpsc::unbounded_channel();
    let seal = crate::ShutdownSeal::new();
    let control = RunControl {
        manifest: Some(manifest),
        hook_plane: Some(Arc::new(BlockingHooks { started })),
        shutdown_seal: Some(seal.clone()),
        ..Default::default()
    };
    let (writer, records) = writer();
    let (out, _rx) = mpsc::channel(128);
    let mut req = request();
    let provider = Messages {
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let plane = crate::StubToolPlane::new();
    {
        let run = run_body(
            &mut req,
            crate::replay::replay(&[]).unwrap(),
            None,
            false,
            false,
            false,
            &control,
            &provider,
            &plane,
            &crate::AllowAll,
            &writer,
            || Ok(false),
            None,
            &out,
            None,
        );
        tokio::pin!(run);
        tokio::select! { result = &mut run => panic!("run ended before seal: {result:?}"), id = arrived.recv() => assert_eq!(id.as_deref(), Some("assistant")) }
        seal.seal_and_collect(std::time::Duration::ZERO).await;
        seal.record_hook_fallbacks(&writer).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut run)
                .await
                .is_err()
        );
        let records = records.lock().unwrap().clone();
        assert_eq!(records.iter().filter(|r| matches!(r, WalRecord::ModelStepFinished { hooks:Some(h), .. } if h.unavailable == Some(vec!["p0".into()]))).count(), 1);
        assert!(!records.iter().any(|r| matches!(
            r,
            WalRecord::ToolDispatchIntent { .. }
                | WalRecord::TurnFinished { .. }
                | WalRecord::ToolBatchFinished { .. }
        )));
        assert!(plane.log().is_empty());
    }
    let _ = writer.shutdown().await;
}

async fn sealed_post_tool(
    policies: Vec<Vec<Value>>,
    parallel: bool,
    expected_provider: Option<&str>,
) {
    let manifest = frozen(policies, None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (started, mut arrived) = mpsc::unbounded_channel();
    let hooks: Arc<dyn HookPlane> = Arc::new(BlockingHooks { started });
    let seal = crate::ShutdownSeal::new();
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let mut runtime = runtime(&plan, &hooks, &bind, &writer);
    runtime.seal = Some(&seal);
    let plane = crate::StubToolPlane::new().with_canned_text("edit", "original");
    let mut tool = tool();
    tool.execution_mode = if parallel {
        ExecutionMode::Parallel
    } else {
        ExecutionMode::Sequential
    };
    let tools = ToolManifest::from_tools(vec![tool]);
    let (out, _rx) = mpsc::channel(128);
    let calls = if parallel {
        vec![call("z"), call("a")]
    } else {
        vec![call("a")]
    };
    let count = calls.len();
    {
        let run_id = RunId("r".into());
        let run = dispatch_batch(
            &run_id,
            StepId(1),
            &bind,
            None,
            calls,
            &tools,
            &plane,
            &crate::AllowAll,
            &writer,
            None,
            &[],
            false,
            None,
            Some(&runtime),
            None,
            &out,
            None,
        );
        tokio::pin!(run);
        for _ in 0..count {
            tokio::select! { result = &mut run => panic!("batch ended before seal: {}", result.is_ok()), id = arrived.recv() => assert!(id.is_some()) }
        }
        seal.seal_and_collect(std::time::Duration::ZERO).await;
        seal.record_hook_fallbacks(&writer).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut run)
                .await
                .is_err()
        );
        let records = records.lock().unwrap().clone();
        let terminals: Vec<_> = records
            .iter()
            .filter_map(|r| {
                if let WalRecord::ToolResult { result, reason, .. } = r {
                    Some((result, reason))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(terminals.len(), count);
        for (result, reason) in &terminals {
            if let Some(provider) = expected_provider {
                assert_eq!(reason.as_deref(), Some("post_tool_unavailable"));
                let ToolOutput::Error { message } = &result.output else {
                    panic!("result must be withheld")
                };
                assert!(message.contains(provider));
                assert!(message.contains("call executed"));
            } else {
                assert_eq!(
                    result.output,
                    ToolOutput::Text {
                        text: "original".into()
                    }
                );
            }
        }
        if parallel {
            assert_eq!(
                terminals
                    .iter()
                    .map(|(r, _)| r.tool_call_id.0.as_str())
                    .collect::<Vec<_>>(),
                ["z", "a"]
            );
        }
        assert!(!records.iter().any(|r| matches!(
            r,
            WalRecord::ToolBatchFinished { .. } | WalRecord::TurnFinished { .. }
        )));
    }
    let _ = writer.shutdown().await;
}
#[tokio::test]
async fn seal_during_post_tool_pass_writes_one_fallback() {
    sealed_post_tool(vec![vec![sub("post_tool", None, "pass")]], false, None).await;
}
#[tokio::test]
async fn seal_during_post_tool_refuse_withholds_one_fallback() {
    sealed_post_tool(
        vec![vec![sub("post_tool", None, "refuse")]],
        false,
        Some("p0"),
    )
    .await;
}
#[tokio::test]
async fn seal_during_pass_then_refuse_names_unanswered_refuse_provider() {
    sealed_post_tool(
        vec![
            vec![sub("post_tool", None, "pass")],
            vec![sub("post_tool", None, "refuse")],
        ],
        false,
        Some("p1"),
    )
    .await;
}
#[tokio::test]
async fn parallel_post_tool_seal_writes_two_fallbacks_in_source_order() {
    sealed_post_tool(vec![vec![sub("post_tool", None, "pass")]], true, None).await;
}

#[tokio::test]
async fn first_pre_user_omits_lineage_then_recorded_session_supplies_it() {
    let scratch = scratch("hook-lineage");
    let wal = broca_wal::FileWal::open(scratch.as_path().join("session.wal")).unwrap();
    let writer = WalHandle::spawn(Box::new(wal), 1).unwrap();
    let mut subscription = sub("pre_user", None, "pass");
    subscription["ops"] = json!(["append"]);
    let manifest = frozen(vec![vec![subscription]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Answer(HookAnswer::Pass));
    let bind = BindIdentity::new(scratch.as_path(), "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let subject = Subject::PreUser {
        blocks: vec!["user".into()],
        mark: None,
        delivery: None,
    };
    runtime
        .chain(subject.clone(), Trace::default(), |_| true)
        .await;
    assert_eq!(seen.lock().unwrap()[0].1.lineage_id, None);
    writer.record(WalRecord::ToolBatchStarted {
        batch_id: BatchId("seed".into()),
        step_id: StepId(1),
        planned_calls: vec![],
    });
    writer.sync().await.unwrap();
    runtime.chain(subject, Trace::default(), |_| true).await;
    assert_eq!(
        seen.lock().unwrap()[1].1.lineage_id,
        Some(writer.lineage_id().unwrap().to_string())
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn post_tool_call_key_is_the_durable_intent_key_and_input_render_stays_original() {
    use broca_wal::Wal;
    let scratch = scratch("hook-call-key");
    let path = scratch.as_path().join("session.wal");
    let writer = WalHandle::spawn(
        Box::new(
            broca_wal::FileWal::open(&path)
                .unwrap()
                .with_feature_floor(scratch.join("wal-manifest.json")),
        ),
        1,
    )
    .unwrap();
    let manifest = Arc::new(frozen(
        vec![vec![
            sub("pre_tool", Some("mutate"), "pass"),
            sub("post_tool", None, "pass"),
        ]],
        None,
        false,
    ));
    let (hook_plane, seen) = scripted(|_, call| match &call.subject {
        Subject::PreTool { .. } => {
            assert_eq!(call.subject.tool(), Some("edit"));
            Reply::Answer(HookAnswer::Mutate {
                input: json!({"f":"rewritten"}),
                note: None,
            })
        }
        _ => Reply::Answer(HookAnswer::Pass),
    });
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Messages {
        requests: requests.clone(),
    };
    let plane = crate::StubToolPlane::new().with_tool("edit", |call| {
        assert_eq!(call.input, json!({"f":"rewritten"}));
        crate::StubReply::Output(ToolOutput::Text { text: "ok".into() })
    });
    let control = RunControl {
        manifest: Some(manifest),
        hook_plane: Some(hook_plane),
        ..Default::default()
    };
    let (out, _rx) = mpsc::channel(128);
    run_body(
        &mut request(),
        crate::replay::replay(&[]).unwrap(),
        None,
        false,
        false,
        false,
        &control,
        &provider,
        &plane,
        &crate::AllowAll,
        &writer,
        || Ok(false),
        None,
        &out,
        None,
    )
    .await
    .unwrap();
    let lineage = writer.lineage_id().unwrap().to_string();
    let _ = writer.shutdown().await;
    let records = broca_wal::FileWal::open(path).unwrap().replay().unwrap();
    let intent = records
        .iter()
        .find(|r| matches!(r.record, WalRecord::ToolDispatchIntent { .. }))
        .unwrap();
    let expected = format!("{lineage}:{}", intent.seq);
    assert!(seen.lock().unwrap().iter().any(|(_, call)| matches!(&call.subject, Subject::PostTool { call_key:Some(key), .. } if key == &expected)));
    assert!(seen
        .lock()
        .unwrap()
        .iter()
        .all(|(_, call)| match &call.subject {
            Subject::PreTool { call_key, .. } => call_key.is_none(),
            _ => true,
        }));
    let requests = requests.lock().unwrap();
    let original = requests[1]
        .prompt
        .iter()
        .find_map(|message| {
            message.content.iter().find_map(|block| {
                if let ContentBlock::ToolCall(call) = block {
                    Some(call)
                } else {
                    None
                }
            })
        })
        .unwrap();
    assert_eq!(original.input, json!({"f":"original"}));
    assert_eq!(original.thought_signature.as_deref(), Some("signed"));
}

fn scratch(label: &str) -> std::path::PathBuf {
    let path = broca_test_support::scratch_root().join(label);
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[tokio::test]
async fn matching_tool_coverage_and_phase_call_only_the_selected_subscription() {
    let mut mutate = sub("pre_tool", Some("mutate"), "pass");
    mutate["tools"] = json!(["other"]);
    let mut validate = sub("pre_tool", Some("validate"), "pass");
    validate["tools"] = json!(["edit"]);
    let manifest = frozen(vec![vec![mutate, validate]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Answer(HookAnswer::Pass));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    batch(
        vec![call("c")],
        &crate::StubToolPlane::new().with_canned_text("edit", "ok"),
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(
        seen.lock().unwrap()[0].1.subject.phase(),
        Some(Phase::Validate)
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn answers_must_obey_the_subscription_operation_and_subject_kind() {
    for answer in [
        HookAnswer::Ops {
            ops: vec![Operation::Prepend {
                block: 0,
                text: "not subscribed".into(),
                note: None,
            }],
        },
        HookAnswer::Mutate {
            input: json!({"bad":"kind"}),
            note: None,
        },
    ] {
        let mut subscription = sub("post_assistant", None, "pass");
        subscription["ops"] = json!(["append"]);
        let manifest = frozen(vec![vec![subscription]], None, false);
        let plan = HookPlan::from_manifest(&manifest).unwrap();
        let (hooks, _) = scripted(move |_, _| Reply::Answer(answer.clone()));
        let (writer, _) = writer();
        let bind = BindIdentity::new("/project", "caller", "s");
        let runtime = runtime(&plan, &hooks, &bind, &writer);
        let output = runtime
            .chain(
                Subject::PostAssistant {
                    step_id: "1".into(),
                    blocks: vec!["original".into()],
                },
                Trace::default(),
                |_| true,
            )
            .await;
        assert_eq!(output.subject.blocks().unwrap(), ["original"]);
        assert_eq!(output.trace.unavailable, ["p0"]);
        let _ = writer.shutdown().await;
    }
}

#[tokio::test]
async fn pre_user_seal_cancellation_returns_discardable_output_instead_of_parking() {
    let mut subscription = sub("pre_user", None, "pass");
    subscription["ops"] = json!(["append"]);
    let manifest = frozen(vec![vec![subscription]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Never);
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let seal = crate::ShutdownSeal::new();
    seal.seal_and_collect(std::time::Duration::ZERO).await;
    let mut runtime = runtime(&plan, &hooks, &bind, &writer);
    runtime.seal = Some(&seal);
    let mut chain = Box::pin(runtime.chain(
        Subject::PreUser {
            blocks: vec!["original".into()],
            mark: None,
            delivery: None,
        },
        Trace::default(),
        |_| true,
    ));
    let std::task::Poll::Ready(output) = futures::poll!(chain.as_mut()) else {
        panic!("a send cancellation must not park its actor");
    };
    assert!(output.cancellation.is_some());
    assert!(output.trace.unavailable.is_empty());
    assert!(seen.lock().unwrap().is_empty());
    assert!(records.lock().unwrap().is_empty());
    drop(chain);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn input_hook_cancel_is_not_unavailability_and_never_applies_refuse_policy() {
    let manifest = frozen(
        vec![vec![sub("pre_tool", Some("mutate"), "refuse")]],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let (hooks, _) = scripted(move |_, _| Reply::Cancel(token.clone()));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let mut runtime = runtime(&plan, &hooks, &bind, &writer);
    runtime.cancel = Some(&cancel);
    let output = runtime
        .chain(
            pre_tool_subject(StepId(1), &call("c"), Phase::Mutate),
            Trace::default(),
            |_| true,
        )
        .await;
    assert!(output.cancellation.is_some());
    assert!(output.refusal.is_none());
    assert!(output.trace.unavailable.is_empty());
    let _ = writer.shutdown().await;
}

struct DropTracked {
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait::async_trait]
impl HookPlane for DropTracked {
    async fn hook(
        &self,
        _: &str,
        _: &BindIdentity,
        _: HookCall,
    ) -> Result<HookAnswer, HookUnavailable> {
        struct OnDrop(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let _guard = OnDrop(self.dropped.clone());
        std::future::pending().await
    }
}
#[tokio::test]
async fn seal_cancels_pending_hook_immediately_and_parks_without_an_input_slot() {
    let manifest = frozen(
        vec![vec![sub("pre_tool", Some("mutate"), "pass")]],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hooks: Arc<dyn HookPlane> = Arc::new(DropTracked {
        dropped: dropped.clone(),
    });
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let seal = crate::ShutdownSeal::new();
    let mut runtime = runtime(&plan, &hooks, &bind, &writer);
    runtime.seal = Some(&seal);
    let mut chain = Box::pin(runtime.chain(
        pre_tool_subject(StepId(1), &call("c"), Phase::Mutate),
        Trace::default(),
        |_| true,
    ));
    assert!(futures::poll!(chain.as_mut()).is_pending());
    assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
    seal.seal_and_collect(std::time::Duration::ZERO).await;
    assert!(futures::poll!(chain.as_mut()).is_pending());
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    assert!(records.lock().unwrap().is_empty());
    drop(chain);
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn pre_tool_stop_during_hook_writes_no_intent_and_finishes_batch() {
    let manifest = frozen(
        vec![vec![sub("pre_tool", Some("mutate"), "pass")]],
        None,
        false,
    );
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let (hooks, _) = scripted(move |_, _| Reply::Cancel(token.clone()));
    let (writer, records) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let mut runtime = runtime(&plan, &hooks, &bind, &writer);
    runtime.stop = Some(&stop);
    let plane = crate::StubToolPlane::new();
    let result = batch(
        vec![call("c")],
        &plane,
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert_eq!(result.interruption, Some(BatchInterruption::StopRequested));
    assert!(!records
        .lock()
        .unwrap()
        .iter()
        .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. })));
    assert!(matches!(
        plane.log().last(),
        Some(crate::StubEvent::BatchFinished)
    ));
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn post_tool_subscriber_is_not_called_for_other_subjects() {
    let manifest = frozen(vec![vec![sub("post_tool", None, "pass")]], None, false);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|_, _| Reply::Answer(append("wrong hook")));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    for subject in [
        Subject::PreUser {
            blocks: vec!["user".into()],
            mark: None,
            delivery: None,
        },
        Subject::PostAssistant {
            step_id: "1".into(),
            blocks: vec!["assistant".into()],
        },
    ] {
        runtime.chain(subject, Trace::default(), |_| true).await;
        assert!(seen.lock().unwrap().is_empty());
    }
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn frozen_approve_admits_refuse_and_rejects_pass_on_resume() {
    let manifest = frozen(
        vec![vec![sub("pre_tool", Some("approve"), "refuse")]],
        None,
        false,
    );
    assert!(HookPlan::from_manifest(&manifest).unwrap().approves());
    let unsafe_manifest = frozen(
        vec![vec![sub("pre_tool", Some("approve"), "pass")]],
        None,
        false,
    );
    assert!(
        matches!(HookPlan::from_manifest(&unsafe_manifest), Err(error) if error == "approve_on_unavailable_pass")
    );
}

#[tokio::test]
async fn providers_apply_in_frozen_order_not_lexical_order() {
    let mut manifest = frozen(
        vec![
            vec![sub("post_assistant", None, "pass")],
            vec![sub("post_assistant", None, "pass")],
        ],
        None,
        false,
    );
    let mut plan_value: Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    for (index, provider) in ["z", "a"].into_iter().enumerate() {
        plan_value["step_transform_items"][index]["provider"] = provider.into();
        manifest.declarations[index].provider = provider.into();
    }
    manifest.plan = broca_wal::OpaqueJson::from_json_text(plan_value.to_string()).unwrap();
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, seen) = scripted(|provider, _| Reply::Answer(append(provider)));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let output = runtime
        .chain(
            Subject::PostAssistant {
                step_id: "1".into(),
                blocks: vec!["base".into()],
            },
            Trace::default(),
            |_| true,
        )
        .await;
    assert_eq!(output.subject.blocks().unwrap(), ["baseza"]);
    assert_eq!(
        seen.lock()
            .unwrap()
            .iter()
            .map(|(provider, _)| provider.as_str())
            .collect::<Vec<_>>(),
        ["z", "a"]
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn catalog_permissions_are_scoped_to_the_selected_tool_owner() {
    let mut manifest = frozen(
        vec![vec![sub("post_tool", None, "refuse")]],
        Some(vec![]),
        false,
    );
    let mut other = manifest.fetches[0].clone();
    other.provider = "unused-provider".into();
    if let broca_wal::FetchOutcome::Answered { result_ops, .. } = &mut other.outcome {
        *result_ops = Some([("edit".into(), vec!["append".into(), "replace".into()])].into());
    }
    manifest.fetches.push(other);
    let plan = HookPlan::from_manifest(&manifest).unwrap();
    let (hooks, _) = scripted(|_, _| Reply::Answer(append("!")));
    let (writer, _) = writer();
    let bind = BindIdentity::new("/project", "caller", "s");
    let runtime = runtime(&plan, &hooks, &bind, &writer);
    let result = batch(
        vec![call("c")],
        &crate::StubToolPlane::new().with_canned_text("edit", "original"),
        &crate::AllowAll,
        &writer,
        &runtime,
        None,
    )
    .await;
    assert!(result.results[0].is_error);
    let _ = writer.shutdown().await;
}

#[test]
fn text_splicing_counts_only_text_and_preserves_signed_non_text_bytes() {
    let original = vec![
        ContentBlock::Reasoning {
            text: "reason".into(),
            signature: Some("sig".into()),
        },
        ContentBlock::Text {
            text: "first".into(),
        },
        ContentBlock::RedactedReasoning {
            data: "opaque".into(),
            provider_format: None,
        },
        ContentBlock::ToolCall(call("signed")),
        ContentBlock::Text {
            text: "last".into(),
        },
    ];
    let mut content = original.clone();
    crate::hooks::apply_message_ops(
        &mut content,
        &[
            Operation::Append {
                block: 0,
                text: "+one".into(),
                note: None,
            },
            Operation::Append {
                block: 1,
                text: "+two".into(),
                note: None,
            },
        ],
    )
    .unwrap();
    for index in [0, 2, 3] {
        assert_eq!(
            serde_json::to_vec(&content[index]).unwrap(),
            serde_json::to_vec(&original[index]).unwrap(),
            "signed non-text part {index} changed"
        );
    }
    assert_eq!(
        content[1],
        ContentBlock::Text {
            text: "first+one".into()
        }
    );
    assert_eq!(
        content[4],
        ContentBlock::Text {
            text: "last+two".into()
        }
    );
}

#[test]
fn text_splicing_removes_state_only_from_rewritten_provider_text() {
    let block: ContentBlock = serde_json::from_value(json!({"type":"text_with_state","text":"answer","provider_state":{"format":"gemini","data":"opaque"}})).unwrap();
    let mut content = vec![block.clone(), block.clone()];
    crate::hooks::apply_message_ops(
        &mut content,
        &[Operation::Append {
            block: 0,
            text: " edited".into(),
            note: None,
        }],
    )
    .unwrap();
    assert_eq!(
        content[0],
        ContentBlock::Text {
            text: "answer edited".into()
        }
    );
    assert_eq!(content[1], block);
}

async fn pre_tool_terminal_cause(caller: bool) {
    let manifest = Arc::new(frozen(
        vec![vec![sub("pre_tool", Some("mutate"), "pass")]],
        None,
        false,
    ));
    let signal = CancellationToken::new();
    let token = signal.clone();
    let (hooks, _) = scripted(move |_, _| Reply::Cancel(token.clone()));
    let mut control = RunControl {
        manifest: Some(manifest),
        hook_plane: Some(hooks),
        ..Default::default()
    };
    if caller {
        control.cancel = Some(signal);
    } else {
        control.stop_requested = Some(signal);
    }
    let (writer, records) = writer();
    let plane = crate::StubToolPlane::new();
    let (out, _rx) = mpsc::channel(128);
    let summary = run_body(
        &mut request(),
        crate::replay::replay(&[]).unwrap(),
        None,
        false,
        false,
        false,
        &control,
        &Messages {
            requests: Arc::new(Mutex::new(Vec::new())),
        },
        &plane,
        &crate::AllowAll,
        &writer,
        || Ok(false),
        None,
        &out,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        summary.reason,
        if caller {
            RunFinishReason::Cancelled
        } else {
            RunFinishReason::Interrupted
        }
    );
    assert!(!records
        .lock()
        .unwrap()
        .iter()
        .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. })));
    assert!(matches!(
        plane.log().last(),
        Some(crate::StubEvent::BatchFinished)
    ));
    let _ = writer.shutdown().await;
}
#[tokio::test]
async fn caller_cancel_during_pre_tool_finishes_cancelled() {
    pre_tool_terminal_cause(true).await;
}
#[tokio::test]
async fn stop_during_pre_tool_finishes_interrupted() {
    pre_tool_terminal_cause(false).await;
}

#[tokio::test]
async fn lost_dispatch_intent_stays_interrupted_even_when_caller_cancel_is_ready() {
    let manifest = Arc::new(frozen(
        vec![vec![
            sub("pre_tool", Some("mutate"), "pass"),
            sub("post_tool", None, "pass"),
        ]],
        None,
        false,
    ));
    let (seed_hooks, _) = scripted(|_, _| Reply::Answer(HookAnswer::Pass));
    let control = RunControl {
        manifest: Some(manifest),
        hook_plane: Some(seed_hooks),
        ..Default::default()
    };
    let (seed_writer, seed) = writer();
    let (out, _rx) = mpsc::channel(128);
    run_body(
        &mut request(),
        crate::replay::replay(&[]).unwrap(),
        None,
        false,
        false,
        false,
        &control,
        &Messages {
            requests: Arc::new(Mutex::new(Vec::new())),
        },
        &crate::StubToolPlane::new().with_canned_text("edit", "ok"),
        &crate::AllowAll,
        &seed_writer,
        || Ok(false),
        None,
        &out,
        None,
    )
    .await
    .unwrap();
    let _ = seed_writer.shutdown().await;
    let mut prefix = Vec::new();
    for record in seed.lock().unwrap().iter() {
        prefix.push(record.clone());
        if matches!(record, WalRecord::ToolDispatchIntent { .. }) {
            break;
        }
    }
    let mut prior = crate::replay::replay(&prefix).unwrap();
    prior.manifest = prefix.iter().find_map(|record| {
        if let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = record
        {
            Some(Arc::new(manifest.as_ref().clone()))
        } else {
            None
        }
    });
    let (hooks, seen) = scripted(|_, _| Reply::Never);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let control = RunControl {
        hook_plane: Some(hooks),
        cancel: Some(cancel),
        ..Default::default()
    };
    let (writer, records) = writer();
    let plane = crate::StubToolPlane::new();
    let summary = run_body(
        &mut request(),
        prior,
        None,
        true,
        false,
        false,
        &control,
        &Messages {
            requests: Arc::new(Mutex::new(Vec::new())),
        },
        &plane,
        &crate::AllowAll,
        &writer,
        || Ok(false),
        None,
        &out,
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert!(seen.lock().unwrap().is_empty());
    assert!(plane.log().is_empty());
    assert!(records.lock().unwrap().iter().any(|r| matches!(
        r,
        WalRecord::ToolResult {
            outcome: DispatchOutcome::Unknown,
            ..
        }
    )));
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn lost_post_tool_handover_parks_before_batch_or_turn_markers() {
    use crate::shutdown_seal::HookStage;
    let seal = crate::ShutdownSeal::new();
    let (writer, records) = writer();
    let stage = HookStage::PostTool {
        source_index: 0,
        tool_call_id: "c".into(),
    };
    assert!(seal.deposit_hook(stage.clone(), result_record("c", "fallback")));
    seal.seal_and_collect(std::time::Duration::ZERO).await;
    let result = tokio::time::timeout(std::time::Duration::from_millis(20), async {
        record_hook_stage(
            &seal,
            &stage,
            &writer,
            result_record("c", "loop must not write"),
        )
        .await;
        writer.record(WalRecord::ToolBatchFinished {
            batch_id: BatchId("b".into()),
        });
        writer.record(WalRecord::TurnFinished { step_id: StepId(1) });
    })
    .await;
    assert!(result.is_err());
    assert!(records.lock().unwrap().is_empty());
    seal.record_hook_fallbacks(&writer).await.unwrap();
    assert_eq!(
        records_with_observed_result_times(&records),
        vec![result_record("c", "fallback")]
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn actor_input_hooks_are_rendered_and_recorded_without_final_metadata() {
    let mut req = request();
    let original = json!(req.input);
    let mut final_input = req.input.clone();
    final_input[0].content = vec![ContentBlock::Text {
        text: "actor-final".into(),
    }];
    let metadata = broca_wal::Hooks {
        original: Some(original.clone()),
        final_value: Some(json!(final_input)),
        ops: vec![broca_wal::HookOp {
            module: "input-provider".into(),
            hook: "pre_user".into(),
            phase: None,
            op: "append".into(),
            block: Some(0),
            note: None,
        }],
        unavailable: Some(vec!["passed-provider".into()]),
    };
    let control = RunControl {
        input_hooks: Some(Box::new(metadata.clone())),
        ..Default::default()
    };
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Messages {
        requests: requests.clone(),
    };
    let (writer, records) = writer();
    let (out, _events) = mpsc::channel(128);
    run_body(
        &mut req,
        crate::replay::replay(&[]).unwrap(),
        None,
        false,
        false,
        false,
        &control,
        &provider,
        &crate::StubToolPlane::new().with_canned_text("edit", "ok"),
        &crate::AllowAll,
        &writer,
        || Ok(false),
        None,
        &out,
        None,
    )
    .await
    .unwrap();
    let records = records.lock().unwrap().clone();
    let WalRecord::RunStarted {
        input,
        hooks: Some(hooks),
        ..
    } = &records[0]
    else {
        panic!("input metadata missing")
    };
    assert_eq!(input.as_ref(), &final_input);
    assert_eq!(hooks.original, Some(original));
    assert_eq!(hooks.ops, metadata.ops);
    assert_eq!(hooks.unavailable, metadata.unavailable);
    assert_eq!(hooks.final_value, None);
    assert_eq!(
        requests.lock().unwrap()[0].prompt[0].content,
        final_input[0].content
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn actor_input_refusal_is_durable_before_any_provider_request() {
    let error: ProviderError = serde_json::from_value(json!({"class":"permanent", "provider_code":"pre_user_unavailable", "message":"unavailable: p0"})).unwrap();
    let control = RunControl {
        pre_user_error: Some(error.clone()),
        ..Default::default()
    };
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Messages {
        requests: requests.clone(),
    };
    let (writer, records) = writer();
    let (out, _events) = mpsc::channel(128);
    let summary = run_body(
        &mut request(),
        crate::replay::replay(&[]).unwrap(),
        None,
        false,
        false,
        false,
        &control,
        &provider,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &writer,
        || Ok(false),
        None,
        &out,
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.reason, RunFinishReason::Error);
    assert!(requests.lock().unwrap().is_empty());
    let records = records.lock().unwrap().clone();
    assert_eq!(records.len(), 2);
    assert!(matches!(records[0], WalRecord::RunStarted { .. }));
    assert!(
        matches!(&records[1], WalRecord::RunFinished { error: Some(recorded), .. } if recorded == &error)
    );
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn cut_step_shutdown_fallback_keeps_exact_rejection_counts() {
    let manifest = Arc::new(frozen(
        vec![vec![sub("post_assistant", None, "pass")]],
        None,
        false,
    ));
    let (started, mut arrived) = mpsc::unbounded_channel();
    let seal = crate::ShutdownSeal::new();
    let control = RunControl {
        manifest: Some(manifest),
        hook_plane: Some(Arc::new(BlockingHooks { started })),
        shutdown_seal: Some(seal.clone()),
        ..Default::default()
    };
    let (writer, records) = writer();
    let (out, _rx) = mpsc::channel(128);
    let mut req = request();
    let provider = crate::StubProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage {
            cut: broca_protocol::provider::ToolArgsTrailingGarbage {
                tool_call_id: ToolCallId("c".into()),
                tool_name: "edit".into(),
                json_bytes: 9,
                whitespace_bytes: 4097,
                omitted_tool_calls: 0,
            },
        }),
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: broca_protocol::MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(call("c"))],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]]);
    let plane = crate::StubToolPlane::new();
    {
        let run = run_body(
            &mut req,
            crate::replay::replay(&[]).unwrap(),
            None,
            false,
            false,
            false,
            &control,
            &provider,
            &plane,
            &crate::AllowAll,
            &writer,
            || Ok(false),
            None,
            &out,
            None,
        );
        tokio::pin!(run);
        tokio::select! { result = &mut run => panic!("run ended before seal: {result:?}"), id = arrived.recv() => assert_eq!(id.as_deref(), Some("assistant")) }
        seal.seal_and_collect(std::time::Duration::ZERO).await;
        seal.record_hook_fallbacks(&writer).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut run)
                .await
                .is_err()
        );
        let records = records.lock().unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r, WalRecord::ModelStepFinished { .. }))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r, WalRecord::ToolBatchStarted { .. }))
                .count(),
            1
        );
        assert!(records.iter().any(|r| matches!(r, WalRecord::ToolResult { result, .. } if matches!(&result.output,
            ToolOutput::Error { message } if message.starts_with("tool_args_trailing_garbage:") && message.contains("json_bytes=9, whitespace_bytes=4097")))));
        assert!(!records
            .iter()
            .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. })));
        assert!(plane.log().is_empty());
    }
    let _ = writer.shutdown().await;
}

#[path = "dispatch_cancel_tests.rs"]
mod dispatch_cancel_tests;

#[tokio::test]
async fn post_hook_restream_counts_delta_less_stored_blocks() {
    let message = AssistantMessage {
        message_id: broca_protocol::MessageId("hooked".into()),
        content: vec![
            ContentBlock::RedactedReasoning {
                data: "opaque".into(),
                provider_format: None,
            },
            ContentBlock::Reasoning {
                text: "thinking".into(),
                signature: None,
            },
            ContentBlock::Text {
                text: "answer".into(),
            },
        ],
    };
    let (out, mut events) = mpsc::channel(8);
    restream_message(&message, &out).await;
    assert_eq!(
        events.try_recv().unwrap(),
        OutputEvent::Display(DisplayEvent::DisplayReset)
    );
    let parts: Vec<_> = std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|e| match e {
            OutputEvent::Display(e) => e.coordinates().map(|(_, part)| part),
            _ => None,
        })
        .collect();
    assert_eq!(parts, vec![1, 2]);
}
