//! A versioned tool on a live session (llm-runner `tool_versions`): a run at a prefix rebuild
//! takes the tool's same-major definition its provider now serves, and any other run keeps the
//! one its session froze; a call its provider refuses for a new major records the refusal's
//! code and detail on its outcome.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::stub_plane::StubToolPlane;
use broca_core::tool_plane::{BatchCtx, CompatibleTool, DispatchResult, ToolPlane, ToolPlaneError};
use broca_core::ToolManifest;
use broca_protocol::content::{
    AssistantMessage, ContentBlock, FinishReason, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::request::{CallOptions, Tool};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, StepId, SubmissionRequest,
    ToolCallId, Usage,
};
use broca_records::{
    record::DispatchSchemaPin, record::FetchOutcome, record::FrozenProviderDeclarations,
    record::PlanManifest,
};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use serde_json::json;
use tokio::sync::mpsc;

use crate::parts::MemoryParts;
use crate::run::Seams;

const FROZEN: &str = "Run a command.";
const CURRENT: &str = "Run a command, with a timeout.";

fn finished(content: Vec<ContentBlock>, finish_reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("m-{}", content.len())),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(10),
                output_tokens: Some(1),
                ..Usage::default()
            },
            finish_reason,
            retries_used: 0,
        }),
    ]
}

/// One step that calls `bash`, then a final answer. Records the description of `bash` each
/// request offered.
struct Scripted {
    steps: Mutex<VecDeque<Vec<NormalizedEvent>>>,
    offered: Mutex<Vec<String>>,
}

#[async_trait]
impl broca_core::provider::Provider for Scripted {
    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        if let Some(bash) = opts.tools.iter().find(|tool| tool.name == "bash") {
            self.offered.lock().unwrap().push(bash.description.clone());
        }
        let step = self.steps.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(8);
        for event in step {
            tx.try_send(event).unwrap();
        }
        rx
    }
}

/// Lists `bash` as compatible at its current definition, and answers or refuses its call.
struct Tools {
    inner: StubToolPlane,
    refuse: bool,
}

#[async_trait]
impl ToolPlane for Tools {
    async fn compatible_tools(
        &self,
        _bind: &broca_protocol::BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        _manifest: &PlanManifest,
    ) -> Vec<CompatibleTool> {
        vec![CompatibleTool {
            provider: "aft".into(),
            tool: bash(CURRENT),
            pin: pin('b'),
        }]
    }

    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.inner.batch_started(ctx, calls, manifest).await
    }

    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        if !self.refuse {
            return self.inner.dispatch(ctx, call).await;
        }
        DispatchResult::Refused {
            result: ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::error("tool/route rejected: tool_major_changed"),
                is_error: true,
            },
            code: "tool_major_changed".into(),
            detail: json!({"provider": "aft", "refusal": {"tool": "bash"}}),
        }
    }

    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.inner.batch_finished(ctx).await
    }
}

fn bash(description: &str) -> Tool {
    let mut tool: Tool = serde_json::from_value(json!({
        "name": "bash",
        "description": description,
        "input_schema": {"type": "object"},
    }))
    .unwrap();
    tool.module = Some("aft".into());
    tool
}

fn pin(digest: char) -> DispatchSchemaPin {
    DispatchSchemaPin {
        schema_digest: digest.to_string().repeat(64),
        semantics: 1,
        major: Some(2),
    }
}

fn manifest() -> PlanManifest {
    let tools = Arc::new(vec![bash(FROZEN)]);
    PlanManifest {
        rich_ui: None,
        plan: broca_records::record::OpaqueJson::from_json_text(
            json!({"tool_items": [{"provider": "aft"}]}).to_string(),
        )
        .unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: "composition".into(),
        fetches: vec![broca_records::record::ProviderFetch {
            provider: "aft".into(),
            outcome: FetchOutcome::Answered {
                catalog_digest: "one".into(),
                tools: Arc::clone(&tools),
                text: None,
                result_ops: None,
                reply: None,
                declarations_frozen: Some(Box::new(FrozenProviderDeclarations {
                    schema_pins: Some([("bash".into(), pin('a'))].into()),
                    ..Default::default()
                })),
                reference_slots: None,
            },
        }],
        tools,
        system_text: String::new(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        declarations: Vec::new(),
    }
}

/// Runs one `bash` call and a final answer. Returns what each request offered as `bash`'s
/// description, and the session's records.
async fn run(rebuild_point: bool, refuse: bool) -> (Vec<String>, Vec<Record>) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let bind = BindIdentity::new("/synthetic", "versions", "s1");
    let triple = SessionTriple {
        project_root: "/synthetic".into(),
        harness: "versions".into(),
        session: "s1".into(),
    };
    let mut session = Session::create(
        backend,
        SessionAddr("f00df00df00df00d".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]).creating(Cipher::None),
        false,
    )
    .await
    .unwrap();
    let call = ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        provider_state: None,
        tool_call_id: ToolCallId("bash-1".into()),
        tool_name: "bash".into(),
        input: json!({}).into(),
    });
    let provider = Scripted {
        steps: Mutex::new(VecDeque::from([
            finished(vec![call], FinishReason::ToolCalls),
            finished(
                vec![ContentBlock::Text {
                    text: "Done.".into(),
                }],
                FinishReason::Stop,
            ),
        ])),
        offered: Mutex::default(),
    };
    let submission: SubmissionRequest = serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": "run it"}]}],
    }))
    .unwrap();
    let mut request = broca_core::RunRequest::from_submission(
        submission,
        RunId("versions".into()),
        bind.clone(),
        broca_protocol::SubmissionId(String::new()),
    );
    request.tools = vec![bash(FROZEN)];
    request.policy_allowed = ["bash".to_owned()].into();
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        manifest: Some(Arc::new(manifest())),
        rebuild_point,
        ..Default::default()
    };
    let now = || 1_800_000_000_000;
    let admission = crate::admit::admission(
        &mut request,
        &control,
        &provider,
        crate::writer::Writer::operations(&session),
        Some(now()),
    )
    .unwrap();
    let tools = Tools {
        inner: StubToolPlane::new().with_canned_text("bash", "ran"),
        refuse,
    };
    let seams = Seams {
        steer_yields: None,
        provider: &provider,
        tools: &tools,
        bind,
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        approval: &broca_core::capability::AllowAll,
        hook_plane: None,
        now_ms: &now,
        session_address: None,
        compaction_plane: None,
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        accepting_drain: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    crate::run::run(&mut session, &mut MemoryParts::default(), admission, &seams)
        .await
        .unwrap();
    let records = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .map(|(_, record)| record)
        .collect();
    let offered = provider.offered.lock().unwrap().clone();
    (offered, records)
}

#[tokio::test]
async fn a_run_at_a_prefix_rebuild_offers_the_same_major_definition_its_provider_serves() {
    let (offered, _) = run(true, false).await;
    assert_eq!(offered, [CURRENT, CURRENT]);
}

#[tokio::test]
async fn any_other_run_keeps_the_definition_its_session_froze() {
    let (offered, _) = run(false, false).await;
    assert_eq!(offered, [FROZEN, FROZEN]);
}

#[tokio::test]
async fn a_major_change_refusal_is_recorded_by_code_and_detail_on_the_calls_outcome() {
    let (_, records) = run(false, true).await;
    let outcome = records
        .iter()
        .find_map(|record| match record {
            Record::ToolOutcome(outcome) => Some(outcome),
            _ => None,
        })
        .expect("the refused call has an outcome");
    assert!(outcome.is_error);
    assert_eq!(outcome.reason.as_deref(), Some("tool_major_changed"));
    assert_eq!(
        outcome.detail.as_ref().map(|detail| detail.parse()),
        Some(json!({"provider": "aft", "refusal": {"tool": "bash"}}))
    );
}
