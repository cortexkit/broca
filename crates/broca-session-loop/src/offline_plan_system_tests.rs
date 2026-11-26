//! A step rebuilt offline (for a recorded or manual cache warm) renders the system text the live
//! step sent. Once a plan refresh applied, the live step shows the applied plan's system text in
//! place of the session's original system message; a rebuild that kept the original would render
//! another request than the one whose digest was recorded, and warm the wrong prefix.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::stub_plane::StubToolPlane;
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, Message, Role};
use broca_protocol::request::CallOptions;
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, StepId, SubmissionRequest,
    Usage,
};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::{Record, SessionRefresh};
use broca_session_store::session::Session;
use tokio::sync::mpsc;

use crate::parts::MemoryParts;
use crate::run::Seams;
use crate::writer::Writer;

const ORIGINAL: &str = "you are a worker";
const REFRESHED: &str = "you are a reviewer";

/// Answers every step with a final text, keeping the prompt each step was sent.
#[derive(Default)]
struct Recording {
    prompts: Mutex<Vec<Vec<Message>>>,
}

#[async_trait]
impl broca_core::provider::Provider for Recording {
    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.prompts.lock().unwrap().push(opts.prompt.clone());
        let (tx, rx) = mpsc::channel(8);
        tx.try_send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m-answer".into()),
                    content: vec![ContentBlock::Text {
                        text: "Done.".into(),
                    }],
                },
            },
        ))
        .unwrap();
        tx.try_send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(10),
                output_tokens: Some(1),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }))
        .unwrap();
        rx
    }
}

fn system_text(message: &Message) -> (Role, String) {
    let text = message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.to_string()),
            _ => None,
        })
        .collect();
    (message.role, text)
}

#[tokio::test]
async fn a_rebuilt_step_renders_the_applied_plans_system_text_as_sent() {
    let backend = MemoryBackend::default();
    let bind = BindIdentity::new("/synthetic", "offline", "s1");
    let mut session = Session::create(
        &backend,
        SessionAddr("0ff1c0ff1c0ff1c0".into()),
        SessionTriple {
            project_root: "/synthetic".into(),
            harness: "offline".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]).creating(Cipher::None),
        false,
    )
    .await
    .unwrap();
    // A refresh accepted before the run, which the run's start applies.
    session
        .append(
            &[Record::SessionRefresh(SessionRefresh {
                generation: 1,
                policy: "immediately".into(),
                run_in_progress: false,
                applies_from_next_run: true,
                setup: Vec::new(),
                manifest: None,
                selection: None,
                refetch: Vec::new(),
                system_note: None,
                accepted_at_ms: None,
            })],
            true,
        )
        .await
        .unwrap();
    let manifest: broca_records::record::PlanManifest = serde_json::from_value(serde_json::json!({
        "plan": "{}", "plan_identity": "plan-2", "composition_digest": "composition",
        "fetches": [], "tools": [], "system_text": REFRESHED, "system_digest": "system",
        "join_version": 1, "role_versions": {},
    }))
    .unwrap();
    let provider = Recording::default();
    let request: SubmissionRequest = serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [
            {"role": "system", "content": [{"type": "text", "text": ORIGINAL}]},
            {"role": "user", "content": [{"type": "text", "text": "review it"}]},
        ],
    }))
    .unwrap();
    let mut request = broca_core::RunRequest::from_submission(
        request,
        RunId("offline".into()),
        bind.clone(),
        broca_protocol::SubmissionId(String::new()),
    );
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        manifest: Some(Arc::new(manifest)),
        applies_refresh: Some(broca_protocol::session_change::AppliedChange {
            generation: 1,
            applied_by: "append".into(),
            manifest_digest: "manifest".into(),
            composition_digest: None,
            absent_items: Vec::new(),
            reason: None,
        }),
        ..Default::default()
    };
    let now = || 1_800_000_000_000;
    let admission = crate::admit::admission(
        &mut request,
        &control,
        &provider,
        Writer::operations(&session),
        Some(now()),
    )
    .unwrap();
    let tools = StubToolPlane::new();
    let seams = Seams {
        steer_yields: None,
        provider: &provider,
        tools: &tools,
        bind: bind.clone(),
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
    let mut parts = MemoryParts::default();
    crate::run::run(&mut session, &mut parts, admission, &seams)
        .await
        .unwrap();
    assert_eq!(
        Writer::operations(&session)
            .plan
            .as_ref()
            .map(|plan| plan.applied_generation),
        Some(1),
        "the run's start applied the refresh"
    );
    let sent = provider.prompts.lock().unwrap()[0][0].clone();
    assert_eq!(
        system_text(&sent),
        (Role::System, REFRESHED.to_owned()),
        "the live step shows the applied plan's system text"
    );
    let rebuilt = crate::run::offline_request(&session, &parts, &bind, 1)
        .await
        .unwrap();
    assert_eq!(
        system_text(&rebuilt.prompt[0]),
        system_text(&sent),
        "the rebuilt step renders the system text the live step sent"
    );
}
