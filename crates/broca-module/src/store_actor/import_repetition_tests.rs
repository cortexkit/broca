use super::*;
use broca_protocol::{Message, Role, ToolOutput};

fn repeated_history() -> Vec<Message> {
    let mut messages = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "Imported prompt.".into(),
        }],
    )];
    for index in 0..8 {
        let id = format!("call-{index}");
        messages.push(serde_json::from_value(serde_json::json!({"role":"assistant","content":[{
            "type":"tool_call","tool_call_id":id,"tool_name":"read","input":{"path":"fixture.txt"}
        }]})).unwrap());
        messages.push(serde_json::from_value(serde_json::json!({"role":"tool","content":[{
            "type":"tool_result","tool_call_id":id,"output":{"kind":"text","text":""},"is_error":false
        }]})).unwrap());
    }
    messages
}

async fn run_import(
    legacy: bool,
) -> (
    Session<&'static MemoryBackend>,
    MemoryParts,
    broca_protocol::CallOptions,
    Vec<u8>,
) {
    use broca_provider::wire::WireFamily;
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let (commands, served) = serve(create(backend).await.unwrap());
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Import {
            request: Box::new(broca_core::ImportRequest {
                project_id: None,
                principal: None,
                run_id: broca_protocol::RunId("seed".into()),
                session: bind(),
                messages: repeated_history(),
                origin: broca_core::ImportOrigin {
                    source_ref: "synthetic:repeated-tools".into(),
                },
            }),
            reply,
        })
        .unwrap();
    assert!(matches!(
        answer.await.unwrap(),
        crate::actor::ImportOutcome::Imported(_)
    ));
    drop(commands);
    let mut session = served.await.unwrap();
    let provider = broca_core::provider::StubProvider::new(vec![text_step("m-next", "ok")]);
    let mut req = broca_core::RunRequest::from_submission(
        request("next"),
        broca_protocol::RunId("r-next".into()),
        bind(),
        broca_protocol::SubmissionId(String::new()),
    );
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        ..Default::default()
    };
    let mut admission = broca_session_loop::admit::admission(
        &mut req,
        &control,
        &provider,
        session.operations(),
        Some(10_000),
    )
    .unwrap();
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-sonnet-4-5");
    let mut blob = serde_json::to_value(&frozen).unwrap();
    if !legacy {
        blob["repetition_after_input"] = serde_json::json!(true);
    }
    admission.render_config = Arc::new(blob.clone());
    frozen = serde_json::from_value(blob).unwrap();
    let tools = broca_core::stub_plane::StubToolPlane::new();
    let now = || 10_000;
    let seams = broca_session_loop::run::Seams {
        person_steers: None,
        provider: &provider,
        tools: &tools,
        bind: bind(),
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
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let mut parts = MemoryParts::default();
    broca_session_loop::run::run(&mut session, &mut parts, admission, &seams)
        .await
        .unwrap();
    let opts = provider.calls().remove(0);
    let bytes = broca_provider::families::AnthropicMessages.render(&frozen, &opts);
    (session, parts, opts, bytes)
}

#[tokio::test]
async fn a_direct_imports_tool_results_are_not_repetition_history_for_a_new_run() {
    let (_session, _parts, opts, _bytes) = run_import(false).await;
    let outputs: Vec<_> = opts
        .prompt
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(&result.output),
            _ => None,
        })
        .collect();
    assert_eq!(outputs.len(), 8);
    for output in outputs {
        assert_eq!(
            output,
            &ToolOutput::Text {
                text: String::new()
            }
        );
    }
}

#[tokio::test]
async fn an_old_admissions_import_request_keeps_its_recorded_bytes_without_the_flag() {
    use broca_provider::wire::WireFamily;
    let (mut session, parts, opts, bytes) = run_import(true).await;
    assert!(serde_json::to_string(&opts.prompt)
        .unwrap()
        .contains("Tool repetition guidance:"));
    let cap = opts
        .step_context
        .as_ref()
        .and_then(|context| context.max_output_tokens);
    // Offline byte verification receives the stored prefix through the request's step start,
    // not its later model answer. Keep that boundary on this render-only conversation copy.
    session.conversation = session
        .conversation
        .before_entry(session.conversation.placed)
        .unwrap();
    let mut opts = broca_session_loop::run::offline_request(&session, &parts, &bind(), 1)
        .await
        .unwrap();
    if let Some(context) = opts.step_context.as_mut() {
        context.max_output_tokens = cap;
    }
    let rebuilt = broca_provider::families::AnthropicMessages.render(
        &broca_provider::families::anthropic_frozen_config("claude-sonnet-4-5"),
        &opts,
    );
    assert_eq!(
        rebuilt, bytes,
        "offline replay must preserve the old request, not fix it retroactively"
    );
    assert_eq!(
        format!("{:x}", sha2::Sha256::digest(&bytes)),
        "aabb268353ffec67577545db25eecfdb7736ade44bd7b9a12d4fbb682410c00b"
    );
}

#[tokio::test]
async fn an_old_checkpoint_without_the_boundary_uses_native_entry_facts() {
    use broca_provider::wire::WireFamily;
    let (mut session, parts, opts, bytes) = run_import(false).await;
    assert!(session
        .conversation
        .episodes
        .iter()
        .any(|episode| episode.first_model_answer.is_some()));
    for episode in &mut session.conversation.episodes {
        episode.first_model_answer = None;
    }
    session.conversation = session
        .conversation
        .before_entry(session.conversation.placed)
        .unwrap();
    let cap = opts
        .step_context
        .as_ref()
        .and_then(|context| context.max_output_tokens);
    let mut opts = broca_session_loop::run::offline_request(&session, &parts, &bind(), 1)
        .await
        .unwrap();
    if let Some(context) = opts.step_context.as_mut() {
        context.max_output_tokens = cap;
    }
    assert_eq!(
        broca_provider::families::AnthropicMessages.render(
            &broca_provider::families::anthropic_frozen_config("claude-sonnet-4-5"),
            &opts
        ),
        bytes
    );
}
