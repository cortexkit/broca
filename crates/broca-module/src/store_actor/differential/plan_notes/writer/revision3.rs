use super::*;

#[tokio::test]
async fn an_unanswered_anchor_redelivers_the_notes_in_place_at_next_run() {
    let first = manifest(&plan("before", &["read"], None, None), "plan-1");
    let pending = manifest(&plan("after", &["read", "search"], None, None), "plan-2");
    let (module, executor) = module_with_executor(
        claude(vec![
            text_step("a1", "ready"),
            failed_without_reply(),
            text_step("a3", "done"),
        ]),
        true,
        true,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending, Some(NOTE)).await;
    send_claude(&module, "r2", None).await;
    send_claude(&module, "r3", None).await;
    let requests = executor.provider.inner.calls();
    assert!(requests[1]
        .prompt
        .iter()
        .any(|message| message.tool_addition.is_some()));
    assert_eq!(names(&requests[2]), ["read"]);
    assert_eq!(
        requests[2].prompt.last().unwrap().tool_addition,
        requests[1].prompt.last().unwrap().tool_addition
    );
    assert!(
        records(&module)
            .await
            .iter()
            .filter(
                |record| matches!(record, Record::PlanNotes(delivery) if !delivery.notes.is_empty())
            )
            .count()
            == 2
    );
    assert!(!records(&module)
        .await
        .iter()
        .any(|record| matches!(record, Record::RefreshApplied(_))));
}

#[tokio::test]
async fn last_resolved_text_form_takes_precedence_over_the_catalog() {
    let first = manifest(&plan("before", &["read"], None, None), "plan-1");
    let pending = manifest(&plan("after", &["read"], None, None), "plan-2");
    let executor = responses(vec![text_step("a1", "ready"), text_step("a2", "done")]);
    let (module, executor) = module_with_executor(executor, true, true).await;
    *executor.next_render_config.lock().unwrap() =
        Some(serde_json::to_value(openai_chat_frozen_config("gpt-4o")).unwrap());
    send_on_same_model(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending, Some(NOTE)).await;
    send_on_same_model(&module, "r2", None).await;
    let requests = executor.provider.inner.calls();
    assert!(shown(&requests[1])
        .iter()
        .any(|(role, _, text)| *role == Role::System && text == "after"));
    assert!(records(&module).await.iter().any(|record| matches!(record, Record::RefreshApplied(apply) if apply.reason.as_deref() == Some("no_note_form"))));
}

#[tokio::test]
async fn no_note_ordered_idle_start_keeps_masters_wire_and_queued_input() {
    use crate::{actor::SendKey, RunKind};
    use broca_provider::{families::OpenAiResponses, wire::WireFamily};
    let first = manifest(&plan("same", &["read"], None, None), "plan-1");
    let mut pending = manifest(&plan("same", &["read"], None, None), "plan-2");
    let noter = Arc::new(super::super::super::controls::Noter::default());
    let with_hook = super::super::super::controls::noting_plan("refuse");
    let mut config: serde_json::Value = serde_json::from_str(pending.plan.as_json_text()).unwrap();
    let hook_config: serde_json::Value =
        serde_json::from_str(with_hook.plan.as_json_text()).unwrap();
    config["step_transform_items"] = hook_config["step_transform_items"].clone();
    config["composition"]["step_transform_items"] = config["step_transform_items"].clone();
    config["composition"]["providers"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"provider":"noter", "tools":[]}));
    pending.composition_digest =
        cortexkit_role_tool_provider::catalog::composition_digest(&config["composition"]).unwrap();
    config["composition_digest"] = pending.composition_digest.clone().into();
    pending.plan = broca_records::record::OpaqueJson::from_json_text(config.to_string()).unwrap();
    pending.declarations = with_hook.declarations.clone();
    let mut executor = responses(vec![text_step("a1", "ready"), text_step("a2", "done")]);
    executor.planes.hooks = Some(noter.clone());
    let (module, executor) = module_with_executor(executor, true, true).await;
    send_on_same_model(&module, "r1", Some(&first)).await;
    // An unchanged rendered surface can apply at the next run without removing any notes.
    assert_accepts(note_refresh(&module, 1, &pending, None).await, true);
    module
        .registry
        .session_flush_prefix(
            &module.key,
            Some(&core()),
            &SendScope::default(),
            "ordered-rebuild".into(),
        )
        .await
        .unwrap();
    let mut req = request("second");
    req.model = ModelHandle::new("openai", "gpt-6-luna");
    req.principal = Some(core());
    req.tools = None;
    req.policy_allowed = Default::default();
    let key = SendKey::Minted;
    let outcome = module
        .registry
        .route_submission_with_order(
            &module.key,
            broca_protocol::RunId("r2".into()),
            module.caller.shape("r2", &key, req),
            RunKind::FreshOrResume,
            key,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            None,
            None,
            None,
            false,
            Some(serde_json::from_value(serde_json::json!({"modes":["idle_start"]})).unwrap()),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    module.closed().await;
    assert_eq!(noter.calls(), 0);
    let calls = executor.provider.inner.calls();
    let sent = OpenAiResponses.render(&openai_responses_frozen_config("gpt-6-luna"), &calls[1]);
    assert_eq!(
        broca_core::render::request_digest_hex(&sent),
        "3a78bd75c485c99ef6c7febb49ad08374731e1be4bcc44a903ae1c26a3bece29",
        "{}",
        String::from_utf8_lossy(&sent)
    );
    let records = records(&module).await;
    assert!(!records
        .iter()
        .any(|record| matches!(record, Record::PlanNotes(_))));
    let normalized: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            Record::RunStart(start) if start.run_id == "r2" => {
                Some(serde_json::json!({"start": start.run_id}))
            }
            Record::Input(input) if input.run_id == "r2" => {
                Some(serde_json::json!({"input":input.message.content}))
            }
            Record::RefreshApplied(apply) if apply.generation == 1 => {
                Some(serde_json::json!({"apply":apply.generation}))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        normalized,
        serde_json::json!([
            {"start":"r2"}, {"input":[{"type":"text", "text":"second"}]}, {"apply":1},
        ])
        .as_array()
        .unwrap()
        .as_slice()
    );
}

#[tokio::test]
async fn reader_does_not_write_a_copy_of_an_unanswered_text_note() {
    let first = manifest(&plan("before", &["read"], None, None), "plan-1");
    let pending = manifest(&plan("after", &["read"], None, None), "plan-2");
    let (module, executor) = module_with_executor(
        claude(vec![
            text_step("a1", "ready"),
            failed_without_reply(),
            text_step("a3", "done"),
        ]),
        true,
        false,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending, Some(NOTE)).await;
    send_claude(&module, "r2", None).await;
    send_claude(&module, "r3", None).await;
    assert_eq!(executor.provider.inner.calls().len(), 3);
    let deliveries = records(&module)
        .await
        .into_iter()
        .filter_map(|record| match record {
            Record::PlanNotes(delivery) => Some(delivery),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].notes[0].text, NOTE);
}

#[tokio::test]
async fn an_unmarked_note_at_a_held_anchor_survives_the_writer_upgrade() {
    let first = manifest(&plan("before", &["read"], None, None), "plan-1");
    let pending = manifest(&plan("after", &["read"], None, None), "plan-2");
    let (module, executor) = module_with_executor(
        claude(vec![
            text_step("a1", "ready"),
            failed_without_reply(),
            text_step("a3", "signed next"),
            text_step("a4", "done"),
        ]),
        true,
        true,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    send_claude(&module, "r2", None).await;
    refresh(&module, 1, pending, Some(NOTE)).await;
    module.closed().await;
    let mut session = module.load().await;
    session
        .append(
            &[Record::PlanNotes(broca_session_store::record::PlanNotes {
                generation: 1,
                anchor: session.conversation.placed,
                blocks: None,
                model_key: "anthropic:claude-opus-5-5".into(),
                refused_tools: vec![],
                notes: vec![broca_session_store::record::PlanNote {
                    kind: broca_session_store::record::PlanNoteKind::System,
                    text: NOTE.into(),
                    form: "turn_system".into(),
                    tool: None,
                    tool_dispatch: None,
                }],
            })],
            true,
        )
        .await
        .unwrap();
    drop(session);
    send_claude(&module, "r3", None).await;
    send_claude(&module, "r4", None).await;
    let frozen = serde_json::from_value(executor.provider.render_config.clone().unwrap()).unwrap();
    use broca_provider::{families::AnthropicMessages, wire::WireFamily};
    let requests = executor.provider.inner.calls();
    let before = AnthropicMessages.shape(&frozen, &requests[2]);
    let after = AnthropicMessages.shape(&frozen, &requests[3]);
    assert_eq!(
        before["messages"][3],
        serde_json::json!({"role":"user", "content":format!("<context-note>{NOTE}</context-note>")})
    );
    assert_eq!(before["messages"][3], after["messages"][3]);
    assert_eq!(
        records(&module)
            .await
            .iter()
            .filter(|record| matches!(record, Record::PlanNotes(_)))
            .count(),
        1
    );
}
