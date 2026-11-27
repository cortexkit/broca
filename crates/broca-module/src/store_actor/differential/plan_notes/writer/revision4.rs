use super::*;

#[tokio::test]
async fn a_full_apply_strips_earlier_reasoning_when_a_sent_removal_note_disappears() {
    let first = manifest(&plan("same", &["read", "review"], None, None), "plan-1");
    let pending = manifest(&plan("same", &["read", "search"], None, None), "plan-2");
    let restored = manifest(
        &plan("same", &["read", "review", "search"], None, None),
        "plan-3",
    );
    let (module, executor) = module_with_executor(
        claude(vec![
            confirmed(text_step("a1", "ready")),
            confirmed(text_step("a2", "noted")),
            confirmed(text_step("a3", "folded")),
            text_step("a4", "done"),
        ]),
        true,
        true,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending, None).await;
    send_claude(&module, "r2", None).await;
    let mut fallback = executor.provider.render_config.clone().unwrap();
    fallback["server_side_fallback"] = true.into();
    *executor.next_render_config.lock().unwrap() = Some(fallback);
    send_claude(&module, "r3", None).await;
    refresh(&module, 2, restored, None).await;
    module
        .registry
        .session_flush_prefix(
            &module.key,
            Some(&core()),
            &SendScope::default(),
            "restore-reviewed".into(),
        )
        .await
        .unwrap();
    send_claude(&module, "r4", None).await;
    let requests = executor.provider.inner.calls();
    assert_eq!(names(&requests[2]), ["read", "review"]);
    assert_eq!(names(&requests[3]), ["read", "review", "search"]);
    let removal = broca_core::plan_notes::tool_removal("review");
    let frozen = serde_json::from_value(executor.provider.render_config.clone().unwrap()).unwrap();
    use broca_provider::{families::AnthropicMessages, wire::WireFamily};
    let rendered = AnthropicMessages.shape(&frozen, &requests[2]);
    let contains_removal = |body: &serde_json::Value| {
        body["messages"].as_array().unwrap().iter().any(|message| {
            let content = &message["content"];
            content.as_str().is_some_and(|text| text.contains(&removal))
                || content
                    .as_array()
                    .is_some_and(|blocks| blocks.iter().any(|block| block["text"] == removal))
        })
    };
    assert!(contains_removal(&rendered), "{rendered}");
    assert!(!contains_removal(
        &AnthropicMessages.shape(&frozen, &requests[3])
    ));
    assert!(requests[3].render_episodes.signed_floor > requests[2].render_episodes.signed_floor);
    assert!(records(&module)
        .await
        .iter()
        .any(|record| matches!(record, Record::RefreshApplied(apply) if apply.generation == 2)));
}

#[tokio::test]
async fn request_dump_at_a_post_commit_cut_uses_the_sent_fallback_renderer() {
    use broca_provider::{
        families::{anthropic_apikey_frozen_config, AnthropicMessages},
        wire::{RefusalFallbackConfig, WireFamily},
    };
    let mut refused = text_step("a1", "not answered");
    for event in &mut refused {
        if let NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) = event {
            *finish_reason = broca_protocol::FinishReason::ContentFilter;
        }
    }
    let fallback = anthropic_apikey_frozen_config("claude-opus-4-5");
    let model = ModelHandle::new("anthropic", "claude-opus-4-5");
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    frozen.refusal_fallback = Some(Box::new(RefusalFallbackConfig {
        model: model.clone(),
        context_limit: Some(100_000),
        frozen: fallback.clone(),
    }));
    let mut executor = executor(
        vec![
            confirmed(refused),
            confirmed(text_step("a2", "fallback reply")),
        ],
        None,
    );
    executor.provider.render_config = Some(serde_json::to_value(&frozen).unwrap());
    let render = move |opts: &broca_protocol::CallOptions| {
        let config = frozen
            .refusal_fallback
            .as_deref()
            .filter(|fallback| opts.target_model.as_ref() == Some(&fallback.model))
            .map_or(&frozen, |fallback| &fallback.frozen);
        AnthropicMessages.render(config, opts)
    };
    let send_bytes = render.clone();
    executor.provider.attempt_digest = Some(Arc::new(move |opts| {
        broca_core::render::request_digest_hex(&send_bytes(opts))
    }));
    let (module, executor) = module_with_executor(executor, true, true).await;
    let first = manifest(&plan("same", &["read"], None, None), "plan-1");
    send_claude(&module, "r1", Some(&first)).await;
    let log = records(&module).await;
    assert!(log
        .iter()
        .any(|record| matches!(record, Record::StepEnd(step) if step.step_id == 1)));
    let sent = executor.provider.inner.calls();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1].target_model.as_ref(), Some(&model));
    let runtime = crate::store_open::StoreRuntime::open(
        &module.dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        executor.clone(),
        IDLE,
    )
    .await
    .unwrap();
    let dump = crate::store_request_dump::serve(&module.registry, &runtime, &module.key, "r1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        dump.steps[0].outcome,
        broca_wire::request_dump::StepDumpOutcome::Written,
        "{dump:?}"
    );
    assert_eq!(
        std::fs::read(std::path::Path::new(&dump.folder).join("s1.json")).unwrap(),
        render(&sent[1])
    );
}
