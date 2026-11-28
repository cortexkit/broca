use super::*;

mod conservative;
mod in_place;
mod revision11;
mod revision3;
mod revision4;

use broca_protocol::{ModelHandle, NormalizedEvent, ProviderControl};
use broca_provider::families::{openai_chat_frozen_config, openai_responses_frozen_config};

fn responses(steps: Vec<Vec<NormalizedEvent>>) -> LoopExecutor {
    let mut executor = executor(steps, None);
    executor.provider.render_config =
        Some(serde_json::to_value(openai_responses_frozen_config("gpt-6-luna")).unwrap());
    executor
}

async fn send_on_same_model(
    module: &Module,
    run: &str,
    first: Option<&broca_records::record::PlanManifest>,
) {
    let mut req = request(run);
    req.model = ModelHandle::new("openai", "gpt-6-luna");
    send_request(module, run, first, &mut req).await;
}

pub(super) async fn refresh(
    module: &Module,
    generation: u64,
    next: broca_records::record::PlanManifest,
    note: Option<&str>,
) {
    let accepted = module
        .registry
        .route_session_refresh(
            &module.key,
            generation,
            "notes_until_rebuild".into(),
            Box::new(next),
            Vec::new(),
            note.map(str::to_owned),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            accepted,
            crate::actor::SessionRefreshOutcome::Accepted {
                applies_from_next_run: false,
                ..
            }
        ),
        "{accepted:?}"
    );
}

fn names(request: &broca_protocol::CallOptions) -> Vec<&str> {
    request
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect()
}

#[tokio::test]
async fn a_late_incompatible_route_full_applies_on_the_original_send() {
    late_incompatible_route(false).await;
}

#[tokio::test]
async fn queued_late_incompatible_route_full_applies_without_resubmission() {
    late_incompatible_route(true).await;
}

async fn late_incompatible_route(ordered: bool) {
    use crate::{actor::SendKey, RunKind};
    let first = manifest(&plan("before", &["read"], None, None), "plan-1");
    let pending = manifest(&plan("after", &["read", "search"], None, None), "plan-2");
    let executor = responses(vec![text_step("a1", "ready"), text_step("a2", "done")]);
    let (module, executor) = module_with_executor(executor, true, true).await;
    send_on_same_model(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending.clone(), Some(NOTE)).await;
    let changed = serde_json::to_value(openai_chat_frozen_config("gpt-4o")).unwrap();
    *executor.next_render_config.lock().unwrap() = Some(changed);
    let mut req = request("r2");
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
            Some(SendPlan {
                identity: "plan-1".into(),
                manifest: None,
                model_check: None,
            }),
            None,
            None,
            None,
            None,
            false,
            ordered.then(|| {
                serde_json::from_value(serde_json::json!({"modes":["idle_start"]})).unwrap()
            }),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    module.closed().await;
    let requests = executor.provider.inner.calls();
    assert_eq!(names(&requests[1]), ["read"]);
    assert!(shown(&requests[1])
        .iter()
        .any(|(_, _, text)| text == "after"));
    let log = records(&module).await;
    assert!(log
        .iter()
        .any(|record| matches!(record, Record::RefreshApplied(apply) if apply.generation == 1)));
    assert_eq!(
        log.iter()
            .filter(|record| matches!(record, Record::RunStart(start) if start.run_id == "r2"))
            .count(),
        1
    );
}

fn failed_without_reply() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(
            serde_json::json!({"class":"permanent", "message":"request ended before the reply"}),
        )
        .unwrap(),
        retries_used: 0,
    })]
}

pub(super) fn confirmed(mut step: Vec<NormalizedEvent>) -> Vec<NormalizedEvent> {
    let attempt = serde_json::from_value(serde_json::json!({
        "attempt":1, "request_body_len":40, "request_body_sha256":"00".repeat(32),
        "response_body_len":30, "response_body_sha256":"11".repeat(32), "outcome":"complete",
        "ts_ms":10, "sent_at_ms":1, "http_status":200, "transport":"https"
    }))
    .unwrap();
    step.insert(
        step.len() - 1,
        NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }),
    );
    step
}

pub(super) fn claude(steps: Vec<Vec<NormalizedEvent>>) -> LoopExecutor {
    let mut executor = executor(steps, None);
    let mut config = broca_provider::families::anthropic_apikey_frozen_config("claude-opus-5-5");
    config.renderer_version = 4;
    config.anthropic_turn_system = true;
    executor.provider.render_config = Some(serde_json::to_value(config).unwrap());
    executor
}

pub(super) async fn send_claude(
    module: &Module,
    run: &str,
    first: Option<&broca_records::record::PlanManifest>,
) {
    let mut request = request(run);
    request.model = ModelHandle::new("anthropic", "claude-opus-5-5");
    send_request(module, run, first, &mut request).await;
}

#[tokio::test]
async fn historical_native_request_is_rebuilt_after_a_later_full_apply() {
    use broca_provider::{families::AnthropicMessages, wire::WireFamily};
    let first = manifest(&plan("same", &["read"], None, None), "plan-1");
    let pending = manifest(&plan("same", &["read", "search"], None, None), "plan-2");
    let mut noted_step = text_step("a2", "noted");
    let attempt = serde_json::from_value(serde_json::json!({
        "attempt":1, "request_body_len":40, "request_body_sha256":"00".repeat(32),
        "response_body_len":30, "response_body_sha256":"11".repeat(32),
        "outcome":"complete", "ts_ms":10, "sent_at_ms":1, "http_status":200, "transport":"https"
    }))
    .unwrap();
    noted_step.insert(
        noted_step.len() - 1,
        NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }),
    );
    let (module, executor) = module_with_executor(
        claude(vec![
            text_step("a1", "ready"),
            noted_step,
            text_step("a3", "applied"),
        ]),
        true,
        true,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending.clone(), None).await;
    import_writer_note(&module, &pending).await;
    send_claude(&module, "r2", None).await;
    module
        .registry
        .session_flush_prefix(
            &module.key,
            Some(&core()),
            &SendScope::default(),
            "finish-history".into(),
        )
        .await
        .unwrap();
    send_claude(&module, "r3", None).await;
    let live = executor.provider.inner.calls();
    assert!(live[1]
        .prompt
        .iter()
        .any(|message| message.tool_addition.is_some()));
    assert_eq!(names(&live[2]), ["read", "search"]);
    let runtime = crate::store_open::StoreRuntime::open(
        &module.dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        executor.clone(),
        IDLE,
    )
    .await
    .unwrap();
    let parts = runtime.parts_for(
        broca_records::session_storage::StorageClass::Ordinary,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    );
    let session = module.load().await;
    let history = session.open_log_records().await.unwrap();
    let rebuilt = crate::store_actor::recorded_warm::request(
        &session,
        &history,
        parts,
        &module.key,
        "r2",
        Some(1),
    )
    .await
    .unwrap();
    let config = serde_json::from_value(rebuilt.config.frozen_render_config).unwrap();
    assert_eq!(
        AnthropicMessages.render(&config, &rebuilt.opts),
        AnthropicMessages.render(&config, &live[1])
    );
}

#[tokio::test]
async fn a_late_apply_cannot_refuse_the_send_when_pending_scope_is_not_held() {
    let first = manifest(&plan("before", &["read"], None, None), "plan-1");
    let mut next = plan("after", &["read"], None, None);
    next["tool_items"][0]["preset"] = broca_fetch_plan::CONFINED_TOOL_PRESET.into();
    let pending = manifest(&next, "plan-2");
    let (module, executor) = module_with_executor(
        responses(vec![text_step("a1", "ready"), text_step("a2", "done")]),
        true,
        true,
    )
    .await;
    send_on_same_model(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending, Some(NOTE)).await;
    *executor.next_render_config.lock().unwrap() =
        Some(serde_json::to_value(openai_chat_frozen_config("gpt-4o")).unwrap());
    send_on_same_model(&module, "r2", None).await;
    let calls = executor.provider.inner.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(names(&calls[1]), ["read"]);
    let entries = records(&module).await;
    assert!(!entries
        .iter()
        .any(|record| matches!(record, Record::RefreshApplied(_))));
    assert!(entries
        .iter()
        .any(|record| matches!(record, Record::RunStart(start) if start.run_id == "r2")));
}
