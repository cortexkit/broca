use super::*;
use broca_core::{Provider, WalStore};
use broca_protocol::RunFinishReason;
use broca_wal::WalRecord;

fn schema() -> Value {
    serde_json::json!({"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false})
}

fn request(root: &std::path::Path) -> broca_core::RunRequest {
    broca_core::RunRequest::from_submission(
        serde_json::from_value(serde_json::json!({
            "model":{"provider_module_id":"anthropic","model_id":"fallback-test"},
            "tools":[],"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],
            "structured_output":{"mode":"submit_tool","schema":schema()}
        }))
        .unwrap(),
        broca_protocol::RunId("fallback-run".into()),
        broca_protocol::BindIdentity::new(root.to_str().unwrap(), "h", "s"),
        broca_protocol::SubmissionId("sub".into()),
    )
}

fn provider(server: &ScriptedServer) -> LiveProvider<AnthropicMessages> {
    let mut frozen = crate::families::anthropic_frozen_config("fallback-test");
    frozen.base_url = Some(server.url.clone());
    frozen.structured_output = Some(
        crate::structured_output::decide(
            WireFamilyId::AnthropicMessages,
            "anthropic",
            None,
            crate::spec::CapabilityFlag::Known(true),
            &serde_json::from_value(serde_json::json!({"mode":"submit_tool","schema":schema()}))
                .unwrap(),
        )
        .unwrap(),
    );
    LiveProvider::new(AnthropicMessages, frozen, AuthHandle::None)
}

fn refusal(message: &str) -> ScriptResponse {
    ScriptResponse::Http {
        status: 400,
        headers: vec![],
        body: Box::leak(serde_json::json!({"type":"error","error":{"type":"invalid_request_error","message":message}}).to_string().into_bytes().into_boxed_slice()),
    }
}

fn submitted() -> ScriptResponse {
    ScriptResponse::Http {
        status: 200,
        headers: vec![],
        body: concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_submit\",\"name\":\"submit_result\",\"input\":{}}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"answer\\\":\\\"ok\\\"}\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":1}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
        ).as_bytes(),
    }
}

async fn drive(
    root: &std::path::Path,
    provider: &LiveProvider<AnthropicMessages>,
) -> broca_core::RunSummary {
    drive_store(
        root,
        provider,
        &broca_core::FileWalStore::new(root.join("wal")),
    )
    .await
}

async fn drive_store<W: WalStore>(
    root: &std::path::Path,
    provider: &LiveProvider<AnthropicMessages>,
    store: &W,
) -> broca_core::RunSummary {
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = tokio::time::timeout(
        Duration::from_secs(20),
        broca_core::run_loop(
            request(root),
            provider,
            &broca_core::StubToolPlane::new(),
            &broca_core::FileLeaseStore::new(root.join("lease")),
            &broca_core::AllowAll,
            store,
            &tx,
        ),
    )
    .await
    .expect("fallback run must terminate")
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    summary
}

#[tokio::test]
async fn compilation_400_is_recorded_before_nonstrict_resend_and_reports_best_effort() {
    let root = broca_test_support::scratch_root().join("schema-fallback-success");
    let server = ScriptedServer::start(vec![
        refusal("The compiled grammar is too large, which would cause performance issues."),
        submitted(),
    ])
    .await;
    let provider = provider(&server);
    let observed = broca_crash_harness::ObservedWalStore::new(root.join("wal"));
    let summary = drive_store(&root, &provider, &observed).await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let bodies = server.request_bodies();
    assert_eq!(bodies.len(), 2);
    let strict: Value = serde_json::from_slice(&bodies[0]).unwrap();
    let loose: Value = serde_json::from_slice(&bodies[1]).unwrap();
    assert_eq!(strict["tools"][0]["strict"], true);
    assert!(loose["tools"][0].get("strict").is_none());

    let store = broca_core::FileWalStore::new(root.join("wal"));
    let records = store.read_records(&request(&root).session).unwrap();
    assert_eq!(
        records
            .iter()
            .filter_map(|f| match &f.record {
                WalRecord::ModelAttemptFinished { attempt, .. } => Some(*attempt),
                _ => None,
            })
            .collect::<Vec<_>>(),
        [0, 0]
    );
    let downgrade = records
        .iter()
        .position(|r| matches!(r.record, WalRecord::StructuredOutputDowngraded { .. }))
        .unwrap();
    let completed = records
        .iter()
        .position(|r| matches!(r.record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    assert!(downgrade < completed);
    assert!(
        observed.synced().contains(&records[downgrade].seq),
        "the downgrade itself must have a durability barrier before resend"
    );
    let state = broca_core::reconcile_run_state(
        &records.iter().map(|f| f.record.clone()).collect::<Vec<_>>(),
        "fallback-run",
    )
    .unwrap();
    let so = state.structured_output.unwrap();
    assert_eq!(so.enforcement, "best_effort");
    assert_eq!(so.reason.as_deref(), Some("grammar_too_large"));
    assert_eq!(so.validation, "passed");
    assert_eq!(state.retries_used, 0);
    assert!(!provider
        .render_request(&CallOptions::new(vec![]))
        .is_empty());
}

#[tokio::test]
async fn other_invalid_request_400_is_not_downgraded() {
    let root = broca_test_support::scratch_root().join("schema-fallback-other-400");
    let server = ScriptedServer::start(vec![
        refusal("messages: field required"),
        refusal("messages: field required"),
    ])
    .await;
    let provider = provider(&server);
    assert_eq!(drive(&root, &provider).await.reason, RunFinishReason::Error);
    assert_eq!(server.request_bodies().len(), 1);
    let records = broca_core::FileWalStore::new(root.join("wal"))
        .read_records(&request(&root).session)
        .unwrap();
    assert!(!records
        .iter()
        .any(|r| matches!(r.record, WalRecord::StructuredOutputDowngraded { .. })));
}

#[tokio::test]
async fn failed_nonstrict_resend_does_not_loop() {
    let root = broca_test_support::scratch_root().join("schema-fallback-no-loop");
    let message = "Schema is too complex for compilation";
    let server = ScriptedServer::start(vec![
        refusal(message),
        refusal(message),
        refusal("messages: field required"),
    ])
    .await;
    assert_eq!(
        drive(&root, &provider(&server)).await.reason,
        RunFinishReason::Error
    );
    assert_eq!(server.request_bodies().len(), 2);
}

#[test]
fn compiler_refusal_is_classified_before_error_body_truncation() {
    let body = serde_json::json!({"error":{"type":"invalid_request_error","message":"Schema is too complex for compilation"},"padding":"x".repeat(MAX_ERROR_BODY_BYTES + 100)}).to_string();
    let error = super::super::http_error_in_family(
        400,
        &body,
        None,
        Some(WireFamilyId::AnthropicMessages),
        None,
    );
    assert!(crate::structured_output::grammar_rejection(&error));
}

#[tokio::test]
async fn crash_cut_after_downgrade_resumes_with_identical_nonstrict_bytes() {
    let root = broca_test_support::scratch_root().join("schema-fallback-crash-cut");
    let server = ScriptedServer::start(vec![
        refusal("Schema is too complex for compilation"),
        submitted(),
        submitted(),
    ])
    .await;
    let reference_provider = provider(&server);
    assert_eq!(
        drive(&root, &reference_provider).await.reason,
        RunFinishReason::Completed
    );
    let bodies = server.request_bodies();
    let store = broca_core::FileWalStore::new(root.join("wal"));
    let path = store.wal_path(&request(&root).session);
    let image = broca_crash_harness::LogImage::read(&path);
    let records = image.records();
    let cut = records
        .iter()
        .position(|r| matches!(r, WalRecord::StructuredOutputDowngraded { .. }))
        .unwrap()
        + 1;
    // Cut exactly after the downgrade, before any non-strict response was recorded. Neither
    // the successful response nor the process memo is available to the recovering provider.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(image.end_of_records(cut) as u64)
        .unwrap();
    let prefix = store.read_records(&request(&root).session).unwrap();
    let replayed =
        broca_core::replay(&prefix.iter().map(|f| f.record.clone()).collect::<Vec<_>>()).unwrap();
    let config = replayed.run_config.unwrap();
    let frozen: crate::wire::FrozenRenderConfig =
        serde_json::from_value(config.frozen_render_config.clone()).unwrap();
    assert_eq!(
        frozen.structured_output.as_ref().unwrap().reason.as_deref(),
        Some("grammar_too_large")
    );
    let admission = broca_core::AdmissionCheckpointV1::from_frames(&prefix).unwrap();
    assert_eq!(
        admission.last_episode.unwrap().config.frozen_render_config,
        config.frozen_render_config
    );
    let resumed = LiveProvider::new(AnthropicMessages, frozen, AuthHandle::None);
    assert_eq!(
        drive(&root, &resumed).await.reason,
        RunFinishReason::Completed
    );
    let after = server.request_bodies();
    assert_eq!(after.len(), 3);
    assert_eq!(
        after[2], bodies[1],
        "a crash before resend must reproduce the non-strict request byte for byte"
    );
    assert!(!resumed.structured_output_grammar_rejection(&http_error(400, "{\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Schema is too complex for compilation\"}}", None)));
}

#[tokio::test]
async fn downgrade_stays_nonstrict_on_later_steps() {
    let root = broca_test_support::scratch_root().join("schema-fallback-later-step");
    let work = match submitted() {
        ScriptResponse::Http {
            status,
            headers,
            body,
        } => ScriptResponse::Http {
            status,
            headers,
            body: Box::leak(
                String::from_utf8(body.to_vec())
                    .unwrap()
                    .replace("submit_result", "work")
                    .into_bytes()
                    .into_boxed_slice(),
            ),
        },
        _ => unreachable!(),
    };
    let server = ScriptedServer::start(vec![
        refusal("Schema is too complex for compilation"),
        work,
        submitted(),
    ])
    .await;
    assert_eq!(
        drive(&root, &provider(&server)).await.reason,
        RunFinishReason::Completed
    );
    let bodies = server.request_bodies();
    assert_eq!(bodies.len(), 3);
    for body in &bodies[1..] {
        let body: Value = serde_json::from_slice(body).unwrap();
        assert!(body["tools"][0].get("strict").is_none());
    }
}

#[tokio::test]
async fn retries_before_schema_downgrade_are_preserved_without_charging_the_resend() {
    let root = broca_test_support::scratch_root().join("schema-fallback-prior-retry");
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 503,
            headers: vec![],
            body: br#"{"error":{"type":"overloaded_error","message":"busy"}}"#,
        },
        refusal("Schema is too complex for compilation"),
        submitted(),
    ])
    .await;
    assert_eq!(
        drive(&root, &provider(&server)).await.reason,
        RunFinishReason::Completed
    );
    assert_eq!(server.request_bodies().len(), 3);
    let records = broca_core::FileWalStore::new(root.join("wal"))
        .read_records(&request(&root).session)
        .unwrap();
    let state =
        broca_core::reconcile_run_state(records.iter().map(|f| &f.record), "fallback-run").unwrap();
    assert_eq!(
        state.retries_used, 1,
        "only the ordinary 503 retry counts, not the schema resend"
    );
}
