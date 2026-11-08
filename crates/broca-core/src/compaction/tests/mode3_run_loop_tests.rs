use super::*;
use crate::compaction_v2::{
    self as v2,
    protocol::{
        notice::NoticeRequest,
        view::{Edit, EditOp, View},
    },
};
use broca_wal::RunConfig;

#[derive(Default)]
struct Plane {
    unavailable: bool,
    view: Option<View>,
    calls: Mutex<Vec<(String, Value)>>,
}

#[async_trait::async_trait]
impl CompactionPlane for Plane {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        panic!("the retired v1 provider protocol must not be called")
    }
    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, v2::runtime::CallError> {
        self.calls.lock().unwrap().push((method.into(), params));
        if self.unavailable {
            return Err(v2::runtime::CallError::Unavailable);
        }
        serde_json::value::to_raw_value(&match method {
            "compaction.setup" => json!({"answer":"ready"}),
            "compaction.notice" => json!({"answer":"received"}),
            "compaction.ride" => match &self.view {
                Some(view) => json!({"answer":"view", "view":view}),
                None => json!({"answer":"nothing"}),
            },
            _ => panic!("unexpected compaction method {method}"),
        })
        .map_err(|_| v2::runtime::CallError::Unavailable)
    }
}

fn cut(version: u64) -> View {
    let through = crate::message_mid(true, Some(&fixture_lineage_id().to_string()), 1).unwrap();
    View::seal(version, vec![Edit::new(version, EditOp::Cut { through })])
}

async fn drive(
    shared: &Shared,
    plane: Arc<Plane>,
) -> (Result<crate::RunSummary, crate::RunError>, PrefixBound) {
    drive_as(shared, plane, "run", crate::StartMode::FreshOrResume).await
}

async fn drive_as(
    shared: &Shared,
    plane: Arc<Plane>,
    run: &str,
    mode: crate::StartMode,
) -> (Result<crate::RunSummary, crate::RunError>, PrefixBound) {
    drive_events(shared, plane, run, mode, model_events()).await
}

async fn drive_events(
    shared: &Shared,
    plane: Arc<Plane>,
    run: &str,
    mode: crate::StartMode,
    events: Vec<broca_protocol::NormalizedEvent>,
) -> (Result<crate::RunSummary, crate::RunError>, PrefixBound) {
    let writer = shared.writer();
    let (mut request, mut control, _) = history_request();
    request.run_id = broca_protocol::RunId(run.into());
    request.context_limit = Some(100_000);
    control.admitted_context_limit = request.context_limit;
    control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
    control.compaction_plane = Some(plane);
    control.compaction_views = None;
    control.compaction_cache_proven_expired = true;
    let model = PrefixBound(crate::StubProvider::new(vec![events]));
    let (out, _events) = tokio::sync::mpsc::channel(256);
    let result = crate::run_loop_on_writer(
        request,
        &writer,
        mode,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        shared,
        None,
        control,
        &out,
    )
    .await;
    writer.shutdown().await.unwrap();
    (result, model)
}

struct RequestedLifetime;

#[async_trait::async_trait]
impl crate::keep_warm_wait::KeepWarmDriver for RequestedLifetime {
    fn timing(&self, _: &RunConfig) -> Result<crate::keep_warm_wait::WarmTiming, String> {
        panic!("these runs do not wait for tools")
    }
    fn requested_ttl_ms(&self, config: &RunConfig) -> Option<u64> {
        config.frozen_render_config["cache_ttl_ms"].as_u64()
    }
    fn last_sent_at_ms(&self) -> Option<u64> {
        None
    }
    fn costs(&self, _: &RunConfig) -> Result<crate::keep_warm::WarmCosts, String> {
        panic!("these runs do not warm")
    }
    async fn warm(
        &self,
        _: &RunConfig,
        _: &broca_protocol::RunId,
    ) -> crate::keep_warm_wait::WarmTick {
        panic!("these runs do not warm")
    }
}

struct CacheProofProvider {
    inner: crate::StubProvider,
    fallback_context_limit: Option<u64>,
}

#[async_trait::async_trait]
impl crate::Provider for CacheProofProvider {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &CallOptions,
    ) -> tokio::sync::mpsc::Receiver<broca_protocol::NormalizedEvent> {
        self.inner.step(step_id, opts).await
    }
    fn reasoning_is_prefix_bound(&self) -> bool {
        true
    }
    fn render_config_blob(&self) -> Value {
        json!({"cache_ttl_ms":300_000, "refusal_fallback":{
            "model":{"provider_module_id":"stub","model_id":"fallback"},
            "frozen":{"cache_ttl_ms":3_600_000}
        }})
    }
    fn refusal_fallback_model(&self) -> Option<broca_protocol::request::ModelHandle> {
        Some(broca_protocol::request::ModelHandle::new(
            "stub", "fallback",
        ))
    }
    fn refusal_fallback_context_limit(&self) -> Option<u64> {
        self.fallback_context_limit
    }
    fn set_step_route(&self, route: crate::refusal_fallback::StepRoute) {
        self.inner.set_step_route(route);
    }
}

async fn drive_cache_identity(
    shared: &Shared,
    plane: Arc<Plane>,
    run: &str,
    model: broca_protocol::request::ModelHandle,
    variant: Option<&str>,
) {
    drive_cache_identity_with_fallback_window(shared, plane, run, model, variant, None).await;
}

async fn drive_cache_identity_with_fallback_window(
    shared: &Shared,
    plane: Arc<Plane>,
    run: &str,
    model: broca_protocol::request::ModelHandle,
    variant: Option<&str>,
    fallback_context_limit: Option<u64>,
) -> CacheProofProvider {
    let writer = shared.writer();
    let (mut request, mut control, _) = history_request();
    request.run_id = broca_protocol::RunId(run.into());
    request.model = model;
    request.variant = variant.map(str::to_owned);
    request.context_limit = Some(100_000);
    control.admitted_context_limit = request.context_limit;
    control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
    control.compaction_plane = Some(plane);
    control.compaction_views = None;
    control.keep_warm = Some(Arc::new(RequestedLifetime));
    assert!(!control.compaction_cache_proven_expired);
    let provider = CacheProofProvider {
        inner: crate::StubProvider::new(vec![model_events()]),
        fallback_context_limit,
    };
    let (out, _events) = tokio::sync::mpsc::channel(256);
    let result = crate::run_loop_on_writer(
        request,
        &writer,
        crate::StartMode::FreshOrResume,
        &provider,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    assert_eq!(result.reason, RunFinishReason::Completed);
    writer.shutdown().await.unwrap();
    provider
}

async fn expired_cache_history() -> Shared {
    let shared = Shared::default();
    drive_cache_identity(
        &shared,
        Arc::new(Plane::default()),
        "first",
        broca_protocol::request::ModelHandle::new("stub", "model"),
        None,
    )
    .await;
    let mut records = shared.records();
    let at = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let WalRecord::ModelStepFinished { step_id, .. } = records[at] else {
        unreachable!()
    };
    records.insert(
        at,
        serde_json::from_value(json!({
            "type":"model_attempt_finished", "step_id":step_id, "attempt":0,
            "request_body_len":1, "request_body_sha256":"cache-prefix",
            "response_body_len":1, "response_body_sha256":"response",
            "http_status":200, "outcome":"complete", "sent_at_ms":1, "ts_ms":2
        }))
        .unwrap(),
    );
    Shared::with_records(records)
}

fn cache_ride_causes(plane: &Plane) -> Vec<Value> {
    plane
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(method, _)| method == "compaction.ride")
        .flat_map(|(_, params)| params["causes"].as_array().unwrap().clone())
        .collect()
}

#[tokio::test]
async fn compaction_v2_model_change_does_not_claim_another_models_cache_expiry() {
    use broca_protocol::request::ModelHandle;
    for (model, variant) in [
        (ModelHandle::new("stub", "other"), None),
        (ModelHandle::new("other-provider", "model"), None),
        (ModelHandle::new("stub", "model"), Some("reasoning")),
    ] {
        let shared = expired_cache_history().await;
        let plane = Arc::new(Plane::default());
        drive_cache_identity(&shared, plane.clone(), "second", model, variant).await;
        let causes = cache_ride_causes(&plane);
        assert!(causes.contains(&json!("model_change")), "{causes:?}");
        assert!(!causes.contains(&json!("cache_expired")), "{causes:?}");
    }
}

#[tokio::test]
async fn compaction_v2_same_model_claims_proven_cache_expiry() {
    let shared = expired_cache_history().await;
    let plane = Arc::new(Plane::default());
    drive_cache_identity(
        &shared,
        plane.clone(),
        "second",
        broca_protocol::request::ModelHandle::new("stub", "model"),
        None,
    )
    .await;
    let causes = cache_ride_causes(&plane);
    assert!(causes.contains(&json!("cache_expired")), "{causes:?}");
    assert!(!causes.contains(&json!("model_change")), "{causes:?}");
}

#[tokio::test]
async fn compaction_v2_return_to_previous_model_does_not_reuse_its_old_touch() {
    use broca_protocol::request::ModelHandle;
    let shared = expired_cache_history().await;
    drive_cache_identity(
        &shared,
        Arc::new(Plane::default()),
        "second",
        ModelHandle::new("stub", "other"),
        None,
    )
    .await;
    let plane = Arc::new(Plane::default());
    drive_cache_identity(
        &shared,
        plane.clone(),
        "third",
        ModelHandle::new("stub", "model"),
        None,
    )
    .await;
    let causes = cache_ride_causes(&plane);
    assert!(causes.contains(&json!("model_change")), "{causes:?}");
    assert!(!causes.contains(&json!("cache_expired")), "{causes:?}");
}

#[tokio::test]
async fn compaction_v2_warm_cannot_restore_a_touch_across_a_model_change() {
    use broca_protocol::request::ModelHandle;
    for switched in [false, true] {
        let shared = expired_cache_history().await;
        if switched {
            drive_cache_identity(
                &shared,
                Arc::new(Plane::default()),
                "second",
                ModelHandle::new("stub", "other"),
                None,
            )
            .await;
        }
        let mut records = shared.records();
        let step_id = records
            .iter()
            .find_map(|record| match record {
                WalRecord::ModelAttemptFinished { step_id, .. } => Some(*step_id),
                _ => None,
            })
            .unwrap();
        records.push(
            serde_json::from_value(json!({
                "type":"cache_warmed", "run_id":"first", "step_id":step_id,
                "request_body_sha256":"cache-prefix", "warm_body_sha256":"warm",
                "usage":{}, "sent_at_ms":10_000
            }))
            .unwrap(),
        );
        let shared = Shared::with_records(records);
        let plane = Arc::new(Plane::default());
        drive_cache_identity(
            &shared,
            plane.clone(),
            "third",
            ModelHandle::new("stub", "model"),
            None,
        )
        .await;
        assert_eq!(
            cache_ride_causes(&plane).contains(&json!("cache_expired")),
            !switched
        );
    }
}

async fn scheduled_cache_history(fallback_sent: bool, sent_at_ms: u64) -> Shared {
    let shared = expired_cache_history().await;
    let schedule = json!({
        "source":{"provider_module_id":"stub","model_id":"model"},
        "fallback":{"provider_module_id":"stub","model_id":"fallback"},
        "phase":{"kind":"fallback","remaining":9}, "re_refusals":0
    });
    let mut records = shared.records();
    for record in &mut records {
        match record {
            WalRecord::ModelStepFinished { fallback, .. } => {
                fallback.refusal_fallback = Some(serde_json::from_value(schedule.clone()).unwrap());
                if fallback_sent {
                    fallback.route = Some(broca_wal::StepRoute {
                        model: broca_protocol::request::ModelHandle::new("stub", "fallback"),
                        reason: broca_wal::StepRouteReason::Schedule,
                    });
                }
            }
            WalRecord::ModelAttemptFinished {
                sent_at_ms: sent, ..
            } => *sent = Some(sent_at_ms),
            _ => {}
        }
    }
    Shared::with_records(records)
}

#[tokio::test]
async fn compaction_v2_first_fallback_request_cannot_use_the_source_touch() {
    let shared = scheduled_cache_history(false, 1).await;
    let plane = Arc::new(Plane::default());
    drive_cache_identity(
        &shared,
        plane.clone(),
        "second",
        broca_protocol::request::ModelHandle::new("stub", "model"),
        None,
    )
    .await;
    assert!(!cache_ride_causes(&plane).contains(&json!("cache_expired")));
    assert!(shared.records().iter().rev().any(|record| matches!(record,
        WalRecord::ModelStepFinished { fallback, .. } if fallback.route.is_some())));
}

#[tokio::test]
async fn compaction_v2_ride_and_send_skip_a_scheduled_fallback_that_does_not_fit() {
    use broca_protocol::request::ModelHandle;
    for fallback_window in [999, 1_000] {
        let shared = scheduled_cache_history(false, 1).await;
        let mut records = shared.records();
        for record in &mut records {
            if let WalRecord::ModelStepFinished { usage, .. } = record {
                usage.input_tokens = Some(1_000);
            }
        }
        let shared = Shared::with_records(records);
        let plane = Arc::new(Plane::default());
        let source = ModelHandle::new("stub", "model");
        let provider = drive_cache_identity_with_fallback_window(
            &shared,
            plane.clone(),
            "second",
            source.clone(),
            None,
            Some(fallback_window),
        )
        .await;
        assert!(
            cache_ride_causes(&plane).contains(&json!("cache_expired")),
            "the source's expired cache, not the untouched fallback cache, dates this ride"
        );
        assert_eq!(
            provider.inner.calls()[0].target_model.as_ref(),
            Some(&source)
        );
        assert_eq!(
            provider.inner.routes(),
            [crate::refusal_fallback::StepRoute::Source]
        );
        let last = shared
            .records()
            .into_iter()
            .rev()
            .find(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
            .unwrap();
        let WalRecord::ModelStepFinished { fallback, .. } = last else {
            unreachable!()
        };
        assert!(
            fallback.route.is_none(),
            "the oversized fallback must not serve the step"
        );
    }
}

#[tokio::test]
async fn compaction_v2_fallback_expiry_uses_the_fallback_requests_own_lifetime() {
    for expired in [false, true] {
        let sent_at_ms = if expired {
            1
        } else {
            crate::compaction::now() - 600_000
        };
        let shared = scheduled_cache_history(true, sent_at_ms).await;
        let plane = Arc::new(Plane::default());
        drive_cache_identity(
            &shared,
            plane.clone(),
            "second",
            broca_protocol::request::ModelHandle::new("stub", "model"),
            None,
        )
        .await;
        assert_eq!(
            cache_ride_causes(&plane).contains(&json!("cache_expired")),
            expired,
            "ten minutes expires the source's five-minute policy, not the fallback's hour"
        );
    }
}

#[tokio::test]
async fn compaction_v2_server_side_handoff_cannot_prove_source_expiry() {
    for model in ["model", "server-fallback"] {
        let shared = expired_cache_history().await;
        let mut records = shared.records();
        for record in &mut records {
            if let WalRecord::ModelStepFinished { fallback, .. } = record {
                fallback.served_model = Some("server-fallback".into());
            }
        }
        let shared = Shared::with_records(records);
        let plane = Arc::new(Plane::default());
        drive_cache_identity(
            &shared,
            plane.clone(),
            "second",
            broca_protocol::request::ModelHandle::new("stub", model),
            None,
        )
        .await;
        assert!(!cache_ride_causes(&plane).contains(&json!("cache_expired")));
    }
}

#[tokio::test]
async fn live_reply_deltas_follow_the_writing_runs_clean_flag() {
    use broca_protocol::{DisplayEvent, MessageId, NormalizedEvent, ProviderControl};
    for (mode3, render) in [(false, false), (false, true), (true, false)] {
        let shared = Shared::default();
        let writer = shared.writer();
        let (mut request, mut control, _) = history_request();
        request.context_limit = Some(100_000);
        control.admitted_context_limit = request.context_limit;
        control.legacy_compaction_fixture = false;
        control.compaction_views = None;
        let mut manifest = control.manifest.as_deref().unwrap().clone();
        let mut plan = json!({"render_tags": render, "composition": {}});
        if mode3 {
            plan["compaction_item"] = json!({"provider":"provider", "preset":"head", "params":{}});
            control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
            control.compaction_plane = Some(Arc::new(Plane::default()));
        } else {
            control.compaction_plane = None;
        }
        manifest.plan = OpaqueJson::from_json_text(plan.to_string()).unwrap();
        control.manifest = Some(Arc::new(manifest));
        let text = |delta: &str| DisplayEvent::TextDelta {
            mid: None,
            part: Some(1),
            message_id: MessageId("a".into()),
            tool_call_id: None,
            delta: delta.into(),
        };
        let reasoning = DisplayEvent::ReasoningDelta {
            mid: None,
            part: Some(0),
            message_id: MessageId("a".into()),
            delta: "§8§ thought".into(),
        };
        let tool = DisplayEvent::ToolInputDelta {
            mid: None,
            part: Some(2),
            tool_call_id: broca_protocol::ToolCallId("call".into()),
            delta: "{\"text\":\"§9§ args\"}".into(),
        };
        let mut script = vec![NormalizedEvent::Display(reasoning.clone())];
        script.extend(
            ["§3", "2§ I have", " read it"].map(|delta| NormalizedEvent::Display(text(delta))),
        );
        script.push(NormalizedEvent::Display(tool.clone()));
        script.extend([
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: broca_protocol::AssistantMessage {
                    message_id: MessageId("a".into()),
                    content: vec![ContentBlock::Text {
                        text: "§32§ I have read it".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ]);
        let model = crate::StubProvider::new(vec![script]);
        let (out, mut events) = tokio::sync::mpsc::channel(256);
        let result = crate::run_loop_on_writer(
            request,
            &writer,
            crate::StartMode::FreshOrResume,
            &model,
            &crate::StubToolPlane::new(),
            &crate::AllowAll,
            &shared,
            None,
            control,
            &out,
        )
        .await
        .unwrap();
        assert_eq!(result.reason, RunFinishReason::Completed);
        drop(out);
        let mut display = Vec::new();
        while let Some(event) = events.recv().await {
            if let crate::OutputEvent::Display(event) = event {
                display.push(event);
            }
        }
        assert_eq!(display.first(), Some(&reasoning));
        assert_eq!(display.last(), Some(&tool));
        let text_deltas: Vec<_> = display
            .iter()
            .filter_map(|event| match event {
                DisplayEvent::TextDelta { delta, .. } => Some(delta.as_str()),
                _ => None,
            })
            .collect();
        if render || mode3 {
            assert_eq!(text_deltas.concat(), "I have read it", "mode3={mode3}");
        } else {
            assert_eq!(text_deltas, ["§3", "2§ I have", " read it"]);
        }
        assert!(shared.records().iter().any(|record| matches!(record,
            WalRecord::ModelStepFinished { assistant_message, .. }
            if assistant_message.content == vec![ContentBlock::Text { text: "§32§ I have read it".into() }]
        )), "stored reply stays raw");
        writer.shutdown().await.unwrap();
    }
}

async fn drive_frozen_legacy(shared: &Shared) -> PrefixBound {
    let writer = shared.writer();
    let (request, mut control, _) = history_request();
    control.legacy_compaction_fixture = false;
    control.compaction_plane = Some(Arc::new(Plane::default()));
    let model = PrefixBound(crate::StubProvider::new(vec![model_events()]));
    let (out, _events) = tokio::sync::mpsc::channel(256);
    crate::run_loop_on_writer(
        request,
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    writer.shutdown().await.unwrap();
    model
}

#[tokio::test]
async fn compaction_v2_legacy_plan_without_setup_stays_raw_and_never_calls_v1() {
    let shared = Shared::default();
    let model = drive_frozen_legacy(&shared).await;
    assert_eq!(model.0.calls().len(), 1);
    let bytes = crate::canonical_request_bytes(&model.0.calls()[0]);
    assert!(String::from_utf8_lossy(&bytes).contains("first question"));
    assert!(!shared.records().iter().any(|record| matches!(
        record,
        WalRecord::CompactionNotice { .. } | WalRecord::CompactionMessage { .. }
    )));
}

#[tokio::test]
async fn compaction_v2_legacy_pending_view_due_is_dropped_without_changing_recorded_request_bytes()
{
    let historical = Shared::default();
    let (model, _) = run_history(&historical).await;
    let expected = crate::canonical_request_bytes(&model.0.calls()[0]);
    let records = historical.records();
    let finished = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let mut prefix = records[..finished].to_vec();
    for record in &mut prefix {
        if let WalRecord::CompactionMessage { view_due, .. } = record {
            *view_due = Some(
                serde_json::from_value(json!({"version":99,"digest":"a".repeat(64)})).unwrap(),
            );
        }
    }
    let original_notices = notice_seqs(&prefix);
    let frozen = Shared::with_records(prefix);
    let model = drive_frozen_legacy(&frozen).await;
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    assert_eq!(notice_seqs(&frozen.records()), original_notices);
}

#[tokio::test]
async fn compaction_v2_unfinished_old_plan_refuses_without_writes_but_completed_history_can_extend()
{
    let shared = Shared::default();
    drive(&shared, Arc::new(Plane::default())).await.0.unwrap();
    let mut records = shared.records();
    for record in &mut records {
        if let WalRecord::RunStarted {
            compaction_plan_version,
            ..
        } = record
        {
            // Version 2 is still supported; version 1 remains an unsupported renderer.
            *compaction_plan_version = Some(1);
        }
    }
    let pending = Shared::with_records(
        records
            .iter()
            .take_while(|record| !matches!(record, WalRecord::RunFinished { .. }))
            .cloned()
            .collect(),
    );
    let before = pending.records();
    let (refused, model) = drive(&pending, Arc::new(Plane::default())).await;
    assert!(refused
        .unwrap_err()
        .to_string()
        .contains("compaction_plan_version"));
    assert!(model.0.calls().is_empty());
    assert_eq!(pending.records(), before);
    let finished = Shared::with_records(records);
    let (result, model) = drive_as(
        &finished,
        Arc::new(Plane::default()),
        "next",
        crate::StartMode::AppendEpisode,
    )
    .await;
    result.unwrap();
    assert_eq!(model.0.calls().len(), 1);
    assert!(
        matches!(finished.records().iter().rev().find(|record| matches!(record, WalRecord::RunStarted { .. })).unwrap(), WalRecord::RunStarted { compaction_plan_version:Some(version), .. } if *version == cortexkit_compaction_view::PLAN_VERSION)
    );
}

#[tokio::test]
async fn compaction_v2_unavailable_setup_ends_before_the_model_and_retries_next_send() {
    let shared = Shared::default();
    let plane = Arc::new(Plane {
        unavailable: true,
        ..Default::default()
    });
    let (result, model) = drive(&shared, plane.clone()).await;
    assert_eq!(
        result.unwrap().reason,
        broca_protocol::RunFinishReason::Error
    );
    assert!(model.0.calls().is_empty());
    assert!(shared.records().iter().any(|record| matches!(record, WalRecord::RunFinished { error:Some(error), .. } if error.provider_code.as_deref() == Some("compaction_unavailable"))));
    assert_eq!(
        plane
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _)| method == "compaction.setup")
            .count(),
        1
    );
    let resumed = Shared::with_records(
        shared
            .records()
            .into_iter()
            .take_while(|record| !matches!(record, WalRecord::RunFinished { .. }))
            .collect(),
    );
    let ready = Arc::new(Plane::default());
    assert!(drive(&resumed, ready.clone()).await.0.is_ok());
    assert_eq!(
        ready
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _)| method == "compaction.setup")
            .count(),
        1
    );
}

async fn live_view() -> (Vec<WalRecord>, Vec<u8>) {
    let shared = Shared::default();
    let (result, model) = drive(
        &shared,
        Arc::new(Plane {
            view: Some(cut(1)),
            ..Default::default()
        }),
    )
    .await;
    result.unwrap();
    let calls = model.0.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].prompt.len(), 1);
    assert!(
        matches!(&calls[0].prompt[0].content[0], ContentBlock::Text { text } if text == "§3§ second question")
    );
    (shared.records(), crate::canonical_request_bytes(&calls[0]))
}

#[tokio::test]
async fn compaction_v2_crash_after_accepted_before_ack_applies_before_restart_setup() {
    let (records, expected) = live_view().await;
    let start = records
        .iter()
        .position(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap();
    let accepted = records
        .iter()
        .find(|record| matches!(record, WalRecord::CompactionViewV2(_)))
        .unwrap()
        .clone();
    let mut prefix = records[..=start].to_vec();
    prefix.push(accepted);
    let shared = Shared::with_records(prefix);
    let plane = Arc::new(Plane::default());
    let (result, model) = drive(&shared, plane.clone()).await;
    result.unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    let calls = plane.calls.lock().unwrap();
    let setup = &calls
        .iter()
        .find(|(method, _)| method == "compaction.setup")
        .unwrap()
        .1;
    assert_eq!(setup["applied_view"]["version"], 1);
}

#[tokio::test]
async fn compaction_v2_crash_after_ride_applied_before_request_reuses_identical_view() {
    let (records, expected) = live_view().await;
    let applied = records
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionViewAppliedV2 { .. }))
        .unwrap();
    let shared = Shared::with_records(records[..=applied].to_vec());
    let plane = Arc::new(Plane {
        view: Some(cut(2)),
        ..Default::default()
    });
    let (result, model) = drive(&shared, plane.clone()).await;
    result.unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    assert!(!plane
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(method, _)| method == "compaction.ride"));
}

#[tokio::test]
async fn compaction_v2_crash_after_marker_before_notice_reports_marker_after_setup() {
    let (records, expected) = live_view().await;
    let marker = records
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionMarker { .. }))
        .unwrap();
    let shared = Shared::with_records(records[..=marker].to_vec());
    let plane = Arc::new(Plane::default());
    let (result, model) = drive(&shared, plane.clone()).await;
    result.unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    let calls = plane.calls.lock().unwrap();
    assert_eq!(calls[0].0, "compaction.setup");
    let notice: NoticeRequest = serde_json::from_value(
        calls
            .iter()
            .find(|(method, _)| method == "compaction.notice")
            .unwrap()
            .1
            .clone(),
    )
    .unwrap();
    assert!(notice.events.iter().any(|event| matches!(
        event,
        v2::protocol::notice::Event::MarkerWritten { version: 1 }
    )));
}

/// A template's `part_tag` slot renders the number the session's next request shows: the
/// newest accepted view when it still fits, else the applied one. Message ids are mode-3 ids,
/// and a part is named by its entry part id or by its `<mid>#<id>` read key.
#[tokio::test]
async fn compaction_v2_next_request_numbers_follow_the_view_the_next_request_renders() {
    use v2::protocol::part::PartBody;
    let numbers_of = |records: &[WalRecord]| {
        let frames: Vec<_> = records
            .iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record: record.clone(),
            })
            .collect();
        let state = v2::session::load(&bind(), &frames).unwrap();
        let transcript =
            v2::transcript::project(&frames, &fixture_lineage_id().to_string()).unwrap();
        let (config, _) = v2::host::frozen(&frames).unwrap();
        let conditions = v2::host::conditions(&config, &transcript.entries, false, None).unwrap();
        (
            v2::shown::next_request(&state, &transcript.entries, &conditions).unwrap(),
            transcript.entries,
        )
    };
    let (mut records, _) = live_view().await;
    let (applied, entries) = numbers_of(&records);
    let question = entries
        .iter()
        .find(|entry| {
            entry.parts.iter().any(
                |part| matches!(&part.body, PartBody::Text { text } if text == "second question"),
            )
        })
        .unwrap();
    assert!(
        question.mid.contains('.'),
        "a mode-3 message id: {}",
        question.mid
    );
    let part = question.parts[0].id.clone();
    assert_eq!(v2::shown::lookup(&applied, &question.mid, &part), Some(3));
    assert_eq!(
        v2::shown::lookup(&applied, &question.mid, &format!("{}#{part}", question.mid)),
        Some(3)
    );
    let first = &entries[0];
    assert_eq!(
        v2::shown::lookup(&applied, &question.mid, &format!("{}#{part}", first.mid)),
        None,
        "a read key naming another message matches nothing"
    );
    assert_eq!(
        v2::shown::lookup(&applied, &first.mid, &first.parts[0].id),
        None,
        "the applied view cuts the first message"
    );

    // A newer accepted view without the cut renders at the next boundary, so its numbers count.
    let newer = View::seal(2, vec![]);
    records.push(WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
        version: 2,
        checksum: newer.checksum.clone(),
        edits: v2::storage::encode(None, &newer.into()),
    }));
    let (pending, _) = numbers_of(&records);
    assert_eq!(
        v2::shown::lookup(&pending, &first.mid, &first.parts[0].id),
        Some(1)
    );
    assert_eq!(v2::shown::lookup(&pending, &question.mid, &part), Some(3));
}

fn reported_prompt_events() -> Vec<broca_protocol::NormalizedEvent> {
    use broca_protocol::{NormalizedEvent, ProviderControl};
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("a".into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(700),
                cached_input_tokens: Some(200),
                cache_write_tokens: Some(100),
                output_tokens: Some(5),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

fn fallback_prompt_events() -> Vec<broca_protocol::NormalizedEvent> {
    use broca_protocol::{NormalizedEvent, ProviderControl};
    let mut events = reported_prompt_events();
    events.insert(
        1,
        NormalizedEvent::Control(ProviderControl::StepFinished {
            step_id: broca_protocol::StepId(0),
            usage: Usage {
                input_tokens: Some(700),
                cached_input_tokens: Some(200),
                cache_write_tokens: Some(100),
                output_tokens: Some(5),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            raw_finish_reason: None,
            service_tier_used: None,
            speed_used: None,
            served_model: Some("fallback-model".into()),
            usage_iterations: Some(
                serde_json::from_value(json!([
                    {"type":"message", "model":"model", "input_tokens":600,
                     "cache_read_input_tokens":300, "cache_creation_input_tokens":200,
                     "output_tokens":1},
                    {"type":"fallback_message", "model":"fallback-model", "input_tokens":700,
                     "cache_read_input_tokens":200, "cache_creation_input_tokens":100,
                     "output_tokens":5}
                ]))
                .unwrap(),
            ),
        }),
    );
    events
}

fn prompt_sizes(calls: &[(String, Value)]) -> Vec<Option<u64>> {
    calls
        .iter()
        .filter(|(method, _)| method == "compaction.notice" || method == "compaction.ride")
        .map(|(_, params)| params["conditions"]["last_prompt_tokens"].as_u64())
        .collect()
}

/// The provider measures a session's fill only from `conditions.last_prompt_tokens`. It is
/// absent before any request has reported its input, then every later notice and ride states
/// the newest reported prompt: uncached input plus cache reads plus cache writes.
#[tokio::test]
async fn compaction_v2_notices_and_rides_state_the_newest_reported_prompt_size() {
    let shared = Shared::default();
    let plane = Arc::new(Plane::default());
    let (first, _) = drive_events(
        &shared,
        plane.clone(),
        "run-1",
        crate::StartMode::FreshOrResume,
        reported_prompt_events(),
    )
    .await;
    first.unwrap();
    let before = plane.calls.lock().unwrap().len();
    let sizes = prompt_sizes(&plane.calls.lock().unwrap());
    assert!(!sizes.is_empty(), "the first run sends a notice");
    assert_eq!(sizes[0], None, "no request has reported its input yet");

    let (second, _) = drive_as(
        &shared,
        plane.clone(),
        "run-2",
        crate::StartMode::FreshOrResume,
    )
    .await;
    second.unwrap();
    let calls = plane.calls.lock().unwrap();
    let sizes = prompt_sizes(&calls[before..]);
    assert!(!sizes.is_empty(), "the second run sends a notice or ride");
    assert!(
        sizes.iter().all(|size| *size == Some(1_000)),
        "every later notice and ride states the newest reported prompt: {sizes:?}"
    );
}

/// A later step of the same run estimates its request from the previous step's measured input
/// plus only the bytes added since that step's request, as a resumed run does from the log. The
/// first request is about 60% of the window, and the first step reports that as its measured
/// input. Counting the whole body again on top of the measured input would put the second request
/// near 120% of the window: it would ride `over_limit` and then end at the context ceiling.
#[tokio::test]
async fn compaction_v2_a_later_step_estimates_from_the_bytes_added_since_the_measured_request() {
    let shared = Shared::default();
    let writer = shared.writer();
    let (mut request, mut control, _) = history_request();
    // About 60,000 tokens at the estimate's bytes per token.
    let prompt = "x".repeat(60_000 * crate::compaction::ESTIMATE_BYTES_PER_TOKEN as usize);
    request.input = vec![text(Role::User, &prompt)];
    request.context_limit = Some(100_000);
    request.generation.max_output_tokens = Some(10_000);
    control.admitted_context_limit = request.context_limit;
    request.tools = vec![serde_json::from_value(
        json!({"name":"edit","description":"edit","input_schema":{"type":"object"}}),
    )
    .unwrap()];
    request.policy_allowed.insert("edit".into());
    control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
    let plane = Arc::new(Plane::default());
    control.compaction_plane = Some(plane.clone());
    // Each step reports the attempt that sent it, as a real transport does: the estimate's
    // measured request is that attempt's body, about the prompt's size.
    let attempted = |body: usize, mut events: Vec<broca_protocol::NormalizedEvent>| {
        let attempt = broca_protocol::ModelAttempt {
            content_progress: Default::default(),
            attempt: 0,
            request_body_len: body as u64,
            request_body_sha256: "aa".into(),
            http_status: Some(200),
            content_type: None,
            response_body_len: 1,
            response_body_sha256: "bb".into(),
            response_body_cap: None,
            outcome: broca_protocol::AttemptOutcome::Complete,
            ts_ms: 0,
            sent_at_ms: None,
            transport: None,
            websocket_request: None,
            cache_evidence: None,
            websocket_close_code: None,
            input_transformations: None,
            input_transformations_unmeasured: false,
            credential_refresh: None,
            served_account_ref: None,
            account_observation: Default::default(),
        };
        events.insert(
            0,
            broca_protocol::NormalizedEvent::Control(
                broca_protocol::ProviderControl::AttemptFinished { attempt },
            ),
        );
        events
    };
    let model = PrefixBound(crate::StubProvider::new(vec![
        attempted(prompt.len(), tool_call_events("edit", 60_000)),
        attempted(prompt.len(), model_events()),
    ]));
    let tools = crate::StubToolPlane::new().with_tool("edit", |_| {
        crate::StubReply::Output(broca_protocol::ToolOutput::Text { text: "ok".into() })
    });
    let (out, _events) = tokio::sync::mpsc::channel(256);
    let summary = crate::run_loop_on_writer(
        request,
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &tools,
        &crate::AllowAll,
        &shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    writer.shutdown().await.unwrap();
    let calls = plane.calls.lock().unwrap();
    let over_limit: Vec<&Value> = calls
        .iter()
        .filter(|(method, params)| {
            method == "compaction.ride"
                && params["causes"]
                    .as_array()
                    .is_some_and(|causes| causes.contains(&json!("over_limit")))
        })
        .map(|(_, params)| &params["request_tokens"])
        .collect();
    assert!(
        over_limit.is_empty(),
        "no step's request is over the window: rides claimed over_limit at {over_limit:?}"
    );
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(summary.steps, 2, "both steps ran");
}

#[tokio::test]
async fn compaction_v2_crash_mid_step_does_not_adopt_a_newer_accepted_view() {
    let (records, expected) = live_view().await;
    let marker = records
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionMarker { .. }))
        .unwrap();
    let mut prefix = records[..=marker].to_vec();
    let newer = View::seal(2, vec![]);
    prefix.push(WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
        version: 2,
        checksum: newer.checksum.clone(),
        edits: v2::storage::encode(None, &newer.into()),
    }));
    let frames: Vec<_> = prefix
        .iter()
        .enumerate()
        .map(|(index, record)| FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record: record.clone(),
        })
        .collect();
    let shown = v2::shown::at_step(
        &frames,
        &bind(),
        &fixture_lineage_id().to_string(),
        "run",
        1,
    )
    .unwrap();
    assert_eq!(shown.keys().copied().collect::<Vec<_>>(), [3]);
    assert_eq!(shown[&3].payload_position, 2);
    let shared = Shared::with_records(prefix);
    let (result, model) = drive(&shared, Arc::new(Plane::default())).await;
    result.unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    assert!(!shared.records().iter().any(|record| matches!(
        record,
        WalRecord::CompactionViewAppliedV2 { version: 2, .. }
    )));
}

#[tokio::test]
async fn compaction_v2_no_plan_globals_are_frozen_outside_input_and_resume_byte_identically() {
    async fn send(shared: &Shared) -> PrefixBound {
        let writer = shared.writer();
        let (mut request, mut control, _) = history_request();
        request.input = vec![
            text(Role::System, "global instructions"),
            text(Role::User, "question"),
        ];
        control.manifest = None;
        control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
        let model = PrefixBound(crate::StubProvider::new(vec![model_events()]));
        let (out, _events) = tokio::sync::mpsc::channel(256);
        crate::run_loop_on_writer(
            request,
            &writer,
            crate::StartMode::FreshOrResume,
            &model,
            &crate::StubToolPlane::new(),
            &crate::AllowAll,
            shared,
            None,
            control,
            &out,
        )
        .await
        .unwrap();
        writer.shutdown().await.unwrap();
        model
    }
    let live = Shared::default();
    let model = send(&live).await;
    let calls = model.0.calls();
    assert_eq!(calls[0].prompt[0].role, Role::System);
    assert_eq!(calls[0].prompt[0].ordinal, None);
    assert_eq!(calls[0].prompt[1].ordinal, Some(0));
    let records = live.records();
    let WalRecord::RunStarted { input, config, .. } = records
        .iter()
        .find(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(input.len(), 1);
    assert_eq!(input[0].role, Role::User);
    assert_eq!(config.global_system.as_ref().unwrap()[0].role, Role::System);
    let before_model = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let resumed = Shared::with_records(records[..before_model].to_vec());
    let model = send(&resumed).await;
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        crate::canonical_request_bytes(&calls[0])
    );
}

#[tokio::test]
async fn compaction_v2_fallback_notices_and_rides_use_the_last_serving_prompt() {
    let shared = Shared::default();
    let plane = Arc::new(Plane::default());
    let (first, _) = drive_events(
        &shared,
        plane.clone(),
        "run-1",
        crate::StartMode::FreshOrResume,
        fallback_prompt_events(),
    )
    .await;
    first.unwrap();
    let before = plane.calls.lock().unwrap().len();
    let (second, _) = drive_as(
        &shared,
        plane.clone(),
        "run-2",
        crate::StartMode::FreshOrResume,
    )
    .await;
    second.unwrap();
    let sizes = prompt_sizes(&plane.calls.lock().unwrap()[before..]);
    assert!(!sizes.is_empty(), "the second run sends a notice or ride");
    assert!(
        sizes.iter().all(|size| *size == Some(1_000)),
        "fallback fill must use the last serving prompt, not billed iterations: {sizes:?}"
    );

    let mut reported = reported_prompt_events();
    for event in &mut reported {
        if let broca_protocol::NormalizedEvent::Control(
            broca_protocol::ProviderControl::Finished { usage, .. },
        ) = event
        {
            usage.input_tokens = Some(1_700);
        }
    }
    let before = plane.calls.lock().unwrap().len();
    let (third, _) = drive_events(
        &shared,
        plane.clone(),
        "run-3",
        crate::StartMode::FreshOrResume,
        reported,
    )
    .await;
    third.unwrap();
    let sizes = prompt_sizes(&plane.calls.lock().unwrap()[before..]);
    assert_eq!(sizes.first(), Some(&Some(1_000)));
    assert_eq!(
        sizes.last(),
        Some(&Some(2_000)),
        "a later step's own input usage replaces the older fallback fill"
    );
}

#[tokio::test]
async fn compaction_v2_fallback_preserves_session_entries_bytes() {
    let shared = Shared::default();
    let (result, _) = drive_events(
        &shared,
        Arc::new(Plane::default()),
        "run-1",
        crate::StartMode::FreshOrResume,
        fallback_prompt_events(),
    )
    .await;
    result.unwrap();
    let frames = shared.frames();
    let lineage = fixture_lineage_id().to_string();
    let entries = v2::transcript::project(&frames, &lineage).unwrap().entries;
    let assistant = entries.last().unwrap();
    assert_eq!(assistant.usage.as_ref().unwrap().prompt_tokens, Some(2_100));
    let mut billed_only = frames;
    for frame in &mut billed_only {
        if let WalRecord::ModelStepFinished { fallback, .. } = &mut frame.record {
            fallback.served_model = None;
            fallback.usage_iterations = None;
        }
    }
    let baseline = v2::transcript::project(&billed_only, &lineage).unwrap();
    assert_eq!(
        serde_json::to_vec(&entries).unwrap(),
        serde_json::to_vec(&baseline.entries).unwrap(),
        "fallback attribution must not change session.entries bytes or digests"
    );
}

#[tokio::test]
async fn compaction_v2_fallback_fill_survives_resume_after_step_completion() {
    let shared = Shared::default();
    let (result, _) = drive_events(
        &shared,
        Arc::new(Plane::default()),
        "run-1",
        crate::StartMode::FreshOrResume,
        fallback_prompt_events(),
    )
    .await;
    result.unwrap();
    let records = shared.records();
    let finished = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let recovered = Shared::with_records(records[..=finished].to_vec());
    let frames = recovered.frames();
    let transcript = v2::transcript::project(&frames, &fixture_lineage_id().to_string()).unwrap();
    let (config, _) = v2::host::frozen(&frames).unwrap();
    let offline = v2::host::conditions(
        &config,
        &transcript.entries,
        false,
        transcript.newest_step_fill(),
    )
    .unwrap();
    assert_eq!(offline.last_prompt_tokens, Some(1_000));
    let plane = Arc::new(Plane::default());
    let (result, model) = drive_as(
        &recovered,
        plane.clone(),
        "run-1",
        crate::StartMode::FreshOrResume,
    )
    .await;
    result.unwrap();
    assert!(
        model.0.calls().is_empty(),
        "the terminal step is already complete"
    );
    // Resume finishes the already-completed terminal step without a model request.
    // The next send must still announce the step's recorded fill.
    let (result, _) = drive_as(
        &recovered,
        plane.clone(),
        "run-2",
        crate::StartMode::FreshOrResume,
    )
    .await;
    result.unwrap();
    let calls = plane.calls.lock().unwrap();
    let sizes = prompt_sizes(&calls);
    assert!(!sizes.is_empty(), "resume sends a notice or ride");
    assert!(
        sizes.iter().all(|size| *size == Some(1_000)),
        "resumed fill must use the recorded last serving prompt: {sizes:?}"
    );
}

#[tokio::test]
async fn compaction_v2_every_result_kind_carries_delivery_without_changing_stored_output() {
    use crate::delivery_order::OrderedSend;
    use broca_protocol::{DeliveryMode, DeliveryOrder, SubmissionId, ToolOutput};
    let outputs = [
        ToolOutput::Text {
            text: "value".into(),
        },
        ToolOutput::Json {
            value: json!({"value":1}),
        },
        ToolOutput::Parts {
            parts: vec![broca_protocol::ToolOutputPart::Text {
                text: "value".into(),
            }],
        },
        ToolOutput::Parts {
            parts: vec![
                broca_protocol::ToolOutputPart::Text {
                    text: "value".into(),
                },
                broca_protocol::ToolOutputPart::Text {
                    text: "caption".into(),
                },
            ],
        },
        ToolOutput::Denied {
            reason: "denied".into(),
        },
        ToolOutput::error("failed"),
    ];
    for compaction in [true, false] {
        for output in outputs.iter().cloned() {
            let shared = Shared::default();
            let writer = shared.writer();
            let (mut request, mut control, _) = history_request();
            request.input = vec![text(Role::User, "question")];
            request.context_limit = Some(100_000);
            control.admitted_context_limit = request.context_limit;
            request.tools = vec![serde_json::from_value(
                json!({"name":"edit","description":"edit","input_schema":{"type":"object"}}),
            )
            .unwrap()];
            request.policy_allowed.insert("edit".into());
            control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
            control.compaction_plane = Some(Arc::new(Plane::default()));
            if !compaction {
                control.manifest = None;
            }
            let inbox = crate::SteerInbox::new();
            inbox.seed_ordered(vec![OrderedSend {
                admitted_at_ms: Some(1),
                submission_id: SubmissionId("reminder".into()),
                order: DeliveryOrder {
                    modes: vec![DeliveryMode::ToolResult],
                    dedupe_key: None,
                    expires_at_ms: None,
                    template: None,
                },
                principal: None,
                input: vec![text(Role::User, "module note")],
            }]);
            control.steer = Some(inbox);
            let model = PrefixBound(crate::StubProvider::new(vec![
                tool_call_events("edit", 1),
                model_events(),
            ]));
            let tool_output = output.clone();
            let tools = crate::StubToolPlane::new().with_tool("edit", move |_| {
                crate::StubReply::Output(tool_output.clone())
            });
            let (out, _events) = tokio::sync::mpsc::channel(256);
            crate::run_loop_on_writer(
                request,
                &writer,
                crate::StartMode::FreshOrResume,
                &model,
                &tools,
                &crate::AllowAll,
                &shared,
                None,
                control,
                &out,
            )
            .await
            .unwrap();
            writer.shutdown().await.unwrap();
            let calls = model.0.calls();
            assert_eq!(calls.len(), 2);
            assert!(serde_json::to_string(&calls[1].prompt)
                .unwrap()
                .contains("module note"));
            let records = shared.records();
            assert_eq!(
                records
                    .iter()
                    .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
                    .count(),
                1
            );
            assert!(records.iter().any(|record| matches!(record, WalRecord::ToolResult { result, .. } if result.output == output)));
            let frames: Vec<_> = records
                .iter()
                .enumerate()
                .map(|(index, record)| FramedRecord {
                    seq: index as u64 + 1,
                    fence: 1,
                    record: record.clone(),
                })
                .collect();
            let transcript =
                v2::transcript::project(&frames, &fixture_lineage_id().to_string()).unwrap();
            let tool = transcript
                .entries
                .iter()
                .flat_map(|entry| &entry.parts)
                .find_map(|part| match &part.body {
                    v2::protocol::part::PartBody::Tool(tool) => Some(tool),
                    _ => None,
                })
                .unwrap();
            assert_eq!(tool.delivered[0].text.as_deref(), Some("module note"));
            assert_eq!(tool.delivered[0].sender, "broca");
            assert!(!tool
                .output
                .as_deref()
                .unwrap_or_default()
                .contains("module note"));
            if !compaction {
                if matches!(&output, ToolOutput::Parts { parts } if parts.len() == 2) {
                    let mut resumed = records.clone();
                    for record in &mut resumed {
                        if let WalRecord::SendDelivered { ride, .. } = record {
                            *ride = Some(broca_wal::record::RideMetadata {
                                call_key: "recorded-carrying-key".into(),
                                send_id: "reminder".into(),
                                sender: "broca".into(),
                                forms: vec!["tool_result".into()],
                                form: broca_wal::record::RideForm::Append,
                            });
                        }
                    }
                    let at = resumed.iter().position(|record| matches!(record, WalRecord::ModelStepFinished { step_id, .. } if step_id.0 == 2)).unwrap();
                    resumed.insert(at, serde_json::from_value(json!({"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":1,"request_body_sha256":"recorded-digest","response_body_len":0,"response_body_sha256":"response","outcome":"complete","http_status":200,"ts_ms":10})).unwrap());
                    let rebuilt = crate::cache_warm::last_recorded_request_with_lineage(
                        &resumed,
                        Some(&fixture_lineage_id().to_string()),
                    )
                    .unwrap();
                    let result = rebuilt
                        .opts
                        .prompt
                        .iter()
                        .flat_map(|message| &message.content)
                        .find_map(|block| match block {
                            ContentBlock::ToolResult(result) => Some(result),
                            _ => None,
                        })
                        .unwrap();
                    assert_eq!(
                        result.output,
                        ToolOutput::Parts {
                            parts: vec![
                                broca_protocol::ToolOutputPart::Text {
                                    text: "value".into()
                                },
                                broca_protocol::ToolOutputPart::Text {
                                    text: "caption\n\nmodule note".into()
                                },
                            ]
                        }
                    );
                }
                assert!(
                    !String::from_utf8_lossy(&crate::canonical_request_bytes(&calls[1]))
                        .contains('§')
                );
                assert!(v2::shown::at_step(
                    &frames,
                    &bind(),
                    &fixture_lineage_id().to_string(),
                    "run",
                    2
                )
                .unwrap()
                .is_empty());
            }
        }
    }
}

#[tokio::test]
async fn compaction_v2_recorded_request_rebuild_uses_the_applied_view_not_a_later_acceptance() {
    let shared = Shared::default();
    let (_, model) = drive(
        &shared,
        Arc::new(Plane {
            view: Some(cut(1)),
            ..Default::default()
        }),
    )
    .await;
    let live = model.0.calls()[0].prompt.clone();
    let mut records = shared.records();
    let attempt_at = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let newer = View::seal(2, vec![]);
    records.insert(
        attempt_at,
        WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
            version: 2,
            checksum: newer.checksum.clone(),
            edits: v2::storage::encode(None, &newer.into()),
        }),
    );
    records.insert(attempt_at + 1, serde_json::from_value(json!({"type":"model_attempt_finished","step_id":1,"attempt":0,"request_body_len":1,"request_body_sha256":"recorded-digest","response_body_len":0,"response_body_sha256":"response","outcome":"complete","http_status":200,"ts_ms":10})).unwrap());
    let request = crate::cache_warm::last_recorded_request_with_lineage(
        &records,
        Some(&fixture_lineage_id().to_string()),
    )
    .unwrap();
    assert_eq!(request.opts.prompt, live);
    assert_eq!(request.request_body_sha256, "recorded-digest");
}

#[tokio::test]
async fn compaction_v2_missing_context_limit_refuses_fresh_admission_and_stops_an_invalid_resume() {
    async fn without_limit(
        shared: &Shared,
        mode3: bool,
    ) -> (
        Result<crate::RunSummary, crate::RunError>,
        PrefixBound,
        Arc<Plane>,
    ) {
        let writer = shared.writer();
        let (mut request, mut control, _) = history_request();
        request.context_limit = None;
        control.legacy_compaction_fixture = false;
        control.new_log_compaction_plan_version =
            mode3.then_some(cortexkit_compaction_view::PLAN_VERSION);
        let plane = Arc::new(Plane::default());
        control.compaction_plane = Some(plane.clone());
        let model = PrefixBound(crate::StubProvider::new(vec![model_events()]));
        let (out, _events) = tokio::sync::mpsc::channel(256);
        let result = crate::run_loop_on_writer(
            request,
            &writer,
            crate::StartMode::FreshOrResume,
            &model,
            &crate::StubToolPlane::new(),
            &crate::AllowAll,
            shared,
            None,
            control,
            &out,
        )
        .await;
        writer.shutdown().await.unwrap();
        (result, model, plane)
    }
    let fresh = Shared::default();
    let (result, model, plane) = without_limit(&fresh, true).await;
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        crate::RunError::CompactionContextLimitMissing
    ));
    assert_eq!(
        error.admission_error_class(),
        broca_protocol::ErrorClass::Permanent
    );
    assert!(fresh.records().is_empty());
    assert!(model.0.calls().is_empty());
    assert!(plane.calls.lock().unwrap().is_empty());
    let legacy = Shared::default();
    let (result, model, _) = without_limit(&legacy, false).await;
    result.unwrap();
    assert_eq!(
        model.0.calls().len(),
        1,
        "legacy admission must not acquire a v2 prerequisite"
    );
    let valid = Shared::default();
    drive(&valid, Arc::new(Plane::default())).await.0.unwrap();
    let mut prefix: Vec<_> = valid
        .records()
        .into_iter()
        .take_while(|record| !matches!(record, WalRecord::ModelStepFinished { .. }))
        .collect();
    for record in &mut prefix {
        if let WalRecord::RunStarted { config, .. } = record {
            config.context_limit = None;
        }
    }
    let invalid = Shared::with_records(prefix);
    let (result, model, plane) = without_limit(&invalid, true).await;
    assert_eq!(result.unwrap().reason, RunFinishReason::Error);
    assert!(model.0.calls().is_empty());
    assert!(plane.calls.lock().unwrap().is_empty());
    assert!(invalid.records().iter().any(|record| matches!(record, WalRecord::RunFinished { error:Some(error), .. } if error.provider_code.as_deref() == Some("compaction_unavailable") && error.message.contains("context_limit"))));
}

#[tokio::test]
async fn compaction_v2_context_folds_refreezes_and_tracks_accepted_attempts_before_step_completion()
{
    let shared = Shared::default();
    drive(&shared, Arc::new(Plane::default())).await.0.unwrap();
    let mut start = shared
        .records()
        .into_iter()
        .find(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap();
    if let WalRecord::RunStarted { config, .. } = &mut start {
        config.auth_selection = Some(routed_account_selection("old"));
    }
    let attempt: WalRecord = serde_json::from_value(json!({"type":"model_attempt_finished","step_id":1,"attempt":0,"request_body_len":1,"request_body_sha256":"request","response_body_len":0,"response_body_sha256":"response","outcome":"complete","http_status":200,"ts_ms":10})).unwrap();
    let mut next = start.clone();
    if let WalRecord::RunStarted { run_id, .. } = &mut next {
        *run_id = broca_protocol::RunId("next".into());
    }
    let mut records = vec![
        start,
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        attempt.clone(),
        serde_json::from_value(
            json!({"type":"run_finished","reason":"error","usage":{},"retries_used":0}),
        )
        .unwrap(),
        next,
        WalRecord::RunConfigRefrozen {
            ts_ms: Some(11),
            renderer_version: 1,
            frozen_render_config: json!({"service_tier":"priority"}),
        },
        WalRecord::RunAuthRefrozen {
            ts_ms: Some(12),
            auth_selection: routed_account_selection("new"),
        },
    ];
    let frames = |records: &[WalRecord]| {
        records
            .iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record: record.clone(),
            })
            .collect::<Vec<_>>()
    };
    let projected = frames(&records);
    let previous = v2::host::previous(&projected).unwrap();
    let (config, manifest) = v2::host::frozen(&projected).unwrap();
    let current = v2::host::context(&config, manifest);
    assert_ne!(previous.account, current.account);
    assert_eq!(current.model.service_tier.as_deref(), Some("fast"));
    assert_eq!(
        v2::conditions::ride_causes(
            Some(&previous),
            &current,
            false,
            false,
            0,
            &v2::host::conditions(&config, &[], false, None).unwrap()
        ),
        vec![
            v2::protocol::ride::RideCause::TierChange,
            v2::protocol::ride::RideCause::AccountChange
        ]
    );
    records.push(WalRecord::StepStarted {
        step_id: broca_protocol::StepId(1),
    });
    records.push(attempt);
    assert_eq!(v2::host::previous(&frames(&records)).unwrap(), current);
}

#[tokio::test]
async fn frozen_rides_live_request_and_restart_are_byte_identical_for_every_carrier() {
    use broca_protocol::{DeliveryMode, DeliveryOrder, SubmissionId, ToolOutput, ToolOutputPart};
    let cases = [
        (
            false,
            false,
            ToolOutput::Text {
                text: "value\n\n".into(),
            },
        ),
        (false, false, ToolOutput::error("failed")),
        (
            false,
            true,
            ToolOutput::Text {
                text: "first duplicate result\n\n".into(),
            },
        ),
        (
            true,
            false,
            ToolOutput::Json {
                value: json!({"value": 1}),
            },
        ),
        (
            true,
            false,
            ToolOutput::Parts {
                parts: vec![
                    ToolOutputPart::Text {
                        text: "value".into(),
                    },
                    ToolOutputPart::Text {
                        text: "caption\n\n".into(),
                    },
                ],
            },
        ),
    ];
    for (mode3, duplicate, output) in cases {
        let shared = Shared::default();
        let writer = Arc::new(shared.writer());
        writer.enable_ride_writer_for_test();
        let (mut request, mut control, _) = history_request();
        request.input = vec![text(Role::User, "question")];
        request.context_limit = Some(100_000);
        control.admitted_context_limit = request.context_limit;
        control.manifest = None;
        control.new_log_compaction_plan_version =
            mode3.then_some(cortexkit_compaction_view::PLAN_VERSION);
        request.tools = vec![serde_json::from_value(
            json!({"name":"edit","description":"edit","input_schema":{"type":"object"}}),
        )
        .unwrap()];
        request.policy_allowed.insert("edit".into());
        let inbox = crate::SteerInbox::new();
        control.steer = Some(inbox.clone());
        let queued_writer = writer.clone();
        let original = output.clone();
        let once = std::sync::atomic::AtomicBool::new(false);
        let tools = crate::StubToolPlane::new().with_tool("edit", move |_| {
            if !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let order = DeliveryOrder { modes: vec![DeliveryMode::ToolResult], dedupe_key: None, expires_at_ms: None, template: None };
                queued_writer.record(serde_json::from_value(json!({"type":"delivery_ordered","submission_id":"reminder","order":order,"sent_by":{"kind":"reserved","module_id":"sender"}})).unwrap());
                queued_writer.record(serde_json::from_value(json!({"type":"queued","submission_id":"reminder","send_id":"client-reminder","sent_by":{"kind":"reserved","module_id":"sender"},"request":{"model":{"provider_module_id":"fake","model_id":"fake"},"input":[{"role":"user","content":[{"type":"text","text":"module note"}]}]}})).unwrap());
                inbox.seed_ordered(vec![crate::delivery_order::OrderedSend { admitted_at_ms: Some(1), submission_id: SubmissionId("reminder".into()), order, principal: None, input: vec![text(Role::User, "module note")] }]);
            }
            crate::StubReply::Output(original.clone())
        });
        let mut first = tool_call_events("edit", 1);
        if duplicate {
            for event in &mut first {
                if let broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::AssistantMessage { message },
                ) = event
                {
                    message.content.push(message.content[0].clone());
                }
            }
        }
        let model = PrefixBound(crate::StubProvider::new(vec![first, model_events()]));
        let (out, _events) = tokio::sync::mpsc::channel(256);
        crate::run_loop_on_writer(
            request,
            &writer,
            crate::StartMode::FreshOrResume,
            &model,
            &tools,
            &crate::AllowAll,
            &shared,
            None,
            control,
            &out,
        )
        .await
        .unwrap();
        drop(tools);
        Arc::try_unwrap(writer)
            .ok()
            .expect("tool closure released writer")
            .shutdown()
            .await
            .unwrap();
        let calls = model.0.calls();
        assert_eq!(calls.len(), 2);
        let records = shared.records();
        assert!(records.iter().any(|record| matches!(record, WalRecord::SendDelivered { ride: Some(ride), .. } if ride.send_id == "client-reminder" && !ride.call_key.is_empty())));
        assert!(records.iter().any(|record| matches!(record, WalRecord::ToolResult { result, .. } if result.output == output)));
        let mut resumed = records;
        let at = resumed.iter().position(|record| matches!(record, WalRecord::ModelStepFinished { step_id, .. } if step_id.0 == 2)).unwrap();
        resumed.insert(at, serde_json::from_value(json!({"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":1,"request_body_sha256":"recorded-digest","response_body_len":0,"response_body_sha256":"response","outcome":"complete","http_status":200,"ts_ms":10})).unwrap());
        let rebuilt = crate::cache_warm::last_recorded_request_with_lineage(
            &resumed,
            Some(&fixture_lineage_id().to_string()),
        )
        .unwrap();
        assert_eq!(
            crate::canonical_request_bytes(&calls[1]),
            crate::canonical_request_bytes(&rebuilt.opts),
            "mode3={mode3} duplicate={duplicate} output={output:?}"
        );
    }
}
