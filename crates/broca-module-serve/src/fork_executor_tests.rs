use broca_module::executor::{ForkCallJob, ForkCallOutcome, ForkCallRefusal, ForkReleaseJob};

// The same RoutingTransport seam as the account-selection tests: only network IO is replaced.
struct ForkRouting {
    requests: Mutex<Vec<serde_json::Value>>,
    reply: broca_module_serve::account_routing::RoutingReply,
    release_fails: bool,
    /// Whether the router declares `account_fork_of`; otherwise its description is unknown.
    fork_of: bool,
}

#[async_trait]
impl broca_module_serve::account_routing::RoutingTransport for ForkRouting {
    async fn capability(
        &self,
        name: &str,
    ) -> broca_module_serve::routing_capabilities::CapabilitySupport {
        use broca_module_serve::routing_capabilities::CapabilitySupport;
        match (self.fork_of, name == cortexkit_role_routing::capabilities::ACCOUNT_FORK_OF) {
            (true, true) => CapabilitySupport::Declared,
            (true, false) => CapabilitySupport::NotDeclared,
            (false, _) => CapabilitySupport::Unknown,
        }
    }
    async fn service_tier_supported(&self) -> Result<bool, String> {
        Ok(true)
    }
    async fn request(&self, bytes: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        let request: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let observe = request["method"] == "route.account_observe";
        self.requests.lock().unwrap().push(request);
        if observe {
            if self.release_fails {
                Err("routing disconnected".into())
            } else {
                Ok(Ok(br#"{"result":{}}"#.to_vec()))
            }
        } else {
            self.reply.clone()
        }
    }
}

fn fork_routing(reply: broca_module_serve::account_routing::RoutingReply) -> Arc<ForkRouting> {
    Arc::new(ForkRouting {
        requests: Mutex::new(Vec::new()),
        reply,
        release_fails: false,
        fork_of: false,
    })
}

fn fork_chosen(
    account: &str,
    credential: &str,
    tier: Option<&str>,
) -> broca_module_serve::account_routing::RoutingReply {
    Ok(Ok(serde_json::to_vec(&serde_json::json!({"result": {
        "account":{"account_id":account,"credential_id":credential},
        "service_tier_chosen":tier
    }}))
    .unwrap()))
}

fn fork_job(root: &std::path::Path, url: &str) -> ForkCallJob {
    let req = drift_request(root);
    let broca_wal::WalRecord::RunStarted { config, .. } =
        run_started_with_user_text(&req.session, "head-run", "head")
    else {
        panic!()
    };
    let mut config = *config;
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    frozen.base_url = Some(url.into());
    config.model = broca_protocol::ModelHandle::new("anthropic", "claude-test");
    config.renderer_version = frozen.renderer_version;
    config.frozen_render_config = serde_json::to_value(&frozen).unwrap();
    config.auth_selection = Some(serde_json::from_value(serde_json::json!({
        "credential_id":"apikey:anthropic", "auth_method":"apikey", "account_ref":"head-account",
        "account_decision":{"path":"routed", "admission_id":"head-admission", "decision_id":"head-decision", "conversation_key":"head-key"}
    })).unwrap());
    config.account_route = Some(
        serde_json::from_value(
            serde_json::json!({"provider":"routing-provider", "model":"routing-model"}),
        )
        .unwrap(),
    );
    let opts: broca_protocol::CallOptions = serde_json::from_value(
        serde_json::json!({"prompt":[{"role":"user","content":[{"type":"text","text":"head"}]}]}),
    )
    .unwrap();
    let provider = broca_catalog::live::provider_for_recorded_config_with_quota_observer(
        "anthropic",
        "claude-test",
        frozen,
        &AuthMode::Secret("stand-in-token".into()),
        &Default::default(),
        Some(AuthMethod::ApiKey),
        Some("head-account"),
        None,
        build_provider_http_client(),
        &broca_core::clock::Clock::default(),
        None,
    )
    .unwrap();
    let digest =
        broca_core::render::request_digest_hex(&provider.try_render_request(&opts).unwrap());
    let mut request = opts.clone();
    request.prompt.push(
        serde_json::from_value(
            serde_json::json!({"role":"user","content":[{"type":"text","text":"fork question"}]}),
        )
        .unwrap(),
    );
    ForkCallJob {
        head: req.session,
        fork_id: "fork-test".into(),
        ask_id: "ask-test".into(),
        attempt: 1,
        run_id: "fork-run".into(),
        admission_id: "fork-admission".into(),
        conversation_key: Some("fork-key".into()),
        base: Box::new(broca_core::RecordedRequest {
            run_id: broca_protocol::RunId("head-run".into()),
            step_id: broca_protocol::StepId(1),
            config,
            opts,
            request_body_sha256: digest,
            route: None,
        }),
        request,
        on_usage: None,
        on_step: None,
    }
}

async fn fork_fixture(
    name: &str,
    router: Arc<ForkRouting>,
) -> (
    std::path::PathBuf,
    CapturingVaultDaemon,
    LiveLoopExecutor,
    ResumeHttpFixture,
) {
    let root =
        broca_test_support::scratch_root().join(format!("fork-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let vault = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_capturing_vault(&root, &vault)
        .await
        .with_routing(Arc::new(RoutingClient::new(router, Default::default())));
    let server = resume_http_fixture().await;
    (root, vault, executor, server)
}

macro_rules! fork_refusal_test {
    ($name:ident, $code:literal, $held:expr) => {
        #[tokio::test]
        async fn $name() {
            let router = fork_routing(Ok(Err(serde_json::to_vec(&serde_json::json!({"code":$code,"message":"refused"})).unwrap())));
            let (root, _vault, executor, server) = fork_fixture(stringify!($name), router.clone()).await;
            let result = executor.fork_call(fork_job(&root, &server.url)).await;
            let refusal = if $held { ForkCallRefusal::AccountHeld {routing_code:Some($code.into()), mismatch:None} }
                else { ForkCallRefusal::AccountBlocked {routing_code:$code.into()} };
            assert_eq!(result, ForkCallOutcome::Refused(refusal));
            assert_eq!(router.requests.lock().unwrap().len(), 1, "refusals hold no admission");
            assert!(server.requests.lock().unwrap().is_empty());
        }
    }
}
fork_refusal_test!(fork_account_hold, "account_hold", true);
fork_refusal_test!(fork_account_pool_exhausted, "account_pool_exhausted", true);
fork_refusal_test!(
    fork_account_selection_unavailable,
    "account_selection_unavailable",
    true
);
fork_refusal_test!(fork_account_auth_required, "auth_required", false);
fork_refusal_test!(
    fork_account_selection_disabled,
    "account_selection_disabled",
    false
);
fork_refusal_test!(fork_account_unknown_refusal, "future_refusal", false);

#[tokio::test]
async fn fork_routing_temporary_unavailable() {
    for reply in [
        Err("disconnected".into()),
        Ok(Ok(br#"{"result":{}}"#.to_vec())),
    ] {
        let router = fork_routing(reply);
        let (root, _vault, executor, server) = fork_fixture("temporary", router.clone()).await;
        assert_eq!(
            executor.fork_call(fork_job(&root, &server.url)).await,
            ForkCallOutcome::Refused(ForkCallRefusal::AccountHeld {
                routing_code: None,
                mismatch: None
            })
        );
        assert_eq!(router.requests.lock().unwrap().len(), 1);
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

macro_rules! fork_mismatch_test {
    ($name:ident, $account:literal, $credential:literal, $tier:expr, $mismatch:literal) => {
        #[tokio::test]
        async fn $name() {
            let router = fork_routing(fork_chosen($account, $credential, $tier));
            let (root, _vault, executor, server) =
                fork_fixture(stringify!($name), router.clone()).await;
            assert_eq!(
                executor.fork_call(fork_job(&root, &server.url)).await,
                ForkCallOutcome::Refused(ForkCallRefusal::AccountHeld {
                    routing_code: None,
                    mismatch: Some($mismatch.into())
                })
            );
            assert_fork_release(&router, 1);
            assert!(server.requests.lock().unwrap().is_empty());
        }
    };
}
fork_mismatch_test!(
    fork_account_mismatch_first,
    "other",
    "other-credential",
    Some("fast"),
    "account"
);
fork_mismatch_test!(
    fork_credential_mismatch_second,
    "head-account",
    "other",
    Some("fast"),
    "credential"
);
fork_mismatch_test!(
    fork_service_tier_mismatch,
    "head-account",
    "apikey:anthropic",
    Some("fast"),
    "service_tier"
);

fn assert_fork_release(router: &ForkRouting, admissions: usize) {
    let requests = router.requests.lock().unwrap();
    assert_eq!(requests.len(), admissions + 1);
    let release = &requests[admissions];
    assert_eq!(release["method"], "route.account_observe");
    assert_eq!(release["params"]["kind"], "run_finished");
    assert_eq!(release["params"]["admission_id"], "fork-admission");
    assert_eq!(release["params"]["run_id"], "fork-run");
}

#[tokio::test]
async fn fork_digest_refusal_releases_without_model_call() {
    let router = fork_routing(fork_chosen("head-account", "apikey:anthropic", None));
    let (root, _vault, executor, server) = fork_fixture("digest", router.clone()).await;
    let mut job = fork_job(&root, &server.url);
    job.base.request_body_sha256 = "not-the-recorded-digest".into();
    assert!(matches!(
        executor.fork_call(job).await,
        ForkCallOutcome::Refused(ForkCallRefusal::BaseUnavailable(_))
    ));
    assert_fork_release(&router, 1);
    assert!(server.requests.lock().unwrap().is_empty());
}

/// A release routing couldn't be told of stays owed. After the attempt, the fork manager asks
/// the executor once whether its release may not have reached routing (`fork_release_failed`).
/// The answer is yes, so the manager records no release and asks routing again when the fork next
/// opens. A second question answers no, since the first took the answer.
#[tokio::test]
async fn a_fork_release_routing_never_received_is_reported_once() {
    let router = Arc::new(ForkRouting {
        requests: Mutex::new(Vec::new()),
        reply: fork_chosen("head-account", "apikey:anthropic", None),
        release_fails: true,
        fork_of: false,
    });
    let (root, _vault, executor, server) = fork_fixture("unreleased", router.clone()).await;
    let mut job = fork_job(&root, &server.url);
    job.base.request_body_sha256 = "not-the-recorded-digest".into();
    assert!(matches!(
        executor.fork_call(job).await,
        ForkCallOutcome::Refused(ForkCallRefusal::BaseUnavailable(_))
    ));
    assert_fork_release(&router, 1);
    assert!(executor.fork_release_failed("fork-admission"));
    assert!(
        !executor.fork_release_failed("fork-admission"),
        "asking takes the answer"
    );
}

#[tokio::test]
async fn fork_restart_release_only_routed_and_reports_delivery_failure() {
    let router = fork_routing(fork_chosen("head-account", "apikey:anthropic", None));
    let (root, _vault, executor, server) = fork_fixture("restart", router.clone()).await;
    let config = fork_job(&root, &server.url).base.config;
    let job = |config| ForkReleaseJob {
        head: broca_protocol::BindIdentity::new(&root, "test", "session"),
        config,
        admission_id: "fork-admission".into(),
        run_id: "fork-run".into(),
    };
    executor.fork_release(job(config.clone())).await.unwrap();
    assert_fork_release(&router, 0);
    let mut static_config = config.clone();
    static_config
        .auth_selection
        .as_mut()
        .unwrap()
        .account_decision
        .as_mut()
        .unwrap()
        .path = "legacy_static".into();
    executor.fork_release(job(static_config)).await.unwrap();
    assert_fork_release(&router, 0);
    let failed = Arc::new(ForkRouting {
        requests: Mutex::new(Vec::new()),
        reply: Err("unused".into()),
        release_fails: true,
        fork_of: false,
    });
    let executor = executor.with_routing(Arc::new(RoutingClient::new(
        failed.clone(),
        Default::default(),
    )));
    assert!(executor.fork_release(job(config)).await.is_err());
    assert_fork_release(&failed, 0);
}

const FORK_LENGTH_REPLY: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"fork\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":3}}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"short\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":2}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n"
);
const FORK_TOOL_REPLY: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"fork\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":3}}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"before\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call\",\"name\":\"never_dispatch\",\"input\":{}}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"text\",\"text\":\"after\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":2}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":2}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n"
);

async fn fork_answer_fixture(
    name: &str,
    script: Option<&'static str>,
    expected_answer: &str,
    finish: &str,
) {
    let router = fork_routing(fork_chosen("head-account", "apikey:anthropic", None));
    let (root, _vault, executor, _server) = fork_fixture(name, router.clone()).await;
    let server = resume_http_fixture_with_body(script).await;
    let mut job = fork_job(&root, &server.url);
    let reports = Arc::new(Mutex::new(Vec::new()));
    let captured = reports.clone();
    job.on_usage = Some(Arc::new(move |usage| captured.lock().unwrap().push(usage)));
    let steps = Arc::new(Mutex::new(Vec::new()));
    let step_capture = steps.clone();
    job.on_step = Some(Arc::new(move |step| step_capture.lock().unwrap().push(step)));
    let result = executor.fork_call(job).await;
    // The provider's own account of the call came with every usage report, for the export.
    let steps = steps.lock().unwrap().clone();
    assert_eq!(steps.len(), reports.lock().unwrap().len());
    assert_eq!(
        steps.last().map(|step| step.usage.output_tokens),
        Some(Some(if script.is_some() { 2 } else { 1 }))
    );
    let ForkCallOutcome::Answered {
        answer,
        finish: actual_finish,
        usage,
    } = result
    else {
        panic!("{result:?}")
    };
    assert_eq!(answer, expected_answer);
    assert_eq!(actual_finish, finish);
    assert_eq!(
        usage.output_tokens,
        Some(if script.is_some() { 2 } else { 1 })
    );
    assert_eq!(reports.lock().unwrap().last(), Some(&usage));
    assert_fork_release(&router, 1);
    let routing = router.requests.lock().unwrap();
    let params = &routing[0]["params"];
    assert_eq!(params["provider"], "routing-provider");
    assert_eq!(params["model"], "routing-model");
    assert_eq!(params["affinity_model"], "routing-model");
    assert_eq!(
        params["requested_account"],
        serde_json::json!({"account_id":"head-account", "credential_id":"apikey:anthropic"})
    );
    assert_eq!(params["conversation_key"], "fork-key");
    assert_eq!(params["admission_id"], "fork-admission");
    assert_eq!(params["hold_wait_max_s"], 0);
    assert!(params.get("decision_id").is_none());
    assert!(params.get("parent_conversation_key").is_none());
    assert!(params.get("pool").is_none());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "exactly one fork step");
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert!(body.to_string().contains("fork question"));
    assert!(
        !executor
            .wal_store
            .wal_path(&broca_protocol::BindIdentity::new(&root, "test", "session"))
            .exists(),
        "fork execution writes no head WAL"
    );
}

#[tokio::test]
async fn fork_finish_end_turn() {
    fork_answer_fixture("stop", None, "done", "end_turn").await;
}

#[tokio::test]
async fn fork_finish_max_tokens() {
    fork_answer_fixture("length", Some(FORK_LENGTH_REPLY), "short", "max_tokens").await;
}

#[tokio::test]
async fn fork_finish_tool_call_refused_only_text_before_call() {
    fork_answer_fixture("tool", Some(FORK_TOOL_REPLY), "before", "tool_call_refused").await;
}

/// A router declaring `account_fork_of` is asked for the head admission's account by that
/// admission's id, never for an account by name: the contract refuses `requested_account`
/// beside `fork_of`, and only the admission tells routing which account the head moved to.
#[tokio::test]
async fn fork_names_the_head_admission_when_routing_declares_fork_of() {
    let router = Arc::new(ForkRouting {
        requests: Mutex::new(Vec::new()),
        reply: fork_chosen("head-account", "apikey:anthropic", None),
        release_fails: false,
        fork_of: true,
    });
    let (root, _vault, executor, server) = fork_fixture("fork-of", router.clone()).await;
    assert!(matches!(
        executor.fork_call(fork_job(&root, &server.url)).await,
        ForkCallOutcome::Answered { .. }
    ));
    let routing = router.requests.lock().unwrap();
    let params = &routing[0]["params"];
    assert_eq!(params["fork_of"], serde_json::json!({"admission_id": "head-admission"}));
    assert!(params.get("requested_account").is_none(), "{params}");
    assert_eq!(params["admission_id"], "fork-admission");
    assert_eq!(params["mode"], "admit");
    let checked = cortexkit_role_routing::account::parse_account_for_request_serving(
        params,
        &[cortexkit_role_routing::capabilities::ACCOUNT_FORK_OF.to_owned()],
    );
    assert!(checked.is_ok(), "routing's own parser refuses it: {checked:?}");
}

#[tokio::test]
async fn fork_static_account_skips_routing() {
    let router = fork_routing(Err("must not route".into()));
    let (root, _vault, executor, server) = fork_fixture("static", router.clone()).await;
    let mut job = fork_job(&root, &server.url);
    job.base
        .config
        .auth_selection
        .as_mut()
        .unwrap()
        .account_decision
        .as_mut()
        .unwrap()
        .path = "legacy_static".into();
    assert!(matches!(
        executor.fork_call(job).await,
        ForkCallOutcome::Answered { .. }
    ));
    assert!(router.requests.lock().unwrap().is_empty());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn fork_credential_failure_is_zero_usage_and_releases() {
    let router = fork_routing(fork_chosen("head-account", "apikey:anthropic", None));
    let (root, _vault, mut executor, server) =
        fork_fixture("credential-failure", router.clone()).await;
    executor.vault = None;
    let outcome = executor.fork_call(fork_job(&root, &server.url)).await;
    assert!(
        matches!(outcome, ForkCallOutcome::ModelFailed {usage, ..} if usage == Default::default())
    );
    assert_fork_release(&router, 1);
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fork_unsendable_config_is_unsupported_and_releases() {
    let router = fork_routing(fork_chosen("head-account", "apikey:anthropic", None));
    let (root, _vault, executor, server) = fork_fixture("unsupported", router.clone()).await;
    let mut job = fork_job(&root, &server.url);
    job.base.config.frozen_render_config = serde_json::Value::Null;
    assert!(matches!(
        executor.fork_call(job).await,
        ForkCallOutcome::Refused(ForkCallRefusal::Unsupported(_))
    ));
    assert_fork_release(&router, 1);
    assert!(server.requests.lock().unwrap().is_empty());
}

struct ForkUsageErrorProvider;

#[async_trait]
impl broca_core::Provider for ForkUsageErrorProvider {
    fn render_config_blob(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
    fn renderer_version(&self) -> u32 {
        1
    }
    async fn step(
        &self,
        id: broca_protocol::StepId,
        _: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<broca_protocol::NormalizedEvent> {
        use broca_protocol::{NormalizedEvent, ProviderControl};
        let (tx, rx) = mpsc::channel(3);
        tx.send(NormalizedEvent::Control(ProviderControl::StepFinished {
            step_id: id,
            usage: serde_json::from_value(serde_json::json!({"input_tokens":5,"output_tokens":2}))
                .unwrap(),
            finish_reason: broca_protocol::FinishReason::Stop,
            raw_finish_reason: None,
            service_tier_used: None,
            speed_used: None,
            served_model: None,
            usage_iterations: None,
        }))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(ProviderControl::Error {
            error: serde_json::from_value(
                serde_json::json!({"class":"permanent","message":"synthetic failure"}),
            )
            .unwrap(),
            retries_used: 0,
        }))
        .await
        .unwrap();
        rx
    }
}

#[tokio::test]
async fn fork_model_error_keeps_reported_step_usage() {
    let root = broca_test_support::scratch_root();
    let mut job = fork_job(&root, "http://unused.invalid");
    let reports = Arc::new(Mutex::new(Vec::new()));
    let capture = reports.clone();
    job.on_usage = Some(Arc::new(move |usage| capture.lock().unwrap().push(usage)));
    let result = fork_step(&ForkUsageErrorProvider, &job).await;
    let ForkCallOutcome::ModelFailed { usage, error } = result else {
        panic!("{result:?}")
    };
    assert_eq!(usage.input_tokens, Some(5));
    assert_eq!(usage.output_tokens, Some(2));
    assert_eq!(error, "synthetic failure");
    assert_eq!(*reports.lock().unwrap(), vec![usage]);
}

fn fork_reply_message(blocks: Vec<broca_protocol::content::ContentBlock>) -> broca_protocol::content::AssistantMessage {
    broca_protocol::content::AssistantMessage {
        message_id: broca_protocol::MessageId("fork-reply".into()),
        content: blocks,
    }
}

fn fork_reply_text(text: &str) -> broca_protocol::content::ContentBlock {
    broca_protocol::content::ContentBlock::Text { text: text.into() }
}

fn fork_reply_call() -> broca_protocol::content::ContentBlock {
    broca_protocol::content::ContentBlock::ToolCall(broca_protocol::content::ToolCall {
        tool_call_id: broca_protocol::ToolCallId("call-1".into()),
        tool_name: "forbidden".into(),
        input: serde_json::json!({}).into(),
        thought_signature: None,
    })
}

fn fork_reply_answer(outcome: ForkCallOutcome) -> (String, String) {
    match outcome {
        ForkCallOutcome::Answered { answer, finish, .. } => (answer, finish),
        ForkCallOutcome::ModelFailed { .. } => (String::new(), "model_error".into()),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_fork_reply_without_a_named_stop_reason_is_a_completed_answer() {
    use broca_protocol::FinishReason;
    let reply = fork_reply_message(vec![fork_reply_text("the answer")]);
    let answer = |reason, raw: Option<&str>| {
        fork_reply_answer(fork_reply(
            &reply,
            reason,
            raw,
            &StreamedText::default(),
            Default::default(),
        ))
    };
    // A chat stream that ended with a token the family doesn't name, or with none, as a head
    // run would take it.
    for raw in [Some("end_turn"), Some("pause_turn"), None] {
        assert_eq!(
            answer(FinishReason::Other, raw),
            ("the answer".into(), "end_turn".into()),
            "{raw:?}"
        );
    }
    assert_eq!(answer(FinishReason::Stop, Some("stop")).1, "end_turn");
    assert_eq!(answer(FinishReason::Length, Some("length")).1, "max_tokens");
    // Failed and refused calls stay failures, named or riding on `Other`.
    for (reason, raw) in [
        (FinishReason::ContentFilter, Some("content_filter")),
        (FinishReason::Error, None),
        (FinishReason::ToolCalls, Some("MALFORMED_FUNCTION_CALL")),
        (FinishReason::Other, Some("failed")),
        (FinishReason::Other, Some("cancelled")),
        (FinishReason::Other, Some("IMAGE_SAFETY")),
        (FinishReason::Other, Some("IMAGE_PROHIBITED_CONTENT")),
        (FinishReason::Other, Some("IMAGE_RECITATION")),
    ] {
        assert_eq!(
            answer(reason, raw),
            (String::new(), "model_error".into()),
            "{reason:?} {raw:?}"
        );
    }
}

#[test]
fn a_refused_tool_call_answers_with_the_text_streamed_before_it() {
    use broca_protocol::display::DisplayEvent;
    use broca_protocol::FinishReason;
    let text = |delta: &str| DisplayEvent::TextDelta {
        mid: None,
        part: None,
        message_id: broca_protocol::MessageId("fork-reply".into()),
        tool_call_id: None,
        delta: delta.into(),
    };
    let input = DisplayEvent::ToolInputDelta {
        mid: None,
        part: None,
        tool_call_id: broca_protocol::ToolCallId("call-1".into()),
        tool_name: None,
        delta: "{}".into(),
    };
    // OpenAI Chat assembles every text of the reply into one block ahead of its calls.
    let chat = fork_reply_message(vec![fork_reply_text("before toolafter tool"), fork_reply_call()]);
    let stream = |events: &[DisplayEvent]| {
        let mut streamed = StreamedText::default();
        for event in events {
            streamed.observe(event);
        }
        streamed
    };
    let answer = |message, streamed: &StreamedText| {
        fork_reply_answer(fork_reply(
            message,
            FinishReason::Other,
            None,
            streamed,
            Default::default(),
        ))
    };
    let ordered = stream(&[text("before tool"), input.clone(), text("after tool")]);
    assert_eq!(
        answer(&chat, &ordered),
        ("before tool".into(), "tool_call_refused".into())
    );
    // Without the complete streamed order, the assembled text ahead of the first call.
    let gapped = stream(&[
        text("before tool"),
        input.clone(),
        DisplayEvent::DisplayGap { dropped: 1 },
    ]);
    assert_eq!(answer(&chat, &gapped).0, "before toolafter tool");
    assert_eq!(answer(&chat, &StreamedText::default()).0, "before toolafter tool");
    // Formats that keep block order answer the same either way.
    let ordered_blocks = fork_reply_message(vec![
        fork_reply_text("before tool"),
        fork_reply_call(),
        fork_reply_text("after tool"),
    ]);
    assert_eq!(answer(&ordered_blocks, &ordered).0, "before tool");
    assert_eq!(answer(&ordered_blocks, &StreamedText::default()).0, "before tool");
}
