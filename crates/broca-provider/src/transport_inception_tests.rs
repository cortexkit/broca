use super::*;
use broca_core::{
    run_loop_with_control, AllowAll, AuthSelection, FileLeaseStore, FileWalStore, OutputEvent,
    RunControl, RunRequest, StubToolPlane, WalStore,
};
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    BindIdentity, GenerationConfig, RunId,
};
use broca_wal::WalRecord;

const FROZEN_CREDENTIAL: &str = "apikey:inception:frozen";

struct RefusalRun {
    live: ProviderError,
    durable: ProviderError,
    attempts: usize,
    waits: Vec<Duration>,
    retries_used: u32,
}

async fn run_refusal(host: &str, status: u16, body: &'static str) -> RefusalRun {
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status,
            headers: vec![("Content-Type", "application/json")],
            body: body.as_bytes(),
        };
        6
    ])
    .await;
    let address = server.url.strip_prefix("http://").unwrap().parse().unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .resolve(host, address)
        .build()
        .unwrap();
    let mut url = reqwest::Url::parse(&server.url).unwrap();
    // Keep the production host in the request URL while sending only to the local fixture.
    url.set_host(Some(host)).unwrap();
    let mut frozen = openai_chat_frozen_config("mercury-2");
    frozen.base_url = Some(url.to_string());
    let waits = Arc::new(Mutex::new(Vec::new()));
    let mut provider = LiveProvider::with_client_and_clock(
        client.into(),
        OpenAiChat,
        frozen,
        AuthHandle::Bearer("test-key".into()),
        broca_core::Clock::default(),
    );
    provider.retry_clock.virtual_waits = Some(Arc::clone(&waits));
    let dir = broca_test_support::scratch_root().join(format!(
        "inception-refusal-{host}-{status}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("project")).unwrap();
    let session = BindIdentity::new(dir.join("project"), "inception", "refusal");
    let request = RunRequest {
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("inception-refusal".into()),
        session: session.clone(),
        model: ModelHandle::new("inception", "mercury-2"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "hi".into() }],
        }],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    };
    let control = RunControl {
        auth_selection: Some(AuthSelection {
            credential_id: FROZEN_CREDENTIAL.into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        }),
        ..RunControl::default()
    };
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });
    let summary = tokio::time::timeout(
        Duration::from_secs(10),
        run_loop_with_control(
            request,
            &provider,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            None,
            control,
            &tx,
        ),
    )
    .await
    .expect("refusal run must finish")
    .expect("run loop");
    drop(tx);
    assert_eq!(summary.reason, RunFinishReason::Error);
    let events = collector.await.unwrap();
    let live = events
        .into_iter()
        .find_map(|event| match event {
            OutputEvent::Control(ControlUnit::Error { error }) => Some(error.as_inner().clone()),
            _ => None,
        })
        .expect("live terminal error");
    let durable = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .find_map(|frame| match frame.record {
            WalRecord::RunFinished { error, .. } => error,
            _ => None,
        })
        .expect("durable terminal error");
    let outcome = RefusalRun {
        live,
        durable,
        attempts: server.request_bodies().len(),
        waits: waits.lock().unwrap().clone(),
        retries_used: summary.retries_used,
    };
    std::fs::remove_dir_all(dir).unwrap();
    outcome
}

#[tokio::test]
async fn inception_402_ends_the_run_with_the_frozen_credential() {
    let outcome = run_refusal("api.inceptionlabs.ai", 402, INCEPTION_ACCOUNT_INACTIVE_BODY).await;
    assert_eq!(outcome.attempts, 1);
    assert!(outcome.waits.is_empty());
    assert_eq!(outcome.retries_used, 0);
    assert_eq!(outcome.live, outcome.durable);
    assert_eq!(outcome.live.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
    assert_eq!(outcome.live.class, ErrorClass::Permanent);
    assert_eq!(outcome.live.status, Some(402));
    assert_eq!(outcome.live.credential_id.as_deref(), Some(FROZEN_CREDENTIAL));
    assert!(outcome.live.message.contains(INCEPTION_ACCOUNT_INACTIVE_BODY));
    assert_eq!(outcome.live.resets_at_ms, None);
}

#[tokio::test]
async fn another_openai_compatible_hosts_402_remains_an_unscoped_account_error() {
    let outcome = run_refusal("api.example.com", 402, INCEPTION_ACCOUNT_INACTIVE_BODY).await;
    assert_eq!(outcome.attempts, 1);
    assert!(outcome.waits.is_empty());
    assert_eq!(outcome.retries_used, 0);
    assert_eq!(outcome.live, outcome.durable);
    assert_eq!(outcome.live.provider_code.as_deref(), Some("account_error"));
    assert_eq!(outcome.live.class, ErrorClass::Permanent);
    assert_eq!(outcome.live.status, Some(402));
    assert_eq!(outcome.live.credential_id, None);
    assert!(outcome.live.message.contains(INCEPTION_ACCOUNT_INACTIVE_BODY));
}

#[tokio::test]
async fn inception_429_still_retries_as_a_rate_limit() {
    let outcome = run_refusal(
        "api.inceptionlabs.ai",
        429,
        r#"{"error":{"message":"Rate limit exceeded","type":"rate_limit_error","code":"rate_limit_error"}}"#,
    )
    .await;
    assert_eq!(outcome.attempts, 6);
    assert_eq!(outcome.waits.len(), 5);
    assert_eq!(outcome.retries_used, 5);
    assert_eq!(outcome.live, outcome.durable);
    assert_eq!(outcome.live.provider_code.as_deref(), Some("rate_limited"));
    assert_eq!(outcome.live.class, ErrorClass::Transient);
    assert_eq!(outcome.live.status, Some(429));
    assert_eq!(outcome.live.credential_id.as_deref(), Some(FROZEN_CREDENTIAL));
}
