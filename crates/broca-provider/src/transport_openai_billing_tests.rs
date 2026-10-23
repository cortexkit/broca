// A whole run against the OpenAI Responses API whose credential has no prepaid credit left. The
// run must end `billing_exhausted`, permanent, naming the frozen credential, so routing's
// credential fence can stop sending work to that key. An ordinary rate limit must still retry.
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

const FROZEN_CREDENTIAL: &str = "apikey:openai:frozen";

const NO_CREDITS_MESSAGE: &str = "You have no credits remaining. Add credits to continue using the API at https://platform.openai.com/settings/organization/billing/.";

/// The in-band error event OpenAI's Responses API streams when the organization's prepaid
/// balance is empty.
const NO_CREDITS_STREAM: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_no_credits\"}}\n\n",
    "data: {\"type\":\"error\",\"code\":\"credit_balance_exhausted\",\"message\":\"You have no credits remaining. Add credits to continue using the API at https://platform.openai.com/settings/organization/billing/.\",\"param\":null,\"sequence_number\":1}\n\n",
);

/// The same refusal as an HTTP error body, with OpenAI's `insufficient_quota` error type.
const NO_CREDITS_BODY: &str = r#"{"error":{"message":"You have no credits remaining. Add credits to continue using the API at https://platform.openai.com/settings/organization/billing/.","type":"insufficient_quota","param":null,"code":"credit_balance_exhausted"}}"#;

const RATE_LIMIT_BODY: &str = r#"{"error":{"message":"Rate limit reached for gpt-4.1 on requests per min (RPM): Limit 500, Used 500, Requested 1. Please try again in 120ms.","type":"requests","param":null,"code":"rate_limit_exceeded"}}"#;

struct OpenAiRun {
    live: ProviderError,
    durable: ProviderError,
    attempts: usize,
    waits: Vec<Duration>,
    retries_used: u32,
}

async fn run_openai(label: &str, script: Vec<ScriptResponse>) -> OpenAiRun {
    let server = ScriptedServer::start(script).await;
    let mut frozen = openai_responses_frozen_config("gpt-4.1");
    frozen.base_url = Some(server.url.clone());
    let waits = Arc::new(Mutex::new(Vec::new()));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut provider = LiveProvider::with_client_and_clock(
        client.into(),
        OpenAiResponses,
        frozen,
        AuthHandle::Bearer("test-key".into()),
        broca_core::Clock::default(),
    );
    provider.retry_clock.virtual_waits = Some(Arc::clone(&waits));
    let dir = broca_test_support::scratch_root().join(format!(
        "openai-billing-{label}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("project")).unwrap();
    let session = BindIdentity::new(dir.join("project"), "openai", label);
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
        run_id: RunId(format!("openai-billing-{label}")),
        session: session.clone(),
        model: ModelHandle::new("openai", "gpt-4.1"),
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
    .expect("refused run must finish")
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
    let outcome = OpenAiRun {
        live,
        durable,
        attempts: server.request_bodies().len(),
        waits: waits.lock().unwrap().clone(),
        retries_used: summary.retries_used,
    };
    std::fs::remove_dir_all(dir).unwrap();
    outcome
}

fn assert_billing_exhausted_on_one_attempt(outcome: &OpenAiRun) {
    assert_eq!(outcome.attempts, 1, "billing exhaustion is never re-sent");
    assert!(outcome.waits.is_empty());
    assert_eq!(outcome.retries_used, 0);
    assert_eq!(outcome.live, outcome.durable);
    assert_eq!(outcome.live.provider_code.as_deref(), Some(BILLING_EXHAUSTED));
    assert_eq!(outcome.live.class, ErrorClass::Permanent);
    assert_eq!(outcome.live.credential_id.as_deref(), Some(FROZEN_CREDENTIAL));
    assert!(
        outcome.live.message.contains(NO_CREDITS_MESSAGE),
        "{}",
        outcome.live.message
    );
    assert_eq!(outcome.live.resets_at_ms, None);
}

#[tokio::test]
async fn openai_stream_credit_balance_exhausted_ends_billing_exhausted_with_the_frozen_credential()
{
    // Every scripted answer is the same refusal, so a retry would show up as a second request.
    let outcome = run_openai(
        "stream",
        vec![
            ScriptResponse::Http {
                status: 200,
                headers: vec![("content-type", "text/event-stream")],
                body: NO_CREDITS_STREAM.as_bytes(),
            };
            6
        ],
    )
    .await;
    assert_billing_exhausted_on_one_attempt(&outcome);
    assert_eq!(outcome.live.status, None);
}

#[tokio::test]
async fn openai_http_credit_balance_exhausted_ends_billing_exhausted_with_the_frozen_credential() {
    let outcome = run_openai(
        "http",
        vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("content-type", "application/json")],
                body: NO_CREDITS_BODY.as_bytes(),
            };
            6
        ],
    )
    .await;
    assert_billing_exhausted_on_one_attempt(&outcome);
    assert_eq!(outcome.live.status, Some(429));
}

#[tokio::test]
async fn openai_rate_limit_exceeded_429_still_retries() {
    let outcome = run_openai(
        "rate-limit",
        vec![
            ScriptResponse::Http {
                status: 429,
                headers: vec![("content-type", "application/json")],
                body: RATE_LIMIT_BODY.as_bytes(),
            };
            6
        ],
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
