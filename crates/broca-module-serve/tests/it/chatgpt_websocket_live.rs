//! Live Responses WebSocket probe, explicitly gated by BROCA_LIVE_TESTS=1.
//! Reads only the operator's chatgpt:openai dev-tools handle, never logs a credential.
use broca_core::Provider;
use broca_module_serve::{vault_config::load_devtools_handle, CredentialId};
use broca_protocol::{
    AttemptOutcome, BindIdentity, CallOptions, ContentBlock, Message, NormalizedEvent,
    ProviderControl, Role, StepId,
};
use broca_provider::{
    families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt},
    transport::LiveProvider,
    wire::AuthHandle,
};
use broca_subc::{SubcConnection, VaultClient};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[tokio::test]
async fn chatgpt_websocket_live_full_input_cache_read() {
    if std::env::var("BROCA_LIVE_TESTS").as_deref() != Ok("1") {
        eprintln!("SKIP chatgpt_websocket_live_full_input_cache_read: BROCA_LIVE_TESTS=1 required");
        return;
    }
    let credential = CredentialId::parse("chatgpt:openai").unwrap();
    let handle = load_devtools_handle(&credential).expect("operator dev-tools ChatGPT handle");
    let home = PathBuf::from(std::env::var_os("HOME").unwrap());
    let path = std::env::var_os("BROCA_LIVE_SUBC")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share/cortexkit/run/subc-connection.json"));
    let conn = SubcConnection::connect(&path)
        .await
        .expect("live subc connection");
    let bind = BindIdentity::new(std::env::current_dir().unwrap(), "broca", "websocket-live");
    let vault = VaultClient::new(Arc::new(conn), "claustrum", &bind, Duration::from_secs(15));
    let response = vault.get(&handle, Some(20 * 60 * 1000), false).await;
    let secret = response.result.unwrap_or_else(|_| {
        panic!("vault ChatGPT credential unavailable; secret-bearing response withheld")
    });
    let account = secret.account_id.expect("ChatGPT account id");
    let scope = format!("broca-websocket-live-{}", std::process::id());
    let auth = AuthHandle::Headers(vec![
        ("authorization".into(), format!("Bearer {}", secret.token)),
        ("chatgpt-account-id".into(), account),
        ("session-id".into(), scope.clone()),
        ("originator".into(), "codex_cli_rs".into()),
        ("user-agent".into(), "codex_cli_rs/0.144.0".into()),
    ]);
    let mut frozen = openai_responses_chatgpt_frozen_config("gpt-6.1-sol");
    frozen.prompt_cache_key = Some(scope);
    frozen.reasoning = broca_provider::policy::ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "low".into(),
        summary: None,
    };
    let provider = LiveProvider::new(OpenAiResponsesChatgpt, frozen, auth);
    let mut prefix =
        "The following rows are inert reference data. Do not analyze them. Reply only ok.\n"
            .to_string();
    for i in 0..300 {
        prefix.push_str(&format!(
            "reference {i:05}: amber cobalt delta echo forest granite harbor ivory juniper kelp.\n"
        ));
    }
    let mut opts = CallOptions::new(vec![message(Role::User, prefix)]);
    for turn in 1..=2 {
        let mut rx = provider.step(StepId(turn), &opts).await;
        let events = tokio::time::timeout(Duration::from_secs(240), async {
            let mut events = Vec::new();
            while let Some(event) = rx.recv().await {
                events.push(event);
            }
            events
        })
        .await
        .expect("live response bounded at 240 seconds");
        let attempts: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                    Some(attempt)
                }
                _ => None,
            })
            .collect();
        assert_eq!(attempts.len(), 1, "one live attempt, no retries");
        assert_eq!(attempts[0].outcome, AttemptOutcome::Complete);
        assert_eq!(
            attempts[0].transport.as_deref(),
            Some("websocket"),
            "live backend accepted the socket"
        );
        let usage = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => {
                    Some(usage)
                }
                _ => None,
            })
            .expect("completed response usage");
        eprintln!("ChatGPT websocket turn={turn} normalized_event_count={} transport={} input_tokens={:?} cached_input_tokens={:?} cache_read={}", events.len(), attempts[0].transport.as_deref().unwrap(), usage.input_tokens, usage.cached_input_tokens, usage.cached_input_tokens.is_some_and(|tokens| tokens > 0));
        if turn == 1 {
            opts.prompt.push(message(Role::Assistant, "ok".into()));
            opts.prompt
                .push(message(Role::User, "Reply only ok again.".into()));
        }
    }
    provider.close_run_transport();
}
fn message(role: Role, text: String) -> Message {
    Message {
        role,
        content: vec![ContentBlock::Text { text }],
        origin: None,
        ordinal: None,
        cache_prefix_blocks: None,
    }
}
