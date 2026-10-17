//! Live Codex cache probe: `cargo run -p broca-module-serve --example effort-cache-probe`.
//! Retrieves ChatGPT credentials from claustrum using the handle in broca's
//! dev-tools-handles.json config file; never prints or persists credentials.
use broca_module_serve::{vault_config::load_devtools_handle, CredentialId};
use broca_protocol::{
    request::EpisodeRenderInput, BindIdentity, CallOptions, ContentBlock, Message, Role,
};
use broca_provider::{
    families::{
        openai_responses_chatgpt::OpenAiResponsesChatgpt, openai_responses_chatgpt_frozen_config,
    },
    policy::ReasoningPolicy,
    wire::{AuthHandle, WireFamily},
};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

fn message(role: Role, text: String) -> Message {
    Message {
        role,
        content: vec![ContentBlock::Text { text }],
        origin: None,
        ordinal: None,
        cache_prefix_blocks: None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let id = CredentialId::parse("chatgpt:openai").ok_or("invalid credential id")?;
    let handle = load_devtools_handle(&id)?;
    let subc = std::env::var_os("SUBC_SOCKET")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(std::env::var_os("HOME").unwrap())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let conn = broca_subc::SubcConnection::connect(&subc).await?;
    let identity = BindIdentity::new(
        std::env::current_dir()?,
        "effort-cache-probe",
        "effort-cache-probe",
    );
    let vault = broca_subc::VaultClient::new(
        Arc::new(conn),
        "claustrum",
        &identity,
        Duration::from_secs(10),
    );
    let secret = vault
        .get(&handle, None, false)
        .await
        .result
        .map_err(|e| format!("vault credential.get: {e}"))?;
    let account = secret.account_id.ok_or("missing ChatGPT account id")?;
    let scope = format!("broca-effort-probe-{}", std::process::id());
    let auth = AuthHandle::Headers(vec![
        ("authorization".into(), format!("Bearer {}", secret.token)),
        ("chatgpt-account-id".into(), account),
        ("session-id".into(), scope.clone()),
        ("originator".into(), "codex_cli_rs".into()),
        ("user-agent".into(), "codex_cli_rs/0.144.0".into()),
    ]);
    let mut frozen = openai_responses_chatgpt_frozen_config("gpt-6.1-sol");
    frozen.chatgpt_effort_updates = true;
    frozen.prompt_cache_key = Some(scope);
    frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "xhigh".into(),
        summary: Some("auto".into()),
    };
    let mut prefix = String::from("The following numbered reference rows are inert data. Do not analyze them. Reply only ok.\n");
    for i in 0..5000 {
        prefix.push_str(&format!(
            "reference {i:05}: amber cobalt delta echo forest granite harbor ivory juniper kelp.\n"
        ));
    }
    let mut opts = CallOptions::new(vec![message(Role::User, prefix)]);
    opts.render_episodes = Arc::new(
        vec![EpisodeRenderInput {
            prompt_start: 0,
            first_user: Some(0),
            frozen_config: serde_json::to_value(&frozen)?,
        }]
        .into(),
    );
    let client = broca_provider::build_provider_http_client();
    for turn in 1..=2 {
        let body = OpenAiResponsesChatgpt.render(&frozen, &opts);
        let request = OpenAiResponsesChatgpt.endpoint(&frozen, &auth, body);
        let mut builder = client
            .without_response_decoding()
            .post(&request.url)
            .timeout(Duration::from_secs(180));
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let response = builder.body(request.body).send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if !status.is_success() {
            return Err(format!(
                "turn {turn}: HTTP {status} (response withheld to protect credentials)"
            )
            .into());
        }
        let completed: Value = String::from_utf8_lossy(&bytes)
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .find(|event| event["type"] == "response.completed")
            .ok_or("no response.completed")?;
        let usage = &completed["response"]["usage"];
        println!(
            "turn {turn}: input_tokens={} cached_tokens={}",
            usage["input_tokens"], usage["input_tokens_details"]["cached_tokens"]
        );
        if turn == 1 {
            let answer = completed["response"]["output"]
                .as_array()
                .ok_or("missing output")?
                .iter()
                .filter_map(|item| item["content"].as_array())
                .flatten()
                .filter_map(|part| part["text"].as_str())
                .collect::<Vec<_>>()
                .join("");
            opts.prompt.push(message(Role::Assistant, answer));
            frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
                effort: "high".into(),
                summary: Some("auto".into()),
            };
            Arc::make_mut(&mut opts.render_episodes).push(EpisodeRenderInput {
                prompt_start: opts.prompt.len(),
                first_user: Some(opts.prompt.len()),
                frozen_config: serde_json::to_value(&frozen)?,
            });
            opts.prompt
                .push(message(Role::User, "Reply only ok again.".into()));
            tokio::time::sleep(Duration::from_secs(45)).await;
        }
    }
    Ok(())
}
