//! LIVE check of Anthropic's thinking-binding instrument (off by default).
//!
//! broca sends the `thinking-binding-controls-2026-08-01` beta whenever thinking is on, and never
//! sets `thinking.block_binding.prefix_mismatch_behavior` (setting it would opt the account into
//! enforcement). With the beta, every response lists in `message_start`'s
//! `input_transformations` each signed thinking block that failed the prefix-binding check.
//!
//! A beta the account or the OAuth path rejected would fail EVERY Anthropic request, so this test
//! is the gate for shipping the header. For each model it proves, through broca's own provider:
//!   1. a first turn with thinking on is accepted;
//!   2. an unchanged follow-up is accepted and reports an EMPTY transformation list (the
//!      field's presence proves the beta took effect);
//!   3. the same follow-up with a deliberately edited earlier user message is accepted and, on a
//!      model that binds thinking to its prefix, REPORTS the thinking block that no longer
//!      matches, so the instrument can see a real failure.
//!
//! The `oauth:anthropic` handle comes from `dev-tools-handles.json` under the config home
//! (override: `BROCA_DEVTOOLS_HANDLES`), then the token is fetched from the vault over subc.
//! Mint with `ck auth mint-handle --id oauth:anthropic` (operator only). The daemon's
//! `vault-handles.json` is not used. The token is never printed, logged or formatted.
//!
//! Run (spends a few thousand tokens of real subscription usage):
//! `BROCA_LIVE_TESTS=1 cargo test -p broca-module-serve --test it anthropic_thinking_binding_live:: -- --nocapture`
//! Optional: `BROCA_LIVE_SUBC` (subc connection file, default the standard install's).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broca_catalog::live::{
    resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig, ResolveOptions,
};
use broca_module_serve::vault_config::load_devtools_handle;
use broca_module_serve::CredentialId;
use broca_protocol::content::{AssistantMessage, ContentBlock, Message, Role};
use broca_protocol::request::{CallOptions, ModelHandle};
use broca_protocol::{BindIdentity, InputTransformation, NormalizedEvent, ProviderControl, StepId};
use broca_provider::families::AnthropicMessages;
use broca_provider::wire::{AuthHandle, FrozenRenderConfig};
use broca_subc::{SubcConnection, VaultClient};

const CREDENTIALS_MODULE: &str = "claustrum";
const CREDENTIAL_ID: &str = "oauth:anthropic";
/// Each model, and whether the API binds its thinking to the prefix. The binding applies to Claude
/// Fable 5.1 and Opus 5.5; measured here, Opus 5 accepts the beta and reports an empty list even
/// for an edited prefix, so only the header's acceptance is checked for it.
const MODELS: [(&str, bool); 2] = [("claude-opus-5", false), ("claude-opus-5-5", true)];

fn live_calls_opted_in() -> bool {
    std::env::var("BROCA_LIVE_TESTS")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").expect("HOME is set"))
}

/// Fetch the Anthropic OAuth token with the operator dev-tools handle.
async fn vault_token() -> Result<String, String> {
    let credential = CredentialId::parse(CREDENTIAL_ID).expect("a canonical credential id");
    let handle = load_devtools_handle(&credential)?;
    let subc = std::env::var_os("BROCA_LIVE_SUBC")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".local/share/cortexkit/run/subc-connection.json"));
    let conn = SubcConnection::connect(&subc)
        .await
        .map_err(|error| format!("connect subc ({}): {error}", subc.display()))?;
    let identity = BindIdentity::new(home().to_string_lossy().to_string(), "broca", "vault");
    let client = VaultClient::new(
        Arc::new(conn),
        CREDENTIALS_MODULE,
        &identity,
        Duration::from_secs(15),
    );
    let token = client
        .get(&handle, Some(20 * 60 * 1000), false)
        .await
        .result
        .map_err(|error| format!("vault credential.get failed: {error}"))?
        .token;
    let present = !token.is_empty();
    assert!(present, "the vault returned an empty token");
    Ok(token)
}

fn install_catalog() {
    let catalog = broca_catalog::Catalog::from_json_str(
        include_str!("../../../broca-catalog/data/models.dev.json"),
        include_str!("../../../broca-catalog/data/overlay.json"),
    )
    .expect("vendored catalog snapshot parses");
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
    broca_catalog::install_global_catalog_cache(cache);
}

/// The production OAuth frozen config for `model`, from the catalog resolver. A model the
/// vendored catalog snapshot does not list yet gets `claude-opus-5`'s resolved config with only
/// the wire model id changed, which is the same family, auth shape and thinking policy.
fn frozen_config(token: &str, model: &str) -> FrozenRenderConfig {
    let options = ResolveOptions {
        auth_method: Some(AuthMethod::Oauth),
        ..ResolveOptions::default()
    };
    let resolve = |id: &str| {
        resolve_catalog_episode_with_client(
            &format!("anthropic/{id}"),
            Some(4096),
            &AuthMode::Secret(token.to_string()),
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig::default(),
        )
        .map(|resolved| {
            serde_json::from_value::<FrozenRenderConfig>(resolved.provider.render_config_blob())
                .expect("the resolved provider's frozen config round-trips")
        })
    };
    match resolve(model) {
        Ok(frozen) => frozen,
        Err(_) => {
            eprintln!("[{model}] not in the vendored catalog; reusing claude-opus-5's config");
            let mut frozen = resolve("claude-opus-5").expect("claude-opus-5 resolves");
            frozen.wire_model_id = model.to_string();
            frozen
        }
    }
}

struct Reply {
    message: AssistantMessage,
    transformations: Option<Vec<InputTransformation>>,
}

async fn drive_step(
    provider: &dyn broca_core::provider::Provider,
    step: u64,
    opts: &CallOptions,
) -> Result<Reply, String> {
    let mut rx = provider.step(StepId(step), opts).await;
    let mut message = None;
    let mut transformations = None;
    let mut error = None;
    while let Some(event) = rx.recv().await {
        match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message: m }) => {
                message = Some(m)
            }
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                if attempt.input_transformations.is_some() {
                    transformations = attempt.input_transformations;
                }
            }
            NormalizedEvent::Control(ProviderControl::Error { error: e, .. }) => {
                error = Some(format!(
                    "status={:?} class={:?} code={:?}: {}",
                    e.status, e.class, e.provider_code, e.message
                ))
            }
            _ => {}
        }
    }
    if let Some(error) = error {
        return Err(error);
    }
    Ok(Reply {
        message: message.ok_or("an accepted step assembled no assistant message")?,
        transformations,
    })
}

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

async fn check_model(token: &str, model: &str, binds_thinking: bool) {
    let frozen = frozen_config(token, model);
    assert_eq!(
        serde_json::to_value(&frozen.reasoning).unwrap()["mode"],
        "anthropic_interleaved",
        "[{model}] thinking must be on, or the beta is never sent"
    );
    let provider = broca_provider::LiveProvider::new(
        AnthropicMessages,
        frozen,
        AuthHandle::Bearer(token.to_string()),
    );
    let handle = ModelHandle::new("anthropic", model);
    let system = Message::new(
        Role::System,
        vec![ContentBlock::Text {
            text: "You are a careful mathematician.".into(),
        }],
    );
    let first_user = |project: &str| {
        user(&format!(
            "<project-context>project={project}</project-context>\nFind the smallest positive \
             integer n such that n^2 + n + 41 is composite, and show it is composite. Think it \
             through carefully, then give only n."
        ))
    };
    let request = |prompt: Vec<Message>| {
        let mut opts = CallOptions::new(prompt);
        opts.target_model = Some(handle.clone());
        opts
    };

    // 1. First turn, thinking on: accepted, and it must think or step 3 has nothing to bind.
    let first = drive_step(
        &provider,
        1,
        &request(vec![system.clone(), first_user("alpha")]),
    )
    .await
    .unwrap_or_else(|error| panic!("[{model}] first turn REJECTED with the beta: {error}"));
    let thought = first.message.content.iter().any(|block| {
        matches!(
            block,
            ContentBlock::Reasoning {
                signature: Some(_),
                ..
            }
        )
    });
    assert!(
        thought,
        "[{model}] the first turn produced no signed thinking"
    );
    eprintln!(
        "[{model}] turn 1 accepted; input_transformations = {:?}",
        first.transformations
    );
    let assistant = Message::assistant_from(first.message.content.clone(), handle.clone());
    let follow_up = user("Now subtract 1000. Only the number.");

    // 2. Unchanged prefix: accepted, reported, nothing transformed.
    let unchanged = drive_step(
        &provider,
        2,
        &request(vec![
            system.clone(),
            first_user("alpha"),
            assistant.clone(),
            follow_up.clone(),
        ]),
    )
    .await
    .unwrap_or_else(|error| panic!("[{model}] unchanged follow-up REJECTED: {error}"));
    eprintln!(
        "[{model}] unchanged follow-up: input_transformations = {:?}",
        unchanged.transformations
    );
    assert_eq!(
        unchanged.transformations,
        Some(Vec::new()),
        "[{model}] an unchanged prefix must be reported, and report nothing"
    );

    // 3. Deliberately invalid edit: the earlier user message changed, so the thinking block
    //    after it no longer matches its prefix. The instrument must see it.
    let edited = drive_step(
        &provider,
        3,
        &request(vec![
            system.clone(),
            first_user("beta"),
            assistant,
            follow_up,
        ]),
    )
    .await
    .unwrap_or_else(|error| panic!("[{model}] edited follow-up REJECTED: {error}"));
    eprintln!(
        "[{model}] edited follow-up: input_transformations = {:?}",
        edited.transformations
    );
    assert!(
        edited.transformations.is_some(),
        "[{model}] the edited follow-up must still carry the report"
    );
    if !binds_thinking {
        return;
    }
    let reported = edited.transformations.unwrap_or_default();
    assert!(
        reported.iter().any(|entry| entry
            .kind
            .as_deref()
            .is_some_and(|kind| kind.starts_with("thinking"))),
        "[{model}] the edited prefix must be reported as a thinking transformation"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn thinking_binding_beta_is_accepted_and_reports_edits_live() -> Result<(), String> {
    if !live_calls_opted_in() {
        eprintln!("SKIP thinking_binding_live: set BROCA_LIVE_TESTS to spend real usage");
        return Ok(());
    }
    let token = vault_token().await?;
    install_catalog();
    for (model, binds_thinking) in MODELS {
        check_model(&token, model, binds_thinking).await;
    }
    Ok(())
}
