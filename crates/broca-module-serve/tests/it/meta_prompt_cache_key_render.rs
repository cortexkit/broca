//! What a Meta (Muse) episode sends as `prompt_cache_key`.
//!
//! Meta's Responses API reuses a cached prefix only for requests that name a cache scope, so
//! admission freezes the session id as the scope when the caller names none. These tests drive
//! the steps the live executor takes: `resolve_prompt_cache_key` against the shipped cache table,
//! then catalog resolution with that key, then render. A resume re-resolves from the key read back
//! out of the frozen render config, as the executor does, and must render the same bytes.

use broca_catalog::live::{
    resolve_catalog_episode_with_client, AuthMode, EpisodeRenderConfig, ProviderResolution,
    ResolveOptions,
};
use broca_module_serve::{resolve_prompt_cache_key, CacheTtlTable};
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::CallOptions,
    BindIdentity,
};
use broca_provider::wire::FrozenRenderConfig;
use serde_json::Value;

const MUSE: &str = "meta/muse-spark-1.3";
const SESSION: &str = "muse-cache-session";

fn text(role: Role, value: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: value.into() }])
}

/// The first step: the system prompt and the user's opening message.
fn first_step() -> CallOptions {
    CallOptions::new(vec![
        text(Role::System, "You are a concise assistant."),
        text(Role::User, "Name three prime numbers."),
    ])
}

/// The next step of the same session: the first step's prompt plus the reply and a follow-up.
fn second_step() -> CallOptions {
    let mut opts = first_step();
    opts.prompt.push(text(Role::Assistant, "2, 3 and 5."));
    opts.prompt.push(text(Role::User, "And the next one?"));
    opts
}

/// Resolve `model` the way a fresh episode does: the admission key from the shipped cache table
/// for a send with no `cache` field, then the catalog provider with that key frozen.
fn admit(model: &str) -> (ProviderResolution, Option<String>) {
    crate::common::install_snapshot();
    let provider_id = model.split_once('/').expect("a catalog id").0;
    let key = resolve_prompt_cache_key(
        provider_id,
        None,
        &CacheTtlTable::shipped_defaults(),
        &BindIdentity::new("/tmp/muse-project", "runner", SESSION),
    );
    let resolution = resolve(model, key.as_deref(), EpisodeRenderConfig::default());
    (resolution, key)
}

fn resolve(
    model: &str,
    prompt_cache_key: Option<&str>,
    render: EpisodeRenderConfig<'_>,
) -> ProviderResolution {
    resolve_catalog_episode_with_client(
        model,
        None,
        &AuthMode::Bearer("rig-only-token".into()),
        &ResolveOptions {
            session_id: Some(SESSION),
            prompt_cache_key,
            ..Default::default()
        },
        broca_provider::build_provider_http_client(),
        render,
    )
    .unwrap_or_else(|_| panic!("{model} resolves from the vendored catalog"))
}

fn body(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("a rendered body is JSON")
}

#[test]
fn a_meta_episode_sends_one_session_prompt_cache_key_on_every_step_and_its_resume() {
    let (admitted, key) = admit(MUSE);
    assert_eq!(key.as_deref(), Some(SESSION), "the session id is the scope");
    let frozen: FrozenRenderConfig =
        serde_json::from_value(admitted.provider.render_config_blob()).unwrap();
    assert_eq!(
        frozen.prompt_cache_key.as_deref(),
        Some(SESSION),
        "admission freezes the key into the render config RunStarted records"
    );

    let first = admitted.provider.render_request(&first_step());
    let second = admitted.provider.render_request(&second_step());
    for (step, bytes) in [("first", &first), ("second", &second)] {
        assert_eq!(
            body(bytes).get("prompt_cache_key"),
            Some(&Value::String(SESSION.into())),
            "the {step} step names the session's cache scope"
        );
    }

    // A resume re-reads the frozen key and output cap and resolves again; it never consults
    // the cache table, so its bytes cannot move even if the shipped defaults do.
    let resumed = resolve(
        MUSE,
        frozen.prompt_cache_key.as_deref(),
        EpisodeRenderConfig {
            max_output_tokens: Some(frozen.max_output_tokens),
            ..Default::default()
        },
    );
    assert_eq!(
        resumed.provider.render_config_blob(),
        admitted.provider.render_config_blob(),
        "the resumed episode freezes the same render config"
    );
    assert_eq!(
        String::from_utf8_lossy(&resumed.provider.render_request(&second_step())),
        String::from_utf8_lossy(&second),
        "the resumed step renders byte-identically, key included"
    );
}

/// The Meta session-key default must leave other providers' request bytes alone. On OpenAI's own
/// Responses API, admission without a cache key returns no key, and the rendered request omits
/// `prompt_cache_key`.
#[test]
fn an_openai_responses_episode_without_a_cache_key_still_sends_none() {
    let (admitted, key) = admit("openai/gpt-4.1");
    assert_eq!(key, None);
    let rendered = body(&admitted.provider.render_request(&second_step()));
    assert!(
        rendered.get("prompt_cache_key").is_none(),
        "no key for openai without a caller scope: {rendered}"
    );
}
