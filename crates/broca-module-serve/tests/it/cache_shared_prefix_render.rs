use broca_catalog::live::{
    resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig, ResolveOptions,
    WireFamilyId,
};
use broca_module_serve::wire::{SendParams, SendParamsExt};
use broca_module_serve::{resolve_cache_policy, resolve_prompt_cache_key, CacheTtlTable};
use broca_protocol::{
    request::{CacheRequest, CallOptions},
    BindIdentity, RunId,
};
use broca_provider::{
    families::{openai_chat_frozen_config, OpenAiChat},
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::{json, Value};

const SHARED_PREFIX: &str = "You are a concise comment reviewer.";
const SHARED_KEY: &str = "comment-review-3f9a1c2b7d5e0a91";
const OTHER_KEY: &str = "comment-review-8d0e6f4a2c1b7e93";

#[derive(Clone, Copy)]
struct Route {
    name: &'static str,
    provider: &'static str,
    model: &'static str,
    auth_method: AuthMethod,
    family_override: Option<WireFamilyId>,
}

const ROUTES: [Route; 3] = [
    Route {
        name: "OpenAI Responses API key",
        provider: "openai",
        model: "gpt-4.1",
        auth_method: AuthMethod::ApiKey,
        family_override: None,
    },
    Route {
        name: "ChatGPT Codex",
        provider: "openai",
        model: "gpt-5.3-codex",
        auth_method: AuthMethod::Chatgpt,
        family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
    },
    Route {
        name: "Meta Responses",
        provider: "meta",
        model: "muse-spark-1.3",
        auth_method: AuthMethod::Oauth,
        family_override: None,
    },
];

struct RenderedSend {
    bytes: Vec<u8>,
    body: Value,
    frozen_key: String,
}

fn render_one_shot(route: Route, session: &str, cache_key: &str, varying: &str) -> RenderedSend {
    crate::common::install_snapshot();
    let params: SendParams = serde_json::from_value(json!({
        "model": {"provider": route.provider, "model": route.model},
        "one_shot": true,
        "prompt_blocks": [SHARED_PREFIX, varying],
        "cache": {"key": cache_key}
    }))
    .expect("session.send params decode");
    let send = params
        .into_send_request(
            BindIdentity::new("/tmp/comment-review-project", "runner", session),
            RunId(format!("run-{session}")),
        )
        .expect("session.send converts into an admitted request");
    assert!(send.skip_materialization, "the request is a one-shot send");
    assert_eq!(send.session.session, session);

    let submission = *send.request;
    assert_eq!(
        submission.model.provider_module_id.as_str(),
        route.provider,
        "the wire send selects the expected provider"
    );
    assert_eq!(
        submission.model.model_id.as_str(),
        route.model,
        "the wire send selects the expected model"
    );
    assert_eq!(
        submission
            .cache
            .as_ref()
            .and_then(|cache| cache.key.as_deref()),
        Some(cache_key),
        "the caller's cache key survives wire conversion"
    );
    let provider_id = submission.model.provider_module_id.as_str();
    let frozen_key = resolve_prompt_cache_key(
        provider_id,
        submission.cache.as_ref(),
        &CacheTtlTable::shipped_defaults(),
        &send.session,
    )
    .expect("the caller's key is frozen for this send");
    let auth = match route.auth_method {
        AuthMethod::ApiKey => AuthMode::Secret("test-openai-api-key".into()),
        AuthMethod::Chatgpt => AuthMode::Bearer("test-chatgpt-token".into()),
        AuthMethod::Oauth => AuthMode::Bearer("test-meta-oauth-token".into()),
        AuthMethod::Antigravity => unreachable!("the test routes do not use Antigravity"),
    };
    let options = ResolveOptions {
        auth_method: Some(route.auth_method),
        family_override: route.family_override,
        session_id: Some(session),
        account_id: (route.auth_method == AuthMethod::Chatgpt).then_some("test-chatgpt-account"),
        prompt_cache_key: Some(&frozen_key),
        ..Default::default()
    };
    let model = format!(
        "{}/{}",
        submission.model.provider_module_id, submission.model.model_id
    );
    let resolution = resolve_catalog_episode_with_client(
        &model,
        None,
        &auth,
        &options,
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig::default(),
    )
    .unwrap_or_else(|error| panic!("{} resolves from the vendored catalog: {error}", route.name));
    let frozen: FrozenRenderConfig =
        serde_json::from_value(resolution.provider.render_config_blob())
            .expect("the route freezes a render config");
    assert_eq!(
        frozen.prompt_cache_key.as_deref(),
        Some(frozen_key.as_str())
    );

    let bytes = resolution
        .provider
        .render_request(&CallOptions::new(submission.input));
    let body: Value = serde_json::from_slice(&bytes).expect("the rendered request is JSON");
    assert_eq!(body["prompt_cache_key"], frozen_key, "{}", route.name);
    RenderedSend {
        bytes,
        body,
        frozen_key,
    }
}

fn bytes_before<'a>(bytes: &'a [u8], needle: &str) -> &'a [u8] {
    let needle = needle.as_bytes();
    let matches: Vec<_> = bytes
        .windows(needle.len())
        .enumerate()
        .filter_map(|(index, window)| (window == needle).then_some(index))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "the varying block appears once in the request"
    );
    &bytes[..matches[0]]
}

fn openai_chat_rendered_prompt_cache_key(cache_key: &str) -> Option<String> {
    let params: SendParams = serde_json::from_value(json!({
        "model": {"provider": "openai", "model": "gpt-4.1"},
        "one_shot": true,
        "prompt_blocks": [SHARED_PREFIX, "review tail chat"],
        "cache": {"key": cache_key}
    }))
    .unwrap();
    let send = params
        .into_send_request(
            BindIdentity::new(
                "/tmp/comment-review-project",
                "runner",
                "comment-session-chat",
            ),
            RunId("run-comment-session-chat".into()),
        )
        .unwrap();
    assert!(send.skip_materialization);
    let submission = *send.request;
    let frozen_key = resolve_prompt_cache_key(
        submission.model.provider_module_id.as_str(),
        submission.cache.as_ref(),
        &CacheTtlTable::shipped_defaults(),
        &send.session,
    )?;
    let mut frozen = openai_chat_frozen_config("gpt-4.1");
    frozen.prompt_cache_key = Some(frozen_key);
    let rendered = OpenAiChat.render(&frozen, &CallOptions::new(submission.input));
    let body: Value = serde_json::from_slice(&rendered).unwrap();
    body.get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[test]
fn key_only_cache_request_is_admitted_on_every_wire_family() {
    let cache = CacheRequest {
        role: None,
        ttl: None,
        key: Some(SHARED_KEY.into()),
        tail: None,
        mode: None,
    };
    let table = CacheTtlTable::shipped_defaults();
    let session = BindIdentity::new("/tmp/comment-review-project", "runner", "one-shot-session");
    for (family, provider) in [
        (WireFamilyId::AnthropicMessages, "anthropic"),
        (WireFamilyId::BedrockConverse, "amazon-bedrock"),
        (WireFamilyId::OpenAiChat, "openai"),
        (WireFamilyId::OpenAiResponses, "openai"),
        (WireFamilyId::OpenAiResponsesChatgpt, "openai"),
        (WireFamilyId::Gemini, "google"),
        (WireFamilyId::GoogleCodeAssist, "google"),
    ] {
        let policy = resolve_cache_policy(family, provider, Some(&cache), &table, &session)
            .unwrap_or_else(|error| {
                panic!("key-only cache request refused by {family:?}/{provider}: {error}")
            });
        assert_eq!(
            resolve_prompt_cache_key(provider, Some(&cache), &table, &session).as_deref(),
            Some(SHARED_KEY),
            "{family:?}/{provider} keeps the caller's scope"
        );
        if family == WireFamilyId::AnthropicMessages && provider == "anthropic" {
            assert!(
                policy.is_some(),
                "first-party Anthropic keeps its default markers"
            );
        } else {
            assert_eq!(
                policy, None,
                "{family:?}/{provider} adds no marker preference"
            );
        }
    }
}

#[test]
fn same_caller_key_freezes_shared_prefix_across_one_shot_routes() {
    for route in ROUTES {
        let first = render_one_shot(route, "comment-session-a", SHARED_KEY, "review tail alpha");
        let second = render_one_shot(route, "comment-session-b", SHARED_KEY, "review tail beta");
        assert_eq!(first.frozen_key, SHARED_KEY, "{}", route.name);
        assert_eq!(second.frozen_key, SHARED_KEY, "{}", route.name);
        assert_eq!(first.body["prompt_cache_key"], SHARED_KEY, "{}", route.name);
        assert_eq!(
            second.body["prompt_cache_key"], SHARED_KEY,
            "{}",
            route.name
        );
        assert_eq!(
            bytes_before(&first.bytes, "review tail alpha"),
            bytes_before(&second.bytes, "review tail beta"),
            "{} renders the same request bytes through the shared prefix",
            route.name
        );

        let differently_keyed =
            render_one_shot(route, "comment-session-c", OTHER_KEY, "review tail gamma");
        assert_eq!(differently_keyed.frozen_key, OTHER_KEY, "{}", route.name);
        assert_eq!(
            differently_keyed.body["prompt_cache_key"], OTHER_KEY,
            "{}",
            route.name
        );
        assert_ne!(
            first.body["prompt_cache_key"], differently_keyed.body["prompt_cache_key"],
            "different caller keys name different rendered scopes on {}",
            route.name
        );
    }
    assert_eq!(
        openai_chat_rendered_prompt_cache_key(SHARED_KEY).as_deref(),
        Some(SHARED_KEY),
        "the OpenAI Chat renderer emits the frozen prompt_cache_key"
    );
}
