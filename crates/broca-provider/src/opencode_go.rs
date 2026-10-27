use crate::wire::{CacheClass, FrozenRenderConfig};

pub(crate) const PI_CLIENT_VERSION: &str = "1.4.0";
// Update the pinned Node runtime with the Pi version; this Rust process never launches Node.
const PI_NODE_RUNTIME: &str = "node/v24.11.1";

/// Identity headers shared by OpenCode Go's Anthropic, Gemini, Chat, and Responses routes.
pub(crate) fn request_headers(frozen: &FrozenRenderConfig) -> Vec<(String, String)> {
    let Some(session_id) = &frozen.opencode_go_session_id else {
        return Vec::new();
    };
    vec![
        ("User-Agent".into(), user_agent()),
        ("x-opencode-client".into(), "pi".into()),
        ("x-opencode-session".into(), session_id.clone()),
    ]
}

/// OpenAI Responses uses Pi's `openai-nosession` compatibility for OpenAI-backed Go models,
/// assigned by `packages/ai/scripts/generate-models.ts:2252-2255` at upstream commit
/// `74f46864025c68504cd10c89eeb626b7e3d64cea`. It keeps `x-client-request-id` but omits the
/// `session_id` header; Go's required `x-opencode-session` remains.
pub(crate) fn responses_request_headers(frozen: &FrozenRenderConfig) -> Vec<(String, String)> {
    let Some(session_id) = &frozen.opencode_go_session_id else {
        return Vec::new();
    };
    // The Responses API's cache-affinity id is optional; OpenCode's routing session header is not.
    // Pi omits `x-client-request-id` for an explicit `none` retention while still sending Go's
    // `x-opencode-session` header.
    let cache_class = frozen.cache.as_ref().map(|cache| &cache.class);
    let send_cache_session_id = !matches!(cache_class, Some(CacheClass::None))
        && !matches!(
            cache_class,
            Some(CacheClass::Hints {
                retention: Some(retention),
                ..
            }) if retention == "none"
        );
    let mut headers = vec![
        ("User-Agent".into(), user_agent()),
        ("x-opencode-client".into(), "pi".into()),
    ];
    if send_cache_session_id {
        headers.push(("x-client-request-id".into(), session_id.clone()));
    }
    headers.push(("x-opencode-session".into(), session_id.clone()));
    headers
}

fn user_agent() -> String {
    format!(
        "pi/{PI_CLIENT_VERSION} ({}; {PI_NODE_RUNTIME}; {})",
        node_platform(std::env::consts::OS),
        node_arch(std::env::consts::ARCH)
    )
}

fn node_platform(platform: &str) -> &str {
    match platform {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    }
}

fn node_arch(architecture: &str) -> &str {
    match architecture {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        "x86" => "ia32",
        "powerpc64" => "ppc64",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{node_arch, node_platform, PI_CLIENT_VERSION};
    use crate::{
        families::{
            anthropic_apikey_frozen_config, bedrock_frozen_config, gemini_frozen_config,
            google_codeassist_frozen_config, openai_chat_frozen_config,
            openai_responses_chatgpt_frozen_config, openai_responses_frozen_config,
            AnthropicMessages, BedrockConverse, Gemini, GoogleCodeAssist, OpenAiChat,
            OpenAiResponses, OpenAiResponsesChatgpt,
        },
        wire::{
            AuthHandle, CacheClass, ClaudeCodePersona, FrozenRenderConfig, HttpRequest, WireFamily,
        },
    };
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        CallOptions, ModelHandle, StepContext,
    };
    use serde_json::Value;

    const GO_OPENAI_BASE: &str = "https://opencode.ai/inference/go/v1";
    const GO_ANTHROPIC_BASE: &str = "https://opencode.ai/inference/go";
    const GO_GOOGLE_BASE: &str = "https://opencode.ai/inference/go/v1";
    const SESSION_REF: &str = "4b6d7c8e9f00112233445566778899aa";
    const OTHER_SESSION_REF: &str = "5c7e8d9f00112233445566778899aabb";

    fn expected_user_agent() -> String {
        let platform = match std::env::consts::OS {
            "macos" => "darwin",
            "windows" => "win32",
            other => other,
        };
        let architecture = match std::env::consts::ARCH {
            "aarch64" => "arm64",
            "x86_64" => "x64",
            "x86" => "ia32",
            "powerpc64" => "ppc64",
            other => other,
        };
        format!("pi/1.4.0 ({platform}; node/v24.11.1; {architecture})")
    }

    fn with_go_identity(mut frozen: FrozenRenderConfig, session_id: &str) -> FrozenRenderConfig {
        frozen.opencode_go_session_id = Some(session_id.to_string());
        frozen
    }

    fn request(family: &dyn WireFamily, frozen: &FrozenRenderConfig) -> HttpRequest {
        request_with_auth(family, frozen, AuthHandle::None)
    }

    fn request_with_auth(
        family: &dyn WireFamily,
        frozen: &FrozenRenderConfig,
        auth: AuthHandle,
    ) -> HttpRequest {
        let body = family.render(frozen, &CallOptions::new(Vec::new()));
        family.endpoint(frozen, &auth, body)
    }

    fn header<'a>(request: &'a HttpRequest, name: &str) -> Option<&'a str> {
        request
            .headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn pi_identity_maps_host_platform_and_architecture_to_node_names() {
        assert_eq!(node_platform("macos"), "darwin");
        assert_eq!(node_platform("windows"), "win32");
        assert_eq!(node_platform("linux"), "linux");
        assert_eq!(node_arch("aarch64"), "arm64");
        assert_eq!(node_arch("x86_64"), "x64");
        assert_eq!(node_arch("x86"), "ia32");
        assert_eq!(PI_CLIENT_VERSION, "1.4.0");
    }

    #[test]
    fn requests_use_pi_urls_and_exact_identity_headers_for_each_format() {
        let session_id = ClaudeCodePersona::derive_session_id(SESSION_REF);

        let mut chat = openai_chat_frozen_config("minimax-m3.1-flash-free");
        chat.base_url = Some(GO_OPENAI_BASE.into());
        let chat = with_go_identity(chat, &session_id);
        let chat_request = request_with_auth(&OpenAiChat, &chat, AuthHandle::Bearer(String::new()));
        assert_eq!(
            chat_request.url,
            "https://opencode.ai/inference/go/v1/chat/completions"
        );
        assert_eq!(
            chat_request.headers,
            vec![
                ("User-Agent".into(), expected_user_agent()),
                ("x-opencode-client".into(), "pi".into()),
                ("x-opencode-session".into(), session_id.clone()),
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "application/json".into()),
                ("authorization".into(), "Bearer ".into()),
            ]
        );

        let mut responses = openai_responses_frozen_config("gpt-6-luna");
        responses.base_url = Some(GO_OPENAI_BASE.into());
        let responses = with_go_identity(responses, &session_id);
        let responses_request = request_with_auth(
            &OpenAiResponses,
            &responses,
            AuthHandle::Bearer(String::new()),
        );
        assert_eq!(
            responses_request.url,
            "https://opencode.ai/inference/go/v1/responses"
        );
        assert_eq!(
            responses_request.headers,
            vec![
                ("User-Agent".into(), expected_user_agent()),
                ("x-opencode-client".into(), "pi".into()),
                ("x-client-request-id".into(), session_id.clone()),
                ("x-opencode-session".into(), session_id.clone()),
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "application/json".into()),
                ("authorization".into(), "Bearer ".into()),
            ]
        );

        let mut anthropic = anthropic_apikey_frozen_config("minimax-m3");
        anthropic.base_url = Some(GO_ANTHROPIC_BASE.into());
        let anthropic = with_go_identity(anthropic, &session_id);
        let anthropic_request = request_with_auth(
            &AnthropicMessages,
            &anthropic,
            AuthHandle::ApiKeyHeader {
                header: "x-api-key".into(),
                value: String::new(),
            },
        );
        assert_eq!(
            anthropic_request.url,
            "https://opencode.ai/inference/go/v1/messages"
        );
        assert_eq!(
            anthropic_request.headers,
            vec![
                ("User-Agent".into(), expected_user_agent()),
                ("x-opencode-client".into(), "pi".into()),
                ("x-opencode-session".into(), session_id.clone()),
                ("anthropic-version".into(), "2023-06-01".into()),
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "application/json".into()),
                ("x-api-key".into(), String::new()),
            ]
        );

        let mut google = gemini_frozen_config("gemini-4-argon-contributor");
        google.base_url = Some(GO_GOOGLE_BASE.into());
        let google = with_go_identity(google, &session_id);
        let google_request = request_with_auth(
            &Gemini,
            &google,
            AuthHandle::ApiKeyHeader {
                header: "x-goog-api-key".into(),
                value: String::new(),
            },
        );
        assert_eq!(
            google_request.url,
            "https://opencode.ai/inference/go/v1/models/gemini-4-argon-contributor:streamGenerateContent?alt=sse"
        );
        assert_eq!(
            google_request.headers,
            vec![
                ("User-Agent".into(), expected_user_agent()),
                ("x-opencode-client".into(), "pi".into()),
                ("x-opencode-session".into(), session_id),
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "text/event-stream".into()),
                ("x-goog-api-key".into(), String::new()),
            ]
        );
    }

    #[test]
    fn responses_omit_cache_affinity_for_explicit_no_cache_policies() {
        let session_id = ClaudeCodePersona::derive_session_id(SESSION_REF);
        let mut frozen = openai_responses_frozen_config("gpt-6-luna");
        frozen.base_url = Some(GO_OPENAI_BASE.into());
        frozen.opencode_go_session_id = Some(session_id.clone());

        let mut no_cache = frozen.clone();
        no_cache.cache = Some(crate::wire::CachePolicy {
            version: 1,
            class: CacheClass::None,
        });
        let no_cache_request = request(&OpenAiResponses, &no_cache);
        assert_eq!(
            header(&no_cache_request, "x-opencode-session"),
            Some(session_id.as_str())
        );
        assert!(header(&no_cache_request, "x-client-request-id").is_none());

        let mut no_retention = frozen;
        no_retention.cache = Some(crate::wire::CachePolicy {
            version: 1,
            class: CacheClass::Hints {
                cache_key: "cache-key-fixture".into(),
                retention: Some("none".into()),
            },
        });
        let no_retention_request = request(&OpenAiResponses, &no_retention);
        assert_eq!(
            header(&no_retention_request, "x-opencode-session"),
            Some(session_id.as_str())
        );
        assert!(header(&no_retention_request, "x-client-request-id").is_none());
    }

    #[test]
    fn session_id_is_stable_when_a_run_is_resumed_and_differs_between_sessions() {
        let session_id = ClaudeCodePersona::derive_session_id(SESSION_REF);
        assert_eq!(session_id.len(), 36);
        assert!(!session_id.contains(SESSION_REF));

        let mut fresh = openai_chat_frozen_config("kimi-k2.7-code");
        fresh.base_url = Some(GO_OPENAI_BASE.into());
        let fresh = with_go_identity(fresh, &session_id);
        let started = request(&OpenAiChat, &fresh);

        // Restart reconstruction reads the same bind identity and derives the same UUID.
        let recorded = serde_json::to_value(&fresh).unwrap();
        let resumed: FrozenRenderConfig = serde_json::from_value(recorded).unwrap();
        let resumed_id = ClaudeCodePersona::derive_session_id(SESSION_REF);
        let resumed = with_go_identity(resumed, &resumed_id);
        let resumed_request = request(&OpenAiChat, &resumed);
        assert_eq!(
            header(&started, "x-opencode-session"),
            Some(session_id.as_str())
        );
        assert_eq!(
            header(&resumed_request, "x-opencode-session"),
            Some(session_id.as_str())
        );

        let other_id = ClaudeCodePersona::derive_session_id(OTHER_SESSION_REF);
        let other = with_go_identity(openai_chat_frozen_config("kimi-k2.7-code"), &other_id);
        let other_request = request(&OpenAiChat, &other);
        assert_ne!(
            header(&started, "x-opencode-session"),
            header(&other_request, "x-opencode-session")
        );
    }

    #[test]
    fn reasoning_signature_uses_the_field_pi_preserves() {
        let model = ModelHandle::new("opencode-go", "kimi-k2.7-code");
        let mut options = CallOptions::new(vec![
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "question".into(),
                }],
            ),
            Message::assistant_from(
                vec![
                    ContentBlock::Reasoning {
                        text: "thinking".into(),
                        signature: Some("reasoning".into()),
                    },
                    ContentBlock::Text {
                        text: "answer".into(),
                    },
                ],
                model,
            ),
        ]);
        options.target_model = Some(ModelHandle::new("opencode-go", "kimi-k2.7-code"));
        let session_id = ClaudeCodePersona::derive_session_id(SESSION_REF);
        let mut frozen = with_go_identity(openai_chat_frozen_config("kimi-k2.7-code"), &session_id);
        frozen.opencode_go_reasoning_signature = true;
        let body: Value = serde_json::from_slice(&OpenAiChat.render(&frozen, &options)).unwrap();
        assert_eq!(body["messages"][1]["reasoning_content"], "thinking");
        assert!(body["messages"][1].get("reasoning").is_none());
    }

    #[test]
    fn go_qwen_and_deepseek_reasoning_modes_keep_their_model_specific_fields() {
        let session_id = ClaudeCodePersona::derive_session_id(SESSION_REF);
        let mut qwen = with_go_identity(openai_chat_frozen_config("qwen3.5-plus"), &session_id);
        qwen.opencode_go_qwen_thinking = true;
        let qwen_body: Value =
            serde_json::from_slice(&OpenAiChat.render(&qwen, &CallOptions::new(Vec::new())))
                .unwrap();
        assert_eq!(qwen_body["enable_thinking"], false);

        let mut deepseek =
            with_go_identity(openai_chat_frozen_config("deepseek-v4-pro"), &session_id);
        deepseek.opencode_go_deepseek_thinking = true;
        let deepseek_body: Value =
            serde_json::from_slice(&OpenAiChat.render(&deepseek, &CallOptions::new(Vec::new())))
                .unwrap();
        assert_eq!(deepseek_body["thinking"]["type"], "disabled");

        let mut kimi = with_go_identity(openai_chat_frozen_config("kimi-k2.6"), &session_id);
        kimi.opencode_go_deepseek_thinking = true;
        let kimi_body: Value =
            serde_json::from_slice(&OpenAiChat.render(&kimi, &CallOptions::new(Vec::new())))
                .unwrap();
        assert_eq!(kimi_body["thinking"]["type"], "disabled");

        let unpinned = openai_chat_frozen_config("qwen3.5-plus");
        let ordinary_body: Value =
            serde_json::from_slice(&OpenAiChat.render(&unpinned, &CallOptions::new(Vec::new())))
                .unwrap();
        assert!(ordinary_body.get("enable_thinking").is_none());
    }

    #[test]
    fn go_kimi_k2_6_omits_unsupported_long_cache_hints() {
        let mut go = openai_chat_frozen_config("kimi-k2.6");
        go.opencode_go_long_cache_retention_unsupported = true;
        go.cache = Some(crate::wire::CachePolicy {
            version: 1,
            class: crate::wire::CacheClass::Hints {
                cache_key: "test-cache-key".into(),
                retention: Some("24h".into()),
            },
        });
        let body: Value =
            serde_json::from_slice(&OpenAiChat.render(&go, &CallOptions::new(Vec::new()))).unwrap();
        assert!(body.get("prompt_cache_key").is_none());
        assert!(body.get("prompt_cache_retention").is_none());

        let mut ordinary = openai_chat_frozen_config("kimi-k2.6");
        ordinary.cache = go.cache;
        let ordinary_body: Value =
            serde_json::from_slice(&OpenAiChat.render(&ordinary, &CallOptions::new(Vec::new())))
                .unwrap();
        assert_eq!(ordinary_body["prompt_cache_key"], "test-cache-key");
        assert_eq!(ordinary_body["prompt_cache_retention"], "24h");
    }

    #[test]
    fn non_go_requests_keep_their_existing_header_sets() {
        let mut options = CallOptions::new(Vec::new());
        options.step_context = Some(StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: "identity-header-test".into(),
            step_index: 1,
            max_output_tokens: None,
        });
        let endpoint = |family: &dyn WireFamily, frozen: FrozenRenderConfig| {
            let body = family.render(&frozen, &options);
            family.endpoint(&frozen, &AuthHandle::None, body)
        };

        assert_eq!(
            endpoint(&OpenAiChat, openai_chat_frozen_config("model")).headers,
            vec![
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "application/json".into()),
            ]
        );
        assert_eq!(
            endpoint(&OpenAiResponses, openai_responses_frozen_config("model")).headers,
            vec![
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "application/json".into()),
            ]
        );
        assert_eq!(
            endpoint(&Gemini, gemini_frozen_config("model")).headers,
            vec![
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "text/event-stream".into()),
            ]
        );
        assert_eq!(
            endpoint(&AnthropicMessages, anthropic_apikey_frozen_config("model")).headers,
            vec![
                ("anthropic-version".into(), "2023-06-01".into()),
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "application/json".into()),
                (
                    "user-agent".into(),
                    format!("broca/{}", env!("CARGO_PKG_VERSION")),
                ),
            ]
        );

        let mut codeassist = google_codeassist_frozen_config("model");
        codeassist.project_id = Some("test-project".into());
        for (family, frozen) in [
            (
                &BedrockConverse as &dyn WireFamily,
                bedrock_frozen_config("model"),
            ),
            (&GoogleCodeAssist as &dyn WireFamily, codeassist),
            (
                &OpenAiResponsesChatgpt as &dyn WireFamily,
                openai_responses_chatgpt_frozen_config("model"),
            ),
        ] {
            let request = endpoint(family, frozen);
            assert!(header(&request, "x-opencode-client").is_none());
            assert!(header(&request, "x-opencode-session").is_none());
        }
    }
}
