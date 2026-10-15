//! The OpenAI ChatGPT (Codex) wire family — the `/responses` body sent to the ChatGPT
//! subscription backend (`chatgpt.com/backend-api/codex`), NOT the platform
//! `api.openai.com`.
//!
//! The request body starts from the OpenAI-Responses shape, then removes fields the stricter
//! codex backend rejects and adds codex-owned controls. This family also differs in:
//!  - ENDPOINT: `https://chatgpt.com/backend-api/codex/responses`;
//!  - HEADERS (all attached at the endpoint, AFTER render → off the cache/resume hash): the
//!    `Authorization: Bearer <chatgpt token>`, plus the codex-specific `ChatGPT-Account-Id`,
//!    `originator`, and `session-id` headers the backend expects.
//!
//! The `ChatGPT-Account-Id` is derived from the served token itself: the access token is a
//! JWT whose payload carries the account id under the claim
//! `https://api.openai.com/auth` → `chatgpt_account_id`. The transport decodes the JWT at
//! send time and sets the header — no extra credential, no vault change. Because the header
//! is off-hash, this derivation never touches the rendered bytes.
//!
//! The `session-id` header is a stable per-session correlation id (the codex backend may use
//! it for conversation threading / server-side caching), supplied by the transport. It is a
//! header (off-hash), so it does not affect rendered-byte determinism.

use broca_protocol::{
    provider::NormalizedEvent, request::CallOptions, EncryptedReasoningFormat, StepId,
};

use crate::families::openai_responses::{build_request_body, OpenAiResponsesNormalizer};
use crate::wire::{
    AuthHandle, FrozenRenderConfig, HttpMethod, HttpRequest, NormalizeError, StreamNormalizer,
    WireFamily,
};

/// The ChatGPT codex backend base. A `base_url` override replaces only the base (e.g. a mock
/// in tests); the `/responses` path is appended. base_url lives in the endpoint, not the
/// rendered body, so overriding it never changes a golden.
const DEFAULT_BASE: &str = "https://chatgpt.com/backend-api/codex";

pub struct OpenAiResponsesChatgpt;

/// Subscription-backend probes on 2026-10-05 measured three gpt-6.1-sol pairs
/// with a ~98k prefix and 45s gap: request-effort changes hit zero cached tokens;
/// configuration_update input items retained ~97.8k cached tokens.
/// gpt-5.6 rejects configuration_update with HTTP 400, so only this family opts in.
const EFFORT_UPDATE_MODEL_FAMILIES: &[&str] = &["gpt-6"];

pub(crate) struct EffortUpdate {
    pub before: usize,
    pub effort: String,
    pub synthetic_user: bool,
}

pub(crate) fn effort_plan(
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
) -> Option<(String, Vec<EffortUpdate>)> {
    if frozen.family_id != crate::spec::WireFamilyId::OpenAiResponsesChatgpt
        || !EFFORT_UPDATE_MODEL_FAMILIES.iter().any(|prefix| {
            frozen.wire_model_id == *prefix
                || frozen
                    .wire_model_id
                    .strip_prefix(prefix)
                    .is_some_and(|tail| tail.starts_with('.') || tail.starts_with('-'))
        })
    {
        return None;
    }
    let configs: Vec<FrozenRenderConfig> = opts
        .render_episodes
        .iter()
        .map(|episode| serde_json::from_value(episode.frozen_config.clone()))
        .collect::<Result<_, _>>()
        .ok()?;
    let first = configs.first()?;
    // The first episode's recorded flag decides whether the session uses input updates.
    // If it lacks the flag, even episodes appended after an upgrade keep the old request
    // shape: inserting updates would change bytes already admitted for that session.
    if !first.chatgpt_effort_updates
        || configs.iter().any(|config| {
            config.family_id != frozen.family_id || config.wire_model_id != frozen.wire_model_id
        })
    {
        return None;
    }
    fn effort(config: &FrozenRenderConfig) -> Option<&str> {
        config
            .reasoning_effort
            .as_deref()
            .or(match &config.reasoning {
                crate::policy::ReasoningPolicy::OpenAiResponsesReasoning { effort, .. } => {
                    Some(effort.as_str())
                }
                _ => None,
            })
    }
    let anchor = effort(first)?.to_owned();
    let mut in_force = anchor.as_str();
    let mut updates = Vec::new();
    for (episode, config) in opts.render_episodes.iter().zip(&configs).skip(1) {
        let next = effort(config)?;
        if next != in_force {
            if !config.chatgpt_effort_updates {
                return None;
            }
            updates.push(EffortUpdate {
                before: episode.first_user.unwrap_or(episode.prompt_start),
                effort: next.to_owned(),
                // The backend requires a user message between configuration updates. If the
                // admitted input has no user, emit a continuation user immediately after its
                // update at the episode start, rather than waiting for a later steer that
                // would move the update and change previously cached prefix bytes.
                synthetic_user: episode.first_user.is_none(),
            });
            in_force = next;
        }
    }
    Some((anchor, updates))
}

impl WireFamily for OpenAiResponsesChatgpt {
    fn warm_body(&self, _frozen: &FrozenRenderConfig, base: &[u8]) -> Result<Vec<u8>, String> {
        crate::cache_warm::responses_warm_body(base)
    }

    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> serde_json::Value {
        // The body STARTS from the shared OpenAI-Responses builder, then applies the codex
        // backend's deltas — the ways the codex/responses backend is STRICTER than the
        // platform /responses API (mirrors opencode's codex transform exactly):
        let mut body = build_request_body(frozen, opts);
        // store:false — the codex backend REJECTS a request without it ("Store must be set
        // to false"). The platform Responses API defaults store on the server side; codex
        // enforces it in the body.
        body["store"] = serde_json::Value::Bool(false);
        // The codex backend rejects platform-only output and sampling controls as unsupported.
        // They must be absent rather than defaulted, even when the caller supplied them.
        if let Some(obj) = body.as_object_mut() {
            for field in ["max_output_tokens", "temperature", "top_p"] {
                obj.remove(field);
            }
        }
        // prompt_cache_key — the run's session id, for codex backend-side prompt-cache
        // locality across the run's turns. A per-run constant frozen into the render config,
        // so a resume reproduces it from the frozen blob (deterministic, in the hash domain).
        if let Some(key) = &frozen.prompt_cache_key {
            body["prompt_cache_key"] = serde_json::Value::String(key.clone());
        }
        // ChatGPT's pro tier is the backend's maximum reasoning effort (wire-captured from
        // the native codex CLI; the models.dev reasoning.mode spelling 400s here). Insert
        // only the effort so any independently configured reasoning fields remain intact.
        if let Some(effort) = &frozen.reasoning_effort {
            let reasoning = body
                .as_object_mut()
                .expect("OpenAI Responses body is an object")
                .entry("reasoning")
                .or_insert_with(|| serde_json::json!({}));
            if !reasoning.is_object() {
                *reasoning = serde_json::json!({});
            }
            reasoning["effort"] = serde_json::Value::String(effort.clone());
        }
        if let Some((anchor, _)) = effort_plan(frozen, opts) {
            body["reasoning"]["effort"] = serde_json::json!(anchor);
        }
        if let Some(service_tier) = &frozen.service_tier {
            body["service_tier"] = serde_json::Value::String(service_tier.clone());
        }
        body
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        let base = frozen.base_url.as_deref().unwrap_or(DEFAULT_BASE);
        let url = format!("{}/responses", base.trim_end_matches('/'));
        let mut headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("accept".to_string(), "text/event-stream".to_string()),
        ];
        // The codex backend expects the Responses Authorization plus its account/originator/
        // session headers. The account id + session id are derived by the transport and
        // passed through the AuthHandle's extra headers (off-hash, post-render).
        match auth {
            AuthHandle::Bearer(token) => {
                headers.push(("authorization".to_string(), format!("Bearer {token}")));
            }
            AuthHandle::ApiKeyHeader { header, value } => {
                headers.push((header.clone(), value.clone()));
            }
            AuthHandle::SigV4 { signed_headers, .. } => {
                headers.extend(signed_headers.iter().cloned());
            }
            AuthHandle::Headers(hs) => headers.extend(hs.iter().cloned()),
            AuthHandle::None => {}
        }
        HttpRequest {
            method: HttpMethod::Post,
            url,
            headers,
            body,
        }
    }

    fn normalizer(
        &self,
        _frozen: &FrozenRenderConfig,
        step_id: StepId,
    ) -> Box<dyn StreamNormalizer> {
        // The codex backend streams the same Responses SSE event union → reuse the proven
        // OpenAI-Responses normalizer unchanged.
        Box::new(ChatgptNormalizer {
            inner: OpenAiResponsesNormalizer::new_for_format(
                step_id,
                EncryptedReasoningFormat::OpenAiResponsesChatgpt,
            ),
        })
    }
}

/// Delegates to the OpenAI-Responses normalizer unchanged (the codex backend streams the
/// same Responses SSE events). A thin wrapper so the family owns its own normalizer type.
struct ChatgptNormalizer {
    inner: OpenAiResponsesNormalizer,
}

impl StreamNormalizer for ChatgptNormalizer {
    fn websocket_telemetry(&mut self) {
        self.inner.websocket_telemetry();
    }
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.inner.content_progress()
    }
    fn private_error_diagnostics(&mut self) {
        self.inner.private_error_diagnostics();
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        self.inner.push(chunk)
    }

    fn progress_marker(&self) -> u64 {
        self.inner.progress_marker()
    }

    fn last_event_type(&self) -> Option<&str> {
        self.inner.last_event_type()
    }

    fn keepalives_after_content(&self) -> Option<u64> {
        self.inner.keepalives_after_content()
    }

    fn event_type_counts(&self) -> Option<&[(String, u64)]> {
        self.inner.event_type_counts()
    }

    fn liveness_marker(&self) -> Option<u64> {
        self.inner.liveness_marker()
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        self.inner.finish()
    }

    fn observe_response_headers(&mut self, headers: &crate::wire::ResponseHeaders) {
        self.inner.observe_response_headers(headers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::families::{openai_responses_chatgpt_frozen_config, OpenAiResponses};
    use crate::wire::{CacheClass, CachePolicy};
    use broca_protocol::content::{ContentBlock, FinishReason, Message, Role};
    use broca_protocol::ProviderControl;

    fn opts() -> CallOptions {
        CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "ping".into(),
            }],
        }])
    }

    #[test]
    fn codex_body_is_responses_plus_the_codex_delta() {
        // With default generation controls, the codex body is the platform OpenAI-Responses
        // body with exactly its required store/token-limit deltas applied.
        let frozen_chatgpt = openai_responses_chatgpt_frozen_config("gpt-5"); // no cache key
        let chatgpt_body = OpenAiResponsesChatgpt.render(&frozen_chatgpt, &opts());
        let mut frozen_as_responses = frozen_chatgpt.clone();
        frozen_as_responses.family_id = crate::spec::WireFamilyId::OpenAiResponses;
        let responses_body = OpenAiResponses.render(&frozen_as_responses, &opts());

        let codex: serde_json::Value = serde_json::from_slice(&chatgpt_body).unwrap();
        let mut responses: serde_json::Value = serde_json::from_slice(&responses_body).unwrap();

        // + store:false (codex requires it; platform body has none).
        assert_eq!(codex["store"], serde_json::json!(false));
        assert!(
            responses.get("store").is_none(),
            "platform body has no store field"
        );
        // − max_output_tokens (codex rejects its presence; platform body always emits it).
        assert!(
            codex.get("max_output_tokens").is_none(),
            "codex body must OMIT max_output_tokens (the backend rejects it)"
        );
        assert!(
            responses.get("max_output_tokens").is_some(),
            "the platform Responses body carries max_output_tokens (the stripped field)"
        );

        // Apply the inverse delta to the platform body (drop max_output_tokens, add
        // store:false) → it must then equal the default codex body exactly.
        let r = responses.as_object_mut().unwrap();
        r.remove("max_output_tokens");
        r.insert("store".to_string(), serde_json::json!(false));
        assert_eq!(
            responses, codex,
            "codex body == platform Responses body − max_output_tokens + store:false"
        );
    }

    #[test]
    fn codex_body_omits_unsupported_sampling_controls() {
        let frozen_chatgpt = openai_responses_chatgpt_frozen_config("gpt-5");
        let mut request = opts();
        let mut generation = request.generation.as_ref().clone();
        generation.temperature = Some(0.2);
        generation.top_p = Some(0.8);
        request.generation = generation.into();

        let codex: serde_json::Value =
            serde_json::from_slice(&OpenAiResponsesChatgpt.render(&frozen_chatgpt, &request))
                .unwrap();
        let mut frozen_as_responses = frozen_chatgpt.clone();
        frozen_as_responses.family_id = crate::spec::WireFamilyId::OpenAiResponses;
        let mut expected: serde_json::Value =
            serde_json::from_slice(&OpenAiResponses.render(&frozen_as_responses, &request))
                .unwrap();

        assert_eq!(expected["temperature"], serde_json::json!(0.2));
        assert_eq!(expected["top_p"], serde_json::json!(0.8));
        let expected = expected.as_object_mut().unwrap();
        expected.remove("max_output_tokens");
        expected.remove("temperature");
        expected.remove("top_p");
        expected.insert("store".into(), serde_json::json!(false));

        assert_eq!(codex, serde_json::Value::Object(expected.clone()));
    }

    #[test]
    fn pro_tier_renders_max_reasoning_effort() {
        let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.6-sol");
        frozen.reasoning_effort = Some("max".into());

        let body: serde_json::Value =
            serde_json::from_slice(&OpenAiResponsesChatgpt.render(&frozen, &opts())).unwrap();

        assert_eq!(body["model"], serde_json::json!("gpt-5.6-sol"));
        assert_eq!(body["reasoning"], serde_json::json!({"effort": "max"}));
        assert_eq!(
            body["include"],
            serde_json::json!(["reasoning.encrypted_content"])
        );
    }

    #[test]
    fn fast_service_tier_uses_priority() {
        let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.6-sol");
        frozen.service_tier = Some("priority".into());

        let body: serde_json::Value =
            serde_json::from_slice(&OpenAiResponsesChatgpt.render(&frozen, &opts())).unwrap();

        assert_eq!(body["model"], serde_json::json!("gpt-5.6-sol"));
        assert_eq!(body["service_tier"], serde_json::json!("priority"));
        assert!(body.get("reasoning").is_none());
        assert!(body.get("include").is_none());
    }

    #[test]
    fn prompt_cache_key_is_set_only_when_frozen() {
        // No key frozen → no prompt_cache_key field; key frozen → it appears (a per-run body
        // field, deterministic-from-frozen on resume).
        let no_key = openai_responses_chatgpt_frozen_config("gpt-5.5");
        let body: serde_json::Value =
            serde_json::from_slice(&OpenAiResponsesChatgpt.render(&no_key, &opts())).unwrap();
        assert!(body.get("prompt_cache_key").is_none());

        let mut with_key = no_key.clone();
        with_key.prompt_cache_key = Some("ses-codex-1".into());
        let body2: serde_json::Value =
            serde_json::from_slice(&OpenAiResponsesChatgpt.render(&with_key, &opts())).unwrap();
        assert_eq!(body2["prompt_cache_key"], serde_json::json!("ses-codex-1"));
    }

    #[test]
    fn policy_hints_do_not_override_codex_owned_cache_key() {
        let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.5");
        frozen.prompt_cache_key = Some("codex-session".into());
        frozen.cache = Some(CachePolicy {
            version: 1,
            class: CacheClass::Hints {
                cache_key: "policy-session".into(),
                retention: Some("24h".into()),
            },
        });

        let body: serde_json::Value =
            serde_json::from_slice(&OpenAiResponsesChatgpt.render(&frozen, &opts())).unwrap();
        // The codex backend already receives a run-session cache key frozen in
        // `prompt_cache_key`; adding the policy key would create two authorities
        // for the same routing hint.
        assert_eq!(body["prompt_cache_key"], serde_json::json!("codex-session"));
        assert!(body.get("prompt_cache_retention").is_none());
    }

    #[test]
    fn codex_normalizer_preserves_streamed_refusal() {
        let recorded = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_refusal\"}}\n\n",
            "data: {\"type\":\"response.refusal.delta\",\"delta\":\"I can’t \"}\n\n",
            "data: {\"type\":\"response.refusal.delta\",\"delta\":\"help with that.\"}\n\n",
            "data: {\"type\":\"response.refusal.done\",\"refusal\":\"I can’t help with that.\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_refusal\",\"status\":\"completed\"}}\n\n",
        );
        let frozen = openai_responses_chatgpt_frozen_config("gpt-5.5");
        let mut normalizer = OpenAiResponsesChatgpt.normalizer(&frozen, StepId(6));
        let mut events = Vec::new();
        for chunk in recorded.as_bytes().chunks(13) {
            events.extend(
                normalizer
                    .push(chunk)
                    .expect("Codex refusal SSE normalizes"),
            );
        }
        events.extend(normalizer.finish().expect("Codex refusal EOF is clean"));

        let (message, finish_reason) = events.iter().fold(
            (None, None),
            |(message, finish_reason), event| match event {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message: value }) => {
                    (Some(value), finish_reason)
                }
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    finish_reason: value,
                    ..
                }) => (message, Some(*value)),
                _ => (message, finish_reason),
            },
        );
        let text: String = message
            .expect("Codex refusal assembles an assistant message")
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "I can’t help with that.");
        assert_eq!(finish_reason, Some(FinishReason::ContentFilter));
    }

    #[test]
    fn endpoint_targets_the_codex_backend() {
        let frozen = openai_responses_chatgpt_frozen_config("gpt-5");
        let req = OpenAiResponsesChatgpt.endpoint(
            &frozen,
            &AuthHandle::Bearer("tok".into()),
            b"{}".to_vec(),
        );
        assert_eq!(req.url, "https://chatgpt.com/backend-api/codex/responses");
        assert!(req
            .headers
            .iter()
            .any(|(k, v)| k == "authorization" && v == "Bearer tok"));
    }

    #[test]
    fn base_url_override_replaces_only_the_base() {
        let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5");
        frozen.base_url = Some("http://127.0.0.1:9999".to_string());
        let req = OpenAiResponsesChatgpt.endpoint(&frozen, &AuthHandle::None, b"{}".to_vec());
        assert_eq!(req.url, "http://127.0.0.1:9999/responses");
    }
}
