//! The Google Antigravity / Code-Assist wire family — `cloudcode-pa` `streamGenerateContent`.
//!
//! This is the Gemini `generateContent` body wrapped in a `cloudcode-pa` ENVELOPE and sent
//! to the Code-Assist backend with an OAuth Bearer token (not an API key). The envelope
//! shape and field order are implemented from the observed Antigravity wire format:
//!
//! ```json
//! { "project": "<project id>", "requestId": "agent/.../.../.../2",
//!   "request": <gemini generateContent body>, "model": "<wire model>",
//!   "userAgent": "antigravity", "requestType": "agent" }
//! ```
//!
//! Response: each SSE `data:` chunk is wrapped `{ "response": <GenerateContentResponse> }`
//! in the observed wire format. The inner value is a standard Gemini response, so the
//! normalizer unwraps `.response` then delegates to the Gemini decode unchanged.
//!
//! New episodes freeze a signed-decimal `request.sessionId`, stable agent and trajectory
//! UUIDs, and an admission-time request counter anchor. These are envelope metadata, not
//! Gemini content. Episodes recorded without the identity keep the original envelope below.
//!
//! ## The legacy determinism gate
//! The envelope's `requestId` is BODY-VISIBLE (in the rendered bytes, so in the cache/resume
//! hash). The observed format is `agent/<uuid>/<timestamp>/<uuid>/2` — fresh per
//! request. That can't be reproduced on resume, so here it is a PURE FUNCTION of the run's
//! `(run_id, step_index)`: same shape, but the uuid-looking segments are deterministic
//! digests of the run identity and the millis-looking segment is the step index. broca uses
//! `requestId` as an opaque client correlation id; deriving it from the run and step makes
//! the rendered bytes reproducible. `render`
//! therefore REQUIRES `CallOptions.step_context`: a missing one is a hard error (never a
//! silent default), so a forgotten populate can't silently emit non-deterministic bytes.
//!
//! `project_id` is also body-visible (the `project` field), frozen into `FrozenRenderConfig`
//! at run start (resolved from the vault credential), so a resume renders the same project.
//! The Bearer token is attached at the endpoint, AFTER render — never in the hashed bytes.

use broca_protocol::{provider::NormalizedEvent, request::CallOptions, StepContext, StepId};
use serde_json::{json, Value};

use crate::families::gemini::{
    build_request_body, build_request_body_with_media, GeminiNormalizer,
};
use crate::media_render::{InlineMedia, MediaRender};
use crate::sse::SseDecoder;
use crate::wire::{
    AuthHandle, FrozenRenderConfig, HttpMethod, HttpRequest, NormalizeError, StreamNormalizer,
    WireFamily,
};

/// The daily Code-Assist endpoint observed in Antigravity requests, selected for its Gemini
/// 3.x tiers. A `base_url` override replaces only the base (e.g. a mock in tests); the path
/// is appended.
const DEFAULT_BASE: &str = "https://daily-cloudcode-pa.googleapis.com";

/// The fixed `userAgent` envelope field observed on the wire: `"antigravity"`.
const ENVELOPE_USER_AGENT: &str = "antigravity";
/// Current native Antigravity content-request identity.
///
/// Daily gates Gemini models on this client version. An A/B/A probe held the captured 3.7 body,
/// account, project, bearer token, and daily endpoint constant and changed only the User-Agent:
/// `1.0.4 darwin/arm64` returned 404, `1.1.13 darwin/arm64` returned 200, and restoring `1.0.4`
/// returned 404 again. Treat a new daily 404 as a stale client-version symptom before concluding
/// that the model is unavailable, and update both values from a current native capture.
///
/// 1.3.5 / 998863838 comes from the native CLI capture of Gemini 4 Argon Low, which the daily
/// endpoint accepted; 1.1.20 got a 404 on all three Argon tiers. Header set, header order and
/// envelope keys are unchanged from the earlier capture.
const ANTIGRAVITY_CLI_VERSION: &str = "1.3.5";
const ANTIGRAVITY_CLIENT_CL: &str = "998863838";
/// The fixed `requestType` envelope field.
const ENVELOPE_REQUEST_TYPE: &str = "agent";

pub struct GoogleCodeAssist;

fn build_envelope(frozen: &FrozenRenderConfig, opts: &CallOptions, mut inner: Value) -> Value {
    // Start with the shared Gemini body, then apply the generation defaults selected by the
    // frozen Antigravity wire tier. Different Gemini families use different thinking knobs.
    apply_antigravity_generation_defaults(&mut inner, frozen);

    // requestId MUST be deterministic — REQUIRE the run/step identity. A missing
    // step_context is a precondition violation (the loop always populates it); rendering
    // a default/empty/random id here would be a silent cache-stability break no golden
    // catches (goldens always populate it). Fail loud instead.
    let step = opts.step_context.as_ref().unwrap_or_else(|| {
        panic!(
            "google_codeassist render requires CallOptions.step_context (the deterministic \
                 requestId derives from run_id + step_index); it was None"
        )
    });
    let request_id = if let Some(identity) = &frozen.codeassist_identity {
        let index = identity.step_offset.saturating_add(step.step_index);
        inner["sessionId"] = json!(identity.session_id);
        inner["labels"] = json!({
            "last_step_index": index.saturating_sub(1).to_string(),
            "trajectory_id": identity.trajectory_id,
            "used_claude": "false",
            "used_claude_conservative": "false",
        });
        // Real clients use wall-clock millis per call. Pin the time at admission and
        // advance it per step so rendering a retry or a resumed step never reads a clock.
        let timestamp = identity
            .admitted_at_ms
            .saturating_add(step.step_index.saturating_sub(1));
        format!(
            "agent/{}/{timestamp}/{}/{index}",
            identity.agent_id, identity.trajectory_id
        )
    } else {
        build_request_id(step)
    };

    let project = frozen.project_id.clone().unwrap_or_else(|| {
        panic!("google_codeassist render requires FrozenRenderConfig.project_id; it was missing")
    });
    let envelope = json!({
        "project": project,
        "requestId": request_id,
        "request": inner,
        "model": frozen.wire_model_id,
        "userAgent": ENVELOPE_USER_AGENT,
        "requestType": ENVELOPE_REQUEST_TYPE,
    });
    // The shaped envelope value. `render` emits it with canonical (sorted-key) bytes —
    // the same determinism mechanism every JSON family uses. The backend parses JSON without
    // depending on object-key order. Sorting keys gives equivalent envelopes stable bytes for
    // cache/resume hashing.
    envelope
}

impl WireFamily for GoogleCodeAssist {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        build_envelope(frozen, opts, build_request_body(frozen, opts))
    }

    fn shape_with_media(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        payloads: &[InlineMedia<'_>],
    ) -> Value {
        let media = MediaRender { frozen, payloads };
        let inner =
            build_request_body_with_media(frozen, opts, frozen.media.as_ref().map(|_| &media));
        build_envelope(frozen, opts, inner)
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        let base = frozen.base_url.as_deref().unwrap_or(DEFAULT_BASE);
        let url = format!(
            "{}/v1internal:streamGenerateContent?alt=sse",
            base.trim_end_matches('/'),
        );
        let mut headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("accept".to_string(), "text/event-stream".to_string()),
            // Attach transport metadata after body rendering so the cache/resume hash covers the
            // rendered payload, not platform-specific headers such as this User-Agent.
            ("user-agent".to_string(), antigravity_user_agent()),
        ];
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
        frozen: &FrozenRenderConfig,
        step_id: StepId,
    ) -> Box<dyn StreamNormalizer> {
        Box::new(CodeAssistNormalizer {
            inner: GeminiNormalizer::new(step_id, frozen.tool_id).with_text_state(frozen),
            sse: SseDecoder::for_content_family(crate::content_progress::ContentFamily::CodeAssist),
        })
    }
}

fn antigravity_user_agent() -> String {
    build_antigravity_user_agent(reference_platform_name(), reference_arch_name())
}

fn build_antigravity_user_agent(platform: &str, arch: &str) -> String {
    format!(
        "antigravity/cli/{ANTIGRAVITY_CLI_VERSION} (aidev_client; os_type={}; arch={}; cl={ANTIGRAVITY_CLIENT_CL}; auth_method=consumer)",
        normalize_antigravity_os(platform),
        normalize_antigravity_arch(arch),
    )
}

fn reference_platform_name() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    }
}

fn normalize_antigravity_os(platform: &str) -> &str {
    match platform {
        "win32" => "windows",
        other => other,
    }
}

fn reference_arch_name() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "x64",
        "x86" => "ia32",
        "aarch64" => "arm64",
        other => other,
    }
}

fn normalize_antigravity_arch(arch: &str) -> &str {
    match arch {
        "x64" => "amd64",
        "ia32" => "386",
        other => other,
    }
}

/// Apply Antigravity's model-resolver defaults after the shared Gemini request is built.
/// An explicit cap is bounded by both admission's model maximum and the tier ceiling;
/// omission retains the tier default. All three inputs are frozen for resume.
fn apply_antigravity_generation_defaults(body: &mut Value, frozen: &FrozenRenderConfig) {
    if let Some(tier_ceiling) = antigravity_max_output_tokens(&frozen.wire_model_id) {
        let max_output_tokens = frozen
            .caller_max_output_tokens
            .map_or(tier_ceiling, |explicit| {
                let capped = explicit.min(tier_ceiling);
                if frozen.renderer_version >= 7 {
                    capped.min(frozen.max_output_tokens)
                } else {
                    capped
                }
            });
        body["generationConfig"]["maxOutputTokens"] = json!(max_output_tokens);
    }
    if let Some(tier) = antigravity_tier(&frozen.wire_model_id) {
        body["generationConfig"]["thinkingConfig"] = tier.thinking_config;
        if tier.omit_tool_config {
            body.as_object_mut()
                .expect("Gemini request object")
                .remove("toolConfig");
        }
    }
}

/// Whether the request body's `maxOutputTokens` is `frozen.max_output_tokens` (or bounded by
/// it), so a step that lowers that value lowers the cap sent. An Antigravity tier model without
/// a caller cap renders its tier ceiling instead, and before renderer version 7 a caller cap is
/// bounded only by the tier ceiling.
pub(crate) fn renders_frozen_output_cap(frozen: &FrozenRenderConfig) -> bool {
    antigravity_max_output_tokens(&frozen.wire_model_id).is_none()
        || (frozen.caller_max_output_tokens.is_some() && frozen.renderer_version >= 7)
}

fn antigravity_max_output_tokens(wire_model_id: &str) -> Option<u32> {
    match wire_model_id {
        "gemini-3.5-flash-low"
        | "gemini-3.5-flash-extra-low"
        | "gemini-3-flash-agent"
        | "gemini-3.6-flash-low"
        | "gemini-3.6-flash-medium"
        | "gemini-3.6-flash-high"
        | "gemini-3.7-flash-low"
        | "gemini-3.7-flash-medium"
        | "gemini-3.7-flash-high"
        | "gemini-3.8-flash-low"
        | "gemini-3.8-flash-medium"
        | "gemini-3.8-flash-high" => Some(65_536),
        "gemini-3.1-pro-low" | "gemini-pro-agent" => Some(65_535),
        _ => None,
    }
}

/// The thinking config a Code Assist backend tier id renders with, or `None` when the id is not
/// one of this family's thinking tiers. This is the family's tier table: on Code Assist a
/// reasoning level is sent as a different backend model id, and an id is a tier exactly when it
/// has thinking settings here. The catalog resolver uses it to decide which reasoning variants a
/// route can render.
pub fn tier_thinking_config(wire_model_id: &str) -> Option<Value> {
    antigravity_tier(wire_model_id).map(|tier| tier.thinking_config)
}

struct CodeAssistTier {
    thinking_config: Value,
    omit_tool_config: bool,
}

fn antigravity_tier(wire_model_id: &str) -> Option<CodeAssistTier> {
    // Native Argon requests use uppercase levels and omit toolConfig even with tools present.
    // Keep that wire policy with the tier's thinking settings so admission and render agree.
    let argon_level = match wire_model_id {
        "gemini-4-argon-low" => Some("LOW"),
        "gemini-4-argon-medium" => Some("MEDIUM"),
        "gemini-4-argon-high" => Some("HIGH"),
        _ => None,
    };
    if let Some(level) = argon_level {
        return Some(CodeAssistTier {
            thinking_config: json!({"includeThoughts": true, "thinkingLevel": level}),
            omit_tool_config: true,
        });
    }
    antigravity_thinking_config(wire_model_id).map(|thinking_config| CodeAssistTier {
        thinking_config,
        omit_tool_config: false,
    })
}

fn antigravity_thinking_config(wire_model_id: &str) -> Option<Value> {
    let budget = match wire_model_id {
        "gemini-3.5-flash-low" => Some(4_000),
        "gemini-3.5-flash-extra-low" | "gemini-3.7-flash-low" | "gemini-3.8-flash-low" => {
            Some(1_000)
        }
        "gemini-3.7-flash-medium" | "gemini-3.8-flash-medium" => Some(4_000),
        // -1 is DYNAMIC, not unlimited: the backend sizes the budget itself. 3.8 shares 3.7's
        // shape because the reference plugin resolves both through one helper called with the
        // same high budget (`getAgyGeminiFlashThinkingBudget(tier, -1)`), where 3.6 and
        // earlier take the default 10000 ceiling.
        "gemini-3.7-flash-high" | "gemini-3.8-flash-high" => Some(-1),
        "gemini-3-flash-agent" => Some(10_000),
        _ => None,
    };
    if let Some(thinking_budget) = budget {
        return Some(json!({
            "includeThoughts": true,
            "thinkingBudget": thinking_budget,
        }));
    }

    antigravity_thinking_level(wire_model_id).map(|thinking_level| {
        json!({
            "includeThoughts": true,
            "thinkingLevel": thinking_level,
        })
    })
}

fn antigravity_thinking_level(wire_model_id: &str) -> Option<&'static str> {
    for level in ["minimal", "low", "medium", "high"] {
        let Some(model) = wire_model_id
            .strip_suffix(level)
            .and_then(|prefix| prefix.strip_suffix('-'))
        else {
            continue;
        };
        // 3.6-flash carries the tier as its thinkingLevel string (low/medium/high); the plugin
        // registers exactly those three tiers for it (no `minimal`).
        if model == "gemini-3-flash"
            || (matches!(level, "low" | "medium" | "high") && model == "gemini-3.6-flash")
            || (matches!(level, "low" | "high") && is_gemini_3_pro(model))
        {
            return Some(level);
        }
    }
    None
}

fn is_gemini_3_pro(model: &str) -> bool {
    if model == "gemini-3-pro" {
        return true;
    }
    model
        .strip_prefix("gemini-3.")
        .and_then(|suffix| suffix.strip_suffix("-pro"))
        .is_some_and(|minor| !minor.is_empty() && minor.chars().all(|c| c.is_ascii_digit()))
}

/// The deterministic `requestId` for a step: `agent/<a>/<b>/<c>/2`, where `a`/`c` are
/// uuid-shaped digests of the run identity and `b` is the step index (the reference's
/// `Date.now()` position — an opaque number the backend echoes). A pure function of
/// `(run_id, step_index)`, so render-twice and resume reproduce identical bytes.
fn build_request_id(step: &StepContext) -> String {
    let a = uuid_shaped_digest(&step.run_id);
    let c = uuid_shaped_digest(&format!("{}|{}", step.run_id, step.step_index));
    format!("agent/{a}/{b}/{c}/2", b = step.step_index)
}

/// A 32-hex-digit, uuid-formatted (`8-4-4-4-12`) deterministic digest of `seed`. Uses a
/// stable, explicit FNV-1a-derived hash (NOT `DefaultHasher`, whose output isn't stable
/// across releases/platforms) so the same seed yields the same hex on every build and host —
/// the requestId must be reproducible for cache stability, including across a resume on a
/// different process/version.
fn uuid_shaped_digest(seed: &str) -> String {
    // Derive 16 bytes by chaining four FNV-1a passes with different salts, then format as a
    // uuid. The value need not be a real uuid (the backend doesn't validate it) — only
    // uuid-SHAPED and deterministic.
    let mut bytes = [0u8; 16];
    for (i, chunk) in bytes.chunks_mut(8).enumerate() {
        let h = crate::hash::fnv1a64(seed, i as u64);
        chunk.copy_from_slice(&h.to_be_bytes());
    }
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32],
    )
}

/// Unwraps the cloudcode-pa `{response: ...}` SSE envelope per record, then feeds the inner
/// `GenerateContentResponse` through the proven Gemini decode. The only NEW logic vs Gemini
/// is the one-line `.response` unwrap (with a fallback: a bare response — no `response` key —
/// is fed through directly, matching the reference's both-shapes guard).
struct CodeAssistNormalizer {
    inner: GeminiNormalizer,
    sse: SseDecoder,
}

impl StreamNormalizer for CodeAssistNormalizer {
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.sse.content_progress()
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut out = Vec::new();
        for payload in self.sse.push(chunk)? {
            if payload.trim() == "[DONE]" {
                continue;
            }
            let parsed: Value = serde_json::from_str(&payload)
                .map_err(|e| NormalizeError::Invalid(e.to_string()))?;
            // cloudcode-pa wraps each chunk as { response: <GenerateContentResponse> }; a
            // bare response (no wrapper) is fed through directly (the reference handles both).
            let response = match parsed.get("response") {
                Some(r) => r,
                None => &parsed,
            };
            self.inner.handle_response_value(response, &mut out)?;
        }
        Ok(out)
    }

    fn progress_marker(&self) -> u64 {
        self.sse.data_records()
    }

    fn last_event_type(&self) -> Option<&str> {
        self.sse.last_event_type()
    }

    fn keepalives_after_content(&self) -> Option<u64> {
        Some(self.sse.keepalives_after_content())
    }

    fn event_type_counts(&self) -> Option<&[(String, u64)]> {
        Some(self.sse.event_type_counts())
    }

    fn liveness_marker(&self) -> Option<u64> {
        Some(self.sse.liveness_records())
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut out = Vec::new();
        for payload in self.sse.flush()? {
            if payload.trim() == "[DONE]" {
                continue;
            }
            let parsed: Value = serde_json::from_str(&payload)
                .map_err(|e| NormalizeError::Invalid(e.to_string()))?;
            let response = match parsed.get("response") {
                Some(r) => r,
                None => &parsed,
            };
            self.inner.handle_response_value(response, &mut out)?;
        }
        out.extend(self.inner.finish()?);
        Ok(out)
    }

    /// Forwarded so the Gemini decode reads `x-gemini-service-tier` if this backend sends it
    /// (it also reads `usageMetadata.serviceTier` inside the unwrapped body). No Code Assist
    /// response has been recorded to show either, so the tier is recorded only if one appears
    /// and is never inferred when both are missing.
    fn observe_response_headers(&mut self, headers: &crate::wire::ResponseHeaders) {
        self.inner.observe_response_headers(headers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{
        content::{ContentBlock, FinishReason, Message, Role, ToolCall, ToolOutput, ToolResult},
        provider::ProviderControl,
        ToolCallId,
    };

    fn user(text: &str) -> Message {
        Message {
            cache_prefix_blocks: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }
    }

    fn step(run_id: &str, idx: u64) -> StepContext {
        StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: run_id.to_string(),
            step_index: idx,
            max_output_tokens: None,
        }
    }

    #[test]
    fn request_id_is_deterministic_and_shaped() {
        let id1 = build_request_id(&step("run-abc", 3));
        let id2 = build_request_id(&step("run-abc", 3));
        assert_eq!(id1, id2, "same (run_id, step) → identical requestId");
        // Shape: agent/<uuid>/<step>/<uuid>/2
        let parts: Vec<&str> = id1.split('/').collect();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[0], "agent");
        assert_eq!(parts[2], "3", "the millis position is the step index");
        assert_eq!(parts[4], "2");
        // uuid-shaped segments: 8-4-4-4-12 hex.
        for seg in [parts[1], parts[3]] {
            let groups: Vec<&str> = seg.split('-').collect();
            assert_eq!(
                groups.iter().map(|g| g.len()).collect::<Vec<_>>(),
                vec![8, 4, 4, 4, 12]
            );
            assert!(seg.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        }
    }

    #[test]
    fn request_id_varies_by_run_and_step() {
        let base = build_request_id(&step("run-abc", 3));
        assert_ne!(
            base,
            build_request_id(&step("run-xyz", 3)),
            "differs by run"
        );
        assert_ne!(
            base,
            build_request_id(&step("run-abc", 4)),
            "differs by step"
        );
    }

    fn frozen() -> FrozenRenderConfig {
        let mut f = crate::families::google_codeassist_frozen_config("gemini-2.5-flash");
        f.project_id = Some("test-project".to_string());
        f
    }

    fn opts_with_step() -> CallOptions {
        let mut o = CallOptions::new(vec![user("hi")]);
        o.step_context = Some(step("run-1", 1));
        o
    }

    fn identity_frozen() -> FrozenRenderConfig {
        let mut frozen = frozen();
        frozen.codeassist_identity = Some(crate::wire::CodeAssistIdentity {
            session_id: "-123456789".into(),
            agent_id: "11111111-1111-4111-8111-111111111111".into(),
            trajectory_id: "22222222-2222-4222-8222-222222222222".into(),
            step_offset: 1,
            admitted_at_ms: 1_700_000_000_000,
        });
        frozen
    }

    #[test]
    fn session_id_is_stable_across_runs() {
        let frozen = identity_frozen();
        let mut opts = opts_with_step();
        let first = GoogleCodeAssist.shape(&frozen, &opts);
        opts.step_context = Some(step("another-run", 4));
        let second = GoogleCodeAssist.shape(&frozen, &opts);
        assert_eq!(first["request"]["sessionId"], json!("-123456789"));
        assert_eq!(
            first["request"]["sessionId"],
            second["request"]["sessionId"]
        );
        assert_eq!(
            first["requestId"].as_str().unwrap().split('/').nth(1),
            second["requestId"].as_str().unwrap().split('/').nth(1)
        );
        assert_eq!(
            first["requestId"].as_str().unwrap().split('/').nth(3),
            second["requestId"].as_str().unwrap().split('/').nth(3)
        );
    }

    #[test]
    fn session_labels_track_the_request_step_and_trajectory() {
        let frozen = identity_frozen();
        let mut opts = opts_with_step();
        for index in [1, 2, 10] {
            opts.step_context = Some(step("run-1", index));
            let body = GoogleCodeAssist.shape(&frozen, &opts);
            assert_eq!(
                body["request"]["labels"],
                json!({
                    "last_step_index": index.to_string(),
                    "trajectory_id": "22222222-2222-4222-8222-222222222222",
                    "used_claude": "false",
                    "used_claude_conservative": "false",
                })
            );
            assert_eq!(
                body["requestId"].as_str().unwrap().rsplit('/').next(),
                Some((index + 1).to_string().as_str())
            );
            assert!(body["request"]["labels"].get("last_execution_id").is_none());
        }
    }

    #[test]
    fn wrapped_prompt_feedback_safety_without_candidates_is_content_filtered() {
        let mut normalizer = CodeAssistNormalizer {
            inner: GeminiNormalizer::new(StepId(1), crate::policy::ToolIdPolicy::SyntheticIndex),
            sse: SseDecoder::default(),
        };
        let record = format!(
            "data: {}\n\n",
            json!({
                "response": {
                    "promptFeedback": {"blockReason": "SAFETY"}
                }
            })
        );
        let mut events = normalizer.push(record.as_bytes()).unwrap();
        events.extend(normalizer.finish().unwrap());

        let message = events.iter().find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        });
        assert!(message.is_some_and(|message| message.content.is_empty()));
        let reasons: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    finish_reason, ..
                })
                | NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) => {
                    Some(*finish_reason)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            reasons,
            vec![FinishReason::ContentFilter, FinishReason::ContentFilter]
        );
    }

    #[test]
    fn wrapped_clean_eof_without_finish_reason_is_truncation() {
        let mut normalizer = GoogleCodeAssist.normalizer(&frozen(), StepId(1));
        let record = b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}],\"role\":\"model\"},\"index\":0}]}}\n\n";

        let events = normalizer.push(record).expect("wrapped content delta");
        assert!(!events.iter().any(|event| matches!(
            event,
            NormalizedEvent::Control(
                ProviderControl::AssistantMessage { .. }
                    | ProviderControl::StepFinished { .. }
                    | ProviderControl::Finished { .. }
            )
        )));
        let error = normalizer
            .finish()
            .expect_err("wrapped EOF without a finish reason must be rejected");
        assert!(
            error
                .to_string()
                .contains("Gemini stream truncated: ended before a terminal chunk"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn render_is_deterministic_with_step_context() {
        let a = GoogleCodeAssist.render(&frozen(), &opts_with_step());
        let b = GoogleCodeAssist.render(&frozen(), &opts_with_step());
        assert_eq!(a, b, "render-twice with same identity → byte-identical");
        // The envelope carries the project + the gemini body nested under `request`.
        let v: Value = serde_json::from_slice(&a).unwrap();
        assert_eq!(v["project"], json!("test-project"));
        assert_eq!(v["userAgent"], json!("antigravity"));
        assert_eq!(v["requestType"], json!("agent"));
        assert!(v.get("request").and_then(|r| r.get("contents")).is_some());
    }

    #[test]
    fn codeassist_inherits_mixed_user_tool_result_rendering() {
        let mut opts = CallOptions::new(vec![
            Message::new(
                Role::Assistant,
                vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("call_lookup".into()),
                    tool_name: "lookup".into(),
                    input: json!({}),
                    thought_signature: None,
                })],
            ),
            Message::new(
                Role::User,
                vec![
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: ToolCallId("call_lookup".into()),
                        output: ToolOutput::Text { text: "ok".into() },
                        is_error: false,
                    }),
                    ContentBlock::Text {
                        text: "Continue".into(),
                    },
                ],
            ),
        ]);
        opts.step_context = Some(step("run-1", 1));

        let rendered: Value =
            serde_json::from_slice(&GoogleCodeAssist.render(&frozen(), &opts)).unwrap();

        assert_eq!(
            rendered["request"]["contents"][1]["parts"],
            json!([
                {
                    "functionResponse": {
                        "name": "lookup",
                        "response": {"result": "ok"}
                    }
                },
                {"text": "Continue"}
            ])
        );
    }

    #[test]
    fn endpoint_uses_daily_host_and_native_content_user_agent() {
        let request = GoogleCodeAssist.endpoint(&frozen(), &AuthHandle::None, Vec::new());
        assert_eq!(
            request.url,
            "https://daily-cloudcode-pa.googleapis.com/\
             v1internal:streamGenerateContent?alt=sse"
        );
        let user_agent = request
            .headers
            .iter()
            .find_map(|(name, value)| (name == "user-agent").then_some(value.as_str()));
        assert_eq!(user_agent, Some(antigravity_user_agent().as_str()));
        assert!(user_agent.unwrap().starts_with("antigravity/cli/1.3.5 "));
        assert!(user_agent.unwrap().contains("cl=998863838"));
    }

    #[test]
    fn native_content_user_agent_is_exact_and_normalizes_reference_platform_names() {
        assert_eq!(
            build_antigravity_user_agent("darwin", "arm64"),
            "antigravity/cli/1.3.5 (aidev_client; os_type=darwin; arch=arm64; \
             cl=998863838; auth_method=consumer)"
        );
        assert_eq!(
            build_antigravity_user_agent("win32", "x64"),
            "antigravity/cli/1.3.5 (aidev_client; os_type=windows; arch=amd64; \
             cl=998863838; auth_method=consumer)"
        );
        assert_eq!(normalize_antigravity_arch("ia32"), "386");
        assert_eq!(normalize_antigravity_os("linux"), "linux");
        assert_eq!(normalize_antigravity_arch("arm64"), "arm64");
    }

    #[test]
    fn resolved_model_generation_defaults_match_antigravity_tiers() {
        let budgets = [
            ("gemini-3.5-flash-low", 4_000),
            ("gemini-3.5-flash-extra-low", 1_000),
            ("gemini-3-flash-agent", 10_000),
            ("gemini-3.7-flash-low", 1_000),
            ("gemini-3.7-flash-medium", 4_000),
            ("gemini-3.7-flash-high", -1),
            // 3.8 mirrors 3.7 because the reference plugin routes both through one helper with
            // the same high budget; the tiers were confirmed live before the rows were added.
            ("gemini-3.8-flash-low", 1_000),
            ("gemini-3.8-flash-medium", 4_000),
            ("gemini-3.8-flash-high", -1),
        ];
        for (model, budget) in budgets {
            assert_eq!(
                antigravity_thinking_config(model),
                Some(json!({ "thinkingBudget": budget, "includeThoughts": true }))
            );
            assert_eq!(antigravity_max_output_tokens(model), Some(65_536));
        }

        let levels = [
            ("gemini-3-flash-minimal", "minimal"),
            ("gemini-3-flash-low", "low"),
            ("gemini-3-flash-medium", "medium"),
            ("gemini-3-flash-high", "high"),
            ("gemini-3-pro-low", "low"),
            ("gemini-3.1-pro-high", "high"),
            // 3.6-flash tiers carry the tier as a thinkingLevel string (low/medium/high).
            ("gemini-3.6-flash-low", "low"),
            ("gemini-3.6-flash-medium", "medium"),
            ("gemini-3.6-flash-high", "high"),
        ];
        for (model, level) in levels {
            assert_eq!(
                antigravity_thinking_config(model),
                Some(json!({ "thinkingLevel": level, "includeThoughts": true }))
            );
        }
        // 3.6-flash has no `minimal` tier (the plugin registers only low/medium/high).
        assert_eq!(
            antigravity_thinking_config("gemini-3.6-flash-minimal"),
            None
        );
        for tier_id in [
            "gemini-3.6-flash-low",
            "gemini-3.6-flash-medium",
            "gemini-3.6-flash-high",
        ] {
            assert_eq!(antigravity_max_output_tokens(tier_id), Some(65_536));
        }

        assert_eq!(
            antigravity_max_output_tokens("gemini-3.1-pro-low"),
            Some(65_535)
        );
        assert_eq!(
            antigravity_max_output_tokens("gemini-pro-agent"),
            Some(65_535)
        );
        assert_eq!(antigravity_thinking_config("gemini-2.5-flash"), None);
        assert_eq!(antigravity_thinking_config("gemini-3-pro-medium"), None);
        assert_eq!(antigravity_max_output_tokens("gemini-2.5-flash"), None);
    }

    #[test]
    #[should_panic(expected = "project_id")]
    fn render_without_project_id_is_a_hard_error() {
        let mut missing = frozen();
        missing.project_id = None;
        let _ = GoogleCodeAssist.render(&missing, &opts_with_step());
    }

    #[test]
    #[should_panic(expected = "step_context")]
    fn render_without_step_context_is_a_hard_error() {
        // The Option footgun guard: a missing step_context must FAIL, never render a default.
        let opts = CallOptions::new(vec![user("hi")]);
        let _ = GoogleCodeAssist.render(&frozen(), &opts);
    }

    #[test]
    fn normalizer_unwraps_response_envelope() {
        let f = frozen();
        let mut n = GoogleCodeAssist.normalizer(&f, StepId(1));
        // A cloudcode-pa chunk: { response: { candidates: [...] } }.
        let chunk = b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"}]},\"finishReason\":\"STOP\"}]}}\n\n";
        let _ = n.push(chunk).unwrap();
        let final_events = n.finish().unwrap();
        // The assembled message carries the decoded text — proving the .response unwrap fed
        // the Gemini decode.
        let assembled_text = final_events.iter().find_map(|e| match e {
            NormalizedEvent::Control(broca_protocol::ProviderControl::AssistantMessage {
                message,
            }) => Some(message.content.clone()),
            _ => None,
        });
        let content = assembled_text.expect("an assistant message was assembled");
        let has_hello = content.iter().any(
            |b| matches!(b, broca_protocol::content::ContentBlock::Text { text } if text == "hello"),
        );
        assert!(
            has_hello,
            "decoded the text from inside the response envelope"
        );
    }

    /// Code-Assist wraps each Gemini response in a `{response: ...}` envelope and feeds
    /// the inner value through the Gemini decode unchanged. The reasoning-token split
    /// (`thoughtsTokenCount`) lives on that inner `usageMetadata`, so the wrapped variant
    /// must inherit the Gemini reasoning mapping for free — this test pins that
    /// inheritance: a wrapped chunk carrying `thoughtsTokenCount` surfaces
    /// `reasoning_tokens` and sums candidates + thoughts into the inclusive
    /// `output_tokens` total (thoughts are a SIBLING of candidates, not a subset).
    #[test]
    fn wrapped_gemini_thoughts_token_count_inherits_reasoning_mapping() {
        let mut normalizer = GoogleCodeAssist.normalizer(&frozen(), StepId(1));
        let chunk = b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"ok\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":12,\"cachedContentTokenCount\":3,\"candidatesTokenCount\":40,\"thoughtsTokenCount\":17}}}\n\n";
        let mut events = normalizer.push(chunk).expect("wrapped usage chunk");
        events.extend(normalizer.finish().expect("clean EOF"));
        let usage = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => {
                    Some(*usage)
                }
                _ => None,
            })
            .expect("step finished with usage");
        assert_eq!(
            usage.reasoning_tokens,
            Some(17),
            "wrapped thoughtsTokenCount surfaces as reasoning_tokens"
        );
        assert_eq!(
            usage.output_tokens,
            Some(57),
            "output_tokens is candidates + thoughts (inclusive total)"
        );
        assert_eq!(
            usage.fill_tokens(),
            Some(12),
            "fill is input + cache-read only"
        );
    }
}
