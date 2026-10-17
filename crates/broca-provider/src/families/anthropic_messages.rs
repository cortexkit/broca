//! The Anthropic Messages wire family — `/v1/messages`.
//!
//! Render and normalize are feature-free (no transport): the request body is built
//! from the frozen render config, and the response normalizer wraps the existing
//! incremental SSE splitter + `AnthropicNormalizer`. The live HTTP/SSE driver
//! (the `live` feature) attaches a token and reqwest around this family.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolOutput},
    request::{CallOptions, Tool, ToolChoice},
    NormalizedEvent, StepId,
};
use serde_json::{json, Value};

use crate::cache_markers::{
    plan_markers, DeclaredMarker, MarkerPlan, MarkerTarget, MarkerViewMessage,
};
use crate::claude_code_persona as persona_rules;
use crate::families::apply_generation_controls;
use crate::normalize::AnthropicNormalizer;
use crate::policy::{ReasoningPolicy, SystemPromptPolicy, ThinkingEchoScope};
use crate::sse::SseDecoder;
use crate::wire::{
    AuthHandle, CacheClass, CacheTail, CacheTtl, ClaudeCodePersona, FrozenRenderConfig, HttpMethod,
    HttpRequest, MarkerSyntax, NormalizeError, StreamNormalizer, WireFamily,
};

/// The default API base (no `base_url` override). The family's `/v1/messages` path
/// is appended; a base_url override (an Anthropic-compatible host, or a mock server
/// in tests) replaces only the base. base_url lives in the endpoint, not the rendered
/// body, so it's outside the cache/resume hash — overriding it never changes a golden.
const DEFAULT_BASE: &str = "https://api.anthropic.com";
use cortexkit_anthropic_policy::{
    credential_header, header_policy, AuthKind, Features, HeaderPolicy, ANTHROPIC_VERSION,
};
/// The Claude Code client identity, sent (with `x-app: cli`) ONLY on the OAuth identity-lead
/// shape (`SystemPromptPolicy::RequiredIdentityLead`), where broca speaks as Claude Code. Every
/// other Anthropic Messages request, such as a plain api-key one, identifies as broca instead
/// (see `user_agent_headers`). ANTHROPIC GATES MODEL ACCESS ON THIS VERSION for the OAuth shape,
/// so it is a live compatibility constant rather than cosmetic telemetry.
///
/// Measured 2026-09-23: at 2.1.177 `claude-opus-5-5` was refused with a 400,
/// `claude_code_version_too_old`, "Claude Code 2.1.177 does not support this model; version
/// 2.1.280 or newer is required" — while `claude-opus-5` on the same identity kept serving.
/// So the gate is PER MODEL, not a blanket floor: an older constant fails silently for exactly
/// the newest models and looks like a model problem rather than a client one.
///
/// SYNAPSE reported it as an `anthropic-auth` version, which is the natural reading because
/// that crate has a `CLAUDE_CODE_VERSION` too. broca does not link it — 0 occurrences in
/// Cargo.lock — and builds this header itself, so bumping anything there fixes nothing here.
///
/// The same shape cost a day on the Antigravity route in August, where `cloudcode-pa` gated on
/// the client version in the User-Agent and a 404 read as a dead endpoint.
///
/// The header is outside the request body, so it is not in the cache or resume hash and a bump
/// changes no golden.
const USER_AGENT: &str = "claude-cli/2.1.280 (external, cli)";
/// broca's own client identity, sent on every Anthropic Messages request that is not the OAuth
/// identity-lead shape. Carries the workspace release version.
const BROCA_USER_AGENT: &str = concat!("broca/", env!("CARGO_PKG_VERSION"));

/// Broca's user-agent follows its generated body identity. Native Claude Code gateways
/// retain their own client's user-agent instead of copying either Broca identity.
fn user_agent_headers(frozen: &FrozenRenderConfig) -> Vec<(String, String)> {
    if matches!(
        frozen.system_prompt,
        SystemPromptPolicy::RequiredIdentityLead { .. }
    ) {
        vec![("user-agent".to_string(), USER_AGENT.to_string())]
    } else {
        vec![("user-agent".to_string(), BROCA_USER_AGENT.to_string())]
    }
}

/// Map Broca's frozen body shape onto shared header inputs. A required Claude Code
/// system-identity lead selects the OAuth beta and x-app, even in token-free render
/// tests. Interleaved thinking also requests reports of failed signed-block prefix
/// checks, but never opts the body into prefix-mismatch enforcement.
fn anthropic_header_policy(frozen: &FrozenRenderConfig) -> HeaderPolicy {
    let auth_kind = if matches!(
        frozen.system_prompt,
        SystemPromptPolicy::RequiredIdentityLead { .. }
    ) {
        AuthKind::OAuth
    } else {
        AuthKind::ApiKey
    };
    let thinking = matches!(
        frozen.reasoning,
        ReasoningPolicy::AnthropicInterleaved { .. }
    );
    header_policy(
        auth_kind,
        Features {
            interleaved_thinking: thinking,
            thinking_binding_controls: thinking,
            fast_mode: frozen.speed.is_some(),
        },
    )
}

fn anthropic_beta_header(frozen: &FrozenRenderConfig) -> Option<String> {
    anthropic_header_policy(frozen).beta
}

/// The frozen Claude Code persona, when this run speaks as one: an OAuth identity-lead run
/// admitted with a persona. A run recorded without one, and every api-key run, renders the
/// shape it always had.
fn persona(frozen: &FrozenRenderConfig) -> Option<&ClaudeCodePersona> {
    match frozen.system_prompt {
        SystemPromptPolicy::RequiredIdentityLead { .. } => frozen.claude_code_persona.as_ref(),
        _ => None,
    }
}

/// The unsigned billing block for `messages`, or `None` when they hold no user message (the
/// reference client adds the block only then). Its text depends only on the frozen persona
/// (with the session's pinned `cc_version` suffix) and, for a persona recorded before the
/// suffix was pinned, the first user message, so it is the same on every step of a run.
fn billing_block(persona: &ClaudeCodePersona, messages: &[Value]) -> Option<Value> {
    if !messages
        .iter()
        .any(|message| message.get("role").and_then(Value::as_str) == Some("user"))
    {
        return None;
    }
    let suffix = match &persona.cc_version_suffix {
        Some(pinned) => pinned.clone(),
        None => persona_rules::cc_version_suffix(
            &persona_rules::first_user_message_text(messages),
            &persona.version,
        ),
    };
    let text = persona_rules::billing_header_text(
        &persona.version,
        &suffix,
        persona_rules::CCH_PLACEHOLDER,
        None,
        persona.prompt_id.as_deref(),
    );
    Some(json!({ "type": "text", "text": text }))
}

/// The `cc_version` suffix for a transcript under client `version`: computed from its first user
/// message exactly as the renderer reads that message off the wire, or `None` when the
/// transcript holds no user message. Admission uses it to pin a session's suffix from the
/// session's first durable user message.
pub fn cc_version_suffix_for_transcript(messages: &[Message], version: &str) -> Option<String> {
    let first_user = messages.iter().find(|message| message.role == Role::User)?;
    let single = std::slice::from_ref(first_user);
    let id_map = crate::tool_id_alias::ToolIdMap::build(single, &id_grammar());
    let name_map = crate::tool_name_alias::ToolNameMap::build(&[], single);
    let wire = convert_message(first_user, false, &id_map, &name_map);
    Some(persona_rules::cc_version_suffix(
        &persona_rules::first_user_message_text(std::slice::from_ref(&wire)),
        version,
    ))
}

/// A persona body's bytes: written in the persona's key order, then signed (`cch`). These are
/// the bytes the run records, retries and dumps, so every copy carries the same signature.
fn persona_bytes(body: &Value) -> Vec<u8> {
    persona_rules::sign_body(&persona_rules::to_persona_json(body)).into_bytes()
}

/// The fixed Claude Code client headers that describe the SDK and runtime the reference client
/// runs on. Only the OS and architecture come from this machine.
const STAINLESS_PACKAGE_VERSION: &str = "0.112.1";
const STAINLESS_RUNTIME_VERSION: &str = "v26.3.0";

fn stainless_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "MacOS",
        "windows" => "Windows",
        "linux" => "Linux",
        "freebsd" => "FreeBSD",
        _ => "Unknown",
    }
}

fn stainless_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        "x86" => "x32",
        other => other,
    }
}

/// A fresh v4 UUID for `x-client-request-id`, a per-request correlation id. It is drawn from
/// the standard library's randomly keyed SipHash over a process counter: unpredictable enough
/// for a correlation id, which guards nothing, and available without the `live` feature.
fn client_request_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let state = std::collections::hash_map::RandomState::new();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0u8; 16];
    for (half, chunk) in bytes.chunks_mut(8).enumerate() {
        let mut hasher = state.build_hasher();
        hasher.write_u64(count);
        hasher.write_usize(half);
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }
    crate::wire::uuid_v4_text(bytes)
}

/// The request a persona run sends: the reference client's URL (`?beta=true`) and headers,
/// in the order its runtime writes them to the wire, with the beta list chosen from the final
/// signed body.
fn persona_endpoint(
    frozen: &FrozenRenderConfig,
    persona: &ClaudeCodePersona,
    auth: &AuthHandle,
    body: Vec<u8>,
) -> HttpRequest {
    let base = frozen.base_url.as_deref().unwrap_or(DEFAULT_BASE);
    let url = format!("{}/v1/messages?beta=true", base.trim_end_matches('/'));
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let betas = persona_rules::select_betas(&parsed, &frozen.wire_model_id, &[]);
    let mut headers = vec![("accept".to_string(), "application/json".to_string())];
    attach_auth(&mut headers, auth);
    headers.extend([
        ("content-type".to_string(), "application/json".to_string()),
        (
            "user-agent".to_string(),
            format!("claude-cli/{} (external, cli)", persona.version),
        ),
        ("anthropic-beta".to_string(), betas),
        (
            "anthropic-dangerous-direct-browser-access".to_string(),
            "true".to_string(),
        ),
        (
            "anthropic-version".to_string(),
            ANTHROPIC_VERSION.to_string(),
        ),
        ("x-app".to_string(), "cli".to_string()),
        (
            "x-claude-code-session-id".to_string(),
            persona.session_id.clone(),
        ),
        ("x-client-request-id".to_string(), client_request_id()),
        ("x-stainless-arch".to_string(), stainless_arch().to_string()),
        ("x-stainless-lang".to_string(), "js".to_string()),
        ("x-stainless-os".to_string(), stainless_os().to_string()),
        (
            "x-stainless-package-version".to_string(),
            STAINLESS_PACKAGE_VERSION.to_string(),
        ),
        ("x-stainless-retry-count".to_string(), "0".to_string()),
        ("x-stainless-runtime".to_string(), "node".to_string()),
        (
            "x-stainless-runtime-version".to_string(),
            STAINLESS_RUNTIME_VERSION.to_string(),
        ),
        ("x-stainless-timeout".to_string(), "600".to_string()),
    ]);
    HttpRequest {
        method: HttpMethod::Post,
        url,
        headers,
        body,
    }
}

/// Attach the credential. Auth attaches after render and never enters the rendered bytes.
fn attach_auth(headers: &mut Vec<(String, String)>, auth: &AuthHandle) {
    match auth {
        AuthHandle::Bearer(token) => {
            let credential = credential_header(AuthKind::OAuth);
            headers.push((
                credential.name.to_string(),
                format!("{}{token}", credential.prefix),
            ));
        }
        AuthHandle::ApiKeyHeader { header, value } => {
            // AuthHandle also supports compatible hosts with nonstandard key headers.
            headers.push((header.clone(), value.clone()));
        }
        AuthHandle::SigV4 { signed_headers, .. } => {
            headers.extend(signed_headers.iter().cloned());
        }
        AuthHandle::Headers(hs) => headers.extend(hs.iter().cloned()),
        AuthHandle::None => {}
    }
}

/// The Anthropic Messages family. Stateless — all per-run config is in the frozen
/// render config passed to each method.
pub struct AnthropicMessages;

impl WireFamily for AnthropicMessages {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        build_request_body(frozen, opts)
    }

    /// A persona run's bytes are the persona's key order with the `cch` signature written in;
    /// every other run keeps the canonical JSON of its shape.
    fn render(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Vec<u8> {
        let body = build_request_body(frozen, opts);
        if persona(frozen).is_some() {
            persona_bytes(&body)
        } else {
            broca_protocol::canonical_json_bytes(&body)
        }
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        if let Some(persona) = persona(frozen) {
            return persona_endpoint(frozen, persona, auth, body);
        }
        let base = frozen.base_url.as_deref().unwrap_or(DEFAULT_BASE);
        let url = format!("{}/v1/messages", base.trim_end_matches('/'));
        let mut headers = vec![
            (
                "anthropic-version".to_string(),
                ANTHROPIC_VERSION.to_string(),
            ),
            ("content-type".to_string(), "application/json".to_string()),
            ("accept".to_string(), "application/json".to_string()),
        ];
        headers.extend(user_agent_headers(frozen));
        // The anthropic-beta header is policy-driven: the OAuth beta only on the OAuth
        // (identity-lead) shape, never on a plain api-key request.
        let policy = anthropic_header_policy(frozen);
        if let Some(x_app) = policy.x_app {
            headers.push(("x-app".to_string(), x_app.to_string()));
        }
        if let Some(beta) = anthropic_beta_header(frozen) {
            headers.push(("anthropic-beta".to_string(), beta));
        }
        // Auth attaches HERE, after render — never on the rendered/hashed bytes.
        attach_auth(&mut headers, auth);
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
        Box::new(AnthropicStreamNormalizer {
            sse: SseDecoder::for_content_family(crate::content_progress::ContentFamily::Anthropic),
            // The body renders `max_tokens` from this same field, so an output-ceiling error
            // names the cap the provider actually enforced.
            inner: AnthropicNormalizer::new(step_id)
                .with_max_output_tokens(frozen.max_output_tokens),
        })
    }

    fn warm_body(&self, frozen: &FrozenRenderConfig, base: &[u8]) -> Result<Vec<u8>, String> {
        let warm = crate::cache_warm::anthropic_warm_body(base)?;
        if persona(frozen).is_none() {
            return Ok(warm);
        }
        // The warm body changes the sent body, so a persona warm is re-emitted in the persona's
        // key order and signed again; its billing block is otherwise the one `base` carried.
        let body: Value = serde_json::from_slice(&warm)
            .map_err(|error| format!("the warm body is not JSON: {error}"))?;
        Ok(persona_bytes(&body))
    }
}

/// Wraps the incremental SSE splitter + `AnthropicNormalizer` behind the
/// grammar-generic `StreamNormalizer` interface.
struct AnthropicStreamNormalizer {
    sse: SseDecoder,
    inner: AnthropicNormalizer,
}

impl StreamNormalizer for AnthropicStreamNormalizer {
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.sse.content_progress()
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut out = Vec::new();
        for payload in self.sse.push(chunk)? {
            let events = self
                .inner
                .push_json(&payload)
                .map_err(crate::normalize::NormalizeError::into_wire)?;
            out.extend(events);
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

    fn input_transformations(&self) -> Option<Vec<broca_protocol::InputTransformation>> {
        self.inner.input_transformations()
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut out = Vec::new();
        for payload in self.sse.flush()? {
            let events = self
                .inner
                .push_json(&payload)
                .map_err(crate::normalize::NormalizeError::into_wire)?;
            out.extend(events);
        }
        Ok(out)
    }
}

// ───────────────────────── request body ─────────────────────────

/// Build the Anthropic Messages request body from the frozen config + this step's
/// `CallOptions`. A pure function of its inputs.
fn build_request_body(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    // A run with structured output adds its submit tool or schema instructions here; a run
    // without renders from `opts` unchanged.
    let prepared = crate::structured_output::prepare(frozen, opts);
    let opts = prepared.as_ref();
    let view = crate::families::compaction_view(frozen.family_id, opts);
    let opts = view.as_ref();
    let marker_policy = anthropic_marker_policy(frozen);

    // System: a required identity may lead (an OAuth requirement), then any system
    // messages from the prompt.
    let mut system = build_system(frozen, opts, marker_policy.is_some());

    // Interleaved thinking: which assistant messages echo their signed thinking is decided by
    // the frozen `echo_scope` -- either just the last assistant message (the original rule,
    // kept so runs frozen under it resume with their recorded bytes) or every assistant
    // message (what current runs freeze). `last_turn_only: false` echoes no reasoning at all.
    let echo_scope = match frozen.reasoning {
        ReasoningPolicy::AnthropicInterleaved {
            last_turn_only: true,
            echo_scope,
            ..
        } => Some(echo_scope),
        _ => None,
    };
    // Whose signed thinking may be echoed to the request's model is frozen too (see
    // `CrossModelThinking`). A config recorded before that rule existed reads back as the
    // original same-model-only rule, so a resumed run keeps its recorded bytes.
    let cross_model = match frozen.reasoning {
        ReasoningPolicy::AnthropicInterleaved { cross_model, .. } => cross_model,
        _ => crate::policy::CrossModelThinking::SameModelOnly,
    };
    let non_system: Vec<&Message> = opts
        .prompt
        .iter()
        .filter(|m| m.role != Role::System)
        .collect();
    // Canonical tool-call ids that violate Anthropic's id grammar (^[a-zA-Z0-9_-]+$, ≤64) —
    // e.g. an OpenAI 450-char `|` id carried in from a provider switch — are aliased to a
    // deterministic Anthropic-valid id, applied to BOTH the tool_use and its tool_result so
    // the arc still pairs. This is render-edge ONLY: the map is built from a read-only scan
    // and the canonical id stays the sole persisted form.
    let id_map = crate::tool_id_alias::ToolIdMap::build(&opts.prompt, &id_grammar());
    let name_map = crate::tool_name_alias::ToolNameMap::build(&opts.tools, &opts.prompt);
    // The index of the first message whose thinking may be echoed, computed in ONE place
    // (`first_echoed_message`) so the request body and the tests' reference renderer read the
    // same positional cutoff and cannot drift.
    let first_echoed = echo_scope.and_then(|scope| first_echoed_message(scope, &non_system));
    let rendered_messages: Vec<(Value, Option<u32>, usize)> = opts
        .prompt
        .iter()
        .enumerate()
        .filter(|(_, m)| m.role != Role::System)
        .enumerate()
        .map(|(i, (prompt_index, m))| {
            // Echo signed thinking back only when BOTH: the message is in the current turn
            // (the interleaved-thinking positional rule above) AND the frozen cross-model rule
            // lets its producing model's thinking reach the CURRENT model. Both rules fail
            // closed on an unknown origin. Only assistant content renders thinking, so other
            // roles in the range are unaffected.
            let positional = first_echoed.is_some_and(|first| i >= first);
            let echo_safe = crate::families::anthropic_thinking_is_echo_safe(
                cross_model,
                m.origin.as_ref(),
                opts.target_model.as_ref(),
            );
            (
                convert_message(m, positional && echo_safe, &id_map, &name_map),
                m.cache_prefix_blocks,
                prompt_index,
            )
        })
        // A cross-model reasoning gate can strip a turn's only block; Anthropic rejects an
        // empty message, so marker planning must see the filtered rendered-message indices.
        .filter(|(message, _, _)| {
            message
                .get("content")
                .and_then(Value::as_array)
                .is_none_or(|content| !content.is_empty())
        })
        .collect();
    // Keep only surviving message identities. System messages are coalesced into the
    // separate system field, and an empty rendered message has no wire-message index.
    let mut rendered_indices = vec![None; opts.prompt.len()];
    let (mut messages, cache_prefix_blocks): (Vec<Value>, Vec<Option<u32>>) = rendered_messages
        .into_iter()
        .enumerate()
        .map(|(wire_index, (message, prefix_blocks, prompt_index))| {
            rendered_indices[prompt_index] = Some(wire_index);
            (message, prefix_blocks)
        })
        .unzip();

    // A persona run leads the system with the billing block, which carries the `cc_version`
    // suffix of the first user message and the unsigned `cch` placeholder; `render` signs it.
    // It is added before markers are planned so the system tail is counted after it.
    if let Some(persona) = persona(frozen) {
        if let Some(block) = billing_block(persona, &messages) {
            system.insert(0, block);
        }
    }

    if let Some((ttl, max_breakpoints, lookback_blocks, tail)) = marker_policy {
        // Marker positions are based on the rendered message blocks, not canonical
        // content, so stripped reasoning and empty text cannot move breakpoints.
        let marker_view = anthropic_marker_view(&messages, &cache_prefix_blocks);
        debug_assert_eq!(marker_view.len(), messages.len());
        let system_tail_blocks = system_tail_block_count(frozen, &system);
        let declared: Vec<DeclaredMarker> = opts
            .declared_markers
            .iter()
            .filter_map(|marker| {
                let message_index = rendered_indices
                    .get(marker.message_index)
                    .copied()
                    .flatten()?;
                Some(DeclaredMarker {
                    message_index,
                    // Compaction ranks increase with stability; the planner's priority
                    // ranks decrease. Inversion keeps equal ranks in declaration order.
                    stability_rank: u32::MAX - marker.rank,
                })
            })
            .collect();
        let plan = plan_markers(
            lookback_blocks,
            max_breakpoints,
            system_tail_blocks,
            tail,
            &marker_view,
            opts.marker_order_version,
            &declared,
        );
        apply_anthropic_marker_plan(&mut system, &mut messages, frozen, ttl, &plan);
    }

    let mut body = json!({
        "model": frozen.wire_model_id,
        "max_tokens": frozen.max_output_tokens,
        "stream": true,
        "system": system,
        "messages": messages,
    });

    if let ReasoningPolicy::AnthropicInterleaved { budget_tokens, .. } = frozen.reasoning {
        body["thinking"] = json!({ "type": "enabled", "budget_tokens": budget_tokens });
    }
    // A caller's reasoning variant replaces the policy's default thinking request outright:
    // its own `thinking` object, or none at all, plus the effort when it sets one.
    if let Some(crate::variant::FrozenVariant {
        request: crate::variant::VariantRequest::Anthropic { thinking, effort },
        ..
    }) = &frozen.variant
    {
        match thinking {
            Some(thinking) => body["thinking"] = thinking.clone(),
            None => {
                body.as_object_mut()
                    .expect("Anthropic body is an object")
                    .remove("thinking");
            }
        }
        if let Some(effort) = effort {
            body["output_config"] = json!({ "effort": effort });
        }
    }

    if !opts.tools.is_empty() {
        body["tools"] = Value::Array(
            opts.tools
                .iter()
                .map(|tool| convert_tool(tool, &name_map))
                .collect(),
        );
        body["tool_choice"] = convert_tool_choice(&opts.tool_choice, &name_map);
    }
    apply_generation_controls(
        body.as_object_mut()
            .expect("Anthropic request body must stay a JSON object"),
        opts,
        "temperature",
        "top_p",
        Some("stop_sequences"),
    );
    // Fast mode, which is how a caller's `priority` tier renders on Anthropic. The matching
    // beta header is added in `anthropic_beta_header`. Absent leaves the body unchanged.
    if let Some(speed) = &frozen.speed {
        body["speed"] = Value::String(speed.clone());
    }
    if let Some(persona) = persona(frozen) {
        body["metadata"] = json!({
            "user_id": persona_rules::metadata_user_id(
                &persona.device_id,
                persona.account_uuid.as_deref(),
                &persona.session_id,
            )
        });
    }
    crate::structured_output::finish(
        frozen,
        &mut body,
        name_map.wire_name(broca_protocol::structured_output::SUBMIT_RESULT_TOOL),
    );
    body
}

fn build_system(
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
    coalesce_instruction_tail: bool,
) -> Vec<Value> {
    let mut system = Vec::new();
    if let SystemPromptPolicy::RequiredIdentityLead { identity } = &frozen.system_prompt {
        system.push(json!({ "type": "text", "text": identity }));
    }
    for m in &opts.prompt {
        if m.role == Role::System {
            for b in &m.content {
                // Drop an empty text part — Anthropic rejects a `{type:text, text:""}` block.
                // Defensive against foreign input; this codebase's own normalizers never
                // emit empty text, so it changes no current render.
                if let ContentBlock::Text { text } = b {
                    if !text.is_empty() {
                        system.push(json!({ "type": "text", "text": text }));
                    }
                }
            }
        }
    }

    if coalesce_instruction_tail {
        coalesce_system_tail(frozen, &mut system);
    }
    system
}

fn coalesce_system_tail(frozen: &FrozenRenderConfig, system: &mut Vec<Value>) {
    let tail_start = system_tail_start(frozen, system);
    if tail_start >= system.len().saturating_sub(1) {
        return;
    }

    let merged = system[tail_start..]
        .iter()
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if merged.is_empty() {
        return;
    }

    system.truncate(tail_start);
    system.push(json!({ "type": "text", "text": merged }));
}

/// Where the caller's system text starts: after the required identity block and, on a
/// persona run, after the billing block before it. Neither lead block ever carries a cache
/// breakpoint or is merged into the caller's text.
fn system_tail_start(frozen: &FrozenRenderConfig, system: &[Value]) -> usize {
    if !matches!(
        frozen.system_prompt,
        SystemPromptPolicy::RequiredIdentityLead { .. }
    ) {
        return 0;
    }
    let billing_leads = persona(frozen).is_some()
        && system
            .first()
            .and_then(|block| block.get("text"))
            .and_then(Value::as_str)
            .is_some_and(|text| text.starts_with(persona_rules::BILLING_HEADER_PREFIX));
    1 + usize::from(billing_leads)
}

fn system_tail_block_count(frozen: &FrozenRenderConfig, system: &[Value]) -> usize {
    system
        .get(system_tail_start(frozen, system)..)
        .unwrap_or_default()
        .iter()
        .filter(|block| is_anthropic_cacheable_block(block))
        .count()
}

fn anthropic_marker_view(
    messages: &[Value],
    cache_prefix_blocks: &[Option<u32>],
) -> Vec<MarkerViewMessage> {
    messages
        .iter()
        .zip(cache_prefix_blocks)
        .map(|(message, cache_prefix_blocks)| {
            let role = match message.get("role").and_then(Value::as_str) {
                Some("user") => Role::User,
                Some("assistant") => Role::Assistant,
                _ => Role::Assistant,
            };
            let block_count = message
                .get("content")
                .and_then(Value::as_array)
                .map(|content| {
                    content
                        .iter()
                        .filter(|block| is_anthropic_cacheable_block(block))
                        .count()
                })
                .unwrap_or(0);
            MarkerViewMessage {
                role,
                block_count,
                has_cacheable: block_count > 0,
                cache_prefix_blocks: *cache_prefix_blocks,
            }
        })
        .collect()
}

fn is_anthropic_cacheable_block(block: &Value) -> bool {
    matches!(
        block.get("type").and_then(Value::as_str),
        Some("text" | "tool_use" | "tool_result")
    )
}

/// The frozen marker policy as the renderer consumes it. The first element is the `ttl` to
/// write on each marker, or `None` to write none and take the provider's default lifetime.
fn anthropic_marker_policy(
    frozen: &FrozenRenderConfig,
) -> Option<(Option<CacheTtl>, u32, u32, Option<CacheTail>)> {
    let cache = frozen.cache.as_ref()?;
    match &cache.class {
        CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl,
            max_breakpoints,
            lookback_blocks,
            tail,
            omit_ttl,
        } => Some((
            (!omit_ttl).then_some(*ttl),
            *max_breakpoints,
            *lookback_blocks,
            *tail,
        )),
        _ => None,
    }
}

fn apply_anthropic_marker_plan(
    system: &mut [Value],
    messages: &mut [Value],
    frozen: &FrozenRenderConfig,
    ttl: Option<CacheTtl>,
    plan: &MarkerPlan,
) {
    if plan.system_tail {
        let tail_start = system_tail_start(frozen, system);
        if let Some(block) = system
            .get_mut(tail_start..)
            .and_then(|tail| tail.last_mut())
        {
            set_anthropic_cache_control(block, ttl);
        }
    }

    if let Some(target) = plan.bridge_message {
        set_anthropic_message_marker(messages, target, ttl);
    }
    for target in &plan.message_anchors {
        set_anthropic_message_marker(messages, *target, ttl);
    }
}

fn set_anthropic_message_marker(
    messages: &mut [Value],
    target: MarkerTarget,
    ttl: Option<CacheTtl>,
) {
    let Some(content) = messages
        .get_mut(target.message_index)
        .and_then(|message| message.get_mut("content"))
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let block = match target.cacheable_block_index {
        Some(index) => content
            .iter_mut()
            .filter(|block| is_anthropic_cacheable_block(block))
            .nth(index),
        None => content
            .iter_mut()
            .rev()
            .find(|block| is_anthropic_cacheable_block(block)),
    };
    if let Some(block) = block {
        set_anthropic_cache_control(block, ttl);
    }
}

/// Write one breakpoint. With no `ttl` the marker is the bare `{"type":"ephemeral"}`, which
/// Anthropic reads as its default 5-minute lifetime.
fn set_anthropic_cache_control(block: &mut Value, ttl: Option<CacheTtl>) {
    if let Some(object) = block.as_object_mut() {
        object.remove("cacheControl");
        object.remove("cache_control");
        let marker = match ttl {
            Some(ttl) => json!({ "type": "ephemeral", "ttl": ttl.as_str() }),
            None => json!({ "type": "ephemeral" }),
        };
        object.insert("cache_control".to_string(), marker);
    }
}

fn convert_tool(t: &Tool, name_map: &crate::tool_name_alias::ToolNameMap) -> Value {
    json!({ "name": name_map.wire_name(&t.name), "description": t.description, "input_schema": t.input_schema })
}

fn convert_tool_choice(tc: &ToolChoice, name_map: &crate::tool_name_alias::ToolNameMap) -> Value {
    match tc {
        ToolChoice::Auto => json!({ "type": "auto" }),
        ToolChoice::None => json!({ "type": "none" }),
        ToolChoice::Required => json!({ "type": "any" }),
        ToolChoice::Tool { name } => {
            json!({ "type": "tool", "name": name_map.wire_name(name) })
        }
    }
}

/// Anthropic's tool-call-id grammar — `^[a-zA-Z0-9_-]+$`, ≤ 64 chars. A canonical id that
/// violates it is aliased (see [`crate::tool_id_alias`]).
fn id_grammar() -> crate::tool_id_alias::IdGrammar {
    crate::tool_id_alias::IdGrammar::anthropic()
}

/// The index of the first non-system message whose assistant thinking is echoed under
/// `scope`, or `None` when no assistant message qualifies. Every assistant message at or
/// after this index echoes its signed thinking (still subject to the cross-model
/// echo-safety gate applied per message); assistant messages before it are stripped.
///
/// This is the ONE place the echo scope becomes a positional cutoff. Both the request body
/// and the tests' reference renderer call it, so they cannot disagree about which messages
/// keep their thinking.
fn first_echoed_message(scope: ThinkingEchoScope, non_system: &[&Message]) -> Option<usize> {
    match scope {
        // Keep every assistant message's thinking: the cutoff is the first message, so no
        // assistant message is ever stripped by position. Recency pruning of stale thinking
        // belongs to the reduction transform, not here.
        ThinkingEchoScope::AllAssistantMessages => Some(0),
        // Only the last assistant message. Its index is the cutoff, and nothing after it is
        // an assistant message, so exactly that one message echoes.
        ThinkingEchoScope::LastAssistantMessage => {
            non_system.iter().rposition(|m| m.role == Role::Assistant)
        }
    }
}

/// Convert one canonical message to an Anthropic message. A `Tool` role becomes a
/// USER turn carrying `tool_result` blocks (Anthropic's shape). `keep_thinking` is
/// true only for assistant messages of the current turn (the interleaved-thinking rule). `id_map` maps
/// each canonical tool-call id to its Anthropic wire id (identity unless aliased).
fn convert_message(
    m: &Message,
    keep_thinking: bool,
    id_map: &crate::tool_id_alias::ToolIdMap,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Value {
    match m.role {
        Role::Tool => {
            let content: Vec<Value> = m
                .content
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::ToolResult(tr) => Some(json!({
                        "type": "tool_result",
                        "tool_use_id": id_map.wire_id(&tr.tool_call_id.0),
                        "content": tool_output_text(&tr.output),
                        "is_error": tr.is_error,
                    })),
                    _ => None,
                })
                .collect();
            json!({ "role": "user", "content": content })
        }
        Role::User => {
            json!({ "role": "user", "content": convert_user_content(&m.content, id_map, name_map) })
        }
        Role::Assistant => {
            json!({ "role": "assistant", "content": convert_content(&m.content, keep_thinking, id_map, name_map) })
        }
        Role::System => {
            json!({ "role": "user", "content": convert_content(&m.content, false, id_map, name_map) })
        }
    }
}

fn convert_user_content(
    blocks: &[ContentBlock],
    id_map: &crate::tool_id_alias::ToolIdMap,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Vec<Value> {
    let mut content = Vec::new();
    for block in blocks {
        match block {
            ContentBlock::ToolResult(tr) => content.push(json!({
                "type": "tool_result",
                "tool_use_id": id_map.wire_id(&tr.tool_call_id.0),
                "content": tool_output_text(&tr.output),
                "is_error": tr.is_error,
            })),
            _ => content.extend(convert_content(
                std::slice::from_ref(block),
                false,
                id_map,
                name_map,
            )),
        }
    }
    content
}

fn convert_content(
    blocks: &[ContentBlock],
    keep_thinking: bool,
    id_map: &crate::tool_id_alias::ToolIdMap,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Vec<Value> {
    use crate::reasoning::{decide_reasoning, ReasoningDecision, SerializerProfile};
    blocks
        .iter()
        .enumerate()
        .filter_map(|(idx, b)| match b {
            // Drop an empty text part (Anthropic rejects `text:""`). Defensive against
            // foreign input; the owned normalizers never produce empty text.
            ContentBlock::Text { text } if text.is_empty() => None,
            ContentBlock::Text { text } => Some(json!({ "type": "text", "text": text })),
            // The reasoning keep/clear decision routes through the SHARED decide_reasoning so
            // the reordering plugin/MITM legs inherit the §6.1.1 segmentation guard from the
            // same locus. This family's render is SOURCE-ORDER (it never reorders tool_use by
            // a thinking boundary), so the guard is a no-op here: the decision is exactly the
            // caller's `keep_thinking` (current-turn AND same-model echo-safe), and
            // the rendered bytes are unchanged. A signed thinking block is echoed
            // byte-verbatim; an unsigned one is never emitted (the provider rejects it).
            ContentBlock::Reasoning { text, signature } => {
                let keep = decide_reasoning(
                    keep_thinking,
                    signature.is_some(),
                    idx,
                    blocks,
                    SerializerProfile::SourceOrder,
                ) == ReasoningDecision::Keep;
                if keep {
                    signature.as_ref().map(
                        |sig| json!({ "type": "thinking", "thinking": text, "signature": sig }),
                    )
                } else {
                    None
                }
            }
            ContentBlock::RedactedReasoning {
                data,
                provider_format,
            } => {
                let keep = provider_format.is_none()
                    && decide_reasoning(
                        keep_thinking,
                        true,
                        idx,
                        blocks,
                        SerializerProfile::SourceOrder,
                    ) == ReasoningDecision::Keep;
                keep.then(|| json!({ "type": "redacted_thinking", "data": data }))
            }
            ContentBlock::ToolCall(tc) => Some(json!({
                "type": "tool_use",
                "id": id_map.wire_id(&tc.tool_call_id.0),
                "name": name_map.wire_name(&tc.tool_name),
                "input": tc.input,
            })),
            // A tool_result inside a non-Tool message is unusual; the Tool-role path
            // above handles results.
            ContentBlock::ToolResult(_) => None,
        })
        .collect()
}

fn tool_output_text(out: &ToolOutput) -> String {
    match out {
        ToolOutput::Text { text } => text.clone(),
        ToolOutput::Json { value } => value.to_string(),
        ToolOutput::Denied { reason } => format!("denied: {reason}"),
        ToolOutput::Error { message } => message.clone(),
    }
}

#[cfg(test)]
mod tests {
    use broca_protocol::{request::ModelHandle, StepContext};

    use super::*;
    use crate::families::{anthropic_apikey_frozen_config, anthropic_frozen_config};
    use broca_protocol::{content::ToolOutput, ToolCallId};

    fn persona_frozen() -> FrozenRenderConfig {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.claude_code_persona = Some(ClaudeCodePersona {
            version: "2.1.280".into(),
            device_id: "0".repeat(64),
            account_uuid: None,
            session_id: "11111111-1111-4111-8111-111111111111".into(),
            prompt_id: None,
            cc_version_suffix: None,
        });
        // Adaptive thinking, so the request can be warmed (a manual budget cannot).
        frozen.variant = Some(crate::variant::FrozenVariant {
            name: "adaptive".into(),
            request: crate::variant::VariantRequest::Anthropic {
                thinking: Some(json!({ "type": "adaptive" })),
                effort: None,
            },
        });
        frozen
    }

    /// A persona warm request changes the sent body, so it is re-emitted in the persona's key
    /// order and signed again, keeping the billing text it was based on.
    #[test]
    fn persona_warm_body_is_reordered_and_signed_again() {
        let frozen = persona_frozen();
        let base = AnthropicMessages.render(&frozen, &opts(vec![user_message("warm me")]));
        let warm = AnthropicMessages
            .warm_body(&frozen, &base)
            .expect("an adaptive persona request can be warmed");
        let warm_text = String::from_utf8(warm).unwrap();
        let parsed: Value = serde_json::from_str(&warm_text).unwrap();
        assert_eq!(persona_rules::to_persona_json(&parsed), warm_text);
        assert_eq!(persona_rules::sign_body(&warm_text), warm_text);
        assert_eq!(parsed["max_tokens"], 0);
        assert!(parsed.get("stream").is_none());
        let base_parsed: Value = serde_json::from_slice(&base).unwrap();
        let without_cch = |value: &Value| {
            let text = value["system"][0]["text"].as_str().unwrap().to_string();
            let at = text.find("cch=").unwrap();
            format!("{}{}", &text[..at], &text[at + 10..])
        };
        assert_eq!(without_cch(&parsed), without_cch(&base_parsed));
        assert_ne!(
            parsed["system"][0], base_parsed["system"][0],
            "a new body, a new cch"
        );
    }

    /// The persona applies only to the OAuth identity-lead shape: an api-key config carrying a
    /// persona renders and addresses exactly as it does without one.
    #[test]
    fn an_api_key_config_ignores_a_persona() {
        let mut with = anthropic_apikey_frozen_config("claude-opus-4-8");
        with.claude_code_persona = persona_frozen().claude_code_persona;
        let without = anthropic_apikey_frozen_config("claude-opus-4-8");
        let request = opts(vec![user_message("hello")]);
        assert_eq!(
            AnthropicMessages.render(&with, &request),
            AnthropicMessages.render(&without, &request)
        );
        let auth = AuthHandle::ApiKeyHeader {
            header: "x-api-key".into(),
            value: "k".into(),
        };
        let with_request = AnthropicMessages.endpoint(&with, &auth, Vec::new());
        let without_request = AnthropicMessages.endpoint(&without, &auth, Vec::new());
        assert_eq!(with_request.url, without_request.url);
        assert_eq!(with_request.headers, without_request.headers);
    }

    fn system_message(text: &str) -> Message {
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::System,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    fn user_message(text: &str) -> Message {
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    fn opts(prompt: Vec<Message>) -> CallOptions {
        let mut opts = CallOptions::new(prompt);
        opts.step_context = Some(StepContext {
            run_id: "run-1".into(),
            step_index: 1,
        });
        opts
    }

    /// Render the messages under the legacy last-assistant-message rule, without the
    /// empty-message filter `build_request_body` applies. It reads the positional cutoff from
    /// the SAME `first_echoed_message` the request body uses, pinned to the legacy scope, so
    /// the two can never disagree about which messages echo -- only the filter differs.
    fn legacy_unfiltered_messages(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Vec<Value> {
        let echo_scope = match frozen.reasoning {
            ReasoningPolicy::AnthropicInterleaved {
                last_turn_only: true,
                ..
            } => Some(ThinkingEchoScope::LastAssistantMessage),
            _ => None,
        };
        let non_system = opts
            .prompt
            .iter()
            .filter(|message| message.role != Role::System)
            .collect::<Vec<_>>();
        let id_map = crate::tool_id_alias::ToolIdMap::build(&opts.prompt, &id_grammar());
        let name_map = crate::tool_name_alias::ToolNameMap::build(&opts.tools, &opts.prompt);
        let first_echoed = echo_scope.and_then(|scope| first_echoed_message(scope, &non_system));

        non_system
            .iter()
            .enumerate()
            .map(|(index, message)| {
                let positional = first_echoed.is_some_and(|first| index >= first);
                let echo_safe = crate::families::signed_reasoning_is_echo_safe(
                    message.origin.as_ref(),
                    opts.target_model.as_ref(),
                );
                convert_message(message, positional && echo_safe, &id_map, &name_map)
            })
            .collect()
    }

    #[test]
    fn cross_model_reasoning_only_turn_contrasts_with_legacy_empty_message() {
        let frozen = anthropic_frozen_config("claude-opus-4-8");
        let mut opts = opts(vec![
            user_message("first question"),
            Message::assistant_from(
                vec![ContentBlock::RedactedReasoning {
                    data: "opaque-signature-only-thought".into(),
                    provider_format: None,
                }],
                ModelHandle::new("google", "gemini-3.5-flash-low"),
            ),
            user_message("continue"),
        ]);
        opts.target_model = Some(ModelHandle::new("anthropic", "claude-opus-4-8"));

        let legacy = legacy_unfiltered_messages(&frozen, &opts);
        assert!(legacy[1]["content"].as_array().unwrap().is_empty());

        let fixed = build_request_body(&frozen, &opts);
        let messages = fixed["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert!(messages.iter().all(|message| {
            message["content"]
                .as_array()
                .is_some_and(|content| !content.is_empty())
        }));
    }

    #[test]
    fn nonempty_prompt_is_byte_identical_to_legacy_unfiltered_renderer() {
        let frozen = anthropic_frozen_config("claude-opus-4-8");
        let mut opts = opts(vec![
            user_message("first question"),
            Message::assistant_from(
                vec![ContentBlock::Text {
                    text: "visible answer".into(),
                }],
                ModelHandle::new("anthropic", "claude-opus-4-8"),
            ),
            user_message("continue"),
        ]);
        opts.target_model = Some(ModelHandle::new("anthropic", "claude-opus-4-8"));

        let fixed = build_request_body(&frozen, &opts);
        let mut legacy = fixed.clone();
        legacy["messages"] = Value::Array(legacy_unfiltered_messages(&frozen, &opts));

        assert_eq!(
            serde_json::to_vec(&fixed).unwrap(),
            serde_json::to_vec(&legacy).unwrap()
        );
    }

    #[test]
    fn deduped_append_prompt_keeps_one_anthropic_system_entry() {
        let frozen = anthropic_apikey_frozen_config("claude-test");
        let old_way = build_request_body(
            &frozen,
            &opts(vec![
                system_message("system rules"),
                user_message("first question"),
                system_message("system rules"),
                user_message("second question"),
            ]),
        );
        let deduped = build_request_body(
            &frozen,
            &opts(vec![
                system_message("system rules"),
                user_message("first question"),
                user_message("second question"),
            ]),
        );

        assert_eq!(old_way["system"].as_array().unwrap().len(), 2);
        assert_eq!(deduped["system"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn deduped_append_prompt_preserves_oauth_identity_lead_plus_one_system_entry() {
        let frozen = anthropic_frozen_config("claude-test");
        let deduped = build_request_body(
            &frozen,
            &opts(vec![
                system_message("system rules"),
                user_message("first question"),
                user_message("second question"),
            ]),
        );
        let system = deduped["system"].as_array().unwrap();

        assert_eq!(
            system.len(),
            2,
            "identity lead plus one user-supplied system entry"
        );
        assert_eq!(system[1]["text"], Value::String("system rules".into()));
    }

    fn reasoning_tool_step(n: usize) -> [Message; 2] {
        let call_id = ToolCallId(format!("toolu_{n}"));
        [
            Message::assistant_from(
                vec![
                    ContentBlock::Reasoning {
                        text: format!("thinking before tick {n}"),
                        signature: Some(format!("sig-{n}")),
                    },
                    ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                        thought_signature: None,
                        tool_call_id: call_id.clone(),
                        tool_name: "tick".into(),
                        input: json!({ "n": n }),
                    }),
                ],
                ModelHandle::new("anthropic", "claude-opus-4-8"),
            ),
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult(
                    broca_protocol::content::ToolResult {
                        tool_call_id: call_id,
                        output: ToolOutput::Text {
                            text: format!("tick {n}"),
                        },
                        is_error: false,
                    },
                )],
            },
        ]
    }

    /// Render every request of a tool loop in which each assistant message reasons and calls
    /// a tool, as the run loop would: request k carries steps 1..k-1.
    fn tool_loop_requests(frozen: &FrozenRenderConfig, steps: usize) -> Vec<Value> {
        let mut prompt = vec![
            system_message("system rules"),
            user_message("earlier question"),
            Message::assistant_from(
                vec![
                    ContentBlock::Reasoning {
                        text: "earlier turn thinking".into(),
                        signature: Some("sig-earlier".into()),
                    },
                    ContentBlock::Text {
                        text: "earlier answer".into(),
                    },
                ],
                ModelHandle::new("anthropic", "claude-opus-4-8"),
            ),
            user_message("call tick until told to stop"),
        ];
        let mut requests = Vec::new();
        for n in 1..=steps {
            let mut request = opts(prompt.clone());
            request.target_model = Some(ModelHandle::new("anthropic", "claude-opus-4-8"));
            requests.push(build_request_body(frozen, &request));
            prompt.extend(reasoning_tool_step(n));
        }
        requests
    }

    /// Under the keep-all scope every assistant message keeps its thinking, INCLUDING the
    /// earlier turn's -- the renderer never strips thinking by position. So each step's prefix
    /// through its last assistant message is byte-identical in the next step, and the earlier
    /// turn's thinking is still present in the final request.
    #[test]
    fn keep_all_scope_keeps_each_step_prefix_and_the_earlier_turn_thinking() {
        let frozen = anthropic_frozen_config("claude-opus-4-8");
        assert!(matches!(
            frozen.reasoning,
            ReasoningPolicy::AnthropicInterleaved {
                echo_scope: ThinkingEchoScope::AllAssistantMessages,
                ..
            }
        ));
        let requests = tool_loop_requests(&frozen, 6);
        for (k, pair) in requests.windows(2).enumerate() {
            let (this, next) = (&pair[0], &pair[1]);
            assert_eq!(this["system"], next["system"]);
            assert_eq!(this["tools"], next["tools"]);
            let this_messages = this["messages"].as_array().unwrap();
            let next_messages = next["messages"].as_array().unwrap();
            let last_assistant = this_messages
                .iter()
                .rposition(|m| m["role"] == "assistant")
                .unwrap();
            let prefix = serde_json::to_vec(&this_messages[..=last_assistant]).unwrap();
            let next_prefix = serde_json::to_vec(&next_messages[..=last_assistant]).unwrap();
            assert!(
                prefix == next_prefix,
                "step {} messages through its last assistant message changed bytes in step {}",
                k + 1,
                k + 2
            );
        }

        // Every assistant message kept its thinking: the earlier turn plus steps 1..=5.
        let last = requests.last().unwrap()["messages"].as_array().unwrap();
        let thinking_messages = last
            .iter()
            .filter(|m| {
                m["content"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|block| block["type"] == "thinking")
            })
            .count();
        assert_eq!(
            thinking_messages, 6,
            "the earlier turn plus steps 1..=5 all keep their thinking under keep-all"
        );
        assert!(serde_json::to_string(last)
            .unwrap()
            .contains("earlier turn thinking"));
    }

    /// The legacy scope still strips thinking from every assistant message but the last, which
    /// is why its step prefixes change: this pins the behaviour recorded runs resume with.
    #[test]
    fn legacy_echo_scope_still_strips_all_but_the_last_assistant_message() {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.reasoning = ReasoningPolicy::AnthropicInterleaved {
            budget_tokens: 1024,
            last_turn_only: true,
            echo_scope: ThinkingEchoScope::LastAssistantMessage,
            cross_model: crate::policy::CrossModelThinking::SameModelOnly,
        };
        let requests = tool_loop_requests(&frozen, 3);
        let this = requests[1]["messages"].as_array().unwrap();
        let next = requests[2]["messages"].as_array().unwrap();
        let last_assistant = this.iter().rposition(|m| m["role"] == "assistant").unwrap();
        assert_eq!(this[last_assistant]["content"][0]["type"], "thinking");
        assert_ne!(next[last_assistant]["content"][0]["type"], "thinking");
    }

    /// Render every request of a conversation with TWO user turns, each followed by a tool
    /// loop in which every assistant message reasons and calls a tool, as the run loop would:
    /// request k carries steps 1..k-1. The second user message carries text (not tool
    /// results), so it starts a fresh tool loop -- the boundary a per-loop scope strips
    /// across. `first_steps` steps follow the first user turn, `second_steps` the second.
    fn two_user_turn_requests(
        frozen: &FrozenRenderConfig,
        first_steps: usize,
        second_steps: usize,
    ) -> Vec<Value> {
        let target = ModelHandle::new("anthropic", "claude-opus-4-8");
        let mut prompt = vec![
            system_message("system rules"),
            user_message("first user turn"),
        ];
        let mut requests = Vec::new();
        let mut n = 0;
        for _ in 0..first_steps {
            n += 1;
            let mut request = opts(prompt.clone());
            request.target_model = Some(target.clone());
            requests.push(build_request_body(frozen, &request));
            prompt.extend(reasoning_tool_step(n));
        }
        // A real user message (text, not tool results) opens the second turn.
        prompt.push(user_message("second user turn"));
        for _ in 0..second_steps {
            n += 1;
            let mut request = opts(prompt.clone());
            request.target_model = Some(target.clone());
            requests.push(build_request_body(frozen, &request));
            prompt.extend(reasoning_tool_step(n));
        }
        requests
    }

    /// Every request's message prefix, up to and including its last assistant message, must be
    /// byte-identical to the same-length prefix of the final (longest) request. Returns the
    /// first offending request when the prefix moved, so a caller can assert either direction.
    fn first_unstable_prefix(requests: &[Value]) -> Option<usize> {
        let reference = requests.last().unwrap()["messages"].as_array().unwrap();
        for (k, request) in requests.iter().enumerate() {
            let messages = request["messages"].as_array().unwrap();
            let Some(last_assistant) = messages.iter().rposition(|m| m["role"] == "assistant")
            else {
                // The very first request precedes any assistant message; nothing to pin yet.
                continue;
            };
            let prefix = serde_json::to_vec(&messages[..=last_assistant]).unwrap();
            let reference_prefix = serde_json::to_vec(&reference[..=last_assistant]).unwrap();
            if prefix != reference_prefix {
                return Some(k);
            }
        }
        None
    }

    /// The prompt cache can only be read at a later step if everything an earlier step sent, up
    /// to its last assistant message, is sent again byte-for-byte later. Under the keep-all
    /// scope every assistant message keeps its thinking forever, so this holds for every
    /// request -- across the user-turn boundary too, where an earlier tool loop's assistant
    /// messages are compared against the final request of the SECOND loop.
    #[test]
    fn keep_all_scope_prefix_is_byte_stable_across_two_user_turns() {
        let frozen = anthropic_frozen_config("claude-opus-4-8");
        assert!(matches!(
            frozen.reasoning,
            ReasoningPolicy::AnthropicInterleaved {
                echo_scope: ThinkingEchoScope::AllAssistantMessages,
                ..
            }
        ));
        let requests = two_user_turn_requests(&frozen, 4, 4);
        // The final request must carry both loops' assistant messages, so the comparison
        // reaches back across the second user turn.
        let final_messages = requests.last().unwrap()["messages"].as_array().unwrap();
        let thinking = final_messages
            .iter()
            .filter(|m| {
                m["content"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|block| block["type"] == "thinking")
            })
            .count();
        assert_eq!(
            thinking, 7,
            "steps 1..=7 all echo thinking in the final request"
        );
        assert_eq!(
            first_unstable_prefix(&requests),
            None,
            "every request's prefix through its last assistant message is byte-stable"
        );
    }

    /// The legacy last-assistant-message scope strips every assistant message but the last, so
    /// an earlier step's prefix does NOT survive into a later request: this is the instability
    /// the keep-all scope removes, pinned here so the positive test above is not vacuous.
    #[test]
    fn legacy_scope_prefix_is_not_stable_across_a_tool_loop() {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.reasoning = ReasoningPolicy::AnthropicInterleaved {
            budget_tokens: 1024,
            last_turn_only: true,
            echo_scope: ThinkingEchoScope::LastAssistantMessage,
            cross_model: crate::policy::CrossModelThinking::SameModelOnly,
        };
        let requests = two_user_turn_requests(&frozen, 4, 4);
        assert!(
            first_unstable_prefix(&requests).is_some(),
            "the legacy scope must break prefix stability by stripping earlier thinking"
        );
    }

    #[test]
    fn anthropic_stream_normalizer_flushes_final_record_ending_in_bare_cr() {
        let mut normalizer = AnthropicStreamNormalizer {
            sse: SseDecoder::default(),
            inner: AnthropicNormalizer::new(StepId(1)),
        };
        let p1 = normalizer.push(b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-3-5-sonnet\",\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n").unwrap();
        assert!(!p1.is_empty());
        let p2 = normalizer
            .push(b"event: message_stop\r\ndata: {\"type\":\"message_stop\"}\r\r")
            .unwrap();
        assert!(p2.is_empty());
        let final_events = normalizer.finish().unwrap();
        assert!(final_events.iter().any(|e| matches!(
            e,
            NormalizedEvent::Control(broca_protocol::ProviderControl::Finished { .. })
        )));
    }
}

#[cfg(test)]
mod append_only_tests;
