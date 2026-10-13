//! Proves the conformance harness is NON-VACUOUS: each check actually FAILS for a
//! family that has the bug it's meant to catch. A gate that can't fail is worthless,
//! so these deliberately-broken families are the harness's own test.

use std::collections::HashMap;

use broca_protocol::{CallOptions, NormalizedEvent, StepId};
use broca_provider::conformance::{check_freeze_against_mutation, check_render_determinism};
use broca_provider::policy::{ReasoningPolicy, SystemPromptPolicy, ToolIdPolicy, UsagePolicy};
use broca_provider::spec::WireFamilyId;
use broca_provider::wire::{
    AuthHandle, FrozenRenderConfig, HttpMethod, HttpRequest, NormalizeError, StreamNormalizer,
    WireFamily,
};

fn frozen() -> FrozenRenderConfig {
    FrozenRenderConfig {
        family_id: WireFamilyId::OpenAiChat,
        renderer_version: 1,
        wire_model_id: "m".into(),
        base_url: None,
        max_output_tokens: 16,
        caller_max_output_tokens: None,
        reasoning: ReasoningPolicy::None,
        tool_id: ToolIdPolicy::Passthrough,
        system_prompt: SystemPromptPolicy::SystemMessage,
        usage: UsagePolicy::OpenAiUsage,
        cache: None,
        token_limit_field: Default::default(),
        static_options: serde_json::Value::Null,
        aws_region: None,
        project_id: None,
        prompt_cache_key: None,
        reasoning_effort: None,
        service_tier: None,
        service_tier_request: None,
        speed: None,
        reasoning_content_required: false,
        tool_call_content_empty_string: false,
        variant: None,
        responses_tools_non_strict: false,
        chatgpt_effort_updates: false,
    }
}

fn no_op_endpoint(body: Vec<u8>) -> HttpRequest {
    HttpRequest {
        method: HttpMethod::Post,
        url: "http://x".into(),
        headers: vec![],
        body,
    }
}

struct NoNormalizer;
impl StreamNormalizer for NoNormalizer {
    fn push(&mut self, _chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        Ok(vec![])
    }
}

/// BROKEN: builds the rendered array by iterating a `HashMap`, so the array ORDER is
/// nondeterministic across renders (the standard HashMap reseeds per instance).
/// Canonical JSON sorts keys but NOT array elements, so this leaks past it — exactly
/// the bug the render-twice-fresh-intermediates check exists to catch.
struct HashMapOrderFamily;
impl WireFamily for HashMapOrderFamily {
    fn shape(&self, _frozen: &FrozenRenderConfig, _opts: &CallOptions) -> serde_json::Value {
        let mut m: HashMap<String, u32> = HashMap::new();
        for i in 0..32 {
            m.insert(format!("k{i:02}"), i);
        }
        // Build a JSON ARRAY from the HashMap's (randomized) iteration order. Canonical JSON
        // (the default `render`) sorts object KEYS but never array ELEMENTS, so the
        // nondeterministic array order leaks straight through to the rendered bytes — exactly
        // what render-twice-fresh-intermediates exists to catch.
        let arr: Vec<serde_json::Value> = m
            .into_iter()
            .map(|(k, v)| serde_json::json!({ "k": k, "v": v }))
            .collect();
        serde_json::json!({ "items": arr })
    }
    fn endpoint(&self, _f: &FrozenRenderConfig, _a: &AuthHandle, body: Vec<u8>) -> HttpRequest {
        no_op_endpoint(body)
    }
    fn normalizer(&self, _f: &FrozenRenderConfig, _s: StepId) -> Box<dyn StreamNormalizer> {
        Box::new(NoNormalizer)
    }
}

#[test]
fn catches_hashmap_array_order_nondeterminism() {
    let report =
        check_render_determinism(&HashMapOrderFamily, &frozen(), &CallOptions::new(vec![]));
    assert!(
        !report.passed(),
        "the harness FAILED to catch HashMap-array-order nondeterminism — the gate is vacuous"
    );
    assert!(
        report
            .failures()
            .iter()
            .any(|f| f.starts_with("render_twice_fresh_intermediates")),
        "expected the render-twice check to flag it; got {:?}",
        report.failures()
    );
}

/// BROKEN: the render reads an environment variable, so its bytes change under the
/// perturbed environment — the bug the perturbed-ambient check exists to catch.
struct EnvReadingFamily;
impl WireFamily for EnvReadingFamily {
    fn shape(&self, _frozen: &FrozenRenderConfig, _opts: &CallOptions) -> serde_json::Value {
        let tz = std::env::var("TZ").unwrap_or_default();
        serde_json::json!({ "tz": tz })
    }
    fn endpoint(&self, _f: &FrozenRenderConfig, _a: &AuthHandle, body: Vec<u8>) -> HttpRequest {
        no_op_endpoint(body)
    }
    fn normalizer(&self, _f: &FrozenRenderConfig, _s: StepId) -> Box<dyn StreamNormalizer> {
        Box::new(NoNormalizer)
    }
}

#[test]
fn catches_ambient_env_leak() {
    let report = check_render_determinism(&EnvReadingFamily, &frozen(), &CallOptions::new(vec![]));
    assert!(
        !report.passed(),
        "the harness FAILED to catch an ambient-env leak — the gate is vacuous"
    );
    assert!(
        report
            .failures()
            .iter()
            .any(|f| f.starts_with("perturbed_ambient")),
        "expected the perturbed-ambient check to flag it; got {:?}",
        report.failures()
    );
}

// A global the BROKEN family reads at render time — standing in for "a render input
// that lives outside the frozen config" (a live catalog/spec field, a global cache).
// The freeze check must catch a render whose bytes depend on this, because mutating
// it after the freeze changes the resumed bytes.
static LEAKED_INPUT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// BROKEN: render reads `LEAKED_INPUT` (external mutable state) instead of taking
/// everything from the frozen config — exactly the leak the catalog-mutation-freeze
/// check exists to catch. The frozen config does not (and cannot) capture it.
struct LeakyFreezeFamily;
impl WireFamily for LeakyFreezeFamily {
    fn shape(&self, _frozen: &FrozenRenderConfig, _opts: &CallOptions) -> serde_json::Value {
        let leaked = LEAKED_INPUT.load(std::sync::atomic::Ordering::SeqCst);
        serde_json::json!({ "leaked": leaked })
    }
    fn endpoint(&self, _f: &FrozenRenderConfig, _a: &AuthHandle, body: Vec<u8>) -> HttpRequest {
        no_op_endpoint(body)
    }
    fn normalizer(&self, _f: &FrozenRenderConfig, _s: StepId) -> Box<dyn StreamNormalizer> {
        Box::new(NoNormalizer)
    }
}

#[test]
fn freeze_check_catches_a_leaked_input() {
    LEAKED_INPUT.store(1, std::sync::atomic::Ordering::SeqCst);
    // The spec is irrelevant to this family's render; the leaked input is the global.
    // `mutate` changes the global AFTER the freeze — a correct family (reading only
    // the frozen config) would be unaffected, but this one's bytes change, so the
    // check must fail.
    let mut spec = ();
    let result = check_freeze_against_mutation(
        &LeakyFreezeFamily,
        &mut spec,
        &CallOptions::new(vec![]),
        |_| frozen(),
        |_| LEAKED_INPUT.store(2, std::sync::atomic::Ordering::SeqCst),
    );
    assert!(
        result.is_err(),
        "the freeze check FAILED to catch a render that reads external mutable state — the gate is vacuous"
    );
}
