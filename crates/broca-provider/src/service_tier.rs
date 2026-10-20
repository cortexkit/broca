//! A caller's service tier (`session.send` `service_tier`): which bytes each provider family
//! sends for it, which tiers a model is offered, and how a provider's refusal of a tier is
//! classified.
//!
//! The bytes are broca's, decided here per (provider, wire family), never read from the
//! catalog. The catalog only answers whether a tier is PRICED: fusiform serves per-model mode
//! rates under the upstream mode name (`rate.input.mode.fast`), and [`tier_for_mode`] is broca's
//! table from that name to a tier. A tier is offered only when both agree: the family has bytes
//! for it, and the catalog prices the mode that tier corresponds to.
//!
//! Account eligibility is deliberately not checked here. Only the provider knows whether this
//! account may use a tier right now, and it answers on every response: served, downgraded (the
//! step's `service_tier_used` stamp records that), or refused with its own error, which
//! [`classify_tier_refusal`] turns into the stable `service_tier_unavailable` code.

use broca_protocol::error::{ErrorClass, ProviderError};
pub use broca_protocol::request::ServiceTier;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::spec::WireFamilyId;
use crate::wire::FrozenRenderConfig;

/// The stable error code a provider's refusal of a requested tier is reported under, so routing
/// can stop asking for that tier on that account instead of retrying it.
pub const SERVICE_TIER_UNAVAILABLE: &str = "service_tier_unavailable";

pub use cortexkit_anthropic_policy::FAST_MODE_BETA as ANTHROPIC_FAST_MODE_BETA;

/// The tier a caller asked for, frozen into the render config beside the bytes it became.
///
/// Rendering never reads this; the bytes live in `FrozenRenderConfig::service_tier` /
/// `speed`. It is kept so a provider's refusal can be recognised as a refusal OF THIS TIER and
/// reported naming the tier and the provider, and so a resume can see what was admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceTierRequest {
    pub tier: ServiceTier,
    /// The catalog provider id the run was admitted against (`openai`, `anthropic`, ...).
    pub provider: String,
}

/// What a tier becomes on the wire for one provider family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierBytes {
    /// A top-level body `service_tier` with this value.
    BodyServiceTier(&'static str),
    /// Anthropic fast mode: body `speed: "fast"` plus the fast-mode beta header.
    AnthropicFastMode,
    /// The provider's default needs no field at all; the request is byte-identical to a send
    /// without a tier.
    Nothing,
}

/// Which providers broca has request bytes for, keyed by catalog provider id AND wire family:
/// the OpenAI Chat family also carries the whole OpenAI-compatible long tail, and sending
/// `service_tier` to one of those would be a guess about someone else's API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TierMapping {
    /// OpenAI Responses, ChatGPT codex, and Chat: `service_tier` is the tier's own name.
    OpenAi,
    /// Anthropic Messages: priority is fast mode, flex has no bytes, default sends nothing.
    Anthropic,
    /// The Gemini API: `service_tier` flex or priority, default sends nothing.
    Gemini,
    /// xAI: `service_tier: "priority"` only; default sends nothing.
    Xai,
}

fn mapping(provider_id: &str, family: WireFamilyId) -> Option<TierMapping> {
    use WireFamilyId::*;
    match (provider_id, family) {
        ("openai", OpenAiChat | OpenAiResponses | OpenAiResponsesChatgpt) => {
            Some(TierMapping::OpenAi)
        }
        ("anthropic", AnthropicMessages) => Some(TierMapping::Anthropic),
        // Google Code Assist (Antigravity) is a different backend; its tier bytes have not been
        // verified against it, so it is not offered there.
        ("google", Gemini) => Some(TierMapping::Gemini),
        ("xai", OpenAiChat | OpenAiResponses) => Some(TierMapping::Xai),
        _ => None,
    }
}

/// The bytes `tier` becomes for this provider on this family, or `None` when broca has none.
pub fn tier_bytes(provider_id: &str, family: WireFamilyId, tier: ServiceTier) -> Option<TierBytes> {
    let mapping = mapping(provider_id, family)?;
    match (mapping, tier) {
        (TierMapping::OpenAi, tier) => Some(TierBytes::BodyServiceTier(tier.as_str())),
        (TierMapping::Anthropic, ServiceTier::Priority) => Some(TierBytes::AnthropicFastMode),
        (TierMapping::Anthropic, ServiceTier::Default) => Some(TierBytes::Nothing),
        (TierMapping::Anthropic, ServiceTier::Flex) => None,
        (TierMapping::Gemini, ServiceTier::Default) => Some(TierBytes::Nothing),
        (TierMapping::Gemini, tier) => Some(TierBytes::BodyServiceTier(tier.as_str())),
        (TierMapping::Xai, ServiceTier::Priority) => Some(TierBytes::BodyServiceTier("priority")),
        (TierMapping::Xai, ServiceTier::Default) => Some(TierBytes::Nothing),
        (TierMapping::Xai, ServiceTier::Flex) => None,
    }
}

/// broca's table from an upstream mode name, as fusiform serves it in `rate.*.mode.<mode>`, to
/// the tier that mode is priced as. OpenAI's and Anthropic's `fast` and fireworks' `priority`
/// are all priority; `flex` is flex. `pro` is a reasoning mode, not a tier, and never maps; nor
/// does any name this table does not list.
pub fn tier_for_mode(mode: &str) -> Option<ServiceTier> {
    match mode {
        "fast" | "priority" => Some(ServiceTier::Priority),
        "flex" => Some(ServiceTier::Flex),
        _ => None,
    }
}

/// Why a tier is not offered for a model. The two reasons are kept apart because they change
/// in different places: a missing rate opens when the catalog prices it, a missing mapping only
/// when broca learns that provider's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TierUnavailable {
    /// broca has no request bytes for this tier on this provider and family.
    NoMapping,
    /// The family has bytes, but the catalog has no priced rate for the mode this tier is.
    NoCatalogRate,
}

/// Whether `tier` is offered for a model of this provider and family, given the modes the
/// catalog prices for it.
///
/// `default` needs no mode rate: it is the tier the model is served at without the field, priced
/// by the base rate like any send. Every other tier needs a priced mode that maps to it; an
/// absent mode rate is unpriced and is never filled from the base rate.
pub fn tier_offered(
    provider_id: &str,
    family: WireFamilyId,
    tier: ServiceTier,
    priced_modes: &[String],
) -> Result<(), TierUnavailable> {
    if tier_bytes(provider_id, family, tier).is_none() {
        return Err(TierUnavailable::NoMapping);
    }
    if tier == ServiceTier::Default
        || priced_modes
            .iter()
            .any(|mode| tier_for_mode(mode) == Some(tier))
    {
        Ok(())
    } else {
        Err(TierUnavailable::NoCatalogRate)
    }
}

/// Every tier offered for a model, cheapest first.
pub fn offered_tiers(
    provider_id: &str,
    family: WireFamilyId,
    priced_modes: &[String],
) -> Vec<ServiceTier> {
    ServiceTier::ALL
        .into_iter()
        .filter(|tier| tier_offered(provider_id, family, *tier, priced_modes).is_ok())
        .collect()
}

/// The admission refusal for a tier that is not offered, naming the reason and the tiers that
/// are. Never a silent downgrade: the caller asked for a tier and gets it or this.
pub fn refusal_message(
    catalog_model: &str,
    provider_id: &str,
    family: WireFamilyId,
    tier: ServiceTier,
    reason: &TierUnavailable,
    priced_modes: &[String],
) -> String {
    let offered = offered_tiers(provider_id, family, priced_modes);
    let offered = if offered.is_empty() {
        "none".to_string()
    } else {
        offered
            .iter()
            .map(|tier| format!("\"{tier}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let why = match reason {
        TierUnavailable::NoMapping => format!(
            "provider '{provider_id}' ({}) has no service-tier mapping in broca for \"{tier}\"",
            family.wire_name()
        ),
        TierUnavailable::NoCatalogRate => format!(
            "no rate in the catalog for service_tier \"{tier}\" on '{catalog_model}' (a tier \
             routing cannot price is not offered)"
        ),
    };
    format!(
        "service_tier \"{tier}\" is not offered for '{catalog_model}': {why}. Tiers offered \
         for this model: {offered}; send one of those or omit service_tier"
    )
}

/// Write `tier`'s bytes for this provider into the frozen render config and record the request.
///
/// Used for a fresh episode after [`tier_offered`] admitted the tier, and for a resume, which
/// re-applies the frozen tier without consulting the catalog. Fails only when this build has no
/// bytes for the tier on this family, which on a resume means the mapping changed since
/// admission.
pub fn freeze_service_tier(
    frozen: &mut FrozenRenderConfig,
    provider_id: &str,
    tier: ServiceTier,
) -> Result<(), String> {
    let bytes = tier_bytes(provider_id, frozen.family_id, tier).ok_or_else(|| {
        format!(
            "service_tier \"{tier}\" has no request bytes for provider '{provider_id}' on {}",
            frozen.family_id.wire_name()
        )
    })?;
    match bytes {
        TierBytes::BodyServiceTier(value) => frozen.service_tier = Some(value.to_string()),
        TierBytes::AnthropicFastMode => frozen.speed = Some("fast".to_string()),
        TierBytes::Nothing => {}
    }
    frozen.service_tier_request = Some(ServiceTierRequest {
        tier,
        provider: provider_id.to_string(),
    });
    Ok(())
}

/// Reclassify a provider's HTTP error as a refusal of the requested tier, when it is one.
///
/// A tier the account is not eligible for is an invalid request for this account: `permanent`,
/// code [`SERVICE_TIER_UNAVAILABLE`], naming the tier and the provider with the provider's own
/// message beside them. It is NEVER `auth_required`, even on a 403, because `auth_required`
/// reports the credential to the vault as dead and can burn a rotating refresh token for a
/// request that was merely not permitted. The match is on the provider's error body (its type,
/// code, parameter and message), never on the status alone. A 401 is left alone (it is about the
/// credential whatever the body says), as are 408/429/5xx, which are capacity rather than
/// eligibility (OpenAI's flex "resource unavailable" is a 429, Anthropic's fast-mode limit a
/// 429 or 529) and keep their retryable class.
///
/// The matchers below are written against each provider's documented error shape, cited at the
/// matcher, except where a real refusal has been captured; those say so and quote it.
pub fn classify_tier_refusal(
    family: WireFamilyId,
    request: Option<&ServiceTierRequest>,
    status: u16,
    body: &str,
    error: ProviderError,
) -> ProviderError {
    let Some(request) = request else {
        return error;
    };
    let Ok(parsed) = serde_json::from_str::<Value>(body) else {
        return error;
    };
    let mapped = mapping(&request.provider, family);
    // The one refusal that arrives as a 429, and the only one captured live so far. Checked
    // before the status filter below, which otherwise leaves every 429 as capacity.
    let captured_429 = status == 429
        && matches!(mapped, Some(TierMapping::Anthropic))
        && anthropic_fast_mode_needs_credits(&parsed).is_some();
    if !captured_429 && !matches!(status, 400 | 403 | 404 | 422) {
        return error;
    }
    let refused = match mapped {
        Some(TierMapping::Anthropic) if captured_429 => anthropic_fast_mode_needs_credits(&parsed),
        Some(TierMapping::OpenAi) => openai_tier_refusal(&parsed),
        Some(TierMapping::Anthropic) => anthropic_tier_refusal(&parsed),
        Some(TierMapping::Gemini) => gemini_tier_refusal(&parsed),
        Some(TierMapping::Xai) => xai_tier_refusal(&parsed),
        None => None,
    };
    let Some(provider_message) = refused else {
        return error;
    };
    ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Permanent,
        message: format!(
        "{status}: provider '{}' refused service_tier \"{}\" for this account: {provider_message}",
        request.provider, request.tier
    ),
        status: Some(status),
        retry_after_secs: error.retry_after_secs,
        provider_code: Some(SERVICE_TIER_UNAVAILABLE.to_string()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn error_object(body: &Value) -> Option<&serde_json::Map<String, Value>> {
    body.get("error").and_then(Value::as_object)
}

fn str_field<'a>(object: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key).and_then(Value::as_str)
}

fn mentions_any(text: &str, needles: &[&str]) -> bool {
    let text = text.to_ascii_lowercase();
    needles.iter().any(|needle| text.contains(needle))
}

/// OpenAI (Responses, Chat, ChatGPT codex). The documented error object is
/// `{"error": {"message", "type", "param", "code"}}`, where `param` names the request
/// parameter at fault (https://platform.openai.com/docs/guides/error-codes, and the
/// `service_tier` parameter at https://developers.openai.com/api/docs/guides/fast-mode). A
/// refusal of the tier names `param: "service_tier"`, or says so in its message. The ChatGPT
/// codex backend also answers with a bare `{"detail": "..."}`, matched on its message.
fn openai_tier_refusal(body: &Value) -> Option<String> {
    const NEEDLES: &[&str] = &[
        "service_tier",
        "service tier",
        "priority processing",
        "fast mode",
        "flex processing",
    ];
    if let Some(error) = error_object(body) {
        let message = str_field(error, "message").unwrap_or_default();
        if str_field(error, "param") == Some("service_tier") || mentions_any(message, NEEDLES) {
            return Some(message.to_string());
        }
        return None;
    }
    let detail = body.get("detail").and_then(Value::as_str)?;
    mentions_any(detail, NEEDLES).then(|| detail.to_string())
}

/// Anthropic fast mode. The documented error shape is `{"type": "error", "error": {"type",
/// "message"}}` with `invalid_request_error` (400) or `permission_error` (403)
/// (https://platform.claude.com/docs/en/api/errors), and requesting `speed: "fast"` where it is
/// not available "returns an error" (https://platform.claude.com/docs/en/build-with-claude/fast-mode,
/// "Checking which speed was used"). Matched when one of those types names speed or fast mode.
fn anthropic_tier_refusal(body: &Value) -> Option<String> {
    let error = error_object(body)?;
    let kind = str_field(error, "type")?;
    if !matches!(kind, "invalid_request_error" | "permission_error") {
        return None;
    }
    let message = str_field(error, "message").unwrap_or_default();
    mentions_any(message, &["speed", "fast mode", "fast-mode"]).then(|| message.to_string())
}

/// Anthropic's refusal of fast mode for an account without usage credits. CAPTURED LIVE on
/// 2026-09-25 (a Claude subscription over OAuth, claude-opus-5, `speed: "fast"`):
///
/// ```text
/// 429 {"type":"error","error":{"type":"rate_limit_error",
///      "message":"Usage credits are required for fast mode."}}
/// ```
///
/// It's typed `rate_limit_error` and sent as a 429, but it isn't capacity: the account can't
/// use the tier at all, and retrying never succeeds. So it's a tier refusal. A genuine fast-mode
/// rate limit is also a 429 `rate_limit_error`, and it must stay retryable, so this matches
/// only a message that names fast mode AND says what the account lacks, never "fast mode" alone.
fn anthropic_fast_mode_needs_credits(body: &Value) -> Option<String> {
    let error = error_object(body)?;
    if str_field(error, "type")? != "rate_limit_error" {
        return None;
    }
    let message = str_field(error, "message").unwrap_or_default();
    let lower = message.to_ascii_lowercase();
    (lower.contains("fast mode") && (lower.contains("credits") || lower.contains("required")))
        .then(|| message.to_string())
}

/// The Gemini API. Google's error shape is `{"error": {"code", "message", "status"}}`
/// (https://ai.google.dev/gemini-api/docs/troubleshooting, linking the API errors reference),
/// and the tier is requested as `service_tier` (https://ai.google.dev/gemini-api/docs/flex-inference,
/// https://ai.google.dev/gemini-api/docs/priority-inference). Matched when an
/// INVALID_ARGUMENT, PERMISSION_DENIED or FAILED_PRECONDITION error names the tier field.
fn gemini_tier_refusal(body: &Value) -> Option<String> {
    let error = error_object(body)?;
    let status = str_field(error, "status")?;
    if !matches!(
        status,
        "INVALID_ARGUMENT" | "PERMISSION_DENIED" | "FAILED_PRECONDITION"
    ) {
        return None;
    }
    let message = str_field(error, "message").unwrap_or_default();
    mentions_any(message, &["service_tier", "servicetier", "service tier"])
        .then(|| message.to_string())
}

/// xAI. Its API is OpenAI-compatible and documents the OpenAI error object; no xAI page for a
/// refused `service_tier` could be found (docs.x.ai was checked on 2026-09-25), so this uses the
/// same parameter-or-message match as OpenAI, restricted to wording about the tier.
fn xai_tier_refusal(body: &Value) -> Option<String> {
    openai_tier_refusal(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modes(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    use crate::families::{frozen_config_for_family, Gemini, OpenAiChat, OpenAiResponses};
    use crate::wire::WireFamily;
    use broca_protocol::request::CallOptions;

    fn rendered(
        family: &dyn WireFamily,
        family_id: WireFamilyId,
        provider: &str,
        tier: Option<ServiceTier>,
    ) -> Value {
        let mut frozen = frozen_config_for_family(family_id, "m");
        if let Some(tier) = tier {
            freeze_service_tier(&mut frozen, provider, tier).expect("tier has bytes");
        }
        serde_json::from_slice(&family.render(&frozen, &CallOptions::new(Vec::new()))).unwrap()
    }

    fn without(mut body: Value, key: &str) -> Value {
        body.as_object_mut().unwrap().remove(key);
        body
    }

    /// One rendering case: family, its id, provider, requested tier, expected `service_tier`.
    type TierCase = (
        &'static dyn WireFamily,
        WireFamilyId,
        &'static str,
        ServiceTier,
        Option<&'static str>,
    );

    /// Each mapped family renders exactly the documented field for each tier it has bytes for,
    /// and nothing else changes.
    #[test]
    fn each_family_renders_exactly_its_documented_tier_bytes() {
        let cases: [TierCase; 9] = [
            (
                &OpenAiResponses,
                WireFamilyId::OpenAiResponses,
                "openai",
                ServiceTier::Flex,
                Some("flex"),
            ),
            (
                &OpenAiResponses,
                WireFamilyId::OpenAiResponses,
                "openai",
                ServiceTier::Default,
                Some("default"),
            ),
            (
                &OpenAiResponses,
                WireFamilyId::OpenAiResponses,
                "openai",
                ServiceTier::Priority,
                Some("priority"),
            ),
            (
                &OpenAiChat,
                WireFamilyId::OpenAiChat,
                "openai",
                ServiceTier::Flex,
                Some("flex"),
            ),
            (
                &OpenAiChat,
                WireFamilyId::OpenAiChat,
                "xai",
                ServiceTier::Priority,
                Some("priority"),
            ),
            (
                &OpenAiChat,
                WireFamilyId::OpenAiChat,
                "xai",
                ServiceTier::Default,
                None,
            ),
            (
                &Gemini,
                WireFamilyId::Gemini,
                "google",
                ServiceTier::Flex,
                Some("flex"),
            ),
            (
                &Gemini,
                WireFamilyId::Gemini,
                "google",
                ServiceTier::Priority,
                Some("priority"),
            ),
            (
                &Gemini,
                WireFamilyId::Gemini,
                "google",
                ServiceTier::Default,
                None,
            ),
        ];
        for (family, family_id, provider, tier, expected) in cases {
            let plain = rendered(family, family_id, provider, None);
            let body = rendered(family, family_id, provider, Some(tier));
            assert_eq!(
                body.get("service_tier").and_then(Value::as_str),
                expected,
                "{provider} {family_id:?} {tier}"
            );
            assert!(body.get("speed").is_none());
            assert_eq!(without(body, "service_tier"), plain, "{provider} {tier}");
        }
    }

    /// A family with no mapping has no bytes to freeze, so nothing can reach its renderer.
    #[test]
    fn a_family_without_a_mapping_cannot_freeze_a_tier() {
        for (provider, family) in [
            ("deepseek", WireFamilyId::OpenAiChat),
            ("google", WireFamilyId::GoogleCodeAssist),
            ("anthropic", WireFamilyId::BedrockConverse),
        ] {
            let mut frozen = frozen_config_for_family(family, "m");
            let before = frozen.clone();
            for tier in ServiceTier::ALL {
                assert!(freeze_service_tier(&mut frozen, provider, tier).is_err());
            }
            assert_eq!(frozen, before, "{provider} {family:?}");
        }
    }

    fn request(provider: &str, tier: ServiceTier) -> ServiceTierRequest {
        ServiceTierRequest {
            tier,
            provider: provider.into(),
        }
    }

    fn classified(
        family: WireFamilyId,
        provider: &str,
        tier: ServiceTier,
        status: u16,
        body: &str,
    ) -> ProviderError {
        let original = ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: format!("{status}: {body}"),
            status: Some(status),
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        classify_tier_refusal(
            family,
            Some(&request(provider, tier)),
            status,
            body,
            original,
        )
    }

    /// Each family's documented refusal shape becomes the stable, permanent tier code, naming
    /// the tier and provider with the provider's message passed through, whatever class the
    /// status alone would have given (the input here is deliberately classed `auth_required`).
    #[test]
    fn documented_tier_refusals_become_permanent_service_tier_unavailable() {
        let cases = [
            (
                WireFamilyId::OpenAiResponses,
                "openai",
                ServiceTier::Priority,
                403,
                r#"{"error":{"message":"Project is not allowed to use this tier.","type":"invalid_request_error","param":"service_tier","code":null}}"#,
                "Project is not allowed to use this tier.",
            ),
            (
                WireFamilyId::OpenAiResponsesChatgpt,
                "openai",
                ServiceTier::Flex,
                400,
                r#"{"detail":"Unsupported service_tier: flex"}"#,
                "Unsupported service_tier: flex",
            ),
            (
                WireFamilyId::AnthropicMessages,
                "anthropic",
                ServiceTier::Priority,
                403,
                r#"{"type":"error","error":{"type":"permission_error","message":"Fast mode is not available for your organization."}}"#,
                "Fast mode is not available for your organization.",
            ),
            (
                WireFamilyId::AnthropicMessages,
                "anthropic",
                ServiceTier::Priority,
                400,
                r#"{"type":"error","error":{"type":"invalid_request_error","message":"speed: fast is not supported for this model"}}"#,
                "speed: fast is not supported for this model",
            ),
            (
                WireFamilyId::Gemini,
                "google",
                ServiceTier::Flex,
                403,
                r#"{"error":{"code":403,"message":"service_tier FLEX is not enabled for this project.","status":"PERMISSION_DENIED"}}"#,
                "service_tier FLEX is not enabled for this project.",
            ),
            (
                WireFamilyId::OpenAiChat,
                "xai",
                ServiceTier::Priority,
                403,
                r#"{"error":{"message":"Your team is not eligible for service_tier priority.","type":"invalid_request_error","param":null,"code":null}}"#,
                "Your team is not eligible for service_tier priority.",
            ),
        ];
        for (family, provider, tier, status, body, provider_message) in cases {
            let error = classified(family, provider, tier, status, body);
            assert_eq!(error.class, ErrorClass::Permanent, "{provider}: {body}");
            assert_eq!(
                error.provider_code.as_deref(),
                Some(SERVICE_TIER_UNAVAILABLE)
            );
            assert_eq!(error.status, Some(status));
            assert!(
                error.message.contains(&format!("\"{tier}\"")),
                "{}",
                error.message
            );
            assert!(
                error.message.contains(&format!("'{provider}'")),
                "{}",
                error.message
            );
            assert!(
                error.message.contains(provider_message),
                "{}",
                error.message
            );
        }
    }

    /// The one refusal captured live, byte for byte: Anthropic sends "no credits for fast mode"
    /// as a 429 `rate_limit_error`. It must become the permanent tier code, while a genuine
    /// fast-mode RATE LIMIT (same status and type, no statement of what the account lacks) must
    /// stay the transient class it came in with, or routing would stop asking for a tier that
    /// was only busy.
    #[test]
    fn anthropic_fast_mode_without_credits_is_a_tier_refusal_but_a_busy_429_is_not() {
        const CAPTURED: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"Usage credits are required for fast mode."},"request_id":"req_011CfQqPwHCYLA9161tYEBSn"}"#;
        let busy = |body: &str| ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Transient,
            message: format!("429: {body}"),
            status: Some(429),
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let refused = classify_tier_refusal(
            WireFamilyId::AnthropicMessages,
            Some(&request("anthropic", ServiceTier::Priority)),
            429,
            CAPTURED,
            busy(CAPTURED),
        );
        assert_eq!(refused.class, ErrorClass::Permanent, "{}", refused.message);
        assert_eq!(
            refused.provider_code.as_deref(),
            Some(SERVICE_TIER_UNAVAILABLE)
        );
        assert!(refused
            .message
            .contains("Usage credits are required for fast mode."));

        const RATE_LIMITED: &str = r#"{"type":"error","error":{"type":"rate_limit_error","message":"Fast mode rate limit exceeded. Please retry later."}}"#;
        let still_busy = classify_tier_refusal(
            WireFamilyId::AnthropicMessages,
            Some(&request("anthropic", ServiceTier::Priority)),
            429,
            RATE_LIMITED,
            busy(RATE_LIMITED),
        );
        assert_eq!(still_busy.class, ErrorClass::Transient);
        assert_eq!(still_busy.provider_code, None);
    }

    /// What is NOT a tier refusal keeps the class it came in with: a 401 (always about the
    /// credential), capacity statuses, a 403 that names the key rather than the tier, and any
    /// error on a request that carried no tier.
    #[test]
    fn credential_capacity_and_unrelated_errors_are_left_alone() {
        let tier_body = r#"{"error":{"message":"service_tier priority unavailable","type":"invalid_request_error","param":"service_tier"}}"#;
        for status in [401, 408, 429, 500, 529] {
            let error = classified(
                WireFamilyId::OpenAiResponses,
                "openai",
                ServiceTier::Priority,
                status,
                tier_body,
            );
            assert_eq!(error.class, ErrorClass::Auth, "{status}");
            assert_eq!(error.provider_code, None, "{status}");
        }
        for (family, provider, body) in [
            (
                WireFamilyId::AnthropicMessages,
                "anthropic",
                r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#,
            ),
            (
                WireFamilyId::AnthropicMessages,
                "anthropic",
                r#"{"type":"error","error":{"type":"permission_error","message":"Your API key does not have permission to use the specified resource."}}"#,
            ),
            (
                WireFamilyId::OpenAiResponses,
                "openai",
                r#"{"error":{"message":"Incorrect API key provided.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}"#,
            ),
            (
                WireFamilyId::Gemini,
                "google",
                r#"{"error":{"code":403,"message":"Method doesn't allow unregistered callers.","status":"PERMISSION_DENIED"}}"#,
            ),
            (WireFamilyId::OpenAiResponses, "openai", "not json"),
        ] {
            let error = classified(family, provider, ServiceTier::Priority, 403, body);
            assert_eq!(error.class, ErrorClass::Auth, "{body}");
            assert_eq!(error.provider_code, None, "{body}");
        }
        let untouched = ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Permanent,
            message: "403".into(),
            status: Some(403),
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        assert_eq!(
            classify_tier_refusal(
                WireFamilyId::OpenAiResponses,
                None,
                403,
                tier_body,
                untouched.clone()
            ),
            untouched
        );
    }

    #[test]
    fn pro_and_unknown_modes_never_map_to_a_tier() {
        assert_eq!(tier_for_mode("fast"), Some(ServiceTier::Priority));
        assert_eq!(tier_for_mode("priority"), Some(ServiceTier::Priority));
        assert_eq!(tier_for_mode("flex"), Some(ServiceTier::Flex));
        for mode in ["pro", "standard", "default", "turbo", ""] {
            assert_eq!(tier_for_mode(mode), None, "{mode}");
        }
        // A priced `pro` mode opens nothing.
        assert_eq!(
            offered_tiers("openai", WireFamilyId::OpenAiResponses, &modes(&["pro"])),
            vec![ServiceTier::Default]
        );
    }

    #[test]
    fn families_without_a_mapping_offer_no_tier_at_all() {
        let every_mode = modes(&["fast", "priority", "flex"]);
        for (provider, family) in [
            ("fireworks-ai", WireFamilyId::OpenAiChat),
            ("deepseek", WireFamilyId::OpenAiChat),
            ("openai-compatible", WireFamilyId::OpenAiChat),
            ("google", WireFamilyId::GoogleCodeAssist),
            ("amazon-bedrock", WireFamilyId::BedrockConverse),
            ("openai", WireFamilyId::AnthropicMessages),
        ] {
            assert!(
                offered_tiers(provider, family, &every_mode).is_empty(),
                "{provider} on {family:?}"
            );
            for tier in ServiceTier::ALL {
                assert_eq!(
                    tier_offered(provider, family, tier, &every_mode),
                    Err(TierUnavailable::NoMapping)
                );
            }
        }
    }

    #[test]
    fn a_family_with_bytes_still_needs_a_priced_mode() {
        let none = modes(&[]);
        assert_eq!(
            tier_offered(
                "openai",
                WireFamilyId::OpenAiChat,
                ServiceTier::Priority,
                &none
            ),
            Err(TierUnavailable::NoCatalogRate)
        );
        assert_eq!(
            tier_offered(
                "anthropic",
                WireFamilyId::AnthropicMessages,
                ServiceTier::Flex,
                &none
            ),
            Err(TierUnavailable::NoMapping)
        );
        assert_eq!(
            offered_tiers(
                "anthropic",
                WireFamilyId::AnthropicMessages,
                &modes(&["fast"])
            ),
            vec![ServiceTier::Default, ServiceTier::Priority]
        );
        assert_eq!(
            offered_tiers(
                "xai",
                WireFamilyId::OpenAiChat,
                &modes(&["priority", "flex"])
            ),
            vec![ServiceTier::Default, ServiceTier::Priority]
        );
    }
}
