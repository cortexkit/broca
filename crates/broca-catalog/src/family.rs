//! Map upstream wire packages and API shapes to Broca's implemented renderers.
//!
//! Runtime admission requires a row's routing facts, except for credential-owned client
//! personas and the closed native endpoint holds below. The provider-id resolver remains
//! for those holds and for the legacy snapshot importer; it must never supply a guessed
//! grammar or endpoint for a migrated provider whose facts are missing.

use broca_provider::spec::WireFamilyId;

/// Fusiform will publish native SDK default endpoints as sourced, curated facts.
/// Until those facts are served, these providers retain the existing resolver path.
/// Delete this closed exception list when the curated endpoints land; an unknown
/// endpoint on any other provider must refuse instead of borrowing a renderer default.
pub fn native_endpoint_is_held(provider: &str) -> bool {
    ["openai", "anthropic", "google", "xai"].contains(&provider)
}

/// Meta subscription credentials require the Meta Responses client. This endpoint is
/// credential policy, including for curated rows that have no models.dev wire metadata.
pub(crate) fn credential_persona_route(
    provider: &str,
    method: &str,
) -> Option<(WireFamilyId, &'static str)> {
    (provider == "meta" && method == "oauth")
        .then_some((WireFamilyId::OpenAiResponses, "https://api.meta.ai/v1"))
}

/// Translate an upstream SDK package and API shape into a renderer Broca implements.
/// Package names describe the upstream protocol, not an endpoint or a model alias.
/// Unknown shapes must not silently fall back to a different request grammar.
pub fn wire_family(npm: &str, shape: Option<&str>) -> Option<WireFamilyId> {
    use WireFamilyId::*;
    match (npm, shape) {
        ("@ai-sdk/openai" | "@ai-sdk/xai", None | Some("responses")) => Some(OpenAiResponses),
        ("@ai-sdk/openai" | "@ai-sdk/openai-compatible", Some("completions")) => Some(OpenAiChat),
        ("@ai-sdk/openai-compatible" | "@ai-sdk/amazon-bedrock/mantle", Some("responses")) => {
            Some(OpenAiResponses)
        }
        ("@ai-sdk/anthropic", None) => Some(AnthropicMessages),
        ("@ai-sdk/google", None) => Some(Gemini),
        ("@ai-sdk/amazon-bedrock", None) => Some(BedrockConverse),
        (
            "@ai-sdk/openai-compatible" | "@openrouter/ai-sdk-provider" | "@ai-sdk/cerebras",
            None,
        ) => Some(OpenAiChat),
        _ => None,
    }
}

/// Legacy native renderer defaults retained by the closed endpoint holds. Migrated
/// providers resolve their package and shape through `wire_family` instead.
const NATIVE_FAMILIES: &[(&str, WireFamilyId)] = &[
    ("anthropic", WireFamilyId::AnthropicMessages),
    ("openai", WireFamilyId::OpenAiChat),
    ("google", WireFamilyId::Gemini),
];

/// Which rule resolved a provider's family — recorded so the per-provider refresh
/// report can show exactly why each provider routes where it does (and a
/// reclassification surfaces as a reviewed row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionSource {
    /// A credential-owned client persona, independent of model metadata.
    CredentialPersona,
    /// An upstream wire package and API shape mapped to a Broca renderer.
    WireFacts,
    /// An explicit `wire_family` in our overlay.
    Overlay,
    /// Our provider-id hand-table.
    ProviderId,
}

impl ResolutionSource {
    /// A stable lowercase tag for the report artifact.
    pub fn tag(self) -> &'static str {
        match self {
            ResolutionSource::CredentialPersona => "credential_persona",
            ResolutionSource::WireFacts => "wire_facts",
            ResolutionSource::Overlay => "overlay",
            ResolutionSource::ProviderId => "provider_id",
        }
    }
}

/// The outcome of resolving a provider's wire family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FamilyResolution {
    /// A renderer was chosen, with the rule that chose it.
    Supported {
        family: WireFamilyId,
        source: ResolutionSource,
    },
    /// We cannot safely choose a renderer yet. Carries the reason for the report.
    Pending { reason: String },
}

/// Resolve the wire family for a provider from our overlay + the provider-id
/// hand-table. See the module docs for the rule order and guardrails.
pub fn resolve_provider_family(
    provider_id: &str,
    overlay_family: Option<WireFamilyId>,
) -> FamilyResolution {
    // Rule 1: an explicit overlay family is the ultimate override.
    if let Some(family) = overlay_family {
        return FamilyResolution::Supported {
            family,
            source: ResolutionSource::Overlay,
        };
    }
    // Rule 2: known provider id → hand-table.
    if let Some((_, family)) = NATIVE_FAMILIES.iter().find(|(id, _)| *id == provider_id) {
        return FamilyResolution::Supported {
            family: *family,
            source: ResolutionSource::ProviderId,
        };
    }
    // This compatibility resolver does not inspect wire facts. Its callers must explicitly
    // map a published package and shape; an unknown provider id alone names no renderer.
    FamilyResolution::Pending {
        reason: format!(
            "{provider_id}: no overlay wire_family pin and no provider-id hand-table entry"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family(r: &FamilyResolution) -> Option<(WireFamilyId, ResolutionSource)> {
        match r {
            FamilyResolution::Supported { family, source } => Some((*family, *source)),
            FamilyResolution::Pending { .. } => None,
        }
    }

    #[test]
    fn overlay_beats_provider_id() {
        // anthropic is in the provider-id table, yet an explicit overlay still wins.
        let r = resolve_provider_family("anthropic", Some(WireFamilyId::OpenAiResponses));
        assert_eq!(
            family(&r),
            Some((WireFamilyId::OpenAiResponses, ResolutionSource::Overlay))
        );
    }

    #[test]
    fn provider_id_resolves_without_overlay() {
        let r = resolve_provider_family("openai", None);
        assert_eq!(
            family(&r),
            Some((WireFamilyId::OpenAiChat, ResolutionSource::ProviderId))
        );
    }

    #[test]
    fn npm_package_does_not_choose_a_family() {
        // Third-party hosts that used to ride the npm allowlist stay Pending
        // until an overlay pin names the family.
        assert!(matches!(
            resolve_provider_family("minimax", None),
            FamilyResolution::Pending { .. }
        ));
        assert!(matches!(
            resolve_provider_family("vivgrid", None),
            FamilyResolution::Pending { .. }
        ));
        assert!(matches!(
            resolve_provider_family("deepseek", None),
            FamilyResolution::Pending { .. }
        ));
    }

    #[test]
    fn vertex_anthropic_without_endpoint_and_auth_policy_is_typed_pending() {
        let resolution = resolve_provider_family("google-vertex-anthropic", None);
        let FamilyResolution::Pending { reason } = resolution else {
            panic!("Vertex Anthropic must fail closed until its endpoint and auth policy exist");
        };
        assert!(reason.contains("no overlay wire_family pin"));
    }

    #[test]
    fn vertex_without_endpoint_and_auth_policy_is_typed_pending() {
        let resolution = resolve_provider_family("google-vertex", None);
        let FamilyResolution::Pending { reason } = resolution else {
            panic!("Vertex must fail closed until its endpoint and auth policy exist");
        };
        assert!(reason.contains("no overlay wire_family pin"));
    }

    #[test]
    fn unsupported_native_providers_stay_pending() {
        for id in ["mistral", "cohere", "groq", "xai", "azure", "openrouter"] {
            assert!(
                matches!(
                    resolve_provider_family(id, None),
                    FamilyResolution::Pending { .. }
                ),
                "{id} must stay Pending without an overlay pin, not be served"
            );
        }
    }

    #[test]
    fn unknown_provider_is_pending() {
        assert!(matches!(
            resolve_provider_family("mystery", None),
            FamilyResolution::Pending { .. }
        ));
    }
}
