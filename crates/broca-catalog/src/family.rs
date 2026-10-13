//! Wire-family selection — the SINGLE place a provider's renderer is chosen.
//!
//! Family selection picks the RENDERER (the exact request bytes), so it is a
//! byte-correctness decision and must be OUR call — never sourced from a
//! catalog-controlled field that an upstream edit could silently change to re-route a
//! native-grammar provider to the wrong renderer.
//!
//! The resolver order (first match wins):
//!   1. an explicit `wire_family` in our overlay (the ultimate override);
//!   2. a known provider id in our hand-table;
//!   3. Pending, NEVER blind-defaulted (defaulting a native grammar to OpenAiChat
//!      would mis-render).
//!
//! Overlay pins carry `wire_family_source`: `authored` means a person chose the
//! family; `generated` means the retired npm/api inference produced it on one
//! snapshot. The mark exists because a bare `wire_family` value cannot tell those
//! apart, and treating an inherited inference as a human decision would make every
//! pin look equally authoritative.

use broca_provider::spec::WireFamilyId;

/// Providers with a NATIVE grammar, mapped by provider id (rule 2). The renderer
/// choice is ours; a provider appears here only once the listed family handles it.
const NATIVE_FAMILIES: &[(&str, WireFamilyId)] = &[
    ("anthropic", WireFamilyId::AnthropicMessages),
    ("openai", WireFamilyId::OpenAiChat),
    ("google", WireFamilyId::Gemini),
    ("amazon-bedrock", WireFamilyId::BedrockConverse),
];

/// Which rule resolved a provider's family — recorded so the per-provider refresh
/// report can show exactly why each provider routes where it does (and a
/// reclassification surfaces as a reviewed row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionSource {
    /// An explicit `wire_family` in our overlay.
    Overlay,
    /// Our provider-id hand-table.
    ProviderId,
}

impl ResolutionSource {
    /// A stable lowercase tag for the report artifact.
    pub fn tag(self) -> &'static str {
        match self {
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
    // The npm allowlist (old rule 3) and `@ai-sdk/openai-compatible` fallback
    // (old rule 4) were removed deliberately. 145 of 184 providers carried that
    // single openai-compatible string, so one upstream repackaging would silently
    // reclassify thousands of models. Family is overlay pin, then this hand-table,
    // then Pending — an unpinned provider fails closed until someone pins it.
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
