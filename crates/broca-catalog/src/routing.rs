//! Upstream routing facts, kept separate from renderer and credential policy.
use std::collections::BTreeMap;

use broca_provider::spec::WireFamilyId;
use serde::Deserialize;
use serde_json::Value;

pub(crate) const MISSING_CODE_ASSIST_TIERS: &str = "Google Code Assist is unavailable for this model: Fusiform has not published reasoning-tier backend model IDs (missing route.reasoning_wire_ids); choose a model with a published Google Code Assist route";

/// Fusiform's wire.api follows models.dev's published Anthropic-root convention:
/// the root already includes `/v1` (https://models.dev/api.json). Broca's unchanged
/// Anthropic renderer instead adds `/v1/messages` to a versionless root
/// (broca-provider/src/families/anthropic_messages.rs).
/// Adapt only served roots before freeze; operator overrides and recorded configs
/// already use Broca's convention and must not be rewritten.
pub(crate) fn renderer_base_url(family: WireFamilyId, served: &str) -> String {
    if family == WireFamilyId::AnthropicMessages {
        if let Some(base) = served.trim_end_matches('/').strip_suffix("/v1") {
            return base.to_string();
        }
    }
    served.to_string()
}

/// Provenance is an open string: a new producer source does not change the served value.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WireFact {
    pub value: String,
    pub from: String,
}

#[derive(Clone, Debug, Default)]
pub struct RoutingFacts {
    pub npm: Option<WireFact>,
    pub api: Option<WireFact>,
    pub shape: Option<WireFact>,
    pub reasoning_wire_ids: Option<BTreeMap<String, String>>,
    pub reasoning_default: Option<String>,
    pub successor: Option<String>,
    error: Option<String>,
}

impl RoutingFacts {
    pub(crate) fn decode(facts: &BTreeMap<String, Value>) -> Self {
        fn read<T: serde::de::DeserializeOwned>(
            facts: &BTreeMap<String, Value>,
            key: &str,
        ) -> Result<Option<T>, String> {
            facts
                .get(key)
                .map(|v| {
                    serde_json::from_value(v.clone()).map_err(|e| format!("invalid {key}: {e}"))
                })
                .transpose()
        }
        let decoded = (|| {
            Ok::<_, String>(Self {
                npm: read(facts, "wire.npm")?,
                api: read(facts, "wire.api")?,
                shape: read(facts, "wire.shape")?,
                reasoning_wire_ids: read(facts, "route.reasoning_wire_ids")?,
                reasoning_default: read(facts, "route.reasoning_default")?,
                successor: read(facts, "successor")?,
                error: None,
            })
        })();
        decoded.unwrap_or_else(|error| Self {
            error: Some(error),
            ..Self::default()
        })
    }

    fn validate(&self) -> Result<(), String> {
        self.error.as_ref().map_or(Ok(()), |e| Err(e.clone()))
    }

    pub fn route(&self) -> Result<(WireFamilyId, String), String> {
        self.validate()?;
        let api = self
            .api
            .as_ref()
            .filter(|api| !api.value.trim().is_empty())
            .ok_or("missing wire.api")?;
        let npm = self.npm.as_ref().ok_or("missing wire.npm")?;
        let family =
            crate::family::wire_family(&npm.value, self.shape.as_ref().map(|s| s.value.as_str()))
                .ok_or_else(|| {
                format!(
                    "unsupported wire.npm '{}' / wire.shape {:?}",
                    npm.value,
                    self.shape.as_ref().map(|s| &s.value)
                )
            })?;
        Ok((family, renderer_base_url(family, &api.value)))
    }

    pub fn tier(&self, requested: Option<&str>) -> Result<String, String> {
        self.validate()?;
        let tiers = self
            .reasoning_wire_ids
            .as_ref()
            .ok_or(MISSING_CODE_ASSIST_TIERS)?;
        let level = requested
            .or(self.reasoning_default.as_deref())
            .ok_or("missing route.reasoning_default")?;
        tiers
            .get(level)
            .filter(|id| !id.is_empty())
            .cloned()
            .ok_or_else(|| format!("route.reasoning_wire_ids has no tier '{level}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vectors() -> BTreeMap<String, Value> {
        serde_json::from_str(include_str!(
            "../tests/fixtures/fusiform_routing_vectors.json"
        ))
        .unwrap()
    }

    fn row(name: &str) -> RoutingFacts {
        let response: crate::FusiformCatalogResponse =
            serde_json::from_value(vectors()[name]["response"].clone()).unwrap();
        RoutingFacts::decode(response.models.values().next().unwrap())
    }

    #[test]
    fn decodes_every_new_fact_from_fusiform_vectors() {
        let facts = row("catalog.get — pi corrected endpoint");
        assert_eq!(
            facts.api.unwrap(),
            WireFact {
                value: "https://opencode.ai/inference/go".into(),
                from: "curated".into()
            }
        );
        assert_eq!(facts.npm.unwrap().value, "@ai-sdk/anthropic");
        assert_eq!(facts.shape.unwrap().value, "observed-shape");
        let facts = row("catalog.get — alias tier wire ids");
        assert_eq!(facts.reasoning_default.as_deref(), Some("medium"));
        for tier in ["low", "medium", "high"] {
            assert_eq!(
                facts.tier(Some(tier)).unwrap(),
                format!("gemini-3.8-flash-{tier}")
            );
        }
        assert_eq!(facts.tier(None).unwrap(), "gemini-3.8-flash-medium");
        assert_eq!(
            row("catalog.get — retired alias successor")
                .successor
                .as_deref(),
            Some("google/antigravity-gemini-3.8-flash")
        );
    }

    #[test]
    fn corrected_provider_and_go_goldens_use_renderer_root_conventions() {
        let native = row("catalog.get — provider default endpoint");
        assert_eq!(native.api.as_ref().unwrap().from, "curated");
        assert_eq!(
            native.route().unwrap(),
            (
                WireFamilyId::OpenAiResponses,
                "https://api.openai.com/v1".into()
            )
        );
        let go = row("catalog.get — pi Anthropic effective endpoint");
        assert_eq!(
            go.api.as_ref().unwrap().value,
            "https://opencode.ai/inference/go/v1"
        );
        assert_eq!(
            go.route().unwrap(),
            (
                WireFamilyId::AnthropicMessages,
                "https://opencode.ai/inference/go".into()
            )
        );
    }

    #[test]
    fn missing_fact_refuses_instead_of_guessing() {
        let facts = row("catalog.get — pi withheld endpoint");
        assert_eq!(facts.route().unwrap_err(), "missing wire.api");
    }

    #[test]
    fn provenance_is_open_but_protocol_shape_is_not() {
        let facts = BTreeMap::from([
            (
                "wire.npm".into(),
                serde_json::json!({"value":"@ai-sdk/openai", "from":"future-source"}),
            ),
            (
                "wire.api".into(),
                serde_json::json!({"value":"https://example.test/v1", "from":"future-source"}),
            ),
        ]);
        let mut routing = RoutingFacts::decode(&facts);
        assert_eq!(
            routing.route().unwrap(),
            (
                WireFamilyId::OpenAiResponses,
                "https://example.test/v1".into()
            )
        );
        routing.shape = Some(WireFact {
            value: "future-shape".into(),
            from: "curated".into(),
        });
        assert!(routing
            .route()
            .unwrap_err()
            .contains("unsupported wire.npm"));
        routing.shape = None;
        routing.npm = None;
        assert_eq!(routing.route().unwrap_err(), "missing wire.npm");
    }

    #[test]
    fn tier_ids_are_upstream_values_not_derived_names() {
        let mut facts = row("catalog.get — alias tier wire ids");
        facts
            .reasoning_wire_ids
            .as_mut()
            .unwrap()
            .insert("high".into(), "opaque-backend-revision".into());
        assert_eq!(facts.tier(Some("high")).unwrap(), "opaque-backend-revision");
        facts.reasoning_default = Some("high".into());
        assert_eq!(facts.tier(None).unwrap(), "opaque-backend-revision");
        facts.reasoning_default = None;
        assert_eq!(
            facts.tier(None).unwrap_err(),
            "missing route.reasoning_default"
        );
    }
}
