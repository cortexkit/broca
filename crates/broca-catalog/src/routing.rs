//! Upstream routing facts, kept separate from renderer and credential policy.
use std::collections::BTreeMap;

use broca_provider::spec::WireFamilyId;
use serde::Deserialize;
use serde_json::Value;

use crate::runtime::SERVER_SIDE_FALLBACK_FACT;

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

/// Read a boolean row fact that may be absent. A value of the wrong type, or an explicit null,
/// simply states nothing here.
///
/// Reporting a malformed value is the row reader's job (`server_side_fallback_fact` in
/// `runtime.rs`), because it is the one place that has the row's catalog id to name. This decode
/// also runs on documents that carry no such id, and a WARN from each of them for one bad fact
/// would report the same defect several times per refresh.
fn optional_flag(facts: &BTreeMap<String, Value>, fact: &str) -> Option<bool> {
    facts.get(fact).and_then(Value::as_bool)
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
    /// Whether the runtime accepts a request that asks it to hand a refusal to another model
    /// itself. `None` when the catalog states nothing (an older catalog, a point-in-time read,
    /// or a value that was malformed), which callers read as false.
    pub server_side_fallback: Option<bool>,
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
                server_side_fallback: optional_flag(facts, SERVER_SIDE_FALLBACK_FACT),
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

    /// The test vectors whose response holds more than one row, read whole: these facts are
    /// per row, so a test picks its row by catalog id.
    fn named_row(vector: &str) -> crate::FusiformCatalogResponse {
        serde_json::from_value(vectors()[vector]["response"].clone()).unwrap()
    }

    /// The server-side fallback fact is a per-row claim, stated as a bare boolean, whose
    /// absence says nothing: unlike every other decoded fact it cannot refuse the row.
    #[test]
    fn server_side_fallback_decodes_beside_the_routing_facts_it_travels_with() {
        let response = named_row("catalog.get — anthropic server-side fallback capability");
        let expected = [
            ("claude-opus-5-5", Some(true)),
            ("claude-haiku-5-5", Some(false)),
            ("claude-sonnet-5", None),
        ];
        for (model, stated) in expected {
            let facts = RoutingFacts::decode(&response.models[&format!("anthropic/{model}")]);
            assert_eq!(
                facts.server_side_fallback, stated,
                "{model} states {stated:?}"
            );
            assert!(facts.validate().is_ok(), "{model} route must decode");
        }
    }

    /// A malformed value disables this one fact and leaves the row's route alone. Today one
    /// unreadable routing fact fails the whole decode, and an optional feature must not take a
    /// model offline. The WARN for the value comes from the row reader, which knows the row's
    /// catalog id; see the catalog tests.
    #[test]
    fn malformed_refusal_fallback_fact_keeps_the_route() {
        let response = named_row("catalog.get — anthropic server-side fallback capability");
        // Give the probe row a complete route, so the assertion below is about this one fact
        // rather than about whatever else the fixture row may lack.
        let mut row_facts = response.models["anthropic/claude-opus-5-5"].clone();
        row_facts.insert(
            "wire.npm".into(),
            serde_json::json!({"value": "@ai-sdk/anthropic", "from": "provider"}),
        );
        row_facts.insert(
            "wire.api".into(),
            serde_json::json!({"value": "https://api.anthropic.com/v1", "from": "curated"}),
        );
        let clean_route = RoutingFacts::decode(&row_facts)
            .route()
            .expect("the probe row routes");

        for value in [
            serde_json::json!("true"),
            serde_json::json!(1),
            serde_json::json!("maybe"),
            serde_json::json!([true]),
            serde_json::json!({"value": true}),
        ] {
            let mut malformed = row_facts.clone();
            malformed.insert(SERVER_SIDE_FALLBACK_FACT.into(), value.clone());

            let facts = RoutingFacts::decode(&malformed);

            assert_eq!(
                facts.route(),
                Ok(clean_route.clone()),
                "{value}: a malformed optional fact must not fail the row's route"
            );
            assert_eq!(
                facts.server_side_fallback, None,
                "{value}: an unreadable value reads as not stated"
            );
            assert_eq!(
                facts.tier(None),
                RoutingFacts::decode(&row_facts).tier(None),
                "{value}"
            );
        }
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
