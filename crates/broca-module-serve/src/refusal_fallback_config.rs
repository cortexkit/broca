//! The refusal fallback: which model a run switches to when the model it is talking to refuses
//! the content of the conversation.
//!
//! Anthropic answers a refusal with a terminal `stop_reason: "refusal"` and nothing the caller
//! can use. This section says, per source model, which model a refused step is re-sent to. It is
//! an operator policy, not a catalog fact: two operators may want different targets for the same
//! source model, and a fallback that silently followed the catalog could change a transcript's
//! producing model without anyone editing a config file.
//!
//! In the config-home `broca.jsonc`:
//!
//! ```jsonc
//! {"refusal_fallback": {
//!     "claude-opus-5-5": "claude-opus-4-8",
//!     "claude-opus-5": "claude-opus-4-8",
//! }}
//! ```
//!
//! With no section, [`DEFAULT_MAPPINGS`] applies. An explicit section replaces the defaults
//! wholesale, and an empty object (`{"refusal_fallback": {}}`) switches the fallback off, so an
//! operator can both add a mapping and withdraw one that shipped. Both sides of every entry name
//! a model on provider [`FALLBACK_PROVIDER`]: the fallback runs on the same account and the same
//! wire family as the model that refused, so only the model id changes.
//!
//! Every entry is validated at load time. A defective entry refuses boot by name rather than
//! being dropped, because a mapping an operator wrote and broca ignored would only be noticed
//! when a refusal happened and nothing switched.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use broca_catalog::CatalogSnapshot;
use serde::Deserialize;

/// The provider every mapped model belongs to.
pub const FALLBACK_PROVIDER: &str = "anthropic";

/// What a refused step on `claude-opus-5-5` and its siblings is re-sent to when
/// `broca.jsonc` configures nothing.
pub const DEFAULT_MAPPINGS: &[(&str, &str)] = &[
    ("claude-opus-5-5", "claude-opus-4-8"),
    ("claude-opus-5", "claude-opus-4-8"),
    ("claude-fable-5", "claude-opus-4-8"),
    ("claude-fable-5-1", "claude-opus-4-8"),
];

/// Where one source model's refused steps go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackTarget {
    pub provider: String,
    pub model: String,
}

/// The `refusal_fallback` section of `broca.jsonc`: source model id → fallback model id, both on
/// [`FALLBACK_PROVIDER`].
///
/// The maps carry model ids rather than resolved routes on purpose. A model's catalog row can be
/// withdrawn between one boot and the next, and the fallback route is resolved (and validated
/// against the catalog that is loaded then) at each episode's admission, not here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusalFallbackConfig {
    mappings: BTreeMap<String, String>,
}

impl Default for RefusalFallbackConfig {
    fn default() -> Self {
        Self {
            mappings: DEFAULT_MAPPINGS
                .iter()
                .map(|(source, fallback)| ((*source).to_string(), (*fallback).to_string()))
                .collect(),
        }
    }
}

impl RefusalFallbackConfig {
    /// Build from pairs. Values are validated exactly as a loaded file's are.
    pub fn new(mappings: BTreeMap<String, String>) -> Result<Self, String> {
        for (source, fallback) in &mappings {
            validate_model_id(
                &format!("refusal_fallback.{source}"),
                "source model",
                source,
            )?;
            validate_model_id(
                &format!("refusal_fallback.{source}"),
                "fallback model",
                fallback,
            )?;
            if source == fallback {
                return Err(format!(
                    "refusal_fallback.{source}: maps {source} to itself, so a refusal would \
                     re-send the step to the model that just refused it"
                ));
            }
        }
        Ok(Self { mappings })
    }

    /// A config with no mappings: every refusal commits as it does with no fallback at all.
    pub fn disabled() -> Self {
        Self {
            mappings: BTreeMap::new(),
        }
    }

    /// Load from the config-home `broca.jsonc`, through the same ladder as its other sections.
    pub fn load_default_path() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Resolved { path, .. } => Self::load(&path),
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    /// A missing file keeps the shipped defaults; an unreadable or malformed one refuses boot.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => return Err(format!("read broca config {}: {error}", path.display())),
        };
        Self::parse(&text)
            .map_err(|error| format!("parse broca config {}: {error}", path.display()))
    }

    /// Parse the `refusal_fallback` section of a `broca.jsonc` text. Other root keys belong to
    /// other loaders and are ignored. A file that keeps every shipped entry spelled out is left
    /// as written; a file that leaves entries out replaces the defaults with exactly what it
    /// names.
    pub fn parse(text: &str) -> Result<Self, String> {
        let stripped = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&stripped).map_err(|error| error.to_string())?;
        let Some(section) = object.get("refusal_fallback") else {
            return Ok(Self::default());
        };
        let entries: BTreeMap<String, String> =
            serde_json::from_value(section.clone()).map_err(|error| {
                format!("refusal_fallback must map model ids to model ids: {error}")
            })?;
        Self::new(entries)
    }

    /// The model a refused step on `source` is re-sent to, or `None` when `source` has no
    /// mapping.
    pub fn target(&self, source: &str) -> Option<FallbackTarget> {
        self.mappings.get(source).map(|fallback| FallbackTarget {
            provider: FALLBACK_PROVIDER.to_string(),
            model: fallback.clone(),
        })
    }

    /// Every mapping, in model-id order.
    pub fn mappings(&self) -> &BTreeMap<String, String> {
        &self.mappings
    }

    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty()
    }

    /// Name every mapped model `catalog` does not serve, for one boot-time report. The loaded
    /// catalog decides whether a route exists, so this is asked of a catalog rather than of the
    /// section: it is a notice, never a refusal, and an entry that names nothing today may name
    /// a model the next refresh serves.
    pub fn unserved_notices(&self, catalog: &CatalogSnapshot) -> Vec<String> {
        let mut named: BTreeSet<(&str, &str)> = BTreeSet::new();
        for (source, fallback) in &self.mappings {
            named.insert((source.as_str(), "source"));
            named.insert((fallback.as_str(), "fallback"));
        }
        named
            .into_iter()
            .filter(|(model, _)| !catalog.contains_model(FALLBACK_PROVIDER, model))
            .map(|(model, side)| {
                format!(
                    "refusal_fallback names {model} as a {side} model, but the loaded catalog \
                     does not serve {FALLBACK_PROVIDER}/{model}; that mapping stays unused until \
                     a catalog refresh serves it"
                )
            })
            .collect()
    }
}

/// Refuse an id that could not name a catalog model. The provider half of the catalog id is
/// fixed by [`FALLBACK_PROVIDER`], so only the model half is configurable.
fn validate_model_id(entry: &str, side: &str, model: &str) -> Result<(), String> {
    if model.trim().is_empty() {
        return Err(format!("{entry}: the {side} must not be empty"));
    }
    if model.contains('/') {
        return Err(format!(
            "{entry}: the {side} {model:?} must be a model id without a provider, because every \
             mapped model belongs to the {FALLBACK_PROVIDER} provider"
        ));
    }
    Ok(())
}

/// The `broca.jsonc` root keys this module reads, as the shape a JSON parser sees them. The
/// parser above reads the raw object instead, so that a defective entry is refused by name;
/// this view pins the root key and the entry type for that reading.
#[derive(Deserialize)]
#[cfg_attr(not(test), allow(dead_code))]
struct BrocaConfigRefusalFallbackView {
    #[serde(default)]
    refusal_fallback: Option<BTreeMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_defaults_map_the_refusing_models_to_opus_4_8() {
        let config = RefusalFallbackConfig::default();
        for source in [
            "claude-opus-5-5",
            "claude-opus-5",
            "claude-fable-5",
            "claude-fable-5-1",
        ] {
            assert_eq!(
                config.target(source),
                Some(FallbackTarget {
                    provider: "anthropic".into(),
                    model: "claude-opus-4-8".into(),
                }),
                "{source}"
            );
        }
        assert_eq!(config.target("claude-haiku-5-5"), None);
        assert_eq!(
            RefusalFallbackConfig::parse("{}").unwrap(),
            RefusalFallbackConfig::default(),
            "a file with no section keeps the shipped defaults"
        );
    }

    #[test]
    fn an_explicit_section_replaces_the_shipped_defaults() {
        let config = RefusalFallbackConfig::parse(
            r#"{
            // Other config loaders own other root keys.
            "providers": {},
            "refusal_fallback": {"claude-opus-5-5": "claude-opus-4-6",},
        }"#,
        )
        .unwrap();
        assert_eq!(
            config.target("claude-opus-5-5").unwrap().model,
            "claude-opus-4-6"
        );
        assert_eq!(
            config.target("claude-fable-5"),
            None,
            "an explicit section replaces the defaults wholesale, so an entry it leaves out is \
             not mapped"
        );
    }

    #[test]
    fn an_empty_section_switches_the_fallback_off() {
        let config = RefusalFallbackConfig::parse(r#"{"refusal_fallback": {}}"#).unwrap();
        assert!(config.is_empty());
        assert_eq!(config.target("claude-opus-5-5"), None);
    }

    #[test]
    fn a_mapping_to_itself_is_refused_naming_the_entry() {
        let error = RefusalFallbackConfig::parse(
            r#"{"refusal_fallback": {"claude-opus-5": "claude-opus-5"}}"#,
        )
        .unwrap_err();
        assert!(error.contains("refusal_fallback.claude-opus-5"), "{error}");
        assert!(error.contains("itself"), "{error}");
    }

    #[test]
    fn a_malformed_entry_is_refused_naming_it() {
        for (text, named) in [
            (
                r#"{"refusal_fallback": {"claude-opus-5": ""}}"#,
                "claude-opus-5",
            ),
            (
                r#"{"refusal_fallback": {"": "claude-opus-4-8"}}"#,
                "refusal_fallback.",
            ),
            (
                r#"{"refusal_fallback": {"claude-opus-5": "anthropic/claude-opus-4-8"}}"#,
                "claude-opus-5",
            ),
            (
                r#"{"refusal_fallback": {"claude-opus-5": 42}}"#,
                "refusal_fallback",
            ),
            (r#"{"refusal_fallback": null}"#, "refusal_fallback"),
            (
                r#"{"refusal_fallback": ["claude-opus-5"]}"#,
                "refusal_fallback",
            ),
        ] {
            let error = RefusalFallbackConfig::parse(text).unwrap_err();
            assert!(error.contains(named), "{text}: {error}");
        }
    }

    #[test]
    fn a_typical_mapping_reports_a_model_the_catalog_does_not_serve() {
        let config = RefusalFallbackConfig::new(BTreeMap::from([(
            "unknown-model".to_string(),
            "claude-opus-4-8".to_string(),
        )]))
        .unwrap();
        let cache = broca_catalog::CatalogCache::empty();
        let catalog = broca_catalog::Catalog::from_json_str(
            include_str!("../../broca-catalog/data/models.dev.json"),
            "{}",
        )
        .expect("bundled snapshot is well formed");
        cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
        let snapshot = cache.snapshot().unwrap();
        let notices = config.unserved_notices(&snapshot);
        assert!(
            notices
                .iter()
                .any(|notice| notice.contains("unknown-model") && notice.contains("as a source")),
            "{notices:?}"
        );
        assert!(
            !notices
                .iter()
                .any(|notice| notice.contains("claude-opus-4-8")),
            "the fallback model is served: {notices:?}"
        );
    }

    #[test]
    fn the_file_view_accepts_the_key_the_parser_reads() {
        let view: BrocaConfigRefusalFallbackView =
            serde_json::from_str(r#"{"refusal_fallback": {"a": "b"}}"#).unwrap();
        assert_eq!(
            view.refusal_fallback.unwrap(),
            BTreeMap::from([("a".to_string(), "b".to_string())])
        );
    }
}
