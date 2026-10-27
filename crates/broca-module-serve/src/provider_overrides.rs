//! Boot-only endpoint overrides. Identity, auth, billing and cache routing stay catalog-owned.
//!
//! In the config-home `broca.jsonc`, set `providers.<provider-id>.base_url`, for example:
//! ```json
//! {"providers":{"deepseek":{"base_url":"http://127.0.0.1:4010/v1"}}}
//! ```
//! Loading adds one config read at boot and a map lookup when resolving a new episode.
//! There is no per-request override or hot reload: in-flight and resumed episodes keep
//! their frozen host. The credential-free mock URL path takes precedence and is unchanged.
use std::{collections::BTreeMap, io, path::Path};

use serde::Deserialize;

#[derive(Clone, Debug, Default)]
pub struct ProviderOverrides(BTreeMap<String, ProviderOverride>);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderOverride {
    base_url: String,
}

impl ProviderOverrides {
    pub fn load_default_path() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Resolved { path, .. } => Self::load(&path),
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => return Err(format!("read broca config {}: {error}", path.display())),
        };
        Self::parse(&text)
            .map_err(|error| format!("parse broca config {}: {error}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let stripped = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&stripped).map_err(|error| error.to_string())?;
        let Some(providers) = object.get("providers") else {
            return Ok(Self::default());
        };
        let providers = providers.as_object().ok_or("providers must be an object")?;
        let mut overrides = BTreeMap::new();
        for (provider, value) in providers {
            let entry: ProviderOverride = serde_json::from_value(value.clone())
                .map_err(|error| format!("providers.{provider} {value}: {error}"))?;
            let valid = url::Url::parse(&entry.base_url).is_ok_and(|url| {
                matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
            });
            if !valid {
                return Err(format!(
                    "providers.{provider}.base_url must be an absolute HTTP(S) URL, got {:?}",
                    entry.base_url
                ));
            }
            overrides.insert(provider.clone(), entry);
        }
        Ok(Self(overrides))
    }

    pub fn base_url(&self, provider: &str) -> Option<&str> {
        self.0.get(provider).map(|entry| entry.base_url.as_str())
    }

    /// Name every override whose provider `catalog` does not serve. Overrides are applied
    /// when each new run starts, so a later catalog refresh that serves the provider makes
    /// the override take effect; the notice only describes the catalog it was given.
    pub fn notices(&self, catalog: &broca_catalog::CatalogSnapshot) -> Vec<String> {
        self.0.keys().filter(|provider| !catalog.contains_provider(provider)).map(|provider| format!(
            "provider base_url override for '{provider}' is unused: the loaded catalog does not serve this provider; new runs use the override once a catalog refresh serves it"
        )).collect()
    }

    /// Report, once at boot, the overrides the catalog does not serve.
    ///
    /// The catalog comes from fusiform and nothing is bundled, so at boot there may be no
    /// catalog loaded yet, and judging overrides then would call every one of them unused.
    /// The report is therefore made against a loaded catalog: at once when `cache` already
    /// holds one (a last-known-good copy), otherwise after `poller`'s first successful
    /// refresh. Returns the poller to start.
    pub fn report_unserved(
        &self,
        cache: &broca_catalog::CatalogCache,
        poller: crate::catalog_refresh::CatalogPoller,
        report: impl Fn(String) + Send + 'static,
    ) -> crate::catalog_refresh::CatalogPoller {
        if self.0.is_empty() {
            return poller;
        }
        match cache.snapshot() {
            Ok(catalog) => {
                self.notices(&catalog).into_iter().for_each(report);
                poller
            }
            Err(_) => {
                let overrides = self.clone();
                poller.on_first_refresh(move |catalog| {
                    overrides.notices(catalog).into_iter().for_each(report);
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_provider_url_names_provider_and_value() {
        for value in ["", "not a url", "ftp://example.com", "http://[bad"] {
            let error = ProviderOverrides::parse(
                &serde_json::json!({
                    "providers": {"deepseek": {"base_url": value}}
                })
                .to_string(),
            )
            .unwrap_err();
            assert!(error.contains("deepseek"), "{error}");
            assert!(error.contains(&format!("{value:?}")), "{error}");
        }
    }

    #[test]
    fn provider_overrides_jsonc_is_strict_only_inside_provider_entries() {
        let overrides = ProviderOverrides::parse(
            r#"{
            // Other config loaders own other root keys.
            "other": true,
            "providers": {"deepseek": {"base_url":"https://proxy.example/v1",},},
        }"#,
        )
        .unwrap();
        assert_eq!(
            overrides.base_url("deepseek"),
            Some("https://proxy.example/v1")
        );
        assert!(ProviderOverrides::parse(
            r#"{"providers":{"deepseek":{"base_url":"http://localhost","auth":"none"}}}"#
        )
        .is_err());
        assert!(ProviderOverrides::parse(r#"{"providers":null}"#).is_err());
        assert!(ProviderOverrides::parse("{}")
            .unwrap()
            .base_url("deepseek")
            .is_none());
    }

    #[test]
    fn unknown_provider_is_unused_with_notice() {
        let overrides = ProviderOverrides::parse(
            r#"{"providers":{"not-served":{"base_url":"http://localhost:4010"}}}"#,
        )
        .unwrap();
        let catalog = broca_catalog::Catalog::from_json_str("{}", "{}").unwrap();
        let cache = broca_catalog::CatalogCache::empty();
        cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
        assert_eq!(overrides.notices(&cache.snapshot().unwrap()), vec![
            "provider base_url override for 'not-served' is unused: the loaded catalog does not serve this provider; new runs use the override once a catalog refresh serves it"
        ]);
        assert!(catalog.provider("not-served").is_none());
    }
}
