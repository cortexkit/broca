//! Operator policy for model tools. Missing configuration is deliberately conservative:
//! private content stays within the explicitly trusted provider set.

/// The routed classifier rollout is independent of LLM account selection. Enabling it never
/// permits fixed-credential fallback when routing is unavailable.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClassifierRoutingConfig {
    pub enabled: bool,
}

impl ClassifierRoutingConfig {
    pub fn load_default() -> Result<Self, String> {
        match crate::CacheTtlTable::default_path() {
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
            crate::config_home::ConfigHome::Resolved { path, .. } => {
                match std::fs::read_to_string(&path) {
                    Ok(text) => Self::parse(&text),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        Ok(Self::default())
                    }
                    Err(error) => Err(format!(
                        "read classifier_routing from {}: {error}",
                        path.display()
                    )),
                }
            }
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let mut file: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&text)
            .map_err(|error| format!("parse classifier_routing.enabled: {error}"))?;
        match file.remove("classifier_routing") {
            None => Ok(Self::default()),
            Some(value) => serde_json::from_value(value)
                .map_err(|error| format!("classifier_routing.enabled: {error}")),
        }
    }
}

#[cfg(test)]
mod classifier_routing_config_tests {
    use super::ClassifierRoutingConfig;
    #[test]
    fn classifier_routing_is_explicit_and_defaults_off() {
        for text in [
            "{}",
            r#"{"classifier_routing":{}}"#,
            r#"{"classifier_routing":{"enabled":false}}"#,
        ] {
            assert!(!ClassifierRoutingConfig::parse(text).unwrap().enabled);
        }
        assert!(
            ClassifierRoutingConfig::parse(r#"{"classifier_routing":{"enabled":true}}"#)
                .unwrap()
                .enabled
        );
        for text in [
            r#"{"classifier_routing":null}"#,
            r#"{"classifier_routing":{"enabled":1}}"#,
            r#"{"classifier_routing":{"typo":true}}"#,
        ] {
            assert!(ClassifierRoutingConfig::parse(text)
                .unwrap_err()
                .contains("classifier_routing"));
        }
    }
}

/// How the new session storage makes its writes durable (`storage_sync` in broca.jsonc). Off by
/// default: on macOS each sync is a plain `fsync`, which survives a broca crash, kill or restart
/// but not a power loss or kernel panic before the drive empties its cache. `full_flush` makes
/// each sync a full flush of the drive's cache (`F_FULLFSYNC`) instead, about 4 ms each, for a
/// machine that can lose power. Other platforms sync fully either way (see
/// `broca_session_store::durable`).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageSyncConfig {
    pub full_flush: bool,
}

impl StorageSyncConfig {
    pub fn load_default() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Resolved { path, .. } => {
                match std::fs::read_to_string(&path) {
                    Ok(text) => Self::parse(&text),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        Ok(Self::default())
                    }
                    Err(error) => Err(format!(
                        "read storage_sync from {}: {error}",
                        path.display()
                    )),
                }
            }
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let mut file: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&text).map_err(|error| format!("parse storage_sync: {error}"))?;
        match file.remove("storage_sync") {
            None => Ok(Self::default()),
            Some(value) => {
                serde_json::from_value(value).map_err(|error| format!("storage_sync: {error}"))
            }
        }
    }
}

#[cfg(test)]
mod storage_sync_config_tests {
    use super::StorageSyncConfig;
    #[test]
    fn storage_sync_defaults_to_the_plain_sync_and_is_strict() {
        for text in [
            "{}",
            r#"{"storage_sync":{}}"#,
            r#"{"storage_sync":{"full_flush":false}}"#,
        ] {
            assert!(!StorageSyncConfig::parse(text).unwrap().full_flush);
        }
        assert!(
            StorageSyncConfig::parse(r#"{"storage_sync":{"full_flush":true}}"#)
                .unwrap()
                .full_flush
        );
        for text in [
            r#"{"storage_sync":{"full_flush":1}}"#,
            r#"{"storage_sync":{"fullflush":true}}"#,
        ] {
            assert!(StorageSyncConfig::parse(text)
                .unwrap_err()
                .contains("storage_sync"));
        }
    }
}

/// Operator-selected wire names, keyed by credential routing key rather than account identity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolNameAliasesConfig(pub std::collections::BTreeMap<String, ToolAliasRoute>);

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum ToolAliasRoute {
    Names {
        names: std::collections::BTreeMap<String, String>,
    },
    Prefix {
        prefix_all: String,
    },
}

impl ToolNameAliasesConfig {
    pub fn load_default() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
            crate::config_home::ConfigHome::Resolved { path, .. } => {
                match std::fs::read_to_string(&path) {
                    Ok(text) => Self::parse(&text),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        Ok(Self::default())
                    }
                    Err(error) => Err(format!(
                        "read tool_name_aliases from {}: {error}",
                        path.display()
                    )),
                }
            }
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let mut file: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&text)
            .map_err(|error| format!("parse tool_name_aliases: {error}"))?;
        match file.remove("tool_name_aliases") {
            None => Ok(Self::default()),
            Some(value) => serde_json::from_value(value)
                .map(Self)
                .map_err(|error| format!("tool_name_aliases: {error}")),
        }
    }

    pub fn for_tools(
        &self,
        route: &str,
        tools: &[broca_protocol::request::Tool],
    ) -> std::collections::BTreeMap<String, String> {
        tools
            .iter()
            .filter_map(|tool| match self.0.get(route)? {
                ToolAliasRoute::Names { names } => names
                    .get(&tool.name)
                    .map(|alias| (tool.name.clone(), alias.clone())),
                ToolAliasRoute::Prefix { prefix_all } => {
                    Some((tool.name.clone(), format!("{prefix_all}{}", tool.name)))
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tool_name_aliases_config_tests {
    use super::*;

    #[test]
    fn tool_name_aliases_config_shape_is_strict() {
        assert!(ToolNameAliasesConfig::parse("{}").unwrap().0.is_empty());
        for text in [
            r#"{"tool_name_aliases":null}"#,
            r#"{"tool_name_aliases":{"oauth:p":{"bash":"client_bash"}}}"#,
            r#"{"tool_name_aliases":{"oauth:p":{"names":{},"prefix_all":"client_"}}}"#,
            r#"{"tool_name_aliases":{"oauth:p":{"prefix_all":"client_","unknown":true}}}"#,
            r#"{"tool_name_aliases":{"oauth:p":{"names":{"bash":1}}}}"#,
        ] {
            assert!(
                ToolNameAliasesConfig::parse(text)
                    .unwrap_err()
                    .contains("tool_name_aliases"),
                "{text}"
            );
        }
        for text in [
            r#"{"tool_name_aliases":{"oauth:p":{"names":{"bash":"client_bash"}}}}"#,
            r#"{"tool_name_aliases":{"oauth:p":{"prefix_all":"client_"}}}"#,
        ] {
            assert_eq!(ToolNameAliasesConfig::parse(text).unwrap().0.len(), 1);
        }
    }
}

/// Opt-in writer policy for the `service-tier-queue/v1` representation.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceTierConfig {
    pub enabled: bool,
}

impl ServiceTierConfig {
    pub fn load_default() -> Result<Self, String> {
        match crate::CacheTtlTable::default_path() {
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
            crate::config_home::ConfigHome::Resolved { path, .. } => {
                match std::fs::read_to_string(&path) {
                    Ok(text) => Self::parse(&text),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        Ok(Self::default())
                    }
                    Err(error) => Err(format!(
                        "read service_tier from {}: {error}",
                        path.display()
                    )),
                }
            }
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let mut file: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&text)
            .map_err(|error| format!("parse service_tier.enabled: {error}"))?;
        match file.remove("service_tier") {
            None => Ok(Self::default()),
            Some(value) => serde_json::from_value(value)
                .map_err(|error| format!("service_tier.enabled: {error}")),
        }
    }
}

#[cfg(test)]
mod service_tier_config_tests {
    use super::ServiceTierConfig;

    #[test]
    fn service_tier_defaults_off_and_refuses_malformed_switches_by_name() {
        for text in [
            "{}",
            r#"{"service_tier":{}}"#,
            r#"{"service_tier":{"enabled":false}}"#,
        ] {
            assert!(!ServiceTierConfig::parse(text).unwrap().enabled);
        }
        assert!(
            ServiceTierConfig::parse(r#"{"service_tier":{"enabled":true}}"#)
                .unwrap()
                .enabled
        );
        for text in [
            r#"{"service_tier":true}"#,
            r#"{"service_tier":{"enabled":"yes"}}"#,
            r#"{"service_tier":null}"#,
            r#"{"service_tier":{"enable":true}}"#,
        ] {
            assert!(ServiceTierConfig::parse(text)
                .unwrap_err()
                .contains("service_tier.enabled"));
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LlmToolsConfig {
    pub advertise_tags: bool,
    pub image_references: bool,
    pub private_content_providers: Vec<String>,
}

impl Default for LlmToolsConfig {
    fn default() -> Self {
        Self {
            advertise_tags: false,
            image_references: false,
            private_content_providers: vec!["anthropic".into(), "anthropic-platform".into()],
        }
    }
}

impl LlmToolsConfig {
    pub fn load_default() -> Result<Self, String> {
        use crate::config_home::ConfigHome;
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            ConfigHome::Resolved { path, .. } => Self::load(&path),
            ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default())
            }
            Err(error) => {
                return Err(format!(
                    "read model-tool config {}: {error}",
                    path.display()
                ))
            }
        };
        #[derive(Default, serde::Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct Tags {
            advertise_tags: bool,
            image_references: bool,
        }
        #[derive(serde::Deserialize)]
        struct File {
            #[serde(default)]
            llm_tools: Tags,
            private_content_providers: Option<Vec<String>>,
        }
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(&text),
        );
        let file: File = serde_json::from_str(&text)
            .map_err(|error| format!("parse model-tool config {}: {error}", path.display()))?;
        let config = Self {
            advertise_tags: file.llm_tools.advertise_tags,
            image_references: file.llm_tools.image_references,
            private_content_providers: file
                .private_content_providers
                .unwrap_or_else(|| Self::default().private_content_providers),
        };
        if config.private_content_providers.len() > 16
            || config.private_content_providers.iter().any(|provider| {
                provider.is_empty()
                    || provider.len() > 64
                    || !provider
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            })
        {
            return Err("private_content_providers requires at most 16 provider ids, each 1-64 lowercase letters, digits or hyphens".into());
        }
        Ok(config)
    }
}

/// Boot-only reader-first rollout switch for compaction notice pricing and chain resets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionSignalsConfig {
    pub enabled: bool,
}

impl Default for CompactionSignalsConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl CompactionSignalsConfig {
    pub fn load_default() -> Result<Self, String> {
        use crate::config_home::ConfigHome;
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            ConfigHome::Resolved { path, .. } => match std::fs::read_to_string(&path) {
                Ok(text) => Self::parse(&text).map_err(|e| format!("{}: {e}", path.display())),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
                Err(e) => Err(format!(
                    "read compaction_signals from {}: {e}",
                    path.display()
                )),
            },
            ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        #[derive(serde::Deserialize)]
        struct File {
            #[serde(default = "enabled")]
            compaction_signals: bool,
        }
        fn enabled() -> bool {
            true
        }
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        serde_json::from_str::<File>(&text)
            .map(|file| Self {
                enabled: file.compaction_signals,
            })
            .map_err(|e| format!("parse compaction_signals: {e}"))
    }
}

#[cfg(test)]
mod compaction_signals_config_tests {
    use super::CompactionSignalsConfig as Config;
    #[test]
    fn default_on_and_explicit_off_are_strict_booleans() {
        for text in ["{}", r#"{"compaction_signals":true}"#] {
            assert!(Config::parse(text).unwrap().enabled);
        }
        assert!(
            !Config::parse("{/* rollback */ \"compaction_signals\":false,}")
                .unwrap()
                .enabled
        );
        for text in [
            r#"{"compaction_signals":null}"#,
            r#"{"compaction_signals":"false"}"#,
            r#"{"compaction_signals":0}"#,
        ] {
            assert!(Config::parse(text)
                .unwrap_err()
                .contains("compaction_signals"));
        }
    }
}

/// Top-level `broca.jsonc` keys that no longer do anything, each with what an operator should
/// know about it. Read only to tell the operator, never parsed: a leftover setting must not
/// stop broca from starting, and must not sit in the file looking as if it were in force.
const OBSOLETE_KEYS: &[(&str, &str)] = &[
    (
        "media",
        "media input follows the frozen model's capabilities",
    ),
    (
        "wal_archive_idle_retention_days",
        "sessions keep no logs to archive; the new storage seals history itself",
    ),
    (
        "session_conversion",
        "every session is in the new storage, so nothing is left to convert",
    ),
];

/// Warn, once per key, about each obsolete key the operator's `broca.jsonc` still sets.
pub fn warn_obsolete_keys() -> Result<(), String> {
    if let crate::config_home::ConfigHome::Resolved { path, .. } =
        crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
    {
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                for (key, why) in
                    obsolete_keys(&text).map_err(|error| format!("{}: {error}", path.display()))?
                {
                    tracing::warn!(target: "config", "broca.jsonc key '{key}' is obsolete and ignored; {why}");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("read config {}: {error}", path.display())),
        }
    }
    Ok(())
}

/// The obsolete keys `text` sets, with what to say about each.
fn obsolete_keys(text: &str) -> Result<Vec<(&'static str, &'static str)>, String> {
    let text = crate::cache_routing::strip_trailing_commas(
        &crate::cache_routing::strip_jsonc_comments(text),
    );
    let file: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&text).map_err(|error| format!("parse broca config: {error}"))?;
    Ok(OBSOLETE_KEYS
        .iter()
        .filter(|(key, _)| file.contains_key(*key))
        .copied()
        .collect())
}

#[cfg(test)]
mod obsolete_key_tests {
    use super::obsolete_keys;

    #[test]
    fn obsolete_keys_are_accepted_without_parsing_their_values() {
        assert!(obsolete_keys("{}").unwrap().is_empty());
        for (text, key) in [
            (r#"{"media":{"enabled":false}}"#, "media"),
            (r#"{"media":{"enabled":true,},}"#, "media"),
            (r#"{"media":{"enabeld":true}}"#, "media"),
            (r#"{"media":{"enabled":"yes"}}"#, "media"),
            (r#"{"media":null}"#, "media"),
            (r#"{"media":[]}"#, "media"),
            (
                r#"{"wal_archive_idle_retention_days":30}"#,
                "wal_archive_idle_retention_days",
            ),
            (
                r#"{"wal_archive_idle_retention_days":"thirty"}"#,
                "wal_archive_idle_retention_days",
            ),
        ] {
            let found = obsolete_keys(text).unwrap();
            assert_eq!(found.len(), 1, "{text}");
            assert_eq!(found[0].0, key, "{text}");
            // Other boot loaders must not reject a retired top-level key either.
            crate::account_routing::AccountSelectionConfig::parse(text).unwrap();
        }
    }
}

/// Physical recovery policy for overhaul logs. Legacy log policy is unaffected.
pub struct WalRecoveryConfig;

impl WalRecoveryConfig {
    pub fn parse(text: &str) -> Result<broca_wal::recovery::RecoveryPolicy, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let file: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| format!("parse wal_recovery: {e}"))?;
        match file.get("wal_recovery") {
            None => Ok(broca_wal::recovery::RecoveryPolicy::Repair),
            Some(value) if value == "repair" => Ok(broca_wal::recovery::RecoveryPolicy::Repair),
            Some(value) if value == "refuse" => Ok(broca_wal::recovery::RecoveryPolicy::Refuse),
            Some(value) => Err(format!(
                "wal_recovery accepts only \"repair\" or \"refuse\"; got {value}"
            )),
        }
    }
    pub fn load_default() -> Result<broca_wal::recovery::RecoveryPolicy, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Unresolvable => {
                Ok(broca_wal::recovery::RecoveryPolicy::Repair)
            }
            crate::config_home::ConfigHome::Resolved { path, .. } => {
                match std::fs::read_to_string(&path) {
                    Ok(text) => Self::parse(&text).map_err(|e| format!("{}: {e}", path.display())),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        Ok(broca_wal::recovery::RecoveryPolicy::Repair)
                    }
                    Err(e) => Err(format!("read wal_recovery from {}: {e}", path.display())),
                }
            }
        }
    }
}

#[cfg(test)]
mod wal_recovery_config_tests {
    use super::*;
    #[test]
    fn wal_recovery_config_defaults_and_refuses_unknown_values_by_name() {
        use broca_wal::recovery::RecoveryPolicy;
        assert_eq!(
            WalRecoveryConfig::parse("{}").unwrap(),
            RecoveryPolicy::Repair
        );
        assert_eq!(
            WalRecoveryConfig::parse("{\"wal_recovery\":\"repair\"}").unwrap(),
            RecoveryPolicy::Repair
        );
        assert_eq!(
            WalRecoveryConfig::parse("{\"wal_recovery\":\"refuse\"}").unwrap(),
            RecoveryPolicy::Refuse
        );
        for value in ["null", "true", "0", "\"repairs\"", "{}"] {
            assert!(
                WalRecoveryConfig::parse(&format!("{{\"wal_recovery\":{value}}}"))
                    .unwrap_err()
                    .contains("wal_recovery")
            );
        }
    }
}

/// The `anthropic_fallbacks` key of `broca.jsonc`: the operator switches for content-filter
/// fallback on Anthropic OAuth runs.
///
/// ```jsonc
/// {"anthropic_fallbacks": {
///     "server_side": false,
///     "refusal_resend": {"enabled": false, "models": {"claude-opus-5-5": "claude-opus-4-8"}}
/// }}
/// ```
///
/// Both are off unless an operator turns them on, so a build carrying the code sends the
/// requests it always did. `server_side` lets Anthropic hand a refusal to another model itself;
/// `refusal_resend.enabled` lets broca re-send a refused step to the model its `models` mapping
/// names. An absent mapping keeps the shipped defaults; an explicit mapping replaces them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AnthropicFallbackConfig {
    /// `anthropic_fallbacks.server_side`: ask Anthropic to hand a content-filter refusal to
    /// another model itself (`fallbacks: "default"` on an OAuth request for a model whose
    /// catalog row supports it).
    pub server_side: bool,
    /// The client-side switch and source-to-target model mapping, frozen only at admission.
    pub refusal_resend: crate::refusal_fallback_config::RefusalFallbackConfig,
}

impl AnthropicFallbackConfig {
    pub fn load_default() -> Result<Self, String> {
        use crate::config_home::ConfigHome;
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            ConfigHome::Resolved { path, .. } => Self::load(&path),
            ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text).map_err(|error| format!("{}: {error}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(format!(
                "read anthropic_fallbacks from {}: {error}",
                path.display()
            )),
        }
    }

    /// Parse the switches out of a whole `broca.jsonc`. The key is strict about its shape: an
    /// unknown member or a malformed value refuses boot rather than being read as off, because
    /// an operator who wrote the key meant to change behaviour and a silent "off" would hide the
    /// typo until a refusal surprised them in production.
    pub fn parse(text: &str) -> Result<Self, String> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fallbacks {
            #[serde(default)]
            server_side: bool,
            #[serde(default)]
            refusal_resend: Resend,
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Resend {
            #[serde(default)]
            enabled: bool,
            #[serde(default = "default_models")]
            models: std::collections::BTreeMap<String, String>,
        }
        impl Default for Resend {
            fn default() -> Self {
                Self {
                    enabled: false,
                    models: default_models(),
                }
            }
        }
        fn default_models() -> std::collections::BTreeMap<String, String> {
            crate::refusal_fallback_config::RefusalFallbackConfig::default()
                .mappings()
                .clone()
        }
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let mut file: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&text)
            .map_err(|error| format!("parse anthropic_fallbacks: {error}"))?;
        for legacy in [
            "refusal_fallback",
            "anthropic_refusal_fallback",
            "anthropic_server_side_fallback",
        ] {
            if file.contains_key(legacy) {
                return Err(format!(
                    "{legacy}: retired root key; configure anthropic_fallbacks instead"
                ));
            }
        }
        let Some(block) = file.remove("anthropic_fallbacks") else {
            return Ok(Self::default());
        };
        let fallbacks: Fallbacks = serde_json::from_value(block).map_err(|error| {
            format!("anthropic_fallbacks.refusal_resend or server_side: {error}")
        })?;
        // The nested object's absence must keep the shipped mapping just like an absent block.
        let resend = fallbacks.refusal_resend;
        Ok(Self {
            server_side: fallbacks.server_side,
            refusal_resend: crate::refusal_fallback_config::RefusalFallbackConfig::new(
                resend.models,
            )?
            .with_enabled(resend.enabled),
        })
    }
}

#[cfg(test)]
mod anthropic_fallback_config_tests {
    use super::AnthropicFallbackConfig;

    #[test]
    fn absent_key_leaves_both_layers_off() {
        for text in ["{}", "{ \"cache\": {} }", "// only a comment\n{}"] {
            assert_eq!(
                AnthropicFallbackConfig::parse(text).unwrap(),
                AnthropicFallbackConfig::default(),
                "{text}"
            );
        }
        let off = AnthropicFallbackConfig::default();
        assert!(!off.server_side);
        assert!(!off.refusal_resend.is_enabled());
        assert_eq!(off.refusal_resend.mappings().len(), 4);
        for source in [
            "claude-opus-5-5",
            "claude-opus-5",
            "claude-fable-5",
            "claude-fable-5-1",
        ] {
            assert_eq!(
                off.refusal_resend.target(source).unwrap().model,
                "claude-opus-4-8"
            );
        }
    }

    #[test]
    fn each_switch_opts_in_its_own_layer() {
        let server_side = AnthropicFallbackConfig::parse(
            "{\n  // staging\n  \"anthropic_fallbacks\": { \"server_side\": true },\n}",
        )
        .unwrap();
        assert!(server_side.server_side);
        assert!(
            !server_side.refusal_resend.is_enabled(),
            "the other switch stays off"
        );
        assert_eq!(server_side.refusal_resend.mappings().len(), 4);

        let refusal = AnthropicFallbackConfig::parse(
            "{\"anthropic_fallbacks\": {\"refusal_resend\": {\"enabled\": true}}}",
        )
        .unwrap();
        assert!(refusal.refusal_resend.is_enabled());
        assert!(!refusal.server_side, "the other switch stays off");

        let both = AnthropicFallbackConfig::parse(
            "{\"anthropic_fallbacks\": {\"server_side\": true, \"refusal_resend\": {\"enabled\": true}}}",
        )
        .unwrap();
        assert!(both.server_side && both.refusal_resend.is_enabled());
    }

    #[test]
    fn a_misspelt_or_wrongly_typed_switch_refuses() {
        for (text, named) in [
            (
                r#"{"anthropic_fallbacks": {"server_sides": true}}"#,
                "server_sides",
            ),
            (
                r#"{"anthropic_fallbacks": {"server_side": "true"}}"#,
                "server_side",
            ),
            (r#"{"anthropic_fallbacks": {"refusal": true}}"#, "refusal"),
            (
                r#"{"anthropic_fallbacks": {"refusal_resend": {"enable": true}}}"#,
                "enable",
            ),
            (
                r#"{"anthropic_fallbacks": {"refusal_resend": {"enabled": 1}}}"#,
                "refusal_resend",
            ),
            (
                r#"{"anthropic_fallbacks": {"refusal_resend": {"model": {}}}}"#,
                "model",
            ),
            (
                r#"{"anthropic_fallbacks": {"refusal_resend": true}}"#,
                "refusal_resend",
            ),
            (r#"{"anthropic_fallbacks": true}"#, "anthropic_fallbacks"),
            (r#"{"anthropic_fallbacks": null}"#, "anthropic_fallbacks"),
        ] {
            let error = AnthropicFallbackConfig::parse(text).unwrap_err();
            assert!(error.contains(named), "{text}: {error}");
        }
    }

    #[test]
    fn other_root_keys_are_ignored() {
        for text in [
            r#"{"cache": {}, "providers": {}}"#,
            r#"{"step_events_enabled": false}"#,
            r#"{"step_events_enabled": true}"#,
            r#"{"step_events_enabled": "ignored"}"#,
        ] {
            let switches = AnthropicFallbackConfig::parse(text).unwrap();
            assert!(!switches.server_side);
            assert!(!switches.refusal_resend.is_enabled());
        }
    }

    #[test]
    fn retired_root_keys_refuse_by_name() {
        for key in [
            "refusal_fallback",
            "anthropic_refusal_fallback",
            "anthropic_server_side_fallback",
        ] {
            let error =
                AnthropicFallbackConfig::parse(&format!(r#"{{"{key}": {{}}}}"#)).unwrap_err();
            assert!(error.contains(key), "{error}");
        }
    }
}

/// Root-level same-account wait cap loaded at boot. Consent is frozen at admission.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountLimitPauseConfig {
    pub max_same_account_wait: broca_core::SameAccountWait,
    pub obsolete_account_limit_pause: bool,
}

impl AccountLimitPauseConfig {
    pub fn load_default() -> Result<Self, String> {
        use crate::config_home::ConfigHome;
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            ConfigHome::Resolved { path, .. } => match std::fs::read_to_string(&path) {
                Ok(text) => {
                    Self::parse(&text).map_err(|error| format!("{}: {error}", path.display()))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
                Err(error) => Err(format!(
                    "read max_same_account_wait from {}: {error}",
                    path.display()
                )),
            },
            ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let file: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&text)
            .map_err(|error| format!("parse max_same_account_wait: {error}"))?;
        let setting = match file.get("max_same_account_wait") {
            None => broca_core::SameAccountWait::default(),
            Some(serde_json::Value::String(value)) if value == "max" => {
                broca_core::SameAccountWait::Max
            }
            Some(value) if value.as_u64().is_some() => {
                broca_core::SameAccountWait::Minutes(value.as_u64().unwrap())
            }
            Some(_) => return Err(
                "max_same_account_wait must be a non-negative integer number of minutes or \"max\""
                    .into(),
            ),
        };
        Ok(Self {
            max_same_account_wait: setting,
            obsolete_account_limit_pause: file.contains_key("account_limit_pause"),
        })
    }
}

#[cfg(test)]
mod account_limit_pause_config_tests {
    use super::AccountLimitPauseConfig;

    #[test]
    fn max_same_account_wait_defaults_and_typed_values() {
        for text in [
            "{}",
            r#"{"account_limit_pause":false}"#,
            r#"{"other":true}"#,
        ] {
            assert_eq!(
                AccountLimitPauseConfig::parse(text)
                    .unwrap()
                    .max_same_account_wait,
                broca_core::SameAccountWait::Minutes(15)
            );
        }
        for (value, expected) in [
            ("0", broca_core::SameAccountWait::Minutes(0)),
            ("42", broca_core::SameAccountWait::Minutes(42)),
            ("\"max\"", broca_core::SameAccountWait::Max),
        ] {
            assert_eq!(
                AccountLimitPauseConfig::parse(&format!(r#"{{"max_same_account_wait":{value}}}"#))
                    .unwrap()
                    .max_same_account_wait,
                expected
            );
        }
        for value in [
            "null", "true", "false", "-1", "1.5", "1.0", "\"15\"", "\"MAX\"", "{}", "[]",
        ] {
            assert!(AccountLimitPauseConfig::parse(&format!(
                r#"{{"max_same_account_wait":{value}}}"#
            ))
            .unwrap_err()
            .contains("max_same_account_wait"));
        }
    }

    #[test]
    fn obsolete_account_limit_pause_is_ignored_without_parsing_its_value() {
        assert!(
            !AccountLimitPauseConfig::parse("{}")
                .unwrap()
                .obsolete_account_limit_pause
        );
        for value in ["true", "false", "null", "1", "{}", "[]", "\"bad\""] {
            let config =
                AccountLimitPauseConfig::parse(&format!(r#"{{"account_limit_pause":{value}}}"#))
                    .unwrap();
            assert!(config.obsolete_account_limit_pause);
            assert_eq!(
                config.max_same_account_wait,
                broca_core::SameAccountWait::Minutes(15)
            );
        }
        let config = AccountLimitPauseConfig::parse(
            r#"{"account_limit_pause":true,"max_same_account_wait":0}"#,
        )
        .unwrap();
        assert!(!config.max_same_account_wait.enabled());
    }
}

/// Where a head's file-source import may read from: `import.roots` in `broca.jsonc`, each an
/// absolute folder. Absent, broca's own import folder under its state root is the only root,
/// the folder Thalamus's `session.export` writes to. A source is read only under a root,
/// resolved after symlinks, and deleted after its import only inside broca's own folder. Loaded
/// once at boot and handed to the new storage's runtime (`StoreRuntime::with_import_roots`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportConfig {
    pub roots: Option<Vec<std::path::PathBuf>>,
}

impl ImportConfig {
    pub fn load_default() -> Result<Self, String> {
        use crate::config_home::ConfigHome;
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            ConfigHome::Resolved { path, .. } => match std::fs::read_to_string(&path) {
                Ok(text) => Self::parse(&text).map_err(|e| format!("{}: {e}", path.display())),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
                Err(e) => Err(format!("read import from {}: {e}", path.display())),
            },
            ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        #[derive(serde::Deserialize)]
        struct File {
            #[serde(default)]
            import: Option<Section>,
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Section {
            #[serde(default)]
            roots: Option<Vec<std::path::PathBuf>>,
        }
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let file = serde_json::from_str::<File>(&text).map_err(|e| format!("parse import: {e}"))?;
        let roots = file.import.and_then(|section| section.roots);
        if let Some(root) = roots.iter().flatten().find(|root| !root.is_absolute()) {
            return Err(format!(
                "import.roots holds {}, which is not an absolute folder",
                root.display()
            ));
        }
        Ok(Self { roots })
    }
}

/// How the new session storage is configured: `session_storage` in `broca.jsonc`.
///
/// `deployment_key_id` names the deployment key encrypted sessions are opened with, 1 when
/// absent. The key itself is the vault record `opaque:broca-deployment:<id>` (see `store_key`).
/// Keys form a ring: a rotation adds a record under a new id and names it here, and never
/// rewrites an existing record.
///
/// `encryption`, `"on"` (the default) or `"off"`, chooses the mode of the sessions created from
/// now on: encrypted, or plain, stored without encryption and served without the deployment key.
/// A session keeps the mode it was created in, so changing it leaves existing sessions as they
/// are, and a deployment can hold both kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionStorageConfig {
    pub deployment_key_id: u16,
    pub encryption: Encryption,
}

/// `session_storage.encryption`: whether the sessions the new storage creates are encrypted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encryption {
    #[default]
    On,
    Off,
}

impl Default for SessionStorageConfig {
    fn default() -> Self {
        Self {
            deployment_key_id: crate::store_key::DEFAULT_KEY_ID,
            encryption: Encryption::On,
        }
    }
}

impl SessionStorageConfig {
    /// The mode of the sessions the new storage creates.
    pub fn new_sessions(&self) -> broca_session_store::keys::Cipher {
        match self.encryption {
            Encryption::On => broca_session_store::keys::Cipher::XChaCha20Poly1305,
            Encryption::Off => broca_session_store::keys::Cipher::None,
        }
    }

    pub fn load_default() -> Result<Self, String> {
        use crate::config_home::ConfigHome;
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            ConfigHome::Resolved { path, .. } => match std::fs::read_to_string(&path) {
                Ok(text) => Self::parse(&text).map_err(|e| format!("{}: {e}", path.display())),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
                Err(e) => Err(format!("read session_storage from {}: {e}", path.display())),
            },
            ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        #[derive(serde::Deserialize)]
        struct File {
            #[serde(default)]
            session_storage: Option<Section>,
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Section {
            #[serde(default)]
            deployment_key_id: Option<u16>,
            #[serde(default)]
            encryption: Option<Encryption>,
        }
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let file = serde_json::from_str::<File>(&text)
            .map_err(|e| format!("parse session_storage: {e}"))?;
        let section = file.session_storage;
        let defaults = Self::default();
        Ok(Self {
            deployment_key_id: section
                .as_ref()
                .and_then(|section| section.deployment_key_id)
                .unwrap_or(defaults.deployment_key_id),
            encryption: section
                .and_then(|section| section.encryption)
                .unwrap_or(defaults.encryption),
        })
    }
}

#[cfg(test)]
mod session_storage_config_tests {
    use super::{Encryption, SessionStorageConfig};
    use broca_session_store::keys::Cipher;

    #[test]
    fn the_deployment_key_id_defaults_to_1_and_is_named_by_session_storage() {
        assert_eq!(
            SessionStorageConfig::parse("{}").unwrap(),
            SessionStorageConfig {
                deployment_key_id: 1,
                encryption: Encryption::On,
            }
        );
        let config = SessionStorageConfig::parse(
            r#"{"session_storage": {"deployment_key_id": 2,}, // the rotated key
            }"#,
        )
        .unwrap();
        assert_eq!(config.deployment_key_id, 2);
        assert!(SessionStorageConfig::parse(r#"{"session_storage": {"key_id": 2}}"#).is_err());
        assert!(
            SessionStorageConfig::parse(r#"{"session_storage": {"deployment_key_id": -1}}"#)
                .is_err()
        );
    }

    /// Encryption is on unless `session_storage.encryption` turns it off, which makes the
    /// sessions created from then on plain. Any other value refuses the configuration.
    #[test]
    fn encryption_is_on_unless_session_storage_turns_it_off() {
        let default = SessionStorageConfig::parse(r#"{"session_storage": {}}"#).unwrap();
        assert_eq!(default.encryption, Encryption::On);
        assert_eq!(default.new_sessions(), Cipher::XChaCha20Poly1305);
        let on =
            SessionStorageConfig::parse(r#"{"session_storage": {"encryption": "on"}}"#).unwrap();
        assert_eq!(on.new_sessions(), Cipher::XChaCha20Poly1305);
        let off = SessionStorageConfig::parse(
            r#"{"session_storage": {"deployment_key_id": 3, "encryption": "off"}}"#,
        )
        .unwrap();
        assert_eq!(
            off,
            SessionStorageConfig {
                deployment_key_id: 3,
                encryption: Encryption::Off,
            }
        );
        assert_eq!(off.new_sessions(), Cipher::None);
        for refused in [r#""none""#, r#""OFF""#, "false", "0"] {
            assert!(
                SessionStorageConfig::parse(&format!(
                    r#"{{"session_storage": {{"encryption": {refused}}}}}"#
                ))
                .is_err(),
                "{refused}"
            );
        }
    }
}

#[cfg(test)]
mod import_config_tests {
    use super::ImportConfig;

    #[test]
    fn import_roots_default_to_brocas_import_folder() {
        assert_eq!(ImportConfig::parse("{}").unwrap(), ImportConfig::default());
        let config = ImportConfig::parse(
            r#"{"import": {"roots": ["/srv/heads",],}, // a comment
            }"#,
        )
        .unwrap();
        assert_eq!(config.roots, Some(vec!["/srv/heads".into()]));
        assert!(ImportConfig::parse(r#"{"import": {"roots": ["heads"]}}"#).is_err());
        assert!(ImportConfig::parse(r#"{"import": {"root": ["/x"]}}"#).is_err());
    }
}
