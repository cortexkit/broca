//! Operator policy for model tools. Missing configuration is deliberately conservative:
//! private content stays within the explicitly trusted provider set.

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

/// The `state_format` key of `broca.jsonc`: whether a NEW state root (missing, empty, or holding
/// only `state-format.tmp`) is created in the data overhaul's `broca-state/2` format. Absent
/// means today's format. Nothing enforces who sets it: it is meant for tests and staging, and
/// production configs leave it out until the cutover build. A root that already holds state keeps the format it has whatever
/// this says, so the key never converts or reinterprets existing state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateFormatConfig {
    /// `"state_format": 2` was set.
    pub overhaul_opt_in: bool,
}

impl StateFormatConfig {
    /// The only value the key accepts.
    pub const OVERHAUL: u64 = 2;

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
                "read state_format from {}: {error}",
                path.display()
            )),
        }
    }

    /// Parse the key out of a whole `broca.jsonc`. Any value other than 2 refuses boot: a
    /// misspelt or wrongly typed opt-in must not silently start an operator's staging root in
    /// today's format, nor anything else be read as an opt-in.
    pub fn parse(text: &str) -> Result<Self, String> {
        #[derive(serde::Deserialize)]
        struct File {
            #[serde(default)]
            state_format: Option<serde_json::Value>,
        }
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let file: File =
            serde_json::from_str(&text).map_err(|error| format!("parse state_format: {error}"))?;
        match file.state_format {
            None => Ok(Self::default()),
            Some(value) if value.as_u64() == Some(Self::OVERHAUL) => Ok(Self {
                overhaul_opt_in: true,
            }),
            Some(value) => Err(format!(
                "state_format accepts only {} (create new state roots as broca-state/2); got \
                 {value}. Remove the key to keep today's format.",
                Self::OVERHAUL
            )),
        }
    }
}

/// The `anthropic_fallbacks` key of `broca.jsonc`: the operator switches for content-filter
/// fallback on Anthropic OAuth runs.
///
/// ```jsonc
/// {"anthropic_fallbacks": {"server_side": true, "refusal": true}}
/// ```
///
/// Both are off unless an operator turns them on, so a build carrying the code sends the
/// requests it always did. `server_side` lets Anthropic hand a refusal to another model itself;
/// `refusal` lets broca re-send a refused step to the model its `refusal_fallback` mapping
/// names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AnthropicFallbackSwitches {
    /// `anthropic_fallbacks.server_side`: ask Anthropic to hand a content-filter refusal to
    /// another model itself (`fallbacks: "default"` on an OAuth request for a model whose
    /// catalog row supports it).
    pub server_side: bool,
    /// `anthropic_fallbacks.refusal`: allow a refused step to be re-sent to the model the
    /// `refusal_fallback` section maps it to.
    pub refusal: bool,
}

impl AnthropicFallbackSwitches {
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
    /// unknown member or a non-boolean value refuses boot rather than being read as off, because
    /// an operator who wrote the key meant to change behaviour and a silent "off" would hide the
    /// typo until a refusal surprised them in production.
    pub fn parse(text: &str) -> Result<Self, String> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Switches {
            #[serde(default)]
            server_side: bool,
            #[serde(default)]
            refusal: bool,
        }
        #[derive(serde::Deserialize)]
        struct File {
            #[serde(default)]
            anthropic_fallbacks: Option<Switches>,
        }
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let file: File = serde_json::from_str(&text)
            .map_err(|error| format!("parse anthropic_fallbacks: {error}"))?;
        Ok(match file.anthropic_fallbacks {
            None => Self::default(),
            Some(switches) => Self {
                server_side: switches.server_side,
                refusal: switches.refusal,
            },
        })
    }
}

#[cfg(test)]
mod anthropic_fallback_switch_tests {
    use super::AnthropicFallbackSwitches;

    #[test]
    fn absent_key_leaves_both_layers_off() {
        for text in ["{}", "{ \"cache\": {} }", "// only a comment\n{}"] {
            assert_eq!(
                AnthropicFallbackSwitches::parse(text).unwrap(),
                AnthropicFallbackSwitches::default(),
                "{text}"
            );
        }
        let off = AnthropicFallbackSwitches::default();
        assert!(!off.server_side);
        assert!(!off.refusal);
    }

    #[test]
    fn each_switch_opts_in_its_own_layer() {
        let server_side = AnthropicFallbackSwitches::parse(
            "{\n  // staging\n  \"anthropic_fallbacks\": { \"server_side\": true },\n}",
        )
        .unwrap();
        assert!(server_side.server_side);
        assert!(!server_side.refusal, "the other switch stays off");

        let refusal =
            AnthropicFallbackSwitches::parse("{\"anthropic_fallbacks\": {\"refusal\": true}}")
                .unwrap();
        assert!(refusal.refusal);
        assert!(!refusal.server_side, "the other switch stays off");

        let both = AnthropicFallbackSwitches::parse(
            "{\"anthropic_fallbacks\": {\"server_side\": true, \"refusal\": true}}",
        )
        .unwrap();
        assert!(both.server_side && both.refusal);
    }

    #[test]
    fn a_misspelt_or_wrongly_typed_switch_refuses() {
        for text in [
            "{\"anthropic_fallbacks\": {\"server_sides\": true}}",
            "{\"anthropic_fallbacks\": {\"server_side\": \"true\"}}",
            "{\"anthropic_fallbacks\": {\"refusals\": true}}",
            "{\"anthropic_fallbacks\": {\"refusal\": 1}}",
            "{\"anthropic_fallbacks\": true}",
        ] {
            assert!(
                AnthropicFallbackSwitches::parse(text).is_err(),
                "{text} must not be read as a switch"
            );
        }
    }

    #[test]
    fn other_root_keys_are_ignored() {
        // This loader owns one block; the file's other sections, including the retired root
        // boolean the refusal mapping used to read, belong to other loaders. A file carrying
        // them still reads both switches as off rather than failing here.
        let switches = AnthropicFallbackSwitches::parse(
            "{\"anthropic_refusal_fallback\": true, \"refusal_fallback\": {\"a\": \"b\"}}",
        )
        .unwrap();
        assert!(!switches.server_side);
        assert!(!switches.refusal);
    }
}

#[cfg(test)]
mod state_format_config_tests {
    use super::StateFormatConfig;

    #[test]
    fn absent_key_keeps_todays_format() {
        for text in ["{}", "{ \"llm_tools\": {} }", "// only a comment\n{}"] {
            assert_eq!(
                StateFormatConfig::parse(text).unwrap(),
                StateFormatConfig::default(),
                "{text}"
            );
        }
    }

    #[test]
    fn two_opts_in() {
        let config =
            StateFormatConfig::parse("{\n  // staging\n  \"state_format\": 2,\n}").unwrap();
        assert!(config.overhaul_opt_in);
    }

    #[test]
    fn any_other_value_refuses() {
        for value in ["1", "3", "\"2\"", "2.5", "true", "-2", "{}"] {
            let error = StateFormatConfig::parse(&format!("{{\"state_format\": {value}}}"))
                .expect_err(value);
            assert!(error.contains("state_format"), "{error}");
        }
    }

    #[test]
    fn a_missing_file_keeps_todays_format() {
        let path = broca_test_support::scratch_root().join(format!(
            "broca-state-format-config-missing-{}.jsonc",
            std::process::id()
        ));
        assert_eq!(
            StateFormatConfig::load(&path).unwrap(),
            StateFormatConfig::default()
        );
    }
}
