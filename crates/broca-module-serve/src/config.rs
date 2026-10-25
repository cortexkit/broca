//! Operator policy for model tools. Missing configuration is deliberately conservative:
//! private content stays within the explicitly trusted provider set.

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LlmToolsConfig {
    pub advertise_tags: bool,
    pub private_content_providers: Vec<String>,
}

impl Default for LlmToolsConfig {
    fn default() -> Self {
        Self {
            advertise_tags: false,
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
