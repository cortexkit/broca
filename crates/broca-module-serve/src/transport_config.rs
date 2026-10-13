//! Runtime transport policy, deliberately separate from the admitted render configuration.
use serde::Deserialize;
use std::{path::Path, sync::Arc};

#[derive(Default, Deserialize)]
struct Config {
    #[serde(default)]
    chatgpt_force_https: bool,
}

fn load(path: &Path) -> Result<bool, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("read transport config {}: {error}", path.display())),
    };
    let stripped = crate::cache_routing::strip_trailing_commas(
        &crate::cache_routing::strip_jsonc_comments(&text),
    );
    let config: Config = serde_json::from_str(&stripped)
        .map_err(|error| format!("parse transport config {}: {error}", path.display()))?;
    Ok(config.chatgpt_force_https)
}

/// Validate at boot; read again at each attempt so an outage switch takes effect on active runs.
pub fn validate_default() -> Result<(), String> {
    match crate::CacheTtlTable::default_path() {
        crate::config_home::ConfigHome::Resolved { path, .. } => load(&path).map(|_| ()),
        crate::config_home::ConfigHome::Unresolvable => Ok(()),
    }
}

/// Invalid edits made after boot fail safe to HTTPS without altering any rendered bytes.
pub fn default_policy() -> Arc<dyn Fn() -> bool + Send + Sync> {
    let resolved = crate::CacheTtlTable::default_path();
    Arc::new(move || {
        match &resolved {
        crate::config_home::ConfigHome::Resolved { path, .. } => load(path).unwrap_or_else(|error| {
            tracing::warn!(target: "provider", %error, "invalid runtime transport policy; forcing ChatGPT HTTPS");
            true
        }),
        crate::config_home::ConfigHome::Unresolvable => false,
    }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn https_break_glass_defaults_off_and_reads_edits() {
        let root = broca_test_support::scratch_root().join("transport-config");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("broca.jsonc");
        assert!(!load(&path).unwrap());
        std::fs::write(&path, "{ // outage\n \"chatgpt_force_https\": true, }").unwrap();
        assert!(load(&path).unwrap());
        std::fs::write(&path, "{}").unwrap();
        assert!(!load(&path).unwrap());
        std::fs::write(&path, r#"{"chatgpt_force_https":"yes"}"#).unwrap();
        assert!(load(&path).is_err());
    }
}
