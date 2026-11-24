//! Runtime transport policy, deliberately separate from the admitted render configuration.
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

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

/// The settings file broca read at boot under a daemon without the settings gate.
static FILE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Validates `path`, the settings file boot read every other setting from, and makes each attempt
/// read the switch from it again, so an outage switch takes effect on active runs. The file
/// must be the same one: boot moves the settings out of broca's old folder, and the copy left
/// there no longer reaches this build.
pub fn use_file(path: &Path) -> Result<(), String> {
    load(path)?;
    let _ = FILE.set(path.to_owned());
    Ok(())
}

/// The value the daemon's settings give `chatgpt_force_https`, once broca runs on them
/// (`settings::gate`). Unset, each attempt reads the file instead.
static MANAGED: std::sync::OnceLock<std::sync::atomic::AtomicBool> = std::sync::OnceLock::new();

/// Records `force_https` from the daemon's settings, for every attempt from now on.
pub fn set_managed(force_https: bool) {
    MANAGED
        .get_or_init(|| std::sync::atomic::AtomicBool::new(force_https))
        .store(force_https, std::sync::atomic::Ordering::Relaxed);
}

/// Invalid edits made after boot fail safe to HTTPS without altering any rendered bytes.
pub fn default_policy() -> Arc<dyn Fn() -> bool + Send + Sync> {
    Arc::new(move || {
        if let Some(managed) = MANAGED.get() {
            return managed.load(std::sync::atomic::Ordering::Relaxed);
        }
        file_policy()
    })
}

/// The switch as the boot settings file now says. With no file recorded, nothing configured it.
fn file_policy() -> bool {
    let Some(path) = FILE.get() else {
        return false;
    };
    load(path).unwrap_or_else(|error| {
        tracing::warn!(target: "provider", %error, "invalid runtime transport policy; forcing ChatGPT HTTPS");
        true
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

    /// Without the settings gate, boot reads `<config home>/cortexkit/broca.jsonc`; the switch
    /// must come from that file, not from the old copy in `<config home>/cortexkit/broca/`.
    #[test]
    fn ungated_https_switch_follows_the_file_boot_read() {
        let home = broca_test_support::scratch_root()
            .join(format!("transport-config-boot-file-{}", std::process::id()));
        let path = home.join("cortexkit/broca.jsonc");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, r#"{"chatgpt_force_https": true}"#).unwrap();
        use_file(&path).unwrap();
        assert!(file_policy(), "the switch set in the boot file was ignored");
        std::fs::write(&path, "{}").unwrap();
        assert!(
            !file_policy(),
            "an edit to the boot file did not reach the next attempt"
        );
    }
}
