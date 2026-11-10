//! The production transport for the coordinator's best-effort step events.

pub use broca_module::step_events::*;

pub fn load_config() -> Result<Config, String> {
    let path =
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Unresolvable => return Ok(Config::default()),
            crate::config_home::ConfigHome::Resolved { path, .. } => path,
        };
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            parse_config(&text).map_err(|e| format!("step events config {}: {e}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(format!("read step events config {}: {e}", path.display())),
    }
}

fn parse_config(text: &str) -> Result<Config, String> {
    let text = crate::cache_routing::strip_trailing_commas(
        &crate::cache_routing::strip_jsonc_comments(text),
    );
    let config: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&text).map_err(|e| e.to_string())?;
    match config.get("step_events_enabled") {
        None => Ok(Config::default()),
        Some(value) => value
            .as_bool()
            .map(|step_events_enabled| Config {
                step_events_enabled,
            })
            .ok_or_else(|| "step_events_enabled must be a boolean".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_events_config_is_opt_in_and_boolean() {
        assert!(!parse_config("{}").unwrap().step_events_enabled);
        assert!(
            !parse_config(r#"{"step_events_enabled":false}"#)
                .unwrap()
                .step_events_enabled
        );
        assert!(
            parse_config("{ // live hints\n\"step_events_enabled\":true,}")
                .unwrap()
                .step_events_enabled
        );
        for value in ["null", "1", "\"true\""] {
            assert!(parse_config(&format!(r#"{{"step_events_enabled":{value}}}"#)).is_err());
        }
    }
}
