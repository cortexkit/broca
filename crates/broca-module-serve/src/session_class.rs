//! Boot configuration for backup placement. Existing WAL locations remain authoritative.
use std::{io, path::Path};

#[derive(Debug, PartialEq, Eq)]
pub struct BackupClasses(pub Vec<String>);
impl Default for BackupClasses {
    fn default() -> Self {
        Self(vec!["head".into()])
    }
}
impl BackupClasses {
    pub fn load_default_path() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Resolved { path, .. } => Self::load(&path),
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }
    fn load(path: &Path) -> Result<Self, String> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text)
                .map_err(|error| format!("backup_classes in {}: {error}", path.display())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(format!(
                "read backup_classes from {}: {error}",
                path.display()
            )),
        }
    }
    fn parse(text: &str) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&text).map_err(|error| error.to_string())?;
        let Some(value) = object.get("backup_classes") else {
            return Ok(Self::default());
        };
        let classes: Vec<String> = serde_json::from_value(value.clone())
            .map_err(|_| "backup_classes must be an array of session_class strings")?;
        for class in &classes {
            broca_protocol::request::validate_session_class(class)
                .map_err(|error| format!("backup_classes: {error}"))?;
        }
        Ok(Self(classes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backup_classes_config_defaults_and_validates() {
        assert_eq!(
            BackupClasses::parse("{}").unwrap(),
            BackupClasses::default()
        );
        assert_eq!(
            BackupClasses::parse(r#"{"backup_classes":[]}"#).unwrap().0,
            Vec::<String>::new()
        );
        assert_eq!(
            BackupClasses::parse("{ // labels\n\"backup_classes\": [\"head\", \"saved-1\",],}")
                .unwrap()
                .0,
            ["head", "saved-1"]
        );
        for value in [
            "null",
            "false",
            "1",
            "\"head\"",
            "[null]",
            "[\"HEAD\"]",
            "[\"\"]",
        ] {
            assert!(
                BackupClasses::parse(&format!("{{\"backup_classes\":{value}}}"))
                    .unwrap_err()
                    .contains("backup_classes")
            );
        }
    }
}
