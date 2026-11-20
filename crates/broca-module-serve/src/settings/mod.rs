//! broca's settings: the top-level object of `broca.jsonc`, read once at start.
//!
//! Every component the file shapes reads its own section from this one document instead of
//! reading the file itself. So the document can come from somewhere other than the file, such
//! as the daemon's managed settings, without any reader changing. A reader that still parses
//! JSON text takes [`Settings::text`]: the document as plain JSON, which the readers' JSONC
//! parsing accepts unchanged.
//!
//! The document is always in the shapes of broca's settings schema (`schema.json`). A file
//! written in a shape broca accepted before is converted as it is read ([`convert`]), and the
//! conversion's notes are logged at start.

use serde_json::{Map, Value};

pub mod convert;
pub mod gate;
pub mod review;
pub mod rewrite;

use crate::config_home::ConfigHome;

/// The settings document and where it came from.
#[derive(Clone, Debug, PartialEq)]
pub struct Settings {
    root: Map<String, Value>,
    /// `root` as plain JSON.
    text: String,
    /// What converting the document from older shapes changed, one line per key.
    notes: Vec<String>,
    /// Where the document came from, named in refusals: a file path, or `broca.jsonc` for text
    /// handed in directly.
    source: String,
}

/// The empty document: every section keeps its default.
impl Default for Settings {
    fn default() -> Self {
        Self::from_root(Map::new(), String::new())
    }
}

impl Settings {
    /// `broca.jsonc` at its resolved path (`$BROCA_CONFIG_PATH`, else the shared config home).
    /// An absent file, or a config home that can't be resolved, is an empty document, so every
    /// section keeps its default. A file that can't be read or parsed refuses start.
    pub fn read_file() -> Result<Self, String> {
        let path = match crate::config_home::config_file(
            crate::cache_routing::CONFIG_PATH_ENV,
            "broca.jsonc",
        ) {
            ConfigHome::Resolved { path, .. } => path,
            ConfigHome::Unresolvable => return Ok(Self::default()),
        };
        Self::read(&path)
    }

    /// The document in the file at `path`; an absent file is an empty document.
    pub fn read(path: &std::path::Path) -> Result<Self, String> {
        let source = path.display().to_string();
        match std::fs::read(path) {
            Ok(bytes) => {
                let text = String::from_utf8(bytes)
                    .map_err(|error| format!("read broca config {source} as utf-8: {error}"))?;
                Self::parse_from(&text, source)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::from_root(Map::new(), source))
            }
            Err(error) => Err(format!("read broca config {source}: {error}")),
        }
    }

    /// The document `text` holds: JSON with comments and trailing commas, as `broca.jsonc` is
    /// written.
    pub fn parse(text: &str) -> Result<Self, String> {
        Self::parse_from(text, "broca.jsonc".into())
    }

    fn parse_from(text: &str, source: String) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let root = serde_json::from_str(&text)
            .map_err(|error| format!("parse broca config {source}: {error}"))?;
        let converted =
            convert::convert(&root).map_err(|error| format!("broca config {source}: {error}"))?;
        let mut settings = Self::from_root(converted.document, source);
        settings.notes = converted.notes;
        Ok(settings)
    }

    /// A view the daemon delivered, already in the schema's shapes: not converted. A view that
    /// isn't an object is read as the empty document; the schema never admits one.
    pub fn from_view(view: &Value, source: &str) -> Self {
        let root = view.as_object().cloned().unwrap_or_default();
        Self::from_root(root, source.into())
    }

    fn from_root(root: Map<String, Value>, source: String) -> Self {
        let text = Value::Object(root.clone()).to_string();
        Self {
            root,
            text,
            notes: Vec::new(),
            source,
        }
    }

    /// Log what converting the document from older shapes changed, so its owner can update the
    /// file.
    pub fn log_conversion(&self) {
        for note in &self.notes {
            tracing::warn!(target: "config", "broca.jsonc in an older shape: {note}");
        }
    }

    /// What converting the document from older shapes changed, one line per key.
    pub fn conversion_notes(&self) -> &[String] {
        &self.notes
    }

    /// The document as plain JSON text.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The value of the top-level `key`, when the document sets it.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.root.get(key)
    }

    /// The top-level section `key` read as `T`; its default when the document doesn't set it.
    pub fn section<T>(&self, key: &str) -> Result<T, String>
    where
        T: serde::de::DeserializeOwned + Default,
    {
        match self.root.get(key) {
            None => Ok(T::default()),
            Some(value) => {
                serde_json::from_value(value.clone()).map_err(|error| format!("{key}: {error}"))
            }
        }
    }

    /// The whole document read as `T`, for a reader that takes several top-level keys at once.
    pub fn whole<T: serde::de::DeserializeOwned>(&self) -> Result<T, String> {
        serde_json::from_value(Value::Object(self.root.clone()))
            .map_err(|error| format!("parse broca config {}: {error}", self.source))
    }

    /// The document's top-level keys, in the order the document lists them.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.root.keys().map(String::as_str)
    }

    /// Where the document came from, as refusals name it.
    pub fn source(&self) -> &str {
        &self.source
    }
}

/// The schema broca declares to the daemon's managed settings ([`gate`]). Only builds with the
/// `settings-gate` switch declare it: several keys take a new shape or meaning in it
/// (`max_same_account_wait`, `tool_name_aliases`, `cache.provider_ttl`, `billing.charge_basis`,
/// `context_management`, `import.roots`, `account_selection.routing_module`), and declaring it
/// would refuse every `broca.jsonc` written in today's shapes until the daemon can convert an
/// old file.
const SCHEMA: &str = include_str!("schema.json");

/// broca's settings schema, as `config.declare` proposes it to the daemon.
pub(crate) fn schema_document() -> &'static Value {
    static DOCUMENT: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
    DOCUMENT.get_or_init(|| serde_json::from_str(SCHEMA).expect("broca's settings schema is JSON"))
}

/// broca's settings schema, checked. Its tests prove the shipped schema is valid.
pub fn schema() -> &'static subc_config::Schema {
    static SCHEMA_CHECKED: std::sync::OnceLock<subc_config::Schema> = std::sync::OnceLock::new();
    SCHEMA_CHECKED.get_or_init(|| {
        subc_config::Schema::new(schema_document().clone())
            .expect("broca's settings schema is valid")
    })
}

#[cfg(test)]
mod schema_tests;

#[cfg(test)]
mod tests {
    use super::Settings;

    #[test]
    fn a_section_keeps_its_default_unless_the_document_sets_it() {
        let settings = Settings::parse(
            r#"{
                // A comment, and a trailing comma, as broca.jsonc allows.
                "storage_sync": {"full_flush": true},
            }"#,
        )
        .unwrap();
        let set: serde_json::Value = settings.section("storage_sync").unwrap();
        assert_eq!(set["full_flush"], true);
        let unset: Option<u8> = settings.section("import").unwrap();
        assert_eq!(unset, None);
        assert_eq!(settings.keys().collect::<Vec<_>>(), ["storage_sync"]);
    }

    #[test]
    fn an_absent_file_is_an_empty_document_and_a_broken_one_refuses() {
        let dir = broca_test_support::scratch_root().join(format!(
            "settings-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let absent = Settings::read(&dir.join("broca.jsonc")).unwrap();
        assert_eq!(absent.keys().count(), 0);
        std::fs::write(dir.join("broken.jsonc"), "{ not json").unwrap();
        let refused = Settings::read(&dir.join("broken.jsonc")).unwrap_err();
        assert!(refused.contains("broken.jsonc"), "{refused}");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
