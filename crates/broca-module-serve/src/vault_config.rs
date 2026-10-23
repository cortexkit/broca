//! Vault credential addressing for the daemon, and the capability-handle map kept for the
//! developer tools that still read credentials outside the daemon.
//!
//! ## The daemon reads by credential id only
//! The broca daemon never loads the handle file. Every credential it serves is read with
//! `credential.get_scoped`, addressed by vault credential id and authorized by the read grant
//! an operator gave this module's supervised principal (`reserved:broca`). The vault id is the
//! routing key itself unless `vault.credential_ids` in `broca.jsonc` maps it:
//! ```jsonc
//! { "vault": { "credential_ids": { "apikey:synthetic": "apikey:synthetic:main" } } }
//! ```
//! That map is ordinary configuration because a credential id is not a secret. See
//! [`VaultAddressing`]. A scoped `not_found` is not retried any other way: a fallback would
//! hide exactly the misconfiguration the operator needs to see.
//!
//! ## The handle file (developer tools only)
//! [`VaultHandles`] maps each CREDENTIAL ID to an opaque `ckh_` handle. The handles are BEARER SECRETS — the file is expected at mode 0600 and the
//! handles are never logged (only the credential ids and a presence flag are observable).
//!
//! Dev-tools location: `~/.config/cortexkit/broca/dev-tools-handles.json`, overridable with
//! `BROCA_DEVTOOLS_HANDLES`, loaded through [`load_devtools_handle`]. The daemon's legacy
//! `vault-handles.json` is not used by dev tools. A dedicated single-purpose
//! secrets file (not folded into a general config file) keeps the secret-bearing surface
//! isolated with unambiguous permissions.
//!
//! Format (keyed by `credential_id` = `<method>:<provider>[:<account>]` — a provider may now
//! have several credentials, e.g. an api key AND an oauth token):
//! ```json
//! { "handles": { "oauth:anthropic": "ckh_...", "antigravity:google": "ckh_...",
//!                "apikey:deepseek": "ckh_...", "chatgpt:openai": "ckh_..." } }
//! ```
//! The live `*_live.rs` tests and the `examples/` probes look a handle up here by the same
//! [`CredentialId`] a run resolves (see `auth_routing`).

use std::collections::{hash_map::Entry, HashMap};
use std::path::{Path, PathBuf};

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::auth_routing::CredentialId;

/// The env var that overrides the default handle-map path.
pub const HANDLES_PATH_ENV: &str = "BROCA_VAULT_HANDLES_PATH";

// This deserialized shape contains bearer secrets. Do not derive `Debug`, which could expose
// every handle if the parsing path is logged during future maintenance.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HandleFile {
    #[serde(deserialize_with = "deserialize_handles")]
    handles: HashMap<String, String>,
}

// A plain HashMap deserializer silently keeps the last value when a JSON object repeats a
// credential id. Reject the whole file instead of selecting one of two conflicting secrets.
// Also used for `vault.credential_ids`, where a repeated key would silently pick one of two ids.
fn deserialize_handles<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct HandlesVisitor;

    impl<'de> Visitor<'de> for HandlesVisitor {
        type Value = HashMap<String, String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a credential-id map with unique keys")
        }

        fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut handles = HashMap::with_capacity(entries.size_hint().unwrap_or(0));
            while let Some((credential_id, handle)) = entries.next_entry::<String, String>()? {
                match handles.entry(credential_id) {
                    Entry::Vacant(entry) => {
                        entry.insert(handle);
                    }
                    Entry::Occupied(_) => {
                        return Err(serde::de::Error::custom("duplicate credential id"));
                    }
                }
            }
            Ok(handles)
        }
    }

    deserializer.deserialize_map(HandlesVisitor)
}

#[derive(Debug)]
pub enum VaultHandlesError {
    Unresolvable,
    ExplicitMissing {
        path: PathBuf,
        source: std::io::Error,
    },
    Inspect {
        path: PathBuf,
        source: std::io::Error,
    },
    #[cfg(unix)]
    Symlink {
        path: PathBuf,
    },
    #[cfg(unix)]
    NotRegularFile {
        path: PathBuf,
    },
    #[cfg(unix)]
    InsecurePermissions {
        path: PathBuf,
        mode: u32,
    },
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        line: usize,
        column: usize,
    },
}

impl std::fmt::Display for VaultHandlesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VaultHandlesError::Unresolvable => write!(
                f,
                "BROCA_AUTH_SOURCE=vault but no config home to locate the handle map"
            ),
            VaultHandlesError::ExplicitMissing { path, source } => write!(
                f,
                "{HANDLES_PATH_ENV} selects missing vault handles file {}: {source}",
                path.display()
            ),
            VaultHandlesError::Inspect { path, source } => {
                write!(f, "inspect vault handles file {}: {source}", path.display())
            }
            #[cfg(unix)]
            VaultHandlesError::Symlink { path } => write!(
                f,
                "vault handles file must not be a symbolic link: {}",
                path.display()
            ),
            #[cfg(unix)]
            VaultHandlesError::NotRegularFile { path } => write!(
                f,
                "vault handles path is not a regular file: {}",
                path.display()
            ),
            #[cfg(unix)]
            VaultHandlesError::InsecurePermissions { path, mode } => write!(
                f,
                "vault handles file is group/world accessible (mode {mode:04o}); chmod 600 {}",
                path.display()
            ),
            VaultHandlesError::Read { path, source } => {
                write!(f, "read vault handle file {}: {source}", path.display())
            }
            VaultHandlesError::Parse { path, line, column } => write!(
                f,
                "parse vault handle file {} (at line {line}, column {column})",
                path.display()
            ),
        }
    }
}

impl std::error::Error for VaultHandlesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            VaultHandlesError::Inspect { source, .. }
            | VaultHandlesError::Read { source, .. }
            | VaultHandlesError::ExplicitMissing { source, .. } => Some(source),
            #[cfg(unix)]
            VaultHandlesError::Symlink { .. }
            | VaultHandlesError::NotRegularFile { .. }
            | VaultHandlesError::InsecurePermissions { .. } => None,
            VaultHandlesError::Parse { .. } | VaultHandlesError::Unresolvable => None,
        }
    }
}

#[cfg(unix)]
fn validate_handle_file(path: &Path) -> Result<(), VaultHandlesError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata =
        std::fs::symlink_metadata(path).map_err(|source| VaultHandlesError::Inspect {
            path: path.to_path_buf(),
            source,
        })?;
    if metadata.file_type().is_symlink() {
        return Err(VaultHandlesError::Symlink {
            path: path.to_path_buf(),
        });
    }
    if !metadata.is_file() {
        return Err(VaultHandlesError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }

    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(VaultHandlesError::InsecurePermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

#[cfg(windows)]
fn validate_handle_file(path: &Path) -> Result<(), VaultHandlesError> {
    broca_owner_only::validate(path).map_err(|source| VaultHandlesError::Inspect {
        path: path.to_path_buf(),
        source,
    })?;
    if !std::fs::symlink_metadata(path)
        .map_err(|source| VaultHandlesError::Inspect {
            path: path.to_path_buf(),
            source,
        })?
        .is_file()
    {
        return Err(VaultHandlesError::Inspect {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "vault handles must be a regular file",
            ),
        });
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn validate_handle_file(_path: &Path) -> Result<(), VaultHandlesError> {
    Ok(())
}

/// The credential-id → vault-handle map, loaded from the handle file. Lookups are by the
/// canonical credential-id string (e.g. `"oauth:anthropic"`). The handle strings are
/// secrets — `Debug` is deliberately NOT derived so a handle can't leak into a log line.
pub struct VaultHandles {
    handles: HashMap<String, String>,
}

impl VaultHandles {
    /// The resolved handle-file path: `$BROCA_VAULT_HANDLES_PATH` if set, else
    /// `vault-handles.json` under the shared config home (`crate::config_home`).
    ///
    /// This file sits BESIDE `broca.jsonc` and `auth-methods.json`, which is why the three used
    /// to be the fleet's most likely instance of QTA's two-resolvers-one-directory defect: one
    /// ladder gaining a rung the others did not would orphan the handle map from the config an
    /// operator had just edited. A missing handle file is fatal when vault auth is enabled.
    pub fn default_path() -> crate::config_home::ConfigHome {
        crate::config_home::config_file(HANDLES_PATH_ENV, "vault-handles.json")
    }

    /// Preserve strict missing-file handling while identifying an operator's broken override.
    /// Wrap only missing-file IO errors; parse diagnostics must never expose secret contents.
    pub fn load_resolved(
        resolved: crate::config_home::ConfigHome,
    ) -> Result<Self, VaultHandlesError> {
        let crate::config_home::ConfigHome::Resolved { path, explicit } = resolved else {
            return Err(VaultHandlesError::Unresolvable);
        };
        Self::load(&path).map_err(|error| match error {
            VaultHandlesError::Inspect { path, source }
            | VaultHandlesError::Read { path, source }
                if explicit && source.kind() == std::io::ErrorKind::NotFound =>
            {
                VaultHandlesError::ExplicitMissing { path, source }
            }
            other => other,
        })
    }

    /// Load and parse the handle file at `path`. Errors never include the file contents, so a
    /// malformed secrets file cannot echo a handle. Unix files must be regular, non-symlink
    /// files with no group or world access.
    pub fn load(path: &Path) -> Result<Self, VaultHandlesError> {
        validate_handle_file(path)?;
        let bytes = std::fs::read(path).map_err(|source| VaultHandlesError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        // Deliberately DROP serde's error detail: a parse error can echo the offending
        // bytes, which in a SECRETS file could leak a handle value into a log. Report only
        // the path + line/column, never the content.
        let parsed: HandleFile =
            serde_json::from_slice(&bytes).map_err(|error| VaultHandlesError::Parse {
                path: path.to_path_buf(),
                line: error.line(),
                column: error.column(),
            })?;
        Ok(Self {
            handles: parsed.handles,
        })
    }

    /// As [`Self::load_resolved`], except that a MISSING file at the default location is an
    /// empty map: a deployment whose credentials are all read by grant needs no handle file.
    /// A missing file that `BROCA_VAULT_HANDLES_PATH` names explicitly is still fatal, since
    /// naming a file that does not exist is a typo, not a choice. A file that exists but is
    /// malformed, insecure, or unreadable is fatal either way.
    pub fn load_resolved_optional(
        resolved: crate::config_home::ConfigHome,
    ) -> Result<Self, VaultHandlesError> {
        match Self::load_resolved(resolved) {
            Err(
                VaultHandlesError::Inspect { source, .. } | VaultHandlesError::Read { source, .. },
            ) if source.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            other => other,
        }
    }

    /// A map with no handles: every credential is read by grant.
    pub fn empty() -> Self {
        Self {
            handles: HashMap::new(),
        }
    }

    /// The vault handle for a credential id, if the map carries one. Looked up by the
    /// canonical `<method>:<provider>[:<account>]` string.
    pub fn handle_for(&self, credential_id: &CredentialId) -> Option<&str> {
        self.handles
            .get(&credential_id.to_id_string())
            .map(String::as_str)
    }

    /// The credential ids the map carries (for a secret-free startup diagnostic). Never
    /// exposes the handle values.
    pub fn credential_ids(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self.handles.keys().map(String::as_str).collect();
        ids.sort_unstable();
        ids
    }
}

/// Load an operator dev tool's bearer handle from `$BROCA_DEVTOOLS_HANDLES`, or
/// `dev-tools-handles.json` under the shared config home. Never reads the daemon's legacy
/// handle file. The same permission, symlink and regular-file checks as [`VaultHandles`] apply.
pub fn load_devtools_handle(credential: &CredentialId) -> Result<String, String> {
    load_devtools_handle_with(credential, |key| std::env::var_os(key))
}

fn load_devtools_handle_with(
    credential: &CredentialId,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<String, String> {
    let id = credential.to_id_string();
    let guidance = format!(
        "Mint a handle with `ck auth mint-handle --id {id}` (operator only) and add it to the \
         dev-tools handle file; the daemon's vault-handles.json is not used by dev tools"
    );
    let crate::config_home::ConfigHome::Resolved { path, .. } =
        crate::config_home::config_file_with(
            env,
            "BROCA_DEVTOOLS_HANDLES",
            "dev-tools-handles.json",
        )
    else {
        return Err(format!(
            "Cannot locate the dev-tools handle file for credential '{id}': no config home; \
             set BROCA_DEVTOOLS_HANDLES. {guidance}"
        ));
    };
    let handles = VaultHandles::load(&path).map_err(|error| {
        let reason = match &error {
            VaultHandlesError::Inspect { source, .. } | VaultHandlesError::Read { source, .. }
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                "file is missing".to_string()
            }
            _ => error.to_string(),
        };
        format!(
            "Dev-tools handle file {} for credential '{id}': {reason}. {guidance}",
            path.display()
        )
    })?;
    handles
        .handle_for(credential)
        .map(str::to_owned)
        .ok_or_else(|| {
            format!(
                "Dev-tools handle file {} is missing credential '{id}'. {guidance}",
                path.display()
            )
        })
}

/// The `vault` section of `broca.jsonc`. Only `credential_ids` exists; an unknown key is
/// refused so a misspelt section name cannot silently leave every alias unapplied.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VaultSection {
    #[serde(default, deserialize_with = "deserialize_handles")]
    credential_ids: HashMap<String, String>,
}

/// The part of `broca.jsonc` this module reads. Other top-level keys belong to other loaders.
#[derive(Deserialize)]
struct BrocaConfigVaultView {
    #[serde(default)]
    vault: Option<VaultSection>,
}

/// `vault.credential_ids` from `broca.jsonc`: broca credential key → the vault credential id
/// to read by grant, for a key whose vault id differs from the key. Not a secret.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VaultCredentialIds {
    ids: HashMap<String, String>,
}

impl VaultCredentialIds {
    /// Read from the config-home `broca.jsonc` (or `$BROCA_CONFIG_PATH`). A missing file, or
    /// no config home, means no aliases.
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
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default())
            }
            Err(error) => return Err(format!("read broca config {}: {error}", path.display())),
        };
        Self::parse(&text)
            .map_err(|error| format!("parse broca config {}: {error}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let stripped = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let view: BrocaConfigVaultView =
            serde_json::from_str(&stripped).map_err(|error| format!("vault: {error}"))?;
        let ids = view
            .vault
            .map(|section| section.credential_ids)
            .unwrap_or_default();
        for (key, vault_id) in &ids {
            // The key is matched against the routing key a run resolves, so one that cannot
            // parse as a credential key can never apply: refuse it rather than ignore it.
            if CredentialId::parse(key).is_none() {
                return Err(format!(
                    "vault.credential_ids key {key:?} is not a credential key \
                     (<method>:<provider>[:<account>])"
                ));
            }
            if vault_id.trim().is_empty() {
                return Err(format!(
                    "vault.credential_ids.{key} must name a credential id"
                ));
            }
        }
        Ok(Self { ids })
    }

    /// Build from explicit pairs, for callers that do not read `broca.jsonc`.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            ids: pairs
                .into_iter()
                .map(|(key, id)| (key.into(), id.into()))
                .collect(),
        }
    }
}

/// Which vault credential id the daemon reads for each broca credential key. Every read is by
/// credential id under this module's grant (`credential.get_scoped`); the daemon holds no
/// capability handles. The id is the key itself unless `vault.credential_ids` maps it.
#[derive(Clone, Debug, Default)]
pub struct VaultAddressing {
    credential_ids: VaultCredentialIds,
}

impl VaultAddressing {
    pub fn new(credential_ids: VaultCredentialIds) -> Self {
        Self { credential_ids }
    }

    /// The vault credential id for a broca credential key: its `vault.credential_ids` entry
    /// when it has one, otherwise the key itself.
    pub fn vault_id_for<'a>(&'a self, key: &CredentialId) -> std::borrow::Cow<'a, str> {
        let key = key.to_id_string();
        match self.credential_ids.ids.get(&key) {
            Some(vault_id) => std::borrow::Cow::Borrowed(vault_id),
            None => std::borrow::Cow::Owned(key),
        }
    }

    /// The vault credential id a frozen selection reads. A selection frozen with its
    /// `auth_method` names the vault's own credential id, read verbatim with no alias lookup;
    /// an older one holds a broca key, mapped like any key. `None` for a key that does not
    /// parse, which no read ever used.
    pub fn vault_id_for_selection(&self, selection: &broca_wal::AuthSelection) -> Option<String> {
        if selection.auth_method.is_some() {
            return Some(selection.credential_id.clone());
        }
        CredentialId::parse(&selection.credential_id)
            .map(|key| self.vault_id_for(&key).into_owned())
    }

    /// The `vault.credential_ids` aliases, sorted by key, for the startup census.
    pub fn aliases(&self) -> Vec<(&str, &str)> {
        let mut aliases: Vec<(&str, &str)> = self
            .credential_ids
            .ids
            .iter()
            .map(|(key, id)| (key.as_str(), id.as_str()))
            .collect();
        aliases.sort_unstable();
        aliases
    }
}

/// The refusal for a scoped read the vault answered `not_found`.
///
/// The vault gives the same answer for "no credential has this id" and "this principal holds no
/// read grant on it", on purpose, so a caller cannot probe which credentials exist. This text
/// therefore names both causes, the principal and the id that was asked for, and points to the
/// vault's own audit log, which records which of the two it was.
pub fn scoped_not_found_message(principal: &str, key: &str, vault_id: &str) -> String {
    let alias = if key == vault_id {
        String::new()
    } else {
        format!(" (broca key '{key}', mapped by vault.credential_ids)")
    };
    format!(
        "vault credential '{vault_id}'{alias} was read by grant as '{principal}' and the vault \
         answered not_found. That means either no credential has id '{vault_id}', or \
         '{principal}' holds no read grant on it: the vault answers both the same way on \
         purpose. `ck auth events` shows which one it was. To grant it: ck auth grant \
         --principal {principal} --selector-kind exact --selector {vault_id} --operation read; \
         or map '{key}' to the right id under vault.credential_ids in broca.jsonc"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp(name: &str, contents: &str) -> PathBuf {
        let path = broca_test_support::scratch_root().join(format!(
            "broca-vault-handles-{}-{}.json",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_file(&path);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.mode(0o600);
        }
        let mut f = options.open(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn devtools_handle_override_wins() {
        let path = write_temp(
            "devtools-override",
            r#"{"handles":{"oauth:anthropic":"ckh_dev"}}"#,
        );
        let handle = load_devtools_handle_with(&key("oauth:anthropic"), |name| match name {
            "BROCA_DEVTOOLS_HANDLES" => Some(path.clone().into_os_string()),
            "BROCA_VAULT_HANDLES_PATH" => Some("/unused/legacy.json".into()),
            "XDG_CONFIG_HOME" => Some("/unused/config".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(handle, "ckh_dev");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn devtools_handle_missing_file_names_path_and_credential() {
        let path = write_temp("devtools-missing", r#"{"handles":{}}"#);
        std::fs::remove_file(&path).unwrap();
        let error = load_devtools_handle_with(&key("oauth:anthropic"), |name| {
            (name == "BROCA_DEVTOOLS_HANDLES").then(|| path.clone().into_os_string())
        })
        .unwrap_err();
        assert!(error.contains("file is missing"), "{error}");
        assert_devtools_missing_error(&error, &path);
    }

    #[test]
    fn devtools_handle_missing_entry_names_credential() {
        let path = write_temp("devtools-empty", r#"{"handles":{}}"#);
        let error = load_devtools_handle_with(&key("oauth:anthropic"), |name| {
            (name == "BROCA_DEVTOOLS_HANDLES").then(|| path.clone().into_os_string())
        })
        .unwrap_err();
        assert!(
            error.contains("missing credential 'oauth:anthropic'"),
            "{error}"
        );
        assert_devtools_missing_error(&error, &path);
        std::fs::remove_file(path).unwrap();
    }

    fn assert_devtools_missing_error(error: &str, path: &Path) {
        for expected in [
            "Dev-tools handle file",
            path.to_str().unwrap(),
            "oauth:anthropic",
            "ck auth mint-handle --id oauth:anthropic",
            "operator only",
            "the daemon's vault-handles.json is not used by dev tools",
        ] {
            assert!(error.contains(expected), "missing {expected:?}: {error}");
        }
    }

    #[test]
    fn loads_handles_keyed_by_credential_id() {
        use crate::auth_routing::{AuthMethod, CredentialId};
        let path = write_temp(
            "ok",
            r#"{ "handles": { "oauth:anthropic": "ckh_a", "antigravity:google": "ckh_g" } }"#,
        );
        let h = match VaultHandles::load(&path) {
            Ok(h) => h,
            Err(e) => panic!("load failed: {e}"),
        };
        assert_eq!(
            h.handle_for(&CredentialId::new(AuthMethod::Oauth, "anthropic", None)),
            Some("ckh_a")
        );
        assert_eq!(
            h.handle_for(&CredentialId::new(AuthMethod::Antigravity, "google", None)),
            Some("ckh_g")
        );
        // A configured provider under a DIFFERENT method is a distinct id → not present.
        assert_eq!(
            h.handle_for(&CredentialId::new(AuthMethod::ApiKey, "anthropic", None)),
            None
        );
        assert_eq!(
            h.credential_ids(),
            vec!["antigravity:google", "oauth:anthropic"]
        );
        let _ = std::fs::remove_file(&path);
    }

    fn key(text: &str) -> CredentialId {
        CredentialId::parse(text).expect("a valid credential key")
    }

    /// Every key is read by grant: as its alias when it has one, and as itself otherwise.
    #[test]
    fn vault_id_is_the_alias_else_the_key() {
        let addressing = VaultAddressing::new(VaultCredentialIds::from_pairs([(
            "apikey:synthetic",
            "apikey:synthetic:main",
        )]));
        assert_eq!(
            addressing.vault_id_for(&key("apikey:synthetic")),
            "apikey:synthetic:main"
        );
        assert_eq!(
            addressing.vault_id_for(&key("oauth:anthropic")),
            "oauth:anthropic"
        );
        assert_eq!(
            addressing.aliases(),
            vec![("apikey:synthetic", "apikey:synthetic:main")]
        );
    }

    #[test]
    fn credential_ids_parse_from_the_vault_section_of_broca_jsonc() {
        let ids = VaultCredentialIds::parse(
            r#"{
                // other sections belong to other loaders
                "providers": {},
                "vault": { "credential_ids": { "apikey:synthetic": "apikey:synthetic:main", }, },
            }"#,
        )
        .unwrap();
        assert_eq!(
            ids,
            VaultCredentialIds::from_pairs([("apikey:synthetic", "apikey:synthetic:main")])
        );
        assert_eq!(
            VaultCredentialIds::parse(r#"{"providers":{}}"#).unwrap(),
            VaultCredentialIds::default()
        );
        for (bad, needle) in [
            (r#"{"vault":{"credential_id":{}}}"#, "unknown field"),
            (
                r#"{"vault":{"credential_ids":{"apikey:a":"x","apikey:a":"y"}}}"#,
                "duplicate",
            ),
            (
                r#"{"vault":{"credential_ids":{"synthetic":"x"}}}"#,
                "not a credential key",
            ),
            (
                r#"{"vault":{"credential_ids":{"apikey:a":" "}}}"#,
                "must name",
            ),
        ] {
            let error = VaultCredentialIds::parse(bad).expect_err(bad);
            assert!(error.contains(needle), "{bad}: {error}");
        }
    }

    /// The handle file is optional at its default location, still fatal when an explicit
    /// override names a missing file, and still fatal when present but malformed.
    #[test]
    fn a_missing_default_handle_file_is_an_empty_map() {
        let missing = broca_test_support::scratch_root().join(format!(
            "broca-vault-handles-{}-absent.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&missing);
        let loaded =
            VaultHandles::load_resolved_optional(crate::config_home::ConfigHome::Resolved {
                path: missing.clone(),
                explicit: false,
            })
            .expect("a missing default handle file is no handles");
        assert!(loaded.credential_ids().is_empty());
        assert!(matches!(
            VaultHandles::load_resolved_optional(crate::config_home::ConfigHome::Resolved {
                path: missing,
                explicit: true,
            }),
            Err(VaultHandlesError::ExplicitMissing { .. })
        ));
        let malformed = write_temp("optional-malformed", r#"{ "handles": 7 }"#);
        assert!(matches!(
            VaultHandles::load_resolved_optional(crate::config_home::ConfigHome::Resolved {
                path: malformed.clone(),
                explicit: false,
            }),
            Err(VaultHandlesError::Parse { .. })
        ));
        let _ = std::fs::remove_file(&malformed);
    }

    #[test]
    fn scoped_not_found_names_both_causes_the_principal_and_the_id() {
        let message = scoped_not_found_message(
            "reserved:broca",
            "apikey:synthetic",
            "apikey:synthetic:main",
        );
        for needle in [
            "ck auth events",
            "reserved:broca",
            "'apikey:synthetic:main'",
            "broca key 'apikey:synthetic'",
            "no credential has id",
            "no read grant",
        ] {
            assert!(message.contains(needle), "missing {needle:?} in: {message}");
        }
    }

    #[test]
    fn duplicate_credential_ids_are_rejected_without_echoing_handles() {
        let path = write_temp(
            "duplicate-credential-id",
            r#"{
                "handles": {
                    "oauth:anthropic": "ckh_intended",
                    "oauth:anthropic": "ckh_shadow"
                }
            }"#,
        );

        let error = match VaultHandles::load(&path) {
            Ok(_) => panic!("duplicate credential ids must not select either handle"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, VaultHandlesError::Parse { path: error_path, .. } if error_path == &path)
        );
        let message = error.to_string();
        assert!(!message.contains("ckh_intended"));
        assert!(!message.contains("ckh_shadow"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn malformed_file_errors_without_echoing_contents() {
        let path = write_temp("bad", r#"{ "handles": "not-an-object" }"#);
        let err = match VaultHandles::load(&path) {
            Ok(_) => panic!("malformed file must not parse"),
            Err(e) => e,
        };
        // The error names the path + the parse problem, never the (secret) contents.
        let message = err.to_string();
        assert!(message.contains("parse vault handle file"));
        assert!(!message.contains("not-an-object"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unknown_top_level_key_is_rejected_without_echoing_contents() {
        let path = write_temp(
            "unknown-top-level",
            r#"{ "handles": {}, "future": "secret-value" }"#,
        );
        let error = match VaultHandles::load(&path) {
            Ok(_) => panic!("unknown top-level key must not parse"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, VaultHandlesError::Parse { path: error_path, .. } if error_path == &path)
        );
        assert!(!error.to_string().contains("secret-value"));
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn group_or_world_access_is_rejected_with_typed_mode_error() {
        use std::os::unix::fs::PermissionsExt;

        let path = write_temp("insecure-mode", r#"{"handles":{}}"#);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = match VaultHandles::load(&path) {
            Ok(_) => panic!("0644 secrets file must be rejected"),
            Err(error) => error,
        };

        match &error {
            VaultHandlesError::InsecurePermissions {
                path: error_path,
                mode,
            } => {
                assert_eq!(error_path, &path);
                assert_eq!(*mode, 0o644);
            }
            other => panic!("expected insecure-permissions error, got {other:?}"),
        }
        let message = error.to_string();
        assert!(message.contains("mode 0644"));
        assert!(message.contains(&format!("chmod 600 {}", path.display())));
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn owner_only_file_loads() {
        let path = write_temp("owner-only", r#"{"handles":{}}"#);
        VaultHandles::load(&path).expect("0600 secrets file must load");
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_link_is_rejected_before_reading_target() {
        use std::os::unix::fs::symlink;

        let target = write_temp("symlink-target", r#"{"handles":{}}"#);
        let link = broca_test_support::scratch_root().join(format!(
            "broca-vault-handles-{}-symlink.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&link);
        symlink(&target, &link).unwrap();
        let error = match VaultHandles::load(&link) {
            Ok(_) => panic!("symlink must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(&error, VaultHandlesError::Symlink { .. }));
        let message = error.to_string();
        assert!(message.contains("symbolic link"));
        assert!(message.contains(&link.display().to_string()));
        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_file(&target);
    }

    /// INJECTS the environment rather than mutating the process's own.
    ///
    /// This used to `set_var` then `remove_var`, which is a data race under a parallel test
    /// runner: the environment is PROCESS-GLOBAL, so between those two calls every other test
    /// thread in this binary sees the override. Any of them reading a config path would resolve
    /// against `/tmp/custom-handles.json` and fail for a reason with no visible cause -- and it
    /// would fail INTERMITTENTLY, on scheduling, which is the expensive kind. The restore also
    /// clears the variable unconditionally rather than restoring a prior value, so a developer
    /// with a real override exported would have it silently deleted for the rest of the run.
    ///
    /// `config_file_with` takes the lookup as a parameter for exactly this reason, so the two
    /// rungs are testable without touching the process at all. Rust 2024 marks `set_var` unsafe
    /// on the same grounds.
    ///
    /// THE REST OF THE WORKSPACE'S ENV MUTATION IS SOUND, and the reason is not the one I first
    /// wrote. I claimed the other sites set env for CHILD processes before spawn; they do not.
    /// Four test files call `remove_var` on the PARENT's own environment to scrub
    /// `SUBC_MODULE_ID` and `SUBC_LAUNCH_NONCE` so a spawned daemon is not rejected. That is
    /// process-global mutation. It is safe for a different reason: a UNIDIRECTIONAL SCRUB toward
    /// the state every reader in the binary wants cannot race a concurrent reader into a wrong
    /// answer, and there is no restore to clobber anyone.
    ///
    /// The two save/restore sites hold a lock. `conformance.rs::with_perturbed_env` runs under
    /// the harness's process-global lock; `transport.rs::StreamFloorEnvGuard::apply` returns the
    /// `MutexGuard` ALONGSIDE the restore guard, so the lock cannot be dropped before the
    /// environment is put back. That pairing is the shape to copy -- the tempting version drops
    /// the lock at the end of `apply()` and leaves the restore unprotected.
    #[test]
    fn env_override_takes_precedence_for_path() {
        let env = |key: &str| {
            (key == HANDLES_PATH_ENV).then(|| std::ffi::OsString::from("/tmp/custom-handles.json"))
        };
        assert_eq!(
            crate::config_home::config_file_with(env, HANDLES_PATH_ENV, "vault-handles.json"),
            crate::config_home::ConfigHome::Resolved {
                path: PathBuf::from("/tmp/custom-handles.json"),
                explicit: true,
            }
        );
    }
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{broaden, deny_acl_read, reset_acl, BroadParent};
    #[test]
    fn windows_vault_handles_refuse_insecure_or_unreadable_acl() {
        let fixture = BroadParent::new("vault-handles");
        let path = fixture.path.join("handles.json");
        use std::io::Write;
        broca_owner_only::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap()
            .write_all(br#"{"handles":{}}"#)
            .unwrap();
        assert!(VaultHandles::load(&path).unwrap().handles.is_empty());
        for unreadable in [false, true] {
            if unreadable {
                deny_acl_read(&path);
            } else {
                broaden(&path);
            }
            assert!(
                matches!(
                    VaultHandles::load(&path),
                    Err(VaultHandlesError::Inspect { .. })
                ),
                "unsafe ACL must be refused before parsing vault handles"
            );
            reset_acl(&path);
            broca_owner_only::repair(&path).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), br#"{"handles":{}}"#);
            assert!(VaultHandles::load(&path).unwrap().handles.is_empty());
        }
    }
}
