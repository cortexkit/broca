//! Vault credential addressing for the daemon, and the capability-handle map kept for the
//! developer tools that still read credentials outside the daemon.
//!
//! ## The daemon reads by credential id only
//! The broca daemon never loads the handle file. Every credential it serves is read with
//! `credential.get_scoped`, addressed by vault credential id and authorized by the read grant
//! an operator gave this module's supervised principal (`reserved:broca`). [`VaultAddressing`]
//! finds the credential in cached vault rows, or uses an already-frozen vault id verbatim.
//! The old `vault.credential_ids` section is ignored. A scoped `not_found` is not retried
//! any other way: a fallback would
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
    Inspect {
        path: PathBuf,
        source: std::io::Error,
    },
    #[cfg(unix)]
    Symlink { path: PathBuf },
    #[cfg(unix)]
    NotRegularFile { path: PathBuf },
    #[cfg(unix)]
    InsecurePermissions { path: PathBuf, mode: u32 },
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
            VaultHandlesError::Inspect { source, .. } | VaultHandlesError::Read { source, .. } => {
                Some(source)
            }
            #[cfg(unix)]
            VaultHandlesError::Symlink { .. }
            | VaultHandlesError::NotRegularFile { .. }
            | VaultHandlesError::InsecurePermissions { .. } => None,
            VaultHandlesError::Parse { .. } => None,
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
    /// The credential ids the map carries (for a secret-free startup diagnostic). Never
    /// exposes the handle values.
    pub fn credential_ids(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self.handles.keys().map(String::as_str).collect();
        ids.sort_unstable();
        ids
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

    /// The vault handle for a credential id, if the map carries one. Looked up by the
    /// canonical `<method>:<provider>[:<account>]` string.
    pub fn handle_for(&self, credential_id: &CredentialId) -> Option<&str> {
        self.handles
            .get(&credential_id.to_id_string())
            .map(String::as_str)
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

/// Row-based addressing shares the same cached listing as default-method selection and routing.
#[derive(Clone, Default)]
pub struct VaultAddressing {
    listing: std::sync::Arc<crate::account_routing::ListingCache>,
}

impl VaultAddressing {
    pub fn new(listing: std::sync::Arc<crate::account_routing::ListingCache>) -> Self {
        Self { listing }
    }

    pub fn vault_id_for(&self, key: &CredentialId) -> Result<Option<String>, broca_core::RunError> {
        let (listing, _) = self
            .listing
            .get()
            .ok_or(broca_core::RunError::CredentialsUnavailable)?;
        Ok(crate::auth_routing::credential_row(&listing, key)?.map(|row| row.id.clone()))
    }

    /// A caller pin is not a method/provider alias: a missing row must not charge another account.
    pub fn vault_id_for_pin(&self, key: &CredentialId) -> Result<String, broca_core::RunError> {
        let (listing, _) = self
            .listing
            .get()
            .ok_or(broca_core::RunError::CredentialsUnavailable)?;
        let id = key.to_id_string();
        if crate::auth_routing::exact_credential_row(&listing, &id).is_some() {
            Ok(id)
        } else {
            Err(broca_core::RunError::Config(format!(
                "pinned vault credential '{id}' is not listed as an eligible active row"
            )))
        }
    }

    /// Frozen vault ids are authoritative even before the listing is loaded. Older selections
    /// without one resolve their frozen key, never the current default method.
    pub fn vault_id_for_selection(
        &self,
        selection: &broca_records::record::AuthSelection,
    ) -> Result<Option<String>, broca_core::RunError> {
        if let Some(id) = &selection.resolved_credential_id {
            return Ok(Some(id.clone()));
        }
        if selection.auth_method.is_some() {
            return Ok(Some(selection.credential_id.clone()));
        }
        match CredentialId::parse(&selection.credential_id) {
            Some(key) => self.vault_id_for(&key),
            None => Ok(None),
        }
    }
}

/// The refusal for a scoped read the vault answered `not_found`.
///
/// The vault gives the same answer for "no credential has this id" and "this principal holds no
/// read grant on it", on purpose, so a caller cannot probe which credentials exist. This text
/// therefore names both causes, the principal and the id that was asked for, and points to the
/// vault's own audit log, which records which of the two it was.
pub fn scoped_not_found_message(principal: &str, key: &str, vault_id: &str) -> String {
    let source = if key == vault_id {
        String::new()
    } else {
        format!(" (broca key '{key}', resolved from vault rows)")
    };
    format!(
        "vault credential '{vault_id}'{source} was looked up from the vault's rows by auth method \
         and provider for '{principal}': not_found. That means either no credential has id '{vault_id}', or \
         '{principal}' holds no read grant on it: the vault answers both the same way on \
         purpose. `ck auth events` shows which one it was. To grant it: ck auth grant \
         --principal {principal} --selector-kind exact --selector {vault_id} --operation read; \
         or set auth_method/provider_ids on the right active vault row for broca key '{key}'"
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

    #[test]
    fn addressing_uses_rows_and_frozen_ids_without_legacy_aliases() {
        let cache = std::sync::Arc::new(crate::account_routing::ListingCache::default());
        let addressing = VaultAddressing::new(cache.clone());
        assert!(matches!(
            addressing.vault_id_for(&key("apikey:openai")),
            Err(broca_core::RunError::CredentialsUnavailable)
        ));
        let frozen: broca_records::record::AuthSelection = serde_json::from_value(serde_json::json!({
            "credential_id":"apikey:kimi-code-plan-global", "resolved_credential_id":"frozen-vault-id"
        })).unwrap();
        assert_eq!(
            addressing
                .vault_id_for_selection(&frozen)
                .unwrap()
                .as_deref(),
            Some("frozen-vault-id")
        );
        cache.store(crate::auth_routing::row_listing());
        assert_eq!(
            addressing
                .vault_id_for(&key("apikey:kimi-code-plan-global"))
                .unwrap()
                .as_deref(),
            Some("apikey:kimi-for-coding")
        );
        let older: broca_records::record::AuthSelection = serde_json::from_value(
            serde_json::json!({"credential_id":"apikey:kimi-code-plan-global"}),
        )
        .unwrap();
        assert_eq!(
            addressing
                .vault_id_for_selection(&older)
                .unwrap()
                .as_deref(),
            Some("apikey:kimi-for-coding")
        );
        assert_eq!(
            addressing
                .vault_id_for_selection(&frozen)
                .unwrap()
                .as_deref(),
            Some("frozen-vault-id")
        );
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

#[cfg(test)]
mod obsolete_credential_config_tests {
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Log(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Log {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn an_obsolete_methods_file_boots_normally_with_one_warning_and_no_values_loaded() {
        let root = broca_test_support::scratch_root()
            .join(format!("obsolete-credentials-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let methods = root.join("auth-methods.json");
        // Invalid method bytes must not be deserialized as a map.
        std::fs::write(&methods, b"not-json-and-not-readable-as-methods").unwrap();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let log = Log(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || log.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let defaults = crate::MethodConfig::load(&methods).unwrap();
            assert_eq!(defaults.method_for("openai"), None);
        });
        let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert_eq!(text.matches("WARN").count(), 1, "{text}");
        assert_eq!(
            text.matches("auth-methods.json is no longer read and can be deleted")
                .count(),
            1,
            "{text}"
        );
        assert!(!text.contains("not-json-and"));
        std::fs::remove_dir_all(root).unwrap();
    }
}
