//! Auth-method routing: which credential (and, for wire-changing methods, which wire
//! family) a run uses for a provider.
//!
//! A provider can now have SEVERAL credentials — an api key, a provider OAuth token, a
//! special-backend OAuth token (Google Antigravity / OpenAI ChatGPT). They are addressed by
//! a `credential_id` of the form `<method>:<provider>[:<account>]` (e.g. `apikey:deepseek`,
//! `antigravity:google`, `oauth:anthropic`). This module owns:
//!  - [`AuthMethod`] — the method token (the first id segment),
//!  - [`CredentialId`] — build/parse the `<method>:<provider>[:<account>]` id,
//!  - [`MethodConfig`] — the per-provider DEFAULT method, read from config-home,
//!  - [`resolve_credential_id`] — pick the run's `(provider, method, account)` → id.
//!
//! It is PURE (the only IO is loading the config file) so the routing rules are unit-tested
//! in isolation from the vault and the loop. The token VALUE never appears here — this
//! decides WHICH credential to fetch, not the secret.

use std::collections::HashMap;

use serde::Deserialize;

pub use broca_catalog::live::AuthMethod;

/// The wire family a method IMPLIES, when the method selects a distinct wire grammar.
///
/// This is the SINGLE authority mapping method → family override. The serve binary
/// consults it for BOTH the family DRIVER (the catalog resolver's `family_override`,
/// which builds the run with this family) AND the run-start consistency check — so the
/// family a run renders with and the family the guard asserts can never come from two
/// sources, hence can never drift to a mismatched (family, credential) pair.
///
/// A same-family method (`ApiKey`/`Oauth`) returns `None` → the catalog's family is used
/// unchanged. The catalog still receives the method separately because Anthropic API key
/// changes frozen quirks and auth headers without changing the family. A wire-changing
/// method returns its family: `Antigravity` renders the Code-Assist envelope
/// (`GoogleCodeAssist`), `Chatgpt` renders the codex `/responses`
/// (`OpenAiResponsesChatgpt`).
///
/// It lives in the library rather than the binary because resolving a model handle the
/// way production resolves it requires this mapping, and a test that cannot reach it
/// cannot check the composition that actually decides whether a model serves.
pub fn family_for_method(method: AuthMethod) -> Option<broca_catalog::live::WireFamilyId> {
    use broca_catalog::live::WireFamilyId;
    match method {
        // Same family: catalog resolution applies any method-specific quirks/auth kind.
        AuthMethod::ApiKey | AuthMethod::Oauth => None,
        // Wire-changing: these select their own grammar (distinct endpoint + body).
        AuthMethod::Antigravity => Some(WireFamilyId::GoogleCodeAssist),
        AuthMethod::Chatgpt => Some(WireFamilyId::OpenAiResponsesChatgpt),
    }
}

/// The env var that overrides the default auth-method config path.
pub const METHODS_PATH_ENV: &str = "BROCA_AUTH_METHODS_PATH";

/// A vault credential id: `<method>:<provider>[:<account>]`. The handle map is keyed by the
/// canonical string form ([`CredentialId::to_id_string`]); a run resolves its
/// `(provider, method, account)` to one of these, then looks up the handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialId {
    pub method: AuthMethod,
    pub provider: String,
    pub account: Option<String>,
}

impl CredentialId {
    pub fn new(method: AuthMethod, provider: impl Into<String>, account: Option<String>) -> Self {
        Self {
            method,
            provider: provider.into(),
            account,
        }
    }

    /// The canonical `<method>:<provider>[:<account>]` string (the handle-map key).
    pub fn to_id_string(&self) -> String {
        match &self.account {
            Some(account) => format!("{}:{}:{}", self.method.as_token(), self.provider, account),
            None => format!("{}:{}", self.method.as_token(), self.provider),
        }
    }

    /// Parse a canonical id. The FIRST segment must be a known method token (so
    /// `oauth:anthropic` parses method=oauth/provider=anthropic, never the reverse). A 3rd
    /// segment is the account. Returns `None` if the first segment isn't a known method or
    /// the shape is wrong — the caller decides if that's a legacy id or an error. (The
    /// vault CLI owns the legacy `<provider>:<account>` compatibility parse; on this side we
    /// only build ids and validate the new scheme.)
    pub fn parse(id: &str) -> Option<Self> {
        let mut parts = id.splitn(3, ':');
        let method = AuthMethod::from_token(parts.next()?)?;
        let provider = parts.next()?;
        if provider.is_empty() {
            return None;
        }
        let account = parts.next().map(str::to_string);
        Some(Self {
            method,
            provider: provider.to_string(),
            account,
        })
    }
}

/// The per-provider DEFAULT auth method, loaded from config-home
/// (`~/.config/cortexkit/broca/auth-methods.json`, override `BROCA_AUTH_METHODS_PATH`).
/// A read-only consumer view; the CK app is the eventual writer. Providers ABSENT from the
/// map fall back to the caller-supplied default (api-key for the long tail), so the config
/// only needs to name the exceptions (the oauth / special-backend providers).
///
/// Format: `{ "defaults": { "google": "antigravity", "openai": "apikey", "anthropic": "oauth" } }`
///
/// Holds no secrets (only provider→method defaults), so `Debug` is safe to derive (unlike
/// the handle map, whose values are bearer secrets).
#[derive(Debug, Clone)]
pub struct MethodConfig {
    defaults: HashMap<String, AuthMethod>,
}

#[derive(Deserialize)]
// Strict BECAUSE of the default below, not despite it. A misspelled top-level key left
// `defaults` unpopulated, serde supplied an empty map, and the loader returned a valid config
// that routed every provider to API-key auth -- a legal value standing in for a missing one,
// and the legal value was the one that looked healthy. Strictness here is a scar; on an
// inbound wire it would be a refusal to interoperate with a newer peer, which is why this
// file has it and crates/broca-subc/src/vault.rs deliberately does not. If you are making
// serde attributes uniform across the crate, the inconsistency is the point.
#[serde(deny_unknown_fields)]
struct MethodConfigFile {
    #[serde(default)]
    defaults: HashMap<String, String>,
}

impl MethodConfig {
    /// An empty config (every provider uses the caller's fallback default). The shape when
    /// no config file exists — the api-key long tail needs no config entry.
    pub fn empty() -> Self {
        Self {
            defaults: HashMap::new(),
        }
    }

    /// The resolved config path: `$BROCA_AUTH_METHODS_PATH` if set, else `auth-methods.json`
    /// under the shared config home (`crate::config_home`).
    pub fn default_path() -> crate::config_home::ConfigHome {
        crate::config_home::config_file(METHODS_PATH_ENV, "auth-methods.json")
    }

    /// Load the selected file without losing whether an operator explicitly named it.
    pub fn load_resolved(resolved: crate::config_home::ConfigHome) -> Result<Self, String> {
        match resolved {
            crate::config_home::ConfigHome::Resolved { path, explicit } => {
                Self::load_with_origin(&path, explicit)
            }
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::empty()),
        }
    }

    /// Load the config from `path`. A MISSING file is not an error — it yields the empty
    /// config (every provider uses the fallback default). A present-but-malformed file IS an
    /// error (a typo'd method would silently mis-route, so fail loud), and an unknown method
    /// token is rejected by name.
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        Self::load_with_origin(path, false)
    }

    fn load_with_origin(path: &std::path::Path, explicit: bool) -> Result<Self, String> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if explicit {
                    return Err(format!(
                        "{METHODS_PATH_ENV} selects missing auth-methods file {}: {e}",
                        path.display()
                    ));
                }
                return Ok(Self::empty());
            }
            Err(e) => return Err(format!("read auth-methods file {}: {e}", path.display())),
        };
        let parsed: MethodConfigFile = serde_json::from_slice(&bytes)
            .map_err(|e| format!("parse auth-methods file {}: {e}", path.display()))?;
        let mut defaults = HashMap::new();
        for (provider, token) in parsed.defaults {
            let method = AuthMethod::from_token(&token).ok_or_else(|| {
                format!(
                    "auth-methods file {}: provider '{provider}' has unknown method '{token}' \
                     (expected apikey|oauth|antigravity|chatgpt)",
                    path.display()
                )
            })?;
            defaults.insert(provider, method);
        }
        Ok(Self { defaults })
    }

    /// The configured default method for a provider, if any.
    pub fn method_for(&self, provider: &str) -> Option<AuthMethod> {
        self.defaults.get(provider).copied()
    }

    /// Every configured `(provider, method)` default, sorted by provider.
    pub fn defaults(&self) -> Vec<(&str, AuthMethod)> {
        let mut defaults: Vec<(&str, AuthMethod)> = self
            .defaults
            .iter()
            .map(|(provider, method)| (provider.as_str(), *method))
            .collect();
        defaults.sort_unstable_by(|left, right| left.0.cmp(right.0));
        defaults
    }
}

/// Resolve the credential id for a run: a per-request `pinned` method wins; else the
/// provider's configured default; else the caller's `fallback` (api-key for the long tail).
/// The `account` rides through for the forward-compat multi-account segment. This is the
/// METHOD decision that gets FROZEN at run start — on resume the frozen method/credential_id
/// is read directly, this resolver is NEVER re-run (account/router state could differ).
pub fn resolve_credential_id(
    provider: &str,
    pinned: Option<AuthMethod>,
    account: Option<&str>,
    config: &MethodConfig,
    fallback: AuthMethod,
) -> CredentialId {
    let method = pinned
        .or_else(|| config.method_for(provider))
        .unwrap_or(fallback);
    CredentialId::new(method, provider, account.map(str::to_string))
}

/// Pick the credential id for a run, honoring the resume invariant: if the
/// run is resuming and the prior run froze a credential id, RE-USE that exact id — do NOT
/// re-resolve from config/router (which could have drifted between run start and resume → a
/// different method → a different wire family → cache bust + provider-invalid history).
/// Only when there is no frozen id (a fresh run) does this resolve from the caller pin or config.
///
/// `frozen` is the prior run's `RunConfig.auth_selection.credential_id` (the durable value);
/// a malformed frozen id falls through to a fresh resolve (defensive — a corrupt frozen id
/// shouldn't strand the run, and the family-consistency guard still catches a bad pair).
pub fn select_credential_id(
    frozen: Option<&str>,
    provider: &str,
    pinned: Option<AuthMethod>,
    account: Option<&str>,
    config: &MethodConfig,
    fallback: AuthMethod,
) -> CredentialId {
    if let Some(id) = frozen {
        if let Some(parsed) = CredentialId::parse(id) {
            return parsed;
        }
    }
    resolve_credential_id(provider, pinned, account, config, fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    #[test]
    fn method_token_round_trips() {
        for m in [
            AuthMethod::ApiKey,
            AuthMethod::Oauth,
            AuthMethod::Antigravity,
            AuthMethod::Chatgpt,
        ] {
            assert_eq!(AuthMethod::from_token(m.as_token()), Some(m));
        }
        assert_eq!(AuthMethod::from_token("nope"), None);
    }

    #[test]
    fn credential_id_format_matches_the_locked_scheme() {
        assert_eq!(
            CredentialId::new(AuthMethod::ApiKey, "deepseek", None).to_id_string(),
            "apikey:deepseek"
        );
        assert_eq!(
            CredentialId::new(AuthMethod::Antigravity, "google", None).to_id_string(),
            "antigravity:google"
        );
        assert_eq!(
            CredentialId::new(AuthMethod::Oauth, "anthropic", Some("work".into())).to_id_string(),
            "oauth:anthropic:work"
        );
    }

    #[test]
    fn parse_keys_method_off_the_first_segment_not_the_provider() {
        // oauth:anthropic must be method=oauth/provider=anthropic — never the reverse.
        let id = CredentialId::parse("oauth:anthropic").unwrap();
        assert_eq!(id.method, AuthMethod::Oauth);
        assert_eq!(id.provider, "anthropic");
        assert_eq!(id.account, None);
        // antigravity:google = method=antigravity/provider=google (opposite segment from oauth).
        let id = CredentialId::parse("antigravity:google").unwrap();
        assert_eq!(id.method, AuthMethod::Antigravity);
        assert_eq!(id.provider, "google");
        // 3-segment carries the account.
        let id = CredentialId::parse("apikey:openai:personal").unwrap();
        assert_eq!(id.account.as_deref(), Some("personal"));
        // A non-method first segment doesn't parse as the new scheme.
        assert!(CredentialId::parse("anthropic:work").is_none());
    }

    #[test]
    fn format_parse_round_trip() {
        for id in [
            CredentialId::new(AuthMethod::ApiKey, "cerebras", None),
            CredentialId::new(AuthMethod::Chatgpt, "openai", None),
            CredentialId::new(AuthMethod::Oauth, "anthropic", Some("acct1".into())),
        ] {
            assert_eq!(CredentialId::parse(&id.to_id_string()), Some(id));
        }
    }

    fn write_temp(name: &str, contents: &str) -> PathBuf {
        let path = broca_test_support::scratch_root().join(format!(
            "broca-auth-methods-{}-{}.json",
            std::process::id(),
            name
        ));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn config_loads_defaults_and_unknown_method_fails_loud() {
        let path = write_temp(
            "ok",
            r#"{ "defaults": { "google": "antigravity", "anthropic": "oauth" } }"#,
        );
        let c = MethodConfig::load(&path).unwrap();
        assert_eq!(c.method_for("google"), Some(AuthMethod::Antigravity));
        assert_eq!(c.method_for("anthropic"), Some(AuthMethod::Oauth));
        assert_eq!(c.method_for("deepseek"), None); // not configured → caller's fallback
        let _ = std::fs::remove_file(&path);

        let bad = write_temp("bad", r#"{ "defaults": { "google": "antigrav" } }"#);
        assert!(MethodConfig::load(&bad)
            .unwrap_err()
            .contains("unknown method"));
        let _ = std::fs::remove_file(&bad);
    }

    #[test]
    fn misspelled_top_level_key_is_rejected() {
        let path = write_temp(
            "misspelled-top-level",
            r#"{ "default": { "google": "antigravity" } }"#,
        );
        let error = MethodConfig::load(&path).unwrap_err();
        assert!(error.starts_with(&format!("parse auth-methods file {}:", path.display())));
        assert!(error.contains("unknown field `default`"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_config_is_empty_not_an_error() {
        let path = broca_test_support::scratch_root()
            .join(format!("broca-nope-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let c = MethodConfig::load(&path).unwrap();
        assert_eq!(c.method_for("anything"), None);
        assert!(c.defaults().is_empty());
    }

    #[test]
    fn select_uses_frozen_id_even_when_config_drifted() {
        // The resume invariant: a resume must re-use the FROZEN credential id, never
        // re-resolve from config — even if the config default changed since run start.
        // Frozen = oauth:anthropic. Config now says anthropic→apikey (drift). select must
        // STILL return oauth:anthropic.
        let path = write_temp("drift", r#"{ "defaults": { "anthropic": "apikey" } }"#);
        let config = MethodConfig::load(&path).unwrap();
        let got = select_credential_id(
            Some("oauth:anthropic"),
            "anthropic",
            None,
            None,
            &config,
            AuthMethod::ApiKey,
        );
        assert_eq!(
            got.to_id_string(),
            "oauth:anthropic",
            "resume must honor the frozen id, not the drifted config default"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn meta_oauth_config_resolves_an_account_scoped_credential() {
        let path = write_temp("meta-oauth", r#"{ "defaults": { "meta": "oauth" } }"#);
        let defaults = MethodConfig::load(&path).unwrap();
        let id = resolve_credential_id("meta", None, Some("test"), &defaults, AuthMethod::ApiKey);
        assert_eq!(id.to_id_string(), "oauth:meta:test");

        let long_tail =
            resolve_credential_id("deepseek", None, None, &defaults, AuthMethod::ApiKey);
        assert_eq!(long_tail.to_id_string(), "apikey:deepseek");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn select_resolves_fresh_when_no_frozen_id() {
        // A fresh run (no frozen id) resolves from config; the drifted-config test above
        // proves this branch is NOT taken on resume.
        let path = write_temp("fresh", r#"{ "defaults": { "google": "antigravity" } }"#);
        let config = MethodConfig::load(&path).unwrap();
        let got = select_credential_id(None, "google", None, None, &config, AuthMethod::ApiKey);
        assert_eq!(got.to_id_string(), "antigravity:google");
        // A malformed frozen id falls through to a fresh resolve (defensive).
        let got = select_credential_id(
            Some("garbage-not-an-id"),
            "google",
            None,
            None,
            &config,
            AuthMethod::ApiKey,
        );
        assert_eq!(got.to_id_string(), "antigravity:google");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn resolve_precedence_pin_then_config_then_fallback() {
        let path = write_temp("res", r#"{ "defaults": { "google": "antigravity" } }"#);
        let config = MethodConfig::load(&path).unwrap();

        // Pin wins over config.
        let id = resolve_credential_id(
            "google",
            Some(AuthMethod::ApiKey),
            None,
            &config,
            AuthMethod::ApiKey,
        );
        assert_eq!(id.to_id_string(), "apikey:google");

        // No pin → config default.
        let id = resolve_credential_id("google", None, None, &config, AuthMethod::ApiKey);
        assert_eq!(id.to_id_string(), "antigravity:google");

        // No pin, not configured → fallback (api-key long tail).
        let id = resolve_credential_id("deepseek", None, None, &config, AuthMethod::ApiKey);
        assert_eq!(id.to_id_string(), "apikey:deepseek");

        // Account rides through.
        let id = resolve_credential_id(
            "anthropic",
            Some(AuthMethod::Oauth),
            Some("work"),
            &config,
            AuthMethod::ApiKey,
        );
        assert_eq!(id.to_id_string(), "oauth:anthropic:work");
        let _ = std::fs::remove_file(&path);
    }
}
