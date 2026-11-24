//! Credential keys and default sign-in methods derived from the cached vault rows.
//! The vendor service name is not a catalog provider id; only `provider_ids` routes a row.

use crate::account_routing::ListingCache;
use broca_subc::vault::{ScopedCredential, ScopedList};
use std::sync::Arc;

pub use broca_catalog::live::AuthMethod;

/// The wire family a method IMPLIES, when the method selects a distinct wire grammar.
///
/// This is the SINGLE authority mapping method → family override. The serve binary
/// consults it for BOTH the family DRIVER (the catalog resolver's `family_override`,
/// which builds the run with this family) AND the run-start consistency check — so the
/// family a run renders with and the family the guard asserts can never come from two
/// sources, hence can never drift to a mismatched (family, credential) pair.
///
/// A same-family method (`ApiKey`/`Oauth`/`Gateway`) returns `None` → the catalog's family is
/// used unchanged. A gateway key borrows the rows of the provider its gateway serves. The catalog still receives the method separately because Anthropic API key
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
        AuthMethod::ApiKey | AuthMethod::Oauth | AuthMethod::Gateway => None,
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

/// Where the catalog rows of a route's provider come from. Routing offers some providers the
/// model catalog doesn't carry, such as `anthropic-platform` (the paid Anthropic API keys) or a
/// company gateway; such a provider borrows the rows of the one catalog provider its vault
/// credentials name in `serves`. This mirrors routing's own rule, so broca and routing agree on
/// which providers are servable and with which rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogProvider {
    /// The catalog serves the provider itself.
    Own,
    /// The catalog lacks the provider, and its active credentials all name this one catalog
    /// provider: the run takes that provider's rows (limits, wire family, quirks) and is billed
    /// as it.
    Borrowed(String),
    /// The catalog lacks the provider and no single catalog provider stands in for it.
    Unresolved(Unresolved),
}

/// Why a provider the catalog lacks has no rows to borrow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unresolved {
    /// No active credential is listed under the provider.
    NoCredential,
    /// Its active credentials name no catalog provider in `serves`.
    NoServedProvider,
    /// Its active credentials name more than one catalog provider.
    SeveralServedProviders(Vec<String>),
}

impl Unresolved {
    /// The refusal text naming why `provider` can't be served.
    pub fn describe(&self, provider: &str) -> String {
        match self {
            Self::NoCredential => format!(
                "provider '{provider}' is not in the model catalog and no active credential is \
                 listed under it"
            ),
            Self::NoServedProvider => format!(
                "provider '{provider}' is not in the model catalog and its credentials name no \
                 catalog provider they serve"
            ),
            Self::SeveralServedProviders(served) => format!(
                "provider '{provider}' is not in the model catalog and its credentials serve \
                 several catalog providers ({}), so none stands in for it",
                served.join(", ")
            ),
        }
    }
}

/// Where the catalog rows of `provider` come from, given whether the catalog serves a provider
/// id (`catalog_serves`) and the vault's listing. Only active rows listed under the provider
/// (`provider_ids`) count, and only `serves` values the catalog itself serves.
pub fn catalog_provider(
    listing: &ScopedList,
    provider: &str,
    catalog_serves: impl Fn(&str) -> bool,
) -> CatalogProvider {
    if catalog_serves(provider) {
        return CatalogProvider::Own;
    }
    let rows: Vec<&ScopedCredential> = listing
        .credentials
        .iter()
        .filter(|row| row.state == "active" && row.provider_ids.iter().any(|id| id == provider))
        .collect();
    if rows.is_empty() {
        return CatalogProvider::Unresolved(Unresolved::NoCredential);
    }
    let served: std::collections::BTreeSet<&str> = rows
        .iter()
        .flat_map(|row| row.serves.iter().map(String::as_str))
        .filter(|served| *served != provider && catalog_serves(served))
        .collect();
    let mut served: Vec<String> = served.into_iter().map(str::to_owned).collect();
    match served.len() {
        0 => CatalogProvider::Unresolved(Unresolved::NoServedProvider),
        1 => CatalogProvider::Borrowed(served.pop().expect("one served provider")),
        _ => CatalogProvider::Unresolved(Unresolved::SeveralServedProviders(served)),
    }
}

/// Subscription credentials precede metered API keys when a provider has several methods.
/// Keep the policy in one place so changing the default never depends on listing order.
pub const DEFAULT_METHOD_ORDER: [AuthMethod; 5] = [
    AuthMethod::Chatgpt,
    AuthMethod::Oauth,
    AuthMethod::Antigravity,
    AuthMethod::ApiKey,
    AuthMethod::Gateway,
];

pub fn default_method(listing: &ScopedList, provider: &str) -> Option<AuthMethod> {
    DEFAULT_METHOD_ORDER.into_iter().find(|method| {
        listing.credentials.iter().any(|row| {
            row.state == "active"
                && row.provider_ids.iter().any(|id| id == provider)
                && row.auth_method.as_deref() == Some(method.as_token())
        })
    })
}

/// Empty provider mappings are deliberately excluded even from exact-id matches.
pub(crate) fn exact_credential_row<'a>(
    listing: &'a ScopedList,
    key: &str,
) -> Option<&'a ScopedCredential> {
    listing
        .credentials
        .iter()
        .find(|row| row.state == "active" && !row.provider_ids.is_empty() && row.id == key)
}

/// Exact public keys take precedence; otherwise metadata must identify one active row.
pub fn credential_row<'a>(
    listing: &'a ScopedList,
    key: &CredentialId,
) -> Result<Option<&'a ScopedCredential>, broca_core::RunError> {
    let key_text = key.to_id_string();
    if let Some(row) = exact_credential_row(listing, &key_text) {
        return Ok(Some(row));
    }
    let mut candidates: Vec<_> = listing
        .credentials
        .iter()
        .filter(|row| {
            row.state == "active"
                && row.auth_method.as_deref() == Some(key.method.as_token())
                && row.provider_ids.iter().any(|id| id == &key.provider)
        })
        .collect();
    candidates.sort_by(|a, b| a.id.cmp(&b.id));
    match candidates.len() {
        0 => Ok(None),
        1 => Ok(candidates.pop()),
        _ => Err(broca_core::RunError::CredentialAmbiguous {
            credential: key_text,
            candidate_ids: candidates.into_iter().map(|row| row.id.clone()).collect(),
        }),
    }
}

/// The default-method view shares the daemon's listing cache; it has no file-backed defaults.
#[derive(Clone, Default)]
pub struct MethodConfig {
    listing: Arc<ListingCache>,
}

impl MethodConfig {
    pub fn empty() -> Self {
        Self::default()
    }
    pub fn from_listing(listing: Arc<ListingCache>) -> Self {
        Self { listing }
    }
    pub fn default_path() -> crate::config_home::ConfigHome {
        crate::config_home::config_file(METHODS_PATH_ENV, "auth-methods.json")
    }
    /// Only test for presence: obsolete file bytes, including invalid JSON, are never read.
    pub fn load_resolved(resolved: crate::config_home::ConfigHome) -> Result<Self, String> {
        if let crate::config_home::ConfigHome::Resolved { path, .. } = resolved {
            return Self::load(&path);
        }
        Ok(Self::empty())
    }
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        if path.exists() {
            tracing::warn!(
                "auth-methods.json is no longer read and can be deleted: {}",
                path.display()
            );
        }
        Ok(Self::empty())
    }
    pub fn method_for(&self, provider: &str) -> Option<AuthMethod> {
        default_method(&self.listing.get()?.0, provider)
    }
}

/// A caller pin takes precedence over the methods in the cached rows. The fallback only
/// constructs a key for the existing not-found refusal; it never authorizes a credential.
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

/// Preserve a frozen key's method and account. Older runs without a frozen vault id still
/// resolve that key against the rows, but never reselect its method from current defaults.
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

/// How a run's frozen credential selection authenticates, and for which provider, without
/// re-deriving anything the selection froze. A selection frozen with its `auth_method` (a
/// credential routing chose, named by the vault's opaque id) takes the method from that field
/// and the provider from the run, since it was offered for exactly that provider; its id is
/// never parsed. An older selection holds a broca key, which names both. `None` for a method
/// token this build does not know or a key that does not parse.
pub fn frozen_selection_credential(
    selection: &broca_records::record::AuthSelection,
    run_provider: &str,
) -> Option<(AuthMethod, String)> {
    match &selection.auth_method {
        Some(token) => AuthMethod::from_token(token).map(|method| (method, run_provider.into())),
        None => CredentialId::parse(&selection.credential_id).map(|id| (id.method, id.provider)),
    }
}

#[cfg(test)]
pub(crate) fn row_listing() -> ScopedList {
    let mut rows = vec![
        (
            "antigravity:google",
            "active",
            "antigravity",
            "antigravity",
            vec!["google"],
        ),
        (
            "antigravity:google:acct1",
            "active",
            "antigravity",
            "antigravity",
            vec!["google"],
        ),
        (
            "apikey:cline-pass",
            "active",
            "apikey",
            "cline-pass",
            vec![],
        ),
        (
            "apikey:google",
            "active",
            "apikey",
            "google",
            vec!["google"],
        ),
        (
            "apikey:inception",
            "retired",
            "apikey",
            "inception",
            vec!["inception"],
        ),
        (
            "apikey:kimi-for-coding",
            "active",
            "apikey",
            "kimi-for-coding",
            vec!["kimi-code-plan-global"],
        ),
        ("apikey:muse", "active", "apikey", "", vec![]),
        (
            "apikey:openai",
            "active",
            "apikey",
            "openai",
            vec!["openai"],
        ),
        (
            "apikey:opencode-zen",
            "active",
            "apikey",
            "opencode-zen",
            vec!["opencode"],
        ),
        (
            "chatgpt:openai",
            "active",
            "chatgpt",
            "openai",
            vec!["openai"],
        ),
        (
            "oauth:anthropic",
            "active",
            "oauth",
            "anthropic",
            vec!["anthropic"],
        ),
        ("oauth:cursor", "active", "oauth", "cursor", vec![]),
        ("opaque:broca-deployment:1", "active", "", "", vec![]),
    ];
    for id in [
        "apikey:anthropic:acct1",
        "apikey:anthropic:acct2",
        "apikey:anthropic:acct3",
        "apikey:anthropic:acct4",
    ] {
        rows.push((
            id,
            "active",
            "apikey",
            "anthropic",
            vec!["anthropic-platform"],
        ));
    }
    for id in [
        "apikey:openai:acct1",
        "apikey:openai:acct2",
        "apikey:openai:acct3",
    ] {
        rows.push((id, "active", "apikey", "openai", vec!["openai"]));
    }
    for id in ["chatgpt:openai:acct1", "chatgpt:openai:acct2"] {
        rows.push((id, "active", "chatgpt", "openai", vec!["openai"]));
    }
    for id in [
        "oauth:anthropic:acct1",
        "oauth:anthropic:acct2",
        "oauth:anthropic:acct3",
        "oauth:anthropic:acct4",
        "oauth:anthropic:acct5",
        "oauth:anthropic:acct6",
    ] {
        rows.push((id, "active", "oauth", "anthropic", vec!["anthropic"]));
    }
    ScopedList {
        credentials: rows.into_iter().map(|(id,state,method,vendor,providers)| serde_json::from_value(serde_json::json!({
            "id":id, "type":method, "categories":["llm"], "serves":[], "provider":vendor,
            "state":state, "record_version":1, "operations":["read"],
            "auth_method":if method.is_empty() { None } else { Some(method) }, "provider_ids":providers,
        })).unwrap()).collect(), grants:1, grant_tuples:vec![], view:"synthetic-11-10".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A listing of rows `(id, state, provider_ids, serves)`.
    fn served_listing(rows: &[(&str, &str, &[&str], &[&str])]) -> ScopedList {
        ScopedList {
            credentials: rows
                .iter()
                .map(|(id, state, providers, serves)| {
                    serde_json::from_value(serde_json::json!({
                        "id": id, "type": "apikey", "categories": ["llm"], "serves": serves,
                        "state": state, "record_version": 1, "operations": ["read"],
                        "auth_method": "apikey", "provider_ids": providers,
                    }))
                    .unwrap()
                })
                .collect(),
            grants: 1,
            grant_tuples: vec![],
            view: "synthetic".into(),
        }
    }

    /// The catalog this test pretends to have: anthropic and openai only.
    fn served(provider: &str) -> bool {
        matches!(provider, "anthropic" | "openai")
    }

    /// A provider the catalog carries is its own. One it lacks borrows the single catalog
    /// provider its active rows name in `serves`, whatever its id looks like (a gateway id
    /// needn't start with its provider's name). It's unresolved, naming why, when it has no
    /// active row, its rows name no catalog provider, or they name several.
    #[test]
    fn a_provider_the_catalog_lacks_borrows_the_one_provider_its_credentials_serve() {
        let listing = served_listing(&[
            (
                "apikey:anthropic:a",
                "active",
                &["anthropic-platform"],
                &["anthropic"],
            ),
            (
                "apikey:anthropic:b",
                "active",
                &["anthropic-platform"],
                &["anthropic"],
            ),
            ("gateway:acme", "active", &["corp-llm"], &["anthropic"]),
            ("apikey:mixed:a", "active", &["mixed"], &["anthropic"]),
            ("apikey:mixed:b", "active", &["mixed"], &["openai"]),
            ("apikey:gone:a", "needs_reauth", &["gone"], &["anthropic"]),
            ("apikey:odd:a", "active", &["odd"], &["not-in-catalog"]),
        ]);
        assert_eq!(
            catalog_provider(&listing, "anthropic", served),
            CatalogProvider::Own
        );
        assert_eq!(
            catalog_provider(&listing, "anthropic-platform", served),
            CatalogProvider::Borrowed("anthropic".into())
        );
        assert_eq!(
            catalog_provider(&listing, "corp-llm", served),
            CatalogProvider::Borrowed("anthropic".into())
        );
        assert_eq!(
            catalog_provider(&listing, "mixed", served),
            CatalogProvider::Unresolved(Unresolved::SeveralServedProviders(vec![
                "anthropic".into(),
                "openai".into()
            ]))
        );
        assert_eq!(
            catalog_provider(&listing, "gone", served),
            CatalogProvider::Unresolved(Unresolved::NoCredential)
        );
        assert_eq!(
            catalog_provider(&listing, "odd", served),
            CatalogProvider::Unresolved(Unresolved::NoServedProvider)
        );
        assert_eq!(
            catalog_provider(&listing, "nobody", served),
            CatalogProvider::Unresolved(Unresolved::NoCredential)
        );
    }

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

    #[test]
    fn vault_rows_resolve_exact_unique_ambiguous_and_missing_keys() {
        let listing = row_listing();
        let resolve = |key| credential_row(&listing, &CredentialId::parse(key).unwrap());
        assert_eq!(
            resolve("apikey:openai").unwrap().unwrap().id,
            "apikey:openai"
        );
        assert_eq!(
            resolve("apikey:kimi-code-plan-global").unwrap().unwrap().id,
            "apikey:kimi-for-coding"
        );
        assert_eq!(
            resolve("apikey:opencode").unwrap().unwrap().id,
            "apikey:opencode-zen"
        );
        assert_eq!(
            resolve("oauth:anthropic:acct1").unwrap().unwrap().id,
            "oauth:anthropic:acct1"
        );
        assert!(resolve("apikey:missing").unwrap().is_none());
        assert!(resolve("apikey:anthropic").unwrap().is_none());
        assert!(resolve("chatgpt:anthropic").unwrap().is_none());
        assert_eq!(
            resolve("oauth:anthropic").unwrap().unwrap().id,
            "oauth:anthropic"
        );
        let Err(broca_core::RunError::CredentialAmbiguous {
            credential,
            candidate_ids,
        }) = resolve("apikey:anthropic-platform")
        else {
            panic!("platform keys are ambiguous");
        };
        assert_eq!(credential, "apikey:anthropic-platform");
        assert_eq!(
            candidate_ids,
            [
                "apikey:anthropic:acct1",
                "apikey:anthropic:acct2",
                "apikey:anthropic:acct3",
                "apikey:anthropic:acct4"
            ]
        );
    }

    #[test]
    fn retired_unmapped_and_opaque_rows_never_match_even_by_exact_id() {
        let listing = row_listing();
        for key in [
            "apikey:inception",
            "apikey:cline-pass",
            "apikey:muse",
            "oauth:cursor",
            "apikey:broca-deployment",
        ] {
            assert!(
                credential_row(&listing, &CredentialId::parse(key).unwrap())
                    .unwrap()
                    .is_none(),
                "{key}"
            );
        }
    }

    #[test]
    fn default_methods_use_only_active_provider_ids_and_subscription_order() {
        let listing = row_listing();
        for (provider, expected) in [
            ("kimi-code-plan-global", Some(AuthMethod::ApiKey)),
            ("anthropic", Some(AuthMethod::Oauth)),
            ("openai", Some(AuthMethod::Chatgpt)),
            ("google", Some(AuthMethod::Antigravity)),
            ("missing", None),
            ("cursor", None),
            ("inception", None),
        ] {
            assert_eq!(default_method(&listing, provider), expected, "{provider}");
        }
        let cache = Arc::new(ListingCache::default());
        cache.store(listing);
        let methods = MethodConfig::from_listing(cache);
        assert_eq!(
            resolve_credential_id(
                "openai",
                Some(AuthMethod::ApiKey),
                Some("acct1"),
                &methods,
                AuthMethod::ApiKey
            )
            .to_id_string(),
            "apikey:openai:acct1"
        );
        assert_eq!(
            select_credential_id(
                Some("apikey:openai"),
                "openai",
                None,
                None,
                &methods,
                AuthMethod::ApiKey
            )
            .to_id_string(),
            "apikey:openai"
        );
        assert_eq!(
            resolve_credential_id("openai", None, None, &methods, AuthMethod::ApiKey)
                .to_id_string(),
            "chatgpt:openai"
        );
    }

    #[test]
    fn obsolete_method_file_is_never_parsed() {
        let path = broca_test_support::scratch_root().join("obsolete-auth-methods.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"not JSON or a method map").unwrap();
        assert!(MethodConfig::load(&path)
            .unwrap()
            .method_for("openai")
            .is_none());
        std::fs::remove_file(path).unwrap();
    }
}
