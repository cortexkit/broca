//! Charge-basis routing for ASTRO metering.
//!
//! The table is keyed by `(provider, auth method)`, not by auth method alone:
//! the same method can be metered for one provider and included in a plan for
//! another.  The shipped rows are conservative; anything unlisted is unknown.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::OnceLock;

use broca_core::{
    charge_basis_overrides_digest, ChargeBasis, ChargeBasisResolver, NO_CHARGE_BASIS_OVERRIDES,
};
use serde::Deserialize;
use serde_json::Value;

use crate::auth_routing::AuthMethod;
use crate::cache_routing::{strip_jsonc_comments, strip_trailing_commas, CONFIG_PATH_ENV};

/// Version carried with the shipped provider/method rows.
///
/// MOVING A PROVIDER INTO A METERED BASIS IS A CROSS-MODULE CHANGE, not a local one. The
/// billing consumer's catalog join fails for most of its spend facts, and it costs nothing only
/// because subscription-included rows never look up a rate. This table decides which rows do.
/// So a reclassification here is the arming condition for a defect that lives entirely in
/// another module, and the facts it would misprice are immutable once written. The same event
/// would also be that module's first real exercise of its pricing path at any scale.
///
/// Deliberately no counts here. They belong to ASTRO's store, so a number restated in this
/// repository is unverifiable from this repository, and a stale one is worst exactly here — at
/// the moment someone is deciding whether the risk is still real. Ask ASTRO to re-run the join
/// measurement against the routing you are proposing, before it lands rather than after: that
/// splits one simultaneous test of two unproven mechanisms into two separate ones.
pub const CHARGE_BASIS_MAPPING_VERSION: u32 = 5;

/// The provider/method table used while deriving spend facts.
///
/// A fact records the table it was resolved against in two fields: the shipped
/// [`CHARGE_BASIS_MAPPING_VERSION`] (which configuration never raises) and
/// `charge_basis_overrides`, which is `"none"` or a digest of the local override rows. The
/// two used to share one number, so a local override on shipped version N recorded N+1 and
/// became indistinguishable from the next shipped table once that shipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChargeBasisTable {
    rows: HashMap<(String, String), ChargeBasis>,
    /// Always the shipped version; kept as a field so every resolution reads one value.
    version: u32,
    /// Local override rows as applied, at most one per `(provider, method)`.
    overrides: Vec<(String, String, ChargeBasis)>,
    /// `"none"` or the digest of `overrides`; stamped on every fact.
    overrides_digest: String,
    /// The version key a config file still carries, if any. It no longer has an effect and is
    /// kept only so startup can say so once.
    ignored_version: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    billing: BillingConfig,
    /// Accepted as a convenience for config files that keep routing tables at
    /// the document root; the documented shape remains `billing.charge_basis`.
    #[serde(default)]
    charge_basis: HashMap<String, Value>,
    #[serde(default)]
    charge_basis_mapping_version: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct BillingConfig {
    /// No longer has any effect: facts record the shipped version and the override rows
    /// separately. Still accepted, with its aliases, so a config that booted before keeps
    /// booting; startup logs that it can be removed.
    #[serde(
        default,
        alias = "charge_basis_mapping_version",
        alias = "charge_basis_version"
    )]
    version: Option<u32>,
    #[serde(default, alias = "charge_basis_mapping")]
    charge_basis: HashMap<String, Value>,
}

impl ChargeBasisTable {
    /// Return a table with no provider rows.  Resolution still returns Unknown,
    /// which is useful for callers that deliberately disable shipped defaults.
    pub fn empty() -> Self {
        Self {
            rows: HashMap::new(),
            version: CHARGE_BASIS_MAPPING_VERSION,
            overrides: Vec::new(),
            overrides_digest: NO_CHARGE_BASIS_OVERRIDES.to_string(),
            ignored_version: None,
        }
    }

    /// The billing rows shipped under [`CHARGE_BASIS_MAPPING_VERSION`].
    pub fn shipped_defaults() -> Self {
        let mut table = Self::empty();
        for (method, provider) in [
            ("apikey", "anthropic"),
            ("apikey", "openai"),
            ("apikey", "deepseek"),
            ("apikey", "xai"),
            ("apikey", "cerebras"),
            ("apikey", "openrouter"),
            ("apikey", "fireworks-ai"),
            ("apikey", "google"),
            ("apikey", "typesafe"),
            ("apikey", "cloudflare-workers-ai"),
        ] {
            table.insert(provider, method, ChargeBasis::MeteredApi);
        }
        table.insert("anthropic", "oauth", ChargeBasis::SubscriptionIncluded);
        table.insert("openai", "chatgpt", ChargeBasis::SubscriptionIncluded);
        table.insert("google", "antigravity", ChargeBasis::SubscriptionIncluded);
        // Meta's Muse coding subscription uses its OAuth inference credential.
        table.insert("meta", "oauth", ChargeBasis::SubscriptionIncluded);
        // Kimi's coding plan, both regional Alibaba Token Plan endpoints, and the Ollama
        // cloud plan are flat subscriptions even though they use API-key-shaped credentials,
        // which is why this table cannot be method-only.
        // models.dev withdrew `kimi-for-coding` on 2026-09-18 and split it by plane into
        // `kimi-code-plan-global` (api.kimi.ai) and `kimi-code-plan-cn` (api.kimi.com). The
        // operator's plan is the global one. The old id is NOT kept: this table only names
        // canonical catalog providers (pinned by test), and facts already written carry the
        // basis they were classified with at their own terminal.
        table.insert(
            "kimi-code-plan-global",
            "apikey",
            ChargeBasis::SubscriptionIncluded,
        );
        for provider in ["alibaba-token-plan", "alibaba-token-plan-cn"] {
            table.insert(provider, "apikey", ChargeBasis::SubscriptionIncluded);
        }
        table.insert("ollama-cloud", "apikey", ChargeBasis::SubscriptionIncluded);
        // xAI is billed per-token on an API key and covered by a quota window under OAuth, so
        // the two methods for the same provider land on different bases -- the clearest case
        // for keying this table on the pair rather than on either half.
        table.insert("xai", "oauth", ChargeBasis::SubscriptionIncluded);
        table
    }

    /// The shipped version, carried with every resolution including Unknown.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// `"none"` when no local override rows are in effect, otherwise their digest (64
    /// lowercase hex characters). This is the value every fact carries as
    /// `charge_basis_overrides`.
    pub fn overrides_digest(&self) -> &str {
        &self.overrides_digest
    }

    /// How many local override rows are in effect.
    pub fn override_row_count(&self) -> usize {
        self.overrides.len()
    }

    /// A startup notice when the config still carries the retired version key.
    pub fn ignored_version_notice(&self) -> Option<String> {
        self.ignored_version.map(|version| {
            format!(
                "broca config sets a charge-basis mapping version ({version}); that key no \
                 longer has an effect and can be removed. Facts record the shipped version \
                 ({CHARGE_BASIS_MAPPING_VERSION}) and the override rows (charge_basis_overrides) \
                 separately"
            )
        })
    }

    /// One startup warning describing the local override rows, or None when there are none.
    ///
    /// Names each row that replaces a shipped row (with both values) and each row identical to
    /// its shipped row (which can be deleted). Rows the shipped table does not define are
    /// listed as additions. This replaces the old refusal to start: an override no longer
    /// makes facts ambiguous, because every fact carries the override digest.
    pub fn override_warning(&self) -> Option<String> {
        if self.overrides.is_empty() {
            return None;
        }
        let shipped = Self::shipped_defaults();
        let mut sorted: Vec<&(String, String, ChargeBasis)> = self.overrides.iter().collect();
        sorted.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
        let rows: Vec<String> = sorted
            .into_iter()
            .map(|(provider, method, basis)| {
                match shipped.rows.get(&(provider.clone(), method.clone())) {
                    Some(shipped_basis) if shipped_basis == basis => format!(
                        "{provider}:{method} = {} is identical to the shipped row and can be deleted",
                        basis.as_str()
                    ),
                    Some(shipped_basis) => format!(
                        "{provider}:{method} replaces shipped {} with {}",
                        shipped_basis.as_str(),
                        basis.as_str()
                    ),
                    None => format!(
                        "{provider}:{method} = {} adds a row the shipped table does not define",
                        basis.as_str()
                    ),
                }
            })
            .collect();
        Some(format!(
            "{} local charge-basis override row(s) in effect on shipped mapping version \
             {CHARGE_BASIS_MAPPING_VERSION} (facts carry charge_basis_overrides={}): {}",
            self.overrides.len(),
            self.overrides_digest,
            rows.join("; ")
        ))
    }

    /// Make this table's override identity readable by `health.check`. Only the first table
    /// published in a process is kept: the serve binary loads exactly one at startup.
    pub fn publish_for_health(&self) {
        let _ = PUBLISHED_OVERRIDES.set(PublishedOverrides {
            digest: self.overrides_digest.clone(),
            rows: self.overrides.len(),
        });
    }

    /// Resolve one provider and auth method.  An unlisted pair is never treated
    /// as covered; it stays Unknown at the table's effective version.
    pub fn resolve(&self, provider_id: &str, method: AuthMethod) -> (ChargeBasis, u32) {
        (
            self.rows
                .get(&(provider_id.to_string(), method.as_token().to_string()))
                .copied()
                .unwrap_or(ChargeBasis::Unknown),
            self.version,
        )
    }

    /// Resolve a method token from a frozen credential id without exposing the
    /// serve adapter's auth enum to the core billing module.
    pub fn resolve_token(&self, provider_id: &str, method: &str) -> (ChargeBasis, u32) {
        AuthMethod::from_token(method)
            .map(|method| self.resolve(provider_id, method))
            .unwrap_or((ChargeBasis::Unknown, self.version))
    }

    /// The charge-basis rows live in the same file as the cache rows, so this resolves the same
    /// path through the same ladder (`crate::config_home`).
    ///
    /// THIS WAS THE FOURTH RESOLVER AND I REPORTED THREE. It used to re-check `CONFIG_PATH_ENV`
    /// itself and then delegate to `CacheTtlTable::default_path()`, which checks the same
    /// variable again — so it was CORRECT BY DELEGATION rather than by design, and it inherited
    /// the XDG_CONFIG_HOME fix for free. That is the luckier outcome, not the safer one: a
    /// duplicated first arm in front of a delegated second is exactly the shape that drifts
    /// when someone adds a rung to only one of them.
    ///
    /// Found 2026-09-06 by CKTUI's sweep — every path a process resolves from the environment,
    /// including the ones that feel private — after I had audited the three I already knew
    /// about and told the fleet that was all of them.
    pub fn default_path() -> crate::config_home::ConfigHome {
        crate::config_home::config_file(CONFIG_PATH_ENV, "broca.jsonc")
    }

    /// Load shipped rows plus the optional override at the default config path.
    pub fn load_default_path() -> Result<Self, String> {
        Self::load_resolved(Self::default_path())
    }

    /// Missing billing configuration keeps shipped defaults, including explicit paths.
    pub fn load_resolved(resolved: crate::config_home::ConfigHome) -> Result<Self, String> {
        match resolved {
            crate::config_home::ConfigHome::Resolved { path, .. } => Self::load(&path),
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::shipped_defaults()),
        }
    }

    /// Load shipped rows plus billing overrides from a JSONC config file.
    /// Missing files use the shipped table; malformed present files fail loud.
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::shipped_defaults())
            }
            Err(error) => return Err(format!("read broca config {}: {error}", path.display())),
        };
        let text = String::from_utf8(bytes)
            .map_err(|error| format!("read broca config {} as utf-8: {error}", path.display()))?;
        let stripped = strip_trailing_commas(&strip_jsonc_comments(&text));
        let parsed: ConfigFile = serde_json::from_str(&stripped)
            .map_err(|error| format!("parse broca config {}: {error}", path.display()))?;

        let mut table = Self::shipped_defaults();
        // Applied in key order, so when two spellings name the same row the later key wins
        // deterministically rather than by hash-map iteration order. As before, a root-level
        // row replaces a `billing` row with the identical key.
        let mut overrides: BTreeMap<String, Value> = BTreeMap::new();
        overrides.extend(parsed.billing.charge_basis);
        overrides.extend(parsed.charge_basis);
        // Read and deliberately ignored. The version recorded on facts is always the shipped
        // one, so a config can neither raise it nor be refused for naming an older one:
        // refusing here would stop broca, and every module's LLM work with it, during an
        // unrelated upgrade.
        table.ignored_version = parsed
            .billing
            .version
            .or(parsed.charge_basis_mapping_version);
        for (key, value) in overrides {
            parse_override(&mut table, path, &key, value)?;
        }
        table.overrides_digest = charge_basis_overrides_digest(&table.overrides);
        Ok(table)
    }

    fn insert(&mut self, provider: &str, method: &str, basis: ChargeBasis) {
        self.rows
            .insert((provider.to_string(), method.to_string()), basis);
    }

    /// Apply one local override row and remember it for the digest, keeping one entry per
    /// `(provider, method)` so a row spelled twice counts once, at the value that won.
    fn insert_override(&mut self, provider: &str, method: &str, basis: ChargeBasis) {
        self.insert(provider, method, basis);
        match self
            .overrides
            .iter_mut()
            .find(|(p, m, _)| p == provider && m == method)
        {
            Some(existing) => existing.2 = basis,
            None => self
                .overrides
                .push((provider.to_string(), method.to_string(), basis)),
        }
    }
}

impl ChargeBasisResolver for ChargeBasisTable {
    fn resolve_charge_basis(&self, provider_id: &str, method: &str) -> (ChargeBasis, u32) {
        self.resolve_token(provider_id, method)
    }

    fn charge_basis_overrides(&self) -> String {
        self.overrides_digest.clone()
    }
}

/// The override identity `health.check` reports, published once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedOverrides {
    /// `"none"` or the digest, the same value facts carry as `charge_basis_overrides`.
    pub digest: String,
    /// Number of local override rows in effect.
    pub rows: usize,
}

static PUBLISHED_OVERRIDES: OnceLock<PublishedOverrides> = OnceLock::new();

/// The override identity of the table this process loaded, or None if none was published.
pub fn published_overrides() -> Option<&'static PublishedOverrides> {
    PUBLISHED_OVERRIDES.get()
}

fn parse_override(
    table: &mut ChargeBasisTable,
    path: &Path,
    key: &str,
    value: Value,
) -> Result<(), String> {
    if let Some(rows) = value.as_object() {
        for (method, basis) in rows {
            let basis = parse_basis(path, &format!("{key}:{method}"), basis)?;
            let method = AuthMethod::from_token(method).ok_or_else(|| {
                format!(
                    "broca config {}: unknown auth method '{method}' in charge-basis row '{key}'",
                    path.display()
                )
            })?;
            table.insert_override(key, method.as_token(), basis);
        }
        return Ok(());
    }

    let (provider, method) = split_pair(key).ok_or_else(|| {
        format!(
            "broca config {}: charge-basis key '{key}' must be '<method>:<provider>' or '<provider>:<method>'",
            path.display()
        )
    })?;
    let basis = parse_basis(path, key, &value)?;
    table.insert_override(provider, method, basis);
    Ok(())
}

fn parse_basis(path: &Path, key: &str, value: &Value) -> Result<ChargeBasis, String> {
    serde_json::from_value(value.clone()).map_err(|error| {
        format!(
            "broca config {}: charge-basis row '{key}' is invalid: {error}",
            path.display()
        )
    })
}

fn split_pair(key: &str) -> Option<(&str, &str)> {
    let (first, second) = key.split_once(':')?;
    if first.is_empty() || second.is_empty() {
        return None;
    }
    if AuthMethod::from_token(first).is_some() {
        Some((second, first))
    } else if AuthMethod::from_token(second).is_some() {
        Some((first, second))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    fn temp_config(contents: &str) -> PathBuf {
        let serial = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = broca_test_support::scratch_root().join(format!(
            "broca-charge-basis-{}-{serial}.jsonc",
            std::process::id()
        ));
        std::fs::write(&path, contents).expect("write test config");
        path
    }

    /// The cache rows and the charge-basis rows live in the SAME FILE, so their two resolvers
    /// must return the same path.
    ///
    /// They agreed before `config_home` existed — but BY DELEGATION: this resolver re-checked
    /// the env var and then called the cache one, inheriting whatever that ladder did. Correct,
    /// and one edit away from not being.
    ///
    /// This calls BOTH REAL RESOLVERS against the real process environment. It cannot inject,
    /// so it does not test the ladder — `config_home`'s own tests do that. What it tests is the
    /// property that was previously true only by accident: that two functions answering "where
    /// is broca.jsonc" cannot drift apart. My first attempt asserted
    /// `config_file_with(..) == config_file_with(..)` with IDENTICAL arguments, which proves the
    /// function is deterministic and nothing else — the vacuity this fleet spent two days
    /// finding, written while fixing an instance of it.
    #[test]
    fn the_charge_basis_and_cache_views_of_one_file_resolve_together() {
        assert_eq!(
            ChargeBasisTable::default_path(),
            crate::cache_routing::CacheTtlTable::default_path(),
            "charge-basis rows and cache rows share broca.jsonc; their resolvers must agree"
        );
    }

    #[test]
    fn shipped_defaults_include_confirmed_provider_specific_rows_and_unknown_fallback() {
        let table = ChargeBasisTable::shipped_defaults();
        assert_eq!(table.version(), CHARGE_BASIS_MAPPING_VERSION);
        for provider in [
            "kimi-code-plan-global",
            "alibaba-token-plan",
            "alibaba-token-plan-cn",
            "ollama-cloud",
        ] {
            assert_eq!(
                table.resolve(provider, AuthMethod::ApiKey),
                (
                    ChargeBasis::SubscriptionIncluded,
                    CHARGE_BASIS_MAPPING_VERSION
                ),
                "confirmed subscription row for {provider}"
            );
        }
        // xAI is the case that proves the table cannot be keyed on provider alone: the same
        // provider bills per-token on an api key and rides a quota window on OAuth.
        assert_eq!(
            table.resolve("xai", AuthMethod::Oauth),
            (
                ChargeBasis::SubscriptionIncluded,
                CHARGE_BASIS_MAPPING_VERSION
            )
        );
        assert_eq!(
            table.resolve("xai", AuthMethod::ApiKey),
            (ChargeBasis::MeteredApi, CHARGE_BASIS_MAPPING_VERSION)
        );
        assert_eq!(
            table.resolve("anthropic", AuthMethod::ApiKey),
            (ChargeBasis::MeteredApi, CHARGE_BASIS_MAPPING_VERSION)
        );
        assert_eq!(
            table.resolve("unlisted", AuthMethod::ApiKey),
            (ChargeBasis::Unknown, CHARGE_BASIS_MAPPING_VERSION)
        );
    }

    #[test]
    fn system_one_api_keys_resolve_metered_from_the_shipped_table() {
        let table = ChargeBasisTable::shipped_defaults();
        for provider in ["typesafe", "cloudflare-workers-ai"] {
            assert_eq!(
                table.resolve(provider, AuthMethod::ApiKey),
                (ChargeBasis::MeteredApi, CHARGE_BASIS_MAPPING_VERSION)
            );
            assert_eq!(
                table.resolve(provider, AuthMethod::Oauth),
                (ChargeBasis::Unknown, CHARGE_BASIS_MAPPING_VERSION)
            );
        }
    }

    #[test]
    fn meta_oauth_is_included_for_muse_subscription_facts() {
        let table = ChargeBasisTable::shipped_defaults();
        assert_eq!(
            table.resolve("meta", AuthMethod::Oauth),
            (
                ChargeBasis::SubscriptionIncluded,
                CHARGE_BASIS_MAPPING_VERSION
            )
        );
        let fact = fact(&table, "meta", "oauth:meta:test");
        assert_eq!(fact.basis, ChargeBasis::SubscriptionIncluded);
        assert_eq!(fact.version, CHARGE_BASIS_MAPPING_VERSION);
    }

    #[test]
    fn shipped_charge_basis_rows_use_canonical_catalog_provider_ids() {
        let report: serde_json::Value = serde_json::from_str(include_str!(
            "../../broca-catalog/data/resolution-report.json"
        ))
        .expect("vendored resolution report is valid JSON");
        let mut provider_ids = report
            .as_array()
            .expect("resolution report is an array")
            .iter()
            .filter_map(|row| row.get("provider_id").and_then(serde_json::Value::as_str))
            .collect::<std::collections::HashSet<_>>();
        // Jev is a Fusiform-curated decision model, not a models.dev chat row. Its pinned
        // route names the canonical provider even though the vendored chat report cannot.
        provider_ids.insert(crate::system_one::JEV_1_13_0.split('/').next().unwrap());

        for provider_id in ChargeBasisTable::shipped_defaults()
            .rows
            .keys()
            .map(|(provider, _)| provider)
        {
            assert!(
                provider_ids.contains(provider_id.as_str()),
                "shipped charge-basis row '{provider_id}' is not a canonical catalog provider id"
            );
        }
    }

    /// One run on `provider` authenticated with `credential_id`, finished, as WAL records.
    fn one_run(provider: &str, credential_id: &str) -> Vec<broca_wal::FramedRecord> {
        let framed = |seq, record| broca_wal::FramedRecord {
            seq,
            fence: 1,
            record,
        };
        vec![
            framed(
                1,
                broca_wal::WalRecord::RunStarted {
                    written_at_ms: None,
                    tags: None,
                    session_class: None,
                    retention: None,
                    hooks: None,
                    title: None,
                    on_restart: None,
                    keep_warm: None,
                    marker_order_version: None,
                    manifest: None,
                    scope: None,
                    send_id: None,
                    request_identity_digest: None,
                    project_id: None,
                    principal: None,
                    run_id: broca_protocol::RunId("run".into()),
                    session: broca_protocol::BindIdentity::new("/tmp/project", "harness", "s"),
                    ts_ms: Some(100),
                    config: Box::new(broca_wal::RunConfig {
                        turn_directives_v1: None,
                        render_tags: false,
                        computed_tags: None,
                        model: broca_protocol::request::ModelHandle::new(provider, "model"),
                        tools: Vec::new().into(),
                        tool_choice: Default::default(),
                        generation: Default::default(),
                        response_format: Default::default(),
                        provider_options: Value::Null,
                        cache_tiers: Value::Null,
                        context_epoch: 0,
                        renderer_version: 1,
                        frozen_render_config: Value::Null,
                        auth_selection: Some(broca_core::AuthSelection {
                            auth_method: None,
                            credential_id: credential_id.into(),
                            account_ref: None,
                            resolved_credential_id: None,
                            vault_request: None,
                            account_decision: None,
                        }),
                        tools_were_omitted: false,
                        budget: None,
                        context_limit: None,
                        work_class: None,
                        variant: None,
                        service_tier: None,
                        stop_when: Vec::new(),
                        context_management: None,
                    }),
                    input: Vec::new().into(),
                    origin: broca_wal::RunOrigin::Fresh,
                    submission_id: None,
                },
            ),
            framed(
                2,
                broca_wal::WalRecord::RunFinished {
                    interrupt_reason: None,
                    ts_ms: Some(200),
                    reason: broca_protocol::caller::RunFinishReason::Completed,
                    usage: broca_protocol::Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ]
    }

    /// The single fact the real resolver stamps for one run.
    fn fact(table: &ChargeBasisTable, provider: &str, credential_id: &str) -> SpendFact {
        let segments =
            broca_core::compute_segments(&one_run(provider, credential_id), "test", table);
        assert_eq!(segments.len(), 1);
        let segment = &segments[0];
        SpendFact {
            basis: segment.charge_basis,
            version: segment.charge_basis_mapping_version,
            overrides: segment.charge_basis_overrides.clone(),
        }
    }

    #[derive(Debug, PartialEq)]
    struct SpendFact {
        basis: ChargeBasis,
        version: u32,
        overrides: Option<String>,
    }

    fn load_str(contents: &str) -> Result<ChargeBasisTable, String> {
        let path = temp_config(contents);
        let result = ChargeBasisTable::load(&path);
        std::fs::remove_file(path).expect("remove test config");
        result
    }

    fn digest_of(contents: &str) -> String {
        load_str(contents)
            .expect("load override config")
            .overrides_digest()
            .to_string()
    }

    /// No override rows: the fact says "none" -- present, never absent -- at the shipped
    /// version, both through a config file and when no config file exists at all.
    #[test]
    fn no_overrides_stamps_none_on_the_fact_at_the_shipped_version() {
        let missing = broca_test_support::scratch_root().join(format!(
            "broca-charge-basis-missing-{}.jsonc",
            std::process::id()
        ));
        for table in [
            load_str(r#"{"billing":{}}"#).expect("load config without overrides"),
            ChargeBasisTable::load(&missing).expect("missing config uses shipped table"),
        ] {
            assert_eq!(table.override_row_count(), 0);
            assert_eq!(table.override_warning(), None);
            assert_eq!(
                fact(&table, "anthropic", "oauth:anthropic"),
                SpendFact {
                    basis: ChargeBasis::SubscriptionIncluded,
                    version: CHARGE_BASIS_MAPPING_VERSION,
                    overrides: Some("none".to_string()),
                }
            );
        }
    }

    /// Override rows change the basis they name and stamp their digest on the fact, and they
    /// do NOT raise the recorded version: that stays the shipped table's, whatever the config
    /// says. This used to record max(shipped, configured), which let a local table and a
    /// later shipped table record the same number.
    #[test]
    fn override_rows_stamp_their_digest_and_keep_the_shipped_version() {
        let table = load_str(&format!(
            r#"{{
                "billing": {{
                    "version": {above},
                    "charge_basis": {{
                        "apikey:kimi-code-plan-global": "metered_api",
                        "anthropic": {{"oauth": "metered_api"}}
                    }}
                }}
            }}"#,
            above = CHARGE_BASIS_MAPPING_VERSION + 1
        ))
        .expect("load override");
        let digest = table.overrides_digest().to_string();
        assert_ne!(digest, "none");
        assert_eq!(table.version(), CHARGE_BASIS_MAPPING_VERSION);
        assert_eq!(table.override_row_count(), 2);
        for (provider, credential) in [
            ("kimi-code-plan-global", "apikey:kimi-code-plan-global"),
            ("anthropic", "oauth:anthropic"),
        ] {
            assert_eq!(
                fact(&table, provider, credential),
                SpendFact {
                    basis: ChargeBasis::MeteredApi,
                    version: CHARGE_BASIS_MAPPING_VERSION,
                    overrides: Some(digest.clone()),
                }
            );
        }
        // An unpatched shipped row still resolves, and its fact carries the same digest: the
        // digest names the whole table in effect, not the row that was looked up.
        assert_eq!(
            fact(&table, "openai", "apikey:openai"),
            SpendFact {
                basis: ChargeBasis::MeteredApi,
                version: CHARGE_BASIS_MAPPING_VERSION,
                overrides: Some(digest),
            }
        );
    }

    /// A config written for the old rule -- `version: 5` beside an override row -- keeps
    /// booting, and its facts record the shipped version rather than 5.
    #[test]
    fn a_version_five_config_with_an_override_loads_and_records_the_shipped_version() {
        let table = load_str(
            r#"{"billing":{"version":5,"charge_basis":{"apikey:example":"subscription_included"}}}"#,
        )
        .expect("a config that booted under the old rule must still boot");
        let recorded = fact(&table, "example", "apikey:example");
        assert_eq!(recorded.version, CHARGE_BASIS_MAPPING_VERSION);
        assert_eq!(recorded.basis, ChargeBasis::SubscriptionIncluded);
        assert_eq!(
            recorded.overrides.as_deref(),
            Some(table.overrides_digest())
        );
        assert!(table
            .ignored_version_notice()
            .expect("the retired key is reported once at startup")
            .contains("no longer has an effect"));
    }

    /// Every spelling of the retired version key is accepted, at any value, with or without
    /// override rows -- including the values the old rule refused (older than shipped, equal
    /// to shipped with overrides, missing with overrides).
    #[test]
    fn the_retired_version_key_is_accepted_in_every_spelling_and_ignored() {
        let shipped = CHARGE_BASIS_MAPPING_VERSION;
        for contents in [
            r#"{"billing":{"version":1}}"#.to_string(),
            r#"{"charge_basis_mapping_version":1}"#.to_string(),
            r#"{"billing":{"charge_basis_mapping_version":1}}"#.to_string(),
            r#"{"billing":{"charge_basis_version":9}}"#.to_string(),
            r#"{"billing":{"version":1,"charge_basis":{"apikey:example":"unknown"}}}"#.to_string(),
            format!(
                r#"{{"billing":{{"version":{shipped},"charge_basis":{{"apikey:example":"unknown"}}}}}}"#
            ),
        ] {
            let table = load_str(&contents)
                .unwrap_or_else(|error| panic!("{contents} must load, got {error}"));
            assert_eq!(table.version(), shipped, "{contents}");
            assert!(table.ignored_version_notice().is_some(), "{contents}");
        }
        // Override rows with no version key at all used to be refused; now they load, and
        // there is nothing retired to report.
        let table = load_str(r#"{"billing":{"charge_basis":{"apikey:example":"unknown"}}}"#)
            .expect("override rows need no version");
        assert_eq!(table.version(), shipped);
        assert_eq!(table.ignored_version_notice(), None);
    }

    /// A malformed override row is still a startup refusal: loading around it would stamp a
    /// digest on facts for a table nobody wrote.
    #[test]
    fn a_malformed_override_row_still_stops_startup() {
        for (contents, expected) in [
            (
                r#"{"billing":{"charge_basis":{"example":{"nope":"unknown"}}}}"#,
                "unknown auth method 'nope'",
            ),
            (
                r#"{"billing":{"charge_basis":{"example:nope":"unknown"}}}"#,
                "must be '<method>:<provider>'",
            ),
            (
                r#"{"billing":{"charge_basis":{"apikey:example":"free"}}}"#,
                "charge-basis row 'apikey:example' is invalid",
            ),
        ] {
            let error = load_str(contents).expect_err("malformed row must fail");
            assert!(error.contains(expected), "{contents}: {error}");
        }
    }

    /// Asserted rather than argued from the alphabet: a real digest is 64 lowercase hex
    /// characters and can never collide with the "none" sentinel.
    #[test]
    fn an_override_digest_is_64_lowercase_hex_and_never_none() {
        let digest = digest_of(r#"{"billing":{"charge_basis":{"apikey:example":"unknown"}}}"#);
        assert_eq!(digest.len(), 64, "{digest}");
        assert!(
            digest
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "{digest}"
        );
        assert_ne!(digest, "none");
        assert_ne!(digest, NO_CHARGE_BASIS_OVERRIDES);
    }

    /// Two deployments with the same override rows get the same digest however their files
    /// order or spell those rows. The two spellings below are applied in different orders
    /// (config keys are applied in key order, and "apikey:zeta" sorts before "oauth:alpha"
    /// while "alpha" sorts before "zeta:apikey"), so this fails if the rows are hashed in
    /// application order instead of sorted.
    #[test]
    fn the_override_digest_ignores_row_order_and_spelling() {
        let first = digest_of(
            r#"{"billing":{"charge_basis":{"oauth:alpha":"metered_api","apikey:zeta":"unknown"}}}"#,
        );
        let reordered = digest_of(
            r#"{"billing":{"charge_basis":{"apikey:zeta":"unknown","oauth:alpha":"metered_api"}}}"#,
        );
        let respelled = digest_of(
            r#"{"billing":{"charge_basis":{"zeta:apikey":"unknown","alpha":{"oauth":"metered_api"}}}}"#,
        );
        assert_eq!(first, reordered);
        assert_eq!(first, respelled);
    }

    /// The control for the test above: a digest that ignored the rows entirely would pass it.
    #[test]
    fn changing_one_override_basis_changes_the_digest() {
        let before = digest_of(
            r#"{"billing":{"charge_basis":{"oauth:alpha":"metered_api","apikey:zeta":"unknown"}}}"#,
        );
        let after = digest_of(
            r#"{"billing":{"charge_basis":{"oauth:alpha":"metered_api","apikey:zeta":"metered_api"}}}"#,
        );
        assert_ne!(before, after);
    }

    /// The startup warning that replaced the refusal names each row that replaces a shipped
    /// row with both values, and each row identical to its shipped row as deletable.
    #[test]
    fn the_override_warning_names_replaced_and_redundant_rows() {
        let table = load_str(
            r#"{"billing":{"charge_basis":{
                "oauth:anthropic":"metered_api",
                "apikey:openai":"metered_api",
                "apikey:example":"unknown"
            }}}"#,
        )
        .expect("load overrides");
        let warning = table.override_warning().expect("overrides are reported");
        assert!(
            warning.contains(
                "anthropic:oauth replaces shipped subscription_included with metered_api"
            ),
            "{warning}"
        );
        assert!(
            warning.contains(
                "openai:apikey = metered_api is identical to the shipped row and can be deleted"
            ),
            "{warning}"
        );
        assert!(
            warning.contains("example:apikey = unknown adds"),
            "{warning}"
        );
        assert!(warning.contains(table.overrides_digest()), "{warning}");
        assert!(
            warning.starts_with("3 local charge-basis override row(s)"),
            "{warning}"
        );
    }

    #[test]
    fn current_version_without_overrides_is_an_allowed_no_op() {
        let path = temp_config(&format!(
            r#"{{"billing":{{"version":{CHARGE_BASIS_MAPPING_VERSION}}}}}"#
        ));
        let table = ChargeBasisTable::load(&path).expect("load current version pin");
        std::fs::remove_file(path).expect("remove test config");

        assert_eq!(table.version(), CHARGE_BASIS_MAPPING_VERSION);
        assert_eq!(table.overrides_digest(), "none");
        assert_eq!(
            table.resolve("alibaba-token-plan-cn", AuthMethod::ApiKey),
            (
                ChargeBasis::SubscriptionIncluded,
                CHARGE_BASIS_MAPPING_VERSION
            )
        );
    }
}
