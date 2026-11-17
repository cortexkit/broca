//! Prompt-cache policy routing: resolve caller cache preferences into the frozen provider
//! render config used by the live executor.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use broca_catalog::live::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax, WireFamilyId};
use broca_protocol::BindIdentity;
use cortexkit_role_llm_runner::cache::{Cache, Ttl};
use serde::Deserialize;

/// The env var that overrides the module config file path.
pub const CONFIG_PATH_ENV: &str = "BROCA_CONFIG_PATH";

const CACHE_POLICY_VERSION: u32 = 1;
const MAX_BREAKPOINTS: u32 = 4;
const LOOKBACK_BLOCKS: u32 = 20;

/// Provider cache defaults loaded from config-home plus the shipped fallback rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheTtlTable {
    rows: HashMap<String, ProviderCacheRow>,
    /// Providers whose prompt cache engages only when the request names a cache scope, so a
    /// send that names none is given its session as the scope (see
    /// [`resolve_prompt_cache_key`]). Separate from `rows`: this is not a cache policy, adds no
    /// retention and admits no `role` or `ttl` preference.
    session_key_providers: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProviderCacheRow {
    Markers {
        primary: CacheTtl,
        ephemeral: CacheTtl,
        /// A lifetime a caller may name with `ttl` that neither role defaults to. The allowed
        /// set is otherwise just the two role defaults; a config-file row has none.
        also_allowed: Option<CacheTtl>,
    },
    Hints {
        retention: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    cache: CacheConfig,
}

#[derive(Debug, Default, Deserialize)]
struct CacheConfig {
    #[serde(default)]
    provider_ttl: HashMap<String, RawProviderCacheRow>,
}

#[derive(Debug, Deserialize)]
struct RawProviderCacheRow {
    primary: Option<String>,
    ephemeral: Option<String>,
    retention: Option<String>,
}

impl CacheTtlTable {
    /// A table with no user rows. Resolution still falls back to shipped provider defaults
    /// when a caller explicitly asks for cache; with no caller request the table is not
    /// consulted at all (see `default_cache_policy`).
    pub fn empty() -> Self {
        Self {
            rows: HashMap::new(),
            session_key_providers: BTreeSet::new(),
        }
    }

    /// The shipped default provider rows.
    ///
    /// These describe what a provider supports, not what any run does. A row is consulted
    /// only when a caller opts in with an explicit cache request; a send with no request gets
    /// `default_cache_policy` instead, so an unused row is inert rather than costly.
    ///
    /// Anthropic's `primary` is 5m, the same lifetime its no-request default uses, so a caller
    /// who asks for caching without naming a TTL gets the one default. A caller who wants the
    /// 1h lifetime (and its higher write price) asks for `ttl: "1h"`.
    ///
    /// That distinction is worth keeping in mind before pruning one on usage evidence: a row
    /// with no traffic behind it is an unexercised capability, and removing it turns a future
    /// caller's opt-in into an admission error rather than saving anything today.
    pub fn shipped_defaults() -> Self {
        let mut rows = HashMap::new();
        rows.insert(
            "anthropic".to_string(),
            ProviderCacheRow::Markers {
                primary: CacheTtl::FiveMinutes,
                ephemeral: CacheTtl::FiveMinutes,
                also_allowed: Some(CacheTtl::OneHour),
            },
        );
        rows.insert(
            "amazon-bedrock".to_string(),
            ProviderCacheRow::Markers {
                primary: CacheTtl::FiveMinutes,
                ephemeral: CacheTtl::FiveMinutes,
                also_allowed: None,
            },
        );
        rows.insert(
            "openai".to_string(),
            ProviderCacheRow::Hints {
                retention: Some("24h".to_string()),
            },
        );
        rows.insert(
            "xai".to_string(),
            ProviderCacheRow::Hints { retention: None },
        );
        rows.insert(
            "fireworks-ai".to_string(),
            ProviderCacheRow::Hints { retention: None },
        );
        // Meta's Responses API (api.meta.ai/v1) reuses a cached prefix only for requests that
        // carry `prompt_cache_key`. Measured on muse-spark-1.3 with one fixed 8,426-token input:
        // two sends without a key both reported `cached_tokens: 0`; with a key, the second send
        // reported 8,419 cached tokens.
        let session_key_providers = BTreeSet::from(["meta".to_string()]);
        Self {
            rows,
            session_key_providers,
        }
    }

    /// The resolved config path: `$BROCA_CONFIG_PATH` if set, else `broca.jsonc` under the
    /// shared config home (`crate::config_home`).
    ///
    /// Resolution moved out of this file on 2026-09-06. It used to walk its own ladder to
    /// `$HOME/.config/...`, as did the auth-method and vault-handle loaders — three identical
    /// copies, all missing the `XDG_CONFIG_HOME` rung the daemon honours. The result retains
    /// file-override provenance, though cache policy still allows any selected file to be absent.
    pub fn default_path() -> crate::config_home::ConfigHome {
        crate::config_home::config_file(CONFIG_PATH_ENV, "broca.jsonc")
    }

    /// Load the cache table from the default config path. A missing file keeps shipped
    /// defaults; a present-but-malformed file is a boot-time configuration error.
    pub fn load_default_path() -> Result<Self, String> {
        Self::load_resolved(Self::default_path())
    }

    /// Missing cache configuration keeps shipped defaults. The daemon refuses to boot on a
    /// missing explicit path before this runs (`config_home::require_explicit_file`).
    pub fn load_resolved(resolved: crate::config_home::ConfigHome) -> Result<Self, String> {
        match resolved {
            crate::config_home::ConfigHome::Resolved { path, .. } => Self::load(&path),
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::shipped_defaults()),
        }
    }

    /// Load and merge a config file. Missing file = shipped defaults; present malformed
    /// JSONC or unsupported values = error.
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::shipped_defaults())
            }
            Err(e) => return Err(format!("read broca config {}: {e}", path.display())),
        };
        let text = String::from_utf8(bytes)
            .map_err(|e| format!("read broca config {} as utf-8: {e}", path.display()))?;
        let stripped = strip_trailing_commas(&strip_jsonc_comments(&text));
        let parsed: ConfigFile = serde_json::from_str(&stripped)
            .map_err(|e| format!("parse broca config {}: {e}", path.display()))?;
        let mut table = Self::shipped_defaults();
        for (provider, raw) in parsed.cache.provider_ttl {
            let row = parse_provider_row(path, &provider, raw)?;
            table.rows.insert(provider, row);
        }
        Ok(table)
    }

    fn defaults_to_session_key(&self, provider: &str) -> bool {
        self.session_key_providers.contains(provider)
    }

    fn row(&self, provider: &str) -> Option<&ProviderCacheRow> {
        self.rows.get(provider)
    }

    fn marker_row(&self, family: WireFamilyId, provider: &str) -> Option<MarkerRow> {
        if let Some(ProviderCacheRow::Markers {
            primary,
            ephemeral,
            also_allowed,
        }) = self.row(provider)
        {
            return Some(MarkerRow {
                primary: *primary,
                ephemeral: *ephemeral,
                also_allowed: *also_allowed,
            });
        }
        match family {
            // Mirrors the shipped `anthropic` row, for Anthropic-grammar providers without one.
            WireFamilyId::AnthropicMessages => Some(MarkerRow {
                primary: CacheTtl::FiveMinutes,
                ephemeral: CacheTtl::FiveMinutes,
                also_allowed: Some(CacheTtl::OneHour),
            }),
            WireFamilyId::BedrockConverse => Some(MarkerRow {
                primary: CacheTtl::FiveMinutes,
                ephemeral: CacheTtl::FiveMinutes,
                also_allowed: None,
            }),
            _ => None,
        }
    }

    fn hints_row(&self, provider: &str) -> Option<Option<String>> {
        if let Some(ProviderCacheRow::Hints { retention }) = self.row(provider) {
            return Some(retention.clone());
        }
        match provider {
            "openai" => Some(Some("24h".to_string())),
            "xai" | "fireworks-ai" => Some(None),
            _ => None,
        }
    }
}

/// One marker provider's lifetimes, as its row names them: the two a config row sets (`primary`
/// and `ephemeral`), plus one more the shipped row offers. A cache policy's `short` and `long`
/// take the shortest and longest of them.
#[derive(Debug, Clone, Copy)]
struct MarkerRow {
    primary: CacheTtl,
    ephemeral: CacheTtl,
    also_allowed: Option<CacheTtl>,
}

impl MarkerRow {
    /// The lifetimes this provider offers, shortest first.
    fn offered(&self) -> impl Iterator<Item = CacheTtl> + '_ {
        [CacheTtl::FiveMinutes, CacheTtl::OneHour]
            .into_iter()
            .filter(|ttl| {
                *ttl == self.primary || *ttl == self.ephemeral || Some(*ttl) == self.also_allowed
            })
    }
}

fn parse_provider_row(
    path: &Path,
    provider: &str,
    raw: RawProviderCacheRow,
) -> Result<ProviderCacheRow, String> {
    match (raw.primary, raw.ephemeral, raw.retention) {
        (Some(primary), Some(ephemeral), None) => Ok(ProviderCacheRow::Markers {
            primary: parse_ttl(&primary).map_err(|e| {
                format!(
                    "broca config {}: provider '{provider}' primary {e}",
                    path.display()
                )
            })?,
            ephemeral: parse_ttl(&ephemeral).map_err(|e| {
                format!(
                    "broca config {}: provider '{provider}' ephemeral {e}",
                    path.display()
                )
            })?,
            also_allowed: None,
        }),
        (None, None, retention) => Ok(ProviderCacheRow::Hints { retention }),
        _ => Err(format!(
            "broca config {}: provider '{provider}' cache row must be either \
             {{primary, ephemeral}} or {{retention}}",
            path.display()
        )),
    }
}

/// Resolve a run's cache policy (llm-runner `cache`) into the policy its render config freezes,
/// by route. A policy is a request, never a requirement, so this never refuses:
///  - `ttl: none` places no markers and sends no hints;
///  - an omitted `ttl`, or no policy, takes the route's default ([`default_cache_policy`]);
///  - `short` and `long` take the shortest and longest explicit lifetime the provider's row
///    offers. A route with no explicit lifetime to choose keeps its provider's own caching.
pub fn resolve_cache_policy(
    family: WireFamilyId,
    provider_id: &str,
    cache: Option<&Cache>,
    table: &CacheTtlTable,
    bind: &BindIdentity,
) -> Option<CachePolicy> {
    let ttl = match cache.and_then(|cache| cache.ttl) {
        Some(Ttl::None) => return None,
        None => return default_cache_policy(family, provider_id),
        Some(ttl) => ttl,
    };
    let long = ttl == Ttl::Long;
    let markers = |syntax| {
        let row = table.marker_row(family, provider_id)?;
        let mut offered = row.offered();
        let ttl = if long { offered.last() } else { offered.next() }?;
        Some(CachePolicy {
            version: CACHE_POLICY_VERSION,
            class: CacheClass::Markers {
                syntax,
                ttl,
                max_breakpoints: MAX_BREAKPOINTS,
                lookback_blocks: LOOKBACK_BLOCKS,
                tail: None,
                // A requested lifetime is written on every marker; only the route default
                // leaves it to the provider.
                omit_ttl: false,
            },
        })
    };
    match family {
        WireFamilyId::AnthropicMessages => markers(MarkerSyntax::AnthropicCacheControl),
        WireFamilyId::BedrockConverse => markers(MarkerSyntax::BedrockCachePoint),
        WireFamilyId::OpenAiChat | WireFamilyId::OpenAiResponses => {
            let row = table.hints_row(provider_id)?;
            // `long` asks OpenAI's own API for its extended retention; `short` keeps the
            // provider's in-memory default, which has no provable lifetime.
            let retention = match (long, provider_id) {
                (true, OPENAI_API_PROVIDER) => Some(OPENAI_EXTENDED_RETENTION.to_owned()),
                (true, _) => row,
                (false, _) => None,
            };
            Some(CachePolicy {
                version: CACHE_POLICY_VERSION,
                class: CacheClass::Hints {
                    cache_key: cache_key(bind),
                    retention,
                },
            })
        }
        // The ChatGPT backend refuses `prompt_cache_retention`, and Gemini and Code Assist have
        // no lifetime to name here: their own caching applies.
        WireFamilyId::OpenAiResponsesChatgpt
        | WireFamilyId::Gemini
        | WireFamilyId::GoogleCodeAssist => None,
    }
}

/// The refusal a `session.send` gets for its `cache` member before anything is decoded or
/// written: `cache` beside `plan`, since a planned session's policy is `plan.cache`, or a policy
/// the contract's shape checks or broca's time-zone database refuse, each naming its field.
/// Whether the session already exists is the session's to check. `None` admits the member.
pub fn send_cache_refusal(
    params: &serde_json::Value,
) -> Option<cortexkit_role_llm_runner::cache::ErrorBody> {
    let cache = params.get("cache")?;
    if params.get("plan").is_some() {
        return Some(cortexkit_role_llm_runner::cache::ErrorBody::invalid_params(
            "cache",
            "a send carrying plan sets its cache policy as plan.cache",
        ));
    }
    cortexkit_role_llm_runner::cache::check_send_value(cache)
        .and_then(|cache| broca_fetch_plan::check_zone(&cache, "cache"))
        .err()
}

/// The provider id of OpenAI's own API, the one hint provider whose endpoints accept a 24h
/// prompt-cache retention.
const OPENAI_API_PROVIDER: &str = "openai";

/// OpenAI's extended `prompt_cache_retention`.
const OPENAI_EXTENDED_RETENTION: &str = "24h";

/// The provider-id of first-party Anthropic, the one provider that caches by default.
const FIRST_PARTY_ANTHROPIC: &str = "anthropic";

/// The policy a run gets when its cache policy names no lifetime.
///
/// First-party Anthropic caches nothing without `cache_control` markers, so a caller who never
/// thought about caching re-sent its whole prefix at full price every step. It therefore gets
/// markers by default, as it does in other harnesses: placed exactly where a requested
/// lifetime places them, at Anthropic's default 5-minute lifetime, written without a `ttl` field.
///
/// Every other provider keeps no policy. Amazon Bedrock and third-party gateways that speak the
/// Anthropic Messages grammar are left alone on purpose: their support for, and pricing of,
/// cache markers is not the first-party API's, and a caller can still ask for a lifetime.
///
/// No hint cache key is set here: the marker path sets none for Anthropic either (only hint
/// families carry `cache_key(bind)`), because Anthropic keys its cache on prefix content.
pub fn default_cache_policy(family: WireFamilyId, provider_id: &str) -> Option<CachePolicy> {
    if family != WireFamilyId::AnthropicMessages || provider_id != FIRST_PARTY_ANTHROPIC {
        return None;
    }
    Some(CachePolicy {
        version: CACHE_POLICY_VERSION,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::FiveMinutes,
            max_breakpoints: MAX_BREAKPOINTS,
            lookback_blocks: LOOKBACK_BLOCKS,
            tail: None,
            omit_ttl: true,
        },
    })
}

/// A caller's `cache.key`, clamped to what a provider's cache hint takes, before the render
/// config freezes it. Only hint routes send it.
pub fn resolve_caller_cache_key(cache: Option<&Cache>) -> Option<String> {
    cache
        .and_then(|cache| cache.key.as_deref())
        .map(broca_provider::hash::clamp_prompt_cache_key)
}

/// The prompt-cache scope a fresh run freezes into its render config.
///
/// The policy's own `key` always wins. Without one, a provider the table lists in
/// `session_key_providers` gets the session id, clamped by the same rule the ChatGPT backend's
/// session-derived key uses, so every step, retry and warm of the session names one scope. A
/// resume does not call this: it re-reads the key frozen in the run's render config.
/// `ttl: none` suppresses the session default, as it suppresses every other default cache hint;
/// it does not drop a key the policy named.
pub fn resolve_prompt_cache_key(
    provider_id: &str,
    cache: Option<&Cache>,
    table: &CacheTtlTable,
    bind: &BindIdentity,
) -> Option<String> {
    resolve_caller_cache_key(cache).or_else(|| {
        let caching_off = cache.and_then(|cache| cache.ttl) == Some(Ttl::None);
        (!caching_off && table.defaults_to_session_key(provider_id))
            .then(|| broca_provider::hash::clamp_prompt_cache_key(&bind.session))
    })
}

fn parse_ttl(raw: &str) -> Result<CacheTtl, String> {
    match raw {
        "5m" => Ok(CacheTtl::FiveMinutes),
        "1h" => Ok(CacheTtl::OneHour),
        _ => Err(format!("cache ttl '{raw}' is unsupported (expected 5m|1h)")),
    }
}

/// Derive the stable provider hint key from the bind identity.
pub fn cache_key(bind: &BindIdentity) -> String {
    let project_root = bind.project_root.to_string_lossy();
    let mut bytes = Vec::new();
    push_len_prefixed(&mut bytes, project_root.as_bytes());
    push_len_prefixed(&mut bytes, bind.harness.as_bytes());
    push_len_prefixed(&mut bytes, bind.session.as_bytes());
    format!("{:016x}", fnv1a_64(&bytes))
}

fn push_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// A small platform-stable FNV-1a 64-bit hash over `bytes`.
pub(crate) fn fnv1a_64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub(crate) fn strip_jsonc_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
            out.push(ch);
            continue;
        }

        if ch == '/' {
            match chars.peek().copied() {
                Some('/') => {
                    let _ = chars.next();
                    for c in chars.by_ref() {
                        if c == '\n' {
                            out.push('\n');
                            break;
                        }
                    }
                    continue;
                }
                Some('*') => {
                    let _ = chars.next();
                    let mut prev = '\0';
                    for c in chars.by_ref() {
                        if c == '\n' {
                            out.push('\n');
                        }
                        if prev == '*' && c == '/' {
                            break;
                        }
                        prev = c;
                    }
                    continue;
                }
                _ => {}
            }
        }
        out.push(ch);
    }
    out
}

pub(crate) fn strip_trailing_commas(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
            out.push(ch);
            continue;
        }

        if ch == ',' {
            let mut lookahead = chars.clone();
            while matches!(lookahead.peek(), Some(c) if c.is_whitespace()) {
                let _ = lookahead.next();
            }
            if matches!(lookahead.peek(), Some('}' | ']')) {
                continue;
            }
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bind(session: &str) -> BindIdentity {
        BindIdentity::new("/tmp/project", "runner", session)
    }

    fn cache(ttl: Option<Ttl>, key: Option<&str>) -> Cache {
        let mut cache = Cache::default();
        cache.ttl = ttl;
        cache.key = key.map(str::to_owned);
        cache
    }

    fn resolve(family: WireFamilyId, provider: &str, cache: Option<&Cache>) -> Option<CachePolicy> {
        resolve_cache_policy(
            family,
            provider,
            cache,
            &CacheTtlTable::shipped_defaults(),
            &bind("s1"),
        )
    }

    fn marker_ttl(policy: Option<CachePolicy>) -> (CacheTtl, bool) {
        match policy.expect("policy").class {
            CacheClass::Markers { ttl, omit_ttl, .. } => (ttl, omit_ttl),
            other => panic!("expected markers, got {other:?}"),
        }
    }

    fn retention(policy: Option<CachePolicy>) -> Option<String> {
        match policy.expect("policy").class {
            CacheClass::Hints { retention, .. } => retention,
            other => panic!("expected hints, got {other:?}"),
        }
    }

    #[test]
    fn no_policy_or_no_ttl_takes_the_route_default() {
        for policy in [None, Some(cache(None, None)), Some(cache(None, Some("k")))] {
            assert_eq!(
                marker_ttl(resolve(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    policy.as_ref()
                )),
                (CacheTtl::FiveMinutes, true),
                "first-party Anthropic gets 5m markers, lifetime left to the provider"
            );
            for (family, provider) in [
                (WireFamilyId::AnthropicMessages, "a-gateway"),
                (WireFamilyId::BedrockConverse, "amazon-bedrock"),
                (WireFamilyId::OpenAiResponses, "openai"),
                (WireFamilyId::OpenAiResponsesChatgpt, "openai"),
                (WireFamilyId::Gemini, "google"),
            ] {
                assert_eq!(
                    resolve(family, provider, policy.as_ref()),
                    None,
                    "{family:?} {provider}"
                );
            }
        }
    }

    #[test]
    fn ttl_none_places_nothing_on_every_route() {
        let none = cache(Some(Ttl::None), None);
        for (family, provider) in [
            (WireFamilyId::AnthropicMessages, "anthropic"),
            (WireFamilyId::BedrockConverse, "amazon-bedrock"),
            (WireFamilyId::OpenAiChat, "openai"),
            (WireFamilyId::OpenAiResponses, "xai"),
        ] {
            assert_eq!(
                resolve(family, provider, Some(&none)),
                None,
                "{family:?} {provider}"
            );
        }
    }

    #[test]
    fn short_and_long_take_the_shortest_and_longest_lifetime_the_row_offers() {
        let (short, long) = (cache(Some(Ttl::Short), None), cache(Some(Ttl::Long), None));
        let anthropic = |policy| {
            marker_ttl(resolve(
                WireFamilyId::AnthropicMessages,
                "anthropic",
                Some(policy),
            ))
        };
        assert_eq!(anthropic(&short), (CacheTtl::FiveMinutes, false));
        assert_eq!(anthropic(&long), (CacheTtl::OneHour, false));
        let bedrock = |policy| {
            marker_ttl(resolve(
                WireFamilyId::BedrockConverse,
                "amazon-bedrock",
                Some(policy),
            ))
        };
        assert_eq!(bedrock(&short), (CacheTtl::FiveMinutes, false));
        assert_eq!(
            bedrock(&long),
            (CacheTtl::FiveMinutes, false),
            "Bedrock offers 5m alone"
        );
    }

    #[test]
    fn openai_long_asks_its_extended_retention_and_short_keeps_in_memory() {
        let (short, long) = (cache(Some(Ttl::Short), None), cache(Some(Ttl::Long), None));
        for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
            assert_eq!(
                retention(resolve(family, "openai", Some(&long))).as_deref(),
                Some("24h")
            );
            assert_eq!(retention(resolve(family, "openai", Some(&short))), None);
            assert_eq!(
                retention(resolve(family, "xai", Some(&long))),
                None,
                "xai offers no retention"
            );
            assert_eq!(
                retention(resolve(family, "fireworks-ai", Some(&short))),
                None
            );
        }
    }

    #[test]
    fn a_route_without_a_lifetime_to_name_keeps_its_own_caching_and_never_refuses() {
        for ttl in [Ttl::Short, Ttl::Long] {
            let policy = cache(Some(ttl), None);
            for (family, provider) in [
                (WireFamilyId::OpenAiResponsesChatgpt, "openai"),
                (WireFamilyId::Gemini, "google"),
                (WireFamilyId::GoogleCodeAssist, "google"),
                (WireFamilyId::OpenAiChat, "a-provider-with-no-row"),
            ] {
                assert_eq!(
                    resolve(family, provider, Some(&policy)),
                    None,
                    "{family:?} {provider}"
                );
            }
        }
    }

    #[test]
    fn the_policy_key_wins_and_meta_alone_defaults_to_its_session() {
        let table = CacheTtlTable::shipped_defaults();
        let long_key = "alfonso-classify-cache-scope-".repeat(6);
        let keyed = cache(None, Some(&long_key));
        assert_eq!(
            resolve_prompt_cache_key("openai", Some(&keyed), &table, &bind("s1")),
            Some(broca_provider::hash::clamp_prompt_cache_key(&long_key))
        );
        assert_eq!(
            resolve_prompt_cache_key("openai", None, &table, &bind("s1")),
            None
        );
        assert_eq!(
            resolve_prompt_cache_key("meta", None, &table, &bind("s1")),
            Some(broca_provider::hash::clamp_prompt_cache_key("s1"))
        );
        let off = cache(Some(Ttl::None), None);
        assert_eq!(
            resolve_prompt_cache_key("meta", Some(&off), &table, &bind("s1")),
            None
        );
        let off_keyed = cache(Some(Ttl::None), Some("k"));
        assert_eq!(
            resolve_prompt_cache_key("meta", Some(&off_keyed), &table, &bind("s1")),
            Some("k".to_owned()),
            "ttl none drops the session default, never a key the policy named"
        );
    }

    #[test]
    fn shipped_cache_rows_use_canonical_catalog_provider_ids() {
        let report: serde_json::Value = serde_json::from_str(include_str!(
            "../../broca-catalog/data/resolution-report.json"
        ))
        .expect("vendored resolution report is valid JSON");
        let provider_ids = report
            .as_array()
            .expect("resolution report is an array")
            .iter()
            .filter_map(|row| row.get("provider_id").and_then(serde_json::Value::as_str))
            .collect::<std::collections::HashSet<_>>();

        let shipped = CacheTtlTable::shipped_defaults();
        for provider_id in shipped.rows.keys().chain(&shipped.session_key_providers) {
            assert!(
                provider_ids.contains(provider_id.as_str()),
                "shipped cache row '{provider_id}' is not a canonical catalog provider id"
            );
        }
    }

    #[test]
    fn cache_key_is_deterministic_session_scoped_and_length_prefixed() {
        let s1 = bind("s1");
        let s1_again = bind("s1");
        let s2 = bind("s2");
        assert_eq!(cache_key(&s1), cache_key(&s1_again));
        assert_ne!(cache_key(&s1), cache_key(&s2));

        let ab_c = BindIdentity::new("ab", "c", "s");
        let a_bc = BindIdentity::new("a", "bc", "s");
        assert_ne!(
            cache_key(&ab_c),
            cache_key(&a_bc),
            "length prefixes keep adjacent fields from aliasing"
        );
    }

    #[test]
    fn a_config_row_sets_what_a_provider_offers() {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-cache-config-{}-{}",
            std::process::id(),
            fnv1a_64(b"config-loader")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("broca.jsonc");
        std::fs::write(
            &path,
            r#"{
              // comments are accepted
              "cache": { "provider_ttl": {
                "anthropic": { "primary": "5m", "ephemeral": "5m" },
                "openai": { "retention": "24h" }
              } }
            }"#,
        )
        .unwrap();
        let table = CacheTtlTable::load(&path).unwrap();
        let long = cache(Some(Ttl::Long), None);
        assert_eq!(
            marker_ttl(resolve_cache_policy(
                WireFamilyId::AnthropicMessages,
                "anthropic",
                Some(&long),
                &table,
                &bind("s1")
            )),
            (CacheTtl::FiveMinutes, false),
            "a row offering 5m alone makes long 5m"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
