//! Prompt-cache policy routing: resolve caller cache preferences into the frozen provider
//! render config used by the live executor.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use broca_catalog::live::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax, WireFamilyId};
use broca_protocol::request::{CacheMode, CacheRequest, CacheRole, CacheTail};
use broca_protocol::BindIdentity;
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

    /// Missing cache configuration keeps shipped defaults, including explicit paths.
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

    #[cfg(test)]
    fn with_marker(mut self, provider: &str, primary: CacheTtl, ephemeral: CacheTtl) -> Self {
        self.rows.insert(
            provider.to_string(),
            ProviderCacheRow::Markers {
                primary,
                ephemeral,
                also_allowed: None,
            },
        );
        self
    }
}

/// One marker provider's TTLs: the default for each role, plus any other lifetime a caller
/// may request by name.
#[derive(Debug, Clone, Copy)]
struct MarkerRow {
    primary: CacheTtl,
    ephemeral: CacheTtl,
    also_allowed: Option<CacheTtl>,
}

impl MarkerRow {
    fn allows(&self, ttl: CacheTtl) -> bool {
        ttl == self.primary || ttl == self.ephemeral || Some(ttl) == self.also_allowed
    }

    /// The allowed lifetimes for an error message, shortest first, e.g. `5m|1h`.
    fn allowed_list(&self) -> String {
        [CacheTtl::FiveMinutes, CacheTtl::OneHour]
            .into_iter()
            .filter(|ttl| self.allows(*ttl))
            .map(CacheTtl::as_str)
            .collect::<Vec<_>>()
            .join("|")
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

/// Resolve role and TTL cache preferences from the resolved wire family, provider table, and
/// bind identity.
///
/// Three request shapes:
///  - `mode: none` switches caching off: no policy, on every family.
///  - no request, or a key-only one, states no marker preference. It gets the provider's
///    default policy (see [`default_cache_policy`]); for every provider but first-party
///    Anthropic that is no policy, the shape these requests always had. A key's caller scope
///    is frozen separately at admission either way.
///  - anything else is an explicit request, resolved against the provider table.
pub fn resolve_cache_policy(
    family: WireFamilyId,
    provider_id: &str,
    request: Option<&CacheRequest>,
    table: &CacheTtlTable,
    bind: &BindIdentity,
) -> Result<Option<CachePolicy>, String> {
    if let Some(request) = request.filter(|request| request.mode == Some(CacheMode::None)) {
        if request.role.is_some() || request.ttl.is_some() || request.tail.is_some() {
            return Err(
                "cache.mode \"none\" switches prompt caching off, so it cannot be \
                        combined with cache.role, cache.ttl or cache.tail, which configure it"
                    .to_string(),
            );
        }
        return Ok(None);
    }
    let request = match request {
        Some(request)
            if !(request.key.is_some() && request.role.is_none() && request.ttl.is_none()) =>
        {
            request
        }
        // No marker preference stated. A key-only request's `tail` still describes its prompt,
        // so it reaches the default policy's placement.
        unstated => {
            return Ok(default_cache_policy(
                family,
                provider_id,
                unstated.and_then(|request| request.tail),
            ))
        }
    };

    match family {
        WireFamilyId::AnthropicMessages => resolve_marker_policy(
            family,
            provider_id,
            request,
            table,
            MarkerSyntax::AnthropicCacheControl,
        )
        .map(Some),
        WireFamilyId::BedrockConverse => resolve_marker_policy(
            family,
            provider_id,
            request,
            table,
            MarkerSyntax::BedrockCachePoint,
        )
        .map(Some),
        WireFamilyId::OpenAiChat | WireFamilyId::OpenAiResponses => {
            let Some(mut retention) = table.hints_row(provider_id) else {
                return Err(unsupported_cache_message(family, provider_id));
            };
            match request.ttl.as_deref() {
                None => {}
                // OpenAI's API-key endpoints keep a cached prefix for up to 24 hours when the
                // request says `prompt_cache_retention: "24h"`. Named explicitly, that lifetime
                // is frozen whatever the configured row says, like a marker provider's TTL.
                Some(OPENAI_EXTENDED_RETENTION) if provider_id == OPENAI_API_PROVIDER => {
                    retention = Some(OPENAI_EXTENDED_RETENTION.to_string());
                }
                Some(ttl) => {
                    return Err(format!(
                        "provider '{provider_id}' uses prompt-cache hints and does not support \
                         explicit cache ttl '{ttl}'{}",
                        if ttl == OPENAI_EXTENDED_RETENTION {
                            " (24h retention is offered only by OpenAI's own API-key endpoints)"
                        } else {
                            ""
                        }
                    ));
                }
            }
            Ok(Some(CachePolicy {
                version: CACHE_POLICY_VERSION,
                class: CacheClass::Hints {
                    cache_key: cache_key(bind),
                    retention,
                },
            }))
        }
        // The ChatGPT backend answers a request carrying `prompt_cache_retention` with HTTP 400,
        // so a 24h request there is refused at admission rather than at the first step.
        WireFamilyId::OpenAiResponsesChatgpt
            if request.ttl.as_deref() == Some(OPENAI_EXTENDED_RETENTION) =>
        {
            Err(format!(
                "cache ttl '24h' is not available for provider '{provider_id}' on the ChatGPT \
                 backend, which rejects prompt_cache_retention; it is offered only on OpenAI's \
                 API-key endpoints"
            ))
        }
        WireFamilyId::OpenAiResponsesChatgpt
        | WireFamilyId::Gemini
        | WireFamilyId::GoogleCodeAssist => Err(unsupported_cache_message(family, provider_id)),
    }
}

/// The provider id of OpenAI's own API, the one hint provider whose endpoints accept a 24h
/// prompt-cache retention.
const OPENAI_API_PROVIDER: &str = "openai";

/// The `cache.ttl` spelling, and the `prompt_cache_retention` value, of OpenAI's extended
/// retention.
const OPENAI_EXTENDED_RETENTION: &str = "24h";

/// The provider-id of first-party Anthropic, the one provider that caches by default.
const FIRST_PARTY_ANTHROPIC: &str = "anthropic";

/// The policy a send gets when it states no marker preference.
///
/// First-party Anthropic caches nothing without `cache_control` markers, so a caller who never
/// thought about caching re-sent its whole prefix at full price every step. It therefore gets
/// markers by default, as it does in other harnesses: the same four-slot planner an explicit
/// request uses, at Anthropic's default 5-minute lifetime, written without a `ttl` field.
///
/// Every other provider keeps no policy. Amazon Bedrock and third-party gateways that speak the
/// Anthropic Messages grammar are left alone on purpose: their support for, and pricing of,
/// cache markers is not the first-party API's, and a caller can still opt in explicitly.
///
/// No hint cache key is set here: the explicit marker path sets none for Anthropic either (only
/// hint families carry `cache_key(bind)`), because Anthropic keys its cache on prefix content.
pub fn default_cache_policy(
    family: WireFamilyId,
    provider_id: &str,
    tail: Option<CacheTail>,
) -> Option<CachePolicy> {
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
            tail,
            omit_ttl: true,
        },
    })
}

/// Clamp a caller scope before it is persisted in the frozen render config. The value is
/// available to all families even though only OpenAI renderers emit it.
pub fn resolve_caller_cache_key(request: Option<&CacheRequest>) -> Option<String> {
    request
        .and_then(|request| request.key.as_deref())
        .map(broca_provider::hash::clamp_prompt_cache_key)
}

/// The prompt-cache scope a fresh episode freezes into its render config.
///
/// A caller's own `cache.key` always wins. Without one, a provider the table lists in
/// `session_key_providers` gets the session id, clamped by the same rule the ChatGPT backend's
/// session-derived key uses, so every step, retry and warm of the session names one scope. A
/// resume does not call this: it re-reads the key frozen in the episode's render config.
/// `cache.mode: "none"` suppresses the session default, as it suppresses every other default
/// cache hint; it does not drop a key the caller named explicitly.
pub fn resolve_prompt_cache_key(
    provider_id: &str,
    request: Option<&CacheRequest>,
    table: &CacheTtlTable,
    bind: &BindIdentity,
) -> Option<String> {
    resolve_caller_cache_key(request).or_else(|| {
        let caching_off = request.is_some_and(|request| request.mode == Some(CacheMode::None));
        (!caching_off && table.defaults_to_session_key(provider_id))
            .then(|| broca_provider::hash::clamp_prompt_cache_key(&bind.session))
    })
}

fn resolve_marker_policy(
    family: WireFamilyId,
    provider_id: &str,
    request: &CacheRequest,
    table: &CacheTtlTable,
    syntax: MarkerSyntax,
) -> Result<CachePolicy, String> {
    let Some(row) = table.marker_row(family, provider_id) else {
        return Err(unsupported_cache_message(family, provider_id));
    };
    let role = request.role.unwrap_or(CacheRole::Primary);
    let row_ttl = match role {
        CacheRole::Primary => row.primary,
        CacheRole::Ephemeral => row.ephemeral,
    };
    let ttl = match request.ttl.as_deref() {
        Some(raw) => {
            let requested = parse_ttl(raw)?;
            if !row.allows(requested) {
                return Err(format!(
                    "cache ttl '{}' is not allowed for provider '{provider_id}' (allowed: {})",
                    requested.as_str(),
                    row.allowed_list()
                ));
            }
            requested
        }
        None => row_ttl,
    };
    Ok(CachePolicy {
        version: CACHE_POLICY_VERSION,
        class: CacheClass::Markers {
            syntax,
            ttl,
            max_breakpoints: MAX_BREAKPOINTS,
            lookback_blocks: LOOKBACK_BLOCKS,
            // Frozen here for the same reason as `ttl`: marker placement must be decided once,
            // at admission, so a resumed step reproduces the original bytes instead of
            // re-deciding from a request the resume path does not read.
            tail: request.tail,
            // An explicit request keeps writing `ttl` on every marker, so what it renders is
            // unchanged; only the no-request default (`default_cache_policy`) omits the field.
            omit_ttl: false,
        },
    })
}

fn unsupported_cache_message(family: WireFamilyId, provider_id: &str) -> String {
    format!(
        "provider '{provider_id}' with wire family {:?} does not support broca cache \
         preferences",
        family
    )
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
    use broca_protocol::request::CacheTail;

    fn req(role: Option<CacheRole>, ttl: Option<&str>) -> CacheRequest {
        CacheRequest {
            role,
            ttl: ttl.map(str::to_string),
            key: None,
            tail: None,
            mode: None,
        }
    }

    fn req_with_key(key: &str) -> CacheRequest {
        CacheRequest {
            role: None,
            ttl: None,
            key: Some(key.to_string()),
            tail: None,
            mode: None,
        }
    }

    fn bind(session: &str) -> BindIdentity {
        BindIdentity::new("/tmp/project", "runner", session)
    }

    fn marker_ttl(policy: Option<CachePolicy>) -> CacheTtl {
        match policy.expect("policy").class {
            CacheClass::Markers { ttl, .. } => ttl,
            other => panic!("expected markers, got {other:?}"),
        }
    }

    fn marker_tail(policy: Option<CachePolicy>) -> Option<CacheTail> {
        match policy.expect("policy").class {
            CacheClass::Markers { tail, .. } => tail,
            other => panic!("expected markers, got {other:?}"),
        }
    }

    /// A send with no cache request used to resolve no policy on every family. First-party
    /// Anthropic is now the one exception, because it caches nothing without markers; every
    /// other provider, including Bedrock and the third-party gateways that speak the Anthropic
    /// grammar, still resolves none and so renders the bytes it always did.
    #[test]
    fn no_request_resolves_no_policy_except_on_first_party_anthropic() {
        let table = CacheTtlTable::shipped_defaults();
        for (family, provider) in [
            (WireFamilyId::AnthropicMessages, "minimax"),
            (WireFamilyId::AnthropicMessages, "thinkingmachines"),
            (WireFamilyId::BedrockConverse, "amazon-bedrock"),
            (WireFamilyId::OpenAiChat, "openai"),
            (WireFamilyId::OpenAiResponses, "openai"),
            (WireFamilyId::OpenAiResponsesChatgpt, "openai"),
            (WireFamilyId::Gemini, "google"),
            (WireFamilyId::GoogleCodeAssist, "google"),
            // The provider id alone is not enough: only the Anthropic grammar carries markers.
            (WireFamilyId::BedrockConverse, "anthropic"),
            (WireFamilyId::OpenAiChat, "anthropic"),
        ] {
            assert_eq!(
                resolve_cache_policy(family, provider, None, &table, &bind("s1")).unwrap(),
                None,
                "{family:?}/{provider} with no caller cache request stays byte-identical"
            );
        }
    }

    #[test]
    fn no_request_on_first_party_anthropic_resolves_the_five_minute_default() {
        // The table is not consulted: a config row changing `primary` cannot move the default.
        for table in [
            CacheTtlTable::shipped_defaults(),
            CacheTtlTable::empty().with_marker("anthropic", CacheTtl::OneHour, CacheTtl::OneHour),
        ] {
            assert_eq!(
                resolve_cache_policy(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    None,
                    &table,
                    &bind("s1")
                )
                .unwrap(),
                Some(CachePolicy {
                    version: CACHE_POLICY_VERSION,
                    class: CacheClass::Markers {
                        syntax: MarkerSyntax::AnthropicCacheControl,
                        ttl: CacheTtl::FiveMinutes,
                        max_breakpoints: 4,
                        lookback_blocks: 20,
                        tail: None,
                        omit_ttl: true,
                    },
                })
            );
        }
    }

    #[test]
    fn mode_none_resolves_no_policy_on_every_family() {
        let none = CacheRequest {
            mode: Some(CacheMode::None),
            ..req(None, None)
        };
        let with_key = CacheRequest {
            key: Some("scope".into()),
            ..none.clone()
        };
        for (family, provider) in [
            (WireFamilyId::AnthropicMessages, "anthropic"),
            (WireFamilyId::BedrockConverse, "amazon-bedrock"),
            (WireFamilyId::OpenAiChat, "openai"),
            (WireFamilyId::OpenAiResponses, "openai"),
        ] {
            for request in [&none, &with_key] {
                assert_eq!(
                    resolve_cache_policy(
                        family,
                        provider,
                        Some(request),
                        &CacheTtlTable::shipped_defaults(),
                        &bind("s1")
                    )
                    .unwrap(),
                    None,
                    "{family:?}/{provider}: mode none switches caching off"
                );
            }
        }
    }

    #[test]
    fn mode_none_refuses_settings_that_configure_caching() {
        for request in [
            req(Some(CacheRole::Primary), None),
            req(None, Some("1h")),
            CacheRequest {
                tail: Some(CacheTail::Volatile),
                ..req(None, None)
            },
        ] {
            let request = CacheRequest {
                mode: Some(CacheMode::None),
                ..request
            };
            let err = resolve_cache_policy(
                WireFamilyId::AnthropicMessages,
                "anthropic",
                Some(&request),
                &CacheTtlTable::shipped_defaults(),
                &bind("s1"),
            )
            .unwrap_err();
            assert!(err.contains("cache.mode"), "got {err}");
        }
    }

    #[test]
    fn explicit_one_hour_is_still_honoured_on_anthropic() {
        for table in [CacheTtlTable::shipped_defaults(), CacheTtlTable::empty()] {
            let policy = resolve_cache_policy(
                WireFamilyId::AnthropicMessages,
                "anthropic",
                Some(&req(None, Some("1h"))),
                &table,
                &bind("s1"),
            )
            .unwrap();
            assert!(matches!(
                policy.as_ref().map(|policy| &policy.class),
                Some(CacheClass::Markers {
                    ttl: CacheTtl::OneHour,
                    omit_ttl: false,
                    ..
                })
            ));
        }
    }

    #[test]
    fn class_matrix_resolves_supported_families_only() {
        let table = CacheTtlTable::shipped_defaults();
        assert!(matches!(
            resolve_cache_policy(
                WireFamilyId::AnthropicMessages,
                "anthropic",
                Some(&req(None, None)),
                &table,
                &bind("s1")
            )
            .unwrap()
            .unwrap()
            .class,
            CacheClass::Markers {
                syntax: MarkerSyntax::AnthropicCacheControl,
                ..
            }
        ));
        assert!(matches!(
            resolve_cache_policy(
                WireFamilyId::BedrockConverse,
                "amazon-bedrock",
                Some(&req(None, None)),
                &table,
                &bind("s1")
            )
            .unwrap()
            .unwrap()
            .class,
            CacheClass::Markers {
                syntax: MarkerSyntax::BedrockCachePoint,
                ..
            }
        ));
        for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
            assert!(matches!(
                resolve_cache_policy(
                    family,
                    "openai",
                    Some(&req(None, None)),
                    &table,
                    &bind("s1")
                )
                .unwrap()
                .unwrap()
                .class,
                CacheClass::Hints {
                    retention: Some(_),
                    ..
                }
            ));
        }
        for family in [
            WireFamilyId::OpenAiResponsesChatgpt,
            WireFamilyId::Gemini,
            WireFamilyId::GoogleCodeAssist,
        ] {
            assert!(resolve_cache_policy(
                family,
                "openai",
                Some(&req(None, None)),
                &table,
                &bind("s1")
            )
            .is_err());
        }
    }

    /// What an explicit cache request on an OpenAI API-key route freezes today: the bind's hint
    /// key and the shipped row's 24h retention. Pinned exactly, so naming the lifetime in the
    /// request cannot change what the same request without one already froze.
    #[test]
    fn explicit_openai_request_freezes_the_shipped_24h_retention() {
        for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
            assert_eq!(
                resolve_cache_policy(
                    family,
                    "openai",
                    Some(&req(Some(CacheRole::Primary), None)),
                    &CacheTtlTable::shipped_defaults(),
                    &bind("s1")
                )
                .unwrap(),
                Some(CachePolicy {
                    version: CACHE_POLICY_VERSION,
                    class: CacheClass::Hints {
                        cache_key: cache_key(&bind("s1")),
                        retention: Some("24h".into()),
                    },
                }),
                "{family:?}"
            );
        }
    }

    /// `ttl: "24h"` on OpenAI's own API-key routes freezes a 24h retention, on Responses and on
    /// Chat, and even over a configured row that keeps no retention.
    #[test]
    fn openai_24h_ttl_freezes_24h_retention_on_api_key_routes() {
        let configured = CacheTtlTable {
            rows: HashMap::from([(
                "openai".to_string(),
                ProviderCacheRow::Hints { retention: None },
            )]),
            session_key_providers: BTreeSet::new(),
        };
        for table in [CacheTtlTable::shipped_defaults(), configured] {
            for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
                assert_eq!(
                    resolve_cache_policy(
                        family,
                        "openai",
                        Some(&req(None, Some("24h"))),
                        &table,
                        &bind("s1")
                    )
                    .unwrap(),
                    Some(CachePolicy {
                        version: CACHE_POLICY_VERSION,
                        class: CacheClass::Hints {
                            cache_key: cache_key(&bind("s1")),
                            retention: Some("24h".into()),
                        },
                    }),
                    "{family:?}"
                );
            }
        }
    }

    /// The ChatGPT backend rejects `prompt_cache_retention` with HTTP 400, so a 24h request on
    /// it is refused at admission, naming why.
    #[test]
    fn openai_24h_ttl_is_refused_on_the_chatgpt_backend() {
        let err = resolve_cache_policy(
            WireFamilyId::OpenAiResponsesChatgpt,
            "openai",
            Some(&req(None, Some("24h"))),
            &CacheTtlTable::shipped_defaults(),
            &bind("s1"),
        )
        .unwrap_err();
        assert!(err.contains("ChatGPT backend"), "got {err}");
        assert!(err.contains("prompt_cache_retention"), "got {err}");
    }

    /// Every provider other than OpenAI's API-key endpoints refuses a 24h lifetime.
    #[test]
    fn a_24h_ttl_is_refused_on_every_other_provider() {
        for (family, provider) in [
            (WireFamilyId::OpenAiChat, "xai"),
            (WireFamilyId::OpenAiResponses, "fireworks-ai"),
            (WireFamilyId::AnthropicMessages, "anthropic"),
            (WireFamilyId::BedrockConverse, "amazon-bedrock"),
            (WireFamilyId::Gemini, "google"),
            (WireFamilyId::GoogleCodeAssist, "google"),
        ] {
            assert!(
                resolve_cache_policy(
                    family,
                    provider,
                    Some(&req(None, Some("24h"))),
                    &CacheTtlTable::shipped_defaults(),
                    &bind("s1")
                )
                .is_err(),
                "{family:?}/{provider} must refuse a 24h ttl"
            );
        }
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

    /// Meta caches a prefix only when the request names a scope, so a Meta send without a
    /// caller key freezes its session id as the key. This default comes from the shipped table:
    /// an empty table supplies no default for Meta, the shipped table lists only Meta, and every
    /// other provider still freezes only the key its caller named.
    #[test]
    fn shipped_table_gives_meta_alone_a_session_prompt_cache_key() {
        let shipped = CacheTtlTable::shipped_defaults();
        assert_eq!(
            shipped.session_key_providers,
            BTreeSet::from(["meta".to_string()]),
            "the shipped session-key providers"
        );
        assert_eq!(
            resolve_prompt_cache_key("meta", None, &shipped, &bind("muse-session")).as_deref(),
            Some("muse-session")
        );
        assert_eq!(
            resolve_prompt_cache_key("meta", Some(&req(None, None)), &shipped, &bind("s1"))
                .as_deref(),
            Some("s1"),
            "a cache request without a key still gets the session default"
        );
        let long_session = "meta-session-".repeat(8);
        assert_eq!(
            resolve_prompt_cache_key("meta", None, &shipped, &bind(&long_session)),
            Some(broca_provider::hash::clamp_prompt_cache_key(&long_session)),
            "a long session id is clamped by the same rule as a caller key"
        );
        assert_eq!(
            resolve_prompt_cache_key(
                "meta",
                Some(&req_with_key("caller-scope")),
                &shipped,
                &bind("s1")
            )
            .as_deref(),
            Some("caller-scope"),
            "a caller-supplied scope wins over the session default"
        );
        let off = CacheRequest {
            mode: Some(CacheMode::None),
            ..req(None, None)
        };
        assert_eq!(
            resolve_prompt_cache_key("meta", Some(&off), &shipped, &bind("s1")),
            None,
            "cache.mode none sends no default cache hint"
        );
        assert_eq!(
            resolve_prompt_cache_key("meta", None, &CacheTtlTable::empty(), &bind("s1")),
            None,
            "the default comes from the table, not from the provider name"
        );
        for provider in ["openai", "xai", "fireworks-ai", "anthropic", "google"] {
            assert_eq!(
                resolve_prompt_cache_key(provider, None, &shipped, &bind("s1")),
                None,
                "{provider} keeps sending no key without a caller scope"
            );
            assert_eq!(
                resolve_prompt_cache_key(
                    provider,
                    Some(&req_with_key("caller-scope")),
                    &shipped,
                    &bind("s1")
                ),
                resolve_caller_cache_key(Some(&req_with_key("caller-scope"))),
                "{provider} freezes exactly the caller key it froze before"
            );
        }
    }

    #[test]
    fn marker_ttl_precedence_request_then_table_then_shipped_default() {
        let empty = CacheTtlTable::empty();
        assert_eq!(
            marker_ttl(
                resolve_cache_policy(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    Some(&req(None, None)),
                    &empty,
                    &bind("s1")
                )
                .unwrap()
            ),
            // Was 1h. The Anthropic primary default is now 5m, the lifetime a send with no
            // cache request also gets, so asking for caching without a TTL does not quietly
            // buy the dearer 1h write. 1h is still available by asking for it.
            CacheTtl::FiveMinutes,
            "shipped primary default applies when the table has no row"
        );

        // A configured row whose primary differs from the shipped 5m, so the assertion below
        // can only pass if an explicit request really reads the table's row.
        let table = CacheTtlTable::empty().with_marker(
            "anthropic",
            CacheTtl::OneHour,
            CacheTtl::FiveMinutes,
        );
        assert_eq!(
            marker_ttl(
                resolve_cache_policy(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    Some(&req(None, None)),
                    &table,
                    &bind("s1")
                )
                .unwrap()
            ),
            CacheTtl::OneHour,
            "table row overrides the shipped primary default"
        );

        let table = CacheTtlTable::empty().with_marker(
            "anthropic",
            CacheTtl::OneHour,
            CacheTtl::FiveMinutes,
        );
        assert_eq!(
            marker_ttl(
                resolve_cache_policy(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    Some(&req(Some(CacheRole::Primary), Some("5m"))),
                    &table,
                    &bind("s1")
                )
                .unwrap()
            ),
            CacheTtl::FiveMinutes,
            "explicit ttl wins when the row allows it"
        );
    }

    #[test]
    fn role_defaults_to_primary_and_ephemeral_selects_short_ttl() {
        let table = CacheTtlTable::empty();
        assert_eq!(
            marker_ttl(
                resolve_cache_policy(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    Some(&req(None, None)),
                    &table,
                    &bind("s1")
                )
                .unwrap()
            ),
            // Was 1h; the primary default is now 5m (see the precedence test above).
            CacheTtl::FiveMinutes
        );
        assert_eq!(
            marker_ttl(
                resolve_cache_policy(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    Some(&req(Some(CacheRole::Ephemeral), None)),
                    &table,
                    &bind("s1")
                )
                .unwrap()
            ),
            CacheTtl::FiveMinutes
        );
    }

    #[test]
    fn unsupported_prefs_and_disallowed_ttl_fail_loud() {
        let table = CacheTtlTable::shipped_defaults();
        let err = resolve_cache_policy(
            WireFamilyId::Gemini,
            "google",
            Some(&req(Some(CacheRole::Primary), None)),
            &table,
            &bind("s1"),
        )
        .unwrap_err();
        assert!(err.contains("does not support"));

        let err = resolve_cache_policy(
            WireFamilyId::BedrockConverse,
            "amazon-bedrock",
            Some(&req(None, Some("1h"))),
            &table,
            &bind("s1"),
        )
        .unwrap_err();
        assert!(err.contains("not allowed"), "got {err}");
    }

    #[test]
    fn caller_tail_declaration_is_frozen_into_the_marker_policy() {
        let table = CacheTtlTable::shipped_defaults();
        let mut request = req(None, None);
        request.tail = Some(CacheTail::Volatile);

        for (family, provider) in [
            (WireFamilyId::AnthropicMessages, "anthropic"),
            (WireFamilyId::BedrockConverse, "amazon-bedrock"),
        ] {
            assert_eq!(
                marker_tail(
                    resolve_cache_policy(family, provider, Some(&request), &table, &bind("s1"))
                        .unwrap()
                ),
                Some(CacheTail::Volatile),
                "{family:?} freezes the caller's tail declaration at admission"
            );
            assert_eq!(
                marker_tail(
                    resolve_cache_policy(
                        family,
                        provider,
                        Some(&req(None, None)),
                        &table,
                        &bind("s1")
                    )
                    .unwrap()
                ),
                None,
                "{family:?} leaves the declaration absent when the caller made none"
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
    fn config_loader_accepts_jsonc_and_overrides_rows() {
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
        assert_eq!(
            marker_ttl(
                resolve_cache_policy(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    Some(&req(None, None)),
                    &table,
                    &bind("s1")
                )
                .unwrap()
            ),
            CacheTtl::FiveMinutes
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn canonical_fireworks_provider_resolves_hint_policy() {
        let policy = resolve_cache_policy(
            WireFamilyId::OpenAiChat,
            "fireworks-ai",
            Some(&req(Some(CacheRole::Primary), None)),
            &CacheTtlTable::shipped_defaults(),
            &bind("s1"),
        )
        .unwrap()
        .unwrap();
        match policy.class {
            CacheClass::Hints {
                cache_key,
                retention,
            } => {
                assert!(!cache_key.is_empty());
                assert_eq!(retention, None);
            }
            other => panic!("expected hints, got {other:?}"),
        }
    }

    #[test]
    fn caller_key_is_clamped_without_planning_cache_policy() {
        let long_key = "alfonso-classify-cache-scope-".repeat(4);
        let request = req_with_key(&long_key);
        for family in [WireFamilyId::OpenAiChat, WireFamilyId::OpenAiResponses] {
            assert_eq!(
                resolve_cache_policy(
                    family,
                    "openai",
                    Some(&request),
                    &CacheTtlTable::shipped_defaults(),
                    &bind("different-session"),
                )
                .unwrap(),
                None,
                "{family:?} does not plan policy for a key-only request"
            );
        }
        assert_eq!(
            resolve_caller_cache_key(Some(&request)),
            Some(broca_provider::hash::clamp_prompt_cache_key(&long_key))
        );
    }

    /// A key names a cache scope; it is not a request for markers. So a key-only request resolves
    /// exactly as a send with no cache request does. That used to be "no policy" everywhere; on
    /// first-party Anthropic it is now the default policy, and the key must not take it away:
    /// otherwise adding a scope key would silently switch Anthropic caching off.
    #[test]
    fn key_only_resolves_like_no_request_and_enables_nothing_itself() {
        let table = CacheTtlTable::shipped_defaults();
        let mut keyed_tail = req_with_key("shared");
        keyed_tail.tail = Some(CacheTail::Volatile);
        assert_eq!(
            resolve_cache_policy(
                WireFamilyId::AnthropicMessages,
                "anthropic",
                Some(&req_with_key("shared")),
                &table,
                &bind("s1")
            )
            .unwrap(),
            resolve_cache_policy(
                WireFamilyId::AnthropicMessages,
                "anthropic",
                None,
                &table,
                &bind("s1")
            )
            .unwrap(),
            "first-party Anthropic keeps its default for a key-only request"
        );
        assert_eq!(
            marker_tail(
                resolve_cache_policy(
                    WireFamilyId::AnthropicMessages,
                    "anthropic",
                    Some(&keyed_tail),
                    &table,
                    &bind("s1")
                )
                .unwrap()
            ),
            Some(CacheTail::Volatile),
            "the default placement honours a key-only request's tail declaration"
        );
        for (family, provider) in [
            (WireFamilyId::AnthropicMessages, "minimax"),
            (WireFamilyId::Gemini, "google"),
            (WireFamilyId::GoogleCodeAssist, "google"),
        ] {
            assert_eq!(
                resolve_cache_policy(
                    family,
                    provider,
                    Some(&req_with_key("shared")),
                    &table,
                    &bind("s1")
                )
                .unwrap(),
                None,
                "{family:?} keeps its cache policy disabled for a key-only request"
            );
        }
    }

    #[test]
    fn codex_key_only_skips_policy_resolution() {
        let policy = resolve_cache_policy(
            WireFamilyId::OpenAiResponsesChatgpt,
            "openai",
            Some(&req_with_key("alfonso-classify-v1")),
            &CacheTtlTable::shipped_defaults(),
            &bind("s1"),
        )
        .unwrap();
        assert_eq!(policy, None);
    }

    #[test]
    fn marker_policy_still_resolves_when_role_and_ttl_accompany_a_key() {
        let request = CacheRequest {
            role: Some(CacheRole::Ephemeral),
            ttl: Some("5m".into()),
            key: Some("shared".into()),
            tail: None,
            mode: None,
        };
        assert!(matches!(
            resolve_cache_policy(
                WireFamilyId::AnthropicMessages,
                "anthropic",
                Some(&request),
                &CacheTtlTable::shipped_defaults(),
                &bind("s1"),
            )
            .unwrap()
            .expect("role and TTL still plan markers")
            .class,
            CacheClass::Markers { .. }
        ));
        assert_eq!(
            resolve_caller_cache_key(Some(&request)).as_deref(),
            Some("shared")
        );
    }
}
