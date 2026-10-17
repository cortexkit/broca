//! The Claude Code OAuth persona: the pure rules behind the request shape an Anthropic
//! subscription (OAuth) `/v1/messages` call sends, as anthropic-auth sends it.
//!
//! Everything here is a function of its arguments: no I/O, no clock, no randomness, no other
//! broca module. The only dependencies are `std`, `serde_json`, `sha2` and `xxhash-rust`, so
//! this file can move into `cortexkit-anthropic-policy` unchanged; a unit test refuses any other
//! import. The shared wire vectors (`claude-oauth-wire-vectors/v1`) pin these functions.
//!
//! The rules:
//! - the billing block: `system[0]` is a text block whose text starts with
//!   `x-anthropic-billing-header: ` and carries `cc_version` (the client version plus a 3-hex
//!   suffix derived from the first user message), `cc_entrypoint`, `cch` and optional lineage
//!   fields. It is not an HTTP header.
//! - `cch`: an xxHash64 signature over the final serialised body, written into the `cch=00000;`
//!   placeholder after every other body change.
//! - the `anthropic-beta` list, chosen from the final body's shape and the model.
//! - `metadata.user_id`, and the body's key order.

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// The Claude Code client version this persona speaks as.
pub const CLAUDE_CODE_VERSION: &str = "2.1.302";
/// The `cc_entrypoint` billing field.
pub const CC_ENTRYPOINT: &str = "cli";
/// The literal that begins the billing block's text.
pub const BILLING_HEADER_PREFIX: &str = "x-anthropic-billing-header: ";
/// The unsigned `cch` value. A body is rendered with it, then signed.
pub const CCH_PLACEHOLDER: &str = "00000";
/// The xxHash64 seed of the `cch` signature.
pub const CCH_SEED: u64 = 0x4d65_9218_e32a_3268;
/// The salt of the `cc_version` suffix hash.
const SUFFIX_SALT: &str = "59cf53e54c78";
/// The UTF-16 positions of the first user text sampled into the `cc_version` suffix.
const SUFFIX_POSITIONS: [usize; 3] = [4, 7, 20];

/// The top-level body keys the persona writes first, in this order, when present. Every other
/// key follows in lexicographic order.
pub const TOP_LEVEL_KEY_ORDER: [&str; 14] = [
    "model",
    "messages",
    "system",
    "tools",
    "tool_choice",
    "metadata",
    "max_tokens",
    "temperature",
    "thinking",
    "context_management",
    "output_config",
    "diagnostics",
    "stream",
    "speed",
];

/// The key order inside nested objects (messages, content blocks, tools, `thinking`,
/// `cache_control`, ...), matching what the persona's reference client writes: the kind first
/// (`role`, `type`), then the identifying and content fields, and `cache_control` last. Keys
/// not listed follow the listed ones in lexicographic order.
const NESTED_KEY_ORDER: [&str; 28] = [
    "role",
    "type",
    "id",
    "tool_use_id",
    "name",
    "description",
    "text",
    "thinking",
    "signature",
    "media_type",
    "data",
    "source",
    "input",
    "input_schema",
    "content",
    "is_error",
    "display",
    "budget_tokens",
    "block_binding",
    "effort",
    "format",
    "schema",
    "properties",
    "required",
    "additionalProperties",
    "output_config",
    "cache_control",
    "ttl",
];

/// The beta list for a body with tools, a system array and `thinking`, `context_management`,
/// `output_config` and `diagnostics` objects.
const FULL_AGENT_BETAS: [&str; 13] = [
    "oauth-2025-04-20",
    "interleaved-thinking-2025-05-14",
    "thinking-token-count-2026-05-13",
    "context-management-2025-06-27",
    "prompt-caching-scope-2026-01-05",
    "claude-code-20250219",
    "advisor-tool-2026-03-01",
    "advanced-tool-use-2025-11-20",
    "mid-conversation-system-2026-04-07",
    "effort-2025-11-24",
    "fallback-credit-2026-06-01",
    "extended-cache-ttl-2025-04-11",
    "cache-diagnosis-2026-04-07",
];
/// The beta list for a body asking for `json_schema` output (and not the full-agent shape).
const STRUCTURED_OUTPUT_BETAS: [&str; 8] = [
    "oauth-2025-04-20",
    "interleaved-thinking-2025-05-14",
    "thinking-token-count-2026-05-13",
    "context-management-2025-06-27",
    "prompt-caching-scope-2026-01-05",
    "advisor-tool-2026-03-01",
    "structured-outputs-2025-12-15",
    "cache-diagnosis-2026-04-07",
];
/// The beta list for every other body.
const BASE_BETAS: [&str; 9] = [
    "oauth-2025-04-20",
    "interleaved-thinking-2025-05-14",
    "thinking-token-count-2026-05-13",
    "context-management-2025-06-27",
    "prompt-caching-scope-2026-01-05",
    "advisor-tool-2026-03-01",
    "advanced-tool-use-2025-11-20",
    "extended-cache-ttl-2025-04-11",
    "cache-diagnosis-2026-04-07",
];
const MID_CONVERSATION_SYSTEM_BETA: &str = "mid-conversation-system-2026-04-07";
const PER_TURN_CONTROL_BETA: &str = "per-turn-control-2026-07-01";
const FAST_MODE_BETA: &str = "fast-mode-2026-02-01";
const SERVER_SIDE_FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";
const THINKING_BINDING_BETA: &str = "thinking-binding-controls-2026-08-01";
const THINKING_DISPLAY_UPDATES_BETA: &str = "thinking-display-updates-2026-08-18";
const MID_CONVERSATION_OUTPUT_CONFIG_BETA: &str = "mid-conversation-output-config-2026-07-01";

/// The 3-hex `cc_version` suffix for `first_user_text` under client `version`.
///
/// The characters at UTF-16 positions 4, 7 and 20 (`'0'` past the end) are hashed as
/// `SHA-256(salt + sampled + version)` and the first three lowercase hex digits are kept. The
/// positions index UTF-16 code units, because the reference client indexes a JavaScript string;
/// a sampled half of a surrogate pair is hashed as U+FFFD, as JavaScript's UTF-8 encoder writes
/// a lone surrogate.
pub fn cc_version_suffix(first_user_text: &str, version: &str) -> String {
    let units: Vec<u16> = first_user_text.encode_utf16().collect();
    let mut sampled = Vec::with_capacity(SUFFIX_POSITIONS.len());
    for position in SUFFIX_POSITIONS {
        sampled.push(units.get(position).copied().unwrap_or(u16::from(b'0')));
    }
    let sampled = String::from_utf16_lossy(&sampled);
    let digest = Sha256::digest(format!("{SUFFIX_SALT}{sampled}{version}").as_bytes());
    hex(&digest)[..3].to_string()
}

/// The text the `cc_version` suffix samples: from the first `role: "user"` message, its string
/// content, or among its text blocks the one containing `<command-name>` (from that substring
/// on), otherwise the last non-empty text block, otherwise `""`.
pub fn first_user_message_text(messages: &[Value]) -> String {
    let Some(message) = messages
        .iter()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
    else {
        return String::new();
    };
    match message.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(blocks)) => {
            let texts: Vec<&str> = blocks
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect();
            if let Some(command) = texts
                .iter()
                .find_map(|text| text.find("<command-name>").map(|at| &text[at..]))
            {
                return command.to_string();
            }
            texts
                .iter()
                .rev()
                .find(|text| !text.is_empty())
                .map_or_else(String::new, |text| (*text).to_string())
        }
        _ => String::new(),
    }
}

/// The billing block's text. `cch` is [`CCH_PLACEHOLDER`] until the body is signed. A lineage
/// field is written only when it is well formed: `previous_request_id` must match
/// `^req_[A-Za-z0-9_-]{8,128}$` and `prompt_id` must be a UUID (version 1-8, RFC variant).
pub fn billing_header_text(
    version: &str,
    suffix: &str,
    cch: &str,
    previous_request_id: Option<&str>,
    prompt_id: Option<&str>,
    prompt_ordinal: Option<u64>,
) -> String {
    let mut text = format!(
        "{BILLING_HEADER_PREFIX}cc_version={version}.{suffix}; cc_entrypoint={CC_ENTRYPOINT}; cch={cch};"
    );
    if let Some(previous) = previous_request_id.filter(|id| is_request_id(id)) {
        text.push_str(&format!(" cc_prev_req={previous};"));
    }
    if let Some(prompt) = prompt_id.filter(|id| is_prompt_uuid(id)) {
        text.push_str(&format!(" cc_prompt_id={prompt};"));
        if let Some(index) = prompt_ordinal.filter(|index| *index > 0) {
            text.push_str(&format!(
                " cc_turn_origin=human; cc_prompt_index={index}; cc_turn_index={index};"
            ));
        }
    }
    text
}

fn is_request_id(id: &str) -> bool {
    id.strip_prefix("req_").is_some_and(|rest| {
        (8..=128).contains(&rest.len())
            && rest
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    })
}

fn is_prompt_uuid(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        let ok = match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            14 => (b'1'..=b'8').contains(byte),
            19 => matches!(byte.to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b'),
            _ => byte.is_ascii_hexdigit(),
        };
        if !ok {
            return false;
        }
    }
    true
}

/// The `cch` of `canonical`: `xxHash64(canonical, CCH_SEED) & 0xFFFFF` as 5 lowercase hex
/// digits.
pub fn compute_cch(canonical: &[u8]) -> String {
    format!(
        "{:05x}",
        xxhash_rust::xxh64::xxh64(canonical, CCH_SEED) & 0xF_FFFF
    )
}

/// The bytes `cch` is computed over, from an unsigned body (its `cch` already
/// [`CCH_PLACEHOLDER`]): the top-level `model` value replaced by `""` in place, the top-level
/// `max_tokens` member removed, everything else unchanged. `None` when `body` is not a JSON
/// object.
///
/// The reference client re-serialises a parsed body; working on the text gives the same bytes
/// for a compact body that is already in that serialisation, without a parser that would
/// reorder keys.
pub fn cch_canonical_body(body: &str) -> Option<String> {
    let members = top_level_members(body)?;
    let mut canonical = String::with_capacity(body.len());
    canonical.push('{');
    let mut first = true;
    for (key, value) in members {
        let (rendered_key, rendered_value) = match key {
            "\"max_tokens\"" => continue,
            "\"model\"" => (key, "\"\""),
            _ => (key, value),
        };
        if !first {
            canonical.push(',');
        }
        first = false;
        canonical.push_str(rendered_key);
        canonical.push(':');
        canonical.push_str(rendered_value);
    }
    canonical.push('}');
    Some(canonical)
}

/// The byte offset of the five `cch` hex digits in `body`, by the reference client's rule: the
/// first match of
/// `"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=[^;"]+; cc_entrypoint=[^;"]+; cch=[0-9a-f]{5};`.
/// So the billing block must be the first system block, with keys `type` then `text`. `None`
/// when nothing matches, and the body is then sent unsigned.
pub fn cch_placeholder_offset(body: &str) -> Option<usize> {
    const MARKER: &str =
        "\"system\":[{\"type\":\"text\",\"text\":\"x-anthropic-billing-header: cc_version=";
    let mut search_from = 0;
    while let Some(found) = body[search_from..].find(MARKER) {
        let start = search_from + found;
        if let Some(offset) = match_billing_fields(body, start + MARKER.len()) {
            return Some(offset);
        }
        search_from = start + 1;
    }
    None
}

/// Match `[^;"]+; cc_entrypoint=[^;"]+; cch=[0-9a-f]{5};` at `at`, returning the offset of the
/// five hex digits.
fn match_billing_fields(body: &str, at: usize) -> Option<usize> {
    let bytes = body.as_bytes();
    let field_end = |from: usize| {
        let length = bytes[from..]
            .iter()
            .take_while(|byte| **byte != b';' && **byte != b'"')
            .count();
        (length > 0).then_some(from + length)
    };
    let after_version = field_end(at)?;
    let entrypoint = after_version + "; cc_entrypoint=".len();
    if !body[after_version..].starts_with("; cc_entrypoint=") {
        return None;
    }
    let after_entrypoint = field_end(entrypoint)?;
    if !body[after_entrypoint..].starts_with("; cch=") {
        return None;
    }
    let digits = after_entrypoint + "; cch=".len();
    let cch = bytes.get(digits..digits + 5)?;
    let hex_ok = cch
        .iter()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte));
    (hex_ok && bytes.get(digits + 5) == Some(&b';')).then_some(digits)
}

/// Sign a serialised body: locate the billing block's `cch`, reset it to
/// [`CCH_PLACEHOLDER`], hash the canonical form of that unsigned body and write the result in
/// its place. Only those five characters change. A body without a billing block is returned
/// unchanged.
pub fn sign_body(body: &str) -> String {
    let Some(offset) = cch_placeholder_offset(body) else {
        return body.to_string();
    };
    let mut unsigned = String::with_capacity(body.len());
    unsigned.push_str(&body[..offset]);
    unsigned.push_str(CCH_PLACEHOLDER);
    unsigned.push_str(&body[offset + CCH_PLACEHOLDER.len()..]);
    let Some(canonical) = cch_canonical_body(&unsigned) else {
        return body.to_string();
    };
    let cch = compute_cch(canonical.as_bytes());
    let mut signed = unsigned;
    signed.replace_range(offset..offset + CCH_PLACEHOLDER.len(), &cch);
    signed
}

/// The `metadata.user_id` value: a JSON string with `device_id`, `account_uuid` (omitted when
/// unknown) and `session_id`, in that order.
pub fn metadata_user_id(device_id: &str, account_uuid: Option<&str>, session_id: &str) -> String {
    let mut text = String::from("{\"device_id\":");
    text.push_str(&Value::from(device_id).to_string());
    if let Some(account) = account_uuid {
        text.push_str(",\"account_uuid\":");
        text.push_str(&Value::from(account).to_string());
    }
    text.push_str(",\"session_id\":");
    text.push_str(&Value::from(session_id).to_string());
    text.push('}');
    text
}

/// The `anthropic-beta` value for a final `body` sent to `model`, with the caller's own
/// `host_betas` (each may itself be a comma-separated list) merged after the persona's.
///
/// The base list is chosen by body shape (full-agent, structured output, otherwise base).
/// `per-turn-control` follows `mid-conversation-system` on Opus 5.5 and Sonnet 5.5,
/// `fast-mode` follows when `speed` is `"fast"`, then the host betas, `server-side-fallback`
/// for `fallbacks: "default"`, `thinking-binding-controls` for an enforcing
/// `thinking.block_binding`, and `mid-conversation-output-config` on Fable 5.1. Duplicates keep
/// their first position.
pub fn select_legacy_betas(body: &Value, model: &str, host_betas: &[&str]) -> String {
    let is_object = |key: &str| body.get(key).is_some_and(Value::is_object);
    let full_agent = body
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
        && body.get("system").is_some_and(Value::is_array)
        && [
            "thinking",
            "context_management",
            "output_config",
            "diagnostics",
        ]
        .into_iter()
        .all(is_object);
    let structured = body
        .pointer("/output_config/format/type")
        .and_then(Value::as_str)
        == Some("json_schema");
    let mut betas: Vec<String> = if full_agent {
        FULL_AGENT_BETAS
            .iter()
            .map(|beta| beta.to_string())
            .collect()
    } else if structured {
        STRUCTURED_OUTPUT_BETAS
            .iter()
            .map(|beta| beta.to_string())
            .collect()
    } else {
        BASE_BETAS.iter().map(|beta| beta.to_string()).collect()
    };

    let base_model = model.strip_suffix("[1m]").unwrap_or(model);
    if base_model.starts_with("claude-opus-5-5") || base_model.starts_with("claude-sonnet-5-5") {
        match betas
            .iter()
            .position(|beta| beta == MID_CONVERSATION_SYSTEM_BETA)
        {
            Some(at) => betas.insert(at + 1, PER_TURN_CONTROL_BETA.to_string()),
            None => betas.push(PER_TURN_CONTROL_BETA.to_string()),
        }
    }
    if body.get("speed").and_then(Value::as_str) == Some("fast") {
        betas.push(FAST_MODE_BETA.to_string());
    }

    for listed in host_betas {
        betas.extend(
            listed
                .split(',')
                .map(str::trim)
                .filter(|beta| !beta.is_empty())
                .map(str::to_string),
        );
    }
    if body.get("fallbacks").and_then(Value::as_str) == Some("default") {
        betas.push(SERVER_SIDE_FALLBACK_BETA.to_string());
    }
    if matches!(
        body.pointer("/thinking/block_binding/prefix_mismatch_behavior")
            .and_then(Value::as_str),
        Some("error" | "drop_block")
    ) {
        betas.push(THINKING_BINDING_BETA.to_string());
    }
    if base_model == "claude-fable-5-1" || base_model.starts_with("claude-fable-5-1-") {
        betas.push(MID_CONVERSATION_OUTPUT_CONFIG_BETA.to_string());
    }

    let mut kept: Vec<String> = Vec::with_capacity(betas.len());
    for beta in betas {
        if !kept.contains(&beta) {
            kept.push(beta);
        }
    }
    kept.join(",")
}

/// Main-request betas measured from interactive Claude Code 2.1.302 with default flags.
/// These apply to main requests regardless of body shape; auxiliary title/quota requests
/// have different lists. Advisor and fallback-credit betas require features broca does not use.
pub fn select_betas(body: &Value, model: &str, host_betas: &[&str]) -> String {
    let mut betas = vec![
        "oauth-2025-04-20",
        "interleaved-thinking-2025-05-14",
        "thinking-token-count-2026-05-13",
        "context-management-2025-06-27",
        "prompt-caching-scope-2026-01-05",
    ];
    if model.starts_with("claude-haiku-") {
        betas.push("claude-code-20250219");
    } else {
        betas.insert(0, "claude-code-20250219");
    }
    if [
        "claude-sonnet-5-5",
        "claude-opus-5-5",
        "claude-fable-5-1",
        "claude-haiku-5-5",
    ]
    .iter()
    .any(|prefix| model.starts_with(prefix))
    {
        betas.push(PER_TURN_CONTROL_BETA);
    }
    betas.extend([
        "mid-conversation-tool-changes-2026-07-01",
        "advanced-tool-use-2025-11-20",
        "mid-conversation-system-clear-at-2026-08-21",
        "effort-2025-11-24",
        THINKING_BINDING_BETA,
        THINKING_DISPLAY_UPDATES_BETA,
        "extended-cache-ttl-2025-04-11",
        "cache-diagnosis-2026-04-07",
    ]);
    if body.get("speed").and_then(Value::as_str) == Some("fast") {
        betas.push(FAST_MODE_BETA);
    }
    for listed in host_betas {
        for beta in listed
            .split(',')
            .map(str::trim)
            .filter(|beta| !beta.is_empty())
        {
            if beta != MID_CONVERSATION_SYSTEM_BETA
                && beta != MID_CONVERSATION_OUTPUT_CONFIG_BETA
                && !betas.contains(&beta)
            {
                betas.push(beta);
            }
        }
    }
    betas.join(",")
}

/// Serialise a request body in the persona's key order (see [`TOP_LEVEL_KEY_ORDER`] and the
/// nested order), compact, with JavaScript's spelling of numbers: a float with no fractional
/// part is written as an integer (`1`, not `1.0`). Strings are escaped exactly as
/// `JSON.stringify` escapes them.
pub fn to_persona_json(body: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, body, Some(&TOP_LEVEL_KEY_ORDER));
    out
}

fn write_value(out: &mut String, value: &Value, order: Option<&[&str]>) {
    match value {
        Value::Object(map) => write_object(out, map, order.unwrap_or(&NESTED_KEY_ORDER)),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_value(out, item, None);
            }
            out.push(']');
        }
        Value::Number(number) => match number.as_f64() {
            Some(float)
                if !number.is_i64()
                    && !number.is_u64()
                    && float.is_finite()
                    && float.fract() == 0.0
                    && float.abs() < 1e21 =>
            {
                out.push_str(&format!("{}", float as i128));
            }
            _ => out.push_str(&number.to_string()),
        },
        other => out.push_str(&other.to_string()),
    }
}

fn write_object(out: &mut String, map: &Map<String, Value>, order: &[&str]) {
    let mut keys: Vec<&String> = map.keys().collect();
    let rank = |key: &str| order.iter().position(|known| *known == key);
    keys.sort_by(|left, right| match (rank(left), rank(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.cmp(right),
    });
    out.push('{');
    for (index, key) in keys.into_iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&Value::from(key.as_str()).to_string());
        out.push(':');
        write_value(out, &map[key], None);
    }
    out.push('}');
}

/// The members of a top-level JSON object as `(key, value)` text slices, the key with its
/// quotes. `None` when `text` is not a single JSON object.
fn top_level_members(text: &str) -> Option<Vec<(&str, &str)>> {
    let bytes = text.as_bytes();
    let mut at = skip_whitespace(bytes, 0);
    if bytes.get(at) != Some(&b'{') {
        return None;
    }
    at += 1;
    let mut members = Vec::new();
    loop {
        at = skip_whitespace(bytes, at);
        match bytes.get(at) {
            Some(b'}') if members.is_empty() => {
                at += 1;
                break;
            }
            Some(b'"') => {}
            _ => return None,
        }
        let key_end = string_end(bytes, at)?;
        let key = &text[at..key_end];
        at = skip_whitespace(bytes, key_end);
        if bytes.get(at) != Some(&b':') {
            return None;
        }
        at = skip_whitespace(bytes, at + 1);
        let value_end = value_end(bytes, at)?;
        members.push((key, &text[at..value_end]));
        at = skip_whitespace(bytes, value_end);
        match bytes.get(at) {
            Some(b',') => at += 1,
            Some(b'}') => {
                at += 1;
                break;
            }
            _ => return None,
        }
    }
    (skip_whitespace(bytes, at) == bytes.len()).then_some(members)
}

fn skip_whitespace(bytes: &[u8], mut at: usize) -> usize {
    while bytes
        .get(at)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
    {
        at += 1;
    }
    at
}

/// The end (exclusive) of the JSON string starting at the quote at `at`.
fn string_end(bytes: &[u8], at: usize) -> Option<usize> {
    let mut index = at + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

/// The end (exclusive) of the JSON value starting at `at`.
fn value_end(bytes: &[u8], at: usize) -> Option<usize> {
    match bytes.get(at)? {
        b'"' => string_end(bytes, at),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut index = at;
            while index < bytes.len() {
                match bytes[index] {
                    b'"' => {
                        index = string_end(bytes, index)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(index + 1);
                        }
                    }
                    _ => {}
                }
                index += 1;
            }
            None
        }
        _ => {
            let length = bytes[at..]
                .iter()
                .take_while(|byte| {
                    !matches!(byte, b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
                })
                .count();
            (length > 0).then_some(at + length)
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[usize::from(byte >> 4)] as char);
        text.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    text
}

#[cfg(test)]
#[path = "claude_code_persona_tests.rs"]
mod tests;
