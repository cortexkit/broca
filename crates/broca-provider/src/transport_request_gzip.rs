//! Request-body gzip for Anthropic's first-party API, following Claude Code 2.1.302's rules.
//!
//! Claude Code compresses a request body when the request goes to `api.anthropic.com` and the
//! JSON text is at least 4,096 UTF-16 code units long. Before compressing it appends `\n` and
//! 0 to 256 random spaces and tabs, so the compressed size does not reveal the exact body
//! length. If something on the path refuses the compressed body, it sends the same request
//! once more uncompressed and stops compressing for the rest of the process.
//!
//! The encoding exists only on the wire. The transport compresses at send time, after the
//! family has rendered the body, so the rendered bytes, request dumps, digests, attempt
//! records, the WAL and resume all keep seeing the uncompressed body.

use std::{
    io::Write,
    sync::atomic::{AtomicBool, Ordering},
};

use rand::Rng;

/// The only host whose requests are compressed. A `base_url` override, a relay or a local
/// mock all have a different host and stay uncompressed, as they do in Claude Code.
pub const ANTHROPIC_API_HOST: &str = "api.anthropic.com";

/// The `accept-encoding` value Claude Code sends, in its order. Sent only to
/// [`ANTHROPIC_API_HOST`]; the client used for that host decodes each of these encodings.
pub const CLAUDE_CODE_ACCEPT_ENCODING: &str = "gzip, deflate, br, zstd";

/// Setting this environment variable to `0` turns request gzip off. Responses are still
/// decoded, because that depends only on what the server sends back.
pub const KILL_SWITCH_ENV: &str = "BROCA_ANTHROPIC_REQUEST_GZIP";

/// Claude Code compresses a body whose JSON string has at least this many UTF-16 code units
/// (JavaScript's `.length`). Non-ASCII text has fewer units than UTF-8 bytes.
pub const MIN_BODY_UTF16_UNITS: usize = 4096;

/// Claude Code appends `\n` and then `floor(random * 257)` characters, so 0 to 256.
pub const MAX_PAD: usize = 256;

/// Claude Code leaves the level to Bun, whose default deflate level is 6.
pub const GZIP_LEVEL: u32 = 6;

/// How much of a 400 body the rejection check reads, as Claude Code does.
const REJECTION_BODY_PREFIX_BYTES: usize = 8 * 1024;

/// Set the first time a compressed request is refused. Every later request in this process,
/// in every session, then goes out uncompressed until the process restarts.
static PROCESS_LATCH: AtomicBool = AtomicBool::new(false);

/// A gzip-encoded body and its sizes.
#[derive(Debug, Clone)]
pub struct GzipBody {
    /// The compressed bytes sent as the request body.
    pub wire: Vec<u8>,
    pub identity_len: usize,
    /// Count of spaces and tabs after the `\n`, so the padded text is identity + 1 + this.
    pub pad_len: usize,
    pub padded_len: usize,
}

/// The identity body followed by `\n` and 0 to 256 spaces and tabs, each picked with even
/// odds, as Claude Code pads a body before compressing it. Trailing whitespace after a JSON
/// document leaves it the same document.
pub fn pad<R: Rng + ?Sized>(identity: &[u8], rng: &mut R) -> Vec<u8> {
    let pad_len = rng.random_range(0..=MAX_PAD);
    let mut out = Vec::with_capacity(identity.len() + 1 + pad_len);
    out.extend_from_slice(identity);
    out.push(b'\n');
    for _ in 0..pad_len {
        out.push(if rng.random::<bool>() { b' ' } else { b'\t' });
    }
    out
}

/// Pad and gzip once at level 6. The gzip header is `1f8b 08 00 00000000 00 ff` (no name,
/// mtime 0, OS byte 255), the same ten bytes captures of Claude Code 2.1.302 show on an
/// ordinary body.
pub fn gzip_body<R: Rng + ?Sized>(identity: &[u8], rng: &mut R) -> std::io::Result<GzipBody> {
    let padded = pad(identity, rng);
    let mut encoder =
        flate2::GzBuilder::new().write(Vec::new(), flate2::Compression::new(GZIP_LEVEL));
    encoder.write_all(&padded)?;
    let wire = encoder.finish()?;
    Ok(GzipBody {
        wire,
        identity_len: identity.len(),
        pad_len: padded.len() - identity.len() - 1,
        padded_len: padded.len(),
    })
}

/// [`gzip_body`] with padding drawn from the thread's random generator: what each send uses.
pub fn gzip_with_fresh_padding(identity: &[u8]) -> std::io::Result<GzipBody> {
    gzip_body(identity, &mut rand::rng())
}

/// Whether the request URL's host is exactly [`ANTHROPIC_API_HOST`]. The port and scheme do
/// not matter; a subdomain, a lookalike or an unparsable URL does not qualify.
pub(crate) fn is_anthropic_api_host(url: &str) -> bool {
    reqwest::Url::parse(url).is_ok_and(|url| url.host_str() == Some(ANTHROPIC_API_HOST))
}

/// Whether the body is long enough to compress, counted in UTF-16 code units as Claude Code
/// counts its JSON string. A body that is not UTF-8 is never Claude Code JSON and stays as is.
pub(crate) fn meets_size_threshold(body: &[u8]) -> bool {
    // A UTF-16 unit never takes less than one UTF-8 byte, so a shorter body cannot qualify
    // and the count is skipped.
    if body.len() < MIN_BODY_UTF16_UNITS {
        return false;
    }
    std::str::from_utf8(body).is_ok_and(|text| text.encode_utf16().count() >= MIN_BODY_UTF16_UNITS)
}

/// Whether the kill-switch value turns request gzip off. Only `0` does, matching Claude Code's
/// `CLAUDE_CODE_GZIP_REQUEST_BODIES=0`; an unset or other value leaves gzip on.
pub(crate) fn kill_switch_disables(value: Option<&str>) -> bool {
    value.is_some_and(|value| value.trim() == "0")
}

/// Whether request gzip is on, and the latch that turns it off after a refusal. Carried by the
/// provider HTTP client so tests can use their own latch; production clients share the one
/// process latch.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RequestGzipPolicy {
    enabled: bool,
    latch: &'static AtomicBool,
}

impl RequestGzipPolicy {
    /// Gzip on unless the kill switch is set, with the process-wide latch.
    pub(crate) fn from_env() -> Self {
        Self {
            enabled: !kill_switch_disables(std::env::var(KILL_SWITCH_ENV).ok().as_deref()),
            latch: &PROCESS_LATCH,
        }
    }

    /// A policy with its own latch, so one test's refusal cannot turn gzip off for another.
    #[cfg(test)]
    pub(crate) fn isolated(enabled: bool) -> Self {
        Self {
            enabled,
            latch: Box::leak(Box::new(AtomicBool::new(false))),
        }
    }

    /// Whether this body, already known to be going to [`ANTHROPIC_API_HOST`], is compressed.
    pub(crate) fn should_gzip(&self, body: &[u8]) -> bool {
        self.enabled && !self.latch.load(Ordering::Acquire) && meets_size_threshold(body)
    }

    #[cfg(test)]
    pub(crate) fn is_latched(&self) -> bool {
        self.latch.load(Ordering::Acquire)
    }

    /// Turn gzip off for the rest of the process after a refusal, logging only the first time.
    pub(crate) fn latch(&self, status: u16, rejection: Rejection) {
        if !self.latch.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                status,
                reason = rejection.reason(),
                "a gzip-encoded request to {ANTHROPIC_API_HOST} was refused; it was re-sent \
                 uncompressed, and request gzip is off for that host until restart"
            );
        }
    }
}

/// Why a response to a compressed request counts as a refusal of the compression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rejection {
    /// A 400, 403 or 415 with neither Anthropic's `req_` request id nor a Cloudflare `cf-ray`
    /// header: something in front of Anthropic answered, not the API.
    NonAnthropicResponse,
    /// A 400 whose body says the origin could not parse the request as JSON, which is what an
    /// undecoded gzip body looks like to it.
    InvalidJsonAtOrigin,
}

impl Rejection {
    /// The names Claude Code's policy uses for the two kinds.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::NonAnthropicResponse => "non_anthropic_rejection",
            Self::InvalidJsonAtOrigin => "invalid_json_at_origin",
        }
    }
}

/// What the status and headers of a response to a compressed request say.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeaderVerdict {
    /// Not a refusal of the compression; the response is handled as usual.
    Pass,
    Rejected(Rejection),
    /// A 400 from Anthropic: only its body can tell a JSON parse failure from an ordinary
    /// invalid request.
    NeedsBody,
}

/// Classify a response to a compressed request from its status and headers.
pub(crate) fn screen_headers(status: u16, headers: &reqwest::header::HeaderMap) -> HeaderVerdict {
    if !matches!(status, 400 | 403 | 415) {
        return HeaderVerdict::Pass;
    }
    let has_request_id = headers
        .get("request-id")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("req_"));
    let has_cf_ray = headers.contains_key("cf-ray");
    let anthropic_answered = has_request_id || has_cf_ray;
    if !anthropic_answered {
        return HeaderVerdict::Rejected(Rejection::NonAnthropicResponse);
    }
    if status == 400 {
        HeaderVerdict::NeedsBody
    } else {
        HeaderVerdict::Pass
    }
}

/// Whether the first 8 KiB of a 400 body say the origin could not parse the request as JSON.
/// These are the wordings Claude Code 2.1.302's gzip policy matches.
pub(crate) fn body_shows_invalid_json(body: &str) -> bool {
    let prefix = utf8_prefix(body, REJECTION_BODY_PREFIX_BYTES);
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(prefix) {
        // Anthropic's own error shape: {"type":"error","error":{"type":…,"message":…}}.
        let anthropic_message = value
            .pointer("/error/message")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| {
                message.starts_with("The request body is not valid JSON")
                    || message.starts_with(
                        "Failed to parse request: could not parse request body as JSON",
                    )
            });
        // An origin's own shape: {"reason":"bad_json","error":"bad json: …"}.
        let bad_json = value.get("reason").and_then(serde_json::Value::as_str) == Some("bad_json")
            && value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|error| error.starts_with("bad json: "));
        return anthropic_message || bad_json;
    }
    // Plain text from a Go JSON decoder.
    prefix.starts_with("bad json: invalid character ")
        || prefix.trim_end() == "bad json: unexpected EOF"
}

/// The response verdict when the whole body is already read, as the cache-warm path reads it.
pub(crate) fn classify_rejection(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &str,
) -> Option<Rejection> {
    match screen_headers(status, headers) {
        HeaderVerdict::Pass => None,
        HeaderVerdict::Rejected(rejection) => Some(rejection),
        HeaderVerdict::NeedsBody => {
            body_shows_invalid_json(body).then_some(Rejection::InvalidJsonAtOrigin)
        }
    }
}

/// At most `limit` bytes of `text`, cut back to a character boundary.
fn utf8_prefix(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use reqwest::header::{HeaderMap, HeaderValue};
    use std::io::Read;

    fn gunzip(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(bytes)
            .read_to_end(&mut out)
            .expect("valid gzip");
        out
    }

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, HeaderValue::from_static(value));
        }
        map
    }

    #[test]
    fn gzip_round_trips_to_the_identity_body_plus_padding() {
        let identity = br#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
        for seed in 0..200u64 {
            let mut rng = StdRng::seed_from_u64(seed);
            let gz = gzip_body(identity, &mut rng).expect("gzip");
            let plain = gunzip(&gz.wire);
            assert_eq!(plain.len(), gz.padded_len);
            assert_eq!(
                &plain[..identity.len()],
                identity,
                "prefix is the identity body"
            );
            assert_eq!(
                plain[identity.len()],
                b'\n',
                "padding starts with a newline"
            );
            let tail = &plain[identity.len() + 1..];
            assert_eq!(tail.len(), gz.pad_len);
            assert!(tail.iter().all(|b| *b == b' ' || *b == b'\t'));
            assert_eq!(gz.identity_len, identity.len());
            // Still the same JSON document once parsed.
            let parsed: serde_json::Value = serde_json::from_slice(&plain).expect("json");
            let original: serde_json::Value = serde_json::from_slice(identity).unwrap();
            assert_eq!(parsed, original);
        }
    }

    #[test]
    fn gzip_header_matches_claude_codes_runtime_header() {
        let gz = gzip_body(b"{}", &mut StdRng::seed_from_u64(7)).expect("gzip");
        assert_eq!(
            &gz.wire[..10],
            &[0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0xff],
            "no flags, mtime 0, XFL 0 at level 6, OS byte 255"
        );
    }

    #[test]
    fn pad_length_stays_in_bounds_and_reaches_both_ends_of_its_range() {
        let mut rng = StdRng::seed_from_u64(42);
        let (mut min, mut max) = (usize::MAX, 0);
        let (mut spaces, mut tabs) = (0usize, 0usize);
        for _ in 0..20_000 {
            let padded = pad(b"{}", &mut rng);
            assert_eq!(&padded[..3], b"{}\n");
            let pad_len = padded.len() - 3;
            assert!(pad_len <= MAX_PAD, "pad {pad_len} exceeds {MAX_PAD}");
            min = min.min(pad_len);
            max = max.max(pad_len);
            spaces += padded[3..].iter().filter(|b| **b == b' ').count();
            tabs += padded[3..].iter().filter(|b| **b == b'\t').count();
        }
        assert_eq!(min, 0, "an empty pad must be possible");
        assert_eq!(max, MAX_PAD, "a full 256-character pad must be possible");
        assert!(spaces > 0 && tabs > 0, "both padding characters appear");
    }

    #[test]
    fn only_the_exact_first_party_host_qualifies() {
        for url in [
            "https://api.anthropic.com/v1/messages",
            "https://API.Anthropic.com/v1/messages",
            "http://api.anthropic.com:8443/v1/messages?beta=true",
        ] {
            assert!(is_anthropic_api_host(url), "{url} is the first-party host");
        }
        for url in [
            "https://eu.api.anthropic.com/v1/messages",
            "https://api.anthropic.com.relay.example/v1/messages",
            "https://anthropic.com/v1/messages",
            "https://relay.example/api.anthropic.com/v1/messages",
            "http://127.0.0.1:8080/v1/messages",
            "https://api.openai.com/v1/chat/completions",
            "not a url",
        ] {
            assert!(!is_anthropic_api_host(url), "{url} must not be gzipped");
        }
    }

    #[test]
    fn the_size_gate_counts_utf16_units_from_exactly_4096() {
        assert!(!meets_size_threshold(&[b'x'; MIN_BODY_UTF16_UNITS - 1]));
        assert!(meets_size_threshold(&[b'x'; MIN_BODY_UTF16_UNITS]));
        assert!(meets_size_threshold(&vec![b'x'; 40_774]));
        // `é` is two UTF-8 bytes but one UTF-16 unit: 4,095 of them are 8,190 bytes and still
        // one unit short.
        let accented = "é".repeat(MIN_BODY_UTF16_UNITS - 1);
        assert!(!meets_size_threshold(accented.as_bytes()));
        assert!(meets_size_threshold(format!("{accented}x").as_bytes()));
        // A character outside the Basic Multilingual Plane is two UTF-16 units.
        let emoji = "😀".repeat(MIN_BODY_UTF16_UNITS / 2);
        assert!(meets_size_threshold(emoji.as_bytes()));
        assert!(!meets_size_threshold(&[0xff; MIN_BODY_UTF16_UNITS * 2]));
    }

    #[test]
    fn only_zero_turns_the_kill_switch_on() {
        assert!(kill_switch_disables(Some("0")));
        assert!(kill_switch_disables(Some(" 0\n")));
        assert!(!kill_switch_disables(None));
        assert!(!kill_switch_disables(Some("1")));
        assert!(!kill_switch_disables(Some("")));
        let off = RequestGzipPolicy {
            enabled: false,
            latch: Box::leak(Box::new(AtomicBool::new(false))),
        };
        assert!(!off.should_gzip(&[b'x'; 8192]));
    }

    #[test]
    fn a_refusal_from_in_front_of_anthropic_is_a_gzip_rejection() {
        for status in [400, 403, 415] {
            assert_eq!(
                screen_headers(status, &HeaderMap::new()),
                HeaderVerdict::Rejected(Rejection::NonAnthropicResponse),
                "{status} without request-id or cf-ray"
            );
            // A request id that is not Anthropic's does not count as Anthropic answering.
            assert_eq!(
                screen_headers(status, &headers(&[("request-id", "abc123")])),
                HeaderVerdict::Rejected(Rejection::NonAnthropicResponse),
            );
            assert_eq!(
                classify_rejection(status, &HeaderMap::new(), "Bad Request"),
                Some(Rejection::NonAnthropicResponse)
            );
        }
    }

    #[test]
    fn an_anthropic_400_with_a_req_id_is_not_a_gzip_rejection() {
        let ordinary = r#"{"type":"error","error":{"type":"invalid_request_error","message":"messages: field required"},"request_id":"req_011Cg89y"}"#;
        let anthropic = headers(&[("request-id", "req_011Cg89y")]);
        assert_eq!(screen_headers(400, &anthropic), HeaderVerdict::NeedsBody);
        assert_eq!(classify_rejection(400, &anthropic, ordinary), None);
        let cloudflare = headers(&[("cf-ray", "8f1e2d3c4b5a6978-SJC")]);
        assert_eq!(classify_rejection(400, &cloudflare, ordinary), None);
        // A 403 or 415 that Anthropic or Cloudflare answered passes through without a body check.
        assert_eq!(screen_headers(403, &anthropic), HeaderVerdict::Pass);
        assert_eq!(screen_headers(415, &cloudflare), HeaderVerdict::Pass);
        // Other statuses never count, with or without the headers.
        for status in [200, 401, 404, 413, 429, 500, 529] {
            assert_eq!(
                screen_headers(status, &HeaderMap::new()),
                HeaderVerdict::Pass
            );
        }
    }

    #[test]
    fn a_400_saying_the_body_is_not_json_is_a_gzip_rejection() {
        let anthropic = headers(&[("request-id", "req_011Cg89y")]);
        for body in [
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"The request body is not valid JSON: expected value at line 1 column 1"}}"#,
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"Failed to parse request: could not parse request body as JSON"}}"#,
            r#"{"reason":"bad_json","error":"bad json: invalid character '\\x1f' looking for beginning of value"}"#,
            "bad json: invalid character '\\x1f' looking for beginning of value",
            "bad json: unexpected EOF\n",
        ] {
            assert!(body_shows_invalid_json(body), "{body}");
            assert_eq!(
                classify_rejection(400, &anthropic, body),
                Some(Rejection::InvalidJsonAtOrigin),
                "{body}"
            );
        }
        // The same wording on a 403 is not a rejection when Anthropic answered.
        assert_eq!(
            classify_rejection(403, &anthropic, "bad json: unexpected EOF"),
            None
        );
        for body in [
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: must be positive"}}"#,
            r#"{"reason":"bad_json","error":"something else"}"#,
            r#"{"reason":"other","error":"bad json: invalid character 'x'"}"#,
            "bad json: unexpected EOF, then more",
            "<html>Bad Request</html>",
            "",
        ] {
            assert!(!body_shows_invalid_json(body), "{body}");
        }
        // Only the first 8 KiB are read: a body that matches only when read whole is not seen.
        let late = format!(
            r#"{{"reason":"bad_json","padding":"{}","error":"bad json: invalid character 'x'"}}"#,
            "x".repeat(9000)
        );
        assert!(!body_shows_invalid_json(&late));
        assert!(body_shows_invalid_json(
            &late.replace(&"x".repeat(9000), "x")
        ));
    }

    #[test]
    fn the_latch_turns_gzip_off_and_stays_set() {
        let policy = RequestGzipPolicy::isolated(true);
        let body = [b'x'; 8192];
        assert!(policy.should_gzip(&body));
        assert!(
            !policy.should_gzip(&[b'x'; 100]),
            "short bodies stay identity"
        );
        policy.latch(415, Rejection::NonAnthropicResponse);
        assert!(policy.is_latched());
        assert!(!policy.should_gzip(&body));
        policy.latch(400, Rejection::InvalidJsonAtOrigin);
        assert!(policy.is_latched());
        // Another policy's latch is untouched, so isolated tests cannot leak into each other.
        assert!(RequestGzipPolicy::isolated(true).should_gzip(&body));
    }
}
