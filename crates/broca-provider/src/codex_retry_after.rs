// Retry-time parsing reproduced from OpenAI Codex, revision af91f6ef2c (Apache-2.0).
// Copyright 2025 OpenAI. See THIRD-PARTY-NOTICES.md and LICENSES/Apache-2.0.txt.

//! Where the two OpenAI Responses families (`openai_responses` and `openai_responses_chatgpt`)
//! read the server's retry time, exactly as Codex reads it:
//!
//! - the `Retry-After` header, as whole delay-seconds or an HTTP date, and nothing else
//!   (`http-client/src/retry_after.rs:19-39`). Codex does not read `retry-after-ms` or any
//!   `x-ratelimit-*` header for a Responses request;
//! - a `Retry-After` inside the `headers` object a streamed `response.failed` error carries
//!   (`codex-api/src/sse/responses_error.rs:28-31`);
//! - for a streamed `rate_limit_exceeded` or `slow_down` error without that header, the
//!   "try again in 11.054s" phrase in its message (`codex-api/src/sse/responses_error.rs:90-143`).
//!
//! Every other family keeps the producer-wide parser in the transport, which also reads
//! `retry-after-ms`.
//!
//! This module is outside the `live` feature because the Responses stream normalizer, which is
//! not live-only, calls it. The HTTP-date form needs the `live` feature's `httpdate`; without it
//! only delay-seconds are read, and nothing outside the live transport acts on the value.

use serde_json::Value;
use std::time::{Duration, SystemTime};

/// The delay a `Retry-After` value asks for, in seconds from `now`, read the way Codex reads it
/// (`http-client/src/retry_after.rs:25-39`): a value made only of ASCII digits is whole seconds;
/// anything else must be an HTTP date, and a date already past is zero. A fractional or negative
/// number is neither, so it gives no retry time. A delay too large to add to the current
/// instant also gives none, as `Instant::checked_add` does there.
pub(crate) fn retry_after_value(value: &str, now: SystemTime) -> Option<f64> {
    let value = value.trim();
    let delay = if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        Duration::from_secs(value.parse().ok()?)
    } else {
        http_date_delay(value, now)?
    };
    std::time::Instant::now().checked_add(delay)?;
    Some(delay.as_secs_f64())
}

#[cfg(feature = "live")]
fn http_date_delay(value: &str, now: SystemTime) -> Option<Duration> {
    Some(
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(now)
            .unwrap_or_default(),
    )
}

#[cfg(not(feature = "live"))]
fn http_date_delay(_value: &str, _now: SystemTime) -> Option<Duration> {
    None
}

/// The retry time in a JSON `headers` object a streamed error or a wrapped socket error frame
/// carries. Header names match case-insensitively, and a string, number or boolean value is read
/// as its text, as Codex converts these objects into HTTP headers
/// (`codex-api/src/responses_headers.rs:10-32`).
pub(crate) fn json_headers_retry_after(headers: &Value, now: SystemTime) -> Option<f64> {
    let value = headers
        .as_object()?
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
        .find_map(|(_, value)| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })?;
    retry_after_value(&value, now)
}

/// The retry time a streamed `response.failed` error states, in seconds
/// (`codex-api/src/sse/responses_error.rs:26-103`): the `Retry-After` in its `headers` object
/// first; then, only for `rate_limit_exceeded` and `slow_down`, a "try again in" phrase in its
/// message.
pub(crate) fn failed_response_retry_after(error: &Value, now: SystemTime) -> Option<f64> {
    if let Some(seconds) = error
        .get("headers")
        .and_then(|headers| json_headers_retry_after(headers, now))
    {
        return Some(seconds);
    }
    match error.get("code").and_then(Value::as_str) {
        Some("rate_limit_exceeded" | "slow_down") => error
            .get("message")
            .and_then(Value::as_str)
            .and_then(try_again_in_delay),
        _ => None,
    }
}

/// The delay in a "try again in <number><unit>" phrase, in seconds. This hand-codes Codex's
/// `(?i)try again in\s*(\d+(?:\.\d+)?)\s*(s|ms|seconds?)` (`codex-api/src/sse/responses_error.rs:
/// 105-143`): the first occurrence that matches wins, `s` and `seconds` are seconds, and `ms`
/// is whole milliseconds (Codex truncates the number to an integer for that unit).
pub(crate) fn try_again_in_delay(message: &str) -> Option<f64> {
    const PHRASE: &[u8] = b"try again in";
    let bytes = message.as_bytes();
    let mut start = 0;
    while start + PHRASE.len() <= bytes.len() {
        if bytes[start..start + PHRASE.len()].eq_ignore_ascii_case(PHRASE) {
            if let Some(seconds) = delay_after_phrase(&bytes[start + PHRASE.len()..]) {
                return Some(seconds);
            }
        }
        start += 1;
    }
    None
}

fn delay_after_phrase(rest: &[u8]) -> Option<f64> {
    let skip_space = |at: usize| {
        at + rest[at..]
            .iter()
            .take_while(|byte| byte.is_ascii_whitespace())
            .count()
    };
    let digits = |at: usize| {
        rest[at..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count()
    };
    let number_start = skip_space(0);
    let whole = digits(number_start);
    if whole == 0 {
        return None;
    }
    let mut number_end = number_start + whole;
    if rest.get(number_end) == Some(&b'.') {
        let fraction = digits(number_end + 1);
        if fraction > 0 {
            number_end += 1 + fraction;
        }
    }
    let value: f64 = std::str::from_utf8(&rest[number_start..number_end])
        .ok()?
        .parse()
        .ok()?;
    let unit_start = skip_space(number_end);
    let unit = &rest[unit_start..];
    let starts_with =
        |word: &[u8]| unit.len() >= word.len() && unit[..word.len()].eq_ignore_ascii_case(word);
    // The regex tries `s` before `ms`, so "s..." (including "seconds") is seconds.
    if starts_with(b"s") {
        return Duration::try_from_secs_f64(value)
            .ok()
            .map(|delay| delay.as_secs_f64());
    }
    if starts_with(b"ms") {
        return Some(Duration::from_millis(value as u64).as_secs_f64());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_reads_whole_seconds_and_rejects_other_numbers() {
        let now = SystemTime::now();
        assert_eq!(retry_after_value("469", now), Some(469.0));
        assert_eq!(retry_after_value(" 7 ", now), Some(7.0));
        assert_eq!(retry_after_value("1.5", now), None);
        assert_eq!(retry_after_value("-3", now), None);
        assert_eq!(retry_after_value("", now), None);
    }

    #[cfg(feature = "live")]
    #[test]
    fn retry_after_reads_an_http_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let later = httpdate::fmt_http_date(now + Duration::from_secs(120));
        assert_eq!(retry_after_value(&later, now), Some(120.0));
        let earlier = httpdate::fmt_http_date(now - Duration::from_secs(5));
        assert_eq!(retry_after_value(&earlier, now), Some(0.0));
    }

    #[test]
    fn json_headers_match_retry_after_case_insensitively() {
        let now = SystemTime::now();
        let headers = serde_json::json!({"Retry-After": "12", "retry-after-ms": "5"});
        assert_eq!(json_headers_retry_after(&headers, now), Some(12.0));
        let numeric = serde_json::json!({"retry-after": 3});
        assert_eq!(json_headers_retry_after(&numeric, now), Some(3.0));
        let millis_only = serde_json::json!({"retry-after-ms": "5000"});
        assert_eq!(json_headers_retry_after(&millis_only, now), None);
    }

    #[test]
    fn failed_response_prefers_headers_then_rate_limit_message() {
        let now = SystemTime::now();
        let with_header =
            serde_json::json!({"code":"server_is_overloaded","headers":{"retry-after":"9"}});
        assert_eq!(failed_response_retry_after(&with_header, now), Some(9.0));
        let message = serde_json::json!({"code":"rate_limit_exceeded","message":"Rate limit reached. Please try again in 11.054s. Visit ..."});
        assert_eq!(failed_response_retry_after(&message, now), Some(11.054));
        let slow = serde_json::json!({"code":"slow_down","message":"try again in 120ms"});
        assert_eq!(failed_response_retry_after(&slow, now), Some(0.12));
        // Only the two rate-limit codes read the message.
        let other = serde_json::json!({"code":"server_error","message":"try again in 3s"});
        assert_eq!(failed_response_retry_after(&other, now), None);
    }

    #[test]
    fn try_again_in_follows_the_codex_pattern() {
        assert_eq!(try_again_in_delay("Try Again In 2 Seconds"), Some(2.0));
        assert_eq!(try_again_in_delay("try again in 1.5ms"), Some(0.001));
        assert_eq!(try_again_in_delay("try again in 5 minutes"), None);
        assert_eq!(
            try_again_in_delay("try again in a bit; try again in 4s"),
            Some(4.0)
        );
        assert_eq!(try_again_in_delay("no hint"), None);
    }
}
