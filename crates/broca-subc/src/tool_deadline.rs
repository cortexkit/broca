//! How long broca waits for ONE tool call's terminal reply.
//!
//! Most tool calls answer inside [`TOOL_CALL_TIMEOUT`]. The exception is AFT's `bash` when the
//! caller asked it to hold the reply until the command ends: a `cargo test` or a build gate run
//! that way legitimately takes many minutes. Waiting only 120 s for it turned a healthy
//! dispatched call into an indeterminate outcome, which ends the whole run Interrupted while the
//! command keeps running. So the deadline is computed per call, here and nowhere else.

use std::time::Duration;

use crate::connection::TOOL_CALL_TIMEOUT;

/// The subc module id AFT registers under. Only calls routed to this module get the long
/// deadline: another module's tool that happens to be named `bash` promised nothing about how
/// long it holds a reply, so it keeps the default.
pub const AFT_MODULE_ID: &str = "aft";

/// AFT's `bash` default hard cap when the call omits `timeout`.
pub const AFT_BASH_DEFAULT_TIMEOUT_MS: u64 = 1_800_000;

/// Headroom past the command's own timeout for AFT to kill the process group and answer.
pub const AFT_BASH_REPLY_GRACE: Duration = Duration::from_secs(30);

/// The largest `timeout` AFT accepts. Above 2^53 AFT refuses the call at once.
const AFT_MAX_TIMEOUT_MS: u64 = 1 << 53;

/// A worker's `bash_watch` hold limit when `timeoutMs` is omitted, and its maximum accepted hold.
const AFT_BASH_WATCH_DEFAULT_TIMEOUT_MS: u64 = 1_800_000;

/// The terminal-reply deadline for one tool call routed to `module_id`.
///
/// THE RULE, agreed with AFT and pinned on their side by
/// `crates/aft/tests/integration/bash_reply_timing_contract_test.rs` (AFT repository, main):
///  1. `bash` with an omitted `timeout` runs under a 1,800,000 ms hard cap;
///  2. `bash` with `wait: true` or `block_to_completion: true` holds its reply until the
///     command ends;
///  3. when the timeout fires AFT kills the whole process group and the held call answers
///     `status: timed_out` within timeout + 8 s (normally ~0.6 s late); a rare retry path can
///     take longer than timeout + 30 s.
///
/// So an AFT `bash` with either flag set waits (timeout, or 1,800,000 ms if omitted) + 30 s.
/// Every other call -- `bash_status`, `bash` without the flags, and any tool of any other
/// module -- waits [`TOOL_CALL_TIMEOUT`].
///
/// NO CAP ON A LARGE TIMEOUT. 1,800,000 ms is only AFT's default: AFT does not clamp a larger
/// `timeout` (its translator accepts any positive integer up to 2^53 and its watchdog enforces
/// what was persisted), so `timeout: 3600000` with `wait: true` legitimately holds its reply
/// about an hour. Capping broca's wait would call that live call ambiguous. Nor is the argument
/// clamped instead: broca dispatches a call exactly as the model produced it, so the recorded
/// intent and what ran stay the same. AFT's own watchdog is what bounds the call. The addition
/// saturates so no timeout value can overflow.
///
/// THE ARGUMENTS ARE READ THE WAY AFT READS THEM, because a mismatch here is not cosmetic: if
/// AFT sees `wait` as true and broca does not, a live call is declared ambiguous at 120 s.
///  - `timeout` mirrors `coerce_optional_int_result` (AFT `subc_translate.rs:937`); see
///    [`aft_timeout_ms`].
///  - `wait` / `block_to_completion` mirror `coerce_boolean` (AFT `subc_translate.rs:1138`);
///    see [`aft_bool`].
///  - `bash_watch`'s `timeoutMs` is renamed to `timeout_ms` by the subc translator (AFT
///    `subc_translate.rs:1217-1241`). For worker calls, omission uses the 1,800,000 ms cap and
///    the handler accepts 1..=1,800,000 ms (AFT `commands/bash_watch.rs:161-209`). Its coercion is
///    not `coerce_optional_int_result`: it accepts unsigned integers or trimmed u64 strings, and
///    rejects zero, empty strings, fractions, negatives, and values above the cap. The handler
///    waits via a deferred reply (AFT `commands/bash_watch.rs:224-229`); worker `background: true`
///    selects the cap regardless of timeout (AFT `commands/bash_watch.rs:196-209`). `pattern` is
///    passed through and can end the wait early (AFT `subc_translate.rs:1217-1241` and
///    `commands/bash_watch.rs:117-145,355-405`).
///
/// A `timeout` AFT refuses (negative, fractional, non-numeric, above 2^53) gets 120 s: AFT
/// answers `invalid_request` at once, so the short deadline never cuts real work.
///
/// NEVER SHORTER THAN THE DEFAULT. A small explicit timeout (e.g. 5,000 ms) would otherwise give
/// 35 s, less than every other call waits. AFT usually answers by timeout + 8 s, but its rare
/// kill-retry path can run past timeout + 30 s, and cutting a live call is what this function
/// exists to prevent, so the long rule only ever lengthens the wait.
pub fn tool_reply_deadline(
    module_id: &str,
    tool_name: &str,
    arguments: &serde_json::Value,
) -> Duration {
    if module_id != AFT_MODULE_ID {
        return TOOL_CALL_TIMEOUT;
    }
    if tool_name == "bash_watch" {
        // A worker's `background: true` is not a quick return: AFT always answers a watch
        // synchronously on this route and turns a worker's async request into a wait up to its
        // worker limit (AFT `commands/bash_watch.rs:196-209`), so it holds as long as an
        // omitted timeout does.
        let timeout = if aft_bool(arguments.get("background")) {
            AftTimeout::Omitted
        } else {
            aft_bash_watch_timeout_ms(arguments.get("timeoutMs"))
        };
        return match timeout {
            AftTimeout::Omitted => Duration::from_millis(AFT_BASH_WATCH_DEFAULT_TIMEOUT_MS)
                .saturating_add(AFT_BASH_REPLY_GRACE)
                .max(TOOL_CALL_TIMEOUT),
            AftTimeout::Millis(ms) => Duration::from_millis(ms)
                .saturating_add(AFT_BASH_REPLY_GRACE)
                .max(TOOL_CALL_TIMEOUT),
            AftTimeout::Refused => TOOL_CALL_TIMEOUT,
        };
    }
    if tool_name != "bash" {
        return TOOL_CALL_TIMEOUT;
    }
    let holds_reply =
        aft_bool(arguments.get("wait")) || aft_bool(arguments.get("block_to_completion"));
    if !holds_reply {
        return TOOL_CALL_TIMEOUT;
    }
    let held = match aft_timeout_ms(arguments.get("timeout")) {
        AftTimeout::Omitted => {
            Duration::from_millis(AFT_BASH_DEFAULT_TIMEOUT_MS).saturating_add(AFT_BASH_REPLY_GRACE)
        }
        AftTimeout::Millis(ms) => Duration::from_millis(ms).saturating_add(AFT_BASH_REPLY_GRACE),
        AftTimeout::Refused => TOOL_CALL_TIMEOUT,
    };
    held.max(TOOL_CALL_TIMEOUT)
}

#[derive(Debug, PartialEq, Eq)]
enum AftTimeout {
    /// AFT applies its 1,800,000 ms default.
    Omitted,
    /// AFT runs the command under this many milliseconds.
    Millis(u64),
    /// AFT rejects the call with `invalid_request` before running anything.
    Refused,
}

/// AFT's reading of `timeout` (`coerce_optional_int_result`, AFT `subc_translate.rs:937`):
/// absent, `null`, `""`, `[]`, `{}` and `0` are all OMITTED; a JSON integer, or a numeric
/// string with no fractional part (`"600000"`, and also `"600000.0"`), is that many ms;
/// anything else -- negative, fractional, non-numeric, above 2^53 -- is refused.
fn aft_bash_watch_timeout_ms(value: Option<&serde_json::Value>) -> AftTimeout {
    use serde_json::Value;
    let parsed = match value {
        None => return AftTimeout::Omitted,
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(text)) => text.trim().parse::<u64>().ok(),
        _ => None,
    };
    match parsed {
        None => AftTimeout::Refused,
        Some(ms) if (1..=AFT_BASH_WATCH_DEFAULT_TIMEOUT_MS).contains(&ms) => AftTimeout::Millis(ms),
        Some(_) => AftTimeout::Refused,
    }
}

fn aft_timeout_ms(value: Option<&serde_json::Value>) -> AftTimeout {
    use serde_json::Value;
    let whole = match value {
        None | Some(Value::Null) => return AftTimeout::Omitted,
        Some(Value::Array(items)) if items.is_empty() => return AftTimeout::Omitted,
        Some(Value::Object(fields)) if fields.is_empty() => return AftTimeout::Omitted,
        Some(Value::String(text)) if text.trim().is_empty() => return AftTimeout::Omitted,
        Some(Value::Number(number)) => {
            if let Some(ms) = number.as_u64() {
                Some(ms)
            } else if number.is_i64() {
                // A negative integer.
                None
            } else {
                // A float literal such as 600000.0 is still whole; 1.5 is not.
                number.as_f64().and_then(whole_f64)
            }
        }
        Some(Value::String(text)) => whole_numeric_string(text.trim()),
        Some(_) => None,
    };
    match whole {
        Some(0) => AftTimeout::Omitted,
        Some(ms) if ms <= AFT_MAX_TIMEOUT_MS => AftTimeout::Millis(ms),
        _ => AftTimeout::Refused,
    }
}

/// A non-negative whole number from a numeric string: digits, optionally followed by a `.` and
/// only zeros. Parsed as an integer rather than through f64 so a value just past 2^53 is not
/// rounded down into range.
fn whole_numeric_string(text: &str) -> Option<u64> {
    let (int_part, frac_part) = match text.split_once('.') {
        Some((int_part, frac_part)) => (int_part, Some(frac_part)),
        None => (text, None),
    };
    if int_part.is_empty() || !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if let Some(frac) = frac_part {
        if !frac.bytes().all(|b| b == b'0') {
            return None;
        }
    }
    int_part.parse::<u64>().ok()
}

fn whole_f64(value: f64) -> Option<u64> {
    if value.is_finite()
        && value >= 0.0
        && value.fract() == 0.0
        && value <= AFT_MAX_TIMEOUT_MS as f64
    {
        Some(value as u64)
    } else {
        None
    }
}

/// AFT's reading of a boolean flag (`coerce_boolean`, AFT `subc_translate.rs:1138`): true for
/// JSON `true`, the number 1, and the strings `"true"` or `"1"` (trimmed, case-insensitive);
/// everything else, including an absent flag, is false.
fn aft_bool(value: Option<&serde_json::Value>) -> bool {
    use serde_json::Value;
    match value {
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(number)) => number.as_f64() == Some(1.0),
        Some(Value::String(text)) => {
            let text = text.trim();
            text.eq_ignore_ascii_case("true") || text == "1"
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const DEFAULT_LONG: Duration = Duration::from_millis(1_800_000 + 30_000);

    fn deadline(args: serde_json::Value) -> Duration {
        tool_reply_deadline(AFT_MODULE_ID, "bash", &args)
    }

    #[test]
    fn waiting_bash_with_omitted_timeout_gets_the_default_plus_grace() {
        assert_eq!(
            deadline(json!({"command": "cargo test", "wait": true})),
            DEFAULT_LONG
        );
    }

    #[test]
    fn waiting_bash_with_explicit_timeout_gets_that_timeout_plus_grace() {
        assert_eq!(
            deadline(json!({"command": "x", "wait": true, "timeout": 600_000})),
            Duration::from_millis(630_000)
        );
        // A small timeout never shortens the wait below the default: AFT's rare kill-retry
        // path can outrun timeout + 30 s, and the long rule exists only to lengthen waits.
        assert_eq!(
            deadline(json!({"command": "x", "wait": true, "timeout": 5_000})),
            TOOL_CALL_TIMEOUT
        );
        assert_eq!(
            deadline(json!({"command": "x", "wait": true, "timeout": 89_999})),
            TOOL_CALL_TIMEOUT
        );
        assert_eq!(
            deadline(json!({"command": "x", "wait": true, "timeout": 90_001})),
            Duration::from_millis(120_001)
        );
    }

    #[test]
    fn block_to_completion_counts_the_same_as_wait() {
        assert_eq!(
            deadline(json!({"command": "x", "block_to_completion": true})),
            DEFAULT_LONG
        );
        assert_eq!(
            deadline(
                json!({"command": "x", "wait": true, "block_to_completion": true, "timeout": 600_000})
            ),
            Duration::from_millis(630_000)
        );
    }

    #[test]
    fn bash_without_either_flag_keeps_the_default() {
        assert_eq!(deadline(json!({"command": "x"})), TOOL_CALL_TIMEOUT);
        assert_eq!(
            deadline(json!({"command": "x", "timeout": 3_600_000})),
            TOOL_CALL_TIMEOUT
        );
        assert_eq!(
            deadline(json!({"command": "x", "wait": false, "block_to_completion": false})),
            TOOL_CALL_TIMEOUT
        );
    }

    #[test]
    fn bash_watch_with_omitted_timeout_gets_default_plus_grace() {
        assert_eq!(
            tool_reply_deadline(AFT_MODULE_ID, "bash_watch", &json!({"taskId": "x"})),
            DEFAULT_LONG
        );
    }

    #[test]
    fn bash_watch_explicit_timeout_and_minimum_follow_aft_hold() {
        assert_eq!(
            tool_reply_deadline(
                AFT_MODULE_ID,
                "bash_watch",
                &json!({"timeoutMs": 1_500_000})
            ),
            Duration::from_millis(1_530_000)
        );
        assert_eq!(
            tool_reply_deadline(AFT_MODULE_ID, "bash_watch", &json!({"timeoutMs": 5_000})),
            TOOL_CALL_TIMEOUT
        );
        assert_eq!(
            tool_reply_deadline(AFT_MODULE_ID, "bash_watch", &json!({"timeoutMs": 90_001})),
            Duration::from_millis(120_001)
        );
    }

    #[test]
    fn bash_watch_background_holds_like_an_omitted_timeout() {
        assert_eq!(
            tool_reply_deadline(AFT_MODULE_ID, "bash_watch", &json!({"background": true})),
            DEFAULT_LONG
        );
        assert_eq!(
            tool_reply_deadline(
                AFT_MODULE_ID,
                "bash_watch",
                &json!({"background": true, "timeoutMs": 5_000})
            ),
            DEFAULT_LONG,
            "a worker's background watch ignores timeoutMs and waits up to its limit"
        );
    }

    #[test]
    fn bash_watch_refused_timeout_keeps_default() {
        assert_eq!(
            tool_reply_deadline(
                AFT_MODULE_ID,
                "bash_watch",
                &json!({"timeoutMs": 1_800_001})
            ),
            TOOL_CALL_TIMEOUT
        );
    }

    #[test]
    fn non_aft_bash_watch_keeps_default() {
        assert_eq!(
            tool_reply_deadline("other", "bash_watch", &json!({"timeoutMs": 1_500_000})),
            TOOL_CALL_TIMEOUT
        );
    }

    #[test]
    fn bash_status_keeps_the_default() {
        assert_eq!(
            tool_reply_deadline(AFT_MODULE_ID, "bash_status", &json!({"wait": true})),
            TOOL_CALL_TIMEOUT
        );
    }

    #[test]
    fn a_bash_tool_on_another_module_keeps_the_default() {
        assert_eq!(
            tool_reply_deadline(
                "shell-module",
                "bash",
                &json!({"wait": true, "timeout": 600_000})
            ),
            TOOL_CALL_TIMEOUT
        );
    }

    #[test]
    fn non_object_arguments_keep_the_default() {
        assert_eq!(deadline(json!(null)), TOOL_CALL_TIMEOUT);
        assert_eq!(deadline(json!("wait")), TOOL_CALL_TIMEOUT);
        assert_eq!(deadline(json!([true])), TOOL_CALL_TIMEOUT);
    }

    /// A timeout far above AFT's default is NOT capped: AFT runs it as given, so broca waits for
    /// it. Also covers the saturating addition at the largest value AFT accepts.
    #[test]
    fn an_hour_long_timeout_is_waited_for_in_full() {
        assert_eq!(
            deadline(json!({"command": "x", "wait": true, "timeout": 3_600_000})),
            Duration::from_millis(3_630_000)
        );
        assert_eq!(
            deadline(json!({"command": "x", "wait": true, "timeout": 1u64 << 53})),
            Duration::from_millis(1u64 << 53).saturating_add(AFT_BASH_REPLY_GRACE)
        );
    }

    /// Every `timeout` shape AFT's `coerce_optional_int_result` distinguishes, with `wait: true`.
    #[test]
    fn timeout_coercion_mirrors_aft() {
        let refused = TOOL_CALL_TIMEOUT;
        let cases: Vec<(serde_json::Value, Duration)> = vec![
            // Integers and whole numeric strings are that timeout.
            (json!(600_000), Duration::from_millis(630_000)),
            (json!("600000"), Duration::from_millis(630_000)),
            (json!("600000.0"), Duration::from_millis(630_000)),
            (json!(600_000.0), Duration::from_millis(630_000)),
            (json!(" 600000 "), Duration::from_millis(630_000)),
            // Omitted: AFT's 1,800,000 ms default, not the 120 s fallback.
            (json!(null), DEFAULT_LONG),
            (json!(""), DEFAULT_LONG),
            (json!([]), DEFAULT_LONG),
            (json!({}), DEFAULT_LONG),
            (json!(0), DEFAULT_LONG),
            (json!("0"), DEFAULT_LONG),
            // Refused by AFT with invalid_request at once: the short deadline is safe.
            (json!(-5), refused),
            (json!("-5"), refused),
            (json!(1.5), refused),
            (json!("1.5"), refused),
            (json!("abc"), refused),
            (json!(true), refused),
            (json!([600_000]), refused),
            (json!({"ms": 600_000}), refused),
            (json!((1u64 << 53) + 1), refused),
            (json!(((1u64 << 53) + 1).to_string()), refused),
        ];
        for (timeout, expected) in cases {
            assert_eq!(
                deadline(json!({"command": "x", "wait": true, "timeout": timeout.clone()})),
                expected,
                "timeout {timeout}"
            );
        }
        // Absent entirely is also omitted.
        assert_eq!(
            deadline(json!({"command": "x", "wait": true})),
            DEFAULT_LONG
        );
    }

    /// Every flag shape AFT's `coerce_boolean` distinguishes, for both flags.
    #[test]
    fn flag_coercion_mirrors_aft() {
        let truthy = [
            json!(true),
            json!(1),
            json!("true"),
            json!("1"),
            json!(" 1 "),
            json!("True"),
            json!(" TRUE "),
        ];
        let falsy = [
            json!(false),
            json!(0),
            json!(2),
            json!(null),
            json!(""),
            json!("false"),
            json!("yes"),
            json!("0"),
            json!([]),
            json!({}),
            json!([true]),
        ];
        for flag in ["wait", "block_to_completion"] {
            for value in &truthy {
                assert_eq!(
                    deadline(json!({"command": "x", flag: value.clone()})),
                    DEFAULT_LONG,
                    "{flag}: {value} should hold the reply"
                );
            }
            for value in &falsy {
                assert_eq!(
                    deadline(json!({"command": "x", flag: value.clone()})),
                    TOOL_CALL_TIMEOUT,
                    "{flag}: {value} should not hold the reply"
                );
            }
        }
    }
}
