//! Strict params decoding: a params member broca does not know refuses instead of
//! silently changing which request the caller made.

use super::*;

/// A misspelled `run_id` must REFUSE rather than answer a different question.
///
/// The camelCase arm is not hypothetical: a Swift or JS encoder emits the property name as
/// written, so a client whose field is `runId` sends `runId`. Before this was strict, that
/// decoded to `run_id: None` -- a valid request for SESSION status -- and the caller got a
/// well-formed answer about the wrong subject with nothing to signal it.
#[test]
fn a_misspelled_run_id_refuses_instead_of_selecting_session_status() {
    let good: StatusParams = serde_json::from_str(r#"{"run_id":"r-1"}"#).unwrap();
    assert_eq!(good.run_id.as_deref(), Some("r-1"));

    for wrong in [
        r#"{"runId":"r-1"}"#,
        r#"{"run":"r-1"}"#,
        r#"{"runID":"r-1"}"#,
    ] {
        let err = serde_json::from_str::<StatusParams>(wrong).unwrap_err();
        assert!(
            err.to_string().contains("unknown field"),
            "{wrong} must be refused by name, got: {err}"
        );
    }
}

/// A retract names only the submission id broca returned. An extra member such as the
/// caller's own `send_id` is refused by name rather than ignored.
#[test]
fn a_retract_with_an_extra_member_refuses() {
    let good: RetractParams = serde_json::from_str(r#"{"submission_id":"sub-1"}"#).unwrap();
    assert_eq!(good.submission_id, "sub-1");
    for wrong in [
        r#"{"submission_id":"sub-1","send_id":"s-1"}"#,
        r#"{"submissionId":"sub-1"}"#,
    ] {
        let err = serde_json::from_str::<RetractParams>(wrong).unwrap_err();
        assert!(
            err.to_string().contains("unknown field"),
            "{wrong} must be refused by name, got: {err}"
        );
    }
}

/// The legitimate absence still works: `{}` is "session status", which is the whole reason
/// the field is optional. Without this arm, strictness could be "fixed" by making the field
/// required, which would break every session-status caller.
#[test]
fn an_absent_run_id_still_selects_session_status() {
    let p: StatusParams = serde_json::from_str("{}").unwrap();
    assert!(p.run_id.is_none());
}
