use super as framing;
use crate::WalRecord;

const PAUSED: &str = r#"{"type":"run_paused","run_id":"r","ts_ms":1000,"reason":"account_limit","error":{"class":"transient","message":"account window exhausted","status":429,"retry_after_secs":60.0,"provider_code":"account_limit_exhausted","resets_at_ms":61000},"account_limit":{"failed_step":0,"failed_attempt_seq":2,"attempt":0,"paused_at_ms":1000,"observed_at_ms":1000,"auth_method":"oauth","credential_id":"oauth:anthropic:work","account_ref":"acct-a","disposition":{"kind":"account_limit","scope":"account"},"resets_at_ms":61000,"retry_after_secs":60.0,"resume_not_before_ms":61000,"request_body_len":12,"request_body_sha256":"0000000000000000000000000000000000000000000000000000000000000000","selection_round":0}}"#;

#[test]
fn account_limit_pause_reader_keeps_pinned_bytes_and_requires_feature() {
    let record: WalRecord =
        serde_json::from_str(PAUSED).expect("reader decodes timed account pause");
    assert_eq!(serde_json::to_string(&record).unwrap(), PAUSED);
    assert_eq!(record.required_features(), ["account-limit-pause/v1"]);
    let bytes = framing::encode(3, 1, &record).unwrap();
    let framing::DecodeStep::Record { framed, consumed } = framing::decode_next(&bytes) else {
        panic!("current reader must decode the complete pause frame");
    };
    assert_eq!(consumed, bytes.len());
    assert_eq!(serde_json::to_string(&framed.record).unwrap(), PAUSED);
    let older: Vec<_> = framing::KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|name| *name != "account-limit-pause/v1")
        .collect();
    assert!(matches!(framing::decode_next_with(&bytes, &older),
        framing::DecodeStep::NewerWriter { missing, .. } if missing == ["account-limit-pause/v1"]));
}

#[test]
fn account_limit_pause_reader_leaves_legacy_pause_bytes_unchanged() {
    for pinned in [
        r#"{"type":"run_paused","run_id":"r","reason":"auth_required"}"#,
        r#"{"type":"run_paused","run_id":"r","reason":"restart"}"#,
        r#"{"type":"run_paused","run_id":"r","reason":"tool_held"}"#,
    ] {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        assert!(!record
            .required_features()
            .contains(&"account-limit-pause/v1"));
    }
}
