use broca_wal::WalRecord;

#[test]
fn pre_optional_usage_record_preserves_explicit_zero_measurements() {
    // Old writers emitted all five keys, even for concepts a provider never reported.
    // The decoder cannot retroactively recover absence from those recorded zeros.
    let record: WalRecord = serde_json::from_str(
        r#"{
        "type":"run_finished", "reason":"completed",
        "usage":{"input_tokens":12,"cached_input_tokens":0,"cache_write_tokens":0,
                 "output_tokens":8,"reasoning_tokens":0}, "retries_used":0
    }"#,
    )
    .unwrap();
    let WalRecord::RunFinished { usage, .. } = record else {
        panic!("run terminal")
    };
    assert_eq!(usage.input_tokens, Some(12));
    assert_eq!(usage.cached_input_tokens, Some(0));
    assert_eq!(usage.cache_write_tokens, Some(0));
    assert_eq!(usage.output_tokens, Some(8));
    assert_eq!(usage.reasoning_tokens, Some(0));
}
