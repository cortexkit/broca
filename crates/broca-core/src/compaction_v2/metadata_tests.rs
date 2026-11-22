use broca_protocol::Usage;

use super::metadata;

#[test]
fn compaction_v2_usage_keeps_absent_counts_distinct_from_reported_zero() {
    assert_eq!(metadata::step_usage(Usage::default()), None);
    let usage = metadata::step_usage(Usage {
        input_tokens: Some(0),
        output_tokens: Some(0),
        ..Usage::default()
    })
    .unwrap();
    assert_eq!(usage.prompt_tokens, None);
    assert_eq!(usage.output_tokens, Some(0));
    assert_eq!(usage.cache_read_tokens, None);
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.reasoning_tokens, None);
}
