use super::*;

#[test]
fn ceiling_records_and_terminal_require_a_named_reader_gate() {
    let records = [
        WalRecord::ContextCeilingWait {
            step_id: broca_protocol::StepId(1),
            deadline_ms: 12,
            unavailable: None,
        },
        WalRecord::ContextCeilingDecided {
            step_id: broca_protocol::StepId(1),
            detail: Some(broca_protocol::ContextCeilingDetail::new(
                10,
                11,
                "over_window",
            )),
        },
        WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::ContextCeiling,
            ts_ms: None,
            error: None,
            usage: Usage::default(),
            retries_used: 0,
        },
    ];
    for record in records {
        assert!(
            record.required_features().contains(&"context-ceiling/v1"),
            "{record:?}"
        );
    }
}
