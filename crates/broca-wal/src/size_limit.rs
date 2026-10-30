use crate::WalRecord;
use broca_protocol::{ErrorClass, ProviderError, RunFinishReason, Usage};

pub const CODE: &str = "retained_session_too_large";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeScope {
    Session,
    RetainedSession,
}

pub fn scoped_error(max_bytes: u64, scope: SizeScope) -> ProviderError {
    let (subject, code) = match scope {
        SizeScope::Session => ("session", "session_too_large"),
        SizeScope::RetainedSession => ("retained session", CODE),
    };
    ProviderError {
        provider_details: None,
        provider_subcode: None,
        class: ErrorClass::Permanent,
        message: format!("{subject} log cannot exceed {max_bytes} bytes"),
        provider_code: Some(code.into()),
        status: None,
        retry_after_secs: None,
        refusal: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

pub fn error(max_bytes: u64) -> ProviderError {
    scoped_error(max_bytes, SizeScope::RetainedSession)
}

pub fn scoped_terminal(
    max_bytes: u64,
    scope: SizeScope,
    ts_ms: Option<u64>,
    usage: Usage,
    retries_used: u32,
) -> WalRecord {
    let mut record = terminal(max_bytes, ts_ms, usage, retries_used);
    if let WalRecord::RunFinished { error, .. } = &mut record {
        *error = Some(scoped_error(max_bytes, scope));
    }
    record
}

pub fn terminal(max_bytes: u64, ts_ms: Option<u64>, usage: Usage, retries_used: u32) -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms,
        reason: RunFinishReason::Error,
        usage,
        error: Some(error(max_bytes)),
        retries_used,
    }
}

pub fn reserve_bytes(overhead: u64) -> u64 {
    terminal_reserve_bytes(overhead, crate::WalConfig::LEGACY)
}

pub fn terminal_reserve_bytes(overhead: u64, config: crate::WalConfig) -> u64 {
    // The size-refusal RunFinished record has bounded fields: fixed error text/code plus
    // integer accounting, timestamp and max_bytes in its message. Encode maximum-width
    // integers so refusing an append always leaves room to commit that complete record.
    let usage = Usage {
        input_tokens: Some(u64::MAX),
        cached_input_tokens: Some(u64::MAX),
        cache_write_tokens: Some(u64::MAX),
        output_tokens: Some(u64::MAX),
        reasoning_tokens: Some(u64::MAX),
    };
    let mut largest = terminal(u64::MAX, Some(u64::MAX), usage, u32::MAX);
    if let WalRecord::RunFinished { reason, .. } = &mut largest {
        *reason = RunFinishReason::Interrupted;
    }
    let frame_len = if config.is_overhaul() {
        crate::PreparedV4Frame::prepare(&largest)
            .expect("bounded terminal encodes")
            .frame_len()
    } else {
        crate::framing::encode(u64::MAX, u64::MAX, &largest)
            .expect("bounded terminal encodes")
            .len()
    };
    frame_len as u64 + overhead
}

pub fn reopen_reserve_bytes(config: crate::WalConfig) -> u64 {
    if !config.is_overhaul() {
        return 0;
    }
    // Lifecycle IDs are JSON byte arrays, so each byte needs up to three decimal digits.
    // Reclaim has the longest cause emitted by the writer; sequence and IDs use their
    // maximum widths. V4 stamped metadata and the two trailing lineage frames are fixed-size.
    let writer_id = [u8::MAX; 16];
    let opened = WalRecord::WriterOpened {
        writer_id,
        previous_writer_id: Some(writer_id),
        recovered_through_seq: u64::MAX,
        cause: "reclaim".into(),
    };
    let closed = WalRecord::WriterClosed { writer_id };
    [&opened, &closed]
        .into_iter()
        .map(|record| {
            crate::PreparedV4Frame::prepare(record)
                .expect("bounded writer lifecycle encodes")
                .frame_len() as u64
                + crate::framing::LINEAGE_FRAME_LEN as u64
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhaul_reopen_reserve_bounds_maximum_width_lifecycle_frames() {
        let config = crate::WalConfig::OVERHAUL;
        let bound = reopen_reserve_bytes(config);
        assert_eq!(bound, 726);
        for byte in [0, 9, 10, 99, 100, u8::MAX] {
            for cause in ["new", "reopen", "reclaim"] {
                let writer_id = [byte; 16];
                let records = [
                    WalRecord::WriterOpened {
                        writer_id,
                        previous_writer_id: Some([u8::MAX; 16]),
                        recovered_through_seq: u64::MAX,
                        cause: cause.into(),
                    },
                    WalRecord::WriterClosed { writer_id },
                ];
                let mut bytes = 0;
                for record in records {
                    let frame = crate::PreparedV4Frame::prepare(&record)
                        .unwrap()
                        .stamp(crate::framing::v4::Metadata {
                            seq: u64::MAX,
                            fence: u64::MAX,
                            appended_ms: u64::MAX,
                            writer_id,
                        })
                        .unwrap();
                    bytes += frame.len() as u64 + crate::framing::LINEAGE_FRAME_LEN as u64;
                }
                assert!(
                    bytes <= bound,
                    "ID byte {byte}, cause {cause}: {bytes} > {bound}"
                );
                if byte == u8::MAX && cause == "reclaim" {
                    assert_eq!(bytes, bound);
                }
            }
        }
    }

    #[test]
    fn legacy_terminal_reserve_is_unchanged_and_has_no_reopen_reserve() {
        assert_eq!(reserve_bytes(crate::framing::LINEAGE_FRAME_LEN as u64), 580);
        assert_eq!(reopen_reserve_bytes(crate::WalConfig::LEGACY), 0);
    }
}
