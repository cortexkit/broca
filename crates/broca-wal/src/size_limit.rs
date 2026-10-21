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
    crate::framing::encode(u64::MAX, u64::MAX, &largest)
        .expect("bounded terminal encodes")
        .len() as u64
        + overhead
}
