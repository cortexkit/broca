//! The record a model attempt leaves: the provider's evidence, typed where billing and the
//! session's folded state read it.

use serde_json::{json, Value};

use broca_protocol::ModelAttempt;
use broca_session_store::codec::Digest;
use broca_session_store::record::{Attempt, Record};

use super::{hex_digest, LoopError};

/// One HTTP attempt's record, from the provider's evidence. Billing and the session's folded state read the typed
/// fields; every other field the provider measured is kept as diagnostics telemetry under the
/// name it has on the provider's evidence. Billing details are absent: no attempt in this loop
/// is billed through a reservation yet.
/// `requested_ttl_ms` is the cache lifetime the run's requests ask for, recorded on an attempt
/// that reached the cache.
/// `account_limit_pause` is the run's frozen consent to a timed same-account pause: with it, an
/// attempt the provider refused for the account's limit records that limit's disposition, as
/// today's `account_limit_pause::attempt_record` does.
pub(super) fn attempt_record(
    run_id: &str,
    step_id: u64,
    attempt: ModelAttempt,
    requested_ttl_ms: Option<u64>,
    account_limit_pause: bool,
) -> Result<Record, LoopError> {
    let disposition = account_limit_pause
        .then(|| {
            attempt
                .account_observation
                .limit
                .as_ref()
                .map(|limit| limit.disposition)
        })
        .flatten();
    let invalid = |what: &str| LoopError::Parts(format!("attempt evidence: {what}"));
    let mut value = serde_json::to_value(&attempt).map_err(|e| invalid(&e.to_string()))?;
    let fields = value
        .as_object_mut()
        .ok_or_else(|| invalid("not an object"))?;
    let mut take = |key: &str| fields.remove(key);
    let digest = |value: Option<Value>, key: &str| -> Result<Digest, LoopError> {
        let text = value.as_ref().and_then(Value::as_str).unwrap_or_default();
        hex_digest(text).ok_or_else(|| invalid(&format!("{key} is not a SHA-256 hex digest")))
    };
    let outcome = take("outcome")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| invalid("outcome"))?;
    take("attempt");
    take("request_body_len");
    take("response_body_len");
    take("http_status");
    take("ts_ms");
    take("sent_at_ms");
    take("first_content_at_ms");
    take("last_content_at_ms");
    take("transport");
    take("served_account_ref");
    let reached =
        broca_core::cache_warm::attempt_reached_the_cache(attempt.http_status, &attempt.outcome);
    let request_body_sha256 = digest(take("request_body_sha256"), "request_body_sha256")?;
    let response_body_sha256 = digest(take("response_body_sha256"), "response_body_sha256")?;
    let telemetry = (!fields.is_empty()).then(|| Value::Object(std::mem::take(fields)));
    Ok(Record::Attempt(Attempt {
        run_id: run_id.into(),
        step_id,
        attempt: attempt.attempt,
        attempt_id: Some(
            broca_core::attempt_id::mint_attempt_id()
                .map_err(|e| invalid(&format!("attempt identity: {e}")))?
                .as_str()
                .to_owned(),
        ),
        outcome,
        http_status: attempt.http_status.map(u32::from),
        request_body_len: attempt.request_body_len,
        request_body_sha256,
        response_body_len: attempt.response_body_len,
        response_body_sha256,
        ts_ms: attempt.ts_ms,
        sent_at_ms: attempt.sent_at_ms,
        first_content_at_ms: attempt.content_progress.first_content_at_ms,
        last_content_at_ms: attempt.content_progress.last_content_at_ms,
        transport: attempt.transport.clone(),
        served_account_ref: attempt.served_account_ref.clone(),
        billing: (disposition
            .map(|disposition| {
                serde_json::to_value(disposition)
                    .map(|disposition| json!({ "account_limit_disposition": disposition }))
            })
            .transpose()
            .map_err(|e| invalid(&e.to_string()))?)
        .map(Into::into),
        telemetry: telemetry.map(Into::into),
        reached_cache: reached.then_some(true),
        requested_cache_ttl_ms: requested_ttl_ms.filter(|_| reached),
    }))
}
