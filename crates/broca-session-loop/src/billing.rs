//! A run's spend facts, for a session kept in the session store.
//!
//! The usage export holds the facts computed by
//! [`broca_core::billing::compute_segments_for_run`], and its readers read nothing else. That
//! builder handles segments, refreezes, refusal-fallback routing and warms billed under the run
//! whose request they re-sent. This code builds its input records
//! ([`broca_core::billing::BillingRecord`]) from the session's records and decides which are
//! handed over; the usage export writer hands them to the builder.
//!
//! For one run, the builder reads:
//! - the run's start;
//! - from there until the next run's start, the run's steps (each with its assistant message,
//!   which holds the step's usage and time), refused steps, refreezes, pauses and end, and the
//!   warms naming it;
//! - after that, only the warms naming it: a warm can follow its run's end, even another run's
//!   start, and is billed under the run whose request it re-sent;
//! - when the run never ended, the next run's start, because that start closes the run's last
//!   segment differently from the end of the records.
//!
//! No record before the run's first start changes its facts. The builder tracks every run's
//! config so that a warm is billed under the run it names, but a warm naming an earlier run
//! yields that run's facts, never this one's.
//!
//! The builder's records carry the store's seqs. The builder reads a seq only as a warm fact's
//! segment ordinal and in its fact id, and a store seq never repeats within a session, so warm
//! fact ids stay unique.

use broca_core::billing::attempt::AttemptEvidence;
use broca_core::billing::{BillingEvent, BillingRecord};
use broca_protocol::{BindIdentity, RunId, StepId};
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{Assistant, Record, RunStart};
use broca_session_store::session::Session;
use serde_json::{Map, Value};

use crate::parts::Parts;

/// The builder's input records for `run_id`, built from `records`: the session's records in seq
/// order, transcript and ledger records alike, from the run's first start or earlier. `session` is the session's
/// identity, which the builder names each fact's session with.
pub fn billing_records(
    records: &[(u64, Record)],
    run_id: &str,
    session: &BindIdentity,
    parts: &dyn Parts,
) -> Result<Vec<BillingRecord>, String> {
    let first = records
        .iter()
        .position(|(_, record)| matches!(record, Record::RunStart(start) if start.run_id == run_id))
        .ok_or_else(|| format!("the session holds no start of the run {run_id}"))?;
    let mut out = Vec::new();
    // Whether the records being read are the run's own, from one of its starts up to the next
    // run's start, and whether those ended it.
    let mut own = false;
    let mut ended = false;
    let mut assistant: Option<&Assistant> = None;
    for (seq, record) in records.iter().skip(first) {
        let event = match record {
            Record::RunStart(start) if start.run_id == run_id => {
                own = true;
                ended = false;
                assistant = None;
                Some(run_started(start, session, parts)?)
            }
            Record::RunStart(start) => {
                let closes = own && !ended;
                own = false;
                if closes {
                    Some(run_started(start, session, parts)?)
                } else {
                    None
                }
            }
            Record::Warmed(warmed) if warmed.run_id == run_id => Some(BillingEvent::Warmed {
                run_id: RunId(warmed.run_id.clone()),
                step_id: StepId(warmed.step_id),
                usage: warmed.usage,
                sent_at_ms: warmed.sent_at_ms,
                envelope: members(warmed.billing.as_ref())?
                    .remove("envelope")
                    .map(|envelope| decode(envelope, "a warm's billing envelope"))
                    .transpose()?,
                trigger: warmed.trigger.clone(),
            }),
            _ if !own => None,
            Record::Assistant(message) => {
                assistant = Some(message);
                None
            }
            Record::StepEnd(end) => {
                let message = assistant
                    .take()
                    .filter(|message| {
                        message.run_id == end.run_id && message.step_id == end.step_id
                    })
                    .ok_or_else(|| {
                        format!(
                            "the step {} of the run {run_id} ended without its assistant message",
                            end.step_id
                        )
                    })?;
                Some(BillingEvent::StepFinished {
                    step_id: StepId(end.step_id),
                    written_at_ms: message.written_at_ms,
                    usage: message.usage.ok_or_else(|| {
                        format!(
                            "the step {} of the run {run_id} records no usage",
                            end.step_id
                        )
                    })?,
                    retries_used: end.retries_used,
                    service_tier_used: end.service_tier_used.clone(),
                    speed_used: end.speed_used.clone(),
                    fallback: Box::new(decode(
                        Value::Object(members(end.fallback.as_ref())?),
                        "a step's fallback",
                    )?),
                })
            }
            Record::StepRefused(refused) => Some(BillingEvent::StepRefused {
                step_id: StepId(refused.step_id),
                ts_ms: refused.ts_ms,
                usage: refused.usage,
                served_model: refused.served_model.clone(),
                usage_iterations: refused
                    .usage_iterations
                    .clone()
                    .map(|iterations| decode(iterations, "a refused step's usage iterations"))
                    .transpose()?,
            }),
            Record::ConfigRefrozen(refrozen) => Some(BillingEvent::ConfigRefrozen {
                renderer_version: refrozen.renderer_version,
                ts_ms: refrozen.ts_ms,
                frozen_render_config: refrozen.render_config.clone(),
            }),
            Record::AuthRefrozen(refrozen) => Some(BillingEvent::AuthRefrozen {
                ts_ms: refrozen.ts_ms,
                auth_selection: decode(
                    refrozen.auth_selection.clone(),
                    "a refreeze's auth selection",
                )?,
            }),
            Record::Pause(pause) => Some(BillingEvent::RunPaused { ts_ms: pause.ts_ms }),
            Record::RunEnd(end) => {
                ended = true;
                // A run end that recorded no usage can't be billed: its facts would claim a
                // total nobody measured.
                end.usage
                    .ok_or_else(|| format!("the end of the run {run_id} records no usage"))?;
                Some(BillingEvent::RunFinished {
                    reason: end.reason,
                    ts_ms: end.ts_ms,
                    retries_used: end.retries_used,
                })
            }
            _ => None,
        };
        if let Some(event) = event {
            out.push(BillingRecord { seq: *seq, event });
        }
    }
    Ok(out)
}

/// The builder's record of `start`: the run's config as the serve executor reads it from the
/// start and the setup parts it names, under the session's identity.
fn run_started(
    start: &RunStart,
    session: &BindIdentity,
    parts: &dyn Parts,
) -> Result<BillingEvent, String> {
    let (config, _) = crate::run::run_config_members(
        start,
        crate::run::SetupParts::All,
        |part| crate::parts::value(parts, part),
        || "a carried payload is not an object".to_string(),
    )?;
    Ok(BillingEvent::RunStarted {
        run_id: RunId(start.run_id.clone()),
        session: session.clone(),
        config: Box::new(decode(Value::Object(config), "a run's config")?),
        ts_ms: start.ts_ms,
        scope: start
            .scope
            .clone()
            .map(|scope| decode(scope, "a run's scope"))
            .transpose()?,
    })
}

/// The members of a payload a record holds as JSON text; none when it holds none.
fn members(payload: Option<&broca_protocol::json::Json>) -> Result<Map<String, Value>, String> {
    match payload.map(broca_protocol::json::Json::parse) {
        None => Ok(Map::new()),
        Some(Value::Object(object)) => Ok(object),
        Some(_) => Err("a carried payload is not an object".into()),
    }
}

/// `value` read as `T`, naming `what` it is when it doesn't read.
fn decode<T: serde::de::DeserializeOwned>(value: Value, what: &str) -> Result<T, String> {
    serde_json::from_value(value).map_err(|error| format!("{what} doesn't read: {error}"))
}

/// The session's records the builder may read for `run_id`: every record of both streams from
/// the run's first start on, sealed or in the open log. The id index finds the start.
pub async fn session_records<B: SessionBackend>(
    session: &Session<B>,
    run_id: &str,
) -> Result<Vec<(u64, Record)>, String> {
    let named = session
        .find_id(IdKind::RunId, run_id)
        .await
        .map_err(|error| format!("the run's records could not be found: {error:?}"))?;
    let start = named
        .iter()
        .find(|(_, record)| matches!(record, Record::RunStart(_)))
        .map(|(seq, _)| *seq)
        .ok_or_else(|| format!("the session holds no start of the run {run_id}"))?;
    session
        .records_from(start)
        .await
        .map_err(|error| format!("the run's records could not be read: {error:?}"))
}

/// Records for `run_id`, read from `session` and rebuilt.
pub async fn run_records<B: SessionBackend>(
    session: &Session<B>,
    run_id: &str,
    bind: &BindIdentity,
    parts: &dyn Parts,
) -> Result<Vec<BillingRecord>, String> {
    let records = session_records(session, run_id).await?;
    billing_records(&records, run_id, bind, parts)
}

/// What the usage export needs to bill `run_id`: the builder's input records and the run's model
/// attempts in seq order, each with its usage fact
/// ([`broca_core::billing::attempt`]).
pub async fn run_billing_inputs<B: SessionBackend>(
    session: &Session<B>,
    run_id: &str,
    bind: &BindIdentity,
    parts: &dyn Parts,
) -> Result<(Vec<BillingRecord>, Vec<AttemptEvidence>), String> {
    let records = session_records(session, run_id).await?;
    let attempts = attempt_evidence(&records, run_id);
    Ok((billing_records(&records, run_id, bind, parts)?, attempts))
}

/// The attempt records of `run_id` among `records`, as the attempt facts read them.
pub fn attempt_evidence(records: &[(u64, Record)], run_id: &str) -> Vec<AttemptEvidence> {
    records
        .iter()
        .filter_map(|(seq, record)| match record {
            Record::Attempt(attempt) if attempt.run_id == run_id => Some(AttemptEvidence {
                seq: *seq,
                step_id: attempt.step_id,
                attempt: attempt.attempt,
                attempt_id: attempt.attempt_id.clone(),
                outcome: attempt.outcome.clone(),
                http_status: attempt.http_status,
                response_bytes: attempt.response_body_len,
                content_received: attempt.first_content_at_ms.is_some(),
                sent_at_ms: attempt.sent_at_ms,
                recorded_at_ms: attempt.ts_ms,
                served_account_ref: attempt.served_account_ref.clone(),
                input_transformations: attempt
                    .telemetry
                    .as_ref()
                    .and_then(reported_input_transformations),
            }),
            _ => None,
        })
        .collect()
}

/// The provider's input-transformation report an attempt record's telemetry keeps, under the
/// names the provider's evidence gives them: `None` when the record has none, or when the
/// request didn't ask for one (`input_transformations_unmeasured`), which is unknown rather
/// than "no change".
pub(crate) fn reported_input_transformations(
    telemetry: &broca_protocol::json::Json,
) -> Option<Vec<broca_protocol::InputTransformation>> {
    #[derive(serde::Deserialize)]
    struct Report {
        #[serde(default)]
        input_transformations: Option<Vec<broca_protocol::InputTransformation>>,
        #[serde(default)]
        input_transformations_unmeasured: bool,
    }
    let report: Report = serde_json::from_str(telemetry.text()).ok()?;
    report
        .input_transformations
        .filter(|_| !report.input_transformations_unmeasured)
}

#[cfg(test)]
mod tests;
