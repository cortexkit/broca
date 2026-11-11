//! A run's spend facts, for a session kept in the new storage.
//!
//! The usage export holds the facts today's builder writes
//! (`broca_core::billing::compute_segments_for_run`), and its readers read nothing else. That
//! builder's rules are many: segments, refreezes, refusal-fallback routing, warms billed under
//! the run whose request they re-sent. So a run kept here isn't billed by a second copy of them.
//! The records the builder reads are rebuilt in today's form ([`crate::today`]) and handed to
//! the same builder, through the export writer today's runs use. Which records are handed over
//! is the one thing decided here.
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
//! The rebuilt records carry the store's seqs. Today's builder reads a seq only as a warm fact's
//! segment ordinal and in its fact id, and a store seq never repeats within a session, so warm
//! fact ids stay unique.

use broca_protocol::BindIdentity;
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{Assistant, Input, Placement, Record};
use broca_session_store::session::Session;
use broca_wal::FramedRecord;
use serde_json::Value;

use crate::parts::Parts;
use crate::today;

/// Today's records the builder reads for `run_id`, rebuilt from `records`: the session's records
/// in seq order, both streams, from the run's first start on or from earlier. `session` is the
/// session's identity, which today's run starts repeat and the new format keeps once.
pub fn today_records(
    records: &[(u64, Record)],
    run_id: &str,
    session: &BindIdentity,
    parts: &dyn Parts,
) -> Result<Vec<FramedRecord>, String> {
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
    for (index, (seq, record)) in records.iter().enumerate().skip(first) {
        let rebuilt = match record {
            Record::RunStart(start) if start.run_id == run_id => {
                own = true;
                ended = false;
                assistant = None;
                // Its input entries follow it, as today's record holds them.
                let inputs: Vec<&Input> = records[index + 1..]
                    .iter()
                    .map_while(|(_, record)| match record {
                        Record::Input(input)
                            if input.placement == Placement::RunInput
                                && input.run_id == start.run_id =>
                        {
                            Some(input)
                        }
                        _ => None,
                    })
                    .collect();
                Some(run_started(start, &inputs, session, parts)?)
            }
            Record::RunStart(start) => {
                let closes = own && !ended;
                own = false;
                if closes {
                    Some(run_started(start, &[], session, parts)?)
                } else {
                    None
                }
            }
            Record::Warmed(warmed) if warmed.run_id == run_id => Some(today::cache_warmed(warmed)?),
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
                Some(today::model_step_finished(message, end)?)
            }
            Record::StepRefused(refused) => Some(today::model_step_refused(refused)),
            Record::ConfigRefrozen(refrozen) => Some(today::run_config_refrozen(refrozen)),
            Record::AuthRefrozen(refrozen) => Some(today::run_auth_refrozen(refrozen)),
            Record::Pause(pause) => Some(today::run_paused(pause)),
            Record::RunEnd(end) => {
                ended = true;
                Some(today::run_finished(end))
            }
            _ => None,
        };
        if let Some(value) = rebuilt {
            let kind = value["type"].as_str().unwrap_or("?").to_owned();
            let record = serde_json::from_value(value).map_err(|error| {
                format!("the rebuilt {kind} at seq {seq} doesn't read: {error}")
            })?;
            out.push(FramedRecord {
                seq: *seq,
                fence: 0,
                record,
            });
        }
    }
    Ok(out)
}

/// Today's `run_started` for `start`, under the session's identity.
fn run_started(
    start: &broca_session_store::record::RunStart,
    inputs: &[&Input],
    session: &BindIdentity,
    parts: &dyn Parts,
) -> Result<Value, String> {
    let mut value = today::run_started(start, inputs, |part| crate::parts::value(parts, part))?;
    if let Value::Object(object) = &mut value {
        object.insert("session".into(), today::json(session));
    }
    Ok(value)
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

/// Today's records the builder reads for `run_id`, read from `session` and rebuilt.
pub async fn run_records<B: SessionBackend>(
    session: &Session<B>,
    run_id: &str,
    bind: &BindIdentity,
    parts: &dyn Parts,
) -> Result<Vec<FramedRecord>, String> {
    let records = session_records(session, run_id).await?;
    today_records(&records, run_id, bind, parts)
}

#[cfg(test)]
mod tests;
