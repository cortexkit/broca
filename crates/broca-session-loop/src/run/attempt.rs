//! The record a model attempt leaves: the provider's evidence, typed where billing and the
//! session's folded state read it.

use serde_json::{json, Value};

use broca_core::provider::Provider;
use broca_protocol::{ModelAttempt, RunId, StepId};
use broca_session_store::codec::Digest;
use broca_session_store::record::{Attempt, Record};

use crate::responses_reasoning::ResponsesReasoningRemovals;
use crate::writer::Writer;

use super::{event_id, FailedAccountLimitAttempt, LoopError, Seams};

/// Records one attempt's evidence, around the hooks broca-core's run loop calls: keep-warm
/// learns its send time and rate-limit headers, its input transformations are reported, and the
/// provider adds its cache evidence before the record is written and learns which record holds
/// it after. A failed attempt is durable at once (it may have been billed and no step's end will
/// follow it); a completed one rides the step's end. Returns the attempt, with its record's seq,
/// when the provider refused it for the account's limit.
#[allow(clippy::too_many_arguments)]
pub(super) async fn record_attempt<W: Writer, P: Provider, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    mut attempt: ModelAttempt,
    context: &AttemptContext,
    selection: Option<&broca_records::record::AuthSelection>,
    mut responses_removals: Option<&mut ResponsesReasoningRemovals>,
) -> Result<Option<Box<FailedAccountLimitAttempt>>, LoopError> {
    if let Some(warm) = &seams.keep_warm {
        warm.note_attempt(&attempt);
    }
    broca_core::input_transformations::report_input_transformations_filtered(
        &seams.bind,
        &RunId(run_id.into()),
        StepId(step_id),
        &attempt,
        |entry| {
            responses_removals
                .as_deref_mut()
                .is_none_or(|removals| removals.observe_live(step_id, entry))
        },
    );
    seams.provider.annotate_cache_evidence(&mut attempt);
    let durable = attempt.outcome != broca_protocol::AttemptOutcome::Complete;
    // The seq the record took, which a writer shared with the session actor knows only once it
    // is appended.
    let seq = writer
        .append(
            &[attempt_record(run_id, step_id, attempt.clone(), context)?],
            durable,
        )
        .await?;
    seams
        .provider
        .cache_attempt_recorded(Some(event_id(writer, seq)));
    if let Some(reports) = &seams.account_reports {
        reports.attempt_recorded_on(&event_id(writer, seq), seq, &attempt, selection);
    }
    Ok(attempt
        .account_observation
        .limit
        .is_some()
        .then(|| Box::new(FailedAccountLimitAttempt { seq, attempt })))
}

/// One HTTP attempt's record, from the provider's evidence. Billing and the session's folded
/// state read the typed fields; every other field the provider measured is kept as diagnostics
/// telemetry under the name it has on the provider's evidence. Billing details are absent: no
/// attempt in this loop is billed through a reservation yet.
/// `context` holds what every attempt of the step shares ([`AttemptContext`]).
pub(super) fn attempt_record(
    run_id: &str,
    step_id: u64,
    attempt: ModelAttempt,
    context: &AttemptContext,
) -> Result<Record, LoopError> {
    let AttemptContext {
        requested_ttl_ms,
        account_limit_pause,
        request,
    } = context;
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
        tools_digest: Some(request.tools_digest.clone()),
        system_digest: Some(request.system_digest.clone()),
        signed_floor: request.signed_floor.clone(),
    }))
}

/// What every attempt of one step records alike.
#[derive(Debug, Clone, Default)]
pub(super) struct AttemptContext {
    /// The cache lifetime the run's requests ask for, recorded on an attempt that reached the
    /// cache.
    pub requested_ttl_ms: Option<u64>,
    /// The run's frozen consent to a timed same-account pause: with it, an attempt the provider
    /// refused for the account's limit records that limit's disposition, as
    /// `account_limit_pause::attempt_record` does.
    pub account_limit_pause: bool,
    /// What the step's request offered.
    pub request: RequestFacts,
}

/// What a step's request offers its provider, for the step events that compare requests: its
/// tool list and system text as content digests, and the entry the session's signed floor goes
/// through. A digest covers content only; the step event folds in the provider, model and render
/// config that turned it into bytes (`step_events::store` in broca-module).
#[derive(Debug, Clone, Default)]
pub(super) struct RequestFacts {
    pub tools_digest: String,
    pub system_digest: String,
    pub signed_floor: Option<String>,
}

impl RequestFacts {
    /// The facts of `opts`, with the session's signed floor at `signed_floor` (an entry id). The
    /// system text is the system messages leading the prompt; a turn-scoped system message
    /// further on is conversation, not system text. Neither digest covers cache markers, which
    /// live on messages beside their content.
    pub(super) fn of(
        opts: &broca_protocol::request::CallOptions,
        signed_floor: Option<String>,
    ) -> Self {
        use sha2::{Digest as _, Sha256};
        let digest = |value: serde_json::Value| {
            Sha256::digest(value.to_string().as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let system: Vec<_> = opts
            .prompt
            .iter()
            .take_while(|message| {
                message.role == broca_protocol::content::Role::System && !message.turn_system
            })
            .map(|message| &message.content)
            .collect();
        Self {
            tools_digest: digest(json!(opts.tools.as_slice())),
            system_digest: digest(json!(system)),
            signed_floor,
        }
    }
}

/// The digest a 64-character hex string spells, or `None`.
fn hex_digest(text: &str) -> Option<Digest> {
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        out[index] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(Digest(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::InputTransformation;

    /// The system digest covers the system messages leading the prompt and nothing after them;
    /// a cache marker or a later turn-scoped system message leaves it unchanged. The tool digest
    /// follows the tool list.
    #[test]
    fn request_facts_digest_the_leading_system_text_and_the_tool_list() {
        use broca_protocol::content::{ContentBlock, Message, Role};
        let text = |role, text: &str| Message {
            turn_system: false,
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role,
            content: vec![ContentBlock::Text { text: text.into() }],
        };
        let base = broca_protocol::request::CallOptions::new(vec![
            text(Role::System, "be brief"),
            text(Role::User, "hello"),
        ]);
        let facts = RequestFacts::of(&base, Some("m-1".into()));
        assert_eq!(facts.signed_floor.as_deref(), Some("m-1"));
        assert_eq!(facts.system_digest.len(), 64);
        let mut later = base.clone();
        later.prompt[0].cache_prefix_blocks = Some(1);
        let mut reminder = text(Role::System, "a reminder");
        reminder.turn_system = true;
        later.prompt.push(reminder);
        later.prompt.push(text(Role::User, "more"));
        let same = RequestFacts::of(&later, None);
        assert_eq!(same.system_digest, facts.system_digest);
        assert_eq!(same.tools_digest, facts.tools_digest);
        let mut other = base.clone();
        other.prompt[0] = text(Role::System, "be thorough");
        assert_ne!(
            RequestFacts::of(&other, None).system_digest,
            facts.system_digest
        );
        let mut tooled = base;
        tooled.tools = std::sync::Arc::new(vec![serde_json::from_value(json!({
            "name": "read", "description": "reads a file", "input_schema": {"type": "object"},
        }))
        .unwrap()]);
        assert_ne!(
            RequestFacts::of(&tooled, None).tools_digest,
            facts.tools_digest
        );
    }

    fn model_attempt(report: Option<Vec<InputTransformation>>, unmeasured: bool) -> ModelAttempt {
        ModelAttempt {
            content_progress: Default::default(),
            attempt: 1,
            request_body_len: 1,
            request_body_sha256: "aa".repeat(32),
            http_status: Some(200),
            content_type: None,
            response_body_len: 0,
            response_body_sha256: "bb".repeat(32),
            response_body_cap: None,
            outcome: broca_protocol::provider::AttemptOutcome::Complete,
            ts_ms: 10,
            sent_at_ms: None,
            transport: None,
            websocket_request: None,
            cache_evidence: None,
            websocket_close_code: None,
            input_transformations: report,
            input_transformations_unmeasured: unmeasured,
            credential_refresh: None,
            served_account_ref: None,
            account_observation: Default::default(),
        }
    }

    /// What the attempt's evidence for the usage export reads back from the record.
    fn read_back(attempt: ModelAttempt) -> Option<Vec<InputTransformation>> {
        let record = attempt_record("run", 1, attempt, &AttemptContext::default()).unwrap();
        let evidence = crate::billing::attempt_evidence(&[(5, record)], "run");
        assert_eq!(evidence.len(), 1);
        evidence.into_iter().next().unwrap().input_transformations
    }

    /// A provider's input-transformation report survives the attempt's record and reaches the
    /// usage export's evidence verbatim; a request that didn't ask for the report reads as no
    /// report even if one were present, and a missing report stays missing.
    #[test]
    fn the_input_transformation_report_survives_the_attempt_record() {
        let removed = InputTransformation {
            kind: Some("reasoning".into()),
            path: None,
            id: Some("rs_1".into()),
            action: Some("removed".into()),
            reason: Some("prefix_mismatch".into()),
        };
        assert_eq!(
            read_back(model_attempt(Some(vec![removed.clone()]), false)),
            Some(vec![removed.clone()])
        );
        assert_eq!(
            read_back(model_attempt(Some(Vec::new()), false)),
            Some(Vec::new())
        );
        assert_eq!(read_back(model_attempt(Some(vec![removed]), true)), None);
        assert_eq!(read_back(model_attempt(None, false)), None);
    }
}
