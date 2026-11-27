//! Observability for provider changes to the input sent to a model.

use broca_protocol::{BindIdentity, RunId, StepId};

/// Provider-reported input transformations seen by this process since it started.
static INPUT_TRANSFORMATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Provider attempts this process sent without asking for the report, since it started.
static INPUT_TRANSFORMATIONS_UNMEASURED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many entries providers have listed in `input_transformations` since this process started.
/// Every entry means the provider changed what the model saw: Anthropic drops (or lets through)
/// a signed thinking block whose earlier context no longer matched, and OpenAI's Responses API
/// removes a reasoning item whose earlier context changed (`prefix_mismatch`) or that it won't
/// carry for another reason. Either way the rendered conversation was not append-only. Zero is
/// the healthy reading only for the attempts that asked for the report: see
/// [`input_transformations_unmeasured_attempts`].
pub fn input_transformations_reported() -> u64 {
    INPUT_TRANSFORMATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// How many provider attempts this process sent without asking for the
/// `input_transformations` report (every Claude Code persona request, which carries no
/// thinking-binding beta). Their transformations are unknown, not zero, and are never counted in
/// [`input_transformations_reported`].
pub fn input_transformations_unmeasured_attempts() -> u64 {
    INPUT_TRANSFORMATIONS_UNMEASURED.load(std::sync::atomic::Ordering::Relaxed)
}

/// What one attempt's record says about provider input transformations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransformationReading {
    /// The request did not ask for the report, so nothing is known.
    NotMeasured,
    /// The response carried no report although the request may have asked (or the record
    /// predates the report).
    Absent,
    /// The response reported this many transformations.
    Reported(usize),
}

impl TransformationReading {
    pub(crate) fn of(attempt: &broca_protocol::ModelAttempt) -> Self {
        if attempt.input_transformations_unmeasured {
            return Self::NotMeasured;
        }
        match &attempt.input_transformations {
            Some(entries) => Self::Reported(entries.len()),
            None => Self::Absent,
        }
    }
}

/// Count and warn about every entry of an attempt's `input_transformations`, whatever its reason.
/// A provider can silently drop mismatched signed thinking instead of refusing the request, so
/// the log line and counter expose that loss.
pub fn report_input_transformations(
    session: &BindIdentity,
    run_id: &RunId,
    step_id: StepId,
    attempt: &broca_protocol::ModelAttempt,
) {
    report_input_transformations_filtered(session, run_id, step_id, attempt, |_| true);
}

/// Count the complete report, but log only entries selected by `warn_about`. The session loop
/// uses this to avoid warning again about a Responses reasoning item already known to be removed.
pub fn report_input_transformations_filtered(
    session: &BindIdentity,
    run_id: &RunId,
    step_id: StepId,
    attempt: &broca_protocol::ModelAttempt,
    mut warn_about: impl FnMut(&broca_protocol::InputTransformation) -> bool,
) {
    let entries = match TransformationReading::of(attempt) {
        TransformationReading::NotMeasured => {
            // Not measured is not "none": counted apart, so the reported total never reads a
            // persona run's silence as a healthy zero.
            INPUT_TRANSFORMATIONS_UNMEASURED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        TransformationReading::Absent | TransformationReading::Reported(0) => return,
        TransformationReading::Reported(_) => {
            attempt.input_transformations.as_deref().unwrap_or_default()
        }
    };
    INPUT_TRANSFORMATIONS.fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
    let reasons: Vec<String> = entries
        .iter()
        .filter(|entry| warn_about(entry))
        .map(|entry| {
            // Anthropic places an entry by its path in the request; OpenAI by the item's id and
            // says what it did (`removed`).
            format!(
                "{}{}:{}@{}",
                entry.kind.as_deref().unwrap_or("?"),
                entry
                    .action
                    .as_deref()
                    .map(|action| format!("/{action}"))
                    .unwrap_or_default(),
                entry.reason.as_deref().unwrap_or("?"),
                entry.path.as_deref().or(entry.id.as_deref()).unwrap_or("?"),
            )
        })
        .collect();
    if reasons.is_empty() {
        return;
    }
    tracing::warn!(
        target: "provider",
        session = %session.session,
        run_id = %run_id.0,
        step = step_id.0,
        attempt = attempt.attempt,
        count = reasons.len(),
        transformations = ?reasons,
        "provider transformed the request input: an earlier part of the conversation changed"
    );
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn transformation_attempt(
        report: Option<Vec<broca_protocol::InputTransformation>>,
        unmeasured: bool,
    ) -> broca_protocol::ModelAttempt {
        broca_protocol::ModelAttempt {
            content_progress: Default::default(),
            attempt: 0,
            request_body_len: 1,
            request_body_sha256: "aa".into(),
            http_status: Some(200),
            content_type: None,
            response_body_len: 0,
            response_body_sha256: "bb".into(),
            response_body_cap: None,
            outcome: broca_protocol::provider::AttemptOutcome::Complete,
            ts_ms: 0,
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

    /// An attempt whose request did not ask for the report (a Claude Code persona request) is
    /// "not measured", never "no transformations", even if a report field were present.
    #[test]
    fn an_unmeasured_attempt_never_reads_as_no_transformations() {
        assert_eq!(
            TransformationReading::of(&transformation_attempt(None, true)),
            TransformationReading::NotMeasured
        );
        assert_eq!(
            TransformationReading::of(&transformation_attempt(Some(Vec::new()), true)),
            TransformationReading::NotMeasured
        );
        assert_eq!(
            TransformationReading::of(&transformation_attempt(Some(Vec::new()), false)),
            TransformationReading::Reported(0)
        );
        assert_eq!(
            TransformationReading::of(&transformation_attempt(None, false)),
            TransformationReading::Absent
        );
    }
}
