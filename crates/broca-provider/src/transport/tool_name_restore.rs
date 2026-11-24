//! The wrapper every live attempt reads its stream through.

use super::*;

/// Restore model-echoed tool aliases at the transport boundary without changing the canonical
/// family normalizers or any persisted message. Each request builds one map and every retry shares
/// it through a fresh wrapper, so aliases are applied only to inbound rendered events.
pub(super) struct ToolNameRestoringNormalizer {
    pub(super) inner: Box<dyn StreamNormalizer>,
    pub(super) names: Arc<crate::tool_name_alias::ToolNameMap>,
    pub(super) live_progress: Option<broca_core::provider::LiveStepProgress>,
}

impl ToolNameRestoringNormalizer {
    fn report_content(&self) {
        if let Some(progress) = &self.live_progress {
            if let Some(step) = progress.lock().unwrap().as_mut() {
                let content = self.inner.content_progress();
                step.last_content_at_ms = content.last_content_at_ms;
                step.max_content_gap_ms = content.max_content_gap_ms;
                step.max_content_gap_ended_at_ms = content.max_content_gap_ended_at_ms;
            }
        }
    }
}

impl StreamNormalizer for ToolNameRestoringNormalizer {
    fn set_raw_sse_observer(&mut self, observer: crate::wire::RawSseObserver) {
        self.inner.set_raw_sse_observer(observer);
    }
    fn observe_error_response(&mut self, response: &Value) {
        self.inner.observe_error_response(response);
    }
    fn set_response_evidence(&mut self, evidence: crate::normalize::SharedResponseEvidence) {
        self.inner.set_response_evidence(evidence);
    }
    fn websocket_telemetry(&mut self) {
        self.inner.websocket_telemetry();
    }
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.inner.content_progress()
    }
    fn private_error_diagnostics(&mut self) {
        self.inner.private_error_diagnostics();
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let result = self.inner.push(chunk);
        self.report_content();
        result.map(|events| {
            events
                .into_iter()
                .map(|event| self.names.restore_event(event))
                .collect()
        })
    }

    fn progress_marker(&self) -> u64 {
        self.inner.progress_marker()
    }

    fn observe_response_headers(&mut self, headers: &crate::wire::ResponseHeaders) {
        self.inner.observe_response_headers(headers);
    }

    fn last_event_type(&self) -> Option<&str> {
        self.inner.last_event_type()
    }

    fn step_ended(&self) -> bool {
        self.inner.step_ended()
    }

    fn keepalives_after_content(&self) -> Option<u64> {
        self.inner.keepalives_after_content()
    }

    fn event_type_counts(&self) -> Option<&[(String, u64)]> {
        self.inner.event_type_counts()
    }

    fn liveness_marker(&self) -> Option<u64> {
        self.inner.liveness_marker()
    }

    fn content_deadline(&self) -> Option<std::time::Duration> {
        self.inner.content_deadline()
    }

    fn input_transformations(&self) -> Option<Vec<broca_protocol::InputTransformation>> {
        self.inner.input_transformations()
    }

    fn input_transformations_unmeasured(&self) -> bool {
        self.inner.input_transformations_unmeasured()
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let result = self.inner.finish();
        self.report_content();
        result.map(|events| {
            events
                .into_iter()
                .map(|event| self.names.restore_event(event))
                .collect()
        })
    }
}
