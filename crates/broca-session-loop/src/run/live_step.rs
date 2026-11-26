//! Own the live progress slot at the boundary between model and tool execution.

use std::sync::Arc;

use broca_core::provider::LiveStepProgress;
use broca_protocol::provider::{InFlightRequest, InFlightStep, RunStatusStepPhase};
use broca_protocol::{request::CallOptions, StepId};

pub(super) struct Model<'a> {
    progress: Option<&'a LiveStepProgress>,
    keep_for_tools: bool,
}

impl<'a> Model<'a> {
    pub(super) fn new(progress: Option<&'a LiveStepProgress>) -> Self {
        Self {
            progress,
            keep_for_tools: false,
        }
    }

    pub(super) fn start(&self, step_id: StepId, now: u64, options: &Arc<CallOptions>) {
        if let Some(progress) = self.progress {
            *progress.lock().unwrap() = Some(InFlightStep {
                retry_awaits: None,
                report_awaits: true,
                step_id,
                started_at_ms: now,
                attempt: 0,
                attempt_started_at_ms: now,
                liveness: Default::default(),
                last_content_at_ms: None,
                max_content_gap_ms: None,
                max_content_gap_ended_at_ms: None,
                request: InFlightRequest::of(options),
            });
        }
    }

    pub(super) fn keep_for_tools(&mut self) {
        self.keep_for_tools = true;
    }
}

impl Drop for Model<'_> {
    fn drop(&mut self) {
        if let Some(progress) = self.progress {
            let mut progress = progress.lock().unwrap();
            if self.keep_for_tools {
                if let Some(step) = progress.as_mut() {
                    step.request = Default::default();
                }
            } else {
                *progress = None;
            }
        }
    }
}

pub(super) struct Tools<'a>(Option<&'a LiveStepProgress>);

impl<'a> Tools<'a> {
    pub(super) fn start(progress: Option<&'a LiveStepProgress>) -> Self {
        if let Some(progress) = progress {
            if let Some(step) = progress.lock().unwrap().as_mut() {
                step.liveness.phase = Some(RunStatusStepPhase::RunningTools);
                step.liveness.idle_deadline_ms = None;
                step.liveness.content_deadline_ms = None;
            }
        }
        Self(progress)
    }
}

impl Drop for Tools<'_> {
    fn drop(&mut self) {
        if let Some(progress) = self.0 {
            *progress.lock().unwrap() = None;
        }
    }
}
