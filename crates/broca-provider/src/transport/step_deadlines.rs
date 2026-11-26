//! A single schedule drives both transport timeouts and the live status snapshot.

use std::pin::Pin;
use std::time::Duration;

use broca_core::provider::LiveStepProgress;
use broca_protocol::provider::{ContentProgress, RunStatusStepPhase, StepLiveness};
use tokio::time::{sleep_until, Instant, Sleep};

use super::{now_millis, STREAM_CONTENT_TIMEOUT, STREAM_IDLE_TIMEOUT};

// Poll the deadline first: a record only renews a deadline if it arrived before expiry.
macro_rules! before_expiry {
    ($deadlines:expr, $future:expr) => {
        tokio::select! {
            biased;
            _ = $deadlines.timer() => None,
            result = $future => Some(result),
        }
    };
}
pub(super) use before_expiry;

pub(super) struct StepDeadlines {
    progress: Option<LiveStepProgress>,
    started: Instant,
    started_at_ms: u64,
    idle: Instant,
    content: Instant,
    content_limit: Duration,
    expiry: Pin<Box<Sleep>>,
}

impl StepDeadlines {
    pub(super) fn start(
        progress: Option<LiveStepProgress>,
        first_idle: Duration,
        content_limit: Option<Duration>,
    ) -> Self {
        let started = Instant::now();
        let started_at_ms = now_millis();
        let content_limit = content_limit.unwrap_or(STREAM_CONTENT_TIMEOUT);
        let deadlines = Self {
            progress,
            started,
            started_at_ms,
            idle: started + first_idle,
            content: started + content_limit,
            content_limit,
            expiry: Box::pin(sleep_until(started + first_idle.min(content_limit))),
        };
        if let Some(progress) = &deadlines.progress {
            if let Some(step) = progress.lock().unwrap().as_mut() {
                step.attempt_started_at_ms = started_at_ms;
                step.liveness = StepLiveness {
                    phase: Some(RunStatusStepPhase::AwaitingResponse),
                    idle_deadline_ms: Some(deadlines.at_ms(deadlines.idle)),
                    content_deadline_ms: Some(deadlines.at_ms(deadlines.content)),
                    last_event_at_ms: None,
                };
            }
        }
        deadlines
    }

    fn at_ms(&self, instant: Instant) -> u64 {
        self.started_at_ms
            .saturating_add(instant.duration_since(self.started).as_millis() as u64)
    }

    pub(super) fn idle(&self) -> Instant {
        self.idle
    }

    pub(super) fn content(&self) -> Instant {
        self.content
    }

    pub(super) fn content_limit(&self) -> Duration {
        self.content_limit
    }

    pub(super) fn timer(&mut self) -> Pin<&mut Sleep> {
        self.expiry.as_mut()
    }

    // HTTP headers end the first-byte allowance, but aren't a model stream record. A held
    // first record still appears as awaiting_response with no last_event_at_ms.
    pub(super) fn headers(&mut self) {
        self.idle = Instant::now() + STREAM_IDLE_TIMEOUT;
        self.expiry.as_mut().reset(self.idle.min(self.content));
        if let Some(progress) = &self.progress {
            if let Some(step) = progress.lock().unwrap().as_mut() {
                step.liveness.idle_deadline_ms = Some(self.at_ms(self.idle));
            }
        }
    }

    pub(super) fn record(&mut self, made_content: bool, content: Option<&ContentProgress>) {
        let now = Instant::now();
        self.idle = now + STREAM_IDLE_TIMEOUT;
        if made_content {
            self.content = now + self.content_limit;
        }
        self.expiry.as_mut().reset(self.idle.min(self.content));
        if let Some(progress) = &self.progress {
            if let Some(step) = progress.lock().unwrap().as_mut() {
                if let Some(content) = content {
                    step.last_content_at_ms = content.last_content_at_ms;
                    step.max_content_gap_ms = content.max_content_gap_ms;
                    step.max_content_gap_ended_at_ms = content.max_content_gap_ended_at_ms;
                }
                step.liveness = StepLiveness {
                    phase: Some(RunStatusStepPhase::Streaming),
                    last_event_at_ms: Some(self.at_ms(now)),
                    idle_deadline_ms: Some(self.at_ms(self.idle)),
                    content_deadline_ms: Some(self.at_ms(self.content)),
                };
            }
        }
    }

    pub(super) fn clear_model_phase(&self) {
        if let Some(progress) = &self.progress {
            if let Some(step) = progress.lock().unwrap().as_mut() {
                step.liveness.phase = None;
                step.liveness.idle_deadline_ms = None;
                step.liveness.content_deadline_ms = None;
            }
        }
    }
}

impl Drop for StepDeadlines {
    fn drop(&mut self) {
        self.clear_model_phase();
    }
}
