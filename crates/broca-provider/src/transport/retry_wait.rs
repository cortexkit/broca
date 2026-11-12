//! Ephemeral status while the attempt driver owns a retry delay.

use super::*;
use broca_protocol::provider::{AwaitParty, RunAwaits};

pub(super) struct Wait {
    progress: broca_core::provider::LiveStepProgress,
    run_id: broca_protocol::RunId,
    wait: RunAwaits,
    published: bool,
}

impl Wait {
    pub(super) fn new(
        seal: &StepSeal<'_>,
        run_id: Option<&str>,
        error: &ProviderError,
        delay: Duration,
        attempt: u32,
    ) -> Option<Self> {
        let progress = seal.progress.as_ref()?;
        if !progress.lock().unwrap().as_ref()?.report_awaits {
            return None;
        }
        let since = now_millis();
        let mut wait = RunAwaits::new(AwaitParty::Runner, since);
        wait.message = broca_protocol::provider::await_message(error);
        wait.next_ms = Some(since.saturating_add(delay.as_millis().try_into().unwrap_or(u64::MAX)));
        wait.attempt = Some(attempt);
        wait.reason = Some(
            match error.status {
                Some(429) => "rate_limited",
                Some(503 | 529) => "server_busy",
                _ => "provider_retry",
            }
            .to_owned()
            .into(),
        );
        Some(Self {
            progress: Arc::clone(progress),
            run_id: broca_protocol::RunId(run_id?.to_owned()),
            wait,
            published: false,
        })
    }

    pub(super) async fn publish(&mut self, tx: &mpsc::Sender<NormalizedEvent>) {
        self.published = true;
        if let Some(step) = self.progress.lock().unwrap().as_mut() {
            step.retry_awaits = Some(Box::new(self.wait.clone()));
        }
        // Old readers decode run_status as a display reset. These frames are sent only after
        // abandoning an attempt and before the next attempt, never during a live generation.
        let event = self.event(Some(self.wait.clone()));
        let _ = tx.send(NormalizedEvent::Display(event)).await;
    }

    pub(super) async fn finish(self, tx: &mpsc::Sender<NormalizedEvent>) {
        self.clear();
        if self.published {
            let _ = tx.send(NormalizedEvent::Display(self.event(None))).await;
        }
    }

    fn clear(&self) {
        if let Some(step) = self.progress.lock().unwrap().as_mut() {
            step.retry_awaits = None;
        }
    }

    fn event(&self, awaits: Option<RunAwaits>) -> DisplayEvent {
        broca_protocol::provider::retry_status_display(
            self.run_id.clone(),
            awaits.as_ref(),
            now_millis(),
        )
    }
}

impl Drop for Wait {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(report_awaits: bool) -> broca_core::provider::LiveStepProgress {
        Arc::new(std::sync::Mutex::new(Some(
            broca_protocol::provider::InFlightStep {
                report_awaits,
                retry_awaits: None,
                step_id: StepId(1),
                started_at_ms: 1,
                attempt: 0,
                attempt_started_at_ms: 1,
                last_content_at_ms: None,
                max_content_gap_ms: None,
                max_content_gap_ended_at_ms: None,
                request: Default::default(),
            },
        )))
    }

    fn seal(progress: &broca_core::provider::LiveStepProgress) -> StepSeal<'_> {
        StepSeal {
            progress: Some(progress),
            seal: None,
            step_id: StepId(1),
            cache_ttl_ms: None,
            first_attempt: 0,
            route: broca_core::refusal_fallback::StepRoute::Source,
        }
    }

    #[tokio::test]
    async fn run_awaits_transport_retries_classify_and_clear_before_the_next_attempt() {
        for (status, reason) in [
            (Some(429), "rate_limited"),
            (Some(503), "server_busy"),
            (Some(529), "server_busy"),
            (Some(500), "provider_retry"),
            (None, "provider_retry"),
        ] {
            let progress = progress(true);
            let mut error = transport_error("a transport diagnostic of unknown origin");
            error.status = status;
            let mut wait = Wait::new(
                &seal(&progress),
                Some("r1"),
                &error,
                Duration::from_secs(2),
                2,
            )
            .unwrap();
            assert_eq!(wait.wait.next_ms, Some(wait.wait.since_ms + 2000));
            assert_eq!(wait.wait.attempt, Some(2));
            assert_eq!(
                wait.wait.reason.clone().map(String::from).as_deref(),
                Some(reason)
            );
            assert!(wait.wait.message.is_none());
            let (tx, mut rx) = mpsc::channel(4);
            wait.publish(&tx).await;
            assert!(progress
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .retry_awaits
                .is_some());
            assert!(matches!(
                rx.recv().await.unwrap(),
                NormalizedEvent::Display(DisplayEvent::RunStatus {
                    awaits: Some(_),
                    ..
                })
            ));
            wait.finish(&tx).await;
            assert!(progress
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .retry_awaits
                .is_none());
            assert!(matches!(
                rx.recv().await.unwrap(),
                NormalizedEvent::Display(DisplayEvent::RunStatus { awaits: None, .. })
            ));
        }
        let progress = progress(false);
        assert!(Wait::new(
            &seal(&progress),
            Some("r1"),
            &transport_error("legacy"),
            Duration::from_secs(2),
            2
        )
        .is_none());
    }

    #[tokio::test]
    async fn run_awaits_transport_cancel_drops_the_in_memory_wait() {
        let progress = progress(true);
        let mut wait = Wait::new(
            &seal(&progress),
            Some("r1"),
            &transport_error("cancelled"),
            Duration::from_secs(2),
            2,
        )
        .unwrap();
        let (tx, _rx) = mpsc::channel(4);
        wait.publish(&tx).await;
        drop(wait);
        assert!(progress
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .retry_awaits
            .is_none());
    }
}
