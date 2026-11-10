use super::AttemptTrace;
use std::time::Duration;
use tokio::time::Instant;

// Diagnostic names follow the same bounded, payload-free policy as chunk-error logs.
#[derive(Clone)]
struct EventName {
    bytes: [u8; 64],
    len: usize,
}

impl EventName {
    fn new(name: &str) -> Self {
        let name = crate::sse::safe_event_type_name(name);
        let mut bytes = [0; 64];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        Self {
            bytes,
            len: name.len(),
        }
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len]).expect("safe event names are ASCII")
    }
}

impl Default for EventName {
    fn default() -> Self {
        Self::new("request_sent")
    }
}

/// Log-only wire-event timing, deliberately separate from the real-output classification
/// persisted in ContentProgress. Lifecycle and metadata events also end a silent gap here.
#[derive(Clone, Default)]
pub(super) struct ContentGaps {
    started: Option<Instant>,
    ended: Option<Instant>,
    first: Option<Duration>,
    last: Duration,
    last_event: EventName,
    max: Duration,
    max_after: EventName,
}

pub(super) struct Snapshot<'a> {
    pub max_content_gap_ms: Option<u64>,
    pub first_content_ms: Option<u64>,
    pub tail_gap_ms: Option<u64>,
    pub max_gap_after_event: &'a str,
}

impl ContentGaps {
    pub(super) fn start(&mut self) {
        *self = Self {
            started: Some(Instant::now()),
            ..Self::default()
        };
    }

    pub(super) fn observe(&mut self, event: Option<&str>) {
        let Some(started) = self.started else { return };
        let elapsed = started.elapsed();
        let gap = elapsed.saturating_sub(self.last);
        if gap > self.max {
            self.max = gap;
            self.max_after = self.last_event.clone();
        }
        self.first.get_or_insert(elapsed);
        self.last = elapsed;
        self.last_event = EventName::new(event.unwrap_or("unknown"));
    }

    pub(super) fn finish(&mut self) -> bool {
        if self.ended.is_some() {
            return false;
        }
        self.ended = Some(Instant::now());
        true
    }

    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        let tail = self.started.map(|started| {
            self.ended
                .unwrap_or_else(Instant::now)
                .saturating_duration_since(started)
                .saturating_sub(self.last)
        });
        let tail_is_max = tail.is_some_and(|tail| tail > self.max);
        Snapshot {
            max_content_gap_ms: tail.map(|tail| self.max.max(tail).as_millis() as u64),
            first_content_ms: self.first.map(|first| first.as_millis() as u64),
            tail_gap_ms: tail.map(|tail| tail.as_millis() as u64),
            max_gap_after_event: if self.started.is_none() {
                "none"
            } else if tail_is_max {
                self.last_event.as_str()
            } else {
                self.max_after.as_str()
            },
        }
    }
}

pub(super) struct SseObservationGuard<'a> {
    pub trace: &'a mut AttemptTrace,
    pub failed: bool,
}

impl<'a> SseObservationGuard<'a> {
    pub(super) fn begin(trace: &'a mut AttemptTrace) -> Self {
        Self {
            trace,
            failed: true,
        }
    }
}

impl Drop for SseObservationGuard<'_> {
    fn drop(&mut self) {
        log_sse_attempt(self.trace, self.failed);
    }
}

pub(super) fn log_sse_attempt(trace: &mut AttemptTrace, failed: bool) {
    if !trace.content_gaps.finish() {
        return;
    }
    let gaps = trace.content_gaps.snapshot();
    macro_rules! emit {
            ($level:ident) => {
                tracing::$level!(target: "provider.sse.attempt",
                    transport = "sse", failed,
                    max_content_gap_ms = gaps.max_content_gap_ms,
                    first_content_ms = gaps.first_content_ms,
                    tail_gap_ms = gaps.tail_gap_ms,
                    max_gap_after_event = if trace.ephemeral_session { "private" } else { gaps.max_gap_after_event },
                    "SSE attempt summary"
                );
            };
        }
    if failed
        || trace
            .elapsed_since_send()
            .is_some_and(|elapsed| elapsed > Duration::from_secs(120))
    {
        emit!(info);
    } else {
        emit!(debug);
    }
}

#[cfg(test)]
mod tests {
    use super::super::{drive_body_stream_traced, responses_websocket, AttemptOutcome};
    use super::*;
    use crate::families::openai_responses::OpenAiResponsesNormalizer;
    use broca_protocol::StepId;
    use futures::StreamExt;

    async fn scripted_summary(
        websocket: bool,
        ephemeral: bool,
    ) -> Vec<broca_test_support::LogEvent> {
        let mut trace = AttemptTrace {
            transport: Some(if websocket { "websocket" } else { "https" }.into()),
            ephemeral_session: ephemeral,
            ..Default::default()
        };
        trace.content_gaps.start();
        let content = |kind: &str| {
            let json = format!(r#"{{"type":"{kind}"}}"#);
            if websocket {
                json.into_bytes()
            } else {
                format!("data: {json}\n\n").into_bytes()
            }
        };
        // The socket pump delivers Ping and Pong as empty records; the body reader adapts
        // those records into SSE comments. Neither form should reset the content clock.
        let keepalive = if websocket {
            Vec::new()
        } else {
            b": keepalive\n\n".to_vec()
        };
        let script: Vec<(u64, Result<Vec<u8>, std::io::Error>)> = vec![
            (7, Ok(content("response.created"))),
            (4, Ok(keepalive.clone())),
            (6, Ok(keepalive)),
            (10, Ok(content("response.in_progress"))),
            (5, Err(std::io::Error::other("scripted end"))),
        ];
        let stream = futures::stream::iter(script).then(|(ms, chunk)| async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            chunk
        });
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let normalizer = Box::new(OpenAiResponsesNormalizer::new(StepId(1)));
        let (_, logs) = broca_test_support::capture_logs(async {
            let failure = if websocket {
                let guard = responses_websocket::ObservationGuard::begin(&mut trace);
                drive_body_stream_traced(stream, normalizer, &tx, usize::MAX, guard.trace).await
            } else {
                let guard = SseObservationGuard::begin(&mut trace);
                drive_body_stream_traced(stream, normalizer, &tx, usize::MAX, guard.trace).await
            }
            .unwrap_err();
            assert_eq!(failure.outcome, AttemptOutcome::ChunkError);
        })
        .await;
        // Lifecycle events count for these logs even though they are not real model output.
        assert_eq!(trace.content_progress.first_content_at_ms, None);
        logs
    }

    fn assert_summary(logs: &[broca_test_support::LogEvent], message: &str) {
        let summaries: Vec<_> = logs.iter().filter(|log| log.message() == message).collect();
        assert_eq!(summaries.len(), 1);
        let summary = summaries[0];
        assert_eq!(summary.fields["max_content_gap_ms"], "20");
        assert_eq!(summary.fields["first_content_ms"], "7");
        assert_eq!(summary.fields["tail_gap_ms"], "5");
        assert_eq!(summary.fields["max_gap_after_event"], "response.created");
        let warning = logs
            .iter()
            .find(|log| log.message() == "model attempt ended chunk_error")
            .unwrap();
        for field in [
            "max_content_gap_ms",
            "first_content_ms",
            "tail_gap_ms",
            "max_gap_after_event",
        ] {
            assert_eq!(warning.fields[field], summary.fields[field], "{field}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sse_content_gap_summary_ignores_keepalives() {
        let logs = scripted_summary(false, false).await;
        assert_summary(&logs, "SSE attempt summary");
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_content_gap_summary_ignores_ping_pong() {
        let logs = scripted_summary(true, false).await;
        assert_summary(&logs, "Responses websocket attempt summary");
    }

    #[tokio::test(start_paused = true)]
    async fn ephemeral_attempt_logs_keep_timings_but_withhold_event_names() {
        for websocket in [false, true] {
            let logs = scripted_summary(websocket, true).await;
            let timed: Vec<_> = logs
                .iter()
                .filter(|log| log.fields.contains_key("max_content_gap_ms"))
                .collect();
            assert_eq!(timed.len(), 2, "summary and warning both retain timings");
            for log in timed {
                assert_eq!(log.fields["max_content_gap_ms"], "20");
                assert_eq!(log.fields["first_content_ms"], "7");
                assert_eq!(log.fields["tail_gap_ms"], "5");
                assert_eq!(log.fields["max_gap_after_event"], "private");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn initial_and_tail_gaps_can_be_the_longest() {
        let mut gaps = ContentGaps::default();
        assert_eq!(gaps.snapshot().max_content_gap_ms, None);
        assert_eq!(gaps.snapshot().max_gap_after_event, "none");
        gaps.start();
        tokio::time::advance(Duration::from_millis(30)).await;
        gaps.observe(Some("response.created"));
        tokio::time::advance(Duration::from_millis(5)).await;
        gaps.observe(Some("response.in_progress"));
        let initial = gaps.snapshot();
        assert_eq!(initial.max_content_gap_ms, Some(30));
        assert_eq!(initial.first_content_ms, Some(30));
        assert_eq!(initial.max_gap_after_event, "request_sent");
        tokio::time::advance(Duration::from_millis(40)).await;
        gaps.finish();
        tokio::time::advance(Duration::from_secs(1)).await;
        let tail = gaps.snapshot();
        assert_eq!(tail.max_content_gap_ms, Some(40));
        assert_eq!(tail.tail_gap_ms, Some(40));
        assert_eq!(tail.max_gap_after_event, "response.in_progress");
    }

    #[tokio::test(start_paused = true)]
    async fn no_events_measure_the_whole_sent_attempt() {
        let mut gaps = ContentGaps::default();
        gaps.start();
        tokio::time::advance(Duration::from_millis(42)).await;
        gaps.finish();
        let snapshot = gaps.snapshot();
        assert_eq!(snapshot.first_content_ms, None);
        assert_eq!(snapshot.max_content_gap_ms, Some(42));
        assert_eq!(snapshot.tail_gap_ms, Some(42));
        assert_eq!(snapshot.max_gap_after_event, "request_sent");
    }

    #[tokio::test(start_paused = true)]
    async fn compression_resends_log_each_send_without_duplicate_summaries() {
        let mut trace = AttemptTrace::default();
        let (_, logs) = broca_test_support::capture_logs(async {
            let mut guard = SseObservationGuard::begin(&mut trace);
            guard.trace.content_gaps.start();
            tokio::time::advance(Duration::from_millis(40)).await;
            log_sse_attempt(guard.trace, true);
            tokio::time::advance(Duration::from_millis(100)).await;
            log_sse_attempt(guard.trace, true);
            guard.trace.content_gaps.start();
            tokio::time::advance(Duration::from_millis(20)).await;
            guard.failed = false;
        })
        .await;
        let summaries: Vec<_> = logs
            .iter()
            .filter(|log| log.message() == "SSE attempt summary")
            .collect();
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].fields["tail_gap_ms"], "40");
        assert_eq!(summaries[1].fields["tail_gap_ms"], "20");
    }

    #[tokio::test(start_paused = true)]
    async fn refused_websocket_upgrade_does_not_borrow_sse_timing() {
        let mut trace = AttemptTrace::default();
        let (_, logs) = broca_test_support::capture_logs(async {
            let guard = responses_websocket::ObservationGuard::begin(&mut trace);
            guard.trace.transport = Some("https".into());
            guard.trace.content_gaps.start();
            tokio::time::advance(Duration::from_millis(40)).await;
            log_sse_attempt(guard.trace, false);
        })
        .await;
        let websocket = logs
            .iter()
            .find(|log| log.message() == "Responses websocket attempt summary")
            .unwrap();
        assert!(!websocket.fields.contains_key("max_content_gap_ms"));
        assert!(!websocket.fields.contains_key("tail_gap_ms"));
        assert_eq!(websocket.fields["max_gap_after_event"], "none");
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_freezes_each_transports_tail_gap() {
        for websocket in [false, true] {
            let mut trace = AttemptTrace {
                transport: Some(if websocket { "websocket" } else { "https" }.into()),
                ..Default::default()
            };
            trace.content_gaps.start();
            let stream = futures::stream::once(async move {
                tokio::time::sleep(Duration::from_millis(7)).await;
                let raw = if websocket {
                    br#"{"type":"response.created"}"#.to_vec()
                } else {
                    b"data: {\"type\":\"response.created\"}\n\n".to_vec()
                };
                Ok::<_, std::io::Error>(raw)
            })
            .chain(futures::stream::pending());
            let normalizer = Box::new(OpenAiResponsesNormalizer::new(StepId(1)));
            let (tx, _rx) = tokio::sync::mpsc::channel(16);
            let (_, logs) = broca_test_support::capture_logs(async {
                let result = tokio::time::timeout(Duration::from_millis(30), async {
                    if websocket {
                        let guard = responses_websocket::ObservationGuard::begin(&mut trace);
                        drive_body_stream_traced(stream, normalizer, &tx, usize::MAX, guard.trace)
                            .await
                    } else {
                        let guard = SseObservationGuard::begin(&mut trace);
                        drive_body_stream_traced(stream, normalizer, &tx, usize::MAX, guard.trace)
                            .await
                    }
                })
                .await;
                assert!(result.is_err());
            })
            .await;
            let summary = logs
                .iter()
                .find(|log| log.message().ends_with("attempt summary"))
                .unwrap();
            assert_eq!(summary.fields["first_content_ms"], "7");
            assert_eq!(summary.fields["tail_gap_ms"], "23");
            assert_eq!(summary.fields["max_content_gap_ms"], "23");
            assert_eq!(summary.fields["max_gap_after_event"], "response.created");
            tokio::time::advance(Duration::from_secs(1)).await;
            assert_eq!(trace.content_gaps.snapshot().tail_gap_ms, Some(23));
        }
    }
}
