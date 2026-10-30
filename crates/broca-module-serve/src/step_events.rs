//! Best-effort, content-free `step.finished.v1` hints, separate from durable billing.
//!
//! No shipping route installs this publisher. Publishing must remain disabled until
//! broca has permission to publish on the bus's `live` root, the core publisher is
//! installed, and llm-runner 0.6.12's attempt-id event contract is published for the
//! runner release line used by broca.
//! Normal close reports this writer's open reservations after `WriterClosed` syncs.
//! After a crash, opening a writer deliberately emits no predecessor hints: replaying
//! a session for a live hint defeats bounded activation reads. Durable readers still
//! classify those reservations as possibly billed. The offset index can later answer
//! "open reservations at the last boundary" cheaply to close this live-view gap.

use async_trait::async_trait;
use broca_core::wal_writer::{DurableAttempt, RecordObserver};
use broca_wal::record::BillingAttemptOutcome;
use cortexkit_role_llm_runner::step_events::{StepFinished, StepRef};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

const QUEUE_CAPACITY: usize = 128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Config {
    pub step_events_enabled: bool,
}

/// A plain live publish, never a stream, durable outbox or billing acknowledgement.
#[async_trait]
pub trait StepEventPublisher: Send + Sync + 'static {
    async fn publish(&self, subject: &str, body: Vec<u8>) -> Result<(), String>;
}

pub struct StepEvents {
    tx: tokio::sync::mpsc::Sender<DurableAttempt>,
    dropped: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl StepEvents {
    /// With the switch off, do not allocate a queue, spawn a task or touch the bus.
    /// Shipping routes must stay disabled until the production publisher and its
    /// required permission to publish on the bus's `live` root are available.
    pub fn start(
        config: Config,
        account: &str,
        runner: &str,
        publisher: Arc<dyn StepEventPublisher>,
    ) -> Result<Option<Self>, String> {
        if !config.step_events_enabled {
            return Ok(None);
        }
        let concrete = |token: &str| {
            !token.is_empty()
                && !token
                    .bytes()
                    .any(|byte| byte.is_ascii_whitespace() || b".*>".contains(&byte))
        };
        if !concrete(account) || !concrete(runner) {
            return Err("step event account and runner must be concrete bus tokens".into());
        }
        let subject = format!("ck.{account}.live.{runner}.step.finished.v1");
        let runner = runner.to_owned();
        let (tx, mut rx) = tokio::sync::mpsc::channel(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            while let Some(attempt) = rx.recv().await {
                let body =
                    event(&runner, &attempt).and_then(|event| serde_json::to_vec(&event).ok());
                let failed = match body {
                    Some(body) => publisher.publish(&subject, body).await.is_err(),
                    None => true,
                };
                if failed {
                    // No retry: the WAL is authority, and a hint must not build a backlog.
                    task_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        Ok(Some(Self { tx, dropped, task }))
    }

    pub fn observe(&self, observer: RecordObserver) -> RecordObserver {
        observer.with_step_events(self.tx.clone(), self.dropped.clone())
    }

    /// Queue overflows, disconnected receivers, invalid evidence and failed publishes.
    /// Activation can expose this counter in health without involving the WAL writer.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for StepEvents {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn event(runner: &str, attempt: &DurableAttempt) -> Option<StepFinished> {
    let reservation = &attempt.reservation;
    let envelope = &attempt.envelope;
    let writer = attempt.writer_id?;
    let writer = writer
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let position = StepRef::new(
        runner,
        &reservation.session.session_ref,
        format!("{}.{writer}", attempt.lineage?),
        attempt.seq,
    );
    let terminal_status = if attempt.interrupted_at_ms.is_some() {
        "interrupted"
    } else {
        match envelope.outcome {
            BillingAttemptOutcome::Complete => "completed",
            BillingAttemptOutcome::Cancelled => "cancelled",
            BillingAttemptOutcome::Unknown | BillingAttemptOutcome::SealedInFlight => "interrupted",
            _ => "failed",
        }
    };
    let mut event = StepFinished::new(
        position,
        &reservation.run_id.0,
        reservation.step_id.0.to_string(),
        std::num::NonZeroU64::new(u64::from(reservation.attempt) + 1)?,
        terminal_status,
        &reservation.session.bind.harness,
        &reservation.session.bind.session,
        &reservation.provider,
        envelope
            .model_used
            .as_ref()
            .unwrap_or(&reservation.model_sent),
        envelope.sent_at_ms.unwrap_or(reservation.reserved_at_ms),
        attempt.interrupted_at_ms.or(envelope.finished_at_ms)?,
    )
    .with_attempt_id(attempt.attempt_id.as_str());
    event.work_class = reservation.run.work_class.clone();
    event.request_body_bytes = Some(reservation.request_body_bytes);
    event.context_limit = reservation.run.context_limit;
    // Interrupted reservations carry no usage measurement, including no estimated zero.
    if attempt.interrupted_at_ms.is_none() {
        let mut usage = envelope.usage.unwrap_or_default();
        let mut split_5m = envelope.cache_write_5m_tokens;
        let mut split_1h = envelope.cache_write_1h_tokens;
        if let Some(models) = &envelope.usage_by_model {
            usage = Default::default();
            split_5m = None;
            split_1h = None;
            for model in models {
                usage += model.usage;
                split_5m =
                    broca_protocol::usage::sum_reported(split_5m, model.cache_write_5m_tokens);
                split_1h =
                    broca_protocol::usage::sum_reported(split_1h, model.cache_write_1h_tokens);
            }
        }
        event.input_tokens = usage.input_tokens;
        event.cached_input_tokens = usage.cached_input_tokens;
        event.cache_write_tokens = usage.cache_write_tokens;
        event.output_tokens = usage.output_tokens;
        event.reasoning_tokens = usage.reasoning_tokens;
        event.cache_write_5m_tokens = split_5m;
        event.cache_write_1h_tokens = split_1h;
        event.reasoning_included_in_output = envelope.reasoning_included_in_output;
        event.provider_request_id = envelope.vendor_request_id.clone();
        event.finish_reason = envelope.finish_reason.and_then(|reason| {
            serde_json::to_value(reason)
                .ok()?
                .as_str()
                .map(str::to_owned)
        });
    }
    event.check().ok()?;
    Some(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_wal::{FileWal, Wal};
    use serde_json::{json, Value};

    fn vectors() -> Value {
        serde_json::from_str(cortexkit_role_llm_runner::vectors::file("step-events.json").unwrap())
            .unwrap()
    }

    fn fixture(full: bool) -> DurableAttempt {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "step-event-vector-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let wal = FileWal::open(root.join("session.wal")).unwrap();
        let lineage = wal.lineage_id();
        drop(wal);
        std::fs::remove_dir_all(root).unwrap();
        let vector = &vectors()[if full { "full" } else { "minimal" }];
        let reservation = serde_json::from_value(json!({
            "run_id":"run_a", "step_id":7, "attempt":if full {1} else {0},
            "send_index":1, "warm":false, "reserved_at_ms":vector["started_at_ms"],
            "provider":"provider_a", "endpoint_host":"example.com", "model_sent":"model_a",
            "credential":{"credential_ref":"private-credential"},
            "session":{"project_root":"/private/project", "harness":"host_a", "session":"session_a", "session_ref":"runner_session_a"},
            "run":if full {json!({"work_class":"future_work_class", "context_limit":200000})} else {json!({})},
            "request_body_bytes":4096, "compressed":false
        })).unwrap();
        let mut envelope = json!({"sent":true, "response_bytes":100, "outcome":"complete",
            "sent_at_ms":vector["started_at_ms"], "finished_at_ms":vector["finished_at_ms"]});
        if full {
            envelope.as_object_mut().unwrap().extend(json!({
                "vendor_request_id":"request_a", "finish_reason":"stop",
                "usage":{"input_tokens":100, "cached_input_tokens":50, "cache_write_tokens":30, "output_tokens":20, "reasoning_tokens":7},
                "cache_write_5m_tokens":10, "cache_write_1h_tokens":20,
                "reasoning_included_in_output":true,
                "usage_raw":{"prompt":"must not escape", "error":"private text"}
            }).as_object().unwrap().clone());
        }
        DurableAttempt {
            seq: if full { 42 } else { 43 },
            lineage,
            writer_id: Some([0x11; 16]),
            interrupted_at_ms: None,
            attempt_id: serde_json::from_value(json!("0123456789abcdef0123456789abcdef")).unwrap(),
            reservation: Box::new(reservation),
            envelope: Box::new(serde_json::from_value(envelope).unwrap()),
        }
    }

    #[test]
    fn golden_body_matches_role_step_events_vectors() {
        for full in [false, true] {
            let attempt = fixture(full);
            let event = event("runner_a", &attempt).unwrap();
            let mut expected = vectors()[if full { "full" } else { "minimal" }].clone();
            // The vectors use invented host identities; broca uses its WAL position and
            // numeric step id, and always joins on the durable random attempt identity.
            expected["log_incarnation"] = json!(format!(
                "{}.11111111111111111111111111111111",
                attempt.lineage.unwrap()
            ));
            expected["step_id"] = json!("7");
            expected["attempt_id"] = json!("0123456789abcdef0123456789abcdef");
            expected["terminal_status"] = json!("completed");
            expected["request_body_bytes"] = json!(4096);
            if full {
                expected.as_object_mut().unwrap().remove("harness_step_ref");
                expected["finish_reason"] = json!("stop");
            }
            let actual = serde_json::to_value(&event).unwrap();
            assert_eq!(actual, expected);
            for member in vectors()["refused_members"].as_array().unwrap() {
                for name in member.as_object().unwrap().keys() {
                    assert!(actual.get(name).is_none(), "content member {name} escaped");
                }
            }
            let decoded = cortexkit_role_llm_runner::step_events::decode_step_finished(
                "ck.account_a.live.runner_a.step.finished.v1",
                &serde_json::to_vec(&event).unwrap(),
            )
            .unwrap();
            assert_eq!(decoded, event);
        }
    }

    #[test]
    fn interrupted_reservation_has_no_invented_usage_or_new_position() {
        let mut attempt = fixture(false);
        attempt.interrupted_at_ms = Some(1790000003000);
        let event = event("runner_a", &attempt).unwrap();
        assert_eq!(event.terminal_status, "interrupted");
        assert_eq!(event.seq, 43);
        assert_eq!(
            event.attempt_id.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(event.input_tokens, None);
        assert_eq!(event.finished_at_ms, 1790000003000);
    }

    #[test]
    fn missing_identity_or_finish_time_drops_hint_instead_of_inventing_facts() {
        let mut attempt = fixture(false);
        attempt.writer_id = None;
        assert!(event("runner_a", &attempt).is_none());
        attempt.writer_id = Some([0x11; 16]);
        attempt.envelope.finished_at_ms = None;
        assert!(event("runner_a", &attempt).is_none());
    }

    struct Publisher {
        calls: tokio::sync::mpsc::UnboundedSender<(String, Vec<u8>)>,
        fail: bool,
    }
    #[async_trait]
    impl StepEventPublisher for Publisher {
        async fn publish(&self, subject: &str, body: Vec<u8>) -> Result<(), String> {
            self.calls.send((subject.into(), body)).unwrap();
            if self.fail {
                Err("bus unavailable".into())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn shipping_build_has_no_path_to_publish_step_events() {
        let (calls, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let publisher = Arc::new(Publisher { calls, fail: false });
        // No runtime exists here: an accidental task spawn with defaults would panic.
        assert!(!Config::default().step_events_enabled);
        assert!(
            StepEvents::start(Config::default(), "account", "broca", publisher)
                .unwrap()
                .is_none()
        );
        assert!(inbox.try_recv().is_err());
        for source in [include_str!("main.rs"), include_str!("serve.rs")] {
            for activation in ["step_events::", "step_events_enabled", ".with_step_events("] {
                assert!(
                    !source.contains(activation),
                    "shipping entry point activates {activation}"
                );
            }
        }
    }

    #[tokio::test]
    async fn enabled_publisher_uses_plain_live_subject_and_counts_failures_without_retry() {
        let (calls, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let events = StepEvents::start(
            Config {
                step_events_enabled: true,
            },
            "account_a",
            "runner_a",
            Arc::new(Publisher { calls, fail: true }),
        )
        .unwrap()
        .unwrap();
        events.tx.try_send(fixture(false)).unwrap();
        let (subject, body) = inbox.recv().await.unwrap();
        assert_eq!(subject, "ck.account_a.live.runner_a.step.finished.v1");
        cortexkit_role_llm_runner::step_events::decode_step_finished(&subject, &body).unwrap();
        assert_eq!(events.dropped(), 1);
        assert!(inbox.try_recv().is_err(), "failed hint was retried");
    }
}
