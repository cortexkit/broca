//! Best-effort, content-free `step.finished.v1` hints, separate from durable billing.
//!
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

pub(crate) type WarmStops = Arc<std::sync::Mutex<std::collections::BTreeMap<String, String>>>;

/// A plain live publish, never a stream, durable outbox or billing acknowledgement.
#[async_trait]
pub trait StepEventPublisher: Send + Sync + 'static {
    async fn publish(&self, subject: &str, body: Vec<u8>) -> Result<(), String>;

    /// Resolve the daemon-attested account lazily, off the run's durability path.
    async fn live_subject(&self, _runner: &str) -> Result<String, String> {
        Err("step event publisher has no bus account resolver".into())
    }
}

pub struct StepEvents {
    tx: tokio::sync::mpsc::Sender<DurableAttempt>,
    store_tx: tokio::sync::mpsc::Sender<StepFinished>,
    dropped: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl StepEvents {
    pub fn start(
        account: &str,
        runner: &str,
        publisher: Arc<dyn StepEventPublisher>,
    ) -> Result<Self, String> {
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
        Ok(Self::spawn(Some(subject), runner, publisher))
    }

    /// Bus credentials are resolved only by the publisher task, never by a session open.
    pub fn start_on_bus(publisher: Arc<dyn StepEventPublisher>) -> Self {
        Self::spawn(None, "broca", publisher)
    }

    fn spawn(
        subject: Option<String>,
        runner: &str,
        publisher: Arc<dyn StepEventPublisher>,
    ) -> Self {
        let runner = runner.to_owned();
        let (tx, mut rx) = tokio::sync::mpsc::channel(QUEUE_CAPACITY);
        let (store_tx, mut store_rx) = tokio::sync::mpsc::channel(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let mut last_warning = None;
            loop {
                let event = tokio::select! {
                    Some(attempt) = rx.recv() => event(&runner, &attempt),
                    Some(event) = store_rx.recv() => Some(event),
                    else => break,
                };
                let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    let body = event
                        .and_then(|event| serde_json::to_vec(&event).ok())
                        .ok_or("invalid step event evidence")?;
                    let subject = match &subject {
                        Some(subject) => subject.clone(),
                        None => publisher.live_subject(&runner).await?,
                    };
                    publisher.publish(&subject, body).await
                })
                .await;
                if !matches!(result, Ok(Ok(()))) {
                    // No retry: the WAL is authority, and a hint must not build a backlog.
                    task_dropped.fetch_add(1, Ordering::Relaxed);
                    let now = tokio::time::Instant::now();
                    if last_warning.is_none_or(|last| {
                        now.duration_since(last) >= std::time::Duration::from_secs(60)
                    }) {
                        tracing::warn!(target: "broca.step_events", "live step event dropped; durable attempt evidence is unchanged");
                        last_warning = Some(now);
                    }
                }
            }
        });
        Self {
            tx,
            store_tx,
            dropped,
            task,
        }
    }

    /// Publishes the step event of a fork attempt's model call, best effort as every step event
    /// is. It is an ordinary event of the fork's own session and the attempt's own run, which
    /// keep it apart from the head's steps.
    pub(crate) fn publish_fork(&self, event: StepFinished) {
        if event.check().is_err() || self.store_tx.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn observe(&self, observer: RecordObserver) -> RecordObserver {
        observer.with_step_events(self.tx.clone(), self.dropped.clone())
    }

    pub(crate) fn store_observer(
        &self,
        bind: broca_protocol::BindIdentity,
        incarnation: [u8; 16],
        first_live_seq: u64,
        parts: broca_session_loop::parts::SharedParts,
        warm_stops: WarmStops,
    ) -> store::Observer {
        store::Observer::new(
            bind,
            incarnation,
            first_live_seq,
            self.store_tx.clone(),
            self.dropped.clone(),
            parts,
            warm_stops,
        )
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

struct Facts<'a> {
    position: StepRef,
    run_id: &'a str,
    step_id: u64,
    attempt: u32,
    status: &'a str,
    harness: &'a str,
    session: &'a str,
    provider: &'a str,
    model: &'a str,
    started_at_ms: u64,
    finished_at_ms: u64,
    attempt_id: &'a str,
}

impl Facts<'_> {
    fn build(self) -> Option<StepFinished> {
        Some(
            StepFinished::new(
                self.position,
                self.run_id,
                self.step_id.to_string(),
                std::num::NonZeroU64::new(u64::from(self.attempt) + 1)?,
                self.status,
                self.harness,
                self.session,
                self.provider,
                self.model,
                self.started_at_ms,
                self.finished_at_ms,
            )
            .with_attempt_id(self.attempt_id),
        )
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
    let event = Facts {
        position,
        run_id: &reservation.run_id.0,
        step_id: reservation.step_id.0,
        attempt: reservation.attempt,
        status: terminal_status,
        harness: &reservation.session.bind.harness,
        session: &reservation.session.bind.session,
        provider: &reservation.provider,
        model: envelope
            .model_used
            .as_ref()
            .unwrap_or(&reservation.model_sent),
        started_at_ms: envelope.sent_at_ms.unwrap_or(reservation.reserved_at_ms),
        finished_at_ms: attempt.interrupted_at_ms.or(envelope.finished_at_ms)?,
        attempt_id: attempt.attempt_id.as_str(),
    }
    .build()?;
    let mut event = finish(event, envelope, terminal_status == "interrupted");
    event.work_class = reservation.run.work_class.clone();
    event.request_body_bytes = Some(reservation.request_body_bytes);
    event.context_limit = reservation.run.context_limit;
    event
        .route_decision_id
        .clone_from(&attempt.route_decision_id);
    event.sent_to_last_content_ms = attempt.sent_to_last_content_ms;
    event.first_to_last_content_ms = attempt.first_to_last_content_ms;
    if let Some(cache) = &attempt.cache {
        event.cache = cache_report(
            &cache.frozen_render_config,
            &event.provider,
            cache.keep_warm,
            cache.stop_reason.as_deref(),
        );
    }
    event.check().ok()?;
    Some(event)
}

fn cache_report(
    frozen: &serde_json::Value,
    provider: &str,
    keep_warm: Option<broca_protocol::request::KeepWarm>,
    stop: Option<&str>,
) -> Option<cortexkit_role_llm_runner::cache::Report> {
    use broca_provider::wire::{cache_lifetime, CacheLifetime};
    use cortexkit_role_llm_runner::cache::{Report, Warming};
    let lifetime = (|| {
        let family = serde_json::from_value(frozen.get("family_id")?.clone()).ok()?;
        let policy = frozen
            .get("cache")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .ok()?;
        cache_lifetime(policy.as_ref(), family, provider)
    })();
    // A route whose cache lifetime broca can't tell omits `cache` entirely, warming included:
    // the contract reads a missing object as unknown, and a present one must carry exactly one
    // lifetime form.
    let mut report = match lifetime? {
        CacheLifetime::Explicit(ttl) => Report::explicit(ttl),
        CacheLifetime::Implicit => Report::implicit(),
    };
    report.warming = match keep_warm {
        Some(broca_protocol::request::KeepWarm::Off) => Some(Warming::new(false)),
        Some(broca_protocol::request::KeepWarm::Auto) => match stop {
            None | Some("tools_done" | "cancelled") => Some(Warming::new(true)),
            Some("budget") => Some(Warming::stopped("cost-cap")),
            Some("count_cap") => Some(Warming::stopped("count-cap")),
            Some("outside_hours") => Some(Warming::stopped("outside-hours")),
            Some("unsupported" | "warm_missed" | "quota") => Some(Warming::stopped("unsupported")),
            Some("session_close" | "session_moving" | "session_moved") => {
                Some(Warming::stopped("session-ended"))
            }
            Some(_) => None,
        },
        None => None,
    };
    Some(report)
}

/// A fork attempt's model call, as its step event reports it (see [`fork_step_event`]).
pub struct ForkStep<'a> {
    /// The fork's own session, and the incarnation of its log.
    pub session: &'a broca_protocol::BindIdentity,
    pub incarnation: [u8; 16],
    /// The seq of the fork record that stored the attempt's end.
    pub seq: u64,
    /// The attempt's own run identity, which also names the event's attempt id.
    pub run_id: &'a str,
    /// The attempt's number for its ask, counted from 1.
    pub attempt: u64,
    pub provider: &'a str,
    pub model: &'a str,
    pub usage: broca_protocol::Usage,
    pub finish_reason: Option<broca_protocol::content::FinishReason>,
    pub failed: bool,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
}

/// The step event of a fork attempt's model call: one step of the attempt's own run, in the
/// fork's own session, with the usage the provider reported.
pub fn fork_step_event(step: &ForkStep<'_>) -> Option<StepFinished> {
    // A fork attempt's run id (`fork:` and a digest) identifies the attempt, but an attempt id
    // holds only letters, digits, `_` and `-`.
    let attempt_id = step.run_id.replace(':', "-");
    let incarnation: String = step
        .incarnation
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let event = Facts {
        position: StepRef::new(
            "broca",
            broca_core::agent_session_ref(step.session),
            &incarnation,
            step.seq,
        ),
        run_id: step.run_id,
        step_id: 1,
        attempt: u32::try_from(step.attempt.checked_sub(1)?).ok()?,
        status: if step.failed { "failed" } else { "completed" },
        harness: &step.session.harness,
        session: &step.session.session,
        provider: step.provider,
        model: step.model,
        started_at_ms: step.started_at_ms,
        finished_at_ms: step.finished_at_ms,
        attempt_id: &attempt_id,
    }
    .build()?;
    let envelope = broca_wal::record::AttemptEnvelope {
        usage: Some(step.usage),
        finish_reason: step.finish_reason,
        // Usage is canonical: recorded reasoning is a subset of output, not extra output.
        reasoning_included_in_output: step.usage.reasoning_tokens.map(|_| true),
        ..Default::default()
    };
    let event = finish(event, &envelope, false);
    event.check().ok()?;
    Some(event)
}

/// Both storages use the same usage projection. Missing or interrupted evidence stays absent.
fn finish(
    mut event: StepFinished,
    envelope: &broca_wal::record::AttemptEnvelope,
    interrupted: bool,
) -> StepFinished {
    if !interrupted {
        let mut usage = envelope.usage.unwrap_or_default();
        let mut split_5m = envelope.cache_write_5m_tokens;
        let mut split_1h = envelope.cache_write_1h_tokens;
        if let Some(models) = &envelope.usage_by_model {
            usage = Default::default();
            split_5m = Some(0u64);
            split_1h = Some(0u64);
            for model in models {
                usage += model.usage;
                split_5m = split_5m
                    .zip(model.cache_write_5m_tokens)
                    .and_then(|(sum, n)| sum.checked_add(n));
                split_1h = split_1h
                    .zip(model.cache_write_1h_tokens)
                    .and_then(|(sum, n)| sum.checked_add(n));
            }
        }
        // A partial split is not a breakdown of the total; keep both lifetimes unknown.
        if split_5m
            .zip(split_1h)
            .and_then(|(a, b)| a.checked_add(b))
            .is_none_or(|sum| Some(sum) != usage.cache_write_tokens)
        {
            split_5m = None;
            split_1h = None;
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
    event
}

mod store;

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
            route_decision_id: None,
            sent_to_last_content_ms: None,
            first_to_last_content_ms: None,
            cache: None,
        }
    }

    #[test]
    fn synthetic_step_cache_samples() {
        use broca_protocol::request::KeepWarm::{Auto, Off};
        use broca_provider::{
            spec::WireFamilyId,
            wire::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax},
        };
        let dir = std::env::var_os("BROCA_STEP_CACHE_SAMPLES_DIR").map(std::path::PathBuf::from);
        if let Some(dir) = &dir {
            std::fs::create_dir_all(dir).unwrap();
        }
        for (name, provider, model) in [
            ("anthropic-5m-off", "anthropic", "claude-synthetic"),
            ("anthropic-1h-off", "anthropic", "claude-synthetic"),
            ("chatgpt-implicit-off", "openai-chatgpt", "gpt-synthetic"),
            ("anthropic-5m-auto-on", "anthropic", "claude-synthetic"),
            ("anthropic-5m-auto-stopped", "anthropic", "claude-synthetic"),
        ] {
            let mut attempt = fixture(true);
            attempt.reservation.provider = provider.into();
            attempt.reservation.model_sent = model.into();
            let chatgpt = provider == "openai-chatgpt";
            let ttl = if name.contains("1h") {
                CacheTtl::OneHour
            } else {
                CacheTtl::FiveMinutes
            };
            let policy = CachePolicy {
                version: 1,
                class: CacheClass::Markers {
                    syntax: MarkerSyntax::AnthropicCacheControl,
                    ttl,
                    max_breakpoints: 4,
                    lookback_blocks: 20,
                    tail: None,
                    omit_ttl: ttl == CacheTtl::FiveMinutes,
                },
            };
            let family = if chatgpt {
                WireFamilyId::OpenAiResponsesChatgpt
            } else {
                WireFamilyId::AnthropicMessages
            };
            let stopped = name.ends_with("stopped");
            attempt.cache = Some(broca_core::wal_writer::StepEventCache {
                frozen_render_config: json!({"family_id":family, "cache":if chatgpt { Value::Null } else { serde_json::to_value(policy).unwrap() }}),
                keep_warm: Some(if name.contains("auto") { Auto } else { Off }),
                stop_reason: stopped.then(|| "budget".into()),
            });
            let event = event("runner_a", &attempt).unwrap();
            let value = serde_json::to_value(&event).unwrap();
            if chatgpt {
                assert_eq!(value["cache"]["implicit"], true);
                assert!(value["cache"].get("ttl_ms").is_none());
            } else {
                assert_eq!(
                    value["cache"]["ttl_ms"],
                    if ttl == CacheTtl::OneHour {
                        3_600_000
                    } else {
                        300_000
                    }
                );
            }
            assert_eq!(
                value["cache"]["warming"]["on"],
                name.contains("auto") && !stopped
            );
            assert_eq!(
                value["cache"]["warming"].get("stop_reason"),
                stopped.then_some(&json!("cost-cap"))
            );
            let body = serde_json::to_vec_pretty(&event).unwrap();
            if let Some(dir) = &dir {
                std::fs::write(dir.join(format!("{name}.json")), body).unwrap();
            }
        }
    }

    #[test]
    fn warming_report_preserves_unknown_and_maps_only_session_stops() {
        use broca_protocol::request::KeepWarm::{Auto, Off};
        use broca_provider::spec::WireFamilyId;
        use cortexkit_role_llm_runner::cache::Warming;
        // ChatGPT's cache is implicit, so its steps always have a lifetime to report.
        let implicit = json!({"family_id": WireFamilyId::OpenAiResponsesChatgpt, "cache": null});
        for (stop, reason) in [
            ("budget", "cost-cap"),
            ("count_cap", "count-cap"),
            ("outside_hours", "outside-hours"),
            ("unsupported", "unsupported"),
            ("warm_missed", "unsupported"),
            ("quota", "unsupported"),
            ("session_close", "session-ended"),
            ("session_moving", "session-ended"),
            ("session_moved", "session-ended"),
        ] {
            let report = cache_report(&implicit, "openai", Some(Auto), Some(stop)).unwrap();
            assert_eq!(report.warming, Some(Warming::stopped(reason)));
            report.check().unwrap();
        }
        for stop in [None, Some("tools_done"), Some("cancelled")] {
            assert_eq!(
                cache_report(&implicit, "openai", Some(Auto), stop)
                    .unwrap()
                    .warming,
                Some(Warming::new(true))
            );
        }
        assert_eq!(
            cache_report(&implicit, "openai", Some(Off), Some("budget"))
                .unwrap()
                .warming,
            Some(Warming::new(false))
        );
        let unknown = cache_report(&implicit, "openai", Some(Auto), Some("future-stop")).unwrap();
        assert_eq!(unknown.implicit, Some(true));
        assert_eq!(unknown.warming, None);
        // No lifetime means no cache object, even when warming is known.
        for keep_warm in [None, Some(Auto), Some(Off)] {
            assert_eq!(
                cache_report(&Value::Null, "synthetic", keep_warm, Some("budget")),
                None
            );
        }
    }

    #[test]
    fn multi_model_cache_write_split_requires_every_model_and_exact_total() {
        let mut attempt = fixture(true);
        for (second, total, expected) in [
            (
                json!({"model_used":"b", "usage":{"cache_write_tokens":7}}),
                37,
                None,
            ),
            (
                json!({"model_used":"b", "usage":{"cache_write_tokens":7}, "cache_write_5m_tokens":3, "cache_write_1h_tokens":4}),
                37,
                Some((13, 24)),
            ),
            (
                json!({"model_used":"b", "usage":{"cache_write_tokens":8}, "cache_write_5m_tokens":3, "cache_write_1h_tokens":4}),
                38,
                None,
            ),
            (
                json!({"model_used":"b", "usage":{"cache_write_tokens":7}, "cache_write_5m_tokens":3}),
                37,
                None,
            ),
        ] {
            attempt.envelope.usage_by_model = Some(serde_json::from_value(json!([
                {"model_used":"a", "usage":{"cache_write_tokens":30}, "cache_write_5m_tokens":10, "cache_write_1h_tokens":20}, second
            ])).unwrap());
            let event = event("runner_a", &attempt).unwrap();
            assert_eq!(event.cache_write_tokens, Some(total));
            assert_eq!(
                event.cache_write_5m_tokens.zip(event.cache_write_1h_tokens),
                expected
            );
            if expected.is_none() {
                let body = serde_json::to_value(event).unwrap();
                assert!(body.get("cache_write_5m_tokens").is_none());
                assert!(body.get("cache_write_1h_tokens").is_none());
            }
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
    fn recorded_content_intervals_and_decision_are_optional_and_checked() {
        let mut attempt = fixture(false);
        attempt.route_decision_id = Some("frozen-caller".into());
        attempt.sent_to_last_content_ms = Some(80);
        attempt.first_to_last_content_ms = Some(60);
        let value = serde_json::to_value(event("runner_a", &attempt).unwrap()).unwrap();
        assert_eq!(value["route_decision_id"], "frozen-caller");
        assert_eq!(value["sent_to_last_content_ms"], 80);
        assert_eq!(value["first_to_last_content_ms"], 60);
        let absent = serde_json::to_value(event("runner_a", &fixture(false)).unwrap()).unwrap();
        for key in [
            "route_decision_id",
            "sent_to_last_content_ms",
            "first_to_last_content_ms",
        ] {
            assert!(absent.get(key).is_none(), "{key}");
        }
        attempt.first_to_last_content_ms = Some(81);
        assert!(event("runner_a", &attempt).is_none());
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
        attempt.interrupted_at_ms = None;
        attempt.envelope.outcome = BillingAttemptOutcome::SealedInFlight;
        attempt.envelope.usage =
            Some(serde_json::from_value(json!({"input_tokens": 100})).unwrap());
        let event = super::event("runner_a", &attempt).unwrap();
        assert_eq!(event.terminal_status, "interrupted");
        assert_eq!(event.input_tokens, None);
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
    fn shipping_build_wires_the_bus_publisher() {
        let main = include_str!("../../broca-module-serve/src/main.rs");
        assert!(main.contains("StepEvents::start_on_bus"));
        assert!(main.contains(".with_step_events("));
        assert!(include_str!("opener.rs").contains("self.step_events.as_deref()"));
    }

    #[tokio::test]
    async fn publisher_uses_plain_live_subject_and_counts_failures_without_retry() {
        let (calls, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let events = StepEvents::start(
            "account_a",
            "runner_a",
            Arc::new(Publisher { calls, fail: true }),
        )
        .unwrap();
        events.tx.try_send(fixture(false)).unwrap();
        let (subject, body) = inbox.recv().await.unwrap();
        assert_eq!(subject, "ck.account_a.live.runner_a.step.finished.v1");
        cortexkit_role_llm_runner::step_events::decode_step_finished(&subject, &body).unwrap();
        assert_eq!(events.dropped(), 1);
        assert!(inbox.try_recv().is_err(), "failed hint was retried");
    }

    #[tokio::test]
    async fn unavailable_bus_drops_hints_without_publishing() {
        let (calls, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let events = StepEvents::start_on_bus(Arc::new(Publisher { calls, fail: false }));
        events.tx.try_send(fixture(false)).unwrap();
        broca_test_support::wait_until("the hint without a bus account is dropped", || async {
            if events.dropped() == 1 {
                Ok(())
            } else {
                Err(format!("dropped={}", events.dropped()))
            }
        })
        .await;
        assert!(inbox.try_recv().is_err());
    }

    /// A fork attempt's step event and an ordinary run's, as this build publishes them.
    fn fork_and_ordinary() -> (StepFinished, StepFinished) {
        let fork_session = broca_protocol::BindIdentity::new(
            "/synthetic/project",
            "runner",
            "head:fork:fork-72f6bdfa7eb7fb4cdf456d38c4d99623",
        );
        let usage = broca_protocol::Usage {
            input_tokens: Some(1200),
            cached_input_tokens: Some(1000),
            output_tokens: Some(40),
            ..Default::default()
        };
        let fork = fork_step_event(&ForkStep {
            session: &fork_session,
            incarnation: [7; 16],
            seq: 5,
            run_id: "fork:1c5eb44ba863f5575b157804a784d72aee9e330ad3e7d5d8806e99272eabd6a0",
            attempt: 1,
            provider: "anthropic",
            model: "claude-opus-4-8",
            usage,
            finish_reason: Some(broca_protocol::content::FinishReason::Stop),
            failed: false,
            started_at_ms: 1_790_000_000_000,
            finished_at_ms: 1_790_000_001_500,
        })
        .expect("a fork step event");
        let head = broca_protocol::BindIdentity::new("/synthetic/project", "runner", "head");
        let ordinary = Facts {
            position: StepRef::new("broca", broca_core::agent_session_ref(&head), "0707", 9),
            run_id: "run-head-1",
            step_id: 1,
            attempt: 0,
            status: "completed",
            harness: &head.harness,
            session: &head.session,
            provider: "anthropic",
            model: "claude-opus-4-8",
            started_at_ms: 1_789_999_990_000,
            finished_at_ms: 1_789_999_991_000,
            attempt_id: "attempt-head-1",
        }
        .build()
        .unwrap();
        let envelope = broca_wal::record::AttemptEnvelope {
            usage: Some(usage),
            finish_reason: Some(broca_protocol::content::FinishReason::Stop),
            ..Default::default()
        };
        (fork, finish(ordinary, &envelope, false))
    }

    #[test]
    fn a_fork_step_event_is_an_ordinary_event_of_the_fork_session_and_attempt_run() {
        let (fork, ordinary) = fork_and_ordinary();
        assert_eq!(
            fork.session,
            "head:fork:fork-72f6bdfa7eb7fb4cdf456d38c4d99623"
        );
        assert_eq!(
            fork.run_id,
            "fork:1c5eb44ba863f5575b157804a784d72aee9e330ad3e7d5d8806e99272eabd6a0"
        );
        assert_eq!(
            fork.attempt_id.as_deref(),
            Some("fork-1c5eb44ba863f5575b157804a784d72aee9e330ad3e7d5d8806e99272eabd6a0")
        );
        assert_eq!(fork.attempt.get(), 1);
        assert_eq!(fork.terminal_status, "completed");
        assert_eq!(
            (fork.input_tokens, fork.output_tokens),
            (Some(1200), Some(40))
        );
        fork.check().unwrap();
        // The bytes are the ones the event's readers accepted: the members an ordinary event
        // has, and nothing that names the fork beyond its session and run.
        let body = serde_json::to_vec(&fork).unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            r#"{"runner":"broca","session_ref":"90c94fcf081d2e49c645072abad2426e","log_incarnation":"07070707070707070707070707070707","seq":5,"run_id":"fork:1c5eb44ba863f5575b157804a784d72aee9e330ad3e7d5d8806e99272eabd6a0","step_id":"1","attempt":1,"terminal_status":"completed","attempt_id":"fork-1c5eb44ba863f5575b157804a784d72aee9e330ad3e7d5d8806e99272eabd6a0","harness":"runner","session":"head:fork:fork-72f6bdfa7eb7fb4cdf456d38c4d99623","provider":"anthropic","model":"claude-opus-4-8","input_tokens":1200,"cached_input_tokens":1000,"output_tokens":40,"finish_reason":"stop","started_at_ms":1790000000000,"finished_at_ms":1790000001500}"#
        );
        let decoded: StepFinished = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded, fork);
        let members = |event: &StepFinished| -> Vec<String> {
            serde_json::to_value(event)
                .unwrap()
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect()
        };
        assert_eq!(members(&fork), members(&ordinary));
    }
}
