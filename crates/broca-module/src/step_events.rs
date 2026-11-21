//! Best-effort, content-free `step.finished.v1` hints, separate from durable billing.
//!
//! Each session's append observer (`store`) builds a step's hint from the session's own
//! records and hands it over once those records are durable. A hint is never retried or
//! queued on disk: a crash or a full queue loses the hint, never the billing evidence.

use async_trait::async_trait;
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
    tx: tokio::sync::mpsc::Sender<StepFinished>,
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<StepFinished>(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let mut last_warning = None;
            while let Some(event) = rx.recv().await {
                let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    let body =
                        serde_json::to_vec(&event).map_err(|_| "invalid step event evidence")?;
                    let subject = match &subject {
                        Some(subject) => subject.clone(),
                        None => publisher.live_subject(&runner).await?,
                    };
                    publisher.publish(&subject, body).await
                })
                .await;
                if !matches!(result, Ok(Ok(()))) {
                    // No retry: the session's records are the authority, and a hint must not
                    // build a backlog.
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
        Self { tx, dropped, task }
    }

    /// Publishes the step event of a fork attempt's model call, best effort as every step event
    /// is. It is an ordinary event of the fork's own session and the attempt's own run, which
    /// keep it apart from the head's steps.
    pub(crate) fn publish_fork(&self, event: StepFinished) {
        if event.check().is_err() || self.tx.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
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
            self.tx.clone(),
            self.dropped.clone(),
            parts,
            warm_stops,
        )
    }

    /// Queue overflows, disconnected receivers, invalid evidence and failed publishes.
    /// Health reports this counter.
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

fn cache_report(
    frozen: &serde_json::Value,
    provider: &str,
    keep_warm: Option<&broca_protocol::request::KeepWarm>,
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
        Some(
            broca_protocol::request::KeepWarm::Auto
            | broca_protocol::request::KeepWarm::Always
            | broca_protocol::request::KeepWarm::Hours(_),
        ) => match stop {
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
    let measured = Measured {
        usage: Some(step.usage),
        finish_reason: step.finish_reason,
        // Usage is canonical: recorded reasoning is a subset of output, not extra output.
        reasoning_included_in_output: step.usage.reasoning_tokens.map(|_| true),
        ..Default::default()
    };
    let event = finish(event, &measured);
    event.check().ok()?;
    Some(event)
}

/// What a step measured, as [`finish`] reports it. Absent values stay absent.
#[derive(Default)]
struct Measured {
    usage: Option<broca_protocol::Usage>,
    finish_reason: Option<broca_protocol::FinishReason>,
    reasoning_included_in_output: Option<bool>,
    cache_write_5m_tokens: Option<u64>,
    cache_write_1h_tokens: Option<u64>,
}

/// Puts a finished step's usage on its event. Usage the step did not measure stays absent.
fn finish(mut event: StepFinished, measured: &Measured) -> StepFinished {
    let usage = measured.usage.unwrap_or_default();
    let (mut split_5m, mut split_1h) = (
        measured.cache_write_5m_tokens,
        measured.cache_write_1h_tokens,
    );
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
    event.reasoning_included_in_output = measured.reasoning_included_in_output;
    event.finish_reason = measured.finish_reason.and_then(|reason| {
        serde_json::to_value(reason)
            .ok()?
            .as_str()
            .map(str::to_owned)
    });
    event
}

mod store;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

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
            let report = cache_report(&implicit, "openai", Some(&Auto), Some(stop)).unwrap();
            assert_eq!(report.warming, Some(Warming::stopped(reason)));
            report.check().unwrap();
        }
        for stop in [None, Some("tools_done"), Some("cancelled")] {
            assert_eq!(
                cache_report(&implicit, "openai", Some(&Auto), stop)
                    .unwrap()
                    .warming,
                Some(Warming::new(true))
            );
        }
        assert_eq!(
            cache_report(&implicit, "openai", Some(&Off), Some("budget"))
                .unwrap()
                .warming,
            Some(Warming::new(false))
        );
        let unknown = cache_report(&implicit, "openai", Some(&Auto), Some("future-stop")).unwrap();
        assert_eq!(unknown.implicit, Some(true));
        assert_eq!(unknown.warming, None);
        // No lifetime means no cache object, even when warming is known.
        for keep_warm in [None, Some(&Auto), Some(&Off)] {
            assert_eq!(
                cache_report(&Value::Null, "synthetic", keep_warm, Some("budget")),
                None
            );
        }
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

    /// A valid step hint, as a session's observer hands it to the publisher.
    fn hint() -> StepFinished {
        Facts {
            position: StepRef::new("broca", "runner_session_a", "log_a", 43),
            run_id: "run_a",
            step_id: 7,
            attempt: 0,
            status: "completed",
            harness: "host_a",
            session: "session_a",
            provider: "provider_a",
            model: "model_a",
            started_at_ms: 1_790_000_001_001,
            finished_at_ms: 1_790_000_002_000,
            attempt_id: "0123456789abcdef0123456789abcdef",
        }
        .build()
        .unwrap()
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
            "broca",
            Arc::new(Publisher { calls, fail: true }),
        )
        .unwrap();
        events.tx.try_send(hint()).unwrap();
        let (subject, body) = inbox.recv().await.unwrap();
        assert_eq!(subject, "ck.account_a.live.broca.step.finished.v1");
        cortexkit_role_llm_runner::step_events::decode_step_finished(&subject, &body).unwrap();
        assert_eq!(events.dropped(), 1);
        assert!(inbox.try_recv().is_err(), "failed hint was retried");
    }

    #[tokio::test]
    async fn unavailable_bus_drops_hints_without_publishing() {
        let (calls, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let events = StepEvents::start_on_bus(Arc::new(Publisher { calls, fail: false }));
        events.tx.try_send(hint()).unwrap();
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
        let measured = Measured {
            usage: Some(usage),
            finish_reason: Some(broca_protocol::content::FinishReason::Stop),
            ..Default::default()
        };
        (fork, finish(ordinary, &measured))
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
