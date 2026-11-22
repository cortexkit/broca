//! Keep-warm on a session kept in the session store: while a run whose keep-warm is `auto` waits
//! on tool calls, `broca_core::keep_warm_wait` re-sends the run's last request through this
//! driver to keep the provider's cached prefix alive. The session's cache policy schedules
//! warms; the executor prices them and checks the account's quota. A warm answered by the
//! provider is recorded and dates the cache.
//!
//! The request warmed is the bytes the run's transport last started sending. Before the first
//! send after a restart, the driver instead rebuilds the recorded step's request from the view
//! before its tool turn, waits for media readiness, and checks its recorded digest before
//! sending.
//!
//! The run's writer is busy with the tool wait, so a warm's record goes to the actor, which
//! appends it as its own record. It is queued before the driver awaits anything, so a warm cut
//! short by tool results still has its record written.

use std::sync::{Arc, Mutex};

use broca_core::keep_warm_wait::{KeepWarmDriver, WarmTick, WarmTiming};
use broca_core::{CacheClock, ShutdownSeal};
use broca_protocol::{BindIdentity, RunId};
use broca_records::record::RunConfig;
use broca_session_loop::run::Admission;
use broca_session_store::record::{Record, Warmed};
use tokio::sync::{mpsc, oneshot};

use crate::executor::{RunExecutor, WarmBase, WarmJob};
use crate::keep_warm::WarmSlot;
use crate::CacheDeclaration;

/// A warm's record on its way to the actor, and where the actor answers once it is durable.
pub(super) struct WarmRecord {
    pub(super) record: Record,
    pub(super) finished_at_ms: u64,
    pub(super) request_body_bytes: Option<u64>,
    pub(super) durable: oneshot::Sender<Result<(), String>>,
}

/// The run's config as the actor last read it from the session, with the refreezes it read it
/// under, so it is read again only when one changes.
type ConfigSlot = Arc<Mutex<Option<(RefreezeKey, RunConfig)>>>;
type RefreezeKey = (Option<serde_json::Value>, Option<serde_json::Value>);

pub(super) type RecordedBase = Result<
    (
        broca_core::RecordedRequest,
        Arc<crate::store_media::LazyRenderer>,
    ),
    String,
>;

/// The actor's end of a run's keep-warm.
pub(super) struct RunWarming {
    /// The records of the warms the run's driver sent.
    pub(super) records: mpsc::UnboundedReceiver<WarmRecord>,
    config: ConfigSlot,
}

impl RunWarming {
    /// Keeps the driver's copy of the run's config current: the config the run's start froze,
    /// with the refreezes recorded since applied.
    pub(super) fn refresh(
        &self,
        run: &broca_session_store::fold::operations::Run,
        parts: &dyn broca_session_loop::parts::Parts,
    ) {
        let Some(start) = run.start.as_ref() else {
            return;
        };
        let key = (run.refrozen_render.clone(), run.refrozen_auth.clone());
        let mut slot = self.config.lock().unwrap();
        if slot.as_ref().is_some_and(|(read, _)| *read == key) {
            return;
        }
        if let Ok(config) = super::warm::run_config(run, start, parts) {
            *slot = Some((key, config));
        }
    }
}

/// Reads a run's cache policy from its config, as the module's registry declares it.
type Declare = Arc<dyn Fn(&RunConfig) -> Option<CacheDeclaration> + Send + Sync>;

/// A run's keep-warm driver, called by the scheduler with the run's admission.
pub(super) struct StoreWarmer {
    session: BindIdentity,
    executor: Arc<dyn RunExecutor>,
    /// When the session's cache was last sent to, and what that request measured.
    clock: CacheClock,
    /// Where the run's model calls note each request their transport starts sending.
    seal: ShutdownSeal,
    /// Lets one warm at a time be sent, and keeps the latest rate-limit headers the run's
    /// attempts reported, which the quota check reads.
    slot: WarmSlot,
    declare: Declare,
    config: ConfigSlot,
    records: mpsc::UnboundedSender<WarmRecord>,
    recorded: Option<RecordedBase>,
    warm_stops: crate::step_events::WarmStops,
}

impl StoreWarmer {
    /// A driver for a run of the session `session`, and the actor's end of it. `last_send` is
    /// when the session's cache was last sent to, with what that request measured, as the
    /// session's records date it; the run's own sends move the clock on from it.
    pub(super) fn new(
        session: BindIdentity,
        executor: Arc<dyn RunExecutor>,
        declare: Declare,
        last_send: Option<(u64, Option<u64>, broca_protocol::Usage)>,
        recorded: Option<RecordedBase>,
        warm_stops: crate::step_events::WarmStops,
    ) -> (Arc<Self>, RunWarming) {
        let clock = CacheClock::default();
        if let Some((sent_at_ms, ttl_ms, usage)) = last_send {
            clock.note_sent(sent_at_ms, ttl_ms);
            clock.note_usage(sent_at_ms, usage);
        }
        let (records, receiver) = mpsc::unbounded_channel();
        let config = ConfigSlot::default();
        let warmer = Arc::new(Self {
            session,
            executor,
            seal: ShutdownSeal::with_cache_clock(clock.clone()),
            clock,
            slot: WarmSlot::default(),
            declare,
            config: Arc::clone(&config),
            records,
            recorded,
            warm_stops,
        });
        (
            warmer,
            RunWarming {
                records: receiver,
                config,
            },
        )
    }

    /// The run's config, once the actor read it.
    fn config(&self) -> Result<RunConfig, String> {
        self.config
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, config)| config.clone())
            .ok_or_else(|| "the run's config isn't read yet".into())
    }

    fn timing_of(&self, config: &RunConfig) -> Result<WarmTiming, String> {
        let declaration = (self.declare)(config).ok_or("cache declaration unavailable")?;
        declaration.warm?;
        let ttl_ms = declaration
            .ttl_ms
            .filter(|ttl| *ttl > 0)
            .ok_or("cache TTL unavailable")?;
        Ok(WarmTiming::for_ttl(
            ttl_ms,
            self.executor.warm_after_send_ms(config),
        ))
    }

    fn costs_of(&self, config: &RunConfig) -> Result<broca_core::keep_warm::WarmCosts, String> {
        let prefix = self
            .clock
            .read()
            .and_then(|reading| reading.prefix_tokens)
            .ok_or("prefix_tokens_unavailable")?;
        self.executor
            .warm_costs(config, prefix, self.timing_of(config)?.ttl_ms)
    }
}

/// Whether requests under `config` go to an OpenAI Responses endpoint, whose warms are priced
/// from the usage they report.
pub(super) fn responses(config: &RunConfig) -> bool {
    matches!(
        config
            .frozen_render_config
            .get("family_id")
            .and_then(serde_json::Value::as_str),
        Some("openai_responses" | "openai_responses_chatgpt")
    )
}

#[async_trait::async_trait]
impl KeepWarmDriver<Admission> for StoreWarmer {
    fn note_stop(&self, run_id: &RunId, reason: &str) {
        if matches!(reason, "tools_done" | "cancelled") {
            return;
        }
        self.warm_stops
            .lock()
            .unwrap()
            .entry(run_id.0.clone())
            .or_insert_with(|| reason.into());
    }
    fn timing(&self, _admission: &Admission) -> Result<WarmTiming, String> {
        self.timing_of(&self.config()?)
    }

    fn requested_ttl_ms(&self, _admission: &Admission) -> Option<u64> {
        (self.declare)(&self.config().ok()?)?.requested_ttl_ms
    }

    fn last_sent_at_ms(&self) -> Option<u64> {
        self.clock.read().map(|reading| reading.last_sent_at_ms)
    }

    fn note_attempt(&self, attempt: &broca_protocol::ModelAttempt) {
        self.slot.note_headers(
            attempt.sent_at_ms.unwrap_or_default(),
            attempt.account_observation.ratelimit_headers.clone(),
        );
    }

    fn step_seal(&self) -> Option<ShutdownSeal> {
        Some(self.seal.clone())
    }

    fn costs(&self, _admission: &Admission) -> Result<broca_core::keep_warm::WarmCosts, String> {
        self.costs_of(&self.config()?)
    }

    fn recorded_cost(
        &self,
        _admission: &Admission,
        usage: &broca_protocol::Usage,
    ) -> Result<f64, String> {
        let config = self.config()?;
        if responses(&config) {
            self.executor
                .responses_warm_cost(&config, *usage)
                .or_else(|_| self.costs_of(&config).map(|costs| costs.warm))
        } else {
            self.costs_of(&config).map(|costs| costs.warm)
        }
    }

    fn failed_cost(
        &self,
        _admission: &Admission,
        reservation: &broca_records::record::AttemptReservation,
        envelope: &broca_records::record::AttemptEnvelope,
        ttl_ms: Option<u64>,
    ) -> Result<f64, String> {
        self.executor
            .failed_warm_cost(&self.config()?, reservation, envelope, ttl_ms)
    }

    async fn warm(&self, _admission: &Admission, run_id: &RunId) -> WarmTick {
        self.warm_run(run_id).await
    }
}

impl StoreWarmer {
    /// One warm of run `run_id`'s cache.
    pub(super) async fn warm_run(&self, run_id: &RunId) -> WarmTick {
        let Some(_permit) = self.slot.acquire() else {
            return WarmTick::Skipped;
        };
        let stop = |reason: &str, detail: String| WarmTick::Stop {
            reason: reason.into(),
            detail: Some(detail),
        };
        let config = match self.config() {
            Ok(config) => config,
            Err(error) => return stop("unsupported", error),
        };
        let timing = match self.timing_of(&config) {
            Ok(timing) => timing,
            Err(error) => return stop("unsupported", error),
        };
        let headers = self.slot.latest_headers();
        if let Err(code) = self.executor.warm_quota(&config, &headers).await {
            return stop("quota", code);
        }
        // Check the margin after the quota query, which may outlast the cached prompt: a warm too
        // close to cache expiry would write the prefix again in full.
        let margin = 5_000.min(timing.ttl_ms / 10);
        if timing.after_send_ms.is_none()
            && self.clock.read().is_none_or(|reading| {
                reading.last_sent_at_ms.saturating_add(timing.ttl_ms)
                    <= crate::actor::epoch_ms().saturating_add(margin)
            })
        {
            return stop("unsupported", "cache_expired".into());
        }
        let (base, step_id, job_config) = match self.seal.last_sent() {
            Some(sent) => {
                let step = sent.step_id;
                (WarmBase::InFlight(sent), step, config.clone())
            }
            None => match self.recorded.as_ref() {
                Some(Ok((request, renderer))) => (
                    WarmBase::RecordedMedia {
                        request: Box::new(request.clone()),
                        renderer: Arc::clone(renderer),
                    },
                    request.step_id,
                    request.config.clone(),
                ),
                unavailable => {
                    let reason = unavailable
                        .and_then(|result| result.as_ref().err())
                        .map(String::as_str)
                        .unwrap_or("no recorded request is available");
                    tracing::warn!(target: "cache_warm", session = %self.session.session, run_id = %run_id.0, reason,
                        "the resumed run's keep-warm prefix is unavailable");
                    return stop("unsupported", "warm_prefix_unavailable".into());
                }
            },
        };
        let digest = base.request_body_sha256().to_owned();
        let job = WarmJob {
            session: self.session.clone(),
            config: job_config,
            base,
            attempt_hook: None,
        };
        let sent = match self.executor.warm(job).await {
            Ok(sent) => sent,
            Err(crate::executor::WarmError::Provider(error))
                if error.provider_code.as_deref()
                    == Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED) =>
            {
                return stop(
                    "quota",
                    broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into(),
                );
            }
            Err(error) => {
                tracing::warn!(target: "cache_warm", session = %self.session.session, run_id = %run_id.0, ?error,
                    "the keep-warm request was not sent");
                return stop("unsupported", format!("{error:?}"));
            }
        };
        let finished_at_ms = crate::actor::epoch_ms();
        self.slot.note_headers(
            sent.sent_at_ms,
            sent.ratelimit_headers.iter().cloned().collect(),
        );
        let record = match warmed(
            run_id,
            step_id.0,
            &digest,
            &sent,
            timing.ttl_ms,
            (self.declare)(&config).and_then(|declaration| declaration.requested_ttl_ms),
        ) {
            Ok(record) => record,
            Err(error) => return stop("unsupported", error),
        };
        // Queued before anything is awaited, so tool results that end the wait now still leave
        // the provider's answer recorded.
        let (durable, written) = oneshot::channel();
        if self
            .records
            .send(WarmRecord {
                record: Record::Warmed(record),
                finished_at_ms,
                request_body_bytes: sent.request_body_bytes,
                durable,
            })
            .is_err()
        {
            return stop("unsupported", "the session's actor is gone".into());
        }
        self.clock.note_sent(sent.sent_at_ms, Some(timing.ttl_ms));
        self.clock.note_usage(sent.sent_at_ms, sent.usage);
        match written.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return stop("unsupported", error),
            Err(_) => return stop("unsupported", "the session's actor is gone".into()),
        }
        if responses(&config) {
            let cost = self
                .executor
                .responses_warm_cost(&config, sent.usage)
                .unwrap_or_else(|_| {
                    self.costs_of(&config)
                        .map(|costs| costs.warm)
                        .unwrap_or(f64::INFINITY)
                });
            WarmTick::ResponsesSent {
                usage: sent.usage,
                cost,
            }
        } else {
            WarmTick::Sent
        }
    }
}

/// The record of a keep-warm of step `step_id` of run `run_id`, whose request had the digest
/// `request_body_sha256`: the provider answered the request, so it reached the cache.
pub(super) fn warmed(
    run_id: &RunId,
    step_id: u64,
    request_body_sha256: &str,
    sent: &broca_core::WarmSent,
    ttl_ms: u64,
    requested_cache_ttl_ms: Option<u64>,
) -> Result<Warmed, String> {
    let digest = |hex: &str| {
        super::warm::sha256_digest(hex).ok_or_else(|| format!("{hex} is not a SHA-256 digest"))
    };
    let headers: std::collections::BTreeMap<String, String> =
        sent.ratelimit_headers.iter().cloned().collect();
    Ok(Warmed {
        run_id: run_id.0.clone(),
        step_id,
        trigger: Some("keep_warm".into()),
        request_body_sha256: digest(request_body_sha256)?,
        warm_body_sha256: digest(&sent.warm_body_sha256)?,
        usage: sent.usage,
        served_account_ref: sent.served_account_ref.clone(),
        sent_at_ms: sent.sent_at_ms,
        ttl_ms: Some(ttl_ms),
        expires_at_ms: Some(sent.sent_at_ms.saturating_add(ttl_ms)),
        attempt_id: None,
        billing: None,
        ratelimit_headers: ((!headers.is_empty())
            .then(|| serde_json::to_value(&headers).expect("headers serialize")))
        .map(Into::into),
        reached_cache: Some(true),
        requested_cache_ttl_ms,
    })
}

impl<B: broca_session_store::backend::SessionBackend + Send + Sync + 'static> super::StoreActor<B> {
    /// The keep-warm of a run started in `mode`, when the run keeps its cache warm: a new run
    /// whose admission chose a warming mode (`fresh`), a run going on when its start froze one.
    /// Without the module's registry no cache policy can be read, so no run warms.
    pub(super) async fn keep_warm_for(
        &self,
        fresh: Option<&broca_protocol::request::KeepWarm>,
        mode: crate::executor::StoreRunMode,
        media: &mut Option<crate::store_media::StoreRunMediaWait>,
    ) -> Option<(Arc<StoreWarmer>, RunWarming)> {
        use crate::executor::StoreRunMode;
        use broca_protocol::request::KeepWarm;
        use broca_session_loop::writer::Writer as _;
        let operations = self.session.operations();
        let latest = operations.run.as_ref();
        let warms = match mode {
            StoreRunMode::Fresh => fresh.is_some_and(KeepWarm::warms),
            StoreRunMode::Resume | StoreRunMode::RecheckSignIn | StoreRunMode::ProbeSignIn => {
                latest
                    .and_then(|run| run.start.as_ref())
                    .and_then(|start| start.keep_warm.clone())
                    .and_then(|keep_warm| serde_json::from_value::<KeepWarm>(keep_warm).ok())
                    .is_some_and(|keep_warm| keep_warm.warms())
            }
            StoreRunMode::Seal(_) => false,
        };
        let registry = self.registry.clone().filter(|_| warms)?;
        // The session's cache clock, as its records date it: the last request that reached the
        // cache, with the lifetime the latest run's cache policy declares, and that run's last
        // measured request.
        let last_send = operations.last_cache_send.as_ref().map(|send| {
            let config = latest.and_then(|run| {
                super::warm::run_config(run, run.start.as_ref()?, &self.parts).ok()
            });
            let ttl_ms = config
                .and_then(|config| registry.cache_declaration(&config))
                .and_then(|declaration| declaration.ttl_ms);
            let usage = latest
                .filter(|run| run.run_id == send.run_id)
                .and_then(|run| run.last_step_usage)
                .unwrap_or_default();
            (send.sent_at_ms, ttl_ms, usage)
        });
        let recorded = if matches!(
            mode,
            StoreRunMode::Resume | StoreRunMode::RecheckSignIn | StoreRunMode::ProbeSignIn
        ) {
            Some(self.recorded_warm_request().await.and_then(|request| {
                super::recorded_warm::media(media).map(|renderer| (request, renderer))
            }))
        } else {
            None
        };
        let (driver, warming) = StoreWarmer::new(
            self.key.clone(),
            Arc::clone(&self.executor),
            Arc::new(move |config| registry.cache_declaration(config)),
            last_send,
            recorded,
            self.step_event_warm_stops.clone(),
        );
        Some((driver, warming))
    }

    /// Appends the record of a warm the active run's keep-warm sent, durable before the driver
    /// hears of it.
    pub(super) async fn record_kept_warm(&mut self, mut kept: WarmRecord) {
        let result = async {
            if let Record::Warmed(warmed) = &mut kept.record {
                let id = broca_core::attempt_id::mint_attempt_id()
                    .map_err(|e| format!("warm attempt identity: {e}"))?;
                warmed.attempt_id = Some(id.as_str().to_owned());
                let mut facts = serde_json::Map::new();
                facts.insert(
                    "finished_at_ms".into(),
                    serde_json::json!(kept.finished_at_ms),
                );
                if let Some(bytes) = kept.request_body_bytes {
                    facts.insert("request_body_bytes".into(), serde_json::json!(bytes));
                }
                warmed.billing = Some(serde_json::Value::Object(facts).into());
            }
            self.append_own(kept.record).await
        }
        .await;
        if let Err(error) = &result {
            // The provider answered (and billed) the warm; only its record failed.
            tracing::warn!(
                target: "cache_warm",
                session = %self.key.session,
                "a keep-warm on the new storage was sent but not recorded: {error}",
            );
        }
        let _ = kept.durable.send(result);
    }

    /// Hands the active run's keep-warm the run's config as the session now holds it.
    pub(super) fn refresh_warm_config(&self) {
        use broca_session_loop::writer::Writer as _;
        let Some(active) = self.run.as_ref() else {
            return;
        };
        let Some(warming) = active.warming.as_ref() else {
            return;
        };
        let operations = self.session.operations();
        if let Some(run) = operations
            .run
            .as_ref()
            .filter(|run| run.run_id == active.run_id.0)
        {
            warming.refresh(run, &self.parts);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::WarmError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// An executor that prices warms and counts the warms it is asked to send, answering each.
    #[derive(Default)]
    struct Counting {
        warms: AtomicUsize,
        sent: Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait::async_trait]
    impl RunExecutor for Counting {
        fn warm_costs(
            &self,
            _config: &RunConfig,
            _prefix_tokens: u64,
            _ttl_ms: u64,
        ) -> Result<broca_core::keep_warm::WarmCosts, String> {
            Ok(broca_core::keep_warm::WarmCosts {
                warm: 0.01,
                bust: 10.0,
            })
        }

        async fn warm(&self, job: WarmJob) -> Result<broca_core::WarmSent, WarmError> {
            self.warms.fetch_add(1, Ordering::SeqCst);
            let WarmBase::InFlight(sent) = job.base else {
                panic!("a store run's keep-warm re-sends only bytes its transport sent");
            };
            self.sent.lock().unwrap().push(sent.bytes().to_vec());
            Ok(broca_core::WarmSent {
                request_body_bytes: None,
                warm_body_sha256: sent.sha256.clone(),
                sent_at_ms: crate::actor::epoch_ms(),
                usage: broca_protocol::Usage {
                    cached_input_tokens: Some(100),
                    ..Default::default()
                },
                served_account_ref: None,
                ratelimit_headers: Vec::new(),
                http_status: 200,
            })
        }
    }

    fn config() -> RunConfig {
        RunConfig {
            account_route: None,
            global_system: None,
            route_decision_id: None,
            served_provider: None,
            account_limit_pause: false,
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: broca_protocol::request::ModelHandle::new("stub", "m"),
            tools: vec![].into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        }
    }

    /// A driver for a run whose session last sent to its cache a minute ago, under a one-hour
    /// policy that allows warms, with the run's config read.
    fn driver(executor: &Arc<Counting>) -> (Arc<StoreWarmer>, RunWarming) {
        let (warmer, warming) = StoreWarmer::new(
            BindIdentity::new("/project", "store-actor", "s1"),
            Arc::clone(executor) as Arc<dyn RunExecutor>,
            Arc::new(|_| {
                Some(CacheDeclaration {
                    ttl_ms: Some(3_600_000),
                    ttl_options_ms: Vec::new(),
                    read_refreshes_ttl: Some(true),
                    warm: Ok(()),
                    basis: None,
                    requested_ttl_ms: None,
                })
            }),
            Some((
                crate::actor::epoch_ms() - 60_000,
                Some(3_600_000),
                broca_protocol::Usage {
                    input_tokens: Some(100),
                    ..Default::default()
                },
            )),
            None,
            Default::default(),
        );
        *warming.config.lock().unwrap() = Some(((None, None), config()));
        (warmer, warming)
    }

    #[test]
    fn live_step_report_retains_the_first_warming_stop_not_tool_completion() {
        let executor = Arc::new(Counting::default());
        let (driver, _) = driver(&executor);
        let run = RunId("synthetic".into());
        driver.note_stop(&run, "tools_done");
        driver.note_stop(&run, "cancelled");
        assert!(driver.warm_stops.lock().unwrap().is_empty());
        driver.note_stop(&run, "budget");
        driver.note_stop(&run, "quota");
        assert_eq!(
            driver
                .warm_stops
                .lock()
                .unwrap()
                .get(&run.0)
                .map(String::as_str),
            Some("budget")
        );
    }

    /// With neither an in-process send nor a recorded request, a warm stops before rendering or
    /// sending anything.
    #[tokio::test]
    async fn a_run_that_sent_nothing_yet_warms_nothing() {
        let executor = Arc::new(Counting::default());
        let (warmer, mut warming) = driver(&executor);
        let tick = warmer.warm_run(&RunId("r1".into())).await;
        let WarmTick::Stop { reason, detail } = tick else {
            panic!("the warm stops");
        };
        assert_eq!(reason, "unsupported");
        assert_eq!(detail.as_deref(), Some("warm_prefix_unavailable"));
        assert_eq!(executor.warms.load(Ordering::SeqCst), 0, "nothing rendered");
        assert!(warming.records.try_recv().is_err(), "nothing recorded");
    }

    /// Once the run's transport notes a request on the driver's seal, a warm re-sends exactly
    /// those bytes and queues its record for the actor, dated from the warm's own send.
    #[tokio::test]
    async fn a_warm_resends_the_bytes_the_run_sent_and_queues_its_record() {
        let executor = Arc::new(Counting::default());
        let (warmer, mut warming) = driver(&executor);
        let body = b"the step's request".to_vec();
        let sha256 = broca_core::render::request_digest_hex(&body);
        warmer.step_seal().unwrap().note_sent(
            broca_core::SentRequest {
                step_id: broca_protocol::StepId(2),
                body: Arc::new(body.clone()),
                sha256: sha256.clone(),
                sent_at_ms: crate::actor::epoch_ms(),
                route: broca_core::refusal_fallback::StepRoute::Source,
            },
            None,
        );
        let run_id = RunId("r1".into());
        let warm = warmer.warm_run(&run_id);
        tokio::pin!(warm);
        // The record is queued before the driver waits for it to be durable.
        let kept = tokio::select! {
            tick = &mut warm => panic!("the warm waits for its record: {}", matches!(tick, WarmTick::Sent)),
            kept = warming.records.recv() => kept.unwrap(),
        };
        let Record::Warmed(record) = &kept.record else {
            panic!("a warm's record");
        };
        assert_eq!(record.trigger.as_deref(), Some("keep_warm"));
        assert_eq!(record.run_id, "r1");
        assert_eq!(record.step_id, 2);
        assert_eq!(
            Some(record.request_body_sha256),
            super::super::warm::sha256_digest(&sha256)
        );
        assert_eq!(record.ttl_ms, Some(3_600_000));
        assert_eq!(
            record.expires_at_ms,
            Some(record.sent_at_ms + 3_600_000),
            "dated from the warm's own send"
        );
        assert_eq!(record.reached_cache, Some(true));
        kept.durable.send(Ok(())).unwrap();
        assert!(matches!(warm.await, WarmTick::Sent));
        assert_eq!(*executor.sent.lock().unwrap(), [body]);
        assert_eq!(
            warmer.last_sent_at_ms(),
            Some(record.sent_at_ms),
            "the warm moved the cache clock"
        );
    }
}
