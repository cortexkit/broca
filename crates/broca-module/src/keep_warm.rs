//! Connect scheduled prompt-cache refreshes to the session executor. Manual and automatic
//! refreshes acquire the same permit, so their provider requests cannot overlap.

use crate::executor::{WarmBase, WarmJob};
use crate::{CacheDeclaration, RunExecutor};
use broca_core::keep_warm_wait::{KeepWarmDriver, WarmTick, WarmTiming};
use broca_core::{CacheClock, RecordedRequest, ShutdownSeal, WalHandle};
use broca_protocol::{BindIdentity, RunId};
use broca_wal::{RunConfig, WalRecord};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

type ObservedHeaders = (u64, BTreeMap<String, String>);

#[derive(Clone, Default)]
pub(crate) struct WarmSlot {
    occupied: Arc<AtomicBool>,
    headers: Arc<Mutex<Option<ObservedHeaders>>>,
}
pub(crate) struct WarmPermit(WarmSlot);
impl Drop for WarmPermit {
    fn drop(&mut self) {
        self.0.occupied.store(false, Ordering::SeqCst);
    }
}
impl WarmSlot {
    pub(crate) fn occupied(&self) -> bool {
        self.occupied.load(Ordering::SeqCst)
    }
    /// A slow response from an older send must not overwrite newer account usage evidence.
    pub(crate) fn note_headers(&self, sent_at_ms: u64, values: BTreeMap<String, String>) {
        let mut headers = self.headers.lock().unwrap();
        if headers
            .as_ref()
            .is_none_or(|(current, _)| *current <= sent_at_ms)
        {
            *headers = Some((sent_at_ms, values));
        }
    }
    fn latest_headers(&self) -> BTreeMap<String, String> {
        self.headers
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, values)| values.clone())
            .unwrap_or_default()
    }
    pub(crate) fn acquire(&self) -> Option<WarmPermit> {
        self.occupied
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| WarmPermit(self.clone()))
    }
}

pub(crate) type Declare = Arc<dyn Fn(&RunConfig) -> Option<CacheDeclaration> + Send + Sync>;

pub(crate) fn manual_warm_hook(
    writer: &Arc<WalHandle>,
    active_seal: Option<&ShutdownSeal>,
    job: &WarmJob,
    run_id: &RunId,
) -> Option<Arc<broca_core::cache_warm::WarmJournal>> {
    if !writer.is_overhaul() {
        return None;
    }
    warm_journal(
        writer.clone(),
        active_seal.cloned().unwrap_or_default(),
        job,
        run_id,
        None,
    )
}

fn automatic_warm_hook(
    writer: &Arc<WalHandle>,
    seal: &ShutdownSeal,
    job: &WarmJob,
    run_id: &RunId,
) -> Option<Arc<broca_core::cache_warm::WarmJournal>> {
    if !writer.is_overhaul() {
        return None;
    }
    warm_journal(
        writer.clone(),
        seal.clone(),
        job,
        run_id,
        Some("keep_warm".into()),
    )
}

/// Invoke the actual setup guards under an allocator-owning test executable.
#[cfg(feature = "test-clock")]
pub fn warm_setup_for_allocation_test(
    writer: &Arc<WalHandle>,
    seal: &ShutdownSeal,
    job: &WarmJob,
    run_id: &RunId,
    automatic: bool,
) -> bool {
    if automatic {
        automatic_warm_hook(writer, seal, job, run_id).is_some()
    } else {
        manual_warm_hook(writer, None, job, run_id).is_some()
    }
}

/// Construct the real automatic driver for focused pricing and replay tests.
#[cfg(feature = "test-clock")]
pub fn automatic_warmer_for_test(
    writer: Arc<WalHandle>,
    executor: Arc<dyn RunExecutor>,
    session: BindIdentity,
    config: RunConfig,
    ttl_ms: u64,
) -> Arc<dyn KeepWarmDriver> {
    let clock = CacheClock::default();
    let now = epoch_ms();
    clock.note_sent(now, Some(ttl_ms));
    clock.note_usage(
        now,
        broca_protocol::Usage {
            cached_input_tokens: Some(1000),
            ..Default::default()
        },
    );
    Arc::new(AutomaticWarmer {
        retention: false,
        session,
        writer,
        executor,
        clock,
        seal: ShutdownSeal::new(),
        slot: WarmSlot::default(),
        declare: Arc::new(move |_| {
            Some(CacheDeclaration {
                ttl_ms: Some(ttl_ms),
                ttl_options_ms: vec![],
                read_refreshes_ttl: Some(true),
                warm: Ok(()),
                basis: None,
            })
        }),
        recorded: Mutex::new(Some(RecordedRequest {
            run_id: RunId("r".into()),
            step_id: broca_protocol::StepId(1),
            config,
            opts: broca_protocol::CallOptions::new(vec![]),
            request_body_sha256: "base".into(),
            route: None,
        })),
    })
}

pub(crate) fn warm_journal(
    writer: Arc<WalHandle>,
    seal: ShutdownSeal,
    job: &WarmJob,
    run_id: &RunId,
    trigger: Option<String>,
) -> Option<Arc<broca_core::cache_warm::WarmJournal>> {
    if !writer.is_overhaul() {
        return None;
    }
    use broca_wal::record::{AttemptCredential, AttemptReservation, AttemptRun, AttemptSession};
    let step_id = match &job.base {
        WarmBase::InFlight(sent) => sent.step_id,
        WarmBase::Recorded(recorded) => recorded.step_id,
    };
    let auth = job.config.auth_selection.clone();
    let template = AttemptReservation {
        run_id: run_id.clone(),
        step_id,
        attempt: 0,
        send_index: 1,
        warm: true,
        reserved_at_ms: 0,
        provider: job.config.model.provider_module_id.clone(),
        endpoint_host: String::new(),
        model_sent: String::new(),
        credential: AttemptCredential {
            credential_ref: None,
            account_ref: None,
            auth_method: auth.as_ref().and_then(|a| a.auth_method.clone()),
            auth_selection: auth,
            provider_ids: None,
            plan_tier: None,
            plan_tier_source: None,
        },
        session: AttemptSession {
            bind: job.session.clone(),
            session_ref: broca_core::agent_session_ref(&job.session),
        },
        run: AttemptRun {
            work_class: job.config.work_class.clone(),
            context_limit: job.config.context_limit,
            service_tier_requested: job.config.service_tier.map(|t| t.as_str().to_owned()),
            ..Default::default()
        },
        request_body_bytes: 0,
        compressed: false,
        input_tokens_estimate: None,
    };
    Some(Arc::new(broca_core::cache_warm::WarmJournal::new(
        writer,
        seal,
        template,
        job.base.request_body_sha256().to_owned(),
        trigger,
    )))
}
pub(crate) struct AutomaticWarmer {
    pub(crate) retention: bool,
    pub(crate) session: BindIdentity,
    pub(crate) writer: Arc<WalHandle>,
    pub(crate) executor: Arc<dyn RunExecutor>,
    pub(crate) clock: CacheClock,
    pub(crate) seal: ShutdownSeal,
    pub(crate) slot: WarmSlot,
    pub(crate) declare: Declare,
    pub(crate) recorded: Mutex<Option<RecordedRequest>>,
}

#[async_trait::async_trait]
impl KeepWarmDriver for AutomaticWarmer {
    fn timing(&self, config: &RunConfig) -> Result<WarmTiming, String> {
        let declaration = (self.declare)(config).ok_or("cache declaration unavailable")?;
        declaration.warm?;
        let ttl_ms = declaration
            .ttl_ms
            .filter(|ttl| *ttl > 0)
            .ok_or("cache TTL unavailable")?;
        Ok(WarmTiming {
            ttl_ms,
            lead_ms: 30_000.min(ttl_ms / 2),
            after_send_ms: self.executor.warm_after_send_ms(config),
        })
    }
    fn last_sent_at_ms(&self) -> Option<u64> {
        self.clock.read().map(|r| r.last_sent_at_ms)
    }
    fn note_attempt(&self, attempt: &broca_protocol::ModelAttempt) {
        self.slot.note_headers(
            attempt.sent_at_ms.unwrap_or_default(),
            attempt.account_observation.ratelimit_headers.clone(),
        );
    }
    fn costs(&self, config: &RunConfig) -> Result<broca_core::keep_warm::WarmCosts, String> {
        let prefix = self
            .clock
            .read()
            .and_then(|r| r.prefix_tokens)
            .ok_or("prefix_tokens_unavailable")?;
        self.executor
            .warm_costs(config, prefix, self.timing(config)?.ttl_ms)
    }
    fn recorded_cost(
        &self,
        config: &RunConfig,
        usage: &broca_protocol::Usage,
    ) -> Result<f64, String> {
        if matches!(
            config
                .frozen_render_config
                .get("family_id")
                .and_then(serde_json::Value::as_str),
            Some("openai_responses" | "openai_responses_chatgpt")
        ) {
            self.executor
                .responses_warm_cost(config, *usage)
                .or_else(|_| self.costs(config).map(|costs| costs.warm))
        } else {
            self.costs(config).map(|costs| costs.warm)
        }
    }
    fn failed_cost(
        &self,
        config: &RunConfig,
        reservation: &broca_wal::record::AttemptReservation,
        envelope: &broca_wal::record::AttemptEnvelope,
        ttl_ms: Option<u64>,
    ) -> Result<f64, String> {
        let result = self
            .executor
            .failed_warm_cost(config, reservation, envelope, ttl_ms);
        if let Err(detail) = &result {
            tracing::warn!(target: "cache_warm", stop = "pricing_unavailable", detail = %detail,
                provider = %reservation.provider, model = %reservation.model_sent,
                "keep_warm budget exhausted by unavailable failed-attempt pricing");
        }
        result
    }
    async fn warm(&self, config: &RunConfig, run_id: &RunId) -> WarmTick {
        if let Err(error) = self.writer.check_move() {
            let (reason, detail) = match error.into_move_refusal() {
                Ok(refusal @ broca_wal::migration::MoveRefusal::Moving { .. }) => {
                    ("session_moving", refusal.to_string())
                }
                Ok(refusal @ broca_wal::migration::MoveRefusal::Moved { .. }) => {
                    ("session_moved", refusal.to_string())
                }
                Err(error) => ("unavailable", error.to_string()),
            };
            return WarmTick::Stop {
                reason: reason.into(),
                detail: Some(detail),
            };
        }
        let Some(_permit) = self.slot.acquire() else {
            return WarmTick::Skipped;
        };
        let stop = |reason: &str, detail: String| WarmTick::Stop {
            reason: reason.into(),
            detail: Some(if self.retention {
                reason.to_owned()
            } else {
                detail
            }),
        };
        let timing = match self.timing(config) {
            Ok(timing) => timing,
            Err(error) => return stop("unsupported", error),
        };
        let headers = self.slot.latest_headers();
        if let Err(code) = self.executor.warm_quota(config, &headers).await {
            return stop("quota", code);
        }
        // Recheck expiry after querying quota: that query may outlast the cached prompt.
        // Reserve five seconds for delivery before expiry, after which Anthropic bills a new
        // cache write. Cap the margin at 10% of the TTL so very short local-test lifetimes
        // still leave time to warm.
        let margin = 5_000.min(timing.ttl_ms / 10);
        if timing.after_send_ms.is_none()
            && self.clock.read().is_none_or(|r| {
                r.last_sent_at_ms.saturating_add(timing.ttl_ms) <= epoch_ms().saturating_add(margin)
            })
        {
            return stop("unsupported", "cache_expired".into());
        }
        let base = match self.seal.last_sent() {
            Some(sent) => WarmBase::InFlight(sent),
            None => match self.recorded.lock().unwrap().clone() {
                Some(recorded) => WarmBase::Recorded(Box::new(recorded)),
                None => return stop("unsupported", "warm_prefix_unavailable".into()),
            },
        };
        let step_id = match &base {
            WarmBase::InFlight(sent) => sent.step_id,
            WarmBase::Recorded(recorded) => recorded.step_id,
        };
        let digest = base.request_body_sha256().to_owned();
        let mut job = WarmJob {
            session: self.session.clone(),
            config: config.clone(),
            base,
            attempt_hook: None,
        };
        let journal = automatic_warm_hook(&self.writer, &self.seal, &job, run_id);
        job.attempt_hook = journal
            .as_ref()
            .map(|journal| journal.clone() as Arc<dyn broca_core::run::AttemptHook>);
        let success_estimate = journal
            .as_ref()
            .map(|_| self.costs(config).map(|c| c.warm).unwrap_or(f64::INFINITY));
        let failed_cost = || {
            journal
                .as_ref()
                .map(|j| {
                    j.failed_attempts()
                        .iter()
                        .map(|attempt| {
                            self.failed_cost(
                                config,
                                &attempt.reservation,
                                &attempt.envelope,
                                attempt.ttl_ms,
                            )
                            .unwrap_or(f64::INFINITY)
                        })
                        .sum()
                })
                .unwrap_or(0.0)
        };
        let failed_tick = |reason: String, detail: Option<String>| {
            if journal.is_some() {
                WarmTick::StopWithCost {
                    reason,
                    detail,
                    cost: failed_cost(),
                }
            } else {
                WarmTick::Stop { reason, detail }
            }
        };
        let sent = match self.executor.warm(job).await {
            Ok(sent) => sent,
            Err(crate::executor::WarmError::Provider(error))
                if error.provider_code.as_deref()
                    == Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED) =>
            {
                return failed_tick(
                    "quota".into(),
                    Some(if self.retention {
                        "Provider".into()
                    } else {
                        broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()
                    }),
                );
            }
            Err(error) => {
                let detail = if self.retention {
                    error.variant_name().to_owned()
                } else {
                    format!("{error:?}")
                };
                return failed_tick("unsupported".into(), Some(detail));
            }
        };
        // Queue the provider's returned usage in CacheWarmed before awaiting disk sync.
        // Tool results can drop this future during sync; the run's next WAL barrier still
        // commits the queued record, so returned billable usage is not discarded.
        self.slot.note_headers(
            sent.sent_at_ms,
            sent.ratelimit_headers.iter().cloned().collect(),
        );
        if !self.writer.is_overhaul() {
            self.writer.record(WalRecord::CacheWarmed {
                attempt_id: None,
                reservation: None,
                envelope: None,
                trigger: Some("keep_warm".into()),
                run_id: run_id.clone(),
                step_id,
                request_body_sha256: digest,
                warm_body_sha256: sent.warm_body_sha256,
                usage: sent.usage,
                served_account_ref: sent.served_account_ref,
                ratelimit_headers: sent.ratelimit_headers.into_iter().collect(),
                sent_at_ms: sent.sent_at_ms,
                ttl_ms: Some(timing.ttl_ms),
                expires_at_ms: Some(sent.sent_at_ms.saturating_add(timing.ttl_ms)),
            });
        }
        self.clock.note_sent(sent.sent_at_ms, Some(timing.ttl_ms));
        self.clock.note_usage(sent.sent_at_ms, sent.usage);
        if let Err(error) = self.writer.sync().await {
            return stop("unsupported", error.to_string());
        }
        let responses = matches!(
            config
                .frozen_render_config
                .get("family_id")
                .and_then(serde_json::Value::as_str),
            Some("openai_responses" | "openai_responses_chatgpt")
        );
        if responses {
            let cost = self
                .executor
                .responses_warm_cost(config, sent.usage)
                .unwrap_or_else(|_| {
                    self.costs(config)
                        .map(|costs| costs.warm)
                        .unwrap_or(f64::INFINITY)
                });
            WarmTick::ResponsesSent {
                usage: sent.usage,
                cost: cost + failed_cost(),
            }
        } else {
            match success_estimate {
                Some(cost) => WarmTick::SentWithCost {
                    cost: cost + failed_cost(),
                },
                None => WarmTick::Sent,
            }
        }
    }
}
fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use broca_core::WalStore;

    struct BudgetWarm {
        attempts: Mutex<Vec<broca_wal::record::AttemptEnvelope>>,
    }
    #[async_trait::async_trait]
    impl RunExecutor for BudgetWarm {
        async fn execute(
            &self,
            _: crate::executor::RunJob,
            _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
        ) -> Result<broca_core::RunSummary, broca_core::RunError> {
            unreachable!()
        }
        fn warm_costs(
            &self,
            _: &RunConfig,
            _: u64,
            _: u64,
        ) -> Result<broca_core::keep_warm::WarmCosts, String> {
            Ok(broca_core::keep_warm::WarmCosts {
                warm: 10.0,
                bust: 100.0,
            })
        }
        fn failed_warm_cost(
            &self,
            _: &RunConfig,
            _: &broca_wal::record::AttemptReservation,
            e: &broca_wal::record::AttemptEnvelope,
            _: Option<u64>,
        ) -> Result<f64, String> {
            Ok(e.usage.and_then(|u| u.input_tokens).unwrap_or(0) as f64 * 2.0)
        }
        async fn warm(
            &self,
            job: WarmJob,
        ) -> Result<broca_core::WarmSent, crate::executor::WarmError> {
            let hook = job
                .attempt_hook
                .expect("overhaul warm must install the journal");
            let attempts = std::mem::take(&mut *self.attempts.lock().unwrap());
            let mut answer = None;
            for (index, e) in attempts.into_iter().enumerate() {
                let id = hook
                    .before_send(
                        broca_core::run::PhysicalSend {
                            attempt: index as u32,
                            send_index: index as u32 + 1,
                            endpoint_host: "example.com".into(),
                            model_sent: "m".into(),
                            request_body_bytes: 100,
                            compressed: index == 0,
                            account_ref: None,
                            speed_requested: None,
                            warm: None,
                        },
                        Default::default(),
                    )
                    .await
                    .map_err(|e| crate::executor::WarmError::Provider(Box::new(e)))?;
                let sent =
                    (e.outcome == broca_wal::record::BillingAttemptOutcome::Complete).then(|| {
                        broca_core::WarmSent {
                            warm_body_sha256: "warm".into(),
                            sent_at_ms: epoch_ms(),
                            usage: e.usage.unwrap_or_default(),
                            served_account_ref: None,
                            ratelimit_headers: vec![],
                            http_status: 200,
                        }
                    });
                hook.warm_finished(id, e, sent.clone(), "warm".into())
                    .await
                    .map_err(|e| crate::executor::WarmError::Provider(Box::new(e)))?;
                answer = sent;
            }
            answer.ok_or_else(|| {
                crate::executor::WarmError::Provider(Box::new(
                    serde_json::from_value(
                        serde_json::json!({"class":"transient", "message":"warm failed"}),
                    )
                    .unwrap(),
                ))
            })
        }
    }

    async fn restart_budget_case(
        outcome: broca_wal::record::BillingAttemptOutcome,
        usage: Option<u64>,
        resend: bool,
        expected: f64,
    ) {
        use broca_wal::record::{AttemptEnvelope, AttemptSent, BillingAttemptOutcome as Outcome};
        let root = broca_test_support::scratch_root().join(format!(
            "warm-budget-{}",
            broca_core::attempt_id::mint_attempt_id().unwrap().as_str()
        ));
        let store = Arc::new(broca_core::FileWalStore::with_config(
            root.join("wal"),
            broca_wal::WalConfig::OVERHAUL,
        ));
        let key = BindIdentity::new("/p", "runner", "budget");
        let tenure = broca_core::lease::WalLeaseDomain::new(root.join("leases"))
            .acquire(&key)
            .unwrap();
        let writer = Arc::new(
            WalHandle::open_and_spawn_owned(store.clone(), key.clone(), tenure, None)
                .await
                .unwrap(),
        );
        let mut attempts = vec![AttemptEnvelope {
            sent: if outcome == Outcome::ConnectFailed {
                AttemptSent::No
            } else {
                AttemptSent::Yes
            },
            http_status: if outcome == Outcome::ConnectFailed {
                None
            } else {
                Some(415)
            },
            outcome,
            usage: usage.map(|n| broca_protocol::Usage {
                input_tokens: Some(n),
                ..Default::default()
            }),
            ..Default::default()
        }];
        if resend {
            attempts.push(AttemptEnvelope {
                sent: AttemptSent::Yes,
                http_status: Some(200),
                outcome: Outcome::Complete,
                usage: Some(broca_protocol::Usage {
                    cached_input_tokens: Some(1000),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        let WalRecord::RunStarted { config, .. } = crate::move_install::tests::started(&key, "r")
        else {
            unreachable!()
        };
        let clock = CacheClock::default();
        let now = epoch_ms();
        clock.note_sent(now, Some(600_000));
        clock.note_usage(
            now,
            broca_protocol::Usage {
                cached_input_tokens: Some(1000),
                ..Default::default()
            },
        );
        let warmer = AutomaticWarmer {
            retention: false,
            session: key.clone(),
            writer: writer.clone(),
            executor: Arc::new(BudgetWarm {
                attempts: Mutex::new(attempts),
            }),
            clock,
            seal: ShutdownSeal::new(),
            slot: WarmSlot::default(),
            declare: Arc::new(|_| {
                Some(crate::cache::CacheDeclaration {
                    ttl_ms: Some(600_000),
                    ttl_options_ms: vec![],
                    read_refreshes_ttl: Some(true),
                    warm: Ok(()),
                    basis: None,
                })
            }),
            recorded: Mutex::new(Some(RecordedRequest {
                run_id: RunId("r".into()),
                step_id: broca_protocol::StepId(1),
                config: *config.clone(),
                opts: broca_protocol::CallOptions::new(vec![]),
                request_body_sha256: "base".into(),
                route: None,
            })),
        };
        let run_id = RunId("r".into());
        let tick = warmer.warm(&config, &run_id).await;
        let live = match tick {
            WarmTick::SentWithCost { cost } | WarmTick::StopWithCost { cost, .. } => cost,
            _ => panic!("live warm must expose its budget charge"),
        };
        assert_eq!(live, expected);
        let driver: Arc<dyn KeepWarmDriver> = Arc::new(warmer);
        let unsupported = AtomicBool::new(false);
        let missed = AtomicBool::new(false);
        let wait = broca_core::keep_warm_wait::WarmWait {
            driver: &driver,
            config: &config,
            run_id: &run_id,
            cancel: None,
            unsupported_logged: &unsupported,
            warm_missed: &missed,
        };
        let frames = store.read_records(&key).unwrap();
        assert_eq!(
            broca_core::keep_warm_wait::batch_spend(
                Some(&wait),
                broca_protocol::StepId(1),
                &frames
            ),
            expected
        );
        drop(driver);
        Arc::try_unwrap(writer)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn restart_after_connect_failed_matches_live_warm_spend() {
        restart_budget_case(
            broca_wal::record::BillingAttemptOutcome::ConnectFailed,
            None,
            false,
            0.0,
        )
        .await;
    }
    #[tokio::test]
    async fn restart_after_http_refusal_matches_live_warm_spend() {
        restart_budget_case(
            broca_wal::record::BillingAttemptOutcome::HttpError,
            None,
            false,
            0.0,
        )
        .await;
        restart_budget_case(
            broca_wal::record::BillingAttemptOutcome::HttpError,
            Some(0),
            false,
            0.0,
        )
        .await;
        restart_budget_case(
            broca_wal::record::BillingAttemptOutcome::HttpError,
            Some(3),
            false,
            6.0,
        )
        .await;
    }
    #[tokio::test]
    async fn restart_after_compression_refusal_matches_live_warm_spend() {
        restart_budget_case(
            broca_wal::record::BillingAttemptOutcome::CompressionRefused,
            None,
            true,
            10.0,
        )
        .await;
        restart_budget_case(
            broca_wal::record::BillingAttemptOutcome::CompressionRefused,
            Some(2),
            true,
            14.0,
        )
        .await;
    }

    struct FailingWarm;
    #[async_trait::async_trait]
    impl crate::executor::RunExecutor for FailingWarm {
        async fn execute(
            &self,
            _: crate::executor::RunJob,
            _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
        ) -> Result<broca_core::RunSummary, broca_core::RunError> {
            unreachable!("only warm is tested")
        }
        async fn warm(
            &self,
            _: WarmJob,
        ) -> Result<broca_core::WarmSent, crate::executor::WarmError> {
            let error = serde_json::from_value(serde_json::json!({"class":"permanent", "status":400, "message":"WARM-PRIVATE-MARKER", "provider_code":"WARM-PRIVATE-MARKER"})).unwrap();
            Err(crate::executor::WarmError::Provider(Box::new(error)))
        }
    }

    #[tokio::test]
    async fn automatic_warm_checks_the_move_latch_before_provider_work() {
        use broca_core::{FileLeaseStore, LeaseStore};
        let root = broca_test_support::scratch_root()
            .join(format!("move-auto-warm-{}", std::process::id()));
        let wal = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let key = BindIdentity::new("/p", "runner", "warm");
        let lease = FileLeaseStore::new(root.join("leases"))
            .acquire(&broca_core::lease_key_for(&key))
            .unwrap();
        let (handle, _lease) =
            WalHandle::open_and_spawn_observed(wal.clone(), key.clone(), lease, None)
                .await
                .unwrap();
        let writer = Arc::new(handle);
        let WalRecord::RunStarted { config, .. } = crate::move_install::tests::started(&key, "r")
        else {
            unreachable!()
        };
        let clock = CacheClock::default();
        clock.note_sent(epoch_ms(), Some(600_000));
        let warmer = AutomaticWarmer {
            retention: false,
            session: key.clone(),
            writer: writer.clone(),
            executor: Arc::new(FailingWarm),
            clock,
            seal: ShutdownSeal::default(),
            slot: WarmSlot::default(),
            declare: Arc::new(|_| {
                Some(crate::cache::CacheDeclaration {
                    ttl_ms: Some(600_000),
                    ttl_options_ms: vec![],
                    read_refreshes_ttl: Some(true),
                    warm: Ok(()),
                    basis: None,
                })
            }),
            recorded: Mutex::new(Some(RecordedRequest {
                run_id: RunId("r".into()),
                step_id: broca_protocol::StepId(1),
                config: *config.clone(),
                opts: broca_protocol::CallOptions::new(vec![]),
                request_body_sha256: "digest".into(),
                route: None,
            })),
        };
        let WarmTick::Stop {
            detail: Some(detail),
            ..
        } = warmer.warm(&config, &RunId("r".into())).await
        else {
            panic!("positive provider control must stop")
        };
        assert!(detail.contains("WARM-PRIVATE-MARKER"));
        let path = wal.wal_path(&key);
        let before = std::fs::read(&path).unwrap();
        broca_wal::migration::freeze(
            &root,
            &path,
            &broca_wal::migration::Freeze {
                cut_id: "cut".into(),
                end_offset: before.len() as u64,
                lineage_id: "lineage".into(),
            },
        )
        .unwrap()
        .unwrap();
        let WarmTick::Stop { reason, .. } = warmer.warm(&config, &RunId("r".into())).await else {
            panic!("frozen warm must stop")
        };
        assert_eq!(reason, "session_moving");
        broca_wal::migration::release(&root, &path, "cut", "remote")
            .unwrap()
            .unwrap();
        let WarmTick::Stop { reason, .. } = warmer.warm(&config, &RunId("r".into())).await else {
            panic!("moved warm must stop")
        };
        assert_eq!(reason, "session_moved");
        assert_eq!(std::fs::read(path).unwrap(), before);
        drop(warmer);
        let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
    }

    #[tokio::test]
    async fn retention_keep_warm_stop_detail_is_variant_only() {
        let root = broca_test_support::scratch_root()
            .join(format!("retention-warm-redaction-{}", std::process::id()));
        let wal = broca_core::FileWalStore::new(root.join("wal"));
        let key = BindIdentity::new("/retention-test", "runner", "warm-redaction");
        let writer = Arc::new(WalHandle::spawn(wal.open(&key).unwrap(), 1).unwrap());
        let started: WalRecord = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
        ))
        .unwrap();
        let WalRecord::RunStarted { config, .. } = started else {
            unreachable!()
        };
        for retention in [true, false] {
            let clock = CacheClock::default();
            clock.note_sent(epoch_ms(), Some(600_000));
            let warmer = AutomaticWarmer {
                retention,
                session: key.clone(),
                writer: writer.clone(),
                executor: Arc::new(FailingWarm),
                clock,
                seal: ShutdownSeal::default(),
                slot: WarmSlot::default(),
                declare: Arc::new(|_| {
                    Some(crate::cache::CacheDeclaration {
                        ttl_ms: Some(600_000),
                        ttl_options_ms: vec![],
                        read_refreshes_ttl: Some(true),
                        warm: Ok(()),
                        basis: None,
                    })
                }),
                recorded: Mutex::new(Some(RecordedRequest {
                    run_id: RunId("r1".into()),
                    step_id: broca_protocol::StepId(1),
                    config: *config.clone(),
                    opts: broca_protocol::CallOptions::new(vec![]),
                    request_body_sha256: "digest".into(),
                    route: None,
                })),
            };
            let WarmTick::Stop {
                detail: Some(detail),
                ..
            } = warmer.warm(&config, &RunId("r1".into())).await
            else {
                panic!("provider error must stop warming")
            };
            if retention {
                assert_eq!(detail, "Provider");
                assert!(!detail.contains("WARM-PRIVATE-MARKER"));
            } else {
                assert!(
                    detail.contains("WARM-PRIVATE-MARKER"),
                    "forever positive control preserves diagnostics"
                );
            }
        }
        let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }
}
