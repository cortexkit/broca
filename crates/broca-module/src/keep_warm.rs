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
        let sent = match self
            .executor
            .warm(WarmJob {
                session: self.session.clone(),
                config: config.clone(),
                base,
            })
            .await
        {
            Ok(sent) => sent,
            Err(crate::executor::WarmError::Provider(error))
                if error.provider_code.as_deref()
                    == Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED) =>
            {
                return WarmTick::Stop {
                    reason: "quota".into(),
                    detail: Some(if self.retention {
                        "Provider".into()
                    } else {
                        broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()
                    }),
                };
            }
            Err(error) => {
                let detail = if self.retention {
                    error.variant_name().to_owned()
                } else {
                    format!("{error:?}")
                };
                return WarmTick::Stop {
                    reason: "unsupported".into(),
                    detail: Some(detail),
                };
            }
        };
        // Queue the provider's returned usage in CacheWarmed before awaiting disk sync.
        // Tool results can drop this future during sync; the run's next WAL barrier still
        // commits the queued record, so returned billable usage is not discarded.
        self.slot.note_headers(
            sent.sent_at_ms,
            sent.ratelimit_headers.iter().cloned().collect(),
        );
        self.writer.record(WalRecord::CacheWarmed {
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
                cost,
            }
        } else {
            WarmTick::Sent
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
