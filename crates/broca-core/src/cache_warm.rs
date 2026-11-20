//! Keeping a session's provider prompt cache warm between rounds.
//!
//! A provider that caches a request's prefix keeps it for a fixed lifetime from the moment the
//! request was sent. A caller whose next round arrives after that lifetime pays to write the whole
//! prefix again. A warm re-sends the session's most recent model request, shaped so the provider
//! generates nothing, which reads the cached prefix and restarts its lifetime.
//!
//! This module holds the provider-agnostic half: the exact bytes of the request in flight (kept
//! in memory by the run's transport), the per-session clock that says when the cache was last
//! refreshed, the reconstruction of the most recent request from the WAL when no run holds it in
//! memory, and the facts the WAL can answer before anything is sent.

use std::sync::{Arc, Mutex};

use broca_protocol::{
    error::ProviderError, AttemptOutcome, CallOptions, RunId, StepContext, StepId, ToolChoice,
    Usage,
};
use broca_wal::{RunConfig, WalRecord};

/// A warm is refused, and nothing sent, when less than this much of the cache's estimated
/// lifetime remains. The estimate is only that, so a warm at the very edge could arrive after the
/// provider dropped the prefix and pay for a full cache write instead of a read.
pub const WARM_EXPIRY_MARGIN_MS: u64 = 5_000;

/// The only basis broca has for a cache's expiry: the provider's documented lifetime counted from
/// when broca started sending. No provider reports the expiry itself.
pub const EXPIRY_BASIS_ESTIMATED: &str = "estimated";

/// New source-maintenance records opt into source-model billing attribution. Other trigger
/// values, including absence, retain the historical projection for the same WAL bytes.
pub const SOURCE_KEEP_WARM_TRIGGER: &str = "source_keep_warm";

/// The exact bytes of one rendered model request, as the transport sent them.
#[derive(Clone)]
pub struct SentRequest {
    /// The step the request belongs to.
    pub step_id: StepId,
    /// The request body, shared with the transport rather than copied.
    pub body: Arc<dyn AsRef<[u8]> + Send + Sync>,
    /// Lowercase hex SHA-256 of `body`, the digest the attempt's record carries.
    pub sha256: String,
    /// When the transport started sending it, in milliseconds since the Unix epoch.
    pub sent_at_ms: u64,
    /// The model the transport sent it to: the episode's own model, or the refusal-fallback
    /// model while a refusal round routes steps there. A warm of these bytes must be shaped and
    /// addressed for the same model, or it would refresh a cache the run is not using.
    pub route: crate::refusal_fallback::StepRoute,
}

impl SentRequest {
    pub fn bytes(&self) -> &[u8] {
        (*self.body).as_ref()
    }
}

impl std::fmt::Debug for SentRequest {
    // The body holds the prompt, so it is never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentRequest")
            .field("step_id", &self.step_id)
            .field("body", &format_args!("<{} bytes>", self.bytes().len()))
            .field("sha256", &self.sha256)
            .field("sent_at_ms", &self.sent_at_ms)
            .field("route", &self.route)
            .finish()
    }
}

/// When a session last sent a request whose prefix the provider caches, and how long the
/// provider keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheClockReading {
    pub last_sent_at_ms: u64,
    /// The cache lifetime, when the session's cache policy declares one.
    pub ttl_ms: Option<u64>,
    /// The most recent request's reported input occupancy, not cumulative run usage.
    pub prefix_tokens: Option<u64>,
}

impl CacheClockReading {
    /// The estimated expiry: the last send start plus the lifetime.
    pub fn expires_at_ms(&self) -> Option<u64> {
        self.ttl_ms
            .map(|ttl_ms| self.last_sent_at_ms.saturating_add(ttl_ms))
    }
}

/// One session's cache clock, shared by every run of the session and by its warms. It lives in
/// memory, so after a restart it starts empty; [`CacheClock::seed_if_unknown`] then rebuilds it
/// from the session's WAL.
#[derive(Clone, Default)]
pub struct CacheClock(Arc<Mutex<Option<CacheClockReading>>>);

impl CacheClock {
    /// Whether another handle currently shares this clock. A cache table may
    /// discard a table-only clock while holding the lock that protects cloning
    /// its entries; shared clocks must remain discoverable while owners update them.
    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }

    /// A request started sending at `sent_at_ms`. The clock never moves backwards, so a slow
    /// attempt that reports late cannot undo a later send.
    pub fn note_sent(&self, sent_at_ms: u64, ttl_ms: Option<u64>) {
        let mut reading = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        match reading.as_mut() {
            Some(current) if current.last_sent_at_ms > sent_at_ms => {}
            _ => {
                *reading = Some(CacheClockReading {
                    last_sent_at_ms: sent_at_ms,
                    ttl_ms,
                    prefix_tokens: None,
                })
            }
        }
    }

    /// Attach usage only to the send it describes; a late response cannot label a newer send.
    pub fn note_usage(&self, sent_at_ms: u64, usage: Usage) {
        let mut reading = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(current) = reading.as_mut().filter(|r| r.last_sent_at_ms == sent_at_ms) {
            current.prefix_tokens = prefix_tokens(usage);
        }
    }

    pub fn read(&self) -> Option<CacheClockReading> {
        *self.0.lock().unwrap_or_else(|poison| poison.into_inner())
    }
}

/// Each reported input component contributes; no reported component means unknown, not zero.
pub fn prefix_tokens(usage: Usage) -> Option<u64> {
    let counts = [
        usage.input_tokens,
        usage.cached_input_tokens,
        usage.cache_write_tokens,
    ];
    counts.into_iter().flatten().reduce(u64::saturating_add)
}

// A standby source warm can finish after a fallback request. Its digest identifies another
// request and must not extend the active route's clock or replace its token-count evidence.

/// Whether a model request attempt reached the provider and was accepted for processing, so its
/// prompt prefix was read into or written to the provider's cache. A 2xx status says so, and so
/// does 101 with an answer: a request on a WebSocket connection records 101, the socket's upgrade
/// status, at the moment it is sent on the open socket, and its outcome says whether the provider
/// answered it. Before this counted them, every ChatGPT WebSocket request was left out, so the
/// session's last send looked hours old and its cache looked expired. Not counted:
/// - no status at all (`connect_failed`, `header_timeout`, `no_status`, or a cancel or shutdown
///   before the status line) means the provider may never have seen the request;
/// - a 4xx or 5xx status is a refusal before any generation, which leaves the cache alone;
/// - `provider_error` (an error event inside a 2xx body, such as an overload) and an outcome
///   this build does not know may also be refusals, so they are not counted either.
///
/// Counted: `complete`, and the 2xx or 101 attempts that broke off while the response streamed
/// (`eof_before_completion`, `chunk_error`, `idle_timeout`, `content_timeout`, `byte_limit`,
/// `decode_error`, `rate_floor`, `cancelled`, `sealed_in_flight`). Leaving out a doubtful attempt
/// only falls back to an earlier send, which errs early.
pub fn attempt_reached_the_cache(http_status: Option<u16>, outcome: &AttemptOutcome) -> bool {
    http_status.is_some_and(|status| (200..300).contains(&status) || status == 101)
        && !matches!(
            outcome,
            AttemptOutcome::ProviderError | AttemptOutcome::Unknown(_)
        )
}

/// Whether a warm sent at `now_ms` would still find the cached prefix. `Err` carries the reason
/// it would not: the expiry is unknown, or less than [`WARM_EXPIRY_MARGIN_MS`] of it remains.
pub fn check_warm_expiry(reading: Option<CacheClockReading>, now_ms: u64) -> Result<u64, String> {
    let Some(reading) = reading else {
        return Err(
            "no request of this session has been sent since this broca started, so the cache's \
             expiry is unknown"
                .into(),
        );
    };
    let Some(expires_at_ms) = reading.expires_at_ms() else {
        return Err("the session's cache policy declares no lifetime".into());
    };
    if expires_at_ms.saturating_sub(now_ms) < WARM_EXPIRY_MARGIN_MS {
        return Err(format!(
            "the cache's estimated expiry {expires_at_ms} is less than {WARM_EXPIRY_MARGIN_MS} ms \
             away (now {now_ms}); a warm would write the whole prefix again"
        ));
    }
    Ok(expires_at_ms)
}

/// What a sent warm reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmSent {
    /// Actual warm request body length, absent when the sender did not measure it.
    pub request_body_bytes: Option<u64>,
    /// Lowercase hex SHA-256 of the bytes actually sent.
    pub warm_body_sha256: String,
    /// When sending started, in milliseconds since the Unix epoch.
    pub sent_at_ms: u64,
    /// The usage the provider reported, each count absent when it reported none.
    pub usage: Usage,
    /// The account the credential that sent the warm belongs to, when known.
    pub served_account_ref: Option<String>,
    /// The provider's rate-limit headers on the response, verbatim (lowercase names).
    pub ratelimit_headers: Vec<(String, String)>,
    pub http_status: u16,
}

/// Missing or reported-zero counts consume no warm budget and require no price lookup.
/// Here `Usage.output_tokens` already includes reasoning tokens; do not add them again.
pub fn warm_budget_needs_pricing(envelope: &broca_wal::record::AttemptEnvelope) -> bool {
    envelope.sent != broca_wal::record::AttemptSent::No
        && envelope.usage.is_some_and(|usage| {
            [
                usage.input_tokens,
                usage.cached_input_tokens,
                usage.cache_write_tokens,
                usage.output_tokens,
                envelope.cache_write_5m_tokens,
                envelope.cache_write_1h_tokens,
            ]
            .into_iter()
            .flatten()
            .any(|tokens| tokens > 0)
        })
}

/// The same reservation and measured outcome used by live and restarted warm-budget pricing.
/// The reservation identifies the actual model, which can differ from the episode's model.
#[derive(Debug, Clone)]
pub struct FailedWarmAttempt {
    pub reservation: broca_wal::record::AttemptReservation,
    pub envelope: broca_wal::record::AttemptEnvelope,
    /// Marker TTL captured when the actual frozen route was prepared; absent means unknown.
    pub ttl_ms: Option<u64>,
}

/// Warm-only send facts taken from the exact frozen render route used for the request.
#[derive(Debug, Clone, Default)]
pub struct WarmSendPolicy {
    /// Whether the served token was refreshed after admission recorded its resolved credential ID.
    pub auth_refreshed: bool,
    /// A routed fallback's catalog provider; the source provider is already in the reservation.
    pub provider: Option<String>,
    /// The selected frozen marker policy, never the episode's cache-clock estimate.
    pub ttl_ms: Option<u64>,
}

/// A warm owns its journal even when no run is active. The seal orders permission and
/// settlement against shutdown, while the writer's barrier precedes every physical send.
/// A full WAL can refuse a terminal append even after permission was durable. Such a reservation
/// remains conservative possibly-billed evidence for reclaim; a per-ID terminal is not guaranteed
/// across storage refusal or power loss.
pub struct WarmJournal {
    writer: Arc<crate::WalHandle>,
    seal: crate::ShutdownSeal,
    template: broca_wal::record::AttemptReservation,
    request_body_sha256: String,
    trigger: Option<String>,
    pending: Mutex<std::collections::BTreeMap<broca_wal::record::AttemptId, WarmPending>>,
    #[cfg(feature = "test-clock")]
    permission_gate: Mutex<Option<Arc<WarmPermissionGate>>>,
}

/// A deterministic pending durability barrier for cross-layer warm permission tests.
#[cfg(feature = "test-clock")]
#[derive(Default)]
pub struct WarmPermissionGate {
    pub entered: tokio::sync::Notify,
    pub release: tokio_util::sync::CancellationToken,
    pub acknowledged: tokio::sync::Notify,
    pub successful_ack: std::sync::atomic::AtomicBool,
    pub receiver_dropped: std::sync::atomic::AtomicBool,
    late_success: bool,
}

#[cfg(feature = "test-clock")]
impl WarmPermissionGate {}

struct WarmPending {
    reservation: broca_wal::record::AttemptReservation,
    started: bool,
    ttl_ms: Option<u64>,
}

impl WarmJournal {
    fn terminal(
        &self,
        id: broca_wal::record::AttemptId,
        pending: &WarmPending,
        envelope: broca_wal::record::AttemptEnvelope,
        sent: Option<WarmSent>,
        warm_body_sha256: String,
    ) -> WalRecord {
        WalRecord::CacheWarmed {
            attempt_id: Some(id),
            reservation: Some(Box::new(pending.reservation.clone())),
            usage: envelope.usage.unwrap_or_default(),
            sent_at_ms: envelope.sent_at_ms.unwrap_or_default(),
            envelope: Some(Box::new(envelope)),
            trigger: self.trigger.clone(),
            run_id: pending.reservation.run_id.clone(),
            step_id: pending.reservation.step_id,
            request_body_sha256: self.request_body_sha256.clone(),
            warm_body_sha256,
            served_account_ref: pending.reservation.credential.account_ref.clone(),
            ratelimit_headers: sent
                .as_ref()
                .map(|s| s.ratelimit_headers.iter().cloned().collect())
                .unwrap_or_default(),
            ttl_ms: pending.ttl_ms,
            expires_at_ms: sent
                .as_ref()
                .and_then(|s| pending.ttl_ms.map(|ttl| s.sent_at_ms.saturating_add(ttl))),
        }
    }

    fn fallback(&self, id: broca_wal::record::AttemptId, pending: &WarmPending) -> WalRecord {
        use broca_wal::record::{AttemptEnvelope, AttemptSent};
        let envelope = AttemptEnvelope {
            sent: if pending.started {
                AttemptSent::Unknown
            } else {
                AttemptSent::No
            },
            finished_at_ms: Some(warm_epoch_ms()),
            ..Default::default()
        };
        if pending.started {
            self.terminal(id, pending, envelope, None, String::new())
        } else {
            WalRecord::AttemptReleased {
                attempt_id: id,
                reservation: Box::new(pending.reservation.clone()),
                envelope: Box::new(envelope),
            }
        }
    }

    fn abandon(&self, id: &broca_wal::record::AttemptId) {
        if let Some(pending) = self.pending.lock().unwrap().remove(id) {
            // The seal may already own the fallback; settling through it prevents duplicates.
            let _ = self.seal.settle_billing_attempt(
                &self.writer,
                id,
                self.fallback(id.clone(), &pending),
            );
        }
    }
}

impl Drop for WarmJournal {
    fn drop(&mut self) {
        let ids: Vec<_> = self.pending.lock().unwrap().keys().cloned().collect();
        for id in ids {
            self.abandon(&id);
        }
    }
}

struct WarmPermission<'a> {
    journal: &'a WarmJournal,
    id: Option<broca_wal::record::AttemptId>,
}
impl Drop for WarmPermission<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.journal.abandon(&id);
        }
    }
}

fn warm_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn warm_journal_error(detail: impl std::fmt::Display) -> ProviderError {
    ProviderError {
        class: broca_protocol::ErrorClass::Permanent,
        message: format!("cache warm attempt durability failed: {detail}"),
        provider_code: Some("attempt_durability_failed".into()),
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        status: None,
        retry_after_secs: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

#[async_trait::async_trait]
impl crate::run::AttemptHook for WarmJournal {
    async fn finished(
        &self,
        _id: broca_wal::record::AttemptId,
        _attempt: broca_protocol::ModelAttempt,
        _outcome: broca_wal::record::BillingAttemptOutcome,
        _response: Option<broca_wal::record::AttemptEnvelope>,
    ) -> Result<(), ProviderError> {
        Err(warm_journal_error(
            "model settlement sent to a warm journal",
        ))
    }

    async fn before_send(
        &self,
        send: crate::run::PhysicalSend,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<broca_wal::record::AttemptId, ProviderError> {
        if cancel.is_cancelled() {
            return Err(warm_journal_error("reservation cancelled"));
        }
        let id = crate::attempt_id::mint_attempt_id().map_err(warm_journal_error)?;
        let pending = WarmPending {
            reservation: broca_wal::record::AttemptReservation {
                attempt: send.attempt,
                send_index: send.send_index,
                reserved_at_ms: warm_epoch_ms(),
                endpoint_host: send.endpoint_host,
                model_sent: send.model_sent,
                request_body_bytes: send.request_body_bytes,
                compressed: send.compressed,
                provider: send
                    .warm
                    .as_ref()
                    .and_then(|policy| policy.provider.clone())
                    .unwrap_or_else(|| self.template.provider.clone()),
                credential: crate::admission::attempt_credential_for_send(
                    &self.template.credential,
                    send.account_ref,
                    send.auth_refreshed,
                ),
                run: broca_wal::record::AttemptRun {
                    speed_requested: send.speed_requested,
                    ..self.template.run.clone()
                },
                ..self.template.clone()
            },
            started: false,
            ttl_ms: send.warm.as_ref().and_then(|policy| policy.ttl_ms),
        };
        let record = WalRecord::AttemptReserved {
            attempt_id: id.clone(),
            reservation: Box::new(pending.reservation.clone()),
        };
        if !self
            .seal
            .reserve_billing_attempt(
                &self.writer,
                id.clone(),
                record,
                self.fallback(id.clone(), &pending),
            )
            .map_err(warm_journal_error)?
        {
            return Err(warm_journal_error(
                "shutdown sealed before warm reservation",
            ));
        }
        self.pending.lock().unwrap().insert(id.clone(), pending);
        let mut permission = WarmPermission {
            journal: self,
            id: Some(id.clone()),
        };
        let barrier = async {
            #[cfg(feature = "test-clock")]
            {
                let gate = self.permission_gate.lock().unwrap().clone();
                if let Some(gate) = gate {
                    if !gate.late_success {
                        gate.entered.notify_one();
                        gate.release.cancelled().await;
                        return Err(crate::WalWriteError::from(
                            "warm durability barrier refused by test gate",
                        ));
                    }
                    let result = self.writer.sync().await.map(|_| ());
                    let (ack, receipt) = tokio::sync::oneshot::channel();
                    tokio::spawn(async move {
                        gate.entered.notify_one();
                        gate.release.cancelled().await;
                        gate.successful_ack
                            .store(result.is_ok(), std::sync::atomic::Ordering::SeqCst);
                        gate.receiver_dropped.store(
                            ack.send(result).is_err(),
                            std::sync::atomic::Ordering::SeqCst,
                        );
                        gate.acknowledged.notify_one();
                    });
                    return receipt.await.map_err(|_| {
                        crate::WalWriteError::from("warm test acknowledgement task ended")
                    })?;
                }
            }
            self.writer.sync().await.map(|_| ())
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(warm_journal_error("reservation cancelled before send permission")),
            _ = self.seal.tripped() => return Err(warm_journal_error("shutdown sealed before send permission")),
            result = tokio::time::timeout(std::time::Duration::from_secs(30), barrier) => {
                result.map_err(|_| warm_journal_error("reservation permission deadline expired"))?.map_err(warm_journal_error)?;
            }
        }
        if cancel.is_cancelled() {
            return Err(warm_journal_error(
                "reservation cancelled before warm start",
            ));
        }
        {
            let mut pending = self.pending.lock().unwrap();
            let pending = pending
                .get_mut(&id)
                .expect("warm reservation retained until permission");
            let started = WarmPending {
                reservation: pending.reservation.clone(),
                started: true,
                ttl_ms: pending.ttl_ms,
            };
            if !self
                .seal
                .start_billing_attempt(&id, self.fallback(id.clone(), &started))
            {
                return Err(warm_journal_error("shutdown sealed before warm start"));
            }
            pending.started = true;
        }
        permission.id.take();
        Ok(id)
    }

    #[cfg(feature = "test-clock")]
    fn set_warm_permission_gate_for_test(&self, gate: Arc<WarmPermissionGate>) {
        *self.permission_gate.lock().unwrap() = Some(gate);
    }
}

/// Why a provider did not warm.
#[derive(Debug, Clone, PartialEq)]
pub enum WarmFailure {
    /// The provider, or the shape the session froze, cannot be warmed. Nothing was sent.
    Unsupported(String),
    /// The credential now belongs to a different account than the one the session froze.
    /// Nothing was sent.
    AccountChanged {
        frozen: Option<String>,
        served: Option<String>,
    },
    /// The warm was sent and the provider refused it.
    Provider(Box<ProviderError>),
}

/// The most recent model request a session's WAL records, rebuilt as the run loop assembled it.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub run_id: RunId,
    pub step_id: StepId,
    /// The episode's config as it stood when the step was sent (a refreeze applied).
    pub config: RunConfig,
    pub opts: CallOptions,
    /// The digest the step's attempt records carry.
    pub request_body_sha256: String,
    /// Set when the request went to the refusal-fallback model instead of the episode's: the
    /// warm must render it for that model (`opts.target_model` already names it), and its
    /// digest only matches when it does. `None` for a request to the episode's own model.
    pub route: Option<broca_wal::StepRoute>,
}

/// Why the most recent request cannot be rebuilt from the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedRequestError {
    /// The session never sent a model request.
    NoStep,
    /// A request was sent, but the WAL cannot reproduce it.
    Unavailable(String),
}

/// Whether the WAL records any model request at all.
pub fn has_model_step(records: &[WalRecord]) -> bool {
    records.iter().any(|record| {
        matches!(
            record,
            WalRecord::ModelAttemptFinished { .. } | WalRecord::ModelStepFinished { .. }
        )
    })
}

/// The config of the episode starting at `start`, with its refreezes before `end` applied.
fn effective_config(records: &[WalRecord], start: usize, end: usize) -> Option<(RunId, RunConfig)> {
    let WalRecord::RunStarted { run_id, config, .. } = &records[start] else {
        return None;
    };
    let mut config = (**config).clone();
    for record in &records[start..end] {
        fold_refreeze(&mut config, record);
    }
    Some((run_id.clone(), config))
}

pub(crate) fn fold_refreeze(config: &mut RunConfig, record: &WalRecord) {
    match record {
        WalRecord::RunConfigRefrozen {
            renderer_version,
            frozen_render_config,
            ..
        } => {
            config.renderer_version = *renderer_version;
            config.frozen_render_config = frozen_render_config.clone();
        }
        WalRecord::RunAuthRefrozen { auth_selection, .. } => {
            config.auth_selection = Some(auth_selection.clone())
        }
        _ => {}
    }
}

pub fn last_recorded_request_with_lineage(
    records: &[WalRecord],
    lineage: Option<&str>,
) -> Result<RecordedRequest, RecordedRequestError> {
    let mode3 = crate::is_mode3_log(records.iter());
    let Some(attempt_at) = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::ModelAttemptFinished { .. }))
    else {
        return Err(if has_model_step(records) {
            RecordedRequestError::Unavailable(
                "the session's model steps carry no record of the request bytes they sent".into(),
            )
        } else {
            RecordedRequestError::NoStep
        });
    };
    let WalRecord::ModelAttemptFinished {
        step_id,
        request_body_sha256,
        ..
    } = &records[attempt_at]
    else {
        unreachable!("the position found is an attempt record");
    };
    let start = records[..attempt_at]
        .iter()
        .rposition(|record| matches!(record, WalRecord::RunStarted { .. }))
        .ok_or_else(|| {
            RecordedRequestError::Unavailable(
                "the episode that sent the request is not in the WAL".into(),
            )
        })?;
    let step_started = records[start..attempt_at]
        .iter()
        .rposition(|record| matches!(record, WalRecord::StepStarted { step_id: s } if s == step_id))
        .map(|offset| start + offset)
        .ok_or_else(|| {
            RecordedRequestError::Unavailable(format!(
                "the WAL has no step_started record for step {}",
                step_id.0
            ))
        })?;
    let (run_id, config) = effective_config(records, start, attempt_at).ok_or_else(|| {
        RecordedRequestError::Unavailable("the episode's run_started is unreadable".into())
    })?;
    let committed_route = records[attempt_at..]
        .iter()
        .find_map(|record| match record {
            WalRecord::ModelStepFinished {
                step_id: committed,
                fallback,
                ..
            } if committed == step_id => Some(fallback.route.clone()),
            _ => None,
        });
    // A refusal recorded before the attempt means the attempt is the step's re-send.
    let resent_after_refusal =
        records[step_started..attempt_at]
            .iter()
            .find_map(|record| match record {
                WalRecord::ModelStepRefused {
                    step_id: refused,
                    schedule,
                    ..
                } if refused == step_id => Some(broca_wal::StepRoute {
                    model: schedule.fallback.clone(),
                    reason: broca_wal::StepRouteReason::Refusal,
                }),
                _ => None,
            });

    let mut prior = crate::replay::replay(&records[..step_started])
        .map_err(|error| RecordedRequestError::Unavailable(format!("replay the WAL: {error}")))?;
    // The live loop decorates repeated tool results before sending them; replay keeps the raw
    // results, so the same decoration is reapplied per episode.
    let spans = prior.episode_step_spans.clone().ok_or_else(|| {
        RecordedRequestError::Unavailable("the replayed prompt has no episode boundaries".into())
    })?;
    for span in spans {
        if span.end <= prior.prompt.len() {
            crate::run::restore_repetition_guidance(&mut prior.prompt[span]);
        }
    }
    // A step that has not committed went where the schedule pointed when it opened. Whether
    // the run was eligible for the fallback is not in the WAL, so this is a best guess, which
    // the caller's digest comparison confirms before anything is sent.
    let route = match committed_route {
        Some(route) => route,
        None => resent_after_refusal.or_else(|| {
            prior
                .refusal_fallback
                .as_ref()
                .filter(|schedule| {
                    schedule.source == config.model
                        && crate::refusal_fallback::route(Some(schedule), true)
                            == crate::refusal_fallback::StepRoute::Fallback
                })
                .map(|schedule| broca_wal::StepRoute {
                    model: schedule.fallback.clone(),
                    reason: broca_wal::StepRouteReason::Schedule,
                })
        }),
    };
    let mut prompt = prior.prompt;
    // Steers written before the step opened are rendered by that step; replay of the records
    // before `step_started` holds them back as pending. The step numbered them as it placed
    // them, so the rebuild numbers them the same way.
    let placed = prompt.len();
    prompt.extend(prior.pending_steers);
    crate::tags::TagNumbering::number_appended(&mut prompt, placed, &config).map_err(|error| {
        RecordedRequestError::Unavailable(format!("number the held-back steers: {error}"))
    })?;
    broca_protocol::content::stamp_ordinals_from(&mut prompt, 0);
    // A run frozen with `render_tags` sent its tagged user and tool-result text with the tag
    // prefix, after the guidance above; the rebuilt request must carry the same bytes. Any run
    // sent the model's text cleaned where an earlier run under the computed rules cleans it.
    if !mode3 && config.render_tags {
        broca_protocol::tags::render_tags_into(
            &mut prompt,
            broca_protocol::tags::TagRendering::for_render_config(&config.frozen_render_config),
        );
    } else if !mode3 {
        broca_protocol::tags::clean_computed_into(&mut prompt);
    }
    // The one turn a budget allows after its finalize message renders with tool calls disabled.
    let tool_choice = if prior.budget_finalize_pending {
        ToolChoice::None
    } else {
        config.tool_choice.clone()
    };
    let mut opts = CallOptions {
        declared_markers: Vec::new(),
        ephemeral_session: crate::retention::is_retention_records(records),
        prompt,
        tools: Arc::clone(&config.tools),
        tool_choice: Arc::new(tool_choice),
        response_format: Arc::new(config.response_format.clone()),
        generation: Arc::new(config.generation.clone()),
        provider_options: Arc::new(config.provider_options.clone()),
        step_context: Some(StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: run_id.0.clone(),
            step_index: step_id.0,
            // A cap the step lowered to fit its context limit is part of the bytes it sent.
            max_output_tokens: crate::output_reservation::recorded_cap(
                &records[..attempt_at],
                *step_id,
            ),
        }),
        target_model: Some(
            route
                .as_ref()
                .map_or_else(|| config.model.clone(), |route| route.model.clone()),
        ),
        marker_order_version: prior.marker_order_version,
        render_episodes: Arc::new(broca_protocol::request::RenderHistory {
            signed_floor: prior
                .signed_floor
                .known()
                .and_then(crate::signed_floor::SignedFloor::floor),
            ..prior.render_episodes.unwrap_or_default().into()
        }),
    };
    // Compaction may apply after StepStarted and before the attempt. Recover the view
    // through the attempt boundary, not just the prompt boundary used for steers.
    if mode3 {
        let lineage = lineage.ok_or_else(|| {
            RecordedRequestError::Unavailable(
                "mode-3 request reconstruction requires the WAL lineage".into(),
            )
        })?;
        let frames: Vec<_> = records[..attempt_at]
            .iter()
            .enumerate()
            .map(|(index, record)| broca_wal::FramedRecord {
                seq: index as u64 + 1,
                fence: 0,
                record: record.clone(),
            })
            .collect();
        let session = frames
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::RunStarted { session, .. } => Some(session),
                _ => None,
            })
            .expect("run found above");
        let state = crate::compaction_v2::session::load(session, &frames)
            .map_err(RecordedRequestError::Unavailable)?;
        opts = crate::compaction_v2::render::request_options(
            &frames,
            lineage,
            opts,
            state.view_for_step(step_id.0),
        )
        .map_err(RecordedRequestError::Unavailable)?;
    } else {
        let compaction = crate::replay::replay(&records[..attempt_at])
            .map_err(|error| {
                RecordedRequestError::Unavailable(format!("replay compaction: {error}"))
            })?
            .compaction;
        if let Some(state) = compaction.state() {
            opts = crate::compaction::model_options(&opts, state)
                .map_err(|error| RecordedRequestError::Unavailable(error.to_string()))?;
        }
    }
    Ok(RecordedRequest {
        run_id,
        step_id: *step_id,
        config,
        opts,
        request_body_sha256: request_body_sha256.clone(),
        route,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_expiry_fence_refuses_under_five_seconds_and_unknown_clocks() {
        let reading = |ttl_ms| {
            Some(CacheClockReading {
                last_sent_at_ms: 1_000_000,
                ttl_ms,
                prefix_tokens: None,
            })
        };
        assert_eq!(
            check_warm_expiry(reading(Some(300_000)), 1_000_000 + 300_000 - 5_000),
            Ok(1_300_000)
        );
        assert!(check_warm_expiry(reading(Some(300_000)), 1_000_000 + 300_000 - 4_999).is_err());
        assert!(check_warm_expiry(reading(Some(300_000)), 2_000_000).is_err());
        assert!(check_warm_expiry(reading(None), 1_000_000).is_err());
        assert!(check_warm_expiry(None, 1_000_000).is_err());
    }

    #[test]
    fn prefix_tokens_counts_reported_input_components_only() {
        assert_eq!(prefix_tokens(Usage::default()), None);
        assert_eq!(
            prefix_tokens(Usage {
                output_tokens: Some(16),
                ..Usage::default()
            }),
            None
        );
        assert_eq!(
            prefix_tokens(Usage {
                cached_input_tokens: Some(40),
                ..Usage::default()
            }),
            Some(40)
        );
        assert_eq!(
            prefix_tokens(Usage {
                input_tokens: Some(3),
                cached_input_tokens: Some(40),
                cache_write_tokens: Some(7),
                output_tokens: Some(100),
                ..Usage::default()
            }),
            Some(50)
        );
        assert_eq!(
            prefix_tokens(Usage {
                input_tokens: Some(0),
                ..Usage::default()
            }),
            Some(0)
        );
    }
}
