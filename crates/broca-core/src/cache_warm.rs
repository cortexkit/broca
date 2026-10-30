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
    error::{ProviderError, ACCOUNT_LIMIT_EXHAUSTED},
    AttemptOutcome, CallOptions, RunId, StepContext, StepId, ToolChoice, Usage,
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
    pub fn new() -> Self {
        Self::default()
    }

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

    /// When this process has noted no send of the session (it restarted since, or forgot the
    /// session), seed the clock from `records`, the session's WAL, with the latest send
    /// [`last_cache_send_at_ms`] finds there and `ttl_ms`, the lifetime the session's frozen cache
    /// policy declares. Without such a send the clock stays unknown.
    ///
    /// A send noted in memory always wins: it is never older than what the WAL holds, because
    /// every send this process makes is noted before its record is written. The seed is noted
    /// like any send, so the next real send or warm moves the clock on from it as usual.
    pub fn seed_if_unknown(&self, records: &[WalRecord], ttl_ms: Option<u64>) {
        if self.read().is_some() {
            return;
        }
        if let Some(sent_at_ms) = last_cache_send_at_ms(records) {
            self.note_sent(sent_at_ms, ttl_ms);
            if let Some(usage) = last_cache_usage(records, sent_at_ms) {
                self.note_usage(sent_at_ms, usage);
            }
        }
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

fn last_cache_usage(records: &[WalRecord], last_sent_at_ms: u64) -> Option<Usage> {
    let mut bound = SendLowerBound::default();
    let mut latest_step = None;
    let mut usage = None;
    for record in records {
        match record {
            WalRecord::ModelAttemptFinished {
                step_id,
                sent_at_ms,
                http_status,
                outcome,
                ..
            } if attempt_reached_the_cache(*http_status, outcome) => {
                if sent_at_ms.or(bound.at_ms) == Some(last_sent_at_ms) {
                    latest_step = Some(*step_id);
                    usage = None;
                }
            }
            WalRecord::ModelStepFinished {
                step_id,
                usage: reported,
                ..
            } if latest_step == Some(*step_id) => usage = Some(*reported),
            WalRecord::CacheWarmed {
                request_body_sha256,
                sent_at_ms,
                usage: reported,
                envelope,
                trigger,
                ..
            } if *sent_at_ms == last_sent_at_ms
                && warm_reached_the_cache(envelope.as_deref())
                && (trigger.as_deref() != Some(SOURCE_KEEP_WARM_TRIGGER)
                    || latest_cache_request_digest(records)
                        .is_none_or(|digest| digest == request_body_sha256)) =>
            {
                latest_step = None;
                usage = Some(*reported);
            }
            _ => {}
        }
        bound.observe(record);
    }
    usage
}

/// When the most recent request the session's WAL records as having reached the provider's cache
/// started sending: the later of its last such model request and its last matching warm. `None` when the
/// WAL records no such request, and the cache's age is then unknown.
///
/// The cache's lifetime starts when a request is sent, not when its response ends, so a model
/// request counts from its attempt's `sent_at_ms`. An attempt written before that field existed
/// records only when it ended, which can be minutes after the send; for it the latest time the WAL
/// proves passed BEFORE the send stands in (see [`SendLowerBound`]). That is earlier than the real
/// send, so the expiry is underestimated and a warm refused a little early, never sent so late that
/// the provider has already dropped the prefix and charges to write it again. An old attempt with
/// no such bound is not counted.
///
/// A warm counts from its own `sent_at_ms` only when its billing envelope records success.
/// Legacy warm records have no envelope and were written only for answered warms.
/// Only the new source-maintenance trigger opts into matching the latest request's digest;
/// unmarked records retain their historical clock inference.
pub fn last_cache_send_at_ms(records: &[WalRecord]) -> Option<u64> {
    let mut latest: Option<u64> = None;
    let mut bound = SendLowerBound::default();
    for record in records {
        let sent_at_ms = match record {
            WalRecord::ModelAttemptFinished {
                http_status,
                outcome,
                sent_at_ms,
                ..
            } if attempt_reached_the_cache(*http_status, outcome) => sent_at_ms.or(bound.at_ms),
            WalRecord::CacheWarmed {
                request_body_sha256,
                sent_at_ms,
                envelope,
                trigger,
                ..
            } if warm_reached_the_cache(envelope.as_deref())
                && (trigger.as_deref() != Some(SOURCE_KEEP_WARM_TRIGGER)
                    || latest_cache_request_digest(records)
                        .is_none_or(|digest| digest == request_body_sha256)) =>
            {
                Some(*sent_at_ms)
            }
            _ => None,
        };
        latest = latest.max(sent_at_ms);
        bound.observe(record);
    }
    latest
}

// A standby source warm can finish after a fallback request. Its digest identifies another
// request and must not extend the active route's clock or replace its token-count evidence.
fn latest_cache_request_digest(records: &[WalRecord]) -> Option<&str> {
    records.iter().rev().find_map(|record| match record {
        WalRecord::ModelAttemptFinished {
            request_body_sha256,
            http_status,
            outcome,
            ..
        } if attempt_reached_the_cache(*http_status, outcome) => Some(request_body_sha256.as_str()),
        _ => None,
    })
}

pub(crate) fn warm_reached_the_cache(
    envelope: Option<&broca_wal::record::AttemptEnvelope>,
) -> bool {
    use broca_wal::record::{AttemptSent, BillingAttemptOutcome};
    envelope.is_none_or(|e| {
        e.sent == AttemptSent::Yes
            && e.http_status.is_some_and(|s| (200..300).contains(&s))
            && e.outcome == BillingAttemptOutcome::Complete
    })
}

/// Whether a model request attempt reached the provider and was accepted for processing, so its
/// prompt prefix was read into or written to the provider's cache. Only a 2xx status says so:
/// - no status at all (`connect_failed`, `header_timeout`, `no_status`, or a cancel or shutdown
///   before the status line) means the provider may never have seen the request;
/// - a 4xx or 5xx status is a refusal before any generation, which leaves the cache alone;
/// - `provider_error` (an error event inside a 2xx body, such as an overload) and an outcome
///   this build does not know may also be refusals, so they are not counted either.
///
/// Counted: `complete`, and the 2xx attempts that broke off while the response streamed
/// (`eof_before_completion`, `chunk_error`, `idle_timeout`, `content_timeout`, `byte_limit`,
/// `decode_error`, `rate_floor`, `cancelled`, `sealed_in_flight`). Leaving out a doubtful attempt
/// only falls back to an earlier send, which errs early.
fn attempt_reached_the_cache(http_status: Option<u16>, outcome: &AttemptOutcome) -> bool {
    http_status.is_some_and(|status| (200..300).contains(&status))
        && !matches!(
            outcome,
            AttemptOutcome::ProviderError | AttemptOutcome::Unknown(_)
        )
}

/// The latest wall-clock time the WAL proves had passed before the next model request was sent,
/// for an attempt whose record carries no send time. Each record it reads is written before any
/// model request that follows it in the WAL is sent:
/// - `run_started` opens an episode before its first request;
/// - `run_config_refrozen` and `run_auth_refrozen` are synced before the request they apply to;
/// - `run_paused` and `run_finished` stop an episode, so any later request is sent after the
///   episode resumes or a new one starts;
/// - an earlier `model_attempt_finished` (its `ts_ms` is when that attempt ended) is written
///   before the next attempt or step starts, whatever its outcome.
///
/// A `cache_warmed` record is not read: a warm can run while a model request is in flight, so its
/// record can come after a send that started earlier than the warm.
#[derive(Default)]
struct SendLowerBound {
    at_ms: Option<u64>,
}

impl SendLowerBound {
    fn observe(&mut self, record: &WalRecord) {
        let at_ms = match record {
            WalRecord::RunStarted { ts_ms, .. }
            | WalRecord::RunConfigRefrozen { ts_ms, .. }
            | WalRecord::RunAuthRefrozen { ts_ms, .. }
            | WalRecord::RunPaused { ts_ms, .. }
            | WalRecord::RunFinished { ts_ms, .. } => *ts_ms,
            WalRecord::ModelAttemptFinished { ts_ms, .. } => Some(*ts_ms),
            _ => None,
        };
        self.at_ms = self.at_ms.max(at_ms);
    }
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
    failed: Mutex<Vec<FailedWarmAttempt>>,
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
impl WarmPermissionGate {
    /// Hold a real successful sync receipt, then acknowledge it independently of the waiter.
    pub fn successful() -> Self {
        Self {
            late_success: true,
            ..Self::default()
        }
    }
}

struct WarmPending {
    reservation: broca_wal::record::AttemptReservation,
    started: bool,
    ttl_ms: Option<u64>,
}

impl WarmJournal {
    pub fn new(
        writer: Arc<crate::WalHandle>,
        seal: crate::ShutdownSeal,
        template: broca_wal::record::AttemptReservation,
        request_body_sha256: String,
        trigger: Option<String>,
    ) -> Self {
        Self {
            writer,
            seal,
            template,
            request_body_sha256,
            trigger,
            pending: Mutex::new(Default::default()),
            failed: Mutex::new(Vec::new()),
            #[cfg(feature = "test-clock")]
            permission_gate: Mutex::new(None),
        }
    }

    /// Report only measured failed-attempt usage to the live warm budget. The WAL contains
    /// the same evidence for restart accounting; absent usage is never priced using the
    /// successful-refresh estimate.
    pub fn failed_attempts(&self) -> Vec<FailedWarmAttempt> {
        self.failed.lock().unwrap().clone()
    }

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
    }
}

#[async_trait::async_trait]
impl crate::run::AttemptHook for WarmJournal {
    #[cfg(feature = "test-clock")]
    fn set_warm_permission_gate_for_test(&self, gate: Arc<WarmPermissionGate>) {
        *self.permission_gate.lock().unwrap() = Some(gate);
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
                credential: broca_wal::record::AttemptCredential {
                    account_ref: send.account_ref,
                    ..self.template.credential.clone()
                },
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

    async fn finished(
        &self,
        _id: broca_wal::record::AttemptId,
        _attempt: broca_protocol::ModelAttempt,
        _outcome: broca_wal::record::BillingAttemptOutcome,
    ) -> Result<(), ProviderError> {
        Err(warm_journal_error(
            "model settlement sent to a warm journal",
        ))
    }

    async fn warm_finished(
        &self,
        id: broca_wal::record::AttemptId,
        envelope: broca_wal::record::AttemptEnvelope,
        sent: Option<WarmSent>,
        warm_body_sha256: String,
    ) -> Result<(), ProviderError> {
        let compressed_refusal =
            envelope.outcome == broca_wal::record::BillingAttemptOutcome::CompressionRefused;
        let failed = (!warm_reached_the_cache(Some(&envelope))
            && warm_budget_needs_pricing(&envelope))
        .then(|| broca_wal::record::AttemptEnvelope {
            sent: envelope.sent,
            usage: envelope.usage,
            outcome: envelope.outcome,
            cache_write_5m_tokens: envelope.cache_write_5m_tokens,
            cache_write_1h_tokens: envelope.cache_write_1h_tokens,
            ..Default::default()
        });
        let failed = {
            let mut pending = self.pending.lock().unwrap();
            let Some(entry) = pending.get(&id) else {
                return Err(warm_journal_error("warm reservation already settled"));
            };
            let failed = failed.map(|envelope| FailedWarmAttempt {
                reservation: entry.reservation.clone(),
                envelope,
                ttl_ms: entry.ttl_ms,
            });
            let record = self.terminal(id.clone(), entry, envelope, sent, warm_body_sha256);
            self.seal
                .settle_billing_attempt(&self.writer, &id, record)
                .map_err(warm_journal_error)?;
            pending.remove(&id);
            failed
        };
        if let Some(failed) = failed {
            self.failed.lock().unwrap().push(failed);
        }
        // The resend reservation's barrier commits the compression refusal and new permission
        // together. A final outcome has its own barrier because an idle session may write no more.
        if !compressed_refusal {
            self.writer.sync().await.map_err(warm_journal_error)?;
        }
        Ok(())
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
    Provider(ProviderError),
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

/// The latest episode's run id and its effective config: the `RunStarted` value with every later
/// refreeze of that episode applied.
pub fn latest_episode_config(records: &[WalRecord]) -> Option<(RunId, RunConfig)> {
    let start = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::RunStarted { .. }))?;
    effective_config(records, start, records.len())
}

/// The config of the episode starting at `start`, with its refreezes before `end` applied.
fn effective_config(records: &[WalRecord], start: usize, end: usize) -> Option<(RunId, RunConfig)> {
    let WalRecord::RunStarted { run_id, config, .. } = &records[start] else {
        return None;
    };
    let mut config = (**config).clone();
    for record in &records[start..end] {
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
                config.auth_selection = Some(auth_selection.clone());
            }
            _ => {}
        }
    }
    Some((run_id.clone(), config))
}

/// When the session's most recent outcome was an exhausted provider account whose reset is still
/// in the future, that reset time. A later successful step or warm clears it.
pub fn account_wall(records: &[WalRecord], now_ms: u64) -> Option<u64> {
    let latest_error = records.iter().rev().find_map(|record| match record {
        WalRecord::RunFinished { error, .. } | WalRecord::RunPaused { error, .. } => {
            Some(error.as_ref())
        }
        WalRecord::ModelStepFinished { .. } => Some(None),
        WalRecord::CacheWarmed { envelope, .. } if warm_reached_the_cache(envelope.as_deref()) => {
            Some(None)
        }
        _ => None,
    })??;
    (latest_error.provider_code.as_deref() == Some(ACCOUNT_LIMIT_EXHAUSTED))
        .then_some(latest_error.resets_at_ms)
        .flatten()
        .filter(|resets_at_ms| *resets_at_ms > now_ms)
}

/// Rebuild the most recent model request the WAL records, exactly as the run loop assembled it
/// for the provider: the replayed prompt before the step opened, with the steered messages the
/// step took and the tool-repetition guidance the loop adds, and the episode's frozen request
/// settings.
///
/// The caller renders the result and compares it with `request_body_sha256` before sending
/// anything: a prompt-reduction transform, or a WAL whose earlier part was folded away, rebuilds
/// a different request, and only that comparison can tell.
pub fn last_recorded_request(
    records: &[WalRecord],
) -> Result<RecordedRequest, RecordedRequestError> {
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
    if config.render_tags {
        broca_protocol::tags::render_tags_into(
            &mut prompt,
            broca_protocol::tags::TagRendering::for_render_config(&config.frozen_render_config),
        );
    } else {
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
        render_episodes: Arc::new(prior.render_episodes.unwrap_or_default().into()),
    };
    // Compaction may apply after StepStarted and before the attempt. Recover the view
    // through the attempt boundary, not just the prompt boundary used for steers.
    let compaction = crate::replay::replay(&records[..attempt_at])
        .map_err(|error| RecordedRequestError::Unavailable(format!("replay compaction: {error}")))?
        .compaction;
    if let Some(state) = compaction.state() {
        opts = crate::compaction::model_options(&opts, state)
            .map_err(|error| RecordedRequestError::Unavailable(error.to_string()))?;
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

    #[derive(Default)]
    struct WarmSyncGate {
        held: Mutex<bool>,
        release: std::sync::Condvar,
        entered: tokio::sync::Notify,
        records: Mutex<Vec<broca_wal::FramedRecord>>,
    }

    struct WarmTestWal(Arc<WarmSyncGate>);
    impl broca_wal::Wal for WarmTestWal {
        fn append(
            &mut self,
            fence: u64,
            record: &WalRecord,
            _: bool,
        ) -> Result<u64, broca_wal::WalError> {
            let mut records = self.0.records.lock().unwrap();
            let seq = records.len() as u64 + 1;
            records.push(broca_wal::FramedRecord {
                seq,
                fence,
                record: record.clone(),
            });
            Ok(seq)
        }
        fn sync(&self) -> Result<(), broca_wal::WalError> {
            let mut held = self.0.held.lock().unwrap();
            self.0.entered.notify_one();
            while *held {
                held = self.0.release.wait(held).unwrap();
            }
            Ok(())
        }
        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(self.0.records.lock().unwrap().clone())
        }
        fn tail_after_seq(
            &self,
            seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(self.replay()?.into_iter().filter(|r| r.seq > seq).collect())
        }
        fn last_seq(&self) -> u64 {
            self.0.records.lock().unwrap().len() as u64
        }
    }

    fn warm_template() -> broca_wal::record::AttemptReservation {
        serde_json::from_value(serde_json::json!({
            "run_id":"r", "step_id":1, "attempt":0, "send_index":1, "warm":true,
            "reserved_at_ms":100, "provider":"p", "endpoint_host":"example.com", "model_sent":"m",
            "credential":{}, "run":{},
            "session":{"project_root":"/project", "harness":"runner", "session":"s", "session_ref":"ref"},
            "request_body_bytes":10, "compressed":false
        })).unwrap()
    }

    fn warm_physical_send() -> crate::run::PhysicalSend {
        crate::run::PhysicalSend {
            attempt: 0,
            send_index: 1,
            endpoint_host: "example.com".into(),
            model_sent: "m".into(),
            request_body_bytes: 10,
            compressed: false,
            account_ref: None,
            speed_requested: None,
            warm: None,
        }
    }

    #[tokio::test]
    async fn warm_timeout_during_hook_releases() {
        warm_permission_interrupted("timeout").await;
    }

    #[tokio::test]
    async fn warm_cancel_and_seal_during_hook_release() {
        warm_permission_interrupted("cancel").await;
        warm_permission_interrupted("seal").await;
    }

    #[tokio::test]
    async fn warm_already_cancelled_or_sealed_never_reserves() {
        use crate::run::AttemptHook;
        for sealed in [false, true] {
            let gate = Arc::new(WarmSyncGate::default());
            let writer =
                Arc::new(crate::WalHandle::spawn(Box::new(WarmTestWal(gate.clone())), 1).unwrap());
            let seal = crate::ShutdownSeal::new();
            let cancel = tokio_util::sync::CancellationToken::new();
            if sealed {
                seal.seal_and_collect(std::time::Duration::ZERO).await;
            } else {
                cancel.cancel();
            }
            let journal =
                WarmJournal::new(writer.clone(), seal, warm_template(), "base".into(), None);
            assert!(journal
                .before_send(warm_physical_send(), cancel)
                .await
                .is_err());
            writer.sync().await.unwrap();
            assert!(!gate.records.lock().unwrap().iter().any(|f| matches!(
                f.record,
                WalRecord::AttemptReserved { .. }
                    | WalRecord::AttemptReleased { .. }
                    | WalRecord::CacheWarmed { .. }
            )));
            drop(journal);
            Arc::try_unwrap(writer)
                .ok()
                .unwrap()
                .shutdown()
                .await
                .unwrap();
        }
    }

    async fn warm_permission_interrupted(cause: &str) {
        use crate::run::AttemptHook;
        let gate = Arc::new(WarmSyncGate::default());
        let writer =
            Arc::new(crate::WalHandle::spawn(Box::new(WarmTestWal(gate.clone())), 1).unwrap());
        // Consume the writer's initial recovery sync before gating the reservation barrier.
        gate.entered.notified().await;
        *gate.held.lock().unwrap() = true;
        let seal = crate::ShutdownSeal::new();
        let journal = Arc::new(WarmJournal::new(
            writer.clone(),
            seal.clone(),
            warm_template(),
            "aa".into(),
            None,
        ));
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = {
            let journal = journal.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { journal.before_send(warm_physical_send(), cancel).await })
        };
        gate.entered.notified().await;
        match cause {
            "timeout" => {
                tokio::time::pause();
                tokio::time::advance(std::time::Duration::from_secs(31)).await;
            }
            "cancel" => cancel.cancel(),
            "seal" => {
                seal.seal_and_collect(std::time::Duration::ZERO).await;
            }
            _ => unreachable!(),
        }
        let result = task.await.unwrap();
        if cause == "timeout" {
            tokio::time::resume();
        }
        *gate.held.lock().unwrap() = false;
        gate.release.notify_all();
        let error = result.expect_err("no send permission while durability is stalled");
        assert_eq!(
            error.provider_code.as_deref(),
            Some("attempt_durability_failed")
        );
        writer.sync().await.unwrap();
        {
            let records = gate.records.lock().unwrap();
            let ids: Vec<_> = records
                .iter()
                .filter_map(|r| match &r.record {
                    WalRecord::AttemptReserved { attempt_id, .. } => Some(attempt_id),
                    _ => None,
                })
                .collect();
            assert_eq!(ids.len(), 1);
            assert_eq!(records.iter().filter(|r| matches!(&r.record, WalRecord::AttemptReleased { attempt_id, envelope, .. }
            if attempt_id == ids[0] && envelope.sent == broca_wal::record::AttemptSent::No && envelope.usage.is_none())).count(), 1);
            assert!(!records
                .iter()
                .any(|r| matches!(r.record, WalRecord::CacheWarmed { .. })));
        }
        drop(journal);
        Arc::try_unwrap(writer)
            .ok()
            .expect("warm journal released the writer")
            .shutdown()
            .await
            .unwrap();
    }

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

    /// A run start, decoded from the bytes the WAL holds so the test does not depend on every
    /// field of the record.
    fn run_started(ts_ms: Option<u64>) -> WalRecord {
        let ts = ts_ms.map_or(String::new(), |ts_ms| format!(r#""ts_ms":{ts_ms},"#));
        serde_json::from_str(&format!(
            r#"{{"type":"run_started","run_id":"r","session":{{"project_root":"/tmp/p","harness":"h","session":"s"}},{ts}"config":{{"model":{{"provider_module_id":"p","model_id":"m"}},"tools":[],"tool_choice":{{"type":"auto"}},"generation":{{}},"response_format":{{"type":"text"}},"context_epoch":0,"renderer_version":1}},"input":[],"origin":{{"kind":"fresh"}}}}"#
        ))
        .unwrap()
    }

    fn attempt(
        http_status: Option<u16>,
        outcome: &str,
        sent_at_ms: Option<u64>,
        ts_ms: u64,
    ) -> WalRecord {
        let status = http_status.map_or(String::new(), |status| {
            format!(r#""http_status":{status},"#)
        });
        let sent = sent_at_ms.map_or(String::new(), |sent| format!(r#","sent_at_ms":{sent}"#));
        let record: WalRecord = serde_json::from_str(&format!(
            r#"{{"type":"model_attempt_finished","step_id":1,"attempt":0,"request_body_len":1,"request_body_sha256":"aa",{status}"response_body_len":0,"response_body_sha256":"bb","outcome":"{outcome}","ts_ms":{ts_ms}{sent}}}"#
        ))
        .unwrap();
        assert!(matches!(record, WalRecord::ModelAttemptFinished { .. }));
        record
    }

    fn warmed(sent_at_ms: u64) -> WalRecord {
        serde_json::from_str(&format!(
            r#"{{"type":"cache_warmed","run_id":"r","step_id":1,"request_body_sha256":"aa","warm_body_sha256":"bb","usage":{{}},"sent_at_ms":{sent_at_ms}}}"#
        ))
        .unwrap()
    }

    #[test]
    fn failed_warm_not_cache_send() {
        use broca_wal::record::{AttemptEnvelope, AttemptSent, BillingAttemptOutcome};
        for (status, outcome) in [
            (Some(529), BillingAttemptOutcome::HttpError),
            (Some(200), BillingAttemptOutcome::IdleTimeout),
            (Some(200), BillingAttemptOutcome::ChunkError),
            (Some(200), BillingAttemptOutcome::Unknown),
            (None, BillingAttemptOutcome::HeaderTimeout),
        ] {
            let mut failed = warmed(200);
            if let WalRecord::CacheWarmed { envelope, .. } = &mut failed {
                *envelope = Some(Box::new(AttemptEnvelope {
                    sent: AttemptSent::Yes,
                    http_status: status,
                    outcome,
                    ..Default::default()
                }));
            }
            let records = [attempt(Some(200), "complete", Some(100), 110), failed];
            assert_eq!(last_cache_send_at_ms(&records), Some(100), "{outcome:?}");
            let clock = CacheClock::new();
            clock.seed_if_unknown(&records, Some(300_000));
            assert_eq!(clock.read().unwrap().last_sent_at_ms, 100);
        }
        let mut success = warmed(200);
        if let WalRecord::CacheWarmed { envelope, .. } = &mut success {
            *envelope = Some(Box::new(AttemptEnvelope {
                sent: AttemptSent::Yes,
                http_status: Some(200),
                outcome: BillingAttemptOutcome::Complete,
                ..Default::default()
            }));
        }
        assert_eq!(last_cache_send_at_ms(&[success]), Some(200));
    }

    #[test]
    fn a_request_counts_from_its_send_not_from_the_end_of_its_response() {
        // Sent at 5 s, and its response streamed for a minute.
        let records = [
            run_started(Some(1_000)),
            attempt(Some(200), "complete", Some(5_000), 65_000),
        ];
        assert_eq!(last_cache_send_at_ms(&records), Some(5_000));
    }

    #[test]
    fn a_request_recorded_without_its_send_time_counts_from_the_latest_time_before_it() {
        // Two steps recorded before attempts carried their send time: the second step was sent
        // after the first one's response ended, at 20 s, and that is all the WAL proves.
        let records = [
            run_started(Some(1_000)),
            attempt(Some(200), "complete", None, 20_000),
            attempt(Some(200), "complete", None, 90_000),
        ];
        assert_eq!(last_cache_send_at_ms(&records), Some(20_000));
        // The same step with its send time recorded counts from that instead.
        let records = [
            run_started(Some(1_000)),
            attempt(Some(200), "complete", None, 20_000),
            attempt(Some(200), "complete", Some(30_000), 90_000),
        ];
        assert_eq!(last_cache_send_at_ms(&records), Some(30_000));
        // With nothing dated before it, an old record proves no send time at all.
        let records = [
            run_started(None),
            attempt(Some(200), "complete", None, 20_000),
        ];
        assert_eq!(last_cache_send_at_ms(&records), None);
    }

    #[test]
    fn only_requests_the_provider_accepted_move_the_clock() {
        let accepted = [
            run_started(Some(1_000)),
            attempt(Some(200), "complete", Some(2_000), 3_000),
        ];
        for (status, outcome) in [
            (None, "connect_failed"),
            (None, "header_timeout"),
            (None, "no_status"),
            (None, "cancelled"),
            (Some(400), "http_error"),
            (Some(429), "http_error"),
            (Some(529), "http_error"),
            (Some(200), "provider_error"),
            (Some(200), "an_outcome_from_a_later_build"),
        ] {
            let mut records = accepted.to_vec();
            records.push(attempt(status, outcome, Some(9_000), 9_500));
            assert_eq!(
                last_cache_send_at_ms(&records),
                Some(2_000),
                "{status:?} {outcome} must not move the clock"
            );
        }
        for outcome in [
            "chunk_error",
            "idle_timeout",
            "cancelled",
            "sealed_in_flight",
        ] {
            let mut records = accepted.to_vec();
            records.push(attempt(Some(200), outcome, Some(9_000), 9_500));
            assert_eq!(last_cache_send_at_ms(&records), Some(9_000), "{outcome}");
        }
        // A refusal is still evidence that time had passed before a later request.
        let records = [
            run_started(Some(1_000)),
            attempt(None, "connect_failed", None, 7_000),
            attempt(Some(200), "complete", None, 60_000),
        ];
        assert_eq!(last_cache_send_at_ms(&records), Some(7_000));
    }

    #[test]
    fn the_later_of_the_last_request_and_the_last_warm_counts() {
        let records = [
            run_started(Some(1_000)),
            attempt(Some(200), "complete", Some(2_000), 3_000),
            warmed(50_000),
        ];
        assert_eq!(last_cache_send_at_ms(&records), Some(50_000));
        let records = [
            warmed(1_500),
            run_started(Some(1_000)),
            attempt(Some(200), "complete", Some(60_000), 61_000),
        ];
        assert_eq!(last_cache_send_at_ms(&records), Some(60_000));
        assert_eq!(
            last_cache_send_at_ms(&[run_started(Some(1_000)), warmed(4_000)]),
            Some(4_000)
        );
    }

    #[test]
    fn an_empty_clock_is_seeded_from_the_wal_and_a_noted_send_wins() {
        let records = [
            run_started(Some(1_000)),
            attempt(Some(200), "complete", Some(5_000), 65_000),
        ];
        let clock = CacheClock::new();
        clock.seed_if_unknown(&records, Some(3_600_000));
        assert_eq!(
            clock.read(),
            Some(CacheClockReading {
                last_sent_at_ms: 5_000,
                ttl_ms: Some(3_600_000),
                prefix_tokens: None
            })
        );
        // A later send moves the seeded clock on, and the WAL no longer seeds a clock that
        // holds a reading.
        clock.note_sent(70_000, Some(3_600_000));
        clock.seed_if_unknown(&records, Some(3_600_000));
        assert_eq!(clock.read().unwrap().last_sent_at_ms, 70_000);
        let noted = CacheClock::new();
        noted.note_sent(4_000, Some(300_000));
        noted.seed_if_unknown(&records, Some(3_600_000));
        assert_eq!(
            noted.read(),
            Some(CacheClockReading {
                last_sent_at_ms: 4_000,
                ttl_ms: Some(300_000),
                prefix_tokens: None
            })
        );

        // Without a send the WAL can date, the clock stays unknown, and the refusal says so.
        let unknown = CacheClock::new();
        let records = [
            run_started(Some(1_000)),
            attempt(None, "connect_failed", Some(5_000), 6_000),
        ];
        unknown.seed_if_unknown(&records, Some(3_600_000));
        assert_eq!(unknown.read(), None);
        assert_eq!(
            check_warm_expiry(None, 10_000),
            Err(
                "no request of this session has been sent since this broca started, so the \
                 cache's expiry is unknown"
                    .into()
            )
        );
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

    #[test]
    fn prefix_tokens_tracks_one_send_and_ignores_late_usage() {
        let clock = CacheClock::new();
        clock.note_sent(100, Some(300_000));
        clock.note_usage(
            100,
            Usage {
                input_tokens: Some(5),
                ..Usage::default()
            },
        );
        assert_eq!(clock.read().unwrap().prefix_tokens, Some(5));
        clock.note_sent(200, Some(300_000));
        assert_eq!(clock.read().unwrap().prefix_tokens, None);
        clock.note_usage(
            100,
            Usage {
                input_tokens: Some(99),
                ..Usage::default()
            },
        );
        assert_eq!(clock.read().unwrap().prefix_tokens, None);
        clock.note_usage(
            200,
            Usage {
                cached_input_tokens: Some(10),
                ..Usage::default()
            },
        );
        assert_eq!(clock.read().unwrap().prefix_tokens, Some(10));
    }

    #[test]
    fn prefix_tokens_is_rebuilt_from_the_latest_step_or_warm_after_restart() {
        let step: WalRecord = serde_json::from_str(r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[]},"usage":{"input_tokens":3,"cached_input_tokens":40,"cache_write_tokens":7},"retries_used":0}"#).unwrap();
        let mut records = vec![
            run_started(Some(1_000)),
            attempt(Some(200), "complete", Some(2_000), 3_000),
            step,
        ];
        let clock = CacheClock::new();
        clock.seed_if_unknown(&records, Some(300_000));
        assert_eq!(clock.read().unwrap().prefix_tokens, Some(50));
        records.push(serde_json::from_str(r#"{"type":"cache_warmed","run_id":"r","step_id":1,"request_body_sha256":"aa","warm_body_sha256":"bb","usage":{"cached_input_tokens":80},"sent_at_ms":5000}"#).unwrap());
        let clock = CacheClock::new();
        clock.seed_if_unknown(&records, Some(300_000));
        assert_eq!(clock.read().unwrap().prefix_tokens, Some(80));
        records.push(warmed(6_000));
        let clock = CacheClock::new();
        clock.seed_if_unknown(&records, Some(300_000));
        assert_eq!(clock.read().unwrap().prefix_tokens, None);
    }

    /// The request a warm re-sends goes to the model its step went to: a committed step's
    /// recorded route, the fallback model for the re-send that follows a refusal, the route the
    /// schedule gave a step still in flight, and otherwise the episode's model.
    #[test]
    fn the_recorded_request_follows_the_refusal_fallback_route() {
        use broca_protocol::request::ModelHandle;
        use serde_json::json;
        let source = ModelHandle::new("p", "m");
        let fallback = ModelHandle::new("p", "m-fallback");
        let schedule = |remaining: u32| {
            json!({"source":source,"fallback":fallback,
                "phase":{"kind":"fallback","remaining":remaining},"re_refusals":0})
        };
        let record =
            |value: serde_json::Value| -> WalRecord { serde_json::from_value(value).unwrap() };
        let started = WalRecord::StepStarted { step_id: StepId(1) };
        let sent = || attempt(Some(200), "complete", Some(2_000), 3_000);
        let step = |extra: serde_json::Value| {
            let mut value = json!({"type":"model_step_finished","step_id":1,
                "assistant_message":{"message_id":"m","content":[{"type":"text","text":"ok"}]},
                "usage":{},"retries_used":0});
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            record(value)
        };
        let refusal = record(json!({"type":"model_step_refused","step_id":1,"usage":{},
            "discarded_blocks":0,"schedule":schedule(10)}));
        let route_of = |records: &[WalRecord]| {
            let recorded = last_recorded_request(records).expect("the request rebuilds");
            (
                recorded.route.map(|route| (route.model, route.reason)),
                recorded.opts.target_model,
            )
        };
        use broca_wal::StepRouteReason::{Refusal, Schedule};

        // A step to the episode's model.
        let plain = [run_started(None), started.clone(), sent(), step(json!({}))];
        assert_eq!(route_of(&plain), (None, Some(source.clone())));

        // A committed step routed by the schedule.
        let committed = [
            run_started(None),
            started.clone(),
            sent(),
            step(json!({"route":{"model":fallback,"reason":"schedule"},
                "refusal_fallback":schedule(9)})),
        ];
        assert_eq!(
            route_of(&committed),
            (Some((fallback.clone(), Schedule)), Some(fallback.clone()))
        );

        // The re-send after a refusal, before its answer is durable; and the refused source
        // attempt itself when the re-send has not been sent yet.
        let resent = [
            run_started(None),
            started.clone(),
            sent(),
            refusal.clone(),
            sent(),
        ];
        assert_eq!(
            route_of(&resent),
            (Some((fallback.clone(), Refusal)), Some(fallback.clone()))
        );
        assert_eq!(route_of(&resent[..4]), (None, Some(source.clone())));

        // A step of the next episode still in flight while the schedule routes to the fallback.
        let mut next = run_started(None);
        if let WalRecord::RunStarted { run_id, .. } = &mut next {
            *run_id = RunId("r2".into());
        }
        let in_flight = [
            run_started(None),
            started.clone(),
            sent(),
            refusal,
            sent(),
            step(json!({"route":{"model":fallback,"reason":"refusal"},
                "refusal_fallback":schedule(9)})),
            record(json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0})),
            next,
            started,
            sent(),
        ];
        assert_eq!(
            route_of(&in_flight),
            (Some((fallback.clone(), Schedule)), Some(fallback))
        );
    }

    #[test]
    fn the_clock_never_moves_backwards() {
        let clock = CacheClock::new();
        clock.note_sent(2_000, Some(300_000));
        clock.note_sent(1_000, Some(300_000));
        assert_eq!(clock.read().unwrap().last_sent_at_ms, 2_000);
        clock.note_sent(3_000, Some(3_600_000));
        assert_eq!(
            clock.read(),
            Some(CacheClockReading {
                last_sent_at_ms: 3_000,
                ttl_ms: Some(3_600_000),
                prefix_tokens: None
            })
        );
    }

    #[test]
    fn a_source_warm_does_not_extend_the_active_fallback_cache_clock() {
        let mut started = run_started(Some(1));
        if let WalRecord::RunStarted { config, .. } = &mut started {
            config.frozen_render_config = serde_json::json!({"refusal_fallback":{}});
        }
        let mut fallback = attempt(Some(200), "complete", Some(200), 300);
        if let WalRecord::ModelAttemptFinished {
            request_body_sha256,
            ..
        } = &mut fallback
        {
            *request_body_sha256 = "fallback-digest".into();
        }
        let mut records = vec![
            started,
            attempt(Some(200), "complete", Some(100), 150),
            fallback,
            serde_json::from_value(serde_json::json!({
                "type":"model_step_finished", "step_id":1,
                "assistant_message":{"message_id":"fallback-answer","content":[]},
                "usage":{"cached_input_tokens":50}
            }))
            .unwrap(),
            warmed(400),
        ];
        assert_eq!(
            last_cache_send_at_ms(&records),
            Some(400),
            "unmarked historical warms retain the legacy clock"
        );
        if let Some(WalRecord::CacheWarmed { trigger, .. }) = records.last_mut() {
            *trigger = Some(SOURCE_KEEP_WARM_TRIGGER.into());
        }
        assert_eq!(last_cache_send_at_ms(&records), Some(200));
        let clock = CacheClock::default();
        clock.seed_if_unknown(&records, Some(300_000));
        assert_eq!(clock.read().unwrap().last_sent_at_ms, 200);
        assert_eq!(clock.read().unwrap().prefix_tokens, Some(50));
    }
}
