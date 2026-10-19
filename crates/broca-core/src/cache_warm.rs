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
                sent_at_ms,
                usage: reported,
                ..
            } if *sent_at_ms == last_sent_at_ms => {
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
/// started sending: the later of its last such model request and its last warm. `None` when the
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
/// A warm counts from its own `sent_at_ms`: a `CacheWarmed` record is written only for a warm the
/// provider answered.
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
            WalRecord::CacheWarmed { sent_at_ms, .. } => Some(*sent_at_ms),
            _ => None,
        };
        latest = latest.max(sent_at_ms);
        bound.observe(record);
    }
    latest
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
        WalRecord::ModelStepFinished { .. } | WalRecord::CacheWarmed { .. } => Some(None),
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
    let mut prompt = prior.prompt;
    // Steers written before the step opened are rendered by that step; replay of the records
    // before `step_started` holds them back as pending.
    prompt.extend(prior.pending_steers);
    broca_protocol::content::stamp_ordinals_from(&mut prompt, 0);
    // A run frozen with `render_tags` sent its tagged user and tool-result text with the tag
    // prefix, after the guidance above; the rebuilt request must carry the same bytes.
    if config.render_tags {
        broca_protocol::tags::render_tags_into(
            &mut prompt,
            broca_protocol::tags::TagRendering::for_render_config(&config.frozen_render_config),
        );
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
            run_id: run_id.0.clone(),
            step_index: step_id.0,
        }),
        target_model: Some(config.model.clone()),
        marker_order_version: prior.marker_order_version,
        render_episodes: Arc::new(prior.render_episodes.unwrap_or_default().into()),
    };
    // Compaction may apply after StepStarted and before the attempt. Recover the view
    // through the attempt boundary, not just the prompt boundary used for steers.
    let compaction = crate::replay::replay(&records[..attempt_at])
        .map_err(|error| RecordedRequestError::Unavailable(format!("replay compaction: {error}")))?
        .compaction;
    if let Some(state) = compaction.state() {
        opts = crate::compaction::model_options(&opts, state);
    }
    Ok(RecordedRequest {
        run_id,
        step_id: *step_id,
        config,
        opts,
        request_body_sha256: request_body_sha256.clone(),
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
}
