//! Per-session, in-memory call lane. Notices are serialized and coalesced, not
//! written to the log or published on a bus.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use cortexkit_role_compaction::{
    notice::{Conditions, Event, NoticeAnswer, NoticeRequest},
    read::{Cursor, Entry},
    ride::{RideCause, RideRequest},
    setup::{SetupAnswer, SetupRequest},
    view::AppliedView,
    wire::Refusal,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{received::ReceivedView, views};

pub const CALL_DEADLINE: Duration = Duration::from_secs(2);
pub const COLD_RIDE_DEADLINE: Duration = Duration::from_secs(30);
/// How long a run with no applied view keeps re-sending a Setup that got no answer before it
/// ends `compaction_unavailable`. When the daemon restarts every module, broca resumes its runs
/// while the provider may still be starting; it answers again within seconds. A refused Setup
/// is never re-sent within the run.
pub const UNANSWERED_SETUP_WAIT: Duration = Duration::from_secs(120);
/// The pause before each re-send of an unanswered Setup doubles from this, up to
/// [`UNANSWERED_SETUP_MAX_PAUSE`].
const UNANSWERED_SETUP_FIRST_PAUSE: Duration = Duration::from_secs(1);
const UNANSWERED_SETUP_MAX_PAUSE: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub enum CallError {
    Refused(Refusal),
    Unavailable,
}

/// Transport identity is supplied by the session-bound adapter, never in bodies.
#[async_trait]
pub trait Plane: Send + Sync {
    fn view_refused(&self, _view: &RawValue, _code: &str) {}
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<Box<RawValue>, CallError>;
    /// Whether the provider's catalog entry lists `compaction-entry-speaks-for/v1`, asked when
    /// the runner sends Setup. A plane that can't read the catalog answers false, so the
    /// provider is sent no attribution it may not understand.
    async fn provides_entry_speaks_for(&self) -> bool {
        false
    }
    /// Whether the provider's catalog entry lists `compaction-signed-floor/v1`. A plane that
    /// can't read the catalog answers false, so the provider is sent no floor it may not
    /// understand.
    async fn provides_signed_floor(&self) -> bool {
        false
    }
    /// Whether the provider can receive a separate count of persistent runner notes.
    async fn provides_notes_tokens(&self) -> bool {
        false
    }
    /// Whether the provider can order runner state by the session's log position.
    async fn provides_statement_seq(&self) -> bool {
        false
    }
    /// Declarations read together when the runner sends Setup. A catalog-backed plane answers
    /// them from one read.
    async fn declarations(&self) -> Declarations {
        Declarations {
            entry_speaks_for: AttributionDeclaration::known(self.provides_entry_speaks_for().await),
            signed_floor: self.provides_signed_floor().await,
            notes_tokens: self.provides_notes_tokens().await,
            statement_seq: self.provides_statement_seq().await,
        }
    }
}

/// Whether a catalog read established that entries may carry speaker attribution.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AttributionDeclaration {
    Declared,
    NotDeclared,
    #[default]
    Unknown,
}

impl AttributionDeclaration {
    pub fn known(declared: bool) -> Self {
        if declared {
            Self::Declared
        } else {
            Self::NotDeclared
        }
    }

    pub fn value(self) -> Option<bool> {
        match self {
            Self::Declared => Some(true),
            Self::NotDeclared => Some(false),
            Self::Unknown => None,
        }
    }
}

/// What the provider's catalog entry declares that changes what the runner sends it, as the
/// latest Setup read it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Declarations {
    /// `compaction-entry-speaks-for/v1`: entries carry stored speaker marks.
    pub entry_speaks_for: AttributionDeclaration,
    /// `compaction-signed-floor/v1`: Setup and conditions carry the signed floor.
    pub signed_floor: bool,
    /// `compaction-notes-tokens/v1`: notice conditions separate persistent note counts.
    pub notes_tokens: bool,
    /// `compaction-statement-seq/v1`: Setup and conditions carry the state stamp.
    pub statement_seq: bool,
}

impl Declarations {
    fn of(provides: &[String]) -> Self {
        Self {
            entry_speaks_for: AttributionDeclaration::known(
                cortexkit_role_compaction::provides_entry_speaks_for(provides),
            ),
            signed_floor: cortexkit_role_compaction::provides_signed_floor(provides),
            notes_tokens: cortexkit_role_compaction::provides_notes_tokens(provides),
            statement_seq: cortexkit_role_compaction::provides_statement_seq(provides),
        }
    }
}

pub struct BoundPlane<'a> {
    pub plane: Option<&'a dyn crate::CompactionPlane>,
    pub provider: &'a str,
    pub bind: &'a broca_protocol::BindIdentity,
    pub scope: Option<&'a broca_protocol::SessionScope>,
}

#[async_trait]
impl Plane for BoundPlane<'_> {
    fn view_refused(&self, view: &RawValue, code: &str) {
        super::diagnostics::Publication::from_raw(view).warn(self.bind, code);
    }
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<Box<RawValue>, CallError> {
        let plane = self.plane.ok_or(CallError::Unavailable)?;
        let deadline = if method == "compaction.ride" {
            params
                .get("deadline_ms")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(CALL_DEADLINE.as_millis() as u64)
        } else {
            CALL_DEADLINE.as_millis() as u64
        };
        plane
            .call_v2(
                self.provider,
                self.bind,
                self.scope,
                method,
                params,
                deadline,
            )
            .await
    }

    async fn provides_entry_speaks_for(&self) -> bool {
        self.declarations().await.entry_speaks_for == AttributionDeclaration::Declared
    }

    async fn provides_signed_floor(&self) -> bool {
        self.declarations().await.signed_floor
    }

    async fn provides_notes_tokens(&self) -> bool {
        self.declarations().await.notes_tokens
    }

    async fn provides_statement_seq(&self) -> bool {
        self.declarations().await.statement_seq
    }

    async fn declarations(&self) -> Declarations {
        let Some(plane) = self.plane else {
            return Declarations::default();
        };
        match plane.provider_capabilities(self.provider).await {
            crate::ProviderCapabilities::Declared(provides) => Declarations::of(&provides),
            crate::ProviderCapabilities::NotInCatalog => Declarations::of(&[]),
            crate::ProviderCapabilities::Unknown => Declarations::default(),
        }
    }
}

#[derive(Default)]
#[cfg_attr(debug_assertions, derive(Clone, PartialEq))]
struct Pending {
    token_family: Option<String>,
    entries: BTreeMap<u64, (Entry, u64)>,
    entry_bytes: u64,
    entries_omitted: bool,
    changed: bool,
    observed_through: u64,
    // Only the latest assistant can still gain tool results or deliveries.
    // Older native entries are immutable, so no transcript-sized digest ledger is needed.
    assistant: Option<(u64, String)>,
    known_marker: Option<u64>,
    events: Vec<Event>,
    newest: Option<Cursor>,
}

impl Pending {
    /// Drops the queued entries a ready Setup naming `newest` covers. The provider reads the
    /// window for itself after Setup (compaction contract N6), and each entry it reads is at
    /// least as new as the one queued here: every caller observes the entries a Setup is built
    /// from before sending it, so nothing queued is newer. Without this, a fresh runtime (an actor
    /// reopened, a restarted process)
    /// would send its whole first observation, up to the notice budget, in the first notice.
    ///
    /// Nothing else changes. `assistant` keeps the digest last observed, which is the one Setup
    /// was built from, so a tool result or delivery reaching that entry after Setup changes its
    /// digest and goes in the next notice. An entry not yet observed when Setup was sent is still
    /// sent when it is: that only repeats an entry the provider has, which costs bytes, not state.
    fn seen_at_setup(&mut self, newest: Option<&Cursor>) {
        let Some(newest) = newest else {
            return;
        };
        self.entries.retain(|ordinal, _| *ordinal > newest.ordinal);
        self.entry_bytes = self.entries.values().map(|(_, bytes)| *bytes).sum();
        // Entries left out for the budget are all covered when none past `newest` was observed.
        if self.entries_omitted && self.observed_through <= newest.ordinal {
            self.entries_omitted = false;
        }
        self.changed = !self.entries.is_empty() || self.entries_omitted;
    }
}

struct Lane {
    seq: u64,
    ready: bool,
    setup_due: bool,
    /// Whether the Setup the provider accepted listed `cache_expired` in its `detects`. A ride
    /// claims the cause only then, so the provider never hears a cause it was told broca can't
    /// detect.
    detects_cache_expired: bool,
    /// Whether the provider's catalog entry declared `compaction-signed-floor/v1` when the last
    /// Setup was sent. Setup, notices and rides carry the session's signed floor only then
    /// (compaction contract W3.5); the declaration holds until the next Setup.
    signed_floor_declared: bool,
    notes_tokens_declared: bool,
    statement_seq_declared: bool,
    /// The epoch the latest Setup carried, which every later notice and ride repeats beside its
    /// stamp (compaction contract W3.5). It is minted fresh at each Setup and kept only in
    /// memory: a restarted process always sends a new Setup first. `None` when the provider
    /// didn't declare `compaction-statement-seq/v1`, or no epoch could be minted; the lane then
    /// sends neither the epoch nor the stamp, because a stamp alone is dropped by the provider.
    statement_epoch: Option<String>,
    /// The latest Setup got no answer (no route to the provider, no reply in time, a reply that
    /// didn't decode), as opposed to a refusal or an answer other than `ready`. Only such a
    /// Setup is worth sending again within the run: the provider may be restarting.
    setup_unanswered: bool,
}

impl Default for Lane {
    fn default() -> Self {
        Self {
            seq: 0,
            ready: false,
            setup_unanswered: false,
            setup_due: true,
            detects_cache_expired: false,
            signed_floor_declared: false,
            notes_tokens_declared: false,
            statement_seq_declared: false,
            statement_epoch: None,
        }
    }
}

impl Lane {
    /// `conditions` as this lane's provider may receive them, omitting optional state whose
    /// capability the provider did not declare at Setup.
    fn conditions(&self, conditions: &Conditions) -> Conditions {
        let mut conditions = conditions.clone();
        if !self.signed_floor_declared {
            conditions.signed_floor = None;
        }
        if !self.notes_tokens_declared {
            conditions.notes_tokens = None;
        }
        match &self.statement_epoch {
            Some(epoch) if conditions.statement_seq.is_some() => {
                conditions.statement_epoch = Some(epoch.clone());
            }
            _ => {
                conditions.statement_epoch = None;
                conditions.statement_seq = None;
            }
        }
        conditions
    }
}

/// A new Setup epoch: 32 lowercase hex digits from the operating system's random source, which
/// fits the contract's 1 to 64 visible ASCII characters. `None` if the source fails.
fn mint_statement_epoch() -> Option<String> {
    use std::fmt::Write as _;
    let mut bytes = [0u8; 16];
    if let Err(error) = getrandom::fill(&mut bytes) {
        tracing::warn!(
            %error,
            "no compaction statement epoch could be minted; this Setup's statements carry no stamp"
        );
        return None;
    }
    Some(
        bytes
            .iter()
            .fold(String::with_capacity(32), |mut hex, byte| {
                let _ = write!(hex, "{byte:02x}");
                hex
            }),
    )
}

/// A single actor owns this object across runs. The lane lock spans each call;
/// producers only take `pending`, so a slow notice cannot accumulate a queue.
#[derive(Default)]
pub struct Runtime {
    pub tokens: Mutex<super::tokens::Counts>,
    pending: Mutex<Pending>,
    lane: tokio::sync::Mutex<Lane>,
    /// Whether the provider declared `compaction-entry-speaks-for/v1` in its catalog entry when
    /// the latest Setup was sent, kept until the next Setup: one of the `ATTRIBUTION_` values.
    /// Kept apart from the lane, so a `session.entries` read the provider makes while a notice
    /// holds the lane never waits for that notice's answer.
    entry_speaks_for: AtomicU8,
}

/// No Setup was sent in this runtime's life, and no read has asked the catalog since.
const ATTRIBUTION_UNKNOWN: u8 = 0;
const ATTRIBUTION_UNDECLARED: u8 = 1;
const ATTRIBUTION_DECLARED: u8 = 2;

fn attribution(declared: bool) -> u8 {
    if declared {
        ATTRIBUTION_DECLARED
    } else {
        ATTRIBUTION_UNDECLARED
    }
}

impl Runtime {
    /// Observe durable entries. Completion metadata alone changes no digest and
    /// does not emit a notice, whereas final tool results and deliveries do.
    ///
    /// `entries` come in ordinal order, and only two kinds can change anything here: those past
    /// the newest ordinal observed, and the newest assistant entry with parts, which can still
    /// gain results or deliveries. So once an assistant entry has been observed, only the entries
    /// from the lower of those two ordinals on are looked at; until then, all of them.
    pub fn observe(&self, entries: &[Entry]) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let from = match &pending.assistant {
            Some((assistant, _)) => {
                let first = pending.observed_through.saturating_add(1).min(*assistant);
                entries.partition_point(|entry| entry.ordinal < first)
            }
            None => 0,
        };
        let mut counts = self
            .tokens
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        refresh_pending_counts(&mut pending, entries, &mut counts);
        #[cfg(debug_assertions)]
        let whole = {
            let mut whole = pending.clone();
            observe_from(&mut whole, entries, 0, &mut counts);
            whole
        };
        observe_from(&mut pending, entries, from, &mut counts);
        #[cfg(test)]
        OBSERVED.with(|count| count.set(count.get() + entries.len().saturating_sub(from)));
        #[cfg(debug_assertions)]
        assert!(
            *pending == whole,
            "observing the entries from position {from} differs from observing them all"
        );
    }

    pub fn event(&self, event: Event) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Event::MarkerWritten { version } = &event {
            pending.known_marker = Some(*version);
        }
        if !pending.events.contains(&event) {
            pending.events.push(event);
        }
    }

    pub fn restore_marker(&self, version: Option<u64>) {
        let Some(version) = version else {
            return;
        };
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if pending.known_marker != Some(version) {
            pending.known_marker = Some(version);
            pending.events.push(Event::MarkerWritten { version });
        }
    }

    /// Called when serving starts, after a runner restart, before a send after
    /// failed Setup, and when the provider has lost the session. A prior run's
    /// successful Setup does not suppress these calls. If this returns false
    /// with no applied view, the caller ends the run with compaction_unavailable
    /// before any model I/O.
    pub async fn setup(&self, plane: &dyn Plane, request: &SetupRequest) -> bool {
        let mut lane = self.lane.lock().await;
        self.setup_on(&mut lane, plane, request).await
    }

    /// Sends `request` on `lane` and, once the provider is ready, drops the queued entries that
    /// Setup covers, so the first notice after it carries only what changed since (compaction
    /// contract N6).
    async fn setup_on(&self, lane: &mut Lane, plane: &dyn Plane, request: &SetupRequest) -> bool {
        let ready = setup(lane, plane, request, &self.entry_speaks_for).await;
        if ready {
            self.pending
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .seen_at_setup(request.newest.as_ref());
        }
        ready
    }

    /// Whether entries handed to the provider may carry `speaks_for`: its catalog entry declared
    /// `compaction-entry-speaks-for/v1` when the session's latest Setup was sent. `None` when this
    /// runtime has sent no Setup and learned nothing since (see [`Self::learn_entry_speaks_for`]).
    pub fn entry_speaks_for(&self) -> Option<bool> {
        match self.entry_speaks_for.load(Ordering::Acquire) {
            ATTRIBUTION_DECLARED => Some(true),
            ATTRIBUTION_UNDECLARED => Some(false),
            _ => None,
        }
    }

    /// Keeps a declaration read from the provider's catalog outside a Setup, for a session whose
    /// provider was set up before this runtime existed (an actor reopened after closing idle, or
    /// a restarted daemon). A provider can't withdraw the declaration without restarting, and a
    /// restarted provider is sent Setup again, so the catalog answers what the latest Setup found.
    /// A Setup sent meanwhile wins: this only fills a runtime that still knows nothing.
    pub fn learn_entry_speaks_for(&self, declared: bool) {
        let _ = self.entry_speaks_for.compare_exchange(
            ATTRIBUTION_UNKNOWN,
            attribution(declared),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// A failed Setup without an applied view is retried before the next send,
    /// not repeatedly within the turn that must already end as unavailable.
    pub async fn begin_send(&self, has_applied_view: bool) {
        let mut lane = self.lane.lock().await;
        if !lane.ready && !has_applied_view {
            lane.setup_due = true;
        }
    }

    /// Whether the provider's accepted Setup listed `cache_expired` as a cause broca detects.
    pub async fn detects_cache_expired(&self) -> bool {
        let lane = self.lane.lock().await;
        lane.ready && lane.detects_cache_expired
    }

    pub async fn ensure_setup(&self, plane: &dyn Plane, request: &SetupRequest) -> bool {
        let mut lane = self.lane.lock().await;
        if !lane.ready && lane.setup_due {
            self.setup_on(&mut lane, plane, request).await
        } else {
            lane.ready
        }
    }

    /// For a run with no applied view, whose Setup just failed: while the failure was an
    /// unanswered Setup, sends it again after a pause that doubles from 1 s to 10 s, for at most
    /// [`UNANSWERED_SETUP_WAIT`], and answers whether one was accepted. A refused Setup, or any
    /// answer other than `ready`, ends the wait at once (compaction contract W1.3a): only the
    /// provider being unreachable, such as while it restarts, is waited out. The lane stays held
    /// throughout, so the session's notices wait behind the Setup as they would behind one call.
    pub async fn wait_out_unanswered_setup(
        &self,
        plane: &dyn Plane,
        request: &SetupRequest,
    ) -> bool {
        let mut lane = self.lane.lock().await;
        let until = tokio::time::Instant::now() + UNANSWERED_SETUP_WAIT;
        let mut pause = UNANSWERED_SETUP_FIRST_PAUSE;
        let mut attempts = 0u32;
        while !lane.ready && lane.setup_unanswered {
            let now = tokio::time::Instant::now();
            if now >= until {
                tracing::warn!(
                    target: "broca.compaction",
                    attempts,
                    "compaction provider still gave no Setup answer after {} s; the run ends compaction_unavailable",
                    UNANSWERED_SETUP_WAIT.as_secs()
                );
                return false;
            }
            tokio::time::sleep(pause.min(until - now)).await;
            pause = (pause * 2).min(UNANSWERED_SETUP_MAX_PAUSE);
            attempts += 1;
            if self.setup_on(&mut lane, plane, request).await {
                tracing::info!(
                    target: "broca.compaction",
                    attempts,
                    "compaction provider accepted Setup after it had given no answer"
                );
            }
        }
        lane.ready
    }

    /// Acquiring the lane first waits for the in-flight notice. Then every
    /// pending batch is sent before the ride may use this same lane (N3).
    pub async fn flush(
        &self,
        plane: &dyn Plane,
        setup_request: &SetupRequest,
        conditions: &Conditions,
        applied: Option<&AppliedView>,
    ) {
        let mut lane = self.lane.lock().await;
        self.flush_locked(&mut lane, plane, setup_request, conditions, applied)
            .await;
    }

    async fn flush_locked(
        &self,
        lane: &mut Lane,
        plane: &dyn Plane,
        setup_request: &SetupRequest,
        conditions: &Conditions,
        applied: Option<&AppliedView>,
    ) {
        let mut restarted = false;
        loop {
            if !lane.ready && (!lane.setup_due || !self.setup_on(lane, plane, setup_request).await)
            {
                return;
            }
            let batch = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if !pending.changed && pending.events.is_empty() {
                    return;
                }
                refresh_pending_counts(
                    &mut pending,
                    &[],
                    &mut self
                        .tokens
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()),
                );
                lane.seq += 1;
                let mut notice = NoticeRequest::new(lane.seq, lane.conditions(conditions));
                notice.newest = pending.newest.clone();
                notice.applied_view = applied.cloned();
                notice.entries = std::mem::take(&mut pending.entries)
                    .into_values()
                    .map(|(entry, _)| entry)
                    .collect();
                super::entries::gate_speaks_for(
                    &mut notice.entries,
                    self.entry_speaks_for().unwrap_or(false),
                );
                pending.entry_bytes = 0;
                pending.entries_omitted = false;
                pending.changed = false;
                notice.events = std::mem::take(&mut pending.events);
                if serde_json::to_vec(&notice.entries)
                    .expect("entries serialize")
                    .len()
                    > 1_048_576
                {
                    // Counts are expendable before entries; surface counts remain useful even
                    // when the provider has to read the omitted transcript entries separately.
                    super::tokens::strip_counts(&mut notice.entries);
                    if serde_json::to_vec(&notice.entries)
                        .expect("entries serialize")
                        .len()
                        > 1_048_576
                    {
                        notice.entries.clear();
                    }
                }
                self.tokens
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .notice(&mut notice, lane.notes_tokens_declared);
                notice
            };
            let answer: Result<NoticeAnswer, _> =
                call(plane, "compaction.notice", &batch, CALL_DEADLINE).await;
            match answer {
                Ok(NoticeAnswer::Received) => {}
                Ok(_) => {
                    tracing::debug!(
                        method = "compaction.notice",
                        "compaction provider requires Setup before further notices"
                    );
                    lane.ready = false;
                    lane.setup_due = true;
                    if restarted {
                        return;
                    }
                    restarted = true;
                }
                Err(_) => return,
            }
        }
    }

    /// A ride cannot overtake either an in-flight notice or changes accumulated
    /// while it was unanswered. Unknown and late ride answers mean `nothing`.
    pub async fn ride(
        &self,
        plane: &dyn Plane,
        setup_request: &SetupRequest,
        request: &RideRequest,
    ) -> Option<ReceivedView> {
        let mut lane = self.lane.lock().await;
        self.flush_locked(
            &mut lane,
            plane,
            setup_request,
            &request.conditions,
            request.applied_view.as_ref(),
        )
        .await;
        #[derive(Deserialize)]
        struct RawRideAnswer {
            answer: String,
            #[serde(default)]
            view: Option<Box<RawValue>>,
        }
        let mut request = request.clone();
        request.conditions = lane.conditions(&request.conditions);
        request.conditions.system_tokens = None;
        request.conditions.tools_tokens = None;
        request.conditions.notes_tokens = None;
        let answer: Result<RawRideAnswer, _> = call(
            plane,
            "compaction.ride",
            &request,
            Duration::from_millis(request.deadline_ms),
        )
        .await;
        match answer {
            Ok(RawRideAnswer {
                answer,
                view: Some(view),
            }) if answer == "view" => match ReceivedView::parse(&view) {
                Ok(received) => Some(received),
                Err(_) => {
                    plane.view_refused(&view, "invalid_params");
                    None
                }
            },
            Err(CallError::Refused(ref refusal)) if refusal.code == "unknown_session" => {
                lane.ready = false;
                lane.setup_due = true;
                None
            }
            Ok(RawRideAnswer { answer, .. }) if answer == "nothing" => None,
            Ok(_) => {
                tracing::warn!(
                    method = "compaction.ride",
                    "compaction provider returned an unknown or malformed ride answer"
                );
                None
            }
            Err(_) => None,
        }
    }
}

async fn setup(
    lane: &mut Lane,
    plane: &dyn Plane,
    request: &SetupRequest,
    entry_speaks_for: &AtomicU8,
) -> bool {
    lane.seq = 0;
    // The provider's declarations are read from the catalog at every Setup, before the Setup is
    // sent, so a provider that reads entries as soon as it is ready already sees them as
    // declared, and what they say holds for every call until the next Setup. A catalog that
    // can't be read in time sends no optional fields, but leaves attribution unknown so an
    // entries read cannot mistake the fallback for a provider's declaration.
    let declared = tokio::time::timeout(CALL_DEADLINE, plane.declarations())
        .await
        .unwrap_or_default();
    entry_speaks_for.store(
        declared
            .entry_speaks_for
            .value()
            .map_or(ATTRIBUTION_UNKNOWN, attribution),
        Ordering::Release,
    );
    lane.signed_floor_declared = declared.signed_floor;
    lane.notes_tokens_declared = declared.notes_tokens;
    lane.statement_seq_declared = declared.statement_seq;
    lane.statement_epoch = if lane.statement_seq_declared {
        mint_statement_epoch()
    } else {
        None
    };
    let mut request = request.clone();
    if !lane.signed_floor_declared {
        request.signed_floor = None;
    }
    match &lane.statement_epoch {
        Some(epoch) if request.statement_seq.is_some() => {
            request.statement_epoch = Some(epoch.clone());
        }
        _ => {
            request.statement_epoch = None;
            request.statement_seq = None;
            // A Setup without the pair clears the provider's held epoch, so the statements that
            // follow it must not carry one either.
            lane.statement_epoch = None;
        }
    }
    let answer: Result<SetupAnswer, _> =
        call(plane, "compaction.setup", &request, CALL_DEADLINE).await;
    lane.setup_unanswered = matches!(answer, Err(CallError::Unavailable));
    lane.ready = match answer {
        Ok(SetupAnswer::Ready) => true,
        Ok(_) => {
            tracing::warn!(
                method = "compaction.setup",
                "compaction provider returned an unknown Setup answer"
            );
            false
        }
        Err(_) => false,
    };
    lane.detects_cache_expired = lane.ready && request.detects.contains(&RideCause::CacheExpired);
    lane.setup_due = false;
    lane.ready
}

async fn call<Q: Serialize, A: DeserializeOwned>(
    plane: &dyn Plane,
    method: &str,
    request: &Q,
    deadline: Duration,
) -> Result<A, CallError> {
    let params = serde_json::to_value(request).map_err(|_| CallError::Unavailable)?;
    let until = tokio::time::Instant::now() + deadline;
    let value = tokio::time::timeout_at(until, plane.call(method, params))
        .await
        .map_err(|_| CallError::Unavailable)??;
    if tokio::time::Instant::now() >= until {
        return Err(CallError::Unavailable);
    }
    serde_json::from_str(value.get()).map_err(|_| CallError::Unavailable)
}

/// The ride causes broca can detect. `cache_expired` is listed only when the session's requests
/// ask the provider for a cache lifetime it honours, the only lifetime that can prove expiry;
/// elsewhere (ChatGPT's backend, gateways) broca can't tell, and an unlisted cause tells the
/// provider so.
pub fn detects(cache_expired: bool) -> Vec<RideCause> {
    let mut causes = vec![
        RideCause::CacheExpired,
        RideCause::ChainReset,
        RideCause::ModelChange,
        RideCause::EffortChange,
        RideCause::TierChange,
        RideCause::AccountChange,
        RideCause::SurfaceChange,
        RideCause::OverLimit,
    ];
    if !cache_expired {
        causes.retain(|cause| *cause != RideCause::CacheExpired);
    }
    causes
}

#[cfg(test)]
thread_local! {
    /// How many entries this thread's observations looked at.
    static OBSERVED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Recount queued entries under a new family when full content is available; otherwise drop
/// their old metadata rather than label an earlier family's counts with the new family.
fn refresh_pending_counts(
    pending: &mut Pending,
    entries: &[Entry],
    counts: &mut super::tokens::Counts,
) {
    if pending.token_family.as_deref() == counts.family_id() {
        return;
    }
    pending.token_family = counts.family_id().map(str::to_owned);
    pending.entry_bytes = 0;
    // The pending batch is bounded by the notice budget. Refresh only that batch, not the
    // history, when the model's counting rules change before a coalesced notice is flushed.
    for (ordinal, (entry, bytes)) in &mut pending.entries {
        if let Ok(index) = entries.binary_search_by_key(ordinal, |entry| entry.ordinal) {
            let mut full = entries[index].clone();
            counts.annotate(std::slice::from_mut(&mut full));
            *entry = super::entries::compact(&full);
        } else {
            super::tokens::strip_counts(std::slice::from_mut(entry));
        }
        *bytes = broca_protocol::serialized_json_bytes(entry);
        pending.entry_bytes += *bytes;
    }
}

/// [`Runtime::observe`] of `entries[from..]`, with the newest of all `entries`.
fn observe_from(
    pending: &mut Pending,
    entries: &[Entry],
    from: usize,
    counts: &mut super::tokens::Counts,
) {
    if let Some(newest) = views::newest(entries) {
        if pending
            .newest
            .as_ref()
            .is_none_or(|previous| previous.ordinal <= newest.ordinal)
        {
            pending.newest = Some(newest);
        }
    }
    let through = pending.observed_through;
    let assistant = pending.assistant.clone();
    for entry in &entries[from..] {
        let changed = entry.ordinal > through
            || assistant.as_ref().is_some_and(|(ordinal, digest)| {
                *ordinal == entry.ordinal && digest != &entry.digest
            });
        if changed {
            pending.changed = true;
            if !pending.entries_omitted {
                let mut counted = entry.clone();
                counts.annotate(std::slice::from_mut(&mut counted));
                let compact = super::entries::compact(&counted);
                let bytes = broca_protocol::serialized_json_bytes(&compact);
                if let Some((_, old_bytes)) =
                    pending.entries.insert(entry.ordinal, (compact, bytes))
                {
                    pending.entry_bytes = pending.entry_bytes.saturating_sub(old_bytes);
                }
                pending.entry_bytes += bytes;
                if pending.entry_bytes + 2 + pending.entries.len().saturating_sub(1) as u64
                    > 1_048_576
                {
                    pending.entry_bytes = 0;
                    for (entry, bytes) in pending.entries.values_mut() {
                        super::tokens::strip_counts(std::slice::from_mut(entry));
                        *bytes = broca_protocol::serialized_json_bytes(entry);
                        pending.entry_bytes += *bytes;
                    }
                    if pending.entry_bytes + 2 + pending.entries.len().saturating_sub(1) as u64
                        > 1_048_576
                    {
                        pending.entries.clear();
                        pending.entry_bytes = 0;
                        pending.entries_omitted = true;
                    }
                }
            }
        }
        pending.observed_through = pending.observed_through.max(entry.ordinal);
        if entry.role == cortexkit_role_compaction::read::Role::Assistant
            && !entry.parts.is_empty()
            && pending
                .assistant
                .as_ref()
                .is_none_or(|(ordinal, _)| *ordinal <= entry.ordinal)
        {
            pending.assistant = Some((entry.ordinal, entry.digest.to_string()));
        }
    }
}

#[cfg(test)]
mod pending_tests {
    use super::*;
    use cortexkit_role_compaction::{
        part::{Part, PartBody},
        read::Role,
    };

    #[test]
    fn compaction_v2_pending_notice_omits_bulk_without_retaining_a_transcript_queue() {
        let runtime = Runtime::default();
        let entry = Entry::new(
            1,
            "mid",
            Role::User,
            1,
            vec![Part::new(
                "0",
                PartBody::Text {
                    text: "x".repeat(1_048_577).into(),
                },
            )],
        );
        runtime.observe(&[entry]);
        let pending = runtime.pending.lock().unwrap();
        assert!(pending.changed);
        assert!(pending.entries_omitted);
        assert!(pending.entries.is_empty());
        assert_eq!(pending.newest.as_ref().unwrap().ordinal, 1);
    }

    /// Once an assistant entry has been observed, an observation looks only at the entries past
    /// the newest observed and at that assistant entry, so its cost doesn't grow with how many
    /// entries it's handed.
    #[test]
    fn an_observation_looks_only_at_what_can_have_changed() {
        let entry = |ordinal: u64| {
            let role = if ordinal.is_multiple_of(2) {
                Role::Assistant
            } else {
                Role::User
            };
            Entry::new(
                ordinal,
                format!("m{ordinal}"),
                role,
                1,
                vec![Part::new("0", PartBody::Text { text: "x".into() })],
            )
        };
        let looked_at = |window: u64| {
            let runtime = Runtime::default();
            let mut entries: Vec<Entry> = (1..=window).map(entry).collect();
            runtime.observe(&entries);
            entries.push(entry(window + 1));
            let before = OBSERVED.with(std::cell::Cell::get);
            runtime.observe(&entries);
            let looked = OBSERVED.with(std::cell::Cell::get) - before;
            // The new entry was noticed.
            let pending = runtime.pending.lock().unwrap();
            assert_eq!(pending.observed_through, window + 1);
            assert!(pending.entries.contains_key(&(window + 1)));
            looked
        };
        // The newest assistant entry (the window's last) and the new one.
        assert_eq!(looked_at(10), 2);
        assert_eq!(looked_at(1_000), 2);
    }
}
