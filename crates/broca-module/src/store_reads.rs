//! Reads of a session kept in the new storage, answered without reloading the session.
//!
//! A read used to load the session read-only (`Session::view`) on every call: the whole open log
//! read, authenticated and folded again, which on a long open log costs far more than the read.
//! Reads now come from one of three places:
//!
//! - **The open session's actor.** When the registry holds the session's store actor open, the
//!   read is a command to it ([`StoreReadRequest`]), answered from the actor's live session by
//!   borrow between its other commands. A run writes through the actor's client, so a read never
//!   waits on a model step. The actor answers only from durable state: a read that arrives after
//!   a batch appended without a barrier makes the log durable first, once, with a claim frame
//!   (`Session::sync`); a sync that fails refuses the read, transient,
//!   `durability_flush_failed`. The actor answers from memory only (`Session::in_memory`): it
//!   never reads storage for a read.
//! - **History the actor doesn't hold.** A read that needs sealed history, or a run's records
//!   the actor no longer keeps, comes back as the session's position: its head version, its open
//!   log and the log's durable length ([`StoreReadOutcome::Stored`]). The caller reads the
//!   session as it stood there itself (`Session::view_at`). Sealed objects never change and the
//!   durable part of an open log only grows, so what it reads is what the actor would have read.
//! - **A closed session.** Its view, loaded at the session's current position, is kept in a
//!   bounded cache ([`ViewCache`]) and served again until the session moves on.
//!
//! Every path runs the same read ([`execute`]) over the session it got, so a read answers the
//! same wherever it was served from.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use broca_protocol::{BindPrincipal, RunId};
use broca_session_loop::parts::Parts;
use broca_session_store::backend::{LocalBackend, SessionBackend};
use broca_session_store::session::{Position, Session, SessionView, StoredEntry};

use crate::serve::ServeError;
use crate::store_status::SettledRuns;

/// A read of a session kept in the new storage.
#[derive(Debug, Clone)]
pub enum StoreReadRequest {
    /// `run.status` for one run.
    RunStatus(RunId),
    /// `run.status` as the wire answers a run id, which may name a queued send.
    RunStatusReply(RunId),
    /// `run.result`.
    RunResult(RunId),
    /// `run.tool_calls`.
    RunToolCalls(cortexkit_role_llm_runner::tool_calls::ToolCallsRequest),
    /// `send.status` and `session.send_status`.
    SendStatus {
        own_run: RunId,
        send_id: String,
        principal: Option<BindPrincipal>,
    },
    /// `session.head`, as the session alone decides it.
    Head,
    /// A `session.read` page.
    Page(PageRequest),
    /// A page of entries, as `session.entries` reads them: up to `limit` from ordinal `from`.
    Entries { from: u64, limit: usize },
    /// Internal authorization only: ownership without forcing pending records durable.
    AuthorizationOwnership,
    /// Internal authorization only: the immutable sender of a queued submission.
    AuthorizationSubmissionSender(String),
    /// One `compaction-runner/v1` operation (see `store_entries::serve_compaction`), for the
    /// session `key` and the daemon-attested route `caller`.
    Compaction {
        key: broca_protocol::BindIdentity,
        caller: Option<String>,
        method: String,
        params: Box<serde_json::value::RawValue>,
    },
    /// What `session.history` reads about the session besides its pages, resolving the message
    /// `mid` names when it names one (see `store_entries::HistoryHead`).
    HistoryHead { mid: Option<String> },
    /// The medium with digest `sha256` that a record of the session names, with its bytes, as
    /// `session.media_read` serves it.
    Media { sha256: String },
    /// Where part number `tag` sits, for output references (see
    /// `store_output_refs::tag_location`).
    TagLocation(u64),
    /// The compaction view the model step that wrote message `ordinal` (counting from 0) was
    /// rendered under, for output references (see `store_output_refs::writing_view`).
    WritingView { ordinal: u64 },
    /// What a plan-change op's preflight reads about the session, for a request naming
    /// `generation` and `send_id` (see `store_plan_change::change_read`).
    PlanChange {
        generation: Option<u64>,
        send_id: Option<String>,
    },
    /// The plan identities a later send's plan is checked against once the session took a
    /// refresh (see `store_plan_change::identities`).
    PlanIdentities,
}

impl StoreReadRequest {
    pub(crate) fn is_authorization(&self) -> bool {
        matches!(
            self,
            Self::AuthorizationOwnership | Self::AuthorizationSubmissionSender(_)
        )
    }
}

/// What a `session.read` page asks of the session itself.
#[derive(Debug, Clone, Default)]
pub struct PageRequest {
    /// Serve the page only while the session's history version is this one.
    pub history_version: Option<u64>,
    /// Start after the message with this id.
    pub after_mid: Option<String>,
    /// Start at this ordinal, counting from 1; the newest page without one.
    pub from_ordinal: Option<u64>,
    pub limit: Option<u64>,
    /// What the read opts into: each message's display facts, the latest run's tools, and each
    /// assistant message's Rich UI stamp.
    pub include_display_facts: bool,
    pub include_tools: bool,
    pub include_rich_ui: bool,
}

impl PageRequest {
    /// What `req` asks of the session itself.
    pub fn of(req: &crate::serve::SessionReadRequest) -> Self {
        Self {
            history_version: req.history_version,
            after_mid: req.after_mid.clone(),
            from_ordinal: req.from_ordinal,
            limit: req.limit,
            include_display_facts: req.include_display_facts,
            include_tools: req.include_tools,
            include_rich_ui: req.include_rich_ui,
        }
    }
}

/// A `session.read` page and what the reply says about the session beside it.
pub struct PageAnswer {
    /// The reply as far as the session decides it (see `store_read::page`).
    pub response: broca_wire::SessionReadResponse,
    /// Where the page started, counting from 0; `None` for the newest page.
    pub from_ordinal: Option<u64>,
}

/// The answer to a [`StoreReadRequest`] of the same name.
pub enum StoreReadReply {
    RunStatus(Result<broca_wire::RunStatusResponse, ServeError>),
    RunStatusReply(Result<broca_wire::RunStatusReply, ServeError>),
    RunResult(Result<broca_wire::RunResultResponse, ServeError>),
    RunToolCalls(Result<cortexkit_role_llm_runner::tool_calls::ToolCallsPage, ServeError>),
    SendStatus(Result<broca_wire::SendReplyWire, ServeError>),
    Head(Result<Box<crate::store_head::StoreHead>, ServeError>),
    Page(Result<PageAnswer, ServeError>),
    Entries(Result<Vec<StoredEntry>, ServeError>),
    AuthorizationOwnership(Result<broca_core::SessionOwnershipV1, String>),
    AuthorizationSubmissionSender(Result<Option<BindPrincipal>, ServeError>),
    Compaction(
        Result<
            Box<serde_json::value::RawValue>,
            broca_core::compaction_v2::protocol::wire::Refusal,
        >,
    ),
    HistoryHead(Result<crate::store_entries::HistoryHead, ServeError>),
    /// `None` when no record of the session names the digest.
    Media(Result<Option<StoredMedia>, ServeError>),
    TagLocation(Result<broca_core::TagLocation, ServeError>),
    WritingView(Result<Option<broca_core::compaction_provider::view::CompactionView>, ServeError>),
    PlanChange(Result<PlanChangeRead, String>),
    PlanIdentities(Result<RefreshedIdentities, String>),
}

/// A medium a record of the session names: its reference, and its bytes or why they couldn't be
/// read.
#[derive(Debug)]
pub struct StoredMedia {
    pub reference: broca_protocol::MediaRef,
    pub bytes: Result<Vec<u8>, String>,
}

/// The plan identities a later send's plan is checked against once the session took a refresh:
/// the applied plan's and the latest accepted one's; `None` before any refresh.
pub type RefreshedIdentities = Option<(Option<String>, Option<String>)>;

/// What a plan-change op's preflight read about a session kept in the new storage.
pub struct PlanChangeRead(pub(crate) crate::serve::ChangeRead);

/// How the session's store actor answered a read.
pub enum StoreReadOutcome {
    /// Answered whole from the actor's session.
    Served(Box<StoreReadReply>),
    /// The read needs history the actor doesn't hold: read the session as it stood at this
    /// position. `live_step` is the actor's run's in-flight step, as the actor would have served
    /// it.
    Stored {
        position: Position,
        live_step: Option<LiveStep>,
        /// The actor's `ReadContext::entry_speaks_for`, which the read keeps.
        entry_speaks_for: crate::store_entries::EntryAttribution,
    },
    /// The read can't be answered now. Transient.
    Refused(ServeError),
    /// The actor is closing: read the session as no actor holds it.
    Reopen,
}

/// The in-flight model step of the run `run_id`, as the actor running it knows it: what today's
/// `run.status` serves as an active run's `step`.
#[derive(Debug, Clone)]
pub struct LiveStep {
    pub run_id: String,
    pub step: Option<broca_protocol::provider::InFlightStep>,
}

/// What a read needs besides the session.
pub struct ReadContext<'a> {
    /// The deployment's setup parts, which a run's start names.
    pub parts: &'a dyn Parts,
    /// The registry holds a slot for the session (an actor is open, opening or closing), so an
    /// unfinished run is running rather than cut off by a crash.
    pub held: bool,
    /// The session's actor is open, as `session.head` tells a live run from an interrupted one.
    pub open: bool,
    /// Where finished runs' facts are kept, and the session's address that keys them.
    pub settled: Option<(&'a SettledRuns, &'a str)>,
    /// The session's id, which a reply too large to send names.
    pub session_name: &'a str,
    /// The in-flight step of the actor's running run, if any.
    pub live_step: Option<&'a LiveStep>,
    /// The cursor of the session's last control unit, as the actor's live projection holds it
    /// (`Some(None)` for a stream with no unit yet), or `None` when no seeded projection knows
    /// it, so a page projects it from storage.
    pub stream_head: Option<Option<broca_core::Cursor>>,
    /// Whether compaction entries the read serves keep `speaks_for`: whether the session's
    /// compaction provider declared `compaction-entry-speaks-for/v1`.
    pub entry_speaks_for: crate::store_entries::EntryAttribution,
}

impl ReadContext<'_> {
    fn reader(&self) -> crate::store_status::Reader<'_> {
        crate::store_status::Reader {
            parts: self.parts,
            live: self.held,
            settled: self.settled,
            session_name: self.session_name,
            live_step: self.live_step,
        }
    }
}

/// A reply that doesn't answer the request it was given: a bug, never a session's state.
pub(crate) fn mismatched() -> ServeError {
    ServeError::Open(
        "a store read was answered with another read's reply".into(),
        Some(broca_protocol::ErrorClass::Permanent),
    )
}

/// A read's refusal as the registry's authorization paths report it, keeping its class.
pub(crate) fn open_error(error: ServeError) -> crate::registry::OpenError {
    use crate::registry::OpenError;
    match error {
        ServeError::Open(reason, Some(class)) => OpenError::Classified { reason, class },
        ServeError::Open(reason, None) => OpenError::Open(reason),
        other => OpenError::Open(format!("{other:?}")),
    }
}

/// Runs `request` over `session`. The actor runs it over its own session, from memory only; a
/// caller runs it over a view. Either way it is the same read.
pub async fn execute<B: SessionBackend>(
    session: &Session<B>,
    ctx: &ReadContext<'_>,
    request: &StoreReadRequest,
) -> StoreReadReply {
    let reader = ctx.reader();
    match request {
        StoreReadRequest::RunStatus(run_id) => {
            StoreReadReply::RunStatus(reader.run_status(session, run_id).await)
        }
        StoreReadRequest::RunStatusReply(run_id) => {
            StoreReadReply::RunStatusReply(reader.run_status_reply(session, run_id).await)
        }
        StoreReadRequest::RunResult(run_id) => {
            StoreReadReply::RunResult(reader.run_result(session, run_id).await)
        }
        StoreReadRequest::RunToolCalls(request) => StoreReadReply::RunToolCalls(
            crate::store_tool_calls::run_tool_calls(session, request).await,
        ),
        StoreReadRequest::SendStatus {
            own_run,
            send_id,
            principal,
        } => StoreReadReply::SendStatus(
            reader
                .send_status(session, own_run.clone(), send_id, principal.as_ref())
                .await,
        ),
        StoreReadRequest::Head => StoreReadReply::Head(
            crate::store_head::build(session, ctx.parts, ctx.open)
                .await
                .map(Box::new)
                .map_err(crate::store_head::head_failed),
        ),
        StoreReadRequest::Page(page) => StoreReadReply::Page(
            crate::store_read::page(session, ctx.parts, page, ctx.stream_head).await,
        ),
        StoreReadRequest::Entries { from, limit } => StoreReadReply::Entries(
            crate::store_read::served_entries(session, *from, *limit)
                .await
                .map_err(crate::store_read::read_failed),
        ),
        StoreReadRequest::AuthorizationOwnership => StoreReadReply::AuthorizationOwnership(
            broca_session_loop::owner::ownership_with_plan(&session.operations, ctx.parts)
                .map_err(|error| format!("the session's stored owner can't be read: {error}")),
        ),
        StoreReadRequest::AuthorizationSubmissionSender(id) => {
            StoreReadReply::AuthorizationSubmissionSender(
                crate::store_status::authorization_submission_sender(session, id)
                    .await
                    .map_err(Into::into),
            )
        }
        StoreReadRequest::Compaction {
            key,
            caller,
            method,
            params,
        } => StoreReadReply::Compaction(
            crate::store_entries::serve_compaction(
                ctx.parts,
                Some(session),
                key,
                caller.as_deref(),
                method,
                params,
                &ctx.entry_speaks_for,
            )
            .await,
        ),
        StoreReadRequest::HistoryHead { mid } => StoreReadReply::HistoryHead(
            crate::store_entries::history_head_of(session, mid.as_deref()).await,
        ),
        StoreReadRequest::Media { sha256 } => {
            StoreReadReply::Media(stored_media(session, sha256).await)
        }
        StoreReadRequest::TagLocation(tag) => StoreReadReply::TagLocation(
            crate::store_output_refs::tag_location(session, ctx.parts, *tag)
                .await
                .map_err(output_refs_failed),
        ),
        StoreReadRequest::WritingView { ordinal } => StoreReadReply::WritingView(
            crate::store_output_refs::writing_view(session, *ordinal)
                .await
                .map_err(output_refs_failed),
        ),
        StoreReadRequest::PlanChange {
            generation,
            send_id,
        } => StoreReadReply::PlanChange(
            crate::store_plan_change::change_read(
                session,
                ctx.parts,
                *generation,
                send_id.as_deref(),
            )
            .await
            .map(PlanChangeRead),
        ),
        StoreReadRequest::PlanIdentities => {
            StoreReadReply::PlanIdentities(crate::store_plan_change::identities(session).await)
        }
    }
}

/// The medium with digest `sha256` that a record of `session` names, with its bytes.
async fn stored_media<B: SessionBackend>(
    session: &Session<B>,
    sha256: &str,
) -> Result<Option<StoredMedia>, ServeError> {
    let Some(medium) = session
        .named_media(sha256)
        .await
        .map_err(crate::store_read::read_failed)?
    else {
        return Ok(None);
    };
    let bytes = session
        .named_media_bytes(&medium)
        .await
        .map_err(|error| format!("{error:?}"));
    Ok(Some(StoredMedia {
        reference: medium.reference,
        bytes,
    }))
}

/// A read output references make of the session that failed: a backend failure and damaged
/// facts read alike here, so no class is claimed.
fn output_refs_failed(error: String) -> ServeError {
    ServeError::Open(
        format!("the session's output reference facts can't be read: {error}"),
        None,
    )
}

/// Makes `session`'s latest records durable before a read reports them: the read's refusal when
/// they can't be.
pub(crate) async fn durable_for_read<B: SessionBackend>(
    session: &mut Session<B>,
) -> Option<StoreReadOutcome> {
    if session.unsynced() {
        // A batch appended without a barrier isn't durable yet; a read must not report it until
        // it is. Making it durable early is harmless.
        if let Err(error) = session.sync().await {
            return Some(StoreReadOutcome::Refused(ServeError::Open(
                format!(
                    "durability_flush_failed: the session's latest records could not be made \
                     durable before reading them: {error:?}"
                ),
                Some(broca_protocol::ErrorClass::Transient),
            )));
        }
    }
    None
}

/// Answers `request` from memory only, flushing first unless the read is authorization-only.
/// `None` from `answer` means the read needed storage. The actor calls this between its commands,
/// with nothing else appending.
pub async fn answer_in_memory<B: SessionBackend>(
    session: &mut Session<B>,
    ctx: &ReadContext<'_>,
    request: &StoreReadRequest,
) -> StoreReadOutcome {
    use futures::FutureExt as _;
    // Authorization may inspect pending records without flushing them: an admitted operation
    // writes after those records in this single-writer log, so its write cannot become durable
    // without them. A refusal writes nothing. Never use these requests to serve caller data.
    if !request.is_authorization() {
        if let Some(refused) = durable_for_read(session).await {
            return refused;
        }
    }
    let session: &Session<B> = session;
    let memory = session.in_memory();
    // From memory only, a read never waits: a read that would have waited on storage refused
    // instead, and is answered from storage by the caller.
    let reply = execute(session, ctx, request).now_or_never();
    let needs_storage = memory.refused();
    drop(memory);
    match reply {
        Some(reply) if !needs_storage => StoreReadOutcome::Served(Box::new(reply)),
        _ => StoreReadOutcome::Stored {
            position: session.position(),
            live_step: ctx.live_step.cloned(),
            entry_speaks_for: ctx.entry_speaks_for.clone(),
        },
    }
}

/// Views of closed sessions, each loaded at a position and kept while it is the newest position
/// read for its session.
///
/// **Keyed by position.** An append moves a session's open log but not its head version, so a
/// view is keyed by the session's address and its whole position: the head version, which every
/// seal, revert, recovery, writer open and re-creation moves, and the open log with its length,
/// which every append moves. A read probes the position first (the head, and the log's length),
/// so an entry for an older position is never served: it is unreachable, and the next load for
/// its session replaces it.
///
/// **Shared loads.** Readers of the same session at the same position share one load.
///
/// **Bounded** by [`ViewCache::MAX_ENTRIES`] views and [`ViewCache::MAX_BYTES`] of estimated size;
/// the least recently used view goes first.
pub struct ViewCache<V = SessionView<LocalBackend>> {
    inner: Mutex<CacheInner<V>>,
    max_entries: usize,
    max_bytes: u64,
}

struct CacheInner<V> {
    entries: HashMap<String, CacheEntry<V>>,
    bytes: u64,
    clock: u64,
}

impl<V> Default for CacheInner<V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            clock: 0,
        }
    }
}

struct CacheEntry<V> {
    position: Position,
    cell: Arc<tokio::sync::OnceCell<Arc<V>>>,
    /// The view's estimated size, once loaded.
    bytes: u64,
    used: u64,
}

impl ViewCache {
    /// How many closed sessions' views are kept. A module serves a few sessions at a time that no
    /// actor holds (heads listed by a UI, a session read after its run ended); a session read
    /// again after this many others were read reloads.
    pub const MAX_ENTRIES: usize = 64;

    /// The estimated bytes of all kept views together: 1 GiB. A view holds its open log's records
    /// and the folds built from them, about [`ViewCache::BYTES_PER_LOG_BYTE`] times the log's
    /// length. A view of the largest live log today (a 14 MB WAL) then takes about 70 MB, so a
    /// dozen such sessions, or dozens of ordinary ones, stay loaded. A session whose view alone
    /// would pass the bound (an open log past about 200 MB, some 100,000 messages of tool turns)
    /// is never kept: each of its reads while closed loads it, as every read did before views were
    /// kept. Holding several gigabytes for sessions nobody may read again would cost more than
    /// those occasional loads; a session that long and in use is open, and its actor answers.
    pub const MAX_BYTES: u64 = 1024 * 1024 * 1024;

    /// A loaded view's resident size per byte of its open log. The read cost bar
    /// (`store_actor::read_cost`) measured 1.39 GB for a 275 MB log of 150,000 messages, 5.1
    /// bytes per byte; a small session adds a few megabytes of its own on top.
    pub const BYTES_PER_LOG_BYTE: u64 = 5;
}

impl<V> ViewCache<V> {
    pub fn new(max_entries: usize, max_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(CacheInner::default()),
            max_entries: max_entries.max(1),
            max_bytes,
        }
    }

    /// The view of the session at `addr` at `position`: the kept one, or the one `load` loads,
    /// which is then kept. Concurrent callers for the same position share one load. A failed load
    /// keeps nothing, and the next caller loads again.
    pub async fn view<E>(
        &self,
        addr: &str,
        position: Position,
        load: impl std::future::Future<Output = Result<V, E>>,
    ) -> Result<Arc<V>, E> {
        let cell = {
            let mut inner = self.inner.lock().unwrap();
            inner.clock += 1;
            let clock = inner.clock;
            match inner.entries.get_mut(addr) {
                Some(entry) if entry.position == position => {
                    entry.used = clock;
                    Arc::clone(&entry.cell)
                }
                _ => {
                    let cell = Arc::new(tokio::sync::OnceCell::new());
                    let replaced = inner.entries.insert(
                        addr.to_owned(),
                        CacheEntry {
                            position,
                            cell: Arc::clone(&cell),
                            bytes: 0,
                            used: clock,
                        },
                    );
                    if let Some(replaced) = replaced {
                        inner.bytes -= replaced.bytes;
                    }
                    cell
                }
            }
        };
        let view = cell
            .get_or_try_init(|| async { load.await.map(Arc::new) })
            .await?
            .clone();
        let mut inner = self.inner.lock().unwrap();
        let size = view_bytes(&position);
        if let Some(entry) = inner.entries.get_mut(addr) {
            if Arc::ptr_eq(&entry.cell, &cell) && entry.bytes == 0 {
                entry.bytes = size;
                inner.bytes += size;
            }
        }
        inner.evict(self.max_entries, self.max_bytes);
        Ok(view)
    }

    /// How many views are kept, and their estimated bytes.
    pub fn usage(&self) -> (usize, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.entries.len(), inner.bytes)
    }
}

impl Default for ViewCache {
    fn default() -> Self {
        Self::new(Self::MAX_ENTRIES, Self::MAX_BYTES)
    }
}

impl<V> CacheInner<V> {
    /// Drops the least recently used views until the cache is within its bounds.
    fn evict(&mut self, max_entries: usize, max_bytes: u64) {
        while self.entries.len() > max_entries || self.bytes > max_bytes {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(addr, _)| addr.clone())
            else {
                return;
            };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.bytes -= entry.bytes;
            }
        }
    }
}

/// A view's estimated resident size: its open log's length times what the cost bar measured per
/// byte. A view at a position never grows, apart from sealed entries it reads and keeps.
fn view_bytes(position: &Position) -> u64 {
    position
        .len
        .saturating_mul(ViewCache::BYTES_PER_LOG_BYTE)
        .max(1)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use broca_session_store::codec::Id128;
    use broca_session_store::session::Position;

    use super::ViewCache;

    fn at(version: u64, len: u64) -> Position {
        Position {
            version,
            log: Id128([1; 16]),
            len,
        }
    }

    /// Readers of one session at one position share one load; the next read is served from the
    /// cache without loading.
    #[tokio::test]
    async fn concurrent_readers_share_one_load() {
        let cache: Arc<ViewCache<u64>> = Arc::new(ViewCache::new(4, u64::MAX));
        let loads = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let readers: Vec<_> = (0..8)
            .map(|_| {
                let (cache, loads, release) =
                    (Arc::clone(&cache), Arc::clone(&loads), Arc::clone(&release));
                tokio::spawn(async move {
                    cache
                        .view("s", at(1, 10), async {
                            loads.fetch_add(1, Ordering::SeqCst);
                            release.notified().await;
                            Ok::<_, ()>(7)
                        })
                        .await
                })
            })
            .collect();
        broca_test_support::wait_until("the first reader is loading", || async {
            match loads.load(Ordering::SeqCst) {
                0 => Err("no load started yet".into()),
                _ => Ok(()),
            }
        })
        .await;
        release.notify_one();
        for reader in readers {
            assert_eq!(*reader.await.unwrap().unwrap(), 7);
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        let again = cache
            .view("s", at(1, 10), async {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(8)
            })
            .await
            .unwrap();
        assert_eq!((*again, loads.load(Ordering::SeqCst)), (7, 1));
    }

    /// A session at a new position (another head version, or a longer log) is loaded again, and
    /// its view replaces the older one.
    #[tokio::test]
    async fn a_new_position_loads_again_and_replaces_the_old_view() {
        let cache: ViewCache<u64> = ViewCache::new(4, u64::MAX);
        let load = |value: u64| async move { Ok::<_, ()>(value) };
        assert_eq!(*cache.view("s", at(1, 10), load(1)).await.unwrap(), 1);
        assert_eq!(*cache.view("s", at(2, 10), load(2)).await.unwrap(), 2);
        assert_eq!(*cache.view("s", at(2, 11), load(3)).await.unwrap(), 3);
        assert_eq!(cache.usage().0, 1);
        // A failed load keeps nothing, and the next reader loads.
        assert!(cache.view("s", at(3, 1), async { Err(()) }).await.is_err());
        assert_eq!(*cache.view("s", at(3, 1), load(4)).await.unwrap(), 4);
    }

    /// The least recently used view goes first, by count and by estimated bytes.
    #[tokio::test]
    async fn the_cache_stays_within_its_bounds() {
        let load = |value: u64| async move { Ok::<_, ()>(value) };
        let cache: ViewCache<u64> = ViewCache::new(2, u64::MAX);
        cache.view("a", at(1, 1), load(1)).await.unwrap();
        cache.view("b", at(1, 1), load(2)).await.unwrap();
        cache.view("a", at(1, 1), load(9)).await.unwrap();
        cache.view("c", at(1, 1), load(3)).await.unwrap();
        assert_eq!(cache.usage().0, 2);
        // `b` was the least recently used, so it is loaded again.
        assert_eq!(*cache.view("b", at(1, 1), load(4)).await.unwrap(), 4);
        assert_eq!(*cache.view("c", at(1, 1), load(9)).await.unwrap(), 3);

        let per_view = super::view_bytes(&at(1, 100));
        let cache: ViewCache<u64> = ViewCache::new(10, 2 * per_view);
        for name in ["a", "b", "c"] {
            cache.view(name, at(1, 100), load(1)).await.unwrap();
        }
        assert_eq!(cache.usage(), (2, 2 * per_view));
    }
}
