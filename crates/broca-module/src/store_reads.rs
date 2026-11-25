//! Reads of a session kept in the session store, answered without reloading the session.
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

/// A read of a session kept in the session store.
#[derive(Debug, Clone)]
pub enum StoreReadRequest {
    /// `run.admission` for one run's frozen start.
    RunAdmission(RunId),
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
    /// Internal reader-policy admission, without page work or an actor recovery.
    AuthorizationRead(crate::reader_admission::ReadAccess),
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
    /// The plan manifest run `run_id` was admitted under, for output references (see
    /// `store_output_refs::run_manifest`).
    RunManifest(RunId),
    /// The namespace of output references: the source log's lineage after conversion, otherwise
    /// the store incarnation. It is independent of the public history cursor's incarnation.
    OutputRefNamespace,
    /// What a plan-change op's preflight reads about the session, for a request naming
    /// `generation` and `send_id` (see `store_plan_change::change_read`).
    PlanChange {
        generation: Option<u64>,
        send_id: Option<String>,
    },
    /// The plan identities a later send's plan is checked against once the session took a
    /// refresh (see `store_plan_change::identities`).
    PlanIdentities,
    /// What `session.baseline` reports, before its owner check and cache declaration (see
    /// `store_plan_change::baseline`).
    Baseline,
    /// The applied manifest and dated requests under it, without opening an actor.
    PlanView,
    /// The ledger records the late-results intake classifies the provider-held calls named by
    /// `call_keys` by (see `late_results_puller::stored::held_call_records`). Broca reads it for
    /// itself, for no caller.
    HeldCalls { call_keys: Vec<String> },
    /// Where the session stands, for a read that runs its own code over the session as stored
    /// (`StoreRuntime::durable_view`), counted in the memory report under the name it carries.
    /// An actor answers it like any caller read, after making its records durable, so the
    /// position it names is durable.
    DurableView(&'static str),
}

impl StoreReadRequest {
    pub(crate) fn is_authorization(&self) -> bool {
        matches!(
            self,
            Self::AuthorizationOwnership
                | Self::AuthorizationSubmissionSender(_)
                | Self::AuthorizationRead(_)
        )
    }

    /// Whether broca reads it for itself: it needs durable records like a caller's read, but
    /// no subscribe cursor comes with its answer, so it doesn't seed the subscriber projection.
    pub(crate) fn is_internal(&self) -> bool {
        matches!(self, Self::HeldCalls { .. })
    }

    /// The read's name, as the memory report counts the closed-session views it loads.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::RunAdmission(_) => "run.admission",
            Self::RunStatus(_) | Self::RunStatusReply(_) => "run.status",
            Self::RunResult(_) => "run.result",
            Self::RunToolCalls(_) => "run.tool_calls",
            Self::SendStatus { .. } => "send_status",
            Self::Head => "session.head",
            Self::Page(_) => "session.read",
            Self::Entries { .. } => "session.entries",
            Self::AuthorizationOwnership
            | Self::AuthorizationRead(_)
            | Self::AuthorizationSubmissionSender(_) => "authorization",
            Self::Compaction { .. } => "compaction",
            Self::HistoryHead { .. } => "session.history",
            Self::Media { .. } => "session.media_read",
            Self::TagLocation(_)
            | Self::WritingView { .. }
            | Self::OutputRefNamespace
            | Self::RunManifest(_) => "output_refs",
            Self::PlanChange { .. } | Self::PlanIdentities => "plan_change",
            Self::Baseline => "session.baseline",
            Self::PlanView => "session.plan_view",
            Self::HeldCalls { .. } => "held_calls",
            Self::DurableView(reason) => reason,
        }
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
    RunAdmission(Result<Option<broca_wire::RunAdmissionResponse>, ServeError>),
    RunStatus(Result<broca_wire::RunStatusResponse, ServeError>),
    RunStatusReply(Result<broca_wire::RunStatusReply, ServeError>),
    RunResult(Result<broca_wire::RunResultResponse, ServeError>),
    RunToolCalls(Result<cortexkit_role_llm_runner::tool_calls::ToolCallsPage, ServeError>),
    SendStatus(Result<broca_wire::SendReplyWire, ServeError>),
    Head(Result<Box<crate::store_head::StoreHead>, ServeError>),
    Page(Result<PageAnswer, ServeError>),
    Entries(Result<Vec<StoredEntry>, ServeError>),
    AuthorizationOwnership(Result<broca_core::SessionOwnershipV1, String>),
    AuthorizationRead(Result<(), ServeError>),
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
    RunManifest(Result<Option<Arc<broca_records::record::PlanManifest>>, ServeError>),
    OutputRefNamespace(String),
    PlanChange(Result<PlanChangeRead, String>),
    PlanIdentities(Result<RefreshedIdentities, String>),
    /// `None` before the session's first run.
    Baseline(Result<Option<Box<crate::store_plan_change::BaselineFacts>>, String>),
    PlanView(Result<broca_wire::plan_view::PlanViewResponse, crate::store_plan_view::Error>),
    HeldCalls(Result<crate::late_results_puller::HeldCallRecords, String>),
    /// The position of the session the read ran over.
    Position(Position),
}

/// A medium a record of the session names: its reference, and its bytes or why they couldn't be
/// read.
#[derive(Debug)]
pub struct StoredMedia {
    pub reference: broca_protocol::MediaRef,
    pub bytes: Result<Arc<Vec<u8>>, String>,
}

/// The plan identities a later send's plan is checked against once the session took a refresh:
/// the applied plan's and the latest accepted one's; `None` before any refresh.
pub type RefreshedIdentities = Option<(Option<String>, Option<String>)>;

/// What a plan-change op's preflight read about a session kept in the session store.
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
        /// The cursor of the session's last control unit at `position`, as the actor's live
        /// projection holds it (`ReadContext::stream_head`), so the read needn't project it from
        /// the stored log again.
        stream_head: Option<Option<broca_core::Cursor>>,
        /// The compaction state whose token counts the actor's own reads count with, which a
        /// read of the session as stored counts with too, so they aren't counted afresh per read.
        compaction: Option<Arc<broca_core::compaction_v2::session::Session>>,
    },
    /// The read can't be answered now. Transient.
    Refused(ServeError),
    /// The actor is closing: read the session as no actor holds it.
    Reopen,
}

/// The in-flight model step of the run `run_id`, as known by the actor running it; the `step`
/// served for an active run.
#[derive(Debug, Clone)]
pub struct LiveStep {
    pub run_id: String,
    pub step: Option<broca_protocol::provider::InFlightStep>,
}

/// What a read needs besides the session.
pub struct ReadContext<'a> {
    pub labels: crate::store_awaits::Labels,
    pub token_counts: Option<&'a std::sync::Mutex<broca_core::compaction_v2::tokens::Counts>>,
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
    /// The root the session is stored in, which decides the storage policy broca assigns a
    /// session its starter sent none. `None` where the read serves no head.
    pub class: Option<broca_records::session_storage::StorageClass>,
}

impl ReadContext<'_> {
    fn reader(&self) -> crate::store_status::Reader<'_> {
        crate::store_status::Reader {
            labels: self.labels.clone(),
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
        StoreReadRequest::RunAdmission(run_id) => {
            StoreReadReply::RunAdmission(reader.run_admission(session, run_id).await)
        }
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
            async {
                let mut head = crate::store_head::build_with_retry(
                    session,
                    ctx.parts,
                    ctx.open,
                    ctx.labels.clone(),
                    ctx.live_step
                        .and_then(|live| live.step.as_ref())
                        .and_then(|step| step.retry_awaits.as_deref()),
                )
                .await
                .map_err(crate::store_head::head_failed)?;
                // The cursor a `session.read` page of the same session carries now, which a
                // subscribe resumes from.
                head.reply.head = crate::store_read::stream_head(session, ctx.stream_head)
                    .await?
                    .map(broca_wire::CursorWire::from);
                if let Some(class) = ctx.class {
                    crate::retention::policy::fill_head(
                        &mut head.reply,
                        &session.operations,
                        class,
                        session.open_log_activity_ms(),
                    )
                    .map_err(|error| {
                        ServeError::Open(error, Some(broca_protocol::ErrorClass::Permanent))
                    })?;
                }
                Ok(Box::new(head))
            }
            .await,
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
        StoreReadRequest::AuthorizationRead(access) => StoreReadReply::AuthorizationRead(
            crate::reader_admission::admit(session, ctx.parts, access).await,
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
                ctx.token_counts,
            )
            .await,
        ),
        StoreReadRequest::HistoryHead { mid } => StoreReadReply::HistoryHead(
            crate::store_entries::history_head_of(session, mid.as_deref()).await,
        ),
        StoreReadRequest::Media { sha256 } => {
            StoreReadReply::Media(stored_media(session, sha256, ctx.settled).await)
        }
        StoreReadRequest::TagLocation(tag) => StoreReadReply::TagLocation(
            crate::store_output_refs::tag_location(session, ctx.parts, *tag)
                .await
                .map_err(output_refs_failed),
        ),
        StoreReadRequest::OutputRefNamespace => StoreReadReply::OutputRefNamespace(
            session
                .operations
                .source_lineage
                .clone()
                .unwrap_or_else(|| broca_session_store::codec::hex(&session.head().incarnation.0)),
        ),
        StoreReadRequest::WritingView { ordinal } => StoreReadReply::WritingView(
            crate::store_output_refs::writing_view(session, *ordinal)
                .await
                .map_err(output_refs_failed),
        ),
        StoreReadRequest::RunManifest(run_id) => StoreReadReply::RunManifest(
            crate::store_output_refs::run_manifest(session, ctx.parts, &run_id.0)
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
        StoreReadRequest::Baseline => StoreReadReply::Baseline(
            crate::store_plan_change::baseline(session, ctx.parts).map(|facts| facts.map(Box::new)),
        ),
        StoreReadRequest::PlanView => {
            StoreReadReply::PlanView(crate::store_plan_view::build(session, ctx.parts).await)
        }
        StoreReadRequest::HeldCalls { call_keys } => StoreReadReply::HeldCalls(
            crate::late_results_puller::held_call_records(session, call_keys)
                .await
                .map_err(|error| format!("{error:?}")),
        ),
        StoreReadRequest::DurableView(_) => StoreReadReply::Position(session.position()),
    }
}

/// The medium with digest `sha256` that a record of `session` names, with its bytes. A medium a
/// record stored is kept in `settled`, under the session's address there, once found, with its
/// bytes once read and checked, so the next chunk of it reads neither the session's records nor
/// its object again (see `store_status::SettledRuns::medium`).
async fn stored_media<B: SessionBackend>(
    session: &Session<B>,
    sha256: &str,
    settled: Option<(&SettledRuns, &str)>,
) -> Result<Option<StoredMedia>, ServeError> {
    let key = settled.map(|(_, addr)| {
        (
            addr.to_owned(),
            session.head().incarnation.0,
            session.head().history_version,
            sha256.to_owned(),
        )
    });
    let kept = settled
        .zip(key.as_ref())
        .and_then(|((settled, _), key)| settled.medium(key));
    let (medium, bytes) = match kept {
        Some((medium, Some(bytes))) => (medium, Ok(bytes)),
        found => {
            let medium = match found {
                Some((medium, None)) => medium,
                _ => match session
                    .named_media(sha256)
                    .await
                    .map_err(crate::store_read::read_failed)?
                {
                    Some(medium) => medium,
                    None => return Ok(None),
                },
            };
            let bytes = session
                .named_media_bytes(&medium)
                .await
                .map(Arc::new)
                .map_err(|error| format!("{error:?}"));
            // Only a medium a record stored is kept: a later record may yet store one named
            // without its bytes, and the read then serves that one.
            if let (Some((settled, _)), Some(key), true) = (settled, key, medium.stored()) {
                settled.keep_medium(key, medium.clone(), bytes.as_ref().ok().cloned());
            }
            (medium, bytes)
        }
    };
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
    let refused_at = memory.refused_at();
    drop(memory);
    match reply {
        Some(reply) if !needs_storage => StoreReadOutcome::Served(Box::new(reply)),
        _ => {
            count_stored_read(&stored_read_name(request), refused_at);
            StoreReadOutcome::Stored {
                position: session.position(),
                live_step: ctx.live_step.cloned(),
                entry_speaks_for: ctx.entry_speaks_for.clone(),
                stream_head: ctx.stream_head,
                compaction: None,
            }
        }
    }
}

/// Reads open sessions' actors sent to storage, by `<read> @ <source line>` (see
/// `broca_wire::MemoryCache::stored_reads`). Process-wide, like the other load counts.
static STORED_READS: std::sync::Mutex<std::collections::BTreeMap<String, u64>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

/// How the memory report names a read an actor sent to storage: its name, and for a compaction
/// operation also its method and the `select.by` of its parameters, since one name covers every
/// compaction operation and each reads differently.
fn stored_read_name(request: &StoreReadRequest) -> String {
    let StoreReadRequest::Compaction { method, params, .. } = request else {
        return request.name().to_owned();
    };
    #[derive(serde::Deserialize)]
    struct Select {
        by: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct Params {
        select: Option<Select>,
    }
    let by = serde_json::from_str::<Params>(params.get())
        .ok()
        .and_then(|params| params.select?.by);
    match by {
        Some(by) => format!("compaction {method} by {by}"),
        None => format!("compaction {method}"),
    }
}

/// Counts one read an actor couldn't answer from memory: `at` is where it needed storage, or
/// `None` for a read that would have waited without asking for storage.
fn count_stored_read(read: &str, at: Option<&'static std::panic::Location<'static>>) {
    let site = at.map_or_else(
        || "waited".to_owned(),
        |at| format!("{}:{}", at.file(), at.line()),
    );
    *STORED_READS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(format!("{read} @ {site}"))
        .or_default() += 1;
}

pub(crate) fn stored_reads() -> Option<std::collections::BTreeMap<String, u64>> {
    let counts = STORED_READS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    (!counts.is_empty()).then_some(counts)
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
/// **Bounded** by [`ViewCache::MAX_ENTRIES`] views and [`ViewCache::MAX_BYTES`] of their walked
/// size (see [`CachedBytes`]) and the token counts kept beside each; the least recently used view
/// goes first.
pub struct ViewCache<V = SessionView<LocalBackend>> {
    inner: Mutex<CacheInner<V>>,
    max_entries: usize,
    max_bytes: u64,
}

struct CacheInner<V> {
    entries: HashMap<String, CacheEntry<V>>,
    bytes: u64,
    clock: u64,
    /// Views loaded and not kept because each alone was larger than the byte cap.
    over_cap: u64,
    /// Views loaded since the cache was made, kept or not (see [`ViewCache::count_unkept`]),
    /// and their walked bytes.
    loads: u64,
    loaded_bytes: u64,
    /// Of those loads, the ones that replaced a view of the same session at an older position:
    /// the session was written since its view loaded.
    reloads: u64,
    /// The same three counts per reason a view was loaded for (see [`ViewCache::view`]).
    by_reason: std::collections::BTreeMap<&'static str, broca_wire::ViewLoads>,
    /// The same counts per session, with the reason of its latest load, for at most
    /// [`SESSIONS_COUNTED`] sessions: a new session pushes out the one whose loads read the
    /// fewest bytes, so the sessions that cost the most stay counted.
    by_session: HashMap<String, (broca_wire::ViewLoads, &'static str)>,
}

/// How many sessions the view cache counts loads for (see `CacheInner::by_session`).
const SESSIONS_COUNTED: usize = 512;

/// How many of those the memory report names.
const SESSIONS_REPORTED: usize = 10;

/// Views of closed sessions this process loaded and didn't keep because each alone was larger
/// than its cache's byte cap, over every cache: `health.check` reports it as `views_over_cap`.
static VIEWS_OVER_CAP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many views of closed sessions were loaded and not kept since the process started,
/// because each alone was larger than the view cache's byte cap (see [`ViewCache::MAX_BYTES`]).
pub fn views_over_cap() -> u64 {
    VIEWS_OVER_CAP.load(std::sync::atomic::Ordering::Relaxed)
}

impl<V> Default for CacheInner<V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            clock: 0,
            over_cap: 0,
            loads: 0,
            loaded_bytes: 0,
            reloads: 0,
            by_reason: Default::default(),
            by_session: HashMap::new(),
        }
    }
}

struct CacheEntry<V> {
    token_counts: Arc<Mutex<broca_core::compaction_v2::tokens::Counts>>,
    position: Position,
    cell: Arc<tokio::sync::OnceCell<Arc<V>>>,
    /// The view's walked size when it loaded, once counted; 0 until then.
    loaded: u64,
    /// What `token_counts` held the last time the entry was counted.
    token_bytes: u64,
    /// What the view counts against the byte cap: its walked size when it loaded, what its reads
    /// have kept since, and its token counts, as of the last time it was served.
    bytes: u64,
    used: u64,
    /// Whether it replaced a view of its session at an older position.
    replaced_older: bool,
    /// Why the view was loaded: the read or check that asked for it first.
    reason: &'static str,
}

/// What a kept view holds, as [`ViewCache`] counts it against its byte cap.
pub trait CachedBytes {
    /// What the view holds when it has just loaded, walked once (see
    /// `broca_session_store::held`).
    fn loaded_bytes(&self) -> u64;
    /// What the view's reads have kept since it loaded, read without walking the view: a view
    /// keeps the sealed entries it reads by id (at most
    /// `broca_session_store::session::SEALED_ENTRIES_MAX_BYTES`), and nothing else grows.
    fn grown_bytes(&self) -> u64;
}

impl<B: broca_session_store::backend::SessionBackend> CachedBytes for SessionView<B> {
    fn loaded_bytes(&self) -> u64 {
        self.held_bytes().total() as u64
    }

    fn grown_bytes(&self) -> u64 {
        self.sealed_entries_bytes() as u64
    }
}

/// A stored view counts as the session view it boxes, with its own few fields (the probed
/// position and the control stream's head) on top.
impl<B: broca_session_store::backend::SessionBackend> CachedBytes
    for crate::store_read::StoredView<B>
{
    fn loaded_bytes(&self) -> u64 {
        std::mem::size_of::<SessionView<B>>() as u64 + SessionView::<B>::loaded_bytes(self)
    }

    fn grown_bytes(&self) -> u64 {
        SessionView::<B>::grown_bytes(self)
    }
}

impl ViewCache {
    /// How many closed sessions' views are kept. A module serves a few sessions at a time that no
    /// actor holds (heads listed by a UI, a session read after its run ended); a session read
    /// again after this many others were read reloads.
    pub const MAX_ENTRIES: usize = 64;

    /// What all kept views hold together, by their walked size (see [`CachedBytes`]): 96 MiB.
    /// A closed session's view held about 3 MB in production (64 views, 177 MB), so this keeps
    /// about 30 of them, and the entry cap binds only for small sessions. A head read of an
    /// unchanged closed session no longer needs its view (see `store_open`'s kept heads), so the
    /// views kept here serve page reads. A view holds its open log's records and the state built
    /// from them (its runs, queued sends and owed spend, among others). A read of a session
    /// whose view isn't kept loads it from storage again, as every read did before views were
    /// kept, and a session whose view alone passes the bound is never kept. A session that long
    /// and in use is open, and its actor answers its reads.
    pub const MAX_BYTES: u64 = 96 * 1024 * 1024;
}

impl<V: CachedBytes> ViewCache<V> {
    /// How many views are kept, and their estimated bytes.
    pub fn usage(&self) -> (usize, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.entries.len(), inner.bytes)
    }

    pub fn new(max_entries: usize, max_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(CacheInner::default()),
            max_entries: max_entries.max(1),
            max_bytes,
        }
    }

    /// The view of the session at `addr` at `position`: the kept one, or the one `load` loads,
    /// which is then kept. Concurrent callers for the same position share one load. A failed load
    /// keeps nothing, and the next caller loads again. `reason` names the read or check asking,
    /// which the memory report counts loads by.
    pub async fn view<E>(
        &self,
        addr: &str,
        position: Position,
        reason: &'static str,
        load: impl std::future::Future<Output = Result<V, E>>,
    ) -> Result<Arc<V>, E> {
        let cell = {
            let mut inner = self.inner.lock().unwrap();
            inner.clock += 1;
            let clock = inner.clock;
            match inner.entries.get_mut(addr) {
                Some(entry) if entry.position == position => {
                    entry.used = clock;
                    let cell = Arc::clone(&entry.cell);
                    // A kept view's reads may have kept sealed entries and token counts since it
                    // was last served. A view whose load filled the cell but whose loader hasn't
                    // counted it yet is counted here, in full, and the loader then counts nothing.
                    if let Some(view) = cell.get() {
                        inner.count(addr, view.as_ref());
                    }
                    cell
                }
                _ => {
                    let replaced_older = inner.entries.contains_key(addr);
                    let cell = Arc::new(tokio::sync::OnceCell::new());
                    let token_counts = inner
                        .entries
                        .get(addr)
                        .map(|entry| Arc::clone(&entry.token_counts))
                        .unwrap_or_default();
                    let replaced = inner.entries.insert(
                        addr.to_owned(),
                        CacheEntry {
                            token_counts,
                            position,
                            cell: Arc::clone(&cell),
                            loaded: 0,
                            token_bytes: 0,
                            bytes: 0,
                            used: clock,
                            replaced_older,
                            reason,
                        },
                    );
                    if let Some(replaced) = replaced {
                        inner.bytes -= replaced.bytes;
                    }
                    cell
                }
            }
        };
        let view = match cell
            .get_or_try_init(|| async { load.await.map(Arc::new) })
            .await
        {
            Ok(view) => view.clone(),
            Err(error) => {
                // Eviction runs only after a load, so an entry left for a failed load would stay
                // past the entry cap: reading many sessions that each fail to load would grow
                // the map without bound.
                let mut inner = self.inner.lock().unwrap();
                let unfilled = inner.entries.get(addr).is_some_and(|entry| {
                    Arc::ptr_eq(&entry.cell, &cell) && entry.cell.get().is_none()
                });
                if unfilled {
                    if let Some(entry) = inner.entries.remove(addr) {
                        inner.bytes -= entry.bytes;
                    }
                }
                return Err(error);
            }
        };
        let mut inner = self.inner.lock().unwrap();
        if inner
            .entries
            .get(addr)
            .is_some_and(|entry| Arc::ptr_eq(&entry.cell, &cell))
        {
            inner.count(addr, view.as_ref());
        }
        for (addr, bytes) in inner.evict(self.max_entries, self.max_bytes) {
            inner.over_cap += 1;
            VIEWS_OVER_CAP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::info!(
                session = %addr,
                counted_bytes = bytes,
                max_bytes = self.max_bytes,
                "a closed session's view is larger than the view cache's byte cap, so it isn't \
                 kept: each read of the session while it is closed loads it again"
            );
        }
        Ok(view)
    }

    /// What the cache holds, with its caps: the bytes it counts against its byte cap (each view's
    /// walked size when it loaded and what its reads kept since, see [`CachedBytes`]), and the
    /// bytes each loaded view holds as `estimate` walks it now.
    pub fn memory(&self, estimate: impl Fn(&V) -> usize) -> broca_wire::MemoryCache {
        let inner = self.inner.lock().unwrap();
        let estimated = inner
            .entries
            .values()
            .filter_map(|entry| Some(estimate(entry.cell.get()?) as u64 + entry.token_bytes))
            .sum::<u64>();
        broca_wire::MemoryCache {
            entries: inner.entries.len() as u64,
            max_entries: self.max_entries as u64,
            counted_bytes: Some(inner.bytes),
            max_bytes: Some(self.max_bytes),
            estimated_bytes: estimated,
            views_over_cap: Some(inner.over_cap),
            loads: Some(inner.loads),
            loaded_bytes: Some(inner.loaded_bytes),
            reloads: Some(inner.reloads),
            loads_by_reason: Some(
                inner
                    .by_reason
                    .iter()
                    .map(|(reason, loads)| ((*reason).to_owned(), loads.clone()))
                    .collect(),
            ),
            top_sessions: Some(inner.top_sessions()),
            stored_reads: stored_reads(),
        }
    }

    /// The view kept for the session at `addr` at `position`, if one is, without loading or
    /// keeping one: for a read whose view is of no use to the next one, which still takes a view
    /// loaded at the same position by an earlier read.
    pub fn kept(&self, addr: &str, position: Position) -> Option<Arc<V>> {
        let mut inner = self.inner.lock().unwrap();
        inner.clock += 1;
        let clock = inner.clock;
        let entry = inner
            .entries
            .get_mut(addr)
            .filter(|entry| entry.position == position)?;
        let view = Arc::clone(entry.cell.get()?);
        entry.used = clock;
        Some(view)
    }

    /// Counts `view` of the session at `addr`, loaded for `reason` and not kept (see
    /// [`kept`](Self::kept)), among the loads the memory report counts, so a load that kept
    /// nothing still shows there.
    pub fn count_unkept(&self, addr: &str, reason: &'static str, view: &V) {
        let loaded = view.loaded_bytes().max(1);
        let mut inner = self.inner.lock().unwrap();
        inner.loads += 1;
        inner.loaded_bytes += loaded;
        let by_reason = inner.by_reason.entry(reason).or_default();
        by_reason.loads += 1;
        by_reason.loaded_bytes += loaded;
        inner.count_session(addr, loaded, false, reason);
    }

    pub(crate) fn token_counts(
        &self,
        addr: &str,
    ) -> Arc<Mutex<broca_core::compaction_v2::tokens::Counts>> {
        self.inner
            .lock()
            .unwrap()
            .entries
            .get(addr)
            .map(|entry| Arc::clone(&entry.token_counts))
            .unwrap_or_default()
    }
}

impl Default for ViewCache {
    fn default() -> Self {
        Self::new(Self::MAX_ENTRIES, Self::MAX_BYTES)
    }
}

impl<V: CachedBytes> CacheInner<V> {
    /// Counts `view`, the loaded view of the entry at `addr`, against the byte cap: in full the
    /// first time, as a load (walked once; at least one byte, so a counted entry is never taken
    /// for an uncounted one), and after that only what its reads kept since. Either the loader
    /// or a reader that found the cell filled first may count it, so the load is counted once
    /// whichever of them takes the lock first.
    fn count(&mut self, addr: &str, view: &V) {
        let Some(entry) = self.entries.get_mut(addr) else {
            return;
        };
        let first = entry.loaded == 0;
        if first {
            entry.loaded = view.loaded_bytes().max(1);
        }
        // Token counts are kept beside the view and grow with each part a read counts. A count
        // in progress holds them locked: keep its last size rather than wait for it under the
        // cache's lock.
        if let Ok(counts) = entry.token_counts.try_lock() {
            entry.token_bytes = counts.held_bytes() as u64;
        }
        let bytes = entry.loaded + view.grown_bytes() + entry.token_bytes;
        let before = std::mem::replace(&mut entry.bytes, bytes);
        let (loaded, reload, reason) = (entry.loaded, entry.replaced_older, entry.reason);
        self.bytes = self.bytes - before + bytes;
        if first {
            self.loads += 1;
            self.loaded_bytes += loaded;
            self.reloads += u64::from(reload);
            let by_reason = self.by_reason.entry(reason).or_default();
            by_reason.loads += 1;
            by_reason.loaded_bytes += loaded;
            by_reason.reloads += u64::from(reload);
            self.count_session(addr, loaded, reload, reason);
        }
    }
}

impl<V> CacheInner<V> {
    /// Counts one load of the session at `addr` (see `by_session`).
    fn count_session(&mut self, addr: &str, loaded: u64, reload: bool, reason: &'static str) {
        if !self.by_session.contains_key(addr) && self.by_session.len() >= SESSIONS_COUNTED {
            let cheapest = self
                .by_session
                .iter()
                .min_by_key(|(_, (loads, _))| loads.loaded_bytes)
                .map(|(addr, _)| addr.clone());
            if let Some(cheapest) = cheapest {
                self.by_session.remove(&cheapest);
            }
        }
        let (loads, latest) = self.by_session.entry(addr.to_owned()).or_default();
        loads.loads += 1;
        loads.loaded_bytes += loaded;
        loads.reloads += u64::from(reload);
        *latest = reason;
    }

    /// The counted sessions whose loads read the most bytes, most first.
    fn top_sessions(&self) -> Vec<broca_wire::SessionViewLoads> {
        let mut top: Vec<_> = self
            .by_session
            .iter()
            .map(|(address, (loads, reason))| broca_wire::SessionViewLoads {
                address: address.clone(),
                loads: loads.loads,
                loaded_bytes: loads.loaded_bytes,
                reloads: loads.reloads,
                reason: (*reason).to_owned(),
            })
            .collect();
        top.sort_by(|a, b| {
            b.loaded_bytes
                .cmp(&a.loaded_bytes)
                .then_with(|| a.address.cmp(&b.address))
        });
        top.truncate(SESSIONS_REPORTED);
        top
    }
    /// Drops views until the cache fits its caps, and returns the views dropped because each
    /// alone was larger than the byte cap, with their counted bytes. Such a view goes first, so
    /// it never pushes the views that fit out of the cache.
    fn evict(&mut self, max_entries: usize, max_bytes: u64) -> Vec<(String, u64)> {
        let over: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.bytes > max_bytes)
            .map(|(addr, _)| addr.clone())
            .collect();
        let mut dropped = Vec::new();
        for addr in over {
            if let Some(entry) = self.entries.remove(&addr) {
                self.bytes -= entry.bytes;
                dropped.push((addr, entry.bytes));
            }
        }
        while self.entries.len() > max_entries || self.bytes > max_bytes {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(addr, _)| addr.clone())
            else {
                return dropped;
            };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.bytes -= entry.bytes;
            }
        }
        dropped
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use broca_session_store::codec::Id128;
    use broca_session_store::session::Position;

    use super::{CachedBytes, ViewCache};

    /// A compaction read sent to storage is named with its method and selector; any other read
    /// by its name alone.
    #[test]
    fn a_stored_compaction_read_names_its_method_and_selector() {
        let compaction = |method: &str, params: &str| super::StoreReadRequest::Compaction {
            key: broca_protocol::BindIdentity {
                project_root: "/p".into(),
                harness: "h".into(),
                session: "s".into(),
            },
            caller: None,
            method: method.into(),
            params: serde_json::value::RawValue::from_string(params.into()).unwrap(),
        };
        assert_eq!(
            super::stored_read_name(&compaction(
                "session.entries",
                r#"{"select": {"by": "after", "mid": "g1"}}"#
            )),
            "compaction session.entries by after"
        );
        assert_eq!(
            super::stored_read_name(&compaction("session.view_state", "{}")),
            "compaction session.view_state"
        );
        assert_eq!(
            super::stored_read_name(&super::StoreReadRequest::Head),
            "session.head"
        );
    }

    /// A test view whose number is its size.
    impl CachedBytes for u64 {
        fn loaded_bytes(&self) -> u64 {
            *self
        }

        fn grown_bytes(&self) -> u64 {
            0
        }
    }

    fn at(version: u64, len: u64) -> Position {
        Position {
            version,
            log: Id128([1; 16]),
            len,
        }
    }

    /// A test view that holds `loaded` bytes when it loads and has kept `grown` since.
    struct Grown {
        loaded: u64,
        grown: u64,
    }

    impl CachedBytes for Grown {
        fn loaded_bytes(&self) -> u64 {
            self.loaded
        }

        fn grown_bytes(&self) -> u64 {
            self.grown
        }
    }

    /// A failed load keeps no entry: reading many sessions that each fail to load leaves the
    /// cache empty, rather than one entry per session past the entry cap.
    #[tokio::test]
    async fn failed_loads_keep_no_entry() {
        let cache: ViewCache<u64> = ViewCache::new(4, u64::MAX);
        for n in 0..16 {
            let failed = cache
                .view(&format!("s{n}"), at(1, 1), "test", async {
                    Err::<u64, ()>(())
                })
                .await;
            assert!(failed.is_err());
        }
        assert_eq!(cache.usage(), (0, 0));
    }

    /// A reader that finds a view loaded before its loader counted it counts the load in full,
    /// once: the view counts its loaded bytes against the cap, not only what it grew since.
    #[tokio::test]
    async fn a_view_found_before_its_loader_counts_it_is_counted_in_full_once() {
        let cache: ViewCache<Grown> = ViewCache::new(4, u64::MAX);
        // The cache as a load leaves it between filling the view's cell and counting the view.
        let first = cache
            .view("s", at(1, 1), "test", async {
                Ok::<_, ()>(Grown {
                    loaded: 100,
                    grown: 5,
                })
            })
            .await
            .unwrap();
        {
            let mut inner = cache.inner.lock().unwrap();
            let entry = inner.entries.get_mut("s").unwrap();
            let bytes = std::mem::take(&mut entry.bytes);
            entry.loaded = 0;
            inner.bytes -= bytes;
            inner.loads = 0;
            inner.loaded_bytes = 0;
            inner.by_reason.clear();
        }
        let found = cache
            .view("s", at(1, 1), "test", async {
                Ok::<_, ()>(Grown {
                    loaded: 1,
                    grown: 1,
                })
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &found));
        assert_eq!(cache.usage(), (1, 105));
        assert_eq!(cache.memory(|_| 0).loads, Some(1));
        assert_eq!(cache.memory(|_| 0).loaded_bytes, Some(100));
        // The loader then takes the lock and counts nothing more.
        cache.inner.lock().unwrap().count("s", &first);
        assert_eq!(cache.usage(), (1, 105));
        assert_eq!(cache.memory(|_| 0).loads, Some(1));
    }

    /// The token counts kept beside a view count against the byte cap, as they grow with the
    /// parts its reads count.
    #[cfg(feature = "test-clock")]
    #[tokio::test]
    async fn a_views_token_counts_count_against_the_byte_cap() {
        let cache: ViewCache<u64> = ViewCache::new(4, u64::MAX);
        let load = |value: u64| async move { Ok::<_, ()>(value) };
        cache.view("s", at(1, 1), "test", load(10)).await.unwrap();
        cache
            .token_counts("s")
            .lock()
            .unwrap()
            .count_parts_for_test(1_000);
        cache.view("s", at(1, 1), "test", load(9)).await.unwrap();
        let (_, bytes) = cache.usage();
        assert!(
            bytes > 10 + 1_000 * 64,
            "a thousand counted parts count against the cap: {bytes} bytes"
        );
        // A view too large with its counts is dropped, counts and all.
        let small: ViewCache<u64> = ViewCache::new(4, 10_000);
        small.view("s", at(1, 1), "test", load(10)).await.unwrap();
        small
            .token_counts("s")
            .lock()
            .unwrap()
            .count_parts_for_test(1_000);
        small.view("s", at(1, 1), "test", load(9)).await.unwrap();
        assert_eq!(small.usage(), (0, 0));
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
                        .view("s", at(1, 10), "test", async {
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
            .view("s", at(1, 10), "test", async {
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
        assert_eq!(
            *cache.view("s", at(1, 10), "test", load(1)).await.unwrap(),
            1
        );
        assert_eq!(
            *cache
                .view("s", at(2, 10), "run.result", load(2))
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            *cache.view("s", at(2, 11), "test", load(3)).await.unwrap(),
            3
        );
        // A reader served the kept view counts no load under its own reason.
        assert_eq!(
            *cache.view("s", at(2, 11), "expiry", load(9)).await.unwrap(),
            3
        );
        assert_eq!(cache.usage().0, 1);
        // A failed load keeps nothing, not even the view it was to replace, and the next reader
        // loads.
        assert!(cache
            .view("s", at(3, 1), "test", async { Err(()) })
            .await
            .is_err());
        assert_eq!(cache.usage(), (0, 0));
        assert_eq!(
            *cache.view("s", at(3, 1), "test", load(4)).await.unwrap(),
            4
        );
        // The memory report counts each load that succeeded and its bytes, and the two that
        // replaced a view of the session at an older position: the last replaced none, the
        // failed load having dropped the view it was to replace.
        let memory = cache.memory(|view| *view as usize);
        assert_eq!(memory.loads, Some(4));
        assert_eq!(memory.loaded_bytes, Some(1 + 2 + 3 + 4));
        assert_eq!(memory.reloads, Some(2));
        // And the same counts by the reason of the reader whose load filled the view.
        let by = |loads, loaded_bytes, reloads| broca_wire::ViewLoads {
            loads,
            loaded_bytes,
            reloads,
        };
        assert_eq!(
            memory.loads_by_reason,
            Some(
                [
                    ("run.result".to_owned(), by(1, 2, 1)),
                    ("test".to_owned(), by(3, 1 + 3 + 4, 1)),
                ]
                .into()
            )
        );
    }

    /// The least recently used view goes first, by count and by estimated bytes.
    #[tokio::test]
    async fn the_cache_stays_within_its_bounds() {
        let load = |value: u64| async move { Ok::<_, ()>(value) };
        let cache: ViewCache<u64> = ViewCache::new(2, u64::MAX);
        cache.view("a", at(1, 1), "test", load(1)).await.unwrap();
        cache.view("b", at(1, 1), "test", load(2)).await.unwrap();
        cache.view("a", at(1, 1), "test", load(9)).await.unwrap();
        cache.view("c", at(1, 1), "test", load(3)).await.unwrap();
        assert_eq!(cache.usage().0, 2);
        // `b` was the least recently used, so it is loaded again.
        assert_eq!(
            *cache.view("b", at(1, 1), "test", load(4)).await.unwrap(),
            4
        );
        assert_eq!(
            *cache.view("c", at(1, 1), "test", load(9)).await.unwrap(),
            3
        );

        let per_view = 500;
        let cache: ViewCache<u64> = ViewCache::new(10, 2 * per_view);
        for name in ["a", "b", "c"] {
            cache
                .view(name, at(1, 100), "test", load(per_view))
                .await
                .unwrap();
        }
        assert_eq!(cache.usage(), (2, 2 * per_view));
    }

    /// A view larger than the byte cap alone isn't kept, never pushes out the views that fit, and
    /// is counted, by the cache and over the process.
    #[tokio::test]
    async fn a_view_over_the_byte_cap_is_counted_and_never_kept() {
        let load = |value: u64| async move { Ok::<_, ()>(value) };
        let cache: ViewCache<u64> = ViewCache::new(10, 1_000);
        cache.view("a", at(1, 1), "test", load(400)).await.unwrap();
        cache.view("b", at(1, 1), "test", load(400)).await.unwrap();
        let before = super::views_over_cap();
        assert_eq!(
            *cache
                .view("big", at(1, 1), "test", load(1_001))
                .await
                .unwrap(),
            1_001
        );
        assert_eq!(cache.usage(), (2, 800));
        assert_eq!(cache.memory(|_| 0).views_over_cap, Some(1));
        assert!(super::views_over_cap() > before);
        // The views that fit are still served without loading.
        assert_eq!(
            *cache.view("a", at(1, 1), "test", load(9)).await.unwrap(),
            400
        );
        assert_eq!(
            *cache.view("b", at(1, 1), "test", load(9)).await.unwrap(),
            400
        );
    }
}
