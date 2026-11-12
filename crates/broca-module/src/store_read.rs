//! Reads of a session kept in the new session storage.
//!
//! A read is served by the session's store actor from the session it holds, or, for a session no
//! actor holds, from a read-only view (`Session::view_at`) kept while the session stays where it
//! was loaded (see `store_reads`). A view never takes the lease or writes, a torn tail reads as
//! not yet written, and a log that needs a recovery refuses until a writer repairs it. Each reply
//! is built from what the session decided when it placed each message (`EntryFacts`) and from its
//! operational fold, never from a replay. A reply here equals the reply today's reader builds from
//! a WAL for the same history, except that it has no WAL lineage, its cursors use the store's
//! own record seqs, and the replies and history version are exposed as `store_exposure` says.

use std::collections::BTreeMap;

use broca_core::{
    RunAttribution, SessionReadLineageState, SessionReadMessage, SessionReadPage,
    SessionReadReason, SessionReadStateKind,
};
use broca_protocol::content::Role;
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::operations::Run;
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError, StoredEntry};

pub mod history_sides;
pub mod todays;

/// Why a store read refused.
#[derive(Debug)]
pub enum StoreReadError {
    /// The session couldn't be read: missing, damaged, or a backend failure.
    Session(SessionError),
    /// A message has no entry facts, so the page can't say what today's read says about it.
    /// Only a session converted from today's storage without them can hold one.
    NoFacts { ordinal: u64 },
    /// A message's entry facts name a value no build of this format writes.
    BadFact { ordinal: u64, field: &'static str },
    /// A run's start, or a setup part it names, couldn't be read back.
    Setup(String),
}

impl From<SessionError> for StoreReadError {
    fn from(error: SessionError) -> Self {
        StoreReadError::Session(error)
    }
}

/// How many messages reads serve of the session's transcript: the placed ones, and the
/// assistant message whose tool calls are still running, at its reserved ordinal, but not the
/// results of a batch that hasn't closed (see [`results_await_close`]).
pub fn transcript_len<B: SessionBackend>(session: &Session<B>) -> u64 {
    session.conversation.placed + u64::from(session.conversation.open_tool_turn().is_some())
        - u64::from(results_await_close(session))
}

/// Whether the newest placed message holds the results of a tool batch that hasn't closed. The
/// session places a turn's results once every call has one, but its batch closes after: as it
/// closes, a delivered note may still join the last result, and the note's facts land with it.
/// Reads serve such results only once the batch is closed, as today's do, so no entry they
/// served changes afterwards; until then the turn reads as still running.
pub fn results_await_close<B: SessionBackend>(session: &Session<B>) -> bool {
    let conversation = &session.conversation;
    let Some(batch) = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.open_batch.as_ref())
    else {
        return false;
    };
    if conversation.open_tool_turn().is_some() {
        return false;
    }
    conversation
        .window_message(conversation.placed)
        .is_some_and(|newest| {
            newest.role == Role::Tool
                && newest.content.iter().all(|block| match block {
                    broca_protocol::ContentBlock::ToolResult(result) => batch
                        .calls
                        .iter()
                        .any(|call| call.id == result.tool_call_id),
                    _ => false,
                })
        })
}

/// Up to `limit` entries from ordinal `from` (counting from 1), as reads serve them: the
/// results of a batch that hasn't closed held back, and the turn they answer served as still
/// running (see [`results_await_close`]).
pub async fn served_entries<B: SessionBackend>(
    session: &Session<B>,
    from: u64,
    limit: usize,
) -> Result<Vec<StoredEntry>, SessionError> {
    let entries = session.entries(from, limit).await?;
    Ok(held_back(session, entries))
}

/// [`served_entries`] as compaction reads them (`Session::compaction_entries`): a session that
/// enrolled in the current compaction contract serves the messages placed before it as the
/// enrollment decided, the global system messages it suppressed as entries with no parts, at
/// their own ordinals. What `session.entries` serves and a view is checked against.
pub async fn served_compaction_entries<B: SessionBackend>(
    session: &Session<B>,
    from: u64,
    limit: usize,
) -> Result<Vec<StoredEntry>, SessionError> {
    let entries = session.compaction_entries(from, limit).await?;
    Ok(held_back(session, entries))
}

/// The zero-based ordinals of every assistant message of `session` whose entry facts say its run
/// cleans replies (`EntryFacts::clean_reply`): the messages every read serves cleaned of tag
/// notation (`crate::store_exposure::served_message`). Reads the whole transcript, a page at a
/// time; for the converter's checks, which compare whole sessions.
pub async fn clean_reply_ordinals<B: SessionBackend>(
    session: &Session<B>,
) -> Result<std::collections::BTreeSet<u64>, SessionError> {
    const PAGE: usize = 256;
    let len = transcript_len(session);
    let mut ordinals = std::collections::BTreeSet::new();
    let mut from = 1;
    while from <= len {
        let entries = served_entries(session, from, PAGE).await?;
        let Some(last) = entries.last().map(|entry| entry.ordinal) else {
            break;
        };
        ordinals.extend(
            entries
                .iter()
                .filter(|entry| {
                    entry.message.role == Role::Assistant
                        && entry
                            .state
                            .facts
                            .as_ref()
                            .is_some_and(|facts| facts.clean_reply)
                })
                .map(|entry| entry.ordinal - 1),
        );
        from = last + 1;
    }
    Ok(ordinals)
}

/// `entries` with the results of a batch that hasn't closed held back, and the turn they answer
/// served as still running (see [`results_await_close`]).
fn held_back<B: SessionBackend>(
    session: &Session<B>,
    mut entries: Vec<StoredEntry>,
) -> Vec<StoredEntry> {
    if results_await_close(session) {
        let held = session.conversation.placed;
        entries.retain(|entry| entry.ordinal < held);
        if let Some(turn) = entries.iter_mut().find(|entry| entry.ordinal + 1 == held) {
            turn.next = None;
            turn.open = true;
        }
    }
    entries
}

/// A `session.read` page, each of its messages' stable id (`EntryFacts::mid`), in order, and
/// what an `include_display_facts` read serves about each message, by zero-based ordinal.
pub struct StorePage {
    pub page: SessionReadPage,
    pub mids: Vec<String>,
    /// What each message's tool calls are served from, in order (see
    /// `crate::store_calls::attribute_tool_calls`).
    pub turns: Vec<crate::store_calls::TurnOf>,
    pub facts: BTreeMap<u64, broca_core::display_facts::DisplayFacts>,
    /// The zero-based ordinals of the messages served without display facts
    /// (`EntryFacts::no_display_facts`).
    pub withheld: std::collections::BTreeSet<u64>,
    /// The reasoning summaries each message's entry facts recorded, by zero-based ordinal.
    pub summaries: crate::serve::RecordedSummaries,
    /// The Rich UI stamp of each run whose assistant messages the page holds, as their entry
    /// facts recorded it.
    pub stamps: BTreeMap<String, Option<broca_protocol::RichUiStamp>>,
    /// For each message a run numbered under its computed part-number rules, by zero-based
    /// ordinal: that run's id and the number before the message's first part
    /// (`EntryFacts::runner_tag_base`).
    pub numbering: BTreeMap<u64, (String, u64)>,
    /// The zero-based ordinals of the page's assistant messages whose entry facts say their run
    /// cleans replies (`EntryFacts::clean_reply`), which a served read shows cleaned
    /// (`crate::store_exposure::served_message`).
    pub clean_replies: std::collections::BTreeSet<u64>,
}

/// A `session.read` page: up to `limit` messages from `from_ordinal` (counting from 0, as
/// today's page builder takes it), or the newest `limit` without one. Its dispatch facts, head
/// cursor and lineage id are left to the caller.
pub async fn read_page<B: SessionBackend>(
    session: &Session<B>,
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> Result<StorePage, StoreReadError> {
    read_page_with_facts(session, from_ordinal, limit, true).await
}

async fn read_page_with_facts<B: SessionBackend>(
    session: &Session<B>,
    from_ordinal: Option<u64>,
    limit: Option<u64>,
    include_display_facts: bool,
) -> Result<StorePage, StoreReadError> {
    let capped_limit = limit
        .unwrap_or(broca_core::SESSION_READ_DEFAULT_LIMIT)
        .clamp(1, broca_core::SESSION_READ_MAX_LIMIT);
    let prompt_len = transcript_len(session);
    let start = match from_ordinal {
        Some(from) => from.min(prompt_len),
        None => prompt_len.saturating_sub(capped_limit),
    };
    let end = start.saturating_add(capped_limit).min(prompt_len);
    // One entry past the page, to tell whether the page's last message ends its run.
    let wanted = usize::try_from(end - start)
        .unwrap_or(usize::MAX)
        .saturating_add(1);
    let mut entries = served_entries(session, start + 1, wanted).await?;
    let after = (entries.len() as u64 > end - start)
        .then(|| entries.pop())
        .flatten();
    let latest = session.operations.run.as_ref();
    let send_ids = if include_display_facts {
        session.entry_send_ids(&entries).await?
    } else {
        BTreeMap::new()
    };
    let mut messages = Vec::with_capacity(entries.len());
    let mut mids = Vec::with_capacity(entries.len());
    let mut turns = Vec::with_capacity(entries.len());
    let mut facts = BTreeMap::new();
    let mut withheld = std::collections::BTreeSet::new();
    let mut summaries = crate::serve::RecordedSummaries::new();
    let mut stamps = BTreeMap::new();
    let mut numbering = BTreeMap::new();
    let mut clean_replies = std::collections::BTreeSet::new();
    for (index, entry) in entries.iter().enumerate() {
        let next = entries.get(index + 1).or(after.as_ref());
        messages.push(page_message(entry, next, latest)?);
        turns.push(crate::store_calls::TurnOf::of(entry));
        let mut served = display_facts(entry).served();
        if let Some(id) = send_ids.get(&entry.ordinal) {
            served.send_id = id.clone();
        }
        served.notices.extend(
            imported_notices(
                session,
                entry,
                index.checked_sub(1).and_then(|at| entries.get(at)),
            )
            .await?
            .into_iter()
            .map(|fact| broca_core::projection::ProjectedNotice {
                fact,
                submission_id: String::new(),
            }),
        );
        // A turn whose results are placed has its facts, though reads still serve it as running
        // until its batch closes (`results_await_close`).
        if entry.open && entry.state.turn.is_none() {
            served.tool_times = open_turn_times(session, &entry.message).await?;
        }
        facts.insert(entry.ordinal - 1, served);
        if let Some(facts) = &entry.state.facts {
            if facts.no_display_facts == Some(true) {
                withheld.insert(entry.ordinal - 1);
            }
            if let (Some(run), Some(base)) = (&facts.run_id, facts.runner_tag_base) {
                numbering.insert(entry.ordinal - 1, (run.clone(), base));
            }
            if facts.clean_reply && entry.message.role == Role::Assistant {
                clean_replies.insert(entry.ordinal - 1);
            }
            if let (Some(run), Role::Assistant) = (&facts.run_id, entry.message.role) {
                let stamp = facts.rich_ui.as_ref().and_then(|stamp| {
                    serde_json::from_value(serde_json::json!({
                        "dialect": stamp.dialect,
                        "catalog_digest": stamp.catalog_digest,
                    }))
                    .ok()
                });
                stamps.entry(run.clone()).or_insert(stamp);
            }
            summaries.insert(
                entry.ordinal - 1,
                facts
                    .reasoning_summaries
                    .iter()
                    .map(|summary| {
                        cortexkit_role_llm_runner::read::ReasoningSummary::new(
                            summary.part,
                            summary.text.clone(),
                        )
                    })
                    .collect(),
            );
        }
        // `page_message` has refused an entry without facts.
        mids.push(
            entry
                .state
                .facts
                .as_ref()
                .map(|facts| facts.mid.clone())
                .unwrap_or_default(),
        );
    }
    let next_from_ordinal = (end < prompt_len).then_some(end);
    let page = SessionReadPage {
        messages,
        next_from_ordinal,
        head: None,
        lineage_id: None,
        lineage_state: lineage_state(session),
        prompt_len,
        deferred_calls: None,
        late_approvals: Vec::new(),
        withdrawals_owed: false,
        tags_await_setup: false,
    };
    Ok(StorePage {
        page,
        mids,
        turns,
        facts,
        withheld,
        summaries,
        stamps,
        numbering,
        clean_replies,
    })
}

/// Imports keep riding sends beside the assistant call. Readers attach those notices to
/// the corresponding result part, including when a page starts at the results message.
async fn imported_notices<B: SessionBackend>(
    session: &Session<B>,
    entry: &StoredEntry,
    previous: Option<&StoredEntry>,
) -> Result<Vec<cortexkit_role_llm_runner::read::NoticeFact>, SessionError> {
    if entry.message.role != Role::Tool
        || entry
            .state
            .facts
            .as_ref()
            .is_some_and(|facts| facts.run_id.is_some())
        || entry.ordinal <= 1
    {
        return Ok(Vec::new());
    }
    let loaded;
    let previous = match previous {
        Some(previous) => previous,
        None => {
            loaded = session.entries(entry.ordinal - 1, 1).await?;
            let Some(previous) = loaded.first() else {
                return Ok(Vec::new());
            };
            previous
        }
    };
    let mut notices = Vec::new();
    for delivered in &previous.state.delivered {
        let Some(broca_protocol::ContentBlock::ToolCall(tool)) =
            previous.message.content.get(delivered.part as usize)
        else {
            continue;
        };
        let Some(call) = previous
            .state
            .turn
            .iter()
            .flat_map(|turn| &turn.calls)
            .find(|call| call.tool_call_id == tool.tool_call_id && call.carrier.is_some())
        else {
            continue;
        };
        let Some(part) = entry.message.content.iter().position(|block| {
            matches!(block, broca_protocol::ContentBlock::ToolResult(result) if result.tool_call_id == tool.tool_call_id)
        }) else {
            continue;
        };
        if let (Some(send_id), Some(notice)) = (&delivered.send_id, &delivered.notice) {
            let mut fact = cortexkit_role_llm_runner::read::NoticeFact::new(
                part as u32,
                send_id,
                &notice.sender,
                &delivered.text,
            )
            .with_call_key(&call.call_key);
            fact.call_carrier = call.carrier.clone();
            notices.push(fact);
        }
    }
    Ok(notices)
}

/// Serves `session.read` for a session kept in the new storage. The reply is today's for the
/// same history, built from the session's head, its folds and its entry facts, never from a WAL.
///
/// A late-approval cursor is refused by name: the new storage takes no approval questions, so no
/// cursor names one.
pub async fn serve_read(
    registry: &crate::LineageRegistry,
    store: &crate::store_open::StoreRuntime,
    req: crate::serve::SessionReadRequest,
) -> Result<broca_wire::SessionReadResponse, crate::serve::ServeError> {
    let _timing = broca_session_store::metrics::Phase::Read
        .start()
        .with_session(|| Some(broca_core::session_addr(&req.session)));
    use crate::serve::ServeError;
    use crate::store_reads::{PageRequest, StoreReadReply, StoreReadRequest};
    if let Some(cursor) = req.late_after.as_deref() {
        let refusal = broca_wire::InvalidLateCursor::new(cursor);
        return Err(ServeError::InvalidParams {
            field: refusal.detail.field,
            message: refusal.message,
        });
    }
    let request = StoreReadRequest::Page(PageRequest::of(&req));
    let answer = match store.read(registry, &req.session, request).await? {
        Some(StoreReadReply::Page(answer)) => answer?,
        Some(_) => return Err(crate::store_reads::mismatched()),
        None => {
            // The session's creation never committed, so it holds nothing: an empty transcript.
            let mut response = crate::serve::session_read_response(empty_page(), None);
            response.incarnation = "unwritten".into();
            return Ok(response);
        }
    };
    finish(answer, &req)
}

/// The `session.read` reply for `req` from `session`, loaded by the caller, reading its run
/// starts' setup parts from `parts`: what [`serve_read`] serves once it has the session, built
/// by the same [`page`] the session's actor runs.
pub async fn serve_view<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    req: &crate::serve::SessionReadRequest,
) -> Result<broca_wire::SessionReadResponse, crate::serve::ServeError> {
    let answer = page(
        session,
        parts,
        &crate::store_reads::PageRequest::of(req),
        None,
    )
    .await?;
    finish(answer, req)
}

/// Repeated reads of one unchanged session, sharing its control-stream head. The borrowed
/// session keeps the records its loader retained; sealed history may still read storage.
pub struct ViewReader<'a, B: SessionBackend> {
    session: &'a Session<B>,
    parts: &'a dyn Parts,
    stream_head: ViewHead,
}

/// An immutable stored position and its control-stream head share one cache lifetime.
pub struct StoredView<B: SessionBackend> {
    view: Box<broca_session_store::session::SessionView<B>>,
    stream_head: ViewHead,
    position: broca_session_store::session::Position,
}

/// A local stored snapshot shared by expiry, recovery and page serving.
pub type StoredLocalView = std::sync::Arc<StoredView<broca_session_store::backend::LocalBackend>>;

impl<B: SessionBackend> StoredView<B> {
    pub async fn new(
        view: Box<broca_session_store::session::SessionView<B>>,
        position: broca_session_store::session::Position,
    ) -> Self {
        // Head projection walks records through several large folds. Keep that future off
        // the caller's stack while a stored read is also loading its view.
        let stream_head = Box::pin(ViewHead::new(&view)).await;
        Self {
            view,
            stream_head,
            position,
        }
    }

    /// The probed byte position, including any harmless tail the decoder did not fold.
    pub fn position(&self) -> broca_session_store::session::Position {
        self.position
    }

    pub(crate) fn stream_head(
        &self,
    ) -> Result<Option<broca_core::Cursor>, crate::serve::ServeError> {
        self.stream_head.get()
    }
}

impl<B: SessionBackend> std::ops::Deref for StoredView<B> {
    type Target = broca_session_store::session::SessionView<B>;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

struct ViewHead(Result<Option<broca_core::Cursor>, String>);

impl ViewHead {
    async fn new<B: SessionBackend>(session: &Session<B>) -> Self {
        Self(crate::store_fanout::last_cursor(session).await)
    }

    fn get(&self) -> Result<Option<broca_core::Cursor>, crate::serve::ServeError> {
        self.0.as_ref().copied().map_err(|error| {
            crate::serve::ServeError::Open(
                format!("the session's control stream can't be projected: {error}"),
                Some(broca_protocol::ErrorClass::Permanent),
            )
        })
    }
}

impl<'a, B: SessionBackend> ViewReader<'a, B> {
    /// Projects the same head as [`serve_view`] once, before walking the session's pages.
    /// The session must remain unchanged for the lifetime of the reader.
    pub async fn new(
        session: &'a Session<B>,
        parts: &'a dyn Parts,
    ) -> Result<Self, crate::serve::ServeError> {
        let stream_head = ViewHead::new(session).await;
        stream_head.get()?;
        Ok(Self {
            session,
            parts,
            stream_head,
        })
    }

    /// The real page builder and reply finishing used by [`serve_view`], with the shared head.
    pub async fn read(
        &self,
        req: &crate::serve::SessionReadRequest,
    ) -> Result<broca_wire::SessionReadResponse, crate::serve::ServeError> {
        let answer = page(
            self.session,
            self.parts,
            &crate::store_reads::PageRequest::of(req),
            Some(self.stream_head.get()?),
        )
        .await?;
        finish(answer, req)
    }
}

/// What a reply takes besides the session: the media mode and the byte cap `req` asks for.
fn finish(
    answer: crate::store_reads::PageAnswer,
    req: &crate::serve::SessionReadRequest,
) -> Result<broca_wire::SessionReadResponse, crate::serve::ServeError> {
    let mut response = answer.response;
    crate::serve::apply_media_mode(&mut response, req.media, &req.session.session)?;
    let cap = req
        .max_bytes
        .unwrap_or(broca_wire::READ_DEFAULT_MAX_BYTES)
        .min(broca_wire::READ_MAXIMUM_MAX_BYTES);
    crate::serve::cap_page_bytes(
        &mut response,
        cap,
        answer.from_ordinal.is_none(),
        &req.session.session,
    )?;
    Ok(response)
}

/// The `session.read` reply `request` asks of `session`, as far as the session decides it: the
/// page, from the message `after_mid` names or the ordinal asked for, refused when the session's
/// history version isn't the one asked for (the version the llm-runner surface serves,
/// `store_exposure::served_history_version`); each message's part numbers attached as its run's
/// tag rules number it, and each model-written message marked for reply cleaning by them
/// (`store_entries::attach_part_tags`); each reply whose entry facts say its run cleans replies
/// served cleaned (`store_exposure::served_message`); each message's tool calls attributed from
/// its turn facts; and what
/// each opt-in adds, from the page's own entry and turn facts and the session's folds. The
/// session's actor runs this over the session it holds, so all of it reads from memory or none of
/// it is served there. What doesn't depend on the session (media mode, the byte cap) is the
/// caller's.
pub(crate) async fn page<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn broca_session_loop::parts::Parts,
    request: &crate::store_reads::PageRequest,
    stream_head: Option<Option<broca_core::Cursor>>,
) -> Result<crate::store_reads::PageAnswer, crate::serve::ServeError> {
    use crate::serve::ServeError;
    let head = session.head();
    let served_version = crate::store_exposure::served_history_version(head.history_version);
    if request
        .history_version
        .is_some_and(|requested| requested != served_version)
    {
        return Err(ServeError::HistoryChanged);
    }
    let from_ordinal = match &request.after_mid {
        Some(mid) => match session.find_message(mid).await.map_err(read_failed)? {
            // The 1-based ordinal of the message named, which is the 0-based start after it.
            Some(ordinal) => Some(ordinal),
            None => return Err(ServeError::UnknownMid(mid.clone())),
        },
        None => request.from_ordinal.map(|at| at.saturating_sub(1)),
    };
    let StorePage {
        mut page,
        mids,
        turns,
        facts,
        withheld,
        summaries,
        stamps,
        numbering,
        clean_replies,
    } = read_page_with_facts(
        session,
        from_ordinal,
        request.limit,
        request.include_display_facts,
    )
    .await
    .map_err(refused)?;
    crate::store_entries::attach_part_tags(session, parts, &mut page.messages, &numbering)
        .await
        .map_err(|error| {
            // A backend failure and a damaged start read alike here, so no class is claimed.
            ServeError::Open(
                format!("the session's run settings can't be read: {error}"),
                None,
            )
        })?;
    // Numbered first, from the stored text, so cleaning changes no part's number.
    for message in &mut page.messages {
        if clean_replies.contains(&message.ordinal) {
            let served = crate::store_exposure::served_message(&message.message, true);
            if let std::borrow::Cow::Owned(served) = served {
                message.message = served;
            }
        }
    }
    let times = request
        .include_display_facts
        .then(|| crate::serve::DisplayTimes::of(&page));
    let tools = if request.include_tools {
        latest_tools(session, parts).map_err(refused)?
    } else {
        None
    };
    let mut response = crate::serve::session_read_response(page, tools);
    response.history_version = served_version;
    response.incarnation = broca_session_store::codec::hex(&head.incarnation.0);
    // The cursor at the read's durable end, which a subscribe resumes from with nothing missed
    // or repeated. Actors provide their maintained cursor; stored views share one projection.
    // A caller without either projects the retained log, or reads storage when needed.
    let stream_head = match stream_head {
        Some(known) => known,
        None => ViewHead::new(session).await.get()?,
    };
    response.head = stream_head.map(broca_wire::CursorWire::from);
    for (entry, mid) in response.messages.iter_mut().zip(mids) {
        entry.mid = mid;
    }
    if let Some(times) = &times {
        // A message served without display facts keeps the form it has before they are added.
        let before: Vec<_> = response
            .messages
            .iter()
            .filter(|entry| withheld.contains(&(entry.ordinal - 1)))
            .cloned()
            .collect();
        crate::serve::apply_display_facts(&mut response, times, &facts, Some(&summaries));
        for original in before {
            if let Some(entry) = response
                .messages
                .iter_mut()
                .find(|entry| entry.ordinal == original.ordinal)
            {
                *entry = original;
            }
        }
    }
    if request.include_rich_ui {
        crate::serve::apply_rich_ui(&mut response, &stamps);
    }
    crate::store_calls::attribute_tool_calls(session, &mut response, &turns)
        .await
        .map_err(read_failed)?;
    Ok(crate::store_reads::PageAnswer {
        response,
        from_ordinal,
    })
}

/// What an `include_display_facts` read serves about each tool call of `message`, the
/// assistant message whose calls are still running: its tool turn has no facts until its last
/// call is answered, so each call is served from the open batch the operational fold keeps,
/// with the key, dispatch time and schema pin its intent recorded (found through the session's
/// id lookup, as the run loop finds it, and as `store_calls` attributes the same call). A call
/// answered without an intent is named with the key its outcome recorded, and one without
/// either with none. The open batch doesn't keep a call's finish time or why it has no answer,
/// so those wait for the turn's facts.
async fn open_turn_times<B: SessionBackend>(
    session: &Session<B>,
    message: &broca_protocol::Message,
) -> Result<Vec<cortexkit_role_llm_runner::read::ToolTime>, StoreReadError> {
    use cortexkit_role_llm_runner::read::ToolTime;
    let batch = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.open_batch.as_ref());
    let mut copies: BTreeMap<&str, usize> = BTreeMap::new();
    let mut times = Vec::new();
    for block in &message.content {
        let broca_protocol::ContentBlock::ToolCall(call) = block else {
            continue;
        };
        let copy = copies.entry(call.tool_call_id.0.as_str()).or_default();
        let position = batch.and_then(|batch| {
            (0..batch.calls.len())
                .filter(|position| batch.id(*position) == &call.tool_call_id)
                .nth(*copy)
        });
        *copy += 1;
        let mut time = ToolTime::new(&call.tool_call_id.0);
        // The fold marks every copy of an id with the intent that anchored its first copy
        // (`store_calls::open_call`): only that copy is the intent's.
        let (key, dispatched) = batch
            .zip(position)
            .map_or((None, false), |(batch, position)| {
                let first = (0..batch.calls.len())
                    .find(|at| batch.id(*at) == &call.tool_call_id && batch.calls[*at].intent);
                let own = &batch.calls[position];
                match first == Some(position) {
                    true => (batch.key(position), true),
                    false => (own.key.as_deref().filter(|_| !own.intent), false),
                }
            });
        if let (Some(key), false) = (key, dispatched) {
            time = time.with_call_key(key);
        } else if let Some(key) = key {
            let intent = session
                .find_id(broca_session_store::idindex::IdKind::CallKey, key)
                .await?
                .into_iter()
                .find_map(|(_, record)| match record {
                    Record::ToolIntent(intent) => Some(intent),
                    _ => None,
                })
                .ok_or_else(|| {
                    StoreReadError::Setup(format!("no dispatch intent records the call key {key}"))
                })?;
            time = time.with_call_key(key);
            time.started_at = intent.dispatched_at_ms;
            if let Some(pin) = intent.schema_pin.as_ref().and_then(|pin| {
                broca_session_loop::entries::served_schema_pin(
                    intent.tool_name.as_deref().unwrap_or(&call.tool_name),
                    pin,
                )
            }) {
                time = time.with_schema_pin(pin);
            }
        }
        times.push(time);
    }
    Ok(times)
}

/// The page of a session that holds nothing.
fn empty_page() -> SessionReadPage {
    SessionReadPage {
        messages: Vec::new(),
        next_from_ordinal: None,
        head: None,
        lineage_id: None,
        lineage_state: SessionReadLineageState {
            last_run_id: None,
            state: SessionReadStateKind::Idle,
            reason: None,
            error: None,
            usage: None,
        },
        prompt_len: 0,
        deferred_calls: None,
        late_approvals: Vec::new(),
        withdrawals_owed: false,
        tags_await_setup: false,
    }
}

/// A store read that failed. Only a backend failure is worth a retry: anything else is the
/// session's stored bytes or state being wrong, which reading again won't change.
pub(crate) fn read_failed(error: SessionError) -> crate::serve::ServeError {
    let class = match error {
        SessionError::Backend(_) => broca_protocol::ErrorClass::Transient,
        _ => broca_protocol::ErrorClass::Permanent,
    };
    crate::serve::ServeError::Open(
        format!("the session could not be read from the new storage: {error:?}"),
        Some(class),
    )
}

/// The serve error for a store read that refused.
fn refused(error: StoreReadError) -> crate::serve::ServeError {
    match error {
        StoreReadError::Session(error) => read_failed(error),
        other => crate::serve::ServeError::Open(
            format!("the session's stored facts can't be read: {other:?}"),
            Some(broca_protocol::ErrorClass::Permanent),
        ),
    }
}

/// The tool list an `include_tools` read serves: the one the session's latest run was frozen
/// with, which its start names as a setup part, with that run's id. `None` before any run.
/// A resumed run continues its run without a new start, so the latest start is the current
/// episode's, as today's read takes the last `RunStarted`.
pub fn latest_tools<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
) -> Result<Option<crate::registry::SessionReadTools>, StoreReadError> {
    let Some(run) = session.operations.run.as_ref() else {
        return Ok(None);
    };
    let start = run
        .start
        .as_ref()
        .ok_or_else(|| StoreReadError::Setup(format!("run {} has no start", run.run_id)))?;
    let (config, _) = broca_session_loop::run::run_config_members(
        start,
        |part| broca_session_loop::parts::value(parts, part).map_err(StoreReadError::Setup),
        || StoreReadError::Setup("a run's frozen choices aren't an object".into()),
    )?;
    let tools: Vec<broca_protocol::request::Tool> = match config.get("tools") {
        Some(tools) => serde_json::from_value(tools.clone())
            .map_err(|error| StoreReadError::Setup(format!("tools part: {error}")))?,
        None => Vec::new(),
    };
    Ok(Some(crate::registry::SessionReadTools {
        run_id: broca_protocol::RunId(start.run_id.clone()),
        tools: std::sync::Arc::new(tools),
    }))
}

/// One page message from its entry. `next` is the entry after it, if any; `latest` is the
/// session's latest run.
fn page_message(
    entry: &StoredEntry,
    next: Option<&StoredEntry>,
    latest: Option<&Run>,
) -> Result<SessionReadMessage, StoreReadError> {
    let facts = entry.state.facts.as_ref().ok_or(StoreReadError::NoFacts {
        ordinal: entry.ordinal,
    })?;
    let ordinal = entry.ordinal - 1;
    let mut message = entry.message.clone();
    message.ordinal = Some(ordinal);
    let completed_at = broca_core::compaction_v2::entries::project_completion(
        &message,
        entry.next.as_ref(),
        facts
            .written_at_ms
            .filter(|_| facts.native && message.role == Role::Assistant),
    );
    let delivery = delivery(facts)?;
    let run = facts.run_id.as_ref().map(|run_id| {
        let next_run = next.and_then(|next| next.state.facts.as_ref()?.run_id.as_ref());
        // A run's messages are contiguous, so its last one is the one the next message doesn't
        // share. It is final once the run has ended: every run before the latest one has, since
        // a run starts only after the one before it ends.
        let ended = match latest {
            Some(run) if &run.run_id == run_id => run.finished.is_some(),
            _ => true,
        };
        RunAttribution {
            run_id: run_id.clone(),
            episode: run_id.clone(),
            is_final: next_run != Some(run_id) && ended,
            delivery: delivery.clone(),
        }
    });
    Ok(SessionReadMessage {
        created_at: facts.created_at_ms,
        written_at: facts.written_at_ms,
        completed_at,
        ordinal,
        message,
        run,
    })
}

/// How a delivered send reached the message, from its entry facts.
fn delivery(
    facts: &broca_session_store::record::EntryFacts,
) -> Result<Option<broca_protocol::tags::DeliveryAttribution>, StoreReadError> {
    let Some(mode) = facts.delivery_mode.clone() else {
        return Ok(None);
    };
    let mode = serde_json::from_value(serde_json::Value::String(mode)).map_err(|_| {
        StoreReadError::BadFact {
            ordinal: facts.ordinal,
            field: "delivery_mode",
        }
    })?;
    Ok(Some(broca_protocol::tags::DeliveryAttribution {
        mode,
        tool_call_id: facts.delivery_tool_call_id.clone(),
    }))
}

/// The session's usage by model, as `session.head` serves it, from the fold that sums each
/// step's model shares. `None` until a step counted.
pub fn session_usage(
    fold: Option<&broca_session_store::fold::operations::SessionUsage>,
) -> Option<cortexkit_role_llm_runner::usage::SessionUsage> {
    use broca_session_store::fold::operations::UsageTotals;
    use cortexkit_role_llm_runner::usage::{ModelUsage, RunUsage, SessionUsage};
    let fold = fold?;
    fn counts(totals: &UsageTotals) -> [Option<u64>; 5] {
        [
            totals.input_tokens,
            totals.cached_input_tokens,
            totals.cache_write_tokens,
            totals.output_tokens,
            totals.reasoning_tokens,
        ]
    }
    let mut run = RunUsage::new();
    run.steps = fold.totals.steps;
    run.unreported_steps = fold.totals.unreported_steps;
    [
        run.input_tokens,
        run.cached_input_tokens,
        run.cache_write_tokens,
        run.output_tokens,
        run.reasoning_tokens,
    ] = counts(&fold.totals);
    run.by_model = fold
        .by_model
        .iter()
        .map(|entry| {
            let mut model = ModelUsage::new(&entry.provider, &entry.model);
            if let Some(variant) = &entry.variant {
                model = model.with_variant(variant);
            }
            model.steps = entry.totals.steps;
            model.unreported_steps = entry.totals.unreported_steps;
            [
                model.input_tokens,
                model.cached_input_tokens,
                model.cache_write_tokens,
                model.output_tokens,
                model.reasoning_tokens,
            ] = counts(&entry.totals);
            model
        })
        .collect();
    let mut usage = SessionUsage::new(&fold.since);
    usage.totals = run;
    Some(usage)
}

/// The session's lifecycle beside a page: its latest run's state, and for an ended or paused
/// run the error and usage its end or pause recorded, which the operational fold keeps.
fn lineage_state<B: SessionBackend>(session: &Session<B>) -> SessionReadLineageState {
    let Some(run) = session.operations.run.as_ref() else {
        return SessionReadLineageState {
            last_run_id: None,
            state: SessionReadStateKind::Idle,
            reason: None,
            error: None,
            usage: None,
        };
    };
    let last_run_id = Some(broca_protocol::RunId(run.run_id.clone()));
    let error = || {
        run.end_error
            .clone()
            .and_then(|error| serde_json::from_value(error).ok())
    };
    match (run.finished, run.paused) {
        (Some(reason), _) => SessionReadLineageState {
            last_run_id,
            state: broca_core::state_for_terminal(reason),
            reason: Some(SessionReadReason::Terminal(reason)),
            error: error(),
            usage: run.end_usage.filter(|_| run.completed_steps > 0),
        },
        (None, Some(reason)) => SessionReadLineageState {
            last_run_id,
            state: SessionReadStateKind::Paused,
            reason: Some(SessionReadReason::Pause(reason)),
            error: error(),
            usage: None,
        },
        (None, None) => SessionReadLineageState {
            last_run_id,
            state: SessionReadStateKind::Active,
            reason: None,
            error: None,
            usage: None,
        },
    }
}

/// What `session.read` serves about a message besides its page entry, from the facts the
/// session recorded when it placed the message: the send's id and marks for a user message, the
/// attested sender of whatever message a send placed, each tool call's key, and the text
/// delivered with the message's tool calls.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DisplayFacts {
    /// The caller-visible id of the send that placed this message, when known.
    pub send_id: Option<String>,
    /// For a user message: the `client_message_id` of the send that placed it.
    pub client_message_id: Option<String>,
    /// The attested module id of the sender of the send that placed the message: every message
    /// of a run's input or of a steer, as today's read names it.
    pub sender: Option<String>,
    /// The send's `speaks_for` mark, as stored.
    pub speaks_for: Option<serde_json::Value>,
    /// The send's positive notice mark.
    pub notice: bool,
    /// One per tool call the message made, in call order.
    pub calls: Vec<CallDisplay>,
    /// Text delivered with the message's tool calls, in the order it was delivered.
    pub notices: Vec<NoticeDisplay>,
    /// For an assistant message whose tool turn has facts: one per tool call it made, in call
    /// order, as reads serve them (see [`turn_times`]). Empty otherwise.
    pub tool_times: Vec<cortexkit_role_llm_runner::read::ToolTime>,
    /// For a tool results message: each send that rode on one of its results with its envelope,
    /// in the order delivered, as its facts recorded it when the ride landed
    /// (`EntryFacts::notices`). These are the notices `session.read` serves.
    pub ride_notices: Vec<cortexkit_role_llm_runner::read::NoticeFact>,
}

/// A tool call's identity, as reads serve it.
#[derive(Debug, Clone, PartialEq)]
pub struct CallDisplay {
    pub tool_call_id: String,
    pub call_key: String,
    /// The runner that minted `call_key`, when it wasn't broca: a key is unique only within the
    /// runner that minted it.
    pub call_carrier: Option<String>,
    pub schema_pin: Option<serde_json::Value>,
}

/// Text delivered with one of a message's tool calls.
#[derive(Debug, Clone, PartialEq)]
pub struct NoticeDisplay {
    /// The tool call's position among the message's content blocks.
    pub part: u32,
    pub call_key: Option<String>,
    pub send_id: Option<String>,
    pub sender: Option<String>,
    pub text: String,
}

/// The display facts of `entry`.
pub fn display_facts(entry: &StoredEntry) -> DisplayFacts {
    let user = entry.message.role == Role::User;
    let facts = entry.state.facts.as_ref();
    let marks = facts.and_then(|facts| facts.marks.as_ref());
    let calls: Vec<CallDisplay> = entry
        .state
        .turn
        .iter()
        .flat_map(|turn| &turn.calls)
        .map(|call| CallDisplay {
            tool_call_id: call.tool_call_id.0.clone(),
            call_key: call.call_key.clone(),
            call_carrier: call.carrier.clone(),
            schema_pin: call.schema_pin.clone(),
        })
        .collect();
    let key_at = |part: u32| {
        let block = entry.message.content.get(part as usize)?;
        let broca_protocol::ContentBlock::ToolCall(call) = block else {
            return None;
        };
        calls
            .iter()
            .find(|known| known.tool_call_id == call.tool_call_id.0)
            .map(|known| known.call_key.clone())
    };
    let notices = entry
        .state
        .delivered
        .iter()
        .map(|delivered| NoticeDisplay {
            part: delivered.part,
            call_key: key_at(delivered.part),
            send_id: delivered.send_id.clone(),
            sender: delivered
                .notice
                .as_ref()
                .map(|notice| notice.sender.clone()),
            text: delivered.text.clone(),
        })
        .collect();
    DisplayFacts {
        send_id: facts.and_then(|facts| facts.send_id.clone()),
        client_message_id: facts
            .and_then(|facts| facts.client_message_id.clone())
            .filter(|_| user),
        sender: facts.and_then(|facts| facts.sender.clone()),
        speaks_for: marks.and_then(|marks| marks.get("speaks_for").cloned()),
        notice: marks.and_then(|marks| marks.get("notice")) == Some(&serde_json::Value::Bool(true)),
        calls,
        notices,
        tool_times: entry
            .state
            .turn
            .as_ref()
            .map(|turn| turn_times(&entry.message, turn))
            .unwrap_or_default(),
        ride_notices: facts
            .iter()
            .flat_map(|facts| &facts.notices)
            .map(|notice| {
                let fact = cortexkit_role_llm_runner::read::NoticeFact::new(
                    notice.part,
                    &notice.send_id,
                    &notice.sender,
                    &notice.text,
                )
                .with_call_key(&notice.call_key);
                match speaks_for(notice.speaks_for.as_ref()) {
                    Some(speaks_for) => fact.with_speaks_for(speaks_for),
                    None => fact,
                }
            })
            .collect(),
    }
}

/// What an `include_display_facts` read serves about each tool call of `message`, whose tool
/// turn recorded `turn`: one per call, in the order the message made them, with its key, schema
/// pin, dispatch and finish times and why it has no answer, as the turn recorded them (see
/// [`facts_by_call`]); a call the turn has no facts for is named with none. Imported calls
/// retain the key and carrier of the runner that recorded them.
pub fn turn_times(
    message: &broca_protocol::Message,
    turn: &broca_session_store::record::TurnFacts,
) -> Vec<cortexkit_role_llm_runner::read::ToolTime> {
    use cortexkit_role_llm_runner::read::ToolTime;
    let ids: Vec<&str> = message
        .content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::ToolCall(call) => Some(call.tool_call_id.0.as_str()),
            _ => None,
        })
        .collect();
    ids.iter()
        .zip(facts_by_call(&ids, turn))
        .map(|(id, call)| {
            let Some(call) = call else {
                return ToolTime::new(*id);
            };
            let mut time = ToolTime::new(*id).with_call_key(&call.call_key);
            time.call_carrier = call.carrier.clone();
            time.started_at = call.dispatched_at_ms;
            time.finished_at = call.finished_at_ms;
            time.unanswered = call.unanswered.map(|why| why.as_str().to_owned());
            if let Some(serde_json::Value::String(pin)) = &call.schema_pin {
                time = time.with_schema_pin(pin);
            }
            time
        })
        .collect()
}

/// The facts `turn` recorded for each of a message's tool calls `ids`, in call order. A turn
/// records the facts of the calls that have them, in the order the message made the calls, so
/// each call takes the next facts naming its id: a model's repeated call id finds each copy's
/// own.
pub fn facts_by_call<'t>(
    ids: &[&str],
    turn: &'t broca_session_store::record::TurnFacts,
) -> Vec<Option<&'t broca_session_store::record::CallFacts>> {
    let mut facts = turn.calls.iter().peekable();
    ids.iter()
        .map(|id| facts.next_if(|call| call.tool_call_id.0 == *id))
        .collect()
}

impl DisplayFacts {
    /// What an `include_display_facts` read serves from these facts, in the form today's read
    /// builds for a page message (`crate::serve::apply_display_facts`): the sender and the send's
    /// marks, for a user message the send's `client_message_id`, each tool call's facts, and for a
    /// tool results message the notices of the sends that rode on its results.
    pub fn served(&self) -> broca_core::display_facts::DisplayFacts {
        broca_core::display_facts::DisplayFacts {
            send_id: self.send_id.clone(),
            message_marks: self.message_marks(),
            sender: self.sender.clone(),
            client_message_id: self.client_message_id.clone(),
            tool_times: self.tool_times.clone(),
            notices: self
                .ride_notices
                .iter()
                .map(|fact| broca_core::projection::ProjectedNotice {
                    fact: fact.clone(),
                    // A served notice names its send by the send's id alone.
                    submission_id: String::new(),
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The send's marks as today's display facts hold them: absent when the send carried
    /// neither a `speaks_for` nor a true `notice`.
    fn message_marks(&self) -> Option<broca_protocol::MessageMarks> {
        let speaks_for = speaks_for(self.speaks_for.as_ref());
        if speaks_for.is_none() && !self.notice {
            return None;
        }
        let mut marks = broca_protocol::MessageMarks::default();
        marks.speaks_for = speaks_for;
        marks.notice = self.notice;
        Some(marks)
    }
}

/// A stored `speaks_for` mark (llm-runner's `SpeaksFor` JSON) as reads serve it, or `None` when
/// there is none or it doesn't decode.
pub(crate) fn speaks_for(stored: Option<&serde_json::Value>) -> Option<broca_protocol::SpeaksFor> {
    stored.and_then(|value| serde_json::from_value(value.clone()).ok())
}
