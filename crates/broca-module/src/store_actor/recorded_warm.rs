//! Rebuild the request before a recorded tool turn, never the next request after its results.

use std::sync::Arc;

use broca_core::RecordedRequest;
use broca_protocol::{BindIdentity, RunId, StepId};
use broca_session_loop::{parts::Parts, writer::Writer};
use broca_session_store::fold::{conversation::Conversation, operations::Operations};
use broca_session_store::record::Record;
use broca_session_store::record::Stream;
use broca_session_store::session::{Carry, EnrollmentOverlay, SessionError, StoredEntry};

/// Build the request of run `run_id`'s last recorded attempt, or of its last attempt for step
/// `step` when one is named, from a snapshot: the actor and the running loop must keep the
/// complete conversation. `writer` is the session as it stands now; `records` must reach from
/// the run's start through that attempt.
pub(crate) async fn request<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
) -> Result<RecordedRequest, String> {
    rebuild(
        writer,
        records,
        parts,
        bind,
        run_id,
        step,
        false,
        Through::Request,
    )
    .await
    .map(|replay| replay.request)
}

/// Where a rebuilt request's conversation ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Through {
    /// Before the step's own reply: the request the step sent.
    Request,
    /// After the step's own reply: the request the step sent with the reply appended, rendered
    /// as history, as the session's next request would carry it. The reply must already be
    /// placed in the conversation, which a reply that asked for no tool call is when its step
    /// ends. The rebuilt request keeps the step's settings; its digest is not the recorded one.
    Answer,
}

pub(crate) struct RequestReplay {
    pub request: RecordedRequest,
    pub media: Option<MediaReplay>,
}

pub(crate) struct MediaReplay {
    /// The media renderer sees the index before the transport's attempt is recorded.
    pub before_attempt: u64,
    pub render_tags: bool,
}

/// Build the request step `step` of run `run_id` sent, as the session stood when it sent it, for
/// a step that may be long past: the views it renders with and the session's other state are
/// folded from `records`, which must be the session's whole history through the step's attempt,
/// not taken from the session as it stands now. Only the conversation comes from `writer`, cut
/// before the step's tool turn. A view applied since, or a seal since that moved the step's
/// records out of the open log, therefore doesn't change the rebuild.
pub(crate) async fn request_as_of<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: u64,
) -> Result<RecordedRequest, String> {
    rebuild(
        writer,
        records,
        parts,
        bind,
        run_id,
        Some(step),
        true,
        Through::Request,
    )
    .await
    .map(|replay| replay.request)
}

#[allow(clippy::too_many_arguments)]
async fn rebuild<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
    as_of: bool,
    through: Through,
) -> Result<RequestReplay, String> {
    let (attempt_at, attempt) = records
        .iter()
        .enumerate()
        .rev()
        .find_map(|(at, (_, record))| match record {
            Record::Attempt(attempt)
                if attempt.run_id == run_id && step.is_none_or(|step| attempt.step_id == step) =>
            {
                Some((at, attempt))
            }
            _ => None,
        })
        .ok_or("the run's request has no recorded attempt")?;
    let step = attempt.step_id;
    let (start_at, mid) = records[..attempt_at]
        .iter()
        .enumerate()
        .rev()
        .find_map(|(at, (_, record))| match record {
            Record::StepStart(start) if start.run_id == run_id && start.step_id == step => {
                start.mid.as_deref().map(|mid| (at, mid))
            }
            _ => None,
        })
        .ok_or("the recorded step has no assistant message id")?;
    // As of the step, the conversation is folded from the step's own records, sealed at the cut
    // its views had marked by then: a seal since may have cut deeper, moving the step's messages
    // out of the session's window.
    let folded;
    let conversation = if as_of {
        let first_kept = records[..=attempt_at]
            .iter()
            .rev()
            .find_map(|(_, record)| match record {
                Record::Marker(marker) => Some(marker.first_kept_ordinal),
                _ => None,
            })
            .unwrap_or(1);
        let enrolled = {
            let mut operations = Operations::default();
            for (seq, record) in &records[..=attempt_at] {
                if record.stream() == Stream::Ledger
                    && !matches!(record, Record::ChangeIntent(_) | Record::ChangeAborted(_))
                {
                    operations
                        .apply_at(Some(*seq), record)
                        .map_err(|error| format!("the recorded step's state: {error:?}"))?;
                }
            }
            operations.enrolled_through()
        };
        // Through the facts that place the step's own assistant message, so the fold knows its
        // ordinal; nothing later, so a later cut's marker never reaches it. A step whose reply was
        // never placed (the provider refused it, or it was cut off) is folded through its
        // attempt, which is everything its request held.
        let placed_at = records[attempt_at..]
            .iter()
            .position(|(_, record)| matches!(record, Record::EntryFacts(facts) if facts.mid == mid))
            .map_or(attempt_at, |at| attempt_at + at);
        folded = conversation_as_of(
            &records[..=placed_at],
            writer.conversation().appends_in_conversation,
            enrolled,
            first_kept,
        )?;
        &folded
    } else {
        // Content the session released from memory is read back: the rebuild renders the whole
        // window, as the step did.
        folded = broca_session_loop::writer::whole_conversation(writer)
            .await
            .map_err(|error| format!("the session's window: {error:?}"))?;
        &folded
    };
    let ordinal = conversation
        .open_entry()
        .filter(|(_, _, entry)| entry.facts.as_ref().is_some_and(|facts| facts.mid == mid))
        .map(|(ordinal, _, _)| ordinal)
        .or_else(|| {
            conversation
                .entries
                .iter()
                .position(|entry| entry.facts.as_ref().is_some_and(|facts| facts.mid == mid))
                .map(|at| conversation.window_start() + at as u64)
        })
        .or_else(|| {
            // An unplaced reply's request ends after the last message folded so far.
            let answered = records[attempt_at..]
                .iter()
                .any(|(_, record)| matches!(record, Record::EntryFacts(facts) if facts.mid == mid));
            (as_of && through == Through::Request && !answered)
                .then(|| conversation.window_start() + conversation.entries.len() as u64)
        })
        .ok_or("the recorded step's tool turn is outside the conversation window")?;
    let run_at = records[..=attempt_at]
        .iter()
        .rposition(
            |(_, record)| matches!(record, Record::RunStart(start) if start.run_id == run_id),
        )
        .ok_or("the recorded run's start is outside the readable history")?;
    // As of the step, every ledger record before it is folded into the session's state, and the
    // compaction views come from the ledger of the log the step ran in, as the live session read
    // them. A seal opens its new log with whole copies of the views it names (a stored view is a
    // change against the views before it): the applied view, the record applying it, then a
    // newer accepted view if there is one. This history holds those copies beside the originals,
    // and folding both would take a view again after a newer one. Views only grow newer within
    // one log, so a view no newer than the newest before it in its log is a copy that starts the
    // next log. A seal's change records mark where one log closed, which this fold skips.
    let (fold_from, ledger) = if as_of {
        let mut log_start = 0;
        let mut newest_in_log: Option<u64> = None;
        for (at, (_, record)) in records[..=attempt_at].iter().enumerate() {
            if let Record::View(view) = record {
                if newest_in_log.is_some_and(|newest| view.version <= newest) {
                    log_start = at;
                }
                newest_in_log = Some(view.version);
            }
        }
        let ledger: Vec<(u64, Record)> = records[log_start..=attempt_at]
            .iter()
            .filter(|(_, record)| {
                record.stream() == Stream::Ledger
                    && !matches!(record, Record::ChangeIntent(_) | Record::ChangeAborted(_))
            })
            .cloned()
            .collect();
        (0, Some(ledger))
    } else {
        (run_at, None)
    };
    let mut prefix = Operations::default();
    for (seq, record) in &records[fold_from..=attempt_at] {
        if record.stream() == Stream::Ledger
            && !(as_of && matches!(record, Record::ChangeIntent(_) | Record::ChangeAborted(_)))
        {
            prefix
                .apply_at(Some(*seq), record)
                .map_err(|error| format!("the recorded step's state: {error:?}"))?;
        }
    }
    let operations = if as_of {
        prefix
    } else {
        let mut operations = writer.operations().clone();
        operations.run = prefix.run;
        operations
    };
    let conversation = match through {
        Through::Request => conversation
            .before_entry(ordinal)
            .ok_or("the recorded step is outside the window")?,
        // The open turn is a reply still waiting on its tool calls, which is never appended.
        Through::Answer => conversation
            .before_entry(ordinal + 1)
            .ok_or("the recorded step's reply is not placed in the conversation")?,
    };
    let before = Before {
        writer,
        conversation,
        operations,
        ledger,
    };
    let run = before
        .operations
        .run
        .as_ref()
        .ok_or("the recorded step has no run")?;
    let start = run.start.as_ref().ok_or("the recorded run has no start")?;
    let config = super::warm::run_config(run, start, parts)
        .map_err(|error| format!("the recorded run's config: {error:?}"))?;
    let mut opts = broca_session_loop::run::offline_media_request(&before, parts, bind, step)
        .await
        .map_err(|error| format!("the recorded step's request: {error:?}"))?;
    let cap = records[start_at..attempt_at]
        .iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::OutputCapLowered(cap) if cap.step_id == step => Some(cap),
            _ => None,
        });
    if let Some(context) = opts.step_context.as_mut() {
        context.max_output_tokens = cap.map(|cap| cap.max_output_tokens);
    }
    let route = records[attempt_at..]
        .iter()
        .find_map(|(_, record)| match record {
            Record::StepEnd(end) if end.run_id == run_id && end.step_id == step => {
                end.fallback.as_ref()
            }
            _ => None,
        })
        .map(broca_protocol::json::Json::parse)
        .and_then(|fallback| fallback.get("route").cloned())
        .map(serde_json::from_value::<broca_records::record::StepRoute>)
        .transpose()
        .map_err(|error| format!("the recorded step's route: {error}"))?;
    if let Some(route) = &route {
        opts.target_model = Some(route.model.clone());
    }
    let media = if opts
        .prompt
        .iter()
        .any(|message| !broca_session_store::session::message_media(message).is_empty())
    {
        let render_tags = config.render_tags
            || broca_session_loop::run::frozen_manifest(start, parts)
                .map_err(|error| format!("the recorded run's plan: {error:?}"))?
                .as_deref()
                .is_some_and(broca_core::tags::plan_renders_tags);
        Some(MediaReplay {
            before_attempt: records[attempt_at].0 - 1,
            render_tags,
        })
    } else {
        None
    };
    let request = RecordedRequest {
        run_id: RunId(run.run_id.clone()),
        step_id: StepId(step),
        config,
        opts,
        request_body_sha256: attempt
            .request_body_sha256
            .0
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        route,
    };
    Ok(RequestReplay { request, media })
}

/// The request `session` last sent for run `run_id` (for its step `step`, when one is named),
/// rebuilt from the session's records: its open log when that holds the run's start and the
/// attempt, else its whole history read through `readers`, the backend of the root holding its
/// sealed logs. With [`Through::Answer`], the step's reply follows the request. Writes nothing.
pub(crate) async fn recorded_request<B, R>(
    session: &broca_session_store::session::Session<B>,
    readers: Option<R>,
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
    through: Through,
) -> Result<RequestReplay, String>
where
    B: broca_session_store::backend::SessionBackend,
    R: broca_session_store::backend::SessionBackend,
{
    let mut records = session
        .open_log_records()
        .await
        .map_err(|error| format!("read the recorded request: {error:?}"))?;
    let attempted = |records: &[(u64, Record)]| {
        records.iter().any(|(_, record)| {
            matches!(record, Record::Attempt(attempt)
                if attempt.run_id == run_id && step.is_none_or(|step| attempt.step_id == step))
        })
    };
    if !attempted(&records)
        || !records
            .iter()
            .any(|(_, record)| matches!(record, Record::RunStart(start) if start.run_id == run_id))
    {
        let readers = readers.ok_or("the recorded request's sealed history cannot be read")?;
        records = session
            .stored_records(readers)
            .records_between(1, session.next_seq().saturating_sub(1))
            .await
            .map_err(|error| format!("read the recorded request's history: {error:?}"))?;
    }
    rebuild(session, &records, parts, bind, run_id, step, false, through).await
}

impl<B: broca_session_store::backend::SessionBackend + Send + Sync + 'static> super::StoreActor<B> {
    /// Called while the first attempt is finishing, before that attempt is appended. A
    /// diagnostic read needs only the newest attempted run and the setup recorded for it.
    pub(super) async fn cache_predecessor(
        &self,
    ) -> Result<Option<broca_core::provider::CachePredecessor>, String> {
        let mut run_id = None;
        let mut records = self
            .session
            .ledger_tail_until(|record| {
                if let Record::Attempt(attempt) = record {
                    run_id.get_or_insert_with(|| attempt.run_id.clone());
                }
                matches!(record, Record::RunStart(start) if Some(&start.run_id) == run_id.as_ref())
            })
            .await
            .map_err(|error| format!("read the cache predecessor: {error:?}"))?;
        records.reverse();
        predecessor(&self.session, &records, &self.parts, &self.key).await
    }

    pub(super) async fn recorded_warm_request(&self) -> Result<RecordedRequest, String> {
        let run_id = self
            .session
            .operations
            .run
            .as_ref()
            .ok_or("no recorded run")?
            .run_id
            .clone();
        recorded_request(
            &self.session,
            self.readers.as_ref().map(|readers| readers()),
            &self.parts,
            &self.key,
            &run_id,
            None,
            Through::Request,
        )
        .await
        .map(|replay| replay.request)
    }
}

async fn predecessor<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
) -> Result<Option<broca_core::provider::CachePredecessor>, String> {
    let Some((seq, attempt)) = records.iter().rev().find_map(|(seq, record)| match record {
        Record::Attempt(attempt) => Some((*seq, attempt)),
        _ => None,
    }) else {
        return Ok(None);
    };
    let start = records
        .iter()
        .position(|(_, record)| {
            matches!(record,
        Record::RunStart(start) if start.run_id == attempt.run_id)
        })
        .ok_or("the cache predecessor's run start is outside readable history")?;
    let mut prefix = Operations::default();
    for (at, record) in &records[start..] {
        if *at > seq {
            break;
        }
        prefix
            .apply_at(Some(*at), record)
            .map_err(|error| format!("the cache predecessor's state: {error:?}"))?;
    }
    let mut operations = writer.operations().clone();
    operations.run = prefix.run;
    let snapshot = Before {
        writer,
        conversation: broca_session_loop::writer::whole_conversation(writer)
            .await
            .map_err(|error| format!("the session's window: {error:?}"))?,
        operations,
        ledger: None,
    };
    let request = request(
        &snapshot,
        &records[start..],
        parts,
        bind,
        &attempt.run_id,
        None,
    )
    .await?;
    Ok(Some(broca_core::provider::CachePredecessor {
        request,
        sent_at_ms: attempt.sent_at_ms,
        account: attempt.served_account_ref.clone(),
        locator: format!("{}:{seq}", writer.session_id()),
        view: None,
    }))
}

/// Share the run's media delivery with a warm that rebuilds one recorded request: both render
/// from the same session index, and neither changes it.
pub(super) fn media(
    wait: &mut Option<crate::store_media::StoreRunMediaWait>,
) -> Result<Arc<crate::store_media::LazyRenderer>, String> {
    let original = wait
        .take()
        .ok_or("the resumed run has no media readiness wait")?;
    let (live, live_wait) = tokio::sync::oneshot::channel();
    let (recorded, recorded_wait) = tokio::sync::oneshot::channel();
    *wait = Some(live_wait);
    tokio::spawn(async move {
        if let Ok(media) = original.await {
            let _ = recorded.send(media.clone());
            let _ = live.send(media);
        }
    });
    Ok(crate::store_media::LazyRenderer::new(
        recorded_wait,
        Box::new(|media| Ok(media.renderer())),
    ))
}

/// Offline rendering reads the snapshot's conversation and the session's durable lookups. No
/// write is permitted through this adapter, including a compaction seal.
/// The conversation as a step saw it: the transcript records before its attempt (`records`, the
/// session's history from its first record) folded in order, with the window sealed at the
/// step's cut, `first_kept`, as the session had sealed it by then. History below the cut is
/// sealed every [`AS_OF_SEAL_EVERY`] messages on the way, so the fold holds about that many
/// messages besides the step's window, whatever the session's length.
fn conversation_as_of(
    records: &[(u64, Record)],
    appends_in_conversation: bool,
    enrolled_through: Option<u64>,
    first_kept: u64,
) -> Result<Conversation, String> {
    let mut conversation = Conversation::new(appends_in_conversation);
    conversation.enrolled(enrolled_through);
    let fold = |error| format!("the recorded step's conversation: {error:?}");
    for (_, record) in records {
        if record.stream() != Stream::Transcript {
            continue;
        }
        // A log starts, and so a seal falls, only at an input or an assistant message with no
        // tool turn open before it.
        let next = conversation.placed + 1;
        let at_boundary = matches!(record, Record::Input(_) | Record::Assistant(_))
            && conversation.open_tool_turn().is_none();
        if at_boundary && next <= first_kept && next > conversation.window_start() {
            if next == first_kept {
                let starts_run = match record {
                    Record::Input(input) => Some(input.run_id.as_str()),
                    _ => None,
                };
                conversation
                    .seal_window(first_kept, starts_run)
                    .map_err(fold)?;
            } else if conversation.messages.len() >= AS_OF_SEAL_EVERY {
                conversation.seal_window(next, None).map_err(fold)?;
            }
        }
        conversation.apply(record).map_err(fold)?;
    }
    Ok(conversation)
}

/// How many messages below a step's cut [`conversation_as_of`] holds before sealing them.
const AS_OF_SEAL_EVERY: usize = 4096;

struct Before<'a, W> {
    writer: &'a W,
    conversation: Conversation,
    operations: Operations,
    /// The ledger records through the step, served in place of the writer's open log for a
    /// rebuild as of the step (see [`request_as_of`]).
    ledger: Option<Vec<(u64, Record)>>,
}

impl<W: Writer> Writer for Before<'_, W> {
    async fn append(&mut self, _: &[Record], _: bool) -> Result<u64, SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn caught_up(&mut self) -> Result<(), SessionError> {
        // The session as it stood before a seq: nothing appends to it.
        Ok(())
    }
    async fn send_records(&mut self, _: &str) -> Result<Vec<Record>, SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn append_with(
        &mut self,
        _: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record> + Send),
        _: bool,
    ) -> Result<Vec<Record>, SessionError> {
        Err(SessionError::OwnerGone)
    }
    fn conversation(&self) -> &Conversation {
        &self.conversation
    }
    fn operations(&self) -> &Operations {
        &self.operations
    }
    fn session_id(&self) -> String {
        self.writer.session_id()
    }
    fn next_seq(&self) -> u64 {
        self.writer.next_seq()
    }
    fn message_id_prefix(&self) -> String {
        self.writer.message_id_prefix()
    }
    fn retired_tag_floor(&self) -> u64 {
        self.writer.retired_tag_floor()
    }
    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        match &self.ledger {
            Some(ledger) => Ok(ledger.clone()),
            None => self.writer.open_log_records().await,
        }
    }
    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        self.writer.run_start(run_id).await
    }

    async fn window_message_whole(
        &self,
        ordinal: u64,
    ) -> Result<std::sync::Arc<broca_protocol::Message>, SessionError> {
        self.writer.window_message_whole(ordinal).await
    }

    /// A rebuild only reads the session as it stood, so it releases nothing.
    async fn release(&mut self, _blocks: &[(u64, Vec<usize>)]) -> Result<(), SessionError> {
        Ok(())
    }
    async fn tool_intent(
        &self,
        key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        self.writer.tool_intent(key).await
    }
    async fn enrollment(&self) -> Result<Option<EnrollmentOverlay>, SessionError> {
        self.writer.enrollment().await
    }
    async fn put_enrollment(
        &mut self,
        _: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn seal_at_entry(&mut self, _: u64, _: &Carry<'_>, _: u64) -> Result<(), SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn seal_ledger(&mut self, _: &Carry<'_>, _: u64) -> Result<(), SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn entry_by_mid(&self, mid: &str) -> Result<Option<StoredEntry>, SessionError> {
        self.writer.entry_by_mid(mid).await
    }
    async fn entry_at(&self, ordinal: u64) -> Result<Option<StoredEntry>, SessionError> {
        self.writer.entry_at(ordinal).await
    }

    async fn moved_through(&self) -> Result<Option<(String, u64)>, SessionError> {
        self.writer.moved_through().await
    }
    async fn entries_by_mid(&self, mids: &[String]) -> Result<Vec<StoredEntry>, SessionError> {
        self.writer.entries_by_mid(mids).await
    }
}
