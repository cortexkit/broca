//! Rebuild a request as of its recorded provider attempt.

use crate::{parts::Parts, writer::Writer};
use broca_core::RecordedRequest;
use broca_protocol::{BindIdentity, RunId, StepId};
use broca_session_store::fold::{conversation::Conversation, operations::Operations};
use broca_session_store::record::{Record, Stream};
use broca_session_store::session::{Carry, EnrollmentOverlay, SessionError, StoredEntry};

/// Where a rebuilt request's conversation ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Through {
    /// Before the step's own reply: the request the step sent.
    Request,
    /// After the step's own reply: the request the step sent with the reply appended, rendered
    /// as history, as the session's next request would carry it. The reply must already be
    /// placed in the conversation, which a reply that asked for no tool call is when its step
    /// ends. The rebuilt request keeps the step's settings; its digest is not the recorded one.
    Answer,
}

#[derive(Debug)]
pub enum RebuildError {
    RetiredEcho,
    Unavailable(String),
}

impl From<&str> for RebuildError {
    fn from(message: &str) -> Self {
        Self::Unavailable(message.into())
    }
}

impl From<String> for RebuildError {
    fn from(message: String) -> Self {
        Self::Unavailable(message)
    }
}

impl std::fmt::Display for RebuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RetiredEcho => write!(
                f,
                "the recorded request cannot be reproduced: {}",
                broca_core::provider_errors::LEGACY_ANTHROPIC_ECHO
            ),
            Self::Unavailable(message) => f.write_str(message),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn rebuild<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
    through: Through,
) -> Result<RecordedRequest, String> {
    Box::pin(rebuild_at(
        writer, records, parts, bind, run_id, step, through, None,
    ))
    .await
    .map_err(|error| error.to_string())
}

/// Rebuild an attempt in the open log without scanning older, sealed segments.
#[allow(clippy::too_many_arguments)]
pub async fn rebuild_at<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
    through: Through,
    checkpoint: Option<Box<broca_session_store::checkpoint::Checkpoint>>,
) -> Result<RecordedRequest, RebuildError> {
    let (attempt_at, attempt) = records
        .iter()
        .enumerate()
        .rev()
        .find_map(|(at, (_, record))| match record {
            Record::Attempt(attempt)
                if crate::writer::is_cut_attempt(record)
                    && attempt.run_id == run_id
                    && step.is_none_or(|step| attempt.step_id == step) =>
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
    let mut operations = checkpoint
        .as_ref()
        .map(|cut| cut.operational.clone())
        .unwrap_or_default();
    for (seq, record) in &records[..=attempt_at] {
        if record.stream() == Stream::Ledger
            && checkpoint
                .as_ref()
                .is_none_or(|cut| *seq > cut.covers_through)
            && !matches!(record, Record::ChangeIntent(_) | Record::ChangeAborted(_))
        {
            operations
                .apply_at(Some(*seq), record)
                .map_err(|error| format!("the recorded step's state: {error:?}"))?;
        }
    }
    let conversation = {
        let first_kept = records[..=attempt_at]
            .iter()
            .rev()
            .find_map(|(_, record)| match record {
                Record::Marker(marker) => Some(marker.first_kept_ordinal),
                _ => None,
            })
            .unwrap_or(1);
        let enrolled = operations.enrolled_through();
        // Through the facts that place the step's own assistant message, so the fold knows its
        // ordinal; nothing later, so a later cut's marker never reaches it. A step whose reply was
        // never placed (the provider refused it, or it was cut off) is folded through its
        // attempt, which is everything its request held.
        let placed_at = records[attempt_at..]
            .iter()
            .position(|(_, record)| matches!(record, Record::EntryFacts(facts) if facts.mid == mid))
            .map_or(attempt_at, |at| attempt_at + at);
        if let Some(cut) = checkpoint {
            let mut conversation = cut.conversation;
            conversation.enrolled(enrolled);
            for (_, record) in &records[..=placed_at] {
                if record.stream() == Stream::Transcript {
                    conversation
                        .apply(record)
                        .map_err(|error| format!("the open step's conversation: {error:?}"))?;
                }
            }
            conversation
        } else {
            conversation_as_of(
                &records[..=placed_at],
                writer.conversation().appends_in_conversation,
                enrolled,
                first_kept,
            )?
        }
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
            (through == Through::Request && !answered)
                .then(|| conversation.window_start() + conversation.entries.len() as u64)
        })
        .ok_or("the recorded step's tool turn is outside the conversation window")?;
    // As of the step, every ledger record before it is folded into the session's state, and the
    // compaction views come from the ledger of the log the step ran in, as the live session read
    // them. A seal opens its new log with whole copies of the views it names (a stored view is a
    // change against the views before it): the applied view, the record applying it, then a
    // newer accepted view if there is one. This history holds those copies beside the originals,
    // and folding both would take a view again after a newer one. Views only grow newer within
    // one log, so a view no newer than the newest before it in its log is a copy that starts the
    // next log. A seal's change records mark where one log closed, which this fold skips.
    let ledger = {
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
        Some(
            records[log_start..=attempt_at]
                .iter()
                .filter(|(_, record)| {
                    record.stream() == Stream::Ledger
                        && !matches!(record, Record::ChangeIntent(_) | Record::ChangeAborted(_))
                })
                .cloned()
                .collect(),
        )
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
        attempt,
        writer,
        conversation,
        operations,
        ledger,
        records: &records[..=attempt_at],
        request_seq: records[start_at].0,
    };
    let run = before
        .operations
        .run
        .as_ref()
        .ok_or("the recorded step has no run")?;
    let config = crate::run::run_config_of_run(run, parts)
        .map_err(|error| format!("the recorded run's config: {error:?}"))?;
    let mut opts = crate::run::offline_media_request(&before, parts, bind, step)
        .await
        .map_err(|error| match error {
            crate::run::LoopError::Unsupported(
                broca_core::provider_errors::LEGACY_ANTHROPIC_ECHO,
            ) => RebuildError::RetiredEcho,
            other => RebuildError::Unavailable(format!(
                "the recorded request cannot be reproduced: {other:?}"
            )),
        })?;
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
    let route = crate::run::offline_step_route(
        &before.operations,
        &config.frozen_render_config,
        step,
        &opts,
    )
    .map_err(|error| format!("the recorded step's route: {error:?}"))?;
    Ok(RecordedRequest {
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
    })
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
    attempt: &'a broca_session_store::record::Attempt,
    writer: &'a W,
    conversation: Conversation,
    operations: Operations,
    /// The ledger records through the step, served in place of the writer's open log for a
    /// rebuild as of the step (see [`request_as_of`]).
    ledger: Option<Vec<(u64, Record)>>,
    records: &'a [(u64, Record)],
    request_seq: u64,
}

impl<W: Writer> Writer for Before<'_, W> {
    async fn cut_ordinal(&self, mid: &str) -> Result<Option<u64>, SessionError> {
        if let Some(ordinal) = self
            .records
            .iter()
            .rev()
            .find_map(|(_, record)| match record {
                Record::EntryFacts(facts) if facts.mid == mid => Some(facts.ordinal),
                _ => None,
            })
        {
            return Ok(Some(ordinal));
        }
        self.writer.cut_ordinal(mid).await
    }
    fn replay_attempt(&self) -> Option<&broca_session_store::record::Attempt> {
        Some(self.attempt)
    }
    async fn responses_reasoning_removals(
        &self,
        run_id: &str,
        before_seq: Option<u64>,
    ) -> Result<crate::responses_reasoning::ResponsesReasoningRemovals, SessionError> {
        self.writer
            .responses_reasoning_removals(
                run_id,
                Some(before_seq.map_or(self.request_seq, |seq| seq.min(self.request_seq))),
            )
            .await
    }
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
