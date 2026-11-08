//! Entry facts: what `session.entries` serves about each message, decided once when the message
//! is placed.
//!
//! Today's loop leaves these for its projection to work out again on every read, by folding the
//! whole log (`broca_core::compaction_v2::metadata`). The new loop writes an `EntryFacts`
//! transcript record right after the records that place (or reserve) a message, as the
//! converter does for a converted session, so both kinds of session read the same way. The rules
//! are today's:
//! - **Part numbers:** a message's numbered parts take the numbers after the highest one before
//!   it (`tag_base`). A tool-results message has none of its own.
//! - **Notice:** a user message names the module that sent it, when one did.
//! - **Usage and view:** a model-written message carries its step's usage and the compaction
//!   view the step rendered with.
//! - **Time:** the message's own time, else the nearest earlier entry's.

use std::collections::HashMap;

use broca_core::compaction_v2::metadata::{numbered_parts, step_usage};
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ToolCallId;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::{CallState, Operations};
use broca_session_store::record::{
    CallFacts, CallOutcome, EntryFacts, Notice, Placement, ReasoningSummary, Record, RichUi,
    StepUsage, ToolIntent, ToolOutcome, ToolReply, TurnFacts, Unanswered, ViewRef,
};
use broca_session_store::session::{SessionError, StoredEntry};
use cortexkit_role_compaction::read::Entry;

use crate::writer::Writer;

/// A writer that adds entry facts to every batch it appends, so no place in the loop that
/// writes a message can forget them. Every session records them: `session.read` serves each
/// message from them. Only a session on the current compaction contract records the facts
/// `session.entries` serves (see [`Tags::entries`]). The batch that places a tool turn's results
/// also records the turn's facts ([`turn_facts`]).
pub struct Placed<'w, W> {
    pub inner: &'w mut W,
    /// The run's tag rules.
    pub facts: Tags,
    /// What this writer knows of the open batch's calls.
    pub calls: OpenCalls,
    /// The Rich UI stamp the run's plan froze, which the run's assistant messages record.
    pub rich_ui: Option<RichUi>,
}

/// What a tool turn's facts name about its calls that the operational fold doesn't keep, as far
/// as this writer has seen it appended or looked it up: each dispatch intent, by the call key it
/// recorded, with its seq where known (the fold keeps only that an intent was recorded, and its
/// key), and each call's terminal in the open batch, in the order they were recorded (the fold
/// keeps only the kind).
#[derive(Debug, Default)]
pub struct OpenCalls {
    intents: HashMap<String, (ToolIntent, Option<u64>)>,
    /// The batch the terminals were recorded in, and the terminals.
    terminals: Option<(String, Vec<Terminal>)>,
}

/// What a tool call's terminal says about it, as reads serve it.
#[derive(Debug, Clone, PartialEq)]
pub struct Terminal {
    tool_call_id: ToolCallId,
    /// The tool answered: reads serve the answer's time (`finished_at_ms`) as the call's finish.
    answered: bool,
    finished_at_ms: Option<u64>,
    unanswered: Option<Unanswered>,
    /// The call never left: it took no dispatch intent.
    refused: bool,
    /// The seq of the outcome record, which orders it among the turn's other call records.
    seq: u64,
    /// The key the outcome recorded for a call answered without a dispatch intent
    /// (`ToolOutcome::call_key`).
    call_key: Option<String>,
}

impl Terminal {
    /// The terminal the reply `reply` and its outcome `outcome`, at `seq`, record. A call its tool
    /// answered has its answer's time; any other has why it has no answer, by today's rules:
    /// a refusal or a policy block is `denied`, a call that was never sent or failed validation
    /// is `not_executed`, one abandoned or sent without an answer back is `interrupted`, and one
    /// a step limit stopped is `cancelled`.
    pub fn of(reply: &ToolReply, outcome: &ToolOutcome, seq: u64) -> Self {
        use broca_protocol::content::ToolOutput;
        let unanswered = match outcome.outcome {
            CallOutcome::Completed => match &reply.result.output {
                ToolOutput::Denied { .. } => Some(Unanswered::Denied),
                ToolOutput::Error {
                    nothing_sent: Some(true),
                    ..
                } => Some(Unanswered::NotExecuted),
                _ => None,
            },
            CallOutcome::Unknown | CallOutcome::Interrupted => Some(Unanswered::Interrupted),
            CallOutcome::MaxSteps => Some(Unanswered::Cancelled),
            CallOutcome::NotDispatched => match outcome
                .detail
                .as_ref()
                .and_then(|detail| detail.get("kind"))
                .and_then(serde_json::Value::as_str)
            {
                Some("denied" | "policy_blocked") => Some(Unanswered::Denied),
                Some("validation_failed" | "unavailable") => Some(Unanswered::NotExecuted),
                _ => None,
            },
        };
        Self {
            tool_call_id: outcome.tool_call_id.clone(),
            answered: outcome.outcome == CallOutcome::Completed && unanswered.is_none(),
            finished_at_ms: reply.finished_at_ms,
            unanswered,
            refused: outcome.outcome == CallOutcome::NotDispatched,
            seq,
            call_key: outcome.call_key.clone(),
        }
    }
}

/// The terminals of batch `batch` among `records`, each a reply followed by its outcome, in order.
fn terminals_in<'r>(
    records: impl IntoIterator<Item = (u64, &'r Record)>,
    batch: &str,
) -> Vec<Terminal> {
    let mut reply = None;
    let mut terminals = Vec::new();
    for (seq, record) in records {
        match record {
            Record::ToolReply(next) => reply = Some(next),
            Record::ToolOutcome(outcome) if outcome.batch_id == batch => {
                if let Some(reply) = reply
                    .take()
                    .filter(|reply| reply.result.tool_call_id == outcome.tool_call_id)
                {
                    terminals.push(Terminal::of(reply, outcome, seq));
                }
            }
            _ => {}
        }
    }
    terminals
}

/// The seq a call key the run loop minted names (`format!("{session}:{seq}")`), if `key` is one
/// of this session's.
fn key_seq(key: &str, session: &str) -> Option<u64> {
    key.strip_prefix(session)?.strip_prefix(':')?.parse().ok()
}

impl OpenCalls {
    /// Notes what `records`, the first at `first_seq`, record about the open batch's calls.
    fn note(&mut self, records: &[Record], first_seq: u64) {
        for (seq, record) in (first_seq..).zip(records) {
            if let Record::ToolIntent(intent) = record {
                if let Some(key) = &intent.call_key {
                    self.intents
                        .insert(key.clone(), (intent.clone(), Some(seq)));
                }
            }
        }
        let Some(batch) = records.iter().rev().find_map(|record| match record {
            Record::ToolOutcome(outcome) => Some(outcome.batch_id.clone()),
            _ => None,
        }) else {
            return;
        };
        let terminals = terminals_in((first_seq..).zip(records), &batch);
        match &mut self.terminals {
            Some((known, noted)) if *known == batch => noted.extend(terminals),
            _ => self.terminals = Some((batch, terminals)),
        }
    }

    /// The terminals recorded in batch `batch`.
    fn terminals_of(&self, batch: &str) -> &[Terminal] {
        match &self.terminals {
            Some((known, terminals)) if known == batch => terminals,
            _ => &[],
        }
    }
}

impl<W: Writer> Placed<'_, W> {
    /// Learns what the open batch's calls recorded before this writer was made (by a run a
    /// resume continues, say): the intent of every call that has one, found through the
    /// session's id lookup since it may already be sealed, and the terminals, read from the open
    /// log, which holds every record of a batch still open.
    async fn learn_open_calls(&mut self) -> Result<(), SessionError> {
        let Some(batch) = self
            .inner
            .operations()
            .run
            .as_ref()
            .and_then(|run| run.open_batch.as_ref())
        else {
            return Ok(());
        };
        // A call answered without an intent has its outcome's key, and no intent.
        let missing: Vec<String> = batch
            .calls
            .iter()
            .filter(|call| call.intent)
            .filter_map(|call| call.key.as_deref())
            .filter(|key| !self.calls.intents.contains_key(*key))
            .map(str::to_owned)
            .collect();
        let answered = (0..batch.calls.len())
            .filter(|position| batch.state(*position) == CallState::Terminated)
            .count();
        let batch_id = batch.batch_id.clone();
        let session = self.inner.session_id();
        for key in missing {
            let intent = self.inner.tool_intent(&key).await?.ok_or_else(|| {
                SessionError::Fold(format!("no dispatch intent records the call key {key}"))
            })?;
            let seq = key_seq(&key, &session);
            self.calls.intents.insert(key, (intent, seq));
        }
        if self.calls.terminals_of(&batch_id).len() < answered {
            let records = self.inner.open_log_records().await?;
            let terminals = terminals_in(
                records.iter().map(|(seq, record)| (*seq, record)),
                &batch_id,
            );
            self.calls.terminals = Some((batch_id, terminals));
        }
        Ok(())
    }
}

/// The view a model-written message was written under, as today's projection decides it: the
/// newest view a step applied, which the step that writes the message applied or kept.
fn written_under(operations: &Operations) -> Option<ViewRef> {
    operations.view_applied.as_ref().map(|applied| ViewRef {
        version: applied.view.version,
        checksum: applied.view.checksum.clone(),
    })
}

impl<W: Writer> Writer for Placed<'_, W> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        if records.is_empty() {
            return self.inner.append(records, barrier).await;
        }
        // The facts name the seq of their own record (a message's id) and the ordinals the
        // batch places, so they are decided under the writer's hold, from the folds and the
        // first seq as they stand when the batch goes in. A writer the session actor shares
        // (`ActorWriter`) may otherwise lag the records the actor appended since the run last
        // saw the folds, such as a send queued while a model step was in flight, and every id
        // in the batch would name the wrong record.
        let batch = records.to_vec();
        let mut first = None;
        self.append_with(
            &mut |_, _, first_seq| {
                first = Some(first_seq);
                batch.clone()
            },
            barrier,
        )
        .await?;
        first.ok_or_else(|| SessionError::Fold("a placed batch was never built".into()))
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        self.inner.send_records(submission_id).await
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        self.learn_open_calls().await?;
        let tags = self.facts;
        let prefix = self.inner.message_id_prefix();
        let session = self.inner.session_id();
        let mut failure = None;
        let mut first = 0;
        let calls = &self.calls;
        let rich_ui = self.rich_ui.as_ref();
        let records = self
            .inner
            .append_with(
                &mut |conversation, operations, first_seq| {
                    first = first_seq;
                    let batch = build(conversation, operations, first_seq);
                    let placing = Placing {
                        conversation,
                        first_seq,
                        prefix: &prefix,
                        session: &session,
                        tags,
                        view: written_under(operations),
                        operations,
                        calls,
                        rich_ui,
                    };
                    match with_facts(batch, &placing) {
                        Ok(batch) => batch,
                        Err(error) => {
                            failure = Some(error);
                            Vec::new()
                        }
                    }
                },
                barrier,
            )
            .await?;
        match failure {
            Some(error) => Err(SessionError::Fold(error)),
            None => {
                self.calls.note(&records, first);
                Ok(records)
            }
        }
    }

    fn conversation(&self) -> &Conversation {
        self.inner.conversation()
    }

    fn operations(&self) -> &Operations {
        self.inner.operations()
    }

    fn session_id(&self) -> String {
        self.inner.session_id()
    }

    fn next_seq(&self) -> u64 {
        self.inner.next_seq()
    }

    fn message_id_prefix(&self) -> String {
        self.inner.message_id_prefix()
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        self.inner.open_log_records().await
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        self.inner.run_start(run_id).await
    }

    async fn tool_intent(&self, call_key: &str) -> Result<Option<ToolIntent>, SessionError> {
        self.inner.tool_intent(call_key).await
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        self.inner.seal_at_entry(ordinal, carry, now_ms).await
    }

    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        self.inner.seal_ledger(carry, now_ms).await
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        self.inner.entry_by_mid(mid).await
    }

    async fn entries_by_mid(
        &self,
        mids: &[String],
    ) -> Result<Vec<broca_session_store::session::StoredEntry>, SessionError> {
        self.inner.entries_by_mid(mids).await
    }

    async fn sealed_derived(
        &self,
    ) -> Result<Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>, SessionError> {
        self.inner.sealed_derived().await
    }

    async fn keep_sealed_derived(
        &self,
        value: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), SessionError> {
        self.inner.keep_sealed_derived(value).await
    }

    fn media(&self) -> Option<std::sync::Arc<dyn broca_core::tool_media::MediaCapture>> {
        self.inner.media()
    }
}

/// The run's frozen tag rules, which every message it places records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tags {
    /// Whether the run's requests render part numbers.
    pub render: bool,
    /// Whether the run cleans part numbers out of the model's replies.
    pub clean_replies: bool,
    /// Whether the session is on the current compaction contract, so it serves
    /// `session.entries` and records what that serves: part numbers, notices, usage and the view
    /// in force. Off the contract those facts take fixed values (part numbers from 0, none of
    /// the rest), since nothing reads them there.
    pub entries: bool,
}

impl Tags {
    /// The rules for a run of a session that is (`contract`) or isn't on the current compaction
    /// contract.
    pub fn for_session(
        contract: bool,
        manifest: Option<&broca_wal::PlanManifest>,
        render_config: &serde_json::Value,
    ) -> Self {
        if contract {
            Self::for_run(manifest, render_config)
        } else {
            Self {
                render: false,
                clean_replies: false,
                entries: false,
            }
        }
    }

    /// Today's rules for a run on the current compaction contract: numbers render when the plan
    /// asks for them or names a compaction item, and replies are cleaned when they render, except
    /// a structured-output run's final text.
    pub fn for_run(
        manifest: Option<&broca_wal::PlanManifest>,
        render_config: &serde_json::Value,
    ) -> Self {
        let render = manifest.is_some_and(|manifest| {
            broca_core::tags::plan_renders_tags(manifest)
                || broca_core::compaction::has_compaction(manifest)
        });
        Self {
            render,
            clean_replies: broca_core::compaction_v2::metadata::clean_replies(
                render,
                render_config,
            ),
            entries: true,
        }
    }
}

/// What a batch's messages are placed under besides their own records.
pub struct Placing<'a> {
    pub conversation: &'a Conversation,
    /// The seq the batch's first record will take.
    pub first_seq: u64,
    /// The writer's message id prefix (`Writer::message_id_prefix`).
    pub prefix: &'a str,
    /// The session's id, which a call key names (`Writer::session_id`).
    pub session: &'a str,
    pub tags: Tags,
    /// The compaction view the current step rendered with.
    pub view: Option<ViewRef>,
    /// The operational state before the batch: the step's request, the pending steers and the
    /// queue the batch's messages come from.
    pub operations: &'a Operations,
    /// What the writer knows of the open batch's calls.
    pub calls: &'a OpenCalls,
    /// The run's Rich UI stamp (`Placed::rich_ui`).
    pub rich_ui: Option<&'a RichUi>,
}

/// `batch` with an `EntryFacts` record after the records that place or reserve each message:
/// after an assistant message and its step's end, after a tool result and its outcome, and after
/// a run of input messages together with the step start or run end that places them, as the
/// converter places them for a converted session. The batch that places a tool turn's results
/// also gets the turn's facts after them ([`turn_facts`]); a run's end as cancelled restates its
/// last turn's facts with the calls it abandoned as cancelled, as today's reads state them.
pub fn with_facts(batch: Vec<Record>, at: &Placing<'_>) -> Result<Vec<Record>, String> {
    let mut counts = at.conversation.without_window();
    let mut known = known_of(&counts);
    let mut inserted = false;
    let entries = at.tags.entries;
    let mut high = if entries {
        high_water(at.conversation)?
    } else {
        0
    };
    let mut time = last_time(at.conversation);
    let mut out = Vec::with_capacity(batch.len() + 2);
    let mut pending: Vec<EntryFacts> = Vec::new();
    // The ids the current step and batch announced for the messages they write, which those
    // messages' facts carry: from earlier batches, then from this batch's own starts.
    let run = at.operations.run.as_ref();
    let mut step_mid = run.and_then(|run| run.step_mid.clone());
    // Whether this batch placed or held the message `step_mid` names.
    let mut step_placed = false;
    let mut results_mid = run
        .and_then(|run| run.open_batch.as_ref())
        .and_then(|batch| batch.results_mid.clone());
    // Who sent the input of a run started in this batch, read from its start as today's entry
    // metadata reads it from the run's start, and the send display facts of that input.
    let mut run_sender = None;
    let mut run_send = SendFacts::default();
    let mut records = batch.into_iter().peekable();
    // The tool turn this batch places, written after the records that place it: the ordinal of
    // its assistant message, and the message. Its facts are written once the closing reply's
    // outcome (which may carry the key of a call answered without an intent) is in.
    let mut placed_turn: Option<(u64, Message)> = None;
    // The facts of the last tool turn this batch wrote.
    let mut latest_turn: Option<TurnFacts> = None;
    // What the open turn's calls recorded, in the order recorded: the open batch's, then this
    // batch's own.
    let open_batch = at
        .operations
        .run
        .as_ref()
        .and_then(|run| run.open_batch.as_ref());
    // The fold marks every copy of a call id with the intent that anchored its first copy, so
    // each intent is its first copy's: the open batch's intents, each once, by that copy.
    let anchored: Vec<&broca_session_store::fold::operations::Call> = open_batch
        .map(|batch| {
            let mut seen = std::collections::HashSet::new();
            batch
                .calls
                .iter()
                .filter(|call| call.intent)
                .filter(|call| call.key.as_ref().is_none_or(|key| seen.insert(key)))
                .collect()
        })
        .unwrap_or_default();
    // The calls a dispatch intent anchored whose outcome isn't recorded yet, one entry per
    // intent: each outcome for one of them answers it, and any other outcome answers a call
    // that took no intent.
    let mut dispatched: Vec<ToolCallId> = anchored
        .iter()
        .filter(|call| call.terminal.is_none())
        .map(|call| call.id.clone())
        .collect();
    let mut events: Vec<CallEvent> = Vec::new();
    if let Some(batch) = open_batch {
        for call in &anchored {
            // A call answered without an intent has its outcome's key, which its terminal
            // carries.
            let Some(key) = call.key.as_deref() else {
                continue;
            };
            let (intent, seq) =
                at.calls.intents.get(key).ok_or_else(|| {
                    format!("turn facts: the intent of call key {key} is unknown")
                })?;
            events.push(CallEvent::Intent(Box::new(intent.clone()), *seq));
        }
        events.extend(
            at.calls
                .terminals_of(&batch.batch_id)
                .iter()
                .cloned()
                .map(CallEvent::Terminal),
        );
    }
    let mut reply: Option<ToolReply> = None;
    // A run's end restating its last turn's facts after itself, as it places no message.
    let mut restated: Option<TurnFacts> = None;
    while let Some(mut record) = records.next() {
        // The seq the record takes.
        let seq = at.first_seq + out.len() as u64;
        match &mut record {
            Record::RunStart(start) => {
                run_sender = input_sender(start);
                run_send = run_send_facts(start, at.operations);
                step_mid = None;
                results_mid = None;
            }
            // A step or batch start announces the id of the message it will write, minted from
            // the writer generation and the seq this record takes.
            Record::StepStart(start) => {
                // A step started again after an attempt that wrote no message (a pause, a
                // refusal) writes the message that attempt announced. On today's WAL storage a
                // message's id is its position, which the earlier attempt never took, so the
                // retried step's message keeps the same id there; this keeps it here too.
                let retried = step_mid
                    .take()
                    .filter(|mid| !step_placed && !newest_assistant_is(at.conversation, mid));
                step_mid = Some(
                    start
                        .mid
                        .get_or_insert_with(|| retried.unwrap_or(format!("{}.{seq}", at.prefix)))
                        .clone(),
                );
                step_placed = false;
                results_mid = None;
            }
            Record::Assistant(_) => step_placed = true,
            Record::BatchStart(batch) => {
                results_mid = Some(
                    batch
                        .mid
                        .get_or_insert(format!("{}.{seq}", at.prefix))
                        .clone(),
                );
                dispatched.clear();
            }
            Record::ToolIntent(intent) => {
                dispatched.push(intent.tool_call_id.clone());
                events.push(CallEvent::Intent(Box::new(intent.clone()), Some(seq)));
            }
            Record::ToolReply(next) => reply = Some(next.clone()),
            Record::ToolOutcome(outcome) => {
                // The outcome answers a call an intent anchored, or else a call answered
                // without a dispatch intent, which takes a key minted from the writer
                // generation and the seq its outcome takes, in the form of the store's ids.
                match dispatched.iter().position(|id| *id == outcome.tool_call_id) {
                    Some(position) => {
                        dispatched.remove(position);
                    }
                    None if outcome.call_key.is_none() => {
                        outcome.call_key = Some(format!("{}.{seq}", at.prefix));
                    }
                    None => {}
                }
                if let Some(reply) = reply
                    .take()
                    .filter(|reply| reply.result.tool_call_id == outcome.tool_call_id)
                {
                    events.push(CallEvent::Terminal(Terminal::of(&reply, outcome, seq)));
                }
            }
            Record::RunEnd(end) if end.reason == broca_protocol::RunFinishReason::Cancelled => {
                let latest = latest_turn.clone().or_else(|| newest_turn(at.conversation));
                if let Some(turn) = latest.and_then(cancelled) {
                    // Ahead of the messages the run's end places, so the facts share a sealed
                    // block with their turn; right after the run's end when it places none.
                    let placed_here = out
                        .iter()
                        .rev()
                        .take_while(|record| matches!(record, Record::Input(_)))
                        .count();
                    if placed_here > 0 {
                        out.insert(out.len() - placed_here, Record::TurnFacts(turn.clone()));
                    } else {
                        restated = Some(turn.clone());
                    }
                    latest_turn = Some(turn);
                    inserted = true;
                }
            }
            _ => {}
        }
        if record.stream() == broca_session_store::record::Stream::Transcript {
            let open_turn = counts.open_tool_turn().cloned();
            counts
                .apply(&record)
                .map_err(|error| format!("entry facts: conversation {error:?}"))?;
            counts.clear_window();
            if let (Some(assistant), Record::ToolReply(_), None) =
                (open_turn, &record, counts.open_tool_turn())
            {
                // The reply completed the turn, which placed the assistant message and then its
                // results' message.
                placed_turn = Some((counts.placed - 1, assistant));
            }
            let now_known = known_of(&counts);
            for ordinal in known + 1..=now_known {
                let (message, facts) = describe(&record, at, run_sender.as_ref(), &run_send)?;
                // The message a step or batch announced takes the id it announced: a step's
                // assistant message, and the tool-results message a batch's last reply places.
                let announced = match &record {
                    Record::Assistant(_) => step_mid.clone(),
                    Record::ToolReply(_) if ordinal == now_known => results_mid.clone(),
                    _ => None,
                };
                let own = facts.written_at_ms;
                // On the contract a message without its own time takes the newest earlier
                // entry's, as `session.entries` serves it. Off it, only its own, as
                // `session.read` serves it there, where a zero stamp means none
                // (`broca_core::message_times::fold`).
                let written_at_ms = if entries {
                    let written = own.or(time).unwrap_or(0);
                    time = Some(written);
                    Some(written)
                } else {
                    own.filter(|at| *at != 0)
                };
                // A run's assistant messages carry its Rich UI stamp, as reads serve it.
                let stamped = message
                    .filter(|message| message.role == Role::Assistant && facts.run_id.is_some())
                    .and(at.rich_ui.cloned());
                pending.push(EntryFacts {
                    client_message_id: facts.send.client_message_id,
                    sender: facts.send.sender,
                    marks: None,
                    no_display_facts: None,
                    reasoning_summaries: message.map(reasoning_summaries).unwrap_or_default(),
                    rich_ui: stamped,
                    ordinal,
                    written_at_ms,
                    tag_base: high,
                    notice: facts.notice.filter(|_| entries),
                    usage: facts.usage.filter(|_| entries),
                    generated_under: facts.generated_under.filter(|_| entries),
                    native: facts.native,
                    render_tags: at.tags.render,
                    clean_reply: facts.clean_reply,
                    // Empty until `flush` mints one: only a message nothing announced takes an
                    // id from the seq of its own facts.
                    mid: announced.unwrap_or_default(),
                    run_id: facts.run_id,
                    created_at_ms: facts.created_at_ms,
                    delivery_mode: facts.delivery_mode.map(str::to_owned),
                    delivery_tool_call_id: None,
                });
                high = high
                    .checked_add(if entries {
                        message.map_or(0, numbered_parts)
                    } else {
                        0
                    })
                    .filter(|high| *high <= 9_007_199_254_740_991)
                    .ok_or("compaction tag numbers exceed the protocol integer range")?;
            }
            known = now_known;
        }
        // A budget reminder's firing placed it, just before, so its facts follow it at once.
        let reminder =
            matches!(&record, Record::Input(input) if input.placement == Placement::BudgetNudge);
        let continues = !reminder
            && matches!(
                (&record, records.peek()),
                (Record::Assistant(_), Some(Record::StepEnd(_)))
                    | (Record::ToolReply(_), Some(Record::ToolOutcome(_)))
                    // Messages placed at a boundary enter with the record that places them: the
                    // step's start, or the run's end.
                    | (
                        Record::Input(_),
                        Some(Record::Input(_) | Record::StepStart(_) | Record::RunEnd(_))
                    )
            );
        // A builder may name a later record of its own batch by its place in the batch (a call
        // key names its intent's seq), so no record that can be named may follow inserted facts.
        if inserted && matches!(record, Record::ToolIntent(_)) {
            return Err("entry facts: a tool intent follows a placed message in one batch".into());
        }
        out.push(record);
        if let Some(turn) = restated.take() {
            out.push(Record::TurnFacts(turn));
        }
        if !continues && !pending.is_empty() {
            flush(&mut out, &mut pending, at);
            inserted = true;
        }
        if !continues {
            if let Some((ordinal, assistant)) = placed_turn.take() {
                let turn = turn_facts(ordinal, &assistant, &events);
                out.push(Record::TurnFacts(turn.clone()));
                latest_turn = Some(turn);
                inserted = true;
            }
        }
    }
    flush(&mut out, &mut pending, at);
    if let Some((ordinal, assistant)) = placed_turn.take() {
        out.push(Record::TurnFacts(turn_facts(ordinal, &assistant, &events)));
    }
    Ok(out)
}

/// One thing recorded about a call of the open turn: its dispatch intent, with the intent's seq
/// where known, or its terminal.
#[derive(Debug, Clone)]
pub enum CallEvent {
    Intent(Box<ToolIntent>, Option<u64>),
    Terminal(Terminal),
}

impl CallEvent {
    fn seq(&self) -> Option<u64> {
        match self {
            CallEvent::Intent(_, seq) => *seq,
            CallEvent::Terminal(terminal) => Some(terminal.seq),
        }
    }
}

/// The facts of the tool turn whose assistant message, at `ordinal`, made the calls in
/// `assistant`, from what its calls recorded (`events`), by today's display rules
/// (`broca_core::display_facts` and `broca_core::call_identity`), so a read serves each call as
/// today's read serves it:
///
/// - Each copy of a call id has its own identity: an intent takes the first copy not yet sent or
///   closed, a refusal the first without a key, and a result the first not yet closed. A copy
///   that took no intent takes the key its outcome recorded (`ToolOutcome::call_key`). A copy
///   with neither has no facts.
/// - Times and why a call has no answer go to the first copy of its id, each later record
///   overwriting what an earlier one set, as today's read keeps them.
pub fn turn_facts(ordinal: u64, assistant: &Message, events: &[CallEvent]) -> TurnFacts {
    struct Slot<'a> {
        id: &'a ToolCallId,
        tool_name: &'a str,
        key: Option<String>,
        schema_pin: Option<String>,
        dispatched_to: Option<String>,
        dispatched: bool,
        closed: bool,
        started: Option<u64>,
        finished: Option<u64>,
        unanswered: Option<Unanswered>,
    }
    let mut slots: Vec<Slot<'_>> = assistant
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(Slot {
                id: &call.tool_call_id,
                tool_name: &call.tool_name,
                key: None,
                schema_pin: None,
                dispatched_to: None,
                dispatched: false,
                closed: false,
                started: None,
                finished: None,
                unanswered: None,
            }),
            _ => None,
        })
        .collect();
    // Intents whose seq isn't known were recorded when their batch started, before anything the
    // writer saw after.
    let mut ordered: Vec<&CallEvent> = events.iter().collect();
    ordered.sort_by_key(|event| event.seq().map_or((0, 0), |seq| (1, seq)));
    for event in ordered {
        match event {
            CallEvent::Intent(intent, _) => {
                let id = &intent.tool_call_id;
                if let Some(slot) = slots
                    .iter_mut()
                    .find(|slot| slot.id == id && !slot.dispatched && !slot.closed)
                {
                    slot.dispatched = true;
                    if intent.call_key.is_some() {
                        slot.key.clone_from(&intent.call_key);
                    }
                    slot.dispatched_to.clone_from(&intent.module_id);
                    slot.schema_pin = intent.schema_pin.as_ref().and_then(|pin| {
                        served_schema_pin(
                            intent.tool_name.as_deref().unwrap_or(slot.tool_name),
                            pin,
                        )
                    });
                }
                if let Some(first) = slots.iter_mut().find(|slot| slot.id == id) {
                    first.started = intent.dispatched_at_ms;
                }
            }
            CallEvent::Terminal(terminal) => {
                let id = &terminal.tool_call_id;
                let open = |slot: &Slot<'_>| slot.id == id && !slot.closed;
                let position = if terminal.refused {
                    slots
                        .iter()
                        .position(|slot| open(slot) && slot.key.is_none())
                        .or_else(|| slots.iter().position(open))
                } else {
                    slots.iter().position(open)
                };
                if let Some(slot) = position.map(|position| &mut slots[position]) {
                    if slot.key.is_none() {
                        slot.key.clone_from(&terminal.call_key);
                    }
                    slot.closed = true;
                }
                if let Some(first) = slots.iter_mut().find(|slot| slot.id == id) {
                    if terminal.answered {
                        first.finished = terminal.finished_at_ms;
                    } else if terminal.unanswered.is_some() {
                        first.unanswered = terminal.unanswered;
                    }
                }
            }
        }
    }
    let calls = slots
        .into_iter()
        .filter_map(|slot| {
            Some(CallFacts {
                tool_call_id: slot.id.clone(),
                call_key: slot.key?,
                schema_pin: slot.schema_pin.map(serde_json::Value::String),
                carrier: None,
                dispatched_to: slot.dispatched_to,
                dispatched_at_ms: slot.started,
                finished_at_ms: slot.finished,
                unanswered: slot.unanswered,
            })
        })
        .collect();
    TurnFacts { ordinal, calls }
}

/// The summaries `message`'s reasoning blocks carry, as `session.read` serves them with display
/// facts (llm-runner's `display::reasoning_summary`): one per block that has one, with the
/// block's position among the message's content blocks. Only an assistant message has any.
pub fn reasoning_summaries(message: &Message) -> Vec<ReasoningSummary> {
    if message.role != Role::Assistant {
        return Vec::new();
    }
    message
        .content
        .iter()
        .enumerate()
        .filter_map(|(part, block)| {
            let stored = serde_json::to_value(block).ok()?;
            let text = cortexkit_role_llm_runner::display::reasoning_summary(&stored)?;
            Some(ReasoningSummary {
                part: u32::try_from(part).ok()?,
                text,
            })
        })
        .collect()
}

/// A run's Rich UI stamp as its messages' entry facts record it.
pub fn rich_ui(stamp: &broca_protocol::RichUiStamp) -> Option<RichUi> {
    let stamp = serde_json::to_value(stamp).ok()?;
    Some(RichUi {
        dialect: stamp.get("dialect")?.as_str()?.to_owned(),
        catalog_digest: stamp.get("catalog_digest")?.as_str()?.to_owned(),
    })
}

/// A dispatch intent's schema pin (`ToolIntent::schema_pin`, the pin its tool provider declared)
/// as reads serve it: llm-runner's encoded `SchemaPin` for the tool `tool_name`.
pub fn served_schema_pin(tool_name: &str, pin: &serde_json::Value) -> Option<String> {
    let pin: broca_wal::DispatchSchemaPin = serde_json::from_value(pin.clone()).ok()?;
    cortexkit_role_tool_provider::call::SchemaPin::new(tool_name, &pin.schema_digest, pin.semantics)
        .encode()
        .ok()
}

/// The facts of the conversation's newest assistant message, when it made tool calls that have
/// all been answered: the turn today's reads restate when its run ends cancelled.
fn newest_turn(conversation: &Conversation) -> Option<TurnFacts> {
    if conversation.open_tool_turn().is_some() {
        return None;
    }
    conversation
        .messages
        .iter()
        .zip(&conversation.entries)
        .rev()
        .find(|(message, _)| message.role == Role::Assistant)
        .and_then(|(_, entry)| entry.turn.clone())
}

/// `turn` with each call abandoned as interrupted restated as cancelled, as today's reads state
/// a run's last turn once the run ends cancelled; `None` when none was.
fn cancelled(mut turn: TurnFacts) -> Option<TurnFacts> {
    let mut changed = false;
    for call in &mut turn.calls {
        if call.unanswered == Some(Unanswered::Interrupted) {
            call.unanswered = Some(Unanswered::Cancelled);
            changed = true;
        }
    }
    changed.then_some(turn)
}

/// Who sent the send that placed a message, and the `client_message_id` it carried, as today's
/// display facts name them for every message of a run's input or of a steer or delivered send.
#[derive(Debug, Clone, Default)]
struct SendFacts {
    sender: Option<String>,
    client_message_id: Option<String>,
}

/// The send facts of the input of the run `start` starts, by today's rules: for a fresh run, the
/// queued send it consumes names them (`operations` holds it in its queue until the run starts),
/// else the run's own start: its sender's module (whoever sent on the owner's behalf, else the
/// run's principal), and the `client_message_id` its request identity carries. A run that isn't
/// fresh has none.
fn run_send_facts(
    start: &broca_session_store::record::RunStart,
    operations: &Operations,
) -> SendFacts {
    let fresh = start
        .origin
        .as_ref()
        .and_then(|origin| origin.get("kind"))
        .and_then(serde_json::Value::as_str)
        == Some("fresh");
    if !fresh {
        return SendFacts::default();
    }
    let queued = start.submission_id.as_ref().and_then(|submission| {
        operations
            .queue
            .iter()
            .find(|pending| &pending.submission_id == submission)
    });
    let decode = |value: Option<&serde_json::Value>| {
        value.and_then(|value| {
            serde_json::from_value::<broca_protocol::BindPrincipal>(value.clone()).ok()
        })
    };
    let principal = decode(
        start
            .run_choices
            .as_ref()
            .and_then(|choices| choices.get("record"))
            .and_then(|record| record.get("sent_by")),
    )
    .or_else(|| decode(start.principal.as_ref()));
    let module = match principal {
        Some(broca_protocol::BindPrincipal::Reserved { module_id }) => Some(module_id),
        _ => None,
    };
    SendFacts {
        sender: queued.and_then(|send| send.sender.clone()).or(module),
        client_message_id: queued
            .and_then(|send| send.request_identity_digest.as_deref())
            .and_then(broca_core::digest_client_message_id)
            .or_else(|| {
                start
                    .request_identity_digest
                    .as_deref()
                    .and_then(broca_core::digest_client_message_id)
            }),
    }
}

/// The send facts of a steer or delivered send's message placed by `input`, from the pending
/// input it was accepted as (`receipt_seq`).
fn held_send_facts(input: &broca_session_store::record::Input, at: &Placing<'_>) -> SendFacts {
    let Some(pending) = at.operations.run.as_ref().and_then(|run| {
        run.pending_steers
            .iter()
            .find(|pending| pending.seq.is_some() && pending.seq == input.receipt_seq)
    }) else {
        return SendFacts::default();
    };
    SendFacts {
        sender: pending.sender.clone(),
        client_message_id: pending
            .request_identity_digest
            .as_deref()
            .and_then(broca_core::digest_client_message_id),
    }
}

/// When an input message was accepted: a steer's or delivered send's acceptance, kept across
/// its wait for a boundary, else the message's own time. A budget reminder or imported message
/// has none, as `broca_core::creation_times` rules.
fn accepted_at(input: &broca_session_store::record::Input, at: &Placing<'_>) -> Option<u64> {
    match input.placement {
        Placement::BudgetNudge | Placement::Imported | Placement::DamageNotice => None,
        Placement::RunInput => input.written_at_ms,
        Placement::Steer | Placement::Delivered | Placement::DeliveredTurnSystem => at
            .operations
            .run
            .as_ref()
            .and_then(|run| {
                run.pending_steers
                    .iter()
                    .find(|pending| pending.seq.is_some() && pending.seq == input.receipt_seq)
            })
            .and_then(|pending| pending.written_at_ms)
            .or(input.written_at_ms),
    }
}

/// Whether the newest assistant message `conversation` holds, held for its tool results or
/// placed, has the id `mid`.
fn newest_assistant_is(conversation: &Conversation, mid: &str) -> bool {
    let named = |facts: Option<&EntryFacts>| facts.is_some_and(|facts| facts.mid == mid);
    if let Some((_, _, state)) = conversation.open_entry() {
        return named(state.facts.as_ref());
    }
    (conversation.window_start()..=conversation.placed)
        .rev()
        .filter_map(|ordinal| conversation.window_entry(ordinal))
        .find(|(message, _)| message.role == Role::Assistant)
        .is_some_and(|(_, state)| named(state.facts.as_ref()))
}

fn flush(out: &mut Vec<Record>, pending: &mut Vec<EntryFacts>, at: &Placing<'_>) {
    for mut facts in pending.drain(..) {
        if facts.mid.is_empty() {
            facts.mid = format!("{}.{}", at.prefix, at.first_seq + out.len() as u64);
        }
        out.push(Record::EntryFacts(facts));
    }
}

/// The sender of a run's user input, as today's entry metadata names it: for a fresh run, the
/// module that sent on the owner's behalf, else the run's principal, when that is a module
/// (`delivery_sender`). A person's input has none. A run that consumes a queued send takes that
/// send's origin instead, which needs the queued send's records: not built yet, so such a run's
/// input names no sender.
fn input_sender(start: &broca_session_store::record::RunStart) -> Option<Notice> {
    let fresh = start
        .origin
        .as_ref()
        .and_then(|origin| origin.get("kind"))
        .and_then(serde_json::Value::as_str)
        == Some("fresh");
    if !fresh || start.submission_id.is_some() {
        return None;
    }
    let decode = |value: Option<&serde_json::Value>| {
        value.and_then(|value| {
            serde_json::from_value::<broca_protocol::BindPrincipal>(value.clone()).ok()
        })
    };
    let sent_by = decode(
        start
            .run_choices
            .as_ref()
            .and_then(|choices| choices.get("record"))
            .and_then(|record| record.get("sent_by")),
    );
    let principal = sent_by.or_else(|| decode(start.principal.as_ref()));
    let sender = broca_core::compaction_v2::entries::delivery_sender(
        principal.as_ref(),
        broca_protocol::DeliveryMode::Steer,
    )?;
    Some(Notice {
        sender,
        template: None,
    })
}

struct Described {
    written_at_ms: Option<u64>,
    /// The run that placed the message, when one owns it.
    run_id: Option<String>,
    /// When the message was accepted (`EntryFacts::created_at_ms`).
    created_at_ms: Option<u64>,
    /// How a delivered send placed the message: `steer`, `turn_system` or `idle_start`.
    delivery_mode: Option<&'static str>,
    notice: Option<Notice>,
    usage: Option<StepUsage>,
    generated_under: Option<ViewRef>,
    native: bool,
    clean_reply: bool,
    /// Who sent the send that placed the message, and its `client_message_id`.
    send: SendFacts,
}

/// Whether the message `record` places was written by broca itself (a model step's message, a
/// tool-results message, or a budget reminder), rather than given as input. A model step's
/// message is complete when its tool calls all have results.
pub fn native(record: &Record) -> bool {
    match record {
        Record::Input(input) => input.placement == Placement::BudgetNudge,
        Record::Assistant(_) | Record::ToolReply(_) => true,
        _ => false,
    }
}

/// The message `record` placed or reserved, and its facts besides number and time. A tool result
/// that completes its turn places the tool-results message, which has no numbered parts.
fn describe<'r>(
    record: &'r Record,
    at: &Placing<'_>,
    run_sender: Option<&Notice>,
    run_send: &SendFacts,
) -> Result<(Option<&'r Message>, Described), String> {
    Ok(match record {
        // A budget reminder is the runner's own message, sent as a notice from broca, as
        // today's entries describe it.
        Record::Input(input) if input.placement == Placement::BudgetNudge => (
            Some(&input.message),
            Described {
                written_at_ms: input.written_at_ms,
                run_id: Some(input.run_id.clone()),
                created_at_ms: None,
                delivery_mode: None,
                notice: Some(Notice {
                    sender: "broca".into(),
                    template: None,
                }),
                usage: None,
                generated_under: None,
                native: native(record),
                clean_reply: false,
                send: SendFacts::default(),
            },
        ),
        Record::Input(input) => (
            Some(&input.message),
            Described {
                written_at_ms: input.written_at_ms,
                // An imported seed belongs to no run.
                run_id: (input.placement != Placement::Imported).then(|| input.run_id.clone()),
                created_at_ms: accepted_at(input, at),
                // A run started from a queued send with a delivery order (`idle_start`) and a send
                // riding on a tool result (`tool_result`) aren't named here: a session in the new
                // storage takes neither yet, and refuses both by name.
                delivery_mode: match input.placement {
                    Placement::Delivered => Some("steer"),
                    Placement::DeliveredTurnSystem => Some("turn_system"),
                    _ => None,
                },
                notice: match input.placement {
                    Placement::RunInput => run_sender.cloned(),
                    _ => input.notice.clone(),
                }
                .filter(|_| input.message.role == Role::User),
                usage: None,
                generated_under: None,
                native: native(record),
                clean_reply: at.tags.clean_replies && input.message.role == Role::Assistant,
                send: match input.placement {
                    Placement::RunInput => run_send.clone(),
                    Placement::Steer | Placement::Delivered | Placement::DeliveredTurnSystem => {
                        held_send_facts(input, at)
                    }
                    Placement::BudgetNudge | Placement::Imported | Placement::DamageNotice => {
                        SendFacts::default()
                    }
                },
            },
        ),
        Record::Assistant(assistant) => (
            Some(&assistant.message),
            Described {
                // A step that recorded no written time takes its completing attempt's finish,
                // as `broca_core::message_times` rules.
                written_at_ms: assistant.written_at_ms.or_else(|| {
                    at.operations
                        .run
                        .as_ref()
                        .and_then(|run| run.step_attempt.as_ref())
                        .filter(|attempt| attempt.step_id == assistant.step_id)
                        .map(|attempt| attempt.ts_ms)
                }),
                run_id: Some(assistant.run_id.clone()),
                created_at_ms: at
                    .operations
                    .run
                    .as_ref()
                    .and_then(|run| run.step_request.as_ref())
                    .filter(|request| request.step_id == assistant.step_id)
                    .and_then(|request| request.sent_at_ms),
                delivery_mode: None,
                notice: None,
                usage: assistant.usage.and_then(step_usage).map(|usage| {
                    // `StepUsage` from the contract crate can't be read field by field outside
                    // it; its JSON form is its contract.
                    let value = serde_json::to_value(usage).expect("usage serializes");
                    let count = |key: &str| value.get(key).and_then(serde_json::Value::as_u64);
                    StepUsage {
                        prompt_tokens: count("prompt_tokens"),
                        cache_read_tokens: count("cache_read_tokens"),
                        cache_write_tokens: count("cache_write_tokens"),
                        output_tokens: count("output_tokens"),
                        reasoning_tokens: count("reasoning_tokens"),
                    }
                }),
                generated_under: at.view.clone(),
                native: native(record),
                clean_reply: at.tags.clean_replies,
                send: SendFacts::default(),
            },
        ),
        Record::ToolReply(reply) => (
            None,
            Described {
                written_at_ms: reply.finished_at_ms,
                run_id: Some(reply.run_id.clone()),
                // A tool-results message has no creation time of its own.
                created_at_ms: None,
                delivery_mode: None,
                notice: None,
                usage: None,
                generated_under: None,
                native: native(record),
                clean_reply: false,
                send: SendFacts::default(),
            },
        ),
        other => return Err(format!("entry facts: {} places no message", other.name())),
    })
}

/// The ordinals the conversation knows: those it placed and the one it reserved for an
/// assistant message still waiting for its tool results.
fn known_of(conversation: &Conversation) -> u64 {
    conversation.placed + u64::from(conversation.open_tool_turn().is_some())
}

/// The highest part number so far: the newest message's base plus its own numbered parts, or
/// the marker's checkpoint when the window is empty.
fn high_water(conversation: &Conversation) -> Result<u64, String> {
    let newest = match (&conversation.open_entry, conversation.open_tool_turn()) {
        (Some(entry), Some(message)) => Some((message, entry)),
        _ => conversation
            .messages
            .last()
            .zip(conversation.entries.last()),
    };
    match newest {
        Some((message, entry)) => {
            let facts = entry
                .facts
                .as_ref()
                .ok_or("entry facts: the newest message has none")?;
            Ok(facts.tag_base + numbered_parts(message))
        }
        None => Ok(conversation
            .marker
            .as_ref()
            .map_or(0, |marker| marker.checkpoint)),
    }
}

/// The newest entry's time, which a message without its own time takes.
fn last_time(conversation: &Conversation) -> Option<u64> {
    conversation
        .open_entry
        .as_ref()
        .filter(|_| conversation.open_tool_turn().is_some())
        .or(conversation.entries.last())
        .and_then(|entry| entry.facts.as_ref())
        .and_then(|facts| facts.written_at_ms)
}

/// The compaction entry for a message the new format stored, built only from what it stored:
/// the message, the message after it, its entry facts and the text delivered with its calls.
/// Projected with today's projection function, so the result can be compared with today's
/// entry.
pub fn project(stored: &StoredEntry) -> Result<Entry, String> {
    use broca_core::compaction_v2::entries::{project, Deliveries, EntryFacts as Facts};
    let facts = stored
        .state
        .facts
        .as_ref()
        .ok_or_else(|| format!("message {} has no entry facts", stored.ordinal))?;
    let mid = facts.mid.as_str();
    // Only a session on the compaction contract serves entries, and its facts always carry a
    // time.
    let written_at = facts
        .written_at_ms
        .ok_or_else(|| format!("message {} has entry facts with no time", stored.ordinal))?;
    let usage = facts.usage.map(|usage| {
        let mut out = cortexkit_role_compaction::read::StepUsage::default();
        out.prompt_tokens = usage.prompt_tokens;
        out.cache_read_tokens = usage.cache_read_tokens;
        out.cache_write_tokens = usage.cache_write_tokens;
        out.output_tokens = usage.output_tokens;
        out.reasoning_tokens = usage.reasoning_tokens;
        out
    });
    let generated_under = facts
        .generated_under
        .as_ref()
        .map(|view| {
            serde_json::from_value(serde_json::json!({
                "version": view.version,
                "checksum": view.checksum,
            }))
        })
        .transpose()
        .map_err(|e| e.to_string())?;
    let notice = facts.notice.as_ref().map(|notice| {
        let mut origin = cortexkit_role_compaction::read::NoticeOrigin::new(&notice.sender);
        origin.template = notice.template.clone();
        origin
    });
    let first = Facts {
        mid: mid.to_owned(),
        ordinal: stored.ordinal,
        tag_base: Some(facts.tag_base),
        written_at,
        completed_at: (stored.message.role == Role::Assistant && facts.native)
            .then_some(written_at),
        notice,
        usage,
        generated_under,
    };
    let mut messages = vec![stored.message.clone()];
    let mut all = vec![first];
    if let Some(next) = &stored.next {
        messages.push(next.clone());
        all.push(Facts {
            mid: format!("{mid}:next"),
            ordinal: stored.ordinal + 1,
            tag_base: Some(facts.tag_base),
            written_at,
            ..Facts::default()
        });
    }
    let mut deliveries = Deliveries::new();
    for delivered in &stored.state.delivered {
        let notice = delivered
            .notice
            .as_ref()
            .ok_or("delivered text has no sender")?;
        let text = delivered
            .text
            .strip_prefix("\n\n")
            .unwrap_or(&delivered.text);
        let mut out = cortexkit_role_compaction::part::Delivered::new(&notice.sender, text);
        out.template = notice.template.clone();
        deliveries
            .entry((mid.to_owned(), delivered.part.to_string()))
            .or_default()
            .push(out);
    }
    project(&messages, &all, &deliveries, true)
        .into_iter()
        .next()
        .ok_or_else(|| "empty projection".into())
}
