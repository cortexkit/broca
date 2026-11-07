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

use broca_core::compaction_v2::metadata::{numbered_parts, step_usage};
use broca_protocol::content::{Message, Role};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::{EntryFacts, Notice, Placement, Record, StepUsage, ViewRef};
use broca_session_store::session::{SessionError, StoredEntry};
use cortexkit_role_compaction::read::Entry;

use crate::writer::Writer;

/// A writer that adds entry facts to every batch it appends, so no place in the loop that
/// writes a message can forget them. Every session records them: `session.read` serves each
/// message from them. Only a session on the current compaction contract records the facts
/// `session.entries` serves (see [`Tags::entries`]).
pub struct Placed<'w, W> {
    pub inner: &'w mut W,
    /// The run's tag rules.
    pub facts: Tags,
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
        let tags = self.facts;
        let prefix = self.inner.message_id_prefix();
        let records = with_facts(
            records.to_vec(),
            &Placing {
                conversation: self.inner.conversation(),
                first_seq: self.inner.next_seq(),
                prefix: &prefix,
                tags,
                view: written_under(self.inner.operations()),
                operations: self.inner.operations(),
            },
        )
        .map_err(SessionError::Fold)?;
        self.inner.append(&records, barrier).await
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
        let tags = self.facts;
        let prefix = self.inner.message_id_prefix();
        let mut failure = None;
        let records = self
            .inner
            .append_with(
                &mut |conversation, operations, first_seq| {
                    let batch = build(conversation, operations, first_seq);
                    let placing = Placing {
                        conversation,
                        first_seq,
                        prefix: &prefix,
                        tags,
                        view: written_under(operations),
                        operations,
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
            None => Ok(records),
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
    pub tags: Tags,
    /// The compaction view the current step rendered with.
    pub view: Option<ViewRef>,
    /// The operational state before the batch: the step's request, the pending steers and the
    /// queue the batch's messages come from.
    pub operations: &'a Operations,
}

/// `batch` with an `EntryFacts` record after the records that place or reserve each message:
/// after an assistant message and its step's end, after a tool result and its outcome, and after
/// a run of input messages together with the step start or run end that places them, as the
/// converter places them for a converted session.
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
    // Who sent the input of a run started in this batch, read from its start as today's entry
    // metadata reads it from the run's start.
    let mut run_sender = None;
    let mut records = batch.into_iter().peekable();
    while let Some(record) = records.next() {
        if let Record::RunStart(start) = &record {
            run_sender = input_sender(start);
        }
        if record.stream() == broca_session_store::record::Stream::Transcript {
            counts
                .apply(&record)
                .map_err(|error| format!("entry facts: conversation {error:?}"))?;
            counts.clear_window();
            let now_known = known_of(&counts);
            for ordinal in known + 1..=now_known {
                let (message, facts) = describe(&record, at, run_sender.as_ref())?;
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
                pending.push(EntryFacts {
                    ordinal,
                    written_at_ms,
                    tag_base: high,
                    notice: facts.notice.filter(|_| entries),
                    usage: facts.usage.filter(|_| entries),
                    generated_under: facts.generated_under.filter(|_| entries),
                    native: facts.native,
                    render_tags: at.tags.render,
                    clean_reply: facts.clean_reply,
                    mid: String::new(),
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
        if !continues && !pending.is_empty() {
            flush(&mut out, &mut pending, at);
            inserted = true;
        }
    }
    flush(&mut out, &mut pending, at);
    Ok(out)
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

fn flush(out: &mut Vec<Record>, pending: &mut Vec<EntryFacts>, at: &Placing<'_>) {
    for mut facts in pending.drain(..) {
        facts.mid = format!("{}.{}", at.prefix, at.first_seq + out.len() as u64);
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
