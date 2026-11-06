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

use async_trait::async_trait;
use broca_core::compaction_v2::metadata::{numbered_parts, step_usage};
use broca_protocol::content::{Message, Role};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::{EntryFacts, Notice, Placement, Record, StepUsage, ViewRef};
use broca_session_store::session::{SessionError, StoredEntry};
use cortexkit_role_compaction::read::Entry;

use crate::writer::Writer;

/// A writer that adds entry facts to every batch it appends, so no place in the loop that
/// writes a message can forget them. Only a session on the current compaction contract records
/// entry facts; for any other, `facts` is `None` and batches pass through unchanged.
pub struct Placed<'w, W> {
    pub inner: &'w mut W,
    /// The run's tag rules, for a session that records entry facts.
    pub facts: Option<Tags>,
}

/// The view a model-written message was written under, as today's projection decides it: the
/// newest view a step applied, which the step that writes the message applied or kept.
fn written_under(operations: &Operations) -> Option<ViewRef> {
    operations.view_applied.as_ref().map(|applied| ViewRef {
        version: applied.view.version,
        checksum: applied.view.checksum.clone(),
    })
}

#[async_trait(?Send)]
impl<W: Writer> Writer for Placed<'_, W> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        let Some(tags) = self.facts else {
            return self.inner.append(records, barrier).await;
        };
        let prefix = self.inner.message_id_prefix();
        let records = with_facts(
            records.to_vec(),
            &Placing {
                conversation: self.inner.conversation(),
                first_seq: self.inner.next_seq(),
                prefix: &prefix,
                tags,
                view: written_under(self.inner.operations()),
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
        let Some(tags) = self.facts else {
            return self.inner.append_with(build, barrier).await;
        };
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
}

impl Tags {
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
}

/// `batch` with an `EntryFacts` record after the records that place or reserve each message:
/// after an assistant message and its step's end, after a tool result and its outcome, and after
/// a run of input messages together with the step start or run end that places them, as the
/// converter places them for a converted session.
pub fn with_facts(batch: Vec<Record>, at: &Placing<'_>) -> Result<Vec<Record>, String> {
    let mut counts = at.conversation.without_window();
    let mut known = known_of(&counts);
    let mut inserted = false;
    let mut high = high_water(at.conversation)?;
    let mut time = last_time(at.conversation);
    let mut out = Vec::with_capacity(batch.len() + 2);
    let mut pending: Vec<EntryFacts> = Vec::new();
    let mut records = batch.into_iter().peekable();
    while let Some(record) = records.next() {
        if record.stream() == broca_session_store::record::Stream::Transcript {
            counts
                .apply(&record)
                .map_err(|error| format!("entry facts: conversation {error:?}"))?;
            counts.clear_window();
            let now_known = known_of(&counts);
            for ordinal in known + 1..=now_known {
                let (message, facts) = describe(&record, at)?;
                let own = facts.written_at_ms;
                let written_at_ms = own.or(time).unwrap_or(0);
                time = Some(written_at_ms);
                pending.push(EntryFacts {
                    ordinal,
                    written_at_ms,
                    tag_base: high,
                    notice: facts.notice,
                    usage: facts.usage,
                    generated_under: facts.generated_under,
                    native: facts.native,
                    render_tags: at.tags.render,
                    clean_reply: facts.clean_reply,
                    mid: String::new(),
                });
                high = high
                    .checked_add(message.map_or(0, numbered_parts))
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

fn flush(out: &mut Vec<Record>, pending: &mut Vec<EntryFacts>, at: &Placing<'_>) {
    for mut facts in pending.drain(..) {
        facts.mid = format!("{}.{}", at.prefix, at.first_seq + out.len() as u64);
        out.push(Record::EntryFacts(facts));
    }
}

struct Described {
    written_at_ms: Option<u64>,
    notice: Option<Notice>,
    usage: Option<StepUsage>,
    generated_under: Option<ViewRef>,
    native: bool,
    clean_reply: bool,
}

/// The message `record` placed or reserved, and its facts besides number and time. A tool result
/// that completes its turn places the tool-results message, which has no numbered parts.
fn describe<'r>(
    record: &'r Record,
    at: &Placing<'_>,
) -> Result<(Option<&'r Message>, Described), String> {
    Ok(match record {
        // A budget reminder is the runner's own message, sent as a notice from broca, as
        // today's entries describe it.
        Record::Input(input) if input.placement == Placement::BudgetNudge => (
            Some(&input.message),
            Described {
                written_at_ms: input.written_at_ms,
                notice: Some(Notice {
                    sender: "broca".into(),
                    template: None,
                }),
                usage: None,
                generated_under: None,
                native: true,
                clean_reply: false,
            },
        ),
        Record::Input(input) => (
            Some(&input.message),
            Described {
                written_at_ms: input.written_at_ms,
                notice: input
                    .notice
                    .clone()
                    .filter(|_| input.message.role == Role::User),
                usage: None,
                generated_under: None,
                native: false,
                clean_reply: at.tags.clean_replies && input.message.role == Role::Assistant,
            },
        ),
        Record::Assistant(assistant) => (
            Some(&assistant.message),
            Described {
                written_at_ms: assistant.written_at_ms,
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
                native: true,
                clean_reply: at.tags.clean_replies,
            },
        ),
        Record::ToolReply(reply) => (
            None,
            Described {
                written_at_ms: reply.finished_at_ms,
                notice: None,
                usage: None,
                generated_under: None,
                native: true,
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
        .map(|facts| facts.written_at_ms)
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
        written_at: facts.written_at_ms,
        completed_at: (stored.message.role == Role::Assistant && facts.native)
            .then_some(facts.written_at_ms),
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
            written_at: facts.written_at_ms,
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
