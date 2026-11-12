//! The media fold: the facts broca's image engine reads, produced from the store's own records.
//!
//! "Media" means every medium a request can hold: an image, a PDF. For each one in a request,
//! broca must know which capture it is, whether it is still protected from aging, and whether it
//! has aged out. The engine that decides this lives outside the store and reads
//! [`MediaEvent`]s: content-free facts made of ids, conversation positions, media references and
//! flags. This fold reads each record as the store folds it, when a session loads its open log
//! and when it appends, and turns the records into those facts. It keeps no record.
//!
//! A session that has captured no medium holds four scalars here and nothing else: the step
//! counter, the latest step started in the current run, whether a step is open, and the latest
//! tool batch's call order. Nothing is handed over for it, not even a step's start or end. Its
//! first capture hands over those scalars ([`MediaStart`]) for the engine to start from, and from
//! then on every fact is handed over ([`MediaFeed`]).
//!
//! Two facts the store's other folds already hold are read from them instead of being kept here
//! again: the ordinal the conversation reserved for the assistant message waiting for its tool
//! results, and the current run's render config. So [`MediaFold::apply`] takes both folds as they
//! stand before the record.
//!
//! Ordinals in events count from 0, as `Message::ordinal` does. The store's `EntryFacts` count
//! from 1.

use std::collections::VecDeque;

use broca_protocol::content::{ToolOutput, ToolOutputPart, ToolResult};
use broca_protocol::ids::{StepId, ToolCallId};
use broca_protocol::media_event::{MediaEvent, MediaInput, MediaLimits, MediaMessage, MediaPart};

use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;
use crate::fold::Bound;
use crate::record::{Input, MediaObject, PartKind, PartRef, Placement, Record};

crate::fold::state! {
    /// The media fold's state. A log's checkpoint carries it (`session::MediaIndexRef`), so a
    /// session that has captured nothing keeps its step counter across a seal without any
    /// saved index.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct MediaFold {
        /// How many model steps have started in the session; a retry of a step doesn't count.
        /// An image stays protected from aging until the step after the one that first showed it
        /// starts, which the engine tells by this counter.
        1 req pub steps: u64 => { owner: "run loop", reset: "never", bound: Bound::Fixed },
        /// The latest step started in the current run, which tells a retry of a step from the
        /// next one.
        2 opt pub last_step: u64 => { owner: "run loop", reset: "RunStart", bound: Bound::Fixed },
        /// A step started and hasn't ended.
        3 req pub step_open: bool => { owner: "run loop", reset: "RunStart", bound: Bound::Fixed },
        /// The latest tool batch's calls in planned order: each call's result sits at the call's
        /// position in the batch's results message. Kept after the batch ends, as the engine
        /// keeps it, since a result that arrives late is still captured at that position.
        4 rep pub batch: ToolCallId => {
            owner: "tool router",
            reset: "BatchStart, or a cut of the transcript",
            bound: Bound::OpenWork("the latest tool batch's planned calls"),
        },
        /// Set at the session's first capture: from then on every fact is handed over.
        5 opt pub captured: Captured => { owner: "media", reset: "never", bound: Bound::Declared },
    }
    runtime {
        /// What the records of the current batch left open.
        work: Option<Box<Work>> => {
            owner: "media",
            reset: "the end of each batch",
            bound: Bound::OpenWork("the records of one batch"),
        },
    }
}

crate::fold::state! {
    /// What the fold keeps once the session has captured a medium.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct Captured {
        /// The seq of the first record whose accepted messages were handed over and aren't
        /// placed yet: their placement starts at this record's first placed copy. The first
        /// capture hands over the messages still waiting then, so this is the first record
        /// waiting; only on a log whose checkpoint names no media state can copies of
        /// messages the engine never held come first, and those are passed over.
        1 opt pub first_accepted: u64 => {
            owner: "queue",
            reset: "a placement, RunStart, or a cut of the transcript",
            bound: Bound::Fixed,
        },
    }
}

/// The records of one batch that are waiting for others of the same batch.
#[derive(Debug, Clone, Default, PartialEq)]
struct Work {
    /// A run start whose input messages are still being read.
    run: Option<OpenRun>,
    /// Input messages placed since a run start or a placement began, waiting for their
    /// `EntryFacts`. The facts follow in the order the messages were placed, so each one
    /// belongs to the oldest message still waiting.
    awaiting: VecDeque<Awaiting>,
    /// A step start held back until the placement written in its batch is handed over, since
    /// the placement must reach the engine first.
    held_step: Option<u64>,
    /// Tool replies whose `ToolOutcome` hasn't come yet. The outcome is the capture's record,
    /// and it follows its reply in the same batch.
    replies: Vec<Reply>,
}

#[derive(Debug, Clone, PartialEq)]
struct OpenRun {
    record: u64,
    render_config: Option<PartRef>,
    /// How many input messages the run has placed so far.
    inputs: usize,
    /// Its input messages that hold a medium.
    media: Vec<RunInput>,
}

#[derive(Debug, Clone, PartialEq)]
struct RunInput {
    /// The message's position among the run's input messages.
    message: usize,
    /// Its ordinal (from 1), once its `EntryFacts` named it.
    ordinal: Option<u64>,
    /// Where the conversation placed it (from 1), for a log written before entry facts were
    /// recorded.
    placed_at: u64,
    skeleton: MediaMessage,
}

#[derive(Debug, Clone, PartialEq)]
struct Awaiting {
    /// Where the conversation placed the message (from 1), for a log written before entry facts
    /// were recorded.
    placed_at: u64,
    what: Awaited,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Awaited {
    /// The run's input holding a medium, by its position in `OpenRun::media`.
    RunInput(usize),
    /// The first placed copy of `Captured::first_accepted`'s messages.
    FirstAccepted,
    /// A message whose ordinal the engine doesn't need.
    Other,
}

#[derive(Debug, Clone, PartialEq)]
struct Reply {
    call: ToolCallId,
    ordinal: Option<u64>,
    media: Vec<MediaPart>,
}

/// What a batch's waiting records hold, for the memory report (see `crate::held`).
impl crate::held::HeldBytes for Work {
    fn heap_bytes(&self) -> usize {
        use std::mem::size_of;
        let reference = |reference: &broca_protocol::content::MediaRef| reference.heap_bytes();
        let skeleton = |message: &MediaMessage| {
            message.slots.capacity() * size_of::<broca_protocol::media_event::MediaSlot>()
                + message
                    .slots
                    .iter()
                    .map(|slot| {
                        reference(&slot.reference)
                            + slot
                                .tool_part
                                .as_ref()
                                .map_or(0, |(call, _)| call.heap_bytes())
                    })
                    .sum::<usize>()
        };
        let Work {
            run,
            awaiting,
            held_step: _,
            replies,
        } = self;
        let run = run.as_ref().map_or(0, |run| {
            run.render_config.heap_bytes()
                + run.media.capacity() * size_of::<RunInput>()
                + run
                    .media
                    .iter()
                    .map(|input| skeleton(&input.skeleton))
                    .sum::<usize>()
        });
        let replies = replies.capacity() * size_of::<Reply>()
            + replies
                .iter()
                .map(|reply| {
                    reply.call.heap_bytes()
                        + reply.media.capacity() * size_of::<MediaPart>()
                        + reply
                            .media
                            .iter()
                            .map(|part| reference(&part.reference))
                            .sum::<usize>()
                })
                .sum::<usize>();
        run + awaiting.capacity() * size_of::<Awaiting>() + replies
    }
}

/// One fact the fold hands over: a complete event, or a run start still missing its media
/// limits, which only the run's render config holds.
#[derive(Debug, Clone, PartialEq)]
pub enum MediaItem {
    Event(MediaEvent),
    RunStart(RunStartNeedingLimits),
}

/// A run start whose media limits are the `media` member of its frozen render config. That
/// config is a setup part, which the store keeps by digest and doesn't decode: the caller reads
/// it from its shared setup parts and completes the event with [`Self::with_limits`].
#[derive(Debug, Clone, PartialEq)]
pub struct RunStartNeedingLimits {
    pub record: u64,
    pub inputs: Vec<MediaInput>,
    /// The setup part holding the run's frozen render config.
    pub render_config: PartRef,
}

impl RunStartNeedingLimits {
    /// The complete event, with the limits read from the run's render config: `None` when the
    /// config has no `media` member (see [`limits_in`]).
    pub fn with_limits(self, limits: Option<MediaLimits>) -> MediaEvent {
        MediaEvent::RunStart {
            record: self.record,
            inputs: self.inputs,
            limits,
        }
    }
}

crate::fold::state! {
    /// What a log's checkpoint says about the session's media: the media fold's state, and the
    /// saved index of the image engine, both current through one seq. An open folds only the
    /// records after it. Written by every change this build makes; a checkpoint without it was
    /// written before it existed, or by an older build, which skips it as an unknown optional
    /// member.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct MediaIndexRef {
        /// The seq the fold state and the saved index are current through.
        1 req pub through: u64 => { owner: "media", reset: "ChangeIntent", bound: Bound::Fixed },
        /// The session object holding the engine's index through `through`. Absent when the
        /// session had captured nothing, and when no index current through `through` could be
        /// saved: after an undo or a recovery cut, or when the change wasn't handed one. A session
        /// that captured something and opens without it builds its index once from all its
        /// records.
        2 opt pub object: String => { owner: "media", reset: "ChangeIntent", bound: Bound::Fixed },
        3 req pub fold: MediaFold => { owner: "media", reset: "ChangeIntent", bound: Bound::Declared },
    }
}

/// Where the media limits in force come from.
#[derive(Debug, Clone, PartialEq)]
pub enum LimitsSource {
    /// Known as they are: `None` when the run froze no media limits, or no run has started.
    Known(Option<MediaLimits>),
    /// The `media` member of the render config this setup part holds.
    Part(PartRef),
}

/// The scalars a session kept while it had captured nothing, as they stood just before its first
/// capture: the engine's index is created from them.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaStart {
    pub steps: u64,
    pub last_step: Option<StepId>,
    pub step_open: bool,
    pub batch: Vec<ToolCallId>,
    /// The current run's media limits, for a first capture in the middle of a run. When the
    /// first capture is a run's input, the run start's own item carries them, and this is
    /// `Known(None)`.
    pub limits: LimitsSource,
}

/// What the fold handed over for some records, in record order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MediaFeed {
    /// Set when these records hold the session's first capture: the engine's index is created
    /// from it, and the first item is that capture's.
    pub start: Option<MediaStart>,
    pub items: Vec<MediaItem>,
    /// The seq of a record the events can't express: a record of the first compaction protocol,
    /// which changes which media show through compaction code the engine doesn't run on events.
    /// Only an index saved after it can carry the session past it; the items after it are
    /// relative to such an index.
    pub needs_saved_index: Option<u64>,
    /// Where the records stored the bytes of the media they name, by digest, in record order:
    /// a medium's bytes are read from the object a record names for them, which its digest
    /// alone doesn't say. Handed over for every record that names one, whether or not the
    /// session captured anything yet, so a queued send's media are known when it is placed.
    pub objects: Vec<(String, MediaObject)>,
}

impl MediaFeed {
    pub fn is_empty(&self) -> bool {
        self.start.is_none()
            && self.items.is_empty()
            && self.needs_saved_index.is_none()
            && self.objects.is_empty()
    }

    /// Appends what later records handed over.
    pub fn extend(&mut self, later: MediaFeed) {
        if self.start.is_none() {
            self.start = later.start;
        }
        self.items.extend(later.items);
        if self.needs_saved_index.is_none() {
            self.needs_saved_index = later.needs_saved_index;
        }
        self.objects.extend(later.objects);
    }

    fn event(&mut self, event: MediaEvent) {
        self.items.push(MediaItem::Event(event));
    }
}

/// The media limits in a decoded render config: its `media` member, unless absent or null.
pub fn limits_in(render_config: &serde_json::Value) -> Option<MediaLimits> {
    render_config
        .get("media")
        .filter(|media| !media.is_null())
        .cloned()
        .map(MediaLimits)
}

/// The media limits in force for the latest run: those a resume refroze, else those its start
/// froze. A refrozen config without media limits leaves the start's in force, as the engine does.
fn run_limits(operations: &Operations) -> LimitsSource {
    let Some(run) = operations.run.as_ref() else {
        return LimitsSource::Known(None);
    };
    if let Some(limits) = run
        .refrozen_render
        .as_ref()
        .and_then(|refrozen| refrozen.get("render_config"))
        .and_then(limits_in)
    {
        return LimitsSource::Known(Some(limits));
    }
    match run
        .start
        .as_ref()
        .and_then(|start| render_config(&start.setup))
    {
        Some(part) => LimitsSource::Part(part),
        None => LimitsSource::Known(None),
    }
}

fn render_config(setup: &[PartRef]) -> Option<PartRef> {
    setup
        .iter()
        .find(|part| part.kind == PartKind::RenderConfig)
        .cloned()
}

/// The media among a tool result's parts, or `None` for a result whose output isn't a list of
/// parts: such a result has no event.
fn result_media(result: &ToolResult) -> Option<Vec<MediaPart>> {
    let ToolOutput::Parts { parts } = &result.output else {
        return None;
    };
    Some(
        parts
            .iter()
            .enumerate()
            .filter_map(|(part, value)| match value {
                ToolOutputPart::Media { media } => Some(MediaPart {
                    part,
                    reference: media.clone(),
                }),
                _ => None,
            })
            .collect(),
    )
}

/// The ordinal (from 0) of the results message of the assistant message waiting for its tool
/// results: the ordinal the conversation reserved for that message, which its `EntryFacts`
/// named, plus one. `None` when no assistant message is waiting.
fn results_ordinal(conversation: &Conversation) -> Option<u64> {
    // The reserved ordinal counts from 1, so it is also the results message's ordinal from 0.
    conversation.open_entry().map(|(reserved, ..)| reserved)
}

impl MediaFold {
    /// The fold for a log whose checkpoint names no fold state: one sealed before the state was
    /// recorded, or by an older build. Whether the session captured anything is unknown, so it
    /// hands over every fact, as for a session that did. The engine is then built once from all
    /// the session's records and takes these facts after them. Accepted messages waiting at the
    /// checkpoint were all handed over by that build, so their placement starts at the first.
    pub fn unknown(operations: &Operations) -> Self {
        let first_accepted = operations
            .run
            .as_ref()
            .and_then(|run| run.pending_steers.first())
            .and_then(|pending| pending.seq);
        Self {
            captured: Some(Captured { first_accepted }),
            ..Self::default()
        }
    }

    /// The scalars this fold keeps, as the start of an index created from them, with the media
    /// limits of the latest run in `operations`.
    pub fn start(&self, operations: &Operations) -> MediaStart {
        MediaStart {
            steps: self.steps,
            last_step: self.last_step.map(StepId),
            step_open: self.step_open,
            batch: self.batch.clone(),
            limits: run_limits(operations),
        }
    }

    /// Whether the session has captured a medium, so that every fact is handed over.
    pub fn has_captured(&self) -> bool {
        self.captured.is_some()
    }

    /// The fold of a session that has captured nothing, with these scalars.
    pub fn from_scalars(
        steps: u64,
        last_step: Option<u64>,
        step_open: bool,
        batch: Vec<ToolCallId>,
    ) -> Self {
        Self {
            steps,
            last_step,
            step_open,
            batch,
            captured: None,
            work: None,
        }
    }

    /// Folds the record at `seq` into the state and hands over what it means to `out`.
    /// `conversation` and `operations` are the store's folds as they stand before the record.
    /// Call [`MediaFold::flush`] at the end of each batch.
    pub fn apply(
        &mut self,
        seq: u64,
        record: &Record,
        conversation: &Conversation,
        operations: &Operations,
        out: &mut MediaFeed,
    ) {
        out.objects.extend(crate::session::record_objects(record));
        if self.settles(record) {
            self.settle(out);
        }
        match record {
            Record::Input(input) => self.input(input, conversation),
            Record::EntryFacts(facts) => self.facts(facts.ordinal, out),
            Record::RunStart(start) => {
                self.last_step = None;
                self.step_open = false;
                if let Some(captured) = self.captured.as_mut() {
                    // The engine forgets accepted messages a run never placed when the next run
                    // starts.
                    captured.first_accepted = None;
                }
                let work = self.work();
                work.replies.clear();
                work.run = Some(OpenRun {
                    record: seq,
                    render_config: render_config(&start.setup),
                    inputs: 0,
                    media: Vec::new(),
                });
            }
            Record::Steer(steer) => self.accept(seq, &steer.input, operations, out),
            // A delivery as text appended to a tool result holds no message.
            Record::Delivery(delivery) if !delivery.input.is_empty() => {
                self.accept(seq, &delivery.input, operations, out)
            }
            Record::StepStart(start) => {
                if self.last_step != Some(start.step_id) {
                    self.steps += 1;
                    self.last_step = Some(start.step_id);
                }
                self.step_open = true;
                if self.captured.is_some() {
                    // A boundary that places accepted messages writes their copies, then the
                    // step's start, then the copies' facts, all in one batch: the step's start
                    // waits for the placement.
                    if self.awaits_placement() {
                        self.work().held_step = Some(start.step_id);
                    } else {
                        out.event(MediaEvent::StepStart {
                            step: StepId(start.step_id),
                        });
                    }
                }
            }
            Record::Attempt(_) => {
                if self.captured.is_some() {
                    out.event(MediaEvent::AttemptEnd);
                }
            }
            Record::StepEnd(_) => {
                self.step_open = false;
                if self.captured.is_some() {
                    out.event(MediaEvent::StepEnd);
                }
            }
            Record::BatchStart(batch) => {
                self.batch.clone_from(&batch.planned);
                if self.captured.is_some() {
                    out.event(MediaEvent::BatchStart {
                        calls: batch.planned.clone(),
                    });
                }
            }
            // A reply comes before its outcome, in the same batch. The capture's record is the
            // outcome, so the reply waits for it.
            Record::ToolReply(reply) => {
                if let Some(media) = result_media(&reply.result) {
                    if self.captured.is_some() || !media.is_empty() {
                        self.work().replies.push(Reply {
                            call: reply.result.tool_call_id.clone(),
                            ordinal: results_ordinal(conversation),
                            media,
                        });
                    }
                }
            }
            Record::ToolOutcome(outcome) => {
                let reply = self.work.as_mut().and_then(|work| {
                    let at = work
                        .replies
                        .iter()
                        .position(|reply| reply.call == outcome.tool_call_id)?;
                    Some(work.replies.remove(at))
                });
                if let Some(reply) = reply {
                    self.tool_result(seq, reply, true, operations, out);
                }
            }
            Record::ConfigRefrozen(refrozen) => {
                if self.captured.is_some() {
                    if let Some(limits) = limits_in(&refrozen.render_config) {
                        out.event(MediaEvent::LimitsChanged { limits });
                    }
                }
            }
            // An undo ended the branch before message `ordinal`. Like entry facts, the record
            // counts messages from 1, so the transcript keeps the `ordinal - 1` messages before
            // it: ordinals `0..ordinal - 1` as events count them.
            Record::TranscriptReverted(reverted) => {
                self.cut(reverted.ordinal.saturating_sub(1), out);
            }
            // A recovery that may have lost transcript records names the first message (from 1)
            // whose records it may have lost; the messages before it remain. A recovery that
            // lost none names none, and removes no message.
            Record::RecoveryCut(cut) => {
                if let Some(ordinal) = cut.ordinal {
                    self.cut(ordinal.saturating_sub(1), out);
                }
            }
            // A record of the first compaction protocol, found only on sessions converted from
            // the old storage.
            Record::Compaction(_) => {
                out.needs_saved_index.get_or_insert(seq);
            }
            Record::Event(event) => {
                if event.name.starts_with("compaction_") {
                    // The first compaction protocol's applied view, kept whole as a ledger event
                    // on a converted session; the operations fold names it the same way.
                    out.needs_saved_index.get_or_insert(seq);
                } else if event.name == "late_result" {
                    // A tool result that arrived after its batch closed, found only on sessions
                    // converted from the old storage: the new loop records a held call's result
                    // in its still-open batch. It is captured but never shown.
                    let result = event
                        .body
                        .as_ref()
                        .and_then(|body| body.get("result"))
                        .and_then(|result| {
                            serde_json::from_value::<ToolResult>(result.clone()).ok()
                        });
                    match result {
                        Some(result) => {
                            if let Some(media) =
                                result_media(&result).filter(|media| !media.is_empty())
                            {
                                let reply = Reply {
                                    call: result.tool_call_id.clone(),
                                    ordinal: results_ordinal(conversation),
                                    media,
                                };
                                self.tool_result(seq, reply, false, operations, out);
                            }
                        }
                        // Whether it held media can't be told: never guess.
                        None => {
                            out.needs_saved_index.get_or_insert(seq);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Ends a batch: what its records left waiting is handed over as it stands.
    pub fn flush(&mut self, out: &mut MediaFeed) {
        self.settle(out);
        self.work = None;
    }

    fn work(&mut self) -> &mut Work {
        self.work.get_or_insert_with(Default::default)
    }

    /// Whether the first placed copy of the accepted messages the engine holds is waiting for
    /// its facts.
    fn awaits_placement(&self) -> bool {
        self.work.as_ref().is_some_and(|work| {
            work.awaiting
                .iter()
                .any(|awaiting| awaiting.what == Awaited::FirstAccepted)
        })
    }

    /// Whether `record` ends what earlier records of its batch left waiting. Records that sit
    /// among a run's inputs or a placement's copies and their facts don't; a step's start or a
    /// run's end doesn't while a placement waits for the facts written after it.
    fn settles(&self, record: &Record) -> bool {
        match record {
            Record::Input(_)
            | Record::EntryFacts(_)
            | Record::TurnFacts(_)
            | Record::ResultAppend(_)
            | Record::SetupStub(_)
            | Record::Marker(_)
            | Record::RefreshApplied(_)
            | Record::CompactionEnrolled(_)
            | Record::BudgetFire(_)
            | Record::SendEnded(_)
            | Record::Billed(_)
            | Record::WriterOpened(_) => false,
            Record::StepStart(_) | Record::RunEnd(_) => !self.awaits_placement(),
            _ => true,
        }
    }

    /// Hands over what is waiting: a run start with the inputs read so far, a placement whose
    /// facts never came, and a held step start. A message without facts takes the ordinal the
    /// conversation placed it at, as on a log written before entry facts were recorded.
    fn settle(&mut self, out: &mut MediaFeed) {
        let Some(work) = self.work.as_mut() else {
            return;
        };
        let awaiting = std::mem::take(&mut work.awaiting);
        let run = work.run.take();
        let held_step = work.held_step.take();
        for awaiting in awaiting {
            if awaiting.what == Awaited::FirstAccepted {
                self.placed(awaiting.placed_at, out);
            }
        }
        if let Some(run) = run {
            self.run_start(run, out);
        }
        if let Some(step) = held_step {
            out.event(MediaEvent::StepStart { step: StepId(step) });
        }
    }

    fn input(&mut self, input: &Input, conversation: &Conversation) {
        // An input is placed as soon as it is folded, so this is the ordinal its facts name.
        let placed_at = conversation.placed + 1;
        let first_accepted = self
            .captured
            .as_ref()
            .and_then(|captured| captured.first_accepted);
        // A copy placing messages a steer or a delivery accepted earlier names that record.
        let copy_of = match input.placement {
            Placement::Steer | Placement::Delivered | Placement::DeliveredTurnSystem => {
                input.receipt_seq
            }
            _ => None,
        };
        let waiting = self
            .work
            .as_ref()
            .is_some_and(|work| work.run.is_some() || !work.awaiting.is_empty());
        // Only a run's inputs, and the copies placing the accepted messages the engine holds,
        // wait for their facts. A copy of messages the engine never held may come ahead of
        // those, and its facts come first, so it waits too.
        if !waiting && (copy_of.is_none() || first_accepted.is_none()) {
            return;
        }
        let work = self.work();
        let what = match work.run.as_mut() {
            Some(run) if matches!(input.placement, Placement::RunInput | Placement::Imported) => {
                let message = run.inputs;
                run.inputs += 1;
                let skeleton = MediaMessage::of(&input.message);
                if skeleton.holds_media() {
                    run.media.push(RunInput {
                        message,
                        ordinal: None,
                        placed_at,
                        skeleton,
                    });
                    Awaited::RunInput(run.media.len() - 1)
                } else {
                    Awaited::Other
                }
            }
            _ if copy_of.is_some()
                && copy_of == first_accepted
                && !work
                    .awaiting
                    .iter()
                    .any(|awaiting| awaiting.what == Awaited::FirstAccepted) =>
            {
                Awaited::FirstAccepted
            }
            _ => Awaited::Other,
        };
        work.awaiting.push_back(Awaiting { placed_at, what });
    }

    /// An `EntryFacts` for the oldest message still waiting for one. Facts for any other message
    /// (an assistant message, a tool results message) find nothing waiting.
    fn facts(&mut self, ordinal: u64, out: &mut MediaFeed) {
        let Some(work) = self.work.as_mut() else {
            return;
        };
        let Some(awaiting) = work.awaiting.pop_front() else {
            return;
        };
        match awaiting.what {
            Awaited::RunInput(at) => {
                if let Some(input) = work.run.as_mut().and_then(|run| run.media.get_mut(at)) {
                    input.ordinal = Some(ordinal);
                }
            }
            Awaited::FirstAccepted => {
                let held = work.held_step.take();
                self.placed(ordinal, out);
                if let Some(step) = held {
                    out.event(MediaEvent::StepStart { step: StepId(step) });
                }
            }
            Awaited::Other => {}
        }
    }

    /// The accepted messages the engine holds are placed from `ordinal` (from 1).
    fn placed(&mut self, ordinal: u64, out: &mut MediaFeed) {
        if let Some(captured) = self.captured.as_mut() {
            captured.first_accepted = None;
            out.event(MediaEvent::Placed { first: ordinal - 1 });
        }
    }

    fn run_start(&mut self, run: OpenRun, out: &mut MediaFeed) {
        if self.captured.is_none() {
            if run.media.is_empty() {
                return;
            }
            // The run start's own item carries its limits, and the run has accepted nothing yet.
            self.first_capture(LimitsSource::Known(None), None, out);
        }
        let inputs = run
            .media
            .into_iter()
            .map(|input| MediaInput {
                message: input.message,
                ordinal: input.ordinal.unwrap_or(input.placed_at) - 1,
                skeleton: input.skeleton,
            })
            .collect();
        out.items.push(match run.render_config {
            Some(render_config) => MediaItem::RunStart(RunStartNeedingLimits {
                record: run.record,
                inputs,
                render_config,
            }),
            None => MediaItem::Event(MediaEvent::RunStart {
                record: run.record,
                inputs,
                limits: None,
            }),
        });
    }

    /// Messages accepted into a running run by the record at `seq`.
    fn accept(
        &mut self,
        seq: u64,
        messages: &[broca_protocol::content::Message],
        operations: &Operations,
        out: &mut MediaFeed,
    ) {
        let messages: Vec<MediaMessage> = messages.iter().map(MediaMessage::of).collect();
        if self.captured.is_none() {
            if !messages.iter().any(MediaMessage::holds_media) {
                return;
            }
            self.first_capture(run_limits(operations), Some(operations), out);
        }
        let captured = self.captured.as_mut().expect("set above");
        captured.first_accepted.get_or_insert(seq);
        out.event(MediaEvent::Accepted {
            record: seq,
            messages,
        });
    }

    fn tool_result(
        &mut self,
        seq: u64,
        reply: Reply,
        shown: bool,
        operations: &Operations,
        out: &mut MediaFeed,
    ) {
        if self.captured.is_none() {
            if reply.media.is_empty() {
                return;
            }
            self.first_capture(run_limits(operations), Some(operations), out);
        }
        out.event(MediaEvent::ToolResult {
            record: seq,
            call: reply.call,
            ordinal: reply.ordinal,
            shown,
            media: reply.media,
        });
    }

    /// The transcript is cut back to its first `len` messages. Messages accepted but not placed
    /// are dropped with it, and the run's batch and step end, as they do in the engine: its step
    /// counter moves past the step whose media lose their protection.
    fn cut(&mut self, len: u64, out: &mut MediaFeed) {
        self.steps += 1;
        self.last_step = None;
        self.step_open = false;
        self.batch.clear();
        if let Some(captured) = self.captured.as_mut() {
            captured.first_accepted = None;
            out.event(MediaEvent::Truncated { len });
        }
    }

    /// The session's first capture: hands over the scalars the engine's index starts from, and
    /// the messages accepted before it that still wait for their place (`operations` holds
    /// them), so the engine places them with the later ones as the full history would.
    fn first_capture(
        &mut self,
        limits: LimitsSource,
        operations: Option<&Operations>,
        out: &mut MediaFeed,
    ) {
        out.start = Some(MediaStart {
            steps: self.steps,
            last_step: self.last_step.map(StepId),
            step_open: self.step_open,
            batch: self.batch.clone(),
            limits,
        });
        let waiting = operations
            .and_then(|operations| operations.run.as_ref())
            .map(|run| run.pending_steers.as_slice())
            .unwrap_or_default();
        let mut first_accepted = None;
        for pending in waiting {
            let Some(record) = pending.seq else {
                continue;
            };
            first_accepted.get_or_insert(record);
            out.event(MediaEvent::Accepted {
                record,
                messages: pending.messages.iter().map(MediaMessage::of).collect(),
            });
        }
        self.captured = Some(Captured { first_accepted });
    }
}

#[cfg(test)]
pub(crate) mod tests;
