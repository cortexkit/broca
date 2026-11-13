//! The image engine fed a converted session two ways, compared: `session-convert media-check`.
//!
//! One way is today's: the old log's records, folded by today's entry point from the first record
//! (`media_today::OldEngine`). The other is the new storage's: the media events the session
//! store's own fold (`fold::media`) hands over for the converted records, applied through
//! `SessionMedia::apply`, with each append's events handed over when the append ends, as the
//! staged session's appends end. The store's engine starts at the session's first capture, from
//! the scalars the fold kept until then, as the session's owner starts it. A record of the first
//! compaction protocol has no events, and a session holding one can't build its engine from its
//! records: there the store's engines continue from today's state at that record, so the records
//! after it are still compared.
//!
//! Captures are named differently by the two: today's by the old record that holds them, the
//! store's by the converted record (see `media_today::capture_records`). Today's are renamed
//! before every comparison.
//!
//! The two are compared before every run starts and at the end, field by field (see
//! [`FIELDS`]). Aging is compared under the limits the records freeze, changing as they change,
//! and when the records freeze more than one set of limits, under each of them throughout. A
//! difference names the point (`run <id>` or `end`), the field, and where the values first
//! differ, as capture ids, media references, positions and counts, never a message's text.

use std::collections::{BTreeMap, BTreeSet};

use broca_core::media_request::{MediaId, MediaIndexState, SessionMedia};
use broca_protocol::ids::StepId;
use broca_protocol::media_event::{MediaEvent, MediaLimits, MediaMessage, MediaSlot};
use broca_protocol::{Role, ToolCallId};
use broca_provider::media_request::{AgingState, MediaAging};
use broca_session_loop::parts::{MemoryParts, Parts as _};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::Record;
use broca_session_store::session::{
    limits_in, LimitsSource, MediaFeed, MediaFold, MediaItem, MediaStart,
};
use broca_wal::FramedRecord;
use serde::Serialize;

use crate::media_today::{self, Limits, NoBytes, OldEngine};
use crate::Converted;

/// Every field a comparison names: of the engine's state, then of each aging state (named
/// `aging[<limits>].<field>`).
pub const FIELDS: &[&str] = &[
    "captures",
    "pending",
    "accepted",
    "protected",
    "steps",
    "last_step",
    "step_open",
    "batch",
    "placed",
    "held_messages",
    "selection",
    "covered",
    "replacements",
    "has_view",
    "limits",
    "aging.totals",
    "aging.aged",
    "aging.shown",
    "aging.unprotected",
];

/// One field on which two sides differ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Difference {
    /// Where: `run <id>` before that run starts, or `end`.
    pub at: String,
    /// The two sides compared, `today/store`.
    pub sides: String,
    pub field: String,
    /// Where the values first differ, by position and content-free value.
    pub detail: String,
}

/// What the engine holds at one point, as compared.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub state: MediaIndexState,
    /// The media the engine selects to show, in order, with whether each is protected.
    pub selected: Vec<(MediaId, bool)>,
    /// The aging state under each set of limits compared: those the records freeze first, then
    /// each set throughout.
    pub aging: Vec<AgingState>,
    /// The limits in force, as recorded.
    pub limits: Option<MediaLimits>,
    pub step_open: bool,
    /// Whether this is an engine's state. Before the session's first capture the store keeps no
    /// engine, only its fold's scalars: the step counter, the latest step, whether a step is
    /// open and the batch order.
    pub engine: bool,
}

/// A log compared.
#[derive(Debug, Clone)]
pub struct Checked {
    /// The run starts compared at.
    pub runs: u64,
    /// The media today's engine captured, over the whole log.
    pub images: u64,
    pub differences: Vec<Difference>,
    /// The name of each aging state compared, in [`Snapshot::aging`]'s order.
    pub limits: Vec<String>,
    /// Today's engine at the end, with captures named as the converted session names them.
    pub today: Snapshot,
    /// The store's engine at the end; `None` when the store's fold captured nothing.
    pub store: Option<Snapshot>,
}

/// The image engine fed the media events of the new storage.
#[derive(Debug, Clone)]
struct EventEngine {
    media: SessionMedia,
    aging: MediaAging,
    limits: Limits,
    step_open: bool,
}

/// The limits an event freezes.
fn event_limits(event: &MediaEvent) -> Option<&MediaLimits> {
    match event {
        MediaEvent::RunStart {
            limits: Some(limits),
            ..
        }
        | MediaEvent::LimitsChanged { limits } => Some(limits),
        _ => None,
    }
}

impl EventEngine {
    /// The engine the session's first capture starts, from the scalars the store's fold kept
    /// until then, under `limits`; under the limits recorded, those of the run in force when it
    /// froze some.
    fn started(start: &MediaStart, limits: &Limits, parts: &MemoryParts) -> Result<Self, String> {
        let media = SessionMedia::from_index_state(
            MediaIndexState {
                boundary: start.steps,
                last_step: start.last_step,
                batch: start.batch.clone(),
                ..MediaIndexState::default()
            },
            NoBytes,
        )?;
        let in_run = match &start.limits {
            LimitsSource::Known(limits) => limits.clone(),
            LimitsSource::Part(part) => limits_in(&parts.get(part)?),
        };
        let limits = match (limits, in_run) {
            (Limits::Recorded(_), Some(in_run)) => {
                Limits::Recorded(Some((media_today::profile(&in_run)?, in_run)))
            }
            (limits, _) => limits.clone(),
        };
        Ok(Self {
            media,
            aging: MediaAging::default(),
            limits,
            step_open: start.step_open,
        })
    }

    /// The engine continuing from today's `old`, where a record of the first compaction protocol
    /// leaves the store's events nothing to continue from.
    fn continuing(old: &OldEngine, map: &BTreeMap<u64, u64>) -> Result<Self, String> {
        let (media, aging) = media_today::renumbered(old, map)?;
        Ok(Self {
            media,
            aging,
            limits: old.limits.clone(),
            step_open: old.step_open,
        })
    }

    fn apply(&mut self, event: &MediaEvent) -> Result<(), String> {
        match &mut self.limits {
            Limits::Recorded(Some((active, raw))) => {
                let before = active.clone();
                self.aging
                    .fold_media_event(active, &mut self.media, event)?;
                if *active != before {
                    if let Some(limits) = event_limits(event) {
                        *raw = limits.clone();
                    }
                }
            }
            Limits::Recorded(None) => {
                self.media.apply(event)?;
                self.media.take_changes();
            }
            Limits::Fixed(profile) => {
                // The same limits throughout: whatever an event freezes is left out.
                let event = match event {
                    MediaEvent::LimitsChanged { .. } => None,
                    MediaEvent::RunStart { record, inputs, .. } => Some(MediaEvent::RunStart {
                        record: *record,
                        inputs: inputs.clone(),
                        limits: None,
                    }),
                    other => Some(other.clone()),
                };
                if let Some(event) = event {
                    self.aging
                        .fold_media_event(profile, &mut self.media, &event)?;
                }
            }
        }
        match event {
            MediaEvent::RunStart { .. } | MediaEvent::StepEnd | MediaEvent::Truncated { .. } => {
                self.step_open = false
            }
            MediaEvent::StepStart { .. } => self.step_open = true,
            _ => {}
        }
        Ok(())
    }
}

/// The snapshot of engines fed alike, one per set of limits, the first under those recorded.
fn snapshot<'a>(
    media: &SessionMedia,
    agings: impl Iterator<Item = &'a MediaAging>,
    limits: Option<&MediaLimits>,
    step_open: bool,
) -> Snapshot {
    Snapshot {
        state: media.index_state(),
        selected: media
            .selected_bindings()
            .into_iter()
            .map(|binding| (binding.id, binding.protected))
            .collect(),
        aging: agings.map(MediaAging::state).collect(),
        limits: limits.cloned(),
        step_open,
        engine: true,
    }
}

fn today_snapshot(old: &[OldEngine], map: &BTreeMap<u64, u64>) -> Result<Snapshot, String> {
    let live = &old[0];
    let renamed = old
        .iter()
        .map(|engine| media_today::renumber_aging(engine.aging.state(), map))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Snapshot {
        state: media_today::renumber_state(live.media.index_state(), map)?,
        selected: live
            .media
            .selected_bindings()
            .into_iter()
            .map(|binding| {
                Ok((
                    media_today::renumber_id(binding.id, map)?,
                    binding.protected,
                ))
            })
            .collect::<Result<_, String>>()?,
        aging: renamed,
        limits: live.limits.in_force().cloned(),
        step_open: live.step_open,
        engine: true,
    })
}

fn store_snapshot(engines: &[EventEngine]) -> Snapshot {
    let live = &engines[0];
    snapshot(
        &live.media,
        engines.iter().map(|engine| &engine.aging),
        live.limits.in_force(),
        live.step_open,
    )
}

/// A planned batch as the engine keeps it: each call at its last planned position.
fn batch_order(batch: &[ToolCallId]) -> Vec<(ToolCallId, usize)> {
    let mut order: BTreeMap<&str, usize> = BTreeMap::new();
    for (position, call) in batch.iter().enumerate() {
        order.insert(&call.0, position);
    }
    let mut order: Vec<_> = order
        .into_iter()
        .map(|(call, position)| (ToolCallId(call.to_owned()), position))
        .collect();
    order.sort_by_key(|(_, position)| *position);
    order
}

/// Where the media of a skeleton sit: its role and slots, without its number of blocks, which
/// a results message's skeleton counts only from the results the engine saw.
fn slots(message: &MediaMessage) -> Slots<'_> {
    (message.role, &message.slots)
}

/// A skeleton as compared (see [`slots`]).
type Slots<'a> = (Role, &'a [MediaSlot]);

/// The placed skeletons, by ordinal (see [`slots`]).
fn placed(state: &MediaIndexState) -> Vec<(u64, Slots<'_>)> {
    state
        .skeleton
        .iter()
        .map(|(ordinal, message)| (*ordinal, slots(message)))
        .collect()
}

/// The skeletons of accepted messages not placed yet, by record and position (see [`slots`]).
fn held(state: &MediaIndexState) -> Vec<((u64, usize), Slots<'_>)> {
    state
        .held_messages
        .iter()
        .map(|(key, message)| (*key, slots(message)))
        .collect()
}

/// Where two lists first differ, and their lengths.
fn first_difference<T: PartialEq + std::fmt::Debug>(left: &[T], right: &[T]) -> String {
    let at = left
        .iter()
        .zip(right)
        .position(|(left, right)| left != right)
        .unwrap_or(left.len().min(right.len()));
    let shown = |list: &[T]| {
        list.get(at)
            .map_or("nothing".to_owned(), |value| format!("{value:?}"))
            .chars()
            .take(240)
            .collect::<String>()
    };
    format!(
        "{} and {} entries; at {at}: {} and {}",
        left.len(),
        right.len(),
        shown(left),
        shown(right)
    )
}

struct Comparison<'a> {
    at: &'a str,
    sides: String,
    out: &'a mut Vec<Difference>,
}

impl Comparison<'_> {
    fn list<T: PartialEq + std::fmt::Debug>(&mut self, field: &str, left: &[T], right: &[T]) {
        if left != right {
            self.out.push(Difference {
                at: self.at.to_owned(),
                sides: self.sides.clone(),
                field: field.to_owned(),
                detail: first_difference(left, right),
            });
        }
    }

    fn value<T: PartialEq + std::fmt::Debug>(&mut self, field: &str, left: &T, right: &T) {
        if left != right {
            self.out.push(Difference {
                at: self.at.to_owned(),
                sides: self.sides.clone(),
                field: field.to_owned(),
                detail: format!("{left:?} and {right:?}")
                    .chars()
                    .take(480)
                    .collect(),
            });
        }
    }
}

/// Compares two snapshots at `at`, naming them `left` and `right`, and the aging states under
/// the limits `limits` names, as many as both hold.
pub fn compare(
    at: &str,
    (left_name, left): (&str, &Snapshot),
    (right_name, right): (&str, &Snapshot),
    limits: &[String],
    out: &mut Vec<Difference>,
) {
    let mut compared = Comparison {
        at,
        sides: format!("{left_name}/{right_name}"),
        out,
    };
    let (a, b) = (&left.state, &right.state);
    compared.list("captures", &a.captures, &b.captures);
    compared.value("steps", &a.boundary, &b.boundary);
    compared.value("last_step", &a.last_step, &b.last_step);
    compared.value("step_open", &left.step_open, &right.step_open);
    compared.list("batch", &batch_order(&a.batch), &batch_order(&b.batch));
    if !(left.engine && right.engine) {
        // Before the store's first capture only its fold's scalars compare, and the captures,
        // which today's engine must not hold either.
        return;
    }
    compared.list("pending", &a.pending, &b.pending);
    compared.list("accepted", &a.held, &b.held);
    compared.list("protected", &a.protected, &b.protected);
    compared.list("placed", &placed(a), &placed(b));
    compared.list("held_messages", &held(a), &held(b));
    compared.list("selection", &left.selected, &right.selected);
    compared.value("covered", &a.covered, &b.covered);
    compared.list("replacements", &a.replacements, &b.replacements);
    compared.value("has_view", &a.has_view, &b.has_view);
    // As aging reads them: the canonical JSON a run's setup parts are kept in may write a number
    // differently from the old record.
    let decoded = |limits: &Option<MediaLimits>| limits.as_ref().map(media_today::profile);
    compared.value("limits", &decoded(&left.limits), &decoded(&right.limits));
    for ((name, a), b) in limits.iter().zip(&left.aging).zip(&right.aging) {
        compared.value(
            &format!("aging[{name}].totals"),
            &(a.bytes, a.tokens),
            &(b.bytes, b.tokens),
        );
        compared.list(&format!("aging[{name}].aged"), &a.aged, &b.aged);
        compared.list(&format!("aging[{name}].shown"), &a.shown, &b.shown);
        compared.list(
            &format!("aging[{name}].unprotected"),
            &a.unprotected,
            &b.unprotected,
        );
    }
}

/// The store's side: its folds of the converted records, and its engines once it captured.
struct StoreSide<'a> {
    conversation: Conversation,
    operations: Operations,
    fold: MediaFold,
    feed: MediaFeed,
    engines: Option<Vec<EventEngine>>,
    limits: &'a [Limits],
    parts: &'a MemoryParts,
}

impl<'a> StoreSide<'a> {
    fn new(appends_in_conversation: bool, limits: &'a [Limits], parts: &'a MemoryParts) -> Self {
        Self {
            conversation: Conversation::new(appends_in_conversation),
            operations: Operations::default(),
            fold: MediaFold::default(),
            feed: MediaFeed::default(),
            engines: None,
            limits,
            parts,
        }
    }

    /// Folds the record at `seq`, as an append folds it.
    fn record(&mut self, seq: u64, record: &Record) -> Result<(), String> {
        self.fold.apply(
            seq,
            record,
            &self.conversation,
            &self.operations,
            &mut self.feed,
        );
        broca_session_store::session::apply(
            &mut self.conversation,
            &mut self.operations,
            seq,
            record,
        )
        .map_err(|error| format!("the store's fold of record {seq}: {error}"))
    }

    /// The end of an append: what it handed over reaches the engines.
    fn flush(&mut self) -> Result<(), String> {
        self.fold.flush(&mut self.feed);
        self.deliver()
    }

    /// Hands what the fold handed over so far to the engines, starting them at the first
    /// capture.
    fn deliver(&mut self) -> Result<(), String> {
        let mut feed = std::mem::take(&mut self.feed);
        // Whether these records hold one the events can't express is the caller's to act on.
        self.feed.needs_saved_index = feed.needs_saved_index.take();
        if let Some(start) = &feed.start {
            if self.engines.is_none() {
                self.engines = Some(
                    self.limits
                        .iter()
                        .map(|limits| EventEngine::started(start, limits, self.parts))
                        .collect::<Result<_, _>>()?,
                );
            }
        }
        let Some(engines) = self.engines.as_mut() else {
            return Ok(());
        };
        for item in feed.items {
            let event = match item {
                MediaItem::Event(event) => event,
                MediaItem::RunStart(run) => {
                    let limits = limits_in(&self.parts.get(&run.render_config)?);
                    run.with_limits(limits)
                }
            };
            for engine in engines.iter_mut() {
                engine.apply(&event)?;
            }
        }
        Ok(())
    }

    /// The store's state: its engines', or before its first capture, its fold's scalars.
    fn snapshot(&self) -> Snapshot {
        match &self.engines {
            Some(engines) => store_snapshot(engines),
            None => Snapshot {
                state: MediaIndexState {
                    boundary: self.fold.steps,
                    last_step: self.fold.last_step.map(StepId),
                    batch: self.fold.batch.clone(),
                    ..MediaIndexState::default()
                },
                selected: Vec::new(),
                aging: vec![AgingState::default(); self.limits.len()],
                limits: None,
                step_open: self.fold.step_open,
                engine: false,
            },
        }
    }
}

/// The sets of limits a log's aging is compared under, with their names: those its records
/// freeze first, then, when they freeze more than one, each set throughout.
fn limits_compared(old: &[FramedRecord]) -> Result<(Vec<Limits>, Vec<String>), String> {
    let recorded = media_today::recorded_profiles(old)?;
    let mut limits = vec![Limits::recorded(old)?];
    let mut names = vec!["recorded".to_owned()];
    if recorded.len() > 1 {
        for (n, (profile, _)) in recorded.into_iter().enumerate() {
            limits.push(Limits::Fixed(profile));
            names.push(format!("limits {}", n + 1));
        }
    }
    Ok((limits, names))
}

/// Folds today's engines through the old record at `seq`. The log's records are numbered from 1
/// without gaps, so record `n` is `frames[n - 1]`.
fn advance(old: &mut [OldEngine], frames: &[FramedRecord], seq: u64) -> Result<(), String> {
    for engine in old.iter_mut() {
        while engine.folded < seq {
            let frame = frames
                .get(engine.folded as usize)
                .ok_or_else(|| format!("the log has no record {}", engine.folded + 1))?;
            engine.fold(frame)?;
        }
    }
    Ok(())
}

/// Compares the image engine fed the old log `old` with the engine fed the events of
/// `converted`, its conversion, appended as the staging writer appends it, from seq
/// `crate::FIRST_SEQ`, into a session whose conversation layout is `appends_in_conversation`.
/// `Err` when either side can't fold the log at all.
pub fn check(
    old: &[FramedRecord],
    converted: &mut Converted,
    appends_in_conversation: bool,
) -> Result<Checked, String> {
    if old
        .iter()
        .enumerate()
        .any(|(at, frame)| frame.seq != at as u64 + 1)
    {
        return Err("the log's records aren't numbered from 1 without gaps".into());
    }
    let first = crate::FIRST_SEQ;
    // The records name each other by the seqs a staged session gives them.
    converted.start_at(first);
    let converted = &*converted;
    let (limits, names) = limits_compared(old)?;
    let parts = MemoryParts {
        parts: converted.parts.clone(),
    };
    let map = media_today::capture_records(old, converted, first);
    let mut today: Vec<OldEngine> = limits.iter().cloned().map(OldEngine::new).collect();
    let mut store = StoreSide::new(appends_in_conversation, &limits, &parts);
    let ends: BTreeSet<usize> = crate::stage::appends(&converted.records)
        .into_iter()
        .map(|append| append.end)
        .collect();
    // The old record each converted record came from.
    let origin = |index: usize| -> u64 {
        let at = converted.origins.partition_point(|start| *start <= index);
        old[at.saturating_sub(1)].seq
    };
    let mut differences = Vec::new();
    let mut runs = 0;
    for (index, record) in converted.records.iter().enumerate() {
        let seq = first + index as u64;
        if let Record::RunStart(start) = record {
            // Before the run starts: everything earlier on both sides.
            store.flush()?;
            advance(&mut today, old, origin(index) - 1)?;
            runs += 1;
            compare(
                &format!("run {}", start.run_id),
                ("today", &today_snapshot(&today, &map)?),
                ("store", &store.snapshot()),
                &names,
                &mut differences,
            );
        }
        store.record(seq, record)?;
        if store.feed.needs_saved_index.is_some() {
            // A record the events can't express: what came before it reaches the engines, which
            // then continue from today's state through it, so the records after it still compare.
            store.deliver()?;
            store.feed.needs_saved_index = None;
            advance(&mut today, old, origin(index))?;
            if store.fold.has_captured() || !today[0].media.facts().captures.is_empty() {
                store.engines = Some(
                    today
                        .iter()
                        .map(|engine| EventEngine::continuing(engine, &map))
                        .collect::<Result<_, _>>()?,
                );
            }
        }
        if ends.contains(&(index + 1)) {
            store.flush()?;
        }
    }
    store.flush()?;
    advance(&mut today, old, old.last().map_or(0, |frame| frame.seq))?;
    let today_end = today_snapshot(&today, &map)?;
    let store_end = store.snapshot();
    compare(
        "end",
        ("today", &today_end),
        ("store", &store_end),
        &names,
        &mut differences,
    );
    Ok(Checked {
        runs,
        images: today[0].media.facts().captures.len() as u64,
        differences,
        limits: names,
        today: today_end,
        store: store.engines.is_some().then_some(store_end),
    })
}
