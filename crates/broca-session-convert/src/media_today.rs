//! The image engine as today's storage folds it, with each capture named as the converted
//! session names it, for comparing it with the engine the session store's media events drive
//! (see `media_compare`).
//!
//! Today's storage names a capture by the old record that holds it; the new storage by the
//! converted record that holds it (a run start for a run's inputs, a steer or a delivery for
//! accepted messages, a tool outcome for a tool result, the ledger event a late result is kept
//! as). [`capture_records`] maps one to the other through [`Converted::origins`], and the
//! renumber functions rename an engine's state with it.

use std::collections::BTreeMap;

use broca_core::media_request::{MediaBinding, MediaId, MediaIndexState, SessionMedia};
use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::media_event::MediaLimits;
use broca_protocol::MediaRef;
use broca_provider::media_profile::MediaProfile;
use broca_provider::media_request::{AgingEvent, AgingState, MediaAging, Occurrence};
use broca_session_store::record::Record;
use broca_wal::{FramedRecord, WalRecord};

use crate::Converted;

/// The bytes source of an engine that only decides which capture each medium is, whether it is
/// protected and whether it has aged out: it reads no bytes.
#[derive(Debug, Clone, Copy)]
pub struct NoBytes;

impl MediaBytes for NoBytes {
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        Err(MediaReadError::Missing(reference.sha256.clone()))
    }
}

/// The media limits an old record freezes: the `media` member of a run start's or a refreeze's
/// render config, unless absent or null.
pub fn recorded_limits(record: &WalRecord) -> Option<MediaLimits> {
    let frozen = match record {
        WalRecord::RunStarted { config, .. } => &config.frozen_render_config,
        WalRecord::RunConfigRefrozen {
            frozen_render_config,
            ..
        } => frozen_render_config,
        _ => return None,
    };
    frozen
        .get("media")
        .filter(|media| !media.is_null())
        .cloned()
        .map(MediaLimits)
}

/// The limits as aging reads them.
pub fn profile(limits: &MediaLimits) -> Result<MediaProfile, String> {
    serde_json::from_value(limits.0.clone())
        .map_err(|error| format!("invalid frozen media profile: {error}"))
}

/// Each distinct set of media limits the log's records freeze, in the order first frozen, as
/// aging reads them and as recorded.
pub fn recorded_profiles(old: &[FramedRecord]) -> Result<Vec<(MediaProfile, MediaLimits)>, String> {
    let mut found: Vec<(MediaProfile, MediaLimits)> = Vec::new();
    for frame in old {
        if let Some(limits) = recorded_limits(&frame.record) {
            let decoded = profile(&limits)?;
            if !found.iter().any(|(known, _)| *known == decoded) {
                found.push((decoded, limits));
            }
        }
    }
    Ok(found)
}

/// Which media limits an engine ages under.
#[derive(Debug, Clone)]
pub enum Limits {
    /// The limits the records freeze, changing where a run start or a refreeze freezes others,
    /// as a session's engine ages: the limits in force (as aging reads them, and as recorded),
    /// or `None` while none is known. An engine starts under the first limits its log freezes,
    /// so media captured before any run froze limits age under those.
    Recorded(Option<(MediaProfile, MediaLimits)>),
    /// The same limits throughout, whatever the records freeze: what aging would decide if every
    /// request were rendered under them.
    Fixed(MediaProfile),
}

impl Limits {
    /// The limits a session starts under: the first its records freeze.
    pub fn recorded(old: &[FramedRecord]) -> Result<Self, String> {
        Ok(Self::Recorded(recorded_profiles(old)?.into_iter().next()))
    }

    /// The limits in force, as recorded, for the recorded case.
    pub fn in_force(&self) -> Option<&MediaLimits> {
        match self {
            Self::Recorded(active) => active.as_ref().map(|(_, limits)| limits),
            Self::Fixed(_) => None,
        }
    }
}

/// The image engine fed today's records through today's entry point (`SessionMedia::fold` and
/// `MediaAging`'s fold of each record), from the log's first record.
#[derive(Debug, Clone)]
pub struct OldEngine {
    pub media: SessionMedia,
    pub aging: MediaAging,
    pub limits: Limits,
    /// A step started and its model step hasn't finished, as today's media checkpoint keeps it.
    pub step_open: bool,
    /// The seq of the last record folded.
    pub folded: u64,
}

impl OldEngine {
    pub fn new(limits: Limits) -> Self {
        Self {
            media: SessionMedia::new(NoBytes),
            aging: MediaAging::default(),
            limits,
            step_open: false,
            folded: 0,
        }
    }

    /// Folds the record after the last folded.
    pub fn fold(&mut self, frame: &FramedRecord) -> Result<(), String> {
        if frame.seq != self.folded + 1 {
            return Err(format!(
                "today's media fold expected record {} and got record {}",
                self.folded + 1,
                frame.seq
            ));
        }
        let record = &frame.record;
        match &mut self.limits {
            Limits::Recorded(Some((active, raw))) => {
                let before = active.clone();
                self.aging
                    .fold_wal_record(active, &mut self.media, record)
                    .map_err(|error| format!("record {}: {error}", frame.seq))?;
                if *active != before {
                    if let Some(next) = recorded_limits(record) {
                        *raw = next;
                    }
                }
            }
            Limits::Recorded(None) => {
                self.media
                    .fold(record)
                    .map_err(|error| format!("record {}: {error}", frame.seq))?;
                self.media.take_changes();
            }
            Limits::Fixed(profile) => {
                self.media
                    .fold(record)
                    .map_err(|error| format!("record {}: {error}", frame.seq))?;
                self.aging.fold_changes(profile, self.media.take_changes());
                if matches!(
                    record,
                    WalRecord::ModelAttemptFinished { .. } | WalRecord::ModelStepFinished { .. }
                ) {
                    self.aging.apply(profile, AgingEvent::Boundary);
                }
            }
        }
        match record {
            WalRecord::RunStarted { .. } | WalRecord::ModelStepFinished { .. } => {
                self.step_open = false
            }
            WalRecord::StepStarted { .. } => self.step_open = true,
            _ => {}
        }
        self.folded = frame.seq;
        Ok(())
    }
}

/// Where each old record that can capture a medium was converted to: its seq, by the seq of the
/// record of the converted session that holds what it captures, as the new storage names a
/// capture (a run start for a run's inputs, a steer or a delivery for accepted messages, a tool
/// outcome for a tool result, the ledger event a late result is kept as). `first` is the seq the
/// converted session's first converted record takes.
pub fn capture_records(
    old: &[FramedRecord],
    converted: &Converted,
    first: u64,
) -> BTreeMap<u64, u64> {
    let mut map = BTreeMap::new();
    for (at, frame) in old.iter().enumerate() {
        let Some(&start) = converted.origins.get(at) else {
            break;
        };
        let end = converted
            .origins
            .get(at + 1)
            .copied()
            .unwrap_or(converted.records.len());
        let holds = |record: &Record| match (&frame.record, record) {
            (WalRecord::RunStarted { .. }, Record::RunStart(_))
            | (WalRecord::Steered { .. }, Record::Steer(_))
            | (WalRecord::SendDelivered { .. }, Record::Delivery(_))
            | (WalRecord::ToolResult { .. }, Record::ToolOutcome(_)) => true,
            (WalRecord::LateResult { .. }, Record::Event(event)) => event.name == "late_result",
            _ => false,
        };
        if let Some(offset) = converted.records[start..end].iter().position(holds) {
            map.insert(frame.seq, first + (start + offset) as u64);
        }
    }
    map
}

/// `id` named by the converted record that holds its capture.
pub fn renumber_id(id: MediaId, map: &BTreeMap<u64, u64>) -> Result<MediaId, String> {
    let record = map.get(&id.record).copied().ok_or_else(|| {
        format!(
            "the capture of record {} has no converted record holding it",
            id.record
        )
    })?;
    Ok(MediaId { record, ..id })
}

fn renumber_ids(ids: Vec<MediaId>, map: &BTreeMap<u64, u64>) -> Result<Vec<MediaId>, String> {
    ids.into_iter().map(|id| renumber_id(id, map)).collect()
}

pub fn renumber_binding(
    binding: MediaBinding,
    map: &BTreeMap<u64, u64>,
) -> Result<MediaBinding, String> {
    Ok(MediaBinding {
        id: renumber_id(binding.id, map)?,
        ..binding
    })
}

/// An engine's state with every capture named by the converted record that holds it. The order
/// of every list stays: the converted records are in the order of the records they came from.
pub fn renumber_state(
    state: MediaIndexState,
    map: &BTreeMap<u64, u64>,
) -> Result<MediaIndexState, String> {
    Ok(MediaIndexState {
        captures: state
            .captures
            .into_iter()
            .map(|mut capture| {
                capture.id = renumber_id(capture.id, map)?;
                Ok(capture)
            })
            .collect::<Result<_, String>>()?,
        pending: renumber_ids(state.pending, map)?,
        held: state
            .held
            .into_iter()
            .map(|ids| renumber_ids(ids, map))
            .collect::<Result<_, _>>()?,
        protected: renumber_ids(state.protected, map)?,
        held_messages: state
            .held_messages
            .into_iter()
            .map(|((record, message), skeleton)| {
                let record = map.get(&record).copied().ok_or_else(|| {
                    format!("the accepted messages of record {record} have no converted record")
                })?;
                Ok(((record, message), skeleton))
            })
            .collect::<Result<_, String>>()?,
        selection: state
            .selection
            .map(|bindings| {
                bindings
                    .into_iter()
                    .map(|binding| renumber_binding(binding, map))
                    .collect::<Result<_, _>>()
            })
            .transpose()?,
        ..state
    })
}

/// An aging state with every capture named by the converted record that holds it.
pub fn renumber_aging(aging: AgingState, map: &BTreeMap<u64, u64>) -> Result<AgingState, String> {
    let occurrence = |occurrence: Occurrence| -> Result<Occurrence, String> {
        Ok(Occurrence {
            capture: renumber_id(occurrence.capture, map)?,
            ..occurrence
        })
    };
    Ok(AgingState {
        shown: aging
            .shown
            .into_iter()
            .map(|(id, cost, protected)| Ok((occurrence(id)?, cost, protected)))
            .collect::<Result<_, String>>()?,
        unprotected: aging
            .unprotected
            .into_iter()
            .map(occurrence)
            .collect::<Result<_, _>>()?,
        aged: aging
            .aged
            .into_iter()
            .map(occurrence)
            .collect::<Result<_, _>>()?,
        ..aging
    })
}

/// `old`'s engine and aging state as the converted session names its captures: an engine to
/// continue from after the records it folded.
pub fn renumbered(
    old: &OldEngine,
    map: &BTreeMap<u64, u64>,
) -> Result<(SessionMedia, MediaAging), String> {
    let media =
        SessionMedia::from_index_state(renumber_state(old.media.index_state(), map)?, NoBytes)?;
    let aging = MediaAging::from_state(renumber_aging(old.aging.state(), map)?)?;
    Ok((media, aging))
}
