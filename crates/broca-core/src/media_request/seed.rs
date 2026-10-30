//! Content-free checkpoint of capture positions and aging inputs. No transcript is persisted.
use super::*;
use broca_protocol::Role;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Slot {
    block: usize,
    part: Option<usize>,
    call: Option<ToolCallId>,
    reference: MediaRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Skeleton {
    ordinal: Option<u64>,
    role: Role,
    blocks: usize,
    slots: Vec<Slot>,
}

impl Skeleton {
    fn capture(message: &Message) -> Self {
        let mut slots = Vec::new();
        for (block, value) in message.content.iter().enumerate() {
            match value {
                ContentBlock::Media(reference) => slots.push(Slot {
                    block,
                    part: None,
                    call: None,
                    reference: reference.clone(),
                }),
                ContentBlock::ToolResult(result) => {
                    if let ToolOutput::Parts { parts } = &result.output {
                        for (part, value) in parts.iter().enumerate() {
                            if let ToolOutputPart::Media { media } = value {
                                slots.push(Slot {
                                    block,
                                    part: Some(part),
                                    call: Some(result.tool_call_id.clone()),
                                    reference: media.clone(),
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Self {
            ordinal: message.ordinal,
            role: message.role,
            blocks: message.content.len(),
            slots,
        }
    }

    fn restore(&self) -> Message {
        let mut message = Message::new(
            self.role,
            vec![
                ContentBlock::Text {
                    text: String::new()
                };
                self.blocks
            ],
        );
        message.ordinal = self.ordinal;
        for slot in &self.slots {
            if let (Some(part), Some(call)) = (slot.part, &slot.call) {
                if !matches!(message.content[slot.block], ContentBlock::ToolResult(_)) {
                    message.content[slot.block] =
                        ContentBlock::ToolResult(broca_protocol::ToolResult {
                            tool_call_id: call.clone(),
                            output: ToolOutput::Parts { parts: Vec::new() },
                            is_error: false,
                        });
                }
                if let ContentBlock::ToolResult(result) = &mut message.content[slot.block] {
                    if let ToolOutput::Parts { parts } = &mut result.output {
                        parts.resize_with(part + 1, || ToolOutputPart::Text {
                            text: String::new(),
                        });
                        parts[part] = ToolOutputPart::Media {
                            media: slot.reference.clone(),
                        };
                    }
                }
            } else {
                message.content[slot.block] = ContentBlock::Media(slot.reference.clone());
            }
        }
        message
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Index {
    placement: Placement,
    records: u64,
    boundary: u64,
    last_step: Option<StepId>,
    captures: Vec<Capture>,
    pending: Vec<MediaId>,
    steers: Vec<Vec<MediaId>>,
    protected: Vec<MediaId>,
    skeleton: Vec<(u64, Skeleton)>,
    held: Vec<((u64, usize), Skeleton)>,
    batch_order: Vec<(ToolCallId, usize)>,
    replacements: Vec<(Vec<MediaRef>, u64, u64)>,
    selected: Vec<MediaBinding>,
    covered: Option<u64>,
    has_view: bool,
}

impl Index {
    fn capture(session: &SessionMedia) -> Self {
        let mut captures: Vec<_> = session.captures.values().cloned().collect();
        captures.sort_by_key(|capture| capture.id);
        let mut held: Vec<_> = session
            .held_messages
            .iter()
            .map(|(key, message)| (*key, Skeleton::capture(message)))
            .collect();
        held.sort_by_key(|(key, _)| *key);
        let mut batch_order: Vec<_> = session
            .batch_order
            .iter()
            .map(|(id, order)| (id.clone(), *order))
            .collect();
        batch_order.sort_by_key(|(_, order)| *order);
        Self {
            placement: session.placement.clone(),
            records: session.records,
            boundary: session.boundary,
            last_step: session.last_step,
            captures,
            pending: session.pending.clone(),
            steers: session.steers.clone(),
            protected: session.protected.clone(),
            skeleton: session
                .skeleton
                .iter()
                .map(|(ordinal, message)| (*ordinal, Skeleton::capture(message)))
                .collect(),
            held,
            batch_order,
            replacements: session.replacements.clone(),
            selected: session.selected_bindings(),
            covered: session.covered(),
            has_view: session.compaction.view.is_some() || session.restored_view,
        }
    }

    fn restore(self, store: Option<MediaStore>) -> SessionMedia {
        let mut session = SessionMedia::metadata(store);
        session.placement = self.placement;
        session.records = self.records;
        session.boundary = self.boundary;
        session.last_step = self.last_step;
        for capture in self.captures {
            session.compaction.captured.insert(&capture.reference);
            session.insert(capture, false);
        }
        session.pending = self.pending;
        session.steers = self.steers;
        session.protected = self.protected;
        session.skeleton = self
            .skeleton
            .into_iter()
            .map(|(ordinal, message)| (ordinal, message.restore()))
            .collect();
        session.held_messages = self
            .held
            .into_iter()
            .map(|(key, message)| (key, message.restore()))
            .collect();
        session.batch_order = self.batch_order.into_iter().collect();
        session.replacements = self.replacements;
        session.restored_selection = Some(self.selected);
        session.restored_covered = self.covered;
        session.restored_view = self.has_view;
        session
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MediaHistory {
    Profile {
        media: serde_json::Value,
        selected: Vec<MediaBinding>,
    },
    Changes(Vec<MediaChange>),
    Boundary,
}

#[derive(Debug, Clone)]
pub struct MediaCheckpoint {
    pub at_seq: u64,
    pub step_open: bool,
    pub history: Vec<MediaHistory>,
    pub(crate) session: SessionMedia,
    profile: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
struct WireSeed {
    version: u32,
    at_seq: u64,
    step_open: bool,
    history: Vec<MediaHistory>,
    index: Index,
    // Detect damaged metadata independently of media files. This is not authorization;
    // the surrounding checkpoint's verified WAL boundary remains the provenance proof.
    sha256: String,
}

fn digest(seed: &WireSeed) -> String {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(&(
        seed.version,
        seed.at_seq,
        seed.step_open,
        &seed.history,
        &seed.index,
    ))
    .expect("media metadata serializes");
    format!("{:x}", Sha256::digest(bytes))
}

impl MediaCheckpoint {
    pub fn empty() -> Self {
        Self {
            at_seq: 0,
            step_open: false,
            history: Vec::new(),
            session: SessionMedia::metadata(None),
            profile: None,
        }
    }

    pub fn empty_at(placement: Placement, at_seq: u64) -> Self {
        let mut checkpoint = Self::empty();
        checkpoint.at_seq = at_seq;
        checkpoint.session.records = at_seq;
        checkpoint.session.placement = placement;
        checkpoint
    }

    pub fn decode(value: &serde_json::Value) -> Result<Self, String> {
        if let Some(version) = value.get("version").and_then(|version| version.as_u64()) {
            if version != 1 {
                return Err(format!("media_seed_unknown_version: {version}"));
            }
        }
        let wire: WireSeed = serde_json::from_value(value.clone())
            .map_err(|error| format!("media_seed_undecodable: {error}"))?;
        if wire.version != 1 {
            return Err(format!("media_seed_unknown_version: {}", wire.version));
        }
        if wire.sha256 != digest(&wire) {
            return Err("media_seed_undecodable: metadata digest mismatch".into());
        }
        let ids: std::collections::HashMap<_, _> = wire
            .index
            .captures
            .iter()
            .map(|capture| (capture.id, &capture.reference))
            .collect();
        // Every slot occupied bytes in its original WAL frame. Seed positions cannot exceed
        // that frame's payload bound, even when a damaged index claims much larger offsets.
        let max_slots = broca_wal::framing::MAX_PAYLOAD_LEN as usize;
        let valid_skeleton = |skeleton: &Skeleton| {
            skeleton.blocks <= max_slots
                && skeleton.slots.iter().all(|slot| {
                    slot.block < skeleton.blocks && slot.part.is_none_or(|part| part < max_slots)
                })
        };
        if wire.index.records != wire.at_seq
            || ids.len() != wire.index.captures.len()
            || wire
                .index
                .captures
                .iter()
                .any(|capture| capture.id.record > wire.at_seq)
            || wire
                .index
                .pending
                .iter()
                .chain(&wire.index.protected)
                .chain(wire.index.steers.iter().flatten())
                .any(|id| !ids.contains_key(id))
            || wire
                .index
                .selected
                .iter()
                .any(|binding| ids.get(&binding.id).copied() != Some(&binding.reference))
            || wire
                .index
                .skeleton
                .iter()
                .any(|(_, skeleton)| !valid_skeleton(skeleton))
            || wire
                .index
                .held
                .iter()
                .any(|(_, skeleton)| !valid_skeleton(skeleton))
        {
            return Err("media_seed_undecodable: invalid capture positions".into());
        }
        let profile = wire.history.iter().rev().find_map(|event| match event {
            MediaHistory::Profile { media, .. } => Some(media.clone()),
            _ => None,
        });
        Ok(Self {
            at_seq: wire.at_seq,
            step_open: wire.step_open,
            history: wire.history,
            session: wire.index.restore(None),
            profile,
        })
    }

    pub fn encode(&self) -> serde_json::Value {
        let mut wire = WireSeed {
            version: 1,
            at_seq: self.at_seq,
            step_open: self.step_open,
            history: self.history.clone(),
            index: Index::capture(&self.session),
            sha256: String::new(),
        };
        wire.sha256 = digest(&wire);
        serde_json::to_value(wire).expect("media metadata serializes")
    }

    pub fn resume_budget(&mut self, budget: Option<broca_protocol::request::BudgetConfig>) {
        self.session.placement = Placement::from_index_seed(self.session.placement.clone(), budget);
    }

    pub fn fold(&mut self, seq: u64, record: &WalRecord) -> Result<(), String> {
        self.session.fold(record)?;
        let frozen = match record {
            WalRecord::RunStarted { config, .. } => Some(&config.frozen_render_config),
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => Some(frozen_render_config),
            _ => None,
        };
        if let Some(media) = frozen
            .and_then(|frozen| frozen.get("media"))
            .filter(|media| !media.is_null())
            .filter(|media| self.profile.as_ref() != Some(*media))
        {
            self.profile = Some(media.clone());
            self.history.push(MediaHistory::Profile {
                media: media.clone(),
                selected: self.session.selected_bindings(),
            });
        }
        let changes = self.session.take_changes();
        if !changes.is_empty() {
            self.history.push(MediaHistory::Changes(changes));
        }
        match record {
            WalRecord::RunStarted { .. } => self.step_open = false,
            WalRecord::StepStarted { .. } => self.step_open = true,
            WalRecord::ModelAttemptFinished { .. } | WalRecord::ModelStepFinished { .. } => {
                if !matches!(self.history.last(), Some(MediaHistory::Boundary)) {
                    self.history.push(MediaHistory::Boundary);
                }
                if matches!(record, WalRecord::ModelStepFinished { .. }) {
                    self.step_open = false;
                }
            }
            _ => {}
        }
        self.at_seq = seq;
        Ok(())
    }

    pub fn into_session(
        mut self,
        store: MediaStore,
        budget: Option<broca_protocol::request::BudgetConfig>,
    ) -> (SessionMedia, Vec<MediaHistory>, bool, u64) {
        self.session.store = Some(store);
        self.session.placement = Placement::from_index_seed(self.session.placement, budget);
        (self.session, self.history, self.step_open, self.at_seq)
    }
}

/// Unknown or damaged media metadata must not make the surrounding legacy checkpoint
/// undecodable. Admission can name the seed failure and rebuild from the durable log.
#[derive(Debug, Clone)]
pub enum DurableMediaSeed {
    Ready(Box<MediaCheckpoint>),
    Unreadable(serde_json::Value),
}

impl PartialEq for DurableMediaSeed {
    fn eq(&self, other: &Self) -> bool {
        serde_json::to_value(self).ok() == serde_json::to_value(other).ok()
    }
}

impl Serialize for DurableMediaSeed {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Ready(seed) => seed.encode().serialize(serializer),
            Self::Unreadable(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for DurableMediaSeed {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        Ok(match MediaCheckpoint::decode(&value) {
            Ok(seed) => Self::Ready(Box::new(seed)),
            Err(_) => Self::Unreadable(value),
        })
    }
}

static SEED_FALLBACKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn seed_fallback_count() -> u64 {
    SEED_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Record a named repair when a media reader cannot trust its stored checkpoint.
pub fn count_seed_fallback(reason: &str) {
    SEED_FALLBACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tracing::warn!(reason, "media checkpoint requires full durable replay");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restored_view_coverage_wins_over_later_setup_metadata() {
        let mut checkpoint = MediaCheckpoint::empty();
        checkpoint.session.restored_selection = Some(Vec::new());
        checkpoint.session.restored_covered = Some(2);
        checkpoint.session.restored_view = true;
        let setup: WalRecord = serde_json::from_value(serde_json::json!({
            "type":"compaction_message", "provider":"compactor", "kind":"setup",
            "compaction_id":"later-setup", "version":1, "range":{"from":0,"to":9},
            "replacement":[], "sha256":"fixture"
        }))
        .unwrap();
        checkpoint.fold(1, &setup).unwrap();
        let encoded = checkpoint.encode();
        assert_eq!(
            encoded["index"]["covered"], 2,
            "an applied view still takes precedence over Setup"
        );
        let restored = MediaCheckpoint::decode(&encoded).unwrap();
        assert_eq!(restored.session.covered(), Some(2));
    }
}
