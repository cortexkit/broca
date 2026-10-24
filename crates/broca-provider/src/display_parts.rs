//! Correct live array positions when a provider introduces an earlier stored block late.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use broca_protocol::{DisplayEvent, NormalizedEvent, StepId};

static INDEX_RESETS: AtomicU64 = AtomicU64::new(0);

pub fn index_resets() -> u64 {
    INDEX_RESETS.load(Ordering::Relaxed)
}

type Key = (u64, u64);
const MAX_BYTES: usize = 1024 * 1024;
const MAX_PARTS: usize = 1024;

#[derive(Default)]
struct Part {
    event: Option<DisplayEvent>,
    emitted: bool,
    dropped: bool,
}

#[derive(Default)]
pub(crate) struct DisplayParts {
    parts: BTreeMap<Key, Part>,
    bytes: usize,
    overflowed: bool,
}

impl DisplayParts {
    pub(crate) fn reserve(
        &mut self,
        key: Key,
        family: &str,
        step: StepId,
        out: &mut Vec<NormalizedEvent>,
    ) {
        if self.parts.contains_key(&key) {
            return;
        }
        if self.overflowed {
            return;
        }
        if self.parts.len() == MAX_PARTS {
            self.overflowed = true;
            self.parts.clear();
            self.bytes = 0;
            out.push(NormalizedEvent::Display(DisplayEvent::DisplayGap {
                dropped: 1,
            }));
            return;
        }
        let shifts_emitted = self.parts.range(key..).any(|(_, value)| value.emitted);
        self.parts.insert(key, Part::default());
        if shifts_emitted {
            self.restream(family, step, out);
        }
    }

    pub(crate) fn delta(
        &mut self,
        key: Key,
        mut event: DisplayEvent,
        family: &str,
        step: StepId,
        out: &mut Vec<NormalizedEvent>,
    ) {
        self.reserve(key, family, step, out);
        if self.overflowed {
            return;
        }
        let index = self.parts.range(..key).count() as u64;
        event.set_part(index);
        let slot = self.parts.get_mut(&key).expect("reserved display part");
        slot.emitted = true;
        let text = event.delta().unwrap_or_default();
        if !slot.dropped {
            let added = if slot.event.is_some() {
                text.len()
            } else {
                event.retained_bytes()
            };
            if self.bytes.saturating_add(added) > MAX_BYTES {
                self.bytes -= slot.event.take().map(|e| e.retained_bytes()).unwrap_or(0);
                slot.dropped = true;
            } else {
                self.bytes += added;
                if let Some(previous) = &mut slot.event {
                    previous.append_delta(text);
                } else {
                    slot.event = Some(event.clone());
                }
            }
        }
        out.push(NormalizedEvent::Display(event));
    }

    /// Incomplete trailing blocks may be discarded at assembly. Correct the positions of
    /// surviving emitted blocks if a discarded block was before them.
    pub(crate) fn retain(
        &mut self,
        keys: &[(u64, u64)],
        family: &str,
        step: StepId,
        out: &mut Vec<NormalizedEvent>,
    ) {
        let mut next = 0;
        let mut shifted = false;
        let mut discarded = false;
        for (old, (key, value)) in self.parts.iter().enumerate() {
            if keys.contains(key) {
                shifted |= value.emitted && old != next;
                next += 1;
            } else {
                discarded |= value.emitted;
            }
        }
        self.parts.retain(|key, _| keys.contains(key));
        self.bytes = self
            .parts
            .values()
            .filter_map(|p| p.event.as_ref())
            .map(|e| e.retained_bytes())
            .sum();
        if shifted {
            self.restream(family, step, out);
        } else if discarded {
            // A dropped trailing block moves no surviving index, but its live row must not
            // survive assembly. This is not an index-shift or retry reset.
            tracing::debug!(
                target: "provider.display.indices",
                family,
                step = step.0,
                "discarded display block at assembly; resetting"
            );
            self.replay(out);
        }
    }

    fn restream(&self, family: &str, step: StepId, out: &mut Vec<NormalizedEvent>) {
        INDEX_RESETS.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "provider.display.indices",
            family,
            step = step.0,
            "display block indices shifted; resetting and re-streaming"
        );
        self.replay(out);
    }

    fn replay(&self, out: &mut Vec<NormalizedEvent>) {
        out.push(NormalizedEvent::Display(DisplayEvent::DisplayReset));
        for (index, value) in self.parts.values().enumerate() {
            if value.dropped {
                out.push(NormalizedEvent::Display(DisplayEvent::DisplayGap {
                    dropped: 1,
                }));
            } else if let Some(event) = &value.event {
                let mut event = event.clone();
                event.set_part(index as u64);
                out.push(NormalizedEvent::Display(event));
            }
        }
    }
}
