//! Bounded, advisory current-step replay. Nothing here is a durable transcript source.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use broca_protocol::DisplayEvent;
use tokio::sync::broadcast;

use super::LazyRing;

const MAX_BYTES: usize = 1024 * 1024;
const MAX_PARTS: usize = 1024;

#[derive(Default)]
struct State {
    status_source: Option<RunStatusSource>,
    bytes: usize,
    parts: BTreeMap<u64, Option<DisplayEvent>>,
    gap: bool,
}

struct RunStatusSource {
    run_id: broca_protocol::RunId,
    progress: Weak<Mutex<Option<broca_protocol::provider::InFlightStep>>>,
}

impl State {
    fn clear_display(&mut self) {
        let status_source = self.status_source.take();
        *self = Self {
            status_source,
            ..Default::default()
        };
    }
}

#[derive(Clone)]
pub struct DisplaySender {
    ring: LazyRing<Arc<DisplayEvent>>,
    state: Arc<Mutex<State>>,
}

impl DisplaySender {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            ring: LazyRing::new(capacity),
            state: Arc::default(),
        }
    }

    pub fn finish_step(&self) {
        self.state.lock().unwrap().clear_display();
    }

    pub(crate) fn set_run_status_source(
        &self,
        run_id: broca_protocol::RunId,
        progress: &broca_core::provider::LiveStepProgress,
    ) {
        self.state.lock().unwrap().status_source = Some(RunStatusSource {
            run_id,
            progress: Arc::downgrade(progress),
        });
    }

    pub fn send(
        &self,
        event: Arc<DisplayEvent>,
    ) -> Result<usize, broadcast::error::SendError<Arc<DisplayEvent>>> {
        let mut state = self.state.lock().unwrap();
        if matches!(
            *event,
            DisplayEvent::DisplayReset | DisplayEvent::RunStatus { .. }
        ) {
            state.clear_display();
        } else if let (Some((_, part)), Some(delta)) = (event.coordinates(), event.delta()) {
            if state.parts.len() >= MAX_PARTS && !state.parts.contains_key(&part) {
                state.gap = true;
            } else if state.parts.get(&part) != Some(&None) {
                let added = if state.parts.contains_key(&part) {
                    delta.len()
                } else {
                    event.retained_bytes()
                };
                if state.bytes.saturating_add(added) > MAX_BYTES {
                    if let Some(Some(previous)) = state.parts.insert(part, None) {
                        state.bytes -= previous.retained_bytes();
                    }
                } else {
                    state.bytes += added;
                    match state.parts.entry(part) {
                        std::collections::btree_map::Entry::Vacant(slot) => {
                            slot.insert(Some((*event).clone()));
                        }
                        std::collections::btree_map::Entry::Occupied(mut slot) => {
                            if let Some(previous) = slot.get_mut() {
                                previous.append_delta(delta);
                            }
                        }
                    }
                }
            }
        } else if matches!(*event, DisplayEvent::DisplayGap { .. }) {
            state.gap = true;
        }
        self.ring.send(event)
    }

    /// Register under the same lock as publication: the snapshot ends exactly where this
    /// receiver starts, even when provider output arrives during a slow durable replay.
    pub(crate) fn subscribe_snapshot(
        &self,
    ) -> (broadcast::Receiver<Arc<DisplayEvent>>, Vec<DisplayEvent>) {
        let state = self.state.lock().unwrap();
        let rx = self.ring.subscribe();
        let mut events = vec![DisplayEvent::DisplayReset];
        // Captured frames are not current run state. A weak reference reads the actor's slot
        // without keeping a finished step alive; cleared waits and past next times stay absent.
        if let Some(source) = &state.status_source {
            if let Some(progress) = source.progress.upgrade() {
                let step = progress.lock().unwrap();
                if let Some(wait) = step.as_ref().and_then(|step| step.retry_awaits.as_deref()) {
                    let event = broca_protocol::provider::retry_status_display(
                        source.run_id.clone(),
                        Some(wait),
                        crate::store_awaits::now_ms(),
                    );
                    if matches!(
                        &event,
                        DisplayEvent::RunStatus {
                            awaits: Some(_),
                            ..
                        }
                    ) {
                        events.push(event);
                    }
                }
            }
        }
        for value in state.parts.values() {
            events.push(
                value
                    .clone()
                    .unwrap_or(DisplayEvent::DisplayGap { dropped: 1 }),
            );
        }
        if state.gap {
            events.push(DisplayEvent::DisplayGap { dropped: 1 });
        }
        (rx, events)
    }

    #[cfg(test)]
    pub(super) fn is_allocated(&self) -> bool {
        self.ring.is_allocated()
    }
}

impl From<broadcast::Sender<Arc<DisplayEvent>>> for DisplaySender {
    fn from(sender: broadcast::Sender<Arc<DisplayEvent>>) -> Self {
        Self {
            ring: sender.into(),
            state: Arc::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_awaits_snapshot_reads_the_current_slot_without_retaining_a_finished_step() {
        use broca_protocol::provider::{AwaitParty, RunAwaits};
        let display = DisplaySender::new(16);
        let progress = Arc::new(Mutex::new(Some(broca_protocol::provider::InFlightStep {
            retry_awaits: None,
            report_awaits: true,
            step_id: broca_protocol::StepId(1),
            started_at_ms: 1,
            attempt: 1,
            attempt_started_at_ms: 1,
            last_content_at_ms: None,
            max_content_gap_ms: None,
            max_content_gap_ended_at_ms: None,
            request: Default::default(),
        })));
        display.set_run_status_source(broca_protocol::RunId("r1".into()), &progress);
        let _ = display.send(Arc::new(DisplayEvent::DisplayReset));
        let now = crate::store_awaits::now_ms();
        let mut wait = RunAwaits::new(AwaitParty::Runner, now - 2000);
        wait.next_ms = Some(now - 1000);
        progress.lock().unwrap().as_mut().unwrap().retry_awaits = Some(Box::new(wait.clone()));
        let (_, events) = display.subscribe_snapshot();
        let Some(DisplayEvent::RunStatus {
            awaits: Some(actual),
            ..
        }) = events.get(1)
        else {
            panic!("the retry slot is visible after a display reset")
        };
        assert_eq!(actual.since_ms, wait.since_ms);
        assert_eq!(actual.next_ms, None);
        assert_eq!(Arc::strong_count(&progress), 1);
        progress.lock().unwrap().as_mut().unwrap().retry_awaits = None;
        display.finish_step();
        assert!(display
            .subscribe_snapshot()
            .1
            .iter()
            .all(|event| !matches!(event, DisplayEvent::RunStatus { .. })));
        drop(progress);
        assert!(display
            .subscribe_snapshot()
            .1
            .iter()
            .all(|event| !matches!(event, DisplayEvent::RunStatus { .. })));
    }
    fn text(part: u64, delta: &str) -> Arc<DisplayEvent> {
        Arc::new(DisplayEvent::TextDelta {
            mid: Some("m7".into()),
            part: Some(part),
            message_id: broca_protocol::MessageId("provider".into()),
            tool_call_id: None,
            delta: delta.into(),
        })
    }

    #[test]
    fn attach_coalesces_parts_then_delivers_each_live_delta_once() {
        let display = DisplaySender::new(8);
        let _ = display.send(text(1, "a"));
        let _ = display.send(text(1, "b"));
        let _ = display.send(text(3, "c"));
        let (mut rx, snapshot) = display.subscribe_snapshot();
        assert_eq!(
            snapshot,
            vec![
                DisplayEvent::DisplayReset,
                (*text(1, "ab")).clone(),
                (*text(3, "c")).clone()
            ]
        );
        assert!(rx.try_recv().is_err());
        display.send(text(1, "d")).unwrap();
        assert_eq!(rx.try_recv().unwrap(), text(1, "d"));
        display.finish_step();
        assert_eq!(
            display.subscribe_snapshot().1,
            vec![DisplayEvent::DisplayReset]
        );
    }

    #[test]
    fn capped_part_is_dropped_until_reset_without_losing_other_parts() {
        let display = DisplaySender::new(8);
        let _ = display.send(text(0, &"x".repeat(MAX_BYTES)));
        let _ = display.send(text(0, "overflow"));
        let _ = display.send(text(0, "must not restart the lost prefix"));
        let _ = display.send(text(1, "retained"));
        let snapshot = display.subscribe_snapshot().1;
        assert_eq!(
            snapshot,
            vec![
                DisplayEvent::DisplayReset,
                DisplayEvent::DisplayGap { dropped: 1 },
                (*text(1, "retained")).clone()
            ]
        );
        let state = display.state.lock().unwrap();
        assert_eq!(state.bytes, text(1, "retained").retained_bytes());
        drop(state);
        let _ = display.send(Arc::new(DisplayEvent::DisplayReset));
        let _ = display.send(text(0, "retry"));
        assert_eq!(display.subscribe_snapshot().1[1], *text(0, "retry"));
    }
}

#[cfg(test)]
#[test]
fn oversized_provider_identity_cannot_escape_the_step_byte_cap() {
    let display = DisplaySender::new(8);
    let event = DisplayEvent::TextDelta {
        mid: Some("m1".into()),
        part: Some(0),
        message_id: broca_protocol::MessageId("p".repeat(MAX_BYTES + 1)),
        tool_call_id: None,
        delta: "x".into(),
    };
    let _ = display.send(Arc::new(event));
    assert_eq!(
        display.subscribe_snapshot().1,
        vec![
            DisplayEvent::DisplayReset,
            DisplayEvent::DisplayGap { dropped: 1 }
        ]
    );
    assert_eq!(display.state.lock().unwrap().bytes, 0);
}
