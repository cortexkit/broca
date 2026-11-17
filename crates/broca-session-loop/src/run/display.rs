//! The step's events on their way to the display lane: tool-input deltas joined per call, and
//! an answer streamed again when what streamed live isn't what is stored.
//!
//! Tool-input deltas, joined per call.
//!
//! A provider streams a call's input as many small fragments, a large input as hundreds of them
//! in a burst. Each is a display event, and the display lane keeps only the newest 256 events per
//! session for its readers: a burst longer than that overruns a reader that falls behind, which
//! then gets a gap where the frames were. Joining a call's consecutive fragments into one event
//! keeps such a burst to a few events. A joined event is held at most [`HOLD`] or until it
//! reaches [`MAX_BYTES`], and goes out before any other event, so a reader still sees the input
//! grow, and in order with everything around it.

use broca_core::output_events::OutputEvent;
use broca_protocol::content::AssistantMessage;
use broca_protocol::DisplayEvent;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

/// The longest a joined tool-input delta waits for the next fragment of its call.
const HOLD: Duration = Duration::from_millis(100);

/// The most input text one joined delta carries before it goes out.
const MAX_BYTES: usize = 4096;

/// The tool-input delta being joined, and when its first fragment arrived.
#[derive(Default)]
pub(super) struct ToolInputJoin {
    held: Option<(DisplayEvent, Instant)>,
}

impl ToolInputJoin {
    /// Takes `event`, arriving at `now`, and returns the events to send now, in order. A
    /// fragment of the held call's input is added to it; any other event first releases the
    /// held one.
    pub(super) fn take(&mut self, event: DisplayEvent, now: Instant) -> Vec<DisplayEvent> {
        let mut out = Vec::new();
        if !matches!(event, DisplayEvent::ToolInputDelta { .. }) {
            out.extend(self.release());
            out.push(event);
            return out;
        }
        match &mut self.held {
            Some((held, _)) if same_call(held, &event) => {
                held.append_delta(event.delta().unwrap_or_default());
            }
            _ => {
                out.extend(self.release());
                self.held = Some((event, now));
            }
        }
        if let Some((held, since)) = &self.held {
            let full = held.delta().map_or(0, str::len) >= MAX_BYTES;
            if full || now.duration_since(*since) >= HOLD {
                out.extend(self.release());
            }
        }
        out
    }

    /// The held delta, if any, to send before anything that isn't a display event.
    pub(super) fn release(&mut self) -> Option<DisplayEvent> {
        self.held.take().map(|(event, _)| event)
    }

    /// Sends the held delta, if any, on `out`.
    pub(super) async fn flush(&mut self, out: Option<&mpsc::Sender<OutputEvent>>) {
        if let (Some(out), Some(held)) = (out, self.release()) {
            let _ = out.send(OutputEvent::Display(held)).await;
        }
    }

    /// When the held delta must go out if no fragment arrives first: a provider that pauses
    /// mid-input must not leave the fragments it already streamed unshown. `None` holds nothing.
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.held.as_ref().map(|(_, since)| *since + HOLD)
    }
}

/// Notes in `live` whether `event` leaves streamed words on the display lane: a reset clears them,
/// and a delta or a gap marker puts words there.
pub(super) fn note_live(event: &DisplayEvent, live: &mut bool) {
    if matches!(event, DisplayEvent::DisplayReset) {
        *live = false;
    } else if event.delta().is_some() || matches!(event, DisplayEvent::DisplayGap { .. }) {
        *live = true;
    }
}

/// Waits until `deadline`, or forever when there is none.
pub(super) async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Resets the display lane and streams `message` on it again, each text, reasoning and tool-input
/// block at its index in `message`, so a live view shows the answer as it will be stored.
pub(super) async fn restream(
    out: Option<&mpsc::Sender<OutputEvent>>,
    message: &AssistantMessage,
    live: &mut bool,
) {
    use broca_protocol::content::ContentBlock;
    use broca_protocol::DisplayEvent;
    let Some(out) = out else {
        return;
    };
    let _ = out
        .send(OutputEvent::Display(DisplayEvent::DisplayReset))
        .await;
    *live = false;
    for (index, part) in message.content.iter().enumerate() {
        let mut event = match part {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                DisplayEvent::TextDelta {
                    mid: None,
                    part: None,
                    message_id: message.message_id.clone(),
                    tool_call_id: None,
                    delta: text.to_string(),
                }
            }
            ContentBlock::Reasoning { text, .. } => DisplayEvent::ReasoningDelta {
                mid: None,
                part: None,
                message_id: message.message_id.clone(),
                delta: text.to_string(),
            },
            ContentBlock::ToolCall(call) => DisplayEvent::ToolInputDelta {
                mid: None,
                part: None,
                tool_call_id: call.tool_call_id.clone(),
                delta: call.input.to_string(),
            },
            _ => continue,
        };
        event.set_part(index as u64);
        let _ = out.send(OutputEvent::Display(event)).await;
        *live = true;
    }
}

/// Whether two tool-input deltas are fragments of the same call's input, at the same place.
fn same_call(held: &DisplayEvent, next: &DisplayEvent) -> bool {
    match (held, next) {
        (
            DisplayEvent::ToolInputDelta {
                mid,
                part,
                tool_call_id,
                ..
            },
            DisplayEvent::ToolInputDelta {
                mid: next_mid,
                part: next_part,
                tool_call_id: next_call,
                ..
            },
        ) => mid == next_mid && part == next_part && tool_call_id == next_call,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::ToolCallId;

    fn fragment(call: &str, text: &str) -> DisplayEvent {
        DisplayEvent::ToolInputDelta {
            mid: None,
            part: None,
            tool_call_id: ToolCallId(call.into()),
            delta: text.into(),
        }
    }

    fn text(delta: &str) -> DisplayEvent {
        DisplayEvent::TextDelta {
            mid: None,
            part: None,
            message_id: broca_protocol::ids::MessageId("m".into()),
            tool_call_id: None,
            delta: delta.into(),
        }
    }

    /// A burst of one call's fragments goes out as one event carrying all of them, released by
    /// the next event of another kind, which follows it.
    #[test]
    fn a_burst_of_one_calls_fragments_goes_out_as_one_event() {
        let start = Instant::now();
        let mut join = ToolInputJoin::default();
        for piece in ["{\"pa", "th\":", " \"a\"", "}"] {
            assert!(join.take(fragment("c1", piece), start).is_empty());
        }
        assert_eq!(
            join.take(text("done"), start),
            [fragment("c1", "{\"path\": \"a\"}"), text("done")]
        );
        assert_eq!(join.release(), None);
    }

    /// A fragment of another call releases the held one first, so each call's input stays in
    /// order and apart.
    #[test]
    fn another_calls_fragment_releases_the_held_one() {
        let start = Instant::now();
        let mut join = ToolInputJoin::default();
        join.take(fragment("c1", "{\"a\""), start);
        assert_eq!(
            join.take(fragment("c2", "{\"b\""), start),
            [fragment("c1", "{\"a\"")]
        );
        assert_eq!(join.release(), Some(fragment("c2", "{\"b\"")));
    }

    /// A held delta goes out once it has waited its time or reached its size, so a slow or long
    /// input still shows progress.
    #[test]
    fn a_held_delta_goes_out_after_its_time_or_size() {
        let start = Instant::now();
        let mut join = ToolInputJoin::default();
        join.take(fragment("c1", "a"), start);
        assert_eq!(
            join.take(fragment("c1", "b"), start + HOLD),
            [fragment("c1", "ab")]
        );
        let long = "x".repeat(MAX_BYTES);
        assert_eq!(
            join.take(fragment("c1", &long), start + HOLD),
            [fragment("c1", &long)]
        );
        assert_eq!(join.release(), None);
    }
}
