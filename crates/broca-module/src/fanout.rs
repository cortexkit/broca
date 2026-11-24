//! The subscribe lanes' shared pieces: the units a subscriber receives ([`Delivery`]), where a
//! subscription starts ([`FromCursor`]), the open step's replay ([`StepReplay`]), the attach
//! barrier's dedup decision ([`deliver_from_ring`]), the ring sizes, and delivery to a sink with
//! its counters. The session store's fan-out (`store_fanout`) builds on these.
//!
//! The control lane is reliable: it is projected from the session's records and addressed by a
//! cursor, so a subscriber that falls behind detaches and replays from its durable cursor. The
//! display lane is lossy and never replayed.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use broca_core::Cursor;
use broca_protocol::caller::ControlUnit;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// One cursor-tagged control unit on the stream, with its live event when the unit has one (a
/// unit without a lifecycle of its own, such as one tool call of a stored assistant message, is
/// sent with none).
/// The bounded Tokio broadcast channel shares this tuple behind an [`Arc`] so each receiver
/// clones a pointer, not the owned message/tool payload.
pub type ControlEvent = (
    Cursor,
    ControlUnit,
    Option<cortexkit_role_llm_runner::live::LiveEvent>,
);

/// What a subscriber receives on the unified session stream. Per the module surface, one
/// `session.subscribe` carries BOTH the reliable+replayable control stream and the
/// live+lossy display stream.
///
/// `Control` is a cursor-tagged control unit (reliable; replayed on attach, deduped).
/// Both event variants share their owned payload so fan-out only clones an [`Arc`] per subscriber.
/// `Display` is advisory current-step replay followed by live display (both lossy).
/// `Detached` means the subscriber fell behind the bounded control broadcast channel and must
/// resubscribe from its last cursor. A client never consumes a post-gap control event as contiguous.
/// Display lag is signalled in-band by a `DisplayGap` event, not a detach.
#[derive(Debug, Clone)]
pub enum Delivery {
    /// Internal wake-up: recheck this subscriber's admission without emitting a wire event.
    AdmissionChanged,
    CursorExpired,
    Expired {
        expired_at_ms: u64,
    },
    Control(Arc<ControlEvent>),
    Display(Arc<broca_protocol::display::DisplayEvent>),
    /// The subscriber could not replay the durable prefix; the serve adapter emits a typed
    /// stream Error frame instead of misreporting a clean empty stream.
    Error(String),
    /// The subscriber's read access ended with a change the session accepted: a refresh that
    /// dropped it, or an adoption that changed the owner. The stream ends with the refusal a new
    /// subscribe would get, and no event recorded after the change is sent.
    AccessEnded(Box<broca_core::ScopeRefusal>),
    Detached,
}

/// Where a subscriber attaches from.
#[derive(Debug, Clone, Copy)]
pub enum FromCursor {
    /// From the very start of the lineage (seq 1).
    Start,
    /// Strictly after this durable cursor (a resume from the client's last-processed
    /// position — at-least-once from here; re-delivery of `<= After` is the client's to
    /// ignore).
    After(Cursor),
    Resume {
        cursor: Cursor,
        proof: ResumeCursorProof,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct ResumeCursorProof {
    incarnation: [u8; 32],
    digest: u64,
}

impl ResumeCursorProof {
    pub fn new(incarnation: &str, digest: &str) -> Option<Self> {
        use sha2::{Digest, Sha256};
        if incarnation.is_empty()
            || digest.len() != 16
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return None;
        }
        Some(Self {
            incarnation: Sha256::digest(incarnation.as_bytes()).into(),
            digest: u64::from_str_radix(digest, 16).ok()?,
        })
    }

    pub(crate) fn matches(self, cursor: Cursor) -> bool {
        use sha2::{Digest, Sha256};
        if let Some((incarnation, digest)) = cursor.store_identity() {
            let text: String = incarnation
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            let incarnation: [u8; 32] = Sha256::digest(text.as_bytes()).into();
            return incarnation == self.incarnation && digest == self.digest;
        }
        false
    }
}

impl FromCursor {
    /// Should an event at `cursor` be delivered for this attach point?
    pub(crate) fn includes(&self, cursor: Cursor) -> bool {
        match self {
            FromCursor::Start => true,
            FromCursor::After(c) | FromCursor::Resume { cursor: c, .. } => cursor > *c,
        }
    }

    pub(crate) fn position(self) -> Option<Cursor> {
        match self {
            Self::Start => None,
            Self::After(cursor) | Self::Resume { cursor, .. } => Some(cursor),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AttachFrom {
    Cursor(FromCursor),
    Live,
}

/// Retains only the open step's replay state.
pub(crate) struct StepReplay<T> {
    current: Option<Vec<T>>,
}

impl<T> Default for StepReplay<T> {
    fn default() -> Self {
        Self { current: None }
    }
}

impl<T> StepReplay<T> {
    pub(crate) fn observe(&mut self, record: &broca_session_store::record::Record) {
        use broca_session_store::record::Record as S;
        match record {
            S::StepStart(_) => {
                self.current = Some(Vec::new());
            }
            S::RunStart(_) | S::BatchEnd(_) | S::RunEnd(_) | S::Pause(_) => {
                self.current = None;
            }
            _ => {}
        }
    }

    pub(crate) fn push(&mut self, value: T) {
        if let Some(current) = &mut self.current {
            current.push(value);
        }
    }

    pub(crate) fn units(&self) -> &[T] {
        self.current.as_deref().unwrap_or_default()
    }
}

/// The attach-barrier decision for a unit arriving on the LIVE ring (step 2 of the
/// subscriber task): deliver it ONLY if it's past the barrier head `h` AND past the
/// client's attach point. A unit with `cursor.wal_seq <= h` was already delivered by the
/// replay (step 1), so it's the DEDUP skip — without it, a record fsynced before `h` but
/// ring-published after the subscriber registered would be delivered twice. Pure so the
/// dedup is unit-testable in isolation (the integration stress proves it fires under real
/// concurrency; this proves the decision itself is correct).
pub(crate) fn deliver_from_ring(cursor: Cursor, h: u64, from: FromCursor) -> bool {
    cursor.wal_seq > h && from.includes(cursor)
}

/// How many live control events the ring buffers before a slow subscriber lags + detaches.
pub(crate) const RING_CAPACITY: usize = 1024;

/// How many live DISPLAY events the display ring buffers. Display is LOSSY by design — a
/// slow display subscriber drops frames (a `DisplayGap` marks the gap), it never detaches
/// or backpressures the run. Smaller than the control ring: display is a render
/// convenience, not durable.
pub(crate) const DISPLAY_RING_CAPACITY: usize = 256;

/// The live display stream's sender, handed to a run's output drain.
#[path = "display_replay.rs"]
mod display_replay;
pub(crate) use display_replay::DisplaySender;

/// Successful subscriber deliveries by lane, and successful wire encodes, since boot.
///
/// They give `health.check` a positive signal that fan-out is delivering anything at all, and
/// a denominator for turning the per-delivery cost of the fan-out seam into a deployed cost.
/// `stream_chunks` can't serve as that denominator: one decoded SSE payload normalizes to zero
/// or several display events. The `frames_dropped_*` counters reported beside these are
/// unrelated: they live in `broca-subc`'s connection and count inbound frames on the client
/// socket, not subscriber fan-out.
///
/// Process-local and lifetime; they reset on restart, so diff two readings carrying the SAME
/// `launch_id` to get a rate. Relaxed ordering: these are counters, not fences, and no
/// decision reads them.
static CONTROL_DELIVERIES: AtomicU64 = AtomicU64::new(0);
static DISPLAY_DELIVERIES: AtomicU64 = AtomicU64::new(0);
static DISPLAY_GAPS: AtomicU64 = AtomicU64::new(0);

/// Control-lane units delivered to a subscriber sink since boot.
pub fn control_deliveries() -> u64 {
    CONTROL_DELIVERIES.load(Ordering::Relaxed)
}

/// Display-lane events delivered to a subscriber sink since boot.
pub fn display_deliveries() -> u64 {
    DISPLAY_DELIVERIES.load(Ordering::Relaxed)
}

/// Display-lane gap notices delivered since boot — the lossy lane's only loss signal.
///
/// Display lag is BENIGN by design (the lane is lossy and has no durable replay), so a gap is
/// not an error and must not read as one. It is also not a delivery: counting it as one made
/// `display_deliveries` rise on LOSS, which is the reading an operator would least expect.
pub fn display_gaps() -> u64 {
    DISPLAY_GAPS.load(Ordering::Relaxed)
}

/// Deliver one item to the sink, honoring cancellation. Returns `Break` if the subscriber
/// should stop (the sink closed = the client went away, or cancelled = close teardown).
pub(crate) async fn deliver(
    sink: &mpsc::Sender<Delivery>,
    cancel: &CancellationToken,
    item: Delivery,
) -> std::ops::ControlFlow<()> {
    // COUNT THE LANE BEFORE THE MOVE, because `item` is consumed by `send`.
    //
    // A GAP IS A LOSS NOTICE WEARING A DISPLAY DELIVERY'S ENVELOPE. The first version of this
    // match discriminated on `Delivery` alone, and the comment above it already said a Gap must
    // not count — but `DisplayGap` is a DisplayEvent PAYLOAD variant, wrapped in
    // `Delivery::Display` at the lag arm, so `display_deliveries` incremented on display LOSS.
    // The rule was stated at the right level and implemented one level too shallow.
    //
    // Detached and Error carry no unit at all, so they never count either.
    let lane = match &item {
        Delivery::Control(_) => Some(&CONTROL_DELIVERIES),
        Delivery::Display(event) => match **event {
            // Not a delivery AND not nothing: a gap is the display lane's only loss signal,
            // so it gets its own counter rather than vanishing from both readings.
            broca_protocol::display::DisplayEvent::DisplayGap { .. } => Some(&DISPLAY_GAPS),
            _ => Some(&DISPLAY_DELIVERIES),
        },
        _ => None,
    };
    tokio::select! {
        _ = cancel.cancelled() => std::ops::ControlFlow::Break(()),
        r = sink.send(item) => {
            if r.is_err() {
                std::ops::ControlFlow::Break(()) // the client dropped its receiver
            } else {
                // AFTER the send succeeds: a counter incremented before it would count
                // attempts, and the question an operator asks is whether anything ARRIVED.
                if let Some(counter) = lane {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                std::ops::ControlFlow::Continue(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs `test` alone in a child copy of this test binary and returns whether the caller is
    /// that child, which then runs the test body. The parent only checks that the child passed.
    fn in_isolated_process(test: &str) -> bool {
        const CHILD_TEST: &str = "BROCA_ISOLATED_FANOUT_TEST";
        if std::env::var(CHILD_TEST).as_deref() == Ok(test) {
            return true;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .env(CHILD_TEST, test)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("spawn the isolated test");
        assert!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "isolated test {test} failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        false
    }

    /// THE DELIVERY COUNTERS MOVE ON A REAL DELIVERY, AND ONLY ON ONE.
    ///
    /// A counter that reads zero forever is worse than no counter: it answers the operator's
    /// question with "nothing is being delivered" when the truth is "I am not wired". That
    /// shipped here once already, when a projection-failure counter sat inside `#[cfg(test)]`
    /// and read permanently zero in release builds.
    ///
    /// Three arms, because the failure modes are different: a payload delivery must count, a
    /// control-plane notice carrying no unit must NOT (it would inflate the denominator), and a
    /// send to a closed sink must NOT (it never arrived). The counters are process-global, so
    /// another test delivering at the same moment would move them between a read and its
    /// check; the test therefore runs alone in a child copy of this binary.
    #[tokio::test]
    async fn delivery_counters_count_arrivals_and_nothing_else() {
        if !in_isolated_process("fanout::tests::delivery_counters_count_arrivals_and_nothing_else")
        {
            return;
        }
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel::<Delivery>(8);

        let before = display_deliveries();
        let event = Arc::new(broca_protocol::display::DisplayEvent::TextDelta {
            mid: None,
            part: Some(0),
            message_id: broca_protocol::MessageId("m1".to_string()),
            tool_call_id: None,
            delta: "hi".to_string(),
        });
        assert!(matches!(
            deliver(&tx, &cancel, Delivery::Display(Arc::clone(&event))).await,
            std::ops::ControlFlow::Continue(())
        ));
        assert_eq!(
            display_deliveries() - before,
            1,
            "a delivered display event must count exactly once"
        );

        // A GAP IS THE ARM THIS TEST DID NOT HAVE, and its absence is why the defect
        // shipped: a DisplayGap rides in a Delivery::Display envelope, so a match on the
        // envelope alone counted display LOSS as a delivery.
        let before_gap_d = display_deliveries();
        let before_gap_g = display_gaps();
        assert!(matches!(
            deliver(
                &tx,
                &cancel,
                Delivery::Display(Arc::new(
                    broca_protocol::display::DisplayEvent::DisplayGap { dropped: 3 }
                ))
            )
            .await,
            std::ops::ControlFlow::Continue(())
        ));
        assert_eq!(
            (
                display_deliveries() - before_gap_d,
                display_gaps() - before_gap_g
            ),
            (0, 1),
            "a gap is LOSS: it must not count as a delivery, and must not vanish either"
        );

        let before_notice = display_deliveries();
        let before_control = control_deliveries();
        assert!(matches!(
            deliver(&tx, &cancel, Delivery::Detached).await,
            std::ops::ControlFlow::Continue(())
        ));
        assert_eq!(
            (
                display_deliveries() - before_notice,
                control_deliveries() - before_control
            ),
            (0, 0),
            "a Detached notice carries no unit and must not count on either lane"
        );

        // Drop the receiver: the client went away, so the send fails and nothing arrived.
        drop(rx);
        let before_closed = display_deliveries();
        assert!(matches!(
            deliver(&tx, &cancel, Delivery::Display(event)).await,
            std::ops::ControlFlow::Break(())
        ));
        assert_eq!(
            display_deliveries() - before_closed,
            0,
            "a send to a closed sink never arrived and must not count"
        );
    }

    /// The dedup decision is correct + non-vacuous: a live unit at `wal_seq <= h` was
    /// already replayed (step 1) so it MUST be skipped (the dedup); one at `wal_seq > h`
    /// MUST be delivered. This is the no-DUP half of the attach barrier, proven on the pure
    /// decision (the free-running stress proves it fires under real concurrency).
    #[test]
    fn ring_dedup_skips_at_or_below_h_delivers_above() {
        let h = 5u64;
        // At/below H → already in the replay → SKIP (the dedup; without it = a duplicate).
        assert!(!deliver_from_ring(Cursor::new(3, 0), h, FromCursor::Start));
        assert!(!deliver_from_ring(Cursor::new(5, 0), h, FromCursor::Start));
        // A multi-unit record at exactly H (sub_index > 0) is still ≤ H → skip.
        assert!(!deliver_from_ring(Cursor::new(5, 2), h, FromCursor::Start));
        // Above H → only on the live ring → DELIVER.
        assert!(deliver_from_ring(Cursor::new(6, 0), h, FromCursor::Start));
    }

    /// The attach-point filter composes with the dedup: a `From::After(c)` subscriber gets
    /// only units strictly after BOTH `h` and its own cursor `c`.
    #[test]
    fn ring_filter_honors_attach_point() {
        let h = 5u64;
        let from = FromCursor::After(Cursor::new(7, 0));
        // > h but not past the attach point → skip.
        assert!(!deliver_from_ring(Cursor::new(6, 0), h, from));
        assert!(!deliver_from_ring(Cursor::new(7, 0), h, from));
        // past both → deliver.
        assert!(deliver_from_ring(Cursor::new(8, 0), h, from));
    }
}
