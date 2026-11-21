//! The stop signal the module's shutdown seal gives a run's provider transport.
//!
//! A caller's cancel travels on the run's cancel token, and the run loop answers it by writing
//! a `Cancelled` terminal. The shutdown seal must never produce that terminal: `Cancelled` means
//! a caller decided, and once written it cannot be told apart from one. So the seal does not
//! touch the cancel token at all. It trips THIS signal instead, which only the transport
//! observes. The transport stops the attempt it has in flight, hands that attempt's record back
//! here (outcome `sealed_in_flight`), and then goes quiet without ending its stream, so the run
//! loop sees nothing and decides nothing. The seal then aborts the run task and itself appends
//! the handed-over record followed by the run's `Interrupted` terminal.
//!
//! Every method is cheap and synchronous except [`ShutdownSeal::seal_and_collect`], whose wait
//! is bounded by its caller. A provider that never uses this signal (every in-process provider)
//! never reports an attempt in flight, so sealing it costs nothing.

use std::sync::{Arc, Mutex};

use broca_protocol::{ModelAttempt, StepId};

use crate::cache_warm::{CacheClock, SentRequest};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// One run's shutdown stop signal. Clones share state: the actor keeps one, and the run's
/// provider transport gets another through [`crate::RunControl::shutdown_seal`].
#[derive(Clone, Default)]
pub struct ShutdownSeal {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Tripped once by the seal. Separate from the state below only so the transport can
    /// `select!` on it.
    tripped: CancellationToken,
    state: Mutex<State>,
    /// Woken whenever the in-flight attempt ends or its record is handed over.
    changed: Notify,
    /// The most recent request this run's transport started sending, kept so a cache warm
    /// during the run re-sends exactly those bytes. Separate from `state` so reading it never
    /// waits on the seal.
    last_sent: Mutex<Option<SentRequest>>,
    /// The session's cache clock, which every send of this run moves.
    cache_clock: Option<CacheClock>,
}

#[derive(Default)]
struct State {
    sealed: bool,
    in_flight: bool,
    handed_over: Option<SealedAttempt>,
}

/// The record of the attempt that was in flight when the seal stopped it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedAttempt {
    /// The step the attempt belongs to.
    pub step_id: StepId,
    /// The attempt record, with outcome `sealed_in_flight`.
    pub attempt: ModelAttempt,
}

/// Write-time stage ordered by step first, then tool calls in batch source order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum HookStage {
    PostAssistant,
    PostTool {
        source_index: usize,
        tool_call_id: String,
    },
}

impl ShutdownSeal {
    /// A seal whose run's sends also move the session's cache clock.
    pub fn with_cache_clock(cache_clock: CacheClock) -> Self {
        Self {
            inner: Arc::new(Inner {
                cache_clock: Some(cache_clock),
                ..Inner::default()
            }),
        }
    }

    /// Transport side: an attempt started sending `request`. A provider whose cache lives
    /// `cache_ttl_ms` restarts that lifetime now.
    pub fn note_sent(&self, request: SentRequest, cache_ttl_ms: Option<u64>) {
        if let Some(clock) = &self.inner.cache_clock {
            clock.note_sent(request.sent_at_ms, cache_ttl_ms);
        }
        *self
            .inner
            .last_sent
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(request);
    }

    /// Label the matching send with its response's input occupancy. A warm sent later keeps
    /// its own usage even when the real step's response arrives afterwards.
    pub fn note_usage(&self, step_id: StepId, usage: broca_protocol::Usage) {
        if let (Some(clock), Some(sent)) = (&self.inner.cache_clock, self.last_sent()) {
            if sent.step_id == step_id {
                clock.note_usage(sent.sent_at_ms, usage);
            }
        }
    }

    /// The most recent request this run's transport started sending, if any.
    pub fn last_sent(&self) -> Option<SentRequest> {
        self.inner
            .last_sent
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Transport side: an HTTP attempt is about to start. Returns false once the seal has
    /// tripped, in which case the attempt must not start. Checked under the same lock the seal
    /// trips under, so an attempt either starts before the seal looks (and the seal waits for
    /// its record) or never starts.
    pub fn begin_attempt(&self) -> bool {
        let mut state = self.inner.state.lock().unwrap();
        if state.sealed {
            return false;
        }
        state.in_flight = true;
        true
    }

    /// Transport side: the attempt that [`Self::begin_attempt`] started ended on its own, and
    /// its record went to the run loop the ordinary way. Nothing is handed over for it.
    pub fn end_attempt(&self) {
        self.inner.state.lock().unwrap().in_flight = false;
        self.inner.changed.notify_waiters();
    }

    /// Transport side: resolves once the seal has tripped.
    pub async fn tripped(&self) {
        self.inner.tripped.cancelled().await
    }

    /// Whether the seal has tripped.
    pub fn is_tripped(&self) -> bool {
        self.inner.tripped.is_cancelled()
    }

    /// Transport side: the seal stopped the in-flight attempt; hand over its record. The
    /// attempt is no longer in flight afterwards.
    pub fn hand_over(&self, step_id: StepId, attempt: ModelAttempt) {
        let mut state = self.inner.state.lock().unwrap();
        state.in_flight = false;
        state.handed_over = Some(SealedAttempt { step_id, attempt });
        drop(state);
        self.inner.changed.notify_waiters();
    }
}
