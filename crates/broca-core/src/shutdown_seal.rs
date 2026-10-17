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
use std::time::Duration;

use broca_protocol::{ModelAttempt, StepId};
use broca_wal::WalRecord;

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
    hooks: std::collections::BTreeMap<HookStage, Vec<WalRecord>>,
    sealed_hooks: Vec<Vec<WalRecord>>,
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
    /// Deposit or replace the whole-chain fallback under the seal's linearization lock.
    /// False means the loop has lost to shutdown and must park without further writes.
    pub fn deposit_hook(&self, stage: HookStage, record: WalRecord) -> bool {
        self.deposit_hook_records(stage, vec![record])
    }

    /// When broca stops runaway tool arguments, save the step and the call's error result
    /// together. Shutdown must not preserve the call while losing the reason it cannot run.
    pub fn deposit_hook_records(&self, stage: HookStage, records: Vec<WalRecord>) -> bool {
        let mut state = self.inner.state.lock().unwrap();
        if state.sealed {
            return false;
        }
        state.hooks.insert(stage, records);
        true
    }

    /// Remove the slot and enqueue the final record at one linearization point. No await
    /// can let shutdown take the fallback between removal and enqueue.
    pub fn record_hook(
        &self,
        stage: &HookStage,
        writer: &crate::WalHandle,
        record: WalRecord,
    ) -> bool {
        self.record_hook_records(stage, writer, vec![record])
    }

    /// Enqueue the assistant step, batch start and rejected-call results under the seal
    /// lock, so shutdown cannot take only the step before its errors reach the writer.
    pub fn record_hook_records(
        &self,
        stage: &HookStage,
        writer: &crate::WalHandle,
        records: Vec<WalRecord>,
    ) -> bool {
        let mut state = self.inner.state.lock().unwrap();
        if state.sealed {
            return false;
        }
        state.hooks.remove(stage);
        let _ = writer.record_group(records);
        true
    }

    /// Actor side: after seal_and_collect and before interrupted close-out, persist the
    /// slots taken by the seal. This barrier must complete before replay reads the WAL.
    pub async fn record_hook_fallbacks(
        &self,
        writer: &crate::WalHandle,
    ) -> Result<(), crate::WalWriteError> {
        {
            let mut state = self.inner.state.lock().unwrap();
            assert!(
                state.sealed,
                "hook fallbacks may only be taken by a sealed actor"
            );
            for records in std::mem::take(&mut state.sealed_hooks) {
                let _ = writer.record_group(records);
            }
        }
        writer.sync_for_seal().await?;
        Ok(())
    }

    pub fn new() -> Self {
        Self::default()
    }

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

    /// Seal side: trip the signal, then wait at most `bound` for the in-flight attempt's
    /// record. Returns `None` at once when no attempt was in flight, and `None` after `bound`
    /// when the transport never handed one over (the record is then lost; the seal goes on).
    ///
    /// THE CALLER MUST THEN ABORT THE RUN TASK. Once tripped, the transport keeps its stream open
    /// and silent on purpose, so the run loop can't pick a terminal of its own; nothing else ever
    /// wakes it. Tripping this without aborting the run leaves that run parked forever. The only
    /// caller today is the actor's shutdown seal, which aborts immediately after this returns.
    pub async fn seal_and_collect(&self, bound: Duration) -> Option<SealedAttempt> {
        {
            let mut state = self.inner.state.lock().unwrap();
            state.sealed = true;
            state.sealed_hooks = std::mem::take(&mut state.hooks).into_values().collect();
        }
        self.inner.tripped.cancel();
        let deadline = tokio::time::Instant::now() + bound;
        loop {
            // Register for the wakeup BEFORE reading the state, so a hand-over that lands
            // between the read and the wait is not missed.
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.inner.state.lock().unwrap();
                if let Some(sealed) = state.handed_over.take() {
                    return Some(sealed);
                }
                if !state.in_flight {
                    return None;
                }
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::AttemptOutcome;

    fn attempt() -> ModelAttempt {
        ModelAttempt {
            content_progress: Default::default(),
            attempt: 0,
            request_body_len: 1,
            request_body_sha256: "aa".into(),
            http_status: Some(200),
            content_type: None,
            response_body_len: 0,
            response_body_sha256: "bb".into(),
            response_body_cap: None,
            outcome: AttemptOutcome::SealedInFlight,
            ts_ms: 0,
            sent_at_ms: None,
            transport: None,
            websocket_request: None,
            websocket_close_code: None,
            input_transformations: None,
            input_transformations_unmeasured: false,
            credential_refresh: None,
            served_account_ref: None,
            account_observation: Default::default(),
        }
    }

    #[tokio::test]
    async fn nothing_in_flight_returns_at_once_and_blocks_later_attempts() {
        let seal = ShutdownSeal::new();
        let started = tokio::time::Instant::now();
        assert_eq!(seal.seal_and_collect(Duration::from_secs(5)).await, None);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!seal.begin_attempt(), "no attempt may start after the seal");
    }

    #[tokio::test]
    async fn an_in_flight_attempt_is_handed_over() {
        let seal = ShutdownSeal::new();
        assert!(seal.begin_attempt());
        let transport = seal.clone();
        tokio::spawn(async move {
            transport.tripped().await;
            transport.hand_over(StepId(3), attempt());
        });
        let sealed = seal.seal_and_collect(Duration::from_secs(5)).await;
        assert_eq!(
            sealed,
            Some(SealedAttempt {
                step_id: StepId(3),
                attempt: attempt()
            })
        );
    }

    #[tokio::test]
    async fn an_attempt_that_ends_on_its_own_hands_nothing_over() {
        let seal = ShutdownSeal::new();
        assert!(seal.begin_attempt());
        let transport = seal.clone();
        tokio::spawn(async move {
            transport.tripped().await;
            transport.end_attempt();
        });
        assert_eq!(seal.seal_and_collect(Duration::from_secs(5)).await, None);
    }

    #[tokio::test]
    async fn a_transport_that_ignores_the_seal_costs_at_most_the_bound() {
        let seal = ShutdownSeal::new();
        assert!(seal.begin_attempt());
        let started = tokio::time::Instant::now();
        assert_eq!(
            seal.seal_and_collect(Duration::from_millis(300)).await,
            None
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(3),
            "waited {elapsed:?} for a 300ms bound"
        );
    }
}
