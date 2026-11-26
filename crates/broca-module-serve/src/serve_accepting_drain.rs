//! The registered connection stays open through the accepting phase and the readiness exchange.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use subc_protocol::session::{AcceptingDrainCommand, ModuleDrainReady, ACCEPTING_DRAIN_CAPABILITY};
use subc_protocol::ModuleHelloAckBody;
use tokio::time::Instant;

use super::{
    control_flags, now_epoch_ms, send, EgressSender, ErrorBody, Frame, FrameType, LineageRegistry,
    ModuleControlCommand,
};

// This is a one-shot request. HELLO uses 1, while the settings gate and scope describer
// allocate from higher, separate ranges.
const READY_CORR: u64 = 2;
const CHECK_INTERVAL: Duration = Duration::from_millis(10);
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(super) struct Drain {
    supported: bool,
    phase: Phase,
    sessionless: Arc<AtomicUsize>,
}

#[derive(Default)]
enum Phase {
    #[default]
    Serving,
    Accepting {
        started: Instant,
        deadline: Instant,
        check_at: Instant,
        ver: u8,
    },
    AwaitingAck {
        until: Instant,
        deadline: Instant,
    },
    Stopping,
}

/// Created at dispatch rather than when the task first polls, so a received request
/// cannot be missed by a readiness check on the same connection.
pub(super) struct InFlight(Arc<AtomicUsize>);

impl InFlight {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(count))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drain {
    pub(super) fn negotiated(ack: &ModuleHelloAckBody) -> Self {
        Self {
            supported: ack
                .subc_capabilities
                .iter()
                .any(|cap| cap == ACCEPTING_DRAIN_CAPABILITY),
            ..Default::default()
        }
    }

    pub(super) fn sessionless_call(&self) -> InFlight {
        InFlight::new(&self.sessionless)
    }

    pub(super) fn on_push(&mut self, frame: &Frame, registry: &LineageRegistry) {
        let accepting = if self.supported {
            serde_json::from_slice(&frame.body).ok()
        } else {
            None
        };
        if let Some(AcceptingDrainCommand::Draining {
            reason,
            deadline_ms,
            accepting: true,
        }) = accepting
        {
            if matches!(self.phase, Phase::Serving) {
                let started = Instant::now();
                let remaining = deadline_ms.saturating_sub(now_epoch_ms().max(0) as u64);
                let deadline = started
                    .checked_add(Duration::from_millis(remaining))
                    .unwrap_or(started);
                registry.begin_accepting_drain();
                let runs_in_flight = registry.slot_counts().runs_in_flight;
                tracing::info!(target: "lifecycle", ?reason, deadline_ms, runs_in_flight,
                        "accepting drain started; deferring new runs until restart");
                self.phase = Phase::Accepting {
                    started,
                    deadline,
                    check_at: started,
                    ver: frame.header.ver,
                };
            }
            return;
        }
        match serde_json::from_slice::<ModuleControlCommand>(&frame.body) {
            Ok(ModuleControlCommand::Draining {
                reason,
                deadline_ms,
            }) => {
                // Keep serving health probes and current tool replies. Starting the seal
                // timer here would spend the post-GOODBYE grace inside the drain window.
                self.stop(registry);
                tracing::info!(target: "lifecycle", ?reason, deadline_ms,
                    "module draining; admission stopped, waiting for step boundaries");
            }
            result => tracing::warn!(target: "wire", error = ?result.err(),
                "ignored unknown or malformed channel-0 control push"),
        }
    }

    pub(super) fn next_check(&self) -> impl std::future::Future<Output = ()> + use<> {
        let wake = self.wake_at();
        async move {
            match wake {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        }
    }

    fn wake_at(&self) -> Option<Instant> {
        match self.phase {
            Phase::Accepting { check_at, .. } => Some(check_at),
            Phase::AwaitingAck { until, .. } => Some(until),
            _ => None,
        }
    }

    pub(super) async fn check(
        &mut self,
        registry: &LineageRegistry,
        writer: &EgressSender<Frame>,
        llm_tools: Option<&crate::llm_tools::LlmTools>,
    ) {
        match self.phase {
            Phase::Accepting {
                started,
                deadline,
                ver,
                ..
            } => {
                let now = Instant::now();
                if now >= deadline {
                    self.deadline_won(registry);
                    return;
                }
                let counts = registry.slot_counts();
                let sessionless = self.sessionless.load(Ordering::Relaxed);
                let completions_in_flight =
                    llm_tools.map_or(0, |tools| tools.completions_in_flight());
                if counts.runs_in_flight != 0
                    || counts.opening != 0
                    || counts.batches_in_flight != 0
                    || sessionless != 0
                    || completions_in_flight != 0
                {
                    self.phase = Phase::Accepting {
                        started,
                        deadline,
                        check_at: (now + CHECK_INTERVAL).min(deadline),
                        ver,
                    };
                    return;
                }
                let until = (now + ACK_TIMEOUT).min(deadline);
                self.phase = Phase::AwaitingAck { until, deadline };
                let body = serde_json::to_vec(&ModuleDrainReady::Ready {})
                    .expect("drain readiness encodes");
                let frame = Frame::build_with_version(
                    ver,
                    FrameType::Request,
                    control_flags(),
                    0,
                    0,
                    READY_CORR,
                    body,
                )
                .expect("valid drain readiness request");
                match tokio::time::timeout_at(until, send(writer, frame)).await {
                    Ok(Ok(())) => tracing::info!(target: "lifecycle",
                        elapsed_ms = started.elapsed().as_millis(),
                        runs_in_flight = counts.runs_in_flight, sessionless,
                        system_one_batches = counts.batches_in_flight, completions_in_flight,
                        "module.drain_ready sent"),
                    Ok(Err(error)) => {
                        tracing::warn!(target: "lifecycle", %error, "module.drain_ready could not be sent");
                        self.stop(registry);
                    }
                    Err(_) => self.ack_timed_out(deadline, registry),
                }
            }
            Phase::AwaitingAck { deadline, .. } => self.ack_timed_out(deadline, registry),
            _ => {}
        }
    }

    pub(super) fn on_reply(&mut self, frame: &Frame, registry: &LineageRegistry) -> bool {
        let Phase::AwaitingAck { deadline, .. } = self.phase else {
            return false;
        };
        if frame.header.channel != 0
            || frame.header.epoch != 0
            || frame.header.corr != READY_CORR
            || !matches!(frame.header.ty, FrameType::Response | FrameType::Error)
        {
            return false;
        }
        if Instant::now() >= deadline {
            self.deadline_won(registry);
        } else {
            match frame.header.ty {
                FrameType::Response
                    if serde_json::from_slice::<ModuleDrainReady>(&frame.body).is_ok() => {}
                FrameType::Error => {
                    let refusal = serde_json::from_slice::<ErrorBody>(&frame.body);
                    tracing::warn!(target: "lifecycle", ?refusal, "module.drain_ready refused");
                }
                _ => {
                    tracing::warn!(target: "lifecycle", "invalid module.drain_ready acknowledgement")
                }
            }
            self.stop(registry);
        }
        true
    }

    fn ack_timed_out(&mut self, deadline: Instant, registry: &LineageRegistry) {
        tracing::warn!(target: "lifecycle", "module.drain_ready timed out");
        if Instant::now() >= deadline {
            self.deadline_won(registry);
        } else {
            self.stop(registry);
        }
    }

    fn deadline_won(&mut self, registry: &LineageRegistry) {
        tracing::info!(target: "lifecycle", "accepting drain deadline reached; stopping admission");
        self.stop(registry);
    }

    fn stop(&mut self, registry: &LineageRegistry) {
        self.phase = Phase::Stopping;
        registry.begin_stop();
    }
}
