//! An OS-thread witness survives a parked Tokio runtime and bypasses the ordinary logger.
//! Snapshots are observations, not stack traces: a frozen phase can also be synchronous
//! work (including logging) inside that phase. No request payload is retained.

use broca_owner_only::OpenOptions;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use broca_diagnostics::{now_ms, Egress};

const STALE_MS: u64 = 5_000;
const REPEAT_MS: u64 = 10_000;
static ACTIVE: OnceLock<Arc<State>> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Phase {
    Idle = 0,
    AwaitFrame = 1,
    FlowAuthorization = 2,
    BindOwnership = 3,
    RequestPermit = 4,
    EgressSend = 5,
    Control = 6,
    Dispatch = 7,
}
impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::AwaitFrame => "awaiting_frame",
            Self::FlowAuthorization => "flow_authorization",
            Self::BindOwnership => "bind_ownership",
            Self::RequestPermit => "request_permit",
            Self::EgressSend => "egress_send",
            Self::Control => "channel_0_control",
            Self::Dispatch => "dispatch",
        }
    }
    fn decode(code: u64) -> Self {
        match code & 15 {
            1 => Self::AwaitFrame,
            2 => Self::FlowAuthorization,
            3 => Self::BindOwnership,
            4 => Self::RequestPermit,
            5 => Self::EgressSend,
            6 => Self::Control,
            7 => Self::Dispatch,
            _ => Self::Idle,
        }
    }
}

pub(crate) struct State {
    // One packed marker: phase:4, method:8, channel:16, monotonic ms:36. Atomic
    // publication prevents a phase transition from borrowing the prior entry time.
    reader: AtomicU64,
    heartbeat: AtomicU64,
    heartbeat_ticks: AtomicU64,
    pub(crate) egress: Arc<Egress>,
    latest: AtomicU64,
    last_trigger: AtomicU64,
    output_errors: AtomicU64,
    stop: AtomicBool,
    #[cfg(test)]
    pub(crate) gate: Option<Gate>,
}
impl State {
    pub(crate) fn new() -> Self {
        Self {
            reader: AtomicU64::new(0),
            heartbeat: AtomicU64::new(now_ms()),
            heartbeat_ticks: AtomicU64::new(0),
            egress: Arc::default(),
            latest: AtomicU64::new(0),
            output_errors: AtomicU64::new(0),
            last_trigger: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            #[cfg(test)]
            gate: None,
        }
    }
    pub(crate) fn set_reader(&self, phase: Phase, method: u8, channel: u16) {
        self.reader.store(
            (now_ms() << 28) | ((channel as u64) << 12) | ((method as u64) << 4) | phase as u64,
            Ordering::Relaxed,
        );
    }
    fn health(&self) -> serde_json::Value {
        let latest = self.latest.load(Ordering::Relaxed);
        let trigger = self.last_trigger.load(Ordering::Relaxed);
        serde_json::json!({
            "enabled": true,
            "last_trigger_unix_ms": (trigger != 0).then_some(trigger >> 4),
            "last_trigger_kind": (trigger != 0).then(|| kind_name(trigger & 15)),
            "last_snapshot_unix_ms": (latest != 0).then_some(latest >> 4),
            "last_snapshot_kind": (latest != 0).then(|| kind_name(latest & 15)),
            "output_errors": self.output_errors.load(Ordering::Relaxed),
            "logger_contention": "not_instrumented",
        })
    }
}

pub(crate) fn connection_state() -> Arc<State> {
    ACTIVE
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(State::new()))
}

pub fn health_gauges() -> serde_json::Value {
    ACTIVE
        .get()
        .map(|s| s.health())
        .unwrap_or_else(|| serde_json::json!({"enabled": false}))
}

tokio::task_local! { pub(crate) static READER: Arc<State>; }

pub(crate) struct PhaseGuard {
    state: Option<Arc<State>>,
    previous: u64,
}

pub(crate) fn reader_lifetime(state: Arc<State>) -> PhaseGuard {
    PhaseGuard {
        state: Some(state),
        previous: 0,
    }
}
impl Drop for PhaseGuard {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.reader.store(self.previous, Ordering::Relaxed);
        }
    }
}

/// Only the reader task has this context. Spawned request/stream tasks must not
/// overwrite its phase while the reader waits for the next daemon frame.
pub(crate) async fn enter(phase: Phase) -> PhaseGuard {
    let state = READER.try_with(Arc::clone).ok();
    let previous = state.as_ref().map_or(0, |s| {
        let previous = s.reader.load(Ordering::Relaxed);
        let method = ((previous >> 4) & 255) as u8;
        let channel = ((previous >> 12) & 65535) as u16;
        s.set_reader(phase, method, channel);
        previous
    });
    #[cfg(test)]
    if let Some(gate) = state.as_ref().and_then(|s| s.gate.as_ref()) {
        if gate.phase == phase {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }
    PhaseGuard { state, previous }
}

pub(crate) fn frame_context(method: u8, channel: u16) {
    let _ = READER.try_with(|s| s.set_reader(Phase::Dispatch, method, channel));
}

pub(crate) fn method_context(method: u8) {
    let _ = READER.try_with(|s| {
        let old = s.reader.load(Ordering::Relaxed);
        s.reader.store(
            (old & !(255 << 4)) | ((method as u64) << 4),
            Ordering::Relaxed,
        );
    });
}

fn kind_name(code: u64) -> &'static str {
    match code {
        1 => "heartbeat",
        2 => "reader_phase",
        3 => "egress_write",
        4 => "recovery",
        _ => "none",
    }
}

async fn heartbeat(state: Arc<State>, period: Duration) {
    let mut timer = tokio::time::interval(period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        state.heartbeat_ticks.fetch_add(1, Ordering::Relaxed);
        state.heartbeat.store(now_ms(), Ordering::Relaxed);
    }
}

struct Observer {
    metrics: tokio::runtime::RuntimeMetrics,
    file: File,
    stale_ms: u64,
    repeat_ms: u64,
    last_emit: Option<u64>,
    stalled: bool,
}
impl Observer {
    fn tick(&mut self, state: &State) {
        let now = now_ms();
        let reader = state.reader.load(Ordering::Relaxed);
        let phase = Phase::decode(reader);
        let reader_age = now.saturating_sub(reader >> 28);
        let heartbeat_age = now.saturating_sub(state.heartbeat.load(Ordering::Relaxed));
        let operation = state.egress.operation.load(Ordering::Relaxed);
        let write_age = if operation == 0 {
            0
        } else {
            now.saturating_sub(operation >> 1)
        };
        let heartbeat_stale = heartbeat_age > self.stale_ms;
        let reader_stale =
            !matches!(phase, Phase::AwaitFrame | Phase::Idle) && reader_age > self.stale_ms;
        let egress_stale = operation != 0 && write_age > self.stale_ms;
        let code = if heartbeat_stale {
            1
        } else if reader_stale {
            2
        } else if egress_stale {
            3
        } else {
            0
        };
        let emit = if code == 0 {
            self.stalled
        } else {
            self.last_emit
                .is_none_or(|last| now.saturating_sub(last) >= self.repeat_ms)
                || !self.stalled
        };
        self.stalled = code != 0;
        if !emit {
            return;
        }
        self.last_emit = Some(now);
        let code = if code == 0 { 4 } else { code };
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let written = state.egress.written.load(Ordering::Relaxed);
        let sent = state.egress.sent.load(Ordering::Relaxed);
        let mut triggers = Vec::new();
        if heartbeat_stale {
            triggers.push("heartbeat");
        }
        if reader_stale {
            triggers.push("reader_phase");
        }
        if egress_stale {
            triggers.push("egress_write");
        }
        #[cfg(target_has_atomic = "64")]
        let workers: Vec<_> = (0..self.metrics.num_workers())
            .map(|i| {
                serde_json::json!({
                    "worker": i, "busy_ms": self.metrics.worker_total_busy_duration(i).as_millis(),
                    "park_count": self.metrics.worker_park_count(i),
                })
            })
            .collect();
        #[cfg(not(target_has_atomic = "64"))]
        let workers = "unavailable: target lacks 64-bit atomic runtime metrics";
        let snapshot = serde_json::json!({
            "unix_ms": unix_ms, "monotonic_ms": now, "pid": std::process::id(),
            "kind": kind_name(code), "triggers": triggers,
            "heartbeat": {"age_ms": heartbeat_age, "ticks": state.heartbeat_ticks.load(Ordering::Relaxed)},
            "reader": {"phase": phase.name(), "phase_code": phase as u8, "method_code": (reader >> 4) & 255,
                "channel": (reader >> 12) & 65535, "entered_ms": reader >> 28, "age_ms": reader_age},
            "egress": {"sent": sent, "written": written, "occupancy": sent.saturating_sub(written),
                "flushed_frames": state.egress.flushed.load(Ordering::Relaxed),
                "operation": if operation == 0 {"idle"} else if operation & 1 == 0 {"write"} else {"flush"},
                "started_ms": (operation != 0).then_some(operation >> 1), "age_ms": write_age},
            "runtime": {"num_workers": self.metrics.num_workers(), "num_alive_tasks": self.metrics.num_alive_tasks(),
                "global_queue_depth": self.metrics.global_queue_depth(), "workers": workers,
                "blocking_pool": "unavailable: requires tokio_unstable"},
            "broca_jobs": broca_diagnostics::broca_jobs(now),
            "logger_contention": "not_instrumented",
            "output_errors": state.output_errors.load(Ordering::Relaxed),
        });
        // A single independent file handle is the entire output path. In particular, even
        // an output error must not enter the global tracing subscriber and its file mutex.
        let result = serde_json::to_vec(&snapshot)
            .map_err(std::io::Error::other)
            .and_then(|mut line| {
                line.push(b'\n');
                self.file.write_all(&line)
            });
        if result.is_err() {
            state.output_errors.fetch_add(1, Ordering::Relaxed);
        }
        state.latest.store((unix_ms << 4) | code, Ordering::Relaxed);
        if code != 4 {
            state
                .last_trigger
                .store((unix_ms << 4) | code, Ordering::Relaxed);
        }
    }
}

pub struct Watchdog {
    state: Arc<State>,
    thread: Option<std::thread::JoinHandle<()>>,
    heartbeat: tokio::task::JoinHandle<()>,
}
impl Drop for Watchdog {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Relaxed);
        self.heartbeat.abort();
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            // Do not join: a filesystem stalled in write_all must not stall shutdown too.
        }
    }
}

pub fn start(root: &Path) -> std::io::Result<Watchdog> {
    let logs = root.join("logs");
    broca_owner_only::create_dir_all(&logs)?;
    let date: chrono::DateTime<chrono::Utc> = SystemTime::now().into();
    let path = logs.join(format!("stall-{}.jsonl", date.format("%Y-%m-%d")));
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    let state = Arc::new(State::new());
    let watchdog = launch(
        state.clone(),
        file,
        STALE_MS,
        REPEAT_MS,
        Duration::from_secs(1),
        Duration::from_millis(250),
    )?;
    ACTIVE
        .set(state)
        .map_err(|_| std::io::Error::other("stall watchdog already started"))?;
    Ok(watchdog)
}

fn launch(
    state: Arc<State>,
    file: File,
    stale_ms: u64,
    repeat_ms: u64,
    poll: Duration,
    beat: Duration,
) -> std::io::Result<Watchdog> {
    let handle = tokio::runtime::Handle::current();
    let mut observer = Observer {
        metrics: handle.metrics(),
        file,
        stale_ms,
        repeat_ms,
        last_emit: None,
        stalled: false,
    };
    let thread_state = state.clone();
    let thread = std::thread::Builder::new()
        .name("broca-stall-watchdog".into())
        .spawn(move || {
            while !thread_state.stop.load(Ordering::Relaxed) {
                std::thread::park_timeout(poll);
                if !thread_state.stop.load(Ordering::Relaxed) {
                    observer.tick(&thread_state);
                }
            }
        })?;
    let heartbeat = handle.spawn(heartbeat(state.clone(), beat));
    Ok(Watchdog {
        state,
        thread: Some(thread),
        heartbeat,
    })
}

#[cfg(test)]
pub(crate) struct Gate {
    pub(crate) phase: Phase,
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, observe, BroadParent,
    };
    #[test]
    fn windows_stall_log_and_directory_are_private() {
        let fixture = BroadParent::new("stall");
        let seen = std::rc::Rc::new(std::cell::Cell::new(0));
        let output = seen.clone();
        let watchdog = observe(
            move |path| {
                assert_private(path);
                output.set(output.get() + 1);
            },
            || start(&fixture.path).unwrap(),
        );
        assert_eq!(seen.get(), 1);
        assert_private(&fixture.path.join("logs"));
        drop(watchdog);
    }
}
