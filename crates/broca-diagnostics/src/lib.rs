//! Observes submission, start and completion of broca's blocking jobs. Dependency jobs
//! (including DNS) are invisible here. A bounded table of atomic job timestamps lets the
//! watchdog find old queued/running jobs without waiting on the code it diagnoses.
#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Milliseconds on a process-local monotonic clock. Always nonzero so atomic timers
/// can reserve zero for an inactive operation.
pub fn now_ms() -> u64 {
    ORIGIN.elapsed().as_millis() as u64 + 1
}

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum JobKind {
    StoreRead,
    StoreWrite,
    WalRead,
    WalWriter,
    Projection,
    Render,
    Dump,
    Retention,
    Recovery,
    Archive,
}

const NAMES: [&str; 10] = [
    "store_read",
    "store_write",
    "wal_read",
    "wal_writer",
    "projection",
    "render",
    "dump",
    "retention",
    "recovery",
    "archive",
];
struct Counters {
    submitted: AtomicU64,
    started: AtomicU64,
    completed: AtomicU64,
    untracked: AtomicU64,
    cancelled: AtomicU64,
}
impl Counters {
    const fn new() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            started: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            untracked: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
        }
    }
}
static COUNTERS: [Counters; 10] = [const { Counters::new() }; 10];
// A slot packs kind (low 4 bits), running (bit 4), and entry time. One atomic read
// cannot confuse a reused slot's kind with the previous job's timestamp.
static JOBS: [AtomicU64; 4096] = [const { AtomicU64::new(0) }; 4096];
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Job {
    kind: JobKind,
    slot: Option<usize>,
    started: bool,
}
impl Job {
    fn submit(kind: JobKind) -> Self {
        COUNTERS[kind as usize]
            .submitted
            .fetch_add(1, Ordering::Relaxed);
        let entry = (now_ms() << 5) | kind as u64;
        let first = NEXT.fetch_add(1, Ordering::Relaxed) as usize % JOBS.len();
        let slot = (0..JOBS.len())
            .map(|n| (first + n) % JOBS.len())
            .find(|&i| {
                JOBS[i]
                    .compare_exchange(0, entry, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            });
        if slot.is_none() {
            COUNTERS[kind as usize]
                .untracked
                .fetch_add(1, Ordering::Relaxed);
        }
        Self {
            kind,
            slot,
            started: false,
        }
    }
    fn start(&mut self) {
        self.started = true;
        COUNTERS[self.kind as usize]
            .started
            .fetch_add(1, Ordering::Relaxed);
        if let Some(i) = self.slot {
            JOBS[i].store((now_ms() << 5) | 16 | self.kind as u64, Ordering::Relaxed);
        }
    }
}
impl Drop for Job {
    fn drop(&mut self) {
        if self.started {
            COUNTERS[self.kind as usize]
                .completed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            COUNTERS[self.kind as usize]
                .cancelled
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Some(i) = self.slot {
            JOBS[i].store(0, Ordering::Relaxed);
        }
    }
}

pub fn spawn_blocking<F, R>(kind: JobKind, f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    spawn_blocking_on(&tokio::runtime::Handle::current(), kind, f)
}

pub fn spawn_blocking_on<F, R>(
    handle: &tokio::runtime::Handle,
    kind: JobKind,
    f: F,
) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let mut job = Job::submit(kind);
    handle.spawn_blocking(move || {
        job.start();
        let result = f();
        drop(job);
        result
    })
}

#[derive(serde::Serialize)]
pub struct JobReading {
    pub kind: &'static str,
    pub submitted: u64,
    pub started: u64,
    pub completed: u64,
    pub cancelled_before_start: u64,
    pub oldest_queued_ms: Option<u64>,
    pub oldest_running_ms: Option<u64>,
    pub age_tracking_overflow: u64,
}

pub fn broca_jobs(now: u64) -> Vec<JobReading> {
    let mut readings: Vec<_> = NAMES
        .iter()
        .enumerate()
        .map(|(i, &kind)| {
            let c = &COUNTERS[i];
            JobReading {
                kind,
                submitted: c.submitted.load(Ordering::Relaxed),
                started: c.started.load(Ordering::Relaxed),
                completed: c.completed.load(Ordering::Relaxed),
                cancelled_before_start: c.cancelled.load(Ordering::Relaxed),
                oldest_queued_ms: None,
                oldest_running_ms: None,
                age_tracking_overflow: c.untracked.load(Ordering::Relaxed),
            }
        })
        .collect();
    for slot in &JOBS {
        let value = slot.load(Ordering::Relaxed);
        if value == 0 {
            continue;
        }
        let r = &mut readings[(value & 15) as usize];
        let age = now.saturating_sub(value >> 5);
        let oldest = if value & 16 == 0 {
            &mut r.oldest_queued_ms
        } else {
            &mut r.oldest_running_ms
        };
        *oldest = Some(oldest.unwrap_or(0).max(age));
    }
    readings
}

#[derive(Default)]
pub struct Egress {
    pub sent: AtomicU64,
    pub written: AtomicU64,
    pub flushed: AtomicU64,
    pub operation: AtomicU64,
}

/// Admission is counted before publishing the frame, so the writer can never complete
/// a frame before it has been counted. Reservations waiting for space are not occupancy.
pub struct Sender<T> {
    tx: tokio::sync::mpsc::Sender<T>,
    pub state: Arc<Egress>,
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            state: self.state.clone(),
        }
    }
}
impl<T> From<tokio::sync::mpsc::Sender<T>> for Sender<T> {
    fn from(tx: tokio::sync::mpsc::Sender<T>) -> Self {
        Self {
            tx,
            state: Arc::default(),
        }
    }
}
impl<T> Sender<T> {
    pub fn new(tx: tokio::sync::mpsc::Sender<T>, state: Arc<Egress>) -> Self {
        Self { tx, state }
    }
    pub async fn send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        let permit = match self.tx.reserve().await {
            Ok(permit) => permit,
            Err(_) => return Err(tokio::sync::mpsc::error::SendError(value)),
        };
        self.state.sent.fetch_add(1, Ordering::Relaxed);
        permit.send(value);
        Ok(())
    }
}

pub struct Receiver<T> {
    rx: tokio::sync::mpsc::Receiver<T>,
    pub state: Arc<Egress>,
}
impl<T> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.rx.recv().await
    }
    pub fn try_recv(&mut self) -> Result<T, tokio::sync::mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

pub fn channel<T>(capacity: usize, state: Arc<Egress>) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    (Sender::new(tx, state.clone()), Receiver { rx, state })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn broca_jobs_distinguish_queued_running_and_completed() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let _entered = rt.enter();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let running = spawn_blocking(JobKind::Archive, move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let queued = spawn_blocking(JobKind::Archive, || 42);
        let reading = broca_jobs(now_ms())
            .into_iter()
            .find(|r| r.kind == "archive")
            .unwrap();
        release_tx.send(()).unwrap();
        rt.block_on(async {
            running.await.unwrap();
            assert_eq!(queued.await.unwrap(), 42);
        });
        assert_eq!(
            (reading.submitted, reading.started, reading.completed),
            (2, 1, 0)
        );
        assert!(reading.oldest_queued_ms.is_some());
        assert!(reading.oldest_running_ms.is_some());
        let reading = broca_jobs(now_ms())
            .into_iter()
            .find(|r| r.kind == "archive")
            .unwrap();
        assert_eq!(
            (reading.submitted, reading.started, reading.completed),
            (2, 2, 2)
        );
        assert!(reading.oldest_queued_ms.is_none());
        assert!(reading.oldest_running_ms.is_none());
    }

    #[tokio::test]
    async fn admitted_frames_are_counted_before_the_writer_can_receive() {
        let state = Arc::new(Egress::default());
        let (tx, mut rx) = channel(1, state.clone());
        tx.send(42).await.unwrap();
        assert_eq!(rx.recv().await, Some(42));
        assert_eq!(state.sent.load(Ordering::Relaxed), 1);
        drop(rx);
        assert!(tx.send(43).await.is_err());
        assert_eq!(state.sent.load(Ordering::Relaxed), 1);
    }
}
