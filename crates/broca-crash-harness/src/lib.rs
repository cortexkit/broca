//! The shared crash-cut harness.
//!
//! Every durable record the runner writes comes with the same exit test: kill the runner at
//! each record boundary, resume, and check that the resume invariants still hold. This crate
//! is that test, written once, so a new record kind is covered by adding a scenario rather than
//! by writing another harness beside the real resume path. It never reconstructs a resume: every
//! resume goes through the real run loop, `broca_core::run_loop_with_control`, on a real file
//! log under a fresh state root.
//!
//! # Shape
//!
//! A [`Scenario`] is a scripted run. [`RunnerHandle::new`] runs it once uninterrupted as the
//! reference, recording its log, the request bytes of every step and where every durability
//! barrier fell. Then:
//!
//! - [`RunnerHandle::kill_at`] makes ONE kill on ONE fresh state root and reports how it killed:
//!   - [`KillPoint::RecordBoundary`] with [`KillTrigger::Truncate`] copies the reference log cut
//!     just after a given record (optionally with part of the next frame, a torn append). A
//!     power loss leaves exactly such a prefix, and every boundary is reachable this way, both
//!     just after a sync and inside a batch of appends that had not been synced.
//!   - [`KillPoint::InFlight`] with [`KillTrigger::ProcessKill`] spawns the runner as its own
//!     process, waits until it is inside a named tool dispatch or provider call, and kills it
//!     with SIGKILL. That is the one way to catch state that lives only in memory, which a
//!     truncated copy of the log cannot.
//! - [`RunnerHandle::resume`] resumes the killed state through the real run loop with a fresh
//!   provider and tool plane, and [`RunnerHandle::check`] asserts the invariants of
//!   [`invariants::check`] against what the kill left.
//! - [`RunnerHandle::every_record_boundary`] and [`RunnerHandle::every_boundary_inside_resume`]
//!   do all three for every boundary.
//! - A [`Suite`] collects the kills of one suite and fails unless at least one of them was a
//!   real process kill.
//!
//! The kill API follows the shape of commons' `cortexkit-role-harness`, which the `llm-runner`
//! conformance suite uses to kill a runner: `kill_at(handle, point, trigger)`, one kill per
//! fresh state root, each kill reporting its method. Keeping that shape lets the suite drive
//! this harness later without reshaping it.
//!
//! # Process kills
//!
//! A test binary that uses [`KillTrigger::ProcessKill`] must define the child entry with
//! [`crash_harness_child!`], naming a function that finds a scenario by name. The harness
//! re-runs the current test binary with only that test selected; the entry is a no-op in an
//! ordinary test run.

pub mod invariants;
pub mod power_loss;
pub mod scenario;
pub mod wal;

#[cfg(test)]
mod compaction;

#[cfg(test)]
crash_harness_child!(compaction::find);

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use broca_core::FileWalStore;
use broca_protocol::caller::RunFinishReason;
use broca_wal::WalRecord;
use tokio_util::sync::CancellationToken;

pub use scenario::{
    check_tool_pairing, drive, HarnessPlane, InFlight, Render, RunOutcome, Scenario,
    ScriptedProvider,
};
pub use wal::{wal_path, LogImage, ObservedWalStore};

/// The name of the test [`crash_harness_child!`] defines. A process kill re-runs the current
/// test binary with only this test selected, and that test then runs the scenario as the
/// runner to be killed.
pub const CHILD_TEST_NAME: &str = "crash_harness_child";
const ENV_SCENARIO: &str = "BROCA_CRASH_HARNESS_SCENARIO";
const ENV_ROOT: &str = "BROCA_CRASH_HARNESS_ROOT";
const ENV_POINT: &str = "BROCA_CRASH_HARNESS_POINT";
/// How long a spawned runner may take to reach its in-flight point.
const REACH_TIMEOUT: Duration = Duration::from_secs(60);

/// Where a kill lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KillPoint {
    /// Exactly the first `records` records are durable. With `torn`, part of the next frame is
    /// on disk as well, as an append the kill cut short.
    RecordBoundary { records: usize, torn: bool },
    /// The runner is inside this call when it dies.
    InFlight(InFlight),
}

/// How a kill is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillTrigger {
    /// Leave the durable state a kill at the point would leave by writing a cut copy of the
    /// reference log. Valid for [`KillPoint::RecordBoundary`].
    Truncate,
    /// Spawn the runner as a separate process and kill it with SIGKILL once it reaches the
    /// point. Valid for [`KillPoint::InFlight`].
    ProcessKill,
}

/// The method a kill actually used, as each kill reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillMethod {
    Truncation,
    /// A real process kill: SIGKILL on Unix (the platform's forced termination elsewhere).
    Sigkill,
}

/// One kill: where it landed, how it was made, and the durable state it left.
#[derive(Debug, Clone)]
pub struct KillReport {
    pub scenario: &'static str,
    pub point: KillPoint,
    pub method: KillMethod,
    /// For a record boundary: whether a durability barrier completed exactly there (`true`), or
    /// the boundary falls inside appends not yet synced (`false`). `None` for an in-flight kill.
    pub after_sync: Option<bool>,
    /// For a kill inside a resume: how many records the log held when that resume began.
    pub inside_resume_from: Option<usize>,
    /// The fresh state root this kill, and only this kill, used.
    pub state_root: PathBuf,
    /// The log exactly as the kill left it.
    pub durable: LogImage,
}

impl KillReport {
    /// A label naming this kill in failure messages.
    pub fn label(&self) -> String {
        let side = match self.after_sync {
            Some(true) => " after a sync",
            Some(false) => " inside an unsynced batch",
            None => "",
        };
        let nested = self
            .inside_resume_from
            .map(|from| format!(" inside the resume from {from} records"))
            .unwrap_or_default();
        format!(
            "{}: {:?} by {:?}{side}{nested} ({})",
            self.scenario,
            self.point,
            self.method,
            self.state_root.display()
        )
    }
}

/// A resume after a kill, and what it left.
pub struct Resumed {
    pub reason: RunFinishReason,
    pub provider: ScriptedProvider,
    pub plane: HarnessPlane,
    pub log: LogImage,
    pub outcome: RunOutcome,
}

/// The uninterrupted run every kill is cut from and compared with.
pub struct Reference {
    pub reason: RunFinishReason,
    pub sent: BTreeMap<usize, Vec<u8>>,
    pub log: LogImage,
    /// The sequence numbers the log had reached at each completed durability barrier.
    pub synced: BTreeSet<u64>,
}

/// When the module's stop request reaches a run (see `broca_core::RunControl::stop_requested`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopAt {
    /// While the run's first tool call is running: that call finishes, and the run starts
    /// nothing after it.
    FirstToolCall,
}

/// A scenario and its reference run: what [`RunnerHandle::kill_at`] kills.
pub struct RunnerHandle {
    pub scenario: Scenario,
    pub reference: Reference,
    /// Set for a run the module asked to stop; see [`RunnerHandle::stopping`].
    stop: Option<StopAt>,
    base: PathBuf,
    next_root: AtomicUsize,
}

impl RunnerHandle {
    /// Run `scenario` once uninterrupted under a new directory for this handle, observing its
    /// durability barriers, and check that it ends as the scenario expects.
    pub async fn new(scenario: Scenario) -> Self {
        Self::build(scenario, None).await
    }

    /// As [`RunnerHandle::new`], for a run the module asks to stop at `stop`. The reference run
    /// receives the request at that point and ends there, as the scenario's expected reason
    /// says. Every resume runs with the request already made, as in a process that is still
    /// stopping: each is checked against the reference, and only a resume that also starts
    /// nothing new can end as the reference did. Record-boundary kills only: a spawned runner
    /// is rebuilt from the scenario's name and knows nothing of the stop.
    pub async fn stopping(scenario: Scenario, stop: StopAt) -> Self {
        Self::build(scenario, Some(stop)).await
    }

    /// The run's controls: the scenario's own, plus the stop request for a stopping run.
    /// `reference` selects the uninterrupted run, whose request arrives on the way; every
    /// resume gets one already made.
    fn control(&self, reference: bool) -> (broca_core::RunControl, Option<CancellationToken>) {
        stop_control(&self.scenario, self.stop, reference)
    }

    async fn build(scenario: Scenario, stop: Option<StopAt>) -> Self {
        static HANDLES: AtomicUsize = AtomicUsize::new(0);
        let base = broca_test_support::scratch_root().join(format!(
            "crash-harness-{}-{}",
            scenario.name,
            HANDLES.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&base);
        let root = base.join("reference");
        std::fs::create_dir_all(&root).expect("create the reference state root");
        let store = ObservedWalStore::new(root.join("wal"));
        let provider = ScriptedProvider::for_scenario(&scenario);
        let (control, trip) = stop_control(&scenario, stop, true);
        let plane = match trip {
            Some(token) => HarnessPlane::for_scenario(&scenario).stopping_on_dispatch(token),
            None => HarnessPlane::for_scenario(&scenario),
        };
        let outcome = drive(
            (scenario.request)(),
            control,
            &root,
            &store,
            &provider,
            &plane,
        )
        .await;
        assert_eq!(
            outcome.reason, scenario.expected,
            "{}: the uninterrupted run must end as expected",
            scenario.name
        );
        assert!(
            provider.rejected().is_empty(),
            "{}: the provider refused the uninterrupted run: {:?}",
            scenario.name,
            provider.rejected()
        );
        let log = LogImage::read(&wal_path(&root, &(scenario.request)().session));
        assert_eq!(
            log.torn_tail, 0,
            "{}: the reference log is torn",
            scenario.name
        );
        Self {
            scenario,
            reference: Reference {
                reason: outcome.reason,
                sent: provider.sent(),
                log,
                synced: store.synced(),
            },
            stop,
            base,
            next_root: AtomicUsize::new(0),
        }
    }

    fn fresh_root(&self) -> PathBuf {
        let root = self.base.join(format!(
            "kill-{}",
            self.next_root.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create a fresh state root");
        root
    }

    fn session_wal(&self, root: &Path) -> PathBuf {
        wal_path(root, &(self.scenario.request)().session)
    }

    /// Every record boundary of the reference log, from before the first record through after
    /// the last, each optionally also as a torn append of the frame after it.
    pub fn record_boundaries(&self, torn: bool) -> Vec<KillPoint> {
        let total = self.reference.log.record_count();
        let mut points = Vec::new();
        for records in 0..=total {
            points.push(KillPoint::RecordBoundary {
                records,
                torn: false,
            });
            if torn && records < total {
                points.push(KillPoint::RecordBoundary {
                    records,
                    torn: true,
                });
            }
        }
        points
    }

    /// Make one kill at `point` by `trigger` on a fresh state root.
    pub async fn kill_at(&self, point: KillPoint, trigger: KillTrigger) -> KillReport {
        match (&point, trigger) {
            (KillPoint::RecordBoundary { records, torn }, KillTrigger::Truncate) => {
                let root = self.fresh_root();
                let cut = cut_log(&self.reference.log, *records, *torn);
                write_log(&self.session_wal(&root), &cut);
                let after_sync = Some(if *records == 0 {
                    true
                } else {
                    let seq = match &self.reference.log.record_frame(records - 1).unwrap().body {
                        wal::FrameBody::Record(framed) => framed.seq,
                        wal::FrameBody::Lineage(_) => unreachable!("a record frame"),
                    };
                    self.reference.synced.contains(&seq)
                });
                KillReport {
                    scenario: self.scenario.name,
                    point,
                    method: KillMethod::Truncation,
                    after_sync,
                    inside_resume_from: None,
                    state_root: root,
                    durable: LogImage::parse(cut),
                }
            }
            (KillPoint::InFlight(in_flight), KillTrigger::ProcessKill) => {
                assert!(
                    self.stop.is_none(),
                    "{}: a spawned runner cannot be asked to stop",
                    self.scenario.name
                );
                let root = self.fresh_root();
                kill_spawned_runner(self.scenario.name, &root, in_flight);
                let durable = LogImage::read(&self.session_wal(&root));
                KillReport {
                    scenario: self.scenario.name,
                    point,
                    method: KillMethod::Sigkill,
                    after_sync: None,
                    inside_resume_from: None,
                    state_root: root,
                    durable,
                }
            }
            (point, trigger) => panic!("{trigger:?} cannot make a kill at {point:?}"),
        }
    }

    /// Resume the state `kill` left through the real run loop, with a fresh provider and tool
    /// plane so everything they record belongs to the resume alone.
    pub async fn resume(&self, kill: &KillReport) -> Resumed {
        let provider = ScriptedProvider::for_scenario(&self.scenario);
        let plane = HarnessPlane::for_scenario(&self.scenario);
        let store = FileWalStore::new(kill.state_root.join("wal"));
        let (control, _) = self.control(false);
        let outcome = drive(
            (self.scenario.request)(),
            control,
            &kill.state_root,
            &store,
            &provider,
            &plane,
        )
        .await;
        let log = LogImage::read(&self.session_wal(&kill.state_root));
        Resumed {
            reason: outcome.reason,
            provider,
            plane,
            log,
            outcome,
        }
    }

    /// Assert every resume invariant for `kill` and the resume after it.
    pub fn check(&self, kill: &KillReport, resumed: &Resumed) {
        invariants::check(&invariants::CheckInput {
            label: &kill.label(),
            prefix: &kill.durable,
            after: &resumed.log,
            reason: resumed.reason,
            expected_uninterrupted: self.scenario.expected,
            provider: &resumed.provider,
            plane: &resumed.plane,
            reference_sent: &self.reference.sent,
        });
    }

    /// Kill at `point`, resume, and check.
    pub async fn kill_resume_check(
        &self,
        point: KillPoint,
        trigger: KillTrigger,
    ) -> (KillReport, Resumed) {
        let kill = self.kill_at(point, trigger).await;
        let resumed = self.resume(&kill).await;
        self.check(&kill, &resumed);
        (kill, resumed)
    }

    /// Kill at every record boundary of the reference log (and, with `torn`, as a torn append
    /// after each), resume each, and check each.
    pub async fn every_record_boundary(&self, torn: bool) -> Vec<KillReport> {
        let mut kills = Vec::new();
        for point in self.record_boundaries(torn) {
            kills.push(self.kill_resume_check(point, KillTrigger::Truncate).await.0);
        }
        kills
    }

    /// Resume `kill`, then kill that resume at every boundary of what it appended, and resume
    /// and check again from each. This is the crash inside a crash-recovery: the second resume
    /// must converge, write its own resume record, and add nothing the first already made
    /// durable.
    pub async fn every_boundary_inside_resume(&self, kill: &KillReport) -> Vec<KillReport> {
        let first = self.resume(kill).await;
        self.check(kill, &first);
        let from = kill.durable.record_count();
        let total = first.log.record_count();
        let mut kills = Vec::new();
        for records in from + 1..=total {
            let root = self.fresh_root();
            let cut = cut_log(&first.log, records, false);
            write_log(&self.session_wal(&root), &cut);
            let nested = KillReport {
                scenario: self.scenario.name,
                point: KillPoint::RecordBoundary {
                    records,
                    torn: false,
                },
                method: KillMethod::Truncation,
                after_sync: None,
                inside_resume_from: Some(from),
                state_root: root,
                durable: LogImage::parse(cut),
            };
            let resumed = self.resume(&nested).await;
            self.check(&nested, &resumed);
            kills.push(nested);
        }
        kills
    }
}

/// The scenario's controls for one run, with the stop request of a stopping run. For the
/// reference run the request is not made yet; it is returned so the tool plane can make it when
/// the first call is dispatched. For a resume it is made before the run starts.
fn stop_control(
    scenario: &Scenario,
    stop: Option<StopAt>,
    reference: bool,
) -> (broca_core::RunControl, Option<CancellationToken>) {
    let mut control = (scenario.control)();
    let Some(StopAt::FirstToolCall) = stop else {
        return (control, None);
    };
    let token = CancellationToken::new();
    control.stop_requested = Some(token.clone());
    if reference {
        (control, Some(token))
    } else {
        token.cancel();
        (control, None)
    }
}

/// The bytes of `log` cut just after its first `records` records, plus half of the next frame
/// when `torn`.
fn cut_log(log: &LogImage, records: usize, torn: bool) -> Vec<u8> {
    let mut end = log.end_of_records(records);
    if torn {
        let next = log
            .record_frame(records)
            .unwrap_or_else(|| panic!("no frame follows record {records} to tear"));
        end = next.start + (next.end - next.start) / 2;
    }
    log.bytes[..end].to_vec()
}

fn write_log(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().expect("a log lives in a directory"))
        .expect("create the log directory");
    std::fs::write(path, bytes).expect("write the cut log");
}

/// Spawn the current test binary as a runner of `scenario` under `root`, wait until it is
/// inside `in_flight`, and kill it with SIGKILL. Panics if it finishes or dies first.
fn kill_spawned_runner(scenario: &str, root: &Path, in_flight: &InFlight) {
    let marker = scenario::reached_marker(root);
    let point = in_flight.encode();
    kill_child_test_at_marker(
        &format!("{scenario} at {in_flight:?}"),
        CHILD_TEST_NAME,
        &[
            (ENV_SCENARIO, std::ffi::OsStr::new(scenario)),
            (ENV_ROOT, root.as_os_str()),
            (ENV_POINT, std::ffi::OsStr::new(&point)),
        ],
        &marker,
    );
}

/// Re-run the current test binary with only `child_test` selected and `envs` set, wait until
/// the child creates `marker`, and kill it with SIGKILL (the platform's forced termination
/// elsewhere). Panics if the child exits first, never reaches the marker, or does not die by
/// the kill. Returns how it killed, for the caller's [`KillReport`].
///
/// This is the real-kill mechanism [`RunnerHandle::kill_at`] uses, exposed for a runner that is
/// not a [`Scenario`] of the bare run loop: a test whose child hosts a whole lineage actor, for
/// instance, so that state held only in the actor's memory dies with the process.
pub fn kill_child_test_at_marker(
    label: &str,
    child_test: &str,
    envs: &[(&str, &std::ffi::OsStr)],
    marker: &Path,
) -> KillMethod {
    let mut command =
        std::process::Command::new(std::env::current_exe().expect("current test binary"));
    command
        .args(["--exact", child_test, "--nocapture"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("spawn the runner process");
    let scenario = label;
    let in_flight = "its kill point";
    let deadline = Instant::now() + REACH_TIMEOUT;
    while !marker.exists() {
        if let Some(status) = child.try_wait().expect("poll the runner process") {
            panic!("{scenario}: the runner exited ({status}) before reaching {in_flight}");
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{scenario}: the runner never reached {in_flight}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    // `Child::kill` is SIGKILL on Unix: the runner gets no chance to flush, sync or seal.
    child.kill().expect("kill the runner process");
    let status = child.wait().expect("reap the runner process");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(9),
            "{scenario}: the runner must die by SIGKILL"
        );
    }
    #[cfg(not(unix))]
    assert!(!status.success(), "{scenario}: the runner must be killed");
    KillMethod::Sigkill
}

/// The body of the child-entry test. In an ordinary test run the harness variables are unset
/// and this returns at once. In a runner process spawned by [`RunnerHandle::kill_at`] it runs
/// the named scenario on the given state root and blocks forever at the in-flight point.
pub fn serve_child(find: fn(&str) -> Option<Scenario>) {
    let (Ok(name), Ok(root), Ok(point)) = (
        std::env::var(ENV_SCENARIO),
        std::env::var(ENV_ROOT),
        std::env::var(ENV_POINT),
    ) else {
        return;
    };
    let scenario = find(&name).unwrap_or_else(|| panic!("no scenario named {name}"));
    let in_flight =
        InFlight::decode(&point).unwrap_or_else(|| panic!("bad in-flight point {point}"));
    let root = PathBuf::from(root);
    let marker = scenario::reached_marker(&root);
    let mut provider = ScriptedProvider::for_scenario(&scenario);
    let mut plane = HarnessPlane::for_scenario(&scenario);
    match in_flight {
        InFlight::ToolDispatch(id) => plane = plane.blocking_at(id, marker),
        InFlight::ProviderStep(step) => provider = provider.blocking_at(step, marker),
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runner runtime");
    runtime.block_on(async {
        let store = FileWalStore::new(root.join("wal"));
        drive(
            (scenario.request)(),
            (scenario.control)(),
            &root,
            &store,
            &provider,
            &plane,
        )
        .await;
    });
    // Reaching here means the run finished without passing its in-flight point; the parent
    // sees the process exit and fails the kill.
    panic!("{name}: the run finished without reaching {point}");
}

/// Define the child-entry test a test binary needs for [`KillTrigger::ProcessKill`].
/// `$find` is a `fn(&str) -> Option<Scenario>` that knows every scenario the binary kills.
#[macro_export]
macro_rules! crash_harness_child {
    ($find:path) => {
        #[test]
        fn crash_harness_child() {
            $crate::serve_child($find);
        }
    };
}

/// The kills of one suite. [`Suite::finish`] fails unless at least one was a real process
/// kill, because truncating a log can only reproduce state that reached the log.
#[derive(Default)]
pub struct Suite {
    pub name: &'static str,
    kills: Vec<(String, KillMethod)>,
}

impl Suite {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            kills: Vec::new(),
        }
    }

    pub fn record(&mut self, kills: impl IntoIterator<Item = KillReport>) {
        self.kills
            .extend(kills.into_iter().map(|kill| (kill.label(), kill.method)));
    }

    /// How many kills used each method.
    pub fn methods(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for (_, method) in &self.kills {
            *counts.entry(format!("{method:?}")).or_default() += 1;
        }
        counts
    }

    pub fn finish(self) -> BTreeMap<String, usize> {
        assert!(
            self.kills
                .iter()
                .any(|(_, method)| *method == KillMethod::Sigkill),
            "suite {} made no real process kill; truncation alone cannot catch state that lives \
             only in memory",
            self.name
        );
        self.methods()
    }
}

/// Whether `records` hold a `resume_started` record. A convenience for scenario tests.
pub fn resume_records(records: &[WalRecord]) -> usize {
    records
        .iter()
        .filter(|record| matches!(record, WalRecord::ResumeStarted { .. }))
        .count()
}
