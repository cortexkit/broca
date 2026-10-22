//! Boot-configured automatic WAL archiving; absence creates no background task.
use broca_core::wal_fold::{
    fold_with_identities, ArchivePassContended, FoldOptions, FoldReport, FoldSettings,
};
use broca_diagnostics::{spawn_blocking, JobKind};

use std::{
    io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

// A day-based threshold needs no sub-daily scan. Waiting a full day before every
// pass also ensures a crash-looping process never folds at each restart.
pub const WAL_ARCHIVE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// How often the schedule is CHECKED, as opposed to how often a pass RUNS.
///
/// Short on purpose. The decision is made against the wall clock and a durable on-disk
/// anchor, so this only bounds how late a due pass can be -- and, more usefully, how
/// long a broken schedule can stay invisible. One long sleep made that a full day.
const WAL_ARCHIVE_POLL: Duration = Duration::from_secs(60 * 60);

/// Schedule checks made by this process since boot, readable on `health.check`.
///
/// THE LIVENESS SIGNAL THIS FEATURE DID NOT HAVE. On 2026-09-14 the first scheduled pass
/// never appeared, and from outside the process "the task is sleeping", "the task never
/// spawned" and "the task died" were one observation -- every other fact was checkable
/// and this one was not, so the diagnosis dead-ended on silence.
///
/// A count that climbs each hour answers it without a log line per poll: stuck at zero
/// means the task is not running, climbing means it is and the decision is simply not
/// due yet. It resets on restart, so read two samples from one `launch_id`.
static WAL_ARCHIVE_POLLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// See [`WAL_ARCHIVE_POLLS`]. Zero on a configured daemon that has been up more than an
/// hour is a defect; zero with the knob unset is correct and expected.
pub fn wal_archive_polls() -> u64 {
    WAL_ARCHIVE_POLLS.load(std::sync::atomic::Ordering::Relaxed)
}
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
// Names both the action and the idle-age unit, not a deletion policy or timer cadence.
const KEY: &str = "wal_archive_idle_retention_days";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalArchiveConfig {
    days: Option<u64>,
}
impl WalArchiveConfig {
    pub fn enabled(self) -> bool {
        self.days.is_some()
    }

    pub fn load_default_path() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Resolved { path, .. } => Self::load(&path),
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(format!("read broca config {}: {e}", path.display())),
        };
        Self::parse(&text).map_err(|e| format!("parse broca config {}: {e}", path.display()))
    }

    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let stripped = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&stripped).map_err(|e| e.to_string())?;
        let Some(value) = object.get(KEY) else {
            return Ok(Self::default());
        };
        let days = value
            .as_u64()
            .filter(|days| *days > 0 && days.checked_mul(DAY_MS).is_some())
            .ok_or_else(|| {
                format!(
                    "{KEY} must be a positive whole number of days representable in milliseconds"
                )
            })?;
        if days < 14 {
            tracing::warn!(target: "archive", "{KEY}={days} is below 14 days: risk of archive/extract/re-archive churn, not data loss; measured append horizon 0.61 days, multi-day append 95% upper bound 0.10%");
        }
        Ok(Self { days: Some(days) })
    }

    fn options(self, resume: Option<String>) -> FoldOptions {
        FoldOptions {
            write: true,
            resume,
            ..Default::default()
        }
    }

    pub fn spawn(self, root: PathBuf) -> Option<tokio::task::JoinHandle<()>> {
        self.spawn_every(root, WAL_ARCHIVE_INTERVAL, WAL_ARCHIVE_POLL)
    }

    /// The interval is a PARAMETER so a test can watch a real sleep complete.
    ///
    /// It was a const read inside the loop, which left `start_paused` virtual time as the
    /// only way to test firing -- and virtual time cannot tell you that a wall-clock timer
    /// works. Production is `spawn`; this exists so the real-clock path has a test at all.
    pub fn spawn_every(
        self,
        root: PathBuf,
        interval: Duration,
        poll: Duration,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !self.enabled() {
            return None;
        }
        // SAY THAT THE TIMER ARMED. Nothing on this path logged, and the failure that
        // exposed it is exactly the one silence cannot distinguish: the first scheduled
        // pass did not appear, and from outside the process "armed and still sleeping",
        // "never armed", and "armed and died" are one observation. Every other fact was
        // checkable -- the config loads Some(30), its mtime predates boot, the binary
        // carries the code, the host slept 2.9 minutes, no pass is running -- and the one
        // thing that would have settled it in a line was the one thing not recorded.
        //
        // A feature whose whole contract is "something happens later, by itself" must say
        // so when it arms, or its first report is 24 hours after the question is asked.
        // PRINT WHAT THIS CALL WILL DO, not what the module's constants say. The wording
        // named the MONOTONIC clock, which was true when the line was written and false
        // an hour later when the schedule moved to a wall-clock decision against the
        // newest container or journal -- so the one line added to make this feature
        // diagnosable was describing a mechanism it no longer had, and pointing a reader
        // at host sleep, which is exactly the dead end that cost a night.
        //
        // The values come from the ARGUMENTS for the same reason: `spawn_every` takes
        // both, so printing the constants would assert a number this call did not read.
        // INFO: true on a healthy boot. It says the timer exists, which is worth having -- the
        // first scheduled fold was a silent no-show and this line is what separates "never armed"
        // from "armed and not yet due". But it is not a degraded state, so an operator filtering
        // to `warn` should not have to read it.
        tracing::info!(
            target: "archive",
            "{}",
            arming_notice(self.days.unwrap_or_default(), interval, poll)
        );
        // POLL SHORT, DECIDE ON THE WALL CLOCK. One 24-hour sleep made the schedule a
        // property of THIS PROCESS's uptime, which is wrong on its own merits: a restart
        // resets it, so a daemon restarted more often than the interval never folds at
        // all. Five releases were placed here in one day; the process that was supposed
        // to fold survived 24h48m by luck rather than design.
        //
        // The durable anchor is already on disk. Containers and journals are named
        // `fold-<unix-ms>`, so even a journal-only no-op advances the anchor -- and
        // it counts an operator's `store-maintenance wal-fold` too, so the scheduled and
        // manual paths stop disagreeing about when a day has passed.
        //
        // This does not claim to explain a missed pass. It removes a dependency that is
        // indefensible once you have seen the restart cadence, and it turns a miss into
        // something visible within an hour instead of a day.
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(poll).await;
                WAL_ARCHIVE_POLLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let probe = root.clone();
                let last = match spawn_blocking(JobKind::Archive, move || last_pass(&probe)).await {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::warn!(target: "archive", "WAL archive schedule probe failed: {e}");
                        continue;
                    }
                };
                match last {
                    Some(reference) => {
                        let elapsed = SystemTime::now()
                            .duration_since(reference)
                            .unwrap_or_default();
                        if elapsed < interval {
                            continue;
                        }
                        tracing::info!(
                            target: "archive",
                            "WAL archive pass due: {}s since the last pass, interval {}s",
                            elapsed.as_secs(),
                            interval.as_secs()
                        );
                    }
                    // No usable pass timestamp was found under this root (none was ever
                    // recorded, or the archive directories cannot be read), so there is no
                    // durable anchor to measure from. Measuring from this process's start would let a daemon
                    // restarted more often than the interval wait forever, and with no first
                    // pass no anchor would ever appear. The first poll of uptime is the grace
                    // period; the pass it runs records the anchor every later decision uses.
                    None => tracing::info!(
                        target: "archive",
                        "WAL archive pass due: no earlier pass is recorded under this root"
                    ),
                }
                let root = root.clone();
                match spawn_blocking(JobKind::Archive, move || scheduled_pass(&root, self)).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(target: "archive", "scheduled WAL archive failed: {e}")
                    }
                    Err(e) => tracing::warn!(target: "archive", "scheduled WAL archive task: {e}"),
                }
            }
        }))
    }
}

/// When the last fold started, read from the archive directory itself.
///
/// Both `fold-<unix-ms>.ark` and `.journal` record a pass, including no-op passes
/// that create only a journal. This anchor survives restarts without new state.
/// An operator's `store-maintenance wal-fold` writes the same names, so a manual fold
/// postpones the scheduled one exactly as a scheduled fold would.
///
/// `None` means no usable pass timestamp was found (none recorded yet, or the archive
/// directories are unreadable), NOT "long ago": the caller runs the pass on its
/// first poll, which records the anchor, rather than measuring an interval from its own
/// start time, which a daemon restarted more often than the interval would never reach.
fn last_pass(root: &Path) -> Option<SystemTime> {
    let newest = ["wal-archive", "wal-archive-durable"]
        .into_iter()
        .filter_map(|name| std::fs::read_dir(root.join(name)).ok())
        .flatten()
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if !path
                .extension()
                .is_some_and(|ext| ext == "ark" || ext == "journal")
            {
                return None;
            }
            broca_wal::archive::fold_timestamp(path.with_extension("ark")).ok()
        })
        .max()?;
    Some(UNIX_EPOCH + Duration::from_millis(newest))
}

/// The arming notice, as a value so a test can read what an operator will read.
///
/// Built from the ARGUMENTS in force for this call rather than the module constants:
/// `spawn_every` takes both, so the constants would assert numbers this call never read.
/// THE POLL CADENCE IS MONOTONIC AND THE DUE DECISION IS WALL-CLOCK, deliberately.
///
/// `tokio::time::sleep` is Instant-based, and Rust's Instant on macOS is
/// `mach_absolute_time` == `CLOCK_UPTIME_RAW`, which STOPS DURING SYSTEM SLEEP. So on a
/// laptop the poll fires per AWAKE hour, not per wall hour: observed here at 5.88h wall
/// against 0.96h awake, a factor of six.
///
/// That is tolerable only because the two clocks are separated. The pass-due test below
/// compares `SystemTime::now()` against the newest pass stamp, both wall-clock, so
/// sleep makes polls RARER without ever making a due pass invisible -- the next poll to
/// fire sees the full elapsed wall time and folds. A monotonic due test would silently
/// stop folding on a machine that sleeps, which is the defect the boot-uptime schedule
/// had in a different costume.
///
/// HOW TO MEASURE AWAKE TIME, because both obvious instruments are wrong on macOS:
/// `clock_gettime(CLOCK_UPTIME_RAW)` differenced against `kern.boottime` wall. NOT
/// CLOCK_MONOTONIC, which on Darwin does NOT stop during sleep and disagrees with
/// UPTIME_RAW by a factor of six here; and NOT the pmset log, which writes a Wake record
/// at sleep-entry time, so pairing Sleep to the first Wake counts one second where the
/// real interval is thirteen minutes. That parse under-reported by 400x and was used to
/// (wrongly) rule this mechanism out as the cause of a missed pass.
///
/// CONSEQUENCE FOR ANY LIVENESS CHECK ON `wal_archive_polls`: zero is only evidence of a
/// dead loop when AWAKE time exceeds the poll interval. Measured against `ps etime`, which
/// is wall, a healthy timer reads as dead. It did, and I nearly reported it.
fn arming_notice(days: u64, interval: Duration, poll: Duration) -> String {
    format!(
        "WAL archive timer armed: retention={} days, interval={}s, checked every {}s against the newest container or journal on the wall clock",
        days,
        interval.as_secs(),
        poll.as_secs()
    )
}

fn scheduled_pass(root: &Path, config: WalArchiveConfig) -> io::Result<Option<FoldReport>> {
    // RESUME AN UNFINISHED PASS RATHER THAN ASKING FOR A FRESH ONE.
    //
    // A fresh fold refuses while an unfinished journal exists, and that gate is right --
    // a half-retired pass must be finished deliberately. But the refusal names `--resume`
    // to an operator, and this caller is not one. Without this it asks for a fresh fold
    // every time and is refused every time.
    //
    // The wedge is ordinary, not exotic: phase 1 archives a cold session and releases its
    // lease; before phase 2 reacquires it, a request reopens the session and the actor
    // holds the lease for its lifetime. Phase 2 skips that member, so the journal never
    // reaches a terminal state -- and from then on EVERY scheduled pass under this root
    // is refused, across restarts, long after the competing actor is gone. One transient
    // collision, permanent stoppage, and `finish` returns Ok so nothing says so.
    //
    // Resuming is strictly better than the status quo even when it cannot succeed: a
    // journal that needs an operator fails loudly here, which is what a refused fresh
    // fold was already doing every pass.
    let resume = broca_core::wal_fold::unfinished_pass(root)?;
    if let Some(name) = &resume {
        tracing::warn!(target: "archive", "scheduled WAL archive resuming unfinished pass {name}");
    }
    let identities = broca_store::fold_identities(root)
        .map_err(|e| io::Error::other(format!("cannot read fold identities: {e}")))?;
    let options = config.options(resume);
    let settings = FoldSettings {
        options: &options,
        cold_ms: config.days.expect("scheduled only when configured") * DAY_MS,
    };
    match fold_with_identities(root, settings, &identities) {
        Ok(report) => {
            let skipped: std::collections::BTreeMap<_, _> = report
                .skipped
                .iter()
                .map(|(reason, sessions)| (reason, sessions.len()))
                .collect();
            // INFO: the result of a pass that ran. A pass finding nothing is the steady state
            // once the tree is folded -- the first scheduled pass reported candidates=0 and that
            // was correct, not a failure. The FAILURE arms below stay at warn.
            tracing::info!(target: "archive", "scheduled WAL archive: candidates={} removed={} restored={} superseded={} resolved_from_store={} cold_pre_schema={} skipped={skipped:?} journal={:?}",
                report.candidates.len(), report.removed, report.restored, report.superseded,
                report.resolved_from_store.len(), report.cold_pre_schema.len(),
                    report.journal);
            if let Some(notice) = report.permanent_skip_notice {
                tracing::warn!(target: "archive", "scheduled WAL archive: {notice}");
            }
            Ok(Some(report))
        }
        Err(e) if e.get_ref().is_some_and(|e| e.is::<ArchivePassContended>()) => {
            tracing::warn!(target: "archive", "scheduled WAL archive skipped (lease contention): {e}");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) struct TempRoot(PathBuf);
    impl TempRoot {
        pub(super) fn new() -> io::Result<Self> {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = broca_test_support::scratch_root().join(format!(
                "broca-auto-fold-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir(&path)?;
            Ok(Self(path))
        }
        pub(super) fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn config_validation() {
        for value in [
            "0",
            "-1",
            "1.5",
            "1.0",
            "null",
            "true",
            "\"30\"",
            "18446744073709551615",
        ] {
            assert!(WalArchiveConfig::parse(&format!("{{\"{KEY}\":{value}}}"))
                .unwrap_err()
                .contains(KEY));
        }
        assert!(WalArchiveConfig::parse("{").is_err());
        assert_eq!(
            WalArchiveConfig::parse("{ // off\n }").unwrap(),
            WalArchiveConfig::default()
        );
        assert_eq!(
            WalArchiveConfig::parse(&format!("{{\"{KEY}\":1,}}"))
                .unwrap()
                .days,
            Some(1)
        );
    }

    #[test]
    fn config_file_missing_is_off_but_malformed_is_an_error() {
        let root = TempRoot::new().unwrap();
        let path = root.path().join("broca.jsonc");
        assert_eq!(
            WalArchiveConfig::load(&path).unwrap(),
            WalArchiveConfig::default()
        );
        std::fs::write(&path, "{").unwrap();
        assert!(WalArchiveConfig::load(&path)
            .unwrap_err()
            .contains("broca.jsonc"));
        std::fs::write(&path, "{\"wal_archive_idle_retention_days\":30}").unwrap();
        assert!(WalArchiveConfig::load(&path).unwrap().enabled());
    }

    #[tokio::test]
    async fn absent_knob_spawns_no_timer() {
        let task = WalArchiveConfig::parse("{}").unwrap().spawn(PathBuf::new());
        let spawned = task.is_some();
        if let Some(task) = task {
            task.abort();
        }
        assert!(!spawned, "absent knob must not spawn any timer");
    }

    #[test]
    fn configured_threshold_reaches_fold() {
        use broca_core::{FileWalStore, WalStore};
        use broca_protocol::{BindIdentity, RunId};
        use broca_wal::{RunConfig, RunOrigin, WalRecord};
        let root = TempRoot::new().unwrap();
        std::fs::create_dir(root.path().join("wal")).unwrap();
        let store = FileWalStore::new(root.path().join("wal"));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        for (name, age) in [("twenty-days", 20), ("forty-days", 40)] {
            let session = BindIdentity::new("/auto-fold", "runner", name);
            let mut wal = store.open(&session).unwrap();
            wal.append(
                1,
                &WalRecord::RunStarted {
                    tags: None,
                    session_class: None,
                    retention: None,
                    hooks: None,
                    title: None,
                    on_restart: None,
                    keep_warm: None,
                    marker_order_version: None,
                    manifest: None,
                    scope: None,
                    send_id: None,
                    request_identity_digest: None,
                    project_id: None,
                    principal: None,
                    run_id: RunId(name.into()),
                    session,
                    ts_ms: Some(now - age * DAY_MS),
                    config: Box::new(RunConfig {
                        turn_directives_v1: None,
                        render_tags: false,
                        computed_tags: None,
                        model: broca_protocol::ModelHandle::new("stub", "m"),
                        tools: vec![].into(),
                        tool_choice: broca_protocol::ToolChoice::Auto,
                        generation: Default::default(),
                        response_format: Default::default(),
                        provider_options: serde_json::Value::Null,
                        cache_tiers: serde_json::Value::Null,
                        context_epoch: 0,
                        renderer_version: 1,
                        frozen_render_config: serde_json::Value::Null,
                        auth_selection: None,
                        tools_were_omitted: false,
                        budget: None,
                        context_limit: None,
                        work_class: None,
                        variant: None,
                        service_tier: None,
                        stop_when: vec![],
                        context_management: None,
                    }),
                    input: Default::default(),
                    origin: RunOrigin::Fresh,
                    submission_id: None,
                },
                true,
            )
            .unwrap();
        }
        // This is the operator CLI's unchanged no-threshold options path.
        let preview = fold_with_identities(
            root.path(),
            &FoldOptions::default(),
            &broca_store::fold_identities(root.path()).unwrap(),
        )
        .unwrap();
        assert_eq!(preview.candidates.len(), 2);
        let config = WalArchiveConfig::parse(&format!("{{\"{KEY}\":30}}")).unwrap();
        let report = scheduled_pass(root.path(), config).unwrap().unwrap();
        assert_eq!(report.removed, 1);
        assert_eq!(report.skipped.get("warm").unwrap().len(), 1);
        assert_eq!(
            std::fs::read_dir(root.path().join("wal")).unwrap().count(),
            1
        );
    }

    /// The timer fires under REAL time, not only virtual time.
    ///
    /// `configured_timer_waits_a_full_day` runs under `start_paused` and advances the
    /// clock by hand, so it proves the loop's SHAPE and nothing about whether a real
    /// sleep ever completes. When the first production pass did not appear, that test was
    /// green and had no bearing on the question -- virtual time cannot tell you that a
    /// wall-clock timer works.
    ///
    /// This one uses the real clock with a short interval, which is only expressible
    /// because the interval is now a parameter rather than a const read inside the loop.
    #[tokio::test]
    async fn timer_fires_under_real_time() {
        let root = TempRoot::new().unwrap();
        std::fs::create_dir(root.path().join("wal")).unwrap();
        let task = WalArchiveConfig { days: Some(30) }
            .spawn_every(
                root.path().to_owned(),
                Duration::from_millis(50),
                Duration::from_millis(20),
            )
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if root.path().join("wal-archive").exists() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the scheduled pass never ran under real time"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        task.abort();
    }

    #[test]
    fn no_op_pass_defers_the_next_due_time_by_a_day() {
        let root = TempRoot::new().unwrap();
        std::fs::create_dir_all(root.path().join("wal")).unwrap();
        let archive = root.path().join("wal-archive");
        std::fs::create_dir_all(&archive).unwrap();
        // An old container makes an ark-only anchor immediately due, not absent.
        broca_wal::archive::ArchiveWriter::create(archive.join("fold-1.ark"))
            .unwrap()
            .without_feature_floor()
            .seal()
            .unwrap();
        let report = broca_core::wal_fold::fold(
            root.path(),
            &FoldOptions {
                write: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(report.candidates.is_empty());
        let journal = archive.join(report.journal.unwrap());
        assert!(!journal.with_extension("ark").exists());
        let stamp = broca_wal::archive::fold_timestamp(journal.with_extension("ark")).unwrap();
        let ran_at = UNIX_EPOCH + Duration::from_millis(stamp);
        // Probe disk again as a restarted scheduler would; use the same elapsed
        // comparison as the polling loop, without depending on timer sleeps.
        let anchor = last_pass(root.path()).unwrap();
        let due = |now: SystemTime| {
            now.duration_since(anchor).unwrap_or_default() >= WAL_ARCHIVE_INTERVAL
        };
        assert!(!due(ran_at), "a no-op pass must not be due immediately");
        assert!(!due(
            ran_at + WAL_ARCHIVE_INTERVAL - Duration::from_millis(1)
        ));
        assert!(due(ran_at + WAL_ARCHIVE_INTERVAL));
    }

    /// The anchor is the newest container's own name, so it survives a restart.
    #[test]
    fn last_pass_reads_the_newest_container_stamp() {
        let root = TempRoot::new().unwrap();
        let archive = root.path().join("wal-archive");
        std::fs::create_dir_all(&archive).unwrap();
        assert!(
            last_pass(root.path()).is_none(),
            "no container means no pass has run, NOT a pass long ago -- the caller runs the \
             first pass on its first poll rather than inventing an anchor"
        );
        std::fs::write(archive.join("fold-1000.ark"), b"").unwrap();
        std::fs::write(archive.join("fold-9000.ark"), b"").unwrap();
        std::fs::write(archive.join("fold-2000.ark"), b"").unwrap();
        // Numeric precedence, not lexicographic: fold-9000 is newer than fold-10000 would
        // sort. Reusing the archive crate's own parser keeps that rule in one place.
        assert_eq!(
            last_pass(root.path()),
            Some(UNIX_EPOCH + Duration::from_millis(9000))
        );
        std::fs::write(archive.join("not-a-fold.ark"), b"").unwrap();
        assert_eq!(
            last_pass(root.path()),
            Some(UNIX_EPOCH + Duration::from_millis(9000)),
            "an unparseable name must not become the anchor"
        );
    }

    /// The scheduled pass RESUMES a blocking journal instead of asking for a fresh fold.
    ///
    /// A fresh fold is refused while an unfinished journal exists. That gate is correct,
    /// and it names `--resume` to an operator -- but this caller is not one, so before
    /// this it asked for a fresh fold every time and was refused every time. One
    /// transient phase-2 lease collision stopped scheduled archiving permanently under
    /// the root, across restarts, long after the competing actor was gone.
    ///
    /// The journal here is written directly rather than produced by contention: the hook
    /// that creates that race is `#[cfg(test)]` inside broca-core and unreachable from
    /// this crate. `unfinished_pass` is asserted FIRST so the fixture cannot pass by
    /// being unparseable -- an unreadable journal would also avoid the refusal, for the
    /// wrong reason.
    #[test]
    fn a_blocking_journal_is_resumed_rather_than_refused_forever() {
        let root = TempRoot::new().unwrap();
        std::fs::create_dir_all(root.path().join("wal")).unwrap();
        let archive = root.path().join("wal-archive");
        std::fs::create_dir_all(&archive).unwrap();
        let identity = broca_protocol::BindIdentity::new("/p", "runner", "wedge");
        // `archived` is non-terminal: phase 1 sealed the member, phase 2 never retired it.
        // The real addressing function: the journal validates that its address is the
        // FNV hash of its own identity triple, so a hand-written constant is refused.
        let addr = broca_core::wal_writer::session_addr(&identity);
        let line = serde_json::json!({
            "event": "session",
            "addr": addr,
            "session": identity,
            "digest": vec![0u8; 32],
            "state": "archived",
        });
        std::fs::write(archive.join("fold-1000.journal"), format!("{line}\n")).unwrap();

        assert_eq!(
            broca_core::wal_fold::unfinished_pass(root.path()).unwrap(),
            Some("fold-1000.journal".to_owned()),
            "the fixture must be a journal the fold can read and considers unfinished"
        );

        let outcome = scheduled_pass(root.path(), WalArchiveConfig { days: Some(30) });
        let message = outcome.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            !message.contains("unfinished archive pass"),
            "the scheduled pass must resume the journal, not be refused by it: {message}"
        );
    }

    /// The arming line must describe the anchor the loop ACTUALLY uses.
    ///
    /// It said "on the monotonic clock" for six hours after the schedule moved to a
    /// wall-clock decision against the newest container or journal. A peer read it during a
    /// placement and could not tell whether the line was stale or the fix had not shipped
    /// -- the one line added to make this feature diagnosable, pointing at host sleep,
    /// which is precisely the dead end that cost a night of diagnosis.
    #[test]
    fn the_arming_line_names_the_anchor_the_loop_uses() {
        let armed = arming_notice(30, Duration::from_secs(86_400), Duration::from_secs(3_600));
        assert!(
            armed.contains("newest container or journal") && armed.contains("wall clock"),
            "the line must name the durable anchor it decides against: {armed}"
        );
        assert!(
            !armed.contains("monotonic"),
            "the monotonic anchor was replaced; a line still naming it sends a reader to \
             host sleep, which is not the mechanism: {armed}"
        );
        // The ARGUMENTS, not the module constants: spawn_every takes both, so printing
        // the constants would assert numbers this call did not read.
        let odd = arming_notice(7, Duration::from_secs(60), Duration::from_secs(5));
        assert!(
            odd.contains("retention=7 days")
                && odd.contains("interval=60s")
                && odd.contains("every 5s"),
            "the line must report the values in force for this call: {odd}"
        );
    }

    /// The poll counter climbs, which is the liveness signal the feature lacked.
    ///
    /// A due-pass log line only appears when a pass is DUE, so it cannot distinguish a
    /// sleeping task from a dead one in between. This counter can: stuck at zero on a
    /// configured daemon means the task is not running.
    #[tokio::test]
    async fn each_schedule_check_is_counted_so_a_dead_task_is_visible() {
        let root = TempRoot::new().unwrap();
        std::fs::create_dir(root.path().join("wal")).unwrap();
        let before = wal_archive_polls();
        let task = WalArchiveConfig { days: Some(30) }
            // A long interval with a short poll: nothing is ever due, so this counts
            // SCHEDULE CHECKS and not passes -- which is the distinction that matters,
            // since a task can be alive for 23 hours with nothing due.
            .spawn_every(
                root.path().to_owned(),
                WAL_ARCHIVE_INTERVAL,
                Duration::from_millis(20),
            )
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        task.abort();
        assert!(
            wal_archive_polls() > before,
            "a running timer must be observable without waiting for a pass to fall due"
        );
    }

    /// A tree that has never folded runs its first pass on the first poll, then waits for the
    /// interval measured from the anchor that pass recorded.
    ///
    /// The first pass used to wait a full interval from the process's own start, so a daemon
    /// restarted more often than the interval never ran it and never gained an anchor. The
    /// short poll must still not fold every time: this runs in real time with a 20 ms poll
    /// against a 24-hour interval, and exactly one pass may be recorded.
    #[tokio::test]
    async fn a_never_folded_tree_folds_once_on_its_first_poll() {
        let root = TempRoot::new().unwrap();
        std::fs::create_dir(root.path().join("wal")).unwrap();
        let task = WalArchiveConfig { days: Some(30) }
            .spawn_every(
                root.path().to_owned(),
                WAL_ARCHIVE_INTERVAL,
                Duration::from_millis(20),
            )
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        task.abort();
        let passes = std::fs::read_dir(root.path().join("wal-archive"))
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|entry| {
                        entry
                            .path()
                            .extension()
                            .is_some_and(|ext| ext == "ark" || ext == "journal")
                    })
                    .count()
            })
            .unwrap_or(0);
        assert_eq!(
            passes, 1,
            "a never-folded tree must run exactly one pass, on its first poll"
        );
    }

    #[test]
    fn lease_contention_skips_scheduled_pass() {
        let root = TempRoot::new().unwrap();
        std::fs::create_dir(root.path().join("wal")).unwrap();
        let _lease = broca_core::lease::acquire_archive_pass(root.path()).unwrap();
        assert!(
            scheduled_pass(root.path(), WalArchiveConfig { days: Some(30) })
                .unwrap()
                .is_none()
        );
        assert!(!root.path().join("wal-archive").exists());
    }

    #[test]
    fn zero_work_pass_is_reported_and_other_errors_propagate() {
        let root = TempRoot::new().unwrap();
        let config = WalArchiveConfig { days: Some(30) };
        assert!(scheduled_pass(root.path(), config).is_err());
        std::fs::create_dir(root.path().join("wal")).unwrap();
        let report = scheduled_pass(root.path(), config).unwrap().unwrap();
        assert_eq!(report.removed, 0);
        assert!(report.journal.is_some());
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use broca_core::{FileWalStore, WalStore};

    #[test]
    fn scheduled_pass_archives_store_resolved_population() {
        let temp = super::tests::TempRoot::new().unwrap();
        let root = temp.path().to_owned();
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let session = broca_protocol::BindIdentity::new("/store-resolved", "runner", "legacy");
        let store = broca_store::SqliteStore::open(root.join("run-index.db")).unwrap();
        let wals = FileWalStore::new(root.join("wal"));
        let mut wal = wals.open(&session).unwrap();
        // A rekeyed WAL retains the old embedded identity; only the store vouches for its filename.
        wal.append(
            1,
            &broca_wal::WalRecord::RunStarted {
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: broca_protocol::RunId("legacy".into()),
                session: broca_protocol::BindIdentity::new("/old-location", "runner", "legacy"),
                ts_ms: Some(1),
                config: Box::new(broca_wal::RunConfig {
                    turn_directives_v1: None,
                    render_tags: false,
                    computed_tags: None,
                    model: broca_protocol::ModelHandle::new("stub", "m"),
                    tools: vec![].into(),
                    tool_choice: broca_protocol::ToolChoice::Auto,
                    generation: Default::default(),
                    response_format: Default::default(),
                    provider_options: serde_json::Value::Null,
                    cache_tiers: serde_json::Value::Null,
                    context_epoch: 0,
                    renderer_version: 1,
                    frozen_render_config: serde_json::Value::Null,
                    auth_selection: None,
                    tools_were_omitted: false,
                    budget: None,
                    context_limit: None,
                    work_class: None,
                    variant: None,
                    service_tier: None,
                    stop_when: vec![],
                    context_management: None,
                }),
                input: vec![broca_protocol::content::Message::new(
                    broca_protocol::content::Role::User,
                    vec![broca_protocol::content::ContentBlock::Text {
                        text: "legacy input".into(),
                    }],
                )]
                .into(),
                origin: broca_wal::RunOrigin::SeededFrom {
                    origin_kind: broca_wal::SeedOriginKind::Import,
                    source_ref: "test:legacy".into(),
                    origin_device_pseudonym: None,
                    base_seq: None,
                },
                submission_id: None,
            },
            true,
        )
        .unwrap();
        wal.append(
            1,
            &broca_wal::WalRecord::RunFinished {
                interrupt_reason: None,
                reason: broca_protocol::RunFinishReason::Completed,
                ts_ms: Some(1),
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
        drop(wal);
        store
            .project(&session, &wals.read_records(&session).unwrap())
            .unwrap();
        drop(store);
        let identities = broca_store::fold_identities(&root).unwrap();
        let addr = broca_core::session_addr(&session);
        assert_eq!(identities.get(&addr), Some(&session));
        let baseline = broca_core::wal_fold::fold(&root, &FoldOptions::default()).unwrap();
        assert!(baseline.candidates.is_empty());
        let operator = fold_with_identities(&root, &FoldOptions::default(), &identities).unwrap();
        assert_eq!(operator.candidates, std::slice::from_ref(&addr));
        let config = WalArchiveConfig::parse("{\"wal_archive_idle_retention_days\":30}").unwrap();
        let report = scheduled_pass(&root, config).unwrap().unwrap();
        assert_eq!(report.removed, 1);
        assert_eq!(report.resolved_from_store, [addr]);
        assert!(!wals.wal_path(&session).exists());
    }
}
