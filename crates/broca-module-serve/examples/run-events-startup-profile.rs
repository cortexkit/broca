//! Measure what turning run events on costs against a real state root.
//!
//! RUN IT ON A COPY. It writes to the state root it is given: it begins an enabled period (which
//! discards an earlier period's unpublished events), projects into the outbox and appends
//! synthetic sessions. On macOS an APFS clone costs nothing:
//!
//!   cp -c -R ~/.local/share/cortexkit/broca /tmp/broca-clone
//!   CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true \
//!   cargo run --release -p broca-module-serve --example run-events-startup-profile -- \
//!       --state-root /tmp/broca-clone --new-runs 200 --max-seconds 600
//!
//! Examples link this crate's dev-dependencies, which include a test-only broca-wal feature
//! that refuses to compile without debug assertions. Keeping them on in an optimised build
//! measures optimised code while that guard still keeps the build from being deployable.
//!
//! It does what the publisher does on its first enable, with a stub publisher that acknowledges
//! every event at once in place of ck-bus: begin the period, run the startup recovery walk and
//! drain the outbox. Then it writes `--new-runs` synthetic finished runs, each on a new session
//! under the project root `/run-events-startup-profile`, and waits until every one's
//! `run.started` and `run.ended` is published. It prints one line:
//!
//!   wall and process CPU time (getrusage), the outbox's rows and unpublished rows before and
//!   after, sessions the recovery walk visited, bytes read from WAL files, and peak RSS.
//!
//! `--publisher legacy` replays 0.3.232's first enable instead: every session in the run index
//! and the session-ref table, each WAL read whole and flushed, each projected from its first
//! record, and the pending outbox recounted after every projection, then the drain. That is a
//! reconstruction on this build's store, which no longer has the old code paths. Its failure
//! was unbounded, so `--max-seconds` stops either mode and the line then says
//! `finished=false` with the counters reached by then. Use a fresh clone for each run: a legacy
//! run leaves its backfill in the outbox, and a later current run discards it.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use broca_core::{FileWalStore, WalStore};
use broca_module_serve::bus_events::{self, BusConnection, BusConnector, Config, Hints, Service};
use broca_protocol::{BindIdentity, RunFinishReason, RunId};
use broca_store::bus_events::{BusPublisher, Publication};
use broca_store::SqliteStore;
use broca_wal::WalRecord;
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_trait::PublishReceipt;

const ACCOUNT: &str = "box_run_events_profile";
const PROJECT: &str = "/run-events-startup-profile";

struct Args {
    state_root: PathBuf,
    new_runs: usize,
    max_seconds: u64,
    legacy: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut state_root = None;
    let mut new_runs = 200;
    let mut max_seconds = 600;
    let mut legacy = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--state-root" => state_root = Some(PathBuf::from(value()?)),
            "--new-runs" => new_runs = value()?.parse().map_err(|e| format!("--new-runs: {e}"))?,
            "--max-seconds" => {
                max_seconds = value()?
                    .parse()
                    .map_err(|e| format!("--max-seconds: {e}"))?
            }
            "--publisher" => match value()?.as_str() {
                "current" => legacy = false,
                "legacy" => legacy = true,
                other => {
                    return Err(format!(
                        "--publisher must be current or legacy, not {other}"
                    ))
                }
            },
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(Args {
        state_root: state_root.ok_or("--state-root is required")?,
        new_runs,
        max_seconds,
        legacy,
    })
}

/// Acknowledges every publication at once and remembers which runs it saw.
#[derive(Default)]
struct StubBus {
    published: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl BusPublisher for StubBus {
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
        let mut published = self.published.lock().unwrap();
        published.push((
            event.headers["kind"].clone(),
            event.headers["run_id"].clone(),
        ));
        Ok(PublishReceipt::new(published.len() as u64, false))
    }
}

impl StubBus {
    /// Synthetic runs with both lifecycle events published.
    fn finished_profile_runs(&self) -> usize {
        let published = self.published.lock().unwrap();
        let ended: HashSet<&str> = published
            .iter()
            .filter(|(kind, run)| kind == "run.ended" && run.starts_with("profile-"))
            .map(|(_, run)| run.as_str())
            .collect();
        published
            .iter()
            .filter(|(kind, run)| kind == "run.started" && ended.contains(run.as_str()))
            .count()
    }
}

struct StubConnector(Arc<StubBus>);

#[async_trait]
impl BusConnector for StubConnector {
    async fn connect(&self) -> Result<BusConnection, String> {
        Ok(BusConnection {
            publisher: self.0.clone(),
            account: AccountNames::derive(ACCOUNT).map_err(|e| e.to_string())?,
            refresh_at_ms: u64::MAX,
        })
    }
}

struct NoActor;

#[async_trait]
impl broca_module::registry::LineageOpener for NoActor {
    async fn open(
        &self,
        _key: &broca_module::LineageKey,
        _gen: u64,
        _registry: broca_module::registry::RegistryHandle,
    ) -> Result<broca_module::registry::ActorHandle, broca_module::registry::OpenError> {
        Err(broca_module::registry::OpenError::Open(
            "the profile opens no session actor".into(),
        ))
    }
}

/// Outbox rows and unpublished lifecycle rows, read on a connection of its own.
fn outbox_counts(db: &Path) -> (u64, u64) {
    let Ok(conn) =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
    else {
        return (0, 0);
    };
    let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, u64>(0)).unwrap_or(0);
    (
        count("SELECT count(*) FROM bus_event_outbox"),
        count("SELECT count(*) FROM bus_event_outbox WHERE published_ms IS NULL AND best_effort=0"),
    )
}

/// Process CPU time and peak resident set size, in milliseconds and bytes.
#[cfg(unix)]
fn usage() -> (u64, u64) {
    // SAFETY: getrusage only writes the struct it is given.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let ms = |t: libc::timeval| t.tv_sec as u64 * 1000 + t.tv_usec as u64 / 1000;
    // macOS reports the peak in bytes, Linux in kibibytes.
    let rss = if cfg!(target_os = "macos") {
        usage.ru_maxrss as u64
    } else {
        usage.ru_maxrss as u64 * 1024
    };
    (ms(usage.ru_utime) + ms(usage.ru_stime), rss)
}

#[cfg(not(unix))]
fn usage() -> (u64, u64) {
    (0, 0)
}

fn run_started(session: &BindIdentity, run_id: &str, ts_ms: u64) -> WalRecord {
    WalRecord::RunStarted {
        tags: None,
        run_id: RunId(run_id.into()),
        session: session.clone(),
        ts_ms: Some(ts_ms),
        config: Box::new(broca_wal::RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            model: broca_protocol::request::ModelHandle::new("profile", "model"),
            tools: vec![].into(),
            tool_choice: Default::default(),
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
        input: Arc::new(vec![]),
        origin: Default::default(),
        on_restart: None,
        keep_warm: None,
        project_id: None,
        principal: None,
        retention: None,
        session_class: None,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        title: None,
        scope: None,
        marker_order_version: None,
        manifest: None,
        hooks: None,
    }
}

fn run_finished(ts_ms: u64) -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        reason: RunFinishReason::Completed,
        ts_ms: Some(ts_ms),
        usage: Default::default(),
        error: None,
        retries_used: 0,
    }
}

/// Append `count` finished synthetic runs, each on a session of its own.
fn write_new_runs(wal: &FileWalStore, count: usize) -> Result<(), String> {
    for n in 0..count {
        let session = BindIdentity::new(PROJECT, "profile", format!("profile-{n}"));
        let mut writer = wal.open(&session).map_err(|e| e.to_string())?;
        let now = bus_events::now_ms();
        writer
            .append(
                1,
                &run_started(&session, &format!("profile-{n}"), now),
                true,
            )
            .map_err(|e| e.to_string())?;
        writer
            .append(1, &run_finished(bus_events::now_ms()), true)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// What 0.3.232's publisher did on its first enable, until done or `stop` is set.
/// Returns the sessions visited.
fn legacy_first_enable(
    store: &SqliteStore,
    wal: &FileWalStore,
    db: &Path,
    stop: &AtomicBool,
) -> Result<u64, String> {
    let conn = rusqlite::Connection::open(db).map_err(|e| e.to_string())?;
    let mut sessions: Vec<BindIdentity> = conn
        .prepare("SELECT DISTINCT session FROM run_index")
        .and_then(|mut s| {
            s.query_map([], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|e| e.to_string())?
        .iter()
        .filter_map(|json| serde_json::from_str(json).ok())
        .collect();
    let refs: Vec<BindIdentity> = conn
        .prepare("SELECT project_root,harness,session FROM agent_data_session_ref")
        .and_then(|mut s| {
            s.query_map([], |r| {
                Ok(BindIdentity::new(
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|e| e.to_string())?;
    sessions.extend(refs);
    sessions.sort_by(|a, b| {
        (&a.project_root, &a.harness, &a.session).cmp(&(&b.project_root, &b.harness, &b.session))
    });
    sessions.dedup();
    // First every head, then every projection, as recover_heads and the loop after it did.
    let mut heads = Vec::new();
    let mut visited = 0;
    for session in sessions {
        if stop.load(Ordering::Relaxed) {
            return Ok(visited);
        }
        visited += 1;
        let Ok(range) = wal.read_records_range(&session, 0, None, None) else {
            continue;
        };
        let Some(epoch) = range.lineage_id.map(|id| id.to_string()) else {
            continue;
        };
        let head = match range.records() {
            broca_wal::WalRangeRecords::Complete(records) => records.last().map_or(0, |r| r.seq),
            _ => continue,
        };
        if wal.flush_for_read(&session).is_ok() {
            heads.push((session, head, epoch));
        }
    }
    for (session, head, epoch) in heads {
        if stop.load(Ordering::Relaxed) {
            return Ok(visited);
        }
        let after = store.bus_event_position(&session, &epoch).unwrap_or(0);
        let Ok(range) = wal.read_records_range(&session, after, Some(head), None) else {
            continue;
        };
        let broca_wal::WalRangeRecords::Complete(records) = range.into_records() else {
            continue;
        };
        let _ = store.project_bus_events(&session, &epoch, &records, bus_events::now_ms());
        // 0.3.232 recounted the pending outbox after every projection.
        let _ = conn.query_row(
            "SELECT count(*) FROM bus_event_outbox WHERE published_ms IS NULL AND best_effort=0",
            [],
            |r| r.get::<_, u64>(0),
        );
    }
    Ok(visited)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("run-events-startup-profile: {error}");
            eprintln!(
                "usage: run-events-startup-profile --state-root DIR [--new-runs N] \
                 [--max-seconds S] [--publisher current|legacy]"
            );
            std::process::exit(2);
        }
    };
    let db = args.state_root.join("run-index.db");
    let before = outbox_counts(&db);
    let store = Arc::new(SqliteStore::open(&db).expect("open the run index"));
    let hints = Arc::new(Hints::default());
    let wal = Arc::new(
        FileWalStore::new(args.state_root.join("wal")).with_durable_observer(hints.observer()),
    );
    wal.set_session_ref_index(store.clone());
    let deadline = Instant::now() + Duration::from_secs(args.max_seconds);
    let bus = Arc::new(StubBus::default());
    let started = Instant::now();
    let wal_bytes_start = broca_wal::wal_bytes_read();
    let mut finished = true;
    let mut startup_ms = None;
    let mut visited = 0;

    if args.legacy {
        store.enable_bus_events(ACCOUNT).expect("enable the outbox");
        let stop = Arc::new(AtomicBool::new(false));
        let work = {
            let (store, wal, db, stop) = (store.clone(), wal.clone(), db.clone(), stop.clone());
            tokio::task::spawn_blocking(move || legacy_first_enable(&store, &wal, &db, &stop))
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, work).await {
            Ok(Ok(Ok(count))) => visited = count,
            Ok(other) => {
                eprintln!("run-events-startup-profile: legacy pass failed: {other:?}");
                std::process::exit(1);
            }
            Err(_) => {
                stop.store(true, Ordering::Relaxed);
                finished = false;
            }
        }
        while finished {
            if Instant::now() >= deadline {
                finished = false;
                break;
            }
            match store
                .drain_bus_event(bus.as_ref(), bus_events::now_ms())
                .await
            {
                Ok(broca_store::bus_events::DrainResult::Published) => {}
                _ => break,
            }
        }
        if finished {
            startup_ms = Some(started.elapsed().as_millis());
            write_new_runs(&wal, args.new_runs).expect("write the synthetic runs");
            for n in 0..args.new_runs {
                let session = BindIdentity::new(PROJECT, "profile", format!("profile-{n}"));
                let range = wal.read_records_range(&session, 0, None, None).unwrap();
                let epoch = range.lineage_id.unwrap().to_string();
                if let broca_wal::WalRangeRecords::Complete(records) = range.into_records() {
                    let _ =
                        store.project_bus_events(&session, &epoch, &records, bus_events::now_ms());
                }
            }
            while let Ok(broca_store::bus_events::DrainResult::Published) = store
                .drain_bus_event(bus.as_ref(), bus_events::now_ms())
                .await
            {}
        }
    } else {
        let stop = tokio_util::sync::CancellationToken::new();
        let _service = Service {
            config: Config { enabled: true },
            store: store.clone(),
            wal: wal.clone(),
            registry: broca_module::LineageRegistry::new_with_reader(
                Arc::new(NoActor),
                wal.clone(),
                Some(store.clone()),
            ),
            connector: Some(Arc::new(StubConnector(bus.clone()))),
            hints,
            stop: stop.clone(),
        }
        .spawn();
        let reading = |key: &str| bus_events::health()[key].as_u64().unwrap_or(0);
        // Startup is done when the first recovery walk finished and the outbox is drained.
        loop {
            if reading("bus_recovery_walks") >= 1 && reading("bus_outbox_depth") == 0 {
                startup_ms = Some(started.elapsed().as_millis());
                break;
            }
            if Instant::now() >= deadline {
                finished = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if finished {
            let wal_writer = wal.clone();
            let count = args.new_runs;
            tokio::task::spawn_blocking(move || write_new_runs(&wal_writer, count))
                .await
                .unwrap()
                .expect("write the synthetic runs");
            while bus.finished_profile_runs() < args.new_runs {
                if Instant::now() >= deadline {
                    finished = false;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        visited = reading("bus_recovery_visits");
        stop.cancel();
    }

    let wall_ms = started.elapsed().as_millis();
    let (cpu_ms, peak_rss) = usage();
    let after = outbox_counts(&db);
    let mut line = format!(
        "run-events-startup-profile publisher={} finished={finished}",
        if args.legacy { "legacy" } else { "current" }
    );
    if !finished {
        line.push_str(&format!(" did_not_finish_in_s={}", args.max_seconds));
    }
    line.push_str(&format!(
        " wall_ms={wall_ms} startup_ms={} cpu_ms={cpu_ms} outbox_rows_before={} \
         outbox_unpublished_before={} outbox_rows_after={} outbox_unpublished_after={} \
         sessions_visited={visited} wal_bytes_read={} peak_rss_bytes={peak_rss} \
         new_runs_published={}/{} events_published={}",
        startup_ms.map_or("none".to_string(), |ms| ms.to_string()),
        before.0,
        before.1,
        after.0,
        after.1,
        broca_wal::wal_bytes_read() - wal_bytes_start,
        bus.finished_profile_runs(),
        args.new_runs,
        bus.published.lock().unwrap().len(),
    ));
    println!("{line}");
    // A legacy pass stopped at the deadline may still be inside one WAL read; do not wait.
    std::process::exit(0);
}
