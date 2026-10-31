use super::*;

/// The stale threshold every Witness gives the watchdog, in milliseconds.
const STALE_MS: u64 = 200;
/// The minimum spacing of repeated snapshots for one ongoing stall, in milliseconds.
const REPEAT_MS: u64 = 1_000;

/// The records with heartbeat-starvation episodes taken out.
///
/// The heartbeat is a task on this test's runtime. When a loaded host does not schedule that
/// runtime for longer than STALE_MS and nothing else is stale, the watchdog correctly writes a
/// snapshot whose only trigger is the heartbeat, and a recovery once nothing is stale. Those
/// episodes say nothing about the reader and egress paths these tests drive, so they are set
/// aside here instead of being counted by exact-count assertions. Each one must show a heartbeat
/// older than STALE_MS: a watchdog that fired on a fresh heartbeat still fails. A snapshot that
/// also names another trigger is kept.
fn without_starvation(records: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut kept = Vec::new();
    let mut starving = false;
    for record in records {
        if record["triggers"] == serde_json::json!(["heartbeat"]) {
            let age = record["heartbeat"]["age_ms"].as_u64().unwrap_or(0);
            assert!(
                age > STALE_MS,
                "a heartbeat snapshot with a {age} ms heartbeat: {record}"
            );
            starving = true;
            continue;
        }
        if starving && record["kind"] == "recovery" {
            starving = false;
            continue;
        }
        starving = false;
        kept.push(record.clone());
    }
    kept
}

pub(crate) struct Witness {
    pub(crate) state: Arc<State>,
    pub(crate) watchdog: Watchdog,
    path: std::path::PathBuf,
}
impl Witness {
    pub(crate) fn new(state: State) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = broca_test_support::scratch_root().join(format!(
            "stall-{}-{}-{}.jsonl",
            std::process::id(),
            now_ms(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        let state = Arc::new(state);
        let watchdog = launch(
            state.clone(),
            file,
            STALE_MS,
            REPEAT_MS,
            Duration::from_millis(5),
            Duration::from_millis(10),
        )
        .unwrap();
        Self {
            state,
            watchdog,
            path,
        }
    }
    pub(crate) fn records(&self) -> Vec<serde_json::Value> {
        std::fs::read_to_string(&self.path)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }
    pub(crate) async fn snapshot(&self) -> serde_json::Value {
        self.snapshot_matching(|_| true).await
    }

    async fn snapshot_matching(
        &self,
        matches: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(record) = self.records().into_iter().find(&matches) {
                    return record;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("watchdog snapshot deadline")
    }

    /// Select a stale gated reader with a healthy heartbeat, not an earlier
    /// starvation episode or a sample where heartbeat legitimately has priority.
    /// Do not filter by kind: the caller must still detect a classification bug.
    pub(crate) async fn reader_phase_snapshot(&self, phase: Phase) -> serde_json::Value {
        self.snapshot_matching(|record| {
            record["reader"]["phase_code"].as_u64() == Some(phase as u64)
                && record["reader"]["age_ms"]
                    .as_u64()
                    .is_some_and(|age| age > STALE_MS)
                && record["heartbeat"]["age_ms"]
                    .as_u64()
                    .is_some_and(|age| age <= STALE_MS)
        })
        .await
    }
}
impl Drop for Witness {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Relaxed);
        self.watchdog.heartbeat.abort();
        if let Some(thread) = self.watchdog.thread.take() {
            thread.thread().unpark();
            thread.join().unwrap();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

fn append_snapshots(witness: &Witness, records: &[serde_json::Value]) {
    let mut file = OpenOptions::new().append(true).open(&witness.path).unwrap();
    for record in records {
        writeln!(file, "{record}").unwrap();
    }
}

fn phase_snapshot(phase: Phase, heartbeat_age: u64, kind: &str) -> serde_json::Value {
    serde_json::json!({
        "kind": kind,
        "heartbeat": {"age_ms": heartbeat_age},
        "reader": {"phase_code": phase as u8, "age_ms": STALE_MS + 1},
    })
}

#[tokio::test]
async fn reader_phase_snapshot_waits_for_phase_with_fresh_heartbeat() {
    let witness = Witness::new(State::new());
    let expected = phase_snapshot(Phase::Control, 0, "reader_phase");
    append_snapshots(
        &witness,
        &[
            phase_snapshot(Phase::AwaitFrame, STALE_MS + 1, "heartbeat"),
            phase_snapshot(Phase::Control, STALE_MS + 1, "heartbeat"),
            phase_snapshot(Phase::EgressSend, 0, "reader_phase"),
            expected.clone(),
        ],
    );
    assert_eq!(
        witness.reader_phase_snapshot(Phase::Control).await,
        expected
    );
}

#[tokio::test]
async fn reader_phase_snapshot_does_not_filter_the_reported_kind() {
    let witness = Witness::new(State::new());
    // A classifier bug must reach the caller's kind assertion, not disappear
    // because the snapshot selector searched for the expected classification.
    let incorrect = phase_snapshot(Phase::Control, 0, "heartbeat");
    append_snapshots(&witness, std::slice::from_ref(&incorrect));
    assert_eq!(
        witness.reader_phase_snapshot(Phase::Control).await,
        incorrect
    );
}

#[tokio::test]
async fn healthy_reader_and_heartbeat_write_no_snapshots() {
    let witness = Witness::new(State::new());
    witness.state.set_reader(Phase::AwaitFrame, 0, 0);
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert!(
        without_starvation(&witness.records()).is_empty(),
        "healthy idle connection emitted a stall: {:?}",
        witness.records()
    );
    assert!(witness.state.heartbeat_ticks.load(Ordering::Relaxed) > 1);
}

#[test]
fn parked_runtime_workers_trigger_heartbeat_snapshot() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _entered = rt.enter();
    let witness = Witness::new(State::new());
    rt.block_on(tokio::time::sleep(Duration::from_millis(30)));
    assert!(witness.state.heartbeat_ticks.load(Ordering::Relaxed) > 0);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let mut releases = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        releases.push(release_tx);
        let entered = entered_tx.clone();
        tasks.push(rt.spawn(async move {
            entered.send(()).unwrap();
            // Test-only synchronous parking occupies an async worker without any CPU load.
            let _ = release_rx.recv_timeout(Duration::from_secs(3));
        }));
    }
    for _ in 0..2 {
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while witness.records().is_empty() && std::time::Instant::now() < deadline {
        std::thread::park_timeout(Duration::from_millis(5));
    }
    let records = witness.records();
    // Release and join before assertions so a failure cannot leave the task parked.
    for release in releases {
        let _ = release.send(());
    }
    rt.block_on(async {
        for task in tasks {
            task.await.unwrap();
        }
    });
    assert!(
        !records.is_empty(),
        "parked workers did not trigger heartbeat snapshot"
    );
    assert_eq!(records[0]["kind"], "heartbeat");
    assert_eq!(records[0]["runtime"]["num_workers"], 2);
    assert_eq!(
        records[0]["runtime"]["blocking_pool"],
        "unavailable: requires tokio_unstable"
    );
    // The watchdog thread writes the record before it stores the trigger in shared state
    // (stall.rs `tick`), so the record can be visible a moment before health reports it.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while witness.state.health()["last_trigger_kind"].is_null()
        && std::time::Instant::now() < deadline
    {
        std::thread::park_timeout(Duration::from_millis(5));
    }
    assert_eq!(witness.state.health()["last_trigger_kind"], "heartbeat");
}

#[tokio::test]
async fn repeats_are_limited_and_recovery_is_emitted_once() {
    let witness = Witness::new(State::new());
    witness
        .state
        .egress
        .operation
        .store(now_ms() << 1, Ordering::Relaxed);
    witness.snapshot().await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    // Repeats are limited by spacing, read from the records' own clock: a late wake-up of this
    // test may legitimately let a second snapshot through, but never two closer than REPEAT_MS.
    let stalls = without_starvation(&witness.records());
    assert!(!stalls.is_empty());
    // A late heartbeat can add itself to a stall snapshot's triggers, so check egress is among them.
    for record in &stalls {
        assert!(
            record["triggers"]
                .as_array()
                .is_some_and(|triggers| triggers.iter().any(|trigger| trigger == "egress_write")),
            "{record}"
        );
    }
    for pair in stalls.windows(2) {
        let gap =
            pair[1]["monotonic_ms"].as_u64().unwrap() - pair[0]["monotonic_ms"].as_u64().unwrap();
        assert!(
            gap >= REPEAT_MS,
            "stall snapshots {gap} ms apart: {stalls:?}"
        );
    }
    witness.state.egress.operation.store(0, Ordering::Relaxed);
    // The watchdog thread notices the cleared operation on its own tick. Wait for that event with
    // the shared hang guard rather than a fixed 50 ms, which a busy CI runner overran.
    let recovered = |records: &[serde_json::Value]| {
        without_starvation(records)
            .last()
            .is_some_and(|record| record["kind"] == "recovery")
    };
    let deadline = std::time::Instant::now() + broca_test_support::EVENT_HANG_GUARD;
    while !recovered(&witness.records()) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let records = witness.records();
    assert!(
        recovered(&records),
        "no recovery after the egress stall: {records:?}"
    );
    // A snapshot the late heartbeat caused (alone, or alongside the egress stall, where the
    // heartbeat names the kind) may make heartbeat the last trigger; nothing else may.
    let last_trigger = witness.state.health()["last_trigger_kind"].clone();
    if last_trigger == "heartbeat" {
        // Read after the trigger: the watchdog writes a record before it stores its trigger.
        let records = witness.records();
        assert!(
            records.iter().any(|record| record["kind"] == "heartbeat"),
            "last trigger is heartbeat with no heartbeat snapshot: {records:?}"
        );
    } else {
        assert_eq!(last_trigger, "egress_write");
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    let kept = without_starvation(&witness.records());
    let recoveries = kept
        .iter()
        .filter(|record| record["kind"] == "recovery")
        .count();
    assert_eq!(recoveries, 1, "recovery must be emitted once: {kept:?}");
    assert_eq!(kept.last().unwrap()["kind"], "recovery", "{kept:?}");
}
