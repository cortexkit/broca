use super::*;

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
            200,
            1_000,
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
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(record) = self.records().first() {
                    return record.clone();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("watchdog snapshot deadline")
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

#[tokio::test]
async fn healthy_reader_and_heartbeat_write_no_snapshots() {
    let witness = Witness::new(State::new());
    witness.state.set_reader(Phase::AwaitFrame, 0, 0);
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert!(
        witness.records().is_empty(),
        "healthy idle connection emitted a stall"
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
    // Release and join before assertions, including the intentionally red mutation run.
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
    assert_eq!(witness.records().len(), 1);
    witness.state.egress.operation.store(0, Ordering::Relaxed);
    // The watchdog thread notices the cleared operation on its own tick. Wait for that event with
    // the shared hang guard rather than a fixed 50 ms, which a busy CI runner overran.
    let deadline = std::time::Instant::now() + broca_test_support::EVENT_HANG_GUARD;
    while witness.records().len() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let records = witness.records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["kind"], "recovery");
    assert_eq!(witness.state.health()["last_trigger_kind"], "egress_write");
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(witness.records().len(), 2);
}
