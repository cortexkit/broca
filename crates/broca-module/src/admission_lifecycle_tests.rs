struct HeldAdmission {
    live: Arc<AtomicBool>,
    started: Semaphore,
    release: tokio::sync::Notify,
}
struct AdmissionTaskGuard(Arc<AtomicBool>);
impl Drop for AdmissionTaskGuard {
    fn drop(&mut self) { self.0.store(false, Ordering::SeqCst); }
}
#[async_trait]
impl RunExecutor for HeldAdmission {
    async fn execute(&self, _job: RunJob, _out: mpsc::Sender<OutputEvent>) -> Result<RunSummary, RunError> {
        assert!(!self.live.swap(true, Ordering::SeqCst), "admission attempts must not overlap");
        let _guard = AdmissionTaskGuard(self.live.clone());
        self.started.add_permits(1);
        self.release.notified().await;
        Err(RunError::Config("dependency refused admission".into()))
    }
}

fn seed_admission_episode(wal: &FlakyReadWalStore, key: &BindIdentity, now: u64, restart: bool) {
    let mut writer = if restart { wal.inner.open(key).unwrap() }
        else { wal.inner.open_with_retention(key, Some(1)).unwrap() };
    let mut record: WalRecord = serde_json::from_str(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
    )).unwrap();
    if let WalRecord::RunStarted { session, run_id, retention, ts_ms, config, on_restart, .. } = &mut record {
        *session = key.clone();
        *run_id = RunId("prior".into());
        *retention = (!restart).then_some(1);
        *ts_ms = Some(now);
        **config = min_config();
        *on_restart = restart.then_some(broca_protocol::request::OnRestart::Resume);
    }
    writer.append(1, &record, true).unwrap();
    let end = if restart {
        WalRecord::RunPaused { run_id: RunId("prior".into()), ts_ms: Some(now),
            reason: broca_protocol::PauseReason::Restart, error: None }
    } else {
        WalRecord::RunFinished { ts_ms: Some(now), reason: RunFinishReason::Completed,
            usage: Usage::default(), error: None, retries_used: 0 }
    };
    writer.append(1, &end, true).unwrap();
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn expiry_read_failure_joins_nonrestart_admission_before_next_attempt() {
    let dir = broca_test_support::scratch_root().join(format!("admission-expiry-read-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = BindIdentity::new(dir.join("project"), "runner", "session");
    let wal = Arc::new(FlakyReadWalStore::new(dir.join("wal")));
    let leases = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("index.db")).unwrap());
    let retention = Arc::new(crate::retention::RetentionService::new(
        Arc::new(FileWalStore::new(dir.join("wal"))), store.clone(), leases.clone(), store,
        Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
    ).unwrap());
    seed_admission_episode(&wal, &key, retention.clock.now_ms(), false);
    let executor = Arc::new(HeldAdmission { live: Arc::new(AtomicBool::new(false)),
        started: Semaphore::new(0), release: tokio::sync::Notify::new() });
    let registry = LineageRegistry::new(Arc::new(crate::opener::DefaultOpener::new(
        leases, wal.clone(), executor.clone(),
    ).with_idle_ttl(Duration::from_secs(60))));
    registry.set_retention_service(retention.clone());
    registry.route_status(&key).await.unwrap();
    let send = tokio::spawn({
        let registry = registry.clone(); let key = key.clone();
        async move { registry.route_send(&key, text_req(&key, "new"), RunKind::AppendEpisode).await }
    });
    executor.started.acquire().await.unwrap().forget();
    wal.fail_next_read();
    std::fs::write(dir.join("test-clock"), br#"{"offset_ms":2000}"#).unwrap();
    retention.clock.refresh().unwrap();
    assert!(matches!(tokio::time::timeout(Duration::from_secs(15), send).await.unwrap().unwrap().unwrap(), SendOutcome::Error(_)));
    assert!(!executor.live.load(Ordering::SeqCst), "failed expiry read must join the admission task before replying");
    // A new attempt may only start after the previous executor has dropped its guard.
    let next = tokio::spawn({
        let registry = registry.clone(); let key = key.clone();
        async move { registry.route_send(&key, text_req(&key, "next"), RunKind::FreshOrResume).await }
    });
    tokio::time::timeout(Duration::from_secs(15), executor.started.acquire()).await.unwrap().unwrap().forget();
    executor.release.notify_one();
    let _ = tokio::time::timeout(Duration::from_secs(15), next).await.unwrap().unwrap();
    let _ = registry.stop_and_seal().await;
    let _ = std::fs::remove_dir_all(dir);
}

async fn restart_waiter_fixture(name: &str) -> (
    std::path::PathBuf, BindIdentity, Arc<FlakyReadWalStore>, Arc<HeldAdmission>,
    LineageRegistry, tokio::task::JoinHandle<(usize, usize)>,
) {
    let dir = broca_test_support::scratch_root().join(format!("restart-waiter-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = BindIdentity::new(dir.join("project"), "runner", "session");
    let wal = Arc::new(FlakyReadWalStore::new(dir.join("wal")));
    seed_admission_episode(&wal, &key, 100, true);
    let executor = Arc::new(HeldAdmission { live: Arc::new(AtomicBool::new(false)),
        started: Semaphore::new(0), release: tokio::sync::Notify::new() });
    let registry = LineageRegistry::new(Arc::new(crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))), wal.clone(), executor.clone(),
    ).with_idle_ttl(Duration::from_secs(60))));
    let resume = tokio::spawn({
        let registry = registry.clone(); let wal = wal.clone(); let key = key.clone();
        async move { registry.resume_restart_pauses(wal, vec![("prior".into(), key)]).await }
    });
    tokio::time::timeout(Duration::from_secs(15), executor.started.acquire()).await.unwrap().unwrap().forget();
    (dir, key, wal, executor, registry, resume)
}

async fn enqueue_restart_waiter(registry: &LineageRegistry, key: &BindIdentity) -> oneshot::Receiver<Result<(), String>> {
    let actor = registry.actor_for(key).await.unwrap();
    let (tx, rx) = oneshot::channel();
    assert!(actor.send(LineageCmd::ResumeRestart { reply: tx }).await.is_ok());
    let (status_tx, status_rx) = oneshot::channel();
    assert!(actor.send(LineageCmd::Status { reply: status_tx }).await.is_ok());
    tokio::time::timeout(Duration::from_secs(15), status_rx).await.unwrap().unwrap();
    rx
}

#[tokio::test]
async fn queued_restart_waiter_receives_explicit_error_on_shutdown() {
    let (dir, key, _wal, executor, registry, resume) = restart_waiter_fixture("shutdown").await;
    let waiter = enqueue_restart_waiter(&registry, &key).await;
    let stop = tokio::spawn({ let registry = registry.clone(); async move { registry.stop_and_seal().await } });
    assert!(tokio::time::timeout(Duration::from_secs(15), waiter).await.unwrap()
        .expect("waiter must receive a result rather than a dropped sender").is_err());
    let _ = tokio::time::timeout(Duration::from_secs(15), stop).await.unwrap().unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(15), resume).await.unwrap().unwrap();
    assert!(!executor.live.load(Ordering::SeqCst));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn queued_restart_waiter_receives_explicit_error_on_failed_seal() {
    let (dir, key, wal, executor, registry, resume) = restart_waiter_fixture("failed-seal").await;
    let waiter = enqueue_restart_waiter(&registry, &key).await;
    wal.set_fail_reads(true);
    executor.release.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(15), waiter).await.unwrap()
        .expect("failed sealing must explicitly resolve the restart waiter").is_err());
    assert_eq!(registry.slot_state(&key), Some("open"));
    wal.set_fail_reads(false);
    let _ = registry.stop_and_seal().await;
    let _ = tokio::time::timeout(Duration::from_secs(15), resume).await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
