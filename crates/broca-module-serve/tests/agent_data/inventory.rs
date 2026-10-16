use super::*;
use std::time::Duration;

#[derive(Default)]
struct ParkDoor {
    entered: tokio::sync::Notify,
    dropped: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl SessionDoor for ParkDoor {
    async fn cover(&self, _: &str) -> Result<u64, SessionDoorError> {
        unreachable!()
    }
    async fn export(&self, _: &str, _: &[u8]) -> Result<(), SessionDoorError> {
        unreachable!()
    }
    async fn read(&self, _: &str, _: u64, _: &str) -> Result<DoorRead, SessionDoorError> {
        struct ReadGuard<'a>(&'a tokio::sync::Notify);
        impl Drop for ReadGuard<'_> {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }
        let _guard = ReadGuard(&self.dropped);
        self.entered.notify_one();
        std::future::pending().await
    }
}

async fn cancelled_agent_data_request(connection_close: bool) {
    let mut rig = Rig::new("inventory-route-cancel", Arc::new(FakeDoor::default()));
    let door = Arc::new(ParkDoor::default());
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.wal.clone(),
        rig.index.clone(),
        door.clone(),
    ));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.send(
        7,
        1,
        10,
        FrameType::Request,
        json!({"method":"agent_data.import","params":rig.params(CUT, &[rig.session("incoming")])}),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), door.entered.notified())
        .await
        .unwrap();
    if connection_close {
        // Unlike Wire::drop, close the transport without aborting the server task.
        use tokio::io::AsyncWriteExt;
        wire.stream.shutdown().await.unwrap();
    } else {
        let frame = Frame::build_with_version(2, FrameType::Goodbye, flags(), 7, 1, 11, Vec::new())
            .unwrap();
        write_frame(&mut wire.stream, &frame).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), door.dropped.notified())
        .await
        .expect("closed route must drop the in-flight agent-data handler");
    assert_nothing_staged(&rig);
}

#[tokio::test]
async fn route_goodbye_cancels_inflight_agent_data_handler() {
    cancelled_agent_data_request(false).await;
}

#[tokio::test]
async fn connection_eof_cancels_inflight_agent_data_handler() {
    cancelled_agent_data_request(true).await;
}

#[tokio::test]
async fn busy_one_ref_reads_only_its_wal_after_inventory_build() {
    let rig = Rig::new("inventory-cost", Arc::new(FakeDoor::default()));
    for i in 0..128 {
        rig.seed(
            &rig.session(&format!("session-{i}")),
            &format!("run-{i}"),
            true,
        );
    }
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let before = rig.wal.move_read_count();
    let reply = wire
        .call(
            "busy",
            json!({"refs":rig.params(CUT, &[rig.session("session-0")])["refs"]}),
        )
        .await
        .unwrap();
    assert_eq!(reply["refs"][0]["state"], "idle");
    assert_eq!(
        rig.wal.move_read_count() - before,
        1,
        "a named-ref probe must not read unrelated WALs"
    );
    wire.send(0, 0, 12, FrameType::Request, json!({"op":"health.check"}))
        .await;
    let response = wire.next().await;
    assert_eq!(response.header.ty, FrameType::Response);
    let health: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(health["metrics"]["agent_data_ops"]["busy"], 1);
}

#[tokio::test]
async fn empty_refs_do_not_start_or_read_inventory() {
    let rig = Rig::new("empty-inventory", Arc::new(FakeDoor::default()));
    rig.seed(&rig.session("broken"), "broken-run", true);
    unreadable_wal(&rig, &rig.session("broken"));
    let mut wire = Wire::without_inventory(&rig, entorhinal(), true, Value::Null).await;
    assert_eq!(
        wire.call("busy", json!({"refs":[]})).await.unwrap(),
        json!({"refs":[]})
    );
    assert_eq!(
        wire.call("diverged", json!({"cutId":CUT,"refs":[]}))
            .await
            .unwrap(),
        json!({"refs":[]})
    );
    assert_eq!(rig.wal.move_read_count(), 0);
}

#[tokio::test]
async fn incomplete_inventory_is_unknown_until_boot_build_finishes() {
    let rig = Rig::new("inventory-not-ready", Arc::new(FakeDoor::default()));
    let absent = rig.session("not-here");
    let mut wire = Wire::without_inventory(&rig, entorhinal(), true, Value::Null).await;
    let params = rig.params(CUT, &[absent]);
    let reply = wire.call("diverged", params.clone()).await.unwrap();
    assert_eq!(reply["refs"][0]["state"], "unknown");
    assert_eq!(rig.wal.move_read_count(), 0);
    rig.service.wait_inventory().await;
    let reply = wire.call("diverged", params).await.unwrap();
    assert_eq!(reply["refs"][0]["state"], "at_cut");
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unsynced_run_started_after_build_publishes_new_session() {
    let rig = Rig::new("inventory-admission", Arc::new(FakeDoor::default()));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let session = rig.session("new-session");
    let mut wal = rig.wal.open(&session).unwrap();
    wal.append(1, &started(&session, "new-run"), false).unwrap();
    let reference = rig.params(CUT, std::slice::from_ref(&session))["refs"][0]["ref"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        rig.wal
            .move_session_refs(std::iter::once(reference.as_str()))
            .sessions
            .len(),
        1
    );
    drop(wal);
    let reply = wire
        .call("busy", json!({"refs":rig.params(CUT, &[session])["refs"]}))
        .await
        .unwrap();
    assert_eq!(reply["refs"][0]["state"], "busy");
    assert_eq!(reply["refs"][0]["reason_code"], "run_active");
}

#[tokio::test]
async fn archived_ref_survives_fold_and_extraction_after_build() {
    let rig = Rig::new("inventory-fold", Arc::new(FakeDoor::default()));
    let session = rig.session("folded");
    rig.seed(&session, "folded-run", true);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let archive_dir = rig.root.join("wal-archive");
    std::fs::create_dir_all(&archive_dir).unwrap();
    let mut archive = broca_wal::archive::ArchiveWriter::create(archive_dir.join("fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    archive
        .append(&broca_core::session_addr(&session), &rig.bytes(&session))
        .unwrap();
    archive.seal().unwrap();
    std::fs::remove_file(rig.wal.wal_path(&session)).unwrap();
    let params = json!({"refs":rig.params(CUT, std::slice::from_ref(&session))["refs"]});
    assert_eq!(
        wire.call("busy", params.clone()).await.unwrap()["refs"][0]["state"],
        "idle"
    );
    assert!(!rig.wal.wal_path(&session).exists());
    drop(rig.wal.open(&session).unwrap());
    assert_eq!(
        wire.call("busy", params).await.unwrap()["refs"][0]["state"],
        "idle"
    );
    assert!(rig.wal.wal_path(&session).exists());
}

#[tokio::test]
async fn rekeyed_wal_does_not_claim_its_historical_triple() {
    let rig = Rig::new("inventory-rekey", Arc::new(FakeDoor::default()));
    let old = rig.session("old");
    let new = rig.session("new");
    rig.seed(&old, "historical-run", true);
    std::fs::rename(rig.wal.wal_path(&old), rig.wal.wal_path(&new)).unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let reply = wire
        .call("diverged", rig.params(CUT, &[old]))
        .await
        .unwrap();
    assert_eq!(reply["refs"][0]["state"], "at_cut");
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn boot_inventory_finds_eligible_identity_after_historical_runs() {
    let mut rig = Rig::new("inventory-late-identity", Arc::new(FakeDoor::default()));
    let old = rig.session("old");
    let current = rig.session("current");
    let mut wal = rig.wal.open(&current).unwrap();
    wal.append(1, &started(&old, "old-run"), true).unwrap();
    wal.append(1, &started(&current, "current-run"), true)
        .unwrap();
    drop(wal);
    // A new store has no append notifications from the seeding process.
    rig.wal = Arc::new(FileWalStore::new(rig.root.join("wal")));
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.wal.clone(),
        rig.index.clone(),
        rig.door.clone(),
    ));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("busy", json!({"refs":rig.params(CUT, &[current])["refs"]}))
            .await
            .unwrap()["refs"][0]["state"],
        "busy"
    );
    assert_eq!(
        rig.wal
            .move_session_refs(std::iter::once(
                rig.params(CUT, &[old])["refs"][0]["ref"].as_str().unwrap()
            ))
            .sessions
            .len(),
        0
    );
}

#[tokio::test]
#[ignore = "synthetic 3000-session timing experiment"]
async fn inventory_thousands_session_timing() {
    let rig = Rig::new("inventory-timing", Arc::new(FakeDoor::default()));
    for i in 0..3000 {
        rig.seed(
            &rig.session(&format!("session-{i}")),
            &format!("run-{i}"),
            true,
        );
    }
    let start = std::time::Instant::now();
    let (addresses, unreadable) = rig.wal.move_addresses().unwrap();
    assert!(unreadable.is_empty());
    let mut bytes = 0;
    for address in addresses {
        bytes += std::fs::metadata(rig.wal.root().join(format!("{address}.wal")))
            .unwrap()
            .len();
        // Reproduce the old plan's identity discovery without its move-only API.
        assert!(!rig.wal.read_address_records(&address).unwrap().is_empty());
    }
    let before_ms = start.elapsed().as_secs_f64() * 1000.0;
    let start = std::time::Instant::now();
    rig.service.wait_inventory().await;
    let build_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let params = json!({"refs":rig.params(CUT, &[rig.session("session-0")])["refs"]});
    let start = std::time::Instant::now();
    for _ in 0..10 {
        assert_eq!(
            wire.call("busy", params.clone()).await.unwrap()["refs"][0]["state"],
            "idle"
        );
    }
    eprintln!("inventory benchmark: sessions=3000 bytes={bytes} old_discovery_ms={before_ms:.3} boot_build_ms={build_ms:.3} indexed_busy_ms={:.3}", start.elapsed().as_secs_f64() * 100.0);
}
