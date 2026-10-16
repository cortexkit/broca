use super::*;
use std::sync::atomic::AtomicU64;
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
    rig.seed(&rig.session("held"), "run", true);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.send(
        7,
        1,
        10,
        FrameType::Request,
        json!({"method":"agent_data.diverged","params":rig.params(CUT, &[rig.session("held")])}),
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
    assert!(!rig.root.join("wal-staging").exists());
    assert!(rig.wal.wal_path(&rig.session("held")).exists());
}

#[tokio::test]
async fn route_goodbye_cancels_inflight_read_only_agent_data_handler() {
    cancelled_agent_data_request(false).await;
}

#[tokio::test]
async fn connection_eof_cancels_inflight_read_only_agent_data_handler() {
    cancelled_agent_data_request(true).await;
}

#[derive(Default)]
struct ParkExportDoor {
    covered: AtomicU64,
    exporting: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    exported: tokio::sync::Notify,
    exported_bytes: Mutex<Vec<u8>>,
}

#[async_trait::async_trait]
impl SessionDoor for ParkExportDoor {
    async fn cover(&self, _: &str) -> Result<u64, SessionDoorError> {
        self.covered.fetch_add(1, Ordering::SeqCst);
        Ok(0)
    }
    async fn export(&self, _: &str, bytes: &[u8]) -> Result<(), SessionDoorError> {
        self.exporting.notify_one();
        self.resume.notified().await;
        *self.exported_bytes.lock().unwrap() = bytes.to_vec();
        self.exported.notify_one();
        Ok(())
    }
    async fn read(&self, _: &str, _: u64, _: &str) -> Result<DoorRead, SessionDoorError> {
        unreachable!()
    }
}

async fn export_survives_closed_route(connection_close: bool) {
    let door = Arc::new(ParkExportDoor::default());
    let mut rig = Rig::new("inventory-export-goodbye", Arc::new(FakeDoor::default()));
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.wal.clone(),
        rig.index.clone(),
        door.clone(),
    ));
    let session = rig.session("held");
    rig.seed(&session, "run", true);
    let expected = rig.bytes(&session);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let params = rig.params(CUT, &[session]);
    wire.call("prepare", params.clone()).await.unwrap();
    assert_eq!(door.covered.load(Ordering::SeqCst), 1);
    wire.send(
        7,
        1,
        10,
        FrameType::Request,
        json!({"method":"agent_data.export","params":params}),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), door.exporting.notified())
        .await
        .unwrap();
    if connection_close {
        use tokio::io::AsyncWriteExt;
        wire.stream.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), &mut wire.serving)
            .await
            .expect("connection teardown must not wait on the parked mutation")
            .unwrap()
            .unwrap();
    } else {
        let goodbye =
            Frame::build_with_version(2, FrameType::Goodbye, flags(), 7, 1, 11, Vec::new())
                .unwrap();
        write_frame(&mut wire.stream, &goodbye).await.unwrap();
        // A subsequent control response proves the server processed GOODBYE before
        // the parked export is released, rather than merely writing it to a socket.
        wire.send(0, 0, 12, FrameType::Request, json!({"op":"health.check"}))
            .await;
        assert_eq!(wire.next().await.header.ty, FrameType::Response);
    }
    door.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), door.exported.notified())
        .await
        .expect("export must finish after its route closes");
    assert_eq!(*door.exported_bytes.lock().unwrap(), expected);
}

#[tokio::test]
async fn route_goodbye_does_not_drop_export_between_cover_and_export() {
    export_survives_closed_route(false).await;
}

#[tokio::test]
async fn connection_eof_does_not_drop_export_between_cover_and_export() {
    export_survives_closed_route(true).await;
}

#[derive(Default)]
struct ParkPrepareDoor {
    covering: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    completed: tokio::sync::Notify,
    calls: AtomicU64,
    covers: AtomicU64,
}

#[async_trait::async_trait]
impl SessionDoor for ParkPrepareDoor {
    async fn cover(&self, _: &str) -> Result<u64, SessionDoorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.covering.notify_one();
        self.resume.notified().await;
        self.covers.fetch_add(1, Ordering::SeqCst);
        self.completed.notify_one();
        Ok(0)
    }
    async fn export(&self, _: &str, _: &[u8]) -> Result<(), SessionDoorError> {
        unreachable!()
    }
    async fn read(&self, _: &str, _: u64, _: &str) -> Result<DoorRead, SessionDoorError> {
        unreachable!()
    }
}

async fn prepare_survives_closed_route(connection_close: bool) {
    let door = Arc::new(ParkPrepareDoor::default());
    let mut rig = Rig::new("inventory-prepare-close", Arc::new(FakeDoor::default()));
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.wal.clone(),
        rig.index.clone(),
        door.clone(),
    ));
    let session = rig.session("held");
    rig.seed(&session, "run", true);
    let path = rig.wal.wal_path(&session).with_extension("moving");
    let end_offset = rig.bytes(&session).len() as u64;
    let params = rig.params(CUT, &[session]);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.send(
        7,
        1,
        10,
        FrameType::Request,
        json!({"method":"agent_data.prepare","params":params}),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), door.covering.notified())
        .await
        .expect("prepare must reach cover");
    let frozen = std::fs::read(&path).expect("prepare must freeze the WAL before awaiting cover");
    let freeze: broca_wal::migration::Freeze = serde_json::from_slice(&frozen).unwrap();
    assert_eq!(freeze.cut_id, CUT);
    assert_eq!(freeze.end_offset, end_offset);
    assert_eq!(door.covers.load(Ordering::SeqCst), 0);
    if connection_close {
        use tokio::io::AsyncWriteExt;
        wire.stream.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), &mut wire.serving)
            .await
            .expect("connection teardown must not await parked prepare")
            .unwrap()
            .unwrap();
    } else {
        let goodbye =
            Frame::build_with_version(2, FrameType::Goodbye, flags(), 7, 1, 11, Vec::new())
                .unwrap();
        write_frame(&mut wire.stream, &goodbye).await.unwrap();
        // The health reply proves the server processed GOODBYE before we
        // release the paused cover call, not merely that the frame was sent.
        wire.send(0, 0, 12, FrameType::Request, json!({"op":"health.check"}))
            .await;
        assert_eq!(wire.next().await.header.ty, FrameType::Response);
    }
    door.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), door.completed.notified())
        .await
        .expect("prepare must finish cover after its route closes");
    assert_eq!(std::fs::read(&path).unwrap(), frozen);
    let mut retry = Wire::new(&rig, entorhinal(), true).await;
    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        retry.call("prepare", params.clone()),
    )
    .await
    .expect("lost-reply prepare must replay without another cover")
    .unwrap();
    let cut_path = rig.root.join("agent-data/cuts").join(format!("{CUT}.json"));
    let completed = std::fs::read(&cut_path).unwrap();
    let state: broca_module::cut_state::CutState = serde_json::from_slice(&completed).unwrap();
    assert!(state.intent.is_none());
    assert_eq!(state.replies["agent_data.prepare"].value, reply);
    let replay = tokio::time::timeout(Duration::from_secs(5), retry.call("prepare", params))
        .await
        .expect("repeated prepare must remain idempotent")
        .unwrap();
    assert_eq!(replay, reply);
    assert_eq!(std::fs::read(cut_path).unwrap(), completed);
    assert_eq!(std::fs::read(path).unwrap(), frozen);
    assert_eq!(door.calls.load(Ordering::SeqCst), 1);
    assert_eq!(door.covers.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn route_goodbye_does_not_drop_prepare_between_freeze_and_cover() {
    prepare_survives_closed_route(false).await;
}

#[tokio::test]
async fn connection_eof_does_not_drop_prepare_between_freeze_and_cover() {
    prepare_survives_closed_route(true).await;
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
    assert_eq!(
        health["metrics"]["agent_data_inventory"],
        json!({"ready":true,"unreadable":0})
    );
}

#[tokio::test]
async fn diverged_held_ref_reads_one_local_snapshot_live_or_archived() {
    for archived in [false, true] {
        let rig = Rig::new(
            if archived {
                "diverged-one-archive-read"
            } else {
                "diverged-one-live-read"
            },
            Arc::new(FakeDoor::default()),
        );
        let session = rig.session("held");
        rig.seed(&session, "run", true);
        let bytes = rig.bytes(&session);
        let reference = contract::session_ref(
            &session.harness,
            &session.project_root.to_string_lossy(),
            &session.session,
        );
        rig.door.plant(CUT, &reference, bytes.clone());
        if archived {
            let directory = rig.root.join("wal-archive");
            std::fs::create_dir_all(&directory).unwrap();
            let mut writer =
                broca_wal::archive::ArchiveWriter::create(directory.join("fold-1.ark"))
                    .unwrap()
                    .without_feature_floor();
            writer
                .append(&broca_core::session_addr(&session), &bytes)
                .unwrap();
            writer.seal().unwrap();
            std::fs::remove_file(rig.wal.wal_path(&session)).unwrap();
        }
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let before = rig.wal.move_read_count();
        let reply = wire
            .call("diverged", rig.params(CUT, &[session]))
            .await
            .unwrap();
        assert_eq!(reply["refs"][0]["state"], "at_cut");
        assert_eq!(
            rig.wal.move_read_count() - before,
            1,
            "retention classification and reply must reuse the same local snapshot"
        );
    }
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
    wire.send(0, 0, 12, FrameType::Request, json!({"op":"health.check"}))
        .await;
    let health: Value = serde_json::from_slice(&wire.next().await.body).unwrap();
    assert_eq!(
        health["metrics"]["agent_data_inventory"],
        json!({"ready":false,"unreadable":0})
    );
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
