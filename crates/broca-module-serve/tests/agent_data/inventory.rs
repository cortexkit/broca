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
    async fn durable_offset(&self, _: &str) -> Result<u64, SessionDoorError> {
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
    let mut rig = Rig::new("inventory-route-cancel", Arc::new(FakeDoor::default())).await;
    let door = Arc::new(ParkDoor::default());
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.index.clone(),
        door.clone(),
    ));
    rig.seed(&rig.session("held"), true).await;
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
    assert_nothing_staged(&rig);
    assert!(!rig.stored(&rig.session("held")).is_empty());
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
    async fn durable_offset(&self, _: &str) -> Result<u64, SessionDoorError> {
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
    let mut rig = Rig::new("inventory-export-goodbye", Arc::new(FakeDoor::default())).await;
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.index.clone(),
        door.clone(),
    ));
    let session = rig.session("held");
    rig.seed(&session, true).await;
    let stream = rig
        .root
        .join("session-store-moves/streams")
        .join(broca_core::session_addr(&session));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let params = rig.params(CUT, &[session]);
    wire.call("prepare", params.clone()).await.unwrap();
    // Prepare asks how much the door holds when it plans, and again just before it freezes.
    assert_eq!(door.covered.load(Ordering::SeqCst), 2);
    // Export sends the stream prepare cached: the door has covered none of it.
    let expected = std::fs::read(stream).unwrap();
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
    async fn durable_offset(&self, _: &str) -> Result<u64, SessionDoorError> {
        // Only the first question parks: prepare asks again before it freezes.
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.covering.notify_one();
            self.resume.notified().await;
        }
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
    let mut rig = Rig::new("inventory-prepare-close", Arc::new(FakeDoor::default())).await;
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.index.clone(),
        door.clone(),
    ));
    let session = rig.session("held");
    rig.seed(&session, true).await;
    let path = rig.fence(&session).with_extension("moving");
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
    // Prepare asks the door how much of the session's stream it already holds before it
    // freezes the session, so nothing is frozen while the question is open.
    assert!(!path.exists());
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
    broca_test_support::wait_until(
        "prepare to freeze the session after its route closed",
        || async {
            if path.exists() {
                Ok(())
            } else {
                Err("no freeze yet".to_owned())
            }
        },
    )
    .await;
    let frozen = std::fs::read(&path).unwrap();
    let freeze: broca_records::move_fence::Freeze = serde_json::from_slice(&frozen).unwrap();
    assert_eq!(freeze.cut_id, CUT);
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
    assert_eq!(door.calls.load(Ordering::SeqCst), 2);
    assert_eq!(door.covers.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn route_goodbye_does_not_drop_prepare_between_freeze_and_cover() {
    prepare_survives_closed_route(false).await;
}

#[tokio::test]
async fn connection_eof_does_not_drop_prepare_between_freeze_and_cover() {
    prepare_survives_closed_route(true).await;
}

#[cfg(unix)]
#[tokio::test]
async fn boot_error_names_its_step_when_the_state_root_is_missing() {
    let rig = Rig::new("boot-missing-root", Arc::new(FakeDoor::default())).await;
    std::fs::rename(&rig.root, rig.root.with_extension("moved")).unwrap();
    let error = rig.service.reconcile_boot().await.unwrap_err();
    assert!(
        error.message.starts_with("rebuild_fence_index: ")
            && error.message.contains("No such file or directory"),
        "{}",
        error.message
    );
}

#[tokio::test]
async fn boot_serves_reads_but_fences_writes_when_a_cut_is_unreadable() {
    let rig = Rig::new("boot-unreadable-cut", Arc::new(FakeDoor::default())).await;
    let session = BindIdentity::new("broca-unscoped-v1", "broca", "healthy");
    rig.seed(&session, true).await;
    // A restart must discard an existing ready marker rather than trust a stale fence index.
    broca_records::move_fence::rebuild_fence_index(&rig.root).unwrap();
    let cuts = rig.root.join("agent-data/cuts");
    std::fs::create_dir_all(&cuts).unwrap();
    let damaged = cuts.join(format!("{CUT}.json"));
    std::fs::write(&damaged, b"not json").unwrap();

    rig.service.reconcile_boot().await.unwrap();
    assert_eq!(std::fs::read(damaged).unwrap(), b"not json");
    assert!(rig.registry.check_move(&session, true).is_err());
    let mut reader = Wire::new(&rig, Value::Null, false).await;
    reader.bind_session(&session).await;
    assert!(reader.management("session.read", json!({})).await.is_ok());
}

#[tokio::test]
async fn activation_registers_installed_ref_before_acknowledgment() {
    let (rig, params, _) = incoming_rig("projection-activation").await;
    let reference = params["refs"][0]["ref"].as_str().unwrap();
    assert!(rig
        .index
        .session_refs([reference].into_iter())
        .unwrap()
        .sessions
        .is_empty());
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("import", params.clone()).await.unwrap();
    assert!(
        rig.index
            .session_refs([reference].into_iter())
            .unwrap()
            .sessions
            .is_empty(),
        "staging is not a held local session"
    );
    wire.call("activate", params.clone()).await.unwrap();
    let found = rig.index.session_refs([reference].into_iter()).unwrap();
    assert!(found.complete);
    let session = &found.sessions[reference].1;
    rig.registry.check_move(session, true).unwrap();
    assert!(!rig.stored(session).is_empty());
    let reopened = rig.reopen().await;
    assert_eq!(
        reopened
            .index
            .session_refs([reference].into_iter())
            .unwrap()
            .sessions[reference]
            .1,
        *session
    );
}

/// A session a move installs while the session-ref index can't take its row is unknown, never
/// absent, until boot repairs the index; then it is found like any other.
#[tokio::test]
async fn locked_store_admission_is_unknown_until_boot_repairs_gap() {
    let (rig, params, _) = incoming_rig("store-outage").await;
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("import", params.clone()).await.unwrap();
    let blocker = rusqlite::Connection::open(rig.root.join("run-index.db")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    wire.call("activate", params.clone()).await.unwrap();
    assert!(rig
        .root
        .join(broca_core::SESSION_PROJECTION_INCOMPLETE)
        .exists());
    let busy = json!({"refs":params["refs"]});
    assert_eq!(
        wire.call("busy", busy.clone()).await.unwrap()["refs"][0]["state"],
        "unknown"
    );
    // A cut that never recorded the session finds it only through the index.
    let unrecorded = json!({"cutId":BACK,"refs":params["refs"]});
    assert_eq!(
        wire.call("diverged", unrecorded).await.unwrap()["refs"][0]["state"],
        "unknown"
    );
    blocker.execute_batch("ROLLBACK").unwrap();
    rig.service.reconcile_boot().await.unwrap();
    broca_test_support::wait_until(
        "boot census to remove the projection gap marker",
        || async {
            if rig
                .root
                .join(broca_core::SESSION_PROJECTION_INCOMPLETE)
                .exists()
            {
                Err("projection gap marker still present".to_owned())
            } else {
                Ok(())
            }
        },
    )
    .await;
    assert_eq!(
        wire.call("busy", busy).await.unwrap()["refs"][0]["state"],
        "idle"
    );
}
