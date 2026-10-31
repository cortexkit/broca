//! Existing sessions retain their inventory root; only new scoped sessions take the stamp's
//! workdir. Both paths preserve the same WAL, lease, run-index and reference derivations.

use crate::delivery_rig;

use delivery_rig::*;

use std::path::{Path, PathBuf};
use std::time::Duration;

use broca_core::{LeaseStore, RunIndex, SessionRefIndex};
use broca_protocol::BindIdentity;
use broca_wal::WalRecord;
use serde_json::{json, Value};
use subc_protocol::scope::{ScopeKind, ScopeStamp};
use subc_protocol::session::{ModuleControlRequest, ModuleControlResponse};
use subc_protocol::{BindIdentity as SubcBindIdentity, FrameType, RouteTarget};

fn scope_owner() -> subc_protocol::Principal {
    subc_protocol::Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

/// Bind `channel` to session `name` under the rig's root and harness, with a scope stamp
/// naming `workdir` as the session folder.
async fn bind_stamped(rig: &mut Rig, channel: u16, name: &str, workdir: &Path) -> BindIdentity {
    let root = rig.session.project_root.clone();
    bind_at(rig, channel, name, &root, Some(workdir), 1).await
}

async fn bind_at(
    rig: &mut Rig,
    channel: u16,
    name: &str,
    route_root: &Path,
    workdir: Option<&Path>,
    epoch: u32,
) -> BindIdentity {
    let stamp = workdir.map(|workdir| {
        ScopeStamp::new(
            scope_owner(),
            format!("head-{name}"),
            u64::from(epoch),
            ScopeKind::Head,
        )
        .with_workdir(Some(workdir.to_string_lossy().into_owned()))
        .with_project_id(Some(format!("pj-{name}")))
        .with_owner_authorized(true)
    });
    let bind = ModuleControlRequest::RouteBind {
        route_channel: channel,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(route_root, rig.session.harness.clone(), name),
        principal: Some(scope_owner()),
        consumer_capabilities: None,
        admission_facts: None,
        scope: stamp,
        role_versions: None,
        call_mode: None,
    };
    let reply = rig.request(0, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(
        reply.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&reply.body)
    );
    BindIdentity::new(route_root, rig.session.harness.clone(), name)
}

/// Send `prompt` on `channel` and wait for the session to go idle; returns the run id.
async fn send_and_settle(rig: &mut Rig, channel: u16, send_id: &str) -> String {
    rig.script.push(&[Step::Text("done")]);
    let reply = rig
        .request(
            channel,
            json!({"method": "session.send", "params": {
                "prompt": "hi",
                "send_id": send_id,
                "model": {"provider": "fake", "model": "fake"},
                "tools": [],
            }}),
        )
        .await;
    let run_id = result(&reply)["run_id"]
        .as_str()
        .expect("an admitted send names its run")
        .to_owned();
    broca_test_support::wait_for(
        "scoped session becomes idle",
        async {
            loop {
                let status = rig
                    .request(channel, json!({"method": "run.status", "params": {}}))
                    .await;
                if result(&status)["state"] == "idle" {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || format!("channel {channel} never went idle"),
    )
    .await;
    run_id
}

async fn health_metrics(rig: &mut Rig) -> Value {
    let reply = rig
        .request(
            0,
            serde_json::to_value(ModuleControlRequest::HealthCheck {}).unwrap(),
        )
        .await;
    let ModuleControlResponse::HealthCheck {
        metrics: Some(metrics),
        ..
    } = serde_json::from_slice(&reply.body).unwrap()
    else {
        panic!("health.check carries metrics");
    };
    metrics
}

async fn workdir_counters(rig: &mut Rig) -> Value {
    health_metrics(rig).await["scoped_session_workdirs"].clone()
}

fn counter(counters: &Value, name: &str) -> u64 {
    counters[name]
        .as_u64()
        .unwrap_or_else(|| panic!("health.check reports {name}: {counters}"))
}

/// Everything a session is addressed by, checked against its bind triple. `elsewhere` is a
/// stamped folder that differs from the bind root: no state may appear under a key built
/// from it.
fn assert_keyed_by_bind(
    rig: &Rig,
    session: &BindIdentity,
    run_ids: &[String],
    elsewhere: Option<&Path>,
) {
    let wal = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let wal_path = wal.wal_path(session);
    assert_eq!(
        wal_path.file_stem().and_then(|stem| stem.to_str()),
        Some(broca_core::session_addr(session).as_str()),
        "the WAL is named by the bind triple's address"
    );
    let records: Vec<WalRecord> =
        broca_wal::replay_file(&wal_path, broca_wal::DEFAULT_MAX_READ_BYTES)
            .unwrap()
            .into_iter()
            .map(|frame| frame.record)
            .collect();
    let started: Vec<&BindIdentity> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { session, .. } => Some(session),
            _ => None,
        })
        .collect();
    assert_eq!(started.len(), run_ids.len(), "{records:?}");
    assert!(
        started.iter().all(|recorded| *recorded == session),
        "every episode records the bind triple: {started:?}"
    );

    for run_id in run_ids {
        let row = rig
            .store
            .lookup_run(run_id)
            .unwrap()
            .expect("the run is indexed");
        assert_eq!(
            &row.session, session,
            "the run index keys {run_id} by the bind triple"
        );
    }

    let reference = broca_core::agent_session_ref(session);
    let refs = rig
        .store
        .session_refs(std::iter::once(reference.as_str()))
        .unwrap();
    assert_eq!(
        refs.sessions.get(&reference),
        Some(&(broca_core::session_addr(session), session.clone())),
        "the session ref resolves to the bind triple's address"
    );

    // The idle session's actor still holds its writer lease, under the bind triple's key.
    let leases = broca_core::FileLeaseStore::new(rig.dir.join("leases"));
    assert!(
        matches!(
            leases.acquire(&broca_core::lease_key_for(session)),
            Err(broca_core::LeaseError::Held { .. })
        ),
        "the session's writer holds the lease keyed by the bind triple"
    );

    let Some(elsewhere) = elsewhere else {
        return;
    };
    let moved = BindIdentity::new(elsewhere, session.harness.clone(), session.session.clone());
    assert!(
        !wal.wal_path(&moved).exists(),
        "nothing is written under the stamped folder's key"
    );
    assert!(
        leases.acquire(&broca_core::lease_key_for(&moved)).is_ok(),
        "nothing holds a lease under the stamped folder's key"
    );
    assert!(
        rig.store
            .session_refs(std::iter::once(
                broca_core::agent_session_ref(&moved).as_str()
            ))
            .unwrap()
            .sessions
            .values()
            .all(|(address, _)| *address != broca_core::session_addr(&moved)),
        "no ref maps to the stamped folder's address"
    );
}

#[tokio::test]
async fn existing_sessions_keep_their_key_and_new_scoped_sessions_use_workdir() {
    let mut rig = Rig::start_with_scopes("scope-workdir").await;
    let root: PathBuf = rig.session.project_root.clone();
    let elsewhere = rig.dir.join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();

    // Keep stamped-folder sends in this test so the process-wide comparison counters have
    // no other test changing their matching/differing totals.
    let before = workdir_counters(&mut rig).await;

    let same = bind_stamped(&mut rig, 8, "stamped-same", &root).await;
    let same_run = send_and_settle(&mut rig, 8, "turn-1").await;

    let moved = bind_stamped(&mut rig, 9, "stamped-moved", &root).await;
    let moved_first = send_and_settle(&mut rig, 9, "turn-1").await;
    let legacy_bytes =
        std::fs::read(broca_core::FileWalStore::new(rig.dir.join("wal")).wal_path(&moved)).unwrap();
    bind_at(
        &mut rig,
        9,
        "stamped-moved",
        &elsewhere,
        Some(&elsewhere),
        2,
    )
    .await;
    let moved_second = send_and_settle(&mut rig, 9, "turn-2").await;

    bind_stamped(&mut rig, 10, "new-workdir", &elsewhere).await;
    let new_run = send_and_settle(&mut rig, 10, "turn-1").await;
    let new_key = BindIdentity::new(&elsewhere, rig.session.harness.clone(), "new-workdir");

    let old_seat = bind_at(&mut rig, 11, "unscoped-seat", &root, None, 1).await;
    let seat_first = send_and_settle(&mut rig, 11, "consult-round-1").await;
    bind_at(&mut rig, 11, "unscoped-seat", &elsewhere, None, 2).await;
    let seat_second = send_and_settle(&mut rig, 11, "consult-round-2").await;

    let new_unscoped = bind_at(&mut rig, 12, "new-unscoped", &elsewhere, None, 1).await;
    let unscoped_run = send_and_settle(&mut rig, 12, "turn-1").await;

    let after = workdir_counters(&mut rig).await;
    assert_eq!(
        counter(&after, "sessions_differ") - counter(&before, "sessions_differ"),
        1,
        "the differing session is counted once however many times it sends: {before} -> {after}"
    );
    assert_eq!(
        counter(&after, "sessions_same") - counter(&before, "sessions_same"),
        3,
        "the matching session is counted as matching: {before} -> {after}"
    );

    assert_keyed_by_bind(&rig, &same, &[same_run], None);
    assert_keyed_by_bind(&rig, &moved, &[moved_first, moved_second], Some(&elsewhere));
    assert_keyed_by_bind(&rig, &new_key, &[new_run], Some(&root));
    assert_keyed_by_bind(
        &rig,
        &old_seat,
        &[seat_first, seat_second],
        Some(&elsewhere),
    );
    assert_keyed_by_bind(&rig, &new_unscoped, &[unscoped_run], Some(&root));
    let resumed_bytes =
        std::fs::read(broca_core::FileWalStore::new(rig.dir.join("wal")).wal_path(&moved)).unwrap();
    assert_eq!(&resumed_bytes[..legacy_bytes.len()], legacy_bytes);
    rig.finish().await;
}

#[tokio::test]
async fn ambiguous_and_incomplete_addresses_refuse_before_wal_or_lease_creation() {
    let mut rig = Rig::start_with_scopes("address-refusal").await;
    let counters_before = health_metrics(&mut rig).await["session_address_refusals"].clone();
    let first = BindIdentity::new(
        &rig.session.project_root,
        &rig.session.harness,
        "ambiguous-pair",
    );
    let second = BindIdentity::new(
        rig.dir.join("second-root"),
        &rig.session.harness,
        "ambiguous-pair",
    );
    for identity in [&first, &second] {
        rig.store
            .record_session(&broca_core::session_addr(identity), identity)
            .unwrap();
    }
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let count = || {
        connection
            .query_row("SELECT count(*) FROM agent_data_session_ref", [], |row| {
                row.get::<_, u64>(0)
            })
            .unwrap()
    };
    let before = count();
    for (code, class) in [
        ("session_address_ambiguous", "permanent"),
        ("session_address_unavailable", "transient"),
    ] {
        if class == "transient" {
            connection
                .execute("DELETE FROM agent_data_census_done", [])
                .unwrap();
        }
        let name = if class == "transient" {
            "unrecorded-pair"
        } else {
            "ambiguous-pair"
        };
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 13,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(&first.project_root, &first.harness, name),
            principal: Some(scope_owner()),
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        let reply = rig.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(reply.header.ty, FrameType::Error);
        let error: Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(error["code"], code);
        assert_eq!(error["detail"]["class"], class);
        let reason = if class == "transient" {
            "incomplete_inventory"
        } else {
            "ambiguous"
        };
        let after = health_metrics(&mut rig).await;
        assert_eq!(
            after["session_address_refusals"][reason].as_u64().unwrap(),
            counters_before[reason].as_u64().unwrap() + 1
        );
        assert!(error["message"].as_str().unwrap().contains(name));
        assert_eq!(count(), before);
        let wal = broca_core::FileWalStore::new(rig.dir.join("wal"));
        let leases = broca_core::FileLeaseStore::new(rig.dir.join("leases"));
        for identity in [&first, &second] {
            assert!(!wal.wal_path(identity).exists());
            assert!(leases.acquire(&broca_core::lease_key_for(identity)).is_ok());
        }
    }
    rig.finish().await;
}

#[tokio::test]
async fn inventory_wait_refusals_keep_the_precise_transient_code_and_reason() {
    use broca_module::registry::SessionInventoryReadiness;
    use broca_store::session_address::{InventoryUnavailable, SessionAddressError};

    struct RefusedReadiness(&'static str);
    #[async_trait::async_trait]
    impl SessionInventoryReadiness for RefusedReadiness {
        fn start(&self) {}
        async fn wait(&self) -> Result<(), SessionAddressError> {
            Err(SessionAddressError::Unavailable(match self.0 {
                "wait_expired" => InventoryUnavailable::WaitExpired,
                "migration_failed" => {
                    InventoryUnavailable::MigrationFailed("unreadable WAL".into())
                }
                "gap_marker" => InventoryUnavailable::GapMarker,
                _ => unreachable!(),
            }))
        }
    }

    for reason in ["wait_expired", "migration_failed", "gap_marker"] {
        let mut rig = Rig::start_with_scopes(reason).await;
        rig.registry()
            .set_session_inventory_readiness(std::sync::Arc::new(RefusedReadiness(reason)));
        let conn = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
        conn.execute("DELETE FROM agent_data_census_done", [])
            .unwrap();
        let before: u64 = conn
            .query_row("SELECT count(*) FROM agent_data_session_ref", [], |row| {
                row.get(0)
            })
            .unwrap();
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 13,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(
                &rig.session.project_root,
                &rig.session.harness,
                "new-after-repair",
            ),
            principal: Some(scope_owner()),
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        let reply = rig.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(reply.header.ty, FrameType::Error);
        let error: Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(error["code"], "session_address_unavailable");
        assert_eq!(
            error["detail"],
            json!({"class":"transient", "reason":reason})
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM agent_data_session_ref", [], |row| row
                .get::<_, u64>(0))
                .unwrap(),
            before
        );
        let identity = BindIdentity::new(
            &rig.session.project_root,
            &rig.session.harness,
            "new-after-repair",
        );
        assert!(!broca_core::FileWalStore::new(rig.dir.join("wal"))
            .wal_path(&identity)
            .exists());
        rig.finish().await;
    }
}
