//! Existing sessions retain their inventory root; only new scoped sessions take the stamp's
//! workdir. Either way the session is stored, and its inventory ref resolves, under the
//! address the bind triple derives.

use crate::delivery_rig;
use broca_session_store::record::Record;

use delivery_rig::*;

use std::path::{Path, PathBuf};
use std::time::Duration;

use broca_core::SessionRefIndex;
use broca_protocol::BindIdentity;
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
        identity: SubcBindIdentity::new(rig.session.harness.clone(), name),
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

/// The session `session` names is stored in the session store under its bind triple's address,
/// holds exactly the runs `run_ids`, and is what the inventory's ref resolves to. With
/// `elsewhere`, nothing is stored or referenced under the same name in that folder.
async fn assert_keyed_by_bind(
    rig: &Rig,
    session: &BindIdentity,
    run_ids: &[String],
    elsewhere: Option<&Path>,
) {
    let runtime = rig.registry().store_runtime().unwrap();
    assert!(
        broca_wal::session_storage_class(
            rig.registry().store_runtime().unwrap().state_root(),
            &broca_core::session_addr(session)
        )
        .unwrap()
        .is_some(),
        "the session is stored under the bind triple's address"
    );
    // Opening the session checks the triple its head records against `session`.
    let view = runtime
        .current_view(session)
        .await
        .unwrap()
        .expect("the session opens under its bind triple");
    let started: Vec<String> = view
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::RunStart(start) => Some(start.run_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        started, run_ids,
        "every run is stored in the bind triple's session"
    );

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

    let Some(elsewhere) = elsewhere else {
        return;
    };
    let moved = BindIdentity::new(elsewhere, session.harness.clone(), session.session.clone());
    assert!(
        broca_wal::session_storage_class(
            rig.registry().store_runtime().unwrap().state_root(),
            &broca_core::session_addr(&moved)
        )
        .unwrap()
        .is_none(),
        "nothing is stored under the stamped folder's key"
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

/// The usage facts the rig's index holds for `run_id`.
fn facts_of(rig: &Rig, run_id: &str) -> Vec<Value> {
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let mut statement = connection
        .prepare("SELECT segment_json FROM export_facts")
        .unwrap();
    let rows: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows.iter()
        .map(|row| serde_json::from_str::<Value>(row).unwrap())
        .filter(|fact| fact["run_id"] == run_id)
        .collect()
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

    let old_seat = BindIdentity::new(&root, rig.session.harness.clone(), "unscoped-seat");
    rig.store
        .record_session(&broca_core::session_addr(&old_seat), &old_seat)
        .unwrap();
    bind_at(&mut rig, 11, "unscoped-seat", &root, None, 1).await;
    let seat_first = send_and_settle(&mut rig, 11, "consult-round-1").await;
    bind_at(&mut rig, 11, "unscoped-seat", &elsewhere, None, 2).await;
    let seat_second = send_and_settle(&mut rig, 11, "consult-round-2").await;

    let new_unscoped = bind_at(
        &mut rig,
        12,
        "new-unscoped",
        Path::new("broca-unscoped-v1"),
        None,
        1,
    )
    .await;
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

    assert_keyed_by_bind(&rig, &same, &[same_run], None).await;
    assert_keyed_by_bind(&rig, &moved, &[moved_first, moved_second], Some(&elsewhere)).await;
    assert_keyed_by_bind(&rig, &new_key, std::slice::from_ref(&new_run), Some(&root)).await;
    assert_keyed_by_bind(
        &rig,
        &old_seat,
        &[seat_first, seat_second],
        Some(&elsewhere),
    )
    .await;
    assert_keyed_by_bind(
        &rig,
        &new_unscoped,
        std::slice::from_ref(&unscoped_run),
        Some(&root),
    )
    .await;
    for (label, run_id) in [("SCOPED", &new_run), ("UNSCOPED", &unscoped_run)] {
        let facts = facts_of(&rig, run_id);
        assert_eq!(facts.len(), 1, "{label}: {facts:?}");
        if label == "UNSCOPED" {
            assert!(facts[0]["session"].get("project_root").is_none());
        } else {
            assert_eq!(
                facts[0]["session"]["project_root"],
                json!(new_key.project_root)
            );
        }
        println!("FACT_SAMPLE_{label}={}", facts[0]);
    }
    for (index, old_root) in [&root, &elsewhere].into_iter().enumerate() {
        for scoped in [false, true] {
            let name = format!("legacy-{index}-{scoped}");
            let legacy = BindIdentity::new(old_root, &rig.session.harness, &name);
            rig.store
                .record_session(&broca_core::session_addr(&legacy), &legacy)
                .unwrap();
            bind_at(&mut rig, 13, &name, old_root, None, 1).await;
            let first = send_and_settle(&mut rig, 13, "first").await;
            bind_at(
                &mut rig,
                13,
                &name,
                Path::new("broca-unscoped-v1"),
                scoped.then_some(elsewhere.as_path()),
                2,
            )
            .await;
            let second = send_and_settle(&mut rig, 13, "second").await;
            assert_keyed_by_bind(
                &rig,
                &legacy,
                &[first, second],
                Some(Path::new("broca-unscoped-v1")),
            )
            .await;
        }
    }
    rig.finish().await;
}

#[tokio::test]
async fn ambiguous_and_incomplete_addresses_refuse_before_any_session_is_stored() {
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
            identity: SubcBindIdentity::new(&first.harness, name),
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
        for identity in [&first, &second] {
            assert!(
                broca_wal::session_storage_class(
                    rig.registry().store_runtime().unwrap().state_root(),
                    &broca_core::session_addr(identity)
                )
                .unwrap()
                .is_none(),
                "a refused bind stores nothing"
            );
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
            identity: SubcBindIdentity::new(&rig.session.harness, "new-after-repair"),
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
        assert!(broca_wal::session_storage_class(
            rig.registry().store_runtime().unwrap().state_root(),
            &broca_core::session_addr(&identity)
        )
        .unwrap()
        .is_none());
        rig.finish().await;
    }
}
