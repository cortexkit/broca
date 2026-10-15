use super::*;
use crate::cut_state::CutStore;
use crate::registry::{ActorHandle, LineageKey, LineageOpener, OpenError, RegistryHandle};
use broca_core::WalStore;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
const CUT: &str = "0123456789abcdef0123456789abcdef";
struct NeverOpen;
#[async_trait::async_trait]
impl LineageOpener for NeverOpen {
    async fn open(
        &self,
        _key: &LineageKey,
        _gen: u64,
        _registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        panic!("move opened a session actor")
    }
}
struct Driver {
    plans: Vec<PlannedSession>,
    fail_reply: AtomicBool,
    fail_second: AtomicBool,
    second: PathBuf,
}
#[async_trait::async_trait]
impl MoveDriver for Driver {
    async fn plan(
        &self,
        _cut_id: &str,
        refs: &Refs,
        op: Op,
    ) -> Result<Vec<PlannedSession>, ErrorBody> {
        tokio::task::yield_now().await;
        let mut plans = Vec::new();
        for reference in refs.iter() {
            match self.plans.iter().find(|p| p.reference == reference.r#ref) {
                Some(plan) => plans.push(plan.clone()),
                None if op == Op::Abort => {}
                None => return Err(refusal("unknown_ref", "unknown session", json!(reference))),
            }
        }
        Ok(plans)
    }
    async fn cover(&self, _ref: &str) -> Result<u64, ErrorBody> {
        Ok(0)
    }
    async fn export(&self, _ref: &str, _frozen: &[u8]) -> Result<(), ErrorBody> {
        Ok(())
    }
    fn reply(
        &self,
        op: Op,
        _plans: &[PlannedSession],
        _offsets: &[u64],
        observations: &[MoveObservation],
    ) -> Result<Value, ErrorBody> {
        if op == Op::Busy {
            return Ok(
                json!({"refs": observations.iter().map(|o| json!({"state":if o.busy_reason.is_some() {"busy"} else {"idle"}})).collect::<Vec<_>>()}),
            );
        }
        if self.fail_reply.load(Ordering::SeqCst) {
            Err(unavailable("reply checkpoint failure"))
        } else {
            Ok(json!({"enforced":true}))
        }
    }
    fn drop_projection(&self, _session: &BindIdentity) -> Result<(), String> {
        if self.fail_second.load(Ordering::SeqCst) && !self.second.exists() {
            std::fs::create_dir(&self.second).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
struct Rig {
    root: PathBuf,
    wal: Arc<FileWalStore>,
    store: CutStore,
    artifacts: FileCutArtifacts,
    driver: Arc<Driver>,
    params: Value,
}
impl Rig {
    fn new(tag: &str, source: bool) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("file-cut-{}-{tag}", std::process::id()));
        migration::durable_dir(&root.join("wal")).unwrap();
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let fixture = FileWalStore::new(root.join("incoming"));
        let mut plans = Vec::new();
        for name in ["a", "b"] {
            let session = BindIdentity::new("/p", "runner", name);
            {
                let mut writer = fixture.open(&session).unwrap();
                if tag == "source-rekey-no-run" {
                    let historical = BindIdentity::new("/before", "runner", name);
                    crate::move_install::tests::episode(writer.as_mut(), &historical, name);
                } else if tag == "source-busy" {
                    writer
                        .append(
                            1,
                            &crate::move_install::tests::started(&session, name),
                            true,
                        )
                        .unwrap();
                } else {
                    crate::move_install::tests::episode(writer.as_mut(), &session, name);
                }
            }
            let bytes = std::fs::read(fixture.wal_path(&session)).unwrap();
            let lineage = broca_wal::read_records_range(
                fixture.wal_path(&session),
                broca_wal::DEFAULT_MAX_READ_BYTES,
                0,
                None,
                None,
            )
            .unwrap()
            .lineage_id
            .unwrap()
            .to_string();
            if source {
                std::fs::write(wal.wal_path(&session), &bytes).unwrap();
            }
            plans.push(PlannedSession {
                reference: broca_wire::agent_data::session_ref("runner", "/p", name),
                session,
                end_offset: bytes.len() as u64,
                sealed_digest: if tag == "missing-digest" {
                    None
                } else {
                    Some(migration::digest(&bytes))
                },
                digest: migration::digest(&bytes),
                bytes,
                format: "broca-wal/v3;features=".into(),
                lineage_id: lineage,
                local_archive: None,
            });
        }
        let params = json!({"cutId":CUT,"refs":plans.iter().map(|p|json!({"ref_kind":"broca-session","ref":p.reference})).collect::<Vec<_>>()});
        let driver = Arc::new(Driver {
            second: wal.wal_path(&plans[1].session),
            plans,
            fail_reply: AtomicBool::new(false),
            fail_second: AtomicBool::new(false),
        });
        let registry = LineageRegistry::new_with_reader(Arc::new(NeverOpen), wal.clone(), None);
        let export = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
        let artifacts = FileCutArtifacts::new(registry, wal.clone(), export, driver.clone());
        let store = CutStore::new(&root);
        Self {
            root,
            wal,
            store,
            artifacts,
            driver,
            params,
        }
    }
    async fn op(&self, op: Op) -> Result<Value, ErrorBody> {
        self.store
            .execute(
                broca_wire::agent_data::ADMITTED_CALLER,
                op.method(),
                &self.params,
                &self.artifacts,
            )
            .await
    }
}

#[tokio::test]
async fn two_cut_ids_cannot_replace_or_abort_the_others_freeze_across_restart() {
    let rig = Rig::new("two-cuts", true);
    rig.op(Op::Prepare).await.unwrap();
    let path = rig
        .wal
        .wal_path(&rig.driver.plans[0].session)
        .with_extension("moving");
    let before = std::fs::read(&path).unwrap();
    let mut other = rig.params.clone();
    other["cutId"] = json!("fedcba9876543210fedcba9876543210");
    let after_restart = CutStore::new(&rig.root);
    let (a, b) = futures::join!(
        after_restart.execute(
            broca_wire::agent_data::ADMITTED_CALLER,
            Op::Prepare.method(),
            &other,
            &rig.artifacts
        ),
        rig.op(Op::Prepare)
    );
    assert_eq!(a.unwrap_err().code.as_str(), "busy");
    b.unwrap();
    after_restart
        .execute(
            broca_wire::agent_data::ADMITTED_CALLER,
            Op::Abort.method(),
            &other,
            &rig.artifacts,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(path).unwrap(), before);
    for plan in &rig.driver.plans {
        assert!(matches!(
            rig.wal.open(&plan.session),
            Err(broca_wal::WalError::SessionMoving { .. })
        ));
    }
}

#[tokio::test]
async fn real_filesystem_activation_partial_failure_is_fenced_until_both_refs_recover() {
    let rig = Rig::new("partial-activate", false);
    rig.op(Op::Import).await.unwrap();
    rig.driver.fail_second.store(true, Ordering::SeqCst);
    let (activate, abort) = futures::join!(rig.op(Op::Activate), rig.op(Op::Abort));
    assert_eq!(activate.unwrap_err().code.as_str(), "unavailable");
    assert_eq!(abort.unwrap_err().code.as_str(), "invalid_state");
    let first = &rig.driver.plans[0];
    let path = rig.wal.wal_path(&first.session);
    assert_eq!(std::fs::read(&path).unwrap(), first.bytes);
    assert!(matches!(
        rig.wal.open(&first.session),
        Err(broca_wal::WalError::SessionMoving { .. })
    ));
    let before = std::fs::read(rig.store.path(CUT)).unwrap();
    assert_eq!(
        rig.op(Op::Abort).await.unwrap_err().code.as_str(),
        "invalid_state"
    );
    rig.op(Op::Status).await.unwrap();
    assert_eq!(std::fs::read(rig.store.path(CUT)).unwrap(), before);
    std::fs::remove_dir(&rig.driver.second).unwrap();
    rig.driver.fail_second.store(false, Ordering::SeqCst);
    let replay = rig.op(Op::Activate).await.unwrap();
    assert_eq!(rig.op(Op::Activate).await.unwrap(), replay);
    for plan in &rig.driver.plans {
        assert_eq!(
            std::fs::read(rig.wal.wal_path(&plan.session)).unwrap(),
            plan.bytes
        );
        rig.wal.open(&plan.session).unwrap();
    }
}

#[tokio::test]
async fn installed_wal_without_recorded_activation_stays_fenced_on_reader_release() {
    let rig = Rig::new("last-rename", false);
    rig.op(Op::Import).await.unwrap();
    rig.driver.fail_reply.store(true, Ordering::SeqCst);
    assert_eq!(
        rig.op(Op::Activate).await.unwrap_err().code.as_str(),
        "unavailable"
    );
    let before = std::fs::read(rig.store.path(CUT)).unwrap();
    let downgraded = FileWalStore::new(rig.root.join("wal"));
    for plan in &rig.driver.plans {
        let path = downgraded.wal_path(&plan.session);
        let bytes = std::fs::read(&path).unwrap();
        assert!(!path.with_extension("moving").exists());
        assert!(!path.with_extension("moved").exists());
        assert!(matches!(
            downgraded.open(&plan.session),
            Err(broca_wal::WalError::SessionMoving { .. })
        ));
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    rig.op(Op::Status).await.unwrap();
    assert_eq!(std::fs::read(rig.store.path(CUT)).unwrap(), before);
    rig.driver.fail_reply.store(false, Ordering::SeqCst);
    rig.store.reconcile_boot(&rig.artifacts).await.unwrap();
    for plan in &rig.driver.plans {
        downgraded.open(&plan.session).unwrap();
    }
}

struct NeverExecute;
#[async_trait::async_trait]
impl crate::executor::RunExecutor for NeverExecute {
    async fn execute(
        &self,
        _job: crate::executor::RunJob,
        _out: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        panic!("reader guard started execution")
    }
}

#[tokio::test]
async fn actor_critical_section_holds_dispatch_until_completion_and_then_refuses_warm() {
    let root =
        broca_test_support::scratch_root().join(format!("move-actor-{}", std::process::id()));
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let opener = Arc::new(crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        wal.clone(),
        Arc::new(NeverExecute),
    ));
    let registry = LineageRegistry::new_with_reader(opener, wal.clone(), None);
    let session = BindIdentity::new("/p", "runner", "idle");
    let handle = registry.actor_for(&session).await.unwrap();
    let guard = registry
        .lock_move_sessions(std::slice::from_ref(&session), false)
        .await
        .unwrap();
    assert_eq!(guard.busy[0].1, None);
    let (reply, mut wait) = tokio::sync::oneshot::channel();
    handle
        .send(crate::actor::LineageCmd::Status { reply })
        .await
        .unwrap_or_else(|_| panic!("actor closed"));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut wait)
            .await
            .is_err(),
        "actor dispatched while cut completion was not durable"
    );
    let path = wal.wal_path(&session);
    let before = std::fs::read(&path).unwrap();
    migration::freeze(
        &path,
        &Freeze {
            cut_id: CUT.into(),
            end_offset: before.len() as u64,
            lineage_id: "lineage".into(),
        },
    )
    .unwrap()
    .unwrap();
    drop(guard);
    assert_eq!(wait.await.unwrap(), crate::actor::LineageStatus::Idle);
    let (reply, wait) = tokio::sync::oneshot::channel();
    handle
        .send(crate::actor::LineageCmd::Warm {
            lineage_id: None,
            reply,
        })
        .await
        .unwrap_or_else(|_| panic!("actor closed"));
    assert_eq!(
        wait.await.unwrap(),
        crate::actor::WarmOutcome::SessionMoving { cut_id: CUT.into() }
    );
    for delivery in [
        broca_protocol::Delivery::Queue,
        broca_protocol::Delivery::Steer,
        broca_protocol::Delivery::Interrupt,
    ] {
        let (reply, wait) = tokio::sync::oneshot::channel();
        let request = serde_json::from_value(
            json!({"model":broca_protocol::ModelHandle::new("stub","m"),"input":[]}),
        )
        .unwrap();
        handle
            .send(crate::actor::LineageCmd::Send {
                retention: None,
                stop_sequences_named: false,
                conversation_key: None,
                title: None,
                run_id: broca_protocol::RunId("new".into()),
                request: Box::new(request),
                kind: crate::executor::RunKind::FreshOrResume,
                key: crate::actor::SendKey::RunId,
                skip_materialization: false,
                delivery,
                plan: None,
                reply,
            })
            .await
            .unwrap_or_else(|_| panic!("actor closed"));
        assert_eq!(
            wait.await.unwrap(),
            crate::actor::SendOutcome::SessionMoving { cut_id: CUT.into() }
        );
    }
    let (reply, wait) = tokio::sync::oneshot::channel();
    handle
        .send(crate::actor::LineageCmd::Cancel {
            run_id: broca_protocol::RunId("new".into()),
            reply,
        })
        .await
        .unwrap_or_else(|_| panic!("actor closed"));
    assert_eq!(
        wait.await.unwrap(),
        crate::actor::CancelOutcome::SessionMoving { cut_id: CUT.into() }
    );
    let (reply, wait) = tokio::sync::oneshot::channel();
    handle
        .send(crate::actor::LineageCmd::Retract {
            submission_id: broca_protocol::SubmissionId("new".into()),
            reply,
        })
        .await
        .unwrap_or_else(|_| panic!("actor closed"));
    assert_eq!(
        wait.await.unwrap(),
        crate::actor::RetractOutcome::SessionMoving { cut_id: CUT.into() }
    );
    let (reply, wait) = tokio::sync::oneshot::channel();
    let request = broca_core::ImportRequest {
        project_id: None,
        principal: None,
        run_id: broca_protocol::RunId("import".into()),
        session: session.clone(),
        messages: vec![broca_protocol::content::Message::new(
            broca_protocol::content::Role::User,
            vec![broca_protocol::content::ContentBlock::Text {
                text: "hello".into(),
            }],
        )],
        origin: broca_core::ImportOrigin {
            source_ref: "copy".into(),
        },
    };
    handle
        .send(crate::actor::LineageCmd::Import {
            request: Box::new(request),
            reply,
        })
        .await
        .unwrap_or_else(|_| panic!("actor closed"));
    assert!(
        matches!(wait.await.unwrap(),crate::actor::ImportOutcome::SessionMoving {cut_id} if cut_id == CUT)
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(registry.stop_and_seal().await);
}

#[tokio::test]
async fn source_rekey_without_current_run_started_is_refused_before_freezing() {
    let rig = Rig::new("source-rekey-no-run", true);
    let error = rig.op(Op::Prepare).await.unwrap_err();
    assert_eq!(error.code.as_str(), "invalid_request");
    assert!(error.message.contains(&rig.driver.plans[0].reference));
    for plan in &rig.driver.plans {
        assert!(!rig
            .wal
            .wal_path(&plan.session)
            .with_extension("moving")
            .exists());
    }
    assert!(!rig.store.path(CUT).exists());
}

#[tokio::test]
async fn missing_sealed_digest_stages_nothing_and_stores_no_intent() {
    let rig = Rig::new("missing-digest", false);
    let error = rig.op(Op::Import).await.unwrap_err();
    assert_eq!(error.code.as_str(), "unavailable");
    assert!(error.message.contains("sealed digest"));
    assert!(!rig.root.join("wal-staging").exists());
    assert!(!rig.store.path(CUT).exists());
}

#[tokio::test]
async fn source_unfinished_run_is_busy_before_any_freeze() {
    let rig = Rig::new("source-busy", true);
    assert_eq!(rig.op(Op::Prepare).await.unwrap_err().code.as_str(), "busy");
    for plan in &rig.driver.plans {
        assert!(!rig
            .wal
            .wal_path(&plan.session)
            .with_extension("moving")
            .exists());
    }
    assert!(!rig.store.path(CUT).exists());
}

#[tokio::test]
async fn receiver_open_actor_refuses_the_entire_set_before_any_staging() {
    let mut rig = Rig::new("receiver-busy", false);
    for plan in &rig.driver.plans {
        std::fs::write(rig.wal.wal_path(&plan.session), &plan.bytes).unwrap();
    }
    let opener = Arc::new(crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(rig.root.join("leases"))),
        rig.wal.clone(),
        Arc::new(NeverExecute),
    ));
    rig.artifacts.registry = LineageRegistry::new_with_reader(opener, rig.wal.clone(), None);
    rig.artifacts
        .registry
        .actor_for(&rig.driver.plans[1].session)
        .await
        .unwrap();
    let error = rig.op(Op::Import).await.unwrap_err();
    assert_eq!(error.code.as_str(), "busy");
    assert!(!rig.root.join("wal-staging").exists());
    assert!(!rig.store.path(CUT).exists());
    assert!(rig.artifacts.registry.stop_and_seal().await);
}

#[tokio::test]
async fn abort_recovery_of_identical_hidden_wal_never_becomes_activation() {
    let rig = Rig::new("abort-hidden", false);
    for plan in &rig.driver.plans {
        let live = rig.wal.wal_path(&plan.session);
        std::fs::write(&live, &plan.bytes).unwrap();
        migration::atomic_json(
            &live.with_extension("moved"),
            &Moved {
                cut_id: "old-cut".into(),
                holder: "remote".into(),
                lineage_id: plan.lineage_id.clone(),
            },
        )
        .unwrap();
    }
    rig.op(Op::Import).await.unwrap();
    let path = rig.store.path(CUT);
    let mut state: Value = migration::read_json(&path).unwrap().unwrap();
    state["intent"] = json!({"op":"abort","request":rig.params});
    migration::atomic_json(&path, &state).unwrap();
    migration::unlink(&rig.artifacts.staged(CUT, &rig.driver.plans[0]).unwrap()).unwrap();
    rig.store.reconcile_boot(&rig.artifacts).await.unwrap();
    let state: Value = migration::read_json(&path).unwrap().unwrap();
    assert!(state["intent"].is_null());
    assert!(state["replies"].get(Op::Abort.method()).is_some());
    assert!(state["replies"].get(Op::Activate.method()).is_none());
    for plan in &rig.driver.plans {
        assert!(rig
            .wal
            .wal_path(&plan.session)
            .with_extension("moved")
            .exists());
        assert!(!rig.artifacts.staged(CUT, plan).unwrap().exists());
    }
}

#[tokio::test]
async fn busy_without_cut_id_reads_held_refs_and_refuses_first_unknown() {
    let rig = Rig::new("busy-no-cut-id", true);
    let params = json!({"refs":rig.params["refs"]});
    let reply = rig
        .store
        .execute(
            "reserved:entorhinal",
            "agent_data.busy",
            &params,
            &rig.artifacts,
        )
        .await
        .unwrap();
    assert_eq!(reply, json!({"refs":[{"state":"idle"},{"state":"idle"}]}));
    assert!(!rig.store.path(CUT).exists());
    let unknown = json!({"ref_kind":"broca-session","ref":"ffffffffffffffffffffffffffffffff"});
    let mixed = json!({"refs":[rig.params["refs"][0],unknown]});
    let error = rig
        .store
        .execute(
            "reserved:entorhinal",
            "agent_data.busy",
            &mixed,
            &rig.artifacts,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code.as_str(), "unknown_ref");
    assert_eq!(serde_json::to_value(error).unwrap()["detail"], unknown);
}

#[tokio::test]
async fn abort_unseen_cut_with_unknown_refs_is_durable_and_replayed() {
    let rig = Rig::new("abort-unknown", false);
    let params = json!({"cutId":CUT,"refs":[{"ref_kind":"broca-session","ref":"ffffffffffffffffffffffffffffffff"}]});
    let first = rig
        .store
        .execute(
            "reserved:entorhinal",
            "agent_data.abort",
            &params,
            &rig.artifacts,
        )
        .await
        .unwrap();
    let reopened = CutStore::new(&rig.root);
    assert_eq!(
        reopened
            .execute(
                "reserved:entorhinal",
                "agent_data.status",
                &params,
                &rig.artifacts
            )
            .await
            .unwrap()["step"],
        "aborted"
    );
    assert_eq!(
        reopened
            .execute(
                "reserved:entorhinal",
                "agent_data.abort",
                &params,
                &rig.artifacts
            )
            .await
            .unwrap(),
        first
    );
}

#[tokio::test]
async fn abort_mixed_refs_leaves_another_cuts_freeze_untouched() {
    let rig = Rig::new("abort-mixed", true);
    rig.store
        .execute(
            "reserved:entorhinal",
            "agent_data.prepare",
            &rig.params,
            &rig.artifacts,
        )
        .await
        .unwrap();
    let moving = rig
        .wal
        .wal_path(&rig.driver.plans[0].session)
        .with_extension("moving");
    let before = std::fs::read(&moving).unwrap();
    let params = json!({"cutId":"ffffffffffffffffffffffffffffffff","refs":[rig.params["refs"][0],{"ref_kind":"broca-session","ref":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}]});
    rig.store
        .execute(
            "reserved:entorhinal",
            "agent_data.abort",
            &params,
            &rig.artifacts,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(moving).unwrap(), before);
}

#[tokio::test]
async fn source_export_and_release_lease_contention_use_retryable_unavailable() {
    let rig = Rig::new("source-lease", true);
    rig.op(Op::Prepare).await.unwrap();
    let leases = FileLeaseStore::new(rig.root.join("leases"));
    let held = leases
        .acquire(&broca_core::lease_key_for(&rig.driver.plans[1].session))
        .unwrap();
    let error = rig.op(Op::Export).await.unwrap_err();
    assert_eq!(error.code.as_str(), "unavailable");
    assert!(broca_wire::agent_data::errors::driver_action_for(Op::Export, &error.code).retryable());
    drop(held);
    rig.op(Op::Export).await.unwrap();
    let held = leases
        .acquire(&broca_core::lease_key_for(&rig.driver.plans[1].session))
        .unwrap();
    let mut params = rig.params.clone();
    params["holder"] = json!("receiver");
    let error = rig
        .store
        .execute(
            "reserved:entorhinal",
            "agent_data.release",
            &params,
            &rig.artifacts,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code.as_str(), "unavailable");
    assert!(
        broca_wire::agent_data::errors::driver_action_for(Op::Release, &error.code).retryable()
    );
    drop(held);
    rig.store
        .execute(
            "reserved:entorhinal",
            "agent_data.release",
            &params,
            &rig.artifacts,
        )
        .await
        .unwrap();
}
