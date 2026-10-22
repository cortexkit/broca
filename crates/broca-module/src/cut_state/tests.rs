use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
const CUT: &str = "0123456789abcdef0123456789abcdef";
fn root(tag: &str) -> PathBuf {
    let root =
        broca_test_support::scratch_root().join(format!("cut-state-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    root
}
fn params() -> Value {
    json!({"cutId":CUT,"refs":[{"ref_kind":"broca-session","ref":"a"},{"ref_kind":"broca-session","ref":"b"}]})
}

struct Artifacts {
    root: PathBuf,
    fail_after: AtomicUsize,
    applied: AtomicUsize,
    calls: AtomicUsize,
    fail_b: AtomicBool,
    locks: AtomicUsize,
}
impl Artifacts {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            fail_after: AtomicUsize::new(usize::MAX),
            applied: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            fail_b: AtomicBool::new(false),
            locks: AtomicUsize::new(0),
        }
    }
    fn staged(&self, reference: &str) -> PathBuf {
        broca_wal::migration::staging_path(&self.root.join("wal"), CUT, reference).unwrap()
    }
    fn path(&self, reference: &str) -> PathBuf {
        self.root.join(format!("wal/{reference}.wal"))
    }
    fn ref_names(state: &CutState) -> Vec<String> {
        serde_json::to_value(state.refs.as_ref().unwrap())
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["ref"].as_str().unwrap().into())
            .collect()
    }
}
#[async_trait::async_trait]
impl CutArtifacts for Artifacts {
    async fn lock_refs(
        &self,
        _cut_id: &str,
        _refs: &Refs,
        _op: Op,
    ) -> Result<Box<dyn CutGuard>, ErrorBody> {
        self.locks.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(()))
    }
    async fn check(&self, _request: &Request, _state: &CutState) -> Result<(), ErrorBody> {
        Ok(())
    }
    async fn apply(&self, request: &Request, state: &CutState) -> Result<Value, ErrorBody> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let op = request.op();
        for name in Self::ref_names(state) {
            match op {
                Op::Prepare => {
                    broca_wal::migration::freeze(
                        &self.root,
                        &self.path(&name),
                        &broca_wal::migration::Freeze {
                            cut_id: CUT.into(),
                            end_offset: 0,
                            lineage_id: name.clone(),
                        },
                    )
                    .map_err(unavailable)?
                    .map_err(unavailable)?;
                }
                Op::Release => {
                    broca_wal::migration::release(&self.root, &self.path(&name), CUT, "remote")
                        .map_err(unavailable)?
                        .map_err(unavailable)?;
                }
                Op::Abort => {
                    broca_wal::migration::abort_freeze(&self.path(&name), CUT)
                        .map_err(unavailable)?;
                    broca_wal::migration::unlink(&self.staged(&name)).map_err(unavailable)?;
                }
                Op::Export => {}
                Op::Import => {
                    let bytes = format!("cut:{name}");
                    broca_wal::migration::stage(
                        &self.root,
                        &self.staged(&name),
                        bytes.as_bytes(),
                        &broca_wal::migration::digest(bytes.as_bytes()),
                    )
                    .map_err(unavailable)?;
                }
                Op::Activate => {
                    if name == "b" && self.fail_b.load(Ordering::SeqCst) {
                        return Err(unavailable("B rename failed"));
                    }
                    if self.staged(&name).exists() {
                        std::fs::rename(self.staged(&name), self.path(&name))
                            .map_err(unavailable)?;
                    } else if std::fs::read(self.path(&name)).map_err(unavailable)?
                        != format!("cut:{name}").as_bytes()
                    {
                        return Err(unavailable("installed digest mismatch"));
                    }
                    broca_wal::migration::sync_dir(self.path(&name).parent().unwrap())
                        .map_err(unavailable)?;
                }
                Op::Describe | Op::Busy | Op::Diverged | Op::Status => {}
            }
            tokio::task::yield_now().await;
            if self.applied.fetch_add(1, Ordering::SeqCst) + 1
                == self.fail_after.load(Ordering::SeqCst)
            {
                return Err(unavailable("injected artifact failure"));
            }
        }
        Ok(json!({"enforced":true}))
    }
    async fn rollback(&self, _request: &Request, state: &CutState) -> Result<(), ErrorBody> {
        for name in Self::ref_names(state) {
            broca_wal::migration::abort_freeze(&self.path(&name), CUT).map_err(unavailable)?;
            broca_wal::migration::unlink(&self.staged(&name)).map_err(unavailable)?;
        }
        Ok(())
    }
    async fn reconcile(
        &self,
        state: &CutState,
        read_only: bool,
    ) -> Result<Option<Value>, ErrorBody> {
        if read_only {
            return Ok(None);
        }
        let intent = state.intent.as_ref().unwrap();
        match intent.op.as_str() {
            "release" | "abort" => {
                self.apply(&intent_request(intent)?, state).await?;
                Ok(Some(json!({"enforced":true})))
            }
            "import" => {
                for name in Self::ref_names(state) {
                    let expected = format!("cut:{name}");
                    match std::fs::read(self.staged(&name)) {
                        Ok(bytes)
                            if broca_wal::migration::digest(&bytes)
                                == broca_wal::migration::digest(expected.as_bytes()) => {}
                        Ok(_) => {
                            broca_wal::migration::unlink(&self.staged(&name))
                                .map_err(unavailable)?;
                            return Ok(None);
                        }
                        Err(_) => return Ok(None),
                    }
                }
                Ok(Some(json!({"enforced":true})))
            }
            "activate" => {
                self.apply(&intent_request(intent)?, state).await?;
                Ok(Some(json!({"enforced":true})))
            }
            "prepare" | "export" => Ok(None),
            _ => panic!("unexpected test intent"),
        }
    }
}

#[tokio::test]
async fn status_keeps_prepare_intent_byte_identical_and_repeat_rolls_forward_all_refs() {
    let root = root("prepare");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    let request = Request::decode(Op::Prepare.method(), &params()).unwrap();
    let state = CutState {
        refs: Some(serde_json::from_value(params()["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "prepare".into(),
            request: params(),
        }),
        ..CutState::default()
    };
    store.save(CUT, &state).unwrap();
    broca_wal::migration::freeze(
        &root,
        &artifacts.path("a"),
        &broca_wal::migration::Freeze {
            cut_id: CUT.into(),
            end_offset: 0,
            lineage_id: "a".into(),
        },
    )
    .unwrap()
    .unwrap();
    let before = std::fs::read(store.path(CUT)).unwrap();
    let status = store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Status.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    assert_eq!(status["step"], serde_json::to_value(Step::None).unwrap());
    assert_eq!(std::fs::read(store.path(CUT)).unwrap(), before);
    store
        .execute(
            contract::ADMITTED_CALLER,
            request.op().method(),
            &request.params(),
            &artifacts,
        )
        .await
        .unwrap();
    let state = store.load(CUT).unwrap();
    assert_eq!(state.step, Step::Prepared);
    assert!(state.intent.is_none());
    assert!(artifacts.path("b").with_extension("moving").exists());
    let calls = artifacts.calls.load(Ordering::SeqCst);
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Prepare.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    assert_eq!(artifacts.calls.load(Ordering::SeqCst), calls);
    let mut changed = params();
    changed["refs"].as_array_mut().unwrap().pop();
    assert_eq!(
        store
            .execute(
                contract::ADMITTED_CALLER,
                Op::Prepare.method(),
                &changed,
                &artifacts
            )
            .await
            .unwrap_err()
            .code
            .as_str(),
        "id_reused"
    );
}

#[tokio::test]
async fn reversible_partial_prepare_failure_rolls_back_every_owned_freeze() {
    let root = root("rollback");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    artifacts.fail_after.store(1, Ordering::SeqCst);
    assert_eq!(
        store
            .execute(
                contract::ADMITTED_CALLER,
                Op::Prepare.method(),
                &params(),
                &artifacts
            )
            .await
            .unwrap_err()
            .code
            .as_str(),
        "unavailable"
    );
    assert_eq!(artifacts.calls.load(Ordering::SeqCst), 1);
    assert!(store.load(CUT).unwrap().intent.is_none());
    assert_eq!(store.load(CUT).unwrap().step, Step::None);
    for name in ["a", "b"] {
        assert!(!artifacts.path(name).with_extension("moving").exists());
    }
}

#[tokio::test]
async fn multi_ref_release_failure_retains_intent_fence_and_boot_completes_every_ref() {
    let root = root("release");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Prepare.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Export.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    artifacts.applied.store(0, Ordering::SeqCst);
    artifacts.fail_after.store(1, Ordering::SeqCst);
    let mut release = params();
    release["holder"] = json!("remote");
    let result = store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Release.method(),
            &release,
            &artifacts,
        )
        .await;
    assert!(result.is_err(), "{result:?}");
    let state = store.load(CUT).unwrap();
    assert!(state.intent.is_some());
    assert!(!state.replies.contains_key(Op::Release.method()));
    assert!(artifacts.path("a").with_extension("moved").exists());
    assert!(!artifacts.path("b").with_extension("moved").exists());
    let before = std::fs::read(store.path(CUT)).unwrap();
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Status.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(store.path(CUT)).unwrap(), before);
    assert_eq!(
        broca_wal::migration::cut_fence(&root.join("wal"), "b").unwrap(),
        Some(CUT.into())
    );
    artifacts.fail_after.store(usize::MAX, Ordering::SeqCst);
    store.reconcile_boot(&artifacts).await.unwrap();
    assert!(store.load(CUT).unwrap().intent.is_none());
    assert!(store
        .load(CUT)
        .unwrap()
        .replies
        .contains_key(Op::Release.method()));
    assert!(artifacts.path("b").with_extension("moved").exists());
}

#[tokio::test]
async fn caller_is_refused_before_any_cut_authority_is_read() {
    let root = root("caller");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    std::fs::create_dir_all(store.path(CUT).parent().unwrap()).unwrap();
    std::fs::write(store.path(CUT), b"corrupt").unwrap();
    assert_eq!(
        store
            .execute("reserved:other", Op::Status.method(), &params(), &artifacts)
            .await
            .unwrap_err()
            .code
            .as_str(),
        "caller_forbidden"
    );
}

#[tokio::test]
async fn pending_import_verifies_the_entire_ref_set_before_recording_completion() {
    let root = root("import");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    let state = CutState {
        refs: Some(serde_json::from_value(params()["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "import".into(),
            request: params(),
        }),
        ..CutState::default()
    };
    store.save(CUT, &state).unwrap();
    broca_wal::migration::stage(
        &root,
        &artifacts.staged("a"),
        b"cut:a",
        &broca_wal::migration::digest(b"cut:a"),
    )
    .unwrap();
    store.reconcile_boot(&artifacts).await.unwrap();
    assert_eq!(store.load(CUT).unwrap().step, Step::None);
    assert!(!store
        .load(CUT)
        .unwrap()
        .replies
        .contains_key(Op::Import.method()));
    broca_wal::migration::stage(
        &root,
        &artifacts.staged("b"),
        b"invalid",
        &broca_wal::migration::digest(b"invalid"),
    )
    .unwrap();
    store.reconcile_boot(&artifacts).await.unwrap();
    assert!(!artifacts.staged("b").exists());
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Import.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    assert_eq!(store.load(CUT).unwrap().step, Step::Imported);
    assert!(store.load(CUT).unwrap().intent.is_none());
    assert_eq!(std::fs::read(artifacts.staged("b")).unwrap(), b"cut:b");
}

#[tokio::test]
async fn abort_intent_after_staging_unlink_is_not_mistaken_for_activation() {
    let root = root("abort");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Import.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    for name in ["a", "b"] {
        broca_wal::migration::durable_dir(&root, artifacts.path(name).parent().unwrap()).unwrap();
        std::fs::write(artifacts.path(name), format!("cut:{name}")).unwrap();
        broca_wal::migration::atomic_json(
            &root,
            &artifacts.path(name).with_extension("moved"),
            &broca_wal::migration::Moved {
                cut_id: "old-cut".into(),
                holder: "remote".into(),
                lineage_id: name.into(),
            },
        )
        .unwrap();
    }
    let mut state = store.load(CUT).unwrap();
    state.intent = Some(Intent {
        op: "abort".into(),
        request: params(),
    });
    store.save(CUT, &state).unwrap();
    broca_wal::migration::unlink(&artifacts.staged("a")).unwrap();
    store.reconcile_boot(&artifacts).await.unwrap();
    let state = store.load(CUT).unwrap();
    assert!(state.replies.contains_key(Op::Abort.method()));
    assert!(!state.replies.contains_key(Op::Activate.method()));
    assert!(state.intent.is_none());
    for name in ["a", "b"] {
        assert!(!artifacts.staged(name).exists());
        assert!(artifacts.path(name).with_extension("moved").exists());
    }
}

#[tokio::test]
async fn activate_failure_fences_the_first_installed_ref_and_refuses_abort() {
    let root = root("activate");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    broca_wal::migration::durable_dir(&root, &root.join("wal")).unwrap();
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Import.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    artifacts.applied.store(0, Ordering::SeqCst);
    artifacts.fail_after.store(1, Ordering::SeqCst);
    artifacts.fail_b.store(true, Ordering::SeqCst);
    assert_eq!(
        store
            .execute(
                contract::ADMITTED_CALLER,
                Op::Activate.method(),
                &params(),
                &artifacts
            )
            .await
            .unwrap_err()
            .code
            .as_str(),
        "unavailable"
    );
    let before = std::fs::read(store.path(CUT)).unwrap();
    assert_eq!(
        broca_wal::migration::cut_fence(&root.join("wal"), "a").unwrap(),
        Some(CUT.into())
    );
    assert_eq!(std::fs::read(artifacts.path("a")).unwrap(), b"cut:a");
    assert!(!artifacts.path("b").exists());
    assert!(store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Abort.method(),
            &params(),
            &artifacts
        )
        .await
        .is_err());
    assert_eq!(std::fs::read(store.path(CUT)).unwrap(), before);
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Status.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(store.path(CUT)).unwrap(), before);
    artifacts.fail_b.store(false, Ordering::SeqCst);
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Activate.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    assert!(store.load(CUT).unwrap().intent.is_none());
    assert_eq!(std::fs::read(artifacts.path("b")).unwrap(), b"cut:b");
    assert_eq!(
        broca_wal::migration::cut_fence(&root.join("wal"), "a").unwrap(),
        None
    );
}

#[tokio::test]
async fn concurrent_repeats_serialize_across_independent_cut_store_instances() {
    let root = root("serialization");
    let a = CutStore::new(&root);
    let b = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    let params = params();
    let (first, second) = futures::join!(
        a.execute(
            contract::ADMITTED_CALLER,
            Op::Prepare.method(),
            &params,
            &artifacts
        ),
        b.execute(
            contract::ADMITTED_CALLER,
            Op::Prepare.method(),
            &params,
            &artifacts
        )
    );
    assert_eq!(first.unwrap(), second.unwrap());
    assert_eq!(artifacts.calls.load(Ordering::SeqCst), 1);
    assert!(a.load(CUT).unwrap().intent.is_none());
}

#[tokio::test]
async fn unknown_cut_status_is_contract_typed_admitted_and_creates_no_authority() {
    let root = root("unknown-status");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    let value = store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Status.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    let _: contract::status::StatusReply = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(value["step"], serde_json::to_value(Step::None).unwrap());
    assert!(!store.path(CUT).exists());
}

#[tokio::test]
async fn changed_ref_set_is_refused_before_the_intent_or_any_export_changes() {
    let root = root("refs");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Prepare.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    let before = std::fs::read(store.path(CUT)).unwrap();
    let calls = artifacts.calls.load(Ordering::SeqCst);
    let mut changed = params();
    changed["refs"].as_array_mut().unwrap().pop();
    assert_eq!(
        store
            .execute(
                contract::ADMITTED_CALLER,
                Op::Export.method(),
                &changed,
                &artifacts
            )
            .await
            .unwrap_err()
            .code
            .as_str(),
        "invalid_state"
    );
    assert_eq!(std::fs::read(store.path(CUT)).unwrap(), before);
    assert_eq!(artifacts.calls.load(Ordering::SeqCst), calls);
}

#[tokio::test]
async fn rolled_back_first_prepare_and_import_clear_the_uncommitted_ref_set() {
    for op in [Op::Prepare, Op::Import] {
        let root = root(op.method());
        let store = CutStore::new(&root);
        let artifacts = Artifacts::new(root.clone());
        artifacts.fail_after.store(1, Ordering::SeqCst);
        let a = json!({"cutId":CUT,"refs":[params()["refs"][0]]});
        assert_eq!(
            store
                .execute("reserved:entorhinal", op.method(), &a, &artifacts)
                .await
                .unwrap_err()
                .code
                .as_str(),
            "unavailable"
        );
        let state = store.load(CUT).unwrap();
        assert_eq!(state.step, Step::None);
        assert!(state.intent.is_none());
        assert!(
            state.refs.is_none(),
            "refused {op:?} retained a hidden first-request binding"
        );
        artifacts.fail_after.store(usize::MAX, Ordering::SeqCst);
        let b = json!({"cutId":CUT,"refs":[params()["refs"][1]]});
        store
            .execute("reserved:entorhinal", op.method(), &b, &artifacts)
            .await
            .unwrap();
        assert_eq!(
            store.load(CUT).unwrap().refs.unwrap(),
            serde_json::from_value::<Refs>(b["refs"].clone()).unwrap()
        );
        store
            .execute("reserved:entorhinal", Op::Abort.method(), &b, &artifacts)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn mapless_cut_is_unreadable_before_any_artifact_resolution() {
    let root = root("mapless-cut");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    let state = json!({"step":"prepared","refs":params()["refs"],"intent":null,"replies":{}});
    broca_wal::migration::atomic_json(&root, &store.path(CUT), &state).unwrap();
    let error = store
        .execute(
            "reserved:entorhinal",
            Op::Status.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code.as_str(), "unavailable");
    assert!(error.message.contains(store.path(CUT).to_str().unwrap()));
    assert!(error.message.contains("sessions"));
    assert_eq!(artifacts.locks.load(Ordering::SeqCst), 0);
}

/// A damaged or foreign file under the cuts directory does not stop boot reconciliation: the
/// valid pending cut beside it still completes, and the damaged file is left as it was, where
/// the move fence still reads it and fails closed.
#[tokio::test]
async fn a_damaged_cut_file_does_not_stop_boot_reconciliation() {
    let root = root("damaged-boot");
    let store = CutStore::new(&root);
    let artifacts = Artifacts::new(root.clone());
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Prepare.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Export.method(),
            &params(),
            &artifacts,
        )
        .await
        .unwrap();
    artifacts.applied.store(0, Ordering::SeqCst);
    artifacts.fail_after.store(1, Ordering::SeqCst);
    let mut release = params();
    release["holder"] = json!("remote");
    assert!(store
        .execute(
            contract::ADMITTED_CALLER,
            Op::Release.method(),
            &release,
            &artifacts,
        )
        .await
        .is_err());
    assert!(store.load(CUT).unwrap().intent.is_some());
    let cuts = store.path(CUT).parent().unwrap().to_owned();
    std::fs::write(cuts.join("damaged.json"), b"corrupt").unwrap();
    std::fs::write(cuts.join("not.a.cut.json"), b"{}").unwrap();
    artifacts.fail_after.store(usize::MAX, Ordering::SeqCst);
    store
        .reconcile_boot(&artifacts)
        .await
        .expect("one damaged cut file must not fail boot reconciliation");
    assert!(
        store.load(CUT).unwrap().intent.is_none(),
        "the valid cut beside the damaged file still completes"
    );
    assert_eq!(
        std::fs::read(cuts.join("damaged.json")).unwrap(),
        b"corrupt",
        "the damaged cut is left untouched"
    );
    assert!(
        broca_wal::migration::cut_fence(&root.join("wal"), "b").is_err(),
        "the move fence still fails closed on the damaged cut"
    );
}
