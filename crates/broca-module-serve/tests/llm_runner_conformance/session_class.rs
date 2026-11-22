//! Production admission, restart placement, and the released reader's refusal boundary.
use super::*;
use broca_session_store::record::Record;
use cortexkit_role_llm_runner_conformance::ScriptedTurn;

pub(super) fn rigs_present() -> bool {
    for (name, key) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        if !sibling_bin(name, key).exists() {
            assert!(
                !require_subc(),
                "required real-daemon binary is missing: {name}"
            );
            println!("SKIP session_class: missing {name}");
            return false;
        }
    }
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    true
}
pub(super) fn scratch(tag: &str) -> PathBuf {
    let path = broca_test_support::scratch_root().join(format!("{tag}-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn response(reply: Reply) -> Value {
    match reply {
        Reply::Response(value) => value,
        other => panic!("expected response: {other:?}"),
    }
}

async fn finish(route: &dyn RunnerRoute, id: &Value) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = response(
            route
                .request("run.status", json!({"run_id":id}))
                .await
                .unwrap(),
        );
        if status["state"] != "active" && status["state"] != "pending" {
            break;
        }
        assert!(Instant::now() < deadline, "run never finished: {status}");
        sleep(Duration::from_millis(20)).await;
    }
}

fn golden(name: &str, value: Value) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden")
        .join(name);
    let bytes = serde_json::to_vec(&value).unwrap();
    if std::env::var_os("BROCA_REGENERATE_SESSION_CLASS_GOLDENS").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    assert_eq!(bytes, std::fs::read(path).unwrap(), "{name}");
}

/// `session` is stored in the root of class `placed`, and in no other.
fn assert_placed(
    subject: &BrocaSubject,
    live: &Live,
    session: &str,
    placed: broca_records::session_storage::StorageClass,
) {
    let address = broca_core::session_addr(&subject.bind_identity(session));
    assert_eq!(
        broca_records::session_storage::session_storage_class(&live.state_root, &address).unwrap(),
        Some(placed),
        "{session} is stored as {}",
        placed.name()
    );
}

async fn assert_stored_head(
    subject: &BrocaSubject,
    live: &Live,
    session: &str,
    class: broca_records::session_storage::StorageClass,
    runs: u64,
) {
    use broca_session_store::{
        backend::{LocalBackend, SessionAddr},
        head::SessionTriple,
        keys::DeploymentKey,
        session::Session,
    };
    let identity = subject.bind_identity(session);
    let address = broca_core::session_addr(&identity);
    assert_eq!(
        broca_records::session_storage::session_storage_class(&live.state_root, &address).unwrap(),
        Some(class),
        "the head keeps its backup class"
    );
    assert!(broca_records::session_storage::session_storage_head(
        &live.state_root,
        class,
        &address
    )
    .is_file());
    let stored = Session::view(
        LocalBackend::new(class.store_root(&live.state_root)),
        SessionAddr(address),
        &SessionTriple {
            project_root: identity.project_root.display().to_string(),
            harness: identity.harness,
            session: identity.session,
        },
        &DeploymentKey::unloaded(1),
    )
    .await
    .expect("read the plain head without opening a writer");
    let operations = &stored.operations;
    assert_eq!(operations.owner.episodes, runs);
    // With one run, or two runs across restart, the first and latest starts cover every run.
    for start in [
        operations.owner.first_start.as_ref().unwrap(),
        operations.run.as_ref().unwrap().start.as_ref().unwrap(),
    ] {
        assert_eq!(start.session_class.as_deref(), Some("head"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classed_sends_route_freeze_and_keep_placement_after_restart() {
    if !rigs_present() {
        return;
    }
    let scratch = scratch("session-class");
    let mut subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("rig");
    let mut live = subject.start(&root).await.unwrap();
    use broca_records::session_storage::StorageClass;
    for (session, class, retention, placed) in [
        ("durable-head", Some("head"), None, StorageClass::Durable),
        ("ordinary", None, None, StorageClass::Ordinary),
        (
            "retained-head",
            Some("head"),
            Some(3600),
            StorageClass::Ephemeral,
        ),
        ("worker", Some("worker"), None, StorageClass::Ordinary),
    ] {
        subject
            .install_script(
                session,
                Script {
                    turns: (0..3)
                        .map(|_| {
                            ScriptedTurn::new(vec![ScriptedPart::Text("answer".into())]).with_usage(
                                cortexkit_role_llm_runner_conformance::ScriptedUsage::new(
                                    1, 0, 0, 1,
                                ),
                            )
                        })
                        .collect(),
                },
            )
            .await
            .unwrap();
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let mut params = subject.send_fields(session, true);
        params.insert("prompt".into(), json!("first"));
        if let Some(class) = class {
            params.insert("session_class".into(), json!(class));
        }
        if let Some(seconds) = retention {
            params.insert("retention".into(), json!(seconds));
        }
        let sent = response(
            route
                .request("session.send", Value::Object(params))
                .await
                .unwrap(),
        );
        if session == "durable-head" {
            let mut normalized = sent.clone();
            normalized["run_id"] = json!("run-1");
            golden("send_reply.session_class.json", normalized);
        }
        finish(&route, &sent["run_id"]).await;
        assert_placed(&subject, &live, session, placed);
        if class == Some("head") {
            let storage_class = if retention.is_some() {
                broca_records::session_storage::StorageClass::Ephemeral
            } else {
                broca_records::session_storage::StorageClass::Durable
            };
            assert_stored_head(&subject, &live, session, storage_class, 1).await;
        } else {
            assert!(subject.records(&live, session).await.iter().any(|record| matches!(record, Record::RunStart(start) if start.session_class.as_deref() == class)));
        }
        let read = response(route.request("session.read", json!({})).await.unwrap());
        assert!(!read["messages"].as_array().unwrap().is_empty());
    }
    let route = subject
        .session_route(&live, "durable-head", &subject.owner_stamp())
        .await
        .unwrap();
    let mut changed = subject.send_fields("durable-head", false);
    changed.insert("prompt".into(), json!("next"));
    changed.insert("session_class".into(), json!("worker"));
    match route
        .request("session.send", Value::Object(changed))
        .await
        .unwrap()
    {
        Reply::Error(error) => {
            assert_eq!(error.code, "invalid_params");
            assert!(error.message.contains("session_class"));
            golden(
                "send_reply.session_class_changed.json",
                serde_json::to_value(error).unwrap(),
            );
        }
        other => panic!("changed session_class accepted: {other:?}"),
    }
    drop(route);
    live.kill_all();
    drop(live);
    subject.backup_classes = Some(vec!["worker".into()]);
    let mut live = subject.start(&root).await.unwrap();
    // A session keeps the class it was created in: `worker` now names a backed-up class, and the
    // worker session stays where it is.
    for (session, class, placed) in [
        ("durable-head", None, StorageClass::Durable),
        ("worker", Some("worker"), StorageClass::Ordinary),
    ] {
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let describe = response(route.request("role.describe", json!({})).await.unwrap());
        assert_eq!(
            describe["session_class"]["backup_classes"],
            json!(["worker"])
        );
        assert!(describe["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("session_class")));
        let mut params = subject.send_fields(session, false);
        params.insert("prompt".into(), json!("next"));
        if let Some(class) = class {
            params.insert("session_class".into(), json!(class));
        }
        let sent = response(
            route
                .request("session.send", Value::Object(params))
                .await
                .unwrap(),
        );
        finish(&route, &sent["run_id"]).await;
        assert_placed(&subject, &live, session, placed);
        if session == "durable-head" {
            assert_stored_head(
                &subject,
                &live,
                session,
                broca_records::session_storage::StorageClass::Durable,
                2,
            )
            .await;
            let mut changed = subject.send_fields(session, false);
            changed.insert("prompt".into(), json!("change class after restart"));
            changed.insert("session_class".into(), json!("worker"));
            match route
                .request("session.send", Value::Object(changed))
                .await
                .unwrap()
            {
                Reply::Error(error) => {
                    assert_eq!(error.code, "invalid_params");
                    assert!(error.message.contains("session_class"));
                }
                other => panic!("changed session_class accepted after restart: {other:?}"),
            }
            assert_stored_head(
                &subject,
                &live,
                session,
                broca_records::session_storage::StorageClass::Durable,
                2,
            )
            .await;
            continue;
        }
        let classes: Vec<_> = subject
            .records(&live, session)
            .await
            .iter()
            .filter_map(|record| match record {
                Record::RunStart(start) => Some(start.session_class.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(classes.len(), 2);
        assert!(classes.iter().all(|class| class.as_deref()
            == Some(if session == "worker" {
                "worker"
            } else {
                "head"
            })));
    }
    live.kill_all();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn released_03205_refuses_session_class_by_feature_and_reads_unclassed() {
    if !rigs_present() {
        return;
    }
    let reader = std::env::var_os("BROCA_SESSION_CLASS_READER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.test-rigs/ck-broca-0.3.205")
        });
    if !reader.exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_SESSION_CLASS_READER").is_none(),
            "stage v0.3.205 reader"
        );
        println!("SKIP released_03205_refuses_session_class_by_feature_and_reads_unclassed: no v0.3.205 reader");
        return;
    }
    let version = broca_test_support::ckdev_command(&reader, broca_test_support::scratch_root())
        .arg("--version")
        .output()
        .unwrap();
    assert!(version.status.success());
    assert!(String::from_utf8_lossy(&version.stdout).contains("0.3.205"));
    let scratch = scratch("class-rollback");
    let mut subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("rig");
    let mut live = subject.start(&root).await.unwrap();
    for (session, class) in [
        ("classed-rollback", Some("worker")),
        ("unclassed-rollback", None),
    ] {
        subject
            .install_script(
                session,
                Script {
                    turns: vec![ScriptedTurn::new(vec![ScriptedPart::Text("answer".into())])
                        .with_usage(cortexkit_role_llm_runner_conformance::ScriptedUsage::new(
                            1, 0, 0, 1,
                        ))],
                },
            )
            .await
            .unwrap();
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let mut params = subject.send_fields(session, true);
        params.insert("prompt".into(), json!("first"));
        if let Some(class) = class {
            params.insert("session_class".into(), json!(class));
        }
        let sent = response(
            route
                .request("session.send", Value::Object(params))
                .await
                .unwrap(),
        );
        finish(&route, &sent["run_id"]).await;
    }
    let path = live.state_root.join("wal").join(format!(
        "{}.wal",
        broca_core::session_addr(&subject.bind_identity("classed-rollback"))
    ));
    live.kill_all();
    drop(live);
    let bytes = std::fs::read(&path).unwrap();
    subject.binary = Some(reader);
    let mut rollback = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&rollback, "classed-rollback", &subject.owner_stamp())
        .await
        .unwrap();
    match route.request("session.read", json!({})).await.unwrap() {
        Reply::Error(error) => {
            assert_eq!(error.code, "open_failed");
            assert!(
                format!("{error:?}").contains("session-class/v1"),
                "{error:?}"
            );
        }
        other => panic!("released reader ignored frozen session_class: {other:?}"),
    }
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    let route = subject
        .session_route(&rollback, "unclassed-rollback", &subject.owner_stamp())
        .await
        .unwrap();
    let read = response(route.request("session.read", json!({})).await.unwrap());
    assert!(!read["messages"].as_array().unwrap().is_empty());
    rollback.kill_all();
}
