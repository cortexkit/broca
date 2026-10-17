use super::*;
use broca_core::MaterializedStore;
use cortexkit_role_llm_runner_conformance::ScriptedTurn;

fn response(reply: Reply) -> Value {
    match reply {
        Reply::Response(value) => value,
        other => panic!("expected response, got {other:?}"),
    }
}

async fn send_and_finish(
    subject: &BrocaSubject,
    live: &Live,
    session: &str,
    send_id: &str,
    first: bool,
) -> String {
    let route = subject
        .session_route(live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let mut params = subject.send_fields(session, first);
    params.insert("send_id".into(), json!(send_id));
    params.insert(
        "prompt".into(),
        json!(format!("{SESSION_MARKER}{session} {send_id}")),
    );
    let answer = response(
        route
            .request("session.send", Value::Object(params))
            .await
            .unwrap(),
    );
    let run_id = answer["run_id"].as_str().unwrap().to_owned();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = response(
                route
                    .request("run.result", json!({"run_id":run_id}))
                    .await
                    .unwrap(),
            );
            if status["state"] == "completed" {
                return;
            }
            subject.pause().await;
        }
    })
    .await
    .expect("scripted run completes");
    run_id
}

async fn page(subject: &BrocaSubject, live: &Live, session: &str) -> Value {
    let route = subject
        .session_route(live, session, &subject.owner_stamp())
        .await
        .unwrap();
    response(
        route
            .request("session.read", json!({"from_ordinal":0,"limit":20}))
            .await
            .unwrap(),
    )
}

fn attributed(page: &Value, ids: &[String]) {
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages.len(), ids.len() * 2 + 1);
    for (index, message) in messages.iter().enumerate() {
        assert_eq!(message["run"]["run_id"], ids[index.saturating_sub(1) / 2]);
        assert_eq!(message["run"]["episode"], ids[index.saturating_sub(1) / 2]);
        assert_eq!(message["run"]["final"], index > 0 && index % 2 == 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_0194_store_backfills_and_rollback_appends_are_rederived() {
    let reader = std::env::var_os("BROCA_ATTRIBUTION_READER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.test-rigs/ck-broca-0.3.194")
        });
    if !reader.exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_ATTRIBUTION_READER").is_none(),
            "released 0.3.194 reader must be staged"
        );
        println!("SKIP released_0194_store_backfills_and_rollback_appends_are_rederived: stage BROCA_ATTRIBUTION_READER_BIN (released 0.3.194); BROCA_REQUIRE_ATTRIBUTION_READER makes absence fail");
        return;
    }
    let version = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::ckdev_command(&reader, broca_test_support::scratch_root())
            .arg("--version")
            .env_clear()
            .output()
    })
    .unwrap();
    assert!(version.status.success());
    assert!(String::from_utf8_lossy(&version.stdout).contains("0.3.194"));
    let scratch = broca_test_support::scratch_root()
        .join(format!("run-attribution-rollback-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let mut subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("rig");
    let session = "attribution-rollback";
    subject
        .install_script(
            session,
            Script {
                turns: ["old answer", "new answer", "rollback answer"]
                    .into_iter()
                    .map(|text| {
                        ScriptedTurn::new(vec![ScriptedPart::Text(text.into())]).with_usage(
                            cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
                        )
                    })
                    .collect(),
            },
        )
        .await
        .unwrap();
    subject.binary = Some(reader.clone());
    let mut old = subject.start(&root).await.unwrap();
    let first = send_and_finish(&subject, &old, session, "old-send", true).await;
    let old_page = page(&subject, &old, session).await;
    assert!(old_page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .all(|m| m.get("run").is_none()));
    let db_path = old.state_root.join("run-index.db");
    old.kill_all();
    drop(old);
    subject.binary = None;
    let mut new = subject.start(&root).await.unwrap();
    let first_page = page(&subject, &new, session).await;
    attributed(&first_page, std::slice::from_ref(&first));
    let identity = subject.bind_identity(session);
    let store = broca_store::SqliteStore::open(&db_path).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(snapshot) =
                store.read_page_with_watermark(&identity, Some(0), 20, broca_core::ReadView::Raw)
            {
                assert!(snapshot.rows.last().unwrap().run.as_ref().unwrap().is_final);
                break;
            }
            subject.pause().await;
        }
    })
    .await
    .expect("base-written store is lazily reprojected");
    let second = send_and_finish(&subject, &new, session, "new-send", false).await;
    let new_page = page(&subject, &new, session).await;
    attributed(&new_page, &[first.clone(), second.clone()]);
    let wal_path = new
        .state_root
        .join("wal")
        .join(format!("{}.wal", broca_core::session_addr(&identity)));
    let new_bytes = std::fs::read(&wal_path).unwrap();
    new.kill_all();
    drop(new);
    subject.binary = Some(reader);
    let mut rollback = subject.start(&root).await.unwrap();
    let rollback_page = page(&subject, &rollback, session).await;
    assert_eq!(rollback_page["messages"].as_array().unwrap().len(), 5);
    for (old, new) in rollback_page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .zip(new_page["messages"].as_array().unwrap())
    {
        assert_eq!(old["ordinal"], new["ordinal"]);
        assert_eq!(old["message"], new["message"]);
    }
    assert_eq!(
        std::fs::read(&wal_path).unwrap(),
        new_bytes,
        "rollback reads must not rewrite the WAL"
    );
    let third = send_and_finish(&subject, &rollback, session, "rollback-send", false).await;
    rollback.kill_all();
    drop(rollback);
    assert!(
        store
            .read_page_with_watermark(&identity, Some(0), 20, broca_core::ReadView::Raw)
            .unwrap_err()
            .contains("run attribution requires WAL backfill"),
        "old-format writes under an upgraded version are still unknown"
    );
    subject.binary = None;
    let mut upgraded = subject.start(&root).await.unwrap();
    attributed(
        &page(&subject, &upgraded, session).await,
        &[first, second, third],
    );
    upgraded.kill_all();
    drop(upgraded);
    drop(store);
    broca_test_support::remove_rig_dir(scratch);
}
