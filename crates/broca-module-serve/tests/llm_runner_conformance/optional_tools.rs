use super::*;
use cortexkit_role_llm_runner_conformance::ScriptedTurn;

fn response(reply: Reply) -> Value {
    match reply {
        Reply::Response(value) => value,
        other => panic!("expected response, got {other:?}"),
    }
}

async fn finish(route: &BrocaRoute, run_id: &Value) {
    broca_test_support::wait_until("optional-tool run completes", || async {
        let result = response(
            route
                .request("run.result", json!({"run_id":run_id}))
                .await
                .unwrap(),
        );
        if result["state"] == "completed" {
            Ok(())
        } else {
            Err(result.to_string())
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_03201_resumes_an_absent_optional_tool_manifest() {
    if !require_subc() {
        return;
    }
    let reader = std::env::var_os("BROCA_OPTIONAL_TOOL_READER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.test-rigs/ck-broca-0.3.201")
        });
    if !reader.exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_OPTIONAL_TOOL_READER").is_none(),
            "stage released 0.3.201 reader"
        );
        println!("SKIP released_03201_resumes_an_absent_optional_tool_manifest: stage BROCA_OPTIONAL_TOOL_READER_BIN");
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
    assert!(String::from_utf8_lossy(&version.stdout).contains("0.3.201"));
    let scratch = broca_test_support::scratch_root()
        .join(format!("optional-tools-rollback-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let mut subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("rig");
    let session = "optional-tools-rollback";
    subject
        .install_script(
            session,
            Script {
                turns: ["new answer", "rollback answer"]
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
    let composition = json!({"providers":[{"provider":TOOL_MODULE,"tools":[{"name":"web_search","capabilities":[]}]}]});
    let plan = json!({
        "tool_items":[{"provider":TOOL_MODULE,"preset":"worker","params":{},"optional":true}],
        "system_text_items":[], "step_transform_items":[], "composition":composition,
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap()
    });
    let mut live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let sent = response(
        route
            .request(
                "session.send",
                json!({
                    "model":{"provider":"mock","model":"mock-model"},"plan":plan,
                    "prompt":format!("{SESSION_MARKER}{session}"),"send_id":"writer"
                }),
            )
            .await
            .unwrap(),
    );
    finish(&route, &sent["run_id"]).await;
    let frozen = subject
        .records(&live, session)
        .into_iter()
        .find_map(|r| match r {
            broca_wal::WalRecord::RunStarted { manifest, .. } => manifest,
            _ => None,
        })
        .unwrap();
    assert!(frozen.tools.is_empty());
    assert!(
        matches!(&frozen.fetches[0].outcome, broca_wal::FetchOutcome::Absent { reason } if reason == "plan_stale")
    );
    let wal_path = live.state_root.join("wal").join(format!(
        "{}.wal",
        broca_core::session_addr(&subject.bind_identity(session))
    ));
    let bytes = std::fs::read(&wal_path).unwrap();
    drop(route);
    live.kill_all();
    drop(live);
    // Start the old binary on the new writer's durable state. Success requires reading
    // the absent tool's manifest without parsing or fetching the original plan again.
    subject.binary = Some(reader);
    let mut rollback = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&rollback, session, &subject.owner_stamp())
        .await
        .unwrap();
    let baseline = response(route.request("session.baseline", json!({})).await.unwrap());
    assert_eq!(baseline["baseline"]["tool_names"], json!([]));
    assert_eq!(
        baseline["baseline"]["composition_digest"],
        frozen.composition_digest
    );
    assert_eq!(
        std::fs::read(&wal_path).unwrap(),
        bytes,
        "reader must not rewrite the WAL"
    );
    let sent = response(
        route
            .request(
                "session.send",
                json!({
                    "model":{"provider":"mock","model":"mock-model"},
                    "prompt":format!("{SESSION_MARKER}{session} continue"),"send_id":"rollback"
                }),
            )
            .await
            .unwrap(),
    );
    finish(&route, &sent["run_id"]).await;
    let manifests: Vec<_> = subject
        .records(&rollback, session)
        .into_iter()
        .filter_map(|r| match r {
            broca_wal::WalRecord::RunStarted { manifest, .. } => manifest,
            _ => None,
        })
        .collect();
    assert_eq!(manifests, vec![frozen.clone(), frozen]);
    drop(route);
    rollback.kill_all();
}
