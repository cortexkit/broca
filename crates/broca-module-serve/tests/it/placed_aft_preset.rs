//! Runs the placed release AFT under an isolated daemon, never the operator's daemon.

use super::preset_real_daemon::{call, dispatch, plan_for, register, Rig, SdkDescriber};
use broca_core::ToolManifest;
use broca_fetch_plan::PlanAdmitter;
use broca_subc::{SubcPlanAdmitter, SubcToolPlane};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Production {
    log: PathBuf,
    starts: usize,
    pids: String,
}

impl Production {
    fn snapshot(home: &std::path::Path) -> Self {
        let date = std::process::Command::new("date")
            .args(["-u", "+%Y-%m-%d"])
            .output()
            .unwrap();
        assert!(date.status.success());
        let log = home.join(format!(
            ".local/share/cortexkit/run/logs/subc.{}.log",
            String::from_utf8(date.stdout).unwrap().trim()
        ));
        let starts = std::fs::read_to_string(&log)
            .unwrap_or_default()
            .matches("subc daemon starting")
            .count();
        let pids = std::process::Command::new("pgrep")
            .args(["-f", "cortexkit/bin/"])
            .output()
            .unwrap();
        assert!(pids.status.success() || pids.status.code() == Some(1));
        let pids = String::from_utf8(pids.stdout).unwrap();
        println!(
            "production log={} subc daemon starting count={starts}; production pid set={pids:?}",
            log.display()
        );
        Self { log, starts, pids }
    }
}

fn op_answer(value: Value) -> Value {
    if value.get("catalog_digest").is_some() {
        return value;
    }
    assert_ne!(value["isError"], true, "{value}");
    serde_json::from_str(
        value["content"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|block| block["text"].as_str())
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the placed AFT release and production process invariants; run explicitly"]
async fn placed_aft_requires_a_scoped_preset_and_worker_read_bash_watch_succeed() {
    let home = PathBuf::from(std::env::var_os("HOME").expect("placed release test requires HOME"));
    let source = home.join(".local/share/cortexkit/bin/ck-aft");
    assert!(source.exists(), "placed AFT release missing: {source:?}");
    use sha2::Digest;
    let sha = sha2::Sha256::digest(std::fs::read(&source).unwrap())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let binary = broca_test_support::ckdev_binary(&source, broca_test_support::scratch_root());
    let before = Production::snapshot(&home);
    let mut rig = Rig::start(Some(&binary)).await;
    // Even a version probe inherits only scratch directories, and never starts a daemon.
    let version_child = broca_test_support::retry_executable_file_busy(|| {
        tokio::process::Command::new(&binary)
            .arg("--version")
            .env("HOME", rig.dir.join("home"))
            .env("XDG_DATA_HOME", rig.dir.join("data"))
            .env("XDG_RUNTIME_DIR", rig.dir.join("runtime"))
            .env("XDG_CONFIG_HOME", rig.dir.join("config"))
            // `spawn` inherits stdio, unlike `output`; the version is read from stdout.
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
    })
    .unwrap();
    let version = version_child.wait_with_output().await.unwrap();
    assert!(version.status.success());
    println!(
        "placed AFT SHA256={sha} --version={}",
        String::from_utf8_lossy(&version.stdout).trim()
    );
    let (core_handle, core) =
        register(&rig, "prefrontal-core", Arc::new(Mutex::new(Vec::new()))).await;
    let conn = rig.connect().await;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let catalog = conn.catalog_list().await.unwrap();
        if catalog
            .iter()
            .any(|entry| entry.module_id == "aft" && entry.ready)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "placed AFT did not register; stderr tail: {}",
            rig.logs()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let scope = rig.scope().await;
    let session = rig.session("placed-worker");
    let selector = subc_protocol::scope::ScopeSelector::new(
        subc_protocol::Principal::Reserved {
            module_id: "preset-owner".into(),
        },
        scope.scope_ref.clone(),
        Some(scope.scope_epoch),
    );
    let route = conn
        .route_open_tool_provider_v1("aft", &session, Some(selector))
        .await
        .unwrap();
    let empty = plan_for(json!([]));
    let catalog = op_answer(
        conn.tool_call_with_preset(
            &route,
            "tool.catalog",
            &json!({"preset": "worker", "params": {}, "composition": empty["composition"]}),
            None,
            Duration::from_secs(30),
            Some("worker"),
        )
        .await
        .unwrap(),
    );
    let mut tools = catalog["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| json!({"name": tool["name"], "capabilities": tool["capabilities"]}))
        .collect::<Vec<_>>();
    tools.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    for required in ["read", "bash", "bash_watch"] {
        assert!(
            tools.iter().any(|tool| tool["name"] == required),
            "worker catalog lacks {required}: {catalog}"
        );
    }
    let value = plan_for(json!(tools));
    let plan =
        broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap();
    let frozen = SubcPlanAdmitter::new(conn.clone())
        .admit(&plan, &session, Some(&scope))
        .await
        .unwrap()
        .manifest;
    let manifest = ToolManifest::from_tools(frozen.tools.as_ref().clone())
        .with_plan_manifest(&frozen)
        .unwrap();
    let plane = SubcToolPlane::new(Arc::try_unwrap(rig.connect().await).ok().unwrap(), "aft");
    plane.set_scope_describer(Arc::new(SdkDescriber(core_handle)));
    let read = dispatch(
        &plane,
        session.clone(),
        Some(scope.clone()),
        &manifest,
        &call("read", json!({"filePath": "hello.txt"}), "read"),
    )
    .await;
    assert!(!read.is_error, "worker read: {read:?}");
    assert!(format!("{:?}", read.output).contains("frozen preset reaches AFT"));
    let bash = dispatch(&plane, session.clone(), Some(scope.clone()), &manifest, &call("bash", json!({"command": "printf preset-ok", "background": true, "description": "Verify worker preset background task"}), "bash")).await;
    assert!(!bash.is_error, "worker bash: {bash:?}");
    let serialized = serde_json::to_string(&bash.output).unwrap();
    let start = serialized
        .find("bash-")
        .unwrap_or_else(|| panic!("background bash returned no task id: {serialized}"));
    let task_id = serialized[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
        .collect::<String>();
    let watch = dispatch(
        &plane,
        session.clone(),
        Some(scope.clone()),
        &manifest,
        &call("bash_watch", json!({"taskId": task_id}), "watch"),
    )
    .await;
    assert!(!watch.is_error, "worker bash_watch: {watch:?}");
    let bytes = serde_json::to_vec(&subc_protocol::tool_call::ToolCallRequest {
        name: "read".into(),
        arguments: json!({"filePath": "hello.txt"}),
        tool_call_id: Some("without-preset".into()),
        progress_token: None,
        call_key: None,
        schema_pin: None,
        origin: None,
        preset: None,
        output_refs: None,
    })
    .unwrap();
    let refusal = conn
        .route_unary_reply(&route, bytes, Duration::from_secs(30))
        .await
        .unwrap()
        .expect_err("AFT refuses a scoped call without preset");
    println!(
        "AFT scoped missing-preset refusal code verbatim: {}",
        refusal.code
    );
    assert_eq!(refusal.code, "invalid_request");
    assert!(refusal.message.contains("preset"), "{refusal:?}");
    println!(
        "AFT missing-preset refusal message verbatim: {}",
        refusal.message
    );
    plane.close_session_routes(&session).await;
    conn.route_close(&route).await.unwrap();
    core.abort();
    rig.stop().await;
    let after = Production::snapshot(&home);
    assert_eq!(before.log, after.log, "UTC log date changed during test");
    assert_eq!(
        before.starts, after.starts,
        "production daemon start count changed"
    );
    assert_eq!(before.pids, after.pids, "production pid set changed");
}
