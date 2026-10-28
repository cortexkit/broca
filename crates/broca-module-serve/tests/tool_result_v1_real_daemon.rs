//! Negotiation over a real daemon, with replies shaped like the AFT tool provider's encoder
//! stored by the real serve/core path.
#![cfg(unix)]

mod delivery_rig;

use std::{path::PathBuf, sync::Arc};

use broca_protocol::{ContentBlock, ToolCall, ToolCallId};
use broca_subc::{SubcConnection, SubcPlanAdmitter, SubcToolPlane};
use delivery_rig::{result, Rig, Step};
use serde_json::{json, Value};

fn schema() -> Value {
    json!({"type":"object","properties":{"fixture":{"type":"string","enum":["text","image","error"]}},"required":["fixture"]})
}

fn plan() -> Value {
    let composition = json!({"providers":[{"provider":"aft","tools":[{"name":"read","capabilities":[]}]}],
        "step_transform_items":[]});
    json!({"tool_items":[{"provider":"aft","preset":"reader","params":{}}],"system_text_items":[],
        "step_transform_items":[],
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition})
}

fn hex(value: impl AsRef<std::path::Path>) -> String {
    value
        .as_ref()
        .to_str()
        .unwrap()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn binary(name: &str, variable: &str) -> PathBuf {
    std::env::var_os(variable)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../.test-rigs")
                .join(name)
        })
}

async fn capture(
    mode: &str,
    daemon_bin: &std::path::Path,
    surface: &std::path::Path,
) -> (Vec<Value>, Vec<u8>) {
    let mut rig = Rig::start(&format!("tool-result-{mode}")).await;
    let config = rig.dir.join("config/cortexkit");
    let runtime = rig.dir.join("runtime");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::create_dir_all(&runtime).unwrap();
    let fixtures = rig.dir.join("aft-fixtures.json");
    let requests = rig.dir.join("aft-requests.txt");
    let plan = plan();
    std::fs::write(&fixtures, json!({"catalog":{"generation":"1","catalog_digest":"aft-fixture",
        "composition_digest":plan["composition_digest"],"tools":[{"name":"read","description":"Read a fixture",
        "schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema()).unwrap(),"semantics":1,"capabilities":[],"input_schema":schema()}]}}).to_string()).unwrap();
    let surface = broca_test_support::ckdev_binary(surface, &rig.dir);
    // The daemon hands a module its connection file as `--subc <path>`; fake-surface reads it
    // from SUBC_CONNECTION, so the config names the file the daemon publishes.
    let connection = runtime.join("subc-connection.json");
    std::fs::write(config.join("subc.jsonc"), json!({"version":1,"modules":{"aft":{
        "program":surface,"args":["--module-id","aft",format!("--SURFACE_FIXTURES={}",hex(&fixtures)),format!("--SURFACE_REQUESTS={}",hex(&requests))],
        "enabled":true,"env":{"SURFACE_TOOL_RESULT":mode,"SUBC_CONNECTION":connection}}}}).to_string()).unwrap();
    let daemon_bin = broca_test_support::ckdev_binary(daemon_bin, &rig.dir);
    let child = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::tokio_rig_command(&daemon_bin, &rig.dir, "daemon")?
            .env("XDG_CONFIG_HOME", rig.dir.join("config"))
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("XDG_DATA_HOME", rig.dir.join("data-home"))
            .env("SUBC_PORT", "0")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
    })
    .unwrap();
    let mut daemon = broca_test_support::RigChild::new(child, &rig.dir, "daemon");
    broca_test_support::wait_until("AFT stand-in ready", || async {
        let log = std::fs::read_to_string(&requests).map_err(|e| e.to_string())?;
        if log.contains("hello-ack") {
            Ok(())
        } else {
            Err(log)
        }
    })
    .await;
    let conn = Arc::new(SubcConnection::connect(&connection).await.unwrap());
    rig.registry()
        .set_plan_admitter(Arc::new(SubcPlanAdmitter::new(conn.clone())));
    *rig.script.tool_plane.lock().unwrap() = Some(Arc::new(SubcToolPlane::new(
        SubcConnection::connect(&connection).await.unwrap(),
        "aft",
    )));
    let calls = ["text", "image", "error"]
        .into_iter()
        .map(|fixture| {
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(format!("read-{fixture}")),
                tool_name: "read".into(),
                input: json!({"fixture":fixture}),
                thought_signature: None,
            })
        })
        .collect();
    rig.script.push(&[Step::Stream(calls), Step::Text("done")]);
    let sent = rig.request(7, json!({"method":"session.send","params":{"send_id":"read-fixtures","prompt":"Read the fixtures","model":{"provider":"fake","model":"fake"},"plan":plan}})).await;
    result(&sent);
    let records = rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, broca_wal::WalRecord::ToolResult { .. }))
            .count(),
        3
    );
    let read = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    let page = result(&read);
    let outputs = ["text", "image", "error"]
        .into_iter()
        .map(|fixture| {
            page["messages"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|row| row["message"]["content"].as_array().unwrap())
                .find(|block| {
                    block["tool_call_id"] == format!("read-{fixture}")
                        && block["output"].is_object()
                })
                .unwrap_or_else(|| panic!("no result for {fixture}: {page}"))["output"]
                .clone()
        })
        .collect();
    let log = std::fs::read_to_string(requests).unwrap();
    let binds: Vec<Value> = log
        .lines()
        .filter_map(|line| line.strip_prefix("route.bind "))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(binds.iter().all(|bind| !bind["consumer_capabilities"]
        .as_array()
        .is_some_and(|caps| caps.iter().any(|cap| cap == "held-reply/v1"))));
    assert_eq!(
        binds
            .iter()
            .filter(|bind| bind["consumer_capabilities"] == json!(["tool-result/v1"]))
            .count(),
        usize::from(mode == "v1")
    );
    let tool_calls: Vec<Value> = log
        .lines()
        .filter_map(|line| line.strip_prefix("tool-result-request "))
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|r| r["name"] == "read")
        .collect();
    assert_eq!(tool_calls.len(), 3, "each call dispatches once: {log}");
    assert!(tool_calls.iter().all(|call| call.get("call_key").is_none()));
    rig.finish().await;
    daemon.kill().await.unwrap();
    (outputs, read.body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_tool_result_v1_read_stores_notice_or_captures_png() {
    let daemon = binary("ck-subc", "SUBC_CORE_BIN");
    let surface = binary("fake-surface", "FAKE_SURFACE_BIN");
    if !daemon.exists() || !surface.exists() {
        assert!(
            std::env::var("BROCA_REQUIRE_SUBC").as_deref() != Ok("1"),
            "required ck-subc/fake-surface missing: {daemon:?}, {surface:?}"
        );
        eprintln!("SKIP real_daemon_tool_result_v1: staged ck-subc/fake-surface required");
        return;
    }
    let (before, before_bytes) = capture("legacy", &daemon, &surface).await;
    let (after, after_bytes) = capture("v1", &daemon, &surface).await;
    assert_eq!(
        before[0],
        json!({"kind":"text","text":"Read text: 12:00\n"})
    );
    assert_eq!(after[0], before[0]);
    assert_eq!(
        before[2],
        json!({"kind":"error","message":"File not found: missing.txt"})
    );
    assert_eq!(after[2], before[2]);
    assert_eq!(
        before[1],
        json!({"kind":"text","text":"Read image attachment (image/png, 1×1, 69 B)."})
    );
    if cfg!(feature = "test-attachments") {
        assert_eq!(after[1]["kind"], "parts");
        assert_eq!(after[1]["parts"][1]["kind"], "media");
        assert_eq!(after[1]["parts"][1]["media"]["mime"], "image/png");
    } else {
        assert_eq!(
            after[1],
            json!({"kind":"text","text":"Read image attachment (image/png, 1×1, 69 B).\n[image/png not shown to the model: this runner does not pass tool images to models yet]"})
        );
    }
    if let Some(dir) = std::env::var_os("BROCA_TOOL_RESULT_SAMPLES") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("before.session_read.reply.json"), before_bytes).unwrap();
        std::fs::write(dir.join("after.session_read.reply.json"), after_bytes).unwrap();
    }
}
