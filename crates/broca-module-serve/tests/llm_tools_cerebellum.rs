//! Opt-in wire-contract check against a real, daemon-supervised Cerebellum.
#![cfg(all(unix, debug_assertions))]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use broca_protocol::BindIdentity;
use broca_subc::{PrivateContent, PrivateContentClient, PrivateContentError, SubcConnection};
use serde_json::json;

fn cerebellum_binary() -> Option<PathBuf> {
    let path = std::env::var_os("CEREBELLUM_BIN")
        .map(PathBuf::from)
        .filter(|path| path.is_file());
    if path.is_none() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_CEREBELLUM").is_none(),
            "required real-Cerebellum fixture missing: set CEREBELLUM_BIN"
        );
        eprintln!("SKIP real Cerebellum query: set CEREBELLUM_BIN; no real-module claim is made");
    }
    path
}

/// CEREBELLUM_ARGS_JSON and CEREBELLUM_ENV_JSON accept target-only configuration, as in
/// the real-Cingulate lane. The daemon's XDG paths point into its scratch root; target-only
/// environment overrides can replace them.
#[tokio::test]
async fn real_cerebellum_answers_unmarked_scope() {
    let Some(cerebellum) = cerebellum_binary() else {
        return;
    };
    let subc = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
    let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
    assert!(
        subc.is_file(),
        "set SUBC_CORE_BIN to the real ck-subc binary"
    );
    assert!(
        stub.is_file(),
        "set FAKE_AFT_STUB_BIN to the module identity fixture"
    );
    let root = broca_test_support::scratch_root()
        .join(format!("real-cerebellum-query-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&root);
    for dir in ["config/cortexkit", "runtime", "data", "home", "project"] {
        std::fs::create_dir_all(root.join(dir)).unwrap();
    }
    let args: Vec<String> = serde_json::from_str(
        &std::env::var("CEREBELLUM_ARGS_JSON").unwrap_or_else(|_| "[]".into()),
    )
    .expect("CEREBELLUM_ARGS_JSON is a JSON argv array");
    let extra_env: BTreeMap<String, String> =
        serde_json::from_str(&std::env::var("CEREBELLUM_ENV_JSON").unwrap_or_else(|_| "{}".into()))
            .expect("CEREBELLUM_ENV_JSON is a JSON string map");
    // The stub registers broca's reserved identity; the client connects with its daemon-issued
    // nonce. Cerebellum still serves the real management route and decodes the real request.
    let modules = json!({
        "broca": {
            "program": broca_test_support::ckdev_binary(&stub, broca_test_support::scratch_root()),
            "args": [], "reserved": true, "enabled": true,
            "env": {"FAKE_AFT_MODULE_ID": "broca", "FAKE_AFT_LAUNCH_NONCE_PATH": root.join("broca.nonce"), "FAKE_AFT_EVENTS_PATH": root.join("broca.events")}
        },
        "cerebellum": {
            "program": broca_test_support::ckdev_binary(&cerebellum, broca_test_support::scratch_root()),
            "args": args, "env": extra_env, "reserved": true, "enabled": true
        }
    });
    std::fs::write(
        root.join("config/cortexkit/subc.jsonc"),
        json!({"version": 1, "modules": modules}).to_string(),
    )
    .unwrap();
    let child = broca_test_support::tokio_rig_command(
        broca_test_support::ckdev_binary(subc, broca_test_support::scratch_root()),
        &root,
        "daemon",
    )
    .unwrap()
    .env("XDG_CONFIG_HOME", root.join("config"))
    .env("XDG_DATA_HOME", root.join("data"))
    .env("XDG_RUNTIME_DIR", root.join("runtime"))
    .env("HOME", root.join("home"))
    .env("SUBC_PORT", "0")
    .env_remove("SUBC_MODULE_ID")
    .env_remove("SUBC_LAUNCH_NONCE")
    .env_remove("SUBC_LAUNCH_NONCE_FD")
    .env_remove("BROCA_STATE_ROOT")
    .kill_on_drop(true)
    .spawn()
    .unwrap();
    let mut daemon = broca_test_support::RigChild::new(child, &root, "daemon");
    let nonce = broca_test_support::wait_until("broca identity registered", || async {
        std::fs::read_to_string(root.join("broca.nonce")).map_err(|error| error.to_string())
    })
    .await;
    let conn = Arc::new(
        SubcConnection::connect_with_consumer_identity(
            &root.join("runtime/subc-connection.json"),
            Some(subc_control::ConsumerIdentity {
                module_id: "broca".into(),
                launch_nonce: nonce.trim().into(),
            }),
        )
        .await
        .unwrap(),
    );
    let client = PrivateContentClient::new(
        conn,
        BindIdentity::new(root.join("project"), "test", "real-cerebellum-query"),
    );
    let scope_ref = format!("unmarked-cerebellum-contract-{}", std::process::id());
    let answer = broca_test_support::wait_until("real Cerebellum management route", || async {
        match client.query(&scope_ref, None).await {
            Ok(answer) => Ok(answer),
            // The daemon can register broca before Cerebellum. Retry only route startup;
            // a refused, malformed or unanswered query is not contract evidence.
            Err(PrivateContentError::Route(error)) => Err(error),
            Err(error) => panic!("real Cerebellum did not answer the query: {error}"),
        }
    })
    .await;
    assert_eq!(answer, PrivateContent::unmarked());
    broca_test_support::kill_and_reap_tree(&mut daemon);
}
