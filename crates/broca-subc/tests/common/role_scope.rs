//! Scoped role-route tests connect as broca using a daemon-issued launch nonce.
//! The scope lists broca as a carrier, allowing it to open provider routes on the owner's behalf.

use std::sync::Arc;

use broca_protocol::SessionScope;
use broca_subc::SubcConnection;
use serde_json::json;

use super::{checkout_bin, ModuleSpec, Rig};

pub const OWNER: &str = "role-owner";

pub async fn start(tag: &str) -> (Rig, Arc<SubcConnection>, SessionScope) {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-rig-{}-{tag}", std::process::id()));
    let sync = dir.join("scopes.json");
    let events = dir.join("scope-events.jsonl");
    let nonce = dir.join("broca.nonce");
    let module = |id: &str, env| ModuleSpec {
        module_id: id.into(),
        program: checkout_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
        args: vec![],
        env,
    };
    let rig = Rig::start_with_scope_owners(
        tag,
        vec![
            module(
                OWNER,
                vec![
                    ("FAKE_AFT_MODULE_ID".into(), OWNER.into()),
                    (
                        "FAKE_AFT_SCOPE_SYNC_PATH".into(),
                        sync.display().to_string(),
                    ),
                    ("FAKE_AFT_EVENTS_PATH".into(), events.display().to_string()),
                ],
            ),
            module(
                "broca",
                vec![
                    ("FAKE_AFT_MODULE_ID".into(), "broca".into()),
                    (
                        "FAKE_AFT_LAUNCH_NONCE_PATH".into(),
                        nonce.display().to_string(),
                    ),
                ],
            ),
        ],
        &[OWNER],
    )
    .await
    .expect("real daemon and stub required");
    let workdir = rig.dir.canonicalize().unwrap();
    std::fs::write(
        &sync,
        serde_json::to_vec(&json!([
            {"ref":"role-scope", "scope_epoch":1, "kind":"head", "workdir":workdir,
             "attributes":{}, "carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]},
            {"ref":"other-scope", "scope_epoch":1, "kind":"head", "workdir":workdir,
             "attributes":{}, "carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]},
            {"ref":"denied-scope", "scope_epoch":1, "kind":"head", "workdir":workdir,
             "attributes":{}, "carriers":[]}
        ]))
        .unwrap(),
    )
    .unwrap();
    let launch_nonce =
        broca_test_support::wait_until("role scope sync accepted and carrier attested", || async {
            let text = std::fs::read_to_string(&events).unwrap_or_default();
            let reply = text
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .find(|event| event["corr"] == 1_000_001 && event["kind"] == "scope_sync_response")
                .ok_or("scope sync pending")?;
            assert!(
                reply["body_json"]["results"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|result| result["outcome"] != "refused"),
                "{reply}"
            );
            let nonce = std::fs::read_to_string(&nonce).map_err(|_| "carrier nonce pending")?;
            if nonce.trim().is_empty() {
                Err("carrier nonce pending".into())
            } else {
                Ok(nonce.trim().to_owned())
            }
        })
        .await;
    let conn = SubcConnection::connect_with_consumer_identity(
        &rig.connection_file,
        Some(subc_control::ConsumerIdentity {
            module_id: "broca".into(),
            launch_nonce,
        }),
    )
    .await
    .unwrap();
    let scope = serde_json::from_value(
        json!({"owner":{"kind":"reserved","module_id":OWNER}, "ref":"role-scope", "scope_epoch":1}),
    )
    .unwrap();
    (rig, Arc::new(conn), scope)
}
