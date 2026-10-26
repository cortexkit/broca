//! Native process cuts around the durable consent coordinator, plus the optional target
//! Cingulate admission test. The latter requires real supervised module binaries.

#![cfg(all(unix, debug_assertions))]

use broca_module_serve::llm_tools::consent::{Consent, ConsentIo};
use broca_protocol::SessionScope;
use broca_store::grants::*;
use broca_subc::{AnswersPage, ConsentError, ConsentRecord, ConsentState};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct DiskIo {
    root: PathBuf,
    stop_after_request: bool,
}
#[async_trait::async_trait]
impl ConsentIo for DiskIo {
    async fn request(&self, _: &SessionScope, _: &Value) -> Result<String, ConsentError> {
        // Model Cingulate's durable pending-key refresh: refile returns the same deciding id.
        std::fs::write(self.root.join("remote-card"), "one-deciding-card").unwrap();
        if self.stop_after_request {
            println!("CONSENT_REQUEST_ACCEPTED_BEFORE_REPLY");
            use std::io::Write;
            std::io::stdout().flush().unwrap();
            std::future::pending::<()>().await;
        }
        Ok("one-deciding-card".into())
    }
    async fn answers(&self, _: &SessionScope, _: u64) -> Result<AnswersPage, ConsentError> {
        Ok(AnswersPage {
            records: if self.root.join("answered").exists() {
                vec![serde_json::to_value(answer()).unwrap()]
            } else {
                vec![]
            },
            cursor: 1,
            continuation: None,
        })
    }
    async fn wait(&self, _: &SessionScope, _: &str) -> Result<ConsentRecord, ConsentError> {
        Ok(answer())
    }
    async fn withdraw(&self, _: &SessionScope, id: &str) {
        std::fs::write(self.root.join("withdrawn"), id).unwrap();
    }
    async fn run_live(
        &self,
        _: &SessionScope,
    ) -> broca_module_serve::llm_tools::consent::RunLiveness {
        broca_module_serve::llm_tools::consent::RunLiveness::Live
    }
}

fn answer() -> ConsentRecord {
    ConsentRecord {
        elicitation_id: "one-deciding-card".into(),
        state: ConsentState::Answered,
        fields: BTreeMap::from([
            ("requester_ref".into(), json!(KEY)),
            ("answered_choice_id".into(), json!("allow_run")),
        ]),
    }
}

#[tokio::test]
#[ignore = "subprocess helper, invoked only by the native kill tests"]
async fn consent_request_cut_helper() {
    let root = PathBuf::from(std::env::var_os("BROCA_CONSENT_CUT_ROOT").expect("cut root"));
    let store = Arc::new(GrantsStore::open(root.join("grants.db")).unwrap());
    let consent = Consent::new(
        store,
        Arc::new(DiskIo {
            root,
            stop_after_request: true,
        }),
    )
    .with_clock(Arc::new(|| 1000));
    let request = HoldRequest {
        batch_id: "call".into(),
        scope: ScopeIdentity {
            owner: "reserved:owner".into(),
            scope_ref: "run".into(),
            scope_epoch: 1,
        },
        batch_scope: broca_store::system_one::BatchScope::Run {
            agent_id: "agent".into(),
            run_id: "run".into(),
        },
        principal: "reserved:caller".into(),
        call_key: "raw:key".into(),
        agent_id: "agent".into(),
        provider: "typesafe".into(),
        model: "typesafe/jev-1.13.0".into(),
        request: "validated".into(),
        args_digest: "digest".into(),
        frozen_row: "{}".into(),
        dedup_key: KEY.into(),
        admitted_at_ms: 1000,
    };
    let body = broca_subc::consent_card::private_content_card(
        "TypeSafe",
        "Jev",
        &[broca_subc::SourceKind::PersonBrowser],
        KEY,
    )
    .unwrap();
    consent.file(request, body, 1000).await.unwrap();
    panic!("cut helper must be killed before recording a reply");
}

async fn cut(answer_while_down: bool) {
    let root = broca_test_support::scratch_root().join(format!(
        "consent-cut-{}-{answer_while_down}",
        std::process::id()
    ));
    broca_test_support::remove_rig_dir(&root);
    std::fs::create_dir_all(&root).unwrap();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "consent_request_cut_helper",
            "--ignored",
            "--nocapture",
        ])
        .env("BROCA_CONSENT_CUT_ROOT", &root)
        .env("BROCA_STATE_ROOT", root.join("broca-state"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if line.contains("CONSENT_REQUEST_ACCEPTED_BEFORE_REPLY") {
                return;
            }
        }
        panic!("helper exited before reaching consent cut");
    })
    .await
    .expect("native helper did not reach cut");
    child.start_kill().unwrap();
    assert!(!child.wait().await.unwrap().success());
    let store = Arc::new(GrantsStore::open(root.join("grants.db")).unwrap());
    let row = store.lookup("call", 1000).unwrap().unwrap();
    assert_eq!(row.state, HeldState::Filing);
    assert!(row.card_id.is_none());
    if answer_while_down {
        std::fs::write(root.join("answered"), "allow_run").unwrap();
    }
    let consent = Consent::new(
        store.clone(),
        Arc::new(DiskIo {
            root: root.clone(),
            stop_after_request: false,
        }),
    )
    .with_clock(Arc::new(|| 1000));
    consent.recover(KEY, 1000).await.unwrap();
    if !answer_while_down {
        assert_eq!(
            store.lookup("call", 1000).unwrap().unwrap().state,
            HeldState::Held
        );
        consent.wait_once(KEY, 1000).await.unwrap();
    }
    assert_eq!(
        store.lookup("call", 1000).unwrap().unwrap().state,
        HeldState::Authorized
    );
    assert_eq!(store.grants().unwrap().len(), 1);
    assert_eq!(
        store.grants().unwrap()[0].source_card_id,
        "one-deciding-card"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("remote-card")).unwrap(),
        "one-deciding-card"
    );
}

#[tokio::test]
async fn native_kill_before_card_id_pending_recovers_one_deciding_card() {
    cut(false).await;
}

#[tokio::test]
async fn native_kill_before_card_id_answered_while_down_adopts_answer() {
    cut(true).await;
}

/// A real Cingulate is not part of the shared fixture set, so this test has its own switch,
/// like the Magic Context end-to-end test: `BROCA_REQUIRE_CINGULATE=1` turns a missing binary
/// into a failure, and otherwise it skips loudly. `BROCA_REQUIRE_SUBC` alone must not, because
/// CI requires the daemon fixtures but has no Cingulate build.
fn required_binary(variable: &str) -> Option<PathBuf> {
    let path = std::env::var_os(variable)
        .map(PathBuf::from)
        .filter(|path| path.is_file());
    if path.is_none() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_CINGULATE").is_none(),
            "required real-Cingulate fixture missing: set {variable}"
        );
        eprintln!(
            "SKIP target Cingulate admission: set {variable}; no target-daemon claim is made"
        );
    }
    path
}

/// CINGULATE_ARGS_JSON is the target owner's argv prefix (default []). The supervisor adds
/// its normal module-id, connection and nonce arguments. CINGULATE_ENV_JSON adds target-only
/// configuration; all writable XDG state remains under this test's scratch root.
#[tokio::test]
async fn target_cingulate_admits_reserved_broca_with_private_content_card_scope() {
    let Some(cingulate) = required_binary("CINGULATE_BIN") else {
        return;
    };
    // The stand-in cerebellum is the shared fixture `FAKE_SURFACE_BIN`; `S6_SURFACE_BIN`
    // overrides it for a run that wants a different build.
    let surface_variable = if std::env::var_os("S6_SURFACE_BIN").is_some() {
        "S6_SURFACE_BIN"
    } else {
        "FAKE_SURFACE_BIN"
    };
    let Some(surface) = required_binary(surface_variable) else {
        return;
    };
    let subc = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
    assert!(
        subc.is_file(),
        "set SUBC_CORE_BIN to the real ck-subc binary"
    );
    let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
    assert!(
        stub.is_file(),
        "set FAKE_AFT_STUB_BIN to the scope authority fixture"
    );
    let root = broca_test_support::scratch_root()
        .join(format!("cingulate-broca-admission-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&root);
    for dir in [
        "config/cortexkit",
        "runtime",
        "data",
        "home",
        "project",
        "state/catalog",
    ] {
        std::fs::create_dir_all(root.join(dir)).unwrap();
    }
    let state = root.join("state");
    std::fs::write(state.join("catalog/fusiform-lkg.json"), json!({"catalog_version":1,"resolved_at_ms":1,"source":"models.dev","models":{"typesafe/jev-1.13.0":{"existence":"present","model.kind":"decision","model.name":"Jev 1.13","provider.name":"TypeSafe","rate.input":{"currency":"USD","exponent":9,"state":"priced","units":42_000_000,"unit_provenance":{"kind":"stated"}},"rate.output":{"state":"stated_zero"}}}}).to_string()).unwrap();
    std::fs::write(
        root.join("config/cortexkit/broca.jsonc"),
        json!({"private_content_providers":[],"llm_tools":{"advertise_tags":true}}).to_string(),
    )
    .unwrap();
    let scopes = root.join("scopes.json");
    std::fs::write(&scopes, json!([{"ref":"run","scope_epoch":1,"kind":"head","attributes":{"agent_id":"example-agent","run_id":"run"},"carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]}]).to_string()).unwrap();
    let fixtures = root.join("cerebellum.json");
    // Cerebellum fbcc6766, crates/cerebellum/tests/vectors/private-content-query-v1/success-marked.json.
    // Whole-file SHA-256 (provenance only, not body bytes):
    // eb4c452cadd0b5d48e8855db366c599c3dcccb3408da1ea403fa5d8703032d1c.
    std::fs::write(&fixtures, json!([{"match":{"method":"private_content.query"},"stdout":"{\"private_content\":true,\"sources\":[\"signed_in_site\"]}"}]).to_string()).unwrap();
    let handoff = root.join("cerebellum.sh");
    std::fs::write(&handoff, format!("#!/bin/sh\nwhile [ \"$#\" -gt 0 ]; do case \"$1\" in --subc) export SUBC_CONNECTION=\"$2\"; shift 2;; *) shift;; esac; done\nexec '{}' --module-id cerebellum --SURFACE_FIXTURES={} --SURFACE_REQUESTS={}\n", surface.display(), hex_path(&fixtures), hex_path(&root.join("queries")))).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&handoff, std::fs::Permissions::from_mode(0o755)).unwrap();
    let args: Vec<String> =
        serde_json::from_str(&std::env::var("CINGULATE_ARGS_JSON").unwrap_or_else(|_| "[]".into()))
            .expect("CINGULATE_ARGS_JSON is a JSON argv array");
    let extra_env: BTreeMap<String, String> =
        serde_json::from_str(&std::env::var("CINGULATE_ENV_JSON").unwrap_or_else(|_| "{}".into()))
            .expect("CINGULATE_ENV_JSON is a JSON string map");
    let modules = json!({
        "owner":{"program":broca_test_support::ckdev_binary(&stub, broca_test_support::scratch_root()),"args":[],"reserved":true,"enabled":true,"env":{"FAKE_AFT_MODULE_ID":"owner","FAKE_AFT_SCOPE_SYNC_PATH":scopes,"FAKE_AFT_LAUNCH_NONCE_PATH":root.join("owner.nonce"),"FAKE_AFT_EVENTS_PATH":root.join("owner.events")}},
        "cerebellum":{"program":handoff,"args":[],"reserved":true,"enabled":true},
        "cingulate":{"program":broca_test_support::ckdev_binary(&cingulate, broca_test_support::scratch_root()),"args":args,"env":extra_env,"reserved":true,"enabled":true},
        "broca":{"program":broca_test_support::ckdev_binary(Path::new(env!("CARGO_BIN_EXE_ck-broca")), broca_test_support::scratch_root()),"args":[],"reserved":true,"enabled":true,"env":{"BROCA_STATE_ROOT":state,"BROCA_CONFIG_PATH":root.join("config/cortexkit/broca.jsonc")}}
    });
    std::fs::write(
        root.join("config/cortexkit/subc.jsonc"),
        json!({"version":1,"scope_authority_owners":["owner"],"modules":modules}).to_string(),
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
    let nonce = broca_test_support::wait_until("scope authority registered", || async {
        std::fs::read_to_string(root.join("owner.nonce")).map_err(|e| e.to_string())
    })
    .await;
    let conn = Arc::new(
        broca_subc::SubcConnection::connect_with_consumer_identity(
            &root.join("runtime/subc-connection.json"),
            Some(subc_control::ConsumerIdentity {
                module_id: "owner".into(),
                launch_nonce: nonce.trim().into(),
            }),
        )
        .await
        .unwrap(),
    );
    let selector = subc_protocol::scope::ScopeSelector::new(
        subc_protocol::Principal::Reserved {
            module_id: "owner".into(),
        },
        "run",
        Some(1),
    );
    let route = broca_test_support::wait_until("broca tool route", || async {
        conn.route_open_tool_provider_held_v1_for_tests(
            "broca",
            &broca_protocol::BindIdentity::new(root.join("project"), "test", "cingulate-admission"),
            Some(selector.clone()),
        )
        .await
        .map_err(|e| e.to_string())
    })
    .await;
    let request = json!({"name":"llm_classify","arguments":{"model":"typesafe/jev-1.13.0","questions":{"q":{"type":"noul","instructions":"Likely?"}},"item":{"state":"example private content"},"max_cost_usd":1},"call_key":"cingulate-admission"});
    // broca's route can open before the stand-in cerebellum has registered, and a call in that
    // window fails closed as unreachable without writing anything, so it is asked again, as the
    // private-content tests do. Any other refusal is the answer and fails the test.
    let reply = broca_test_support::wait_until("a call that reaches cerebellum", || async {
        match conn
            .route_unary_reply(
                &route,
                serde_json::to_vec(&request).unwrap(),
                Duration::from_secs(30),
            )
            .await
            .map_err(|error| format!("{error:?}"))?
        {
            Ok(reply) => Ok(reply),
            Err(error)
                if error
                    .detail
                    .as_ref()
                    .and_then(|detail| detail.get("consent"))
                    .and_then(Value::as_str)
                    == Some("private_content_unreachable") =>
            {
                Err(format!("cerebellum is not reachable yet: {error:?}"))
            }
            Err(error) => panic!("the admission call was refused: {error:?}"),
        }
    })
    .await;
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert_eq!(
        reply["held"]["reason"], "consent",
        "target Cingulate must admit reserved:broca: {reply}"
    );
    let grants = GrantsStore::open(state.join("grants.db")).unwrap();
    let cards = grants.pending_cards().unwrap();
    assert_eq!(cards.len(), 1);
    assert_eq!(
        cards[0].state,
        CardState::Pending,
        "a lost or refused reply is not admission proof"
    );
    assert!(cards[0].card_id.as_deref().is_some_and(|id| !id.is_empty()));
    let _ = conn.route_close(&route).await;
    broca_test_support::kill_and_reap_tree(&mut daemon);
}

fn hex_path(path: &Path) -> String {
    path.to_string_lossy()
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
