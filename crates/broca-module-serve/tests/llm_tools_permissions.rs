use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use broca_module_serve::llm_tools::{
    consent::{Consent, ConsentIo, RunLiveness},
    permissions::Permissions,
};
use broca_protocol::{BindIdentity, BindPrincipal, SessionScope};
use broca_store::grants::*;
use broca_subc::{AnswersPage, ConsentError, ConsentRecord, RouteOpenRetry, SubcConnection};
use serde_json::{json, Value};

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

fn scope(run: &str) -> SessionScope {
    SessionScope {
        owner: BindPrincipal::Reserved {
            module_id: "owner".into(),
        },
        scope_ref: run.into(),
        scope_epoch: 1,
        agent_id: Some("agent".into()),
        run_id: Some(run.into()),
        flow_id: None,
        sender: None,
    }
}

fn identity(run: &str) -> ScopeIdentity {
    ScopeIdentity {
        owner: "reserved:owner".into(),
        scope_ref: run.into(),
        scope_epoch: 1,
    }
}

fn seed(store: &GrantsStore, key: &str, run: &str, decision: Decision, at: i64) -> Grant {
    store
        .put_card(&Card {
            dedup_key: key.into(),
            scope: identity(run),
            agent_id: "agent".into(),
            provider: "typesafe".into(),
            content: "{}".into(),
            card_id: Some(format!("card-{key}")),
            state: CardState::Pending,
        })
        .unwrap();
    store
        .apply_answer(
            &identity(run),
            CardAnswer {
                dedup_key: key.into(),
                card_id: format!("card-{key}"),
                decision,
                run_live: true,
            },
            at,
        )
        .unwrap();
    store
        .grants()
        .unwrap()
        .into_iter()
        .find(|g| g.source_card_id == format!("card-{key}"))
        .unwrap()
}

fn request(name: &str, arguments: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"name":name,"arguments":arguments})).unwrap()
}

async fn call(permissions: &Permissions, name: &str, arguments: Value) -> Value {
    let reply = permissions
        .answer(&request(name, arguments), Some(&BindPrincipal::Direct))
        .await
        .unwrap();
    serde_json::from_str(reply.get()).unwrap()
}

#[derive(Default)]
struct Io {
    requests: Mutex<Vec<Value>>,
    ended: Mutex<Vec<String>>,
    unknown: Mutex<Vec<String>>,
    answer: Mutex<Option<ConsentRecord>>,
}

#[async_trait::async_trait]
impl ConsentIo for Io {
    async fn request(&self, _: &SessionScope, body: &Value) -> Result<String, ConsentError> {
        let mut requests = self.requests.lock().unwrap();
        requests.push(body.clone());
        Ok(format!("new-card-{}", requests.len()))
    }
    async fn answers(&self, _: &SessionScope, _: u64) -> Result<AnswersPage, ConsentError> {
        Ok(AnswersPage {
            records: vec![],
            cursor: 0,
            continuation: None,
        })
    }
    async fn wait(&self, _: &SessionScope, _: &str) -> Result<ConsentRecord, ConsentError> {
        self.answer
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| ConsentError::Transport("no answer".into()))
    }
    async fn withdraw(&self, _: &SessionScope, _: &str) {}
    async fn run_live(&self, scope: &SessionScope) -> RunLiveness {
        if scope.scope_epoch != 1 || self.ended.lock().unwrap().contains(&scope.scope_ref) {
            RunLiveness::Ended
        } else if self.unknown.lock().unwrap().contains(&scope.scope_ref) {
            RunLiveness::Unknown
        } else {
            RunLiveness::Live
        }
    }
}

#[tokio::test]
async fn only_stamped_operator_principals_can_read_or_revoke() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let grant = seed(&store, "grant", "a", Decision::Allow24h, now());
    let permissions = Permissions::new(store.clone(), None);
    let agent = serde_json::from_value::<BindPrincipal>(json!({"kind":"agent","agent_id":"agent"}))
        .unwrap();
    for principal in [
        None,
        Some(agent),
        Some(BindPrincipal::Unverified),
        Some(BindPrincipal::Reserved {
            module_id: "broca".into(),
        }),
        Some(BindPrincipal::Reserved {
            module_id: "basal".into(),
        }),
    ] {
        for body in [
            request("permissions.list", json!({})),
            request("permissions.revoke", json!({"id":grant.id})),
            b"not JSON".to_vec(),
        ] {
            let error: Value = serde_json::from_slice(
                &permissions
                    .answer(&body, principal.as_ref())
                    .await
                    .unwrap_err(),
            )
            .unwrap();
            assert_eq!(error["code"], "permissions_operator_required");
            assert_eq!(error["detail"]["class"], "permanent");
        }
    }
    assert_eq!(store.grants().unwrap(), vec![grant]);
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 0);
    let cingulate = BindPrincipal::Reserved {
        module_id: "cingulate".into(),
    };
    assert!(permissions
        .answer(&request("permissions.list", json!({})), Some(&cingulate))
        .await
        .is_ok());
}

#[tokio::test]
async fn list_filters_agent_expiry_and_run_liveness_and_projects_labels() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let at = now();
    let live = seed(&store, "live", "a", Decision::AllowRun, at);
    seed(&store, "ended", "ended", Decision::AllowRun, at);
    seed(&store, "expired", "b", Decision::Allow24h, at - 86_400_000);
    let io = Arc::new(Io::default());
    io.ended.lock().unwrap().push("ended".into());
    let permissions = Permissions::new(store, Some(io));
    assert_eq!(
        call(
            &permissions,
            "permissions.list",
            json!({"agent_id":"another"})
        )
        .await,
        json!({"grants":[]})
    );
    assert_eq!(
        call(
            &permissions,
            "permissions.list",
            json!({"agent_id":"agent"})
        )
        .await,
        json!({"grants":[{
            "id":live.id, "agent_id":"agent", "holder":{"kind":"agent","agent_id":"agent"},
            "module":"broca", "subject_label":"Private content", "scope":"provider",
            "scope_label":"Send private content to typesafe for this program run", "granted_at_ms":at, "card_ref":"card-live"
        }]})
    );
}

struct Rig {
    _daemon: broca_test_support::RigChild,
    root: PathBuf,
    state: PathBuf,
}

impl Rig {
    async fn start(name: &str) -> Option<Self> {
        let subc = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        if !subc.exists() {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
                "required real daemon missing: {}",
                subc.display()
            );
            eprintln!("SKIP {name}: real daemon missing");
            return None;
        }
        let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        assert!(stub.exists(), "principal fixture missing");
        let root = broca_test_support::scratch_root()
            .join(format!("permissions-{name}-{}", std::process::id()));
        broca_test_support::remove_rig_dir(&root);
        for dir in [
            "config/cortexkit",
            "runtime",
            "data",
            "home",
            "state/catalog",
        ] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        let state = root.join("state");
        std::fs::write(state.join("catalog/fusiform-lkg.json"), json!({"catalog_version":1,"resolved_at_ms":1,"source":"models.dev","models":{"typesafe/jev-1.13.0":{"existence":"present","model.kind":"decision","rate.input":{"state":"stated_zero"},"rate.output":{"state":"stated_zero"}}}}).to_string()).unwrap();
        let config = root.join("config/cortexkit/broca.jsonc");
        std::fs::write(&config, "{}").unwrap();
        let mut modules = json!({});
        for id in ["cingulate", "basal", "owner"] {
            modules[id] = json!({"program":broca_test_support::ckdev_binary(&stub, broca_test_support::scratch_root()),"args":[],"reserved":true,"enabled":true,"env":{
                "FAKE_AFT_MODULE_ID":id,"FAKE_AFT_LAUNCH_NONCE_PATH":root.join(format!("{id}.nonce")),"FAKE_AFT_EVENTS_PATH":root.join(format!("{id}.events"))
            }});
        }
        modules["broca"] = json!({"program":broca_test_support::ckdev_binary(Path::new(env!("CARGO_BIN_EXE_ck-broca")), broca_test_support::scratch_root()),"args":[],"enabled":true,"env":{"BROCA_STATE_ROOT":state,"BROCA_CONFIG_PATH":config}});
        std::fs::write(
            root.join("config/cortexkit/subc.jsonc"),
            json!({"version":1,"modules":modules}).to_string(),
        )
        .unwrap();
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(
                broca_test_support::ckdev_binary(&subc, broca_test_support::scratch_root()),
                &root,
                "daemon",
            )?
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env("XDG_RUNTIME_DIR", root.join("runtime"))
            .env("XDG_DATA_HOME", root.join("data"))
            .env("HOME", root.join("home"))
            .env("SUBC_PORT", "0")
            .env_remove("BROCA_STATE_ROOT")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
        })
        .unwrap();
        Some(Self {
            _daemon: broca_test_support::RigChild::new(child, &root, "daemon"),
            root,
            state,
        })
    }

    async fn connect(&self, module: Option<&str>) -> Arc<SubcConnection> {
        let identity = if let Some(id) = module {
            let nonce = broca_test_support::wait_until("permissions caller nonce", || async {
                std::fs::read_to_string(self.root.join(format!("{id}.nonce")))
                    .map_err(|e| e.to_string())
            })
            .await;
            Some(subc_control::ConsumerIdentity {
                module_id: id.into(),
                launch_nonce: nonce.trim().into(),
            })
        } else {
            None
        };
        Arc::new(
            broca_test_support::wait_until("permissions daemon connection", || {
                let identity = identity.clone();
                async move {
                    SubcConnection::connect_with_consumer_identity(
                        &self.root.join("runtime/subc-connection.json"),
                        identity,
                    )
                    .await
                    .map_err(|e| e.to_string())
                }
            })
            .await,
        )
    }

    async fn open(&self, connection: &SubcConnection, role: &str) -> broca_subc::RouteHandle {
        // Permissions do not create sessions, so empty harness and session names must work.
        let identity = BindIdentity::new(self.root.canonicalize().unwrap(), "", "");
        connection
            .route_open_management_roles_within(
                "broca",
                &identity,
                std::collections::BTreeMap::from([(role.into(), "v1".into())]),
                RouteOpenRetry::default_patience(),
            )
            .await
            .unwrap()
    }
}

async fn wire(
    connection: &SubcConnection,
    route: &broca_subc::RouteHandle,
    name: &str,
    args: Value,
) -> Result<Value, Value> {
    let response = broca_test_support::event_timeout(
        "permissions response",
        connection.route_request_terminal(route, request(name, args)),
        || format!("no response for {name}"),
    )
    .await
    .unwrap()
    .unwrap();
    response
        .map(|body| serde_json::from_slice(&body).unwrap())
        .map_err(|body| serde_json::from_slice(&body).unwrap())
}

fn rows(path: &Path, table: &str) -> Vec<String> {
    if !path.exists() {
        return vec![];
    }
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut statement = connection
        .prepare(&format!("SELECT * FROM {table}"))
        .unwrap();
    let columns = statement.column_count();
    let mut rows = statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| format!("{:?}", row.get_ref(i).unwrap()))
                .collect::<Vec<_>>()
                .join("|"))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows.sort();
    rows
}

fn artifacts(rig: &Rig) -> Value {
    let mut value = json!({});
    for (file, tables) in [
        (
            "system-one.db",
            vec!["batch", "item_outcome", "usage_outbox"],
        ),
        ("run-index.db", vec!["export_facts"]),
        (
            "grants.db",
            vec![
                "grants",
                "denies",
                "held_calls",
                "cards",
                "generations",
                "cursors",
            ],
        ),
    ] {
        for table in tables {
            value[file][table] = json!(rows(&rig.state.join(file), table));
        }
    }
    value["sessions"] = json!(std::fs::read_dir(rig.state.join("sessions"))
        .map(|e| e.count())
        .unwrap_or(0));
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_manifest_and_operator_calls_are_sessionless_and_role_isolated() {
    let Some(rig) = Rig::start("operators").await else {
        return;
    };
    let direct = rig.connect(None).await;
    let route = rig.open(&direct, "permissions").await;
    let catalog = direct.catalog_list().await.unwrap();
    let manifest = serde_json::to_value(
        catalog
            .iter()
            .find(|entry| entry.module_id == "broca")
            .unwrap(),
    )
    .unwrap();
    assert!(manifest["capabilities"]["provides"]
        .as_array()
        .unwrap()
        .contains(&json!("permissions/v1")));
    for (name, kind) in [
        ("permissions.list", "query"),
        ("permissions.revoke", "mutate"),
    ] {
        assert!(manifest["roles"]
            .as_array()
            .unwrap()
            .iter()
            .any(|role| role["operations"].as_array().is_some_and(|ops| ops
                .iter()
                .any(|op| op["name"] == name && op["kind"] == kind))));
    }
    let store = GrantsStore::open(rig.state.join("grants.db")).unwrap();
    let grant = seed(&store, "native", "a", Decision::Allow24h, now());
    let before = artifacts(&rig);
    assert_eq!(
        wire(&direct, &route, "permissions.list", json!({}))
            .await
            .unwrap()["grants"][0]["id"],
        grant.id
    );
    assert_eq!(artifacts(&rig), before);
    let cingulate = rig.connect(Some("cingulate")).await;
    let cingulate_route = rig.open(&cingulate, "permissions").await;
    assert_eq!(
        wire(
            &cingulate,
            &cingulate_route,
            "permissions.list",
            json!({"agent_id":"agent"})
        )
        .await
        .unwrap()["grants"][0]["holder"],
        json!({"kind":"agent","agent_id":"agent"})
    );
    assert_eq!(
        wire(
            &cingulate,
            &cingulate_route,
            "permissions.revoke",
            json!({"id":grant.id})
        )
        .await
        .unwrap(),
        json!({"outcome":"revoked"})
    );
    let after = artifacts(&rig);
    assert_eq!(after["sessions"], 0);
    for file in ["system-one.db", "run-index.db"] {
        assert_eq!(before[file], after[file]);
    }
    for table in ["held_calls", "cards", "denies", "cursors"] {
        assert_eq!(before["grants.db"][table], after["grants.db"][table]);
    }
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 1);
    assert_eq!(
        wire(
            &direct,
            &route,
            "permissions.revoke",
            json!({"id":grant.id})
        )
        .await
        .unwrap(),
        json!({"outcome":"not_found"})
    );
    assert_eq!(
        wire(&direct, &route, "tool.catalog", json!({}))
            .await
            .unwrap_err()["code"],
        "unknown_method"
    );
    let tools = rig.open(&direct, "tool-provider").await;
    assert_eq!(
        wire(&direct, &tools, "permissions.list", json!({}))
            .await
            .unwrap_err()["code"],
        "unknown_method"
    );
    assert_eq!(artifacts(&rig), after);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_other_reserved_callers_cannot_read_or_revoke_and_write_nothing() {
    let Some(rig) = Rig::start("refused").await else {
        return;
    };
    let direct = rig.connect(None).await;
    let _route = rig.open(&direct, "permissions").await;
    let store = GrantsStore::open(rig.state.join("grants.db")).unwrap();
    let grant = seed(&store, "native", "a", Decision::Allow24h, now());
    let before = artifacts(&rig);
    for module in ["basal", "owner"] {
        let connection = rig.connect(Some(module)).await;
        let route = rig.open(&connection, "permissions").await;
        for (name, args) in [
            ("permissions.list", json!({})),
            ("permissions.revoke", json!({"id":grant.id})),
        ] {
            assert_eq!(
                wire(&connection, &route, name, args).await.unwrap_err()["code"],
                "permissions_operator_required"
            );
        }
    }
    assert_eq!(artifacts(&rig), before);
    assert_eq!(before["sessions"], 0);
}

#[tokio::test]
async fn revoke_unknown_expired_and_repeated_ids_returns_not_found_without_generation_change() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let expired = seed(
        &store,
        "expired",
        "a",
        Decision::Allow24h,
        now() - 86_400_000,
    );
    let live = seed(&store, "live", "b", Decision::Allow24h, now());
    let permissions = Permissions::new(store.clone(), None);
    for id in ["absent", expired.id.as_str()] {
        assert_eq!(
            call(&permissions, "permissions.revoke", json!({"id":id})).await,
            json!({"outcome":"not_found"})
        );
    }
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 0);
    assert_eq!(
        call(&permissions, "permissions.revoke", json!({"id":live.id})).await,
        json!({"outcome":"revoked"})
    );
    assert_eq!(
        call(&permissions, "permissions.revoke", json!({"id":live.id})).await,
        json!({"outcome":"not_found"})
    );
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 1);
}

#[tokio::test]
async fn overlapping_grants_revoke_only_the_named_grant() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let run = seed(&store, "run", "a", Decision::AllowRun, now());
    let hours = seed(&store, "hours", "b", Decision::Allow24h, now());
    let io = Arc::new(Io::default());
    let consent = Consent::new(store.clone(), io.clone());
    let permissions = Permissions::new(store.clone(), Some(io));
    assert!(consent
        .granted(&scope("a"), "typesafe", now())
        .await
        .unwrap());
    assert_eq!(
        call(&permissions, "permissions.revoke", json!({"id":run.id})).await["outcome"],
        "revoked"
    );
    assert_eq!(store.grants().unwrap(), vec![hours.clone()]);
    assert!(consent
        .granted(&scope("a"), "typesafe", now())
        .await
        .unwrap());
    assert_eq!(
        call(&permissions, "permissions.revoke", json!({"id":hours.id})).await["outcome"],
        "revoked"
    );
    assert!(!consent
        .granted(&scope("a"), "typesafe", now())
        .await
        .unwrap());
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 2);
}

#[tokio::test]
async fn unknown_run_liveness_keeps_operator_visibility() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let grant = seed(&store, "unknown", "a", Decision::AllowRun, now());
    let io = Arc::new(Io::default());
    io.unknown.lock().unwrap().push("a".into());
    let permissions = Permissions::new(store.clone(), Some(io));
    assert_eq!(
        call(&permissions, "permissions.list", json!({})).await["grants"][0]["id"],
        grant.id
    );
    assert_eq!(
        call(
            &Permissions::new(store, None),
            "permissions.list",
            json!({})
        )
        .await["grants"][0]["id"],
        grant.id
    );
}

#[tokio::test]
async fn unknown_run_liveness_cannot_prevent_durable_revocation() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let grant = seed(&store, "unknown", "a", Decision::AllowRun, now());
    let io = Arc::new(Io::default());
    io.unknown.lock().unwrap().push("a".into());
    let permissions = Permissions::new(store.clone(), Some(io.clone()));
    assert_eq!(
        call(&permissions, "permissions.revoke", json!({"id":grant.id})).await,
        json!({"outcome":"revoked"})
    );
    assert!(store.grants().unwrap().is_empty());
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 1);
    io.unknown.lock().unwrap().clear();
    assert!(!Consent::new(store, io)
        .granted(&scope("a"), "typesafe", now())
        .await
        .unwrap());
}

#[tokio::test]
async fn unknown_liveness_cannot_mint_or_use_run_grants() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    seed(&store, "unknown", "a", Decision::AllowRun, now());
    let io = Arc::new(Io::default());
    io.unknown.lock().unwrap().push("a".into());
    let consent = Consent::new(store.clone(), io.clone());
    assert!(!consent
        .granted(&scope("a"), "typesafe", now())
        .await
        .unwrap());
    io.unknown.lock().unwrap().clear();
    assert!(consent
        .granted(&scope("a"), "typesafe", now())
        .await
        .unwrap());
    store.revoke(&store.grants().unwrap()[0].id).unwrap();
    io.unknown.lock().unwrap().push("a".into());
    let key = consent
        .dedup_key("agent", "typesafe", &identity("a"))
        .unwrap();
    let body = broca_subc::consent_card::private_content_card(
        "TypeSafe",
        "Jev",
        &[broca_subc::SourceKind::PersonBrowser],
        &key,
    )
    .unwrap();
    consent
        .file(
            HoldRequest {
                batch_id: "unknown-call".into(),
                scope: identity("a"),
                batch_scope: broca_store::system_one::BatchScope::Run {
                    agent_id: "agent".into(),
                    run_id: "a".into(),
                },
                principal: "reserved:owner".into(),
                call_key: "unknown-call".into(),
                agent_id: "agent".into(),
                provider: "typesafe".into(),
                model: "typesafe/jev-1.13.0".into(),
                request: "{}".into(),
                args_digest: "digest".into(),
                frozen_row: "{}".into(),
                dedup_key: key.clone(),
                admitted_at_ms: now(),
            },
            body,
            now(),
        )
        .await
        .unwrap();
    *io.answer.lock().unwrap() = Some(ConsentRecord {
        elicitation_id: "new-card-1".into(),
        state: broca_subc::ConsentState::Answered,
        fields: std::collections::BTreeMap::from([
            ("answered_choice_id".into(), json!("allow_run")),
            ("requester_ref".into(), json!(key)),
        ]),
    });
    consent.wait_once(&key, now()).await.unwrap();
    assert!(store.grants().unwrap().is_empty());
}

#[tokio::test]
async fn revoke_restart_changes_the_next_card_key_and_leaves_held_calls_waiting() {
    let root = broca_test_support::scratch_root()
        .join(format!("permissions-restart-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&root);
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("grants.db");
    let old_key;
    {
        let store = Arc::new(GrantsStore::open(&path).unwrap());
        let io = Arc::new(Io::default());
        let consent = Consent::new(store.clone(), io.clone());
        old_key = consent
            .dedup_key("agent", "typesafe", &identity("a"))
            .unwrap();
        let grant = seed(&store, &old_key, "a", Decision::AllowRun, now());
        let at = now();
        let hold = HoldRequest {
            batch_id: "waiting-batch".into(),
            scope: identity("a"),
            batch_scope: broca_store::system_one::BatchScope::Run {
                agent_id: "agent".into(),
                run_id: "a".into(),
            },
            principal: "reserved:owner".into(),
            call_key: "waiting".into(),
            agent_id: "agent".into(),
            provider: "typesafe".into(),
            model: "typesafe/jev-1.13.0".into(),
            request: "{}".into(),
            args_digest: "digest".into(),
            frozen_row: "{}".into(),
            dedup_key: "pending-card".into(),
            admitted_at_ms: at,
        };
        store.hold(hold).unwrap();
        store
            .put_card(&Card {
                dedup_key: "pending-card".into(),
                scope: identity("a"),
                agent_id: "agent".into(),
                provider: "typesafe".into(),
                content: "{}".into(),
                card_id: Some("still-waiting-card".into()),
                state: CardState::Pending,
            })
            .unwrap();
        let before = store.lookup("waiting-batch", at).unwrap();
        assert_eq!(before.as_ref().unwrap().state, HeldState::Held);
        assert_eq!(
            call(
                &Permissions::new(store.clone(), Some(io)),
                "permissions.revoke",
                json!({"id":grant.id})
            )
            .await["outcome"],
            "revoked"
        );
        assert_eq!(store.lookup("waiting-batch", at).unwrap(), before);
    }
    let store = Arc::new(GrantsStore::open(&path).unwrap());
    let io = Arc::new(Io::default());
    let consent = Consent::new(store.clone(), io.clone());
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 1);
    assert!(!consent
        .granted(&scope("a"), "typesafe", now())
        .await
        .unwrap());
    let new_key = consent
        .dedup_key("agent", "typesafe", &identity("a"))
        .unwrap();
    assert_ne!(old_key, new_key);
    let body = broca_subc::consent_card::private_content_card(
        "TypeSafe",
        "Jev",
        &[broca_subc::SourceKind::PersonBrowser],
        &new_key,
    )
    .unwrap();
    consent
        .file(
            HoldRequest {
                batch_id: "next-keyed-call".into(),
                scope: identity("a"),
                batch_scope: broca_store::system_one::BatchScope::Run {
                    agent_id: "agent".into(),
                    run_id: "a".into(),
                },
                principal: "reserved:owner".into(),
                call_key: "next-call-key".into(),
                agent_id: "agent".into(),
                provider: "typesafe".into(),
                model: "typesafe/jev-1.13.0".into(),
                request: "{}".into(),
                args_digest: "next-digest".into(),
                frozen_row: "{}".into(),
                dedup_key: new_key.clone(),
                admitted_at_ms: now(),
            },
            body,
            now(),
        )
        .await
        .unwrap();
    assert_eq!(io.requests.lock().unwrap().len(), 1);
    assert_eq!(io.requests.lock().unwrap()[0]["dedup_key"], new_key);
}
