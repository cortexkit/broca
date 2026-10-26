use std::sync::{
    atomic::{AtomicI64, AtomicUsize, Ordering},
    Arc,
};

use broca_module_serve::{
    llm_tools::{
        held::{not_started, HeldCalls},
        LlmTools, NoPrivateContent,
    },
    system_one::{
        BatchCatalog, ClassifierProvider, Config, ModelPin, ProviderReply, SystemOne,
        LLM_CLASSIFY_STORE_ROLE,
    },
};
use broca_protocol::{BindPrincipal, BindScope, SendScope, SessionScope};
use broca_store::{grants::*, system_one::BatchScope};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const SEVEN_DAYS_MS: i64 = 604_800_000;

fn scope() -> ScopeIdentity {
    ScopeIdentity {
        owner: "reserved:owner".into(),
        scope_ref: "run".into(),
        scope_epoch: 1,
    }
}

fn principal(name: &str) -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: name.into(),
    }
}

fn stamp() -> SendScope {
    SendScope::Stamped(BindScope {
        scope: SessionScope {
            owner: principal("owner"),
            scope_ref: "run".into(),
            scope_epoch: 1,
            flow_id: None,
            sender: None,
            agent_id: Some("agent".into()),
            run_id: Some("run".into()),
        },
        owner_authorized: true,
    })
}

fn hash(value: Value) -> String {
    format!("{:x}", Sha256::digest(serde_jcs::to_vec(&value).unwrap()))
}

fn arguments() -> Value {
    json!({"model":"typesafe/jev-1.13.0","questions":{"q":{"type":"noul","instructions":"Is this likely?"}},"item":{"state":"hello"},"max_cost_usd":1})
}

fn request(who: &str, key: &str, at: i64) -> HoldRequest {
    let args = arguments();
    let batch_id = hash(
        json!({"principal":format!("reserved:{who}"),"owner":"reserved:owner","scope_ref":"run","scope_epoch":1,"call_key":key}),
    );
    let params = json!({"batch_id":batch_id,"model":args["model"],"questions":args["questions"],"items":[args["item"].clone()],"max_cost_usd":args["max_cost_usd"]}).to_string();
    HoldRequest { batch_id, scope: scope(), batch_scope: BatchScope::Run { agent_id: "agent".into(), run_id: "run".into() },
        principal: format!("reserved:{who}"), call_key: key.into(), agent_id: "agent".into(), provider: "typesafe".into(), model: "typesafe/jev-1.13.0".into(),
        request: params, args_digest: hash(args), frozen_row: json!({"input_rate_nanos":42_000_000,"output_rate_nanos":0,"charge_basis":"metered_api"}).to_string(),
        dedup_key: "one-card".into(), admitted_at_ms: at }
}

fn insert(store: &GrantsStore, who: &str, key: &str, at: i64) -> HeldCall {
    match store.hold(request(who, key, at)).unwrap() {
        HoldAdmission::Inserted(row) => row,
        other => panic!("unexpected admission: {other:?}"),
    }
}

fn card() -> Card {
    Card {
        dedup_key: "one-card".into(),
        scope: scope(),
        agent_id: "agent".into(),
        provider: "typesafe".into(),
        content: "approved card text".into(),
        card_id: Some("card-1".into()),
        state: CardState::Pending,
    }
}

#[test]
fn schema_and_private_file_survive_reopen() {
    let root = broca_test_support::scratch_root();
    let path = root.join("grants.db");
    let store = GrantsStore::open(&path).unwrap();
    let row = insert(&store, "p1", "raw:not-hex", 1000);
    drop(store);
    let conn = rusqlite::Connection::open(&path).unwrap();
    for table in [
        "grants",
        "denies",
        "generations",
        "cards",
        "held_calls",
        "cursors",
        "late_results",
    ] {
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1,
            "missing table {table}"
        );
    }
    let project_columns: i64 = conn
        .query_row(
            "SELECT count(*) FROM pragma_table_info('grants') WHERE name='project' AND [notnull]=0",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(project_columns, 1);
    drop(conn);
    let reopened = GrantsStore::open(&path).unwrap();
    assert_eq!(reopened.lookup(&row.batch_id, 1000).unwrap(), Some(row));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[test]
fn joining_replacement_and_restart_never_extend_individual_deadlines() {
    let root = broca_test_support::scratch_root();
    let path = root.join("deadlines.db");
    let store = GrantsStore::open(&path).unwrap();
    let first = insert(&store, "p1", "first", 1000);
    store.put_card(&card()).unwrap();
    let second = insert(&store, "p1", "second", 301_000);
    let mut replacement = card();
    replacement.card_id = Some("replacement".into());
    replacement.content = "another model".into();
    store.put_card(&replacement).unwrap();
    assert!(matches!(
        store.hold(request("p1", "first", 400_000)).unwrap(),
        HoldAdmission::Replay(_)
    ));
    drop(store);
    let held = HeldCalls::new(
        Arc::new(GrantsStore::open(&path).unwrap()),
        "restart".into(),
    );
    held.expire_due(601_000).unwrap();
    let expired = held
        .store
        .lookup(&first.batch_id, 601_000)
        .unwrap()
        .unwrap();
    let waiting = held
        .store
        .lookup(&second.batch_id, 601_000)
        .unwrap()
        .unwrap();
    assert_eq!(
        expired.deadline_at_ms, 601_000,
        "first deadline is fixed at admission plus ten minutes"
    );
    assert_eq!(expired.final_result.unwrap().body, not_started("pending"));
    assert_eq!(
        waiting.deadline_at_ms, 901_000,
        "joining call gets its own ten minutes"
    );
    assert_eq!(waiting.card_id.as_deref(), Some("replacement"));
    assert_eq!(waiting.state, HeldState::Held);
    held.expire_due(901_000).unwrap();
    assert_eq!(
        held.store
            .lookup(&second.batch_id, 901_000)
            .unwrap()
            .unwrap()
            .state,
        HeldState::Final
    );
}

#[test]
fn authorization_cas_survives_restart_and_late_deadline() {
    let root = broca_test_support::scratch_root();
    let path = root.join("authorized.db");
    let store = GrantsStore::open(&path).unwrap();
    let row = insert(&store, "p1", "race", 1000);
    store.put_card(&card()).unwrap();
    assert!(store.authorize(&row.batch_id, 600_999).unwrap());
    assert!(!store.authorize(&row.batch_id, 600_999).unwrap());
    drop(store);
    let held = HeldCalls::new(
        Arc::new(GrantsStore::open(&path).unwrap()),
        "restart".into(),
    );
    held.expire_due(900_000).unwrap();
    assert!(
        !held
            .store
            .finish_unstarted(&row.batch_id, &not_started("denied"), 900_000)
            .unwrap(),
        "authorized must never become not_started"
    );
    assert!(!held
        .store
        .expire_held(&row.batch_id, &not_started("pending"), 900_000)
        .unwrap());
    assert_eq!(
        held.store
            .lookup(&row.batch_id, 900_000)
            .unwrap()
            .unwrap()
            .state,
        HeldState::Authorized
    );
    assert!(held
        .store
        .complete_authorized(&row.batch_id, "{\"answer\":true}", 900_000)
        .unwrap());
    assert!(!held
        .store
        .complete_authorized(&row.batch_id, "{}", 900_001)
        .unwrap());
    assert_eq!(
        held.store
            .lookup(&row.batch_id, 900_001)
            .unwrap()
            .unwrap()
            .final_result
            .unwrap()
            .kind,
        FinalKind::Result
    );
}

#[test]
fn deadline_cas_prevents_late_authorization_and_double_settlement() {
    let store = GrantsStore::open_in_memory().unwrap();
    let row = insert(&store, "p1", "timeout", 1000);
    store.put_card(&card()).unwrap();
    assert!(!store.expire_held(&row.batch_id, "{}", 600_999).unwrap());
    assert!(!store.authorize(&row.batch_id, 601_000).unwrap());
    assert!(store
        .expire_held(&row.batch_id, &not_started("pending"), 601_000)
        .unwrap());
    assert!(!store.authorize(&row.batch_id, 601_001).unwrap());
    assert!(!store
        .finish_unstarted(&row.batch_id, &not_started("denied"), 601_001)
        .unwrap());
    assert_eq!(
        store
            .late_results("reserved:p1", 0, 10, 601_001)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn retention_clears_request_keeps_digest_and_ack_does_not_shorten_replay() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let row = insert(&store, "p1", "retained", 1000);
    store.put_card(&card()).unwrap();
    assert!(store
        .finish_unstarted(&row.batch_id, &not_started("denied"), 2000)
        .unwrap());
    let final_row = store.lookup(&row.batch_id, 2000).unwrap().unwrap();
    assert_eq!(
        final_row.request, None,
        "final rows must clear request content"
    );
    assert_eq!(final_row.args_digest, row.args_digest);
    let seq = store.late_head("reserved:p1").unwrap();
    assert_eq!(store.acknowledge("reserved:p1", seq).unwrap(), 1);
    assert!(store
        .late_results("reserved:p1", 0, 10, 3000)
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .lookup(&row.batch_id, 1000 + SEVEN_DAYS_MS - 1)
            .unwrap(),
        Some(final_row)
    );
    assert_eq!(store.sweep(1000 + SEVEN_DAYS_MS - 1).unwrap(), 0);
    assert_eq!(store.sweep(1000 + SEVEN_DAYS_MS).unwrap(), 1);
    assert!(store
        .lookup(&row.batch_id, 1000 + SEVEN_DAYS_MS)
        .unwrap()
        .is_none());
    assert!(matches!(
        store
            .hold(request("p1", "retained", 1000 + SEVEN_DAYS_MS))
            .unwrap(),
        HoldAdmission::Inserted(_)
    ));
}

#[test]
fn unacknowledged_delivery_expires_at_admission_not_settlement() {
    let store = GrantsStore::open_in_memory().unwrap();
    let row = insert(&store, "p1", "unacked", 1000);
    store.put_card(&card()).unwrap();
    store.authorize(&row.batch_id, 2000).unwrap();
    store
        .complete_authorized(&row.batch_id, "{}", 86_401_000)
        .unwrap();
    assert_eq!(
        store
            .late_results("reserved:p1", 0, 10, 1000 + SEVEN_DAYS_MS - 1)
            .unwrap()
            .len(),
        1
    );
    assert!(store
        .late_results("reserved:p1", 0, 10, 1000 + SEVEN_DAYS_MS)
        .unwrap()
        .is_empty());
    assert_eq!(store.sweep(1000 + SEVEN_DAYS_MS).unwrap(), 1);
}

#[test]
fn hold_replay_checks_identity_and_digest_without_changing_first_admission() {
    let store = GrantsStore::open_in_memory().unwrap();
    let row = insert(&store, "p1", "identity", 1000);
    let mut changed = request("p1", "identity", 3000);
    changed.scope.scope_epoch = 2;
    assert_eq!(store.hold(changed).unwrap(), HoldAdmission::ScopeMismatch);
    let mut changed = request("p1", "identity", 3000);
    changed.args_digest = "different".into();
    assert_eq!(store.hold(changed).unwrap(), HoldAdmission::ArgsMismatch);
    assert_eq!(
        store.hold(request("p1", "identity", 3000)).unwrap(),
        HoldAdmission::Replay(row)
    );
}

#[test]
fn answer_page_is_atomic_first_decision_wins_and_revoke_survives_replay() {
    let root = broca_test_support::scratch_root();
    let path = root.join("answers.db");
    let store = GrantsStore::open(&path).unwrap();
    store.put_card(&card()).unwrap();
    let allow = || CardAnswer {
        dedup_key: "one-card".into(),
        card_id: "card-1".into(),
        decision: Decision::Allow24h,
        run_live: true,
    };
    let bad = CardAnswer {
        dedup_key: "unknown".into(),
        card_id: "missing".into(),
        decision: Decision::Deny,
        run_live: true,
    };
    assert!(store
        .apply_answer_page(&scope(), "page-1", &[allow(), bad], 1000)
        .is_err());
    assert!(store.grants().unwrap().is_empty());
    assert_eq!(store.answer_cursor(&scope()).unwrap(), None);
    assert_eq!(
        store.card("one-card").unwrap().unwrap().state,
        CardState::Pending
    );
    store
        .apply_answer_page(&scope(), "page-1", &[allow()], 1000)
        .unwrap();
    let grant = store.grants().unwrap().remove(0);
    assert_eq!(grant.expires_at_ms, Some(86_401_000));
    assert_eq!(grant.project, None);
    assert!(store.revoke(&grant.id).unwrap());
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 1);
    drop(store);
    let store = GrantsStore::open(path).unwrap();
    store
        .apply_answer_page(
            &scope(),
            "page-2",
            &[
                allow(),
                CardAnswer {
                    dedup_key: "one-card".into(),
                    card_id: "duplicate".into(),
                    decision: Decision::Deny,
                    run_live: true,
                },
            ],
            2000,
        )
        .unwrap();
    assert!(
        store.grants().unwrap().is_empty(),
        "replayed card must not recreate revoked grant"
    );
    assert!(!store.denied("agent", &scope(), "typesafe").unwrap());
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 1);
    assert_eq!(
        store.answer_cursor(&scope()).unwrap().as_deref(),
        Some("page-2")
    );
}

#[test]
fn run_answers_are_epoch_scoped_and_late_run_allow_cannot_mint() {
    let store = GrantsStore::open_in_memory().unwrap();
    store.put_card(&card()).unwrap();
    store
        .apply_answer_page(
            &scope(),
            "page",
            &[CardAnswer {
                dedup_key: "one-card".into(),
                card_id: "card-1".into(),
                decision: Decision::AllowRun,
                run_live: false,
            }],
            1000,
        )
        .unwrap();
    assert!(store.grants().unwrap().is_empty());
    let mut deny = card();
    deny.dedup_key = "deny-card".into();
    deny.card_id = Some("card-2".into());
    store.put_card(&deny).unwrap();
    store
        .apply_answer_page(
            &scope(),
            "page-2",
            &[CardAnswer {
                dedup_key: "deny-card".into(),
                card_id: "card-2".into(),
                decision: Decision::Deny,
                run_live: true,
            }],
            1000,
        )
        .unwrap();
    assert!(store.denied("agent", &scope(), "typesafe").unwrap());
    let mut next = scope();
    next.scope_epoch = 2;
    assert!(!store.denied("agent", &next, "typesafe").unwrap());
    assert_eq!(store.answer_cursor(&next).unwrap(), None);
}

struct AbsentCatalog;
impl BatchCatalog for AbsentCatalog {
    fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
        None
    }
}

#[derive(Default)]
struct Provider(AtomicUsize);
#[async_trait::async_trait]
impl ClassifierProvider for Provider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, _: String) -> ProviderReply {
        self.0.fetch_add(1, Ordering::SeqCst);
        ProviderReply::Answered(br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#.to_vec())
    }
}

fn tools(
    held: Arc<HeldCalls>,
    provider: Arc<dyn ClassifierProvider>,
    batches: Arc<broca_store::SystemOneStore>,
    clock: Arc<AtomicI64>,
) -> Arc<LlmTools> {
    let system = SystemOne::new(
        batches,
        "held-tests",
        Arc::new(AbsentCatalog),
        vec![ModelPin::jev(provider)],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE)
    .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    Arc::new(
        LlmTools::new(
            Arc::new(system),
            Default::default(),
            Arc::new(NoPrivateContent),
        )
        .with_held(held),
    )
}

async fn answer(tools: &Arc<LlmTools>, name: &str, arguments: Value, who: &str) -> Value {
    serde_json::from_str(
        tools
            .answer(
                json!({"name":name,"arguments":arguments})
                    .to_string()
                    .as_bytes(),
                Some(&principal(who)),
                &stamp(),
            )
            .await
            .unwrap()
            .get(),
    )
    .unwrap()
}

#[tokio::test]
async fn two_principals_restart_authorize_and_deliver_under_raw_keys() {
    let root = broca_test_support::scratch_root();
    let path = root.join("two-principals.db");
    let store = GrantsStore::open(&path).unwrap();
    let first = insert(&store, "p1", "raw:key with spaces", 1000);
    let second = insert(&store, "p2", "raw:key with spaces", 1000);
    assert_ne!(first.batch_id, second.batch_id);
    store.put_card(&card()).unwrap();
    drop(store);
    let held = Arc::new(HeldCalls::new(
        Arc::new(GrantsStore::open(path).unwrap()),
        "restart".into(),
    ));
    for row in [&first, &second] {
        assert!(held.store.authorize(&row.batch_id, 2000).unwrap());
        held.store
            .complete_authorized(
                &row.batch_id,
                &json!({"content":[{"type":"text","text":row.principal}]}).to_string(),
                3000,
            )
            .unwrap();
    }
    let tools = tools(
        held.clone(),
        Arc::new(Provider::default()),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(AtomicI64::new(4000)),
    );
    let p2 = answer(&tools, "late_results", json!({"since":null}), "p2").await;
    let p1 = answer(&tools, "late_results", json!({"since":null}), "p1").await;
    assert!(
        held.acknowledge(&json!({"through":p1["cursor"]}), Some(&principal("p2")))
            .is_err(),
        "a cursor is bound to the principal that received it"
    );
    for (page, who) in [(&p1, "reserved:p1"), (&p2, "reserved:p2")] {
        serde_json::from_value::<cortexkit_role_tool_provider::late_results::LateResultsReply>(
            page.clone(),
        )
        .unwrap();
        assert_eq!(
            page["entries"].as_array().unwrap().len(),
            1,
            "late feed must isolate principals"
        );
        assert_eq!(page["entries"][0]["call_key"], "raw:key with spaces");
        assert_eq!(page["entries"][0]["custodian"], who);
        assert_eq!(page["entries"][0]["result"]["content"][0]["text"], who);
        assert_eq!(page["entries"][0]["owner"], "reserved:owner");
    }
    answer(
        &tools,
        "late_results.ack",
        json!({"through":p2["cursor"]}),
        "p2",
    )
    .await;
    assert_eq!(
        answer(&tools, "late_results", json!({"since":null}), "p2").await["entries"],
        json!([])
    );
    assert_eq!(
        answer(&tools, "late_results", json!({"since":null}), "p1").await["entries"],
        p1["entries"]
    );
    assert!(
        held.store.lookup(&second.batch_id, 4000).unwrap().is_some(),
        "ack must preserve replay"
    );
}

#[tokio::test]
async fn held_and_final_replay_precede_catalog_and_keep_original_bytes() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let row = insert(&store, "p1", "held-replay", 1000);
    store.put_card(&card()).unwrap();
    let held = Arc::new(HeldCalls::new(store.clone(), "start".into()));
    let provider = Arc::new(Provider::default());
    let tools = tools(
        held,
        provider.clone(),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(AtomicI64::new(2000)),
    );
    let body =
        json!({"name":"llm_classify","arguments":arguments(),"call_key":"held-replay"}).to_string();
    let first = tools
        .answer(body.as_bytes(), Some(&principal("p1")), &stamp())
        .await
        .unwrap();
    let held_reply: Value = serde_json::from_str(first.get()).unwrap();
    assert_eq!(held_reply["held"]["reason"], "consent");
    assert_eq!(held_reply["held"]["detail"]["call_key"], "held-replay");
    assert_eq!(
        store.card("one-card").unwrap().unwrap().card_id.as_deref(),
        Some("card-1")
    );
    store
        .finish_unstarted(&row.batch_id, &not_started("denied"), 2000)
        .unwrap();
    let denied = tools
        .answer(body.as_bytes(), Some(&principal("p1")), &stamp())
        .await
        .unwrap_err();
    let refusal: cortexkit_role_classifier::errors::Refusal =
        serde_json::from_slice(&denied).unwrap();
    assert_eq!(refusal.code, "not_started");
    assert_eq!(
        refusal.detail.class,
        cortexkit_role_classifier::errors::ErrorClass::Permanent
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&denied).unwrap()["detail"]["consent"],
        "denied"
    );
    assert_eq!(denied, not_started("denied").as_bytes());
    let mut other_scope = stamp();
    if let SendScope::Stamped(bind) = &mut other_scope {
        bind.scope.run_id = Some("another-run".into());
    }
    let error: Value = serde_json::from_slice(
        &tools
            .answer(body.as_bytes(), Some(&principal("p1")), &other_scope)
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(error["code"], "batch_scope_mismatch");
    let mut changed = arguments();
    changed["max_cost_usd"] = json!(2);
    let body =
        json!({"name":"llm_classify","arguments":changed,"call_key":"held-replay"}).to_string();
    let error: Value = serde_json::from_slice(
        &tools
            .answer(body.as_bytes(), Some(&principal("p1")), &stamp())
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(error["code"], "batch_id_reuse");
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn authorized_recovery_executes_frozen_request_after_deadline_and_preserves_retention_anchor()
{
    let root = broca_test_support::scratch_root();
    let path = root.join("recovery.db");
    let store = GrantsStore::open(&path).unwrap();
    let row = insert(&store, "p1", "authorized-replay", 1000);
    store.put_card(&card()).unwrap();
    assert!(store.authorize(&row.batch_id, 2000).unwrap());
    drop(store);
    let held = Arc::new(HeldCalls::new(
        Arc::new(GrantsStore::open(&path).unwrap()),
        "restart".into(),
    ));
    held.expire_due(86_401_000).unwrap();
    let provider = Arc::new(Provider::default());
    let batches = Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap());
    let clock = Arc::new(AtomicI64::new(86_401_000));
    let tools = tools(
        held.clone(),
        provider.clone(),
        batches.clone(),
        clock.clone(),
    );
    tools.recover_authorized().unwrap();
    let body =
        json!({"name":"llm_classify","arguments":arguments(),"call_key":"authorized-replay"})
            .to_string();
    let reply = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tools.answer(body.as_bytes(), Some(&principal("p1")), &stamp()),
    )
    .await
    .unwrap()
    .unwrap();
    let result: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(result["answers"]["q"]["noul"], 0.5);
    assert_eq!(
        provider.0.load(Ordering::SeqCst),
        1,
        "startup recovery and replay must join one execution"
    );
    let final_row = held
        .store
        .lookup(&row.batch_id, 86_401_000)
        .unwrap()
        .unwrap();
    assert_eq!(final_row.state, HeldState::Final);
    assert_eq!(final_row.final_result.unwrap().kind, FinalKind::Result);
    assert_eq!(
        batches
            .lookup(LLM_CLASSIFY_STORE_ROLE, &row.batch_id)
            .unwrap()
            .unwrap()
            .admitted_at_ms,
        1000
    );
    clock.store(1000 + SEVEN_DAYS_MS - 1, Ordering::SeqCst);
    assert_eq!(
        tools
            .answer(body.as_bytes(), Some(&principal("p1")), &stamp())
            .await
            .unwrap()
            .get(),
        reply.get()
    );
    assert!(held
        .store
        .lookup(&row.batch_id, 1000 + SEVEN_DAYS_MS)
        .unwrap()
        .is_none());
    batches.sweep(1000 + SEVEN_DAYS_MS).unwrap();
    assert!(batches
        .lookup(LLM_CLASSIFY_STORE_ROLE, &row.batch_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn late_cursor_validation_pagination_and_restart_event_identity() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    for key in ["a", "b", "c"] {
        let row = insert(&store, "p1", key, 1000);
        store
            .finish_unstarted(&row.batch_id, &not_started("denied"), 2000)
            .unwrap();
    }
    let old = HeldCalls::new(store.clone(), "old".into());
    let page: Value = serde_json::from_str(
        old.read(
            &json!({"since":null,"limit":1}),
            Some(&principal("p1")),
            3000,
        )
        .unwrap()
        .get(),
    )
    .unwrap();
    assert_eq!(page["entries"].as_array().unwrap().len(), 1);
    assert_eq!(page["more"], true);
    let new = HeldCalls::new(store, "new".into());
    let error: Value = serde_json::from_slice(
        &new.read(
            &json!({"since":page["cursor"]}),
            Some(&principal("p1")),
            3000,
        )
        .unwrap_err(),
    )
    .unwrap();
    assert_eq!(error["code"], "cursor_incarnation_changed");
    let again: Value = serde_json::from_str(
        new.read(
            &json!({"since":null,"limit":1}),
            Some(&principal("p1")),
            3000,
        )
        .unwrap()
        .get(),
    )
    .unwrap();
    serde_json::from_value::<cortexkit_role_tool_provider::late_results::LateResultsReply>(
        again.clone(),
    )
    .unwrap();
    assert_eq!(
        again["entries"], page["entries"],
        "event id and raw key must survive restart"
    );
    let second: Value = serde_json::from_str(
        new.read(
            &json!({"since":again["cursor"],"limit":1}),
            Some(&principal("p1")),
            3000,
        )
        .unwrap()
        .get(),
    )
    .unwrap();
    assert_eq!(second["entries"][0]["call_key"], "b");
    let mut future = again["cursor"].clone();
    future["seq"] = json!(999);
    for arguments in [
        json!({"since":future}),
        json!({"since":{"provider_incarnation":"old","seq":0}}),
    ] {
        assert!(new.read(&arguments, Some(&principal("p1")), 3000).is_err());
    }
    assert!(new.read(&json!({"since":null}), None, 3000).is_err());
    assert!(new
        .acknowledge(&json!({"through":again["cursor"]}), None)
        .is_err());
    assert!(new
        .acknowledge(
            &json!({"through":{"provider_incarnation":"new","seq":999}}),
            Some(&principal("p1"))
        )
        .is_err());
}

struct SlowProvider {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[async_trait::async_trait]
impl ClassifierProvider for SlowProvider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, body: String) -> ProviderReply {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Provider::default().call(body).await
    }
}

#[tokio::test]
async fn approval_before_deadline_allows_slow_execution_past_deadline() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let row = insert(&store, "p1", "slow", 1000);
    store.put_card(&card()).unwrap();
    assert!(store.authorize(&row.batch_id, 600_999).unwrap());
    let held = Arc::new(HeldCalls::new(store, "start".into()));
    let clock = Arc::new(AtomicI64::new(600_999));
    let provider = Arc::new(SlowProvider {
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let tools = tools(
        held.clone(),
        provider.clone(),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        clock.clone(),
    );
    let task = tokio::spawn(async move {
        let body =
            json!({"name":"llm_classify","arguments":arguments(),"call_key":"slow"}).to_string();
        tools
            .answer(body.as_bytes(), Some(&principal("p1")), &stamp())
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        provider.entered.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    clock.store(900_000, Ordering::SeqCst);
    held.expire_due(900_000).unwrap();
    assert_eq!(
        held.store
            .lookup(&row.batch_id, 900_000)
            .unwrap()
            .unwrap()
            .state,
        HeldState::Authorized
    );
    provider.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        held.store
            .lookup(&row.batch_id, 900_000)
            .unwrap()
            .unwrap()
            .final_result
            .unwrap()
            .kind,
        FinalKind::Result
    );
}

#[test]
fn independent_connections_race_authorization_and_deadline_with_one_winner() {
    let root = broca_test_support::scratch_root();
    let path = root.join("race.db");
    let store = GrantsStore::open(&path).unwrap();
    let row = insert(&store, "p1", "race", 1000);
    store.put_card(&card()).unwrap();
    let other = GrantsStore::open(&path).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let ready = barrier.clone();
    let batch = row.batch_id.clone();
    let authorize = std::thread::spawn(move || {
        ready.wait();
        other.authorize(&batch, 600_999).unwrap()
    });
    barrier.wait();
    let expired = store
        .expire_held(&row.batch_id, &not_started("pending"), 601_000)
        .unwrap();
    let authorized = authorize.join().unwrap();
    assert_ne!(expired, authorized, "exactly one compare-and-set may win");
    assert_eq!(
        store.lookup(&row.batch_id, 601_000).unwrap().unwrap().state,
        if authorized {
            HeldState::Authorized
        } else {
            HeldState::Final
        }
    );
}

#[tokio::test]
async fn authorized_permanent_failure_is_final_replayed_and_delivered_as_result() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let row = insert(&store, "p1", "failed", 1000);
    store.put_card(&card()).unwrap();
    store.authorize(&row.batch_id, 2000).unwrap();
    let held = Arc::new(HeldCalls::new(store, "start".into()));
    let tools = tools(
        held.clone(),
        Arc::new(UnmeasuredProvider),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(AtomicI64::new(4000)),
    );
    let body =
        json!({"name":"llm_classify","arguments":arguments(),"call_key":"failed"}).to_string();
    let failure = tools
        .answer(body.as_bytes(), Some(&principal("p1")), &stamp())
        .await
        .unwrap_err();
    assert_eq!(
        serde_json::from_slice::<Value>(&failure).unwrap()["code"],
        "cost_unmeasured"
    );
    assert_eq!(
        held.store
            .lookup(&row.batch_id, 4000)
            .unwrap()
            .unwrap()
            .state,
        HeldState::Final
    );
    assert_eq!(
        tools
            .answer(body.as_bytes(), Some(&principal("p1")), &stamp())
            .await
            .unwrap_err(),
        failure
    );
    let page = answer(&tools, "late_results", json!({"since":null}), "p1").await;
    assert_eq!(page["entries"][0]["kind"], "result");
    assert_eq!(page["entries"][0]["result"]["isError"], true);
    serde_json::from_value::<cortexkit_role_tool_provider::late_results::LateResultsReply>(page)
        .unwrap();
}

struct UnmeasuredProvider;
#[async_trait::async_trait]
impl ClassifierProvider for UnmeasuredProvider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, _: String) -> ProviderReply {
        ProviderReply::Answered(
            br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}}}"#.to_vec(),
        )
    }
}

#[test]
fn expired_key_reuse_gets_a_new_late_event_identity() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let first = insert(&store, "p1", "reuse-after-seven-days", 1000);
    store
        .finish_unstarted(&first.batch_id, &not_started("denied"), 2000)
        .unwrap();
    let held = HeldCalls::new(store.clone(), "same-process".into());
    let before: Value = serde_json::from_str(
        held.read(&json!({"since":null}), Some(&principal("p1")), 3000)
            .unwrap()
            .get(),
    )
    .unwrap();
    store.sweep(1000 + SEVEN_DAYS_MS).unwrap();
    let next = insert(&store, "p1", "reuse-after-seven-days", 1000 + SEVEN_DAYS_MS);
    assert_eq!(first.batch_id, next.batch_id);
    store
        .finish_unstarted(
            &next.batch_id,
            &not_started("pending"),
            2000 + SEVEN_DAYS_MS,
        )
        .unwrap();
    let after: Value = serde_json::from_str(
        held.read(
            &json!({"since":null}),
            Some(&principal("p1")),
            3000 + SEVEN_DAYS_MS,
        )
        .unwrap()
        .get(),
    )
    .unwrap();
    assert_eq!(
        before["entries"][0]["call_key"],
        after["entries"][0]["call_key"]
    );
    assert_ne!(
        before["entries"][0]["event_id"], after["entries"][0]["event_id"],
        "new admission must not deduplicate as the previous seven-day-old result"
    );
}

#[derive(Default)]
struct CapturedProvider(std::sync::Mutex<String>);

#[async_trait::async_trait]
impl ClassifierProvider for CapturedProvider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, body: String) -> ProviderReply {
        *self.0.lock().unwrap() = body;
        Provider::default().call(String::new()).await
    }
}

#[tokio::test]
async fn authorized_replay_uses_first_admitted_request_bytes() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let mut args = arguments();
    args["item"]["state"] = json!({"a":1,"b":2});
    let mut first = request("p1", "frozen-bytes", 1000);
    first.args_digest = hash(args.clone());
    first.request = first.request.replace("\"hello\"", "{\"b\":2,\"a\":1}");
    let row = match store.hold(first).unwrap() {
        HoldAdmission::Inserted(row) => row,
        other => panic!("unexpected admission: {other:?}"),
    };
    store.put_card(&card()).unwrap();
    store.authorize(&row.batch_id, 2000).unwrap();
    let provider = Arc::new(CapturedProvider::default());
    let tools = tools(
        Arc::new(HeldCalls::new(store, "start".into())),
        provider.clone(),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(AtomicI64::new(3000)),
    );
    let body =
        json!({"name":"llm_classify","arguments":args,"call_key":"frozen-bytes"}).to_string();
    tools
        .answer(body.as_bytes(), Some(&principal("p1")), &stamp())
        .await
        .unwrap();
    let payload = provider.0.lock().unwrap();
    assert!(payload.find("\"b\"").unwrap() < payload.find("\"a\"").unwrap(), "authorized replay must use the first admitted request, not the retry's canonically equal JSON");
}
