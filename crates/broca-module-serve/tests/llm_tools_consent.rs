use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use broca_module_serve::{
    llm_tools::{
        consent::{Consent, ConsentIo},
        held::HeldCalls,
        Asks, LlmTools,
    },
    system_one::{
        BatchCatalog, ClassifierProvider, Config, ModelPin, ProviderReply, SystemOne,
        LLM_CLASSIFY_STORE_ROLE,
    },
};
use broca_protocol::{BindPrincipal, BindScope, SendScope, SessionScope};
use broca_store::grants::*;
use broca_subc::{AnswersPage, ConsentError, ConsentRecord, ConsentState};
use serde_json::{json, Value};

fn scope() -> SessionScope {
    SessionScope {
        owner: BindPrincipal::Reserved {
            module_id: "owner".into(),
        },
        scope_ref: "run".into(),
        scope_epoch: 1,
        agent_id: Some("agent-id-must-not-be-displayed".into()),
        run_id: Some("run".into()),
        flow_id: None,
        sender: None,
    }
}

fn identity() -> ScopeIdentity {
    ScopeIdentity {
        owner: "reserved:owner".into(),
        scope_ref: "run".into(),
        scope_epoch: 1,
    }
}

fn args(model: &str) -> Value {
    json!({"model":model,"questions":{"q":{"type":"noul","instructions":"Is this likely?"}},"item":{"state":"private text"},"max_cost_usd":1})
}

fn hold(key: &str, dedup: &str, model: &str, at: i64) -> HoldRequest {
    HoldRequest {
        batch_id: key.into(),
        scope: identity(),
        batch_scope: broca_store::system_one::BatchScope::Run {
            agent_id: scope().agent_id.unwrap(),
            run_id: "run".into(),
        },
        principal: "reserved:caller".into(),
        call_key: key.into(),
        agent_id: scope().agent_id.unwrap(),
        provider: "typesafe".into(),
        model: model.into(),
        request: args(model).to_string(),
        args_digest: "digest".into(),
        frozen_row: "{}".into(),
        dedup_key: dedup.into(),
        admitted_at_ms: at,
    }
}

fn body(key: &str, model: &str) -> Value {
    broca_subc::consent_card::private_content_card(
        "TypeSafe",
        model,
        &[broca_subc::SourceKind::PersonBrowser],
        key,
    )
    .unwrap()
}

fn record(id: &str, key: &str, choice: &str) -> ConsentRecord {
    ConsentRecord {
        elicitation_id: id.into(),
        state: ConsentState::Answered,
        fields: BTreeMap::from([
            ("requester_ref".into(), json!(key)),
            ("answered_choice_id".into(), json!(choice)),
        ]),
    }
}

#[derive(Default)]
struct FakeIo {
    requests: Mutex<Vec<Value>>,
    replies: Mutex<VecDeque<Result<String, ConsentError>>>,
    pages: Mutex<VecDeque<AnswersPage>>,
    answers: Mutex<VecDeque<ConsentRecord>>,
    withdrawn: Mutex<Vec<String>>,
    feeds: AtomicUsize,
    fail_feed_at: AtomicUsize,
    awaits: AtomicUsize,
    live: AtomicBool,
    gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    sent: tokio::sync::Notify,
    answered: tokio::sync::Notify,
    wait_gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    waiting: tokio::sync::Notify,
    clock_after_wait: Mutex<Option<(Arc<AtomicI64>, i64)>>,
}

#[async_trait::async_trait]
impl ConsentIo for FakeIo {
    async fn request(&self, _: &SessionScope, body: &Value) -> Result<String, ConsentError> {
        let index = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(body.clone());
            requests.len()
        };
        self.sent.notify_one();
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Ok(format!("card-{index}")))
    }
    async fn answers(&self, _: &SessionScope, _: u64) -> Result<AnswersPage, ConsentError> {
        let call = self.feeds.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_feed_at.load(Ordering::SeqCst) == call {
            return Err(ConsentError::Transport("answer feed disconnected".into()));
        }
        Ok(self
            .pages
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(AnswersPage {
                records: vec![],
                cursor: 1,
                continuation: None,
            }))
    }
    async fn wait(&self, _: &SessionScope, _: &str) -> Result<ConsentRecord, ConsentError> {
        self.awaits.fetch_add(1, Ordering::SeqCst);
        if let Some((clock, at)) = self.clock_after_wait.lock().unwrap().take() {
            clock.store(at, Ordering::SeqCst);
        }
        loop {
            let notified = self.answered.notified();
            let record = self.answers.lock().unwrap().pop_front();
            if let Some(record) = record {
                let gate = self.wait_gate.lock().unwrap().take();
                if let Some(gate) = gate {
                    self.waiting.notify_one();
                    gate.acquire().await.unwrap().forget();
                }
                return Ok(record);
            }
            notified.await;
        }
    }
    async fn withdraw(&self, _: &SessionScope, id: &str) {
        self.withdrawn.lock().unwrap().push(id.into());
    }
    async fn run_live(
        &self,
        _: &SessionScope,
    ) -> broca_module_serve::llm_tools::consent::RunLiveness {
        if self.live.load(Ordering::SeqCst) {
            broca_module_serve::llm_tools::consent::RunLiveness::Live
        } else {
            broca_module_serve::llm_tools::consent::RunLiveness::Ended
        }
    }
}

fn coordinator(store: Arc<GrantsStore>, io: Arc<FakeIo>, clock: Arc<AtomicI64>) -> Arc<Consent> {
    Arc::new(Consent::new(store, io).with_clock(Arc::new(move || clock.load(Ordering::SeqCst))))
}

#[test]
fn dedup_key_uses_full_scope_provider_and_revocation_generation() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let consent = coordinator(
        store.clone(),
        Arc::new(FakeIo::default()),
        Arc::new(AtomicI64::new(1000)),
    );
    let agent = scope().agent_id.unwrap();
    let key = consent.dedup_key(&agent, "typesafe", &identity()).unwrap();
    assert_eq!(
        key,
        "0ab378f55349aebe99550e29113304f58add2abe9c4fb03825326dfa2992b733"
    );
    let mut other = identity();
    other.scope_epoch += 1;
    assert_ne!(key, consent.dedup_key(&agent, "typesafe", &other).unwrap());
    other = identity();
    other.owner = "reserved:another-owner".into();
    assert_ne!(key, consent.dedup_key(&agent, "typesafe", &other).unwrap());
    assert_ne!(
        key,
        consent
            .dedup_key(&agent, "cloudflare-workers-ai", &identity())
            .unwrap()
    );
    store
        .put_card(&Card {
            dedup_key: key.clone(),
            scope: identity(),
            agent_id: agent.clone(),
            provider: "typesafe".into(),
            content: "card".into(),
            card_id: Some("granted-card".into()),
            state: CardState::Pending,
        })
        .unwrap();
    store
        .apply_answer(
            &identity(),
            CardAnswer {
                dedup_key: key.clone(),
                card_id: "granted-card".into(),
                decision: Decision::Allow24h,
                run_live: true,
            },
            1000,
        )
        .unwrap();
    assert!(store.revoke("granted-card").unwrap());
    assert_ne!(
        key,
        consent.dedup_key(&agent, "typesafe", &identity()).unwrap()
    );
}

#[tokio::test]
async fn concurrent_same_content_files_one_card_and_replacement_serializes_delayed_reply() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let key = "a".repeat(64);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *io.gate.lock().unwrap() = Some(gate.clone());
    let first = tokio::spawn({
        let consent = consent.clone();
        let key = key.clone();
        async move {
            consent
                .file(hold("first", &key, "Jev", 1000), body(&key, "Jev"), 1000)
                .await
                .unwrap()
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), io.sent.notified())
        .await
        .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let same = tokio::spawn({
        let consent = consent.clone();
        let key = key.clone();
        async move {
            started_tx.send(()).unwrap();
            consent
                .file(hold("same", &key, "Jev", 1000), body(&key, "Jev"), 1000)
                .await
                .unwrap()
        }
    });
    started_rx.await.unwrap();
    assert_eq!(
        io.requests.lock().unwrap().len(),
        1,
        "a second caller must wait for the first filing reply"
    );
    let (replacement_tx, replacement_rx) = tokio::sync::oneshot::channel();
    let replacement = tokio::spawn({
        let consent = consent.clone();
        let key = key.clone();
        async move {
            replacement_tx.send(()).unwrap();
            consent
                .file(
                    hold("replacement", &key, "Clef Flash", 1001),
                    body(&key, "Clef Flash"),
                    1001,
                )
                .await
                .unwrap()
        }
    });
    replacement_rx.await.unwrap();
    assert_eq!(
        io.requests.lock().unwrap().len(),
        1,
        "a different model must also wait for the delayed filing reply"
    );
    gate.add_permits(1);
    let (first, same, replacement) = tokio::join!(first, same, replacement);
    assert_eq!(first.unwrap().card_id, same.unwrap().card_id);
    assert_eq!(replacement.unwrap().card_id.as_deref(), Some("card-2"));
    assert_eq!(io.requests.lock().unwrap().len(), 2);
    for id in ["first", "same", "replacement"] {
        assert_eq!(
            store.lookup(id, 1001).unwrap().unwrap().card_id.as_deref(),
            Some("card-2")
        );
    }
    io.live.store(true, Ordering::SeqCst);
    io.answers
        .lock()
        .unwrap()
        .push_back(record("card-2", &key, "allow_run"));
    assert!(!consent.wait_once(&key, 1001).await.unwrap());
    for id in ["first", "same", "replacement"] {
        assert_eq!(
            store.lookup(id, 1001).unwrap().unwrap().state,
            HeldState::Authorized
        );
    }
    assert_eq!(
        io.feeds.load(Ordering::SeqCst),
        0,
        "normal intake uses await, not the feed"
    );
}

#[tokio::test]
async fn filing_refusals_finalize_without_grants_but_transport_and_unreadable_stay_filing() {
    for (index, reply) in [
        Err(ConsentError::Refused {
            code: "elicitation_requester_refused".into(),
            message: "requester refused".into(),
            detail: None,
        }),
        Err(ConsentError::Refused {
            code: "elicitation_invalid_request".into(),
            message: "invalid card".into(),
            detail: None,
        }),
        Err(ConsentError::Refused {
            code: "unknown_method".into(),
            message: "absent".into(),
            detail: None,
        }),
        Err(ConsentError::Refused {
            code: "transient".into(),
            message: "still a refusal".into(),
            detail: None,
        }),
        Err(ConsentError::Transport("timeout".into())),
        Err(ConsentError::AcceptedFilingReplyUnreadable(
            "bad bytes".into(),
        )),
    ]
    .into_iter()
    .enumerate()
    {
        let store = Arc::new(GrantsStore::open_in_memory().unwrap());
        let io = Arc::new(FakeIo::default());
        io.replies.lock().unwrap().push_back(reply);
        let consent = coordinator(store.clone(), io, Arc::new(AtomicI64::new(1000)));
        let key = "b".repeat(64);
        let row = consent
            .file(hold("call", &key, "Jev", 1000), body(&key, "Jev"), 1000)
            .await
            .unwrap();
        if index < 4 {
            assert_eq!(row.state, HeldState::Final);
            assert_eq!(
                row.final_result.unwrap().body,
                broca_module_serve::llm_tools::held::not_started("consent_unavailable")
            );
            assert!(store.card(&key).unwrap().is_none());
        } else {
            assert_eq!(row.state, HeldState::Filing);
        }
        assert!(store.grants().unwrap().is_empty());
        assert!(!store
            .denied(&scope().agent_id.unwrap(), &identity(), "typesafe")
            .unwrap());
    }
}

#[tokio::test]
async fn recovery_adopts_older_answer_after_refile_and_opposite_duplicate_cannot_win() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    io.replies
        .lock()
        .unwrap()
        .push_back(Err(ConsentError::Transport(
            "accepted but reply lost".into(),
        )));
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let key = "c".repeat(64);
    consent
        .file(hold("call", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    io.pages.lock().unwrap().extend([
        AnswersPage {
            records: vec![],
            cursor: 1,
            continuation: None,
        },
        AnswersPage {
            records: vec![
                serde_json::to_value(record("older", &key, "deny")).unwrap(),
                serde_json::to_value(record("duplicate", &key, "allow_24h")).unwrap(),
            ],
            cursor: 3,
            continuation: None,
        },
    ]);
    consent.recover(&key, 1000).await.unwrap();
    assert_eq!(
        store
            .lookup("call", 1000)
            .unwrap()
            .unwrap()
            .final_result
            .unwrap()
            .body,
        broca_module_serve::llm_tools::held::not_started("denied")
    );
    assert!(store.grants().unwrap().is_empty());
    assert!(store
        .denied(&scope().agent_id.unwrap(), &identity(), "typesafe")
        .unwrap());
    assert_eq!(
        store.card(&key).unwrap().unwrap().card_id.as_deref(),
        Some("older")
    );
    let withdrawn = io.withdrawn.lock().unwrap();
    assert!(withdrawn.contains(&"card-2".into()));
    assert!(withdrawn.contains(&"duplicate".into()));
    assert_eq!(
        store.answer_cursor(&identity()).unwrap().as_deref(),
        Some("3")
    );
}

#[tokio::test]
async fn recovery_reads_all_pages_before_refiling_and_adopts_a_settled_card() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    io.replies
        .lock()
        .unwrap()
        .push_back(Err(ConsentError::Transport("lost reply".into())));
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let key = "f".repeat(64);
    consent
        .file(hold("call", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    io.pages.lock().unwrap().extend([
        AnswersPage {
            records: vec![json!({"elicitation_id":"unrelated","state":"expired"})],
            cursor: 5,
            continuation: Some(5),
        },
        AnswersPage {
            records: vec![
                serde_json::to_value(record("settled-while-down", &key, "allow_24h")).unwrap(),
            ],
            cursor: 6,
            continuation: None,
        },
    ]);
    consent.recover(&key, 1000).await.unwrap();
    assert_eq!(
        io.requests.lock().unwrap().len(),
        1,
        "a settled answer must be looked up before refiling"
    );
    assert_eq!(
        store.lookup("call", 1000).unwrap().unwrap().state,
        HeldState::Authorized
    );
    assert_eq!(
        store.grants().unwrap()[0].source_card_id,
        "settled-while-down"
    );
    assert_eq!(
        store.answer_cursor(&identity()).unwrap().as_deref(),
        Some("6")
    );
}

#[tokio::test]
async fn recovery_page_locks_every_card_without_cross_key_deadlock() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let a = "4".repeat(64);
    let b = "5".repeat(64);
    io.replies
        .lock()
        .unwrap()
        .push_back(Err(ConsentError::Transport("lost first reply".into())));
    consent
        .file(hold("a", &a, "Jev", 1000), body(&a, "Jev"), 1000)
        .await
        .unwrap();
    io.sent.notified().await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *io.gate.lock().unwrap() = Some(gate.clone());
    let filing_b = tokio::spawn({
        let consent = consent.clone();
        let b = b.clone();
        async move {
            let mut request = hold("b", &b, "cloudflare-workers-ai/@cf/cloudflare/clef", 1000);
            request.provider = "cloudflare-workers-ai".into();
            let card = broca_subc::consent_card::private_content_card(
                "Cloudflare Workers AI",
                "Clef",
                &[broca_subc::SourceKind::PersonBrowser],
                &b,
            )
            .unwrap();
            consent.file(request, card, 1000).await.unwrap()
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), io.sent.notified())
        .await
        .unwrap();
    io.pages.lock().unwrap().extend([
        AnswersPage {
            records: vec![serde_json::to_value(record("settled-b", &b, "allow_24h")).unwrap()],
            cursor: 1,
            continuation: Some(1),
        },
        AnswersPage {
            records: vec![serde_json::to_value(record("settled-b", &b, "allow_24h")).unwrap()],
            cursor: 2,
            continuation: None,
        },
    ]);
    let recovering_a = tokio::spawn({
        let consent = consent.clone();
        let a = a.clone();
        async move { consent.recover(&a, 1000).await.unwrap() }
    });
    broca_test_support::wait_until("recovery feed page", || async {
        if io.feeds.load(Ordering::SeqCst) > 0 {
            Ok(())
        } else {
            Err("feed not read".to_owned())
        }
    })
    .await;
    assert!(
        store.grants().unwrap().is_empty(),
        "a feed page must not settle another key during its in-flight filing"
    );
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let recovering_b = tokio::spawn({
        let consent = consent.clone();
        let b = b.clone();
        async move {
            started_tx.send(()).unwrap();
            consent.recover(&b, 1000).await.unwrap()
        }
    });
    started_rx.await.unwrap();
    gate.add_permits(1);
    let joined = broca_test_support::event_timeout(
        "per-scope recovery across card keys",
        async { tokio::join!(filing_b, recovering_a, recovering_b) },
        || "filing and both recovery tasks must all finish".to_owned(),
    )
    .await
    .expect("per-scope recovery deadlocked across card keys");
    joined.0.unwrap();
    joined.1.unwrap();
    joined.2.unwrap();
    assert_eq!(store.grants().unwrap().len(), 1);
    assert_eq!(
        store.lookup("b", 1000).unwrap().unwrap().state,
        HeldState::Authorized
    );
}

#[tokio::test]
async fn recovery_feed_never_revives_a_card_past_its_last_reading_deadline() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let clock = Arc::new(AtomicI64::new(1000));
    let consent = coordinator(store.clone(), io.clone(), clock.clone());
    let old = "6".repeat(64);
    let active = "7".repeat(64);
    consent
        .file(hold("old", &old, "Jev", 1000), body(&old, "Jev"), 1000)
        .await
        .unwrap();
    let now = 601_000 + 86_400_000;
    clock.store(now, Ordering::SeqCst);
    io.replies
        .lock()
        .unwrap()
        .push_back(Err(ConsentError::Transport("lost active card".into())));
    consent
        .file(
            hold("active", &active, "Jev", now),
            body(&active, "Jev"),
            now,
        )
        .await
        .unwrap();
    io.pages.lock().unwrap().push_back(AnswersPage {
        records: vec![serde_json::to_value(record("card-1", &old, "allow_24h")).unwrap()],
        cursor: 1,
        continuation: None,
    });
    consent.recover(&active, now).await.unwrap();
    assert!(store.grants().unwrap().is_empty());
    assert_eq!(store.card(&old).unwrap().unwrap().state, CardState::Pending);
}

#[tokio::test]
async fn failed_post_refile_lookup_remains_filing_across_restart() {
    let path = broca_test_support::scratch_root().join("post-refile.db");
    let store = Arc::new(GrantsStore::open(&path).unwrap());
    let io = Arc::new(FakeIo::default());
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let key = "8".repeat(64);
    io.replies
        .lock()
        .unwrap()
        .push_back(Err(ConsentError::Transport("lost first reply".into())));
    consent
        .file(hold("call", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    io.fail_feed_at.store(2, Ordering::SeqCst);
    consent.recover(&key, 1000).await.unwrap();
    let card = store.card(&key).unwrap().unwrap();
    assert_eq!(card.state, CardState::Filing);
    assert_eq!(card.card_id.as_deref(), Some("card-2"));
    drop(consent);
    drop(store);
    let reopened = Arc::new(GrantsStore::open(&path).unwrap());
    let consent = coordinator(reopened.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    io.pages.lock().unwrap().push_back(AnswersPage {
        records: vec![serde_json::to_value(record("older", &key, "deny")).unwrap()],
        cursor: 3,
        continuation: None,
    });
    consent.recover(&key, 1000).await.unwrap();
    assert_eq!(
        reopened.card(&key).unwrap().unwrap().state,
        CardState::Denied
    );
    assert_eq!(
        io.requests.lock().unwrap().len(),
        2,
        "recovery must adopt before another refile"
    );
    assert!(io.withdrawn.lock().unwrap().contains(&"card-2".into()));
    assert!(reopened.grants().unwrap().is_empty());
}

#[tokio::test]
async fn stale_await_after_replacement_cannot_authorize_the_new_card() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    io.live.store(true, Ordering::SeqCst);
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let key = "1".repeat(64);
    consent
        .file(hold("first", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *io.wait_gate.lock().unwrap() = Some(gate.clone());
    io.answers
        .lock()
        .unwrap()
        .push_back(record("card-1", &key, "allow_run"));
    let stale = tokio::spawn({
        let consent = consent.clone();
        let key = key.clone();
        async move { consent.wait_once(&key, 1000).await.unwrap() }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), io.waiting.notified())
        .await
        .unwrap();
    consent
        .file(
            hold("second", &key, "Clef Flash", 1000),
            body(&key, "Clef Flash"),
            1000,
        )
        .await
        .unwrap();
    gate.add_permits(1);
    assert!(stale.await.unwrap());
    assert!(store.grants().unwrap().is_empty());
    assert_eq!(
        store.card(&key).unwrap().unwrap().card_id.as_deref(),
        Some("card-2")
    );
    assert!(io.withdrawn.lock().unwrap().contains(&"card-1".into()));
    io.answers
        .lock()
        .unwrap()
        .push_back(record("card-2", &key, "allow_run"));
    assert!(!consent.wait_once(&key, 1000).await.unwrap());
    for id in ["first", "second"] {
        assert_eq!(
            store.lookup(id, 1000).unwrap().unwrap().state,
            HeldState::Authorized
        );
    }
}

#[tokio::test]
async fn uncorrelated_answer_never_mints_a_grant() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let key = "2".repeat(64);
    consent
        .file(hold("call", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    io.answers
        .lock()
        .unwrap()
        .push_back(record("card-1", "another-reference", "allow_24h"));
    assert!(consent.wait_once(&key, 1000).await.unwrap());
    assert!(store.grants().unwrap().is_empty());
    assert_eq!(
        store.lookup("call", 1000).unwrap().unwrap().state,
        HeldState::Held
    );
}

#[tokio::test]
async fn late_answers_keep_24h_grants_but_ended_run_allow_records_nothing() {
    for (choice, live, grants) in [
        ("allow_run", false, 0),
        ("allow_24h", false, 1),
        ("deny", false, 0),
    ] {
        let store = Arc::new(GrantsStore::open_in_memory().unwrap());
        let io = Arc::new(FakeIo::default());
        io.live.store(live, Ordering::SeqCst);
        let clock = Arc::new(AtomicI64::new(1000));
        let consent = coordinator(store.clone(), io.clone(), clock.clone());
        let key = "d".repeat(64);
        consent
            .file(hold("late", &key, "Jev", 1000), body(&key, "Jev"), 1000)
            .await
            .unwrap();
        HeldCalls::new(store.clone(), "test".into())
            .expire_due(601_000)
            .unwrap();
        clock.store(602_000, Ordering::SeqCst);
        io.answers
            .lock()
            .unwrap()
            .push_back(record("card-1", &key, choice));
        assert!(!consent.wait_once(&key, 602_000).await.unwrap());
        assert_eq!(store.grants().unwrap().len(), grants);
        assert_eq!(
            store
                .lookup("late", 602_000)
                .unwrap()
                .unwrap()
                .final_result
                .unwrap()
                .body,
            broca_module_serve::llm_tools::held::not_started("pending")
        );
        assert_eq!(
            store
                .denied(&scope().agent_id.unwrap(), &identity(), "typesafe")
                .unwrap(),
            choice == "deny"
        );
        assert_eq!(io.feeds.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn await_reply_after_deadline_cannot_authorize_with_wait_start_time() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    io.live.store(true, Ordering::SeqCst);
    let clock = Arc::new(AtomicI64::new(1000));
    let consent = coordinator(store.clone(), io.clone(), clock.clone());
    let key = "e".repeat(64);
    consent
        .file(
            hold("slow-wait", &key, "Jev", 1000),
            body(&key, "Jev"),
            1000,
        )
        .await
        .unwrap();
    io.answers
        .lock()
        .unwrap()
        .push_back(record("card-1", &key, "allow_run"));
    *io.clock_after_wait.lock().unwrap() = Some((clock, 601_000));
    consent.wait_once(&key, 600_999).await.unwrap();
    assert_eq!(
        store.lookup("slow-wait", 601_000).unwrap().unwrap().state,
        HeldState::Held
    );
    assert_eq!(
        store.grants().unwrap().len(),
        1,
        "late run allow is still recorded for a live run"
    );
}

#[tokio::test]
async fn await_crossing_late_answer_cutoff_records_no_grant() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let clock = Arc::new(AtomicI64::new(1000));
    let consent = coordinator(store.clone(), io.clone(), clock.clone());
    let key = "3".repeat(64);
    consent
        .file(hold("call", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    io.answers
        .lock()
        .unwrap()
        .push_back(record("card-1", &key, "allow_24h"));
    *io.clock_after_wait.lock().unwrap() = Some((clock, 601_000 + 86_400_000));
    assert!(!consent
        .wait_once(&key, 601_000 + 86_400_000 - 1)
        .await
        .unwrap());
    assert!(store.grants().unwrap().is_empty());
    assert_eq!(store.card(&key).unwrap().unwrap().state, CardState::Pending);
}

#[tokio::test]
async fn a_late_deny_records_the_run_deny_but_the_overdue_call_is_pending() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let clock = Arc::new(AtomicI64::new(1000));
    let consent = coordinator(store.clone(), io.clone(), clock.clone());
    let key = "9".repeat(64);
    consent
        .file(hold("call", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    clock.store(601_000, Ordering::SeqCst);
    io.answers
        .lock()
        .unwrap()
        .push_back(record("card-1", &key, "deny"));
    assert!(!consent.wait_once(&key, 601_000).await.unwrap());
    assert_eq!(
        store
            .lookup("call", 601_000)
            .unwrap()
            .unwrap()
            .final_result
            .unwrap()
            .body,
        broca_module_serve::llm_tools::held::not_started("pending")
    );
    assert!(store
        .denied(&scope().agent_id.unwrap(), &identity(), "typesafe")
        .unwrap());
}

#[tokio::test]
async fn a_new_24h_grant_releases_a_different_runs_waiting_call_without_refiling() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let key = "a".repeat(64);
    let other = "b".repeat(64);
    consent
        .file(hold("waiting", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    let mut other_scope = identity();
    other_scope.scope_ref = "another-run".into();
    store
        .put_card(&Card {
            dedup_key: other.clone(),
            scope: other_scope.clone(),
            agent_id: scope().agent_id.unwrap(),
            provider: "typesafe".into(),
            content: "other run".into(),
            card_id: Some("24h-source".into()),
            state: CardState::Pending,
        })
        .unwrap();
    store
        .apply_answer(
            &other_scope,
            CardAnswer {
                dedup_key: other,
                card_id: "24h-source".into(),
                decision: Decision::Allow24h,
                run_live: false,
            },
            1000,
        )
        .unwrap();
    assert_eq!(consent.resume_granted(1000).await.unwrap(), 1);
    assert_eq!(
        store.lookup("waiting", 1000).unwrap().unwrap().state,
        HeldState::Authorized
    );
    assert_eq!(io.requests.lock().unwrap().len(), 1);
    assert_eq!(io.awaits.load(Ordering::SeqCst), 0);
}

struct Catalog {
    provider_name: Option<String>,
}
impl BatchCatalog for Catalog {
    fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
        Some(broca_catalog::ModelBatchRow {
            model_name: Some("Jev 1.13".into()),
            provider_name: self.provider_name.clone(),
            kind: Some("decision".into()),
            context_tokens: Some(100_000),
            attachment: false,
            input_rate_nanos: Some(42_000_000),
            output_rate_nanos: Some(0),
        })
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
struct Marked;
#[async_trait::async_trait]
impl Asks for Marked {
    async fn marked(&self, _: &str, _: Option<&str>) -> Option<broca_subc::PrivateContent> {
        Some(broca_subc::PrivateContent {
            marked: true,
            sources: vec![broca_subc::SourceKind::PersonBrowser],
        })
    }
    async fn flow_providers(
        &self,
        _: &str,
    ) -> Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError> {
        unreachable!("run only")
    }
}

#[tokio::test]
async fn real_tool_path_captures_card_golden_and_ordinary_or_keyless_route_never_holds() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let clock = Arc::new(AtomicI64::new(1000));
    let consent = coordinator(store.clone(), io.clone(), clock);
    let provider = Arc::new(Provider::default());
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "consent-test",
        Arc::new(Catalog {
            provider_name: Some("TypeSafe".into()),
        }),
        vec![ModelPin::jev(provider.clone())],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE)
    .with_clock(Arc::new(|| 1000));
    let stop = tokio_util::sync::CancellationToken::new();
    let tools = Arc::new(
        LlmTools::new(Arc::new(system), Default::default(), Arc::new(Marked))
            .with_held(Arc::new(HeldCalls::new(
                store.clone(),
                "consent-test".into(),
            )))
            .with_consent(consent.clone(), stop.clone()),
    );
    let principal = BindPrincipal::Reserved {
        module_id: "caller".into(),
    };
    let stamp = SendScope::Stamped(BindScope {
        scope: scope(),
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    });
    for (key, held_capability) in [(Some("ordinary"), false), (None, true)] {
        let mut request = json!({"name":"llm_classify","arguments":args("typesafe/jev-1.13.0")});
        if let Some(key) = key {
            request["call_key"] = json!(key);
        }
        let refusal = tools
            .answer_with_capabilities(
                request.to_string().as_bytes(),
                Some(&principal),
                &stamp,
                held_capability,
            )
            .await
            .unwrap_err();
        assert_eq!(
            serde_json::from_slice::<Value>(&refusal).unwrap()["detail"]["consent"],
            "consent_required"
        );
    }
    assert!(io.requests.lock().unwrap().is_empty());
    assert!(store.recovering(1000).unwrap().is_empty());
    let request = json!({"name":"llm_classify","arguments":args("typesafe/jev-1.13.0"),"call_key":"raw:key-not-hex"});
    let response = tools
        .answer_with_capabilities(
            request.to_string().as_bytes(),
            Some(&principal),
            &stamp,
            true,
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(response.get()).unwrap()["held"]["reason"],
        "consent"
    );
    let captured = io.requests.lock().unwrap()[0].clone();
    let golden: Value =
        serde_json::from_str(include_str!("fixtures/llm-tools/private-content-card.json")).unwrap();
    assert_eq!(captured, golden);
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    io.live.store(true, Ordering::SeqCst);
    io.answers.lock().unwrap().push_back(record(
        "card-1",
        captured["dedup_key"].as_str().unwrap(),
        "allow_run",
    ));
    io.answered.notify_one();
    broca_test_support::wait_until("authorized tool outcome", || async {
        let rows = store
            .card_calls(captured["dedup_key"].as_str().unwrap(), 1000)
            .unwrap();
        if rows[0].state == HeldState::Final {
            Ok(())
        } else {
            Err("consent has not released the provider".to_owned())
        }
    })
    .await;
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    let replay = tools
        .answer_with_capabilities(
            request.to_string().as_bytes(),
            Some(&principal),
            &stamp,
            true,
        )
        .await
        .unwrap();
    assert!(serde_json::from_str::<Value>(replay.get())
        .unwrap()
        .get("held")
        .is_none());
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    assert_eq!(io.requests.lock().unwrap().len(), 1);
    assert_eq!(io.feeds.load(Ordering::SeqCst), 0);
    stop.cancel();
}

#[tokio::test]
async fn missing_provider_display_name_refuses_before_card_or_hold_is_written() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let provider = Arc::new(Provider::default());
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "missing-name",
        Arc::new(Catalog {
            provider_name: None,
        }),
        vec![ModelPin::jev(provider.clone())],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE)
    .with_clock(Arc::new(|| 1000));
    let stop = tokio_util::sync::CancellationToken::new();
    let tools = Arc::new(
        LlmTools::new(Arc::new(system), Default::default(), Arc::new(Marked))
            .with_held(Arc::new(HeldCalls::new(
                store.clone(),
                "missing-name".into(),
            )))
            .with_consent(consent, stop.clone()),
    );
    let principal = BindPrincipal::Reserved {
        module_id: "caller".into(),
    };
    let stamp = SendScope::Stamped(BindScope {
        scope: scope(),
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    });
    let request = json!({"name":"llm_classify","arguments":args("typesafe/jev-1.13.0"),"call_key":"missing-provider-name"});
    let reply = tools
        .answer_with_capabilities(
            request.to_string().as_bytes(),
            Some(&principal),
            &stamp,
            true,
        )
        .await
        .unwrap_err();
    assert_eq!(
        serde_json::from_slice::<Value>(&reply).unwrap()["detail"]["consent"],
        "consent_unavailable"
    );
    assert!(io.requests.lock().unwrap().is_empty());
    assert!(store.recovering(1000).unwrap().is_empty());
    assert!(store.pending_cards().unwrap().is_empty());
    assert!(store.grants().unwrap().is_empty());
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    stop.cancel();
}

#[tokio::test]
async fn startup_resumes_a_held_call_whose_card_and_grant_committed_before_authorization() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    io.live.store(true, Ordering::SeqCst);
    let consent = coordinator(store.clone(), io, Arc::new(AtomicI64::new(1000)));
    let key = "c".repeat(64);
    let batch = "d".repeat(64);
    let arguments = args("typesafe/jev-1.13.0");
    let mut request = hold(&batch, &key, "typesafe/jev-1.13.0", 1000);
    request.request = json!({"batch_id":batch,"model":arguments["model"],"questions":arguments["questions"],"items":[arguments["item"].clone()],"max_cost_usd":arguments["max_cost_usd"]}).to_string();
    request.frozen_row =
        json!({"input_rate_nanos":42_000_000,"output_rate_nanos":0,"charge_basis":"metered_api"})
            .to_string();
    store.hold(request).unwrap();
    store
        .put_card(&Card {
            dedup_key: key.clone(),
            scope: identity(),
            agent_id: scope().agent_id.unwrap(),
            provider: "typesafe".into(),
            content: "approved".into(),
            card_id: Some("approved-source".into()),
            state: CardState::Pending,
        })
        .unwrap();
    store
        .apply_answer(
            &identity(),
            CardAnswer {
                dedup_key: key,
                card_id: "approved-source".into(),
                decision: Decision::AllowRun,
                run_live: true,
            },
            1000,
        )
        .unwrap();
    assert_eq!(
        store.lookup(&batch, 1000).unwrap().unwrap().state,
        HeldState::Held
    );
    let provider = Arc::new(Provider::default());
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "restart-after-answer",
        Arc::new(Catalog {
            provider_name: Some("TypeSafe".into()),
        }),
        vec![ModelPin::jev(provider.clone())],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE)
    .with_clock(Arc::new(|| 1000));
    let stop = tokio_util::sync::CancellationToken::new();
    let tools = Arc::new(
        LlmTools::new(Arc::new(system), Default::default(), Arc::new(Marked))
            .with_held(Arc::new(HeldCalls::new(store.clone(), "restart".into())))
            .with_consent(consent, stop.clone()),
    );
    tools.recover_consent().unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        broca_test_support::wait_until("settled held-call startup", || async {
            let row = store.lookup(&batch, 1000).unwrap().unwrap();
            if row.state == HeldState::Final {
                Ok(row)
            } else {
                Err("grant has not resumed its held call".to_owned())
            }
        }),
    )
    .await
    .expect("settled card was not resumed");
    let row = store.lookup(&batch, 1000).unwrap().unwrap();
    assert!(!row.final_result.unwrap().is_error);
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    stop.cancel();
}
