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
        Asks, ConsumerFormat, LlmTools,
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
        project_id: None,
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
            project_id: None,
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
            project_id: None,
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

struct ProjectRig {
    tools: Arc<LlmTools>,
    store: Arc<GrantsStore>,
    io: Arc<FakeIo>,
    provider: Arc<Provider>,
    clock: Arc<AtomicI64>,
    stop: tokio_util::sync::CancellationToken,
}

impl ProjectRig {
    fn new() -> Self {
        let store = Arc::new(GrantsStore::open_in_memory().unwrap());
        let io = Arc::new(FakeIo::default());
        let clock = Arc::new(AtomicI64::new(1000));
        let consent = coordinator(store.clone(), io.clone(), clock.clone());
        let provider = Arc::new(Provider::default());
        let system_clock = clock.clone();
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
        .with_clock(Arc::new(move || system_clock.load(Ordering::SeqCst)));
        let stop = tokio_util::sync::CancellationToken::new();
        let tools = Arc::new(
            LlmTools::new(Arc::new(system), Default::default(), Arc::new(Marked))
                .with_held(Arc::new(HeldCalls::new(
                    store.clone(),
                    "consent-test".into(),
                )))
                .with_consent(consent, stop.clone()),
        );
        Self {
            tools,
            store,
            io,
            provider,
            clock,
            stop,
        }
    }

    async fn call(&self, run: &str, project: Option<&str>, key: &str) -> Value {
        let mut session = scope();
        session.scope_ref = run.into();
        session.run_id = Some(run.into());
        let stamp = SendScope::Stamped(BindScope {
            scope: session,
            owner_authorized: true,
            kind: broca_protocol::ScopeKind::Head,
            parent: None,
            parent_state: None,
        });
        let request =
            json!({"name":"llm_classify", "arguments":args("typesafe/jev-1.13.0"), "call_key":key});
        let response = self
            .tools
            .answer_for_consumer_in_project(
                request.to_string().as_bytes(),
                Some(&BindPrincipal::Reserved {
                    module_id: "caller".into(),
                }),
                &stamp,
                ConsumerFormat {
                    result_format: None,
                    held_reply: true,
                },
                project,
            )
            .await
            .unwrap();
        serde_json::from_str(response.get()).unwrap()
    }

    async fn allow_always(&self) -> Grant {
        let card = self.io.requests.lock().unwrap()[0].clone();
        self.io.answers.lock().unwrap().push_back(record(
            "card-1",
            card["dedup_key"].as_str().unwrap(),
            "allow_always",
        ));
        self.io.answered.notify_one();
        broca_test_support::wait_until("always grant persisted", || async {
            self.store
                .grants()
                .unwrap()
                .into_iter()
                .next()
                .ok_or_else(|| "answer has not committed".to_owned())
        })
        .await
    }
}

impl Drop for ProjectRig {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[tokio::test]
async fn always_grants_are_project_scoped_listed_and_revocable_through_the_tool_path() {
    let rig = ProjectRig::new();
    assert_eq!(
        rig.call("run", Some("project-a"), "first").await["held"]["reason"],
        "consent"
    );
    let golden: Value = serde_json::from_str(include_str!(
        "../../broca-subc/tests/fixtures/consent/broca/private-content-card-project.json"
    ))
    .unwrap();
    assert_eq!(rig.io.requests.lock().unwrap()[0], golden);
    let grant = rig.allow_always().await;
    broca_test_support::wait_until("original project call completed", || async {
        let row = rig
            .store
            .card_calls(golden["dedup_key"].as_str().unwrap(), 1000)
            .unwrap()
            .pop()
            .unwrap();
        if row.state == HeldState::Final {
            Ok(())
        } else {
            Err("held call has not executed".to_owned())
        }
    })
    .await;
    assert_eq!(grant.kind, GrantKind::Always);
    assert_eq!(grant.project.as_deref(), Some("project-a"));
    assert_eq!(grant.holder, scope().agent_id.unwrap());
    assert_eq!(grant.provider, "typesafe");
    assert_eq!(grant.source_card_id, "card-1");
    assert!(grant.scope.is_none());
    assert!(grant.expires_at_ms.is_none());
    assert!(rig
        .call("later-run", Some("project-a"), "later")
        .await
        .get("held")
        .is_none());
    assert_eq!(rig.provider.0.load(Ordering::SeqCst), 2);
    assert_eq!(rig.io.requests.lock().unwrap().len(), 1);
    assert_eq!(
        rig.call("other-project", Some("project-b"), "other").await["held"]["reason"],
        "consent"
    );
    assert_eq!(
        rig.call("no-project", None, "unstamped").await["held"]["reason"],
        "consent"
    );
    let cards = rig.io.requests.lock().unwrap().clone();
    assert_eq!(cards.len(), 3);
    assert_eq!(
        cards[2]["options"],
        json!([
            {"id":"allow_run","label":"Allow for this program run","effect":"grant"},
            {"id":"allow_24h","label":"Allow for 24 hours","effect":"grant"},
            {"id":"deny","label":"Deny","effect":"decline"}
        ])
    );
    let permissions = broca_module_serve::llm_tools::permissions::Permissions::new(
        rig.store.clone(),
        Some(rig.io.clone()),
    );
    let list = permissions
        .answer(
            br#"{"name":"permissions.list","arguments":{}}"#,
            Some(&BindPrincipal::Direct),
        )
        .await
        .unwrap();
    let list: Value = serde_json::from_str(list.get()).unwrap();
    assert_eq!(list["grants"][0]["card_ref"], "card-1");
    assert_eq!(
        list["grants"][0]["scope_label"],
        "Send private content to typesafe always in project-a"
    );
    let revoke = permissions
        .answer(
            br#"{"name":"permissions.revoke","arguments":{"id":"card-1"}}"#,
            Some(&BindPrincipal::Direct),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(revoke.get()).unwrap()["outcome"],
        "revoked"
    );
    assert_eq!(rig.store.generation(&grant.holder, "typesafe").unwrap(), 1);
    assert_eq!(
        rig.call("run", Some("project-a"), "after-revoke").await["held"]["reason"],
        "consent"
    );
    let cards = rig.io.requests.lock().unwrap();
    assert_eq!(cards.len(), 4);
    assert_ne!(cards[0]["dedup_key"], cards[3]["dedup_key"]);
}

#[tokio::test]
async fn late_always_answer_records_the_attested_project_and_admits_the_next_run() {
    let rig = ProjectRig::new();
    rig.call("run", Some("project-a"), "first").await;
    rig.clock.store(1000 + HOLD_MS + 1, Ordering::SeqCst);
    let grant = rig.allow_always().await;
    assert_eq!(grant.created_at_ms, 1000 + HOLD_MS + 1);
    assert_eq!(grant.project.as_deref(), Some("project-a"));
    assert_eq!(
        rig.provider.0.load(Ordering::SeqCst),
        0,
        "an expired call must not execute"
    );
    assert!(rig
        .call("next-run", Some("project-a"), "next")
        .await
        .get("held")
        .is_none());
    assert_eq!(rig.provider.0.load(Ordering::SeqCst), 1);
    assert_eq!(rig.io.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn recovered_held_calls_match_always_grants_only_against_their_frozen_project() {
    let path = broca_test_support::scratch_root().join("project-recovery.db");
    let store = Arc::new(GrantsStore::open(&path).unwrap());
    let clock = Arc::new(AtomicI64::new(1001));
    let consent = coordinator(store.clone(), Arc::new(FakeIo::default()), clock.clone());
    let mut rows = Vec::new();
    for (run, project) in [
        ("a", Some("project-a")),
        ("b", Some("project-b")),
        ("none", None),
    ] {
        let mut request = hold(run, "", "typesafe/jev-1.13.0", 1000);
        request.scope.scope_ref = run.into();
        request.batch_scope = broca_store::system_one::BatchScope::Run {
            agent_id: request.agent_id.clone(),
            run_id: run.into(),
        };
        request.project_id = project.map(str::to_owned);
        request.dedup_key = consent
            .dedup_key(&request.agent_id, "typesafe", &request.scope)
            .unwrap();
        let body = broca_subc::consent_card::private_content_card_for_project(
            "TypeSafe",
            "Jev",
            &[broca_subc::SourceKind::PersonBrowser],
            &request.dedup_key,
            project,
        )
        .unwrap();
        rows.push(consent.file(request, body, 1000).await.unwrap());
    }
    store
        .apply_answer(
            &rows[0].scope,
            CardAnswer {
                dedup_key: rows[0].dedup_key.clone(),
                card_id: "card-1".into(),
                decision: Decision::AllowAlways,
                run_live: false,
            },
            1001,
        )
        .unwrap();
    drop(consent);
    drop(store);
    let store = Arc::new(GrantsStore::open(&path).unwrap());
    let consent = coordinator(store.clone(), Arc::new(FakeIo::default()), clock);
    assert_eq!(consent.resume_granted(1001).await.unwrap(), 1);
    for (index, expected) in [HeldState::Authorized, HeldState::Held, HeldState::Held]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            store
                .lookup(&rows[index].batch_id, 1001)
                .unwrap()
                .unwrap()
                .state,
            expected
        );
    }
}

#[tokio::test]
async fn adding_the_always_option_replaces_card_content_without_changing_the_dedup_rule() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    io.live.store(true, Ordering::SeqCst);
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let key = consent
        .dedup_key(&scope().agent_id.unwrap(), "typesafe", &identity())
        .unwrap();
    consent
        .file(hold("first", &key, "Jev", 1000), body(&key, "Jev"), 1000)
        .await
        .unwrap();
    let mut request = hold("joining", &key, "Jev", 1000);
    request.project_id = Some("project-a".into());
    let body = broca_subc::consent_card::private_content_card_for_project(
        "TypeSafe",
        "Jev",
        &[broca_subc::SourceKind::PersonBrowser],
        &key,
        Some("project-a"),
    )
    .unwrap();
    consent.file(request, body, 1000).await.unwrap();
    let cards = io.requests.lock().unwrap().clone();
    assert_eq!(cards.len(), 2);
    assert_eq!(cards[0]["dedup_key"], cards[1]["dedup_key"]);
    assert_ne!(cards[0]["options"], cards[1]["options"]);
    io.answers
        .lock()
        .unwrap()
        .push_back(record("card-2", &key, "allow_run"));
    assert!(!consent.wait_once(&key, 1000).await.unwrap());
    for row in store.card_calls(&key, 1000).unwrap() {
        assert_eq!(row.card_id.as_deref(), Some("card-2"));
        assert_eq!(row.state, HeldState::Authorized);
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

#[test]
fn either_reply_format_name_lets_a_consumer_hold_calls() {
    let caps = |names: &[&str]| names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
    for (declared, result_format, held_reply) in [
        (
            caps(&["tool-result/v1", "held-reply/v1"]),
            Some("tool-result/v1"),
            true,
        ),
        (
            caps(&["content/v1", "held-reply/v1"]),
            Some("content/v1"),
            true,
        ),
        (caps(&["tool-result/v1"]), Some("tool-result/v1"), false),
        (caps(&["held-reply/v1"]), None, false),
        (caps(&[]), None, false),
    ] {
        assert_eq!(
            ConsumerFormat::from_capabilities(Some(&declared)),
            ConsumerFormat {
                result_format,
                held_reply
            },
            "{declared:?}"
        );
    }
    assert_eq!(
        ConsumerFormat::from_capabilities(None),
        ConsumerFormat::default()
    );
}

/// A consumer built against tool-provider 0.13 declares the reply format as `tool-result/v1`.
/// It must get the same held call and consent card as a `content/v1` consumer, the catalog in
/// its own name, and answers as tool results whose `structuredContent` is the tool result.
#[tokio::test]
async fn a_tool_result_v1_consumer_is_held_for_consent_and_gets_structured_answers() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let provider = Arc::new(Provider::default());
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "tool-result-consumer",
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
                "tool-result-consumer".into(),
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
    let declared = vec!["tool-result/v1".to_owned(), "held-reply/v1".to_owned()];
    let consumer = ConsumerFormat::from_capabilities(Some(&declared));

    let catalog = |consumer| {
        let tools = tools.clone();
        let stamp = stamp.clone();
        async move {
            let body = json!({"name":"tool.catalog","arguments":{"params":{}}}).to_string();
            let reply = tools
                .answer_for_consumer(body.as_bytes(), None, &stamp, consumer)
                .await
                .unwrap();
            serde_json::from_str::<Value>(reply.get()).unwrap()["tools"][0]["result_format"].clone()
        }
    };
    assert_eq!(catalog(consumer).await, "tool-result/v1");
    assert_eq!(catalog(ConsumerFormat::default()).await, "tool-result/v1");

    let request = json!({"name":"llm_classify","arguments":args("typesafe/jev-1.13.0"),"call_key":"raw:tool-result"});
    let held = tools
        .answer_for_consumer(
            request.to_string().as_bytes(),
            Some(&principal),
            &stamp,
            consumer,
        )
        .await
        .unwrap();
    let held: Value = serde_json::from_str(held.get()).unwrap();
    assert_eq!(held["held"]["reason"], "consent");
    assert!(held.get("structuredContent").is_none());
    let captured = io.requests.lock().unwrap()[0].clone();
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
    let answer = tools
        .answer_for_consumer(
            request.to_string().as_bytes(),
            Some(&principal),
            &stamp,
            consumer,
        )
        .await
        .unwrap();
    let answer: Value = serde_json::from_str(answer.get()).unwrap();
    let structured = &answer["structuredContent"];
    assert_eq!(structured["answers"]["q"]["noul"], 0.5);
    assert!(structured["cost_usd"].is_number());
    assert_eq!(
        serde_json::from_str::<Value>(answer["content"][0]["text"].as_str().unwrap()).unwrap(),
        *structured
    );
    assert!(answer.get("held").is_none());
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
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
            project_id: None,
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

/// A cerebellum stand-in whose answer each step of a test sets: `None` is no answer.
#[derive(Default)]
struct Answering(Mutex<Option<broca_subc::PrivateContent>>);
impl Answering {
    fn set(&self, answer: Option<broca_subc::PrivateContent>) {
        *self.0.lock().unwrap() = answer;
    }
}
#[async_trait::async_trait]
impl Asks for Answering {
    async fn marked(&self, _: &str, _: Option<&str>) -> Option<broca_subc::PrivateContent> {
        self.0.lock().unwrap().clone()
    }
    async fn flow_providers(
        &self,
        _: &str,
    ) -> Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError> {
        unreachable!("run only")
    }
}

/// A catalog whose provider display name can be withdrawn, which is what leaves a card with
/// nothing to name and ends the consent step `consent_unavailable`.
struct NamedCatalog(AtomicBool);
impl BatchCatalog for NamedCatalog {
    fn row(&self, model: &str) -> Option<broca_catalog::ModelBatchRow> {
        let mut row = Catalog {
            provider_name: Some("TypeSafe".into()),
        }
        .row(model)?;
        if !self.0.load(Ordering::SeqCst) {
            row.provider_name = None;
        }
        Some(row)
    }
}

fn marked_by(sources: Vec<broca_subc::SourceKind>) -> Option<broca_subc::PrivateContent> {
    Some(broca_subc::PrivateContent {
        marked: true,
        sources,
    })
}

/// Settle a card for `scope_ref` the way an answer would, minting a grant (or a deny) for the
/// test agent and `typesafe`. The card is never filed, so no held call waits on it.
fn decide(store: &GrantsStore, scope_ref: &str, card_id: &str, decision: Decision) {
    let mut card_scope = identity();
    card_scope.scope_ref = scope_ref.into();
    let dedup_key = format!("decided-{card_id}");
    store
        .put_card(&Card {
            project_id: None,
            dedup_key: dedup_key.clone(),
            scope: card_scope.clone(),
            agent_id: scope().agent_id.unwrap(),
            provider: "typesafe".into(),
            content: "decided elsewhere".into(),
            card_id: Some(card_id.into()),
            state: CardState::Pending,
        })
        .unwrap();
    store
        .apply_answer(
            &card_scope,
            CardAnswer {
                dedup_key,
                card_id: card_id.into(),
                decision,
                run_live: false,
            },
            1000,
        )
        .unwrap();
}

/// Every private-content verdict and every consent outcome a run can reach moves its own
/// `health.check` counter and no other, and the first call each standing grant releases is
/// logged once, naming the grant and never the call's content.
#[tokio::test]
async fn consent_health_counts_each_verdict_and_outcome_and_logs_each_grant_once() {
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let io = Arc::new(FakeIo::default());
    let consent = coordinator(store.clone(), io.clone(), Arc::new(AtomicI64::new(1000)));
    let provider = Arc::new(Provider::default());
    let catalog = Arc::new(NamedCatalog(AtomicBool::new(true)));
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "consent-health",
        catalog.clone(),
        vec![ModelPin::jev(provider.clone())],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE)
    .with_clock(Arc::new(|| 1000));
    let cerebellum = Arc::new(Answering::default());
    let stop = tokio_util::sync::CancellationToken::new();
    let tools = Arc::new(
        LlmTools::new(Arc::new(system), Default::default(), cerebellum.clone())
            .with_held(Arc::new(HeldCalls::new(
                store.clone(),
                "consent-health".into(),
            )))
            .with_consent(consent, stop.clone()),
    );
    let principal = BindPrincipal::Reserved {
        module_id: "caller".into(),
    };
    let logs = broca_test_support::LogCapture::new();
    let call = |scope_ref: &str, key: &str, reads_held_reply: bool| {
        let mut session = scope();
        session.scope_ref = scope_ref.into();
        let stamp = SendScope::Stamped(BindScope {
            scope: session,
            owner_authorized: true,
            kind: broca_protocol::ScopeKind::Head,
            parent: None,
            parent_state: None,
        });
        let request =
            json!({"name":"llm_classify","arguments":args("typesafe/jev-1.13.0"),"call_key":key});
        let tools = tools.clone();
        let principal = principal.clone();
        logs.scope_future(async move {
            match tools
                .answer_with_capabilities(
                    request.to_string().as_bytes(),
                    Some(&principal),
                    &stamp,
                    reads_held_reply,
                )
                .await
            {
                Ok(reply) => Ok(serde_json::from_str::<Value>(reply.get()).unwrap()),
                Err(body) => Err(serde_json::from_slice::<Value>(&body).unwrap()),
            }
        })
    };
    let health = || tools.consent_health();
    let zero = json!({
        "verdicts": {"not_asked": 0, "unmarked": 0, "marked": 0, "unreachable": 0},
        "marked_outcomes": {
            "released_by_grant": 0, "card_filed": 0, "denied_by_standing_deny": 0,
            "refused_consent_required": 0, "refused_private_content_unknown": 0,
            "refused_private_content_unreachable": 0, "refused_by_flow_policy": 0,
            "consent_unavailable": 0,
        },
    });
    assert_eq!(health(), zero);
    let mut expected = zero.clone();

    // Unmarked: the call reaches its provider and no consent outcome moves.
    cerebellum.set(Some(broca_subc::PrivateContent::unmarked()));
    assert!(call("run", "unmarked", true).await.is_ok());
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    expected["verdicts"]["unmarked"] = json!(1);
    assert_eq!(health(), expected);

    // No answer: counted apart from a marked scope, and refused for want of a source.
    cerebellum.set(None);
    let refused = call("run", "unreachable", true).await.unwrap_err();
    assert_eq!(refused["detail"]["consent"], "private_content_unreachable");
    expected["verdicts"]["unreachable"] = json!(1);
    expected["marked_outcomes"]["refused_private_content_unreachable"] = json!(1);
    assert_eq!(health(), expected);

    // Marked with no source a card can name.
    cerebellum.set(marked_by(vec![broca_subc::SourceKind::Other(
        "future_source".into(),
    )]));
    let refused = call("unknown-run", "unknown", true).await.unwrap_err();
    assert_eq!(refused["detail"]["consent"], "private_content_unknown");
    expected["verdicts"]["marked"] = json!(1);
    expected["marked_outcomes"]["refused_private_content_unknown"] = json!(1);
    assert_eq!(health(), expected);

    // Marked, and the caller cannot read a held reply.
    cerebellum.set(marked_by(vec![broca_subc::SourceKind::PersonBrowser]));
    let refused = call("run", "no-hold", false).await.unwrap_err();
    assert_eq!(refused["detail"]["consent"], "consent_required");
    expected["verdicts"]["marked"] = json!(2);
    expected["marked_outcomes"]["refused_consent_required"] = json!(1);
    assert_eq!(health(), expected);

    // Marked, and the card has no provider name to show.
    catalog.0.store(false, Ordering::SeqCst);
    let refused = call("run", "unnamed", true).await.unwrap_err();
    assert_eq!(refused["detail"]["consent"], "consent_unavailable");
    catalog.0.store(true, Ordering::SeqCst);
    expected["verdicts"]["marked"] = json!(3);
    expected["marked_outcomes"]["consent_unavailable"] = json!(1);
    assert_eq!(health(), expected);

    // Marked, and the person already denied this provider for this run.
    decide(&store, "denied-run", "deny-card", Decision::Deny);
    let refused = call("denied-run", "denied", true).await.unwrap_err();
    assert_eq!(refused["detail"]["consent"], "denied");
    expected["verdicts"]["marked"] = json!(4);
    expected["marked_outcomes"]["denied_by_standing_deny"] = json!(1);
    assert_eq!(health(), expected);

    // Marked, and a standing 24-hour grant releases the call without a card. Two calls under
    // one grant log one line; a second grant logs its own.
    let releases = || {
        logs.events()
            .into_iter()
            .filter(|event| {
                event.target == "llm_tools.consent"
                    && event.level == tracing::Level::INFO
                    && event.message()
                        == "a standing grant released a private-content call without a consent card"
            })
            .collect::<Vec<_>>()
    };
    assert!(releases().is_empty());
    decide(&store, "another-run", "grant-a", Decision::Allow24h);
    assert!(call("run", "grant-a-1", true).await.is_ok());
    assert!(call("run", "grant-a-2", true).await.is_ok());
    assert_eq!(provider.0.load(Ordering::SeqCst), 3);
    expected["verdicts"]["marked"] = json!(6);
    expected["marked_outcomes"]["released_by_grant"] = json!(2);
    assert_eq!(health(), expected);
    let logged = releases();
    assert_eq!(logged.len(), 1, "{logged:?}");
    let line = &logged[0];
    assert_eq!(line.fields["grant_id"], "grant-a");
    assert_eq!(line.fields["agent_id"], "agent-id-must-not-be-displayed");
    assert_eq!(line.fields["provider"], "typesafe");
    assert_eq!(line.fields["kind"], "24_hours");
    assert_eq!(
        line.fields["expires_at_ms"],
        (1000 + 24 * 60 * 60 * 1000).to_string()
    );
    assert_eq!(line.fields["scope_ref"], "run");
    // Nothing from the call itself: not the question, the item or the model's answer.
    for value in line.fields.values() {
        for content in ["Is this likely?", "private text", "noul"] {
            assert!(!value.contains(content), "{value}");
        }
    }

    assert!(store.revoke("grant-a").unwrap());
    decide(&store, "another-run", "grant-b", Decision::Allow24h);
    assert!(call("run", "grant-b-1", true).await.is_ok());
    expected["verdicts"]["marked"] = json!(7);
    expected["marked_outcomes"]["released_by_grant"] = json!(3);
    assert_eq!(health(), expected);
    let logged = releases();
    assert_eq!(logged.len(), 2, "{logged:?}");
    assert_eq!(logged[1].fields["grant_id"], "grant-b");

    // Marked, and held behind a card. This runs after the grants are revoked: a standing
    // 24-hour grant covers every call of the agent on that provider, so a call held earlier
    // could be released in the background whenever its card's waiter next wakes, which would
    // make the counts above a race.
    assert!(store.revoke("grant-b").unwrap());
    let held = call("run", "card", true).await.unwrap();
    assert_eq!(held["held"]["reason"], "consent");
    assert_eq!(io.requests.lock().unwrap().len(), 1);
    expected["verdicts"]["marked"] = json!(8);
    expected["marked_outcomes"]["card_filed"] = json!(1);
    assert_eq!(health(), expected);
    assert_eq!(releases().len(), 2, "a held call is not a grant release");

    // Every call the check found marked or could not check ended in exactly one outcome.
    let outcomes: u64 = expected["marked_outcomes"]
        .as_object()
        .unwrap()
        .values()
        .map(|count| count.as_u64().unwrap())
        .sum();
    assert_eq!(outcomes, 8 + 1);
    stop.cancel();
}
