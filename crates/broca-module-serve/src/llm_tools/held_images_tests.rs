use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_protocol::{
    BindPrincipal, BindScope, ParentState, ScopeKind, ScopeParent, SendScope, SessionScope,
};
use broca_store::grants::{GrantsStore, HeldCall, HeldState};
use broca_subc::{
    AnswersPage, ConsentError, ConsentRecord, ConsentState, ImageResolveError, ResolvedImage,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::consent::{Consent, ConsentIo, RunLiveness};
use super::held::HeldCalls;
use super::held_images::HELD_IMAGE_MAX_BYTES;
use super::{Asks, LlmTools};
use crate::system_one::{
    BatchCatalog, ClassifierProvider, Config, ImageResolver, ModelPin, ProviderReply, SystemOne,
    CLEF, LLM_CLASSIFY_STORE_ROLE,
};

const PIXEL: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";
const HANDLE: &str = "ih0123456789abcdef0123456789abcdef";

struct Source {
    present: AtomicBool,
    calls: AtomicUsize,
    scopes: Mutex<Vec<(String, Option<String>)>>,
}

#[async_trait]
impl ImageResolver for Source {
    async fn resolve(
        &self,
        _: &str,
        scope: &str,
        parent: Option<&str>,
    ) -> Result<ResolvedImage, ImageResolveError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.scopes
            .lock()
            .unwrap()
            .push((scope.into(), parent.map(str::to_owned)));
        if !self.present.load(Ordering::SeqCst) {
            return Err(ImageResolveError::Unavailable);
        }
        let bytes = STANDARD.decode(PIXEL).expect("fixture base64");
        Ok(ResolvedImage {
            mime: "image/png".into(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            bytes: PIXEL.into(),
        })
    }
}

#[derive(Default)]
struct Provider(Mutex<Vec<Value>>);

#[async_trait]
impl ClassifierProvider for Provider {
    fn egress_host(&self) -> String {
        "api.cloudflare.com".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, body: String) -> ProviderReply {
        self.0
            .lock()
            .unwrap()
            .push(serde_json::from_str(&body).expect("provider request JSON"));
        ProviderReply::Answered(br#"{"model":"clef","answers":{"q":{"type":"noul","noul":0.7}},"usage":{"input_tokens":10}}"#.to_vec())
    }
}

struct Catalog;
impl BatchCatalog for Catalog {
    fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
        Some(broca_catalog::ModelBatchRow {
            model_name: Some("Clef".into()),
            provider_name: Some("Cloudflare".into()),
            kind: Some("decision".into()),
            context_tokens: None,
            attachment: true,
            input_rate_nanos: Some(240_000_000),
            output_rate_nanos: Some(0),
        })
    }
}

struct Marked;
#[async_trait]
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
        Ok(broca_subc::FlowPolicy::NotApproved {
            message: "not a flow".into(),
        })
    }
}

struct Io {
    answer: Mutex<Option<ConsentRecord>>,
    live: Mutex<RunLiveness>,
    feed: Mutex<Vec<Value>>,
    refuse: AtomicBool,
    filing_gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    filing_entered: tokio::sync::Notify,
}
impl Default for Io {
    fn default() -> Self {
        Self {
            answer: Mutex::new(None),
            live: Mutex::new(RunLiveness::Live),
            feed: Mutex::new(Vec::new()),
            refuse: AtomicBool::new(false),
            filing_gate: Mutex::new(None),
            filing_entered: tokio::sync::Notify::new(),
        }
    }
}
#[async_trait]
impl ConsentIo for Io {
    async fn request(&self, _: &SessionScope, _: &Value) -> Result<String, ConsentError> {
        let gate = self.filing_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            self.filing_entered.notify_one();
            let _permit = gate.acquire().await.expect("filing gate open");
        }
        if self.refuse.load(Ordering::SeqCst) {
            return Err(ConsentError::Refused {
                code: "unavailable".into(),
                message: "no consent card".into(),
                detail: None,
            });
        }
        Ok("card".into())
    }
    async fn answers(&self, _: &SessionScope, _: u64) -> Result<AnswersPage, ConsentError> {
        Ok(AnswersPage {
            records: std::mem::take(&mut *self.feed.lock().unwrap()),
            cursor: 1,
            continuation: None,
        })
    }
    async fn wait(&self, _: &SessionScope, _: &str) -> Result<ConsentRecord, ConsentError> {
        let answer = self.answer.lock().unwrap().take();
        match answer {
            Some(answer) => Ok(answer),
            None => std::future::pending().await,
        }
    }
    async fn withdraw(&self, _: &SessionScope, _: &str) {}
    async fn run_live(&self, _: &SessionScope) -> RunLiveness {
        *self.live.lock().unwrap()
    }
}

fn principal() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "caller".into(),
    }
}
fn stamp() -> SendScope {
    SendScope::Stamped(BindScope {
        scope: SessionScope {
            owner: BindPrincipal::Reserved {
                module_id: "owner".into(),
            },
            scope_ref: "run".into(),
            scope_epoch: 1,
            agent_id: Some("agent".into()),
            run_id: Some("run".into()),
            flow_id: None,
            sender: None,
        },
        kind: ScopeKind::Ephemeral,
        parent: Some(Box::new(ScopeParent {
            owner: BindPrincipal::Reserved {
                module_id: "owner".into(),
            },
            scope_ref: "original-parent".into(),
            scope_epoch: 1,
        })),
        parent_state: Some(ParentState::Linked),
        owner_authorized: true,
    })
}
fn body(key: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"name":"llm_classify","call_key":key,"arguments":{
        "model":CLEF,"questions":{"q":{"type":"noul","instructions":"Is it urgent?"}},
        "item":{"state":"private screenshot","images":[{"handle":HANDLE}]},"max_cost_usd":1
    }}))
    .expect("call JSON")
}

struct Rig {
    tools: Arc<LlmTools>,
    held: Arc<HeldCalls>,
    consent: Arc<Consent>,
    io: Arc<Io>,
    source: Arc<Source>,
    provider: Arc<Provider>,
    system: Arc<SystemOne>,
    clock: Arc<AtomicI64>,
}
impl Rig {
    fn new(limit: usize) -> Self {
        let source = Arc::new(Source {
            present: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            scopes: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(Provider::default());
        let clock = Arc::new(AtomicI64::new(1000));
        let now = clock.clone();
        let system = Arc::new(
            SystemOne::new(
                Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
                "held-images",
                Arc::new(Catalog),
                vec![ModelPin::clef(provider.clone())],
                Config::default(),
                Arc::default(),
            )
            .with_store_role(LLM_CLASSIFY_STORE_ROLE)
            .with_image_resolver(source.clone())
            .with_clock(Arc::new(move || now.load(Ordering::SeqCst))),
        );
        Self::with_store(
            system,
            source,
            provider,
            clock,
            Arc::new(GrantsStore::open_in_memory().unwrap()),
            limit,
        )
    }
    fn with_store(
        system: Arc<SystemOne>,
        source: Arc<Source>,
        provider: Arc<Provider>,
        clock: Arc<AtomicI64>,
        store: Arc<GrantsStore>,
        limit: usize,
    ) -> Self {
        let held = Arc::new(HeldCalls::with_image_limit(
            store.clone(),
            "process".into(),
            limit,
        ));
        let io = Arc::new(Io::default());
        let now = clock.clone();
        let consent = Arc::new(
            Consent::new(store, io.clone())
                .with_clock(Arc::new(move || now.load(Ordering::SeqCst))),
        );
        // Tests drive decisions explicitly rather than racing a background consent reader.
        let stop = CancellationToken::new();
        stop.cancel();
        let tools = Arc::new(
            LlmTools::new(
                system.clone(),
                crate::config::LlmToolsConfig {
                    image_references: true,
                    private_content_providers: vec![],
                    ..Default::default()
                },
                Arc::new(Marked),
            )
            .with_held(held.clone())
            .with_consent(consent.clone(), stop),
        );
        Self {
            tools,
            held,
            consent,
            io,
            source,
            provider,
            system,
            clock,
        }
    }
    async fn call(&self, key: &str) -> Result<Value, Value> {
        self.tools
            .answer(&body(key), Some(&principal()), &stamp())
            .await
            .map(|raw| serde_json::from_str(raw.get()).expect("tool reply JSON"))
            .map_err(|bytes| serde_json::from_slice(&bytes).expect("tool refusal JSON"))
    }
    fn row(&self, key: &str) -> HeldCall {
        self.held
            .store
            .recovering(self.clock.load(Ordering::SeqCst))
            .unwrap()
            .into_iter()
            .find(|row| row.call_key == key)
            .expect("held call")
    }
    async fn approve(&self, key: &str) {
        let row = self.row(key);
        *self.io.answer.lock().unwrap() = Some(ConsentRecord {
            elicitation_id: "card".into(),
            state: ConsentState::Answered,
            fields: BTreeMap::from([
                ("requester_ref".into(), json!(row.dedup_key)),
                ("answered_choice_id".into(), json!("allow_run")),
            ]),
        });
        self.consent
            .wait_once(&row.dedup_key, self.clock.load(Ordering::SeqCst))
            .await
            .unwrap();
        assert_eq!(self.row(key).state, HeldState::Authorized);
    }
    async fn execute_authorized(&self, key: &str) -> HeldCall {
        let row = self.row(key);
        self.tools.recover_authorized().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let current = self
                    .held
                    .store
                    .lookup(&row.batch_id, self.clock.load(Ordering::SeqCst))
                    .unwrap()
                    .expect("call retained");
                if current.state == HeldState::Final && !self.held.images.contains(&row.batch_id) {
                    break current;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("authorized execution completes")
    }
}

#[tokio::test]
async fn held_image_approval_uses_original_snapshot_after_source_forgets_handle() {
    let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
    assert!(rig.call("first").await.unwrap().get("held").is_some());
    assert!(rig.held.images.bytes() > 0);
    rig.source.present.store(false, Ordering::SeqCst);
    rig.clock.store(2000, Ordering::SeqCst);
    rig.approve("first").await;
    assert!(
        !rig.execute_authorized("first")
            .await
            .final_result
            .expect("executed result")
            .is_error
    );
    assert_eq!(rig.source.calls.load(Ordering::SeqCst), 1);
    let calls = rig.provider.0.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0]["images"][0]["base64"].as_str() == Some(PIXEL));
    assert_eq!(rig.held.images.bytes(), 0);
}

#[tokio::test]
async fn held_image_over_cap_still_holds_without_bytes_then_resolves_again() {
    assert_eq!(
        HELD_IMAGE_MAX_BYTES,
        cortexkit_model_batch::image::IMAGE_MAX_BYTES * 16
    );
    for expired in [false, true] {
        let rig = Rig::new(1);
        assert!(rig.call("uncached").await.unwrap().get("held").is_some());
        assert_eq!(rig.held.images.bytes(), 0);
        assert!(rig.held.images.contains(&rig.row("uncached").batch_id));
        rig.source.present.store(!expired, Ordering::SeqCst);
        rig.approve("uncached").await;
        let result = rig
            .execute_authorized("uncached")
            .await
            .final_result
            .expect("final result");
        assert_eq!(result.is_error, expired);
        if expired {
            assert_eq!(
                serde_json::from_str::<Value>(&result.body).unwrap()["code"],
                "image_unavailable"
            );
        }
        assert_eq!(rig.source.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            rig.source.scopes.lock().unwrap().as_slice(),
            &[
                ("run".into(), Some("original-parent".into())),
                ("run".into(), Some("original-parent".into()))
            ]
        );
    }
}

#[tokio::test]
async fn held_image_snapshot_is_not_shared_with_another_call_using_the_same_handle() {
    let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
    assert!(rig.call("first").await.unwrap().get("held").is_some());
    let retained = rig.held.images.bytes();
    rig.source.present.store(false, Ordering::SeqCst);
    assert_eq!(
        rig.call("second").await.unwrap_err()["code"],
        "image_unavailable"
    );
    assert_eq!(rig.source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(rig.held.images.bytes(), retained);
    assert_eq!(rig.held.store.recovering(1000).unwrap().len(), 1);
}

#[tokio::test]
async fn held_image_restart_loses_scope_and_refuses_without_resolving() {
    let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
    assert!(rig.call("restart").await.unwrap().get("held").is_some());
    let restarted = Rig::with_store(
        rig.system.clone(),
        rig.source.clone(),
        rig.provider.clone(),
        rig.clock.clone(),
        rig.held.store.clone(),
        HELD_IMAGE_MAX_BYTES,
    );
    drop(rig);
    assert_eq!(restarted.held.images.bytes(), 0);
    restarted.approve("restart").await;
    let result = restarted
        .execute_authorized("restart")
        .await
        .final_result
        .expect("restart refusal");
    assert_eq!(
        serde_json::from_str::<Value>(&result.body).unwrap()["code"],
        "image_unavailable"
    );
    assert_eq!(restarted.source.calls.load(Ordering::SeqCst), 1);
    assert!(restarted.provider.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn held_image_terminal_paths_release_the_counter_synchronously() {
    for reason in [
        "declined",
        "withdrawn",
        "cancelled",
        "run_ended",
        "refused",
        "expired",
        "retained_too_long",
    ] {
        let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
        assert!(rig.call(reason).await.unwrap().get("held").is_some());
        assert!(rig.held.images.bytes() > 0);
        let row = rig.row(reason);
        let body = super::held::not_started(reason);
        match reason {
            "refused" => rig
                .held
                .store
                .refuse_card(&row.dedup_key, &body, 2000)
                .unwrap(),
            "expired" => rig.held.expire_due(row.deadline_at_ms).unwrap(),
            "retained_too_long" => {
                rig.held
                    .store
                    .sweep(row.admitted_at_ms + broca_store::grants::RETENTION_MS)
                    .unwrap();
            }
            _ => {
                assert!(rig
                    .held
                    .store
                    .finish_unstarted(&row.batch_id, &body, 2000)
                    .unwrap());
            }
        }
        assert_eq!(rig.held.images.bytes(), 0, "{reason}");
    }
}

fn admission(row: &HeldCall) -> super::admission::Admission {
    super::admission::Admission {
        scope: row.batch_scope.clone(),
        batch_id: row.batch_id.clone(),
        args_digest: row.args_digest.clone(),
        params: row.request.clone().expect("held request"),
        provider: row.provider.clone(),
    }
}

#[tokio::test]
async fn held_image_snapshot_requires_the_full_stamp_and_argument_digest() {
    let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
    rig.call("binding").await.unwrap();
    rig.approve("binding").await;
    let row = rig.row("binding");
    let retained = rig.held.images.bytes();
    rig.held
        .images
        .remove(&row.batch_id, row.admitted_at_ms - 1);
    assert_eq!(rig.held.images.bytes(), retained);
    assert!(rig
        .held
        .images
        .get(&row, &stamp(), &admission(&row))
        .unwrap()
        .is_some());
    let mut changed = admission(&row);
    changed.args_digest = "different".into();
    assert!(rig.held.images.get(&row, &stamp(), &changed).is_err());
    let mut changed = row.clone();
    changed.scope.scope_epoch += 1;
    assert!(rig
        .held
        .images
        .get(&changed, &stamp(), &admission(&changed))
        .is_err());
    let mut changed = stamp();
    if let SendScope::Stamped(bind) = &mut changed {
        bind.kind = ScopeKind::Head;
    }
    assert!(rig
        .held
        .images
        .get(&row, &changed, &admission(&row))
        .is_err());
}

#[tokio::test]
async fn held_image_cap_never_evicts_an_existing_snapshot() {
    let rig = Rig::new(1024);
    rig.call("first").await.unwrap();
    let mut retained = rig.held.images.bytes();
    assert!(retained > 0);
    let mut uncached = None;
    for i in 0..16 {
        let key = format!("later-{i}");
        assert!(rig.call(&key).await.unwrap().get("held").is_some());
        let current = rig.held.images.bytes();
        assert!(current <= 1024);
        if current == retained {
            uncached = Some(key);
            break;
        }
        retained = current;
    }
    let uncached = uncached.expect("the test cap must fill");
    let before = rig.source.calls.load(Ordering::SeqCst);
    rig.source.present.store(false, Ordering::SeqCst);
    rig.approve("first").await;
    rig.call("first").await.unwrap();
    assert_eq!(rig.source.calls.load(Ordering::SeqCst), before);
    assert_eq!(
        rig.call(&uncached).await.unwrap_err()["code"],
        "image_unavailable"
    );
    assert_eq!(rig.source.calls.load(Ordering::SeqCst), before + 1);
}

#[tokio::test]
async fn held_image_filing_refusal_releases_snapshot_before_returning() {
    let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
    rig.io.refuse.store(true, Ordering::SeqCst);
    assert_eq!(
        rig.call("filing-refused").await.unwrap_err()["code"],
        "not_started"
    );
    assert_eq!(rig.held.images.bytes(), 0);
    assert!(rig.held.images.entries().is_empty());
    assert!(rig.provider.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn held_image_sweep_keeps_unknown_runs_and_drops_provably_ended_runs() {
    let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
    rig.call("liveness").await.unwrap();
    let row = rig.row("liveness");
    let before = rig.held.images.bytes();
    *rig.io.live.lock().unwrap() = RunLiveness::Unknown;
    rig.held
        .sweep_image_entries(&rig.consent, 2000)
        .await
        .unwrap();
    assert_eq!(rig.held.images.bytes(), before);
    assert!(rig.held.images.contains(&row.batch_id));
    *rig.io.live.lock().unwrap() = RunLiveness::Ended;
    rig.held
        .sweep_image_entries(&rig.consent, 2000)
        .await
        .unwrap();
    assert_eq!(rig.held.images.bytes(), 0);
    assert!(!rig.held.images.contains(&row.batch_id));
    assert_eq!(
        rig.held
            .store
            .lookup(&row.batch_id, 2000)
            .unwrap()
            .unwrap()
            .state,
        HeldState::Held
    );
}

#[tokio::test]
async fn held_image_sweep_drops_entries_whose_durable_row_is_gone() {
    let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
    rig.call("gone").await.unwrap();
    let row = rig.row("gone");
    rig.held
        .sweep_image_entries(
            &rig.consent,
            row.admitted_at_ms + broca_store::grants::RETENTION_MS,
        )
        .await
        .unwrap();
    assert_eq!(rig.held.images.bytes(), 0);
    assert!(!rig.held.images.contains(&row.batch_id));
}

#[tokio::test]
async fn held_image_observed_consent_decline_withdrawal_and_expiry_drop_snapshots() {
    for (state, choice) in [
        (ConsentState::Answered, Some("deny")),
        (ConsentState::Withdrawn, None),
        (ConsentState::Expired, None),
    ] {
        let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
        rig.call("terminal-card").await.unwrap();
        let row = rig.row("terminal-card");
        let mut fields = BTreeMap::from([("requester_ref".into(), json!(row.dedup_key))]);
        if let Some(choice) = choice {
            fields.insert("answered_choice_id".into(), json!(choice));
        }
        *rig.io.answer.lock().unwrap() = Some(ConsentRecord {
            elicitation_id: "card".into(),
            state,
            fields,
        });
        assert!(!rig.consent.wait_once(&row.dedup_key, 1000).await.unwrap());
        assert_eq!(rig.held.images.bytes(), 0);
        assert!(!rig.held.images.contains(&row.batch_id));
        assert_eq!(
            rig.held
                .store
                .lookup(&row.batch_id, 1000)
                .unwrap()
                .unwrap()
                .state,
            HeldState::Final
        );
        assert!(rig.provider.0.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn held_image_recovery_feed_terminal_records_match_the_current_card_only() {
    for current in [false, true] {
        let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
        rig.call("feed-terminal").await.unwrap();
        let row = rig.row("feed-terminal");
        rig.io.feed.lock().unwrap().push(
            serde_json::to_value(ConsentRecord {
                elicitation_id: if current { "card" } else { "replaced-card" }.into(),
                state: ConsentState::Withdrawn,
                fields: BTreeMap::from([("requester_ref".into(), json!(row.dedup_key))]),
            })
            .expect("record JSON"),
        );
        rig.consent.recover(&row.dedup_key, 1000).await.unwrap();
        assert_eq!(rig.held.images.bytes() == 0, current);
        assert_eq!(rig.held.images.contains(&row.batch_id), !current);
        assert_eq!(
            rig.held
                .store
                .lookup(&row.batch_id, 1000)
                .unwrap()
                .unwrap()
                .state,
            if current {
                HeldState::Final
            } else {
                HeldState::Held
            }
        );
    }
}

#[tokio::test]
async fn held_image_recovery_explicit_answer_survives_replacement_expiry_on_the_same_page() {
    for expired_first in [false, true] {
        let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
        rig.call("mixed-page").await.unwrap();
        let row = rig.row("mixed-page");
        let answer = ConsentRecord {
            elicitation_id: "older-answered-card".into(),
            state: ConsentState::Answered,
            fields: BTreeMap::from([
                ("requester_ref".into(), json!(row.dedup_key)),
                ("answered_choice_id".into(), json!("allow_run")),
            ]),
        };
        let expired = ConsentRecord {
            elicitation_id: "card".into(),
            state: ConsentState::Expired,
            fields: BTreeMap::from([("requester_ref".into(), json!(row.dedup_key))]),
        };
        let records = if expired_first {
            vec![expired, answer]
        } else {
            vec![answer, expired]
        };
        *rig.io.feed.lock().unwrap() = records
            .into_iter()
            .map(|record| serde_json::to_value(record).expect("record JSON"))
            .collect();
        rig.consent.recover(&row.dedup_key, 1000).await.unwrap();
        assert_eq!(rig.row("mixed-page").state, HeldState::Authorized);
        assert!(rig.held.images.bytes() > 0);
        rig.source.present.store(false, Ordering::SeqCst);
        assert!(
            !rig.execute_authorized("mixed-page")
                .await
                .final_result
                .expect("authorized result")
                .is_error
        );
        assert_eq!(rig.source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(rig.held.images.bytes(), 0);
    }
}

#[tokio::test]
async fn held_image_cancelled_filing_keeps_the_first_persisted_snapshot_for_recovery() {
    let rig = Rig::new(HELD_IMAGE_MAX_BYTES);
    *rig.io.filing_gate.lock().unwrap() = Some(Arc::new(tokio::sync::Semaphore::new(0)));
    let tools = rig.tools.clone();
    let call = tokio::spawn(async move {
        tools
            .answer(&body("cancelled-filing"), Some(&principal()), &stamp())
            .await
    });
    rig.io.filing_entered.notified().await;
    let row = rig.row("cancelled-filing");
    assert_eq!(row.state, HeldState::Filing);
    let before = rig.held.images.bytes();
    assert!(before > 0);
    call.abort();
    assert!(call.await.expect_err("filing cancelled").is_cancelled());
    rig.source.present.store(false, Ordering::SeqCst);
    assert!(rig
        .call("cancelled-filing")
        .await
        .unwrap()
        .get("held")
        .is_some());
    rig.consent.recover(&row.dedup_key, 1000).await.unwrap();
    rig.approve("cancelled-filing").await;
    assert!(
        !rig.execute_authorized("cancelled-filing")
            .await
            .final_result
            .expect("executed result")
            .is_error
    );
    assert_eq!(rig.source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(rig.held.images.bytes(), 0);
}
