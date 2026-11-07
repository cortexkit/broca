//! The run controls the store actor hands the new loop, compared with what today's actor hands
//! today's loop, by sending the same sends to both: the compaction contract a planned session
//! starts on, the compaction state a session keeps from one run to the next, whether a send
//! named its own tools, and a plan's input transforms (`pre_user`) on a fresh run, a steer and a
//! queued send.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::hooks::{HookAnswer, HookCall};
use broca_core::WalStore as _;
use broca_core::{HookPlane, HookUnavailable};
use broca_protocol::content::ContentBlock;
use broca_protocol::SubmissionRequest;
use broca_protocol::{BindIdentity, Delivery, NormalizedEvent, RunId, SessionScope};
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{FrozenDeclaration, OpaqueJson, PlanManifest, WalRecord};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Notify};

use super::super::tests::{self as store, LoopExecutor, Planes};
use super::{today_with, Today};
use crate::actor::{LineageCmd, SendKey, SendOutcome, SendPlan};
use crate::executor::RunKind;

/// A `pre_user` provider that puts a note before the text of each user message it is asked
/// about, as Magic Context's input transform does in production. Once `unavailable` is set, it
/// can't be reached.
#[derive(Default)]
struct Noter {
    calls: AtomicUsize,
    unavailable: AtomicBool,
}

#[async_trait]
impl HookPlane for Noter {
    async fn hook(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&SessionScope>,
        _: HookCall,
    ) -> Result<HookAnswer, HookUnavailable> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(HookUnavailable);
        }
        Ok(serde_json::from_value(json!({
            "answer": "ops",
            "ops": [{"op": "prepend", "block": 0, "text": "[noted] "}],
        }))
        .unwrap())
    }
}

impl Noter {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

/// A compaction provider that is always ready and never has a view to offer. It keeps each
/// call it is sent: its method, and for a notice its sequence number and the ordinals of the
/// entries it carries.
#[derive(Default)]
struct Compactor {
    methods: Mutex<Vec<String>>,
    calls: Mutex<Vec<Value>>,
}

impl Compactor {
    fn setups(&self) -> usize {
        self.methods
            .lock()
            .unwrap()
            .iter()
            .filter(|method| *method == "compaction.setup")
            .count()
    }

    fn calls(&self) -> Vec<Value> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl broca_core::CompactionPlane for Compactor {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        // This is the first compaction protocol's Setup, which a session on the current
        // compaction contract never calls (it uses `call_v2`); the count shows it if one does.
        self.methods
            .lock()
            .unwrap()
            .push("first protocol setup".into());
        Err(broca_core::CompactionUnavailable)
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&SessionScope>,
        method: &str,
        params: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        self.methods.lock().unwrap().push(method.to_owned());
        let ordinals: Vec<Value> = params
            .get("entries")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.get("ordinal").cloned())
            .collect();
        self.calls.lock().unwrap().push(json!({
            "method": method,
            "seq": params.get("seq"),
            "entries": ordinals,
        }));
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => json!({"answer": "nothing"}),
            _ => return Err(broca_core::compaction_v2::runtime::CallError::Unavailable),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

/// A fetch plan's manifest: `plan` as the caller sent it, the `read` tool both sides' tool
/// planes answer, a system text, and what each subscribed provider declared.
fn manifest(plan: Value, declarations: Vec<FrozenDeclaration>) -> Arc<PlanManifest> {
    Arc::new(PlanManifest {
        rich_ui: None,
        plan: OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(vec![serde_json::from_value(json!({
            "name": "read",
            "description": "reads a file",
            "input_schema": {"type": "object"},
        }))
        .unwrap()]),
        system_text: "you are a worker".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations,
    })
}

/// A plan subscribing the provider `noter` to `pre_user`, with the `prepend` operation and the
/// given `on_unavailable`, the shape the production subscriptions to `pre_user` have.
fn noting_plan(on_unavailable: &str) -> Arc<PlanManifest> {
    let subscription = json!({
        "hook": "pre_user",
        "ops": ["prepend"],
        "on_unavailable": on_unavailable,
        "budget_ms": 30000,
    });
    manifest(
        json!({"step_transform_items": [{
            "provider": "noter",
            "preset": "frozen",
            "params": {},
            "subscriptions": [subscription],
        }]}),
        vec![FrozenDeclaration {
            provider: "noter".into(),
            answer: OpaqueJson::from_json_text(
                json!({"subscriptions": [subscription]}).to_string(),
            )
            .unwrap(),
        }],
    )
}

/// A plan naming a compaction provider.
fn compacting_plan() -> Arc<PlanManifest> {
    manifest(
        json!({"compaction_item": {"provider": "compactor", "preset": "default"}}),
        vec![],
    )
}

/// A send carrying no tools of its own, as a planned session's sends do.
fn untooled(text: &str) -> SubmissionRequest {
    let mut request = store::request(text);
    request.tools = None;
    request.policy_allowed = Default::default();
    request
}

fn noting(noter: &Arc<Noter>) -> Planes {
    Planes {
        hooks: Some(Arc::clone(noter) as Arc<dyn HookPlane>),
        compaction: None,
    }
}

/// One session on today's storage and one in the new storage, run over the same script, each
/// executor offering its own planes.
struct Pair {
    today: Today,
    commands: mpsc::UnboundedSender<LineageCmd>,
    served: tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
    store: Arc<LoopExecutor>,
    /// The store actor's setup parts, which reads of its session name tools and plans from.
    parts: broca_session_loop::parts::SharedParts,
    /// Each side's gate on its model calls, when the pair is gated: every model call waits for
    /// one release.
    gates: Vec<Arc<Notify>>,
}

/// What a pair left once its store session's actor ended: both sessions, and the store side's
/// executor.
struct Ended {
    today: Today,
    session: Session<&'static MemoryBackend>,
    store: Arc<LoopExecutor>,
    parts: broca_session_loop::parts::SharedParts,
}

impl Pair {
    async fn new(
        script: Vec<Vec<NormalizedEvent>>,
        gated: bool,
        today_planes: Planes,
        store_planes: Planes,
    ) -> Self {
        let today_gate = gated.then(|| Arc::new(Notify::new()));
        let store_gate = gated.then(|| Arc::new(Notify::new()));
        let today = today_with(script.clone(), today_gate.clone(), today_planes);
        let mut executor = store::executor(script, store_gate.clone());
        executor.planes = store_planes;
        let executor = Arc::new(executor);
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let session = Session::create(
            backend,
            SessionAddr(store::ADDR.into()),
            store::triple(),
            &store::deployment(),
            false,
        )
        .await
        .unwrap();
        let parts = broca_session_loop::parts::SharedParts::new(Box::new(MemoryParts::default()));
        let actor = super::super::StoreActor::new(
            store::bind(),
            session,
            executor.clone(),
            Box::new(parts.clone()),
        );
        let (commands, receiver) = mpsc::unbounded_channel();
        let served = tokio::spawn(actor.serve(receiver));
        Self {
            today,
            commands,
            served,
            store: executor,
            parts,
            gates: today_gate.into_iter().chain(store_gate).collect(),
        }
    }

    /// Sends `request` as `run_id` to both sessions, delivered as `delivery` and carrying a
    /// plan with `plan`'s manifest when one is given. Returns today's reply and the store's.
    async fn send(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        plan: Option<&Arc<PlanManifest>>,
        delivery: Delivery,
    ) -> (SendOutcome, SendOutcome) {
        let send_plan = || {
            plan.map(|manifest| SendPlan {
                identity: manifest.plan_identity.clone(),
                manifest: Some(Arc::clone(manifest)),
                model_check: None,
            })
        };
        let old = self
            .today
            .registry
            .route_submission_with_order(
                &self.today.key,
                RunId(run_id.into()),
                request.clone(),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                delivery,
                send_plan(),
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("today's actor opens");
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(LineageCmd::Send {
                session_class: None,
                retention: None,
                stop_sequences_named: false,
                conversation_key: None,
                title: None,
                run_id: RunId(run_id.into()),
                request: Box::new(request),
                kind: RunKind::FreshOrResume,
                key: SendKey::Minted,
                skip_materialization: false,
                delivery,
                plan: send_plan().map(Box::new),
                order: None,
                reply,
            })
            .unwrap();
        (old, answer.await.unwrap())
    }

    /// Lets one waiting model call through on each side.
    fn release(&self) {
        for gate in &self.gates {
            gate.notify_one();
        }
    }

    async fn idle(&self) {
        self.today.idle().await;
        store::idle(&self.commands).await;
    }

    /// Ends the store session's actor.
    async fn end(self) -> Ended {
        drop(self.commands);
        Ended {
            session: self.served.await.unwrap(),
            today: self.today,
            store: self.store,
            parts: self.parts,
        }
    }
}

impl Ended {
    /// What each run's start recorded that these comparisons read, from today's log.
    fn today_starts(&self) -> Vec<Value> {
        self.today_records()
            .into_iter()
            .filter_map(|record| match record {
                WalRecord::RunStarted {
                    run_id,
                    config,
                    compaction_plan_version,
                    hooks,
                    ..
                } => Some(json!({
                    "run_id": run_id.0,
                    "compaction_plan_version": compaction_plan_version,
                    "context_limit": config.context_limit,
                    "tools_were_omitted": config.tools_were_omitted,
                    "hooks": hooks,
                })),
                _ => None,
            })
            .collect()
    }

    /// The same facts of each run's start, from the store session.
    async fn store_starts(&self) -> Vec<Value> {
        self.store_records()
            .await
            .into_iter()
            .filter_map(|record| match record {
                Record::RunStart(start) => Some(json!({
                    "run_id": start.run_id,
                    "compaction_plan_version": start.compaction_plan_version,
                    "context_limit": start.context_limit,
                    "tools_were_omitted": start
                        .run_choices
                        .as_ref()
                        .and_then(|choices| choices.pointer("/config/tools_were_omitted"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    "hooks": start.hooks,
                })),
                _ => None,
            })
            .collect()
    }

    /// How each run ended, from today's log.
    fn today_ends(&self) -> Vec<Value> {
        self.today_records()
            .into_iter()
            .filter_map(|record| match record {
                WalRecord::RunFinished { reason, error, .. } => {
                    Some(json!({"reason": reason, "error": error}))
                }
                _ => None,
            })
            .collect()
    }

    /// How each run ended, from the store session.
    async fn store_ends(&self) -> Vec<Value> {
        self.store_records()
            .await
            .into_iter()
            .filter_map(|record| match record {
                Record::RunEnd(end) => Some(json!({"reason": end.reason, "error": end.error})),
                _ => None,
            })
            .collect()
    }

    /// The hooks each steer and queued send recorded, in order, from today's log.
    fn today_send_hooks(&self) -> Vec<(&'static str, Value)> {
        self.today_records()
            .into_iter()
            .filter_map(|record| match record {
                WalRecord::Steered { hooks, .. } => Some(("steer", json!(hooks))),
                WalRecord::Queued { hooks, .. } => Some(("queued", json!(hooks))),
                _ => None,
            })
            .collect()
    }

    /// The same, from the store session, whose records keep them in their receipts.
    async fn store_send_hooks(&self) -> Vec<(&'static str, Value)> {
        let hooks = |receipt: &Option<Value>| {
            receipt
                .as_ref()
                .and_then(|receipt| receipt.get("hooks"))
                .cloned()
                .unwrap_or(Value::Null)
        };
        self.store_records()
            .await
            .into_iter()
            .filter_map(|record| match record {
                Record::Steer(steer) => Some(("steer", hooks(&steer.receipt))),
                Record::QueuedSend(send) => Some(("queued", hooks(&send.receipt))),
                _ => None,
            })
            .collect()
    }

    fn today_records(&self) -> Vec<WalRecord> {
        self.today
            .wal
            .read_records(&self.today.key)
            .unwrap()
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    }

    async fn store_records(&self) -> Vec<Record> {
        self.session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .map(|(_, record)| record)
            .collect()
    }

    /// The text of every message each model call was sent, today's side first.
    fn prompts(&self) -> (Vec<Vec<String>>, Vec<Vec<String>>) {
        let texts = |calls: Vec<broca_protocol::request::CallOptions>| {
            calls
                .iter()
                .map(|call| {
                    call.prompt
                        .iter()
                        .flat_map(|message| &message.content)
                        .filter_map(|block| match block {
                            ContentBlock::Text { text } => Some(text.clone()),
                            _ => None,
                        })
                        .collect()
                })
                .collect()
        };
        (
            texts(self.today.executor.provider.inner.calls()),
            texts(self.store.provider.inner.calls()),
        )
    }

    /// The two sessions hold the same conversation, their runs' starts record the same
    /// compaction, tool and hook facts, every model call was sent the same messages, and reads
    /// with each opt-in serve the same.
    async fn assert_alike(&self) {
        assert_eq!(
            store::texts(&self.session),
            self.today.texts().await,
            "the conversations differ"
        );
        assert_eq!(
            self.store_starts().await,
            self.today_starts(),
            "the runs' starts differ"
        );
        let (old, new) = self.prompts();
        assert_eq!(new, old, "the model calls were sent different messages");
        super::display_reads_alike(&self.today, &self.session, &self.parts).await;
    }

    async fn stop(self) {
        assert!(self.today.registry.stop_and_seal().await);
    }
}

/// A planned send whose plan names a compaction provider starts its session on the compaction
/// contract on both storages, and both runs' starts record the same compaction facts. The
/// second run keeps the compaction state the first left, as today's actor keeps it for its
/// life: neither storage sends the provider Setup again.
#[tokio::test]
async fn a_plan_naming_a_compaction_provider_starts_on_the_contract_alike() {
    let (pair, old, new) = compacting_pair(super::hello(), false).await;
    let plan = compacting_plan();
    let (today, store) = pair
        .send("r1", limited("first"), Some(&plan), Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    let (today, store) = pair
        .send("r2", limited("second"), None, Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    let ended = pair.end().await;
    ended.assert_alike().await;
    let starts = ended.store_starts().await;
    assert_eq!(starts.len(), 2);
    for start in &starts {
        assert_eq!(
            start["compaction_plan_version"],
            json!(broca_core::compaction_v2::PLAN_VERSION),
            "{start}"
        );
    }
    assert_eq!(
        old.setups(),
        1,
        "today's actor sends Setup once in its life"
    );
    assert_eq!(
        new.setups(),
        old.setups(),
        "the store actor's second run sent Setup again: {:?}",
        new.methods.lock().unwrap()
    );
    ended.stop().await;
}

/// A planned send whose plan subscribes a provider to `pre_user` has its input transformed on
/// both storages: both runs' starts record the same hooks, the model is sent the same noted
/// input, and both conversations hold it.
#[tokio::test]
async fn a_planned_sends_input_is_transformed_alike() {
    let (old, new) = (Arc::new(Noter::default()), Arc::new(Noter::default()));
    let pair = Pair::new(super::hello(), false, noting(&old), noting(&new)).await;
    let plan = noting_plan("pass");
    let (today, store) = pair
        .send("r1", untooled("first"), Some(&plan), Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    let ended = pair.end().await;
    ended.assert_alike().await;
    assert_eq!((old.calls(), new.calls()), (1, 1));
    let starts = ended.store_starts().await;
    assert!(starts[0]["hooks"].is_object(), "{}", starts[0]);
    let (_, prompts) = ended.prompts();
    assert!(
        prompts[0].contains(&"[noted] first".to_owned()),
        "{prompts:?}"
    );
    ended.stop().await;
}

/// A steer into a planned session's running run goes through the same transforms on both
/// storages, and its record keeps the same hooks.
#[tokio::test]
async fn a_steers_input_is_transformed_alike() {
    let (old, new) = (Arc::new(Noter::default()), Arc::new(Noter::default()));
    let pair = Pair::new(super::hello(), true, noting(&old), noting(&new)).await;
    let plan = noting_plan("pass");
    let (today, store) = pair
        .send("r1", untooled("first"), Some(&plan), Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    let (today, store) = pair
        .send("s1", untooled("more"), None, Delivery::Steer)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.release();
    pair.release();
    pair.idle().await;
    let ended = pair.end().await;
    ended.assert_alike().await;
    assert_eq!((old.calls(), new.calls()), (2, 2));
    let hooks = ended.store_send_hooks().await;
    assert_eq!(hooks, ended.today_send_hooks());
    assert!(
        matches!(hooks.as_slice(), [("steer", Value::Object(_))]),
        "{hooks:?}"
    );
    assert!(
        store::texts(&ended.session).contains(&"[noted] more".to_owned()),
        "{:?}",
        store::texts(&ended.session)
    );
    ended.stop().await;
}

/// A send queued behind a planned session's running run goes through the transforms when it is
/// queued. When it drains, its run reuses what they did rather than asking again, on both
/// storages: the queued records and the drained run's start keep the same hooks, and the model
/// is sent the same noted input.
#[tokio::test]
async fn a_queued_sends_transforms_are_reused_when_it_drains_alike() {
    let (old, new) = (Arc::new(Noter::default()), Arc::new(Noter::default()));
    let pair = Pair::new(super::hello(), true, noting(&old), noting(&new)).await;
    let plan = noting_plan("pass");
    let (today, store) = pair
        .send("r1", untooled("first"), Some(&plan), Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    let (today, store) = pair
        .send("r2", untooled("second"), None, Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Pending { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.release();
    pair.release();
    pair.idle().await;
    let ended = pair.end().await;
    ended.assert_alike().await;
    assert_eq!(
        (old.calls(), new.calls()),
        (2, 2),
        "the queued send is transformed once, when it is queued"
    );
    let hooks = ended.store_send_hooks().await;
    assert_eq!(hooks, ended.today_send_hooks());
    assert!(
        matches!(hooks.as_slice(), [("queued", Value::Object(_))]),
        "{hooks:?}"
    );
    let starts = ended.store_starts().await;
    assert_eq!(starts.len(), 2);
    assert!(starts[1]["hooks"].is_object(), "{}", starts[1]);
    let (_, prompts) = ended.prompts();
    assert!(
        prompts[1].contains(&"[noted] second".to_owned()),
        "{prompts:?}"
    );
    ended.stop().await;
}

/// A send naming no tools, into a session whose plan confines its tools, runs on the plan's
/// tools, and both runs' starts record that the send left them out.
#[tokio::test]
async fn a_send_leaving_its_tools_to_a_confining_plan_records_it_alike() {
    let pair = Pair::new(super::hello(), false, Planes::default(), Planes::default()).await;
    let plan = manifest(json!({"tool_items": [{"preset": "worker"}]}), vec![]);
    let mut request = untooled("first");
    request.principal = Some(super::core());
    request.scope = super::head_scope(super::core());
    let (today, store) = pair.send("r1", request, Some(&plan), Delivery::Queue).await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    let ended = pair.end().await;
    ended.assert_alike().await;
    assert_eq!(
        ended.store_starts().await[0]["tools_were_omitted"],
        json!(true)
    );
    ended.stop().await;
}

/// A fresh run whose input a refusing transform can't reach (`on_unavailable: refuse`, the
/// provider unavailable) is still answered active on both storages: its start is written, and
/// it ends with the same error before any model call.
#[tokio::test]
async fn a_refused_input_ends_its_run_before_the_model_alike() {
    let (old, new) = (Arc::new(Noter::default()), Arc::new(Noter::default()));
    for noter in [&old, &new] {
        noter.unavailable.store(true, Ordering::SeqCst);
    }
    let pair = Pair::new(super::hello(), false, noting(&old), noting(&new)).await;
    let plan = noting_plan("refuse");
    let (today, store) = pair
        .send("r1", untooled("first"), Some(&plan), Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    let ended = pair.end().await;
    ended.assert_alike().await;
    let ends = ended.store_ends().await;
    assert_eq!(ends, ended.today_ends());
    assert_eq!(ends.len(), 1);
    assert_eq!(ends[0]["reason"], json!("error"), "{}", ends[0]);
    assert_eq!(
        ends[0]["error"]["provider_code"],
        json!(cortexkit_role_llm_runner::errors::PRE_USER_UNAVAILABLE),
        "{}",
        ends[0]
    );
    assert_eq!(ended.prompts(), (vec![], vec![]), "no model call was made");
    ended.stop().await;
}

/// Once the provider a refusing plan subscribes can't be reached, a steer into the running run
/// and a send queued behind it are both refused `pre_user_unavailable` on both storages, and
/// neither is written.
#[tokio::test]
async fn a_refused_steer_or_queued_send_writes_nothing_alike() {
    let (old, new) = (Arc::new(Noter::default()), Arc::new(Noter::default()));
    let pair = Pair::new(super::hello(), true, noting(&old), noting(&new)).await;
    let plan = noting_plan("refuse");
    let (today, store) = pair
        .send("r1", untooled("first"), Some(&plan), Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    for noter in [&old, &new] {
        noter.unavailable.store(true, Ordering::SeqCst);
    }
    for (run_id, delivery) in [("s1", Delivery::Steer), ("r2", Delivery::Queue)] {
        let (today, store) = pair.send(run_id, untooled("more"), None, delivery).await;
        assert_eq!(
            today,
            SendOutcome::PreUserUnavailable {
                provider: "noter".into()
            },
            "{delivery:?}"
        );
        assert_eq!(store, today, "{delivery:?}");
    }
    pair.release();
    pair.idle().await;
    let ended = pair.end().await;
    ended.assert_alike().await;
    assert_eq!(ended.today_send_hooks(), vec![]);
    assert_eq!(ended.store_send_hooks().await, vec![]);
    ended.stop().await;
}

/// The planes of an executor whose compaction provider is `compactor`.
fn compacting(compactor: &Arc<Compactor>) -> Planes {
    Planes {
        hooks: None,
        compaction: Some(Arc::clone(compactor) as Arc<dyn broca_core::CompactionPlane>),
    }
}

/// A send with no tools of its own and a context window, which a run on the compaction
/// contract needs.
fn limited(text: &str) -> SubmissionRequest {
    let mut request = untooled(text);
    request.context_limit = Some(200_000);
    request
}

/// A pair whose executors each offer their own compaction provider, which keeps every call it
/// is sent. Returns the pair, then today's side's provider, then the store side's.
async fn compacting_pair(
    script: Vec<Vec<NormalizedEvent>>,
    gated: bool,
) -> (Pair, Arc<Compactor>, Arc<Compactor>) {
    let (old, new) = (
        Arc::new(Compactor::default()),
        Arc::new(Compactor::default()),
    );
    let pair = Pair::new(script, gated, compacting(&old), compacting(&new)).await;
    (pair, old, new)
}

/// A run on the compaction contract that reaches its step limit with a tool call unanswered
/// ends with that call answered as never run. Today's actor has its runs tell the compaction
/// provider of the session's changes once their end is durable, so on both storages the
/// provider hears of that answer before the session's next run starts, and then the same
/// calls follow.
#[tokio::test]
async fn a_change_at_a_runs_end_is_told_before_the_next_run_alike() {
    let (pair, old, new) = compacting_pair(
        vec![store::read_step("a1"), store::text_step("a2", "again")],
        false,
    )
    .await;
    let plan = compacting_plan();
    let mut first = limited("first");
    first.stop_when = vec![broca_protocol::request::StopCondition::StepCount { n: 1 }];
    let (today, store) = pair.send("r1", first, Some(&plan), Delivery::Queue).await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    assert_eq!(
        new.calls(),
        old.calls(),
        "between the runs the compaction provider was told different things"
    );
    let (today, store) = pair
        .send("r2", limited("second"), None, Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    assert_eq!(new.calls(), old.calls());
    let ended = pair.end().await;
    ended.assert_alike().await;
    ended.stop().await;
}

/// A tool batch's results are told to the compaction provider once they are durable, as
/// today's actor has its runs tell it, not left for the next step's notice. So a message
/// steered in during the step reaches the provider in a notice of its own after the results,
/// on both storages.
#[tokio::test]
async fn a_tool_batchs_results_are_told_once_durable_alike() {
    let (pair, old, new) = compacting_pair(
        vec![store::read_step("a1"), store::text_step("a2", "again")],
        true,
    )
    .await;
    let plan = compacting_plan();
    let (today, store) = pair
        .send("r1", limited("first"), Some(&plan), Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    let (today, store) = pair
        .send("s1", limited("more"), None, Delivery::Steer)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.release();
    pair.release();
    pair.idle().await;
    assert_eq!(new.calls(), old.calls());
    let ended = pair.end().await;
    ended.assert_alike().await;
    ended.stop().await;
}
