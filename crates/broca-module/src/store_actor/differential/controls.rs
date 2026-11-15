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

cipher_tests!(
    a_view_handed_over_at_a_ride_is_applied_alike,
    token_count_pages_and_notices_are_alike
);

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

/// A compaction provider that is always ready and has a view to offer only once told to drop a
/// message at its next ride. It keeps each call it is sent: its method, and for a notice its
/// sequence number and the ordinals of the entries it carries.
#[derive(Default)]
pub(super) struct Compactor {
    bodies: Mutex<Vec<(String, Value)>>,
    methods: Mutex<Vec<String>>,
    calls: Mutex<Vec<Value>>,
    /// Entry ids learned from session.entries after Setup and from later notices. Each storage
    /// mints its own ids, so a view must name entries read from that same session.
    mids: Mutex<std::collections::BTreeMap<u64, String>>,
    read_entries: Mutex<Option<ReadEntries>>,
    /// The ordinal of the message the next ride's view drops.
    drop_at_ride: Mutex<Option<u64>>,
    /// The ordinal of the message the next ride's view cuts the history through.
    cut_at_ride: Mutex<Option<u64>>,
    /// The PNG (standard padded base64) the next ride's view inserts at the start of the
    /// history, in a user message, as Magic Context inserts the image it draws of its memories.
    image_at_ride: Mutex<Option<String>>,
    /// The views rides were answered with.
    views: Mutex<Vec<broca_core::compaction_v2::protocol::view::View>>,
    /// The events the notices told of, such as a view's refusal.
    events: Mutex<Vec<Value>>,
}

type ReadEntries = Arc<
    dyn Fn() -> futures::future::BoxFuture<
            'static,
            Vec<broca_core::compaction_v2::protocol::read::Entry>,
        > + Send
        + Sync,
>;

impl Compactor {
    /// Answers the next ride with a view dropping the message at `ordinal`.
    pub(super) fn drop_at_next_ride(&self, ordinal: u64) {
        *self.drop_at_ride.lock().unwrap() = Some(ordinal);
    }

    /// Answers the next ride with a view cutting the history through the message at
    /// `ordinal`: the session seals what comes before the message after it.
    pub(super) fn cut_at_next_ride(&self, ordinal: u64) {
        *self.cut_at_ride.lock().unwrap() = Some(ordinal);
    }

    /// Answers the next ride with a view inserting a user message that carries `png`.
    pub(super) fn insert_image_at_next_ride(&self, png: String) {
        *self.image_at_ride.lock().unwrap() = Some(png);
    }

    pub(super) fn views(&self) -> usize {
        self.views.lock().unwrap().len()
    }

    pub(super) fn reads(&self, read: ReadEntries) {
        *self.read_entries.lock().unwrap() = Some(read);
    }

    pub(super) fn reads_today(&self, wal: Arc<broca_core::FileWalStore>, key: BindIdentity) {
        self.reads(Arc::new(move || {
            let (records, lineage) = wal.read_records_with_lineage(&key).unwrap();
            let entries = broca_core::compaction_v2::transcript::project(
                &records,
                &lineage.unwrap().to_string(),
            )
            .unwrap()
            .entries;
            Box::pin(async move { entries })
        }));
    }

    pub(super) fn reads_memory(&self, backend: &'static MemoryBackend) {
        self.reads(Arc::new(move || {
            Box::pin(async move {
                let view = Session::view(
                    backend,
                    SessionAddr(store::ADDR.into()),
                    &store::triple(),
                    &store::deployment(),
                )
                .await
                .unwrap();
                crate::store_entries::all_entries(&view).await.unwrap()
            })
        }));
    }

    async fn entries(&self) -> Vec<broca_core::compaction_v2::protocol::read::Entry> {
        let read = self
            .read_entries
            .lock()
            .unwrap()
            .clone()
            .expect("the provider has its session.entries reader");
        read().await
    }

    async fn read_session(&self) {
        // Setup covers the current window, so notices need not repeat any of its entries.
        // Learn their ids through the same projection a provider reads from session.entries.
        let entries = self.entries().await;
        self.mids
            .lock()
            .unwrap()
            .extend(entries.into_iter().map(|entry| (entry.ordinal, entry.mid)));
    }

    /// Restart the provider after the entries are durable, then let Setup cover the whole
    /// window. No notice repeats that window, but the next ride must still name its entries.
    pub(super) async fn assert_drop_after_setup_without_notice(
        &self,
        ordinal: u64,
        bind: &BindIdentity,
    ) {
        use broca_core::compaction_v2::{protocol::ride, runtime};
        let entries = self.entries().await;
        let expected = entries
            .iter()
            .find(|entry| entry.ordinal == ordinal)
            .unwrap();
        let restarted = Self::default();
        restarted.reads(self.read_entries.lock().unwrap().clone().unwrap());
        let (mut setup, conditions) = {
            let bodies = self.bodies.lock().unwrap();
            let setup: broca_core::compaction_v2::protocol::setup::SetupRequest =
                serde_json::from_value(
                    bodies
                        .iter()
                        .rev()
                        .find(|(method, _)| method == "compaction.setup")
                        .unwrap()
                        .1
                        .clone(),
                )
                .unwrap();
            let conditions = serde_json::from_value(
                bodies
                    .iter()
                    .rev()
                    .find(|(method, _)| method == "compaction.notice")
                    .unwrap()
                    .1["conditions"]
                    .clone(),
            )
            .unwrap();
            (setup, conditions)
        };
        setup.newest = broca_core::compaction_v2::views::newest(&entries);
        assert!(setup.newest.as_ref().unwrap().ordinal >= ordinal);
        let runtime = runtime::Runtime::default();
        let plane = runtime::BoundPlane {
            plane: Some(&restarted),
            provider: "compactor",
            bind,
            scope: None,
        };
        runtime.observe(&entries);
        assert!(runtime.setup(&plane, &setup).await);
        restarted.drop_at_next_ride(ordinal);
        let answer = runtime
            .ride(
                &plane,
                &setup,
                &ride::RideRequest::new(vec![ride::RideCause::ModelChange], 1_000, conditions),
            )
            .await
            .unwrap();
        assert_eq!(
            restarted.methods.lock().unwrap().as_slice(),
            ["compaction.setup", "compaction.ride"],
            "Setup covers the entry, so no notice teaches the provider its id",
        );
        assert_eq!(
            serde_json::to_value(answer.view()).unwrap()["edits"][0]["mid"],
            expected.mid,
            "a Setup-covered entry is named without a notice",
        );
    }

    /// What each call stating its conditions (Setup, notice, ride) said of inserted images.
    pub(super) fn stated_accepts_images(&self) -> Vec<Value> {
        self.bodies
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, body)| body.get("conditions"))
            .map(|conditions| conditions["accepts_images"].clone())
            .collect()
    }

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
        if method == "compaction.setup" {
            self.read_session().await;
        }
        self.methods.lock().unwrap().push(method.to_owned());
        self.bodies
            .lock()
            .unwrap()
            .push((method.to_owned(), params.clone()));
        if let Some(events) = params.get("events").and_then(Value::as_array) {
            self.events.lock().unwrap().extend(events.iter().cloned());
        }
        let entries = params.get("entries").and_then(Value::as_array);
        let ordinals: Vec<Value> = entries
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.get("ordinal").cloned())
            .collect();
        for entry in entries.into_iter().flatten() {
            if let (Some(ordinal), Some(mid)) = (
                entry.get("ordinal").and_then(Value::as_u64),
                entry.get("mid").and_then(Value::as_str),
            ) {
                self.mids.lock().unwrap().insert(ordinal, mid.to_owned());
            }
        }
        self.calls.lock().unwrap().push(json!({
            "method": method,
            "seq": params.get("seq"),
            "entries": ordinals,
        }));
        let answer =
            match method {
                "compaction.setup" => json!({"answer": "ready"}),
                "compaction.notice" => json!({"answer": "received"}),
                "compaction.ride" => {
                    use broca_core::compaction_v2::protocol::{
                        read::Role,
                        view::{Edit, EditOp, InsertAt, InsertedImage, InsertedMessage, View},
                    };
                    // A missing entry gives an id no message has, which the session refuses; the
                    // scenario then finds its view unapplied.
                    let mid = |ordinal: u64| {
                        self.mids
                            .lock()
                            .unwrap()
                            .get(&ordinal)
                            .cloned()
                            .unwrap_or_default()
                    };
                    // A drop asked for answers this ride; a cut asked for waits for a ride with none.
                    let drop = self.drop_at_ride.lock().unwrap().take();
                    let op =
                        match drop {
                            Some(ordinal) => Some(EditOp::Drop {
                                mid: mid(ordinal),
                                part: None,
                            }),
                            None => match self.image_at_ride.lock().unwrap().take() {
                                Some(png) => Some(EditOp::Insert {
                                    at: InsertAt::Start,
                                    message: InsertedMessage::new(
                                        "memories",
                                        Role::User,
                                        vec!["Memories.".into()],
                                    )
                                    .with_images(vec![InsertedImage::new("image/png", png)]),
                                }),
                                None => self.cut_at_ride.lock().unwrap().take().map(|ordinal| {
                                    EditOp::Cut {
                                        through: mid(ordinal),
                                    }
                                }),
                            },
                        };
                    match op {
                        None => json!({"answer": "nothing"}),
                        Some(op) => {
                            let view = View::seal(1, vec![Edit::new(1, op)]);
                            self.views.lock().unwrap().push(view.clone());
                            json!({"answer": "view", "view": view})
                        }
                    }
                }
                _ => return Err(broca_core::compaction_v2::runtime::CallError::Unavailable),
            };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

/// A fetch plan's manifest: `plan` as the caller sent it, the `read` tool both sides' tool
/// planes answer, a system text, and what each subscribed provider declared.
pub(super) fn manifest(plan: Value, declarations: Vec<FrozenDeclaration>) -> Arc<PlanManifest> {
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
pub(super) fn compacting_plan() -> Arc<PlanManifest> {
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
        tool_gate: None,
    }
}

/// One session on today's storage and one in the new storage, run over the same script, each
/// executor offering its own planes.
struct Pair {
    today: Today,
    backend: &'static MemoryBackend,
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
        Self::new_in(
            script,
            gated,
            today_planes,
            store_planes,
            broca_session_store::keys::Cipher::XChaCha20Poly1305,
        )
        .await
    }

    /// [`Pair::new`], with the store's session in mode `cipher`.
    async fn new_in(
        script: Vec<Vec<NormalizedEvent>>,
        gated: bool,
        today_planes: Planes,
        store_planes: Planes,
        cipher: broca_session_store::keys::Cipher,
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
            &store::deployment().creating(cipher),
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
        )
        .with_readers(Arc::new(move || backend));
        let (commands, receiver) = mpsc::unbounded_channel();
        let served = tokio::spawn(actor.serve(receiver));
        Self {
            today,
            backend,
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

    /// Ends the store session's actor, once both sessions' control streams from their start
    /// were found the same.
    async fn end(self) -> Ended {
        super::streams::assert_alike(
            &super::streams::store(&self.commands).await,
            &super::streams::routed(&self.today.registry, &self.today.key).await,
        );
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
        let hooks = |receipt: &Option<broca_protocol::json::Json>| {
            receipt
                .as_ref()
                .and_then(|receipt| receipt.parse().get("hooks").cloned())
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
        tool_gate: None,
    }
}

/// A send with no tools of its own and a context window, which a run on the compaction
/// contract needs.
pub(super) fn limited(text: &str) -> SubmissionRequest {
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
    compacting_pair_in(
        script,
        gated,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    )
    .await
}

/// [`compacting_pair`], with the store's session in mode `cipher`.
async fn compacting_pair_in(
    script: Vec<Vec<NormalizedEvent>>,
    gated: bool,
    cipher: broca_session_store::keys::Cipher,
) -> (Pair, Arc<Compactor>, Arc<Compactor>) {
    let (old, new) = (
        Arc::new(Compactor::default()),
        Arc::new(Compactor::default()),
    );
    let pair = Pair::new_in(script, gated, compacting(&old), compacting(&new), cipher).await;
    old.reads_today(Arc::clone(&pair.today.wal), pair.today.key.clone());
    new.reads_memory(pair.backend);
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

/// Count metadata travels through the real read routes and notice lanes on both storages.
async fn token_count_pages_and_notices_are_alike(cipher: broca_session_store::keys::Cipher) {
    let (pair, old, new) = compacting_pair_in(
        vec![store::read_step("a1"), store::text_step("a2", "answer")],
        false,
        cipher,
    )
    .await;
    let plan = compacting_plan();
    let (today, store) = pair
        .send(
            "r1",
            limited("count these parts"),
            Some(&plan),
            Delivery::Queue,
        )
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    let ended = pair.end().await;
    let request =
        serde_json::value::to_raw_value(&json!({"select": {"by": "after"}, "detail": "full"}))
            .unwrap();
    let old_page = ended
        .today
        .registry
        .route_compaction_v2(
            &ended.today.key,
            Some("compactor".into()),
            "session.entries".into(),
            request.clone(),
        )
        .await
        .unwrap();
    let new_page = crate::store_entries::serve_compaction(
        &ended.parts,
        Some(&ended.session),
        &store::bind(),
        Some("compactor"),
        "session.entries",
        &request,
        &crate::store_entries::EntryAttribution::Known(false),
        None,
    )
    .await
    .unwrap();
    let old_page: Value = serde_json::from_str(old_page.get()).unwrap();
    let new_page: Value = serde_json::from_str(new_page.get()).unwrap();
    fn mask(value: &mut Value) {
        match value {
            Value::Object(object) => {
                for (key, member) in object {
                    if ["mid", "digest", "written_at", "completed_at"].contains(&key.as_str()) {
                        *member = json!(!member.is_null());
                    } else {
                        mask(member);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(mask),
            _ => {}
        }
    }
    let masked = |value: &Value| {
        let mut value = value.clone();
        mask(&mut value);
        value
    };
    assert_eq!(
        masked(&new_page),
        masked(&old_page),
        "full count-bearing pages"
    );
    assert!(new_page["token_family"].is_string());
    assert!(
        new_page["entries"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|entry| entry["parts"].as_array().unwrap())
            .any(|part| part["tokens"].get("output").is_some()),
        "a finished tool has its pair of counts"
    );
    let old_notices: Vec<_> = old
        .bodies
        .lock()
        .unwrap()
        .iter()
        .filter(|(method, _)| method == "compaction.notice")
        .map(|(_, body)| body.clone())
        .collect();
    let new_notices: Vec<_> = new
        .bodies
        .lock()
        .unwrap()
        .iter()
        .filter(|(method, _)| method == "compaction.notice")
        .map(|(_, body)| body.clone())
        .collect();
    assert!(!new_notices.is_empty());
    // Today's loop never ties its reading to a view; the new loop names the view each reading's
    // request was rendered under, none here (`0`). The new loop also states it takes no
    // inserted images, which it renders only as markers; today's states what the model takes.
    // Apart from those members the notices match.
    let without_view = |notice: &Value| {
        let mut notice = notice.clone();
        let conditions = notice["conditions"].as_object_mut().unwrap();
        let view = conditions.remove("last_prompt_view");
        let expected = conditions.get("last_prompt_tokens").map(|_| json!(0));
        assert_eq!(view, expected, "{notice:?}");
        assert_eq!(conditions.remove("accepts_images"), Some(json!(false)));
        masked(&notice)
    };
    let without_images = |notice: &Value| {
        let mut notice = notice.clone();
        notice["conditions"]
            .as_object_mut()
            .unwrap()
            .remove("accepts_images");
        masked(&notice)
    };
    assert_eq!(
        new_notices.iter().map(without_view).collect::<Vec<_>>(),
        old_notices.iter().map(without_images).collect::<Vec<_>>()
    );
    for notice in &new_notices {
        assert!(notice["token_family"].is_string());
        assert!(notice["conditions"]["system_tokens"].is_object());
        assert!(notice["conditions"]["tools_tokens"].is_object());
    }
    if cipher == broca_session_store::keys::Cipher::None {
        if let Some(dir) = std::env::var_os("BROCA_TOKEN_SAMPLE_DIR") {
            let dir = std::path::PathBuf::from(dir);
            let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(dir);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join("entries.json"),
                serde_json::to_vec_pretty(&new_page).unwrap(),
            )
            .unwrap();
            std::fs::write(
                dir.join("notice.json"),
                serde_json::to_vec_pretty(new_notices.last().unwrap()).unwrap(),
            )
            .unwrap();
        }
    }
    ended.assert_alike().await;
    ended.stop().await;
}

/// A view the compaction provider hands over at a ride, here the one a run on another model
/// starts with, is accepted and applied at that step's boundary, alike: both sessions record
/// the view and its application, the model is sent the same messages with the dropped message
/// left out, and a subscriber from each session's start gets the same control stream.
async fn a_view_handed_over_at_a_ride_is_applied_alike(cipher: broca_session_store::keys::Cipher) {
    let (pair, old, new) = compacting_pair_in(
        vec![
            store::text_step("a1", "hello"),
            store::text_step("a2", "again"),
        ],
        false,
        cipher,
    )
    .await;
    let plan = compacting_plan();
    let (today, store) = pair
        .send("r1", limited("first"), Some(&plan), Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    // The view drops the first run's answer, the session's second message.
    old.drop_at_next_ride(2);
    new.drop_at_next_ride(2);
    let mut second = limited("second");
    second.model = broca_protocol::request::ModelHandle::new("stub", "another");
    let (today, store) = pair.send("r2", second, None, Delivery::Queue).await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    assert_eq!(
        (old.views(), new.views()),
        (1, 1),
        "a ride handed a view over"
    );
    assert_eq!(new.calls(), old.calls());
    super::super::subscribe_tests::every_unit_resumes(&pair.commands).await;
    let ended = pair.end().await;
    ended.assert_alike().await;
    let (prompts, _) = ended.prompts();
    let last = prompts.last().unwrap();
    let has = |text: &str| last.iter().any(|said| said.ends_with(text));
    assert!(
        has("second") && !has("hello"),
        "the second run's model call leaves the dropped answer out: {last:?}; the view {:?}, \
         the ids notices named {:?}, the events they told {:?}",
        old.views.lock().unwrap(),
        old.mids.lock().unwrap(),
        old.events.lock().unwrap(),
    );
    let today_applied = ended
        .today_records()
        .iter()
        .filter(|record| matches!(record, WalRecord::CompactionViewAppliedV2 { .. }))
        .count();
    let store_applied = ended
        .store_records()
        .await
        .iter()
        .filter(|record| matches!(record, Record::ViewApplied(_)))
        .count();
    assert_eq!((today_applied, store_applied), (1, 1));
    ended.stop().await;
}

/// A step whose two tool calls share an id: the first copy is dispatched and the second is
/// answered with the duplicate error, alike. On the control lane the call starts once, and each
/// copy's finish carries its own key, today's as the store's: the dispatched copy's from its
/// intent, the refused copy's from its answer.
#[tokio::test]
async fn a_repeated_call_ids_copies_finish_with_keys_of_their_own_alike() {
    use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
    use broca_protocol::{MessageId, ProviderControl, ToolCallId, Usage};
    let call = |n: u64| {
        ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("c1".into()),
            tool_name: "read".into(),
            input: json!({ "n": n }).into(),
        })
    };
    let repeated = vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("a1".into()),
                content: vec![call(1), call(2)],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ];
    let pair = Pair::new(
        vec![repeated, store::text_step("a2", "done")],
        false,
        Planes::default(),
        Planes::default(),
    )
    .await;
    let (today, store) = pair
        .send("r1", store::request("first"), None, Delivery::Queue)
        .await;
    assert!(matches!(today, SendOutcome::Active { .. }), "{today:?}");
    assert_eq!(store, today);
    pair.idle().await;
    // Today's control lane: one start, and two finishes under two distinct keys, the first
    // the start's.
    let stream = super::streams::routed(&pair.today.registry, &pair.today.key).await;
    let keys = |kind: &str| -> Vec<Value> {
        stream
            .iter()
            .filter(|unit| unit["event"]["type"] == kind && unit["event"]["tool_call_id"] == "c1")
            .map(|unit| unit["event"]["call_key"].clone())
            .collect()
    };
    let (started, finished) = (keys("tool_started"), keys("tool_finished"));
    assert_eq!(started.len(), 1, "{stream:#?}");
    assert_eq!(finished.len(), 2, "{stream:#?}");
    assert!(finished.iter().all(Value::is_string), "{finished:?}");
    assert_eq!(finished[0], started[0]);
    assert_ne!(finished[0], finished[1]);
    // And the store's stream is the same, unit for unit.
    let ended = pair.end().await;
    ended.stop().await;
}
