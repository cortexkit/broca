//! Sends with an ordered delivery list, and the string sends a plan's senders make, through
//! today's actor and the store actor, compared reply by reply and by what each session recorded
//! about them: each order, each queued send's sender, principal and mode, each ending, each
//! delivery, and each run's start and end. Admission's refusals, a send expired on arrival, a
//! dedupe key superseding an undelivered send, an `idle_start` send starting a run, a send that
//! outlives a run's end and rides on a later one, and interrupts (on an idle session, during a
//! model step, during a tool call, and across a crash before the interrupt's run) are each
//! answered and recorded alike. Each scenario's sessions then read alike: `session.read` in every
//! opt-in mode on a few pages (a ride's notice among the carrying message's display facts, each
//! delivered entry's `delivered_as`), and the control stream from the start, where a ride is one
//! `notice_delivered`.

use std::sync::Arc;

use broca_protocol::content::ContentBlock;
use broca_protocol::{BindPrincipal, Delivery, NormalizedEvent, RunId, SubmissionRequest};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{OpaqueJson, PlanManifest, WalRecord};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Notify};

use super::super::tests::{self as store, LoopExecutor};
use super::{display_reads_alike, store_starts, streams, today_restarted, today_riding, Today};
use crate::actor::{LineageCmd, LineageStatus, SendKey, SendOutcome, SendPlan};
use crate::executor::RunKind;

/// The session's owner.
fn core() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

/// A module the plan names as a sender, without the `idle_start` grant, with one template.
const AFT: &str = "aft";
/// A module the plan names as a sender, granted `idle_start`.
const MC: &str = "magic-context";
/// A module the plan names as a sender, granted `interrupt`.
const OCCI: &str = "occi";

fn module(name: &str) -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: name.into(),
    }
}

/// The template the plan gives `aft`: a reminder with an integer slot.
fn reminder() -> cortexkit_role_llm_runner::delivery_order::Template {
    serde_json::from_value(json!({
        "id": "reminder",
        "text": "{n} files changed",
        "slots": {"n": {"type": "integer", "min": 0, "max": 99}},
    }))
    .unwrap()
}

/// A template whose slot names a part of the session, which this storage doesn't render yet.
fn pointer() -> cortexkit_role_llm_runner::delivery_order::Template {
    serde_json::from_value(json!({
        "id": "pointer",
        "text": "see {part}",
        "slots": {"part": {"type": "part_tag"}},
    }))
    .unwrap()
}

/// The session's plan: the `read` tool, and its senders.
fn plan() -> Arc<PlanManifest> {
    let plan = json!({"senders": [
        {"module": AFT, "templates": [reminder(), pointer()]},
        {"module": MC, "idle_start": true},
        {"module": OCCI, "interrupt": true},
    ]});
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
        declarations: vec![],
    })
}

/// A send of `text` from `principal`, carrying no tools of its own, as a planned session's sends
/// do.
fn from(principal: BindPrincipal, text: &str) -> SubmissionRequest {
    let mut request = store::request(text);
    request.tools = None;
    request.policy_allowed = Default::default();
    request.principal = Some(principal);
    request
}

/// A send naming `template` with `slots` and no text of its own.
fn templated(
    principal: BindPrincipal,
    template: &cortexkit_role_llm_runner::delivery_order::Template,
    digest: Option<&str>,
    slots: Value,
) -> (SubmissionRequest, Value) {
    let mut request = from(principal, "");
    request.input = Vec::new();
    let digest = digest
        .map(str::to_owned)
        .unwrap_or_else(|| cortexkit_role_llm_runner::delivery_order::template_digest(template));
    (
        request,
        json!({"id": template.id, "digest": digest, "slots": slots}),
    )
}

/// One session on today's storage and one in the new storage, over the same script.
struct Pair {
    today: Today,
    commands: mpsc::UnboundedSender<LineageCmd>,
    served: tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
    store: Arc<LoopExecutor>,
    /// The store actor's setup parts, kept so its session's reads can name its runs' tools.
    parts: SharedParts,
    /// Where the store's session is kept, which a restarted actor loads it from.
    backend: &'static MemoryBackend,
    gates: Vec<Arc<Notify>>,
}

impl Pair {
    async fn new(script: Vec<Vec<NormalizedEvent>>, gated: bool) -> Self {
        Self::with_planes(script, gated, Default::default(), Default::default()).await
    }

    /// [`Self::new`], with today's executor offering `today_planes` and the store's
    /// `store_planes`.
    async fn with_planes(
        script: Vec<Vec<NormalizedEvent>>,
        gated: bool,
        today_planes: store::Planes,
        store_planes: store::Planes,
    ) -> Self {
        let today_gate = gated.then(|| Arc::new(Notify::new()));
        let store_gate = gated.then(|| Arc::new(Notify::new()));
        let today = today_riding(script.clone(), today_gate.clone(), today_planes);
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
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
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
            commands,
            served,
            store: executor,
            parts,
            backend,
            gates: today_gate.into_iter().chain(store_gate).collect(),
        }
    }

    /// Both processes die where they stand and start again, each finding only its durable
    /// records: today's module over a copy of its log, and a store actor loading the session.
    /// Each new executor answers with `script`. The dead processes' runs never write again.
    async fn restarted(self, script: Vec<Vec<NormalizedEvent>>) -> Self {
        let today = today_restarted(&self.today, script.clone());
        self.served.abort();
        let _ = self.served.await;
        let executor = Arc::new(store::executor(script, None));
        let (commands, served) =
            store::reopen(self.backend, Arc::clone(&executor), &self.parts).await;
        Self {
            today,
            commands,
            served,
            store: executor,
            parts: self.parts,
            backend: self.backend,
            gates: Vec::new(),
        }
    }

    /// The session's first send, from its owner, carrying the plan; both sides start its run.
    async fn first(&self, run_id: &str) {
        let (old, new) = self
            .send(run_id, from(core(), "first"), None, Some(plan()), None)
            .await;
        assert!(matches!(old, SendOutcome::Active { .. }), "{old:?}");
        assert_eq!(new, old, "the first send's replies differ");
    }

    /// Sends `request` as `run_id` to both sessions with the delivery list `order`, or as a
    /// plain queued send without one, under `key` when given. Returns today's reply and the
    /// store's.
    async fn send(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        order: Option<Value>,
        plan: Option<Arc<PlanManifest>>,
        key: Option<&str>,
    ) -> (SendOutcome, SendOutcome) {
        self.send_as(run_id, request, Delivery::Queue, order, plan, key)
            .await
    }

    /// [`Self::send`] with the string `delivery` beside the order.
    async fn send_as(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        delivery: Delivery,
        order: Option<Value>,
        plan: Option<Arc<PlanManifest>>,
        key: Option<&str>,
    ) -> (SendOutcome, SendOutcome) {
        let order: Option<broca_protocol::DeliveryOrder> =
            order.map(|order| serde_json::from_value(order).unwrap());
        let send_plan = || {
            plan.as_ref().map(|manifest| SendPlan {
                identity: manifest.plan_identity.clone(),
                manifest: Some(Arc::clone(manifest)),
                model_check: None,
            })
        };
        let send_key = || match key {
            Some(key) => SendKey::SendId(key.into()),
            None => SendKey::Minted,
        };
        let old = self
            .today
            .registry
            .route_submission_with_order(
                &self.today.key,
                RunId(run_id.into()),
                request.clone(),
                RunKind::FreshOrResume,
                send_key(),
                false,
                delivery,
                send_plan(),
                None,
                None,
                None,
                None,
                false,
                order.clone(),
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
                key: send_key(),
                skip_materialization: false,
                delivery,
                plan: send_plan().map(Box::new),
                order: order.map(Box::new),
                reply,
            })
            .unwrap();
        (old, answer.await.unwrap())
    }

    /// A send with the string `delivery` and no list, on both sides; the replies must be equal.
    /// Returns today's.
    async fn alike_as(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        delivery: Delivery,
    ) -> SendOutcome {
        let (old, new) = self
            .send_as(run_id, request, delivery, None, None, None)
            .await;
        assert_eq!(new, old, "the replies to {run_id} differ");
        old
    }

    /// Sends on both sides, and the replies must be equal; returns today's.
    async fn alike(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        order: Option<Value>,
        key: Option<&str>,
    ) -> SendOutcome {
        let (old, new) = self.send(run_id, request, order, None, key).await;
        assert_eq!(new, old, "the replies to {run_id} differ");
        old
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

    /// Waits until both sessions run `run_id`, then lets one model call through on each side,
    /// so each release meets that run's call, never an earlier run's.
    async fn release_into(&self, run_id: &str) {
        let running = |status: LineageStatus| matches!(&status, LineageStatus::Active { run_id: running, .. } if running.0 == run_id);
        broca_test_support::wait_until(&format!("both sessions to run {run_id}"), || async {
            let today = self
                .today
                .registry
                .route_status(&self.today.key)
                .await
                .unwrap();
            let store = store::status(&self.commands).await;
            if running(today.clone()) && running(store.clone()) {
                Ok(())
            } else {
                Err(format!("today {today:?}, store {store:?}"))
            }
        })
        .await;
        self.release();
    }

    /// Ends the store session's actor, and checks the two sessions recorded their sends alike:
    /// the same conversation, the same send ledger, the same sends still queued, the same runs'
    /// starts, and every model call sent the same messages and results; and that they read
    /// alike: the same control stream from the start, and the same `session.read` replies in
    /// every opt-in mode. Returns the ledger, the queue, and how many notices the session holds:
    /// `notice_delivered` events in its stream, and notices its `session.read` serves.
    async fn assert_alike(self) -> (Vec<Value>, Vec<String>, (usize, usize)) {
        self.assert_alike_ids(true).await
    }

    /// [`Self::assert_alike`]; `same_ids` false sets each stream unit's message id aside (see
    /// [`streams::assert_alike_but_message_ids`]).
    async fn assert_alike_ids(self, same_ids: bool) -> (Vec<Value>, Vec<String>, (usize, usize)) {
        let stream = streams::store(&self.commands).await;
        let today_stream = streams::routed(&self.today.registry, &self.today.key).await;
        if same_ids {
            streams::assert_alike(&stream, &today_stream);
        } else {
            streams::assert_alike_but_message_ids(&stream, &today_stream);
        }
        let notices = stream
            .iter()
            .filter(|unit| unit["event"]["type"] == "notice_delivered")
            .count();
        drop(self.commands);
        let session = self.served.await.unwrap();
        display_reads_alike(&self.today, &session, &self.parts).await;
        let mut whole =
            crate::serve::SessionReadRequest::new(self.today.key.clone(), None, Some(500));
        whole.include_display_facts = true;
        let read = crate::store_read::serve_view(&session, &self.parts, &whole)
            .await
            .expect("the store's session reads");
        let served: usize = read
            .messages
            .iter()
            .map(|message| message.notices.len())
            .sum();
        let records: Vec<Record> = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        let ledger = store_ledger(&records);
        assert_eq!(
            ledger,
            today_ledger(&today_records(&self.today)),
            "the sessions recorded their sends differently"
        );
        let queue: Vec<String> = session
            .operations
            .queue
            .iter()
            .map(|pending| pending.submission_id.clone())
            .collect();
        let today_queue: Vec<String> =
            broca_core::queue::pending_submissions(&today_records(&self.today))
                .into_iter()
                .map(|pending| pending.submission_id.0)
                .collect();
        assert_eq!(
            queue, today_queue,
            "the sessions hold different sends queued"
        );
        assert_eq!(
            store::texts(&session),
            self.today.texts().await,
            "the conversations differ"
        );
        assert_eq!(
            store_starts(&session).await,
            self.today.starts(),
            "the runs' starts differ"
        );
        assert_eq!(
            prompts(self.store.provider.inner.calls()),
            prompts(self.today.executor.provider.inner.calls()),
            "the model calls were sent different messages"
        );
        assert!(self.today.registry.stop_and_seal().await);
        (ledger, queue, (notices, served))
    }
}

fn today_records(today: &Today) -> Vec<WalRecord> {
    use broca_core::WalStore as _;
    today
        .wal
        .read_records(&today.key)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect()
}

/// Every message each model call was sent, as text: a text block, or a tool result's output.
fn prompts(calls: Vec<broca_protocol::request::CallOptions>) -> Vec<Vec<String>> {
    calls
        .iter()
        .map(|call| {
            call.prompt
                .iter()
                .flat_map(|message| &message.content)
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.clone()),
                    ContentBlock::ToolResult(result) => Some(format!(
                        "result of {}: {}",
                        result.tool_call_id.0,
                        serde_json::to_string(&result.output).unwrap()
                    )),
                    _ => None,
                })
                .collect()
        })
        .collect()
}

/// What today's log recorded about its sends, in the shape [`store_ledger`] reads them.
fn today_ledger(records: &[WalRecord]) -> Vec<Value> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::DeliveryOrdered {
                submission_id,
                order,
                sent_by,
            } => Some(json!({"ordered": submission_id, "order": order, "sent_by": sent_by})),
            WalRecord::Queued {
                submission_id,
                sent_by,
                principal,
                delivery,
                ..
            } => Some(json!({
                "queued": submission_id,
                "sent_by": sent_by,
                "principal": principal,
                "delivery": delivery,
            })),
            WalRecord::SendEnded {
                submission_id,
                reason,
            } => Some(json!({"ended": submission_id, "reason": reason})),
            WalRecord::SendDelivered {
                submission_id,
                mode,
                tool_call_id,
                appended_text,
                ..
            } => Some(json!({
                "delivered": submission_id,
                "mode": mode,
                "tool_call_id": tool_call_id,
                "appended_text": appended_text,
            })),
            WalRecord::RunStarted {
                run_id,
                submission_id,
                ..
            } => Some(json!({"run": run_id, "submission_id": submission_id})),
            WalRecord::RunFinished {
                reason,
                interrupt_reason,
                ..
            } => Some(json!({"run_ended": reason, "interrupt_reason": interrupt_reason})),
            _ => None,
        })
        .collect()
}

/// What the store session recorded about its sends.
fn store_ledger(records: &[Record]) -> Vec<Value> {
    let receipt = |receipt: &Option<Value>, name: &str| {
        receipt
            .as_ref()
            .and_then(|receipt| receipt.get(name))
            .cloned()
            .unwrap_or(Value::Null)
    };
    records
        .iter()
        .filter_map(|record| match record {
            Record::DeliveryOrdered(ordered) => Some(json!({
                "ordered": ordered.submission_id,
                "order": ordered.order,
                "sent_by": ordered.sent_by,
            })),
            Record::QueuedSend(send) => Some(json!({
                "queued": send.submission_id,
                "sent_by": receipt(&send.receipt, "sent_by"),
                "principal": receipt(&send.receipt, "principal"),
                "delivery": send.delivery,
            })),
            Record::SendEnded(ended) => {
                Some(json!({"ended": ended.submission_id, "reason": ended.reason}))
            }
            Record::Delivery(delivery) => Some(json!({
                "delivered": delivery.submission_id,
                "mode": delivery.mode,
                "tool_call_id": delivery.tool_call_id,
                "appended_text": delivery.appended_text,
            })),
            Record::RunStart(start) => {
                Some(json!({"run": start.run_id, "submission_id": start.submission_id}))
            }
            Record::RunEnd(end) => Some(json!({
                "run_ended": end.reason,
                "interrupt_reason": end.interrupt_reason,
            })),
            _ => None,
        })
        .collect()
}

fn hello() -> Vec<Vec<NormalizedEvent>> {
    vec![
        store::text_step("a1", "hello"),
        store::text_step("a2", "again"),
        store::text_step("a3", "more"),
    ]
}

/// Each admission refusal of an ordered send, and a send expired on arrival, are answered
/// alike, and none of them is queued.
#[tokio::test]
async fn ordered_admission_refusals_are_answered_alike() {
    let pair = Pair::new(hello(), false).await;
    pair.first("r1").await;
    pair.idle().await;
    let refused = |outcome: &SendOutcome, field: &str| {
        assert!(
            matches!(outcome, SendOutcome::DeliveryRefused { field: refused, .. } if *refused == field),
            "{outcome:?}"
        );
    };
    // A module the plan doesn't name.
    let outcome = pair
        .alike(
            "q1",
            from(module("stranger"), "hi"),
            Some(json!({"modes": ["steer"]})),
            None,
        )
        .await;
    refused(&outcome, "delivery");
    // A named sender without the `idle_start` grant, asking to start a run, as a list and as
    // the string `queue` that stands for one.
    let outcome = pair
        .alike(
            "q2",
            from(module(AFT), "hi"),
            Some(json!({"modes": ["idle_start"]})),
            None,
        )
        .await;
    refused(&outcome, "delivery");
    let outcome = pair.alike("q3", from(module(AFT), "hi"), None, None).await;
    refused(&outcome, "delivery");
    // Templates: one the sender's entry doesn't declare, a stale digest, a slot out of range,
    // and one from the owner, whose plan entry declares none.
    for (index, (request, template)) in [
        templated(module(AFT), &reminder(), None, json!({"n": 3})),
        templated(module(AFT), &reminder(), Some("stale"), json!({"n": 3})),
        templated(module(AFT), &reminder(), None, json!({"n": 300})),
        templated(core(), &reminder(), None, json!({"n": 3})),
    ]
    .into_iter()
    .enumerate()
    {
        let mut template = template;
        if index == 0 {
            template["id"] = json!("unknown");
        }
        let outcome = pair
            .alike(
                &format!("t{index}"),
                request,
                Some(json!({"modes": ["steer"], "template": template})),
                None,
            )
            .await;
        refused(&outcome, "template");
    }
    // A send whose expiry has passed is recorded ended, and answered so.
    let outcome = pair
        .alike(
            "x1",
            from(module(AFT), "too late"),
            Some(json!({"modes": ["tool_result"], "expires_at_ms": 1})),
            None,
        )
        .await;
    assert!(
        matches!(&outcome, SendOutcome::SendEnded { .. }),
        "{outcome:?}"
    );
    // A template naming a part of the session renders a number this storage doesn't look up
    // yet: the store actor refuses it by name, while today's actor answers it.
    let (request, template) = templated(
        module(AFT),
        &pointer(),
        None,
        json!({"part": {"mid": "m-gone", "part": 0}}),
    );
    let (old, new) = pair
        .send(
            "p1",
            request,
            Some(json!({"modes": ["steer"], "template": template})),
            None,
            None,
        )
        .await;
    assert!(!matches!(old, SendOutcome::Error(_)), "{old:?}");
    assert_eq!(
        new,
        SendOutcome::Error(super::super::unserved("a template with a part_tag slot"))
    );
    let (_, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "nothing refused is queued: {queue:?}");
}

/// A script whose second run calls `read` once, then answers.
fn reading_later() -> Vec<Vec<NormalizedEvent>> {
    vec![
        store::text_step("a1", "hello"),
        store::read_step("a2"),
        store::text_step("a3", "done"),
    ]
}

/// A send under a dedupe key supersedes the same sender's undelivered send while a run runs;
/// a list without `idle_start` outlives the run's end, queued; the next run's tool batch
/// carries it. Alike.
#[tokio::test]
async fn a_superseded_send_and_one_outliving_its_run_are_recorded_alike() {
    let pair = Pair::new(reading_later(), true).await;
    pair.first("r1").await;
    let order = json!({"modes": ["tool_result"], "dedupe_key": "status"});
    let outcome = pair
        .alike(
            "q1",
            from(module(AFT), "old status"),
            Some(order.clone()),
            None,
        )
        .await;
    assert!(
        matches!(outcome, SendOutcome::Pending { .. }),
        "{outcome:?}"
    );
    pair.alike("q2", from(module(AFT), "new status"), Some(order), None)
        .await;
    pair.release();
    pair.idle().await;
    let outcome = pair.alike("r2", from(core(), "read a"), None, None).await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    pair.release();
    pair.release();
    pair.idle().await;
    let (ledger, queue, notices) = pair.assert_alike().await;
    assert!(queue.is_empty(), "the send rode: {queue:?}");
    // The one ride is one `notice_delivered` and one notice on the message carrying it.
    assert_eq!(notices, (1, 1), "the ride's notices");
    // The older send ended superseded, and the newer one waited out the first run to ride on
    // the second run's tool batch.
    let at = |entry: &Value| ledger.iter().position(|recorded| recorded == entry);
    let superseded = at(&json!({
        "ended": "sub-q1",
        "reason": {"kind": "superseded", "by": "sub-q2"},
    }));
    let second_run = at(&json!({"run": "r2", "submission_id": null}));
    let rode = ledger
        .iter()
        .position(|entry| entry["delivered"] == "sub-q2" && entry["mode"] == "tool_result");
    assert!(
        superseded.is_some() && second_run.is_some() && rode > second_run,
        "{ledger:#?}"
    );
}

/// Two senders' notes queued while a run runs both ride on the next run's tool batch: each is
/// one `notice_delivered` and one notice on the results message carrying it, read alike.
#[tokio::test]
async fn two_rides_on_one_batch_are_two_notices_alike() {
    let pair = Pair::new(reading_later(), true).await;
    pair.first("r1").await;
    for (run_id, sender, text) in [("q1", AFT, "aft's note"), ("q2", MC, "mc's note")] {
        let outcome = pair
            .alike(
                run_id,
                from(module(sender), text),
                Some(json!({"modes": ["tool_result"]})),
                None,
            )
            .await;
        assert!(
            matches!(outcome, SendOutcome::Pending { .. }),
            "{outcome:?}"
        );
    }
    pair.release();
    pair.idle().await;
    pair.alike("r2", from(core(), "read a"), None, None).await;
    pair.release();
    pair.release();
    pair.idle().await;
    let (_, queue, notices) = pair.assert_alike().await;
    assert!(queue.is_empty(), "both sends rode: {queue:?}");
    assert_eq!(notices, (2, 2), "the rides' notices");
}

/// A list naming `idle_start` from a sender granted it starts its run on an idle session, and
/// the owner's `[steer, idle_start]` does too. Alike, with each run's start naming its send.
#[tokio::test]
async fn an_idle_start_send_starts_a_run_alike() {
    let pair = Pair::new(hello(), false).await;
    pair.first("r1").await;
    pair.idle().await;
    let outcome = pair
        .alike(
            "q1",
            from(module(MC), "wake up"),
            Some(json!({"modes": ["idle_start"]})),
            None,
        )
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    pair.idle().await;
    let outcome = pair
        .alike(
            "q2",
            from(core(), "go on"),
            Some(json!({"modes": ["steer", "idle_start"]})),
            None,
        )
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    pair.idle().await;
    pair.assert_alike().await;
}

/// A script whose first run calls `read`, then answers.
fn reading() -> Vec<Vec<NormalizedEvent>> {
    vec![store::read_step("a1"), store::text_step("a2", "done")]
}

/// An ordered steer joins the running run at its next step, and a templated
/// `[turn_system, steer]` from a named sender steers on a route without turn-scoped system
/// messages. Alike.
#[tokio::test]
async fn ordered_steers_join_the_running_run_alike() {
    let pair = Pair::new(reading(), true).await;
    pair.first("r1").await;
    let outcome = pair
        .alike(
            "q1",
            from(core(), "also check b"),
            Some(json!({"modes": ["steer"]})),
            None,
        )
        .await;
    assert!(
        matches!(outcome, SendOutcome::Pending { .. }),
        "{outcome:?}"
    );
    let (request, template) = templated(module(AFT), &reminder(), None, json!({"n": 3}));
    pair.alike(
        "q2",
        request,
        Some(json!({"modes": ["turn_system", "steer"], "template": template})),
        None,
    )
    .await;
    pair.release();
    pair.release();
    pair.idle().await;
    let (ledger, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "both sends were delivered: {queue:?}");
    for id in ["sub-q1", "sub-q2"] {
        assert!(
            ledger
                .iter()
                .any(|entry| entry["delivered"] == id && entry["mode"] == "steer"),
            "{id} steered: {ledger:#?}"
        );
    }
}

/// A queued ordered send whose expiry passes while the session is idle stays queued, as today's
/// actor leaves it, until the next run's first boundary ends it. Alike.
#[tokio::test]
async fn an_expired_send_waits_on_an_idle_session_alike() {
    let pair = Pair::new(hello(), true).await;
    pair.first("r1").await;
    let expires = broca_core::run::epoch_millis().unwrap() + 400;
    pair.alike(
        "q1",
        from(module(AFT), "soon stale"),
        Some(json!({"modes": ["tool_result"], "expires_at_ms": expires})),
        None,
    )
    .await;
    pair.release();
    pair.idle().await;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    pair.alike("r2", from(core(), "next"), None, None).await;
    pair.release();
    pair.idle().await;
    let (ledger, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "the expired send ended: {queue:?}");
    // Nothing ended it while the session sat idle: it ended after the next run started.
    let ended = ledger
        .iter()
        .position(|entry| entry["ended"] == "sub-q1" && entry["reason"]["kind"] == "expired");
    let second_run = ledger.iter().position(|entry| entry["run"] == "r2");
    assert!(ended.is_some() && ended > second_run, "{ledger:#?}");
}

/// A keyed ordered send's retry is answered from the queue while it waits; a retry asking for
/// another list, or none, is refused naming `delivery`. Alike.
#[tokio::test]
async fn a_queued_ordered_sends_retry_is_answered_alike() {
    let pair = Pair::new(hello(), true).await;
    pair.first("r1").await;
    let order = json!({"modes": ["tool_result"]});
    let mut replies = Vec::new();
    for retry in [
        Some(order.clone()),
        Some(order.clone()),
        Some(json!({"modes": ["steer"]})),
        None,
    ] {
        replies.push(
            pair.alike("r-k1", from(core(), "note"), retry, Some("k1"))
                .await,
        );
    }
    let pending = SendOutcome::Pending {
        submission_id: broca_protocol::SubmissionId("sub-r-k1".into()),
    };
    let reuse = SendOutcome::SendIdReuse {
        differing: "delivery",
    };
    assert_eq!(replies, [pending.clone(), pending, reuse.clone(), reuse]);
    pair.release();
    pair.idle().await;
    let (_, queue, _) = pair.assert_alike().await;
    assert_eq!(queue, vec!["sub-r-k1".to_owned()], "the keyed send waits");
}

/// A plan-named sender's string `delivery` is admitted as today's actor admits it: `steer` from
/// a sender granted `idle_start` joins the running run as the list `[steer, idle_start]`, and
/// `queue` starts a run on the idle session as `[idle_start]`; a sender without the grant is
/// refused both, and an `interrupt` it isn't granted; a module the plan doesn't name is left to
/// the owner check. Alike, records and reads included.
#[tokio::test]
async fn string_sends_from_plan_named_senders_are_admitted_alike() {
    let mut script = hello();
    script.push(store::text_step("a4", "last"));
    let pair = Pair::new(script, true).await;
    pair.first("r1").await;
    let refused = |outcome: &SendOutcome| {
        assert!(
            matches!(
                outcome,
                SendOutcome::DeliveryRefused {
                    field: "delivery",
                    ..
                }
            ),
            "{outcome:?}"
        );
    };
    let outcome = pair
        .alike_as("s1", from(module(MC), "steered in"), Delivery::Steer)
        .await;
    assert!(
        !matches!(
            outcome,
            SendOutcome::DeliveryRefused { .. } | SendOutcome::Error(_)
        ),
        "{outcome:?}"
    );
    refused(
        &pair
            .alike_as("s2", from(module(AFT), "steered in"), Delivery::Steer)
            .await,
    );
    refused(
        &pair
            .alike_as("s3", from(module(AFT), "queued"), Delivery::Queue)
            .await,
    );
    refused(
        &pair
            .alike_as("s4", from(module(AFT), "stop"), Delivery::Interrupt)
            .await,
    );
    pair.release();
    pair.release();
    pair.idle().await;
    let outcome = pair
        .alike_as("s6", from(module(MC), "wake up"), Delivery::Queue)
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    pair.release();
    pair.idle().await;
    // A module the plan doesn't name is left to the owner check, which on this unscoped session
    // admits its send as today's actor does while no refresh has set the session's senders.
    let outcome = pair
        .alike_as("s7", from(module("stranger"), "hi"), Delivery::Queue)
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    pair.release();
    pair.idle().await;
    let (ledger, _, _) = pair.assert_alike().await;
    // Each admitted string send was recorded as the list it stands for, under its sender.
    for (submission, modes) in [
        ("sub-s1", json!(["steer", "idle_start"])),
        ("sub-s6", json!(["idle_start"])),
    ] {
        assert!(
            ledger.iter().any(|entry| entry["ordered"] == submission
                && entry["order"]["modes"] == modes
                && entry["sent_by"].to_string().contains(MC)),
            "{submission}: {ledger:#?}"
        );
    }
}

/// Each run's start and end in the ledger, in order: `start <run>` and `end <reason>`, with an
/// interrupted end's reason after a colon.
fn run_story(ledger: &[Value]) -> Vec<String> {
    ledger
        .iter()
        .filter_map(|entry| {
            if let Some(run) = entry["run"].as_str() {
                return Some(format!("start {run}"));
            }
            let reason = entry["run_ended"].as_str()?;
            Some(match entry["interrupt_reason"].as_str() {
                Some(why) => format!("end {reason}: {why}"),
                None => format!("end {reason}"),
            })
        })
        .collect()
}

/// An interrupt on an idle session starts its run at once, from the owner and from a sender the
/// plan grants one, alike; the plan sender's run continues under the session's principal.
#[tokio::test]
async fn an_interrupt_on_an_idle_session_starts_its_run_alike() {
    let pair = Pair::new(hello(), false).await;
    pair.first("r1").await;
    pair.idle().await;
    for (run_id, sender) in [("i1", core()), ("i2", module(OCCI))] {
        let outcome = pair
            .alike_as(run_id, from(sender, "do this now"), Delivery::Interrupt)
            .await;
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        pair.idle().await;
    }
    let (ledger, _, _) = pair.assert_alike().await;
    assert_eq!(
        run_story(&ledger),
        [
            "start r1",
            "end completed",
            "start i1",
            "end completed",
            "start i2",
            "end completed"
        ]
    );
}

/// An interrupt during a model step, with a plain send already queued: its message is durable
/// first and answered `pending`, the running run ends `cancelled` without its step's answer, and
/// the interrupt runs next, ahead of the plain send, alike.
#[tokio::test]
async fn an_interrupt_during_a_model_step_cancels_the_run_and_runs_first_alike() {
    let pair = Pair::new(hello(), true).await;
    pair.first("r1").await;
    let plain = pair
        .alike("p1", from(core(), "after that"), None, None)
        .await;
    assert!(matches!(plain, SendOutcome::Pending { .. }), "{plain:?}");
    let interrupt = pair
        .alike_as("i1", from(core(), "stop and do this"), Delivery::Interrupt)
        .await;
    assert!(
        matches!(interrupt, SendOutcome::Pending { .. }),
        "{interrupt:?}"
    );
    pair.release_into("i1").await;
    pair.release_into("p1").await;
    pair.idle().await;
    // r1's step announced an id it never placed; today's storage gives the interrupt's message
    // that position id, the new storage a new one.
    let (ledger, queue, _) = pair.assert_alike_ids(false).await;
    assert!(queue.is_empty(), "{queue:?}");
    assert!(
        ledger.contains(&json!({
            "queued": "sub-i1",
            "sent_by": core(),
            "principal": core(),
            "delivery": "interrupt",
        })),
        "{ledger:#?}"
    );
    assert_eq!(
        run_story(&ledger),
        [
            "start r1",
            "end cancelled",
            "start i1",
            "end completed",
            "start p1",
            "end completed"
        ]
    );
}

/// The planes whose tool calls each wait at a gate once sent: the call's arrival is signalled on
/// the first `Notify`, and it answers once the second is notified.
fn tool_gated() -> (store::Planes, Arc<Notify>, Arc<Notify>) {
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let planes = store::Planes {
        tool_gate: Some((Arc::clone(&reached), Arc::clone(&release))),
        ..Default::default()
    };
    (planes, reached, release)
}

/// A step that calls `read`, and the answer of the run after it.
fn reading_then_done() -> Vec<Vec<NormalizedEvent>> {
    vec![store::read_step("a1"), store::text_step("a2", "done")]
}

/// An interrupt while a tool call is in flight: the call is answered, not abandoned; then the
/// run ends `cancelled` at its boundary, with the result kept, and the interrupt runs next,
/// alike.
#[tokio::test]
async fn an_interrupt_during_a_tool_call_waits_for_it_then_runs_alike() {
    let (today_planes, today_reached, today_release) = tool_gated();
    let (store_planes, store_reached, store_release) = tool_gated();
    let pair = Pair::with_planes(reading_then_done(), false, today_planes, store_planes).await;
    pair.first("r1").await;
    today_reached.notified().await;
    store_reached.notified().await;
    let interrupt = pair
        .alike_as("i1", from(core(), "stop and do this"), Delivery::Interrupt)
        .await;
    assert!(
        matches!(interrupt, SendOutcome::Pending { .. }),
        "{interrupt:?}"
    );
    today_release.notify_one();
    store_release.notify_one();
    pair.idle().await;
    let today_calls = pair.today.executor.provider.inner.calls().len();
    let (ledger, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "{queue:?}");
    assert_eq!(
        run_story(&ledger),
        ["start r1", "end cancelled", "start i1", "end completed"]
    );
    assert_eq!(today_calls, 2, "r1's tool step and i1's answer");
}

/// A process that dies after an interrupt's message is durable and before the run it stopped
/// has ended: the next open ends that run `interrupted` as a crash's orphan, never resuming it,
/// and the interrupt runs first, alike.
#[tokio::test]
async fn a_crash_between_an_interrupt_and_its_run_is_recovered_alike() {
    let (today_planes, today_reached, _today_release) = tool_gated();
    let (store_planes, store_reached, _store_release) = tool_gated();
    let pair = Pair::with_planes(reading_then_done(), false, today_planes, store_planes).await;
    pair.first("r1").await;
    today_reached.notified().await;
    store_reached.notified().await;
    let interrupt = pair
        .alike_as("i1", from(core(), "stop and do this"), Delivery::Interrupt)
        .await;
    assert!(
        matches!(interrupt, SendOutcome::Pending { .. }),
        "{interrupt:?}"
    );
    // The tool call never answers: the processes die with the run still waiting on it.
    let pair = pair.restarted(vec![store::text_step("a2", "done")]).await;
    pair.idle().await;
    let (ledger, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "{queue:?}");
    assert_eq!(
        run_story(&ledger),
        [
            "start r1",
            "end interrupted: runner_crash",
            "start i1",
            "end completed"
        ]
    );
}
