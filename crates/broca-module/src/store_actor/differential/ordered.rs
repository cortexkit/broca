//! Sends with an ordered delivery list, and the string sends a plan's senders make, through
//! the store actor, each reply pinned and what the session recorded about them: each order, each
//! queued send's sender, principal and mode, each ending, each delivery, and each run's start and
//! end. Admission's refusals, a send expired on arrival, a
//! send whose expiry passes while the session is idle or while its run makes its last model call,
//! a dedupe key superseding an undelivered send, an `idle_start` send starting a run, a send that
//! outlives a run's end and rides on a later one, and interrupts (on an idle session, during a
//! model step, during a tool call, and across a crash before the interrupt's run) are each
//! answered and recorded. Each scenario's session then reads back, pinned: `session.read` in every
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
use broca_wal::{OpaqueJson, PlanManifest};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Notify};

use super::super::tests::{self as store, LoopExecutor};
use super::{display_reads, store_starts, streams};
use crate::actor::{LineageCmd, LineageStatus, SendKey, SendOutcome, SendPlan};
use crate::executor::RunKind;
use broca_session_store::keys::Cipher;

cipher_tests!(
    ordered_admission_refusals_are_answered_alike,
    a_superseded_send_and_one_outliving_its_run_are_recorded_alike,
    two_rides_on_one_batch_are_two_notices_alike,
    an_idle_start_send_starts_a_run_alike,
    ordered_steers_join_the_running_run_alike,
    an_expired_send_waits_on_an_idle_session_alike,
    an_expired_send_ends_when_its_run_ends_alike,
    a_queued_ordered_sends_retry_is_answered_alike,
    string_sends_from_plan_named_senders_are_admitted_alike,
    an_interrupt_on_an_idle_session_starts_its_run_alike,
    an_interrupt_during_a_model_step_cancels_the_run_and_runs_first_alike,
    an_interrupt_during_a_tool_call_waits_for_it_then_runs_alike,
    a_crash_between_an_interrupt_and_its_run_is_recovered_alike,
    a_rides_marks_are_kept_alike,
    an_idle_start_sends_marks_are_kept_alike,
    broca_delivers_a_late_notice_for_a_provider_its_plan_does_not_name,
);

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

/// A template whose slot names a part of the session.
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

/// A session in the new storage served by the store actor over a script.
struct Pair {
    commands: store::Commands,
    served: tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
    store: Arc<LoopExecutor>,
    /// The store actor's setup parts, kept so its session's reads can name its runs' tools.
    parts: SharedParts,
    /// Where the session is kept, which a restarted actor loads it from.
    backend: &'static MemoryBackend,
    gate: Option<Arc<Notify>>,
}

impl Pair {
    async fn new(script: Vec<Vec<NormalizedEvent>>, gated: bool, cipher: Cipher) -> Self {
        Self::with_planes(script, gated, Default::default(), cipher).await
    }

    /// [`Self::new`], with the executor offering `planes`.
    async fn with_planes(
        script: Vec<Vec<NormalizedEvent>>,
        gated: bool,
        planes: store::Planes,
        cipher: Cipher,
    ) -> Self {
        let gate = gated.then(|| Arc::new(Notify::new()));
        let mut executor = store::executor(script, gate.clone());
        executor.planes = planes;
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
            commands: store::Commands::over(commands, backend),
            served,
            store: executor,
            parts,
            backend,
            gate,
        }
    }

    /// The process dies where it stands and starts again, finding only its durable records: a
    /// store actor loading the session, whose new executor answers with `script`. The dead
    /// process's runs never write again.
    async fn restarted(self, script: Vec<Vec<NormalizedEvent>>) -> Self {
        self.served.abort();
        let _ = self.served.await;
        let executor = Arc::new(store::executor(script, None));
        let (commands, served) =
            store::reopen(self.backend, Arc::clone(&executor), &self.parts).await;
        Self {
            commands,
            served,
            store: executor,
            parts: self.parts,
            backend: self.backend,
            gate: None,
        }
    }

    /// The session's first send, from its owner, carrying the plan, which starts its run.
    async fn first(&self, run_id: &str) {
        let reply = self
            .send(run_id, from(core(), "first"), None, Some(plan()), None)
            .await;
        assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
        broca_test_support::pin!("the first send's reply", reply);
    }

    /// Sends `request` as `run_id` with the delivery list `order`, or as a plain queued send
    /// without one, under `key` when given. Returns the reply.
    async fn send(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        order: Option<Value>,
        plan: Option<Arc<PlanManifest>>,
        key: Option<&str>,
    ) -> SendOutcome {
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
    ) -> SendOutcome {
        let order: Option<broca_protocol::DeliveryOrder> =
            order.map(|order| serde_json::from_value(order).unwrap());
        let plan = plan.as_ref().map(|manifest| SendPlan {
            identity: manifest.plan_identity.clone(),
            manifest: Some(Arc::clone(manifest)),
            model_check: None,
        });
        let key = match key {
            Some(key) => SendKey::SendId(key.into()),
            None => SendKey::Minted,
        };
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
                key,
                skip_materialization: false,
                delivery,
                plan: plan.map(Box::new),
                order: order.map(Box::new),
                reply,
            })
            .unwrap();
        answer.await.unwrap()
    }

    /// A send with the string `delivery` and no list, its reply pinned; returns it.
    async fn alike_as(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        delivery: Delivery,
    ) -> SendOutcome {
        let reply = self
            .send_as(run_id, request, delivery, None, None, None)
            .await;
        broca_test_support::pin!(format!("the reply to {run_id}"), reply);
        reply
    }

    /// Sends, its reply pinned; returns it.
    async fn alike(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        order: Option<Value>,
        key: Option<&str>,
    ) -> SendOutcome {
        let reply = self.send(run_id, request, order, None, key).await;
        broca_test_support::pin!(format!("the reply to {run_id}"), reply);
        reply
    }

    /// `session.read` of the session as it stands, from a copy of its storage.
    async fn read(
        &self,
        request: crate::serve::SessionReadRequest,
    ) -> broca_wire::SessionReadResponse {
        let backend = self.backend.snapshot();
        let session = Session::load(
            &backend,
            SessionAddr(store::ADDR.into()),
            &store::triple(),
            &store::deployment(),
        )
        .await
        .unwrap();
        crate::store_read::serve_view(&session, &self.parts, &request)
            .await
            .expect("the session reads")
    }

    /// Lets one waiting model call through.
    fn release(&self) {
        if let Some(gate) = &self.gate {
            gate.notify_one();
        }
    }

    async fn idle(&self) {
        store::idle(&self.commands).await;
    }

    /// Waits until the session runs `run_id`, then lets one model call through, so the release
    /// meets that run's call, never an earlier run's.
    async fn release_into(&self, run_id: &str) {
        let running = |status: LineageStatus| matches!(&status, LineageStatus::Active { run_id: running, .. } if running.0 == run_id);
        broca_test_support::wait_until(&format!("the session to run {run_id}"), || async {
            let store = store::status(&self.commands).await;
            if running(store.clone()) {
                Ok(())
            } else {
                Err(format!("{store:?}"))
            }
        })
        .await;
        self.release();
    }

    /// Ends the session's actor and pins what it recorded about its sends: the conversation,
    /// the send ledger, the sends still queued, the runs' starts, and the messages and results
    /// every model call was sent; and how it reads: the control stream from the start, and
    /// `session.read` in every opt-in mode. Returns the ledger, the queue, and how many notices
    /// the session holds: `notice_delivered` events in its stream, and notices its
    /// `session.read` serves.
    async fn assert_alike(self) -> (Vec<Value>, Vec<String>, (usize, usize)) {
        self.assert_alike_ids(true).await
    }

    /// [`Self::assert_alike`]; `same_ids` false pins the stream as
    /// [`streams::pin_after_cancel`] reads it, after a cancel.
    async fn assert_alike_ids(self, same_ids: bool) -> (Vec<Value>, Vec<String>, (usize, usize)) {
        let stream = streams::store(&self.commands).await;
        if same_ids {
            streams::pin(&stream);
        } else {
            streams::pin_after_cancel(&stream);
        }
        let notices = stream
            .iter()
            .filter(|unit| unit["event"]["type"] == "notice_delivered")
            .count();
        drop(self.commands);
        let session = self.served.await.unwrap();
        display_reads(&session, &self.parts).await;
        let key = broca_protocol::BindIdentity::new("/project", "store-actor", "s1");
        let mut whole = crate::serve::SessionReadRequest::new(key, None, Some(500));
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
        broca_test_support::pin!("the sends the session recorded", timeless(&ledger));
        let queue: Vec<String> = session
            .operations
            .queue
            .iter()
            .map(|pending| pending.submission_id.clone())
            .collect();
        broca_test_support::pin!("the sends queued", queue);
        broca_test_support::pin!("the conversation", store::texts(&session).await);
        broca_test_support::pin!("the runs' starts", store_starts(&session).await);
        broca_test_support::pin!(
            "the messages each model call was sent",
            prompts(self.store.provider.inner.calls())
        );
        (ledger, queue, (notices, served))
    }
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
                    ContentBlock::Text { text } => Some(text.to_string()),
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

/// `ledger` with each order's expiry kept as whether it is there: a test sets it from the wall
/// clock.
fn timeless(ledger: &[Value]) -> Vec<Value> {
    ledger
        .iter()
        .map(|entry| {
            let mut entry = entry.clone();
            if let Some(expires) = entry
                .get_mut("order")
                .and_then(|order| order.get_mut("expires_at_ms"))
            {
                *expires = json!(!expires.is_null());
            }
            entry
        })
        .collect()
}

/// What the store session recorded about its sends.
fn store_ledger(records: &[Record]) -> Vec<Value> {
    let receipt = |receipt: &Option<broca_protocol::json::Json>, name: &str| {
        receipt
            .as_ref()
            .and_then(|receipt| receipt.parse().get(name).cloned())
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
async fn ordered_admission_refusals_are_answered_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), false, cipher).await;
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
    // A template naming a part the session's next request doesn't show ends the send by name,
    // naming the slot.
    let (request, template) = templated(
        module(AFT),
        &pointer(),
        None,
        json!({"part": {"mid": "m-gone", "part": "m-gone#0"}}),
    );
    let outcome = pair
        .alike(
            "p1",
            request,
            Some(json!({"modes": ["steer"], "template": template})),
            None,
        )
        .await;
    assert_eq!(
        outcome,
        SendOutcome::TemplatePartGone {
            slot: "part".into()
        }
    );
    let (_, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "nothing refused is queued: {queue:?}");
}

/// Broca's own delivery of a handed-back call's late result, an internal-scope send as the call's
/// provider under the notice's own send id, is admitted though the plan doesn't name that module:
/// on an idle session it waits for the next run, and asking to start one is refused, so it never
/// starts a run. The same send arriving over a route (an unstamped one here), or an internal send
/// without the notice's send id, is still refused.
async fn broca_delivers_a_late_notice_for_a_provider_its_plan_does_not_name(cipher: Cipher) {
    use broca_protocol::SendScope;
    let pair = Pair::new(hello(), false, cipher).await;
    pair.first("r1").await;
    pair.idle().await;
    let notice = |scope: SendScope| {
        let mut request = marked(
            from(module("stranger"), "the call finished"),
            json!({"notice": true}),
        );
        request.scope = scope;
        request
    };
    let key = crate::late_results_puller::notice_send_id("lineage:1");
    let refused = |outcome: &SendOutcome, expected: &str| {
        assert!(
            matches!(outcome, SendOutcome::DeliveryRefused { code, .. } if *code == expected),
            "{outcome:?}"
        );
    };
    let starting = pair
        .alike(
            "n1",
            notice(SendScope::Internal),
            Some(json!({"modes": ["tool_result", "steer", "idle_start"]})),
            Some(&key),
        )
        .await;
    refused(&starting, cortexkit_role_llm_runner::errors::INVALID_PARAMS);
    let waiting = pair
        .alike(
            "n2",
            notice(SendScope::Internal),
            Some(json!({"modes": ["tool_result", "steer"]})),
            Some(&key),
        )
        .await;
    assert!(
        matches!(waiting, SendOutcome::Pending { .. }),
        "{waiting:?}"
    );
    let outside = pair
        .alike(
            "n3",
            notice(SendScope::Unstamped),
            Some(json!({"modes": ["tool_result", "steer"]})),
            Some(&crate::late_results_puller::notice_send_id("lineage:2")),
        )
        .await;
    refused(
        &outside,
        cortexkit_role_llm_runner::errors::SCOPE_OWNER_MISMATCH,
    );
    let unkeyed = pair
        .alike(
            "n4",
            notice(SendScope::Internal),
            Some(json!({"modes": ["tool_result", "steer"]})),
            Some("some-other-send"),
        )
        .await;
    refused(
        &unkeyed,
        cortexkit_role_llm_runner::errors::SCOPE_OWNER_MISMATCH,
    );
    let (_, queue, _) = pair.assert_alike().await;
    assert_eq!(
        queue.len(),
        1,
        "only the internal, notice-keyed send waits: {queue:?}"
    );
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
async fn a_superseded_send_and_one_outliving_its_run_are_recorded_alike(cipher: Cipher) {
    let pair = Pair::new(reading_later(), true, cipher).await;
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
async fn two_rides_on_one_batch_are_two_notices_alike(cipher: Cipher) {
    let pair = Pair::new(reading_later(), true, cipher).await;
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
async fn an_idle_start_send_starts_a_run_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), false, cipher).await;
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
    let outcome = pair
        .alike(
            "q3",
            from(module(MC), "keyed wake up"),
            Some(json!({"modes": ["idle_start"]})),
            Some("idle-key"),
        )
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    pair.idle().await;
    let read = pair
        .read(super::opted_request(
            &broca_protocol::BindIdentity::new("/project", "store-actor", "s1"),
            (Some(1), Some(100)),
            (true, false, false),
        ))
        .await;
    for (text, id) in [
        ("wake up", None),
        ("go on", None),
        ("keyed wake up", Some("idle-key")),
    ] {
        let entry = read
            .messages
            .iter()
            .find(|entry| {
                entry.message.content.iter().any(
                    |part| matches!(part, ContentBlock::Text { text: actual } if actual == text),
                )
            })
            .unwrap();
        assert_eq!(entry.send_id.as_deref(), id, "{text}");
    }
    pair.assert_alike().await;
}

/// A script whose first run calls `read`, then answers.
fn reading() -> Vec<Vec<NormalizedEvent>> {
    vec![store::read_step("a1"), store::text_step("a2", "done")]
}

/// An ordered steer joins the running run at its next step, and a templated
/// `[turn_system, steer]` from a named sender steers on a route without turn-scoped system
/// messages. Alike.
async fn ordered_steers_join_the_running_run_alike(cipher: Cipher) {
    let pair = Pair::new(reading(), true, cipher).await;
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

/// How far ahead of now a test sets an ordered send's expiry: far enough that both sessions admit
/// the send before it passes, near enough to wait out.
const EXPIRY_MS: u64 = 1_000;

/// Waits until the wall clock, which both sessions judge an ordered send's expiry by, is past
/// `expires`.
async fn past(expires: u64) {
    broca_test_support::wait_until("the send's expiry to pass", || async {
        let now = broca_core::clock::epoch_millis().unwrap_or(0);
        if now > expires {
            Ok(())
        } else {
            Err(format!("{} ms to go", expires - now))
        }
    })
    .await;
}

/// Waits until the session's running run `r1` has its first model step in flight. Its step
/// boundary, the point before each model step where a run decides its queued ordered sends, has
/// then passed, and the run reaches no other step boundary before it ends.
async fn in_first_step(pair: &Pair) {
    use crate::store_reads::{StoreReadOutcome, StoreReadReply, StoreReadRequest};
    broca_test_support::wait_until("the run's first model step in flight", || async {
        let (reply, answer) = oneshot::channel();
        pair.commands
            .send(LineageCmd::StoreRead {
                request: Box::new(StoreReadRequest::RunStatusReply(RunId("r1".into()))),
                reply,
            })
            .unwrap();
        let StoreReadOutcome::Served(answer) = answer.await.unwrap() else {
            return Err("the read wasn't answered in memory".into());
        };
        let StoreReadReply::RunStatusReply(Ok(answer)) = *answer else {
            return Err("the run's status was refused".into());
        };
        let new = serde_json::to_value(answer).unwrap();
        if new["step"]["step_id"] == 1 {
            Ok(())
        } else {
            Err(format!("{new}"))
        }
    })
    .await;
}

/// Where the ledger records `sub-q1` ending expired, the first run's end, and the run `r2`
/// starting.
fn expiry_story(ledger: &[Value]) -> (Option<usize>, Option<usize>, Option<usize>) {
    let ended = ledger
        .iter()
        .position(|entry| entry["ended"] == "sub-q1" && entry["reason"]["kind"] == "expired");
    let first_end = ledger
        .iter()
        .position(|entry| entry["run_ended"].is_string());
    let second_run = ledger.iter().position(|entry| entry["run"] == "r2");
    (ended, first_end, second_run)
}

/// A queued ordered send whose expiry passes while the session is idle stays queued until the
/// next run's first step boundary (where a run decides its queued ordered sends, before its first
/// model step) ends it. The send is queued once the session is idle, so no run's end can come
/// after its expiry.
async fn an_expired_send_waits_on_an_idle_session_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), true, cipher).await;
    pair.first("r1").await;
    pair.release();
    pair.idle().await;
    let expires = broca_core::clock::epoch_millis().unwrap() + EXPIRY_MS;
    // Its list names no `idle_start`, so it starts no run on the idle session.
    let queued = pair
        .alike(
            "q1",
            from(module(AFT), "soon stale"),
            Some(json!({"modes": ["tool_result"], "expires_at_ms": expires})),
            None,
        )
        .await;
    assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
    past(expires).await;
    pair.alike("r2", from(core(), "next"), None, None).await;
    pair.release();
    pair.idle().await;
    let (ledger, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "the expired send ended: {queue:?}");
    // Nothing ended it while the session sat idle: it ended after the next run started.
    let (ended, _, second_run) = expiry_story(&ledger);
    assert!(ended.is_some() && ended > second_run, "{ledger:#?}");
}

/// A queued ordered send whose expiry passes while the run it waits on is in its last model call
/// ends when that run ends, before the next run starts. Today's actor ends it at the queue drain
/// that follows the run's end (`expire_ordered_sends`), and the store actor at its own drain
/// (`expire_ordered`): the run makes no further model step, so no step boundary, where a run
/// decides its queued ordered sends, can end it first. Alike.
async fn an_expired_send_ends_when_its_run_ends_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), true, cipher).await;
    pair.first("r1").await;
    in_first_step(&pair).await;
    let expires = broca_core::clock::epoch_millis().unwrap() + EXPIRY_MS;
    let queued = pair
        .alike(
            "q1",
            from(module(AFT), "soon stale"),
            Some(json!({"modes": ["tool_result"], "expires_at_ms": expires})),
            None,
        )
        .await;
    assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
    past(expires).await;
    pair.release();
    pair.idle().await;
    pair.alike("r2", from(core(), "next"), None, None).await;
    pair.release();
    pair.idle().await;
    let (ledger, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "the expired send ended: {queue:?}");
    let (ended, first_end, second_run) = expiry_story(&ledger);
    assert!(
        first_end.is_some() && ended > first_end && second_run > ended,
        "{ledger:#?}"
    );
}

/// A keyed ordered send's retry is answered from the queue while it waits; a retry asking for
/// another list, or none, is refused naming `delivery`. Alike.
async fn a_queued_ordered_sends_retry_is_answered_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), true, cipher).await;
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

/// A plan-named sender's string `delivery`: `steer` from a sender granted `idle_start` joins the
/// running run as the list `[steer, idle_start]`, and `queue` starts a run on the idle session as
/// `[idle_start]`; a sender without the grant is refused both, and an `interrupt` it isn't
/// granted; a module the plan doesn't name is left to the owner check. Records and reads are
/// pinned too.
async fn string_sends_from_plan_named_senders_are_admitted_alike(cipher: Cipher) {
    let mut script = hello();
    script.push(store::text_step("a4", "last"));
    let pair = Pair::new(script, true, cipher).await;
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
    // admits its send while no refresh has set the session's senders.
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
async fn an_interrupt_on_an_idle_session_starts_its_run_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), false, cipher).await;
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
async fn an_interrupt_during_a_model_step_cancels_the_run_and_runs_first_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), true, cipher).await;
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
    // r1's step announced an id it never placed; the interrupt's message gets a new one.
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
async fn an_interrupt_during_a_tool_call_waits_for_it_then_runs_alike(cipher: Cipher) {
    let (store_planes, store_reached, store_release) = tool_gated();
    let pair = Pair::with_planes(reading_then_done(), false, store_planes, cipher).await;
    pair.first("r1").await;
    store_reached.notified().await;
    let interrupt = pair
        .alike_as("i1", from(core(), "stop and do this"), Delivery::Interrupt)
        .await;
    assert!(
        matches!(interrupt, SendOutcome::Pending { .. }),
        "{interrupt:?}"
    );
    store_release.notify_one();
    pair.idle().await;
    let calls = pair.store.provider.inner.calls().len();
    let (ledger, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "{queue:?}");
    assert_eq!(
        run_story(&ledger),
        ["start r1", "end cancelled", "start i1", "end completed"]
    );
    assert_eq!(calls, 2, "r1's tool step and i1's answer");
}

/// A process that dies after an interrupt's message is durable and before the run it stopped
/// has ended: the next open ends that run `interrupted` as a crash's orphan, never resuming it,
/// and the interrupt runs first, alike.
async fn a_crash_between_an_interrupt_and_its_run_is_recovered_alike(cipher: Cipher) {
    let (store_planes, store_reached, _store_release) = tool_gated();
    let pair = Pair::with_planes(reading_then_done(), false, store_planes, cipher).await;
    pair.first("r1").await;
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

/// A send of `text` from the owner under its head scope, as a scoped session's sends come.
fn scoped(text: &str) -> SubmissionRequest {
    let mut request = from(core(), text);
    request.scope = super::head_scope(core());
    request
}

/// `request` carrying the display marks `marks`.
fn marked(mut request: SubmissionRequest, marks: Value) -> SubmissionRequest {
    request.message_marks = marks.as_object().expect("marks are an object").clone();
    request
}

/// The scoped session's first send, from its owner, carrying the plan, which starts its run.
async fn first_scoped(pair: &Pair, run_id: &str) {
    let reply = pair
        .send(run_id, scoped("first"), None, Some(plan()), None)
        .await;
    assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
    broca_test_support::pin!("the first send's reply", reply);
}

/// Sends queued to ride on a tool batch keep their display marks: a plan sender's notice, and
/// the scope's owner speaking for its agent. Both ride on the next run's tool batch, and
/// `session.read` serves each ride's notice, with the agent spoken for, alike.
async fn a_rides_marks_are_kept_alike(cipher: Cipher) {
    let pair = Pair::new(reading_later(), true, cipher).await;
    first_scoped(&pair, "r1").await;
    for (run_id, request) in [
        (
            "q1",
            marked(from(module(AFT), "aft's note"), json!({"notice": true})),
        ),
        (
            "q2",
            marked(
                scoped("core's note"),
                json!({"speaks_for": {"agent_id": "agent-1"}}),
            ),
        ),
    ] {
        let outcome = pair
            .alike(
                run_id,
                request,
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
    pair.alike("r2", scoped("read a"), None, None).await;
    pair.release();
    pair.release();
    pair.idle().await;
    let (_, queue, notices) = pair.assert_alike().await;
    assert!(queue.is_empty(), "both sends rode: {queue:?}");
    assert_eq!(notices, (2, 2), "the rides' notices");
}

/// An `idle_start` send keeps its display marks for the run it starts: a plan sender's notice,
/// and the scope's owner speaking for its agent, alike.
async fn an_idle_start_sends_marks_are_kept_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), false, cipher).await;
    first_scoped(&pair, "r1").await;
    pair.idle().await;
    let outcome = pair
        .alike(
            "q1",
            marked(from(module(MC), "wake up"), json!({"notice": true})),
            Some(json!({"modes": ["idle_start"]})),
            None,
        )
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    pair.idle().await;
    let outcome = pair
        .alike(
            "q2",
            marked(
                scoped("go on"),
                json!({"speaks_for": {"agent_id": "agent-1"}}),
            ),
            Some(json!({"modes": ["steer", "idle_start"]})),
            None,
        )
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    pair.idle().await;
    pair.assert_alike().await;
}

mod frozen_tools;
mod pauses_and_retries;

/// The plan of [`plan`], with a gateway sender the plan marks as carrying the person's own
/// messages (llm-runner `call_yield`).
fn person_plan() -> Arc<PlanManifest> {
    let senders = json!({"senders": [
        {"module": "gateway", "person": true},
        {"module": AFT},
    ]});
    let mut manifest = (*plan()).clone();
    manifest.plan = OpaqueJson::from_json_text(senders.to_string()).unwrap();
    Arc::new(manifest)
}

/// The count of the person's steers the actor hands the running run, which the run's waiting
/// calls hand back on.
fn person_steers(pair: &Pair) -> u64 {
    let steers = pair.store.person_steers.lock().unwrap();
    let count = *steers.as_ref().expect("the run took a count").0.borrow();
    count
}

/// Only the person's own steer asks the running run's waiting calls to hand back: a steer from
/// the sender the plan marks `person`, carrying no notice mark, once it is durable. A steer from
/// another sender, a notice from the person's sender, and a queued send ask nothing.
#[tokio::test]
async fn only_the_persons_own_steer_hands_calls_back() {
    let pair = Pair::new(reading(), true, Cipher::None).await;
    let reply = pair
        .send("r1", from(core(), "first"), None, Some(person_plan()), None)
        .await;
    assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
    assert_eq!(person_steers(&pair), 0);
    let steer = || Some(json!({"modes": ["steer"]}));
    let other = pair
        .send(
            "q1",
            from(module(AFT), "a file changed"),
            steer(),
            None,
            None,
        )
        .await;
    assert!(matches!(other, SendOutcome::Pending { .. }), "{other:?}");
    let notice = pair
        .send(
            "q2",
            marked(from(module("gateway"), "relayed"), json!({"notice": true})),
            steer(),
            None,
            None,
        )
        .await;
    assert!(matches!(notice, SendOutcome::Pending { .. }), "{notice:?}");
    let queued = pair
        .send(
            "q3",
            from(module("gateway"), "later"),
            Some(json!({"modes": ["tool_result"]})),
            None,
            None,
        )
        .await;
    assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
    assert_eq!(
        person_steers(&pair),
        0,
        "none of those is the person's steer"
    );
    let person = pair
        .send(
            "q4",
            from(module("gateway"), "stop and look"),
            steer(),
            None,
            None,
        )
        .await;
    assert!(matches!(person, SendOutcome::Pending { .. }), "{person:?}");
    assert_eq!(person_steers(&pair), 1);
    pair.release();
    pair.release();
    pair.idle().await;
}
