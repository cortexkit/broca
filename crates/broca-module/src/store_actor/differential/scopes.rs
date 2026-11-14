//! Scope admission through today's actor and the store actor, compared reply by reply: sends
//! under an agent-run scope, sends reopening a scope at a later epoch (plain, behind a running or
//! paused run, and as the owner's ordered send that starts its run at once), and cancels from a
//! module other than the session's owner, which the session's plan may grant `interrupt`.

use std::sync::Arc;

use broca_core::scope_admission::{OwnerMismatch, RunMismatchField};
use broca_core::ScopeRefusal;
use broca_protocol::{
    BindPrincipal, BindScope, Delivery, DeliveryOrder, NormalizedEvent, RunId, SendScope,
    SessionScope, SubmissionRequest,
};
use broca_wal::record::RunCancelAt;
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::super::tests::{auth_failure, request, text_step};
use super::head::{both, head_json, occipital, send_titled};
use super::{compare, interrupting, keyed, send, steered, tweaked, Module, Step};
use crate::actor::{CancelOutcome, ScopeFreezeBusyReason, SendKey, SendOutcome, SendPlan};
use crate::executor::RunKind;
use crate::serve::ServeHandler;

/// `n` model steps, each answering with text.
fn texts(n: usize) -> Vec<Vec<NormalizedEvent>> {
    (0..n)
        .map(|step| text_step(&format!("a{step}"), "hello"))
        .collect()
}

/// The module that carries agent runs for their owner.
fn basal() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: broca_core::scope_admission::FLOW_ONLY_MODULE.into(),
    }
}

/// The scope core opens for one run of its agent `agent-1`, which Basal carries.
fn run_scope() -> SessionScope {
    SessionScope {
        owner: super::core(),
        agent_id_frozen: false,
        scope_ref: "parent-run".into(),
        scope_epoch: 1,
        flow_id: None,
        sender: Some(basal()),
        agent_id: Some("agent-1".into()),
        run_id: Some("parent-run".into()),
    }
}

fn stamped(scope: SessionScope) -> SendScope {
    SendScope::Stamped(BindScope {
        scope,
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    })
}

/// The stamp `request` was sent under.
fn stamp_of(request: &mut SubmissionRequest) -> &mut SessionScope {
    stamp_of_scope(&mut request.scope)
}

/// Basal, carrying core's agent run. An agent run is model-only, so the send names no tools.
fn carried(request: &mut SubmissionRequest) {
    request.principal = Some(basal());
    request.scope = stamped(run_scope());
    request.tools = None;
}

/// [`carried`], declaring the default request's tools anyway.
fn carried_with_tools(request: &mut SubmissionRequest) {
    carried(request);
    request.tools = super::super::tests::request("").tools;
}

/// [`carried`], under another run of the same agent.
fn carried_into_another_run(request: &mut SubmissionRequest) {
    carried(request);
    stamp_of(request).run_id = Some("other-run".into());
}

/// [`carried`], for another agent.
fn carried_for_another_agent(request: &mut SubmissionRequest) {
    carried(request);
    stamp_of(request).agent_id = Some("agent-2".into());
}

/// The agent run's owner, sending over the run's route itself.
fn driven_by_its_owner(request: &mut SubmissionRequest) {
    carried(request);
    request.principal = Some(super::core());
}

/// Basal, over a route without a stamp.
fn carried_unstamped(request: &mut SubmissionRequest) {
    carried(request);
    request.scope = SendScope::Unstamped;
}

/// An agent run's first send freezes its scope, sender included, and later sends under it run,
/// alike. A send declaring tools is refused naming them, and one under another run, for another
/// agent, from the run's owner or without a stamp is refused by the session's history, alike.
#[tokio::test]
async fn agent_run_sends_are_admitted_alike() {
    let answers = compare(
        texts(2),
        false,
        vec![
            tweaked(send("r1", "first"), carried),
            Step::Idle,
            tweaked(send("r2", "second"), carried),
            Step::Idle,
            tweaked(send("r3", "tools"), carried_with_tools),
            tweaked(send("r4", "another run"), carried_into_another_run),
            tweaked(send("r5", "another agent"), carried_for_another_agent),
            tweaked(send("r6", "the owner"), driven_by_its_owner),
            tweaked(send("r7", "unstamped"), carried_unstamped),
        ],
    )
    .await;
    assert!(
        matches!(answers[0], SendOutcome::Active { .. }),
        "{answers:?}"
    );
    assert!(
        matches!(answers[1], SendOutcome::Active { .. }),
        "{answers:?}"
    );
    assert_eq!(
        answers[2],
        SendOutcome::Error("agent-run sends cannot carry tools".into())
    );
    let mismatch = |field| SendOutcome::ScopeRefused(ScopeRefusal::RunMismatch { field });
    assert_eq!(answers[3], mismatch(RunMismatchField::RunId));
    assert_eq!(answers[4], mismatch(RunMismatchField::AgentId));
    assert!(
        matches!(answers[5], SendOutcome::ScopeRefused(_)),
        "{answers:?}"
    );
    assert_eq!(answers[6], mismatch(RunMismatchField::RunId));
}

/// Core under its own head scope at `epoch`.
fn at_epoch(request: &mut SubmissionRequest, epoch: u64) {
    super::scoped_by_core(request);
    stamp_of(request).scope_epoch = epoch;
}

fn at_epoch_2(request: &mut SubmissionRequest) {
    at_epoch(request, 2);
}

fn at_epoch_3(request: &mut SubmissionRequest) {
    at_epoch(request, 3);
}

/// The refusal of a send under `presented` on a session that froze core's head scope at
/// `frozen`.
fn scoped_mismatch(frozen: u64, presented: u64) -> SendOutcome {
    let scope = |epoch| {
        let mut request = request("");
        at_epoch(&mut request, epoch);
        stamp_of(&mut request).clone()
    };
    SendOutcome::ScopeRefused(ScopeRefusal::OwnerMismatch(Box::new(
        OwnerMismatch::Scoped {
            frozen: scope(frozen),
            presented_principal: Some(super::core()),
            presented_scope: Some(scope(presented)),
        },
    )))
}

fn busy_with(run_id: &str) -> SendOutcome {
    SendOutcome::ScopeFreezeBusy {
        reason: ScopeFreezeBusyReason::RunActive,
        run_id: Some(RunId(run_id.into())),
    }
}

/// The owner reopens its scope at a later epoch on an idle session: the reopening run records
/// the new epoch, alike. While that run runs, a steer, a queued send, an interrupt and a retry of
/// the run's own send under a still later epoch are each refused, alike: the steer and the retry
/// would join the running run, which belongs to the reopened epoch, and the others would wait in
/// a queue that keeps no scope. A send under the reopened epoch queues and runs under it, and the
/// epoch the reopen replaced is refused afterwards, alike.
#[tokio::test]
async fn reopening_a_scope_at_a_later_epoch_is_admitted_alike() {
    let answers = compare(
        texts(4),
        true,
        vec![
            tweaked(send("r1", "first"), super::scoped_by_core),
            Step::Release,
            Step::Idle,
            tweaked(keyed("k2", "reopened"), at_epoch_2),
            steered(tweaked(send("r3", "steer"), at_epoch_3)),
            tweaked(send("r4", "queued"), at_epoch_3),
            interrupting(tweaked(send("r5", "interrupt"), at_epoch_3)),
            tweaked(keyed("k2", "reopened"), at_epoch_3),
            tweaked(send("r6", "queued at the reopened epoch"), at_epoch_2),
            Step::Release,
            Step::Release,
            Step::Idle,
            tweaked(send("r7", "the replaced epoch"), super::scoped_by_core),
            tweaked(send("r8", "again"), at_epoch_2),
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
    assert!(
        matches!(answers[0], SendOutcome::Active { .. }),
        "{answers:?}"
    );
    assert!(
        matches!(answers[1], SendOutcome::Active { .. }),
        "{answers:?}"
    );
    assert_eq!(answers[2], scoped_mismatch(2, 3));
    assert_eq!(answers[3], busy_with("r-k2"));
    assert_eq!(answers[4], busy_with("r-k2"));
    assert_eq!(answers[5], scoped_mismatch(2, 3));
    assert!(
        matches!(answers[6], SendOutcome::Pending { .. }),
        "{answers:?}"
    );
    assert_eq!(answers[7], scoped_mismatch(2, 1));
    assert!(
        matches!(answers[8], SendOutcome::Active { .. }),
        "{answers:?}"
    );
}

/// A run paused for sign-in holds the session against a reopen: a new send under the later epoch
/// is busy naming the paused run, and a steer or a retry of the paused run's own send under it is
/// refused rather than resuming that run, alike. The retry under the frozen epoch resumes it, and
/// once it has run the reopen is admitted, alike.
#[tokio::test]
async fn a_sign_in_pause_holds_off_a_reopen_alike() {
    let mut script = vec![auth_failure()];
    script.extend(texts(2));
    let answers = compare(
        script,
        false,
        vec![
            tweaked(keyed("k1", "first"), super::scoped_by_core),
            Step::Idle,
            tweaked(send("r2", "reopened"), at_epoch_2),
            steered(tweaked(send("r3", "steer"), at_epoch_2)),
            tweaked(keyed("k1", "first"), at_epoch_2),
            tweaked(keyed("k1", "first"), super::scoped_by_core),
            Step::Idle,
            tweaked(send("r4", "reopened"), at_epoch_2),
            Step::Idle,
        ],
    )
    .await;
    assert!(
        matches!(answers[0], SendOutcome::Active { .. }),
        "{answers:?}"
    );
    assert_eq!(answers[1], busy_with("r-k1"));
    assert_eq!(answers[2], scoped_mismatch(1, 2));
    assert_eq!(answers[3], scoped_mismatch(1, 2));
    assert!(
        matches!(answers[4], SendOutcome::Active { .. }),
        "{answers:?}"
    );
    assert!(
        matches!(answers[5], SendOutcome::Active { .. }),
        "{answers:?}"
    );
}

/// Sends `request` as `run_id` to both modules, with the delivery list `order` when given, as the
/// module's caller sends it. The replies must be equal; returns today's.
async fn sends_alike(
    today: &Module,
    store: &Module,
    run_id: &str,
    request: &SubmissionRequest,
    order: Option<Value>,
) -> SendOutcome {
    let order: Option<DeliveryOrder> = order.map(|order| serde_json::from_value(order).unwrap());
    let mut answers = Vec::new();
    for module in [today, store] {
        let shaped = module
            .caller
            .shape(run_id, &SendKey::Minted, request.clone());
        answers.push(
            module
                .registry
                .route_submission_with_order(
                    &module.key,
                    RunId(run_id.into()),
                    shaped,
                    RunKind::FreshOrResume,
                    SendKey::Minted,
                    false,
                    Delivery::Queue,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                    order.clone(),
                )
                .await
                .expect("the session opens"),
        );
        module.closed().await;
    }
    assert_eq!(answers[1], answers[0], "{run_id}: replies differ");
    answers.swap_remove(0)
}

/// Each run's id and the scope its start recorded, read from today's log.
fn today_run_scopes(module: &Module) -> Vec<Value> {
    use broca_core::WalStore;
    module
        .wal
        .read_records(&module.key)
        .unwrap()
        .into_iter()
        .filter_map(|frame| match frame.record {
            broca_wal::WalRecord::RunStarted { run_id, scope, .. } => {
                Some(json!({"run_id": run_id.0, "scope": scope}))
            }
            _ => None,
        })
        .collect()
}

/// [`today_run_scopes`], read from the store session.
async fn store_run_scopes(module: &Module) -> Vec<Value> {
    use broca_session_store::record::Record;
    module
        .load()
        .await
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::RunStart(start) => Some(json!({"run_id": start.run_id, "scope": start.scope})),
            _ => None,
        })
        .collect()
}

/// The owner's ordered send that starts its run at once records the scope it was admitted
/// under on that run's start, alike: on an unscoped session, the head scope it adopts, and then
/// a later epoch of that scope, which it reopens. Afterwards the replaced epoch is refused,
/// alike.
#[tokio::test]
async fn an_ordered_send_starting_its_run_records_its_scope_alike() {
    let (today, store, _, _) = both(texts(3), false).await;
    let mut first = request("first");
    super::unstamped_by_core(&mut first);
    assert!(matches!(
        sends_alike(&today, &store, "r1", &first, None).await,
        SendOutcome::Active { .. }
    ));
    assert!(store.head().exists() && !today.head().exists());
    let idle_start = || Some(json!({"modes": ["idle_start"]}));
    let mut adopting = request("adopting");
    super::scoped_by_core(&mut adopting);
    assert!(matches!(
        sends_alike(&today, &store, "r2", &adopting, idle_start()).await,
        SendOutcome::Active { .. }
    ));
    let mut reopening = request("reopening");
    at_epoch_2(&mut reopening);
    assert!(matches!(
        sends_alike(&today, &store, "r3", &reopening, idle_start()).await,
        SendOutcome::Active { .. }
    ));
    let mut replaced = request("replaced");
    super::scoped_by_core(&mut replaced);
    assert_eq!(
        sends_alike(&today, &store, "r4", &replaced, None).await,
        scoped_mismatch(2, 1)
    );
    let scopes = today_run_scopes(&today);
    assert_eq!(store_run_scopes(&store).await, scopes);
    let epochs: Vec<_> = scopes
        .iter()
        .map(|start| start["scope"]["scope_epoch"].clone())
        .collect();
    assert_eq!(epochs, [Value::Null, json!(1), json!(2)]);
}

/// A plan whose senders grant `occipital` the right to interrupt, giving the session one tool.
fn granting_plan() -> SendPlan {
    let plan = json!({"senders": [{"module": "occipital", "interrupt": true}]});
    let manifest = broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
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
    };
    SendPlan {
        identity: "plan-1".into(),
        manifest: Some(Arc::new(manifest)),
        model_check: None,
    }
}

/// A send of `text` from core, carrying no tools of its own, as a planned session's sends do.
fn from_core(text: &str) -> SubmissionRequest {
    let mut request = request(text);
    request.tools = None;
    request.policy_allowed = Default::default();
    request.principal = Some(super::core());
    request
}

/// Cancels `run_id` on both modules from `principal`, over a route that says `scope`, as the
/// serve layer routes a cancel from a module other than the owner. The replies must be equal;
/// returns today's.
async fn cancels_alike(
    today: &Module,
    store: &Module,
    run_id: &str,
    principal: &BindPrincipal,
    scope: &SendScope,
    at: RunCancelAt,
) -> CancelOutcome {
    let mut answers = Vec::new();
    for module in [today, store] {
        answers.push(
            module
                .registry
                .route_cancel_from(
                    &module.key,
                    RunId(run_id.into()),
                    at,
                    Some(principal.clone()),
                    scope.clone(),
                )
                .await
                .expect("the cancel is routed"),
        );
    }
    assert_eq!(
        answers[1], answers[0],
        "{principal:?} over {scope:?}: replies differ"
    );
    answers.swap_remove(0)
}

/// Lets every model step of `module` through until its runs are over and its actor has closed.
async fn run_out(module: &Module, gate: &Option<Arc<Notify>>) {
    broca_test_support::wait_until("the session's actor closes", || async {
        if let Some(gate) = gate {
            gate.notify_one();
        }
        match module.registry.slot_state(&module.key) {
            None => Ok(()),
            Some(state) => Err(format!("{state:?}")),
        }
    })
    .await
}

/// A cancel from a module other than the session's owner is checked against the session as it
/// stands, alike: a module the plan doesn't name, the owner over a flow- or run-stamped route
/// (which carries no owner authority), and the granted module at a step boundary are each
/// refused, and the granted module's cancel now stops the run; both sessions then stream alike.
/// The owner, over its own route, cancels its next run, and both sessions then head alike.
#[tokio::test]
async fn a_cancel_from_another_module_is_checked_against_the_plan_alike() {
    use broca_session_store::keys::Cipher;
    for cipher in [Cipher::XChaCha20Poly1305, Cipher::None] {
        cancel_from_another_module(cipher).await;
    }
}

async fn cancel_from_another_module(cipher: broca_session_store::keys::Cipher) {
    let (today, store, today_gate, store_gate) =
        super::head::both_with_cipher(texts(4), true, cipher).await;
    for module in [&today, &store] {
        let sent = send_titled(
            module,
            "r1",
            from_core("first"),
            None,
            Some(granting_plan()),
        )
        .await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        in_model_step(module, "r1").await;
    }
    assert!(store.head().exists() && !today.head().exists());
    let stranger = BindPrincipal::Reserved {
        module_id: "stranger".into(),
    };
    let mut flow_route = stamped(run_scope());
    let flow = stamp_of_scope(&mut flow_route);
    flow.run_id = None;
    flow.agent_id = None;
    flow.sender = None;
    flow.flow_id = Some("flow-1".into());
    for (principal, scope, at) in [
        (&stranger, SendScope::Unstamped, RunCancelAt::Now),
        (&super::core(), flow_route, RunCancelAt::Now),
        (&super::core(), stamped(run_scope()), RunCancelAt::Now),
        (
            &occipital(),
            SendScope::Unstamped,
            RunCancelAt::StepBoundary,
        ),
    ] {
        let refused = cancels_alike(&today, &store, "r1", principal, &scope, at).await;
        assert!(
            matches!(
                &refused,
                CancelOutcome::ScopeRefused(ScopeRefusal::OwnerMismatch(mismatch))
                    if matches!(**mismatch, OwnerMismatch::OwnerOperation { .. })
            ),
            "{principal:?} over {scope:?}: {refused:?}"
        );
    }
    assert_eq!(
        cancels_alike(
            &today,
            &store,
            "r1",
            &occipital(),
            &SendScope::Unstamped,
            RunCancelAt::Now
        )
        .await,
        CancelOutcome::Ack
    );
    run_out(&today, &today_gate).await;
    run_out(&store, &store_gate).await;
    super::streams::assert_alike(
        &super::streams::routed(&store.registry, &store.key).await,
        &super::streams::routed(&today.registry, &today.key).await,
    );
    // `run_out` may leave a permit on a gate, which would let the next run's step through at
    // once; take it, so that run is still running when it is cancelled.
    for gate in today_gate.iter().chain(store_gate.iter()) {
        let _ = tokio::time::timeout(std::time::Duration::from_millis(20), gate.notified()).await;
    }
    for module in [&today, &store] {
        let sent = send_titled(module, "r2", from_core("second"), None, None).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        in_model_step(module, "r2").await;
    }
    assert_eq!(
        cancels_alike(
            &today,
            &store,
            "r2",
            &super::core(),
            &SendScope::Unstamped,
            RunCancelAt::Now
        )
        .await,
        CancelOutcome::Ack
    );
    run_out(&today, &today_gate).await;
    run_out(&store, &store_gate).await;
    for principal in [None, Some(super::core()), Some(occipital())] {
        assert_eq!(
            head_json(&store, principal.as_ref()).await,
            head_json(&today, principal.as_ref()).await,
            "heads differ for {principal:?}"
        );
    }
    let head = head_json(&store, None).await;
    assert_eq!(head["last_run_state"]["run_id"], "r2", "{head}");
    assert_eq!(head["last_run_state"]["state"], "cancelled", "{head}");
    super::streams::assert_alike_after_cancel(
        &super::streams::routed(&store.registry, &store.key).await,
        &super::streams::routed(&today.registry, &today.key).await,
    );
}

/// A cancel must meet an in-flight model step, not race the run's admission.
async fn in_model_step(module: &Module, run: &str) {
    broca_test_support::wait_until("the model step to be in flight", || async {
        let status = serde_json::to_value(
            module
                .registry
                .run_status_reply(&module.key, RunId(run.into()))
                .await
                .unwrap(),
        )
        .unwrap();
        if status["step"]["step_id"] == 1 {
            Ok(())
        } else {
            Err(format!("{run}: {status}"))
        }
    })
    .await;
}

/// The scope a stamped route carries.
fn stamp_of_scope(scope: &mut SendScope) -> &mut SessionScope {
    match scope {
        SendScope::Stamped(bind) => &mut bind.scope,
        other => panic!("the route carries no stamp: {other:?}"),
    }
}
