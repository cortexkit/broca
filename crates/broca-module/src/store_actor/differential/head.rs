//! `session.head` on a session kept in the session store.
//!
//! Each scenario pins the head as JSON, for an anonymous caller, the session's owner and another
//! module. Only what the storage keeps for itself is taken out: the log identity
//! (`incarnation`), the history version the storage counts for itself, the subscribe cursor
//! (`head`), and wall-clock times, which are kept as whether they're present.

use std::sync::Arc;

use broca_core::FileLeaseStore;
use broca_protocol::{BindIdentity, BindPrincipal, NormalizedEvent, ProviderControl, RunId};
use broca_protocol::{SendScope, SubmissionRequest};
use tokio::sync::Notify;

use super::super::tests::{auth_failure, executor, request, text_step};
use super::{Module, IDLE};
use crate::actor::{SendKey, SendOutcome, SendPlan};
use crate::executor::{RunExecutor, RunKind};
use crate::registry::LineageRegistry;
use crate::serve::ServeHandler;

/// A module as [`super::module`] builds it, with every model step on both sides waiting for
/// `gate` when one is given, so a head can be read while a run runs.
async fn gated_module(steps: Vec<Vec<NormalizedEvent>>, gate: Option<Arc<Notify>>) -> Module {
    gated_module_in(
        steps,
        gate,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
    )
    .await
}

/// [`gated_module`], whose new storage creates sessions under `deployment`.
async fn gated_module_in(
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<Notify>>,
    deployment: broca_session_store::keys::DeploymentKey,
) -> Module {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-head-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let executor: Arc<dyn RunExecutor> = Arc::new(executor(steps, gate));
    let runtime =
        crate::store_open::StoreRuntime::open(&dir, deployment, Arc::clone(&executor), IDLE)
            .await
            .unwrap();
    let registry = LineageRegistry::new(Arc::new(
        crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
            std::sync::Arc::new(crate::store_open::StoreSlot::loaded(Arc::new(runtime))),
        )
        .with_idle_ttl(IDLE),
    ));
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    Module {
        dir,
        registry,
        key,
        caller: Default::default(),
    }
}

/// A module whose sessions start in the session store, over `steps`, and the gate its model steps
/// wait for when `gated`.
pub(super) async fn both(
    steps: Vec<Vec<NormalizedEvent>>,
    gated: bool,
) -> (Module, Option<Arc<Notify>>) {
    both_with_cipher(
        steps,
        gated,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    )
    .await
}

/// [`both`], creating the store session in the selected cipher mode.
pub(super) async fn both_with_cipher(
    steps: Vec<Vec<NormalizedEvent>>,
    gated: bool,
    cipher: broca_session_store::keys::Cipher,
) -> (Module, Option<Arc<Notify>>) {
    let gate = gated.then(|| Arc::new(Notify::new()));
    let deployment = broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher);
    let store = gated_module_in(steps, gate.clone(), deployment).await;
    (store, gate)
}

/// [`both`] without a gate, creating sessions in mode `cipher`.
pub(super) async fn both_in(
    steps: Vec<Vec<NormalizedEvent>>,
    cipher: broca_session_store::keys::Cipher,
) -> Module {
    let deployment = broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher);
    gated_module_in(steps, None, deployment).await
}

/// A send through the registry's own route, with the title and plan the serve layer hands over,
/// as the module's caller sends it.
pub(super) async fn send_titled(
    module: &Module,
    run_id: &str,
    request: SubmissionRequest,
    title: Option<&str>,
    plan: Option<SendPlan>,
) -> SendOutcome {
    module
        .registry
        .route_submission_with_order(
            &module.key,
            RunId(run_id.into()),
            module.caller.shape(run_id, &SendKey::Minted, request),
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            plan,
            None,
            title.map(str::to_owned),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("the session opens")
}

/// The module's head for `principal`, as JSON with what each storage keeps for itself taken out.
/// `mode3` isn't on the wire, so it is added under its own name.
pub(super) async fn head_json(
    module: &Module,
    principal: Option<&BindPrincipal>,
) -> serde_json::Value {
    let head = module
        .registry
        .head_for(&module.key, principal, &SendScope::default())
        .await
        .expect("the head reads");
    let mut value = serde_json::to_value(&head).unwrap();
    if let Some(last) = value
        .get_mut("last_run_state")
        .and_then(serde_json::Value::as_object_mut)
    {
        last.remove("awaits");
    }
    let object = value.as_object_mut().unwrap();
    for member in ["incarnation", "history_version", "head"] {
        object.remove(member);
    }
    if let Some(at) = object.get_mut("updated_at") {
        *at = serde_json::json!(!at.is_null());
    }
    if let Some(cache) = object
        .get_mut("cache")
        .and_then(|cache| cache.as_object_mut())
    {
        for time in ["last_sent_at_ms", "expires_at_ms"] {
            if let Some(at) = cache.get_mut(time) {
                *at = serde_json::json!(!at.is_null());
            }
        }
    }
    object.insert("mode3".into(), serde_json::json!(head.mode3));
    value
}

pub(super) fn core() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

pub(super) fn occipital() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "occipital".into(),
    }
}

/// Pins the session's head for an anonymous caller, core, and a module the session never heard
/// from, and its reads with each opt-in. Returns the head for the anonymous caller, to check
/// what it says.
async fn heads(store: &Module, label: &str) -> serde_json::Value {
    assert!(store.head().exists(), "{label}");
    for principal in [None, Some(core()), Some(occipital())] {
        let head = head_json(store, principal.as_ref()).await;
        broca_test_support::pin!(format!("{label}: head for {principal:?}"), head);
    }
    super::opted_reads(store, label).await;
    head_json(store, None).await
}

/// Lets every model step of `module` through until its runs are over and its actor has closed.
/// A step waits for one permit of `gate`, which holds at most one, so the permit is offered
/// again until the session is closed.
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

/// A provider step that fails for good, ending its run in an error.
fn permanent_failure() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(serde_json::json!({
            "class": "permanent",
            "message": "the request was refused",
            "status": 400
        }))
        .unwrap(),
        retries_used: 0,
    })]
}

/// Two finished runs, the first with a title: the head names the second run completed, the
/// first send's title, the whole transcript's ordinals and the session's usage, alike.
#[tokio::test]
async fn finished_runs_head_alike() {
    let (store, _) = both(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        false,
    )
    .await;
    for module in [&store] {
        let sent = send_titled(module, "r1", request("first"), Some("the title"), None).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        module.closed().await;
        let sent = send_titled(module, "r2", request("second"), None, None).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        module.closed().await;
    }
    let head = heads(&store, "finished").await;
    assert_eq!(
        head["last_run_state"],
        serde_json::json!({"run_id": "r2", "state": "completed", "reason": "completed"})
    );
    assert_eq!(head["title"], "the title");
    assert_eq!(head["last_ordinal"], 4);
    assert_eq!(head["updated_at"], true);
    assert!(head["usage"].is_object(), "{head}");
}

/// A run whose model step failed for good: the head names the run's error and its retry facts,
/// alike.
#[tokio::test]
async fn a_failed_run_heads_alike() {
    let (store, _) = both(vec![permanent_failure()], false).await;
    for module in [&store] {
        send_titled(module, "r1", request("first"), None, None).await;
        module.closed().await;
    }
    let head = heads(&store, "failed").await;
    assert_eq!(head["last_run_state"]["state"], "error");
    assert!(head["last_run_error"].is_object(), "{head}");
}

/// A run cancelled while its model step was in flight: the head names it cancelled, alike.
#[tokio::test]
async fn a_cancelled_run_heads_alike() {
    let (store, store_gate) = both(vec![text_step("a1", "hello")], true).await;
    for module in [&store] {
        let sent = send_titled(module, "r1", request("first"), None, None).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        module
            .registry
            .route_cancel(&module.key, RunId("r1".into()))
            .await
            .expect("the cancel is routed");
    }
    run_out(&store, &store_gate).await;
    let head = heads(&store, "cancelled").await;
    assert_eq!(head["last_run_state"]["state"], "cancelled");
    streams_pinned(&store).await;
}

#[tokio::test]
async fn flow_and_run_routes_have_no_owner_authority_alike() {
    use crate::serve::{ServeError, ServeHandler};
    let (store, store_gate) = both(vec![text_step("a1", "hello")], true).await;
    for module in [&store] {
        let mut sent = request("first");
        sent.principal = Some(core());
        assert!(matches!(
            send_titled(module, "r1", sent, None, None).await,
            SendOutcome::Active { .. }
        ));
    }
    for route in ["flow", "run"] {
        let mut scope = super::head_scope(core());
        let SendScope::Stamped(bind) = &mut scope else {
            unreachable!()
        };
        if route == "flow" {
            bind.scope.flow_id = Some("flow-1".into());
        } else {
            bind.scope.run_id = Some("run-1".into());
        }
        let mut answers = Vec::new();
        for module in [&store] {
            let mut refused = Vec::new();
            for answer in [
                ServeHandler::authorize_session_cancel(
                    &module.registry,
                    &module.key,
                    Some(&core()),
                    &scope,
                    Some(broca_wal::record::RunCancelAt::Now),
                )
                .await,
                ServeHandler::authorize_session_retract(
                    &module.registry,
                    &module.key,
                    Some(&core()),
                    &scope,
                    Some("missing-send"),
                )
                .await,
                ServeHandler::upload(
                    &module.registry,
                    &module.key,
                    Some(&core()),
                    &scope,
                    "session.upload_begin",
                    serde_json::json!({"mime": "image/png", "bytes": 1}),
                )
                .await
                .map(|_| ()),
            ] {
                let Err(ServeError::ScopeRefused(refusal)) = answer else {
                    panic!("{route} owner operation was not refused: {answer:?}");
                };
                assert!(matches!(
                    refusal,
                    broca_core::ScopeRefusal::OwnerMismatch(_)
                ));
                refused.push(format!("{refusal:?}"));
            }
            let head = module
                .registry
                .head_for(&module.key, Some(&core()), &scope)
                .await
                .unwrap();
            assert!(head.sender.is_none(), "{route} route received owner grant");
            answers.push(refused);
        }
        broca_test_support::pin!(format!("{route} refusals"), answers[0]);
    }
    run_out(&store, &store_gate).await;
}

/// Pins the control stream a subscriber from the start of the module's session gets.
async fn streams_pinned(store: &Module) {
    super::streams::pin(&super::streams::routed(&store.registry, &store.key).await);
}

/// A send queued behind a running run: while the run runs, the head names it active and the
/// queued send's title, the newest one sent; once both have run, the second run completed,
/// alike.
#[tokio::test]
async fn a_queued_send_behind_a_running_run_heads_alike() {
    let (store, store_gate) = both(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        true,
    )
    .await;
    for module in [&store] {
        let sent = send_titled(module, "r1", request("first"), Some("running"), None).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        let sent = send_titled(module, "r2", request("second"), Some("queued"), None).await;
        assert!(matches!(sent, SendOutcome::Pending { .. }), "{sent:?}");
    }
    let running = heads(&store, "running with a send queued").await;
    assert_eq!(running["last_run_state"]["state"], "active");
    assert_eq!(running["last_run_state"]["run_id"], "r1");
    assert_eq!(running["title"], "queued");
    run_out(&store, &store_gate).await;
    let ended = heads(&store, "after both runs").await;
    assert_eq!(ended["last_run_state"]["run_id"], "r2");
    assert_eq!(ended["last_run_state"]["state"], "completed");
    streams_pinned(&store).await;
}

/// A run paused for sign-in: the head names the pause and the input profile the run froze,
/// alike.
#[tokio::test]
async fn a_sign_in_pause_heads_alike() {
    let (store, _) = both(vec![auth_failure()], false).await;
    for module in [&store] {
        send_titled(module, "r1", request("first"), None, None).await;
        module.closed().await;
    }
    let head = heads(&store, "paused").await;
    assert_eq!(
        head["last_run_state"],
        serde_json::json!({"run_id": "r1", "state": "paused", "reason": "auth_required"})
    );
}

/// A session whose first send froze a plan: the head serves the plan's model choice in place
/// of the run's, and tells the module the plan names as a sender what it may send, alike.
#[tokio::test]
async fn a_planned_session_heads_alike() {
    let (store, _) = both(vec![text_step("a1", "hello")], false).await;
    let plan = serde_json::json!({
        "model": {"provider": "stub", "model": "m", "variant": "high"},
        "senders": [{"module": "occipital", "idle_start": true}],
    });
    let manifest = broca_wal::PlanManifest {
        rich_ui: Some(
            serde_json::from_value(
                serde_json::json!({"dialect": "1.0", "catalog_digest": "a".repeat(64)}),
            )
            .unwrap(),
        ),
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(Vec::new()),
        system_text: "you are a worker".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    };
    for module in [&store] {
        let mut sent = request("first");
        sent.tools = None;
        sent.policy_allowed = Default::default();
        sent.principal = Some(core());
        let plan = SendPlan {
            identity: "plan-1".into(),
            manifest: Some(Arc::new(manifest.clone())),
            model_check: None,
        };
        let outcome = send_titled(module, "r1", sent, None, Some(plan)).await;
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        module.closed().await;
    }
    heads(&store, "planned").await;
    let head = head_json(&store, None).await;
    assert_eq!(
        head["model"],
        serde_json::json!({"provider": "stub", "model": "m", "variant": "high"})
    );
    assert_eq!(head["last_run_rich_ui"]["dialect"], "1.0");
    // Reads with Rich UI stamps (pinned in `heads`) serve the run's stamp on its assistant
    // message.
    let read = {
        use crate::serve::ServeHandler;
        store
            .registry
            .read(super::opted_request(
                &store.key,
                (None, None),
                (false, false, true),
            ))
            .await
            .expect("the store's session reads")
    };
    let stamped: Vec<_> = read
        .messages
        .iter()
        .filter_map(|message| message.rich_ui.as_ref())
        .collect();
    assert_eq!(stamped.len(), 1, "{read:?}");
    assert_eq!(serde_json::to_value(stamped[0]).unwrap()["dialect"], "1.0");
    let planned_sender = head_json(&store, Some(&occipital())).await;
    assert!(planned_sender["sender"].is_object(), "{planned_sender}");
}

/// A store session whose latest run is still running with no live actor holding it was left by
/// a process that stopped without ending the run. The first head read recovers it and names
/// the crash, where a live actor's run reads active. Reopening and sealing the session keep
/// the interrupted terminal and its reason unchanged.
#[tokio::test]
async fn a_run_no_live_actor_holds_reads_interrupted() {
    let store = gated_module(vec![text_step("a1", "hello")], None).await;
    let sent = send_titled(&store, "r1", request("first"), None, None).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    store.closed().await;
    // A second run whose start is durable and whose end never was, as a stopped process leaves
    // it.
    let mut session = store.load().await;
    let mut start = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.clone())
        .expect("the first run's start");
    start.run_id = "r2".into();
    start.submission_id = None;
    start.send_id = None;
    session
        .append(
            &[broca_session_store::record::Record::RunStart(Box::new(
                start,
            ))],
            false,
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    let left = head_json(&store, None).await;
    assert_eq!(
        left["last_run_state"],
        serde_json::json!({"run_id": "r2", "state": "interrupted", "reason": "runner_crash"})
    );
    drop(
        store
            .registry
            .actor_for(&store.key)
            .await
            .expect("the session opens"),
    );
    store.closed().await;
    let ended = head_json(&store, None).await;
    assert_eq!(
        ended["last_run_state"],
        serde_json::json!({"run_id": "r2", "state": "interrupted", "reason": "runner_crash"})
    );
    let mut session = store.load().await;
    session.seal(None, 0).await.unwrap();
    session.close().await.unwrap();
    let sealed = head_json(&store, None).await;
    assert_eq!(sealed["last_run_state"], ended["last_run_state"]);
    assert_eq!(sealed["updated_at"], true);
}

/// A store session no record of which carries a time can't say when it last changed, so its
/// head is refused by name rather than served with a made-up time. Only a session converted from
/// a log that recorded no time can be one: every run start the store actor writes is timed.
#[tokio::test]
async fn a_session_with_no_recorded_time_is_refused_by_name() {
    let store = gated_module(Vec::new(), None).await;
    let triple = broca_session_store::head::SessionTriple {
        project_root: store.key.project_root.display().to_string(),
        harness: store.key.harness.clone(),
        session: store.key.session.clone(),
    };
    let mut session = broca_session_store::session::Session::create(
        broca_session_store::backend::LocalBackend::new(
            store.dir.join(broca_wal::SESSION_STORAGE_DIR),
        ),
        broca_session_store::backend::SessionAddr(broca_core::session_addr(&store.key)),
        triple,
        &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    session
        .append(
            &[broca_session_store::record::Record::SetupStub(
                broca_session_store::record::SetupStub {
                    parts: Vec::new(),
                    model: broca_protocol::request::ModelHandle::new("stub", "m"),
                    tool_names: Vec::new(),
                    render_key: None,
                },
            )],
            false,
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    assert!(store.head().exists());
    match store
        .registry
        .head_for(&store.key, None, &SendScope::default())
        .await
    {
        Err(crate::serve::ServeError::Open(
            message,
            Some(broca_protocol::ErrorClass::Permanent),
        )) if message.starts_with("session_time_unknown:") => {}
        other => panic!("expected a session_time_unknown refusal, got {other:?}"),
    }
}
