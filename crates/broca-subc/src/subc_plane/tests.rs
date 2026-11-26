pub(super) mod run_transport_tests;
mod aft_harness {
    include!("tests/aft_harness_tests.rs");
}
include!("direct_carrier_tests.rs");
include!("route_close_followup_tests.rs");
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};

use broca_protocol::request::{CallOptions, Tool};
use broca_provider::{
    families::{openai_chat_frozen_config, OpenAiChat},
    wire::WireFamily,
};
use tokio::sync::Notify;

use super::*;

#[cfg(not(feature = "test-attachments"))]
mod release_tool_capture_tests;
#[cfg(feature = "test-attachments")]
mod tool_capture_tests;

#[derive(Clone)]
struct RouteOpenGate {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl RouteOpenGate {
    fn new() -> Self {
        Self {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

#[derive(Clone, Default)]
struct RouteOpenScript {
    delay: Duration,
    error: Option<ScriptedRouteOpenError>,
    gate: Option<RouteOpenGate>,
    /// The route opens but the daemon closes it straight away, so the first call on it
    /// finds it gone before anything is sent and re-opens.
    closed: bool,
}

#[derive(Clone)]
enum ScriptedRouteOpenError {
    /// A daemon Error frame body, decoded the way a live route open decodes it.
    RejectedBody(Vec<u8>),
    Transport(String),
}

impl RouteOpenScript {
    fn rejected(message: impl Into<String>) -> Self {
        let message = message.into();
        let (code, detail) = message.split_once(" — ").unwrap_or((&message, ""));
        Self::rejected_body(&subc_protocol::ErrorBody::new(code, detail))
    }

    fn rejected_body(body: &subc_protocol::ErrorBody) -> Self {
        Self {
            error: Some(ScriptedRouteOpenError::RejectedBody(
                serde_json::to_vec(body).unwrap(),
            )),
            ..Self::default()
        }
    }

    fn transport(message: impl Into<String>) -> Self {
        Self {
            error: Some(ScriptedRouteOpenError::Transport(message.into())),
            ..Self::default()
        }
    }
}

struct RouteOpenActivity<'a>(&'a AtomicUsize);

impl Drop for RouteOpenActivity<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ScriptedToolPlaneConnection {
    catalogs: Mutex<VecDeque<Vec<CatalogEntry>>>,
    catalog_calls: AtomicUsize,
    next_channel: AtomicU16,
    route_open_scripts: Mutex<HashMap<String, VecDeque<RouteOpenScript>>>,
    route_open_calls: AtomicUsize,
    active_route_opens: AtomicUsize,
    max_concurrent_route_opens: AtomicUsize,
    route_open_completions: Mutex<Vec<String>>,
    tool_replies: Mutex<VecDeque<Result<serde_json::Value, ConnError>>>,
    approved_setup_error: Mutex<Option<ConnError>>,
    tool_calls: AtomicUsize,
    /// Every id the plane put on the wire, in dispatch order. RECORDED rather than
    /// ignored so a test can assert the retry reuses the SAME id: broca re-sends only
    /// when the daemon proved the first attempt never arrived, so a module seeing one
    /// id twice means it executed twice, and that is a defect rather than a retry.
    dispatched_ids: Mutex<Vec<Option<String>>>,
    closed_routes: Mutex<Vec<RouteHandle>>,
    close_gate: Mutex<Option<RouteOpenGate>>,
    close_failures: AtomicUsize,
    /// Every route.open attempt, in order: the session it was for and the scope it carried.
    open_attempts: Mutex<Vec<(String, Option<ScopeSelector>)>>,
    /// What `server.describe` advertises; `scopes/v1` unless a test removes it.
    capabilities: Mutex<Vec<String>>,
    /// Routes the daemon has closed, by channel, as the connection learns from a GOODBYE.
    closed: Mutex<std::collections::HashSet<u16>>,
    /// When set, the next tool call's route closes while the call is in flight: the
    /// request counts as sent and its route closes under it.
    close_during_call: Mutex<bool>,
    dispatched_presets: Mutex<Vec<Option<String>>>,
    dispatched_schema_pins: Mutex<Vec<Option<String>>>,
    dispatched_deadlines: Mutex<Vec<Duration>>,
    dispatched_arguments: Mutex<Vec<serde_json::Value>>,
    /// Real per-module availability gates, installed by tests that prove waiting
    /// calls share one probe while the module is down. Modules without an
    /// installed gate open ungated, as before.
    gates: Mutex<HashMap<String, Arc<ModuleGate>>>,
    /// When set, the next tool call announces itself and waits for its release before it
    /// answers, so a test can act while that call is in flight.
    send_gate: Mutex<Option<RouteOpenGate>>,
    /// What [`ToolPlaneConnection::catalog_generation`] reports; `None` (can't tell) unless a
    /// test moves it.
    catalog_generation: Mutex<Option<crate::connection::CatalogGeneration>>,
}

impl ScriptedToolPlaneConnection {
    /// Close `route` from the daemon's side, as its GOODBYE would.
    fn close(&self, route: RouteHandle) {
        self.closed.lock().unwrap().insert(route.channel);
    }

    fn opened_scopes(&self) -> Vec<(String, Option<ScopeSelector>)> {
        self.open_attempts.lock().unwrap().clone()
    }

    fn new(
        catalogs: Vec<Vec<CatalogEntry>>,
        tool_replies: Vec<Result<serde_json::Value, ConnError>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            catalogs: Mutex::new(catalogs.into()),
            catalog_calls: AtomicUsize::new(0),
            next_channel: AtomicU16::new(1),
            route_open_scripts: Mutex::new(HashMap::new()),
            route_open_calls: AtomicUsize::new(0),
            active_route_opens: AtomicUsize::new(0),
            max_concurrent_route_opens: AtomicUsize::new(0),
            route_open_completions: Mutex::new(Vec::new()),
            tool_replies: Mutex::new(tool_replies.into()),
            approved_setup_error: Mutex::new(None),
            tool_calls: AtomicUsize::new(0),
            dispatched_ids: Mutex::new(Vec::new()),
            closed_routes: Mutex::new(Vec::new()),
            close_gate: Mutex::new(None),
            close_failures: AtomicUsize::new(0),
            open_attempts: Mutex::new(Vec::new()),
            capabilities: Mutex::new(vec![CAP_SCOPES_V1.to_owned()]),
            closed: Mutex::new(Default::default()),
            close_during_call: Mutex::new(false),
            dispatched_presets: Mutex::new(Vec::new()),
            dispatched_schema_pins: Mutex::new(Vec::new()),
            dispatched_deadlines: Mutex::new(Vec::new()),
            dispatched_arguments: Mutex::new(Vec::new()),
            gates: Mutex::new(HashMap::new()),
            send_gate: Mutex::new(None),
            catalog_generation: Mutex::new(None),
        })
    }

    fn script_route_open(&self, module_id: &str, delay: Duration, error: Option<&str>) {
        self.route_open_scripts.lock().unwrap().insert(
            module_id.to_string(),
            VecDeque::from([RouteOpenScript {
                delay,
                error: error.map(|message| ScriptedRouteOpenError::Transport(message.to_owned())),
                gate: None,
                closed: false,
            }]),
        );
    }

    fn script_route_open_sequence(&self, module_id: &str, scripts: Vec<RouteOpenScript>) {
        self.route_open_scripts
            .lock()
            .unwrap()
            .insert(module_id.to_string(), scripts.into());
    }

    /// Install a real per-module availability gate (as the live connection returns),
    /// so the test proves waiting calls share one probe while the module is down.
    /// `jitter` paces the probes: zero for exact attempt counts, a fixed small wait
    /// for waits a cancel must cut.
    fn install_real_gate_with_jitter(
        &self,
        module_id: &str,
        jitter: impl Fn(Duration) -> Duration + Send + Sync + 'static,
    ) {
        self.gates.lock().unwrap().insert(
            module_id.to_string(),
            Arc::new(ModuleGate::with_jitter(jitter, CancellationToken::new())),
        );
    }

    /// Install a real gate with no probe jitter, so the attempt count is exact.
    fn install_real_gate(&self, module_id: &str) {
        self.install_real_gate_with_jitter(module_id, |_| Duration::ZERO);
    }

    fn gate_route_open(&self, module_id: &str) -> RouteOpenGate {
        let gate = RouteOpenGate::new();
        self.route_open_scripts.lock().unwrap().insert(
            module_id.to_string(),
            VecDeque::from([RouteOpenScript {
                gate: Some(gate.clone()),
                ..RouteOpenScript::default()
            }]),
        );
        gate
    }

    fn enter_route_open(&self) -> RouteOpenActivity<'_> {
        let active = self.active_route_opens.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_concurrent_route_opens
            .fetch_max(active, Ordering::SeqCst);
        RouteOpenActivity(&self.active_route_opens)
    }
}

#[async_trait]
impl ToolPlaneConnection for ScriptedToolPlaneConnection {
    async fn catalog_list(&self) -> Result<Arc<Vec<CatalogEntry>>, ConnError> {
        self.catalog_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(
            self.catalogs
                .lock()
                .unwrap()
                .pop_front()
                .expect("test must script every catalog.list response"),
        ))
    }

    fn catalog_generation(&self) -> Option<crate::connection::CatalogGeneration> {
        *self.catalog_generation.lock().unwrap()
    }

    async fn route_open(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<&ScopeSelector>,
    ) -> Result<RouteHandle, ConnError> {
        self.route_open_calls.fetch_add(1, Ordering::SeqCst);
        self.open_attempts
            .lock()
            .unwrap()
            .push((identity.session.clone(), scope.cloned()));
        let _activity = self.enter_route_open();
        let script = {
            let mut scripts = self.route_open_scripts.lock().unwrap();
            let scripts = scripts.get_mut(module_id);
            scripts
                .and_then(|scripts| {
                    if scripts.len() > 1 {
                        scripts.pop_front()
                    } else {
                        scripts.front().cloned()
                    }
                })
                .unwrap_or_default()
        };
        if !script.delay.is_zero() {
            tokio::time::sleep(script.delay).await;
        }
        if let Some(gate) = script.gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        self.route_open_completions
            .lock()
            .unwrap()
            .push(module_id.to_string());
        if let Some(error) = script.error {
            return Err(match error {
                ScriptedRouteOpenError::RejectedBody(body) => {
                    ConnError::Rejected(crate::connection::decode_error_body(&body))
                }
                ScriptedRouteOpenError::Transport(message) => ConnError::Transport(message),
            });
        }
        let route = RouteHandle::synthetic(self.next_channel.fetch_add(1, Ordering::SeqCst));
        if script.closed {
            self.close(route);
        }
        Ok(route)
    }

    async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError> {
        let gate = self.close_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        if self
            .close_failures
            .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(ConnError::Transport("injected close failure".into()));
        }
        self.closed_routes.lock().unwrap().push(*route);
        Ok(())
    }

    async fn tool_call(
        &self,
        route: &RouteHandle,
        _name: &str,
        arguments: &serde_json::Value,
        tool_call_id: Option<&str>,
        deadline: Duration,
        preset: Option<&str>,
    ) -> Result<serde_json::Value, ConnError> {
        // A route the connection already saw closed is refused before anything is written,
        // as the real connection does for a handle missing from its live epoch map.
        if self.closed.lock().unwrap().contains(&route.channel) {
            return Err(ConnError::StaleRouteHandle(*route));
        }
        let send_gate = self.send_gate.lock().unwrap().take();
        if let Some(gate) = send_gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        self.dispatched_presets
            .lock()
            .unwrap()
            .push(preset.map(str::to_owned));
        self.tool_calls.fetch_add(1, Ordering::SeqCst);
        self.dispatched_deadlines.lock().unwrap().push(deadline);
        self.dispatched_arguments
            .lock()
            .unwrap()
            .push(arguments.clone());
        self.dispatched_ids
            .lock()
            .unwrap()
            .push(tool_call_id.map(str::to_string));
        if std::mem::take(&mut *self.close_during_call.lock().unwrap()) {
            self.close(*route);
            return Err(ConnError::Transport("route gone (GOODBYE)".into()));
        }
        self.tool_replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("test must script every tool-call response")
    }

    async fn tool_call_staged(
        &self,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: crate::connection::ToolReplyDeadline,
        preset: Option<&str>,
        _call_key: Option<&str>,
        schema_pin: Option<&str>,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        if signals.interrupted() {
            return Err(crate::connection::ApprovedCallError::BeforePush(
                crate::connection::ApprovedSetupError::Interrupted,
            ));
        }
        self.dispatched_schema_pins
            .lock()
            .unwrap()
            .push(schema_pin.map(str::to_owned));
        self.tool_call(
            route,
            &call.tool_name,
            &(call.input).parse(),
            Some(&call.tool_call_id.0),
            deadline.duration,
            preset,
        )
        .await
        .map_err(crate::connection::ApprovedCallError::AfterPush)
    }

    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
        Ok(self.capabilities.lock().unwrap().clone())
    }

    async fn tool_call_approved(
        &self,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: crate::connection::ToolReplyDeadline,
        preset: Option<&str>,
        args: &ApprovedArgs,
        _schema_pin: Option<&str>,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        if let Some(error) = self.approved_setup_error.lock().unwrap().take() {
            return Err(crate::connection::ApprovedCallError::BeforePush(
                error.into(),
            ));
        }
        let arguments = serde_json::from_str(&args.bytes).expect("approved fixture arguments");
        self.tool_call(
            route,
            &call.tool_name,
            &arguments,
            Some(&call.tool_call_id.0),
            deadline.duration,
            preset,
        )
        .await
        .map_err(crate::connection::ApprovedCallError::AfterPush)
    }

    fn route_is_open(&self, route: &RouteHandle) -> bool {
        !self.closed.lock().unwrap().contains(&route.channel)
    }

    fn module_gate(&self, module_id: &str) -> Option<Arc<ModuleGate>> {
        self.gates.lock().unwrap().get(module_id).cloned()
    }
}

#[tokio::test]
async fn approved_route_failure_is_not_sent() {
    let conn = ScriptedToolPlaneConnection::new(vec![], vec![]);
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("approved-unresolved");
    let call = call("edit");
    plane.batch_routes.lock().unwrap().insert(
        SubcToolPlane::batch_route_key(&ctx),
        HashMap::from([(
            call.tool_call_id.clone(),
            ToolRoute::Failure("missing provider".into()),
        )]),
    );
    let result = plane
        .dispatch_approved(&ctx, &call, "{}", "key", &Default::default())
        .await;
    assert!(matches!(result, DispatchResult::NotSent { reason } if reason == "route_unresolved"));
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 0);
    assert!(plane
        .fence
        .lock()
        .unwrap()
        .get(&(SubcToolPlane::session_key(&ctx.bind), call.tool_call_id))
        .is_none());
}

#[tokio::test]
async fn approved_fence_precedes_route_and_input_checks() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![vec![catalog_entry("aft", &["edit"])]],
        vec![successful_tool_reply("original provider output")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("approved-repeat");
    let call = call("edit");
    let DispatchResult::Completed(prior) = plane
        .dispatch_approved(&ctx, &call, "{}", "key", &Default::default())
        .await
    else {
        panic!("first approved dispatch must complete")
    };
    plane.batch_routes.lock().unwrap().insert(
        SubcToolPlane::batch_route_key(&ctx),
        HashMap::from([(
            call.tool_call_id.clone(),
            ToolRoute::Failure("provider disappeared".into()),
        )]),
    );
    let result = plane
        .dispatch_approved(&ctx, &call, "not json", "key", &Default::default())
        .await;
    let DispatchResult::Completed(repeated) = result else {
        panic!("fenced call must return its prior result")
    };
    assert_eq!(
        serde_json::to_value(repeated).unwrap(),
        serde_json::to_value(prior).unwrap()
    );
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn approved_before_push_stale_handle_reopens_and_resends_once() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![
            vec![catalog_entry("aft", &["edit"])],
            vec![catalog_entry("aft", &["edit"])],
        ],
        vec![successful_tool_reply("done")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("approved-stale-before-push");
    let (route, _) = plane
        .ensure_route(&ctx.bind, None, "aft", &ToolOpenGuard::default())
        .await
        .unwrap();
    *conn.approved_setup_error.lock().unwrap() = Some(ConnError::StaleRouteHandle(route));
    let result = plane
        .dispatch_approved(&ctx, &call("edit"), "{}", "call-key", &Default::default())
        .await;
    assert!(matches!(result, DispatchResult::Completed(_)));
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn approved_cancel_or_stop_during_ensure_route_sends_nothing() {
    for cancel in [true, false] {
        let conn = ScriptedToolPlaneConnection::new(
            vec![vec![catalog_entry("aft", &["edit"])]],
            vec![successful_tool_reply("must not send")],
        );
        let gate = conn.gate_route_open("aft");
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let ctx = batch_ctx("approved-open");
        let call = call("edit");
        let token = CancellationToken::new();
        let signals = broca_core::tool_plane::DispatchSignals::new(
            if cancel { Some(&token) } else { None },
            if cancel { None } else { Some(&token) },
        );
        let dispatch = plane.dispatch_approved(&ctx, &call, "{}", "call-key", &signals);
        let interrupt = async {
            gate.entered.notified().await;
            token.cancel();
            gate.release.notify_one();
        };
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(dispatch, interrupt)
        })
        .await;
        gate.release.notify_one();
        let (result, ()) = result.expect("held approved route must finish");
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            0,
            "cancelled approved route-open wait must make zero physical sends"
        );
        assert!(
            matches!(result, DispatchResult::NotSent { ref reason } if reason == "approved_dispatch_stopped_before_send")
        );
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn approved_cancel_or_stop_during_permit_wait_sends_nothing() {
    use futures::FutureExt;
    for cancel in [true, false] {
        let conn = ScriptedToolPlaneConnection::new(
            vec![vec![catalog_entry("aft", &["edit"])]],
            vec![successful_tool_reply("must not send")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let ctx = batch_ctx("approved-permit");
        let call = call("edit");
        let (_, sem) = plane
            .ensure_route(&ctx.bind, None, "aft", &ToolOpenGuard::default())
            .await
            .unwrap();
        let permit = sem.clone().acquire_owned().await.unwrap();
        let token = CancellationToken::new();
        let signals = broca_core::tool_plane::DispatchSignals::new(
            if cancel { Some(&token) } else { None },
            if cancel { None } else { Some(&token) },
        );
        let mut dispatch =
            Box::pin(plane.dispatch_approved(&ctx, &call, "{}", "call-key", &signals));
        assert!(
            dispatch.as_mut().now_or_never().is_none(),
            "cached route is ready and the only wait is flow-control credit"
        );
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
        token.cancel();
        drop(permit);
        let result = tokio::time::timeout(Duration::from_secs(2), dispatch)
            .await
            .unwrap();
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            0,
            "cancelled approved permit wait must make zero physical sends"
        );
        assert!(
            matches!(result, DispatchResult::NotSent { ref reason } if reason == "approved_dispatch_stopped_before_send")
        );
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn uncancelled_approved_dispatch_still_physically_sends_once() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![vec![catalog_entry("aft", &["edit"])]],
        vec![successful_tool_reply("sent")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    assert!(matches!(
        plane
            .dispatch_approved(
                &batch_ctx("approved-live"),
                &call("edit"),
                "{}",
                "key",
                &broca_core::tool_plane::DispatchSignals::default()
            )
            .await,
        DispatchResult::Completed(_)
    ));
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn completed_approved_fence_still_wins_over_later_cancel_signal() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![vec![catalog_entry("aft", &["edit"])]],
        vec![successful_tool_reply("prior")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("approved-fence");
    let call = call("edit");
    assert!(matches!(
        plane
            .dispatch_approved(
                &ctx,
                &call,
                "{}",
                "key",
                &broca_core::tool_plane::DispatchSignals::default()
            )
            .await,
        DispatchResult::Completed(_)
    ));
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        plane
            .dispatch_approved(
                &ctx,
                &call,
                "{}",
                "key",
                &broca_core::tool_plane::DispatchSignals::new(Some(&cancel), None)
            )
            .await,
        DispatchResult::Completed(_)
    ));
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
}

fn batch_ctx(batch_id: &str) -> BatchCtx {
    batch_ctx_for(batch_id, "/tmp/catalog-memo", "session")
}

fn batch_ctx_for(batch_id: &str, project_root: &str, session: &str) -> BatchCtx {
    BatchCtx {
        media_capture: None,
        batch_id: BatchId(batch_id.into()),
        bind: BindIdentity::new(project_root, "test", session),
        run_id: broca_protocol::RunId("r1".into()),
        scope: None,
    }
}

fn successful_tool_reply(text: &str) -> Result<serde_json::Value, ConnError> {
    Ok(serde_json::json!({
        "content": [{ "type": "text", "text": text }],
        "isError": false
    }))
}

async fn wait_for_closed_routes(conn: &ScriptedToolPlaneConnection, count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if conn.closed_routes.lock().unwrap().len() == count {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("best-effort route close should run promptly");
}

fn assert_completed_success(outcome: DispatchResult) {
    assert!(
        matches!(outcome, DispatchResult::Completed(result) if !result.is_error),
        "scripted tool call should complete successfully"
    );
}

#[test]
fn window_mapping() {
    assert_eq!(window_for(&Concurrency::Serial), 1);
    assert_eq!(window_for(&Concurrency::ModuleManaged), 32);
    assert_eq!(window_for(&Concurrency::StatelessParallel), 1024);
}

#[test]
fn warm_bind_backoff_doubles_to_a_cap() {
    let ms: Vec<u64> = (1..=8)
        .map(|a| warm_bind_backoff(a).as_millis() as u64)
        .collect();
    assert_eq!(ms, vec![250, 500, 1000, 2000, 4000, 5000, 5000, 5000]);
    // A pathological attempt count must not overflow the shift.
    assert_eq!(warm_bind_backoff(u32::MAX).as_millis() as u64, 5000);
    // The whole 30s warm window fits within ~10 attempts — a deterministic
    // rejection can no longer produce a route.open storm (the old flat 250ms
    // interval allowed ~120 attempts in the same window).
    let total: u64 = (1..=10)
        .map(|a| warm_bind_backoff(a).as_millis() as u64)
        .sum();
    assert!(
        total >= 30_000,
        "10 backoff intervals span the 30s window: {total}ms"
    );
}

#[tokio::test]
async fn mid_run_module_warming_then_success_retries_route_open() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence(
        "aft",
        vec![
            RouteOpenScript::rejected("module_warming — warming"),
            RouteOpenScript::default(),
        ],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/route-open-retry", "test", "session");
    let lifecycle = CancellationToken::new();
    tokio::time::pause();
    let started = Instant::now();
    let task = tokio::spawn(async move {
        plane
            .ensure_route_for_lifecycle_inner(
                &bind,
                None,
                "aft",
                &lifecycle,
                None,
                &ToolOpenGuard::default(),
                DEFAULT_ROUTE_OPEN_PATIENCE,
            )
            .await
            .map_err(RouteEnsureError::into_plane)
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(100)).await;
    let result = task.await.unwrap();

    assert!(result.is_ok());
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn mid_run_module_warming_forever_stops_at_retry_deadline() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence(
        "aft",
        vec![RouteOpenScript::rejected("module_warming — warming")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/route-open-retry-deadline", "test", "session");
    let lifecycle = CancellationToken::new();
    tokio::time::pause();
    let started = Instant::now();
    let task = tokio::spawn(async move {
        plane
            .ensure_route_for_lifecycle_inner(
                &bind,
                None,
                "aft",
                &lifecycle,
                None,
                &ToolOpenGuard::default(),
                DEFAULT_ROUTE_OPEN_PATIENCE,
            )
            .await
            .map_err(RouteEnsureError::into_plane)
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;
    let result = task.await.unwrap();

    assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
    assert!(conn.route_open_calls.load(Ordering::SeqCst) > 1);
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_secs(2));
    assert!(elapsed < Duration::from_secs(4));
}

#[tokio::test]
async fn warm_bind_respects_its_own_timeout_under_persistent_warming() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence(
        "aft",
        vec![RouteOpenScript::rejected("module_warming — warming")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/warm-short-timeout", "test", "session");
    let lifecycle = CancellationToken::new();

    tokio::time::pause();
    let started = Instant::now();
    let task = tokio::spawn(async move {
        plane
            .warm_bind_for_lifecycle(&bind, None, Duration::from_millis(500), lifecycle)
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(600)).await;
    tokio::task::yield_now().await;

    assert!(task.is_finished(), "warm bind exceeded its caller deadline");
    let result = task.await.unwrap();
    assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
    assert!(conn.route_open_calls.load(Ordering::SeqCst) > 1);
    assert!(started.elapsed() >= Duration::from_millis(500));
    assert!(started.elapsed() <= Duration::from_millis(600));
}

#[tokio::test]
async fn warm_bind_caps_route_open_attempts_per_outer_iteration() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence(
        "aft",
        vec![RouteOpenScript::rejected("module_warming — warming")],
    );
    let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/warm-cap", "test", "session");
    let lifecycle = CancellationToken::new();

    tokio::time::pause();
    let task = {
        let plane = Arc::clone(&plane);
        let bind = bind.clone();
        let lifecycle = lifecycle.clone();
        tokio::spawn(async move {
            plane
                .warm_bind_for_lifecycle(&bind, None, Duration::from_secs(30), lifecycle)
                .await
        })
    };
    tokio::task::yield_now().await;
    for _ in 0..200 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    // MEASURED, both designs, 20s of virtual time against a 30s caller budget:
    //   min(caller, cap)  -> 80 opens    the inner loop returns at the 3s cap, so the outer
    //                                    loop's exponential backoff sleeps between bursts
    //   caller alone      -> 101 opens   one uninterrupted burst at the flat retry interval
    // The bound sits between those, not adjacent to either: at `< 100` the failing design
    // clears by one open, so any drift in either direction makes this pass vacuously. The
    // separation is only ~21 because a window this much longer than the cap lets the capped
    // design accumulate several bursts -- worth knowing before widening the window.
    let opens = conn.route_open_calls.load(Ordering::SeqCst);
    assert!(opens > 1, "persistent warming must enter the retry arm");
    assert!(
        opens < 90,
        "one outer warm iteration must cap route.open attempts, got {opens}"
    );

    lifecycle.cancel();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(task.await.unwrap().is_err());
}

// Paused clock: the elapsed check measures virtual time, so it sees a retry backoff or
// wait exactly and never the host's scheduling delay.
#[tokio::test(start_paused = true)]
async fn mid_run_module_removed_is_not_retried() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence(
        "aft",
        vec![RouteOpenScript::rejected("module_removed — gone")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/route-open-terminal", "test", "session");
    let lifecycle = CancellationToken::new();
    let started = Instant::now();

    let result = plane
        .ensure_route_for_lifecycle_inner(
            &bind,
            None,
            "aft",
            &lifecycle,
            None,
            &ToolOpenGuard::default(),
            DEFAULT_ROUTE_OPEN_PATIENCE,
        )
        .await
        .map_err(RouteEnsureError::into_plane);

    assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() < Duration::from_millis(100));
}

// Paused clock: the elapsed check measures virtual time, so it sees a retry backoff or
// wait exactly and never the host's scheduling delay.
#[tokio::test(start_paused = true)]
async fn mid_run_unknown_route_open_code_is_not_retried() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence(
        "aft",
        vec![RouteOpenScript::rejected("some_future_code — future")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/route-open-unknown", "test", "session");
    let lifecycle = CancellationToken::new();
    let started = Instant::now();

    let result = plane
        .ensure_route_for_lifecycle_inner(
            &bind,
            None,
            "aft",
            &lifecycle,
            None,
            &ToolOpenGuard::default(),
            DEFAULT_ROUTE_OPEN_PATIENCE,
        )
        .await
        .map_err(RouteEnsureError::into_plane);

    assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() < Duration::from_millis(100));
}

// Paused clock: the elapsed check measures virtual time, so it sees a retry backoff or
// wait exactly and never the host's scheduling delay.
#[tokio::test(start_paused = true)]
async fn mid_run_transport_route_open_error_is_not_retried() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence("aft", vec![RouteOpenScript::transport("socket lost")]);
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/route-open-transport", "test", "session");
    let lifecycle = CancellationToken::new();
    let started = Instant::now();

    let result = plane
        .ensure_route_for_lifecycle_inner(
            &bind,
            None,
            "aft",
            &lifecycle,
            None,
            &ToolOpenGuard::default(),
            DEFAULT_ROUTE_OPEN_PATIENCE,
        )
        .await
        .map_err(RouteEnsureError::into_plane);

    assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() < Duration::from_millis(100));
}

// Paused clock: the elapsed check measures virtual time, so it sees a retry backoff or
// wait exactly and never the host's scheduling delay.
#[tokio::test(start_paused = true)]
async fn warm_module_removed_stops_after_one_attempt() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence(
        "aft",
        vec![RouteOpenScript::rejected("module_removed — gone")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/warm-terminal", "test", "session");
    let lifecycle = CancellationToken::new();
    let started = Instant::now();

    let result = plane
        .warm_bind_for_lifecycle(&bind, None, Duration::from_millis(500), lifecycle)
        .await;

    assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() < Duration::from_millis(100));
}

/// The daemon passes a module's refusal through as a structured error body. The warmer
/// must stop on its code at once, as it does for the legacy text form above, rather than
/// retrying until its own timeout.
// Paused clock: the elapsed check measures virtual time, so it sees a retry backoff or
// wait exactly and never the host's scheduling delay.
#[tokio::test(start_paused = true)]
async fn warm_structured_module_refusal_stops_after_one_attempt() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    let body = subc_protocol::ErrorBody::new("module_rejected", "module refused the route")
        .with_detail(serde_json::json!({
            "module_code": "project_origin_unclassifiable",
            "module_id": "aft",
        }));
    conn.script_route_open_sequence("aft", vec![RouteOpenScript::rejected_body(&body)]);
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/warm-structured-terminal", "test", "session");
    let started = Instant::now();

    let result = plane
        .warm_bind_for_lifecycle(
            &bind,
            None,
            Duration::from_millis(500),
            CancellationToken::new(),
        )
        .await;

    assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() < Duration::from_millis(100));
}

/// Companion to the structured stop: a retryable code in the same body form is retried.
/// If the body were not decoded, the whole JSON text would be read as the code, and that
/// unknown code would stop the warmer after one attempt.
#[tokio::test]
async fn warm_structured_retryable_refusal_is_retried() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    let body = subc_protocol::ErrorBody::new("module_warming", "module is starting");
    conn.script_route_open_sequence(
        "aft",
        vec![
            RouteOpenScript::rejected_body(&body),
            RouteOpenScript::default(),
        ],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/warm-structured-retry", "test", "session");

    let result = plane
        .warm_bind_for_lifecycle(
            &bind,
            None,
            Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn warm_transport_route_open_error_still_retries_to_timeout() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    conn.script_route_open_sequence("aft", vec![RouteOpenScript::transport("socket lost")]);
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.windows.lock().unwrap().insert("aft".into(), 1);
    *plane.catalog_memo.lock().unwrap() = Some((None, Arc::new(Vec::new().into())));
    let bind = BindIdentity::new("/tmp/warm-transport", "test", "session");
    let lifecycle = CancellationToken::new();
    let started = Instant::now();

    let result = plane
        .warm_bind_for_lifecycle(&bind, None, Duration::from_millis(300), lifecycle)
        .await;

    assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
    assert!(conn.route_open_calls.load(Ordering::SeqCst) > 1);
    assert!(started.elapsed() >= Duration::from_millis(300));
}

#[tokio::test(start_paused = true)]
async fn tool_route_health_counts_ownership_and_idle_age_without_names() {
    let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
    let plane = SubcToolPlane::with_connection(conn, "aft");
    let live = BindIdentity::new("/project", "alfonso", "live-private-name");
    let orphan = BindIdentity::new("/project", "alfonso", "closed-private-name");
    plane
        .warm_bind(&live, Duration::from_secs(1))
        .await
        .unwrap();
    plane
        .warm_bind(&orphan, Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(7)).await;
    let health = plane.tool_routes_health(std::slice::from_ref(&live));
    assert_eq!(health["open_session_actors"], 1);
    assert_eq!(health["cached_routes"], 2);
    assert_eq!(health["cached_routes_per_provider"]["aft"], 2);
    assert_eq!(health["routes_for_closed_sessions"], 1);
    assert_eq!(health["oldest_route_idle_secs"], 7);
    assert!(!health.to_string().contains("private-name"));
    plane.close_session_routes(&orphan).await;
    assert_eq!(
        plane.tool_routes_health(std::slice::from_ref(&live))["routes_for_closed_sessions"],
        0
    );
    plane.close_session_routes(&live).await;
    assert!(plane.tool_routes_health(&[])["oldest_route_idle_secs"].is_null());
}

#[tokio::test]
async fn late_open_cleanup_preserves_reopened_generation_route() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new(), Vec::new()],
        vec![successful_tool_reply("fresh")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("reopened");
    plane
        .warm_bind(&ctx.bind, Duration::from_secs(1))
        .await
        .unwrap();
    let key = SubcToolPlane::route_key(&ctx.bind, None, "aft");
    let old = plane.routes.lock().unwrap()[&key].handle;
    plane.close_session_routes(&ctx.bind).await;
    plane
        .warm_bind(&ctx.bind, Duration::from_secs(1))
        .await
        .unwrap();
    let fresh = plane.routes.lock().unwrap()[&key].handle;
    assert_ne!(fresh, old);
    // A late old open finishes cleanup after the same session address has reopened.
    assert!(plane.take_route_if_owned(&key, old).is_none());
    assert_eq!(plane.routes.lock().unwrap()[&key].handle, fresh);
    let call = call("echo");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &ToolManifest::from_tools(vec![]),
        )
        .await
        .unwrap();
    assert_completed_success(plane.dispatch(&ctx, &call).await);
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        plane.tool_routes_health(std::slice::from_ref(&ctx.bind))["cached_routes"],
        1
    );
}

#[tokio::test]
async fn cancelled_close_waiter_still_closes_every_provider_route() {
    let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
    let gate = RouteOpenGate::new();
    *conn.close_gate.lock().unwrap() = Some(gate.clone());
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let bind = BindIdentity::new("/project", "alfonso", "timed-out-close");
    plane
        .warm_bind(&bind, Duration::from_secs(1))
        .await
        .unwrap();
    let close = plane.close_session_routes(&bind);
    drop(close);
    gate.entered.notified().await;
    gate.release.notify_one();
    wait_for_closed_routes(&conn, 1).await;
    assert_eq!(plane.tool_routes_health(&[])["cached_routes"], 0);
}

#[tokio::test]
async fn failed_goodbye_retries_without_dispatching_a_call() {
    let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
    conn.close_failures.store(1, Ordering::SeqCst);
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let bind = BindIdentity::new("/project", "alfonso", "retry");
    plane
        .warm_bind(&bind, Duration::from_secs(1))
        .await
        .unwrap();
    plane.close_session_routes(&bind).await;
    assert_eq!(
        plane.tool_routes_health(&[])["goodbye_failures_since_boot"],
        1
    );
    broca_test_support::wait_until("Goodbye retry", || async {
        if conn.closed_routes.lock().unwrap().len() == 1 {
            Ok(())
        } else {
            Err(plane.tool_routes_health(&[]).to_string())
        }
    })
    .await;
    assert_eq!(plane.tool_routes_health(&[])["pending_goodbyes"], 0);
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn captured_lifecycle_rejects_a_delayed_first_poll_after_close() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
    let bind = BindIdentity::new("/tmp/delayed-warm", "test", "session");
    let lifecycle = plane.capture_session_lifecycle_token(&bind);
    let release_first_poll = Arc::new(Notify::new());
    let warm = {
        let plane = Arc::clone(&plane);
        let bind = bind.clone();
        let release_first_poll = Arc::clone(&release_first_poll);
        tokio::spawn(async move {
            release_first_poll.notified().await;
            plane
                .warm_bind_for_lifecycle(&bind, None, Duration::from_secs(30), lifecycle)
                .await
        })
    };

    plane.close_session_routes(&bind).await;
    release_first_poll.notify_one();

    let error = warm.await.unwrap().unwrap_err();
    assert!(matches!(error, ToolPlaneError::Unavailable(_)));
    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(plane.cached_route_channel_for_module(&bind, "aft"), None);
    assert!(conn.closed_routes.lock().unwrap().is_empty());
    assert_eq!(
        plane.tool_routes_health(&[])["routes_for_closed_sessions"],
        0
    );
}

#[tokio::test]
async fn captured_lifecycle_closes_a_route_opened_during_close() {
    let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
    let gate = conn.gate_route_open("aft");
    let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
    let bind = BindIdentity::new("/tmp/inflight-warm", "test", "session");
    let lifecycle = plane.capture_session_lifecycle_token(&bind);
    let warm = {
        let plane = Arc::clone(&plane);
        let bind = bind.clone();
        tokio::spawn(async move {
            plane
                .warm_bind_for_lifecycle(&bind, None, Duration::from_secs(30), lifecycle)
                .await
        })
    };

    gate.entered.notified().await;
    plane.close_session_routes(&bind).await;
    gate.release.notify_one();

    let error = warm.await.unwrap().unwrap_err();
    assert!(matches!(error, ToolPlaneError::Unavailable(_)));
    assert_eq!(plane.cached_route_channel_for_module(&bind, "aft"), None);
    assert_eq!(conn.closed_routes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn reopened_session_warms_with_a_fresh_lifecycle() {
    let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let bind = BindIdentity::new("/tmp/reopened-warm", "test", "session");
    let closed_lifecycle = plane.capture_session_lifecycle_token(&bind);

    plane.close_session_routes(&bind).await;
    assert!(closed_lifecycle.is_cancelled());

    let reopened_lifecycle = plane.capture_session_lifecycle_token(&bind);
    assert!(!reopened_lifecycle.is_cancelled());
    plane
        .warm_bind_for_lifecycle(&bind, None, Duration::from_secs(30), reopened_lifecycle)
        .await
        .unwrap();

    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
    assert!(plane
        .cached_route_channel_for_module(&bind, "aft")
        .is_some());
    plane.close_session_routes(&bind).await;
    assert_eq!(conn.closed_routes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cold_multi_module_batch_opens_routes_concurrently() {
    let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    let module_ids = ["module-a", "module-b", "module-c"];
    for module_id in module_ids {
        plane.windows.lock().unwrap().insert(module_id.into(), 1);
        conn.script_route_open(module_id, Duration::from_millis(25), None);
    }
    let calls = [call("tool-a"), call("tool-b"), call("tool-c")];
    let tools = [
        request_tool("tool-a", Some("module-a")),
        request_tool("tool-b", Some("module-b")),
        request_tool("tool-c", Some("module-c")),
    ];

    plane
        .batch_started(
            &batch_ctx("batch-overlap"),
            &calls,
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();

    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        conn.max_concurrent_route_opens.load(Ordering::SeqCst),
        3,
        "all three cold route opens must overlap; the old serial loop observed one"
    );
}

#[tokio::test]
async fn concurrent_route_setup_returns_first_btreeset_error() {
    let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    for module_id in ["module-a", "module-b"] {
        plane.windows.lock().unwrap().insert(module_id.into(), 1);
    }
    conn.script_route_open(
        "module-a",
        Duration::from_millis(40),
        Some("module-a failed after module-b"),
    );
    conn.script_route_open(
        "module-b",
        Duration::from_millis(1),
        Some("module-b failed first"),
    );
    let calls = [call("tool-a"), call("tool-b")];
    let tools = [
        request_tool("tool-a", Some("module-a")),
        request_tool("tool-b", Some("module-b")),
    ];

    let error = plane
        .batch_started(
            &batch_ctx("batch-errors"),
            &calls,
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap_err();

    assert_eq!(
        conn.route_open_completions.lock().unwrap().as_slice(),
        &["module-b", "module-a"],
        "module-b must fail by timing before the BTreeSet-first module-a"
    );
    assert_eq!(
        error.to_string(),
        "tool plane unavailable (route bind or transport failed): \
             subc transport: module-a failed after module-b"
    );
}

#[tokio::test]
async fn warm_single_module_batch_reuses_the_cached_route() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new()],
        vec![
            successful_tool_reply("first"),
            successful_tool_reply("second"),
        ],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    plane.windows.lock().unwrap().insert("module-a".into(), 1);
    let tool_call = call("tool-a");
    let tools = [request_tool("tool-a", Some("module-a"))];

    let first = batch_ctx("batch-cold");
    plane
        .batch_started(
            &first,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();
    let route = plane
        .cached_route_channel_for_module(&first.bind, "module-a")
        .expect("the cold batch caches its route");
    assert_completed_success(plane.dispatch(&first, &tool_call).await);
    plane.batch_finished(&first).await.unwrap();

    let second = batch_ctx("batch-warm");
    plane
        .batch_started(
            &second,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();
    assert_completed_success(plane.dispatch(&second, &tool_call).await);
    plane.batch_finished(&second).await.unwrap();

    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(conn.max_concurrent_route_opens.load(Ordering::SeqCst), 1);
    assert_eq!(
        plane.cached_route_channel_for_module(&second.bind, "module-a"),
        Some(route)
    );
}

#[tokio::test]
async fn legacy_dispatch_rendering_goldens() {
    let cases = [
        (
            serde_json::json!({
                "content": [{"type": "text", "text": "12:"}, {"type": "text", "text": "00"}],
                "structuredContent": {"ignored": true}
            }),
            include_str!("../../tests/fixtures/content_v1/legacy_text.json"),
        ),
        (
            serde_json::json!({
                "content": [
                    {"type": "text", "text": "before"},
                    {"type": "image", "mimeType": "image/png", "data": "aGVsbG8=", "width": 1, "height": 1},
                    {"type": "text", "text": "after"}
                ],
                "isError": true,
                "errorCode": "provider_error"
            }),
            include_str!("../../tests/fixtures/content_v1/legacy_image_error.json"),
        ),
        (
            serde_json::json!({"structuredContent": {"program_facing": "still visible on legacy path"}}),
            include_str!("../../tests/fixtures/content_v1/legacy_structured.json"),
        ),
    ];
    // Even a reply shaped like content/v1 stays legacy without route and catalog opt-in.
    for (raw, golden) in cases {
        let conn = ScriptedToolPlaneConnection::new(
            vec![vec![catalog_entry("aft", &["read"])]; 2],
            vec![Ok(raw)],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let ctx = batch_ctx("legacy-render");
        let mut call = call("read");
        call.tool_call_id = ToolCallId("call_01".into());
        let manifest = ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]);
        plane
            .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
            .await
            .unwrap();
        let DispatchResult::Completed(result) = plane.dispatch(&ctx, &call).await else {
            panic!("legacy call did not complete");
        };
        let rendered = crate::content_v1::tests::render_model_request(result);
        assert_eq!(String::from_utf8(rendered).unwrap(), golden.trim_end());
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    }
}

fn uncaptured_result(id: &ToolCallId, raw: &serde_json::Value) -> ToolResult {
    map_legacy_call_tool_result(id, raw)
}

fn legacy_map_call_tool_result(id: &ToolCallId, raw: &serde_json::Value) -> ToolResult {
    let is_error = raw
        .get("isError")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let text = raw
        .get("content")
        .and_then(serde_json::Value::as_array)
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|block| block.get("text").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();
    let output = if is_error {
        ToolOutput::error(text)
    } else {
        ToolOutput::Text { text: text.into() }
    };
    ToolResult {
        tool_call_id: id.clone(),
        output,
        is_error,
    }
}

#[test]
fn image_only_tool_result_maps_to_a_compact_placeholder() {
    let id = ToolCallId("t1".into());
    let raw = serde_json::json!({
        "content": [{
            "type": "image",
            "mimeType": "image/png",
            "data": "aGVsbG8="
        }],
        "isError": false
    });

    assert!(matches!(
        legacy_map_call_tool_result(&id, &raw).output,
        ToolOutput::Text { text } if text.is_empty()
    ));
    let result = uncaptured_result(&id, &raw);
    assert!(!result.is_error);
    assert_eq!(
            result.output,
            ToolOutput::Text {
                text: "[image/png not shown to the model: this runner does not pass tool images to models yet]"
                    .into()
            }
        );
}

#[test]
fn mixed_tool_content_preserves_text_and_appends_media_notice() {
    let raw = serde_json::json!({
        "content": [
            { "type": "text", "text": "before" },
            { "type": "image", "mimeType": "image/jpeg", "data": "abcd" },
            { "type": "text", "text": "after" }
        ],
        "isError": false
    });

    let result = uncaptured_result(&ToolCallId("t1".into()), &raw);
    assert_eq!(result.output, ToolOutput::Text {
            text: "beforeafter\n[image/jpeg not shown to the model: this runner does not pass tool images to models yet]".into()
        });
}

#[test]
fn structured_content_only_maps_to_json() {
    let structured = serde_json::json!({"temperature": 12, "unit": "C"});
    let raw = serde_json::json!({
        "structuredContent": structured.clone(),
        "isError": false
    });

    let result = uncaptured_result(&ToolCallId("t1".into()), &raw);
    assert!(!result.is_error);
    assert_eq!(
        result.output,
        ToolOutput::Json {
            value: structured.into()
        }
    );
}

#[test]
fn text_only_tool_result_is_byte_identical_to_legacy_mapping() {
    let id = ToolCallId("t1".into());
    let raw = serde_json::json!({
        "content": [
            { "type": "text", "text": "12:" },
            { "type": "text", "text": "00" }
        ],
        "structuredContent": {"ignored": true},
        "isError": false
    });

    let legacy = serde_json::to_vec(&legacy_map_call_tool_result(&id, &raw)).unwrap();
    let mapped = serde_json::to_vec(&uncaptured_result(&id, &raw)).unwrap();
    assert_eq!(mapped, legacy);
}

#[test]
fn error_with_non_text_content_keeps_error_and_media_notice() {
    let raw = serde_json::json!({
        "content": [{ "type": "audio", "mimeType": "audio/wav", "data": "123456" }],
        "isError": true
    });

    let result = uncaptured_result(&ToolCallId("t1".into()), &raw);
    assert!(result.is_error);
    assert_eq!(
            result.output,
            ToolOutput::error(
                "[audio/wav not shown to the model: this runner does not pass tool images to models yet]"
            )
        );
}

#[test]
fn reply_without_content_structured_content_or_is_error_is_an_error_quoting_raw_reply() {
    let raw = serde_json::json!({
        "result": { "hits": ["a", "b"] }
    });

    let result = uncaptured_result(&ToolCallId("t1".into()), &raw);
    assert!(result.is_error);
    let ToolOutput::Error { message, code, .. } = result.output else {
        panic!("expected an error output, got {:?}", result.output);
    };
    assert_eq!(code.as_deref(), Some("outcome_unknown"));
    assert!(
        message.contains("none of `content`, `structuredContent` or `isError`"),
        "{message}"
    );
    assert!(
        message
            .ends_with(r#"raw reply: {"result":{"hits":["a","b"]}}. Check before trying again."#),
        "{message}"
    );
}

#[test]
fn empty_content_array_stays_an_empty_success() {
    let raw = serde_json::json!({ "content": [] });

    let result = uncaptured_result(&ToolCallId("t1".into()), &raw);
    assert!(!result.is_error);
    assert_eq!(
        result.output,
        ToolOutput::Text {
            text: String::new().into()
        }
    );
}

#[test]
fn structured_content_alone_without_is_error_still_maps_to_json() {
    let structured = serde_json::json!({"answer": 42});
    let raw = serde_json::json!({ "structuredContent": structured.clone() });

    let result = uncaptured_result(&ToolCallId("t1".into()), &raw);
    assert!(!result.is_error);
    assert_eq!(
        result.output,
        ToolOutput::Json {
            value: structured.into()
        }
    );
}

#[test]
fn malformed_reply_quote_is_truncated_to_the_bound() {
    // Multi-byte characters make the byte bound land mid-character, so this also
    // proves the cut backs off to a char boundary instead of panicking.
    let raw =
        serde_json::json!({ "result": "é".repeat(10 * MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES) });
    let full_len = serde_json::to_string(&raw).unwrap().len();

    let result = uncaptured_result(&ToolCallId("t1".into()), &raw);
    assert!(result.is_error);
    let ToolOutput::Error { message, .. } = result.output else {
        panic!("expected an error output");
    };
    let quoted = message.split_once("raw reply: ").unwrap().1;
    let (body, marker) = quoted.split_once('…').unwrap();
    assert!(body.len() <= MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES);
    assert!(body.len() > MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES - 4);
    assert!(body.starts_with(r#"{"result":"éé"#));
    assert_eq!(
        marker,
        format!(
            " [truncated: {} of {full_len} bytes shown]. Check before trying again.",
            body.len()
        )
    );
}

#[test]
fn maps_error_call_tool_result() {
    let raw = serde_json::json!({
        "content": [{ "type": "text", "text": "boom" }],
        "isError": true
    });
    let r = uncaptured_result(&ToolCallId("t1".into()), &raw);
    assert!(r.is_error);
    assert!(matches!(r.output, ToolOutput::Error { message, .. } if message == "boom"));
}

fn result(text: &str) -> ToolResult {
    ToolResult {
        tool_call_id: ToolCallId("ignored".into()),
        output: ToolOutput::Text { text: text.into() },
        is_error: false,
    }
}

fn session_key(project_root: &str, harness: &str, session: &str) -> SessionKey {
    SessionKey {
        project_root: PathBuf::from(project_root),
        harness: harness.into(),
        session: session.into(),
    }
}

fn route_key(project_root: &str, harness: &str, session: &str, module_id: &str) -> RouteKey {
    RouteKey {
        session: session_key(project_root, harness, session),
        scope: None,
        module_id: module_id.into(),
        shape: RouteShape::default(),
        held_reply_v1: false,
    }
}

#[tokio::test]
async fn dispatch_uses_the_routed_providers_frozen_preset() {
    let conn = ScriptedToolPlaneConnection::new(
        (0..3)
            .map(|_| {
                vec![
                    catalog_entry("aft", &["read"]),
                    catalog_entry("other", &["look"]),
                ]
            })
            .collect(),
        (0..3)
            .map(|_| Ok(serde_json::json!({"content": []})))
            .collect(),
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let mut frozen = broca_records::record::PlanManifest {
        rich_ui: None,
        declarations: Vec::new(),
        plan: broca_records::record::OpaqueJson::from_json_text(
            serde_json::json!({"tool_items": [
                {"provider": "other", "preset": "head"}, {"provider": "aft", "preset": "worker"}
            ]})
            .to_string(),
        )
        .unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        role_versions: Default::default(),
        fetches: Vec::new(),
        tools: Arc::new(Vec::new()),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
    };
    let tools = vec![
        request_tool("read", Some("aft")),
        request_tool("look", Some("other")),
    ];
    let manifest = ToolManifest::from_tools(tools.clone())
        .with_plan_manifest(&frozen)
        .unwrap();
    // Mutating a later plan value cannot change the already constructed manifest.
    frozen.plan =
        broca_records::record::OpaqueJson::from_json_text(r#"{"tool_items": []}"#).unwrap();
    let ctx = batch_ctx_for("preset", "project", "planned");
    for name in ["read", "look"] {
        let call = call(name);
        plane
            .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
            .await
            .unwrap();
        assert!(matches!(
            plane.dispatch(&ctx, &call).await,
            DispatchResult::Completed(_)
        ));
        plane.batch_finished(&ctx).await.unwrap();
    }
    let call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &ToolManifest::from_tools(tools),
        )
        .await
        .unwrap();
    plane.dispatch(&ctx, &call).await;
    plane.batch_finished(&ctx).await.unwrap();
    assert_eq!(
        *conn.dispatched_presets.lock().unwrap(),
        [Some("worker".into()), Some("head".into()), None]
    );
    assert!(plane.batch_manifests.lock().unwrap().is_empty());
}

/// A manifest whose frozen plan gives `provider` a preset, a reply deadline and a schema pin
/// for `tool`, which it declares as served by `provider`.
fn pinned_plan(provider: &str, tool: &str) -> ToolManifest {
    let plan = broca_records::record::PlanManifest {
        rich_ui: None,
        declarations: Vec::new(),
        plan: broca_records::record::OpaqueJson::from_json_text(
            serde_json::json!({"tool_items": [{"provider": provider, "preset": "frozen-preset"}]})
                .to_string(),
        )
        .unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        role_versions: Default::default(),
        fetches: vec![broca_records::record::ProviderFetch {
            provider: provider.into(),
            outcome: broca_records::record::FetchOutcome::Answered {
                catalog_digest: "digest".into(),
                tools: Vec::new().into(),
                text: None,
                result_ops: None,
                reply: Some(BTreeMap::from([(
                    tool.to_owned(),
                    broca_records::record::FrozenReply {
                        max_ms: 600_000,
                        hold_argument: None,
                    },
                )])),
                declarations_frozen: Some(Box::new(
                    broca_records::record::FrozenProviderDeclarations {
                        schema_pins: Some(BTreeMap::from([(tool.to_owned(), frozen_pin())])),
                        ..Default::default()
                    },
                )),
                reference_slots: None,
            },
        }],
        tools: Arc::new(Vec::new()),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
    };
    ToolManifest::from_tools(vec![request_tool(tool, Some(provider))])
        .with_plan_manifest(&plan)
        .unwrap()
}

fn frozen_pin() -> broca_records::record::DispatchSchemaPin {
    broca_records::record::DispatchSchemaPin {
        schema_digest: "634bdb1fe768fce0974f9c89e6f15f889470a8288277c6433e4711ab6a5b9335".into(),
        semantics: 7,
        major: None,
    }
}

/// A plan frozen before a module changed its id names the old id for the module's tools, preset,
/// reply deadline and schema pins, while the catalog now lists only the new id. The call reaches
/// the module under its new id and still carries everything the plan froze for it.
#[tokio::test]
async fn a_call_to_a_renamed_provider_carries_what_the_plan_froze_under_its_old_id() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![vec![catalog_entry("prefrontal", &["recall"])]; 3],
        vec![successful_tool_reply("recalled")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("renamed-provider");
    let recall = call("recall");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&recall),
            &pinned_plan("prefrontal-core", "recall"),
        )
        .await
        .unwrap();
    assert_eq!(
        plane.dispatch_target(&ctx, &recall.tool_call_id).as_deref(),
        Some("prefrontal")
    );
    assert_completed_success(plane.dispatch(&ctx, &recall).await);
    assert_eq!(
        *conn.dispatched_presets.lock().unwrap(),
        [Some("frozen-preset".to_owned())]
    );
    assert_eq!(
        *conn.dispatched_schema_pins.lock().unwrap(),
        [broca_core::projection::schema_pin_text(
            "recall",
            &frozen_pin()
        )]
    );
    assert_eq!(
        *conn.dispatched_deadlines.lock().unwrap(),
        [Duration::from_millis(600_000)]
    );
}

/// A call refused before delivery because its module is restarting is re-sent once, and the
/// re-send is the same request: the same id, preset and schema pin, even when the call's module
/// is not the plane's default one.
#[tokio::test]
async fn a_resend_after_module_reloading_carries_the_first_attempts_pin() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![vec![catalog_entry("plexus", &["read"])]; 3],
        vec![
            Err(ConnError::Rejected(crate::connection::decode_error_body(
                br#"{"code":"module_reloading","message":"draining endpoint"}"#,
            ))),
            successful_tool_reply("served after the restart"),
        ],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("resend-pin");
    let read = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&read),
            &pinned_plan("plexus", "read"),
        )
        .await
        .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(5), plane.dispatch(&ctx, &read))
        .await
        .expect("the re-send must not wait on a lifecycle cancel");
    assert_completed_success(outcome);
    let pin = broca_core::projection::schema_pin_text("read", &frozen_pin());
    assert!(pin.is_some());
    assert_eq!(
        *conn.dispatched_schema_pins.lock().unwrap(),
        [pin.clone(), pin]
    );
    assert_eq!(
        *conn.dispatched_presets.lock().unwrap(),
        [
            Some("frozen-preset".to_owned()),
            Some("frozen-preset".to_owned())
        ]
    );
    let ids = conn.dispatched_ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 2, "one refused attempt, one re-send: {ids:?}");
    assert_eq!(ids[0], ids[1]);
}

fn request_tool(name: &str, module: Option<&str>) -> Tool {
    Tool {
        name: name.into(),
        description: String::new(),
        input_schema: serde_json::json!({"type": "object"}).into(),
        module: module.map(ToOwned::to_owned),
        execution_mode: broca_protocol::request::ExecutionMode::Parallel,
        reply: None,
    }
}

fn catalog_entry(module_id: &str, tool_names: &[&str]) -> CatalogEntry {
    // Routing uses module id and tool names, not build versions or capabilities. The fixture
    // represents a ready module, rather than the daemon's draining/reloading state.
    CatalogEntry::new(
        module_id,
        vec![ProviderRole::ToolProvider {
            tools: tool_names
                .iter()
                .map(|name| subc_protocol::manifest::Tool {
                    name: (*name).into(),
                    description: None,
                    execution_mode: subc_protocol::manifest::ExecutionMode::Pure,
                    schema: serde_json::json!({"type": "object"}),
                })
                .collect(),
            identity_scope: Vec::new(),
            concurrency: Concurrency::Serial,
            emits_push: false,
            sub_supervises: true,
        }],
        Vec::new(),
    )
    .with_ready(true)
}

fn call(tool_name: &str) -> ToolCall {
    ToolCall {
        thought_signature: None,
        provider_state: None,
        tool_call_id: ToolCallId(format!("call-{tool_name}")),
        tool_name: tool_name.into(),
        input: serde_json::json!({}).into(),
    }
}

fn assert_failure_result_names_tool(route: ToolRoute, tool_name: &str) {
    let ToolRoute::Failure(message) = route else {
        panic!("expected route failure for {tool_name}");
    };
    let result = route_failure_result(&call(tool_name), message);
    assert!(
        result.is_error,
        "routing failures are model-visible tool errors"
    );
    assert!(
        matches!(result.output, ToolOutput::Error { message, .. } if message.contains(tool_name)),
        "routing failure message should name the unresolved tool"
    );
}

#[tokio::test]
async fn consecutive_batches_share_one_catalog_snapshot() {
    let catalog = vec![catalog_entry("module-m", &["echo"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog.clone(), catalog],
        vec![
            successful_tool_reply("first"),
            successful_tool_reply("second"),
        ],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    plane.windows.lock().unwrap().insert("module-m".into(), 1);
    let tool_call = call("echo");
    let tools = [request_tool("echo", None)];

    let first = batch_ctx("batch-1");
    plane
        .batch_started(
            &first,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();
    assert_completed_success(plane.dispatch(&first, &tool_call).await);
    plane.batch_finished(&first).await.unwrap();

    let second = batch_ctx("batch-2");
    plane
        .batch_started(
            &second,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();
    assert_completed_success(plane.dispatch(&second, &tool_call).await);
    plane.batch_finished(&second).await.unwrap();

    assert_eq!(
        conn.catalog_calls.load(Ordering::SeqCst),
        1,
        "the second batch must reuse the plane's decoded catalog snapshot"
    );
}

/// A scoped batch finds its session's routes under its scope as one range of the route map:
/// exactly those routes, whatever other sessions and scopes, including neighbours in the map's
/// order, have cached.
#[test]
fn a_sessions_routes_under_one_scope_are_read_as_one_range() {
    let session = |name: &str| SessionKey {
        project_root: PathBuf::from("/project"),
        harness: "test".into(),
        session: name.into(),
    };
    let scope = |epoch: u64| -> ScopeKey { Some(("prefrontal".into(), "head".into(), epoch)) };
    let mut routes = BTreeMap::new();
    let mut channel = 0;
    for (name, scope) in [
        ("a", scope(1)),
        ("b", None),
        ("b", scope(1)),
        ("b", scope(2)),
        ("bb", scope(1)),
        ("c", scope(1)),
    ] {
        for module_id in ["", "aft", "magic-context"] {
            for held_reply_v1 in [false, true] {
                channel += 1;
                routes.insert(
                    RouteKey {
                        session: session(name),
                        scope: scope.clone(),
                        module_id: module_id.into(),
                        shape: RouteShape {
                            tool_result_v1: held_reply_v1,
                            ..RouteShape::default()
                        },
                        held_reply_v1,
                    },
                    RouteState {
                        handle: RouteHandle::synthetic(channel),
                        sem: Arc::new(Semaphore::new(1)),
                        stamp: None,
                        last_used: Instant::now(),
                    },
                );
            }
        }
    }
    let wanted = session("b");
    let wanted_scope = scope(1);
    let found: Vec<_> = session_scope_routes(&routes, &wanted, &wanted_scope)
        .map(|(key, _)| key.clone())
        .collect();
    let expected: Vec<_> = routes
        .keys()
        .filter(|key| key.session == wanted && key.scope == wanted_scope)
        .cloned()
        .collect();
    assert_eq!(expected.len(), 6);
    assert_eq!(found, expected);
}

/// The plane's catalog snapshot follows the connection's catalog generation. Once the daemon
/// reports a new catalog, the next batch resolves against it: a tool that a second provider now
/// serves too is refused as ambiguous instead of still going to the first.
#[tokio::test]
async fn a_new_catalog_generation_replaces_the_snapshot_before_the_next_batch() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![
            vec![catalog_entry("module-a", &["echo"])],
            vec![
                catalog_entry("module-a", &["echo"]),
                catalog_entry("module-b", &["echo"]),
            ],
            vec![
                catalog_entry("module-a", &["echo"]),
                catalog_entry("module-b", &["echo"]),
            ],
        ],
        vec![successful_tool_reply("first")],
    );
    *conn.catalog_generation.lock().unwrap() =
        Some(crate::connection::CatalogGeneration::for_test(1));
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    for module in ["module-a", "module-b"] {
        plane.windows.lock().unwrap().insert(module.into(), 1);
    }
    let tool_call = call("echo");
    let manifest = ToolManifest::from_tools(vec![request_tool("echo", None)]);

    let first = batch_ctx("batch-1");
    plane
        .batch_started(&first, std::slice::from_ref(&tool_call), &manifest)
        .await
        .unwrap();
    assert_completed_success(plane.dispatch(&first, &tool_call).await);
    plane.batch_finished(&first).await.unwrap();

    *conn.catalog_generation.lock().unwrap() =
        Some(crate::connection::CatalogGeneration::for_test(2));
    let second = batch_ctx("batch-2");
    plane
        .batch_started(&second, std::slice::from_ref(&tool_call), &manifest)
        .await
        .unwrap();
    let outcome = plane.dispatch(&second, &tool_call).await;
    assert!(
        matches!(&outcome, DispatchResult::Completed(result) if result.is_error),
        "an ambiguous tool must not be dispatched: {outcome:?}"
    );
    plane.batch_finished(&second).await.unwrap();
    assert_eq!(
        conn.catalog_calls.load(Ordering::SeqCst),
        3,
        "the new generation is read, then read once more as every unresolved call is"
    );
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        1,
        "only the first batch's call was sent"
    );
}

#[tokio::test]
async fn overlapping_sessions_keep_batch_routes_isolated_at_terminal() {
    let catalog = vec![
        catalog_entry("module-a", &["echo"]),
        catalog_entry("module-b", &["echo"]),
    ];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog],
        vec![
            successful_tool_reply("from-a"),
            successful_tool_reply("from-b"),
        ],
    );
    let plane = SubcToolPlane::with_connection(conn, "");
    plane.windows.lock().unwrap().insert("module-a".into(), 1);
    plane.windows.lock().unwrap().insert("module-b".into(), 1);
    let tool_call = call("echo");
    let ctx_a = batch_ctx_for("shared-batch", "/tmp/a", "session-a");
    let ctx_b = batch_ctx_for("shared-batch", "/tmp/b", "session-b");

    plane
        .batch_started(
            &ctx_a,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("echo", Some("module-a"))]),
        )
        .await
        .unwrap();
    plane
        .batch_started(
            &ctx_b,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("echo", Some("module-b"))]),
        )
        .await
        .unwrap();

    {
        let batches = plane.batch_routes.lock().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches
                .get(&SubcToolPlane::batch_route_key(&ctx_a))
                .and_then(|routes| routes.get(&tool_call.tool_call_id)),
            Some(&ToolRoute::Module("module-a".into()))
        );
        assert_eq!(
            batches
                .get(&SubcToolPlane::batch_route_key(&ctx_b))
                .and_then(|routes| routes.get(&tool_call.tool_call_id)),
            Some(&ToolRoute::Module("module-b".into()))
        );
    }
    // The module each batch's intent records is the one that batch then dispatches to.
    assert_eq!(
        plane
            .dispatch_target(&ctx_a, &tool_call.tool_call_id)
            .as_deref(),
        Some("module-a")
    );
    assert_eq!(
        plane
            .dispatch_target(&ctx_b, &tool_call.tool_call_id)
            .as_deref(),
        Some("module-b")
    );

    assert_completed_success(plane.dispatch(&ctx_a, &tool_call).await);
    plane.batch_finished(&ctx_a).await.unwrap();
    assert!(plane
        .batch_routes
        .lock()
        .unwrap()
        .contains_key(&SubcToolPlane::batch_route_key(&ctx_b)));
    assert_completed_success(plane.dispatch(&ctx_b, &tool_call).await);
    plane.batch_finished(&ctx_b).await.unwrap();
    assert!(plane.batch_routes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dispatch_invalidation_removes_only_the_targeted_call_route() {
    let catalog = vec![catalog_entry("module-m", &["echo-a", "echo-b"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog],
        vec![Err(ConnError::Transport("connection reset".into()))],
    );
    let plane = SubcToolPlane::with_connection(conn, "");
    plane.windows.lock().unwrap().insert("module-m".into(), 2);
    let first = call("echo-a");
    let second = call("echo-b");
    let calls = [first.clone(), second.clone()];
    let tools = [
        request_tool("echo-a", Some("module-m")),
        request_tool("echo-b", Some("module-m")),
    ];
    let ctx = batch_ctx("batch-invalidation-scope");
    plane
        .batch_started(&ctx, &calls, &ToolManifest::from_tools(tools.to_vec()))
        .await
        .unwrap();

    assert!(matches!(
        plane.dispatch(&ctx, &first).await,
        DispatchResult::SentButOutcomeUnknown { .. }
    ));

    let batches = plane.batch_routes.lock().unwrap();
    let routes = batches
        .get(&SubcToolPlane::batch_route_key(&ctx))
        .expect("the other call keeps the batch alive");
    assert!(!routes.contains_key(&first.tool_call_id));
    assert_eq!(
        routes.get(&second.tool_call_id),
        Some(&ToolRoute::Module("module-m".into()))
    );
}

#[tokio::test]
async fn close_session_removes_all_of_its_batches_only() {
    let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
    let plane = SubcToolPlane::with_connection(conn, "");
    let ctx_a1 = batch_ctx_for("batch-1", "/tmp/a", "session-a");
    let ctx_a2 = batch_ctx_for("batch-2", "/tmp/a", "session-a");
    let ctx_b = batch_ctx_for("batch-1", "/tmp/b", "session-b");
    let route = HashMap::from([(
        ToolCallId("call".into()),
        ToolRoute::Module("module-m".into()),
    )]);
    {
        let mut batches = plane.batch_routes.lock().unwrap();
        batches.insert(SubcToolPlane::batch_route_key(&ctx_a1), route.clone());
        batches.insert(SubcToolPlane::batch_route_key(&ctx_a2), route.clone());
        batches.insert(SubcToolPlane::batch_route_key(&ctx_b), route);
    }

    plane.close_session_routes(&ctx_a1.bind).await;

    let batches = plane.batch_routes.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert!(batches.contains_key(&SubcToolPlane::batch_route_key(&ctx_b)));
}

#[tokio::test]
async fn timed_out_dispatch_closes_the_possibly_live_route() {
    let catalog = vec![catalog_entry("module-m", &["echo"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog],
        vec![Err(ConnError::Transport("timed out after 120s".into()))],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    plane.windows.lock().unwrap().insert("module-m".into(), 1);
    let tool_call = call("echo");
    let tools = [request_tool("echo", None)];
    let ctx = batch_ctx("batch-timeout");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();

    assert!(matches!(
        plane.dispatch(&ctx, &tool_call).await,
        DispatchResult::SentButOutcomeUnknown { .. }
    ));
    wait_for_closed_routes(&conn, 1).await;

    assert_eq!(
        conn.closed_routes.lock().unwrap().as_slice(),
        &[RouteHandle::synthetic(1)],
        "revert check: dropping the cache entry without the fix emits no route GOODBYE"
    );
}

#[tokio::test]
async fn route_gone_dispatch_does_not_send_a_second_goodbye() {
    let catalog = vec![catalog_entry("module-m", &["echo"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog],
        vec![Err(ConnError::Transport(
            "route gone (module endpoint dropped)".into(),
        ))],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    plane.windows.lock().unwrap().insert("module-m".into(), 1);
    let tool_call = call("echo");
    let tools = [request_tool("echo", None)];
    let ctx = batch_ctx("batch-route-gone");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();

    assert!(matches!(
        plane.dispatch(&ctx, &tool_call).await,
        DispatchResult::SentButOutcomeUnknown { .. }
    ));
    tokio::task::yield_now().await;

    assert!(
        conn.closed_routes.lock().unwrap().is_empty(),
        "the daemon already freed a route that produced route-gone GOODBYE"
    );
}

#[tokio::test]
async fn dispatch_failure_clears_catalog_snapshot_for_next_batch() {
    let catalog = vec![catalog_entry("module-m", &["echo"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog.clone(), catalog],
        vec![Err(ConnError::Transport("connection reset".into()))],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    plane.windows.lock().unwrap().insert("module-m".into(), 1);
    let tool_call = call("echo");
    let tools = [request_tool("echo", None)];

    let first = batch_ctx("batch-failure");
    plane
        .batch_started(
            &first,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();
    assert!(matches!(
        plane.dispatch(&first, &tool_call).await,
        DispatchResult::SentButOutcomeUnknown { .. }
    ));
    assert_eq!(
        plane.cached_route_channel_for_module(&first.bind, "module-m"),
        None
    );
    plane.batch_finished(&first).await.unwrap();

    let second = batch_ctx("batch-after-failure");
    plane
        .batch_started(
            &second,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(tools.to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(
        conn.catalog_calls.load(Ordering::SeqCst),
        2,
        "a learned-dead route must force the following batch to fetch a fresh catalog"
    );
}

/// THE PROVIDER ID REACHES THE WIRE.
///
/// Until 2026-09-03 the envelope was `{name, arguments}` with no id at all, so every module
/// downstream either synthesized an identity or refused the call -- one of them silently for
/// weeks, which is the failure this pins.
///
/// SCOPED TO ONE DISPATCH ON PURPOSE. My first version also asserted that a transparent
/// retry reuses the id, and it HUNG the suite: driving an `unknown_channel` rejection evicts
/// the route and opens a fresh one, which lands in warm_bind's unbounded backoff waiting for
/// a session lifecycle cancel a unit test never delivers. The retry property is real and
/// worth stating, but the honest place for it is the spine test with a live daemon
/// (tool_fault_spine.rs), not a stub that has to fake the machinery it depends on.
#[tokio::test]
async fn dispatch_puts_the_provider_id_on_the_wire() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new(), Vec::new(), Vec::new()],
        vec![Ok(serde_json::json!({"content": [], "isError": false}))],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");

    let ctx = batch_ctx("batch-id-on-wire");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();
    let _ = plane.dispatch(&ctx, &tool_call).await;

    let ids = conn.dispatched_ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 1, "one dispatch, one wire call");
    assert_eq!(
        ids[0].as_deref(),
        Some(tool_call.tool_call_id.0.as_str()),
        "the provider id must reach the wire rather than being dropped at the envelope"
    );
}

/// A TOOL-MODULE RESTART MID-DISPATCH IS ONE TRANSPARENT RETRY, NOT A MODEL-VISIBLE ERROR.
///
/// Measured live before the fix: an acceptance turn dispatched while AFT was restarting
/// and the model received `tool/route rejected: module_reloading -- module endpoint for
/// route channel 1 is reloading`. That code is pre-send by subc-protocol's documented
/// contract, so the plane may re-dispatch it -- once, with the SAME id.
///
/// Three properties, each carrying weight:
///  - the call completes, so the model never sees the restart;
///  - the wire saw the same id twice, which is the evidence the retry is a re-send of a
///    call the module never received rather than a second call;
///  - the evicted route is not closed, since closing a draining endpoint could cut off
///    an in-flight sibling's reply.
///
/// Bounded by a timeout because an earlier retry test here HUNG: re-opening after an
/// eviction can wait on a lifecycle cancel a unit test never delivers. If this ever
/// hangs, it fails in five seconds and says why instead of stalling the suite.
#[tokio::test]
async fn module_reloading_mid_dispatch_is_one_transparent_retry() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new(), Vec::new(), Vec::new()],
        vec![
            Err(ConnError::Rejected(crate::connection::decode_error_body(
                br#"{"code":"module_reloading","message":"draining endpoint"}"#,
            ))),
            successful_tool_reply("served after the restart"),
        ],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("batch-reloading");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(5), plane.dispatch(&ctx, &tool_call))
        .await
        .expect("the retry must re-open without waiting on a lifecycle cancel");
    assert_completed_success(outcome);

    let ids = conn.dispatched_ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 2, "one refused attempt, one retry: {ids:?}");
    assert_eq!(
        ids[0], ids[1],
        "the retry must re-send the SAME id, not mint a second call"
    );

    // NOT CLOSED, deliberately. The route is still bound on the draining endpoint, and a
    // GOODBYE there could cut off a sibling call the drain is waiting for -- or, since the
    // eviction is by key, close a FRESH route a sibling already re-opened. The drain
    // removes the old binding itself.
    tokio::task::yield_now().await;
    assert!(
        conn.closed_routes.lock().unwrap().is_empty(),
        "a reloading route must be forgotten, not closed: a close can cut off an in-flight sibling"
    );
}

#[tokio::test]
async fn oversized_reloading_refusal_keeps_its_code_and_discards_its_message() {
    let refusal = |message: &str| {
        Err(ConnError::Rejected(crate::connection::decode_error_body(
            &serde_json::to_vec(&serde_json::json!({"code":"module_reloading", "message":message}))
                .unwrap(),
        )))
    };
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new(), Vec::new(), Vec::new()],
        vec![refusal("draining endpoint"), refusal(&"x".repeat(65_537))],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("batch-oversized-refusal");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();
    let DispatchResult::Completed(result) = plane.dispatch(&ctx, &tool_call).await else {
        panic!("a final refusal must complete the call");
    };
    let ToolOutput::Error {
        code,
        message,
        detail,
        ..
    } = result.output
    else {
        panic!("an oversized refusal must remain an error");
    };
    assert_eq!(code.as_deref(), Some("module_reloading"));
    assert!(
        message.contains("refused") && message.contains("discarded"),
        "{message}"
    );
    assert!(
        !message.contains("tool ran"),
        "a refusal must not claim execution"
    );
    let detail: serde_json::Value = serde_json::from_str(detail.as_deref().unwrap()).unwrap();
    assert_eq!(detail["bytes"], 65_537);
    assert_eq!(detail["limit"], 65_536);
    assert_eq!(
        conn.dispatched_ids.lock().unwrap().len(),
        2,
        "no resend after the final refusal"
    );
}

#[tokio::test]
async fn module_reloading_prose_and_other_codes_do_not_trigger_resend() {
    let mut refusals = vec![ConnError::Rejected(
        "module_reloading — legacy prose".into(),
    )];
    for code in ["target_unavailable", "module_warming", "permission_denied"] {
        let body = subc_protocol::ErrorBody::new(code, "module_reloading — misleading prose");
        refusals.push(ConnError::Rejected(crate::connection::decode_error_body(
            &serde_json::to_vec(&body).unwrap(),
        )));
    }
    for refusal in refusals {
        let conn = ScriptedToolPlaneConnection::new(
            vec![Vec::new(), Vec::new(), Vec::new()],
            vec![Err(refusal), successful_tool_reply("unsafe resend")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let ctx = batch_ctx("no-prose-resend");
        let tool_call = call("read");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
            )
            .await
            .unwrap();
        match plane.dispatch(&ctx, &tool_call).await {
            DispatchResult::Completed(result) => assert!(result.is_error),
            other => panic!("expected a terminal refusal, got {other:?}"),
        }
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn tool_role_refusals_are_permanent_keep_detail_and_never_resend() {
    let refusal = cortexkit_role_tool_provider::errors::tool_role_not_declared(None);
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new(), Vec::new()],
        vec![Err(ConnError::Rejected(
            crate::connection::decode_error_body(&serde_json::to_vec(&refusal).unwrap()),
        ))],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("role-refusal");
    let call = call("read");
    let manifest = ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]);
    plane
        .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
        .await
        .unwrap();
    for _ in 0..2 {
        let DispatchResult::Completed(result) = plane.dispatch(&ctx, &call).await else {
            panic!("not terminal")
        };
        assert!(result.is_error);
        assert_eq!(
            result.output,
            ToolOutput::Json {
                value: serde_json::json!({
                    "class":"permanent", "code":"tool_role_not_declared",
                    "detail":{"role":"tool-provider", "declared":null}
                })
                .into()
            }
        );
    }
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);

    let error = map_tool_open_refusal(
        RouteEnsureError::Connection(ConnError::Rejected(crate::connection::decode_error_body(
            &serde_json::to_vec(&refusal).unwrap(),
        ))),
        "aft",
    );
    assert!(
        matches!(error, ToolPlaneError::Permanent { ref code, ref detail }
            if code == "tool_role_not_declared" && *detail == refusal.detail)
    );
}

/// A RESTART LONGER THAN THE DEFAULT 3s STILL RETRIES THROUGH.
///
/// This is the arm the first version of the fix did not have, and why that version was
/// wrong: its scripted re-open succeeded instantly, so it never met a real drain. Measured
/// from the daemon's logs, 13 of 16 AFT module_reloading episodes lasted longer than 3s,
/// clustered at 15-30s. With the default patience every one of those ended in NotSent --
/// the model still got an error, three seconds later.
///
/// Here the re-open is refused module_reloading for over 7s of (paused) time before the
/// module comes back. The call must complete, not report NotSent.
#[tokio::test(start_paused = true)]
async fn a_reload_longer_than_the_default_patience_still_completes() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new(), Vec::new(), Vec::new()],
        vec![
            Err(ConnError::Rejected(crate::connection::decode_error_body(
                br#"{"code":"module_reloading","message":"draining endpoint"}"#,
            ))),
            successful_tool_reply("served after a long drain"),
        ],
    );
    // The first open (the dispatch's own) succeeds; the re-open after the refusal is
    // refused eight times, then the module is back. Each refusal takes a second to arrive,
    // so the drain lasts at least 8s of paused time whatever the backoff's full jitter
    // draws: the waits between attempts can be near zero, and without this the total
    // sometimes fell under the 3s default patience the test exists to outlast.
    let mut sequence = vec![RouteOpenScript::default()];
    sequence.extend((0..8).map(|_| RouteOpenScript {
        delay: Duration::from_secs(1),
        ..RouteOpenScript::rejected("module_reloading \u{2014} module_id 'aft' is reloading")
    }));
    sequence.push(RouteOpenScript::default());
    conn.script_route_open_sequence("aft", sequence);

    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("batch-long-reload");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();

    let started = Instant::now();
    let outcome = plane.dispatch(&ctx, &tool_call).await;
    let waited = started.elapsed();

    assert_completed_success(outcome);
    assert!(
        waited > ROUTE_OPEN_RETRY_DEADLINE,
        "the re-open must have outlasted the default patience to prove anything: {waited:?}"
    );
    assert!(
        waited < TOOL_DISPATCH_PATIENCE.deadline,
        "and still be bounded: {waited:?}"
    );
}

/// THE CONTROL: a rejection that is NOT pre-send is still a definite answer.
///
/// Without this arm, a change that retried EVERY rejection would pass the one above -- and
/// that change is the dangerous one, because a rejection after delivery re-sent is a
/// non-idempotent tool running twice.
#[tokio::test]
async fn a_rejection_that_is_not_pre_send_is_never_redispatched() {
    let conn = ScriptedToolPlaneConnection::new(
        vec![Vec::new(), Vec::new(), Vec::new()],
        vec![Err(ConnError::Rejected(
            "capability_forbidden — not permitted".into(),
        ))],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("batch-forbidden");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();

    match plane.dispatch(&ctx, &tool_call).await {
        DispatchResult::Completed(result) => assert!(result.is_error),
        other => panic!("expected a model-visible rejection, got {other:?}"),
    }
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        1,
        "a definite answer must be delivered, never re-sent"
    );
    assert!(conn.closed_routes.lock().unwrap().is_empty());
}

/// A tool-call batch waits out a 25 s provider-module restart instead of failing the turn.
///
/// The route open is refused `target_unavailable` for 25 s of paused time, then the
/// module accepts: the batch opens, the call completes, and exactly one tool call went
/// out. Against the 3 s budget this batch start fails; the 90 s tool-dispatch patience
/// rides it out through the same per-target gate every waiter shares.
#[tokio::test(start_paused = true)]
async fn tool_dispatch_waits_out_a_25s_module_restart() {
    let catalog = vec![catalog_entry("aft", &["read"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog.clone(), catalog.clone(), catalog],
        vec![successful_tool_reply("served after restart")],
    );
    // 25 refusals, each taking a second to arrive: a 25 s outage.
    let mut sequence = (0..25)
        .map(|_| RouteOpenScript {
            delay: Duration::from_secs(1),
            ..RouteOpenScript::rejected("target_unavailable — module_id 'aft' is unavailable")
        })
        .collect::<Vec<_>>();
    sequence.push(RouteOpenScript::default());
    conn.script_route_open_sequence("aft", sequence);
    conn.install_real_gate("aft");
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("batch-restart-25s");
    let tool_call = call("read");
    let started = Instant::now();
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .expect("the 90 s patience must ride out a 25 s restart");
    assert_completed_success(plane.dispatch(&ctx, &tool_call).await);
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_secs(25),
        "the batch must have outlasted the old 3 s budget to prove anything: {waited:?}"
    );
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        1,
        "exactly one send after the outage: nothing was sent while the module was down"
    );
    assert!(
        conn.route_open_calls.load(Ordering::SeqCst) >= 10,
        "the open kept retrying through the outage"
    );
}

/// A module down past the 90 s tool-dispatch patience ends the batch as
/// module-unavailable: proven undelivered, naming the module, the daemon's code and its
/// detail. Nothing is ever sent.
#[tokio::test(start_paused = true)]
async fn tool_dispatch_past_90s_reports_module_unavailable() {
    let catalog = vec![catalog_entry("aft", &["read"])];
    let conn =
        ScriptedToolPlaneConnection::new(vec![catalog.clone(), catalog.clone(), catalog], vec![]);
    // The module never comes back: every open is refused, each taking 5 s to arrive.
    conn.script_route_open_sequence(
        "aft",
        vec![RouteOpenScript {
            delay: Duration::from_secs(5),
            ..RouteOpenScript::rejected("target_unavailable — module_id 'aft' is unavailable")
        }],
    );
    conn.install_real_gate("aft");
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("batch-restart-forever");
    let tool_call = call("read");
    let started = Instant::now();
    let error = plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .expect_err("a module down past the patience must fail the batch");
    let waited = started.elapsed();
    assert!(
        waited >= TOOL_DISPATCH_PATIENCE.deadline,
        "the batch must have waited out the whole patience: {waited:?}"
    );
    match error {
        ToolPlaneError::ModuleUnavailable {
            module,
            code,
            detail,
        } => {
            assert_eq!(module, "aft");
            assert_eq!(code, "target_unavailable");
            assert!(!detail.is_empty(), "the daemon's detail is kept");
        }
        other => panic!("expected the attributed outage, got {other:?}"),
    }
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
    let opens = conn.route_open_calls.load(Ordering::SeqCst);
    assert!(
        opens >= 10,
        "the open kept retrying instead of giving up at once: {opens}"
    );
    assert!(
        opens < 40,
        "the gate paces probes while the module is down, one per tick: {opens}"
    );
}

/// Run cancel ends a module-outage wait at once instead of holding the patience.
#[tokio::test(start_paused = true)]
async fn cancel_mid_wait_ends_promptly() {
    let catalog = vec![catalog_entry("aft", &["read"])];
    let conn =
        ScriptedToolPlaneConnection::new(vec![catalog.clone(), catalog.clone(), catalog], vec![]);
    // Instant refusals: the wait is all backoff and gate pacing, which cancel must cut.
    // The probes are 50 ms apart (not zero), so the wait parks where cancel observes it
    // instead of spinning through an always-due probe.
    conn.script_route_open_sequence(
        "aft",
        vec![RouteOpenScript::rejected(
            "target_unavailable — module_id 'aft' is unavailable",
        )],
    );
    conn.install_real_gate_with_jitter("aft", |_| Duration::from_millis(50));
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("batch-cancel-mid-wait");
    let tool_call = call("read");
    let manifest = ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]);
    let cancel = CancellationToken::new();
    let guard = ToolOpenGuard::new(Some(&cancel), None);
    let started = Instant::now();
    let task = tokio::spawn(async move {
        plane
            .batch_started_guarded(&ctx, std::slice::from_ref(&tool_call), &manifest, &guard)
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    cancel.cancel();
    let result = task.await.expect("the wait must end on cancel");
    let waited = started.elapsed();
    assert!(
        matches!(result, Err(ToolPlaneError::Unavailable(_))),
        "a cancelled wait is an unsent failure, never a success: {result:?}"
    );
    assert!(
        waited < TOOL_DISPATCH_PATIENCE.deadline,
        "cancel must end the wait at once, not at the patience: {waited:?}"
    );
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
}

/// A call already in flight when the route drops is indeterminate and never retried:
/// the request may have run, so the plane reports an unknown outcome instead of re-sending.
#[tokio::test]
async fn in_flight_call_when_route_drops_is_indeterminate() {
    let catalog = vec![catalog_entry("aft", &["read"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog.clone(), catalog.clone(), catalog],
        vec![Err(ConnError::Transport("route gone (GOODBYE)".into()))],
    );
    *conn.close_during_call.lock().unwrap() = true;
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("batch-route-drop-in-flight");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();
    match plane.dispatch(&ctx, &tool_call).await {
        DispatchResult::SentButOutcomeUnknown { .. } => {}
        other => panic!("a sent call with no result is indeterminate, got {other:?}"),
    }
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        1,
        "the sent call is never re-sent"
    );
}

#[tokio::test]
async fn unresolved_tool_refetches_stale_catalog_once_and_succeeds() {
    let stale = vec![catalog_entry("older-module", &["older-tool"])];
    let fresh = vec![catalog_entry("module-m", &["new-tool"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![stale, fresh],
        vec![successful_tool_reply("resolved after refresh")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "");
    plane.windows.lock().unwrap().insert("module-m".into(), 1);

    let priming = batch_ctx("batch-prime-stale-catalog");
    plane
        .batch_started(&priming, &[], &ToolManifest::from_tools(Vec::new()))
        .await
        .unwrap();
    plane.batch_finished(&priming).await.unwrap();
    assert_eq!(conn.catalog_calls.load(Ordering::SeqCst), 1);

    let target = batch_ctx("batch-new-module");
    let tool_call = call("new-tool");
    plane
        .batch_started(
            &target,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("new-tool", None)]),
        )
        .await
        .unwrap();
    assert_completed_success(plane.dispatch(&target, &tool_call).await);
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        conn.catalog_calls.load(Ordering::SeqCst),
        2,
        "resolution failure must refetch once before becoming model-visible"
    );
}

#[test]
fn duplicate_call_id_keeps_first_calls_route() {
    let duplicate_id = ToolCallId("duplicate".into());
    let mut first = call("first-tool");
    first.tool_call_id = duplicate_id.clone();
    let mut duplicate = call("second-tool");
    duplicate.tool_call_id = duplicate_id.clone();

    let routes = resolve_batch_routes(
        &[first, duplicate],
        &ToolManifest::from_tools(vec![
            request_tool("first-tool", None),
            request_tool("second-tool", None),
        ]),
        &[
            catalog_entry("first-module", &["first-tool"]),
            catalog_entry("second-module", &["second-tool"]),
        ],
        None,
    );

    assert_eq!(routes.len(), 1);
    assert_eq!(
            routes.get(&duplicate_id),
            Some(&ToolRoute::Module("first-module".into())),
            "the first call is the one core dispatches; a collecting last-wins map would route it to second-module",
        );
}

#[tokio::test]
async fn catalog_discovery_keeps_selected_module_when_tool_names_collide() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": { "path": { "type": "string" } }
    });
    let mut selected = catalog_entry("selected-module", &["shared"]);
    let ProviderRole::ToolProvider { tools, .. } = &mut selected.roles[0] else {
        panic!("selected catalog entry must provide tools");
    };
    tools[0].schema = schema.clone();
    let catalog = vec![selected, catalog_entry("colliding-module", &["shared"])];
    let conn = ScriptedToolPlaneConnection::new(
        vec![catalog.clone(), catalog.clone(), catalog],
        vec![successful_tool_reply("selected")],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "selected-module");

    let discovered = plane.tools_from_catalog().await.unwrap();
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].module.as_deref(), Some("selected-module"));

    let mut hinted = CallOptions::new(Vec::new());
    hinted.tools = discovered.clone().into();
    let mut hintless = hinted.clone();
    let mut hintless_tools = hintless.tools.as_ref().clone();
    hintless_tools[0].module = None;
    hintless.tools = hintless_tools.into();
    let frozen = openai_chat_frozen_config("test-model");
    let hinted_wire = OpenAiChat.render(&frozen, &hinted);
    assert_eq!(
        hinted_wire,
        OpenAiChat.render(&frozen, &hintless),
        "routing metadata must not change provider request bytes"
    );
    let body: serde_json::Value = serde_json::from_slice(&hinted_wire).unwrap();
    assert_eq!(
        body.pointer("/tools/0/function/name"),
        Some(&serde_json::json!("shared"))
    );
    assert_eq!(body.pointer("/tools/0/function/parameters"), Some(&schema));

    let manifest = ToolManifest::from_tools(discovered);
    let ctx = batch_ctx("catalog-selected-module");
    let call = call("shared");
    plane
        .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
        .await
        .unwrap();
    assert_completed_success(plane.dispatch(&ctx, &call).await);
    assert_eq!(
        conn.route_open_completions.lock().unwrap().as_slice(),
        ["selected-module"]
    );
}

#[test]
fn resolves_caller_hint_before_catalog() {
    let route = resolve_tool_module(
        "shared",
        &ToolManifest::from_tools(vec![request_tool("shared", Some("hinted"))]),
        &[catalog_entry("catalog", &["shared"])],
        Some("default"),
    );
    assert_eq!(route, ToolRoute::Module("hinted".into()));
}

#[test]
fn resolves_catalog_unique_provider() {
    let route = resolve_tool_module(
        "unique",
        &ToolManifest::from_tools(vec![request_tool("unique", None)]),
        &[catalog_entry("catalog", &["unique"])],
        Some("default"),
    );
    assert_eq!(route, ToolRoute::Module("catalog".into()));
}

#[test]
fn ambiguous_without_hint_fails_the_call() {
    let route = resolve_tool_module(
        "shared",
        &ToolManifest::from_tools(vec![request_tool("shared", None)]),
        &[
            catalog_entry("module-a", &["shared"]),
            catalog_entry("module-b", &["shared"]),
        ],
        Some("default"),
    );
    assert_failure_result_names_tool(route, "shared");
}

#[test]
fn default_module_fallback_preserves_single_module_behavior() {
    let route = resolve_tool_module(
        "unknown_to_catalog",
        &ToolManifest::from_tools(vec![request_tool("unknown_to_catalog", None)]),
        &[],
        Some("aft"),
    );
    assert_eq!(route, ToolRoute::Module("aft".into()));
}

#[test]
fn unknown_everywhere_without_default_fails_the_call() {
    let route = resolve_tool_module(
        "missing",
        &ToolManifest::from_tools(vec![request_tool("missing", None)]),
        &[],
        None,
    );
    assert_failure_result_names_tool(route, "missing");
}

/// COLLISION-SAFETY: the SAME tool_call_id under TWO different sessions must NOT
/// false-dedup — the fence keys on (session, tool_call_id), so each session's call sees
/// only its own result. Keying by the id alone (the old bug) would return session A's
/// result for session B's identically-named call.
#[test]
fn fence_does_not_cross_session_dedup() {
    let mut fence = Fence::default();
    let id = ToolCallId("toolu_same".into());
    let batch = BatchId("b1".into());
    // Session A dispatches the call and gets result "A".
    fence.insert(
        &batch,
        (session_key("/tmp/a", "runner", "sessionA"), id.clone()),
        result("A"),
    );

    // Session B's identically-named call is NOT a fence hit (different session key).
    assert_eq!(
        fence.get(&(session_key("/tmp/b", "runner", "sessionB"), id.clone())),
        None,
        "the same tool_call_id under a DIFFERENT session must not be a fence hit"
    );
    // Session A's repeat IS a hit (same session + id) and returns the exact fenced result.
    let hit = fence.get(&(session_key("/tmp/a", "runner", "sessionA"), id));
    assert_eq!(
        hit,
        Some(result("A")),
        "session A's repeat call returns its original result"
    );
}

/// Two sessions can run batches with the same id: finishing one session's batch evicts only that
/// session's entries.
#[test]
fn fence_eviction_stays_within_the_batchs_session() {
    let mut fence = Fence::default();
    let batch = BatchId("b1".into());
    let a = session_key("/tmp/a", "runner", "sessionA");
    let b = session_key("/tmp/b", "runner", "sessionB");
    let id = ToolCallId("c1".into());
    fence.insert(&batch, (a.clone(), id.clone()), result("A"));
    fence.insert(&batch, (b.clone(), id.clone()), result("B"));
    fence.evict_batch(&(a.clone(), batch));
    assert!(fence.get(&(a, id.clone())).is_none());
    assert_eq!(fence.get(&(b, id)), Some(result("B")));
}

/// EVICTION: a batch's fence entries are gone after its terminal — bounded growth.
/// Without eviction the fence grows unbounded over thousands of batches.
#[test]
fn fence_evicts_a_batchs_entries_on_terminal() {
    let mut fence = Fence::default();
    let b1 = BatchId("b1".into());
    let b2 = BatchId("b2".into());
    let session = session_key("/tmp/s", "runner", "session");
    fence.insert(
        &b1,
        (session.clone(), ToolCallId("c1".into())),
        result("r1"),
    );
    fence.insert(
        &b1,
        (session.clone(), ToolCallId("c2".into())),
        result("r2"),
    );
    fence.insert(
        &b2,
        (session.clone(), ToolCallId("c3".into())),
        result("r3"),
    );
    assert_eq!(fence.len(), 3);

    // Evicting b1 drops ONLY b1's two entries; b2's survives.
    fence.evict_batch(&(session.clone(), b1.clone()));
    assert_eq!(fence.len(), 1, "only b1's entries evicted");
    assert!(fence
        .get(&(session.clone(), ToolCallId("c3".into())))
        .is_some());
    assert!(fence
        .get(&(session.clone(), ToolCallId("c1".into())))
        .is_none());

    fence.evict_batch(&(session.clone(), b2.clone()));
    assert_eq!(
        fence.len(),
        0,
        "the fence is empty after all batches terminate"
    );
}

#[test]
fn structured_session_key_prevents_separator_collisions() {
    let bind_a = BindIdentity::new(PathBuf::from("/tmp/proj\u{1f}runner"), "a", "b");
    let bind_b = BindIdentity::new(PathBuf::from("/tmp/proj"), "runner\u{1f}a", "b");

    let legacy_a = format!(
        "{}\u{1f}{}\u{1f}{}",
        bind_a.project_root.display(),
        bind_a.harness,
        bind_a.session
    );
    let legacy_b = format!(
        "{}\u{1f}{}\u{1f}{}",
        bind_b.project_root.display(),
        bind_b.harness,
        bind_b.session
    );
    assert_eq!(legacy_a, legacy_b, "the old string key would collide");
    assert_ne!(
        SubcToolPlane::session_key(&bind_a),
        SubcToolPlane::session_key(&bind_b),
        "the structured key keeps distinct binds distinct"
    );
}

#[test]
fn cache_route_state_keeps_first_route_and_reports_the_loser() {
    let key = route_key("/tmp/proj", "runner", "s1", "aft");
    let first_sem = Arc::new(Semaphore::new(1));
    let second_sem = Arc::new(Semaphore::new(2));
    let mut routes = BTreeMap::new();

    let first_handle = RouteHandle::synthetic(11);
    let second_handle = RouteHandle::synthetic(12);
    let first = cache_route_state(
        &mut routes,
        key.clone(),
        first_handle,
        Arc::clone(&first_sem),
        None,
    );
    assert_eq!(first.handle, first_handle);
    assert!(first.loser_handle.is_none());
    assert!(Arc::ptr_eq(&first.sem, &first_sem));

    let second = cache_route_state(
        &mut routes,
        key.clone(),
        second_handle,
        Arc::clone(&second_sem),
        None,
    );
    assert_eq!(
        second.handle, first_handle,
        "the first cached route stays authoritative"
    );
    assert_eq!(
        second.loser_handle,
        Some(second_handle),
        "the redundant daemon route must be closed"
    );
    assert!(Arc::ptr_eq(&second.sem, &first_sem));
    assert_eq!(routes.get(&key).unwrap().handle, first_handle);
}

#[test]
fn route_cache_separates_every_wire_shape_member() {
    let mut routes = BTreeMap::new();
    let key = route_key("/project", "broca", "session", "provider");
    for member in 0..6 {
        let mut handle = RouteHandle::synthetic(7 + member);
        handle.reads_tool_result = member == 1;
        handle.reads_held_reply = member == 2;
        handle.call_keepalive = member == 3;
        handle.tool_provider_v1 = member == 4;
        handle.session_identity = member == 5;
        let cached = cache_route_state(
            &mut routes,
            key.clone(),
            handle,
            Arc::new(Semaphore::new(1)),
            None,
        );
        assert_eq!(cached.handle, handle);
        assert!(cached.loser_handle.is_none());
    }
    assert_eq!(routes.len(), 6);
}

#[tokio::test]
async fn a_non_compaction_hook_providers_tool_and_hook_routes_share_one_identity() {
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
    let conn = Arc::new(conn);
    let plane = SubcToolPlane::with_connection(conn.clone(), "dual");
    plane
        .windows
        .lock()
        .unwrap()
        .extend([("dual".into(), 1), ("only-tools".into(), 1)]);
    let ctx = batch_ctx("hook-tool-identity");
    let manifest = broca_records::record::PlanManifest {
        rich_ui: None,
        declarations: Vec::new(),
        plan: broca_records::record::OpaqueJson::from_json_text(
            serde_json::json!({
                "tool_items":[{"provider":"dual"},{"provider":"only-tools"}],
                "step_transform_items":[{"provider":"dual","subscriptions":[]}],
                "compaction_item":{"provider":"different-compactor"}
            })
            .to_string(),
        )
        .unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        fetches: Vec::new(),
        tools: Vec::new().into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
    };
    let manifest = ToolManifest::from_tools(vec![
        request_tool("dual_tool", Some("dual")),
        request_tool("plain_tool", Some("only-tools")),
    ])
    .with_plan_manifest(&manifest)
    .unwrap();
    let open = async {
        plane
            .batch_started(&ctx, &[call("dual_tool"), call("plain_tool")], &manifest)
            .await
            .unwrap();
        conn.route_open_role_within(
            "dual",
            &ctx.bind,
            "step-transform-provider",
            "v2",
            None,
            crate::RouteOpenRetry::default_patience(),
        )
        .await
        .unwrap();
    };
    let serve = async {
        let inventory = daemon.next_request().await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&inventory.body).unwrap()["op"],
            "catalog.list"
        );
        let modules: Vec<_> = ["dual", "only-tools"]
            .iter()
            .map(|module| {
                let mut entry = catalog_entry(module, &[]);
                entry.capabilities = Some(
                    serde_json::from_value(serde_json::json!({"provides":["tool-provider/v1"]}))
                        .unwrap(),
                );
                entry
            })
            .collect();
        daemon.reply(
            &inventory,
            serde_json::to_vec(&subc_control::ClientControlResponse::CatalogList {
                generation: 1,
                modules,
                subc_ops: Vec::new(),
            })
            .unwrap(),
        );
        let mut identities = HashMap::new();
        for channel in 7..=9 {
            let request = daemon.next_request().await;
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let is_hook = body["role_versions"]["step-transform-provider"] == "v2";
            assert_eq!(body["op"], "route.open", "unexpected request: {body}");
            let key = if is_hook {
                "hook".to_owned()
            } else {
                body["target"]["module_id"]
                    .as_str()
                    .expect("tool route target")
                    .to_owned()
            };
            identities.insert(key, body["identity"].clone());
            daemon.reply(
                &request,
                serde_json::to_vec(&subc_control::ClientControlResponse::RouteOpen {
                    route_channel: channel,
                    route_epoch: 1,
                    request_window: None,
                    watch_window: None,
                })
                .unwrap(),
            );
        }
        assert_eq!(
            identities["only-tools"],
            serde_json::json!({"harness":"runner", "session":ctx.bind.session})
        );
        assert_eq!(
            identities["hook"],
            serde_json::json!({"harness":ctx.bind.harness, "session":ctx.bind.session})
        );
        assert_eq!(identities["dual"], identities["hook"]);
    };
    tokio::join!(open, serve);
}

#[test]
fn route_match_rejects_a_replaced_route_even_if_the_channel_number_is_reused() {
    let key = route_key("/tmp/proj", "runner", "s1", "aft");
    let old_sem = Arc::new(Semaphore::new(1));
    let new_sem = Arc::new(Semaphore::new(1));
    let mut routes = BTreeMap::new();

    let old_handle = RouteHandle::synthetic_epoch(11, 1);
    let new_handle = RouteHandle::synthetic_epoch(11, 2);
    routes.insert(
        key.clone(),
        RouteState {
            handle: old_handle,
            sem: Arc::clone(&old_sem),
            stamp: None,
            last_used: Instant::now(),
        },
    );
    routes.insert(
        key.clone(),
        RouteState {
            handle: new_handle,
            sem: Arc::clone(&new_sem),
            stamp: None,
            last_used: Instant::now(),
        },
    );

    assert!(
        !route_matches(&routes, &key, old_handle, &old_sem),
        "a waiting task must not send on a route that was invalidated and replaced"
    );
    assert!(route_matches(&routes, &key, new_handle, &new_sem));
}

/// A tool plane connection whose route open is pre-granted but whose TOOL CALLS go through
/// the real `SubcConnection` request path, so the reply deadline under test is the one the
/// production connection actually applies, not a re-implementation of it.
struct InProcessWireConnection {
    conn: SubcConnection,
    route: RouteHandle,
    close_on_wire: bool,
}

#[tokio::test]
async fn reply_deadline_vectors_reach_real_dispatch_selection() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../broca-fetch-plan/tests/fixtures/tool-provider-v1/reply-deadline.json"
    ))
    .unwrap();
    for case in vectors["deadlines"].as_array().unwrap() {
        let reply = if case["reply"].is_null() {
            serde_json::Value::Null
        } else {
            serde_json::json!({"read": case["reply"]})
        };
        let tool = serde_json::json!({"name":"read", "description":"probe", "input_schema": vectors["input_schema"], "module":"aft", "execution_mode":"parallel"});
        let frozen: broca_records::record::PlanManifest = serde_json::from_value(serde_json::json!({
                "plan": r#"{"tool_items":[{"provider":"aft","preset":"worker"}]}"#,
                "plan_identity":"", "composition_digest":"", "fetches":[{
                    "provider":"aft", "catalog_digest":"", "tools":[tool], "reply":reply
                }], "tools":[tool], "system_text":"", "system_digest":"", "join_version":1, "role_versions":{}
            })).unwrap();
        // Decode the same bytes a resumed run reads before building its dispatch manifest.
        let manifest = ToolManifest::from_tools(frozen.tools.as_ref().clone())
            .with_plan_manifest(&frozen)
            .unwrap();
        let conn = ScriptedToolPlaneConnection::new(
            vec![vec![]],
            vec![Ok(serde_json::json!({"content":[], "isError":false}))],
        );
        let fallback = case
            .get("runner_fallback_ms")
            .unwrap_or(&vectors["runner_fallback_ms"])
            .as_u64()
            .unwrap();
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft")
            .with_reply_fallback_for_test(fallback);
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let call = ToolCall {
            tool_call_id: ToolCallId("deadline-vector".into()),
            tool_name: "read".into(),
            input: case["arguments"].clone().into(),
            thought_signature: None,
            provider_state: None,
        };
        let ctx = batch_ctx("deadline-vector");
        plane
            .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
            .await
            .unwrap();
        assert!(matches!(
            plane.dispatch(&ctx, &call).await,
            DispatchResult::Completed(_)
        ));
        assert_eq!(
            *conn.dispatched_deadlines.lock().unwrap(),
            [Duration::from_millis(case["expected_ms"].as_u64().unwrap())],
            "{}",
            case["name"]
        );
        assert_eq!(
            *conn.dispatched_arguments.lock().unwrap(),
            [case["arguments"].clone()]
        );
        plane.batch_finished(&ctx).await.unwrap();
    }
}

/// A tool declared on the send (no fetch plan) waits its own frozen `reply.max_ms`, through
/// the same deadline arithmetic as a catalog tool: never below the runner's fallback, and not
/// shortened by the debug-only short fallback. A plan provider's declaration still wins.
#[tokio::test]
async fn send_declared_reply_reaches_dispatch_deadline() {
    let plan_reply = |max_ms: u64| {
        let tool = serde_json::json!({"name":"read", "description":"probe", "input_schema":{}, "module":"aft", "execution_mode":"parallel", "reply":{"max_ms":1_800_000}});
        let frozen: broca_records::record::PlanManifest = serde_json::from_value(serde_json::json!({
                "plan": r#"{"tool_items":[{"provider":"aft","preset":"worker"}]}"#,
                "plan_identity":"", "composition_digest":"", "fetches":[{
                    "provider":"aft", "catalog_digest":"", "tools":[tool], "reply":{"read":{"max_ms":max_ms}}
                }], "tools":[tool], "system_text":"", "system_digest":"", "join_version":1, "role_versions":{}
            }))
            .unwrap();
        ToolManifest::from_tools(frozen.tools.as_ref().clone())
            .with_plan_manifest(&frozen)
            .unwrap()
    };
    let declared = |reply: Option<u64>| {
        let mut tool = serde_json::json!({"name":"read", "description":"probe", "input_schema":{}, "module":"aft"});
        if let Some(max_ms) = reply {
            tool["reply"] = serde_json::json!({"max_ms": max_ms});
        }
        ToolManifest::from_tools(vec![serde_json::from_value(tool).unwrap()])
    };
    for (name, manifest, fallback, expected_ms) in [
        (
            "declared outlasts the short fallback",
            declared(Some(1_800_000)),
            1_200,
            1_800_000,
        ),
        (
            "declared outlasts the default",
            declared(Some(1_800_000)),
            120_000,
            1_800_000,
        ),
        (
            "a short declared reply keeps the fallback floor",
            declared(Some(500)),
            1_200,
            1_200,
        ),
        (
            "a short declared reply keeps the default floor",
            declared(Some(5_000)),
            120_000,
            120_000,
        ),
        (
            "no declared reply waits the fallback",
            declared(None),
            1_200,
            1_200,
        ),
        (
            "the plan provider's reply wins",
            plan_reply(300_000),
            1_200,
            300_000,
        ),
    ] {
        let conn = ScriptedToolPlaneConnection::new(
            vec![vec![]],
            vec![Ok(serde_json::json!({"content":[], "isError":false}))],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft")
            .with_reply_fallback_for_test(fallback);
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let call = ToolCall {
            tool_call_id: ToolCallId("declared-reply".into()),
            tool_name: "read".into(),
            input: serde_json::json!({}).into(),
            thought_signature: None,
            provider_state: None,
        };
        let ctx = batch_ctx("declared-reply");
        plane
            .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
            .await
            .unwrap();
        assert!(matches!(
            plane.dispatch(&ctx, &call).await,
            DispatchResult::Completed(_)
        ));
        assert_eq!(
            *conn.dispatched_deadlines.lock().unwrap(),
            [Duration::from_millis(expected_ms)],
            "{name}"
        );
        plane.batch_finished(&ctx).await.unwrap();
    }
}

#[async_trait]
impl ToolPlaneConnection for InProcessWireConnection {
    async fn tool_call_staged(
        &self,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: crate::connection::ToolReplyDeadline,
        preset: Option<&str>,
        call_key: Option<&str>,
        schema_pin: Option<&str>,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        <SubcConnection as ToolPlaneConnection>::tool_call_staged(
            &self.conn, route, call, deadline, preset, call_key, schema_pin, signals,
        )
        .await
    }

    async fn route_open_dispatch(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<&ScopeSelector>,
        shape: RouteShape,
        provider_held_installed: bool,
        aft_harness: Option<Arc<crate::provider_catalog::AftHarness>>,
    ) -> Result<RouteHandle, ConnError> {
        self.conn
            .route_open_dispatch_single_attempt(
                module_id,
                identity,
                scope,
                shape,
                provider_held_installed,
                aft_harness,
            )
            .await
    }

    async fn catalog_list(&self) -> Result<Arc<Vec<CatalogEntry>>, ConnError> {
        Ok(Arc::new(Vec::new()))
    }

    async fn route_open(
        &self,
        _module_id: &str,
        _identity: &BindIdentity,
        _scope: Option<&ScopeSelector>,
    ) -> Result<RouteHandle, ConnError> {
        Ok(self.route)
    }

    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
        Ok(Vec::new())
    }

    fn route_is_open(&self, _route: &RouteHandle) -> bool {
        true
    }

    async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError> {
        if self.close_on_wire {
            self.conn.route_close(route).await
        } else {
            Ok(())
        }
    }

    async fn tool_call(
        &self,
        route: &RouteHandle,
        name: &str,
        arguments: &serde_json::Value,
        tool_call_id: Option<&str>,
        deadline: Duration,
        preset: Option<&str>,
    ) -> Result<serde_json::Value, ConnError> {
        <SubcConnection as ToolPlaneConnection>::tool_call(
            &self.conn,
            route,
            name,
            arguments,
            tool_call_id,
            deadline,
            preset,
        )
        .await
    }
}

/// Dispatch one AFT call whose fake module answers `reply_after` after receiving it, and
/// report the outcome with the (virtual) time it took.
async fn dispatch_with_slow_aft_reply(
    tool_name: &str,
    input: serde_json::Value,
    reply_after: Duration,
) -> (DispatchResult, Duration) {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(7, 1);
    let plane = SubcToolPlane::with_connection(
        Arc::new(InProcessWireConnection {
            conn,
            route,
            close_on_wire: false,
        }),
        crate::tool_deadline::AFT_MODULE_ID,
    );
    plane
        .windows
        .lock()
        .unwrap()
        .insert(crate::tool_deadline::AFT_MODULE_ID.into(), 1);
    let call = ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId("call-slow".into()),
        tool_name: tool_name.into(),
        input: input.into(),
        provider_state: None,
    };
    let ctx = batch_ctx("batch-slow");
    let fake_module = tokio::spawn(async move {
        let (request, keepalive) = call_keepalive::bound_request(&mut module).await;
        assert!(!keepalive);
        sleep(reply_after).await;
        module.reply(
            &request,
            serde_json::to_vec(&serde_json::json!({
                "content": [{ "type": "text", "text": "tests passed" }],
                "isError": false
            }))
            .unwrap(),
        );
    });
    let started = Instant::now();
    let outcome = plane.dispatch(&ctx, &call).await;
    let elapsed = started.elapsed();
    fake_module.abort();
    (outcome, elapsed)
}

/// PAUSED TIME PROVES THE WIRING, NOT A WALL CLOCK: tokio auto-advances the clock whenever
/// every task is idle, so this shows that the deadline `dispatch` hands the real connection
/// outlasts a reply arriving after 150 s. Whether AFT really answers in time is AFT's
/// contract, pinned on their side.
///
/// Completed carries the tool's answer; SentButOutcomeUnknown is what the loop turns into
/// an Interrupted run, the failure this guards.
#[tokio::test(start_paused = true)]
async fn a_waiting_aft_bash_reply_after_120s_completes_the_call() {
    let (outcome, elapsed) = dispatch_with_slow_aft_reply(
        "bash",
        serde_json::json!({"command": "cargo test", "wait": true}),
        Duration::from_secs(150),
    )
    .await;
    assert!(
        matches!(&outcome, DispatchResult::Completed(result) if !result.is_error),
        "a waiting bash call must outlast a 150s reply; got {outcome:?}"
    );
    assert!(elapsed >= Duration::from_secs(150), "elapsed {elapsed:?}");
}

/// The control: the same slow reply to a bash call that did NOT ask to hold its reply is
/// still cut at the 120 s default, so the long deadline is not simply applied everywhere.
#[tokio::test(start_paused = true)]
async fn a_non_waiting_bash_reply_after_120s_still_times_out() {
    let (outcome, elapsed) = dispatch_with_slow_aft_reply(
        "bash",
        serde_json::json!({"command": "cargo test"}),
        Duration::from_secs(150),
    )
    .await;
    assert!(
        matches!(&outcome, DispatchResult::SentButOutcomeUnknown { reason } if reason.contains("timed out after 120s")),
        "a non-waiting call keeps the 120s deadline; got {outcome:?}"
    );
    assert!(
        elapsed >= crate::connection::TOOL_CALL_TIMEOUT && elapsed < Duration::from_secs(150),
        "elapsed {elapsed:?}"
    );
}

/// The outbound half of the scope carrier: a run admitted under a scope opens its provider
/// routes under exactly that scope, holds only on the daemon's two retryable scope
/// refusals, and resolves a call whose route closed by the scope's current state
/// (`scope.describe`) and by what the call had already done.
mod scope_carrier {
    #[tokio::test]
    async fn session_close_drops_unfinished_fence_results_and_eviction_keys() {
        let (_, plane, _) = rig(vec![successful_tool_reply("a"), successful_tool_reply("b")]);
        let first = ctx("same-batch", "first", "r", None);
        let second = ctx("same-batch", "second", "r", None);
        let read = call("read");
        assert_completed_success(plane.dispatch(&first, &read).await);
        assert_completed_success(plane.dispatch(&second, &read).await);
        plane.close_session_routes(&first.bind).await;
        {
            let fence = plane.fence.lock().unwrap();
            assert_eq!(fence.len(), 1);
            let first_key = SubcToolPlane::session_key(&first.bind);
            assert!(fence
                .by_batch
                .values()
                .flatten()
                .all(|(owner, _)| owner != &first_key));
            assert!(fence
                .get(&(SubcToolPlane::session_key(&second.bind), read.tool_call_id))
                .is_some());
        }
        plane.close_session_routes(&second.bind).await;
        assert!(plane.fence.lock().unwrap().by_batch.is_empty());
    }

    #[tokio::test]
    async fn sibling_rebind_never_sends_an_old_batch_to_a_new_agent() {
        for close_original in [false, true] {
            let (conn, plane, describer) = rig(vec![successful_tool_reply("must not send")]);
            describer.script("head", vec![live("head", 1, "agent-a")]);
            let (scoped, read) = started(&plane).await;
            let original = route_of(&plane, &scoped);
            describer.script("head", vec![live("head", 1, "agent-b")]);
            if close_original {
                conn.close(original);
            } else {
                plane.forget_route(&scoped, "aft", original);
                plane
                    .ensure_route(
                        &scoped.bind,
                        scoped.scope.as_ref(),
                        "aft",
                        &ToolOpenGuard::default(),
                    )
                    .await
                    .unwrap();
            }
            assert_drained(
                plane.dispatch(&scoped, &read).await,
                SCOPE_AGENT_CHANGED,
                false,
            );
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
            assert!(
                plane
                    .cached_route_channel_for_scope(&scoped.bind, scoped.scope.as_ref(), "aft")
                    .is_none(),
                "an incompatible fresh route must not remain cached for siblings"
            );
        }
    }

    use super::*;
    use crate::scope_describe::DescribedScope;
    use subc_protocol::scope::{ScopeAttributes, ScopeKind};

    const OWNER: &str = "prefrontal";

    fn scope(scope_ref: &str, epoch: u64) -> SessionScope {
        SessionScope {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: OWNER.into(),
            },
            agent_id_frozen: false,
            scope_ref: scope_ref.into(),
            scope_epoch: epoch,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        }
    }

    fn selector(scope_ref: &str, epoch: u64) -> ScopeSelector {
        ScopeSelector::new(
            Principal::Reserved {
                module_id: OWNER.into(),
            },
            scope_ref,
            Some(epoch),
        )
    }

    /// The stamp attributes named `name` in the fixture file. They live in a JSON fixture
    /// because `scope_stamp_source.rs` allows exactly one place in broca's source files
    /// to name the agent attribute, and that place is not a test.
    fn attributes(name: &str) -> ScopeAttributes {
        let all: HashMap<String, ScopeAttributes> =
            serde_json::from_str(include_str!("../../tests/fixtures/scope_attributes.json"))
                .unwrap();
        all.get(name).cloned().expect("a fixture attribute set")
    }

    fn stamp(scope_ref: &str, epoch: u64, attrs: &str) -> ScopeStamp {
        ScopeStamp::new(
            Principal::Reserved {
                module_id: OWNER.into(),
            },
            scope_ref,
            epoch,
            ScopeKind::Head,
        )
        .with_attributes(attributes(attrs))
        .with_owner_authorized(true)
    }

    fn live(scope_ref: &str, epoch: u64, attrs: &str) -> Result<DescribedScope, String> {
        Ok(DescribedScope {
            status: ScopeStatus::Live,
            scope_epoch: Some(epoch),
            owner_synced: true,
            owner_configured: true,
            stamp: Some(stamp(scope_ref, epoch, attrs)),
        })
    }

    fn parent_ended(scope_ref: &str, epoch: u64) -> Result<DescribedScope, String> {
        let mut answer = live(scope_ref, epoch, "none").unwrap();
        answer.stamp.as_mut().unwrap().parent_state = Some(ParentState::Ended);
        Ok(answer)
    }

    fn not_live(owner_synced: bool) -> Result<DescribedScope, String> {
        Ok(DescribedScope {
            status: ScopeStatus::NotLive,
            scope_epoch: None,
            owner_synced,
            owner_configured: true,
            stamp: None,
        })
    }

    fn ended(epoch: u64) -> Result<DescribedScope, String> {
        Ok(DescribedScope {
            status: ScopeStatus::Ended,
            scope_epoch: Some(epoch),
            owner_synced: true,
            owner_configured: true,
            stamp: None,
        })
    }

    /// `scope.describe` as a script per ref: answers in order, the last one repeating. A
    /// ref with no script is described live at the epoch its scope was frozen at.
    #[derive(Default)]
    struct ScriptedDescriber {
        scripts: Mutex<HashMap<String, VecDeque<Result<DescribedScope, String>>>>,
        epochs: Mutex<HashMap<String, u64>>,
        calls: AtomicUsize,
    }

    impl ScriptedDescriber {
        fn script(&self, scope_ref: &str, answers: Vec<Result<DescribedScope, String>>) {
            self.scripts
                .lock()
                .unwrap()
                .insert(scope_ref.into(), answers.into());
        }
    }

    #[async_trait]
    impl ScopeDescriber for ScriptedDescriber {
        async fn describe(
            &self,
            _owner: &Principal,
            scope_ref: &str,
        ) -> Result<DescribedScope, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut scripts = self.scripts.lock().unwrap();
            match scripts.get_mut(scope_ref) {
                Some(answers) if answers.len() > 1 => answers.pop_front().unwrap(),
                Some(answers) => answers.front().cloned().unwrap(),
                None => {
                    let epoch = self.epochs.lock().unwrap().get(scope_ref).copied();
                    live(scope_ref, epoch.unwrap_or(1), "none")
                }
            }
        }
    }

    fn ctx(batch: &str, session: &str, run: &str, scope: Option<SessionScope>) -> BatchCtx {
        BatchCtx {
            media_capture: None,
            batch_id: BatchId(batch.into()),
            bind: BindIdentity::new("/tmp/scope-carrier", "test", session),
            run_id: RunId(run.into()),
            scope,
        }
    }

    /// A connection with enough empty catalog answers for any test here, a plane over it
    /// whose `aft` window is already known, and a scripted describer installed on it.
    fn rig(
        tool_replies: Vec<Result<serde_json::Value, ConnError>>,
    ) -> (
        Arc<ScriptedToolPlaneConnection>,
        SubcToolPlane,
        Arc<ScriptedDescriber>,
    ) {
        let conn = ScriptedToolPlaneConnection::new(vec![Vec::new(); 16], tool_replies);
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 4);
        let describer = Arc::new(ScriptedDescriber::default());
        for (scope_ref, epoch) in [("head-a", 3), ("head-b", 9)] {
            describer
                .epochs
                .lock()
                .unwrap()
                .insert(scope_ref.into(), epoch);
        }
        plane.set_scope_describer(describer.clone());
        (conn, plane, describer)
    }

    fn manifest() -> ToolManifest {
        ToolManifest::from_tools(vec![request_tool("read", Some("aft"))])
    }

    fn route_of(plane: &SubcToolPlane, ctx: &BatchCtx) -> RouteHandle {
        let channel = plane
            .cached_route_channel_for_scope(&ctx.bind, ctx.scope.as_ref(), "aft")
            .expect("the batch opened a route");
        RouteHandle::synthetic(channel)
    }

    /// Start a scoped batch of one `read` call under `head` epoch 1, as run `r`.
    async fn started(plane: &SubcToolPlane) -> (BatchCtx, ToolCall) {
        let scoped = ctx("r-b1", "s", "r", Some(scope("head", 1)));
        let read = call("read");
        plane
            .batch_started(&scoped, std::slice::from_ref(&read), &manifest())
            .await
            .expect("the scoped batch starts");
        (scoped, read)
    }

    fn assert_drained(outcome: DispatchResult, reason: &str, sent: bool) {
        match outcome {
            DispatchResult::ScopeDrained {
                reason: named,
                sent: was_sent,
            } => {
                assert_eq!(named, reason);
                assert_eq!(was_sent, sent, "sent");
            }
            other => panic!("expected ScopeDrained({reason}, sent: {sent}), got {other:?}"),
        }
    }

    /// Two sessions under two scopes, a third unscoped, and an unscoped run of the first
    /// session: each route opens under the scope of the run it serves and no other, and no
    /// run reuses a route opened under a scope it was not admitted under.
    #[tokio::test]
    async fn no_route_is_opened_or_reused_under_a_scope_the_run_was_not_admitted_under() {
        let (conn, plane, _) = rig((0..4).map(|_| successful_tool_reply("ok")).collect());
        let a = ctx("a-b1", "session-a", "run-a", Some(scope("head-a", 3)));
        let b = ctx("b-b1", "session-b", "run-b", Some(scope("head-b", 9)));
        let c = ctx("c-b1", "session-c", "run-c", None);
        // Session A again, in a run that was not admitted under A's scope.
        let a_unscoped = ctx("a-b2", "session-a", "run-a2", None);
        let read = call("read");
        for ctx in [&a, &b, &c, &a_unscoped] {
            plane
                .batch_started(ctx, std::slice::from_ref(&read), &manifest())
                .await
                .unwrap();
            assert_completed_success(plane.dispatch(ctx, &read).await);
        }

        assert_eq!(
            conn.opened_scopes(),
            vec![
                ("session-a".to_owned(), Some(selector("head-a", 3))),
                ("session-b".to_owned(), Some(selector("head-b", 9))),
                ("session-c".to_owned(), None),
                ("session-a".to_owned(), None),
            ],
            "every route opens under the scope of the run it serves, with the frozen epoch"
        );
        assert_eq!(
            plane.cached_route_channel_for_scope(&b.bind, a.scope.as_ref(), "aft"),
            None,
            "no route of session B exists under session A's scope"
        );
        assert_ne!(
            plane.cached_route_channel_for_scope(&a.bind, a.scope.as_ref(), "aft"),
            plane.cached_route_channel_for_scope(&a.bind, None, "aft"),
            "A's unscoped run did not reuse the route A's scoped run opened"
        );
    }

    /// A warm bind for a scoped run opens under that scope and caches the route only there.
    #[tokio::test]
    async fn a_scoped_warm_bind_opens_and_caches_under_its_scope_only() {
        let (conn, plane, _) = rig(Vec::new());
        let bind = BindIdentity::new("/tmp/scope-carrier", "test", "warm");
        let head = scope("head-w", 1);
        plane
            .warm_bind_for_lifecycle(
                &bind,
                Some(&head),
                Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            conn.opened_scopes(),
            vec![("warm".to_owned(), Some(selector("head-w", 1)))]
        );
        assert!(plane
            .cached_route_channel_for_scope(&bind, Some(&head), "aft")
            .is_some());
        assert_eq!(plane.cached_route_channel_for_module(&bind, "aft"), None);
        assert_eq!(
            plane.tool_routes_health(std::slice::from_ref(&bind))["cached_routes"],
            1
        );
        plane.close_session_routes(&bind).await;
        assert_eq!(
            plane.tool_routes_health(&[])["routes_for_closed_sessions"],
            0
        );
        assert_eq!(plane.tool_routes_health(&[])["cached_routes"], 0);
    }

    /// On a daemon without `scopes/v1` a scoped call is refused `scope_unsupported` and no
    /// route opens, through batch start, dispatch and the warm bind alike. An unscoped
    /// session on the same daemon is unaffected.
    #[tokio::test]
    async fn a_scoped_session_on_a_daemon_without_scopes_v1_never_opens_a_route() {
        let (conn, plane, _) = rig(vec![successful_tool_reply("ok")]);
        conn.capabilities.lock().unwrap().clear();
        let scoped = ctx("s-b1", "scoped", "run-1", Some(scope("head", 1)));
        let read = call("read");

        let refused = plane
            .batch_started(&scoped, std::slice::from_ref(&read), &manifest())
            .await
            .expect_err("a scoped batch cannot start");
        assert!(
            matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == "scope_unsupported"),
            "{refused:?}"
        );
        match plane.dispatch(&scoped, &read).await {
            DispatchResult::NotSent { reason } => {
                assert!(reason.starts_with("scope_unsupported: "), "{reason}")
            }
            other => panic!("expected scope_unsupported, got {other:?}"),
        }
        let warm = plane
            .warm_bind_for_lifecycle(
                &scoped.bind,
                scoped.scope.as_ref(),
                Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await
            .expect_err("a scoped warm bind cannot open");
        assert!(
            matches!(&warm, ToolPlaneError::ScopeRefused { code, .. } if code == "scope_unsupported")
        );
        assert_eq!(
            conn.route_open_calls.load(Ordering::SeqCst),
            0,
            "nothing was opened, scoped or not"
        );
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);

        let unscoped = ctx("u-b1", "unscoped", "run-2", None);
        plane
            .batch_started(&unscoped, std::slice::from_ref(&read), &manifest())
            .await
            .unwrap();
        assert_completed_success(plane.dispatch(&unscoped, &read).await);
        assert_eq!(conn.opened_scopes(), vec![("unscoped".to_owned(), None)]);
    }

    /// Without a module connection to describe the scope over, a scoped open cannot learn
    /// its route's stamp, so nothing opens and the call is `scope_unverifiable`.
    #[tokio::test]
    async fn a_scoped_open_without_a_describer_opens_nothing() {
        let conn = ScriptedToolPlaneConnection::new(vec![Vec::new(); 4], Vec::new());
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let scoped = ctx("b1", "s", "r", Some(scope("head", 1)));
        let refused = plane
            .batch_started(&scoped, &[call("read")], &manifest())
            .await
            .expect_err("no describer");
        assert!(
            matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == SCOPE_UNVERIFIABLE),
            "{refused:?}"
        );
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 0);
    }

    /// `scope_not_synced` on route.open holds with backoff and then opens.
    #[tokio::test(start_paused = true)]
    async fn scope_not_synced_holds_with_backoff_until_the_owner_syncs() {
        let (conn, plane, _) = rig(Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![
                RouteOpenScript::rejected("scope_not_synced — owner has not synced"),
                RouteOpenScript::rejected("scope_not_synced — owner has not synced"),
                RouteOpenScript::default(),
            ],
        );
        let started_at = Instant::now();
        started(&plane).await;
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 3);
        // Two waits, 100 ms then 200 ms, before the third attempt: a backoff, not a busy
        // loop.
        assert!(started_at.elapsed() >= Duration::from_millis(300));
    }

    /// `scope_changed` on route.open re-opens at once, with no wait.
    #[tokio::test(start_paused = true)]
    async fn scope_changed_reopens_at_once() {
        let (conn, plane, _) = rig(Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![
                RouteOpenScript::rejected("scope_changed — record changed before commit"),
                RouteOpenScript::default(),
            ],
        );
        let started_at = Instant::now();
        started(&plane).await;
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            started_at.elapsed(),
            Duration::ZERO,
            "no wait before the re-open"
        );
    }

    /// A route.open hold that runs past `SCOPE_SYNC_HOLD` gives `scope_unverifiable`, at the
    /// bound, and nothing is dispatched.
    #[tokio::test(start_paused = true)]
    async fn a_hold_past_the_bound_is_scope_unverifiable_and_dispatches_nothing() {
        let (conn, plane, _) = rig(vec![successful_tool_reply("never sent")]);
        conn.script_route_open_sequence(
            "aft",
            vec![RouteOpenScript::rejected(
                "scope_not_synced — owner has not synced",
            )],
        );
        let scoped = ctx("b1", "s", "r", Some(scope("head", 1)));
        let read = call("read");

        let started_at = Instant::now();
        let refused = plane
            .batch_started(&scoped, std::slice::from_ref(&read), &manifest())
            .await
            .expect_err("the hold runs out");
        let held = started_at.elapsed();
        assert!(
            matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == SCOPE_UNVERIFIABLE),
            "{refused:?}"
        );
        assert!(
            held >= SCOPE_SYNC_HOLD && held < SCOPE_SYNC_HOLD + Duration::from_secs(3),
            "held {held:?}"
        );

        match plane.dispatch(&scoped, &read).await {
            DispatchResult::NotSent { reason } => {
                assert!(reason.starts_with("scope_unverifiable: "), "{reason}")
            }
            other => panic!("expected scope_unverifiable, got {other:?}"),
        }
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            0,
            "nothing dispatched"
        );
    }

    /// Every route.open scope refusal outside `scope_not_synced` and `scope_changed` is
    /// terminal: answered by its own name after exactly one attempt, never held.
    #[tokio::test(start_paused = true)]
    async fn a_terminal_scope_refusal_is_never_retried_or_held() {
        for code in [
            "scope_ended",
            "scope_parent_ended",
            "scope_carrier_removed",
            "scope_epoch_ended",
            "scope_kind_changed",
            "scope_not_live",
            "scope_owner_mismatch",
            "scope_not_carrier",
            "scope_epoch_required",
        ] {
            let (conn, plane, _) = rig(Vec::new());
            conn.script_route_open_sequence(
                "aft",
                vec![RouteOpenScript::rejected(format!("{code} — refused"))],
            );
            let scoped = ctx("b1", "s", "r", Some(scope("head", 1)));
            let started_at = Instant::now();
            let refused = plane
                .batch_started(&scoped, &[call("read")], &manifest())
                .await
                .expect_err("a terminal refusal");
            assert!(
                matches!(&refused, ToolPlaneError::ScopeRefused { code: named, .. } if named == code),
                "{code}: {refused:?}"
            );
            assert_eq!(
                conn.route_open_calls.load(Ordering::SeqCst),
                1,
                "{code} was retried"
            );
            assert_eq!(started_at.elapsed(), Duration::ZERO, "{code} was held");
        }
    }

    /// `scope_not_live` refuses the rest of the current run's calls at once, opening nothing
    /// more; a later run of the same session is judged afresh and admitted.
    #[tokio::test]
    async fn scope_not_live_refuses_the_rest_of_the_run_and_a_later_run_is_admitted() {
        let (conn, plane, _) = rig(vec![successful_tool_reply("ok")]);
        conn.script_route_open_sequence(
            "aft",
            vec![
                RouteOpenScript::rejected("scope_not_live — not in the owner's set"),
                RouteOpenScript::default(),
            ],
        );
        let read = call("read");
        let first = ctx("r1-b1", "s", "r1", Some(scope("head", 1)));
        assert!(plane
            .batch_started(&first, std::slice::from_ref(&read), &manifest())
            .await
            .is_err());
        let later_batch = ctx("r1-b2", "s", "r1", Some(scope("head", 1)));
        let refused = plane
            .batch_started(&later_batch, std::slice::from_ref(&read), &manifest())
            .await
            .expect_err("the rest of the run is refused");
        assert!(
            matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == "scope_not_live")
        );
        match plane.dispatch(&later_batch, &read).await {
            DispatchResult::ScopeDrained {
                reason,
                sent: false,
            } => {
                assert_eq!(reason, "scope_not_live")
            }
            other => panic!("expected scope_not_live, got {other:?}"),
        }
        assert_eq!(
            conn.route_open_calls.load(Ordering::SeqCst),
            1,
            "the refused run opened nothing more"
        );

        let next_run = ctx("r2-b1", "s", "r2", Some(scope("head", 1)));
        plane
            .batch_started(&next_run, std::slice::from_ref(&read), &manifest())
            .await
            .expect("a later run is admitted normally");
        assert_completed_success(plane.dispatch(&next_run, &read).await);
    }

    /// The open is bracketed by two describes. When they disagree on the agent, the route
    /// may have been bound under either stamp: it is closed and opened again, and only the
    /// agreeing pair becomes the route's stamp, the one a later re-send is compared to.
    #[tokio::test]
    async fn a_rebind_between_the_bracketing_describes_is_not_recorded_as_the_routes_stamp() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("re-sent")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-b"),
                live("head", 1, "agent-b"),
                live("head", 1, "agent-b"),
            ],
        );
        let (scoped, read) = started(&plane).await;
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            conn.closed_routes.lock().unwrap().len(),
            1,
            "the route bound during the rebind was closed"
        );

        // The recorded stamp is agent-b's: after a close with the scope still on agent-b,
        // the call is re-sent. Had agent-a's been recorded, it would be drained instead.
        conn.close(route_of(&plane, &scoped));
        assert_completed_success(plane.dispatch(&scoped, &read).await);
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    }

    /// A call not yet sent whose route closed while the scope, or its parent, is gone:
    /// `route_drained` naming what describe said, never re-dispatched, and the run's next
    /// batch stops. A describe that answers for a different epoch counts as ended.
    #[tokio::test]
    async fn an_unsent_call_whose_scope_is_gone_drains_later_tool_batches() {
        for (answer, reason) in [
            (ended(1), "scope_ended"),
            (not_live(true), "scope_not_live"),
            (live("head", 2, "none"), "scope_ended"),
            (parent_ended("head", 1), "scope_parent_ended"),
        ] {
            // One reply is scripted so a plane that wrongly re-dispatched would complete the
            // call instead of failing on a missing script.
            let (conn, plane, describer) = rig(vec![successful_tool_reply("re-dispatched")]);
            describer.script(
                "head",
                vec![live("head", 1, "none"), live("head", 1, "none"), answer],
            );
            let (scoped, read) = started(&plane).await;
            conn.close(route_of(&plane, &scoped));

            assert_drained(plane.dispatch(&scoped, &read).await, reason, false);
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0, "{reason}: sent");
            assert_eq!(
                conn.route_open_calls.load(Ordering::SeqCst),
                1,
                "{reason}: a fresh route was opened for the drained call"
            );
            plane.batch_finished(&scoped).await.unwrap();

            let next = ctx("r-b2", "s", "r", Some(scope("head", 1)));
            let stopped = plane
                .batch_started(&next, &[call("read")], &manifest())
                .await;
            assert!(
                matches!(&stopped, Err(ToolPlaneError::ScopeDrained { reason: named }) if named == reason),
                "{reason}: the run's next batch must stop, got {stopped:?}"
            );
        }
    }

    /// A session runs one run at a time, so a batch of a new run means the session's earlier
    /// runs have ended: what they latched is dropped then, not kept until the session closes.
    /// Another session's latches, and the current run's, stay.
    #[tokio::test]
    async fn a_new_run_drops_the_latches_of_its_sessions_ended_runs() {
        let (_conn, plane, _describer) = rig(Vec::new());
        let refused = || RunScopeLatch::Refused {
            code: SCOPE_NOT_LIVE.into(),
            detail: "scripted".into(),
        };
        let ended = ctx("r-b1", "s", "r", Some(scope("head", 1)));
        let other_session = ctx("x-b1", "other", "x", Some(scope("head", 1)));
        plane.latch_run(&ended, refused());
        plane.latch_run(&other_session, refused());

        let next = ctx("r2-b1", "s", "r2", Some(scope("head", 1)));
        plane
            .batch_started(&next, &[call("read")], &manifest())
            .await
            .expect("the new run's batch starts");
        plane.latch_run(&next, refused());
        let latched: std::collections::BTreeSet<_> = plane
            .run_latches
            .lock()
            .unwrap()
            .keys()
            .map(|(_, run)| run.0.clone())
            .collect();
        assert_eq!(
            latched,
            std::collections::BTreeSet::from(["r2".to_owned(), "x".to_owned()])
        );
    }

    /// A describe answering for another epoch than the route was opened under means the
    /// scope we were admitted under ended (a newer session reused the ref).
    #[test]
    fn a_describe_for_a_different_epoch_counts_as_ended() {
        for answer in [live("head", 2, "none").unwrap(), {
            let mut answer = live("head", 1, "none").unwrap();
            answer.scope_epoch = Some(2);
            answer
        }] {
            assert!(
                matches!(read_describe(1, &answer), ScopeReading::Gone(SCOPE_ENDED)),
                "{answer:?}"
            );
        }
        assert!(matches!(
            read_describe(1, &live("head", 1, "none").unwrap()),
            ScopeReading::Live(_)
        ));
    }

    /// A delegation narrowing leaves the scope live under the same epoch and agent, so the
    /// call not yet sent is re-sent on a fresh route under the current stamp.
    #[tokio::test]
    async fn a_delegation_narrowing_resends_the_unsent_call_on_a_fresh_route() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("sent once")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a-delegating"),
                live("head", 1, "agent-a-delegating"),
                live("head", 1, "agent-a"),
            ],
        );
        let (scoped, read) = started(&plane).await;
        conn.close(route_of(&plane, &scoped));

        assert_completed_success(plane.dispatch(&scoped, &read).await);
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            1,
            "sent exactly once"
        );
        assert_eq!(
            conn.route_open_calls.load(Ordering::SeqCst),
            2,
            "on a fresh route"
        );
    }

    /// A call refused before delivery because its module is restarting is re-sent, once the scope
    /// reads live again, as the request its first attempt carried: with the schema pin the plan
    /// froze for the call's own module, which is not the plane's default one here.
    #[tokio::test]
    async fn a_scoped_resend_keeps_the_calls_schema_pin() {
        let (conn, plane, _) = rig(vec![
            Err(ConnError::Rejected(crate::connection::decode_error_body(
                br#"{"code":"module_reloading","message":"draining endpoint"}"#,
            ))),
            successful_tool_reply("sent once"),
        ]);
        let scoped = ctx("r-b1", "s", "r", Some(scope("head", 1)));
        let read = call("read");
        plane
            .batch_started(
                &scoped,
                std::slice::from_ref(&read),
                &pinned_plan("plexus", "read"),
            )
            .await
            .expect("the scoped batch starts");

        assert_completed_success(plane.dispatch(&scoped, &read).await);
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
        let pin = broca_core::projection::schema_pin_text("read", &frozen_pin());
        assert!(pin.is_some());
        // The attempt refused while the module drained, then its re-send.
        assert_eq!(
            *conn.dispatched_schema_pins.lock().unwrap(),
            [pin.clone(), pin]
        );
        let ids = conn.dispatched_ids.lock().unwrap().clone();
        assert_eq!(ids.len(), 2, "one refused attempt, one re-send: {ids:?}");
        assert_eq!(ids[0], ids[1]);
    }

    /// A rebind to another agent leaves the scope live, but the call was composed for the old
    /// agent: `route_drained` as `scope_agent_changed`, and nothing is dispatched.
    #[tokio::test]
    async fn a_rebound_agent_drains_the_unsent_call_and_dispatches_nothing() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-a"),
                live("head", 1, "agent-b"),
            ],
        );
        let (scoped, read) = started(&plane).await;
        conn.close(route_of(&plane, &scoped));

        assert_drained(
            plane.dispatch(&scoped, &read).await,
            SCOPE_AGENT_CHANGED,
            false,
        );
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            0,
            "nothing dispatched"
        );
    }

    /// The route re-opened for a call the rebind then drains is live, unlike the closed one
    /// it replaced: it is closed (GOODBYE), not just dropped from the cache, so the daemon
    /// frees the channel instead of holding it until the session ends.
    #[tokio::test]
    async fn a_rebound_agent_closes_the_fresh_route_it_refused_to_send_on() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-a"),
                live("head", 1, "agent-b"),
            ],
        );
        let (scoped, read) = started(&plane).await;
        let first = route_of(&plane, &scoped);
        conn.close(first);

        assert_drained(
            plane.dispatch(&scoped, &read).await,
            SCOPE_AGENT_CHANGED,
            false,
        );
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
        assert!(
            plane
                .cached_route_channel_for_scope(&scoped.bind, scoped.scope.as_ref(), "aft")
                .is_none(),
            "the refused route is no longer cached"
        );
        wait_for_closed_routes(&conn, 1).await;
        let closed = conn.closed_routes.lock().unwrap().clone();
        assert_ne!(
            closed[0].channel, first.channel,
            "the fresh route was closed"
        );
    }

    /// When the daemon describes the scope as not live because its owner has not synced
    /// since the daemon started, the unsent call waits, as a `scope_not_synced` open does.
    /// It is re-sent once the owner syncs; if the hold runs out first it is drained as
    /// `scope_unverifiable` and never sent.
    #[tokio::test(start_paused = true)]
    async fn an_unsynced_owner_holds_the_unsent_call_inside_the_scope_sync_hold() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("after the sync")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-a"),
                not_live(false),
                not_live(false),
            ],
        );
        let (scoped, read) = started(&plane).await;
        conn.close(route_of(&plane, &scoped));
        let described_before = describer.calls.load(Ordering::SeqCst);
        let mut dispatch = Box::pin(plane.dispatch(&scoped, &read));
        assert!(futures::poll!(&mut dispatch).is_pending());
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            0,
            "unsynced calls stay unsent"
        );
        assert!(describer.calls.load(Ordering::SeqCst) > described_before);
        describer.script("head", vec![live("head", 1, "agent-a")]);
        assert_completed_success(dispatch.await);
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);

        let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-a"),
                not_live(false),
            ],
        );
        let (scoped, read) = started(&plane).await;
        conn.close(route_of(&plane, &scoped));
        let started_at = Instant::now();
        assert_drained(
            plane.dispatch(&scoped, &read).await,
            SCOPE_UNVERIFIABLE,
            false,
        );
        assert!(started_at.elapsed() >= SCOPE_SYNC_HOLD, "held to the bound");
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
    }

    /// Run cancel while an unsent scoped call holds for an unsynced owner (its route closed
    /// and `scope.describe` says the owner has not synced) ends the hold at once: the call
    /// is stopped before send, not held to the scope-sync bound and then drained.
    #[tokio::test(start_paused = true)]
    async fn run_cancel_ends_the_describe_hold_of_an_unsent_scoped_call() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-a"),
                not_live(false),
            ],
        );
        let (scoped, read) = started(&plane).await;
        conn.close(route_of(&plane, &scoped));
        let cancel = CancellationToken::new();
        let guard = ToolOpenGuard::new(Some(&cancel), None);
        let started_at = Instant::now();
        let interrupt = async {
            sleep(Duration::from_secs(1)).await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(
            plane.dispatch_guarded(&scoped, &read, None, &guard),
            interrupt
        );
        let waited = started_at.elapsed();
        assert!(
            waited < Duration::from_secs(2),
            "the cancel ends the hold: {waited:?}"
        );
        assert!(
            matches!(&result, DispatchResult::NotSent { reason } if reason == "dispatch_stopped_before_send"),
            "{result:?}"
        );
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            0,
            "nothing was sent"
        );
    }

    /// Run cancel while a scoped batch's open holds on its first describe (the owner has not
    /// synced) ends the hold at once: the batch fails as interrupted and nothing opens.
    #[tokio::test(start_paused = true)]
    async fn run_cancel_ends_the_describe_hold_before_a_scoped_open() {
        let (conn, plane, describer) = rig(Vec::new());
        describer.script("head", vec![not_live(false)]);
        let scoped = ctx("r-b1", "s", "r", Some(scope("head", 1)));
        let cancel = CancellationToken::new();
        let guard = ToolOpenGuard::new(Some(&cancel), None);
        let started_at = Instant::now();
        let interrupt = async {
            sleep(Duration::from_secs(1)).await;
            cancel.cancel();
        };
        let calls = [call("read")];
        let manifest = manifest();
        let (result, ()) = tokio::join!(
            plane.batch_started_guarded(&scoped, &calls, &manifest, &guard),
            interrupt
        );
        let waited = started_at.elapsed();
        assert!(
            waited < Duration::from_secs(2),
            "the cancel ends the hold: {waited:?}"
        );
        assert!(
            matches!(&result, Err(ToolPlaneError::Unavailable(message)) if message.contains("run cancelled or stopped")),
            "{result:?}"
        );
        assert_eq!(
            conn.route_open_calls.load(Ordering::SeqCst),
            0,
            "nothing opened"
        );
    }

    /// When asking the daemon about the scope fails, nothing shows that a fresh route would
    /// carry the same epoch and agent, so the unsent call is drained as `scope_unverifiable`
    /// and never re-sent.
    #[tokio::test]
    async fn an_unanswered_describe_drains_the_unsent_call() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-a"),
                Err("module connection closed".into()),
            ],
        );
        let (scoped, read) = started(&plane).await;
        conn.close(route_of(&plane, &scoped));
        assert_drained(
            plane.dispatch(&scoped, &read).await,
            SCOPE_UNVERIFIABLE,
            false,
        );
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_upgrade_cut_does_not_drain_an_unsent_scoped_call() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-a"),
                Err("scope.describe was refused while the daemon was switching".into()),
            ],
        );
        let (scoped, read) = started(&plane).await;
        let ceiling = tokio::time::Instant::now() + Duration::from_secs(2);
        crate::daemon_upgrade::announce(ceiling);
        conn.close(route_of(&plane, &scoped));
        let mut dispatch = std::pin::pin!(plane.dispatch(&scoped, &read));
        assert!(
            futures::poll!(&mut dispatch).is_pending(),
            "the switch is not evidence the scope ended"
        );
        crate::daemon_upgrade::resumed(ceiling);
        assert!(matches!(dispatch.await, DispatchResult::NotSent { .. }));
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            0,
            "scope compatibility is still required"
        );
        crate::daemon_upgrade::abandon();
    }

    #[tokio::test(start_paused = true)]
    async fn scope_describe_loss_can_race_the_upgrade_notice() {
        let (conn, plane, describer) = rig(vec![successful_tool_reply("after resume")]);
        describer.script(
            "head",
            vec![
                live("head", 1, "agent-a"),
                live("head", 1, "agent-a"),
                Err("module connection closed".into()),
            ],
        );
        let (scoped, read) = started(&plane).await;
        crate::daemon_upgrade::expect_upgrade_notices(true);
        conn.close(route_of(&plane, &scoped));
        let mut dispatch = std::pin::pin!(plane.dispatch(&scoped, &read));
        assert!(
            futures::poll!(&mut dispatch).is_pending(),
            "unanswered describe must wait for the racing notice"
        );
        let ceiling = tokio::time::Instant::now() + Duration::from_secs(2);
        crate::daemon_upgrade::announce(ceiling);
        assert!(futures::poll!(&mut dispatch).is_pending());
        describer.script("head", vec![live("head", 1, "agent-a")]);
        crate::daemon_upgrade::resumed(ceiling);
        assert_completed_success(dispatch.await);
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
        crate::daemon_upgrade::abandon();
    }

    #[tokio::test(start_paused = true)]
    async fn an_upgrade_missing_sibling_stamp_is_not_scope_drained() {
        let (conn, plane, describer) = rig(vec![]);
        describer.script("head", vec![live("head", 1, "agent-a")]);
        let (scoped, read) = started(&plane).await;
        let route = route_of(&plane, &scoped);
        plane.forget_route(&scoped, "aft", route);
        let ceiling = tokio::time::Instant::now() + Duration::from_secs(2);
        crate::daemon_upgrade::announce(ceiling);
        crate::daemon_upgrade::resumed(ceiling);
        let result = plane
            .resend_or_drain(
                &scoped,
                &read,
                "aft",
                route,
                None,
                TOOL_DISPATCH_PATIENCE,
                &ToolOpenGuard::default(),
                (
                    SubcToolPlane::session_key(&scoped.bind),
                    read.tool_call_id.clone(),
                ),
                None,
                &[],
            )
            .await;
        assert!(
            matches!(result, DispatchResult::NotSent { .. }),
            "a sibling's missing stamp after resume is not a scope drain: {result:?}"
        );
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
        crate::daemon_upgrade::abandon();
    }

    /// A call already sent when its route closed: the outcome is unknown whatever the
    /// scope's state. Describe names why when the scope or parent is gone or the agent
    /// changed; a live, unchanged scope leaves the ordinary unknown outcome.
    #[tokio::test]
    async fn a_call_sent_when_its_route_closed_is_outcome_unknown() {
        for (answer, reason) in [
            (ended(1), Some("scope_ended")),
            (parent_ended("head", 1), Some("scope_parent_ended")),
            (live("head", 1, "agent-b"), Some(SCOPE_AGENT_CHANGED)),
            (live("head", 1, "agent-a"), None),
        ] {
            let (conn, plane, describer) = rig(Vec::new());
            describer.script(
                "head",
                vec![
                    live("head", 1, "agent-a"),
                    live("head", 1, "agent-a"),
                    answer,
                ],
            );
            let (scoped, read) = started(&plane).await;
            *conn.close_during_call.lock().unwrap() = true;

            let outcome = plane.dispatch(&scoped, &read).await;
            match reason {
                Some(reason) => assert_drained(outcome, reason, true),
                None => assert!(
                    matches!(outcome, DispatchResult::SentButOutcomeUnknown { .. }),
                    "{outcome:?}"
                ),
            }
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
        }
    }

    /// With nothing in flight, a closed route whose scope or parent is gone stops the rest
    /// of the run at its next batch, before anything opens. A live scope is ordinary route
    /// loss: the next batch re-opens. A later run is judged afresh.
    #[tokio::test]
    async fn a_close_with_nothing_in_flight_drains_later_tool_batches_only_if_the_scope_is_gone() {
        for (answer, reason) in [
            (ended(1), Some("scope_ended")),
            (parent_ended("head", 1), Some("scope_parent_ended")),
            (live("head", 1, "agent-b"), None),
        ] {
            let (conn, plane, describer) =
                rig((0..3).map(|_| successful_tool_reply("ok")).collect());
            describer.script(
                "head",
                vec![
                    live("head", 1, "agent-a"),
                    live("head", 1, "agent-a"),
                    answer.clone(),
                ],
            );
            let (first, read) = started(&plane).await;
            assert_completed_success(plane.dispatch(&first, &read).await);
            plane.batch_finished(&first).await.unwrap();
            conn.close(route_of(&plane, &first));

            let next = ctx("r-b2", "s", "r", Some(scope("head", 1)));
            let started_next = plane
                .batch_started(&next, std::slice::from_ref(&read), &manifest())
                .await;
            let Some(reason) = reason else {
                started_next.expect("a live scope is ordinary route loss");
                assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
                continue;
            };
            match started_next {
                Err(ToolPlaneError::ScopeDrained { reason: named }) => {
                    assert_eq!(named, reason)
                }
                other => panic!("{reason}: expected the run to end, got {other:?}"),
            }
            assert_eq!(
                conn.route_open_calls.load(Ordering::SeqCst),
                1,
                "{reason}: the ended run opened nothing more"
            );

            describer.script("head", vec![live("head", 1, "agent-a")]);
            let later_run = ctx("r2-b1", "s", "r2", Some(scope("head", 1)));
            plane
                .batch_started(&later_run, std::slice::from_ref(&read), &manifest())
                .await
                .expect("a later run is judged afresh");
        }
    }

    /// A provider that already received the call and refuses it `scope_not_synced` gets the
    /// refusal recorded as the call's error result, and the call is not sent again. The
    /// provider holds the first request, and broca sends no durable call identity that
    /// would let it recognise a second send as the same request.
    #[tokio::test(start_paused = true)]
    async fn a_provider_scope_not_synced_refusal_is_recorded_and_never_resent() {
        let (conn, plane, _) = rig(vec![
            Err(ConnError::Rejected(
                "scope_not_synced — the scope's owner has not synced".into(),
            )),
            successful_tool_reply("a second send"),
        ]);
        let (scoped, read) = started(&plane).await;
        match plane.dispatch(&scoped, &read).await {
            DispatchResult::Completed(result) => {
                assert!(result.is_error);
                let ToolOutput::Error { message, .. } = &result.output else {
                    panic!("expected an error result, got {result:?}");
                };
                assert!(message.contains("scope_not_synced"), "{message}");
            }
            other => panic!("expected the refusal as the call's result, got {other:?}"),
        }
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1, "never re-sent");
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
    }

    /// A provider now serving another major of the tool refuses the call `tool_major_changed`.
    /// The refusal is the call's error result, never re-sent, and comes back with its code and
    /// a detail naming the refusing provider beside the refusal's own pins, which the call's
    /// outcome records for the control lane.
    #[tokio::test(start_paused = true)]
    async fn a_major_change_refusal_is_returned_with_its_provider_and_pins() {
        let refusal = serde_json::json!({
            "tool": "read",
            "expected": format!("tp2:read:{}:2", "a".repeat(64)),
            "current": format!("tp2:read:{}:3", "b".repeat(64)),
        });
        let body = serde_json::json!({
            "code": "tool_major_changed",
            "message": "this tool changed incompatibly",
            "detail": refusal,
        });
        let (conn, plane, _) = rig(vec![
            Err(ConnError::Rejected(
                crate::connection::RejectedMessage::from_error_frame_body_for_test(
                    &serde_json::to_vec(&body).unwrap(),
                ),
            )),
            successful_tool_reply("a second send"),
        ]);
        let (scoped, read) = started(&plane).await;
        match plane.dispatch(&scoped, &read).await {
            DispatchResult::Refused {
                result,
                code,
                detail,
            } => {
                assert!(result.is_error);
                assert_eq!(code, "tool_major_changed");
                assert_eq!(
                    detail,
                    serde_json::json!({"provider": "aft", "refusal": refusal})
                );
            }
            other => panic!("expected the refusal with its detail, got {other:?}"),
        }
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1, "never re-sent");
    }
}

/// A scope frozen under a renamed module's old id: after the daemon retires the old id, the
/// scope check finds the twin the owner re-synced under its new id, and only then.
mod renamed_scope_owner {
    use super::*;

    /// `scope.describe` answering per owner id.
    struct ByOwner(HashMap<String, DescribedScope>, Mutex<Vec<String>>);

    #[async_trait]
    impl ScopeDescriber for ByOwner {
        async fn describe(&self, owner: &Principal, _: &str) -> Result<DescribedScope, String> {
            let Principal::Reserved { module_id } = owner else {
                return Err("not a reserved owner".into());
            };
            self.1.lock().unwrap().push(module_id.clone());
            self.0
                .get(module_id)
                .cloned()
                .ok_or_else(|| "no answer".into())
        }
    }

    fn answer(status: ScopeStatus, owner_configured: bool) -> DescribedScope {
        DescribedScope {
            status,
            scope_epoch: Some(3),
            owner_synced: owner_configured,
            owner_configured,
            stamp: None,
        }
    }

    fn reserved(module_id: &str) -> Principal {
        Principal::Reserved {
            module_id: module_id.into(),
        }
    }

    #[tokio::test]
    async fn a_retired_old_owner_reads_the_twin_under_the_new_id() {
        let describer = ByOwner(
            HashMap::from([
                (
                    "prefrontal-core".into(),
                    answer(ScopeStatus::NotLive, false),
                ),
                ("prefrontal".into(), answer(ScopeStatus::Live, true)),
            ]),
            Mutex::default(),
        );
        let read = describe_either_owner(&describer, &reserved("prefrontal-core"), "head")
            .await
            .unwrap();
        assert_eq!(read.status, ScopeStatus::Live);
        assert_eq!(
            *describer.1.lock().unwrap(),
            ["prefrontal-core", "prefrontal"]
        );
    }

    /// The handover may leave the old owner's record ended rather than gone: the live twin
    /// under the new id still answers.
    #[tokio::test]
    async fn an_old_owner_record_the_handover_ended_reads_the_live_twin() {
        let describer = ByOwner(
            HashMap::from([
                ("prefrontal-core".into(), answer(ScopeStatus::Ended, true)),
                ("prefrontal".into(), answer(ScopeStatus::Live, true)),
            ]),
            Mutex::default(),
        );
        let read = describe_either_owner(&describer, &reserved("prefrontal-core"), "head")
            .await
            .unwrap();
        assert_eq!(read.status, ScopeStatus::Live);
    }

    #[tokio::test]
    async fn a_live_scope_or_an_unrenamed_owner_asks_once() {
        let describer = ByOwner(
            HashMap::from([
                ("prefrontal-core".into(), answer(ScopeStatus::Live, true)),
                ("aft".into(), answer(ScopeStatus::NotLive, false)),
            ]),
            Mutex::default(),
        );
        for owner in ["prefrontal-core", "aft"] {
            describe_either_owner(&describer, &reserved(owner), "head")
                .await
                .unwrap();
        }
        assert_eq!(*describer.1.lock().unwrap(), ["prefrontal-core", "aft"]);
    }

    /// While the frozen owner is still configured, its "not live" stands when the twin is not
    /// live either: it may only be waiting to sync.
    #[tokio::test]
    async fn a_configured_owner_keeps_its_answer_when_the_twin_is_not_live_either() {
        let describer = ByOwner(
            HashMap::from([
                ("prefrontal-core".into(), answer(ScopeStatus::NotLive, true)),
                ("prefrontal".into(), answer(ScopeStatus::NotLive, false)),
            ]),
            Mutex::default(),
        );
        let read = describe_either_owner(&describer, &reserved("prefrontal-core"), "head")
            .await
            .unwrap();
        assert!(read.owner_configured);
    }
}

/// Answers the plane's route open, as the module end of an in-process connection, and returns
/// the request that follows it with whether the open offered call keepalive.
mod call_keepalive {
    use super::*;
    use crate::connection::InProcessModuleEnd;
    use subc_protocol::FrameType;

    pub(super) async fn bound_request(
        module: &mut InProcessModuleEnd,
    ) -> (subc_protocol::Frame, bool) {
        let bind = module.next_request().await;
        let raw: serde_json::Value = serde_json::from_slice(&bind.body).unwrap();
        assert_eq!(raw["op"], "route.open");
        let keepalive = raw["consumer_capabilities"]
            .as_array()
            .is_some_and(|caps| caps.iter().any(|cap| cap == "call-keepalive/v1"));
        let answer = subc_control::ClientControlResponse::RouteOpen {
            route_channel: 7,
            route_epoch: 1,
            request_window: None,
            watch_window: None,
        };
        assert!(module.reply(&bind, serde_json::to_vec(&answer).unwrap()));
        (module.next_request().await, keepalive)
    }

    fn frozen_manifest(keepalive: bool, reply: Option<u64>) -> broca_records::record::PlanManifest {
        let tool = serde_json::json!({
            "name":"read", "description":"probe", "input_schema": {"type":"object"},
            "module":"aft", "execution_mode":"parallel"
        });
        let mut fetch =
            serde_json::json!({"provider":"aft", "catalog_digest":"digest", "tools":[tool]});
        if keepalive {
            fetch["declarations_frozen"] = serde_json::json!({"call_keepalive":true});
        }
        if let Some(max_ms) = reply {
            fetch["reply"] = serde_json::json!({"read":{"max_ms":max_ms}});
        }
        let manifest: broca_records::record::PlanManifest =
            serde_json::from_value(serde_json::json!({
                "plan":r#"{"tool_items":[{"provider":"aft"}]}"#,
                "plan_identity":"identity", "composition_digest":"composition",
                "fetches":[fetch], "tools":[tool], "system_text":"", "system_digest":"",
                "join_version":1, "role_versions":{}
            }))
            .unwrap();
        // Resume reads only the stored promise, not the provider's current catalog.
        serde_json::from_slice(&serde_json::to_vec(&manifest).unwrap()).unwrap()
    }

    fn rig() -> (SubcToolPlane, InProcessModuleEnd) {
        let (conn, route, module) = SubcConnection::in_process_for_test(7, 1);
        let plane = SubcToolPlane::with_connection(
            Arc::new(InProcessWireConnection {
                conn,
                route,
                close_on_wire: false,
            }),
            "aft",
        );
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        (plane, module)
    }

    #[derive(Clone, Copy)]
    enum Cutoff {
        Silence,
        Deadline,
        RouteClosed,
        UndecodableReply,
        SilenceWithSibling,
    }

    async fn run_through_cutoff(cutoff: Cutoff) {
        use broca_core::{provider::StubProvider, AllowAll, RunControl, RunRequest};
        use broca_protocol::content::{AssistantMessage, ContentBlock};
        use broca_protocol::{FinishReason, MessageId, NormalizedEvent, ProviderControl, Usage};
        use broca_session_loop::{run, MemoryParts, Seams, Writer};
        use broca_session_store::{
            backend::{MemoryBackend, SessionAddr},
            head::SessionTriple,
            keys::DeploymentKey,
            session::Session,
        };

        let keepalive = matches!(cutoff, Cutoff::Silence | Cutoff::SilenceWithSibling);
        let parallel = matches!(cutoff, Cutoff::SilenceWithSibling);
        let (conn, route, mut module) = SubcConnection::in_process_for_test(7, 1);
        let plane = SubcToolPlane::with_connection(
            Arc::new(InProcessWireConnection {
                conn,
                route,
                close_on_wire: parallel,
            }),
            "aft",
        );
        plane
            .windows
            .lock()
            .unwrap()
            .insert("aft".into(), if parallel { 2 } else { 1 });
        let plan = frozen_manifest(keepalive, Some(120_000));
        let submission = serde_json::from_value(serde_json::json!({
            "model":{"provider_module_id":"stub","model_id":"stub"},
            "input":[{"role":"user","content":[{"type":"text","text":"read it"}]}],
            "tools": plan.tools,
        }))
        .unwrap();
        let ctx = batch_ctx("live-cutoff");
        let mut req = RunRequest::from_submission(
            submission,
            ctx.run_id,
            ctx.bind.clone(),
            broca_protocol::SubmissionId("s1".into()),
        );
        req.submission_id = None;
        req.policy_allowed.insert("read".into());
        let control = RunControl {
            manifest: Some(Arc::new(plan)),
            ..RunControl::default()
        };
        let make_step = |id: &str, content, finish_reason| {
            vec![
                NormalizedEvent::Control(ProviderControl::AssistantMessage {
                    message: AssistantMessage {
                        message_id: MessageId(id.into()),
                        content,
                    },
                }),
                NormalizedEvent::Control(ProviderControl::Finished {
                    finish_reason,
                    usage: Usage::default(),
                    retries_used: 0,
                }),
            ]
        };
        let tool_call = |id: &str| {
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(id.into()),
                ..call("read")
            })
        };
        let mut calls = vec![tool_call("first")];
        if parallel {
            calls.push(tool_call("sibling"));
        }
        let provider = StubProvider::new(vec![
            make_step("tool-step", calls, FinishReason::ToolCalls),
            make_step(
                "done-step",
                vec![ContentBlock::Text {
                    text: "done".into(),
                }],
                FinishReason::Stop,
            ),
        ]);
        let backend = MemoryBackend::default();
        let mut session = Session::create(
            &backend,
            SessionAddr("0123456789abcdef".into()),
            SessionTriple {
                project_root: "/project".into(),
                harness: "runner".into(),
                session: "cutoff".into(),
            },
            &DeploymentKey::new(1, &[7; 32]),
            false,
        )
        .await
        .unwrap();
        let mut parts = MemoryParts::default();
        let admission = broca_session_loop::admit::admission(
            &mut req,
            &control,
            &provider,
            session.operations(),
            Some(1),
        )
        .unwrap();
        let seams = Seams {
            steer_yields: None,
            provider: &provider,
            tools: &plane,
            bind: ctx.bind,
            out: None,
            cancel: CancellationToken::new(),
            stop: CancellationToken::new(),
            held: None,
            approval: &AllowAll,
            hook_plane: None,
            now_ms: &|| 1,
            session_address: None,
            compaction_plane: None,
            compaction_session: None,
            compaction_changes: false,
            keep_warm: None,
            account_reports: None,
            max_same_account_wait: Default::default(),
            auth_selection: None,
            step_progress: None,
            accepting_drain: None,
            model_call_ready: None,
            pause_for_auth_at_start: None,
            account_continue: None,
        };
        let transport = async {
            let (first, offered) = bound_request(&mut module).await;
            assert_eq!(offered, keepalive);
            let mut requests = vec![first];
            if parallel {
                requests.push(module.next_request().await);
                assert_eq!(requests[0].header.channel, requests[1].header.channel);
                assert_eq!(requests[0].header.epoch, requests[1].header.epoch);
                let id = |frame: &subc_protocol::Frame| {
                    serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()
                        ["tool_call_id"]
                        .clone()
                };
                if id(&requests[0]) == "sibling" {
                    requests.swap(0, 1);
                }
                assert_eq!(id(&requests[0]), "first");
                assert_eq!(id(&requests[1]), "sibling");
                sleep(Duration::from_secs(45)).await;
                assert!(module.push(
                    &requests[1],
                    cortexkit_role_tool_provider::keepalive_body().to_vec()
                ));
            }
            if matches!(cutoff, Cutoff::RouteClosed) {
                assert!(module.goodbye(&requests[0]));
            }
            if matches!(cutoff, Cutoff::UndecodableReply) {
                assert!(module.reply(&requests[0], b"not JSON".to_vec()));
            }
            requests
        };
        let (finished, sent) = tokio::time::timeout(Duration::from_secs(180), async {
            tokio::join!(
                Box::pin(run(&mut session, &mut parts, admission, &seams)),
                transport
            )
        })
        .await
        .expect("model and tool plane must finish");
        assert_eq!(
            finished.unwrap().reason,
            broca_protocol::RunFinishReason::Completed
        );
        let model_calls = provider.calls();
        assert_eq!(model_calls.len(), 2);
        let results: Vec<_> = model_calls[1]
            .prompt
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::ToolResult(result) => Some(result),
                _ => None,
            })
            .collect();
        assert_eq!(results.len(), sent.len());
        for result in results {
            let reason = match (cutoff, result.tool_call_id.0.as_str()) {
                (Cutoff::Silence | Cutoff::SilenceWithSibling, "first") => "went silent for 90s",
                (Cutoff::Deadline, "first") => "timed out after 120s",
                (Cutoff::UndecodableReply, "first") => "decode tool result",
                (Cutoff::RouteClosed, "first") | (Cutoff::SilenceWithSibling, "sibling") => {
                    "tool route closed"
                }
                _ => panic!("unexpected tool result: {result:?}"),
            };
            let ToolOutput::Error { message, code, .. } = &result.output else {
                panic!("{result:?}")
            };
            assert!(result.is_error);
            assert_eq!(code.as_deref(), Some("outcome_unknown"));
            assert!(message.starts_with("outcome_unknown:"), "{message}");
            assert!(
                message.contains("may have run") && message.contains(reason),
                "{message}"
            );
        }
        assert!(
            module
                .take_written_for_test()
                .iter()
                .all(|frame| frame.header.ty != FrameType::Request),
            "no re-dispatch"
        );
        for request in sent {
            assert!(
                !module.reply(&request, br#"{"content":[]}"#.to_vec()),
                "late terminal is not accepted"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn provider_silence_reaches_the_next_model_request() {
        run_through_cutoff(Cutoff::Silence).await;
    }

    #[tokio::test(start_paused = true)]
    async fn tool_deadline_reaches_the_next_model_request() {
        run_through_cutoff(Cutoff::Deadline).await;
    }

    #[tokio::test(start_paused = true)]
    async fn tool_route_closed_mid_call_reaches_the_next_model_request() {
        run_through_cutoff(Cutoff::RouteClosed).await;
    }

    #[tokio::test(start_paused = true)]
    async fn undecodable_tool_reply_reaches_the_next_model_request_as_unknown() {
        run_through_cutoff(Cutoff::UndecodableReply).await;
    }

    #[tokio::test(start_paused = true)]
    async fn provider_silence_and_the_sibling_cut_by_route_cleanup_both_reach_the_model() {
        run_through_cutoff(Cutoff::SilenceWithSibling).await;
    }

    fn assert_cancel_and_ignore_late(
        module: &mut InProcessModuleEnd,
        request: &subc_protocol::Frame,
    ) {
        let frames = module.take_written_for_test();
        let cancels: Vec<_> = frames
            .iter()
            .filter(|f| f.header.ty == FrameType::Cancel)
            .collect();
        assert_eq!(cancels.len(), 1, "{frames:?}");
        assert_eq!(cancels[0].header.corr, request.header.corr);
        assert_eq!(cancels[0].header.channel, request.header.channel);
        assert_eq!(cancels[0].header.epoch, request.header.epoch);
        assert!(
            !frames.iter().any(|f| f.header.ty == FrameType::Request),
            "no re-send"
        );
        assert_eq!(module.pending_count_for_test(), 0);
        assert!(!module.push(
            request,
            cortexkit_role_tool_provider::keepalive_body().to_vec()
        ));
        assert!(!module.reply(
            request,
            br#"{"content":[{"type":"text","text":"late"}]}"#.to_vec()
        ));
    }

    #[tokio::test]
    async fn kit_keepalive_deadline_vectors_reach_frozen_dispatch_selection() {
        let vectors: serde_json::Value = serde_json::from_str(
            cortexkit_role_tool_provider_conformance::wire::vectors::CALL_KEEPALIVE_JSON,
        )
        .unwrap();
        for case in vectors["deadlines"].as_array().unwrap() {
            for keepalive in [false, true] {
                let mut frozen = serde_json::to_value(frozen_manifest(keepalive, None)).unwrap();
                if !case["reply"].is_null() {
                    frozen["fetches"][0]["reply"] = serde_json::json!({"read":case["reply"]});
                }
                let frozen: broca_records::record::PlanManifest =
                    serde_json::from_value(frozen).unwrap();
                let manifest = ToolManifest::from_tools(frozen.tools.as_ref().clone())
                    .with_plan_manifest(&frozen)
                    .unwrap();
                let conn = ScriptedToolPlaneConnection::new(vec![vec![]], vec![]);
                let plane = SubcToolPlane::with_connection(conn, "aft")
                    .with_reply_fallback_for_test(case["runner_fallback_ms"].as_u64().unwrap());
                plane.windows.lock().unwrap().insert("aft".into(), 1);
                let ctx = batch_ctx("vectors");
                let mut call = call("read");
                call.input = case["arguments"].clone().into();
                plane
                    .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                    .await
                    .unwrap();
                assert_eq!(plane.frozen_contract(&ctx, "aft").call_keepalive, keepalive);
                let expected = if keepalive {
                    "with_keepalive_ms"
                } else {
                    "without_keepalive_ms"
                };
                assert_eq!(
                    plane.reply_deadline(&ctx, "aft", &call),
                    Duration::from_millis(case[expected].as_u64().unwrap()),
                    "{} (keepalive={keepalive})",
                    case["name"],
                );
            }
        }
    }

    #[tokio::test]
    async fn declared_aft_bounds_cover_blocking_bash_and_background_worker_watch_arguments() {
        for (name, input) in [
            ("bash", serde_json::json!({"command":"work","wait":true})),
            ("bash", serde_json::json!({"command":"work"})),
            (
                "bash_watch",
                serde_json::json!({"taskId":"task","background":true,"timeoutMs":600_000}),
            ),
            ("bash_watch", serde_json::json!({"taskId":"task"})),
        ] {
            let mut value = serde_json::to_value(frozen_manifest(false, None)).unwrap();
            value["tools"][0]["name"] = serde_json::json!(name);
            value["fetches"][0]["tools"][0]["name"] = serde_json::json!(name);
            value["fetches"][0]["reply"] = serde_json::json!({name:{"max_ms":2_040_000}});
            let plan: broca_records::record::PlanManifest = serde_json::from_value(value).unwrap();
            let manifest = ToolManifest::from_tools(plan.tools.as_ref().clone())
                .with_plan_manifest(&plan)
                .unwrap();
            let conn =
                ScriptedToolPlaneConnection::new(vec![vec![]], vec![successful_tool_reply("done")]);
            let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
            plane.windows.lock().unwrap().insert("aft".into(), 1);
            let ctx = batch_ctx("aft-declared-bounds");
            let mut call = call(name);
            call.input = input.into();
            plane
                .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                .await
                .unwrap();
            assert!(matches!(
                plane.dispatch(&ctx, &call).await,
                DispatchResult::Completed(_)
            ));
            assert_eq!(
                *conn.dispatched_deadlines.lock().unwrap(),
                [Duration::from_millis(2_040_000)]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn module_restart_bind_wait_leaves_the_full_declared_budget_after_enqueue() {
        for keepalive in [false, true] {
            let (plane, mut module) = rig();
            let ctx = batch_ctx("restart-budget");
            let call = call("read");
            let plan = frozen_manifest(keepalive, Some(120_000));
            let manifest = ToolManifest::from_tools(plan.tools.as_ref().clone())
                .with_plan_manifest(&plan)
                .unwrap();
            let dispatch = async {
                plane
                    .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                    .await
                    .unwrap();
                plane.dispatch(&ctx, &call).await
            };
            let started = Instant::now();
            let work = async {
                loop {
                    let bind = module.next_request().await;
                    let raw: serde_json::Value = serde_json::from_slice(&bind.body).unwrap();
                    assert_eq!(raw["op"], "route.open");
                    if started.elapsed() < Duration::from_secs(25) {
                        assert!(module.refuse(&bind, "module_reloading"));
                    } else {
                        let answer = subc_control::ClientControlResponse::RouteOpen {
                            route_channel: 7,
                            route_epoch: 1,
                            request_window: None,
                            watch_window: None,
                        };
                        assert!(module.reply(&bind, serde_json::to_vec(&answer).unwrap()));
                        break;
                    }
                }
                let request = module.next_request().await;
                let enqueued = Instant::now();
                for _ in 0..3 {
                    sleep(Duration::from_secs(30)).await;
                    assert!(module.push(&request, br#"{"kind":"progress"}"#.to_vec()));
                }
                sleep(Duration::from_secs(20)).await;
                assert!(module.reply(&request, br#"{"content":[],"isError":false}"#.to_vec()));
                enqueued
            };
            let (outcome, enqueued) = tokio::join!(dispatch, work);
            assert!(
                matches!(outcome, DispatchResult::Completed(ref r) if !r.is_error),
                "{outcome:?}"
            );
            assert_eq!(enqueued.elapsed(), Duration::from_secs(110));
            assert!(started.elapsed() >= Duration::from_secs(135));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn declared_reply_is_absolute_without_keepalive_even_with_progress() {
        for frozen in [true, false] {
            let (plane, mut module) = rig();
            let ctx = batch_ctx("declared-progress");
            let call = call("read");
            let manifest = if frozen {
                let plan = frozen_manifest(false, Some(120_000));
                ToolManifest::from_tools(plan.tools.as_ref().clone())
                    .with_plan_manifest(&plan)
                    .unwrap()
            } else {
                ToolManifest::from_tools(vec![serde_json::from_value(serde_json::json!({
                    "name":"read", "description":"probe", "input_schema":{}, "module":"aft",
                    "reply":{"max_ms":120_000}
                }))
                .unwrap()])
            };
            let dispatch = async {
                plane
                    .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                    .await
                    .unwrap();
                plane.dispatch(&ctx, &call).await
            };
            let provider = async {
                let (request, keepalive) = bound_request(&mut module).await;
                assert!(!keepalive);
                let started = Instant::now();
                for _ in 0..3 {
                    sleep(Duration::from_secs(30)).await;
                    assert!(module.push(
                        &request,
                        br#"{"kind":"progress","message":"working"}"#.to_vec()
                    ));
                }
                (request, started)
            };
            let (outcome, (request, started)) = tokio::join!(dispatch, provider);
            assert_eq!(
                started.elapsed(),
                Duration::from_secs(120),
                "frozen={frozen}"
            );
            assert!(
                matches!(outcome, DispatchResult::SentButOutcomeUnknown { ref reason }
                if reason.contains("timed out after 120s")),
                "{outcome:?}"
            );
            assert_cancel_and_ignore_late(&mut module, &request);
        }
    }
}

mod outage_wait_tests;
