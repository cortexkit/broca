use std::sync::atomic::{AtomicU16, Ordering};

use super::*;

/// A daemon whose one module is stopped until `up_at`: every route.open before then is
/// refused `target_unavailable`, every one after opens a fresh route. Each open takes 5 ms,
/// so opens started together are really in flight together.
struct StoppedModuleConnection {
    up_at: Instant,
    /// When each route.open started, and for which session.
    attempts: Mutex<Vec<(Instant, String)>>,
    next_channel: AtomicU16,
    gate: Arc<ModuleGate>,
}

#[async_trait]
impl ToolPlaneConnection for StoppedModuleConnection {
    async fn catalog_list(&self) -> Result<Arc<Vec<CatalogEntry>>, ConnError> {
        Ok(Arc::new(Vec::new()))
    }

    async fn route_open(
        &self,
        _module_id: &str,
        identity: &BindIdentity,
        _scope: Option<&ScopeSelector>,
    ) -> Result<RouteHandle, ConnError> {
        let now = Instant::now();
        self.attempts
            .lock()
            .unwrap()
            .push((now, identity.session.clone()));
        sleep(Duration::from_millis(5)).await;
        if now >= self.up_at {
            Ok(RouteHandle::synthetic(
                self.next_channel.fetch_add(1, Ordering::SeqCst),
            ))
        } else {
            let body = serde_json::to_vec(&subc_protocol::ErrorBody::new(
                subc_protocol::error_codes::TARGET_UNAVAILABLE,
                "supervised_not_registered".to_owned(),
            ))
            .unwrap();
            Err(ConnError::Rejected(crate::connection::decode_error_body(
                &body,
            )))
        }
    }

    async fn route_close(&self, _route: &RouteHandle) -> Result<(), ConnError> {
        Ok(())
    }

    async fn tool_call(
        &self,
        _route: &RouteHandle,
        _name: &str,
        _arguments: &serde_json::Value,
        _tool_call_id: Option<&str>,
        _deadline: Duration,
        _preset: Option<&str>,
    ) -> Result<serde_json::Value, ConnError> {
        unreachable!("these tests only open routes")
    }

    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
        Ok(Vec::new())
    }

    fn route_is_open(&self, _route: &RouteHandle) -> bool {
        true
    }

    fn module_gate(&self, _module_id: &str) -> Option<Arc<ModuleGate>> {
        Some(Arc::clone(&self.gate))
    }
}

/// Fifty sessions, so fifty different routes, to one module stopped for ten seconds. All
/// fifty try once at the start, before any refusal is back; after that the module sees one
/// route.open per backoff tick in total, through its gate, rather than one per session.
/// Every session's route opens once the module is back.
#[tokio::test(start_paused = true)]
async fn fifty_routes_to_a_stopped_module_are_paced_by_its_gate() {
    let start = Instant::now();
    let up_at = start + Duration::from_secs(10);
    let conn = Arc::new(StoppedModuleConnection {
        up_at,
        attempts: Mutex::new(Vec::new()),
        next_channel: AtomicU16::new(1),
        // No jitter, so the probe schedule is exact.
        gate: Arc::new(ModuleGate::with_jitter(
            |envelope| envelope,
            CancellationToken::new(),
        )),
    });
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let lifecycle = CancellationToken::new();
    let binds: Vec<BindIdentity> = (0..50)
        .map(|n| BindIdentity::new("/project", "harness", format!("worker-{n}")))
        .collect();
    let patience = RouteOpenPatience {
        deadline: Duration::from_secs(30),
        max_interval: ROUTE_OPEN_RETRY_MAX_INTERVAL,
        waits_out_outage: false,
    };
    let guard = ToolOpenGuard::default();
    let results = join_all(binds.iter().map(|bind| {
        plane
            .ensure_route_for_lifecycle_inner(bind, None, "aft", &lifecycle, None, &guard, patience)
    }))
    .await;
    let attempts = conn.attempts.lock().unwrap().clone();
    let first_round = attempts.iter().filter(|(at, _)| *at == start).count();
    let while_down = attempts
        .iter()
        .filter(|(at, _)| *at > start && *at < up_at)
        .count();
    assert_eq!(
        first_round, 50,
        "each session tries once before anything is known"
    );
    assert!(
        while_down <= 8,
        "while the module is down, fifty routes must make about one route.open per tick \
         between them (100 ms doubling to 5 s gives 6 in ten seconds), got {while_down}"
    );
    let after_return: BTreeSet<&str> = attempts
        .iter()
        .filter(|(at, _)| *at >= up_at)
        .map(|(_, session)| session.as_str())
        .collect();
    assert_eq!(after_return.len(), 50, "each session opens its own route");
    let channels: BTreeSet<u16> = results
        .into_iter()
        .map(|result| match result {
            Ok((handle, _)) => handle.channel,
            Err(error) => panic!("the route must open: {}", error.into_plane()),
        })
        .collect();
    assert_eq!(channels.len(), 50);
}
