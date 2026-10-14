use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_catalog::{CatalogCache, FusiformCatalogResponse, FUSIFORM_FACT_PREFIXES};
use broca_protocol::BindIdentity;
use broca_subc::{ConnError, RouteHandle, SubcConnection};
use serde::Deserialize;

pub const FUSIFORM_MODULE_ID: &str = "fusiform";
pub const FUSIFORM_CATALOG_TOOL: &str = "catalog.get";
pub const CATALOG_POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollError {
    Transport(String),
    Protocol(String),
    Cache(String),
}

impl std::fmt::Display for PollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "fusiform transport: {error}"),
            Self::Protocol(error) => write!(f, "fusiform protocol: {error}"),
            Self::Cache(error) => write!(f, "catalog cache: {error}"),
        }
    }
}

impl std::error::Error for PollError {}

/// Which transport a catalog poll is running over.
///
/// `Fake` is gated to test builds and SUBSTITUTES for the real subc route rather than adding to
/// it, so it is worth being explicit about what a `Fake`-based test does NOT establish: route
/// opening, epoch validation, and every transport failure mode are skipped by construction. The
/// refusal and equal-version rebuild logic below is well covered through this variant; the route
/// beneath it is not covered at all.
///
/// That gap is not theoretical — production has lost a poll to a stale subc route handle, which is
/// precisely the class no test here can reach. Anyone reading a green suite as evidence the poll
/// path is sound should read it as evidence the poll LOGIC is sound.
#[derive(Clone)]
enum PollRouteHandle {
    Subc(RouteHandle),
    #[cfg(test)]
    Fake,
}

trait CatalogRoute: Send + Sync {
    fn open<'a>(
        &'a self,
        module_id: &'a str,
        identity: &'a BindIdentity,
    ) -> Pin<Box<dyn Future<Output = Result<PollRouteHandle, ConnError>> + Send + 'a>>;

    fn call<'a>(
        &'a self,
        route: &'a PollRouteHandle,
        name: &'a str,
        arguments: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, ConnError>> + Send + 'a>>;
}

impl CatalogRoute for SubcConnection {
    fn open<'a>(
        &'a self,
        module_id: &'a str,
        identity: &'a BindIdentity,
    ) -> Pin<Box<dyn Future<Output = Result<PollRouteHandle, ConnError>> + Send + 'a>> {
        Box::pin(async move {
            self.route_open(module_id, identity)
                .await
                .map(PollRouteHandle::Subc)
        })
    }

    fn call<'a>(
        &'a self,
        route: &'a PollRouteHandle,
        name: &'a str,
        arguments: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, ConnError>> + Send + 'a>> {
        match route {
            PollRouteHandle::Subc(route) => {
                // THE HONEST `None`. This borrows the tool-call envelope shape to reach a
                // module operation; no model asked for it and no provider minted an id, so
                // there is nothing to supply. Passing a synthesized id here would put a
                // value on the wire that looks like a provider tool-call id and is not.
                Box::pin(self.tool_call(route, name, arguments, None))
            }
            #[cfg(test)]
            PollRouteHandle::Fake => unreachable!("real subc connection received fake route"),
        }
    }
}

/// Lazily opens and reuses the fusiform route. Construction and boot do not contact fusiform; the
/// first invocation of [`Self::poll`] is the first place route opening can occur.
pub struct CatalogPoller {
    route: Arc<dyn CatalogRoute>,
    identity: BindIdentity,
    cache: Arc<CatalogCache>,
    handle: Mutex<Option<PollRouteHandle>>,
}

impl CatalogPoller {
    pub fn new(
        connection: Arc<SubcConnection>,
        identity: BindIdentity,
        cache: Arc<CatalogCache>,
    ) -> Self {
        Self::with_route(connection, identity, cache)
    }

    fn with_route(
        route: Arc<dyn CatalogRoute>,
        identity: BindIdentity,
        cache: Arc<CatalogCache>,
    ) -> Self {
        Self {
            route,
            identity,
            cache,
            handle: Mutex::new(None),
        }
    }

    pub async fn poll(&self) -> Result<(), PollError> {
        let arguments = serde_json::json!({
            "fact_prefixes": FUSIFORM_FACT_PREFIXES,
            // Preserve the known-negative state. With the default false, an upstream withdrawal
            // is indistinguishable from a model id that never existed.
            "include_retired": true,
            // DO NOT ADD `at_ms` HERE. A point-in-time read looks like a good idea for
            // reproducibility and would wedge the catalog permanently.
            //
            // Measured on the live producer: a historical read returns fewer identities while
            // carrying the CURRENT catalog version, because their version describes the store at
            // the time of the read rather than the snapshot the read resolved. That response is
            // byte-identical in shape to a restored producer -- higher version, smaller identity
            // set -- which passes the stale guard and trips the completeness guard. A refused
            // refresh does not install, so the comparison basis never advances and every
            // subsequent poll fails the same way.
            //
            // The refresh path needs the producer's CURRENT view, and this is the only caller.
        });
        let mut handle = self.handle.lock().unwrap().clone();
        if handle.is_none() {
            handle = Some(
                self.route
                    .open(FUSIFORM_MODULE_ID, &self.identity)
                    .await
                    .map_err(conn_error)?,
            );
        }
        let handle = handle.expect("route was opened above");
        // EVICT AND RETRY ONCE, which is what every other route consumer here does.
        //
        // The tool plane and the vault client both re-open and retry on this class; this
        // poller evicted and returned, so the next attempt was 30 MINUTES away. A target
        // module restarting under us -- an ordinary placement -- therefore cost a
        // half-hour stale window by construction, and a run of them kept the catalog on
        // LKG indefinitely while nothing looked broken.
        //
        // Retrying needs no error classification here, unlike a tool dispatch: `catalog.get`
        // is a pure read, so a second attempt cannot double an effect. The only cost of a
        // needless retry is one round trip.
        let mut handle = handle;
        let mut attempt = 0;
        let response = loop {
            match self
                .route
                .call(&handle, FUSIFORM_CATALOG_TOOL, &arguments)
                .await
            {
                Ok(response) => break response,
                Err(error) => {
                    self.handle.lock().unwrap().take();
                    if attempt == 1 {
                        return Err(conn_error(error));
                    }
                    attempt += 1;
                    // A FRESH route, not the evicted one. Re-calling on the same handle
                    // would retry the exact condition that just failed.
                    handle = match self.route.open(FUSIFORM_MODULE_ID, &self.identity).await {
                        Ok(reopened) => reopened,
                        // Report the ORIGINAL failure: it is the one that describes what
                        // went wrong with the catalog, and a reopen failure on top of it
                        // is a symptom of the same outage.
                        Err(_) => return Err(conn_error(error)),
                    };
                }
            }
        };
        // A well-formed terminal proves the route is alive even when the application payload or
        // cache validation fails. Retain it so one bad document cannot leak a route every poll.
        *self.handle.lock().unwrap() = Some(handle.clone());
        let response = decode_tool_response(response)?;
        self.cache
            .refresh(response)
            .map_err(|error| PollError::Cache(error.to_string()))?;
        Ok(())
    }
}

/// Which poll failures the LOOP counts toward the refusal streak.
///
/// `cache.refresh` counts its own refusals, so a `Cache` error is already on the books
/// by the time it reaches the loop. Counting it again double-weights it: two refused
/// polls would read as a streak of FOUR and cross the degrade threshold a poll early,
/// while transport failures counted once. A shared counter is only meaningful if every
/// failed poll weighs the same.
///
/// Named rather than inlined so the rule has somewhere to be tested and somewhere to be
/// read; the bug it replaced was four lines of my own, shipped the same evening.
fn counts_at_the_loop(error: &PollError) -> bool {
    match error {
        PollError::Transport(_) | PollError::Protocol(_) => true,
        PollError::Cache(_) => false,
    }
}

/// Start the 30-minute refresh loop. The first poll runs in the detached task, after startup has
/// finished wiring the cache; no fusiform route is opened by this function itself.
pub fn spawn_catalog_poll(poller: Arc<CatalogPoller>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Make startup observably side-effect free toward fusiform. The task's first timer tick is
        // the first poll; constructing the poller and spawning this loop never opens the route.
        let first_poll = tokio::time::Instant::now() + Duration::from_millis(1);
        let mut interval = tokio::time::interval_at(first_poll, CATALOG_POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = poller.poll().await {
                // Count it where the health probe can see it. A transport failure never
                // reaches `cache.refresh`, so without this the probe reads ZERO refusals
                // through a total refresh outage -- which is what happened on 09-14. Which
                // failures count here, and why a Cache error must not, is the rule at
                // `counts_at_the_loop`.
                if counts_at_the_loop(&error) {
                    poller.cache.note_poll_failure(error.to_string());
                }
                tracing::warn!(
                    target: "catalog",
                    "fusiform catalog refresh failed; retaining last-known-good memory: {error}"
                );
            }
        }
    })
}

#[derive(Deserialize)]
struct ToolTextContent {
    #[serde(rename = "type")]
    kind: String,
    text: String,
}

#[derive(Deserialize)]
struct ToolResult {
    content: Vec<ToolTextContent>,
    #[serde(rename = "isError", default)]
    is_error: bool,
}

fn decode_tool_response(value: serde_json::Value) -> Result<FusiformCatalogResponse, PollError> {
    // Tool providers normally return CallToolResult text content. Accept the direct response too:
    // fusiform's source-level route tests use it, and keeping decoding explicit makes either
    // contract change fail as a protocol error rather than loading an empty catalog.
    if value.get("models").is_some() {
        return serde_json::from_value(value)
            .map_err(|error| PollError::Protocol(format!("decode catalog.get response: {error}")));
    }
    let result: ToolResult = serde_json::from_value(value)
        .map_err(|error| PollError::Protocol(format!("decode tool result envelope: {error}")))?;
    if result.is_error {
        return Err(PollError::Protocol(
            "catalog.get returned an application error".to_string(),
        ));
    }
    let text = result
        .content
        .iter()
        .find(|content| content.kind == "text")
        .map(|content| content.text.as_str())
        .ok_or_else(|| PollError::Protocol("catalog.get returned no text content".to_string()))?;
    serde_json::from_str(text)
        .map_err(|error| PollError::Protocol(format!("decode catalog.get text: {error}")))
}

fn conn_error(error: ConnError) -> PollError {
    PollError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_catalog::{catalog, CatalogSource};
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    #[derive(Default)]
    struct FakeRoute {
        opens: AtomicUsize,
        delay: Mutex<Option<Duration>>,
        last_arguments: Mutex<Option<serde_json::Value>>,
        responses: Mutex<VecDeque<Result<serde_json::Value, ConnError>>>,
    }

    impl CatalogRoute for FakeRoute {
        fn open<'a>(
            &'a self,
            _module_id: &'a str,
            _identity: &'a BindIdentity,
        ) -> Pin<Box<dyn Future<Output = Result<PollRouteHandle, ConnError>> + Send + 'a>> {
            Box::pin(async move {
                self.opens.fetch_add(1, Ordering::Relaxed);
                Ok(PollRouteHandle::Fake)
            })
        }

        fn call<'a>(
            &'a self,
            _route: &'a PollRouteHandle,
            _name: &'a str,
            arguments: &'a serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, ConnError>> + Send + 'a>>
        {
            Box::pin(async move {
                *self.last_arguments.lock().unwrap() = Some(arguments.clone());
                let delay = *self.delay.lock().unwrap();
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| Err(ConnError::Transport("fusiform unreachable".into())))
            })
        }
    }

    fn response(resolved_at_ms: i64) -> FusiformCatalogResponse {
        FusiformCatalogResponse {
            source: "models.dev".into(),
            resolved_at_ms,
            catalog_version: 7,
            models: BTreeMap::from([
                (
                    "anthropic/claude-sonnet-4-5".into(),
                    BTreeMap::from([
                        ("existence".into(), serde_json::json!("present")),
                        ("limit.context".into(), serde_json::json!(200000)),
                        ("capability.attachment".into(), serde_json::json!(false)),
                        ("capability.reasoning".into(), serde_json::json!(true)),
                        ("capability.tool_call".into(), serde_json::json!(true)),
                    ]),
                ),
                (
                    "anthropic/claude-withdrawn".into(),
                    BTreeMap::from([("existence".into(), serde_json::json!("retired"))]),
                ),
            ]),
            withheld: Vec::new(),
            uncertain: Vec::new(),
        }
    }

    /// The refresh request must ask for the producer's CURRENT view.
    ///
    /// A comment can be read past; this cannot. A point-in-time read returns fewer identities
    /// under the current catalog version, which is indistinguishable from a restored producer
    /// and wedges the completeness guard permanently -- so the absence of a time argument is a
    /// correctness property of this request, not a default nobody got round to changing.
    #[tokio::test]
    async fn the_refresh_request_asks_for_the_current_view() {
        let route = Arc::new(FakeRoute::default());
        let cache = Arc::new(CatalogCache::empty());
        let poller = CatalogPoller::with_route(route.clone(), identity(), cache);
        let _ = poller.poll().await;

        let sent = route
            .last_arguments
            .lock()
            .unwrap()
            .clone()
            .expect("one call");
        let object = sent.as_object().expect("arguments are an object");
        for forbidden in ["at_ms", "as_of_ms", "at", "as_of"] {
            assert!(
                !object.contains_key(forbidden),
                "the refresh must not pin a point in time; found {forbidden} in {sent}"
            );
        }
        assert_eq!(
            object.get("include_retired"),
            Some(&serde_json::json!(true)),
            "withdrawal must stay distinguishable from never-existed"
        );
    }

    fn identity() -> BindIdentity {
        BindIdentity::new("/tmp", "broca", "catalog-refresh")
    }

    #[tokio::test]
    async fn route_is_not_opened_until_the_first_poll() {
        let route = Arc::new(FakeRoute::default());
        let cache = Arc::new(CatalogCache::empty());
        let poller = CatalogPoller::with_route(route.clone(), identity(), cache);
        assert_eq!(route.opens.load(Ordering::Relaxed), 0);

        let error = poller.poll().await.expect_err("scripted route is down");
        assert!(matches!(error, PollError::Transport(_)));
        // TWO opens for one poll: the call fails, the handle is evicted, and the retry
        // opens a FRESH route before giving up. The subject here is still "zero before
        // the first poll"; the exact count is kept rather than loosened to non-zero so
        // that a change to the retry shape has to come through this assertion.
        assert_eq!(route.opens.load(Ordering::Relaxed), 2);
    }

    /// Every failed poll counts exactly ONCE, whatever refused it.
    ///
    /// `cache.refresh` counts its own refusals, so the loop must not count them again.
    /// Before this, two refused polls read as a streak of FOUR and crossed the degrade
    /// threshold a poll early, while transport failures counted once -- a shared counter
    /// whose classes weighed differently. Found by an audit round the same evening the
    /// double count shipped.
    #[test]
    fn the_loop_counts_only_failures_the_cache_did_not_already_count() {
        assert!(counts_at_the_loop(&PollError::Transport(
            "dead route".into()
        )));
        assert!(counts_at_the_loop(&PollError::Protocol("bad frame".into())));
        assert!(
            !counts_at_the_loop(&PollError::Cache("refused".into())),
            "cache.refresh already counted this one"
        );
    }

    /// And the counter itself still moves, so the rule above cannot pass by counting
    /// nothing at all.
    #[test]
    fn note_poll_failure_moves_both_counters() {
        let cache = CatalogCache::empty();
        assert_eq!(cache.consecutive_refusals().0, 0);
        cache.note_poll_failure("stale subc route handle".into());
        assert_eq!(cache.consecutive_refusals().0, 1);
        assert_eq!(cache.total_refusals(), 1);
    }

    /// A transport failure must be VISIBLE on the health probe.
    ///
    /// The counters lived inside `cache.refresh`, so a poll that died in transport left
    /// them at zero. On 2026-09-14 that read 0 refusals through a four-hour outage while
    /// only `catalog_age_ms` moved — a reassuring value returned through the exact
    /// failure the field exists to report.
    #[tokio::test]
    async fn a_transport_failure_is_counted_where_the_health_probe_can_see_it() {
        let route = Arc::new(FakeRoute::default());
        let cache = Arc::new(CatalogCache::empty());
        // Both attempts fail: the retry is exhausted, so the poll genuinely fails.
        for _ in 0..2 {
            route
                .responses
                .lock()
                .unwrap()
                .push_back(Err(ConnError::Transport("stale subc route handle".into())));
        }
        let poller = Arc::new(CatalogPoller::with_route(
            route.clone(),
            identity(),
            cache.clone(),
        ));
        assert_eq!(cache.consecutive_refusals().0, 0);

        let error = poller.poll().await.expect_err("both attempts fail");
        poller.cache.note_poll_failure(error.to_string());

        let (streak, reason) = cache.consecutive_refusals();
        assert_eq!(
            streak, 1,
            "a transport failure is a poll that did not refresh"
        );
        assert!(
            reason
                .unwrap_or_default()
                .contains("stale subc route handle"),
            "the reason must name the transport cause, since the counter is now shared \
             with cache refusals and only the reason distinguishes them"
        );
        assert_eq!(cache.total_refusals(), 1);
    }

    /// A blip costs a round trip, not thirty minutes.
    ///
    /// The tool plane and the vault client both evict-and-retry-once on this class; this
    /// poller evicted and returned, so an ordinary placement of the target module left
    /// the catalog on LKG until the next interval. Retrying is safe WITHOUT error
    /// classification because `catalog.get` is a pure read -- a second attempt cannot
    /// double an effect, unlike a tool dispatch.
    #[tokio::test]
    async fn a_transport_blip_is_retried_once_rather_than_costing_an_interval() {
        let route = Arc::new(FakeRoute::default());
        let cache = Arc::new(CatalogCache::empty());
        route
            .responses
            .lock()
            .unwrap()
            .push_back(Err(ConnError::Transport("stale subc route handle".into())));
        route
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(serde_json::to_value(response(1)).unwrap()));
        let poller = CatalogPoller::with_route(route.clone(), identity(), cache.clone());

        poller
            .poll()
            .await
            .expect("the retry must recover the poll, not merely be attempted");
        assert_eq!(
            route.opens.load(Ordering::Relaxed),
            2,
            "the retry must open a FRESH route; re-calling the evicted handle would \
             retry the exact condition that just failed"
        );
    }

    #[tokio::test]
    async fn admission_resolves_from_memory_while_fusiform_is_unreachable() {
        let route = Arc::new(FakeRoute::default());
        let cache = Arc::new(CatalogCache::empty());
        cache
            .install_for_test(response(1), CatalogSource::Fusiform)
            .unwrap();
        broca_catalog::install_global_catalog_cache(cache.clone());
        let poller = CatalogPoller::with_route(route, identity(), cache);

        let error = poller.poll().await.expect_err("fusiform is unreachable");
        assert!(matches!(error, PollError::Transport(_)));
        assert!(catalog()
            .expect("memory remains available")
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .is_some());
    }

    #[test]
    fn a_withdrawn_model_is_refused_by_a_fresh_episode_resolve() {
        let cache = Arc::new(CatalogCache::empty());
        cache
            .install_for_test(response(1), CatalogSource::Fusiform)
            .unwrap();
        broca_catalog::install_global_catalog_cache(cache);
        let result = broca_catalog::live::resolve_provider(
            "anthropic/claude-withdrawn",
            None,
            1024,
            &broca_catalog::live::AuthMode::Bearer("unused".into()),
            &broca_catalog::live::ResolveOptions::default(),
        );
        let error = match result {
            Ok(_) => panic!("withdrawn model unexpectedly resolved"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("withdrawn upstream"), "{error}");
        assert!(
            error.to_string().contains("anthropic/claude-withdrawn"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn resolution_does_not_wait_for_a_reachable_but_slow_route() {
        let route = Arc::new(FakeRoute::default());
        *route.delay.lock().unwrap() = Some(Duration::from_millis(250));
        route
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(serde_json::to_value(response(1)).unwrap()));
        let cache = Arc::new(CatalogCache::empty());
        cache
            .install_for_test(response(1), CatalogSource::Fusiform)
            .unwrap();
        broca_catalog::install_global_catalog_cache(cache.clone());
        let poller = Arc::new(CatalogPoller::with_route(route.clone(), identity(), cache));
        let slow_poll = tokio::spawn({
            let poller = poller.clone();
            async move { poller.poll().await }
        });
        tokio::task::yield_now().await;

        let started = Instant::now();
        let model = catalog()
            .expect("memory catalog exists")
            .model_for_serving("anthropic", "claude-sonnet-4-5");
        let elapsed = started.elapsed();
        assert!(model.is_some());
        assert!(
            elapsed < Duration::from_millis(25),
            "memory resolution waited {elapsed:?} for the slow route"
        );
        slow_poll.await.unwrap().unwrap();
        assert_eq!(
            route.last_arguments.lock().unwrap().as_ref(),
            Some(&serde_json::json!({
                "fact_prefixes": FUSIFORM_FACT_PREFIXES,
                "include_retired": true,
            }))
        );
    }
}
