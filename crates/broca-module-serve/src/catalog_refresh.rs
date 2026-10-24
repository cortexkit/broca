use std::collections::BTreeSet;
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
const MAX_CATALOG_PAGES: usize = 256;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_CATALOG_RESTARTS: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollError {
    Transport(String),
    Protocol(String),
    Refused(String),
    Cache(String),
}

impl std::fmt::Display for PollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "fusiform transport: {error}"),
            Self::Protocol(error) => write!(f, "fusiform protocol: {error}"),
            Self::Refused(error) => write!(f, "fusiform refused: {error}"),
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
        let poll_started = std::time::Instant::now();
        let (response, page_count) = self
            .read_catalog(MAX_CATALOG_PAGES, MAX_CATALOG_BYTES)
            .await?;
        // Only a complete, pinned read reaches the existing parser and durable cache path.
        self.cache
            .refresh_polled(response, page_count, poll_started)
            .map_err(|error| PollError::Cache(error.to_string()))?;
        Ok(())
    }

    async fn read_catalog(
        &self,
        max_pages: usize,
        max_bytes: usize,
    ) -> Result<(FusiformCatalogResponse, usize), PollError> {
        let arguments = serde_json::json!({
            "fact_prefixes": FUSIFORM_FACT_PREFIXES,
            // Preserve the known-negative state. With the default false, an upstream withdrawal
            // is indistinguishable from a model id that never existed.
            "include_retired": true,
            "paged": true,
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
        let mut handle = handle.expect("route was opened above");
        let mut transport_retried = false;
        // Charge abandoned attempts too: a changing producer must not bypass the read bounds.
        let mut pages = 0;
        let mut bytes = 0;
        for restart in 0..=MAX_CATALOG_RESTARTS {
            let mut arguments = arguments.clone();
            let mut tokens = BTreeSet::new();
            let mut assembled: Option<FusiformCatalogResponse> = None;
            let mut snapshot_pages = 0;
            loop {
                let result = self
                    .call_page(
                        &mut handle,
                        &mut transport_retried,
                        &arguments,
                        &mut pages,
                        max_pages,
                    )
                    .await;
                let page = match result {
                    Ok(value) => {
                        bytes += serde_json::to_vec(&value)
                            .map_err(|error| PollError::Protocol(error.to_string()))?
                            .len();
                        if bytes > max_bytes {
                            return Err(PollError::Refused(
                                "catalog.get byte limit exceeded".into(),
                            ));
                        }
                        decode_tool_response(value)
                    }
                    Err(error) => Err(error),
                };
                let page = match page.and_then(|page| {
                    if let Some(first) = &assembled {
                        if page.catalog.catalog_version != first.catalog_version
                            || page.catalog.resolved_at_ms != first.resolved_at_ms
                        {
                            return Err(ReadError::Changed(
                                "catalog.get version or resolved instant changed between pages"
                                    .into(),
                            ));
                        }
                    }
                    Ok(page)
                }) {
                    Ok(page) => page,
                    Err(ReadError::Changed(reason)) => {
                        if restart == MAX_CATALOG_RESTARTS {
                            return Err(PollError::Refused(format!(
                                "catalog.get snapshot restart limit exceeded: {reason}"
                            )));
                        }
                        break;
                    }
                    Err(ReadError::Failed(error)) => return Err(error),
                };
                match &mut assembled {
                    Some(first) => merge_page(first, page.catalog)?,
                    None => {
                        validate_qualifications(&page.catalog)?;
                        assembled = Some(page.catalog);
                    }
                }
                snapshot_pages += 1;
                match page.next_page_token {
                    Some(token) => {
                        if !tokens.insert(token.clone()) {
                            return Err(PollError::Protocol(
                                "catalog.get repeated next_page_token".into(),
                            ));
                        }
                        arguments["page_token"] = token.into();
                    }
                    None => {
                        return Ok((assembled.expect("first page was assembled"), snapshot_pages))
                    }
                }
            }
        }
        unreachable!("the last restart either completes or refuses")
    }

    async fn call_page(
        &self,
        handle: &mut PollRouteHandle,
        transport_retried: &mut bool,
        arguments: &serde_json::Value,
        pages: &mut usize,
        max_pages: usize,
    ) -> Result<serde_json::Value, ReadError> {
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
        let response = loop {
            if *pages == max_pages {
                return Err(PollError::Refused("catalog.get page limit exceeded".into()).into());
            }
            *pages += 1;
            match self
                .route
                .call(handle, FUSIFORM_CATALOG_TOOL, arguments)
                .await
            {
                Ok(response) => break response,
                Err(error) => {
                    if let ConnError::Rejected(message) = &error {
                        *self.handle.lock().unwrap() = Some(handle.clone());
                        let (code, reason) = message.split_once(" — ").unwrap_or(("", message));
                        if code == "catalog_changed" {
                            return Err(ReadError::Changed(reason.into()));
                        }
                        return Err(PollError::Refused(format!("catalog.get: {message}")).into());
                    }
                    self.handle.lock().unwrap().take();
                    if *transport_retried {
                        return Err(conn_error(error).into());
                    }
                    *transport_retried = true;
                    // A FRESH route, not the evicted one. Re-calling on the same handle
                    // would retry the exact condition that just failed.
                    *handle = match self.route.open(FUSIFORM_MODULE_ID, &self.identity).await {
                        Ok(reopened) => reopened,
                        // Report the ORIGINAL failure: it is the one that describes what
                        // went wrong with the catalog, and a reopen failure on top of it
                        // is a symptom of the same outage.
                        Err(_) => return Err(conn_error(error).into()),
                    };
                }
            }
        };
        // A well-formed terminal proves the route is alive even when the application payload or
        // cache validation fails. Retain it so one bad document cannot leak a route every poll.
        *self.handle.lock().unwrap() = Some(handle.clone());
        Ok(response)
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
        PollError::Transport(_) | PollError::Protocol(_) | PollError::Refused(_) => true,
        PollError::Cache(_) => false,
    }
}

/// The first retry after a failed poll. Each further consecutive failure doubles the wait.
const CATALOG_RETRY_FIRST: Duration = Duration::from_secs(5);
/// The longest wait between retries of a failing poll.
const CATALOG_RETRY_MAX: Duration = Duration::from_secs(5 * 60);

/// How long to wait before the next poll, given how many polls in a row have just failed.
///
/// A failure waits a short, doubling delay rather than the full refresh interval. At boot
/// fusiform is often not yet reachable when the first poll runs, and a short fusiform outage
/// is ordinary; waiting the full interval after either kept the catalog on its last-known-good
/// copy for half an hour per failed poll. A success returns to the normal cadence.
fn next_poll_delay(consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return CATALOG_POLL_INTERVAL;
    }
    let doublings = (consecutive_failures - 1).min(16);
    CATALOG_RETRY_FIRST
        .saturating_mul(1 << doublings)
        .min(CATALOG_RETRY_MAX)
}

/// Start the refresh loop: every 30 minutes after a successful poll, sooner after a failed one
/// (see [`next_poll_delay`]). The first poll runs in the detached task, after startup has
/// finished wiring the cache; no fusiform route is opened by this function itself.
pub fn spawn_catalog_poll(poller: Arc<CatalogPoller>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Make startup observably side-effect free toward fusiform. The first poll runs after
        // this first sleep; constructing the poller and spawning this loop never opens the route.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let mut consecutive_failures: u32 = 0;
        loop {
            match poller.poll().await {
                Ok(()) => consecutive_failures = 0,
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    // Count it where the health probe can see it. A transport failure never
                    // reaches `cache.refresh`, so without this the probe would read ZERO
                    // refusals through a total refresh outage. Which
                    // failures count here, and why a Cache error must not, is the rule at
                    // `counts_at_the_loop`.
                    if counts_at_the_loop(&error) {
                        poller.cache.note_poll_failure(error.to_string());
                    }
                    tracing::warn!(
                        target: "catalog",
                        "fusiform catalog refresh failed; retaining last-known-good memory, \
                         retrying in {}s: {error}",
                        next_poll_delay(consecutive_failures).as_secs()
                    );
                }
            }
            tokio::time::sleep(next_poll_delay(consecutive_failures)).await;
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

#[derive(Debug)]
enum ReadError {
    Changed(String),
    Failed(PollError),
}

impl From<PollError> for ReadError {
    fn from(error: PollError) -> Self {
        Self::Failed(error)
    }
}

struct CatalogPage {
    catalog: FusiformCatalogResponse,
    next_page_token: Option<String>,
}

fn decode_tool_response(mut value: serde_json::Value) -> Result<CatalogPage, ReadError> {
    // Decode both routed tool envelopes and direct replies through the same catalog parser.
    let mut is_error = false;
    if value.get("content").is_some() {
        let result: ToolResult = serde_json::from_value(value).map_err(|error| {
            PollError::Protocol(format!("decode tool result envelope: {error}"))
        })?;
        is_error = result.is_error;
        let text = result
            .content
            .iter()
            .find(|content| content.kind == "text")
            .map(|content| content.text.as_str())
            .ok_or_else(|| PollError::Protocol("catalog.get returned no text content".into()))?;
        value = serde_json::from_str(text)
            .map_err(|error| PollError::Protocol(format!("decode catalog.get text: {error}")))?;
    }
    if let Some(code) = value.get("code").and_then(|code| code.as_str()) {
        let message = value
            .get("message")
            .and_then(|message| message.as_str())
            .unwrap_or("");
        // Only the dedicated code identifies a stale pinned snapshot. Message text is diagnostic.
        if code == "catalog_changed" {
            return Err(ReadError::Changed(message.into()));
        }
        return Err(PollError::Refused(format!("catalog.get {code}: {message}")).into());
    }
    if is_error {
        return Err(PollError::Protocol("catalog.get returned an application error".into()).into());
    }
    let next_page_token = match value.get("next_page_token") {
        None => None,
        Some(serde_json::Value::String(token)) => Some(token.clone()),
        Some(_) => {
            return Err(
                PollError::Protocol("catalog.get next_page_token is not a string".into()).into(),
            )
        }
    };
    let catalog = serde_json::from_value(value)
        .map_err(|error| PollError::Protocol(format!("decode catalog.get response: {error}")))?;
    Ok(CatalogPage {
        catalog,
        next_page_token,
    })
}

fn validate_qualifications(catalog: &FusiformCatalogResponse) -> Result<(), PollError> {
    for (name, rows) in [
        ("withheld", &catalog.withheld),
        ("uncertain", &catalog.uncertain),
    ] {
        let mut seen = BTreeSet::new();
        for row in rows {
            if !seen.insert(row.to_string()) {
                return Err(PollError::Protocol(format!(
                    "catalog.get duplicate {name} row"
                )));
            }
        }
    }
    Ok(())
}

fn merge_page(
    assembled: &mut FusiformCatalogResponse,
    page: FusiformCatalogResponse,
) -> Result<(), PollError> {
    if assembled.source != page.source {
        return Err(PollError::Protocol(
            "catalog.get source changed between pages".into(),
        ));
    }
    for (model, facts) in page.models {
        if assembled.models.insert(model.clone(), facts).is_some() {
            return Err(PollError::Protocol(format!(
                "catalog.get duplicate model {model}"
            )));
        }
    }
    assembled.withheld.extend(page.withheld);
    assembled.uncertain.extend(page.uncertain);
    assembled.wire_corrections.extend(page.wire_corrections);
    validate_qualifications(assembled)
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
        arguments: Mutex<Vec<serde_json::Value>>,
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
                self.arguments.lock().unwrap().push(arguments.clone());
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
            wire_corrections: Vec::new(),
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
        assert_eq!(object.get("paged"), Some(&serde_json::json!(true)));
        assert!(!object.contains_key("page_token"));
    }

    /// A failed poll retries after a short, doubling, capped delay; a success restores the
    /// normal interval.
    #[test]
    fn a_failed_poll_retries_on_a_capped_backoff() {
        assert_eq!(next_poll_delay(0), CATALOG_POLL_INTERVAL);
        assert_eq!(next_poll_delay(1), Duration::from_secs(5));
        assert_eq!(next_poll_delay(2), Duration::from_secs(10));
        assert_eq!(next_poll_delay(3), Duration::from_secs(20));
        assert_eq!(next_poll_delay(8), CATALOG_RETRY_MAX);
        assert_eq!(next_poll_delay(u32::MAX), CATALOG_RETRY_MAX);
    }

    /// The refresh loop polls again within a minute when fusiform is unreachable at boot,
    /// instead of serving the last-known-good catalog for the full refresh interval.
    #[tokio::test(start_paused = true)]
    async fn an_unreachable_fusiform_at_boot_is_polled_again_promptly() {
        let route = Arc::new(FakeRoute::default());
        let poller = Arc::new(CatalogPoller::with_route(
            route.clone(),
            identity(),
            Arc::new(CatalogCache::empty()),
        ));
        let task = spawn_catalog_poll(poller);
        tokio::time::sleep(Duration::from_secs(60)).await;
        task.abort();
        // Each failed poll calls once and retries once on a reopened route.
        let polls = route.arguments.lock().unwrap().len() / 2;
        assert!(
            polls >= 3,
            "a failing poll must be retried within a minute, not after the refresh interval \
             ({polls} polls in 60s)"
        );
    }

    fn identity() -> BindIdentity {
        BindIdentity::new("/tmp", "broca", "catalog-refresh")
    }

    fn test_dir() -> std::path::PathBuf {
        static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "catalog-paging-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn full_response(version: i64, instant: i64) -> FusiformCatalogResponse {
        let mut full = response(instant);
        full.catalog_version = version;
        full.models.insert(
            "openai/gpt-4o".into(),
            full.models["anthropic/claude-sonnet-4-5"].clone(),
        );
        full
    }

    fn split_pages(full: &FusiformCatalogResponse) -> [serde_json::Value; 2] {
        let mut first = full.clone();
        let mut last = full.clone();
        first
            .models
            .retain(|model, _| model.starts_with("anthropic/"));
        last.models
            .retain(|model, _| !model.starts_with("anthropic/"));
        last.withheld = first.withheld.split_off(full.withheld.len() / 2);
        last.uncertain = first.uncertain.split_off(full.uncertain.len() / 2);
        let mut first = serde_json::to_value(first).unwrap();
        // The cursor is deliberately not a version or an offset; the consumer must echo it.
        first["next_page_token"] = serde_json::json!("opaque +/?= cursor ☃");
        [first, serde_json::to_value(last).unwrap()]
    }

    fn tool_reply(value: serde_json::Value, is_error: bool) -> serde_json::Value {
        serde_json::json!({
            "content": [{"type": "text", "text": value.to_string()}],
            "isError": is_error,
        })
    }

    async fn capture_catalog_info<F: std::future::Future>(future: F) -> (F::Output, Vec<String>) {
        let (output, events) = broca_test_support::capture_logs(future).await;
        let logs = events
            .into_iter()
            .filter(|event| event.target == "catalog" && event.level == tracing::Level::INFO)
            .map(|event| event.message().to_owned())
            .collect();
        (output, logs)
    }

    fn scripted_route(responses: Vec<Result<serde_json::Value, ConnError>>) -> Arc<FakeRoute> {
        Arc::new(FakeRoute {
            responses: Mutex::new(responses.into()),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn paged_catalog_is_byte_identical_to_unpaged_lkg() {
        let full = full_response(7, 10);
        let [first, last] = split_pages(&full);
        let route = scripted_route(vec![
            Ok(tool_reply(first, false)),
            Ok(tool_reply(last, false)),
        ]);
        let dir = test_dir();
        let paged_path = dir.join("paged.json");
        let cache = Arc::new(CatalogCache::open(&paged_path));
        let (poll_result, logs) = capture_catalog_info(
            CatalogPoller::with_route(route.clone(), identity(), cache.clone()).poll(),
        )
        .await;
        poll_result.unwrap();
        assert_eq!(logs.len(), 1, "one success record per refresh: {logs:?}");
        let success = &logs[0];
        assert!(success.contains("pages=2"), "{success}");
        assert!(success.contains("catalog_version=7"), "{success}");
        assert!(success.contains("resolved_at_ms=10"), "{success}");
        assert!(success.contains("models=3"), "{success}");
        let elapsed_ms = success
            .split_once("elapsed_ms=")
            .expect("success record includes elapsed time")
            .1
            .parse::<u64>()
            .expect("elapsed time is an integer number of milliseconds");
        assert!(elapsed_ms < 10_000, "unexpected poll duration: {success}");

        let unpaged_path = dir.join("unpaged.json");
        let unpaged = Arc::new(CatalogCache::open(&unpaged_path));
        let old_route = scripted_route(vec![Ok(serde_json::to_value(&full).unwrap())]);
        CatalogPoller::with_route(old_route, identity(), unpaged)
            .poll()
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(&paged_path).unwrap(),
            std::fs::read(unpaged_path).unwrap()
        );
        assert_eq!(
            std::fs::read(paged_path).unwrap(),
            serde_json::to_vec(&full).unwrap()
        );
        assert!(cache
            .snapshot()
            .unwrap()
            .model_for_serving("openai", "gpt-4o")
            .is_some());
        assert_eq!(cache.total_refusals(), 0);
        let arguments = route.arguments.lock().unwrap();
        assert_eq!(arguments.len(), 2);
        assert!(arguments[0].get("page_token").is_none());
        assert_eq!(arguments[1]["page_token"], "opaque +/?= cursor ☃");
        for args in arguments.iter() {
            assert_eq!(args["paged"], true);
            assert_eq!(args["include_retired"], true);
            assert_eq!(
                args["fact_prefixes"],
                serde_json::json!(FUSIFORM_FACT_PREFIXES)
            );
        }
    }

    #[tokio::test]
    async fn page_qualifications_are_merged_before_the_existing_cache_refusal() {
        let mut full = full_response(7, 10);
        full.withheld = vec![
            serde_json::json!({"model": "anthropic/hidden"}),
            serde_json::json!({"model": "openai/hidden"}),
        ];
        full.uncertain = vec![
            serde_json::json!({"model": "anthropic/uncertain"}),
            serde_json::json!({"model": "openai/uncertain"}),
        ];
        let pages = split_pages(&full);
        let route = scripted_route(pages.iter().cloned().map(Ok).collect());
        let cache = Arc::new(CatalogCache::empty());
        let poller = CatalogPoller::with_route(route.clone(), identity(), cache.clone());
        let (assembled, page_count) = poller
            .read_catalog(MAX_CATALOG_PAGES, MAX_CATALOG_BYTES)
            .await
            .unwrap();
        assert_eq!(page_count, 2);
        assert_eq!(
            serde_json::to_vec(&assembled).unwrap(),
            serde_json::to_vec(&full).unwrap()
        );
        route
            .responses
            .lock()
            .unwrap()
            .extend(pages.into_iter().map(Ok));
        assert!(matches!(poller.poll().await, Err(PollError::Cache(_))));
        assert_eq!(cache.total_refusals(), 1);
        assert!(cache.snapshot().is_err());
    }

    #[tokio::test]
    async fn duplicate_qualifications_across_pages_are_protocol_errors() {
        for field in ["withheld", "uncertain"] {
            let [mut first, mut last] = split_pages(&full_response(7, 10));
            first[field] = serde_json::json!([{"model": "duplicate"}]);
            last[field] = first[field].clone();
            let route = scripted_route(vec![Ok(first), Ok(last)]);
            let poller =
                CatalogPoller::with_route(route, identity(), Arc::new(CatalogCache::empty()));
            assert!(
                matches!(poller.poll().await, Err(PollError::Protocol(message)) if message.contains(field))
            );
        }
    }

    async fn assert_restarts_without_mixing(trigger: Result<serde_json::Value, ConnError>) {
        let abandoned = full_response(7, 10);
        let fresh = full_response(8, 20);
        let [first, _] = split_pages(&abandoned);
        let [fresh_first, fresh_last] = split_pages(&fresh);
        let route = scripted_route(vec![Ok(first), trigger, Ok(fresh_first), Ok(fresh_last)]);
        let dir = test_dir();
        let path = dir.join("catalog.json");
        let cache = Arc::new(CatalogCache::open(&path));
        CatalogPoller::with_route(route.clone(), identity(), cache.clone())
            .poll()
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(path).unwrap(),
            serde_json::to_vec(&fresh).unwrap()
        );
        assert_eq!(
            cache.total_refusals(),
            0,
            "abandoned reads are not failed polls"
        );
        let args = route.arguments.lock().unwrap();
        assert_eq!(args.len(), 4);
        assert!(
            args[2].get("page_token").is_none(),
            "restart must return to page one"
        );
        assert_eq!(
            route.opens.load(Ordering::Relaxed),
            1,
            "a snapshot refusal is not a dead route"
        );
    }

    #[tokio::test]
    async fn catalog_changed_code_restarts_without_mixing_pages() {
        assert_restarts_without_mixing(Ok(tool_reply(
            serde_json::json!({
                "code": "catalog_changed",
                "message": "snapshot lease is no longer valid",
            }),
            true,
        )))
        .await;
    }

    #[tokio::test]
    async fn routed_catalog_changed_code_restarts_without_mixing_pages() {
        assert_restarts_without_mixing(Err(ConnError::Rejected(
            "catalog_changed — snapshot lease is no longer valid".into(),
        )))
        .await;
    }

    #[tokio::test]
    async fn catalog_version_mismatch_restarts_without_mixing_pages() {
        let [_, mut last] = split_pages(&full_response(7, 10));
        last["catalog_version"] = serde_json::json!(8);
        assert_restarts_without_mixing(Ok(last)).await;
    }

    #[tokio::test]
    async fn resolved_instant_mismatch_restarts_without_mixing_pages() {
        let [_, mut last] = split_pages(&full_response(7, 10));
        last["resolved_at_ms"] = serde_json::json!(11);
        assert_restarts_without_mixing(Ok(last)).await;
    }

    #[tokio::test]
    async fn repeated_page_token_fails_closed() {
        let last = serde_json::to_value(response(10)).unwrap();
        let mut first = last.clone();
        first["models"] = serde_json::json!({});
        first["next_page_token"] = serde_json::json!("same");
        let route = scripted_route(vec![Ok(first.clone()), Ok(first), Ok(last)]);
        let cache = Arc::new(CatalogCache::empty());
        cache
            .install_for_test(response(1), CatalogSource::Fusiform)
            .unwrap();
        let poller = CatalogPoller::with_route(route.clone(), identity(), cache.clone());
        let error = poller.poll().await.unwrap_err();
        assert!(
            matches!(error, PollError::Protocol(ref message) if message.contains("repeated next_page_token")),
            "{error}"
        );
        assert_eq!(route.arguments.lock().unwrap().len(), 2);
        assert_eq!(cache.snapshot().unwrap().resolved_at_ms(), 1);
    }

    #[tokio::test]
    async fn catalog_page_limit_is_refused() {
        let mut empty = response(10);
        empty.models.clear();
        let route = scripted_route(
            (0..MAX_CATALOG_PAGES)
                .map(|index| {
                    let mut page = serde_json::to_value(&empty).unwrap();
                    page["next_page_token"] = serde_json::json!(format!("page-{index}"));
                    Ok(page)
                })
                .collect(),
        );
        let cache = Arc::new(CatalogCache::empty());
        let poller = CatalogPoller::with_route(route.clone(), identity(), cache);
        let error = poller.poll().await.unwrap_err();
        assert!(
            matches!(error, PollError::Refused(ref message) if message.contains("page limit")),
            "{error}"
        );
        assert_eq!(route.arguments.lock().unwrap().len(), MAX_CATALOG_PAGES);
    }

    #[tokio::test]
    async fn catalog_byte_limit_is_refused() {
        let route = scripted_route(vec![Ok(serde_json::to_value(response(10)).unwrap())]);
        let poller = CatalogPoller::with_route(route, identity(), Arc::new(CatalogCache::empty()));
        let error = poller.read_catalog(MAX_CATALOG_PAGES, 1).await.unwrap_err();
        assert!(
            matches!(error, PollError::Refused(ref message) if message.contains("byte limit")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn catalog_bounds_charge_continuations_restarts_and_transport_retries() {
        let [first, last] = split_pages(&full_response(7, 10));
        let max_single_page = serde_json::to_vec(&first)
            .unwrap()
            .len()
            .max(serde_json::to_vec(&last).unwrap().len());
        let route = scripted_route(vec![Ok(first.clone()), Ok(last.clone())]);
        let poller =
            CatalogPoller::with_route(route.clone(), identity(), Arc::new(CatalogCache::empty()));
        assert!(
            matches!(poller.read_catalog(MAX_CATALOG_PAGES, max_single_page).await,
            Err(PollError::Refused(message)) if message.contains("byte limit"))
        );
        assert_eq!(route.arguments.lock().unwrap().len(), 2);

        let mut mismatch = last;
        mismatch["catalog_version"] = serde_json::json!(8);
        mismatch["resolved_at_ms"] = serde_json::json!(11);
        let route = scripted_route(vec![Ok(first.clone()), Ok(mismatch), Ok(first)]);
        let poller =
            CatalogPoller::with_route(route.clone(), identity(), Arc::new(CatalogCache::empty()));
        assert!(matches!(poller.read_catalog(3, MAX_CATALOG_BYTES).await,
            Err(PollError::Refused(message)) if message.contains("page limit")));
        assert_eq!(route.arguments.lock().unwrap().len(), 3);

        let route = scripted_route(vec![
            Err(ConnError::Transport("lost route".into())),
            Ok(serde_json::to_value(response(10)).unwrap()),
        ]);
        let poller =
            CatalogPoller::with_route(route.clone(), identity(), Arc::new(CatalogCache::empty()));
        assert!(matches!(poller.read_catalog(1, MAX_CATALOG_BYTES).await,
            Err(PollError::Refused(message)) if message.contains("page limit")));
        assert_eq!(route.arguments.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn snapshot_restart_limit_keeps_lkg_and_counts_one_failed_poll() {
        let full = full_response(7, 10);
        let [first, mut last] = split_pages(&full);
        last["catalog_version"] = serde_json::json!(8);
        last["resolved_at_ms"] = serde_json::json!(11);
        let route = scripted_route(
            (0..=MAX_CATALOG_RESTARTS)
                .flat_map(|_| [Ok(first.clone()), Ok(last.clone())])
                .collect(),
        );
        let dir = test_dir();
        let path = dir.join("catalog.json");
        let cache = Arc::new(CatalogCache::open(&path));
        cache.refresh(full).unwrap();
        let before = std::fs::read(&path).unwrap();
        let poller = CatalogPoller::with_route(route.clone(), identity(), cache.clone());
        let error = poller.poll().await.unwrap_err();
        assert!(
            matches!(error, PollError::Refused(ref message) if message.contains("snapshot restart limit")),
            "{error}"
        );
        assert!(counts_at_the_loop(&error));
        cache.note_poll_failure(error.to_string());
        assert_eq!(cache.total_refusals(), 1);
        assert_eq!(cache.consecutive_refusals().0, 1);
        assert_eq!(cache.snapshot().unwrap().resolved_at_ms(), 10);
        assert_eq!(std::fs::read(path).unwrap(), before);
        assert_eq!(
            route.arguments.lock().unwrap().len(),
            2 * (MAX_CATALOG_RESTARTS + 1)
        );
    }

    #[tokio::test]
    async fn malformed_or_duplicate_pages_fail_closed_without_restart() {
        let full = full_response(7, 10);
        let [first, last] = split_pages(&full);
        let mut invalid_pages = Vec::new();
        for field in ["source", "models", "catalog_version", "resolved_at_ms"] {
            let mut missing = last.clone();
            missing.as_object_mut().unwrap().remove(field);
            invalid_pages.push(missing);
        }
        let mut invalid_token = last.clone();
        invalid_token["next_page_token"] = serde_json::Value::Null;
        invalid_pages.push(invalid_token);
        let mut changed_source = last.clone();
        changed_source["source"] = serde_json::json!("other source");
        invalid_pages.push(changed_source);
        let mut duplicate_model = last.clone();
        duplicate_model["models"] = first["models"].clone();
        invalid_pages.push(duplicate_model);
        for field in ["withheld", "uncertain"] {
            let mut duplicate_row = last.clone();
            duplicate_row[field] = serde_json::json!([{"model": "same"}, {"model": "same"}]);
            invalid_pages.push(duplicate_row);
        }
        for invalid in invalid_pages {
            let route = scripted_route(vec![Ok(first.clone()), Ok(invalid.clone())]);
            let cache = Arc::new(CatalogCache::empty());
            let poller = CatalogPoller::with_route(route.clone(), identity(), cache.clone());
            let error = poller.poll().await.unwrap_err();
            assert!(
                matches!(error, PollError::Protocol(_)),
                "{invalid}: {error}"
            );
            assert_eq!(route.arguments.lock().unwrap().len(), 2);
            assert!(cache.snapshot().is_err());
        }
    }

    #[tokio::test]
    async fn unrelated_refusal_does_not_restart() {
        let route = scripted_route(vec![Ok(tool_reply(
            serde_json::json!({
                "code": "refused", "message": "catalog.get denied by policy",
            }),
            true,
        ))]);
        let poller =
            CatalogPoller::with_route(route.clone(), identity(), Arc::new(CatalogCache::empty()));
        assert!(matches!(poller.poll().await, Err(PollError::Refused(_))));
        assert_eq!(route.arguments.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn other_continuation_refusals_are_terminal_even_with_catalog_changed_text() {
        for code in ["refused", "permission_denied"] {
            let message = "catalog changed since the first page; restart from page one";
            let refusal = serde_json::json!({"code": code, "message": message});
            for result in [
                Ok(tool_reply(refusal.clone(), true)),
                Ok(refusal),
                Err(ConnError::Rejected(format!("{code} — {message}").into())),
            ] {
                let [first, _] = split_pages(&full_response(7, 10));
                let route = scripted_route(vec![Ok(first), result]);
                let cache = Arc::new(CatalogCache::empty());
                cache
                    .install_for_test(full_response(7, 1), CatalogSource::Fusiform)
                    .unwrap();
                let poller = CatalogPoller::with_route(route.clone(), identity(), cache.clone());
                let error = poller.poll().await.unwrap_err();
                assert!(
                    matches!(error, PollError::Refused(ref reason) if reason.contains(code)),
                    "{error}"
                );
                let arguments = route.arguments.lock().unwrap();
                assert_eq!(
                    arguments.len(),
                    2,
                    "ordinary refusals must not restart the read"
                );
                assert!(arguments[1].get("page_token").is_some());
                assert_eq!(route.opens.load(Ordering::Relaxed), 1);
                assert_eq!(cache.snapshot().unwrap().resolved_at_ms(), 1);
            }
        }
    }

    #[tokio::test]
    async fn first_page_refusal_is_terminal() {
        for message in [
            "snapshot lease is no longer valid",
            "catalog changed since the first page; restart from page one",
        ] {
            let refusal = serde_json::json!({"code": "refused", "message": message});
            for result in [
                Ok(tool_reply(refusal.clone(), true)),
                Ok(refusal),
                Err(ConnError::Rejected(format!("refused — {message}").into())),
            ] {
                let route = scripted_route(vec![result]);
                let cache = Arc::new(CatalogCache::empty());
                cache
                    .install_for_test(response(1), CatalogSource::Fusiform)
                    .unwrap();
                let poller = CatalogPoller::with_route(route.clone(), identity(), cache.clone());
                let error = poller.poll().await.unwrap_err();
                assert!(
                    matches!(error, PollError::Refused(ref reason) if reason.contains(message)),
                    "{error}"
                );
                let arguments = route.arguments.lock().unwrap();
                assert_eq!(
                    arguments.len(),
                    1,
                    "first-page refusals must not be retried"
                );
                assert!(arguments[0].get("page_token").is_none());
                assert_eq!(route.opens.load(Ordering::Relaxed), 1);
                assert_eq!(cache.snapshot().unwrap().resolved_at_ms(), 1);
            }
        }
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
                "paged": true,
            }))
        );
    }
}
