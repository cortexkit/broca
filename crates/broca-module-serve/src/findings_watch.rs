//! Publishes `findings_changed` (see [`broca_store::findings_events`]) when broca's
//! self-diagnosis findings change, and answers `events_get` for the stored events.
//!
//! A flow that reads `diagnostics.findings` on a schedule learns of a problem only at its next
//! pass. The event lets it wake within seconds instead: every [`INTERVAL`] this task builds the
//! same report `diagnostics.findings` answers with, over its default window, and records it. The
//! store writes an event only when the findings differ from the last ones recorded.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use broca_store::findings_events::{FINDINGS_EVENT, FINDINGS_VERSION};
use broca_store::SqliteStore;
use broca_wire::diagnostics::{Reply, Severity, DEFAULT_WINDOW_MS};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_protocol::ErrorBody;
use tokio_util::sync::CancellationToken;

pub const OP_EVENTS_GET: &str = "events_get";
/// How often the findings are rebuilt and compared.
pub const INTERVAL: Duration = Duration::from_secs(10);

/// The store `events_get` reads, set when the watch starts.
static STORE: OnceLock<Arc<SqliteStore>> = OnceLock::new();

/// What the findings are, for comparing one report with the next: a digest of each finding's
/// check, key and severity, and the worst severity (`ok` when there is no finding). Counts,
/// times and samples move with every run and are left out, so an event marks a finding
/// appearing, clearing or changing severity, not every failed run.
pub fn findings_digest(reply: &Reply) -> (String, &'static str) {
    let mut findings: Vec<_> = reply
        .findings
        .iter()
        .map(|f| (f.check.as_str(), f.key.as_str(), severity(&f.severity)))
        .collect();
    findings.sort_unstable();
    let worst = if findings.iter().any(|f| f.2 == "error") {
        "error"
    } else if findings.iter().any(|f| f.2 == "warn") {
        "warn"
    } else {
        "ok"
    };
    let digest = Sha256::digest(serde_json::to_vec(&(worst, &findings)).unwrap_or_default());
    (format!("{digest:x}"), worst)
}

fn severity(severity: &Severity) -> &'static str {
    match severity {
        Severity::Info => "info",
        Severity::Warn => "warn",
        Severity::Error => "error",
    }
}

/// Rebuilds and records the findings every [`INTERVAL`] until `stop`. `catalog_age` answers the
/// catalog's age as `diagnostics.findings` reads it, so both report the same findings.
pub fn spawn(
    store: Arc<SqliteStore>,
    root: PathBuf,
    catalog_age: impl Fn(i64) -> Result<i64, String> + Send + Sync + 'static,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let _ = STORE.set(Arc::clone(&store));
    let catalog_age = Arc::new(catalog_age);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tick.tick() => {}
            }
            let (store, root, catalog_age) =
                (Arc::clone(&store), root.clone(), Arc::clone(&catalog_age));
            let recorded = tokio::task::spawn_blocking(move || {
                let now = broca_module::diagnostics::now_ms();
                let window = DEFAULT_WINDOW_MS;
                let evidence =
                    crate::diagnostics::gather(&root, catalog_age(now), now - window, now)
                        .map_err(|error| error.message)?;
                let (digest, worst) =
                    findings_digest(&crate::diagnostics::report(evidence, now, window));
                store
                    .record_findings(&digest, worst, u64::try_from(now).unwrap_or_default())
                    .map(|key| key.map(|key| (key, worst)))
                    .map_err(|error| error.to_string())
            })
            .await;
            match recorded {
                Ok(Ok(Some((key, worst)))) => {
                    tracing::info!(target: "diagnostics", event_key = %key,
                    worst_severity = worst, "self-diagnosis findings changed; findings_changed recorded")
                }
                Ok(Ok(None)) => {}
                Ok(Err(error)) => tracing::warn!(target: "diagnostics", %error,
                    "could not record the self-diagnosis findings; retrying next pass"),
                Err(error) => tracing::warn!(target: "diagnostics", %error,
                    "the self-diagnosis findings pass stopped; retrying next pass"),
            }
        }
    })
}

/// Answers an `events_get` request's body.
pub fn answer(body: &[u8]) -> Result<Value, ErrorBody> {
    events_get(crate::diagnostics::request_params(body)?)
}

/// `events_get`'s parameters; any other member is refused.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventsGet {
    event_key: String,
    event_name: String,
    version: u32,
}

/// Answers `events_get`: the stored body of a `findings_changed` event, with its digest and when
/// it was recorded. A key, name or version that names no kept event is `event_not_visible`.
pub fn events_get(params: Value) -> Result<Value, ErrorBody> {
    let request: EventsGet = serde_json::from_value(params)
        .map_err(|error| ErrorBody::new("invalid_request", error.to_string()))?;
    let not_visible = || ErrorBody::new("event_not_visible", "event is not visible");
    if request.event_name != FINDINGS_EVENT || request.version != FINDINGS_VERSION {
        return Err(not_visible());
    }
    let store = STORE
        .get()
        .ok_or_else(|| ErrorBody::new("unavailable", "the findings event store is not open"))?;
    let event = store
        .findings_event(&request.event_key)
        .map_err(|error| ErrorBody::new("unavailable", error.to_string()))?
        .ok_or_else(not_visible)?;
    let body = String::from_utf8(event.body.clone())
        .map_err(|error| ErrorBody::new("unavailable", error.to_string()))?;
    let published_at = chrono::DateTime::from_timestamp_millis(event.at_ms as i64)
        .ok_or_else(|| ErrorBody::new("unavailable", "the event's time is out of range"))?
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    Ok(json!({
        "body": body,
        "digest": format!("{:x}", Sha256::digest(&event.body)),
        "event_key": request.event_key,
        "event_name": FINDINGS_EVENT,
        "published_at": published_at,
    }))
}

#[cfg(test)]
#[path = "findings_watch_tests.rs"]
mod tests;
