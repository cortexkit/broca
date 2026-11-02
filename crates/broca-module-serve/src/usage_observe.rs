//! Best-effort quota observations, independent of the run's durable and billing lanes.

use crate::account_routing::{RoutingTransport, SubcRoutingTransport};
use broca_provider::quota::QuotaObserver;
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

const ROW_LIMIT: usize = 16 * 1024;
// At most two MiB of rows per batch, below the management transport's frame limit.
const PENDING_LIMIT: usize = 128;
const INTERVAL: Duration = Duration::from_secs(30);
const SEND_BUDGET: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Health {
    rows_sent: std::sync::atomic::AtomicU64,
    batches_sent: std::sync::atomic::AtomicU64,
    transport_failures: std::sync::atomic::AtomicU64,
    rows_dropped: std::sync::atomic::AtomicU64,
    ignored: Mutex<BTreeMap<String, u64>>,
}
use std::sync::atomic::Ordering::Relaxed;

impl Health {
    fn snapshot(&self) -> Value {
        json!({
            "rows_sent": self.rows_sent.load(Relaxed),
            "batches_sent": self.batches_sent.load(Relaxed),
            "transport_failures": self.transport_failures.load(Relaxed),
            "rows_dropped": self.rows_dropped.load(Relaxed),
            "ignored": *self.ignored.lock().unwrap(),
        })
    }
    fn reply(&self, body: &[u8]) -> Result<(), serde_json::Error> {
        #[derive(Deserialize)]
        struct Reply {
            #[serde(rename = "accepted")]
            _accepted: u64,
            ignored: Vec<Ignored>,
        }
        #[derive(Deserialize)]
        struct Ignored {
            reason: String,
        }
        #[derive(Deserialize)]
        struct Envelope {
            result: Reply,
        }
        let reply = serde_json::from_slice::<Envelope>(body)?.result;
        let mut ignored = self.ignored.lock().unwrap();
        for row in reply.ignored {
            *ignored.entry(row.reason).or_default() += 1;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
enum Provider {
    Claude,
    Codex,
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Headers,
    Frame,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Row<'a> {
    provider: Provider,
    credential_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    account: Option<&'a str>,
    observed_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<BTreeMap<&'a str, &'a str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    frame: Option<&'a RawValue>,
}
struct Pending {
    observed_at_ms: u64,
    receive_order: u64,
    row: Box<RawValue>,
}
type Key = (String, Provider, Kind);

#[derive(Default)]
struct Queue {
    receive_order: std::sync::atomic::AtomicU64,
    pending: Mutex<BTreeMap<Key, Pending>>,
    health: Health,
}

impl Queue {
    fn push(&self, row: Row<'_>, kind: Kind, receive_order: u64) {
        // A limited writer rejects escaped JSON larger than the wire cap without retaining it.
        struct Limited(Vec<u8>);
        impl std::io::Write for Limited {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                if self.0.len() + buf.len() >= ROW_LIMIT {
                    return Err(std::io::Error::other("quota row too large"));
                }
                self.0.extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut encoded = Limited(Vec::new());
        if serde_json::to_writer(&mut encoded, &row).is_err() {
            self.health.rows_dropped.fetch_add(1, Relaxed);
            return;
        }
        let Ok(mut pending) = self.pending.try_lock() else {
            self.health.rows_dropped.fetch_add(1, Relaxed);
            return;
        };
        let key = (row.credential_id.to_owned(), row.provider, kind);
        if let Some(old) = pending.get(&key) {
            if (old.observed_at_ms, old.receive_order) > (row.observed_at_ms, receive_order) {
                return;
            }
        } else if pending.len() >= PENDING_LIMIT {
            self.health.rows_dropped.fetch_add(1, Relaxed);
            return;
        }
        let raw = String::from_utf8(encoded.0).expect("JSON is UTF-8");
        pending.insert(
            key,
            Pending {
                observed_at_ms: row.observed_at_ms,
                receive_order,
                row: RawValue::from_string(raw).expect("serialized row"),
            },
        );
    }

    async fn flush(&self, transport: &dyn RoutingTransport) {
        let rows: Vec<_> = std::mem::take(&mut *self.pending.lock().unwrap())
            .into_values()
            .map(|pending| pending.row)
            .collect();
        if rows.is_empty() {
            return;
        }
        let body = request(&rows);
        self.health.batches_sent.fetch_add(1, Relaxed);
        self.health.rows_sent.fetch_add(rows.len() as u64, Relaxed);
        // Taking the rows before IO makes every send one-shot, even on timeout or refusal.
        let result = tokio::time::timeout(SEND_BUDGET, transport.request(body)).await;
        if !matches!(result, Ok(Ok(Ok(ref reply))) if self.health.reply(reply).is_ok()) {
            self.health.transport_failures.fetch_add(1, Relaxed);
        }
    }
}

fn request(rows: &[Box<RawValue>]) -> Vec<u8> {
    #[derive(Serialize)]
    struct Params<'a> {
        observations: &'a [Box<RawValue>],
    }
    #[derive(Serialize)]
    struct Request<'a> {
        method: &'static str,
        params: Params<'a>,
    }
    serde_json::to_vec(&Request {
        method: "usage.observe",
        params: Params { observations: rows },
    })
    .expect("raw quota rows serialize")
}

struct Observer {
    queue: Arc<Queue>,
    provider: Provider,
    credential_id: String,
    account: Option<String>,
}
impl std::fmt::Debug for Observer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuotaObserver")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}
impl QuotaObserver for Observer {
    fn headers(&self, headers: &reqwest::header::HeaderMap, observed_at_ms: u64) {
        let receive_order = self.queue.receive_order.fetch_add(1, Relaxed);
        let mut selected = BTreeMap::new();
        for (name, value) in headers {
            let name = name.as_str();
            let allowed = match self.provider {
                Provider::Claude => name.starts_with("anthropic-ratelimit-unified-"),
                Provider::Codex => codex_header(name),
            };
            if allowed {
                let Ok(value) = std::str::from_utf8(value.as_bytes()) else {
                    // JSON strings cannot preserve non-UTF-8 header bytes. Do not invent a value.
                    self.queue.health.rows_dropped.fetch_add(1, Relaxed);
                    return;
                };
                selected.insert(name, value);
            }
        }
        if !selected.is_empty() {
            self.queue.push(
                self.row(observed_at_ms, Some(selected), None),
                Kind::Headers,
                receive_order,
            );
        }
    }
    fn frame(&self, frame: &str, observed_at_ms: u64) {
        if self.provider != Provider::Codex {
            return;
        }
        let receive_order = self.queue.receive_order.fetch_add(1, Relaxed);
        #[derive(Deserialize)]
        struct Event<'a> {
            #[serde(borrow, rename = "type")]
            kind: std::borrow::Cow<'a, str>,
        }
        if !serde_json::from_str::<Event<'_>>(frame).is_ok_and(|e| e.kind == "codex.rate_limits") {
            return;
        }
        // RawValue preserves the original JSON, including number and whitespace spelling.
        if let Ok(raw) = serde_json::from_str::<&RawValue>(frame) {
            self.queue.push(
                self.row(observed_at_ms, None, Some(raw)),
                Kind::Frame,
                receive_order,
            );
        }
    }
    fn wants_frames(&self) -> bool {
        self.provider == Provider::Codex
    }
}
impl Observer {
    fn row<'a>(
        &'a self,
        observed_at_ms: u64,
        headers: Option<BTreeMap<&'a str, &'a str>>,
        frame: Option<&'a RawValue>,
    ) -> Row<'a> {
        Row {
            provider: self.provider,
            credential_id: &self.credential_id,
            account: self.account.as_deref(),
            observed_at_ms,
            headers,
            frame,
        }
    }
}
fn codex_header(name: &str) -> bool {
    name != "x-codex-turn-state"
        && name.starts_with("x-")
        && (name.starts_with("x-codex-")
            || name[2..].contains("-primary-")
            || name[2..].contains("-secondary-")
            || name[2..].ends_with("-limit-name"))
}

static PUBLISHED: OnceLock<Arc<Queue>> = OnceLock::new();

/// Start the process's one best-effort sender on the attested consumer connection.
pub fn start(conn: Arc<broca_subc::SubcConnection>, identity: broca_protocol::BindIdentity) {
    let queue = Arc::new(Queue::default());
    if PUBLISHED.set(Arc::clone(&queue)).is_err() {
        return;
    }
    let transport = SubcRoutingTransport::new(conn, "insula".into(), identity);
    spawn_sender(queue, Arc::new(transport));
}
fn spawn_sender(queue: Arc<Queue>, transport: Arc<dyn RoutingTransport>) {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + INTERVAL, INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            queue.flush(transport.as_ref()).await;
        }
    });
}

/// Use the run's frozen vault-returned credential id and account reference, never an alias
/// or the identity freshly resolved during resume.
pub fn observer(
    provider: &str,
    method: Option<&str>,
    selection: Option<&broca_core::AuthSelection>,
) -> Option<Arc<dyn QuotaObserver>> {
    observer_for(provider, method, selection, || PUBLISHED.get().cloned())
}

fn observer_for(
    provider: &str,
    method: Option<&str>,
    selection: Option<&broca_core::AuthSelection>,
    queue: impl FnOnce() -> Option<Arc<Queue>>,
) -> Option<Arc<dyn QuotaObserver>> {
    let provider = match (provider, method) {
        ("anthropic", Some("oauth")) => Provider::Claude,
        ("openai", Some("chatgpt")) => Provider::Codex,
        _ => return None,
    };
    let selection = selection?;
    let credential_id = selection.resolved_credential_id.as_ref()?;
    Some(Arc::new(Observer {
        queue: queue()?,
        provider,
        credential_id: credential_id.clone(),
        account: selection.account_ref.clone(),
    }))
}

pub fn published_metrics() -> Value {
    PUBLISHED.get().map_or_else(
        || Health::default().snapshot(),
        |queue| queue.health.snapshot(),
    )
}

#[cfg(test)]
#[path = "usage_observe_tests.rs"]
mod tests;
