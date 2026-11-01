//! Process-local reader census. Principal labels are prepared at bind time, not on each poll.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

const KEY_LIMIT: usize = 256;

#[derive(Hash, PartialEq, Eq)]
struct Key {
    principal: Arc<str>,
    op: &'static str,
    owner: Option<bool>,
}

struct Reading {
    count: u64,
    last_ms: i64,
}

struct Census {
    since_ms: i64,
    entries: HashMap<Key, Reading>,
    overflow: u64,
}

impl Census {
    fn new(since_ms: i64) -> Self {
        Self {
            since_ms,
            entries: HashMap::new(),
            overflow: 0,
        }
    }

    fn record(&mut self, key: Key, now_ms: i64) {
        if let Some(reading) = self.entries.get_mut(&key) {
            reading.count = reading.count.saturating_add(1);
            reading.last_ms = now_ms;
        } else if self.entries.len() < KEY_LIMIT {
            self.entries.insert(
                key,
                Reading {
                    count: 1,
                    last_ms: now_ms,
                },
            );
        } else {
            self.overflow = self.overflow.saturating_add(1);
        }
    }

    fn health(&self) -> serde_json::Value {
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_unstable_by_key(|(key, _)| (&key.principal, key.op, key.owner));
        let entries: Vec<_> = entries
            .into_iter()
            .map(|(key, reading)| {
                serde_json::json!({
                    "principal": key.principal.as_ref(),
                    "op": key.op,
                    "owner": key.owner,
                    "count": reading.count,
                    "last_ms": reading.last_ms,
                })
            })
            .collect();
        serde_json::json!({
            "since_ms": self.since_ms,
            "entries": entries,
            "overflow": self.overflow,
        })
    }
}

static CENSUS: LazyLock<Mutex<Census>> =
    LazyLock::new(|| Mutex::new(Census::new(broca_module::diagnostics::now_ms())));
static UNATTESTED: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from("unattested"));

pub(crate) fn initialize() {
    LazyLock::force(&CENSUS);
}

pub(crate) fn principal_label(principal: Option<&broca_protocol::BindPrincipal>) -> Arc<str> {
    principal.map_or_else(|| UNATTESTED.clone(), |p| Arc::from(p.as_owner_string()))
}

pub(crate) fn operation(method: &str) -> Option<&'static str> {
    match method {
        "session.read" => Some("session.read"),
        "session.history" => Some("session.history"),
        "session.head" => Some("session.head"),
        "session.subscribe" => Some("session.subscribe"),
        "run.status" => Some("run.status"),
        "run.result" => Some("run.result"),
        "run.tool_calls" => Some("run.tool_calls"),
        "send.status" => Some("send.status"),
        "session.media_read" => Some("session.media_read"),
        _ => None,
    }
}

pub(crate) fn record(principal: Option<&Arc<str>>, op: &'static str, owner: Option<bool>) {
    let key = Key {
        principal: principal.unwrap_or(&UNATTESTED).clone(),
        op,
        owner,
    };
    CENSUS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(key, broca_module::diagnostics::now_ms());
}

pub(crate) fn health() -> serde_json::Value {
    CENSUS.lock().unwrap_or_else(|p| p.into_inner()).health()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_keys_keep_counting_existing_readers_after_overflow() {
        let mut census = Census::new(10);
        assert_eq!(
            census.health(),
            serde_json::json!({
                "since_ms": 10, "entries": [], "overflow": 0,
            })
        );
        let first: Arc<str> = Arc::from("reserved:reader-0");
        for i in 0..KEY_LIMIT {
            census.record(
                Key {
                    principal: Arc::from(format!("reserved:reader-{i}")),
                    op: "run.status",
                    owner: Some(false),
                },
                20,
            );
        }
        for owner in [Some(false), Some(true), None] {
            census.record(
                Key {
                    principal: first.clone(),
                    op: "session.head",
                    owner,
                },
                30,
            );
        }
        census.record(
            Key {
                principal: first.clone(),
                op: "run.status",
                owner: Some(true),
            },
            30,
        );
        census.record(
            Key {
                principal: first,
                op: "run.status",
                owner: Some(false),
            },
            40,
        );
        assert_eq!(census.entries.len(), KEY_LIMIT);
        assert_eq!(census.overflow, 4);
        let health = census.health();
        let first = &health["entries"][0];
        assert_eq!(first["principal"], "reserved:reader-0");
        assert_eq!(first["count"], 2);
        assert_eq!(first["last_ms"], 40);
        assert_eq!(health["since_ms"], 10);
    }

    #[test]
    fn labels_preserve_attestation_and_operations_are_exact() {
        use broca_protocol::BindPrincipal;
        for (principal, expected) in [
            (None, "unattested"),
            (Some(BindPrincipal::Direct), "direct"),
            (Some(BindPrincipal::Unverified), "unverified"),
            (
                Some(BindPrincipal::Reserved {
                    module_id: "core".into(),
                }),
                "reserved:core",
            ),
        ] {
            assert_eq!(principal_label(principal.as_ref()).as_ref(), expected);
        }
        assert_eq!(operation("session.media_read"), Some("session.media_read"));
        assert_eq!(operation("session.send"), None);
        assert_eq!(operation("session.read.extra"), None);
    }
}
