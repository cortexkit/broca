//! Content-free live evidence. Historical coverage begins at process start, never at epoch zero.
use std::collections::{BTreeMap, VecDeque};
use std::sync::{LazyLock, Mutex};

pub const CAP: usize = 20_000;
pub const HORIZON_MS: i64 = 604_800_000;

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Clone, Debug)]
pub struct Event {
    pub at_ms: i64,
    pub module: String,
    pub code: String,
}

pub struct Ring {
    since_ms: i64,
    state: Mutex<(VecDeque<Event>, Option<i64>)>,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub since_ms: i64,
    pub truncated: bool,
    pub events: Vec<Event>,
}

impl Ring {
    pub fn new(since_ms: i64) -> Self {
        Self {
            since_ms,
            state: Mutex::new((VecDeque::new(), None)),
        }
    }

    pub fn record(&self, mut event: Event) {
        // Oversized identifiers remain correlatable without retaining unbounded producer data.
        fn bounded(id: String, limit: usize) -> String {
            if id.len() <= limit {
                return id;
            }
            use sha2::{Digest, Sha256};
            format!("oversized:{:x}", Sha256::digest(id.as_bytes()))
        }
        event.module = bounded(event.module, 256);
        event.code = bounded(event.code, 128);
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        // Keep a monotonic coverage watermark even if the wall clock moves backwards.
        let horizon = event.at_ms.saturating_sub(HORIZON_MS);
        while state.0.len() >= CAP || state.0.front().is_some_and(|old| old.at_ms < horizon) {
            if let Some(old) = state.0.pop_front() {
                state.1 = Some(state.1.unwrap_or(self.since_ms).max(old.at_ms));
            }
        }
        state.0.push_back(event);
    }

    pub fn snapshot(&self, since: i64, now: i64) -> Result<Snapshot, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "diagnostic event ring is poisoned")?;
        Ok(Snapshot {
            since_ms: self
                .since_ms
                .max(if state.1.is_some() {
                    state
                        .0
                        .front()
                        .map_or(now, |event| event.at_ms)
                        .max(state.1.unwrap_or(self.since_ms))
                } else {
                    self.since_ms
                })
                .max(now.saturating_sub(HORIZON_MS)),
            truncated: state.1.is_some_and(|at| at >= since),
            events: state
                .0
                .iter()
                .filter(|e| e.at_ms >= since && e.at_ms <= now)
                .cloned()
                .collect(),
        })
    }
}

pub static SEND_REFUSALS: LazyLock<Ring> = LazyLock::new(|| Ring::new(*START_MS));
static START_MS: LazyLock<i64> = LazyLock::new(now_ms);
pub static CATALOG_REFUSALS: LazyLock<Ring> = LazyLock::new(|| Ring::new(*START_MS));
pub static TOOL_OUTAGES: LazyLock<Ring> = LazyLock::new(|| Ring::new(*START_MS));

pub fn initialize() {
    LazyLock::force(&SEND_REFUSALS);
    LazyLock::force(&CATALOG_REFUSALS);
    LazyLock::force(&TOOL_OUTAGES);
}

/// Called only for new durable records, never replay. Unrelated append variants do no work.
pub(crate) fn observe_outage<'a>(record: &broca_wal::WalRecord, ring: impl FnOnce() -> &'a Ring) {
    use broca_wal::{NoDispatchReason, WalRecord};
    let (module, code, at) = match record {
        WalRecord::ToolNoDispatch {
            reason:
                NoDispatchReason::Unavailable {
                    module: Some(module),
                    code: Some(code),
                    ..
                },
            dispatched_at_ms,
            ..
        } => (module, code, *dispatched_at_ms),
        WalRecord::ToolResult {
            result,
            finished_at_ms,
            ..
        } => match &result.output {
            broca_protocol::ToolOutput::Error {
                message,
                module: Some(module),
                code: Some(code),
                nothing_sent: Some(true),
                ..
            } if result.is_error && message == broca_protocol::TOOL_PLANE_UNAVAILABLE_MESSAGE => {
                (module, code, *finished_at_ms)
            }
            _ => return,
        },
        _ => return,
    };
    ring().record(Event {
        module: module.clone(),
        code: code.clone(),
        at_ms: at
            .and_then(|ms| i64::try_from(ms).ok())
            .unwrap_or_else(now_ms),
    });
}

#[derive(Clone, Debug)]
pub struct Fence {
    pub session: String,
    pub kind: String,
    pub first_ms: i64,
    pub last_ms: i64,
}

#[derive(Default)]
struct Fences {
    rows: BTreeMap<(String, String), Fence>,
    overflow: bool,
}
static FENCES: LazyLock<Mutex<Fences>> = LazyLock::new(Mutex::default);

pub fn retention_fenced(session: &broca_protocol::BindIdentity, kind: &str, at_ms: u64) {
    let Ok(mut fences) = FENCES.lock() else {
        return;
    };
    let session = broca_core::session_addr(session);
    let key = (session.clone(), kind.to_owned());
    if fences.rows.len() >= CAP && !fences.rows.contains_key(&key) {
        fences.overflow = true;
        return;
    }
    let at_ms = i64::try_from(at_ms).unwrap_or(i64::MAX);
    let row = fences.rows.entry(key).or_insert_with(|| Fence {
        session,
        kind: kind.to_owned(),
        first_ms: at_ms,
        last_ms: at_ms,
    });
    row.last_ms = at_ms;
}

pub fn retention_deleted(session: &broca_protocol::BindIdentity) {
    if let Ok(mut fences) = FENCES.lock() {
        let address = broca_core::session_addr(session);
        fences.rows.retain(|(session, _), _| session != &address);
    }
}

pub fn retention_fences() -> Result<Vec<Fence>, String> {
    let fences = FENCES
        .lock()
        .map_err(|_| "retention fence map is poisoned")?;
    if fences.overflow {
        return Err("retention fence map exceeded its 20000-condition bound".into());
    }
    Ok(fences.rows.values().cloned().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostics_ring_boot_and_cap_coverage() {
        let ring = Ring::new(100);
        let boot = ring.snapshot(0, 200).unwrap();
        assert_eq!(boot.since_ms, 100);
        assert!(!boot.truncated);
        assert!(boot.events.is_empty());
        for i in 0..=CAP {
            ring.record(Event {
                at_ms: 101 + i as i64,
                module: "module".into(),
                code: "code".into(),
            });
        }
        let snapshot = ring.snapshot(0, 30_000).unwrap();
        assert_eq!(snapshot.events.len(), CAP);
        assert!(snapshot.truncated);
        assert!(snapshot.since_ms > 100);
    }
    #[test]
    fn diagnostics_outage_match_ignores_unrelated_appends_without_resolving_ring() {
        observe_outage(
            &broca_wal::WalRecord::WriterClosed { writer_id: [0; 16] },
            || panic!("unrelated record resolved outage ring"),
        );
    }
}
