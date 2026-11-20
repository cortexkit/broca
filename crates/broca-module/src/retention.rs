//! Retention: a session sent with a `retention` policy is deleted once that many seconds have
//! passed since its last activity while no run of it is active or paused. Every such session is
//! kept in the session store's ephemeral root; the sweep that deletes them and the expiry check
//! every operation makes are in [`store`].

use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use broca_core::{Clock, LeaseHandle, LeaseStore};
use broca_protocol::BindIdentity;

pub const MAX_RETENTION_SECONDS: u64 = 2_592_000;
pub const SWEEP_INTERVAL_MS: u64 = 300_000;

pub(crate) use cortexkit_role_llm_runner::errors::RetentionInvalidDetail as InvalidRetention;
pub use cortexkit_role_llm_runner::errors::{ExpiredDetail, EXPIRED};

pub fn validate_retention(
    requested: Option<u64>,
    current: Option<u64>,
    exists: bool,
) -> Result<Option<u64>, InvalidRetention> {
    let mut request = cortexkit_role_llm_runner::send::SendRequest::new("", "");
    request.retention = requested;
    let limits =
        cortexkit_role_llm_runner::describe::Retention::new(MAX_RETENTION_SECONDS, 3_600_000);
    request.check_retention(Some(&limits), exists.then_some(current))?;
    Ok(requested.filter(|seconds| current != Some(*seconds)))
}

/// What one record says about its session's retention clock. The store's records map to these
/// in `store::store_policy`, and [`policy_of`] decides from them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PolicyEvent {
    /// A run started, with the retention its send set, if it set one. Not named `RunStarted`:
    /// broca-wal's retention-writer scan reads that name as a log record being written.
    Started {
        retention: Option<u64>,
        ts_ms: Option<u64>,
    },
    /// A send was queued, or steered into a running run, with the retention it set, if any.
    Sent {
        retention: Option<u64>,
        ts_ms: Option<u64>,
    },
    RunFinished {
        ts_ms: Option<u64>,
    },
    RunPaused {
        ts_ms: Option<u64>,
    },
}

/// The retention seconds, the last activity and the deadline of a retained session whose records
/// say `events`, in record order. The latest retention a run start or send set wins; each event's
/// time is the session's latest activity; and a session whose latest run is active or paused has
/// no deadline.
pub(crate) fn policy_of(events: impl IntoIterator<Item = PolicyEvent>) -> (u64, u64, Option<u64>) {
    let mut seconds = 0;
    let mut activity = 0;
    let mut nonterminal = false;
    for event in events {
        match event {
            PolicyEvent::Started { retention, ts_ms } => {
                if let Some(value) = retention {
                    seconds = value;
                }
                activity = ts_ms.unwrap_or(activity);
                nonterminal = true;
            }
            PolicyEvent::Sent { retention, ts_ms } => {
                if let Some(value) = retention {
                    seconds = value;
                }
                if let Some(time) = ts_ms {
                    activity = time;
                }
            }
            PolicyEvent::RunFinished { ts_ms } => {
                activity = ts_ms.unwrap_or(activity);
                nonterminal = false;
            }
            PolicyEvent::RunPaused { ts_ms } => {
                activity = ts_ms.unwrap_or(activity);
                nonterminal = true;
            }
        }
    }
    let deadline = (!nonterminal).then_some(activity.saturating_add(seconds.saturating_mul(1000)));
    (seconds, activity, deadline)
}

type EvictCaches = dyn Fn(&BindIdentity) + Send + Sync;

type AcquireLease = dyn Fn(&BindIdentity) -> Result<Box<dyn LeaseHandle>, String> + Send + Sync;

/// Shared by actors and the registry. A deletion holds the session's lease.
pub struct RetentionService {
    /// Addresses of sessions whose records couldn't be read to decide their expiry.
    unreadable: Mutex<HashSet<String>>,
    pub clock: Clock,
    evict: std::sync::OnceLock<Arc<EvictCaches>>,
    acquire: Arc<AcquireLease>,
    store: store::StoreRetention,
}

impl RetentionService {
    /// The service for the state root at `state_root`, whose clock is anchored there, deleting
    /// sessions under leases from `leases`.
    pub fn new<L: LeaseStore + 'static>(state_root: &Path, leases: Arc<L>) -> io::Result<Self> {
        Ok(Self {
            unreadable: Mutex::default(),
            clock: Clock::for_state_root(state_root)?,
            evict: std::sync::OnceLock::new(),
            acquire: Arc::new(move |session| {
                leases
                    .acquire(&broca_core::lease_key_for(session))
                    .map_err(|e| e.to_string())
            }),
            store: store::StoreRetention::default(),
        })
    }

    pub(crate) fn set_evictor(&self, evict: Arc<EvictCaches>) {
        let _ = self.evict.set(evict);
    }

    pub(crate) fn lease(&self, key: &BindIdentity) -> Result<Box<dyn LeaseHandle>, String> {
        (self.acquire)(key)
    }

    /// How many sessions' records couldn't be read to decide their expiry.
    pub fn unreadable_sessions(&self) -> usize {
        self.unreadable.lock().unwrap().len()
    }

    fn mark_unreadable_address(&self, address: String) {
        if self.unreadable.lock().unwrap().insert(address.clone()) {
            tracing::error!(target: "retention", session = %address,
                "retention deletion blocked: the session's records can't be read");
        }
    }
}

#[path = "retention_store.rs"]
pub mod store;
