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
/// Every this many passes, a pass reads the whole ephemeral root: it lists every session folder
/// and checks every head for a change. The passes between read only the sessions whose kept
/// deadline has come. Listing the root's tens of thousands of folders costs about 100 MB of disk
/// metadata reads each time, so the full pass runs about once an hour. A session created or
/// changed since the last full pass is found by the next one; its reads answer expired from its
/// own records until then (`store_expiry`), so a late deletion frees disk late and nothing else.
pub const FULL_PASS_EVERY: u32 = 12;

/// Whether the next pass is full, and the count of passes since the last full one to carry to
/// the pass after it. `since_full` starts at 0, so the first pass is full. A pass the clock's
/// change started is full too, so a test that moves the clock sees every session.
pub(crate) fn next_pass(since_full: u32, clock_changed: bool) -> (bool, u32) {
    let full = clock_changed || since_full == 0;
    let since_full = if full {
        1
    } else {
        (since_full + 1) % FULL_PASS_EVERY
    };
    (full, since_full)
}

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
/// say `events`, in record order ([`PolicyFold`]).
pub(crate) fn policy_of(events: impl IntoIterator<Item = PolicyEvent>) -> (u64, u64, Option<u64>) {
    let mut fold = PolicyFold::default();
    for event in events {
        fold.apply(event);
    }
    fold.decided()
}

/// A retained session's retention as its records say so far, folded one record at a time, so a
/// reader that has folded a session's older records folds only the ones appended since.
///
/// The shortest retention any run start or send set wins: a send may shorten a session's
/// retention but never lengthen it, and a send queued behind one that shortened it was admitted
/// against that shorter one, whatever seconds its own record carries. Each event's time is the
/// session's latest activity, and a session whose latest run is active or paused has no deadline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PolicyFold {
    seconds: Option<u64>,
    activity: u64,
    nonterminal: bool,
}

impl PolicyFold {
    pub(crate) fn apply(&mut self, event: PolicyEvent) {
        match event {
            PolicyEvent::Started { retention, ts_ms } => {
                self.shorten(retention);
                self.activity = ts_ms.unwrap_or(self.activity);
                self.nonterminal = true;
            }
            PolicyEvent::Sent { retention, ts_ms } => {
                self.shorten(retention);
                if let Some(time) = ts_ms {
                    self.activity = time;
                }
            }
            PolicyEvent::RunFinished { ts_ms } => {
                self.activity = ts_ms.unwrap_or(self.activity);
                self.nonterminal = false;
            }
            PolicyEvent::RunPaused { ts_ms } => {
                self.activity = ts_ms.unwrap_or(self.activity);
                self.nonterminal = true;
            }
        }
    }

    fn shorten(&mut self, retention: Option<u64>) {
        if let Some(value) = retention {
            self.seconds = Some(self.seconds.map_or(value, |seconds| seconds.min(value)));
        }
    }

    /// The shortest retention a run start or send set so far, if any set one.
    pub(crate) fn seconds(&self) -> Option<u64> {
        self.seconds
    }

    /// The retention seconds, the last activity and the deadline the events so far decide.
    pub(crate) fn decided(&self) -> (u64, u64, Option<u64>) {
        let seconds = self.seconds.unwrap_or(0);
        let deadline = (!self.nonterminal)
            .then_some(self.activity.saturating_add(seconds.saturating_mul(1000)));
        (seconds, self.activity, deadline)
    }
}

type EvictCaches = dyn Fn(&BindIdentity) + Send + Sync;

type AcquireLease = dyn Fn(&BindIdentity) -> Result<Box<dyn LeaseHandle>, String> + Send + Sync;

/// Shared by actors and the registry. A deletion holds the session's lease.
/// A call a provider still holds for a session about to be deleted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldAtDeletion {
    pub module: String,
    pub call_key: String,
    /// Whether the withdrawal names the session, as the call's run plan declared for `module`.
    pub session_identity: bool,
}

/// Withdraws the held calls of a session about to be deleted, while its ledger still names
/// them. A provider keeps running a held call after its route closes (tool-provider contract,
/// custody of keyed calls), and once the session is gone nothing would ever collect the result,
/// so each is withdrawn first. The deletion waits for one attempt per call, bounded, and goes
/// ahead whatever the answer; a withdrawal still owed is retried after the deletion.
#[async_trait::async_trait]
pub trait WithdrawBeforeDelete: Send + Sync {
    async fn withdraw_before_delete(&self, calls: Vec<HeldAtDeletion>);
}

pub struct RetentionService {
    /// Addresses of sessions whose records couldn't be read to decide their expiry.
    unreadable: Mutex<HashSet<String>>,
    pub clock: Clock,
    evict: std::sync::OnceLock<Arc<EvictCaches>>,
    /// Withdraws the calls a provider still holds for a session about to be deleted.
    withdraw: std::sync::OnceLock<Arc<dyn WithdrawBeforeDelete>>,
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
            withdraw: std::sync::OnceLock::new(),
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

    /// Installs what withdraws a session's held calls before the session is deleted. Without
    /// it, a session is deleted without withdrawing them.
    pub fn set_withdraw_before_delete(&self, withdraw: Arc<dyn WithdrawBeforeDelete>) {
        let _ = self.withdraw.set(withdraw);
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

#[path = "retention_policy.rs"]
pub mod policy;
#[path = "retention_store.rs"]
pub mod store;
