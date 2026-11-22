//! Deleting what sealed changes left behind, in sessions kept in the session store that no actor
//! holds.
//!
//! A session's store actor collects its own session when it opens it and after each seal (see
//! `store_actor`). A session nobody opens again, such as a finished worker's, would keep the logs
//! and objects its changes replaced forever, so this sweep collects it instead. Every
//! [`SWEEP_INTERVAL_MS`] it reads the heads of up to [`HEADS_PER_PASS`] sessions, going on from
//! where the last pass stopped, and collects at most [`COLLECTS_PER_PASS`] of them: those whose
//! head holds deletions that have come due (`broca_session_store::session::collect_stored`).
//! Each pass logs what it read, collected and skipped.
//!
//! Only files a head stopped naming a grace period ago are deleted, so a read of the session
//! needs no exclusion. A writer does: a session the registry holds a slot for (an actor open,
//! opening or closing) is skipped, and the sweep takes the session's lease before it writes, and
//! skips the session when another owner holds the lease.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_core::{LeaseError, LeaseStore};
use broca_protocol::BindIdentity;
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::{self, HeadError};
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::session::{self, SessionError};
use tokio_util::sync::CancellationToken;

use crate::store_open::StoreSlot;

/// How often a pass runs. A change's deletions come due a day after it, so a pass every few
/// minutes deletes them soon after that without reading every head often.
pub const SWEEP_INTERVAL_MS: u64 = 10 * 60 * 1000;

/// The most session heads a pass reads. A head is one small file.
pub const HEADS_PER_PASS: usize = 1024;

/// The most sessions a pass collects. Collecting one deletes its due files and writes its head
/// once, under its lease.
pub const COLLECTS_PER_PASS: usize = 32;

/// Whether the registry holds a slot for a session: an actor serves it, or is opening or
/// closing it.
pub type Served = Arc<dyn Fn(&BindIdentity) -> bool + Send + Sync>;

/// The sweep over one state root's new storage.
pub struct StoreCollectSweep {
    state_root: PathBuf,
    slot: Arc<StoreSlot>,
    leases: Arc<dyn LeaseStore>,
    served: Served,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    heads_per_pass: usize,
    collects_per_pass: usize,
    /// The last session the previous pass read, by class and address; the next pass starts
    /// after it. `None` starts from the first.
    cursor: Mutex<Option<(usize, String)>>,
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepPass {
    /// Session heads read.
    pub read: usize,
    /// Sessions collected.
    pub collected: usize,
    /// Changes whose deletions ran, over every session collected.
    pub changes: usize,
    /// Sessions with deletions due that were left for a later pass: an actor or another owner
    /// holds them, or they are encrypted and the deployment key hasn't loaded.
    pub skipped: usize,
    /// Sessions whose collection failed; a later pass tries again.
    pub failed: usize,
}

/// What collecting one session came to.
enum Outcome {
    NothingDue,
    Collected(usize),
    Skipped,
    Failed(String),
}

impl StoreCollectSweep {
    /// The sweep over the session store under `state_root`, opened with `slot`'s runtime. It
    /// skips the sessions `served` names and holds each session it writes through `leases`,
    /// as an actor opening it would.
    pub fn new(
        state_root: impl Into<PathBuf>,
        slot: Arc<StoreSlot>,
        leases: Arc<dyn LeaseStore>,
        served: Served,
    ) -> Self {
        Self {
            state_root: state_root.into(),
            slot,
            leases,
            served,
            now_ms: Arc::new(|| broca_core::clock::epoch_millis().unwrap_or(0)),
            heads_per_pass: HEADS_PER_PASS,
            collects_per_pass: COLLECTS_PER_PASS,
            cursor: Mutex::new(None),
        }
    }

    /// The sweep, skipping the sessions `registry` holds a slot for.
    pub fn for_registry(
        state_root: impl Into<PathBuf>,
        slot: Arc<StoreSlot>,
        leases: Arc<dyn LeaseStore>,
        registry: crate::LineageRegistry,
    ) -> Self {
        let served: Served = Arc::new(move |key| registry.slot_state(key).is_some());
        Self::new(state_root, slot, leases, served)
    }

    /// Reads the time deletions come due by from `now_ms`, in place of the wall clock.
    #[cfg(test)]
    pub(crate) fn with_clock(mut self, now_ms: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.now_ms = now_ms;
        self
    }

    /// Reads at most `heads` session heads and collects at most `collects` sessions per pass.
    #[cfg(test)]
    pub(crate) fn with_bounds(mut self, heads: usize, collects: usize) -> Self {
        self.heads_per_pass = heads;
        self.collects_per_pass = collects;
        self
    }

    /// Runs a pass every [`SWEEP_INTERVAL_MS`], the first one interval after it starts, until
    /// `stopping` fires.
    pub async fn run(self: Arc<Self>, stopping: CancellationToken) {
        tracing::info!(
            target: "session_storage",
            interval_ms = SWEEP_INTERVAL_MS,
            heads_per_pass = self.heads_per_pass,
            collects_per_pass = self.collects_per_pass,
            grace_ms = session::GRACE_MS,
            "new storage collection sweep armed"
        );
        let period = Duration::from_millis(SWEEP_INTERVAL_MS);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stopping.cancelled() => return,
                _ = interval.tick() => {}
            }
            let sweep = Arc::clone(&self);
            let stop = stopping.clone();
            // Every file it touches is read and written in place, so the pass runs off the
            // runtime's workers.
            match tokio::task::spawn_blocking(move || sweep.sweep_once(&stop)).await {
                Ok(pass) => tracing::info!(
                    target: "session_storage",
                    read = pass.read,
                    collected = pass.collected,
                    changes = pass.changes,
                    skipped = pass.skipped,
                    failed = pass.failed,
                    "new storage collection sweep complete"
                ),
                Err(error) => tracing::warn!(
                    target: "session_storage",
                    error = %error,
                    "new storage collection sweep stopped; retrying next pass"
                ),
            }
        }
    }

    /// One pass: reads the next [`HEADS_PER_PASS`] session heads after the last pass's, and
    /// collects those with deletions due, at most [`COLLECTS_PER_PASS`] of them. Blocks on file
    /// reads and writes.
    pub fn sweep_once(&self, stopping: &CancellationToken) -> SweepPass {
        let _timing = broca_session_store::metrics::Phase::CollectionSweep
            .start()
            .with_session(|| None);
        let mut pass = SweepPass::default();
        // Until the session store's runtime opens, no session there is served or collected.
        let Some(runtime) = self.slot.runtime() else {
            return pass;
        };
        let deployment = runtime.deployment();
        let now_ms = (self.now_ms)();
        let sessions = self.sessions(&mut pass);
        let mut cursor = self.cursor.lock().unwrap();
        let start = cursor.as_ref().map_or(0, |after| {
            sessions
                .partition_point(|(class, addr)| (class.index(), &addr.0) <= (after.0, &after.1))
        });
        // Past the last session, the sweep starts again from the first.
        let start = if start >= sessions.len() { 0 } else { start };
        let mut last = None;
        for (index, (class, addr)) in sessions.iter().enumerate().skip(start) {
            if stopping.is_cancelled()
                || pass.read >= self.heads_per_pass
                || pass.collected >= self.collects_per_pass
            {
                break;
            }
            last = Some(index);
            pass.read += 1;
            match self.collect_one(*class, addr, deployment, now_ms) {
                Outcome::NothingDue => {}
                Outcome::Collected(changes) => {
                    pass.collected += 1;
                    pass.changes += changes;
                }
                Outcome::Skipped => pass.skipped += 1,
                Outcome::Failed(error) => {
                    pass.failed += 1;
                    tracing::warn!(
                        target: "session_storage",
                        class = %class,
                        session = %addr.0,
                        error = %error,
                        "a session's due deletions could not run; a later pass retries"
                    );
                }
            }
        }
        // The next pass goes on after the last session read, or from the first once this one
        // reached the end. A pass that read none leaves the place as it was.
        if let Some(index) = last {
            *cursor = (index + 1 < sessions.len()).then(|| {
                let (class, addr) = &sessions[index];
                (class.index(), addr.0.clone())
            });
        }
        pass
    }

    /// Every session folder in the session store's roots, ordered by class, then address. A pass
    /// reads the heads of only some of them, and a folder without a head reads as nothing due.
    fn sessions(&self, pass: &mut SweepPass) -> Vec<(StorageClass, SessionAddr)> {
        let mut sessions = Vec::new();
        for class in StorageClass::ALL {
            match LocalBackend::new(class.store_root(&self.state_root)).session_folders() {
                Ok(listed) => sessions.extend(listed.into_iter().map(|addr| (class, addr))),
                Err(error) => {
                    pass.failed += 1;
                    tracing::warn!(
                        target: "session_storage",
                        class = %class,
                        error = ?error,
                        "the new storage's sessions in a root could not be listed"
                    );
                }
            }
        }
        sessions.sort_by(|a, b| (a.0.index(), &a.1 .0).cmp(&(b.0.index(), &b.1 .0)));
        sessions
    }

    /// Collects the session at `addr` in the root of `class`, if its head holds deletions due
    /// at `now_ms` and nothing else writes it.
    fn collect_one(
        &self,
        class: StorageClass,
        addr: &SessionAddr,
        deployment: &DeploymentKey,
        now_ms: u64,
    ) -> Outcome {
        let _timing = broca_session_store::metrics::Phase::Collection
            .start()
            .with_session(|| Some(addr.0.clone()));
        let store = LocalBackend::new(class.store_root(&self.state_root));
        let bytes = match store.read_stored_head(addr) {
            Ok(Some((bytes, _))) => bytes,
            // A folder without a head: deleted since the listing, or one a creation or move left.
            Ok(None) => return Outcome::NothingDue,
            Err(error) => return Outcome::Failed(format!("its head couldn't be read: {error:?}")),
        };
        match session::stored_due(&bytes, now_ms) {
            Ok(true) => {}
            Ok(false) => return Outcome::NothingDue,
            Err(error) => return Outcome::Failed(format!("its head doesn't decode: {error:?}")),
        }
        // An encrypted head is checked under the deployment key before anything is deleted.
        if matches!(head::stored_cipher(&bytes), Ok(Cipher::XChaCha20Poly1305))
            && !deployment.is_loaded()
        {
            return Outcome::Skipped;
        }
        // The session as its head names it, which its lease is keyed by.
        let bind = match head::stored_triple(&bytes) {
            Ok(triple) => BindIdentity::new(
                PathBuf::from(triple.project_root),
                triple.harness,
                triple.session,
            ),
            Err(error) => return Outcome::Failed(format!("its head doesn't decode: {error:?}")),
        };
        if broca_core::session_addr(&bind) != addr.0 {
            return Outcome::Failed("its head names another session than its address".into());
        }
        if (self.served)(&bind) {
            return Outcome::Skipped;
        }
        let lease = match self.leases.acquire(&broca_core::lease_key_for(&bind)) {
            Ok(lease) => lease,
            Err(LeaseError::Held { .. }) => return Outcome::Skipped,
            Err(error) => return Outcome::Failed(format!("its lease couldn't be taken: {error}")),
        };
        // An actor that began opening the session before the lease was taken keeps it: the
        // actor collects it itself.
        if (self.served)(&bind) {
            drop(lease);
            return Outcome::Skipped;
        }
        let collected =
            futures::executor::block_on(session::collect_stored(&store, addr, deployment, now_ms));
        drop(lease);
        match collected {
            // Nothing was due once the head was read again under the lease, or the session's
            // creation or recovery hasn't committed.
            Ok(0) => Outcome::NothingDue,
            Ok(changes) => Outcome::Collected(changes),
            Err(SessionError::Head(HeadError::NotLoaded)) => Outcome::Skipped,
            Err(error) => Outcome::Failed(format!("{error:?}")),
        }
    }
}

#[cfg(test)]
mod tests;
