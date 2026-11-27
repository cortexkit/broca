//! A closed position at which the reader need not open a recovering actor before serving status.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use broca_protocol::BindIdentity;
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::{SessionAddr, SessionBackend};
use broca_session_store::session::{Position, Session};

use super::StoreRuntime;
use crate::store_reads::{StoreReadReply, StoreReadRequest};

const MAX_POSITIONS: usize = 1024;
// Include the full bind identity, not only the store address, which is a short hash.
const MAX_KEY_BYTES: usize = 512;

#[derive(Default)]
pub(super) struct ParkedStatus(Mutex<Positions>);

#[derive(Default)]
struct Positions {
    entries: HashMap<BindIdentity, SafePosition>,
    order: VecDeque<BindIdentity>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SafePosition {
    class: StorageClass,
    at: Position,
    incarnation: [u8; 16],
    history_version: u64,
}

impl Positions {
    fn remove(&mut self, key: &BindIdentity) {
        if self.entries.remove(key).is_some() {
            self.order.retain(|older| older != key);
        }
    }
}

impl ParkedStatus {
    fn lock(&self) -> std::sync::MutexGuard<'_, Positions> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl StoreRuntime {
    /// Learn from a validated closed view or the durable session at idle close. A queued
    /// send can become ready even if the position hasn't changed, so don't keep queued work.
    pub(crate) fn keep_parked_status<B: SessionBackend>(
        &self,
        key: &BindIdentity,
        class: StorageClass,
        session: &Session<B>,
    ) {
        if key.project_root.as_os_str().len() + key.harness.len() + key.session.len()
            > MAX_KEY_BYTES
        {
            return;
        }
        let mut cache = self.parked_status.lock();
        if !session.operations.queue.is_empty()
            || crate::store_actor::recovery_changes_reads(&session.operations)
        {
            cache.remove(key);
            return;
        }
        let position = SafePosition {
            class,
            at: session.position(),
            incarnation: session.head().incarnation.0,
            history_version: session.head().history_version,
        };
        if cache.entries.get(key) == Some(&position) {
            return;
        }
        cache.remove(key);
        if cache.entries.len() == MAX_POSITIONS {
            if let Some(old) = cache.order.pop_front() {
                cache.entries.remove(&old);
            }
        }
        cache.entries.insert(key.clone(), position);
        cache.order.push_back(key.clone());
    }

    /// After checking session expiry, use cached run facts without opening a whole view only
    /// if the closed session is still at the position known not to need recovery.
    pub(super) async fn read_parked_status(
        &self,
        registry: &crate::LineageRegistry,
        key: &BindIdentity,
        class: StorageClass,
        checked_at: Option<Position>,
        request: &StoreReadRequest,
    ) -> Option<(StoreReadReply, Position)> {
        let run = request.parked_run()?;
        if registry.slot_state(key).is_some() {
            return None;
        }
        let cached = *self.parked_status.lock().entries.get(key)?;
        let addr = broca_core::session_addr(key);
        let current = match checked_at {
            Some(at) => Some(at),
            None => Session::stored_position(
                &self.backend(class),
                &SessionAddr(addr.clone()),
                self.deployment(),
            )
            .await
            .ok()
            .flatten(),
        };
        if cached.class != class || current != Some(cached.at) {
            let mut cache = self.parked_status.lock();
            if cache.entries.get(key) == Some(&cached) {
                cache.remove(key);
            }
            return None;
        }
        if registry.slot_state(key).is_some() {
            return None;
        }
        let run_key = (
            addr,
            cached.incarnation,
            cached.history_version,
            run.0.clone(),
        );
        let settled = self.settled_runs();
        let reply = match request {
            StoreReadRequest::RunAdmission(_) => {
                StoreReadReply::RunAdmission(Ok(Some(settled.parked_admission(&run_key)?)))
            }
            StoreReadRequest::RunStatus(_) | StoreReadRequest::RunStatusReply(_) => {
                let status = settled.parked_status(
                    &run_key,
                    crate::store_awaits::Labels(Some(self.executor.clone())),
                )?;
                if matches!(request, StoreReadRequest::RunStatus(_)) {
                    StoreReadReply::RunStatus(status)
                } else {
                    StoreReadReply::RunStatusReply(status.map(Into::into))
                }
            }
            _ => return None,
        };
        Some((reply, cached.at))
    }
}
