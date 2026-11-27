//! Authorization facts of closed sessions, independent of the display head and whole view.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use broca_core::{FirstEpisodeV1, SessionOwnershipV1};
use broca_protocol::BindIdentity;
use broca_records::{
    record::{FrozenFetchOutcome, FrozenPlan},
    session_storage::StorageClass,
};
use broca_session_loop::{owner, parts::Parts, plan};
use broca_session_store::{
    backend::SessionBackend,
    held::{serde_heap_bytes, HeldBytes},
    session::{Position, Session},
};
use serde_json::Value;

use crate::reader_admission::{self, ReadPolicy};
use crate::store_reads::{StoreReadReply, StoreReadRequest};

const MAX_ENTRIES: usize = 4096;
// Budget 8 KiB each for 1,024 closed sessions. Most entries are smaller; sessions with many
// principals, senders or readers can still exceed the budget for one entry.
const MAX_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct AuthorizationCache(Mutex<Kept>);

#[derive(Default)]
struct Kept {
    entries: HashMap<BindIdentity, Entry>,
    order: BTreeMap<u64, BindIdentity>,
    clock: u64,
    bytes: usize,
}

struct Entry {
    class: StorageClass,
    position: Position,
    facts: Arc<AuthorizationFacts>,
    bytes: usize,
    used: u64,
}

pub(crate) struct AuthorizationFacts {
    ownership: Option<OwnershipChecks>,
    read_policy: Option<ReadPolicy>,
}

/// Ownership as the authorization checks see it, without the first episode's tools or plan JSON.
#[derive(Debug, Clone, Default)]
pub struct OwnershipChecks {
    pub(crate) owner: SessionOwnershipV1,
    pub(crate) has_first_episode: bool,
    pub(crate) first_plan_identity: Option<String>,
    pub(crate) requires_hooks: bool,
    pub(crate) requires_approval: bool,
    // Upload standing uses the first plan, unlike interrupt grants after a plan refresh.
    pub(crate) first_senders: Vec<String>,
    pub(crate) interrupt_senders: Vec<String>,
}

impl OwnershipChecks {
    pub(crate) fn project(ownership: SessionOwnershipV1) -> Self {
        let first = ownership.first_episode.as_ref();
        let frozen = first.and_then(|first| first.plan.as_deref());
        let first_senders = frozen
            .map(|frozen| {
                broca_core::admission::plan_senders(&frozen.plan)
                    .into_iter()
                    .map(|sender| sender.module)
                    .collect()
            })
            .unwrap_or_default();
        let plan = frozen
            .and_then(|frozen| serde_json::from_str::<Value>(frozen.plan.as_json_text()).ok());
        let transforms = plan
            .as_ref()
            .and_then(|plan| plan.get("step_transform_items"))
            .and_then(Value::as_array);
        let requires_approval = transforms.is_some_and(|items| {
            items.iter().any(|item| {
                item.get("subscriptions")
                    .and_then(Value::as_array)
                    .is_some_and(|subscriptions| {
                        subscriptions.iter().any(|subscription| {
                            subscription.get("phase").and_then(Value::as_str) == Some("approve")
                        })
                    })
            })
        });
        let mut owner = ownership.for_owner_access();
        owner.reservation = ownership.reservation.clone();
        Self {
            owner,
            has_first_episode: first.is_some(),
            first_plan_identity: frozen.map(|frozen| frozen.plan_identity.clone()),
            requires_hooks: transforms.is_some_and(|items| !items.is_empty()),
            requires_approval,
            first_senders,
            interrupt_senders: ownership
                .effective_senders()
                .into_iter()
                .filter(|sender| sender.interrupt)
                .map(|sender| sender.module)
                .collect(),
        }
    }

    fn heap_bytes(&self) -> usize {
        serde_heap_bytes(&self.owner)
            + self.first_plan_identity.heap_bytes()
            + self.first_senders.heap_bytes()
            + self.interrupt_senders.heap_bytes()
    }
}

impl AuthorizationFacts {
    pub(crate) fn reply(&self, request: &StoreReadRequest) -> Option<StoreReadReply> {
        match request {
            StoreReadRequest::AuthorizationChecks => Some(StoreReadReply::AuthorizationChecks(Ok(
                Box::new(self.ownership.as_ref()?.clone()),
            ))),
            StoreReadRequest::AuthorizationRead(access) => {
                let decision = reader_admission::decide(self.read_policy.as_ref()?, access, None)?;
                Some(StoreReadReply::AuthorizationRead(decision))
            }
            _ => None,
        }
    }

    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + self
                .ownership
                .as_ref()
                .map_or(0, OwnershipChecks::heap_bytes)
            + self.read_policy.as_ref().map_or(0, ReadPolicy::heap_bytes)
    }
}

impl AuthorizationCache {
    fn lock(&self) -> MutexGuard<'_, Kept> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn get(
        &self,
        key: &BindIdentity,
        class: StorageClass,
        position: Position,
    ) -> Option<Arc<AuthorizationFacts>> {
        let mut kept = self.lock();
        kept.clock += 1;
        let clock = kept.clock;
        let entry = kept
            .entries
            .get_mut(key)
            .filter(|entry| entry.class == class && entry.position == position)?;
        let facts = Arc::clone(&entry.facts);
        let used = std::mem::replace(&mut entry.used, clock);
        let (_, key) = kept
            .order
            .remove_entry(&used)
            .expect("each entry has an order key");
        kept.order.insert(clock, key);
        Some(facts)
    }

    /// Learn from a verified closed view or a durable actor at the point it closes. An
    /// unreadable setup part can become readable later without a write; never cache that error.
    pub(crate) fn learn<B: SessionBackend>(
        &self,
        key: &BindIdentity,
        class: StorageClass,
        session: &Session<B>,
        parts: &dyn Parts,
        first: Option<&FirstEpisodeV1>,
    ) -> Arc<AuthorizationFacts> {
        let position = session.position();
        let previous = self.get(key, class, position);
        if let Some(facts) = &previous {
            if facts.ownership.is_some() && facts.read_policy.is_some() {
                return Arc::clone(facts);
            }
        }
        let facts = Arc::new(AuthorizationFacts {
            ownership: previous
                .as_ref()
                .and_then(|facts| facts.ownership.clone())
                .or_else(|| {
                    ownership(session, parts, first)
                        .ok()
                        .map(OwnershipChecks::project)
                }),
            read_policy: previous
                .as_ref()
                .and_then(|facts| facts.read_policy.clone())
                .or_else(|| reader_admission::read_policy(session, parts).ok()),
        });
        if facts.ownership.is_some() || facts.read_policy.is_some() {
            self.keep(key, class, position, Arc::clone(&facts));
        }
        facts
    }

    fn keep(
        &self,
        key: &BindIdentity,
        class: StorageClass,
        position: Position,
        facts: Arc<AuthorizationFacts>,
    ) {
        let key_bytes = Self::key_bytes(key);
        let facts =
            if key_bytes + facts.estimated_bytes() > MAX_BYTES && facts.read_policy.is_some() {
                // An oversized ownership summary need not displace the smaller read policy.
                Arc::new(AuthorizationFacts {
                    ownership: None,
                    read_policy: facts.read_policy.clone(),
                })
            } else {
                facts
            };
        let bytes = key_bytes + facts.estimated_bytes();
        if bytes > MAX_BYTES {
            return;
        }
        let mut kept = self.lock();
        kept.clock += 1;
        let used = kept.clock;
        if let Some(old) = kept.entries.insert(
            key.clone(),
            Entry {
                class,
                position,
                facts,
                bytes,
                used,
            },
        ) {
            kept.order.remove(&old.used);
            kept.bytes -= old.bytes;
        }
        kept.order.insert(used, key.clone());
        kept.bytes += bytes;
        while kept.entries.len() > MAX_ENTRIES || kept.bytes > MAX_BYTES {
            let (_, oldest) = kept
                .order
                .pop_first()
                .expect("an over-cap cache has entries");
            let old = kept
                .entries
                .remove(&oldest)
                .expect("each order key has an entry");
            kept.bytes -= old.bytes;
        }
    }

    fn key_bytes(key: &BindIdentity) -> usize {
        let key_bytes =
            key.project_root.capacity() + key.harness.capacity() + key.session.capacity();
        2 * key_bytes
            + std::mem::size_of::<(BindIdentity, Entry)>()
            + std::mem::size_of::<(u64, BindIdentity)>()
    }

    pub(crate) fn memory(&self) -> broca_wire::MemoryCache {
        let kept = self.lock();
        broca_wire::MemoryCache {
            entries: kept.entries.len() as u64,
            max_entries: MAX_ENTRIES as u64,
            counted_bytes: Some(kept.bytes as u64),
            max_bytes: Some(MAX_BYTES as u64),
            estimated_bytes: kept.bytes as u64,
            ..Default::default()
        }
    }
}

fn ownership<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    first: Option<&FirstEpisodeV1>,
) -> Result<SessionOwnershipV1, String> {
    let operations = &session.operations;
    let Some(first) = first.filter(|first| {
        !operations.owner.awaiting_import_admission()
            && operations
                .owner
                .first_start
                .as_ref()
                .is_some_and(|start| start.run_id == first.run_id.0)
    }) else {
        return owner::ownership_with_plan(operations, parts);
    };
    // The actor already resolved the immutable first start. Share its frozen plan rather
    // than building another copy from the setup parts. Sender grants still follow refreshes.
    let mut ownership = owner::ownership(&operations.owner)?;
    ownership.first_episode = Some(first.clone());
    if plan::refreshed(operations) {
        ownership.session_change_recorded = true;
        ownership.sender_admission = Some(broca_core::admission::SenderAdmissionV1 {
            version: 1,
            accepted_senders: Some(plan::senders(operations, parts)?),
        });
    }
    Ok(ownership)
}

/// The bytes of the frozen first plan, which `FirstEpisodeV1` skips during serialization.
pub(crate) fn frozen_plan_bytes(plan: &FrozenPlan) -> usize {
    2 * std::mem::size_of::<usize>()
        + std::mem::size_of_val(plan)
        + plan.plan.as_json_text().len()
        + plan.plan_identity.heap_bytes()
        + plan.composition_digest.heap_bytes()
        + plan.fetches.capacity() * std::mem::size_of::<broca_records::record::FrozenFetch>()
        + plan
            .fetches
            .iter()
            .map(|fetch| {
                fetch.provider.heap_bytes()
                    + match &fetch.outcome {
                        FrozenFetchOutcome::Answered { catalog_digest } => {
                            catalog_digest.heap_bytes()
                        }
                        FrozenFetchOutcome::Absent { reason } => reason.heap_bytes(),
                    }
            })
            .sum::<usize>()
}

#[cfg(test)]
#[path = "store_authorization_tests.rs"]
mod tests;
