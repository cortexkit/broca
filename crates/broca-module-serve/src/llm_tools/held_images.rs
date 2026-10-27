//! Process-local image snapshots for consent-held calls; the durable row stores only handles.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use broca_protocol::{BindScope, SendScope};
use broca_store::grants::{GrantsStore, HeldCall, HeldState};

use crate::system_one::PreparedImages;

use super::admission::Admission;

// Sixteen model-batch per-image byte limits bound the retained image buffers across all
// consent holds. Count encoded buffer capacity, not decoded size, so base64 cannot exceed it.
pub(super) const HELD_IMAGE_MAX_BYTES: usize = cortexkit_model_batch::image::IMAGE_MAX_BYTES * 16;

#[derive(Clone, PartialEq, Eq)]
struct Binding {
    stamp: BindScope,
    digest: String,
    principal: String,
    call_key: String,
    batch_scope: broca_store::system_one::BatchScope,
    admitted_at_ms: i64,
}

impl Binding {
    fn matches(&self, row: &HeldCall) -> bool {
        row.scope == super::consent::identity(&self.stamp.scope)
            && row.args_digest == self.digest
            && row.principal == self.principal
            && row.call_key == self.call_key
            && row.batch_scope == self.batch_scope
            && row.admitted_at_ms == self.admitted_at_ms
    }
}

struct Entry {
    binding: Binding,
    images: Option<Arc<PreparedImages>>,
}

impl Entry {
    fn bytes(&self) -> usize {
        self.images
            .as_ref()
            .map_or(0, |images| images.retained_bytes())
    }
}

pub(super) struct KeptImages {
    pub images: Option<Arc<PreparedImages>>,
    pub stamp: BindScope,
}

#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    bytes: usize,
}

pub(super) struct HeldImages {
    state: Mutex<State>,
    limit: usize,
}

impl HeldImages {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            limit,
        }
    }

    pub(super) fn retain(
        &self,
        store: Arc<GrantsStore>,
        row: &HeldCall,
        stamp: &BindScope,
        images: Arc<PreparedImages>,
        now: i64,
    ) {
        let binding = Binding {
            stamp: stamp.clone(),
            digest: row.args_digest.clone(),
            principal: row.principal.clone(),
            call_key: row.call_key.clone(),
            batch_scope: row.batch_scope.clone(),
            admitted_at_ms: row.admitted_at_ms,
        };
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let size = images.retained_bytes();
        let inserted = size > 0 && !state.entries.contains_key(&row.batch_id);
        if inserted {
            let images = if size <= self.limit.saturating_sub(state.bytes) {
                state.bytes += size;
                Some(images)
            } else {
                None
            };
            // Budget exhaustion drops only the bytes, never the original resolution scope.
            state.entries.insert(
                row.batch_id.clone(),
                Entry {
                    binding: binding.clone(),
                    images,
                },
            );
        }
        drop(state);
        // Finalization can race insertion on another thread after the admission commit.
        // Recheck outside the image lock; later finalizations invoke synchronous cleanup.
        if inserted
            && !store
                .lookup(&row.batch_id, now)
                .ok()
                .flatten()
                .is_some_and(|current| {
                    current.state != HeldState::Final && binding.matches(&current)
                })
        {
            self.remove(&row.batch_id, row.admitted_at_ms);
        }
    }

    pub(super) fn get(
        &self,
        row: &HeldCall,
        send: &SendScope,
        admission: &Admission,
    ) -> Result<Option<KeptImages>, cortexkit_role_classifier::errors::Refusal> {
        if row.state != HeldState::Authorized
            || row.args_digest != admission.args_digest
            || row.batch_id != admission.batch_id
            || row.batch_scope != admission.scope
        {
            return Err(super::admission::refusal(
                "batch_scope_mismatch",
                "held image snapshot belongs to another call",
            ));
        }
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let Some(entry) = state.entries.get(&row.batch_id) else {
            return Ok(None);
        };
        // Internal recovery uses the bound durable row; an external retry must also carry
        // exactly the original daemon stamp, including its parent and attribution.
        if !entry.binding.matches(row)
            || match send {
                SendScope::Internal => false,
                SendScope::Stamped(stamp) => stamp != &entry.binding.stamp,
                _ => true,
            }
        {
            return Err(super::admission::refusal(
                "batch_scope_mismatch",
                "held image snapshot belongs to another stamped call",
            ));
        }
        Ok(Some(KeptImages {
            images: entry.images.clone(),
            stamp: entry.binding.stamp.clone(),
        }))
    }

    pub(super) fn remove(&self, key: &str, admitted_at_ms: i64) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state
            .entries
            .get(key)
            .is_some_and(|entry| entry.binding.admitted_at_ms == admitted_at_ms)
        {
            if let Some(entry) = state.entries.remove(key) {
                state.bytes -= entry.bytes();
            }
        }
    }

    pub(super) fn entries(&self) -> Vec<(String, i64, broca_protocol::SessionScope)> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .iter()
            .map(|(key, entry)| {
                (
                    key.clone(),
                    entry.binding.admitted_at_ms,
                    entry.binding.stamp.scope.clone(),
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).bytes
    }

    #[cfg(test)]
    pub(super) fn contains(&self, key: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .contains_key(key)
    }
}
