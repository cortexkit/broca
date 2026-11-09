//! Where a run's setup parts (tool list, response format, system text, plan) are stored.
//!
//! A run's start names its setup parts by content digest, so a part shared by many runs is
//! stored once. Each part is the canonical JSON (JCS) of its value; its digest is the SHA-256
//! of those bytes, as the converter computes it, so converted and new runs name equal parts
//! alike.

use std::collections::BTreeMap;

use broca_session_store::codec::Digest;
use broca_session_store::record::{PartKind, PartRef};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// Stores setup parts and reads them back. `Sync` as well as `Send`, because a run borrows its
/// parts across awaits and a run's future must stay `Send`.
pub trait Parts: Send + Sync {
    /// Stores `value` as a part of `kind` and names it.
    fn put(&mut self, kind: PartKind, value: &Value) -> Result<PartRef, String>;

    /// The value a part holds.
    fn get(&self, part: &PartRef) -> Result<Value, String>;

    /// The parts put here that aren't durable yet, with their bytes. A writer stores each
    /// before it appends a record, so no record names a part only memory holds. A store that
    /// keeps nothing durable, such as one for tests, lists none.
    fn unstored(&self) -> Vec<(PartRef, Vec<u8>)> {
        Vec::new()
    }

    /// Notes that `parts` are durable now.
    fn mark_stored(&mut self, _parts: &[PartRef]) {}
}

/// Parts held in memory, for tests and for runs whose parts the caller keeps elsewhere.
#[derive(Default)]
pub struct MemoryParts {
    pub parts: BTreeMap<Digest, Vec<u8>>,
}

/// A part's stored bytes and their digest.
pub fn canonical(value: &Value) -> Result<(Vec<u8>, Digest), String> {
    let bytes = serde_jcs::to_vec(value).map_err(|e| e.to_string())?;
    let digest = Digest(Sha256::digest(&bytes).into());
    Ok((bytes, digest))
}

/// The digest of what renderers read from a run's render config when rendering other runs'
/// messages (`broca_provider`'s `episode_render_key`), which a setup marker records so the
/// conversation keeps runs past the window only where it changes.
pub fn render_key(render_config: &Value) -> Result<Digest, String> {
    canonical(
        &broca_provider::families::openai_responses_chatgpt::episode_render_key(render_config),
    )
    .map(|(_, digest)| digest)
}

impl Parts for MemoryParts {
    fn put(&mut self, kind: PartKind, value: &Value) -> Result<PartRef, String> {
        let (bytes, digest) = canonical(value)?;
        self.parts.entry(digest).or_insert(bytes);
        Ok(PartRef { kind, digest })
    }

    fn get(&self, part: &PartRef) -> Result<Value, String> {
        let bytes = self
            .parts
            .get(&part.digest)
            .ok_or_else(|| "setup part missing".to_string())?;
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }
}

/// One parts store shared by several holders: a session's actor and its run both store parts
/// in it, the actor while the run holds its own handle. Each call takes the lock for that call
/// only, and parts calls never wait on anything else, so neither holder blocks the other for
/// long.
#[derive(Clone)]
pub struct SharedParts(pub std::sync::Arc<std::sync::Mutex<Box<dyn Parts>>>);

impl SharedParts {
    pub fn new(parts: Box<dyn Parts>) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(parts)))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Box<dyn Parts>>, String> {
        self.0
            .lock()
            .map_err(|_| "a holder of the parts store panicked while using it".to_owned())
    }
}

impl Parts for SharedParts {
    fn put(&mut self, kind: PartKind, value: &Value) -> Result<PartRef, String> {
        self.lock()?.put(kind, value)
    }

    fn get(&self, part: &PartRef) -> Result<Value, String> {
        self.lock()?.get(part)
    }

    fn unstored(&self) -> Vec<(PartRef, Vec<u8>)> {
        self.lock()
            .map(|parts| parts.unstored())
            .unwrap_or_default()
    }

    fn mark_stored(&mut self, parts: &[PartRef]) {
        if let Ok(mut held) = self.lock() {
            held.mark_stored(parts);
        }
    }
}

/// The deployment's setup parts, shared by every session in the process: what its setup store
/// held when the process loaded it, and what runs have put since. A part put here is held in
/// memory and listed by [`Parts::unstored`] until a writer stores it. The list is the
/// deployment's, not a session's: one session may name a part another session put first, so
/// whichever writer appends first stores it.
#[derive(Clone, Default)]
pub struct DeploymentParts(std::sync::Arc<std::sync::Mutex<DeploymentState>>);

#[derive(Default)]
struct DeploymentState {
    parts: BTreeMap<Digest, std::sync::Arc<[u8]>>,
    unstored: BTreeMap<Digest, PartKind>,
}

impl DeploymentParts {
    /// The parts the deployment's setup store holds, as its `load_all` read them.
    pub fn loaded(parts: impl IntoIterator<Item = (Digest, std::sync::Arc<[u8]>)>) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(
            DeploymentState {
                parts: parts.into_iter().collect(),
                unstored: BTreeMap::new(),
            },
        )))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, DeploymentState>, String> {
        self.0
            .lock()
            .map_err(|_| "a holder of the deployment's parts panicked while using them".to_owned())
    }
}

impl Parts for DeploymentParts {
    fn put(&mut self, kind: PartKind, value: &Value) -> Result<PartRef, String> {
        let (bytes, digest) = canonical(value)?;
        let mut state = self.lock()?;
        if let std::collections::btree_map::Entry::Vacant(entry) = state.parts.entry(digest) {
            entry.insert(bytes.into());
            state.unstored.insert(digest, kind);
        }
        Ok(PartRef { kind, digest })
    }

    fn get(&self, part: &PartRef) -> Result<Value, String> {
        let state = self.lock()?;
        let bytes = state
            .parts
            .get(&part.digest)
            .ok_or_else(|| "setup part missing".to_string())?;
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }

    fn unstored(&self) -> Vec<(PartRef, Vec<u8>)> {
        let Ok(state) = self.lock() else {
            return Vec::new();
        };
        state
            .unstored
            .iter()
            .filter_map(|(digest, kind)| {
                let bytes = state.parts.get(digest)?;
                Some((
                    PartRef {
                        kind: *kind,
                        digest: *digest,
                    },
                    bytes.to_vec(),
                ))
            })
            .collect()
    }

    fn mark_stored(&mut self, parts: &[PartRef]) {
        if let Ok(mut state) = self.lock() {
            for part in parts {
                state.unstored.remove(&part.digest);
            }
        }
    }
}
