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
