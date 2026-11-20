//! Where a run's setup parts (tool list, response format, system text, plan) are stored.
//!
//! A run's start names its setup parts by content digest, so a part shared by many runs is
//! stored once. Each part is the canonical JSON (JCS) of its value; its digest is the SHA-256
//! of those bytes, as the converter computes it, so converted and new runs name equal parts
//! alike.

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

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

    /// The decoded values of this store's parts that their holders share (see
    /// [`DecodedParts`]). `None` for a store that keeps none, so every read decodes again.
    fn decoded(&self) -> Option<DecodedParts> {
        None
    }

    /// What this store holds and how it has been read since it was made, for the memory
    /// report. `None` for a store that doesn't count.
    fn usage(&self) -> Option<PartsUsage> {
        None
    }
}

/// What a deployment's parts store holds and how it has been read since the process started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartsUsage {
    /// Stored parts held in memory, and their bytes.
    pub held_parts: u64,
    pub held_bytes: u64,
    /// Bytes of parts put here and not stored yet.
    pub unstored_bytes: u64,
    /// Parts read from the setup store because memory didn't hold them, and their bytes.
    pub store_reads: u64,
    pub store_read_bytes: u64,
    /// Of those reads, the ones of a part memory held until the held bytes' bound dropped it
    /// recently: reads a larger bound would have saved.
    pub reads_after_eviction: u64,
    /// Parts decoded from their bytes into a JSON value, and the bytes decoded.
    pub decodes: u64,
    pub decoded_bytes: u64,
}

/// Parts held in memory, for tests and for runs whose parts the caller keeps elsewhere. It keeps
/// no decoded parts of its own: a [`SharedParts`] over it does.
#[derive(Default)]
pub struct MemoryParts {
    pub parts: BTreeMap<Digest, Vec<u8>>,
}

/// Setup parts already decoded, shared by every session and run of the process that uses them.
///
/// An entry is keyed by the part's digest and the type it was decoded as, so one part decoded
/// as two types is two entries. The cache holds each value weakly: the value is freed when its
/// last holder drops it, and the dead entry goes at the next insert. While any holder keeps a
/// value, every later read of the same part as the same type gets that value and decodes
/// nothing. A shared value is never changed: a holder that needs another value makes its own.
#[derive(Clone, Default)]
pub struct DecodedParts(Arc<std::sync::Mutex<Entries>>);

/// The decoded-part cache's entries: each value held weakly, by its part's digest and the type
/// it was decoded as.
type Entries = HashMap<(Digest, TypeId), Weak<dyn Any + Send + Sync>>;

impl DecodedParts {
    /// How many entries the cache holds, live or not yet removed.
    pub fn len(&self) -> usize {
        self.entries().len()
    }

    /// Whether the cache holds no entry.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The value held under `key` as a `T` while any holder keeps it, otherwise what `decode`
    /// gives, which later reads then share. Two threads that miss together both decode, and
    /// the second to finish takes the first one's value. `decode` may read other entries.
    pub fn get_or_decode<T: Send + Sync + 'static>(
        &self,
        key: Digest,
        decode: impl FnOnce() -> Result<T, String>,
    ) -> Result<Arc<T>, String> {
        let slot = (key, TypeId::of::<T>());
        if let Some(held) = live::<T>(&self.entries(), &slot) {
            return Ok(held);
        }
        // Decoded without the lock held, so a decode that reads other entries can't deadlock.
        let decoded = Arc::new(decode()?);
        let mut entries = self.entries();
        if let Some(held) = live::<T>(&entries, &slot) {
            return Ok(held);
        }
        entries.retain(|_, entry| entry.strong_count() > 0);
        let entry: Weak<T> = Arc::downgrade(&decoded);
        entries.insert(slot, entry);
        Ok(decoded)
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, Entries> {
        // Every change to the map is a single insert or removal, so a holder that panicked
        // left it whole.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The value `entries` holds under `slot`, while any holder keeps it.
fn live<T: Send + Sync + 'static>(entries: &Entries, slot: &(Digest, TypeId)) -> Option<Arc<T>> {
    entries.get(slot)?.upgrade()?.downcast::<T>().ok()
}

thread_local! {
    /// How many times this thread decoded a part's stored bytes.
    static DECODES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How many times this thread decoded a part's stored bytes, through any store here. Tests and
/// measurements read it to see which reads decode and which share a decoded value.
pub fn decodes_on_this_thread() -> u64 {
    DECODES.with(std::cell::Cell::get)
}

/// A part's stored bytes decoded, counted for [`decodes_on_this_thread`].
fn decode_bytes(bytes: &[u8]) -> Result<Value, String> {
    DECODES.with(|count| count.set(count.get() + 1));
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}

/// `part`'s value decoded as a `T`, shared with every other holder of the same part as a `T`
/// in the process (see [`DecodedParts`]). It is exactly what decoding the part's value as a `T`
/// gives; a store that keeps no decoded parts decodes it on every call.
pub fn shared<T, P>(parts: &P, part: &PartRef) -> Result<Arc<T>, String>
where
    T: serde::de::DeserializeOwned + Send + Sync + 'static,
    P: Parts + ?Sized,
{
    shared_or(parts, part, |error| error.to_string())
}

/// [`shared`], with `invalid` saying why a part's value isn't a `T`.
pub fn shared_or<T, P>(
    parts: &P,
    part: &PartRef,
    invalid: impl FnOnce(serde_json::Error) -> String,
) -> Result<Arc<T>, String>
where
    T: serde::de::DeserializeOwned + Send + Sync + 'static,
    P: Parts + ?Sized,
{
    let decode = || serde_json::from_value(parts.get(part)?).map_err(invalid);
    match parts.decoded() {
        Some(decoded) => decoded.get_or_decode(part.digest, decode),
        None => decode().map(Arc::new),
    }
}

/// `part`'s value as an owned JSON value, for a reader that builds a value of its own from it.
/// Read through [`shared`]: while another holder keeps the part decoded as a `Value`, this
/// copies that value instead of decoding the part again.
pub fn value<P: Parts + ?Sized>(parts: &P, part: &PartRef) -> Result<Value, String> {
    shared::<Value, P>(parts, part).map(Arc::unwrap_or_clone)
}

/// The key a value derived from other decoded values is shared under: `what` names the
/// derivation, `from` the digests it is derived from, `rest` any other input, so no two
/// derivations and no part share a key.
pub fn derived_key<'a>(
    what: &str,
    from: impl IntoIterator<Item = (PartKind, &'a Digest)>,
    rest: &[u8],
) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"broca derived setup value\0");
    hasher.update(what.as_bytes());
    hasher.update([0]);
    for (kind, digest) in from {
        hasher.update(format!("{kind:?}").as_bytes());
        hasher.update([0]);
        hasher.update(digest.0);
    }
    hasher.update((rest.len() as u64).to_be_bytes());
    hasher.update(rest);
    Digest(hasher.finalize().into())
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
        decode_bytes(bytes)
    }
}

/// One parts store shared by several holders: a session's actor and its run both store parts
/// in it, the actor while the run holds its own handle. Each call takes the lock for that call
/// only, and parts calls never wait on anything else, so neither holder blocks the other for
/// long. The holders share decoded parts too: the store's own when it keeps them, otherwise
/// the ones this handle and its clones keep.
#[derive(Clone)]
pub struct SharedParts(
    pub std::sync::Arc<std::sync::Mutex<Box<dyn Parts>>>,
    DecodedParts,
);

impl SharedParts {
    pub fn new(parts: Box<dyn Parts>) -> Self {
        Self(
            std::sync::Arc::new(std::sync::Mutex::new(parts)),
            DecodedParts::default(),
        )
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

    fn decoded(&self) -> Option<DecodedParts> {
        let held = self.lock().ok().and_then(|parts| parts.decoded());
        Some(held.unwrap_or_else(|| self.1.clone()))
    }

    fn usage(&self) -> Option<PartsUsage> {
        self.lock().ok().and_then(|parts| parts.usage())
    }
}

/// Where a deployment's stored parts are read from once memory holds them no longer.
pub trait StoredParts: Send + Sync {
    /// The stored bytes of the part with `digest`, checked against it; `None` when no such part
    /// is stored.
    fn read(&self, digest: &Digest) -> Result<Option<Arc<[u8]>>, String>;
}

/// A setup store's parts in one cipher mode, read on the caller's task, as the store's other
/// reads are. A read of the local backend returns at once; a backend whose reads would wait
/// can't be read here, and the read says so rather than block.
pub struct SetupSource<B> {
    pub backend: B,
    pub key: broca_session_store::setup::SetupKey,
}

impl<B: broca_session_store::backend::SessionBackend + Send + Sync> StoredParts for SetupSource<B> {
    fn read(&self, digest: &Digest) -> Result<Option<Arc<[u8]>>, String> {
        use futures::FutureExt as _;
        broca_session_store::setup::SetupStore::new(&self.backend, &self.key)
            .read(digest)
            .now_or_never()
            .ok_or("the setup store's backend can't be read inline")?
            .map_err(|error| format!("a stored setup part can't be read: {error:?}"))
    }
}

/// Parts held in a map and read from it: the stored parts of a deployment a test loaded.
struct MapSource(BTreeMap<Digest, Arc<[u8]>>);

impl StoredParts for MapSource {
    fn read(&self, digest: &Digest) -> Result<Option<Arc<[u8]>>, String> {
        Ok(self.0.get(digest).cloned())
    }
}

/// How many bytes of recently read or stored parts a deployment keeps in memory. Every run
/// start names its parts, and most runs name the same few (a project's tools, a role's system
/// text), so those stay held; a part only an old session names is read from the store when that
/// session runs again. The store holds every part ever stored, converted sessions' included:
/// 177,000 parts and 385 MB at one count, all of which this process once held at once.
pub const HELD_PART_BYTES: usize = 32 * 1024 * 1024;

/// The deployment's setup parts, shared by every session in the process. A part put here is held
/// in memory and listed by [`Parts::unstored`] until a writer stores it: the list is the
/// deployment's, not a session's, since one session may name a part another session put first,
/// so whichever writer appends first stores it. A stored part is read from the setup store when
/// it is named, and the most recently named stay held, up to [`HELD_PART_BYTES`]. Their decoded
/// values are shared the same way.
#[derive(Clone, Default)]
pub struct DeploymentParts {
    state: Arc<std::sync::Mutex<DeploymentState>>,
    decoded: DecodedParts,
    /// Where stored parts are read from; none for a deployment that stores nothing.
    stored: Option<Arc<dyn StoredParts>>,
    /// How its parts were read, for [`Parts::usage`].
    counts: Arc<ReadCounts>,
}

#[derive(Default)]
struct ReadCounts {
    store_reads: AtomicU64,
    store_read_bytes: AtomicU64,
    reads_after_eviction: AtomicU64,
    decodes: AtomicU64,
    decoded_bytes: AtomicU64,
}

/// How many of the parts the held bytes' bound dropped last are remembered, so a store read of
/// one of them counts as a read a larger bound would have saved.
const EVICTIONS_REMEMBERED: usize = 4096;

#[derive(Default)]
struct DeploymentState {
    /// Parts put here that no writer has stored yet, kept until one does.
    unstored: BTreeMap<Digest, (PartKind, Arc<[u8]>)>,
    held: HeldParts,
}

/// Parts read or stored recently, the least recently named dropped first once they hold more
/// than [`HELD_PART_BYTES`].
#[derive(Default)]
struct HeldParts {
    parts: HashMap<Digest, (Arc<[u8]>, u64)>,
    /// Each held part's digest by when it was last named.
    order: BTreeMap<u64, Digest>,
    bytes: usize,
    clock: u64,
    /// The digests the bound dropped last, oldest first, at most [`EVICTIONS_REMEMBERED`].
    evicted: VecDeque<Digest>,
    evicted_set: HashSet<Digest>,
}

impl HeldParts {
    fn get(&mut self, digest: &Digest) -> Option<Arc<[u8]>> {
        let (bytes, named) = self.parts.get_mut(digest)?;
        self.order.remove(named);
        self.clock += 1;
        *named = self.clock;
        self.order.insert(self.clock, *digest);
        Some(Arc::clone(bytes))
    }

    fn hold(&mut self, digest: Digest, bytes: Arc<[u8]>) {
        if self.get(&digest).is_some() {
            return;
        }
        self.clock += 1;
        self.bytes += bytes.len();
        self.parts.insert(digest, (bytes, self.clock));
        self.order.insert(self.clock, digest);
        while self.bytes > HELD_PART_BYTES {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some((bytes, _)) = self.parts.remove(&oldest) {
                self.bytes -= bytes.len();
                self.remember_eviction(oldest);
            }
        }
    }

    fn remember_eviction(&mut self, digest: Digest) {
        if self.evicted.len() == EVICTIONS_REMEMBERED {
            if let Some(forgotten) = self.evicted.pop_front() {
                self.evicted_set.remove(&forgotten);
            }
        }
        if self.evicted_set.insert(digest) {
            self.evicted.push_back(digest);
        }
    }

    /// Whether the bound dropped `digest` recently; a part read back is no longer counted so.
    fn take_eviction(&mut self, digest: &Digest) -> bool {
        if !self.evicted_set.remove(digest) {
            return false;
        }
        self.evicted.retain(|evicted| evicted != digest);
        true
    }
}

impl DeploymentParts {
    /// The parts of a deployment whose setup store is `stored`, none of them held yet.
    pub fn stored(stored: Arc<dyn StoredParts>) -> Self {
        Self {
            stored: Some(stored),
            ..Self::default()
        }
    }

    /// A deployment whose stored parts are `parts`, as a setup store holding them would serve
    /// them.
    pub fn loaded(parts: impl IntoIterator<Item = (Digest, Arc<[u8]>)>) -> Self {
        Self::stored(Arc::new(MapSource(parts.into_iter().collect())))
    }

    /// The bytes of the parts held in memory now, stored or not.
    pub fn held_bytes(&self) -> usize {
        self.lock().map_or(0, |state| {
            state.held.bytes
                + state
                    .unstored
                    .values()
                    .map(|(_, bytes)| bytes.len())
                    .sum::<usize>()
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, DeploymentState>, String> {
        self.state
            .lock()
            .map_err(|_| "a holder of the deployment's parts panicked while using them".to_owned())
    }

    /// The bytes of the part with `digest`, held, not yet stored, or read from the store.
    fn bytes(&self, digest: &Digest) -> Result<Option<Arc<[u8]>>, String> {
        {
            let mut state = self.lock()?;
            if let Some((_, bytes)) = state.unstored.get(digest) {
                return Ok(Some(Arc::clone(bytes)));
            }
            if let Some(bytes) = state.held.get(digest) {
                return Ok(Some(bytes));
            }
        }
        // Read without the lock held, so other sessions' parts aren't kept waiting on a file.
        let Some(stored) = &self.stored else {
            return Ok(None);
        };
        let read = stored.read(digest)?;
        if let Some(bytes) = &read {
            self.counts.store_reads.fetch_add(1, Ordering::Relaxed);
            self.counts
                .store_read_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            let mut state = self.lock()?;
            if state.held.take_eviction(digest) {
                self.counts
                    .reads_after_eviction
                    .fetch_add(1, Ordering::Relaxed);
            }
            state.held.hold(*digest, Arc::clone(bytes));
        }
        Ok(read)
    }
}

impl Parts for DeploymentParts {
    fn put(&mut self, kind: PartKind, value: &Value) -> Result<PartRef, String> {
        let (bytes, digest) = canonical(value)?;
        let part = PartRef { kind, digest };
        if self.bytes(&digest)?.is_none() {
            self.lock()?.unstored.insert(digest, (kind, bytes.into()));
        }
        Ok(part)
    }

    fn get(&self, part: &PartRef) -> Result<Value, String> {
        let bytes = self
            .bytes(&part.digest)?
            .ok_or_else(|| "setup part missing".to_string())?;
        self.counts.decodes.fetch_add(1, Ordering::Relaxed);
        self.counts
            .decoded_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        decode_bytes(&bytes)
    }

    fn unstored(&self) -> Vec<(PartRef, Vec<u8>)> {
        let Ok(state) = self.lock() else {
            return Vec::new();
        };
        state
            .unstored
            .iter()
            .map(|(digest, (kind, bytes))| {
                (
                    PartRef {
                        kind: *kind,
                        digest: *digest,
                    },
                    bytes.to_vec(),
                )
            })
            .collect()
    }

    fn mark_stored(&mut self, parts: &[PartRef]) {
        if let Ok(mut state) = self.lock() {
            for part in parts {
                if let Some((_, bytes)) = state.unstored.remove(&part.digest) {
                    state.held.hold(part.digest, bytes);
                }
            }
        }
    }

    fn decoded(&self) -> Option<DecodedParts> {
        Some(self.decoded.clone())
    }

    fn usage(&self) -> Option<PartsUsage> {
        let state = self.lock().ok()?;
        let count = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Some(PartsUsage {
            held_parts: state.held.parts.len() as u64,
            held_bytes: state.held.bytes as u64,
            unstored_bytes: state
                .unstored
                .values()
                .map(|(_, bytes)| bytes.len() as u64)
                .sum(),
            store_reads: count(&self.counts.store_reads),
            store_read_bytes: count(&self.counts.store_read_bytes),
            reads_after_eviction: count(&self.counts.reads_after_eviction),
            decodes: count(&self.counts.decodes),
            decoded_bytes: count(&self.counts.decoded_bytes),
        })
    }
}

#[cfg(test)]
mod tests;
