//! Maintenance-only reconciliation. The WAL is authoritative, but live writers
//! retain their entire manifest until their session lease is released.

use super::*;
use crate::lease::{lease_key_for, FileLeaseStore, LeaseError, LeaseStore};
use crate::wal_writer::{session_addr, FileWalStore};
use broca_wal::{migration, FramedRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const CLASSES: [MediaStoreClass; 3] = [
    MediaStoreClass::Durable,
    MediaStoreClass::NonDurable,
    MediaStoreClass::Ephemeral,
];

#[derive(Debug, Default)]
pub struct SweepReport {
    pub leased: usize,
    pub reconciled: usize,
    pub manifests_removed: usize,
    pub objects_removed: usize,
    pub wal_reads: usize,
    pub retained_classes: usize,
}

/// Run outside admission: startup neither waits for maintenance nor queues on a
/// busy store lock. A deadline also bounds cooperative work on a large store.
pub fn spawn_startup_sweep(state_root: PathBuf) -> io::Result<std::thread::JoinHandle<()>> {
    spawn_sweep(move || {
        if let Err(error) = sweep_until(
            &state_root,
            Some(Instant::now() + Duration::from_secs(1)),
            true,
        ) {
            tracing::warn!(target: "media", "startup media sweep deferred: {error}");
        }
    })
}

fn spawn_sweep(action: impl FnOnce() + Send + 'static) -> io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("media-sweep".into())
        .spawn(action)
}

/// Call only from store-maintenance (or the detached startup task), never from
/// the run loop. Busy stores are retried by a later maintenance invocation.
pub fn sweep(state_root: &Path) -> io::Result<SweepReport> {
    sweep_until(state_root, None, true)
}

/// Compute the same collection plan without creating files, bumping lease epochs
/// or changing manifests and objects. Locks protect the observed snapshot only.
pub fn preview_sweep(state_root: &Path) -> io::Result<SweepReport> {
    sweep_until(state_root, None, false)
}

fn budget(deadline: Option<Instant>) -> io::Result<()> {
    if deadline.is_some_and(|end| Instant::now() >= end) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "media sweep budget exhausted",
        ));
    }
    Ok(())
}

fn sweep_until(
    state_root: &Path,
    deadline: Option<Instant>,
    write: bool,
) -> io::Result<SweepReport> {
    budget(deadline)?;
    let mut stores = Vec::new();
    for class in CLASSES {
        let root = state_root.join(class.directory());
        if !has_objects(&root.join("objects"))? {
            continue;
        }
        stores.push((MediaStore { root }, class));
    }
    let mut report = SweepReport::default();
    if stores.is_empty() {
        return Ok(report);
    }
    // Archive publication and live-file removal must not straddle our inventory.
    // Both this and session leases are nonblocking, avoiding lock-order cycles.
    let _archive = if write {
        Some(crate::lease::acquire_archive_pass(state_root).map_err(io::Error::other)?)
    } else {
        None
    };
    let _archive_preview = if write {
        None
    } else {
        let path = crate::lease::archive_pass_lock_path(state_root);
        let lock = existing_lock(&path)?;
        if let Some(file) = &lock {
            file.try_lock_shared().map_err(io::Error::other)?;
        }
        lock
    };
    let wal = FileWalStore::new(state_root.join("wal"));
    let leases = FileLeaseStore::new(state_root.join("leases"));
    let mut locks = Vec::new();
    // Hold every participating store lock until the shared inventory and all
    // collection decisions finish. Registrations cannot change its candidate set.
    for (store, _) in &stores {
        budget(deadline)?;
        let lock = if write {
            Some(store.lock()?)
        } else {
            existing_lock(&store.root.join("store.lock"))?
        };
        if let Some(lock) = lock {
            lock.try_lock().map_err(io::Error::other)?;
            locks.push(lock);
        }
    }
    let mut candidates = BTreeSet::new();
    let mut readable = Vec::new();
    for (store, class) in stores {
        match manifest_candidates(&store, deadline) {
            Ok(addresses) => {
                candidates.extend(addresses);
                readable.push((store, class));
            }
            Err(error) if error.kind() == io::ErrorKind::TimedOut => return Err(error),
            Err(_) => report.retained_classes += 1,
        }
    }
    let inventory = inventory(&wal, &candidates, deadline, &mut report)?;
    for (store, class) in readable {
        reconcile(
            &store,
            class,
            &inventory,
            &leases,
            deadline,
            write,
            &mut report,
        )?;
    }
    Ok(report)
}

fn existing_lock(path: &Path) -> io::Result<Option<File>> {
    match fs::OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn has_objects(directory: &Path) -> io::Result<bool> {
    match fs::read_dir(directory) {
        Ok(mut entries) => entries.next().transpose().map(|entry| entry.is_some()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn manifest_candidates(
    store: &MediaStore,
    deadline: Option<Instant>,
) -> io::Result<BTreeSet<String>> {
    let mut candidates = BTreeSet::new();
    for directory in ["refs", "identities"] {
        let path = store.root.join(directory);
        if directory == "identities" && !path.try_exists()? {
            continue;
        }
        for entry in fs::read_dir(path)? {
            budget(deadline)?;
            let path = entry?.path();
            let address = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| invalid("invalid media manifest address"))?;
            if directory == "identities" && address.ends_with(".json.tmp") {
                continue;
            }
            if address.len() != 16 || !address.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(invalid("invalid media manifest address"));
            }
            if directory == "refs" {
                let bytes = fs::read(&path)?;
                if !bytes.is_empty() && !bytes.ends_with(b"\n") {
                    return Err(invalid("incomplete media manifest digest"));
                }
                digests(&bytes)?;
            } else {
                let session: BindIdentity =
                    serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
                if session_addr(&session) != address {
                    return Err(invalid("media manifest identity mismatch"));
                }
            }
            candidates.insert(address.to_owned());
        }
    }
    Ok(candidates)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn read_optional(path: &Path) -> io::Result<Vec<u8>> {
    match fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

fn digests(bytes: &[u8]) -> io::Result<BTreeSet<String>> {
    let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
    let mut result = BTreeSet::new();
    // Ignore only an incomplete final append, as the registration recovery does.
    for line in text
        .split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
    {
        let digest = line.trim_end_matches('\n');
        if !valid_digest(digest) {
            return Err(invalid("invalid media manifest digest"));
        }
        result.insert(digest.to_owned());
    }
    Ok(result)
}

/// Read without creating a store, manifest or object. Move preflight must leave
/// no-media sessions byte-identical and refuse before publishing a cut intent.
pub fn session_media_digests(
    state_root: &Path,
    identity: &BindIdentity,
) -> io::Result<BTreeSet<String>> {
    let mut result = BTreeSet::new();
    for class in CLASSES {
        let bytes = read_optional(
            &state_root
                .join(class.directory())
                .join("refs")
                .join(session_addr(identity)),
        )?;
        result.extend(digests(&bytes)?);
    }
    Ok(result)
}

/// Include opaque compaction views and hook payloads as well as typed input and
/// tool results. Conservative extra matches in opaque JSON can only retain data,
/// never authorize a render or cause a referenced object to be deleted.
fn wal_digests(
    records: &[FramedRecord],
    deadline: Option<Instant>,
) -> io::Result<BTreeSet<String>> {
    fn visit(value: &serde_json::Value, out: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::Object(map) => {
                if map.get("type").and_then(|v| v.as_str()) == Some("media") {
                    if let Some(digest) = map.get("sha256").and_then(|v| v.as_str()) {
                        out.insert(digest.to_owned());
                    }
                }
                if map.get("kind").and_then(|v| v.as_str()) == Some("media") {
                    if let Some(digest) = value.pointer("/media/sha256").and_then(|v| v.as_str()) {
                        out.insert(digest.to_owned());
                    }
                }
                for value in map.values() {
                    visit(value, out);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(value, out);
                }
            }
            _ => {}
        }
    }
    let mut result = BTreeSet::new();
    for frame in records {
        budget(deadline)?;
        visit(
            &serde_json::to_value(&frame.record).map_err(io::Error::other)?,
            &mut result,
        );
    }
    Ok(result)
}

fn identity(records: &[FramedRecord], address: &str) -> Option<BindIdentity> {
    records.iter().find_map(|frame| match &frame.record {
        broca_wal::WalRecord::RunStarted { session, .. } if session_addr(session) == address => {
            Some(session.clone())
        }
        _ => None,
    })
}

type Inventory = BTreeMap<
    String,
    (
        BTreeSet<String>,
        Option<BindIdentity>,
        Option<MediaStoreClass>,
    ),
>;

fn inventory(
    wal: &FileWalStore,
    candidates: &BTreeSet<String>,
    deadline: Option<Instant>,
    report: &mut SweepReport,
) -> io::Result<Inventory> {
    let mut result = BTreeMap::new();
    if candidates.is_empty() {
        return Ok(result);
    }
    for address in candidates {
        budget(deadline)?;
        let class = wal.media_store_class(address).map_err(io::Error::other)?;
        if class.is_none() {
            continue;
        }
        let records = wal
            .read_address_records(address)
            .map_err(io::Error::other)?;
        report.wal_reads += 1;
        result.insert(
            address.clone(),
            (
                wal_digests(&records, deadline)?,
                identity(&records, address),
                class,
            ),
        );
    }
    // An interrupted retention deletion still owns its objects. Its WAL remains
    // authoritative until recovery removes the .deleting file and syncs the tree.
    for root in [
        wal.root().to_path_buf(),
        wal.durable_root(),
        wal.ephemeral_root(),
    ] {
        if !root.try_exists()? {
            continue;
        }
        for entry in fs::read_dir(root)? {
            budget(deadline)?;
            let path = entry?.path();
            if path.extension().is_none_or(|ext| ext != "deleting") {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| invalid("invalid deleting WAL name"))?;
            let address = name.split('.').next().unwrap();
            if !candidates.contains(address) {
                continue;
            }
            let records =
                broca_wal::replay_file(&path, wal.max_read_bytes()).map_err(io::Error::other)?;
            report.wal_reads += 1;
            let item = result.entry(address.to_owned()).or_insert_with(|| {
                (
                    BTreeSet::new(),
                    identity(&records, address),
                    Some(MediaStoreClass::Ephemeral),
                )
            });
            item.0.extend(wal_digests(&records, deadline)?);
        }
    }
    Ok(result)
}

fn sync_wal_directories(state_root: &Path) -> io::Result<()> {
    sync_wal_directories_with(state_root, &mut RealSteps)
}

fn sync_wal_directories_with(state_root: &Path, steps: &mut impl StorageSteps) -> io::Result<()> {
    for tree in [
        "wal",
        "wal-durable",
        "wal-ephemeral",
        "wal-archive",
        "wal-archive-durable",
    ] {
        let path = state_root.join(tree);
        if path.try_exists()? {
            steps.dir_sync(&path, Step::WalDirectorySynced)?;
        }
    }
    Ok(())
}

fn replace_manifest(store: &MediaStore, path: &Path, bytes: &[u8]) -> io::Result<()> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let temp = store.root.join(format!(
        ".manifest-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = broca_owner_only::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, path)?;
        migration::sync_dir(path.parent().unwrap())?;
        migration::sync_dir(&store.root)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

fn reconcile(
    store: &MediaStore,
    class: MediaStoreClass,
    inventory: &Inventory,
    leases: &FileLeaseStore,
    deadline: Option<Instant>,
    write: bool,
    report: &mut SweepReport,
) -> io::Result<()> {
    // Registration durably names the session before its WAL append. The shared
    // candidate inventory also covers identities whose manifest was lost; no
    // unrelated session WAL needs to be decoded to protect its objects.
    let mut retained: BTreeSet<_> = inventory
        .values()
        .filter(|(_, _, owner)| *owner == Some(class))
        .flat_map(|(digests, _, _)| digests.iter().cloned())
        .collect();
    let refs = store.root.join("refs");
    for entry in fs::read_dir(&refs)? {
        budget(deadline)?;
        let path = entry?.path();
        let address = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("invalid media manifest address"))?;
        if address.len() != 16 || !address.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid(format!(
                "unrecognized media manifest {}",
                path.display()
            )));
        }
        let bytes = fs::read(&path)?;
        let manifest = digests(&bytes)?;
        let captured = inventory.get(address);
        let sidecar = store.root.join("identities").join(address);
        let session: Option<BindIdentity> = migration::read_json(&sidecar)?;
        let session = session.or_else(|| captured.and_then(|(_, session, _)| session.clone()));
        let Some(session) = session else {
            // Older digest-only manifests cannot prove which lease to check if
            // their WAL is absent. Retain them rather than guess a writer's identity.
            retained.extend(manifest);
            continue;
        };
        if session_addr(&session) != address {
            return Err(invalid("media manifest identity mismatch"));
        }
        let lease = session_lease(store.root.parent().unwrap(), leases, &session, write)?;
        if lease.is_none() {
            report.leased += 1;
            retained.extend(manifest);
            continue;
        }
        let cache = store.root.join("swept").join(address);
        let Some((named, _, _)) = captured else {
            if !write {
                report.manifests_removed += 1;
                continue;
            }
            // Absence is not durability: repay even a previous process's failed
            // unlink barrier before dropping the last manifest reference.
            sync_wal_directories(store.root.parent().unwrap())?;
            migration::unlink(&path)?;
            migration::unlink(&sidecar)?;
            migration::unlink(&cache)?;
            report.manifests_removed += 1;
            continue;
        };
        let canonical: String = named.iter().map(|digest| format!("{digest}\n")).collect();
        let stamp = format!("{:x}", Sha256::digest(canonical.as_bytes()));
        if bytes != canonical.as_bytes()
            || migration::read_json::<String>(&cache)?.as_deref() != Some(&stamp)
        {
            if write && bytes != canonical.as_bytes() {
                replace_manifest(store, &path, canonical.as_bytes())?;
            }
            // Cache only completed, unleased reconciliations. A refused steer
            // must be revisited on the first sweep after the lease is released.
            if write {
                migration::atomic_json(store.root.parent().unwrap(), &cache, &stamp)?;
            }
            report.reconciled += 1;
        }
        retained.extend(named.iter().cloned());
    }
    // All manifests and WALs were readable before deleting any object. An error
    // above aborts collection rather than converting unreadable references to zero.
    for entry in fs::read_dir(store.root.join("objects"))? {
        budget(deadline)?;
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("invalid media object name"))?;
        if !retained.contains(name) {
            if write {
                migration::unlink(&path)?;
            }
            report.objects_removed += 1;
        }
    }
    Ok(())
}

struct SessionLease {
    _writer: Option<Box<dyn crate::lease::LeaseHandle>>,
    _preview: Option<File>,
}

fn session_lease(
    state_root: &Path,
    leases: &FileLeaseStore,
    session: &BindIdentity,
    write: bool,
) -> io::Result<Option<SessionLease>> {
    if write {
        match leases.acquire(&lease_key_for(session)) {
            Ok(lease) => Ok(Some(SessionLease {
                _writer: Some(lease),
                _preview: None,
            })),
            Err(LeaseError::Held { .. }) => Ok(None),
            Err(error) => Err(io::Error::other(error)),
        }
    } else {
        match crate::lease::probe_writer(state_root, session)? {
            crate::lease::LeaseProbe::Held => Ok(None),
            crate::lease::LeaseProbe::Free(file) => Ok(Some(SessionLease {
                _writer: None,
                _preview: file,
            })),
        }
    }
}

impl MediaStore {
    /// The caller holds the session lease. Repeat WAL directory barriers even
    /// when recovery finds the file absent; a previous unlink may not be durable.
    /// Archive folds deliberately never call this: their WAL still exists.
    pub fn remove_session_after_wal_removal(&self, session: &BindIdentity) -> io::Result<()> {
        self.remove_session_with(session, &mut RealSteps)
    }

    fn remove_session_with(
        &self,
        session: &BindIdentity,
        steps: &mut impl StorageSteps,
    ) -> io::Result<()> {
        let lock = self.lock()?;
        lock.lock_shared()?;
        let state_root = self.root.parent().unwrap();
        let address = session_addr(session);
        // No media, no scan: when there is nothing to unlink, skip both the
        // WAL existence checks and the WAL directory barrier. The barrier only
        // orders the WAL unlink before dropping media entries. It is repeated
        // even when the WAL file is already absent because a previous unlink
        // may not yet be durable. With no refs, identities, or swept entry
        // there is nothing to drop, so this call can skip the checks and the
        // barrier.
        if !self.root.join("refs").join(&address).try_exists()?
            && !self.root.join("identities").join(&address).try_exists()?
            && !self.root.join("swept").join(&address).try_exists()?
        {
            return Ok(());
        }
        let wal = FileWalStore::new(state_root.join("wal"));
        // A `.deleting` file is only ever created in the ephemeral tree as
        // `{address}.{lineage}.deleting` (`FileWalStore::deleting_path` and
        // retention's `deleting`), and its tombstone is written first. The
        // caller holds the session lease, and `delete_expired` finishes every
        // incomplete tombstone for this address before starting a new deletion,
        // so no other lineage's `.deleting` can appear meanwhile. Checking the
        // exact path for each tombstone-known lineage is therefore enough. Never
        // list a whole WAL tree here: a deletion's cost must not grow with the
        // number of sessions on disk.
        if has_deleting_for_address(&wal, &address)?
            || wal
                .media_store_class(&address)
                .map_err(io::Error::other)?
                .is_some()
        {
            return Err(invalid(format!(
                "cannot drop media manifest while WAL {address} exists"
            )));
        }
        sync_wal_directories_with(state_root, steps)?;
        for directory in ["refs", "identities", "swept"] {
            migration::unlink(&self.root.join(directory).join(&address))?;
        }
        Ok(())
    }
}

/// Exact `.deleting` checks for the tombstone-known lineages of one address.
/// A `.deleting` file implies its tombstone was written first (the tombstone
/// precedes the rename in `RetentionService::complete`), so enumerating the
/// tiny per-address `expired` directory finds every production candidate
/// without listing any WAL tree. A tombstoneless `.deleting` cannot occur in
/// production; the once-per-sweep WAL scan in `inventory` still protects that
/// case for collection.
fn has_deleting_for_address(wal: &FileWalStore, address: &str) -> io::Result<bool> {
    let expired = wal.ephemeral_root().join(format!("{address}.expired"));
    let entries = match fs::read_dir(&expired) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let name = entry?.file_name();
        let lineage = name.to_string_lossy();
        if lineage.ends_with(".tmp") {
            continue;
        }
        if wal
            .ephemeral_root()
            .join(format!("{address}.{lineage}.deleting"))
            .try_exists()?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests;
