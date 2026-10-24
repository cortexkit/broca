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
}

/// Run outside admission: startup neither waits for maintenance nor queues on a
/// busy store lock. A deadline also bounds cooperative work on a large store.
pub fn spawn_startup_sweep(state_root: PathBuf) -> io::Result<std::thread::JoinHandle<()>> {
    spawn_sweep(move || {
        if let Err(error) = sweep_until(&state_root, Some(Instant::now() + Duration::from_secs(1)))
        {
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
    sweep_until(state_root, None)
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

fn sweep_until(state_root: &Path, deadline: Option<Instant>) -> io::Result<SweepReport> {
    budget(deadline)?;
    // Archive publication and live-file removal must not straddle our inventory.
    // Both this and session leases are nonblocking, avoiding lock-order cycles.
    let _archive = crate::lease::acquire_archive_pass(state_root).map_err(io::Error::other)?;
    let wal = FileWalStore::new(state_root.join("wal"));
    let leases = FileLeaseStore::new(state_root.join("leases"));
    let mut report = SweepReport::default();
    for class in CLASSES {
        budget(deadline)?;
        if !state_root.join(class.directory()).try_exists()? {
            continue;
        }
        let store = MediaStore::open(state_root, class)?;
        let lock = store.lock()?;
        lock.try_lock().map_err(io::Error::other)?;
        reconcile(&store, class, &wal, &leases, deadline, &mut report)?;
    }
    Ok(report)
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

fn inventory(wal: &FileWalStore, deadline: Option<Instant>) -> io::Result<Inventory> {
    let mut result = BTreeMap::new();
    let addresses = wal
        .media_addresses(&|| deadline.is_some_and(|end| Instant::now() >= end))
        .map_err(io::Error::other)?;
    for address in addresses {
        budget(deadline)?;
        let records = wal
            .read_address_records(&address)
            .map_err(io::Error::other)?;
        result.insert(
            address.clone(),
            (
                wal_digests(&records, deadline)?,
                identity(&records, &address),
                wal.media_store_class(&address).map_err(io::Error::other)?,
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
            let records =
                broca_wal::replay_file(&path, wal.max_read_bytes()).map_err(io::Error::other)?;
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
    wal: &FileWalStore,
    leases: &FileLeaseStore,
    deadline: Option<Instant>,
    report: &mut SweepReport,
) -> io::Result<()> {
    let inventory = inventory(wal, deadline)?;
    // Even a damaged/missing manifest must not make a WAL-owned object garbage.
    // Keep the WAL union independently of manifest reconciliation.
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
        let lease = match leases.acquire(&lease_key_for(&session)) {
            Ok(lease) => Some(lease),
            Err(LeaseError::Held { .. }) => None,
            Err(error) => return Err(io::Error::other(error)),
        };
        if lease.is_none() {
            report.leased += 1;
            retained.extend(manifest);
            continue;
        }
        let cache = store.root.join("swept").join(address);
        let Some((named, _, _)) = captured else {
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
            if bytes != canonical.as_bytes() {
                replace_manifest(store, &path, canonical.as_bytes())?;
            }
            // Cache only completed, unleased reconciliations. A refused steer
            // must be revisited on the first sweep after the lease is released.
            migration::atomic_json(store.root.parent().unwrap(), &cache, &stamp)?;
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
            migration::unlink(&path)?;
            report.objects_removed += 1;
        }
    }
    Ok(())
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
        let wal = FileWalStore::new(state_root.join("wal"));
        let address = session_addr(session);
        let mut deleting = false;
        for tree in [
            wal.root().to_path_buf(),
            wal.ephemeral_root(),
            wal.durable_root(),
        ] {
            if !tree.try_exists()? {
                continue;
            }
            for entry in fs::read_dir(tree)? {
                let path = entry?.path();
                deleting |= path.extension().is_some_and(|ext| ext == "deleting")
                    && path.file_name().is_some_and(|name| {
                        name.to_string_lossy().starts_with(&format!("{address}."))
                    });
            }
        }
        if deleting
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

#[cfg(test)]
mod tests;
