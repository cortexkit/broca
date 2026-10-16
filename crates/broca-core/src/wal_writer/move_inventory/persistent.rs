//! Restart hints: validate every current location before reusing its identities.
use super::*;
use broca_wal::{archive::ArchiveReader, file_wal, framing, WalRangeRecords};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom, Write};

mod pacing;
#[cfg(test)]
mod tests;

const CACHE_VERSION: u32 = 3;
const CACHE_LIMIT: u64 = 128 * 1024 * 1024;
const CHECKPOINT_ADDRESSES: usize = 1024;
const CHECKPOINT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Default)]
pub struct MoveInventoryBuildStats {
    pub payload_reads: u64,
    pub payload_bytes: u64,
    pub lineage_bytes: u64,
    pub cache_bytes: u64,
    pub archive_metadata_bytes: u64,
    pub reused: u64,
    pub suffixes: u64,
    pub elapsed_us: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileStamp {
    len: u64,
    file_id: Option<(u64, u64)>,
    modified_ns: Option<u64>,
}

impl FileStamp {
    fn at(path: &Path) -> Result<Self, WalError> {
        let metadata = std::fs::metadata(path).map_err(WalError::Io)?;
        if !metadata.is_file() {
            return Err(invalid("inventory location is not a regular file"));
        }
        Ok(Self {
            len: metadata.len(),
            file_id: persistent_file_identity(&metadata),
            modified_ns: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|elapsed| elapsed.as_nanos().try_into().ok()),
        })
    }
}

#[cfg(unix)]
fn persistent_file_identity(metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    // Persist raw identifiers, not a standard-library hash whose algorithm can
    // change between compiler releases and invalidate every restart hint.
    Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn persistent_file_identity(_: &std::fs::Metadata) -> Option<(u64, u64)> {
    None
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Source {
    Live {
        path: PathBuf,
        stamp: FileStamp,
        lineage_id: Option<String>,
        sealed_tail: bool,
    },
    Archive {
        path: PathBuf,
        stamp: FileStamp,
        offset: u64,
        payload_len: u64,
        digest: Option<[u8; 32]>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CachedAddress {
    source: Source,
    committed_end: u64,
    last_seq: u64,
    triples: Vec<BindIdentity>,
    used_features: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cache {
    version: u32,
    reader_features: Vec<String>,
    checksum: String,
    addresses: BTreeMap<String, CachedAddress>,
}

fn invalid(message: &str) -> WalError {
    WalError::Io(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn changed(message: &str) -> WalError {
    WalError::Io(io::Error::new(io::ErrorKind::WouldBlock, message))
}

fn cache_path(store: &FileWalStore) -> PathBuf {
    store.root.with_file_name("move-inventory.json")
}

fn load(
    store: &FileWalStore,
    stats: &mut MoveInventoryBuildStats,
) -> BTreeMap<String, CachedAddress> {
    let result = (|| -> Result<Cache, WalError> {
        let path = cache_path(store);
        let metadata = std::fs::symlink_metadata(&path).map_err(WalError::Io)?;
        if !metadata.is_file() || metadata.len() > CACHE_LIMIT {
            return Err(invalid(
                "move inventory cache is not a bounded regular file",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(invalid("move inventory cache is not owner-only"));
            }
        }
        let mut bytes = Vec::new();
        broca_owner_only::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(WalError::Io)?
            .take(CACHE_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(WalError::Io)?;
        stats.cache_bytes += bytes.len() as u64;
        let cache: Cache = serde_json::from_slice(&bytes)
            .map_err(|error| invalid(&format!("invalid move inventory cache: {error}")))?;
        if cache.version != CACHE_VERSION || cache.checksum != checksum(&cache.addresses)? {
            return Err(invalid(
                "move inventory cache version or checksum does not match",
            ));
        }
        for (address, entry) in &cache.addresses {
            if address.len() != 16
                || !address.bytes().all(|b| b.is_ascii_hexdigit())
                || entry
                    .triples
                    .iter()
                    .any(|session| session_addr(session) != *address)
            {
                return Err(invalid(
                    "move inventory cache has an invalid address or triple",
                ));
            }
        }
        Ok(cache)
    })();
    match result {
        Ok(cache) => {
            let known = reader_features();
            cache
                .addresses
                .into_iter()
                .filter(|(_, entry)| {
                    entry
                        .used_features
                        .iter()
                        .all(|feature| known.contains(feature))
                })
                .collect()
        }
        Err(error) => {
            tracing::warn!(%error, "move inventory cache unavailable; rebuilding from WALs");
            BTreeMap::new()
        }
    }
}

fn checksum(addresses: &BTreeMap<String, CachedAddress>) -> Result<String, WalError> {
    let value = serde_json::to_value(addresses).map_err(|error| invalid(&error.to_string()))?;
    let bytes = serde_json::to_vec(&value).map_err(|error| invalid(&error.to_string()))?;
    Ok(broca_wal::migration::digest(&bytes))
}

fn reader_features() -> Vec<String> {
    // Only entries using an unknown feature need decoding after a downgrade.
    // New reader capabilities do not invalidate unrelated restart hints.
    let mut features = broca_wal::KNOWN_FEATURES
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    features.sort();
    features
}

fn save(store: &FileWalStore, addresses: BTreeMap<String, CachedAddress>) -> Result<(), WalError> {
    let path = cache_path(store);
    let parent = path
        .parent()
        .ok_or_else(|| invalid("move inventory cache has no parent"))?;
    let cache = Cache {
        version: CACHE_VERSION,
        reader_features: reader_features(),
        checksum: checksum(&addresses)?,
        addresses,
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| invalid(&error.to_string()))?
        .as_nanos();
    let temp = path.with_extension(format!("tmp-{}-{nonce}", std::process::id()));
    let result = (|| -> Result<(), WalError> {
        let mut file = broca_owner_only::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(WalError::Io)?;
        serde_json::to_writer(&mut file, &cache).map_err(|error| invalid(&error.to_string()))?;
        file.flush().map_err(WalError::Io)?;
        file.sync_all().map_err(WalError::Io)?;
        drop(file);
        std::fs::rename(&temp, &path).map_err(WalError::Io)?;
        broca_wal::migration::sync_dir(parent).map_err(WalError::Io)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

fn live_source(path: PathBuf, stats: &mut MoveInventoryBuildStats) -> Result<Source, WalError> {
    let stamp = FileStamp::at(&path)?;
    // Modern WALs begin with lineage metadata and every append also ends with
    // it. The tail probe identifies legacy WALs that gained lineage later, too.
    let mut file = broca_owner_only::OpenOptions::new()
        .read(true)
        .open(&path)
        .map_err(WalError::Io)?;
    let mut prefix = [0; framing::HEADER_LEN + 16];
    let mut read = 0;
    while read < prefix.len() {
        let n = file.read(&mut prefix[read..]).map_err(WalError::Io)?;
        if n == 0 {
            break;
        }
        read += n;
    }
    stats.lineage_bytes += read as u64;
    let head_id = match framing::decode_next(&prefix[..read]) {
        framing::DecodeStep::Lineage { lineage_id, .. } => Some(lineage_id.to_string()),
        _ => None,
    };
    let mut tail_id = None;
    if stamp.len >= prefix.len() as u64 {
        file.seek(SeekFrom::Start(stamp.len - prefix.len() as u64))
            .map_err(WalError::Io)?;
        file.read_exact(&mut prefix).map_err(WalError::Io)?;
        stats.lineage_bytes += prefix.len() as u64;
        if let framing::DecodeStep::Lineage { lineage_id, .. } = framing::decode_next(&prefix) {
            tail_id = Some(lineage_id.to_string());
        }
    }
    if head_id.is_some() && tail_id.is_some() && head_id != tail_id {
        return Err(invalid("inventory WAL head and tail lineage disagree"));
    }
    let sealed_tail = tail_id.is_some();
    let lineage_id = head_id.or(tail_id);
    Ok(Source::Live {
        path,
        stamp,
        lineage_id,
        sealed_tail,
    })
}

fn cancelled(cancel: &CancellationToken) -> Result<(), WalError> {
    if cancel.is_cancelled() {
        Err(WalError::Io(io::Error::new(
            io::ErrorKind::Interrupted,
            "move inventory cancelled",
        )))
    } else {
        Ok(())
    }
}

struct CountedMetadata<'a> {
    file: std::fs::File,
    bytes: &'a mut u64,
}
impl Read for CountedMetadata<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(bytes)?;
        *self.bytes += n as u64;
        Ok(n)
    }
}
impl Seek for CountedMetadata<'_> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file.seek(position)
    }
}

fn locations(
    store: &FileWalStore,
    cancel: &CancellationToken,
    unreadable: &mut Vec<String>,
    stats: &mut MoveInventoryBuildStats,
) -> Result<BTreeMap<String, Source>, WalError> {
    locations_after_live(store, cancel, unreadable, stats, || {})
}

fn locations_after_live(
    store: &FileWalStore,
    cancel: &CancellationToken,
    unreadable: &mut Vec<String>,
    stats: &mut MoveInventoryBuildStats,
    after_live: impl FnOnce(),
) -> Result<BTreeMap<String, Source>, WalError> {
    // Folds copy into a sealed archive before deleting live files. Listing live
    // first ensures a fold between listings is seen in at least one location.
    let (live, ambiguous) = live_locations(store, cancel, unreadable)?;
    after_live();
    let mut sources = archive_locations(store, cancel, unreadable, stats)?;
    for (address, path) in live {
        cancelled(cancel)?;
        // Live always overrides archived history at the same address.
        sources.insert(
            address,
            Source::Live {
                path,
                stamp: FileStamp {
                    len: 0,
                    file_id: None,
                    modified_ns: None,
                },
                lineage_id: None,
                sealed_tail: false,
            },
        );
    }
    for address in ambiguous {
        sources.remove(&address);
        unreadable.push(address);
    }
    Ok(sources)
}

type LiveLocations = (
    BTreeMap<String, PathBuf>,
    std::collections::BTreeSet<String>,
);

fn live_locations(
    store: &FileWalStore,
    cancel: &CancellationToken,
    unreadable: &mut Vec<String>,
) -> Result<LiveLocations, WalError> {
    let mut live = BTreeMap::new();
    let mut ambiguous = std::collections::BTreeSet::new();
    for root in [&store.root, &store.ephemeral_root()] {
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => {
                unreadable.push(root.display().to_string());
                continue;
            }
        };
        for entry in entries {
            cancelled(cancel)?;
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(_) => {
                    unreadable.push(root.display().to_string());
                    continue;
                }
            };
            if path.extension().is_some_and(|ext| ext == "wal") {
                if let Some(address) = path.file_stem().and_then(|stem| stem.to_str()) {
                    if live.insert(address.to_owned(), path.clone()).is_some() {
                        ambiguous.insert(address.to_owned());
                    }
                }
            }
        }
    }
    Ok((live, ambiguous))
}

fn archive_locations(
    store: &FileWalStore,
    cancel: &CancellationToken,
    unreadable: &mut Vec<String>,
    stats: &mut MoveInventoryBuildStats,
) -> Result<BTreeMap<String, Source>, WalError> {
    let mut sources = BTreeMap::new();
    let mut paths = store.archive_containers()?.unwrap_or_default();
    paths.sort_by_key(|path| broca_wal::archive::fold_timestamp(path).unwrap_or(0));
    for path in paths {
        cancelled(cancel)?;
        let entries = (|| -> Result<Vec<(String, Source)>, WalError> {
            let stamp = FileStamp::at(&path)?;
            let input = CountedMetadata {
                file: broca_owner_only::OpenOptions::new()
                    .read(true)
                    .open(&path)
                    .map_err(WalError::Io)?,
                bytes: &mut stats.archive_metadata_bytes,
            };
            let mut reader = ArchiveReader::new(input, store.max_read_bytes);
            if let Ok(stamp) = broca_wal::archive::fold_timestamp(&path) {
                reader = reader.with_stamp(stamp);
            }
            if reader.state().map_err(WalError::Io)? == broca_wal::archive::ContainerState::Unsealed
            {
                return Err(invalid("inventory archive is not sealed yet"));
            }
            // Verified v2 indexes prove member coverage without touching payloads.
            // V1 containers need header walks, which also skip every payload.
            let entries = match reader.verified_members().map_err(WalError::Io)? {
                Some(entries) => entries
                    .into_iter()
                    .map(|info| {
                        (
                            info.session_addr,
                            Source::Archive {
                                path: path.clone(),
                                stamp: stamp.clone(),
                                offset: info.offset,
                                payload_len: info.payload_len,
                                digest: None,
                            },
                        )
                    })
                    .collect(),
                None => reader
                    .rescan()
                    .map_err(WalError::Io)?
                    .into_iter()
                    .map(|info| {
                        (
                            info.session_addr,
                            Source::Archive {
                                path: path.clone(),
                                stamp: stamp.clone(),
                                offset: info.offset,
                                payload_len: info.payload_len,
                                digest: Some(info.payload_sha256),
                            },
                        )
                    })
                    .collect(),
            };
            if stamp != FileStamp::at(&path)? {
                return Err(changed("archive changed during inventory"));
            }
            Ok(entries)
        })();
        match entries {
            Ok(entries) => {
                for (address, source) in entries {
                    cancelled(cancel)?;
                    sources.insert(address, source);
                }
            }
            Err(error) => {
                tracing::error!(path = %path.display(), %error, "move inventory cannot inspect archive index");
                unreadable.push(path.display().to_string());
            }
        }
    }
    Ok(sources)
}

fn scan_address(
    store: &FileWalStore,
    cancel: &CancellationToken,
    address: &str,
    source: Source,
    old: Option<&CachedAddress>,
    stats: &mut MoveInventoryBuildStats,
) -> Result<Option<CachedAddress>, WalError> {
    let mut source = source;
    for attempt in 0..3 {
        cancelled(cancel)?;
        let result = scan_address_once(store, cancel, address, source, old, stats);
        if !matches!(&result, Err(WalError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock)
            || attempt == 2
        {
            return result;
        }
        let Some(current) = resolve_address(store, cancel, address, stats)? else {
            return Ok(None);
        };
        source = current;
    }
    unreachable!("bounded scan always returns on its final attempt")
}

fn resolve_address(
    store: &FileWalStore,
    cancel: &CancellationToken,
    address: &str,
    stats: &mut MoveInventoryBuildStats,
) -> Result<Option<Source>, WalError> {
    let mut unreadable = Vec::new();
    let mut current = locations(store, cancel, &mut unreadable, stats)?;
    if !unreadable.is_empty() {
        return Err(invalid(&format!(
            "cannot re-resolve inventory address: {}",
            unreadable.join(", ")
        )));
    }
    Ok(current.remove(address))
}

fn scan_address_once(
    store: &FileWalStore,
    cancel: &CancellationToken,
    address: &str,
    source: Source,
    old: Option<&CachedAddress>,
    stats: &mut MoveInventoryBuildStats,
) -> Result<Option<CachedAddress>, WalError> {
    match scan(store, address, source, old, stats) {
        Ok(entry) => Ok(Some(entry)),
        Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            // A fold or activation can remove the enumerated path. Re-resolve
            // live first, then the newest archive; only a complete census can
            // prove that an address now has no bytes anywhere.
            resolve_address(store, cancel, address, stats)?
                .map(|source| scan(store, address, source, old, stats))
                .transpose()
        }
        Err(error) => Err(error),
    }
}

fn add_records(
    triples: &mut Vec<BindIdentity>,
    address: &str,
    range: &broca_wal::WalRangeRead,
) -> Result<(), WalError> {
    let WalRangeRecords::Complete(records) = range.records() else {
        return Err(invalid("inventory WAL records are incomplete"));
    };
    for frame in records {
        if let WalRecord::RunStarted { session, .. } = &frame.record {
            if session_addr(session) == address && !triples.contains(session) {
                triples.push(session.clone());
            }
        }
    }
    Ok(())
}

fn same_live_lineage(old: &Source, current: &Source) -> bool {
    // Legacy logs have no lineage. Their stable file identity and unchanged
    // stamp still prove the same append-only prefix; without either identity
    // mechanism we must decode again instead of reusing an absence proof.
    matches!((old, current),
        (Source::Live { path: a, stamp: x, lineage_id: l, .. },
         Source::Live { path: b, stamp: y, lineage_id: r, .. })
        if a == b && (l.is_some() || x.file_id.is_some()) && x.file_id == y.file_id && l == r)
}

fn reusable(old: &CachedAddress, current: &Source) -> bool {
    match current {
        Source::Live { stamp, .. } => {
            same_live_lineage(&old.source, current)
                && old.source == *current
                && old.committed_end == stamp.len
        }
        Source::Archive { payload_len, .. } => {
            old.source == *current && old.committed_end == *payload_len
        }
    }
}

fn scan(
    store: &FileWalStore,
    address: &str,
    location: Source,
    old: Option<&CachedAddress>,
    stats: &mut MoveInventoryBuildStats,
) -> Result<CachedAddress, WalError> {
    scan_after_stamp(store, address, location, old, stats, || {})
}

fn scan_after_stamp(
    store: &FileWalStore,
    address: &str,
    location: Source,
    old: Option<&CachedAddress>,
    stats: &mut MoveInventoryBuildStats,
    after_stamp: impl FnOnce(),
) -> Result<CachedAddress, WalError> {
    if address.len() != 16 || !address.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(invalid("invalid inventory WAL address"));
    }
    let source = match location {
        Source::Live { path, .. } => live_source(path, stats)?,
        source => source,
    };
    let len = match &source {
        Source::Live { stamp, .. } => stamp.len,
        Source::Archive { payload_len, .. } => *payload_len,
    };
    if len > store.max_read_bytes {
        return Err(WalError::TooLarge {
            max_bytes: store.max_read_bytes,
            file_size: len,
        });
    }
    if let Some(old) = old.filter(|old| reusable(old, &source)) {
        stats.reused += 1;
        return Ok(old.clone());
    }
    after_stamp();
    let mut triples = Vec::new();
    let mut used_features;
    let (committed_end, last_seq) = match &source {
        Source::Live {
            path,
            stamp,
            lineage_id,
            sealed_tail,
        } => {
            let suffix = old.filter(|old| {
                same_live_lineage(&old.source, &source)
                    && old.committed_end > 0
                    && old.committed_end < stamp.len
                    && matches!(&old.source, Source::Live { stamp: before, .. }
                    if before.len <= stamp.len)
            });
            let mut range = None;
            if let Some(old) = suffix {
                if let Some(file_id) = file_wal::file_identity(path)? {
                    stats.payload_reads += 1;
                    stats.payload_bytes += stamp.len - old.committed_end;
                    store.move_reads.fetch_add(1, Ordering::Relaxed);
                    range = file_wal::read_records_range_from_valid_anchor(
                        path,
                        store.max_read_bytes,
                        old.last_seq,
                        None,
                        &broca_wal::WalByteAnchor {
                            file_id,
                            seq: old.last_seq,
                            byte_end: old.committed_end,
                        },
                    )?;
                    if range.is_some() {
                        triples = old.triples.clone();
                        stats.suffixes += 1;
                    }
                }
            }
            let reused_prefix = range.is_some();
            let range = match range {
                Some(range) => range,
                None => {
                    stats.payload_reads += 1;
                    stats.payload_bytes += stamp.len;
                    store.move_reads.fetch_add(1, Ordering::Relaxed);
                    file_wal::read_records_range(path, store.max_read_bytes, 0, None, None)?
                }
            };
            used_features = range.used_features().to_vec();
            if reused_prefix {
                used_features.extend(
                    old.expect("validated cached suffix")
                        .used_features
                        .iter()
                        .cloned(),
                );
            }
            if range.lineage_id.is_some_and(|id| {
                lineage_id
                    .as_ref()
                    .is_some_and(|expected| id.to_string() != *expected)
            }) {
                return Err(changed("inventory WAL lineage changed during read"));
            }
            let after = live_source(path.clone(), stats)?;
            let grew = matches!(&after, Source::Live { stamp: later, .. }
                if later.len > stamp.len && same_live_lineage(&source, &after));
            if after != source && !grew {
                return Err(changed("WAL changed during inventory read"));
            }
            let WalRangeRecords::Complete(records) = range.records() else {
                return Err(invalid("inventory WAL records are incomplete"));
            };
            let mut last_seq = suffix.map_or(0, |old| old.last_seq);
            let mut end = suffix.map_or(0, |old| old.committed_end);
            // A growing append-only WAL still proves its stamped prefix. Only
            // retain frames ending at strict anchors within that prefix; later
            // in-process RunStarted appends publish their own candidates.
            for frame in records {
                let anchor = range.anchor_at(frame.seq);
                if grew && anchor.is_none() {
                    return Err(changed("growing inventory WAL has no strict byte anchors"));
                }
                if anchor.is_some_and(|anchor| anchor.byte_end > stamp.len) {
                    break;
                }
                if let WalRecord::RunStarted { session, .. } = &frame.record {
                    if session_addr(session) == address && !triples.contains(session) {
                        triples.push(session.clone());
                    }
                }
                last_seq = frame.seq;
                end = anchor.map_or(0, |anchor| anchor.byte_end);
            }
            if *sealed_tail {
                end = stamp.len;
            }
            (end, last_seq)
        }
        Source::Archive {
            path,
            stamp,
            payload_len,
            ..
        } => {
            stats.payload_reads += 1;
            stats.payload_bytes += *payload_len;
            store.move_reads.fetch_add(1, Ordering::Relaxed);
            let member = ArchiveReader::open(path, store.max_read_bytes)
                .map_err(WalError::Io)?
                .read_member(address)
                .map_err(WalError::Io)?
                .ok_or_else(|| invalid("archive inventory member disappeared"))?;
            let range = file_wal::read_verified_member_range(&member.payload, 0, 0, None)?;
            used_features = range.used_features().to_vec();
            add_records(&mut triples, address, &range)?;
            if FileStamp::at(path)? != *stamp {
                return Err(changed("archive changed during inventory read"));
            }
            (
                *payload_len,
                range.next_anchor.map_or(0, |anchor| anchor.seq),
            )
        }
    };
    used_features.sort();
    used_features.dedup();
    Ok(CachedAddress {
        source,
        committed_end,
        last_seq,
        triples,
        used_features,
    })
}

pub(super) fn build(store: &FileWalStore, cancel: &CancellationToken) {
    build_with_hooks(store, cancel, |_| {}, |_| {})
}

fn build_with_hooks(
    store: &FileWalStore,
    cancel: &CancellationToken,
    mut after_pass: impl FnMut(usize),
    mut after_address: impl FnMut(usize),
) {
    let start = std::time::Instant::now();
    let mut stats = MoveInventoryBuildStats::default();
    let cached = load(store, &mut stats);
    let mut inventory = MoveInventory::default();
    let mut next_cache = BTreeMap::new();
    let mut checkpoint = (0, std::time::Instant::now());
    // The cache never defines which addresses exist. Every boot enumerates the
    // current roots/indexes, including locations absent from the previous cache.
    for pass in 0..2 {
        match locations(store, cancel, &mut inventory.unreadable, &mut stats) {
            Ok(locations) => {
                for (address, source) in locations {
                    if next_cache.contains_key(&address) {
                        continue;
                    }
                    if cancel.is_cancelled() {
                        return;
                    }
                    let work = std::time::Instant::now();
                    let reads_before = stats.payload_reads;
                    match scan_address(
                        store,
                        cancel,
                        &address,
                        source,
                        cached.get(&address),
                        &mut stats,
                    ) {
                        Ok(Some(entry)) => {
                            for session in &entry.triples {
                                insert(&mut inventory, &address, session);
                            }
                            next_cache.insert(address, entry);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::error!(address, %error, "move inventory cannot read WAL");
                            inventory.unreadable.push(address);
                        }
                    }
                    if stats.payload_reads != reads_before {
                        pacing::after_payload(work.elapsed(), cancel);
                    }
                    // Partial hints are safe: every boot still takes a fresh census
                    // and validates each entry. Bound lost work across restarts,
                    // without rewriting an already reusable warm cache in chunks.
                    if stats.payload_reads > 0
                        && next_cache.len() > checkpoint.0
                        && (next_cache.len() - checkpoint.0 >= CHECKPOINT_ADDRESSES
                            || checkpoint.1.elapsed() >= CHECKPOINT_INTERVAL)
                    {
                        if let Err(error) = save(store, next_cache.clone()) {
                            tracing::warn!(%error, "move inventory checkpoint could not be saved");
                        }
                        checkpoint = (next_cache.len(), std::time::Instant::now());
                    }
                    after_address(next_cache.len());
                    std::thread::yield_now();
                }
            }
            Err(error) => inventory.unreadable.push(error.to_string()),
        }
        after_pass(pass);
        if inventory.unreadable.is_empty() || pass == 1 {
            break;
        }
        // Retry failures once after the other addresses have been scanned. A fresh
        // census also finds members of an archive that was being sealed earlier.
        inventory.unreadable.clear();
    }
    if cancel.is_cancelled() {
        return;
    }
    // Cache failure affects the next startup's cost, never discovery correctness.
    if let Err(error) = save(store, next_cache) {
        tracing::warn!(%error, "move inventory cache could not be saved");
    }
    if cancel.is_cancelled() {
        return;
    }
    let mut state = store.move_inventory.state.lock().unwrap();
    // Merge identities from RunStarted writes synced while this scan ran. The
    // saved hints can lag those writes: the next boot detects their larger WAL
    // length and reads the appended records before proving inventory complete.
    inventory.sessions.append(&mut state.inventory.sessions);
    inventory.unreadable.append(&mut state.inventory.unreadable);
    stats.elapsed_us = start.elapsed().as_micros() as u64;
    tracing::info!(
        refs = inventory.sessions.len(),
        unreadable = inventory.unreadable.len(),
        elapsed_ms = stats.elapsed_us / 1000,
        payload_reads = stats.payload_reads,
        payload_bytes = stats.payload_bytes,
        lineage_bytes = stats.lineage_bytes,
        cache_bytes = stats.cache_bytes,
        archive_metadata_bytes = stats.archive_metadata_bytes,
        reused = stats.reused,
        suffixes = stats.suffixes,
        "move identity inventory built"
    );
    state.inventory = inventory;
    state.stats = stats;
    state.finished = true;
    drop(state);
    store.move_inventory.ready.notify_waiters();
}
