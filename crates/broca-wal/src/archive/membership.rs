//! Process-shared negative membership for immutable sealed files on Unix.
//!
//! Two kinds of container are cached, and the budget is address bytes, not containers.
//!
//! A container whose index is VERIFIED (index format v2, digest matched) needs no header
//! work: its index is every member. It is keyed on (path, stamp, size, index length,
//! index digest), pins no file, and its verification is remembered even when its address
//! set is not resident. An uncached lookup in it re-reads the 72-byte tail, finds the
//! entry by its digest, and binary-searches the entries on disk: a few positioned reads,
//! never the member region and never the whole index. A replacement file cannot alias an
//! entry, because its own digest, length or size would differ.
//!
//! Every other container is UNVERIFIED (a v1 index, or a v2 index that fails its digest)
//! and is cached as before: keyed on its file identity with a pinned handle, built by an
//! index-checked header pass, and with no fast path at all when uncached, since a miss
//! then costs a full header scan. These are few, so they are capped by count, and they
//! take budget first: admitting one evicts resident verified address sets, whose
//! fallback is cheap, regardless of which containers are newer.
//!
//! Entries for missing paths are reclaimed; unverified entries are never evicted or
//! invalidated. Pinned handles can retain unlinked archive bytes until the next resolver
//! lookup. The authoritative scan's temporary allocation is unchanged.
use super::*;
use std::{
    collections::HashMap,
    os::unix::fs::MetadataExt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
};

// Resident address bytes across every cached container. Each archived session is one member
// of 16 address bytes, and about 3,700 sessions a day are archived, so a year of archive is
// about 21.6 MB: this budget
// covers roughly three years before any verified container has to fall back to its
// on-disk search.
const BUDGET_BYTES: usize = 64 << 20;
const ADDRESS_BYTES: usize = 16;
// Unverified containers pin a file handle each, so they are bounded by count. Only
// containers sealed by a v1 writer, or damaged v2 ones, are unverified.
const MAX_PINNED: usize = 32;
// Verified entries pin nothing and cost a few hundred bytes of key each; the cap only
// bounds memory if containers pile up for decades.
const MAX_VERIFIED: usize = 4096;
const MAX_MEMBERS: usize = 200_000;

// Header checks are independent random reads, and on a cold page cache each one costs a
// full device round trip. Eight in flight measured 0.40-1.86 s cold for 27,693 members
// against 3.0-6.2 s for one at a time; 32 was no better than 8.
const HEADER_CHECK_THREADS: usize = 8;
// Below this many headers per thread, spawning costs more than it saves.
const HEADERS_PER_THREAD: usize = 64;

/// Positioned reads on a shared handle, so several threads can check headers of one
/// container at once without a seek position to fight over.
trait ReadAt: Sync {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
}

impl ReadAt for File {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }
}

/// A source the cache can key: positioned reads plus the file whose identity it is.
trait Subject: Read + Seek + ReadAt {
    fn file(&self) -> &File;
}

impl Subject for File {
    fn file(&self) -> &File {
        self
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct Key {
    path: PathBuf,
    stamp: u64,
    dev: u64,
    ino: u64,
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl Key {
    fn of(path: &Path, stamp: u64, file: &File) -> io::Result<Self> {
        let metadata = file.metadata()?;
        Ok(Self {
            path: path.to_owned(),
            stamp,
            dev: metadata.dev(),
            ino: metadata.ino(),
            size: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct VerifiedKey {
    path: PathBuf,
    stamp: u64,
    size: u64,
    index_len: u32,
    digest: [u8; 32],
}

/// An unverified container's cache entry.
struct Entry {
    // Keeping the inode alive prevents inode-number reuse after unlink/replacement.
    _file: File,
    // `None` once built means no negatives from the cache: the build refused, or the
    // budget had no room (`over_budget`).
    members: OnceLock<Option<Box<[[u8; 16]]>>>,
    // Whether `members` came from the index after every header matched it, rather than
    // from a header rescan. Written before `members` is initialized; only diagnostics read it.
    from_index: AtomicBool,
    over_budget: AtomicBool,
    #[cfg(test)]
    builds: std::sync::atomic::AtomicUsize,
}

/// A verified container's cache entry. The verification itself is never dropped while
/// the path exists; only the resident address set can be.
struct Verified {
    count: u64,
    sorted_on_disk: bool,
    members: Mutex<Option<Arc<[[u8; 16]]>>>,
}

#[derive(Default)]
struct State {
    unverified: HashMap<Key, Arc<Entry>>,
    verified: HashMap<VerifiedKey, Arc<Verified>>,
    // Resident address bytes, unverified and verified together.
    used: usize,
}

struct Cache {
    state: Mutex<State>,
    budget: usize,
}

impl Default for Cache {
    fn default() -> Self {
        Self::with_budget(BUDGET_BYTES)
    }
}

/// What the cache knows about one address in one container.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Answer {
    /// Not a member: the container need not be read.
    Absent,
    /// A verified index lists the address here. The caller still checks the header and
    /// the payload digest.
    Located { offset: u64, payload_len: u64 },
    /// No answer: read the container through `ArchiveReader::read_member`.
    Unknown,
}

/// The 72 bytes before the end of a container: a v2 index extension and the trailer.
struct Tail {
    size: u64,
    index_len: u32,
    digest: [u8; 32],
}

const TAIL_LEN: u64 = INDEX_V2_EXTENSION_LEN + 8;

/// The tail of a sealed container whose trailer and extension have the v2 shape, or
/// `None` for anything else. The shape is only a lookup key: nothing is trusted until the
/// full index has verified under this key at least once.
fn read_tail<S: Subject>(reader: &mut ArchiveReader<S>) -> io::Result<Option<Tail>> {
    let size = reader.source.seek(SeekFrom::End(0))?;
    if size < TAIL_LEN {
        return Ok(None);
    }
    let mut bytes = [0; TAIL_LEN as usize];
    reader
        .source
        .get_ref()
        .read_exact_at(&mut bytes, size - TAIL_LEN)?;
    let index_len = u32::from_le_bytes(bytes[64..68].try_into().unwrap());
    let v2_shape = u32::from_le_bytes(bytes[68..72].try_into().unwrap()) == MAGIC
        && bytes[32..36] == INDEX_V2_TAG
        && bytes[36..64].iter().all(|byte| *byte == 0)
        && u64::from(index_len) >= 4 + INDEX_V2_EXTENSION_LEN
        && (u64::from(index_len) - 4) % INDEX_ENTRY_LEN == 0
        && u64::from(index_len) <= size - 8;
    Ok(v2_shape.then(|| Tail {
        size,
        index_len,
        digest: bytes[..32].try_into().unwrap(),
    }))
}

/// Binary-search the entries of a verified, address-sorted index on disk. Any entry that
/// does not parse means the bytes are not the ones that verified, so the answer is
/// `Unknown` and the caller reads the container fully.
fn search_on_disk<S: ReadAt>(
    source: &S,
    tail: &Tail,
    count: u64,
    addr: &[u8; 16],
) -> io::Result<Answer> {
    let entries = tail.size - 8 - u64::from(tail.index_len) + 4;
    let (mut low, mut high) = (0, count);
    while low < high {
        let middle = low + (high - low) / 2;
        let mut entry = [0; INDEX_ENTRY_LEN as usize];
        source.read_exact_at(&mut entry, entries + middle * INDEX_ENTRY_LEN)?;
        if !entry[..16].iter().all(u8::is_ascii_hexdigit) {
            return Ok(Answer::Unknown);
        }
        match entry[..16].cmp(addr.as_slice()) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                return Ok(Answer::Located {
                    offset: u64::from_le_bytes(entry[16..24].try_into().unwrap()),
                    payload_len: u64::from_le_bytes(entry[24..32].try_into().unwrap()),
                })
            }
        }
    }
    Ok(Answer::Absent)
}

impl Cache {
    fn with_budget(budget: usize) -> Self {
        Self {
            state: Mutex::default(),
            budget,
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn reap_missing(&self) {
        let paths = {
            let state = self.state();
            state
                .unverified
                .iter()
                .filter(|(_, entry)| entry.members.get().is_some())
                .map(|(key, _)| key.path.clone())
                .chain(state.verified.keys().map(|key| key.path.clone()))
                .collect::<BTreeSet<_>>()
        };
        for path in paths {
            // This removes a missing subject, not changed contents under a stable
            // key. Never broaden it to rebuild an entry for a live mutable file.
            // Stat outside the map lock; permission and other errors retain it.
            if !std::fs::metadata(&path).is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
            {
                continue;
            }
            let mut state = self.state();
            let mut freed = 0;
            state.unverified.retain(|key, entry| {
                // An entry still building keeps its key, so lookups that arrive meanwhile
                // wait for that build instead of starting another.
                let keep = key.path != path || entry.members.get().is_none();
                if !keep {
                    freed += entry
                        .members
                        .get()
                        .and_then(Option::as_ref)
                        .map_or(0, |m| m.len() * ADDRESS_BYTES);
                }
                keep
            });
            state.verified.retain(|key, entry| {
                let keep = key.path != path;
                if !keep {
                    freed += resident_bytes(entry);
                }
                keep
            });
            state.used -= freed;
        }
    }

    fn entry(&self, path: &Path, stamp: u64, file: &File) -> io::Result<Option<Arc<Entry>>> {
        let key = Key::of(path, stamp, file)?;
        let mut state = self.state();
        if let Some(entry) = state.unverified.get(&key) {
            return Ok(Some(entry.clone()));
        }
        if state.unverified.len() == MAX_PINNED {
            return Ok(None);
        }
        let entry = Arc::new(Entry {
            _file: file.try_clone()?,
            members: OnceLock::new(),
            from_index: AtomicBool::new(false),
            over_budget: AtomicBool::new(false),
            #[cfg(test)]
            builds: std::sync::atomic::AtomicUsize::new(0),
        });
        state.unverified.insert(key, entry.clone());
        Ok(Some(entry))
    }

    /// Make room for `bytes` of an unverified container's addresses, evicting resident
    /// verified sets (oldest stamp first) if needed. False if even that is not enough.
    fn admit_unverified(&self, bytes: usize) -> bool {
        let mut state = self.state();
        if state.used + bytes > self.budget {
            let mut resident = state
                .verified
                .iter()
                .filter(|(_, entry)| resident_bytes(entry) > 0)
                .map(|(key, entry)| (key.stamp, entry.clone()))
                .collect::<Vec<_>>();
            resident.sort_by_key(|(stamp, _)| *stamp);
            for (_, entry) in resident {
                if state.used + bytes <= self.budget {
                    break;
                }
                let freed = resident_bytes(&entry);
                *entry.members.lock().unwrap_or_else(|e| e.into_inner()) = None;
                state.used -= freed;
            }
        }
        if state.used + bytes > self.budget {
            return false;
        }
        state.used += bytes;
        true
    }

    #[cfg(test)]
    fn absent<S: Subject>(
        &self,
        path: &Path,
        stamp: u64,
        reader: &mut ArchiveReader<S>,
        addr: &[u8; 16],
    ) -> io::Result<bool> {
        Ok(self.lookup(path, stamp, reader, addr)? == Answer::Absent)
    }

    fn lookup<S: Subject>(
        &self,
        path: &Path,
        stamp: u64,
        reader: &mut ArchiveReader<S>,
        addr: &[u8; 16],
    ) -> io::Result<Answer> {
        let tail = read_tail(reader)?;
        let verified_key = tail.as_ref().map(|tail| VerifiedKey {
            path: path.to_owned(),
            stamp,
            size: tail.size,
            index_len: tail.index_len,
            digest: tail.digest,
        });
        if let Some(key) = &verified_key {
            let known = self.state().verified.get(key).cloned();
            if let Some(entry) = known {
                return verified_answer(&entry, reader, tail.as_ref().unwrap(), addr);
            }
        }
        // An unverified container already cached under its file identity is answered
        // from there, without loading its index again.
        if let Some(entry) = self.known_unverified(path, stamp, reader.source.get_ref().file())? {
            return self.unverified_answer(&entry, path, reader, addr);
        }
        if let (Some(key), Some(tail)) = (verified_key, &tail) {
            if let Some(answer) = self.verify(key, tail, reader, addr)? {
                return Ok(answer);
            }
        }
        let Some(entry) = self.entry(path, stamp, reader.source.get_ref().file())? else {
            return Ok(Answer::Unknown);
        };
        self.unverified_answer(&entry, path, reader, addr)
    }

    fn known_unverified(
        &self,
        path: &Path,
        stamp: u64,
        file: &File,
    ) -> io::Result<Option<Arc<Entry>>> {
        let key = Key::of(path, stamp, file)?;
        Ok(self.state().unverified.get(&key).cloned())
    }

    /// Load and verify the index of a container whose tail has the v2 shape. On success,
    /// remember the verification (and the address set, if the budget has room) and answer
    /// from the loaded entries. `None` when it does not verify.
    fn verify<S: Subject>(
        &self,
        key: VerifiedKey,
        tail: &Tail,
        reader: &mut ArchiveReader<S>,
        addr: &[u8; 16],
    ) -> io::Result<Option<Answer>> {
        let index_len = tail.index_len;
        let started = std::time::Instant::now();
        let IndexOutcome::Verified {
            entries,
            digest,
            sorted_on_disk,
        } = reader.load_index(tail.size - 8, index_len)?
        else {
            return Ok(None);
        };
        // The tail read earlier and the index read now must be the same bytes.
        if digest != key.digest {
            return Ok(None);
        }
        let answer =
            match entries.binary_search_by(|entry| entry.address.as_slice().cmp(addr.as_slice())) {
                Ok(i) => Answer::Located {
                    offset: entries[i].offset,
                    payload_len: entries[i].payload_len,
                },
                Err(_) => Answer::Absent,
            };
        let bytes = entries.len() * ADDRESS_BYTES;
        let mut state = self.state();
        if state.verified.contains_key(&key) || state.verified.len() >= MAX_VERIFIED {
            return Ok(Some(answer));
        }
        let resident = state.used + bytes <= self.budget;
        let members = resident.then(|| {
            state.used += bytes;
            entries
                .iter()
                .map(|entry| entry.address)
                .collect::<Arc<[[u8; 16]]>>()
        });
        state.verified.insert(
            key.clone(),
            Arc::new(Verified {
                count: entries.len() as u64,
                sorted_on_disk,
                members: Mutex::new(members),
            }),
        );
        drop(state);
        // Logged once per container per process, like the unverified builds below.
        tracing::info!(
            target: "archive",
            "archive membership cache verified the index of {} with {} members in {} ms ({})",
            key.path.display(),
            entries.len(),
            started.elapsed().as_millis(),
            if resident {
                "addresses cached"
            } else {
                "over the address budget; lookups search the index on disk"
            }
        );
        Ok(Some(answer))
    }

    fn unverified_answer<S: Subject>(
        &self,
        entry: &Entry,
        path: &Path,
        reader: &mut ArchiveReader<S>,
        addr: &[u8; 16],
    ) -> io::Result<Answer> {
        // The map lock has been released. Only readers of this exact container wait
        // for initialization; other containers can build or probe concurrently.
        let members = entry.members.get_or_init(|| {
            #[cfg(test)]
            entry
                .builds
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Timed and logged once per container per process. The build reads every member
            // header in the container. The original sequential header rescan measured ~150 ms
            // warm and 10,821 ms cold on the 27,693-member production container right after a
            // restart (2026-09-25 18:46Z), which is why the daemon prewarms at boot
            // (FileWalStore::prewarm_archive_membership) instead of letting the first new
            // session pay it inside admission. Logged at the unconfigured default level so
            // each boot shows which route built it and whether the prewarm still covers it.
            let started = std::time::Instant::now();
            let (mut built, from_index) = build(reader, path);
            if let Some(members) = &built {
                if !self.admit_unverified(members.len() * ADDRESS_BYTES) {
                    entry.over_budget.store(true, Ordering::Release);
                    built = None;
                }
            }
            entry.from_index.store(from_index, Ordering::Release);
            tracing::info!(
                target: "archive",
                "archive membership cache {} for {} in {} ms",
                match (&built, from_index) {
                    (Some(members), true) => format!(
                        "built from the index (every member header checked) with {} members",
                        members.len()
                    ),
                    (Some(members), false) => {
                        format!("built from a header rescan with {} members", members.len())
                    }
                    (None, _) if entry.over_budget.load(Ordering::Acquire) => {
                        "not cached: the address budget is full (uncached lookups continue)"
                            .to_string()
                    }
                    (None, _) => "not built (uncached lookups continue)".to_string(),
                },
                path.display(),
                started.elapsed().as_millis()
            );
            built
        });
        Ok(
            if members
                .as_ref()
                .is_some_and(|members| members.binary_search(addr).is_err())
            {
                Answer::Absent
            } else {
                Answer::Unknown
            },
        )
    }
}

fn resident_bytes(entry: &Verified) -> usize {
    entry
        .members
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .map_or(0, |members| members.len() * ADDRESS_BYTES)
}

/// Answer from a remembered verification: the resident address set when there is one,
/// and the address-sorted entries on disk to locate a member or to prove absence when
/// there is not.
fn verified_answer<S: Subject>(
    entry: &Verified,
    reader: &mut ArchiveReader<S>,
    tail: &Tail,
    addr: &[u8; 16],
) -> io::Result<Answer> {
    let resident = entry
        .members
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    if resident
        .as_ref()
        .is_some_and(|members| members.binary_search(addr).is_err())
    {
        return Ok(Answer::Absent);
    }
    if !entry.sorted_on_disk {
        return Ok(Answer::Unknown);
    }
    match search_on_disk(reader.source.get_ref(), tail, entry.count, addr)? {
        // The resident set lists it but the disk does not: let the full read decide.
        Answer::Absent if resident.is_some() => Ok(Answer::Unknown),
        answer => Ok(answer),
    }
}

/// Build an unverified container's membership, and report whether the index route
/// produced it. (A verified container never comes here: its index is its membership.)
///
/// The fast route trusts nothing it has not checked: it reads the index, then reads the
/// 64-byte member header at every offset the index names, and accepts the index's
/// address set only if every one of those headers agrees with its entry. On any doubt
/// (no seal, an index that fails validation, an I/O error, any disagreeing header) it
/// runs [`build_from_headers`], the full header rescan plus cross-check, which then
/// decides exactly as it always has: a damaged header or fabricated index address still
/// disables the cache there.
///
/// # Why a successful index route equals the rescan
///
/// `ArchiveReader::load_index` only returns entries that tile the member region exactly
/// (`check_coverage` in archive.rs: the coverage, bounds and "omits members" checks): the
/// first offset is 0, each next offset is `offset + 64 + payload_len`, the last member
/// ends exactly where the index starts, and no address repeats. [`headers_match`] then
/// confirms, through the same `parse_header` the rescan uses, that the header at each
/// entry's offset is a valid version-1 member header whose address and payload length
/// equal the entry's. So a rescan starting at offset 0 (`ArchiveReader::scan`) would
/// read exactly those headers: each one passes every check it applies, and each header's
/// length moves it to the next entry's offset, never in between. After the last member
/// it reaches the index, where its magic test (the `MAGIC_MEMBER` comparison in `scan`)
/// reads the index's `u32` entry count. That count is at most `(u32::MAX - 4) / 32`,
/// because `index_len` is a `u32` equal to `4 + 32 * count` (v1) or `4 + 32 * count + 64`
/// (v2), and `load_index` checks that equation, so it is always far below `MAGIC_MEMBER`
/// (0x4D4C4157). The rescan therefore stops there, before any v2 extension, and returns
/// exactly the index's entries, without a duplicate. Its cross-check (every index address
/// among the headers) then passes, and it returns the sorted index addresses: the same
/// set this route returns. When the index has more than `MAX_MEMBERS` entries the rescan
/// can only refuse to cache (too many headers, or too few to contain every index
/// address), so this route gives that same answer, no cache, without reading any header
/// and without falling back.
///
/// A header that disagrees is exactly what the rescan's cross-check exists to catch, so
/// that case never takes this route's answer: it falls back, and the rescan decides.
fn build<R: Read + Seek + ReadAt>(
    reader: &mut ArchiveReader<R>,
    path: &Path,
) -> (Option<Box<[[u8; 16]]>>, bool) {
    match from_index(reader) {
        Some(members) => (members, true),
        None => (build_from_headers(reader, path), false),
    }
}

/// The index route of [`build`]. `None` means "not established; fall back", never "no
/// cache"; `Some(None)` is the container that is too large to cache at all.
fn from_index<R: Read + Seek + ReadAt>(
    reader: &mut ArchiveReader<R>,
) -> Option<Option<Box<[[u8; 16]]>>> {
    let (end, index_len) = reader.sealed_trailer().ok()?;
    let mut entries = match reader.load_index(end, index_len).ok()? {
        IndexOutcome::Verified { entries, .. } | IndexOutcome::Unverified { entries } => entries,
        IndexOutcome::Invalid(_) => return None,
    };
    if entries.len() > MAX_MEMBERS {
        return Some(None);
    }
    let mut members = entries
        .iter()
        .map(|entry| entry.address)
        .collect::<Vec<_>>();
    members.sort_unstable();
    // Offset order makes the header reads follow file order, so the storage sees
    // forward-moving reads. Measured cold on one thread over 11,101 members, address
    // order took 6.0 s against 1.2 to 3.7 s in offset order.
    entries.sort_unstable_by_key(|entry| entry.offset);
    let members_end = end - u64::from(index_len);
    headers_match(reader.source.get_ref(), &entries, members_end)
        .then(|| Some(members.into_boxed_slice()))
}

/// True only if the header at every entry's offset parses as a member header and names
/// that entry's address and payload length. Stops early once any check fails.
fn headers_match<S: ReadAt>(source: &S, entries: &[IndexEntry], end: u64) -> bool {
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let check = || {
        while !failed.load(Ordering::Relaxed) {
            let Some(entry) = entries.get(next.fetch_add(1, Ordering::Relaxed)) else {
                return;
            };
            let mut bytes = [0; MEMBER_HEADER_LEN as usize];
            let matches = source.read_exact_at(&mut bytes, entry.offset).is_ok()
                && parse_header(&bytes, entry.offset, end).is_ok_and(|info| {
                    info.session_addr.as_bytes() == entry.address
                        && info.payload_len == entry.payload_len
                });
            if !matches {
                failed.store(true, Ordering::Relaxed);
            }
        }
    };
    let threads = entries
        .len()
        .div_ceil(HEADERS_PER_THREAD)
        .min(HEADER_CHECK_THREADS);
    std::thread::scope(|scope| {
        // The calling thread is one of the workers. A helper that fails to spawn only
        // costs speed: the workers left still take every remaining entry.
        for _ in 1..threads {
            let _ = std::thread::Builder::new()
                .name("archive-membership".into())
                .spawn_scoped(scope, check);
        }
        check();
    });
    // The scope has joined every worker, so every result is visible here.
    !failed.load(Ordering::Relaxed)
}

/// The original build, and still the authority whenever the index route cannot vouch
/// for the container: walk every member header, then cross-check the index against them.
fn build_from_headers<R: Read + Seek>(
    reader: &mut ArchiveReader<R>,
    path: &Path,
) -> Option<Box<[[u8; 16]]>> {
    let headers = reader.rescan().ok()?;
    if headers.len() > MAX_MEMBERS {
        return None;
    }
    let mut members = headers
        .iter()
        .map(|header| address(&header.session_addr).expect("rescan validates addresses"))
        .collect::<Vec<_>>();
    members.sort_unstable();
    let (end, len) = reader.sealed_trailer().ok()?;
    match reader.load_index(end, len) {
        Ok(
            IndexOutcome::Verified { entries: index, .. }
            | IndexOutcome::Unverified { entries: index },
        ) => {
            if index
                .iter()
                .any(|entry| members.binary_search(&entry.address).is_err())
            {
                // A fabricated index address would previously report disagreement on
                // lookup. Do not turn that error into a cached negative answer.
                // A LOG RECORD RATHER THAN `eprintln!`: this runs inside the daemon, whose
                // log is shared with every sibling module. Incremental eprintln syscalls
                // splice under concurrency, and an unattributed line gets attributed to
                // whoever the reader assumes -- 4,710 store-projection warnings were read
                // as broca's when they came from a sibling process. The logger writes each
                // record in one call and stamps it with this module's id, which is where
                // the attribution the old hand-written `broca-wal:` prefix carried now lives.
                tracing::warn!(
                    target: "archive",
                    "archive membership cache disabled for {}: an index address is absent from the headers, so negatives for this container stay authoritative",
                    path.display()
                );
                return None;
            }
        }
        Ok(IndexOutcome::Invalid(_)) => {}
        Err(_) => return None,
    }
    Some(members.into_boxed_slice())
}

static CACHE: OnceLock<Cache> = OnceLock::new();

pub(super) fn reap_missing() {
    if let Some(cache) = CACHE.get() {
        cache.reap_missing();
    }
}

pub(super) fn cached(path: &Path) -> Option<CachedMembership> {
    CACHE.get()?.cached(path)
}

impl Cache {
    fn cached(&self, path: &Path) -> Option<CachedMembership> {
        let state = self.state();
        if state.verified.keys().any(|key| key.path == path) {
            return Some(CachedMembership::Verified);
        }
        state
            .unverified
            .iter()
            .filter(|(key, _)| key.path == path)
            .find_map(|(_, entry)| {
                Some(match entry.members.get()? {
                    None if entry.over_budget.load(Ordering::Acquire) => {
                        CachedMembership::OverBudget
                    }
                    None => CachedMembership::Disabled,
                    Some(_) if entry.from_index.load(Ordering::Acquire) => {
                        CachedMembership::FromIndex
                    }
                    Some(_) => CachedMembership::FromHeaders,
                })
            })
    }
}

pub(super) fn lookup(
    path: &Path,
    stamp: u64,
    reader: &mut ArchiveReader<File>,
    addr: &[u8; 16],
) -> io::Result<Answer> {
    CACHE
        .get_or_init(Cache::default)
        .lookup(path, stamp, reader, addr)
}

#[cfg(test)]
mod tests;
