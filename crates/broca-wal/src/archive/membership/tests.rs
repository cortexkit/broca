use super::*;
use std::sync::{atomic::Ordering, Barrier};

struct Fixture {
    dir: PathBuf,
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-membership-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("fold-1.ark");
        let fixture = Self { dir, path };
        fixture.write("0000000000000001");
        fixture
    }
    // A v1 container: these tests cover the unverified route, which header-checks the
    // index and pins the file.
    fn write(&self, addr: &str) {
        std::fs::write(
            &self.path,
            super::super::v2_tests::v1_container(&[(addr.to_owned(), b"history".to_vec())]),
        )
        .unwrap();
    }
    fn reader(&self) -> ArchiveReader<File> {
        ArchiveReader::open(&self.path, 100).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn lookup(cache: &Cache, fixture: &Fixture, addr: &str) -> io::Result<Option<ArchiveMember>> {
    let mut reader = fixture.reader();
    if cache.absent(&fixture.path, 1, &mut reader, &address(addr)?)? {
        return Ok(None);
    }
    reader.read_member(addr)
}

#[test]
fn unpopulated_membership_never_hides_present_member() {
    let fixture = Fixture::new();
    let cache = Cache::default();
    assert_eq!(
        lookup(&cache, &fixture, "0000000000000001")
            .unwrap()
            .unwrap()
            .payload,
        b"history"
    );
    assert!(lookup(&cache, &fixture, "0000000000000002")
        .unwrap()
        .is_none());
    let reader = fixture.reader();
    let entry = cache
        .entry(&fixture.path, 1, reader.source.get_ref())
        .unwrap()
        .unwrap();
    assert!(entry.members.get().unwrap().is_some());
    assert_eq!(entry.builds.load(Ordering::SeqCst), 1);
}

#[test]
fn corrupt_index_membership_recovers_present_and_preserves_disagreement() {
    let fixture = Fixture::new();
    let mut bytes = std::fs::read(&fixture.path).unwrap();
    let index_address = 64 + b"history".len() + 4;
    bytes[index_address..index_address + 16].copy_from_slice(b"0000000000000002");
    std::fs::write(&fixture.path, bytes).unwrap();
    let cache = Cache::default();
    for _ in 0..2 {
        assert_eq!(
            lookup(&cache, &fixture, "0000000000000001")
                .unwrap()
                .unwrap()
                .payload,
            b"history"
        );
        assert_eq!(
            lookup(&cache, &fixture, "0000000000000002")
                .unwrap_err()
                .to_string(),
            "archive index disagrees with header"
        );
    }
    let reader = fixture.reader();
    let entry = cache
        .entry(&fixture.path, 1, reader.source.get_ref())
        .unwrap()
        .unwrap();
    assert!(entry.members.get().unwrap().is_none());
    let good = Fixture::new();
    assert!(cache
        .absent(&good.path, 1, &mut good.reader(), b"0000000000000002")
        .unwrap());
}

#[test]
fn replacement_at_same_path_has_independent_membership() {
    let fixture = Fixture::new();
    let cache = Cache::default();
    assert!(lookup(&cache, &fixture, "0000000000000002")
        .unwrap()
        .is_none());
    std::fs::rename(&fixture.path, fixture.dir.join("original.ark")).unwrap();
    fixture.write("0000000000000002");
    assert_eq!(
        lookup(&cache, &fixture, "0000000000000002")
            .unwrap()
            .unwrap()
            .payload,
        b"history"
    );
}

#[test]
fn concurrent_membership_misses_build_once_without_blocking_other_containers() {
    let fixture = Fixture::new();
    let cache = Cache::default();
    let barrier = Barrier::new(12);
    std::thread::scope(|scope| {
        for _ in 0..12 {
            scope.spawn(|| {
                barrier.wait();
                assert!(lookup(&cache, &fixture, "0000000000000002")
                    .unwrap()
                    .is_none());
            });
        }
    });
    let reader = fixture.reader();
    let entry = cache
        .entry(&fixture.path, 1, reader.source.get_ref())
        .unwrap()
        .unwrap();
    assert_eq!(entry.builds.load(Ordering::SeqCst), 1);
    // An initializing cell must not hold the cache map lock. While one container's
    // initialization is parked, a second container must finish a real lookup.
    let pending = Fixture::new();
    let other = Fixture::new();
    let pending_entry = cache
        .entry(&pending.path, 1, pending.reader().source.get_ref())
        .unwrap()
        .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            pending_entry.members.get_or_init(|| {
                started_tx.send(()).unwrap();
                done_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                None
            });
        });
        started_rx.recv().unwrap();
        assert!(lookup(&cache, &other, "0000000000000002")
            .unwrap()
            .is_none());
        done_tx.send(()).unwrap();
    });
}

#[test]
fn full_membership_cache_falls_back_without_hiding_history() {
    let cache = Cache::default();
    let fixtures = (0..MAX_PINNED).map(|_| Fixture::new()).collect::<Vec<_>>();
    for fixture in &fixtures {
        assert!(lookup(&cache, fixture, "0000000000000002")
            .unwrap()
            .is_none());
    }
    let overflow = Fixture::new();
    assert_eq!(
        lookup(&cache, &overflow, "0000000000000001")
            .unwrap()
            .unwrap()
            .payload,
        b"history"
    );
    assert_eq!(cache.state().unverified.len(), MAX_PINNED);
    assert!(cache
        .entry(&overflow.path, 1, overflow.reader().source.get_ref())
        .unwrap()
        .is_none());
}

#[test]
fn shared_membership_matches_fresh_readers_including_corruption() {
    for position in [None, Some(0), Some(8), Some(32), Some(64), Some(75)] {
        let fixture = Fixture::new();
        if let Some(position) = position {
            let mut bytes = std::fs::read(&fixture.path).unwrap();
            bytes[position] ^= 1;
            std::fs::write(&fixture.path, bytes).unwrap();
        }
        let cache = Cache::default();
        for addr in ["0000000000000001", "0000000000000002", "0000000000000001"] {
            let expected = fixture
                .reader()
                .read_member(addr)
                .map_err(|e| (e.kind(), e.to_string()));
            assert_eq!(
                lookup(&cache, &fixture, addr).map_err(|e| (e.kind(), e.to_string())),
                expected
            );
        }
    }
}

fn addr(n: u64) -> String {
    format!("{n:016x}")
}

/// A v1 container, whose membership is built by the header-checked route.
fn container(path: &Path, members: &[(String, Vec<u8>)]) {
    std::fs::write(path, super::super::v2_tests::v1_container(members)).unwrap();
}

fn sorted_rescan(path: &Path) -> Box<[[u8; 16]]> {
    let mut members = ArchiveReader::open(path, 0)
        .unwrap()
        .rescan()
        .unwrap()
        .iter()
        .map(|info| address(&info.session_addr).unwrap())
        .collect::<Vec<_>>();
    members.sort_unstable();
    members.into_boxed_slice()
}

/// Payload sizes that matter to a header walk: empty, small, and larger than the 64 KiB
/// read buffer. Enough members to spread the header checks over several threads.
fn mixed_members(count: u64) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|n| {
            let len = match n % 7 {
                0 => 0,
                3 => 64 * 1024 + 1 + n as usize,
                _ => 1 + n as usize,
            };
            // Spread addresses so address order differs from offset order.
            (
                addr(n.wrapping_mul(0x9e37_79b9) & 0xffff_ffff),
                vec![n as u8; len],
            )
        })
        .collect()
}

#[test]
fn index_driven_membership_equals_rescan_membership() {
    let fixture = Fixture::new();
    let cases = [
        Vec::new(),
        vec![(addr(1), b"history".to_vec())],
        vec![(addr(1), Vec::new())],
        vec![(addr(1), vec![1; 70 * 1024])],
        mixed_members(3),
        mixed_members(300),
    ];
    for (n, members) in cases.iter().enumerate() {
        let path = fixture.dir.join(format!("fold-{}.ark", 100 + n));
        container(&path, members);
        let expected = sorted_rescan(&path);
        assert_eq!(expected.len(), members.len(), "case {n}");
        assert_eq!(
            from_index(&mut ArchiveReader::open(&path, 0).unwrap()),
            Some(Some(expected.clone())),
            "case {n}"
        );
        assert_eq!(
            build(&mut ArchiveReader::open(&path, 0).unwrap(), &path),
            (Some(expected.clone()), true),
            "case {n}"
        );
        assert_eq!(
            build_from_headers(&mut ArchiveReader::open(&path, 0).unwrap(), &path),
            Some(expected),
            "case {n}"
        );
    }
    // Damage every byte of a small container in turn. Whatever the damage, the build
    // must decide exactly as the header rescan does, and whenever the index route
    // answered itself it must have produced the rescan's set.
    let intact = std::fs::read(fixture.dir.join("fold-104.ark")).unwrap();
    let damaged_path = fixture.dir.join("fold-200.ark");
    let mut variants = (0..intact.len())
        .map(|position| {
            let mut bytes = intact.clone();
            bytes[position] ^= 0x41;
            bytes
        })
        .collect::<Vec<_>>();
    let trailer = intact.len() - 8;
    let index_len = u32::from_le_bytes(intact[trailer..trailer + 4].try_into().unwrap()) as usize;
    let mut no_index = intact[..trailer - index_len].to_vec();
    no_index.extend_from_slice(&0u32.to_le_bytes());
    no_index.extend_from_slice(&MAGIC.to_le_bytes());
    variants.push(no_index);
    let mut from_index_count = 0;
    for (position, bytes) in variants.iter().enumerate() {
        std::fs::write(&damaged_path, bytes).unwrap();
        let open = || ArchiveReader::open(&damaged_path, 0).unwrap();
        let (built, indexed) = build(&mut open(), &damaged_path);
        let rescanned = build_from_headers(&mut open(), &damaged_path);
        assert_eq!(built, rescanned, "damaged position {position}");
        if indexed {
            from_index_count += 1;
            assert_eq!(
                built.as_deref(),
                open()
                    .rescan()
                    .ok()
                    .map(|_| sorted_rescan(&damaged_path))
                    .as_deref(),
                "damaged position {position}"
            );
        }
    }
    // Payload and digest bytes are not part of membership, so damage there keeps the
    // index route; damage to any checked header field or the index must not.
    assert!(from_index_count > 0);
    assert!(from_index_count < variants.len());
}

/// Counts every byte the build reads, whether through the reader's stream or through
/// the positioned reads the header checks use.
struct Counting {
    file: File,
    read: Arc<std::sync::atomic::AtomicU64>,
}
impl Read for Counting {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(buf)?;
        self.read.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}
impl Seek for Counting {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}
impl ReadAt for Counting {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        ReadAt::read_exact_at(&self.file, buf, offset)?;
        self.read.fetch_add(buf.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn index_driven_membership_reads_headers_not_payloads() {
    let fixture = Fixture::new();
    let path = fixture.dir.join("fold-2.ark");
    let members = mixed_members(200);
    container(&path, &members);
    let count = members.len() as u64;
    let index_len = 4 + 32 * count;
    let bound = 8 + index_len + 64 * count;
    let measure = |capacity: usize, index_route: bool| {
        let read = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut reader = ArchiveReader {
            source: BufReader::with_capacity(
                capacity,
                Counting {
                    file: File::open(&path).unwrap(),
                    read: read.clone(),
                },
            ),
            max_member_bytes: 0,
            stamp: None,
            index: None,
        };
        let built = if index_route {
            let (built, indexed) = build(&mut reader, &path);
            assert!(indexed);
            built
        } else {
            build_from_headers(&mut reader, &path)
        };
        assert_eq!(built.unwrap().len(), members.len());
        read.load(Ordering::Relaxed)
    };
    // Unbuffered, as `ArchiveReader::new` reads a generic source. The index route reads
    // the 8-byte trailer, the index, and one 64-byte header per member, and nothing
    // else, so it must stay within `bound`.
    let unbuffered = measure(0, true);
    assert!(unbuffered <= bound, "{unbuffered} > {bound}");
    // Buffered as `ArchiveReader::open` reads a file. The index read fills the buffer to
    // the end of the file, so it also takes the 8 trailer bytes a second time.
    let buffered = measure(64 * 1024, true);
    assert!(buffered <= bound + 8, "{buffered} > {bound} + 8");
    // The header rescan reads more than the index route's bound in both cases (it reads
    // the trailer twice and probes 64 bytes at the start of the index), which shows
    // these bounds are tight enough to fail.
    let rescan_unbuffered = measure(0, false);
    assert!(rescan_unbuffered > bound, "{rescan_unbuffered} <= {bound}");
    let rescan_buffered = measure(64 * 1024, false);
    assert!(
        rescan_buffered > bound + 8,
        "{rescan_buffered} <= {bound} + 8"
    );
    eprintln!(
        "membership bytes read for {count} members: index route {unbuffered} unbuffered, \
         {buffered} buffered; header rescan {rescan_unbuffered} unbuffered, \
         {rescan_buffered} buffered; bound {bound}"
    );
}

#[test]
fn missing_container_releases_pinned_handle_and_slot() {
    let fixture = Fixture::new();
    let cache = Cache::default();
    assert!(lookup(&cache, &fixture, "0000000000000002")
        .unwrap()
        .is_none());
    let entry = cache
        .entry(&fixture.path, 1, fixture.reader().source.get_ref())
        .unwrap()
        .unwrap();
    let weak = Arc::downgrade(&entry);
    drop(entry);
    cache.reap_missing();
    assert!(weak.upgrade().is_some(), "live subjects must remain cached");
    std::fs::remove_file(&fixture.path).unwrap();
    cache.reap_missing();
    assert!(
        weak.upgrade().is_none(),
        "missing subject must release its pinned handle"
    );
    assert!(cache.state().unverified.is_empty());
}

/// Records every read below any buffering, both streamed and positioned, as
/// (offset, length).
type Reads = Arc<Mutex<Vec<(u64, u64)>>>;

struct TracedFile {
    file: File,
    reads: Reads,
}
impl Read for TracedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let at = self.file.stream_position()?;
        let n = self.file.read(buf)?;
        if n > 0 {
            self.reads.lock().unwrap().push((at, n as u64));
        }
        Ok(n)
    }
}
impl Seek for TracedFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}
impl ReadAt for TracedFile {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        ReadAt::read_exact_at(&self.file, buf, offset)?;
        self.reads.lock().unwrap().push((offset, buf.len() as u64));
        Ok(())
    }
}
impl Subject for TracedFile {
    fn file(&self) -> &File {
        &self.file
    }
}

/// Open `path` as `ArchiveReader::open` would, with every read below the buffer traced.
fn traced_reader(path: &Path, stamp: u64) -> (ArchiveReader<TracedFile>, Reads) {
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = ArchiveReader {
        source: BufReader::with_capacity(
            64 * 1024,
            TracedFile {
                file: File::open(path).unwrap(),
                reads: reads.clone(),
            },
        ),
        max_member_bytes: 1 << 20,
        stamp: Some(stamp),
        index: None,
    };
    (reader, reads)
}

fn members_end(path: &Path) -> u64 {
    let bytes = std::fs::read(path).unwrap();
    let trailer = bytes.len() - 8;
    trailer as u64
        - u64::from(u32::from_le_bytes(
            bytes[trailer..trailer + 4].try_into().unwrap(),
        ))
}

fn resident(cache: &Cache, path: &Path) -> bool {
    let state = cache.state();
    state
        .verified
        .iter()
        .filter(|(key, _)| key.path == path)
        .any(|(_, entry)| resident_bytes(entry) > 0)
        || state
            .unverified
            .iter()
            .filter(|(key, _)| key.path == path)
            .any(|(_, entry)| matches!(entry.members.get(), Some(Some(_))))
}

/// Past any container count, with a deliberately tiny address budget and
/// 40 mixed v1/v2 containers (empty, small and large, plus one v2 container whose digest
/// is damaged), a new-session lookup reads no member bytes anywhere and never a whole
/// verified index, whether or not that container's addresses fit the budget. Unverified
/// containers stay cached ahead of verified ones, although the prewarm reaches the newer
/// verified containers first.
#[test]
fn a_tiny_budget_never_costs_a_scan_and_keeps_unverified_containers_cached() {
    let fixture = Fixture::new();
    let damaged = 17;
    let mut containers = Vec::new();
    let mut unverified_bytes = 0;
    for stamp in 1..=40u64 {
        let count = match stamp % 3 {
            0 => 0,
            1 => 3,
            _ => 300,
        };
        let members = (0..count)
            .map(|i| {
                (
                    addr(stamp * 1_000_000 + i * 7919),
                    vec![i as u8; (i % 50) as usize],
                )
            })
            .collect::<Vec<_>>();
        let path = fixture.dir.join(format!("fold-{stamp}.ark"));
        let v1 = stamp % 5 == 0;
        if v1 {
            container(&path, &members);
        } else {
            let mut bytes = super::super::v2_tests::v2_container(stamp, &members);
            if stamp == damaged {
                let digest = bytes.len() - 8 - 64;
                bytes[digest] ^= 1;
            }
            std::fs::write(&path, bytes).unwrap();
        }
        if v1 || stamp == damaged {
            unverified_bytes += members.len() * ADDRESS_BYTES;
        }
        containers.push((stamp, path, members, v1 || stamp == damaged));
    }
    // Room for every unverified set plus about one large verified one.
    let cache = Cache::with_budget(unverified_bytes + 400 * ADDRESS_BYTES);
    let probe = address("ffffffffffffffff").unwrap();
    // Newest first, as the boot prewarm and every lookup visit them.
    for (stamp, path, _, _) in containers.iter().rev() {
        let mut reader = ArchiveReader::open(path, 1 << 20).unwrap();
        assert_eq!(
            cache.lookup(path, *stamp, &mut reader, &probe).unwrap(),
            Answer::Absent
        );
    }
    let mut verified_not_resident = 0;
    for (stamp, path, members, unverified) in &containers {
        if *unverified {
            assert_eq!(
                cache.cached(path),
                Some(CachedMembership::FromIndex),
                "fold-{stamp}"
            );
            assert!(
                resident(&cache, path),
                "fold-{stamp}: unverified addresses must stay cached"
            );
        } else {
            assert_eq!(
                cache.cached(path),
                Some(CachedMembership::Verified),
                "fold-{stamp}"
            );
            if !members.is_empty() && !resident(&cache, path) {
                verified_not_resident += 1;
            }
        }
    }
    assert!(
        verified_not_resident > 0,
        "the budget must have overflowed for the test to mean anything"
    );
    assert!(cache.state().used <= cache.budget);

    // A new session's lookup in every container, newest first.
    let new_session = address("eeeeeeeeeeeeeeee").unwrap();
    for (stamp, path, members, unverified) in containers.iter().rev() {
        let (mut reader, reads) = traced_reader(path, *stamp);
        assert_eq!(
            cache
                .lookup(path, *stamp, &mut reader, &new_session)
                .unwrap(),
            Answer::Absent,
            "fold-{stamp}"
        );
        let reads = reads.lock().unwrap();
        let region_end = members_end(path);
        let total = reads.iter().map(|(_, len)| len).sum::<u64>();
        for (offset, len) in reads.iter() {
            assert!(
                *offset >= region_end,
                "fold-{stamp}: {len} bytes read at {offset}, inside the member region"
            );
        }
        // The 72-byte tail (v2 extension plus trailer), then one 32-byte entry read per
        // step of the binary search over the entries.
        let probes = u64::from(u64::BITS - (members.len() as u64).leading_zeros());
        let bound = TAIL_LEN + INDEX_ENTRY_LEN * probes;
        if !unverified {
            let index_len = (members.len() as u64) * 32 + 4 + 64;
            assert!(
                total <= bound,
                "fold-{stamp}: {total} bytes read, bound {bound}"
            );
            assert!(
                members.is_empty() || total < index_len,
                "fold-{stamp}: read a whole index"
            );
        } else {
            // Only the tail, which an empty v1 container is too short to have.
            assert!(
                total <= TAIL_LEN,
                "fold-{stamp}: an unverified cached lookup reads only the tail"
            );
        }
    }

    // A member of a verified container whose addresses are not resident is located from
    // the index on disk, and the read through it is the member.
    let (stamp, path, members, _) = containers
        .iter()
        .find(|(_, path, members, unverified)| {
            !unverified && !members.is_empty() && !resident(&cache, path)
        })
        .unwrap();
    let mut offset = 0;
    for (name, payload) in members {
        let mut reader = ArchiveReader::open(path, 1 << 20).unwrap();
        assert_eq!(
            cache
                .lookup(path, *stamp, &mut reader, &address(name).unwrap())
                .unwrap(),
            Answer::Located {
                offset,
                payload_len: payload.len() as u64
            }
        );
        assert_eq!(
            &reader
                .read_located(name, offset, payload.len() as u64)
                .unwrap()
                .payload,
            payload
        );
        offset += 64 + payload.len() as u64;
    }
}

/// A verified container replaced at the same path by another one gets its own entry:
/// the size, index length or digest in the key differs, so the old addresses never
/// answer for the new file. A removed path releases its verification and its budget.
#[test]
fn verified_entries_follow_the_file_and_are_reclaimed_with_it() {
    let fixture = Fixture::new();
    let path = fixture.dir.join("fold-5.ark");
    let write = |n: u64| {
        let bytes = super::super::v2_tests::v2_container(5, &[(addr(n), b"history".to_vec())]);
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, bytes).unwrap();
    };
    let cache = Cache::default();
    let lookup = |n: u64| {
        let mut reader = ArchiveReader::open(&path, 100).unwrap();
        cache
            .lookup(&path, 5, &mut reader, &address(&addr(n)).unwrap())
            .unwrap()
    };
    write(1);
    assert_eq!(lookup(2), Answer::Absent);
    assert_eq!(
        lookup(1),
        Answer::Located {
            offset: 0,
            payload_len: 7
        }
    );
    write(2);
    assert_eq!(
        lookup(2),
        Answer::Located {
            offset: 0,
            payload_len: 7
        }
    );
    assert_eq!(lookup(1), Answer::Absent);
    assert_eq!(cache.state().verified.len(), 2);
    assert_eq!(cache.state().used, 2 * ADDRESS_BYTES);
    std::fs::remove_file(&path).unwrap();
    cache.reap_missing();
    assert!(cache.state().verified.is_empty());
    assert_eq!(cache.state().used, 0);
}
