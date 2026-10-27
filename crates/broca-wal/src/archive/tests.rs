use super::*;
use std::{
    cell::Cell,
    io::Cursor,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = broca_test_support::scratch_root().join(format!(
            "broca-archive-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, stamp: u64) -> PathBuf {
        self.0.join(format!("fold-{stamp}.ark"))
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn addr(n: u64) -> String {
    format!("{n:016x}")
}
fn fixture(path: &Path, members: &[(String, Vec<u8>)]) {
    let mut writer = ArchiveWriter::create(path).unwrap().without_feature_floor();
    for (name, payload) in members {
        writer.append(name, payload).unwrap();
    }
    writer.seal().unwrap();
}
/// A v1 container, as sealed before index format v2, for tests of the unverified route.
fn fixture_v1(path: &Path, members: &[(String, Vec<u8>)]) {
    std::fs::write(path, super::v2_tests::v1_container(members)).unwrap();
}
fn without_index(bytes: &[u8]) -> Vec<u8> {
    let trailer = bytes.len() - 8;
    let len = u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap()) as usize;
    let mut result = bytes[..trailer - len].to_vec();
    // Delete only the cache, retaining the seal that distinguishes a completed pass.
    result.extend_from_slice(&0u32.to_le_bytes());
    result.extend_from_slice(&MAGIC.to_le_bytes());
    result
}

#[test]
fn round_trip_verbatim_wals_and_digests() {
    let dir = TempDir::new();
    let path = dir.path(1);
    let lineage = crate::framing::encode_lineage(crate::WalLineageId::from_bytes([0x5a; 16]));
    let members = vec![
        (addr(0), Vec::new()),
        (addr(1), lineage.to_vec()),
        (
            addr(2),
            crate::framing::encode(
                1,
                42,
                &crate::WalRecord::TurnFinished {
                    step_id: broca_protocol::StepId(1),
                },
            )
            .unwrap(),
        ),
        (addr(3), (0..=255).collect()),
    ];
    fixture(&path, &members);
    let mut reader = ArchiveReader::open(&path, 1024).unwrap();
    assert_eq!(reader.state().unwrap(), ContainerState::Sealed);
    for (name, payload) in members {
        let member = reader.read_member(&name).unwrap().unwrap();
        assert_eq!(member.payload, payload);
        assert_eq!(
            member.info.payload_sha256.as_slice(),
            Sha256::digest(&payload).as_slice()
        );
    }
    assert!(reader.read_member(&addr(99)).unwrap().is_none());
}

#[test]
fn wire_layout_is_pinned() {
    let dir = TempDir::new();
    let path = dir.path(1);
    fixture(&path, &[("0123456789abcdef".into(), b"abc".to_vec())]);
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(bytes.len(), 175);
    assert_eq!(&bytes[0..8], b"WALM\x01\x00\x00\x00");
    assert_eq!(&bytes[8..24], b"0123456789abcdef");
    assert_eq!(&bytes[24..32], &3u64.to_le_bytes());
    // SHA-256("abc"), a published test vector independent of this codec's hashing.
    assert_eq!(
        &bytes[32..64],
        &[
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad
        ]
    );
    assert_eq!(&bytes[64..67], b"abc");
    assert_eq!(&bytes[67..71], &1u32.to_le_bytes());
    assert_eq!(&bytes[71..87], b"0123456789abcdef");
    assert_eq!(&bytes[87..95], &0u64.to_le_bytes());
    assert_eq!(&bytes[95..103], &3u64.to_le_bytes());
    // Index format v2: the extension follows the entries, its fixed bytes next to the
    // trailer, and `index_len` counts it. The digest is recomputed here from the layout.
    let mut hasher = Sha256::new();
    hasher.update(b"broca-ark-index/v2\0");
    hasher.update(1u64.to_le_bytes());
    hasher.update(100u32.to_le_bytes());
    hasher.update(&bytes[67..103]);
    hasher.update(&bytes[135..167]);
    assert_eq!(&bytes[103..135], hasher.finalize().as_slice());
    assert_eq!(&bytes[135..139], b"IDX2");
    assert_eq!(&bytes[139..167], &[0; 28]);
    assert_eq!(&bytes[167..171], &100u32.to_le_bytes());
    assert_eq!(&bytes[171..175], b"WALK");
}

struct Counted<R> {
    inner: R,
    read: Rc<Cell<u64>>,
}
impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read.set(self.read.get() + n as u64);
        Ok(n)
    }
}
impl<R: Seek> Seek for Counted<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

#[test]
fn bounded_read_measures_one_index_and_one_member() {
    let dir = TempDir::new();
    let path = dir.path(1);
    let members = (0..128)
        .map(|n| (addr(n), vec![n as u8; 4096]))
        .collect::<Vec<_>>();
    fixture(&path, &members);
    let size = std::fs::metadata(&path).unwrap().len();
    let read = Rc::new(Cell::new(0));
    let mut reader = ArchiveReader::new(
        Counted {
            inner: File::open(path).unwrap(),
            read: read.clone(),
        },
        4096,
    );
    assert_eq!(
        reader.read_member(&addr(127)).unwrap().unwrap().payload,
        vec![127; 4096]
    );
    eprintln!(
        "bounded read: {} bytes read / {size} container bytes",
        read.get()
    );
    assert!(read.get() < size);
    // The trailer, the index with its 64-byte v2 extension, then one header and payload.
    assert_eq!(read.get(), 8 + 4 + 128 * 32 + 64 + 64 + 4096);
}

/// A header walk reads each member's 64-byte header once and nothing else: no payload
/// bytes, and no second read of the same header. The source is buffered exactly as
/// `ArchiveReader::open` buffers a file, because the waste this guards against came from
/// that buffer: every seek empties it, so a small read through it refills 64 KiB.
#[test]
fn rescan_reads_each_member_header_once() {
    let dir = TempDir::new();
    let path = dir.path(1);
    let members = (0..50)
        .map(|n| (addr(n), vec![n as u8; 1000]))
        .collect::<Vec<_>>();
    fixture(&path, &members);
    let read = Rc::new(Cell::new(0));
    let mut reader = ArchiveReader {
        source: BufReader::with_capacity(
            64 * 1024,
            Counted {
                inner: File::open(&path).unwrap(),
                read: read.clone(),
            },
        ),
        max_member_bytes: 1000,
        stamp: None,
        index: None,
    };
    assert_eq!(reader.rescan().unwrap().len(), 50);
    // The seal supplies the index boundary, so the walk reads only member headers.
    assert_eq!(read.get(), 8 + 50 * 64);
}

#[test]
fn deleted_index_resolves_every_member_and_preserves_numeric_precedence() {
    let dir = TempDir::new();
    let older = dir.path(9);
    let newer = dir.path(10);
    let paths = vec![older.clone(), newer.clone()];
    let members = (0..101)
        .map(|n| (addr(n), vec![n as u8; 100]))
        .collect::<Vec<_>>();
    fixture(&older, &members);
    fixture(&newer, &[(addr(50), b"post-append transcript".to_vec())]);
    for delete in [false, true] {
        if delete {
            for path in &paths {
                std::fs::write(path, without_index(&std::fs::read(path).unwrap())).unwrap();
            }
        }
        for (name, bytes) in &members {
            let member = read_newest_member(&paths, name, 1000).unwrap().unwrap();
            let expected = if name == &addr(50) {
                b"post-append transcript".as_slice()
            } else {
                bytes.as_slice()
            };
            assert_eq!(member.payload, expected, "deleted={delete} address={name}");
        }
        let mut reader = ArchiveReader::open(&older, 1000).unwrap();
        assert_eq!(reader.rescan().unwrap().len(), 101);
    }
}

#[test]
fn corrupt_index_fields_recover_without_hiding_members() {
    let dir = TempDir::new();
    let path = dir.path(1);
    fixture(
        &path,
        &[(addr(1), b"one".to_vec()), (addr(2), b"two".to_vec())],
    );
    let bytes = std::fs::read(path).unwrap();
    let index = 134;
    for position in [index, index + 4, index + 20, index + 28] {
        let mut corrupt = bytes.clone();
        corrupt[position] ^= 0xff;
        let mut reader = ArchiveReader::new(Cursor::new(corrupt), 100);
        assert_eq!(
            reader.read_member(&addr(1)).unwrap().unwrap().payload,
            b"one"
        );
        assert_eq!(
            reader.read_member(&addr(2)).unwrap().unwrap().payload,
            b"two"
        );
    }
    let mut bad_boundary = bytes.clone();
    let trailer = bad_boundary.len() - 8;
    bad_boundary[trailer] ^= 0xff;
    assert!(ArchiveReader::new(Cursor::new(bad_boundary), 100)
        .read_member(&addr(1))
        .is_err());
    // A syntactically valid but wrong address must not turn a cache miss into absence.
    let mut corrupt = bytes;
    corrupt[index + 4..index + 20].copy_from_slice(addr(3).as_bytes());
    let mut reader = ArchiveReader::new(Cursor::new(corrupt), 100);
    assert_eq!(
        reader.read_member(&addr(1)).unwrap().unwrap().payload,
        b"one"
    );
}

#[test]
fn unsealed_lookup_and_rescan_refuse_and_newest_ignores_crashed_pass() {
    let dir = TempDir::new();
    let old = dir.path(9);
    let new = dir.path(10);
    fixture(&old, &[(addr(1), b"committed".to_vec())]);
    let mut writer = ArchiveWriter::create(&new).unwrap().without_feature_floor();
    writer.append(&addr(1), b"uncommitted").unwrap();
    drop(writer);
    let mut reader = ArchiveReader::open(&new, 100).unwrap();
    assert_eq!(reader.state().unwrap(), ContainerState::Unsealed);
    assert_eq!(
        reader.read_member(&addr(1)).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        reader.rescan().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        read_newest_member(&[old, new], &addr(1), 100)
            .unwrap()
            .unwrap()
            .payload,
        b"committed"
    );
}

#[test]
fn sealed_path_cannot_be_reopened_for_writing() {
    let dir = TempDir::new();
    let path = dir.path(1);
    fixture(&path, &[(addr(1), b"immutable".to_vec())]);
    let before = std::fs::read(&path).unwrap();
    assert!(ArchiveWriter::create(&path).is_err());
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn every_truncation_refuses_members_without_panicking() {
    let dir = TempDir::new();
    let path = dir.path(1);
    fixture(&path, &[(addr(1), vec![7; 40]), (addr(2), vec![8; 30])]);
    let bytes = std::fs::read(path).unwrap();
    for cut in 0..bytes.len() {
        let mut reader = ArchiveReader::new(Cursor::new(bytes[..cut].to_vec()), 1000);
        assert_eq!(
            reader.state().unwrap(),
            ContainerState::Unsealed,
            "cut={cut}"
        );
        assert!(reader.read_member(&addr(1)).is_err(), "cut={cut}");
        assert!(reader.read_member(&addr(2)).is_err(), "cut={cut}");
    }
    // Retaining a seal cannot make a mid-header or mid-payload cut valid either.
    for cut in [3, 30, 66, 104 + 10, 104 + 64 + 4] {
        let mut corrupt = bytes[..cut].to_vec();
        corrupt.extend_from_slice(&0u32.to_le_bytes());
        corrupt.extend_from_slice(&MAGIC.to_le_bytes());
        let mut reader = ArchiveReader::new(Cursor::new(corrupt), 1000);
        assert!(reader.rescan().is_err(), "cut={cut}");
        assert!(reader.read_member(&addr(1)).is_err(), "cut={cut}");
    }
}

#[test]
fn corrupt_payload_is_never_served_even_without_index() {
    let dir = TempDir::new();
    let path = dir.path(1);
    fixture(&path, &[(addr(1), b"original".to_vec())]);
    let bytes = std::fs::read(path).unwrap();
    for mut corrupt in [bytes.clone(), without_index(&bytes)] {
        corrupt[64] ^= 0xff;
        let mut reader = ArchiveReader::new(Cursor::new(corrupt), 100);
        assert_eq!(
            reader.read_member(&addr(1)).unwrap_err().to_string(),
            "archive member digest mismatch"
        );
    }
}

#[test]
fn member_lengths_are_bounded_before_allocation() {
    let dir = TempDir::new();
    let path = dir.path(1);
    fixture(&path, &[(addr(1), b"original".to_vec())]);
    let bytes = std::fs::read(path).unwrap();
    for length in [u64::MAX, 1 << 40] {
        let mut corrupt = without_index(&bytes);
        corrupt[24..32].copy_from_slice(&length.to_le_bytes());
        let mut reader = ArchiveReader::new(Cursor::new(corrupt), u64::MAX);
        assert_eq!(
            reader.read_member(&addr(1)).unwrap_err().to_string(),
            "archive member exceeds container bounds"
        );
    }
    let mut reader = ArchiveReader::new(Cursor::new(bytes), 7);
    assert_eq!(
        reader.read_member(&addr(1)).unwrap_err().to_string(),
        "archive member exceeds per-member read limit"
    );
}

#[test]
fn container_above_one_gib_still_serves_small_member_with_bounded_io() {
    let dir = TempDir::new();
    let path = dir.path(1);
    // Sparse payload makes the real file larger than the per-WAL cap without writing
    // a gigabyte. Only the small member is read, so the skipped digest is irrelevant.
    let huge = crate::DEFAULT_MAX_READ_BYTES + 1;
    let offset = 64 + huge;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut header = [0u8; 64];
    header[..4].copy_from_slice(&MAGIC_MEMBER.to_le_bytes());
    header[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    header[8..24].copy_from_slice(addr(1).as_bytes());
    header[24..32].copy_from_slice(&huge.to_le_bytes());
    file.write_all(&header).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    header[8..24].copy_from_slice(addr(2).as_bytes());
    header[24..32].copy_from_slice(&3u64.to_le_bytes());
    header[32..64].copy_from_slice(&Sha256::digest(b"abc"));
    file.write_all(&header).unwrap();
    file.write_all(b"abc").unwrap();
    file.write_all(&2u32.to_le_bytes()).unwrap();
    for (n, position, len) in [(1, 0, huge), (2, offset, 3)] {
        file.write_all(addr(n).as_bytes()).unwrap();
        file.write_all(&position.to_le_bytes()).unwrap();
        file.write_all(&len.to_le_bytes()).unwrap();
    }
    file.write_all(&68u32.to_le_bytes()).unwrap();
    file.write_all(&MAGIC.to_le_bytes()).unwrap();
    drop(file);
    let size = std::fs::metadata(&path).unwrap().len();
    assert!(size > crate::DEFAULT_MAX_READ_BYTES);
    let read = Rc::new(Cell::new(0));
    let mut reader = ArchiveReader::new(
        Counted {
            inner: File::open(path).unwrap(),
            read: read.clone(),
        },
        crate::DEFAULT_MAX_READ_BYTES,
    );
    assert_eq!(
        reader.read_member(&addr(2)).unwrap().unwrap().payload,
        b"abc"
    );
    eprintln!(
        "large container: {} bytes read / {size} container bytes",
        read.get()
    );
    assert_eq!(read.get(), 8 + 68 + 64 + 3);
    assert_eq!(reader.rescan().unwrap().len(), 2);
    assert!(reader.read_member(&addr(1)).is_err());
}

#[test]
fn invalid_names_and_duplicate_members_are_refused() {
    for name in [
        "fold-01.ark",
        "fold--1.ark",
        "fold-+1.ark",
        "fold-1.journal",
        "fold-18446744073709551616.ark",
    ] {
        assert!(fold_timestamp(name).is_err());
    }
    let dir = TempDir::new();
    let path = dir.path(1);
    let mut writer = ArchiveWriter::create(&path)
        .unwrap()
        .without_feature_floor();
    assert!(writer.append("not hex", b"a").is_err());
    writer.append(&addr(1), b"a").unwrap();
    assert!(writer.append(&addr(1), b"b").is_err());
    writer.seal().unwrap();
    assert!(read_newest_member(&[path.clone(), path], &addr(1), 100).is_err());
}

#[test]
fn invalid_headers_and_newer_corruption_fail_closed() {
    let dir = TempDir::new();
    let old = dir.path(1);
    let new = dir.path(2);
    fixture(&old, &[(addr(1), b"old".to_vec())]);
    fixture(&new, &[(addr(1), b"new".to_vec())]);
    let bytes = std::fs::read(&new).unwrap();
    for position in [4, 6, 8, 32, 64] {
        let mut corrupt = bytes.clone();
        corrupt[position] ^= 0xff;
        std::fs::write(&new, corrupt).unwrap();
        assert!(
            read_newest_member(&[old.clone(), new.clone()], &addr(1), 100).is_err(),
            "position={position}"
        );
    }
}

#[test]
fn failed_write_cannot_be_sealed() {
    let dir = TempDir::new();
    let path = dir.path(1);
    let mut writer = ArchiveWriter::create(&path)
        .unwrap()
        .without_feature_floor();
    // Replace the writable descriptor with a read-only one to force a real I/O error.
    writer.file = File::open(&path).unwrap();
    assert!(writer.append(&addr(1), b"payload").is_err());
    assert!(writer.append(&addr(2), b"later").is_err());
    assert!(writer.seal().is_err());
    assert_eq!(
        ArchiveReader::open(&path, 100).unwrap().state().unwrap(),
        ContainerState::Unsealed
    );
}

#[test]
fn newer_damaged_member_magic_never_returns_older_transcript() {
    let dir = TempDir::new();
    let old = dir.path(9);
    let new = dir.path(10);
    let older_payload = b"older transcript";
    fixture(&old, &[(addr(1), older_payload.to_vec())]);
    fixture(&new, &[(addr(1), b"newer complete transcript".to_vec())]);
    let intact = std::fs::read(&new).unwrap();
    let mut damaged = intact.clone();
    damaged[0] ^= 1;
    assert_eq!(&damaged[1..], &intact[1..]);
    std::fs::write(&new, damaged).unwrap();

    let result = read_newest_member(&[old, new], &addr(1), 100);
    assert_ne!(
        result
            .as_ref()
            .ok()
            .and_then(|member| member.as_ref())
            .map(|member| member.payload.as_slice()),
        Some(older_payload.as_slice()),
        "damaged newest header must not return the older transcript"
    );
    let error = result.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "invalid archive member header");
}

#[test]
fn a_container_retired_between_discovery_and_open_does_not_fail_the_lookup() {
    // Callers snapshot PATHNAMES and reads are lease-free, so a fold can retire a
    // container after discovery and before open. Propagating that NotFound failed the
    // whole lookup: an unrelated archived transcript returned a storage error, and a
    // genuinely new session could fail admission while proving it has no predecessor.
    let dir = TempDir::new();
    let old = dir.path(9);
    let retired = dir.path(10);
    fixture(&old, &[(addr(1), b"older transcript".to_vec())]);
    fixture(&retired, &[(addr(2), b"unrelated".to_vec())]);
    let paths = [old.clone(), retired.clone()];

    // The fold unlinks the newer container while the reader holds only its pathname.
    std::fs::remove_file(&retired).unwrap();

    assert_eq!(
        read_newest_member(&paths, &addr(1), 100)
            .unwrap()
            .unwrap()
            .payload,
        b"older transcript",
        "an intact older transcript must still be served"
    );
    assert!(
        read_newest_member(&paths, &addr(2), 100).unwrap().is_none(),
        "a session that lived ONLY in the retired container reads as absent, not as an error"
    );
}

/// The NotFound skip is scoped on purpose: an UNREADABLE container still fails loud.
///
/// A permission or I/O fault is a real storage failure and must not be laundered into
/// "no member here". (A TRUNCATED container is a different case and deliberately not
/// this test: it presents as unsealed, which `read_newest_member` skips by design
/// because an unsealed container belongs to a pass that never completed.)
#[cfg(unix)]
#[test]
fn a_container_that_cannot_be_opened_still_fails_loud() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new();
    let path = dir.path(9);
    fixture(&path, &[(addr(1), b"transcript".to_vec())]);
    // Readable first, so the failure below is attributable to the permission change.
    assert!(
        read_newest_member(std::slice::from_ref(&path), &addr(1), 100)
            .unwrap()
            .is_some()
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
    let outcome = read_newest_member(std::slice::from_ref(&path), &addr(1), 100);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        outcome.is_err_and(|e| e.kind() != std::io::ErrorKind::NotFound),
        "an unreadable container must not read as an absent member"
    );
}

#[test]
fn newer_absent_session_still_returns_older_transcript() {
    let dir = TempDir::new();
    let old = dir.path(9);
    let new = dir.path(10);
    fixture(&old, &[(addr(1), b"older transcript".to_vec())]);
    fixture(&new, &[(addr(2), b"different session".to_vec())]);
    assert!(ArchiveReader::open(&new, 100)
        .unwrap()
        .read_member(&addr(1))
        .unwrap()
        .is_none());
    assert_eq!(
        read_newest_member(&[old, new], &addr(1), 100)
            .unwrap()
            .unwrap()
            .payload,
        b"older transcript"
    );
}

/// Fill the process-wide membership cache for `paths` with an address no fixture uses,
/// through the fallback scan, as the daemon's boot prewarm does. The answer is incidental.
#[cfg(unix)]
fn prewarm(paths: &[PathBuf]) -> io::Result<Option<ArchiveMember>> {
    scan_newest_member(paths, "ffffffffffffffff", 100)
}

/// The index names member 2 under another valid address, which `load_index` cannot
/// detect. The header check at that entry fails, so the cache falls back to the header
/// rescan, which refuses to cache the container. Member 2 must still be found under its
/// real address, and the fabricated address must still report the disagreement.
#[cfg(unix)]
#[test]
fn index_driven_cache_never_hides_a_member_behind_a_fabricated_index_address() {
    let dir = TempDir::new();
    let path = dir.path(1);
    fixture(
        &path,
        &[
            (addr(1), b"one".to_vec()),
            (addr(2), b"two".to_vec()),
            (addr(3), b"three".to_vec()),
        ],
    );
    let mut bytes = std::fs::read(&path).unwrap();
    // Three headers and 11 payload bytes, then the 4-byte count; entries are in
    // append order, so the second one is member 2's.
    let entry = 3 * 64 + 11 + 4 + 32;
    assert_eq!(&bytes[entry..entry + 16], addr(2).as_bytes());
    bytes[entry..entry + 16].copy_from_slice(addr(9).as_bytes());
    std::fs::write(&path, bytes).unwrap();
    let paths = [path.clone()];
    assert!(prewarm(&paths).unwrap().is_none());
    for _ in 0..2 {
        assert_eq!(
            read_newest_member(&paths, &addr(2), 100)
                .unwrap()
                .expect("a fabricated index address must not hide the real member")
                .payload,
            b"two"
        );
        assert_eq!(
            read_newest_member(&paths, &addr(9), 100)
                .unwrap_err()
                .to_string(),
            "archive index disagrees with header"
        );
    }
    assert_eq!(cached_membership(&path), Some(CachedMembership::Disabled));
}

/// Both containers hold address 1, and the newer one's header for it has damaged magic
/// under an intact index. With the cache already built, a lookup must refuse with the
/// header error, never fall through to the older container's transcript, and the newer
/// container's membership must not have been taken from its index. Both are v1
/// containers, whose membership is built by the header-checked route.
#[cfg(unix)]
#[test]
fn index_driven_cache_never_serves_an_older_transcript_past_damaged_newer_magic() {
    let dir = TempDir::new();
    let old = dir.path(9);
    let new = dir.path(10);
    let older_payload = b"older transcript";
    fixture_v1(&old, &[(addr(1), older_payload.to_vec())]);
    fixture_v1(
        &new,
        &[
            (addr(5), b"five".to_vec()),
            (addr(1), b"newer transcript".to_vec()),
            (addr(7), b"seven".to_vec()),
        ],
    );
    let mut bytes = std::fs::read(&new).unwrap();
    // Member 1 is the second member, after one header and the 4-byte payload "five".
    let header = 64 + 4;
    assert_eq!(&bytes[header + 8..header + 24], addr(1).as_bytes());
    bytes[header] ^= 1;
    std::fs::write(&new, bytes).unwrap();
    assert!(prewarm(std::slice::from_ref(&old)).unwrap().is_none());
    let paths = [old.clone(), new.clone()];
    assert!(prewarm(&paths).is_err());
    assert_eq!(cached_membership(&old), Some(CachedMembership::FromIndex));
    for _ in 0..2 {
        let result = read_newest_member(&paths, &addr(1), 100);
        assert_ne!(
            result
                .as_ref()
                .ok()
                .and_then(|member| member.as_ref())
                .map(|member| member.payload.as_slice()),
            Some(older_payload.as_slice()),
            "damaged newest header must not return the older transcript"
        );
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "invalid archive member header");
    }
    assert_eq!(cached_membership(&new), Some(CachedMembership::Disabled));
}

/// A member header whose address or payload length was damaged, under an intact index:
/// the header check fails, the header rescan refuses to cache the container, and an
/// uncached lookup of that member fails loudly instead of reading as absent. A v1
/// container, whose membership is built by the header-checked route.
#[cfg(unix)]
#[test]
fn index_driven_cache_is_disabled_by_a_damaged_header_address_or_length() {
    let dir = TempDir::new();
    // Member 2's header follows member 1's header and its 3-byte payload.
    let header = 64 + 3;
    for (stamp, field, value) in [
        (1, 8..24, addr(9).into_bytes()),
        (2, 24..32, 4u64.to_le_bytes().to_vec()),
    ] {
        let path = dir.path(stamp);
        fixture_v1(
            &path,
            &[
                (addr(1), b"one".to_vec()),
                (addr(2), b"two".to_vec()),
                (addr(3), b"three".to_vec()),
            ],
        );
        let mut bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[header + 8..header + 24], addr(2).as_bytes());
        bytes[header + field.start..header + field.end].copy_from_slice(&value);
        std::fs::write(&path, bytes).unwrap();
        let paths = [path.clone()];
        let _ = prewarm(&paths);
        assert_eq!(
            cached_membership(&path),
            Some(CachedMembership::Disabled),
            "field {field:?}"
        );
        let error = read_newest_member(&paths, &addr(2), 100).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "field {field:?}");
        assert_eq!(
            error.to_string(),
            "archive index disagrees with header",
            "field {field:?}"
        );
    }
}

/// A pass that crashed before its seal leaves an unsealed container. Lookups skip it
/// before consulting the membership cache, so no membership is ever built for it.
#[cfg(unix)]
#[test]
fn unsealed_container_is_skipped_and_never_builds_membership() {
    let dir = TempDir::new();
    let old = dir.path(9);
    let new = dir.path(10);
    fixture(&old, &[(addr(1), b"committed".to_vec())]);
    let mut writer = ArchiveWriter::create(&new).unwrap().without_feature_floor();
    writer.append(&addr(1), b"uncommitted").unwrap();
    drop(writer);
    let paths = [old.clone(), new.clone()];
    assert!(prewarm(&paths).unwrap().is_none());
    assert_eq!(
        read_newest_member(&paths, &addr(1), 100)
            .unwrap()
            .unwrap()
            .payload,
        b"committed"
    );
    assert_eq!(cached_membership(&new), None);
    assert_eq!(cached_membership(&old), Some(CachedMembership::Verified));
}

/// A container whose index was removed for recovery (index_len = 0, seal kept) has no
/// index to check headers against, so its membership comes from the header rescan and
/// every member still resolves.
#[cfg(unix)]
#[test]
fn container_without_index_builds_membership_by_rescan() {
    let dir = TempDir::new();
    let path = dir.path(1);
    let members = (0..20)
        .map(|n| (addr(n), vec![n as u8; 100]))
        .collect::<Vec<_>>();
    fixture(&path, &members);
    std::fs::write(&path, without_index(&std::fs::read(&path).unwrap())).unwrap();
    let paths = [path.clone()];
    assert!(prewarm(&paths).unwrap().is_none());
    assert_eq!(
        cached_membership(&path),
        Some(CachedMembership::FromHeaders)
    );
    for (name, payload) in &members {
        assert_eq!(
            &read_newest_member(&paths, name, 1000)
                .unwrap()
                .unwrap()
                .payload,
            payload
        );
    }
}

/// Reports the membership build time and route for real containers. Set
/// BROCA_ARCHIVE_PROBE to one container path, or several joined like PATH. The first
/// lookup of each container builds its membership; later samples hit the built cache.
/// To measure a cold build, evict the files from the page cache before running it.
#[test]
#[ignore = "requires BROCA_ARCHIVE_PROBE pointing to a real sealed container"]
fn real_container_absent_lookup_probe() {
    let probe = std::env::var_os("BROCA_ARCHIVE_PROBE").expect("archive path");
    let containers = std::env::split_paths(&probe).collect::<Vec<_>>();
    for path in &containers {
        let start = std::time::Instant::now();
        assert!(
            read_newest_member(std::slice::from_ref(path), "ffffffffffffffff", 1024)
                .unwrap()
                .is_none()
        );
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        #[cfg(unix)]
        let route = format!("{:?}", cached_membership(path));
        #[cfg(not(unix))]
        let route = "uncached on this platform";
        eprintln!(
            "membership build (first lookup) {}: {elapsed:.3} ms, route {route}",
            path.display()
        );
    }
    let paths = containers;
    let mut samples = Vec::new();
    for n in 0..11 {
        let start = std::time::Instant::now();
        assert!(read_newest_member(&paths, "ffffffffffffffff", 1024)
            .unwrap()
            .is_none());
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        eprintln!("absent read_newest_member sample {n}: {elapsed:.6} ms");
        if n != 0 {
            samples.push(elapsed);
        }
    }
    samples.sort_by(f64::total_cmp);
    eprintln!("warm median: {:.6} ms", (samples[4] + samples[5]) / 2.0);
}

#[test]
fn missing_index_legacy_container_bytes_are_pinned() {
    let bytes = without_index(include_bytes!(
        "../../tests/fixtures/archive/v1/fold-1001.ark"
    ));
    assert_eq!(bytes.len(), 827);
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "8f1c97ed5a9bc50773da34470ab138cadbe9afd5f5d09e5ce29937182bc6d118"
    );
    assert_eq!(&bytes[bytes.len() - 8..], b"\0\0\0\0WALK");
    assert!(!ArchiveReader::new(Cursor::new(bytes), 4096)
        .rescan()
        .unwrap()
        .is_empty());
}

#[test]
fn newer_bad_magic_with_missing_index_never_returns_older_transcript() {
    let dir = TempDir::new();
    for missing in [true, false] {
        let old = dir.path(if missing { 11 } else { 21 });
        let new = dir.path(if missing { 12 } else { 22 });
        fixture_v1(&old, &[(addr(1), b"older transcript".to_vec())]);
        fixture_v1(
            &new,
            &[
                (addr(5), b"five".to_vec()),
                (addr(1), b"newer transcript".to_vec()),
                (addr(7), b"seven".to_vec()),
            ],
        );
        let mut bytes = std::fs::read(&new).unwrap();
        if missing {
            bytes = without_index(&bytes);
        } else {
            let index_len =
                u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
                    as usize;
            let start = bytes.len() - 8 - index_len;
            bytes[start] ^= 0xff;
        }
        bytes[64 + 4] ^= 1;
        std::fs::write(&new, bytes).unwrap();
        let mut direct = ArchiveReader::open(&new, 100).unwrap();
        assert!(direct.rescan().is_err());
        assert!(direct.read_member(&addr(1)).is_err());
        let paths = [old, new.clone()];
        for _ in 0..2 {
            assert!(
                read_newest_member(&paths, &addr(1), 100).is_err(),
                "uncertain newer coverage must not return older bytes"
            );
        }
        #[cfg(unix)]
        assert_eq!(cached_membership(&new), Some(CachedMembership::Disabled));
    }
}

#[test]
fn archive_scan_refuses_uncertain_member_boundary() {
    let dir = TempDir::new();
    let path = dir.path(30);
    fixture_v1(
        &path,
        &[(addr(1), b"one".to_vec()), (addr(2), b"two".to_vec())],
    );
    let mut bytes = std::fs::read(&path).unwrap();
    let trailer = bytes.len() - 8;
    bytes[trailer..trailer + 4].copy_from_slice(&135u32.to_le_bytes());
    let mut reader = ArchiveReader::new(Cursor::new(bytes), 100);
    assert!(
        reader.rescan().is_err(),
        "a trailer length swallowing a member is not coverage proof"
    );
}
