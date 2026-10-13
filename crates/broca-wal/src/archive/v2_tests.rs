//! Index format v2: the digest-bound index, read by the current reader.
//!
//! Containers here are built by a writer local to these tests, which lays the bytes out
//! from the format description rather than through `ArchiveWriter`, so a reader defect and
//! a writer defect cannot cancel out.
use super::*;
use std::{
    cell::RefCell,
    io::Cursor,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};

const STAMP: u64 = 7;

fn addr(n: u64) -> String {
    format!("{n:016x}")
}

/// The member region `ArchiveWriter::append` lays out, and one index entry per member in
/// append order.
pub(super) fn member_region(members: &[(String, Vec<u8>)]) -> (Vec<u8>, Vec<[u8; 32]>) {
    let mut region = Vec::new();
    let mut entries = Vec::new();
    for (name, payload) in members {
        let offset = region.len() as u64;
        region.extend_from_slice(b"WALM");
        region.extend_from_slice(&1u16.to_le_bytes());
        region.extend_from_slice(&[0, 0]);
        region.extend_from_slice(name.as_bytes());
        region.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        region.extend_from_slice(&Sha256::digest(payload));
        region.extend_from_slice(payload);
        let mut entry = [0; 32];
        entry[..16].copy_from_slice(name.as_bytes());
        entry[16..24].copy_from_slice(&offset.to_le_bytes());
        entry[24..32].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        entries.push(entry);
    }
    (region, entries)
}

/// Seal `region` with a v2 index listing `entries` in exactly the given order, its digest
/// computed over those bytes and bound to `stamp`.
pub(super) fn seal_v2(stamp: u64, region: &[u8], entries: &[[u8; 32]]) -> Vec<u8> {
    let mut index = (entries.len() as u32).to_le_bytes().to_vec();
    for entry in entries {
        index.extend_from_slice(entry);
    }
    let mut fixed = [0u8; 32];
    fixed[..4].copy_from_slice(b"IDX2");
    let index_len = (index.len() + 64) as u32;
    let mut hasher = Sha256::new();
    hasher.update(b"broca-ark-index/v2\0");
    hasher.update(stamp.to_le_bytes());
    hasher.update(index_len.to_le_bytes());
    hasher.update(&index);
    hasher.update(fixed);
    let mut bytes = region.to_vec();
    bytes.extend_from_slice(&index);
    bytes.extend_from_slice(&hasher.finalize());
    bytes.extend_from_slice(&fixed);
    bytes.extend_from_slice(&index_len.to_le_bytes());
    bytes.extend_from_slice(b"WALK");
    bytes
}

/// A v2 container as the format requires it to be sealed: entries sorted by address.
pub(super) fn v2_container(stamp: u64, members: &[(String, Vec<u8>)]) -> Vec<u8> {
    let (region, mut entries) = member_region(members);
    entries.sort_unstable_by(|a, b| a[..16].cmp(&b[..16]));
    seal_v2(stamp, &region, &entries)
}

/// A v1 container, as every writer before index format v2 sealed it: the index lists the
/// members in append (offset) order, with no extension.
pub(super) fn v1_container(members: &[(String, Vec<u8>)]) -> Vec<u8> {
    let (mut bytes, entries) = member_region(members);
    let start = bytes.len();
    bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in &entries {
        bytes.extend_from_slice(entry);
    }
    let index_len = (bytes.len() - start) as u32;
    bytes.extend_from_slice(&index_len.to_le_bytes());
    bytes.extend_from_slice(b"WALK");
    bytes
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) enum Kind {
    Unsealed,
    Verified,
    Unverified,
    Invalid,
}

pub(super) fn kind_of<R: Read + Seek>(reader: &mut ArchiveReader<R>) -> Kind {
    let (state, end, index_len) = reader.trailer().unwrap();
    if state == ContainerState::Unsealed {
        return Kind::Unsealed;
    }
    match reader.load_index(end, index_len).unwrap() {
        IndexOutcome::Verified { .. } => Kind::Verified,
        IndexOutcome::Unverified { .. } => Kind::Unverified,
        IndexOutcome::Invalid(_) => Kind::Invalid,
    }
}

pub(super) fn kind(bytes: &[u8], stamp: u64) -> Kind {
    kind_of(&mut ArchiveReader::new(Cursor::new(bytes.to_vec()), 0).with_stamp(stamp))
}

fn reader(bytes: &[u8]) -> ArchiveReader<Cursor<Vec<u8>>> {
    ArchiveReader::new(Cursor::new(bytes.to_vec()), 1 << 20).with_stamp(STAMP)
}

fn members(count: u64) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|n| {
            // Spread the addresses so address order differs from append order.
            (
                addr(n.wrapping_mul(0x9e37_79b9) & 0xffff_ffff),
                vec![n as u8; (n as usize * 37) % 200],
            )
        })
        .collect()
}

/// Every read a traced source served, as (offset, length).
type Reads = Rc<RefCell<Vec<(u64, u64)>>>;

/// Records every read the source serves, as (offset, length), below any buffering.
struct Traced {
    inner: Cursor<Vec<u8>>,
    reads: Reads,
}
impl Read for Traced {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let at = self.inner.position();
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.reads.borrow_mut().push((at, n as u64));
        }
        Ok(n)
    }
}
impl Seek for Traced {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

fn traced(bytes: &[u8], capacity: usize) -> (ArchiveReader<Traced>, Reads) {
    let reads = Rc::new(RefCell::new(Vec::new()));
    let reader = ArchiveReader {
        source: BufReader::with_capacity(
            capacity,
            Traced {
                inner: Cursor::new(bytes.to_vec()),
                reads: reads.clone(),
            },
        ),
        max_member_bytes: 1 << 20,
        stamp: Some(STAMP),
        index: None,
    };
    (reader, reads)
}

fn members_end(bytes: &[u8]) -> u64 {
    let trailer = bytes.len() - 8;
    let index_len = u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap());
    (trailer - index_len as usize) as u64
}

/// Every member of a v2 container resolves, and a miss is answered by
/// the verified index alone, with no read of the member region at the unbuffered layer,
/// both unbuffered and through the 64 KiB buffer `ArchiveReader::open` uses.
#[test]
fn v2_round_trip_resolves_every_member_and_a_miss_reads_no_member_bytes() {
    let members = members(60);
    let bytes = v2_container(STAMP, &members);
    let mut reader = reader(&bytes);
    for (name, payload) in &members {
        assert_eq!(&reader.read_member(name).unwrap().unwrap().payload, payload);
    }
    assert!(matches!(reader.index, Some(IndexOutcome::Verified { .. })));
    let region_end = members_end(&bytes);
    for capacity in [0, 64 * 1024] {
        let (mut reader, reads) = traced(&bytes, capacity);
        for n in [1u64 << 40, 3, 0xffff_ffff_ffff] {
            assert!(reader.read_member(&addr(n)).unwrap().is_none());
        }
        let reads = reads.borrow();
        assert!(!reads.is_empty());
        for (offset, len) in reads.iter() {
            assert!(
                *offset >= region_end,
                "capacity {capacity}: read of {len} bytes at {offset} is inside the member region ending at {region_end}"
            );
        }
    }
    // The same members under a v1 index: the same misses scan every header, so the
    // assertion above can fail.
    let (mut reader, reads) = traced(&v1_container(&members), 0);
    assert!(reader.read_member(&addr(3)).unwrap().is_none());
    assert!(reads.borrow().iter().any(|(offset, _)| *offset == 0));
}

/// Flip every byte of a small v2 container's index, extension and
/// trailer, three ways each, and change `index_len` by whole entries and extensions. No
/// variant may verify. Every variant must also keep the guarded reader's semantics: a
/// member is served intact or refused with an error, never reported absent, and an index
/// address the damage fabricated still reports its disagreement with the header.
#[test]
fn every_index_extension_and_trailer_flip_is_never_verified() {
    let members = members(3);
    let intact = v2_container(STAMP, &members);
    assert_eq!(kind(&intact, STAMP), Kind::Verified);
    let start = members_end(&intact) as usize;
    let trailer = intact.len() - 8;
    let mut variants = Vec::new();
    for position in start..intact.len() {
        for pattern in [0x01, 0x20, 0xff] {
            let mut bytes = intact.clone();
            bytes[position] ^= pattern;
            variants.push((
                format!("byte {position} ^ {pattern:#04x}"),
                Some(position),
                bytes,
            ));
        }
    }
    let index_len = (intact.len() - 8 - start) as i64;
    for delta in [-64, -32, 32, 64, -index_len] {
        let mut bytes = intact.clone();
        bytes[trailer..trailer + 4].copy_from_slice(&((index_len + delta) as u32).to_le_bytes());
        variants.push((format!("index_len {delta:+}"), None, bytes));
    }
    let names = members
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    for (label, position, bytes) in &variants {
        let kind = kind(bytes, STAMP);
        seen.insert(format!("{kind:?}"));
        assert_ne!(kind, Kind::Verified, "{label}");
        if kind == Kind::Unsealed {
            assert!(
                position.is_some_and(|p| p >= trailer + 4),
                "{label}: only damage to the trailer magic may unseal"
            );
        }
        let mut reader = reader(bytes);
        for (name, payload) in &members {
            match reader.read_member(name) {
                Ok(Some(member)) => assert_eq!(&member.payload, payload, "{label}"),
                Ok(None) => panic!("{label}: present member {name} read as absent"),
                Err(error) if kind == Kind::Unsealed => {
                    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied, "{label}")
                }
                Err(error) => assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{label}"),
            }
        }
        if kind != Kind::Unverified {
            continue;
        }
        // Any valid address the damage wrote into an entry names no member. The guarded
        // reader reports that disagreement rather than a miss.
        for entry in 0..members.len() {
            let at = start + 4 + 32 * entry;
            let Ok(name) = std::str::from_utf8(&bytes[at..at + 16]) else {
                continue;
            };
            if address(name).is_err() || names.contains(name) {
                continue;
            }
            assert_eq!(
                reader.read_member(name).unwrap_err().to_string(),
                "archive index disagrees with header",
                "{label}"
            );
        }
    }
    // The flips reach every other outcome, so none of the branches above is vacuous.
    assert_eq!(
        seen,
        ["Invalid", "Unsealed", "Unverified"]
            .map(String::from)
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
}

/// A member header relabelled to another valid address, under an intact
/// verified index, cannot serve that member as the other address. The relabelled address
/// is absent and the real one fails loudly. Under a v1 index the same damage serves the
/// member under the wrong address, which is the gap the verified index closes.
#[test]
fn a_relabelled_header_reads_as_absent_under_a_verified_index() {
    let members = vec![
        (addr(1), b"one".to_vec()),
        (addr(2), b"two".to_vec()),
        (addr(3), b"three".to_vec()),
    ];
    let relabel = |mut bytes: Vec<u8>| {
        // Member 2's header follows member 1's header and its 3-byte payload.
        let header = 64 + 3;
        assert_eq!(&bytes[header + 8..header + 24], addr(2).as_bytes());
        bytes[header + 8..header + 24].copy_from_slice(addr(9).as_bytes());
        bytes
    };
    let v2 = relabel(v2_container(STAMP, &members));
    assert_eq!(kind(&v2, STAMP), Kind::Verified);
    let mut reader = reader(&v2);
    assert!(reader.read_member(&addr(9)).unwrap().is_none());
    assert_eq!(
        reader.read_member(&addr(2)).unwrap_err().to_string(),
        "archive index disagrees with header"
    );
    let v1 = relabel(v1_container(&members));
    let mut reader = reader_v1(&v1);
    assert_eq!(
        reader.read_member(&addr(9)).unwrap().unwrap().payload,
        b"two"
    );

    // Through the process-wide lookup, which consults the membership cache first.
    let dir = scratch("relabel");
    let path = dir.join(format!("fold-{STAMP}.ark"));
    std::fs::write(&path, &v2).unwrap();
    let paths = [path];
    for _ in 0..2 {
        assert!(read_newest_member(&paths, &addr(9), 100).unwrap().is_none());
        assert_eq!(
            read_newest_member(&paths, &addr(2), 100)
                .unwrap_err()
                .to_string(),
            "archive index disagrees with header"
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}

fn reader_v1(bytes: &[u8]) -> ArchiveReader<Cursor<Vec<u8>>> {
    ArchiveReader::new(Cursor::new(bytes.to_vec()), 1 << 20).with_stamp(STAMP)
}

fn scratch(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-archive-v2-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// An index tail copied from another container, whose offsets and lengths fit this
/// one exactly, never verifies, because the digest binds the stamp in the file name.
#[test]
fn a_spliced_index_tail_from_another_container_never_verifies() {
    let donor = v2_container(
        5,
        &[(addr(1), b"abc".to_vec()), (addr(2), b"defg".to_vec())],
    );
    assert_eq!(kind(&donor, 5), Kind::Verified);
    let donor_tail = &donor[members_end(&donor) as usize..];
    // Same payload lengths, so the donor's offsets tile this container exactly.
    for host_members in [
        vec![(addr(3), b"xyz".to_vec()), (addr(4), b"uvwx".to_vec())],
        vec![(addr(1), b"xyz".to_vec()), (addr(2), b"uvwx".to_vec())],
    ] {
        let host = v2_container(6, &host_members);
        let mut spliced = host[..members_end(&host) as usize].to_vec();
        spliced.extend_from_slice(donor_tail);
        assert_eq!(kind(&spliced, 6), Kind::Unverified);
        let mut reader = ArchiveReader::new(Cursor::new(spliced), 100).with_stamp(6);
        for (name, payload) in &host_members {
            assert_eq!(&reader.read_member(name).unwrap().unwrap().payload, payload);
        }
        if host_members[0].0 == addr(3) {
            assert_eq!(
                reader.read_member(&addr(1)).unwrap_err().to_string(),
                "archive index disagrees with header"
            );
        }
    }
}

/// A v2 container whose index no longer verifies keeps the guarded
/// reader's cross-checks. A damaged header of a member the index lists still fails the
/// lookup loudly instead of letting an older container's transcript through.
#[test]
fn a_damaged_member_header_under_an_unverified_v2_index_fails_loudly() {
    let older = b"older transcript";
    let newer = vec![
        (addr(5), b"five".to_vec()),
        (addr(1), b"newer transcript".to_vec()),
        (addr(7), b"seven".to_vec()),
    ];
    for damage_digest in [true, false] {
        let dir = scratch("damaged-header");
        let old = dir.join("fold-9.ark");
        let new = dir.join("fold-10.ark");
        std::fs::write(&old, v2_container(9, &[(addr(1), older.to_vec())])).unwrap();
        let mut bytes = v2_container(10, &newer);
        // Member 1 is the second member, after one header and the 4-byte payload "five".
        let header = 64 + 4;
        assert_eq!(&bytes[header + 8..header + 24], addr(1).as_bytes());
        bytes[header] ^= 1;
        if damage_digest {
            let digest = bytes.len() - 8 - 64;
            bytes[digest] ^= 1;
        }
        assert_eq!(
            kind(&bytes, 10),
            if damage_digest {
                Kind::Unverified
            } else {
                Kind::Verified
            }
        );
        std::fs::write(&new, bytes).unwrap();
        let paths = [old.clone(), new.clone()];
        for _ in 0..2 {
            let error = read_newest_member(&paths, &addr(1), 100).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(error.to_string(), "invalid archive member header");
        }
        // The other members still resolve, and an absent address is still absent.
        assert_eq!(
            read_newest_member(&paths, &addr(7), 100)
                .unwrap()
                .unwrap()
                .payload,
            b"seven"
        );
        assert!(read_newest_member(&paths, &addr(8), 100).unwrap().is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }
}

/// A digest that matches never excuses geometry a v1 index would fail: a gap, a
/// duplicate, an entry past the member region, or an index that omits a member. Each is
/// `Invalid` and every member is still found by the header scan.
#[test]
fn a_matching_digest_never_excuses_bad_geometry() {
    let members = members(4);
    let (region, entries) = member_region(&members);
    let mut gap = entries.clone();
    let second_offset = u64::from_le_bytes(entries[1][16..24].try_into().unwrap());
    gap[1][16..24].copy_from_slice(&(second_offset + 1).to_le_bytes());
    let mut duplicate = entries.clone();
    duplicate[2][..16].copy_from_slice(&entries[1][..16]);
    let mut beyond = entries.clone();
    beyond[3][24..32].copy_from_slice(&(1u64 << 40).to_le_bytes());
    let omitted = entries[..3].to_vec();
    for (label, entries) in [
        ("gap", gap),
        ("duplicate", duplicate),
        ("beyond", beyond),
        ("omitted", omitted),
    ] {
        let bytes = seal_v2(STAMP, &region, &entries);
        assert_eq!(kind(&bytes, STAMP), Kind::Invalid, "{label}");
        let mut reader = reader(&bytes);
        for (name, payload) in &members {
            assert_eq!(
                &reader.read_member(name).unwrap().unwrap().payload,
                payload,
                "{label}"
            );
        }
    }
}

/// The digest covers the entries exactly as they are on disk. An index whose entries are
/// not in address order, with a digest over those bytes, verifies; it is only marked as
/// unsuitable for an on-disk binary search.
#[test]
fn an_unsorted_index_verifies_by_the_bytes_on_disk() {
    let members = members(20);
    let (region, entries) = member_region(&members);
    let bytes = seal_v2(STAMP, &region, &entries);
    let mut reader = reader(&bytes);
    let (end, index_len) = reader.sealed_trailer().unwrap();
    assert!(matches!(
        reader.load_index(end, index_len).unwrap(),
        IndexOutcome::Verified {
            sorted_on_disk: false,
            ..
        }
    ));
    for (name, payload) in &members {
        assert_eq!(&reader.read_member(name).unwrap().unwrap().payload, payload);
    }
    assert!(reader.read_member(&addr(1 << 40)).unwrap().is_none());
    assert!(matches!(reader.index, Some(IndexOutcome::Verified { .. })));
    let sorted = v2_container(STAMP, &members);
    let mut reader = reader_v1(&sorted);
    let (end, index_len) = reader.sealed_trailer().unwrap();
    assert!(matches!(
        reader.load_index(end, index_len).unwrap(),
        IndexOutcome::Verified {
            sorted_on_disk: true,
            ..
        }
    ));
}

/// The stamp comes from the container's name. Without one, or with another one, a v2
/// index is read exactly as a v1 index is: never trusted for absence.
#[test]
fn a_v2_index_without_its_stamp_is_never_verified() {
    let members = members(5);
    let bytes = v2_container(STAMP, &members);
    assert_eq!(kind(&bytes, STAMP + 1), Kind::Unverified);
    let mut unstamped = ArchiveReader::new(Cursor::new(bytes.clone()), 1 << 20);
    assert_eq!(kind_of(&mut unstamped), Kind::Unverified);
    let dir = scratch("names");
    for (name, expected) in [
        (format!("fold-{STAMP}.ark"), Kind::Verified),
        (format!("fold-0{STAMP}.ark"), Kind::Unverified),
        ("container.ark".to_owned(), Kind::Unverified),
    ] {
        let path = dir.join(&name);
        std::fs::write(&path, &bytes).unwrap();
        let mut reader = ArchiveReader::open(&path, 1 << 20).unwrap();
        assert_eq!(kind_of(&mut reader), expected, "{name}");
        for (member, payload) in &members {
            assert_eq!(
                &reader.read_member(member).unwrap().unwrap().payload,
                payload
            );
        }
        assert!(reader.read_member(&addr(1 << 40)).unwrap().is_none());
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// A read that fails while the index loads is returned as that error, never as a miss,
/// and is not remembered: the next lookup on the same reader retries and succeeds.
#[test]
fn an_io_error_while_loading_the_index_is_returned_not_cached() {
    struct Flaky {
        inner: Cursor<Vec<u8>>,
        fail_from: u64,
        failures: Rc<RefCell<u32>>,
    }
    impl Read for Flaky {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let mut failures = self.failures.borrow_mut();
            if *failures > 0 && self.inner.position() >= self.fail_from {
                *failures -= 1;
                return Err(io::Error::other("injected read failure"));
            }
            self.inner.read(buf)
        }
    }
    impl Seek for Flaky {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.inner.seek(pos)
        }
    }
    let members = members(5);
    let bytes = v2_container(STAMP, &members);
    let failures = Rc::new(RefCell::new(0));
    let mut reader = ArchiveReader::new(
        Flaky {
            fail_from: members_end(&bytes),
            inner: Cursor::new(bytes.clone()),
            failures: failures.clone(),
        },
        1 << 20,
    )
    .with_stamp(STAMP);
    // The trailer is read first and succeeds; the index read that follows fails.
    reader.sealed_trailer().unwrap();
    *failures.borrow_mut() = 1;
    let (end, index_len) = (
        bytes.len() as u64 - 8,
        (bytes.len() as u64 - 8 - members_end(&bytes)) as u32,
    );
    assert_eq!(
        reader
            .indexed(&addr(1 << 40), end, index_len)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
    assert!(reader.index.is_none());
    assert!(reader.read_member(&addr(1 << 40)).unwrap().is_none());
    assert!(matches!(reader.index, Some(IndexOutcome::Verified { .. })));
}
